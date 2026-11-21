"""Multi-table CREATE interread using unchanged actual protocol values."""
import copy,fcntl,json,shutil
from pathlib import Path
from create_reference_oracle import canonical_independent
from empty_catalog_oracle import transport_native
from empty_reference_fixture import export_actual_tables
from reverse_reference import Reference,qi
from lake_harness import Blocked,sha256,write_json

ROOT=Path('/home/qiumc/data/code/cpp');DRIVER=ROOT/'integration/opengauss-native-lake'


def table_identity_map(exported):
    schemas={row['schema_id']:row['schema_name'] for table in exported['tables']
        if table['table_name']=='ducklake_schema' for row in table['rows']}
    rows=[row for table in exported['tables'] if table['table_name']=='ducklake_table' for row in table['rows']]
    logical={(schemas[row['schema_id']],row['table_name']):row['table_id'] for row in rows}
    if len(logical)!=len(rows) or len({row['table_id'] for row in rows})!=len(rows):
        raise AssertionError('This CREATE-only oracle requires distinct logical and permanent table identities')
    if sorted(logical.values())!=list(range(1,len(rows)+1)):
        raise AssertionError('CREATE did not allocate the exact catalog ID range')
    return {logical[name]:index for index,name in enumerate(sorted(logical),1)}


def canonical_changes(text,mapping):
    # Snapshot change groups are sets; quote-aware tokenization preserves commas
    # and doubled quotes inside names and preserves duplicate group multiplicity.
    chunks=[];part=[];quoted=False;i=0
    while i<len(text):
        char=text[i]
        if char=='"':
            part.append(char)
            if quoted and i+1<len(text) and text[i+1]=='"':
                part.append('"');i+=2;continue
            quoted=not quoted
        elif char==',' and not quoted:
            chunks.append(''.join(part));part=[]
        else:part.append(char)
        i+=1
    if quoted:raise AssertionError('Malformed quoted snapshot change group')
    chunks.append(''.join(part))
    for index,chunk in enumerate(chunks):
        if chunk.startswith(('inlined_insert:','inlined_delete:')):
            kind,identity=chunk.split(':',1)
            chunks[index]=kind+':'+str(mapping[int(identity)])
    return ','.join(sorted(chunks))


def comparison(exported):
    # Only an independently generated UUID-derived directory is normalized.
    # The actual catalog/transport retains the original path and UUID bytes.
    copied=copy.deepcopy(exported);mapping=table_identity_map(exported)
    for table in copied['tables']:
        for row in table['rows']:
            if 'table_id' in row and row['table_id'] is not None:
                row['table_id']=mapping[row['table_id']]
            if table['table_name']=='ducklake_table':
                if row['path']==str(row['table_uuid'])+'/':
                    row['path']='<uuid-derived-table-path:%s>/'%row['table_id']
            elif table['table_name']=='ducklake_snapshot_changes':
                row['changes_made']=canonical_changes(row['changes_made'],mapping)
    return canonical_independent(copied)


def interread(t,groups,business,continuation,label):
    """business maps actual names to exact expected visible business values."""
    out=t.args.output.parent/label;out.mkdir(exist_ok=False)
    lock_path=DRIVER/'source-lock.json';lock_hash=sha256(lock_path)
    receipt=json.loads((ROOT/'.reference/opengauss-native-lake/build/receipt.json').read_text())
    ref=Reference(out,json.loads(lock_path.read_text()),receipt)
    if receipt['source_lock_hash']!=lock_hash or sha256(ref.binary)!=receipt['binary_sha256']:
        raise Blocked('Fixed reference build receipt is stale')
    native=t.export(label+'-native.json');original=json.dumps(native,sort_keys=True,ensure_ascii=False)
    source=Path(t.empty_manifest['catalog']);source_hash=sha256(source)
    matches=[x for x in t.empty_manifest['files'] if x['path']==str(source)]
    if len(matches)!=1 or matches[0]['sha256']!=source_hash or Path(str(source)+'.wal').exists():
        raise AssertionError('Original EMPTY changed')
    by_name={x['table_name']:x for x in native['tables']}
    definitions={x['table_name']:x for x in by_name['ducklake_table']['rows'] if x['end_snapshot'] is None}
    registrations={x['table_id']:x for x in by_name['ducklake_inlined_data_tables']['rows']}
    t.check('visible business definitions match the full test population',sorted(definitions),sorted(business))
    with (ROOT/'.reference/opengauss-native-lake/.driver.lock').open('a+') as mutex:
        fcntl.flock(mutex,fcntl.LOCK_EX|fcntl.LOCK_NB)
        native_catalog=out/'native.ducklake';transport_native(ref,native_catalog,native)
        unchanged=sha256(native_catalog)
        for name,expected in business.items():
            table=definitions[name];registered=registrations[table['table_id']]
            physical=by_name[registered['table_name']]['rows']
            visible=[x for x in physical if x['_ducklake_end_snapshot'] is None]
            visible.sort(key=lambda x:x['_ducklake_row_id'])
            logical=[{'logical_rowid':x['_ducklake_row_id'],'begin_snapshot':x['_ducklake_begin_snapshot'],
                **{k:v for k,v in x.items() if not k.startswith('_ducklake_')}} for x in visible]
            actual=ref.logical(native_catalog,'lake.main.'+qi(name),'native-'+str(table['table_id']))
            t.check('normal reference reads exact native identity and versions: '+name,actual,logical)
            t.check('native business values match independently specified expected rows: '+name,
                sorted([{k:v for k,v in row.items() if k not in ('logical_rowid','begin_snapshot')} for row in actual],key=lambda x:x['id']),expected)
        t.check('readonly normal ATTACH preserves native transport bytes',sha256(native_catalog),unchanged)
        replay=out/'independent.ducklake';shutil.copy2(source,replay)
        for index,sql in enumerate(groups):
            ref.run(ref.attach(replay)+'BEGIN;\n'+';\n'.join(sql)+';\nCOMMIT;','independent-'+str(index))
        independent={'tables':export_actual_tables(ref,replay)}
        write_json(out/'independent-export.json',independent)
        write_json(out/'comparison-only-table-allocation.json',{'native':table_identity_map(native),
            'independent_reference':table_identity_map(independent),
            'scope':'Bijection for differential comparison only; actual native/transport IDs are never modified. Snapshot/column/row IDs and counter high-water marks stay exact.'})
        observed,wanted=comparison(native),comparison(independent)
        write_json(out/'native-comparison.json',observed);write_json(out/'reference-comparison.json',wanted)
        t.check('independent complete standard and physical carrier comparison',observed,wanted)
        continued=out/'continued.ducklake';shutil.copy2(native_catalog,continued)
        for name,case in continuation.items():
            relation='lake.main.'+qi(name)
            before=ref.logical(continued,relation,'before-continuation-'+str(definitions[name]['table_id']))
            ref.run(ref.attach(continued)+'BEGIN;\n'+';\n'.join(case['sql'])+';\nCOMMIT;',
                'continue-'+str(definitions[name]['table_id']))
            after=ref.logical(continued,relation,'after-continuation-'+str(definitions[name]['table_id']))
            t.check('normal reference DML continuation business values: '+name,
                sorted([{k:v for k,v in row.items() if k not in ('logical_rowid','begin_snapshot')} for row in after],key=lambda x:x['id']),case['expected'])
            old={x['id']:x['logical_rowid'] for x in before};new={x['id']:x['logical_rowid'] for x in after}
            t.check('continuation preserves surviving native row identities: '+name,
                {k:new[k] for k in old if k in new},{k:v for k,v in old.items() if k in new})
        t.check('normal reference continuation leaves original transport immutable',sha256(native_catalog),unchanged)
        t.check('initial EMPTY remains immutable',sha256(source),source_hash)
        t.check('source lock remains frozen',sha256(lock_path),lock_hash)
        t.check('actual native protocol export was never rewritten',json.dumps(native,sort_keys=True,ensure_ascii=False),original)
    t.result.setdefault('reference_checkpoints',[]).append({'directory':str(out),'catalog_sha256':unchanged,
        'comparison_only_normalizations':['independent schema/table UUID','UUID-derived table path','snapshot timestamp','producer','registered carrier spelling','independent table-ID allocation bijection and unordered change groups'],
        'scope':'offline actual native transport, independent business replay and normal reference continuation'})
