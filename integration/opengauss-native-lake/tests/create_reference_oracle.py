"""Offline CREATE/file-DML oracle; no protocol value is repaired during transport.

Runtime helper only. Import/compile does not run DuckDB or openGauss.
"""
from collections import Counter
import fcntl,json,shutil,sys
from pathlib import Path

ROOT=Path('/home/qiumc/data/code/cpp')
DRIVER=ROOT/'integration/opengauss-native-lake'
sys.path.insert(0,str(DRIVER));sys.path.insert(0,str(DRIVER/'tests'))
from inline_fixture import query,scalar,normalize
from lake_harness import Blocked,sha256,write_json
from empty_catalog_oracle import read_native_catalog,canonical_shape,transport_native
from empty_reference_fixture import export_actual_tables
from reverse_reference import Reference,qi


def export_created_catalog(conn,schema,empty_export,output):
    """Use genuine EMPTY for standard tables and actual metadata for carriers.

    Inlined file deletions use the pinned DuckLake deterministic table-ID name,
    not ducklake_inlined_data_tables. Admit only such names that really exist;
    their exact three-column shape is checked after the independent host read.

    read_native_catalog independently reads host physical definitions and values.
    The extra names below are observed metadata, never fabricated protocol rows.
    Caller must own the DB and freeze its writer connections during export.
    """
    from psycopg2 import sql
    carriers=[r[0] for r in query(conn,sql.SQL('SELECT table_name FROM {}.ducklake_inlined_data_tables ORDER BY table_name').format(sql.Identifier(schema)))]
    if len(set(carriers))!=len(carriers):raise AssertionError('Duplicate actual inline carriers')
    table_ids=[r[0] for r in query(conn,sql.SQL('SELECT DISTINCT table_id FROM {}.ducklake_table ORDER BY table_id').format(sql.Identifier(schema)))]
    if any(type(table_id) is not int or table_id<0 for table_id in table_ids):
        raise AssertionError('Invalid actual lake table identity for file-delete carrier')
    possible_deletes={'ducklake_inlined_delete_'+str(table_id) for table_id in table_ids}
    observed_names={r[0] for r in query(conn,
        'SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace '
        "WHERE n.nspname=%s AND c.relkind IN ('r','f','v','m','p','S')",(schema,))}
    deletes=sorted(possible_deletes & observed_names)
    if set(carriers) & possible_deletes:
        raise AssertionError('File-delete carrier incorrectly registered as inline data')
    expected={'tables':list(empty_export['tables'])+[{'table_name':name} for name in carriers+deletes]}
    actual=read_native_catalog(conn,schema,expected,output)
    by_name={t['table_name']:t for t in actual['tables']}
    for source in empty_export['tables']:
        if canonical_shape(by_name[source['table_name']])!=canonical_shape(source):
            raise AssertionError('Actual standard physical shape differs: '+source['table_name'])
    for name in carriers:
        cols=by_name[name]['columns']
        if [(c['column_name'],c['data_type']) for c in cols[:3]]!=[
                ('_ducklake_row_id','BIGINT'),('_ducklake_begin_snapshot','BIGINT'),('_ducklake_end_snapshot','BIGINT')]:
            raise AssertionError('Actual carrier does not preserve three standard identity columns')
    for name in deletes:
        if canonical_shape(by_name[name])!={'columns':[
                ('file_id','BIGINT',True),('row_id','BIGINT',True),
                ('begin_snapshot','BIGINT',True)],'keys':[]} or by_name[name]['constraints']:
            raise AssertionError('Actual file-delete carrier differs from pinned standard shape: '+name)
    return actual


def canonical_independent(exported):
    """Comparison-only normalization; exported/transported values stay unchanged.

    These first cases have a single safe ASCII table name; its path must match
    exactly. No table/column/snapshot/row ID, count, bounds or changeset is masked.
    Only independent UUIDs, commit timestamps, producer, and registered physical
    carrier spelling differ. Registered carrier names also key actual table maps.
    """
    by_name={t['table_name']:t for t in exported['tables']}
    names={r['table_name']:'<carrier:%s:%s>'%(r['table_id'],r['schema_version'])
           for r in by_name['ducklake_inlined_data_tables']['rows']}
    if len(names)!=len(by_name['ducklake_inlined_data_tables']['rows']):raise AssertionError('Ambiguous carrier mapping')
    result={}
    for name,table in by_name.items():
        key=names.get(name,name)
        if key in result:raise AssertionError('Duplicate comparison key')
        rows=[]
        for raw in table['rows']:
            row={c['column_name']:normalize(raw[c['column_name']],c['data_type']) for c in table['columns']}
            if name=='ducklake_snapshot':row['snapshot_time']='<independent-transaction-time>'
            elif name=='ducklake_schema':row['schema_uuid']='<independent-schema-uuid:%s>'%row['schema_id']
            elif name=='ducklake_table':row['table_uuid']='<independent-table-uuid:%s>'%row['table_id']
            elif name=='ducklake_metadata' and row['key']=='created_by':row['value']='<declared-producer>'
            elif name=='ducklake_inlined_data_tables':row['table_name']=names[row['table_name']]
            rows.append(json.dumps(row,ensure_ascii=False,sort_keys=True,separators=(',',':')))
        result[key]={'shape':canonical_shape(table),'rows':dict(Counter(rows))}
    return result


def check_reference_continuation(t,native,groups,expected,output):
    """Original native transport is readonly; continuation mutates owned copies.

    groups contains exact independent business DDL/DML per native commit; no
    standard metadata UPDATE, UUID replacement or implicit host commit occurs.
    """
    output=Path(output);output.mkdir(parents=True,exist_ok=False)
    lock_path=DRIVER/'source-lock.json';lock_hash=sha256(lock_path)
    lock=json.loads(lock_path.read_text())
    receipt=json.loads((ROOT/'.reference/opengauss-native-lake/build/receipt.json').read_text())
    ref=Reference(output,lock,receipt)
    if receipt['source_lock_hash']!=lock_hash or sha256(ref.binary)!=receipt['binary_sha256']:
        raise Blocked('Actual fixed reference build receipt is stale')
    if sha256(DRIVER/'reference.sh')!=lock['reference_build']['script_sha256']:
        raise Blocked('Actual fixed reference driver differs from source lock')
    source=Path(t.empty_manifest['catalog']);source_hash=sha256(source)
    source_entries=[item for item in t.empty_manifest['files'] if item['path']==str(source)]
    if len(source_entries)!=1 or source_hash!=source_entries[0]['sha256'] or Path(str(source)+'.wal').exists():
        raise AssertionError('Genuine EMPTY changed or has uncheckpointed WAL')
    original_json=json.dumps(native,ensure_ascii=False,sort_keys=True)
    with (ROOT/'.reference/opengauss-native-lake/.driver.lock').open('a+') as mutex:
        try:fcntl.flock(mutex,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError as exc:raise Blocked('Reference driver busy') from exc
        transported=output/'native-publication.ducklake'
        transport_native(ref,transported,native)
        transport_hash=sha256(transported)
        relation='lake.main.'+qi(t.name)
        expected_logical=[{'logical_rowid':i,'begin_snapshot':1,**row} for i,row in enumerate(t.allocated_rows)]
        t.check('fixed reference reads actual native UUID carrier',ref.logical(transported,relation,'native-read'),expected_logical)
        t.check('readonly ATTACH preserves native transport bytes',sha256(transported),transport_hash)
        # Independent legal CREATE/DML oracle; UUID/time/carrier-name differences
        # remain recorded, are normalized only in the comparison object.
        replay=output/'independent-reference.ducklake';shutil.copy2(source,replay)
        for number,statements in enumerate(groups,1):
            ref.run(ref.attach(replay)+'BEGIN;\n'+';\n'.join(statements)+';\nCOMMIT;',
                    'independent-commit-'+str(number))
        independent={'tables':export_actual_tables(ref,replay)}
        write_json(output/'independent-reference-export.json',independent)
        observed=canonical_independent(native);wanted=canonical_independent(independent)
        write_json(output/'comparison-native.json',observed);write_json(output/'comparison-independent.json',wanted)
        t.check('full independent standard/catalog/carrier comparison',observed,wanted)
        continued=output/'reference-continued.ducklake';shutil.copy2(transported,continued)
        by_name={x['table_name']:x for x in native['tables']}
        entries=by_name['ducklake_inlined_data_tables']['rows']
        t.check('one UUID carrier registered before continuation',len(entries),1)
        carrier=entries[0]['table_name'];table_id=entries[0]['table_id']
        before_ids={r['id']:i for i,r in enumerate(t.allocated_rows)}
        # All rows are tiny and kept inline by actual reference ATTACH option.
        ref.run(ref.attach(continued)+'INSERT INTO '+relation+' VALUES(9001,90),(9002,NULL);','continued-insert')
        after_insert=ref.logical(continued,relation,'after-insert')
        wanted_business=sorted(expected+[{'id':9001,'val':90},{'id':9002,'val':None}],key=lambda r:r['id'])
        t.check('reference INSERT into UUID carrier returns all business rows',
                sorted([{k:r[k] for k in ('id','val')} for r in after_insert],key=lambda r:r['id']),wanted_business)
        ids={r['id']:r['logical_rowid'] for r in after_insert}
        t.check('reference allocates following native frontiers',(ids[9001],ids[9002]),(len(expected),len(expected)+1))
        t.check('reference INSERT preserves existing native row identities',{k:ids[k] for k in before_ids},before_ids)
        ref.run(ref.attach(continued)+'UPDATE '+relation+' SET val=901 WHERE id=9001; DELETE FROM '+relation+' WHERE id=9002;','continued-update-delete')
        final_rows=ref.logical(continued,relation,'after-update-delete')
        t.check('reference UPDATE preserves continuation identity',[(r['logical_rowid'],r['val']) for r in final_rows if r['id']==9001],[(ids[9001],901)])
        t.check('reference DELETE hides only the selected row',sorted(r['id'] for r in final_rows),sorted(list(before_ids)+[9001]))
        registration=ref.run('SELECT table_id,table_name,schema_version FROM ducklake_inlined_data_tables ORDER BY table_id','carrier-registration-after-dml',continued,True,True)
        t.check('fixed reference continues using actual UUID carrier registration',registration,entries)
        tables=ref.run('SELECT count(*) AS n FROM duckdb_tables() WHERE database_name=current_database() AND table_name='+"'"+carrier.replace("'","''")+"'",'carrier-still-physical',continued,True,True)
        t.check('physical UUID carrier remains present during reference DML',tables,[{'n':1}])
        t.check('continuation created no Parquet files',ref.run('SELECT count(*) AS n FROM ducklake_data_file','no-files',continued,True,True),[{'n':0}])
        # DROP is logical cleanup only; physical retirement/flush is a separate
        # future case until its real reference maintenance contract is fixed.
        ref.run(ref.attach(continued)+'DROP TABLE '+relation+';','reference-logical-drop')
        t.check('reference DROP retires the table at a committed snapshot',ref.run(
            'SELECT count(*) AS n FROM ducklake_table WHERE table_id='+str(table_id)+' AND end_snapshot IS NOT NULL',
            'reference-drop-metadata',continued,True,True),[{'n':1}])
        t.check('original native publication transport remains unchanged',sha256(transported),transport_hash)
        t.check('source EMPTY immutable after all interop work',sha256(source),source_hash)
        t.check('source lock remained frozen',sha256(lock_path),lock_hash)
        t.check('actual native export values never rewritten',json.dumps(native,ensure_ascii=False,sort_keys=True),original_json)
    t.result.setdefault('reference_checkpoints',[]).append({'directory':str(output),'native_transport_sha256':transport_hash,
        'normalizations_for_independent_comparison_only':['schema/table UUID','snapshot_time','created_by','registry-described carrier spelling'],
        'not_covered':['physical carrier reclamation/flush','live external connector','reference-to-native reimport']})
