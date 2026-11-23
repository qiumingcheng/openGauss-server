"""Exact offline transport of actual native file-DML catalog publications.

This never repairs snapshot/row/file identities. Source Parquet bytes remain
immutable and are read independently by the pinned DuckLake implementation.
"""
import fcntl,json
from pathlib import Path
from inline_fixture import query,scalar
from empty_catalog_oracle import read_native_catalog,canonical_shape,transport_native
from reverse_reference import Reference
from lake_harness import sha256

def check_file_catalog(t,directory,expected_ids,continuation=False):
    directory=Path(directory);directory.mkdir(exist_ok=False)
    root=Path('/home/qiumc/data/code/cpp');driver=root/'integration/opengauss-native-lake'
    lock_path=driver/'source-lock.json';lock=json.loads(lock_path.read_text());receipt=json.loads((root/'.reference/opengauss-native-lake/build/receipt.json').read_text())
    if receipt['source_lock_hash']!=sha256(lock_path):raise RuntimeError('reference build receipt is stale')
    mutex=(root/'.reference/opengauss-native-lake/.driver.lock').open('a+')
    fcntl.flock(mutex,fcntl.LOCK_EX|fcntl.LOCK_NB)
    try:
        names=[row[0]for row in query(t.observer,"SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='lake_metadata' AND c.relkind='r' ORDER BY c.relname")]
        native=read_native_catalog(t.observer,'lake_metadata',{'tables':[{'table_name':name}for name in names]},directory/'native-catalog.json')
        by_name={table['table_name']:table for table in native['tables']}
        for before in t.exported['tables']:
            t.check('reference physical shape retained: '+before['table_name'],canonical_shape(by_name[before['table_name']]),canonical_shape(before))
        registered=[row['table_name']for row in by_name['ducklake_inlined_data_tables']['rows']]
        standard={table['table_name']for table in t.exported['tables']}
        delete_names={'ducklake_inlined_delete_'+str(row['table_id'])for row in by_name['ducklake_table']['rows']}
        allowed=standard|set(registered)|delete_names
        t.check('only standard observed carrier/position names added',set(by_name)<=allowed,True)
        for name in set(by_name)-standard:
            columns=[(c['column_name'],c['data_type'])for c in by_name[name]['columns']]
            wanted=[('file_id','BIGINT'),('row_id','BIGINT'),('begin_snapshot','BIGINT')]if name in delete_names else [('_ducklake_row_id','BIGINT'),('_ducklake_begin_snapshot','BIGINT'),('_ducklake_end_snapshot','BIGINT')]+[(c['name'],'VARCHAR'if c['type']=='VARCHAR'else c['type'])for c in t.manifest['schema']]
            t.check('new carrier has exact standard physical columns: '+name,columns,wanted)
        ref=Reference(directory,lock,receipt);catalog=directory/'transport.ducklake';transport_native(ref,catalog,native)
        before=sha256(catalog);table='lake.'+'"'+t.manifest['schema_name']+'".'+'"'+t.manifest['table_name']+'"'if 'schema_name'in t.manifest else 'lake.biz.orders'
        rows=ref.logical(catalog,table,'read-exact-native-file-publication')
        actual=sorted((r['id'],r['val'])for r in rows)
        t.check('independent DuckLake reads exact native file-DML rows',actual,t.rows(t.observer))
        t.check('independent DuckLake preserves actual logical identity',sorted((r['id'],r['logical_rowid'])for r in rows),sorted(expected_ids.items()))
        if t.args.mode=='stats_unknown':
            filtered=ref.run(ref.attach(catalog,True)+'SELECT id,val FROM '+table+' WHERE val=0 ORDER BY id','read-surviving-file-value-with-predicate',result=True)
            t.check('independent predicate cannot prune surviving predecessor file value',[(r['id'],r['val'])for r in filtered],[(1,0)])
        t.check('readonly interread retains all native catalog values',sha256(catalog),before)
        t.check('source-lock unchanged during interread',sha256(lock_path),receipt['source_lock_hash'])
        continued=None
        if continuation:
            next_id=t.next_id()
            ref.run(ref.attach(catalog)+'UPDATE '+table+' SET val=val+1000 WHERE id=2; INSERT INTO '+table+' VALUES(90,900);','normal-reference-continuation')
            continued=ref.logical(catalog,table,'read-reference-continuation')
            wanted=dict(t.rows(t.observer));wanted[2]+=1000;wanted[90]=900
            wanted_ids=dict(expected_ids);wanted_ids[90]=next_id
            t.check('normal DuckLake writer continues native file/inline publication',sorted((r['id'],r['val'])for r in continued),sorted(wanted.items()))
            t.check('continued reference preserves old and newly allocated identities',sorted((r['id'],r['logical_rowid'])for r in continued),sorted(wanted_ids.items()))
        return {'scope':'exact offline committed native transport; not live shared-catalog/RAC interoperability','catalog':str(catalog),'visible_rows':rows,'continued_rows':continued,'native_export_sha256':sha256(directory/'native-catalog.json')}
    finally:fcntl.flock(mutex,fcntl.LOCK_UN);mutex.close()
