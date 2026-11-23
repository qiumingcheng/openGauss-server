#!/usr/bin/env python3
"""Real prepared Parquet + later DML, with exact history/position verification."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
import traceback
from types import SimpleNamespace
ROOT=Path('/home/qiumc/data/code/cpp');D=ROOT/'integration/opengauss-native-lake'
sys.path[:0]=[str(D),str(D/'tests')]
from file_dml_acceptance import check_build
from inline_fixture import query,scalar
from lake_harness import sha256,validate_run_id,write_json
from native_auto_flush import connect_control
from native_runtime import PROFILE
from native_test_context import OwnedDatabase
from native_table_create import Case
from permission_api import OwnedRoles,cleanup_roles
from empty_catalog_oracle import read_native_catalog,transport_native
from reverse_reference import Reference,literal
MODES=['update','delete','delete_all','double_update','history_update','mixed_sources','cancel']


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--run-id',required=True)
    p.add_argument('--build-receipt',type=Path,required=True);p.add_argument('--mode',choices=MODES,required=True)
    a=p.parse_args();validate_run_id(a.run_id)
    out=ROOT/'公共文档/openGauss原生湖存储/验证'/a.run_id;out.mkdir(exist_ok=False)
    r={'status':'fail','scope':'Real prepared flush and late '+a.mode,'assertions':[],
       'limitations':['Native closed SQL0.11 catalog; profile-specific correctness only. No external writer, scale/soak, physical GC, remote SDK or target RAC qualification.']}
    inputs=[Path(__file__),D/'tests/native_table_create.py',D/'native_test_context.py',D/'tests/permission_api.py',
            D/'tests/empty_catalog_oracle.py',D/'tests/reverse_reference.py',D/'source-lock.json']
    inputs += [D / 'native_runtime.py', D / 'build_driver.py', D / 'tests/inline_cleanup.py']
    r['inputs']={str(x):sha256(x)for x in inputs};archive=out/'executed-inputs';archive.mkdir()
    for x in inputs:shutil.copy2(x,archive/x.name)
    db=roles=control=t=holder=pool=future=None
    def check(name,got,want=True):
        ok=got==want;r['assertions'].append({'name':name,'observed':got,'expected':want,'status':'pass'if ok else'fail'})
        if not ok:raise AssertionError(name)
    try:
        import psycopg2
        from psycopg2 import sql
        built=check_build(a.build_receipt);r.update(source_head=built['source_head'],binary_sha256=built['binary_sha256'])
        control=connect_control();check('automatic scheduler disabled for exact ordering',scalar(control,'SHOW enable_lake_maintenance'),'off')
        with (out/'fixture.log').open('w')as log:
            subprocess.run([sys.executable,str(D/'tests/catalog_fixture_bundle.py'),'--run-id',a.run_id,'--output',str(out/'fixture/manifest.json')],stdout=log,stderr=subprocess.STDOUT,check=True,timeout=300)
        db=OwnedDatabase(a.run_id,out).create();roles=OwnedRoles(control,a.run_id,out)
        t=Case(db,SimpleNamespace(mode='net_dml',sql_version='0.11',manifest=out/'fixture/manifest.json',output=out/'result.json',run_id=a.run_id),r,roles)
        t.create();query(t.a,'CREATE TABLE biz.reference(id integer PRIMARY KEY,val integer)')
        query(t.a,'BEGIN');query(t.a,'INSERT INTO biz.orders VALUES(1,10),(2,20),(3,30)');query(t.a,'INSERT INTO biz.reference VALUES(1,10),(2,20),(3,30)');query(t.a,'COMMIT')
        original=[(1,10),(2,20),(3,30)]
        def head():return scalar(t.observer,'SELECT max(snapshot_id) FROM created_metadata.ducklake_snapshot')
        history=[(head(),original)]
        old=db.connect();query(old,'BEGIN ISOLATION LEVEL READ COMMITTED');check('retained RC lake view starts with original data',t.rows(conn=old),original)
        original_file=None
        if a.mode=='mixed_sources':
            query(t.a,"SELECT og_lake.flush('biz.orders'::regclass)");history.append((head(),original))
            original_file=scalar(t.observer,'SELECT data_file_id FROM created_metadata.ducklake_data_file')
        if a.mode in ['history_update','mixed_sources']:
            query(t.b,'BEGIN');query(t.b,'UPDATE biz.orders SET val=11 WHERE id=1');query(t.b,'UPDATE biz.reference SET val=11 WHERE id=1');query(t.b,'COMMIT')
            history.append((head(),[(1,11),(2,20),(3,30)]))
        carrier=scalar(t.observer,'SELECT table_name FROM created_metadata.ducklake_inlined_data_tables')
        raw=sql.SQL('SELECT _ducklake_row_id,_ducklake_begin_snapshot,_ducklake_end_snapshot,id,val FROM created_metadata.{} ORDER BY 1,2').format(sql.Identifier(carrier))
        before=query(t.observer,raw);cutoff=head()
        r['prepared_next_file_id']=scalar(t.observer,'SELECT next_file_id FROM created_metadata.ducklake_snapshot ORDER BY snapshot_id DESC LIMIT 1')
        root=Path(t.empty_manifest['data_path']);disk_before=sorted(str(x)for x in root.rglob('*.parquet'))
        query(t.a,'BEGIN');query(t.a,'INSERT INTO biz.heap_marker VALUES(77)');query(t.a,"SELECT og_lake.flush('biz.orders'::regclass)")
        check('real Parquet prepared before later user transaction',len(list(root.rglob('*.parquet')))>len(disk_before))
        query(t.b,'BEGIN')
        if a.mode in ['delete','delete_all']:
            if a.mode=='delete_all':
                query(t.b,'DELETE FROM biz.orders');query(t.b,'DELETE FROM biz.reference')
            else:
                query(t.b,'DELETE FROM biz.orders WHERE id=1');query(t.b,'DELETE FROM biz.reference WHERE id=1')
        else:
            value=12 if a.mode in ['history_update','mixed_sources']else 11
            query(t.b,'UPDATE biz.orders SET val=%s WHERE id=1',(value,));query(t.b,'UPDATE biz.reference SET val=%s WHERE id=1',(value,))
            if a.mode=='mixed_sources':
                query(t.b,'UPDATE biz.orders SET val=22 WHERE id=2');query(t.b,'UPDATE biz.reference SET val=22 WHERE id=2')
        query(t.b,'COMMIT');late_snapshot=head();history.append((late_snapshot,query(t.observer,'SELECT id,val FROM biz.reference ORDER BY id')))
        if a.mode=='double_update':
            query(t.b,'BEGIN');query(t.b,'UPDATE biz.orders SET val=12 WHERE id=1');query(t.b,'UPDATE biz.reference SET val=12 WHERE id=1');query(t.b,'COMMIT')
            history.append((head(),[(1,12),(2,20),(3,30)]))
        after_dml=query(t.observer,raw);protocol_after_dml=t.protocol();outcome='committed'
        first_file=scalar(t.observer,'SELECT next_file_id FROM created_metadata.ducklake_snapshot ORDER BY snapshot_id DESC LIMIT 1')
        r['publication_next_file_id']=first_file
        if a.mode=='cancel':
            holder=db.connect();query(holder,'BEGIN');query(holder,'LOCK TABLE created_metadata.ducklake_snapshot_changes IN SHARE MODE')
            pid=scalar(t.a,'SELECT pg_backend_pid()');pool=ThreadPoolExecutor(max_workers=1)
            def commit():
                try:query(t.a,'COMMIT');return 'committed'
                except psycopg2.Error as exc:return exc.pgcode
            future=pool.submit(commit);deadline=time.monotonic()+10;waits=[]
            while time.monotonic()<deadline:
                waits=query(t.observer,"SELECT pid FROM pg_locks WHERE relation='created_metadata.ducklake_snapshot_changes'::regclass AND mode='RowExclusiveLock' AND NOT granted AND pid=%s",(pid,))
                if waits or future.done():break
                time.sleep(.01)
            check('real publisher reached changeset after outputs and source transfer',len(waits),1)
            check('prepared transfer including late end is invisible before commit',t.protocol(),protocol_after_dml)
            check('cancel owned publisher at actual SPI barrier',scalar(t.observer,'SELECT pg_cancel_backend(%s)',(pid,)))
            outcome=future.result(timeout=10);query(t.a,'ROLLBACK');query(holder,'ROLLBACK');pool.shutdown(wait=True);pool=None
        else:
            try:query(t.a,'COMMIT')
            except psycopg2.Error as exc:outcome=exc.pgcode;r['publication_error']=str(exc);query(t.a,'ROLLBACK')
        r.update(publication_outcome=outcome,cutoff=cutoff,late_snapshot=late_snapshot,prepared_versions=before,post_dml_versions=after_dml,
                 actual_lake_rows=t.rows(),actual_heap_rows=query(t.observer,'SELECT id,val FROM biz.reference ORDER BY id'))
        check('late DML and prepared flush coexist atomically'if a.mode!='cancel'else'cancel returns native57014',outcome,'57014'if a.mode=='cancel'else'committed')
        check('all current values equal independent transactional heap',r['actual_lake_rows'],r['actual_heap_rows'])
        check('retained old RC lake view survives the transfer',t.rows(conn=old),original);query(old,'COMMIT')
        check('flush and cancellation never consume new logical row identities',scalar(t.observer,'SELECT next_row_id FROM created_metadata.ducklake_table_stats'),3)
        check('heap marker follows same transaction outcome',query(t.observer,'SELECT * FROM biz.heap_marker'),[]if a.mode=='cancel'else[(77,)])
        if a.mode=='cancel':
            check('cancel rolls all standard catalog and carrier state back',t.protocol(),protocol_after_dml)
            check('cancel removes every finished unpublished candidate',sorted(str(x)for x in root.rglob('*.parquet')),disk_before)
            check('cancel retains committed later inline versions',query(t.observer,raw),after_dml)
        else:
            moved={(x[0],x[1])for x in before}
            check('only selected physical versions leave inline',query(t.observer,raw),[x for x in after_dml if(x[0],x[1])not in moved])
            tid=scalar(t.observer,'SELECT table_id FROM created_metadata.ducklake_table')
            marks=sql.SQL('SELECT file_id,row_id,begin_snapshot FROM created_metadata.{} ORDER BY 1,2,3').format(sql.Identifier('ducklake_inlined_delete_'+str(tid)))
            actual_marks=query(t.observer,marks);r['actual_position_marks']=actual_marks
            position=1 if a.mode=='history_update'else 0
            expected_marks=[(first_file,i,late_snapshot)for i in range(3)]if a.mode=='delete_all'else[(first_file,position,late_snapshot)]
            if a.mode=='mixed_sources':expected_marks.append((original_file,1,late_snapshot));expected_marks.sort()
            check('late ends follow exact prepared physical positions and original delete snapshots',actual_marks,expected_marks)
            names=query(t.observer,"SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='created_metadata' AND c.relkind='r' ORDER BY c.relname")
            native=read_native_catalog(t.observer,'created_metadata',{'tables':[{'table_name':x}for x, in names]},out/'native-catalog.json')
            lock=json.loads((D/'source-lock.json').read_text());receipt=json.loads((ROOT/'.reference/opengauss-native-lake/build/receipt.json').read_text());check('pinned independent reference lock',receipt['source_lock_hash'],sha256(D/'source-lock.json'))
            ref=Reference(out,lock,receipt);transport=out/'transport.ducklake';transport_native(ref,transport,native)
            found=ref.run(ref.attach(transport,True)+'SELECT id,val FROM lake.main.orders ORDER BY id','current',result=True)
            check('pinned DuckLake reads all final values exactly once',[tuple(x.values())for x in found],r['actual_heap_rows'])
            for snapshot,expected in history:
                found=ref.run(ref.attach(transport,True)+'SELECT id,val FROM lake.main.orders AT(VERSION => '+str(snapshot)+') ORDER BY id','history-'+str(snapshot),result=True)
                check('independent historical values at snapshot'+str(snapshot),[tuple(x.values())for x in found],expected)
            file_row=query(t.observer,'SELECT path,record_count FROM created_metadata.ducklake_data_file WHERE data_file_id=%s',(first_file,))[0]
            matches=list(root.rglob(file_row[0]));check('prepared data output path unique',len(matches),1)
            physical=ref.run('SELECT file_row_number,_ducklake_internal_row_id,_ducklake_internal_snapshot_id,id,val FROM read_parquet('+literal(str(matches[0]))+',file_row_number=true) ORDER BY file_row_number','physical',result=True)
            check('immutable prepared file keeps all original payloads IDs and birth snapshots',[tuple(x.values())for x in physical],[(i,x[0],x[1],x[3],x[4])for i,x in enumerate(before)])
            check('registered physical file rows exact',file_row[1],len(before))
        check('all executed input bytes unchanged',all(sha256(f)==h for f,h in r['inputs'].items()));check_build(a.build_receipt);r['status']='pass'
    except BaseException as exc:r.update(reason=str(exc),traceback=traceback.format_exc())
    finally:
        if future is not None and not future.done()and t is not None:
            try:scalar(t.observer,'SELECT pg_cancel_backend(%s)',(pid,))
            except BaseException:pass
        if holder is not None:
            try:query(holder,'ROLLBACK')
            except BaseException:pass
        if pool is not None:pool.shutdown(wait=True)
        if db is not None:
            try:db.cleanup();r['database_cleanup']=db.cleanup_result
            except BaseException as exc:r.update(status='fail',cleanup_error=str(exc))
        if roles is not None:
            try:
                if not r.get('database_cleanup')or r['database_cleanup']['returncode']:raise RuntimeError('Owned database cleanup unproven; role retained')
                r['role_cleanup']=cleanup_roles(control,roles.path,a.run_id);r=roles.redact(r)
            except BaseException as exc:r.update(status='fail',role_cleanup_error=str(exc))
        if control is not None:control.close()
        write_json(out/'result.json',r)
    print(json.dumps({'status':r['status'],'checks':len(r['assertions']),'mode':a.mode,'outcome':r.get('publication_outcome'),'reason':r.get('reason'),'result':str(out/'result.json')},ensure_ascii=False))
    return 0 if r['status']=='pass'else 1

if __name__=='__main__':raise SystemExit(main())
