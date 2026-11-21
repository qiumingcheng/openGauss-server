#!/usr/bin/env python3
"""Actual scalar-file execution, complete fallback and heap-result differential.

This is Debug correctness only, not Ustore/DiskANN/fusion, remote/RAC,
performance, physical GC or commercial qualification.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import traceback
from types import SimpleNamespace

ROOT=Path('/home/qiumc/data/code/cpp');D=ROOT/'integration/opengauss-native-lake'
sys.path[:0]=[str(D),str(D/'tests')]
from file_dml_acceptance import check_build
from inline_fixture import query,scalar
from lake_harness import sha256,validate_run_id,write_json
from native_auto_flush import connect_control
from native_test_context import OwnedDatabase
from native_table_create import Case
from permission_api import OwnedRoles,cleanup_roles

def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--run-id',required=True)
    p.add_argument('--build-receipt',type=Path,required=True)
    p.add_argument('--mode',choices=['predicates','visibility','fallback','lifetime','rename'],required=True)
    a=p.parse_args();validate_run_id(a.run_id)
    out=ROOT/'公共文档/openGauss原生湖存储/验证'/a.run_id;out.mkdir(exist_ok=False)
    r={'status':'fail','scope':__doc__+' '+a.mode,'assertions':[]}
    files=[Path(__file__),D/'tests/native_table_create.py',D/'tests/permission_api.py',D/'native_test_context.py',
        D/'native_runtime.py',D/'tests/inline_cleanup.py',D/'tests/file_dml_acceptance.py',D/'inline_fixture.py',
        D/'tests/native_auto_flush.py',D/'source-lock.json']
    r['inputs']={str(f):sha256(f)for f in files};archive=out/'executed-inputs';archive.mkdir()
    for f in files:shutil.copy2(f,archive/f.name)
    control=db=roles=t=None
    def image(value):
        return {'rows':len(value),'sha256':hashlib.sha256(repr(value).encode()).hexdigest()}if isinstance(value,list)and len(value)>30 else value
    def check(name,got,want=True):
        good=got==want;r['assertions'].append(dict(name=name,observed=image(got),expected=image(want),status='pass'if good else'fail'))
        if not good:raise AssertionError(name)
    try:
        built=check_build(a.build_receipt);r.update(source_head=built['source_head'],binary_sha256=built['binary_sha256'])
        control=connect_control();check('automatic scheduler remains off',scalar(control,'SHOW enable_lake_maintenance'),'off')
        with(out/'fixture.log').open('w')as log:
            subprocess.run([sys.executable,str(D/'tests/catalog_fixture_bundle.py'),'--run-id',a.run_id,'--output',str(out/'fixture/manifest.json')],
                stdout=log,stderr=subprocess.STDOUT,check=True,timeout=300)
        db=OwnedDatabase(a.run_id,out).create();roles=OwnedRoles(control,a.run_id,out)
        t=Case(db,SimpleNamespace(mode='net_dml',sql_version='0.16',manifest=out/'fixture/manifest.json',output=out/'result.json',run_id=a.run_id),r,roles)
        t.create();query(t.a,'CREATE TABLE biz.reference(id integer,val integer)')
        query(t.a,"SELECT og_lake.set_option('biz.orders'::regclass,'data_inlining_row_limit','50000')")
        def both(statement):
            query(t.a,'BEGIN')
            for table in ['biz.orders','biz.reference']:query(t.a,statement.format(table=table))
            query(t.a,'COMMIT')
        both('INSERT INTO {table} SELECT CASE WHEN i%1001=0 THEN NULL ELSE i END,i FROM generate_series(1,12000)i')
        both('INSERT INTO {table} VALUES(7,70007),(7,70008)')
        both('UPDATE {table} SET val=val+20000 WHERE id=7')
        both('UPDATE {table} SET val=val+20000 WHERE id=7')
        both('DELETE FROM {table} WHERE id=11')
        query(t.a,'ANALYZE biz.orders');query(t.a,"SELECT og_lake.flush('biz.orders'::regclass)")
        store=out/'index-store';store.mkdir()
        query(t.a,"SELECT og_lake.set_option('created_lake'::name,'index_root',%s::text)",(str(store),))
        uuid=str(scalar(t.a,"SELECT og_lake.create_lake_index('biz.orders'::regclass,'ix_id',%s::jsonb)",
            (json.dumps(dict(version=1,kind='scalar',method='btree',column='id')),)))
        def build():query(t.a,'SELECT og_lake.build_lake_index(%s::uuid)',(uuid,))
        build();head=t.head();protocol=t.protocol()
        def equal(predicate,args=None,conn=t.a,label=None):
            statement='SELECT id,val FROM {table} WHERE '+predicate+' ORDER BY id NULLS LAST,val'
            got=query(conn,statement.format(table='biz.orders'),args);want=query(conn,statement.format(table='biz.reference'),args)
            check(label or 'complete heap differential '+predicate,got,want);return got
        def actual(predicate='id=7',conn=t.a,table='biz.orders'):
            text='\n'.join(x[0]for x in query(conn,'EXPLAIN (ANALYZE TRUE,COSTS FALSE) SELECT id,val FROM '+table+' WHERE '+predicate))
            r.setdefault('actual_plans',[]).append(text)
            check('real executor uses verified file candidates','Actual File Access: verified sorted Parquet candidates'in text)
            check('candidate coverage is actually nonzero','Verified Scalar File Coverage: 0'not in text)
            return text
        actual();equal('id=7')
        if a.mode=='predicates':
            for predicate in ['id<4','id<=4','id>11997','id>=11997','7=id','id IS NULL','id IS NOT NULL',
                "id>'2147483647'::bigint","id<'-2147483649'::bigint",'id=7::smallint',
                'id=ANY(ARRAY[7,1,7,NULL,12000]::bigint[])','id=ANY(ARRAY[]::integer[])',
                'id=ANY(NULL::integer[])','id=ANY(ARRAY[NULL]::integer[])','id<>7','val=1 OR id=7']:
                equal(predicate)
            query(t.a,'PREPARE scalar_file(bigint) AS SELECT id,val FROM biz.orders WHERE id=$1 ORDER BY val')
            for value in [7,12000,1,7,None,2147483648,-2147483649,7,2,7]:
                check('generic cross-integer parameter '+str(value),query(t.a,'EXECUTE scalar_file(%s)',(str(value)if value is not None else None,)),
                    query(t.a,'SELECT id,val FROM biz.reference WHERE id=%s::bigint ORDER BY val',(str(value)if value is not None else None,)))
            query(t.a,'DEALLOCATE scalar_file')
            query(t.a,'CREATE TEMP TABLE keys(id integer)');query(t.a,'INSERT INTO keys VALUES(1),(7),(12),(NULL)')
            statement='SELECT k.id,(SELECT min(o.val) FROM {table} o WHERE o.id=k.id) FROM keys k ORDER BY k.id NULLS LAST'
            check('related scalar parameter rescans',query(t.a,statement.format(table='biz.orders')),query(t.a,statement.format(table='biz.reference')))
            query(t.a,'SET enable_indexscan=off');equal('id=7');query(t.a,'RESET enable_indexscan');actual()
        elif a.mode=='visibility':
            both('INSERT INTO {table} SELECT i,i FROM generate_series(13001,13100)i')
            query(t.a,"SELECT og_lake.flush('biz.orders'::regclass)")
            text=actual();check('new uncovered files are explicitly retained','Complete File Fallback Inputs: 1'in text)
            equal('id=13099');equal('id=7');equal('id IS NULL')
            query(t.a,'BEGIN')
            for table in ['biz.orders','biz.reference']:query(t.a,'UPDATE '+table+' SET id=19001,val=val+1 WHERE id=7')
            equal('id=7');equal('id=19001');query(t.a,'SAVEPOINT changed')
            for table in ['biz.orders','biz.reference']:query(t.a,'UPDATE '+table+' SET val=val+1 WHERE id=19001')
            equal('id=19001');query(t.a,'ROLLBACK TO changed');query(t.a,'RELEASE SAVEPOINT changed');equal('id=19001')
            query(t.a,'ROLLBACK');equal('id=7');equal('id=19001');build();actual();equal('id=13099')
        elif a.mode=='fallback':
            directory=Path(scalar(t.a,"SELECT artifact_root_uri FROM og_lake_internal.index_segment WHERE index_uuid=%s::uuid AND state='READY' ORDER BY segment_uuid LIMIT 1",(uuid,)))
            lookup=directory/'page_lookup.parquet';preserved=directory/'page_lookup.parquet.owned-preserved'
            lookup.rename(preserved)
            try:
                equal('id=7');text='\n'.join(x[0]for x in query(t.a,'EXPLAIN (ANALYZE TRUE,COSTS FALSE) SELECT * FROM biz.orders WHERE id=7'))
                check('missing index artifact falls back before output','Actual File Access: complete SDK scan'in text)
            finally:preserved.rename(lookup)
            actual();data=directory/'page_data.parquet';saved=data.read_bytes();data.write_bytes(saved[:-16])
            try:equal('id=7');equal('id IS NULL')
            finally:data.write_bytes(saved)
            actual();source=Path(scalar(t.a,'SELECT source_path FROM og_lake_internal.index_coverage WHERE index_uuid=%s::uuid AND active ORDER BY source_file_id LIMIT 1',(uuid,)))
            st=source.stat();fd=os.open(source,os.O_RDWR)
            try:
                byte=os.pread(fd,1,4);os.pwrite(fd,bytes([byte[0]^1]),4);os.fsync(fd);os.pwrite(fd,byte,4);os.fsync(fd)
                os.utime(fd,ns=(st.st_atime_ns,st.st_mtime_ns))
            finally:os.close(fd)
            equal('id=7');text='\n'.join(x[0]for x in query(t.a,'EXPLAIN (ANALYZE TRUE,COSTS FALSE) SELECT * FROM biz.orders WHERE id=7'))
            check('changed source object identity disables stale coverage','Actual File Access: complete SDK scan'in text)
            build();actual();equal('id=7')
        elif a.mode=='rename':
            statement='SELECT segment_uuid::text,state,artifact_root_uri,manifest_hash FROM og_lake_internal.index_segment WHERE index_uuid=%s::uuid ORDER BY segment_uuid'
            segments=query(t.a,statement,(uuid,));artifacts={str(f):sha256(f)for f in store.rglob('*.parquet')}
            rowids=query(t.a,'SELECT table_id,next_row_id FROM created_metadata.ducklake_table_stats ORDER BY table_id')
            query(t.a,'BEGIN');query(t.a,'SAVEPOINT label');query(t.a,'ALTER FOREIGN TABLE biz.orders RENAME TO temporary_name')
            query(t.a,'ROLLBACK TO label');query(t.a,'RELEASE SAVEPOINT label');query(t.a,'COMMIT')
            actual();equal('id=7')
            for source,target in [('orders','renamed_orders'),('renamed_orders','orders')]:
                query(t.a,'ALTER FOREIGN TABLE biz.'+source+' RENAME TO '+target)
                actual(table='biz.'+target)
                check('renamed table keeps every indexed value '+target,
                    query(t.a,'SELECT id,val FROM biz.'+target+' WHERE id=7 ORDER BY val'),
                    query(t.a,'SELECT id,val FROM biz.reference WHERE id=7 ORDER BY val'))
                build();check('rename and verified rebuild reuse immutable segments '+target,query(t.a,statement,(uuid,)),segments)
            check('rename never rewrites derived artifact bytes',{str(f):sha256(f)for f in store.rglob('*.parquet')},artifacts)
            check('rename never reallocates logical row IDs',query(t.a,'SELECT table_id,next_row_id FROM created_metadata.ducklake_table_stats ORDER BY table_id'),rowids)
        else:
            pid=int((ROOT/'.runtime/opengauss-native-lake/debug/data/postmaster.pid').read_text().splitlines()[0])
            data_root=str(Path(t.empty_manifest['data_path']))
            def leaked():
                found=[]
                for fd in (Path('/proc')/str(pid)/'fd').iterdir():
                    try:target=os.readlink(fd)
                    except FileNotFoundError:continue
                    if target.startswith((str(store),data_root))and '.parquet'in target:found.append(target)
                return sorted(found)
            for iteration in range(12):
                equal('id=7')
                query(t.a,'BEGIN');query(t.a,'DECLARE held NO SCROLL CURSOR FOR SELECT id,val FROM biz.orders WHERE id=7')
                query(t.a,'FETCH 1 FROM held');query(t.a,'CLOSE held');query(t.a,'COMMIT')
                check('early cursor close releases all owned Parquet descriptors '+str(iteration),leaked(),[])
        if a.mode in ['predicates','fallback','lifetime']:
            check('read path and derived rebuild allocate no business snapshot or row IDs',t.head(),head)
            check('complete standard catalog and business carriers remain untouched',t.protocol(),protocol)
        check('all query cases still match the complete heap',query(t.a,'SELECT id,val FROM biz.orders ORDER BY id NULLS LAST,val'),query(t.a,'SELECT id,val FROM biz.reference ORDER BY id NULLS LAST,val'))
        check('all executed inputs unchanged',all(sha256(f)==h for f,h in r['inputs'].items()));check_build(a.build_receipt);r['status']='pass'
    except BaseException as exc:r.update(reason=str(exc),traceback=traceback.format_exc())
    finally:
        if db is not None:
            try:db.cleanup();r['database_cleanup']=db.cleanup_result
            except BaseException as exc:r.update(status='fail',cleanup_error=str(exc))
        if roles is not None:
            try:
                if not r.get('database_cleanup')or r['database_cleanup']['returncode']:raise RuntimeError('Owned DB cleanup unproven; roles retained')
                r['role_cleanup']=cleanup_roles(control,roles.path,a.run_id);r=roles.redact(r)
            except BaseException as exc:r.update(status='fail',role_cleanup_error=str(exc))
        if control is not None:control.close()
        write_json(out/'result.json',r)
    print(json.dumps(dict(status=r['status'],checks=len(r['assertions']),reason=r.get('reason'),result=str(out/'result.json')),ensure_ascii=False))
    return 0 if r['status']=='pass'else 1

if __name__=='__main__':raise SystemExit(main())
