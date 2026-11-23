#!/usr/bin/env python3
"""Pending index definition must coexist with real flush and complete scan fallback."""
import argparse
import json
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
from empty_catalog_oracle import read_native_catalog,transport_native
from reverse_reference import Reference


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--run-id',required=True)
    p.add_argument('--build-receipt',type=Path,required=True);a=p.parse_args();validate_run_id(a.run_id)
    out=ROOT/'公共文档/openGauss原生湖存储/验证'/a.run_id;out.mkdir(exist_ok=False)
    r={'status':'fail','scope':'SQL0.14 pending index plus real prepared flush and late UPDATE/INSERT',
       'assertions':[],'limitations':['Definition registry only; no index backend or indexed query qualification.']}
    files=[Path(__file__),D/'tests/native_table_create.py',D/'tests/permission_api.py',D/'native_test_context.py',
           D/'native_runtime.py',D/'tests/inline_cleanup.py',D/'tests/file_dml_acceptance.py',D/'inline_fixture.py',
           D/'tests/native_auto_flush.py',D/'tests/empty_catalog_oracle.py',D/'tests/reverse_reference.py',D/'source-lock.json']
    r['inputs']={str(f):sha256(f)for f in files};archive=out/'executed-inputs';archive.mkdir()
    for f in files:shutil.copy2(f,archive/f.name)
    control=db=roles=t=None
    def check(name,got,want=True):
        ok=got==want;r['assertions'].append({'name':name,'observed':got,'expected':want,'status':'pass'if ok else'fail'})
        if not ok:raise AssertionError(name)
    try:
        built=check_build(a.build_receipt);r.update(source_head=built['source_head'],binary_sha256=built['binary_sha256'])
        control=connect_control();check('automatic scheduler remains disabled',scalar(control,'SHOW enable_lake_maintenance'),'off')
        with (out/'fixture.log').open('w')as log:
            subprocess.run([sys.executable,str(D/'tests/catalog_fixture_bundle.py'),'--run-id',a.run_id,'--output',str(out/'fixture/manifest.json')],
                           stdout=log,stderr=subprocess.STDOUT,check=True,timeout=300)
        db=OwnedDatabase(a.run_id,out).create();roles=OwnedRoles(control,a.run_id,out)
        t=Case(db,SimpleNamespace(mode='net_dml',sql_version='0.14',manifest=out/'fixture/manifest.json',output=out/'result.json',run_id=a.run_id),r,roles)
        t.create();query(t.a,'INSERT INTO biz.orders VALUES(1,10),(2,20),(3,30)')
        original=[(1,10),(2,20),(3,30)];start=t.head()[0]
        uuid=str(scalar(t.a,'SELECT og_lake.create_lake_index(\'biz.orders\'::regclass,\'ix_id\'::name,%s::jsonb)',
                        (json.dumps({'version':1,'kind':'scalar','method':'btree','column':'id'}),)))
        definition=query(t.observer,'SELECT index_uuid::text,table_uuid::text,column_id,definition_hash,definition::text FROM og_lake_internal.index_definition')
        def status():return json.loads(scalar(t.a,'SELECT og_lake.lake_index_status(%s::uuid)',(uuid,)))
        def prepared(conn=t.a):return query(conn,'EXECUTE exact_id(1)')
        query(t.a,'PREPARE exact_id(integer) AS SELECT id,val FROM biz.orders WHERE id=$1 ORDER BY val')
        check('prepared predicate uses complete native scan',prepared(),[(1,10)])
        old=db.connect();query(old,'BEGIN ISOLATION LEVEL READ COMMITTED');check('old RC read fixes legal lake version',t.rows(conn=old),original)
        root=Path(t.empty_manifest['data_path']);before=sorted(str(f)for f in root.rglob('*.parquet'))
        query(t.a,'BEGIN');query(t.a,"SELECT og_lake.flush('biz.orders'::regclass)")
        check('real immutable Parquet is prepared before late DML',len(list(root.rglob('*.parquet')))>len(before))
        query(t.b,'BEGIN');query(t.b,'UPDATE biz.orders SET val=11 WHERE id=1');query(t.b,'INSERT INTO biz.orders VALUES(4,40)');query(t.b,'COMMIT')
        late=t.head()[0];expected=[(1,11),(2,20),(3,30),(4,40)]
        check('late UPDATE and INSERT are visible through complete scan',t.rows(conn=t.b),expected)
        query(t.a,'COMMIT');flushed=t.head()[0]
        check('flush publishes after the independent later DML',flushed>late)
        check('prepared query does not miss late changed key',prepared(),[(1,11)])
        check('mixed file/inline full result has no missing or stale versions',t.rows(),expected)
        check('old reader survives prepared transfer',t.rows(conn=old),original);query(old,'COMMIT')
        check('logical row IDs allocated only for four inserts',scalar(t.observer,'SELECT next_row_id FROM created_metadata.ducklake_table_stats'),4)
        check('definition identity survives physical representation change',query(t.observer,'SELECT index_uuid::text,table_uuid::text,column_id,definition_hash,definition::text FROM og_lake_internal.index_definition'),definition)
        check('pending index never claims file coverage',(scalar(t.observer,'SELECT count(*) FROM og_lake_internal.index_segment'),scalar(t.observer,'SELECT count(*) FROM og_lake_internal.index_coverage')),(0,0))
        check('pending definition is still explicitly unusable',status(),{'index_uuid':uuid,'state':'ACTIVE','ready_segments':0,'usable':False,'reason':'index_backend_not_implemented'})
        query(t.a,'DEALLOCATE exact_id');query(t.a,'ALTER FOREIGN TABLE biz.orders RENAME TO renamed');t.name='renamed'
        query(t.a,'PREPARE exact_id(integer) AS SELECT id,val FROM biz.renamed WHERE id=$1 ORDER BY val')
        check('fresh prepared predicate follows committed renamed table',prepared(),[(1,11)])
        check('rename preserves semantic index identity',query(t.observer,'SELECT index_uuid::text,table_uuid::text,column_id,definition_hash,definition::text FROM og_lake_internal.index_definition'),definition)
        names=query(t.observer,"SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=%s AND c.relkind='r' ORDER BY c.relname",(t.schema,))
        native=read_native_catalog(t.observer,t.schema,{'tables':[{'table_name':n}for n,in names]},out/'native-export.json')
        lock=json.loads((D/'source-lock.json').read_text());receipt=json.loads((ROOT/'.reference/opengauss-native-lake/build/receipt.json').read_text())
        check('reference source lock remains exact',receipt['source_lock_hash'],sha256(D/'source-lock.json'))
        ref=Reference(out,lock,receipt);transport=out/'transport.ducklake';transport_native(ref,transport,native)
        found=ref.run(ref.attach(transport,True)+'SELECT id,val FROM lake.main.renamed ORDER BY id','current',result=True)
        check('independent DuckLake reads file/inline mix without index metadata',[tuple(x.values())for x in found],expected)
        found=ref.run(ref.attach(transport,True)+'SELECT id,val FROM lake.main.orders AT(VERSION => '+str(start)+') ORDER BY id','history',result=True)
        check('independent original history remains unchanged',[tuple(x.values())for x in found],original)
        query(t.a,'DEALLOCATE exact_id');query(t.a,'DROP FOREIGN TABLE biz.renamed')
        check('host unbind retires pending definition without physical data loss',status()['state'],'DROPPING')
        check('all executed inputs unchanged',all(sha256(f)==h for f,h in r['inputs'].items()));check_build(a.build_receipt);r['status']='pass'
    except BaseException as exc:r.update(reason=str(exc),traceback=traceback.format_exc())
    finally:
        if db is not None:
            try:db.cleanup();r['database_cleanup']=db.cleanup_result
            except BaseException as exc:r.update(status='fail',cleanup_error=str(exc))
        if roles is not None:
            try:
                if not r.get('database_cleanup')or r['database_cleanup']['returncode']:raise RuntimeError('Owned DB cleanup unproven; role retained')
                r['role_cleanup']=cleanup_roles(control,roles.path,a.run_id);r=roles.redact(r)
            except BaseException as exc:r.update(status='fail',role_cleanup_error=str(exc))
        if control is not None:control.close()
        write_json(out/'result.json',r)
    print(json.dumps({'status':r['status'],'checks':len(r['assertions']),'reason':r.get('reason'),'result':str(out/'result.json')},ensure_ascii=False))
    return 0 if r['status']=='pass'else 1


if __name__=='__main__':raise SystemExit(main())
