#!/usr/bin/env python3
"""Native B-tree NULL scan keys preserve lake visibility, rescans and mixed input semantics."""
import argparse
import hashlib
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


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--run-id',required=True)
    p.add_argument('--build-receipt',type=Path,required=True);a=p.parse_args();validate_run_id(a.run_id)
    out=ROOT/'公共文档/openGauss原生湖存储/验证'/a.run_id;out.mkdir(exist_ok=False)
    r={'status':'fail','scope':'SQL0.15 native SK_SEARCHNULL/NOTNULL with actual execution, correlated rescans, private DML and full file fallback','assertions':[],
       'limitations':['Heap inline int4 only, no remote/file-index/ANN/performance or production qualification.']}
    paths=[Path(__file__),D/'tests/native_table_create.py',D/'tests/permission_api.py',D/'native_test_context.py',
           D/'native_runtime.py',D/'tests/inline_cleanup.py',D/'tests/file_dml_acceptance.py',D/'inline_fixture.py',
           D/'tests/native_auto_flush.py',D/'source-lock.json']
    r['inputs']={str(f):sha256(f)for f in paths};archive=out/'executed-inputs';archive.mkdir()
    for f in paths:shutil.copy2(f,archive/f.name)
    control=db=roles=t=None

    def check(name,got,want=True):
        ok=got==want
        def summary(value):
            if isinstance(value,list) and len(value)>30:
                return {'rows':len(value),'sha256':hashlib.sha256(repr(value).encode()).hexdigest()}
            return value
        r['assertions'].append({'name':name,'observed':summary(got),'expected':summary(want),'status':'pass'if ok else'fail'})
        if not ok:raise AssertionError(name)

    try:
        build=check_build(a.build_receipt);r.update(source_head=build['source_head'],binary_sha256=build['binary_sha256'])
        control=connect_control();check('automatic scheduler stays disabled',scalar(control,'SHOW enable_lake_maintenance'),'off')
        with(out/'fixture.log').open('w')as log:
            subprocess.run([sys.executable,str(D/'tests/catalog_fixture_bundle.py'),'--run-id',a.run_id,'--output',str(out/'fixture/manifest.json')],
                           stdout=log,stderr=subprocess.STDOUT,check=True,timeout=300)
        db=OwnedDatabase(a.run_id,out).create();roles=OwnedRoles(control,a.run_id,out)
        t=Case(db,SimpleNamespace(mode='net_dml',sql_version='0.15',manifest=out/'fixture/manifest.json',output=out/'result.json',run_id=a.run_id),r,roles)
        r['plans']=[]
        for name,mostly_null in [('orders',False),('mostnulls',True)]:
            t.create(name);table='biz.'+name;heap='biz.ref_'+name
            query(t.a,'CREATE TABLE '+heap+'(id integer,val integer)')
            query(t.a,"SELECT og_lake.set_option(%s::regclass,'data_inlining_row_limit','20000')",(table,))
            query(t.a,'BEGIN')
            for target in [table,heap]:
                query(t.a,'INSERT INTO '+target+' SELECT '+('NULL' if mostly_null else 'i')+',i%17 FROM generate_series(1,6000) i')
                query(t.a,'INSERT INTO '+target+(' VALUES(1,91),(2,92),(2,93)' if mostly_null else ' VALUES(NULL,91),(NULL,92),(NULL,93)'))
            query(t.a,'COMMIT');query(t.a,'ANALYZE '+table)
            uuid=str(scalar(t.a,"SELECT og_lake.create_lake_index(%s::regclass,'ix_id'::name,%s::jsonb)",
                            (table,json.dumps({'version':1,'kind':'scalar','method':'btree','column':'id'}))))
            query(t.a,'SELECT og_lake.build_lake_index(%s::uuid)',(uuid,))
            predicate='id IS NOT NULL' if mostly_null else 'id IS NULL'
            plan='\n'.join(x[0]for x in query(t.a,'EXPLAIN ANALYZE SELECT id,val FROM '+table+' WHERE '+predicate))
            r['plans'].append(plan)
            check(name+' null predicate planned native B-tree','Planned Inline Access: native B-tree'in plan)
            check(name+' null predicate actual native IndexAM','Actual Inline Access: native B-tree'in plan)
            for where in ['id IS NULL','id IS NOT NULL','id IS NULL AND val>=92','id IS NOT NULL AND val>=92','id IS NULL OR id=2']:
                statement='SELECT id,val FROM '+table+' WHERE '+where+' ORDER BY id NULLS LAST,val'
                check(name+' complete values for '+where,query(t.a,statement),query(t.a,statement.replace(table,heap)))
            if mostly_null:continue
            query(t.a,'CREATE TABLE biz.rescan_values(v integer)')
            query(t.a,'INSERT INTO biz.rescan_values VALUES(91),(92),(94),(91)')
            statement='SELECT q.v,(SELECT count(*) FROM '+table+' o WHERE o.id IS NULL AND o.val>=q.v) FROM biz.rescan_values q ORDER BY q.v'
            plan='\n'.join(x[0]for x in query(t.a,'EXPLAIN ANALYZE '+statement));r['plans'].append(plan)
            check('correlated null key really uses native IndexAM','Actual Inline Access: native B-tree'in plan)
            check('correlated NULL scan reopens correctly on changing outer filters',query(t.a,statement),query(t.a,statement.replace(table,heap)))
            query(t.a,'PREPARE fixed_null AS SELECT id,val FROM '+table+' WHERE id IS NULL ORDER BY val')

            def compare(label):
                check(label,query(t.a,'EXECUTE fixed_null'),query(t.a,'SELECT id,val FROM '+heap+' WHERE id IS NULL ORDER BY val'))

            query(t.a,'BEGIN')
            for target in [table,heap]:query(t.a,'UPDATE '+target+' SET id=70000,val=val+1 WHERE id IS NULL')
            compare('private update hides every old indexed NULL')
            query(t.a,'SAVEPOINT child')
            for target in [table,heap]:
                query(t.a,'UPDATE '+target+' SET id=NULL WHERE id=1')
                query(t.a,'INSERT INTO '+target+' VALUES(NULL,999)')
            compare('private key change and insert produce NULL values without index entries')
            query(t.a,'ROLLBACK TO child');query(t.a,'RELEASE SAVEPOINT child')
            compare('child rollback restores parent NULL population');query(t.a,'COMMIT')
            query(t.a,'BEGIN')
            for target in [table,heap]:query(t.a,'INSERT INTO '+target+' VALUES(NULL,1000)')
            query(t.a,'COMMIT');compare('committed native index receives new NULL key')
            query(t.a,'SELECT og_lake.flush(%s::regclass)',(table,))
            check('NULL case contains genuine Parquet files',scalar(t.observer,'SELECT count(*) FROM created_metadata.ducklake_data_file')>0)
            compare('cached NULL key includes complete file fallback after flush')
            query(t.a,'BEGIN')
            for target in [table,heap]:query(t.a,'UPDATE '+target+' SET val=val+3 WHERE id IS NULL')
            compare('file-origin NULL update hides old version and returns private replacement')
            query(t.a,'COMMIT');compare('committed mixed NULL versions remain exact')
            query(t.a,'DEALLOCATE fixed_null')
        check('all executed inputs unchanged',all(sha256(Path(f))==h for f,h in r['inputs'].items()));check_build(a.build_receipt);r['status']='pass'
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
