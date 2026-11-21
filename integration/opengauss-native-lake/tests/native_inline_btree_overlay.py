#!/usr/bin/env python3
"""An indexed inline stream must include private changed-key versions exactly."""
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


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--run-id',required=True)
    p.add_argument('--build-receipt',type=Path,required=True);a=p.parse_args();validate_run_id(a.run_id)
    out=ROOT/'公共文档/openGauss原生湖存储/验证'/a.run_id;out.mkdir(exist_ok=False)
    r={'status':'fail','scope':'SQL0.15 actual indexed stream with own changed-key after-images and savepoints','assertions':[],
       'limitations':['Heap inline int4 only, no remote/file-index/ANN/performance or production qualification.']}
    paths=[Path(__file__),D/'tests/native_table_create.py',D/'tests/permission_api.py',D/'native_test_context.py',
           D/'native_runtime.py',D/'tests/inline_cleanup.py',D/'tests/file_dml_acceptance.py',D/'inline_fixture.py',
           D/'tests/native_auto_flush.py',D/'source-lock.json']
    r['inputs']={str(f):sha256(f)for f in paths};archive=out/'executed-inputs';archive.mkdir()
    for f in paths:shutil.copy2(f,archive/f.name)
    control=db=roles=t=None

    def check(name,got,want=True):
        ok=got==want;r['assertions'].append({'name':name,'observed':got,'expected':want,'status':'pass'if ok else'fail'})
        if not ok:raise AssertionError(name)

    try:
        build=check_build(a.build_receipt);r.update(source_head=build['source_head'],binary_sha256=build['binary_sha256'])
        control=connect_control();check('automatic scheduler stays disabled',scalar(control,'SHOW enable_lake_maintenance'),'off')
        with(out/'fixture.log').open('w')as log:
            subprocess.run([sys.executable,str(D/'tests/catalog_fixture_bundle.py'),'--run-id',a.run_id,'--output',str(out/'fixture/manifest.json')],
                           stdout=log,stderr=subprocess.STDOUT,check=True,timeout=300)
        db=OwnedDatabase(a.run_id,out).create();roles=OwnedRoles(control,a.run_id,out)
        t=Case(db,SimpleNamespace(mode='net_dml',sql_version='0.15',manifest=out/'fixture/manifest.json',output=out/'result.json',run_id=a.run_id),r,roles)
        t.create();query(t.a,'CREATE TABLE biz.reference(id integer,val integer)')
        query(t.a,"SELECT og_lake.set_option('biz.orders'::regclass,'data_inlining_row_limit'::text,'20000'::text)")
        query(t.a,'BEGIN')
        for name in ['biz.orders','biz.reference']:
            query(t.a,'INSERT INTO '+name+' SELECT i,i%17 FROM generate_series(1,6000) i')
            query(t.a,'INSERT INTO '+name+' VALUES(7,700),(7,701)')
        query(t.a,'COMMIT');query(t.a,'ANALYZE biz.orders')
        uuid=str(scalar(t.a,"SELECT og_lake.create_lake_index('biz.orders'::regclass,'ix_id'::name,%s::jsonb)",
                        (json.dumps({'version':1,'kind':'scalar','method':'btree','column':'id'}),)))
        query(t.a,'SELECT og_lake.build_lake_index(%s::uuid)',(uuid,))
        plan='\n'.join(x[0]for x in query(t.a,'EXPLAIN ANALYZE SELECT id,val FROM biz.orders WHERE id=7'))
        r['plan']=plan;check('baseline planner chooses native B-tree','Planned Inline Access: native B-tree'in plan)
        check('actual execution opened native IndexAM instead of scan fallback','Actual Inline Access: native B-tree'in plan)

        def compare(label,key):
            got=query(t.a,'SELECT id,val FROM biz.orders WHERE id=%s ORDER BY val',(key,))
            want=query(t.a,'SELECT id,val FROM biz.reference WHERE id=%s ORDER BY val',(key,))
            check(label,got,want)

        query(t.a,'BEGIN')
        for name in ['biz.orders','biz.reference']:query(t.a,'UPDATE '+name+' SET id=10007,val=val+1 WHERE id=7')
        compare('old indexed key hides all three overwritten base versions',7)
        compare('new key includes all private after-images without physical index entries',10007)
        query(t.a,'SAVEPOINT child')
        for name in ['biz.orders','biz.reference']:
            query(t.a,'INSERT INTO '+name+' VALUES(7,777)')
            query(t.a,'UPDATE '+name+' SET val=val+9 WHERE id=10007')
        compare('new private row with old key is returned exactly once',7)
        compare('second private update supersedes every prior private version',10007)
        query(t.a,'ROLLBACK TO child');query(t.a,'RELEASE SAVEPOINT child')
        compare('child rollback removes private inserted key',7)
        compare('child rollback restores parent private versions',10007)
        query(t.a,'COMMIT')
        compare('committed native index hides ended old-key versions',7)
        compare('committed native B-tree contains all moved-key versions',10007)
        check('whole result cardinality remains exact',scalar(t.a,'SELECT count(*) FROM biz.orders'),scalar(t.a,'SELECT count(*) FROM biz.reference'))
        query(t.a,'BEGIN')
        for name in ['biz.orders','biz.reference']:query(t.a,'DELETE FROM '+name+' WHERE id=10007')
        compare('own delete hides matching indexed versions',10007);query(t.a,'ROLLBACK')
        compare('parent rollback restores committed indexed versions',10007)
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
