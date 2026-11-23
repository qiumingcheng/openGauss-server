#!/usr/bin/env python3
"""SQL0.16 independent index-root configuration and private catalog upgrade."""
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
    p.add_argument('--build-receipt',type=Path,required=True);p.add_argument('--mode',choices=['configuration','upgrade','permissions','concurrency'],required=True);a=p.parse_args();validate_run_id(a.run_id)
    out=ROOT/'公共文档/openGauss原生湖存储/验证'/a.run_id;out.mkdir(exist_ok=False)
    r={'status':'fail','scope':'SQL0.16 native index_root '+a.mode,'assertions':[],
       'limitations':['Root configuration and upgrade only; no native file index builder/READY/coverage/query/remote/RAC or production qualification.']}
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
        t=Case(db,SimpleNamespace(mode='net_dml',sql_version='0.15' if a.mode=='upgrade' else '0.16',manifest=out/'fixture/manifest.json',output=out/'result.json',run_id=a.run_id),r,roles)
        t.create();before=t.protocol();head=t.head()
        parent=out/'index-stores';parent.mkdir();roots=[parent/name for name in ['a','b','c']]
        for path in roots:path.mkdir()
        def stored(conn=t.observer):
            return scalar(conn,"SELECT index_root FROM og_lake_internal.catalog_binding WHERE server_oid=(SELECT oid FROM pg_foreign_server WHERE srvname='created_lake')")
        def setting(path,conn=t.a):
            return query(conn,"SELECT og_lake.set_option('created_lake'::name,'index_root'::text,%s::text)",(str(path),))
        def rejected(statement,code,args=None,conn=t.a):
            import psycopg2
            try:query(conn,statement,args)
            except psycopg2.Error as exc:
                check('rejected '+statement[:100],exc.pgcode,code);query(conn,'ROLLBACK')
            else:raise AssertionError('Unexpectedly accepted '+statement)
        if a.mode=='upgrade':
            setting_sql="SELECT og_lake.set_option('created_lake'::name,'index_root',%s::text)"
            rejected(setting_sql,'0A000',(str(roots[0]),))
            query(t.a,"ALTER EXTENSION og_lake UPDATE TO '0.16'")
            check('configured owner upgrade adds a NULL root',stored(),None)
            check('catalog relation keeps its exact private owner',scalar(t.a,"SELECT pg_get_userbyid(relowner) FROM pg_class WHERE oid='og_lake_internal.catalog_binding'::regclass"),t.owner)
            check('upgrade preserves all standard metadata and data',t.protocol(),before)
        check('root starts unconfigured',stored(),None)
        setting(roots[0]);check('root normalized and persisted',stored(),str(roots[0])+'/')
        setting(str(roots[0])+'////');check('canonical idempotence retains the same root',stored(),str(roots[0])+'/')
        check('configuration creates no business snapshot or IDs',t.head(),head)
        check('configuration does not modify any standard table',t.protocol(),before)
        check('host advertises its implemented file builder',json.loads(scalar(t.a,'SELECT og_lake.host_status()'))['lake_index_file_build'],True)
        check('root configuration alone creates no index segments',scalar(t.a,'SELECT count(*) FROM og_lake_internal.index_segment'),0)
        if a.mode in ['configuration','upgrade']:
            query(t.a,'BEGIN');setting(roots[1]);query(t.a,'INSERT INTO biz.heap_marker VALUES(77)')
            check('own option is visible in its transaction',stored(t.a),str(roots[1])+'/')
            check('other session retains committed root',stored(),str(roots[0])+'/');query(t.a,'ROLLBACK')
            check('root and ordinary heap roll back together',(stored(),query(t.observer,'SELECT * FROM biz.heap_marker')),(str(roots[0])+'/',[]))
            query(t.a,'BEGIN');query(t.a,'SAVEPOINT child');setting(roots[1]);query(t.a,'ROLLBACK TO child');query(t.a,'RELEASE SAVEPOINT child');query(t.a,'COMMIT')
            check('savepoint rollback restores parent option',stored(),str(roots[0])+'/')
            query(t.a,'BEGIN');setting(roots[1]);rejected('INSERT INTO biz.orders VALUES(1,10)','0A000')
            check('mixed DML rejection rolls back its root',stored(),str(roots[0])+'/')
            query(t.a,'BEGIN');query(t.a,'INSERT INTO biz.orders VALUES(1,10)');rejected("SELECT og_lake.set_option('created_lake'::name,'index_root',%s::text)",'0A000',(str(roots[1]),))
            check('reverse mixed transaction rolls back all lake rows',query(t.a,'SELECT * FROM biz.orders'),[])
        if a.mode=='configuration':
            data_root=Path(scalar(t.a,"SELECT data_root FROM og_lake_internal.catalog_binding WHERE server_oid=(SELECT oid FROM pg_foreign_server WHERE srvname='created_lake')"));data_root.mkdir(parents=True,exist_ok=True)
            child=data_root/'child';child.mkdir();link=parent/'link';link.symlink_to(roots[0]);file=parent/'ordinary-file';file.write_bytes(b'owned test file')
            statement="SELECT og_lake.set_option('created_lake'::name,'index_root',%s::text)"
            for value,code in [(data_root,'55000'),(child,'55000'),(data_root.parent,'55000'),(link,'55000'),(file,'55000'),('relative/index','0A000'),('','22023'),('/','22023'),(str(parent)+'/../index-stores/a','22023'),(parent/'missing','58P01')]:
                rejected(statement,code,(str(value),));check('bad path keeps original root',stored(),str(roots[0])+'/')
            rejected("SELECT og_lake.set_option('biz.orders'::regclass,'index_root',%s::text)",'22023',(str(roots[1]),))
            rejected("SELECT og_lake.create_catalog('overlap'::name,'overlap_metadata'::name,%s::text,NULL::text)",'55000',(str(roots[0]/'new_catalog'),))
            check('reverse registration refuses the index namespace atomically',scalar(t.a,"SELECT count(*) FROM pg_foreign_server WHERE srvname='overlap'"),0)
            uuid=str(scalar(t.a,"SELECT og_lake.create_lake_index('biz.orders'::regclass,'ix_id',%s::jsonb)",(json.dumps({'version':1,'kind':'scalar','method':'btree','column':'id'}),)))
            # Synthetic FAILED metadata tests only root retention, never index artifacts/readiness.
            query(t.a,"INSERT INTO og_lake_internal.index_segment SELECT gen_random_uuid(),index_uuid,definition_generation,gen_random_uuid(),0,%s,1,1,%s,NULL,'FAILED' FROM og_lake_internal.index_definition WHERE index_uuid=%s::uuid",('0'*64,str(roots[0]/'retained-metadata-only'),uuid))
            rejected(statement,'55000',(str(roots[1]),));setting(roots[0])
            check('retained metadata pins root but permits idempotence',stored(),str(roots[0])+'/')
        elif a.mode=='permissions':
            from psycopg2 import sql
            role=roles.create('caller');caller,identity=roles.connect(db,'caller');r['ordinary_identities']=[identity]
            query(t.a,sql.SQL('GRANT USAGE ON SCHEMA og_lake TO {}').format(sql.Identifier(role)))
            rejected("SELECT og_lake.set_option('created_lake'::name,'index_root',%s::text)",'42501',(str(roots[1]),),caller)
            rejected('SELECT index_root FROM og_lake_internal.catalog_binding','42501',conn=caller)
            check('ordinary caller retains identity',scalar(caller,'SELECT current_user'),role)
            query(t.a,'BEGIN READ ONLY');rejected("SELECT og_lake.set_option('created_lake'::name,'index_root',%s::text)",'25006',(str(roots[1]),))
            check('all denied writes preserve root',stored(),str(roots[0])+'/')
        elif a.mode=='concurrency':
            import concurrent.futures,time
            pid=int(scalar(t.b,'SELECT pg_backend_pid()'));relation=scalar(t.a,"SELECT 'og_lake_internal.catalog_binding'::regclass::oid")
            def writer():
                import psycopg2
                try:
                    query(t.b,'BEGIN');setting(roots[2],t.b);query(t.b,'INSERT INTO biz.heap_marker VALUES(8)');query(t.b,'COMMIT');return 'committed'
                except psycopg2.Error as exc:query(t.b,'ROLLBACK');return exc.pgcode
            for cancel in [False,True]:
                setting(roots[0]);query(t.a,'BEGIN');setting(roots[1])
                executor=concurrent.futures.ThreadPoolExecutor(max_workers=1);future=executor.submit(writer)
                try:
                    deadline=time.monotonic()+15
                    while not scalar(t.observer,"SELECT EXISTS(SELECT 1 FROM pg_locks WHERE pid=%s AND locktype='relation' AND relation=%s AND NOT granted)",(pid,relation)):
                        if time.monotonic()>deadline:raise TimeoutError('Exact root writer did not wait on registration lock')
                        time.sleep(.01)
                    check('root writer waits on real registration relation lock',not future.done())
                    if cancel:
                        check('cancel only the owned blocked writer',scalar(t.observer,'SELECT pg_cancel_backend(%s)',(pid,)))
                        outcome=future.result(timeout=15);query(t.a,'ROLLBACK');check('cancelled waiting writer rolls back',outcome,'57014')
                        check('cancelled configuration leaves committed root',stored(),str(roots[0])+'/')
                    else:
                        query(t.a,'INSERT INTO biz.heap_marker VALUES(7)');query(t.a,'COMMIT')
                        check('post-wait fresh RC writer commits',future.result(timeout=15),'committed')
                        check('later serialized root is current',stored(),str(roots[2])+'/')
                        check('both ordinary heap commits remain',query(t.observer,'SELECT * FROM biz.heap_marker ORDER BY id'),[(7,),(8,)])
                    check('waiting writer connection is reusable',scalar(t.b,'SELECT 6*7'),42)
                finally:
                    query(t.a,'ROLLBACK')
                    if not future.done():scalar(t.observer,'SELECT pg_cancel_backend(%s)',(pid,))
                    executor.shutdown(wait=True);query(t.b,'ROLLBACK')
        check('final standard protocol remains unchanged',t.protocol(),before)
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
