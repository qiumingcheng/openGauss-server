#!/usr/bin/env python3
"""Timed real RC heap/lake atomicity under automatic flush and concurrent DML.

Every acknowledged transaction updates both the native heap oracle and the lake.
Active readers compare full row multiplicities within one SQL statement. Known
serialization failures retry the whole transaction; unknown outcomes stop work.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import datetime
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import threading
import time
import traceback
from types import SimpleNamespace

ROOT=Path('/home/qiumc/data/code/cpp'); D=ROOT/'integration/opengauss-native-lake'
sys.path[:0]=[str(D),str(D/'tests')]
from native_auto_flush import RUNTIME, atomic_bytes, connect_control, cleanup_owned_after_restart
from native_runtime import PROFILE, INSTALL
from file_dml_acceptance import check_build
from inline_fixture import query,scalar
from lake_harness import sha256,validate_run_id,write_json
from native_test_context import OwnedDatabase
from native_table_create import Case
from permission_api import OwnedRoles


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--run-id',required=True);p.add_argument('--build-receipt',type=Path,required=True)
    p.add_argument('--seconds',type=int,default=120);p.add_argument('--wait-for',type=Path)
    p.add_argument('--maintenance',choices=['on','off'],default='on')
    p.add_argument('--schedule',choices=['stress','default'],default='stress')
    a=p.parse_args();validate_run_id(a.run_id)
    if not 30<=a.seconds<=86400:p.error('seconds must be30..86400')
    out=ROOT/'公共文档/openGauss原生湖存储/验证'/a.run_id;out.mkdir(exist_ok=False)
    profiles={'stress':{'lake_flush_naptime':200,'lake_flush_min_rows':128,'lake_flush_max_delay':1000,'lake_flush_worker_budget':5000,'lake_flush_max_batches':2},
              'default':{'lake_flush_naptime':1000,'lake_flush_min_rows':100000,'lake_flush_max_delay':10000,'lake_flush_worker_budget':30000,'lake_flush_max_batches':8}}
    profile=profiles[a.schedule]
    result={'status':'preparing','seconds_requested':a.seconds,'schedule':a.schedule,'configured_schedule':profile,'assertions':[],
            'scope':'Real native READ COMMITTED mixed heap/lake DML; automatic maintenance='+a.maintenance,
            'limitations':[PROFILE+' correctness only; no qualified throughput, full long soak, physical GC, private SDK or actual OGRAC qualification.']}
    inputs=[Path(__file__),D/'tests/native_auto_flush.py',D/'tests/native_table_create.py',D/'native_test_context.py',
            D/'tests/permission_api.py',D/'inline_fixture.py',D/'source-lock.json']
    inputs += [D / 'native_runtime.py', D / 'build_driver.py', D / 'tests/inline_cleanup.py']
    result['inputs']={str(x):sha256(x)for x in inputs};archive=out/'executed-inputs';archive.mkdir()
    for x in inputs:shutil.copy2(x,archive/x.name)
    db=roles=control=t=None;original=installed=None;changed=restored=False;conf=RUNTIME/'data/postgresql.conf'
    stop=threading.Event();mutex=threading.Lock();executor=None;futures=[]
    counters={'writer_commits':{1:0,2:0},'serialization_retries':{1:0,2:0},'reader_checks':0}
    expected={1:0,2:0,3:0};actor_errors=[]
    def save():
        with mutex:result['counters']=json.loads(json.dumps(counters))
        result['updated_at']=datetime.datetime.now(datetime.timezone.utc).isoformat()
        write_json(out/'result.json',result)
    def check(name,got,want=True):
        ok=got==want;result['assertions'].append({'name':name,'observed':got,'expected':want,'status':'pass'if ok else'fail'})
        save()
        if not ok:raise AssertionError(name)
    def restart(suffix):
        run=a.run_id+'_'+suffix
        with (out/(suffix+'.log')).open('w')as log:
            subprocess.run(['bash',str(D/'build.sh'),PROFILE,'restart','--run-id',run],stdout=log,stderr=subprocess.STDOUT,check=True,timeout=180)
        receipt=ROOT/'公共文档/openGauss原生湖存储/验证'/run/'build.json'
        b=check_build(receipt);check(suffix+' exact native binary',b['binary_sha256'],result['binary_sha256'])
        result[suffix+'_receipt']=str(receipt)
    try:
        if a.wait_for:
            deadline=time.monotonic()+1800
            while True:
                prior=json.loads(a.wait_for.read_text())
                if prior['status']=='passed':break
                if prior['status']=='failed':raise RuntimeError('Prerequisite campaign failed; workload not started')
                if time.monotonic()>deadline:raise TimeoutError('Waiting prerequisite qualification')
                time.sleep(3)
        import psycopg2
        b=check_build(a.build_receipt);result.update(source_head=b['source_head'],binary_sha256=b['binary_sha256'])
        result['build_profile']=b['profile']
        provider=INSTALL/'lib/postgresql/og_lake_sdk/libog_lake_sdk_arrow.so'
        result['provider_sha256']=sha256(provider)
        with (out/'fixture.log').open('w')as log:
            subprocess.run([sys.executable,str(D/'tests/catalog_fixture_bundle.py'),'--run-id',a.run_id,
                            '--output',str(out/'fixture/manifest.json')],stdout=log,stderr=subprocess.STDOUT,check=True,timeout=300)
        control=connect_control()
        check('dedicated data directory',scalar(control,'SHOW data_directory'),str(RUNTIME/'data'))
        check('initial automatic maintenance disabled',scalar(control,'SHOW enable_lake_maintenance'),'off')
        check('only baseline database exists',query(control,'SELECT datname FROM pg_database WHERE NOT datistemplate ORDER BY datname'),[('postgres',)])
        own=scalar(control,'SELECT pg_backend_pid()')
        check('no other frontend before controlled restart',query(control,'SELECT pid FROM pg_stat_activity WHERE client_port IS NOT NULL AND pid<>%s',(own,)),[])
        control.close();control=None
        original=conf.read_bytes();(out/'original-postgresql.conf').write_bytes(original);mode=conf.stat().st_mode&0o777
        installed=original+('\n# owned timed workload '+a.run_id+'\nenable_lake_maintenance='+a.maintenance+'\n'+
                            ''.join(key+'='+str(value)+'\n'for key,value in profile.items())).encode()
        atomic_bytes(conf,installed,mode);changed=True;restart('enable')
        control=connect_control();check('exact configured maintenance mode',scalar(control,'SHOW enable_lake_maintenance'),a.maintenance)
        for key,value in profile.items():check('exact active schedule '+key,scalar(control,'SELECT setting::bigint FROM pg_settings WHERE name=%s',(key,)),value)
        db=OwnedDatabase(a.run_id,out).create();roles=OwnedRoles(control,a.run_id,out)
        t=Case(db,SimpleNamespace(mode='net_dml',sql_version='0.11',manifest=out/'fixture/manifest.json',output=out/'result.json',run_id=a.run_id),result,roles)
        t.create();query(t.a,'CREATE TABLE biz.reference(id integer PRIMARY KEY,val integer)')
        query(t.a,'BEGIN');query(t.a,'INSERT INTO biz.orders VALUES(1,0),(2,0),(3,0)')
        query(t.a,'INSERT INTO biz.reference VALUES(1,0),(2,0),(3,0)');query(t.a,'COMMIT')
        clients=[db.connect()for _ in range(3)]
        for conn in clients:
            query(conn,"SET default_transaction_isolation='read committed'");query(conn,'SET query_dop=1')
        difference='SELECT count(*) FROM ((SELECT id,val FROM biz.orders EXCEPT ALL SELECT id,val FROM biz.reference) UNION ALL (SELECT id,val FROM biz.reference EXCEPT ALL SELECT id,val FROM biz.orders)) AS mismatches'
        def writer(actor,conn):
            live=[];turn=1
            try:
                while not stop.is_set():
                    inserted=actor*1000000+turn if turn%7==0 else None
                    deleted=live[0]if turn%11==0 and live else None
                    for attempt in range(40):
                        try:
                            query(conn,'BEGIN')
                            query(conn,'UPDATE biz.orders SET val=val+1 WHERE id=%s',(actor,))
                            query(conn,'UPDATE biz.reference SET val=val+1 WHERE id=%s',(actor,))
                            if inserted is not None:
                                query(conn,'INSERT INTO biz.orders VALUES(%s,%s)',(inserted,turn))
                                query(conn,'INSERT INTO biz.reference VALUES(%s,%s)',(inserted,turn))
                            if deleted is not None:
                                query(conn,'DELETE FROM biz.orders WHERE id=%s',(deleted,))
                                query(conn,'DELETE FROM biz.reference WHERE id=%s',(deleted,))
                            query(conn,'COMMIT')
                            with mutex:
                                expected[actor]+=1;counters['writer_commits'][actor]+=1
                                if inserted is not None:expected[inserted]=turn;live.append(inserted)
                                if deleted is not None:del expected[deleted];live.pop(0)
                            turn+=1;break
                        except psycopg2.Error as exc:
                            query(conn,'ROLLBACK')
                            if exc.pgcode!='40001':raise
                            with mutex:counters['serialization_retries'][actor]+=1
                            if attempt==39:raise
                            stop.wait(.005)
                    stop.wait(.03)
            except BaseException as exc:
                with mutex:actor_errors.append({'actor':actor,'error':str(exc),'sqlstate':getattr(exc,'pgcode',None),'traceback':traceback.format_exc()})
                stop.set();raise
        def reader(conn):
            try:
                while not stop.is_set():
                    mismatches=scalar(conn,difference)
                    if mismatches:raise AssertionError('Active one-statement native heap/lake multiplicities differ: '+str(mismatches))
                    with mutex:counters['reader_checks']+=1
                    stop.wait(.05)
            except BaseException as exc:
                with mutex:actor_errors.append({'actor':'reader','error':str(exc),'sqlstate':getattr(exc,'pgcode',None),'traceback':traceback.format_exc()})
                stop.set();raise
        executor=ThreadPoolExecutor(max_workers=3)
        futures=[executor.submit(writer,1,clients[0]),executor.submit(writer,2,clients[1]),executor.submit(reader,clients[2])]
        started=time.monotonic();deadline=started+a.seconds;result['status']='running';result['progress']=[];save()
        while time.monotonic()<deadline and not stop.is_set():
            state=json.loads(scalar(t.observer,'SELECT og_lake.host_status()'))['automatic_flush']
            with mutex:sample={'seconds':time.monotonic()-started,'commits':dict(counters['writer_commits']),'reads':counters['reader_checks'],'automatic':state}
            result['progress'].append(sample);save();stop.wait(2)
        stop.set();executor.shutdown(wait=True);executor=None
        for future in futures:future.result()
        result['seconds_executed']=time.monotonic()-started;result['actor_errors']=actor_errors
        check('requested timed workload completed',result['seconds_executed']>=a.seconds)
        check('both real concurrent writers committed',all(counters['writer_commits'][x]>0 for x in [1,2]))
        check('active same-statement reader ran',counters['reader_checks']>0)
        check('no unexpected actor errors',actor_errors,[])
        check('final ordinary heap matches acknowledged transaction model',query(t.observer,'SELECT id,val FROM biz.reference ORDER BY id'),sorted(expected.items()))
        check('final lake matches same acknowledged transaction model',t.rows(),sorted(expected.items()))
        check('final multiplicities equal',scalar(t.observer,difference),0)
        final=json.loads(scalar(t.observer,'SELECT og_lake.host_status()'))['automatic_flush']
        if a.maintenance=='on':
            check('automatic background flush actually committed',final['completed_batches']>0)
            check('actual registered Parquet data output exists',scalar(t.observer,'SELECT count(*) FROM created_metadata.ducklake_data_file')>0)
        else:
            check('control has no automatic worker publication',final['completed_batches'],0)
            check('control remains entirely inline',scalar(t.observer,'SELECT count(*) FROM created_metadata.ducklake_data_file'),0)
        check('native SDK unchanged',sha256(provider),result['provider_sha256'])
        check('all frozen input bytes unchanged',all(sha256(p)==h for p,h in result['inputs'].items()))
        check_build(a.build_receipt);result['status']='pass'
    except BaseException as exc:
        result.update(status='fail',reason=str(exc),traceback=traceback.format_exc(),actor_errors=actor_errors)
    finally:
        stop.set()
        if executor is not None:
            executor.shutdown(wait=True)
            for future in futures:
                if future.exception()is not None:result.update(status='fail',actor_error=str(future.exception()))
        result['acknowledged_model']=sorted(expected.items())
        if t is not None:
            try:
                result['after_stop_lake_rows']=t.rows()
                result['after_stop_heap_rows']=query(t.observer,'SELECT id,val FROM biz.reference ORDER BY id')
            except BaseException as diagnostic:result['after_stop_diagnostic_error']=str(diagnostic)
        if db is not None:
            for conn in db.connections:
                try:conn.close()
                except BaseException:pass
            db.connections=[]
        if control is not None:control.close();control=None
        if changed:
            try:
                if conf.read_bytes()!=installed:raise RuntimeError('Configuration changed externally; not overwritten')
                atomic_bytes(conf,original,mode);restart('restore');restored=True
                check('configuration exactly restored',sha256(conf),hashlib.sha256(original).hexdigest())
            except BaseException as exc:result.update(status='fail',restoration_error=str(exc))
        if db is not None:
            try:
                if changed and not restored:raise RuntimeError('Restoration unproven; retain owned objects')
                control=connect_control();check('automatic maintenance disabled before cleanup',scalar(control,'SHOW enable_lake_maintenance'),'off')
                result.update(cleanup_owned_after_restart(db,roles,a.run_id,out,control,result.get('restore_receipt')))
                if roles is not None:result=roles.redact(result)
            except BaseException as exc:result.update(status='fail',cleanup_error=str(exc),cleanup_traceback=traceback.format_exc())
        if control is not None:control.close()
        save()
    print(json.dumps({'status':result['status'],'reason':result.get('reason'),'checks':len(result['assertions']),
                      'counters':result.get('counters'),'result':str(out/'result.json')},ensure_ascii=False))
    return 0 if result['status']=='pass'else 1

if __name__=='__main__':raise SystemExit(main())
