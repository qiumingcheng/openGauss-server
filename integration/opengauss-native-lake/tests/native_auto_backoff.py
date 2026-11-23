#!/usr/bin/env python3
"""Exercise the real automatic worker's error cooldown under committed hints.

A native metadata lock pins the worker after genuine Parquet preparation. The
owned worker is cancelled, then ordinary business INSERTs keep issuing hints.
No manual flush or synthetic maintenance shared-memory state is used.
"""
import argparse
import datetime
import hashlib
import json
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
from native_auto_flush import RUNTIME,atomic_bytes,connect_control,cleanup_owned_after_restart
from native_runtime import PROFILE
from native_test_context import OwnedDatabase
from native_table_create import Case
from permission_api import OwnedRoles


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--run-id',required=True);p.add_argument('--build-receipt',type=Path,required=True)
    a=p.parse_args();validate_run_id(a.run_id)
    out=ROOT/'公共文档/openGauss原生湖存储/验证'/a.run_id;out.mkdir(exist_ok=False)
    r={'status':'preparing','assertions':[],'scope':'Real native30-second error cooldown despite new committed hints',
       'limitations':['Single-instance '+PROFILE+' correctness; not qualified performance, long soak, remote SDK or RAC acceptance.']}
    inputs=[Path(__file__),D/'tests/native_auto_flush.py',D/'tests/native_table_create.py',
            D/'tests/permission_api.py',D/'native_test_context.py',D/'inline_fixture.py',D/'source-lock.json']
    inputs += [D / 'native_runtime.py', D / 'build_driver.py', D / 'tests/inline_cleanup.py']
    r['inputs']={str(x):sha256(x)for x in inputs};archive=out/'executed-inputs';archive.mkdir()
    for x in inputs:shutil.copy2(x,archive/x.name)
    db=roles=control=t=None;changed=restored=False;conf=RUNTIME/'data/postgresql.conf'
    original=installed=None
    def save():r['updated_at']=datetime.datetime.now(datetime.timezone.utc).isoformat();write_json(out/'result.json',r)
    def check(name,got,want=True):
        ok=got==want;r['assertions'].append({'name':name,'observed':got,'expected':want,'status':'pass'if ok else'fail'});save()
        if not ok:raise AssertionError(name)
    def restart(suffix):
        run=a.run_id+'_'+suffix
        with (out/(suffix+'.log')).open('w')as log:
            subprocess.run(['bash',str(D/'build.sh'),PROFILE,'restart','--run-id',run],stdout=log,stderr=subprocess.STDOUT,check=True,timeout=180)
        receipt=ROOT/'公共文档/openGauss原生湖存储/验证'/run/'build.json'
        b=check_build(receipt);check(suffix+' keeps exact native binary',b['binary_sha256'],r['binary_sha256']);r[suffix+'_receipt']=str(receipt)
    def state():return json.loads(scalar(t.observer,'SELECT og_lake.host_status()'))['automatic_flush']
    def until(label,predicate,seconds=15):
        deadline=time.monotonic()+seconds
        while True:
            s=state()
            if predicate(s):return s
            if time.monotonic()>deadline:raise TimeoutError(label)
            time.sleep(.02)
    try:
        b=check_build(a.build_receipt);r.update(source_head=b['source_head'],binary_sha256=b['binary_sha256'])
        with (out/'fixture.log').open('w')as log:
            subprocess.run([sys.executable,str(D/'tests/catalog_fixture_bundle.py'),'--run-id',a.run_id,
                            '--output',str(out/'fixture/manifest.json')],stdout=log,stderr=subprocess.STDOUT,check=True,timeout=300)
        control=connect_control()
        check('dedicated data directory',scalar(control,'SHOW data_directory'),str(RUNTIME/'data'))
        check('maintenance initially disabled',scalar(control,'SHOW enable_lake_maintenance'),'off')
        check('only baseline database exists',query(control,'SELECT datname FROM pg_database WHERE NOT datistemplate ORDER BY datname'),[('postgres',)])
        own=scalar(control,'SELECT pg_backend_pid()')
        check('no unrelated frontend before restart',query(control,'SELECT pid FROM pg_stat_activity WHERE client_port IS NOT NULL AND pid<>%s',(own,)),[])
        control.close();control=None
        original=conf.read_bytes();(out/'original-postgresql.conf').write_bytes(original);mode=conf.stat().st_mode&0o777
        installed=original+('\n# owned error cooldown '+a.run_id+'\nenable_lake_maintenance=on\nlake_flush_naptime=100\n'
                            'lake_flush_min_rows=3\nlake_flush_max_delay=60000\nlake_flush_worker_budget=5000\nlake_flush_max_batches=1\n').encode()
        atomic_bytes(conf,installed,mode);changed=True;restart('enable');control=connect_control()
        db=OwnedDatabase(a.run_id,out).create();roles=OwnedRoles(control,a.run_id,out)
        t=Case(db,SimpleNamespace(mode='net_dml',sql_version='0.11',manifest=out/'fixture/manifest.json',output=out/'result.json',run_id=a.run_id),r,roles)
        check('owned session obtains maintenance claim',scalar(t.a,'SELECT og_lake_internal.test_maintenance_claim()'))
        t.create();query(t.a,'INSERT INTO biz.orders VALUES(1,10),(2,20),(3,30)')
        generation=state()['generation']
        until('scheduler reaches claimed database',lambda s:s['database_oid']==db.allocation['database_oid']and s['generation']>generation)
        baseline_protocol=t.protocol();root=Path(t.empty_manifest['data_path']);baseline_disk=sorted(str(x)for x in root.rglob('*.parquet'))
        before=state();query(t.b,'BEGIN');query(t.b,'LOCK TABLE created_metadata.ducklake_table_stats IN SHARE MODE')
        check('release claim for actual worker',scalar(t.a,'SELECT og_lake_internal.test_maintenance_release()'))
        deadline=time.monotonic()+15;blocked=[]
        while not blocked:
            blocked=query(t.observer,"SELECT pid FROM pg_locks WHERE relation='created_metadata.ducklake_table_stats'::regclass AND mode='RowExclusiveLock' AND NOT granted")
            if time.monotonic()>deadline:raise TimeoutError('Actual automatic publisher did not reach SPI metadata lock')
            if not blocked:time.sleep(.01)
        active=state();check('one worker at actual barrier',len(blocked),1)
        check('blocked publisher is registered native worker',blocked[0][0],active['worker_pid'])
        check('uncommitted directory invisible',t.protocol(),baseline_protocol)
        check('cancel owned worker in actual SPI',scalar(t.observer,'SELECT pg_cancel_backend(%s)',(blocked[0][0],)))
        ended=until('cancelled worker exit',lambda s:s['failed_batches']>before['failed_batches']and(s['worker_pid']==0 or s['generation']!=active['generation']))
        start=time.monotonic();r['cancelled_generation']=active['generation'];r['cancelled_exit_state']=ended
        query(t.b,'ROLLBACK')
        check('cancel fully rolled back directory',t.protocol(),baseline_protocol)
        check('cancel removed unpublished real candidates',sorted(str(x)for x in root.rglob('*.parquet')),baseline_disk)
        expected=[(1,10),(2,20),(3,30)];hints=0;first_retry=None;r['progress']=[]
        while time.monotonic()-start<40:
            elapsed=time.monotonic()-start
            if hints<3 and elapsed>=hints*5:
                identifier=4+hints;query(t.a,'INSERT INTO biz.orders VALUES(%s,%s)',(identifier,identifier*10));expected.append((identifier,identifier*10));hints+=1
                r.setdefault('hint_commits',[]).append({'seconds':time.monotonic()-start,'id':identifier})
            s=state();elapsed=time.monotonic()-start
            files=scalar(t.observer,'SELECT count(*) FROM created_metadata.ducklake_data_file')
            own_retry=s['database_oid']==db.allocation['database_oid']and s['generation']>active['generation']
            if own_retry or files:
                if first_retry is None:
                    first_retry=elapsed;r['first_retry_seconds_after_observed_exit']=elapsed;r['first_retry_state']=s
                    check('new commit hints cannot bypass30-second error cooldown with500ms observation tolerance',elapsed>=29.5)
                if files:break
            if not r['progress']or elapsed-r['progress'][-1]['seconds']>=1:
                r['progress'].append({'seconds':elapsed,'state':s,'files':files});save()
            time.sleep(.05)
        check('three actual committed hints exercised',hints,3)
        check('error cooldown eventually expires and automatic flush commits',scalar(t.observer,'SELECT count(*) FROM created_metadata.ducklake_data_file')>0)
        until('retry completes and maintenance claim releases',lambda s:scalar(t.a,'SELECT og_lake_internal.test_maintenance_claim()'),seconds=5)
        check('all acknowledged business values exactly once',t.rows(),expected)
        check('native row allocation frontier unaffected by flush',scalar(t.observer,'SELECT next_row_id FROM created_metadata.ducklake_table_stats'),6)
        check('automatic retry registered a successful batch',state()['completed_batches']>before['completed_batches'])
        check('all frozen input bytes unchanged',all(sha256(f)==h for f,h in r['inputs'].items()))
        check_build(a.build_receipt);r['status']='pass'
    except BaseException as exc:r.update(status='fail',reason=str(exc),traceback=traceback.format_exc())
    finally:
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
                check('exact original configuration restored',sha256(conf),hashlib.sha256(original).hexdigest())
            except BaseException as exc:r.update(status='fail',restoration_error=str(exc))
        if db is not None:
            try:
                if changed and not restored:raise RuntimeError('Restoration unproven; owned objects retained')
                control=connect_control();check('maintenance disabled before cleanup',scalar(control,'SHOW enable_lake_maintenance'),'off')
                r.update(cleanup_owned_after_restart(db,roles,a.run_id,out,control,r.get('restore_receipt')))
                if roles is not None:r=roles.redact(r)
            except BaseException as exc:r.update(status='fail',cleanup_error=str(exc),cleanup_traceback=traceback.format_exc())
        if control is not None:control.close()
        save()
    print(json.dumps({'status':r['status'],'checks':len(r['assertions']),'reason':r.get('reason'),
                      'first_retry_seconds':r.get('first_retry_seconds_after_observed_exit'),'result':str(out/'result.json')},ensure_ascii=False))
    return 0 if r['status']=='pass'else 1

if __name__=='__main__':raise SystemExit(main())
