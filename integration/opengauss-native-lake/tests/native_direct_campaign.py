#!/usr/bin/env python3
"""Serial bounded-direct INSERT campaign; default prepares without Native work.

Only an explicitly executed child may allocate its exact database. Hard timeout
signals only this controller's Python process group. It never kills database
backends, retries cleanup across restart, changes configuration or starts a
second fixture after failure.
"""
import argparse
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys
import time
import traceback

HERE=Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('direct_draft',HERE/'native_direct_insert.py')
leaf=importlib.util.module_from_spec(spec);spec.loader.exec_module(leaf)
ROOT,D=leaf.ROOT,leaf.D


def assertions(data):
    rows=data.get('assertions',[])
    if not rows or any(x.get('status')!='pass' or 'observed' not in x or 'expected' not in x
                       or x['observed']!=x['expected'] for x in rows):
        raise RuntimeError('Leaf assertion records failed/absent/mutated')
    return len(rows)


def verify_cleanup(data, child_id, out, expected_start):
    clean=data.get('database_cleanups',[])
    if not clean: raise RuntimeError('Leaf has no exact database cleanup receipts')
    ids=[]
    for item in clean:
        owner_info=item.get('owner') or {}
        owner=owner_info.get('receipt') or {}
        path=Path(owner_info.get('path',''))
        if not path.is_file() or leaf.digest(path)!=owner_info.get('sha256'):
            raise RuntimeError('Owned database receipt bytes changed or are absent')
        if json.loads(path.read_text())!=owner: raise RuntimeError('Owned allocation record differs')
        allowed_run=child_id+'-reverse' if Path(item['attempt']).name=='reverse-db' else child_id
        if (owner.get('run_id')!=allowed_run or owner.get('phase')!='owned'
                or owner.get('runtime_profile')!='debug' or owner.get('postmaster_start')!=expected_start
                or Path(owner.get('attempt','')).resolve()!=Path(item['attempt']).resolve()
                or not re.fullmatch(r'lake_inline_[0-9]{14}_[0-9a-f]{8}',owner.get('database',''))
                or not owner.get('marker','').startswith('og_lake_inline_attempt:'+allowed_run+':')
                or not isinstance(owner.get('database_oid'),int)):
            raise RuntimeError('Owned allocation OID/marker/runtime mismatch')
        cleanup=item.get('cleanup',{})
        if cleanup.get('returncode')!=0: raise RuntimeError('Leaf cleanup failed/unproven')
        report=json.loads(cleanup['stdout']);immutable=Path(report.get('immutable_report',''))
        if (immutable.parent.resolve()!=Path(item['attempt']).resolve() or not immutable.is_file()
                or json.loads(immutable.read_text())!=report or report.get('status')!='pass'
                or report.get('terminated_backends')!=[] or report.get('run_id')!=allowed_run
                or report.get('dropped_database_oid')!=owner['database_oid']):
            raise RuntimeError('Leaf cleanup did not remove exactly the owned OID')
        ids.append({'database_oid':owner['database_oid'],'marker':owner['marker'],
                    'owner_receipt_sha256':leaf.digest(path),'immutable_cleanup':str(immutable),
                    'immutable_cleanup_sha256':leaf.digest(immutable)})
    roles=data.get('role_cleanup',{});rolepath=out/'permission-roles.json'
    role=json.loads(rolepath.read_text())
    if (roles.get('status')!='pass' or role.get('run_id')!=child_id
            or role.get('postmaster_start')!=expected_start or not role.get('roles')
            or set(roles.get('removed',[]))!={x['name'] for x in role['roles']}
            or any(x.get('phase')!='dropped' or not isinstance(x.get('oid'),int)
                   or not x.get('marker','').startswith('og_lake_role_attempt:'+child_id+':') for x in role['roles'])):
        raise RuntimeError('Exact private role cleanup unproven')
    return {'owned_databases':ids,'role_receipt_sha256':leaf.digest(rolepath)}


def verify_archive(data):
    inputs=data.get('inputs',{})
    if not inputs: raise RuntimeError('Leaf executed inputs absent')
    for original,item in inputs.items():
        if leaf.digest(original)!=item['sha256'] or leaf.digest(item['archive'])!=item['sha256']:
            raise RuntimeError('Frozen source/archive/binary changed: '+original)
    return len(inputs)


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--execute',action='store_true');p.add_argument('--run-id')
    p.add_argument('--build-receipt',type=Path);p.add_argument('--source-contract',type=Path)
    p.add_argument('--modes',nargs='+',choices=leaf.MODES,default=list(leaf.MODES))
    p.add_argument('--case-timeout',type=int,default=1800)
    p.add_argument('--sql-version',choices=['0.19','0.20','0.21'],default='0.19',help='types keeps retained19; legacy keeps18')
    p.add_argument('--prepare-output',type=Path)
    a=p.parse_args()
    if not 120<=a.case_timeout<=7200:p.error('case-timeout must be120..7200s')
    plan={'status':'prepared_not_executed','cases':[{'mode':x,'sql_version':'0.18' if x in ('legacy','carrier-legacy') else ('0.19'if x=='types'else a.sql_version),
          'hard_seconds':a.case_timeout} for x in dict.fromkeys(a.modes)],'native_cases_executed':0,
          'runner_sha256':leaf.digest(HERE/'native_direct_insert.py'),'controller_sha256':leaf.digest(__file__),
          'limitations':leaf.LIMITATIONS,'fixture_order':'strictly serial, stop first fail; no automatic cleanup/restart by controller'}
    if not a.execute:
        if a.prepare_output:
            if a.prepare_output.exists():p.error('Never overwrite prior receipt')
            leaf.dump(a.prepare_output,plan)
        print(json.dumps(plan,ensure_ascii=False,indent=2));return 0
    if not all([a.run_id,a.build_receipt,a.source_contract]):p.error('execute requires run-id/build-receipt/source-contract')
    sys.path[:0]=[str(D),str(D/'tests')]
    from file_dml_acceptance import check_build
    from lake_harness import validate_run_id
    from native_runtime import PROFILE,RUNTIME
    validate_run_id(a.run_id)
    if PROFILE!='debug':p.error('MainDebug baseline only')
    import fcntl
    out=ROOT/'公共文档/openGauss原生湖存储/验证'/a.run_id;out.mkdir(exist_ok=False)
    r={'status':'fail','scope':'serial SQL019 direct INSERT scoped correctness','plan':plan,'cases':[],
       'runtime_executed':False,'controller_database_cleanup_attempted':False}
    mutex=None
    try:
        mutex=(RUNTIME/'native-direct-campaign.lock').open('a+')
        fcntl.flock(mutex,fcntl.LOCK_EX|fcntl.LOCK_NB)
        build=check_build(a.build_receipt);leaf.source_contract(a.source_contract,build)
        identity=(build['source_head'],build['source_diff_sha256'],build['binary_sha256'])
        postmaster=(RUNTIME/'data/postmaster.pid').read_text().splitlines()[:3]
        controller_inputs={}
        for path in [Path(__file__),HERE/'native_direct_insert.py',D/'tests/native_table_create.py',D/'native_test_context.py',
                     HERE/'native_direct_matrix.json',a.build_receipt,a.source_contract]:
            target=out/('executed-'+path.name)
            shutil.copy2(path,target)
            controller_inputs[str(path.resolve())]={'sha256':leaf.digest(path),'archive':str(target)}
        r['controller_inputs']=controller_inputs;r['postmaster_pidfile_identity']=postmaster
        shutil.copy2(a.build_receipt,out/'official-build.json');shutil.copy2(a.source_contract,out/'reviewed-source-contract.json')
        for case in plan['cases']:
            before=check_build(a.build_receipt)
            if (RUNTIME/'data/postmaster.pid').read_text().splitlines()[:3]!=postmaster:
                raise RuntimeError('Runtime restarted between serialcases')
            for original,item in controller_inputs.items():
                if leaf.digest(original)!=item['sha256'] or leaf.digest(item['archive'])!=item['sha256']:
                    raise RuntimeError('Controller/leaf/source contract changed duringcampaign')
            if (before['source_head'],before['source_diff_sha256'],before['binary_sha256'])!=identity:
                raise RuntimeError('Campaign source/binary identity changed')
            child_id=a.run_id+'_'+case['mode'].replace('-','_');validate_run_id(child_id)
            child_out=ROOT/'公共文档/openGauss原生湖存储/验证'/child_id
            if child_out.exists():raise RuntimeError('Prior child evidence exists')
            command=[sys.executable,str(HERE/'native_direct_insert.py'),'--execute','--mode',case['mode'],
                     '--sql-version',case['sql_version'],'--run-id',child_id,'--build-receipt',str(a.build_receipt.resolve()),
                     '--source-contract',str(a.source_contract.resolve())]
            env=dict(os.environ);env['PYTHONDONTWRITEBYTECODE']='1';env['OG_LAKE_TEST_PROFILE']='debug'
            log=out/(case['mode']+'.stdout');timed=False;signals=[];started=time.monotonic()
            r['runtime_executed']=True
            with log.open('wb') as stream:
                child=subprocess.Popen(command,stdout=stream,stderr=subprocess.STDOUT,env=env,start_new_session=True)
                try:child.wait(timeout=a.case_timeout)
                except (subprocess.TimeoutExpired,KeyboardInterrupt):
                    timed=True
                    for sig,grace in [(signal.SIGTERM,5),(signal.SIGKILL,10)]:
                        if child.poll() is not None:break
                        try:os.killpg(child.pid,sig);signals.append(int(sig))
                        except ProcessLookupError:break
                        try:child.wait(timeout=grace)
                        except subprocess.TimeoutExpired:pass
            row={'mode':case['mode'],'command':command,'python_pid':child.pid,'returncode':child.returncode,
                 'hard_timeout':timed,'signals_to_owned_python_group':signals,'seconds':round(time.monotonic()-started,3),
                 'stdout':str(log),'stdout_sha256':leaf.digest(log),'result':str(child_out/'result.json'),'status':'fail'}
            r['cases'].append(row);leaf.dump(out/'index.json',r)
            if timed:raise RuntimeError('Owned Python child timedout; preserve DB/roles, no next fixture or controller cleanup')
            data=json.loads((child_out/'result.json').read_text())
            row['result_sha256']=leaf.digest(child_out/'result.json')
            if (child.returncode!=0 or data.get('status')!='pass' or data.get('executed') is not True
                    or data.get('mode')!=case['mode'] or data.get('sql_version')!=case['sql_version']
                    or data.get('binary_sha256')!=identity[2] or data.get('source_head')!=identity[0]
                    or data.get('source_diff_sha256')!=identity[1]):
                raise RuntimeError('Native child failed or source/binary/scope differs')
            row['assertions']=assertions(data);row['archived_inputs']=verify_archive(data)
            row['cleanup']=verify_cleanup(data,child_id,child_out,data['postmaster_start'])
            after=check_build(a.build_receipt)
            if (RUNTIME/'data/postmaster.pid').read_text().splitlines()[:3]!=postmaster:
                raise RuntimeError('Runtime restarted duringcase')
            if (after['source_head'],after['source_diff_sha256'],after['binary_sha256'])!=identity:
                raise RuntimeError('Source/binary changed aftercase')
            shutil.copy2(child_out/'result.json',out/(case['mode']+'-result.json'))
            row['status']='pass';leaf.dump(out/'index.json',r)
        r['status']='pass'
    except BaseException as exc:r.update(reason=str(exc),traceback=traceback.format_exc())
    finally:
        if mutex is not None:mutex.close()
        leaf.dump(out/'index.json',r)
    print(json.dumps({'status':r['status'],'cases':len(r['cases']),'index':str(out/'index.json'),'reason':r.get('reason')}))
    return 0 if r['status']=='pass' else 1

if __name__=='__main__':raise SystemExit(main())
