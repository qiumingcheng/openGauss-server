#!/usr/bin/env python3
"""Exclusive Debug GC18 campaign with official restart and exact config restore.

Preparation/AST is not execution. This runner never holds the reference driver
flock across child fixtures, never deletes by prefix and never treats a failed
child or retained owned object as a passing campaign.
"""
import argparse
import base64
import fcntl
import hashlib
import json
import os
import secrets
import re
from pathlib import Path
import shutil
import subprocess
import sys
import time
import traceback

# Prepare uses stdlib only; future typed execute must pass a closed-by-default
# exact contract gate before any Native helper import. Ordinary CLI is retained.
if __name__=='__main__' and ('--prepare' in sys.argv or '--execute' in sys.argv):
    private=argparse.ArgumentParser(description='Exact SQL26 typed automatic admission')
    private.add_argument('--prepare',action='store_true');private.add_argument('--execute',action='store_true')
    private.add_argument('--cases',nargs='+',choices=['rowhead-auto','rowhead-vacuum-reuse','manual','auto','vacuum','policy','relations'])
    private.add_argument('--source-contract',type=Path)
    private.add_argument('--sql-version');private.add_argument('--runtime-version')
    parsed,unknown=private.parse_known_args()
    if parsed.prepare:
        if parsed.execute or unknown:private.error('Prepare cannot execute or accept runtime arguments')
        print(json.dumps(dict(status='prepared_not_executed',Native=0,SDK=0,fixtures=0,
            cases=parsed.cases or ['rowhead-auto','rowhead-vacuum-reuse','manual','auto','vacuum','policy','relations'],ready_for_native_qualification=False)))
        raise SystemExit(0)
    if not parsed.source_contract or not parsed.cases or parsed.sql_version!='0.26' or parsed.runtime_version!='0.26':
        private.error('Typed execute requires source contract, explicit26/26 and typed subset')
    if set(parsed.cases)<={'rowhead-auto','rowhead-vacuum-reuse'}:
        from native_parent_sql026_admission import require_contract
    elif set(parsed.cases)<={'manual','auto','vacuum','policy','relations'}:
        from native_row_head_sql026_admission import require_contract
    else:private.error('Do not mix typed and ordinary case families in one contract')
    for mode in parsed.cases:require_contract(json.loads(parsed.source_contract.read_text()),mode,'0.26')
ROOT=Path(os.getenv('CPP_WORKSPACE','/home/qiumc/data/code/cpp')).resolve();D=ROOT/'integration/opengauss-native-lake' 
sys.path[:0]=[str(D),str(D/'tests')]
from file_dml_acceptance import check_build
from inline_fixture import query,scalar
from lake_harness import sha256,validate_run_id,write_json
from native_auto_flush import atomic_bytes,connect_control
from native_runtime import PROFILE,RUNTIME,SOURCE,INSTALL,PORT
from native_test_context import OwnedDatabase
from permission_api import cleanup_roles

SETTINGS=dict(enable_lake_maintenance='on',lake_flush_naptime='100',lake_flush_worker_budget='5000',
    lake_flush_max_batches='1',lake_flush_min_rows='1000000',lake_flush_max_delay='3600000')
MODES=['migration','manual','auto','fairness','retention','vacuum','lifecycle','cancel','protocol']
TYPED_AUTO_MODES=['rowhead-auto','rowhead-vacuum-reuse']
ORDINARY26_MODES=['manual','auto','vacuum','policy','relations']


def cleanup_timeout_record(exc, db, folder):
    """Record an unproven owned cleanup; never retry or change ownership here."""
    def safe(value):
        # TimeoutExpired can retain bytes even when subprocess.run(text=True).
        # Preserve every byte as base64, including invalid UTF-8, for JSON.
        if isinstance(value, bytes):
            return dict(encoding='base64', data=base64.b64encode(value).decode('ascii'), bytes=len(value))
        if isinstance(value, (list, tuple)):
            return [safe(item) for item in value]
        return value
    owner=Path(db.attempt)/'database-owner.json'
    record=dict(status='fail', kind='subprocess_timeout', returncode='timeout',
        run_id=db.run_id, attempt=str(Path(db.attempt).resolve()),
        argv=safe(exc.cmd), timeout_seconds=exc.timeout,
        owner_receipt=str(owner), owner_receipt_sha256=sha256(owner) if owner.exists() else None,
        partial_stdout=safe(exc.stdout), partial_stderr=safe(exc.stderr),
        cleanup_proven=False, cross_restart_cleanup_attempted=False,
        error_type=type(exc).__name__, error=str(exc))
    report=Path(folder)/('database-cleanup-timeout-'+str(time.time_ns())+'-'+str(os.getpid())+'.json')
    write_json(report,record)
    return dict(record, diagnostic_report=str(report), diagnostic_report_sha256=sha256(report))


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--run-id',required=True)
    p.add_argument('--execute',action='store_true',help='Explicit reviewed typed26 activation; ordinary modes unchanged')
    p.add_argument('--build-receipt',required=True,type=Path);p.add_argument('--budget-seconds',type=int,default=90)
    p.add_argument('--case-timeout',type=int,default=900)
    p.add_argument('--sql-version',choices=['0.18','0.23','0.26'],default='0.18')
    p.add_argument('--runtime-version',choices=['0.18','0.23','0.26'],default='0.18')
    p.add_argument('--source-contract',type=Path);p.add_argument('--sdk-qualification',type=Path);p.add_argument('--sdk-index-sha256')
    p.add_argument('--cases',nargs='+',choices=MODES+['policy','relations']+TYPED_AUTO_MODES,help='Explicit affected subset; default runs all cases')
    a=p.parse_args();validate_run_id(a.run_id)
    if a.cases and len(a.cases)!=len(set(a.cases)):p.error('Duplicate case names are not allowed')
    if a.sql_version=='0.23'and a.runtime_version!='0.23':p.error('SQL23 requires explicit runtime23')
    runtime26=(a.sql_version,a.runtime_version)==('0.26','0.26')
    typed=runtime26 and bool(a.cases) and set(a.cases)<=set(TYPED_AUTO_MODES)
    ordinary26=runtime26 and bool(a.cases) and set(a.cases)<=set(ORDINARY26_MODES)
    if runtime26 and not a.execute:p.error('SQL26 campaign requires explicit --execute and exact actual admission')
    if '0.26' in (a.sql_version,a.runtime_version) and not runtime26:p.error('Exact runtime26/SQL26 required')
    if runtime26 and not(typed or ordinary26):p.error('Choose one explicit typed or ordinary26 affected subset')
    if ordinary26 and not a.source_contract:p.error('Ordinary26 requires reviewed source/build/QA contract')
    if typed and(not a.cases or any(case not in TYPED_AUTO_MODES for case in a.cases) or not all([a.source_contract,a.sdk_qualification,a.sdk_index_sha256])):
        p.error('Typed worker requires explicit typed subset and reviewed source/SDK receipts')
    if not typed and a.cases and any(case in TYPED_AUTO_MODES for case in a.cases):p.error('Typed modes require26/26')
    new_admission=(a.sql_version,a.runtime_version)!=('0.18','0.18')
    if new_admission and not typed and not ordinary26 and(not a.cases or any(case not in ['auto','vacuum']for case in a.cases)):

        p.error('New runtime/SQL admission requires explicit --cases auto vacuum subset')
    supervision=None
    if new_admission:
        # Import definitions only. Its prepare/execute/main are never called.
        import native_ann_cache_lifecycle_campaign as supervision
        if sha256(Path(supervision.__file__))!='99fd39ddbc7cebc9988239552154e428ec1cef857054d9aef3e12f23bdb0b3e0':
            p.error('Reviewed process-group helper changed; independent review required')
    if PROFILE!='debug' or len(a.run_id)>60:p.error('Only owned Debug and a run ID of at most60 characters are allowed')
    if not 20<=a.budget_seconds<=180 or not 300<=a.case_timeout<=1800:p.error('Bounded observation20..180 and case timeout300..1800 required')
    out=ROOT/'公共文档/openGauss原生湖存储/验证'/a.run_id;out.mkdir(exist_ok=False)
    selected=[(mode,D/'tests/native_row_head_reclaim.py',['--mode',mode,'--budget-seconds',str(a.budget_seconds)])for mode in MODES]
    selected += [('policy',D/'tests/native_row_head_reclaim_policy.py',[]),('relations',D/'tests/native_row_head_reclaim_relations.py',['--budget-seconds',str(a.budget_seconds)])]
    if typed:
        selected=[(mode,D/'tests/native_parent_partition_mutation.py',[
            '--mode',mode,'--execute','--_child','--budget-seconds',str(a.budget_seconds),
            '--case-timeout',str(a.case_timeout),'--sql-version','0.26',
            '--source-contract',str(a.source_contract),'--sdk-qualification',str(a.sdk_qualification),
            '--sdk-index-sha256',a.sdk_index_sha256])for mode in TYPED_AUTO_MODES]
    if a.cases:
        by_name={x[0]:x for x in selected};selected=[by_name[name] for name in a.cases]
    if new_admission and not typed:
        selected=[(name,script,extra+['--sql-version',a.sql_version,'--runtime-version',a.runtime_version])for name,script,extra in selected]
    if ordinary26:
        selected=[(name,script,extra+['--execute','--source-contract',str(a.source_contract)])for name,script,extra in selected]
    r=dict(status='failed',scope=__doc__,profile=PROFILE,sql_version=a.sql_version,expected_runtime=a.runtime_version,cases=[],assertions=[],selected_cases=[x[0]for x in selected],
        limitations=['No dynamic primitive-internal ERROR/OOM, exact32 exhaustion, RAC, hard quota, performance or commercial qualification.'])
    inputs=[Path(__file__),D/'build.sh',D/'build_driver.py',D/'pq_native_build_inputs.py',D/'native_runtime.py',
        D/'lake_harness.py',D/'inline_fixture.py',D/'native_test_context.py',D/'tests/inline_cleanup.py',
        D/'tests/file_dml_acceptance.py',D/'tests/native_auto_flush.py',D/'tests/permission_api.py']+[x[1]for x in selected]
    if new_admission:inputs.append(Path(supervision.__file__))
    if typed:
        inputs += [a.source_contract,a.sdk_qualification,D/'tests/native_parent_sql026_admission.py',D/'tests/native_parent_sql026_expected.json']
        typed_contract=json.loads(a.source_contract.read_text())
        inputs += [Path(typed_contract['actual_full_build']['path']),Path(typed_contract['actual_sdk_install']['path'])]
        inputs += [D/'tests'/name for name in ['native_parent_rowhead_adapter.py','native_parent_rowhead_cases.py',
            'native_parent_rowhead_policy_cases.py','native_parent_rowhead_auto_cases.py','native_table_create.py']]

    if ordinary26:
        from native_row_head_sql026_admission import extra_inputs
        inputs+=extra_inputs(a)
    inputs=list(dict.fromkeys(inputs));r['inputs']={str(f):sha256(f)for f in inputs}
    archive=out/'executed-inputs';archive.mkdir()
    if typed or ordinary26:
        r['input_archives']={}
        for i,f in enumerate(inputs):
            dest=archive/(str(i)+'-'+f.name);shutil.copy2(f,dest);r['input_archives'][str(f)]=str(dest)
    else:
        for f in inputs:shutil.copy2(f,archive/f.name)
    shutil.copy2(a.build_receipt,archive/'original-build.json')
    control=campaign_lock=None;original=installed=None;changed=False;enable_started=False;restored=False;children=[];original_mode=None
    child_ownership_unproven=False
    restart_completion_unproven=False
    conf=RUNTIME/'data/postgresql.conf'
    r['pre_restore_connect']=dict(status='not_attempted')
    r['pre_restore_cleanup_attempts']=[]
    r['pre_restore_cleanup_retry']=dict(status='not_attempted',reason='Configuration restore has not reached its SQL admission step.')
    def check(label,got,want=True):
        ok=got==want;r['assertions'].append(dict(name=label,observed=got,expected=want,status='pass'if ok else'fail'));write_json(out/'index.json',r)
        if not ok:raise AssertionError(label+': '+repr(got))
    def run_owned_child(argv,label,log):
        nonlocal child_ownership_unproven
        nonce=secrets.token_hex(32);nonce_digest=hashlib.sha256(nonce.encode()).hexdigest()
        env=dict(os.environ,OG_LAKE_CACHE_OWNER_NONCE=nonce)
        # One campaign-owned session; never spawn the fixture's second supervisor.
        # Existing nonce/UID/PGID/SID/start_ticks checks and5+10s retirement remain below.
        if typed:env['OG_LAKE_PARENT_TEST_PARENT']=str(os.getpid())
        evidence=dict(case=label,deadline_seconds=a.case_timeout,nonce_sha256=nonce_digest,status='starting')
        if typed:evidence['campaign_pid']=os.getpid()
        r.setdefault('owned_child_processes',[]).append(evidence)
        child=record=None;retired=False;timed=False;proof_refused=False
        def save():write_json(out/'index.json',r)
        def current_members():
            nonlocal proof_refused
            if proof_refused:raise RuntimeError('Prior ownership proof failed; refuse recheck/signals')
            try:return supervision.members(record,nonce_digest)
            except BaseException:proof_refused=True;raise
        def retire_once():
            nonlocal retired,proof_refused
            if retired:raise RuntimeError('Owned group retirement budget already used')
            retired=True;evidence['retirement']={}
            try:supervision.stop_owned(child,record,nonce_digest,evidence['retirement'],save)
            except BaseException:proof_refused=True;raise
        started=time.monotonic()
        try:
            child=subprocess.Popen(argv,stdout=log,stderr=subprocess.STDOUT,env=env,start_new_session=True)
            # Persist PID before /proc identity lookup; never write plaintext nonce.
            evidence.update(pid=child.pid,status='running');save()
            try:record=supervision.spawn_identity(child,nonce_digest)
            except BaseException:proof_refused=True;raise
            evidence['identity']=record;save()
            while child.poll()is None:
                if time.monotonic()-started>=a.case_timeout:
                    timed=True;retire_once()
                    break
                time.sleep(.05)
            evidence['returncode']=child.poll();evidence['timed_out']=timed;save()
            live=current_members()
            orphaned=bool(live)
            if live and not retired:
                retire_once()
            live=current_members()
            evidence.update(returncode=child.poll(),remaining=live,drained=not live,status='drained');save()
            if live or child.poll()is None:
                raise RuntimeError('Owned child did not fully drain; preserve exact scene')
            saved_returncode=evidence['returncode']
            child=None  # All later decisions use the already persisted result.
            if timed:raise RuntimeError('Owned child exceeded original case deadline')
            if orphaned:raise RuntimeError('Direct child exited with living group members; case is not passing')
            return saved_returncode
        except BaseException as exc:
            evidence.update(status='failed',reason=str(exc));save()
            if child is not None:
                try:
                    if proof_refused or record is None:
                        raise RuntimeError('Ownership proof failed/incomplete; refuse recheck/signals/restart')
                    live=current_members()
                    if live and not retired:
                        retire_once()
                    live=current_members()
                    evidence.update(returncode=child.poll(),remaining=live,drained=not live);save()
                    if live or child.poll()is None:raise RuntimeError('Owned child remains unproven')
                except BaseException as cleanup_error:
                    child_ownership_unproven=True
                    evidence['ownership_refused']=str(cleanup_error);save()
            raise

    def connect():
        c=connect_control()
        try:
            query(c,'SET session_timeout=0');query(c,'SET statement_timeout=15000');return c
        except BaseException:
            c.close();raise
    def settings(c):
        return dict(query(c,"SELECT name,setting FROM pg_settings WHERE name IN ('enable_lake_maintenance','lake_flush_naptime','lake_flush_worker_budget','lake_flush_max_batches','lake_flush_min_rows','lake_flush_max_delay') ORDER BY name"))
    def quiet(c,label,postgres_only=True):
        own=scalar(c,'SELECT pg_backend_pid()')
        check(label+' has no unrelated frontend',query(c,'SELECT pid,datid,application_name FROM pg_stat_activity WHERE client_port IS NOT NULL AND pid<>%s',(own,)),[])
        if postgres_only:check(label+' has only postgres business database',query(c,'SELECT datname FROM pg_database WHERE NOT datistemplate ORDER BY datname'),[('postgres',)])
    def dep_args(build):
        args=[]
        for key,flag in [('roaring','--roaring-build'),('pq','--pq-build')]:
            before=build.get(key+'_inputs_before');after=build.get(key+'_inputs_after')
            if before!=after:raise AssertionError('Original dependency changed during its official build: '+key)
            if before is None:
                if key=='roaring':raise AssertionError('Native bitmap campaign requires its explicit qualified CRoaring receipt')
                continue
            receipt=Path(before['receipt']).resolve(strict=True)
            if sha256(receipt)!=before['receipt_sha256']:raise AssertionError('Qualified dependency receipt changed: '+key)
            shutil.copy2(receipt,archive/(key+'-build.json'))
            r.setdefault('dependency_receipts',{})[key]=dict(path=str(receipt),sha256=sha256(receipt))
            args += [flag,str(receipt)]
        return args
    def restart(label):
        nonlocal restart_completion_unproven
        name=a.run_id+'_'+label
        argv=[sys.executable,str(D/'build_driver.py'),'debug','restart','--run-id',name]+dependencies
        driver=None;process_record=None
        if new_admission:
            # The official driver controls service processes. This is not a
            # fixture group: never signal it or its gaussdb/gs_ctl descendants.
            restart_completion_unproven=True
            r['restart_completion_unproven']=True
            process_record=dict(argv=argv,deadline_seconds=240,status='starting',started_monotonic=time.monotonic())
            r.setdefault('official_restart_processes',{})[label]=process_record
            write_json(out/'index.json',r)
        try:
            with(out/(label+'.log')).open('w')as log:
                if new_admission:
                    driver=subprocess.Popen(argv,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
                    process_record.update(pid=driver.pid,status='running')
                    write_json(out/'index.json',r) # PID before any identity lookup.
                    proc_path=Path('/proc')/str(driver.pid)
                    stat=(proc_path/'stat').read_text();tail=stat[stat.rfind(')')+2:].split()
                    observed=[os.fsdecode(value)for value in(proc_path/'cmdline').read_bytes().split(b'\0')if value]
                    identity=dict(pid=driver.pid,pgid=int(tail[2]),sid=int(tail[3]),start_ticks=int(tail[19]),uid=proc_path.stat().st_uid,argv=observed)
                    process_record['identity']=identity;write_json(out/'index.json',r)
                    if identity['uid']!=os.getuid()or identity['pgid']!=driver.pid or identity['sid']!=driver.pid or observed!=argv:
                        raise RuntimeError('Official restart driver process identity differs; preserve scene')
                    remaining=240-(time.monotonic()-process_record['started_monotonic'])
                    if remaining<=0:raise subprocess.TimeoutExpired(argv,240)
                    driver.wait(timeout=remaining) # Popen.wait never kills on timeout.
                    process_record.update(returncode=driver.returncode,status='driver_exited')
                    write_json(out/'index.json',r)
                    proc=driver
                else:
                    proc=subprocess.run(argv,stdout=log,stderr=subprocess.STDOUT,timeout=240)
            path=ROOT/'公共文档/openGauss原生湖存储/验证'/name/'build.json'
            record=dict(argv=argv,returncode=proc.returncode,receipt=str(path),receipt_sha256=sha256(path)if path.exists()else None)
            r.setdefault('restarts',{})[label]=record;write_json(out/'index.json',r)
            if proc.returncode:raise AssertionError('Official restart failed: '+label)
            found=check_build(path)
            if found['binary_sha256']!=built['binary_sha256']:raise AssertionError('Restart changed compiled Native binary')
            for key in ['roaring','pq']:
                if found.get(key+'_inputs_before')!=built.get(key+'_inputs_before') or found.get(key+'_inputs_after')!=built.get(key+'_inputs_after'):
                    raise AssertionError('Restart did not retain exact qualified dependency: '+key)
            if new_admission:
                for key in ['source_head','source_diff_sha256','native_source_inputs_after','build_wrapper_inputs_after']:
                    if found.get(key)!=built.get(key):raise AssertionError('Restart changed original compiled input: '+key)
                # Source/wrapper/dependency/binary admission comes from the
                # unchanged official receipt validator, not process exit alone.
                process_record.update(status='official_receipt_verified',receipt=str(path),receipt_sha256=sha256(path))
                restart_completion_unproven=False;r['restart_completion_unproven']=False
                write_json(out/'index.json',r)
            if ordinary26:
                from native_row_head_sql026_admission import require_actual
                restart_args=argparse.Namespace(**vars(a));restart_args.build_receipt=path
                require_actual(restart_args,a.cases[0],found)
            return path
        except BaseException as exc:
            if new_admission:
                restart_completion_unproven=True
                if process_record is not None:
                    process_record.update(status='completion_unproven',error_type=type(exc).__name__,reason=str(exc),returncode=driver.poll()if driver is not None else None)
                r.update(restart_completion_unproven=True,configuration_restored=False,
                    preserve_owned_scene=True,restoration_pending_root=True,
                    restoration_error='Official restart completion unproven; do not overwrite configuration, start restore or signal service descendants')
                write_json(out/'index.json',r)
            raise
    def verify_child(path,run):
        if typed:
            from native_parent_rowhead_adapter import verify_typed_child
            return verify_typed_child(path,run,built,enabled_receipt,a.sql_version,a.runtime_version,r['owned_child_processes'][-1])
        result=json.loads(path.read_text());require=lambda v,m:None if v else (_ for _ in ()).throw(AssertionError(m))
        require(result.get('status')=='pass','Original child result failed: '+run)
        if new_admission:
            require(result.get('sql_version')==a.sql_version and result.get('runtime_module')==a.runtime_version,
                'Child actual runtime/installed SQL does not match explicit selection')
        require(result.get('source_head')==built['source_head']and result.get('binary_sha256')==built['binary_sha256'],'Wrong child product binary')
        require(result.get('build_sha256')==sha256(enabled_receipt),'Child did not use the official enabled receipt')
        if ordinary26:
            from native_row_head_sql026_admission import verify_child_admission
            require(result.get('mode')==r['owned_child_processes'][-1]['case'],'Wrong ordinary child mode')
            verify_child_admission(result,a.source_contract,path.parent)
        require(bool(result.get('assertions')),'Child has no actual assertions')
        for v in result['assertions']:require(v['status']=='pass'and v['observed']==v['expected'],'Child contains failed/mismatched assertion')
        require(bool(result.get('inputs')),'Child lacks executed input identities')
        for name,wanted in result['inputs'].items():
            require(sha256(Path(name))==wanted and sha256(path.parent/'executed-inputs'/Path(name).name)==wanted,'Child current/archive input changed: '+name)
        require(sha256(path.parent/'executed-inputs/build.json')==sha256(enabled_receipt),'Child archived wrong official build')
        cleanup=result.get('database_cleanup',{});require(cleanup.get('returncode')==0,'Child database cleanup failed')
        summary=json.loads(cleanup['stdout']);require(summary.get('status')=='pass','Child cleanup summary failed')
        owner=json.loads((path.parent/'database-owner.json').read_text());require(owner['run_id']==run==summary['run_id'],'Child exact owned run mismatch')
        require(owner['database_oid']==summary.get('dropped_database_oid'),'Child exact database OID cleanup is missing')
        immutable=Path(summary['immutable_report']);require(immutable.parent.resolve()==path.parent.resolve(),'Cleanup immutable report escaped attempt')
        detail=json.loads(immutable.read_text());require(detail['status']=='pass'and detail['dropped_database_oid']==owner['database_oid'],'Immutable owned cleanup proof mismatch')
        require(result.get('role_cleanup',{}).get('status')=='pass','Child role cleanup failed')
        return dict(result_sha256=sha256(path),assertions=len(result['assertions']),cleanup_report=str(immutable),cleanup_report_sha256=sha256(immutable))
    def retry_child_cleanup_before_restart():
        # Original child FAIL is immutable. Any additional cleanup has its own
        # report and is accepted only under the SAME postmaster identity.
        observations=[]
        r['pre_restore_cleanup_retry']=dict(status='running',child_count=len(children))
        write_json(out/'index.json',r)
        for run,folder in children:
            owner=folder/'database-owner.json';roles=folder/'permission-roles.json'
            if not owner.exists():continue
            item=dict(run_id=run,owner_receipt=str(owner),owner_receipt_sha256=sha256(owner))
            db=None
            try:
                data=json.loads(owner.read_text())
                if data['run_id']!=run or data['attempt']!=str(folder.resolve()):raise AssertionError('Cleanup ownership receipt changed')
                result_path=folder/'result.json';child=json.loads(result_path.read_text())if result_path.exists()else{}
                db_done=child.get('database_cleanup',{}).get('returncode')==0
                role_done=child.get('role_cleanup',{}).get('status')=='pass'
                observed=query(control,"SELECT oid,datname,datdba,shobj_description(oid,'pg_database') FROM pg_database WHERE oid=%s OR datname=%s",(data['database_oid'],data['database']))
                if observed:
                    if db_done:raise AssertionError('Previously removed child DB unexpectedly reappeared')
                    expected=(data['database_oid'],data['database'],data['owner_oid'],data['marker'])
                    if observed!=[expected]:raise AssertionError('Owned DB identity changed before retry cleanup')
                    db=OwnedDatabase(run,folder);db.database=data['database'];db.allocation=data;db.cleanup();item['database_cleanup']=db.cleanup_result
                else:item['database_already_absent']=True
                if roles.exists():
                    if role_done:
                        for role in json.loads(roles.read_text())['roles']:
                            if query(control,'SELECT oid,rolname FROM pg_roles WHERE oid=%s OR rolname=%s',(role['oid']or 0,role['name'])):raise AssertionError('Previously removed child role unexpectedly reappeared')
                        item['role_cleanup_already_passed']=True
                    else:item['role_cleanup']=cleanup_roles(control,roles,run)
                item['status']='passed'
            except subprocess.TimeoutExpired as exc:
                item.update(status='failed',reason=str(exc))
                if db is not None:item['database_cleanup']=cleanup_timeout_record(exc,db,folder)
            except BaseException as exc:item.update(status='failed',reason=str(exc),error_type=type(exc).__name__)
            observations.append(item)
            r['pre_restore_cleanup_attempts']=observations
            write_json(out/'index.json',r)
        failed=any(x['status']!='passed'for x in observations)
        r['pre_restore_cleanup_retry']=dict(status='failed' if failed else 'passed',child_count=len(children),attempt_count=len(observations))
        if failed:r.update(status='failed',cleanup_error='Owned child cleanup incomplete before configuration restore')
        write_json(out/'index.json',r)
    try:
        built=check_build(a.build_receipt)
        if typed:
            from native_parent_rowhead_adapter import require_typed_campaign_admission
            # Exact projection/build/SDK/adapters must be sealed BEFORE config mutation.
            # The private implementation intentionally refuses all Native activation.
            require_typed_campaign_admission(a,built)
        if ordinary26:
            from native_row_head_sql026_admission import require_campaign,archive_receipts
            r['ordinary26_admission']=archive_receipts(out,*require_campaign(a,built))
        if new_admission:
            versions=re.findall(r"^default_version\s*=\s*'([^']+)'",(SOURCE/'contrib/og_lake/og_lake.control').read_text(),re.M)
            check('official source module matches selected runtime',versions,[a.runtime_version])
        dependencies=dep_args(built)
        r.update(source_head=built['source_head'],binary_sha256=built['binary_sha256'],original_build=str(a.build_receipt.resolve()),original_build_sha256=sha256(a.build_receipt),dependency_args=dependencies)
        owner=json.loads((RUNTIME/'owner.json').read_text())
        check('exact owned Debug runtime identity',owner,dict(project='opengauss-native-lake',source=str(SOURCE),data=str(RUNTIME/'data'),install=str(INSTALL),profile='debug',port=PORT))
        campaign_lock=(RUNTIME/'.row-head-reclaim-campaign.lock').open('a+');fcntl.flock(campaign_lock.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB)
        control=connect();check('owned data directory',str(Path(scalar(control,'SHOW data_directory')).resolve()),str((RUNTIME/'data').resolve()))
        check('controller can inspect every role frontend before restart',scalar(control,'SELECT rolsuper FROM pg_roles WHERE rolname=current_user'),True)
        quiet(control,'pre-enable');before=settings(control);check('automatic maintenance originally off',before['enable_lake_maintenance'],'off')
        r['original_settings']=before;r['postmaster_start_before']=str(scalar(control,'SELECT pg_postmaster_start_time()'))
        original=conf.read_bytes();original_mode=conf.stat().st_mode&0o777
        backup=RUNTIME/('row-head-reclaim-config-'+a.run_id);backup.mkdir(mode=0o700)
        saved=backup/'original-postgresql.conf';saved.write_bytes(original);saved.chmod(0o600)
        r['private_original_config']=str(saved);r['original_config_sha256']=hashlib.sha256(original).hexdigest()
        installed=original+('\n# owned row-head reclaim campaign '+a.run_id+'\n'+''.join(k+' = '+v+'\n'for k,v in SETTINGS.items())).encode()
        r['temporary_config_sha256']=hashlib.sha256(installed).hexdigest()
        control.close();control=None;changed=True;atomic_bytes(conf,installed,original_mode)
        enable_started=True;enabled_receipt=restart('enable')
        control=connect();check('temporary settings are exact',settings(control),SETTINGS)
        r['postmaster_start_enabled']=str(scalar(control,'SELECT pg_postmaster_start_time()'));control.close();control=None
        for label,script,extra in selected:
            control=connect();quiet(control,'before '+label);check('temporary config still exact before '+label,sha256(conf),r['temporary_config_sha256']);control.close();control=None
            run=a.run_id+'_'+label;folder=ROOT/'公共文档/openGauss原生湖存储/验证'/run;children.append((run,folder))
            argv=[sys.executable,str(script),'--run-id',run,'--build-receipt',str(enabled_receipt)]+extra
            with(out/(label+'.log')).open('w')as log:
                if new_admission:
                    child_supervision_error=None
                    try:returncode=run_owned_child(argv,label,log)
                    except BaseException as child_error:
                        child_supervision_error=str(child_error)
                        returncode=r['owned_child_processes'][-1].get('returncode','unknown')
                else:
                    try:proc=subprocess.run(argv,stdout=log,stderr=subprocess.STDOUT,timeout=a.case_timeout);returncode=proc.returncode
                    except subprocess.TimeoutExpired:returncode='timeout'
            result=folder/'result.json';item=dict(case=label,run_id=run,argv=argv,returncode=returncode,result=str(result),result_sha256=sha256(result)if result.exists()and not child_ownership_unproven else None,status='failed')
            if new_admission and child_supervision_error is not None:item['owned_supervision_error']=child_supervision_error
            r['cases'].append(item);write_json(out/'index.json',r)
            if new_admission and child_supervision_error is not None:
                raise AssertionError('Original child supervision failed; matrix stopped: '+label+': '+child_supervision_error)
            if returncode!=0:raise AssertionError('Original child process failed; matrix stopped: '+label)
            item.update(verify_child(result,run));item['status']='passed';write_json(out/'index.json',r)
        control=connect();quiet(control,'after all children');control.close();control=None
        check('every campaign input unchanged',all(sha256(Path(f))==wanted for f,wanted in r['inputs'].items()))
        if typed or ordinary26:check('every version26 campaign archive unchanged',all(sha256(Path(r['input_archives'][f]))==wanted for f,wanted in r['inputs'].items()))
        r['status']='passed'
    except BaseException as exc:r.update(status='failed',reason=str(exc),traceback=traceback.format_exc())
    finally:
        if control is not None:control.close();control=None
        if changed and(child_ownership_unproven or restart_completion_unproven):
            # Do not restart a server or retry DB/role cleanup while unknown
            # Python-group ownership can still execute against this scene.
            r.update(status='failed',configuration_restored=False,preserve_owned_scene=True,
                restoration_pending_root=True,restoration_error='Child ownership or official restart completion unproven; preserve exact configuration/scene for root restoration')
        elif changed:
            try:
                # Restoring bytes must not depend on a successful SQL connect:
                # a failed enable restart may have left the instance stopped.
                current=conf.read_bytes()
                if current not in (original,installed):raise AssertionError('Configuration changed externally; no overwrite')
                atomic_bytes(conf,original,original_mode)
                try:
                    control=connect()
                    r['pre_restore_connect']=dict(status='passed')
                except Exception as exc:
                    control=None
                    r['pre_restore_connect']=dict(status='failed',error_type=type(exc).__name__,
                        error=str(exc),error_repr=repr(exc),sqlstate=getattr(exc,'pgcode',None),traceback=traceback.format_exc())
                    r['pre_restore_cleanup_retry']=dict(status='skipped',reason='pre_restore_connect_failed',child_count=len(children),
                        detail='SQL unavailable under the original postmaster identity; no cleanup retry or cross-restart deletion attempted.')
                    if children:r.update(status='failed',cleanup_error='SQL unavailable before restore; original child ownership retained, no cross-restart deletion')
                write_json(out/'index.json',r)
                if control is not None:retry_child_cleanup_before_restart()
                # A surviving unrelated session is never an authorized restart
                # target. Child-owned leftovers remain reported, never silently
                # rebound to a new postmaster owner receipt for deletion.
                if control is not None:
                    own=scalar(control,'SELECT pg_backend_pid()');unrelated=[]
                    allowed={json.loads((folder/'database-owner.json').read_text())['database_oid']for _,folder in children if(folder/'database-owner.json').exists()}
                    for row in query(control,'SELECT pid,datid,application_name FROM pg_stat_activity WHERE client_port IS NOT NULL AND pid<>%s',(own,)):
                        if row[1]not in allowed or not str(row[2]).startswith(('og_lake_owned_','og_lake_permissions_login')):unrelated.append(row)
                    if unrelated:raise AssertionError('Unrelated frontend arrived before restore; no restart performed: '+repr(unrelated))
                    r['databases_before_restore']=query(control,'SELECT oid,datname FROM pg_database WHERE NOT datistemplate ORDER BY oid')
                    r['postmaster_start_cleanup']=str(scalar(control,'SELECT pg_postmaster_start_time()'))
                    control.close();control=None
                if enable_started:
                    restored_receipt=restart('restore');r['restore_receipt']=str(restored_receipt)
                control=connect();check('configuration restored byte for byte',sha256(conf),r['original_config_sha256'])
                check('original effective settings restored',settings(control),r['original_settings'])
                r['postmaster_start_restored']=str(scalar(control,'SELECT pg_postmaster_start_time()'))
                if enable_started:check('restore created a fresh postmaster identity',r['postmaster_start_restored']!=r.get('postmaster_start_enabled',r['postmaster_start_before']))
                restored=True;r['configuration_restored']=True
                leftovers=query(control,'SELECT oid,datname FROM pg_database WHERE NOT datistemplate AND datname<>%s ORDER BY oid',('postgres',));r['retained_owned_databases']=leftovers
                if leftovers:r.update(status='failed',cleanup_error='Owned DB cleanup incomplete before restart; retained with original receipts, no cross-restart deletion')
            except BaseException as exc:r.update(status='failed',restoration_error=str(exc),restoration_traceback=traceback.format_exc(),configuration_restored=False)
        if control is not None:control.close()
        if campaign_lock is not None:campaign_lock.close()
        r['completed_cases']=len([x for x in r['cases']if x['status']=='passed']);r['case_count']=len(r['cases'])
        r['assertion_count']=sum(x.get('assertions',0)for x in r['cases'])
        r['remaining_cases']=[x for x in r['selected_cases']if x not in {c['case']for c in r['cases']if c['status']=='passed'}]
        r['unattempted_cases']=[x for x in r['selected_cases']if x not in {c['case']for c in r['cases']}]
        if changed and not restored:r['status']='failed'
        write_json(out/'index.json',r)
    print(str(out/'index.json')+': '+r['status']);return 0 if r['status']=='passed'else 1


if __name__=='__main__':sys.exit(main())
