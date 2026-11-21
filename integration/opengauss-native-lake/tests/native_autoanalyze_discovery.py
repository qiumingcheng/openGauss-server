#!/usr/bin/env python3
"""Prepared Native auto-ANALYZE64-row discovery regression.

No work without --execute. Root supplies enabled dedicated Debug workers and
an exact official selector-fix build/SDK.70 real tables are first analyzed by
the worker; only normal business UPDATE makes the newest-attempt table due.
No analyze_state/pg_class/pg_statistic values are written by this test.
Run under an owned external process-group deadline at least whole-seconds+90.
"""
import argparse
from copy import deepcopy
import hashlib
import json
import math
import os
from pathlib import Path
import shutil
import struct
import subprocess
import sys
import time
import traceback
from types import SimpleNamespace

ROOT=Path(os.environ.get('CPP_WORKSPACE','/home/qiumc/data/code/cpp')).resolve()
D=ROOT/'integration/opengauss-native-lake'
SELECTOR_SHA='e0b1225f2852b1cf6ac1571f331080b4a7d5ff89d399c446b9291d564c61c9a9'


def safe(value):
    if isinstance(value,Path):return str(value)
    if isinstance(value,dict):return {str(k):safe(v)for k,v in value.items()}
    if isinstance(value,(tuple,list)):return [safe(v)for v in value]
    return value


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--execute',action='store_true');p.add_argument('--run-id')
    p.add_argument('--build-receipt',type=Path);p.add_argument('--sdk-index',type=Path);p.add_argument('--sdk-index-sha256')
    p.add_argument('--tables',type=int,default=70);p.add_argument('--initial-seconds',type=int,default=900)
    p.add_argument('--target-seconds',type=int,default=180);p.add_argument('--whole-seconds',type=int,default=1800)
    a=p.parse_args()
    if not 70<=a.tables<=128 or not 600<=a.initial_seconds<=1800 or not 90<=a.target_seconds<=300:
        p.error('tables70..128, first-analysis600..1800s, target90..300s are explicit new-case contracts')
    if a.whole_seconds<a.initial_seconds+a.target_seconds+600:
        p.error('whole-seconds must include first-analysis+target+600s fixture/DDL/cleanup allowance')
    if not a.execute:
        print(json.dumps(dict(status='prepared_not_executed',tables=a.tables,rows_per_table=100,
            first_analysis_seconds=a.initial_seconds,target_seconds=a.target_seconds,whole_seconds=a.whole_seconds,
            selector_sha256=SELECTOR_SHA,Native=0,SDK=0,fixtures=0,
            requirements=['NormalCREATE/INSERT then real worker analyzes everytable','Session maintenance claim freezes candidate order; choose latest last_attempt with>=64 earlier notdue tables',
                'Business UPDATE alone raises exact committed counter above original float4 threshold','Worker must reach target across64row discovery page while earlier attempts remain unchanged',
                'Existing8case90s tests unchanged; root serial fixtures,enabled worker,indexmaintenanceoff,exactSDK and official build; no restart/configuration by leaf'])))
        return 0
    if not all([a.run_id,a.build_receipt,a.sdk_index,a.sdk_index_sha256]):p.error('execute requires run-id/build-receipt/sdk-index+exactSHA')
    sys.path[:0]=[str(D),str(D/'tests')]
    from file_dml_acceptance import check_build
    from inline_fixture import query,scalar
    from lake_harness import sha256,validate_run_id,write_json
    from native_auto_flush import connect_control
    from native_runtime import PROFILE,SOURCE,INSTALL
    from native_test_context import OwnedDatabase,reviewed_runtime_version
    from permission_api import OwnedRoles,cleanup_roles
    from empty_manifest import validate_empty_manifest
    from pq_sql020_build_inputs import sdk_index
    from psycopg2 import sql
    import fcntl
    validate_run_id(a.run_id)
    if PROFILE!='debug' or len(a.run_id)>80:p.error('Owned Debug only; run-id<=80 preserves fixture suffix budget')
    out=ROOT/'公共文档/openGauss原生湖存储/验证'/a.run_id;out.mkdir(exist_ok=False)
    result=dict(status='fail',mode='discovery64',tables=a.tables,assertions=[],executed=False,
        contracts=dict(initial_seconds=a.initial_seconds,target_seconds=a.target_seconds,whole_seconds=a.whole_seconds),
        scope=__doc__)
    started=time.monotonic();control=db=roles=admin=observer=reservation=reference_lock=None
    claimed=claim_ready=False;cleanup_safe=True;relations=[]

    def check(label,got,want=True):
        got,want=safe(got),safe(want);ok=got==want
        result['assertions'].append(dict(name=label,observed=deepcopy(got),expected=deepcopy(want),status='pass'if ok else'fail'))
        write_json(out/'result.json',result)
        if not ok:raise AssertionError(label)
    def remaining():return a.whole_seconds-(time.monotonic()-started)
    def wait(label,predicate,seconds,retiring=False):
        deadline=time.monotonic()+seconds;last=None
        while time.monotonic()<deadline:
            if not retiring and remaining()<=0:raise TimeoutError('whole-case contract expired/'+label)
            last=predicate()
            if last:return last
            time.sleep(.2)
        result.setdefault('wait_failures',[]).append(dict(label=label,last=safe(last)))
        raise TimeoutError(label)
    def claim(retiring=False):
        nonlocal claimed
        wait('exact DB native maintenance claim',lambda:scalar(reservation,'SELECT og_lake_internal.test_maintenance_claim()'),90,retiring)
        claimed=True
    def release():
        nonlocal claimed
        check('release exact owned DB maintenance claim',scalar(reservation,'SELECT og_lake_internal.test_maintenance_release()'))
        claimed=False
    def records():
        # Client observer is not an elevated LakeMetadataQuery scope. Host
        # fields here are observation only; the production selector uses cache.
        return query(observer,
            'SELECT r.relation_oid,s.committed_changes,s.analyzed_changes,'
            's.last_success IS NOT NULL,s.last_attempt::text,s.last_success::text,'
            's.retry_after IS NOT NULL,p.reltuples,p.relpages,'
            "(s.last_success>pg_catalog.clock_timestamp()-%s*interval '1 millisecond') "
            'FROM og_lake_internal.relation_binding r JOIN og_lake_internal.analyze_state s USING(binding_id) '
            'JOIN pg_catalog.pg_class p ON p.oid=r.relation_oid '
            'WHERE r.relation_oid=ANY(%s::oid[]) '
            'ORDER BY s.last_attempt ASC NULLS FIRST,r.relation_oid',(max_delay,relations))
    def complete():
        rows=records()
        result['last_initial_poll']=dict(rows=len(rows),completed=sum(bool(x[3])and x[1]==x[2]for x in rows))
        return rows if len(rows)==a.tables and all(x[3]and x[1]==x[2]for x in rows)else None
    def session(c):
        for text in ['SET session_timeout=0','SET statement_timeout=30000','SET query_dop=1',
                     'SET enable_lake_smp=off','SET enable_vector_engine=off','SET default_statistics_target=100']:
            query(c,text)

    try:
        build=check_build(a.build_receipt)
        check('official exact native selector source',build['native_source_inputs_after'].get('contrib/og_lake/lake_autoanalyze.cpp'),SELECTOR_SHA)
        qualified=sdk_index(a.sdk_index,a.sdk_index_sha256,ROOT)
        sdk=INSTALL/'lib/postgresql/og_lake_sdk';owner=json.loads((sdk/'owner.json').read_text())
        check('SDK prefix itself is canonical',not sdk.is_symlink()and sdk.resolve()==sdk)
        check('SDK exact owned project/phase',(owner['project'],owner['phase']),('opengauss-native-lake-local-provider','prepared-for-atomic-install'))
        payload={str(x.relative_to(sdk)):sha256(x)for x in sdk.rglob('*')if x.is_file()and x!=sdk/'owner.json'}
        check('SDK has no aliases or special files',all(not x.is_symlink()and(x.is_file()or x.is_dir())for x in sdk.rglob('*')))
        check('whole SDK inventory exact',payload,owner['files']);check('actual qualified provider',payload['libog_lake_sdk_arrow.so'],qualified['library_sha256'])
        check('exact SDK selected index',owner['qualification_sha256'],a.sdk_index_sha256)
        check('SDK qualification kind',owner['qualification_kind'],'verified_independent_sdk_sql020_int16_only')
        check('SDK owner exact target',str(Path(owner['target']).resolve()),str(sdk.resolve()))
        public={str(SOURCE/'contrib/og_lake'/n):sha256(SOURCE/'contrib/og_lake'/n)for n in ['og_lake.control','sdk/lake_parquet_sdk.h','vendor/arrow20/abi.h']}
        check('SDK owner native public identity',owner['native_public_headers'],public)
        check('Native full SDK header matches qualified component',public[str(SOURCE/'contrib/og_lake/sdk/lake_parquet_sdk.h')],qualified['header_sha256'])
        check('installed SDK full public header matches qualified component',payload['include/og_lake/lake_parquet_sdk.h'],qualified['header_sha256'])
        result.update(build=str(a.build_receipt.resolve()),build_sha256=sha256(a.build_receipt),binary_sha256=build['binary_sha256'],sdk_qualification=qualified)
        control=connect_control();session(control)
        for name,value in [('enable_thread_pool','off'),('enable_lake_maintenance','on'),('enable_lake_autoanalyze','on'),('enable_lake_index_maintenance','off')]:check('prerequisite/'+name,scalar(control,'SHOW '+name),value)
        check('original worker30s budget',scalar(control,"SELECT setting::int FROM pg_settings WHERE name='lake_autoanalyze_budget'"),30000)
        max_delay=scalar(control,"SELECT setting::int FROM pg_settings WHERE name='lake_autoanalyze_max_delay'")
        threshold=int(scalar(control,'SHOW autovacuum_analyze_threshold'))
        scale=struct.unpack('f',struct.pack('f',float(scalar(control,'SHOW autovacuum_analyze_scale_factor'))))[0]
        result['settings']=dict(max_delay_ms=max_delay,threshold=threshold,scale_float4=scale)
        manifest=out/'fixture/manifest.json';data_root=manifest.parent/'empty-reference-generation/data'
        command=[sys.executable,str(D/'tests/empty_reference_fixture.py'),'--run-id',a.run_id,'--output',str(manifest),'--data-root',str(data_root)]
        with(out/'fixture.log').open('w')as log:subprocess.run(command,stdout=log,stderr=subprocess.STDOUT,check=True,timeout=300)
        empty,_=validate_empty_manifest(manifest,SimpleNamespace(run_id=a.run_id))
        check('genuine fresh EMPTY has no existing data directory',not Path(empty['data_path']).exists())
        check('owned EMPTY path remains under cleanup owner',Path(empty['data_path']).resolve().is_relative_to(Path(empty['cleanup_owner']).resolve()))
        reference_lock=(ROOT/'.reference/opengauss-native-lake/.driver.lock').open('a+')
        fcntl.flock(reference_lock.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB)
        result['reference_reservation']=dict(path=str(reference_lock.name),acquired=True)
        inputs=[Path(__file__),a.build_receipt,a.sdk_index,manifest,sdk/'owner.json']
        inputs +=[D/n for n in ['taskctl.py','native_test_context.py','native_runtime.py','inline_fixture.py','lake_harness.py','source-lock.json','reference.sh','pq_sql020_build_inputs.py','pq_native_build_inputs.py']]
        inputs +=[D/'tests'/n for n in ['native_autoanalyze.py','file_dml_acceptance.py','native_auto_flush.py','permission_api.py','inline_cleanup.py','empty_manifest.py','empty_reference_fixture.py','empty_catalog_oracle.py','reverse_reference.py']]
        inputs +=[SOURCE/n for n in build['native_source_inputs_after']]+[Path(n)for n in build['build_wrapper_inputs_after']]
        inputs +=[Path(x['path'])for x in empty['files']]
        archive=out/'executed-inputs';archive.mkdir();result['inputs']={}
        for i,path in enumerate(dict.fromkeys(x.resolve()for x in inputs)):
            saved=archive/(str(i)+'-'+path.name);shutil.copy2(path,saved)
            check('input archive bytes/'+str(i),sha256(saved),sha256(path));result['inputs'][str(path)]=dict(sha256=sha256(path),archive=str(saved))
        result['hash_only_inputs']={str(x.resolve()):sha256(x)for x in [INSTALL/'bin/gaussdb']+[sdk/n for n in payload]}
        db=OwnedDatabase(a.run_id,out);db.create();roles=OwnedRoles(control,a.run_id,out)
        admin,observer,reservation=[db.connect()for _ in range(3)]
        for c in [admin,observer,reservation]:session(c)
        query(admin,"CREATE EXTENSION og_lake VERSION '0.21'")
        check('actual module equals reviewed source',json.loads(scalar(admin,'SELECT og_lake.host_status()'))['module_version'],reviewed_runtime_version())
        check('explicit SQL21 installed',scalar(admin,"SELECT extversion FROM pg_extension WHERE extname='og_lake'"),'0.21')
        private=roles.create('private',False);query(admin,'SELECT og_lake.configure_internal_owner(%s::name)',(private,))
        claim_ready=True;claim()
        query(admin,'SELECT og_lake.create_catalog(%s::name,%s::name,%s::text,NULL::text)',('created_lake','created_metadata',empty['data_path']))
        query(admin,'CREATE SCHEMA biz');query(admin,'CREATE TABLE biz.truth(table_no integer,id integer,val integer)')
        query(admin,"SELECT og_lake.set_option('created_lake'::name,'data_inlining_row_limit','10000')")
        names={}
        for number in range(a.tables):
            if remaining()<=0:raise TimeoutError('whole-case contract during normalCREATE')
            name='lake_'+str(number).zfill(3)
            query(admin,sql.SQL("CREATE FOREIGN TABLE biz.{}(id integer,val integer) SERVER created_lake OPTIONS(binding_mode 'create',lake_schema 'main')").format(sql.Identifier(name)))
            query(admin,'BEGIN')
            query(admin,sql.SQL('INSERT INTO biz.{} SELECT i,i FROM generate_series(1,100)i').format(sql.Identifier(name)))
            query(admin,'INSERT INTO biz.truth SELECT %s,i,i FROM generate_series(1,100)i',(number,));query(admin,'COMMIT')
            oid=scalar(admin,'SELECT %s::regclass::oid',('biz.'+name,));relations.append(oid);names[oid]=(number,name)
        result['relations']={str(oid):dict(number=n,name=name)for oid,(n,name)in names.items()};result['executed']=True
        check('normalCREATE produced70+ distinct real lake OIDs',len(set(relations)),a.tables)
        check('all committed counters come from100 original INSERTs',[(x[0],x[1],x[2])for x in records()],[(oid,100,0)for oid in sorted(relations)])
        release();wait('every real table finishes first automatic ANALYZE',complete,a.initial_seconds);claim()
        before=records();check('all first analyses cover exact original100rows',[(x[1],x[2],x[3],x[6],x[7],x[8]>0)for x in before],[(100,100,True,False,100.0,True)]*a.tables)
        check('every earlier candidate age remains notdue',all(x[9]for x in before))
        target=before[-1];prefix=before[:-1]
        check('newest attempted target has at least64 preceding candidates',len(prefix)>=64)
        check('all predecessors have zero pending changes',all(x[1]==x[2]for x in prefix))
        result['before_order']=safe(before);target_oid=target[0];number,name=names[target_oid]
        required=threshold+scale*max(target[7],0.);rounds=max(1,math.floor(required/100)+1)
        check('bounded ordinary UPDATE rounds suffice for configured threshold',rounds<=20)
        for _ in range(rounds):
            query(admin,'BEGIN');query(admin,sql.SQL('UPDATE biz.{} SET val=val+1').format(sql.Identifier(name)))
            query(admin,'UPDATE biz.truth SET val=val+1 WHERE table_no=%s',(number,));query(admin,'COMMIT')
        pending=next(x for x in records()if x[0]==target_oid)
        check('business UPDATE alone creates exact committed delta',pending[1]-pending[2],100*rounds)
        check('delta strictly exceeds original float4 threshold',float(pending[1]-pending[2])>required)
        check('maintenance claim kept target unanalyzed during UPDATE',pending[4:6],target[4:6])
        result['selected_target']=dict(oid=target_oid,name=name,rank=len(before),preceding=len(prefix),threshold=required,update_rounds=rounds,before=safe(target),after_update=safe(pending))
        release()
        wait('real worker crosses64 notdue candidates and analyzes newest target',
             lambda:next((x for x in records()if x[0]==target_oid and x[1]==x[2]and x[4]!=target[4]),None),a.target_seconds)
        claim();after=records();by={x[0]:x for x in after};result['after_order']=safe(after)
        check('target advanced automatic coverage exactly once to committed counter',by[target_oid][1:3],(100+100*rounds,100+100*rounds))
        check('all earlier attempts unchanged while target was reached',[(x[0],by[x[0]][4:6])for x in prefix],[(x[0],x[4:6])for x in prefix])
        check('earlier candidates remain below threshold and not aged at completion',all(by[x[0]][1]==by[x[0]][2]and by[x[0]][9]for x in prefix))
        for oid,(n,name)in names.items():
            check('exact lake/ordinary heap values/'+name,query(admin,sql.SQL('SELECT id,val FROM biz.{} ORDER BY id').format(sql.Identifier(name))),query(observer,'SELECT id,val FROM biz.truth WHERE table_no=%s ORDER BY id',(n,)))
            check('host statistics retains actual100 rows/'+name,scalar(observer,'SELECT reltuples FROM pg_class WHERE oid=%s',(oid,)),100)
        check('all executed input archives stayed exact',all(sha256(path)==x['sha256']==sha256(x['archive'])for path,x in result['inputs'].items()))
        check('all immutable binary/payload identities stayed exact',all(sha256(path)==h for path,h in result['hash_only_inputs'].items()))
        check_build(a.build_receipt);result['status']='pass'
    except BaseException as exc:result.update(reason=str(exc),sqlstate=getattr(exc,'pgcode',None),traceback=traceback.format_exc())
    finally:
        if admin is not None:
            try:query(admin,'ROLLBACK')
            except BaseException:pass
        if claim_ready and not claimed:
            try:claim(retiring=True)
            except BaseException as exc:cleanup_safe=False;result.update(status='fail',worker_retirement_error=str(exc),preserve_owned_scene=True)
        if claimed:
            try:release()
            except BaseException as exc:cleanup_safe=False;result.update(status='fail',claim_cleanup_error=str(exc),preserve_owned_scene=True)
        if db is not None and cleanup_safe:
            try:db.cleanup();result['database_cleanup']=db.cleanup_result
            except BaseException as exc:result.update(status='fail',database_cleanup_error=str(exc))
        if roles is not None and cleanup_safe:
            try:
                if not result.get('database_cleanup')or result['database_cleanup']['returncode']:raise RuntimeError('Exact owned DB cleanup unproven; preserve roles')
                result['role_cleanup']=cleanup_roles(control,roles.path,a.run_id);result=roles.redact(result)
            except BaseException as exc:result.update(status='fail',role_cleanup_error=str(exc))
        if control is not None:
            try:control.close()
            except BaseException as exc:result.update(status='fail',control_cleanup_error=str(exc))
        if reference_lock is not None:
            fcntl.flock(reference_lock.fileno(),fcntl.LOCK_UN);reference_lock.close()
            result.setdefault('reference_reservation',{})['released_after_cleanup_attempt']=True
        write_json(out/'result.json',result)
    print(json.dumps(dict(status=result['status'],result=str(out/'result.json'),assertions=len(result['assertions']),reason=result.get('reason'))))
    return 0 if result['status']=='pass'else 1


if __name__=='__main__':raise SystemExit(main())
