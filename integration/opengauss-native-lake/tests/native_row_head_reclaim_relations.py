#!/usr/bin/env python3
"""SQL0.18 real worker fairness across three tables with max_batches=1.

No synthetic scheduler or metadata protocol records. Ordinary rows are seeded
only in exact registered private head heaps after real Native DML created them.
"""
import argparse
import base64
import fcntl
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
import traceback

from native_row_head_sql026_admission import early_gate,add_arguments,require_actual,extra_inputs,archive_receipts,check_ordinary_layout
if __name__=='__main__':early_gate('relations')

ROOT=Path(__file__).resolve().parents[3];D=ROOT/'integration/opengauss-native-lake'
sys.path[:0]=[str(D),str(D/'tests')]
from file_dml_acceptance import check_build
from inline_fixture import query,scalar
from lake_harness import sha256,validate_run_id,write_json
from native_auto_flush import connect_control
from native_runtime import PROFILE
from native_test_context import OwnedDatabase
from permission_api import OwnedRoles,cleanup_roles
from empty_manifest import validate_empty_bundle,validate_empty_manifest
from empty_catalog_oracle import compare_initialized
from create_reference_oracle import export_created_catalog


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
    p.add_argument('--build-receipt',type=Path,required=True);p.add_argument('--budget-seconds',type=int,default=90)
    p.add_argument('--sql-version',choices=['0.18','0.26'],default='0.18');p.add_argument('--runtime-version',choices=['0.18','0.26'],default='0.18')
    add_arguments(p);a=p.parse_args();validate_run_id(a.run_id);a.mode='relations'
    ordinary26=(a.sql_version,a.runtime_version)==('0.26','0.26')
    if (a.sql_version,a.runtime_version) not in [('0.18','0.18'),('0.26','0.26')]:p.error('Only exact historical18 or fresh26/runtime26 admitted')
    if PROFILE!='debug' or not 20<=a.budget_seconds<=180:p.error('Owned Debug and 20..180 second observation budget required')
    admission=require_actual(a,a.mode)if ordinary26 else None
    out=ROOT/'公共文档/openGauss原生湖存储/验证'/a.run_id;out.mkdir(exist_ok=False)
    r=dict(status='fail',scope=__doc__,mode=a.mode,sql_version=a.sql_version,expected_runtime=a.runtime_version,assertions=[],observations=[],limitations=['Not Native 32-retry, fault injection, RAC, hard quota or performance qualification.'])
    files=[Path(__file__),D/'tests/file_dml_acceptance.py',D/'inline_fixture.py',D/'lake_harness.py',D/'tests/native_auto_flush.py',
        D/'native_runtime.py',D/'native_test_context.py',D/'tests/inline_cleanup.py',D/'tests/permission_api.py',D/'tests/native_table_create.py',
        D/'tests/catalog_fixture_bundle.py',D/'tests/empty_manifest.py',D/'tests/empty_reference_fixture.py',D/'tests/empty_catalog_oracle.py',
        D/'tests/create_reference_oracle.py',D/'tests/reverse_reference.py',D/'reference.sh',D/'fixture.py',D/'source-lock.json']
    if ordinary26:
        files+=extra_inputs(a);r['ordinary26_admission']=archive_receipts(out,*admission)
    archive=out/'executed-inputs';archive.mkdir();r['inputs']={str(f):sha256(f)for f in files}
    for f in files:shutil.copy2(f,archive/f.name)
    shutil.copy2(a.build_receipt,archive/'build.json')
    control=db=roles=reservation=None;actors=[];claimed=False
    def check(label,got,want=True):
        ok=got==want;r['assertions'].append(dict(name=label,observed=got,expected=want,status='pass'if ok else'fail'));write_json(out/'result.json',r)
        if not ok:raise AssertionError(label+': '+repr(got))
    def configure(c):query(c,'SET session_timeout=0');query(c,'SET statement_timeout=60000')
    def conn():
        c=db.connect();actors.append(c);configure(c);return c
    def claim():
        nonlocal claimed
        deadline=time.monotonic()+a.budget_seconds
        while not scalar(admin,'SELECT og_lake_internal.test_maintenance_claim()'):
            if time.monotonic()>=deadline:raise AssertionError('Cannot claim owned maintenance')
            time.sleep(.05)
        claimed=True
    def release():
        nonlocal claimed
        check('release exact owned maintenance claim',scalar(admin,'SELECT og_lake_internal.test_maintenance_release()'));claimed=False
    def state():
        records=[]
        for h in heads:
            row=query(observer,'SELECT reclaim_scanned,reclaimed_rows,reclaim_rounds,last_reclaim_at::text FROM og_lake_internal.row_lock_binding WHERE relation_oid=%s AND row_head_oid=%s',(h['host'],h['oid']))
            if len(row)!=1:raise AssertionError('Canonical mapping changed')
            records.append(dict(host=h['host'],head=h['oid'],scanned=row[0][0],deleted=row[0][1],rounds=row[0][2],at=row[0][3],live=scalar(observer,sql.SQL('SELECT count(*) FROM {}').format(h['sql']))))
        return records
    def drained(label,baseline):
        deadline=time.monotonic()+a.budget_seconds
        while True:
            image=state();r['observations'].append(dict(label=label,t=time.monotonic(),tables=image))
            if all(x['live']==0 and x['deleted']>baseline[i]for i,x in enumerate(image)):check(label,True);return image
            if time.monotonic()>=deadline:raise AssertionError('A table starved despite max_batches=1: '+label)
            time.sleep(.1)
    try:
        from psycopg2 import sql
        b=check_build(a.build_receipt);r.update(source_head=b['source_head'],binary_sha256=b['binary_sha256'],build=str(a.build_receipt.resolve()),build_sha256=sha256(a.build_receipt))
        control=connect_control();configure(control)
        check('real automatic maintenance enabled',scalar(control,'SHOW enable_lake_maintenance'),'on')
        check('one head/flush batch per worker',int(scalar(control,'SHOW lake_flush_max_batches')),1)
        check('tiny inline business rows are below automatic flush threshold',int(scalar(control,'SHOW lake_flush_min_rows'))>=100000)
        check('business rows cannot age into flush within this bounded test',int(scalar(control,"SELECT setting FROM pg_settings WHERE name='lake_flush_max_delay'"))>=3600000)
        with(out/'fixture.log').open('w')as log:subprocess.run([sys.executable,str(D/'tests/catalog_fixture_bundle.py'),'--run-id',a.run_id,'--output',str(out/'fixture/manifest.json')],stdout=log,stderr=subprocess.STDOUT,check=True,timeout=300)
        bundle=validate_empty_bundle(out/'fixture/manifest.json');empty,exported=validate_empty_manifest(bundle['entries']['same']['manifest'])
        reservation=(ROOT/'.reference/opengauss-native-lake/.driver.lock').open('a+');fcntl.flock(reservation.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB)
        db=OwnedDatabase(a.run_id,out);db.create();configure(db.admin);roles=OwnedRoles(control,a.run_id,out);admin,observer=conn(),conn()
        query(admin,"CREATE EXTENSION og_lake VERSION '"+a.sql_version+"'")
        actual_runtime=json.loads(scalar(admin,'SELECT og_lake.host_status()'))['module_version'];r['runtime_module']=actual_runtime
        check('exact Native module'+a.runtime_version,actual_runtime,a.runtime_version)
        check('exact selected installed SQL version',scalar(admin,"SELECT extversion FROM pg_extension WHERE extname='og_lake'"),a.sql_version)
        claim();private=roles.create('private',False);query(admin,'SELECT og_lake.configure_internal_owner(%s::name)',(private,))
        query(admin,"SELECT og_lake.create_catalog('created_lake'::name,'created_metadata'::name,%s::text,NULL::text)",(empty['data_path'],))
        native=export_created_catalog(admin,'created_metadata',exported,out/'native-initial.json');r['initial_independent_comparison']=compare_initialized(exported,native,empty['data_path'],'openGauss og_lake '+a.runtime_version)
        query(admin,'CREATE SCHEMA biz');heads=[]
        for name in ['first','second','third']:
            query(admin,sql.SQL("CREATE FOREIGN TABLE biz.{}(id integer,val integer) SERVER created_lake OPTIONS(binding_mode 'create',lake_schema 'main')").format(sql.Identifier(name)))
            query(admin,sql.SQL('INSERT INTO biz.{} VALUES(1,10),(2,20),(3,30)').format(sql.Identifier(name)))
            query(admin,sql.SQL('UPDATE biz.{} SET val=val+1 WHERE id=1').format(sql.Identifier(name)))
            result=query(observer,"SELECT h.binding_id::text,h.relation_oid,h.row_head_oid,n.nspname,c.relname FROM og_lake_internal.row_lock_binding h JOIN og_lake_internal.relation_binding r ON r.relation_oid=h.relation_oid AND r.binding_id=h.binding_id JOIN pg_class c ON c.oid=h.row_head_oid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE h.relation_oid=%s::regclass",('biz.'+name,))
            check('one canonical mapping '+name,len(result),1);binding,host,oid,ns,rel=result[0]
            check('private registered namespace '+name,ns,'og_lake_internal');head=dict(name=name,binding=binding,host=host,oid=oid,sql=sql.Identifier(ns,rel));heads.append(head)
            query(admin,sql.SQL('INSERT INTO {}(row_id) SELECT i FROM generate_series(1000,1015)i').format(head['sql']))
        if ordinary26:check_ordinary_layout(observer,query,check,[h['host']for h in heads])
        check('three distinct increasing business relation OIDs',[h['host']for h in heads],sorted(set(h['host']for h in heads)))
        check('three distinct physical head carriers',len({h['oid']for h in heads}),3)
        r['head_identities']=[{k:v for k,v in h.items()if k!='sql'}for h in heads]
        snapshot=scalar(observer,'SELECT max(snapshot_id) FROM created_metadata.ducklake_snapshot');initial=state();check('all three real head heaps contain work',[x['live']for x in initial],[17,17,17])
        release();first=drained('all three tables progress under one-batch worker budget',[x['deleted']for x in initial])
        claim()
        for h in heads:query(admin,sql.SQL('INSERT INTO {}(row_id) VALUES(500)').format(h['sql']))
        release();drained('all three tables receive another fair round',[x['deleted']for x in first]);claim()
        check('background head GC never publishes a business snapshot',scalar(observer,'SELECT max(snapshot_id) FROM created_metadata.ducklake_snapshot'),snapshot)
        check('due=false business tables never auto-flush',scalar(observer,'SELECT count(*) FROM created_metadata.ducklake_data_file'),0)
        for h in heads:check('exact business rows preserved '+h['name'],query(observer,sql.SQL('SELECT id,val FROM biz.{} ORDER BY id').format(sql.Identifier(h['name']))),[(1,11),(2,20),(3,30)])
        check('all executed inputs unchanged',all(sha256(Path(f))==wanted for f,wanted in r['inputs'].items()));check_build(a.build_receipt)
        if ordinary26:require_actual(a,a.mode)
        r['status']='pass'
    except BaseException as exc:r.update(reason=str(exc),traceback=traceback.format_exc())
    finally:
        for c in actors:
            try:query(c,'ROLLBACK')
            except Exception:pass
        if claimed:
            try:query(admin,'SELECT og_lake_internal.test_maintenance_release()')
            except Exception:pass
        for c in actors:
            try:c.close()
            except Exception:pass
        if db is not None:
            try:db.cleanup();r['database_cleanup']=db.cleanup_result
            except subprocess.TimeoutExpired as exc:
                r.update(status='fail',cleanup_error=str(exc))
                r['database_cleanup']=cleanup_timeout_record(exc,db,out)
            except BaseException as exc:r.update(status='fail',cleanup_error=str(exc))
        if roles is not None:
            try:
                if not r.get('database_cleanup') or r['database_cleanup']['returncode']:
                    raise RuntimeError('Owned DB cleanup unproven; exact roles retained')
                r['role_cleanup']=cleanup_roles(control,roles.path,a.run_id)
            except BaseException as exc:r.update(status='fail',role_cleanup_error=str(exc))
            r=roles.redact(r)
        if control is not None:control.close()
        if reservation is not None:reservation.close()
        write_json(out/'result.json',r)
    print(str(out/'result.json')+': '+r['status']);return 0 if r['status']=='pass'else 1


if __name__=='__main__':sys.exit(main())
