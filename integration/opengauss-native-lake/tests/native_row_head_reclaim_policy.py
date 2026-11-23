#!/usr/bin/env python3
"""SQL0.18 private row-head SKIP policy; real Native manual API, AST first.

Exercise caller allow_concurrent_tuple_update=off without relaxing business
DML. Native primitive-internal ERROR/OOM and worker-global off configuration
require separate qualification; this is not a fault-injection proof.
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
if __name__=='__main__':early_gate('policy')

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
    p.add_argument('--build-receipt',required=True,type=Path);p.add_argument('--sql-version',choices=['0.18','0.26'],default='0.18');p.add_argument('--runtime-version',choices=['0.18','0.26'],default='0.18')
    add_arguments(p);a=p.parse_args();validate_run_id(a.run_id);a.mode='policy'
    ordinary26=(a.sql_version,a.runtime_version)==('0.26','0.26')
    if (a.sql_version,a.runtime_version) not in [('0.18','0.18'),('0.26','0.26')]:p.error('Only exact historical18 or fresh26/runtime26 admitted')
    if PROFILE!='debug':p.error('Requires the explicitly owned Debug runtime')
    admission=require_actual(a,a.mode)if ordinary26 else None
    out=ROOT/'公共文档/openGauss原生湖存储/验证'/a.run_id;out.mkdir(exist_ok=False)
    r=dict(status='fail',scope=__doc__,mode=a.mode,sql_version=a.sql_version,expected_runtime=a.runtime_version,assertions=[],limitations=[
        'Actual cursor arithmetic ERROR is tested inside the private guard; no primitive-internal heap_lock_tuple ERROR/OOM injection.',
        'No worker/server configuration restart to force automatic worker policy off.',
        'No Native 32-attempt exhaustion, target RAC, hard quota or performance qualification.'])
    files=[Path(__file__),D/'tests/file_dml_acceptance.py',D/'inline_fixture.py',D/'lake_harness.py',
        D/'tests/native_auto_flush.py',D/'native_runtime.py',D/'native_test_context.py',D/'tests/inline_cleanup.py',
        D/'tests/permission_api.py',D/'tests/native_table_create.py',D/'tests/catalog_fixture_bundle.py',
        D/'tests/empty_manifest.py',D/'tests/empty_reference_fixture.py',D/'tests/empty_catalog_oracle.py',
        D/'tests/create_reference_oracle.py',D/'tests/reverse_reference.py',D/'reference.sh',D/'fixture.py',D/'source-lock.json']
    if ordinary26:
        files+=extra_inputs(a);r['ordinary26_admission']=archive_receipts(out,*admission)
    archive=out/'executed-inputs';archive.mkdir();r['inputs']={str(f):sha256(f)for f in files}
    for f in files:shutil.copy2(f,archive/f.name)
    shutil.copy2(a.build_receipt,archive/'build.json')
    control=db=roles=reservation=None;actors=[];claimed=False

    def check(label,got,want=True):
        ok=got==want;r['assertions'].append(dict(name=label,observed=got,expected=want,status='pass'if ok else'fail'))
        write_json(out/'result.json',r)
        if not ok:raise AssertionError(label+': '+repr(got))

    def configure(c):
        query(c,'SET session_timeout=0');query(c,'SET statement_timeout=15000')

    def conn():
        c=db.connect();actors.append(c);configure(c);return c

    def policy(label):check(label,scalar(admin,'SHOW allow_concurrent_tuple_update'),'off')

    def expected_error(label,statement,args,states):
        query(admin,'SAVEPOINT expected_error')
        try:query(admin,statement,args);state=None
        except Exception as exc:state=getattr(exc,'pgcode',None)
        finally:query(admin,'ROLLBACK TO expected_error');query(admin,'RELEASE SAVEPOINT expected_error')
        r.setdefault('observed_sqlstates',{})[label]=state
        check(label,state, next(iter(states)) if len(states)==1 else states)

    def batch():return int(scalar(admin,'SELECT og_lake.reclaim_row_heads(%s::regclass,512)',('biz.orders',)))

    try:
        from psycopg2 import sql
        b=check_build(a.build_receipt);r.update(source_head=b['source_head'],binary_sha256=b['binary_sha256'],
            build=str(a.build_receipt.resolve()),build_sha256=sha256(a.build_receipt))
        control=connect_control();configure(control)
        with(out/'fixture.log').open('w')as log:
            subprocess.run([sys.executable,str(D/'tests/catalog_fixture_bundle.py'),'--run-id',a.run_id,
                '--output',str(out/'fixture/manifest.json')],stdout=log,stderr=subprocess.STDOUT,check=True,timeout=300)
        bundle=validate_empty_bundle(out/'fixture/manifest.json');empty,exported=validate_empty_manifest(bundle['entries']['same']['manifest'])
        reservation=(ROOT/'.reference/opengauss-native-lake/.driver.lock').open('a+');fcntl.flock(reservation.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB)
        db=OwnedDatabase(a.run_id,out);db.create();configure(db.admin)
        roles=OwnedRoles(control,a.run_id,out);admin,observer,writer,blocker=conn(),conn(),conn(),conn()
        query(admin,"CREATE EXTENSION og_lake VERSION '"+a.sql_version+"'")
        actual_runtime=json.loads(scalar(admin,'SELECT og_lake.host_status()'))['module_version'];r['runtime_module']=actual_runtime
        check('exact Native module'+a.runtime_version,actual_runtime,a.runtime_version)
        check('exact selected installed SQL version',scalar(admin,"SELECT extversion FROM pg_extension WHERE extname='og_lake'"),a.sql_version)
        check('claim exact owned database maintenance',scalar(admin,'SELECT og_lake_internal.test_maintenance_claim()'));claimed=True
        private=roles.create('private',False);query(admin,'SELECT og_lake.configure_internal_owner(%s::name)',(private,))
        query(admin,"SELECT og_lake.create_catalog('created_lake'::name,'created_metadata'::name,%s::text,NULL::text)",(empty['data_path'],))
        native=export_created_catalog(admin,'created_metadata',exported,out/'native-initial.json')
        r['initial_independent_comparison']=compare_initialized(exported,native,empty['data_path'],'openGauss og_lake '+a.runtime_version)
        query(admin,'CREATE SCHEMA biz')
        query(admin,"CREATE FOREIGN TABLE biz.orders(id integer,val integer) SERVER created_lake OPTIONS(binding_mode 'create',lake_schema 'main')")
        query(admin,'INSERT INTO biz.orders VALUES(1,10),(2,20),(3,30)')
        query(admin,'CREATE TABLE biz.policy_heap(id integer PRIMARY KEY)')
        query(admin,'INSERT INTO biz.policy_heap VALUES(1)')
        identities=query(observer,"SELECT h.binding_id::text,h.relation_oid,h.row_head_oid,n.nspname,c.relname FROM og_lake_internal.row_lock_binding h JOIN og_lake_internal.relation_binding r ON r.relation_oid=h.relation_oid AND r.binding_id=h.binding_id JOIN pg_class c ON c.oid=h.row_head_oid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE h.relation_oid='biz.orders'::regclass")
        check('one exact canonical head mapping',len(identities),1)
        binding,host,head_oid,namespace,name=identities[0];check('exact private head namespace',namespace,'og_lake_internal')
        head=sql.Identifier(namespace,name);r['head_identity']=dict(binding=binding,host_oid=host,head_oid=head_oid)
        if ordinary26:check_ordinary_layout(observer,query,check,[host])
        query(admin,'UPDATE biz.orders SET val=val+1 WHERE id IN(1,2)')
        check('two stable logical heads before contention',query(observer,sql.SQL('SELECT row_id FROM {} ORDER BY row_id').format(head)),[(0,),(1,)])
        query(writer,'BEGIN');query(writer,'UPDATE biz.orders SET val=val+1 WHERE id=1')
        query(writer,'SELECT id FROM biz.policy_heap WHERE id=1 FOR UPDATE')
        writer_pid=scalar(writer,'SELECT pg_backend_pid()');writer_xid=str(scalar(writer,'SELECT txid_current()'))
        r['holder']=dict(pid=writer_pid,xid=writer_xid)
        check('writer has exact active XID lock',scalar(observer,"SELECT count(*) FROM pg_locks WHERE pid=%s AND locktype='transactionid' AND mode='ExclusiveLock' AND granted AND transactionid::text=%s",(writer_pid,writer_xid)),1)
        query(admin,'SET allow_concurrent_tuple_update=off');policy('caller policy starts off')
        removed=batch();check('manual off policy skips busy head and deletes idle head',removed,1)
        check('busy low head retained and idle head reclaimed',query(observer,sql.SQL('SELECT row_id FROM {} ORDER BY row_id').format(head)),[(0,)])
        policy('successful head SKIP restores caller off policy')
        query(blocker,'BEGIN');query(blocker,'SELECT row_head_oid FROM og_lake_internal.row_lock_binding WHERE relation_oid=%s FOR UPDATE',(host,))
        check('manual off policy skips busy canonical map row',batch(),0)
        policy('zero-map SKIP restores caller off policy');query(blocker,'ROLLBACK')
        query(admin,'BEGIN');query(admin,'SET LOCAL statement_timeout=3000')
        # Native heap tuple locking translates caller off to LockWaitError.
        # Lake UPDATE first runs INSERT ON CONFLICT for its stable row head;
        # that uniqueness check can wait before heap_lock_tuple is reached.
        expected_error('ordinary heap tuple lock retains caller off behavior','SELECT id FROM biz.policy_heap WHERE id=1 FOR UPDATE',None,{'55P03'})
        policy('ordinary heap lock ERROR leaves caller off policy')
        expected_error('business row-head ensure remains bounded by statement timeout','UPDATE biz.orders SET val=val+10 WHERE id=1',None,{'57014'})
        policy('business ensure timeout leaves caller off policy');query(admin,'ROLLBACK')
        query(writer,'COMMIT')
        check('idle former writer head is now reclaimable with policy off',batch(),1)
        policy('second successful batch preserves caller policy')
        query(admin,'BEGIN');query(admin,'SAVEPOINT corrupt_cursor')
        query(admin,'UPDATE og_lake_internal.row_lock_binding SET reclaim_cursor=-2 WHERE relation_oid=%s',(host,))
        expected_error('downstream cursor validation ERROR follows private map guard','SELECT og_lake.reclaim_row_heads(%s::regclass,512)',('biz.orders',),{'55000'})
        policy('post-guard ERROR does not leak private policy')
        query(admin,'ROLLBACK TO corrupt_cursor');query(admin,'RELEASE SAVEPOINT corrupt_cursor');query(admin,'COMMIT')
        check('rollback restores usable metadata cursor',scalar(observer,'SELECT reclaim_cursor>=-1 FROM og_lake_internal.row_lock_binding WHERE relation_oid=%s',(host,)))
        check('after rollback another bounded batch remains usable',batch(),0)
        policy('empty-head successful path preserves caller policy')
        # A real arithmetic ERROR in cursor UPDATE occurs after the private
        # head DELETE, inside the outer policy guard. No Native result is faked.
        query(admin,sql.SQL('INSERT INTO {}(row_id) VALUES(10000)').format(head))
        query(admin,'BEGIN')
        query(admin,'UPDATE og_lake_internal.row_lock_binding SET reclaim_cursor=-1,reclaim_round_end=-1,reclaim_scanned=9223372036854775807 WHERE relation_oid=%s',(host,))
        expected_error('real cursor counter overflow inside private guard','SELECT og_lake.reclaim_row_heads(%s::regclass,512)',('biz.orders',),{'22003'})
        policy('guard-internal ERROR restores caller off policy')
        check('guard-internal ERROR rolls back the preceding private head DELETE',query(admin,sql.SQL('SELECT row_id FROM {} ORDER BY row_id').format(head)),[(10000,)])
        query(admin,'ROLLBACK')
        check('same backend reclaims restored head after guard ERROR',batch(),1)
        policy('same backend success after guard ERROR retains off policy')
        check('GC and rejected business write preserve exact visible rows',query(observer,'SELECT id,val FROM biz.orders ORDER BY id'),[(1,12),(2,21),(3,30)])
        query(admin,'SET allow_concurrent_tuple_update=on')
        check('caller may independently restore its business setting',scalar(admin,'SHOW allow_concurrent_tuple_update'),'on')
        check('every executed script/helper input unchanged',all(sha256(Path(f))==h for f,h in r['inputs'].items()))
        check_build(a.build_receipt)
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
