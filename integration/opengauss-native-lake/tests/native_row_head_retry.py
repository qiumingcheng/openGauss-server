#!/usr/bin/env python3
"""Deterministic real Native row-head ensure/lock loop; future diagnostic build.

The private builtin only observes actual Native results and pauses at standard
advisory gates. The controller performs real committed head INSERT/manual GC;
it never supplies or replaces locked.count. Not run during preparation.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor,TimeoutError
import fcntl
import json
from pathlib import Path
import secrets
import shutil
import subprocess
import sys
import time
import traceback

ROOT=Path(__file__).resolve().parents[3];D=ROOT/'integration/opengauss-native-lake'
sys.path[:0]=[str(D),str(D/'tests')]
from file_dml_acceptance import check_build
from inline_fixture import query,scalar
from lake_harness import sha256,validate_run_id,write_json
from native_auto_flush import connect_control
from native_runtime import PROFILE
from native_test_context import OwnedDatabase
from permission_api import OwnedRoles,cleanup_roles,ordinary_connection
from empty_manifest import validate_empty_bundle,validate_empty_manifest
from empty_catalog_oracle import compare_initialized
from create_reference_oracle import export_created_catalog


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--run-id',required=True)
    p.add_argument('--build-receipt',type=Path,required=True);p.add_argument('--mode',required=True,choices=['once','exhaust','cancel-before','cancel-after'])
    p.add_argument('--gate-timeout',type=int,default=10);a=p.parse_args();validate_run_id(a.run_id)
    if PROFILE!='debug' or not 3<=a.gate_timeout<=30:p.error('Owned Debug and gate timeout3..30 required')
    out=ROOT/'公共文档/openGauss原生湖存储/验证'/a.run_id;out.mkdir(exist_ok=False)
    r=dict(status='fail',scope=__doc__,mode=a.mode,native_loop_verified=False,native_zero_retry_verified=False,native_32_exhaustion_verified=False,assertions=[],gate_evidence=[],gc_evidence=[],
        limitations=['No production hook SQL installed; dedicated single-node root backend diagnostic only.',
            'Native32 exhaustion is qualified only by exhaust mode actual32 zero-result counters; no ordinary-SQL simulation substitutes.',
            'No primitive OOM/corruption injection, RAC/threadpool/SMP/performance or commercial qualification.'])
    files=[Path(__file__),D/'tests/file_dml_acceptance.py',D/'inline_fixture.py',D/'lake_harness.py',D/'tests/native_auto_flush.py',
        D/'native_runtime.py',D/'native_test_context.py',D/'tests/inline_cleanup.py',D/'tests/permission_api.py',D/'tests/native_table_create.py',
        D/'tests/catalog_fixture_bundle.py',D/'tests/empty_manifest.py',D/'tests/empty_reference_fixture.py',D/'tests/empty_catalog_oracle.py',
        D/'tests/create_reference_oracle.py',D/'tests/reverse_reference.py',D/'reference.sh',D/'fixture.py',D/'source-lock.json']
    archive=out/'executed-inputs';archive.mkdir();r['inputs']={str(f):sha256(f)for f in files}
    for f in files:shutil.copy2(f,archive/f.name)
    shutil.copy2(a.build_receipt,archive/'build.json')
    control=db=roles=reservation=pool=future=None;actors=[];held=set();claimed=False;writer=None
    def check(label,got,want=True):
        ok=got==want;r['assertions'].append(dict(name=label,observed=got,expected=want,status='pass'if ok else'fail'));write_json(out/'result.json',r)
        if not ok:raise AssertionError(label+': '+repr(got))
    def configure(c):query(c,'SET session_timeout=0');query(c,'SET statement_timeout=120000');query(c,'SET query_dop=1')
    def conn():
        c=db.connect();actors.append(c);configure(c);return c
    def head_rows(c=None):return query(c or observer,sql.SQL('SELECT row_id,ctid::text,xmin::text,xmax::text FROM {} ORDER BY row_id').format(head))
    def business(c=None):return query(c or observer,'SELECT id,val FROM biz.orders ORDER BY id')
    def raw_rows():return query(observer,sql.SQL('SELECT * FROM created_metadata.{} ORDER BY _ducklake_row_id,_ducklake_begin_snapshot').format(sql.Identifier(carrier)))
    def marks():
        if not scalar(observer,"SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='created_metadata' AND c.relname=%s",(delete_name,)):return None
        return query(observer,sql.SQL('SELECT * FROM created_metadata.{} ORDER BY file_id,row_id,begin_snapshot').format(sql.Identifier(delete_name)))
    def get_probe():return json.loads(scalar(writer,'SELECT biz.rowhead_probe(0,NULL::regclass,0::bigint,0::bigint)'))
    def release(key):
        check('release only exact controller gate '+str(key),scalar(admin,'SELECT pg_advisory_unlock(%s::bigint)',(key,)))
        held.remove(key)
    def wait_gate(attempt,after):
        key=base+(attempt-1)*2+(1 if after else 0);deadline=time.monotonic()+a.gate_timeout
        while True:
            if future.done():raise AssertionError('Native UPDATE completed before required gate '+str((attempt,after))+': '+repr(future.result()))
            predicate="locktype='advisory' AND database=%s AND classid=%s AND objid=%s AND objsubid=1 AND mode='ExclusiveLock'"
            args=(db.allocation['database_oid'],key>>32,key&0xffffffff)
            waiting=query(observer,'SELECT pid,granted FROM pg_locks WHERE '+predicate+' AND pid=%s AND NOT granted',args+(writer_pid,))
            holding=query(observer,'SELECT pid,granted FROM pg_locks WHERE '+predicate+' AND pid=%s AND granted',args+(admin_pid,))
            if waiting and holding:
                r['gate_evidence'].append(dict(attempt=attempt,phase='after'if after else'before',key=key,waiting=waiting,holding=holding));check('actual Native bound gate '+str((attempt,after)),True);return key
            if time.monotonic()>=deadline:
                r['gate_timeout_locks']=query(observer,'SELECT locktype,mode,granted,relation,transactionid::text FROM pg_locks WHERE pid=%s',(writer_pid,))
                raise AssertionError('No exact Native advisory wait for '+str((attempt,after)))
            time.sleep(.01)
    def gc(attempt):
        before=head_rows();locks=query(observer,'SELECT locktype,mode,granted,relation,transactionid::text FROM pg_locks WHERE pid=%s',(writer_pid,))
        removed=int(scalar(admin,'SELECT og_lake.reclaim_row_heads(%s::regclass,512)',('biz.orders',)))
        after=head_rows();r['gc_evidence'].append(dict(attempt=attempt,before=before,writer_locks=locks,removed=removed,after=after));write_json(out/'result.json',r)
        check('real manual GC removes committed head after ensure '+str(attempt),removed,1)
        check('real GC leaves no committed live carrier '+str(attempt),after,[])
    try:
        from psycopg2 import sql
        b=check_build(a.build_receipt);r.update(source_head=b['source_head'],binary_sha256=b['binary_sha256'],build=str(a.build_receipt.resolve()),build_sha256=sha256(a.build_receipt))
        control=connect_control();configure(control)
        with(out/'fixture.log').open('w')as log:subprocess.run([sys.executable,str(D/'tests/catalog_fixture_bundle.py'),'--run-id',a.run_id,'--output',str(out/'fixture/manifest.json')],stdout=log,stderr=subprocess.STDOUT,check=True,timeout=300)
        bundle=validate_empty_bundle(out/'fixture/manifest.json');empty,exported=validate_empty_manifest(bundle['entries']['same']['manifest'])
        reservation=(ROOT/'.reference/opengauss-native-lake/.driver.lock').open('a+');fcntl.flock(reservation.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB)
        db=OwnedDatabase(a.run_id,out);db.create();configure(db.admin);roles=OwnedRoles(control,a.run_id,out)
        admin,observer,writer=conn(),conn(),conn();writer_pid=scalar(writer,'SELECT pg_backend_pid()');admin_pid=scalar(admin,'SELECT pg_backend_pid()')
        query(admin,"CREATE EXTENSION og_lake VERSION '0.18'");check('exact Native module0.18',json.loads(scalar(admin,'SELECT og_lake.host_status()'))['module_version'],'0.18')
        deadline=time.monotonic()+15
        while not scalar(admin,'SELECT og_lake_internal.test_maintenance_claim()'):
            if time.monotonic()>=deadline:raise AssertionError('Cannot claim owned maintenance')
            time.sleep(.05)
        claimed=True;private=roles.create('private',False);query(admin,'SELECT og_lake.configure_internal_owner(%s::name)',(private,))
        query(admin,"SELECT og_lake.create_catalog('created_lake'::name,'created_metadata'::name,%s::text,NULL::text)",(empty['data_path'],))
        native=export_created_catalog(admin,'created_metadata',exported,out/'native-initial.json');r['initial_independent_comparison']=compare_initialized(exported,native,empty['data_path'],'openGauss og_lake 0.18')
        query(admin,'CREATE SCHEMA biz')
        query(admin,"CREATE FOREIGN TABLE biz.orders(id integer,val integer) SERVER created_lake OPTIONS(binding_mode 'create',lake_schema 'main')")
        # Flush preserves ended historical versions. Use a completed first
        # file for the deleted seed rows, then a second one-row file for id=3.
        # Its physical position is0 while the permanent logical ID is positive.
        query(admin,'INSERT INTO biz.orders VALUES(1,100),(2,200)')
        table_id=scalar(observer,"SELECT table_id FROM created_metadata.ducklake_table WHERE table_name='orders' AND end_snapshot IS NULL")
        carrier=scalar(observer,'SELECT table_name FROM created_metadata.ducklake_inlined_data_tables WHERE table_id=%s',(table_id,))
        query(admin,'DELETE FROM biz.orders WHERE id IN(1,2)');query(admin,"SELECT og_lake.flush('biz.orders'::regclass)")
        check('seed history remains two physical versions',query(observer,'SELECT record_count FROM created_metadata.ducklake_data_file WHERE table_id=%s AND end_snapshot IS NULL ORDER BY data_file_id',(table_id,)),[(2,)])
        check('seed position deletes hide both historical versions',business(),[])
        query(admin,'INSERT INTO biz.orders VALUES(3,10)')
        row_id=int(scalar(observer,sql.SQL('SELECT _ducklake_row_id FROM created_metadata.{} WHERE id=3 AND _ducklake_end_snapshot IS NULL').format(sql.Identifier(carrier))))
        query(admin,"SELECT og_lake.flush('biz.orders'::regclass)")
        check('flushed target has no inline/delta physical versions',raw_rows(),[])
        data_files=query(observer,'SELECT data_file_id,record_count FROM created_metadata.ducklake_data_file WHERE table_id=%s AND end_snapshot IS NULL ORDER BY data_file_id',(table_id,))
        check('separate one-row target file follows the retained seed history',[x[1]for x in data_files],[2,1])
        r['physical_files']=data_files
        check('stable logical ID differs from only physical position0',row_id>0)
        check('one exact target remains',business(),[(3,10)])
        mapping=query(observer,"SELECT h.binding_id::text,h.relation_oid,h.row_head_oid,n.nspname,c.relname FROM og_lake_internal.row_lock_binding h JOIN og_lake_internal.relation_binding r ON r.relation_oid=h.relation_oid AND r.binding_id=h.binding_id JOIN pg_class c ON c.oid=h.row_head_oid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE h.relation_oid='biz.orders'::regclass")
        check('one exact canonical stable head carrier',len(mapping),1);binding,host,head_oid,namespace,name=mapping[0]
        check('head private namespace',namespace,'og_lake_internal');head=sql.Identifier(namespace,name)
        r['target']=dict(binding=binding,host_oid=host,head_oid=head_oid,row_id=row_id,file_id=data_files[1][0],physical_position=0,writer_pid=writer_pid)
        check('retire setup DELETE synchronization heads',int(scalar(admin,'SELECT og_lake.reclaim_row_heads(%s::regclass,512)',('biz.orders',))),2)
        check('target begins with no head',head_rows(),[])
        delete_name='ducklake_inlined_delete_'+str(table_id);before_marks=marks();before_snapshot=scalar(observer,'SELECT max(snapshot_id) FROM created_metadata.ducklake_snapshot')
        query(admin,"CREATE FUNCTION biz.rowhead_probe(integer,regclass,bigint,bigint) RETURNS text AS '$libdir/og_lake','og_lake_test_rowhead_probe' LANGUAGE C VOLATILE NOT FENCED")
        query(admin,'REVOKE ALL ON FUNCTION biz.rowhead_probe(integer,regclass,bigint,bigint) FROM PUBLIC')
        outsider=roles.create('outsider',True);query(admin,sql.SQL('GRANT USAGE ON SCHEMA biz TO {}').format(sql.Identifier(outsider)))
        query(admin,sql.SQL('GRANT EXECUTE ON FUNCTION biz.rowhead_probe(integer,regclass,bigint,bigint) TO {}').format(sql.Identifier(outsider)))
        ordinary=ordinary_connection(db.database,outsider,roles.passwords['outsider']);actors.append(ordinary);configure(ordinary)
        try:query(ordinary,'SELECT biz.rowhead_probe(0,NULL::regclass,0::bigint,0::bigint)');denied=None
        except Exception as exc:denied=getattr(exc,'pgcode',None)
        check('builtin body rejects granted non-superuser caller',denied,'42501');ordinary.close()
        base=(0x4f47<<32)+secrets.randbelow(1<<24);r['gate_base']=base
        rounds=32 if a.mode=='exhaust'else 1
        for key in range(base,base+rounds*2):query(admin,'SELECT pg_advisory_lock(%s::bigint)',(key,));held.add(key)
        if a.mode=='once':query(admin,sql.SQL('INSERT INTO {}(row_id) VALUES(%s)').format(head),(row_id,))
        old=head_rows()[0][1]if a.mode=='once'else None
        armed=json.loads(scalar(writer,'SELECT biz.rowhead_probe(1,%s::regclass,%s::bigint,%s::bigint)',('biz.orders',row_id,base)))
        check('arming JSON honestly precedes autocommit readiness',armed['rowhead_state'],1)
        query(writer,'BEGIN');query(writer,'SAVEPOINT work')
        def update():
            try:return dict(status='ok',rows=query(writer,'UPDATE biz.orders SET val=val+1 WHERE id=3 RETURNING id,val'))
            except Exception as exc:return dict(status='error',sqlstate=getattr(exc,'pgcode',None),message=str(exc))
        pool=ThreadPoolExecutor(max_workers=1);future=pool.submit(update)
        for attempt in range(1,rounds+1):
            key=wait_gate(attempt,False)
            if a.mode=='cancel-before':
                check('cancel exact Native writer before real ensure',scalar(admin,'SELECT pg_cancel_backend(%s)',(writer_pid,)));break
            if a.mode=='exhaust':
                query(admin,sql.SQL('INSERT INTO {}(row_id) VALUES(%s)').format(head),(row_id,))
                check('committed controller carrier before real ensure '+str(attempt),[x[0]for x in head_rows()],[row_id])
            release(key);key=wait_gate(attempt,True)
            if a.mode=='cancel-after':
                check('actual own ensure remains invisible to independent observer',head_rows(),[])
                check('cancel exact Native writer after real ensure',scalar(admin,'SELECT pg_cancel_backend(%s)',(writer_pid,)));break
            gc(attempt);release(key)
        reply=future.result(timeout=20);r['native_reply']=reply
        if a.mode=='once':
            check('real Native update succeeds after zero-row retry',reply,dict(status='ok',rows=[(3,11)]))
            probe=get_probe();check('actual Native retry counts',{k:probe[k]for k in ['rowhead_attempts','rowhead_ensures','rowhead_inserted','rowhead_zero','rowhead_success','rowhead_exhausts','rowhead_aborts']},dict(rowhead_attempts=2,rowhead_ensures=2,rowhead_inserted=1,rowhead_zero=1,rowhead_success=1,rowhead_exhausts=0,rowhead_aborts=0))
            owned=head_rows(writer);check('native retry holds one replacement stable key',[x[0]for x in owned],[row_id]);check('native retry owns a new physical ctid',owned[0][1]!=old)
            query(writer,'RELEASE SAVEPOINT work');query(writer,'COMMIT');check('one committed business after-image',business(),[(3,11)])
        else:
            check('real Native error SQLSTATE',reply.get('sqlstate'),'55P03'if a.mode=='exhaust'else'57014')
            query(writer,'ROLLBACK TO work');query(writer,'RELEASE SAVEPOINT work');probe=get_probe();query(writer,'COMMIT')
            expected=dict(rowhead_attempts=32,rowhead_ensures=32,rowhead_inserted=0,rowhead_zero=32,rowhead_success=0,rowhead_exhausts=1,rowhead_aborts=1)if a.mode=='exhaust'else dict(rowhead_attempts=1,rowhead_ensures=1 if a.mode=='cancel-after'else 0,rowhead_inserted=1 if a.mode=='cancel-after'else 0,rowhead_zero=0,rowhead_success=0,rowhead_exhausts=0,rowhead_aborts=1)
            check('actual Native error/rollback counts',{k:probe[k]for k in expected},expected)
            check('failed native writer leaves no live carrier',head_rows(),[]);check('business update fully rolled back',business(),[(3,10)])
            check('failed writer publishes no business snapshot',scalar(observer,'SELECT max(snapshot_id) FROM created_metadata.ducklake_snapshot'),before_snapshot)
            check('failed writer publishes no inline after-image',raw_rows(),[]);check('failed writer preserves original file delete marks',marks(),before_marks)
        r['native_probe']=probe;check('probe remained bound to exact rel,R,session',(probe['rowhead_relation'],probe['rowhead_row'],probe['rowhead_session']),(host,row_id,writer_pid))
        check('probe expires after real Native execution',probe['rowhead_state'],0)
        check('all executed inputs remain unchanged',all(sha256(Path(f))==h for f,h in r['inputs'].items()));check_build(a.build_receipt)
        r.update(status='pass',native_loop_verified=True,native_zero_retry_verified=a.mode=='once',native_32_exhaustion_verified=a.mode=='exhaust')
    except BaseException as exc:r.update(reason=str(exc),traceback=traceback.format_exc())
    finally:
        # Releasing only controller-owned advisory keys permits a cancelled
        # backend to unwind; its business transaction is never auto-committed.
        if 'admin'in locals():
            for key in list(held):
                try:query(admin,'SELECT pg_advisory_unlock(%s::bigint)',(key,));held.remove(key)
                except Exception:pass
        if future is not None and not future.done():
            try:writer.cancel()
            except Exception:pass
        unfinished=False
        if future is not None:
            try:future.result(timeout=30)
            except TimeoutError:unfinished=True;r.update(status='fail',cleanup_error='Native async query did not unwind; owned DB preserved')
            except Exception:pass
        if pool is not None:pool.shutdown(wait=False)
        if claimed:
            try:
                r['maintenance_claim_released']=bool(scalar(admin,'SELECT og_lake_internal.test_maintenance_release()'))
                if not r['maintenance_claim_released']:r.update(status='fail',claim_cleanup_error='Owned maintenance claim was not held')
            except Exception as exc:r.update(status='fail',claim_cleanup_error=str(exc))
        for c in actors:
            if unfinished and c is writer:continue
            try:query(c,'ROLLBACK');c.close()
            except Exception:pass
        if db is not None and not unfinished:
            try:db.cleanup();r['database_cleanup']=db.cleanup_result
            except BaseException as exc:r.update(status='fail',cleanup_error=str(exc))
        if roles is not None and not unfinished:
            try:r['role_cleanup']=cleanup_roles(control,roles.path,a.run_id)
            except BaseException as exc:r.update(status='fail',role_cleanup_error=str(exc))
        if control is not None:control.close()
        if reservation is not None:reservation.close()
        if r['status']!='pass':r.update(native_loop_verified=False,native_zero_retry_verified=False,native_32_exhaustion_verified=False)
        if roles is not None:r=roles.redact(r)
        write_json(out/'result.json',r)
    print(str(out/'result.json')+': '+r['status']);return 0 if r['status']=='pass'else 1


if __name__=='__main__':sys.exit(main())
