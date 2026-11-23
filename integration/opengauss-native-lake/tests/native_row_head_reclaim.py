#!/usr/bin/env python3
"""Native SQL0.18 row-head reclamation acceptance; prepare/AST before serial run.

Real DuckLake EMPTY provenance, exact native build, actual manual API and real
launcher workers. No random schedule is promoted to a 32-retry proof. Remote
file GC, target RAC, hard physical quotas and performance are outside scope.
"""
import argparse
import base64
from concurrent.futures import ThreadPoolExecutor
import fcntl
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import threading
import time
import traceback

from native_row_head_sql026_admission import early_gate,add_arguments,require_actual,extra_inputs,archive_receipts
if __name__=='__main__':early_gate(None)

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

FIELDS=['binding_id','relation_oid','row_head_oid','reclaim_cursor','reclaim_round_end','reclaim_scanned',
    'reclaimed_rows','reclaim_rounds','last_reclaim_at','last_vacuum_at','live_rows_estimate','dead_rows_estimate',
    'head_bytes','soft_budget_exceeded','last_delete_xid','oldest_xmin_observed']


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
    p.add_argument('--build-receipt',required=True,type=Path)
    p.add_argument('--mode',required=True,choices=['migration','manual','auto','fairness','retention','vacuum','lifecycle','cancel','protocol'])
    p.add_argument('--sql-version',choices=['0.18','0.23','0.26'],default='0.18')
    p.add_argument('--runtime-version',choices=['0.18','0.23','0.26'],default='0.18')
    p.add_argument('--budget-seconds',type=int,default=90);add_arguments(p);a=p.parse_args();validate_run_id(a.run_id)
    ordinary26=(a.sql_version,a.runtime_version)==('0.26','0.26')
    if '0.26' in (a.sql_version,a.runtime_version) and (not ordinary26 or a.mode not in ['manual','auto','vacuum']):p.error('Only fresh26/runtime26 manual/auto/vacuum admitted')
    if a.sql_version=='0.23'and a.runtime_version!='0.23':p.error('SQL23 requires explicit runtime23')
    if not ordinary26 and (a.sql_version,a.runtime_version)!=('0.18','0.18') and a.mode not in ['auto','vacuum']:
        p.error('New version admission is limited to the reviewed auto/vacuum affected subset')
    if PROFILE!='debug':p.error('Requires an explicitly owned Debug runtime')
    if not 20<=a.budget_seconds<=180:p.error('Bounded observation budget must be 20..180 seconds')
    admission=require_actual(a,a.mode)if ordinary26 else None
    out=ROOT/'公共文档/openGauss原生湖存储/验证'/a.run_id;out.mkdir(exist_ok=False)
    r=dict(status='fail',scope=__doc__,mode=a.mode,sql_version=a.sql_version,expected_runtime=a.runtime_version,assertions=[],observations=[],
        limitations=['Native 32-attempt exhaustion is not proved by the protocol mode; deterministic native after-ensure hook/unit guard qualification remains separate.',
            'No hard physical capacity or production/RAC/performance qualification.'])
    files=[Path(__file__),D/'tests/file_dml_acceptance.py',D/'inline_fixture.py',D/'lake_harness.py',
        D/'tests/native_auto_flush.py',D/'native_runtime.py',D/'native_test_context.py',D/'tests/inline_cleanup.py',
        D/'tests/permission_api.py',D/'tests/catalog_fixture_bundle.py',D/'tests/empty_manifest.py',
        D/'tests/empty_reference_fixture.py',D/'tests/empty_catalog_oracle.py',D/'tests/create_reference_oracle.py',
        D/'tests/reverse_reference.py',D/'tests/native_table_create.py',D/'reference.sh',D/'fixture.py',D/'source-lock.json']
    if ordinary26:
        files+=extra_inputs(a);r['ordinary26_admission']=archive_receipts(out,*admission)
    archive=out/'executed-inputs';archive.mkdir();r['inputs']={str(f):sha256(f)for f in files}
    for f in files:shutil.copy2(f,archive/f.name)
    shutil.copy2(a.build_receipt,archive/'build.json')
    control=db=roles=reservation=None;actors=[];jobs=[];claimed=False;stop=threading.Event()

    def check(name,got,want=True):
        ok=got==want;r['assertions'].append(dict(name=name,observed=got,expected=want,status='pass'if ok else'fail'))
        write_json(out/'result.json',r)
        if not ok:raise AssertionError(name+': '+repr(got))

    def configure(c):
        query(c,'SET session_timeout=0');query(c,'SET statement_timeout=180000')

    def conn():
        c=db.connect();configure(c);actors.append(c);return c

    def expected_error(c,label,statement,states,args=None):
        query(c,'SAVEPOINT expected_failure')
        try:
            query(c,statement,args);got=None
        except Exception as exc:
            got=getattr(exc,'pgcode',None)
        finally:query(c,'ROLLBACK TO expected_failure');query(c,'RELEASE SAVEPOINT expected_failure')
        check(label,got in states)

    def metadata_snapshot():return scalar(admin,'SELECT max(snapshot_id) FROM created_metadata.ducklake_snapshot')
    def rows(c=None,table='orders'):return query(c or observer,'SELECT id,val FROM biz.'+table+' ORDER BY id')

    def head_identity(table='orders'):
        data=query(observer,"SELECT h.binding_id::text,h.relation_oid,h.row_head_oid,n.nspname,c.relname,c.relowner "
            "FROM og_lake_internal.row_lock_binding h JOIN og_lake_internal.relation_binding r "
            "ON r.relation_oid=h.relation_oid AND r.binding_id=h.binding_id JOIN pg_class c ON c.oid=h.row_head_oid "
            "JOIN pg_namespace n ON n.oid=c.relnamespace WHERE h.relation_oid=%s::regclass",('biz.'+table,))
        check('one canonical head mapping '+table,len(data),1)
        binding,host,oid,namespace,name,owner=data[0]
        check('head exact private namespace '+table,namespace,'og_lake_internal')
        return dict(binding=binding,host=host,oid=oid,owner=owner,sql=sql.Identifier(namespace,name))

    def layout(c=None):return [x[0]for x in query(c or observer,"SELECT attname FROM pg_attribute WHERE attrelid='og_lake_internal.row_lock_binding'::regclass AND attnum>0 AND NOT attisdropped ORDER BY attnum")]

    def mapping():
        cols=[x+'::text'if x in ['binding_id','last_reclaim_at','last_vacuum_at']else x for x in FIELDS]
        values=query(observer,'SELECT '+','.join(cols)+' FROM og_lake_internal.row_lock_binding WHERE relation_oid=%s',(head['host'],))
        if len(values)!=1:raise AssertionError('Exact mapping disappeared')
        return dict(zip(FIELDS,values[0]))

    def count_heads(predicate='true',c=None):return scalar(c or observer,sql.SQL('SELECT count(*) FROM {} WHERE '+predicate).format(head['sql']))

    def seed(first,count,c=None):
        query(c or admin,sql.SQL('INSERT INTO {}(row_id) SELECT i FROM generate_series(%s,%s)i ON CONFLICT DO NOTHING').format(head['sql']),(first,first+count-1))

    def batch(c=None,limit=512,table='orders'):
        return int(scalar(c or admin,'SELECT og_lake.reclaim_row_heads(%s::regclass,%s)',('biz.'+table,limit)))

    def until(label,predicate,seconds=None):
        deadline=time.monotonic()+(seconds or a.budget_seconds)
        while True:
            image=mapping();r['observations'].append(dict(label=label,at=time.monotonic(),state=image))
            if predicate(image):check(label,True);return image
            if time.monotonic()>=deadline:raise AssertionError('Bounded deadline: '+label)
            time.sleep(.1)

    def claim():
        nonlocal claimed
        deadline=time.monotonic()+a.budget_seconds
        while not scalar(admin,'SELECT og_lake_internal.test_maintenance_claim()'):
            if time.monotonic()>=deadline:raise AssertionError('Cannot claim owned database maintenance')
            time.sleep(.1)
        claimed=True

    def release():
        nonlocal claimed
        check('release only owned database maintenance claim',scalar(admin,'SELECT og_lake_internal.test_maintenance_release()'))
        claimed=False

    def async_sql(c,statement,args=None):
        # The only async operation is a deliberately blocked tuple-lock probe.
        # Bound it independently of large synchronous seed/vacuum statements.
        query(c,'SET statement_timeout=20000')
        def execute():
            try:return dict(state='ok',rows=query(c,statement,args))
            except Exception as exc:
                reply=dict(state='error',sqlstate=getattr(exc,'pgcode',None),message=str(exc));query(c,'ROLLBACK');return reply
        pool=ThreadPoolExecutor(max_workers=1);job=dict(conn=c,pool=pool,future=pool.submit(execute));jobs.append(job);return job

    def wait_lock(c,label,holder_xid,job):
        pid=actor_pids[id(c)]
        deadline=time.monotonic()+10
        while True:
            if job['future'].done():raise AssertionError('Waiter completed before the required GC XID wait: '+label)
            locks=query(observer,"SELECT locktype,mode,granted,relation,transactionid::text FROM pg_locks WHERE pid=%s AND locktype='transactionid' AND transactionid::text=%s AND NOT granted",(pid,holder_xid))
            if locks:r.setdefault('wait_evidence',[]).append(dict(label=label,pid=pid,gc_xid=holder_xid,locks=locks));check(label,True);return
            if time.monotonic()>=deadline:raise AssertionError('No real wait: '+label)
            time.sleep(.02)

    try:
        from psycopg2 import sql
        b=check_build(a.build_receipt);r.update(source_head=b['source_head'],binary_sha256=b['binary_sha256'],
            build=str(a.build_receipt.resolve()),build_sha256=sha256(a.build_receipt))
        control=connect_control();configure(control)
        enabled=scalar(control,'SHOW enable_lake_maintenance')
        if a.mode in ['auto','fairness','retention','vacuum','cancel']:
            check('automatic case requires an already enabled owned launcher',enabled,'on')
        with(out/'fixture.log').open('w')as log:
            subprocess.run([sys.executable,str(D/'tests/catalog_fixture_bundle.py'),'--run-id',a.run_id,
                '--output',str(out/'fixture/manifest.json')],stdout=log,stderr=subprocess.STDOUT,check=True,timeout=300)
        bundle=validate_empty_bundle(out/'fixture/manifest.json');empty,exported=validate_empty_manifest(bundle['entries']['same']['manifest'])
        reservation=(ROOT/'.reference/opengauss-native-lake/.driver.lock').open('a+');fcntl.flock(reservation.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB)
        db=OwnedDatabase(a.run_id,out);db.create();configure(db.admin);roles=OwnedRoles(control,a.run_id,out)
        admin,observer=conn(),conn()
        query(admin,"CREATE EXTENSION og_lake VERSION '"+('0.17'if a.mode=='migration'else a.sql_version)+"'")
        actual_runtime=json.loads(scalar(admin,'SELECT og_lake.host_status()'))['module_version']
        r['runtime_module']=actual_runtime
        check('exact Native module'+a.runtime_version,actual_runtime,a.runtime_version)
        check('exact selected installed SQL version',scalar(admin,"SELECT extversion FROM pg_extension WHERE extname='og_lake'"),'0.17'if a.mode=='migration'else a.sql_version)
        claim();private=roles.create('private',False)
        query(admin,'SELECT og_lake.configure_internal_owner(%s::name)',(private,))
        query(admin,"SELECT og_lake.create_catalog('created_lake'::name,'created_metadata'::name,%s::text,NULL::text)",(empty['data_path'],))
        native=export_created_catalog(admin,'created_metadata',exported,out/'native-initial.json')
        r['initial_independent_comparison']=compare_initialized(exported,native,empty['data_path'],'openGauss og_lake '+a.runtime_version)
        query(admin,'CREATE SCHEMA biz')
        query(admin,"CREATE FOREIGN TABLE biz.orders(id integer,val integer) SERVER created_lake OPTIONS(binding_mode 'create',lake_schema 'main')")
        query(admin,'INSERT INTO biz.orders VALUES(1,10),(2,20),(3,30)')
        head=head_identity();r['head_identity']={k:v for k,v in head.items()if k!='sql'}
        actor_pids={id(c):scalar(c,'SELECT pg_backend_pid()')for c in actors}

        if a.mode=='migration':
            check('SQL17 retains exactly three mapping fields',layout(),FIELDS[:3])
            query(admin,'UPDATE biz.orders SET val=val+1 WHERE id=1')
            before=query(observer,sql.SQL('SELECT row_id FROM {} ORDER BY row_id').format(head['sql']));snapshot=metadata_snapshot()
            query(admin,'BEGIN');query(admin,'SAVEPOINT upgrade')
            query(admin,"ALTER EXTENSION og_lake UPDATE TO '0.18'")
            check('migration installs exact16 fields',layout(admin),FIELDS)
            query(admin,'ROLLBACK TO upgrade');query(admin,'RELEASE SAVEPOINT upgrade');query(admin,'COMMIT')
            check('upgrade rollback restores SQL17 version',scalar(observer,"SELECT extversion FROM pg_extension WHERE extname='og_lake'"),'0.17')
            check('upgrade rollback restores three fields',layout(),FIELDS[:3])
            query(admin,"ALTER EXTENSION og_lake UPDATE TO '0.18'")
            check('committed migration installs exact16 fields',layout(),FIELDS)
            migrated=head_identity();identity_fields=['binding','host','oid','owner']
            check('migration preserves stable head OID/owner/binding',{k:migrated[k]for k in identity_fields},{k:head[k]for k in identity_fields})
            initial_state=mapping()
            check('migration backfills exact initial maintenance state',{k:initial_state[k]for k in FIELDS[3:]},dict(reclaim_cursor=-1,reclaim_round_end=-1,reclaim_scanned=0,reclaimed_rows=0,reclaim_rounds=0,last_reclaim_at=None,last_vacuum_at=None,live_rows_estimate=None,dead_rows_estimate=None,head_bytes=None,soft_budget_exceeded=False,last_delete_xid=None,oldest_xmin_observed=None))
            check('migration preserves every existing key',query(observer,sql.SQL('SELECT row_id FROM {} ORDER BY row_id').format(head['sql'])),before)
            check('constant backfill defaults were removed',scalar(observer,"SELECT count(*) FROM pg_attrdef WHERE adrelid='og_lake_internal.row_lock_binding'::regclass"),0)
            check('migration does not publish business snapshot',metadata_snapshot(),snapshot)
            check('migrated RC DML still works',query(admin,'UPDATE biz.orders SET val=val+1 WHERE id=1 RETURNING id,val'),[(1,12)])
        else:
            expected_fields=FIELDS+(['leaf_partition_oid','partition_no']if a.sql_version in ['0.23','0.26']else [])
            check('fresh SQL'+a.sql_version[2:]+' exact'+str(len(expected_fields))+' fields',layout(),expected_fields)
            if a.sql_version in ['0.23','0.26']:
                check('ordinary row-head map retains typed identity zero/zero',query(observer,
                    'SELECT leaf_partition_oid::bigint,partition_no FROM og_lake_internal.row_lock_binding WHERE relation_oid=%s',
                    (head['host'],)),[(0,0)])
            snapshot=metadata_snapshot()
            if a.mode in ['manual','lifecycle']:
                seed(1000,640);original=count_heads()
                query(admin,'BEGIN');query(admin,'SAVEPOINT batch')
                removed=batch(limit=17);check('manual requested bound',0<=removed<=17)
                check('manual return equals actual own-visible deletion',original-count_heads(c=admin),removed)
                query(admin,'ROLLBACK TO batch');query(admin,'RELEASE SAVEPOINT batch');query(admin,'COMMIT')
                check('manual savepoint restores all head rows',count_heads(),original)
                check('manual savepoint restores persistent cursor',mapping()['reclaim_scanned'],0)
                query(admin,'BEGIN')
                expected_error(admin,'reject zero bound','SELECT og_lake.reclaim_row_heads(%s::regclass,0)',{'22023'},('biz.orders',))
                expected_error(admin,'reject >512 bound','SELECT og_lake.reclaim_row_heads(%s::regclass,513)',{'22023'},('biz.orders',))
                query(admin,'CREATE TABLE biz.ordinary(id integer)')
                expected_error(admin,'reject ordinary heap','SELECT og_lake.reclaim_row_heads(%s::regclass,512)',{'42809'},('biz.ordinary',))
                query(admin,'COMMIT')
                query(admin,'BEGIN READ ONLY');expected_error(admin,'reject readonly transaction','SELECT og_lake.reclaim_row_heads(%s::regclass,512)',{'25006'},('biz.orders',));query(admin,'ROLLBACK')
                query(admin,'BEGIN');query(admin,'SAVEPOINT mutation');batch(limit=1)
                expected_error(admin,'maintenance marker blocks DML','UPDATE biz.orders SET val=val+1 WHERE id=1',{'0A000'})
                expected_error(admin,'maintenance marker blocks pending CREATE',"CREATE FOREIGN TABLE biz.forbidden_create(id integer,val integer) SERVER created_lake OPTIONS(binding_mode 'create',lake_schema 'main')",{'0A000'})
                expected_error(admin,'maintenance marker blocks flush',"SELECT og_lake.flush('biz.orders'::regclass)",{'0A000'})
                query(admin,'ROLLBACK TO mutation');query(admin,'RELEASE SAVEPOINT mutation')
                check('rollback mutation marker permits later DML',query(admin,'UPDATE biz.orders SET val=val+1 WHERE id=1 RETURNING id,val'),[(1,11)])
                expected_error(admin,'prior DML blocks metadata maintenance','SELECT og_lake.reclaim_row_heads(%s::regclass,1)',{'0A000'},('biz.orders',));query(admin,'ROLLBACK')
                query(admin,'BEGIN');query(admin,'SAVEPOINT created')
                query(admin,"CREATE FOREIGN TABLE biz.pending_mix(id integer,val integer) SERVER created_lake OPTIONS(binding_mode 'create',lake_schema 'main')")
                expected_error(admin,'pending CREATE blocks metadata maintenance','SELECT og_lake.reclaim_row_heads(%s::regclass,1)',{'0A000'},('biz.orders',))
                query(admin,'ROLLBACK TO created');query(admin,'RELEASE SAVEPOINT created');query(admin,'COMMIT')
                query(admin,'BEGIN');query(admin,"SELECT og_lake.flush('biz.orders'::regclass)")
                expected_error(admin,'prepared flush blocks metadata maintenance','SELECT og_lake.reclaim_row_heads(%s::regclass,1)',{'0A000'},('biz.orders',));query(admin,'ROLLBACK')
                drain_deadline=time.monotonic()+a.budget_seconds
                while count_heads()>0:
                    if time.monotonic()>=drain_deadline:raise AssertionError('manual bounded drain made insufficient progress')
                    before=count_heads();removed=batch();check('single committed batch bounded',0<=removed<=512)
                    check('manual actual committed count',before-count_heads(),removed)
                check('manual head GC leaves business rows unchanged',rows(),[(1,10),(2,20),(3,30)])
                check('manual metadata work never publishes a lake snapshot',metadata_snapshot(),snapshot)
                if a.mode=='lifecycle':
                    seed(5000,4);old_mapping=mapping()
                    query(admin,'BEGIN');query(admin,'SAVEPOINT dropped');query(admin,'DROP FOREIGN TABLE biz.orders')
                    check('DROP removes exact mapping',scalar(admin,'SELECT count(*) FROM og_lake_internal.row_lock_binding WHERE relation_oid=%s',(head['host'],)),0)
                    query(admin,'ROLLBACK TO dropped');query(admin,'RELEASE SAVEPOINT dropped');query(admin,'COMMIT')
                    check('DROP rollback restores exact mapping/cursor',mapping(),old_mapping)
                    check('DROP rollback retains exact physical head OID',head_identity()['oid'],head['oid'])
                    user=roles.create('table_owner',True)
                    query(admin,sql.SQL('GRANT USAGE,CREATE ON SCHEMA biz TO {}').format(sql.Identifier(user)))
                    query(admin,sql.SQL('GRANT USAGE ON FOREIGN SERVER created_lake TO {}').format(sql.Identifier(user)))
                    ordinary=ordinary_connection(db.database,user,roles.passwords['table_owner']);actors.append(ordinary);configure(ordinary)
                    query(ordinary,"CREATE FOREIGN TABLE biz.owned(id integer,val integer) SERVER created_lake OPTIONS(binding_mode 'create',lake_schema 'main')")
                    query(ordinary,'INSERT INTO biz.owned VALUES(7,70)');query(ordinary,'UPDATE biz.owned SET val=val+1 WHERE id=7')
                    check('table owner can reclaim its real head',batch(ordinary,512,'owned'),1)
                    query(ordinary,'BEGIN');expected_error(ordinary,'nonowner cannot reclaim another business table','SELECT og_lake.reclaim_row_heads(%s::regclass,512)',{'42501'},('biz.orders',))
                    expected_error(ordinary,'ordinary owner cannot read private registry','SELECT * FROM og_lake_internal.row_lock_binding',{'42501'});query(ordinary,'ROLLBACK')
                    query(admin,'DROP FOREIGN TABLE biz.owned');query(admin,'DROP FOREIGN TABLE biz.orders')
                    check('final DROP removes canonical host and head',scalar(observer,'SELECT count(*) FROM pg_class WHERE oid IN(%s,%s)',(head['host'],head['oid'])),0)
            elif a.mode=='protocol':
                # A real registered head and the real manual GC entry, with an
                # explicit ordinary protocol waiter. This does NOT claim the
                # opaque native UPDATE loop reached its own 32-attempt bound.
                seed(0,1);old=scalar(observer,sql.SQL('SELECT ctid::text FROM {} WHERE row_id=0').format(head['sql']))
                waiter=conn();actor_pids[id(waiter)]=scalar(waiter,'SELECT pg_backend_pid()');query(waiter,'BEGIN')
                query(waiter,sql.SQL('INSERT INTO {} VALUES(0) ON CONFLICT DO NOTHING').format(head['sql']))
                query(admin,'BEGIN');check('real manual GC marks old head deleted',batch(limit=1),1)
                holder_xid=str(scalar(admin,'SELECT txid_current()'))
                check('GC transaction owns its exact XID lock',scalar(observer,"SELECT count(*) FROM pg_locks WHERE pid=%s AND locktype='transactionid' AND mode='ExclusiveLock' AND granted AND transactionid::text=%s",(actor_pids[id(admin)],holder_xid)),1)
                job=async_sql(waiter,sql.SQL('SELECT ctid::text FROM {} WHERE row_id=0 FOR UPDATE').format(head['sql']));wait_lock(waiter,'old head waiter blocks real manual GC',holder_xid,job)
                query(admin,'COMMIT');check('actual native tuple deletion produces zero locked rows',job['future'].result(timeout=15),dict(state='ok',rows=[]))
                query(waiter,sql.SQL('INSERT INTO {} VALUES(0) ON CONFLICT DO NOTHING').format(head['sql']))
                new=scalar(waiter,sql.SQL('SELECT ctid::text FROM {} WHERE row_id=0 FOR UPDATE').format(head['sql']))
                check('ensure locks a new physical carrier',new!=old)
                check('production DML respects the recovered same-transaction head',query(waiter,'UPDATE biz.orders SET val=val+1 WHERE id=1 RETURNING id,val'),[(1,11)])
                query(waiter,'COMMIT');check('protocol recovery preserves exactly one business version',rows(),[(1,11),(2,20),(3,30)])
                r['native_32_retry_exhaustion_verified']=False
            else:
                # Manual flush under a real maintenance claim is safe and
                # must not remove/recreate the synchronization relation.
                query(admin,"SELECT og_lake.flush('biz.orders'::regclass)")
                check('manual flush retains exact canonical head heap',head_identity()['oid'],head['oid'])
                if a.mode in ['auto','fairness']:
                    query(admin,'UPDATE biz.orders SET val=val+1 WHERE id=1')
                    writer=conn();actor_pids[id(writer)]=scalar(writer,'SELECT pg_backend_pid()');query(writer,'BEGIN')
                    query(writer,'UPDATE biz.orders SET val=val+1 WHERE id=1')
                    seed(1000,640);release()
                    until('automatic worker passes a busy low head and reclaims high window',lambda m:count_heads('row_id=1639')==0)
                    check('active writer head survives real SKIP LOCKED',count_heads('row_id=0'),1)
                    if a.mode=='fairness':
                        claim();seed(100000,40)
                        # Establish an actual in-progress round whose cursor
                        # has passed the future low key before inserting it.
                        deadline=time.monotonic()+a.budget_seconds
                        while True:
                            image=mapping()
                            if image['reclaim_cursor']>=100000 and image['reclaim_round_end']>=100000:break
                            if time.monotonic()>=deadline:raise AssertionError('Cannot establish fixed-round fairness barrier')
                            removed=batch(limit=1);check('fairness barrier uses one bounded native batch',0<=removed<=1)
                        r['fairness_barrier']=image;seed(500,1)
                        check('new low key lies behind the real persistent cursor',500<image['reclaim_cursor']<=image['reclaim_round_end'])
                        check('new low key is visible before releasing worker',count_heads('row_id=500'),1)
                        release()
                        until('fixed round wraps to a lower newly inserted key',lambda m:m['reclaim_rounds']>image['reclaim_rounds'] and count_heads('row_id=500')==0)
                    query(writer,'COMMIT');until('idle former writer head becomes reclaimable',lambda m:count_heads('row_id=0')==0)
                    check('automatic maintenance preserves committed business update',rows(),[(1,12),(2,20),(3,30)])
                elif a.mode in ['retention','vacuum']:
                    reader=None
                    if a.mode=='retention':
                        reader=conn()
                        baseline=json.loads(scalar(reader,'SELECT og_lake_internal.test_xact_state()'))
                        r['retained_cursor_snapshot_baseline']=baseline
                        check('fresh reader has no lake transaction tables',baseline['transaction_tables'],0)
                        query(reader,'BEGIN READ ONLY')
                        transaction_baseline=json.loads(scalar(reader,'SELECT og_lake_internal.test_xact_state()'))
                        r['retained_cursor_snapshot_transaction_baseline']=transaction_baseline
                        query(reader,'DECLARE retained NO SCROLL CURSOR FOR SELECT * FROM biz.orders');query(reader,'FETCH 1 FROM retained')
                        references=json.loads(scalar(reader,'SELECT og_lake_internal.test_xact_state()')).get('registered_snapshots')
                        check('real retained cursor adds registered snapshots beyond diagnostic SELECT',isinstance(references,int)and references>transaction_baseline['registered_snapshots'])
                    seed(10000,20000)
                    heap_before,index_before,total_before=[int(x)for x in query(observer,'SELECT pg_relation_size(%s::regclass),pg_indexes_size(%s::regclass),pg_total_relation_size(%s::regclass)',(head['oid'],head['oid'],head['oid']))[0]]
                    release();drained=until('automatic logical reclamation drains 20k heads',lambda m:count_heads()==0)
                    if reader is not None:
                        # Require a completed vacuum observation AFTER logical
                        # drain, rather than stale stats from an earlier batch.
                        until('post-drain vacuum still observes retained cursor backlog',lambda m:m['last_vacuum_at']is not None and m['last_vacuum_at']!=drained['last_vacuum_at'] and (m['dead_rows_estimate']or 0)>0 and m['last_delete_xid']is not None and m['oldest_xmin_observed']<=m['last_delete_xid'])
                        query(reader,'CLOSE retained')
                        after_close=json.loads(scalar(reader,'SELECT og_lake_internal.test_xact_state()'))
                        r['retained_cursor_snapshot_after_close']=after_close
                        check('CLOSE alone releases cursor snapshots before transaction cleanup',after_close['registered_snapshots'],transaction_baseline['registered_snapshots'])
                        check('read-only cursor creates no lake row journal',after_close['transaction_rows'],0)
                        query(reader,'COMMIT')
                        # The diagnostic SELECT itself registers native portal and
                        # executor snapshots. Compare identical statements,
                        # not the impossible zero while that SELECT executes.
                        closed=json.loads(scalar(reader,'SELECT og_lake_internal.test_xact_state()'))
                        r['retained_cursor_snapshot_after_commit']=closed
                        check('cursor transaction releases snapshots back to diagnostic baseline',closed['registered_snapshots'],baseline['registered_snapshots'])
                        check('cursor transaction releases lake catalog state',closed['transaction_catalogs'],0)
                        check('cursor transaction releases lake table state',closed['transaction_tables'],0)
                        check('cursor transaction releases lake row state',closed['transaction_rows'],0)
                        check('cursor transaction releases staged bytes',closed['staged_bytes'],0)
                    if a.mode=='vacuum' and a.sql_version in ['0.23','0.26']:
                        # Native lazy VACUUM deliberately retains small LP_DEAD
                        # index batches.  One original observation window must
                        # see a safe cutoff, then a later completed worker vacuum
                        # at the same deletion frontier.  Actual page reuse below
                        # is the space criterion; an estimate is not an exact
                        # physical-clearance counter.
                        barrier={'first_safe':None,'later_vacuum':None}
                        r['vacuum_reuse_barrier']=barrier
                        def completed_after_safe_cutoff(m):
                            if(m['last_vacuum_at']is None or m['last_vacuum_at']==drained['last_vacuum_at'] or
                               m['last_delete_xid']is None or m['oldest_xmin_observed']is None or
                               m['oldest_xmin_observed']<=m['last_delete_xid'] or m['reclaimed_rows']<20000):
                                return False
                            first=barrier['first_safe']
                            if first is None or first['last_delete_xid']!=m['last_delete_xid']:
                                barrier['first_safe']=dict(m)
                                return False
                            if m['last_vacuum_at']==first['last_vacuum_at']:return False
                            barrier['later_vacuum']=dict(m)
                            return True
                        until('native worker completes later vacuum after safe cutoff',completed_after_safe_cutoff)
                    else:
                        until('native worker vacuum reaches safe cutoff and removes dead versions',lambda m:m['last_vacuum_at']is not None and m['dead_rows_estimate']==0 and m['last_delete_xid']is not None and m['oldest_xmin_observed']>m['last_delete_xid'])
                    claim()
                    if a.mode=='vacuum' and a.sql_version in ['0.23','0.26']:
                        check('actual old head population is empty before reuse',count_heads(),0)
                    seed(10000,20000)
                    if a.mode=='vacuum' and a.sql_version in ['0.23','0.26']:
                        check('actual next head population is exactly twenty thousand',count_heads(),20000)
                        check('vacuum reuse preserves all business values',rows(),[(1,10),(2,20),(3,30)])
                    heap_after,index_after,total_after=[int(x)for x in query(observer,'SELECT pg_relation_size(%s::regclass),pg_indexes_size(%s::regclass),pg_total_relation_size(%s::regclass)',(head['oid'],head['oid'],head['oid']))[0]]
                    r['space_reuse']=dict(rows=20000,heap_before=heap_before,heap_after=heap_after,index_before=index_before,index_after=index_after,total_before=total_before,total_after=total_after,total_bytes_per_live_head_after=total_after/20000.0)
                    check('native vacuum makes next head batch reuse heap pages',heap_after<=heap_before+8192)
                    check('GC does not delete canonical relation',head_identity()['oid'],head['oid'])
                elif a.mode=='cancel':
                    seed(1000,640);blocker=conn();query(blocker,'BEGIN');query(blocker,'LOCK TABLE og_lake_internal.row_lock_binding IN ACCESS EXCLUSIVE MODE');release()
                    deadline=time.monotonic()+15;victim=None
                    while time.monotonic()<deadline:
                        workers=query(observer,"SELECT a.pid FROM pg_stat_activity a JOIN pg_locks l ON l.pid=a.pid WHERE a.datid=%s AND a.application_name='og_lake flush worker' AND NOT l.granted AND l.relation='og_lake_internal.row_lock_binding'::regclass",(db.allocation['database_oid'],))
                        if workers:victim=workers[0][0];break
                        time.sleep(.02)
                    check('cancel target is an actually waiting owned database worker',victim is not None)
                    check('cancel only exact owned worker',scalar(observer,'SELECT pg_cancel_backend(%s)',(victim,)))
                    query(blocker,'ROLLBACK')
                    until('worker recovers after cancellation/cooldown and makes progress',lambda m:m['reclaimed_rows']>0)
                    r['cancellation_scope']='real worker metadata-admission wait; not a forced mid-delete cancellation proof'
                check('automatic modes preserve exact three business IDs',query(observer,'SELECT id,count(*) FROM biz.orders GROUP BY id ORDER BY id'),[(1,1),(2,1),(3,1)])
        check('all executed input hashes unchanged',all(sha256(Path(f))==h for f,h in r['inputs'].items()))
        check_build(a.build_receipt)
        if ordinary26:require_actual(a,a.mode)
        r['status']='pass'
    except BaseException as exc:r.update(reason=str(exc),traceback=traceback.format_exc())
    finally:
        stop.set()
        busy={id(j['conn'])for j in jobs if not j['future'].done()}
        # Release same-test lock holders before draining a cancelled waiter.
        # Never use one connection concurrently from the main and worker thread.
        for c in actors:
            if id(c)not in busy:
                try:query(c,'ROLLBACK')
                except Exception:pass
        for j in jobs:
            if not j['future'].done():
                try:j['conn'].cancel()
                except Exception:pass
        unfinished=set()
        for j in jobs:
            try:j['future'].result(timeout=25)
            except TimeoutError:unfinished.add(id(j['conn']))
            except Exception:pass
            j['pool'].shutdown(wait=False)
        if unfinished:r.update(status='fail',cleanup_error='Owned async waiter did not stop after holder rollback/cancel; database preserved')
        if claimed:
            try:query(admin,'SELECT og_lake_internal.test_maintenance_release()')
            except Exception:pass
        for c in actors:
            if id(c)in unfinished:continue
            try:query(c,'ROLLBACK');c.close()
            except Exception:pass
        if db is not None and not unfinished:
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
