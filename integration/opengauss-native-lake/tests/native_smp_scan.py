#!/usr/bin/env python3
"""First real Native Stream lake scan acceptance draft (not executed in stage).

Requires an official Debug full whose exact input map includes lake_stream.cpp.
Full multisets, real Stream plans/task logs, fixed statement binding, worker-held
SDK batch cancellation and partial-start cleanup. Extra SQL connections only
control the deterministic concurrency gate; they never implement the scan.
No vector-batch, parameterized SMP rescan, join-SMP, remote/RAC or performance
qualification follows from this bounded first whole-file slice.
"""
from copy import deepcopy
import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import hashlib,json,os
from pathlib import Path
import re,shutil,signal,subprocess,sys,time,traceback
from types import SimpleNamespace
ROOT=Path(os.getenv('CPP_WORKSPACE',str(Path(__file__).resolve().parents[3]))).resolve()
D=ROOT/'integration/opengauss-native-lake';sys.path[:0]=[str(D),str(D/'tests')]
from file_dml_acceptance import check_build
from inline_fixture import query,scalar
from lake_harness import sha256,validate_run_id,write_json
from native_auto_flush import connect_control
from native_runtime import PROFILE,RUNTIME,SOURCE
from native_runtime_log import NativeLogSource
from native_table_create import Case
from native_test_context import OwnedDatabase
from permission_api import OwnedRoles,cleanup_roles

TASK=re.compile(r'lake SMP task query=(\d+) scan=(\d+) snapshot=(\d+) participant=(\d+)/(\d+) task=(\d+)(?: file=(\d+))? inline=([01])')
DONE=re.compile(r'lake SMP completion query=(\d+) scan=(\d+) snapshot=(\d+) participant=(\d+)/(\d+) claimed=(\d+) done=(\d+) rows=(\d+)')



def supervised_case(args):
    """Hard process deadline also covers a broken server-side cancellation.

    Only this owned test process group is killed. Native services and owned
    database/roles are retained on timeout; original partial receipt survives.
    """
    out=ROOT/'公共文档/openGauss原生湖存储/验证'/args.run_id
    if out.exists():raise FileExistsError('Refusing to reuse an existing case directory: '+str(out))
    env=dict(os.environ);env['CPP_WORKSPACE']=str(ROOT)
    env['OG_LAKE_SMP_ISOLATED_CHILD_PARENT']=str(os.getpid())
    command=[sys.executable,str(Path(__file__).resolve()),*sys.argv[1:],'--_isolated-child']
    process=subprocess.Popen(command,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,
                             text=True,env=env,start_new_session=True)
    reason=None;interrupted=False;kill_sent=False;drain_incomplete=False;started=time.monotonic()
    try:
        output,_=process.communicate(timeout=args.case_timeout)
    except (subprocess.TimeoutExpired,KeyboardInterrupt) as exc:
        interrupted=isinstance(exc,KeyboardInterrupt)
        reason='controller interrupted' if interrupted else 'isolated case exceeded hard process deadline'
        # A separate session makes the target exactly our child/fixture group;
        # gaussdb was started externally and cannot belong to this group.
        try:os.killpg(process.pid,signal.SIGKILL);kill_sent=True
        except ProcessLookupError:pass
        try:output,_=process.communicate(timeout=10)
        except subprocess.TimeoutExpired as drain:
            drain_incomplete=True;output=drain.output or ''
            if isinstance(output,bytes):output=output.decode('utf-8','replace')
            if process.stdout is not None:process.stdout.close()
            try:process.wait(timeout=1)
            except subprocess.TimeoutExpired:pass
    out.mkdir(parents=True,exist_ok=True)
    (out/'isolated-child.stdout').write_text(output or '')
    child_result=out/'result.json'
    if child_result.exists():
        shutil.copy2(child_result,out/'isolated-child-result.json')
        try:result=json.loads(child_result.read_text())
        except (ValueError,OSError):result=dict(status='fail',assertions=[],reason='child result was incomplete')
    else:result=dict(status='fail',assertions=[],reason='child exited without a result receipt')
    supervision=dict(command=command,child_pid=process.pid,returncode=process.returncode,
                     hard_deadline_seconds=args.case_timeout,elapsed_seconds=round(time.monotonic()-started,3),
                     test_process_group_sigkill_sent=kill_sent,pipe_drain_incomplete=drain_incomplete,
                     automatic_database_cleanup_on_timeout=False,
                     original_child_result='isolated-child-result.json' if child_result.exists() else None)
    if reason is not None or process.returncode!=0:
        result['status']='fail'
        if reason is not None:
            result['reason']=reason
            result['preserve_owned_scene']=True
            supervision['retained_scene']='database/roles/backend may remain; no controller SQL cleanup attempted'
    result['isolation_controller']=supervision
    write_json(out/'controller.json',supervision);write_json(out/'result.json',result)
    print(json.dumps(dict(status=result['status'],checks=len(result.get('assertions',[])),reason=result.get('reason'),
                          result=str(out/'result.json')),ensure_ascii=False))
    return 130 if interrupted else 0 if result['status']=='pass' else 1


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--run-id',required=True);p.add_argument('--build-receipt',required=True,type=Path)
    p.add_argument('--sql-version',choices=['0.17','0.18'],default='0.17')
    p.add_argument('--rows-per-file',type=int,default=8192)
    p.add_argument('--case-timeout',type=int,default=600,help='Hard whole-case process deadline, seconds')
    p.add_argument('--_isolated-child',action='store_true',help=argparse.SUPPRESS)
    a=p.parse_args();validate_run_id(a.run_id)
    if PROFILE!='debug':p.error('Only the owned Debug full is admitted')
    if not 8192<=a.rows_per_file<=16384:p.error('Each of four bounded files must contain 8192..16384 rows')
    if not 120<=a.case_timeout<=1800:p.error('--case-timeout must be120..1800 seconds')
    if not a._isolated_child:return supervised_case(a)
    if os.environ.get('OG_LAKE_SMP_ISOLATED_CHILD_PARENT')!=str(os.getppid()):
        p.error('Internal child mode must be launched by the supervised controller')
    out=ROOT/'公共文档/openGauss原生湖存储/验证'/a.run_id;out.mkdir(exist_ok=False)
    r=dict(status='fail',scope=__doc__,assertions=[],native_runtime_executed=False,sql_version=a.sql_version)
    db=roles=control=t=None;log_source=None
    preserve_owned_scene=False;exit_without_thread_join=False
    inputs=[Path(__file__),D/'tests/file_dml_acceptance.py',D/'native_test_context.py',D/'native_runtime.py',D/'inline_fixture.py',
        D/'tests/native_table_create.py',D/'tests/permission_api.py',D/'tests/inline_cleanup.py',D/'tests/native_auto_flush.py',D/'tests/native_runtime_log.py',
        D/'tests/catalog_fixture_bundle.py',D/'lake_harness.py',D/'source-lock.json',a.build_receipt]
    r['inputs']={str(f):sha256(f)for f in inputs};archive=out/'executed-inputs';archive.mkdir()
    for i,f in enumerate(inputs):shutil.copy2(f,archive/(str(i)+'-'+f.name))
    def short(x):
        if isinstance(x,list)and len(x)>20:return dict(length=len(x),sha256=hashlib.sha256(repr(x).encode()).hexdigest())
        return x
    def check(name,got,want=True):
        good=got==want;r['assertions'].append(dict(name=name,observed=deepcopy(short(got)),expected=deepcopy(short(want)),status='pass'if good else'fail'))
        write_json(out/'result.json',r)
        if not good:raise AssertionError(name+': '+repr(short(got)))
    def error(c,statement,code,args=None):
        try:query(c,statement,args)
        except Exception as exc:check('SQLSTATE/'+statement,getattr(exc,'pgcode',None),code);return
        check('expected error/'+statement,False)
    def configure(c,dop=1):
        query(c,'SET session_timeout=0');query(c,'SET statement_timeout=120000');query(c,'SET enable_vector_engine=off')
        query(c,'SET enable_lake_smp=on');query(c,'SET query_dop='+str(1000+dop if dop>1 else 1))
        query(c,'SET enable_indexscan=off');query(c,'SET enable_bitmapscan=off')
    def stats(mode=0,participant=0,gate=0):return json.loads(scalar(t.a,'SELECT biz.smp_probe(%s,%s,%s)',(mode,participant,gate)))
    def log_offset():return log_source.mark()
    def read_log(offset,label,qid,scan,task_count=None,done_count=None):
        def ready(text):
            tasks=[m for m in TASK.finditer(text)if int(m[1])==qid and int(m[2])==scan]
            done=[m for m in DONE.finditer(text)if int(m[1])==qid and int(m[2])==scan]
            return (task_count is None or len(tasks)>=task_count)and(done_count is None or len(done)>=done_count)
        try:text=offset.wait(ready,5.0)
        finally:write_json(out/(label+'-log-source.json'),offset.receipt())
        check(label+' bounded owned server log',offset.read_bytes<=16*1024*1024)
        (out/(label+'-server.log')).write_text(text);return text
    def completed(label,state,offset,want_dop,full_population=None):
        check(label+' actual producer degree',state['participants'],want_dop)
        check(label+' every producer started once',[x['started']for x in state['workers']],[1]*want_dop)
        check(label+' every task owner claimed once',[x['claimed']for x in state['workers']],[1]*want_dop)
        check(label+' every reader opened',[x['reader_ready']for x in state['workers']],[1]*want_dop)
        check(label+' every participant ended normally',[x['done']for x in state['workers']],[1]*want_dop)
        if full_population is not None:check(label+' exact combined visible scanner population',sum(x['rows']for x in state['workers']),full_population)
        qid,scan=state['query'],state['scan']
        offset_task=int(state['inline_task']);expected=[]
        if offset_task:expected.append((qid,scan,state['snapshot'],0,want_dop,0,-1,1))
        for i,fid in enumerate(state['files']):expected.append((qid,scan,state['snapshot'],(i+offset_task)%want_dop,want_dop,i+offset_task,fid,0))
        text=read_log(offset,label,qid,scan,len(expected),want_dop)
        rows=[tuple(map(int,m.groups(default='-1')))for m in TASK.finditer(text)if int(m[1])==qid and int(m[2])==scan]
        check(label+' each file/inline task assigned exactly once',sorted(rows),sorted(expected))
        done=[tuple(map(int,m.groups()))for m in DONE.finditer(text)if int(m[1])==qid and int(m[2])==scan]
        check(label+' one joined completion per participant',len(done),want_dop)
        return state
    def capture(label,statement,expected,dop=4,args=None,full_population=None):
        configure(t.a,dop);stats(-1);before=log_offset();got=query(t.a,statement,args)
        check(label+' complete rows/multiplicities',got,expected);state=stats();completed(label,state,before,dop,full_population);return state
    statement='SELECT id,val FROM biz.orders ORDER BY id,val NULLS FIRST'
    heap_statement=statement.replace('biz.orders','biz.exact')
    try:
        b=check_build(a.build_receipt);r.update(source_head=b['source_head'],binary_sha256=b['binary_sha256'])
        check('official source includes this real SMP implementation','contrib/og_lake/lake_stream.cpp'in b['native_source_inputs_after'])
        check('frozen acceptance source belongs to chosen build',sha256(SOURCE/'contrib/og_lake/lake_stream.cpp'),b['native_source_inputs_after']['contrib/og_lake/lake_stream.cpp'])
        control=connect_control();query(control,'SET session_timeout=0');query(control,'SET statement_timeout=30000');check('automatic maintenance remains off',scalar(control,'SHOW enable_lake_maintenance'),'off')
        check('deterministic internal diagnostic uses ordinary backend mode',scalar(control,'SHOW enable_thread_pool'),'off')
        log_source=NativeLogSource.from_connection(control,RUNTIME)
        r['native_log_source']=log_source.identity
        with(out/'fixture.log').open('w')as f:subprocess.run([sys.executable,str(D/'tests/catalog_fixture_bundle.py'),'--run-id',a.run_id,'--output',str(out/'fixture/manifest.json')],stdout=f,stderr=subprocess.STDOUT,timeout=300,check=True)
        db=OwnedDatabase(a.run_id,out)
        db.create()
        roles=OwnedRoles(control,a.run_id,out)
        t=Case(db,SimpleNamespace(mode='net_dml',sql_version=a.sql_version,manifest=out/'fixture/manifest.json',output=out/'result.json',run_id=a.run_id),r,roles)
        for c in [t.a,t.b,t.observer]:configure(c)
        r['native_runtime_executed']=True
        query(t.a,"CREATE FUNCTION biz.smp_probe(integer,integer,bigint) RETURNS text AS '$libdir/og_lake','og_lake_test_smp_probe' LANGUAGE C VOLATILE NOT FENCED")
        query(t.a,'REVOKE ALL ON FUNCTION biz.smp_probe(integer,integer,bigint) FROM PUBLIC')
        t.create();query(t.a,'CREATE TABLE biz.exact(id integer,val integer)')
        query(t.a,"SELECT og_lake.set_option('biz.orders'::regclass,'data_inlining_row_limit','100000')")
        configure(t.a,4);stats(-1);check('empty lake read remains complete',query(t.a,statement),[]);check('empty input stays serial',stats()['participants'],0)
        configure(t.a)
        query(t.a,'INSERT INTO biz.orders VALUES(7,70),(7,70),(NULL,NULL),(9,NULL)')
        query(t.a,'INSERT INTO biz.exact VALUES(7,70),(7,70),(NULL,NULL),(9,NULL)')
        configure(t.a,4);stats(-1);check('pure inline duplicate/NULL rows',query(t.a,statement),query(t.observer,heap_statement));check('single inline task stays serial',stats()['participants'],0)
        configure(t.a)
        # Four independent host commits/flushes produce four complete immutable
        # multi-rowgroup files through the real SDK, with no catalog forging.
        for source in range(4):
            first=source*a.rows_per_file+1;last=(source+1)*a.rows_per_file
            values='SELECT i%257,CASE WHEN i%13=0 THEN NULL ELSE i%101 END FROM generate_series('+str(first)+','+str(last)+') AS i'
            for table in ['biz.orders','biz.exact']:query(t.a,'INSERT INTO '+table+' '+values)
            query(t.a,"SELECT og_lake.flush('biz.orders'::regclass)")
        query(t.a,'INSERT INTO biz.orders VALUES(7,700),(7,700),(NULL,4),(1000,NULL)')
        query(t.a,'INSERT INTO biz.exact VALUES(7,700),(7,700),(NULL,4),(1000,NULL)')
        # Create real source deletion marks plus an updated inline after-image.
        for table in ['biz.orders','biz.exact']:
            query(t.a,'DELETE FROM '+table+' WHERE id=17');query(t.a,'UPDATE '+table+' SET val=val+1 WHERE id=23')
        expected=query(t.observer,heap_statement);population=len(expected)
        check('fixture contains four actual data files',scalar(t.observer,'SELECT count(*) FROM created_metadata.ducklake_data_file'),4)
        check('every source has multiple recorded row groups',scalar(t.observer,'SELECT min(row_group_count) FROM created_metadata.ducklake_data_file')>=2)
        for dop in [2,4]:
            state=capture('ordered-dop'+str(dop),statement,expected,dop,full_population=population)
            plan='\n'.join(x[0]for x in query(t.a,'EXPLAIN (ANALYZE,COSTS OFF) '+statement));(out/('dop'+str(dop)+'-plan.txt')).write_text(plan+'\n')
            check('real EXPLAIN Stream/'+str(dop),'Stream'in plan and 'Lake SMP Planned DOP: '+str(dop)in plan)
            filtered='SELECT id,val FROM biz.orders WHERE id>10 AND val IS NOT NULL ORDER BY id,val'
            capture('filter-dop'+str(dop),filtered,query(t.observer,filtered.replace('biz.orders','biz.exact')),dop,full_population=population)
        guc_statement="SELECT id,val,current_setting('enable_lake_smp'),current_setting('enable_lake_ann') FROM biz.orders ORDER BY id,val NULLS FIRST"
        query(t.a,'SET enable_lake_ann=off')
        # current_setting() is STABLE but not shippable in this single-node
        # host. Check its serial semantics separately; its presence must not
        # accidentally turn the worker-GUC test into a serial-only success.
        stats(-1)
        check('session-setting projection complete rows',query(t.a,guc_statement),[(i,v,'on','off')for i,v in expected])
        check('nonshippable session-setting projection stays serial',stats()['participants'],0)
        check('parent GUC image before real workers',query(t.a,'SHOW enable_lake_smp'),[('on',)])
        check('ANN off GUC before real workers',query(t.a,'SHOW enable_lake_ann'),[('off',)])
        # LakeStreamOpenWorker checks all three prebound lake GUCs before
        # opening each reader. Real DOP4 success with both ANN images,
        # followed by the normal task/reader/join assertions, proves that this
        # execution used matching worker images without a forbidden function.
        capture('worker-synchronized-lake-GUC',statement,expected,4,full_population=population)
        query(t.a,'SET enable_lake_ann=on')
        check('nondefault ANN on GUC before real workers',query(t.a,'SHOW enable_lake_ann'),[('on',)])
        capture('worker-synchronized-lake-GUC-ann-on',statement,expected,4,full_population=population)
        aggregate='SELECT val,count(*),sum(id) FROM biz.orders WHERE id%3=0 GROUP BY val ORDER BY val NULLS FIRST'
        capture('native-aggregate',aggregate,query(t.observer,aggregate.replace('biz.orders','biz.exact')),4,full_population=population)
        query(t.a,'PREPARE smp_parameter(integer) AS SELECT id,val FROM biz.orders WHERE id>$1 ORDER BY id,val NULLS FIRST')
        previous=None
        for value in [10,31]:
            state=capture('external-param-'+str(value),'EXECUTE smp_parameter('+str(value)+')',query(t.observer,'SELECT id,val FROM biz.exact WHERE id>%s ORDER BY id,val NULLS FIRST',(value,)),4,full_population=population)
            if previous is not None:check('re-execution uses a fresh Native execution nonce',state['query']!=previous)
            previous=state['query']
        query(t.a,'DEALLOCATE smp_parameter')
        # An independent controller waits for a file-only worker's first batch.
        # SQL rows are still read by the single real Stream query above.
        key=(1<<44)+int(hashlib.sha256(a.run_id.encode()).hexdigest()[:9],16)
        dbid=scalar(t.observer,'SELECT oid FROM pg_database WHERE datname=current_database()')
        def gated(label,cancel=False,early=False):
            nonlocal expected,population,preserve_owned_scene,exit_without_thread_join
            configure(t.a,4);stats(-1);before=log_offset();frozen=list(expected);frozen_head=t.head()[0];parent_pid=scalar(t.a,'SELECT pg_backend_pid()');holder_pid=scalar(t.b,'SELECT pg_backend_pid()')
            query(t.b,'SELECT pg_advisory_lock(%s::bigint)',(key,));pool=ThreadPoolExecutor(max_workers=1);future=None;locks=[]
            try:
                stats(1,1,key);future=pool.submit(query,t.a,'SELECT id,val FROM biz.orders LIMIT 1' if early else statement);deadline=time.monotonic()+30
                while time.monotonic()<deadline and not future.done():
                    locks=query(t.observer,"SELECT pid,granted FROM pg_locks WHERE locktype='advisory' AND database=%s AND classid=%s::oid AND objid=%s::oid AND objsubid=1 AND mode='ExclusiveLock' AND NOT granted",(dbid,key>>32,key&0xffffffff))
                    if locks:break
                    time.sleep(.01)
                check(label+' observed exact SDK-batch-held worker gate',len(locks),1);check(label+' gate is ungranted',locks[0][1],False)
                if cancel:
                    check(label+' cancel real top consumer',scalar(t.observer,'SELECT pg_cancel_backend(%s)',(parent_pid,)))
                    try:future.result(timeout=30)
                    except Exception as exc:check(label+' actual cancellation SQLSTATE',getattr(exc,'pgcode',None),'57014')
                    else:check(label+' cancelled query cannot succeed',False)
                elif early:
                    # No Sort/TopN and one source owner is pinned at its actual
                    # first SDK batch. Wait until the top consumer has finished
                    # its LIMIT and reached the native Stream join, then release
                    # only this diagnostic gate. This is a real wait-state
                    # handshake, never a sleep-based completion assumption.
                    deadline=time.monotonic()+30;joined=[]
                    while time.monotonic()<deadline and not future.done():
                        joined=query(t.observer,"SELECT tid,query_id,wait_status FROM pg_thread_wait_status WHERE tid=%s AND wait_status='synchronize quit'",(parent_pid,))
                        if joined:break
                        time.sleep(.01)
                    check(label+' top consumer completed LIMIT before held worker batch release',len(joined),1)
                    r[label+'-join-wait']=joined
                    check(label+' exact worker still holds first-batch wait',query(t.observer,
                        "SELECT pid,granted FROM pg_locks WHERE locktype='advisory' AND database=%s AND classid=%s::oid AND objid=%s::oid AND objsubid=1 AND mode='ExclusiveLock' AND pid=%s AND NOT granted",
                        (dbid,key>>32,key&0xffffffff,locks[0][0])),locks)
                    query(t.b,'SELECT pg_advisory_unlock(%s::bigint)',(key,))
                    got=future.result(timeout=30)
                    check(label+' unordered LIMIT returns one visible original row',len(got),1)
                    check(label+' unordered row belongs to exact committed heap image',all(Counter(got)[x]<=Counter(frozen)[x]for x in Counter(got)))
                    state=stats();r[label+'-actual-producers']=state
                    check(label+' actual producer degree',state['participants'],4)
                    check(label+' gated file owner opened actual reader',state['workers'][1]['reader_ready'],1)
                    check(label+' every claimed producer retired after native join',all(x['done']in(1,2)for x in state['workers']if x['claimed']))
                    materialized=sum(x['rows']for x in state['workers'])
                    r[label+'-materialized-population']=dict(actual=materialized,complete=len(frozen))
                    check(label+' source producers stopped before full visible population',0<materialized<len(frozen))
                    check(label+' gate file owner did not enumerate its full assigned file',state['workers'][1]['rows']<a.rows_per_file)
                    qid,scan=state['query'],state['scan']
                    text=read_log(before,label,qid,scan,None,4)
                    done=[tuple(map(int,m.groups()))for m in DONE.finditer(text)if int(m[1])==qid and int(m[2])==scan]
                    check(label+' one joined completion per participant',len(done),4)
                else:
                    configure(t.b)
                    query(t.b,'BEGIN')
                    for table in ['biz.orders','biz.exact']:query(t.b,'UPDATE '+table+' SET val=COALESCE(val,0)+1000 WHERE id=7')
                    query(t.b,'COMMIT');query(t.b,"SELECT og_lake.flush('biz.orders'::regclass)")
                    query(t.b,'SELECT pg_advisory_unlock(%s::bigint)',(key,))
                    check(label+' original complete S_stmt survives UPDATE and flush',future.result(timeout=60),frozen)
                    state=stats();check(label+' all workers kept original lake snapshot',state['snapshot'],frozen_head)
                    completed(label,state,before,4,len(frozen))
                    expected=query(t.observer,heap_statement);population=len(expected)
                    capture(label+'-next-statement',statement,expected,4,full_population=population)
            finally:
                # A failed concurrent UPDATE may have left the holder's
                # transaction aborted. Recover before releasing session lock.
                cleanup_error=None
                try:query(t.b,'ROLLBACK')
                except BaseException as exc:cleanup_error=str(exc)
                # Always attempt the exact controller-key unlock even if its
                # ROLLBACK failed. This precedes cancellation/drain/join.
                try:query(t.b,'SELECT pg_advisory_unlock(%s::bigint)',(key,))
                except BaseException as exc:cleanup_error=cleanup_error or str(exc)
                try:
                    held_after=query(t.observer,"SELECT pid FROM pg_locks WHERE locktype='advisory' AND database=%s AND classid=%s::oid AND objid=%s::oid AND objsubid=1 AND pid=%s AND mode='ExclusiveLock' AND granted",(dbid,key>>32,key&0xffffffff,holder_pid))
                    r.setdefault('gate_cleanup_evidence',[]).append(dict(label=label,holder_pid=holder_pid,key=key,remaining=held_after))
                    if held_after:cleanup_error=cleanup_error or 'exact controller advisory gate remains held before query drain'
                except BaseException as exc:cleanup_error=cleanup_error or str(exc)
                if future is not None and not future.done():
                    try:t.a.cancel()
                    except BaseException as exc:cleanup_error=cleanup_error or str(exc)
                if future is not None:
                    try:future.result(timeout=30)
                    except BaseException:pass
                pending=future is not None and not future.done()
                if pending:
                    preserve_owned_scene=True;exit_without_thread_join=True
                    r['preserved_worker']=dict(label=label,top_consumer_pid=parent_pid,observed_worker_locks=locks,
                        reason='worker did not drain within30s after gate release/cancel')
                    pool.shutdown(wait=False,cancel_futures=True)
                    raise RuntimeError('SMP fault worker did not drain; preserving owned database/roles and exiting isolated child')
                pool.shutdown(wait=True)
                if cleanup_error is not None:
                    preserve_owned_scene=True
                    raise RuntimeError('SMP gate cleanup was not proven: '+cleanup_error)
            if cancel:
                state=stats();check(label+' real task reader reached live batch',state['workers'][1]['reader_ready'],1)
                check(label+' every started task retired before registry release',all(x['done']in(1,2)for x in state['workers']if x['claimed']))
                capture(label+'-same-backend-reuse',statement,expected,4,full_population=population)
        gated('fixed-binding-concurrent-update-flush')
        gated('live-SDK-batch-cancel',cancel=True)
        for mode,label in [(2,'worker-error'),(3,'pre-start-error'),(4,'partial-real-start-error')]:
            stats(-1);stats(mode,1,0);error(t.a,statement,'XX000');state=stats();r[label]=state
            if mode==3:check('pre-start failure starts zero participants',sum(x['started']for x in state['workers']),0)
            if mode==4:check('partial-start failure really starts exactly one source participant',sum(x['started']for x in state['workers']),1)
            check(label+' each claimed task retired',all(x['done']in(1,2)for x in state['workers']if x['claimed']))
            capture(label+'-same-backend-reuse',statement,expected,4,full_population=population)
        # Parameterized/rescanned paths are ordinary native serial executions.
        configure(t.a,4);stats(-1)
        rescan='SELECT k.x,s.id,s.val FROM (VALUES(7),(23)) AS k(x) CROSS JOIN LATERAL (SELECT id,val FROM biz.orders WHERE id=k.x OFFSET 0) AS s ORDER BY 1,2,3 NULLS FIRST'
        check('parameterized native rescan complete rows',query(t.a,rescan),query(t.observer,rescan.replace('biz.orders','biz.exact')));check('parameterized rescan stayed serial',stats()['participants'],0)
        # Own journal admission is decided during planning and rechecked before
        # any startup for a no-parameter cached SMP plan.
        configure(t.a,4);query(t.a,'PREPARE smp_cached AS '+statement);query(t.a,'EXECUTE smp_cached');stats(-1)
        query(t.a,'BEGIN');query(t.a,'INSERT INTO biz.orders VALUES(90001,1)');error(t.a,'EXECUTE smp_cached','0A000');query(t.a,'ROLLBACK')
        check('cached own-write failure starts no source worker',stats()['participants'],0);query(t.a,'DEALLOCATE smp_cached')
        query(t.a,'BEGIN');query(t.a,'INSERT INTO biz.orders VALUES(90002,2)');stats(-1)
        own=query(t.a,statement);check('new own-write plan uses serial journal exactly once',Counter(own)==Counter(expected+[(90002,2)]));check('own-write fresh plan stayed serial',stats()['participants'],0);query(t.a,'ROLLBACK')
        # Ordered LIMIT retains its SQL/result and joined-resource checks.
        # Global Sort/TopN consumes its input; genuine source early-stop has a
        # separate unordered gated test below.
        configure(t.a,4)
        for i in range(3):
            stats(-1);check('LIMIT completes correct row/'+str(i),query(t.a,statement+' LIMIT 1'),expected[:1]);state=stats()
            check('LIMIT actually exercised Stream/'+str(i),state['participants'],4)
            check('LIMIT releases every claimed participant/'+str(i),all(x['done']in(1,2)for x in state['workers']if x['claimed']))
        # The ordered LIMIT cases above preserve their original SQL oracle,
        # but their global Sort/TopN must consume input. This separate query
        # proves genuine source early-stop and EXPLAIN-only startup behavior.
        configure(t.a,4);stats(-1)
        early_plan='\n'.join(x[0]for x in query(t.a,'EXPLAIN (COSTS OFF) SELECT id,val FROM biz.orders LIMIT 1'))
        (out/'unordered-limit-plan.txt').write_text(early_plan+'\n')
        check('unordered LIMIT planned real Stream','Stream'in early_plan and 'Lake SMP Planned DOP: 4'in early_plan)
        check('unordered LIMIT has no global Sort/TopN blocker',re.search(r'\b(Sort|TopN)\b',early_plan)is None)
        check('EXPLAIN-only starts no lake producer or read registry',stats()['participants'],0)
        gated('unordered-limit-real-source-early-stop',early=True)
        capture('unordered-limit-same-backend-reuse',statement,expected,4,full_population=population)
        # Zero-visible work must still retain genuine physical file/inline
        # sources. Use a separate bounded fixture: deleting the large skewed
        # scan population is a DML scale test, not a prerequisite for this
        # lifecycle check (the original 40k-row timeout remains recorded).
        configure(t.b);t.create(name='empty_inputs',conn=t.b)
        for first in [1,3,5,7]:
            query(t.b,'INSERT INTO biz.empty_inputs VALUES(%s,%s),(%s,%s)',(first,first*10,first+1,(first+1)*10))
            query(t.b,"SELECT og_lake.flush('biz.empty_inputs'::regclass)")
        query(t.b,'INSERT INTO biz.empty_inputs VALUES(9,90)')
        empty_statement='SELECT id,val FROM biz.empty_inputs ORDER BY id,val NULLS FIRST'
        empty_expected=[(i,i*10)for i in range(1,10)]
        empty_files=query(t.observer,"SELECT f.path,f.record_count FROM created_metadata.ducklake_data_file f JOIN created_metadata.ducklake_table t ON t.table_id=f.table_id WHERE t.table_name='empty_inputs' AND t.end_snapshot IS NULL AND f.end_snapshot IS NULL ORDER BY f.data_file_id")
        check('zero-visible fixture has four real immutable sources',len(empty_files),4)
        check('zero-visible fixture has eight physical file rows',sum(x[1]for x in empty_files),8)
        empty_root=Path(t.empty_manifest['data_path']).resolve();empty_archives=[]
        for raw_path,physical_rows in empty_files:
            matches=[x for x in empty_root.rglob('*.parquet')if x.name==Path(raw_path).name]
            check('zero-visible fixture resolves exactly one owned source',len(matches),1)
            path=matches[0];check('zero-visible source stays inside fixture root',path.resolve().is_relative_to(empty_root))
            destination=out/'fixture-input-files'/path.relative_to(empty_root)
            destination.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(path,destination)
            check('zero-visible source archive matches bytes',sha256(destination),sha256(path))
            empty_archives.append(dict(path=str(path),sha256=sha256(path),record_count=physical_rows,archive=str(destination.relative_to(out))))
        write_json(out/'zero-visible-fixture.json',dict(files=empty_archives,visible_before=empty_expected))
        capture('zero-visible-fixture-before-delete',empty_statement,empty_expected,4,full_population=9)
        check('zero-visible fixture real DELETE returns all nine rows',sorted(query(t.b,'DELETE FROM biz.empty_inputs RETURNING id,val')),empty_expected)
        check('zero-visible fixture keeps physical file sources',query(t.observer,"SELECT f.path,f.record_count FROM created_metadata.ducklake_data_file f JOIN created_metadata.ducklake_table t ON t.table_id=f.table_id WHERE t.table_name='empty_inputs' AND t.end_snapshot IS NULL AND f.end_snapshot IS NULL ORDER BY f.data_file_id"),empty_files)
        capture('all-input-rows-deleted',empty_statement,[],4,full_population=0)
        check('all recorded executed inputs unchanged',all(sha256(f)==h for f,h in r['inputs'].items()));check_build(a.build_receipt)
        r['status']='pass'
    except BaseException as exc:r.update(reason=str(exc),traceback=traceback.format_exc())
    finally:
        if preserve_owned_scene:
            r['preserve_owned_scene']=True
            r['preserved_database']=dict(database=db.database if db else None,
                allocation=getattr(db,'allocation',None),role_allocation=str(roles.path)if roles else None)
        if db is not None and not preserve_owned_scene:
            try:db.cleanup();r['database_cleanup']=db.cleanup_result
            except BaseException as exc:r.update(status='fail',cleanup_error=str(exc))
        if roles is not None and preserve_owned_scene:r=roles.redact(r)
        if roles is not None and not preserve_owned_scene:
            try:
                if not r.get('database_cleanup')or r['database_cleanup']['returncode']:raise RuntimeError('Owned database cleanup not proven; roles retained')
                r['role_cleanup']=cleanup_roles(control,roles.path,a.run_id);r=roles.redact(r)
            except BaseException as exc:r.update(status='fail',role_cleanup_error=str(exc))
        if control is not None:control.close()
        write_json(out/'result.json',r)
    print(json.dumps(dict(status=r['status'],checks=len(r['assertions']),reason=r.get('reason'),result=str(out/'result.json')),ensure_ascii=False))
    if exit_without_thread_join:
        # ThreadPoolExecutor registers an interpreter-exit join even after
        # shutdown(wait=False). This is only our isolated test child: persist
        # evidence first, then close its OS process without that unbounded join.
        sys.stdout.flush();sys.stderr.flush();os._exit(1)
    return 0 if r['status']=='pass'else 1


if __name__=='__main__':raise SystemExit(main())
