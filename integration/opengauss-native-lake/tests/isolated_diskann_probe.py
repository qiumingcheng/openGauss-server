#!/usr/bin/env python3
"""Bounded existing-DiskANN probe in a separately owned disposable test cluster.

No lake ANN feature is implemented here. Binary bytes are copied, data and
ownership evidence are retained, and an unresponsive probe cannot stop the main
lake test instance. Immediate shutdown is a failed-probe cleanup, not acceptance.
"""
import argparse
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

ROOT=Path('/home/qiumc/data/code/cpp');D=ROOT/'integration/opengauss-native-lake'
sys.path[:0]=[str(D),str(D/'tests')]
from build_driver import bootstrap,port_check
from file_dml_acceptance import check_build
from lake_harness import sha256,validate_run_id,write_json
from inline_fixture import query,scalar


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--run-id',required=True)
    p.add_argument('--build-receipt',type=Path,required=True)
    p.add_argument('--storage',choices=['typed','text-array'],default='typed')
    p.add_argument('--rows',type=int,default=64);p.add_argument('--seconds',type=int,default=15)
    p.add_argument('--lifecycle',action='store_true',help='Check native MVCC, index DML, rollback and prepared rescans')
    p.add_argument('--pq',action='store_true',help='Require native DiskPQ; missing library is a failed capability probe')
    p.add_argument('--serial-build',action='store_true',help='Verify the absent parallel_workers option; native builder defaults to zero')
    p.add_argument('--zero-only',action='store_true',help='Cosine build must exclude zero-norm inputs without invalid graph nodes')
    p.add_argument('--metric',choices=['l2','ip','cosine'],default='l2')
    p.add_argument('--expect-cancel',action='store_true',help='Require SQL timeout during build, rollback and same-connection reuse')
    p.add_argument('--port',type=int,default=55660);a=p.parse_args();validate_run_id(a.run_id)
    if not 16<=a.rows<=2048 or not 5<=a.seconds<=60 or not 55600<=a.port<=55900:p.error('bounded test arguments required')
    if a.zero_only and(a.metric!='cosine' or a.lifecycle or a.expect_cancel):p.error('zero-only is a separate cosine case')
    if a.expect_cancel and(a.lifecycle or a.rows<512 or a.seconds<15):p.error('cancellation needs at least512 rows,15s and its own case')
    out=ROOT/'公共文档/openGauss原生湖存储/验证'/a.run_id;out.mkdir(exist_ok=False)
    # Keep the native Unix socket comfortably within sockaddr_un's path limit.
    runtime=ROOT/'.runtime/opengauss-native-lake'/('probe-'+hashlib.sha256(a.run_id.encode()).hexdigest()[:12])
    prefix=ROOT/'.install'/('og-lake-probe-'+a.run_id);data=runtime/'data';sock=runtime/'socket'
    r={'status':'failed','scope':__doc__,'started_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),
       'storage':a.storage,'rows':a.rows,'wall_budget_seconds':a.seconds,'runtime':str(runtime),'install':str(prefix),'port':a.port,
       'metric':a.metric,'pq':a.pq,'lifecycle':a.lifecycle,'serial_build':a.serial_build,'expect_cancel':a.expect_cancel,'zero_only':a.zero_only,'input_rows':a.rows}
    files=[Path(__file__),D/'build_driver.py',D/'native_runtime.py',D/'tests/file_dml_acceptance.py',D/'inline_fixture.py',D/'source-lock.json']
    r['inputs']={str(f):sha256(f)for f in files};archive=out/'executed-inputs';archive.mkdir()
    for f in files:shutil.copy2(f,archive/f.name)
    env=dict(os.environ);worker=None;state={};started=False
    def command(label,args,timeout=45):
        with(out/(label+'.log')).open('w')as log:
            try:code=subprocess.run(list(map(str,args)),env=env,stdout=log,stderr=subprocess.STDOUT,timeout=timeout).returncode
            except subprocess.TimeoutExpired:code=124
        r.setdefault('commands',[]).append(dict(label=label,args=list(map(str,args)),returncode=code))
        return code
    try:
        b=check_build(a.build_receipt);main=Path(b['install']);r.update(source_head=b['source_head'],binary_sha256=b['binary_sha256'])
        port_check(a.port);runtime.mkdir(exist_ok=False);prefix.mkdir(exist_ok=False);(prefix/'bin').mkdir()
        for name in ['gaussdb','gs_ctl','gs_initdb']:
            shutil.copy2(main/'bin'/name,prefix/'bin'/name)
            if sha256(main/'bin'/name)!=sha256(prefix/'bin'/name):raise RuntimeError('Probe binary copy mismatch')
        # Shared read-only dependency paths are recorded; no build or SDK swap
        # may run concurrently with this short probe. The main executable itself
        # is never used as the probe's mutable installation target.
        for name in ['lib','share']:(prefix/name).symlink_to(main/name,target_is_directory=True)
        identity=dict(project='opengauss-native-lake-isolated-probe',run_id=a.run_id,data=str(data),install=str(prefix),
            binary_sha256=b['binary_sha256'],main_install=str(main),port=a.port)
        write_json(runtime/'owner.json',identity);bootstrap(data,runtime,out,prefix=prefix)
        env.update(GAUSSHOME=str(prefix),LD_LIBRARY_PATH=str(main/'lib')+':'+str(ROOT/'binarylibs/buildtools/gcc10.3/gcc/lib64'))
        with(data/'postgresql.conf').open('a')as conf:conf.write('\nport='+str(a.port)+'\nenable_lake_maintenance=off\n')
        started=True
        if command('start',[prefix/'bin/gs_ctl','start','-w','-t','30','-D',data,'-Z','single_node','-l',runtime/'server.log','-o','-p '+str(a.port)]):
            raise RuntimeError('Isolated native startup failed')
        pid=int((data/'postmaster.pid').read_text().splitlines()[0]);r['pid']=pid
        if Path('/proc/'+str(pid)+'/exe').resolve()!=prefix/'bin/gaussdb':raise RuntimeError('Probe process belongs to another binary')
        if sha256('/proc/'+str(pid)+'/exe')!=b['binary_sha256']:raise RuntimeError('Probe running binary mismatch')
        expression='v'if a.storage=='typed'else"(pg_catalog.string_to_array(pg_catalog.btrim(v,'[]'),',')::real[])::vector(3)"
        value="('['||i::text||','||(i%13)::text||','||(i%17)::text||']')"+('::vector(3)'if a.storage=='typed'else'')
        if a.zero_only:value="'[0,0,0]'"+('::vector(3)'if a.storage=='typed'else'')
        operator,opclass={'l2':('<->','vector_l2_ops'),'ip':('<#>','vector_ip_ops'),
                          'cosine':('<=>','vector_cosine_ops')}[a.metric]
        distance=expression+' '+operator+" '[5,3,7]'::vector"
        ordered='SELECT id,'+distance+' AS distance FROM physical_vectors'+(' WHERE vector_norm('+expression+')>0'if a.zero_only else'')+' ORDER BY '+distance+' LIMIT 10'
        statements=['CREATE TABLE physical_vectors(id integer,v '+('vector(3)'if a.storage=='typed'else'text')+')',
            'INSERT INTO physical_vectors SELECT i,'+value+' FROM generate_series(1,'+str(a.rows)+')i',
            'CREATE INDEX reused_diskann ON physical_vectors USING diskann (('+expression+') '+opclass+') WITH(enable_pq='+
                ('on,pq_m=1'if a.pq else'off')+')',ordered]
        (out/'probe.sql').write_text(';\n'.join(statements)+';\n')
        def sql_worker():
            c=None;query_sql=ordered
            try:
                import psycopg2
                c=psycopg2.connect(host=str(sock),port=a.port,dbname='postgres',user=os.getenv('USER','qiumc'),connect_timeout=5,
                    application_name='owned_diskann_probe_'+a.run_id);c.autocommit=True;state['connection']=c
                query(c,'SET statement_timeout=5000');state['backend_pid']=scalar(c,'SELECT pg_backend_pid()')
                checks=[];state['assertions']=checks
                def check(name,passed,actual=None):
                    checks.append(dict(name=name,passed=bool(passed),actual=actual))
                    if not passed:raise AssertionError(name+': '+repr(actual))
                for n,statement in enumerate(statements):
                    if n==len(statements)-1:query(c,'SET enable_seqscan=off')
                    state['phase']=n
                    began=time.monotonic()
                    try:state['rows']=query(c,statement)
                    except psycopg2.Error as exc:
                        if not(a.expect_cancel and n==2 and exc.pgcode=='57014'):raise
                        state.update(build_cancelled=True,cancel_sqlstate=exc.pgcode,
                                     cancel_elapsed_seconds=time.monotonic()-began)
                        check('timeout_returns_before_wall_budget',state['cancel_elapsed_seconds']<a.seconds-2,
                              state['cancel_elapsed_seconds'])
                        check('cancel_keeps_committed_rows',scalar(c,'SELECT count(*) FROM physical_vectors')==a.rows)
                        check('cancel_rolls_back_index',scalar(c,"SELECT count(*) FROM pg_class WHERE relname='reused_diskann'")==0)
                        # Preserve the original committed population. Use a
                        # separate small relation to prove that cancellation
                        # released the builder's locks and backend resources.
                        query(c,'SET statement_timeout=5000')
                        query(c,'CREATE TABLE small_vectors AS SELECT * FROM physical_vectors WHERE id<=64')
                        query(c,statements[2].replace('ON physical_vectors','ON small_vectors'))
                        check('same_connection_build_after_cancel',scalar(c,
                              "SELECT count(*) FROM pg_index WHERE indexrelid='reused_diskann'::regclass AND indisvalid")==1)
                        query_sql=statements[-1].replace('FROM physical_vectors','FROM small_vectors')
                        break
                    if n==0 and a.serial_build:
                        # This branch accepts only explicit values 1..128.
                        # PlanCreateIndexWorkers instead passes default 0 to
                        # RelationGetParallelWorkers when the option is absent.
                        setting=scalar(c,"SELECT reloptions::text FROM pg_class WHERE oid='physical_vectors'::regclass")
                        check('native_serial_default',setting is None or'parallel_workers='not in setting,setting)
                if a.expect_cancel:
                    check('large_build_was_actually_cancelled',state.get('build_cancelled',False))
                    query(c,'SET enable_seqscan=off');state['rows']=query(c,query_sql)
                state['plan']='\n'.join(x[0]for x in query(c,'EXPLAIN (ANALYZE,COSTS OFF) '+query_sql))
                check('actual_native_ann_plan','Ann Index Scan using reused_diskann'in state['plan'],state['plan'])
                check('zero_norm_query_returns_no_rows'if a.zero_only else'ten_ordered_candidates',len(state['rows'])==(0 if a.zero_only else 10) and
                      [x[1]for x in state['rows']]==sorted(x[1]for x in state['rows']),state['rows'])
                if a.zero_only:
                    check('zero_norm_table_population',scalar(c,'SELECT count(*) FROM physical_vectors')==a.rows)
                    check('zero_norm_index_population',scalar(c,"SELECT reltuples FROM pg_class WHERE oid='reused_diskann'::regclass")==0)
                    state['activity']='insert_null_into_empty_cosine_index'
                    query(c,'INSERT INTO physical_vectors VALUES(90000,NULL)')
                    check('null_insert_skips_graph',query(c,ordered)==[])
                    state['activity']='insert_zero_into_empty_cosine_index'
                    query(c,"INSERT INTO physical_vectors VALUES(90001,'[0,0,0]'"+('::vector(3)'if a.storage=='typed'else'')+')')
                    check('zero_insert_skips_graph',query(c,ordered)==[])
                    state['activity']='insert_first_nonzero_into_empty_cosine_index'
                    query(c,"INSERT INTO physical_vectors VALUES(90002,'[5,3,7]'"+('::vector(3)'if a.storage=='typed'else'')+')')
                    first=query(c,ordered)
                    check('first_nonzero_insert_seeds_graph',len(first)==1 and first[0][0]==90002 and abs(first[0][1])<1e-6,first)
                    query(c,'DELETE FROM physical_vectors WHERE id=90002')
                    check('deleted_seed_is_not_a_result',query(c,ordered)==[])
                    check('unindexed_null_and_zero_rows_preserved',scalar(c,'SELECT count(*) FROM physical_vectors')==a.rows+2)
                if a.lifecycle:
                    exact="SELECT id,"+distance+" AS distance FROM physical_vectors ORDER BY ("+distance+")+0 LIMIT 10"
                    # Adding zero defeats index ordering, retaining the same
                    # SQL distance. Verify the actual reference plan as well.
                    exactplan='\n'.join(x[0]for x in query(c,'EXPLAIN (COSTS OFF) '+exact))
                    check('reference_is_exact_scan','Ann Index Scan'not in exactplan and'Sort'in exactplan,exactplan)
                    baseline=query(c,exact)
                    check('initial_topk_distances',[x[1]for x in state['rows']]==[x[1]for x in baseline])
                    best='[1000000,1000000,1000000]'if a.metric=='ip'else'[5,3,7]'
                    query(c,"UPDATE physical_vectors SET v='"+best+"'"+('::vector(3)'if a.storage=='typed'else'')+' WHERE id=1')
                    changed=query(c,ordered)
                    check('index_update_new_version',changed[0][0]==1 and abs(changed[0][1]-query(c,
                          'SELECT '+distance+' FROM physical_vectors WHERE id=1')[0][0])<1e-12,changed)
                    query(c,'DELETE FROM physical_vectors WHERE id=1')
                    deleted=query(c,ordered)
                    check('deleted_tuple_not_returned',all(x[0]!=1 for x in deleted)and len(deleted)==10,deleted)
                    query(c,'BEGIN');query(c,"INSERT INTO physical_vectors VALUES(90001,'"+best+"'"+
                          ('::vector(3)'if a.storage=='typed'else'')+')')
                    own=query(c,ordered)
                    check('own_insert_visible',any(x[0]==90001 for x in own),own)
                    query(c,'ROLLBACK')
                    rolled=query(c,ordered)
                    check('rolled_back_insert_absent',all(x[0]!=90001 for x in rolled),rolled)
                    # Reuse one prepared plan with two distinct query values.
                    query(c,'PREPARE reprobe(vector) AS SELECT id,'+expression+' '+operator+
                          ' $1 AS distance FROM physical_vectors ORDER BY '+expression+' '+operator+' $1 LIMIT 10')
                    one=query(c,"EXECUTE reprobe('[5,3,7]')")
                    two=query(c,"EXECUTE reprobe('[19,6,2]')")
                    again=query(c,"EXECUTE reprobe('[5,3,7]')")
                    check('prepared_parameter_changes',one!=two)
                    check('prepared_reuse_restores_result',one==again)
                    query(c,'DEALLOCATE reprobe')
                    query(c,'VACUUM physical_vectors')
                    after=query(c,ordered)
                    check('vacuum_keeps_visible_population',all(x[0]not in(1,90001)for x in after)and len(after)==10,after)
                state['completed']=True
            except BaseException as exc:state.update(error=str(exc),sqlstate=getattr(exc,'pgcode',None),traceback=traceback.format_exc())
            finally:
                if c is not None:c.close()
        worker=threading.Thread(target=sql_worker,daemon=True);worker.start();worker.join(a.seconds)
        if worker.is_alive():
            r['timed_out']=True
            if state.get('connection')is not None:
                state['connection'].cancel();worker.join(2)
        else:r['timed_out']=False
        r.update({k:v for k,v in state.items()if k!='connection'})
        r['main_binary_unchanged']=sha256(main/'bin/gaussdb')==b['binary_sha256']
        if(r.get('completed')and not r['timed_out']and len(r.get('rows',[]))==(0 if a.zero_only else 10) and
            'reused_diskann'in r.get('plan','')and'Index Scan'in r.get('plan','')and
            [x[1]for x in r['rows']]==sorted(x[1]for x in r['rows'])and
            r['main_binary_unchanged']and all(x['passed']for x in r.get('assertions',[]))and
            all(sha256(f)==h for f,h in r['inputs'].items())):r['status']='passed'
    except BaseException as exc:r.update(reason=str(exc),traceback=traceback.format_exc())
    finally:
        if started:
            # The owner, PID, binary and path are rechecked before each stop.
            try:
                owner=json.loads((runtime/'owner.json').read_text());pidfile=data/'postmaster.pid'
                assert owner['project']=='opengauss-native-lake-isolated-probe'and owner['run_id']==a.run_id
                if pidfile.exists():
                    current=int(pidfile.read_text().splitlines()[0])
                    if'pid'in r:assert current==r['pid']
                    else:r['pid']=current
                    if Path('/proc/'+str(current)).exists():
                        assert Path('/proc/'+str(current)+'/exe').resolve()==prefix/'bin/gaussdb'
                        code=command('stop-fast',[prefix/'bin/gs_ctl','stop','-D',data,'-m','fast','-w','-t','5'],10)
                        r['shutdown_mode']='fast'
                        if code:
                            r['status']='failed';r['shutdown_mode']='immediate'
                            command('stop-immediate',[prefix/'bin/gs_ctl','stop','-D',data,'-m','immediate','-w','-t','5'],10)
                r['process_exited']='pid'not in r or not Path('/proc/'+str(r['pid'])).exists()
                if not r['process_exited']:r['status']='failed'
            except BaseException as exc:r.update(status='failed',shutdown_error=str(exc))
        if not started:
            r.update(process_exited=True,shutdown_mode='not_started')
        if worker is not None:worker.join(2)
        r['finished_at']=datetime.datetime.now(datetime.timezone.utc).isoformat();write_json(out/'result.json',r)
    print(json.dumps(dict(status=r['status'],phase=r.get('phase'),timed_out=r.get('timed_out'),reason=r.get('reason',r.get('error')),result=str(out/'result.json')),ensure_ascii=False))
    return 0 if r['status']=='passed'else 1

if __name__=='__main__':raise SystemExit(main())
