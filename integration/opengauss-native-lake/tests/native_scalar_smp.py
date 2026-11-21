#!/usr/bin/env python3
"""Real Native scalar-index Stream qualification candidate; not executed in stage.

Requires a matching official Debug source/binary receipt and an idle serialized
reference fixture. Numeric sorted/bitmap file candidates are made independently
by actual native Stream participants, inline remains a complete TableAM task.
Compare complete rows with ordinary heap SQL; no ANN, vector-batch, remote/RAC,
parameterized SMP rescans, performance or production conclusion is implied.
"""
from copy import deepcopy
import argparse,ast,stat
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
from native_runtime import PROFILE,RUNTIME,SOURCE,INSTALL
from native_runtime_log import NativeLogSource
from native_table_create import Case
from native_test_context import OwnedDatabase, reviewed_runtime_version
from permission_api import OwnedRoles,cleanup_roles
TASK=re.compile(r'lake SMP task query=(\d+) scan=(\d+) snapshot=(\d+) participant=(\d+)/(\d+) task=(\d+)(?: file=(\d+))? inline=([01])')
INDEX=re.compile(r'lake SMP index query=(\d+) scan=(\d+) snapshot=(\d+) participant=(\d+)/(\d+) verified=(\d+) bitmap=(\d+) fallback=(\d+)')
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
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-id',required=True);parser.add_argument('--build-receipt',type=Path,required=True)
    parser.add_argument('--method',choices=['btree','bitmap'],required=True)
    parser.add_argument('--type',choices=['integer','bigint','real','double'],default='integer')
    parser.add_argument('--storage',choices=['heap','ustore'],default='heap')
    parser.add_argument('--mode',choices=['predicates','visibility','fallback','cancel'],required=True)
    parser.add_argument('--sql-version',choices=['0.18','0.19','0.20','0.21'],default='0.19')
    parser.add_argument('--case-timeout',type=int,default=900)
    parser.add_argument('--_isolated-child',action='store_true',help=argparse.SUPPRESS)
    a=parser.parse_args();validate_run_id(a.run_id)
    if PROFILE!='debug':parser.error('This candidate admits only the owned Debug runtime')
    if not 300<=a.case_timeout<=1800:parser.error('--case-timeout must be300..1800')
    if not a._isolated_child:return supervised_case(a)
    if os.environ.get('OG_LAKE_SMP_ISOLATED_CHILD_PARENT')!=str(os.getppid()):parser.error('Unsupervised child')
    out=ROOT/'公共文档/openGauss原生湖存储/验证'/a.run_id;out.mkdir(exist_ok=False)
    r=dict(status='fail',scope=__doc__,mode=a.mode,method=a.method,type=a.type,storage=a.storage,sql_version=a.sql_version,
           assertions=[],native_runtime_executed=False)
    control=db=roles=t=log_source=None;preserve=False;exit_without_join=False
    inputs=[Path(__file__),a.build_receipt,D/'tests/file_dml_acceptance.py',D/'native_test_context.py',
        D/'native_runtime.py',D/'inline_fixture.py',D/'lake_harness.py',D/'source-lock.json',
        D/'tests/native_table_create.py',D/'tests/permission_api.py',D/'tests/inline_cleanup.py',
        D/'tests/native_auto_flush.py',D/'tests/native_runtime_log.py',D/'tests/catalog_fixture_bundle.py',
        D/'tests/empty_reference_fixture.py',D/'tests/empty_manifest.py',D/'tests/empty_catalog_oracle.py',
        D/'tests/create_reference_oracle.py',D/'reference.sh',D/'tests/reverse_reference.py',D/'fixture.py']
    # Archive transitive local Python imports too; the fixture generator runs
    # in a separate child but belongs to this same frozen acceptance input.
    pending=list(inputs);seen=set(inputs)
    while pending:
        current=pending.pop()
        if current.suffix!='.py':continue
        for node in ast.walk(ast.parse(current.read_text())):
            names=[x.name.split('.')[0]for x in node.names]if isinstance(node,ast.Import)else [node.module.split('.')[0]]if isinstance(node,ast.ImportFrom)and node.module else []
            for name in names:
                for parent in [D,D/'tests']:
                    candidate=parent/(name+'.py')
                    if candidate.is_file()and candidate not in seen:seen.add(candidate);pending.append(candidate)
    provider=INSTALL/'lib/postgresql/og_lake_sdk/libog_lake_sdk_arrow.so'
    inputs=sorted(seen,key=str)+[provider]
    r['inputs']={str(p):sha256(p)for p in inputs};archive=out/'executed-inputs';archive.mkdir()
    for i,p in enumerate(inputs):shutil.copy2(p,archive/(str(i)+'-'+p.name))
    def image(x):
        if isinstance(x,list)and len(x)>30:return dict(rows=len(x),sha256=hashlib.sha256(repr(x).encode()).hexdigest())
        return x
    def check(label,got,want=True):
        good=got==want;r['assertions'].append(dict(name=label,observed=deepcopy(image(got)),expected=deepcopy(image(want)),status='pass'if good else'fail'))
        write_json(out/'result.json',r)
        if not good:raise AssertionError(label+': '+repr(image(got)))
    def configure(c,dop=1):
        for sql in ['SET session_timeout=0','SET statement_timeout=180000','SET enable_vector_engine=off',
                    'SET enable_lake_smp=on','SET enable_indexscan=on','SET enable_seqscan=off',
                    'SET query_dop='+str(1000+dop if dop>1 else 1)]:query(c,sql)
    def stats(mode=0,participant=0,gate=0):return json.loads(scalar(t.a,'SELECT biz.smp_probe(%s,%s,%s)',(mode,participant,gate)))
    def both(sql):
        query(t.b,'BEGIN')
        try:
            for table in ['biz.orders','biz.exact']:query(t.b,sql.format(table=table))
            query(t.b,'COMMIT')
        except BaseException:query(t.b,'ROLLBACK');raise
    def select(predicate):return 'SELECT id,val FROM {table} WHERE '+predicate+' ORDER BY id NULLS FIRST,val'
    def completed(label,state,mark,dop,covered,fallback):
        check(label+' actual Stream producer DOP',state['participants'],dop)
        for field in ['started','claimed','done','reader_ready']:check(label+' all producers '+field,[w[field]for w in state['workers']],[1]*dop)
        check(label+' actual verified scalar source count',sum(w['scalar_files']for w in state['workers']),covered)
        check(label+' actual complete fallback source count',sum(w['fallback_files']for w in state['workers']),fallback)
        check(label+' actual bitmap backend count',sum(w['bitmap_files']for w in state['workers']),covered if a.method=='bitmap'else 0)
        offset=int(state['inline_task']);expected=[]
        if offset:expected.append((state['query'],state['scan'],state['snapshot'],0,dop,0,-1,1))
        for f,fid in enumerate(state['files']):expected.append((state['query'],state['scan'],state['snapshot'],(f+offset)%dop,dop,f+offset,fid,0))
        def ready(text):return len([m for m in INDEX.finditer(text)if int(m[1])==state['query']and int(m[2])==state['scan']])>=dop
        try:text=mark.wait(ready,5)
        finally:write_json(out/(label+'-log-source.json'),mark.receipt())
        (out/(label+'-server.log')).write_text(text)
        tasks=[tuple(map(int,m.groups(default='-1')))for m in TASK.finditer(text)if int(m[1])==state['query']and int(m[2])==state['scan']]
        check(label+' exact one owner for every real file and inline task',sorted(tasks),sorted(expected))
        coverage=[tuple(map(int,m.groups()))for m in INDEX.finditer(text)if int(m[1])==state['query']and int(m[2])==state['scan']]
        wanted_coverage=[(state['query'],state['scan'],state['snapshot'],w['participant'],dop,w['scalar_files'],w['bitmap_files'],w['fallback_files'])for w in state['workers']]
        check(label+' exact real index report per participant',sorted(coverage),sorted(wanted_coverage))
        for w in state['workers']:
            p=w['participant'];owned=sum((i+offset)%dop==p for i in range(len(state['files'])))
            check(label+' participant source partition/'+str(p),w['scalar_files']+w['fallback_files'],owned)
    def capture(label,predicate,dop=4,covered=4,fallback=0,args=None,may_prune=False):
        configure(t.a,dop);stats(-1);mark=log_source.mark();statement=select(predicate)
        check(label+' exact ordinary heap SQL rows and multiplicities',query(t.a,statement.format(table='biz.orders'),args),query(t.observer,statement.format(table='biz.exact'),args))
        state=stats()
        if state['participants']==0 and may_prune:
            text='\n'.join(x[0]for x in query(t.a,'EXPLAIN (COSTS OFF) '+statement.format(table='biz.orders'),args))
            (out/(label+'-pruned-plan.txt')).write_text(text+'\n')
            check(label+' empty constant was pruned by ordinary optimizer',('Foreign Scan'not in text)and('Result'in text))
            r.setdefault('ordinary_constant_pruned_cases',[]).append(label)
        else:completed(label,state,mark,dop,covered,fallback)
        return state
    try:
        b=check_build(a.build_receipt);r.update(source_head=b['source_head'],binary_sha256=b['binary_sha256'],build_receipt=str(a.build_receipt.resolve()))
        owned=['lake_stream.cpp','lake_stream.h','lake_file_scalar.cpp','lake_file_scalar.h','lake_file_reader.cpp',
               'lake_file_reader.h','lake_inline_reader.cpp','lake_inline_reader.h','lake_inline_index.cpp','lake_inline_scan.cpp']
        for name in owned:
            rel='contrib/og_lake/'+name
            check('exact compiled input/'+name,sha256(SOURCE/rel),b['native_source_inputs_after'][rel])
        check('actual source contains prebound per-worker index interface','LakeStreamWorkerScalarSources'in(SOURCE/'contrib/og_lake/lake_stream.cpp').read_text())
        control=connect_control();query(control,'SET session_timeout=0');query(control,'SET statement_timeout=30000')
        check('automatic maintenance remains off',scalar(control,'SHOW enable_lake_maintenance'),'off')
        check('dedicated diagnostic backend',scalar(control,'SHOW enable_thread_pool'),'off')
        log_source=NativeLogSource.from_connection(control,RUNTIME)
        with(out/'fixture.log').open('w')as log:subprocess.run([sys.executable,str(D/'tests/catalog_fixture_bundle.py'),'--run-id',a.run_id,'--output',str(out/'fixture/manifest.json')],stdout=log,stderr=subprocess.STDOUT,timeout=300,check=True)
        db=OwnedDatabase(a.run_id,out);db.create();roles=OwnedRoles(control,a.run_id,out)
        t=Case(db,SimpleNamespace(mode='net_dml',sql_version=a.sql_version,manifest=out/'fixture/manifest.json',output=out/'result.json',run_id=a.run_id),r,roles)
        r['installed_sql']=scalar(t.a,"SELECT extversion FROM pg_extension WHERE extname='og_lake'")
        r['runtime_module_version']=json.loads(scalar(t.a,'SELECT og_lake.host_status()'))['module_version']
        check('fresh installation uses explicitly selected SQL',r['installed_sql'],a.sql_version)
        r['runtime_default_version']=reviewed_runtime_version()
        check('SMP runtime matches exact official source control',r['runtime_module_version'],r['runtime_default_version'])
        for c in [t.a,t.b,t.observer]:configure(c)
        typ='double precision'if a.type=='double'else a.type
        query(t.a,'SET enable_default_ustore_table='+('on'if a.storage=='ustore'else'off'))
        query(t.a,'CREATE FOREIGN TABLE biz.orders(id '+typ+",val integer) SERVER created_lake OPTIONS(binding_mode 'create',lake_schema 'main')")
        query(t.a,'SET enable_default_ustore_table=off');query(t.a,'CREATE TABLE biz.exact(id '+typ+',val integer)')
        query(t.a,"SELECT og_lake.set_option('biz.orders'::regclass,'data_inlining_row_limit','100000')")
        query(t.a,"CREATE FUNCTION biz.smp_probe(integer,integer,bigint) RETURNS text AS '$libdir/og_lake','og_lake_test_smp_probe' LANGUAGE C VOLATILE NOT FENCED")
        query(t.a,'REVOKE ALL ON FUNCTION biz.smp_probe(integer,integer,bigint) FROM PUBLIC')
        r['native_runtime_executed']=True
        for f in range(4):
            lo=8192*f+1;hi=8192*(f+1)
            both('INSERT INTO {table} SELECT CASE WHEN i%997=0 THEN NULL ELSE i%257 END,i FROM generate_series('+str(lo)+','+str(hi)+')i')
            query(t.a,"SELECT og_lake.flush('biz.orders'::regclass)")
        both('INSERT INTO {table} VALUES(7,90007),(7,90008),(NULL,90009),(1000,90010)')
        carrier=query(t.a,"SELECT c.reloptions FROM created_metadata.ducklake_inlined_data_tables t JOIN pg_class c ON c.relname=t.table_name AND c.relnamespace=(SELECT oid FROM pg_namespace WHERE nspname='created_metadata')")
        check('real requested inline storage',len(carrier)==1 and ('storage_type=ustore'in(carrier[0][0]or []))==(a.storage=='ustore'))
        check('four actual immutable source files',scalar(t.a,'SELECT count(*) FROM created_metadata.ducklake_data_file'),4)
        store=out/'index-store';store.mkdir();query(t.a,"SELECT og_lake.set_option('created_lake'::name,'index_root',%s::text)",(str(store),))
        uuid=str(scalar(t.a,"SELECT og_lake.create_lake_index('biz.orders'::regclass,'scalar_smp',%s::jsonb)",(json.dumps(dict(version=1,kind='scalar',method=a.method,column='id')),)))
        query(t.a,'SELECT og_lake.build_lake_index(%s::uuid)',(uuid,));query(t.a,'ANALYZE biz.orders')
        artifact_inputs={str(p):sha256(p)for p in store.rglob('*.parquet')}
        r['owned_index_artifact_inputs']=artifact_inputs
        check('actual immutable index artifacts exist',len(artifact_inputs)>=4)
        native_sources=[Path(p)for(p,)in query(t.a,'SELECT DISTINCT source_path FROM og_lake_internal.index_coverage WHERE index_uuid=%s::uuid AND active ORDER BY source_path',(uuid,))]
        r['business_source_inputs']={str(p):sha256(p)for p in native_sources}
        check('actual complete source coverage recorded',len(native_sources),4)
        configure(t.a,4)
        plan='\n'.join(x[0]for x in query(t.a,'EXPLAIN (ANALYZE,COSTS OFF) '+select('id=7').format(table='biz.orders')))
        (out/'planned-stream.txt').write_text(plan+'\n');check('actual planned Stream scalar path','Stream'in plan and 'Lake SMP Planned DOP: 4'in plan and 'Planned File Access: scalar Parquet'in plan)
        for dop in [2,4]:
            state=capture('initial-dop'+str(dop),'id=7',dop)
            if dop==2:
                check('capacity regression has four real partitioned files',len(state['files']),4)
                check('capacity regression includes the independent inline task',state['inline_task'],True)
                check('capacity regression each actual lane consumed two indexed source entries',
                    [w['scalar_files']+w['fallback_files']for w in state['workers']],[2,2])
                r['capacity_regression_dop2']=deepcopy(state)
        head=t.head();protocol=t.protocol()
        if a.mode=='predicates':
            for i,predicate in enumerate(['id<4','id<=4','id>253','id>=253','7=id','id IS NULL','id IS NOT NULL',
                "id>'2147483647'::bigint","id<'-2147483649'::bigint",'id=ANY(ARRAY[7,1,7,NULL,257]::bigint[])',
                'id=ANY(ARRAY[]::integer[])','id=ANY(NULL::integer[])','id=ANY(ARRAY[NULL]::integer[])','id=7 AND val>10000']):
                # Float cross-family RHS is deliberately planned through builtin
                # float arguments, matching the codec's existing type contract.
                if a.type in ['real','double']and('bigint'in predicate or 'integer[]'in predicate):continue
                capture('predicate-'+str(i),predicate,may_prune=True)
            query(t.a,'SET plan_cache_mode=force_generic_plan')
            query(t.a,'PREPARE scalar_smp_parameter('+typ+') AS '+select('id=$1').format(table='biz.orders'))
            for i,value in enumerate([7,1,None,7,256,2,7]):
                configure(t.a,4);stats(-1);mark=log_source.mark()
                check('native external parameter rows/'+str(i),query(t.a,'EXECUTE scalar_smp_parameter(%s)',(value,)),query(t.observer,select('id=%s').format(table='biz.exact'),(value,)))
                completed('param-'+str(i),stats(),mark,4,4,0)
            query(t.a,'DEALLOCATE scalar_smp_parameter');query(t.a,'SET plan_cache_mode=auto')
            capture('unsupported-OR-complete-SMP','id=7 OR val=90010',covered=0,fallback=4)
        elif a.mode=='visibility':
            both('UPDATE {table} SET id=900,val=900123 WHERE val=123')
            both('UPDATE {table} SET id=901,val=901123 WHERE val=900123')
            both('DELETE FROM {table} WHERE val=124')
            for i,predicate in enumerate(['id=123','id=900','id=901','id=124','id IS NULL']):capture('twice-update-'+str(i),predicate)
            query(t.b,"SELECT og_lake.flush('biz.orders'::regclass)")
            capture('uncovered-flushed-afterimages','id=901',covered=4,fallback=1)
            capture('deleted-base-version','id=123',covered=4,fallback=1)
        elif a.mode=='fallback':
            directory=Path(scalar(t.a,"SELECT artifact_root_uri FROM og_lake_internal.index_segment WHERE index_uuid=%s::uuid AND state='READY' ORDER BY segment_uuid LIMIT 1",(uuid,)))
            check('derived directory is inside exact owned store',directory.resolve(strict=True).is_relative_to(store.resolve(strict=True)))
            check('derived directory has exact OS owner and no symlink',not directory.is_symlink()and directory.stat().st_uid==os.getuid())
            artifact=directory/('bitmap_data.parquet'if a.method=='bitmap'else'page_lookup.parquet')
            # Use the actual producer filename; refusal precedes any mutation.
            if a.method=='bitmap':
                names=[p for p in directory.iterdir()if p.is_file()and p.suffix=='.parquet'];check('single bitmap artifact',len(names),1);artifact=names[0]
            check('owned artifact is a regular non-symlink file',stat.S_ISREG(artifact.lstat().st_mode)and not artifact.is_symlink()and artifact.stat().st_uid==os.getuid())
            original=artifact.read_bytes();preserved=artifact.with_name(artifact.name+'.owned-preserved');check('owned artifact exists',bool(original))
            check('owned preservation name does not overwrite anything',not preserved.exists()and not preserved.is_symlink())
            artifact.rename(preserved)
            try:capture('missing-derived-source-complete-fallback','id=7',covered=3,fallback=1)
            finally:preserved.rename(artifact)
            artifact.write_bytes(original[:-16])
            try:capture('bad-derived-source-complete-fallback','id IS NULL',covered=3,fallback=1)
            finally:artifact.write_bytes(original)
            capture('restored-derived-source','id=7')
        else:
            key=(1<<43)+int(hashlib.sha256(a.run_id.encode()).hexdigest()[:9],16)
            query(t.b,'SELECT pg_advisory_lock(%s::bigint)',(key,));configure(t.a,4);stats(-1);stats(1,1,key)
            pid=scalar(t.a,'SELECT pg_backend_pid()');holder_pid=scalar(t.b,'SELECT pg_backend_pid()');dbid=scalar(t.observer,'SELECT oid FROM pg_database WHERE datname=current_database()')
            pool=ThreadPoolExecutor(max_workers=1);future=None
            try:
                future=pool.submit(query,t.a,select('id>=0').format(table='biz.orders'));deadline=time.monotonic()+30;locks=[]
                while time.monotonic()<deadline and not future.done():
                    locks=query(t.observer,"SELECT pid FROM pg_locks WHERE locktype='advisory' AND database=%s AND classid=%s::oid AND objid=%s::oid AND objsubid=1 AND mode='ExclusiveLock' AND NOT granted",(dbid,key>>32,key&0xffffffff))
                    if locks:break
                    time.sleep(.01)
                check('actual candidate SDK row gate reached',len(locks),1)
                check('cancel native top consumer',scalar(t.observer,'SELECT pg_cancel_backend(%s)',(pid,)))
                try:future.result(timeout=30)
                except BaseException as exc:check('native cancellation SQLSTATE',getattr(exc,'pgcode',None),'57014')
                else:check('cancel cannot return partial success',False)
            finally:
                try:
                    query(t.b,'ROLLBACK');query(t.b,'SELECT pg_advisory_unlock(%s::bigint)',(key,))
                    held=query(t.observer,"SELECT pid FROM pg_locks WHERE locktype='advisory' AND database=%s AND classid=%s::oid AND objid=%s::oid AND objsubid=1 AND pid=%s AND mode='ExclusiveLock' AND granted",(dbid,key>>32,key&0xffffffff,holder_pid))
                    r['exact_gate_cleanup']=dict(pid=holder_pid,key=key,remaining=held)
                    if held:preserve=True
                except BaseException:preserve=True
                if future is not None and not future.done():
                    try:t.a.cancel()
                    except BaseException:pass
                    try:future.result(timeout=30)
                    except BaseException:pass
                if future is not None and not future.done():preserve=True;exit_without_join=True;pool.shutdown(wait=False,cancel_futures=True)
                else:pool.shutdown(wait=True)
            if preserve:raise RuntimeError('Unproven exact gate/drain cleanup; preserve owned scene')
            state=stats();r['cancel_failed_round']=deepcopy(state)
            check('actual cancel participant used at least one verified scalar file',state['workers'][1]['scalar_files']>=1)
            check('actual index participant had initialized reader',state['workers'][1]['reader_ready'],1)
            check('each claimed candidate owner retired',all(w['done']in(1,2)for w in state['workers']if w['claimed']))
            capture('same-backend-after-cancel','id=7')
            for mode,label in [(2,'worker-error'),(3,'pre-start-error'),(4,'partial-start-error')]:
                stats(-1);stats(mode,1,0)
                try:query(t.a,select('id>=0').format(table='biz.orders'))
                except BaseException as exc:check(label+' native SQLSTATE',getattr(exc,'pgcode',None),'XX000')
                else:check(label+' must fail',False)
                failed=stats();r[label+'-failed-round']=deepcopy(failed)
                if mode==2:
                    check('worker error came from claimed file participant1',failed['workers'][1]['claimed'],1)
                    check('worker error followed its real reader initialization',failed['workers'][1]['reader_ready'],1)
                    check('worker error held a verified index selection',failed['workers'][1]['scalar_files']>=1)
                if mode==3:check('index pre-start failure started zero participants',sum(w['started']for w in failed['workers']),0)
                if mode==4:check('index partial-start failure started exactly one participant',sum(w['started']for w in failed['workers']),1)
                check(label+' all claimed participants retired before probe reset',all(w['done']in(1,2)for w in failed['workers']if w['claimed']))
                capture(label+'-reuse','id=7')
        if a.mode!='visibility':
            check('read-only native index SMP allocates no lake snapshot or IDs',t.head(),head)
            check('read-only native index SMP mutates no lake or host carrier data',t.protocol(),protocol)
        check('derived artifacts restored byte for byte',{str(p):sha256(p)for p in store.rglob('*.parquet')},artifact_inputs)
        for path,h in r['business_source_inputs'].items():check('immutable original business source/'+Path(path).name,sha256(path),h)
        for p,h in r['inputs'].items():check('frozen executed input/' +Path(p).name,sha256(p),h)
        check('final installed SQL remains selected',scalar(t.a,"SELECT extversion FROM pg_extension WHERE extname='og_lake'"),a.sql_version)
        check_build(a.build_receipt);r['status']='pass'
    except BaseException as exc:r.update(reason=str(exc),traceback=traceback.format_exc())
    finally:
        if preserve:r.update(status='fail',preserve_owned_scene=True)
        elif db is not None:
            try:db.cleanup();r['database_cleanup']=db.cleanup_result
            except BaseException as exc:r.update(status='fail',database_cleanup_error=str(exc))
        if roles is not None and not preserve:
            try:
                if not r.get('database_cleanup')or r['database_cleanup']['returncode']:raise RuntimeError('Owned database cleanup unproven')
                r['role_cleanup']=cleanup_roles(control,roles.path,a.run_id);r=roles.redact(r)
            except BaseException as exc:r.update(status='fail',role_cleanup_error=str(exc))
        if control is not None:control.close()
        write_json(out/'result.json',r)
    if exit_without_join:os._exit(1)
    return 0 if r['status']=='pass'else 1
if __name__=='__main__':raise SystemExit(main())
