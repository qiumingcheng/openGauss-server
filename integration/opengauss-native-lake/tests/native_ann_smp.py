#!/usr/bin/env python3
"""Candidate Native raw filtered-file ANN Stream qualification; not yet executed.

Target scope is SQL19, two raw format1 sources, one numeric scalar leaf and
DOP2. IP/cosine extension qualification is pending; the runner is a candidate. Original SQL distances and global Sort/Limit retain authority.
Native exact local Sort/Limit may reduce transfer after evaluating original
SQL distances; the lake ANN layer must not truncate its candidate domain.
This checks the same finite fixture candidate domain as serial ANN and exact
ordinary heap SQL, not general ANN recall, PQ, arbitrary fusion, global ANN
TopK parallelism, performance, object storage or target-RAC production use.
"""
import argparse,ast,hashlib,json,math,os,re,shutil,signal,subprocess,sys,time,traceback
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
ROOT=Path(os.environ.get('CPP_WORKSPACE','/home/qiumc/data/code/cpp')).resolve()
D=ROOT/'integration/opengauss-native-lake';sys.path[:0]=[str(D),str(D/'tests')]
from file_dml_acceptance import check_build
from inline_fixture import query,scalar
from lake_harness import sha256,validate_run_id,write_json
from native_auto_flush import connect_control
from native_runtime import PROFILE,RUNTIME,SOURCE,INSTALL
from native_runtime_log import NativeLogSource
from native_table_create import Case
from native_test_context import OwnedDatabase
from permission_api import OwnedRoles,cleanup_roles
TASK=re.compile(r'lake SMP task query=(\d+) scan=(\d+) snapshot=(\d+) participant=(\d+)/(\d+) task=(\d+)(?: file=(\d+))? inline=([01])')
ANN=re.compile(r'lake SMP ANN query=(\d+) scan=(\d+) snapshot=(\d+) participant=(\d+)/(\d+) verified=(\d+) pq=(\d+) fallback=(\d+) candidates=(\d+) local_k=none')
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



def distance_class(value):
    return 'NULL'if value is None else'NaN'if math.isnan(value)else float(value).hex()

def matching_identity_distances(rows, universe):
    # Fixture id plus original vector bytes identifies one ordinary heap row.
    oracle={tuple(row[:2]):distance_class(row[2])for row in universe}
    return len(oracle)==len(universe)and all(tuple(row[:2])in oracle and
        distance_class(row[2])==oracle[tuple(row[:2])]for row in rows)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-id',required=True);parser.add_argument('--build-receipt',type=Path,required=True)
    parser.add_argument('--storage',choices=['heap','ustore'],default='heap')
    parser.add_argument('--metric',choices=['l2','ip','cosine'],default='l2')
    parser.add_argument('--mode',choices=['domain','visibility','fallback','cancel','boundaries','snapshot','ties','metric-edges'],required=True)
    parser.add_argument('--case-timeout',type=int,default=900)
    parser.add_argument('--_isolated-child',action='store_true',help=argparse.SUPPRESS)
    a=parser.parse_args();validate_run_id(a.run_id)
    if a.metric!='l2'and a.mode in ['snapshot','ties']:parser.error('snapshot/ties remain the original L2-only qualification; use targeted metric modes first')
    if PROFILE!='ann-debug':parser.error('This candidate requires matching owned ann-debug catalogs')
    if not 300<=a.case_timeout<=1800:parser.error('--case-timeout must be300..1800')
    if not a._isolated_child:return supervised_case(a)
    if os.environ.get('OG_LAKE_SMP_ISOLATED_CHILD_PARENT')!=str(os.getppid()):parser.error('Unsupervised child')
    out=ROOT/'公共文档/openGauss原生湖存储/验证'/a.run_id;out.mkdir(exist_ok=False)
    r=dict(status='fail',scope=__doc__,mode=a.mode,storage=a.storage,metric=a.metric,assertions=[],native_runtime_executed=False)
    control=db=roles=t=log_source=None;preserve=False;exit_without_join=False
    inputs={Path(__file__),a.build_receipt,D/'tests/file_dml_acceptance.py',D/'tests/native_table_create.py',
        D/'tests/native_runtime_log.py',D/'native_test_context.py',D/'native_runtime.py',D/'inline_fixture.py',
        D/'lake_harness.py',D/'tests/native_auto_flush.py',D/'tests/permission_api.py',D/'tests/catalog_fixture_bundle.py',
        D/'source-lock.json',D/'reference.sh',D/'fixture.py'}
    pending=list(inputs)
    while pending:
        current=pending.pop()
        if current.suffix!='.py':continue
        for node in ast.walk(ast.parse(current.read_text())):
            names=[x.name.split('.')[0]for x in node.names]if isinstance(node,ast.Import)else [node.module.split('.')[0]]if isinstance(node,ast.ImportFrom)and node.module else []
            for name in names:
                for parent in [D,D/'tests']:
                    candidate=parent/(name+'.py')
                    if candidate.is_file()and candidate not in inputs:inputs.add(candidate);pending.append(candidate)
    provider=INSTALL/'lib/postgresql/og_lake_sdk/libog_lake_sdk_arrow.so';inputs.add(provider)
    r['inputs']={str(p):sha256(p)for p in sorted(inputs,key=str)};archive=out/'executed-inputs';archive.mkdir()
    for i,p in enumerate(sorted(inputs,key=str)):shutil.copy2(p,archive/(str(i)+'-'+p.name))
    def image(v):
        if isinstance(v,Counter):return sorted((repr(k),n)for k,n in v.items())
        return dict(count=len(v),sha256=hashlib.sha256(repr(v).encode()).hexdigest())if isinstance(v,list)and len(v)>30 else deepcopy(v)
    def check(label,got,want=True):
        ok=got==want;r['assertions'].append(dict(name=label,observed=image(got),expected=image(want),status='pass'if ok else'fail'))
        write_json(out/'result.json',r)
        if not ok:raise AssertionError(label+': '+repr(image(got)))
    def configure(c,dop=2):
        for statement in ['SET session_timeout=0','SET statement_timeout=90000','SET enable_vector_engine=off',
            'SET enable_lake_smp=on','SET enable_lake_ann=on','SET ann_orderby_distance_reuse=off',
            'SET enable_indexscan=on','SET enable_seqscan=off','SET query_max_mem=0',
            'SET query_dop='+str(1000+dop if dop>1 else 1)]:query(c,statement)
    def stats(mode=0,participant=0,gate=0):return json.loads(scalar(t.a,'SELECT biz.smp_probe(%s,%s,%s)',(mode,participant,gate)))
    def both(statement):
        query(t.b,'BEGIN')
        try:
            for table in ['biz.vectors','biz.exact']:query(t.b,statement.format(table=table))
            query(t.b,'COMMIT')
        except BaseException:query(t.b,'ROLLBACK');raise
    operator={'l2':'<->','ip':'<#>','cosine':'<=>'}[a.metric]
    metric_name={'l2':'L2','ip':'IP','cosine':'cosine'}[a.metric]
    per_file_candidates=31 if a.metric=='cosine'else 32
    file_candidates=per_file_candidates*2
    def statement(table='biz.vectors',rhs='%s::vector',predicate='id>=1',k=10,positive_norm=True):
        where='v IS NOT NULL'+(' AND vector_norm(v)>0'if a.metric=='cosine'and positive_norm else'')
        return 'SELECT id,v,v '+operator+' '+rhs+' AS distance FROM '+table+' WHERE '+where+' AND '+predicate+' ORDER BY v '+operator+' '+rhs+' LIMIT '+str(k)
    def oracle_rows(label,actual,expected):
        check(label+' original-vector SQL distance order',[distance_class(row[2])for row in actual],[distance_class(row[2])for row in expected])
        check(label+' original row and its own distance/multiplicity',Counter((*row[:2],distance_class(row[2]))for row in actual),Counter((*row[:2],distance_class(row[2]))for row in expected))
    def completed(label,state,mark,covered=2,fallback=0,candidates=None):
        check(label+' actual two native participants',state['participants'],2)
        for field in ['started','claimed','done','reader_ready']:check(label+' each worker '+field,[w[field]for w in state['workers']],[1,1])
        check(label+' actual raw ANN source coverage',sum(w['ann_files']for w in state['workers']),covered)
        check(label+' no scalar index substitution',sum(w['scalar_files']for w in state['workers']),0)
        check(label+' full-source fallbacks',sum(w['fallback_files']for w in state['workers']),fallback)
        if candidates is not None:check(label+' candidates are not truncated to worker K',sum(w['ann_candidates']for w in state['workers']),candidates)
        def ready(text):return len([m for m in ANN.finditer(text)if int(m[1])==state['query']and int(m[2])==state['scan']])>=2
        try:text=mark.wait(ready,5)
        finally:write_json(out/(label+'-log-source.json'),mark.receipt())
        (out/(label+'-server.log')).write_text(text)
        offset=int(state['inline_task']);expected=[]
        if offset:expected.append((state['query'],state['scan'],state['snapshot'],0,2,0,-1,1))
        for f,fid in enumerate(state['files']):expected.append((state['query'],state['scan'],state['snapshot'],(f+offset)%2,2,f+offset,fid,0))
        tasks=[tuple(map(int,m.groups(default='-1')))for m in TASK.finditer(text)if int(m[1])==state['query']and int(m[2])==state['scan']]
        check(label+' each real source and inline task has one exact owner',sorted(tasks),sorted(expected))
        evidence=[tuple(map(int,m.groups()))for m in ANN.finditer(text)if int(m[1])==state['query']and int(m[2])==state['scan']]
        check(label+' one real ANN report per participant',len(evidence),2)
        for w in state['workers']:
            p=w['participant'];owned=sum((f+offset)%2==p for f in range(len(state['files'])))
            check(label+' partition coverage/'+str(p),w['ann_files']+w['fallback_files'],owned)
        r.setdefault('completed_scans',{})[label]=state
    def capture(label,predicate='id>=1',q='[0.123,0.3,0.7]',covered=2,fallback=0,candidates=None):
        configure(t.a);stats(-1);mark=log_source.mark()
        actual=query(t.a,statement(predicate=predicate),(q,q));expected=query(t.observer,statement('biz.exact',predicate=predicate),(q,q))
        serial=query(t.b,statement(predicate=predicate),(q,q))
        oracle_rows(label,actual,expected);oracle_rows(label+' serial ANN reference',serial,expected)
        check(label+' same original-vector rows as DOP1 ANN',actual if a.metric=='l2'else Counter(actual),serial if a.metric=='l2'else Counter(serial))
        completed(label,stats(),mark,covered,fallback,candidates)
        return actual
    try:
        receipt=check_build(a.build_receipt);r.update(source_head=receipt['source_head'],binary_sha256=receipt['binary_sha256'])
        for name in ['lake_stream.cpp','lake_stream.h','lake_file_ann_catalog.cpp','lake_file_ann_catalog.h',
            'lake_file_reader.cpp','lake_file_reader.h','lake_inline_reader.cpp','lake_inline_reader.h','lake_inline_scan.cpp','lake_inline_ann.cpp']:
            rel='contrib/og_lake/'+name;check('exact compiled input/'+name,sha256(SOURCE/rel),receipt['native_source_inputs_after'][rel])
        check('compiled raw ANN worker contract','LakeStreamWorkerAnnReady'in(SOURCE/'contrib/og_lake/lake_stream.cpp').read_text())
        control=connect_control();query(control,'SET session_timeout=0');query(control,'SET statement_timeout=30000')
        check('maintenance is off',scalar(control,'SHOW enable_lake_maintenance'),'off')
        check('dedicated non-thread-pool runtime',scalar(control,'SHOW enable_thread_pool'),'off')
        log_source=NativeLogSource.from_connection(control,RUNTIME)
        with(out/'fixture.log').open('w')as log:subprocess.run([sys.executable,str(D/'tests/catalog_fixture_bundle.py'),'--run-id',a.run_id,'--output',str(out/'fixture/manifest.json')],stdout=log,stderr=subprocess.STDOUT,timeout=300,check=True)
        db=OwnedDatabase(a.run_id,out);db.create();roles=OwnedRoles(control,a.run_id,out)
        t=Case(db,SimpleNamespace(mode='net_dml',sql_version='0.19',manifest=out/'fixture/manifest.json',output=out/'result.json',run_id=a.run_id),r,roles)
        for c in [t.a,t.b,t.observer]:configure(c,1)
        query(t.a,'SET enable_default_ustore_table='+('on'if a.storage=='ustore'else'off'))
        query(t.a,"CREATE FOREIGN TABLE biz.vectors(id integer,v vector(3)) SERVER created_lake OPTIONS(binding_mode 'create',lake_schema 'main')")
        query(t.a,'SET enable_default_ustore_table=off');query(t.a,'CREATE TABLE biz.exact(id integer,v vector(3))')
        query(t.a,"SELECT og_lake.set_option('biz.vectors'::regclass,'data_inlining_row_limit','100000')")
        query(t.a,"CREATE FUNCTION biz.smp_probe(integer,integer,bigint) RETURNS text AS '$libdir/og_lake','og_lake_test_smp_probe' LANGUAGE C VOLATILE NOT FENCED")
        query(t.a,'REVOKE ALL ON FUNCTION biz.smp_probe(integer,integer,bigint) FROM PUBLIC')
        r['native_runtime_executed']=True
        for lo,hi in [(1,32),(1001,1032)]:
            if a.metric=='l2':
                both("INSERT INTO {table} SELECT i,('['||i::text||',0,0]')::vector FROM generate_series("+str(lo)+','+str(hi)+')i')
            else:
                # Reuse the already reviewed three-distance serial fixture's
                # noncollinear coordinates, mapping the second file to33..64.
                n='(CASE WHEN i>=1000 THEN i-968 ELSE i END)'
                vector="('['||"+n+"::text||','||("+n+"*"+n+"/100.0)::text||','||("+n+"%13+.125)::text||']')::vector"
                if a.metric=='cosine':
                    vector="CASE WHEN i IN(1,1001)THEN '[0,0,0]'::vector WHEN i IN(2,1002)THEN '[1e-30,0,0]'::vector ELSE "+vector+' END'
                both('INSERT INTO {table} SELECT i,'+vector+' FROM generate_series('+str(lo)+','+str(hi)+')i')
            query(t.a,"SELECT og_lake.flush('biz.vectors'::regclass)")
        both("INSERT INTO {table} VALUES(2001,'[0.1,0,0]'::vector),(2002,'[0.2,0,0]'::vector)")
        check('exactly two physical files',scalar(t.a,'SELECT count(*) FROM created_metadata.ducklake_data_file'),2)
        check('exactly64 physical data rows',int(scalar(t.a,'SELECT sum(record_count) FROM created_metadata.ducklake_data_file')),64)
        store=out/'index-store';store.mkdir();query(t.a,"SELECT og_lake.set_option('created_lake'::name,'index_root',%s::text)",(str(store),))
        spec=dict(version=1,kind='vector',method='diskann',column='v',metric=a.metric,enable_pq=False)
        uuid=str(scalar(t.a,"SELECT og_lake.create_lake_index('biz.vectors'::regclass,'ann_smp',%s::jsonb)",(json.dumps(spec),)))
        query(t.a,'SELECT og_lake.build_lake_index(%s::uuid)',(uuid,));query(t.a,'ANALYZE biz.vectors')
        other_uuid=None
        if a.mode=='metric-edges':
            other_metric='ip'if a.metric=='l2'else'l2'
            other_spec=dict(spec,metric=other_metric)
            other_uuid=str(scalar(t.a,"SELECT og_lake.create_lake_index('biz.vectors'::regclass,'ann_other_metric',%s::jsonb)",(json.dumps(other_spec),)))
            query(t.a,'SELECT og_lake.build_lake_index(%s::uuid)',(other_uuid,))
            check('other metric has its own genuine source coverage',scalar(t.a,"SELECT count(*) FROM og_lake_internal.index_coverage WHERE index_uuid=%s::uuid AND active",(other_uuid,)),2)
        r['owned_index_artifact_inputs']={str(p):sha256(p)for p in store.rglob('*.parquet')}
        check('two active raw source segments',scalar(t.a,"SELECT count(*) FROM og_lake_internal.index_segment WHERE index_uuid=%s::uuid AND state='READY' AND format_version=1",(uuid,)),2)
        source_paths=[Path(row[0])for row in query(t.a,'SELECT source_path FROM og_lake_internal.index_coverage WHERE index_uuid=%s::uuid AND active ORDER BY source_file_id',(uuid,))]
        r['business_source_inputs']={str(p):sha256(p)for p in source_paths};check('two independently hashed sources',len(source_paths),2)
        serial_plan='\n'.join(row[0]for row in query(t.b,'EXPLAIN (ANALYZE,COSTS OFF) '+statement(),('[0.123,0.3,0.7]','[0.123,0.3,0.7]')))
        (out/'serial-ann-plan.txt').write_text(serial_plan+'\n')
        check('DOP1 reference really reads both ANN sources','Verified ANN File Coverage: 2'in serial_plan and 'Lake SMP ANN:'not in serial_plan)
        configure(t.a)
        unshipped_sql=statement().replace('SELECT id,v,v ',"SELECT id,encode(vector_send(v),'hex'),v ",1)
        unshipped_plan='\n'.join(row[0]for row in query(t.a,'EXPLAIN (ANALYZE,COSTS OFF) '+unshipped_sql,('[0.123,0.3,0.7]','[0.123,0.3,0.7]')))
        (out/'unshippable-vector-send-plan.txt').write_text(unshipped_plan+'\n')
        check('host inserts no Stream for unshippable vector_send projection','Streaming'in unshipped_plan,False)
        check('unshippable vector_send projection is serial ANN','Lake SMP: serial path'in unshipped_plan and 'Verified ANN File Coverage: 2'in unshipped_plan)
        plan='\n'.join(row[0]for row in query(t.a,'EXPLAIN (ANALYZE,COSTS OFF) '+statement(),('[0.123,0.3,0.7]','[0.123,0.3,0.7]')))
        (out/'initial-plan.txt').write_text(plan+'\n')
        check('real raw ANN DOP2 path','Lake SMP Planned DOP: 2'in plan and('Lake SMP ANN: raw '+metric_name+' single-filter')in plan)
        check('original SQL global consumer sort above Stream',plan.index('Sort')<plan.index('Stream'))
        # Native exact Sort/Limit may safely reduce each producer to the SAME
        # SQL K after all original candidate distances/quals. This is separate
        # from forbidden truncation in ANN graph/source-candidate selection.
        r['native_topk_plan_scope']=dict(plan='initial-plan.txt',
            local_limit_allowed_after_exact_original_distance=True,
            ann_candidate_count_is_not_stream_transmission_count=True,
            worker_sort_limit_nodes=plan.count('Limit'),
            source='createplan.cpp parallel_limit_sort passes unchanged limitCount; noOFFSET/Distinct/Agg in this slice')
        check('actual one-consumer two-producer native local gather',bool(re.search(r'Streaming\(type: LOCAL GATHER dop: 1/2\)',plan)))
        rows=capture('initial',candidates=file_candidates)
        if a.metric=='l2':check('inline nearest rows compete with one entire near source',Counter(row[0]for row in rows),Counter([2001,2002,*range(1,9)]))
        elif a.metric=='ip':check('original SQL negative inner product is not L2',all(row[2]<0 for row in rows))
        else:check('original finite SQL cosine distances retained',all(math.isfinite(row[2])and 0<=row[2]<=2 for row in rows))
        head=t.head();protocol=t.protocol()
        if a.mode=='domain':
            capture('one-far-source','id>=1000',candidates=per_file_candidates)
            capture('only-inline','id>=2000',candidates=0)
            query(t.a,'SET plan_cache_mode=force_generic_plan')
            query(t.a,'PREPARE ann_smp_parameter(vector,integer) AS '+statement(rhs='$1',predicate='id>=$2'))
            for i,(q,bound,count)in enumerate([('[0.123,0.3,0.7]',1,file_candidates),('[1005.31,0.3,0.7]',1000,per_file_candidates),('[0.123,0.3,0.7]',2000,0),('[5.31,0.3,0.7]',1,file_candidates),('[5.31,0.3,0.7]',None,0)]):
                stats(-1);mark=log_source.mark();actual=query(t.a,'EXECUTE ann_smp_parameter(%s,%s)',(q,bound))
                oracle_rows('external epoch/'+str(i),actual,query(t.observer,statement('biz.exact',predicate='id>=%s'),(q,bound,q)))
                completed('external-'+str(i),stats(),mark,candidates=count)
            query(t.a,'DEALLOCATE ann_smp_parameter');query(t.a,'SET plan_cache_mode=auto')
        elif a.mode=='metric-edges':
            # Scope a valid Parquet/metric swap to exact owned derivative paths;
            # standard metadata, source data, tokens and roots are never edited.
            swap=query(t.a,"SELECT s.artifact_root_uri,other.artifact_root_uri FROM og_lake_internal.index_coverage c JOIN og_lake_internal.index_segment s USING(segment_uuid,index_uuid,definition_generation) JOIN og_lake_internal.index_coverage oc ON oc.source_file_id=c.source_file_id AND oc.index_uuid=%s::uuid AND oc.active JOIN og_lake_internal.index_segment other ON other.segment_uuid=oc.segment_uuid WHERE c.index_uuid=%s::uuid AND c.active ORDER BY c.source_file_id LIMIT 1",(other_uuid,uuid))
            check('real wrong-metric donor shares the exact physical source',len(swap),1)
            target,donor=map(Path,swap[0]);saved={}
            for directory in [target,donor]:check('metric-swap path is exact owned artifact',directory.resolve(strict=True).is_relative_to(store.resolve(strict=True))and not directory.is_symlink()and directory.stat().st_uid==os.getuid())
            try:
                for name in ['ann_nodes.parquet','ann_lookup.parquet']:
                    path=target/name;saved[path]=path.read_bytes();path.write_bytes((donor/name).read_bytes())
                capture('valid-parquet-wrong-metric-complete-fallback',covered=1,fallback=1,candidates=per_file_candidates)
            finally:
                for path,data in saved.items():path.write_bytes(data)
            capture('same-backend-after-metric-restore',candidates=file_candidates)
            # NULL/cosine-zero input is valid SQL. Compare exact native distance
            # classes and original rows while allowing legal ORDER BY ties.
            # NULL literal folding may remove the ANN sort expression entirely.
            # Retain a generic Param plan so NULL exercises the cached-reader
            # complete-source fallback rather than an unrelated ordinary path.
            for connection in [t.a,t.b]:
                query(connection,'SET plan_cache_mode=force_generic_plan')
                query(connection,'PREPARE metric_edge_query(vector) AS '+statement(rhs='$1'))
                query(connection,"EXECUTE metric_edge_query('[0.123,0.3,0.7]')")
            for label,q,covered,fallback in [('null-query',None,0,2)]+([('zero-cosine-query','[0,0,0]',0,2),('tiny-nonzero-cosine-query','[1e-30,0,0]',2,0)]if a.metric=='cosine'else[]):
                configure(t.a);stats(-1);mark=log_source.mark();args=(q,q)
                actual=query(t.a,'EXECUTE metric_edge_query(%s)',(q,));serial=query(t.b,'EXECUTE metric_edge_query(%s)',(q,))
                expected=query(t.observer,statement('biz.exact'),args)
                universe=query(t.observer,statement('biz.exact',k=1000),args)
                check(label+' native SQL distance classes',[distance_class(row[2])for row in actual],[distance_class(row[2])for row in expected])
                check(label+' serial distance classes',[distance_class(row[2])for row in serial],[distance_class(row[2])for row in expected])
                check(label+' every actual row carries its own heap SQL distance',matching_identity_distances(actual,universe))
                check(label+' every serial row carries its own heap SQL distance',matching_identity_distances(serial,universe))
                check(label+' exact LIMIT population',len(actual),10)
                allowed=set(row[:2]for row in universe)
                check(label+' original source rows with no duplicate identity',len({row[:2]for row in actual})==10 and all(row[:2]in allowed for row in actual))
                completed(label,stats(),mark,covered,fallback,file_candidates if covered else 0)
                write_json(out/(label+'-canonical-rows.json'),dict(actual=[[*row[:2],distance_class(row[2])]for row in actual],expected=[[*row[:2],distance_class(row[2])]for row in expected]))
            for connection in [t.a,t.b]:
                query(connection,'DEALLOCATE metric_edge_query');query(connection,'SET plan_cache_mode=auto')
            if a.metric=='cosine':
                sql=statement(k=1000,positive_norm=False);args=('[0.123,0.3,0.7]','[0.123,0.3,0.7]')
                text='\n'.join(row[0]for row in query(t.a,'EXPLAIN (ANALYZE,COSTS OFF) '+sql,args));(out/'cosine-without-norm-plan.txt').write_text(text+'\n')
                check('absent positive source norm proof has no ANN SMP','Lake SMP ANN:'not in text)
                actual=query(t.a,sql,args);expected=query(t.observer,sql.replace('biz.vectors','biz.exact'),args)
                def canonical(rows):return Counter((row[0],row[1],distance_class(row[2]))for row in rows)
                check('ordinary SQL preserves zero-source native distances',canonical(actual),canonical(expected))
                check('two zero source rows are still physically visible',sum(row[0]in(1,1001)for row in actual),2)
        elif a.mode=='fallback':
            directory=Path(scalar(t.a,"SELECT artifact_root_uri FROM og_lake_internal.index_segment WHERE index_uuid=%s::uuid AND state='READY' ORDER BY segment_uuid LIMIT 1",(uuid,)))
            check('only this exact owned artifact directory',directory.resolve(strict=True).is_relative_to(store.resolve(strict=True))and not directory.is_symlink()and directory.stat().st_uid==os.getuid())
            artifact=directory/'ann_lookup.parquet';original=artifact.read_bytes();shutil.copy2(artifact,out/'original-owned-lookup.parquet')
            try:
                artifact.write_bytes(b'PAR1bad graph derived dataPAR1');capture('corrupt-graph-complete-source',covered=1,fallback=1,candidates=per_file_candidates)
            finally:artifact.write_bytes(original)
            query(t.a,'SELECT og_lake.build_lake_index(%s::uuid)',(uuid,));capture('explicit-build-restores',candidates=file_candidates)
        elif a.mode=='visibility':
            both("UPDATE {table} SET v='[0.15,0,0]'::vector WHERE id=1")
            both("UPDATE {table} SET v='[0.16,0,0]'::vector WHERE id=1")
            both('DELETE FROM {table} WHERE id=2')
            capture('updated-source-complete-fallback',covered=1,fallback=1,candidates=per_file_candidates)
            query(t.a,"SELECT og_lake.flush('biz.vectors'::regclass)")
            configure(t.a);plain='\n'.join(row[0]for row in query(t.a,'EXPLAIN (ANALYZE,COSTS OFF) '+statement(),('[0.123,0.3,0.7]','[0.123,0.3,0.7]')))
            (out/'three-file-unqualified-ann-plan.txt').write_text(plain+'\n')
            check('three files do not use this two-file ANN SMP path','Lake SMP ANN:'not in plain)
            oracle_rows('fresh committed flush complete SQL',query(t.a,statement(),('[0.123,0.3,0.7]','[0.123,0.3,0.7]')),query(t.observer,statement('biz.exact'),('[0.123,0.3,0.7]','[0.123,0.3,0.7]')))
        elif a.mode=='boundaries':
            query(t.a,'SET plan_cache_mode=force_generic_plan');query(t.a,'PREPARE cached_ann(vector) AS '+statement(rhs='$1'))
            query(t.a,"EXECUTE cached_ann('[0.123,0.3,0.7]')")
            for setting,restore in [('SET enable_lake_ann=off','SET enable_lake_ann=on'),('SET enable_lake_smp=off','SET enable_lake_smp=on'),('SET query_max_mem=131072','SET query_max_mem=0')]:
                stats(-1);query(t.a,setting)
                try:query(t.a,"EXECUTE cached_ann('[0.123,0.3,0.7]')")
                except BaseException as exc:check('cached pre-start contract/'+setting,getattr(exc,'pgcode',None),'0A000')
                else:check('cached unsupported ANN parallel input rejected/'+setting,False)
                check('rejection is before any actual workers/'+setting,stats()['participants'],0);query(t.a,restore)
            query(t.a,'DEALLOCATE cached_ann');query(t.a,'SET plan_cache_mode=auto');capture('post-rejection-reuse',candidates=file_candidates)
            for i,predicate in enumerate(['id>=1 AND id<20','id=1 OR id=1001','NOT(id=1)','id=ANY(ARRAY[1,1001])']):
                configure(t.a);q=statement(predicate=predicate);args=('[0.123,0.3,0.7]','[0.123,0.3,0.7]')
                text='\n'.join(row[0]for row in query(t.a,'EXPLAIN (ANALYZE,COSTS OFF) '+q,args));(out/('unsupported-'+str(i)+'-plan.txt')).write_text(text+'\n')
                check('complex filter retains another qualified path/'+str(i),'Lake SMP ANN:'not in text)
                oracle_rows('complex SQL unchanged/'+str(i),query(t.a,q,args),query(t.observer,statement('biz.exact',predicate=predicate),args))
        elif a.mode=='ties':
            # The tie boundary spans a real file row and twenty current inline
            # identities, exceeds SQL K=3, and crosses the two source owners.
            both("INSERT INTO {table} SELECT i,'[1,0,0]'::vector FROM generate_series(3001,3020)i")
            check('tie rows remain inline and preserve exactly two source files',scalar(t.observer,'SELECT count(*) FROM created_metadata.ducklake_data_file'),2)
            base=statement(k=3).rsplit(' LIMIT 3',1)[0]
            args=('[0,0,0]','[0,0,0]')
            for suffix,label,want_ann in [(' FETCH FIRST 3 ROWS WITH TIES','with-ties',True),
                (' FETCH FIRST 20 PERCENT ROWS WITH TIES','percent-with-ties',False)]:
                configure(t.a);stats(-1);mark=log_source.mark();sql=base+suffix
                actual=query(t.a,sql,args);expected=query(t.observer,sql.replace('biz.vectors','biz.exact'),args)
                oracle_rows(label+' complete boundary population',actual,expected)
                check(label+' all twenty-one boundary ties survive beyond SQL K',Counter(row[0]for row in actual),Counter([2001,2002,1,*range(3001,3021)]))
                if want_ann:completed(label,stats(),mark,2,0,64)
                text='\n'.join(row[0]for row in query(t.a,'EXPLAIN (ANALYZE,COSTS OFF) '+sql,args))
                (out/(label+'-plan.txt')).write_text(text+'\n')
                write_json(out/(label+'-rows.json'),dict(expected=expected,actual=actual))
                check(label+' has real native local gather',bool(re.search(r'Streaming\(type: LOCAL GATHER dop: 1/2\)',text)))
                check(label+' native sort follows global gather',text.index('Sort')<text.index('Streaming'))
                below=text[text.index('Streaming'):]
                check(label+' actual producer subtree has no plain worker limit',not re.search(r'(?:^|\n)\s*(?:->\s*)?Limit(?:\s|\()',below))
                check(label+' actual raw ANN qualification scope',('Lake SMP ANN:'in text),want_ann)
        elif a.mode=='snapshot':
            args=('[0.123,0.3,0.7]','[0.123,0.3,0.7]')
            old_expected=query(t.observer,statement('biz.exact'),args);old_head=t.head()[0]
            key=(1<<44)+int(hashlib.sha256(a.run_id.encode()).hexdigest()[:9],16)
            query(t.b,'SELECT pg_advisory_lock(%s::bigint)',(key,))
            holder=scalar(t.b,'SELECT pg_backend_pid()');dbid=scalar(t.observer,'SELECT oid FROM pg_database WHERE datname=current_database()')
            configure(t.a);stats(-1);stats(1,1,key);mark=log_source.mark();pool=ThreadPoolExecutor(max_workers=1);future=None
            old_actual=None;advanced_head=None;locks=[]
            try:
                future=pool.submit(query,t.a,statement(),args);deadline=time.monotonic()+30
                while time.monotonic()<deadline and not future.done():
                    locks=query(t.observer,"SELECT pid FROM pg_locks WHERE locktype='advisory' AND database=%s AND classid=%s::oid AND objid=%s::oid AND objsubid=1 AND mode='ExclusiveLock' AND NOT granted",(dbid,key>>32,key&0xffffffff))
                    if locks:break
                    time.sleep(.01)
                check('snapshot query really paused in participant1 live source SDK batch',len(locks),1)
                check('snapshot query has not completed before concurrent changes',future.done(),False)
                both("UPDATE {table} SET v='[0.123,0.3,0.7]'::vector WHERE id=1001")
                query(t.b,"SELECT og_lake.flush('biz.vectors'::regclass)")
                advanced_head=t.head()[0]
                check('concurrent UPDATE plus flush commits a newer lake snapshot',advanced_head>old_head)
                check('new flush really publishes an uncovered third source',scalar(t.observer,'SELECT count(*) FROM created_metadata.ducklake_data_file'),3)
                check('reader remains stopped at same source gate through UPDATE and flush',not future.done())
                r['snapshot_gate']=dict(holder_pid=holder,waiters=locks,key=key,database_oid=dbid,old_business_snapshot=old_head,new_business_snapshot=advanced_head)
            finally:
                # Unlock first. A normal successful query is allowed to drain;
                # cancellation is only the bounded failure cleanup fallback.
                try:
                    query(t.b,'ROLLBACK');query(t.b,'SELECT pg_advisory_unlock(%s::bigint)',(key,))
                    held=query(t.observer,"SELECT pid FROM pg_locks WHERE locktype='advisory' AND database=%s AND classid=%s::oid AND objid=%s::oid AND objsubid=1 AND pid=%s AND mode='ExclusiveLock' AND granted",(dbid,key>>32,key&0xffffffff,holder))
                    r['exact_snapshot_gate_cleanup']=dict(pid=holder,key=key,remaining=held)
                    if held:preserve=True
                except BaseException:preserve=True
                if future is not None and not future.done():
                    try:future.result(timeout=30)
                    except BaseException:pass
                if future is not None and not future.done():
                    try:t.a.cancel()
                    except BaseException:pass
                    try:future.result(timeout=30)
                    except BaseException:pass
                if future is not None and not future.done():preserve=True;exit_without_join=True;pool.shutdown(wait=False,cancel_futures=True)
                else:pool.shutdown(wait=True)
            if preserve:raise RuntimeError('Unproven snapshot gate/drain cleanup; retain exact owned scene')
            old_actual=future.result(timeout=0)
            write_json(out/'snapshot-old-rows.json',dict(expected=old_expected,actual=old_actual,business_snapshot=old_head))
            oracle_rows('ANN held old S_stmt through UPDATE plus flush',old_actual,old_expected)
            state=stats();completed('fixed-business-snapshot',state,mark,2,0,64)
            check('every participant bound the pre-change business snapshot',state['snapshot'],old_head)
            configure(t.a)
            fresh=query(t.a,statement(),args);fresh_expected=query(t.observer,statement('biz.exact'),args)
            write_json(out/'snapshot-fresh-rows.json',dict(expected=fresh_expected,actual=fresh,business_snapshot=advanced_head))
            oracle_rows('next statement sees committed UPDATE plus flush',fresh,fresh_expected)
            check('newly updated far-source identity now wins global distance order',fresh[0][0],1001)
            text='\n'.join(row[0]for row in query(t.a,'EXPLAIN (ANALYZE,COSTS OFF) '+statement(),args))
            (out/'snapshot-next-three-file-plan.txt').write_text(text+'\n')
            check('next three-file query retains first-slice planning boundary','Lake SMP ANN:'not in text)
        else:
            key=(1<<43)+int(hashlib.sha256(a.run_id.encode()).hexdigest()[:9],16)
            query(t.b,'SELECT pg_advisory_lock(%s::bigint)',(key,));pid=scalar(t.a,'SELECT pg_backend_pid()')
            dbid=scalar(t.observer,'SELECT oid FROM pg_database WHERE datname=current_database()');holder=scalar(t.b,'SELECT pg_backend_pid()')
            stats(-1);stats(1,1,key);pool=ThreadPoolExecutor(max_workers=1);future=None
            try:
                future=pool.submit(query,t.a,statement(),('[0.123,0.3,0.7]','[0.123,0.3,0.7]'));deadline=time.monotonic()+30;locks=[]
                while time.monotonic()<deadline and not future.done():
                    locks=query(t.observer,"SELECT pid FROM pg_locks WHERE locktype='advisory' AND database=%s AND classid=%s::oid AND objid=%s::oid AND objsubid=1 AND mode='ExclusiveLock' AND NOT granted",(dbid,key>>32,key&0xffffffff))
                    if locks:break
                    time.sleep(.01)
                check('real worker gate after live source SDK batch',len(locks),1)
                check('cancel actual top consumer',scalar(t.observer,'SELECT pg_cancel_backend(%s)',(pid,)))
                try:future.result(timeout=30)
                except BaseException as exc:check('real cancellation SQLSTATE',getattr(exc,'pgcode',None),'57014')
                else:check('cancel cannot return partial success',False)
            finally:
                try:
                    query(t.b,'ROLLBACK');query(t.b,'SELECT pg_advisory_unlock(%s::bigint)',(key,))
                    held=query(t.observer,"SELECT pid FROM pg_locks WHERE locktype='advisory' AND database=%s AND classid=%s::oid AND objid=%s::oid AND objsubid=1 AND pid=%s AND mode='ExclusiveLock' AND granted",(dbid,key>>32,key&0xffffffff,holder))
                    r['exact_gate_cleanup']=dict(pid=holder,key=key,remaining=held)
                    if held:preserve=True
                except BaseException:preserve=True
                if future is not None and not future.done():
                    try:t.a.cancel()
                    except BaseException:pass
                    try:future.result(timeout=30)
                    except BaseException:pass
                if future is not None and not future.done():preserve=True;exit_without_join=True;pool.shutdown(wait=False,cancel_futures=True)
                else:pool.shutdown(wait=True)
            if preserve:raise RuntimeError('Unproven bounded cancellation drain; preserve exact owned scene')
            state=stats();check('live worker initialized source reader',state['workers'][1]['reader_ready'],1)
            check('all claimed workers retired after cancel',all(w['done']in(1,2)for w in state['workers']if w['claimed']))
            capture('same-backend-after-source-cancel',candidates=file_candidates)
            for mode,label in [(2,'worker-error'),(3,'before-start'),(4,'partial-start')]:
                stats(-1);stats(mode,1,0)
                try:query(t.a,statement(),('[0.123,0.3,0.7]','[0.123,0.3,0.7]'))
                except BaseException as exc:check(label+' real SQLSTATE',getattr(exc,'pgcode',None),'XX000')
                else:check(label+' cannot succeed',False)
                capture(label+'-reuse',candidates=file_candidates)
        if a.mode not in ['visibility','snapshot','ties']:
            check('read-only queries allocate no lake snapshot or IDs',t.head(),head)
            check('read-only queries mutate no catalog or carrier',t.protocol(),protocol)
        check('immutable owned graph artifacts restored byte-for-byte',{str(p):sha256(p)for p in store.rglob('*.parquet')},r['owned_index_artifact_inputs'])
        for path,h in r['business_source_inputs'].items():check('immutable authoritative source/'+Path(path).name,sha256(path),h)
        for path,h in r['inputs'].items():check('frozen executed input/'+Path(path).name,sha256(path),h)
        check_build(a.build_receipt);r['status']='pass'
    except BaseException as exc:r.update(reason=str(exc),traceback=traceback.format_exc())
    finally:
        if preserve:r.update(status='fail',preserve_owned_scene=True)
        elif db is not None:
            try:db.cleanup();r['database_cleanup']=db.cleanup_result
            except BaseException as exc:r.update(status='fail',database_cleanup_error=str(exc))
        if roles is not None and not preserve:
            try:
                if not r.get('database_cleanup')or r['database_cleanup']['returncode']:raise RuntimeError('Exact owned DB cleanup unproven; retain roles')
                r['role_cleanup']=cleanup_roles(control,roles.path,a.run_id);r=roles.redact(r)
            except BaseException as exc:r.update(status='fail',role_cleanup_error=str(exc))
        if control is not None:control.close()
        write_json(out/'result.json',r)
    if exit_without_join:os._exit(1)
    return 0 if r['status']=='pass'else 1
if __name__=='__main__':raise SystemExit(main())
