#!/usr/bin/env python3
"""Prepared serial Native scalar PARAM_EXEC regression; default performs no SQL.

Each method gets its own fresh real EMPTY catalog, INT4/INT8 columns, two small
indexed files and inline data. Correlated subplans are checked against ordinary
heap SQL and actual ForeignScan loop/coverage diagnostics. No vector column,
ANN, Stream worker, performance or broader type qualification is involved.
"""
import argparse
import ast
from copy import deepcopy
import hashlib
import json
import os
import re
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import traceback
from types import SimpleNamespace
ROOT=Path(os.getenv('CPP_WORKSPACE','/home/qiumc/data/code/cpp')).resolve()
D=ROOT/'integration/opengauss-native-lake'


def prepare(method,sql_version="0.19"):
    return dict(status='prepared_not_executed',method=method,sql_version=sql_version,
        native_calls=0,file_rows=[32,32],inline_rows=3,outer_rows=32,
        cases=['INT4/INT8 correlated equality with32 executions and31 host rescans',
               'NULL initial outer key then5 changing-key rescans',
               'empty outer relation:0 executions of retained scalar subplan',
               'outer LIMIT3:actual3 subplan executions, no full lake rescans'],
        prerequisites=['Official matching Debug/ann-debug receipt with deferred serial PARAM_EXEC setup.',
            'SQL19 installed explicitly; maintenance and thread pool off; serial fixture reservation.',
            'Bitmap requires qualified private Roaring; B-tree and bitmap use distinct run IDs.'],
        limits=['Two methods, INT4/INT8, ordinary heap carrier and local files only.',
            'Inline access is native B-tree for both methods; bitmap qualifies file candidates only.',
            'No external PARAM_EXTERN, ANN/vector literal, SMP, concurrent DML, RAC or performance claim.'])


def statement(column,variant,table='biz.orders'):
    key={'id':'key4','k':'key8'}[column]
    where=' WHERE seq BETWEEN 2 AND 6'if variant=='null-first'else' WHERE seq<0'if variant=='zero'else''
    suffix=' LIMIT 3'if variant=='limit'else' ORDER BY q.seq'
    return ('SELECT q.seq,(SELECT o.val FROM '+table+' o WHERE o.'+column+'=q.'+key+
            ' ORDER BY o.val LIMIT 1 OFFSET 0) FROM (SELECT seq,key4,key8 FROM biz.keys'+where+
            ' ORDER BY seq OFFSET 0) q'+suffix)


def unique_object(pairs):
    result={}
    for name,value in pairs:
        if name in result:raise ValueError('Duplicate EXPLAIN key: '+name)
        result[name]=value
    return result


def supervise(args):
    # Same owned process-group pattern as the existing scalar SMP/cost probes.
    out=ROOT/'公共文档/openGauss原生湖存储/验证'/args.run_id
    if out.exists():raise FileExistsError('Refusing existing evidence: '+str(out))
    env=dict(os.environ,CPP_WORKSPACE=str(ROOT),PYTHONDONTWRITEBYTECODE='1',OG_LAKE_SCALAR_PARAM_CHILD=str(os.getpid()))
    command=[sys.executable,str(Path(__file__).resolve()),*sys.argv[1:],'--_isolated-child']
    process=subprocess.Popen(command,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True,env=env,start_new_session=True)
    reason=None
    try:output,_=process.communicate(timeout=args.case_timeout)
    except(subprocess.TimeoutExpired,KeyboardInterrupt)as exc:
        reason=type(exc).__name__+': owned scalar PARAM_EXEC deadline/interruption'
        try:os.killpg(process.pid,signal.SIGKILL)
        except ProcessLookupError:pass
        try:output,_=process.communicate(timeout=10)
        except subprocess.TimeoutExpired as drain:
            output=drain.output or''
            if isinstance(output,bytes):output=output.decode('utf-8','replace')
            if process.stdout is not None:process.stdout.close()
    out.mkdir(exist_ok=True);(out/'isolated-child.stdout').write_text(output or'')
    path=out/'result.json'
    try:data=json.loads(path.read_text())if path.is_file()else dict(status='fail',assertions=[])
    except(ValueError,OSError):data=dict(status='fail',assertions=[],reason='Child result incomplete')
    if path.is_file():shutil.copy2(path,out/'isolated-child-result.json')
    data['supervision']=dict(command=command,pid=process.pid,returncode=process.returncode,deadline_seconds=args.case_timeout,
        owned_process_group_killed=reason is not None,controller_sql_cleanup_attempted=False)
    if reason or process.returncode!=0:data['status']='fail'
    if reason:data.update(reason=reason,preserve_owned_scene=True)
    path.write_text(json.dumps(data,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps(dict(status=data['status'],result=str(path),reason=data.get('reason'))))
    return 0 if data['status']=='pass'else 1


def execute(args):
    sys.path[:0]=[str(D),str(D/'tests')]
    from psycopg2.extras import register_default_json
    from file_dml_acceptance import check_build
    from inline_fixture import query,scalar
    from lake_harness import sha256,validate_run_id,write_json
    from native_auto_flush import connect_control
    from native_runtime import PROFILE,SOURCE,INSTALL
    from native_table_create import Case
    from native_test_context import OwnedDatabase, reviewed_runtime_version
    from native_vector_batch_scan import walk
    from permission_api import OwnedRoles,cleanup_roles
    validate_run_id(args.run_id)
    if PROFILE not in ['debug','ann-debug']:raise ValueError('Owned Debug runtime required')
    if len(args.run_id)>90:raise ValueError('Leave genuine fixture suffix budget: run-id max90')
    out=ROOT/'公共文档/openGauss原生湖存储/验证'/args.run_id;out.mkdir(exist_ok=False)
    result=dict(status='fail',assertions=[],scope=__doc__,method=args.method,selected_sql=args.sql_version,plan=prepare(args.method,args.sql_version),plans=[],executed=False)
    control=db=roles=case=reservation=None
    def check(name,got,want=True):
        good=got==want;result['assertions'].append(dict(name=name,observed=deepcopy(got),expected=deepcopy(want),status='pass'if good else'fail'))
        write_json(out/'result.json',result)
        if not good:raise AssertionError(name+': '+repr(got))
    def session(c):
        for sql in ['SET session_timeout=0','SET statement_timeout=180000','SET query_dop=1','SET enable_vector_engine=off',
                    'SET enable_lake_smp=off','SET enable_default_ustore_table=off','SET enable_seqscan=off','SET enable_indexscan=on']:
            query(c,sql)
    def both(sql):
        query(case.b,'BEGIN')
        try:
            for table in ['biz.orders','biz.exact']:query(case.b,sql.format(table=table))
            query(case.b,'COMMIT')
        except BaseException:query(case.b,'ROLLBACK');raise
    def probe(column,variant,loops):
        label=column+'-'+variant;q=statement(column,variant)
        got=query(case.a,q);want=query(case.observer,statement(column,variant,'biz.exact'))
        check(label+' each original correlated result equals ordinary heap',got,want)
        check(label+' actual outer cardinality',len(got),loops)
        if variant=='all':
            check(label+' repeated input produces changing values',len({row[1]for row in got})>=6)
            check(label+' NULL outer key retains NULL scalar result',[row[1]for row in got if row[0]%8==2],[None]*4)
        with case.a.cursor()as cursor:
            register_default_json(cursor,loads=lambda raw:raw)
            cursor.execute('EXPLAIN (ANALYZE,TIMING OFF,VERBOSE,COSTS OFF,FORMAT JSON) '+q)
            raw=cursor.fetchone()[0]
        check(label+' raw JSON before duplicate-key decoding',isinstance(raw,str))
        (out/(label+'-explain.json')).write_text(raw+'\n');plan=json.loads(raw,object_pairs_hook=unique_object)
        nodes=[node for node in walk(plan)if'Node Type'in node];scans=[node for node in nodes if'Native Lake'in node]
        check(label+' exactly one retained lake scan',len(scans),1);scan=scans[0]
        check(label+' actual serial correlated subplan remains',any(node.get('Parent Relationship')=='SubPlan'for node in nodes))
        check(label+' no Stream producer',any('Stream'in str(node['Node Type'])for node in nodes),False)
        check(label+' actual foreign iterations',scan.get('Actual Loops'),loops)
        check(label+' real numeric candidate path','scalar Parquet'in scan['Planned File Access'])
        check(label+' no ANN path','ANN Semantics'in scan,False)
        check(label+' original correlated SQL predicate retained',('q.'+('key4'if column=='id'else'key8'))in scan.get('Filter','')or'$'in scan.get('Filter',''))
        check(label+' actual verified scalar files',scan['Verified Scalar File Coverage'],2 if loops else 0)
        check(label+' actual bitmap file backend',scan['Verified Bitmap File Coverage'],2 if loops and args.method=='bitmap'else 0)
        if loops:check(label+' native inline B-tree backend',scan['Actual Inline Access'],'native B-tree')
        result['plans'].append(dict(label=label,sql=q,rows=got,plan=plan));write_json(out/'result.json',result)
    try:
        build=check_build(args.build_receipt);result.update(source_head=build['source_head'],binary_sha256=build['binary_sha256'],build_receipt=str(args.build_receipt.resolve()))
        source=(SOURCE/'contrib/og_lake/lake_inline_scan.cpp').read_text()
        check('qualified serial PARAM_EXEC deferral is present','state->index_pending=!streamScan&&LakeInlineHasExecParam'in source)
        if args.method=='bitmap':check('qualified bitmap dependency identity in build',bool(build.get('roaring_inputs_after')))
        archive=out/'executed-inputs';archive.mkdir();inputs=[Path(__file__),args.build_receipt,D/'native_runtime.py',D/'native_test_context.py',D/'inline_fixture.py',D/'lake_harness.py',D/'source-lock.json',
            D/'tests/file_dml_acceptance.py',D/'tests/native_auto_flush.py',D/'tests/native_table_create.py',D/'tests/permission_api.py',D/'tests/inline_cleanup.py',D/'tests/native_vector_batch_scan.py',D/'tests/catalog_fixture_bundle.py',
            D/'tests/empty_manifest.py',D/'tests/empty_reference_fixture.py',D/'tests/empty_catalog_oracle.py',D/'tests/create_reference_oracle.py',D/'tests/reverse_reference.py',D/'reference.sh',D/'fixture.py',
            ROOT/'.reference/opengauss-native-lake/build/receipt.json',INSTALL/'lib/postgresql/og_lake_sdk/libog_lake_sdk_arrow.so']
        pending=list(inputs);seen=set(inputs)
        while pending:
            p=pending.pop()
            if p.suffix!='.py':continue
            for node in ast.walk(ast.parse(p.read_text())):
                names=[x.name.split('.')[0]for x in node.names]if isinstance(node,ast.Import)else[node.module.split('.')[0]]if isinstance(node,ast.ImportFrom)and node.module else[]
                for name in names:
                    for parent in [D,D/'tests']:
                        q=parent/(name+'.py')
                        if q.is_file()and q not in seen:seen.add(q);pending.append(q)
        inputs=sorted(seen,key=str);result['inputs']={str(p):sha256(p)for p in inputs}
        for i,p in enumerate(inputs):shutil.copy2(p,archive/(str(i)+'-'+p.name))
        result['input_archives']={str(p):str(archive/(str(i)+'-'+p.name))for i,p in enumerate(inputs)}
        for name,digest in build['native_source_inputs_after'].items():
            if Path(name).is_absolute()or'..'in Path(name).parts:raise ValueError('Unsafe native receipt source path')
            p=SOURCE/name;check('exact built source/'+name,sha256(p),digest);target=out/'native-source'/name;target.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(p,target)
        check('archived input bytes match executed identities',all(sha256(Path(result['input_archives'][name]))==digest for name,digest in result['inputs'].items()))
        control=connect_control();session(control)
        check('unrelated automatic maintenance stays off',scalar(control,'SHOW enable_lake_maintenance'),'off')
        check('dedicated scalar backend',scalar(control,'SHOW enable_thread_pool'),'off')
        with(out/'fixture.log').open('w')as log:subprocess.run([sys.executable,str(D/'tests/catalog_fixture_bundle.py'),'--run-id',args.run_id,'--output',str(out/'fixture/manifest.json')],stdout=log,stderr=subprocess.STDOUT,timeout=300,check=True)
        import fcntl
        reservation=(ROOT/'.reference/opengauss-native-lake/.driver.lock').open('a+');fcntl.flock(reservation.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB)
        db=OwnedDatabase(args.run_id,out);db.create();roles=OwnedRoles(control,args.run_id,out)
        case=Case(db,SimpleNamespace(mode='net_dml',sql_version=args.sql_version,manifest=out/'fixture/manifest.json',output=out/'result.json',run_id=args.run_id),result,roles)
        for c in [case.a,case.b,case.observer]:session(c)
        installed=scalar(case.a,"SELECT extversion FROM pg_extension WHERE extname='og_lake'");check('actual installed SQL version',installed,args.sql_version)
        result['installed_sql']=installed;result['runtime_module_version']=json.loads(scalar(case.a,'SELECT og_lake.host_status()'))['module_version']
        runtime_default=reviewed_runtime_version();check('runtime matches exact source control',result['runtime_module_version'],runtime_default)
        check('source runtime supports selected inherited SQL',runtime_default in ['0.19','0.20','0.21'])
        result['runtime_default_version']=runtime_default
        query(case.a,"CREATE FOREIGN TABLE biz.orders(id integer,k bigint,val integer) SERVER created_lake OPTIONS(binding_mode 'create',lake_schema 'main')")
        query(case.a,'CREATE TABLE biz.exact(id integer,k bigint,val integer)');query(case.a,'CREATE TABLE biz.keys(seq integer,key4 integer,key8 bigint)')
        query(case.a,"SELECT og_lake.set_option('biz.orders'::regclass,'data_inlining_row_limit','100000')")
        for lo,hi in [(1,32),(33,64)]:
            both('INSERT INTO {table} SELECT CASE WHEN i%17=0 THEN NULL ELSE i%11 END,CASE WHEN i%17=0 THEN NULL ELSE 10000000000::bigint+(i%11)::bigint*10000000001::bigint END,i FROM generate_series('+str(lo)+','+str(hi)+')i')
            query(case.a,"SELECT og_lake.flush('biz.orders'::regclass)")
        both('INSERT INTO {table} VALUES(5,60000000005,90005),(NULL,NULL,90006),(1000,9223372036854775807,90007)')
        check('two real small immutable files',query(case.a,'SELECT record_count FROM created_metadata.ducklake_data_file WHERE end_snapshot IS NULL ORDER BY data_file_id'),[(32,),(32,)])
        check('complete visible fixture cardinality',scalar(case.a,'SELECT count(*) FROM biz.orders'),67)
        keys=[(5,60000000005),(None,None),(0,10000000000),(10,110000000010),(5,60000000005),(1,20000000001),(1000,9223372036854775807),(2147483647,9223372036854775806)]
        with case.a.cursor()as cursor:cursor.executemany('INSERT INTO biz.keys VALUES(%s,%s,%s)',[(i,*keys[(i-1)%8])for i in range(1,33)])
        check('real independent outer rows',scalar(case.a,'SELECT count(*) FROM biz.keys'),32)
        store=out/'index-store';store.mkdir();query(case.a,"SELECT og_lake.set_option('created_lake'::name,'index_root',%s::text)",(str(store),))
        for column in ['id','k']:
            uuid=str(scalar(case.a,"SELECT og_lake.create_lake_index('biz.orders'::regclass,%s::name,%s::jsonb)",('param_'+column,json.dumps(dict(version=1,kind='scalar',method=args.method,column=column)))))
            query(case.a,'SELECT og_lake.build_lake_index(%s::uuid)',(uuid,))
            check('actual complete file coverage/'+column,scalar(case.a,'SELECT count(*) FROM og_lake_internal.index_coverage WHERE index_uuid=%s::uuid AND active',(uuid,)),2)
        query(case.a,'ANALYZE biz.orders');query(case.a,'ANALYZE biz.keys')
        sources={str(p):sha256(p)for p in Path(case.empty_manifest['data_path']).rglob('*.parquet')};artifacts={str(p):sha256(p)for p in store.rglob('*.parquet')}
        result.update(executed=True,source_files=sources,index_files=artifacts)
        for column in ['id','k']:
            for variant,loops in [('all',32),('null-first',5),('zero',0),('limit',3)]:probe(column,variant,loops)
        check('all immutable data files unchanged',{p:sha256(Path(p))for p in sources},sources)
        check('all derived index files unchanged',{p:sha256(Path(p))for p in artifacts},artifacts)
        check('all executed inputs unchanged',{p:sha256(Path(p))for p in result['inputs']},result['inputs'])
        check('archived complete Native source bytes unchanged',all(sha256(out/'native-source'/name)==digest for name,digest in build['native_source_inputs_after'].items()))
        check_build(args.build_receipt);result['status']='pass'
    except BaseException as exc:result.update(reason=str(exc),traceback=traceback.format_exc())
    finally:
        if db is not None:
            try:db.cleanup();result['database_cleanup']=db.cleanup_result
            except BaseException as exc:result.update(status='fail',database_cleanup_error=str(exc))
        if roles is not None:
            try:
                if not result.get('database_cleanup')or result['database_cleanup']['returncode']!=0:raise RuntimeError('Exact owned database cleanup unproven; retain roles')
                result['role_cleanup']=cleanup_roles(control,roles.path,args.run_id);result=roles.redact(result)
            except BaseException as exc:result.update(status='fail',role_cleanup_error=str(exc))
        if control is not None:control.close()
        if reservation is not None:reservation.close()
        write_json(out/'result.json',result)
    return 0 if result['status']=='pass'else 1


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--method',choices=['btree','bitmap'],required=True);p.add_argument('--execute',action='store_true')
    p.add_argument('--run-id');p.add_argument('--build-receipt',type=Path);p.add_argument('--sql-version',choices=['0.19','0.20','0.21'],default='0.19');p.add_argument('--case-timeout',type=int,default=1200)
    p.add_argument('--_isolated-child',action='store_true',help=argparse.SUPPRESS);a=p.parse_args()
    if not a.execute:print(json.dumps(prepare(a.method,a.sql_version),ensure_ascii=False,indent=2));return 0
    if not a.run_id or not a.build_receipt:p.error('--execute requires run-id and exact official build receipt')
    if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,89}',a.run_id):p.error('Run ID must be safe ASCII and leave fixture suffix space (max90)')
    if not 300<=a.case_timeout<=2400:p.error('Owned whole-case deadline must be300..2400 seconds')
    if a._isolated_child:
        if os.getenv('OG_LAKE_SCALAR_PARAM_CHILD')!=str(os.getppid()):p.error('Unsupervised child refused')
        return execute(a)
    return supervise(a)
if __name__=='__main__':raise SystemExit(main())
