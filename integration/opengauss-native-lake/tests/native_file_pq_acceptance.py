#!/usr/bin/env python3
"""Real Native PQ acceptance; run only after an official PQ-enabled full build.

No ANN global exact-order assertion: returned values/distances use the original
Native vectors, while PQ only scores the candidate frontier. Live-lease cancel
is proven by the diagnostic's exact ungranted advisory gate lock, never sleep.
This file prepares no production readiness or performance/SMP/RAC conclusion.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import math
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time
import traceback
from types import SimpleNamespace
ROOT=Path(os.getenv('CPP_WORKSPACE',str(Path(__file__).resolve().parents[3]))).resolve();D=ROOT/'integration/opengauss-native-lake'
sys.path[:0]=[str(D),str(D/'tests')]
from file_dml_acceptance import check_build
from inline_fixture import query,scalar
from lake_harness import sha256,validate_run_id,write_json
from native_auto_flush import connect_control
from native_runtime import PROFILE,SOURCE
from native_test_context import OwnedDatabase, reviewed_runtime_version
from native_table_create import Case
from permission_api import OwnedRoles,cleanup_roles
from native_pq_oracle import verify,literal,SIDE_FILES


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--run-id',required=True);p.add_argument('--build-receipt',type=Path,required=True)
    p.add_argument('--metric',choices=['l2','ip','cosine'],required=True);p.add_argument('--sources',type=int,choices=[1,4],default=1)
    p.add_argument('--build-timeout-ms',type=int,choices=[120000,300000,900000],default=300000,help='Debug construction only: 300000 per source or explicit 900000 for a maximum-three-source batch; query/live cancellation budgets remain unchanged')
    p.add_argument('--sql-version',choices=['0.17','0.18','0.19','0.20','0.21'],default='0.18')
    p.add_argument('--sdk-index',type=Path,help='Required for source runtime20/21: immutable new-kind SDK component index')
    p.add_argument('--sdk-index-sha256',help='Exact approved new-kind SDK index SHA256')
    a=p.parse_args();validate_run_id(a.run_id)
    if bool(a.sdk_index)!=bool(a.sdk_index_sha256):p.error('sdk-index and exact SHA must be provided together')
    if PROFILE!='ann-debug':p.error('This gate requires the separately owned ann-debug profile')
    out=ROOT/'公共文档/openGauss原生湖存储/验证'/a.run_id;out.mkdir(exist_ok=False)
    r=dict(status='running',scope=__doc__,metric=a.metric,sources=a.sources,build_timeout_ms=a.build_timeout_ms,installed_sql=a.sql_version,assertions=[],native_runtime_executed=False)
    control=db=roles=t=None;source=Path(os.getenv('OG_LAKE_SOURCE',str(ROOT/'.worktrees/opengauss-native-lake')))
    files=[Path(__file__),Path(__file__).with_name('native_pq_oracle.py'),Path(__file__).with_name('native_pq_mutate.cpp'),D/'tests/file_dml_acceptance.py',D/'tests/native_table_create.py',D/'tests/permission_api.py',D/'native_test_context.py',D/'native_runtime.py',D/'tests/inline_cleanup.py',D/'tests/native_auto_flush.py',D/'tests/catalog_fixture_bundle.py',D/'inline_fixture.py',D/'lake_harness.py',D/'fixture.py',D/'tests/empty_manifest.py',D/'tests/empty_catalog_oracle.py',D/'tests/empty_reference_fixture.py',D/'tests/create_reference_oracle.py',D/'tests/reverse_reference.py',D/'reference.sh',D/'source-lock.json',a.build_receipt]
    files += [SOURCE/'contrib/og_lake/sdk/lake_parquet_sdk.h']
    if a.sdk_index:
        files += [a.sdk_index,D/'pq_sql020_build_inputs.py',D/'pq_native_build_inputs.py']
    r['inputs']={str(f):sha256(f)for f in files};archive=out/'executed-inputs';archive.mkdir()
    for i,f in enumerate(files):shutil.copy2(f,archive/(str(i)+'-'+f.name))
    duckdb=ROOT/'.reference/opengauss-native-lake/build/duckdb-gcc10/duckdb'
    provider=ROOT/'.install/opengauss-native-lake-debug/lib/postgresql/og_lake_sdk/libog_lake_sdk_arrow.so'
    r.update(provider_sha256=sha256(provider),duckdb_sha256=sha256(duckdb))
    def image(x):
        # Primitive manifest fields are bytes, including short eight-byte
        # integers. Keep their exact representation JSON-safe in the receipt;
        # comparison still uses the original values above this presentation.
        if isinstance(x,bytes):return dict(length=len(x),hex=x.hex())if len(x)<=30 else dict(length=len(x),sha256=hashlib.sha256(x).hexdigest())
        if isinstance(x,list)and len(x)>30:return dict(length=len(x),sha256=hashlib.sha256(repr(x).encode()).hexdigest())
        if isinstance(x,(list,tuple)):return [image(v)for v in x]
        if isinstance(x,dict):return {k:image(v)for k,v in x.items()}
        return x
    def check(name,got,want=True):
        good=got==want;r['assertions'].append(dict(name=name,observed=image(got),expected=image(want),status='pass'if good else'fail'))
        write_json(out/'result.json',r)
        if not good:raise AssertionError(name+': '+repr(got))
    def command(label,argv,env=None,timeout=120):
        cp=subprocess.run(list(map(str,argv)),text=True,capture_output=True,timeout=timeout,env=env)
        (out/(label+'.stdout')).write_text(cp.stdout);(out/(label+'.stderr')).write_text(cp.stderr)
        if cp.returncode:raise RuntimeError(label+' failed: '+str(cp.returncode))
        return cp.stdout
    def duck(statement,label):
        (out/(label+'.sql')).write_text(statement+'\n')
        text=command(label,[duckdb,'-no-init','-batch','-json','-c','SET threads=1;'+statement]);return json.loads(text)if text.strip()else[]
    def qualified_sdk(build):
        # Reuse the exact new-kind admission implementation: its12 actual SDK
        # cases, full-domain signed16 oracle, all proofs/sources/prefix/license
        # inventories and genuine old-provider negative are mandatory.
        from pq_sql020_build_inputs import sdk_index
        selected=sdk_index(a.sdk_index,a.sdk_index_sha256,ROOT)
        if sha256(provider)!=selected['library_sha256']:
            raise RuntimeError('Installed runtime SDK differs from the qualified new-kind provider')
        header=build['native_source_inputs_after']['contrib/og_lake/sdk/lake_parquet_sdk.h']
        if selected['header_sha256']!=header:
            raise RuntimeError('Qualified SDK header differs from exact Native compiled header')
        pq=build.get('pq_inputs_after',{})
        if pq.get('dependency_kind')!='pq_sql020_int16_v1' or pq.get('files',{}).get('include/sdk/lake_parquet_sdk.h')!=header:
            raise RuntimeError('Qualified private PQ kind/header differs from exact Native/SDK header')
        return dict(component=selected,installed_library_sha256=sha256(provider),header_sha256=header)

    try:
        b=check_build(a.build_receipt);r.update(source_head=b['source_head'],binary_sha256=b['binary_sha256'])
        check('official full build admitted PQ dependency',bool(b.get('pq_inputs_after')))
        check('PQ dependency unchanged through official full',b.get('pq_inputs_before'),b.get('pq_inputs_after'))
        runtime_default=reviewed_runtime_version()
        if a.sdk_index:
            r['qualified_sdk']=qualified_sdk(b)
        else:
            check('legacy SDK receipt is restricted to source runtime18/19',runtime_default in ['0.18','0.19'])
            check('installed real historical SDK5ae',r['provider_sha256'],'5ae9f8fecba4b0565b414e766cfad4dbc62e7f8714326f7610c8a2e51e3c083a')
        control=connect_control();query(control,'SET session_timeout=0');check('automatic maintenance off',scalar(control,'SHOW enable_lake_maintenance'),'off')
        with(out/'fixture.log').open('w')as log:subprocess.run([sys.executable,str(D/'tests/catalog_fixture_bundle.py'),'--run-id',a.run_id,'--output',str(out/'fixture/manifest.json')],stdout=log,stderr=subprocess.STDOUT,check=True,timeout=300)
        # Retain the owned receipt object even if post-CREATE validation fails.
        db=OwnedDatabase(a.run_id,out);db.create();roles=OwnedRoles(control,a.run_id,out)
        t=Case(db,SimpleNamespace(mode='net_dml',sql_version=a.sql_version,manifest=out/'fixture/manifest.json',output=out/'result.json',run_id=a.run_id),r,roles);c=t.a;r['native_runtime_executed']=True
        runtime_defaults=re.findall(r"^default_version\s*=\s*'([^']+)'",(SOURCE/'contrib/og_lake/og_lake.control').read_text(),re.M)
        check('one exact source runtime default',len(runtime_defaults),1)
        runtime_module=json.loads(scalar(c,'SELECT og_lake.host_status()'))['module_version']
        installed_sql=scalar(c,"SELECT extversion FROM pg_extension WHERE extname='og_lake'")
        check('actual module matches exact built source control',runtime_module,runtime_defaults[0])
        check('actual installed SQL matches explicit PQ selection',installed_sql,a.sql_version)
        check('reviewed inherited PQ source runtime',runtime_defaults[0] in ['0.18','0.19','0.20','0.21'])
        r.update(runtime_default_version=runtime_defaults[0],runtime_module_version=runtime_module,installed_sql=installed_sql)
        for conn in [control,db.admin,c,t.b,t.observer]:query(conn,'SET session_timeout=0')
        check('long-running owned sessions disable only idle expiry',[scalar(conn,"SELECT setting::bigint FROM pg_settings WHERE name='session_timeout'")for conn in [control,db.admin,c,t.b,t.observer]],[0]*5)
        for conn in [c,t.b,t.observer]:query(conn,'SET statement_timeout=120000')
        query(c,"CREATE FUNCTION biz.pq_probe(integer,bigint,bigint) RETURNS text AS '$libdir/og_lake','og_lake_test_pq_probe' LANGUAGE C VOLATILE NOT FENCED")
        query(c,'REVOKE ALL ON FUNCTION biz.pq_probe(integer,bigint,bigint) FROM PUBLIC')
        query(c,'CREATE TABLE biz.exact(id integer PRIMARY KEY,v vector(16),note text)')
        query(c,"CREATE FOREIGN TABLE biz.vectors(id integer,v vector(16),note text) SERVER created_lake OPTIONS(binding_mode 'create',lake_schema 'main')")
        query(c,"SELECT og_lake.set_option('biz.vectors'::regclass,'data_inlining_row_limit','50000')")
        def vector(i):return '['+','.join(format((((i*131+j*79+((i*(j+1))%29)*17)%2039)-1019)/1024.0,'.9g')for j in range(16))+']'
        for f in range(a.sources):
            rows=','.join('('+str(i)+','+literal(vector(i))+'::vector,'+literal('row/'+str(i))+')'for i in range(f*512+1,(f+1)*512+1))
            for target in ['biz.exact','biz.vectors']:query(c,'INSERT INTO '+target+' VALUES '+rows)
            query(c,"SELECT og_lake.flush('biz.vectors'::regclass)")
        check('effective source vector population',scalar(c,'SELECT count(DISTINCT encode(vector_send(v),\'hex\')) FROM biz.vectors'),512*a.sources)
        spec=dict(version=1,kind='vector',method='diskann',column='v',metric=a.metric,pq=True,pq_chunks=4)
        uuid=str(scalar(c,"SELECT og_lake.create_lake_index('biz.vectors'::regclass,'ix_pq',%s::jsonb)",(json.dumps(spec),)))
        store=out/'index-store';store.mkdir();query(c,"SELECT og_lake.set_option('created_lake'::name,'index_root',%s::text)",(str(store),))
        original_head=t.head();original_protocol=t.protocol()
        def build():
            event=dict(at=time.monotonic(),state='started');r.setdefault('build_events',[]).append(event);write_json(out/'result.json',r)
            query(c,'SET statement_timeout='+str(a.build_timeout_ms))
            try:
                reply=query(c,'SELECT og_lake.build_lake_index(%s::uuid)',(uuid,))
            except BaseException as exc:
                event.update(state='error',sqlstate=getattr(exc,'pgcode',None),elapsed=time.monotonic()-event['at']);write_json(out/'result.json',r)
                # An aborted explicit transaction cannot execute a restoring
                # SET. Preserve its original error; owned cleanup rolls it back.
                raise
            query(c,'SET statement_timeout=120000')
            event.update(state='completed',elapsed=time.monotonic()-event['at']);write_json(out/'result.json',r)
            return reply
        def all_segments(conn=t.observer):return query(conn,'SELECT segment_uuid::text,state,artifact_root_uri,manifest_hash,format_version FROM og_lake_internal.index_segment WHERE index_uuid=%s::uuid ORDER BY segment_uuid',(uuid,))
        def active(conn=t.observer):
            rows=query(conn,"SELECT s.segment_uuid::text,s.artifact_root_uri,s.manifest_hash,s.format_version,s.build_token::text,c.source_file_id,c.schema_mapping_hash,s.source_manifest_hash,d.definition_generation,d.definition_hash,c.physical_row_count FROM og_lake_internal.index_definition d JOIN og_lake_internal.index_segment s USING(index_uuid,definition_generation) JOIN og_lake_internal.index_coverage c USING(segment_uuid,index_uuid,definition_generation) WHERE d.index_uuid=%s::uuid AND s.state='READY' AND c.active ORDER BY c.source_file_id",(uuid,))
            keys=['segment','directory','manifest','format','token','file_id','schema_hash','source_hash','generation','definition_hash','physical_rows']
            return [dict(zip(keys,row),uuid=uuid,metric={'l2':1,'ip':2,'cosine':3}[a.metric])for row in rows]
        # Each invocation is its own host transaction. A PQ build may register
        # at most3 complete5-file source candidates; do not call twice inside
        # the same transaction and manufacture the16-candidate limit error.
        for child in [False,True]:
            query(c,'BEGIN')
            if child:query(c,'SAVEPOINT pq_build')
            build();pending=all_segments(c);paths=[Path(row[2])/name for row in pending for name in ['ann_nodes.parquet','ann_lookup.parquet']+SIDE_FILES]
            check('all five candidate files durably finished before '+('subabort'if child else'abort'),bool(paths)and all(x.is_file()for x in paths))
            check('observer cannot see pending READY',all_segments(),[])
            if child:query(c,'ROLLBACK TO pq_build');query(c,'RELEASE SAVEPOINT pq_build');query(c,'INSERT INTO biz.heap_marker VALUES(17)');query(c,'COMMIT')
            else:query(c,'ROLLBACK')
            check('rollback aborts every graph/PQ candidate',all(not x.exists()for x in paths));check('rollback removes graph/PQ catalog rows',all_segments(),[])
        for attempt in range(a.sources+1):
            if len(active())==a.sources:break
            before=len(active());build();after=len(active());check('bounded build makes source progress '+str(attempt),before<after<=min(before+3,a.sources))
        current=active();check('all requested sources covered over committed batches',len(current),a.sources);check('every committed source is actual format2',all(x['format']==2 for x in current))
        r['initial_coverage']=current;r['independent_oracles']=[verify(Path(x['directory']),x,duck,check,'oracle-'+str(i))for i,x in enumerate(current)]
        def stats(mode=0,file=0,gate=0):return json.loads(scalar(c,'SELECT biz.pq_probe(%s,%s,%s)',(mode,file,gate)))
        def balanced(label):
            s=stats();check(label+' zero active/violation',(s['active'],s['violations']),(0,0));check(label+' exact once close and destruction',(s['opened'],s['closed'],s['destroyed']),(s['opened'],s['opened'],s['opened']));return s
        query(c,'SET enable_lake_ann=on');query(c,'SET enable_seqscan=off');query(c,'SET client_min_messages=debug1')
        op={'l2':'<->','ip':'<#>','cosine':'<=>'}[a.metric];q=vector(47)
        statement='SELECT id,encode(vector_send(v),\'hex\'),note,v '+op+' %s::vector AS d FROM biz.vectors WHERE v IS NOT NULL'+(' AND vector_norm(v)>0::double precision'if a.metric=='cosine'else'')+' ORDER BY d LIMIT 10'
        def returned(label):
            actual=query(c,statement,(q,));check(label+' query emitted requested candidate rows',len(actual),10)
            for row in actual:
                expected=query(c,'SELECT id,encode(vector_send(v),\'hex\'),note,v '+op+' %s::vector FROM biz.exact WHERE id=%s',(q,row[0]))
                check(label+' returned original row and exact Native SQL distance/'+str(row[0]),[row],expected)
            return actual
        def plan(label,want_pq=a.sources,want_ann=a.sources):
            text='\n'.join(x[0]for x in query(c,'EXPLAIN (ANALYZE,COSTS OFF) '+statement,(q,)));(out/(label+'-plan.txt')).write_text(text+'\n')
            def count(name):
                m=re.search(re.escape(name)+r': (\d+)',text);return int(m.group(1))if m else-1
            check(label+' actual verified PQ file coverage',count('Verified PQ File Coverage'),want_pq)
            check(label+' actual verified raw/quantized ANN coverage',count('Verified ANN File Coverage'),want_ann)
            check(label+' original-vector SQL rerank remains visible','ANN Semantics:'in text and 'Sort'in text);return text
        stats(-1);returned('initial PQ');plan('initial PQ');s=balanced('normal query boundaries');check('qualified PQ really opened',s['opened']>=2*a.sources);check('actual EntryDistance calls succeeded through PQ LUT',s['distances']>0)
        stats(-1);null_rows=query(c,statement,(None,));check('NULL distance argument preserves requested SQL population',len(null_rows),10);check('NULL distance argument emits Native NULL distances',all(x[3]is None for x in null_rows));s=balanced('NULL argument');check('NULL argument never opens a PQ model',s['opened'],0)
        baseline=all_segments();build();check('verified repair/build is idempotent',all_segments(),baseline)
        # Deterministic lifecycle fault points occur with a successfully opened
        # primitive model/codes/LUT. Plain pg_sleep or upper executor errors
        # would only test cleanup after the file lease had already closed.
        target=current[0]['file_id'];stats(-1);stats(2,target)
        try:query(c,statement,(q,))
        except Exception as exc:check('ERROR injected inside live frontier',getattr(exc,'pgcode',None),'XX000')
        else:check('live-frontier ERROR observed',False)
        s=balanced('live-frontier ERROR');check('real context-first release ran',s['context'],1)
        stats(-1);stats(3,target);query(c,'BEGIN');query(c,'SAVEPOINT outer_pq');returned('real internal subcommit');query(c,'RELEASE SAVEPOINT outer_pq');query(c,'COMMIT');s=balanced('real internal subcommit');check('native SUBCOMMIT and reparent observed',(s['subcommit'],s['reparent']),(1,1))
        stats(-1);stats(5,target);returned('native dedicated owner-first release');s=balanced('owner-first release');check('native BEFORE_LOCKS owner callback ran',s['owner'],1)
        stats(-1);stats(6,target);returned('partial frontier failure');s=balanced('partial frontier restart');check('injected third node distance forced raw restart',s['restart'],1)
        # Only one selected source is forced raw; remaining files still use PQ.
        stats(-1);stats(6,target);plan('partial-frontier-restart',a.sources-1,a.sources);balanced('restart EXPLAIN')
        stats(-1);key=(1<<43)+int(hashlib.sha256(a.run_id.encode()).hexdigest()[:9],16);pid=scalar(c,'SELECT pg_backend_pid()');dbid=scalar(c,'SELECT oid FROM pg_database WHERE datname=current_database()')
        query(t.b,'SELECT pg_advisory_lock(%s::bigint)',(key,));future=None;pool=ThreadPoolExecutor(max_workers=1)
        try:
            stats(1,target,key);future=pool.submit(query,c,statement,(q,));deadline=time.monotonic()+30;locks=[]
            while time.monotonic()<deadline and not future.done():
                locks=query(t.observer,"SELECT pid,granted FROM pg_locks WHERE locktype='advisory' AND pid=%s AND database=%s AND classid=%s::oid AND objid=%s::oid AND objsubid=1 AND mode='ExclusiveLock' AND NOT granted",(pid,dbid,key>>32,key&0xffffffff))
                if locks:break
                time.sleep(.01)
            check('observer proves exact lease-held cancellation gate',locks,[(pid,False)])
            check('cancel sent only while frontier gate still blocked',not future.done())
            check('real target backend cancellation accepted',scalar(t.observer,'SELECT pg_cancel_backend(%s)',(pid,)))
            try:future.result(timeout=30)
            except Exception as exc:check('lease-held cancel SQLSTATE',getattr(exc,'pgcode',None),'57014')
            else:check('lease-held cancel observed',False)
        finally:
            # Release the gate BEFORE joining the executor, including failed
            # observer assertions; otherwise a negative gate could deadlock.
            if future is not None and not future.done():scalar(t.observer,'SELECT pg_cancel_backend(%s)',(pid,))
            check('owned cancellation gate released',scalar(t.b,'SELECT pg_advisory_unlock(%s::bigint)',(key,)))
            if future is not None:
                try:future.result(timeout=30)
                except Exception:pass
            pool.shutdown(wait=True)
        s=balanced('live-frontier cancellation');check('cancel closes the actually opened lease',s['opened']>=1);check('cancel uses real cleanup callback',s['context']+s['owner'],1)
        returned('same backend after cancel');balanced('same backend reuse')
        # A standalone SDK tool produces legal, identical required schemas.
        # Compilation is jobs1 and occurs only when this explicit gate runs.
        gcc=ROOT/'binarylibs/buildtools/gcc10.3';ssl=ROOT/'binarylibs/kernel/dependency/openssl/comm';env=dict(os.environ)
        env['LD_LIBRARY_PATH']=':'.join([str(ssl/'lib')]+[str(gcc/x)for x in ['gcc/lib64','isl/lib','mpc/lib','mpfr/lib','gmp/lib']]);env['OMP_NUM_THREADS']=env['OPENBLAS_NUM_THREADS']='1'
        mutate=out/'native-pq-mutate';command('compile-real-sdk-mutator',[gcc/'gcc/bin/g++','-std=c++14','-O0','-g','-I',source/'contrib/og_lake',Path(__file__).with_name('native_pq_mutate.cpp'),'-ldl','-o',mutate],env=env)
        for kind,name in enumerate(SIDE_FILES,1):
            for damage in ['missing','legal-schema-content']:
                old=active()[0];directory=Path(old['directory']);bad=directory/name;label=str(kind)+'-'+damage;backup=out/(label+'-original.parquet');shutil.copy2(bad,backup)
                if damage=='missing':bad.unlink()
                else:
                    changed=out/(label+'-changed.parquet');command('mutate-'+label,[mutate,provider,bad,changed,kind],env=env)
                    schema=lambda x:duck('SELECT name,type,repetition_type,field_id FROM parquet_schema('+literal(x)+')','schema-'+label+'-'+x.stem)
                    check('valid required schema unchanged/'+label,schema(changed),schema(bad));os.replace(changed,bad)
                returned('derived '+label+' raw graph fallback');plan('fallback-'+label,a.sources-1,a.sources);balanced('damaged sidecar '+label)
                build();fresh=next(x for x in active()if x['file_id']==old['file_id']);check('repair creates new immutable segment/'+label,fresh['segment']!=old['segment']);check('repair retires damaged generation/'+label,next(x[1]for x in all_segments()if x[0]==old['segment']),'RETIRED')
                check('repair retains physical source coverage/'+label,(fresh['source_hash'],fresh['schema_hash'],fresh['physical_rows']),(old['source_hash'],old['schema_hash'],old['physical_rows']))
                r['independent_oracles'].append(verify(Path(fresh['directory']),fresh,duck,check,'repaired-'+label));plan('repaired-'+label)
        check('PQ build/repair/query allocate no business snapshot',t.head(),original_head)
        check('PQ derived operations preserve complete DuckLake standard protocol',t.protocol(),original_protocol)
        # The separately indexed small source explicitly keeps the original
        # graph protocol, rather than claiming an untrained PQ cache.
        query(c,"CREATE FOREIGN TABLE biz.small(id integer,v vector(16)) SERVER created_lake OPTIONS(binding_mode 'create',lake_schema 'main')")
        query(c,'INSERT INTO biz.small VALUES '+','.join('('+str(i)+','+literal(vector(i))+'::vector)'for i in range(1,17)));query(c,"SELECT og_lake.flush('biz.small'::regclass)")
        small=str(scalar(c,"SELECT og_lake.create_lake_index('biz.small'::regclass,'ix_small',%s::jsonb)",(json.dumps(spec),)));query(c,'SELECT og_lake.build_lake_index(%s::uuid)',(small,))
        small_rows=query(c,"SELECT format_version,manifest_hash,artifact_root_uri FROM og_lake_internal.index_segment WHERE index_uuid=%s::uuid AND state='READY'",(small,));check('small trained population uses actual raw format1',len(small_rows)==1 and small_rows[0][0]==1 and bool(re.fullmatch('[0-9a-f]{64}',small_rows[0][1])))
        check('small raw fallback creates no fake PQ sidecars',all(not(Path(small_rows[0][2])/x).exists()for x in SIDE_FILES))
        small_status=json.loads(scalar(c,'SELECT og_lake.lake_index_status(%s::uuid)',(small,)));check('small status distinguishes request from actual PQ',(small_status['pq_requested'],small_status['pq_ready_segments']),(True,0))
        check('business vectors unchanged',scalar(c,'SELECT count(*) FROM biz.vectors'),512*a.sources);check('provider bytes unchanged',sha256(provider),r['provider_sha256']);check('executed input identities frozen',all(sha256(f)==h for f,h in r['inputs'].items()));check('final installed PQ SQL version',scalar(c,"SELECT extversion FROM pg_extension WHERE extname='og_lake'"),a.sql_version);check_build(a.build_receipt)
        if a.sdk_index:check('qualified SDK identity remains exact',qualified_sdk(b),r['qualified_sdk'])
        r['status']='pass'
    except BaseException as exc:r.update(status='fail',reason=str(exc),traceback=traceback.format_exc())
    finally:
        if db is not None:
            try:db.cleanup();r['database_cleanup']=db.cleanup_result
            except BaseException as exc:r.update(status='fail',cleanup_error=str(exc))
        if roles is not None:
            try:
                if not r.get('database_cleanup')or r['database_cleanup']['returncode']:raise RuntimeError('Owned DB cleanup unproven; roles retained')
                r['role_cleanup']=cleanup_roles(control,roles.path,a.run_id);r=roles.redact(r)
            except BaseException as exc:r.update(status='fail',role_cleanup_error=str(exc))
        if control is not None:control.close()
        write_json(out/'result.json',r)
    print(json.dumps(dict(status=r['status'],checks=len(r['assertions']),reason=r.get('reason'),result=str(out/'result.json')),ensure_ascii=False));return 0 if r['status']=='pass'else 1
if __name__=='__main__':raise SystemExit(main())
