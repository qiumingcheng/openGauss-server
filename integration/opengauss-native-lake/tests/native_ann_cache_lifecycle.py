#!/usr/bin/env python3
"""Prepared actual Native cache lifetime QA; no action without --execute.
Ordinary SQL proves cursor/savepoint/budget behavior. Private modes10..12 alone
exercise real live-cache Concat, never the old diagnostic frontier path.
Requires an exact future build/deployment and root-owned serial hard supervisor.
"""
import argparse
from copy import deepcopy
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
from types import SimpleNamespace
ROOT=Path(os.environ.get('CPP_WORKSPACE','/home/qiumc/data/code/cpp')).resolve()
D=ROOT/'integration/opengauss-native-lake'
MODES=['cursor','subxact','multi-budget','concat','concat-error','concat-cancel','target-miss','abort-before-load']


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--execute',action='store_true');p.add_argument('--mode',choices=MODES,required=True)
    p.add_argument('--run-id');p.add_argument('--build-receipt',type=Path)
    p.add_argument('--sql-version',choices=['0.19','0.20','0.21'],default='0.19')
    a=p.parse_args()
    if not a.execute:
        print(json.dumps(dict(status='prepared_not_executed',mode=a.mode,sql=a.sql_version,
            files=2,rows_per_file=512,dimension=16,cache_cap_bytes=32*1024*1024,
            native_calls=0,fixtures=0,builds=0,private_modes={'concat':10,'concat-error':11,'concat-cancel':12,'target-miss':10,'abort-before-load':13}.get(a.mode),
            prerequisite='Exact future official cache+diagnostic build, matching ann-debug deployment, dedicated backends,maintenance off,qualified PQ SDK/archive,root serial fixture ownership/hard1200s controller.')))
        return 0
    if not a.run_id or a.build_receipt is None:p.error('execute requires run-id/build-receipt')
    sys.path[:0]=[str(D),str(D/'tests')]
    from file_dml_acceptance import check_build
    from inline_fixture import query,scalar
    from lake_harness import sha256,validate_run_id,write_json
    from native_auto_flush import connect_control
    from native_runtime import PROFILE,SOURCE,INSTALL
    from native_test_context import OwnedDatabase
    from native_table_create import Case
    from permission_api import OwnedRoles,cleanup_roles
    validate_run_id(a.run_id)
    if PROFILE!='ann-debug':p.error('Requires exact separately owned ann-debug catalogs')
    out=ROOT/'公共文档/openGauss原生湖存储/验证'/a.run_id;out.mkdir(exist_ok=False)
    result=dict(status='fail',mode=a.mode,assertions=[],sql_version=a.sql_version,scope=__doc__)
    control=db=roles=t=None;worker=None;gate_held=False
    def check(name,got,want=True):
        ok=got==want;result['assertions'].append(deepcopy(dict(name=name,observed=got,expected=want,status='pass'if ok else'fail')))
        write_json(out/'result.json',result)
        if not ok:raise AssertionError(name)
    def setup_session(c):
        for sql in ['SET session_timeout=0','SET statement_timeout=600000','SET query_dop=1','SET enable_lake_smp=off','SET enable_vector_engine=off','SET enable_default_ustore_table=off','SET enable_lake_ann=on','SET enable_seqscan=off']:
            query(c,sql)
    try:
        build=check_build(a.build_receipt);result.update(build=str(a.build_receipt.resolve()),build_sha256=sha256(a.build_receipt),binary_sha256=build['binary_sha256'])
        control=connect_control();setup_session(control)
        check('maintenance stays off',scalar(control,'SHOW enable_lake_maintenance'),'off')
        check('dedicated backend budget scope',scalar(control,'SHOW enable_thread_pool'),'off')
        files=[Path(__file__),a.build_receipt,D/'native_test_context.py',D/'native_runtime.py',D/'inline_fixture.py',D/'lake_harness.py',D/'tests/file_dml_acceptance.py',D/'tests/native_table_create.py',D/'tests/permission_api.py',D/'tests/inline_cleanup.py',D/'tests/native_auto_flush.py',D/'tests/catalog_fixture_bundle.py',D/'tests/empty_reference_fixture.py',D/'tests/empty_manifest.py',D/'tests/empty_catalog_oracle.py',D/'tests/create_reference_oracle.py',D/'tests/reverse_reference.py',D/'fixture.py',D/'reference.sh',D/'source-lock.json']
        files += [SOURCE/name for name in build['native_source_inputs_after']]
        files += [Path(name)for name in build['build_wrapper_inputs_after']]
        sdk=INSTALL/'lib/postgresql/og_lake_sdk';files +=[p for p in sdk.rglob('*')if p.is_file()]
        files +=list((INSTALL/'share/postgresql/extension').glob('og_lake*.sql'))
        reference_receipt=ROOT/'.reference/opengauss-native-lake/build/receipt.json';rr=json.loads(reference_receipt.read_text());reference=Path(rr['binary'])
        check('fixed independent reference binary',sha256(reference),rr['binary_sha256']);files +=[reference_receipt,reference,INSTALL/'bin/gaussdb']
        archive=out/'executed-inputs';archive.mkdir();result['inputs']={}
        for n,path in enumerate(dict.fromkeys(p.resolve()for p in files)):
            destination=archive/(str(n)+'-'+path.name);shutil.copy2(path,destination)
            result['inputs'][str(path)]=dict(sha256=sha256(path),archive=str(destination))
        with(out/'fixture.log').open('w')as log:
            subprocess.run([sys.executable,D/'tests/catalog_fixture_bundle.py','--run-id',a.run_id,'--output',out/'fixture/manifest.json'],stdout=log,stderr=subprocess.STDOUT,check=True,timeout=300)
        db=OwnedDatabase(a.run_id,out);db.create();setup_session(db.admin)
        roles=OwnedRoles(control,a.run_id,out)
        t=Case(db,SimpleNamespace(mode='net_dml',sql_version=a.sql_version,manifest=out/'fixture/manifest.json',output=out/'result.json',run_id=a.run_id),result,roles)
        for c in [t.a,t.b,t.observer]:setup_session(c)
        c=t.a
        query(c,'CREATE TABLE biz.exact(id integer,v vector(16),note text)')
        query(c,"CREATE FOREIGN TABLE biz.vectors(id integer,v vector(16),note text) SERVER created_lake OPTIONS(binding_mode 'create',lake_schema 'main')")
        query(c,"SELECT og_lake.set_option('biz.vectors'::regclass,'data_inlining_row_limit','50000')")
        terms=['(sin(i*.37+'+str(j*.19)+')*(1+i/512.0)+cos(i*.017+'+str(j*.071)+'))::text'for j in range(16)]
        payload="('['||"+"||','||".join(terms)+"||']')::vector"
        for first,last in [(1,512),(513,1024)]:
            query(c,'BEGIN')
            for table in ['biz.exact','biz.vectors']:
                query(c,"INSERT INTO "+table+" SELECT i,"+payload+",'row/'||lpad(i::text,4,'0') FROM generate_series("+str(first)+','+str(last)+')i')
            query(c,'COMMIT');query(c,"SELECT og_lake.flush('biz.vectors'::regclass)")
        store=out/'index-store';store.mkdir();query(c,"SELECT og_lake.set_option('created_lake'::name,'index_root',%s::text)",(str(store),))
        spec=dict(version=1,kind='vector',method='diskann',column='v',metric='l2',pq=True,pq_chunks=4)
        index=str(scalar(c,"SELECT og_lake.create_lake_index('biz.vectors'::regclass,'ix_lifetime',%s::jsonb)",(json.dumps(spec),)))
        query(c,'SELECT og_lake.build_lake_index(%s::uuid)',(index,))
        coverage=query(c,'SELECT c.source_file_id,c.physical_row_count FROM og_lake_internal.index_coverage c JOIN og_lake_internal.index_segment s USING(segment_uuid,index_uuid,definition_generation) WHERE c.index_uuid=%s::uuid AND c.active AND s.state=\'READY\' ORDER BY c.source_file_id',(index,))
        check('two actual512row sources', [x[1]for x in coverage],[512,512])
        check('both sources have real format2',query(c,'SELECT DISTINCT format_version FROM og_lake_internal.index_segment WHERE index_uuid=%s::uuid',(index,)),[(2,)])
        manifests=query(c,'SELECT manifest_hash FROM og_lake_internal.index_segment WHERE index_uuid=%s::uuid ORDER BY segment_uuid',(index,))
        check('actual PQ descriptors retain requested four chunks',[row[0].split('|')[3]for row in manifests],['4','4'])
        adapter_lower=8*1024*1024+256*4*16+4*1024*4
        result['budget_lower_bound']=dict(adapter_per_entry=adapter_lower,four_entries_minimum=4*adapter_lower,limit=32*1024*1024)
        check('four real model adapters alone cannot fit the global cache',4*adapter_lower>32*1024*1024)
        query(c,'CREATE TABLE biz.cache_keys(seq integer,q1 vector(16),q2 vector(16),q3 vector(16))')
        vectors=['['+','.join(str((i+1)*.137+j*.071)for i in range(16))+']'for j in range(3)]
        query(c,'INSERT INTO biz.cache_keys SELECT i,%s::vector,%s::vector,%s::vector FROM generate_series(1,8)i',tuple(vectors))
        query(c,'CREATE INDEX cache_keys_seq ON biz.cache_keys(seq)')
        query(c,"CREATE FUNCTION biz.cache_probe(integer,bigint,bigint) RETURNS text AS '$libdir/og_lake','og_lake_test_pq_probe' LANGUAGE C VOLATILE NOT FENCED")
        query(c,'REVOKE ALL ON FUNCTION biz.cache_probe(integer,bigint,bigint) FROM PUBLIC')
        def probe(mode=0,gate=0):return json.loads(scalar(c,'SELECT biz.cache_probe(%s,%s,%s)',(mode,coverage[0][0],gate)))
        def cache():return probe()['ann_rescan_cache']
        def clean(label):
            value=cache();check(label+' no live images/retained bytes',(value['live'],value['bytes']),(0,0));check(label+' opens/closes balance',value['opened'],value['closed']);return value
        def select(table='biz.vectors',count=1):
            parts=['q.seq']+["(SELECT note FROM "+table+" WHERE v IS NOT NULL AND id>=q.seq ORDER BY v <-> q.q"+str(i)+" LIMIT 1)"for i in range(1,count+1)]
            return 'SELECT '+','.join(parts)+' FROM (SELECT * FROM biz.cache_keys ORDER BY seq OFFSET 0)q ORDER BY q.seq'
        single=select();want=query(t.observer,select('biz.exact'))
        text='\n'.join(x[0]for x in query(c,'EXPLAIN (COSTS OFF) '+single));(out/'plan.txt').write_text(text+'\n')
        encoded=query(c,'EXPLAIN (FORMAT JSON,COSTS OFF) '+single)[0][0]
        document=json.loads(encoded)if isinstance(encoded,str)else encoded
        write_json(out/'stream-plan.json',document)
        main_chain=[]
        def main_nodes(plan):
            main_chain.append(plan)
            for child in plan.get('Plans',[]):
                if child.get('Parent Relationship')not in ['SubPlan','InitPlan'] and 'Subplan Name'not in child:
                    main_nodes(child)
        main_nodes(document[0]['Plan'])
        check('outer cursor path has no blocking materializer', [x['Node Type']for x in main_chain if x['Node Type']in ['Sort','Materialize','Hash','HashAggregate','Aggregate']], [])
        check('outer keys stream in original seq index order', any(x.get('Index Name')=='cache_keys_seq' and x.get('Node Type')in ['Index Scan','Index Only Scan']for x in main_chain))
        check('actual correlated plan contains native ANN subplan','SubPlan'in text and 'Planned File Access: DiskANN Parquet candidates'in text)
        probe(-1)
        if a.mode=='cursor':
            query(c,'BEGIN');query(c,'DECLARE cache_cursor NO SCROLL CURSOR FOR '+single)
            check('first FETCH exact heap row',query(c,'FETCH 1 FROM cache_cursor'),want[:1]);v1=cache();check('partial cursor retains immutable images',v1['live']>0 and v1['bytes']>0 and v1['loads']>0);check('FETCH1 executes exactly one two-file parameter epoch',v1['queries'],2)
            check('continued FETCH exact heap rows',query(c,'FETCH 2 FROM cache_cursor'),want[1:3]);v2=cache();check('FETCH reuses statement images',v2['hits']>v1['hits']);check('next FETCH2 executes exactly two more epochs',v2['queries'],6)
            query(c,'CLOSE cache_cursor');clean('CLOSE');query(c,'COMMIT')
            probe(-1);query(c,'BEGIN');query(c,'DECLARE held_cache NO SCROLL CURSOR WITH HOLD FOR '+single);check('held first FETCH',query(c,'FETCH 1 FROM held_cache'),want[:1]);check('held cursor really loads cache',cache()['loads']>0)
            query(c,'COMMIT');clean('WITH HOLD materialization COMMIT');check('held rows survive commit independently of released cache',query(c,'FETCH ALL FROM held_cache'),want[1:]);query(c,'CLOSE held_cache')
        elif a.mode=='subxact':
            query(c,'BEGIN');query(c,'SAVEPOINT opened_here');query(c,'DECLARE release_cache NO SCROLL CURSOR FOR '+single);check('subxact first FETCH',query(c,'FETCH 1 FROM release_cache'),want[:1]);v1=cache()
            query(c,'RELEASE opened_here');v2=cache();check('portal survives real ResourceOwnerNewParent',v2['live'],v1['live']);check('RELEASE keeps charged images',v2['bytes'],v1['bytes']);check('post RELEASE FETCH exact',query(c,'FETCH 1 FROM release_cache'),want[1:2]);check('post RELEASE still actual cache hit',cache()['hits']>v2['hits']);query(c,'CLOSE release_cache');clean('RELEASE child CLOSE');query(c,'COMMIT')
            probe(-1);query(c,'BEGIN');query(c,'DECLARE outer_cache NO SCROLL CURSOR FOR '+single);check('outer first FETCH',query(c,'FETCH 1 FROM outer_cache'),want[:1]);outer=cache()
            query(c,'SAVEPOINT child');query(c,'DECLARE child_cache NO SCROLL CURSOR FOR '+single);check('child first FETCH',query(c,'FETCH 1 FROM child_cache'),want[:1]);child=cache();check('child has a separate actual cache',child['live']>outer['live'])
            query(c,'ROLLBACK TO child');after=cache();check('rollback releases only child portal cache',(after['live'],after['bytes']),(outer['live'],outer['bytes']));check('child portal really removed',scalar(c,"SELECT count(*) FROM pg_cursors WHERE name='child_cache'"),0);check('preexisting outer FETCH survives child rollback',query(c,'FETCH 1 FROM outer_cache'),want[1:2]);query(c,'CLOSE outer_cache');query(c,'RELEASE child');clean('ROLLBACK child/outer CLOSE');query(c,'COMMIT')
        elif a.mode=='multi-budget':
            statement=select(count=3);expected=query(t.observer,select('biz.exact',3));text='\n'.join(x[0]for x in query(c,'EXPLAIN (COSTS OFF) '+statement));(out/'multi-plan.txt').write_text(text+'\n');check('three actual foreign scans, not SQL client concurrency',text.count('Foreign Scan'),3)
            probe(-1);check('all three correlated sources keep exact heap results',query(c,statement),expected);v=clean('multi reader End');result['multi_probe']=probe();check('all three ForeignScans actually own separate statement caches',v['opened'],3);check('real model images loaded and reused',v['loads']>0 and v['hits']>0);check('global retention across all three scans bounded',3*adapter_lower<=v['peak_bytes']<=32*1024*1024);check('actual cache capacity rejection, not another admission gate',v['budget_bypasses']>0);check('budget bypass actually runs original complete verified frontier path',result['multi_probe']['opened']>0);check('original fallback leases close',result['multi_probe']['active'],0)
        elif a.mode=='target-miss':
            missing=max(row[0]for row in coverage)+1
            query(c,'SELECT biz.cache_probe(10,%s,0)',(missing,))
            check('unmatched diagnostic source keeps exact heap results',query(c,single),want)
            v=clean('unmatched target End');raw=probe();result['target_miss_probe']=raw
            check('unmatched target still ran real cached images',v['loads']>0)
            check('unmatched target never transferred a private owner',v['reparent'],0)
            check('unmatched target remains genuinely unconsumed',raw['armed'],10)
            probe(-1);check('unmatched diagnostic can disarm after safe release',probe()['armed'],0)
        elif a.mode=='abort-before-load':
            probe(13)
            try:query(c,single)
            except BaseException as exc:
                check('controlled error before immutable image load',getattr(exc,'pgcode',None),'XX000')
                check('exact live-cache preload fault reached',getattr(getattr(exc,'diag',None),'message_primary',None),
                      'controlled live-cache error before immutable image load')
            else:check('preload error must be observed',False)
            v=clean('preload abort');raw=probe();result['preload_probe']=raw
            check('preload abort registered actual cache owner',v['opened']>0)
            check('preload abort did not load an immutable image',v['loads'],0)
            check('preload abort never reached Concat',v['reparent'],0)
            check('preload abort consumed the exact source diagnostic',raw['armed'],0)
            check('same backend remains usable after owner cleanup',query(c,single),want);clean('preload abort recovery')
        else:
            mode={'concat':10,'concat-error':11,'concat-cancel':12}[a.mode]
            if mode!=12:
                probe(mode)
                if mode==10:check('actual Concat result parity',query(c,single),want)
                else:
                    try:query(c,single)
                    except BaseException as exc:
                        check('controlled error after real cache Concat',getattr(exc,'pgcode',None),'XX000')
                        check('exact live-cache post-Concat fault reached',getattr(getattr(exc,'diag',None),'message_primary',None),
                              'controlled live-cache error after Concat')
                    else:check('controlled error must be observed',False)
            else:
                key=(1<<44)+int(hashlib.sha256((a.run_id+'/concat').encode()).hexdigest()[:9],16);dbid=db.allocation['database_oid'];pid=scalar(c,'SELECT pg_backend_pid()');holder=scalar(t.b,'SELECT pg_backend_pid()');tag=(dbid,key>>32,key&0xffffffff)
                where="locktype='advisory' AND database=%s AND classid=%s::oid AND objid=%s::oid AND objsubid=1 AND mode='ExclusiveLock'"
                outcome={}
                def run():
                    try:outcome['rows']=query(c,single)
                    except BaseException as exc:outcome.update(sqlstate=getattr(exc,'pgcode',None),error=str(exc))
                query(t.b,'SELECT pg_advisory_lock(%s::bigint)',(key,));gate_held=True;probe(12,key);worker=threading.Thread(target=run,daemon=True);worker.start()
                deadline=time.monotonic()+30;waiter=[]
                while time.monotonic()<deadline and worker.is_alive():
                    waiter=query(t.observer,'SELECT pid,granted FROM pg_locks WHERE '+where+' AND pid=%s AND NOT granted',tag+(pid,))
                    if waiter:break
                    time.sleep(.01)
                check('actual live-cache post-Concat gate',waiter,[(pid,False)]);check('gate exact controller holder',query(t.observer,'SELECT pid,granted FROM pg_locks WHERE '+where+' AND pid=%s AND granted',tag+(holder,)),[(holder,True)]);check('cancel exact actor',scalar(t.observer,'SELECT pg_cancel_backend(%s)',(pid,)))
                check('release holder before join',scalar(t.b,'SELECT pg_advisory_unlock(%s::bigint)',(key,)));gate_held=False;worker.join(timeout=30);check('actor drains',worker.is_alive(),False);check('actual cache cancellation57014',outcome.get('sqlstate'),'57014');result['cancel']=outcome
            v=clean('post Concat operation');result['concat_probe']=probe();check('live immutable image really loaded',v['loads']>0);check('actual host reparent callback observed',v['reparent']>0);check('old diagnostic frontier path was not used',result['concat_probe']['opened'],0);check('cache arm consumed',result['concat_probe']['armed'],0)
        check('all frozen source/provider inputs and archives unchanged',all(sha256(path)==entry['sha256']==sha256(entry['archive'])for path,entry in result['inputs'].items()));check_build(a.build_receipt);result['status']='pass'
    except BaseException as exc:result.update(reason=str(exc),sqlstate=getattr(exc,'pgcode',None),traceback=traceback.format_exc())
    finally:
        if gate_held and t is not None:
            try:query(t.b,'SELECT pg_advisory_unlock(%s::bigint)',(key,));gate_held=False
            except BaseException as exc:result['gate_cleanup_error']=str(exc)
        if worker is not None and worker.is_alive():
            try:t.a.cancel()
            except BaseException as exc:result['cancel_cleanup_error']=str(exc)
            worker.join(timeout=30)
        busy=worker is not None and worker.is_alive()
        if busy:result.update(status='fail',cleanup_error='Actor did not drain; preserve exact owned DB/roles')
        if db is not None and not busy:
            try:db.cleanup();result['database_cleanup']=db.cleanup_result
            except BaseException as exc:result.update(status='fail',database_cleanup_error=str(exc))
        if roles is not None:
            try:
                if not result.get('database_cleanup')or result['database_cleanup']['returncode']:raise RuntimeError('Exact DB cleanup unproven; preserve roles')
                result['role_cleanup']=cleanup_roles(control,roles.path,a.run_id);result=roles.redact(result)
            except BaseException as exc:result.update(status='fail',role_cleanup_error=str(exc))
        if control is not None:control.close()
        write_json(out/'result.json',result)
    return 0 if result['status']=='pass'else 1
if __name__=='__main__':raise SystemExit(main())
