#!/usr/bin/env python3
"""Real ResourceOwnerConcat with a live Native PQ lease, isolated future gate.

One real512-vector file and one committed graph/PQ build. Modes7/8 transfer to
an audited private target and back; mode9/cancel abort after first transfer and
source deletion. Reparent from subcommit is explicitly excluded. Preparation
or AST checks alone do not establish Native runtime qualification.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor,TimeoutError
from copy import deepcopy
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time
import traceback
from types import SimpleNamespace
ROOT=Path('/home/qiumc/data/code/cpp');D=ROOT/'integration/opengauss-native-lake'
sys.path[:0]=[str(D),str(D/'tests')]
from file_dml_acceptance import check_build
from inline_fixture import query,scalar
from lake_harness import sha256,validate_run_id,write_json
from native_auto_flush import connect_control
from native_runtime import PROFILE,INSTALL
from native_test_context import OwnedDatabase
from native_table_create import Case
from permission_api import OwnedRoles,cleanup_roles
from native_pq_oracle import literal


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--run-id',required=True)
    p.add_argument('--build-receipt',type=Path,required=True);p.add_argument('--candidate-receipt',type=Path,required=True)
    p.add_argument('--metric',choices=['l2','ip','cosine'],default='l2')
    p.add_argument('--build-timeout-ms',type=int,choices=[300000,900000],default=300000)
    a=p.parse_args();validate_run_id(a.run_id)
    if PROFILE not in ['debug','ann-debug']:p.error('This diagnostic requires an explicitly owned Debug profile')
    out=ROOT/'公共文档/openGauss原生湖存储/验证'/a.run_id;out.mkdir(exist_ok=False)
    r=dict(status='fail',scope=__doc__,metric=a.metric,profile=PROFILE,assertions=[],runtime_executed=False,
           limits=['One ordinary backend/one source/512vectors; no SMP/RAC/global ANN recall/performance or commercial qualification.',
                   'Modes7..9 are superuser-only private SQL diagnostics, absent from extension installation SQL.'])
    files=[Path(__file__),a.build_receipt,a.candidate_receipt,D/'tests/file_dml_acceptance.py',D/'tests/native_pq_oracle.py',D/'tests/native_table_create.py',D/'tests/permission_api.py',D/'native_test_context.py',D/'native_runtime.py',D/'tests/inline_cleanup.py',D/'tests/native_auto_flush.py',D/'tests/catalog_fixture_bundle.py',D/'inline_fixture.py',D/'lake_harness.py',D/'fixture.py',D/'tests/empty_manifest.py',D/'tests/empty_catalog_oracle.py',D/'tests/empty_reference_fixture.py',D/'tests/create_reference_oracle.py',D/'tests/reverse_reference.py',D/'reference.sh',D/'source-lock.json']
    r['inputs']={str(f.resolve()):sha256(f)for f in files};archive=out/'executed-inputs';archive.mkdir()
    for i,f in enumerate(files):shutil.copy2(f,archive/(str(i)+'-'+f.name))
    control=db=roles=t=reservation=pool=future=None;gate_held=False;key=pid=None;busy=False
    source=Path(os.getenv('OG_LAKE_SOURCE',str(ROOT/'.worktrees/opengauss-native-lake')))
    def check(label,got,want=True):
        good=got==want;r['assertions'].append(deepcopy(dict(name=label,observed=got,expected=want,status='pass'if good else'fail')))
        write_json(out/'result.json',r)
        if not good:raise AssertionError(label+': '+repr(got))
    def configure(c):query(c,'SET session_timeout=0');query(c,'SET statement_timeout=120000');query(c,'SET query_dop=1')
    try:
        b=check_build(a.build_receipt);r.update(source_head=b['source_head'],binary_sha256=b['binary_sha256'],build_sha256=sha256(a.build_receipt))
        check('official qualified PQ dependency present',bool(b.get('pq_inputs_after')));check('PQ dependency unchanged',b.get('pq_inputs_before'),b.get('pq_inputs_after'))
        candidate=json.loads(a.candidate_receipt.read_text())
        for f,h in candidate['native_inputs'].items():check('compiled exact diagnostic source '+f,b['native_source_inputs_after'].get(f),h)
        owner=(source/'src/common/backend/utils/resowner/resowner.cpp').read_text();flat=re.sub(r'\s+','',owner)
        check('audited default Concat layout guarded at compile time','static_assert(sizeof(ResourceOwnerData)==464,'in flat)
        check('audited HTAP Concat layout guarded at compile time','static_assert(sizeof(ResourceOwnerData)==496,'in flat)
        check('original runtime448 guard cannot abort first Concat','Assert(sizeof(ResourceOwnerData)==448)'not in flat)
        control=connect_control();configure(control);check('automatic maintenance remains off',scalar(control,'SHOW enable_lake_maintenance'),'off')
        with(out/'fixture.log').open('w')as log:subprocess.run([sys.executable,str(D/'tests/catalog_fixture_bundle.py'),'--run-id',a.run_id,'--output',str(out/'fixture/manifest.json')],stdout=log,stderr=subprocess.STDOUT,check=True,timeout=300)
        reservation=(ROOT/'.reference/opengauss-native-lake/.driver.lock').open('a+');fcntl.flock(reservation.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB)
        db=OwnedDatabase(a.run_id,out);db.create();configure(db.admin);roles=OwnedRoles(control,a.run_id,out)
        t=Case(db,SimpleNamespace(mode='net_dml',sql_version='0.18',manifest=out/'fixture/manifest.json',output=out/'result.json',run_id=a.run_id),r,roles)
        c=t.a
        for conn in [c,t.b,t.observer]:configure(conn)
        check('dedicated ordinary backend only',scalar(c,'SHOW enable_thread_pool'),'off')
        query(c,"CREATE FUNCTION biz.pq_probe(integer,bigint,bigint) RETURNS text AS '$libdir/og_lake','og_lake_test_pq_probe' LANGUAGE C VOLATILE NOT FENCED")
        query(c,'REVOKE ALL ON FUNCTION biz.pq_probe(integer,bigint,bigint) FROM PUBLIC')
        query(c,'CREATE TABLE biz.exact(id integer PRIMARY KEY,v vector(16),note text)')
        query(c,"CREATE FOREIGN TABLE biz.vectors(id integer,v vector(16),note text) SERVER created_lake OPTIONS(binding_mode 'create',lake_schema 'main')")
        query(c,"SELECT og_lake.set_option('biz.vectors'::regclass,'data_inlining_row_limit','50000')")
        def vector(i):return '['+','.join(format((((i*131+j*79+((i*(j+1))%29)*17)%2039)-1019)/1024.0,'.9g')for j in range(16))+']'
        rows=','.join('('+str(i)+','+literal(vector(i))+'::vector,'+literal('row/'+str(i))+')'for i in range(1,513))
        for table in ['biz.exact','biz.vectors']:query(c,'INSERT INTO '+table+' VALUES '+rows)
        query(c,"SELECT og_lake.flush('biz.vectors'::regclass)")
        check('actual source population',scalar(c,'SELECT count(*) FROM biz.vectors'),512)
        spec=dict(version=1,kind='vector',method='diskann',column='v',metric=a.metric,pq=True,pq_chunks=4)
        uuid=str(scalar(c,"SELECT og_lake.create_lake_index('biz.vectors'::regclass,'ix_pq_concat',%s::jsonb)",(json.dumps(spec),)))
        store=out/'index-store';store.mkdir();query(c,"SELECT og_lake.set_option('created_lake'::name,'index_root',%s::text)",(str(store),))
        before_head=t.head();before_protocol=t.protocol();query(c,'SET statement_timeout='+str(a.build_timeout_ms))
        query(c,'SELECT og_lake.build_lake_index(%s::uuid)',(uuid,));query(c,'SET statement_timeout=120000')
        coverage=query(c,"SELECT c.source_file_id,c.physical_row_count,s.format_version,s.artifact_root_uri,s.manifest_hash FROM og_lake_internal.index_segment s JOIN og_lake_internal.index_coverage c USING(segment_uuid,index_uuid,definition_generation) WHERE s.index_uuid=%s::uuid AND s.state='READY' AND c.active ORDER BY c.source_file_id",(uuid,))
        check('exact one real trained PQ source',len(coverage),1);target,n,fmt,directory,manifest=coverage[0]
        check('actual physical512 and format2',(n,fmt),(512,2));r['actual_coverage']=coverage
        check('actual five immutable graph/PQ files exist',all((Path(directory)/name).is_file()for name in ['ann_nodes.parquet','ann_lookup.parquet','ann_pq_model.parquet','ann_pq_codes.parquet','ann_pq_manifest.parquet']))
        check('fixture has one catalog for source-file probe identity',scalar(c,'SELECT count(*) FROM og_lake_internal.catalog_binding'),1)
        def stats(mode=0,gate=0):return json.loads(scalar(c,'SELECT biz.pq_probe(%s,%s,%s)',(mode,target,gate)))
        query(c,'SET enable_lake_ann=on');query(c,'SET enable_seqscan=off')
        op={'l2':'<->','ip':'<#>','cosine':'<=>'}[a.metric];q=vector(47)
        statement="SELECT id,encode(vector_send(v),'hex'),note,v "+op+' %s::vector AS d FROM biz.vectors WHERE v IS NOT NULL ORDER BY d LIMIT 10'
        def returned(label):
            got=query(c,statement,(q,));check(label+' ten returned rows',len(got),10)
            for row in got:check(label+' exact original vector/distance '+str(row[0]),[row],query(c,"SELECT id,encode(vector_send(v),'hex'),note,v "+op+' %s::vector FROM biz.exact WHERE id=%s',(q,row[0])))
            return got
        def balance(label,reparents,owner_closes=None):
            s=stats();r.setdefault('probes',{})[label]=deepcopy(s)
            check(label+' one actual lease closed/destroyed once',(s['opened'],s['closed'],s['destroyed'],s['active'],s['violations']),(1,1,1,0,0))
            check(label+' Concat reparent and no subcommit',(s['reparent'],s['subcommit']),(reparents,0))
            check(label+' one actual closing path',s['owner']+s['context']+s['explicit'],1)
            if owner_closes is not None:check(label+' actual owner/context cleanup',(s['owner'],s['context']),(owner_closes,0))
            check(label+' positive mode consumed',s['armed'],0)
            return s
        plan='\n'.join(row[0]for row in query(c,'EXPLAIN(ANALYZE,COSTS OFF) '+statement,(q,)));(out/'actual-pq-plan.txt').write_text(plan+'\n')
        check('actual PQ path verified','Verified PQ File Coverage: 1'in plan and 'Verified ANN File Coverage: 1'in plan);r['runtime_executed']=True
        for mode,label in [(7,'direct Concat'),(8,'descendant Concat')]:
            stats(-1);stats(mode);returned(label);s=balance(label,2);check(label+' distances run after deleted source owners',s['distances']>0)
        stats(-1);stats(9)
        try:query(c,statement,(q,))
        except Exception as exc:check('post-Concat actual ERROR preserves SQLSTATE',getattr(exc,'pgcode',None),'XX000')
        else:check('post-Concat error actually raised',False)
        balance('post-Concat ERROR',1,1)
        stats(-1);key=(1<<43)+int(hashlib.sha256(a.run_id.encode()).hexdigest()[:9],16);pid=scalar(c,'SELECT pg_backend_pid()');dbid=db.allocation['database_oid']
        query(t.b,'SELECT pg_advisory_lock(%s::bigint)',(key,));gate_held=True
        pool=ThreadPoolExecutor(max_workers=1);stats(7,key);future=pool.submit(query,c,statement,(q,));busy=True
        deadline=time.monotonic()+20;waits=[];holder=[];holder_pid=scalar(t.b,'SELECT pg_backend_pid()')
        while time.monotonic()<deadline and not future.done():
            predicate="locktype='advisory' AND database=%s AND classid=%s::oid AND objid=%s::oid AND objsubid=1 AND mode='ExclusiveLock'"
            args=(dbid,key>>32,key&0xffffffff)
            waits=query(t.observer,'SELECT pid,granted FROM pg_locks WHERE '+predicate+' AND pid=%s AND NOT granted',args+(pid,))
            holder=query(t.observer,'SELECT pid,granted FROM pg_locks WHERE '+predicate+' AND pid=%s AND granted',args+(holder_pid,))
            if waits and holder:break
            time.sleep(.01)
        check('exact post-Concat live lease gate waiting',waits,[(pid,False)]);check('exact controller gate owner',holder,[(holder_pid,True)])
        check('query still live at cancel',not future.done());check('cancel exact Native backend',scalar(t.observer,'SELECT pg_cancel_backend(%s)',(pid,)))
        try:future.result(timeout=30)
        except Exception as exc:check('post-Concat cancellation actual SQLSTATE',getattr(exc,'pgcode',None),'57014')
        else:check('post-Concat cancellation was observed',False)
        busy=False;check('release only exact owned gate',scalar(t.b,'SELECT pg_advisory_unlock(%s::bigint)',(key,)));gate_held=False
        pool.shutdown(wait=True);pool=None;future=None
        balance('post-Concat cancellation',1,1)
        stats(-1);returned('same backend after Concat cancel');s=balance('reused ordinary owner',0);check('same backend still scores actual PQ',s['distances']>0)
        check('all diagnostics preserve business head',t.head(),before_head);check('all diagnostics preserve standard catalog',t.protocol(),before_protocol)
        check('executed input identities frozen',all(sha256(f)==h for f,h in r['inputs'].items()));check_build(a.build_receipt);r['status']='pass'
    except BaseException as exc:r.update(status='fail',reason=str(exc),traceback=traceback.format_exc())
    finally:
        # Always release the controller gate before any drain/join. Do not
        # overwrite an original FAIL with cleanup or bounded-cancel success.
        if gate_held and t is not None:
            try:r['gate_released_in_cleanup']=bool(scalar(t.b,'SELECT pg_advisory_unlock(%s::bigint)',(key,)));gate_held=not r['gate_released_in_cleanup']
            except BaseException as exc:r['gate_cleanup_error']=str(exc)
        if future is not None and not future.done()and t is not None:
            try:scalar(t.observer,'SELECT pg_cancel_backend(%s)',(pid,))
            except BaseException as exc:r['cancel_cleanup_error']=str(exc)
            try:future.result(timeout=30)
            except TimeoutError:
                # This PID and database OID were captured from our own actor.
                # No prefix-based or unrelated-session termination is allowed.
                try:r['terminated_owned_actor']=query(t.observer,'SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid=%s AND datid=%s',(pid,db.allocation['database_oid']));future.result(timeout=30)
                except BaseException as exc:r['drain_cleanup_error']=str(exc)
            except BaseException:pass
        busy=future is not None and not future.done()
        if pool is not None:pool.shutdown(wait=not busy)
        if db is not None and not busy and not gate_held:
            try:db.cleanup();r['database_cleanup']=db.cleanup_result
            except BaseException as exc:r.update(status='fail',cleanup_error=str(exc))
        elif db is not None:r.update(status='fail',cleanup_error='Busy actor or gate retained; exact owned receipts retained for controller cleanup.')
        if roles is not None:
            try:
                if not r.get('database_cleanup')or r['database_cleanup']['returncode']:raise RuntimeError('Owned database cleanup unproven; roles retained')
                r['role_cleanup']=cleanup_roles(control,roles.path,a.run_id);r=roles.redact(r)
            except BaseException as exc:r.update(status='fail',role_cleanup_error=str(exc))
        if control is not None:control.close()
        if reservation is not None:reservation.close()
        write_json(out/'result.json',r)
    print(json.dumps(dict(status=r['status'],checks=len(r['assertions']),reason=r.get('reason'),result=str(out/'result.json')),ensure_ascii=False));return 0 if r['status']=='pass'else 1


if __name__=='__main__':raise SystemExit(main())
