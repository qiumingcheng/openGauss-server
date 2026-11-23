#!/usr/bin/env python3
"""Prepared file-ANALYZE edge gates. No fixture/build is run by preparation.

Each future invocation owns one database. A separate owned SQL client holds
ANALYZE's real first sampled row/SDK batch gate; no Python thread join is used.
On drain failure retain owned database/roles and stop the serial campaign.
"""
from copy import deepcopy
import argparse,hashlib,json,os
from pathlib import Path
import shutil,subprocess,sys,time,traceback
from types import SimpleNamespace
ROOT=Path(os.getenv('CPP_WORKSPACE',str(Path(__file__).resolve().parents[3]))).resolve()
D=ROOT/'integration/opengauss-native-lake';sys.path[:0]=[str(D),str(D/'tests')]
from file_dml_acceptance import check_build
from inline_fixture import query,scalar
from lake_harness import sha256,validate_run_id,write_json
from native_auto_flush import connect_control
from native_runtime import PROFILE,RUNTIME,PORT
from native_table_create import Case
from native_test_context import OwnedDatabase
from permission_api import OwnedRoles,cleanup_roles
MODES=['empty','null_extremes','admission','precision','mvcc','cancel']


def analyze_client(a):
    import psycopg2
    owner=json.loads(a.owner_manifest.read_text())
    if owner.get('phase')!='owned' or owner.get('run_id')!=a.run_id or owner.get('runtime_profile')!=PROFILE:
        raise RuntimeError('Client lacks exact owned database identity')
    c=None;r=dict(status='fail',pid=None,sqlstate=None);out=a.client_result
    try:
        check_build(a.build_receipt)
        c=psycopg2.connect(host=str(RUNTIME/'socket'),port=PORT,dbname=owner['database'],user=os.getenv('USER','qiumc'),connect_timeout=10);c.autocommit=True
        observed=query(c,"SELECT oid,shobj_description(oid,'pg_database') FROM pg_database WHERE datname=current_database()")
        if observed!=[(owner['database_oid'],owner['marker'])]:raise RuntimeError('Client database ownership marker differs')
        for s in ['SET session_timeout=0','SET statement_timeout=90000','SET query_dop=1','SET default_statistics_target=10','SET client_min_messages=debug1']:query(c,s)
        pid=scalar(c,'SELECT pg_backend_pid()');r.update(pid=pid,database=owner['database'],phase='arming');write_json(out,r)
        query(c,"SELECT biz.analyze_pause('biz.orders'::regclass,1,%s::bigint)",(a.gate,))
        # No SQL may intervene between arm and this top-level ANALYZE.
        r['phase']='armed';write_json(out,r)
        try:query(c,'ANALYZE biz.orders');r.update(status='pass',phase='analyzed')
        except Exception as exc:r.update(status='cancelled'if getattr(exc,'pgcode',None)=='57014'else'fail',sqlstate=getattr(exc,'pgcode',None),reason=str(exc),phase='analyze-error')
        r['reuse_count']=scalar(c,'SELECT count(*) FROM biz.orders')
        r['notices']=list(c.notices)[-256:];check_build(a.build_receipt)
    except BaseException as exc:r.update(status='fail',reason=str(exc),traceback=traceback.format_exc())
    finally:
        if c is not None:c.close()
        write_json(out,r)
    return 0 if r['status']in('pass','cancelled')else 1


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--run-id',required=True);p.add_argument('--build-receipt',required=True,type=Path)
    p.add_argument('--mode',choices=MODES);p.add_argument('--_client',action='store_true',help=argparse.SUPPRESS)
    p.add_argument('--owner-manifest',type=Path);p.add_argument('--client-result',type=Path);p.add_argument('--gate',type=int,default=0)
    a=p.parse_args();validate_run_id(a.run_id)
    if PROFILE!='debug':p.error('Requires the exact owned Debug runtime')
    if a._client:
        if not a.owner_manifest or not a.client_result or a.gate<=0:p.error('Owned analyze client arguments missing')
        return analyze_client(a)
    if a.mode is None:p.error('--mode required')
    out=ROOT/'公共文档/openGauss原生湖存储/验证'/a.run_id;out.mkdir(exist_ok=False)
    r=dict(status='fail',scope=__doc__,mode=a.mode,assertions=[],native_runtime_executed=False);db=roles=control=t=client=None;preserve=False;client_state={};gate_held=False;key=None
    inputs=[Path(__file__),D/'tests/file_dml_acceptance.py',D/'native_test_context.py',D/'native_runtime.py',D/'inline_fixture.py',D/'tests/native_table_create.py',D/'tests/permission_api.py',D/'tests/native_auto_flush.py',D/'tests/catalog_fixture_bundle.py',D/'tests/inline_cleanup.py',D/'source-lock.json',a.build_receipt]
    r['inputs']={str(x):sha256(x)for x in inputs};archive=out/'executed-inputs';archive.mkdir()
    for i,x in enumerate(inputs):shutil.copy2(x,archive/(str(i)+'-'+x.name))
    def image(x):
        if isinstance(x,bytes):return dict(bytes=len(x),sha256=hashlib.sha256(x).hexdigest())
        if isinstance(x,list)and len(x)>30:return dict(rows=len(x),sha256=hashlib.sha256(repr(x).encode()).hexdigest())
        return x
    def check(label,got,want=True):
        good=got==want;r['assertions'].append(dict(name=label,status='pass'if good else'fail',observed=deepcopy(image(got)),expected=deepcopy(image(want))));write_json(out/'result.json',r)
        if not good:raise AssertionError(label)
    def stat_image():return query(t.observer,"SELECT staattnum,s::text FROM pg_statistic s WHERE starelid='biz.orders'::regclass ORDER BY staattnum"),query(t.observer,"SELECT reltuples,relpages,relallvisible FROM pg_class WHERE oid='biz.orders'::regclass")
    def pgstats(table):return query(t.observer,"SELECT attname,null_frac,avg_width,n_distinct,most_common_vals::text,most_common_freqs::text,histogram_bounds::text FROM pg_stats WHERE schemaname='biz' AND tablename=%s AND NOT inherited ORDER BY attname",(table,))
    def both(sql):
        query(t.b,'BEGIN')
        try:
            for name in ['biz.orders','biz.reference']:query(t.b,sql.format(table=name))
            query(t.b,'COMMIT')
        except BaseException:query(t.b,'ROLLBACK');raise
    def deny(sql,code):
        try:query(t.a,sql)
        except Exception as exc:check('SQLSTATE/'+sql,getattr(exc,'pgcode',None),code)
        else:check('expected failure/'+sql,False)
    try:
        b=check_build(a.build_receipt);r.update(binary_sha256=b['binary_sha256'],source_head=b['source_head']);check('actual sampler source is in official input map','contrib/og_lake/lake_sample.cpp'in b['native_source_inputs_after'])
        control=connect_control();query(control,'SET session_timeout=0');query(control,'SET statement_timeout=30000');check('maintenance remains off',scalar(control,'SHOW enable_lake_maintenance'),'off');check('PID oracle uses nonpool runtime',scalar(control,'SHOW enable_thread_pool'),'off')
        with(out/'fixture.log').open('w')as f:subprocess.run([sys.executable,str(D/'tests/catalog_fixture_bundle.py'),'--run-id',a.run_id,'--output',str(out/'fixture/manifest.json')],stdout=f,stderr=subprocess.STDOUT,timeout=300,check=True)
        db=OwnedDatabase(a.run_id,out);db.create();roles=OwnedRoles(control,a.run_id,out)
        t=Case(db,SimpleNamespace(mode='net_dml',sql_version='0.18',manifest=out/'fixture/manifest.json',output=out/'result.json',run_id=a.run_id),r,roles)
        for c in [t.a,t.b,t.observer]:
            for sql in ['SET session_timeout=0','SET statement_timeout=90000','SET query_dop=1','SET default_statistics_target=10','SET enable_default_ustore_table=off']:query(c,sql)
        r['native_runtime_executed']=True;t.create();query(t.a,'CREATE TABLE biz.reference(id integer,val integer)')
        query(t.a,"CREATE FUNCTION biz.analyze_pause(oid,bigint,bigint) RETURNS void AS '$libdir/og_lake','og_lake_test_analyze_pause' LANGUAGE C VOLATILE NOT FENCED")
        query(t.a,'REVOKE ALL ON FUNCTION biz.analyze_pause(oid,bigint,bigint) FROM PUBLIC')
        query(t.a,"SELECT og_lake.set_option('biz.orders'::regclass,'data_inlining_row_limit','50000')")
        n=400000 if a.mode=='precision' else 20000 if a.mode=='cancel' else 32 if a.mode=='empty' else 1000
        expression='NULL::integer'if a.mode=='null_extremes'else'CASE WHEN i%4=0 THEN NULL WHEN i%2=0 THEN -2147483648 ELSE 2147483647 END'
        for first in range(1,n+1,50000):
            both('INSERT INTO {table} SELECT i,'+expression+' FROM generate_series('+str(first)+','+str(min(n,first+49999))+')i')
            if a.mode=='precision':query(t.a,"SELECT og_lake.flush('biz.orders'::regclass)")
        both('UPDATE {table} SET val=NULL WHERE id=1');both('UPDATE {table} SET val=42 WHERE id=1');both('DELETE FROM {table} WHERE id=2')
        query(t.a,"SELECT og_lake.flush('biz.orders'::regclass)");both('INSERT INTO {table} VALUES(NULL,NULL),(100000,NULL)')
        query(t.a,'ANALYZE biz.orders');query(t.b,'ANALYZE biz.reference')
        if a.mode not in ('cancel','precision'):check('complete small file+inline sample equals host NULL/MCV/NDV/histogram',pgstats('orders'),pgstats('reference'))
        before=stat_image();before_stats=pgstats('orders');head=t.head();protocol=t.protocol();expected_count=scalar(t.observer,'SELECT count(*) FROM biz.reference')
        input_root=Path(t.empty_manifest['data_path']).resolve();input_files={}
        for path in sorted(input_root.rglob('*.parquet')):
            check('ANALYZE input remains inside owned root',path.resolve().is_relative_to(input_root))
            destination=out/'fixture-input-files'/path.relative_to(input_root);destination.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(path,destination)
            check('frozen ANALYZE source copy matches',sha256(destination),sha256(path))
            input_files[str(path)]=dict(sha256=sha256(path),bytes=path.stat().st_size,archive=str(destination.relative_to(out)))
        carrier=scalar(t.observer,'SELECT table_name FROM created_metadata.ducklake_inlined_data_tables')
        inline_input=query(t.observer,t.sql.SQL('SELECT _ducklake_row_id,_ducklake_begin_snapshot,_ducklake_end_snapshot,id,val FROM created_metadata.{} ORDER BY ctid').format(t.sql.Identifier(carrier)))
        write_json(out/'fixture-input.json',dict(head=head,files=input_files,inline=inline_input))
        if a.mode=='admission':
            mcv=scalar(t.observer,"SELECT most_common_vals::text FROM pg_stats WHERE schemaname='biz'AND tablename='orders'AND attname='val'")
            check('int32 min/max survive exact full-sample MCV',mcv is not None and '-2147483648'in mcv and '2147483647'in mcv)
        if a.mode=='empty':
            both('DELETE FROM {table}');query(t.a,'ANALYZE biz.orders');query(t.b,'ANALYZE biz.reference')
            check('small all-deleted file+inline population is exactly zero',query(t.observer,"SELECT reltuples FROM pg_class WHERE oid='biz.orders'::regclass"),[(0.0,)])
            check('empty ANALYZE follows native heap old-statistic semantics',pgstats('orders'),pgstats('reference'))
            plan='\n'.join(x[0]for x in query(t.a,'EXPLAIN SELECT * FROM biz.orders'));check('empty analyzed CBO does not lift live count to file physical rows','Row Estimate: pg_class / ANALYZE visible file+inline estimate'in plan)
        elif a.mode=='null_extremes':
            # Update row1 made one non-NULL value; preserve an all-NULL column
            # check after the history-bearing full-sample baseline.
            # Only id=1 is non-NULL in this fixture; all other val rows
            # already are NULL. Keep the full1000-row statistics population.
            both('UPDATE {table} SET val=NULL WHERE id=1');query(t.a,'ANALYZE biz.orders');query(t.b,'ANALYZE biz.reference')
            check('all-NULL statistics match ordinary heap',pgstats('orders'),pgstats('reference'))
            check('all-NULL fraction is exactly one',scalar(t.a,"SELECT null_frac FROM pg_stats WHERE schemaname='biz'AND tablename='orders'AND attname='val'"),1.0)
        elif a.mode=='admission':
            for target in [0,-2]:
                query(t.a,'SET default_statistics_target='+str(target));deny('ANALYZE biz.orders','0A000');query(t.a,'SET default_statistics_target=10')
                check('unsupported target preserves statistics/'+str(target),stat_image(),before)
            query(t.a,'BEGIN');deny('ANALYZE biz.orders','25001');query(t.a,'ROLLBACK')
            check('transaction guard preserves statistics',stat_image(),before);check('admission refusal publishes no lake changes',t.protocol(),protocol)
        elif a.mode=='precision':
            query(t.a,'SET default_statistics_target=1000');deny('ANALYZE biz.orders','54000');query(t.a,'SET default_statistics_target=10')
            check('bounded precision refusal preserves old statistics',stat_image(),before);check('precision refusal publishes no lake changes',t.protocol(),protocol)
        else:
            key=(1<<45)+int(hashlib.sha256(a.run_id.encode()).hexdigest()[:9],16);dbid=db.allocation['database_oid'];client_result=out/'analyze-client.json';client_log=(out/'analyze-client.stdout').open('w')
            query(t.b,'SELECT pg_advisory_lock(%s::bigint)',(key,));gate_held=True
            command=[sys.executable,str(Path(__file__).resolve()),'--_client','--run-id',a.run_id,'--build-receipt',str(a.build_receipt),'--owner-manifest',str(out/'database-owner.json'),'--client-result',str(client_result),'--gate',str(key)]
            env=dict(os.environ);env['CPP_WORKSPACE']=str(ROOT);client=subprocess.Popen(command,stdout=client_log,stderr=subprocess.STDOUT,env=env)
            locks=[];client_state={};deadline=time.monotonic()+30
            while time.monotonic()<deadline and client.poll()is None:
                if client_result.exists():
                    try:client_state=json.loads(client_result.read_text())
                    except ValueError:pass
                if client_state.get('pid'):
                    locks=query(t.observer,"SELECT pid,granted FROM pg_locks WHERE pid=%s AND database=%s AND locktype='advisory'AND classid=%s::oid AND objid=%s::oid AND objsubid=1 AND NOT granted",(client_state['pid'],dbid,key>>32,key&0xffffffff))
                    if locks:break
                time.sleep(.01)
            check('exact ANALYZE sampled-row-held gate observed',locks,[(client_state.get('pid'),False)])
            if a.mode=='cancel':check('cancel targets exact owned analyze client',scalar(t.observer,'SELECT pg_cancel_backend(%s)',(client_state['pid'],)))
            else:
                both('UPDATE {table} SET val=123 WHERE id=7');both('DELETE FROM {table} WHERE id=8')
            query(t.b,'SELECT pg_advisory_unlock(%s::bigint)',(key,));gate_held=False;head_after_dml=t.head()
            try:client.wait(timeout=45)
            except subprocess.TimeoutExpired:preserve=True;client.kill();client.wait(timeout=5);raise RuntimeError('Owned analyze client did not drain; scene retained')
            client_log.close();check('client exits normally after its guarded result',client.returncode,0);observed=json.loads(client_result.read_text());r['analyze_client']=observed
            if a.mode=='cancel':
                check('live sampled-row cancel returns real SQLSTATE',observed['sqlstate'],'57014');check('cancel preserves raw host statistics',stat_image(),before);check('cancel leaves lake metadata untouched',t.protocol(),protocol)
                check('same canceled backend reused reader correctly',observed['reuse_count'],expected_count)
            else:
                check('paused ANALYZE completed successfully',observed['status'],'pass');check('ANALYZE keeps original full-sample population',scalar(t.a,"SELECT reltuples FROM pg_class WHERE oid='biz.orders'::regclass"),float(expected_count))
                check('MVCC ANALYZE returns original logical distributions',pgstats('orders'),before_stats)
                check('new statements see current committed delete/update',query(t.a,'SELECT id,val FROM biz.orders ORDER BY id,val NULLS FIRST'),query(t.observer,'SELECT id,val FROM biz.reference ORDER BY id,val NULLS FIRST'))
            check('ANALYZE allocates no additional lake snapshot',t.head(),head_after_dml)
        check('archived input bytes unchanged',all(sha256(f)==h for f,h in r['inputs'].items()));check_build(a.build_receipt);r['status']='pass'
    except BaseException as exc:r.update(reason=str(exc),traceback=traceback.format_exc())
    finally:
        if client is not None and client.poll()is None:
            preserve=True
            if client_state.get('pid') and t is not None:
                try:query(t.observer,'SELECT pg_cancel_backend(%s)',(client_state['pid'],))
                except Exception:pass
            if gate_held and t is not None:
                try:query(t.b,'SELECT pg_advisory_unlock(%s::bigint)',(key,));gate_held=False
                except Exception:pass
            try:client.wait(timeout=10)
            except subprocess.TimeoutExpired:
                client.kill()
                try:client.wait(timeout=5)
                except subprocess.TimeoutExpired:pass
        if gate_held and t is not None:
            try:query(t.b,'SELECT pg_advisory_unlock(%s::bigint)',(key,));gate_held=False
            except Exception:preserve=True
        if preserve:r['preserve_owned_scene']=True;r['preserved_database']=db.allocation if db else None
        if db is not None and not preserve:
            try:db.cleanup();r['database_cleanup']=db.cleanup_result
            except BaseException as exc:r.update(status='fail',cleanup_error=str(exc))
        if roles is not None and not preserve:
            try:
                if not r.get('database_cleanup')or r['database_cleanup']['returncode']:raise RuntimeError('Owned database cleanup not proven; roles retained')
                r['role_cleanup']=cleanup_roles(control,roles.path,a.run_id);r=roles.redact(r)
            except BaseException as exc:r.update(status='fail',role_cleanup_error=str(exc))
        if roles is not None and preserve:r=roles.redact(r)
        if control is not None:control.close()
        write_json(out/'result.json',r)
    print(json.dumps(dict(status=r['status'],checks=len(r['assertions']),reason=r.get('reason'),result=str(out/'result.json'))));return 0 if r['status']=='pass'else 1


if __name__=='__main__':raise SystemExit(main())
