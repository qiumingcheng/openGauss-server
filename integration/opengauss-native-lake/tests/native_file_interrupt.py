#!/usr/bin/env python3
"""Cancel or terminate a proven owned reader while a real SDK batch is alive."""
import argparse,json,os,shutil,sys,time,traceback,types
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
ROOT=Path('/home/qiumc/data/code/cpp');DRIVER=ROOT/'integration/opengauss-native-lake'
sys.path[:0]=[str(DRIVER),str(DRIVER/'tests')]
from native_file_read import Case
from inline_fixture import query,scalar
from native_test_context import OwnedDatabase
from permission_api import OwnedRoles,cleanup_roles
from lake_harness import Blocked,sha256,validate_run_id,write_json

def execute(t,mode):
    t.bind();t.check('initial native file values',t.rows(),t.expected)
    t.check('initial file descriptors closed',len(t.descriptors()),0)
    pid,database,application=query(t.a,"SELECT pg_backend_pid(),current_database(),current_setting('application_name')")[0]
    t.check('target belongs to owned database',database,t.db.database)
    def slow_read():
        import psycopg2
        try:
            rows=query(t.a,'SELECT pg_sleep(0.002),id FROM biz.orders')
            return {'state':None,'unexpected_rows':len(rows)}
        except psycopg2.Error as exc:return {'state':exc.pgcode,'exception_type':type(exc).__name__}
    pool=ThreadPoolExecutor(max_workers=1);future=None;verified=False
    try:
        future=pool.submit(slow_read);deadline=time.monotonic()+8
        observed=[]
        while time.monotonic()<deadline and not future.done():
            observed=query(t.observer,'SELECT datname,application_name FROM pg_stat_activity WHERE pid=%s',(pid,))
            if observed==[(database,application)] and len(t.descriptors())==1:
                verified=True;break
            time.sleep(.02)
        t.check('reader owns a live real file before interruption',verified,True)
        t.result['interrupted_backend']={'pid':pid,'database':database,'application_name':application,
                                          'observed_activity':observed,'owned_parquet_handles':len(t.descriptors())}
        function='pg_cancel_backend' if mode=='cancel' else 'pg_terminate_backend'
        t.check('signal targets exact proven owned backend',scalar(t.observer,'SELECT '+function+'(%s)',(pid,)),True)
        outcome=future.result(timeout=12);t.result['interruption_outcome']=outcome
        t.check('expected cancellation/termination outcome',outcome['state'] in (['57014'] if mode=='cancel' else ['57P01',None]),True)
        t.check('interruption was a SQL connection exception','exception_type' in outcome,True)
        deadline=time.monotonic()+5
        while t.descriptors() and time.monotonic()<deadline:time.sleep(.02)
        t.check('interruption releases all owned SDK file handles',len(t.descriptors()),0)
        reader=t.a if mode=='cancel' else t.db.connect()
        t.check('same session/new session can still read exact file rows',t.rows(reader),t.expected)
        t.check('subsequent reader also closes its file handles',len(t.descriptors()),0)
        t.check('ordinary SQL remains usable',scalar(reader,'SELECT 42'),42)
    finally:
        if future is not None and not future.done():
            # The libpq object is this exact owned connection even if a backend
            # has disappeared; never cancel arbitrary processes by name/prefix.
            try:t.a.cancel()
            except BaseException:pass
        pool.shutdown(wait=True)

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--mode',choices=['cancel','terminate'],required=True)
    parser.add_argument('--run-id',default=os.getenv('OG_LAKE_RUN_ID'))
    parser.add_argument('--manifest',type=Path,default=os.getenv('OG_LAKE_FIXTURE_MANIFEST'))
    parser.add_argument('--output',type=Path,default=os.getenv('OG_LAKE_RESULT_FILE'));args=parser.parse_args()
    if not all([args.run_id,args.manifest,args.output]):parser.error('run-id, manifest and output required')
    validate_run_id(args.run_id);args.output=args.output.resolve();args.manifest=args.manifest.resolve();args.output.parent.mkdir(parents=True,exist_ok=True)
    result={'status':'fail','scope':'native_file_'+args.mode,'run_id':args.run_id,'assertions':[],'artifacts':[],
        'limitations':['Interruption occurs with a live SDK reader/batch during parent execution; no claim of a forced signal inside a provider instruction',
                       'No file DML, flush, crash, SMP, RAC or commercial qualification']}
    inputs=[Path(__file__),DRIVER/'tests/native_file_read.py',DRIVER/'inline_fixture.py',DRIVER/'native_test_context.py',DRIVER/'tests/permission_api.py']
    frozen={str(path):sha256(path) for path in inputs};archive=args.output.parent/'executed-inputs';archive.mkdir(exist_ok=False)
    for n,path in enumerate(inputs):shutil.copy2(path,archive/(str(n)+'-'+path.name))
    write_json(archive/'input-hashes.json',frozen)
    roles=None;db=None;control=None
    try:
        import psycopg2
        control=psycopg2.connect(host=str(ROOT/'.runtime/opengauss-native-lake/debug/socket'),port=55560,dbname='postgres',user=os.getenv('USER','qiumc'),connect_timeout=10)
        control.autocommit=True;roles=OwnedRoles(control,args.run_id,args.output.parent)
        with OwnedDatabase(args.run_id,args.output.parent) as db:
            setup=types.SimpleNamespace(**dict(vars(args),mode='batch'))
            t=Case(db,setup,result,roles);execute(t,args.mode);t.finish()
            t.check('all executed inputs unchanged',{str(path):sha256(path) for path in inputs},frozen)
        result['status']='pass'
    except Blocked as exc:result.update(status='blocked',reason=str(exc))
    except BaseException as exc:result.update(reason=str(exc),traceback=traceback.format_exc())
    finally:
        if db is not None:result['database_cleanup']=db.cleanup_result;result['artifacts'].extend(db.artifacts())
        if roles is not None:
            try:
                if db is not None and db.cleanup_result and db.cleanup_result['returncode']==0:result['role_cleanup']=cleanup_roles(control,roles.path,args.run_id)
                elif roles.items:raise RuntimeError('owned database cleanup unproven; role cleanup deferred')
            except BaseException as exc:result.update(status='fail',cleanup_error=str(exc))
            result=roles.redact(result)
        if control is not None:control.close()
        seen={entry['path'] for entry in result['artifacts']}
        for path in args.output.parent.rglob('*'):
            if path.is_file() and path!=args.output and path.name!='database-cleanup.json' and str(path) not in seen:
                result['artifacts'].append({'path':str(path)});seen.add(str(path))
        write_json(args.output,result);print(json.dumps({'status':result['status'],'output':str(args.output)}))
    return {'pass':0,'blocked':2}.get(result['status'],1)

if __name__=='__main__':raise SystemExit(main())
