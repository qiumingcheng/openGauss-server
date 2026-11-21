#!/usr/bin/env python3
"""Native file ANN scalar masks over actual NULL/NaN/infinite/signed-zero columns.

Real native DML and flush produce Parquet, and native SQL over a separate heap
is the comparison oracle. This is bounded RC Debug integration, not PQ,
persistent bitmap, SMP, general recall, performance or commercial admission.
"""
import argparse
import json
from pathlib import Path
import shutil
import subprocess
import sys
import traceback
from types import SimpleNamespace
ROOT=Path(__file__).resolve().parents[3];D=ROOT/'integration/opengauss-native-lake'
sys.path[:0]=[str(D),str(D/'tests')]
from file_dml_acceptance import check_build
from inline_fixture import query,scalar
from lake_harness import sha256,validate_run_id,write_json
from native_auto_flush import connect_control
from native_runtime import PROFILE
from native_test_context import OwnedDatabase
from native_table_create import Case
from permission_api import OwnedRoles,cleanup_roles


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--run-id',required=True)
    p.add_argument('--build-receipt',type=Path,required=True);p.add_argument('--type',choices=['real','double precision'],required=True)
    p.add_argument('--metric',choices=['l2','ip','cosine'],default='l2');a=p.parse_args();validate_run_id(a.run_id)
    if PROFILE!='ann-debug':p.error('Requires the matching separately owned ANN runtime')
    out=ROOT/'公共文档/openGauss原生湖存储/验证'/a.run_id;out.mkdir(exist_ok=False)
    r=dict(status='fail',scope=__doc__,key_type=a.type,metric=a.metric,assertions=[]);control=db=roles=t=None
    files=[Path(__file__),D/'tests/file_dml_acceptance.py',D/'tests/native_table_create.py',D/'tests/permission_api.py',D/'native_test_context.py',
           D/'native_runtime.py',D/'tests/inline_cleanup.py',D/'inline_fixture.py',D/'tests/native_auto_flush.py',D/'source-lock.json']
    r['inputs']={str(f):sha256(f)for f in files};archive=out/'executed-inputs';archive.mkdir()
    for f in files:shutil.copy2(f,archive/f.name)
    provider=ROOT/'.install/opengauss-native-lake-debug/lib/postgresql/og_lake_sdk/libog_lake_sdk_arrow.so';r['provider_sha256']=sha256(provider)
    def check(name,got,want=True):
        ok=got==want;r['assertions'].append(dict(name=name,observed=got,expected=want,status='pass'if ok else'fail'))
        if not ok:raise AssertionError(name+': '+repr(got))
    try:
        b=check_build(a.build_receipt);r.update(source_head=b['source_head'],binary_sha256=b['binary_sha256'])
        control=connect_control();check('automatic maintenance remains off',scalar(control,'SHOW enable_lake_maintenance'),'off')
        with(out/'fixture.log').open('w')as log:
            subprocess.run([sys.executable,str(D/'tests/catalog_fixture_bundle.py'),'--run-id',a.run_id,'--output',str(out/'fixture/manifest.json')],stdout=log,stderr=subprocess.STDOUT,check=True,timeout=300)
        db=OwnedDatabase(a.run_id,out).create();roles=OwnedRoles(control,a.run_id,out)
        t=Case(db,SimpleNamespace(mode='net_dml',sql_version='0.16',manifest=out/'fixture/manifest.json',output=out/'result.json',run_id=a.run_id),r,roles);c=t.a
        query(c,'SET statement_timeout=60000')
        columns='(id integer,score '+a.type+',v vector(3))'
        query(c,'CREATE TABLE biz.exact'+columns)
        query(c,'CREATE FOREIGN TABLE biz.vectors'+columns+" SERVER created_lake OPTIONS(binding_mode 'create',lake_schema 'main')")
        query(c,"SELECT og_lake.set_option('biz.vectors'::regclass,'data_inlining_row_limit','50000')")
        def both(statement):
            query(c,'BEGIN')
            for table in ['biz.exact','biz.vectors']:query(c,statement.format(table=table))
            query(c,'COMMIT')
        score="(CASE WHEN i=1 THEN NULL WHEN i IN (2,3) THEN 'NaN' WHEN i=4 THEN 'Infinity' WHEN i=5 THEN '-Infinity' WHEN i=6 THEN '-0' WHEN i=7 THEN '0' ELSE (i-8)::text END)::"+a.type
        both("INSERT INTO {table} SELECT i,"+score+",('['||i::text||','||(i%7+.25)::text||','||(i*i/100.0)::text||']')::vector FROM generate_series(1,32)i")
        query(c,"SELECT og_lake.flush('biz.vectors'::regclass)")
        both("INSERT INTO {table} VALUES(101,'NaN','[5,3,7]'::vector),(102,NULL,'[5,3,7]'::vector)")
        check('source actually contains two NaNs',scalar(c,"SELECT count(*) FROM biz.exact WHERE id<=32 AND score='NaN'::"+a.type),2)
        check('source actually contains NULL and both infinities',query(c,"SELECT count(*) FILTER(WHERE score IS NULL),count(*) FILTER(WHERE score='Infinity'::"+a.type+"),count(*) FILTER(WHERE score='-Infinity'::"+a.type+") FROM biz.exact WHERE id<=32"),[(1,1,1)])
        store=out/'index-store';store.mkdir();query(c,"SELECT og_lake.set_option('created_lake'::name,'index_root',%s::text)",(str(store),))
        spec=dict(version=1,kind='vector',method='diskann',column='v',metric=a.metric,enable_pq=False)
        uuid=str(scalar(c,"SELECT og_lake.create_lake_index('biz.vectors'::regclass,'ix_float_mask',%s::jsonb)",(json.dumps(spec),)))
        head=t.head();query(c,'SELECT og_lake.build_lake_index(%s::uuid)',(uuid,));check('index build allocates no business snapshot',t.head(),head)
        query(c,'SET enable_lake_ann=on');query(c,'SET enable_seqscan=off')
        op={'l2':'<->','ip':'<#>','cosine':'<=>'}[a.metric];base='v IS NOT NULL'+(' AND vector_norm(v)>0'if a.metric=='cosine'else'')
        def sql(table,predicate,arg='%s::vector',limit=1):
            return "SELECT id,encode(vector_send(v),'hex'),v "+op+' '+arg+' AS d FROM '+table+' WHERE '+base+' AND '+predicate+' ORDER BY d LIMIT '+str(limit)
        cases=[('all nonnull','score IS NOT NULL'),('NULL','score IS NULL'),('NaN equal',"score='NaN'::"+a.type),
               ('NaN greater',"score>'NaN'::"+a.type),('positive infinity',"score='Infinity'::"+a.type),
               ('negative infinity',"score='-Infinity'::"+a.type),('signed zero','score=0::'+a.type),
               ('NaN upper bound',"score<'NaN'::"+a.type)]
        for label,predicate in cases:
            args=('[2,2.25,.04]',);limit=3 if label=='signed zero'else 1;q=sql('biz.vectors',predicate,limit=limit)
            check(label+' original tuple and native distance',query(c,q,args),query(c,sql('biz.exact',predicate,limit=limit),args))
            plan='\n'.join(x[0]for x in query(c,'EXPLAIN (ANALYZE,COSTS OFF) '+q,args));(out/(label.replace(' ','-')+'-plan.txt')).write_text(plan+'\n')
            check(label+' actually executes verified file mask','ANN Scalar Filter:'in plan and'Verified ANN File Coverage: 1'in plan)
            check(label+' inline remains complete','Actual Inline Access: TableAM sequential'in plan)
        query(c,'SET plan_cache_mode=force_generic_plan');query(c,'PREPARE float_mask(vector,double precision) AS '+sql('biz.vectors','score<$2','$1'))
        for value in [1.0000001,float('nan'),float('inf'),float('-inf'),None,0.0]:
            args=('[9,2.25,.81]',value)
            check('generic scalar boundary '+str(value),query(c,'EXECUTE float_mask(%s,%s)',args),query(c,sql('biz.exact','score<%s'),args))
        plan='\n'.join(x[0]for x in query(c,"EXPLAIN (ANALYZE,COSTS OFF) EXECUTE float_mask('[9,2.25,.81]',1.0000001)"));(out/'generic-plan.txt').write_text(plan+'\n')
        check('actual generic scalar Param and file mask','$2'in plan and'ANN Scalar Filter:'in plan and'Verified ANN File Coverage: 1'in plan)
        query(c,'DEALLOCATE float_mask')
        check('original complete population retained',scalar(c,'SELECT count(*) FROM biz.vectors'),34)
        check('executed inputs unchanged',all(sha256(f)==h for f,h in r['inputs'].items()));check('SDK unchanged',sha256(provider),r['provider_sha256'])
        r['artifacts']={str(f):sha256(f)for f in out.rglob('*.parquet')};check_build(a.build_receipt);r['status']='pass'
    except BaseException as exc:r.update(reason=str(exc),traceback=traceback.format_exc())
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
    print(json.dumps(dict(status=r['status'],checks=len(r['assertions']),reason=r.get('reason'),result=str(out/'result.json')),ensure_ascii=False))
    return 0 if r['status']=='pass'else 1

if __name__=='__main__':raise SystemExit(main())
