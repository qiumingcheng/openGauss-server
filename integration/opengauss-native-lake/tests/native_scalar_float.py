#!/usr/bin/env python3
"""Native floating B-tree/UB-tree plus encoded Parquet scalar index correctness.

Independent DuckDB orders stored integer keys; native SQL orders original
floats. IEEE special values, cross-float parameters/arrays and version/private
changes are checked. This is RC Debug functionality, not performance or RAC.
"""
import argparse
import hashlib
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
from native_test_context import OwnedDatabase
from native_table_create import Case
from permission_api import OwnedRoles,cleanup_roles

def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--run-id',required=True);p.add_argument('--build-receipt',type=Path,required=True)
    p.add_argument('--type',choices=['real','double'],required=True);p.add_argument('--storage',choices=['heap','ustore'],required=True)
    a=p.parse_args();validate_run_id(a.run_id);out=ROOT/'公共文档/openGauss原生湖存储/验证'/a.run_id;out.mkdir(exist_ok=False)
    r=dict(status='fail',scope=__doc__,type=a.type,storage=a.storage,assertions=[]);control=db=roles=t=None
    files=[Path(__file__),D/'tests/native_table_create.py',D/'tests/permission_api.py',D/'native_test_context.py',D/'native_runtime.py',D/'tests/inline_cleanup.py',D/'tests/file_dml_acceptance.py',D/'inline_fixture.py',D/'tests/native_auto_flush.py',D/'source-lock.json']
    r['inputs']={str(f):sha256(f)for f in files};archive=out/'executed-inputs';archive.mkdir()
    for f in files:shutil.copy2(f,archive/f.name)
    provider=ROOT/'.install/opengauss-native-lake-debug/lib/postgresql/og_lake_sdk/libog_lake_sdk_arrow.so';r['provider_sha256']=sha256(provider)
    duckdb=ROOT/'.reference/opengauss-native-lake/build/duckdb-gcc10/duckdb';r['reference_binary_sha256']=sha256(duckdb)
    def image(v):return dict(rows=len(v),sha256=hashlib.sha256(repr(v).encode()).hexdigest())if isinstance(v,list)and len(v)>30 else v
    def check(name,got,want=True):
        ok=got==want;r['assertions'].append(dict(name=name,observed=image(got),expected=image(want),status='pass'if ok else'fail'))
        if not ok:raise AssertionError(name+': '+repr(got))
    def literal(v):return "'"+str(v).replace("'","''")+"'"
    try:
        b=check_build(a.build_receipt);r.update(source_head=b['source_head'],binary_sha256=b['binary_sha256'])
        control=connect_control();check('automatic maintenance stays off',scalar(control,'SHOW enable_lake_maintenance'),'off')
        with(out/'fixture.log').open('w')as log:subprocess.run([sys.executable,str(D/'tests/catalog_fixture_bundle.py'),'--run-id',a.run_id,'--output',str(out/'fixture/manifest.json')],stdout=log,stderr=subprocess.STDOUT,check=True,timeout=300)
        db=OwnedDatabase(a.run_id,out).create();roles=OwnedRoles(control,a.run_id,out);t=Case(db,SimpleNamespace(mode='net_dml',sql_version='0.16',manifest=out/'fixture/manifest.json',output=out/'result.json',run_id=a.run_id),r,roles);c=t.a
        typ='real'if a.type=='real'else'double precision';send='float4send'if a.type=='real'else'float8send'
        query(c,'CREATE TABLE biz.exact(id integer,k '+typ+',note text)')
        if a.storage=='ustore':query(c,'SET enable_default_ustore_table=on')
        query(c,'CREATE FOREIGN TABLE biz.keys(id integer,k '+typ+",note text) SERVER created_lake OPTIONS(binding_mode 'create',lake_schema 'main')")
        query(c,'SET enable_default_ustore_table=off');query(c,"SELECT og_lake.set_option('biz.keys'::regclass,'data_inlining_row_limit','50000')")
        carrier=query(c,"SELECT c.reloptions FROM created_metadata.ducklake_inlined_data_tables t JOIN pg_class c ON c.relname=t.table_name AND c.relnamespace=(SELECT oid FROM pg_namespace WHERE nspname='created_metadata')")
        check('actual requested carrier storage',len(carrier)==1 and ('storage_type=ustore'in(carrier[0][0]or []))==(a.storage=='ustore'))
        def both(sql):
            query(c,'BEGIN')
            for table in ['biz.keys','biz.exact']:query(c,sql.format(table=table))
            query(c,'COMMIT')
        both('INSERT INTO {table} SELECT i,CASE WHEN i%19=0 THEN NULL ELSE ((i-256)::double precision/4)::'+typ+" END,'row/'||i::text FROM generate_series(1,511)i")
        tiny='1e-30'if a.type=='real'else'1e-300';huge='1e30'if a.type=='real'else'1e300'
        special=['0','-0','NaN','Infinity','-Infinity',tiny,'-'+tiny,huge,'-'+huge,'1.00000011920928955078125','1.000000059604644775390625']
        both('INSERT INTO {table} VALUES'+','.join('('+str(1001+i)+','+literal(v)+'::'+typ+','+literal('special/'+v)+')'for i,v in enumerate(special)))
        query(c,"SELECT og_lake.flush('biz.keys'::regclass)")
        store=out/'index-store';store.mkdir();query(c,"SELECT og_lake.set_option('created_lake'::name,'index_root',%s::text)",(str(store),))
        spec=dict(version=1,kind='scalar',method='btree',column='k');uuid=str(scalar(c,"SELECT og_lake.create_lake_index('biz.keys'::regclass,'float_ix',%s::jsonb)",(json.dumps(spec),)))
        query(c,'SELECT og_lake.build_lake_index(%s::uuid)',(uuid,));segments=query(c,'SELECT segment_uuid::text,manifest_hash,algorithm_version FROM og_lake_internal.index_segment WHERE index_uuid=%s::uuid ORDER BY segment_uuid',(uuid,))
        check('floating adapter codec is explicitly version2',bool(segments)and all(x[2]==2 for x in segments))
        query(c,'SELECT og_lake.build_lake_index(%s::uuid)',(uuid,));check('repeat build retains immutable float segments',query(c,'SELECT segment_uuid::text,manifest_hash,algorithm_version FROM og_lake_internal.index_segment WHERE index_uuid=%s::uuid ORDER BY segment_uuid',(uuid,)),segments)
        coverage=query(c,'SELECT s.artifact_root_uri,c.source_path FROM og_lake_internal.index_segment s JOIN og_lake_internal.index_coverage c USING(segment_uuid,index_uuid,definition_generation) WHERE c.index_uuid=%s::uuid AND c.active',(uuid,));check('one source file has complete encoded coverage',len(coverage),1)
        directory,source=coverage[0];sql='SELECT s.id FROM read_parquet('+literal(Path(directory)/'page_data.parquet')+') d JOIN read_parquet('+literal(source)+',file_row_number=true) s ON d.row_position=s.file_row_number ORDER BY d.values NULLS LAST,s.id;'
        (out/'independent-key-order.sql').write_text(sql+'\n');v=subprocess.run([str(duckdb),'-no-init','-batch','-json','-c','SET threads=1;'+sql],capture_output=True,text=True,timeout=90)
        (out/'independent-key-order.stdout').write_text(v.stdout);(out/'independent-key-order.stderr').write_text(v.stderr)
        if v.returncode:raise RuntimeError('Independent stored-key decoding failed')
        check('stored encoded key order equals native SQL float total order',[x['id']for x in json.loads(v.stdout)],[x[0]for x in query(c,'SELECT id FROM biz.exact ORDER BY k NULLS LAST,id')])
        both("INSERT INTO {table} VALUES(2001,'NaN'::"+typ+",'inline/nan'),(2002,'-0'::"+typ+",'inline/zero'),(2003,NULL,'inline/null')")
        query(c,'SET enable_seqscan=off')
        def equal(predicate,args=None,label=None):
            sql='SELECT id,encode('+send+"(k),'hex'),note FROM {} WHERE "+predicate+' ORDER BY id'
            check(label or predicate,query(c,sql.format('biz.keys'),args),query(c,sql.format('biz.exact'),args))
        def plan(label,predicate='k=0'):
            text='\n'.join(x[0]for x in query(c,'EXPLAIN (ANALYZE,COSTS OFF) SELECT id,k FROM biz.keys WHERE '+predicate));(out/(label+'-plan.txt')).write_text(text+'\n');return text
        text=plan('initial');check('actual native float IndexAM is used',('Actual Inline Access: native UB-tree'if a.storage=='ustore'else'Actual Inline Access: native B-tree')in text);check('actual encoded Parquet candidates are used','Verified Scalar File Coverage: 1'in text)
        for pred in ['k=0','k=-0.0::double precision','k<0','k<=0','k>0','k>=0','k>=1.00000006::double precision','1.00000006::double precision<k',"k='NaN'::double precision","k<'NaN'::double precision","k>'Infinity'::double precision","k='-Infinity'::double precision",'k IS NULL','k IS NOT NULL',"k=ANY(ARRAY['NaN'::double precision,-0.0::double precision,1.25::double precision,NULL])",'k=ANY(ARRAY[]::real[])','k=ANY(NULL::double precision[])','k=ANY(ARRAY[NULL]::double precision[])',"k<='1e-30'::double precision"]:equal(pred)
        query(c,'PREPARE float_key(double precision) AS SELECT id,encode('+send+"(k),'hex'),note FROM biz.keys WHERE k>=$1 ORDER BY id")
        for v in ['0','-1.25','1.25','1.00000006','NaN','Infinity','-Infinity','0']:
            got=query(c,'EXECUTE float_key(%s)',(v,));want=query(c,'SELECT id,encode('+send+"(k),'hex'),note FROM biz.exact WHERE k>=%s::double precision ORDER BY id",(v,));check('prepared cross-float ordered bound '+v,got,want)
        check('NULL ordered parameter returns no rows',query(c,'EXECUTE float_key(NULL)'),[]);query(c,'DEALLOCATE float_key')
        query(c,'BEGIN');query(c,"UPDATE biz.keys SET k='NaN',note='private' WHERE id=1");query(c,'ROLLBACK');equal('k IS NOT NULL',label='private rollback restores original float rows')
        for value in ['-0','NaN']:both('UPDATE {table} SET k='+literal(value)+'::'+typ+",note='updated' WHERE id=1");equal('k IS NOT NULL',label='twice updated key '+value)
        query(c,'BEGIN');query(c,'UPDATE og_lake_internal.index_segment SET algorithm_version=1 WHERE index_uuid=%s::uuid',(uuid,));equal('k IS NOT NULL',label='obsolete float codec falls back before original-row evaluation');check('obsolete codec is never reported as file coverage','Verified Scalar File Coverage: 0'in plan('old-codec','k IS NOT NULL'));query(c,'ROLLBACK')
        equal('k IS NOT NULL',label='rollback resumes current float codec');check('correct codec coverage resumes','Verified Scalar File Coverage: 1'in plan('restored-codec'))
        check('visible row population preserved',scalar(c,'SELECT count(*) FROM biz.keys'),scalar(c,'SELECT count(*) FROM biz.exact'))
        check('test inputs unchanged',all(sha256(f)==h for f,h in r['inputs'].items()));check('provider identity unchanged',sha256(provider),r['provider_sha256']);r['artifacts']={str(f):sha256(f)for f in out.rglob('*.parquet')};check_build(a.build_receipt);r['status']='pass'
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
