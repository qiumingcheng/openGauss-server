#!/usr/bin/env python3
"""Real native lake file DiskANN selection with original-row global SQL rerank.

Small deterministic files test native integration, not general ANN recall,
large-scale speed, PQ, persistent bitmap, SMP, remote or target-RAC readiness.
The optional fusion-ann mode tests one builtin scalar predicate as a source
column position mask; it does not claim arbitrary boolean filter support.
"""
import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import re
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
    p.add_argument('--build-receipt',type=Path,required=True);p.add_argument('--metric',choices=['l2','ip','cosine'],default='l2')
    p.add_argument('--storage',choices=['heap','ustore'],default='heap');p.add_argument('--permissions',action='store_true');p.add_argument('--fusion-exact',action='store_true');p.add_argument('--fusion-ann',action='store_true');p.add_argument('--null-scalar',action='store_true');p.add_argument('--scalar-type',choices=['integer','bigint','real','double precision'],default='integer')
    a=p.parse_args();validate_run_id(a.run_id)
    if PROFILE!='ann-debug':p.error('Requires matching separately owned ann-debug catalogs')
    out=ROOT/'公共文档/openGauss原生湖存储/验证'/a.run_id;out.mkdir(exist_ok=False)
    r=dict(status='fail',scope=__doc__,metric=a.metric,storage=a.storage,scalar_type=a.scalar_type,null_scalar=a.null_scalar,permissions=a.permissions,fusion_exact=a.fusion_exact,fusion_ann=a.fusion_ann,assertions=[]);control=db=roles=t=None
    files=[Path(__file__),D/'tests/file_dml_acceptance.py',D/'tests/native_table_create.py',D/'tests/permission_api.py',D/'native_test_context.py',
        D/'native_runtime.py',D/'tests/inline_cleanup.py',D/'inline_fixture.py',D/'tests/native_auto_flush.py',D/'source-lock.json']
    r['inputs']={str(f):sha256(f)for f in files};archive=out/'executed-inputs';archive.mkdir()
    for f in files:shutil.copy2(f,archive/f.name)
    provider=ROOT/'.install/opengauss-native-lake-debug/lib/postgresql/og_lake_sdk/libog_lake_sdk_arrow.so';r['provider_sha256']=sha256(provider)
    def image(v):return dict(count=len(v),sha256=hashlib.sha256(repr(v).encode()).hexdigest())if isinstance(v,list)and len(v)>30 else v
    def check(name,got,want=True):
        good=got==want;r['assertions'].append(dict(name=name,observed=image(got),expected=image(want),status='pass'if good else'fail'))
        if not good:raise AssertionError(name+': '+repr(got))
    try:
        b=check_build(a.build_receipt);r.update(source_head=b['source_head'],binary_sha256=b['binary_sha256'])
        control=connect_control();check('automatic maintenance stays off',scalar(control,'SHOW enable_lake_maintenance'),'off')
        with(out/'fixture.log').open('w')as log:
            subprocess.run([sys.executable,str(D/'tests/catalog_fixture_bundle.py'),'--run-id',a.run_id,'--output',str(out/'fixture/manifest.json')],stdout=log,stderr=subprocess.STDOUT,check=True,timeout=300)
        db=OwnedDatabase(a.run_id,out).create();roles=OwnedRoles(control,a.run_id,out)
        t=Case(db,SimpleNamespace(mode='net_dml',sql_version='0.16',manifest=out/'fixture/manifest.json',output=out/'result.json',run_id=a.run_id),r,roles)
        admin=c=t.a
        if a.permissions:
            from psycopg2 import sql
            owner=roles.create('business');reader=roles.create('reader');limited=roles.create('limited')
            query(admin,sql.SQL('GRANT USAGE,CREATE ON SCHEMA biz TO {}').format(sql.Identifier(owner)))
            query(admin,sql.SQL('GRANT USAGE ON FOREIGN SERVER created_lake TO {}').format(sql.Identifier(owner)))
            for name in [reader,limited]:query(admin,sql.SQL('GRANT USAGE ON SCHEMA biz TO {}').format(sql.Identifier(name)))
            c,_=roles.connect(db,'business')
        query(c,'SET statement_timeout=60000')
        query(c,'CREATE TABLE biz.exact(id '+a.scalar_type+',v vector(3),note text)')
        if a.storage=='ustore':query(c,'SET enable_default_ustore_table=on')
        query(c,"CREATE FOREIGN TABLE biz.vectors(id "+a.scalar_type+",v vector(3),note text) SERVER created_lake OPTIONS(binding_mode 'create',lake_schema 'main')")
        query(c,'SET enable_default_ustore_table=off')
        query(c,"SELECT og_lake.set_option('biz.vectors'::regclass,'data_inlining_row_limit','50000')")
        carriers=query(admin,"SELECT c.reloptions FROM created_metadata.ducklake_inlined_data_tables t JOIN created_metadata.ducklake_table d USING(table_id) JOIN pg_class c ON c.relname=t.table_name AND c.relnamespace=(SELECT oid FROM pg_namespace WHERE nspname='created_metadata') WHERE d.table_name='vectors' AND d.end_snapshot IS NULL")
        check('requested physical inline carrier exists',len(carriers),1)
        if carriers:check('actual carrier AM matches requested storage','storage_type=ustore' in (carriers[0][0]or []),a.storage=='ustore')
        def both(statement):
            query(c,'BEGIN')
            for table in ['biz.exact','biz.vectors']:query(c,statement.format(table=table))
            query(c,'COMMIT')
        for first,last in [(1,32),(33,64)]:
            both("INSERT INTO {table} SELECT i,('['||i::text||','||(i*i/100.0)::text||','||(i%13+.125)::text||']')::vector,'row/'||i::text FROM generate_series("+str(first)+','+str(last)+')i')
            if a.fusion_ann and a.null_scalar and first==1:both("INSERT INTO {table} VALUES(NULL,'[5,3,7]'::vector,'NULL scalar')")
            query(admin,"SELECT og_lake.flush('biz.vectors'::regclass)")
        both("INSERT INTO {table} VALUES(65,NULL,'null'),(66,'[0,0,0]'::vector,'zero'),(7777,'[5,3,7]'::vector,'dup'),(7777,'[5,3,7]'::vector,'dup')")
        spec=dict(version=1,kind='vector',method='diskann',column='v',metric=a.metric,enable_pq=False)
        uuid=str(scalar(c,"SELECT og_lake.create_lake_index('biz.vectors'::regclass,'ix_file_query',%s::jsonb)",(json.dumps(spec),)))
        store=out/'index-store';store.mkdir();query(admin,"SELECT og_lake.set_option('created_lake'::name,'index_root',%s::text)",(str(store),))
        before=t.head();query(c,'SELECT og_lake.build_lake_index(%s::uuid)',(uuid,));check('graph build creates no business snapshot',t.head(),before)
        check('only heap carrier registers a native inline ANN index',bool(scalar(admin,'SELECT inline_index_oid IS NOT NULL FROM og_lake_internal.index_definition WHERE index_uuid=%s::uuid',(uuid,))),a.storage=='heap')
        segments=query(admin,'SELECT segment_uuid::text,manifest_hash FROM og_lake_internal.index_segment WHERE index_uuid=%s::uuid ORDER BY segment_uuid',(uuid,))
        query(c,'SELECT og_lake.build_lake_index(%s::uuid)',(uuid,))
        check('repeat build retains immutable file segments',query(admin,'SELECT segment_uuid::text,manifest_hash FROM og_lake_internal.index_segment WHERE index_uuid=%s::uuid ORDER BY segment_uuid',(uuid,)),segments)
        if a.storage=='ustore':
            query(c,'SET enable_default_ustore_table=on')
            query(c,"CREATE FOREIGN TABLE biz.only_ustore(id integer,v vector(3)) SERVER created_lake OPTIONS(binding_mode 'create',lake_schema 'main')")
            query(c,'SET enable_default_ustore_table=off')
            query(c,"INSERT INTO biz.only_ustore VALUES(1,'[1,2,3]'::vector)")
            only=str(scalar(c,"SELECT og_lake.create_lake_index('biz.only_ustore'::regclass,'ix_unsupported',%s::jsonb)",(json.dumps(spec),)))
            horizon=t.head()
            try:query(c,'SELECT og_lake.build_lake_index(%s::uuid)',(only,))
            except Exception as exc:check('pure inline Ustore native ANN remains explicitly unsupported',exc.pgcode,'0A000')
            else:check('pure inline Ustore rejects unqualified ANN adapter',False)
            check('unsupported inline build publishes no file segments',scalar(admin,'SELECT count(*) FROM og_lake_internal.index_segment WHERE index_uuid=%s::uuid',(only,)),0)
            check('unsupported inline build publishes no business snapshot',t.head(),horizon)
            check('same connection reads Ustore row after rejection',scalar(c,'SELECT count(*) FROM biz.only_ustore'),1)
        query(c,'SET enable_lake_ann=on');query(c,'SET enable_seqscan=off')
        operator={'l2':'<->','ip':'<#>','cosine':'<=>'}[a.metric]
        where='v IS NOT NULL'+(' AND vector_norm(v)>0'if a.metric=='cosine'else'')
        if a.fusion_ann:where+=' AND id>=1::'+a.scalar_type
        def statement(table='biz.vectors',prepared=False,limit=10):
            arg='$1'if prepared else'%s::vector'
            return "SELECT id,encode(vector_send(v),'hex'),note,v "+operator+' '+arg+' AS distance FROM '+table+' WHERE '+where+' ORDER BY v '+operator+' '+arg+' LIMIT '+str(limit)
        def equivalent(label,value='[5,3,7]',limit=10):
            actual=query(c,statement(limit=limit),(value,value));expected=query(c,statement('biz.exact',limit=limit),(value,value))
            check(label+' global original-vector SQL distances',[x[3]for x in actual],[x[3]for x in expected])
            original=Counter(query(c,"SELECT id,encode(vector_send(v),'hex'),note FROM biz.exact"));got=Counter(x[:3]for x in actual)
            check(label+' original values and multiplicity',all(number<=original[row]for row,number in got.items()))
            return actual
        def plan(label,value='[5,3,7]',limit=10):
            text='\n'.join(x[0]for x in query(c,'EXPLAIN (ANALYZE,COSTS OFF) '+statement(limit=limit),(value,value)))
            (out/(label+'-plan.txt')).write_text(text+'\n')
            match=re.search(r'Verified ANN File Coverage: (\d+)',text);return text,int(match.group(1))if match else-1
        query(c,'SET client_min_messages=debug1');c.notices.clear()
        equivalent('two files plus inline')
        text,count=plan('initial');check('both lake files actually use verified DiskANN candidates',count,2)
        check('final native executor reranks original vectors','Sort'in text and'ANN Semantics:'in text)
        if a.permissions:
            query(c,sql.SQL('GRANT SELECT ON biz.vectors TO {}').format(sql.Identifier(reader)))
            query(c,sql.SQL('GRANT SELECT(id,note) ON biz.vectors TO {}').format(sql.Identifier(limited)))
            read,_=roles.connect(db,'reader');column,_=roles.connect(db,'limited')
            for conn in [read,column]:query(conn,'SET enable_lake_ann=on');query(conn,'SET enable_seqscan=off')
            want=query(c,statement(),('[5,3,7]','[5,3,7]'))
            check('ordinary SELECT grantee original-row distances',query(read,statement(),('[5,3,7]','[5,3,7]')),want)
            text='\n'.join(x[0]for x in query(read,'EXPLAIN (ANALYZE,COSTS OFF) '+statement(),('[5,3,7]','[5,3,7]')))
            check('SELECT grantee actually executes two file ANN sources','Verified ANN File Coverage: 2'in text)
            def denied(label,conn,statement,args=None):
                try:query(conn,statement,args)
                except Exception as exc:check(label,exc.pgcode,'42501')
                else:check(label,False)
            denied('SELECT grantee cannot build',read,'SELECT og_lake.build_lake_index(%s::uuid)',(uuid,))
            denied('SELECT grantee cannot inspect private status',read,'SELECT og_lake.lake_index_status(%s::uuid)',(uuid,))
            denied('SELECT grantee cannot drop',read,'SELECT og_lake.drop_lake_index(%s::uuid)',(uuid,))
            denied('column privilege cannot expose vector',column,statement(),('[5,3,7]','[5,3,7]'))
            denied('ordinary owner cannot read private catalog',c,'SELECT * FROM og_lake_internal.index_definition')
            check('same grantee reusable after denied operations',query(read,statement(),('[5,3,7]','[5,3,7]')),want)

        allrows=equivalent('duplicate business rows are retained',limit=1000)
        check('identity dedup never collapses equal business rows',sum(1 for row in allrows if row[0]==7777),2)
        text,count=plan('large-limit',limit=1000);check('unique physical candidates below SQL K require full-file fallback',count,0)
        for q in ['[19,6,2]','[-1,-2,-3]','[40,20,10]']:equivalent('query '+q,q)
        query(c,'PREPARE file_ann(vector) AS '+statement(prepared=True))
        for q in ['[5,3,7]','[19,6,2]','[7,8,9]','[1,2,3]','[40,20,10]','[3,4,5]','[5,3,7]']:
            got=query(c,'EXECUTE file_ann(%s)',(q,));expected=query(c,statement('biz.exact'),(q,q))
            check('prepared original distance '+q,[x[3]for x in got],[x[3]for x in expected])
        query(c,'SET enable_lake_ann=off');got=query(c,"EXECUTE file_ann('[5,3,7]')")
        check('cached plan honors ANN off',[x[3]for x in got],[x[3]for x in query(c,statement('biz.exact'),('[5,3,7]','[5,3,7]'))])
        query(c,'SET enable_lake_ann=on')
        got=query(c,'EXECUTE file_ann(NULL)');check('NULL parameter retains full SQL population',len(got),10);check('NULL public distances preserved',all(x[3]is None for x in got))
        if a.metric=='cosine':
            import math
            got=query(c,"EXECUTE file_ann('[0,0,0]')");check('zero cosine parameter uses full SQL population',len(got),10);check('zero query preserves native NaN distance',all(math.isnan(x[3])for x in got))
        query(c,'DEALLOCATE file_ann')
        query(c,'SET statement_timeout=20')
        try:query(c,"SELECT pg_sleep(0.03),id,v "+operator+" '[5,3,7]'::vector AS d FROM biz.vectors WHERE "+where+" ORDER BY d LIMIT 10")
        except Exception as exc:check('actual file ANN upper cancellation releases scan resources',exc.pgcode,'57014')
        else:check('file ANN cancellation was observed',False)
        query(c,'SET statement_timeout=60000');equivalent('same session after file ANN cancellation')
        try:query(c,"SELECT id,1/(id-id),v "+operator+" '[5,3,7]'::vector AS d FROM biz.vectors WHERE "+where+" ORDER BY d LIMIT 10")
        except Exception as exc:check('actual file ANN upper executor error releases scan resources',exc.pgcode,'22012')
        else:check('upper executor division error was observed',False)
        equivalent('same session after file ANN upper error')

        if a.fusion_exact:
            scalar_spec=dict(version=1,kind='scalar',method='btree',column='id')
            scalar_uuid=str(scalar(c,"SELECT og_lake.create_lake_index('biz.vectors'::regclass,'ix_scalar_topk',%s::jsonb)",(json.dumps(scalar_spec),)))
            query(c,'SELECT og_lake.build_lake_index(%s::uuid)',(scalar_uuid,))
            for label,predicate in [('file first','id=17'),('file second','id=40'),('duplicate inline','id=7777'),('range','id>=15 AND id<=55')]:
                q="SELECT id,encode(vector_send(v),'hex'),note,v "+operator+" '[5,3,7]'::vector AS d FROM {} WHERE "+where+' AND '+predicate+' ORDER BY d,id LIMIT 10'
                got=query(c,q.format('biz.vectors'));want=query(c,q.format('biz.exact'))
                check('scalar-first exact TopK '+label,got,want)
                text='\n'.join(x[0]for x in query(c,'EXPLAIN (ANALYZE,COSTS OFF) '+q.format('biz.vectors')))
                (out/('scalar-topk-'+label.replace(' ','-')+'-plan.txt')).write_text(text+'\n')
                check('scalar-first exact path uses verified file positions '+label,'Actual File Access: verified sorted Parquet candidates'in text and 'Verified Scalar File Coverage: 2'in text)
                check('scalar-first retains native SQL distance rerank '+label,'Sort'in text and 'ANN Semantics:'not in text)
        if a.fusion_ann:
            check('filtered plan exposes its actual scalar mask source','ANN Scalar Filter: native scalar column scan to physical-position mask' in text)
            # Every emitted candidate still passes the original native SQL
            # filter and distance. No persistent scalar index is assumed.
            base='v IS NOT NULL'+(' AND vector_norm(v)>0'if a.metric=='cosine'else'')
            def filtered(table,predicate,arg='%s::vector',limit=10):
                return "SELECT id,encode(vector_send(v),'hex'),note,v "+operator+' '+arg+' AS d FROM '+table+' WHERE '+base+' AND '+predicate+' ORDER BY d LIMIT '+str(limit)
            for label,predicate in [('right bridge','id>=60'),('left subset','id<20'),('single row','id=40'),('empty mask','id<0'),('NULL key','id IS NULL'),('NOT NULL key','id IS NOT NULL'),('commuted cross type',('10::bigint<id'if a.scalar_type in ['integer','bigint']else'10::double precision<id'))]:
                if a.scalar_type in ['real','double precision']:predicate=re.sub(r'(id[<>=]+)(-?\d+)',lambda m:m[1]+m[2]+'::'+a.scalar_type,predicate)
                q=filtered('biz.vectors',predicate);got=query(c,q,('[5,3,7]',));want=query(c,filtered('biz.exact',predicate),('[5,3,7]',))
                check('filtered original rows and distances '+label,got,want)
                plantext='\n'.join(x[0]for x in query(c,'EXPLAIN (ANALYZE,COSTS OFF) '+q,('[5,3,7]',)))
                (out/('filter-'+label.replace(' ','-')+'-plan.txt')).write_text(plantext+'\n')
                check('actual filtered ANN plan '+label,'ANN Scalar Filter:'in plantext)
                check('filtered inline retains complete TableAM '+label,'Actual Inline Access: TableAM sequential'in plantext)
                if label=='right bridge':check('sparse matching file below K is complete fallback','Verified ANN File Coverage: 1'in plantext)
            query(c,'SET plan_cache_mode=force_generic_plan')
            query(c,'PREPARE filtered_ann(vector,'+('bigint'if a.scalar_type in ['integer','bigint']else'double precision')+') AS '+filtered('biz.vectors','id>=$2','$1'))
            bounds=[1,60,20,40,1,99999,1,None]
            if a.scalar_type in ['real','double precision']:bounds +=[16.0000001,float('-inf'),float('inf'),float('nan')]
            for bound in bounds:
                got=query(c,'EXECUTE filtered_ann(%s,%s)',('[5,3,7]',bound));want=query(c,filtered('biz.exact','id>=%s'),('[5,3,7]',bound))
                check('prepared vector and scalar parameter '+str(bound),got,want)
            generic='\n'.join(x[0]for x in query(c,"EXPLAIN (ANALYZE,COSTS OFF) EXECUTE filtered_ann('[5,3,7]',1)"))
            (out/'filtered-generic-plan.txt').write_text(generic+'\n');check('generic filtered plan retains native mask','ANN Scalar Filter:'in generic and '$2'in generic)
            query(c,'SET enable_lake_ann=off');check('cached filtered plan honors ANN off',query(c,"EXECUTE filtered_ann('[5,3,7]',60)"),query(c,filtered('biz.exact','id>=60'),('[5,3,7]',)))
            off='\n'.join(x[0]for x in query(c,"EXPLAIN (ANALYZE,COSTS OFF) EXECUTE filtered_ann('[5,3,7]',60)"))
            check('cached generic ANN off executes complete files','Verified ANN File Coverage: 0'in off)
            query(c,'SET enable_lake_ann=on');query(c,'DEALLOCATE filtered_ann');query(c,'SET plan_cache_mode=auto')
            for predicate in ['id>=1 AND id<50','id=1 OR id=40',"note='row/40'"]:
                q=filtered('biz.vectors',predicate);check('unsupported mask shape preserves complete SQL '+predicate,query(c,q,('[5,3,7]',)),query(c,filtered('biz.exact',predicate),('[5,3,7]',)))
                check('unsupported shape never claims filtered ANN '+predicate,'ANN Scalar Filter:'not in '\n'.join(x[0]for x in query(c,'EXPLAIN (ANALYZE,COSTS OFF) '+q,('[5,3,7]',))))
        query(c,'BEGIN')
        for table in ['biz.exact','biz.vectors']:query(c,"UPDATE "+table+" SET v='[5,3,7.25]'::vector,note='private update' WHERE id=1")
        equivalent('private file after-image');text,count=plan('private');check('private hides conservatively scan affected files',count,0)
        query(c,'ROLLBACK');equivalent('private rollback');text,count=plan('after-rollback');check('healthy file indexes resume after rollback',count,2)
        both("UPDATE {table} SET v='[5,3,7.5]'::vector,note='first update' WHERE id=1")
        both("UPDATE {table} SET v='[5,3,7.75]'::vector,note='second update' WHERE id=1")
        equivalent('committed second update');text,count=plan('committed-delete');check('one ended source falls back while other stays indexed',count,1)
        both("INSERT INTO {table} VALUES(9001,'[5,3,7.01]'::vector,'uncovered source')")
        query(admin,"SELECT og_lake.flush('biz.vectors'::regclass)");equivalent('uncovered new file competes in global rerank')
        text,count=plan('uncovered');check('uncovered inputs are explicitly retained','Complete File Fallback Inputs:'in text and count>=1)
        # Damage only an artifact created by this exact owned test. A complete
        # fallback must be chosen before a single business row is emitted.
        active=query(admin,'SELECT s.artifact_root_uri FROM og_lake_internal.index_segment s JOIN og_lake_internal.index_coverage c USING(segment_uuid,index_uuid,definition_generation) WHERE c.index_uuid=%s::uuid AND c.active ORDER BY c.source_file_id',(uuid,))
        healthy=Path(active[1][0]);lookup=healthy/'ann_lookup.parquet';shutil.copy2(lookup,out/'removed-owned-lookup.parquet');lookup.unlink()
        equivalent('missing derived graph uses complete source');text,count=plan('missing-graph');check('damaged source is removed from verified coverage',count,0)
        check('missing graph leaves original business population',scalar(c,'SELECT count(*) FROM biz.vectors'),scalar(c,'SELECT count(*) FROM biz.exact'))
        check('executed input bytes unchanged',all(sha256(f)==h for f,h in r['inputs'].items()))
        check('SDK identity unchanged',sha256(provider),r['provider_sha256'])
        r['artifacts']={str(f):sha256(f)for f in out.rglob('*.parquet')}
        check_build(a.build_receipt);r['status']='pass'
    except BaseException as exc:r.update(reason=str(exc),traceback=traceback.format_exc())
    finally:
        if t is not None:r['native_notices']=list(c.notices)[-50:]
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
