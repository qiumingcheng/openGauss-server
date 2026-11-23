#!/usr/bin/env python3
"""Opt-in heap-inline native DiskANN with lake version/private/file rechecks.

Requires the separately owned same-build ann-debug deployment. Non-PQ and
single-instance RC only; no file ANN, Ustore ANN, SMP or commercial claim.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import traceback
from types import SimpleNamespace
ROOT=Path('/home/qiumc/data/code/cpp');D=ROOT/'integration/opengauss-native-lake'
sys.path[:0]=[str(D),str(D/'tests')]
from file_dml_acceptance import check_build
from inline_fixture import query,scalar
from lake_harness import sha256,validate_run_id,write_json
from native_auto_flush import connect_control
from native_runtime import PROFILE
from native_test_context import OwnedDatabase
from native_table_create import Case
from permission_api import OwnedRoles,cleanup_roles


def guard_cases(t,c,uuid,spec,metric,check,equivalent,statement,out):
    import math
    operator={'l2':'<->','ip':'<#>','cosine':'<=>'}[metric]
    q="'[5,3,7]'::vector";positive='v IS NOT NULL'+(' AND vector_norm(v)>0'if metric=='cosine'else'')
    def canonical(rows):
        return sorted((tuple('NaN'if isinstance(v,float)and math.isnan(v)else v for v in row)for row in rows),key=repr)
    patterns=[
      ('extra scalar filter','SELECT id,v '+operator+' '+q+' AS d FROM {table} WHERE '+positive+' AND id%2=0 ORDER BY d LIMIT100'),
      ('descending order','SELECT id,v '+operator+' '+q+' AS d FROM {table} WHERE '+positive+' ORDER BY d DESC LIMIT100'),
      ('missing indexable filter','SELECT id,v '+operator+' '+q+' AS d FROM {table} ORDER BY d LIMIT100'),
      ('join','SELECT id,v '+operator+' '+q+' AS d FROM {table} x JOIN (SELECT 1 AS dummy) y ON true WHERE '+positive+' ORDER BY d LIMIT100'),
      ('CTE','WITH x AS(SELECT * FROM {table}) SELECT id,v '+operator+' '+q+' AS d FROM x WHERE '+positive+' ORDER BY d LIMIT100'),
      ('distinct','SELECT DISTINCT id,v '+operator+' '+q+' AS d FROM {table} WHERE '+positive+' ORDER BY d LIMIT100'),
      ('aggregate','SELECT count(*),min(v '+operator+' '+q+') FROM {table} WHERE '+positive),
    ]
    for name,sql in patterns:
        sql=sql.replace('LIMIT100','LIMIT 100');actual=sql.format(table='biz.vectors');reference=sql.format(table='biz.exact')
        check(name+' complete rows',canonical(query(c,actual)),canonical(query(c,reference)))
        plan='\n'.join(x[0]for x in query(c,'EXPLAIN (ANALYZE,COSTS OFF) '+actual))
        check(name+' full scan admission','ANN Semantics:'not in plan and'Planned Inline Access: TableAM sequential'in plan)
    query(c,'PREPARE guarded_ann(vector) AS '+statement('biz.vectors',True))
    for i in range(7):query(c,"EXECUTE guarded_ann('[5,3,7]')")
    result=query(c,'EXECUTE guarded_ann(NULL)');check('NULL query retains LIMIT population',len(result),10)
    check('NULL query distances remain NULL',all(x[3]is None for x in result))
    if metric=='cosine':
        result=query(c,"EXECUTE guarded_ann('[0,0,0]')")
        check('zero cosine query full population',len(result),10)
        check('zero cosine SQL NaN semantics',all(math.isnan(x[3])for x in result))
    try:query(c,"EXECUTE guarded_ann('[1,2]')")
    except Exception as exc:check('dimension error matches native SQL',exc.pgcode,'22000')
    else:check('dimension error is raised',False)
    equivalent('same connection after bad parameter');query(c,'DEALLOCATE guarded_ann')
    for i in range(3):
        try:query(c,'SELECT id,1/(id-id),v '+operator+' '+q+' FROM biz.vectors WHERE '+positive+' ORDER BY v '+operator+' '+q+' LIMIT 10')
        except Exception as exc:check('upper executor error '+str(i),exc.pgcode,'22012')
        else:check('upper executor error raised '+str(i),False)
        equivalent('query resources after executor error '+str(i))
    query(c,'SET statement_timeout=20')
    try:query(c,'SELECT pg_sleep(0.03),v '+operator+' '+q+' FROM biz.vectors WHERE '+positive+' ORDER BY v '+operator+' '+q+' LIMIT 10')
    except Exception as exc:check('cancelled query releases resources',exc.pgcode,'57014')
    else:check('cancellation was observed',False)
    query(c,'SET statement_timeout=15000');equivalent('same connection after cancellation')
    query(c,"CREATE FOREIGN TABLE biz.empty_ann(id integer,v vector(3),note text) SERVER created_lake OPTIONS(binding_mode 'create',lake_schema 'main')")
    empty=str(scalar(c,"SELECT og_lake.create_lake_index('biz.empty_ann'::regclass,'ix_empty',%s::jsonb)",(json.dumps(spec),)))
    query(c,'SELECT og_lake.build_lake_index(%s::uuid)',(empty,))
    query(c,"INSERT INTO biz.empty_ann VALUES(1,NULL,'null'),(2,'[0,0,0]'::vector,'zero'),(3,'[5,3,7]'::vector,'first nonzero')")
    check('empty lake index accepts nullable rows',scalar(c,'SELECT count(*) FROM biz.empty_ann'),3)
    got=query(c,'SELECT id FROM biz.empty_ann WHERE '+positive+' ORDER BY v '+operator+' '+q+' LIMIT 10')
    check('empty lake index gains live first vertex',any(x[0]==3 for x in got))


def permission_cases(t,c,uuid,spec,metric,check,equivalent,statement,out):
    from psycopg2 import sql
    owner=t.roles.create('business');reader=t.roles.create('reader');limited=t.roles.create('limited')
    query(c,sql.SQL('GRANT USAGE,CREATE ON SCHEMA biz TO {}').format(sql.Identifier(owner)))
    query(c,sql.SQL('GRANT USAGE ON FOREIGN SERVER created_lake TO {}').format(sql.Identifier(owner)))
    query(c,sql.SQL('GRANT SELECT ON biz.vectors TO {}').format(sql.Identifier(owner)))
    for role in [reader,limited]:query(c,sql.SQL('GRANT USAGE ON SCHEMA biz TO {}').format(sql.Identifier(role)))
    ordinary,_=t.roles.connect(t.db,'business');read,_=t.roles.connect(t.db,'reader');column,_=t.roles.connect(t.db,'limited')
    query(ordinary,"CREATE FOREIGN TABLE biz.owned_ann(id integer,v vector(3),note text) SERVER created_lake OPTIONS(binding_mode 'create',lake_schema 'main')")
    query(ordinary,"INSERT INTO biz.owned_ann SELECT * FROM biz.vectors")
    own=str(scalar(ordinary,"SELECT og_lake.create_lake_index('biz.owned_ann'::regclass,'owned_ix',%s::jsonb)",(json.dumps(spec),)))
    query(ordinary,'SELECT og_lake.build_lake_index(%s::uuid)',(own,))
    query(c,sql.SQL('GRANT SELECT ON biz.owned_ann TO {}').format(sql.Identifier(reader)))
    query(c,sql.SQL('GRANT SELECT(id,note) ON biz.owned_ann TO {}').format(sql.Identifier(limited)))
    for conn in [ordinary,read,column]:query(conn,'SET enable_lake_ann=on');query(conn,'SET enable_seqscan=off')
    want=query(c,statement('biz.owned_ann'),('[5,3,7]','[5,3,7]'))
    check('ordinary owner ANN values',query(ordinary,statement('biz.owned_ann'),('[5,3,7]','[5,3,7]')),want)
    check('SELECT grantee ANN values',query(read,statement('biz.owned_ann'),('[5,3,7]','[5,3,7]')),want)
    for name,conn in [('owner',ordinary),('SELECT grantee',read)]:
        plan='\n'.join(x[0]for x in query(conn,'EXPLAIN (ANALYZE,COSTS OFF) '+statement('biz.owned_ann'),('[5,3,7]','[5,3,7]')))
        check(name+' actually uses ANN','Actual Inline Access: native DiskANN approximate candidates'in plan)
    def denied(name,conn,sql,params=None):
        try:query(conn,sql,params)
        except Exception as exc:check(name,exc.pgcode,'42501')
        else:check(name,False)
    denied('reader cannot build index',read,'SELECT og_lake.build_lake_index(%s::uuid)',(own,))
    denied('reader cannot inspect private status',read,'SELECT og_lake.lake_index_status(%s::uuid)',(own,))
    denied('reader cannot drop definition',read,'SELECT og_lake.drop_lake_index(%s::uuid)',(own,))
    denied('reader cannot define index',read,"SELECT og_lake.create_lake_index('biz.owned_ann'::regclass,'bad',%s::jsonb)",(json.dumps(spec),))
    denied('column grants cannot read vector',column,statement('biz.owned_ann'),('[5,3,7]','[5,3,7]'))
    denied('ordinary role cannot read private definitions',ordinary,'SELECT * FROM og_lake_internal.index_definition')
    check('owner identity restored',scalar(ordinary,'SELECT current_user'),owner)
    query(ordinary,'SELECT og_lake.drop_lake_index(%s::uuid)',(own,))
    check('reader complete fallback after retire',query(read,statement('biz.owned_ann'),('[5,3,7]','[5,3,7]')),want)


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--run-id',required=True)
    p.add_argument('--build-receipt',type=Path,required=True);p.add_argument('--metric',choices=['l2','ip','cosine'],default='l2')
    p.add_argument('--mode',choices=['core','guards','permissions'],default='core')
    a=p.parse_args();validate_run_id(a.run_id)
    if PROFILE!='ann-debug':p.error('Use OG_LAKE_TEST_PROFILE=ann-debug; never patch the original runtime catalogs')
    out=ROOT/'公共文档/openGauss原生湖存储/验证'/a.run_id;out.mkdir(exist_ok=False)
    r={'status':'fail','scope':__doc__,'metric':a.metric,'mode':a.mode,'assertions':[]};control=db=roles=t=None
    files=[Path(__file__),D/'tests/file_dml_acceptance.py',D/'tests/native_table_create.py',D/'tests/permission_api.py',
        D/'native_test_context.py',D/'native_runtime.py',D/'tests/inline_cleanup.py',D/'inline_fixture.py',D/'tests/native_auto_flush.py',D/'source-lock.json']
    provider=ROOT/'.install/opengauss-native-lake-debug/lib/postgresql/og_lake_sdk/libog_lake_sdk_arrow.so'
    r['provider_sha256']=sha256(provider)
    r['inputs']={str(f):sha256(f)for f in files};archive=out/'executed-inputs';archive.mkdir()
    for f in files:shutil.copy2(f,archive/f.name)
    def image(value):
        return {'count':len(value),'sha256':hashlib.sha256(repr(value).encode()).hexdigest()}if isinstance(value,list)and len(value)>40 else value
    def check(name,got,want=True):
        good=got==want;r['assertions'].append(dict(name=name,observed=image(got),expected=image(want),status='pass'if good else'fail'))
        if not good:raise AssertionError(name+': '+repr(got))
    def rejected(c,name,statement,code,params=None):
        try:query(c,statement,params)
        except Exception as exc:check(name,getattr(exc,'pgcode',None),code)
        else:check(name,False)
    try:
        b=check_build(a.build_receipt);r.update(source_head=b['source_head'],binary_sha256=b['binary_sha256'])
        require=bool(b.get('compiled_build_receipt'));check('deployment retains official compiled build provenance',require)
        control=connect_control();check('ANN defaults off',scalar(control,'SHOW enable_lake_ann'),'off')
        with(out/'fixture.log').open('w')as log:
            subprocess.run([sys.executable,str(D/'tests/catalog_fixture_bundle.py'),'--run-id',a.run_id,'--output',str(out/'fixture/manifest.json')],stdout=log,stderr=subprocess.STDOUT,check=True,timeout=300)
        db=OwnedDatabase(a.run_id,out).create();roles=OwnedRoles(control,a.run_id,out)
        t=Case(db,SimpleNamespace(mode='net_dml',sql_version='0.16',manifest=out/'fixture/manifest.json',output=out/'result.json',run_id=a.run_id),r,roles)
        c=t.a;operator={'l2':'<->','ip':'<#>','cosine':'<=>'}[a.metric]
        query(c,'CREATE TABLE biz.exact(id integer,v vector(3),note text)')
        query(c,"CREATE FOREIGN TABLE biz.vectors(id integer,v vector(3),note text) SERVER created_lake OPTIONS(binding_mode 'create',lake_schema 'main')")
        query(c,"SELECT og_lake.set_option('biz.vectors'::regclass,'data_inlining_row_limit','50000')")
        query(c,"INSERT INTO biz.exact SELECT i,('['||i::text||','||(i*i/100.0)::text||','||(i%13+.125)::text||']')::vector,'n/'||i::text FROM generate_series(1,64)i")
        query(c,'INSERT INTO biz.exact VALUES(65,NULL,\'null\'),(66,\'[0,0,0]\'::vector,\'zero\')')
        query(c,'INSERT INTO biz.vectors SELECT * FROM biz.exact')
        spec=dict(version=1,kind='vector',method='diskann',column='v',metric=a.metric,enable_pq=False)
        uuid=str(scalar(c,"SELECT og_lake.create_lake_index('biz.vectors'::regclass,'ix_vector',%s::jsonb)",(json.dumps(spec),)))
        head=query(c,'SELECT * FROM created_metadata.ducklake_snapshot ORDER BY snapshot_id');r['snapshots_before_build']=head
        query(c,'SELECT og_lake.build_lake_index(%s::uuid)',(uuid,))
        check('building derived index does not publish business snapshot',query(c,'SELECT * FROM created_metadata.ducklake_snapshot ORDER BY snapshot_id'),head)
        index_oid=scalar(t.observer,'SELECT inline_index_oid FROM og_lake_internal.index_definition WHERE index_uuid=%s::uuid',(uuid,));r['inline_index_oid']=index_oid
        check('native method is DiskANN',scalar(t.observer,'SELECT relam FROM pg_class WHERE oid=%s',(index_oid,)),8303)
        query(c,'SET enable_seqscan=off');query(c,'SET enable_lake_ann=on')
        where='v IS NOT NULL'+(' AND vector_norm(v)>0'if a.metric=='cosine'else'')
        def statement(table,prepared=False):
            q='$1'if prepared else'%s::vector'
            return 'SELECT id,encode(vector_send(v),\'hex\'),note,v '+operator+' '+q+' AS distance FROM '+table+' WHERE '+where+' ORDER BY v '+operator+' '+q+' LIMIT 10'
        def rows(table='biz.vectors',q='[5,3,7]',conn=c):return query(conn,statement(table),(q,q))
        def equivalent(name,q='[5,3,7]',conn=c):
            got=rows(q=q,conn=conn);want=rows('biz.exact',q,conn)
            check(name+' distances',[x[3]for x in got],[x[3]for x in want]);check(name+' distinct identities',len({x[0]for x in got}),len(got))
            reference={x[0]:x for x in query(conn,"SELECT id,encode(vector_send(v),'hex'),note FROM biz.exact")}
            check(name+' visible original values',all(x[:3]==reference[x[0]]for x in got))
            return got
        equivalent('initial ANN candidates')
        plan='\n'.join(x[0]for x in query(c,'EXPLAIN (ANALYZE,COSTS OFF) '+statement('biz.vectors'),('[5,3,7]','[5,3,7]')))
        (out/'initial-plan.txt').write_text(plan+'\n');check('actual lake query uses native DiskANN','Actual Inline Access: native DiskANN approximate candidates'in plan)
        check('SQL distance is sorted by native executor','Sort'in plan and'ANN Semantics:'in plan)
        if a.mode!='core':
            (guard_cases if a.mode=='guards' else permission_cases)(t,c,uuid,spec,a.metric,check,equivalent,statement,out)
        else:
            def both(sql):
                query(c,'BEGIN')
                for table in ['biz.vectors','biz.exact']:query(c,sql.format(table=table))
                query(c,'COMMIT')
            both("INSERT INTO {table} VALUES(6001,NULL,'null after index'),(6002,'[0,0,0]'::vector,'zero after index')")
            equivalent('NULL and zero INSERT after index')
            check('unindexed nullable and zero values remain business rows',
                  query(c,'SELECT id,v IS NULL,vector_norm(v) FROM biz.vectors WHERE id IN(6001,6002) ORDER BY id'),
                  query(c,'SELECT id,v IS NULL,vector_norm(v) FROM biz.exact WHERE id IN(6001,6002) ORDER BY id'))
            both("UPDATE {table} SET v=NULL,note='now null' WHERE id=5");equivalent('indexed vector updated to NULL')
            both("UPDATE {table} SET v='[0,0,0]'::vector,note='now zero' WHERE id=6");equivalent('indexed vector updated to zero')
            both("UPDATE {table} SET v='[5,3,7]'::vector,note='restored' WHERE id=5");equivalent('NULL vector updated to indexed value')
            both("UPDATE {table} SET v='[5,3,7]'::vector,note='update1' WHERE id=1")
            both("UPDATE {table} SET v='[5,3,7.25]'::vector,note='update2' WHERE id=1")
            both('DELETE FROM {table} WHERE id=2');equivalent('committed repeated update and delete')
            query(c,'BEGIN')
            for table in ['biz.vectors','biz.exact']:query(c,"INSERT INTO "+table+" VALUES(9001,'[5,3,7]'::vector,'private')")
            equivalent('private after-image merged')
            query(c,'SAVEPOINT ann_child')
            for table in ['biz.vectors','biz.exact']:query(c,'DELETE FROM '+table+' WHERE id=1')
            equivalent('private tombstone hides indexed old version');query(c,'ROLLBACK TO SAVEPOINT ann_child');equivalent('savepoint restores indexed version')
            query(c,'ROLLBACK');equivalent('rollback restores committed candidates')
            query(c,'PREPARE lake_ann(vector) AS '+statement('biz.vectors',True))
            for i,q in enumerate(['[5,3,7]','[19,6,2]','[7,8,9]','[1,2,3]','[40,20,10]','[3,4,5]','[5,3,7]']):
                got=query(c,'EXECUTE lake_ann(%s)',(q,));want=rows('biz.exact',q)
                check('prepared parameter '+str(i),[x[3]for x in got],[x[3]for x in want])
            query(c,'SET enable_lake_ann=off');got=query(c,"EXECUTE lake_ann('[5,3,7]')");check('cached ANN plan disabled safely',[x[3]for x in got],[x[3]for x in rows('biz.exact')]);query(c,'SET enable_lake_ann=on');query(c,'DEALLOCATE lake_ann')
            # Retain the lake/native snapshot before a new committed update and flush.
            query(t.b,'SET enable_lake_ann=on');query(t.b,'SET enable_seqscan=off');query(t.b,'BEGIN');old=equivalent('old reader before migration',conn=t.b)
            both("UPDATE {table} SET v='[100,200,300]'::vector,note='moved' WHERE id=3")
            query(c,"SELECT og_lake.flush('biz.vectors'::regclass)");equivalent('exact file fallback after flush')
            check('retained indexed reader survives flush',rows(conn=t.b),old);query(t.b,'COMMIT')
            both("INSERT INTO {table} VALUES(9002,'[5,3,7]'::vector,'new inline')");equivalent('file plus newly indexed inline')
            both("UPDATE {table} SET v='[5,3,7.125]'::vector,note='file update' WHERE id=4");equivalent('file-origin private/committed overlay')
            check('stored definition preserves explicit non-PQ',scalar(t.observer,"SELECT definition->>'enable_pq' FROM og_lake_internal.index_definition WHERE index_uuid=%s::uuid",(uuid,)),'false')
            query(c,'SELECT og_lake.drop_lake_index(%s::uuid)',(uuid,));equivalent('retired index falls back completely')
            rejected(c,'PQ request remains rejected',"SELECT og_lake.create_lake_index('biz.vectors'::regclass,'bad_pq',%s::jsonb)",'0A000',(json.dumps({**spec,'enable_pq':True}),))
            # Definition can precede the build by several flush commits. Its
            # usable horizon must come from the locked build source, not CREATE.
            query(t.b,'BEGIN');before_late_build=rows(conn=t.b)
            late=str(scalar(c,"SELECT og_lake.create_lake_index('biz.vectors'::regclass,'ix_late',%s::jsonb)",(json.dumps(spec),)))
            original_horizon=scalar(t.observer,'SELECT registered_snapshot FROM og_lake_internal.index_definition WHERE index_uuid=%s::uuid',(late,))
            both("UPDATE {table} SET v='[777,888,999]'::vector,note='late build' WHERE id=6")
            query(c,"SELECT og_lake.flush('biz.vectors'::regclass)")
            query(c,'SELECT og_lake.build_lake_index(%s::uuid)',(late,))
            build_horizon=scalar(t.observer,'SELECT registered_snapshot FROM og_lake_internal.index_definition WHERE index_uuid=%s::uuid',(late,))
            check('usable backend horizon advances after late build',build_horizon>original_horizon)
            check('pre-build reader keeps original values',rows(conn=t.b),before_late_build)
            old_plan='\n'.join(x[0]for x in query(t.b,'EXPLAIN (ANALYZE,COSTS OFF) '+statement('biz.vectors'),('[5,3,7]','[5,3,7]')))
            (out/'old-build-horizon-plan.txt').write_text(old_plan+'\n')
            check('pre-build snapshot uses full native scan','Actual Inline Access: TableAM sequential'in old_plan)
            query(t.b,'COMMIT');equivalent('current reader after empty-carrier build')
        check('all executed inputs unchanged',all(sha256(f)==h for f,h in r['inputs'].items()))
        check('same SDK provider at completion',sha256(provider),r['provider_sha256'])
        r['artifacts']={str(f):sha256(f)for f in Path(t.empty_manifest['data_path']).rglob('*.parquet')}
        r['status']='pass'
    except BaseException as exc:
        r.update(reason=str(exc),traceback=traceback.format_exc())
        checkpoint=roles.redact(r)if roles is not None else json.loads(json.dumps(r,default=str))
        write_json(out/'execution-failure.json',checkpoint)
    finally:
        if db is not None:
            for c in list(db.connections):
                if not c.closed:
                    try:query(c,'ROLLBACK')
                    except Exception:pass
        try:
            if db is not None:db.cleanup();r['database_cleanup']=db.cleanup_result
            if roles is not None:
                if not r.get('database_cleanup')or r['database_cleanup']['returncode']:raise RuntimeError('Owned DB cleanup unproven; roles retained')
                r['role_cleanup']=cleanup_roles(control,roles.path,a.run_id);r=roles.redact(r)
            r['cleanup']='pass'
        except BaseException as exc:r.update(cleanup='fail',cleanup_error=str(exc),status='fail')
        if control is not None and not control.closed:control.close()
        if roles is not None:r=roles.redact(r)
        write_json(out/'result.json',r)
    print(json.dumps(dict(status=r['status'],assertions=len(r['assertions']),reason=r.get('reason'),result=str(out/'result.json')),ensure_ascii=False));return 0 if r['status']=='pass'else 1
if __name__=='__main__':raise SystemExit(main())
