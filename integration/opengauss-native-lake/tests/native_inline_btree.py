#!/usr/bin/env python3
"""Actual host B-tree build/IndexAM and complete lake scan equivalence."""
import argparse
import hashlib
import json
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
from native_test_context import OwnedDatabase
from native_table_create import Case
from permission_api import OwnedRoles,cleanup_roles

MODES=['plan','transactions','rescan','dml_flush','permissions','upgrade','retire_missing','bigint']
SPEC={'version':1,'kind':'scalar','method':'btree','column':'id'}


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--run-id',required=True)
    p.add_argument('--build-receipt',type=Path,required=True);p.add_argument('--mode',choices=MODES,required=True)
    a=p.parse_args();validate_run_id(a.run_id)
    out=ROOT/'公共文档/openGauss原生湖存储/验证'/a.run_id;out.mkdir(exist_ok=False)
    r={'status':'fail','scope':'Native SQL0.15 inline B-tree '+a.mode,'assertions':[],
       'limitations':['Heap inline integer/bigint comparisons only; file inputs scan completely. No file scalar index, Ustore index, DiskANN, fusion, performance or commercial qualification.']}
    inputs=[Path(__file__),D/'tests/native_table_create.py',D/'tests/permission_api.py',D/'native_test_context.py',
            D/'native_runtime.py',D/'tests/inline_cleanup.py',D/'tests/file_dml_acceptance.py',D/'inline_fixture.py',
            D/'tests/native_auto_flush.py',D/'source-lock.json']
    r['inputs']={str(f):sha256(f)for f in inputs};archive=out/'executed-inputs';archive.mkdir()
    for f in inputs:shutil.copy2(f,archive/f.name)
    control=db=roles=t=None

    def summary(value):
        if isinstance(value,list)and len(value)>30:
            return {'rows':len(value),'sha256':hashlib.sha256(repr(value).encode()).hexdigest()}
        return value

    def check(name,got,want=True):
        ok=got==want;r['assertions'].append({'name':name,'observed':summary(got),'expected':summary(want),'status':'pass'if ok else'fail'})
        if not ok:raise AssertionError(name)

    def rejected(conn,statement,code,args=None):
        import psycopg2
        try:query(conn,statement,args)
        except psycopg2.Error as exc:
            check('rejected '+statement[:120],exc.pgcode,code);query(conn,'ROLLBACK')
        else:raise AssertionError('Unexpectedly accepted '+statement)

    try:
        from psycopg2 import sql
        build=check_build(a.build_receipt);r.update(source_head=build['source_head'],binary_sha256=build['binary_sha256'])
        control=connect_control();check('automatic scheduler stays disabled',scalar(control,'SHOW enable_lake_maintenance'),'off')
        with(out/'fixture.log').open('w')as log:
            subprocess.run([sys.executable,str(D/'tests/catalog_fixture_bundle.py'),'--run-id',a.run_id,'--output',str(out/'fixture/manifest.json')],
                           stdout=log,stderr=subprocess.STDOUT,check=True,timeout=300)
        db=OwnedDatabase(a.run_id,out).create();roles=OwnedRoles(control,a.run_id,out)
        t=Case(db,SimpleNamespace(mode='net_dml',sql_version='0.14'if a.mode=='upgrade'else'0.15',
            manifest=out/'fixture/manifest.json',output=out/'result.json',run_id=a.run_id),r,roles)
        conn=t.a;name='orders'
        if a.mode=='permissions':
            owner=roles.create('business');reader=roles.create('reader')
            for role in [owner,reader]:query(t.a,sql.SQL('GRANT USAGE ON SCHEMA biz TO {}').format(sql.Identifier(role)))
            query(t.a,sql.SQL('GRANT CREATE ON SCHEMA biz TO {}').format(sql.Identifier(owner)))
            query(t.a,sql.SQL('GRANT USAGE ON FOREIGN SERVER created_lake TO {}').format(sql.Identifier(owner)))
            conn,identity=roles.connect(db,'business');unprivileged,other=roles.connect(db,'reader');r['ordinary_identities']=[identity,other]
            name='owned';t.create(name,conn=conn)
        elif a.mode=='bigint':
            query(t.a,"CREATE FOREIGN TABLE biz.orders(id bigint,val integer) SERVER created_lake OPTIONS(binding_mode 'create',lake_schema 'main')")
        else:t.create()
        relation='biz.'+name
        query(t.a,'SELECT og_lake.set_option(%s::regclass,%s::text,%s::text)',(relation,'data_inlining_row_limit','20000'))
        query(t.a,'CREATE TABLE biz.reference(id '+('bigint'if a.mode=='bigint'else'integer')+',val integer)')
        query(t.a,'BEGIN')
        query(t.a,'INSERT INTO '+relation+' SELECT i,i%17 FROM generate_series(1,6000) i')
        query(t.a,'INSERT INTO biz.reference SELECT i,i%17 FROM generate_series(1,6000) i')
        query(t.a,'INSERT INTO '+relation+' VALUES(7,700),(7,701),(NULL,99)')
        query(t.a,'INSERT INTO biz.reference VALUES(7,700),(7,701),(NULL,99)');query(t.a,'COMMIT')
        query(conn,'ANALYZE '+relation)

        def register(index_name='ix_id',connection=conn):
            return str(scalar(connection,'SELECT og_lake.create_lake_index(%s::regclass,%s::name,%s::jsonb)',
                (relation,index_name,json.dumps(SPEC))))

        def build_index(uuid,connection=conn):return query(connection,'SELECT og_lake.build_lake_index(%s::uuid)',(uuid,))

        def physical(uuid,connection=t.observer):
            return scalar(connection,'SELECT inline_index_oid FROM og_lake_internal.index_definition WHERE index_uuid=%s::uuid',(uuid,))

        def status(uuid,connection=conn):return json.loads(scalar(connection,'SELECT og_lake.lake_index_status(%s::uuid)',(uuid,)))

        def rows(connection=conn):return query(connection,'SELECT id,val FROM '+relation+' ORDER BY id NULLS LAST,val')

        def reference():return query(t.observer,'SELECT id,val FROM biz.reference ORDER BY id NULLS LAST,val')

        def plan(statement,connection=conn,args=None):return '\n'.join(x[0]for x in query(connection,'EXPLAIN '+statement,args))

        def assert_index(statement,connection=conn,args=None):
            text=plan(statement,connection,args);r.setdefault('plans',[]).append(text)
            check('real planner chooses native B-tree for '+statement,'Planned Inline Access: native B-tree'in text)

        uuid=register();head=t.head();before=t.protocol()
        if a.mode=='upgrade':
            query(t.a,"ALTER EXTENSION og_lake UPDATE TO '0.15'")
            check('configured private-owner upgrade preserves protocol and all business values',t.protocol(),before)
            check('existing definition keeps identity and starts without physical index',physical(uuid),None)
        if a.mode=='transactions':
            query(conn,'BEGIN');build_index(uuid);oid=physical(uuid,conn)
            check('build makes a real native index in its transaction',scalar(conn,'SELECT relkind FROM pg_class WHERE oid=%s',(oid,)),'i')
            query(conn,'INSERT INTO biz.heap_marker VALUES(77)');query(conn,'ROLLBACK')
            check('rollback removes physical binding and native index',(physical(uuid),scalar(t.observer,'SELECT count(*) FROM pg_class WHERE oid=%s',(oid,))),(None,0))
            check('ordinary heap rolls back with index build',query(t.observer,'SELECT * FROM biz.heap_marker'),[])
            query(conn,'BEGIN');query(conn,'SAVEPOINT built');build_index(uuid);child=physical(uuid,conn)
            query(conn,'ROLLBACK TO built');query(conn,'RELEASE SAVEPOINT built');query(conn,'COMMIT')
            check('savepoint rollback removes index and binding',(physical(uuid),scalar(t.observer,'SELECT count(*) FROM pg_class WHERE oid=%s',(child,))),(None,0))
        build_index(uuid);oid=physical(uuid)
        check('actual native inline B-tree is bound by OID',isinstance(oid,int)and oid>0)
        check('native index parent is the registered carrier',scalar(t.observer,'SELECT i.indrelid=(SELECT c.oid FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace JOIN created_metadata.ducklake_inlined_data_tables d ON d.table_name=c.relname WHERE n.nspname=\'created_metadata\' AND d.table_id=(SELECT table_id FROM og_lake_internal.index_definition WHERE index_uuid=%s::uuid)) FROM pg_index i WHERE i.indexrelid=%s',(uuid,oid)),True)
        build_index(uuid);check('repeated build retains same native index OID',physical(uuid),oid)
        check('index construction creates no new lake snapshot or IDs',t.head(),head)
        check('index construction leaves all standard protocol/data unchanged',t.protocol(),before)
        check('status reports actual inline capability and absent file backend',
              (status(uuid)['inline_index_ready'],status(uuid)['usable'],status(uuid)['file_index_ready']),(True,True,False))
        assert_index('SELECT id,val FROM '+relation+' WHERE id=7'+('::bigint'if a.mode=='bigint'else''))
        for predicate in ['id=7','id<4','id<=4','id>5997','id>=5997','7=id','id IS NULL','id IN(1,7,6000)','val=3 OR id=7']:
            check('complete values including duplicates/NULL for '+predicate,
                  query(conn,'SELECT id,val FROM '+relation+' WHERE '+predicate+' ORDER BY id NULLS LAST,val'),
                  query(t.observer,'SELECT id,val FROM biz.reference WHERE '+predicate+' ORDER BY id NULLS LAST,val'))
        if a.mode=='rescan':
            query(conn,'PREPARE indexed_key('+('bigint'if a.mode=='bigint'else'integer')+') AS SELECT id,val FROM '+relation+' WHERE id=$1 ORDER BY val')
            for key in [7,2,5999,7,100000,1,7,None,6000,7]:
                check('prepared index argument '+str(key),query(conn,'EXECUTE indexed_key(%s)',(key,)),query(t.observer,'SELECT id,val FROM biz.reference WHERE id=%s ORDER BY val',(key,)))
            assert_index('EXECUTE indexed_key(7)')
            query(conn,'CREATE TEMP TABLE rescan_keys AS SELECT i AS id FROM generate_series(1,8) i')
            statement='SELECT q.id,(SELECT min(o.val) FROM '+relation+' o WHERE o.id=q.id) FROM rescan_keys q ORDER BY q.id'
            assert_index(statement)
            check('correlated SubPlan changes indexed Param on actual rescans',query(conn,statement),[(i,i%17)for i in range(1,9)])
            query(conn,'DEALLOCATE indexed_key')
        elif a.mode=='dml_flush':
            old=db.connect();query(old,'BEGIN ISOLATION LEVEL READ COMMITTED')
            original=query(old,'SELECT id,val FROM '+relation+' WHERE id=7 ORDER BY val')
            for statement in ['UPDATE {table} SET val=val+100 WHERE id=7','UPDATE {table} SET val=val+1000 WHERE id=7','DELETE FROM {table} WHERE id=2','INSERT INTO {table} VALUES(8000,80)']:
                query(t.a,'BEGIN');query(t.a,statement.format(table=relation));query(t.a,statement.format(table='biz.reference'));query(t.a,'COMMIT')
            check('all current versions equal transactional heap before flush',rows(),reference())
            check('old indexed reader retains old values',query(old,'SELECT id,val FROM '+relation+' WHERE id=7 ORDER BY val'),original)
            query(t.a,'SELECT og_lake.flush(%s::regclass)',(relation,))
            check('flush has actual committed Parquet inputs',scalar(t.observer,'SELECT count(*) FROM created_metadata.ducklake_data_file')>0)
            check('index plus complete file fallback preserves every row after flush',rows(),reference())
            check('old indexed reader survives physical inline transfer',query(old,'SELECT id,val FROM '+relation+' WHERE id=7 ORDER BY val'),original)
            query(old,'COMMIT')
            query(t.a,'BEGIN');query(t.a,'UPDATE '+relation+' SET val=val+1 WHERE id=7');query(t.a,'UPDATE biz.reference SET val=val+1 WHERE id=7');query(t.a,'COMMIT')
            check('file-origin update populates and maintains existing inline B-tree',rows(),reference())
        elif a.mode=='permissions':
            query(t.a,sql.SQL('GRANT SELECT ON {} TO {}').format(sql.SQL(relation),sql.Identifier(reader)))
            rejected(unprivileged,'SELECT og_lake.build_lake_index(%s::uuid)','42501',(uuid,))
            rejected(conn,'SELECT * FROM og_lake_internal.index_definition','42501')
            check('builder restores real ordinary caller identity',scalar(conn,'SELECT current_user'),owner)
            check('SELECT-only role can execute actual native indexed query',query(unprivileged,'SELECT id,val FROM '+relation+' WHERE id=7 ORDER BY val'),[(7,7),(7,700),(7,701)])
        elif a.mode=='retire_missing':
            query(conn,'PREPARE cached_index AS SELECT id,val FROM '+relation+' WHERE id=7 ORDER BY val')
            check('prepared indexed query starts exact',query(conn,'EXECUTE cached_index'),[(7,7),(7,700),(7,701)])
            query(conn,'SELECT og_lake.drop_lake_index(%s::uuid)',(uuid,))
            check('retired definition is not usable',status(uuid)['usable'],False)
            check('prepared query falls back completely after retirement',query(conn,'EXECUTE cached_index'),[(7,7),(7,700),(7,701)])
            uuid=register('ix_missing');build_index(uuid);oid=physical(uuid)
            qualified=scalar(t.a,'SELECT quote_ident(n.nspname)||\'.\'||quote_ident(c.relname) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.oid=%s',(oid,))
            query(conn,'PREPARE missing_physical AS SELECT id,val FROM '+relation+' WHERE id=7 ORDER BY val')
            check('second prepared plan starts with a real index',query(conn,'EXECUTE missing_physical'),[(7,7),(7,700),(7,701)])
            query(t.a,'DROP INDEX '+qualified)
            check('missing physical index uses full scan before output',query(conn,'EXECUTE missing_physical'),[(7,7),(7,700),(7,701)])
            check('full table remains exact after missing index fallback',rows(),reference())
        elif a.mode=='bigint':
            query(t.a,'BEGIN');query(t.a,'INSERT INTO '+relation+' VALUES(9223372036854775807,1),(-9223372036854775808,2)')
            query(t.a,'INSERT INTO biz.reference VALUES(9223372036854775807,1),(-9223372036854775808,2)');query(t.a,'COMMIT')
            for key in [9223372036854775807,-9223372036854775808,7]:
                # psycopg2's integer literal renders -2^63::bigint as unary minus
                # applied to an overflowing positive bigint in the native parser.
                # Bind decimal text with the same explicit cast for lake and heap.
                check('bigint index boundary '+str(key),query(conn,'SELECT id,val FROM '+relation+' WHERE id=%s::bigint ORDER BY val',(str(key),)),query(t.observer,'SELECT id,val FROM biz.reference WHERE id=%s::bigint ORDER BY val',(str(key),)))
        check('final complete values equal independent heap',rows(),reference())
        check('all actual inputs unchanged',all(sha256(Path(f))==h for f,h in r['inputs'].items()));check_build(a.build_receipt);r['status']='pass'
    except BaseException as exc:r.update(reason=str(exc),traceback=traceback.format_exc())
    finally:
        if db is not None:
            try:db.cleanup();r['database_cleanup']=db.cleanup_result
            except BaseException as exc:r.update(status='fail',cleanup_error=str(exc))
        if roles is not None:
            try:
                if not r.get('database_cleanup')or r['database_cleanup']['returncode']:raise RuntimeError('Owned DB cleanup unproven; role retained')
                r['role_cleanup']=cleanup_roles(control,roles.path,a.run_id);r=roles.redact(r)
            except BaseException as exc:r.update(status='fail',role_cleanup_error=str(exc))
        if control is not None:control.close()
        write_json(out/'result.json',r)
    print(json.dumps({'status':r['status'],'mode':a.mode,'checks':len(r['assertions']),'reason':r.get('reason'),'result':str(out/'result.json')},ensure_ascii=False))
    return 0 if r['status']=='pass'else 1


if __name__=='__main__':raise SystemExit(main())
