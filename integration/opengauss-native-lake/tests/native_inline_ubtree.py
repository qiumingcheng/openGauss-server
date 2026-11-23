#!/usr/bin/env python3
"""Native UB-tree over a genuine reference inline catalog imported as Ustore.

Only the physical carrier AM is adapted. This qualifies neither SMP, DiskANN,
remote/RAC nor production readiness.
"""
import argparse
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import sys
import traceback

ROOT=Path('/home/qiumc/data/code/cpp');D=ROOT/'integration/opengauss-native-lake'
sys.path[:0]=[str(D),str(D/'tests')]
from file_dml_acceptance import check_build
from inline_fixture import BUSINESS_TYPES,export_reference,foreign_table_statement,multiset,query,scalar
from lake_harness import sha256,validate_run_id,write_json
from native_auto_flush import connect_control
from native_test_context import OwnedDatabase
from permission_api import OwnedRoles,adopt_private_owner,cleanup_roles
from ustore_fixture import import_catalog_ustore

def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--run-id',required=True)
    p.add_argument('--build-receipt',type=Path,required=True)
    p.add_argument('--mode',choices=['integer','bigint','rescan','transactions','old_snapshot','old_build_transfer','long_text'],required=True)
    a=p.parse_args();validate_run_id(a.run_id)
    out=ROOT/'公共文档/openGauss原生湖存储/验证'/a.run_id;out.mkdir(exist_ok=False)
    r={'status':'fail','scope':__doc__+' '+a.mode,'assertions':[]}
    files=[Path(__file__),D/'tests/ustore_fixture.py',D/'tests/permission_api.py',D/'native_test_context.py',
        D/'native_runtime.py',D/'tests/inline_cleanup.py',D/'tests/file_dml_acceptance.py',D/'inline_fixture.py',
        D/'tests/native_auto_flush.py',D/'source-lock.json']
    r['inputs']={str(f):sha256(f)for f in files};archive=out/'executed-inputs';archive.mkdir()
    for f in files:shutil.copy2(f,archive/f.name)
    control=db=roles=None
    def image(v):return dict(rows=len(v),sha256=hashlib.sha256(repr(v).encode()).hexdigest())if isinstance(v,list)and len(v)>30 else v
    def check(name,got,want=True):
        good=got==want;r['assertions'].append(dict(name=name,observed=image(got),expected=image(want),status='pass'if good else'fail'))
        if not good:raise AssertionError(name)
    try:
        from psycopg2 import sql
        b=check_build(a.build_receipt);r.update(source_head=b['source_head'],binary_sha256=b['binary_sha256'])
        control=connect_control();check('automatic maintenance stays off',scalar(control,'SHOW enable_lake_maintenance'),'off')
        profile='TYPED'if a.mode=='bigint'else'LONG_TEXT'if a.mode=='long_text'else'USTORE_STREAM'
        manifest=out/'manifest.json'
        with(out/'fixture.log').open('w')as log:
            subprocess.run(['bash',str(D/'reference.sh'),'fixture','--case',profile,'--run-id',a.run_id+'-fixture','--output',str(manifest)],stdout=log,stderr=subprocess.STDOUT,check=True,timeout=300)
        exported,source=export_reference(D,manifest,out/'reference-export.json',a.run_id)
        check('exact independent reference profile',source['case'],profile)
        db=OwnedDatabase(a.run_id,out).create();roles=OwnedRoles(control,a.run_id,out)
        admin,observer,writer=[db.connect()for _ in range(3)]
        query(admin,"CREATE EXTENSION og_lake VERSION '0.16'")
        query(admin,'BEGIN')
        import_catalog_ustore(admin,'lake_metadata',exported);query(admin,'COMMIT')
        owner=roles.create('private',False)
        query(admin,'BEGIN');adopt_private_owner(admin,exported,owner);query(admin,'COMMIT')
        query(admin,'CREATE SCHEMA biz');query(admin,"SELECT og_lake.attach_catalog('lake_reference','lake_metadata',NULL::text,false)")
        query(admin,foreign_table_statement('biz','orders','lake_reference',source['table_id'],source['schema']))
        names=[c['name']for c in source['schema']]
        columns=sql.SQL(',').join(map(sql.Identifier,names));ordering=sql.SQL(',').join(sql.SQL('{} NULLS LAST').format(sql.Identifier(n))for n in names)
        definitions=sql.SQL(',').join(sql.SQL('{} {}').format(sql.Identifier(c['name']),sql.SQL(BUSINESS_TYPES[c['type']]))for c in source['schema'])
        query(admin,sql.SQL('CREATE TABLE biz.reference({})').format(definitions))
        actual=query(admin,sql.SQL('SELECT {} FROM biz.orders').format(columns))
        expected=[tuple(row[n]for n in names)for row in source['expected']]
        check('native Ustore reads equal every genuine reference value',multiset(actual),multiset(expected))
        query(admin,sql.SQL('INSERT INTO biz.reference SELECT {} FROM biz.orders').format(columns))
        def both(statement,args=None):
            query(admin,'BEGIN')
            for table in ['biz.orders','biz.reference']:query(admin,statement.format(table=table),args)
            query(admin,'COMMIT')
        if a.mode=='bigint':
            check('typed profile has the full primitive schema',names,['id','val','flag','bigval','note'])
            both("INSERT INTO {table} SELECT i,i%17,i%2=0,i::bigint*10000000000,repeat('typed-',20) FROM generate_series(1000,6999)i")
        elif a.mode=='long_text':
            large=max(source['expected'],key=lambda row:sum(len(str(v))for v in row.values()if v is not None))
            values=[large[n]for n in names[1:]]
            statement='INSERT INTO {table} SELECT i,'+','.join(['%s']*len(values))+' FROM generate_series(10000,11023)i'
            both(statement,values)
        else:
            check('stream profile includes genuine long noncompressible note values',names,['id','val','note'])
            both("INSERT INTO {table} VALUES(7,70007,'duplicate'),(NULL,0,'null-key'),('-2147483648'::integer,1,'min'),('2147483647'::integer,2,'max')")
        query(admin,'ANALYZE biz.orders')
        if a.mode=='old_build_transfer':
            query(admin,"SELECT og_lake.set_option('biz.orders'::regclass,'data_inlining_row_limit','50000')")
        key='bigval'if a.mode=='bigint'else'id'
        point="bigval='60000000000000'::bigint"if a.mode=='bigint'else'id=10001'if a.mode=='long_text'else'id=7'
        def equal(predicate,conn=admin):
            template=sql.SQL('SELECT {} FROM {} WHERE {} ORDER BY {}')
            check('heap differential '+predicate,
                query(conn,template.format(columns,sql.SQL('biz.orders'),sql.SQL(predicate),ordering)),
                query(conn,template.format(columns,sql.SQL('biz.reference'),sql.SQL(predicate),ordering)))
        def plan(predicate=point,conn=admin,expect='native UB-tree'):
            text='\n'.join(v[0]for v in query(conn,'EXPLAIN (ANALYZE TRUE,COSTS FALSE) SELECT * FROM biz.orders WHERE '+predicate))
            r.setdefault('actual_plans',[]).append(text);check('actual inline method '+expect,'Actual Inline Access: '+expect in text);return text
        before=int(scalar(observer,'SELECT max(snapshot_id) FROM lake_metadata.ducklake_snapshot'))
        uuid=str(scalar(admin,"SELECT og_lake.create_lake_index('biz.orders'::regclass,'ix_key',%s::jsonb)",
            (json.dumps(dict(version=1,kind='scalar',method='btree',column=key)),)))
        def build(conn=admin):query(conn,'SELECT og_lake.build_lake_index(%s::uuid)',(uuid,))
        def physical(conn=observer):return scalar(conn,'SELECT inline_index_oid FROM og_lake_internal.index_definition WHERE index_uuid=%s::uuid',(uuid,))
        old=None
        if a.mode in ['old_snapshot','old_build_transfer']:
            old=db.connect();query(old,'BEGIN');r['host_snapshot_before_build']=scalar(old,'SELECT txid_current_snapshot()::text')
            prior=query(old,'SELECT * FROM biz.orders WHERE id=7 ORDER BY val,note')
        if a.mode=='old_build_transfer':
            query(admin,"SELECT og_lake.flush('biz.orders'::regclass)")
            check('native flush actually moved the retained old physical inputs',scalar(observer,'SELECT count(*) FROM lake_metadata.ducklake_data_file')>0)
            both("INSERT INTO {table} SELECT i,i,repeat('post-transfer-',20) FROM generate_series(10000,15999)i")
            before=int(scalar(observer,'SELECT max(snapshot_id) FROM lake_metadata.ducklake_snapshot'))
        if a.mode=='transactions':
            for child in [False,True]:
                query(admin,'BEGIN')
                if child:query(admin,'SAVEPOINT built')
                build();oid=physical(admin);check('own transaction has real UB-tree',scalar(admin,'SELECT relam FROM pg_class WHERE oid=%s',(oid,)),4439)
                if child:query(admin,'ROLLBACK TO built');query(admin,'RELEASE SAVEPOINT built');query(admin,'COMMIT')
                else:query(admin,'ROLLBACK')
                check('rollback removes binding and physical UB-tree',(physical(),scalar(observer,'SELECT count(*) FROM pg_class WHERE oid=%s',(oid,))),(None,0))
        build();oid=physical();check('actual native UB-tree parent and method',query(observer,"SELECT a.amname,i.indrelid FROM pg_class c JOIN pg_am a ON a.oid=c.relam JOIN pg_index i ON i.indexrelid=c.oid WHERE c.oid=%s",(oid,))[0][0],'ubtree')
        r['native_index_flags']=query(observer,'SELECT indcheckxmin,xmin::text,indisvalid,indisready FROM pg_index WHERE indexrelid=%s',(oid,))
        check('derived index build allocates no lake snapshot',int(scalar(observer,'SELECT max(snapshot_id) FROM lake_metadata.ducklake_snapshot')),before)
        build();check('repeated build preserves the same native index',physical(),oid)
        if a.mode=='old_build_transfer':
            # Advance ordinary RC transaction horizons after the index commit,
            # without changing any lake row or snapshot. A retained business
            # reader must still use its own pre-transfer native snapshot.
            for repeat in range(2):query(writer,'UPDATE biz.reference SET val=val WHERE id=7')
        plan();equal(point)
        if a.mode=='integer':
            for predicate in ['id IS NULL','id IS NOT NULL','id<4','id<=4','id>1020','id>=1020',"id>'2147483647'::bigint","id<'-2147483649'::bigint",'id=ANY(ARRAY[7,1,NULL,7,1024]::bigint[])','id=ANY(ARRAY[]::integer[])']:
                equal(predicate)
        elif a.mode=='bigint':
            for value in ['-9223372036854775808','9223372036854775807','2147483648','-2147483649','60000000000000']:
                equal("bigval='"+value+"'::bigint")
            equal('bigval IS NULL');equal('bigval=ANY(ARRAY[1,2,NULL,1]::smallint[])')
        elif a.mode=='rescan':
            query(admin,'PREPARE key_value(bigint) AS SELECT * FROM biz.orders WHERE id=$1 ORDER BY id,val,note')
            for value in [7,1,1024,None,2147483648,-2147483649,7,2,7]:
                check('prepared UB-tree parameter '+str(value),query(admin,'EXECUTE key_value(%s)',(str(value)if value is not None else None,)),
                    query(admin,'SELECT * FROM biz.reference WHERE id=%s::bigint ORDER BY id,val,note',(str(value)if value is not None else None,)))
            query(admin,'DEALLOCATE key_value')
            query(admin,'CREATE TEMP TABLE rescan_keys(k bigint)')
            query(admin,'INSERT INTO rescan_keys VALUES(7),(1),(1024),(NULL),(2147483648),(-2147483649),(7)')
            statement='SELECT q.k,(SELECT min(o.val) FROM {} o WHERE o.id=q.k) FROM rescan_keys q ORDER BY q.k NULLS LAST'
            check('correlated UB-tree rescan equals heap for wide and NULL parameters',
                query(admin,statement.format('biz.orders')),query(admin,statement.format('biz.reference')))
            text='\n'.join(v[0]for v in query(admin,'EXPLAIN (ANALYZE TRUE,COSTS FALSE) '+statement.format('biz.orders')))
            r.setdefault('actual_plans',[]).append(text)
            check('correlated subplan actually uses native UB-tree','Actual Inline Access: native UB-tree'in text)
            check('correlated subplan actually executes seven times','loops=7'in text)
        elif a.mode=='old_snapshot':
            later=db.connect();query(later,'BEGIN');stable=query(later,'SELECT * FROM biz.orders WHERE id=7 ORDER BY val,note');plan(conn=later)
            for add in [1,10]:query(writer,'UPDATE biz.orders SET val=val+%s WHERE id=7',(add,))
            check('reader predating build retains its legal values',query(old,'SELECT * FROM biz.orders WHERE id=7 ORDER BY val,note'),prior)
            plan(conn=old,expect='TableAM sequential')
            check('post-build retained reader uses native undo values',query(later,'SELECT * FROM biz.orders WHERE id=7 ORDER BY val,note'),stable);plan(conn=later)
            query(old,'COMMIT');query(later,'COMMIT')
            query(admin,'UPDATE biz.reference SET val=val+11 WHERE id=7');equal(point)
        elif a.mode=='old_build_transfer':
            for epoch in range(6):
                query(writer,'UPDATE biz.reference SET val=val WHERE id=7')
                check('reader predating transfer/build loses no old rows at RC epoch '+str(epoch),
                    query(old,'SELECT * FROM biz.orders WHERE id=7 ORDER BY val,note'),prior)
                plan(conn=old,expect='TableAM sequential')
            query(old,'COMMIT')
        for repeat in range(6):equal(point)
        check('complete final Ustore result equals independent heap',query(admin,sql.SQL('SELECT {} FROM biz.orders ORDER BY {}').format(columns,ordering)),query(admin,sql.SQL('SELECT {} FROM biz.reference ORDER BY {}').format(columns,ordering)))
        check('all executed inputs unchanged',all(sha256(f)==h for f,h in r['inputs'].items()));check_build(a.build_receipt);r['status']='pass'
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
