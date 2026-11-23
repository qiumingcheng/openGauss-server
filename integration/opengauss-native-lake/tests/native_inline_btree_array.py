#!/usr/bin/env python3
"""Native B-tree array keys preserve duplicate, NULL, private and file input semantics."""
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


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--run-id',required=True)
    p.add_argument('--build-receipt',type=Path,required=True);p.add_argument('--sql-version',choices=['0.15','0.16'],default='0.15');a=p.parse_args();validate_run_id(a.run_id)
    out=ROOT/'公共文档/openGauss原生湖存储/验证'/a.run_id;out.mkdir(exist_ok=False)
    r={'status':'fail','scope':'SQL0.15 native SK_SEARCHARRAY integer IN/ANY keys with actual execution, generic rescan, private DML and complete file fallback','assertions':[],
       'limitations':['Heap inline int4 only, no remote/file-index/ANN/performance or production qualification.']}
    paths=[Path(__file__),D/'tests/native_table_create.py',D/'tests/permission_api.py',D/'native_test_context.py',
           D/'native_runtime.py',D/'tests/inline_cleanup.py',D/'tests/file_dml_acceptance.py',D/'inline_fixture.py',
           D/'tests/native_auto_flush.py',D/'source-lock.json']
    r['inputs']={str(f):sha256(f)for f in paths};archive=out/'executed-inputs';archive.mkdir()
    for f in paths:shutil.copy2(f,archive/f.name)
    control=db=roles=t=None

    def check(name,got,want=True):
        ok=got==want
        def summary(value):
            if isinstance(value,list) and len(value)>30:
                return {'rows':len(value),'sha256':hashlib.sha256(repr(value).encode()).hexdigest()}
            return value
        r['assertions'].append({'name':name,'observed':summary(got),'expected':summary(want),'status':'pass'if ok else'fail'})
        if not ok:raise AssertionError(name)

    try:
        build=check_build(a.build_receipt);r.update(source_head=build['source_head'],binary_sha256=build['binary_sha256'])
        control=connect_control();check('automatic scheduler stays disabled',scalar(control,'SHOW enable_lake_maintenance'),'off')
        with(out/'fixture.log').open('w')as log:
            subprocess.run([sys.executable,str(D/'tests/catalog_fixture_bundle.py'),'--run-id',a.run_id,'--output',str(out/'fixture/manifest.json')],
                           stdout=log,stderr=subprocess.STDOUT,check=True,timeout=300)
        db=OwnedDatabase(a.run_id,out).create();roles=OwnedRoles(control,a.run_id,out)
        t=Case(db,SimpleNamespace(mode='net_dml',sql_version=a.sql_version,manifest=out/'fixture/manifest.json',output=out/'result.json',run_id=a.run_id),r,roles)
        t.create();query(t.a,'CREATE TABLE biz.reference(id integer,val integer)')
        query(t.a,"SELECT og_lake.set_option('biz.orders'::regclass,'data_inlining_row_limit','30000')")
        query(t.a,'BEGIN')
        for name in ['biz.orders','biz.reference']:
            query(t.a,'INSERT INTO '+name+' SELECT i,i%17 FROM generate_series(1,20000) i')
            query(t.a,'INSERT INTO '+name+' VALUES(7,700),(7,701),(NULL,99)')
        query(t.a,'COMMIT');query(t.a,'ANALYZE biz.orders')
        uuid=str(scalar(t.a,"SELECT og_lake.create_lake_index('biz.orders'::regclass,'ix_id'::name,%s::jsonb)",
                        (json.dumps({'version':1,'kind':'scalar','method':'btree','column':'id'}),)))
        query(t.a,'SELECT og_lake.build_lake_index(%s::uuid)',(uuid,));r['plans']=[]

        def compare(label,array,rhs='bigint[]'):
            statement='SELECT id,val FROM biz.orders WHERE id=ANY(%s::'+rhs+') ORDER BY id,val'
            check(label,query(t.a,statement,(array,)),query(t.a,statement.replace('biz.orders','biz.reference'),(array,)))

        def indexed(label,statement,args=None):
            text='\n'.join(x[0]for x in query(t.a,'EXPLAIN ANALYZE '+statement,args));r['plans'].append(text)
            check(label+' planned native B-tree','Planned Inline Access: native B-tree'in text)
            check(label+' actual native IndexAM','Actual Inline Access: native B-tree'in text)

        for rhs in ['smallint[]','integer[]','bigint[]']:
            indexed(rhs+' array','SELECT id,val FROM biz.orders WHERE id=ANY(%s::'+rhs+')',('{7,2,NULL,7}',))
            for array in ['{7,2,NULL,7}','{}',None,'{NULL,NULL}','{{7,2},{7,1}}','{1,20000}']:
                compare(rhs+' '+str(array),array,rhs)
        for array in ['{-9223372036854775808,9223372036854775807,7}',
                      '{-2147483649,2147483648,7}','{-9223372036854775808}',
                      '{9223372036854775807}']:
            compare('wide array operands '+array,array)
        indexed('plain SQL IN','SELECT id,val FROM biz.orders WHERE id IN(7,2,7,NULL)')
        for predicate in ['id NOT IN(7,2)','id NOT IN(7,2,NULL)','id=ALL(ARRAY[7,2])']:
            statement='SELECT id,val FROM biz.orders WHERE '+predicate+' ORDER BY id,val'
            check('unsupported array operator keeps complete fallback '+predicate,query(t.a,statement),query(t.a,statement.replace('biz.orders','biz.reference')))
        check('plain SQL IN keeps native duplicate semantics',query(t.a,'SELECT id,val FROM biz.orders WHERE id IN(7,2,7,NULL) ORDER BY id,val'),
              query(t.a,'SELECT id,val FROM biz.reference WHERE id IN(7,2,7,NULL) ORDER BY id,val'))
        query(t.a,'PREPARE indexed_array(bigint[]) AS SELECT id,val FROM biz.orders WHERE id=ANY($1) ORDER BY id,val')
        for array in ['{7,2,7}','{}','{6000,1}','{7}',None,'{NULL,NULL}','{7,2}',
                      '{-9223372036854775808,9223372036854775807,7}','{{7,2},{7,1}}']:
            check('prepared array rescan '+str(array),query(t.a,'EXECUTE indexed_array(%s::bigint[])',(array,)),
                  query(t.a,'SELECT id,val FROM biz.reference WHERE id=ANY(%s::bigint[]) ORDER BY id,val',(array,)))
        indexed('generic Param array',"EXECUTE indexed_array('{7,2}'::bigint[])")
        query(t.a,'BEGIN')
        for name in ['biz.orders','biz.reference']:query(t.a,'UPDATE '+name+' SET id=100007,val=val+1 WHERE id=7')
        compare('array includes changed-key private versions and hides ended versions','{7,100007,100007}')
        query(t.a,'SAVEPOINT child')
        for name in ['biz.orders','biz.reference']:query(t.a,'INSERT INTO '+name+' VALUES(7,777)')
        compare('array contains child private insert exactly once','{7,100007,7}')
        query(t.a,'ROLLBACK TO child');query(t.a,'RELEASE SAVEPOINT child')
        compare('array child rollback restores parent private view','{7,100007,7}')
        query(t.a,'COMMIT');compare('committed changed-key array','{7,100007}')
        query(t.a,"SELECT og_lake.flush('biz.orders'::regclass)")
        check('array fallback fixture contains actual Parquet files',scalar(t.observer,'SELECT count(*) FROM created_metadata.ducklake_data_file')>0)
        compare('array preserves all file-origin rows through complete fallback','{1,2,7,100007,20000}')
        query(t.a,'BEGIN')
        for name in ['biz.orders','biz.reference']:query(t.a,'UPDATE '+name+' SET val=val+3 WHERE id=100007')
        compare('array mixes file source suppression and private new versions','{7,100007,100007}')
        query(t.a,'COMMIT');compare('array mixes committed inline and file versions','{1,2,7,100007,20000}')
        check('whole cardinality matches independent heap',scalar(t.a,'SELECT count(*) FROM biz.orders'),scalar(t.a,'SELECT count(*) FROM biz.reference'))
        query(t.a,'DEALLOCATE indexed_array')
        check('all executed inputs unchanged',all(sha256(Path(f))==h for f,h in r['inputs'].items()));check_build(a.build_receipt);r['status']='pass'
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
    print(json.dumps({'status':r['status'],'checks':len(r['assertions']),'reason':r.get('reason'),'result':str(out/'result.json')},ensure_ascii=False))
    return 0 if r['status']=='pass'else 1


if __name__=='__main__':raise SystemExit(main())
