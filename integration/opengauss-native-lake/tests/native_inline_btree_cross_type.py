#!/usr/bin/env python3
"""Cross-integer native scan keys retain the operator RHS subtype without narrowing."""
import argparse
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
    p.add_argument('--build-receipt',type=Path,required=True);a=p.parse_args();validate_run_id(a.run_id)
    out=ROOT/'公共文档/openGauss原生湖存储/验证'/a.run_id;out.mkdir(exist_ok=False)
    r={'status':'fail','scope':'Native inline integer B-tree cross-type comparisons, out-of-range operands, generic Params and actual rescans','assertions':[],
       'limitations':['Heap inline int4/int8 keys and builtin int2/int4/int8 comparisons only; no file-index/ANN/performance or production qualification.']}
    paths=[Path(__file__),D/'tests/native_table_create.py',D/'tests/permission_api.py',D/'native_test_context.py',
           D/'native_runtime.py',D/'tests/inline_cleanup.py',D/'tests/file_dml_acceptance.py',D/'inline_fixture.py',
           D/'tests/native_auto_flush.py',D/'source-lock.json']
    r['inputs']={str(f):sha256(f)for f in paths};archive=out/'executed-inputs';archive.mkdir()
    for f in paths:shutil.copy2(f,archive/f.name)
    control=db=roles=t=None

    def check(name,got,want=True):
        ok=got==want;r['assertions'].append({'name':name,'observed':got,'expected':want,'status':'pass'if ok else'fail'})
        if not ok:raise AssertionError(name)

    try:
        build=check_build(a.build_receipt);r.update(source_head=build['source_head'],binary_sha256=build['binary_sha256'])
        control=connect_control();check('automatic scheduler stays disabled',scalar(control,'SHOW enable_lake_maintenance'),'off')
        with(out/'fixture.log').open('w')as log:
            subprocess.run([sys.executable,str(D/'tests/catalog_fixture_bundle.py'),'--run-id',a.run_id,'--output',str(out/'fixture/manifest.json')],
                           stdout=log,stderr=subprocess.STDOUT,check=True,timeout=300)
        db=OwnedDatabase(a.run_id,out).create();roles=OwnedRoles(control,a.run_id,out)
        t=Case(db,SimpleNamespace(mode='net_dml',sql_version='0.15',manifest=out/'fixture/manifest.json',output=out/'result.json',run_id=a.run_id),r,roles)
        t.create()
        query(t.a,"CREATE FOREIGN TABLE biz.bigorders(id bigint,val integer) SERVER created_lake OPTIONS(binding_mode 'create',lake_schema 'main')")
        r['plans']=[]
        for name,keytype in [('orders','integer'),('bigorders','bigint')]:
            table='biz.'+name;heap='biz.ref_'+name
            query(t.a,'CREATE TABLE '+heap+'(id '+keytype+',val integer)')
            query(t.a,"SELECT og_lake.set_option(%s::regclass,'data_inlining_row_limit','20000')",(table,))
            query(t.a,'BEGIN')
            for target in [table,heap]:
                query(t.a,'INSERT INTO '+target+' SELECT i,i%17 FROM generate_series(1,6000) i')
                query(t.a,'INSERT INTO '+target+' VALUES(7,700),(7,701),(NULL,99)')
                for key,value in ([(-2147483648,81),(2147483647,82)] if keytype=='integer' else [(-9223372036854775808,83),(9223372036854775807,84)]):
                    query(t.a,'INSERT INTO '+target+' VALUES(%s::'+keytype+',%s)',(str(key),value))
            query(t.a,'COMMIT');query(t.a,'ANALYZE '+table)
            uuid=str(scalar(t.a,"SELECT og_lake.create_lake_index(%s::regclass,'ix_id'::name,%s::jsonb)",
                            (table,json.dumps({'version':1,'kind':'scalar','method':'btree','column':'id'}))))
            query(t.a,'SELECT og_lake.build_lake_index(%s::uuid)',(uuid,))

            def compare(label,predicate,args):
                statement='SELECT id,val FROM '+table+' WHERE '+predicate+' ORDER BY id NULLS LAST,val'
                expected=statement.replace(table,heap)
                check(name+' '+label,query(t.a,statement,args),query(t.a,expected,args))

            def indexed(label,statement,args=None):
                text='\n'.join(x[0]for x in query(t.a,'EXPLAIN ANALYZE '+statement,args))
                r['plans'].append(text)
                check(name+' '+label+' planned native B-tree','Planned Inline Access: native B-tree'in text)
                check(name+' '+label+' actual native IndexAM','Actual Inline Access: native B-tree'in text)

            for rhs in ['smallint','integer','bigint']:
                indexed(rhs+' comparison','SELECT id,val FROM '+table+' WHERE id=%s::'+rhs,('7',))
                for op in ['=','<','<=','>','>=']:
                    compare(rhs+' '+op+' duplicate key','id'+op+'%s::'+rhs,('7',))
                compare(rhs+' commuted key','%s::'+rhs+'=id',('7',))
            for key in ['-9223372036854775808','9223372036854775807','-2147483649','2147483648']:
                for op in ['=','<','>']:
                    compare('wide operand '+key+' '+op,'id'+op+'%s::bigint',(key,))
            compare('numeric operand keeps complete fallback','id=%s::numeric',('7.0',))
            compare('NULL parameter','id=%s::bigint',(None,))
            prepared='wide_'+name
            query(t.a,'PREPARE '+prepared+'(bigint) AS SELECT id,val FROM '+table+' WHERE id=$1 ORDER BY val')
            for key in ['7','2','5999','7','100000','1',None,'7','-9223372036854775808','9223372036854775807']:
                check(name+' prepared bigint '+str(key),query(t.a,'EXECUTE '+prepared+'(%s::bigint)',(key,)),
                      query(t.a,'SELECT id,val FROM '+heap+' WHERE id=%s::bigint ORDER BY val',(key,)))
            indexed('generic wide Param','EXECUTE '+prepared+'(7)')
            keys='biz.keys_'+name
            query(t.a,'CREATE TABLE '+keys+'(id bigint)')
            query(t.a,'INSERT INTO '+keys+" VALUES(7),(2),(NULL),('-9223372036854775808'::bigint),('9223372036854775807'::bigint)")
            statement='SELECT q.id,(SELECT min(o.val) FROM '+table+' o WHERE o.id=q.id) FROM '+keys+' q ORDER BY q.id NULLS LAST'
            indexed('correlated wide Param rescan',statement)
            check(name+' actual correlated wide Param results',query(t.a,statement),query(t.a,statement.replace(table,heap)))
            query(t.a,'DEALLOCATE '+prepared)
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
