#!/usr/bin/env python3
"""Actual native CREATE types, multi-table allocation and identifier coverage."""
import argparse,json,os,shutil,sys,traceback,uuid
from pathlib import Path
ROOT=Path('/home/qiumc/data/code/cpp');DRIVER=ROOT/'integration/opengauss-native-lake'
sys.path[:0]=[str(DRIVER),str(DRIVER/'tests')]
from native_table_create import Case,reject
from create_extended_oracle import interread
from inline_fixture import query,scalar
from native_test_context import OwnedDatabase
from permission_api import OwnedRoles,cleanup_roles
from reverse_reference import qi,literal
from lake_harness import Blocked,sha256,validate_run_id,write_json


def create(t,name,columns='id integer,val integer',conn=None,schema='biz'):
    statement=t.sql.SQL("CREATE FOREIGN TABLE {}.{} ({}) SERVER {} OPTIONS(binding_mode 'create',lake_schema 'main')").format(
        t.sql.Identifier(schema),t.sql.Identifier(name),t.sql.SQL(columns),t.sql.Identifier(t.server))
    query(conn or t.a,statement)


def typed(t):
    long_text=('湖'+'x'*61)*256
    query(t.a,'BEGIN');create(t,'typed','id integer,flag boolean,amount bigint,note text')
    query(t.a,'INSERT INTO biz.typed VALUES(%s,%s,%s,%s),(%s,%s,%s,%s),(%s,%s,%s,%s)',
        (1,True,-9223372036854775808,'old',2,False,9223372036854775807,'',3,None,None,None))
    query(t.a,'UPDATE biz.typed SET note=%s,flag=false WHERE id=1',(long_text,))
    query(t.a,'DELETE FROM biz.typed WHERE id=2')
    expected=[{'id':1,'flag':False,'amount':-9223372036854775808,'note':long_text},
              {'id':3,'flag':None,'amount':None,'note':None}]
    t.check('all CREATE types read their final private versions',query(t.a,'SELECT * FROM biz.typed ORDER BY id'),
        [(x['id'],x['flag'],x['amount'],x['note']) for x in expected])
    query(t.a,'COMMIT')
    t.check('typed first publication frontiers',t.head(),(1,1,2,1))
    t.check('all four column types are published exactly',query(t.a,t.sql.SQL('SELECT column_id,column_name,column_type FROM {}.ducklake_column ORDER BY column_id').format(t.sql.Identifier(t.schema))),
        [(1,'id','int32'),(2,'flag','boolean'),(3,'amount','int64'),(4,'note','varchar')])
    t.check('long text and NULLs survive commit',query(t.observer,'SELECT * FROM biz.typed ORDER BY id'),
        [(x['id'],x['flag'],x['amount'],x['note']) for x in expected])
    groups=[["CREATE TABLE lake.main.typed(id INTEGER,flag BOOLEAN,amount BIGINT,note VARCHAR)",
        "INSERT INTO lake.main.typed VALUES(1,true,-9223372036854775808,'old'),(2,false,9223372036854775807,''),(3,NULL,NULL,NULL)",
        'UPDATE lake.main.typed SET note='+literal(long_text)+',flag=false WHERE id=1','DELETE FROM lake.main.typed WHERE id=2']]
    interread(t,groups,{'typed':expected},{'typed':{'sql':["INSERT INTO lake.main.typed VALUES(4,true,0,'new')",
        "UPDATE lake.main.typed SET note='changed' WHERE id=1","DELETE FROM lake.main.typed WHERE id=3"],
        'expected':[{'id':1,'flag':False,'amount':-9223372036854775808,'note':'changed'},
                    {'id':4,'flag':True,'amount':0,'note':'new'}]}},'typed-interread')


def multiple(t):
    query(t.a,'BEGIN')
    for name in ['zeta','empty','alpha']:create(t,name)
    query(t.a,'INSERT INTO biz.zeta VALUES(1,10),(2,NULL)');query(t.a,'UPDATE biz.zeta SET val=11 WHERE id=1')
    query(t.a,'INSERT INTO biz.alpha VALUES(7,70),(8,80)');query(t.a,'DELETE FROM biz.alpha WHERE id=7')
    query(t.a,'INSERT INTO biz.heap_marker VALUES(1)');query(t.a,'COMMIT')
    t.check('three definitions share one schema-version increment',t.head(),(1,1,4,2))
    t.check('three permanent table IDs allocated in creation order',query(t.a,t.sql.SQL('SELECT table_id,table_name FROM {}.ducklake_table ORDER BY table_id').format(t.sql.Identifier(t.schema))),
        [(1,'zeta'),(2,'empty'),(3,'alpha')])
    t.check('each table has its own flat column identity space',query(t.a,t.sql.SQL('SELECT table_id,column_id FROM {}.ducklake_column ORDER BY table_id,column_id').format(t.sql.Identifier(t.schema))),
        [(i,j) for i in (1,2,3) for j in (1,2)])
    t.check('only nonempty tables initialize row frontiers',query(t.a,t.sql.SQL('SELECT table_id,record_count,next_row_id FROM {}.ducklake_table_stats ORDER BY table_id').format(t.sql.Identifier(t.schema))),[(1,2,2),(3,1,1)])
    t.check('heap commits with all three new lake tables',query(t.observer,'SELECT * FROM biz.heap_marker'),[(1,)])
    groups=[['CREATE TABLE lake.main.zeta(id INTEGER,val INTEGER)','CREATE TABLE lake.main.empty(id INTEGER,val INTEGER)',
        'CREATE TABLE lake.main.alpha(id INTEGER,val INTEGER)','INSERT INTO lake.main.zeta VALUES(1,10),(2,NULL)',
        'UPDATE lake.main.zeta SET val=11 WHERE id=1','INSERT INTO lake.main.alpha VALUES(7,70),(8,80)',
        'DELETE FROM lake.main.alpha WHERE id=7']]
    business={'zeta':[{'id':1,'val':11},{'id':2,'val':None}],'empty':[],'alpha':[{'id':8,'val':80}]}
    continuation={'zeta':{'sql':['UPDATE lake.main.zeta SET val=1001 WHERE id=1','INSERT INTO lake.main.zeta VALUES(9,9)','DELETE FROM lake.main.zeta WHERE id=2'],
        'expected':[{'id':1,'val':1001},{'id':9,'val':9}]},
        'empty':{'sql':['INSERT INTO lake.main.empty VALUES(4,40)'],'expected':[{'id':4,'val':40}]},
        'alpha':{'sql':['UPDATE lake.main.alpha SET val=81 WHERE id=8'],'expected':[{'id':8,'val':81}]}}
    interread(t,groups,business,continuation,'multi-interread')


def names(t):
    special='订单,.:"\'X';names=[special,'MiXeD','space name']
    query(t.a,'CREATE SCHEMA biz2');query(t.a,'BEGIN')
    for index,name in enumerate(names,1):
        create(t,name);query(t.a,t.sql.SQL('INSERT INTO biz.{} VALUES(%s,%s)').format(t.sql.Identifier(name)),(index,index*10))
    query(t.a,'SAVEPOINT collision')
    reject(t,t.a,'ASCII name collision across host schemas',t.sql.SQL("CREATE FOREIGN TABLE biz2.mixed(id integer,val integer) SERVER {} OPTIONS(binding_mode 'create',lake_schema 'main')").format(t.sql.Identifier(t.server)),'42P07')
    query(t.a,'ROLLBACK TO collision');query(t.a,'RELEASE SAVEPOINT collision');query(t.a,'COMMIT')
    definitions=query(t.a,t.sql.SQL('SELECT table_id,table_name,table_uuid::text,path FROM {}.ducklake_table ORDER BY table_id').format(t.sql.Identifier(t.schema)))
    t.check('quoted names do not alter table identities',[x[:2] for x in definitions],[(i,n) for i,n in enumerate(names,1)])
    for tid,name,identity,path in definitions:
        expected=name+'/' if name=='MiXeD' else str(uuid.UUID(identity))+'/'
        t.check('safe relative data path for '+name,path,expected)
    statements=[];business={};continuation={}
    for i,name in enumerate(names,1):
        rel='lake.main.'+qi(name)
        statements+=['CREATE TABLE '+rel+'(id INTEGER,val INTEGER)','INSERT INTO '+rel+' VALUES('+str(i)+','+str(i*10)+')']
        business[name]=[{'id':i,'val':i*10}]
        continuation[name]={'sql':['UPDATE '+rel+' SET val=val+1','INSERT INTO '+rel+' VALUES(99,NULL)'],
            'expected':[{'id':i,'val':i*10+1},{'id':99,'val':None}]}
    interread(t,[statements],business,continuation,'quoted-interread')


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--mode',choices=['typed','multiple','names'],required=True)
    p.add_argument('--run-id',default=os.getenv('OG_LAKE_RUN_ID'))
    p.add_argument('--manifest',type=Path,default=os.getenv('OG_LAKE_FIXTURE_MANIFEST'))
    p.add_argument('--output',type=Path,default=os.getenv('OG_LAKE_RESULT_FILE'))
    args=p.parse_args()
    if not all([args.run_id,args.manifest,args.output]):p.error('run-id/real EMPTY_BUNDLE manifest/output required')
    validate_run_id(args.run_id);args.output=args.output.resolve();args.manifest=args.manifest.resolve()
    args.output.parent.mkdir(parents=True,exist_ok=True)
    result={'status':'fail','scope':'native_business_create_'+args.mode,'run_id':args.run_id,'assertions':[],'artifacts':[],
        'limitations':['Scoped business CREATE qualification; does not imply the full feature is accepted.',
        'No file SDK, physical carrier GC, crash, RAC or live external connector is covered.']}
    roles=None;db=None;control=None
    # Archive exact script/helper bytes; never leave a mutable live source path
    # as the only evidence of what executed.
    inputs=[Path(__file__).resolve(),Path(__file__).with_name('native_table_create.py'),
        Path(__file__).with_name('create_extended_oracle.py'),Path(__file__).with_name('create_reference_oracle.py'),
        DRIVER/'native_test_context.py',DRIVER/'tests/permission_api.py',DRIVER/'tests/empty_catalog_oracle.py',
        DRIVER/'tests/empty_manifest.py',DRIVER/'tests/empty_reference_fixture.py',DRIVER/'tests/reverse_reference.py',DRIVER/'inline_fixture.py']
    frozen={str(x):sha256(x) for x in inputs};archive=args.output.parent/'executed-inputs';archive.mkdir(exist_ok=False)
    for i,path in enumerate(inputs):shutil.copy2(path,archive/('%02d-'%i+path.name))
    write_json(archive/'input-hashes.json',frozen)
    try:
        import psycopg2
        control=psycopg2.connect(host=str(ROOT/'.runtime/opengauss-native-lake/debug/socket'),port=55560,
            dbname='postgres',user=os.getenv('USER','qiumc'),connect_timeout=10);control.autocommit=True
        roles=OwnedRoles(control,args.run_id,args.output.parent)
        with OwnedDatabase(args.run_id,args.output.parent) as db:
            t=Case(db,args,result,roles)
            {'typed':typed,'multiple':multiple,'names':names}[args.mode](t)
            t.check('all executed input files unchanged',{str(x):sha256(x) for x in inputs},frozen)
        result['status']='pass'
    except Blocked as exc:result.update(status='blocked',reason=str(exc))
    except BaseException as exc:result.update(reason=str(exc),traceback=traceback.format_exc())
    finally:
        if db is not None:result['database_cleanup']=db.cleanup_result;result['artifacts'].extend(db.artifacts())
        if roles is not None:
            try:
                if db is not None and db.cleanup_result and db.cleanup_result['returncode']==0:
                    result['role_cleanup']=cleanup_roles(control,roles.path,args.run_id)
                elif roles.items:raise RuntimeError('Database cleanup unproven; exact role cleanup deferred')
            except BaseException as exc:result.update(status='fail',cleanup_error=str(exc))
            result=roles.redact(result)
        if control is not None:control.close()
        seen={item['path'] for item in result['artifacts']}
        for path in args.output.parent.rglob('*'):
            if path.is_file() and path!=args.output and path.name!='database-cleanup.json' and str(path) not in seen:
                result['artifacts'].append({'path':str(path)});seen.add(str(path))
        write_json(args.output,result);print(json.dumps({'status':result['status'],'output':str(args.output)}))
    return {'pass':0,'blocked':2}.get(result['status'],1)

if __name__=='__main__':raise SystemExit(main())
