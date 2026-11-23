#!/usr/bin/env python3
"""Real overlapping transactions exercise CREATE publication after pinned reads."""
import argparse,json,os,shutil,sys,traceback
from pathlib import Path
ROOT=Path('/home/qiumc/data/code/cpp');DRIVER=ROOT/'integration/opengauss-native-lake'
sys.path[:0]=[str(DRIVER),str(DRIVER/'tests')]
from native_table_create import Case,reject
from native_create_extended import create
from create_extended_oracle import interread
from inline_fixture import query,scalar
from native_test_context import OwnedDatabase
from permission_api import OwnedRoles,cleanup_roles
from reverse_reference import qi
from lake_harness import Blocked,sha256,validate_run_id,write_json


def seed(t):
    query(t.a,'CREATE SCHEMA biz2');query(t.a,'BEGIN');create(t,'seed')
    query(t.a,'INSERT INTO biz.seed VALUES(1,10)');query(t.a,'COMMIT')
    t.check('seed commit has one durable table and row',t.head(),(1,1,2,1))
    return [['CREATE TABLE lake.main.seed(id INTEGER,val INTEGER)','INSERT INTO lake.main.seed VALUES(1,10)']]


def merged_create(t):
    groups=seed(t);special='later,.:"\'表'
    query(t.a,'BEGIN');t.check('A fixes its read population at S1',query(t.a,'SELECT * FROM biz.seed'),[(1,10)])
    query(t.b,'BEGIN');create(t,special,conn=t.b,schema='biz2')
    query(t.b,t.sql.SQL('INSERT INTO biz2.{} VALUES(2,20)').format(t.sql.Identifier(special)));query(t.b,'COMMIT')
    t.check('B independently publishes a quoted table at S2',t.head(),(2,2,3,2))
    create(t,'after');query(t.a,'INSERT INTO biz.after VALUES(3,30)');query(t.a,'UPDATE biz.seed SET val=11 WHERE id=1')
    t.check('A own old-snapshot update and new table coexist',query(t.a,'SELECT * FROM biz.seed'),[(1,11)])
    query(t.a,'COMMIT');t.check('A merges quoted later CREATE and allocates fresh table ID',t.head(),(3,3,4,4))
    t.check('A does not overwrite the concurrent table definition',query(t.observer,t.sql.SQL('SELECT table_id,table_name FROM {}.ducklake_table ORDER BY table_id').format(t.sql.Identifier(t.schema))),[(1,'seed'),(2,special),(3,'after')])
    groups += [['CREATE TABLE lake.main.'+qi(special)+'(id INTEGER,val INTEGER)','INSERT INTO lake.main.'+qi(special)+' VALUES(2,20)'],
        ['CREATE TABLE lake.main.after(id INTEGER,val INTEGER)','INSERT INTO lake.main.after VALUES(3,30)','UPDATE lake.main.seed SET val=11 WHERE id=1']]
    business={'seed':[{'id':1,'val':11}],special:[{'id':2,'val':20}],'after':[{'id':3,'val':30}]}
    interread(t,groups,business,{},'merged-quoted-create')


def conflicting_create(t):
    groups=seed(t);query(t.a,'BEGIN');query(t.a,'SELECT * FROM biz.seed')
    query(t.b,'BEGIN');create(t,'future',conn=t.b,schema='biz2');query(t.b,'INSERT INTO biz2.future VALUES(2,20)');query(t.b,'COMMIT')
    baseline=t.protocol()
    create(t,'future');query(t.a,'INSERT INTO biz.future VALUES(3,30)');query(t.a,'INSERT INTO biz.heap_marker VALUES(9)')
    t.check('A stages name not visible in its pinned S1 catalog',query(t.a,'SELECT * FROM biz.future'),[(3,30)])
    reject(t,t.a,'later committed same logical name conflicts at publication','COMMIT','40001')
    query(t.a,'ROLLBACK')
    t.check('failed CREATE restores the exact committed B catalog',t.protocol(),baseline)
    t.check('same-name failure rolls back ordinary heap changes',query(t.observer,'SELECT * FROM biz.heap_marker'),[])
    t.check('same-name failure removes only the losing host object',scalar(t.observer,"SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='biz' AND c.relname='future'"),0)
    t.check('winner host and rows remain readable',query(t.observer,'SELECT * FROM biz2.future'),[(2,20)])
    t.check('loser consumes no catalog/row/file counters',t.head(),(2,2,3,2))
    groups += [['CREATE TABLE lake.main.future(id INTEGER,val INTEGER)','INSERT INTO lake.main.future VALUES(2,20)']]
    interread(t,groups,{'seed':[{'id':1,'val':10}],'future':[{'id':2,'val':20}]},{},'same-name-conflict')


def merged_insert(t):
    groups=seed(t);query(t.a,'BEGIN');create(t,'pending');query(t.a,'INSERT INTO biz.pending VALUES(3,30)')
    query(t.b,'BEGIN');query(t.b,'INSERT INTO biz.seed VALUES(2,20)');query(t.b,'COMMIT')
    t.check('ordinary INSERT can publish while A holds CREATE registration lock',t.head(),(2,1,2,2))
    t.check('A keeps fixed business snapshot after B commits',query(t.a,'SELECT * FROM biz.seed'),[(1,10)])
    query(t.a,'COMMIT');t.check('A publishes after unrelated committed insertion',t.head(),(3,2,3,3))
    t.check('both durable row populations remain visible',query(t.observer,'SELECT * FROM biz.seed ORDER BY id'),[(1,10),(2,20)])
    groups += [['INSERT INTO lake.main.seed VALUES(2,20)'],['CREATE TABLE lake.main.pending(id INTEGER,val INTEGER)','INSERT INTO lake.main.pending VALUES(3,30)']]
    interread(t,groups,{'seed':[{'id':1,'val':10},{'id':2,'val':20}],'pending':[{'id':3,'val':30}]},{},'create-after-insert')


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--mode',choices=['merged_create','conflicting_create','merged_insert'],required=True)
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
    inputs=[Path(__file__).resolve(),Path(__file__).with_name('native_create_extended.py'),Path(__file__).with_name('native_table_create.py'),
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
            {'merged_create':merged_create,'conflicting_create':conflicting_create,'merged_insert':merged_insert}[args.mode](t)
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
