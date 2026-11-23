#!/usr/bin/env python3
"""Actual SQL0.10 file-origin DML; real Parquet and standard catalog inputs."""
import argparse,json,os,shutil,sys,time,traceback,uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
ROOT=Path('/home/qiumc/data/code/cpp');DRIVER=ROOT/'integration/opengauss-native-lake';sys.path[:0]=[str(DRIVER),str(DRIVER/'tests')]
from native_file_read import Case
from native_test_context import OwnedDatabase,reviewed_runtime_version
from permission_api import OwnedRoles,cleanup_roles
from inline_fixture import query,scalar
from lake_harness import sha256,write_json,validate_run_id
from file_catalog_oracle import check_file_catalog

class ModifyCase(Case):
 def __init__(self,db,args,result,roles):
  args.installed_version='0.10';args.read_only=False
  args.fixture_profile='FILE_UNKNOWN_BOUNDS'if args.mode=='stats_unknown'else 'FILE_VERSIONED'if args.mode=='versioned'else 'FILE_SCHEMA_GAP'if args.mode=='schema_gap'else 'F'
  super().__init__(db,args,result,roles)
  caps=json.loads(scalar(self.a,'SELECT og_lake.host_status()'));self.check('exact reviewed file-DML runtime with retained SQL0.10',caps['module_version'],reviewed_runtime_version());self.check('candidate declares file-inline DML',caps.get('file_inline_dml'),True)
  self.bind();self.check('initial file rows equal genuine reference',self.rows(),self.expected)
  self.initial_head=self.head();self.initial_next=self.next_id();self.ids={r['id']:r['logical_rowid']for r in self.manifest['row_identity_map']}
  self.original=self.protocol();self.final=dict(self.expected)
  query(self.a,'CREATE TABLE biz.heap_marker(id integer)')
 def head(self,conn=None):return scalar(conn or self.a,'SELECT max(snapshot_id) FROM lake_metadata.ducklake_snapshot')
 def next_id(self):return scalar(self.a,'SELECT next_row_id FROM lake_metadata.ducklake_table_stats WHERE table_id=%s',(self.manifest['table_id'],))
 def carriers(self):return [r[0]for r in query(self.a,'SELECT table_name FROM lake_metadata.ducklake_inlined_data_tables WHERE table_id=%s',(self.manifest['table_id'],))]
 def deleted(self):
  name='ducklake_inlined_delete_'+str(self.manifest['table_id'])
  if not scalar(self.a,"SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='lake_metadata'AND c.relname=%s",(name,)):return []
  return query(self.a,self.sql.SQL('SELECT file_id,row_id,begin_snapshot FROM lake_metadata.{} ORDER BY file_id,row_id,begin_snapshot').format(self.sql.Identifier(name)))
 def protocol(self,conn=None):
  conn=conn or self.a;image={}
  for name,oid in query(conn,"SELECT c.relname,c.oid FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='lake_metadata'AND c.relkind='r'ORDER BY c.relname"):
   rows=query(conn,self.sql.SQL('SELECT * FROM lake_metadata.{}').format(self.sql.Identifier(name)))
   image[name]={'oid':oid,'rows':sorted(json.dumps(r,default=str)for r in rows)}
  return image
 def current(self,name):self.check(name,self.rows(),sorted(self.final.items()))
 def marker_for(self,key):
  observed=next(r for r in self.manifest['row_identity_map']if r['id']==key)
  positions=[r['file_row_number']for r in self.manifest['physical_positions']if r['id']==key and
             ('_ducklake_internal_snapshot_id'not in r or r['_ducklake_internal_snapshot_id']==observed['begin_snapshot'])]
  if len(positions)!=1:raise AssertionError('genuine current physical origin not unique')
  return self.manifest['data_files'][0]['data_file_id'],positions[0]
 def interread(self):
  self.result['independent_file_interread']=check_file_catalog(self,self.args.output.parent/'reference-interread',self.ids,continuation=self.args.mode=='interop')


def execute(t):
 mode=t.args.mode
 if mode in ('commands','repeat','versioned','schema_gap','interop','stats_unknown'):
  original=dict(t.expected);key=2 if mode=='stats_unknown'else 1;new=7 if mode=='stats_unknown'else original[key]+7
  query(t.a,'BEGIN');query(t.a,'INSERT INTO biz.heap_marker VALUES(1)')
  t.check('file UPDATE RETURNING uses actual business row',query(t.a,'UPDATE biz.orders SET val=%s WHERE id=%s RETURNING id,val',(new,key)),[(key,new)])
  t.final[key]=new;t.current('own later statement overlays private file after-image')
  t.check('other session still sees old committed file rows',t.rows(t.observer),t.expected)
  t.check('no snapshot published before COMMIT',t.head(t.observer),t.initial_head)
  t.check('no standard carrier/data mutation before COMMIT',t.protocol(t.observer),t.original)
  if mode=='repeat':
   query(t.a,'UPDATE biz.orders SET val=val*10 WHERE id=1');t.final[1]*=10;t.current('second UPDATE uses current private after-image')
  query(t.a,'COMMIT');t.current('file UPDATE commit exposes only final version')
  t.check('UPDATE preserves row identity allocation frontier',t.next_id(),t.initial_next)
  t.check('one snapshot for the outer UPDATE transaction',t.head(),t.initial_head+1)
  t.check('ordinary heap commits with file change',query(t.a,'SELECT id FROM biz.heap_marker'),[(1,)])
  file_id,position=t.marker_for(key)
  t.check('deletion names the exact original physical position',t.deleted(),[(file_id,position,t.head())])
  carriers=t.carriers();t.check('one current standard data carrier',len(carriers),1)
  physical=query(t.a,t.sql.SQL('SELECT _ducklake_row_id,_ducklake_begin_snapshot,_ducklake_end_snapshot,id,val FROM lake_metadata.{}').format(t.sql.Identifier(carriers[0])))
  t.check('only final version persisted; logical identity preserved',physical,[(t.ids[key],t.head(),None,key,t.final[key])])
  if mode=='repeat':
   query(t.a,'UPDATE biz.orders SET val=val+1 WHERE id=1');t.final[1]+=1;t.current('next transaction updates inline successor without reviving file origin')
   t.check('original file delete marker survives successor update',t.deleted(),[(file_id,position,t.initial_head+1)])
   t.check('two committed updates retain allocation frontier',t.next_id(),t.initial_next)
  if mode=='stats_unknown':
   t.check('unknown file predecessor bounds cannot be narrowed to only new inline values',query(t.a,"SELECT min_value,max_value FROM lake_metadata.ducklake_table_column_stats WHERE table_id=%s AND column_id=(SELECT column_id FROM lake_metadata.ducklake_column WHERE table_id=%s AND column_name='val'AND end_snapshot IS NULL)",(t.manifest['table_id'],t.manifest['table_id'])),[(None,None)])
  t.interread()
 elif mode=='delete':
  t.check('file DELETE RETURNING returns old value',query(t.a,'DELETE FROM biz.orders WHERE id=1 RETURNING id,val'),[t.expected[0]])
  del t.final[1];del t.ids[1];t.current('file delete excludes only its original position')
  t.check('delete-only does not create unnecessary data carrier',t.carriers(),[])
  t.check('delete-only retains allocation frontier',t.next_id(),t.initial_next);t.interread()
 elif mode=='insert':
  query(t.a,'INSERT INTO biz.orders VALUES(4,400)');t.final[4]=400;t.ids[4]=t.initial_next;t.current('first inline INSERT coexists with immutable file rows')
  t.check('INSERT advances permanent row frontier once',t.next_id(),t.initial_next+1)
  t.check('INSERT creates no file delete table',t.deleted(),[]);t.interread()
 elif mode=='rollback':
  query(t.a,'BEGIN');query(t.a,'UPDATE biz.orders SET val=99 WHERE id=1');query(t.a,'DELETE FROM biz.orders WHERE id=2');query(t.a,'INSERT INTO biz.orders VALUES(4,400)');query(t.a,'INSERT INTO biz.heap_marker VALUES(1)')
  t.check('private mixed file changes are visible to own later query',t.rows(),[(1,99),(3,200),(4,400)])
  query(t.a,'ROLLBACK');t.current('outer rollback restores all original file rows')
  t.check('rollback leaves all protocol values and carrier OIDs unchanged',t.protocol(),t.original)
  t.check('ordinary heap rolls back with file journal',query(t.a,'SELECT * FROM biz.heap_marker'),[])
 elif mode=='savepoints':
  query(t.a,'BEGIN');query(t.a,'UPDATE biz.orders SET val=11 WHERE id=1');t.final[1]=11
  query(t.a,'SAVEPOINT child');query(t.a,'UPDATE biz.orders SET val=99 WHERE id=1');query(t.a,'DELETE FROM biz.orders WHERE id=2');query(t.a,'INSERT INTO biz.orders VALUES(4,400)')
  t.check('child private edits are all visible',t.rows(),[(1,99),(3,200),(4,400)])
  query(t.a,'ROLLBACK TO child');t.current('savepoint rollback restores parent file after-image and deleted rows')
  query(t.a,'SAVEPOINT nested');query(t.a,'UPDATE biz.orders SET val=22 WHERE id=2');t.final[2]=22;query(t.a,'RELEASE nested');query(t.a,'COMMIT')
  t.current('released child and parent file edits commit together')
  t.check('rolled-back insert did not consume permanent identity',t.next_id(),t.initial_next);t.interread()
 elif mode=='update_delete':
  query(t.a,'BEGIN');query(t.a,'UPDATE biz.orders SET val=11 WHERE id=1');query(t.a,'DELETE FROM biz.orders WHERE id=1');query(t.a,'INSERT INTO biz.orders VALUES(1,111)')
  t.final[1]=111;t.current('delete plus new logical row overlays original file')
  query(t.a,'COMMIT');t.ids[1]=t.initial_next;t.current('delete and independent INSERT publish atomically')
  t.check('delete/reinsert gets a distinct new permanent identity',t.next_id(),t.initial_next+1);t.interread()
 elif mode=='limits':
  query(t.a,"INSERT INTO lake_metadata.ducklake_metadata(key,value,scope,scope_id)VALUES('data_inlining_row_limit','0',NULL,NULL)")
  t.reject('disabled inline path rejects file UPDATE','UPDATE biz.orders SET val=11 WHERE id=1','0A000');t.current('disabled path leaves file data unchanged')
  query(t.a,"UPDATE lake_metadata.ducklake_metadata SET value='1'WHERE key='data_inlining_row_limit'")
  query(t.a,'BEGIN');query(t.a,'UPDATE biz.orders SET val=val+1 WHERE id IN(1,2)');t.reject('larger batch requiring direct-file output rejects at atomic commit','COMMIT','0A000')
  t.current('failed batch leaves every old row visible')
  t.check('failed direct-file batch creates no data carrier',t.carriers(),[])
 elif mode=='conflict':
  query(t.a,'BEGIN');query(t.observer,'BEGIN')
  query(t.a,'UPDATE biz.orders SET val=11 WHERE id=1');query(t.observer,'UPDATE biz.orders SET val=22 WHERE id=2')
  query(t.a,'COMMIT');t.final[1]=11
  t.reject('stale file writer detects committed change','COMMIT','40001',t.observer)
  t.current('successful file writer remains exact after conflict')
  query(t.observer,'UPDATE biz.orders SET val=22 WHERE id=2');t.final[2]=22;t.current('fresh application retry commits on current file/inline state');t.interread()
 elif mode=='cancel':
  holder=t.db.connect();observer=t.observer;future=executor=None;verified=False;pid=None
  app='lake_file_commit_'+uuid.uuid4().hex[:16]
  query(t.a,'SET application_name=%s',(app,));query(t.a,'SET statement_timeout=30000')
  registration=scalar(observer,"SELECT 'lake_metadata.ducklake_inlined_data_tables'::regclass::oid")
  database=scalar(observer,'SELECT oid FROM pg_database WHERE datname=current_database()')
  try:
   query(holder,'BEGIN');query(holder,'LOCK TABLE lake_metadata.ducklake_inlined_data_tables IN SHARE MODE')
   query(t.a,'BEGIN');pid=scalar(t.a,'SELECT pg_backend_pid()');query(t.a,'UPDATE biz.orders SET val=11 WHERE id=1');query(t.a,'INSERT INTO biz.heap_marker VALUES(1)')
   def commit():
    import psycopg2
    try:query(t.a,'COMMIT');return {'state':'accepted'}
    except psycopg2.Error as exc:return {'state':exc.pgcode,'message':str(exc)}
   executor=ThreadPoolExecutor(max_workers=1);future=executor.submit(commit);deadline=time.monotonic()+12;waits=[]
   while time.monotonic()<deadline:
    waits=query(observer,"SELECT pid,database,relation,mode FROM pg_locks WHERE locktype='relation'AND database=%s AND relation=%s AND pid=%s AND mode='RowExclusiveLock'AND NOT granted",(database,registration,pid))
    if waits or future.done():break
    time.sleep(.02)
   t.check('publisher reaches real first-carrier registration wait',len(waits),1)
   t.check('wait belongs to owned publisher connection',query(observer,'SELECT application_name FROM pg_stat_activity WHERE pid=%s',(pid,)),[(app,)])
   verified=True;t.result['publication_barrier']={'database_oid':database,'pid':pid,'relation_oid':registration,'waits':waits,'source_order':'registration INSERT follows snapshot claim, carrier DDL and private OID registration'}
   t.check('partial carrier publication stays invisible',t.protocol(observer),t.original)
   t.check('cancel exact owned publication',scalar(observer,'SELECT pg_cancel_backend(%s)',(pid,)),True)
   outcome=future.result(timeout=10);t.result['cancelled_commit']=outcome;t.check('cancel COMMIT returns57014',outcome['state'],'57014')
   query(t.a,'ROLLBACK');t.current('cancelled file publication restores source rows')
   t.check('cancel removes snapshot and new carrier/OID effects',t.protocol(),t.original)
   t.check('ordinary heap write rolls back with cancelled file commit',query(t.a,'SELECT * FROM biz.heap_marker'),[])
   t.check('cancelled connection can be reused',scalar(t.a,'SELECT 42'),42)
  finally:
   if future is not None and not future.done()and verified:scalar(observer,'SELECT pg_cancel_backend(%s)',(pid,))
   query(holder,'ROLLBACK')
   if executor is not None:executor.shutdown(wait=True)
   query(t.a,'ROLLBACK')
 elif mode=='acl':
  name=t.roles.create('writer',True)
  for statement in ['GRANT USAGE ON SCHEMA biz TO {}','GRANT USAGE ON FOREIGN SERVER lake_reference TO {}','GRANT SELECT,UPDATE ON biz.orders TO {}']:
   query(t.a,t.sql.SQL(statement).format(t.sql.Identifier(name)))
  login,identity=t.roles.connect(t.db,'writer');t.result['ordinary_identity']=identity
  t.check('ordinary LOGIN updates a file-origin row with RETURNING',query(login,'UPDATE biz.orders SET val=11 WHERE id=1 RETURNING id,val'),[(1,11)])
  t.final[1]=11;t.current('ordinary file write published correct business result')
  t.reject('ordinary writer cannot bypass private carrier permission','SELECT * FROM lake_metadata.ducklake_inlined_delete_'+str(t.manifest['table_id']),'42501',login)
  t.interread()
 else:raise ValueError('unimplemented test mode '+mode)
 t.check('source file descriptors drained after DML',len(t.descriptors()),0)


def main():
 p=argparse.ArgumentParser();p.add_argument('--mode',choices=['commands','delete','insert','repeat','rollback','savepoints','update_delete','versioned','limits','conflict','cancel','acl','interop','schema_gap','stats_unknown'],required=True)
 p.add_argument('--run-id',default=os.getenv('OG_LAKE_RUN_ID'));p.add_argument('--manifest',type=Path,default=os.getenv('OG_LAKE_FIXTURE_MANIFEST'));p.add_argument('--output',type=Path,default=os.getenv('OG_LAKE_RESULT_FILE'));args=p.parse_args()
 if not all([args.run_id,args.manifest,args.output]):p.error('run-id,manifest,output required')
 validate_run_id(args.run_id);args.output=args.output.resolve();args.manifest=args.manifest.resolve();args.output.parent.mkdir(parents=True,exist_ok=True)
 result={'status':'fail','scope':'SQL0.10 bounded standard file-origin DML '+args.mode,'run_id':args.run_id,'assertions':[],'artifacts':[],'limitations':['Positive effective inline limit and bounded path only; no direct-file DML, Puffin, flush, schema evolution, SMP, remote or RAC qualification']}
 inputs=[Path(__file__),DRIVER/'tests/native_file_read.py',DRIVER/'tests/file_catalog_oracle.py',DRIVER/'tests/empty_catalog_oracle.py',DRIVER/'tests/reverse_reference.py',DRIVER/'tests/permission_api.py',DRIVER/'native_test_context.py']
 hashes={str(f):sha256(f)for f in inputs};archive=args.output.parent/'inputs';archive.mkdir(exist_ok=False)
 for n,f in enumerate(inputs):shutil.copy2(f,archive/(str(n)+'-'+f.name))
 control=roles=db=None
 try:
  import psycopg2
  control=psycopg2.connect(host=str(ROOT/'.runtime/opengauss-native-lake/debug/socket'),port=55560,dbname='postgres',user=os.getenv('USER','qiumc'),connect_timeout=10);control.autocommit=True
  roles=OwnedRoles(control,args.run_id,args.output.parent)
  with OwnedDatabase(args.run_id,args.output.parent)as db:
   t=ModifyCase(db,args,result,roles);execute(t);t.finish();t.check('executed inputs remain frozen',{str(f):sha256(f)for f in inputs},hashes)
  result['status']='pass'
 except BaseException as exc:result.update(reason=str(exc),traceback=traceback.format_exc())
 finally:
  if db is not None:result['database_cleanup']=db.cleanup_result;result['artifacts'].extend(db.artifacts())
  if roles is not None:
   try:
    if db is not None and db.cleanup_result and db.cleanup_result['returncode']==0:result['role_cleanup']=cleanup_roles(control,roles.path,args.run_id)
    elif roles.items:raise RuntimeError('owned database cleanup unproven; roles retained')
   except BaseException as exc:result.update(status='fail',cleanup_error=str(exc))
   result=roles.redact(result)
  if control is not None:control.close()
  seen={x['path']for x in result['artifacts']}
  result['artifacts'].extend({'path':str(f)}for f in args.output.parent.rglob('*')if f.is_file()and f!=args.output and f.name!='database-cleanup.json'and str(f)not in seen)
  write_json(args.output,result);print(json.dumps({'status':result['status'],'reason':result.get('reason'),'output':str(args.output)}))
 return 0 if result['status']=='pass'else 1
if __name__=='__main__':raise SystemExit(main())
