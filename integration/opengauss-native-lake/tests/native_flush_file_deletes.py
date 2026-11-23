#!/usr/bin/env python3
"""Actual native inline-file-delete consolidation and immutable history tests."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import json,os,shutil,subprocess,sys,time,traceback
from pathlib import Path
from types import SimpleNamespace
R=Path('/home/qiumc/data/code/cpp');D=R/'integration/opengauss-native-lake'
sys.path[:0]=[str(D),str(D/'tests')]
from file_dml_acceptance import check_build
from inline_fixture import query,scalar
from lake_harness import sha256,write_json
from native_table_create import Case
from native_test_context import OwnedDatabase
from permission_api import OwnedRoles,cleanup_roles
from empty_catalog_oracle import read_native_catalog,canonical_shape,transport_native
from reverse_reference import Reference,literal
MODES=['only','replace_history','mixed','rollback','cancel','concurrent_delete','concurrent_insert','nonzero_row_ids']

def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--run-id',required=True);p.add_argument('--mode',choices=MODES,required=True);p.add_argument('--build-receipt',type=Path,required=True);a=p.parse_args()
 out=R/'公共文档/openGauss原生湖存储/验证'/a.run_id;out.mkdir(exist_ok=False)
 result={'status':'fail','scope':'Actual bounded file-delete flush '+a.mode,'assertions':[],'limitations':['Existing position/output limits; no physical GC, large flush, remote or RAC qualification.']};db=roles=control=holder=executor=future=t=None;pid=None
 inputs=[Path(__file__),D/'native_test_context.py',D/'tests/native_table_create.py',D/'tests/permission_api.py',D/'tests/empty_catalog_oracle.py',D/'tests/reverse_reference.py',D/'inline_fixture.py'];result['input_hashes']={str(x):sha256(x)for x in inputs};archive=out/'executed-inputs';archive.mkdir()
 for x in inputs:shutil.copy2(x,archive/x.name)
 def check(name,got,want=True):
  ok=got==want;result['assertions'].append({'name':name,'observed':got,'expected':want,'status':'pass'if ok else'fail'})
  if not ok:raise AssertionError(name)
 def rejected(conn,statement,code):
  try:query(conn,statement)
  except Exception as exc:check('expected SQLSTATE '+statement,getattr(exc,'pgcode',None),code);return
  raise AssertionError('Expected SQL failure: '+statement)
 try:
  import psycopg2
  from psycopg2 import sql
  build=check_build(a.build_receipt);result.update(source_head=build['source_head'],binary_sha256=build['binary_sha256'],build=str(a.build_receipt),build_sha256=sha256(a.build_receipt))
  with (out/'fixture.log').open('w')as log:subprocess.run([sys.executable,str(D/'tests/catalog_fixture_bundle.py'),'--run-id',a.run_id,'--output',str(out/'fixture/manifest.json')],stdout=log,stderr=subprocess.STDOUT,check=True,timeout=300)
  control=psycopg2.connect(host=str(R/'.runtime/opengauss-native-lake/debug/socket'),port=55560,dbname='postgres',user=os.getenv('USER','qiumc'));control.autocommit=True
  db=OwnedDatabase(a.run_id,out).create();roles=OwnedRoles(control,a.run_id,out)
  args=SimpleNamespace(mode='net_dml',sql_version='0.11',manifest=out/'fixture/manifest.json',output=out/'result.json',run_id=a.run_id);t=Case(db,args,result,roles)
  check('exact runtime declares bounded file-delete flush',json.loads(scalar(t.a,'SELECT og_lake.host_status()')).get('bounded_file_delete_flush'))
  t.create();query(t.a,'INSERT INTO biz.orders VALUES(1,10),(2,20),(3,30)')
  def flush(conn=None):query(conn or t.a,"SELECT og_lake.flush('biz.orders'::regclass)")
  flush();root=Path(t.empty_manifest['data_path']);tid=scalar(t.a,'SELECT table_id FROM created_metadata.ducklake_table');carrier='created_metadata.ducklake_inlined_delete_'+str(tid)
  def marks(conn=None):return query(conn or t.observer,'SELECT file_id,row_id,begin_snapshot FROM '+carrier+' ORDER BY file_id,row_id,begin_snapshot')
  def files():return sorted(str(x)for x in root.rglob('*.parquet'))
  def deletes(conn=None):return query(conn or t.observer,'SELECT delete_file_id,data_file_id,begin_snapshot,partial_max,path,delete_count FROM created_metadata.ducklake_delete_file ORDER BY delete_file_id')
  data=scalar(t.a,'SELECT data_file_id FROM created_metadata.ducklake_data_file');data_path=files()[0];data_hash=sha256(data_path);snapshots=[(t.head()[0],t.rows(t.observer))]
  if a.mode=='nonzero_row_ids':
   query(t.a,'INSERT INTO biz.orders VALUES(4,40),(5,50),(6,60)');name=scalar(t.a,'SELECT table_name FROM created_metadata.ducklake_inlined_data_tables');logical=scalar(t.a,'SELECT _ducklake_row_id FROM created_metadata."'+name+'" WHERE id=5');flush();query(t.a,'DELETE FROM biz.orders WHERE id=5');before=marks();check('second-file logical identity differs from physical position',before[0][1]!=logical);flush();check('second-file marks flushed',marks(),[]);check('all other logical rows survive',t.rows(t.observer),[(1,10),(2,20),(3,30),(4,40),(6,60)])
  elif a.mode=='mixed':
   for statement in ['UPDATE biz.orders SET val=11 WHERE id=1','UPDATE biz.orders SET val=12 WHERE id=1','DELETE FROM biz.orders WHERE id=2']:
    query(t.a,statement);snapshots.append((t.head()[0],t.rows(t.observer)))
   old=marks();check('mixed input has two original-file marks',len(old),2);stats=query(t.a,'SELECT record_count,next_row_id FROM created_metadata.ducklake_table_stats');flush();check('mixed data and file-deletion outputs preserve current rows',t.rows(t.observer),[(1,12),(3,30)]);check('mixed inline marks cleared',marks(),[]);check('data and both deletion files registered',(scalar(t.a,'SELECT count(*) FROM created_metadata.ducklake_data_file'),len(deletes())),(2,2));check('mixed flush leaves logical stats counters unchanged',query(t.a,'SELECT record_count,next_row_id FROM created_metadata.ducklake_table_stats'),stats)
  else:
   query(t.a,'DELETE FROM biz.orders WHERE id=2');first=marks();check('one genuine original-file mark',len(first),1);check('mark references original immutable data file',first[0][0],data);snapshots.append((t.head()[0],t.rows(t.observer)))
   if a.mode in ['only','replace_history']:
    before=t.head();stats=query(t.a,'SELECT * FROM created_metadata.ducklake_table_stats');query(t.b,'BEGIN ISOLATION LEVEL READ COMMITTED');check('old reader sees deletion before marker migration',t.rows(t.b),[(1,10),(3,30)]);flush();check('file-only flush clears marker',marks(),[]);check('file-only flush creates exactly one snapshot',t.head()[0],before[0]+1);check('file-only flush never changes data-file statistics',query(t.a,'SELECT * FROM created_metadata.ducklake_table_stats'),stats);check('old reader survives marker migration',t.rows(t.b),[(1,10),(3,30)]);query(t.b,'ROLLBACK');check('original data file is never rewritten',sha256(data_path),data_hash)
    current=deletes();check('one active replacement position file',len(current),1);check('snapshot range preserves original deletion time',current[0][2:4],(first[0][2],first[0][2]))
    if a.mode=='replace_history':
     old=current[0];old_paths=list(root.rglob(old[4]));check('old deletion object exists',len(old_paths),1);old_hash=sha256(old_paths[0]);snapshots.append((t.head()[0],t.rows(t.observer)))
     query(t.b,'BEGIN ISOLATION LEVEL READ COMMITTED');check('old reader pins first deletion file',t.rows(t.b),[(1,10),(3,30)]);query(t.a,'DELETE FROM biz.orders WHERE id=1');new=marks();snapshots.append((t.head()[0],t.rows(t.observer)));flush()
     check('new and old marks merged',marks(),[]);current=deletes();check('old metadata replaced with one active file',len(current),1);check('new deletion identifier allocated',current[0][0]>old[0]);check('original earliest deletion snapshot preserved',current[0][2],first[0][2]);check('partial maximum covers later mark',current[0][3],new[0][2]);check('old physical deletion object retained for reader',sha256(old_paths[0]),old_hash);check('pinned reader still uses old metadata and object',t.rows(t.b),[(1,10),(3,30)]);query(t.b,'ROLLBACK')
     scheduled=query(t.a,'SELECT data_file_id,path,path_is_relative FROM created_metadata.ducklake_files_scheduled_for_deletion');check('one standard deferred deletion row',len(scheduled),1);check('scheduled ID is retired deletion ID',scheduled[0][0],old[0]);check('scheduled path relative to catalog root',scheduled[0][1],str(old_paths[0].relative_to(root)));check('scheduled path is explicitly relative',scheduled[0][2],True);check('current state after two deletions',t.rows(t.observer),[(3,30)])
   elif a.mode=='rollback':
    protocol=t.protocol();disk=files();query(t.a,'BEGIN');query(t.a,'SAVEPOINT child');flush();query(t.a,'ROLLBACK TO child');check('savepoint restores original complete catalog',t.protocol(),protocol);check('savepoint retains exact marker',marks(),first);check('savepoint discards only new candidate',files(),disk);query(t.a,'ROLLBACK');check('top rollback also preserves original data object',sha256(data_path),data_hash)
   elif a.mode=='concurrent_delete':
    disk=files();query(t.a,'BEGIN');flush();query(t.b,'DELETE FROM biz.orders WHERE id=1')
    late=[row for row in marks()if row not in first];check('one genuine post-cutoff file-position deletion',len(late),1)
    snapshots.append((t.head()[0],[(3,30)]));query(t.a,'COMMIT')
    check('late deletion and prepared marker flush both survive',t.rows(t.observer),[(3,30)])
    check('only the exact post-cutoff marker stays inline',marks(),late)
    current=deletes();check('prepared prior marker is registered in one deletion file',len(current),1)
    check('prepared output keeps original data file identity',current[0][1],data)
    check('prepared deletion range excludes the post-cutoff mark',current[0][2:4],(first[0][2],first[0][2]))
    check('prepared deletion output count exact',current[0][5],1)
    check('all old immutable objects retained and one new output committed',len(files()),len(disk)+1)
    check('late source deletion precedes the flush commit',late[0][2]<t.head()[0])
   elif a.mode=='concurrent_insert':
    query(t.a,'BEGIN');flush();query(t.b,'INSERT INTO biz.orders VALUES(4,40)');query(t.a,'COMMIT');check('insert and migrated marker coexist',t.rows(t.observer),[(1,10),(3,30),(4,40)]);check('original marker consumed',marks(),[])
   elif a.mode=='cancel':
    flush();query(t.a,'DELETE FROM biz.orders WHERE id=1');protocol=t.protocol();disk=files();old=deletes();holder=db.connect();query(holder,'BEGIN');query(holder,'LOCK TABLE created_metadata.ducklake_snapshot_changes IN SHARE MODE');query(t.a,'BEGIN');pid=scalar(t.a,'SELECT pg_backend_pid()');flush()
    def commit():
     try:query(t.a,'COMMIT');return 'accepted'
     except psycopg2.Error as exc:return exc.pgcode
    executor=ThreadPoolExecutor(max_workers=1);future=executor.submit(commit);deadline=time.monotonic()+10;waiting=[]
    while time.monotonic()<deadline:
     waiting=query(t.observer,"SELECT pid,mode FROM pg_locks WHERE relation='created_metadata.ducklake_snapshot_changes'::regclass AND pid=%s AND mode='RowExclusiveLock' AND NOT granted",(pid,))
     if waiting or future.done():break
     time.sleep(.02)
    check('merged deletion publisher reached final changeset wait',len(waiting),1);check('outside reader still sees old committed version',t.rows(t.observer),[(3,30)]);check('all staged retirement and marker changes invisible',t.protocol(),protocol);check('cancel exact owned merge publisher',scalar(t.observer,'SELECT pg_cancel_backend(%s)',(pid,)),True);check('real merge COMMIT cancellation',future.result(timeout=10),'57014');query(t.a,'ROLLBACK');check('retirement and inline clear rolled back',t.protocol(),protocol);check('old deletion metadata remains',deletes(),old);check('new merge candidate removed and old files retained',files(),disk)
  # Read-only exact catalog transport, no protocol repair or inferred identities.
  names=query(t.observer,"SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='created_metadata' AND c.relkind='r' ORDER BY c.relname")
  native=read_native_catalog(t.observer,'created_metadata',{'tables':[{'table_name':x}for x, in names]},out/'native-catalog.json');byname={x['table_name']:x for x in native['tables']}
  for standard in t.empty_export['tables']:check('unchanged standard shape '+standard['table_name'],canonical_shape(byname[standard['table_name']]),canonical_shape(standard))
  lock=json.loads((D/'source-lock.json').read_text());receipt=json.loads((R/'.reference/opengauss-native-lake/build/receipt.json').read_text());check('exact reference source lock',receipt['source_lock_hash'],sha256(D/'source-lock.json'));ref=Reference(out,lock,receipt);catalog=out/'transport.ducklake';transport_native(ref,catalog,native)
  actual=ref.run(ref.attach(catalog,True)+'SELECT rowid,id,val FROM lake.main.orders ORDER BY id','independent-current',result=True);check('independent DuckLake reads exact current values',[(x['id'],x['val'])for x in actual],t.rows(t.observer))
  for sid,expected in snapshots:
   actual=ref.run(ref.attach(catalog,True)+'SELECT id,val FROM lake.main.orders AT (VERSION => '+str(sid)+') ORDER BY id','independent-history-'+str(sid),result=True);check('independent retained history '+str(sid),[(x['id'],x['val'])for x in actual],expected)
  check('all old data bytes unchanged',sha256(data_path),data_hash);check('all executed inputs fixed',all(sha256(p)==h for p,h in result['input_hashes'].items()));check_build(a.build_receipt);result['status']='pass'
 except BaseException as exc:result.update(reason=str(exc),traceback=traceback.format_exc())
 finally:
  if future is not None and not future.done():
   try:scalar(t.observer,'SELECT pg_cancel_backend(%s)',(pid,))
   except BaseException:pass
  if holder is not None:
   try:query(holder,'ROLLBACK')
   except BaseException:pass
  if executor is not None:executor.shutdown(wait=True)
  if db is not None:
   try:db.cleanup();result['database_cleanup']=db.cleanup_result
   except BaseException as exc:result.update(status='fail',cleanup_error=str(exc))
  if roles is not None:
   try:
    if db is None or not db.cleanup_result or db.cleanup_result['returncode']!=0:raise RuntimeError('DB cleanup unproven; role retained')
    result['role_cleanup']=cleanup_roles(control,roles.path,a.run_id);result=roles.redact(result)
   except BaseException as exc:result.update(status='fail',role_cleanup_error=str(exc))
  if control is not None:control.close()
  write_json(out/'result.json',result)
 print(json.dumps({'status':result['status'],'mode':a.mode,'reason':result.get('reason'),'checks':len(result['assertions']),'result':str(out/'result.json')}));return 0 if result['status']=='pass'else 1
if __name__=='__main__':raise SystemExit(main())
