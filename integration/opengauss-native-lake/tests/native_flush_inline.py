#!/usr/bin/env python3
"""Actual bounded SQL0.11 flush: history, concurrency and atomic rollback.

Uses normal native CREATE/DML, real immutable Parquet and pinned DuckLake offline
interread. Offline catalog transport copies exact observed values, never repairs.
"""
import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
import traceback
from types import SimpleNamespace
ROOT=Path('/home/qiumc/data/code/cpp');D=ROOT/'integration/opengauss-native-lake'
sys.path[:0]=[str(D),str(D/'tests')]
from file_dml_acceptance import check_build
from inline_fixture import query,scalar
from lake_harness import sha256,write_json
from native_table_create import Case
from native_test_context import OwnedDatabase
from permission_api import OwnedRoles,cleanup_roles
from empty_catalog_oracle import read_native_catalog,canonical_shape,transport_native
from reverse_reference import Reference,literal
MODES=['history','typed','concurrent_update','old_writer','concurrent_insert','rollback','savepoints','publish_cancel','repeat_file_dml']

def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--run-id',required=True);p.add_argument('--mode',choices=MODES,required=True);p.add_argument('--build-receipt',type=Path,required=True);a=p.parse_args()
 out=ROOT/'公共文档/openGauss原生湖存储/验证'/a.run_id;out.mkdir(exist_ok=False)
 result={'status':'fail','scope':'Actual SQL0.11 bounded inline flush '+a.mode,'assertions':[],'artifacts':[],'limitations':['Bounded local inline data history only; existing file-delete carriers not consolidated.','No multi-output large flush, crash GC, background scheduler, remote or RAC qualification.']}
 db=roles=control=None;t=None
 inputs=[Path(__file__),D/'native_test_context.py',D/'tests/native_table_create.py',D/'tests/permission_api.py',D/'tests/empty_catalog_oracle.py',D/'tests/create_reference_oracle.py',D/'tests/reverse_reference.py',D/'inline_fixture.py']
 result['input_hashes']={str(x):sha256(x) for x in inputs};archive=out/'executed-inputs';archive.mkdir()
 for x in inputs:shutil.copy2(x,archive/x.name)
 def check(name,got,want=True):
  ok=got==want;result['assertions'].append({'name':name,'observed':got,'expected':want,'status':'pass'if ok else'fail'})
  if not ok:raise AssertionError(name)
 def error(conn,statement,code,params=None):
  try:query(conn,statement,params)
  except Exception as exc:check('SQLSTATE '+statement,getattr(exc,'pgcode',None),code);return
  raise AssertionError('Expected failure: '+statement)
 try:
  import psycopg2
  from psycopg2 import sql
  built=check_build(a.build_receipt);result.update(source_head=built['source_head'],binary_sha256=built['binary_sha256'],build=str(a.build_receipt),build_sha256=sha256(a.build_receipt))
  manifest=out/'fixture/manifest.json'
  with (out/'fixture.log').open('w') as log:subprocess.run([sys.executable,str(D/'tests/catalog_fixture_bundle.py'),'--run-id',a.run_id,'--output',str(manifest)],stdout=log,stderr=subprocess.STDOUT,check=True,timeout=300)
  db=OwnedDatabase(a.run_id,out).create();control=psycopg2.connect(host=str(ROOT/'.runtime/opengauss-native-lake/debug/socket'),port=55560,dbname='postgres',user=os.getenv('USER','qiumc'));control.autocommit=True;roles=OwnedRoles(control,a.run_id,out)
  args=SimpleNamespace(mode='net_dml',sql_version='0.11',manifest=manifest,output=out/'result.json',run_id=a.run_id)
  t=Case(db,args,result,roles)
  if a.mode=='typed':
   query(t.a,"CREATE FOREIGN TABLE biz.orders(id integer,val integer,flag boolean,bigval bigint,note text) SERVER created_lake OPTIONS(binding_mode 'create',lake_schema 'main')")
   query(t.a,'INSERT INTO biz.orders VALUES(1,10,true,9223372036854775807,%s),(2,20,NULL,-9223372036854775808,NULL),(3,30,false,NULL,%s)',('湖'*12000,''))
  else:t.create();query(t.a,'INSERT INTO biz.orders VALUES(1,10),(2,20),(3,30)')
  check('actual runtime declares bounded flush',json.loads(scalar(t.a,'SELECT og_lake.host_status()')).get('bounded_inline_flush'))
  carrier=scalar(t.a,'SELECT table_name FROM created_metadata.ducklake_inlined_data_tables');root=Path(t.empty_manifest['data_path'])
  def disk():return sorted(str(x) for x in root.rglob('*.parquet')) if root.exists() else []
  def raw(conn=None):return query(conn or t.observer,sql.SQL('SELECT * FROM created_metadata.{} ORDER BY _ducklake_row_id,_ducklake_begin_snapshot').format(sql.Identifier(carrier)))
  def flush(conn=None):query(conn or t.a,"SELECT og_lake.flush('biz.orders'::regclass)")
  initial=t.protocol();initial_disk=disk();check('genuine starting table is entirely inline',initial_disk,[])
  history=[]
  if a.mode=='history':
   history.append((t.head()[0],t.rows()))
   for statement in ['UPDATE biz.orders SET val=11 WHERE id=1','UPDATE biz.orders SET val=12 WHERE id=1','DELETE FROM biz.orders WHERE id=2']:
    query(t.a,statement);history.append((t.head()[0],t.rows()))
  if a.mode in ['history','typed']:
   before=raw();before_rows=query(t.a,'SELECT * FROM biz.orders ORDER BY id');head=t.head();stats=query(t.a,'SELECT record_count,next_row_id,file_size_bytes FROM created_metadata.ducklake_table_stats')
   query(t.b,'BEGIN ISOLATION LEVEL READ COMMITTED');check('old SQL snapshot before flush',t.rows(t.b),t.rows())
   query(t.a,'BEGIN');flush();check('prepared flush does not publish metadata early',t.head(),head);check('prepared flush leaves input visible to own commands',query(t.a,'SELECT * FROM biz.orders ORDER BY id'),before_rows)
   check('other reader still sees exact unmodified source versions',raw(),before);check('immutable files exist before final claim',len(disk()),2 if a.mode=='history' else 1)
   query(t.a,'COMMIT');check('native rows unchanged after flush',query(t.a,'SELECT * FROM biz.orders ORDER BY id'),before_rows)
   check('old SQL reader survives physical inline DELETE',t.rows(t.b),[(r[0],r[1])for r in before_rows]);query(t.b,'ROLLBACK')
   check('exact transferred inline versions removed',raw(),[]);after=t.head();check('one committed maintenance snapshot',after[0],head[0]+1);check('flush never allocates schema/catalog IDs',after[1:3],head[1:3])
   check('flush table record_count and next_row_id unchanged',query(t.a,'SELECT record_count,next_row_id FROM created_metadata.ducklake_table_stats'),[(stats[0][0],stats[0][1])])
   result['expected_physical_versions']=before;result['expected_history']=history
  elif a.mode=='concurrent_update':
   # Native SQL0.11 now reconciles this late end under the snapshot PK.
   # Old-locator and competing-flush conflicts remain separate safety cases.
   history.append((t.head()[0],[(1,10),(2,20),(3,30)]))
   query(t.a,'BEGIN');flush();check('candidate file actually prepared',len(disk()),1)
   query(t.b,'UPDATE biz.orders SET val=11 WHERE id=1');late=t.head()[0]
   history.append((late,[(1,11),(2,20),(3,30)]));query(t.a,'COMMIT')
   check('read-after-prepare UPDATE retained exactly once',t.rows(),[(1,11),(2,20),(3,30)])
   check('reconciled flush registers its immutable data file',scalar(t.a,'SELECT count(*) FROM created_metadata.ducklake_data_file'),1)
   check('reconciled flush retains exactly the prepared object',len(disk()),1)
   check('only later-born updated version stays inline',[(r[3],r[4])for r in raw()],[(1,11)])
   tid=scalar(t.observer,'SELECT table_id FROM created_metadata.ducklake_table')
   fid=scalar(t.observer,'SELECT data_file_id FROM created_metadata.ducklake_data_file')
   marks=sql.SQL('SELECT file_id,row_id,begin_snapshot FROM created_metadata.{} ORDER BY 1,2').format(sql.Identifier('ducklake_inlined_delete_'+str(tid)))
   check('late end maps to exact prepared position at its original snapshot',query(t.observer,marks),[(fid,0,late)])
   check('concurrent update and flush preserve logical identity frontier',scalar(t.observer,'SELECT next_row_id FROM created_metadata.ducklake_table_stats'),3)
  elif a.mode=='old_writer':
   query(t.a,'BEGIN');check('old writer fixed original inline snapshot',t.rows(t.a),[(1,10),(2,20),(3,30)]);flush(t.b)
   try:query(t.a,'UPDATE biz.orders SET val=11 WHERE id=1')
   except psycopg2.Error as exc:check('old origin rejected during UPDATE',exc.pgcode,'40001')
   else:error(t.a,'COMMIT','40001')
   query(t.a,'ROLLBACK');check('old writer cannot resurrect moved inline version',t.rows(),[(1,10),(2,20),(3,30)])
  elif a.mode=='concurrent_insert':
   query(t.a,'BEGIN');flush();query(t.b,'INSERT INTO biz.orders VALUES(4,40)');query(t.a,'COMMIT')
   check('concurrent insert plus transferred rows visible exactly once',t.rows(),[(1,10),(2,20),(3,30),(4,40)])
   check('post-cutoff insert remains inline',[(r[3],r[4])for r in raw()],[(4,40)])
  elif a.mode=='rollback':
   query(t.a,'BEGIN');query(t.a,'INSERT INTO biz.heap_marker VALUES(1)');flush();query(t.a,'ROLLBACK')
   check('rollback restores exact complete lake catalog',t.protocol(),initial);check('rollback removes finished files',disk(),initial_disk);check('ordinary heap change rolls back with lake',query(t.a,'SELECT * FROM biz.heap_marker'),[])
   query(t.a,'BEGIN READ ONLY');error(t.a,"SELECT og_lake.flush('biz.orders'::regclass)",'25006');query(t.a,'ROLLBACK')
   query(t.a,'BEGIN ISOLATION LEVEL REPEATABLE READ');error(t.a,"SELECT og_lake.flush('biz.orders'::regclass)",'0A000');query(t.a,'ROLLBACK')
   query(t.a,'BEGIN');query(t.a,'UPDATE biz.orders SET val=11 WHERE id=1');error(t.a,"SELECT og_lake.flush('biz.orders'::regclass)",'0A000');query(t.a,'ROLLBACK')
   check('rejected modes never create files or publish data',disk(),[]);check('rejected modes preserve exact catalog',t.protocol(),initial)
  elif a.mode=='savepoints':
   query(t.a,'BEGIN');query(t.a,'SAVEPOINT adopted');flush();query(t.a,'RELEASE adopted');query(t.a,'ROLLBACK')
   check('released child flush rolls back with parent',disk(),[]);check('released child rollback keeps original catalog',t.protocol(),initial)
   query(t.a,'BEGIN');t.rows(t.a);query(t.a,'SAVEPOINT removed');flush();query(t.a,'ROLLBACK TO removed')
   check('ROLLBACK TO discards child candidate immediately',disk(),[]);query(t.a,'UPDATE biz.orders SET val=11 WHERE id=1');query(t.a,'COMMIT')
   check('DML allowed after rolled-back child maintenance',t.rows(),[(1,11),(2,20),(3,30)])
   query(t.a,'BEGIN');flush();query(t.a,'SAVEPOINT rejected');error(t.a,'UPDATE biz.orders SET val=12 WHERE id=1','0A000');query(t.a,'ROLLBACK TO rejected');query(t.a,'SAVEPOINT repeat');error(t.a,"SELECT og_lake.flush('biz.orders'::regclass)",'0A000');query(t.a,'ROLLBACK TO repeat');query(t.a,'COMMIT')
   check('parent flush survives rejected child DML and repeated flush',t.rows(),[(1,11),(2,20),(3,30)]);check('parent maintenance still published',raw(),[])
  elif a.mode=='repeat_file_dml':
   flush();query(t.a,'UPDATE biz.orders SET val=11 WHERE id=1');query(t.a,'UPDATE biz.orders SET val=12 WHERE id=1');query(t.a,'DELETE FROM biz.orders WHERE id=2')
   check('file and inline repeated updates before second flush',t.rows(),[(1,12),(3,30)])
   before_next=scalar(t.a,'SELECT next_row_id FROM created_metadata.ducklake_table_stats')
   tid=scalar(t.a,'SELECT table_id FROM created_metadata.ducklake_table');marked='ducklake_inlined_delete_'+str(tid)
   markers=query(t.a,sql.SQL('SELECT * FROM created_metadata.{} ORDER BY file_id,row_id,begin_snapshot').format(sql.Identifier(marked)))
   check('file-origin deleted positions recorded',len(markers),2)
   check('two inline historical versions remain before second flush',len(raw()),2)
   flush();check('second flush preserves one current version per business row',t.rows(),[(1,12),(3,30)]);check('second flush transfers both inline versions',raw(),[])
   check('second flush allocates no replacement row IDs',scalar(t.a,'SELECT next_row_id FROM created_metadata.ducklake_table_stats'),before_next)
   remaining=query(t.a,sql.SQL('SELECT * FROM created_metadata.{} ORDER BY file_id,row_id,begin_snapshot').format(sql.Identifier(marked)))
   if json.loads(scalar(t.a,'SELECT og_lake.host_status()')).get('bounded_file_delete_flush'):
    check('supported file-delete flush clears exact inline marks',remaining,[])
    check('merged original-file deletion is registered',scalar(t.a,'SELECT count(*) FROM created_metadata.ducklake_delete_file WHERE data_file_id=%s',(markers[0][0],)),1)
   else:check('existing file-delete carrier remains unchanged',remaining,markers)
   check('second data file is registered separately',scalar(t.a,'SELECT count(*) FROM created_metadata.ducklake_data_file'),2)
   before_head=t.head();before_disk=disk();flush();check('empty data flush creates no snapshot',t.head(),before_head);check('empty data flush creates no file',disk(),before_disk)
  elif a.mode=='publish_cancel':
   holder=db.connect();executor=ThreadPoolExecutor(max_workers=1);future=None
   try:
    query(holder,'BEGIN');query(holder,'LOCK TABLE created_metadata.ducklake_table_stats IN SHARE MODE')
    query(t.a,'BEGIN');pid=scalar(t.a,'SELECT pg_backend_pid()');query(t.a,'INSERT INTO biz.heap_marker VALUES(1)');flush()
    def commit():
     try:query(t.a,'COMMIT');return 'accepted'
     except psycopg2.Error as exc:return exc.pgcode
    future=executor.submit(commit);deadline=time.monotonic()+10;waits=[]
    while time.monotonic()<deadline:
     waits=query(t.observer,"SELECT pid,mode FROM pg_locks WHERE relation='created_metadata.ducklake_table_stats'::regclass AND pid=%s AND mode='RowExclusiveLock' AND NOT granted",(pid,))
     if waits or future.done():break
     time.sleep(.02)
    check('publisher blocked after snapshot and file registration',len(waits),1);check('uncommitted publication remains completely invisible',t.protocol(),initial)
    check('cancel exact owned COMMIT',scalar(t.observer,'SELECT pg_cancel_backend(%s)',(pid,)),True);check('real cancellation returned',future.result(timeout=10),'57014');query(t.a,'ROLLBACK')
    check('cancel rolls all file/catalog effects back',t.protocol(),initial);check('cancel deletes already finished candidate',disk(),[]);check('ordinary heap rollback atomic with lake publication',query(t.a,'SELECT * FROM biz.heap_marker'),[])
   finally:
    if future is not None and not future.done():scalar(t.observer,'SELECT pg_cancel_backend(%s)',(pid,))
    query(holder,'ROLLBACK');executor.shutdown(wait=True)
  # Exact offline interread and output inspection for every successfully committed file case.
  if scalar(t.observer,'SELECT count(*) FROM created_metadata.ducklake_data_file'):
   names=query(t.observer,"SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='created_metadata' AND c.relkind='r' ORDER BY c.relname")
   native=read_native_catalog(t.observer,'created_metadata',{'tables':[{'table_name':n}for n, in names]},out/'native-catalog.json')
   byname={x['table_name']:x for x in native['tables']}
   for standard in t.empty_export['tables']:check('unchanged standard catalog shape '+standard['table_name'],canonical_shape(byname[standard['table_name']]),canonical_shape(standard))
   lock=json.loads((D/'source-lock.json').read_text());receipt=json.loads((ROOT/'.reference/opengauss-native-lake/build/receipt.json').read_text());check('exact independent reference lock',receipt['source_lock_hash'],sha256(D/'source-lock.json'))
   ref=Reference(out,lock,receipt);transport=out/'transport.ducklake';transport_native(ref,transport,native)
   found=ref.run(ref.attach(transport,True)+'SELECT * FROM lake.main.orders ORDER BY id','independent-current',result=True)
   check('pinned DuckLake reads exact current native flush values',[tuple(row.values())for row in found],query(t.observer,'SELECT * FROM biz.orders ORDER BY id'))
   for sid,expected in history:
    found=ref.run(ref.attach(transport,True)+'SELECT id,val FROM lake.main.orders AT (VERSION => '+str(sid)+') ORDER BY id','independent-history-'+str(sid),result=True)
    check('independent old snapshot '+str(sid),[(x['id'],x['val'])for x in found],expected)
   if a.mode in ['history','typed']:
    f=byname['ducklake_data_file']['rows'][0];paths=list(root.rglob(f['path']));check('actual data file path unique',len(paths),1)
    physical=ref.run('SELECT file_row_number,* FROM read_parquet('+literal(str(paths[0]))+',file_row_number=true) ORDER BY file_row_number','physical-versions',result=True)
    columns=[c['name'] for c in result.get('columns',[])]
    names=[c['column_name']for c in byname[carrier]['columns']][3:]
    expected=Counter(tuple([x[0],x[1],*x[3:]])for x in before);observed=Counter(tuple([x['_ducklake_internal_row_id'],x['_ducklake_internal_snapshot_id'],*[x[n]for n in names]])for x in physical)
    check('every physical version preserves complete values ID and birth',sorted(map(repr,observed.items())),sorted(map(repr,expected.items())))
    check('file row count is exact physical versions',f['record_count'],len(before));check('file bytes exact',f['file_size_bytes'],paths[0].stat().st_size)
    with paths[0].open('rb') as stream:stream.seek(-8,2);tail=stream.read(8)
    check('serialized footer byte count exact',f['footer_size'],int.from_bytes(tail[:4],'little'))
    actual_stats=byname['ducklake_file_column_stats']['rows'];definitions=byname['ducklake_column']['rows'];ids={x['column_name']:x['column_id']for x in definitions}
    for col,name in enumerate(names):
     stats=[x for x in actual_stats if x['column_id']==ids[name]][0];n=sum(row[col+3]is None for row in before)
     check('exact file null count '+name,stats['null_count'],n);check('exact file nonnull count '+name,stats['value_count'],len(before)-n)
  check('all executed inputs unchanged',all(sha256(p)==h for p,h in result['input_hashes'].items()));check_build(a.build_receipt);result['status']='pass'
 except BaseException as exc:result.update(reason=str(exc),traceback=traceback.format_exc())
 finally:
  if db is not None:
   try:db.cleanup();result['database_cleanup']=db.cleanup_result
   except BaseException as exc:result.update(status='fail',cleanup_error=str(exc))
  if roles is not None:
   try:
    if db is None or not db.cleanup_result or db.cleanup_result['returncode']!=0:raise RuntimeError('Database cleanup unproven; owned roles retained')
    result['role_cleanup']=cleanup_roles(control,roles.path,a.run_id);result=roles.redact(result)
   except BaseException as exc:result.update(status='fail',role_cleanup_error=str(exc))
  if control is not None:control.close()
  write_json(out/'result.json',result)
 print(json.dumps({'status':result['status'],'mode':a.mode,'reason':result.get('reason'),'checks':len(result['assertions']),'result':str(out/'result.json')}));return 0 if result['status']=='pass'else 1
if __name__=='__main__':raise SystemExit(main())
