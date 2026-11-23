#!/usr/bin/env python3
"""Cancel actual COMMIT after inline DELETE, prove metadata/data/file rollback."""
import argparse
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

def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--run-id',required=True);p.add_argument('--build-receipt',type=Path,required=True);a=p.parse_args()
 out=ROOT/'公共文档/openGauss原生湖存储/验证'/a.run_id;out.mkdir(exist_ok=False)
 result={'status':'fail','scope':'Real SQL0.11 COMMIT cancellation after file registration, stats update and physical inline DELETE','assertions':[],'limitations':['Controlled local failure only; no crash, remote or RAC recovery qualification.']}
 db=roles=control=holder=executor=future=t=None;pid=None
 inputs=[Path(__file__),D/'native_test_context.py',D/'tests/native_table_create.py',D/'tests/permission_api.py',D/'inline_fixture.py']
 result['inputs']={str(x):sha256(x)for x in inputs};archive=out/'executed-inputs';archive.mkdir()
 for x in inputs:shutil.copy2(x,archive/x.name)
 def check(name,got,want=True):
  ok=got==want;result['assertions'].append({'name':name,'observed':got,'expected':want,'status':'pass'if ok else'fail'})
  if not ok:raise AssertionError(name)
 try:
  import psycopg2
  from psycopg2 import sql
  build=check_build(a.build_receipt);result.update(source_head=build['source_head'],binary_sha256=build['binary_sha256'],build=str(a.build_receipt),build_sha256=sha256(a.build_receipt))
  with (out/'fixture.log').open('w')as log:subprocess.run([sys.executable,str(D/'tests/catalog_fixture_bundle.py'),'--run-id',a.run_id,'--output',str(out/'fixture/manifest.json')],stdout=log,stderr=subprocess.STDOUT,check=True,timeout=300)
  control=psycopg2.connect(host=str(ROOT/'.runtime/opengauss-native-lake/debug/socket'),port=55560,dbname='postgres',user=os.getenv('USER','qiumc'));control.autocommit=True
  db=OwnedDatabase(a.run_id,out).create();roles=OwnedRoles(control,a.run_id,out)
  args=SimpleNamespace(mode='net_dml',sql_version='0.11',manifest=out/'fixture/manifest.json',output=out/'result.json',run_id=a.run_id)
  t=Case(db,args,result,roles);t.create();query(t.a,'INSERT INTO biz.orders VALUES(1,10),(2,20),(3,30)');query(t.a,'UPDATE biz.orders SET val=11 WHERE id=1')
  original=t.protocol();carrier=scalar(t.a,'SELECT table_name FROM created_metadata.ducklake_inlined_data_tables');root=Path(t.empty_manifest['data_path'])
  source=sql.SQL('created_metadata.{}').format(sql.Identifier(carrier))
  before=query(t.observer,sql.SQL('SELECT * FROM {} ORDER BY _ducklake_row_id,_ducklake_begin_snapshot').format(source));check('four original historical versions',len(before),4)
  holder=db.connect();query(holder,'BEGIN');query(holder,'LOCK TABLE created_metadata.ducklake_snapshot_changes IN SHARE MODE')
  query(t.a,'BEGIN');pid=scalar(t.a,'SELECT pg_backend_pid()');query(t.a,'INSERT INTO biz.heap_marker VALUES(1)');query(t.a,"SELECT og_lake.flush('biz.orders'::regclass)")
  candidates=sorted(str(x)for x in root.rglob('*.parquet'));check('data plus historical delete files durably prepared',len(candidates),2)
  def commit():
   try:query(t.a,'COMMIT');return 'accepted'
   except psycopg2.Error as exc:return exc.pgcode
  executor=ThreadPoolExecutor(max_workers=1);future=executor.submit(commit);deadline=time.monotonic()+10;waits=[]
  while time.monotonic()<deadline:
   waits=query(t.observer,"SELECT pid,relation,mode FROM pg_locks WHERE relation='created_metadata.ducklake_snapshot_changes'::regclass AND pid=%s AND mode='RowExclusiveLock' AND NOT granted",(pid,))
   if waits or future.done():break
   time.sleep(.02)
  check('publisher waits at changeset INSERT after actual inline DELETE',len(waits),1)
  locks=query(t.observer,"SELECT mode FROM pg_locks WHERE relation=%s::regclass AND pid=%s AND mode='RowExclusiveLock' AND granted",('created_metadata.'+carrier,pid))
  check('publisher already owns physical inline DELETE relation lock',len(locks),1)
  ended=scalar(t.observer,sql.SQL("SELECT count(*) FROM {} WHERE xmax::text<>'0'").format(source))
  check('all four visible heap tuples carry uncommitted delete XIDs',ended,4)
  result['barrier']={'waits':waits,'inline_delete_locks':locks,'visible_tuples_with_delete_xids':ended,'publisher_pid':pid,'source_sha256':sha256(ROOT/'.worktrees/opengauss-native-lake/contrib/og_lake/lake_publish.cpp')}
  check('outside SQL reader still sees current original rows',t.rows(t.observer),[(1,11),(2,20),(3,30)]);check('all staged catalog changes remain invisible',t.protocol(),original)
  check('cancel only owned publisher',scalar(t.observer,'SELECT pg_cancel_backend(%s)',(pid,)),True);check('COMMIT reports real cancellation',future.result(timeout=10),'57014');query(t.a,'ROLLBACK')
  check('original physical version values restored by rollback',query(t.observer,sql.SQL('SELECT * FROM {} ORDER BY _ducklake_row_id,_ducklake_begin_snapshot').format(source)),before)
  check('snapshot file registrations and stats all rolled back',t.protocol(),original);check('finished data and delete candidates removed',sorted(str(x)for x in root.rglob('*.parquet')),[])
  check('ordinary heap write rolls back in same host transaction',query(t.a,'SELECT * FROM biz.heap_marker'),[])
  check('aborted publisher connection reusable',scalar(t.a,'SELECT 42'),42)
  check('all executed inputs remain fixed',all(sha256(x)==h for x,h in result['inputs'].items()));check_build(a.build_receipt);result['status']='pass'
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
    if db is None or not db.cleanup_result or db.cleanup_result['returncode']!=0:raise RuntimeError('Owned DB cleanup unproven; role retained')
    result['role_cleanup']=cleanup_roles(control,roles.path,a.run_id);result=roles.redact(result)
   except BaseException as exc:result.update(status='fail',role_cleanup_error=str(exc))
  if control is not None:control.close()
  write_json(out/'result.json',result)
 print(json.dumps({'status':result['status'],'reason':result.get('reason'),'checks':len(result['assertions']),'result':str(out/'result.json')}));return 0 if result['status']=='pass'else 1
if __name__=='__main__':raise SystemExit(main())
