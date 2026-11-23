#!/usr/bin/env python3
"""Repeated real failed flush/savepoint retries must reclaim transaction memory."""
import argparse,json,os,shutil,subprocess,sys,traceback
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

def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--run-id',required=True);p.add_argument('--build-receipt',type=Path,required=True);a=p.parse_args()
 out=R/'公共文档/openGauss原生湖存储/验证'/a.run_id;out.mkdir(exist_ok=False)
 result={'status':'fail','scope':'Actual bounded flush failed-preparation/savepoint memory reclamation','assertions':[]};db=roles=control=None
 inputs=[Path(__file__),D/'native_test_context.py',D/'tests/native_table_create.py',D/'tests/permission_api.py',D/'inline_fixture.py'];result['input_hashes']={str(x):sha256(x)for x in inputs};archive=out/'executed-inputs';archive.mkdir()
 for x in inputs:shutil.copy2(x,archive/x.name)
 def check(name,got,want=True):
  ok=got==want;result['assertions'].append({'name':name,'observed':got,'expected':want,'status':'pass'if ok else'fail'})
  if not ok:raise AssertionError(name)
 try:
  import psycopg2
  built=check_build(a.build_receipt);result.update(source_head=built['source_head'],binary_sha256=built['binary_sha256'],build=str(a.build_receipt),build_sha256=sha256(a.build_receipt))
  with (out/'fixture.log').open('w')as log:subprocess.run([sys.executable,str(D/'tests/catalog_fixture_bundle.py'),'--run-id',a.run_id,'--output',str(out/'fixture/manifest.json')],stdout=log,stderr=subprocess.STDOUT,check=True,timeout=300)
  control=psycopg2.connect(host=str(R/'.runtime/opengauss-native-lake/debug/socket'),port=55560,dbname='postgres',user=os.getenv('USER','qiumc'));control.autocommit=True
  db=OwnedDatabase(a.run_id,out).create();roles=OwnedRoles(control,a.run_id,out)
  args=SimpleNamespace(mode='net_dml',sql_version='0.11',manifest=out/'fixture/manifest.json',output=out/'result.json',run_id=a.run_id)
  t=Case(db,args,result,roles)
  query(t.a,"CREATE FOREIGN TABLE biz.orders(id integer,val integer,note text) SERVER created_lake OPTIONS(binding_mode 'create',lake_schema 'main')")
  query(t.a,"INSERT INTO biz.orders SELECT 1,10,repeat('x',1100000)")
  root=Path(t.empty_manifest['data_path']);head=t.head();query(t.a,'BEGIN');check('ancestor read context established',t.rows(t.a),[(1,10)])
  initial=json.loads(scalar(t.a,'SELECT og_lake.host_status()'))['transaction_memory_bytes'];samples=[]
  for i in range(4):
   query(t.a,'SAVEPOINT retry')
   try:query(t.a,"SELECT og_lake.flush('biz.orders'::regclass)")
   except psycopg2.Error as exc:check('oversized row refused in attempt '+str(i),exc.pgcode,'54000')
   else:raise AssertionError('Expected genuine1MiB builder budget rejection')
   query(t.a,'ROLLBACK TO retry');query(t.a,'RELEASE retry')
   status=json.loads(scalar(t.a,'SELECT og_lake.host_status()'));samples.append(status['transaction_memory_bytes'])
   check('failed candidate physically removed '+str(i),list(root.rglob('*.parquet')),[])
   check('business write journal remains empty '+str(i),status['staged_bytes'],0)
  result['memory']={'before':initial,'after_each_rollback':samples,'allowed_allocator_slack':65536}
  check('repeated failed preparations reclaim their contexts',max(samples)<=initial+65536)
  query(t.a,'ROLLBACK');check('failed attempts publish no snapshot',t.head(),head);check('source value remains intact',query(t.a,'SELECT id,val,length(note) FROM biz.orders'),[(1,10,1100000)])
  check('all test inputs fixed',all(sha256(p)==h for p,h in result['input_hashes'].items()));check_build(a.build_receipt);result['status']='pass'
 except BaseException as exc:result.update(reason=str(exc),traceback=traceback.format_exc())
 finally:
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
 print(json.dumps({'status':result['status'],'reason':result.get('reason'),'memory':result.get('memory'),'result':str(out/'result.json')}));return 0 if result['status']=='pass'else 1
if __name__=='__main__':raise SystemExit(main())
