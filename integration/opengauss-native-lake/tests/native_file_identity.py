#!/usr/bin/env python3
"""Observe actual native file-reader identities through an isolated test DSO."""
import argparse,hashlib,json,os,shutil,sys,traceback
from pathlib import Path
ROOT=Path('/home/qiumc/data/code/cpp');DRIVER=ROOT/'integration/opengauss-native-lake';sys.path[:0]=[str(DRIVER),str(DRIVER/'tests')]
from native_file_read import Case
from native_test_context import OwnedDatabase
from permission_api import OwnedRoles,cleanup_roles
from inline_fixture import query,scalar
from lake_harness import sha256,write_json,validate_run_id

def main():
 p=argparse.ArgumentParser();p.add_argument('--manifest',type=Path,required=True);p.add_argument('--mode',choices=['versions_explicit','versions_batch'],required=True)
 p.add_argument('--probe',type=Path,required=True);p.add_argument('--run-id',required=True);p.add_argument('--output',type=Path,required=True);args=p.parse_args();args.output=args.output.resolve();args.output.parent.mkdir(exist_ok=False);args.manifest=args.manifest.resolve();validate_run_id(args.run_id)
 result={'status':'fail','scope':'actual native FileReader row identity/birth/file-position observation; no new product SQL API','run_id':args.run_id,'assertions':[],'artifacts':[]}
 paths=[Path(__file__),DRIVER/'tests/native_file_read.py',DRIVER/'tests/permission_api.py',DRIVER/'native_test_context.py',args.probe]
 hashes={str(f.resolve()):sha256(f)for f in paths};archive=args.output.parent/'inputs';archive.mkdir()
 for n,f in enumerate(paths):shutil.copy2(f,archive/(str(n)+'-'+f.name))
 roles=db=control=None
 try:
  import psycopg2
  receipt=json.loads(args.probe.read_text());library=Path(receipt['library'])
  if receipt['status']!='built_unloaded' or sha256(library)!=receipt['library_sha256']:raise RuntimeError('test probe build is unqualified or changed')
  if sha256(ROOT/'.install/opengauss-native-lake-debug/bin/gaussdb')!=receipt['native_binary_sha256']:raise RuntimeError('test probe native ABI changed')
  for name,digest in receipt['headers'].items():
   if sha256(name)!=digest:raise RuntimeError('test probe compiled header changed: '+name)
  result['probe_library_sha256']=receipt['library_sha256'];result['native_binary_sha256']=receipt['native_binary_sha256']
  control=psycopg2.connect(host=str(ROOT/'.runtime/opengauss-native-lake/debug/socket'),port=55560,dbname='postgres',user=os.getenv('USER','qiumc'),connect_timeout=10);control.autocommit=True
  roles=OwnedRoles(control,args.run_id,args.output.parent)
  with OwnedDatabase(args.run_id,args.output.parent)as db:
   t=Case(db,args,result,roles);t.bind()
   query(t.a,'CREATE SCHEMA identity_probe');query(t.a,'REVOKE ALL ON SCHEMA identity_probe FROM PUBLIC')
   query(t.a,t.sql.SQL("CREATE FUNCTION identity_probe.file_rows(regclass) RETURNS text AS {},'lake_test_file_identities' LANGUAGE C STRICT STABLE NOT FENCED").format(t.sql.Literal(receipt.get('sql_module_path',str(library)))))
   query(t.a,'REVOKE ALL ON FUNCTION identity_probe.file_rows(regclass) FROM PUBLIC')
   raw=json.loads(scalar(t.a,"SELECT identity_probe.file_rows('biz.orders'::regclass)"))
   file=t.manifest['data_files'][0];identities={r['id']:r for r in t.manifest['row_identity_map']};physical=t.manifest['physical_positions'];wanted=[]
   position_map={}
   for row in physical:
    key=(row['id'],row['_ducklake_internal_row_id'],row['_ducklake_internal_snapshot_id'])
    position_map.setdefault(key,[]).append(row['file_row_number'])
   for key,observed in sorted(identities.items()):
    positions=position_map.get((key,observed['logical_rowid'],observed['begin_snapshot']),[])
    if len(positions)!=1:raise RuntimeError('genuine visible row does not map uniquely to its original physical version')
    wanted.append([key,2,observed['logical_rowid'],observed['begin_snapshot'],file['data_file_id'],positions[0]])
   t.check('actual native logical IDs, births, file IDs and original positions equal genuine reference',sorted(raw),wanted)
   t.check('identity reader releases actual SDK handles',len(t.descriptors()),0)
   (args.output.parent/'native-identities.json').write_text(json.dumps(raw)+'\n');(args.output.parent/'reference-identities.json').write_text(json.dumps(wanted)+'\n')
   t.check('native business rows still equal independent reference',t.rows(),t.expected);t.finish()
   t.check('executed inputs remain unchanged',{str(f.resolve()):sha256(f)for f in paths},hashes)
  result['status']='pass'
 except BaseException as exc:result.update(reason=str(exc),traceback=traceback.format_exc())
 finally:
  if db is not None:result['database_cleanup']=db.cleanup_result;result['artifacts'].extend(db.artifacts())
  if roles is not None:
   try:
    if db is not None and db.cleanup_result and db.cleanup_result['returncode']==0:result['role_cleanup']=cleanup_roles(control,roles.path,args.run_id)
    elif roles.items:raise RuntimeError('owned database cleanup unproven')
   except BaseException as exc:result.update(status='fail',cleanup_error=str(exc))
   result=roles.redact(result)
  if control is not None:control.close()
  result['artifacts'].extend({'path':str(f),'sha256':sha256(f)}for f in args.output.parent.rglob('*')if f.is_file()and f!=args.output and f.name!='database-cleanup.json')
  write_json(args.output,result);print(json.dumps({'status':result['status'],'reason':result.get('reason'),'output':str(args.output)}))
 return 0 if result['status']=='pass'else 1
if __name__=='__main__':raise SystemExit(main())
