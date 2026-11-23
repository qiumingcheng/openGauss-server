#!/usr/bin/env python3
"""Prepared PGL0 one-int4 identity file partition Native qualification.

Default prepare imports no Native helper and makes no connection or fixture.
Execution needs root's explicitly versioned, reviewed source contract and a new
exact official build/SDK receipt. Run one mode at a time under an external owned
process-group deadline1200s. This is not native parent/leaf partitioning, direct
partition writes, pruning, index/SMP/Vec, performance or commercial admission.
"""
import argparse
from collections import Counter
from copy import deepcopy
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import traceback
from types import SimpleNamespace

ROOT=Path(os.getenv('CPP_WORKSPACE','/home/qiumc/data/code/cpp')).resolve()
D=ROOT/'integration/opengauss-native-lake'
MODES=('definition','groups','prefix','rollback','direct-refusal','guards','fair-round','upgrade-round')
PGL_PATCH='d3d8805cc60c61d6cdc3c72ddef1e11561ef835dcde87f4e43f71ae31335e9e8'


def digest(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()


def canonical(value):
 if isinstance(value,Counter):return sorted((repr(k),v)for k,v in value.items())
 if isinstance(value,dict):return {str(k):canonical(v)for k,v in value.items()}
 if isinstance(value,(set,frozenset)):return sorted((canonical(v)for v in value),key=repr)
 if isinstance(value,(list,tuple)):return [canonical(v)for v in value]
 if isinstance(value,Path):return str(value)
 return value



def expected_partition_events(first_snapshot,rowids,values,changes):
 # Values/operations are the explicit original fixture DML. Only stable IDs
 # and commit snapshots are observed; no output file/position/delete metadata
 # is read to invent the business change stream.
 if set(rowids)!={i for i,k,v in values} or len(set(rowids.values()))!=len(rowids):
  raise AssertionError('independent event identities do not exactly match original INSERTs')
 if any(type(R)is not int or R<0 for R in rowids.values()):
  raise AssertionError('original INSERT identity is invalid')
 events=[dict(snapshot_id=first_snapshot,logical_rowid=rowids[i],change_type='insert',id=i,p=k,val=v)for i,k,v in values]
 for snapshot,kind,i,k,v in changes:
  if snapshot<=first_snapshot or i not in rowids or kind not in ('update_preimage','update_postimage','delete'):
   raise AssertionError('invalid independent original DML event')
  events.append(dict(snapshot_id=snapshot,logical_rowid=rowids[i],change_type=kind,id=i,p=k,val=v))
 return sorted(events,key=lambda x:(x['snapshot_id'],x['logical_rowid'],x['change_type']))

def main():
 p=argparse.ArgumentParser(description=__doc__)
 p.add_argument('--mode',choices=MODES,required=True);p.add_argument('--execute',action='store_true')
 p.add_argument('--run-id');p.add_argument('--sql-version');p.add_argument('--build-receipt',type=Path)
 p.add_argument('--sdk-qualification',type=Path);p.add_argument('--sdk-index-sha256');p.add_argument('--source-contract',type=Path)
 a=p.parse_args()
 if not a.execute:
  print(json.dumps(dict(status='prepared_not_executed',mode=a.mode,Native=0,SDK=0,fixtures=0,
   candidate_patch=PGL_PATCH,version='Root selects explicit new install/upgrade capability; retainedSQL19 is closed.',
   requirements=['Reviewed final source/SQL/helpers and exact official Native+SDK receipt','Shared fixture reserved and serial mode; external whole-processgroup deadline1200s',
   'definition=SET/RESET/GET, owner and no-carrier/savepoint boundaries','groups=two-key sameR/multibirth plus captured and late ends, independent actual Parquet and DuckLake full history',
   'prefix=third distinct NULL/int4 key prefix progress; rollback=owned group files and metadata atomicity','direct-refusal=existing1000 limit accepts1000, rejects1001 with exact rollback, then original zero-inline direct refusal','guards=parameter/owner/read-only and index/rewrite/SMP/Vec refusal; groups/prefix also run genuine DuckLake CDC',
   'fair-round=actual lowRchurn + captured>endH/late>S + fixedHdrain + heap/fullhistoryCDC; upgrade-round=explicit21->22/savepoint/owner/drop then fair-round']),indent=2))
  return 0
 if not all([a.run_id,a.sql_version,a.build_receipt,a.sdk_qualification,a.sdk_index_sha256,a.source_contract]):
  p.error('execute needs run-id, explicit sql-version, exact build/sdk receipts and reviewed source-contract')
 if a.sql_version!='0.22' or not re.fullmatch('[0-9a-f]{64}',a.sdk_index_sha256):
  p.error('PGL capability is not open in retainedSQL19/20/21; this reviewed first slice requires explicit SQL22 and exact SDK index SHA')
 sys.path[:0]=[str(D),str(D/'tests')]
 from file_dml_acceptance import check_build
 from inline_fixture import query,scalar
 from lake_harness import sha256,validate_run_id,write_json
 from native_auto_flush import connect_control
 from native_runtime import PROFILE,SOURCE,INSTALL
 from native_test_context import OwnedDatabase
 from native_table_create import Case
 from permission_api import OwnedRoles,cleanup_roles
 from empty_catalog_oracle import read_native_catalog,canonical_shape,transport_native
 from reverse_reference import Reference,literal
 from psycopg2 import sql
 validate_run_id(a.run_id)
 if len(a.run_id)>90:p.error('run-id maximum90 retains child fixture identity budget')
 if PROFILE!='debug':p.error('First PGL qualification requires owned main Debug profile')
 out=ROOT/'公共文档/openGauss原生湖存储/验证'/a.run_id;out.mkdir(exist_ok=False)
 r=dict(status='fail',mode=a.mode,scope=__doc__,assertions=[],executed=False)
 db=roles=control=t=reservation=None;ref=None;history=[];event_first=None;event_ids={};event_values=[];event_changes=[]
 def check(name,got,want=True):
  got,want=canonical(got),canonical(want);ok=got==want;r['assertions'].append(deepcopy(dict(name=name,observed=got,expected=want,status='pass'if ok else'fail')))
  write_json(out/'result.json',r)
  if not ok:raise AssertionError(name)
 def session(c):
  for s in ['SET session_timeout=0','SET statement_timeout=60000','SET query_dop=1','SET enable_lake_smp=off',
            'SET enable_vector_engine=off','SET enable_default_ustore_table=off']:
   query(c,s)
 def reject(c,statement,code='0A000',args=None):
  try:query(c,statement,args)
  except Exception as e:check('SQLSTATE '+statement[:90],getattr(e,'pgcode',None),code)
  else:raise AssertionError('unexpected accepted '+statement)
 def setpart(c=None,key='p'):query(c or t.a,"SELECT og_lake.set_partition('biz.orders'::regclass,%s::text[])",([key],))
 def reset(c=None):query(c or t.a,"SELECT og_lake.reset_partition('biz.orders'::regclass)")
 def spec(c=None):return query(c or t.observer,"SELECT * FROM og_lake.get_partition('biz.orders'::regclass)")
 def carrier():
  z=query(t.observer,'SELECT table_name,schema_version FROM created_metadata.ducklake_inlined_data_tables WHERE table_id=%s',(tid,))
  check('one actual inline registration',len(z),1);return z[0][0]
 def raw(c=None):
  return query(c or t.observer,sql.SQL('SELECT _ducklake_row_id,_ducklake_begin_snapshot,_ducklake_end_snapshot,id,p,val '
    'FROM created_metadata.{} ORDER BY _ducklake_row_id,_ducklake_begin_snapshot').format(sql.Identifier(carrier())))
 def rows(c=None):return query(c or t.observer,'SELECT id,p,val FROM biz.orders ORDER BY id')
 def head():return t.head()[0]
 def remember():
  z=raw();seen={};s=head()
  for R,B,E,i,k,v in z:
   if B<=s and(E is None or E>s):
    if R in seen:raise AssertionError('multiple actual current births')
    seen[R]=dict(logical_rowid=R,begin_snapshot=B,id=i,p=k,val=v)
  check('committed raw/current business parity',[(x['id'],x['p'],x['val'])for x in sorted(seen.values(),key=lambda x:x['id'])],rows())
  history.append((s,[seen[R]for R in sorted(seen)]))
 def disk():return {str(x):sha256(x)for x in sorted(root.rglob('*.parquet'))}
 def state():return dict(catalog=t.protocol(),heap=query(t.observer,'SELECT * FROM biz.heap_marker ORDER BY id'),files=disk())
 def flush(c=None):query(c or t.a,"SELECT og_lake.flush('biz.orders'::regclass)")
 def files():return query(t.observer,'SELECT data_file_id,path,record_count,file_size_bytes,partition_id FROM '
    'created_metadata.ducklake_data_file WHERE table_id=%s AND end_snapshot IS NULL ORDER BY data_file_id',(tid,))
 def data_path(name):
  found=list(root.rglob(name));check('exact owned output path/'+name,len(found),1)
  q=found[0].resolve();check('actual output remains under owned fixture data root',q.is_relative_to(root),True);return q
 def inspect(label):
  physical=[];index={};partitions=query(t.observer,'SELECT data_file_id,partition_key_index,partition_value FROM '
     'created_metadata.ducklake_file_partition_value WHERE table_id=%s ORDER BY data_file_id',(tid,))
  for fid,name,n,sz,pid in files():
   path=data_path(name);check(label+' real bytes/'+str(fid),path.stat().st_size,sz)
   z=ref.run('SELECT file_row_number AS position,_ducklake_internal_row_id AS logical_rowid,'
     '_ducklake_internal_snapshot_id AS begin_snapshot,id,p,val FROM read_parquet('+literal(str(path))+
     ',file_row_number=true) ORDER BY file_row_number',label+'-data-'+str(fid),result=True)
   check(label+' footer physical population/'+str(fid),len(z),n)
   check(label+' dense file-local positions/'+str(fid),[x['position']for x in z],list(range(n)))
   check(label+' file uses original standard identity spec/'+str(fid),pid,partition_id)
   keys={x['p']for x in z};check(label+' one nullable identity key per file/'+str(fid),len(keys),1)
   value=next(iter(keys));check(label+' actual partition value/'+str(fid),[x for x in partitions if x[0]==fid],[(fid,0,None if value is None else str(value))])
   for x in z:
    key=(x['logical_rowid'],x['begin_snapshot']);check(label+' no duplicate logical-version coverage/'+str(key),key not in index)
    index[key]=(fid,x['position']);physical.append(dict(x,file_id=fid))
   defs=query(t.observer,"SELECT column_id FROM created_metadata.ducklake_column WHERE table_id=%s AND column_name='p' AND end_snapshot IS NULL",(tid,))
   check(label+' stable partition column definition',len(defs),1)
   st=query(t.observer,'SELECT value_count,null_count,min_value,max_value,min_is_exact,max_is_exact FROM '
      'created_metadata.ducklake_file_column_stats WHERE data_file_id=%s AND column_id=%s',(fid,defs[0][0]))
   expected=(0,n,None,None,None,None)if value is None else(n,0,str(value),str(value),True,True)
   check(label+' exact identity key file stats/'+str(fid),st,[expected])
  return physical,index
 def reference(label):
  names=query(t.observer,"SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE "
    "n.nspname='created_metadata' AND c.relkind='r' ORDER BY c.relname")
  actual=read_native_catalog(t.observer,t.schema,{'tables':[{'table_name':n}for n, in names]},out/(label+'-native.json'))
  by={x['table_name']:x for x in actual['tables']}
  for original in t.empty_export['tables']:check(label+' standard shape/'+original['table_name'],canonical_shape(by[original['table_name']]),canonical_shape(original))
  catalog=out/(label+'.ducklake');transport_native(ref,catalog,actual);before=sha256(catalog)
  check(label+' genuine DuckLake current',[(x['id'],x['p'],x['val'])for x in sorted(ref.logical(catalog,'lake.main.orders',label+'-current'),key=lambda x:x['id'])],rows())
  for s,expected in history:
   z=ref.run(ref.attach(catalog,True)+'SELECT rowid AS logical_rowid,snapshot_id AS begin_snapshot,id,p,val '
      'FROM lake.main.orders AT(VERSION=>'+str(s)+') ORDER BY rowid',label+'-history-'+str(s),result=True)
   check(label+' original full values/R/birth atS'+str(s),z,expected)
  if event_first is not None:
   expected=expected_partition_events(event_first,event_ids,event_values,event_changes)
   last=head()
   events=ref.run(ref.attach(catalog,True)+'SELECT snapshot_id,rowid AS logical_rowid,change_type,id,p,val '
     "FROM ducklake_table_changes('lake','main','orders',"+str(event_first)+','+str(last)+
     ') ORDER BY snapshot_id,logical_rowid,change_type',label+'-complete-cdc',result=True)
   check(label+' genuine DuckLake CDC equals original INSERT/UPDATE/DELETE event log',events,expected)
   maintenance=ref.run(ref.attach(catalog,True)+'SELECT snapshot_id,rowid AS logical_rowid,change_type,id,p,val '
     "FROM ducklake_table_changes('lake','main','orders',"+str(last)+','+str(last)+
     ') ORDER BY snapshot_id,logical_rowid,change_type',label+'-last-snapshot-cdc',result=True)
   check(label+' flush snapshot produces no spurious business CDC',maintenance,[x for x in expected if x['snapshot_id']==last])
   r['independent_expected_cdc']=deepcopy(expected)
  check(label+' readonly reference preserves actual metadata copy',sha256(catalog),before)
 try:
  build=check_build(a.build_receipt);contract=json.loads(a.source_contract.read_text())
  check('reviewed contract purpose',contract.get('purpose'),'pgl0_identity_partition_qualification')
  check('reviewed contract schema',contract.get('schema_version'),1)
  check('reviewed contract exact PGL candidate',contract.get('pgl_base_patch_sha256'),PGL_PATCH)
  check('source contract explicitly ready for qualification',contract.get('ready_for_native_qualification'),True)
  check('reviewed final blockers empty',contract.get('known_blockers'),[])
  check('root chosen SQL capability matches execute argument',contract.get('partition_sql_version'),a.sql_version)
  if not contract.get('review_evidence')or not contract.get('native_inputs')or not contract.get('shared_helper_inputs'):
   raise AssertionError('missing reviewed source/helper/protocol evidence')
  required={'contrib/og_lake/'+n for n in ['Makefile','lake_catalog.cpp','lake_file_catalog.cpp','lake_file_sdk.cpp','lake_file_sdk.h',
   'lake_flush.cpp','lake_flush.h','lake_flush_file_deletes.cpp','lake_index.cpp','lake_inline_ann.cpp','lake_inline_index.cpp',
   'lake_inline_private.h','lake_inline_scan.cpp','lake_partition.cpp','lake_partition.h','lake_partition_ddl.cpp','lake_publish.cpp',
   'lake_spi.cpp','lake_stream.cpp','lake_txn.cpp','lake_txn_private.h','og_lake.cpp','og_lake.control','lake_version.h']}
  required.add('contrib/og_lake/og_lake--'+a.sql_version+'.sql')
  if a.mode in('fair-round','upgrade-round'):
   check('reviewed source explicitly includes fixed round contract',contract.get('flush_fair_round_contract'),'canonical-table-binding-fixed-birth-v1')
   required.update({'contrib/og_lake/lake_acl.cpp','contrib/og_lake/og_lake--0.21--0.22.sql'})
  if not required<=contract['native_inputs'].keys():raise AssertionError('reviewed contract omits PGL source/install gate inputs')
  if not {'native_test_context.py','tests/native_table_create.py'}<=contract['shared_helper_inputs'].keys():raise AssertionError('reviewed future helper gate identities missing')
  for n,h in contract['native_inputs'].items():
   if Path(n).is_absolute()or '..'in Path(n).parts:raise AssertionError('unsafe source path')
   check('official Native source input/'+n,build['native_source_inputs_after'].get(n),h)
  for n,h in contract['shared_helper_inputs'].items():check('exact approved helper/'+n,sha256(D/n),h)
  for n,h in contract['review_evidence'].items():check('exact review evidence/'+n,sha256(n),h)
  r.update(build=str(a.build_receipt),build_sha256=sha256(a.build_receipt),source_head=build['source_head'],binary_sha256=build['binary_sha256'],executed=True)
  control=connect_control();session(control)
  check('fixture maintenance disabled',scalar(control,'SHOW enable_lake_maintenance'),'off')
  check('dedicated nonpool fixture backend',scalar(control,'SHOW enable_thread_pool'),'off')
  from pq_sql020_build_inputs import sdk_index
  qual=sdk_index(a.sdk_qualification,a.sdk_index_sha256,ROOT)
  sdk=INSTALL/'lib/postgresql/og_lake_sdk';owner=json.loads((sdk/'owner.json').read_text())
  check('exact SDK index selected',sha256(a.sdk_qualification),a.sdk_index_sha256)
  check('installed SDK qualification kind',owner['qualification_kind'],'verified_independent_sdk_sql020_int16_only')
  check('installed SDK belongs exact receipt',owner['qualification_sha256'],a.sdk_index_sha256)
  check('SDK owner installation phase',owner['phase'],'prepared-for-atomic-install')
  native_public=owner.get('native_public_headers',{})
  wanted_public=[SOURCE/'contrib/og_lake'/n for n in ['og_lake.control','sdk/lake_parquet_sdk.h','vendor/arrow20/abi.h']]
  check('SDK marker binds current full Native public identity',native_public,{str(p):sha256(p)for p in wanted_public})
  check('Native SDK header equals independent qualified header',sha256(wanted_public[1]),qual['header_sha256'])
  check('installed SDK owner exact target',str(Path(owner['target']).resolve()),str(sdk.resolve()))
  check('SDK installed owner project',owner['project'],'opengauss-native-lake-local-provider')
  check('SDK prefix has no symlinks',not any(x.is_symlink()for x in sdk.rglob('*')))
  sdkfiles={str(x.relative_to(sdk)):sha256(x)for x in sdk.rglob('*')if x.is_file()and x.name!='owner.json'}
  check('whole SDK prefix hashes match owner',sdkfiles,owner['files']);check('SDK provider binary exact',sdkfiles['libog_lake_sdk_arrow.so'],qual['library_sha256'])
  lock=json.loads((D/'source-lock.json').read_text());receipt_path=ROOT/'.reference/opengauss-native-lake/build/receipt.json';receipt=json.loads(receipt_path.read_text())
  (out/'reference').mkdir();ref=Reference(out/'reference',lock,receipt)
  check('exact pinned reference lock',receipt['source_lock_hash'],sha256(D/'source-lock.json'))
  check('exact pinned reference driver',sha256(D/'reference.sh'),lock['reference_build']['script_sha256'])
  check('exact pinned reference binary',sha256(ref.binary),receipt['binary_sha256'])
  inputs=[Path(__file__),a.build_receipt,a.source_contract,a.sdk_qualification,receipt_path,sdk/'owner.json',D/'source-lock.json',D/'reference.sh',D/'inline_fixture.py',D/'native_test_context.py',D/'native_runtime.py',D/'lake_harness.py',D/'fixture.py']
  inputs +=[D/'tests'/n for n in ['native_table_create.py','file_dml_acceptance.py','native_auto_flush.py','permission_api.py','inline_cleanup.py','catalog_fixture_bundle.py','empty_manifest.py','empty_reference_fixture.py','empty_catalog_oracle.py','create_reference_oracle.py','reverse_reference.py']]
  inputs +=[D/'pq_sql020_build_inputs.py',D/'pq_native_build_inputs.py']
  inputs +=[SOURCE/n for n in build['native_source_inputs_after']]+[Path(n)for n in build['build_wrapper_inputs_after']]
  inputs +=list((INSTALL/'share/postgresql/extension').glob('og_lake*.sql'))
  archive=out/'executed-inputs';archive.mkdir();r['inputs']={}
  for i,x in enumerate(dict.fromkeys(x.resolve()for x in inputs)):
   saved=archive/(str(i)+'-'+x.name);shutil.copy2(x,saved);check('archived input bytes/'+str(i),sha256(saved),sha256(x))
   r['inputs'][str(x)]=dict(sha256=sha256(x),archive=str(saved))
  r['hash_only_inputs']={str(x.resolve()):sha256(x)for x in [INSTALL/'bin/gaussdb',ref.binary]+[sdk/n for n in sdkfiles]}
  with(out/'fixture.log').open('w')as log:
   subprocess.run([sys.executable,str(D/'tests/catalog_fixture_bundle.py'),'--run-id',a.run_id,'--output',str(out/'fixture/manifest.json')],stdout=log,stderr=subprocess.STDOUT,timeout=300,check=True)
  reservation=(ROOT/'.reference/opengauss-native-lake/.driver.lock').open('a+');fcntl.flock(reservation.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB)
  r['reference_reservation']=dict(path=str(reservation.name),acquired=True)
  db=OwnedDatabase(a.run_id,out);db.create();session(db.admin);roles=OwnedRoles(control,a.run_id,out)
  t=Case(db,SimpleNamespace(mode='net_dml',sql_version='0.21'if a.mode=='upgrade-round'else a.sql_version,manifest=out/'fixture/manifest.json',output=out/'result.json',run_id=a.run_id),r,roles)
  for c in[t.a,t.b,t.observer]:session(c)
  check('actual module matches chosen final runtime',json.loads(scalar(t.a,'SELECT og_lake.host_status()'))['module_version'],contract['runtime_version'])
  if a.mode=='upgrade-round':
   def map_shape():return query(t.observer,"SELECT attname,atttypid,attnotnull,atthasdef FROM pg_attribute WHERE attrelid='og_lake_internal.table_binding'::regclass AND attnum>0 AND NOT attisdropped ORDER BY attnum")
   check('explicit retainedSQL21 initial installation',scalar(t.observer,"SELECT extversion FROM pg_extension WHERE extname='og_lake'"),'0.21')
   old_shape=map_shape();check('retained21 canonical map exactly8columns',len(old_shape),8)
   query(t.a,"CREATE FOREIGN TABLE biz.legacy_round(id integer,p integer,val integer) SERVER created_lake OPTIONS(binding_mode 'create',lake_schema 'main')")
   query(t.a,"SELECT og_lake.set_option('biz.legacy_round'::regclass,'data_inlining_row_limit','1000')")
   query(t.a,'INSERT INTO biz.legacy_round VALUES(1,10,100)')
   old_binding=query(t.observer,"SELECT t.binding_id::text,t.table_uuid::text,t.schema_version FROM og_lake_internal.table_binding t JOIN og_lake_internal.relation_binding r USING(binding_id) WHERE r.relation_oid='biz.legacy_round'::regclass")
   owner=scalar(t.observer,"SELECT relowner FROM pg_class WHERE oid='og_lake_internal.table_binding'::regclass")
   query(t.a,'BEGIN');query(t.a,'SAVEPOINT upgrade_discard');query(t.a,"ALTER EXTENSION og_lake UPDATE TO '0.22'")
   shape=query(t.a,"SELECT attname,atttypid,attnotnull,atthasdef FROM pg_attribute WHERE attrelid='og_lake_internal.table_binding'::regclass AND attnum>0 AND NOT attisdropped ORDER BY attnum")
   check('tentative upgrade exposes exact12column shape',len(shape),12)
   query(t.a,'ROLLBACK TO upgrade_discard');query(t.a,'RELEASE upgrade_discard');query(t.a,'COMMIT')
   check('upgrade savepoint rollback retains21',scalar(t.observer,"SELECT extversion FROM pg_extension WHERE extname='og_lake'"),'0.21');check('upgrade rollback restores originalmapshape',map_shape(),old_shape)
   query(t.a,"ALTER EXTENSION og_lake UPDATE TO '0.22'");t.args.sql_version='0.22'
   shape=map_shape();check('committedupgrade exposes12columns',len(shape),12)
   int8oid=scalar(t.observer,"SELECT 'pg_catalog.int8'::regtype::oid");oidoid=scalar(t.observer,"SELECT 'pg_catalog.oid'::regtype::oid")
   check('new progress fields exacttypes nullable withno defaults',shape[8:],[(n,typ,False,False)for n,typ in zip(['flush_birth_cutoff','flush_carrier_oid','flush_source_schema','flush_source_origin'],[int8oid,oidoid,int8oid,int8oid])])
   check('upgrade retains validated privateowner',scalar(t.observer,"SELECT relowner FROM pg_class WHERE oid='og_lake_internal.table_binding'::regclass"),owner)
   check('upgrade retains old businessrows',query(t.observer,'SELECT * FROM biz.legacy_round'),[(1,10,100)])
   check('upgrade retains canonicalstableidentity',query(t.observer,"SELECT t.binding_id::text,t.table_uuid::text,t.schema_version FROM og_lake_internal.table_binding t JOIN og_lake_internal.relation_binding r USING(binding_id) WHERE r.relation_oid='biz.legacy_round'::regclass"),old_binding)
   check('old binding roundinitializedallNULL',query(t.observer,"SELECT t.flush_birth_cutoff,t.flush_carrier_oid,t.flush_source_schema,t.flush_source_origin FROM og_lake_internal.table_binding t JOIN og_lake_internal.relation_binding r USING(binding_id) WHERE r.relation_oid='biz.legacy_round'::regclass"),[(None,None,None,None)])
   query(t.a,"SELECT og_lake.flush('biz.legacy_round'::regclass)")
   check('ordinary nonpartitionedupgradeflush leaves progressNULL',query(t.observer,"SELECT t.flush_birth_cutoff,t.flush_carrier_oid,t.flush_source_schema,t.flush_source_origin FROM og_lake_internal.table_binding t JOIN og_lake_internal.relation_binding r USING(binding_id) WHERE r.relation_oid='biz.legacy_round'::regclass"),[(None,None,None,None)])
   query(t.a,'DROP FOREIGN TABLE biz.legacy_round')
   check('unbind removes upgraded progress together with canonical row',scalar(t.observer,'SELECT count(*) FROM og_lake_internal.table_binding WHERE binding_id=%s::uuid',(str(old_binding[0][0]),)),0)
  query(t.a,"SELECT og_lake.set_option('created_lake'::name,'data_inlining_row_limit','0')")
  query(t.a,"CREATE FOREIGN TABLE biz.orders(id integer,p integer,val integer) SERVER created_lake OPTIONS(binding_mode 'create',lake_schema 'main')")
  tid=scalar(t.observer,"SELECT table_id FROM created_metadata.ducklake_table WHERE table_name='orders' AND end_snapshot IS NULL")
  check('genuine no-carrier CREATE',query(t.observer,'SELECT table_name FROM created_metadata.ducklake_inlined_data_tables WHERE table_id=%s',(tid,)),[])
  check('initial GET unpartitioned',spec(),[]);before=t.head();setpart();check('SET publishes one snapshot',head(),before[0]+1)
  check('SET advances schema once',t.head()[1],before[1]+1);check('GET exact identity column',spec(),[(0,'p','identity')])
  partition_id=scalar(t.observer,'SELECT partition_id FROM created_metadata.ducklake_partition_info WHERE table_id=%s AND end_snapshot IS NULL',(tid,))
  key_id=scalar(t.observer,"SELECT column_id FROM created_metadata.ducklake_column WHERE table_id=%s AND column_name='p' AND parent_column IS NULL AND end_snapshot IS NULL",(tid,))
  check('actual spec stores stable column ID and identity ordinal',query(t.observer,'SELECT partition_key_index,column_id,transform FROM created_metadata.ducklake_partition_column WHERE table_id=%s AND partition_id=%s ORDER BY partition_key_index',(tid,partition_id)),[(0,key_id,'identity')])
  check('SET publishes exact standard altered_table changeset',query(t.observer,'SELECT changes_made FROM created_metadata.ducklake_snapshot_changes WHERE snapshot_id=%s',(head(),)),[('altered_table:'+str(tid),)])
  check('partition uses committed allocator identity',partition_id,before[2]);check('permanent allocator advanced once',t.head()[2],before[2]+1)
  root=Path(t.empty_manifest['data_path']).resolve()
  if a.mode=='definition':
   same=t.head();setpart();check('same-setting no-op new transaction',t.head(),same)
   query(t.a,'BEGIN');query(t.a,'SAVEPOINT discarded');reset();query(t.a,'ROLLBACK TO discarded');query(t.a,'RELEASE discarded')
   setpart();query(t.a,'COMMIT');check('subabort removes partition invocation marker',t.head(),same)
   query(t.a,'BEGIN');query(t.a,'SAVEPOINT retained');reset();query(t.a,'RELEASE retained');query(t.a,'SAVEPOINT denied')
   reject(t.a,"SELECT og_lake.set_partition('biz.orders'::regclass,ARRAY['p'])");query(t.a,'ROLLBACK TO denied');query(t.a,'ROLLBACK')
   check('subcommit marker plus parent rollback retain original spec',t.head(),same)
   before=state();query(t.a,'BEGIN');query(t.a,'INSERT INTO biz.heap_marker VALUES(1)');reset();query(t.a,'ROLLBACK')
   check('partition and heap rollback atomic',state(),before)
   reset();check('empty standard RESET GET',spec(),[])
   check('RESET empty spec contains no key rows',scalar(t.observer,'SELECT count(*) FROM created_metadata.ducklake_partition_column WHERE partition_id=(SELECT partition_id FROM created_metadata.ducklake_partition_info WHERE table_id=%s AND end_snapshot IS NULL)',(tid,)),0)
   setpart();query(t.a,"SELECT og_lake.set_option('biz.orders'::regclass,'data_inlining_row_limit','1000')")
   query(t.a,'INSERT INTO biz.orders VALUES(1,10,100)');query(t.a,'DELETE FROM biz.orders')
   check('deleted-all table is business empty',rows(),[]);check('empty carrier still registered',len(query(t.observer,'SELECT table_name FROM created_metadata.ducklake_inlined_data_tables WHERE table_id=%s',(tid,))),1)
   reject(t.a,"SELECT og_lake.reset_partition('biz.orders'::regclass)")
   reject(t.a,"SELECT og_lake.set_partition('biz.orders'::regclass,ARRAY['p'])")
  elif a.mode=='guards':
   # Test argument/owner gates while this table genuinely has no carrier.
   query(t.a,"CREATE FOREIGN TABLE biz.text_key(id integer,p text) SERVER created_lake OPTIONS(binding_mode 'create',lake_schema 'main')")
   baseline=state()
   for statement,code in [
     ("SELECT og_lake.set_partition('biz.orders'::regclass,NULL::text[])",'22004'),
     ("SELECT og_lake.set_partition('biz.orders'::regclass,ARRAY[]::text[])",'22023'),
     ("SELECT og_lake.set_partition('biz.orders'::regclass,ARRAY[NULL]::text[])",'0A000'),
     ("SELECT og_lake.set_partition('biz.orders'::regclass,ARRAY[ARRAY['p']])",'22023'),
     ("SELECT og_lake.set_partition('biz.orders'::regclass,ARRAY['p','id'])",'0A000'),
     ("SELECT og_lake.set_partition('biz.orders'::regclass,ARRAY[''])",'22023'),
     ("SELECT og_lake.set_partition('biz.orders'::regclass,ARRAY['missing'])",'42703'),
     ("SELECT og_lake.set_partition('biz.orders'::regclass,ARRAY['p+1'])",'42703'),
     ("SELECT og_lake.set_partition('biz.text_key'::regclass,ARRAY['p'])",'0A000'),
     ("SELECT og_lake.set_partition('biz.heap_marker'::regclass,ARRAY['id'])",'42809')]:
    reject(t.a,statement,code);check('parameter/type refusal has no side effects/'+statement,state(),baseline)
   query(t.a,'BEGIN READ ONLY');reject(t.a,"SELECT og_lake.reset_partition('biz.orders'::regclass)",'25006');query(t.a,'ROLLBACK')
   check('read-only refusal has no side effects',state(),baseline)
   other=roles.create('pgl_nonowner')
   query(t.a,sql.SQL('GRANT USAGE ON SCHEMA biz TO {}').format(sql.Identifier(other)))
   query(t.a,sql.SQL('GRANT USAGE ON FOREIGN SERVER created_lake TO {}').format(sql.Identifier(other)))
   query(t.a,sql.SQL('GRANT SELECT ON biz.orders TO {}').format(sql.Identifier(other)))
   nonowner,identity=roles.connect(db,'pgl_nonowner');session(nonowner);r['nonowner_identity']=identity
   for statement in ["SELECT og_lake.set_partition('biz.orders'::regclass,ARRAY['p'])","SELECT og_lake.reset_partition('biz.orders'::regclass)","SELECT * FROM og_lake.get_partition('biz.orders'::regclass)"]:
    reject(nonowner,statement,'42501');check('nonowner refusal has no metadata/file side effect',state(),baseline)
   query(t.a,'BEGIN')
   query(t.a,"CREATE FOREIGN TABLE biz.pending_key(id integer,p integer) SERVER created_lake OPTIONS(binding_mode 'create',lake_schema 'main')")
   reject(t.a,"SELECT og_lake.set_partition('biz.pending_key'::regclass,ARRAY['p'])")
   query(t.a,'ROLLBACK');check('transaction-local CREATE refusal rolls back catalog',state(),baseline)
   query(t.a,"SELECT og_lake.set_option('biz.orders'::regclass,'data_inlining_row_limit','1000')")
   query(t.a,'INSERT INTO biz.orders VALUES(1,10,100),(2,20,200),(3,10,300),(4,20,400)')
   old_ids={i:R for R,B,E,i,k,v in raw()if E is None}
   query(t.a,'UPDATE biz.orders SET p=20,val=101 WHERE id=1');query(t.a,'DELETE FROM biz.orders WHERE id=2')
   check('ordinary partitioned row UPDATE/DELETE exact values',rows(),[(1,20,101),(3,10,300),(4,20,400)])
   check('ordinary UPDATE preserves logical identity across key',[(R,i)for R,B,E,i,k,v in raw()if i==1 and E is None],[(old_ids[1],1)])
   flush();check('guard fixture has two actual partition files',len(files()),2)
   query(t.a,'INSERT INTO biz.orders VALUES(5,10,500)');check('guard fixture also has actual inline task',len(raw())>0)
   baseline=state();definition_count=scalar(t.observer,'SELECT count(*) FROM og_lake_internal.index_definition')
   for method in ['btree','bitmap']:
    reject(t.a,"SELECT og_lake.create_lake_index('biz.orders'::regclass,'partition_"+method+"',%s::jsonb)",args=(json.dumps(dict(version=1,kind='scalar',method=method,column='p')),))
    check('partitioned index refusal leaves definitions untouched',scalar(t.observer,'SELECT count(*) FROM og_lake_internal.index_definition'),definition_count)
    check('partitioned index refusal leaves full state untouched',state(),baseline)
   ids=[files()[0][0]]
   query(t.a,'BEGIN');query(t.a,'INSERT INTO biz.heap_marker VALUES(91)')
   reject(t.a,"SELECT og_lake.rewrite_data_files('biz.orders'::regclass,%s::bigint[])",args=(ids,));query(t.a,'ROLLBACK')
   check('N9 partition guard and ordinary heap rollback are atomic',state(),baseline)
   expected=[(1,20,101),(3,10,300),(4,20,400),(5,10,500)]
   query(t.a,'SET enable_lake_smp=on');query(t.a,'SET query_dop=1004')
   statement='SELECT id,p,val FROM biz.orders ORDER BY id'
   plan='\n'.join(x[0]for x in query(t.a,'EXPLAIN ANALYZE '+statement));(out/'partition-smp-serial-plan.txt').write_text(plan+'\n')
   check('partitioned two-file plus inline scan stays explicit serial',
     'Lake SMP: serial path;'in plan and 'Lake SMP Planned DOP:'not in plan and 'Stream'not in plan)
   check('serial partitioned scan retains every original value',query(t.a,statement),expected)
   session(t.a);query(t.a,'SET enable_vector_engine=on');query(t.a,"SET try_vector_engine_strategy='force'")
   plan='\n'.join(x[0]for x in query(t.a,'EXPLAIN ANALYZE '+statement));(out/'partition-vec-row-plan.txt').write_text(plan+'\n')
   check('partitioned table retains ordinary row delivery',
     'Lake Batch Delivery: row ForeignScan'in plan and 'VecForeignScan'not in plan and 'Vector Foreign Scan'not in plan)
   check('row fallback keeps exact values and NULL semantics',query(t.a,statement),expected)
   session(t.a)
  else:
   query(t.a,"SELECT og_lake.set_option('biz.orders'::regclass,'data_inlining_row_limit','1000')")
   values=[(1,10,100),(2,20,200),(3,10,300),(4,20,400)]
   if a.mode in('fair-round','upgrade-round'):values=[(1,10,100),(2,20,200),(3,30,300),(4,40,400)]
   if a.mode=='prefix':values=[(i,[10,20,None,-2147483648,2147483647,0][(i-1)%6],i*100)for i in range(1,13)]
   query(t.a,'BEGIN')
   with t.a.cursor()as c:c.executemany('INSERT INTO biz.orders VALUES(%s,%s,%s)',values)
   query(t.a,'COMMIT') # One actual initial publication; birth is read, never guessed.
   initial=raw();event_first=head();event_ids={i:R for R,B,E,i,k,v in initial if E is None};event_values=deepcopy(values)
   check('original fixture INSERTs have exactly one stable identity each',len(event_ids),len(values))
   check('business event log uses no file layout identity',set(event_ids),{i for i,k,v in values})
   if a.mode in('fair-round','upgrade-round'):
    # This case uses actual Native updates/flush, not the pure prefix model.
    query(t.a,'CREATE TABLE biz.round_exact(id integer,p integer,val integer)')
    with t.a.cursor()as c:c.executemany('INSERT INTO biz.round_exact VALUES(%s,%s,%s)',values)
    original=raw();stable={i:(R,B)for R,B,E,i,k,v in original};births={i:B for i,(R,B)in stable.items()}
    low=[x[3]for x in original[:2]];high=[x[3]for x in original[2:]];source_keys={i:k for R,B,E,i,k,v in original}
    check('four distinct realR/key source versions',len(original)==4 and len(set(source_keys.values()))==4)
    low_statement='UPDATE biz.orders SET val=val+1 WHERE id IN('+','.join(str(x)for x in low)+')'
    def progress():
     return query(t.observer,'SELECT t.flush_birth_cutoff,t.flush_carrier_oid,t.flush_source_schema,t.flush_source_origin '
       'FROM og_lake_internal.table_binding t JOIN og_lake_internal.relation_binding r USING(binding_id) '
       "WHERE r.relation_oid='biz.orders'::regclass")
    def save_round(label):
     for R,B,E,i,k,v in raw():
      if E is None:
       check(label+' same logicalR/'+str(i),R,stable[i][0]);births[i]=B
     expected=query(t.observer,'SELECT id,p,val FROM biz.round_exact ORDER BY id')
     check(label+' real ordinary heap exact results',rows(),expected)
     history.append((head(),[dict(logical_rowid=stable[i][0],begin_snapshot=births[i],id=i,p=k,val=v)for i,k,v in sorted(expected,key=lambda x:stable[x[0]][0])]))
    def mutate_round(statement,label):
     before_rows={i:(k,v)for i,k,v in query(t.observer,'SELECT id,p,val FROM biz.round_exact ORDER BY id')}
     query(t.b,'BEGIN');query(t.b,statement);query(t.b,statement.replace('biz.orders','biz.round_exact'));query(t.b,'COMMIT')
     after_rows={i:(k,v)for i,k,v in query(t.observer,'SELECT id,p,val FROM biz.round_exact ORDER BY id')}
     # Independent ordinary-heap pre/postimages from the explicit same DML;
     # no output file/delete metadata is used to invent expected CDC.
     for i in sorted(before_rows):
      if before_rows[i]!=after_rows[i]:
       event_changes.extend([(head(),'update_preimage',i,*before_rows[i]),(head(),'update_postimage',i,*after_rows[i])])
     save_round(label)
    inactive=[(None,None,None,None)];check('fresh host round inactive',progress(),inactive);save_round('initial')
    before=dict(state=state(),progress=progress())
    query(t.a,'BEGIN');query(t.a,'SAVEPOINT discard_round');flush();query(t.a,'ROLLBACK TO discard_round');query(t.a,'RELEASE discard_round');query(t.a,'COMMIT')
    check('savepoint abort retains source/catalog and round atomically',dict(state=state(),progress=progress()),before)
    H=head();flush();save_round('first two-key batch')
    active=progress();check('unfinished round persists its original logical cutoff',active[0][0],H)
    expected_oid=scalar(t.observer,'SELECT c.oid FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace '
      'WHERE n.nspname=%s AND c.relname=%s',('created_metadata',carrier()))
    check('round binds actual carrierOID',active[0][1],expected_oid)
    source_schema=scalar(t.observer,'SELECT schema_version FROM created_metadata.ducklake_inlined_data_tables WHERE table_id=%s',(tid,))
    check('round binds actual inline source schema',active[0][2],source_schema)
    source_origin=scalar(t.observer,'SELECT min(begin_snapshot) FROM created_metadata.ducklake_schema_versions WHERE table_id=%s AND schema_version=%s',(tid,source_schema))
    check('round binds exact source origin snapshot',active[0][3],source_origin)
    active_before=dict(state=state(),progress=progress())
    query(t.a,'BEGIN');query(t.a,'SAVEPOINT active_child');flush();query(t.a,'ROLLBACK TO active_child');query(t.a,'RELEASE active_child');query(t.a,'COMMIT')
    check('active round survives child preparation rollback atomically',dict(state=state(),progress=progress()),active_before)
    binding_uuid=str(scalar(t.observer,"SELECT binding_id FROM og_lake_internal.relation_binding WHERE relation_oid='biz.orders'::regclass"))
    query(t.a,'BEGIN');query(t.a,'DROP FOREIGN TABLE biz.orders')
    check('DROP removes active progress in the same canonical row',scalar(t.a,'SELECT count(*) FROM og_lake_internal.table_binding WHERE binding_id=%s::uuid',(binding_uuid,)),0)
    query(t.a,'ROLLBACK');check('DROP rollback restores active H and whole source',dict(state=state(),progress=progress()),active_before)
    check('first two lowR groups only, highR rows remain',[(i,k)for R,B,E,i,k,v in raw()if B<=H],[(x[3],x[4])for x in original[2:]])
    mutate_round(low_statement,'first lowR churn')
    check('new lowR births are beyond fixedH',all(B>H for R,B,E,i,k,v in raw()if i in low))
    mutate_round('UPDATE biz.orders SET val=val+10 WHERE id='+str(high[0]),'captured end newer thanH')
    high3=next(x for x in raw()if x[3]==high[0] and x[1]<=H);check('captured old highR end is newer than roundH',high3[2]>H)
    prepared=raw();S_prep=head();before_files={x[0]for x in files()};query(t.a,'BEGIN');flush()
    check('prepared batch does not publish progress early',progress(),active)
    mutate_round('UPDATE biz.orders SET p=NULL,val=val+40 WHERE id='+str(high[1]),'late highR end')
    late4=next(x for x in raw()if x[3]==high[1] and x[1]<=H);check('late old highR end is newer than actual preparationS',late4[2]>S_prep)
    mutate_round(low_statement,'second lowR churn during prepared batch')
    query(t.a,'COMMIT');save_round('second batch drains originalH')
    check('fixed round closes although lowR updates continue',progress(),inactive)
    check('no originalH version starved behind fresh lowR births',any(B<=H for R,B,E,i,k,v in raw()),False)
    physical,index=inspect('fair-first-round')
    check('every original stable version is in exactly one real file',Counter((x['logical_rowid'],x['begin_snapshot'],x['id'],x['p'],x['val'])for x in physical),Counter((R,B,i,k,v)for R,B,E,i,k,v in original))
    check('second batch physically wrote the two highR keys',sorted({x['p']for x in physical if x['file_id']not in before_files}),sorted(source_keys[x]for x in high))
    fid3,pos3=index[stable[high[0]]];fid4,pos4=index[stable[high[1]]]
    cap_path=scalar(t.observer,'SELECT path FROM created_metadata.ducklake_delete_file WHERE table_id=%s AND data_file_id=%s',(tid,fid3))
    marks=ref.run('SELECT pos,_ducklake_internal_snapshot_id AS ended FROM read_parquet('+literal(str(data_path(cap_path)))+') ORDER BY pos','fair-captured',result=True)
    check('captured end>H but<=S kept its exact physicalP',[(x['pos'],x['ended'])for x in marks],[(pos3,high3[2])])
    late_marks=query(t.observer,sql.SQL('SELECT row_id,begin_snapshot FROM created_metadata.{} WHERE file_id=%s ORDER BY row_id').format(sql.Identifier('ducklake_inlined_delete_'+str(tid))),(fid4,))
    check('late end>S kept group-local physicalP',late_marks,[(pos4,late4[2])])
    # Further bounded actual calls continue low-R production while finite cohorts
    # finish. They are real sequential transactions with overlapping prepare above.
    completions=0
    for turn in range(6):
     mutate_round(low_statement,'continued lowR churn '+str(turn))
     old=progress();fence=head()if old==inactive else old[0][0];eligible=[x for x in raw()if x[1]<=fence]
     check('each churn round has actual eligible original source',bool(eligible))
     flush();save_round('continued bounded flush '+str(turn));remaining=[x for x in raw()if x[1]<=fence]
     check('each real call advances its fixed finite cohort',len(remaining)<len(eligible))
     now=progress()
     if not remaining:check('completed cohort resets scheduling fence only',now,inactive);completions+=1
     else:check('unfinished cohort retains same H',now[0][0],fence)
    check('sustained lowR churn allows repeated complete cohorts',completions>=2)
    reference('fair-round-full-history')
   elif a.mode=='groups':
    remember();query(t.a,'UPDATE biz.orders SET p=20,val=101 WHERE id=1')
    event_changes.extend([(head(),'update_preimage',1,10,100),(head(),'update_postimage',1,20,101)]);remember()
    query(t.a,'UPDATE biz.orders SET p=10,val=102 WHERE id=1')
    event_changes.extend([(head(),'update_preimage',1,20,101),(head(),'update_postimage',1,10,102)]);remember()
    prepared=raw();baseline_rows=rows();frontier=scalar(t.observer,'SELECT next_row_id FROM created_metadata.ducklake_table_stats WHERE table_id=%s',(tid,));old=head()
    query(t.a,'BEGIN');flush();check('two genuine group files prepared before claim',len(disk()),4)
    check('prepared groups metadata still invisible',head(),old);check('prepared original rows remain visible',rows(),baseline_rows)
    query(t.b,'DELETE FROM biz.orders WHERE id=2');event_changes.append((head(),'delete',2,20,200));remember()
    query(t.b,'UPDATE biz.orders SET p=20,val=103 WHERE id=1')
    event_changes.extend([(head(),'update_preimage',1,10,102),(head(),'update_postimage',1,20,103)]);remember()
    late_source=raw();business_stats=query(t.observer,'SELECT record_count,next_row_id FROM created_metadata.ducklake_table_stats WHERE table_id=%s',(tid,))
    query(t.a,'COMMIT');check('group flush is one new snapshot',head(),history[-1][0]+1)
    check('maintenance does not alter physical stored-count or identity frontier',query(t.observer,'SELECT record_count,next_row_id FROM created_metadata.ducklake_table_stats WHERE table_id=%s',(tid,)),business_stats)
    check('two standard data files published',len(files()),2)
    physical,index=inspect('first')
    expected=Counter((R,B,i,k,v)for R,B,E,i,k,v in prepared)
    check('all prepared physical payloads/R/birth exactly once',Counter((x['logical_rowid'],x['begin_snapshot'],x['id'],x['p'],x['val'])for x in physical),expected)
    check('no stable identity allocation during maintenance',scalar(t.observer,'SELECT next_row_id FROM created_metadata.ducklake_table_stats WHERE table_id=%s',(tid,)),frontier)
    known={(R,B)for R,B,E,*_ in prepared};check('only later born version remains inline',[(R,B)for R,B,*_ in raw()],[(R,B)for R,B,*_ in late_source if(R,B)not in known])
    expected_captured=sorted((index[(R,B)][0],index[(R,B)][1],E)for R,B,E,*_ in prepared if E is not None)
    captured=[]
    for fid,path in query(t.observer,'SELECT data_file_id,path FROM created_metadata.ducklake_delete_file WHERE table_id=%s ORDER BY delete_file_id',(tid,)):
     z=ref.run('SELECT file_path,pos,_ducklake_internal_snapshot_id AS end_snapshot FROM read_parquet('+literal(str(data_path(path)))+') ORDER BY pos','captured-'+str(fid),result=True)
     for x in z:check('captured marker exact physical source/'+str(fid),x['file_path'],str(data_path(next(f[1]for f in files()if f[0]==fid))))
     captured +=[(fid,x['pos'],x['end_snapshot'])for x in z]
    check('captured ends use original file-local physical positions',sorted(captured),expected_captured)
    late=[(index[(R,B)][0],index[(R,B)][1],E)for R,B,E,*_ in late_source if(R,B)in known and E is not None and E>old]
    check('late ends use group-local positions',query(t.observer,sql.SQL('SELECT file_id,row_id,begin_snapshot FROM created_metadata.{} ORDER BY file_id,row_id').format(sql.Identifier('ducklake_inlined_delete_'+str(tid)))),sorted(late))
    reference('groups-final')
   elif a.mode=='prefix':
    remember();expected_rows=rows();all_physical=[];rounds=0
    while raw():
     before=raw();keys=[];selected=[]
     for row in before:
      k=row[4]
      if k not in keys:
       if len(keys)==2:break
       keys.append(k)
      selected.append(row)
     oldfiles={x[0]for x in files()};flush();rounds+=1
     now=raw();check('strict logical-version prefix moved/'+str(rounds),now,before[len(selected):])
     check('new group data file count/'+str(rounds),len([x for x in files()if x[0]not in oldfiles]),len(keys))
     check('business values survive bounded prefix/'+str(rounds),rows(),expected_rows)
     if rounds>len(values):raise AssertionError('bounded prefix did not progress')
    physical,index=inspect('prefix-final');check('all12 original stable identities once',len(index),len(values))
    reference('prefix-final')
   elif a.mode=='rollback':
    before=state();query(t.a,'BEGIN');query(t.a,'INSERT INTO biz.heap_marker VALUES(1)');flush();check('actual two candidates exist before rollback',len(disk()),2)
    query(t.a,'ROLLBACK');check('metadata/heap/exact owned candidate rollback',state(),before)
    query(t.a,'BEGIN');query(t.a,'SAVEPOINT child');flush();query(t.a,'ROLLBACK TO child');query(t.a,'RELEASE child');query(t.a,'COMMIT')
    check('child rollback discards group files and source cleanup',state(),before)
    query(t.a,'BEGIN');query(t.a,'SAVEPOINT child');flush();query(t.a,'RELEASE child');query(t.a,'ROLLBACK')
    check('adopted group candidates rollback with parent',state(),before)
   else:
    # Keep the existing1000-row policy; test the transaction's final new
    # versions, not total carrier occupancy. The original four rows remain.
    limit=1000
    query(t.a,"SELECT og_lake.set_option('biz.orders'::regclass,'data_inlining_row_limit','1000')")
    def allocator():return query(t.observer,'SELECT record_count,next_row_id,file_size_bytes FROM created_metadata.ducklake_table_stats WHERE table_id=%s',(tid,))
    before_rows=deepcopy(values);before_raw=raw();before_stats=allocator();before_head=head();before_files=files();before_disk=disk()
    check('positive limit fixture has exact one standard allocator',len(before_stats),1)
    check('positive limit fixture has no data files',before_files,[])
    accepted=[(10000+i,10,1000000+i)for i in range(1,limit+1)]
    query(t.a,'BEGIN');query(t.a,'INSERT INTO biz.heap_marker VALUES(1000)')
    query(t.a,'INSERT INTO biz.orders SELECT 10000+i,10,1000000+i FROM generate_series(1,%s)i',(limit,))
    query(t.a,'COMMIT')
    check('exact1000 configured limit accepts all independent business values',rows(),sorted(before_rows+accepted))
    check('exact1000 configured limit commits ordinary heap atomically',query(t.observer,'SELECT * FROM biz.heap_marker ORDER BY id'),[(1000,)])
    check('exact limit publication advances one snapshot',head(),before_head+1)
    after_stats=allocator();check('exact limit allocator advances only accepted identities',after_stats,[(before_stats[0][0]+limit,before_stats[0][1]+limit,before_stats[0][2])])
    after_raw=raw();old_ids={i for i,k,v in before_rows}
    check('exact limit keeps original row_id birth and values',[x for x in after_raw if x[3]in old_ids],before_raw)
    fresh=[x for x in after_raw if x[3]not in old_ids]
    check('exact limit allocates one standard contiguous identity range',sorted(x[0]for x in fresh),list(range(before_stats[0][1],after_stats[0][1])))
    check('exact limit has one current birth per accepted identity',[(x[1],x[2])for x in fresh],[(head(),None)]*limit)
    check('exact limit remains inline without bypassing partition grouping',files(),before_files)
    check('exact limit creates no Parquet candidate',disk(),before_disk)
    before=state();rollback_stats=allocator();rollback_raw=raw()
    r['direct_limit_boundary']=dict(limit=limit,accepted_rows=limit,rejected_rows=limit+1,accepted_allocator=canonical(after_stats))
    query(t.a,'BEGIN');query(t.a,'INSERT INTO biz.heap_marker VALUES(1001)')
    try:
     query(t.a,'INSERT INTO biz.orders SELECT 20000+i,20,2000000+i FROM generate_series(1,%s)i',(limit+1,));query(t.a,'COMMIT')
    except Exception as e:check('partitioned limit1000 rejects1001 final versions',getattr(e,'pgcode',None),'0A000');query(t.a,'ROLLBACK')
    else:
     r['unexpected_positive_limit_acceptance']=dict(before=before,after=state(),allocator=canonical(allocator()),raw=canonical(raw()))
     write_json(out/'result.json',r);raise AssertionError('partitioned transaction above configured positive inline limit unexpectedly accepted')
    check('limitNplus1 rolls back complete metadata heap and files',state(),before)
    check('limitNplus1 restores exact row_id allocator counts',allocator(),rollback_stats)
    check('limitNplus1 restores every original physical inline identity',raw(),rollback_raw)
    query(t.a,"SELECT og_lake.set_option('biz.orders'::regclass,'data_inlining_row_limit','0')")
    before=state();query(t.a,'BEGIN');query(t.a,'INSERT INTO biz.heap_marker VALUES(1)')
    try:
     query(t.a,'INSERT INTO biz.orders VALUES(99,10,9900)');query(t.a,'COMMIT')
    except Exception as e:check('partitioned direct cannot bypass grouping',getattr(e,'pgcode',None),'0A000');query(t.a,'ROLLBACK')
    else:raise AssertionError('partitioned direct file write unexpectedly accepted')
    check('direct refusal leaves complete state unchanged',state(),before)
  check('all executed code/SQL/evidence inputs unchanged',all(sha256(n)==x['sha256']for n,x in r['inputs'].items()))
  check('all immutable executable/SDK inputs unchanged',all(sha256(n)==h for n,h in r['hash_only_inputs'].items()))
  check_build(a.build_receipt);r['status']='pass'
 except BaseException as e:r.update(reason=str(e),traceback=traceback.format_exc())
 finally:
  if t is not None:
   for c in[t.a,t.b,t.observer]:
    try:
     if not c.closed:query(c,'ROLLBACK')
    except BaseException as e:r.setdefault('rollback_errors',[]).append(str(e))
  if db is not None:
   try:db.cleanup();r['database_cleanup']=db.cleanup_result
   except BaseException as e:r.update(status='fail',database_cleanup_error=str(e))
  if roles is not None:
   try:
    if not r.get('database_cleanup')or r['database_cleanup']['returncode']:raise RuntimeError('owned DB cleanup unproven; preserve roles')
    r['role_cleanup']=cleanup_roles(control,roles.path,a.run_id);r=roles.redact(r)
   except BaseException as e:r.update(status='fail',role_cleanup_error=str(e))
  if control is not None:
   try:control.close()
   except BaseException as e:r.update(status='fail',control_cleanup_error=str(e))
  if reservation is not None:
   if r.get('reference_reservation',{}).get('acquired'):fcntl.flock(reservation.fileno(),fcntl.LOCK_UN)
   reservation.close();r.setdefault('reference_reservation',{})['released_after_cleanup_attempt']=True
  write_json(out/'result.json',r)
 print(json.dumps(dict(status=r['status'],mode=a.mode,checks=len(r['assertions']),reason=r.get('reason'),result=str(out/'result.json'))))
 return 0 if r['status']=='pass'else 1

if __name__=='__main__':raise SystemExit(main())
