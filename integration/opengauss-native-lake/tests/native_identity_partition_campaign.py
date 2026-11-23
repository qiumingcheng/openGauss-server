#!/usr/bin/env python3
"""Serial8 PGL SQL22 qualification, default prepare only.

Root supplies the successful future full/build, reviewed final source contract,
qualified SDK index and stopped competing fixtures. Each executed leaf owns a
fresh EMPTY and its exact DB/roles cleanup. This supervisor never touches DB
cleanup/configuration/services, never retries a failed leaf and never kills a
server backend. A1200s deadline signals only its nonce-verified Python group.
"""
import argparse,ast,fcntl,hashlib,json,os,re,secrets,shutil,signal,subprocess,sys,time,traceback
from pathlib import Path
ROOT=Path(os.getenv('CPP_WORKSPACE','/home/qiumc/data/code/cpp')).resolve()
D=ROOT/'integration/opengauss-native-lake';HERE=Path(__file__).resolve().parent
LEAF=HERE/'native_identity_partition.py'
LEAF_SHA='f4ac372ef5d6ac5f470e9c197101282952a553e47d81027f6856fc4829ca168a'
MODES=['definition','groups','prefix','rollback','direct-refusal','guards','fair-round','upgrade-round']
HARD_SECONDS=1200

def sha(p):
 h=hashlib.sha256()
 with Path(p).open('rb')as f:
  for b in iter(lambda:f.read(1024*1024),b''):h.update(b)
 return h.hexdigest()

def dump(path,value):
 path=Path(path);tmp=path.with_name(path.name+'.new')
 with tmp.open('w')as f:json.dump(value,f,ensure_ascii=False,indent=2);f.write('\n');f.flush();os.fsync(f.fileno())
 os.replace(tmp,path)

def plan(modes):
 if sha(LEAF)!=LEAF_SHA:raise RuntimeError('Exact reviewed8mode leaf differs')
 choices=next(ast.literal_eval(n.value)for n in ast.walk(ast.parse(LEAF.read_text()))if isinstance(n,ast.Assign)and any(isinstance(t,ast.Name)and t.id=='MODES'for t in n.targets))
 if list(choices)!=MODES or len(set(modes))!=len(modes):raise ValueError('Changed or duplicate modes')
 return dict(status='prepared_not_executed',strict_serial=True,Native=0,SDK=0,fixtures=0,leaf_sha256=LEAF_SHA,controller_sha256=sha(__file__),cases=[dict(mode=m,hard_seconds=HARD_SECONDS,termination_grace_seconds=[5,10],initial_sql='0.21'if m=='upgrade-round'else'0.22',final_sql='0.22',fixture='Actual child-owned EMPTY_BUNDLE; exact childrunID and independently disjoint data root')for m in modes],requirements=['Root successful exact SQL22 Debug full and reviewed final source contract with canonical-table-binding-fixed-birth-v1.','Exact12case newSDK qualification index+SHA; installed prefix/Native fullheaders match; source/helpers archived and checked before/after everycase.','Maintenanceoff,indexmaintenanceoff,dedicated backend,nootherfrontend/retainedownedDB; root configures outside thisentry.','No outer driver flock: each leaf owns actual generator/reference reservation.','1200s bound includes fixture/native/cleanup; forcedtimeout preserves scene and stopsmatrix.'])

def members(record,nonce_digest,proc=Path('/proc')):
 """Prove every living member before signalling a group; never expose nonce."""
 found=[]
 for path in proc.iterdir():
  if not path.name.isdigit():continue
  try:
   stat=(path/'stat').read_text();tail=stat[stat.rfind(')')+2:].split()
   if int(tail[2])!=record['pgid']or int(tail[3])!=record['sid']or tail[0]=='Z':continue
   if path.stat().st_uid!=record['uid']:raise RuntimeError('Owned Python group UID changed')
   env=(path/'environ').read_bytes().split(b'\0');entry=next((x.split(b'=',1)[1]for x in env if x.startswith(b'OG_LAKE_PGL_OWNER_NONCE=')),None)
   if entry is None or hashlib.sha256(entry).hexdigest()!=nonce_digest:raise RuntimeError('Group member nonce proof differs; refuse signal')
   ticks=int(tail[19])
   if int(path.name)==record['pid']and ticks!=record['start_ticks']:raise RuntimeError('Leader PID reused; refuse signal')
   found.append(dict(pid=int(path.name),start_ticks=ticks))
  except FileNotFoundError:continue
 return found

def spawn_identity(child,nonce_digest):
 path=Path('/proc')/str(child.pid);stat=(path/'stat').read_text();tail=stat[stat.rfind(')')+2:].split()
 r=dict(pid=child.pid,pgid=int(tail[2]),sid=int(tail[3]),start_ticks=int(tail[19]),uid=path.stat().st_uid,nonce_sha256=nonce_digest)
 if r['pgid']!=child.pid or r['sid']!=child.pid or r['uid']!=os.getuid():raise RuntimeError('New child session ownership differs')
 members(r,nonce_digest);return r

def stop_owned(child,record,nonce_digest):
 signals=[]
 for sig,grace in [(signal.SIGTERM,5),(signal.SIGKILL,10)]:
  if not members(record,nonce_digest):break
  try:os.killpg(record['pgid'],sig);signals.append(int(sig))
  except ProcessLookupError:break
  until=time.monotonic()+grace
  while time.monotonic()<until:
   child.poll()
   if not members(record,nonce_digest):break
   time.sleep(.05)
 if members(record,nonce_digest):raise RuntimeError('Owned Python group not drained; preserve exact scene')
 child.wait(timeout=2);return signals

def execute(a,prepared):
 sys.path[:0]=[str(D),str(D/'tests')]
 from file_dml_acceptance import check_build
 from inline_fixture import query,scalar
 from native_auto_flush import connect_control
 from native_runtime import PROFILE,RUNTIME,SOURCE,INSTALL
 from lake_harness import validate_run_id
 from native_direct_campaign import assertions,verify_archive,verify_cleanup
 from empty_manifest import validate_empty_bundle,validate_empty_manifest
 from pq_sql020_build_inputs import sdk_index
 validate_run_id(a.run_id)
 if PROFILE!='debug'or len(a.run_id)>65:raise RuntimeError('Debug ownedcampaign requires runid<=65 for childsuffix budget')
 out=ROOT/'公共文档/openGauss原生湖存储/验证'/a.run_id;out.mkdir(exist_ok=False)
 result=dict(status='fail',plan=prepared,cases=[],runtime_executed=False,controller_database_cleanup_attempted=False)
 control=None;locks=[];child=record=None;nonce_digest=None
 try:
  for name in ['.n7-qualification.lock','.ix5-qualification.lock','native-pgl-campaign.lock']:
   f=(RUNTIME/name).open('a+');fcntl.flock(f,fcntl.LOCK_EX|fcntl.LOCK_NB);locks.append(f)
  build=check_build(a.build_receipt);qualified=sdk_index(a.sdk_index,a.sdk_index_sha256,ROOT);contract=json.loads(a.source_contract.read_text())
  if contract.get('purpose')!='pgl0_identity_partition_qualification'or contract.get('ready_for_native_qualification')is not True or contract.get('known_blockers')!=[]or contract.get('runtime_version')!='0.22'or contract.get('partition_sql_version')!='0.22'or contract.get('flush_fair_round_contract')!='canonical-table-binding-fixed-birth-v1':raise RuntimeError('FinalSQL22/fairround contract not qualified')
  control=connect_control();query(control,'SET session_timeout=0');query(control,'SET statement_timeout=30000')
  for name,want in [('enable_lake_maintenance','off'),('enable_lake_index_maintenance','off'),('enable_thread_pool','off')]:
   if scalar(control,'SHOW '+name)!=want:raise RuntimeError('Runtime prerequisite '+name)
  versions=re.findall(r"^default_version\s*=\s*'([^']+)'",(SOURCE/'contrib/og_lake/og_lake.control').read_text(),re.M)
  if versions!=['0.22']:raise RuntimeError('Reviewed source module mustbe22; eachownedleaf verifiesactualmodule')
  def identity():return dict(postmaster_start=str(scalar(control,'SELECT pg_postmaster_start_time()')),data=str(Path(scalar(control,'SHOW data_directory')).resolve()),administrator_oid=scalar(control,'SELECT oid FROM pg_roles WHERE rolname=current_user'),binary_sha256=sha(INSTALL/'bin/gaussdb'))
  initial=identity()
  def quiet():
   owned=query(control,"SELECT oid,datname FROM pg_database WHERE datname LIKE 'lake_inline_%%'")
   others=query(control,'SELECT pid,datname FROM pg_stat_activity WHERE client_port IS NOT NULL AND pid<>pg_backend_pid()')
   if owned or others:raise RuntimeError('Retainedscene/unrelatedfrontend; no cleanup: '+repr((owned,others)))
  quiet();archive=out/'frozen';archive.mkdir();inputs={}
  def add(path,relative,want=None):
   path=Path(path).resolve();h=sha(path)
   if want is not None and h!=want:raise RuntimeError('Frozeninput differs: '+str(path))
   if str(path)in inputs:return
   dst=archive/relative;dst.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(path,dst)
   if sha(dst)!=h:raise RuntimeError('Archive changed')
   inputs[str(path)]=dict(sha256=h,archive=str(dst))
  for name,path in [('controller.py',Path(__file__)),('leaf.py',LEAF),('build.json',a.build_receipt),('source-contract.json',a.source_contract),('sdk-index.json',a.sdk_index)]:add(path,name)
  for n,h in build['native_source_inputs_after'].items():add(SOURCE/n,Path('native-source')/n,h)
  for n,h in build.get('build_wrapper_inputs_after',{}).items():add(n,Path('wrappers')/Path(n).relative_to(ROOT),h)
  helpers=set(contract['shared_helper_inputs'])|{'native_runtime.py','native_test_context.py','inline_fixture.py','lake_harness.py','source-lock.json','reference.sh','pq_sql020_build_inputs.py','pq_native_build_inputs.py'}
  helpers|={'tests/'+n for n in ['native_direct_campaign.py','native_direct_insert.py','native_table_create.py','file_dml_acceptance.py','native_auto_flush.py','permission_api.py','inline_cleanup.py','catalog_fixture_bundle.py','empty_manifest.py','empty_reference_fixture.py','empty_catalog_oracle.py','create_reference_oracle.py','reverse_reference.py']}
  for n in sorted(helpers):add(D/n,Path('integration')/n,contract['shared_helper_inputs'].get(n))
  for n,h in contract['review_evidence'].items():add(n,Path('review')/(str(len(inputs))+'-'+Path(n).name),h)
  ref=ROOT/'.reference/opengauss-native-lake/build/receipt.json';reference=json.loads(ref.read_text());add(ref,'reference-build.json')
  if reference['source_lock_hash']!=sha(D/'source-lock.json'):raise RuntimeError('Reference lock differs')
  hashes={str(INSTALL/'bin/gaussdb'):build['binary_sha256'],reference['binary']:reference['binary_sha256']}
  sdk=INSTALL/'lib/postgresql/og_lake_sdk';add(sdk/'owner.json','sdk-owner.json')
  for path in sdk.rglob('*'):
   if path.is_file():hashes[str(path)]=sha(path)
  for n,h in hashes.items():
   if sha(n)!=h:raise RuntimeError('Binary/dependency differs')
  for key in ['roaring_inputs_after','pq_inputs_after']:
   dep=build.get(key)
   if dep:
    add(dep['receipt'],Path('dependencies')/key/'receipt.json',dep['receipt_sha256'])
    for n,h in dep.get('files',{}).items():hashes[str(Path(dep['prefix'])/n)]=h
  frozen=out/'frozen-inputs.json';dump(frozen,dict(inputs=inputs,hash_only_inputs=hashes,identity=initial,sdk_qualification=qualified));result.update(identity=initial,frozen_manifest=str(frozen),frozen_sha256=sha(frozen),binary_sha256=build['binary_sha256']);dump(out/'index.json',result)
  def verify():
   check_build(a.build_receipt);sdk_index(a.sdk_index,a.sdk_index_sha256,ROOT)
   if identity()!=initial or any(sha(n)!=h for n,h in hashes.items()):raise RuntimeError('Runtime/binary/dependency transition; no crossrestartcleanup')
   return verify_archive(dict(inputs=inputs))
  roots=[]
  for spec in prepared['cases']:
   verify();quiet();child_id=a.run_id+'_'+spec['mode'];validate_run_id(child_id);leaf=out.parent/child_id
   if leaf.exists():raise FileExistsError('Never overwrite priorleaf')
   command=[sys.executable,str(LEAF),'--execute','--mode',spec['mode'],'--sql-version','0.22','--run-id',child_id,'--build-receipt',str(a.build_receipt.resolve()),'--source-contract',str(a.source_contract.resolve()),'--sdk-qualification',str(a.sdk_index.resolve()),'--sdk-index-sha256',a.sdk_index_sha256]
   nonce=secrets.token_hex(24);nonce_digest=hashlib.sha256(nonce.encode()).hexdigest();env=dict(os.environ,CPP_WORKSPACE=str(ROOT),OG_LAKE_TEST_PROFILE='debug',PYTHONDONTWRITEBYTECODE='1',OG_LAKE_PGL_OWNER_NONCE=nonce)
   row=dict(mode=spec['mode'],command=command,status='running',hard_seconds=HARD_SECONDS,preserve_owned_scene=True);result['cases'].append(row);result['runtime_executed']=True;started=time.monotonic();timed=False
   with(out/(spec['mode']+'.stdout')).open('wb')as log:
    child=subprocess.Popen(command,stdout=log,stderr=subprocess.STDOUT,env=env,start_new_session=True);row['python_pid']=child.pid;row['started_monotonic']=started;dump(out/'index.json',result)
    record=spawn_identity(child,nonce_digest);row['process_identity']=record;dump(out/'index.json',result)
    try:child.wait(timeout=max(0,HARD_SECONDS-(time.monotonic()-started)))
    except(subprocess.TimeoutExpired,KeyboardInterrupt):
     timed=True;row['signals_to_verified_python_group']=stop_owned(child,record,nonce_digest)
   row.update(status='fail',returncode=child.returncode,hard_timeout=timed,elapsed_seconds=round(time.monotonic()-started,3));dump(out/'index.json',result)
   if timed or child.returncode!=0:raise RuntimeError('ActualPGLleaf failed/timedout; stopmatrix and preserve exactscene')
   if members(record,nonce_digest):raise RuntimeError('Leaf exited but owned helper stilllive; preserve scene')
   child=record=None;nonce_digest=None
   data=json.loads((leaf/'result.json').read_text());row['leaf_result_sha256']=sha(leaf/'result.json')
   if data.get('status')!='pass'or data.get('mode')!=spec['mode']or data.get('executed')is not True or data.get('binary_sha256')!=build['binary_sha256']or data.get('source_head')!=build['source_head']or data.get('build_sha256')!=sha(a.build_receipt):raise RuntimeError('Actualleafstatus/source/binary mismatch')
   row['assertions']=assertions(data);row['archived_inputs']=verify_archive(data)
   if any(sha(n)!=h for n,h in data['hash_only_inputs'].items()):raise RuntimeError('Leaf binary/provider changed')
   bundle=validate_empty_bundle(leaf/'fixture/manifest.json')
   if bundle['run_id']!=child_id:raise RuntimeError('Fixture childidentity differs')
   item=bundle['entries']['same'];empty,_=validate_empty_manifest(item['manifest']);root=Path(empty['data_path']).resolve()
   if any(root==old or root.is_relative_to(old)or old.is_relative_to(root)for old in roots):raise RuntimeError('Fixture roots reused/overlapping')
   roots.append(root);row['fixture_identity']=dict(run_id=child_id,data_root=str(root),manifest_sha256=sha(leaf/'fixture/manifest.json'),empty_sha256=sha(item['manifest']))
   owner_path=leaf/'database-owner.json';owner=json.loads(owner_path.read_text())
   if owner.get('runtime_data')!=initial['data']or owner.get('owner_oid')!=initial['administrator_oid']:raise RuntimeError('Ownedallocation administrator/runtime differs')
   adapted=dict(database_cleanups=[dict(attempt=str(leaf),owner=dict(path=str(owner_path),sha256=sha(owner_path),receipt=owner),cleanup=data['database_cleanup'])],role_cleanup=data['role_cleanup'])
   row['cleanup']=verify_cleanup(adapted,child_id,leaf,initial['postmaster_start']);verify();quiet();shutil.copy2(leaf/'result.json',out/(spec['mode']+'-result.json'));row.update(status='pass',preserve_owned_scene=False);dump(out/'index.json',result)
  result.update(status='pass',independent_fixture_roots=[str(x)for x in roots])
 except BaseException as exc:result.update(reason=str(exc),traceback=traceback.format_exc(),preserve_owned_scene=True)
 finally:
  if child is not None:
   try:
    if record is None and child.poll()is None:record=spawn_identity(child,nonce_digest)
    if record is not None and members(record,nonce_digest):result['exception_group_signals']=stop_owned(child,record,nonce_digest)
   except BaseException as exc:result['process_retirement_error']=str(exc)
  if control is not None:control.close()
  for f in reversed(locks):f.close()
  dump(out/'index.json',result)
 return 0 if result['status']=='pass'else 1

def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--execute',action='store_true');p.add_argument('--run-id');p.add_argument('--build-receipt',type=Path);p.add_argument('--source-contract',type=Path);p.add_argument('--sdk-index',type=Path);p.add_argument('--sdk-index-sha256');p.add_argument('--modes',nargs='+',choices=MODES,default=MODES);a=p.parse_args();prepared=plan(a.modes)
 if not a.execute:print(json.dumps(prepared,ensure_ascii=False,indent=2));return 0
 if not all([a.run_id,a.build_receipt,a.source_contract,a.sdk_index,a.sdk_index_sha256]):p.error('Execute requires explicitrunid/futureSQL22build/finalsourcecontract/newSDKindex+SHA')
 return execute(a,prepared)
if __name__=='__main__':raise SystemExit(main())
