#!/usr/bin/env python3
"""Serial IX5 qualification candidate; default only prepares a plan.

Never builds, starts/restarts services, edits configuration or repairs cleanup.
--execute requires a future matching official receipt and already configured
owned runtime. One child/fixture runs at a time. A failed/timeout case stops;
only its own Python process group may be signalled, and its DB/roles survive.
"""
import argparse
import ast
from copy import deepcopy
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys
import time
import traceback
ROOT=Path(os.getenv('CPP_WORKSPACE','/home/qiumc/data/code/cpp')).resolve()
DRIVER=ROOT/'integration/opengauss-native-lake'
HERE=Path(__file__).resolve().parent
CHILD=HERE/'native_index_maintenance.py'
METHODS=['btree','bitmap','diskann']
MODES=['build','repair','cancel','fault-after-prepare','worker-restart','drop','rename','cleanup-blocked']
SCOPES={
 'build':'real durable BUILDING reservation, source pin, no long host/catalog lock during IO, late READY and actual verified query coverage',
 'repair':'corrupt an exact owned artifact; old segment retired and new source-bound READY segment; no business snapshot',
 'cancel':'cancel exact native worker at post-prepare gate; original57014 and unpublished pin terminal cleanup; not deep SDK/graph cancel',
 'fault-after-prepare':'real prepare then private phase fault; originalXX000, FAILED cleanup, next worker can publish',
 'worker-restart':'only worker proc_exit after durable prepare; retained BUILDING/source pin then recovery; not postmaster/WAL crash',
 'drop':'drop definition at late publication gate; no resurrection or active coverage',
 'rename':'rename business table at prepare gate; stable identity and original values still query correctly',
 'cleanup-blocked':'owned registration SHARE blocks actual cleanup SRE; bounded failure retains pin/originalXX000 then recovers; does not isolate5s alarm from1s lockwait'}

def sha(path):
 h=hashlib.sha256()
 with Path(path).open('rb')as f:
  for block in iter(lambda:f.read(1024*1024),b''):h.update(block)
 return h.hexdigest()

def dump(path,value):
 Path(path).write_text(json.dumps(value,ensure_ascii=False,indent=2)+'\n')

def prepare(profile,methods,modes,budget,sql_version='0.18'):
 source=CHILD.read_text();tree=ast.parse(source);choices={}
 for node in ast.walk(tree):
  if isinstance(node,ast.Call)and isinstance(node.func,ast.Attribute)and node.func.attr=='add_argument'and node.args and isinstance(node.args[0],ast.Constant):
   for kw in node.keywords:
    if kw.arg=='choices':choices[node.args[0].value]=ast.literal_eval(kw.value)
 if choices.get('--sql-version')!=['0.18','0.19','0.20','0.21']:raise RuntimeError('Leaf SQL version choices changed; review required')
 if sql_version not in ['0.18','0.19','0.20','0.21']:raise RuntimeError('IX5 installed SQL must be18/19/20/21')
 if choices.get('--method')!=METHODS or choices.get('--mode')!=MODES:raise RuntimeError('Leaf matrix changed; independent review required')
 if profile=='debug'and'diskann'in methods:raise RuntimeError('diskann requires ann-debug; choose ann-debug for all24 or Debug for scalar16 only')
 cases=[]
 for method in methods:
  for mode in modes:
   cases.append(dict(id=method+'-'+mode,method=method,mode=mode,profile=profile,script=str(CHILD),script_sha256=sha(CHILD),
      args=['--method',method,'--mode',mode,'--budget-seconds',str(budget),'--sql-version',sql_version],
      hard_seconds=300+6*budget+180,scope=SCOPES[mode]))
 return dict(status='prepared_not_executed',matrix_count=len(cases),full_matrix_count=len(METHODS)*len(MODES),strict_serial=True,
    cases=cases,profile=profile,installed_sql=sql_version,reviewed_runtime_defaults=['0.18','0.19','0.20','0.21'],source_sha256=sha(CHILD),controller_sha256=sha(__file__),
    dynamic_requirements=['successful exact official profile/source/binary receipt','reviewed source18..21 and explicitly selected retainedSQL18..21; source control, actual leaf module and installed SQL recorded separately; real provider and reference receipts',
      'already-on enable_lake_maintenance and enable_lake_index_maintenance; no script configuration writes',
      'lake_index_build_budget>=600000ms, lake_flush_max_delay=3600000ms, lake_flush_min_rows>35, enable_thread_pool=off',
      'matching private CRoaring receipt for bitmap; native diskann AM for ann-debug; actual CREATE/build/query still decides capability',
      'no retained lake_inline owned databases or unrelated foreground sessions; root serializes all fixture groups'],
    boundaries=['scalar:int4,heap carrier,32physical rows,one immutable source; not Ustore/all scalar types/scale',
      'DiskANN:L2,vector(3),32nodes,enable_pq=false; not PQ,global exact ANN order or recall qualification',
      'worker-restart is not postmaster crash/restart; cancellation is post-prepare gate',
      'cleanup-blocked validates a bounded cleanup under real lock, not exclusive5s alarm mechanism',
      'no RAC,remote IO,SMP background build,performance,soak or commercial qualification'],
    timeout_policy='per child: fixture300s +6*bounded phase waits +180s cleanup margin; timeout terminates only child process group and stops; exact owned scenes retained, no controller cleanup/restart')

def validate_runtime_sql_pair(runtime,installed):
 if (runtime,installed)not in [(r,s) for r in ['0.18','0.19','0.20','0.21'] for s in ['0.18','0.19','0.20','0.21'] if int(r.split('.')[1])>=int(s.split('.')[1])]:
  raise RuntimeError('Unreviewed IX5 source runtime/installed SQL pair: '+repr((runtime,installed)))
 return dict(runtime_default=runtime,installed_sql=installed)

def safe_relative(name):
 p=Path(name)
 if p.is_absolute()or'..'in p.parts:raise RuntimeError('Unsafe source relative path '+name)
 return p

def checked_assertions(data):
    assertions=data.get('assertions',[])
    if not assertions or any(x.get('status')!='pass' or 'observed'not in x or 'expected'not in x or x['observed']!=x['expected']for x in assertions):
        raise RuntimeError('Leaf assertion records absent, failed or mutated')
    return len(assertions)


def cleanup_receipt(leaf,data,run_id,profile,out,identity):
 owner_path=leaf/'database-owner.json';owner=json.loads(owner_path.read_text())
 if owner.get('phase')!='owned'or owner.get('run_id')!=run_id or owner.get('runtime_profile')!=profile or Path(owner.get('attempt','')).resolve()!=leaf:
  raise RuntimeError('Owned database allocation identity incomplete')
 if owner.get('postmaster_start')!=identity['postmaster_start']or owner.get('runtime_data')!=identity['data']or owner.get('owner_oid')!=identity['administrator_oid']:
  raise RuntimeError('Cleanup allocation runtime/administrator identity differs')
 if not re.fullmatch(r'lake_inline_[0-9]{14}_[0-9a-f]{8}',owner.get('database','')):raise RuntimeError('Malformed owned database name')
 if not isinstance(owner.get('database_oid'),int)or not owner.get('marker','').startswith('og_lake_inline_attempt:'+run_id+':'):
  raise RuntimeError('Owned OID/marker missing')
 cleanup=data.get('database_cleanup',{})
 if cleanup.get('returncode')!=0:raise RuntimeError('Database cleanup failed/unproven')
 summary=json.loads(cleanup['stdout']);immutable=Path(summary.get('immutable_report','')).resolve()
 if immutable.parent!=leaf or not immutable.is_file():raise RuntimeError('Cleanup report not exact leaf artifact')
 report=json.loads(immutable.read_text())
 if report!=summary or report.get('status')!='pass'or report.get('terminated_backends')!=[]:raise RuntimeError('Immutable cleanup report mismatch/termination')
 if report.get('run_id')!=run_id or Path(report.get('attempt','')).resolve()!=leaf or report.get('dropped_database_oid')!=owner['database_oid']:
  raise RuntimeError('Cleanup did not remove exact owned database OID')
 roles=data.get('role_cleanup',{});role_path=leaf/'permission-roles.json';role=json.loads(role_path.read_text())
 if roles.get('status')!='pass'or role.get('run_id')!=run_id or role.get('postmaster_start')!=owner.get('postmaster_start'):
  raise RuntimeError('Role cleanup identity/status mismatch')
 names={item['name']for item in role.get('roles',[])}
 if role.get('administrator_oid')!=identity['administrator_oid']or not names or set(roles.get('removed',[]))!=names or any(item['phase']!='dropped'or not item.get('marker','').startswith('og_lake_role_attempt:'+run_id+':')or not isinstance(item.get('oid'),int)for item in role['roles']):
  raise RuntimeError('Role cleanup not exactly the allocated roles')
 archived=out/'case-artifacts'/run_id;archived.mkdir(parents=True)
 for path in [owner_path,immutable,role_path]:shutil.copy2(path,archived/path.name)
 return dict(verified=True,database_oid=owner['database_oid'],marker=owner['marker'],owner_receipt_sha256=sha(owner_path),
   cleanup_report=str(immutable),cleanup_report_sha256=sha(immutable),roles_receipt_sha256=sha(role_path))

def execute(args,plan):
 # No runtime imports or SQL occur in default prepare mode.
 os.environ['OG_LAKE_TEST_PROFILE']=args.profile
 sys.path[:0]=[str(DRIVER),str(DRIVER/'tests')]
 from file_dml_acceptance import check_build
 from inline_fixture import query,scalar
 from native_auto_flush import connect_control
 from native_runtime import PROFILE,RUNTIME,SOURCE,INSTALL
 from lake_harness import validate_run_id
 validate_run_id(args.run_id)
 if len(args.run_id)>64 or any(len(args.run_id)+1+len(spec['id'])+len('-business')>100 for spec in plan['cases']):
  raise ValueError('Campaign run ID exceeds selected fixture suffix budget (max64 and all descendants max100)')
 out=ROOT/'公共文档/openGauss原生湖存储/验证'/args.run_id;out.mkdir(exist_ok=False)
 result=dict(status='fail',scope=__doc__,plan=deepcopy(plan),strict_serial=True,cases=[],runtime_executed=False)
 locks=[];control=None
 try:
  # These are the existing same-profile campaign locks. Do NOT hold the
  # reference driver lock: each leaf's real fixture needs it itself.
  for name in ['.ix5-qualification.lock','.n7-qualification.lock','.row-head-reclaim-campaign.lock']:
   lock=(RUNTIME/name).open('a+');fcntl.flock(lock.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB);locks.append(lock)
  build=check_build(args.build_receipt)
  if PROFILE!=args.profile:raise RuntimeError('Runtime import profile mismatch')
  required={'contrib/og_lake/lake_index_maintenance.cpp','contrib/og_lake/lake_maintenance.cpp','contrib/og_lake/lake_file_scalar.cpp'}
  if not required<=set(build['native_source_inputs_after']):raise RuntimeError('Official receipt lacks IX5 modules')
  if 'bitmap'in args.methods and not build.get('roaring_inputs_after'):raise RuntimeError('Bitmap dependency not in official receipt')
  for key in ['roaring_inputs','pq_inputs']:
   if build.get(key+'_before')!=build.get(key+'_after'):raise RuntimeError('Dependency changed during build: '+key)
  control=connect_control()
  query(control,'SET session_timeout=0');query(control,'SET statement_timeout=30000')
  def runtime_identity():
   return dict(postmaster_start=str(scalar(control,'SELECT pg_postmaster_start_time()')),data=str(Path(scalar(control,'SHOW data_directory')).resolve()),
      binary_sha256=sha(INSTALL/'bin/gaussdb'),administrator_oid=scalar(control,'SELECT oid FROM pg_roles WHERE rolname=current_user'),pid=int((RUNTIME/'data/postmaster.pid').read_text().splitlines()[0]))
  identity=runtime_identity()
  if identity['data']!=str(RUNTIME/'data'):raise RuntimeError('Wrong runtime data directory')
  for name,wanted in [('enable_lake_maintenance','on'),('enable_lake_index_maintenance','on'),('enable_thread_pool','off')]:
   if scalar(control,'SHOW '+name)!=wanted:raise RuntimeError('Explicit runtime prerequisite missing: '+name)
  settings=dict(query(control,"SELECT name,setting FROM pg_settings WHERE name IN ('lake_index_build_budget','lake_flush_max_delay','lake_flush_min_rows','lake_index_maintenance_naptime','lake_flush_naptime')"))
  if int(settings['lake_index_build_budget'])<600000 or int(settings['lake_flush_max_delay'])!=3600000 or int(settings['lake_flush_min_rows'])<=35:raise RuntimeError('Index/flush scheduling prerequisites differ')
  if 'diskann'in args.methods and not scalar(control,"SELECT EXISTS(SELECT 1 FROM pg_am WHERE amname='diskann')"):raise RuntimeError('Native diskann AM unavailable')
  versions=re.findall(r"^default_version\s*=\s*'([^']+)'",(SOURCE/'contrib/og_lake/og_lake.control').read_text(),re.M)
  if len(versions)!=1:raise RuntimeError('Ambiguous source default version')
  validate_runtime_sql_pair(versions[0],args.sql_version)
  result['runtime_prerequisites']=dict(identity=identity,settings=settings,reviewed_default_version=versions[0],selected_installed_sql=args.sql_version)
  def quiet():
   retained=query(control,"SELECT oid,datname FROM pg_database WHERE datname LIKE 'lake_inline_%%' ORDER BY oid")
   foreground=query(control,'SELECT pid,datname,application_name FROM pg_stat_activity WHERE client_port IS NOT NULL AND pid<>pg_backend_pid()')
   if retained or foreground:raise RuntimeError('Other/retained fixture or foreground session; no cleanup attempted: '+repr((retained,foreground)))
  quiet()
  frozen=out/'frozen';frozen.mkdir();inputs={}
  def add(source,destination,expected=None):
   source=Path(source).resolve()
   if str(source)in inputs:return
   if not source.is_file():raise FileNotFoundError(source)
   digest=sha(source)
   if expected is not None and digest!=expected:raise RuntimeError('Frozen declared hash mismatch: '+str(source))
   target=frozen/destination;target.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(source,target)
   if sha(target)!=digest:raise RuntimeError('Archive copy mismatch: '+str(source))
   inputs[str(source)]=dict(sha256=digest,archive=str(target.relative_to(out)))
  add(args.build_receipt,'official-build.json')
  for name,expected in build['native_source_inputs_after'].items():add(SOURCE/safe_relative(name),Path('native-source')/safe_relative(name),expected)
  for path in sorted(DRIVER.rglob('*')):
   if path.is_file()and'__pycache__'not in path.parts and'state'not in path.relative_to(DRIVER).parts and(path.suffix in {'.py','.sh','.sql','.cpp','.h','.hpp','.control','.inc'}or path.name=='source-lock.json'):
    add(path,Path('integration')/path.relative_to(DRIVER))
  for path in [Path(__file__),CHILD]:add(path,Path('qualification')/path.name)
  add(INSTALL/'bin/gaussdb','binary/gaussdb',build['binary_sha256'])
  add(INSTALL/'lib/postgresql/og_lake_sdk/libog_lake_sdk_arrow.so','binary/libog_lake_sdk_arrow.so')
  for name in ['og_lake.control','og_lake--'+args.sql_version+'.sql']:
   add(INSTALL/'share/postgresql/extension'/name,Path('installed-sql')/name,sha(SOURCE/'contrib/og_lake'/name))
  reference=ROOT/'.reference/opengauss-native-lake/build/receipt.json';rb=json.loads(reference.read_text());add(reference,'reference/build.json')
  if rb['source_lock_hash']!=sha(DRIVER/'source-lock.json'):raise RuntimeError('Genuine reference/source-lock mismatch')
  add(Path(rb['binary']),'reference/duckdb',rb['binary_sha256'])
  for key in ['roaring_inputs_after','pq_inputs_after']:
   dep=build.get(key)
   if not dep:continue
   add(dep['receipt'],Path('dependencies')/key/'build.json',dep['receipt_sha256'])
   for name,expected in dep.get('files',{}).items():add(Path(dep['prefix'])/safe_relative(name),Path('dependencies')/key/safe_relative(name),expected)
  manifest=out/'frozen-inputs.json';dump(manifest,dict(files=inputs,identity=identity))
  def verify():
   check_build(args.build_receipt)
   if runtime_identity()!=identity:raise RuntimeError('Runtime restarted or changed during campaign')
   for source,item in inputs.items():
    if sha(source)!=item['sha256']or sha(out/item['archive'])!=item['sha256']:raise RuntimeError('Frozen source/archive changed: '+source)
   return dict(verified=True,inputs=len(inputs),binary_sha256=identity['binary_sha256'],manifest_sha256=sha(manifest),postmaster_start=identity['postmaster_start'])
  result.update(shared_inputs=str(manifest),shared_inputs_sha256=sha(manifest),source_head=build['source_head'],binary_sha256=build['binary_sha256'])
  for spec in plan['cases']:
   before=verify();quiet();child_id=args.run_id+'_'+spec['id'].replace('-','_');validate_run_id(child_id)
   leaf=ROOT/'公共文档/openGauss原生湖存储/验证'/child_id
   if leaf.exists():raise FileExistsError('Prior leaf evidence exists: '+str(leaf))
   command=[sys.executable,str(CHILD),'--run-id',child_id,'--build-receipt',str(args.build_receipt.resolve()),*spec['args']]
   env=dict(os.environ);env['PYTHONDONTWRITEBYTECODE']='1';env['CPP_WORKSPACE']=str(ROOT);env['OG_LAKE_TEST_PROFILE']=args.profile
   log=out/(spec['id']+'.stdout');started=time.monotonic();timed=False;signals=[]
   result['runtime_executed']=True
   with log.open('wb')as stream:
    child=subprocess.Popen(command,stdout=stream,stderr=subprocess.STDOUT,env=env,start_new_session=True)
    try:child.wait(timeout=spec['hard_seconds'])
    except(subprocess.TimeoutExpired,KeyboardInterrupt):
     timed=True
     for sig,grace in [(signal.SIGTERM,5),(signal.SIGKILL,10)]:
      if child.poll()is not None:break
      try:os.killpg(child.pid,sig);signals.append(int(sig))
      except ProcessLookupError:break
      try:child.wait(timeout=grace)
      except subprocess.TimeoutExpired:pass
   row=dict(id=spec['id'],method=spec['method'],mode=spec['mode'],command=command,pid=child.pid,returncode=child.returncode,
      hard_timeout=timed,signals_to_owned_process_group=signals,seconds=round(time.monotonic()-started,3),status='fail',
      stdout=str(log),stdout_sha256=sha(log),source_binary_before=before,result=str(leaf/'result.json'),controller_cleanup_attempted=False,preserve_owned_scene=True)
   result['cases'].append(row);dump(out/'index.json',result)
   if timed:row['preserve_owned_scene']=True;raise RuntimeError('Owned child process group timed out; DB/roles retained; stop before next fixture')
   data=json.loads((leaf/'result.json').read_text());row['result_sha256']=sha(leaf/'result.json');shutil.copy2(leaf/'result.json',out/(spec['id']+'-result.json'))
   row['source_binary_after']=verify();row['assertions']=checked_assertions(data)
   if child.returncode!=0 or data.get('status')!='pass':
    raise RuntimeError('Leaf failed or assertion record invalid: '+spec['id'])
   if data.get('runtime_default_version')!=versions[0]or data.get('runtime_module_version')!=versions[0]or data.get('installed_sql')!=args.sql_version:raise RuntimeError('Leaf actual runtime/installed SQL identity differs from selected pair')
   if data.get('source_head')!=build['source_head']or data.get('binary_sha256')!=build['binary_sha256']:raise RuntimeError('Leaf source/binary identity differs')
   if data.get('method')!=spec['method']or data.get('mode')!=spec['mode']or not data.get('gates')or not data.get('target'):raise RuntimeError('Leaf method/mode/actual gate proof missing')
   if not data.get('inputs'):raise RuntimeError('Leaf lacks executed input map')
   for source,expected in data['inputs'].items():
    path=leaf/'executed-inputs'/Path(source).name
    if not path.is_file()or sha(path)!=expected:raise RuntimeError('Leaf executed input archive mismatch: '+str(path))
   row['cleanup']=cleanup_receipt(leaf,data,child_id,args.profile,out,identity);quiet()
   dump(leaf/'campaign-frozen-reference.json',dict(campaign=str(out/'index.json'),shared_inputs=str(manifest),shared_inputs_sha256=sha(manifest),before=before,after=row['source_binary_after']))
   row['status']='pass';row['preserve_owned_scene']=False;dump(out/'index.json',result)
  result.update(status='pass',completed_scope='Only exact selected method/modes; all matrix limitations retained')
 except BaseException as exc:result.update(reason=str(exc),traceback=traceback.format_exc())
 finally:
  if control is not None:control.close()
  for lock in reversed(locks):lock.close()
  dump(out/'index.json',result)
 print(json.dumps(dict(status=result['status'],cases=len(result['cases']),reason=result.get('reason'),index=str(out/'index.json'))))
 return 0 if result['status']=='pass' else 1

def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--execute',action='store_true');p.add_argument('--run-id');p.add_argument('--build-receipt',type=Path)
 p.add_argument('--sql-version',choices=['0.18','0.19','0.20','0.21'],default='0.18')
 p.add_argument('--profile',choices=['debug','ann-debug'],default='ann-debug');p.add_argument('--methods',nargs='+',choices=METHODS,default=METHODS);p.add_argument('--modes',nargs='+',choices=MODES,default=MODES);p.add_argument('--budget-seconds',type=int,default=900)
 args=p.parse_args()
 if len(set(args.methods))!=len(args.methods)or len(set(args.modes))!=len(args.modes):p.error('Duplicate method/mode not admitted')
 if not 60<=args.budget_seconds<=1800:p.error('Leaf phase budget60..1800 seconds required')
 plan=prepare(args.profile,args.methods,args.modes,args.budget_seconds,args.sql_version)
 if not args.execute:print(json.dumps(plan,ensure_ascii=False,indent=2));return 0
 if not args.run_id or not args.build_receipt:p.error('--execute requires run-id and official receipt')
 return execute(args,plan)

if __name__=='__main__':raise SystemExit(main())
