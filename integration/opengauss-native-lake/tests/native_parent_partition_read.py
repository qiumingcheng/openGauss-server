#!/usr/bin/env python3
"""Prepared real native parent/pg_partition certified read/bounded INSERT qualification.

Default prepare uses stdlib only, no Native/SDK/fixture/SQL. Execution needs a
root-reviewed newly coordinated SQL capability and exact official build+SDK
receipt. This is the bounded certified read/positive-inline INSERT slice, not native
UPDATE/DELETE/COPY, partition CREATE/ALTER, indexes/SMP/Vec or full PGL3.
"""
import argparse,ast,fcntl,hashlib,json,os,re,secrets,shutil,signal,subprocess,sys,time,traceback
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
ROOT=Path(os.getenv('CPP_WORKSPACE','/home/qiumc/data/code/cpp')).resolve()
D=ROOT/'integration/opengauss-native-lake'
MODES=('basic','certify','readonly','snapshot','lifetime','permissions','fusion','insert','insert-rollback','two-writers','insert-cursor','upgrade','utility-guards')
CANDIDATE='5a8c716aa0f312a58af65575b81125c086629cedd3a6ecaadfb045f00a1a9d24'
FUSION_GUARD='317315450745b3bcc09f7dd3aeb70a934aa18f9ae91a3d55c6ddd2f78d4af4f7'
INSERT_CANDIDATE='28b01166cb4f4f4373b688d705e57d7a7a221d58dda9c4010ea773b2c233f013'
CURSOR_OWNER_FIX='238abc923d79ecb30972d32986fa357aee55189c85a6dc3b5598bc35cb6ec171'


def digest(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def image(v):
 if isinstance(v,dict):return {str(k):image(x)for k,x in v.items()}
 if isinstance(v,(list,tuple)):return [image(x)for x in v]
 if isinstance(v,bytes):return dict(bytes_hex=v.hex())
 if isinstance(v,Path):return str(v)
 return v

def save(p,v):p.write_text(json.dumps(image(v),ensure_ascii=False,indent=2)+'\n')


# Reused process ownership helpers; only names and nonce prefix differ.
def _parent_require(ok,reason):
    if not ok:raise RuntimeError(reason)

def _parent_members(record,nonce_digest,proc=Path('/proc')):
    """Prove all living _parent_members before group signals; never persist the nonce.

    A dead/reaped leader does not prove its children drained. Unknown nonce,
    UID or reused leader identity refuses signals and preserves the scene.
    """
    found=[]
    for path in proc.iterdir():
        if not path.name.isdigit():continue
        try:
            stat=(path/'stat').read_text();tail=stat[stat.rfind(')')+2:].split()
            if int(tail[2])!=record['pgid']or int(tail[3])!=record['sid']or tail[0]=='Z':continue
            _parent_require(path.stat().st_uid==record['uid'],'Owned Python group UID changed; refuse signal')
            environment=(path/'environ').read_bytes().split(b'\0')
            nonce=next((x.split(b'=',1)[1]for x in environment if x.startswith(b'OG_LAKE_PARENT_TEST_NONCE=')),None)
            _parent_require(nonce is not None and hashlib.sha256(nonce).hexdigest()==nonce_digest,'Unknown group member nonce; refuse signal')
            ticks=int(tail[19])
            _parent_require(int(path.name)!=record['pid']or ticks==record['start_ticks'],'Leader PID reused; refuse signal')
            found.append(dict(pid=int(path.name),start_ticks=ticks))
        except FileNotFoundError:continue
    return sorted(found,key=lambda item:item['pid'])

def _parent_spawn_identity(child,nonce_digest):
    path=Path('/proc')/str(child.pid);stat=(path/'stat').read_text();tail=stat[stat.rfind(')')+2:].split()
    record=dict(pid=child.pid,pgid=int(tail[2]),sid=int(tail[3]),start_ticks=int(tail[19]),uid=path.stat().st_uid,nonce_sha256=nonce_digest)
    _parent_require(record['pgid']==child.pid and record['sid']==child.pid and record['uid']==os.getuid(),'New Python session ownership differs')
    record['initial_members']=_parent_members(record,nonce_digest);return record

def _parent_stop_owned(child,record,nonce_digest,evidence,progress=lambda:None):
    """At most5+10s grace; signals _parent_require a complete current membership proof."""
    evidence.update(signals=[],membership_checks=[],drained=False)
    for sig,seconds in [(signal.SIGTERM,5),(signal.SIGKILL,10)]:
        live=_parent_members(record,nonce_digest);evidence['membership_checks'].append(dict(before_signal=int(sig),members=live));progress()
        if not live:break
        try:os.killpg(record['pgid'],sig);evidence['signals'].append(int(sig));progress()
        except ProcessLookupError:break
        until=time.monotonic()+seconds
        while time.monotonic()<until:
            child.poll()
            if not _parent_members(record,nonce_digest):break
            time.sleep(.05)
    remaining=_parent_members(record,nonce_digest);evidence.update(remaining=remaining,drained=not remaining);progress()
    _parent_require(not remaining,'Owned Python group did not drain within5+10s; preserve scene')
    _parent_require(child.poll()is not None,'Leader is still alive outside its proved group; preserve scene')
    return evidence


def _parent_supervisor_save(path,value):
 path=Path(path);temporary=path.with_name(path.name+'.new')
 with temporary.open('w')as stream:
  json.dump(image(value),stream,ensure_ascii=False,indent=2);stream.write('\n');stream.flush();os.fsync(stream.fileno())
 os.replace(temporary,path)


def supervise(a):
 out=ROOT/'公共文档/openGauss原生湖存储/验证'/a.run_id
 if out.exists():raise FileExistsError('Refusing to overwrite original evidence: '+str(out))
 # A sibling journal records PID immediately without precreating the child's
 # exclusive attempt directory. No plaintext nonce enters files or argv.
 journal=out.with_name(out.name+'_supervisor');journal.mkdir(parents=True,exist_ok=False)
 nonce=secrets.token_hex(24);nonce_digest=hashlib.sha256(nonce.encode()).hexdigest()
 env=dict(os.environ,OG_LAKE_PARENT_TEST_PARENT=str(os.getpid()),OG_LAKE_PARENT_TEST_NONCE=nonce)
 command=[sys.executable,str(Path(__file__).resolve()),*sys.argv[1:],'--_child']
 start=time.monotonic();process=record=None;reason=None;interrupted=False;retirement_attempted=False;ownership_refused=False
 evidence=dict(command=command,hard_deadline_seconds=a.case_timeout,owner_nonce_sha256=nonce_digest,
  helper_source_sha256='f07c91e88cd7576e26951ab267f90a38c6f5c71a10f81d48274da3fdc3c0e7ed',
  no_service_control=True,no_automatic_database_cleanup_after_timeout=True)
 def progress():_parent_supervisor_save(journal/'controller.json',evidence)
 progress()
 with(journal/'child.stdout').open('wb')as log:
  try:
   process=subprocess.Popen(command,stdout=log,stderr=subprocess.STDOUT,start_new_session=True,env=env)
   evidence.update(child_pid=process.pid,started_monotonic=start);progress()
   try:record=_parent_spawn_identity(process,nonce_digest)
   except BaseException:
    ownership_refused=True;raise
   evidence['process_identity']=record;progress()
   process.wait(timeout=max(0,a.case_timeout-(time.monotonic()-start)))
   try:remaining=_parent_members(record,nonce_digest)
   except BaseException:
    ownership_refused=True;raise
   evidence['remaining_after_leader_exit']=remaining
   if remaining:raise RuntimeError('Leader exited while owned helpers remain; preserve scene')
  except(subprocess.TimeoutExpired,KeyboardInterrupt)as error:
   interrupted=isinstance(error,KeyboardInterrupt)
   reason='controller interrupted'if interrupted else'owned case hard deadline exceeded'
  except BaseException as error:
   reason=str(error);evidence['supervisor_error']=traceback.format_exc()
  finally:
   # At most one retirement attempt. A failed ownership proof must never be
   # retried into a signal; unknown members keep their original scene intact.
   if process is not None and record is not None and not ownership_refused:
    retirement_attempted=True;evidence['retirement']={}
    try:_parent_stop_owned(process,record,nonce_digest,evidence['retirement'],progress)
    except BaseException as error:
     reason=reason or 'owned process group could not be proved drained'
     evidence['retirement_error']=str(error)
   elif process is not None and reason is None:
    reason='process identity unavailable; preserve scene without signal'
   evidence.update(returncode=process.poll()if process is not None else None,elapsed_seconds=round(time.monotonic()-start,3),
    ownership_refused=ownership_refused,retirement_attempted=retirement_attempted)
   evidence['owned_process_group_sigkill_sent']=int(signal.SIGKILL)in evidence.get('retirement',{}).get('signals',[])
   progress()
 # Do not overwrite a result while an unproved/undrained child may still own
 # it. Only this sibling controller journal is authoritative for such failure.
 drained=(bool(evidence.get('retirement',{}).get('drained'))and
          'retirement_error'not in evidence and evidence.get('returncode')is not None)
 if ownership_refused or not drained:
  evidence.update(status='fail',preserve_owned_scene=True,reason=reason or 'owned group drain unproven');progress()
  print(json.dumps(dict(status='fail',controller=str(journal/'controller.json'),result=str(out/'result.json'),reason=evidence['reason'])))
  return 130 if interrupted else 1
 out.mkdir(parents=True,exist_ok=True);shutil.copyfile(journal/'child.stdout',out/'child.stdout')
 result=out/'result.json'
 if result.exists():shutil.copy2(result,out/'original-child-result.json')
 try:r=json.loads(result.read_text())
 except(OSError,ValueError):r=dict(status='fail',assertions=[],reason='missing/incomplete child result')
 if reason or evidence['returncode']!=0:
  r.update(status='fail',preserve_owned_scene=True)
  if reason:r['reason']=reason
 evidence['status']=r['status'];progress();r['supervision']=evidence;save(out/'controller.json',evidence);save(result,r)
 print(json.dumps(dict(status=r['status'],result=str(result),checks=len(r.get('assertions',[])),reason=r.get('reason'))))
 return 130 if interrupted else 0 if r['status']=='pass'else 1


def main():
 p=argparse.ArgumentParser(description=__doc__)
 p.add_argument('--mode',choices=MODES,required=True);p.add_argument('--execute',action='store_true')
 p.add_argument('--run-id');p.add_argument('--sql-version');p.add_argument('--build-receipt',type=Path)
 p.add_argument('--sdk-qualification',type=Path);p.add_argument('--sdk-index-sha256');p.add_argument('--source-contract',type=Path)
 p.add_argument('--case-timeout',type=int,default=900);p.add_argument('--_child',action='store_true',help=argparse.SUPPRESS)
 a=p.parse_args()
 if not a.execute:
  print(json.dumps(dict(status='prepared_not_executed',mode=a.mode,candidate_patch=CANDIDATE,Native=0,SDK=0,fixtures=0,
   version='Unassigned new root-coordinated capability; retainedSQL versions closed.',
   requirements=['Exact official source/binary/SDK and reviewed final helper+SQL contract','Root serial fixture ownership; owned child process deadline900s; no automatic restart/foreign cleanup',
    'basic: real pg_partition OIDs, original lake file+inline payload, parent heap oracle, 2/1/0 pruning, native iterator and original SQL quals',
    'certify: native leaf SnapshotAny including deleted tuples, out-of-range source, alias/missing leaf, exact complete source shape and transactional absence',
    'readonly: DML/COPY/ALTER/marker/extension/missingbinding reject; ordinary unmarked heap unaffected',
    'snapshot: cursor fixed Executor snapshot across catalog-head advance; fresh cached/new parent queries failclosed',
    'lifetime: bind/savepoint/whole-xact rollback, rebind, early LIMIT/CLOSE and native diagnostic snapshot baseline',
    'permissions: ordinary role parent ACL before/after grant, column ACL and administrator-only binder',
    'fusion: real enable_opfusion+partition_opfusion on, qualified native heap fusion primed before binding, cached read/DML boundary plus fresh DML denial and unchanged heap bytes',
    'insert: controlled mode promotion, two-leaf one-S INSERT/RETURNING, stable table-local ranges+birth and positive-inline/direct refusal',
    'insert-rollback: promotion+business savepoint/parent rollback, 512-row two-leaf statement resource closure',
    'two-writers: real overlapping staged transactions, sequential fresh commits and independent leaf range nonoverlap; NOT simultaneous PK collision proof',
    'insert-cursor: DECLARE in ancestor, first FETCH+child INSERT after SAVEPOINT, rollback then continue FETCH over both leaves; child-created cursor removed by rollback; zero pruning+early CLOSE resources']),indent=2));return 0
 if not all([a.run_id,a.sql_version,a.build_receipt,a.sdk_qualification,a.sdk_index_sha256,a.source_contract]):p.error('execute requires run-id, explicit SQL, official build/SDK and reviewed source-contract')
 if not re.fullmatch(r'0\.[1-9][0-9]*',a.sql_version)or int(a.sql_version.split('.')[1])<=22:
  p.error('No retained SQL19/20/21 or file-partition22 activation; root must allocate and review a separate capability')
 if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,89}',a.run_id):p.error('run-id must be a safe single component, maximum90')
 if not 300<=a.case_timeout<=1800:p.error('case-timeout must be300..1800')
 if not a._child:return supervise(a)
 if os.environ.get('OG_LAKE_PARENT_TEST_PARENT')!=str(os.getppid()):p.error('Unsupervised execute child')
 sys.path[:0]=[str(D),str(D/'tests')]
 from file_dml_acceptance import check_build
 from inline_fixture import query,scalar
 from lake_harness import validate_run_id,write_json
 from native_auto_flush import connect_control
 from native_runtime import PROFILE,SOURCE,INSTALL
 from native_test_context import OwnedDatabase
 from native_table_create import Case
 from permission_api import OwnedRoles,cleanup_roles
 from pq_sql020_build_inputs import sdk_index
 from psycopg2 import sql
 validate_run_id(a.run_id)
 if len(a.run_id)>90:p.error('run-id maximum90 preserves fixture ownership identity')
 if PROFILE!='debug':p.error('First qualification requires owned Debug runtime')
 out=ROOT/'公共文档/openGauss原生湖存储/验证'/a.run_id;out.mkdir(exist_ok=False)
 r=dict(status='fail',mode=a.mode,scope=__doc__,assertions=[],executed=False)
 t=db=control=roles=reservation=None
 def check(label,got,want=True):
  got,want=image(got),image(want);ok=got==want
  r['assertions'].append(deepcopy(dict(name=label,status='pass'if ok else'fail',observed=got,expected=want)));write_json(out/'result.json',r)
  if not ok:raise AssertionError(label)
 def configure(c):
  for statement in ['SET session_timeout=0','SET statement_timeout=60000','SET query_dop=1','SET enable_lake_smp=off',
                    'SET enable_vector_engine=off','SET enable_default_ustore_table=off']:query(c,statement)
 def reject(c,statement,state='0A000',args=None):
  try:query(c,statement,args)
  except Exception as e:check('reject '+statement[:96]+' SQLSTATE',getattr(e,'pgcode',None),state)
  else:raise AssertionError('unexpected accepted '+statement)
 def parent(name='parent'):
  query(t.a,sql.SQL('CREATE TABLE biz.{}(id integer,val integer) PARTITION BY RANGE(id) '
    '(PARTITION low VALUES LESS THAN(10),PARTITION high VALUES LESS THAN(20))').format(sql.Identifier(name)))
  oid=scalar(t.a,sql.SQL('SELECT %s::regclass::oid'),('biz.'+name,))
  z=query(t.a,"SELECT oid,relname,partitionno FROM pg_partition WHERE parentid=%s AND parttype='p' ORDER BY relname",(oid,))
  check(name+' exactly2 real pg_partition leaves',len(z),2);leaves={n:(oid,no)for oid,n,no in z}
  check(name+' leafnames real native low/high',sorted(leaves),['high','low'])
  check(name+' native leaves have no pg_class identities',scalar(t.a,'SELECT count(*) FROM pg_class WHERE oid=ANY(%s::oid[])',([x[0]for x in leaves.values()],)),0)
  check(name+' parent remains ordinary native r/p',query(t.a,'SELECT relkind,parttype FROM pg_class WHERE oid=%s',(oid,)),[('r','p')])
  return oid,[leaves['low'][0],leaves['high'][0]]
 def bind(oid,leaves,ids,c=None):
  return query(c or t.a,'SELECT og_lake.bind_native_partition_readonly(%s::regclass,%s::name,%s::oid[],%s::bigint[])',
    (oid,t.server,leaves,ids))
 def marker(oid,c=None):return scalar(c or t.observer,"SELECT COALESCE('lake_partition_root=true'=ANY(reloptions),false) FROM pg_class WHERE oid=%s",(oid,))
 def maps(oid,c=None):return query(c or t.observer,'SELECT p.leaf_partition_oid,p.partition_no,p.certified_snapshot,p.source_hash,'
   't.object_kind,t.read_only,t.table_id FROM og_lake_internal.partition_binding p JOIN og_lake_internal.table_binding t USING(binding_id) WHERE p.parent_relid=%s ORDER BY p.leaf_partition_oid',(oid,))
 def rows(c=None,predicate='true'):return query(c or t.a,'SELECT id,val FROM biz.parent WHERE '+predicate+' ORDER BY id,val NULLS FIRST')
 def oracle(predicate='true'):return query(t.observer,'SELECT id,val FROM biz.exact WHERE '+predicate+' ORDER BY id,val NULLS FIRST')
 def state():return dict(catalog=t.protocol(),maps=maps(pid),marker=marker(pid),heap=query(t.observer,'SELECT * FROM biz.heap_marker ORDER BY id'))
 def explain(label,predicate='true',iterations=2):
  statement='SELECT id,val FROM biz.parent WHERE '+predicate+' ORDER BY id,val NULLS FIRST'
  raw=scalar(t.a,'EXPLAIN (ANALYZE true,FORMAT JSON,COSTS false) '+statement)
  plan=json.loads(raw)if isinstance(raw,str)else raw;write_json(out/(label+'-plan.json'),plan)
  def walk(x):
   if isinstance(x,dict):
    yield x
    for v in x.values():yield from walk(v)
   elif isinstance(x,list):
    for v in x:yield from walk(v)
  objects=list(walk(plan));native=[x for x in objects if x.get('Native Lake Parent')=='certified readonly native RANGE leaves']
  if iterations:
   check(label+' genuine native lake ForeignScan',len(native),1)
   check(label+' native PartIterator actual pruning iterations',[x['Iterations']for x in objects if 'Iterations'in x],[iterations])
  else:
   it=[x['Iterations']for x in objects if 'Iterations'in x]
   check(label+' empty native pruning or native optimizer dummy plan',it==[0]or(not native and any(x.get('Node Type')=='Result'for x in objects)),True)
  check(label+' exact heap SQL values/multiplicity',query(t.a,statement),oracle(predicate))
 def source(name,values,flushed=False):
  t.create(name);query(t.a,'BEGIN')
  with t.a.cursor()as cur:cur.executemany(sql.SQL('INSERT INTO {} VALUES(%s,%s)').format(t.relation(name)),values)
  query(t.a,'COMMIT')
  if flushed:query(t.a,sql.SQL('SELECT og_lake.flush({}::regclass)').format(sql.Literal('biz.'+name)))
  return scalar(t.observer,'SELECT table_id FROM created_metadata.ducklake_table WHERE table_name=%s AND end_snapshot IS NULL',(name,))
 def diagnostic(c):return json.loads(scalar(c,'SELECT og_lake_internal.parent_test_xact_state()'))
 try:
  build=check_build(a.build_receipt);contract=json.loads(a.source_contract.read_text())
  check('reviewed source contract purpose',contract.get('purpose'),'native_parent_read_qualification')
  check('reviewed exact readonly candidate',contract.get('read_candidate_patch_sha256'),CANDIDATE)
  check('explicit coordinated SQL version',contract.get('partition_sql_version'),a.sql_version)
  check('reviewed exact independent fusion guard',contract.get('fusion_guard_patch_sha256'),FUSION_GUARD)
  check('reviewed exact bounded INSERT candidate',contract.get('insert_candidate_patch_sha256'),INSERT_CANDIDATE)
  if a.mode in ['insert','insert-rollback','two-writers','insert-cursor']:check('reviewed eager leaf-view lifecycle delta',contract.get('cursor_owner_patch_sha256'),CURSOR_OWNER_FIX)
  if a.mode in ['insert','insert-rollback','two-writers','insert-cursor']:check('explicit bounded INSERT capability reviewed',contract.get('bounded_insert_qualified_to_test'),True)
  check('explicit root activation review',contract.get('ready_for_native_qualification'),True);check('final activation blockers empty',contract.get('known_blockers'),[])
  if not contract.get('review_evidence')or not contract.get('native_inputs')or not contract.get('shared_helper_inputs'):raise AssertionError('missing reviewed exact source/helper/evidence contract')
  required={'contrib/og_lake/'+x for x in ['lake_native_partition.cpp','lake_native_partition.h','lake_native_partition_bind.cpp','lake_native_partition_scan.cpp','lake_catalog.cpp','lake_acl.cpp','lake_txn.cpp','og_lake.cpp','Makefile','og_lake.control']}
  required|={'src/'+x for x in ['include/access/og_lake.h','include/utils/rel.h','gausskernel/runtime/executor/nodePartIterator.cpp','gausskernel/runtime/executor/nodeForeignscan.cpp','gausskernel/optimizer/path/allpaths.cpp','gausskernel/optimizer/plan/createplan.cpp','gausskernel/optimizer/util/plancat.cpp','gausskernel/optimizer/commands/copy.cpp','gausskernel/optimizer/commands/tablecmds.cpp','gausskernel/storage/access/common/reloptions.cpp','common/backend/catalog/heap.cpp']}
  required.add('contrib/og_lake/og_lake--'+a.sql_version+'.sql')
  required|={'contrib/og_lake/'+x for x in ['lake_row_lock.cpp','lake_row_lock.h','og_lake--0.22--0.23.sql']}
  required|={'src/'+x for x in ['gausskernel/optimizer/commands/analyze.cpp','gausskernel/optimizer/commands/vacuum.cpp','gausskernel/optimizer/commands/indexcmds.cpp','gausskernel/optimizer/commands/cluster.cpp','common/backend/catalog/index.cpp','gausskernel/process/tcop/utility.cpp']}
  required|={'src/gausskernel/runtime/opfusion/opfusion.cpp','src/gausskernel/runtime/opfusion/opfusion_util.cpp'}
  if not required<=contract['native_inputs'].keys():raise AssertionError('reviewed contract omitted core/SQL admission inputs')
  for n,h in contract['native_inputs'].items():
   if Path(n).is_absolute()or '..'in Path(n).parts:raise AssertionError('unsafe source contract path')
   check('official compiled source/'+n,build['native_source_inputs_after'].get(n),h)
  if not {'native_test_context.py','tests/native_table_create.py'}<=contract['shared_helper_inputs'].keys():raise AssertionError('future shared helper admission missing')
  for n,h in contract['shared_helper_inputs'].items():
   if Path(n).is_absolute()or '..'in Path(n).parts:raise AssertionError('unsafe helper path')
   check('exact approved shared helper/'+n,digest(D/n),h)
  for n,h in contract['review_evidence'].items():check('exact review evidence/'+n,digest(n),h)
  header=(SOURCE/'contrib/og_lake/lake_native_partition.h').read_text()
  if a.mode in ['insert','insert-rollback','two-writers','insert-cursor']:check('explicit write macro exact chosen capability',re.findall(r'^#define OG_LAKE_NATIVE_PARTITION_WRITE_SQL_VERSION "([^"]*)"',header,re.M),[a.sql_version])
  check('capability macro explicitly coordinated',re.findall(r'^#define OG_LAKE_NATIVE_PARTITION_SQL_VERSION "([^"]*)"',header,re.M),[a.sql_version])
  r.update(build=str(a.build_receipt),build_sha256=digest(a.build_receipt),binary_sha256=build['binary_sha256'],source_head=build['source_head'],executed=True)
  qual=sdk_index(a.sdk_qualification,a.sdk_index_sha256,ROOT)
  sdk=INSTALL/'lib/postgresql/og_lake_sdk';owner=json.loads((sdk/'owner.json').read_text())
  check('SDK actual qualified SQL020 kind',owner['qualification_kind'],'verified_independent_sdk_sql020_int16_only')
  check('SDK qualification ownership',owner['qualification_sha256'],a.sdk_index_sha256)
  check('SDK exact owned target',str(Path(owner['target']).resolve()),str(sdk.resolve()));check('SDK project owner',owner['project'],'opengauss-native-lake-local-provider')
  check('SDK prefix no symlinks',not any(x.is_symlink()for x in sdk.rglob('*')))
  files={str(x.relative_to(sdk)):digest(x)for x in sdk.rglob('*')if x.is_file()and x.name!='owner.json'}
  check('SDK entire immutable owned tree',files,owner['files']);check('SDK provider exact receipt',files['libog_lake_sdk_arrow.so'],qual['library_sha256'])
  public={str(SOURCE/'contrib/og_lake'/n):digest(SOURCE/'contrib/og_lake'/n)for n in ['og_lake.control','sdk/lake_parquet_sdk.h','vendor/arrow20/abi.h']}
  check('SDK exact Native public headers',owner['native_public_headers'],public)
  check('Native SDK public ABI exact qualified header',public[str(SOURCE/'contrib/og_lake/sdk/lake_parquet_sdk.h')],qual['header_sha256'])
  check('SDK full installed public header',files['include/og_lake/lake_parquet_sdk.h'],qual['header_sha256'])
  inputs=[Path(__file__),a.source_contract,a.build_receipt,a.sdk_qualification,sdk/'owner.json',D/'source-lock.json',D/'reference.sh',D/'fixture.py']
  inputs +=[D/x for x in ['inline_fixture.py','native_test_context.py','native_runtime.py','lake_harness.py','pq_sql020_build_inputs.py','pq_native_build_inputs.py']]
  inputs +=[D/'tests'/x for x in ['native_table_create.py','file_dml_acceptance.py','permission_api.py','inline_cleanup.py','native_auto_flush.py','catalog_fixture_bundle.py','empty_manifest.py','empty_reference_fixture.py','empty_catalog_oracle.py','create_reference_oracle.py']]
  # Preserve local transitive helper input bytes, including child fixtures.
  pending=list(inputs);seen=set(inputs)
  while pending:
   q=pending.pop()
   if q.suffix!='.py':continue
   for node in ast.walk(ast.parse(q.read_text())):
    names=[x.name.split('.')[0]for x in node.names]if isinstance(node,ast.Import)else[node.module.split('.')[0]]if isinstance(node,ast.ImportFrom)and node.module else[]
    for n in names:
     for directory in[D,D/'tests']:
      f=directory/(n+'.py')
      if f.is_file()and f not in seen:seen.add(f);pending.append(f)
  inputs=sorted(seen,key=str)+[SOURCE/n for n in build['native_source_inputs_after']]+[Path(n)for n in build.get('build_wrapper_inputs_after',{})]
  inputs+=list((INSTALL/'share/postgresql/extension').glob('og_lake*.sql'))
  archive=out/'executed-inputs';archive.mkdir();r['inputs']={}
  for i,q in enumerate(dict.fromkeys(x.resolve()for x in inputs)):
   dest=archive/(str(i)+'-'+q.name);shutil.copy2(q,dest);check('exact executed input archive/'+str(i),digest(dest),digest(q));r['inputs'][str(q)]=dict(sha256=digest(q),archive=str(dest))
  r['hash_only_inputs']={str(x):digest(x)for x in[INSTALL/'bin/gaussdb']+[sdk/n for n in files]}
  control=connect_control();configure(control)
  check('automatic maintenance disabled',scalar(control,'SHOW enable_lake_maintenance'),'off');check('dedicated nonpool backend',scalar(control,'SHOW enable_thread_pool'),'off')
  with(out/'fixture.log').open('w')as log:
   subprocess.run([sys.executable,str(D/'tests/catalog_fixture_bundle.py'),'--run-id',a.run_id,'--output',str(out/'fixture/manifest.json')],stdout=log,stderr=subprocess.STDOUT,check=True,timeout=300)
  reservation=(ROOT/'.reference/opengauss-native-lake/.driver.lock').open('a+');fcntl.flock(reservation.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB)
  db=OwnedDatabase(a.run_id,out);db.create();configure(db.admin);roles=OwnedRoles(control,a.run_id,out)
  t=Case(db,SimpleNamespace(mode='net_dml',sql_version='0.22'if a.mode=='upgrade'else a.sql_version,manifest=out/'fixture/manifest.json',output=out/'result.json',run_id=a.run_id),r,roles)
  for c in[t.a,t.b,t.observer]:configure(c)
  check('exact runtime module',json.loads(scalar(t.a,'SELECT og_lake.host_status()'))['module_version'],contract['runtime_version'])
  query(t.a,"CREATE FUNCTION og_lake_internal.parent_test_xact_state() RETURNS text AS '$libdir/og_lake','og_lake_test_xact_state' LANGUAGE C NOT FENCED")
  if a.mode=='upgrade':
   source('upgrade_probe',[(1,100),(2,200)])
   query(t.a,'UPDATE biz.upgrade_probe SET val=101 WHERE id=1')
   baseline_protocol=t.protocol();baseline_host=t.host_image()
   def head_image(c):
    columns=query(c,"SELECT attname FROM pg_attribute WHERE attrelid='og_lake_internal.row_lock_binding'::regclass AND attnum BETWEEN 1 AND 16 AND NOT attisdropped ORDER BY attnum")
    projection=sql.SQL(',').join(sql.Identifier(x[0])for x in columns)
    return query(c,sql.SQL('SELECT {} FROM og_lake_internal.row_lock_binding ORDER BY binding_id').format(projection))
   def map_width(c):return scalar(c,"SELECT count(*) FROM pg_attribute WHERE attrelid='og_lake_internal.row_lock_binding'::regclass AND attnum>0 AND NOT attisdropped")
   baseline_heads=head_image(t.observer)
   check('upgrade starts with a populated sixteen-column row-head map',map_width(t.observer),16)
   check('upgrade has a real existing ordinary head mapping',len(baseline_heads)>0)
   head_oid=scalar(t.observer,"SELECT row_head_oid FROM og_lake_internal.row_lock_binding WHERE relation_oid='biz.upgrade_probe'::regclass")
   head_relation=scalar(t.observer,'SELECT %s::oid::regclass::text',(head_oid,))
   head_rows=query(t.observer,'SELECT row_id FROM '+head_relation+' ORDER BY row_id')
   check('upgrade has an actual acquired ordinary row head',len(head_rows)>0)
   version_sql="SELECT extversion FROM pg_extension WHERE extname='og_lake'"
   alter=sql.SQL('ALTER EXTENSION og_lake UPDATE TO {}').format(sql.Literal(a.sql_version))
   query(t.a,'BEGIN');query(t.a,'SAVEPOINT schema_upgrade');query(t.a,alter)
   check('upgrade own transaction sees target version',scalar(t.a,version_sql),a.sql_version)
   check('upgrade own transaction sees eighteen columns',map_width(t.a),18)
   check('upgrade preserves all original head map values',head_image(t.a),baseline_heads)
   check('concurrent observer retains committed old version',scalar(t.observer,version_sql),'0.22')
   query(t.a,'ROLLBACK TO schema_upgrade');query(t.a,'RELEASE schema_upgrade');query(t.a,'COMMIT')
   check('upgrade savepoint rollback restores old version and map width',(scalar(t.observer,version_sql),map_width(t.observer)),('0.22',16))
   check('upgrade rollback retains complete old host catalog',t.host_image(),baseline_host)
   check('upgrade rollback retains old head mapping and real rows',(head_image(t.observer),query(t.observer,'SELECT row_id FROM '+head_relation+' ORDER BY row_id')),(baseline_heads,head_rows))
   query(t.a,alter)
   check('committed upgrade exact version',scalar(t.observer,version_sql),a.sql_version)
   check('committed upgrade keeps old map values and identities',head_image(t.observer),baseline_heads)
   check('ordinary existing head identities acquire zero leaf domain',query(t.observer,'SELECT leaf_partition_oid,partition_no FROM og_lake_internal.row_lock_binding ORDER BY binding_id'),[(0,0)]*len(baseline_heads))
   check('catalog-only upgrade does not create lake snapshots or change data',t.protocol(),baseline_protocol)
   check('catalog-only upgrade preserves stable real head heap',query(t.observer,'SELECT row_id FROM '+head_relation+' ORDER BY row_id'),head_rows)
   query(t.a,'UPDATE biz.upgrade_probe SET val=val+1 WHERE id=1')
   check('upgraded ordinary same-row update remains usable',query(t.a,'SELECT id,val FROM biz.upgrade_probe ORDER BY id'),[(1,102),(2,200)])
   check('upgraded ordinary update reuses its original head relation',scalar(t.observer,"SELECT row_head_oid FROM og_lake_internal.row_lock_binding WHERE relation_oid='biz.upgrade_probe'::regclass"),head_oid)
  check('active SQL23 row-head map has exactly eighteen columns',scalar(t.a,"SELECT count(*) FROM pg_attribute WHERE attrelid='og_lake_internal.row_lock_binding'::regclass AND attnum>0 AND NOT attisdropped"),18)
  check('leaf-domain columns retain no temporary defaults',scalar(t.a,"SELECT count(*) FROM pg_attrdef WHERE adrelid='og_lake_internal.row_lock_binding'::regclass AND adnum IN (17,18)"),0)
  check('active SQL23 preserves twelve-column fixed-H table binding',scalar(t.a,"SELECT count(*) FROM pg_attribute WHERE attrelid='og_lake_internal.table_binding'::regclass AND attnum>0 AND NOT attisdropped"),12)
  if a.mode=='insert-rollback':
   # This fixture stages 512 rows before exercising nested rollback. Declare
   # its positive inline policy before any source or rollback baseline.
   query(t.a,"SELECT og_lake.set_option('created_lake'::name,'data_inlining_row_limit','10000')")
   check('rollback fixture exact positive global inline limit without scoped override',query(t.a,
    "SELECT value::text,scope::text,scope_id FROM created_metadata.ducklake_metadata WHERE key='data_inlining_row_limit'"),[('10000',None,None)])
  low=source('source_low',[(1,100),(2,200),(8,None)],True);high=source('source_high',[(10,1000),(19,1900)])
  query(t.a,'UPDATE biz.source_low SET val=201 WHERE id=2')
  check('backing source has real committed Parquet',scalar(t.a,'SELECT count(*) FROM created_metadata.ducklake_data_file WHERE table_id=%s AND end_snapshot IS NULL',(low,))>0)
  check('backing source retains current inline versions',scalar(t.a,'SELECT count(*) FROM created_metadata.ducklake_inlined_data_tables WHERE table_id=ANY(%s::bigint[])',([low,high],))>0)
  # Independent original INSERT/UPDATE tape, never seeded from the same lake reader.
  expected=[(1,100),(2,201),(8,None),(10,1000),(19,1900)]
  observed_source=query(t.a,'SELECT id,val FROM biz.source_low UNION ALL SELECT id,val FROM biz.source_high ORDER BY id')
  check('original committed file/inline source equals independent DML truth',observed_source,expected)
  query(t.a,'CREATE TABLE biz.exact(id integer,val integer)')
  with t.a.cursor()as c:c.executemany('INSERT INTO biz.exact VALUES(%s,%s)',expected)
  before=t.protocol();query(t.a,'DROP FOREIGN TABLE biz.source_low');query(t.a,'DROP FOREIGN TABLE biz.source_high')
  check('dropping host setup preserves complete standard catalog',t.protocol(),before)
  check('setup sources genuinely unbound',scalar(t.a,'SELECT count(*) FROM og_lake_internal.table_binding WHERE table_id=ANY(%s::bigint[])',([low,high],)),0)
  pid,leaves=parent();ids=[low,high];initial=t.head();empty=state()
  if a.mode=='fusion':
   # All switches stay ON through binding and every negative execution.
   for statement in ['SET enable_opfusion=on','SET enable_partition_opfusion=on','SET enable_opfusion_reuse=on',
     "SET plan_cache_mode='force_generic_plan'","SET opfusion_debug_mode=log","SET client_min_messages=debug4",'SET enable_seqscan=off','SET enable_bitmapscan=off']:
    query(t.a,statement)
   check('real fusion enabled',scalar(t.a,'SHOW enable_opfusion'),'on');check('real partition fusion enabled',scalar(t.a,'SHOW enable_partition_opfusion'),'on')
   query(t.a,'CREATE INDEX parent_local_id ON biz.parent(id) LOCAL')
   # Index qualification and normal local heaps are genuine before binding.
   # Failure to qualify on a host configuration is a test failure, not an
   # excuse to turn off fusion and claim its marker boundary was exercised.
   def notice_case(label,statement,expected,code=None):
    t.a.notices[:]=[]
    if code:reject(t.a,statement,code)
    else:raise AssertionError('notice_case requires an explicit source-derived rejection')
    notices=list(t.a.notices);write_json(out/(label+'-client-notices.json'),notices)
    check(label+' actual fusion selected',any(expected in x for x in notices))
   query(t.a,'PREPARE cached_parent_insert(integer,integer) AS INSERT INTO biz.parent VALUES($1,$2)')
   # Force a real fusion execution without leaving a local tuple that would
   # make the legitimate SnapshotAny binding preflight reject the parent.
   notice_case('prime-insert','EXECUTE cached_parent_insert(20,200)','Bypass executed through insert fusion','P0002')
   query(t.a,'PREPARE cached_parent_select AS SELECT id,val FROM biz.parent WHERE id=1')
   t.a.notices[:]=[];check('prime select empty native heap',query(t.a,'EXECUTE cached_parent_select'),[])
   write_json(out/'prime-select-client-notices.json',list(t.a.notices));check('prime actual select fusion',any('Bypass executed through select fusion'in x for x in t.a.notices))
   for kind in ['update','delete']:
    statement='UPDATE biz.parent SET val=val+1 WHERE id=1'if kind=='update'else'DELETE FROM biz.parent WHERE id=1'
    query(t.a,'PREPARE cached_parent_'+kind+' AS '+statement);t.a.notices[:]=[];query(t.a,'EXECUTE cached_parent_'+kind)
    write_json(out/('prime-'+kind+'-client-notices.json'),list(t.a.notices))
    check('prime actual '+kind+' fusion',any('Bypass executed through '+kind+' fusion'in x for x in t.a.notices))
   heap_before=query(t.a,'SELECT oid,pg_partition_size(%s::oid,oid) FROM pg_partition WHERE parentid=%s AND parttype=%s ORDER BY oid',(pid,pid,'p'))
  if a.mode=='certify':
   query(t.a,'INSERT INTO biz.parent VALUES(3,30)');before=t.protocol()
   reject(t.a,'SELECT og_lake.bind_native_partition_readonly(%s::regclass,%s::name,%s::oid[],%s::bigint[])','55000',(pid,t.server,leaves,ids))
   check('visible local heap binding refusal leaves no marker',marker(pid),False);check('visible local heap refusal no mapping',maps(pid),[]);check('visible local heap refusal unchanged standard',t.protocol(),before)
   # Keep a real old snapshot through an unrelated heap cursor so deleted
   # native tuples cannot be pruned away; it holds no lock on the parent.
   query(t.b,'BEGIN');query(t.b,'DECLARE retain_heap NO SCROLL CURSOR FOR SELECT id FROM biz.heap_marker')
   query(t.a,'DELETE FROM biz.parent')
   reject(t.a,'SELECT og_lake.bind_native_partition_readonly(%s::regclass,%s::name,%s::oid[],%s::bigint[])','55000',(pid,t.server,leaves,ids))
   check('SnapshotAny rejects physically retained deleted local tuple',marker(pid),False)
   query(t.b,'CLOSE retain_heap');query(t.b,'COMMIT')
   # A new empty root is needed: VACUUM is not used to erase the proof.
   pid,leaves=parent('certified_parent');query(t.a,'ALTER TABLE biz.parent RENAME TO heap_rejected');query(t.a,'ALTER TABLE biz.certified_parent RENAME TO parent')
   reject(t.a,'SELECT og_lake.bind_native_partition_readonly(%s::regclass,%s::name,%s::oid[],%s::bigint[])','22023',(pid,t.server,[leaves[0],leaves[0]],ids))
   bad=source('bad_payload',[(15,150)],False);query(t.a,'DROP FOREIGN TABLE biz.bad_payload');before=t.protocol()
   reject(t.a,'SELECT og_lake.bind_native_partition_readonly(%s::regclass,%s::name,%s::oid[],%s::bigint[])','23514',(pid,t.server,leaves,[bad,high]))
   check('out-of-range complete payload refuses mapping',maps(pid),[]);check('out-of-range complete payload marker remainsfalse',marker(pid),False);check('range refusal changes no standard data',t.protocol(),before)
   query(t.a,sql.SQL("CREATE FOREIGN TABLE biz.alias(id integer,val integer) SERVER {} OPTIONS(binding_mode 'attach',table_id {})").format(sql.Identifier(t.server),sql.Literal(str(low))))
   reject(t.a,'SELECT og_lake.bind_native_partition_readonly(%s::regclass,%s::name,%s::oid[],%s::bigint[])','42710',(pid,t.server,leaves,ids))
   check('existing ordinary source binding cannot alias native leaf',maps(pid),[])
   query(t.a,'DROP FOREIGN TABLE biz.alias')
   # Restore the valid low/high pair; source head is certified only now.
  if a.mode=='lifetime':
   query(t.a,'BEGIN');query(t.a,'SAVEPOINT bound');bind(pid,leaves,ids)
   check('own bind marks native root',marker(pid,t.a),True);check('other session cannot see uncommitted marker',marker(pid),False)
   check('own bind parent sees complete original rows',rows(),oracle());query(t.a,'ROLLBACK TO bound');query(t.a,'RELEASE bound');query(t.a,'COMMIT')
   check('savepoint rollback removes marker and all mappings',(marker(pid),maps(pid)),(False,[]))
   query(t.a,'BEGIN');query(t.a,'SAVEPOINT adopted');bind(pid,leaves,ids);query(t.a,'RELEASE adopted');query(t.a,'ROLLBACK')
   check('subcommit plus parent rollback restores empty host scaffold',(marker(pid),maps(pid)),(False,[]))
  before=t.head();bind(pid,leaves,ids);check('binding allocates no standard lake snapshot/frontier',t.head(),before)
  check('controlled marker persists',marker(pid),True);bindings=maps(pid)
  check('two complete typed native bindings',len(bindings),2);check('exact real leaf OIDs',[x[0]for x in bindings],sorted(leaves))
  check('both bindings readonly/partition',[(x[4],x[5])for x in bindings],[('partition',True)]*2)
  check('both bindings exact certified head',[x[2]for x in bindings],[before[0]]*2)
  check('source identity digest root syntax',all(re.fullmatch('[0-9a-f]{64}',x[3])for x in bindings))
  check('native parent never has ordinary relation_binding',scalar(t.a,'SELECT count(*) FROM og_lake_internal.relation_binding WHERE relation_oid=%s',(pid,)),0)
  check('no synthetic pg_class leaves after bind',scalar(t.a,'SELECT count(*) FROM pg_class WHERE oid=ANY(%s::oid[])',(leaves,)),0)
  check('native parent data vs original heap oracle',rows(),oracle())
  if a.mode in ['insert','insert-rollback','two-writers','insert-cursor']:
   def mode_flags(c=None):return query(c or t.observer,'SELECT t.read_only FROM og_lake_internal.partition_binding p JOIN og_lake_internal.table_binding t USING(binding_id) WHERE p.parent_relid=%s ORDER BY p.leaf_partition_oid',(pid,))
   def cutoffs():return query(t.observer,'SELECT t.binding_id::text,t.flush_birth_cutoff,t.flush_carrier_oid,t.flush_source_schema,t.flush_source_origin FROM og_lake_internal.partition_binding p JOIN og_lake_internal.table_binding t USING(binding_id) WHERE p.parent_relid=%s ORDER BY t.binding_id',(pid,))
   def frontiers():return {i:(n,cur,sz)for i,n,cur,sz in query(t.observer,'SELECT table_id,record_count,next_row_id,file_size_bytes FROM created_metadata.ducklake_table_stats WHERE table_id=ANY(%s::bigint[])',([low,high],))}
   def enable(c=None):query(c or t.a,'SELECT og_lake.enable_native_partition_insert(%s::regclass)',(pid,))
   def physical(tid,c=None):
    z=query(c or t.observer,'SELECT table_name FROM created_metadata.ducklake_inlined_data_tables WHERE table_id=%s',(tid,));check('one real inline source for leaf '+str(tid),len(z),1)
    return query(c or t.observer,sql.SQL('SELECT _ducklake_row_id,_ducklake_begin_snapshot,_ducklake_end_snapshot,id,val FROM created_metadata.{} ORDER BY _ducklake_row_id,_ducklake_begin_snapshot').format(sql.Identifier(z[0][0])))
   def own_oracle(c):return query(c,'SELECT id,val FROM biz.exact ORDER BY id,val NULLS FIRST')
   def insert_values(c,table,values):
    if not values:raise ValueError('nonempty real INSERT fixture required')
    shape=sql.SQL(',').join(sql.SQL('(%s,%s)')for _ in values)
    statement=sql.SQL('INSERT INTO biz.{} VALUES {}').format(sql.Identifier(table),shape)
    return query(c,statement,tuple(x for row in values for x in row))
   def heap_values(c,values):return insert_values(c,'exact',values)
   def both_insert(c,values):
    # One real multi-row statement per table, same host TX; no metadata writes.
    insert_values(c,'parent',values);insert_values(c,'exact',values)
   check('initial private leaf mode readonly',mode_flags(),[(True,),(True,)])
   oldhead=t.head();initialH=cutoffs()
   if a.mode=='insert-rollback':
    query(t.a,'BEGIN');query(t.a,'SAVEPOINT promotion');enable();check('own promotion is atomic two-leaf mode',mode_flags(t.a),[(False,),(False,)])
    query(t.a,'ROLLBACK TO promotion');query(t.a,'RELEASE promotion');query(t.a,'COMMIT');check('promotion child rollback restores both readonly modes',mode_flags(),[(True,),(True,)])
   enable();check('committed explicit promotion enables both insert modes',mode_flags(),[(False,),(False,)])
   check('mode promotion allocates no business snapshot/frontiers',t.head(),oldhead);check('mode promotion preserves all4 fair H columns',cutoffs(),initialH)
   if a.mode=='insert':
    f=frontiers();s0=t.head()[0];query(t.a,'BEGIN')
    got=query(t.a,'INSERT INTO biz.parent VALUES(3,333),(11,1111) RETURNING id,val');heap_values(t.a,[(3,333),(11,1111)])
    check('native host RETURNING original two business rows',got,[(3,333),(11,1111)])
    check('same TX parent reads own journal once per leaf',rows(),own_oracle(t.a))
    query(t.a,'COMMIT');s1=t.head()[0];check('two changed lake leaves publish one catalog S',s1,s0+1)
    for tid,key,value in [(low,3,333),(high,11,1111)]:
     z=[x for x in physical(tid)if x[3:]==(key,value)];check('one exact committed new leaf version '+str(tid),z,[(f[tid][1],s1,None,key,value)])
     now=frontiers()[tid];check('per-leaf batch allocator advances one '+str(tid),now[1],f[tid][1]+1);check('stored record count advances one '+str(tid),now[0],f[tid][0]+1)
    check('cross-leaf identity uses different table IDs, independent of numeric R',low!=high,True)
    check('no parent lake data table/allocator',scalar(t.a,"SELECT count(*) FROM created_metadata.ducklake_table WHERE table_name='parent'"),0)
    check('same winning S updates both certified heads',[x[2]for x in maps(pid)],[s1,s1])
    check('INSERT leaves H scheduling fields unchanged',cutoffs(),initialH);check('committed parent/heap rows exact',rows(),oracle())
    # Native row routing remains the OG RANGE boundary, not a hand-coded key test.
    old=state();query(t.a,'BEGIN');query(t.a,'INSERT INTO biz.heap_marker VALUES(99)')
    reject(t.a,'INSERT INTO biz.parent VALUES(20,200)','P0002');query(t.a,'ROLLBACK');check('native out-of-range and heap rollback atomic',state(),old)
    query(t.a,"SELECT og_lake.set_option('created_lake'::name,'data_inlining_row_limit','0')");old=state()
    query(t.a,'BEGIN');query(t.a,'INSERT INTO biz.heap_marker VALUES(100)');reject(t.a,'INSERT INTO biz.parent VALUES(4,444)');query(t.a,'ROLLBACK')
    check('limit0 cannot bypass into inline or direct files',state(),old)
    query(t.a,"SELECT og_lake.set_option('created_lake'::name,'data_inlining_row_limit','10000')")
    for statement in ['UPDATE biz.parent SET val=val+1','DELETE FROM biz.parent','COPY biz.parent FROM STDIN']:reject(t.a,statement)
   elif a.mode=='insert-rollback':
    old=state();base=diagnostic(t.a);query(t.a,'BEGIN');inside=diagnostic(t.a)
    values=[(i%20,10000+i)for i in range(512)];both_insert(t.a,values)
    check('512-row two-leaf statement reads complete own overlay',rows(),own_oracle(t.a))
    after=diagnostic(t.a);check('bounded statement uses one catalog and two canonical journals',(after['transaction_catalogs'],after['transaction_tables']),(1,2))
    check('512 inserts retain exactly512 private row versions',after['transaction_rows'],512)
    check('statement End releases both registered leaf views',after['registered_snapshots'],inside['registered_snapshots'])
    query(t.a,'SAVEPOINT discarded');both_insert(t.a,[(3,777),(13,777)]);query(t.a,'ROLLBACK TO discarded');query(t.a,'RELEASE discarded')
    check('subabort discards both leaf child after-images',rows(),own_oracle(t.a));query(t.a,'ROLLBACK')
    check('whole rollback restores source/catalog/head/heap markers',state(),old)
    check('ordinary heap oracle rolled back too',rows(),oracle());closed=diagnostic(t.a)
    check('rollback releases all lake journal state',[closed[x]for x in ['transaction_catalogs','transaction_tables','transaction_rows']],[0,0,0]);check('rollback snapshot diagnostic returns baseline',closed['registered_snapshots'],base['registered_snapshots'])
   elif a.mode=='insert-cursor':
    # No Sort: first FETCH must leave a real suspended native leaf reader,
    # and the second leaf has not yet been opened by that first FETCH.
    cursor_sql='SELECT id,val FROM biz.parent'
    raw=scalar(t.a,'EXPLAIN (FORMAT JSON,COSTS false) '+cursor_sql)
    cursor_plan=json.loads(raw)if isinstance(raw,str)else raw
    write_json(out/'insert-cursor-plan.json',cursor_plan)
    def cursor_nodes(x):
     if isinstance(x,dict):
      yield x
      for v in x.values():yield from cursor_nodes(v)
     elif isinstance(x,list):
      for v in x:yield from cursor_nodes(v)
    plan_nodes=list(cursor_nodes(cursor_plan))
    check('exact cursor plan has one genuine native lake reader',
          sum(x.get('Native Lake Parent')=='certified readonly native RANGE leaves'for x in plan_nodes),1)
    check('exact cursor plan has native partition iterator',
          sum(x.get('Node Type')=='Partition Iterator'for x in plan_nodes),1)
    check('cursor has no eager materializing plan barrier',
          [x.get('Node Type')for x in plan_nodes if x.get('Node Type')in ['Sort','Materialize','Aggregate','HashAggregate','GroupAggregate','Hash']],[])
    baseline=diagnostic(t.a);old=state();expected=oracle()
    query(t.a,'BEGIN');inside=diagnostic(t.a)
    query(t.a,'DECLARE ancestor NO SCROLL CURSOR FOR SELECT id,val FROM biz.parent')
    declared=diagnostic(t.a)
    check('DECLARE eagerly owns both typed journals before first FETCH',
          (declared['transaction_catalogs'],declared['transaction_tables'],declared['transaction_rows']),(1,2,0))
    query(t.a,'SAVEPOINT first_fetch_child')
    first=query(t.a,'FETCH FORWARD 1 FROM ancestor');check('first child FETCH actually returns one row',len(first),1)
    both_insert(t.a,[(3,7777),(13,7777)])
    check('child writes have two real private after-images',diagnostic(t.a)['transaction_rows'],2)
    query(t.a,'ROLLBACK TO first_fetch_child');query(t.a,'RELEASE first_fetch_child')
    restored=diagnostic(t.a)
    check('ancestor cursor retains both journals after child rollback',
          (restored['transaction_catalogs'],restored['transaction_tables'],restored['transaction_rows']),(1,2,0))
    # UPDATE is deliberately still closed in this INSERT-only slice. Its
    # genuine ERROR/rollback must also leave the ancestor portal usable.
    query(t.a,'SAVEPOINT rejected_parent_update')
    reject(t.a,'UPDATE biz.parent SET val=val+1 WHERE id=1')
    query(t.a,'ROLLBACK TO rejected_parent_update');query(t.a,'RELEASE rejected_parent_update')
    rest=query(t.a,'FETCH ALL FROM ancestor')
    check('surviving ancestor cursor returns each fixed-snapshot row exactly once',
          sorted(first+rest,key=lambda x:(x[0],x[1])),sorted(expected,key=lambda x:(x[0],x[1])))
    query(t.a,'CLOSE ancestor')
    check('CLOSE releases all eager leaf snapshot refs in same TX',diagnostic(t.a)['registered_snapshots'],inside['registered_snapshots'])
    # Independently force the first physical read of the later leaf to
    # occur in a child, after all first-leaf rows were returned in the parent.
    low_rows=[x for x in expected if x[0]<10];check('nonempty low and high fixture',0<len(low_rows)<len(expected))
    query(t.a,'DECLARE later_leaf NO SCROLL CURSOR FOR '+cursor_sql)
    low_got=query(t.a,'FETCH FORWARD '+str(len(low_rows))+' FROM later_leaf')
    check('parent consumes exactly the real first native leaf',sorted(low_got),sorted(low_rows))
    query(t.a,'SAVEPOINT second_leaf_first_fetch')
    high_first=query(t.a,'FETCH FORWARD 1 FROM later_leaf')
    check('first child access of second native leaf returns one high row',len(high_first)==1 and high_first[0][0]>=10)
    query(t.a,'ROLLBACK TO second_leaf_first_fetch');query(t.a,'RELEASE second_leaf_first_fetch')
    high_rest=query(t.a,'FETCH ALL FROM later_leaf')
    check('later-leaf reader survives rollback without lost or duplicate rows',
          sorted(low_got+high_first+high_rest),sorted(expected))
    query(t.a,'CLOSE later_leaf')
    check('second-leaf boundary cursor CLOSE releases all eager refs',diagnostic(t.a)['registered_snapshots'],inside['registered_snapshots'])
    # A cursor created by a child is still the host's ordinary aborted portal,
    # never reparented by the lake view fix.
    query(t.a,'SAVEPOINT created_in_child')
    query(t.a,'DECLARE gone NO SCROLL CURSOR FOR SELECT id,val FROM biz.parent')
    check('child-created cursor can execute before rollback',len(query(t.a,'FETCH 1 FROM gone')),1)
    query(t.a,'ROLLBACK TO created_in_child');query(t.a,'RELEASE created_in_child')
    reject(t.a,'FETCH 1 FROM gone','34001');query(t.a,'ROLLBACK')
    check('cursor/subabort leaves catalog and committed data unchanged',state(),old)
    # Empty pruning creates no selected leaf view; early CLOSE must release
    # also the second eager view which has never opened a physical reader.
    query(t.a,'BEGIN');inside=diagnostic(t.a)
    query(t.a,'DECLARE empty_parent NO SCROLL CURSOR FOR SELECT id,val FROM biz.parent WHERE id>=20')
    query(t.a,'SAVEPOINT empty_fetch');check('empty child FETCH',query(t.a,'FETCH ALL FROM empty_parent'),[])
    query(t.a,'ROLLBACK TO empty_fetch');query(t.a,'RELEASE empty_fetch')
    check('empty ancestor cursor remains valid after rollback',query(t.a,'FETCH ALL FROM empty_parent'),[]);query(t.a,'CLOSE empty_parent')
    query(t.a,'DECLARE early_parent NO SCROLL CURSOR FOR SELECT id,val FROM biz.parent')
    check('early cursor fetches only one row',len(query(t.a,'FETCH 1 FROM early_parent')),1);query(t.a,'CLOSE early_parent')
    check('early CLOSE frees even unopened second leaf view',diagnostic(t.a)['registered_snapshots'],inside['registered_snapshots']);query(t.a,'COMMIT')
    closed=diagnostic(t.a)
    check('cursor cases release all canonical journals at transaction end',
          [closed[x]for x in ['transaction_catalogs','transaction_tables','transaction_rows']],[0,0,0])
    check('cursor case final registered snapshot baseline',closed['registered_snapshots'],baseline['registered_snapshots'])
   else:
    old=frontiers();s0=t.head()[0];query(t.a,'BEGIN');query(t.b,'BEGIN')
    both_insert(t.a,[(3,301),(11,1101)]);both_insert(t.b,[(4,402),(12,1202)])
    # Overlapping real TXs; intentionally sequential COMMIT tests fresh
    # publication and allocation. A PK collision gate is NOT simulated here.
    query(t.a,'COMMIT');query(t.b,'COMMIT');check('two real commits advance two global snapshots',t.head()[0],s0+2)
    check('two-writer final exact lake/ordinary heap parity',rows(),oracle())
    for tid,keys in [(low,{3,4}),(high,{11,12})]:
     z=[x for x in physical(tid)if x[3]in keys];check('two new same-leaf permanent IDs never overlap '+str(tid),sorted(x[0]for x in z),[old[tid][1],old[tid][1]+1])
     check('independent per-leaf frontier after both commits '+str(tid),frontiers()[tid][1],old[tid][1]+2)
    check('latest parent fence advances atomically to last winning S',[x[2]for x in maps(pid)],[s0+2,s0+2]);check('two writers preserve H fields',cutoffs(),initialH)
   check('bounded INSERT does not imply native UPDATE/DELETE support',contract.get('native_update_delete_open'),False)
  elif a.mode=='fusion':
   protected=state()
   for statement in ['REINDEX INDEX biz.parent_local_id','REINDEX INDEX CONCURRENTLY biz.parent_local_id']:
    try:query(t.a,statement)
    except Exception as error:
     check('indexed native parent exact utility state/'+statement,getattr(error,'pgcode',None),'0A000')
     check('indexed native parent actual marker guard/'+statement,getattr(getattr(error,'diag',None),'message_primary','').startswith('native lake partition root does not support '))
    else:raise AssertionError('unexpected accepted index utility: '+statement)
   check('index utilities preserve preexisting native index and complete state',state(),protected)
   for statement in ['EXECUTE cached_parent_insert(3,333)','EXECUTE cached_parent_update','EXECUTE cached_parent_delete']:
    reject(t.a,statement)
   try:cached=query(t.a,'EXECUTE cached_parent_select')
   except Exception as e:
    check('stale fusion read explicit SQLSTATE',getattr(e,'pgcode',None),'0A000')
    check('stale fusion read exact runtime guard','cannot execute an OpFusion heap fast path'in str(e))
    r['cached_read_outcome']='runtime marker fence requires replan'
    query(t.a,'DEALLOCATE cached_parent_select');query(t.a,'PREPARE cached_parent_select AS SELECT id,val FROM biz.parent WHERE id=1')
    cached=query(t.a,'EXECUTE cached_parent_select')
   else:r['cached_read_outcome']='host invalidation replanned into qualified native reader'
   check('cached read across binding cannot return empty local heap',cached,[(1,100)])
   # New simple INSERT keeps fusion on. Preserve eligibility diagnostics if
   # reached; the native readonly planner can legitimately reject earlier.
   t.a.notices[:]=[];reject(t.a,'INSERT INTO biz.parent VALUES(3,333)')
   write_json(out/'marked-fresh-insert-client-notices.json',list(t.a.notices))
   r['marked_fresh_insert_admission']='shared fusion eligibility refused'if any("Bypass not executed because query's relation is not support"in x for x in t.a.notices)else'readonly planner/executor rejected before an eligibility log; no claim that this factory branch ran'
   # Exact readonly SQLSTATE plus immutable heap/catalog proof remain required;
   # a legitimate earlier readonly planner error must not be mislabeled as
   # observed factory execution. Actual prebind fusion selection stays strict.
   reject(t.a,'UPDATE biz.parent SET val=val+1 WHERE id=1');reject(t.a,'DELETE FROM biz.parent WHERE id=1')
   check('fusion switches remain enabled after all guards',(scalar(t.a,'SHOW enable_opfusion'),scalar(t.a,'SHOW enable_partition_opfusion')),('on','on'))
   check('fusion boundary preserves complete standard/private/heap state',state(),protected)
   check('both native local heap byte populations unchanged',query(t.a,'SELECT oid,pg_partition_size(%s::oid,oid) FROM pg_partition WHERE parentid=%s AND parttype=%s ORDER BY oid',(pid,pid,'p')),heap_before)
   explain('fusion-parent-qualified',iterations=2)
  elif a.mode in ['basic','upgrade']:
   explain('both',iterations=2);explain('only-low','id<10',1);explain('only-high','id>=10',1);explain('native-empty','id>=20',0)
   check('original host qualification/projection',query(t.a,'SELECT id,val+5 FROM biz.parent WHERE val>150 ORDER BY val DESC,id LIMIT 3'),query(t.observer,'SELECT id,val+5 FROM biz.exact WHERE val>150 ORDER BY val DESC,id LIMIT 3'))
   check('native aggregate sees both complete leaves',query(t.a,'SELECT count(*),sum(id),sum(val) FROM biz.parent'),query(t.observer,'SELECT count(*),sum(id),sum(val) FROM biz.exact'))
  elif a.mode=='utility-guards':
   def reject_guard(statement):
    try:query(t.a,statement)
    except Exception as error:
     check('exact utility rejection SQLSTATE/'+statement,getattr(error,'pgcode',None),'0A000')
     check('actual native marker guard reached/'+statement,getattr(getattr(error,'diag',None),'message_primary','').startswith('native lake partition root does not support '))
    else:raise AssertionError('unexpected accepted native parent utility: '+statement)
   def utility_image():
    return dict(data=rows(),standard=t.protocol(),maps=maps(pid),
     relation_stats=query(t.observer,'SELECT relpages,reltuples,relallvisible FROM pg_class WHERE oid=%s',(pid,)),
     column_stats=query(t.observer,'SELECT * FROM pg_statistic WHERE starelid=%s ORDER BY staattnum',(pid,)),
     indexes=query(t.observer,'SELECT indexrelid,indisvalid,indisready FROM pg_index WHERE indrelid=%s ORDER BY indexrelid',(pid,)),
     leaves=query(t.observer,"SELECT oid,relname,partitionno FROM pg_partition WHERE parentid=%s AND parttype='p' ORDER BY oid",(pid,)))
   for writable in (False,True):
    if writable:query(t.a,'SELECT og_lake.enable_native_partition_insert(%s::regclass)',(pid,))
    protected=utility_image()
    for statement in ['ANALYZE biz.parent','ANALYZE biz.parent PARTITION(low)',
      'VACUUM biz.parent','VACUUM ANALYZE biz.parent','VACUUM FULL biz.parent',
      'VACUUM biz.parent PARTITION(low)','VACUUM FULL biz.parent PARTITION(low)',
      'CREATE INDEX forbidden_local ON biz.parent(id) LOCAL',
      'CREATE INDEX forbidden_global ON biz.parent(id)',
      'REINDEX TABLE biz.parent','REINDEX TABLE CONCURRENTLY biz.parent','REINDEX TABLE biz.parent PARTITION low',
      'CLUSTER biz.parent','CLUSTER biz.parent PARTITION(low)']:
     reject_guard(statement)
     check('utility rejection preserves full parent state/'+str(writable)+'/'+statement,utility_image(),protected)
   query(t.a,'CREATE TABLE biz.ordinary_utility(id integer PRIMARY KEY,val integer)')
   query(t.a,'INSERT INTO biz.ordinary_utility VALUES(1,10),(2,20)')
   for statement in ['ANALYZE biz.ordinary_utility','VACUUM biz.ordinary_utility',
     'REINDEX TABLE biz.ordinary_utility','CLUSTER biz.ordinary_utility USING ordinary_utility_pkey']:
    query(t.a,statement)
   check('unmarked ordinary utility paths still retain exact data',query(t.a,'SELECT * FROM biz.ordinary_utility ORDER BY id'),[(1,10),(2,20)])
  elif a.mode=='readonly':
   before=state()
   reject(t.a,'INSERT INTO biz.parent VALUES(5,50)','25006')
   for statement in['UPDATE biz.parent SET val=val+1','DELETE FROM biz.parent','COPY biz.parent FROM STDIN','COPY biz.parent TO STDOUT',
     'ALTER TABLE biz.parent ADD COLUMN extra integer','TRUNCATE biz.parent','DROP TABLE biz.parent',
     'ALTER TABLE biz.parent SET(lake_partition_root=false)','ALTER TABLE biz.parent RESET(lake_partition_root)']:
    reject(t.a,statement)
   reject(t.a,'DROP EXTENSION og_lake CASCADE','2BP01');check('readonly refusals preserve complete state',state(),before)
   query(t.a,'BEGIN');query(t.a,'SAVEPOINT missing');query(t.a,'DELETE FROM og_lake_internal.partition_binding WHERE parent_relid=%s AND leaf_partition_oid=%s',(pid,leaves[0]))
   reject(t.a,'SELECT * FROM biz.parent','XX001');query(t.a,'ROLLBACK TO missing');query(t.a,'RELEASE missing');query(t.a,'COMMIT')
   check('missing binding cannot silently fall back to local heap',rows(),oracle())
   query(t.a,'CREATE TABLE biz.ordinary(id integer)');query(t.a,'INSERT INTO biz.ordinary VALUES(1),(2)');query(t.a,'UPDATE biz.ordinary SET id=id+1');query(t.a,'DELETE FROM biz.ordinary WHERE id=2')
   check('unmarked ordinary heap path unaffected',query(t.a,'SELECT id FROM biz.ordinary'),[(3,)])
  elif a.mode=='permissions':
   role=roles.create('parent_reader');user,identity=roles.connect(db,'parent_reader');configure(user);r['ordinary_login']=identity
   query(t.a,sql.SQL('GRANT USAGE ON SCHEMA biz TO {}').format(sql.Identifier(role)))
   reject(user,'SELECT id,val FROM biz.parent','42501')
   query(t.a,sql.SQL('GRANT SELECT(id) ON biz.parent TO {}').format(sql.Identifier(role)))
   check('ordinary user reads granted parent column through both native lake leaves',query(user,'SELECT id FROM biz.parent ORDER BY id'),query(t.observer,'SELECT id FROM biz.exact ORDER BY id'))
   reject(user,'SELECT id,val FROM biz.parent','42501')
   query(t.a,sql.SQL('GRANT SELECT ON biz.parent TO {}').format(sql.Identifier(role)))
   check('ordinary user granted full parent ACL sees exact lake rows',query(user,'SELECT id,val FROM biz.parent ORDER BY id'),oracle())
   reject(user,'SELECT og_lake.bind_native_partition_readonly(%s::regclass,%s::name,%s::oid[],%s::bigint[])','42501',(pid,t.server,leaves,ids))
  elif a.mode=='snapshot':
   query(t.a,'PREPARE parent_cached AS SELECT id,val FROM biz.parent ORDER BY id');check('cached qualified parent before advance',query(t.a,'EXECUTE parent_cached'),oracle())
   baseline=diagnostic(t.a);query(t.a,'BEGIN');inside=diagnostic(t.a)
   query(t.a,'DECLARE fixed NO SCROLL CURSOR FOR SELECT id,val FROM biz.parent');first=query(t.a,'FETCH FORWARD 1 FROM fixed')
   check('cursor first leaf row exists',len(first),1)
   # A legitimate unrelated table commit advances standard catalog head.
   t.create('unrelated',t.b);query(t.b,'INSERT INTO biz.unrelated VALUES(99,9900)')
   check('legitimate unrelated publication advanced catalog',t.head()[0]>before[0])
   remaining=query(t.a,'FETCH ALL FROM fixed');check('cursor retains one Executor snapshot across unopened later leaf',sorted(first+remaining,key=lambda x:x[0]),oracle())
   query(t.a,'CLOSE fixed');closed=diagnostic(t.a)
   check('CLOSE returns diagnostic snapshot count to same transaction baseline',closed['registered_snapshots'],inside['registered_snapshots'])
   query(t.a,'COMMIT');after=diagnostic(t.a)
   check('read-only leaf views own no relation journals',[after[x]for x in['transaction_catalogs','transaction_tables','transaction_rows']],[0,0,0])
   check('cursor transaction diagnostic snapshot baseline',after['registered_snapshots'],baseline['registered_snapshots'])
   reject(t.a,'EXECUTE parent_cached');reject(t.observer,'SELECT id,val FROM biz.parent')
  elif a.mode=='lifetime':
   baseline=diagnostic(t.a);query(t.a,'BEGIN');inside=diagnostic(t.a)
   query(t.a,'DECLARE early NO SCROLL CURSOR FOR SELECT id,val FROM biz.parent');check('early cursor one row',len(query(t.a,'FETCH 1 FROM early')),1);query(t.a,'CLOSE early')
   check('early CLOSE releases snapshot baseline',diagnostic(t.a)['registered_snapshots'],inside['registered_snapshots']);query(t.a,'COMMIT')
   for n in range(8):check('early LIMIT closes leaf resources/'+str(n),len(query(t.a,'SELECT id FROM biz.parent LIMIT 1')),1)
   query(t.a,'BEGIN');query(t.a,'SAVEPOINT projection_error');reject(t.a,'SELECT id/(val-val) FROM biz.parent','22012');query(t.a,'ROLLBACK TO projection_error');query(t.a,'RELEASE projection_error');query(t.a,'COMMIT')
   check('error rollback reopens valid native reader',rows(),oracle());closed=diagnostic(t.a)
   check('leaf readers create no retained transaction journals',[closed[x]for x in['transaction_catalogs','transaction_tables','transaction_rows']],[0,0,0])
   check('all diagnostic registered snapshots return baseline',closed['registered_snapshots'],baseline['registered_snapshots'])
  check('all executed input bytes unchanged',all(digest(n)==x['sha256']for n,x in r['inputs'].items()))
  check('all executable/SDK bytes unchanged',all(digest(n)==h for n,h in r['hash_only_inputs'].items()));check_build(a.build_receipt);r['status']='pass'
 except BaseException as e:r.update(reason=str(e),traceback=traceback.format_exc())
 finally:
  if t:
   for c in[t.a,t.b,t.observer]:
    try:
     if not c.closed:query(c,'ROLLBACK')
    except BaseException as e:r.setdefault('rollback_errors',[]).append(str(e))
  if db:
   try:db.cleanup();r['database_cleanup']=db.cleanup_result
   except subprocess.TimeoutExpired as e:
    ownerfile=out/'database-owner.json';r.update(status='fail',database_cleanup_error='timeout')
    r['database_cleanup_timeout']=dict(argv=e.cmd,timeout_seconds=e.timeout,owner_receipt_sha256=digest(ownerfile)if ownerfile.exists()else None,partial_stdout=image(e.stdout),partial_stderr=image(e.stderr),no_automatic_restart_cleanup=True)
   except BaseException as e:r.update(status='fail',database_cleanup_error=str(e))
  if roles:
   try:
    if not r.get('database_cleanup')or r['database_cleanup']['returncode']:raise RuntimeError('exact owned database cleanup unproven, retaining roles')
    r['role_cleanup']=cleanup_roles(control,roles.path,a.run_id);r=roles.redact(r)
   except BaseException as e:r.update(status='fail',role_cleanup_error=str(e))
  if control:
   try:control.close()
   except BaseException as e:r.update(status='fail',control_cleanup_error=str(e))
  if reservation:
   fcntl.flock(reservation.fileno(),fcntl.LOCK_UN);reservation.close();r['reference_reservation_released_after_cleanup_attempt']=True
  write_json(out/'result.json',r)
 print(json.dumps(dict(status=r['status'],mode=a.mode,checks=len(r['assertions']),reason=r.get('reason'),result=str(out/'result.json'))))
 return 0 if r['status']=='pass'else 1

if __name__=='__main__':raise SystemExit(main())
