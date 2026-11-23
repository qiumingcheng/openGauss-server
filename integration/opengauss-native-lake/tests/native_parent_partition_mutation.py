#!/usr/bin/env python3
"""Prepared native parent typed UPDATE/DELETE/RC/atomic routing qualification.

Default prepare uses stdlib only, no Native/SDK/fixture/SQL. Execution needs a
root-reviewed newly coordinated SQL capability and exact official build+SDK
receipt. This PRIVATE leaf prepares typed UPDATE/DELETE only; Native execution is unqualified.
COPY has a separate seven-mode runner; fullDDL/index/SMP/Vec and full PGL3 remain unfinished.
"""
import argparse,ast,fcntl,hashlib,json,os,re,secrets,shutil,signal,subprocess,sys,time,traceback
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
ROOT=Path(os.getenv('CPP_WORKSPACE','/home/qiumc/data/code/cpp')).resolve()
D=ROOT/'integration/opengauss-native-lake'
MODES=('same-leaf', 'savepoint', 'wait-commit', 'wait-abort', 'wait-predicate', 'delete-rm-off', 'delete-rm-on', 'delete-wait-rm-off', 'delete-wait-rm-on', 'opposite-routing', 'reverse-deadlock')
EPQ_MULTIROW_MODE='wait-multirow'
MODES += (EPQ_MULTIROW_MODE,)
EPQ_CONTEXT_FIX='7ce63fef4c75c12096367b763ae41ee25df8663215af184ced0fdc5d0e9de215'
EPQ_MULTIROW_SIDECAR='ca1cf4995a412a439933eb197623cadde6b6ffdd4ff3937f7295e7ad37e2baa9'
ROWHEAD_MODES=('rowhead-identity', 'rowhead-busy-fairness', 'rowhead-rollback', 'rowhead-bounded')
ROWHEAD_POLICY_MODES=('rowhead-policy-legacy25', 'rowhead-policy-access', 'rowhead-policy-readonly-binding', 'rowhead-policy-dml', 'rowhead-policy-flush', 'rowhead-policy-create')
ROWHEAD_AUTO_MODES=('rowhead-auto', 'rowhead-vacuum-reuse')
TYPED_ROWHEAD_MODES=ROWHEAD_MODES+ROWHEAD_POLICY_MODES+ROWHEAD_AUTO_MODES
MODES += TYPED_ROWHEAD_MODES
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


MUTATION_CANDIDATE="d74ec91c741d99f7d288d8d0858ed1013791185200388e31df2b85f52598d4bf"
MUTATION_PREDICATE="1d455475143cc0da4f9789246c47135f47459c237cd3b339b28df4db54936cf4"
QA_TRUTH_BASE="7c5d84093adc7e615e55b4a0c4ca9121c43118efcd746f33cdf262c1eb164007"

# Final source projection is reviewed separately from capability provenance.
FINAL_PRODUCT_PATCH='ed788260db45efa8e17345cc20bf8ed2c625dcccf7ca2163e7a6f63cabc9f01e'
FINAL_PRODUCT_MANIFEST='1b6eccda98c6b2919467edbea6507468047043fa4dd413a24e3493ffbd9aba39'
FINAL_PRODUCT_INPUTS={'contrib/og_lake/CMakeLists.txt': 'acd08e20e6e40698c0da5fa13670ab005d852f03f96c0f242c8ff58a6685db06',
 'contrib/og_lake/Makefile': '6257db37ce8e20dd0d8efe6d8ca9f0e54a2c8359ab624f38229e746e24a28d93',
 'contrib/og_lake/lake_acl.cpp': '5725a951b3b686264c2750d7b9564a3c22408ede9ae1ace98932fdc4cf387e01',
 'contrib/og_lake/lake_autoanalyze.cpp': '3dda56dac476ee5c80902186c9588e5fa5bf5553955ff41f3969f866c5896884',
 'contrib/og_lake/lake_catalog.cpp': '6091dee046c61e55021fc57e234c3e1c3540cec9763299c2d0b2aac93051d501',
 'contrib/og_lake/lake_create_publish.cpp': '8a9517e73e0df1827f42e8d5d8776f7bc9219c0f6509cc289ab49a88fdec6b20',
 'contrib/og_lake/lake_create_publish.h': 'cca54d5636244cf583e55c395bb76976cb5d1f3511089330a8d348e4db654872',
 'contrib/og_lake/lake_locator_plan.cpp': 'bd196138333ca6ebb1d0576e17c436a19f837d0a1f6acd07dadd6378c40e9eef',
 'contrib/og_lake/lake_modify.cpp': '7ed86e3c66c339806b6c75af81e4ee7cc5cb2a313cef9f775faaad38ed53a5dd',
 'contrib/og_lake/lake_native_partition.cpp': 'e3f56c72b9298ac24515e9999b91797057e8599c2dc4843d7f0c5a5a73ee2c1b',
 'contrib/og_lake/lake_native_partition.h': '8ca4290b8be73ae47dcb4c1bf111ffb61a316f2c3102dd4a171c12afb5b9a646',
 'contrib/og_lake/lake_native_partition_analyze.cpp': 'aaecb53ea3e6597d46de406182286de6bd3439d09824701e2307e0b1bdbe125a',
 'contrib/og_lake/lake_native_partition_bind.cpp': 'fd03d4aadfd37bfd1604dc717a2a408e01b4d7bdc614c834db84d95620f5322c',
 'contrib/og_lake/lake_native_partition_modify.cpp': '0879a8e2e79671859ff6fccd385f3693bcdb951272e48b2ae14b9fe829441085',
 'contrib/og_lake/lake_native_partition_scan.cpp': 'bb3b9715df4e8c33d7fe586b0d4de857657511965f6eb643393b8ccd4c5ee6a5',
 'contrib/og_lake/lake_native_partition_write.cpp': 'aa8ff41bf430ae1fffc65bdd1d99e373abbeb48fee49394018b262b1c9d97c4e',
 'contrib/og_lake/lake_publish.cpp': '7c9a316f2912f7208253bb985ab2472a240af8f8a6b318f876ca9a69b6141179',
 'contrib/og_lake/lake_sample.cpp': 'd25358facee6bdb48845f3c8777ac050627248c479f5155429dcb1133772ce40',
 'contrib/og_lake/lake_sample.h': 'af5cb1fa494613cbd238336ca6b92b5d8be51484e939c4f0447d6e99b240db67',
 'contrib/og_lake/lake_test_hooks.cpp': '1362bc71335bc409891c01d9ddd21c0e2ff42c2bd3ade15c99086d8389ec62f1',
 'contrib/og_lake/lake_version.h': 'c0e95929a7318ac1c56ae0b12e5d8a7f84bc4fe8b922e51e59088a77208af1b4',
 'contrib/og_lake/og_lake--0.19--0.20.sql': '6e8fdd89b15c14c31693a4b468e84fe38ddb1b9e19f4286fa815cea88f5cfd58',
 'contrib/og_lake/og_lake--0.20--0.21.sql': '83c557b527e5a63464a6491d7981c3a16ce39f4c446d26aaa859539946ebcdd1',
 'contrib/og_lake/og_lake--0.20.sql': '3a31f4808c00d5ab171e742a1bce1278be16ddf138c6ae46079419018e383d22',
 'contrib/og_lake/og_lake--0.21--0.22.sql': 'c7df0ef3a13e07c2d9d102f2f1e7a8f51696078f15d9766843062ef260ed5a59',
 'contrib/og_lake/og_lake--0.21.sql': '610666fb9b6755c597c9f1a7d6a62e9036d17b68126bebb9c2f8262ebe48f21d',
 'contrib/og_lake/og_lake--0.22--0.23.sql': '8cd4408331af41c070b0e0432709234d1a4e2785c8f0229fd4c49172c11c1756',
 'contrib/og_lake/og_lake--0.22.sql': 'd9de28bfa97b1c385411a071a1af09796213eebd4392a24779c8f2a7b80fbd7f',
 'contrib/og_lake/og_lake--0.23--0.24.sql': '9d8d247e3e6a81d9815be4122f73333d7e77045cd1ff4af05522408c4ac9ca81',
 'contrib/og_lake/og_lake--0.23.sql': '1890baec867245a11f6ad5419c757e3e705a39fec945de9388b96eb34f1850ad',
 'contrib/og_lake/og_lake--0.24--0.25.sql': '6e458ce9259950e5e01ff825f506f8cd69185a07d363a3879292021a01eff9a0',
 'contrib/og_lake/og_lake--0.24.sql': 'adafb3aaffd69879678b6084a14799e791c49c3b8c280662e23bc78454811aa7',
 'contrib/og_lake/og_lake--0.25.sql': 'c99c0ca220270c6635f5a30aeb917293b36f58938921f623568e0e60cf39f37b',
 'contrib/og_lake/og_lake.control': '7abe54390a9e751ecdb293b2006b9bcdc020f62fa6e010219cc0ee9d7c7e6988',
 'contrib/og_lake/og_lake.cpp': '6029fd05de33a346a750dece455f808de49cf8d162d21acf88f12504e0d7d62b',
 'src/gausskernel/optimizer/commands/analyze.cpp': '7c7fd8cb0a32f4a14bde798fd336dc0d6ad26efefcc3bbaed194b7b6cff4d4c0',
 'src/gausskernel/optimizer/commands/copy.cpp': 'dd68756dff8472a150568350b6eb03d8018ae6bd7e279f631dda8421fa4cd80d',
 'src/gausskernel/optimizer/commands/vacuum.cpp': '8f2d9b8d344974a2ac3636a6d453526ce784de282253799fdf5b46a84bef8b9b',
 'src/gausskernel/optimizer/rewrite/rewriteHandler.cpp': '398e59cd45801b1c15d09282dd78f640070b4fe1ccb3b2f79921e70a4828ac20',
 'src/gausskernel/process/tcop/utility.cpp': 'b743c7b6f69f3df35db0feff3e2cb5fff62361267b6a536d69427b025d6af611',
 'src/gausskernel/runtime/executor/execJunk.cpp': 'd83d6e3ad1ae4799c55f11f530cea808951ad105111f96b7b46daeab3cf2c812',
 'src/gausskernel/runtime/executor/nodeModifyTable.cpp': '61032c139fdfe3ae5ccb9c48869f16d2796ae6e70efe2c8687026ca35cae058c',
 'src/include/access/og_lake.h': '53211b36e17c741d41cf4f8ee0c0f6bb77150333418f682b79191a5e9d96910f'}
RETAINED_NATIVE_INPUTS={'contrib/og_lake/lake_txn.cpp': '50b656d07dcb25d529f73bfb61deefd4a851413f146a678b84372dcb2d0cb9b6',
 'src/gausskernel/cbb/instruments/statement/instr_statement.cpp': '76b0ab119e6e4b85f762c3f2620c8a7bdf958a6eaf28b38d4dcf30deeefd1c8e'}

def main():
 p=argparse.ArgumentParser(description=__doc__)
 p.add_argument('--mode',choices=MODES,required=True);p.add_argument('--execute',action='store_true')
 p.add_argument('--run-id');p.add_argument('--sql-version');p.add_argument('--build-receipt',type=Path)
 p.add_argument('--sdk-qualification',type=Path);p.add_argument('--sdk-index-sha256');p.add_argument('--source-contract',type=Path)
 p.add_argument('--budget-seconds',type=int,default=90)
 p.add_argument('--case-timeout',type=int,default=900);p.add_argument('--_child',action='store_true',help=argparse.SUPPRESS)
 a=p.parse_args()
 if a.mode in TYPED_ROWHEAD_MODES and not a.execute:
  print(json.dumps(dict(status='prepared_not_executed',mode=a.mode,Native=0,SDK=0,fixtures=0,
   ready_for_native_qualification=False,required_runtime='0.26',
   required_installed_sql='0.25' if a.mode=='rowhead-policy-legacy25' else '0.26',
   supervision='one ordinary campaign group for auto; original fixture supervisor for manual/policy',
   pending='No execution admission is supplied by this materialized dispatch/lifecycle adapter.'),indent=2));return 0
 if a.mode==EPQ_MULTIROW_MODE and not a.execute:
  print(json.dumps(dict(status='prepared_not_executed',mode=a.mode,
   private_multirow_epq=True,ready_for_native_qualification=False,
   requirements=['exact EPQ lifetime repair, official build/SDK and separately reviewed waiter admission',
    'two real same-leaf Parquet rows and one waiting UPDATE with two latest-row rechecks',
    'independent native heap timeline, RETURNING, final rows and stable version identities',
    'unchanged original owned fixture/supervisor/cleanup and shared SQL60s deadline'],
   Native=0,SDK=0,fixtures=0),indent=2));return 0
 if not a.execute:
  print(json.dumps(dict(status='prepared_not_executed',mode=a.mode,candidate_patch=MUTATION_CANDIDATE,truth_base=QA_TRUTH_BASE,
   native_activation='Runtime0.25; installed SQL0.24 or0.25; READ/INSERT23 retained, MUTATION/COPY24, ANALYZE25; this leaf never changes installed gates.',
   requirements=['Eleven new mutation modes; original thirteen-mode read/INSERT leaf remains byte-for-byte untouched',
    'Independent real native partitioned heap oracle, same parsed SQL/RETURNING/tags; explicit original literal data, never seeded from lake query',
    'Three business fields plus text payload, separate int4 routing shard and business id; same businessid99 in different leaves with real equal row_id',
    'Real file origin R differs from physical P; stable R/birth/ends and same-catalog one-S crossing',
    'Heap and lake independent actual row-lock timelines: commit/abort/false EPQ, A DELETE to B UPDATE and B DELETE waiters with rowmovement on/off',
    'SAVEPOINT/own modifications/error/outer rollback, multirow DELETE RETURNING and final borrowed text row through End',
    'Opposite routes need no destination-row lock; reverse source lock cycle must yield40P01 with one whole transaction rollback',
    'Exact frozen source/build/binary/SDK/all helper receipts; reviewed strict owned supervisor, bounded cancellation/drain, no service restart or uncontrolled cleanup'],
   runtime_version='0.25',allowed_installed_sql=['0.24','0.25'],native_contract_versions={'read': '0.23', 'insert': '0.23', 'mutation': '0.24', 'copy': '0.24', 'analyze': '0.25'},Native=0,SDK=0,fixtures=0),indent=2));return 0
 if not all([a.run_id,a.sql_version,a.build_receipt,a.sdk_qualification,a.sdk_index_sha256,a.source_contract]):p.error('execute requires run-id, explicit SQL, official build/SDK and reviewed source-contract')
 if a.mode in TYPED_ROWHEAD_MODES:
  from native_parent_sql026_admission import require_contract
  require_contract(json.loads(a.source_contract.read_text()),a.mode,a.sql_version)
 if a.mode not in TYPED_ROWHEAD_MODES and a.sql_version not in ('0.24','0.25'):
  p.error('Mutation requires explicit installed SQL0.24/0.25 on reviewed runtime0.25; retained23 stays closed')
 if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,89}',a.run_id):p.error('run-id must be a safe single component, maximum90')
 if not 300<=a.case_timeout<=1800:p.error('case-timeout must be300..1800')
 if a.mode in ROWHEAD_AUTO_MODES and(not a._child or not 20<=a.budget_seconds<=180):p.error('Automatic typed cases require external owned campaign and original20..180s budget')
 if not a._child:return supervise(a)
 if os.environ.get('OG_LAKE_PARENT_TEST_PARENT')!=str(os.getppid()):p.error('Unsupervised execute child')
 sys.path[:0]=[str(D),str(D/'tests')]
 from concurrent.futures import ThreadPoolExecutor,TimeoutError as FutureTimeout
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
 r=dict(status='fail',mode=a.mode,scope=__doc__,assertions=[],executed=False,installed_sql=a.sql_version,required_runtime='0.26'if a.mode in TYPED_ROWHEAD_MODES else'0.25')
 t=db=control=roles=reservation=claim_guard=None
 workers=[]
 def check(label,got,want=True):
  got,want=image(got),image(want);ok=got==want
  r['assertions'].append(deepcopy(dict(name=label,status='pass'if ok else'fail',observed=got,expected=want)));write_json(out/'result.json',r)
  if not ok:raise AssertionError(label)
 def configure(c):
  for statement in ["SET default_transaction_isolation='read committed'",'SET session_timeout=0','SET statement_timeout=60000','SET query_dop=1','SET enable_lake_smp=off',
                    'SET enable_vector_engine=off','SET enable_default_ustore_table=off']:query(c,statement)
 def reject(c,statement,state='0A000',args=None):
  try:query(c,statement,args)
  except Exception as e:check('reject '+statement[:96]+' SQLSTATE',getattr(e,'pgcode',None),state)
  else:raise AssertionError('unexpected accepted '+statement)
 def parent(name='parent'):
  query(t.a,sql.SQL('CREATE TABLE biz.{}(shard integer,id integer,val integer,payload text) PARTITION BY RANGE(shard) '
    '(PARTITION low VALUES LESS THAN(10),PARTITION high VALUES LESS THAN(20))').format(sql.Identifier(name)))
  oid=scalar(t.a,'SELECT %s::regclass::oid',('biz.'+name,))
  z=query(t.a,"SELECT oid,relname,partitionno FROM pg_partition WHERE parentid=%s AND parttype='p' ORDER BY relname",(oid,))
  check(name+' exactly2 real pg_partition leaves',len(z),2);leaves={n:(oid,no)for oid,n,no in z}
  check(name+' real native names',sorted(leaves),['high','low'])
  check(name+' real leaf identity is not pg_class',scalar(t.a,'SELECT count(*) FROM pg_class WHERE oid=ANY(%s::oid[])',([x[0]for x in leaves.values()],)),0)
  query(t.a,sql.SQL('ALTER TABLE biz.{} {} ROW MOVEMENT').format(sql.Identifier(name),sql.SQL('DISABLE' if a.mode in ['delete-rm-off','delete-wait-rm-off'] else 'ENABLE')))
  check(name+' actual native row movement policy',scalar(t.a,'SELECT relrowmovement FROM pg_class WHERE oid=%s',(oid,)),a.mode not in ['delete-rm-off','delete-wait-rm-off'])
  return oid,[leaves['low'][0],leaves['high'][0]]
 def bind(oid,leaves,ids,c=None):
  return query(c or t.a,'SELECT og_lake.bind_native_partition_readonly(%s::regclass,%s::name,%s::oid[],%s::bigint[])',
    (oid,t.server,leaves,ids))
 def marker(oid,c=None):return scalar(c or t.observer,"SELECT COALESCE('lake_partition_root=true'=ANY(reloptions),false) FROM pg_class WHERE oid=%s",(oid,))
 def maps(oid,c=None):return query(c or t.observer,'SELECT p.leaf_partition_oid,p.partition_no,p.certified_snapshot,p.source_hash,'
   't.object_kind,t.read_only,t.table_id FROM og_lake_internal.partition_binding p JOIN og_lake_internal.table_binding t USING(binding_id) WHERE p.parent_relid=%s ORDER BY p.leaf_partition_oid',(oid,))
 def rows(c=None,predicate='true'):return query(c or t.a,'SELECT shard,id,val,payload FROM biz.parent WHERE '+predicate+' ORDER BY shard,id')
 def oracle(c=None):return query(c or t.observer,'SELECT shard,id,val,payload FROM biz.exact ORDER BY shard,id')
 def state():return dict(catalog=t.protocol(),maps=maps(pid),marker=marker(pid),heap=query(t.observer,'SELECT * FROM biz.heap_marker ORDER BY id'))
 def source(name,values,flushed=False):
  query(t.a,sql.SQL("CREATE FOREIGN TABLE {}(shard integer,id integer,val integer,payload text) SERVER {} OPTIONS(binding_mode 'create',lake_schema 'main')").format(t.relation(name),sql.Identifier(t.server)))
  query(t.a,'BEGIN')
  with t.a.cursor()as cur:cur.executemany(sql.SQL('INSERT INTO {} VALUES(%s,%s,%s,%s)').format(t.relation(name)),values)
  query(t.a,'COMMIT')
  if flushed:
   # Keep the ended earlier birth in the true historical Parquet prefix, so
   # target shard8's logical R is not its physical file position.
   query(t.a,sql.SQL('UPDATE {} SET val=201 WHERE shard=2').format(t.relation(name)))
   query(t.a,sql.SQL('SELECT og_lake.flush({}::regclass)').format(sql.Literal('biz.'+name)))
  return scalar(t.observer,'SELECT table_id FROM created_metadata.ducklake_table WHERE table_name=%s AND end_snapshot IS NULL',(name,))
 def diagnostic(c):return json.loads(scalar(c,'SELECT og_lake_internal.parent_test_xact_state()'))
 def command(c,table,statement,args=None):
  if table not in ['parent','exact']:raise ValueError('unknown owned target')
  with c.cursor()as cur:
   cur.execute(sql.SQL(statement).format(table=sql.Identifier('biz',table)),args)
   answer=cur.fetchall()if cur.description else []
   expected=statement.split()[0].upper()+' '+str(len(answer))
   if statement.lstrip().upper().startswith(('UPDATE','DELETE')):
    check(table+' native host command tag/'+statement[:52],cur.statusmessage,expected)
   r.setdefault('command_results',[]).append(deepcopy(dict(table=table,statement=statement,args=args,tag=cur.statusmessage,rows=answer)))
   return answer
 def sortrows(values):return sorted(values,key=lambda x:(x[0],x[1]))
 def both(c,statement,args=None):
  actual=command(c,'parent',statement,args);expected=command(c,'exact',statement,args)
  check('independent native partitioned heap RETURNING/'+statement[:72],sortrows(actual),sortrows(expected))
  check('same-TX full lake/heap visible parity',rows(c),oracle(c));return actual
 def inline_rows(tid,c=None):
  z=query(c or t.observer,'SELECT table_name FROM created_metadata.ducklake_inlined_data_tables WHERE table_id=%s',(tid,));check('one real current inline registration/'+str(tid),len(z),1)
  return query(c or t.observer,sql.SQL('SELECT _ducklake_row_id,_ducklake_begin_snapshot,_ducklake_end_snapshot,shard,id,val,payload FROM created_metadata.{} ORDER BY 1,2').format(sql.Identifier(z[0][0])))
 def deletion_rows(tid):
  name='ducklake_inlined_delete_'+str(tid)
  visible=query(t.observer,"SELECT c.oid FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace JOIN og_lake_internal.metadata_relation m ON m.relation_oid=c.oid AND m.metadata_schema_oid=n.oid WHERE n.nspname='created_metadata' AND c.relname=%s",(name,))
  if not visible:return []
  check('one canonical real position-delete carrier/'+str(tid),len(visible),1)
  return query(t.observer,sql.SQL('SELECT file_id,row_id,begin_snapshot FROM created_metadata.{} ORDER BY 1,2,3').format(sql.Identifier(name)))
 def real_file_origins(tid):
  duckdb=ROOT/'.reference/opengauss-native-lake/build/duckdb-gcc10/duckdb'
  r['hash_only_inputs'][str(duckdb)]=digest(duckdb)
  files=query(t.observer,'SELECT data_file_id,path,record_count FROM created_metadata.ducklake_data_file WHERE table_id=%s AND end_snapshot IS NULL ORDER BY data_file_id',(tid,))
  check('one actual unpartitioned bootstrap data file',len(files),1)
  fid,path,count=files[0];matches=[x for x in Path(t.empty_manifest['data_path']).rglob('*.parquet')if x.name==Path(path).name]
  check('owned dataset resolves exact committed file',len(matches),1);file=matches[0]
  statement="SET threads=1;SELECT file_row_number AS p,_ducklake_internal_row_id AS r,_ducklake_internal_snapshot_id AS b,shard,id,val,payload FROM read_parquet('"+str(file).replace("'","''")+"',file_row_number=true) ORDER BY file_row_number;"
  proc=subprocess.run([str(duckdb),'-no-init','-batch','-json','-c',statement],capture_output=True,text=True,timeout=30)
  (out/'source-physical.sql').write_text(statement);(out/'source-physical.stdout').write_text(proc.stdout);(out/'source-physical.stderr').write_text(proc.stderr)
  check('independent physical Parquet decoder',proc.returncode,0);physical=json.loads(proc.stdout);check('physical source count is actual catalog N',len(physical),count)
  archived=out/'physical-source.parquet';shutil.copy2(file,archived);check('physical source archive exact bytes',digest(archived),digest(file))
  r['physical_origin']=dict(file_id=fid,path=str(file),archive=str(archived),sha256=digest(file),rows=physical)
  return fid,physical
 def next_ids():return dict(query(t.observer,'SELECT table_id,next_row_id FROM created_metadata.ducklake_table_stats WHERE table_id=ANY(%s::bigint[])',([low,high],)))
 def head_map():
  answer=query(t.observer,'SELECT h.binding_id::text,h.relation_oid,h.leaf_partition_oid,h.partition_no,h.row_head_oid,t.table_id FROM og_lake_internal.row_lock_binding h JOIN og_lake_internal.partition_binding p ON p.binding_id=h.binding_id JOIN og_lake_internal.table_binding t ON t.binding_id=h.binding_id WHERE p.parent_relid=%s ORDER BY h.leaf_partition_oid',(pid,))
  check('two exact independent typed row-head maps',len(answer),2);check('same parent owns both typed heads',sorted(set(x[1]for x in answer)),[pid])
  check('row-heads use distinct leaf/binding/head identities',len({(x[0],x[2],x[4])for x in answer}),2)
  return {x[5]:dict(binding=x[0],leaf=x[2],no=x[3],oid=x[4])for x in answer}
 def lock_rows(pid,waiting=False):
  return query(t.observer,'SELECT pid,locktype,database,relation,page,tuple,transactionid::text,mode,granted FROM pg_locks WHERE pid=%s'+(' AND NOT granted'if waiting else'')+' ORDER BY locktype,relation,page,tuple,transactionid,mode,granted',(pid,))
 def async_command(c,table,statement,args=None):
  pid_=scalar(c,'SELECT pg_backend_pid()');app=scalar(c,"SELECT current_setting('application_name')")
  def execute():
   try:
    with c.cursor()as cur:
     cur.execute(sql.SQL(statement).format(table=sql.Identifier('biz',table)),args);answer=cur.fetchall()if cur.description else[]
     return dict(state='completed',rows=answer,tag=cur.statusmessage,finished_monotonic=time.monotonic())
   except Exception as e:return dict(state='error',sqlstate=getattr(e,'pgcode',None),message=str(e),finished_monotonic=time.monotonic())
  pool=ThreadPoolExecutor(max_workers=1);started=time.monotonic()
  timing=dict(pid=pid_,table=table,submitted_monotonic=started,deadline_monotonic=started+60,shared_wait_deadline=True,
   statement_timeout_seconds=60,diagnostic_grace_seconds=5,legacy_wait_seconds=10,legacy_completion_seconds=30)
  item=dict(connection=c,pid=pid_,application_name=app,table=table,pool=pool,timing=timing,future=pool.submit(execute))
  r.setdefault('async_timing',[]).append(timing)
  workers.append(item);return item
 def owned_cancel(w):
  identity=query(t.observer,'SELECT datid,application_name FROM pg_stat_activity WHERE pid=%s',(w['pid'],))
  check('cancel belongs to exact allocated DB/backend/application',identity,[(db.allocation['database_oid'],w['application_name'])])
  check('explicit cancel delivered to owned backend',scalar(t.observer,'SELECT pg_cancel_backend(%s)',(w['pid'],)))
 def deadline_failure(w,reason):
  # Only collect a diagnostic reply/SQLSTATE after the shared deadline. A
  # completed command received in this grace can never turn failure into pass.
  timing=w['timing'];timing['deadline_failure']=reason
  try:
   reply=w['future'].result(timeout=max(0,timing['deadline_monotonic']+5-time.monotonic()))
   timing['diagnostic_reply']=reply;w['pool'].shutdown(wait=True);w['drained']=True
  except FutureTimeout:timing['diagnostic_reply_pending']=True
  timing['failure_recorded_monotonic']=time.monotonic()
  raise AssertionError(reason+'; one SQL command has a shared 60s absolute deadline')
 def finish_worker(w,seconds=30):
  if not w['timing'].get('shared_wait_deadline'):
   reply=w['future'].result(timeout=seconds);w['pool'].shutdown(wait=True);w['drained']=True
   r.setdefault('async_results',[]).append(deepcopy(dict(pid=w['pid'],table=w['table'],reply=reply)));return reply
  timing=w['timing'];started=time.monotonic();timing.update(completion_wait_started_monotonic=started,legacy_call_completion_seconds=seconds)
  try:reply=w['future'].result(timeout=max(0,timing['deadline_monotonic']-started))
  except FutureTimeout:
   elapsed=time.monotonic()-started
   timing.update(completion_wait_seconds=elapsed,legacy_completion_30s_exceeded=elapsed>30,legacy_call_completion_exceeded=elapsed>seconds)
   deadline_failure(w,'waiting command did not finish before its shared deadline')
  finished=reply['finished_monotonic'];elapsed=max(0,finished-started)
  timing.update(finished_monotonic=finished,total_elapsed_seconds=finished-timing['submitted_monotonic'],
   completion_wait_seconds=elapsed,legacy_completion_30s_exceeded=elapsed>30,legacy_call_completion_exceeded=elapsed>seconds)
  if finished>timing['deadline_monotonic']:deadline_failure(w,'waiting command completed after its shared deadline')
  w['pool'].shutdown(wait=True);w['drained']=True
  r.setdefault('async_results',[]).append(deepcopy(dict(pid=w['pid'],table=w['table'],reply=reply)));return reply
 def wait_holder(w,holder):
  timing=w['timing'];started=time.monotonic();timing.update(shared_wait_deadline=True,wait_started_monotonic=started,legacy_wait_10s_exceeded=False)
  while True:
   waiting=lock_rows(w['pid'],True);held=lock_rows(holder);matches=[]
   for row in waiting:
    if row[1]in ['transactionid','subtransactionid']and row[6]is not None and any(x[1]==row[1]and x[6]==row[6]and x[8]for x in held):matches.append(row)
    if row[1]=='tuple'and any(x[1:7]==row[1:7]and x[8]for x in held):matches.append(row)
   observed=time.monotonic();elapsed=observed-started
   timing.update(wait_observation_seconds=elapsed,legacy_wait_10s_exceeded=elapsed>10)
   if observed>=timing['deadline_monotonic']:deadline_failure(w,'no actual matching tuple/transaction wait observed before shared deadline: '+repr(waiting))
   if matches and not w['future'].done():
    timing.update(first_matching_wait_monotonic=observed,submit_to_matching_wait_seconds=observed-timing['submitted_monotonic'])
    proof=dict(waiter=w['pid'],holder=holder,matches=matches,all_waits=waiting,holder_locks=held,observed_monotonic=observed)
    r.setdefault('real_lock_waits',[]).append(deepcopy(proof));return proof
   if w['future'].done():raise AssertionError('contending command finished before holder end: '+repr(w['future'].result()))
   time.sleep(min(.02,max(0,timing['deadline_monotonic']-time.monotonic())))
 def drain_workers():
  unresolved=[]
  for w in workers:
   if w.get('drained'):continue
   try:
    if not w['future'].done():owned_cancel(w)
    w['future'].result(timeout=5);w['pool'].shutdown(wait=True);w['drained']=True
   except BaseException as e:
    unresolved.append(dict(pid=w['pid'],application_name=w['application_name'],reason=str(e)));w['pool'].shutdown(wait=False,cancel_futures=True)
  if unresolved:
   r.update(status='fail',preserve_owned_scene=True,worker_drain_failed=unresolved,no_cleanup_with_active_client_threads=True)
   write_json(out/'result.json',r);print(json.dumps(dict(status='fail',reason='owned worker threads did not drain; preserve exact DB/roles',result=str(out/'result.json'))),flush=True)
   # Avoid Python executor atexit waiting indefinitely. Outer strict owned
   # supervisor journals the failed child; it never restarts/deletes services.
   os._exit(2)
 def wait_timeline(table,first_delete=False,predicate=False,abort=False,waiter_delete=False,target_shard=8):
  if target_shard not in [1,8]:raise ValueError('unreviewed target shard')
  query(t.a,'BEGIN');query(t.b,'BEGIN')
  first=('DELETE FROM {table} WHERE shard='+str(target_shard)+' RETURNING shard,id,val,payload')if first_delete else ("UPDATE {table} SET val=80,payload='A-held' WHERE shard="+str(target_shard)+' RETURNING shard,id,val,payload')
  first_rows=command(t.a,table,first);check(table+' holder changes exactly one original row',len(first_rows),1)
  if table=='parent':
   head_name=scalar(t.a,'SELECT %s::oid::regclass::text',(heads[low]['oid'],))
   target_rows=[x for x in file_rows if x['shard']==target_shard];check('one physical identity for actual wait target',len(target_rows),1);target_r=target_rows[0]['r']
   hrows=query(t.a,'SELECT row_id,xmin::text,xmax::text FROM '+head_name+' WHERE row_id=%s',(target_r,))
   check('holder owns actual exact logical-R tuple in expected leaf head',len(hrows),1)
   r.setdefault('actual_head_lock_rows',[]).append(dict(head_oid=heads[low]['oid'],leaf_oid=heads[low]['leaf'],R=target_r,rows=hrows))
  holder=scalar(t.a,'SELECT pg_backend_pid()')
  second=('DELETE FROM {table} WHERE shard='+str(target_shard)+' RETURNING shard,id,val,payload')if waiter_delete else ("UPDATE {table} SET val=COALESCE(val,0)+1,payload=payload||'-B' WHERE shard="+str(target_shard)+(' AND val IS NULL'if predicate else'')+' RETURNING shard,id,val,payload')
  pending=async_command(t.b,table,second);proof=wait_holder(pending,holder)
  check(table+' waits before holder commit/abort',not pending['future'].done())
  query(t.a,'ROLLBACK'if abort else'COMMIT');after_holder=t.head()[0];reply=finish_worker(pending)
  if reply['state']=='completed':query(t.b,'COMMIT')
  else:query(t.b,'ROLLBACK')
  after_waiter=t.head()[0]
  if table=='parent'and(predicate or first_delete):check('skipped/error waiter publishes no extra lake snapshot',after_waiter,after_holder)
  r.setdefault('wait_timelines',[]).append(deepcopy(dict(table=table,target_shard=target_shard,holder_command='DELETE'if first_delete else'UPDATE',waiter_command='DELETE'if waiter_delete else'UPDATE',first_rows=first_rows,second=second,holder_outcome='abort'if abort else'commit',proof=proof,reply=reply)))
  return reply

 try:
  if a.mode in ROWHEAD_AUTO_MODES:
   from native_parent_rowhead_adapter import external_identity
   r['external_supervision']=external_identity()
  build=check_build(a.build_receipt);contract=json.loads(a.source_contract.read_text())
  if a.mode in TYPED_ROWHEAD_MODES:
   from native_parent_sql026_admission import require_fixture_admission
   contract,typed_admission=require_fixture_admission(a,build)
   r['actual_typed_admission']=typed_admission
   header=(SOURCE/'contrib/og_lake/lake_native_partition.h').read_text()
   check('reviewed exact ROW_HEAD_SQL contract',re.findall(r'^#define OG_LAKE_NATIVE_PARTITION_ROW_HEAD_SQL_VERSION "([^"]*)"',header,re.M),['0.26'])
  else:
   check('reviewed source contract purpose',contract.get('purpose'),'native_parent_mutation_qualification')
   check('reviewed exact readonly candidate',contract.get('read_candidate_patch_sha256'),CANDIDATE)
   check('explicit coordinated SQL version',contract.get('partition_sql_version'),a.sql_version)
   check('reviewed runtime is distinct from installed SQL',contract.get('runtime_version'),'0.25')
   check('reviewed exact native capability inheritance',contract.get('native_contract_versions'),{'read': '0.23', 'insert': '0.23', 'mutation': '0.24', 'copy': '0.24', 'analyze': '0.25'})
   check('reviewed combined CPP capability source',contract.get('cpp_capabilities_patch_sha256'),'2cdeb956dc8072d1bfe903f6eaf2442a5880b12aa3ba339f6b8cbc23394ceb42')
   check('reviewed table certificate source',contract.get('certificate_patch_sha256'),'899fa0cd9ff9fa9c71abf7239295ac538902362302845ec89ee19ecc377f6174')
   check('reviewed complete final43 product patch',contract.get('product_composition_patch_sha256'),FINAL_PRODUCT_PATCH)
   check('explicit post-composition publisher context repair',contract.get('publish_context_fix_patch_sha256'),'d1c4384d8a344824d4120f04e7e6ab9dbf77dde51cf047b6112794ad60395cd0')
   check('explicit empty mutation plan repair',contract.get('empty_mutation_fix_patch_sha256'),'f78e4c417a0cae264be1d14849a494657de77fa50bda1b5b867cb542cf8e9037')
   check('reviewed EPQ context lifetime repair',contract.get('epq_context_lifetime_fix_patch_sha256'),EPQ_CONTEXT_FIX)
   check('reviewed shared waiter deadline policy',contract.get('waiter_absolute_deadline_qualified_to_test'),True)
   if a.mode==EPQ_MULTIROW_MODE:
    check('explicit multirow EPQ ready-to-test before fixture',contract.get('native_multirow_epq_qualified_to_test'),True)
    check('reviewed multirow EPQ sidecar',contract.get('multirow_epq_sidecar_sha256'),EPQ_MULTIROW_SIDECAR)
    check('exact multirow EPQ sidecar bytes',digest(D/'tests/native_parent_multirow_epq_case.py'),EPQ_MULTIROW_SIDECAR)
   check('reviewed complete final43 manifest',contract.get('product_composition_manifest_sha256'),FINAL_PRODUCT_MANIFEST)
   check('reviewed exact resolved43 product inputs',contract.get('product_composition_inputs'),FINAL_PRODUCT_INPUTS)
   check('reviewed independent native repairs',contract.get('retained_native_inputs'),RETAINED_NATIVE_INPUTS)
   check('reviewed whole Native source inventory count',len(contract['native_inputs']),231)
   check('reviewed whole Native input inventory before',build['native_source_inputs_before'],contract['native_inputs'])
   check('reviewed whole Native input inventory after',build['native_source_inputs_after'],contract['native_inputs'])
   check('reviewed whole wrapper inventory count',len(contract.get('build_wrapper_inputs',{})),6)
   check('reviewed whole wrapper inventory before',build['build_wrapper_inputs_before'],contract['build_wrapper_inputs'])
   check('reviewed whole wrapper inventory after',build['build_wrapper_inputs_after'],contract['build_wrapper_inputs'])
   for n,h in FINAL_PRODUCT_INPUTS.items():check('reviewed resolved43 compiled input/'+n,contract['native_inputs'].get(n),h)
   for n,h in RETAINED_NATIVE_INPUTS.items():check('reviewed retained native repair/'+n,contract['native_inputs'].get(n),h)
   check('reviewed exact independent fusion guard',contract.get('fusion_guard_patch_sha256'),FUSION_GUARD)
   check('reviewed exact bounded INSERT candidate',contract.get('insert_candidate_patch_sha256'),INSERT_CANDIDATE)
   check('reviewed eager leaf-view lifecycle delta',contract.get('cursor_owner_patch_sha256'),CURSOR_OWNER_FIX)
   check('explicit bounded INSERT capability reviewed',contract.get('bounded_insert_qualified_to_test'),True)
   check('explicit root activation review',contract.get('ready_for_native_qualification'),True);check('final activation blockers empty',contract.get('known_blockers'),[])
   if not contract.get('review_evidence')or not contract.get('native_inputs')or not contract.get('shared_helper_inputs'):raise AssertionError('missing reviewed exact source/helper/evidence contract')
   required={'contrib/og_lake/'+x for x in ['lake_native_partition.cpp','lake_native_partition.h','lake_native_partition_bind.cpp','lake_native_partition_scan.cpp','lake_catalog.cpp','lake_acl.cpp','lake_txn.cpp','og_lake.cpp','Makefile','og_lake.control']}
   required|={'src/'+x for x in ['include/access/og_lake.h','include/utils/rel.h','gausskernel/runtime/executor/nodePartIterator.cpp','gausskernel/runtime/executor/nodeForeignscan.cpp','gausskernel/optimizer/path/allpaths.cpp','gausskernel/optimizer/plan/createplan.cpp','gausskernel/optimizer/util/plancat.cpp','gausskernel/optimizer/commands/copy.cpp','gausskernel/optimizer/commands/tablecmds.cpp','gausskernel/storage/access/common/reloptions.cpp','common/backend/catalog/heap.cpp']}
   required.add('contrib/og_lake/og_lake--'+a.sql_version+'.sql')
   required|={'contrib/og_lake/'+x for x in ['lake_row_lock.cpp','lake_row_lock.h','og_lake--0.22--0.23.sql']}
   required|={'src/'+x for x in ['gausskernel/optimizer/commands/analyze.cpp','gausskernel/optimizer/commands/vacuum.cpp','gausskernel/optimizer/commands/indexcmds.cpp','gausskernel/optimizer/commands/cluster.cpp','common/backend/catalog/index.cpp','gausskernel/process/tcop/utility.cpp']}
   required|={'src/gausskernel/runtime/opfusion/opfusion.cpp','src/gausskernel/runtime/opfusion/opfusion_util.cpp'}
   required|={'contrib/og_lake/'+x for x in ['lake_native_partition_modify.cpp','lake_modify.cpp','lake_locator_plan.cpp']}
   required|={'src/gausskernel/optimizer/rewrite/rewriteHandler.cpp','src/gausskernel/runtime/executor/execJunk.cpp','src/gausskernel/runtime/executor/nodeModifyTable.cpp'}
   required|=set(['contrib/og_lake/CMakeLists.txt', 'contrib/og_lake/lake_autoanalyze.cpp', 'contrib/og_lake/lake_create_publish.cpp', 'contrib/og_lake/lake_create_publish.h', 'contrib/og_lake/lake_locator.cpp', 'contrib/og_lake/lake_locator.h', 'contrib/og_lake/lake_modify.cpp', 'contrib/og_lake/lake_native_partition_analyze.cpp', 'contrib/og_lake/lake_native_partition_modify.cpp', 'contrib/og_lake/lake_native_partition_write.cpp', 'contrib/og_lake/lake_publish.cpp', 'contrib/og_lake/lake_sample.cpp', 'contrib/og_lake/lake_sample.h', 'contrib/og_lake/lake_test_hooks.cpp', 'contrib/og_lake/lake_version.h', 'contrib/og_lake/og_lake--0.19--0.20.sql', 'contrib/og_lake/og_lake--0.20--0.21.sql', 'contrib/og_lake/og_lake--0.20.sql', 'contrib/og_lake/og_lake--0.21--0.22.sql', 'contrib/og_lake/og_lake--0.21.sql', 'contrib/og_lake/og_lake--0.22--0.23.sql', 'contrib/og_lake/og_lake--0.22.sql', 'contrib/og_lake/og_lake--0.23--0.24.sql', 'contrib/og_lake/og_lake--0.23.sql', 'contrib/og_lake/og_lake--0.24--0.25.sql', 'contrib/og_lake/og_lake--0.24.sql', 'contrib/og_lake/og_lake--0.25.sql', 'src/gausskernel/optimizer/rewrite/rewriteHandler.cpp', 'src/gausskernel/runtime/executor/execJunk.cpp', 'src/gausskernel/runtime/executor/nodeModifyTable.cpp'])
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
   check('reviewed exact native mutation candidate',contract.get('mutation_candidate_patch_sha256'),MUTATION_CANDIDATE)
   check('reviewed exact leak-safe mutation predicate',contract.get('mutation_predicate_fix_patch_sha256'),MUTATION_PREDICATE)
   check('explicit reviewed mutation ready-to-test',contract.get('mutation_qualified_to_test'),True)
   check('UPDATE/DELETE intentionally opened only in reviewed future build',contract.get('native_update_delete_open'),True)
   check('reviewed fixed SQL contract',re.findall(r'^#define OG_LAKE_NATIVE_PARTITION_SQL_VERSION "([^"]*)"',header,re.M),['0.23'])
   check('reviewed fixed WRITE_SQL contract',re.findall(r'^#define OG_LAKE_NATIVE_PARTITION_WRITE_SQL_VERSION "([^"]*)"',header,re.M),['0.23'])
   check('reviewed fixed MUTATION_SQL contract',re.findall(r'^#define OG_LAKE_NATIVE_PARTITION_MUTATION_SQL_VERSION "([^"]*)"',header,re.M),['0.24'])
   check('reviewed fixed COPY_SQL contract',re.findall(r'^#define OG_LAKE_NATIVE_PARTITION_COPY_SQL_VERSION "([^"]*)"',header,re.M),['0.24'])
   check('reviewed fixed ANALYZE_SQL contract',re.findall(r'^#define OG_LAKE_NATIVE_PARTITION_ANALYZE_SQL_VERSION "([^"]*)"',header,re.M),['0.25'])
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
  if a.mode==EPQ_MULTIROW_MODE:inputs.append(D/'tests/native_parent_multirow_epq_case.py')
  if a.mode in TYPED_ROWHEAD_MODES:
   inputs += [D/'tests/native_parent_sql026_admission.py',D/'tests/native_parent_sql026_expected.json',Path(contract['actual_full_build']['path']),Path(contract['actual_sdk_install']['path'])]
   inputs.append(D/'tests/native_parent_rowhead_adapter.py')
   typed_sidecars={
    'native_parent_rowhead_cases.py':('typed_rowhead_sidecar_sha256','4793a97d815c7812d9b85c93a222c62f4db04b399d386002de3b745fc4e84219'),
    'native_parent_rowhead_policy_cases.py':('typed_rowhead_policy_sidecar_sha256','a1f4582339c4acd41f7b1b47d74bcbaf1495756a0dc77d820168265b20749c1d'),
    'native_parent_rowhead_auto_cases.py':('typed_rowhead_auto_sidecar_sha256','70ee16116200e8d2c8e9860504eb95c21b58c3f34aa4dc8342a5f0a387deada2')}
   for name,(key,expected) in typed_sidecars.items():
    sidecar=D/'tests'/name
    check('reviewed typed sidecar contract/'+name,contract.get(key),expected)
    check('exact typed sidecar bytes/'+name,digest(sidecar),expected);inputs.append(sidecar)
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
  if a.mode in ROWHEAD_AUTO_MODES:
   expected_settings={'enable_lake_maintenance':'on','lake_flush_naptime':'100','lake_flush_worker_budget':'5000','lake_flush_max_batches':'1','lake_flush_min_rows':'1000000','lake_flush_max_delay':'3600000'}
   check('exact ordinary automatic campaign settings',dict(query(control,'SELECT name,setting FROM pg_settings WHERE name=ANY(%s::text[]) ORDER BY name',(list(expected_settings),))),expected_settings)
  else:check('automatic maintenance disabled',scalar(control,'SHOW enable_lake_maintenance'),'off')
  check('dedicated nonpool backend',scalar(control,'SHOW enable_thread_pool'),'off')
  with(out/'fixture.log').open('w')as log:
   subprocess.run([sys.executable,str(D/'tests/catalog_fixture_bundle.py'),'--run-id',a.run_id,'--output',str(out/'fixture/manifest.json')],stdout=log,stderr=subprocess.STDOUT,check=True,timeout=300)
  reservation=(ROOT/'.reference/opengauss-native-lake/.driver.lock').open('a+');fcntl.flock(reservation.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB)
  db=OwnedDatabase(a.run_id,out);db.create();configure(db.admin);roles=OwnedRoles(control,a.run_id,out)
  t=Case(db,SimpleNamespace(mode='net_dml',sql_version=a.sql_version,manifest=out/'fixture/manifest.json',output=out/'result.json',run_id=a.run_id),r,roles)
  for c in[t.a,t.b,t.observer]:configure(c)
  if a.mode in ROWHEAD_AUTO_MODES:
   import native_parent_rowhead_auto_cases
   claim_guard=native_parent_rowhead_auto_cases.ClaimGuard(SimpleNamespace(
    contract=contract,t=t,scalar=scalar,check=check,budget_seconds=a.budget_seconds))
   claim_guard.claim()  # Before the first business source exists; only this owned database.

  check('exact runtime module',json.loads(scalar(t.a,'SELECT og_lake.host_status()'))['module_version'],contract['runtime_version'])
  r['runtime_module_version']=r['assertions'][-1]['observed']
  query(t.a,"CREATE FUNCTION og_lake_internal.parent_test_xact_state() RETURNS text AS '$libdir/og_lake','og_lake_test_xact_state' LANGUAGE C NOT FENCED")
  check('current parent map preserves all18 typed-head columns',scalar(t.a,"SELECT count(*) FROM pg_attribute WHERE attrelid='og_lake_internal.row_lock_binding'::regclass AND attnum>0 AND NOT attisdropped"),18)
  low=source('source_low',[(1,99,100,'low-key-湖'),(2,2,200,'middle'),(8,8,None,'tail-湖')],True)
  high=source('source_high',[(10,99,1000,'high-key-湖'),(19,19,1900,'upper')])
  query(t.a,'UPDATE biz.source_low SET val=201 WHERE shard=2')
  expected=[(1,99,100,'low-key-湖'),(2,2,201,'middle'),(8,8,None,'tail-湖'),(10,99,1000,'high-key-湖'),(19,19,1900,'upper')]
  check('original source DML equals independent literal tape',query(t.a,'SELECT shard,id,val,payload FROM biz.source_low UNION ALL SELECT shard,id,val,payload FROM biz.source_high ORDER BY shard,id'),expected)
  # Real native partitioned heap oracle, with the same route/movement policy.
  parent('exact')
  with t.a.cursor()as cur:cur.executemany('INSERT INTO biz.exact VALUES(%s,%s,%s,%s)',expected)
  before=t.protocol();query(t.a,'DROP FOREIGN TABLE biz.source_low');query(t.a,'DROP FOREIGN TABLE biz.source_high')
  check('source host detach preserves standard data/catalog',t.protocol(),before)
  pid,leaves=parent();ids=[low,high];initial=t.head()
  bind(pid,leaves,ids);check('certification has no data commit',t.head(),initial)
  check('both bound leaves have original literal data',rows(),expected)
  query(t.a,'SELECT og_lake.enable_native_partition_insert(%s::regclass)',(pid,))
  heads=head_map()
  for tid,entry in heads.items():
   head_sql=scalar(t.observer,'SELECT %s::oid::regclass::text',(entry['oid'],))
   check('promotion head is an empty synchronization table/'+str(tid),query(t.observer,'SELECT row_id FROM '+head_sql),[])
   check('head only carries nonnull bigint identity/'+str(tid),query(t.observer,'SELECT attname,atttypid,attnotnull FROM pg_attribute WHERE attrelid=%s AND attnum>0 AND NOT attisdropped ORDER BY attnum',(entry['oid'],)),[('row_id',20,True)])
  file_id,file_rows=real_file_origins(low)
  origin=[x for x in file_rows if x['shard']==8];check('one true file-origin target',len(origin),1);origin=origin[0]
  check('actual target R differs from physical P',origin['r']!=origin['p'])
  root_snapshot=diagnostic(t.a)['registered_snapshots'];first_next=next_ids()
  if a.mode in TYPED_ROWHEAD_MODES:
   context=SimpleNamespace(contract=contract,query=query,scalar=scalar,sql=sql,check=check,t=t,
    low=low,high=high,pid=pid,heads=heads,file_rows=file_rows,inline_rows=inline_rows,
    maps=maps,rows=rows,oracle=oracle,next_ids=next_ids,both=both,r=r,first_next=first_next,
    roles=roles,db=db,configure=configure,source=source,parent=parent,bind=bind,marker=marker,
    database_oid=db.allocation['database_oid'],budget_seconds=a.budget_seconds,
    diagnostic=diagnostic,claim_guard=claim_guard)
   if a.mode in ROWHEAD_MODES:
    import native_parent_rowhead_cases
    native_parent_rowhead_cases.run_case(context,a.mode)
   elif a.mode in ROWHEAD_POLICY_MODES:
    import native_parent_rowhead_policy_cases
    native_parent_rowhead_policy_cases.run_case(context,a.mode)
   else:native_parent_rowhead_auto_cases.run_case(context,a.mode)
  elif a.mode==EPQ_MULTIROW_MODE:
   import native_parent_multirow_epq_case
   native_parent_multirow_epq_case.run_case(SimpleNamespace(
    contract=contract,query=query,scalar=scalar,check=check,t=t,low=low,high=high,
    pid=pid,heads=heads,file_rows=file_rows,file_id=file_id,inline_rows=inline_rows,
    deletion_rows=deletion_rows,rows=rows,oracle=oracle,next_ids=next_ids,
    command=command,async_command=async_command,wait_holder=wait_holder,
    finish_worker=finish_worker,r=r,first_next=first_next))
  elif a.mode=='same-leaf':
   # One business id exists in two native leaves; their independently allocated
   # row IDs are really equal, so a row_id-only seen set would lose one update.
   lo=[x for x in file_rows if x['shard']==1][0]
   hi=[x for x in inline_rows(high)if x[3]==10 and x[2]is None];check('one high current target',len(hi),1)
   check('same business key belongs to two leaves',(lo['id'],hi[0][4]),(99,99))
   check('independent leaves really reuse same numeric row_id',lo['r'],hi[0][0])
   query(t.a,'BEGIN');answer=both(t.a,"UPDATE {table} SET val=val+1,payload=payload||'-sameR' WHERE id=99 RETURNING shard,id,val,payload")
   check('sameR seen/lock domains must update both leaves',len(answer),2);query(t.a,'COMMIT')
   check('same-leaf update does not allocate replacement identities',next_ids(),first_next)
   s1=t.head()[0];query(t.a,'BEGIN')
   both(t.a,"UPDATE {table} SET val=COALESCE(val,80)+1,payload=payload||'-first' WHERE shard=8 RETURNING shard,id,val,payload");query(t.a,'COMMIT')
   birth1=t.head()[0];current=[x for x in inline_rows(low)if x[3]==8 and x[2]is None]
   check('file update preserves actual R/new committed birth',[(x[0],x[1])for x in current],[(origin['r'],birth1)])
   check('original Parquet position marked by P not R',(file_id,origin['p'],birth1)in deletion_rows(low))
   query(t.a,'BEGIN');both(t.a,"UPDATE {table} SET val=val+1,payload=payload||'-second' WHERE shard=8 RETURNING shard,id,val,payload");query(t.a,'COMMIT')
   birth2=t.head()[0];versions=[x for x in inline_rows(low)if x[0]==origin['r']]
   check('two same-R versions have correct half-open intervals',[(x[1],x[2])for x in versions],[(birth1,birth2),(birth2,None)])
   check('both file/inline updates retain table-local allocator',next_ids(),first_next)
   query(t.a,'BEGIN')
   answer=both(t.a,'DELETE FROM {table} WHERE shard IN (2,8,10) RETURNING shard,id,val,payload')
   check('multirow DELETE RETURNING returns all3 text-bearing rows',len(answer),3)
   # The last result can borrow mutation state->rows until host RETURNING
   # materializes it; consuming all rows crosses EndMutation in one statement.
   check('last deleted row still owns its exact text payload',sortrows(answer)[-1][3],'high-key-湖-sameR')
   query(t.a,'COMMIT');check('DELETE full final parity',rows(),oracle())
   query(t.a,'BEGIN');both(t.a,'UPDATE {table} SET val=1 WHERE false RETURNING shard,id,val,payload');both(t.a,'DELETE FROM {table} WHERE shard>=20 RETURNING shard,id,val,payload');query(t.a,'COMMIT')
  elif a.mode=='savepoint':
   old=state();query(t.a,'BEGIN')
   both(t.a,"UPDATE {table} SET val=1,payload=payload||'-own1' WHERE shard=8 RETURNING shard,id,val,payload")
   both(t.a,"UPDATE {table} SET val=val+1,payload=payload||'-own2' WHERE shard=8 RETURNING shard,id,val,payload")
   own=rows(t.a);query(t.a,'SAVEPOINT discarded')
   both(t.a,"UPDATE {table} SET shard=18,val=val+10 WHERE shard=8 RETURNING shard,id,val,payload")
   both(t.a,'DELETE FROM {table} WHERE shard=2 RETURNING shard,id,val,payload')
   command(t.a,'parent',"INSERT INTO {table} VALUES(3,3,33,'discarded')");command(t.a,'exact',"INSERT INTO {table} VALUES(3,3,33,'discarded')")
   check('own mixed child DML before abort',rows(t.a),oracle(t.a))
   query(t.a,'ROLLBACK TO discarded');query(t.a,'RELEASE discarded');check('subabort restores both-leaf parent own row state',rows(t.a),own);check('subabort independent heap parity',rows(t.a),oracle(t.a))
   query(t.a,'SAVEPOINT expression_error')
   reject(t.a,'UPDATE biz.parent SET val=10/(shard-2) WHERE shard IN (1,2)','22012')
   query(t.a,'ROLLBACK TO expression_error');query(t.a,'RELEASE expression_error')
   check('expression ERROR restores earlier own versions',rows(t.a),own);query(t.a,'COMMIT')
   check('committed own-version final parity',rows(),oracle());check('rolled-back new identity never consumes allocator',next_ids(),first_next)
   ended=t.head()[0];check('final own after-image keeps original R and one commit birth',[(x[0],x[1])for x in inline_rows(low)if x[3]==8 and x[2]is None],[(origin['r'],ended)])
   committed=state();query(t.a,'BEGIN');both(t.a,'DELETE FROM {table} RETURNING shard,id,val,payload');query(t.a,'ROLLBACK')
   check('outer rollback restores complete catalog/maps',state(),committed);check('outer rollback restores actual independent heap',rows(),oracle())
  elif a.mode in ['wait-commit','wait-abort','wait-predicate','delete-rm-off','delete-rm-on']:
   is_delete=a.mode.startswith('delete-');predicate=a.mode=='wait-predicate';abort=a.mode=='wait-abort'
   if a.mode=='wait-commit':
    query(t.a,'BEGIN');both(t.a,'UPDATE {table} SET val=val WHERE shard=8 RETURNING shard,id,val,payload');query(t.a,'COMMIT')
    check('warm wait uses a real committed existing native head',scalar(t.observer,'SELECT count(*) FROM '+scalar(t.observer,'SELECT %s::oid::regclass::text',(heads[low]['oid'],))),1)
   # Same genuine concurrent timeline on an independent native heap first.
   heap_reply=wait_timeline('exact',is_delete,predicate,abort)
   lake_reply=wait_timeline('parent',is_delete,predicate,abort)
   expected_error='40000'if a.mode=='delete-rm-on'else None
   if expected_error:
    check('native heap rowmovement DELETE policy SQLSTATE',heap_reply.get('sqlstate'),expected_error)
    check('lake follows native partition movement retry policy',lake_reply.get('sqlstate'),expected_error)
   else:
    check('heap waiting command completes',heap_reply['state'],'completed');check('lake waiting command completes',lake_reply['state'],'completed')
    check('exact RC latest-row RETURNING after actual wait',sortrows(lake_reply['rows']),sortrows(heap_reply['rows']))
    check('host RC command tag after wait',lake_reply['tag'],heap_reply['tag'])
    if predicate or a.mode=='delete-rm-off':check('latest predicate false/deleted target skips update',lake_reply['rows'],[])
   check('committed RC final lake/native heap state',rows(),oracle())
  elif a.mode in ['delete-wait-rm-off','delete-wait-rm-on']:
   # B is genuinely ExecDelete. First wait after an UPDATE must re-fetch the
   # latest business row for DELETE RETURNING, then separately test TM_Deleted.
   heap_updated=wait_timeline('exact',False,waiter_delete=True,target_shard=1)
   lake_updated=wait_timeline('parent',False,waiter_delete=True,target_shard=1)
   for target,reply in [('heap',heap_updated),('lake',lake_updated)]:
    check(target+' waiting DELETE after holder UPDATE completes',reply['state'],'completed')
    check(target+' waiting DELETE returns fresh value/payload',reply['rows'],[(1,99,80,'A-held')])
    check(target+' waiting DELETE host command tag',reply['tag'],'DELETE 1')
   check('waiting DELETE latest-row final full heap parity',rows(),oracle())
   heap_deleted=wait_timeline('exact',True,waiter_delete=True)
   lake_deleted=wait_timeline('parent',True,waiter_delete=True)
   if a.mode=='delete-wait-rm-on':
    check('heap waiting DELETE rowmovement conflict',heap_deleted.get('sqlstate'),'40000')
    check('lake waiting DELETE follows native conflict',lake_deleted.get('sqlstate'),'40000')
   else:
    for target,reply in [('heap',heap_deleted),('lake',lake_deleted)]:
     check(target+' waiting DELETE after holder DELETE completes',reply['state'],'completed')
     check(target+' waiting DELETE skips committed deleted identity',reply['rows'],[])
     check(target+' waiting DELETE empty host command tag',reply['tag'],'DELETE 0')
   check('both genuine waiting DELETE paths match independent heap',rows(),oracle())
  elif a.mode=='opposite-routing':
   first='UPDATE {table} SET shard=18,val=18 WHERE shard=8 RETURNING shard,id,val,payload'
   second='UPDATE {table} SET shard=0,val=0 WHERE shard=10 RETURNING shard,id,val,payload'
   for table in ['exact','parent']:
    query(t.a,'BEGIN');query(t.b,'BEGIN');one=async_command(t.a,table,first);two=async_command(t.b,table,second)
    r1=finish_worker(one,15);r2=finish_worker(two,15)
    check(table+' distinct old sources route without a destination-lock deadlock',(r1['state'],r2['state']),('completed','completed'))
    check(table+' opposite routes return both new shard values',(r1['rows'][0][0],r2['rows'][0][0]),(18,0))
    query(t.a,'COMMIT');query(t.b,'COMMIT')
   check('opposite real routing final parity',rows(),oracle())
   # A single transaction crossing one leaf has exactly one metadata commit
   # with old source ended and target fresh identity, independent of numeric R.
   starts=next_ids();before=t.head()[0];old_low=[x for x in inline_rows(low)if x[3]==0 and x[2]is None]
   check('reverse route really produced one low current identity',len(old_low),1)
   query(t.a,'BEGIN');both(t.a,'UPDATE {table} SET shard=11 WHERE shard=0 RETURNING shard,id,val,payload');query(t.a,'COMMIT')
   after=t.head()[0];check('single cross-leaf host TX publishes one lake S',after,before+1)
   old=[x for x in inline_rows(low)if x[0]==old_low[0][0]and x[1]==old_low[0][1]]
   new=[x for x in inline_rows(high)if x[3]==11 and x[2]is None]
   check('cross-leaf source end is exact winning S',old[0][2],after)
   check('destination gets its own fresh table-local identity/birth',[(x[0],x[1])for x in new],[(starts[high],after)])
   check('different table IDs, not guessed different R',low!=high)
   check('cross-leaf final standard read/native heap parity',rows(),oracle())
  elif a.mode=='reverse-deadlock':
   for c in [t.a,t.b]:query(c,"SET deadlock_timeout='2s'")
   # Reverse source locks after two uncommitted opposite routes. No artificial
   # pause/pg_advisory lock is used as a substitute for native tuple waiting.
   query(t.a,'BEGIN');query(t.b,'BEGIN')
   a_first='UPDATE {table} SET shard=18,val=18 WHERE shard=8 RETURNING shard,id,val,payload'
   b_first='UPDATE {table} SET shard=0,val=0 WHERE shard=10 RETURNING shard,id,val,payload'
   command(t.a,'parent',a_first);command(t.b,'parent',b_first)
   a_second='UPDATE {table} SET val=val+1 WHERE shard=10 RETURNING shard,id,val,payload'
   b_second='UPDATE {table} SET val=COALESCE(val,0)+1 WHERE shard=8 RETURNING shard,id,val,payload'
   holder_b=scalar(t.b,'SELECT pg_backend_pid()');left=async_command(t.a,'parent',a_second);wait_holder(left,holder_b)
   right=async_command(t.b,'parent',b_second)
   replies=[finish_worker(left,30),finish_worker(right,30)]
   check('real reverse-route source lock cycle yields exactly one40P01 victim',sorted((x['state'],x.get('sqlstate'))for x in replies),[('completed',None),('error','40P01')])
   winner=0 if replies[0]['state']=='completed'else 1
   for index,c in enumerate([t.a,t.b]):query(c,'COMMIT'if index==winner else'ROLLBACK')
   # Kernel heap oracle replays only the actual winning TX, so the losing TX's
   # first move must be absent, not partially committed by the lake journal.
   query(t.a,'BEGIN');command(t.a,'exact',a_first if winner==0 else b_first);command(t.a,'exact',a_second if winner==0 else b_second);query(t.a,'COMMIT')
   check('deadlock rollback keeps only winning transaction atomically',rows(),oracle())
  check('immutable original Parquet remains byte-exact before cleanup',digest(r['physical_origin']['path']),r['physical_origin']['sha256'])
  check('no fake parent-level DuckLake table or allocator',scalar(t.a,"SELECT count(*) FROM created_metadata.ducklake_table WHERE table_name='parent'"),0)
  check('all modes preserve independent heap row truth',rows(),oracle())
  closed=diagnostic(t.a);check('all statement/root versions close at TX end',[closed[x]for x in ['transaction_catalogs','transaction_tables','transaction_rows']],[0,0,0])
  check('registered snapshots return identical diagnostic baseline',closed['registered_snapshots'],root_snapshot)
  check('all executed input bytes unchanged',all(digest(n)==x['sha256']for n,x in r['inputs'].items()))
  check('all executable/SDK bytes unchanged',all(digest(n)==h for n,h in r['hash_only_inputs'].items()));check_build(a.build_receipt);r['status']='pass'
 except BaseException as e:r.update(reason=str(e),traceback=traceback.format_exc())
 finally:
  drain_workers()
  if t:
   for c in[t.a,t.b,t.observer]:
    try:
     if not c.closed:query(c,'ROLLBACK')
    except BaseException as e:r.setdefault('rollback_errors',[]).append(str(e))
  if claim_guard:
   try:claim_guard.close_after_actor_rollback();r['typed_claim_released_before_cleanup']=not claim_guard.held
   except BaseException as e:r.update(status='fail',typed_claim_release_error=str(e))
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
