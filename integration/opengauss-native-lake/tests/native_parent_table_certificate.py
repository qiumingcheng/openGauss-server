#!/usr/bin/env python3
"""Real source-scoped certificate QA; prepare performs no Native/SDK work.

New runtime0.25 + explicit installed24/25 only (legacy mode starts real23 then
upgrades). Reuses original strict mutation supervisor/admission/archive/cleanup;
all source histories come from real Native or pinned DuckLake transactions.
No production external-writer connector or metadata re-anchor is implemented.
"""
import argparse,ast,fcntl,hashlib,json,os,re,secrets,shutil,signal,subprocess,sys,time,traceback
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
ROOT=Path(os.getenv('CPP_WORKSPACE','/home/qiumc/data/code/cpp')).resolve()
D=ROOT/'integration/opengauss-native-lake'
MODES=('two-parents-other-table', 'unrelated-DDL-and-first-carrier', 'overlapping-parent-publish', 'own-claim-old-C', 'cursor-fixed-S', 'same-target-negative', 'history-and-namespace-negative', 'rollback-and-pair-CAS', 'bounded-catchup', 'legacy-and-permissions')
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
 p.add_argument('--case-timeout',type=int,default=1200);p.add_argument('--_child',action='store_true',help=argparse.SUPPRESS)
 a=p.parse_args()
 if not a.execute:
  print(json.dumps(dict(status='prepared_not_executed',mode=a.mode,runtime_version='0.25',allowed_installed_sql=['0.24','0.25'],
   initial_installed_sql='0.23'if a.mode=='legacy-and-permissions'else'--sql-version',
   certificate_patch_sha256='899fa0cd9ff9fa9c71abf7239295ac538902362302845ec89ee19ecc377f6174',
   native_contract_versions={'read':'0.23','insert':'0.23','mutation':'0.24','copy':'0.24','analyze':'0.25'},
   requirements=['Root-reviewed official runtime25 build, installed24/25 explicit and same exact SDK6ce/index4dd provider',
    'Each case independent genuine EMPTY root, four real file-only source tables, two actual native parents and independent native heap tapes',
    'No generated snapshot IDs/changesets or carrier DELETE; real producer histories are identity-preserving offline replay in dedicated owned test DB only',
    'Actual claim/cancel waits, original fixed cursor, transaction/savepoint pair rollback and ordinary role checks',
    'Bounded-catchup tests real4097-transaction refusal only; re-anchor/expiry, missing/malformed history and deterministic stale-token retry counters remain separate unimplemented or untested boundaries'],
   deadline_seconds=a.case_timeout,Native=0,SDK=0,fixtures=0),indent=2));return 0
 if not all([a.run_id,a.sql_version,a.build_receipt,a.sdk_qualification,a.sdk_index_sha256,a.source_contract]):p.error('execute requires run-id, explicit SQL, official build/SDK and reviewed source-contract')
 if a.sql_version not in ('0.24','0.25'):
  p.error('Certificate QA requires explicit installed SQL0.24/0.25 on reviewed runtime0.25; legacy starts23 and upgrades normally')
 if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,89}',a.run_id):p.error('run-id must be a safe single component, maximum90')
 if not 300<=a.case_timeout<=1800:p.error('case-timeout must be300..1800')
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
 r=dict(status='fail',mode=a.mode,scope=__doc__,assertions=[],executed=False,installed_sql=a.sql_version,initial_installed_sql='0.23'if a.mode=='legacy-and-permissions'else a.sql_version,required_runtime='0.25')
 t=db=control=roles=reservation=None
 workers=[];holders=[];payload='note'if a.mode=='unrelated-DDL-and-first-carrier'else'payload'
 def check(label,got,want=True):
  got,want=image(got),image(want);ok=got==want
  r['assertions'].append(deepcopy(dict(name=label,status='pass'if ok else'fail',observed=got,expected=want)));write_json(out/'result.json',r)
  if not ok:raise AssertionError(label)
 def configure(c):
  for statement in ["SET default_transaction_isolation='read committed'",'SET session_timeout=0','SET statement_timeout=60000','SET query_dop=1','SET enable_lake_smp=off',
                    'SET enable_vector_engine=off','SET enable_default_ustore_table=off']:query(c,statement)
 def reject(c,statement,state='0A000',args=None,primary=None):
  try:query(c,statement,args)
  except Exception as e:
   check('reject '+str(statement)[:84]+' SQLSTATE',getattr(e,'pgcode',None),state)
   actual=getattr(getattr(e,'diag',None),'message_primary',None) or str(e)
   if primary is not None:check('reject exact certificate reason',actual,primary)
   r.setdefault('rejections',[]).append(deepcopy(dict(statement=str(statement),sqlstate=getattr(e,'pgcode',None),primary=actual)))
   return
  raise AssertionError('unexpected accepted '+str(statement))
 def parent(name):
  query(t.a,sql.SQL('CREATE TABLE biz.{}(shard integer,id integer,val integer,{} text) PARTITION BY RANGE(shard) '
    '(PARTITION low VALUES LESS THAN(10),PARTITION high VALUES LESS THAN(20))').format(sql.Identifier(name),sql.Identifier(payload)))
  oid=scalar(t.a,'SELECT %s::regclass::oid',('biz.'+name,))
  z=query(t.a,"SELECT oid,relname,partitionno FROM pg_partition WHERE parentid=%s AND parttype='p' ORDER BY relname",(oid,))
  check(name+' exactly two native leaves',len(z),2);leaves={n:(po,no)for po,n,no in z}
  check(name+' native low/high leaf set',sorted(leaves),['high','low'])
  check(name+' leaf OIDs belong only to pg_partition',scalar(t.a,'SELECT count(*) FROM pg_class WHERE oid=ANY(%s::oid[])',([x[0]for x in leaves.values()],)),0)
  return oid,[leaves['low'][0],leaves['high'][0]]
 def bind(name,ids):
  oid,leaves=parent(name);head=t.head()
  query(t.a,'SELECT og_lake.bind_native_partition_readonly(%s::regclass,%s::name,%s::oid[],%s::bigint[])',(oid,t.server,leaves,ids))
  check(name+' certification makes no lake snapshot',t.head(),head)
  return oid,leaves
 def maps(name,c=None):
  return query(c or t.observer,'SELECT p.leaf_partition_oid,p.partition_no,p.binding_id::text,p.certified_snapshot,p.source_hash,t.table_id,t.table_uuid::text,t.schema_version,t.read_only '
   'FROM og_lake_internal.partition_binding p JOIN og_lake_internal.table_binding t USING(binding_id) '
   'WHERE p.parent_relid=%s::regclass ORDER BY p.leaf_partition_oid',('biz.'+name,))
 def pair(name,c=None):
  data=maps(name,c);check(name+' exactly two certificate tokens',len(data),2)
  check(name+' sibling certified S agrees',len({x[3]for x in data}),1)
  check(name+' sibling schema anchor agrees',len({x[7]for x in data}),1)
  check(name+' two real table UUID identities',len({(x[5],x[6])for x in data}),2)
  return data
 def rows(name='parent',c=None):
  return query(c or t.a,sql.SQL('SELECT shard,id,val,{} FROM biz.{} ORDER BY shard,id').format(sql.Identifier(payload),sql.Identifier(name)))
 def oracle(name='parent',c=None):return rows('exact'if name=='parent'else'exact_other',c or t.observer)
 def parity(label):
  for name in ['parent','other']:check(label+'/'+name,rows(name),oracle(name))
 def insert(name,values,c=None):
  with(c or t.a).cursor()as cur:
   cur.execute(sql.SQL('INSERT INTO biz.{} VALUES ').format(sql.Identifier(name))+sql.SQL(',').join(sql.SQL('(%s,%s,%s,%s)')for _ in values),sum((tuple(x)for x in values),()))
 def both(name,values,c=None):
  insert(name,values,c);insert('exact'if name=='parent'else'exact_other',values,c)
 def state():return dict(catalog=t.protocol(),parent=maps('parent'),other=maps('other'),heap=query(t.observer,'SELECT * FROM biz.heap_marker ORDER BY id'))
 def diagnostic(c):return json.loads(scalar(c,'SELECT og_lake_internal.parent_test_xact_state()'))
 def history(start,end=None):
  return query(t.observer,'SELECT s.snapshot_id,s.schema_version,c.changes_made::text FROM created_metadata.ducklake_snapshot s JOIN created_metadata.ducklake_snapshot_changes c USING(snapshot_id) '
   'WHERE s.snapshot_id>%s AND s.snapshot_id<=%s ORDER BY s.snapshot_id',(start,t.head()[0]if end is None else end))
 def check_select_pure(name='parent'):
  before=state();answer=rows(name);check('read uses independent literal-backed heap/'+name,answer,oracle(name));check('SELECT never advances certificate or catalog',state(),before)
 def check_refresh(name,winning):
  z=pair(name);check(name+' both tokens refresh to own exact winning S',[x[3]for x in z],[winning]*2)
  g=scalar(t.observer,'SELECT schema_version FROM created_metadata.ducklake_snapshot WHERE snapshot_id=%s',(winning,))
  check(name+' both table binding epochs refresh with same winner',[x[7]for x in z],[g]*2)
 def commit_worker(c,label):
  pid_=scalar(c,'SELECT pg_backend_pid()');app=scalar(c,"SELECT current_setting('application_name')")
  def execute():
   try:query(c,'COMMIT');return dict(sqlstate='00000')
   except Exception as e:return dict(sqlstate=getattr(e,'pgcode',None),primary=getattr(getattr(e,'diag',None),'message_primary',None),message=str(e))
  pool=ThreadPoolExecutor(max_workers=1);item=dict(connection=c,pid=pid_,application_name=app,label=label,pool=pool,future=pool.submit(execute));workers.append(item);return item
 def finish_worker(w,seconds=30):
  reply=w['future'].result(timeout=seconds);w['pool'].shutdown(wait=True);w['drained']=True
  r.setdefault('actual_commits',[]).append(deepcopy(dict(pid=w['pid'],label=w['label'],reply=reply)));return reply
 def owned_cancel(w):
  check('cancel exact owned backend identity',query(t.observer,'SELECT datid,application_name FROM pg_stat_activity WHERE pid=%s',(w['pid'],)),[(db.allocation['database_oid'],w['application_name'])])
  check('cancel delivered only to owned backend',scalar(t.observer,'SELECT pg_cancel_backend(%s)',(w['pid'],)))
 def lock_relation(name):
  if name not in ['created_metadata.ducklake_snapshot','og_lake_internal.partition_binding']:raise ValueError('unreviewed barrier')
  c=db.connect();configure(c);query(c,'BEGIN');query(c,'LOCK TABLE '+name+' IN SHARE MODE');holders.append(c);return c
 def wait_relation(w,name):
  oid=scalar(t.observer,'SELECT %s::regclass::oid',(name,));until=time.monotonic()+15
  while True:
   data=query(t.observer,'SELECT pid,relation,mode,granted FROM pg_locks WHERE database=%s AND relation=%s AND pid=ANY(%s::bigint[]) AND NOT granted ORDER BY pid',(db.allocation['database_oid'],oid,[x['pid']for x in w]))
   if len(data)==len(w)and not any(x['future'].done()for x in w):break
   if any(x['future'].done()for x in w)or time.monotonic()>=until:raise AssertionError('publishers did not reach real relation barrier: '+repr(data))
   time.sleep(.02)
  check('all publishers genuinely wait at '+name,[x[0]for x in data],sorted(x['pid']for x in w))
  r.setdefault('relation_barriers',[]).append(deepcopy(dict(relation=name,oid=oid,locks=data)))
 def wait_holder(w,holder):
  hp=scalar(holder,'SELECT pg_backend_pid()');until=time.monotonic()+15
  while True:
   waits=query(t.observer,"SELECT w.locktype,w.transactionid::text,w.mode,h.mode,w.pid,h.pid FROM pg_locks w JOIN pg_locks h ON h.locktype=w.locktype AND h.transactionid=w.transactionid WHERE w.pid=%s AND h.pid=%s AND NOT w.granted AND h.granted AND w.locktype IN('transactionid','subtransactionid')",(w['pid'],hp))
   if waits and not w['future'].done():break
   if w['future'].done()or time.monotonic()>=until:raise AssertionError('no actual exact holder XID wait: '+repr(waits))
   time.sleep(.02)
  r.setdefault('tuple_barriers',[]).append(deepcopy(dict(waiter=w['pid'],holder=hp,locks=waits)));return waits
 def drain_workers():
  for c in holders:
   try:
    if not c.closed:query(c,'ROLLBACK')
   except BaseException as e:r.setdefault('barrier_release_errors',[]).append(str(e))
  unresolved=[]
  for w in workers:
   if w.get('drained'):continue
   try:
    if not w['future'].done():owned_cancel(w)
    w['future'].result(timeout=5);w['pool'].shutdown(wait=True);w['drained']=True
   except BaseException as e:unresolved.append(dict(pid=w['pid'],reason=str(e)));w['pool'].shutdown(wait=False,cancel_futures=True)
  if unresolved:
   r.update(status='fail',preserve_owned_scene=True,worker_drain_failed=unresolved,no_cleanup_with_active_client_threads=True);write_json(out/'result.json',r);os._exit(2)
 def reference(label):
  from reverse_reference import Reference
  p=out/label;p.mkdir();return Reference(p,json.loads((D/'source-lock.json').read_text()),json.loads((ROOT/'.reference/opengauss-native-lake/build/receipt.json').read_text()))
 def replay_actual(before,actual):
  from empty_catalog_oracle import canonical_shape
  from inline_fixture import replace_catalog_values
  previous={x['table_name']:x for x in before['tables']};check('real reference physical table set unchanged',sorted(x['table_name']for x in actual['tables']),sorted(previous))
  adapted=[]
  for x in actual['tables']:
   old=previous[x['table_name']];check('real standard physical shape/'+x['table_name'],canonical_shape(x),canonical_shape(old))
   adapted.append(dict(x,columns=deepcopy(old['columns']),constraints=deepcopy(old['constraints'])))
  query(t.b,'BEGIN')
  try:replace_catalog_values(t.b,'created_metadata',before,{'tables':adapted});query(t.b,'COMMIT')
  except BaseException:query(t.b,'ROLLBACK');raise
  r.setdefault('actual_reference_replays',[]).append(dict(before_tables=len(before['tables']),after_tables=len(actual['tables']),protocol_rows='unmodified actual producer values; only host physical DDL descriptions adapted'))
 def producer(label,statements,inlining=0):
  from empty_catalog_oracle import transport_native
  from empty_reference_fixture import export_actual_tables
  from reverse_reference import literal
  before=t.export(label+'-native.json');ref=reference(label+'-reference');catalog=ref.output/'source.ducklake';transport_native(ref,catalog,before)
  attach='ATTACH '+literal('ducklake:'+str(catalog))+" AS lake (DUCKLAKE_VERSION '1.1',AUTOMATIC_MIGRATION FALSE,DATA_INLINING_ROW_LIMIT "+str(inlining)+');\n'
  ref.run(attach+statements,label+'-real-transactions');actual={'tables':export_actual_tables(ref,catalog)};save(out/(label+'-reference-export.json'),actual)
  replay_actual(before,actual);return before,actual
 try:
  build=check_build(a.build_receipt);contract=json.loads(a.source_contract.read_text())
  check('reviewed source contract purpose',contract.get('purpose'),'native_parent_table_certificate_qualification')
  check('reviewed exact readonly candidate',contract.get('read_candidate_patch_sha256'),CANDIDATE)
  check('explicit coordinated SQL version',contract.get('partition_sql_version'),a.sql_version)
  check('reviewed runtime is distinct from installed SQL',contract.get('runtime_version'),'0.25')
  check('reviewed exact native capability inheritance',contract.get('native_contract_versions'),{'read': '0.23', 'insert': '0.23', 'mutation': '0.24', 'copy': '0.24', 'analyze': '0.25'})
  check('reviewed combined CPP capability source',contract.get('cpp_capabilities_patch_sha256'),'2cdeb956dc8072d1bfe903f6eaf2442a5880b12aa3ba339f6b8cbc23394ceb42')
  check('reviewed table certificate source',contract.get('certificate_patch_sha256'),'899fa0cd9ff9fa9c71abf7239295ac538902362302845ec89ee19ecc377f6174')
  check('reviewed complete final43 product patch',contract.get('product_composition_patch_sha256'),FINAL_PRODUCT_PATCH)
  check('explicit post-composition publisher context repair',contract.get('publish_context_fix_patch_sha256'),'d1c4384d8a344824d4120f04e7e6ab9dbf77dde51cf047b6112794ad60395cd0')
  check('explicit empty mutation plan repair',contract.get('empty_mutation_fix_patch_sha256'),'f78e4c417a0cae264be1d14849a494657de77fa50bda1b5b867cb542cf8e9037')
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
  check('reviewed complete parent descriptor repair',contract.get('descriptor_repair_patch_sha256'),'dcba5204c133195f97f3fcfe8d9a5278a4922ecddc8bc4611aa4c22deb342e4e')
  check('required compiled complete parent descriptor',contract['native_inputs'].get('contrib/og_lake/lake_txn.cpp'),'50b656d07dcb25d529f73bfb61deefd4a851413f146a678b84372dcb2d0cb9b6')
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
  check('explicit reviewed certificate ready-to-test',contract.get('table_certificate_qualified_to_test'),True)
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
  reference_receipt=ROOT/'.reference/opengauss-native-lake/build/receipt.json';reference_identity=json.loads(reference_receipt.read_text());reference_lock=json.loads((D/'source-lock.json').read_text());reference_binary=ROOT/'.reference/opengauss-native-lake'/reference_lock['reference_build']['build_subdir']/'duckdb'
  check('pinned real reference source-lock',reference_identity['source_lock_hash'],digest(D/'source-lock.json'));check('pinned real reference binary',digest(reference_binary),reference_identity['binary_sha256']);inputs.append(reference_receipt)
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
  r['hash_only_inputs']={str(x):digest(x)for x in[INSTALL/'bin/gaussdb',reference_binary]+[sdk/n for n in files]}
  control=connect_control();configure(control)
  check('automatic maintenance disabled',scalar(control,'SHOW enable_lake_maintenance'),'off');check('dedicated nonpool backend',scalar(control,'SHOW enable_thread_pool'),'off')
  with(out/'fixture.log').open('w')as log:
   subprocess.run([sys.executable,str(D/'tests/catalog_fixture_bundle.py'),'--run-id',a.run_id,'--output',str(out/'fixture/manifest.json')],stdout=log,stderr=subprocess.STDOUT,check=True,timeout=300)
  reservation=(ROOT/'.reference/opengauss-native-lake/.driver.lock').open('a+');fcntl.flock(reservation.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB)
  db=OwnedDatabase(a.run_id,out);db.create();configure(db.admin);roles=OwnedRoles(control,a.run_id,out)
  t=Case(db,SimpleNamespace(mode='net_dml',sql_version=('0.23'if a.mode=='legacy-and-permissions'else a.sql_version),manifest=out/'fixture/manifest.json',output=out/'result.json',run_id=a.run_id),r,roles)
  for c in[t.a,t.b,t.observer]:configure(c)
  check('exact runtime module',json.loads(scalar(t.a,'SELECT og_lake.host_status()'))['module_version'],contract['runtime_version'])
  r['runtime_module_version']=r['assertions'][-1]['observed']
  query(t.a,"CREATE FUNCTION og_lake_internal.parent_test_xact_state() RETURNS text AS '$libdir/og_lake','og_lake_test_xact_state' LANGUAGE C NOT FENCED")
  check('current parent map preserves all18 typed-head columns',scalar(t.a,"SELECT count(*) FROM pg_attribute WHERE attrelid='og_lake_internal.row_lock_binding'::regclass AND attnum>0 AND NOT attisdropped"),18)
  # Every case starts from a genuine EMPTY root, then the pinned producer emits
  # four distinct file-only tables. No source table/snapshot/row IDs are made up.
  source_names=['source_low','source_high','other_low','other_high']
  tape={'source_low':[(1,99,100,'low-湖'),(2,2,200,'middle'),(8,8,None,'tail-湖')],
        'source_high':[(10,99,1000,'high-湖'),(19,19,1900,'upper')],
        'other_low':[(3,3,300,'other-low'),(7,7,700,'other-tail')],
        'other_high':[(12,12,1200,'other-high'),(18,18,1800,'other-upper')]}
  from empty_catalog_oracle import transport_native
  from empty_reference_fixture import export_actual_tables
  from inline_fixture import foreign_table_statement
  from reverse_reference import literal
  before=t.export('bootstrap-native-empty.json');ref=reference('bootstrap-real-producer');catalog=ref.output/'source.ducklake';transport_native(ref,catalog,before)
  setup=[]
  for name in source_names:
   values=','.join('('+','.join(literal(x)for x in row)+')'for row in tape[name])
   setup += ['CREATE TABLE lake.main.'+name+'(shard INTEGER,id INTEGER,val INTEGER,payload VARCHAR);','INSERT INTO lake.main.'+name+' VALUES '+values+';']
  ref.run('ATTACH '+literal('ducklake:'+str(catalog))+" AS lake (DUCKLAKE_VERSION '1.1',AUTOMATIC_MIGRATION FALSE,DATA_INLINING_ROW_LIMIT 0);\nBEGIN;\n"+'\n'.join(setup)+'\nCOMMIT;','four-real-file-only-sources')
  actual={'tables':export_actual_tables(ref,catalog)};save(out/'bootstrap-real-catalog.json',actual)
  check('producer file-only registrations empty',next(x['rows']for x in actual['tables']if x['table_name']=='ducklake_inlined_data_tables'),[])
  replay_actual(before,actual)
  source_ids={n:scalar(t.observer,'SELECT table_id FROM created_metadata.ducklake_table WHERE table_name=%s AND end_snapshot IS NULL',(n,))for n in source_names}
  check('four real distinct table IDs',len(set(source_ids.values())),4)
  for name in source_names:
   query(t.a,foreign_table_statement('biz',name,t.server,source_ids[name],[{'name':n,'type':tp}for n,tp in [('shard','INTEGER'),('id','INTEGER'),('val','INTEGER'),('payload','VARCHAR')]]))
   if payload!='payload':query(t.a,sql.SQL('ALTER FOREIGN TABLE biz.{} RENAME COLUMN payload TO {}').format(sql.Identifier(name),sql.Identifier(payload)))
   query(t.a,'SELECT og_lake.set_option(%s::regclass,%s,%s)',('biz.'+name,'data_inlining_row_limit','100'))
   check('source literal truth/'+name,rows(name),tape[name])
  files={str(p.resolve()):digest(p)for p in Path(t.empty_manifest['data_path']).rglob('*.parquet')}
  check('real four nonempty source files exist',len(files),4);r['immutable_bootstrap_files']=files
  query(t.a,sql.SQL("CREATE FOREIGN TABLE biz.ordinary(shard integer,id integer,val integer,{} text) SERVER {} OPTIONS(binding_mode 'create',lake_schema 'main')").format(sql.Identifier(payload),sql.Identifier(t.server)))
  query(t.a,"SELECT og_lake.set_option('biz.ordinary'::regclass,'data_inlining_row_limit','1000000')")
  insert('ordinary',[(5,5,50,'ordinary-seed')]);ordinary_id=scalar(t.observer,"SELECT table_id FROM created_metadata.ducklake_table WHERE table_name='ordinary' AND end_snapshot IS NULL")
  check('ordinary foreign U is a fifth genuine table',ordinary_id not in source_ids.values())
  for name,srcs in [('exact',['source_low','source_high']),('exact_other',['other_low','other_high'])]:
   parent(name);insert(name,sorted(tape[srcs[0]]+tape[srcs[1]]))
  initial_protocol=t.protocol()
  for name in source_names:query(t.a,sql.SQL('DROP FOREIGN TABLE biz.{}').format(sql.Identifier(name)))
  check('detaching host aliases preserves all actual producer data',t.protocol(),initial_protocol)
  pid,leaves=bind('parent',[source_ids['source_low'],source_ids['source_high']]);other_pid,other_leaves=bind('other',[source_ids['other_low'],source_ids['other_high']])
  check('two actual distinct native roots',pid!=other_pid)
  check('four true leaves are distinct',len(set(leaves+other_leaves)),4)
  common=t.head();initial_a=pair('parent');initial_b=pair('other')
  check('four leaves share real certified catalog S',[x[3]for x in initial_a+initial_b],[common[0]]*4)
  check('source-specific schema epochs precede their global certification epoch',all(scalar(t.observer,'SELECT COALESCE(max(schema_version),0) FROM created_metadata.ducklake_schema_versions WHERE table_id=%s',(tid,))<=common[1]for tid in source_ids.values()))
  for oid in [pid,other_pid]:query(t.a,'SELECT og_lake.enable_native_partition_insert(%s::regclass)',(oid,))
  check('promotion retains both read views',rows(),oracle());parity('initial two-parent independent heap truth')
  root_snapshot=diagnostic(t.a)['registered_snapshots'];r['promoted_initial_a']=maps('parent');r['promoted_initial_b']=maps('other');r['initial_certificate_pairs']=deepcopy(dict(parent=pair('parent'),other=pair('other')))
  if a.mode=='two-parents-other-table':
   c0=t.head()[0];both('parent',[(4,40,400,'A-new')]);s1=t.head()[0]
   check('parent A commits one actual snapshot',s1,c0+1);check_refresh('parent',s1)
   check('A publication does not alter B tokens',maps('other'),r['promoted_initial_b'])
   check_select_pure('other');both('other',[(14,140,1400,'B-new')]);s2=t.head()[0];check_refresh('other',s2)
   check('B winning S is subsequent real commit',s2,s1+1);check_select_pure('parent')
   saved_a=maps('parent');saved_b=maps('other');insert('ordinary',[(6,6,60,'U-new')]);query(t.a,'UPDATE biz.ordinary SET val=val+1 WHERE id=5')
   check('ordinary table transaction history advances only U',len(history(s2)),2)
   check('ordinary commit does not rewrite parent anchors',(maps('parent'),maps('other')),(saved_a,saved_b));check_select_pure('parent');check_select_pure('other')
   r['disjoint_history']=history(c0)
  elif a.mode=='unrelated-DDL-and-first-carrier':
   epochs={tid:scalar(t.observer,'SELECT COALESCE(max(schema_version),0) FROM created_metadata.ducklake_schema_versions WHERE table_id=%s',(tid,))for tid in source_ids.values()}
   check('A has no inline registration before first business write',query(t.observer,'SELECT table_id FROM created_metadata.ducklake_inlined_data_tables WHERE table_id=ANY(%s::bigint[])',([source_ids['source_low'],source_ids['source_high']],)),[])
   query(t.a,sql.SQL("CREATE FOREIGN TABLE biz.unrelated(id integer) SERVER {} OPTIONS(binding_mode 'create',lake_schema 'main')").format(sql.Identifier(t.server)))
   check('real unrelatedDDL advances global schema beyond A local epochs',t.head()[1]>max(epochs[source_ids[n]]for n in ['source_low','source_high']))
   check_select_pure();c0=t.head()[0];both('parent',[(6,60,600,'first-carrier-low'),(16,160,1600,'first-carrier-high')]);s=t.head()[0];check('first two-leaf inline commit uses one snapshot',s,c0+1);check_refresh('parent',s)
   registrations=query(t.observer,'SELECT table_id,table_name,schema_version FROM created_metadata.ducklake_inlined_data_tables WHERE table_id=ANY(%s::bigint[]) ORDER BY table_id',([source_ids['source_low'],source_ids['source_high']],))
   check('two actual first carriers',len(registrations),2)
   for tid,name,version in registrations:
    check('carrier takes table-specific schema epoch/'+str(tid),version,epochs[tid])
    cols=query(t.observer,"SELECT attname FROM pg_attribute WHERE attrelid=%s::regclass AND attnum>0 AND NOT attisdropped ORDER BY attnum",('created_metadata.'+name,))
    check('new carrier physical renamed business columns/'+str(tid),cols[-4:],[('shard',),('id',),('val',),(payload,)])
   fresh=db.connect();configure(fresh);check('reconnected first-carrier read keeps stable fieldID mapping',rows(c=fresh),oracle())
   query(fresh,'UPDATE biz.parent SET val=val+1 WHERE shard=6');query(t.a,'UPDATE biz.exact SET val=val+1 WHERE shard=6');parity('first-carrier followup native UPDATE')
   r['table_local_carrier_epochs']=deepcopy(registrations)
  elif a.mode=='overlapping-parent-publish':
   holder=lock_relation('created_metadata.ducklake_snapshot');before=t.head()[0]
   for c in [t.a,t.b]:query(c,'BEGIN')
   both('parent',[(4,40,400,'A-contender')],t.a);both('other',[(14,140,1400,'B-contender')],t.b)
   first=commit_worker(t.a,'parent-A');second=commit_worker(t.b,'parent-B');wait_relation([first,second],'created_metadata.ducklake_snapshot')
   check('before releasing real claim barrier all published pairs unchanged',(maps('parent'),maps('other')),(r['promoted_initial_a'],r['promoted_initial_b']))
   query(holder,'ROLLBACK');check('disjoint parent claim contenders both commit',[finish_worker(x)['sqlstate']for x in [first,second]],['00000','00000'])
   check('exactly two publication snapshots won',t.head()[0],before+2)
   anchors=[pair(x)[0][3]for x in ['parent','other']];check('each winner atomically owns one of the actual two S',sorted(anchors),[before+1,before+2])
   for name,s in zip(['parent','other'],anchors):check_refresh(name,s)
   check_select_pure('parent');check_select_pure('other');r['actual_claim_history']=history(before)
   r['observation_limit']='Both actual publishers wait at the real snapshot table before claim and both complete distinct commits. No new attempt counter is claimed; exact losing-PK retry count needs a dedicated future diagnostic.'
  elif a.mode=='own-claim-old-C':
   old=pair('parent')[0][3]
   for i in range(3):insert('ordinary',[(6,100+i,i,'U-before-A')])
   query(t.a,'BEGIN');both('parent',[(4,40,400,'own-low'),(14,140,1400,'own-high')],t.a);insert('ordinary',[(7,200,200,'U-after-A-stage')],t.b)
   preceding=t.head()[0];check('actual publication predecessor is newer than saved C',preceding>old);query(t.a,'COMMIT');check('own claim wins Q from current predecessor P',t.head()[0],preceding+1);check_refresh('parent',preceding+1)
   r['own_claim_identity']=dict(C=old,P=preceding,Q=t.head()[0],contract='source is read at P=Q-1 by production own-claim binding; the QA does not fabricate a source or snapshot identity')
   check_select_pure();check_select_pure('other')
  elif a.mode=='cursor-fixed-S':
   statement=sql.SQL('SELECT shard,id,val,{} FROM biz.parent').format(sql.Identifier(payload))
   raw_plan=scalar(t.a,sql.SQL('EXPLAIN(FORMAT JSON,COSTS false) ')+statement);plan=json.loads(raw_plan)if isinstance(raw_plan,str)else raw_plan
   def nodes(x):
    if isinstance(x,dict):
     if 'Node Type'in x:yield x
     for v in x.values():yield from nodes(v)
    elif isinstance(x,list):
     for v in x:yield from nodes(v)
   allnodes=list(nodes(plan));save(out/'cursor-plan.json',plan)
   check('cursor uses genuine native parent reader',sum(x.get('Native Lake Parent')=='certified readonly native RANGE leaves'for x in allnodes),1)
   check('cursor uses real native partition iterator',sum(x.get('Node Type')=='Partition Iterator'for x in allnodes),1)
   check('cursor has no preconsuming sort/materialize/aggregate',any(any(word in str(x.get('Node Type',''))for word in ['Sort','Materialize','Aggregate','Stream'])for x in allnodes),False)
   old=oracle();query(t.a,'BEGIN');query(t.a,sql.SQL('DECLARE cert_rows NO SCROLL CURSOR FOR ')+statement)
   prefix=query(t.a,'FETCH 1 FROM cert_rows');check('cursor actually partially consumed',len(prefix),1)
   old_resources=diagnostic(t.a);both('other',[(14,140,1400,'B-during-cursor')],t.b);insert('ordinary',[(6,600,600,'U-during-cursor')],t.b);both('parent',[(4,40,400,'A-during-cursor')],t.b)
   suffix=query(t.a,'FETCH ALL FROM cert_rows');check('fixed old statement snapshot returns exact old rowset',sorted(prefix+suffix),old)
   check('fixed cursor contains no later A committed row',any(x[1]==40 for x in prefix+suffix),False)
   query(t.a,'CLOSE cert_rows');query(t.a,'COMMIT');check('fresh statement after CLOSE sees new parent S',rows(),oracle());r['cursor_live_resources']=old_resources
  elif a.mode in ['same-target-negative','history-and-namespace-negative','bounded-catchup']:
   frozen=state();anchor=pair('parent')[0][3];expected_state='0A000'
   if a.mode=='same-target-negative':
    # The complete certificate protects both table IDs even if native range
    # pruning physically visits only the high leaf. Prove that plan before
    # the real producer mutates ONLY the low source.
    high_only=sql.SQL('SELECT shard,id,val,{} FROM biz.parent WHERE shard>=10 ORDER BY shard,id').format(sql.Identifier(payload))
    raw=scalar(t.a,sql.SQL('EXPLAIN(ANALYZE true,FORMAT JSON,COSTS false) ')+high_only);high_plan=json.loads(raw)if isinstance(raw,str)else raw
    def walk_high(x):
     if isinstance(x,dict):
      yield x
      for value in x.values():yield from walk_high(value)
     elif isinstance(x,list):
      for value in x:yield from walk_high(value)
    high_nodes=list(walk_high(high_plan));write_json(out/'target-negative-high-only-plan.json',high_plan)
    native=[x for x in high_nodes if x.get('Native Lake Parent')=='certified readonly native RANGE leaves']
    check('high-only plan uses exactly one genuine native lake reader',len(native),1)
    check('high-only native pruning selects exactly one physical leaf',[x['Iterations']for x in high_nodes if 'Iterations'in x],[1])
    expected_high=[row for row in oracle()if row[0]>=10]
    check('high-only baseline has actual high data',bool(expected_high))
    check('high-only actual reader rows equal independent high heap rows',query(t.a,high_only),expected_high)
    check('high-only plan actually consumed only the expected high row count',int(native[0]['Actual Rows']),len(expected_high))
    check('high-only reader actually ran once',int(native[0]['Actual Loops']),1)
    check('high-only read/plan changed no catalog or certificate',state(),frozen)
    r['high_only_negative_setup']=dict(plan=str(out/'target-negative-high-only-plan.json'),plan_sha256=digest(out/'target-negative-high-only-plan.json'),expected_high=deepcopy(expected_high),changed_source='source_low',changed_table_id=source_ids['source_low'])
   if a.mode=='same-target-negative':
    program="BEGIN;CALL lake.set_option('data_inlining_row_limit',0,table_name => 'source_low');INSERT INTO lake.main.source_low VALUES(6,600,6000,'actual-external-file-append');COMMIT;";inlining=0
   elif a.mode=='history-and-namespace-negative':program='BEGIN;CREATE SCHEMA lake.unclassified_scope;COMMIT;';inlining=0
   else:
    # Genuine committed producer history; not an invented changeset or a
    # metadata DELETE. Existing ordinary inline U registration is transported.
    program='\n'.join('BEGIN;INSERT INTO lake.main.ordinary VALUES(6,'+str(10000+i)+','+str(i)+",'real-history');COMMIT;"for i in range(4097));inlining=1000000
   before,actual=producer(a.mode,program,inlining)
   if a.mode=='same-target-negative':
    # Explicit table-scoped configuration overrides the transported limit100.
    # This case qualifies external FILE append, not first inline registration.
    old_tables={x['table_name']:x for x in before['tables']};new_tables={x['table_name']:x for x in actual['tables']};target=source_ids['source_low']
    check('actual producer target explicitly disables inlining',[x['value']for x in new_tables['ducklake_metadata']['rows']if x['key']=='data_inlining_row_limit'and x['scope']=='table'and x['scope_id']==target],['0'])
    check('actual file append creates no inline carrier',new_tables['ducklake_inlined_data_tables']['rows'],old_tables['ducklake_inlined_data_tables']['rows'])
    old_files={x['data_file_id']:x for x in old_tables['ducklake_data_file']['rows']};new_files={x['data_file_id']:x for x in new_tables['ducklake_data_file']['rows']}
    check('actual file append preserves every prior file metadata row',{fid:new_files.get(fid)for fid in old_files},old_files)
    added_files=[x for fid,x in new_files.items()if fid not in old_files];check('actual producer appends exactly one data file',len(added_files),1)
    appended=added_files[0];check('actual appended file targets the changed low source',(appended['table_id'],appended['record_count'],appended['file_format'],appended['end_snapshot']),(target,1,'parquet',None))
    check('actual appended file belongs to the one real producer commit',appended['begin_snapshot'],anchor+1)
    r['actual_file_append']=dict(file=deepcopy(appended),configuration='real table-scoped set_option(0) in the same INSERT transaction',scope='External file append certificate rejection; first external inline-carrier registration is not covered or enabled.')
   committed=state();late=history(anchor);check('real producer history contains new committed rows',bool(late))
   snapshots=[x[0]for x in late];check('replayed true history is complete contiguous S',snapshots,list(range(anchor+1,t.head()[0]+1)))
   check('external test replay never alters host native maps',(maps('parent'),maps('other')),(frozen['parent'],frozen['other']))
   if a.mode=='same-target-negative':
    check('actual real changeset identifies target source',any('inserted_into_table:'+str(source_ids['source_low'])in x[2]or'inlined_insert:'+str(source_ids['source_low'])in x[2]for x in late))
    primary='native certified leaf changed outside its atomic publisher'
   elif a.mode=='history-and-namespace-negative':
    check('real unsupported namespace changeset',late[-1][2],'created_schema:"unclassified_scope"');expected_state='40001';primary='native lake write conflicts with an unsupported concurrent catalog or data change'
   else:
    check('actual proof exceeds reviewed4096 history bound',len(late),4097);primary='native certificate history requires bounded maintenance catch-up'
    r['not_implemented']='This mode qualifies only bounded fail-closed refusal. Metadata-only re-anchor/expiry coordination remains unimplemented and has no Native PASS claim.'
   for name in ['parent','other']if a.mode!='same-target-negative'else['parent']:
    reject(t.a,sql.SQL('SELECT * FROM biz.{}').format(sql.Identifier(name)),state=expected_state,primary=primary)
   if a.mode=='same-target-negative':
    reject(t.a,high_only,state='0A000',primary='native certified leaf changed outside its atomic publisher')
   check('failed certificate read writes no catalog or partial certificate',state(),committed)
   r['actual_negative_history']=late;r['offline_replay_scope']='Admin-owned test state replay of genuine producer rows. This is not a production live external-writer API or permission grant.'
  elif a.mode=='rollback-and-pair-CAS':
   before=state();query(t.a,'BEGIN');query(t.a,'SAVEPOINT pair_aborted');both('parent',[(4,40,400,'rollback-low'),(14,140,1400,'rollback-high')],t.a)
   query(t.a,'INSERT INTO biz.heap_marker VALUES(501)');query(t.a,'ROLLBACK TO pair_aborted');query(t.a,'RELEASE pair_aborted');query(t.a,'COMMIT');check('savepoint restores two leaf journals/schema/certificate/heap',state(),before)
   query(t.a,'BEGIN');both('parent',[(4,40,400,'outer-low'),(14,140,1400,'outer-high')],t.a);query(t.a,'ROLLBACK');check('outer rollback restores exact complete before state',state(),before)
   second=pair('parent')[-1];holder=db.connect();configure(holder);holders.append(holder);query(holder,'BEGIN')
   held=query(holder,'SELECT ctid::text,binding_id::text,leaf_partition_oid FROM og_lake_internal.partition_binding WHERE parent_relid=%s AND leaf_partition_oid=%s FOR UPDATE',(pid,second[0]))
   check('real tuple barrier locks only exact second sibling',[x[1:]for x in held],[(second[2],second[0])])
   query(t.a,'BEGIN');both('parent',[(4,40,400,'cancel-low'),(14,140,1400,'cancel-high')],t.a);query(t.a,'INSERT INTO biz.heap_marker VALUES(502)');pending=commit_worker(t.a,'cancel-between-sibling-CAS');wait_holder(pending,holder)
   check('uncommitted publication is invisible at exact second sibling barrier',state(),before)
   owned_cancel(pending);reply=finish_worker(pending,15);check('real postclaim cancellation SQLSTATE',reply['sqlstate'],'57014');query(holder,'ROLLBACK');query(t.a,'ROLLBACK')
   check('ERROR cancels whole source writes plus both cert/schema anchors',state(),before)
   both('parent',[(4,40,400,'recovered-low'),(14,140,1400,'recovered-high')]);check_refresh('parent',t.head()[0]);parity('fresh successful write after cancellation')
   r['pair_CAS_barrier']=dict(second_leaf=second[0],sole_holder_tuple=held,proof='Production numeric sibling order reaches this actual second partition_binding UPDATE after prior sibling CAS; source reviewed separately. No synthetic CAS row or changed mapping value.')
  elif a.mode=='legacy-and-permissions':
   check('legacy test genuinely installed23',scalar(t.a,"SELECT extversion FROM pg_extension WHERE extname='og_lake'"),'0.23')
   legacy=maps('parent');insert('ordinary',[(6,600,600,'legacy-global-head')]);reject(t.a,'SELECT * FROM biz.parent',primary='native partition catalog has changed since certification')
   query(t.a,'BEGIN');query(t.a,'SAVEPOINT upgrade_rolledback');query(t.a,sql.SQL('ALTER EXTENSION og_lake UPDATE TO {}').format(sql.Literal(a.sql_version)));check('selected upgrade sees exact known version',scalar(t.a,"SELECT extversion FROM pg_extension WHERE extname='og_lake'"),a.sql_version);check_select_pure()
   query(t.a,'ROLLBACK TO upgrade_rolledback');query(t.a,'RELEASE upgrade_rolledback');query(t.a,'COMMIT');check('upgrade rollback restores actual installed23',scalar(t.a,"SELECT extversion FROM pg_extension WHERE extname='og_lake'"),'0.23');reject(t.a,'SELECT * FROM biz.parent',primary='native partition catalog has changed since certification');check('legacy refusals keep original anchor',maps('parent'),legacy)
   query(t.a,sql.SQL('ALTER EXTENSION og_lake UPDATE TO {}').format(sql.Literal(a.sql_version)));check_select_pure();check_select_pure('other')
   role=roles.create('cert_reader');user,identity=roles.connect(db,'cert_reader');configure(user);r['ordinary_login']=identity
   query(t.a,sql.SQL('GRANT USAGE ON SCHEMA biz TO {}').format(sql.Identifier(role)));query(t.a,sql.SQL('GRANT SELECT ON biz.parent,biz.other TO {}').format(sql.Identifier(role)))
   check('ordinary read permission uses real certificate',rows(c=user),oracle());reject(user,'INSERT INTO biz.parent VALUES(6,600,600,\'forbidden\')','42501')
   query(t.a,sql.SQL('GRANT INSERT ON biz.parent TO {}').format(sql.Identifier(role)));insert('parent',[(6,600,600,'ordinary-authorized')],user);insert('exact',[(6,600,600,'ordinary-authorized')]);check_refresh('parent',t.head()[0])
   reject(user,'SELECT * FROM og_lake_internal.partition_binding','42501');reject(user,'SELECT * FROM created_metadata.ducklake_snapshot','42501')
  else:raise AssertionError('unimplemented certificate mode')
  if a.mode not in ['same-target-negative','history-and-namespace-negative','bounded-catchup']:parity('final actual parent vs independent native heap')
  check('original immutable source objects remain byte-exact',{p:digest(p)for p in files},files)
  check('no synthetic parent-level standard DuckLake definition',scalar(t.a,"SELECT count(*) FROM created_metadata.ducklake_table WHERE table_name IN('parent','other')"),0)
  closed=diagnostic(t.a);check('all statement/journal state released',[closed[x]for x in ['transaction_catalogs','transaction_tables','transaction_rows']],[0,0,0]);check('registered snapshots restored to same diagnostic baseline',closed['registered_snapshots'],root_snapshot)
  check('all executed input bytes unchanged',all(digest(n)==x['sha256']for n,x in r['inputs'].items()));check('all binary/SDK bytes unchanged',all(digest(n)==h for n,h in r['hash_only_inputs'].items()));check_build(a.build_receipt);r['status']='pass'
 except BaseException as e:r.update(reason=str(e),traceback=traceback.format_exc())
 finally:
  drain_workers()
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
