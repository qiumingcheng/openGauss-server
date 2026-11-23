#!/usr/bin/env python3
"""Closed native parent COPY FROM qualification draft, not executed.

Serial RC STDIN text/CSV routes through typed INSERT/journal. Existing query-form
COPY TO is only an affected-path comparison; relation-form COPY TO remains closed.
Current parent contract remains nullable/NULL-default/no CHECK, no direct file/spill.
Default prepare has no Native/SDK/fixture/SQL operations.
"""
import argparse,ast,fcntl,hashlib,io,json,os,re,secrets,shutil,signal,subprocess,sys,time,traceback
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
ROOT=Path(os.getenv('CPP_WORKSPACE','/home/qiumc/data/code/cpp')).resolve()
D=ROOT/'integration/opengauss-native-lake'
MODES=('text','csv','rollback','errors','policy','permissions','closed')
COPY_CANDIDATE='39c860f76cc2a596a6ed45ddffd2476aad9e001862cdf35798a852244e37c38d'
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
            nonce=next((x.split(b'=',1)[1]for x in environment if x.startswith(b'OG_LAKE_PARENT_COPY_TEST_NONCE=')),None)
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
 env=dict(os.environ,OG_LAKE_PARENT_COPY_TEST_PARENT=str(os.getpid()),OG_LAKE_PARENT_COPY_TEST_NONCE=nonce)
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
 p.add_argument('--case-timeout',type=int,default=900);p.add_argument('--_child',action='store_true',help=argparse.SUPPRESS)
 a=p.parse_args()
 if not a.execute:
  print(json.dumps(dict(status='prepared_not_executed',mode=a.mode,candidate_patch=COPY_CANDIDATE,read_candidate_patch=CANDIDATE,Native=0,SDK=0,fixtures=0,
   version='runtime25; retained23 COPY closed despite compiled24; positive installed24/25 only',runtime_version='0.25',allowed_installed_sql=['0.23']if a.mode=='closed'else['0.24','0.25'],native_contract_versions={'read': '0.23', 'insert': '0.23', 'mutation': '0.24', 'copy': '0.24', 'analyze': '0.25'},
   requirements=['Exact official source/binary/SDK and a separately reviewed COPY source-contract; read/INSERT alone cannot authorize COPY',
    'text: native parser rowcount, one same-catalog snapshot, stable per-leaf allocation; actual local leaves remain empty',
    'csv: host header/quotes/column reorder; omitted column receives existing NULL default; query-form TO matches independent heap',
    'rollback: savepoint and whole-transaction COPY/heap atomic rollback, original snapshot/view counts restored',
    'errors: actual bad input line2/column plus out-of-range routing rollback; same backend can COPY after rollback; no ignored bad row',
    'policy: exactly positive per-leaf limit accepted, one over limit and limit0 rejected atomically; no direct file path or row-ID leak',
    'permissions: original column INSERT ACL, no implicit SELECT/metadata grant; rejected input not consumed',
    'closed: runtime25 with installed23 rejects even empty STDIN despite compiled COPY24 and preserves source/heap/catalog',
    'Unsupported DEFAULT/NOTNULL/CHECK positive support remains outside the existing parent writer contract; no product relaxation' ]),indent=2));return 0
 if not all([a.run_id,a.sql_version,a.build_receipt,a.sdk_qualification,a.sdk_index_sha256,a.source_contract]):p.error('execute requires run-id, explicit SQL, official build/SDK and reviewed source-contract')
 if a.sql_version not in (('0.23',) if a.mode=='closed' else ('0.24','0.25')):
  p.error('COPY closed uses installed23; positive modes use installed24/25; all require reviewed runtime25')
 if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,89}',a.run_id):p.error('run-id must be a safe single component, maximum90')
 if not 300<=a.case_timeout<=1800:p.error('case-timeout must be300..1800')
 if not a._child:return supervise(a)
 if os.environ.get('OG_LAKE_PARENT_COPY_TEST_PARENT')!=str(os.getppid()):p.error('Unsupervised execute child')
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
 r=dict(status='fail',mode=a.mode,scope=__doc__,assertions=[],executed=False,installed_sql=a.sql_version,required_runtime='0.25')
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
  check('reviewed source contract purpose',contract.get('purpose'),'native_parent_copy_qualification')
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
  check('reviewed exact independent fusion guard',contract.get('fusion_guard_patch_sha256'),FUSION_GUARD)
  check('reviewed exact bounded INSERT candidate',contract.get('insert_candidate_patch_sha256'),INSERT_CANDIDATE)
  check('reviewed eager leaf-view lifecycle delta',contract.get('cursor_owner_patch_sha256'),CURSOR_OWNER_FIX)
  check('explicit bounded INSERT capability reviewed',contract.get('bounded_insert_qualified_to_test'),True)
  check('reviewed exact closed COPY candidate',contract.get('copy_candidate_patch_sha256'),COPY_CANDIDATE)
  check('explicit COPY permission to test',contract.get('copy_qualified_to_test'),a.mode!='closed')
  check('explicit root activation review',contract.get('ready_for_native_qualification'),True);check('final activation blockers empty',contract.get('known_blockers'),[])
  if not contract.get('review_evidence')or not contract.get('native_inputs')or not contract.get('shared_helper_inputs'):raise AssertionError('missing reviewed exact source/helper/evidence contract')
  required={'contrib/og_lake/'+x for x in ['lake_native_partition.cpp','lake_native_partition.h','lake_native_partition_bind.cpp','lake_native_partition_scan.cpp','lake_catalog.cpp','lake_acl.cpp','lake_txn.cpp','og_lake.cpp','Makefile','og_lake.control']}
  required|={'src/'+x for x in ['include/access/og_lake.h','include/utils/rel.h','gausskernel/runtime/executor/nodePartIterator.cpp','gausskernel/runtime/executor/nodeForeignscan.cpp','gausskernel/optimizer/path/allpaths.cpp','gausskernel/optimizer/plan/createplan.cpp','gausskernel/optimizer/util/plancat.cpp','gausskernel/optimizer/commands/copy.cpp','gausskernel/optimizer/commands/tablecmds.cpp','gausskernel/storage/access/common/reloptions.cpp','common/backend/catalog/heap.cpp']}
  required.add('contrib/og_lake/og_lake--'+a.sql_version+'.sql')
  required|={'contrib/og_lake/'+x for x in ['lake_row_lock.cpp','lake_row_lock.h','lake_native_partition_write.cpp','og_lake--0.22--0.23.sql']}
  required|={'src/'+x for x in ['gausskernel/optimizer/commands/analyze.cpp','gausskernel/optimizer/commands/vacuum.cpp','gausskernel/optimizer/commands/indexcmds.cpp','gausskernel/optimizer/commands/cluster.cpp','common/backend/catalog/index.cpp','gausskernel/process/tcop/utility.cpp']}
  required|={'src/gausskernel/runtime/opfusion/opfusion.cpp','src/gausskernel/runtime/opfusion/opfusion_util.cpp'}
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
  t=Case(db,SimpleNamespace(mode='net_dml',sql_version=a.sql_version,manifest=out/'fixture/manifest.json',output=out/'result.json',run_id=a.run_id),r,roles)
  for c in[t.a,t.b,t.observer]:configure(c)
  check('exact runtime module',json.loads(scalar(t.a,'SELECT og_lake.host_status()'))['module_version'],contract['runtime_version'])
  r['runtime_module_version']=r['assertions'][-1]['observed']
  query(t.a,"CREATE FUNCTION og_lake_internal.parent_test_xact_state() RETURNS text AS '$libdir/og_lake','og_lake_test_xact_state' LANGUAGE C NOT FENCED")
  check('active SQL23 row-head map has exactly eighteen columns',scalar(t.a,"SELECT count(*) FROM pg_attribute WHERE attrelid='og_lake_internal.row_lock_binding'::regclass AND attnum>0 AND NOT attisdropped"),18)
  check('leaf-domain columns retain no temporary defaults',scalar(t.a,"SELECT count(*) FROM pg_attrdef WHERE adrelid='og_lake_internal.row_lock_binding'::regclass AND adnum IN (17,18)"),0)
  check('active SQL23 preserves twelve-column fixed-H table binding',scalar(t.a,"SELECT count(*) FROM pg_attribute WHERE attrelid='og_lake_internal.table_binding'::regclass AND attnum>0 AND NOT attisdropped"),12)
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
  before=t.head();bind(pid,leaves,ids);check('binding allocates no standard lake snapshot/frontier',t.head(),before)
  check('controlled marker persists',marker(pid),True);bindings=maps(pid)
  check('two complete typed native bindings',len(bindings),2);check('exact real leaf OIDs',[x[0]for x in bindings],sorted(leaves))
  check('both bindings readonly/partition',[(x[4],x[5])for x in bindings],[('partition',True)]*2)
  check('both bindings exact certified head',[x[2]for x in bindings],[before[0]]*2)
  check('source identity digest root syntax',all(re.fullmatch('[0-9a-f]{64}',x[3])for x in bindings))
  check('native parent never has ordinary relation_binding',scalar(t.a,'SELECT count(*) FROM og_lake_internal.relation_binding WHERE relation_oid=%s',(pid,)),0)
  check('no synthetic pg_class leaves after bind',scalar(t.a,'SELECT count(*) FROM pg_class WHERE oid=ANY(%s::oid[])',(leaves,)),0)
  check('native parent data vs original heap oracle',rows(),oracle())
  query(t.a,'SELECT og_lake.enable_native_partition_insert(%s::regclass)',(pid,))
  check('both leaves promoted before COPY',[(x[4],x[5])for x in maps(pid)],[('partition',False)]*2)
  class TrackedInput(io.StringIO):
   def __init__(self,payload):super().__init__(payload);self.read_calls=0
   def read(self,*args):self.read_calls+=1;return super().read(*args)
  r['copies']=[]
  def copy(payload,columns='id,val',options='FORMAT text',c=None,label='copy',want=None):
   stream=TrackedInput(payload);statement='COPY biz.parent('+columns+') FROM STDIN WITH ('+options+')'
   record=dict(label=label,statement=statement,payload_sha256=hashlib.sha256(payload.encode()).hexdigest(),payload_bytes=len(payload.encode()))
   r['copies'].append(record)
   try:
    with(c or t.a).cursor()as cursor:
     cursor.copy_expert(statement,stream);record.update(statusmessage=cursor.statusmessage,rowcount=cursor.rowcount,read_calls=stream.read_calls)
     if want is not None:check(label+' native COPY command rowcount',cursor.rowcount,want)
   except Exception as error:
    record.update(sqlstate=getattr(error,'pgcode',None),message_primary=getattr(getattr(error,'diag',None),'message_primary',None),context=getattr(getattr(error,'diag',None),'context',None),read_calls=stream.read_calls)
    raise
   return record
  def heap_add(values,c=None):
   with(c or t.a).cursor()as cursor:cursor.executemany('INSERT INTO biz.exact VALUES(%s,%s)',values)
  def own_heap():return query(t.a,'SELECT id,val FROM biz.exact ORDER BY id,val NULLS FIRST')
  def frontiers():return query(t.observer,'SELECT table_id,record_count,next_row_id,file_size_bytes FROM created_metadata.ducklake_table_stats WHERE table_id=ANY(%s::bigint[])ORDER BY table_id',([low,high],))
  def full_state():return dict(base=state(),frontiers=frontiers(),oracle=oracle(),heads=query(t.observer,'SELECT * FROM og_lake_internal.row_lock_binding ORDER BY binding_id'))
  def resource_checkpoint(label,before,row_delta=0,journals=None):
   # First same-backend SQL after COPY or ROLLBACK TO. The diagnostic SELECT
   # has the same host snapshot overhead as its in-BEGIN baseline. RC journals
   # do not own statement snapshots; live row journals must remain staged.
   current=diagnostic(t.a)
   r.setdefault('in_transaction_resource_checkpoints',[]).append(deepcopy(dict(
    label=label,baseline=before,observed=current,expected_row_delta=row_delta,
    expected_journals=journals,read_view_counter_exported=False)))
   check(label+' registered snapshot references before top-level cleanup',current['registered_snapshots'],before['registered_snapshots'])
   check(label+' live journal row count retained/restored',current['transaction_rows'],before['transaction_rows']+row_delta)
   if journals is not None:check(label+' canonical journal catalog/table counts',
    [current['transaction_catalogs'],current['transaction_tables']],journals)
   return current
  def local_leaf_bytes():return query(t.observer,"SELECT oid,pg_partition_size(%s::oid,oid) FROM pg_partition WHERE parentid=%s AND parttype='p' ORDER BY oid",(pid,pid))
  original_local=local_leaf_bytes();original_files=query(t.observer,'SELECT * FROM created_metadata.ducklake_data_file ORDER BY data_file_id')
  # Empty commands still initialize and close one COPY state; no lake snapshot.
  original=full_state();baseline=diagnostic(t.a)
  if a.mode=='closed':
   try:copy('',label='closed-empty')
   except Exception as error:check('empty COPY closed gate0A000',getattr(error,'pgcode',None),'0A000')
   else:raise AssertionError('empty COPY bypassed independent closed gate')
   check('closed COPY sends no input bytes',r['copies'][-1]['read_calls'],0)
   check('closed COPY leaves all state unchanged',full_state(),original)
  else:
   copy('',label='empty',want=0);check('empty COPY publishes no snapshot/identity',full_state(),original)
   check('empty COPY releases its pinned snapshot/views',diagnostic(t.a)['registered_snapshots'],baseline['registered_snapshots'])
   if a.mode=='text':
    old_head=t.head()[0];old_front=frontiers();copy('3\t333\n11\t1111\n',want=2);heap_add([(3,333),(11,1111)])
    check('COPY two changed leaves uses one winning snapshot',t.head()[0],old_head+1)
    check('COPY original DML values equal independent ordinary heap',rows(),oracle())
    query(t.a,'CREATE TABLE biz.copy_heap(id integer,val integer)')
    with t.a.cursor()as ordinary:ordinary.copy_expert('COPY biz.copy_heap FROM STDIN',io.StringIO('3\t333\n11\t1111\n'))
    check('ordinary heap COPY remains exact native host path',query(t.a,'SELECT * FROM biz.copy_heap ORDER BY id'),[(3,333),(11,1111)])
    now=frontiers();check('COPY each table allocator advances exactly one',[x[2]-y[2]for x,y in zip(now,old_front)],[1,1])
   elif a.mode=='csv':
    copy('val,id\n"333","3"\n,"11"\n',columns='val,id',options='FORMAT csv, HEADER true',want=2);heap_add([(3,333),(11,None)])
    copy('4\n12\n',columns='id',want=2);heap_add([(4,None),(12,None)])
    check('host column reorder, CSV NULL and omitted NULL default',rows(),oracle())
    actual=io.StringIO();reference=io.StringIO()
    with t.a.cursor()as c:c.copy_expert('COPY (SELECT id,val FROM biz.parent ORDER BY id,val NULLS FIRST) TO STDOUT WITH (FORMAT csv)',actual)
    with t.observer.cursor()as c:c.copy_expert('COPY (SELECT id,val FROM biz.exact ORDER BY id,val NULLS FIRST) TO STDOUT WITH (FORMAT csv)',reference)
    check('existing query-form COPY TO uses actual lake executor, matches independent heap',actual.getvalue(),reference.getvalue())
   elif a.mode=='rollback':
    old=full_state();query(t.a,'BEGIN');copy_txn_baseline=diagnostic(t.a)
    query(t.a,'SAVEPOINT copy_child');query(t.a,'INSERT INTO biz.heap_marker VALUES(99)')
    copy('3\t333\n11\t1111\n',want=2)
    resource_checkpoint('successful child COPY before any subsequent lake SELECT',copy_txn_baseline,2,[1,2])
    heap_add([(3,333),(11,1111)])
    check('same transaction COPY sees exact own overlay once',rows(),own_heap())
    query(t.a,'ROLLBACK TO copy_child')
    resource_checkpoint('ROLLBACK TO child before RELEASE or COMMIT',copy_txn_baseline,0,
     [copy_txn_baseline['transaction_catalogs'],copy_txn_baseline['transaction_tables']])
    query(t.a,'RELEASE copy_child');query(t.a,'SAVEPOINT copy_reuse');reuse_baseline=diagnostic(t.a)
    copy('3\t333\n11\t1111\n',label='same-transaction-after-subabort',want=2)
    resource_checkpoint('COPY reuse while original outer transaction is still open',reuse_baseline,2,[1,2])
    heap_add([(3,333),(11,1111)]);check('same outer transaction COPY and SELECT work after subabort',rows(),own_heap())
    query(t.a,'ROLLBACK TO copy_reuse')
    resource_checkpoint('second ROLLBACK TO before outer COMMIT',reuse_baseline,0,
     [reuse_baseline['transaction_catalogs'],reuse_baseline['transaction_tables']])
    query(t.a,'RELEASE copy_reuse');query(t.a,'COMMIT');check('savepoint COPY and ordinary heap roll back atomically',full_state(),old)
    query(t.a,'BEGIN');whole_txn_baseline=diagnostic(t.a);query(t.a,'INSERT INTO biz.heap_marker VALUES(100)')
    copy('4\t444\n12\t1212\n',want=2)
    resource_checkpoint('successful outer COPY before outer ROLLBACK',whole_txn_baseline,2,[1,2])
    query(t.a,'ROLLBACK')
    check('whole COPY transaction leaves allocator/catalog/files unchanged',full_state(),old)
   elif a.mode=='errors':
    old=full_state();query(t.a,'BEGIN');error_txn_baseline=diagnostic(t.a)
    query(t.a,'SAVEPOINT bad_copy');query(t.a,'INSERT INTO biz.heap_marker VALUES(99)')
    try:copy('3\t333\noops\t444\n',label='bad-second-line')
    except Exception as error:
     check('bad second input row reports original native parser state',getattr(error,'pgcode',None),'22P02')
     check('native parser error retains COPY line2 context',bool(re.search(r'line 2(?:,|:|\b)',getattr(getattr(error,'diag',None),'context','')or'')))
    else:raise AssertionError('invalid COPY input row accepted')
    query(t.a,'ROLLBACK TO bad_copy')
    resource_checkpoint('parser ERROR subabort before outer COMMIT',error_txn_baseline,0,
     [error_txn_baseline['transaction_catalogs'],error_txn_baseline['transaction_tables']])
    check('same outer transaction SELECT works after parser ERROR',rows(),own_heap())
    query(t.a,'RELEASE bad_copy');query(t.a,'COMMIT');check('valid prefix of failed COPY never commits',full_state(),old)
    query(t.a,'BEGIN');route_txn_baseline=diagnostic(t.a);query(t.a,'SAVEPOINT bad_route')
    try:copy('3\t333\n20\t2000\n',label='out-of-range')
    except Exception as error:check('out-of-range native route state',getattr(error,'pgcode',None),'P0002')
    else:raise AssertionError('COPY native range boundary accepted')
    query(t.a,'ROLLBACK TO bad_route')
    resource_checkpoint('routing ERROR subabort before outer COMMIT',route_txn_baseline,0,
     [route_txn_baseline['transaction_catalogs'],route_txn_baseline['transaction_tables']])
    check('same outer transaction SELECT works after routing ERROR',rows(),own_heap())
    query(t.a,'RELEASE bad_route');query(t.a,'COMMIT');check('failed route prefix also rolls back',full_state(),old)
    for option in['FORMAT binary','FORMAT fixed','FREEZE true','ignore_extra_data true','log_errors true','reject_limit 1','loader true']:
     try:copy('3\t333\n',options=option,label='unsupported/'+option)
     except Exception as error:check('unsupported COPY combination rejects0A000/'+option,getattr(error,'pgcode',None),'0A000')
     else:raise AssertionError('unsupported COPY combination accepted')
     check('unsupported option input was not consumed/'+option,r['copies'][-1]['read_calls'],0)
    copy('3\t333\n11\t1111\n',label='same-backend-reuse',want=2);heap_add([(3,333),(11,1111)]);check('same backend works after parser/routing ERROR cleanup',rows(),oracle())
   elif a.mode=='policy':
    query(t.a,"SELECT og_lake.set_option('created_lake'::name,'data_inlining_row_limit','2')")
    copy('3\t333\n4\t444\n11\t1111\n12\t1212\n',label='exact-positive-per-leaf-limit',want=4);heap_add([(3,333),(4,444),(11,1111),(12,1212)])
    check('exact positive limit retains original values',rows(),oracle());old=full_state()
    query(t.a,'BEGIN');query(t.a,'INSERT INTO biz.heap_marker VALUES(99)');copy('5\t555\n6\t666\n7\t777\n',label='over-limit-staged',want=3)
    try:query(t.a,'COMMIT')
    except Exception as error:check('over-limit commit exact0A000',getattr(error,'pgcode',None),'0A000')
    else:raise AssertionError('COPY over per-leaf inline limit committed')
    query(t.a,'ROLLBACK');check('over-limit whole host transaction rolls back allocator and heap',full_state(),old)
    query(t.a,"SELECT og_lake.set_option('created_lake'::name,'data_inlining_row_limit','0')");old=full_state()
    try:copy('',label='zero-limit-empty')
    except Exception as error:check('empty COPY also enforces positive limit',getattr(error,'pgcode',None),'0A000')
    else:raise AssertionError('empty COPY bypassed zero limit')
    check('zero limit produces no file or identity side effect',full_state(),old)
   elif a.mode=='permissions':
    role=roles.create('copy_writer');user,identity=roles.connect(db,'copy_writer');configure(user);r['ordinary_login']=identity
    query(t.a,sql.SQL('GRANT USAGE ON SCHEMA biz TO {}').format(sql.Identifier(role)))
    try:copy('3\t333\n',c=user,label='denied-no-INSERT')
    except Exception as error:check('original COPY permission denied',getattr(error,'pgcode',None),'42501')
    else:raise AssertionError('COPY without column INSERT granted')
    check('denied COPY does not consume input',r['copies'][-1]['read_calls'],0)
    query(t.a,sql.SQL('GRANT INSERT(id) ON biz.parent TO {}').format(sql.Identifier(role)))
    copy('3\n11\n',columns='id',c=user,label='column-INSERT-only',want=2);heap_add([(3,None),(11,None)])
    check('column INSERT-only route needs no SELECT/private metadata grants',rows(),oracle())
  # Relation form is deliberately still not supported by this FROM slice.
  try:
   with t.a.cursor()as c:c.copy_expert('COPY biz.parent TO STDOUT',io.StringIO())
  except Exception as error:check('relation COPY TO remains explicit0A000',getattr(error,'pgcode',None),'0A000')
  else:raise AssertionError('relation COPY TO read empty native heap')
  check('all original physical local leaves remain unchanged',local_leaf_bytes(),original_local)
  check('bounded parent COPY never creates data files',query(t.observer,'SELECT * FROM created_metadata.ducklake_data_file ORDER BY data_file_id'),original_files)
  closed=diagnostic(t.a);check('COPY releases all registered snapshot references',closed['registered_snapshots'],baseline['registered_snapshots'])
  check('COPY final host transaction journals drained',[closed[x]for x in['transaction_catalogs','transaction_tables','transaction_rows']],[0,0,0])
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
