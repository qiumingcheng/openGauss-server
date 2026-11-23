#!/usr/bin/env python3
"""Private SQL0.25 manual whole-parent ANALYZE qualification; default prepare only.

Four independent modes exercise real native parents against an ordinary heap,
not simulated connections or fabricated catalog rows. Root assigned M0.24/A0.25;
this file never installs SQL, changes sources or opens a capability gate.
No Native/SDK/fixture/database work occurs without --execute and exact receipts.
"""
import argparse,ast,fcntl,hashlib,json,os,re,secrets,shutil,signal,subprocess,sys,time,traceback
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
from fractions import Fraction
ROOT=Path(os.getenv('CPP_WORKSPACE','/home/qiumc/data/code/cpp')).resolve()
D=ROOT/'integration/opengauss-native-lake'

def digest(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def image(v):
 if isinstance(v,Fraction):return dict(numerator=v.numerator,denominator=v.denominator)
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


MODES=('binding-stats','whole-visible','sampling-skew','failure-and-guards')
FULL01='5a091d69991722d0b269efb10db647f634546fa88eec3e2bdfc4bb68ecb4141b'
PROBE='2fde20bd093d837310fbc32cc71ae806b9cc305627cdd304fee14ac4c9c9e3fc'
UPGRADE='96b1a5cbfc588e7ddf43b5b1abe3c895298a76410e7fa7c711a737fe29cdd51a'

# Root-assigned contracts; this QA never edits a version gate or installs SQL.
ANALYZE_SQL_VERSION='0.25'
MUTATION_SQL_VERSION='0.24'
CPP_CAPABILITIES_PATCH='2cdeb956dc8072d1bfe903f6eaf2442a5880b12aa3ba339f6b8cbc23394ceb42'
SQL_PACKAGE_PATCH='d9b939cb41622ae03b70a03f558f444ed024032411a7166e602780941c3f3f8c'
SHARED_HELPERS_PATCH='2b75d101103a83bc953ceb73c2181f8afedcb60568f6b321b4ea0910d681ee9f'
COMPOSED_PRODUCT_PATCH='ed788260db45efa8e17345cc20bf8ed2c625dcccf7ca2163e7a6f63cabc9f01e'
UTILITY_ADMISSION_PATCH='f611306e316fbe0f6839d6377b17b7ef58187b65c43c7e18c6b582e7d94247f0'
TUPLEDESC_REPAIR_PATCH='dcba5204c133195f97f3fcfe8d9a5278a4922ecddc8bc4611aa4c22deb342e4e'
UTILITY_ADMISSION_INPUTS={'src/gausskernel/process/tcop/utility.cpp': 'b743c7b6f69f3df35db0feff3e2cb5fff62361267b6a536d69427b025d6af611'}
RETAINED_PRODUCT_PREREQUISITES={'contrib/og_lake/lake_txn.cpp': '50b656d07dcb25d529f73bfb61deefd4a851413f146a678b84372dcb2d0cb9b6'}
COMPOSED_PRODUCT_INPUTS={'contrib/og_lake/CMakeLists.txt': 'acd08e20e6e40698c0da5fa13670ab005d852f03f96c0f242c8ff58a6685db06', 'contrib/og_lake/Makefile': '6257db37ce8e20dd0d8efe6d8ca9f0e54a2c8359ab624f38229e746e24a28d93', 'contrib/og_lake/lake_acl.cpp': '5725a951b3b686264c2750d7b9564a3c22408ede9ae1ace98932fdc4cf387e01', 'contrib/og_lake/lake_autoanalyze.cpp': '3dda56dac476ee5c80902186c9588e5fa5bf5553955ff41f3969f866c5896884', 'contrib/og_lake/lake_catalog.cpp': '6091dee046c61e55021fc57e234c3e1c3540cec9763299c2d0b2aac93051d501', 'contrib/og_lake/lake_create_publish.cpp': '8a9517e73e0df1827f42e8d5d8776f7bc9219c0f6509cc289ab49a88fdec6b20', 'contrib/og_lake/lake_create_publish.h': 'cca54d5636244cf583e55c395bb76976cb5d1f3511089330a8d348e4db654872', 'contrib/og_lake/lake_locator_plan.cpp': 'bd196138333ca6ebb1d0576e17c436a19f837d0a1f6acd07dadd6378c40e9eef', 'contrib/og_lake/lake_modify.cpp': '7ed86e3c66c339806b6c75af81e4ee7cc5cb2a313cef9f775faaad38ed53a5dd', 'contrib/og_lake/lake_native_partition.cpp': 'e3f56c72b9298ac24515e9999b91797057e8599c2dc4843d7f0c5a5a73ee2c1b', 'contrib/og_lake/lake_native_partition.h': '8ca4290b8be73ae47dcb4c1bf111ffb61a316f2c3102dd4a171c12afb5b9a646', 'contrib/og_lake/lake_native_partition_analyze.cpp': 'aaecb53ea3e6597d46de406182286de6bd3439d09824701e2307e0b1bdbe125a', 'contrib/og_lake/lake_native_partition_bind.cpp': 'fd03d4aadfd37bfd1604dc717a2a408e01b4d7bdc614c834db84d95620f5322c', 'contrib/og_lake/lake_native_partition_modify.cpp': '0879a8e2e79671859ff6fccd385f3693bcdb951272e48b2ae14b9fe829441085', 'contrib/og_lake/lake_native_partition_scan.cpp': 'bb3b9715df4e8c33d7fe586b0d4de857657511965f6eb643393b8ccd4c5ee6a5', 'contrib/og_lake/lake_native_partition_write.cpp': 'aa8ff41bf430ae1fffc65bdd1d99e373abbeb48fee49394018b262b1c9d97c4e', 'contrib/og_lake/lake_publish.cpp': '7c9a316f2912f7208253bb985ab2472a240af8f8a6b318f876ca9a69b6141179', 'contrib/og_lake/lake_sample.cpp': 'd25358facee6bdb48845f3c8777ac050627248c479f5155429dcb1133772ce40', 'contrib/og_lake/lake_sample.h': 'af5cb1fa494613cbd238336ca6b92b5d8be51484e939c4f0447d6e99b240db67', 'contrib/og_lake/lake_test_hooks.cpp': '1362bc71335bc409891c01d9ddd21c0e2ff42c2bd3ade15c99086d8389ec62f1', 'contrib/og_lake/lake_version.h': 'c0e95929a7318ac1c56ae0b12e5d8a7f84bc4fe8b922e51e59088a77208af1b4', 'contrib/og_lake/og_lake--0.19--0.20.sql': '6e8fdd89b15c14c31693a4b468e84fe38ddb1b9e19f4286fa815cea88f5cfd58', 'contrib/og_lake/og_lake--0.20--0.21.sql': '83c557b527e5a63464a6491d7981c3a16ce39f4c446d26aaa859539946ebcdd1', 'contrib/og_lake/og_lake--0.20.sql': '3a31f4808c00d5ab171e742a1bce1278be16ddf138c6ae46079419018e383d22', 'contrib/og_lake/og_lake--0.21--0.22.sql': 'c7df0ef3a13e07c2d9d102f2f1e7a8f51696078f15d9766843062ef260ed5a59', 'contrib/og_lake/og_lake--0.21.sql': '610666fb9b6755c597c9f1a7d6a62e9036d17b68126bebb9c2f8262ebe48f21d', 'contrib/og_lake/og_lake--0.22--0.23.sql': '8cd4408331af41c070b0e0432709234d1a4e2785c8f0229fd4c49172c11c1756', 'contrib/og_lake/og_lake--0.22.sql': 'd9de28bfa97b1c385411a071a1af09796213eebd4392a24779c8f2a7b80fbd7f', 'contrib/og_lake/og_lake--0.23--0.24.sql': '9d8d247e3e6a81d9815be4122f73333d7e77045cd1ff4af05522408c4ac9ca81', 'contrib/og_lake/og_lake--0.23.sql': '1890baec867245a11f6ad5419c757e3e705a39fec945de9388b96eb34f1850ad', 'contrib/og_lake/og_lake--0.24--0.25.sql': '6e458ce9259950e5e01ff825f506f8cd69185a07d363a3879292021a01eff9a0', 'contrib/og_lake/og_lake--0.24.sql': 'adafb3aaffd69879678b6084a14799e791c49c3b8c280662e23bc78454811aa7', 'contrib/og_lake/og_lake--0.25.sql': 'c99c0ca220270c6635f5a30aeb917293b36f58938921f623568e0e60cf39f37b', 'contrib/og_lake/og_lake.control': '7abe54390a9e751ecdb293b2006b9bcdc020f62fa6e010219cc0ee9d7c7e6988', 'contrib/og_lake/og_lake.cpp': '6029fd05de33a346a750dece455f808de49cf8d162d21acf88f12504e0d7d62b', 'src/gausskernel/optimizer/commands/analyze.cpp': '7c7fd8cb0a32f4a14bde798fd336dc0d6ad26efefcc3bbaed194b7b6cff4d4c0', 'src/gausskernel/optimizer/commands/copy.cpp': 'dd68756dff8472a150568350b6eb03d8018ae6bd7e279f631dda8421fa4cd80d', 'src/gausskernel/optimizer/commands/vacuum.cpp': '8f2d9b8d344974a2ac3636a6d453526ce784de282253799fdf5b46a84bef8b9b', 'src/gausskernel/optimizer/rewrite/rewriteHandler.cpp': '398e59cd45801b1c15d09282dd78f640070b4fe1ccb3b2f79921e70a4828ac20', 'src/gausskernel/runtime/executor/execJunk.cpp': 'd83d6e3ad1ae4799c55f11f530cea808951ad105111f96b7b46daeab3cf2c812', 'src/gausskernel/runtime/executor/nodeModifyTable.cpp': '61032c139fdfe3ae5ccb9c48869f16d2796ae6e70efe2c8687026ca35cae058c', 'src/include/access/og_lake.h': '53211b36e17c741d41cf4f8ee0c0f6bb77150333418f682b79191a5e9d96910f', 'src/gausskernel/process/tcop/utility.cpp': 'b743c7b6f69f3df35db0feff3e2cb5fff62361267b6a536d69427b025d6af611'}
REVIEWED_CPP_INPUTS={'contrib/og_lake/CMakeLists.txt': '8afe558199ddf524d0f7c4ce5c0c352f6ed8941dd3289f47a0d7663783fc0a4a', 'contrib/og_lake/Makefile': '630aed3389a1f9b87b3df33fa8fc8335d3be6dccee69e0459c8984dd38ae80a6', 'contrib/og_lake/lake_acl.cpp': '5725a951b3b686264c2750d7b9564a3c22408ede9ae1ace98932fdc4cf387e01', 'contrib/og_lake/lake_autoanalyze.cpp': '3dda56dac476ee5c80902186c9588e5fa5bf5553955ff41f3969f866c5896884', 'contrib/og_lake/lake_catalog.cpp': '6091dee046c61e55021fc57e234c3e1c3540cec9763299c2d0b2aac93051d501', 'contrib/og_lake/lake_create_publish.cpp': '8a9517e73e0df1827f42e8d5d8776f7bc9219c0f6509cc289ab49a88fdec6b20', 'contrib/og_lake/lake_create_publish.h': 'cca54d5636244cf583e55c395bb76976cb5d1f3511089330a8d348e4db654872', 'contrib/og_lake/lake_locator_plan.cpp': 'bd196138333ca6ebb1d0576e17c436a19f837d0a1f6acd07dadd6378c40e9eef', 'contrib/og_lake/lake_modify.cpp': '7ed86e3c66c339806b6c75af81e4ee7cc5cb2a313cef9f775faaad38ed53a5dd', 'contrib/og_lake/lake_native_partition.cpp': 'e3f56c72b9298ac24515e9999b91797057e8599c2dc4843d7f0c5a5a73ee2c1b', 'contrib/og_lake/lake_native_partition.h': '8ca4290b8be73ae47dcb4c1bf111ffb61a316f2c3102dd4a171c12afb5b9a646', 'contrib/og_lake/lake_native_partition_analyze.cpp': 'aaecb53ea3e6597d46de406182286de6bd3439d09824701e2307e0b1bdbe125a', 'contrib/og_lake/lake_native_partition_bind.cpp': 'fd03d4aadfd37bfd1604dc717a2a408e01b4d7bdc614c834db84d95620f5322c', 'contrib/og_lake/lake_native_partition_modify.cpp': '0879a8e2e79671859ff6fccd385f3693bcdb951272e48b2ae14b9fe829441085', 'contrib/og_lake/lake_native_partition_scan.cpp': 'bb3b9715df4e8c33d7fe586b0d4de857657511965f6eb643393b8ccd4c5ee6a5', 'contrib/og_lake/lake_native_partition_write.cpp': 'aa8ff41bf430ae1fffc65bdd1d99e373abbeb48fee49394018b262b1c9d97c4e', 'contrib/og_lake/lake_publish.cpp': '7c9a316f2912f7208253bb985ab2472a240af8f8a6b318f876ca9a69b6141179', 'contrib/og_lake/lake_sample.cpp': 'd25358facee6bdb48845f3c8777ac050627248c479f5155429dcb1133772ce40', 'contrib/og_lake/lake_sample.h': 'af5cb1fa494613cbd238336ca6b92b5d8be51484e939c4f0447d6e99b240db67', 'contrib/og_lake/lake_test_hooks.cpp': '1362bc71335bc409891c01d9ddd21c0e2ff42c2bd3ade15c99086d8389ec62f1', 'contrib/og_lake/og_lake.cpp': '6029fd05de33a346a750dece455f808de49cf8d162d21acf88f12504e0d7d62b', 'src/gausskernel/optimizer/commands/analyze.cpp': '7c7fd8cb0a32f4a14bde798fd336dc0d6ad26efefcc3bbaed194b7b6cff4d4c0', 'src/gausskernel/optimizer/commands/copy.cpp': 'dd68756dff8472a150568350b6eb03d8018ae6bd7e279f631dda8421fa4cd80d', 'src/gausskernel/optimizer/commands/vacuum.cpp': '8f2d9b8d344974a2ac3636a6d453526ce784de282253799fdf5b46a84bef8b9b', 'src/gausskernel/optimizer/rewrite/rewriteHandler.cpp': '398e59cd45801b1c15d09282dd78f640070b4fe1ccb3b2f79921e70a4828ac20', 'src/gausskernel/runtime/executor/execJunk.cpp': 'd83d6e3ad1ae4799c55f11f530cea808951ad105111f96b7b46daeab3cf2c812', 'src/gausskernel/runtime/executor/nodeModifyTable.cpp': '61032c139fdfe3ae5ccb9c48869f16d2796ae6e70efe2c8687026ca35cae058c', 'src/include/access/og_lake.h': '53211b36e17c741d41cf4f8ee0c0f6bb77150333418f682b79191a5e9d96910f'}
REVIEWED_SQL_PACKAGE_INPUTS={'contrib/og_lake/CMakeLists.txt': 'debdb94f60f0eccc8a78510f3bd11abdab356a9ce10033ffc5a69dc4fab1392a', 'contrib/og_lake/Makefile': 'a8c3173a091427b5f97c5307d14485a4c76a515ff081374d4b85a132e7f448b8', 'contrib/og_lake/lake_version.h': 'c0e95929a7318ac1c56ae0b12e5d8a7f84bc4fe8b922e51e59088a77208af1b4', 'contrib/og_lake/og_lake--0.19--0.20.sql': '6e8fdd89b15c14c31693a4b468e84fe38ddb1b9e19f4286fa815cea88f5cfd58', 'contrib/og_lake/og_lake--0.20--0.21.sql': '83c557b527e5a63464a6491d7981c3a16ce39f4c446d26aaa859539946ebcdd1', 'contrib/og_lake/og_lake--0.20.sql': '3a31f4808c00d5ab171e742a1bce1278be16ddf138c6ae46079419018e383d22', 'contrib/og_lake/og_lake--0.21--0.22.sql': 'c7df0ef3a13e07c2d9d102f2f1e7a8f51696078f15d9766843062ef260ed5a59', 'contrib/og_lake/og_lake--0.21.sql': '610666fb9b6755c597c9f1a7d6a62e9036d17b68126bebb9c2f8262ebe48f21d', 'contrib/og_lake/og_lake--0.22--0.23.sql': '8cd4408331af41c070b0e0432709234d1a4e2785c8f0229fd4c49172c11c1756', 'contrib/og_lake/og_lake--0.22.sql': 'd9de28bfa97b1c385411a071a1af09796213eebd4392a24779c8f2a7b80fbd7f', 'contrib/og_lake/og_lake--0.23--0.24.sql': '9d8d247e3e6a81d9815be4122f73333d7e77045cd1ff4af05522408c4ac9ca81', 'contrib/og_lake/og_lake--0.23.sql': '1890baec867245a11f6ad5419c757e3e705a39fec945de9388b96eb34f1850ad', 'contrib/og_lake/og_lake--0.24--0.25.sql': '6e458ce9259950e5e01ff825f506f8cd69185a07d363a3879292021a01eff9a0', 'contrib/og_lake/og_lake--0.24.sql': 'adafb3aaffd69879678b6084a14799e791c49c3b8c280662e23bc78454811aa7', 'contrib/og_lake/og_lake--0.25.sql': 'c99c0ca220270c6635f5a30aeb917293b36f58938921f623568e0e60cf39f37b', 'contrib/og_lake/og_lake.control': '7abe54390a9e751ecdb293b2006b9bcdc020f62fa6e010219cc0ee9d7c7e6988'}
CENTRAL_CAPABILITY_INPUTS={'contrib/og_lake/lake_catalog.cpp': '6091dee046c61e55021fc57e234c3e1c3540cec9763299c2d0b2aac93051d501', 'contrib/og_lake/lake_native_partition.h': '8ca4290b8be73ae47dcb4c1bf111ffb61a316f2c3102dd4a171c12afb5b9a646'}
REVIEWED_SHARED_HELPER_INPUTS={'native_test_context.py': '28a7e3eec2ac07125a1e3be103c0fb7bf70888f45f79fa8f8dc526b2590cf909', 'tests/native_table_create.py': '50a913eed5b248741358b8d7f63539075fb7fec24b4caf546220a28f6bba73d9', 'sdk/install_sql020_native.py': '949826fddef5ae69b214f997e4c138d4a9a84837456c2e1118065f524091f1f0'}

def main():
 p=argparse.ArgumentParser(description=__doc__)
 p.add_argument('--mode',choices=MODES,required=True);p.add_argument('--execute',action='store_true')
 p.add_argument('--run-id');p.add_argument('--sql-version',default=ANALYZE_SQL_VERSION,choices=[ANALYZE_SQL_VERSION]);p.add_argument('--build-receipt',type=Path)
 p.add_argument('--sdk-qualification',type=Path);p.add_argument('--sdk-index-sha256');p.add_argument('--source-contract',type=Path)
 p.add_argument('--case-timeout',type=int,default=900);p.add_argument('--_child',action='store_true',help=argparse.SUPPRESS)
 a=p.parse_args()
 if not a.execute:
  print(json.dumps(dict(status='prepared_not_executed',mode=a.mode,future_sql_version=a.sql_version,Native=0,SDK=0,fixtures=0,
   candidates=dict(full01=FULL01,probe=PROBE,upgrade=UPGRADE,central_capabilities=CPP_CAPABILITIES_PATCH,sql_package=SQL_PACKAGE_PATCH,shared_helpers=SHARED_HELPERS_PATCH,utility_admission=UTILITY_ADMISSION_PATCH,descriptor_prerequisite=TUPLEDESC_REPAIR_PATCH),
   contracts=dict(retained_read_insert='0.23',mutation_copy=MUTATION_SQL_VERSION,manual_analyze=ANALYZE_SQL_VERSION,upgrade_hops=['0.23--0.24','0.24--0.25'],ready_for_native_qualification=False),
   requirements=['Exact runtime/A25 with retained READ/INSERT23 and M/COPY24; complete reviewed43 resolved product files and retained50b descriptor prerequisite plus official source/binary/SDK/helper contract; no activation by this runner',
    'Independent heap from original int4(shard,id,val) DML; two real pg_partition leaves and distinct typed same-R identities',
    'One global file-then-inline physical domain: immutable DuckDB(R,P,birth)/delete decoding and raw inline ctid-order before sampler evidence',
    'Bounded nonce-owned process supervision, precise OID/marker/role cleanup and single fixture reservation; failure keeps scene',
    'binding-stats: installed23 old-bound scaffolds, real busy55P03 and whole upgrade rollback/commit, then new A bind reset/rollback',
    'whole-visible: p1 full visible oracle, ordinary pg_stats and readonly/promoted two-view branches; no lake commit/row-ID allocation',
    'sampling-skew:128/4000 original rows, three actual seeds, global exact p and selected positions with no table-local R dedup',
    'failure-and-guards: true first-kept advisory wait, cancel57014/reuse, captured-S concurrent INSERT, old stats preservation and closed non-whole-parent routes'],
   deferred=['Native execution, actual fresh25 and atomic23-to24-to25 qualification, RAC/remote/auto scheduler/stress and host late40001 injection remain unqualified']),indent=2));return 0
 if not all([a.run_id,a.sql_version,a.build_receipt,a.sdk_qualification,a.sdk_index_sha256,a.source_contract]):p.error('execute requires exact A25, owned run ID and official build/SDK/source contract')
 if a.sql_version!=ANALYZE_SQL_VERSION:p.error('only Root-assigned runtime/A25 is reviewed; retained23 does not admit parent ANALYZE')
 if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,89}',a.run_id):p.error('safe unique run-id<=90 required')
 if a.case_timeout!=900:p.error('this first bounded campaign has an explicit900s whole-case budget')
 if not a._child:return supervise(a)
 if os.environ.get('OG_LAKE_PARENT_TEST_PARENT')!=str(os.getppid()):p.error('unsupervised execute child')
 from concurrent.futures import ThreadPoolExecutor
 from fractions import Fraction
 import math
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
 from native_file_analyze import Notices,selected_positions,requested_int4_sample_rows,verified_sample_design,fixture_survivor_floor
 from psycopg2 import sql
 validate_run_id(a.run_id)
 if PROFILE!='debug':p.error('owned Debug qualification only')
 out=ROOT/'公共文档/openGauss原生湖存储/验证'/a.run_id;out.mkdir(exist_ok=False)
 r=dict(status='fail',scope=__doc__,mode=a.mode,future_sql_version=a.sql_version,assertions=[],executed=False)
 control=db=roles=t=reservation=None;workers=[];gate_key=None;gate_held=False;preserve=False
 def check(label,got,want=True):
  got,want=image(got),image(want);ok=got==want;r['assertions'].append(deepcopy(dict(name=label,status='pass'if ok else'fail',observed=got,expected=want)));write_json(out/'result.json',r)
  if not ok:raise AssertionError(label)
 def configure(c,target=10):
  for statement in ['SET session_timeout=0','SET statement_timeout=60000','SET query_dop=1','SET enable_lake_smp=off','SET enable_vector_engine=off','SET enable_default_ustore_table=off','SET enable_opfusion=off','SET default_statistics_target='+str(target)]:query(c,statement)
 def deny(c,statement,code='0A000',args=None):
  try:query(c,statement,args)
  except Exception as e:check('expected refusal/'+str(statement)[:96],getattr(e,'pgcode',None),code)
  else:raise AssertionError('unsupported statement was accepted: '+str(statement))
 def current_version(c=None):return scalar(c or t.observer,"SELECT extversion FROM pg_extension WHERE extname='og_lake'")
 def marker(oid,c=None):return scalar(c or t.observer,"SELECT COALESCE('lake_partition_root=true'=ANY(reloptions),false) FROM pg_class WHERE oid=%s",(oid,))
 def maps(oid,c=None):return query(c or t.observer,'SELECT p.leaf_partition_oid,p.partition_no,p.binding_id::text,p.certified_snapshot,p.source_hash,b.table_id,b.table_uuid::text,b.server_oid,b.read_only FROM og_lake_internal.partition_binding p JOIN og_lake_internal.table_binding b USING(binding_id) WHERE p.parent_relid=%s ORDER BY p.leaf_partition_oid',(oid,))
 def stats_image(g,c=None):
  c=c or t.observer;oid,leaves=g['oid'],g['leaves'];predicate="(starelkind='c' AND starelid=%s)OR(starelkind='p' AND starelid=ANY(%s::oid[]))"
  return dict(parent=query(c,'SELECT oid,relpages,reltuples,relallvisible,c::text FROM pg_class c WHERE oid=%s',(oid,)),
   leaves=query(c,"SELECT oid,parentid,parttype,relpages,reltuples,relallvisible,p::text FROM pg_partition p WHERE oid=ANY(%s::oid[])ORDER BY oid",(leaves,)),
   columns=query(c,'SELECT starelkind,starelid,staattnum,stainherit,s::text FROM pg_statistic s WHERE '+predicate+' ORDER BY starelkind,starelid,staattnum,stainherit',(oid,leaves)),
   extended=query(c,'SELECT starelkind,starelid,s::text FROM pg_statistic_ext s WHERE '+predicate+' ORDER BY starelkind,starelid,s::text',(oid,leaves)))
 def reset_image(g,c=None):
  z=stats_image(g,c);return dict(parent=[row[1:4]for row in z['parent']],leaves=[row[3:6]for row in z['leaves']],columns=z['columns'],extended=z['extended'])
 def require_reset(g,c=None):check(g['name']+' genuine standard stats reset',reset_image(g,c),dict(parent=[(0,0.0,0)],leaves=[(0,0.0,0)]*2,columns=[],extended=[]))
 def business(g,c=None):return query(c or t.a,sql.SQL('SELECT shard,id,val FROM biz.{} ORDER BY shard,id,val NULLS FIRST').format(sql.Identifier(g['name'])))
 def oracle(g):return query(t.observer,sql.SQL('SELECT shard,id,val FROM biz.{} ORDER BY shard,id,val NULLS FIRST').format(sql.Identifier(g['oracle'])))
 def pgstats(name,c=None):return query(c or t.observer,"SELECT attname,null_frac,avg_width,n_distinct,most_common_vals::text,most_common_freqs::text,histogram_bounds::text FROM pg_stats WHERE schemaname='biz'AND tablename=%s AND NOT inherited ORDER BY attname",(name,))
 def bind(g,c=None):return query(c or t.a,'SELECT og_lake.bind_native_partition_readonly(%s::regclass,%s::name,%s::oid[],%s::bigint[])',(g['oid'],t.server,g['leaves'],g['tables']))
 def independent(statement,label):
  (out/(label+'.sql')).write_text(statement+'\n');proc=subprocess.run([str(duckdb),'-no-init','-batch','-json','-c','SET threads=1;'+statement],capture_output=True,text=True,timeout=90)
  (out/(label+'.stdout')).write_text(proc.stdout);(out/(label+'.stderr')).write_text(proc.stderr)
  if proc.returncode:raise RuntimeError('Independent DuckDB refused '+label)
  return json.loads(proc.stdout)if proc.stdout.strip()else[]
 def literal(value):return "'"+str(value).replace("'","''")+"'"
 def create_group(index,low,high,scaffold='empty'):
  name='parent_'+str(index);oracle_name='exact_'+str(index)
  query(t.a,sql.SQL('CREATE TABLE biz.{}(shard integer,id integer,val integer)').format(sql.Identifier(oracle_name)))
  ids=[]
  for side,n,shard,sign in [('low',low,1,-1),('high',high,11,1)]:
   source='source_'+str(index)+'_'+side;target='biz.'+source
   query(t.a,sql.SQL("CREATE FOREIGN TABLE biz.{}(shard integer,id integer,val integer)SERVER {} OPTIONS(binding_mode 'create',lake_schema 'main')").format(sql.Identifier(source),sql.Identifier(t.server)))
   query(t.a,'SELECT og_lake.set_option(%s::regclass,\'data_inlining_row_limit\',\'10000\')',(target,))
   table_id=scalar(t.observer,sql.SQL('SELECT table_id FROM {}.ducklake_table WHERE table_name=%s AND end_snapshot IS NULL').format(sql.Identifier(t.schema)),(source,))
   ids.append(table_id)
   def both(template,args=None):
    query(t.a,'BEGIN')
    try:
     for relation in [target,'biz.'+oracle_name]:query(t.a,sql.SQL(template).format(sql.Identifier(*relation.split('.'))),args)
     query(t.a,'COMMIT')
    except BaseException:query(t.a,'ROLLBACK');raise
   # The partial-sample fixture keeps val in{0,1,NULL}; its MCV choice
   # therefore does not depend on singleton NDV extrapolation versus the
   # smaller ordinary heap oracle. Other modes retain changed31/32/99/77.
   modulo=2 if a.mode=='sampling-skew'else 5
   both('INSERT INTO {} SELECT %s,%s*i,CASE WHEN i%%10=0 THEN NULL ELSE i%%'+str(modulo)+' END FROM generate_series(1,%s)i',(shard,sign,n))
   query(t.a,'SELECT og_lake.flush(%s::regclass)',(target,))
   first,last=(0,1)if a.mode=='sampling-skew'else(31,32)
   both('UPDATE {} SET val=%s WHERE id=%s',(first,sign));both('UPDATE {} SET val=%s WHERE id=%s',(last,sign));both('DELETE FROM {} WHERE id=%s',(sign*2,))
   query(t.a,'SELECT og_lake.flush(%s::regclass)',(target,))
   changed,inserted=(0,0)if a.mode=='sampling-skew'else(99,77)
   both('UPDATE {} SET val=%s WHERE id=%s',(changed,sign*3));both('INSERT INTO {} VALUES(%s,%s,%s),(%s,%s,NULL)',(shard,sign*(n+1),inserted,shard,sign*(n+2)))
  query(t.a,sql.SQL('CREATE TABLE biz.{}(shard integer,id integer,val integer)PARTITION BY RANGE(shard)(PARTITION low VALUES LESS THAN(10),PARTITION high VALUES LESS THAN(20))').format(sql.Identifier(name)))
  oid=scalar(t.a,'SELECT %s::regclass::oid',('biz.'+name,));leaf_rows=query(t.a,"SELECT oid,relname,partitionno FROM pg_partition WHERE parentid=%s AND parttype='p'ORDER BY relname",(oid,));leafmap={name:(oid,no)for oid,name,no in leaf_rows}
  check(name+' two real native leaves',sorted(leafmap),['high','low']);leaves=[leafmap['low'][0],leafmap['high'][0]]
  check(name+' actual native ordinary r/p',query(t.a,'SELECT relkind,parttype FROM pg_class WHERE oid=%s',(oid,)),[('r','p')])
  check(name+' leaves have no synthetic pg_class',scalar(t.a,'SELECT count(*)FROM pg_class WHERE oid=ANY(%s::oid[])',(leaves,)),0)
  g=dict(name=name,oid=oid,leaves=leaves,tables=ids,oracle=oracle_name,source_names=['source_'+str(index)+'_low','source_'+str(index)+'_high'])
  if scaffold=='stale':
   query(t.a,sql.SQL('INSERT INTO biz.{} SELECT * FROM biz.{}').format(sql.Identifier(name),sql.Identifier(oracle_name)))
   query(t.a,sql.SQL('ANALYZE biz.{}').format(sql.Identifier(name)))
   for part in['low','high']:query(t.a,sql.SQL('ANALYZE biz.{} PARTITION({})').format(sql.Identifier(name),sql.Identifier(part)))
   check(name+' ordinary scaffold really produced old distributions',len(stats_image(g)['columns'])>0)
   query(t.a,sql.SQL('TRUNCATE biz.{}').format(sql.Identifier(name)))
   current_leaves=query(t.a,"SELECT oid,relname FROM pg_partition WHERE parentid=%s AND parttype='p'",(oid,));by_name={n:o for o,n in current_leaves};g['leaves']=[by_name['low'],by_name['high']]
   check(name+' old distributions actually survive real TRUNCATE',len(stats_image(g)['columns'])>0)
  else:
   query(t.a,sql.SQL('ANALYZE biz.{}').format(sql.Identifier(name)))
   if scaffold=='nonempty':query(t.a,sql.SQL('INSERT INTO biz.{} VALUES(1,-900000,12)').format(sql.Identifier(name)))
  return g
 def physical_domain(g,label):
  S=t.head()[0];domain=[];inline_domains=[];groups=[];file_domains=[];identities=[];hashes={};counts=[]
  root=Path(t.empty_manifest['data_path']).resolve()
  def owned_file(name):
   candidates=[f for f in root.rglob('*.parquet')if f.name==Path(name).name]
   check(label+' exact owned object/'+name,len(candidates),1);f=candidates[0].resolve();check(label+' immutable path inside owned fixture',f.is_relative_to(root))
   dst=out/'immutable-inputs'/f.relative_to(root);dst.parent.mkdir(parents=True,exist_ok=True)
   if not dst.exists():shutil.copy2(f,dst)
   check(label+' exact immutable file archive/'+f.name,digest(dst),digest(f));hashes[str(f)]=digest(f);return f
  for owner,tid in enumerate(g['tables']):
   table_uuid=scalar(t.observer,sql.SQL('SELECT table_uuid::text FROM {}.ducklake_table WHERE table_id=%s AND begin_snapshot<=%s AND(end_snapshot IS NULL OR end_snapshot>%s)').format(sql.Identifier(t.schema)),(tid,S,S))
   deleted=set();delete_rows={};dn='ducklake_inlined_delete_'+str(tid)
   if scalar(t.observer,'SELECT count(*)FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=%s AND c.relname=%s',(t.schema,dn)):
    deleted.update(query(t.observer,sql.SQL('SELECT file_id,row_id FROM {}.{} WHERE begin_snapshot<=%s').format(sql.Identifier(t.schema),sql.Identifier(dn)),(S,)))
   remote=query(t.observer,sql.SQL('SELECT data_file_id,path FROM {}.ducklake_delete_file WHERE table_id=%s AND begin_snapshot<=%s AND(end_snapshot IS NULL OR end_snapshot>%s)ORDER BY data_file_id').format(sql.Identifier(t.schema)),(tid,S,S))
   check(label+' genuine delete Parquet in leaf'+str(owner),len(remote)>0)
   for at,(fid,name)in enumerate(remote):
    f=owned_file(name);marks=independent('SELECT pos,_ducklake_internal_snapshot_id AS ended FROM read_parquet('+literal(f)+')ORDER BY pos',label+'-delete-'+str(owner)+'-'+str(at))
    delete_rows[fid]=len(marks);deleted.update((fid,row['pos'])for row in marks if row['ended']<=S)
   files=query(t.observer,sql.SQL('SELECT data_file_id,path,record_count FROM {}.ducklake_data_file WHERE table_id=%s AND begin_snapshot<=%s AND(end_snapshot IS NULL OR end_snapshot>%s)ORDER BY data_file_id').format(sql.Identifier(t.schema)),(tid,S,S))
   check(label+' genuine files in leaf'+str(owner),len(files)>0);start_owner=len(domain)
   for at,(fid,name,n)in enumerate(files):
    f=owned_file(name);raw=independent('SELECT file_row_number AS position,_ducklake_internal_row_id AS row_id,_ducklake_internal_snapshot_id AS birth,shard,id,val FROM read_parquet('+literal(f)+',file_row_number=true)ORDER BY file_row_number',label+'-file-'+str(owner)+'-'+str(at))
    check(label+' actual file positions cover metadata/'+str(fid),len(raw),n);start=len(domain)
    check(label+' dense independent original P/'+str(fid),all(row['position']==at for at,row in enumerate(raw)))
    for row in raw:
     visible=row['birth']<=S and(fid,row['position'])not in deleted
     entry=dict(owner=owner,table_id=tid,table_uuid=table_uuid,R=row['row_id'],birth=row['birth'],file_id=fid,P=row['position'],visible=visible,business=[row['shard'],row['id'],row['val']])
     domain.append(entry)
     if visible:identities.append((owner,row['row_id']))
    rg=independent('SELECT row_group_id,max(row_group_num_rows)AS rows FROM parquet_metadata('+literal(f)+')GROUP BY row_group_id ORDER BY row_group_id',label+'-groups-'+str(owner)+'-'+str(at));first=start
    for group in rg:groups.append((first,first+int(group['rows'])));first+=int(group['rows'])
    check(label+' genuine footer groups cover entire source',first,len(domain));file_domains.append(dict(first=start,last=len(domain),delete_rows=delete_rows.get(fid,0),owner=owner,file_id=fid))
   registrations=query(t.observer,sql.SQL('SELECT table_name FROM {}.ducklake_inlined_data_tables WHERE table_id=%s ORDER BY schema_version').format(sql.Identifier(t.schema)),(tid,))
   check(label+' exact one inline carrier per source',len(registrations),1)
   carrier=registrations[0][0];raw=query(t.observer,sql.SQL('SELECT _ducklake_row_id,_ducklake_begin_snapshot,_ducklake_end_snapshot,shard,id,val FROM {}.{} ORDER BY ctid').format(sql.Identifier(t.schema),sql.Identifier(carrier)))
   check(label+' nonempty genuine inline candidate domain/'+str(owner),len(raw)>0)
   for pos,(R,birth,end,shard,business_id,value)in enumerate(raw):
    visible=birth<=S and(end is None or end>S);inline_domains.append(dict(owner=owner,table_id=tid,table_uuid=table_uuid,R=R,birth=birth,end=end,inline_ordinal=pos,visible=visible,business=[shard,business_id,value]))
    if visible:identities.append((owner,R))
   counts.append(dict(owner=owner,files=len(domain)-start_owner,inline=len(raw)))
  # The real shared sampler concatenates ALL file inventories before ALL
  # table-ordered inline versions. Never interleave a leaf's inline early.
  domain.extend(inline_domains)
  p=[tuple(e['business'])if e['visible']else None for e in domain]
  expected=oracle(g);visible=sorted([tuple(e['business'])for e in domain if e['visible']],key=lambda x:(x[0],x[1]))
  check(label+' independent physical visibility equals original heap DML',visible,expected)
  low={R for owner,R in identities if owner==0};high={R for owner,R in identities if owner==1}
  check(label+' equal numeric R genuinely exists across distinct typed leaves',bool(low&high))
  check(label+' both leaf physical candidate bounds',all(c['files']+c['inline']<=4096 for c in counts))
  saved=out/(label+'-physical-domain.json');write_json(saved,dict(snapshot=S,order='all(owner,file_id,P),then all(owner,raw inline ctid ordinal)',rows=domain,groups=groups,source_hashes=hashes,leaf_counts=counts))
  r.setdefault('domain_oracles',[]).append(dict(path=str(saved),sha256=digest(saved),snapshot=S,physical=len(p),visible=len(expected),typed_equal_R=sorted(low&high)))
  return dict(physical=p,groups=groups,files=file_domains,leaf_counts=counts,hashes=hashes,S=S)
 def analyze(g,label,c=None):
  c=c or t.a;notices=Notices();c.notices=notices;query(c,'SET client_min_messages=debug1')
  try:query(c,sql.SQL('ANALYZE biz.{}').format(sql.Identifier(g['name'])))
  finally:
   capture=notices.evidence();write_json(out/(label+'-notices.json'),capture);r.setdefault('analyze_notices',[]).append(dict(label=label,evidence=capture));write_json(out/'result.json',r)
  check(label+' target evidence not dropped',notices.dropped_target_records,0);check(label+' one whole-parent sampler evidence',len(notices.items),1)
  parsed=dict(re.findall(r'(\w+)=([^\s;]+)',notices.items[0]));r.setdefault('sample_evidence',[]).append(dict(label=label,parsed=parsed));return parsed
 def sample_check(g,label,domain,target,partial=False):
  requested=requested_int4_sample_rows(target,query(t.observer,'SELECT attname,atttypid::integer,attstattarget FROM pg_attribute WHERE attrelid=%s AND attnum>0 AND NOT attisdropped ORDER BY attnum',(g['oid'],)))
  evidence=analyze(g,label);p,design=verified_sample_design(evidence,len(domain['physical']),requested);positions=selected_positions(evidence)
  expected=[domain['physical'][i]for i in positions if domain['physical'][i]is not None]
  check(label+' no repeated selected physical positions',len(positions),len(set(positions)));check(label+' survivor count independent of sampler',int(evidence['kept']),len(expected))
  check(label+' actual rows decoded only expected row groups',int(evidence['groups']),sum(any(first<=i<last for i in positions)for first,last in domain['groups']))
  check(label+' actual physical population independent of sampler',int(evidence['physical']),len(domain['physical']))
  estimated=float(Fraction(len(expected),1)/p);actual=scalar(t.observer,'SELECT reltuples FROM pg_class WHERE oid=%s',(g['oid'],))
  check(label+' host live rows use visible/p',math.isclose(actual,estimated,rel_tol=1e-6,abs_tol=.01));check(label+' parent analyzed sentinel/pages',scalar(t.observer,'SELECT relpages FROM pg_class WHERE oid=%s',(g['oid'],))>=1)
  check(label+' standard allvisible remains zero',scalar(t.observer,'SELECT relallvisible FROM pg_class WHERE oid=%s',(g['oid'],)),0)
  expected_name='sample_'+label.replace('-','_');query(t.a,sql.SQL('CREATE TABLE biz.{}(shard integer,id integer,val integer)').format(sql.Identifier(expected_name)))
  if expected:
   with t.a.cursor()as cur:cur.executemany(sql.SQL('INSERT INTO biz.{} VALUES(%s,%s,%s)').format(sql.Identifier(expected_name)),expected)
  query(t.a,sql.SQL('ANALYZE biz.{}').format(sql.Identifier(expected_name)));got,want=pgstats(g['name']),pgstats(expected_name)
  if partial:
   check(label+' genuinely partial inclusion',p<1)
   decoded_data=sum(last-first for first,last in domain['groups']if any(first<=i<last for i in positions))
   decoded_deletes=sum(f['delete_rows']for f in domain['files']if any(f['first']<=i<f['last']for i in positions))
   check(label+' actual complete RG plus delete rows decoded',int(evidence['reader_decoded']),decoded_data+decoded_deletes)
   r.setdefault('partial_decode_observations',[]).append(dict(label=label,actual_seed=evidence['seed'],data_rows=decoded_data,delete_rows=decoded_deletes,file_population=sum(f['last']-f['first']for f in domain['files']),partial_data=decoded_data<sum(f['last']-f['first']for f in domain['files'])))
   check(label+' distributions match independently selected tuple values',[row[:3]+row[4:]for row in got],[row[:3]+row[4:]for row in want])
   ndv={row[0]:row[3]for row in got};check(label+' unique business id retains native unique NDV shape',ndv['id'],-1.0)
   check(label+' other NDVs remain finite valid host estimates',all(math.isfinite(x)and(-1<=x<0 or 0<=x<=max(1,estimated))for x in ndv.values()))
  else:
   check(label+' complete inclusion is one',p,Fraction(1));check(label+' full pg_stats equal independent heap',got,pgstats(g['oracle']))
  write_json(out/(label+'-sample-oracle.json'),dict(design=design,probability=dict(numerator=p.numerator,denominator=p.denominator),positions=positions,visible=expected,estimated_rows=estimated,pgstats=got,heap_sample_pgstats=want))
  check(label+' immutable input unchanged',{name:digest(name)for name in domain['hashes']},domain['hashes'])
  return evidence
 def begin_worker(g,label):
  nonlocal gate_key,gate_held
  gate_key=(1<<45)+int(hashlib.sha256((a.run_id+label).encode()).hexdigest()[:9],16)
  query(t.b,'SELECT pg_advisory_lock(%s::bigint)',(gate_key,));gate_held=True
  pid=scalar(t.a,'SELECT pg_backend_pid()');application='lake_pana_'+hashlib.sha256((a.run_id+label).encode()).hexdigest()[:32]
  query(t.a,'SELECT set_config(\'application_name\',%s,false)',(application,));query(t.a,'SET client_min_messages=debug1');notices=Notices();t.a.notices=notices
  query(t.a,'SELECT biz.analyze_pause(%s::oid,1,%s::bigint)',(g['oid'],gate_key))
  # One SQL already has statement_timeout=60000. Startup and completion
  # share that budget; the former 20s/25s observations remain explicit data.
  timing=dict(label=label,sql_budget_seconds=60,original_wait_seconds=20,original_completion_seconds=25,
   original_wait_observation='pending',original_completion_observation='not_started')
  r.setdefault('worker_timings',[]).append(timing)
  def run():
   try:query(t.a,sql.SQL('ANALYZE biz.{}').format(sql.Identifier(g['name'])));return dict(state='completed',sqlstate=None)
   except Exception as e:return dict(state='error',sqlstate=getattr(e,'pgcode',None),reason=str(e))
   finally:timing['actor_completed_monotonic']=time.monotonic()
  pool=ThreadPoolExecutor(max_workers=1);submitted=time.monotonic();timing.update(submitted_monotonic=submitted,deadline_monotonic=submitted+60)
  worker=dict(pid=pid,application=application,connection=t.a,pool=pool,notices=notices,label=label,timing=timing,gate_key=gate_key)
  worker['future']=pool.submit(run);workers.append(worker)
  while time.monotonic()<timing['deadline_monotonic']:
   waits=query(t.observer,"SELECT pid,granted FROM pg_locks WHERE pid=%s AND database=%s AND locktype='advisory'AND classid=%s::oid AND objid=%s::oid AND objsubid=1 AND NOT granted",(pid,db.allocation['database_oid'],gate_key>>32,gate_key&0xffffffff))
   observed=time.monotonic();elapsed=observed-submitted;timing.update(last_wait_elapsed_seconds=elapsed,last_wait_rows=waits)
   if elapsed>20 and timing['original_wait_observation']=='pending':timing['original_wait_observation']='missed';write_json(out/'result.json',r)
   # A slow observer SQL may itself cross the deadline. Its late lock reply
   # cannot qualify a barrier, even when the returned lock identity matches.
   if observed>timing['deadline_monotonic']:break
   if waits:
    timing.update(barrier_elapsed_seconds=elapsed,original_wait_observation='within_original_20s'if elapsed<=20 else'missed')
    check(label+' actual first-kept advisory wait',waits,[(pid,False)]);check(label+' actor not already complete',worker['future'].done(),False);return worker
   if worker['future'].done():raise AssertionError('actor completed before its real sampled-row barrier: '+repr(worker['future'].result()))
   time.sleep(min(.02,max(0,timing['deadline_monotonic']-time.monotonic())))
  timing['original_wait_observation']='missed'
  raise AssertionError('no exact first-kept advisory wait observed within original SQL60 deadline')
 def worker_identity(worker):
  check(worker['label']+' exact owned DB/application before cancellation',query(t.observer,'SELECT datid,application_name FROM pg_stat_activity WHERE pid=%s',(worker['pid'],)),[(db.allocation['database_oid'],worker['application'])])
 def release_gate():
  nonlocal gate_held
  if gate_held:check('release own exact advisory gate',scalar(t.b,'SELECT pg_advisory_unlock(%s::bigint)',(gate_key,)));gate_held=False
 def capture_worker(worker,phase,answer=None):
  # Call only once the actor is drained: never share its connection or read
  # its live notice list concurrently. Observation failure cannot skip cleanup.
  try:
   capture=worker['notices'].evidence();write_json(out/(worker['label']+'-notices.json'),capture)
   key=worker['gate_key'];locks=query(t.observer,"SELECT pid,granted,mode,classid,objid,objsubid FROM pg_locks WHERE pid=%s AND database=%s AND locktype='advisory'AND classid=%s::oid AND objid=%s::oid AND objsubid=1",(worker['pid'],db.allocation['database_oid'],key>>32,key&0xffffffff))
   r.setdefault('worker_results',[]).append(dict(label=worker['label'],phase=phase,answer=answer,evidence=capture,timing=dict(worker['timing']),final_exact_advisory_locks=locks))
   write_json(out/'result.json',r)
  except BaseException as error:
   r.update(status='fail');r.setdefault('worker_evidence_errors',[]).append(dict(label=worker['label'],phase=phase,reason=str(error)))
 def finish_worker(worker):
  timing=worker['timing'];started=time.monotonic();timing['finish_started_monotonic']=started
  try:
   answer=worker['future'].result(timeout=max(0,timing['deadline_monotonic']-started))
  finally:
   completed=timing.get('actor_completed_monotonic');observed=time.monotonic()
   timing.update(finish_observed_monotonic=observed,finish_wait_seconds=max(0,(completed if completed is not None else observed)-started),
    original_completion_observation=('within_original_25s'if max(0,completed-started)<=25 else'missed')if completed is not None else('missed'if observed-started>=25 else'incomplete_before_original_25s'))
  worker['pool'].shutdown(wait=True);worker['drained']=True;capture_worker(worker,'finish',answer)
  check(worker['label']+' actual actor completion within shared SQL60 deadline',timing['actor_completed_monotonic']<=timing['deadline_monotonic'])
  check(worker['label']+' final actor evidence captured',not r.get('worker_evidence_errors'))
  return answer
 try:
  build=check_build(a.build_receipt);contract=json.loads(a.source_contract.read_text())
  check('exact reviewed purpose',contract.get('purpose'),'native_parent_manual_analyze_qualification');check('explicit future target',contract.get('analyze_sql_version'),a.sql_version)
  check('root reviewed complete admission',contract.get('ready_for_native_qualification'),True);check('no unresolved activation blockers',contract.get('known_blockers'),[])
  check('reviewed manual source patch',contract.get('full01_patch_sha256'),FULL01);check('reviewed actual sampled-row probe',contract.get('probe_patch_sha256'),PROBE);check('reviewed existing-parent updater',contract.get('upgrade_patch_sha256'),UPGRADE)
  check('official entire compiled Native input set',build['native_source_inputs_after'],contract.get('native_inputs'))
  check('reviewed resolved product composition',contract.get('composed_product_patch_sha256'),COMPOSED_PRODUCT_PATCH)
  check('explicit post-composition publisher context repair',contract.get('publish_context_fix_patch_sha256'),'d1c4384d8a344824d4120f04e7e6ab9dbf77dde51cf047b6112794ad60395cd0')
  check('explicit empty mutation plan repair',contract.get('empty_mutation_fix_patch_sha256'),'f78e4c417a0cae264be1d14849a494657de77fa50bda1b5b867cb542cf8e9037')
  check('reviewed central CPP capabilities patch',contract.get('cpp_capabilities_patch_sha256'),CPP_CAPABILITIES_PATCH)
  check('reviewed fresh/adjacent-hop SQL package',contract.get('sql_package_patch_sha256'),SQL_PACKAGE_PATCH)
  check('reviewed shared helpers admission patch',contract.get('shared_helpers_patch_sha256'),SHARED_HELPERS_PATCH)
  check('original28 CPP/header/build candidate inputs',contract.get('cpp_candidate_inputs'),REVIEWED_CPP_INPUTS)
  check('original16 SQL-package candidate inputs',contract.get('sql_package_candidate_inputs'),REVIEWED_SQL_PACKAGE_INPUTS)
  check('central known capabilities exact source contract',contract.get('central_capability_inputs'),CENTRAL_CAPABILITY_INPUTS)
  check('reviewed utility-admission repair',contract.get('utility_admission_patch_sha256'),UTILITY_ADMISSION_PATCH)
  check('exact utility-admission source',contract.get('utility_admission_inputs'),UTILITY_ADMISSION_INPUTS)
  check('retained descriptor prerequisite patch',contract.get('tupledesc_repair_patch_sha256'),TUPLEDESC_REPAIR_PATCH)
  check('retained descriptor prerequisite exact source',contract.get('retained_product_prerequisite_inputs'),RETAINED_PRODUCT_PREREQUISITES)
  required=set(REVIEWED_CPP_INPUTS)|set(REVIEWED_SQL_PACKAGE_INPUTS)|set(UTILITY_ADMISSION_INPUTS)|set(RETAINED_PRODUCT_PREREQUISITES)
  required|={'src/gausskernel/optimizer/commands/analyze.cpp','src/gausskernel/optimizer/commands/vacuum.cpp','src/gausskernel/process/tcop/utility.cpp','src/common/backend/catalog/heap.cpp','src/include/access/og_lake.h'}
  check('all43 product/prerequisite and host admission/upgrade/sampling inputs present',required<=contract['native_inputs'].keys())
  resolved=contract.get('resolved_product_inputs',{})
  check('all43 exact composed product sources',resolved,COMPOSED_PRODUCT_INPUTS)
  for name,want in resolved.items():check('resolved exact official product/'+name,contract['native_inputs'].get(name),want)
  expected=dict(REVIEWED_CPP_INPUTS);expected.update(REVIEWED_SQL_PACKAGE_INPUTS);expected.update(UTILITY_ADMISSION_INPUTS)
  for name,want in expected.items():
   if name not in ['contrib/og_lake/Makefile','contrib/og_lake/CMakeLists.txt']:check('unchanged reviewed product source/'+name,resolved.get(name),want)
  for name,want in CENTRAL_CAPABILITY_INPUTS.items():check('actual known-capability implementation/'+name,contract['native_inputs'].get(name),want)
  for name,want in RETAINED_PRODUCT_PREREQUISITES.items():check('retained already-corrected descriptor source/'+name,contract['native_inputs'].get(name),want)
  check('actual adjacent upgrade source path',contract.get('upgrade_hops'),['0.23--0.24','0.24--0.25'])
  check('selected official binary contract',build['binary_sha256'],contract.get('binary_sha256'))
  helper_inputs=contract.get('shared_helper_inputs',{});check('reviewed Case/oracle/installer source contracts complete',set(REVIEWED_SHARED_HELPER_INPUTS)|{'tests/native_file_analyze.py'}<=helper_inputs.keys())
  for name,want in REVIEWED_SHARED_HELPER_INPUTS.items():check('exact reviewed shared-helper admission/'+name,helper_inputs.get(name),want)
  for name,want in helper_inputs.items():
   if Path(name).is_absolute()or'..'in Path(name).parts:raise AssertionError('unsafe helper input path')
   check('approved exact shared helper/'+name,digest(D/name),want)
  if not contract.get('review_evidence'):raise AssertionError('missing explicit source/gate/upgrade independent review evidence')
  for name,want in contract.get('review_evidence',{}).items():check('frozen review/'+name,digest(name),want)
  references=contract.get('source_reference_inputs',{});check('upgrade host source reference exact','src/gausskernel/optimizer/commands/extension.cpp'in references)
  for name,want in references.items():
   if Path(name).is_absolute()or'..'in Path(name).parts:raise AssertionError('unsafe source reference path')
   check('exact host source reference/'+name,digest(SOURCE/name),want)
  header=(SOURCE/'contrib/og_lake/lake_native_partition.h').read_text()
  check('retained read macro23 remains',[x for x in re.findall(r'^#define OG_LAKE_NATIVE_PARTITION_SQL_VERSION "([^"]*)"',header,re.M)],['0.23'])
  check('retained INSERT macro23 remains',re.findall(r'^#define OG_LAKE_NATIVE_PARTITION_WRITE_SQL_VERSION "([^"]*)"',header,re.M),['0.23'])
  check('M24 mutation macro exact',re.findall(r'^#define OG_LAKE_NATIVE_PARTITION_MUTATION_SQL_VERSION "([^"]*)"',header,re.M),[MUTATION_SQL_VERSION])
  check('M24 COPY macro exact',re.findall(r'^#define OG_LAKE_NATIVE_PARTITION_COPY_SQL_VERSION "([^"]*)"',header,re.M),[MUTATION_SQL_VERSION])
  check('separate A25 ANALYZE macro exact',re.findall(r'^#define OG_LAKE_NATIVE_PARTITION_ANALYZE_SQL_VERSION "([^"]*)"',header,re.M),[ANALYZE_SQL_VERSION])
  qual=sdk_index(a.sdk_qualification,a.sdk_index_sha256,ROOT);sdk=INSTALL/'lib/postgresql/og_lake_sdk';owner=json.loads((sdk/'owner.json').read_text())
  files={str(f.relative_to(sdk)):digest(f)for f in sdk.rglob('*')if f.is_file()and f.name!='owner.json'}
  check('SDK exact owned target',str(Path(owner['target']).resolve()),str(sdk.resolve()));check('SDK complete owned tree',files,owner['files']);check('SDK no symlinks',not any(f.is_symlink()for f in sdk.rglob('*')))
  check('SDK owner project',owner['project'],'opengauss-native-lake-local-provider');check('SDK atomic prepared phase',owner['phase'],'prepared-for-atomic-install');check('SDK reviewed qualification kind',owner['qualification_kind'],'verified_independent_sdk_sql020_int16_only')
  check('SDK exact index ownership',owner['qualification_sha256'],a.sdk_index_sha256);check('SDK actual provider receipt',files['libog_lake_sdk_arrow.so'],qual['library_sha256'])
  public={str(SOURCE/'contrib/og_lake'/name):digest(SOURCE/'contrib/og_lake'/name)for name in['og_lake.control','sdk/lake_parquet_sdk.h','vendor/arrow20/abi.h']}
  check('SDK actual Native public owner inputs',owner['native_public_headers'],public);check('SDK full header qualification',files['include/og_lake/lake_parquet_sdk.h'],qual['header_sha256']);check('Native full SDK header',public[str(SOURCE/'contrib/og_lake/sdk/lake_parquet_sdk.h')],qual['header_sha256'])
  duckdb=ROOT/'.reference/opengauss-native-lake/build/duckdb-gcc10/duckdb';check('locked independent DuckDB binary',digest(duckdb),contract.get('reference_binary_sha256'))
  inputs={Path(__file__),a.source_contract,a.build_receipt,a.sdk_qualification,sdk/'owner.json',D/'source-lock.json',D/'reference.sh',D/'fixture.py'}
  inputs|={D/name for name in helper_inputs};inputs|={SOURCE/name for name in contract['native_inputs']};inputs|={Path(name)for name in build.get('build_wrapper_inputs_after',{})};inputs|={Path(name)for name in contract.get('review_evidence',{})};inputs|={SOURCE/name for name in references}
  inputs|={D/name for name in ['inline_fixture.py','native_test_context.py','native_runtime.py','lake_harness.py','pq_sql020_build_inputs.py','pq_native_build_inputs.py']}
  inputs|={D/'tests'/name for name in ['native_table_create.py','native_file_analyze.py','file_dml_acceptance.py','permission_api.py','inline_cleanup.py','native_auto_flush.py','catalog_fixture_bundle.py','empty_manifest.py','empty_reference_fixture.py','empty_catalog_oracle.py','create_reference_oracle.py']}
  pending=list(inputs)
  while pending:
   f=pending.pop()
   if f.suffix!='.py':continue
   for node in ast.walk(ast.parse(f.read_text())):
    names=[x.name.split('.')[0]for x in node.names]if isinstance(node,ast.Import)else[node.module.split('.')[0]]if isinstance(node,ast.ImportFrom)and node.module else[]
    for name in names:
     for directory in[D,D/'tests']:
      candidate=directory/(name+'.py')
      if candidate.is_file()and candidate not in inputs:inputs.add(candidate);pending.append(candidate)
  inputs|=set((INSTALL/'share/postgresql/extension').glob('og_lake*.sql'));archive=out/'executed-inputs';archive.mkdir();r['inputs']={}
  for i,f in enumerate(sorted({f.resolve()for f in inputs},key=str)):
   saved=archive/(str(i)+'-'+f.name);shutil.copy2(f,saved);check('executed text archive/'+str(i),digest(saved),digest(f));r['inputs'][str(f)]=dict(sha256=digest(f),archive=str(saved))
  r['hash_only_inputs']={str(f):digest(f)for f in[INSTALL/'bin/gaussdb',duckdb]+[sdk/name for name in files]}
  r.update(build=str(a.build_receipt),build_sha256=digest(a.build_receipt),binary_sha256=build['binary_sha256'],source_head=build['source_head'],executed=True)
  control=connect_control();configure(control);check('maintenance remains off',scalar(control,'SHOW enable_lake_maintenance'),'off');check('true backend PID identities without thread pool',scalar(control,'SHOW enable_thread_pool'),'off')
  with(out/'fixture.log').open('w')as log:subprocess.run([sys.executable,str(D/'tests/catalog_fixture_bundle.py'),'--run-id',a.run_id,'--output',str(out/'fixture/manifest.json')],stdout=log,stderr=subprocess.STDOUT,check=True,timeout=300)
  reservation=(ROOT/'.reference/opengauss-native-lake/.driver.lock').open('a+');fcntl.flock(reservation.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB)
  db=OwnedDatabase(a.run_id,out);db.create();configure(db.admin);roles=OwnedRoles(control,a.run_id,out)
  initial_sql='0.23'if a.mode=='binding-stats'else a.sql_version
  t=Case(db,SimpleNamespace(mode='net_dml',sql_version=initial_sql,manifest=out/'fixture/manifest.json',output=out/'result.json',run_id=a.run_id),r,roles)
  target=1 if a.mode=='sampling-skew'else 10
  for c in[t.a,t.b,t.observer]:configure(c,target)
  check('actual initial installed SQL contract',current_version(),initial_sql);check('actual matching target runtime module',json.loads(scalar(t.a,'SELECT og_lake.host_status()'))['module_version'],a.sql_version)
  groups_to_make=[('stale',8,32),('empty',8,32),('stale',8,32),('empty',8,32),('nonempty',8,32)]if a.mode=='binding-stats'else[('empty',128,4000)]if a.mode=='sampling-skew'else[('empty',8,32)]
  all_groups=[create_group(i,low,high,shape)for i,(shape,low,high)in enumerate(groups_to_make)]
  before_drop=t.protocol()
  for g in all_groups:
   for name in g['source_names']:query(t.a,sql.SQL('DROP FOREIGN TABLE biz.{}').format(sql.Identifier(name)))
  check('dropping setup aliases preserves complete standard lake protocol',t.protocol(),before_drop)
  check('all source tables have no aliases',scalar(t.a,'SELECT count(*)FROM og_lake_internal.table_binding WHERE table_id=ANY(%s::bigint[])',([tid for g in all_groups for tid in g['tables']],)),0)
  domains={g['name']:physical_domain(g,g['name']+'-before')for g in all_groups}
  r['prepared_groups']=all_groups;write_json(out/'result.json',r)
  if a.mode=='binding-stats':
   old_groups=all_groups[:2]
   for g in old_groups:
    before=stats_image(g);bind(g);check(g['name']+' retained23 binds without pretending ANALYZE activation',stats_image(g)['columns'],before['columns']);check(g['name']+' SQL23 read remains usable before upgrade',business(g),oracle(g))
   before=[stats_image(g)for g in old_groups];protocol=t.protocol();upgrade=sql.SQL('ALTER EXTENSION og_lake UPDATE TO {}').format(sql.Literal(a.sql_version))
   # A genuine old-parent cursor holds AS, observed natively. Busy AX must
   # abort the entire upgrade, not skip this parent and publish targetA.
   query(t.b,'BEGIN');query(t.b,sql.SQL('DECLARE upgrade_hold NO SCROLL CURSOR FOR SELECT shard,id,val FROM biz.{}').format(sql.Identifier(old_groups[-1]['name'])))
   holder=scalar(t.b,'SELECT pg_backend_pid()');held=query(t.observer,"SELECT mode,granted FROM pg_locks WHERE pid=%s AND relation=%s AND mode='AccessShareLock'AND granted",(holder,old_groups[-1]['oid']))
   check('real old-parent AS lock protects busy upgrade',len(held)>0)
   query(t.a,'BEGIN');deny(t.a,upgrade,'55P03');query(t.a,'ROLLBACK')
   check('busy upgrade keeps retained version',current_version(),'0.23');check('busy upgrade keeps ALL old parent statistics',[stats_image(g)for g in old_groups],before);check('busy upgrade does not publish lake changes',t.protocol(),protocol)
   query(t.b,'CLOSE upgrade_hold');query(t.b,'COMMIT')
   query(t.a,'BEGIN');query(t.a,upgrade)
   check('same upgrade transaction sees targetA',current_version(t.a),a.sql_version)
   for g in old_groups:require_reset(g,t.a)
   check('other session retains committed23 until upgrade commit',current_version(),'0.23');check('observer raw stats retain old committed images during AX',[stats_image(g)for g in old_groups],before)
   query(t.a,'ROLLBACK');check('outer upgrade rollback restores23',current_version(),'0.23');check('outer upgrade rollback restores all raw standard stats',[stats_image(g)for g in old_groups],before)
   query(t.a,upgrade);check('successful real upgrade commits targetA',current_version(),a.sql_version)
   for g in old_groups:require_reset(g);check(g['name']+' targetA lake reader never falls back to empty local heaps',business(g),oracle(g))
   check('upgrader leaves DuckLake source catalog/head unchanged',t.protocol(),protocol)
   deny(t.a,'SELECT og_lake_internal._native_parent_statistics_upgrade()','0A000')
   for g in all_groups[2:-1]:
    before=stats_image(g);before_protocol=t.protocol()
    query(t.a,'BEGIN');query(t.a,'SAVEPOINT bind_reset');bind(g);require_reset(g,t.a)
    check(g['name']+' observer raw stats cannot see uncommitted binding reset',stats_image(g),before);query(t.a,'ROLLBACK TO bind_reset')
    check(g['name']+' savepoint rollback restores raw catalogs',stats_image(g,t.a),before);check(g['name']+' marker removed on savepoint rollback',marker(g['oid'],t.a),False)
    query(t.a,'RELEASE bind_reset');query(t.a,'ROLLBACK');check(g['name']+' whole rollback still exact',stats_image(g),before)
    query(t.a,'BEGIN');bind(g);require_reset(g,t.a);query(t.a,'ROLLBACK');check(g['name']+' direct outer rollback restores exact old stats',stats_image(g),before)
    bind(g);require_reset(g);check(g['name']+' committed new binding reads original lake rows',business(g),oracle(g));check(g['name']+' binding never consumes lake snapshot',t.protocol(),before_protocol)
   bad=all_groups[-1];before=stats_image(bad);before_protocol=t.protocol()
   deny(t.a,'SELECT og_lake.bind_native_partition_readonly(%s::regclass,%s::name,%s::oid[],%s::bigint[])','55000',(bad['oid'],t.server,bad['leaves'],bad['tables']))
   check('physically nonempty scaffold never bypasses certification',marker(bad['oid']),False);check('physical refusal does not clear old statistics',stats_image(bad),before);check('physical refusal preserves original local row',business(bad),[(1,-900000,12)]);check('physical refusal leaves lake sources unchanged',t.protocol(),before_protocol)
  else:
   g=all_groups[0];domain=domains[g['name']];head=t.head();bind(g);require_reset(g);check('binding shares exact head without business snapshot',t.head(),head)
   check('native parent complete rows equal independent original DML',business(g),oracle(g));protocol=t.protocol()
   if a.mode=='whole-visible':
    query(t.a,sql.SQL('ANALYZE biz.{}').format(sql.Identifier(g['oracle'])));sample_check(g,'readonly-p1',domain,target)
    query(t.a,'SELECT og_lake.enable_native_partition_insert(%s::regclass)',(g['oid'],));check('two independent leaf views promoted together',[row[-1]for row in maps(g['oid'])],[False,False])
    sample_check(g,'promoted-p1',domain,target);check('ANALYZE and promotion publish no lake data protocol',t.protocol(),protocol)
    z=stats_image(g);check('whole-parent creates only c/non-inherited distribution rows',[(row[0],row[1],row[3])for row in z['columns']],[('c',g['oid'],False)]*3)
    check('whole-parent does not invent per-leaf distributions',[row for row in z['columns']if row[0]=='p'],[])
    plan=query(t.a,sql.SQL('EXPLAIN SELECT shard,id,val FROM biz.{} WHERE val=1').format(sql.Identifier(g['name'])));write_json(out/'analyzed-parent-plan.json',plan)
    check('CBO keeps native lake read path after ANALYZE',any('Foreign Scan'in row[0]for row in plan));check('CBO retains explicit native parent binding label',any('Native Lake Parent'in row[0]for row in plan))
   elif a.mode=='sampling-skew':
    requested=requested_int4_sample_rows(target,query(t.observer,'SELECT attname,atttypid::integer,attstattarget FROM pg_attribute WHERE attrelid=%s AND attnum>0 AND NOT attisdropped ORDER BY attnum',(g['oid'],)))
    floor=fixture_survivor_floor(domain['physical'],requested);r['global_survivor_floor']=dict(floor,probability=str(floor['probability']))
    check('one global domain actually crosses virtual block boundary',len(domain['physical'])>4096);check('each captured leaf remains within certified4096 bound',all(c['files']+c['inline']<=4096 for c in domain['leaf_counts']))
    check('all-seed visible floor prevents thin-sample refusal',floor['minimum_kept']>=min(requested,max(100,requested//4)));check('global selection probability genuinely below one',floor['probability']<1)
    # Exactly three calls. Native owns its UUID seed; do not keep trying new
    # seeds until a preferred I/O observation appears. Every seed is replayed.
    samples=[sample_check(g,'skew-'+str(turn),domain,target,True)for turn in range(3)]
    seeds=[tuple(int(word)for word in evidence['seed'].split(','))for evidence in samples]
    sample_positions=[selected_positions(evidence)for evidence in samples]
    r['three_sample_diversity']=dict(seeds=seeds,positions=sample_positions,calls=3,retries=0)
    write_json(out/'result.json',r)
    check('three actual sampling seeds are distinct',len(set(seeds)),3)
    check('three actual physical position sequences are distinct',len({tuple(positions)for positions in sample_positions}),3)
    if not any(x['partial_data']for x in r.get('partial_decode_observations',[])):
     r.setdefault('coverage_gaps',[]).append('All3 real seeds touched all data RGs: partial positions proven, actual data-RG skipping not covered; no retry/seed substitution performed')
    check('three-seed sampling never mutates lake protocol',t.protocol(),protocol)
   else:
    query(t.a,sql.SQL('ANALYZE biz.{}').format(sql.Identifier(g['oracle'])));sample_check(g,'failure-baseline',domain,target)
    baseline=stats_image(g);baseline_stats=pgstats(g['name']);old_count=len(oracle(g))
    query(t.a,"CREATE FUNCTION biz.analyze_pause(oid,bigint,bigint)RETURNS void AS '$libdir/og_lake','og_lake_test_analyze_pause'LANGUAGE C VOLATILE NOT FENCED");query(t.a,'REVOKE ALL ON FUNCTION biz.analyze_pause(oid,bigint,bigint)FROM PUBLIC')
    w=begin_worker(g,'cancel');worker_identity(w);check('cancel exact sampled-row actor delivered',scalar(t.observer,'SELECT pg_cancel_backend(%s)',(w['pid'],)));release_gate();reply=finish_worker(w)
    check('actual live sampling cancellation is57014',reply['sqlstate'],'57014');check('cancel keeps all old standard statistics',stats_image(g),baseline);check('cancel keeps complete lake source protocol',t.protocol(),protocol)
    check('same canceled actor backend reuses native reader',business(g),oracle(g));sample_check(g,'post-cancel-reuse',domain,target)
    # A new real statement captures the oldS before a routed INSERT commits.
    # The independent old oracle stays untouched until that first statement
    # has finished, then a later analysis is compared with the new heap truth.
    query(t.a,'SELECT og_lake.enable_native_partition_insert(%s::regclass)',(g['oid'],));before_mvcc=t.protocol();head_before=t.head();w=begin_worker(g,'captured-snapshot')
    query(t.b,sql.SQL('INSERT INTO biz.{} VALUES(1,-900001,123)').format(sql.Identifier(g['name'])));query(t.b,sql.SQL('INSERT INTO biz.{} VALUES(1,-900001,123)').format(sql.Identifier(g['oracle'])))
    head_after=t.head();protocol_after_writer=t.protocol();check('concurrent routed INSERT advances exactly one catalog S',head_after[0],head_before[0]+1);release_gate();reply=finish_worker(w)
    check('paused old-S ANALYZE completes',reply['state'],'completed');check('old-S ANALYZE live rows remain old captured count',scalar(t.observer,'SELECT reltuples FROM pg_class WHERE oid=%s',(g['oid'],)),float(old_count));check('old-S distributions remain captured old heap',pgstats(g['name']),baseline_stats)
    check('old analysis allocates no extra snapshot after writer commit',t.head(),head_after);check('old analysis cannot add lake changes beyond committed writer',t.protocol(),protocol_after_writer);check('new parent statement sees independently inserted row',business(g),oracle(g))
    # Old-S checks used the frozen baseline; the new-S heap oracle now needs fresh statistics.
    query(t.a,sql.SQL('ANALYZE biz.{}').format(sql.Identifier(g['oracle'])))
    new_domain=physical_domain(g,'after-concurrent-insert');sample_check(g,'later-snapshot',new_domain,target)
    guard_baseline=stats_image(g);guard_protocol=t.protocol()
    query(t.a,'SET default_statistics_target=-1');deny(t.a,sql.SQL('ANALYZE biz.{}').format(sql.Identifier(g['name'])));query(t.a,'SET default_statistics_target=10');check('unsupported target preserves valid statistics',stats_image(g),guard_baseline)
    query(t.a,'BEGIN');query(t.a,'SAVEPOINT explicit_transaction');deny(t.a,sql.SQL('ANALYZE biz.{}').format(sql.Identifier(g['name'])),'25001');query(t.a,'ROLLBACK TO explicit_transaction');check('transaction-chain refusal rolls back unchanged',stats_image(g,t.a),guard_baseline);query(t.a,'ROLLBACK')
    for template in['ANALYZE biz.{} PARTITION(low)','VACUUM ANALYZE biz.{}','VACUUM biz.{}','REINDEX TABLE biz.{}','CLUSTER biz.{}']:
     deny(t.a,sql.SQL(template).format(sql.Identifier(g['name'])));check('closed utility cannot modify parent stats/'+template,stats_image(g),guard_baseline)
    t.a.notices=Notices();query(t.a,'ANALYZE');check('database-wide ANALYZE skips marked parent',stats_image(g),guard_baseline);check('database-wide run does not sample native parent',t.a.notices.target_seen,0)
    role=roles.create('stats_reader',login=True);query(t.a,sql.SQL('GRANT USAGE ON SCHEMA biz TO {}').format(sql.Identifier(role)));query(t.a,sql.SQL('GRANT SELECT ON biz.{} TO {}').format(sql.Identifier(g['name']),sql.Identifier(role)))
    role_baseline=stats_image(g);reader,identity=roles.connect(db,'stats_reader');r['stats_reader_identity']=identity
    try:
     query(reader,'SET session_timeout=0');query(reader,'SET statement_timeout=60000')
     check('SELECT-only login is neither table nor database owner',scalar(t.observer,'SELECT c.relowner<>r.oid AND d.datdba<>r.oid FROM pg_class c,pg_database d,pg_roles r WHERE c.oid=%s AND d.datname=current_database() AND r.rolname=%s',(g['oid'],role)))
     reader.notices=[];query(reader,sql.SQL('ANALYZE biz.{}').format(sql.Identifier(g['name'])))
    finally:
     role_notices=list(reader.notices);reader.close();write_json(out/'select-only-role-notices.json',role_notices)
    check('SELECT-only actor follows actual host skip warning',any('skipping'in message and g['name']in message for message in role_notices));check('SELECT-only role cannot replace valid statistics',stats_image(g),role_baseline);check('guards and role skip preserve lake protocol',t.protocol(),guard_protocol)
  check('all text inputs remained exact',all(digest(path)==v['sha256']for path,v in r['inputs'].items()));check('all binary/SDK payloads remained exact',all(digest(path)==value for path,value in r['hash_only_inputs'].items()));check_build(a.build_receipt)
  r.update(status='fail'if r.get('coverage_gaps')else'pass',qualification_scope='Bounded four-mode manual whole-parent source/statistics protocol only; never Native commercial qualification')
  if r.get('coverage_gaps'):r.update(reason='Functional assertions passed, but required actual partial-data decode coverage was not observed in3 fixed-count calls',functional_assertions_passed=True)
 except BaseException as exc:r.update(reason=str(exc),traceback=traceback.format_exc())
 finally:
  # Never use an SQL connection concurrently with its active Python actor.
  for w in workers:
   if w.get('drained'):continue
   try:
    if not w['future'].done():worker_identity(w);query(t.observer,'SELECT pg_cancel_backend(%s)',(w['pid'],))
    release_gate();w['future'].result(timeout=20);w['pool'].shutdown(wait=True);w['drained']=True
    capture_worker(w,'cleanup',w['future'].result(timeout=0))
   except BaseException as error:w['pool'].shutdown(wait=False,cancel_futures=True);r.setdefault('undrained_actors',[]).append(dict(pid=w['pid'],reason=str(error)));preserve=True
  if gate_held and t is not None:
   try:release_gate()
   except BaseException as error:r.setdefault('gate_cleanup_errors',[]).append(str(error));preserve=True
  if preserve:
   r.update(status='fail',preserve_owned_scene=True,preserved_database=db.allocation if db else None);write_json(out/'result.json',roles.redact(r)if roles else r)
   # Avoid ThreadPoolExecutor atexit joining an unproved actor forever. Only
   # the outer nonce-owned supervisor may retire this Python process group.
   os._exit(2)
  if t:
   for c in[t.a,t.b,t.observer]:
    try:query(c,'ROLLBACK');c.close()
    except BaseException as error:r.setdefault('connection_cleanup_errors',[]).append(str(error))
  if db:
   try:db.cleanup();r['database_cleanup']=db.cleanup_result
   except BaseException as error:r.update(status='fail',database_cleanup_error=str(error),preserve_owned_scene=True)
  if roles:
   try:
    if not r.get('database_cleanup')or r['database_cleanup']['returncode']:raise RuntimeError('owned DB cleanup unproven; roles retained')
    r['role_cleanup']=cleanup_roles(control,roles.path,a.run_id);r=roles.redact(r)
   except BaseException as error:r.update(status='fail',role_cleanup_error=str(error))
  if control:
   try:control.close()
   except BaseException as error:r.update(status='fail',control_cleanup_error=str(error))
  if reservation:fcntl.flock(reservation.fileno(),fcntl.LOCK_UN);reservation.close();r['reference_lock_released_after_cleanup_attempt']=True
  write_json(out/'result.json',r)
 print(json.dumps(dict(status=r['status'],mode=a.mode,checks=len(r['assertions']),reason=r.get('reason'),result=str(out/'result.json'))))
 return 0 if r['status']=='pass'else 1
if __name__=='__main__':raise SystemExit(main())
