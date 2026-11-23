#!/usr/bin/env python3
"""Sequential new-prefix SDK/PQ qualification only; prepare unless --execute.
No Native build/deployment, shared installed SDK change, service or source-lock.
"""
import argparse,datetime,hashlib,json,os,re,secrets,signal,subprocess,sys,traceback
from pathlib import Path
D=Path(__file__).resolve().parents[1]
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--execute',action='store_true');p.add_argument('--workspace',type=Path,required=True);p.add_argument('--run-id',required=True);p.add_argument('--source',type=Path,required=True);p.add_argument('--legacy-source',type=Path,required=True)
 a=p.parse_args();root=a.workspace.resolve(strict=True);e=root/'公共文档/openGauss原生湖存储/验证'
 if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,39}',a.run_id):p.error('safe new base run-id max40')
 names={key:a.run_id+'_'+key for key in ['sdk','sdkqual','pq','pqasan','static']}
 sdkprefix=root/'.install/opengauss-native-lake-sdk-smallint'/names['sdk'];sdkbuild=root/'.build/opengauss-native-lake/sdk-smallint'/names['sdk']
 q=lambda name:e/names[name]/'index.json'
 commands=[dict(name='provider',argv=[sys.executable,str(root/'integration/opengauss-native-lake/sdk/build_provider.py'),'--provider-source',str(a.source.resolve()/'contrib/og_lake/sdk'),'--build-directory',str(sdkbuild),'--provider-prefix',str(sdkprefix),'--evidence',str(e/names['sdk'])]),
  dict(name='sdkqual',argv=[sys.executable,str(D/'sdk/qualify_sql020_provider.py'),'--execute','--workspace',str(root),'--run-id',names['sdkqual'],'--provider-build',str(e/names['sdk']/'build-result.json'),'--source',str(a.source.resolve()),'--legacy-source',str(a.legacy_source.resolve())])]
 plan=dict(status='prepared_not_executed',scope=__doc__,component_names=names,initial_commands=commands,remaining=['PQ full6-case normal with actual SDK index SHA','PQ full6-case ASan/UBSan with actual same SDK index SHA','new owned static archive using those actual index SHA values'],native_calls=0,sdk_loads=0,builds=0)
 if not a.execute:print(json.dumps(plan,ensure_ascii=False,indent=2));return 0
 out=e/a.run_id
 if out.exists()or sdkprefix.exists()or sdkbuild.exists()or any((e/name).exists()for name in names.values()):raise FileExistsError('Never reuse qualification output/prefix/build roots')
 out.mkdir();r=dict(status='running',commands=[],native_integration=False,source_lock_updated=False)
 def save():
  temporary=out/'result.json.tmp'
  with temporary.open('w')as stream:
   json.dump(r,stream,ensure_ascii=False,indent=2);stream.write('\n');stream.flush();os.fsync(stream.fileno())
  os.replace(temporary,out/'result.json')
  directory=os.open(out,os.O_RDONLY|os.O_DIRECTORY)
  try:os.fsync(directory)
  finally:os.close(directory)
 env=dict(os.environ,CPP_WORKSPACE=str(root),PYTHONDONTWRITEBYTECODE='1',OMP_NUM_THREADS='1',OPENBLAS_NUM_THREADS='1')
 def run(name,argv,timeout):
  row=dict(name=name,argv=list(map(str,argv)),returncode=None);r['commands'].append(row)
  r.update(active_phase=name,active_pid=None);save()
  nonce=secrets.token_hex(24);child_env=dict(env,OG_LAKE_SQL020_PHASE_OWNER=nonce)
  with(out/(name+'.stdout')).open('w')as so,(out/(name+'.stderr')).open('w')as se:
   child=subprocess.Popen(row['argv'],env=child_env,stdin=subprocess.DEVNULL,stdout=so,stderr=se,start_new_session=True)
   row['pid']=child.pid;row['new_session']=True;row['signals']=[]
   row['owner_uid']=os.geteuid();row['owner_nonce_sha256']=hashlib.sha256(nonce.encode()).hexdigest()
   row['started_at_utc']=datetime.datetime.now(datetime.timezone.utc).isoformat()
   def members():
    result=[]
    for entry in Path('/proc').iterdir():
     if not entry.name.isdigit():continue
     try:
      stat=(entry/'stat').read_text().rsplit(')',1)[1].split()
      if int(stat[2])==child.pid and int(stat[3])==child.pid and stat[0]!='Z':
       tagged=('OG_LAKE_SQL020_PHASE_OWNER='+nonce).encode()in(entry/'environ').read_bytes().split(b'\0')
       result.append((int(entry.name),entry.stat().st_uid==os.geteuid()and tagged))
     except(FileNotFoundError,ProcessLookupError,PermissionError):continue
    return result
   def stop():
    owned=members()
    if owned:
     if not all(ok for _,ok in owned)or child.pid==os.getpgrp():raise RuntimeError('Refuse unproven processgroup; retain exact failed scene')
     os.killpg(child.pid,signal.SIGKILL);row['signals'].append({'signal':int(signal.SIGKILL),'verified_owned_pids':[pid for pid,_ in owned]})
    try:child.wait(timeout=10)
    except subprocess.TimeoutExpired:raise RuntimeError('Owned phase group did not drain; retain scene')
    finally:save()
   try:
    r['active_pid']=child.pid;save()  # Persist PID before any procfs read/wait.
    print(json.dumps({'phase':name,'pid':child.pid,'status':'running'}),flush=True)
    leader=Path('/proc')/str(child.pid);tail=(leader/'stat').read_text().rsplit(')',1)[1].split()
    row['leader_identity']={'pid':child.pid,'start_ticks':int(tail[19]),'pgrp':int(tail[2]),'session':int(tail[3]),'uid':leader.stat().st_uid}
    if row['leader_identity']['pgrp']!=child.pid or row['leader_identity']['session']!=child.pid or row['leader_identity']['uid']!=os.geteuid():
     raise RuntimeError('Phase leader identity does not match owned new session')
    save()
    child.wait(timeout=timeout)
   except BaseException as exc:
    row['phase_error']=str(exc)
    try:stop()
    finally:row['returncode']=child.returncode;save()
    raise
   row['returncode']=child.returncode;save()
   if child.returncode or members():
    stop();raise RuntimeError(name+' failed or retained descendants; preserve exact prefixes/results; stop')
   r.update(active_phase=None,active_pid=None);save()
 try:
  for cmd in commands:run(cmd['name'],cmd['argv'],2400)
  for kind in ['pq','pqasan']:
   run(kind,[sys.executable,D/'indexes/pq_sql020_parquet_component.py','--workspace',root,'--run-id',names[kind],'--sdk-index',q('sdkqual'),'--sdk-index-sha256',sha(q('sdkqual')),*(['--asan']if kind=='pqasan'else[])],2400)
  run('static',[sys.executable,D/'indexes/pq_sql020_dependency_build.py','--workspace',root,'--run-id',names['static'],'--component-index',q('pq'),'--component-index-sha256',sha(q('pq')),'--asan-index',q('pqasan'),'--asan-index-sha256',sha(q('pqasan'))],2400)
  receipt=e/names['static']/'build.json'
  sys.path[:0]=[str(D),str(root/'integration/opengauss-native-lake')]
  from pq_sql020_build_inputs import pq_inputs
  qualified=pq_inputs(receipt,root);r.update(status='qualified_components_only',pq_build=str(receipt),pq_build_sha256=sha(receipt),qualified_dependency=qualified)
 except BaseException as exc:r.update(status='failed',reason=str(exc),traceback=traceback.format_exc())
 save();print(json.dumps(dict(status=r['status'],result=str(out/'result.json'),pq_build=r.get('pq_build'),reason=r.get('reason'))));return 0 if r['status']=='qualified_components_only'else 1
if __name__=='__main__':raise SystemExit(main())
