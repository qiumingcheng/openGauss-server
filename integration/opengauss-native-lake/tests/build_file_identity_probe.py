#!/usr/bin/env python3
"""Compile a test-only shared object against the exact installed native ABI."""
import argparse,hashlib,json,os,shlex,subprocess
from pathlib import Path
ROOT=Path('/home/qiumc/data/code/cpp');PREFIX=ROOT/'.install/opengauss-native-lake-debug';SOURCE=ROOT/'.worktrees/opengauss-native-lake'
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def main():
 p=argparse.ArgumentParser();p.add_argument('--evidence',type=Path,required=True);args=p.parse_args();e=args.evidence.resolve();e.mkdir(exist_ok=False)
 source=Path(__file__).with_name('native_file_identity_probe.cpp');compiler=ROOT/'binarylibs/buildtools/gcc10.3/gcc/bin/g++';gcc=compiler.parent.parent
 env=os.environ.copy();env['GAUSSHOME']=str(PREFIX);env['LD_LIBRARY_PATH']=':'.join(map(str,[PREFIX/'lib',gcc/'lib64']+[gcc.parent/n/'lib'for n in ['isl','mpc','mpfr','gmp']]))
 result={'status':'failed','scope':'test-only native identity observation plugin; no product API installation','source_sha256':sha(source),'compiler_sha256':sha(compiler),'native_binary_sha256':sha(PREFIX/'bin/gaussdb'),'driver_sha256':sha(__file__)}
 def config(option):return subprocess.check_output([str(PREFIX/'bin/pg_config'),option],env=env,text=True).strip()
 try:
  flags=shlex.split(config('--cppflags'))+shlex.split(config('--cflags'))
  command=[str(compiler),*flags,'-fPIC','-shared','-I'+config('--includedir-server'),'-I'+str(SOURCE/'contrib/og_lake'),'-I'+str(SOURCE/'src/include'),'-MMD','-MF',str(e/'dependencies.d'),str(source),'-o',str(e/'file_identity_probe.so')]
  result['command']=command
  (e/'source.cpp').write_bytes(source.read_bytes());(e/'driver.py').write_bytes(Path(__file__).read_bytes())
  with(e/'compile.log').open('w')as log:subprocess.run(command,env=env,stdout=log,stderr=subprocess.STDOUT,check=True,timeout=120)
  data=(e/'dependencies.d').read_text().replace('\\\n',' ');dependencies=shlex.split(data.split(':',1)[1])
  result['headers']={str(Path(f).resolve()):sha(f)for f in dependencies if Path(f).suffix in ['.h','.hpp']}
  if result['native_binary_sha256']!=sha(PREFIX/'bin/gaussdb') or result['source_sha256']!=sha(source):raise RuntimeError('native ABI or probe source changed during compile')
  result.update(status='built_unloaded',library=str(e/'file_identity_probe.so'),library_sha256=sha(e/'file_identity_probe.so'));print(result['library']);return 0
 except BaseException as exc:result['error']=str(exc);print(str(exc));return 1
 finally:(e/'result.json').write_text(json.dumps(result,indent=2)+'\n')
if __name__=='__main__':raise SystemExit(main())
