#!/usr/bin/env python3
"""Build an independent C API client from the exact existing reference archives."""
import argparse,datetime,hashlib,json,os,shlex,subprocess
from pathlib import Path
ROOT=Path('/home/qiumc/data/code/cpp');BUILD=ROOT/'.reference/opengauss-native-lake/build/duckdb-gcc10'
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def main():
 p=argparse.ArgumentParser();p.add_argument('--evidence',type=Path,required=True);args=p.parse_args();e=args.evidence.resolve();e.mkdir(exist_ok=False)
 source=Path(__file__).with_suffix('.c');header=ROOT/'.reference/opengauss-native-lake/ducklake/duckdb/src/include/duckdb.h';client=e/'c-api-flush-client';receipt={'status':'failed','scope':'actual fixed-reference shared DuckDB host with two real C API connections; not native/RAC qualification','source_sha256':sha(source),'header_sha256':sha(header),'driver_sha256':sha(__file__),'started_at':datetime.datetime.now(datetime.timezone.utc).isoformat()}
 (e/source.name).write_bytes(source.read_bytes());(e/'driver.py').write_bytes(Path(__file__).read_bytes())
 def run(label,argv,timeout):
  with(e/(label+'.log')).open('w')as log:subprocess.run(list(map(str,argv)),cwd=BUILD,stdout=log,stderr=subprocess.STDOUT,check=True,timeout=timeout)
 try:
  shell=subprocess.check_output(['ninja','-C',str(BUILD),'-t','commands','shell'],text=True).splitlines()[-1]
  tokens=shlex.split(shell)[2:-2];receipt['original_shell_link']=tokens
  # Reuse the unchanged core/static-extension archives. Shell main objects and
  # libduckdb_shell are replaced; static autoregistration remains exact.
  libs=[t for t in tokens if t.endswith('.a') and 'libduckdb_shell.a'not in t]
  auto=BUILD/'tools/shell/CMakeFiles/shell.dir/__/__/extension/loader/static_extension_autoregister.cpp.o'
  if not auto.is_file():raise RuntimeError('exact static extension registration object missing')
  loader=BUILD/'tools/shell/CMakeFiles/shell.dir/shell_static_extension_loader.c.o'
  if not loader.is_file():raise RuntimeError('exact static extension loader missing')
  receipt['linked_inputs']={str((BUILD/f).resolve()):sha(BUILD/f)for f in libs}
  receipt['linked_inputs'].update({str(auto):sha(auto),str(loader):sha(loader)})
  run('compile',['/usr/bin/cc','-std=c11','-O1','-Wall','-Wextra','-Werror','-I'+str(header.parent),'-c',source,'-o',e/'client.o'],60)
  command=['/usr/bin/c++','-O1',str(e/'client.o'),str(auto),str(loader),'-o',str(client)]+libs+['-ldl','-lpthread']
  receipt['link_command']=command;run('link',command,120)
  run('interleavings',[client,e],120)
  output=(e/'interleavings.log').read_text()
  if 'PASS actual_reference_flush_interleavings'not in output:raise RuntimeError('client did not complete actual transaction checks')
  receipt.update(status='passed',client_sha256=sha(client),outcome_line=output.splitlines()[-1])
  for f,h in receipt['linked_inputs'].items():
   if sha(f)!=h:raise RuntimeError('reference archive changed during transaction test')
  print(receipt['outcome_line']);return 0
 except BaseException as exc:receipt.update(status='failed',error=str(exc));print(str(exc));return 1
 finally:(e/'result.json').write_text(json.dumps(receipt,indent=2)+'\n')
if __name__=='__main__':raise SystemExit(main())
