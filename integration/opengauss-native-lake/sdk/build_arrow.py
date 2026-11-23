#!/usr/bin/env python3
"""Build an exact, isolated Apache Arrow Parquet provider dependency candidate."""
import argparse,datetime,hashlib,json,os,re,subprocess,time,urllib.request
from pathlib import Path
ROOT=Path(__file__).resolve().parents[3]
SDK=ROOT/'.reference/opengauss-native-lake/sdk'
PIN='3ad0370a04ccdae638755b94c3c31c8760a11193'
SOURCE=SDK/'arrow-20.0.0'
BUILD=SDK/'build/arrow-20.0.0'
PREFIX=SDK/'install/arrow-20.0.0'
CMAKE=SDK/'build-tools/bin/cmake'
GCC=ROOT/'binarylibs/buildtools/gcc10.3/gcc'

def sha(p):
 h=hashlib.sha256()
 with Path(p).open('rb') as f:
  for b in iter(lambda:f.read(1048576),b''):h.update(b)
 return h.hexdigest()

def main():
 p=argparse.ArgumentParser();p.add_argument('--evidence',type=Path,required=True);p.add_argument('--jobs',type=int,default=2)
 p.add_argument('--with-static',action='store_true',help='Also install PIC static archives for a self-contained C ABI provider')
 args=p.parse_args()
 if args.jobs not in (1,2):p.error('SDK build is limited to 1 or 2 jobs')
 evidence=args.evidence.resolve();evidence.mkdir(parents=True,exist_ok=True)
 receipt={'scope':'additional Apache Arrow20 local Parquet dependency candidate; not private SDK qualification','status':'failed','source':str(SOURCE),'expected_commit':PIN,'build':str(BUILD),'prefix':str(PREFIX),'commands':[],'started_at':datetime.datetime.now(datetime.timezone.utc).isoformat()}
 started=time.monotonic();env=os.environ.copy()
 gcc_support=[GCC/'lib64']+[GCC.parent/component/'lib' for component in ('isl','mpc','mpfr','gmp')]
 env['LD_LIBRARY_PATH']=':'.join(map(str,gcc_support))
 receipt['compiler_runtime_paths']=list(map(str,gcc_support))
 try:
  actual=subprocess.check_output(['git','-C',str(SOURCE),'rev-parse','HEAD'],text=True).strip()
  dirty=subprocess.check_output(['git','-C',str(SOURCE),'status','--porcelain=v1','--untracked-files=no'],text=True)
  if actual!=PIN or dirty:raise RuntimeError('SDK source is not the exact clean pin')
  receipt.update(actual_commit=actual,license_sha256=sha(SOURCE/'LICENSE.txt'),notice_sha256=sha(SOURCE/'NOTICE.txt'),script_sha256=sha(__file__))
  receipt['compiler_version']=subprocess.check_output([str(GCC/'bin/g++'),'--version'],text=True).splitlines()[0]
  receipt['cmake_version']=subprocess.check_output([str(CMAKE),'--version'],text=True).splitlines()[0]
  # The released Thrift version has moved from live Apache mirrors to archive.
  # Preserve upstream's exact tarball SHA; use a verified local URL instead of
  # suppressing TLS verification or patching the Arrow dependency source.
  versions=(SOURCE/'cpp/thirdparty/versions.txt').read_text()
  thrift_version=re.search(r'^ARROW_THRIFT_BUILD_VERSION=(.+)$',versions,re.M).group(1)
  thrift_sha=re.search(r'^ARROW_THRIFT_BUILD_SHA256_CHECKSUM=(.+)$',versions,re.M).group(1)
  downloads=SDK/'downloads';downloads.mkdir(exist_ok=True)
  tarball=downloads/('thrift-'+thrift_version+'.tar.gz')
  url='https://archive.apache.org/dist/thrift/'+thrift_version+'/'+tarball.name
  if not tarball.exists():
   temporary=tarball.with_suffix(tarball.suffix+'.partial')
   with urllib.request.urlopen(url,timeout=60) as response,temporary.open('wb') as output:
    for block in iter(lambda:response.read(1048576),b''):output.write(block)
   if sha(temporary)!=thrift_sha:raise RuntimeError('Official Thrift archive checksum differs from Arrow pin')
   temporary.rename(tarball)
  if sha(tarball)!=thrift_sha:raise RuntimeError('Cached Thrift archive checksum differs from Arrow pin')
  env['ARROW_THRIFT_URL']=tarball.resolve().as_uri()
  receipt['thrift_source']={'version':thrift_version,'origin_url':url,'configured_source_uri':env['ARROW_THRIFT_URL'],
   'path':str(tarball),'sha256':thrift_sha,'integrity_authority':'exact checksum in pinned Arrow20 source'}
  flags={'ARROW_BUILD_SHARED':'ON','ARROW_BUILD_STATIC':'ON' if args.with_static else 'OFF','ARROW_PARQUET':'ON','ARROW_COMPUTE':'OFF','ARROW_ACERO':'OFF',
   'ARROW_DATASET':'OFF','ARROW_FILESYSTEM':'OFF','ARROW_CSV':'OFF','ARROW_JSON':'OFF','ARROW_IPC':'OFF','ARROW_JEMALLOC':'OFF','ARROW_MIMALLOC':'OFF',
   'ARROW_BUILD_TESTS':'OFF','ARROW_BUILD_EXAMPLES':'OFF','ARROW_BUILD_UTILITIES':'OFF','ARROW_USE_CCACHE':'OFF',
   'ARROW_WITH_ZSTD':'ON','ARROW_WITH_SNAPPY':'ON','ARROW_WITH_ZLIB':'ON','ARROW_WITH_LZ4':'ON','ARROW_WITH_BROTLI':'OFF',
   'ARROW_WITH_RE2':'OFF','ARROW_WITH_UTF8PROC':'OFF','ARROW_DEPENDENCY_SOURCE':'BUNDLED','ARROW_SIMD_LEVEL':'NONE','ARROW_RUNTIME_SIMD_LEVEL':'NONE'}
  configure=[str(CMAKE),'-S',str(SOURCE/'cpp'),'-B',str(BUILD),'-G','Ninja','-DCMAKE_BUILD_TYPE=Release',
   '-DCMAKE_C_COMPILER='+str(GCC/'bin/gcc'),'-DCMAKE_CXX_COMPILER='+str(GCC/'bin/g++'),'-DCMAKE_CXX_STANDARD=17',
   '-DCMAKE_CXX_FLAGS_RELEASE=-O2 -DNDEBUG','-DCMAKE_INSTALL_PREFIX='+str(PREFIX),
   '-DCMAKE_INSTALL_LIBDIR=lib','-DCMAKE_INSTALL_RPATH=$ORIGIN','-DCMAKE_POSITION_INDEPENDENT_CODE=ON']
  configure+=['-D'+k+'='+v for k,v in flags.items()]
  for name,command in [('configure',configure),('build',[str(CMAKE),'--build',str(BUILD),'--parallel',str(args.jobs)]),('install',[str(CMAKE),'--install',str(BUILD)])]:
   receipt['commands'].append({'phase':name,'argv':command});(evidence/'build-progress.json').write_text(json.dumps(receipt,indent=2)+'\n')
   print(name,flush=True)
   with (evidence/(name+'.log')).open('w') as log:subprocess.run(command,env=env,stdout=log,stderr=subprocess.STDOUT,check=True)
  libraries=[p for p in (PREFIX/'lib').glob('lib*.so*') if p.is_file() and not p.is_symlink()]
  if not any(p.name.startswith('libarrow.so.') for p in libraries) or not any(p.name.startswith('libparquet.so.') for p in libraries):
   raise RuntimeError('SDK build produced no versioned Arrow/Parquet shared libraries')
  receipt.update(libraries=[{'path':str(p),'sha256':sha(p)} for p in libraries])
  if args.with_static:
   archives=[PREFIX/'lib'/name for name in ('libarrow.a','libparquet.a','libarrow_bundled_dependencies.a')]
   if not all(p.is_file() for p in archives):raise RuntimeError('PIC static Arrow/Parquet archives are incomplete')
   receipt['static_archives']=[{'path':str(p),'sha256':sha(p)} for p in archives]
  receipt['status']='dependency_built_unqualified'
  print('Real shared libraries built; native provider and file I/O qualification remain pending',flush=True)
  return 0
 except Exception as exc:
  receipt['error']=str(exc);print(str(exc),flush=True);return 1
 finally:
  receipt['elapsed_seconds']=time.monotonic()-started;(evidence/'build-result.json').write_text(json.dumps(receipt,indent=2)+'\n')

if __name__=='__main__':raise SystemExit(main())
