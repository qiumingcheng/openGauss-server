#!/usr/bin/env python3
"""Compile the isolated C ABI provider against the exact Arrow20 dependency."""
import argparse, datetime, hashlib, json, os, shutil, subprocess, time
from pathlib import Path

ROOT=Path(__file__).resolve().parents[3]
SDK=ROOT/'.reference/opengauss-native-lake/sdk'
SOURCE=ROOT/'.worktrees/opengauss-native-lake/contrib/og_lake/sdk'
BUILD=SDK/'build/og-lake-provider'
PREFIX=SDK/'install/arrow-20.0.0'
GCC=ROOT/'binarylibs/buildtools/gcc10.3/gcc'
CMAKE=SDK/'build-tools/bin/cmake'

def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()

def inputs():
    return {p.name:sha(p) for p in sorted(SOURCE.iterdir())
            if p.is_file() and (p.suffix in ('.h','.cpp','.map') or p.name=='CMakeLists.txt')}

def dependencies():
    headers={str(p.relative_to(PREFIX)):sha(p) for p in sorted((PREFIX/'include').rglob('*'))
             if p.is_file() and 'og_lake' not in p.parts}
    return {'shared_libraries':{name:sha(PREFIX/'lib'/name) for name in ('libarrow.so','libparquet.so')},
            'static_archives':{name:sha(PREFIX/'lib'/name) for name in ('libarrow.a','libparquet.a','libarrow_bundled_dependencies.a')},
            'installed_header_count':len(headers),
            'installed_headers_sha256':hashlib.sha256(json.dumps(headers,sort_keys=True).encode()).hexdigest(),
            'compiler_sha256':sha(GCC/'bin/g++')}

def main():
    global SOURCE, BUILD
    parser=argparse.ArgumentParser()
    parser.add_argument('--evidence',type=Path,required=True)
    parser.add_argument("--provider-source",type=Path,default=SOURCE)
    parser.add_argument("--build-directory",type=Path,default=BUILD)
    parser.add_argument("--provider-prefix",type=Path,default=PREFIX)
    args=parser.parse_args()
    SOURCE=args.provider_source.resolve();BUILD=args.build_directory.resolve()
    provider_prefix=args.provider_prefix.resolve()
    evidence=args.evidence.resolve();evidence.mkdir(parents=True,exist_ok=False)
    receipt={'status':'failed','scope':'standalone real Parquet provider; not native file path qualification',
             'provider_source':str(SOURCE),'provider_prefix':str(provider_prefix),
             'started_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),
             'sources_before':inputs(),'dependencies_before':dependencies(),
             'script_sha256':sha(__file__),'commands':[]}
    archive=evidence/'executed-inputs';archive.mkdir(exist_ok=False)
    for name in receipt['sources_before']:shutil.copy2(SOURCE/name,archive/name)
    shutil.copy2(__file__,evidence/'executed-build-driver.py')
    started=time.monotonic()
    env=os.environ.copy()
    support=[GCC/'lib64']+[GCC.parent/p/'lib' for p in ('isl','mpc','mpfr','gmp')]
    env['LD_LIBRARY_PATH']=':'.join(map(str,[PREFIX/'lib']+support))
    try:
        commands=[('configure',[str(CMAKE),'-S',str(SOURCE),'-B',str(BUILD),'-G','Ninja',
            '-DCMAKE_BUILD_TYPE=Release','-DCMAKE_C_COMPILER='+str(GCC/'bin/gcc'),
            '-DCMAKE_CXX_COMPILER='+str(GCC/'bin/g++'),'-DCMAKE_CXX_FLAGS_RELEASE=-O2 -DNDEBUG',
            '-DCMAKE_PREFIX_PATH='+str(PREFIX),'-DCMAKE_INSTALL_PREFIX='+str(provider_prefix)]),
            ('build',[str(CMAKE),'--build',str(BUILD),'--parallel','2']),
            ('install',[str(CMAKE),'--install',str(BUILD)])]
        for name,command in commands:
            print(name,flush=True);receipt['commands'].append(command)
            with (evidence/(name+'.log')).open('w') as log:
                subprocess.run(command,env=env,stdout=log,stderr=subprocess.STDOUT,check=True)
        notices=provider_prefix/'share/licenses/apache-arrow-20.0.0';notices.mkdir(parents=True,exist_ok=True)
        for name in ('LICENSE.txt','NOTICE.txt'):
            original=SDK/'arrow-20.0.0'/name;shutil.copy2(original,notices/name)
        receipt['dependency_notices']={name:sha(notices/name) for name in ('LICENSE.txt','NOTICE.txt')}
        receipt['sources_after']=inputs()
        receipt['dependencies_after']=dependencies()
        if receipt['sources_before']!=receipt['sources_after'] or receipt['dependencies_before']!=receipt['dependencies_after']:
            raise RuntimeError('provider sources changed during build')
        library=provider_prefix/'lib/libog_lake_sdk_arrow.so'
        receipt.update(status='provider_built_unqualified',library=str(library),sha256=sha(library))
        print(receipt['status'],flush=True)
        return 0
    except Exception as exc:
        receipt['error']=str(exc);print(str(exc),flush=True);return 1
    finally:
        receipt['elapsed_seconds']=time.monotonic()-started
        (evidence/'build-result.json').write_text(json.dumps(receipt,indent=2)+'\n')

if __name__=='__main__':raise SystemExit(main())
