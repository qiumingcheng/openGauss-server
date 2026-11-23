#!/usr/bin/env python3
"""Build the locked CRoaring dependency in an isolated, unlinked Debug prefix.

No native gaussdb/SDK installation is changed. This receipt proves dependency
build identity only, not a persistent bitmap index or product integration.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

ROOT=Path(__file__).resolve().parents[3];D=ROOT/'integration/opengauss-native-lake'
sys.path.insert(0,str(D))
from lake_harness import sha256,validate_run_id,write_json


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--run-id',required=True);a=p.parse_args();validate_run_id(a.run_id)
    out=ROOT/'公共文档/openGauss原生湖存储/验证'/a.run_id;out.mkdir(exist_ok=False)
    build=ROOT/'.build/opengauss-native-lake/roaring-component'/a.run_id
    prefix=ROOT/'.install/opengauss-native-lake-roaring-component'/a.run_id
    build.mkdir(parents=True,exist_ok=False);prefix.mkdir(parents=True,exist_ok=False)
    r=dict(status='failed',scope=__doc__,build=str(build),prefix=str(prefix),commands=[])
    try:
        lockpath=D/'source-lock.json';lockhash=sha256(lockpath);spec=json.loads(lockpath.read_text())['repositories']['croaring'];source=ROOT/spec['path']
        git=lambda *args:subprocess.check_output(['git','-C',str(source),*args],text=True).strip()
        assert git('rev-parse','HEAD')==spec['commit'];assert not git('status','--porcelain')
        assert sha256(source/spec['license']['file'])==spec['license']['sha256']
        compiler=ROOT/'binarylibs/buildtools/gcc10.3/gcc/bin/gcc';cxx=compiler.with_name('g++');gcc=compiler.parent.parent
        env=dict(os.environ);env['LD_LIBRARY_PATH']=':'.join(map(str,[gcc/'lib64']+[gcc.parent/n/'lib'for n in ['isl','mpc','mpfr','gmp']]))
        def run(cmd,name):
            r['commands'].append(list(map(str,cmd)))
            with(out/name).open('x')as log:subprocess.run(list(map(str,cmd)),env=env,stdout=log,stderr=subprocess.STDOUT,check=True,timeout=300)
        shutil.copy2(__file__,out/'executed-build.py');shutil.copy2(lockpath,out/'source-lock.json')
        run(['cmake','-S',source,'-B',build,'-DCMAKE_C_COMPILER='+str(compiler),'-DCMAKE_CXX_COMPILER='+str(cxx),
             '-DCMAKE_INSTALL_PREFIX='+str(prefix),'-DCMAKE_BUILD_TYPE=Debug','-DCMAKE_C_FLAGS_DEBUG=-O0 -g',
             '-DCMAKE_POSITION_INDEPENDENT_CODE=ON','-DCMAKE_C_VISIBILITY_PRESET=hidden','-DBUILD_SHARED_LIBS=OFF',
             '-DROARING_BUILD_STATIC=ON','-DROARING_BUILD_LTO=OFF','-DENABLE_ROARING_TESTS=OFF','-DROARING_USE_CPM=OFF',
             '-DROARING_DISABLE_X64=ON','-DROARING_DISABLE_NEON=ON'],'configure.log')
        run(['cmake','--build',build,'--target','roaring','--parallel','1'],'build.log')
        run(['cmake','--install',build],'install.log')
        (prefix/'licenses').mkdir();shutil.copy2(source/spec['license']['file'],prefix/'licenses/CRoaring-LICENSE')
        libraries=list(prefix.glob('lib*/libroaring.a'));assert len(libraries)==1
        assert sha256(lockpath)==lockhash and git('rev-parse','HEAD')==spec['commit'] and not git('status','--porcelain')
        files={str(f.relative_to(prefix)):sha256(f)for f in prefix.rglob('*')if f.is_file()}
        write_json(prefix/'owner.json',dict(run_id=a.run_id,prefix=str(prefix),files=files,scope='isolated unlinked dependency candidate'))
        r.update(status='built_unlinked',source=str(source),source_commit=spec['commit'],source_lock_sha256=lockhash,
                 source_license=spec['license'],compiler=str(compiler),compiler_sha256=sha256(compiler),library=str(libraries[0]),
                 library_sha256=sha256(libraries[0]),files=files,owner_sha256=sha256(prefix/'owner.json'),driver_sha256=sha256(__file__))
    except BaseException as exc:r['error']=str(exc)
    write_json(out/'build.json',r);print(json.dumps(dict(status=r['status'],error=r.get('error'),receipt=str(out/'build.json')),ensure_ascii=False))
    return 0 if r['status']=='built_unlinked'else 1

if __name__=='__main__':raise SystemExit(main())
