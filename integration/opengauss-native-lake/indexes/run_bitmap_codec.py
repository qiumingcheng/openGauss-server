#!/usr/bin/env python3
"""Execute the isolated experimental bitmap decoder, without native linking.

The C++ wrapper and probe use ASan/UBSan. The locked CRoaring Debug dependency
is not sanitizer-instrumented. No native/persistent-index capability is inferred.
"""
import argparse
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import sys
ROOT=Path(__file__).resolve().parents[3];D=ROOT/'integration/opengauss-native-lake'
sys.path.insert(0,str(D))
from lake_harness import sha256,validate_run_id,write_json


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--run-id',required=True)
    p.add_argument('--dependency-build',type=Path,required=True);a=p.parse_args();validate_run_id(a.run_id)
    out=ROOT/'公共文档/openGauss原生湖存储/验证'/a.run_id;out.mkdir(exist_ok=False)
    r=dict(status='fail',scope=__doc__,limitations=['No native/Parquet/dictionary/publication/query qualification',
      'No injected allocation failures or whole-query allocator accounting','No source authentication in portable bytes; verified outer source/key manifest required'])
    try:
        dep=json.loads(a.dependency_build.read_text());assert dep['status']=='built_unlinked';prefix=Path(dep['prefix'])
        assert dep.get('source_patch_sha256')=='291aca7744394c6658dd2316972f6cc465f04461c0dec7c4a5c5885ee4e2441f','Requires qualified run-preallocation dependency variant'
        assert sha256(prefix/'owner.json')==dep['owner_sha256']
        for name,h in dep['files'].items():assert sha256(prefix/name)==h
        source=ROOT/'.worktrees/opengauss-native-lake-inline-index/contrib/og_lake';probe=D/'tests/bitmap_codec_probe.cpp'
        files=[Path(__file__),source/'lake_bitmap_codec.cpp',source/'lake_bitmap_codec.h',source/'sdk/lake_parquet_sdk.h',probe,D/'source-lock.json']
        r['inputs']={str(f):sha256(f)for f in files};archive=out/'executed-inputs';archive.mkdir()
        for f in files:shutil.copy2(f,archive/f.name)
        gcc=ROOT/'binarylibs/buildtools/gcc10.3/gcc';env=os.environ.copy();env['LD_LIBRARY_PATH']=':'.join(map(str,[gcc/'lib64']+[gcc.parent/n/'lib'for n in ['isl','mpc','mpfr','gmp']]))
        compiler=gcc/'bin/g++';binary=out/'probe'
        cmd=[str(compiler),'-std=c++14','-O0','-g','-fsanitize=address,undefined','-fno-omit-frame-pointer',
             '-I'+str(source),'-I'+str(prefix/'include'),'-MD','-MF',str(out/'dependencies.d'),
             str(source/'lake_bitmap_codec.cpp'),str(probe),dep['library'],'-o',str(binary)]
        write_json(out/'command.json',cmd)
        with(out/'compile.log').open('x')as log:subprocess.run(cmd,env=env,stdout=log,stderr=subprocess.STDOUT,check=True,timeout=120)
        with(out/'probe.log').open('x')as log:subprocess.run([str(binary)],env=env,stdout=log,stderr=subprocess.STDOUT,check=True,timeout=120)
        observed=json.loads((out/'probe.log').read_text());assert observed['status']=='pass'and observed['checks']>=3165994
        headers={}
        for line in (out/'dependencies.d').read_text().replace('\\\n',' ').splitlines():
            for name in shlex.split(line.split(':',1)[1]):
                q=Path(name)
                if q.is_file():headers[str(q.resolve())]=sha256(q)
        assert all(sha256(f)==h for f,h in r['inputs'].items())
        assert sha256(dep['library'])==dep['library_sha256']
        r.update(status='pass',dependency_build=str(a.dependency_build.resolve()),dependency_sha256=sha256(a.dependency_build),
                 library_sha256=dep['library_sha256'],compiler_sha256=sha256(compiler),binary_sha256=sha256(binary),headers=headers,
                 probe_checks=observed['checks'],cases=['five valid bitmap domains with full independent membership',
                 '20000 random byte buffers','duplicate array positions','overlapping runs','each truncated array prefix',
                 'cardinality/domain/trailing-byte/budget rejection','idempotent release'],
                 logs={name:sha256(out/name)for name in ['compile.log','probe.log']})
    except BaseException as exc:r['error']=str(exc)
    write_json(out/'result.json',r);print(json.dumps(dict(status=r['status'],error=r.get('error'),result=str(out/'result.json')),ensure_ascii=False))
    return 0 if r['status']=='pass'else 1

if __name__=='__main__':raise SystemExit(main())
