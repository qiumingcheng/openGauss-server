#!/usr/bin/env python3
"""Build an isolated locked-CRoaring portable-run OOM fix; preserve base source/library.

The variant changes only run-container preallocation in portable deserialization.
It is not installed/loaded into gaussdb or SDK and is not a global hook design.
"""
import argparse
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
ROOT=Path(__file__).resolve().parents[3];D=ROOT/'integration/opengauss-native-lake';sys.path.insert(0,str(D))
from lake_harness import sha256,validate_run_id,write_json

def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--run-id',required=True);p.add_argument('--base-build',type=Path,required=True);a=p.parse_args();validate_run_id(a.run_id)
    out=ROOT/'公共文档/openGauss原生湖存储/验证'/a.run_id;out.mkdir(exist_ok=False)
    root=ROOT/'.build/opengauss-native-lake/roaring-oom'/a.run_id;root.mkdir(parents=True,exist_ok=False);source=root/'source';source.mkdir();build=root/'build'
    prefix=ROOT/'.install/opengauss-native-lake-roaring-oom-component'/a.run_id;prefix.mkdir(parents=True,exist_ok=False)
    r=dict(status='failed',scope=__doc__,prefix=str(prefix),build=str(build),source=str(source),commands=[])
    try:
        base=json.loads(a.base_build.read_text());assert base['status']=='built_unlinked';original=Path(base['source']);original_prefix=Path(base['prefix'])
        for name,h in base['files'].items():assert sha256(original_prefix/name)==h
        assert sha256(original_prefix/'owner.json')==base['owner_sha256']
        git=lambda *args:subprocess.check_output(['git','-C',str(original),*args],text=True).strip()
        assert git('rev-parse','HEAD')==base['source_commit']and not git('status','--porcelain')
        content=subprocess.check_output(['git','-C',str(original),'archive',base['source_commit']])
        with tarfile.open(fileobj=io.BytesIO(content),mode='r:')as archive:archive.extractall(source)
        patch=D/'indexes/patches/croaring_portable_run_allocation.patch';shutil.copy2(patch,out/patch.name);shutil.copy2(__file__,out/'executed-build.py');shutil.copy2(a.base_build,out/'base-build.json')
        with(out/'apply.log').open('x')as log:subprocess.run(['git','apply','--directory',str(source.relative_to(ROOT)),str(patch)],cwd=ROOT,stdout=log,stderr=subprocess.STDOUT,check=True,timeout=30)
        pristine=subprocess.check_output(['git','-C',str(original),'show',base['source_commit']+':src/roaring_array.c'])
        assert (source/'src/roaring_array.c').read_bytes()!=pristine
        gcc=ROOT/'binarylibs/buildtools/gcc10.3/gcc';env=dict(os.environ);env['LD_LIBRARY_PATH']=':'.join(map(str,[gcc/'lib64']+[gcc.parent/n/'lib'for n in ['isl','mpc','mpfr','gmp']]))
        def run(cmd,name):
            r['commands'].append(list(map(str,cmd)))
            with(out/name).open('x')as log:subprocess.run(list(map(str,cmd)),env=env,stdout=log,stderr=subprocess.STDOUT,check=True,timeout=300)
        cfg=base['commands'][0][:]
        cfg[cfg.index('-S')+1]=str(source);cfg[cfg.index('-B')+1]=str(build)
        cfg=[('-DCMAKE_INSTALL_PREFIX='+str(prefix))if c.startswith('-DCMAKE_INSTALL_PREFIX=')else c for c in cfg]
        run(cfg,'configure.log');run(['cmake','--build',build,'--target','roaring','--parallel','1'],'build.log');run(['cmake','--install',build],'install.log')
        (prefix/'licenses').mkdir();shutil.copy2(source/base['source_license']['file'],prefix/'licenses/CRoaring-LICENSE')
        libraries=list(prefix.glob('lib*/libroaring.a'));assert len(libraries)==1
        assert git('rev-parse','HEAD')==base['source_commit']and not git('status','--porcelain')
        for name,h in base['files'].items():assert sha256(original_prefix/name)==h
        files={str(f.relative_to(prefix)):sha256(f)for f in prefix.rglob('*')if f.is_file()}
        write_json(prefix/'owner.json',dict(run_id=a.run_id,prefix=str(prefix),files=files,scope='isolated unlinked patched CRoaring candidate'))
        r.update(status='built_unlinked',source_commit=base['source_commit'],source_patch=str(patch),source_patch_sha256=sha256(patch),
            base_build=str(a.base_build.resolve()),base_build_sha256=sha256(a.base_build),base_library_sha256=base['library_sha256'],
            patched_source_sha256=sha256(source/'src/roaring_array.c'),source_license=base['source_license'],library=str(libraries[0]),library_sha256=sha256(libraries[0]),files=files,owner_sha256=sha256(prefix/'owner.json'),driver_sha256=sha256(__file__))
    except BaseException as exc:r['error']=str(exc)
    write_json(out/'build.json',r);print(json.dumps(dict(status=r['status'],error=r.get('error'),receipt=str(out/'build.json')),ensure_ascii=False))
    return 0 if r['status']=='built_unlinked'else 1
if __name__=='__main__':raise SystemExit(main())
