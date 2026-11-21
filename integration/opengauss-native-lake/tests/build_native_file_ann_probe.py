#!/usr/bin/env python3
"""Compile an unloaded diagnostic importing real native graph/search symbols."""
import argparse
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import sys

ROOT=Path('/home/qiumc/data/code/cpp');D=ROOT/'integration/opengauss-native-lake';SOURCE=ROOT/'.worktrees/opengauss-native-lake';PREFIX=ROOT/'.install/opengauss-native-lake-debug'
sys.path[:0]=[str(D),str(D/'tests')]
from file_dml_acceptance import check_build
from lake_harness import sha256,validate_run_id,write_json
from build_native_arrow_probe import builder_symbols
from native_probe_abi import feature_flags,check_layout

def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--run-id',required=True)
    p.add_argument('--build-receipt',type=Path,required=True);p.add_argument('--provider-qualification',type=Path,required=True)
    a=p.parse_args();validate_run_id(a.run_id);out=ROOT/'公共文档/openGauss原生湖存储/验证'/a.run_id;out.mkdir(exist_ok=False)
    r=dict(status='failed',scope=__doc__+' No business query activation or native graph-building qualification.')
    try:
        build=check_build(a.build_receipt);q=json.loads(a.provider_qualification.read_text());assert q['status']=='passed'
        provider_build=Path(q['build']);pb=json.loads(provider_build.read_text());provider=Path(pb['library']);assert sha256(provider)==q['provider_sha256']
        compiler=ROOT/'binarylibs/buildtools/gcc10.3/gcc/bin/g++';gcc=compiler.parent.parent
        env=dict(os.environ);env['GAUSSHOME']=str(PREFIX);env['LD_LIBRARY_PATH']=':'.join(map(str,[PREFIX/'lib',gcc/'lib64']+[gcc.parent/n/'lib'for n in ['isl','mpc','mpfr','gmp']]))
        def config(name):return subprocess.check_output([str(PREFIX/'bin/pg_config'),name],text=True,env=env).strip()
        source=Path(__file__).with_name('native_file_ann_probe.cpp');shutil.copy2(source,out/source.name);shutil.copy2(__file__,out/'executed-build.py')
        artifacts=out/'artifacts';artifacts.mkdir()
        (out/'ann_probe_config.h').write_text('#define OG_LAKE_TEST_ROOT '+json.dumps(str(artifacts))+'\n#define OG_LAKE_TEST_PROVIDER '+json.dumps(str(provider))+'\n')
        library=out/'native_file_ann_probe.so'
        features=feature_flags(SOURCE)
        command=[str(compiler),*shlex.split(config('--cppflags')),*shlex.split(config('--cflags')),*features,'-fPIC','-shared','-I'+config('--includedir-server'),'-I'+str(SOURCE/'src/include'),'-I'+str(SOURCE/'contrib/og_lake'),'-MD','-MF',str(out/'dependencies.d'),str(out/source.name),'-o',str(library)]
        with(out/'compile.log').open('w')as log:subprocess.run(command,env=env,stdout=log,stderr=subprocess.STDOUT,check=True,timeout=120)
        layout=check_layout(PREFIX/'bin/gaussdb',library,out)
        shutil.copy2(Path(__file__).with_name('native_probe_abi.py'),out/'executed-abi-gate.py')
        names=('LakeSearchAnnFile(', 'og_lake_ann_reader_open', 'og_lake_ann_writer_open', 'og_lake_ann_writer_append', 'og_lake_ann_writer_finish', 'LakeFileLimits(', 'LakeFileCheckStatus(')
        imports=builder_symbols(library,names);exports=builder_symbols(PREFIX/'bin/gaussdb',names)
        assert all(any(' U 'in(' '+line)and name in line for line in imports)for name in names)
        assert not any(' T 'in(' '+line)for line in imports)
        assert all(any(' T 'in(' '+line)and name in line for line in exports)for name in names)
        dependencies=(out/'dependencies.d').read_text().replace('\\\n',' ')
        headers={str(Path(n).resolve()):sha256(n)for n in shlex.split(dependencies.split(':',1)[1])if Path(n).suffix in ['.h','.hpp']}
        check_build(a.build_receipt)
        r.update(status='built_unloaded',source_head=build['source_head'],native_binary_sha256=build['binary_sha256'],library=str(library),library_sha256=sha256(library),
                 provider=str(provider),provider_sha256=sha256(provider),provider_qualification=str(a.provider_qualification.resolve()),provider_qualification_sha256=sha256(a.provider_qualification),
                 source_sha256=sha256(source),driver_sha256=sha256(__file__),headers=headers,command=command,artifacts=str(artifacts),config_sha256=sha256(out/'ann_probe_config.h'),actual_native_imports=imports,actual_native_exports=exports)
        r.update(native_feature_flags=features,abi_layout=layout,abi_gate_sha256=sha256(Path(__file__).with_name('native_probe_abi.py')))
    except BaseException as exc:r['error']=str(exc)
    write_json(out/'build.json',r);print(json.dumps(dict(status=r['status'],error=r.get('error'),receipt=str(out/'build.json')),ensure_ascii=False))
    return 0 if r['status']=='built_unloaded'else 1

if __name__=='__main__':raise SystemExit(main())
