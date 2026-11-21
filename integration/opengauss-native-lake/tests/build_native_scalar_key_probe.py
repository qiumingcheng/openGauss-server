#!/usr/bin/env python3
"""Compile and ABI-check an unloaded candidate scalar codec component only."""
import argparse
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import sys
ROOT=Path(__file__).resolve().parents[3];D=ROOT/'integration/opengauss-native-lake';SOURCE=ROOT/'.worktrees/opengauss-native-lake';PREFIX=ROOT/'.install/opengauss-native-lake-debug'
sys.path[:0]=[str(D),str(D/'tests')]
from file_dml_acceptance import check_build
from lake_harness import sha256,validate_run_id,write_json
from native_probe_abi import feature_flags,check_layout

def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--run-id',required=True);p.add_argument('--build-receipt',type=Path,required=True);p.add_argument('--candidate-header',type=Path,required=True)
    a=p.parse_args();validate_run_id(a.run_id);out=ROOT/'公共文档/openGauss原生湖存储/验证'/a.run_id;out.mkdir(exist_ok=False);r=dict(status='failed',scope=__doc__+' Does not qualify floating indexes, planner, SDK or storage integration.')
    try:
        build=check_build(a.build_receipt);header=a.candidate_header.resolve();source=Path(__file__).with_name('native_scalar_key_probe.cpp');shutil.copy2(header,out/'candidate_scalar_key.h');shutil.copy2(source,out/source.name);shutil.copy2(__file__,out/'executed-build.py');shutil.copy2(Path(__file__).with_name('native_probe_abi.py'),out/'executed-abi-gate.py')
        license=header.parent/'licenses/duckdb_radix_LICENSE';(out/'licenses').mkdir();shutil.copy2(license,out/'licenses/duckdb_radix_LICENSE')
        compiler=ROOT/'binarylibs/buildtools/gcc10.3/gcc/bin/g++';gcc=compiler.parent.parent;env=dict(os.environ);env['GAUSSHOME']=str(PREFIX);env['LD_LIBRARY_PATH']=':'.join(map(str,[PREFIX/'lib',gcc/'lib64']+[gcc.parent/n/'lib'for n in ['isl','mpc','mpfr','gmp']]))
        def config(name):return subprocess.check_output([str(PREFIX/'bin/pg_config'),name],text=True,env=env).strip()
        library=out/'native_scalar_key_probe.so';features=feature_flags(SOURCE)
        cmd=[str(compiler),*shlex.split(config('--cppflags')),*shlex.split(config('--cflags')),*features,'-fPIC','-shared','-I'+config('--includedir-server'),'-I'+str(SOURCE/'src/include'),'-MD','-MF',str(out/'dependencies.d'),str(out/source.name),'-o',str(library)]
        with(out/'compile.log').open('w')as log:subprocess.run(cmd,env=env,stdout=log,stderr=subprocess.STDOUT,check=True,timeout=120)
        exported=subprocess.check_output(['nm','-D','--defined-only',str(library)],text=True);(out/'exports.txt').write_text(exported)
        names={line.split()[-1]for line in exported.splitlines()if line.split()}
        required=['lake_test_float4_key','lake_test_float8_key','lake_test_float4_bits','lake_test_float8_bits']
        assert all(n in names and 'pg_finfo_'+n in names for n in required), 'Missing unmangled diagnostic exports'
        layout=check_layout(PREFIX/'bin/gaussdb',library,out);deps=(out/'dependencies.d').read_text().replace('\\\n',' ');headers={str(Path(n).resolve()):sha256(n)for n in shlex.split(deps.split(':',1)[1])if Path(n).suffix in ['.h','.hpp']}
        assert sha256(header)==sha256(out/'candidate_scalar_key.h');check_build(a.build_receipt)
        r.update(status='built_unloaded',source_head=build['source_head'],native_binary_sha256=build['binary_sha256'],library=str(library),library_sha256=sha256(library),candidate_header=str(header),candidate_header_sha256=sha256(header),source_sha256=sha256(source),driver_sha256=sha256(__file__),headers=headers,command=cmd,native_feature_flags=features,abi_layout=layout,abi_gate_sha256=sha256(Path(__file__).with_name('native_probe_abi.py')),license_sha256=sha256(license))
    except BaseException as exc:r['error']=str(exc)
    write_json(out/'build.json',r);print(json.dumps(dict(status=r['status'],error=r.get('error'),receipt=str(out/'build.json')),ensure_ascii=False));return 0 if r['status']=='built_unloaded'else 1
if __name__=='__main__':raise SystemExit(main())
