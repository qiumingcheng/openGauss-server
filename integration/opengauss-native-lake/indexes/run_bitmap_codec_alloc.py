#!/usr/bin/env python3
"""Inject every observed real decoder allocation in separate standalone processes.

No server/library installation is altered. CRoaring itself is an uninstrumented
Debug static dependency; probe and codec use ASan/UBSan. Hook tracking checks
all actual C allocations and wrapper operator-new allocation independently.
"""
import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
ROOT=Path(__file__).resolve().parents[3];D=ROOT/'integration/opengauss-native-lake';sys.path.insert(0,str(D))
from lake_harness import sha256,validate_run_id,write_json


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--run-id',required=True);p.add_argument('--dependency-build',type=Path,required=True);a=p.parse_args();validate_run_id(a.run_id)
    out=ROOT/'公共文档/openGauss原生湖存储/验证'/a.run_id;out.mkdir(exist_ok=False)
    result=dict(status='fail',scope=__doc__,cases=[],limitations=['No native/Parquet/full-query allocator qualification','No production global/threaded memory-hook installation','calloc/realloc hooks installed but not reached by these decode fixtures'])
    try:
        dep=json.loads(a.dependency_build.read_text());assert dep['status']=='built_unlinked';prefix=Path(dep['prefix']);assert sha256(prefix/'owner.json')==dep['owner_sha256']
        assert dep.get('source_patch_sha256')=='291aca7744394c6658dd2316972f6cc465f04461c0dec7c4a5c5885ee4e2441f', 'Exact qualified run-preallocation patch required'
        for n,h in dep['files'].items():assert sha256(prefix/n)==h
        src=ROOT/'.worktrees/opengauss-native-lake-inline-index/contrib/og_lake';probe=D/'tests/bitmap_codec_alloc_probe.cpp'
        inputs=[Path(__file__),probe,src/'lake_bitmap_codec.cpp',src/'lake_bitmap_codec.h',src/'sdk/lake_parquet_sdk.h',D/'source-lock.json']
        result['inputs']={str(f):sha256(f)for f in inputs};archive=out/'executed-inputs';archive.mkdir()
        for f in inputs:shutil.copy2(f,archive/f.name)
        gcc=ROOT/'binarylibs/buildtools/gcc10.3/gcc';env=dict(os.environ);env['LD_LIBRARY_PATH']=':'.join(map(str,[gcc/'lib64']+[gcc.parent/n/'lib'for n in ['isl','mpc','mpfr','gmp']]))
        env['ASAN_OPTIONS']='detect_leaks=1:abort_on_error=1:alloc_dealloc_mismatch=1';env['UBSAN_OPTIONS']='halt_on_error=1:print_stacktrace=1'
        compiler=gcc/'bin/g++';binary=out/'probe';cmd=[str(compiler),'-std=c++14','-O0','-g','-fsanitize=address,undefined','-fno-omit-frame-pointer','-I'+str(src),'-I'+str(prefix/'include'),str(src/'lake_bitmap_codec.cpp'),str(probe),dep['library'],'-o',str(binary)]
        write_json(out/'compile-command.json',cmd)
        with(out/'compile.log').open('x')as log:subprocess.run(cmd,env=env,stdout=log,stderr=subprocess.STDOUT,check=True,timeout=120)
        def run(fixture,fail_at=0,cpp=False):
            stem=fixture+'-c'+str(fail_at)+'-new'+str(int(cpp));args=[str(binary),fixture,str(fail_at),str(int(cpp))]
            proc=subprocess.run(args,env=env,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,timeout=60)
            (out/(stem+'.stdout')).write_text(proc.stdout);(out/(stem+'.stderr')).write_text(proc.stderr)
            case=dict(fixture=fixture,failure_index=fail_at,cpp_failure=cpp,returncode=proc.returncode,stdout_sha256=sha256(out/(stem+'.stdout')),stderr_sha256=sha256(out/(stem+'.stderr')))
            if proc.returncode==0:case.update(observed=json.loads(proc.stdout),status='pass')
            else:case.update(status='fail',stderr=proc.stderr[-2000:])
            result['cases'].append(case);write_json(out/'progress.json',result)
            return case
        fixtures=['empty','array','multi_array','bitset','run','many_runs','mixed','bad_array','bad_run']
        for fixture in fixtures:
            baseline=run(fixture)
            if baseline['status']!='pass':continue
            for fail_at in range(1,baseline['observed']['allocation_calls']+1):run(fixture,fail_at)
            if not fixture.startswith('bad_'):run(fixture,cpp=True)
        assert all(sha256(f)==h for f,h in result['inputs'].items())
        assert sha256(dep['library'])==dep['library_sha256']
        result.update(status='pass'if all(c['status']=='pass'for c in result['cases'])else'fail',dependency_build=str(a.dependency_build.resolve()),dependency_sha256=sha256(a.dependency_build),library_sha256=dep['library_sha256'],compiler_sha256=sha256(compiler),binary_sha256=sha256(binary),case_count=len(result['cases']),passed=sum(c['status']=='pass'for c in result['cases']))
    except BaseException as exc:result['error']=str(exc)
    write_json(out/'result.json',result);print(json.dumps(dict(status=result['status'],cases=len(result['cases']),error=result.get('error'),failed=[dict(fixture=c['fixture'],at=c['failure_index'],cpp=c['cpp_failure'])for c in result['cases']if c['status']!='pass'],result=str(out/'result.json')),ensure_ascii=False))
    return 0 if result['status']=='pass'else 1

if __name__=='__main__':raise SystemExit(main())
