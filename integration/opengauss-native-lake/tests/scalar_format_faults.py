#!/usr/bin/env python3
"""Directed corruption/budget tests over preserved genuine component artifacts."""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import traceback

ROOT=Path('/home/qiumc/data/code/cpp')
def sha(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()

def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--run-id',required=True)
    p.add_argument('--positive-result',type=Path,required=True)
    p.add_argument('--source',type=Path,default=ROOT/'.worktrees/opengauss-native-lake-inline-index')
    p.add_argument('--provider',type=Path,default=ROOT/'.install/opengauss-native-lake-debug/lib/postgresql/og_lake_sdk/libog_lake_sdk_arrow.so')
    a=p.parse_args()
    if not a.run_id or any(c not in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.-' for c in a.run_id):p.error('simple new run-id required')
    out=ROOT/'公共文档/openGauss原生湖存储/验证'/a.run_id;out.mkdir(exist_ok=False)
    source=a.source.resolve()/'contrib/og_lake';probe=Path(__file__).with_name('scalar_format_probe.cpp')
    provider=a.provider.resolve()
    duckdb=ROOT/'.reference/opengauss-native-lake/build/duckdb-gcc10/duckdb'
    gcc=ROOT/'binarylibs/buildtools/gcc10.3';openssl=ROOT/'binarylibs/kernel/dependency/openssl/comm'
    files=[Path(__file__),probe,source/'lake_scalar_format.cpp',source/'lake_scalar_format.h',source/'sdk/lake_parquet_sdk.h',source/'vendor/arrow20/abi.h']
    r={'status':'failed','scope':'Portable scalar Parquet component faults only; no native/catalog/index-query/SDK-remote/RAC/production qualification',
       'assertions':[],'inputs':{str(x):sha(x)for x in files},'positive_result':str(a.positive_result.resolve()),'positive_result_sha256':sha(a.positive_result)}
    archive=out/'executed-inputs';archive.mkdir()
    for x in files:shutil.copy2(x,archive/x.name)
    env=dict(os.environ);env['LD_LIBRARY_PATH']=':'.join(map(str,[ROOT/'.install/opengauss-native-lake-debug/lib',ROOT/'.reference/opengauss-native-lake/sdk/install/arrow-20.0.0/lib']+[gcc/part for part in ['gcc/lib64','isl/lib','mpc/lib','mpfr/lib','gmp/lib']]))
    def check(name,observed,expected=True):
        ok=observed==expected;r['assertions'].append({'name':name,'observed':observed,'expected':expected,'status':'pass'if ok else'fail'})
        if not ok:raise AssertionError(name)
    def command(name,args):
        with(out/(name+'.log')).open('w')as log:subprocess.run(list(map(str,args)),env=env,stdout=log,stderr=subprocess.STDOUT,check=True,timeout=300)
        return (out/(name+'.log')).read_text()
    def literal(value):return "'"+str(value).replace("'","''")+"'"
    try:
        positive=json.loads(a.positive_result.read_text());check('preserved roundtrip and independent SQL passed',positive['status'],'passed')
        for path in [source/'lake_scalar_format.cpp',source/'lake_scalar_format.h',source/'sdk/lake_parquet_sdk.h',source/'vendor/arrow20/abi.h']:
            check('same component bytes as positive '+path.name,sha(path),positive['inputs'][str(path)])
        check('same genuine provider',sha(provider),positive['provider_sha256']);check('same independent engine',sha(duckdb),positive['reference_binary_sha256'])
        artifact=positive['probe']['artifacts'][0];data=Path(artifact['data']);lookup=Path(artifact['lookup'])
        for path in [data,lookup,Path(artifact['input'])]:check('preserved original artifact '+path.name,sha(path),positive['artifacts'][str(path)])
        context='b'*64;manifest=artifact['manifest'];exe=out/'scalar-format-fault-probe'
        command('compile',[gcc/'gcc/bin/g++','-std=c++14','-O0','-g','-D_GLIBCXX_USE_CXX11_ABI=0','-Wall','-Wextra','-Werror','-Wno-deprecated-declarations','-Wno-misleading-indentation','-I',source,'-I',openssl/'include',probe,source/'lake_scalar_format.cpp','-L',openssl/'lib','-lcrypto','-ldl','-o',exe]);r['consumer_binary_sha256']=sha(exe)
        def validate(name,data_path=data,lookup_path=lookup,budget=None):
            args=[exe,provider,out,'validate',context,manifest,3,data_path,lookup_path]
            if budget is not None:args.append(budget)
            value=json.loads(command(name,args));r.setdefault('cases',[]).append({'name':name,**value})
            check(name+' exposes no partial failed query result',value['partial_result'],False);return value
        check('fresh process can reopen preserved good artifact',validate('valid')['code'],0)
        for name,content,is_lookup in [('lookup_truncated',lookup.read_bytes()[:20],True),('data_truncated',data.read_bytes()[:-16],False),('data_magic',b'FAIL'+data.read_bytes()[4:],False)]:
            path=out/(name+'.parquet');path.write_bytes(content);value=validate(name,lookup_path=path)if is_lookup else validate(name,data_path=path)
            check(name+' rejects broken storage',value['code']!=0)
        check('data and lookup schemas cannot be swapped',validate('swapped_data',data_path=lookup)['code']!=0)
        check('lookup and data schemas cannot be swapped',validate('swapped_lookup',lookup_path=data)['code']!=0)
        for name,path,is_lookup in [('lookup_page',lookup,True),('data_page',data,False)]:
            process=subprocess.run([str(duckdb),'-no-init','-batch','-json','-c',
                'SELECT data_page_offset FROM parquet_metadata('+literal(path)+') WHERE row_group_id=0 AND column_id=0'],
                text=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE,check=True,timeout=30)
            offset=int(json.loads(process.stdout)[0]['data_page_offset'])+8
            content=bytearray(path.read_bytes());check(name+' selected actual page bytes',0<offset<len(content)-8)
            content[offset]^=0x5a;corrupt=out/(name+'.parquet');corrupt.write_bytes(content)
            value=validate(name,lookup_path=corrupt)if is_lookup else validate(name,data_path=corrupt)
            check(name+' rejects corrupted real Parquet page',value['code']!=0)
        rewrite=out/'valid-parquet-altered-postings';rewrite.mkdir()
        altered=json.loads(command('rewrite',[exe,provider,rewrite,'rewrite',artifact['input'],3]))
        value=validate('valid_parquet_wrong_postings',data_path=Path(altered['data']))
        check('valid Parquet with ordered altered postings fails content digest',value['code']!=0 and 'leaf checksum'in value['message'])
        limited=validate('posting_budget',budget=1024*1024)
        check('streaming verification succeeds within the small page budget',limited['verify_code'],0)
        check('bounded posting collection returns memory status',limited['code'],4)
        for path,h in r['inputs'].items():check('frozen fault input '+Path(path).name,sha(path),h)
        check('original positive receipt remains unchanged',sha(a.positive_result),r['positive_result_sha256'])
        r.update(status='passed',artifacts={str(x):sha(x)for x in out.rglob('*.parquet')})
    except BaseException as exc:r.update(reason=str(exc),traceback=traceback.format_exc())
    r['finished_at']=datetime.datetime.now(datetime.timezone.utc).isoformat();(out/'result.json').write_text(json.dumps(r,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps({'status':r['status'],'checks':len(r['assertions']),'reason':r.get('reason'),'result':str(out/'result.json')},ensure_ascii=False))
    return 0 if r['status']=='passed'else 1

if __name__=='__main__':raise SystemExit(main())
