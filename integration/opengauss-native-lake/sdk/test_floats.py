#!/usr/bin/env python3
"""Isolated float codec bit roundtrip, independent SQL and old-provider rejection.

No installation, native lake SQL types, nested vector storage or ANN support is
qualified by this component test.
"""
import argparse
import datetime
import hashlib
import json
import math
import os
from pathlib import Path
import shutil
import struct
import subprocess
import traceback

ROOT=Path('/home/qiumc/data/code/cpp');D=ROOT/'integration/opengauss-native-lake'
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()

def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--run-id',required=True)
    p.add_argument('--provider-build',type=Path,required=True)
    p.add_argument('--old-provider',type=Path,required=True)
    p.add_argument('--source',type=Path,default=ROOT/'.worktrees/opengauss-native-lake-inline-index')
    a=p.parse_args()
    if not a.run_id or any(c not in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.-'for c in a.run_id):p.error('simple new run-id required')
    out=ROOT/'公共文档/openGauss原生湖存储/验证'/a.run_id;out.mkdir(exist_ok=False)
    source=a.source.resolve()/'contrib/og_lake';probe=Path(__file__).with_name('provider_float_test.c')
    provider_build=json.loads(a.provider_build.read_text());provider=Path(provider_build['library'])
    old=a.old_provider.resolve()
    installed=ROOT/'.install/opengauss-native-lake-debug/lib/postgresql/og_lake_sdk/libog_lake_sdk_arrow.so'
    duckdb=ROOT/'.reference/opengauss-native-lake/build/duckdb-gcc10/duckdb';gcc=ROOT/'binarylibs/buildtools/gcc10.3'
    files=[Path(__file__),probe,source/'sdk/lake_parquet_sdk.h',source/'sdk/arrow_parquet_provider.cpp',source/'vendor/arrow20/abi.h']
    legacy_header=ROOT/'.worktrees/opengauss-native-lake/contrib/og_lake/sdk/lake_parquet_sdk.h'
    reader_probe=D/'sdk/provider_reader_test.c';files.extend([legacy_header,reader_probe])
    r={'status':'failed','scope':__doc__,'assertions':[],'inputs':{str(f):sha(f)for f in files},
       'provider_build':str(a.provider_build.resolve()),'provider_build_sha256':sha(a.provider_build),
       'provider_sha256':sha(provider),'old_provider_sha256':sha(old),'reference_binary_sha256':sha(duckdb),'installed_provider_sha256':sha(installed)}
    archive=out/'executed-inputs';archive.mkdir()
    r['archive_index']={}
    for number,f in enumerate(files):
        name=str(number)+'-'+f.name;shutil.copy2(f,archive/name);r['archive_index'][str(f)]=name
    env=dict(os.environ);env['LD_LIBRARY_PATH']=':'.join(map(str,[ROOT/'.install/opengauss-native-lake-debug/lib']+[gcc/part for part in ['gcc/lib64','isl/lib','mpc/lib','mpfr/lib','gmp/lib']]))
    def check(name,got,want=True):
        good=got==want;r['assertions'].append(dict(name=name,observed=got,expected=want,status='pass'if good else'fail'))
        if not good:raise AssertionError(name)
    def command(label,args):
        with(out/(label+'.log')).open('w')as log:subprocess.run(list(map(str,args)),env=env,stdout=log,stderr=subprocess.STDOUT,check=True,timeout=300)
        return (out/(label+'.log')).read_text()
    def literal(s):return "'"+str(s).replace("'","''")+"'"
    try:
        check('actual isolated provider build completed',provider_build['status'],'provider_built_unqualified')
        check('exact built library remains intact',sha(provider),provider_build['sha256'])
        for name in ['lake_parquet_sdk.h','arrow_parquet_provider.cpp']:
            check('same tested provider source '+name,sha(source/'sdk'/name),provider_build['sources_before'][name])
        exe=out/'float-consumer'
        command('compile',[gcc/'gcc/bin/gcc','-std=c11','-O0','-g','-Wall','-Wextra','-Werror','-I',source,
            probe,'-ldl','-o',exe]);r['consumer_sha256']=sha(exe)
        data=out/'artifacts';data.mkdir();value=json.loads(command('roundtrip',[exe,provider,data]));r['consumer']=value
        check('actual C consumer passes',value['status'],'pass');check('three independent compression artifacts',value['files'],3)
        old_data=out/'old-provider';old_data.mkdir();legacy=json.loads(command('old-provider',[exe,old,old_data,'unsupported']))
        check('new consumer safely rejects old capability',legacy['status'],'pass')
        check('old provider creates no unsupported artifact',list(old_data.iterdir()),[])
        sql="""SET threads=1; CREATE TABLE input AS SELECT i::INTEGER AS id,
 CASE WHEN i%5=0 THEN NULL ELSE i%2=0 END AS flag,
 CASE WHEN i=0 THEN '-9223372036854775808'::BIGINT WHEN i=1 THEN '9223372036854775807'::BIGINT
      WHEN i%7=0 THEN NULL ELSE i*10000000000 END AS big,
 CASE WHEN i%13=0 THEN NULL WHEN i%11=0 THEN '' ELSE '湖/'||i::VARCHAR||'/'||repeat('x',1024) END AS txt
 FROM range(12288) q(i);
"""
        typed=out/'typed.parquet';no_ids=out/'no_ids.parquet'
        sql+='COPY input TO '+literal(typed)+' (FORMAT PARQUET,COMPRESSION ZSTD,ROW_GROUP_SIZE 2048,FIELD_IDS {id:11,flag:21,big:31,txt:41});'
        sql+='COPY input TO '+literal(no_ids)+' (FORMAT PARQUET,COMPRESSION SNAPPY,ROW_GROUP_SIZE 2048);'
        command('legacy-fixture',[duckdb,'-no-init','-batch','-c',sql])
        corrupt=out/'bad_magic.parquet';truncated=out/'truncated.parquet';raw=typed.read_bytes()
        corrupt.write_bytes(b'FAIL'+raw[4:]);truncated.write_bytes(raw[:20])
        legacy_exe=out/'pre-float-consumer';dependencies=ROOT/'.reference/opengauss-native-lake/sdk/install/arrow-20.0.0'
        command('compile-pre-float',[gcc/'gcc/bin/gcc','-std=c11','-O0','-g','-Wall','-Wextra','-Werror',
            '-I',legacy_header.parent,'-I',dependencies/'include',reader_probe,'-ldl','-o',legacy_exe])
        compatibility=json.loads(command('pre-float-consumer',[legacy_exe,provider,typed,no_ids,corrupt,truncated,out/'missing.parquet']))
        check('consumer compiled with pre-float header still reads every old type',compatibility['status'],'passed')
        r['pre_float_consumer']=compatibility;r['pre_float_consumer_sha256']=sha(legacy_exe)
        patterns32=[0,0x80000000,0x3f800000,0xbf800000,1,0x80000001,0x007fffff,0x00800000,0x7f7fffff,0xff7fffff,0x7f800000,0xff800000,0x7fc12345,0xffc01234,0x3eaaaaab,0xbeaaaaab]
        patterns64=[0,0x8000000000000000,0x3ff0000000000000,0xbff0000000000000,1,0x8000000000000001,0x000fffffffffffff,0x0010000000000000,0x7fefffffffffffff,0xffefffffffffffff,0x7ff0000000000000,0xfff0000000000000,0x7ff8123456789abc,0xfff8fedcba987654,0x3fd5555555555555,0xbfd5555555555555]
        def classes(patterns,wide):
            result=dict(nulls=0,nans=0,infs=0,negative_zeros=0)
            for row in range(8193):
                if row%(19 if wide else 17)==(18 if wide else 16):result['nulls']+=1;continue
                value=struct.unpack('>d'if wide else'>f',struct.pack('>Q'if wide else'>I',patterns[row%16]))[0]
                if math.isnan(value):result['nans']+=1
                elif math.isinf(value):result['infs']+=1
                elif value==0 and math.copysign(1,value)<0:result['negative_zeros']+=1
            return result
        expected32=classes(patterns32,False);expected64=classes(patterns64,True)
        for number in range(3):
            path=data/('floating_'+str(number)+'.parquet')
            sql='SELECT count(*) AS rows,typeof("f32.bits") AS type32,typeof("f64_值") AS type64,'
            fields=[]
            for name,suffix in [('"f32.bits"','32'),('"f64_值"','64')]:
                for label,test in [('nulls',name+' IS NULL'),('nans','isnan('+name+')'),('infs','isinf('+name+')'),('negative_zeros',name+'=0 AND signbit('+name+')')]:
                    fields.append('count(*) FILTER(WHERE '+test+') AS '+label+suffix)
            sql+=','.join(fields)+' FROM read_parquet('+literal(path)+') GROUP BY ALL'
            rows=json.loads(command('independent-'+str(number),[duckdb,'-no-init','-batch','-json','-c','SET threads=1;'+sql]))
            check('independent SQL types/rows '+str(number),(rows[0]['rows'],rows[0]['type32'],rows[0]['type64']),(8193,'FLOAT','DOUBLE'))
            for suffix,expected in [('32',expected32),('64',expected64)]:
                check('independent floating classes '+suffix+'/'+str(number),{key:rows[0][key+suffix]for key in expected},expected)
        check('all frozen sources unchanged',all(sha(f)==h for f,h in r['inputs'].items()))
        check('legacy provider untouched',sha(old),r['old_provider_sha256'])
        check('running native provider untouched',sha(installed),r['installed_provider_sha256'])
        check('isolated library unchanged',sha(provider),r['provider_sha256'])
        r.update(status='passed',artifacts={str(f):sha(f)for f in data.glob('*.parquet')})
    except BaseException as exc:r.update(reason=str(exc),traceback=traceback.format_exc())
    r['finished_at']=datetime.datetime.now(datetime.timezone.utc).isoformat();(out/'result.json').write_text(json.dumps(r,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps(dict(status=r['status'],checks=len(r['assertions']),consumer_checks=r.get('consumer',{}).get('checks'),reason=r.get('reason'),result=str(out/'result.json')),ensure_ascii=False))
    return 0 if r['status']=='passed'else 1

if __name__=='__main__':raise SystemExit(main())
