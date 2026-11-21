#!/usr/bin/env python3
"""Portable DiskANN graph-record component, without algorithm or host integration.

Tests use synthetic topology only to check its exact transport. They are not
native graph construction, ANN recall, file coverage or lake query acceptance.
"""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import shutil
import struct
import subprocess
import traceback

ROOT=Path('/home/qiumc/data/code/cpp')
def sha(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()
def literal(value):return "'"+str(value).replace("'","''")+"'"
def image(rows):return dict(rows=len(rows),sha256=hashlib.sha256(json.dumps(rows,sort_keys=True).encode()).hexdigest())

def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--run-id',required=True)
    p.add_argument('--source',type=Path,default=ROOT/'.worktrees/opengauss-native-lake-inline-index')
    p.add_argument('--provider',type=Path,required=True);a=p.parse_args()
    if not a.run_id or any(c not in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.-'for c in a.run_id):p.error('simple new run-id required')
    out=ROOT/'公共文档/openGauss原生湖存储/验证'/a.run_id;out.mkdir(exist_ok=False)
    source=a.source.resolve()/'contrib/og_lake';probe=Path(__file__).with_name('ann_format_probe.cpp')
    gcc=ROOT/'binarylibs/buildtools/gcc10.3';openssl=ROOT/'binarylibs/kernel/dependency/openssl/comm'
    provider=a.provider.resolve();installed=ROOT/'.install/opengauss-native-lake-debug/lib/postgresql/og_lake_sdk/libog_lake_sdk_arrow.so'
    duckdb=ROOT/'.reference/opengauss-native-lake/build/duckdb-gcc10/duckdb'
    files=[Path(__file__),probe,source/'lake_ann_format.cpp',source/'lake_ann_format.h',source/'sdk/lake_parquet_sdk.h',source/'vendor/arrow20/abi.h']
    r=dict(status='failed',scope=__doc__,assertions=[],inputs={str(f):sha(f)for f in files},provider_sha256=sha(provider),installed_provider_sha256=sha(installed),reference_binary_sha256=sha(duckdb))
    archive=out/'executed-inputs';archive.mkdir()
    for f in files:shutil.copy2(f,archive/f.name)
    env=dict(os.environ);env['LD_LIBRARY_PATH']=':'.join(map(str,[ROOT/'.install/opengauss-native-lake-debug/lib']+[gcc/part for part in ['gcc/lib64','isl/lib','mpc/lib','mpfr/lib','gmp/lib']]))
    def check(name,got,want=True):
        good=got==want;r['assertions'].append(dict(name=name,observed=got,expected=want,status='pass'if good else'fail'))
        if not good:raise AssertionError(name)
    def run(name,args):
        with(out/(name+'.log')).open('w')as log:subprocess.run(list(map(str,args)),env=env,stdout=log,stderr=subprocess.STDOUT,check=True,timeout=180)
        return(out/(name+'.log')).read_text()
    def sql(name,statement):
        (out/(name+'.sql')).write_text(statement+'\n')
        return json.loads(run(name,[duckdb,'-no-init','-batch','-json','-c','SET threads=1;'+statement])or'[]')
    try:
        exe=out/'ann-format-probe'
        run('compile',[gcc/'gcc/bin/g++','-std=c++14','-O0','-g','-D_GLIBCXX_USE_CXX11_ABI=0','-Wall','-Wextra','-Werror','-Wno-deprecated-declarations','-Wno-misleading-indentation','-I',source,'-I',openssl/'include',probe,source/'lake_ann_format.cpp','-L',openssl/'lib','-lcrypto','-ldl','-o',exe])
        check('standalone Debug compilation',exe.is_file());r['consumer_binary_sha256']=sha(exe)
        artifacts=out/'artifacts';artifacts.mkdir();value=json.loads(run('consumer',[exe,provider,artifacts]));r['consumer']=value
        check('real graph format consumer passes',value['status'],'pass');check('three metrics and empty manifest roots',len(value['roots']),4)
        expected=[]
        for i in range(143):
            expected.append(dict(node_id=i,flags=int(i in [0,142]),v=(struct.pack('<f',i+.25)+struct.pack('<II',0x80000000,1)).hex().upper(),
                e=struct.pack('<II',(i+1)%143,(i+142)%143).hex().upper(),
                p=struct.pack('<qqqqqqqq',i//100,i,i+100000,17,i//100+2,i,i+100000,18).hex().upper()))
        for metric in range(1,4):
            directory=artifacts/f'metric{metric}';path=directory/'ann_nodes.parquet'
            rows=sql('independent-'+str(metric),'SELECT node_id,flags,hex(vector_le) AS v,hex(neighbors_le) AS e,hex(postings_le) AS p FROM read_parquet('+literal(path)+') ORDER BY node_id')
            check('independent explicit byte order / physical-version mapping '+str(metric),image(rows),image(expected))
            groups=sql('row-groups-'+str(metric),'SELECT DISTINCT row_group_id,row_group_num_rows FROM parquet_metadata('+literal(path)+') ORDER BY row_group_id')
            check('bounded page row groups '+str(metric),groups,[dict(row_group_id=i,row_group_num_rows=n)for i,n in enumerate([64,64,15])])
            def integer(value):return struct.pack('<Q',value)
            def start(domain):
                return hashlib.sha256(domain.encode()+b'0123456789abcdef'*4+b''.join(integer(v)for v in [1,3,metric,143,2,286,64]))
            pages=[]
            for page_id in range(3):
                selected=rows[page_id*64:(page_id+1)*64];h=start('og_lake_ann_page_v1');h.update(integer(page_id)+integer(len(selected)))
                for row in selected:
                    h.update(integer(row['node_id'])+integer(row['flags']&1)+integer((row['flags']>>1)&1))
                    for name in ['v','e','p']:
                        raw=bytes.fromhex(row[name]);h.update(integer(len(raw))+raw)
                pages.append(dict(page_id=page_id,row_count=len(selected),entry_count=sum(x['flags']&1 for x in selected),posting_count=len(selected)*2,page_hash=h.hexdigest()))
            directory_rows=sql('directory-'+str(metric),'SELECT * FROM read_parquet('+literal(directory/'ann_lookup.parquet')+') ORDER BY page_id')
            check('independent page checksums and population '+str(metric),directory_rows,pages)
            h=start('og_lake_ann_directory_v1');h.update(integer(3))
            for page in pages:h.update(b''.join(integer(page[k])for k in ['page_id','row_count','entry_count','posting_count'])+page['page_hash'].encode())
            check('independent definition/source-bound manifest '+str(metric),h.hexdigest(),value['roots'][metric-1])
            fresh=json.loads(run('reopen-'+str(metric),[exe,provider,directory,value['roots'][metric-1],metric,143,'read']))
            check('fresh process reopens graph '+str(metric),fresh['status'],'pass')
        # Both alternatives are valid Parquet. Only the catalog-bound hash tells
        # the reader which graph belongs to this definition/source generation.
        bad=artifacts/'wrong-page';bad.mkdir();shutil.copy2(artifacts/'altered/ann_nodes.parquet',bad/'ann_nodes.parquet')
        shutil.copy2(artifacts/'metric1/ann_lookup.parquet',bad/'ann_lookup.parquet')
        result=json.loads(run('wrong-page',[exe,provider,bad,value['roots'][0],1,143,'bad']))
        check('changed graph page with original directory rejected',result['status'],'pass')
        corrupt=artifacts/'wrong-directory';corrupt.mkdir();shutil.copy2(artifacts/'metric1/ann_nodes.parquet',corrupt/'ann_nodes.parquet')
        shutil.copy2(artifacts/'altered/ann_lookup.parquet',corrupt/'ann_lookup.parquet')
        result=json.loads(run('wrong-directory',[exe,provider,corrupt,value['roots'][0],1,143,'bad']))
        check('changed page directory rejected',result['status'],'pass')
        short=artifacts/'truncated';short.mkdir();raw=(artifacts/'metric1/ann_nodes.parquet').read_bytes();(short/'ann_nodes.parquet').write_bytes(raw[:20])
        shutil.copy2(artifacts/'metric1/ann_lookup.parquet',short/'ann_lookup.parquet')
        result=json.loads(run('truncated',[exe,provider,short,value['roots'][0],1,143,'bad']))
        check('truncated graph rejected',result['status'],'pass')
        for i in range(9):check('invalid candidate objects discarded '+str(i),list((artifacts/f'invalid{i}').iterdir()),[])
        check('all compiled/executed inputs unchanged',all(sha(f)==h for f,h in r['inputs'].items()))
        check('isolated provider unchanged',sha(provider),r['provider_sha256']);check('native installed provider untouched',sha(installed),r['installed_provider_sha256'])
        r.update(status='passed',artifacts={str(f):sha(f)for f in artifacts.rglob('*.parquet')})
    except BaseException as exc:r.update(reason=str(exc),traceback=traceback.format_exc())
    r['finished_at']=datetime.datetime.now(datetime.timezone.utc).isoformat();(out/'result.json').write_text(json.dumps(r,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps(dict(status=r['status'],checks=len(r['assertions']),native_checks=r.get('consumer',{}).get('checks'),reason=r.get('reason'),result=str(out/'result.json')),ensure_ascii=False))
    return 0 if r['status']=='passed'else 1

if __name__=='__main__':raise SystemExit(main())
