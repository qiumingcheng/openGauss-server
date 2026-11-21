#!/usr/bin/env python3
"""Isolated standard Parquet BINARY transport; no lake ANN/bytea qualification."""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import traceback

ROOT=Path(os.environ.get('CPP_WORKSPACE') or next(
    (str(parent) for parent in Path(__file__).resolve().parents
     if (parent/'integration/opengauss-native-lake/source-lock.json').is_file()),
    str(Path(__file__).resolve().parents[4]))).resolve()
D=ROOT/'integration/opengauss-native-lake'
def sha(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()
def literal(value):return "'"+str(value).replace("'","''")+"'"

def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--run-id',required=True)
    p.add_argument('--provider-build',type=Path,required=True)
    p.add_argument('--source',type=Path,default=ROOT/'.worktrees/opengauss-native-lake-inline-index')
    p.add_argument('--old-provider',type=Path,required=True)
    a=p.parse_args()
    if not a.run_id or any(c not in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.-'for c in a.run_id):p.error('simple run-id required')
    out=ROOT/'公共文档/openGauss原生湖存储/验证'/a.run_id;out.mkdir(exist_ok=False)
    build=json.loads(a.provider_build.read_text());provider=Path(build['library']);source=a.source.resolve()/'contrib/og_lake'
    old=a.old_provider.resolve()
    installed=ROOT/'.install/opengauss-native-lake-debug/lib/postgresql/og_lake_sdk/libog_lake_sdk_arrow.so'
    duckdb=ROOT/'.reference/opengauss-native-lake/build/duckdb-gcc10/duckdb'
    probe=Path(__file__).with_name('provider_binary_test.c');gcc=ROOT/'binarylibs/buildtools/gcc10.3'
    files=[Path(__file__),probe,a.provider_build.resolve(),source/'sdk/lake_parquet_sdk.h',source/'sdk/arrow_parquet_provider.cpp',source/'vendor/arrow20/abi.h']
    r=dict(status='failed',scope=__doc__,assertions=[],provider_sha256=sha(provider),old_provider_sha256=sha(old),installed_provider_sha256=sha(installed),reference_binary_sha256=sha(duckdb),inputs={str(f):sha(f)for f in files})
    archive=out/'executed-inputs';archive.mkdir();r['archive_index']={}
    for number,f in enumerate(files):
        name=str(number)+'-'+f.name;shutil.copy2(f,archive/name);r['archive_index'][str(f)]=name
    env=dict(os.environ);env['LD_LIBRARY_PATH']=':'.join(map(str,[ROOT/'.install/opengauss-native-lake-debug/lib']+[gcc/part for part in ['gcc/lib64','isl/lib','mpc/lib','mpfr/lib','gmp/lib']]))
    def check(name,got,want=True):
        # JSON-normalize tuples so the stored observed/expected values match.
        got=json.loads(json.dumps(got));want=json.loads(json.dumps(want));good=got==want
        r['assertions'].append(dict(name=name,observed=got,expected=want,status='pass'if good else'fail'))
        if not good:raise AssertionError(name)
    def run(name,args):
        with(out/(name+'.log')).open('w')as log:subprocess.run(list(map(str,args)),env=env,stdout=log,stderr=subprocess.STDOUT,check=True,timeout=300)
        return(out/(name+'.log')).read_text()
    try:
        check('isolated build sources exact',all(sha(source/'sdk'/name)==digest for name,digest in build['sources_after'].items()))
        check('built binary exact',sha(provider),build['sha256'])
        exe=out/'binary-consumer'
        run('compile',[gcc/'gcc/bin/gcc','-std=c11','-O0','-g','-Wall','-Wextra','-Werror','-I',source,probe,'-ldl','-o',exe])
        data=out/'data';data.mkdir();r['consumer']=json.loads(run('consumer',[exe,provider,data]));check('real C consumer roundtrip',r['consumer']['status'],'pass')
        legacy=out/'old-provider';legacy.mkdir();r['old_consumer']=json.loads(run('old-provider',[exe,old,legacy,'unsupported']))
        check('old provider safely refuses new field type',r['old_consumer']['status'],'pass');check('unsupported codec creates no file',list(legacy.iterdir()),[])
        def n(row):return 8192 if row%257==0 else 0 if row%8==0 else row%256
        values=[None if i%17==16 else bytes((i*13+j*31)%256 for j in range(n(i)))for i in range(4097)]
        digest=hashlib.sha256('|'.join('NULL'if v is None else v.hex().upper()for v in values).encode()).hexdigest()
        expected=dict(rows=4097,type='BLOB',nulls=sum(v is None for v in values),empty=sum(v==b''for v in values),bytes=sum(len(v)for v in values if v is not None),digest=digest)
        for codec in range(3):
            path=data/f'binary_{codec}.parquet'
            sql='SET threads=1;WITH x AS(SELECT row_number() OVER() AS n,"node.bytes" AS b FROM read_parquet('+literal(path)+')) SELECT count(*) AS rows,any_value(typeof(b)) AS type,count(*) FILTER(WHERE b IS NULL) AS nulls,count(*) FILTER(WHERE octet_length(b)=0) AS empty,sum(octet_length(b)) AS bytes,sha256(string_agg(coalesce(hex(b),\'NULL\'),\'|\' ORDER BY n)) AS digest FROM x'
            sql=sql.replace('sum(octet_length(b)) AS bytes','sum(octet_length(b))::BIGINT AS bytes')
            actual=json.loads(run('independent-'+str(codec),[duckdb,'-no-init','-batch','-json','-c',sql]))[0]
            check('independent SQL byte identity '+str(codec),actual,expected)
        external=out/'duckdb.parquet'
        length='CASE WHEN i%257=0 THEN 8192 WHEN i%8=0 THEN 0 ELSE i%256 END'
        expr="CASE WHEN i%17=16 THEN NULL ELSE unhex(coalesce((SELECT string_agg(lpad(hex((i*13+j*31)%256),2,'0'),'' ORDER BY j) FROM range("+length+")x(j)),'')) END"
        sql='SET threads=1;COPY(SELECT '+expr+' AS "node.bytes" FROM range(4097)r(i) ORDER BY i) TO '+literal(external)+' (FORMAT PARQUET,COMPRESSION ZSTD,ROW_GROUP_SIZE 2048,FIELD_IDS {"node.bytes":101});'
        run('external-writer',[duckdb,'-no-init','-batch','-c',sql]);r['external_consumer']=json.loads(run('external-consumer',[exe,provider,external,'read']))
        check('independent DuckDB binary writer read exactly',r['external_consumer']['status'],'pass')
        # A second process reopens immutable files with no writer state.
        r['reopen_consumer']=json.loads(run('fresh-process-reopen',[exe,provider,data/'binary_1.parquet','read']))
        check('cross-process reopen is independent',r['reopen_consumer']['status'],'pass')
        check('frozen test sources unchanged',all(sha(f)==h for f,h in r['inputs'].items()))
        check('genuine pre-BINARY provider untouched',sha(old),r['old_provider_sha256'])
        check('installed provider untouched',sha(installed),r['installed_provider_sha256'])
        check('isolated provider unchanged',sha(provider),r['provider_sha256'])
        r.update(status='passed',artifacts={str(f):sha(f)for f in [*data.glob('*.parquet'),external]},consumer_sha256=sha(exe))
    except BaseException as exc:r.update(reason=str(exc),traceback=traceback.format_exc())
    r['finished_at']=datetime.datetime.now(datetime.timezone.utc).isoformat();(out/'result.json').write_text(json.dumps(r,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps(dict(status=r['status'],checks=len(r['assertions']),consumer_checks=r.get('consumer',{}).get('checks'),reason=r.get('reason'),result=str(out/'result.json')),ensure_ascii=False))
    return 0 if r['status']=='passed'else 1

if __name__=='__main__':raise SystemExit(main())
