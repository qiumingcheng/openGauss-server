#!/usr/bin/env python3
"""Qualify optional object identities and both directions of the unchanged C ABI.

Isolated provider only; this does not qualify native file-index publication,
query pruning, remote object version semantics or RAC durability.
"""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import traceback

ROOT=Path(__file__).resolve().parents[3]
def sha(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()

def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--evidence',type=Path,required=True)
    p.add_argument('--provider',type=Path,required=True)
    p.add_argument('--source',type=Path,required=True)
    a=p.parse_args();out=a.evidence.resolve();out.mkdir(parents=True,exist_ok=False)
    source=a.source.resolve()/'contrib/og_lake/sdk';library=a.provider.resolve()
    legacy=ROOT/'.install/opengauss-native-lake-debug/lib/postgresql/og_lake_sdk/libog_lake_sdk_arrow.so'
    old_source=ROOT/'.worktrees/opengauss-native-lake/contrib/og_lake/sdk'
    scripts=Path(__file__).parent;gcc=ROOT/'binarylibs/buildtools/gcc10.3/gcc'
    dependencies=ROOT/'.reference/opengauss-native-lake/sdk/install/arrow-20.0.0'
    duckdb=ROOT/'.reference/opengauss-native-lake/build/duckdb-gcc10/duckdb'
    files=[Path(__file__),scripts/'provider_object_version_test.c',scripts/'provider_reader_test.c']
    files+=list(source.glob('*.h'))+list(source.glob('*.cpp'))+[old_source/'lake_parquet_sdk.h']
    r={'status':'failed','scope':__doc__,'assertions':[],
       'inputs':{str(x):sha(x)for x in files},'provider':str(library),'provider_sha256':sha(library),
       'legacy_provider':str(legacy),'legacy_provider_sha256':sha(legacy),'reference_binary_sha256':sha(duckdb)}
    archive=out/'executed-inputs';archive.mkdir()
    for number,x in enumerate(files):shutil.copy2(x,archive/(str(number)+'-'+x.name))
    env=dict(os.environ);env['LD_LIBRARY_PATH']=':'.join(map(str,[gcc/'lib64']+[gcc.parent/x/'lib'for x in ['isl','mpc','mpfr','gmp']]))
    def check(name,actual,expected=True):
        good=actual==expected;r['assertions'].append(dict(name=name,observed=actual,expected=expected,status='pass'if good else'fail'))
        if not good:raise AssertionError(name)
    def command(name,args):
        r.setdefault('commands',[]).append(dict(name=name,argv=list(map(str,args))))
        with(out/(name+'.log')).open('w')as log:
            subprocess.run(list(map(str,args)),env=env,stdout=log,stderr=subprocess.STDOUT,check=True,timeout=300)
        return(out/(name+'.log')).read_text()
    def literal(path):return "'"+str(path).replace("'","''")+"'"
    try:
        good=out/'typed.parquet';no_ids=out/'no_ids.parquet'
        sql="""SET threads=1;
CREATE TABLE input AS SELECT i::INTEGER AS id,
 CASE WHEN i%5=0 THEN NULL ELSE i%2=0 END AS flag,
 CASE WHEN i=0 THEN '-9223372036854775808'::BIGINT WHEN i=1 THEN '9223372036854775807'::BIGINT
      WHEN i%7=0 THEN NULL ELSE i*10000000000 END AS big,
 CASE WHEN i%13=0 THEN NULL WHEN i%11=0 THEN '' ELSE '湖/'||i::VARCHAR||'/'||repeat('x',1024) END AS txt
 FROM range(12288) q(i);
"""
        sql+='COPY input TO '+literal(good)+' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 2048, FIELD_IDS {id:11,flag:21,big:31,txt:41});\n'
        sql+='COPY input TO '+literal(no_ids)+' (FORMAT PARQUET, COMPRESSION SNAPPY, ROW_GROUP_SIZE 2048);\n'
        (out/'fixture.sql').write_text(sql);command('fixture',[duckdb,'-no-init','-batch','-c',sql])
        corrupt=out/'bad_magic.parquet';truncated=out/'truncated.parquet';data=good.read_bytes()
        corrupt.write_bytes(b'FAIL'+data[4:]);truncated.write_bytes(data[:20])
        old=out/'legacy-reader-host'
        command('compile-legacy',[gcc/'bin/gcc','-std=c11','-O0','-g','-Wall','-Wextra','-Werror',
            '-I',old_source,'-I',dependencies/'include',scripts/'provider_reader_test.c','-ldl','-o',old])
        args=[good,no_ids,corrupt,truncated,out/'missing.parquet']
        for name,provider in [('legacy_baseline',legacy),('legacy_host_new_provider',library)]:
            value=json.loads(command(name,[old,provider,*args]));check(name+' complete typed reader',value['status'],'passed')
            r.setdefault('consumers',[]).append(dict(name=name,**value))
        new=out/'object-version-host'
        command('compile-new',[gcc/'bin/gcc','-std=c11','-O0','-g','-Wall','-Wextra','-Werror',
            '-Wno-misleading-indentation','-I',source,scripts/'provider_object_version_test.c','-ldl','-o',new])
        first=out/'mutable.parquet';replacement=out/'replacement.parquet'
        shutil.copy2(good,first);shutil.copy2(good,replacement)
        for name,provider,mode in [('new_host_old_provider',legacy,'legacy'),('new_host_new_provider',library,'version')]:
            value=json.loads(command(name,[new,provider,first,replacement,mode]));check(name+' ABI and identity contract',value['status'],'pass')
            r.setdefault('consumers',[]).append(dict(name=name,**value))
        check('all compiled inputs unchanged',all(sha(x)==h for x,h in r['inputs'].items()))
        check('candidate provider unchanged',sha(library),r['provider_sha256'])
        check('installed native provider was not replaced',sha(legacy),r['legacy_provider_sha256'])
        check('reference engine unchanged',sha(duckdb),r['reference_binary_sha256'])
        r.update(status='passed',artifacts={str(x):sha(x)for x in out.glob('*.parquet')},
                 consumers_binary_sha256={str(x):sha(x)for x in (old,new)})
    except BaseException as exc:r.update(reason=str(exc),traceback=traceback.format_exc())
    r['finished_at']=datetime.datetime.now(datetime.timezone.utc).isoformat()
    (out/'result.json').write_text(json.dumps(r,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps({k:r.get(k)for k in ['status','reason','consumers']},ensure_ascii=False))
    return 0 if r['status']=='passed'else 1

if __name__=='__main__':raise SystemExit(main())
