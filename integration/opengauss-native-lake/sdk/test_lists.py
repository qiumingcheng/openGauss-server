#!/usr/bin/env python3
"""Isolated standard LIST<FLOAT32> codec, not native SQL/vector/DiskANN qualification."""
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
def q(v):return "'"+str(v).replace("'","''")+"'"

def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--run-id',required=True)
    p.add_argument('--provider-build',type=Path,required=True);p.add_argument('--legacy-input',type=Path,required=True)
    p.add_argument('--old-provider',type=Path,required=True);p.add_argument('--prefix-provider',type=Path)
    p.add_argument('--source',type=Path,default=ROOT/'.worktrees/opengauss-native-lake-inline-index');a=p.parse_args()
    if not a.run_id or any(c not in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.-'for c in a.run_id):p.error('simple new run-id required')
    out=ROOT/'公共文档/openGauss原生湖存储/验证'/a.run_id;out.mkdir(exist_ok=False)
    source=a.source.resolve()/'contrib/og_lake';probe=Path(__file__).with_name('provider_list_test.c')
    bpath=a.provider_build.resolve();b=json.loads(bpath.read_text());provider=Path(b['library'])
    old=a.old_provider.resolve()
    installed=ROOT/'.install/opengauss-native-lake-debug/lib/postgresql/og_lake_sdk/libog_lake_sdk_arrow.so'
    receipt_path=ROOT/'.reference/opengauss-native-lake/build/receipt.json';receipt=json.loads(receipt_path.read_text());duckdb=Path(receipt['binary'])
    gcc=ROOT/'binarylibs/buildtools/gcc10.3';files=[Path(__file__),probe,source/'sdk/lake_parquet_sdk.h',source/'sdk/arrow_parquet_provider.cpp',source/'sdk/exports.map',source/'vendor/arrow20/abi.h',D/'source-lock.json',receipt_path,D/'sdk/provider_writer_test.c',D/'sdk/provider_reader_test.c',ROOT/'.worktrees/opengauss-native-lake/contrib/og_lake/sdk/lake_parquet_sdk.h']
    r={'status':'failed','scope':__doc__,'assertions':[],'inputs':{str(f):sha(f)for f in files},
       'provider_build':str(bpath),'provider_build_sha256':sha(bpath),'provider_sha256':sha(provider),
       'old_provider_sha256':sha(old),'reference_binary_sha256':sha(duckdb),'installed_provider_sha256':sha(installed)}
    archive=out/'executed-inputs';archive.mkdir();r['archive_index']={}
    for i,f in enumerate(files):
        name=str(i)+'-'+f.name;shutil.copy2(f,archive/name);r['archive_index'][str(f)]=name
    env=dict(os.environ);env['LD_LIBRARY_PATH']=':'.join(map(str,[ROOT/'.install/opengauss-native-lake-debug/lib']+[gcc/t for t in ['gcc/lib64','isl/lib','mpc/lib','mpfr/lib','gmp/lib']]))
    def check(name,got,want=True):
        good=got==want;r['assertions'].append(dict(name=name,observed=got,expected=want,status='pass'if good else'fail'))
        if not good:raise AssertionError(name)
    def command(label,args):
        with(out/(label+'.log')).open('w')as log:subprocess.run(list(map(str,args)),env=env,stdout=log,stderr=subprocess.STDOUT,check=True,timeout=300)
        return(out/(label+'.log')).read_text()
    def sql(label,query):
        (out/(label+'.sql')).write_text(query+'\n');return json.loads(command(label,[duckdb,'-no-init','-batch','-json','-c','SET threads=1;'+query])or'[]')
    try:
        check('actual isolated provider build',b['status'],'provider_built_unqualified')
        check('built library intact',sha(provider),b['sha256'])
        for name in ['lake_parquet_sdk.h','arrow_parquet_provider.cpp','exports.map']:
            check('same built source '+name,sha(source/'sdk'/name),b['sources_before'][name])
        check('reference source-lock unchanged',sha(D/'source-lock.json'),receipt['source_lock_hash'])
        check('reference actual binary matches pinned receipt',sha(duckdb),receipt['binary_sha256'])
        data=out/'reference-data';data.mkdir();catalog=out/'metadata.ducklake'
        attach='ATTACH '+q('ducklake:'+str(catalog))+' AS lake (DATA_PATH '+q(data)+",DUCKLAKE_VERSION '1.1',AUTOMATIC_MIGRATION FALSE,DATA_INLINING_ROW_LIMIT 0);"
        sql('genuine-create',attach+"CREATE TABLE lake.main.vectors(id INTEGER,embedding FLOAT[]); INSERT INTO lake.main.vectors VALUES (1,[1.5,0,2.25]::FLOAT[]),(2,[]::FLOAT[]),(3,NULL),(4,[NULL,3]::FLOAT[]);")
        columns=sql('genuine-columns',attach+'SELECT column_id,column_name,column_type,parent_column FROM __ducklake_metadata_lake.main.ducklake_column ORDER BY column_id;')
        check('genuine LIST parent and FLOAT child catalog IDs',columns,[{'column_id':1,'column_name':'id','column_type':'int32','parent_column':None},{'column_id':2,'column_name':'embedding','column_type':'list','parent_column':None},{'column_id':3,'column_name':'element','column_type':'float32','parent_column':2}])
        references=list(data.rglob('*.parquet'));check('one genuine DuckLake artifact',len(references),1)
        schema=sql('genuine-parquet-schema','SELECT name,type,converted_type,field_id FROM parquet_schema('+q(references[0])+');')
        check('genuine LIST stores root and element IDs separately',[(f['name'],f['field_id'])for f in schema if f['field_id']is not None],[('id',1),('embedding',2),('element',3)])
        exe=out/'list-consumer';command('compile',[gcc/'gcc/bin/gcc','-std=c11','-O0','-g','-Wall','-Wextra','-Werror','-I',source,probe,'-ldl','-o',exe]);r['consumer_sha256']=sha(exe)
        artifacts=out/'artifacts';artifacts.mkdir();c=json.loads(command('consumer',[exe,provider,artifacts,references[0]]));r['consumer']=c
        check('actual nested C consumer passed',c['status'],'pass');check('three compression artifacts',c['files'],3)
        old_data=out/'old-provider';old_data.mkdir();old_c=json.loads(command('old-provider',[exe,old,old_data,references[0],'unsupported']))
        check('old provider safely rejects nested capability',old_c['status'],'pass');check('old provider creates no unsupported object',list(old_data.iterdir()),[])
        if a.prefix_provider:
            prior=a.prefix_provider.resolve();r['prefix_provider_sha256']=sha(prior)
            prefix=json.loads(command('prefix-provider',[exe,prior,old_data,references[0],'prefix']))
            check('new consumer safely reads the prior footer prefix',prefix['status'],'pass')
        noids=out/'no-field-ids.parquet'
        sql('no-field-ids','COPY(SELECT * FROM read_parquet('+q(references[0])+'))TO '+q(noids)+'(FORMAT PARQUET,COMPRESSION ZSTD);')
        missing=json.loads(command('no-field-ids-consumer',[exe,provider,old_data,noids,'noids']))
        check('absent element IDs are explicitly reported, never inferred',missing['status'],'pass')
        classes=dict(rows=8193,null_lists=0,empty_lists=0,elements=0,null_elements=0,nans=0,infs=0,negative_zeros=0)
        patterns=[0,0x80000000,0x3f800000,0xbf800000,1,0x80000001,0x007fffff,0x00800000,0x7f7fffff,0xff7fffff,0x7f800000,0xff800000,0x7fc12345,0xffc01234,0x3eaaaaab,0xbeaaaaab]
        for row in range(8193):
            if row%17==16:classes['null_lists']+=1;continue
            if row%7==6:classes['empty_lists']+=1;continue
            for k in range(3):
                classes['elements']+=1
                if row%13==12 and k==1:classes['null_elements']+=1;continue
                value=struct.unpack('>f',struct.pack('>I',patterns[(row*3+k)%16]))[0]
                if math.isnan(value):classes['nans']+=1
                elif math.isinf(value):classes['infs']+=1
                elif value==0 and math.copysign(1,value)<0:classes['negative_zeros']+=1
        for n in range(3):
            f=artifacts/('list_'+str(n)+'.parquet')
            s=sql('independent-schema-'+str(n),'SELECT name,type,converted_type,field_id FROM parquet_schema('+q(f)+');')
            check('output standard Parquet parent/child IDs '+str(n),[(v['name'],v['field_id'])for v in s if v['field_id']is not None],[('id',11),('embedding.湖',73),('element',9)])
            query='WITH t AS(SELECT * FROM read_parquet('+q(f)+')), e AS(SELECT unnest("embedding.湖") AS value FROM t) SELECT (SELECT count(*) FROM t)AS rows,(SELECT count(*) FROM t WHERE "embedding.湖" IS NULL)AS null_lists,(SELECT count(*) FROM t WHERE len("embedding.湖")=0)AS empty_lists,count(*)AS elements,count(*)FILTER(WHERE value IS NULL)AS null_elements,count(*)FILTER(WHERE isnan(value))AS nans,count(*)FILTER(WHERE isinf(value))AS infs,count(*)FILTER(WHERE value=0 AND signbit(value))AS negative_zeros FROM e;'
            v=sql('independent-values-'+str(n),query)
            check('independent SQL nested value classes '+str(n),v,[classes])
        # Exercise the actual old writer binary ABI, not only a new-header reader.
        prior=a.legacy_input.resolve()
        check('frozen independently generated legacy input exists',prior.is_file())
        r['legacy_input']={'path':str(prior),'sha256':sha(prior)}
        legacy_header=ROOT/'.worktrees/opengauss-native-lake/contrib/og_lake/sdk'
        old_exe=out/'old-writer';dependencies=ROOT/'.reference/opengauss-native-lake/sdk/install/arrow-20.0.0'
        command('compile-old-writer',[gcc/'gcc/bin/gcc','-std=c11','-O0','-g','-Wall','-Wextra','-Werror','-I',legacy_header,'-I',dependencies/'include',D/'sdk/provider_writer_test.c','-ldl','-o',old_exe])
        legacy_out=out/'old-writer-output';legacy_out.mkdir()
        w=json.loads(command('old-writer',[old_exe,provider,prior,*[legacy_out/f for f in ['written.parquet','abort.parquet','cancel.parquet','replaced.parquet','empty.parquet']]]));r['pre_list_writer']=w;r['pre_list_writer_sha256']=sha(old_exe)
        check('old-header writer abort/cancel/replace/empty and real full roundtrip',w['status'],'passed')
        difference=sql('old-writer-independent','SELECT count(*) AS differences FROM ((SELECT * FROM read_parquet('+q(prior)+') EXCEPT ALL SELECT * FROM read_parquet('+q(legacy_out/'written.parquet')+')) UNION ALL (SELECT * FROM read_parquet('+q(legacy_out/'written.parquet')+') EXCEPT ALL SELECT * FROM read_parquet('+q(prior)+')));')
        check('independent full-value old writer multiset match',difference,[{'differences':0}])
        required_schema=sql('required-elements-schema','SELECT repetition_type FROM parquet_schema('+q(artifacts/'required-elements.parquet')+')WHERE field_id=9;')
        check('independent reader sees required element physical schema',required_schema,[{'repetition_type':'REQUIRED'}])
        check('every frozen source remains unchanged',all(sha(f)==h for f,h in r['inputs'].items()))
        check('legacy provider untouched',sha(old),r['old_provider_sha256'])
        check('running native provider untouched',sha(installed),r['installed_provider_sha256'])
        check('tested isolated library unchanged',sha(provider),r['provider_sha256'])
        r.update(status='passed',artifacts={str(f):sha(f)for f in [*artifacts.glob('*.parquet'),*references]})
    except BaseException as exc:r.update(reason=str(exc),traceback=traceback.format_exc())
    r['finished_at']=datetime.datetime.now(datetime.timezone.utc).isoformat();(out/'result.json').write_text(json.dumps(r,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps(dict(status=r['status'],assertions=len(r['assertions']),consumer_checks=r.get('consumer',{}).get('checks'),reason=r.get('reason'),result=str(out/'result.json')),ensure_ascii=False))
    return 0 if r['status']=='passed'else 1
if __name__=='__main__':raise SystemExit(main())
