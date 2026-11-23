#!/usr/bin/env python3
"""Real file differential fixture, consumed by a C11 host through dlopen."""
import argparse, datetime, hashlib, json, os, subprocess
from pathlib import Path
from build_provider import ROOT,SDK,PREFIX,GCC,SOURCE,sha,inputs,dependencies

def main():
    parser=argparse.ArgumentParser();parser.add_argument('--evidence',type=Path,required=True)
    args=parser.parse_args();evidence=args.evidence.resolve();evidence.mkdir(parents=True,exist_ok=False)
    duckdb=ROOT/'.reference/opengauss-native-lake/build/duckdb-gcc10/duckdb'
    library=PREFIX/'lib/libog_lake_sdk_arrow.so'
    test=Path(__file__).with_name('provider_reader_test.c');exe=evidence/'reader-c-host'
    receipt={'status':'failed','scope':'standalone local real Parquet reader; no native FDW or private SDK claim',
             'started_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),
             'provider_sources_before':inputs(),'dependencies_before':dependencies(),'library_sha256':sha(library),
             'reference_binary_sha256':sha(duckdb),'test_sha256':sha(test),'driver_sha256':sha(__file__)}
    env=os.environ.copy();support=[GCC/'lib64']+[GCC.parent/p/'lib' for p in ('isl','mpc','mpfr','gmp')]
    env['LD_LIBRARY_PATH']=':'.join(map(str,[PREFIX/'lib']+support))
    def command(name,argv):
        with (evidence/(name+'.log')).open('w') as log:
            subprocess.run(list(map(str,argv)),env=env,stdout=log,stderr=subprocess.STDOUT,check=True)
    try:
        good=evidence/'typed.parquet';no_ids=evidence/'no_ids.parquet'
        def literal(path):return "'"+str(path).replace("'","''")+"'"
        sql="""SET threads=1;
CREATE TABLE input AS SELECT i::INTEGER AS id,
 CASE WHEN i%5=0 THEN NULL ELSE i%2=0 END AS flag,
 CASE WHEN i=0 THEN '-9223372036854775808'::BIGINT WHEN i=1 THEN '9223372036854775807'::BIGINT
      WHEN i%7=0 THEN NULL ELSE i*10000000000 END AS big,
 CASE WHEN i%13=0 THEN NULL WHEN i%11=0 THEN '' ELSE '湖/'||i::VARCHAR||'/'||repeat('x',1024) END AS txt
 FROM range(12288) q(i);
"""
        sql+='COPY input TO '+literal(good)+" (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 2048, FIELD_IDS {id:11,flag:21,big:31,txt:41});\n"
        sql+='COPY input TO '+literal(no_ids)+" (FORMAT PARQUET, COMPRESSION SNAPPY, ROW_GROUP_SIZE 2048);\n"
        (evidence/'fixture.sql').write_text(sql)
        command('fixture',[duckdb,'-no-agent','-no-stdin','-c',sql])
        corrupt=evidence/'bad_magic.parquet';truncated=evidence/'truncated.parquet'
        data=good.read_bytes();corrupt.write_bytes(b'FAIL'+data[4:]);truncated.write_bytes(data[:20])
        command('compile',[GCC/'bin/gcc','-std=c11','-O2','-Wall','-Wextra','-Werror',
                '-I'+str(SOURCE),'-I'+str(PREFIX/'include'),test,'-ldl','-o',exe])
        command('reader',[exe,library,good,no_ids,corrupt,truncated,evidence/'does-not-exist.parquet'])
        result=json.loads((evidence/'reader.log').read_text())
        receipt['provider_sources_after']=inputs()
        receipt['dependencies_after']=dependencies()
        if receipt['provider_sources_before']!=receipt['provider_sources_after'] or receipt['dependencies_before']!=receipt['dependencies_after'] or sha(library)!=receipt['library_sha256']:
            raise RuntimeError('provider inputs changed during reader test')
        receipt.update(status='passed',reader=result,fixtures={p.name:sha(p) for p in (good,no_ids,corrupt,truncated)},
                       consumer_binary_sha256=sha(exe))
        print(json.dumps(result),flush=True);return 0
    except Exception as exc:
        receipt['error']=str(exc);print(str(exc),flush=True);return 1
    finally:
        (evidence/'result.json').write_text(json.dumps(receipt,indent=2)+'\n')

if __name__=='__main__':raise SystemExit(main())
