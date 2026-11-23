#!/usr/bin/env python3
"""Write through C ABI, validate through the pinned unmodified DuckDB reader."""
import argparse,datetime,json,os,subprocess
from pathlib import Path
from build_provider import ROOT,PREFIX,GCC,SOURCE,sha,inputs,dependencies

def main():
    parser=argparse.ArgumentParser();parser.add_argument('--evidence',type=Path,required=True)
    parser.add_argument('--input',type=Path,required=True);args=parser.parse_args()
    evidence=args.evidence.resolve();evidence.mkdir(parents=True,exist_ok=False)
    input=args.input.resolve();test=Path(__file__).with_name('provider_writer_test.c')
    reader_test=test.with_name('provider_reader_test.c');exe=evidence/'writer-c-host'
    library=PREFIX/'lib/libog_lake_sdk_arrow.so'
    duckdb=ROOT/'.reference/opengauss-native-lake/build/duckdb-gcc10/duckdb'
    receipt={'status':'failed','scope':'standalone real local Parquet writer; local fsync is not cluster durability',
             'started_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),
             'provider_sources_before':inputs(),'dependencies_before':dependencies(),'library_sha256':sha(library),'input_sha256':sha(input),
             'reference_binary_sha256':sha(duckdb),'driver_sha256':sha(__file__),
             'test_sha256':sha(test),'reader_test_sha256':sha(reader_test)}
    env=os.environ.copy();support=[GCC/'lib64']+[GCC.parent/p/'lib' for p in ('isl','mpc','mpfr','gmp')]
    env['LD_LIBRARY_PATH']=':'.join(map(str,[PREFIX/'lib']+support))
    def command(name,argv):
        with (evidence/(name+'.log')).open('w') as log:
            subprocess.run(list(map(str,argv)),env=env,stdout=log,stderr=subprocess.STDOUT,check=True)
    try:
        command('compile',[GCC/'bin/gcc','-std=c11','-O2','-Wall','-Wextra','-Werror',
            '-I'+str(SOURCE),'-I'+str(PREFIX/'include'),test,'-ldl','-o',exe])
        output=evidence/'written.parquet';empty=evidence/'empty.parquet'
        command('writer',[exe,library,input,output,evidence/'abort.parquet',evidence/'cancel.parquet',
                          evidence/'replaced.parquet',empty])
        result=json.loads((evidence/'writer.log').read_text())
        def literal(path):return "'"+str(path).replace("'","''")+"'"
        sql="SELECT count(*) AS differences FROM ((SELECT * FROM read_parquet("+literal(input)+") EXCEPT ALL SELECT * FROM read_parquet("+literal(output)+")) UNION ALL (SELECT * FROM read_parquet("+literal(output)+") EXCEPT ALL SELECT * FROM read_parquet("+literal(input)+")));"
        sql+=' SELECT count(*) AS empty_count FROM read_parquet('+literal(empty)+');'
        (evidence/'reference.sql').write_text(sql)
        command('reference',[duckdb,'-no-agent','-csv','-no-stdin','-c',sql])
        lines=(evidence/'reference.log').read_text().splitlines()
        if lines!=['differences','0','empty_count','0']:raise RuntimeError('pinned reader sees differences: '+repr(lines))
        receipt['provider_sources_after']=inputs()
        receipt['dependencies_after']=dependencies()
        if receipt['provider_sources_after']!=receipt['provider_sources_before'] or receipt['dependencies_before']!=receipt['dependencies_after'] or sha(library)!=receipt['library_sha256'] or sha(input)!=receipt['input_sha256']:
            raise RuntimeError('writer inputs changed during verification')
        receipt.update(status='passed',writer=result,output_sha256=sha(output),empty_sha256=sha(empty),
                       reference_differences=0,reference_empty_count=0,consumer_binary_sha256=sha(exe))
        print(json.dumps(result),flush=True);return 0
    except Exception as exc:
        receipt['error']=str(exc);print(str(exc),flush=True);return 1
    finally:(evidence/'result.json').write_text(json.dumps(receipt,indent=2)+'\n')

if __name__=='__main__':raise SystemExit(main())
