#!/usr/bin/env python3
"""Actual local I/O failures and borrowed C Data lifetime qualification."""
import argparse,datetime,json,os,subprocess
from pathlib import Path
from build_provider import ROOT,PREFIX,GCC,SOURCE,sha,inputs,dependencies

def main():
    parser=argparse.ArgumentParser();parser.add_argument('--evidence',type=Path,required=True)
    parser.add_argument('--input',type=Path,required=True);parser.add_argument('--provider',type=Path,default=PREFIX/'lib/libog_lake_sdk_arrow.so');args=parser.parse_args()
    evidence=args.evidence.resolve();evidence.mkdir(parents=True,exist_ok=False)
    test=Path(__file__).with_name('provider_writer_fault_test.c');faults=test.with_name('provider_io_faults.c')
    consumer=evidence/'writer-fault-c-host';shim=evidence/'io-faults.so';input=args.input.resolve()
    library=args.provider.resolve();duckdb=ROOT/'.reference/opengauss-native-lake/build/duckdb-gcc10/duckdb'
    receipt={'status':'failed','scope':'standalone real local candidate I/O and lifetime faults; not database crash recovery',
             'started_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),
             'provider_sources_before':inputs(),'dependencies_before':dependencies(),'library_sha256':sha(library),'input_sha256':sha(input),
             'reference_binary_sha256':sha(duckdb),'test_sha256':sha(test),'faults_sha256':sha(faults),
             'common_test_sha256':sha(test.with_name('provider_reader_test.c')),'driver_sha256':sha(__file__)}
    env=os.environ.copy();support=[GCC/'lib64']+[GCC.parent/p/'lib' for p in ('isl','mpc','mpfr','gmp')]
    env['LD_LIBRARY_PATH']=':'.join(map(str,[PREFIX/'lib']+support))
    def command(name,argv,environment=env):
        with (evidence/(name+'.log')).open('w') as log:
            subprocess.run(list(map(str,argv)),env=environment,stdout=log,stderr=subprocess.STDOUT,check=True)
    try:
        flags=[GCC/'bin/gcc','-std=c11','-O2','-Wall','-Wextra','-Werror']
        command('compile-shim',flags+['-fPIC','-shared',faults,'-ldl','-o',shim])
        command('compile',flags+['-I'+str(SOURCE),'-I'+str(PREFIX/'include'),test,'-ldl','-o',consumer])
        runtime=env.copy();runtime['LD_PRELOAD']=str(shim)
        command('faults',[consumer,library,input,evidence],runtime)
        result=json.loads((evidence/'faults.log').read_text())
        output=evidence/'borrowed.parquet'
        literal="'"+str(output).replace("'","''")+"'"
        expected="SELECT * FROM (VALUES (0, true, '-9223372036854775808'::BIGINT, '湖/borrow'), (1, NULL, '9223372036854775807'::BIGINT, ''), (2, false, NULL, NULL)) q(id,flag,big,txt)"
        actual='SELECT * FROM read_parquet('+literal+')'
        sql='SELECT count(*) AS differences FROM (('+actual+' EXCEPT ALL '+expected+') UNION ALL ('+expected+' EXCEPT ALL '+actual+'));'
        (evidence/'reference.sql').write_text(sql)
        command('reference',[duckdb,'-no-agent','-csv','-no-stdin','-c',sql])
        lines=(evidence/'reference.log').read_text().splitlines()
        if lines!=['differences','0']:raise RuntimeError('borrowed input changed after Append: '+repr(lines))
        receipt['provider_sources_after']=inputs()
        receipt['dependencies_after']=dependencies()
        if receipt['provider_sources_after']!=receipt['provider_sources_before'] or receipt['dependencies_before']!=receipt['dependencies_after'] or sha(library)!=receipt['library_sha256'] or sha(input)!=receipt['input_sha256']:
            raise RuntimeError('provider inputs changed during fault test')
        receipt.update(status='passed',faults=result,borrowed_reference_differences=0,
                       output_sha256=sha(output),consumer_sha256=sha(consumer),shim_sha256=sha(shim))
        print(json.dumps(result),flush=True);return 0
    except Exception as exc:
        receipt['error']=str(exc);print(str(exc),flush=True);return 1
    finally:(evidence/'result.json').write_text(json.dumps(receipt,indent=2)+'\n')

if __name__=='__main__':raise SystemExit(main())
