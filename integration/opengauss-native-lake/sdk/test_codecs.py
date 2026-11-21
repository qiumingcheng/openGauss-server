#!/usr/bin/env python3
"""Exercise every advertised compression value and independent file readback."""
import argparse,datetime,json,os,subprocess
from pathlib import Path
from build_provider import ROOT,PREFIX,GCC,SOURCE,sha,inputs,dependencies

def main():
    parser=argparse.ArgumentParser();parser.add_argument('--evidence',type=Path,required=True);parser.add_argument('--input',type=Path,required=True)
    args=parser.parse_args();evidence=args.evidence.resolve();evidence.mkdir(parents=True,exist_ok=False)
    library=PREFIX/'lib/libog_lake_sdk_arrow.so';test=Path(__file__).with_name('provider_codec_test.c');exe=evidence/'codec-c-host';input=args.input.resolve()
    duckdb=ROOT/'.reference/opengauss-native-lake/build/duckdb-gcc10/duckdb'
    receipt={'status':'failed','scope':'standalone advertised codecs only','library_sha256':sha(library),
             'sources_before':inputs(),'dependencies_before':dependencies(),'test_sha256':sha(test),
             'common_test_sha256':sha(test.with_name('provider_reader_test.c')),'driver_sha256':sha(__file__),
             'input_sha256':sha(input),'reference_binary_sha256':sha(duckdb),'cases':[]}
    env=os.environ.copy();support=[GCC/'lib64']+[GCC.parent/p/'lib' for p in ('isl','mpc','mpfr','gmp')]
    env['LD_LIBRARY_PATH']=':'.join(map(str,[PREFIX/'lib']+support))
    def command(name,argv):
        with (evidence/(name+'.log')).open('w') as log:subprocess.run(list(map(str,argv)),env=env,stdout=log,stderr=subprocess.STDOUT,check=True)
    def literal(path):return "'"+str(path).replace("'","''")+"'"
    try:
        command('compile',[GCC/'bin/gcc','-std=c11','-O2','-Wall','-Wextra','-Werror','-I'+str(SOURCE),'-I'+str(PREFIX/'include'),test,'-ldl','-o',exe])
        for codec,name in enumerate(['uncompressed','zstd','snappy','gzip','lz4']):
            output=evidence/(name+'.parquet');command(name,[exe,library,input,output,str(codec)])
            sdk=json.loads((evidence/(name+'.log')).read_text());actual='SELECT * FROM read_parquet('+literal(output)+')';expected='SELECT * FROM read_parquet('+literal(input)+')'
            sql='SELECT count(*) AS differences FROM (('+actual+' EXCEPT ALL '+expected+') UNION ALL ('+expected+' EXCEPT ALL '+actual+'));'
            (evidence/(name+'.sql')).write_text(sql);command(name+'-reference',[duckdb,'-no-agent','-csv','-no-stdin','-c',sql])
            if (evidence/(name+'-reference.log')).read_text().splitlines()!=['differences','0']:raise RuntimeError(name+' independent readback differs')
            receipt['cases'].append({'codec':name,'result':sdk,'output_sha256':sha(output),'independent_differences':0})
        receipt['sources_after']=inputs();receipt['dependencies_after']=dependencies()
        if receipt['sources_before']!=receipt['sources_after'] or receipt['dependencies_before']!=receipt['dependencies_after'] or sha(library)!=receipt['library_sha256'] or sha(input)!=receipt['input_sha256']:
            raise RuntimeError('codec inputs changed')
        receipt['status']='passed';print('five_real_codecs_passed',flush=True);return 0
    except Exception as exc:receipt['error']=str(exc);print(str(exc),flush=True);return 1
    finally:(evidence/'result.json').write_text(json.dumps(receipt,indent=2)+'\n')

if __name__=='__main__':raise SystemExit(main())
