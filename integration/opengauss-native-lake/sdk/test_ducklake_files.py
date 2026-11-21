#!/usr/bin/env python3
"""Read genuine DuckLake flushed Parquet and compare every physical field."""
import argparse,datetime,json,os,subprocess
from pathlib import Path
from build_provider import ROOT,PREFIX,GCC,SOURCE,sha,inputs,dependencies

def main():
    parser=argparse.ArgumentParser();parser.add_argument('--evidence',type=Path,required=True)
    args=parser.parse_args();evidence=args.evidence.resolve();evidence.mkdir(parents=True,exist_ok=False)
    duckdb=ROOT/'.reference/opengauss-native-lake/build/duckdb-gcc10/duckdb'
    library=PREFIX/'lib/libog_lake_sdk_arrow.so';test=Path(__file__).with_name('provider_dump_test.c');exe=evidence/'dump-c-host'
    receipt={'status':'failed','scope':'physical DuckLake-produced file fields and original positions; not table visibility or native flush',
             'started_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),
             'provider_sources_before':inputs(),'dependencies_before':dependencies(),'library_sha256':sha(library),
             'reference_binary_sha256':sha(duckdb),'test_sha256':sha(test),
             'common_test_sha256':sha(test.with_name('provider_reader_test.c')),'driver_sha256':sha(__file__)}
    env=os.environ.copy();support=[GCC/'lib64']+[GCC.parent/p/'lib' for p in ('isl','mpc','mpfr','gmp')]
    env['LD_LIBRARY_PATH']=':'.join(map(str,[PREFIX/'lib']+support))
    def literal(value):return "'"+str(value).replace("'","''")+"'"
    def command(name,argv):
        with (evidence/(name+'.log')).open('w') as log:
            subprocess.run(list(map(str,argv)),env=env,stdout=log,stderr=subprocess.STDOUT,check=True)
    try:
        catalog=evidence/'catalog.ducklake';data=evidence/'data'
        sql='ATTACH '+literal('ducklake:'+str(catalog))+" AS lake (DATA_PATH "+literal(data)+", DUCKLAKE_VERSION '1.1', AUTOMATIC_MIGRATION FALSE, DATA_INLINING_ROW_LIMIT 100);\n"
        sql+='CREATE TABLE lake.orders(id INTEGER,val INTEGER); INSERT INTO lake.orders VALUES(1,10),(2,20),(3,30);\n'
        sql+='UPDATE lake.orders SET val=11 WHERE id=1; UPDATE lake.orders SET val=12 WHERE id=1;\n'
        sql+="CALL ducklake_flush_inlined_data('lake'); SELECT rowid,snapshot_id,* FROM lake.orders ORDER BY id;"
        (evidence/'fixture.sql').write_text(sql);command('fixture',[duckdb,'-no-agent','-json','-no-stdin','-c',sql])
        files=sorted(data.rglob('*.parquet'))
        if not files:raise RuntimeError('normal reference flush did not produce Parquet')
        command('compile',[GCC/'bin/gcc','-std=c11','-O2','-Wall','-Wextra','-Werror',
            '-I'+str(SOURCE),'-I'+str(PREFIX/'include'),test,'-ldl','-o',exe])
        receipt['files']=[];hidden_seen=set()
        for index,path in enumerate(files):
            before=sha(path);command('sdk-'+str(index),[exe,library,path])
            actual=json.loads((evidence/('sdk-'+str(index)+'.log')).read_text())
            sql='SELECT * FROM read_parquet('+literal(path)+');'
            (evidence/('reference-'+str(index)+'.sql')).write_text(sql)
            command('reference-'+str(index),[duckdb,'-no-agent','-json','-no-stdin','-c',sql])
            expected=json.loads((evidence/('reference-'+str(index)+'.log')).read_text())
            names=[f['name'] for f in actual['fields']]
            expected_rows=[[row[name] for name in names] for row in expected]
            if actual['rows']!=expected_rows or before!=sha(path):raise RuntimeError('physical Parquet values or immutability differ')
            hidden_seen.update(f['id'] for f in actual['fields'] if f['id'] in (2147483540,2147483539))
            receipt['files'].append({'path':str(path),'sha256':before,'sdk':actual,'differences':0})
        if hidden_seen!={2147483540,2147483539}:raise RuntimeError('real flush did not exercise both standard hidden field IDs')
        receipt['provider_sources_after']=inputs();receipt['dependencies_after']=dependencies()
        if receipt['provider_sources_after']!=receipt['provider_sources_before'] or receipt['dependencies_before']!=receipt['dependencies_after'] or sha(library)!=receipt['library_sha256']:
            raise RuntimeError('provider inputs changed during real flush file verification')
        receipt.update(status='passed',catalog_sha256=sha(catalog),consumer_sha256=sha(exe))
        print(json.dumps({'status':'passed','files':len(files),'hidden_fields':sorted(hidden_seen)}));return 0
    except Exception as exc:
        receipt['error']=str(exc);print(str(exc),flush=True);return 1
    finally:(evidence/'result.json').write_text(json.dumps(receipt,indent=2)+'\n')

if __name__=='__main__':raise SystemExit(main())
