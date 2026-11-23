#!/usr/bin/env python3
"""Independent protocol oracle for the next explicit-ID/delete-reader slice.

This consumes actual C ABI physical rows and actual DuckLake historical reads.
It is not a native0.8 visibility implementation or an acceptance promotion.
"""
import argparse,datetime,json,os,subprocess
from pathlib import Path
from build_provider import ROOT,PREFIX,GCC,sha

def main():
    parser=argparse.ArgumentParser();parser.add_argument('--fixture',type=Path,required=True)
    parser.add_argument('--evidence',type=Path,required=True);args=parser.parse_args()
    fixture=args.fixture.resolve();evidence=args.evidence.resolve();evidence.mkdir(parents=True,exist_ok=False)
    source=json.loads((fixture/'result.json').read_text());catalog=fixture/'catalog.ducklake'
    if source['status']!='passed':raise RuntimeError('requires actual successful DuckLake file fixture')
    library=PREFIX/'lib/libog_lake_sdk_arrow.so';consumer=fixture/'dump-c-host'
    duckdb=ROOT/'.reference/opengauss-native-lake/build/duckdb-gcc10/duckdb'
    result={'status':'failed','scope':'independent actual-file protocol oracle only; native0.8 still rejects this layout',
            'driver_sha256':sha(__file__),'library_sha256':sha(library),'reference_binary_sha256':sha(duckdb),
            'catalog_sha256_before':sha(catalog),'fixture_result_sha256':sha(fixture/'result.json'),'snapshots':[]}
    env=os.environ.copy();env['LD_LIBRARY_PATH']=':'.join(map(str,[PREFIX/'lib',GCC/'lib64']))
    def literal(value):return "'"+str(value).replace("'","''")+"'"
    def run(name,argv):
        process=subprocess.run(list(map(str,argv)),env=env,text=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        (evidence/(name+'.stdout')).write_text(process.stdout);(evidence/(name+'.stderr')).write_text(process.stderr)
        if process.returncode:raise RuntimeError(name+' command failed')
        return json.loads(process.stdout)
    try:
        physical=[]
        for index,entry in enumerate(source['files']):
            path=Path(entry['path']);data=run('physical-'+str(index),[consumer,library,path])
            if sha(path)!=entry['sha256']:raise RuntimeError('actual Parquet fixture changed')
            ids=[field['id'] for field in data['fields']]
            physical.append({'path':str(path),'ids':ids,'rows':[dict(zip(ids,row)) for row in data['rows']]})
        data_files=[f for f in physical if 2147483540 in f['ids']]
        deletes=[f for f in physical if 2147483545 in f['ids']]
        if len(data_files)!=1 or len(deletes)!=1:raise RuntimeError('oracle requires one real versioned data and delete file')
        data=data_files[0];delete_by_position={}
        for row in deletes[0]['rows']:
            if row[2147483546]!=data['path']:raise RuntimeError('delete references a different actual data file')
            position,snapshot=row[2147483545],row[2147483539]
            if position<0 or position>=len(data['rows']):raise RuntimeError('actual deletion position exceeds its data file')
            delete_by_position[position]=min(delete_by_position.get(position,snapshot),snapshot)
        attach='ATTACH '+literal('ducklake:'+str(catalog))+" AS lake (DUCKLAKE_VERSION '1.1',AUTOMATIC_MIGRATION FALSE,READ_ONLY);\n"
        for snapshot in [2,3,4,5]:
            model=[]
            for position,row in enumerate(data['rows']):
                if row[2147483539]>snapshot:continue
                if position in delete_by_position and delete_by_position[position]<=snapshot:continue
                model.append({'logical_rowid':row[2147483540],'begin_snapshot':row[2147483539],'id':row[1],'val':row[2]})
            model.sort(key=lambda row:row['id'])
            sql=attach+'SELECT rowid AS logical_rowid,snapshot_id AS begin_snapshot,* FROM lake.orders AT (VERSION => '+str(snapshot)+') ORDER BY id;'
            (evidence/('snapshot-'+str(snapshot)+'.sql')).write_text(sql)
            observed=run('snapshot-'+str(snapshot),[duckdb,'-no-agent','-json','-no-stdin','-c',sql])
            if model!=observed:raise RuntimeError('standard position/snapshot visibility differs at '+str(snapshot))
            result['snapshots'].append({'snapshot':snapshot,'model':model,'actual_reference':observed})
        if sha(catalog)!=result['catalog_sha256_before']:raise RuntimeError('readonly historical reads mutated the actual catalog')
        result.update(status='passed',earliest_deletes_by_position=delete_by_position,
                      explicit_ids=[row[2147483540] for row in data['rows']],physical_positions=list(range(len(data['rows']))))
        print('four_actual_snapshot_visibility_checkpoints_passed',flush=True);return 0
    except Exception as exc:result['error']=str(exc);print(str(exc),flush=True);return 1
    finally:(evidence/'result.json').write_text(json.dumps(result,indent=2)+'\n')

if __name__=='__main__':raise SystemExit(main())
