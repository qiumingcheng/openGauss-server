#!/usr/bin/env python3
"""Observe genuine pinned DuckLake flush identity/history/metadata, no row repairs."""
import argparse
from collections import Counter
import json
from pathlib import Path
import shutil
import sys
import traceback
ROOT=Path('/home/qiumc/data/code/cpp');DRIVER=ROOT/'integration/opengauss-native-lake'
sys.path[:0]=[str(DRIVER),str(DRIVER/'tests')]
from lake_harness import sha256,write_json
from reverse_reference import Reference,literal,qi

def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--run-id',required=True);a=p.parse_args()
 out=ROOT/'公共文档/openGauss原生湖存储/验证'/a.run_id;out.mkdir(exist_ok=False)
 result={'status':'failed','scope':'Independent pinned DuckLake history/identity flush oracle; not native OG flush','assertions':[]}
 def check(name,actual,expected):
  ok=actual==expected;result['assertions'].append({'name':name,'observed':actual,'expected':expected,'status':'pass' if ok else 'fail'})
  if not ok:raise AssertionError(name)
 try:
  lock=DRIVER/'source-lock.json';receipt=ROOT/'.reference/opengauss-native-lake/build/receipt.json'
  l=json.loads(lock.read_text());b=json.loads(receipt.read_text());check('reference matches exact source lock',b['source_lock_hash'],sha256(lock))
  ref=Reference(out,l,b);result['reference_binary_sha256']=sha256(ref.binary)
  shutil.copy2(__file__,out/'executed-test.py');result['script_sha256']=sha256(__file__)
  catalog=out/'catalog.ducklake';data=out/'data'
  ref.run('ATTACH '+literal('ducklake:'+str(catalog))+" AS lake (DATA_PATH "+literal(str(data))+",DUCKLAKE_VERSION '1.1',AUTOMATIC_MIGRATION FALSE,DATA_INLINING_ROW_LIMIT 1000000);CREATE TABLE lake.t(id INTEGER,val INTEGER,note VARCHAR)",'initialize')
  def metadata(sql,name):return ref.run(sql,name,catalog,True,True)
  def logical(name,snapshot=None):
   suffix='' if snapshot is None else ' AT (VERSION => '+str(snapshot)+')'
   return ref.run(ref.attach(catalog,True)+'SELECT rowid AS rid,snapshot_id AS birth,id,val,note FROM lake.t'+suffix+' ORDER BY id',name,result=True)
  history=[]
  for i,statement in enumerate(["INSERT INTO lake.t VALUES(1,10,'a'),(2,20,NULL),(3,30,'湖')", "UPDATE lake.t SET val=11,note='b' WHERE id=1", "UPDATE lake.t SET val=12,note='c' WHERE id=1", 'DELETE FROM lake.t WHERE id=2']):
   ref.run(ref.attach(catalog)+statement,'dml-'+str(i));sid=metadata('SELECT max(snapshot_id) AS sid FROM ducklake_snapshot','snapshot-'+str(i))[0]['sid'];history.append((sid,logical('logical-'+str(i))))
  registrations=metadata('SELECT * FROM ducklake_inlined_data_tables','registrations-before');check('one actual inline registration',len(registrations),1)
  raw=metadata('SELECT * FROM '+qi(registrations[0]['table_name'])+' ORDER BY _ducklake_row_id,_ducklake_begin_snapshot','versions-before')
  check('raw inline retains original plus two updated versions',len(raw),5)
  oldstats=metadata('SELECT * FROM ducklake_table_stats','global-stats-before');before=logical('logical-before')
  ref.run(ref.attach(catalog)+"CALL ducklake_flush_inlined_data('lake')",'flush')
  after=logical('logical-after');check('logical identity birth and complete values stable across flush',after,before)
  check('final live payload has no stale versions',[(x['id'],x['val'],x['note']) for x in after],[(1,12,'c'),(3,30,'湖')])
  for sid,expected in history:check('history remains readable at snapshot '+str(sid),logical('history-after-'+str(sid),sid),expected)
  check('flushed inline rows physically removed',metadata('SELECT count(*) AS n FROM '+qi(registrations[0]['table_name']),'inline-after'),[{'n':0}])
  files=metadata('SELECT * FROM ducklake_data_file WHERE end_snapshot IS NULL','data-files-after');check('one actual flush data file',len(files),1);f=files[0]
  check('file records physical versions including ended versions',f['record_count'],len(raw))
  check('partial file starts at earliest input birth',f['begin_snapshot'],min(x['_ducklake_begin_snapshot'] for x in raw))
  check('partial maximum is last input birth',f['partial_max'],max(x['_ducklake_begin_snapshot'] for x in raw))
  file=list(data.rglob(f['path']));check('actual registered immutable data path exists once',len(file),1);file=file[0]
  check('catalog file bytes exact',f['file_size_bytes'],file.stat().st_size)
  with file.open('rb') as stream:stream.seek(-8,2);tail=stream.read(8)
  check('actual output parquet magic',tail[4:].decode(),'PAR1');check('catalog footer excludes trailing 8 bytes',f['footer_size'],int.from_bytes(tail[:4],'little'))
  physical=ref.run('SELECT file_row_number,* FROM read_parquet('+literal(str(file))+',file_row_number=true) ORDER BY file_row_number','physical-output',result=True)
  observed=Counter(tuple(x[k] for k in ['_ducklake_internal_row_id','_ducklake_internal_snapshot_id','id','val','note']) for x in physical)
  expected=Counter(tuple(x[k] for k in ['_ducklake_row_id','_ducklake_begin_snapshot','id','val','note']) for x in raw)
  check('every historical version retains logical identity and birth in Parquet',sorted(map(repr,observed.items())),sorted(map(repr,expected.items())))
  deletes=metadata('SELECT * FROM ducklake_delete_file WHERE end_snapshot IS NULL','deletes-after');check('one actual flush position-delete file',len(deletes),1);d=deletes[0]
  check('three ended versions have position deletions',d['delete_count'],3)
  paths=list(data.rglob(d['path']));check('registered delete path exists once',len(paths),1)
  marks=ref.run('SELECT * FROM read_parquet('+literal(str(paths[0]))+')','position-marks',result=True)
  # Bind real output positions, never assume scan order or derive from logical ID.
  by_version={(x['_ducklake_internal_row_id'],x['_ducklake_internal_snapshot_id']):x['file_row_number'] for x in physical}
  expected_marks=sorted((by_version[(x['_ducklake_row_id'],x['_ducklake_begin_snapshot'])],x['_ducklake_end_snapshot']) for x in raw if x['_ducklake_end_snapshot'] is not None)
  check('position deletes retain each original end snapshot',sorted((x['pos'],x['_ducklake_internal_snapshot_id']) for x in marks),expected_marks)
  stats=metadata('SELECT * FROM ducklake_table_stats','global-stats-after');check('flush does not recount logical record_count',stats[0]['record_count'],oldstats[0]['record_count']);check('flush never allocates replacement logical IDs',stats[0]['next_row_id'],oldstats[0]['next_row_id'])
  check('global file bytes counts data files',stats[0]['file_size_bytes'],oldstats[0]['file_size_bytes']+f['file_size_bytes'])
  result.update(status='passed',history=history,logical_before=before,logical_after=after,data_file=f,delete_file=d)
 except BaseException as exc:result.update(reason=str(exc),traceback=traceback.format_exc())
 finally:write_json(out/'result.json',result)
 print(json.dumps({'status':result['status'],'reason':result.get('reason'),'checks':len(result['assertions']),'result':str(out/'result.json')}))
 return 0 if result['status']=='passed' else 1
if __name__=='__main__':raise SystemExit(main())
