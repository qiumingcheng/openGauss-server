#!/usr/bin/env python3
"""Existing native DiskANN expression-index viability on an ordinary TEXT heap.

This tests reuse against an exactly identified already-built binary while the
next lake candidate builds. It does not implement or qualify lake ANN access.
"""
import argparse
import json
from pathlib import Path
import shutil
import sys
import traceback
ROOT=Path('/home/qiumc/data/code/cpp');D=ROOT/'integration/opengauss-native-lake'
sys.path[:0]=[str(D),str(D/'tests')]
from inline_fixture import query,scalar
from lake_harness import sha256,write_json,validate_run_id
from native_test_context import OwnedDatabase
from native_runtime import RUNTIME,INSTALL


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--run-id',required=True)
    p.add_argument('--binary-receipt',type=Path,required=True)
    p.add_argument('--conversion',choices=['direct','native-array'],default='direct')
    a=p.parse_args();validate_run_id(a.run_id)
    out=ROOT/'公共文档/openGauss原生湖存储/验证'/a.run_id;out.mkdir(exist_ok=False);db=None
    files=[Path(__file__),D/'native_test_context.py',D/'native_runtime.py',D/'inline_fixture.py',D/'tests/inline_cleanup.py']
    archive=out/'executed-inputs';archive.mkdir();r={'status':'fail','scope':__doc__,'assertions':[],
        'inputs':{str(f):sha256(f)for f in files},'binary_receipt':str(a.binary_receipt.resolve()),'binary_receipt_sha256':sha256(a.binary_receipt)}
    for f in files:shutil.copy2(f,archive/f.name)
    def check(name,got,want=True):
        good=got==want;r['assertions'].append(dict(name=name,observed=got,expected=want,status='pass'if good else'fail'))
        if not good:raise AssertionError(name)
    try:
        b=json.loads(a.binary_receipt.read_text());pid=int((RUNTIME/'data/postmaster.pid').read_text().splitlines()[0])
        check('official baseline build',b['status'],'build_and_sql_passed')
        check('exact baseline installed binary',sha256(INSTALL/'bin/gaussdb'),b['binary_sha256'])
        check('exact baseline running binary',sha256(Path('/proc')/str(pid)/'exe'),b['binary_sha256'])
        r.update(source_head=b['source_head'],binary_sha256=b['binary_sha256'])
        db=OwnedDatabase(a.run_id,out).create();c=db.connect();query(c,'SET statement_timeout=45000')
        expression="v::vector(3)"if a.conversion=='direct'else"(pg_catalog.string_to_array(pg_catalog.btrim(v,'[]'),',')::real[])::vector(3)"
        r['conversion']=a.conversion;r['expression']=expression
        statements=["CREATE TABLE physical_vectors(id integer,_ducklake_row_id bigint,_ducklake_begin_snapshot bigint,_ducklake_end_snapshot bigint,v text)",
            "INSERT INTO physical_vectors SELECT i,i::bigint,1::bigint,NULL::bigint,'['||i::text||','||(i%13)::text||','||(i%17)::text||']' FROM generate_series(1,2048)i",
            "CREATE INDEX reused_diskann ON physical_vectors USING diskann (("+expression+") vector_l2_ops) WITH(enable_pq=off)"]
        for n,statement in enumerate(statements):
            (out/('statement-'+str(n)+'.sql')).write_text(statement+';\n');query(c,statement)
        check('index expression retains native vector typmod',query(c,"SELECT a.atttypid,a.atttypmod FROM pg_attribute a WHERE a.attrelid='reused_diskann'::regclass AND a.attnum=1"),[(8305,3)])
        statement="SELECT id,_ducklake_row_id,_ducklake_begin_snapshot,("+expression+" <-> '[500,3,7]'::vector) AS distance FROM physical_vectors ORDER BY "+expression+" <-> '[500,3,7]'::vector LIMIT 10"
        query(c,'SET enable_indexscan=off');exact=query(c,statement);query(c,'SET enable_indexscan=on');query(c,'SET enable_seqscan=off')
        plan='\n'.join(x[0]for x in query(c,'EXPLAIN (ANALYZE, COSTS OFF) '+statement));(out/'native-plan.txt').write_text(plan+'\n')
        check('actual native expression DiskANN path selected','reused_diskann'in plan and'Index Scan'in plan)
        rows=query(c,statement);check('native returns ten candidates',len(rows),10)
        check('heap TIDs resolve original logical identity/version',all(identity==rowid and birth==1 for identity,rowid,birth,distance in rows))
        check('returned exact SQL distances are ordered',[v[3]for v in rows],sorted(v[3]for v in rows))
        r['native_candidates']=rows;r['exact_top_ten']=exact
        r['observed_recall_at_10']=len({v[0]for v in rows}&{v[0]for v in exact})/10
        check('executed inputs unchanged',all(sha256(f)==h for f,h in r['inputs'].items()));r['status']='pass'
    except BaseException as exc:r.update(reason=str(exc),traceback=traceback.format_exc())
    finally:
        if db is not None:
            try:db.cleanup();r['database_cleanup']=db.cleanup_result;r['cleanup']='pass'
            except BaseException as exc:r.update(status='fail',cleanup='fail',cleanup_error=str(exc))
        write_json(out/'result.json',r)
    print(json.dumps(dict(status=r['status'],assertions=len(r['assertions']),reason=r.get('reason'),result=str(out/'result.json')),ensure_ascii=False))
    return 0 if r['status']=='pass'else 1

if __name__=='__main__':raise SystemExit(main())
