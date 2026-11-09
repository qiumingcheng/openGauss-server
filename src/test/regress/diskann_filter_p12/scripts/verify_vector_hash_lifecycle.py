#!/usr/bin/env python3
"""Check complete relational bags through controlled Hash Join spill and reuse."""
import argparse,json,re
from collections import Counter
from pathlib import Path
from verify_query_protocol import connect

def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--output',type=Path,required=True)
    a=p.parse_args();a.output.mkdir(parents=True,exist_ok=False)
    conn=connect();cur=conn.cursor();commands=[];checks=[]
    def sql(q):
        cur.execute(q);rows=cur.fetchall() if cur.description else []
        commands.append(dict(sql=q,rows=rows));return rows
    def check(name,ok,details):
        checks.append(dict(name=name,result='PASS' if ok else 'FAIL',details=details))
        if not ok:print('FAIL '+name+': '+str(details),flush=True)
    def run(label,q,expected):
        rows=sql(q);text='\n'.join(r[0] for r in sql('EXPLAIN(ANALYZE,BUFFERS,COSTS OFF,TIMING OFF) '+q))+'\n'
        (a.output/(label+'.plan.txt')).write_text(text)
        check(label+'_complete_bag',Counter(rows)==Counter(expected),dict(actual_rows=len(rows),expected_rows=len(expected)))
        check(label+'_ordinary_hash',bool(re.search(r'Hash (?:Left |Right |Full )?Join',text)) and 'Ann Index Scan' not in text,text)
        return rows,text
    try:
        sql("SET enable_opfusion=off;SET diskann_filter_mode=exact;SET query_dop=1;SET enable_nestloop=off;SET enable_mergejoin=off;SET enable_indexscan=off;SET enable_bitmapscan=off;SET statement_timeout=30000")
        sql("CREATE TEMP TABLE vf_hash_matches(bucket int,n int,payload text) WITH(storage_type=astore)")
        sql("INSERT INTO vf_hash_matches SELECT b,n,repeat('x',1024) FROM generate_series(0,31)b CROSS JOIN generate_series(1,64)n")
        sql('INSERT INTO vf_hash_matches VALUES(NULL,65,NULL)');sql('ANALYZE vf_hash_matches')
        docs=sql('SELECT id,bucket,tenant FROM vf_join_registration.documents ORDER BY id')
        sql("CREATE TEMP TABLE vf_hash_documents WITH(storage_type=astore) AS SELECT d.id+x.n*256 AS id,d.bucket,d.embedding,repeat('y',1024) AS payload FROM vf_join_registration.documents d CROSS JOIN generate_series(0,3)x(n)")
        sql('ANALYZE vf_hash_documents')
        order=" ORDER BY d.embedding <-> '[7.25,1,2,3,4,5,6,7]'::vector LIMIT 100000"
        q='SELECT d.id,a.n,substring(a.payload FROM 1 FOR d.id%2+1),substring(d.payload FROM 1 FOR a.n%2+1) FROM vf_hash_documents d FULL JOIN vf_hash_matches a ON d.bucket=a.bucket'+order
        expected=[(i+x*256,n,'x'*(i%2+1),'y'*(n%2+1)) for x in range(4) for i,b,t in docs for n in range(1,65)]
        expected.extend((None,n,None,None) for b in range(16,32) for n in range(1,65));expected.append((None,65,None,None))
        sql("SET work_mem='64kB';SET enable_diskann_filter=off")
        reference,off=run('spill_control_off',q,expected)
        sql('SET enable_diskann_filter=on')
        rows,on=run('spill_control_on',q,expected)
        check('spill_control_on_off_same_bag',Counter(rows)==Counter(reference),dict(rows=len(rows)))
        batches=[int(x) for x in re.findall(r'\bBatches:\s*(\d+)',on)]
        check('spill_actual_hash_batches',bool(batches) and max(batches)>1 and 'temp read=' in on,on)
        sql("SET work_mem='4MB';SET enable_nestloop=on;SET enable_material=off")
        sql('CREATE TEMP TABLE vf_hash_reuse AS SELECT bucket,n FROM vf_hash_matches WHERE bucket<4 AND n<=2');sql('ANALYZE vf_hash_reuse')
        nested="SELECT q.id,q.n,x.t FROM (VALUES(1),(3),(1))x(t) CROSS JOIN LATERAL (SELECT d.id,a.n,d.embedding FROM vf_join_registration.documents d LEFT JOIN vf_hash_reuse a ON d.bucket=a.bucket WHERE d.tenant=x.t OFFSET 0)q ORDER BY q.embedding <-> '[7.25,1,2,3,4,5,6,7]'::vector LIMIT 100000"
        expected_nested=[(i,n,t) for t in (1,3,1) for i,b,tenant in docs if tenant==t for n in (range(1,3) if b<4 else (None,))]
        rows,reuse=run('rescan_reuse',nested,expected_nested)
        check('rescan_actual_hash_three_loops',bool(re.search(r'Hash Left Join.*loops=3',reuse)),reuse)
        check('rescan_actual_single_business_build',bool(re.search(r'\bHash\s+\(.*loops=1',reuse)),reuse)
        single=Counter((i,n) for i,b,tenant in docs if tenant==1 for n in (range(1,3) if b<4 else (None,)))
        repeated=Counter((i,n) for i,n,t in rows if t==1)
        check('equal_outer_rows_keep_multiplicity',repeated==Counter({key:2*count for key,count in single.items()}),dict(actual=dict((str(k),v) for k,v in repeated.items()),single=dict((str(k),v) for k,v in single.items())))
        # Unrelated scalar sorting and feature-off queries retain their own
        # ordinary bag. This tests the runtime guard without requiring a
        # particular unspecified tie order.
        scalar=q.replace("d.embedding <-> '[7.25,1,2,3,4,5,6,7]'::vector",'d.id')
        run('scalar_sort_control',scalar,expected)
    finally:
        result=dict(passed=sum(c['result']=='PASS' for c in checks),total=len(checks),checks=checks)
        (a.output/'summary.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
        (a.output/'commands.json').write_text(json.dumps(commands,ensure_ascii=False,indent=2,default=str)+'\n')
        cur.close();conn.close()
    failed=[c['name'] for c in checks if c['result']!='PASS']
    print(json.dumps(dict(result='FAIL' if failed else 'PASS',checks=len(checks),failed=failed)))
    if failed:raise SystemExit(1)

if __name__=='__main__':main()
