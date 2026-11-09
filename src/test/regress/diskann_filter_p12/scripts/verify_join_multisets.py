#!/usr/bin/env python3
"""Preserve complete business matches, projections and final row K across modes."""
import argparse,json,re
from collections import Counter
from pathlib import Path
from join_multiset_cases import CASES,native_query
from verify_query_protocol import connect

def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--oracle',type=Path,required=True)
    p.add_argument('--output',type=Path,required=True);a=p.parse_args();a.output.mkdir(parents=True,exist_ok=False)
    evidence=json.loads(a.oracle.read_text());conn=connect();cur=conn.cursor();commands=[];checks=[]
    def sql(q):
        cur.execute(q);rows=cur.fetchall() if cur.description else []
        commands.append(dict(sql=q,rows=rows));return rows
    def check(name,ok,details):
        checks.append(dict(name=name,result='PASS' if ok else 'FAIL',details=details))
        if not ok:print('FAIL '+name+': '+str(details),flush=True)
    try:
        sql('SET enable_diskann_filter=on;SET enable_opfusion=off;SET query_dop=1;SET diskann_probes=2048;SET statement_timeout=30000')
        sql('BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY')
        for case in CASES:
            name=case['name'];q=native_query(case)
            sql("SET diskann_filter_mode=exact");expected=sql(q)
            oracle_exact=next(r for r in evidence if r['case']==name and r['mode']=='exact')
            check(name+'_ordinary_oracle_bag',Counter(expected)==Counter(map(tuple,oracle_exact['rows'])),dict(native=expected,oracle=oracle_exact['rows']))
            for mode in ('pre','in','post'):
                sql("SET diskann_filter_mode='"+mode+"'");rows=sql(q)
                text='\n'.join(r[0] for r in sql('EXPLAIN(ANALYZE,COSTS OFF,TIMING OFF) '+q))+'\n'
                (a.output/(name+'_'+mode+'.plan.txt')).write_text(text)
                oracle=next(r for r in evidence if r['case']==name and r['mode']==('in_filter' if mode=='in' else mode))
                check(name+'_'+mode+'_original_oracle_bag',Counter(rows)==Counter(map(tuple,oracle['rows'])),dict(native=rows,oracle=oracle['rows']))
                check(name+'_'+mode+'_original_target_order',[r[0] for r in rows]==[r[0] for r in oracle['rows']],dict(native=rows,oracle=oracle['rows']))
                # Native CBO may select a Merge Join for a sorted business key.
                # Require the real output join above the ANN child, independent
                # of which ordinary join algorithm won the cost comparison.
                ann_lines=[i for i,line in enumerate(text.splitlines()) if 'Ann Index Scan using' in line]
                joins=[i for i,line in enumerate(text.splitlines()) if re.search(r'\b(?:Hash Join|Merge Join|Nested Loop)\b',line)]
                check(name+'_'+mode+'_real_output_join',len(ann_lines)==1 and any(i<ann_lines[0] for i in joins) and 'Executed Mode: '+mode.upper() in text and 'Custom Scan' not in text,text)
                # A single distance key and the original user LIMIT remain
                # visible; no target DISTINCT, Unique or hidden inner limit.
                check(name+'_'+mode+'_global_row_limit',len(rows)==min(case['limit'],len(expected)) and sum(bool(re.match(r'^\s*(?:->\s*)?Limit\b',line)) for line in text.splitlines())==1 and 'Unique' not in text,text)
                if name=='four_matches':check(mode+'_one_target_exactly_four_business_rows',Counter(rows)==Counter((31626,g) for g in (1,3,4,5)),rows)
                if name=='row_limit_two':check(mode+'_k_counts_output_rows',rows==[(31626,),(31626,)],rows)
                if name=='many_to_many':check(mode+'_sixteen_real_business_pairs',Counter(rows)==Counter((31626,g,h) for g in (1,3,4,5) for h in (1,3,4,5)),rows)
                if name=='target_projection':check(mode+'_target_projection_keeps_duplicates',Counter(r[0] for r in rows)==Counter({31626:4,33286:2,48626:4}),rows)
                check(name+'_'+mode+'_oracle_vector_observed',oracle['vector_transformed'],oracle['observed_vector_operations'])
            print(json.dumps(dict(case=name,checks=len(checks))),flush=True)
        sql('COMMIT')
    finally:
        result=dict(passed=sum(c['result']=='PASS' for c in checks),total=len(checks),checks=checks,scope='Original six query blocks on the static Oracle corpus; every requested Oracle mode must have a real HNSW operation, including single-ID and many-to-many cases')
        (a.output/'summary.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
        (a.output/'commands.json').write_text(json.dumps(commands,ensure_ascii=False,indent=2,default=str)+'\n');cur.close();conn.close()
    failed=[c['name'] for c in checks if c['result']!='PASS'];print(json.dumps(dict(result='FAIL' if failed else 'PASS',checks=len(checks),failed=failed)))
    if failed:raise SystemExit(1)

if __name__=='__main__':main()
