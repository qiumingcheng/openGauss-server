#!/usr/bin/env python3
"""Verify original NL inner multirow results, real rescan and material replay."""
import argparse,json,re
from collections import Counter
from pathlib import Path
import psycopg2
from join_nl_cases import CASES,query
from verify_query_protocol import connect

def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--oracle',type=Path,required=True)
    p.add_argument('--output',type=Path,required=True);a=p.parse_args();a.output.mkdir(parents=True,exist_ok=False)
    oracle=json.loads(a.oracle.read_text());conn=connect();cur=conn.cursor();checks=[];commands=[]
    def sql(q):
        try:
            cur.execute(q);rows=cur.fetchall() if cur.description else []
            commands.append(dict(sql=q,rows=rows,notices=list(conn.notices)));conn.notices.clear();return rows
        except psycopg2.Error as exc:
            commands.append(dict(sql=q,error=str(exc),sqlstate=exc.pgcode));raise
    def check(name,ok,details):
        checks.append(dict(name=name,result='PASS' if ok else 'FAIL',details=details))
        if not ok:print('FAIL '+name+': '+str(details),flush=True)
    def count(text,label):
        m=re.search(r'^\s*'+re.escape(label)+r': (\d+)\s*$',text,re.M);return int(m[1]) if m else None
    try:
        sql('SET enable_diskann_filter=on;SET enable_opfusion=off;SET query_dop=1;SET diskann_probes=2048;SET statement_timeout=30000')
        for material in ('off','on'):
            sql('SET enable_material='+material)
            for case in CASES:
                q=query(case,False,'exact');sql("SET diskann_filter_mode=exact");expected=sql(q)
                ref=next(r for r in oracle if r['case']==case and r['mode']=='exact')
                check(case+'_'+material+'_ordinary_oracle_bag',Counter(expected)==Counter(map(tuple,ref['rows'])),dict(native=expected,oracle=ref['rows']))
                for mode in ('pre','in','post'):
                    label=case+'_'+material+'_'+mode;sql("SET diskann_filter_mode='"+mode+"'")
                    try:
                        rows=sql(q);text='\n'.join(r[0] for r in sql('EXPLAIN(ANALYZE,COSTS OFF,TIMING OFF) '+q))+'\n'
                    except psycopg2.Error as exc:
                        check(label+'_execute',False,dict(error=str(exc),sqlstate=exc.pgcode));continue
                    (a.output/(label+'.plan.txt')).write_text(text)
                    ref=next(r for r in oracle if r['case']==case and r['mode']==('in_filter' if mode=='in' else mode))
                    check(label+'_original_oracle_bag',Counter(rows)==Counter(map(tuple,ref['rows'])),dict(native=rows,oracle=ref['rows']))
                    check(label+'_oracle_real_vector',ref['vector_transformed'],ref['observed_vector_operations'])
                    ann=re.search(r'Ann Index Scan using[^\n]*actual rows=(\d+) loops=(\d+)',text)
                    check(label+'_real_multirow_ann_inner',ann is not None and 'Nested Loop' in text and text.count('Ann Index Scan using')==1 and 'Executed Mode: '+mode.upper() in text,text)
                    if not ann:continue
                    epoch=count(text,'Scan Epoch');loops=int(ann[2]);rounds=count(text,'Search Rounds')
                    check(label+'_epoch_matches_real_calls',epoch==loops and rounds==loops and int(ann[1])>1,text)
                    limits=sum(bool(re.match(r'^\s*(?:->\s*)?Limit\b',line)) for line in text.splitlines())
                    check(label+'_original_query_block_limits',limits==1 and len(rows)==(10 if case.startswith('global') else 12 if case=='independent_top3' else 18) and 'Unique' not in text,text)
                    if case.startswith('global'):
                        check(label+'_all_outer_occurrences',Counter(r[0] for r in rows)==Counter({31626:4,33286:2,48626:4}),rows)
                        if material=='off':check(label+'_six_rescans',loops==6,text)
                    elif case=='independent_top3':
                        # Native Sort can rewind its complete Top-3 output
                        # even when the separate Materialize node is disabled.
                        # openGauss reports accumulated rows across loops.
                        # Four reads of the original Top-3 are 12 rows total.
                        replay=re.search(r'(?:Materialize|Subquery Scan on v)[^\n]*actual rows=12 loops=4',text)
                        check(label+'_independent_replay',loops==1 and replay is not None,text)
                    else:check(label+'_correlated_six_calls',loops==6 and int(ann[1])>=3,text)
                print(json.dumps(dict(case=case,material=material,checks=len(checks))),flush=True)
    finally:
        result=dict(passed=sum(c['result']=='PASS' for c in checks),total=len(checks),checks=checks)
        (a.output/'summary.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
        (a.output/'commands.json').write_text(json.dumps(commands,ensure_ascii=False,indent=2)+'\n');cur.close();conn.close()
    failed=[c['name'] for c in checks if c['result']!='PASS'];print(json.dumps(dict(result='FAIL' if failed else 'PASS',checks=len(checks),failed=failed)))
    if failed:raise SystemExit(1)

if __name__=='__main__':main()
