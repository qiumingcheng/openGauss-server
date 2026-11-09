#!/usr/bin/env python3
"""Verify native complete Join routes and full result bags on the owned J-02 fixture."""
import argparse
from collections import Counter
import json
from pathlib import Path
import re
import psycopg2
from verify_query_protocol import connect


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    args.output.mkdir(parents=True,exist_ok=False)
    conn=connect();cur=conn.cursor();commands=[];checks=[]
    def sql(statement):
        try:
            cur.execute(statement)
            rows=cur.fetchall() if cur.description else []
            commands.append(dict(sql=statement,rows=rows,notices=list(conn.notices)))
            return rows
        except psycopg2.Error as exc:
            commands.append(dict(sql=statement,error=str(exc),sqlstate=exc.pgcode,notices=list(conn.notices)))
            raise
    def check(name,condition,details):
        checks.append(dict(name=name,result='PASS' if condition else 'FAIL',details=details))
        if not condition:print('FAIL '+name+': '+str(details),flush=True)
    def plan(label,query):
        text='\n'.join(row[0] for row in sql('EXPLAIN(ANALYZE,COSTS OFF,TIMING OFF) '+query))+'\n'
        (args.output/(label+'.plan.txt')).write_text(text)
        return text
    def count(text,label):
        match=re.search(r'^\s*'+re.escape(label)+r': (\d+)\s*$',text,re.M)
        return int(match[1]) if match else None
    schema='vf_join_registration';d=schema+'.documents';a=schema+'.acl';u=schema+'.unique_acl';b=schema+'.flags'
    order=" ORDER BY d.embedding <-> '[7.25,1,2,3,4,5,6,7]'::vector LIMIT 10"
    cases=[
        ('unique',f'SELECT d.id FROM {d} d JOIN {u} a ON d.bucket=a.bucket',True),
        ('unique_filtered',f'SELECT d.id FROM {d} d JOIN {u} a ON d.bucket=a.bucket WHERE a.bucket>=3 AND d.tenant=3',True),
        ('nonunique',f'SELECT d.id FROM {d} d JOIN {a} a ON d.bucket=a.bucket WHERE a.enabled',False),
        ('business_projection',f'SELECT d.id,a.acl_group FROM {d} d JOIN {a} a ON d.bucket=a.bucket WHERE a.enabled AND d.tenant=3',False),
        ('left_on_where',f'SELECT d.id,a.acl_group,a.bucket FROM {d} d LEFT JOIN {a} a ON d.bucket=a.bucket AND a.acl_group=1 AND a.bucket<4 AND a.enabled WHERE d.tenant=3 AND (a.bucket>=3 OR a.bucket IS NULL)',False),
        ('semi',f'SELECT d.id FROM {d} d WHERE EXISTS(SELECT 1 FROM {a} a WHERE a.bucket=d.bucket AND a.acl_group=1 AND a.enabled)',True),
        ('anti',f'SELECT d.id FROM {d} d WHERE NOT EXISTS(SELECT 1 FROM {a} a WHERE a.bucket=d.bucket AND a.acl_group=1 AND a.enabled)',True),
        ('empty_inner',f'SELECT d.id FROM {d} d JOIN {u} a ON d.bucket=a.bucket WHERE a.bucket>=999',True),
        ('three_relations',f'SELECT d.id,a.acl_group FROM {d} d JOIN {a} a ON d.bucket=a.bucket JOIN {b} b ON b.bucket=a.bucket WHERE a.enabled AND b.enabled',False),
        ('target_last',f'SELECT d.id,a.acl_group FROM {a} a JOIN {b} b ON b.bucket=a.bucket JOIN {d} d ON d.bucket=b.bucket WHERE a.enabled AND b.enabled',False),
        ('same_table_alias',f'SELECT d.id FROM {d} x JOIN {d} d ON d.bucket=x.bucket WHERE x.tenant=3',False),
        ('native_not_in',f'SELECT d.id FROM {d} d WHERE d.bucket NOT IN(SELECT a.bucket FROM {a} a)',True),
    ]
    try:
        sql('SET enable_diskann_filter=on; SET enable_opfusion=off; SET query_dop=1; SET diskann_probes=256; SET statement_timeout=30000; SET client_min_messages=warning')
        if sql(f"SELECT obj_description(oid,'pg_namespace') FROM pg_namespace WHERE nspname='{schema}'")!=[('Owned by DiskANN query filter J-02 metadata tests',)]:
            raise RuntimeError('missing or unrelated fixture')
        sql('BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY')
        for label,query,elision in cases:
            query+=order
            sql("SET diskann_filter_mode='exact'")
            expected=sql(query)
            ordinary=plan(label+'_exact',query)
            check(label+'_ordinary_independent','Executed Mode:' not in ordinary,ordinary)
            for mode in ('pre','in','post'):
                sql("SET diskann_filter_mode='"+mode+"'")
                try:
                    actual=sql(query)
                    text=plan(label+'_'+mode,query)
                except psycopg2.Error as exc:
                    check(label+'_'+mode+'_exec',False,dict(sqlstate=exc.pgcode,error=str(exc)))
                    # An error aborts the snapshot; end it before the next test.
                    sql('ROLLBACK');sql('BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY')
                    continue
                check(label+'_'+mode+'_full_bag',Counter(actual)==Counter(expected) and [r[0] for r in actual]==[r[0] for r in expected],dict(expected=expected,actual=actual))
                check(label+'_'+mode+'_one_ann',text.count('Ann Index Scan using')==1 and count(text,'Search Rounds')==1 and count(text,'Scan Epoch')==1 and 'Executed Mode: '+mode.upper() in text and 'Sort' in text and 'Custom Scan' not in text, text)
                ann_indent=next((len(line)-len(line.lstrip()) for line in text.splitlines() if 'Ann Index Scan using' in line),None)
                upper_joins=[line for line in text.splitlines() if 'Join' in line or 'Nested Loop' in line]
                output_joins=[line for line in upper_joins if len(line)-len(line.lstrip())<ann_indent]
                if mode=='post' or not elision:
                    check(label+'_'+mode+'_real_output_join',bool(output_joins),text)
                elif label!='empty_inner':
                    check(label+'_'+mode+'_proven_elision',not output_joins,text)
                if mode=='pre':
                    check(label+'_pre_real_source',bool(upper_joins) and 'SubPlan' not in text and count(text,'Qualification Probes')==0,text)
                if mode=='in':
                    loops=re.search(r'Tid Scan.*loops=(\d+)\)',text)
                    check(label+'_in_sync_native_probe','SubPlan' in text and loops is not None and int(loops[1])==count(text,'Qualification Probes') and count(text,'Qualification Probes')>0 and re.search(r'Index(?: Only)? Scan using .* on .*a',text) is not None,text)
                if label=='left_on_where':
                    check(label+'_'+mode+'_null_extension',sum(row[1] is None for row in actual)>0 and 'IS NULL' in text and 'Left Join' in text,text)
        sql('COMMIT')
        # Each custom query block binds a fresh external parameter. No global
        # DISTINCT, added tie key or hidden LIMIT changes the source SQL.
        correlated=f"SELECT ARRAY(SELECT d.id FROM {d} d JOIN {u} a ON d.bucket=a.bucket WHERE d.tenant=x.n{order}) FROM (VALUES(1),(3),(1))x(n)"
        sql("SET diskann_filter_mode='exact'");expected=sql(correlated)
        for mode in ('pre','in','post'):
            sql("SET diskann_filter_mode='"+mode+"'")
            try:
                actual=sql(correlated);text=plan('external_'+mode,correlated)
                check('external_'+mode+'_fresh_binding',actual==expected,dict(expected=expected,actual=actual,plan=text))
            except psycopg2.Error as exc:
                check('external_'+mode+'_fresh_binding',False,str(exc))
    finally:
        (args.output/'commands.json').write_text(json.dumps(commands,ensure_ascii=False,indent=2,default=str)+'\n')
        result=dict(passed=sum(c['result']=='PASS' for c in checks),total=len(checks),checks=checks,scope='native owned Join routes; Oracle compatibility is a separate P2 gate')
        (args.output/'summary.json').write_text(json.dumps(result,ensure_ascii=False,indent=2,default=str)+'\n')
        cur.close();conn.close()
    failed=[c['name'] for c in checks if c['result']!='PASS']
    print(json.dumps(dict(result='FAIL' if failed else 'PASS',checks=len(checks),failed=failed)))
    if failed:raise SystemExit(1)


if __name__=='__main__':main()
