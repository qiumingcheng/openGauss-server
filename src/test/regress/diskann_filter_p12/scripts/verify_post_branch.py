#!/usr/bin/env python3
"""Verify native POST qualification and original-distance Sort over fixed candidates."""
import argparse
import json
import re
from pathlib import Path
from verify_query_protocol import connect
from verify_row_identity import tie_groups

QUERY="SELECT id,tenant_id,embedding::text,embedding <-> '[7.25,1,2,3,4,5,6,7]'::vector AS distance FROM vf_post.documents WHERE tenant_id=3 ORDER BY embedding <-> '[7.25,1,2,3,4,5,6,7]'::vector LIMIT 1000"


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args();args.output.mkdir(parents=True,exist_ok=False)
    conn,reference=connect(),connect();cur,rc=conn.cursor(),reference.cursor()
    checks,statements=[],[]
    def sql(q,cursor=None):
        cursor=cur if cursor is None else cursor
        cursor.execute(q);rows=cursor.fetchall() if cursor.description else []
        statements.append(dict(sql=q,rows=rows));return rows
    def check(name,ok,details):
        checks.append(dict(name=name,result='PASS' if ok else 'FAIL',details=details))
        if not ok:
            print('FAIL '+name+': '+str(details),flush=True)
    def count(text,label):
        match=re.search(r'^\s*'+re.escape(label)+r': (\d+)\s*$',text,re.M)
        return int(match.group(1)) if match else None
    def plan(label,q):
        text='\n'.join(row[0] for row in sql('EXPLAIN (ANALYZE,COSTS OFF,TIMING OFF) '+q))+'\n'
        (args.output/(label+'.plan.txt')).write_text(text);return text
    try:
        sql('SET enable_opfusion=off; SET enable_diskann_filter=on; SET diskann_filter_mode=exact')
        sql('SET enable_opfusion=off; SET enable_diskann_filter=on; SET diskann_filter_mode=exact',rc)
        sql('CREATE SCHEMA IF NOT EXISTS vf_post; DROP TABLE IF EXISTS vf_post.documents')
        sql('CREATE TABLE vf_post.documents(id int PRIMARY KEY,tenant_id int NOT NULL,embedding vector(8) NOT NULL CHECK(vector_norm(embedding)>0)) WITH(storage_type=astore)')
        sql("INSERT INTO vf_post.documents SELECT i,i%7,('['||i||',1,2,3,4,5,6,7]')::vector FROM generate_series(1,64)i")
        sql("INSERT INTO vf_post.documents VALUES(1001,3,'[7.250001,1,2,3,4,5,6,7]'::vector),(1002,3,'[7.250002,1,2,3,4,5,6,7]'::vector)")
        sql('CREATE INDEX vf_post_l2 ON vf_post.documents USING diskann(embedding vector_l2_ops)')
        sql('CREATE INDEX vf_post_cos ON vf_post.documents USING diskann(embedding vector_cosine_ops)')
        sql('ANALYZE vf_post.documents')
        sql('SET diskann_filter_mode=post; SET diskann_probes=256')
        for metric,op in [('l2','<->'),('cos','<=>')]:
            query=QUERY.replace('<->',op)
            expected=sql(query,rc);actual=sql(query);text=plan(metric,query)
            check(metric+'_original_sql_distance_and_rows',tie_groups(actual)==tie_groups(expected) and len(expected)==11,dict(actual=actual,exact=expected))
            check(metric+'_native_plan_and_filter','Ann Index Scan' in text and 'Sort Key:' in text and op in text and 'Filter: (tenant_id = 3)' in text and 'SubPlan' not in text and 'Custom' not in text and 'VectorFilter' not in text,text)
            check(metric+'_no_business_probe_or_exact',count(text,'Qualification Probes')==0 and count(text,'Probe Cache Hits')==0 and count(text,'Search Rounds')==1 and 'Executed Mode: POST' in text and 'EXACT' not in text,text)
            check(metric+'_post_candidate_then_scalar',count(text,'Candidate Callbacks')==66 and count(text,'Accepted Members')==66 and count(text,'Qualified Target Rows')==11 and 'Rows Removed by Filter: 55' in text,text)
        sql('SET diskann_probes=2')
        for label,predicate in [('some','id>0'),('rejected','id BETWEEN 61 AND 64')]:
            query=QUERY.replace('tenant_id=3',predicate).replace('LIMIT 1000','LIMIT 10')
            rows=sql(query);text=plan(label,query)
            check(label+'_fixed_candidates',len(rows)<=2 and count(text,'Candidate Members')==2 and count(text,'Qualification Probes')==0 and count(text,'Search Rounds')==1 and 'Search End Reason: configured beam completed' in text,text)
            check(label+'_returned_rows_match_counter',count(text,'Qualified Target Rows')==len(rows) and 'Executed Mode: POST' in text and 'EXACT' not in text,dict(rows=rows,plan=text))
            if label=='rejected':
                check('all_returned_candidates_rejected',rows==[],'fixed nearest candidates are outside ids 61..64')
        check('partial_return_is_not_k',len(sql(QUERY.replace('tenant_id=3','id>0').replace('LIMIT 1000','LIMIT 10')))==2,'fixed two-node budget gives two candidates')
        sql('SET diskann_probes=256')
        rescan="""SELECT x.ord,(SELECT array_agg(s.id) FROM
          (SELECT d.id FROM vf_post.documents d WHERE d.tenant_id=x.tenant
           ORDER BY d.embedding <-> '[7.25,1,2,3,4,5,6,7]'::vector LIMIT 10)s)
          FROM (VALUES(1,3),(2,4),(3,3),(4,777))x(ord,tenant) ORDER BY x.ord"""
        text=plan('real_rescan',rescan)
        check('post_real_rescan_rows',sql(rescan)==sql(rescan,rc),rescan)
        check('post_real_rescan_one_search_each',count(text,'Search Rounds')==4 and count(text,'Qualification Probes')==0 and 'loops=4' in text,text)
    finally:
        (args.output/'statements.json').write_text(json.dumps(statements,ensure_ascii=False,indent=2)+'\n')
        (args.output/'summary.json').write_text(json.dumps(dict(checks=checks,passed=sum(c['result']=='PASS' for c in checks),total=len(checks)),ensure_ascii=False,indent=2)+'\n')
        cur.close();rc.close();conn.close();reference.close()
    failed=[c['name'] for c in checks if c['result']!='PASS']
    print(json.dumps(dict(result='FAIL' if failed else 'PASS',checks=len(checks),failed=failed),ensure_ascii=False),flush=True)
    if failed:
        raise SystemExit(1)


if __name__=='__main__':
    main()
