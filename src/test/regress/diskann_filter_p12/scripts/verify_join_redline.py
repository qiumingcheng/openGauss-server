#!/usr/bin/env python3
"""Execute the same unique-business SQL through the three canonical native plans."""
import argparse,json,re
from collections import Counter
from pathlib import Path
import psycopg2
from verify_query_protocol import connect

MARKER='Owned by DiskANN query filter J-01 canonical plan acceptance'
QUERY="SELECT d.id FROM vf_oracle_join_p2.documents d JOIN vf_join_redline.allowed_buckets a ON d.bucket_id=a.bucket_id ORDER BY d.embedding <=> '[1,3,5,7,11,13,17,19]'::vector LIMIT 10"

def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--output',type=Path,required=True);a=p.parse_args();a.output.mkdir(parents=True,exist_ok=False)
    conn=connect();cur=conn.cursor();commands=[];checks=[]
    def sql(q):
        try:
            cur.execute(q);rows=cur.fetchall() if cur.description else []
            commands.append(dict(sql=q,rows=rows if len(rows)<40 else dict(count=len(rows)),notices=list(conn.notices)));conn.notices.clear();return rows
        except psycopg2.Error as exc:
            commands.append(dict(sql=q,error=str(exc),sqlstate=exc.pgcode));raise
    def check(name,ok,details):
        checks.append(dict(name=name,result='PASS' if ok else 'FAIL',details=details))
        if not ok:print('FAIL '+name+': '+str(details),flush=True)
    def plan(label,q=QUERY):
        text='\n'.join(r[0] for r in sql('EXPLAIN(ANALYZE,COSTS OFF,TIMING OFF) '+q))+'\n'
        (a.output/(label+'.plan.txt')).write_text(text);return text
    def count(text,label):
        m=re.search(r'^\s*'+re.escape(label)+r': (\d+)\s*$',text,re.M);return int(m[1]) if m else None
    try:
        sql('SET enable_diskann_filter=on;SET enable_opfusion=off;SET query_dop=1;SET diskann_filter_mode=exact;SET diskann_probes=2048;SET statement_timeout=30000')
        owner=sql("SELECT obj_description(oid,'pg_namespace') FROM pg_namespace WHERE nspname='vf_join_redline'")
        if owner and owner!=[(MARKER,)]:raise RuntimeError('refusing unrelated schema')
        if not owner:
            sql('CREATE SCHEMA vf_join_redline');sql("COMMENT ON SCHEMA vf_join_redline IS '"+MARKER+"'")
        exists=sql("SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='vf_join_redline' AND c.relname='allowed_buckets'")[0][0]
        if not exists:
            sql('CREATE TABLE vf_join_redline.allowed_buckets(bucket_id int PRIMARY KEY) WITH(storage_type=astore)')
            sql('INSERT INTO vf_join_redline.allowed_buckets SELECT generate_series(0,999)');sql('ANALYZE vf_join_redline.allowed_buckets')
        check('unique_business_fixture',sql('SELECT count(*),count(DISTINCT bucket_id),min(bucket_id),max(bucket_id) FROM vf_join_redline.allowed_buckets')==[(1000,1000,0,999)],'owned unique business keys; existing target/index reused')
        sql('BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY');expected=sql(QUERY);ordinary=plan('exact')
        check('ordinary_exact_independent',len(expected)==10 and 'Ann Index Scan' not in ordinary,ordinary)
        for mode in ('pre','in','post'):
            sql("SET diskann_filter_mode='"+mode+"'");rows=sql(QUERY);text=plan(mode)
            ids=','.join(str(row[0]) for row in rows)
            values=sql("SELECT d.id,d.embedding <=> '[1,3,5,7,11,13,17,19]'::vector,encode(float8send(d.embedding <=> '[1,3,5,7,11,13,17,19]'::vector),'hex') FROM vf_oracle_join_p2.documents d WHERE d.id IN("+ids+")")
            distances={row[0]:row[1] for row in values}
            check(mode+'_original_result',Counter(rows)==Counter(expected) and all(distances[x[0]]<=distances[y[0]] for x,y in zip(rows,rows[1:])),dict(native=rows,ordinary=expected,original_distance_bits=values))
            check(mode+'_one_ann_and_user_limit',text.count('Ann Index Scan using')==1 and sum(bool(re.match(r'^\s*(?:->\s*)?Limit\b',line)) for line in text.splitlines())==1 and text.splitlines()[0].startswith('Limit') and 'Custom Scan' not in text and 'Unique' not in text,text)
            lines=text.splitlines();ann=next(i for i,line in enumerate(lines) if 'Ann Index Scan using' in line)
            check(mode+'_real_final_sort',any('Sort Key:' in line and '<=>' in line for line in lines[:ann]) and count(text,'Search Rounds')==1 and count(text,'Scan Epoch')==1 and 'Executed Mode: '+mode.upper() in text,text)
            if mode=='pre':
                check('pre_lower_native_hash_join',any('Hash Join' in line for line in lines[ann+1:]) and not any('Join' in line or 'Nested Loop' in line for line in lines[:ann]) and 'Seq Scan on documents' in text and 'Seq Scan on allowed_buckets' in text and 'SubPlan' not in text,text)
                check('pre_complete_source_once',count(text,'PRE Source Rows')==50000 and count(text,'PRE Build Runs')==1 and count(text,'PRE Seals')==1 and count(text,'Qualification Probes')==0,text)
            elif mode=='in':
                calls=count(text,'Qualification Probes');tid=re.search(r'Tid Scan[^\n]*loops=(\d+)',text)
                check('in_real_local_tid_and_keyed_probe','SubPlan' in text and 'Nested Loop' in text and tid is not None and calls>0 and int(tid[1])==calls and 'Index Scan using allowed_buckets_pkey' in text and 'TID Cond:' in text,text)
                check('in_no_complete_business_scan','Seq Scan on allowed_buckets' not in text and 'Hash Join' not in text,text)
            else:
                check('post_upper_native_hash_join',any('Hash Join' in line for line in lines[:ann]) and 'Seq Scan on allowed_buckets' in text and 'SubPlan' not in text,text)
                check('post_no_exact_or_feedback',count(text,'Qualification Probes')==0 and 'exact search' not in text.lower() and count(text,'Search Rounds')==1,text)
        sql('SET diskann_filter_mode=auto');rows=sql(QUERY);text=plan('auto')
        check('auto_legal_complete_winner',rows==expected and ('Executed Mode:' in text or 'Ann Index Scan' not in text),text)
        sql('SET diskann_probes=2')
        for mode in ('pre','in','post'):
            sql("SET diskann_filter_mode='"+mode+"'");rows=sql(QUERY);text=plan(mode+'_small_budget')
            # The configured beam counts vector nodes. A node may own
            # several distinct row TIDs, so two nodes need not mean two rows.
            check(mode+'_p3_feedback_not_prerequisite',len(rows)<10 and count(text,'Initial Search Nodes')==2 and count(text,'Search Rounds')==1 and 'Executed Mode: '+mode.upper() in text,dict(rows=rows,plan=text))
        sql('COMMIT');sql('SET diskann_probes=2048')
        unsupported=QUERY+' OFFSET 1'
        sql('SET diskann_filter_mode=auto');text=plan('auto_unsupported_offset',unsupported)
        check('auto_unsupported_keeps_ordinary','Ann Index Scan' not in text,text)
        for mode in ('pre','in','post'):
            sql("SET diskann_filter_mode='"+mode+"'")
            try:sql(unsupported)
            except psycopg2.Error as exc:check(mode+'_cannot_bypass_capability',exc.pgcode=='0A000' and 'OFFSET' in str(exc),str(exc))
            else:check(mode+'_cannot_bypass_capability',False,'unsupported query admitted')
    finally:
        (a.output/'commands.json').write_text(json.dumps(commands,ensure_ascii=False,indent=2)+'\n')
        result=dict(passed=sum(c['result']=='PASS' for c in checks),total=len(checks),checks=checks,sql=QUERY)
        (a.output/'summary.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n');cur.close();conn.close()
    failed=[c['name'] for c in checks if c['result']!='PASS'];print(json.dumps(dict(result='FAIL' if failed else 'PASS',checks=len(checks),failed=failed)))
    if failed:raise SystemExit(1)

if __name__=='__main__':main()
