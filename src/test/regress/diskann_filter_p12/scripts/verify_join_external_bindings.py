#!/usr/bin/env python3
"""Keep outer predicates, query vectors and candidate TID parameters separate."""
import argparse,json,re
from collections import Counter
from pathlib import Path
import psycopg2
from verify_query_protocol import connect

def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--output',type=Path,required=True);a=p.parse_args();a.output.mkdir(parents=True,exist_ok=False)
    conn=connect();cur=conn.cursor();commands=[];checks=[]
    def sql(q):
        try:
            cur.execute(q);rows=cur.fetchall() if cur.description else []
            commands.append(dict(sql=q,rows=rows));return rows
        except psycopg2.Error as exc:
            commands.append(dict(sql=q,error=str(exc),sqlstate=exc.pgcode));raise
    def check(name,ok,details):
        checks.append(dict(name=name,result='PASS' if ok else 'FAIL',details=details))
        if not ok:print('FAIL '+name+': '+str(details),flush=True)
    def count(text,label):
        m=re.search(r'^\s*'+re.escape(label)+r': (\d+)\s*$',text,re.M);return int(m[1]) if m else None
    body="SELECT x.ord,x.tenant,r.id FROM (VALUES(1,3),(2,4),(3,3)) x(ord,tenant) CROSS JOIN LATERAL (SELECT d.id FROM vf_join_registration.documents d JOIN vf_join_registration.acl a ON d.bucket=a.bucket WHERE d.tenant=x.tenant AND a.enabled ORDER BY d.embedding <-> $1 LIMIT 3) r ORDER BY x.ord,r.id"
    vectors=['[7.25,1,2,3,4,5,6,7]','[201.25,1,2,3,4,5,6,7]','[7.25,1,2,3,4,5,6,7]']
    try:
        sql('SET enable_diskann_filter=on;SET enable_opfusion=off;SET query_dop=1;SET diskann_probes=256;SET statement_timeout=30000;SET plan_cache_mode=force_custom_plan')
        sql('PREPARE j6_external(vector) AS '+body)
        for mode in ('pre','in','post'):
            outputs=[]
            for i,vector in enumerate(vectors):
                execution="EXECUTE j6_external('"+vector+"')";label=mode+'_'+str(i)
                sql('SET diskann_filter_mode=exact');expected=sql(execution)
                sql("SET diskann_filter_mode='"+mode+"'");rows=sql(execution)
                text='\n'.join(r[0] for r in sql('EXPLAIN(ANALYZE,COSTS OFF,TIMING OFF) '+execution))+'\n'
                (a.output/(label+'.plan.txt')).write_text(text);outputs.append(rows)
                check(label+'_complete_rows',Counter(rows)==Counter(expected) and len(rows)==9,dict(native=rows,ordinary=expected))
                check(label+'_fresh_outer_scans',count(text,'Scan Epoch')==3 and count(text,'Search Rounds')==3 and text.count('Ann Index Scan using')==1 and 'Executed Mode: '+mode.upper() in text,text)
                check(label+'_repeat_outer_key_preserved',[r[2] for r in rows if r[0]==1]==[r[2] for r in rows if r[0]==3],rows)
                if mode=='in':
                    calls=count(text,'Qualification Probes');tid=re.search(r'Tid Scan[^\n]*loops=(\d+)',text)
                    check(label+'_candidate_changes_are_local',calls is not None and calls>3 and tid is not None and int(tid[1])==calls and count(text,'Search Rounds')==3,text)
            check(mode+'_vector_change_and_restore',outputs[0]==outputs[2] and outputs[0]!=outputs[1],outputs)
        sql('SET plan_cache_mode=force_generic_plan;SET diskann_filter_mode=auto')
        rows=sql("EXECUTE j6_external('[7.25,1,2,3,4,5,6,7]')")
        text='\n'.join(r[0] for r in sql("EXPLAIN(COSTS OFF) EXECUTE j6_external('[7.25,1,2,3,4,5,6,7]')"))+'\n'
        check('unproven_generic_vector_keeps_ordinary',len(rows)==9 and 'Ann Index Scan' not in text,text)
        for mode in ('pre','in','post'):
            sql("SET diskann_filter_mode='"+mode+"'")
            try:sql("EXECUTE j6_external('[7.25,1,2,3,4,5,6,7]')")
            except psycopg2.Error as exc:check(mode+'_unproven_generic_vector_specific_rejection',exc.pgcode=='0A000' and 'query vector' in str(exc),str(exc))
            else:check(mode+'_unproven_generic_vector_specific_rejection',False,'unknown vector admitted')
        sql('DEALLOCATE j6_external;SET plan_cache_mode=auto');check('connection_reusable',sql('SELECT 1')==[(1,)],'same connection')
    finally:
        (a.output/'commands.json').write_text(json.dumps(commands,ensure_ascii=False,indent=2)+'\n')
        result=dict(passed=sum(c['result']=='PASS' for c in checks),total=len(checks),checks=checks,scope='Known custom-plan vectors change and restore on one PREPARE; real outer PARAM_EXEC values drive three native scans; unknown generic vectors retain the established capability gate')
        (a.output/'summary.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n');cur.close();conn.close()
    failed=[c['name'] for c in checks if c['result']!='PASS'];print(json.dumps(dict(result='FAIL' if failed else 'PASS',checks=len(checks),failed=failed)))
    if failed:raise SystemExit(1)

if __name__=='__main__':main()
