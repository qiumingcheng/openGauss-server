#!/usr/bin/env python3
"""Compare original limited SQL against fresh Oracle evidence on the identical static rows."""
import argparse,json,re
from collections import Counter
from pathlib import Path
import psycopg2
from prepare_oracle_join_fixture import SCHEMA,OWNER
from run_oracle_join_semantics import CASES,QUERY_VECTOR
from verify_query_protocol import connect

def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--oracle',type=Path,required=True)
    p.add_argument('--oracle-post',type=Path,required=True);p.add_argument('--probes',type=int,default=256)
    p.add_argument('--output',type=Path,required=True);a=p.parse_args();a.output.mkdir(parents=True,exist_ok=False)
    evidence=json.loads(a.oracle.read_text());evidence+=json.loads(a.oracle_post.read_text())
    conn=connect();cur=conn.cursor();commands=[];checks=[]
    def sql(q):
        try:
            cur.execute(q);rows=cur.fetchall() if cur.description else [];commands.append(dict(sql=q,rows=rows));return rows
        except psycopg2.Error as exc:
            commands.append(dict(sql=q,error=str(exc),sqlstate=exc.pgcode));raise
    def check(name,ok,detail):
        checks.append(dict(name=name,result='PASS' if ok else 'FAIL',details=detail))
        if not ok:print('FAIL '+name+': '+str(detail),flush=True)
    try:
        sql('SET enable_diskann_filter=on;SET enable_opfusion=off;SET query_dop=1;SET statement_timeout=30000;SET diskann_probes='+str(a.probes))
        assert sql("SELECT obj_description(oid,'pg_namespace') FROM pg_namespace WHERE nspname='"+SCHEMA+"'")==[(OWNER,)]
        sql('BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY')
        for name,projection,source,condition in CASES:
            source=source.replace('hjbm_documents',SCHEMA+'.documents').replace('hjbm_allowed_buckets',SCHEMA+'.allowed_buckets')
            condition=condition.replace('hjbm_documents',SCHEMA+'.documents').replace('hjbm_allowed_buckets',SCHEMA+'.allowed_buckets')
            query=f"SELECT {projection} FROM {source} WHERE {condition} ORDER BY d.embedding <=> '{QUERY_VECTOR}'::vector LIMIT 10"
            sql("SET diskann_filter_mode='exact'");exact=sql(query)
            oracle_exact=next(s for s in evidence if s['case']==name and s['mode']=='exact')
            check(name+'_ordinary_cross_engine_bag',Counter(exact)==Counter(map(tuple,oracle_exact['rows'])),dict(native=exact,oracle=oracle_exact['rows']))
            (a.output/(name+'_sql.txt')).write_text(query+';\n')
            for mode in ('pre','in','post'):
                unsupported=name in ('nullable_target','full_outer')
                sql("SET diskann_filter_mode='"+mode+"'")
                if unsupported:
                    sql('SAVEPOINT unsupported_mode')
                    try:sql(query)
                    except psycopg2.Error as exc:
                        check(name+'_'+mode+'_specific_rejection',exc.pgcode=='0A000' and 'real output vector target TID' in str(exc),str(exc))
                        sql('ROLLBACK TO SAVEPOINT unsupported_mode')
                    else:check(name+'_'+mode+'_specific_rejection',False,'accepted unsupported target identity')
                    sql('RELEASE SAVEPOINT unsupported_mode');sql("SET diskann_filter_mode='auto'")
                oracle_mode='in_filter' if mode=='in' else mode
                matches=[s for s in evidence if s['case']==name and s['mode']==oracle_mode]
                oracle=next((s for s in matches if s['vector_transformed']),matches[0])
                actual=sql(query)
                text='\n'.join(row[0] for row in sql('EXPLAIN(ANALYZE,COSTS OFF,TIMING OFF) '+query))+'\n'
                (a.output/(name+'_'+mode+'.plan.txt')).write_text(text)
                check(name+'_'+mode+'_oracle_full_bag',Counter(actual)==Counter(map(tuple,oracle['rows'])),dict(native=actual,oracle=oracle['rows']))
                # Repeated business rows at the same target have unspecified
                # internal order. Nullable/full cases cannot use target ID as
                # a strict distance ordering proxy; all output rows still compare.
                if not unsupported:
                    check(name+'_'+mode+'_oracle_target_order',[r[0] for r in actual]==[r[0] for r in oracle['rows']],dict(native=actual,oracle=oracle['rows']))
                    check(name+'_'+mode+'_observed_both_modes',oracle['vector_transformed'] and 'Executed Mode: '+mode.upper() in text,dict(oracle_operations=oracle['observed_vector_operations'],native_plan=text))
                    if mode=='in':
                        loops=re.search(r'Tid Scan.*loops=(\d+)\)',text);calls=re.search(r'Qualification Probes: (\d+)',text)
                        check(name+'_in_real_tid_probe',loops and calls and int(loops[1])==int(calls[1])>0,text)
                else:
                    check(name+'_'+mode+'_both_ordinary',not oracle['vector_transformed'] and 'Ann Index Scan' not in text,text)
            print(json.dumps(dict(case=name,checks=len(checks))),flush=True)
        sql('COMMIT')
    finally:
        (a.output/'commands.json').write_text(json.dumps(commands,ensure_ascii=False,indent=2,default=str)+'\n')
        result=dict(passed=sum(c['result']=='PASS' for c in checks),total=len(checks),checks=checks,probes=a.probes,scope='Fixed static Oracle corpus, seven original limited queries; no arbitrary-input equivalence claim')
        (a.output/'summary.json').write_text(json.dumps(result,ensure_ascii=False,indent=2,default=str)+'\n');cur.close();conn.close()
    failed=[c['name'] for c in checks if c['result']!='PASS']
    print(json.dumps(dict(result='FAIL' if failed else 'PASS',checks=len(checks),failed=failed)))
    if failed:raise SystemExit(1)

if __name__=='__main__':main()
