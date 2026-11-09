#!/usr/bin/env python3
"""Verify real IN SubPlan calls, per-member caches and scan-epoch isolation."""
import argparse
import json
import re
from pathlib import Path
import psycopg2
from verify_query_protocol import connect
from verify_row_identity import tie_groups

QUERY = "SELECT id,tenant_id,embedding::text,ctid::text,embedding <-> '[7.25,1,2,3,4,5,6,7]'::vector FROM vf_in.documents WHERE tenant_id=3 ORDER BY embedding <-> '[7.25,1,2,3,4,5,6,7]'::vector LIMIT 10"
RESCAN = """SELECT x.ord,x.tenant,
 (SELECT array_agg(s.id) FROM
   (SELECT d.id FROM vf_in.documents d WHERE d.tenant_id=x.tenant
    ORDER BY d.embedding <-> '[7.25,1,2,3,4,5,6,7]'::vector LIMIT 10) s) ids
 FROM (VALUES(1,3),(2,4),(3,3),(4,777)) x(ord,tenant) ORDER BY x.ord"""


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True,exist_ok=False)
    conn,reference=connect(),connect()
    cur,rc=conn.cursor(),reference.cursor()
    checks,statements=[],[]

    def sql(query,cursor=None):
        cursor=cur if cursor is None else cursor
        cursor.execute(query)
        rows=cursor.fetchall() if cursor.description else []
        statements.append(dict(sql=query,rows=rows,affected_rows=cursor.rowcount))
        return rows

    def check(name,ok,details):
        checks.append(dict(name=name,result='PASS' if ok else 'FAIL',details=details))
        if not ok:
            print('FAIL '+name+': '+str(details),flush=True)

    def count(text,label):
        m=re.search(r'^\s*'+re.escape(label)+r': (\d+)\s*$',text,re.M)
        return int(m.group(1)) if m else None

    def plan(label,query):
        text='\n'.join(row[0] for row in sql('EXPLAIN (ANALYZE,COSTS OFF,TIMING OFF) '+query))+'\n'
        (args.output/(label+'.plan.txt')).write_text(text)
        return text

    def reconcile(label,text,cycles=1):
        loops=re.search(r'Tid Scan.*loops=(\d+)\)',text)
        calls=count(text,'Qualification Probes')
        callback=count(text,'Candidate Callbacks')
        accept,reject=count(text,'Accepted Members'),count(text,'Rejected Members')
        hits=count(text,'Probe Cache Hits')
        visible=count(text,'Visible Rows')
        keymiss=count(text,'Vector Key Mismatches')
        check(label+'_actual_probe_loops',loops is not None and int(loops.group(1))==calls and calls>0,text)
        check(label+'_callback_verdict_counts',callback==count(text,'Candidate Members') and callback==accept+reject,text)
        check(label+'_cache_vs_actual_calls',calls+hits==visible-keymiss,text)
        check(label+'_one_search_per_cycle',count(text,'Search Rounds')==cycles and 'Executed Mode: IN' in text and 'PRE Build Runs' not in text,text)

    try:
        sql('SET enable_opfusion=off; SET enable_diskann_filter=on; SET diskann_filter_mode=exact; SET diskann_probes=256')
        sql('SET enable_opfusion=off; SET enable_diskann_filter=on; SET diskann_filter_mode=exact',rc)
        sql('CREATE SCHEMA IF NOT EXISTS vf_in')
        sql('DROP TABLE IF EXISTS vf_in.documents')
        sql('CREATE TABLE vf_in.documents(id int PRIMARY KEY,tenant_id int NOT NULL,embedding vector(8) NOT NULL CHECK(vector_norm(embedding)>0)) WITH(storage_type=astore,fillfactor=60)')
        sql("INSERT INTO vf_in.documents SELECT i,i%7,('['||i||',1,2,3,4,5,6,7]')::vector FROM generate_series(1,128)i")
        sql('CREATE INDEX vf_in_l2 ON vf_in.documents USING diskann(embedding vector_l2_ops)')
        sql('CREATE INDEX vf_in_cos ON vf_in.documents USING diskann(embedding vector_cosine_ops)')
        sql('ANALYZE vf_in.documents')
        sql('SET diskann_filter_mode=\'in\'')
        for label,predicate,size in [('all','id>0',128),('empty','tenant_id=777',0),('one','id=3',1),('many','tenant_id=3',18),('null','NULLIF(id,3)=0',0)]:
            query=QUERY.replace('tenant_id=3',predicate).replace('LIMIT 10','LIMIT 1000')
            actual,expected=sql(query),sql(query,rc)
            check(label+'_original_sql_rows',tie_groups(actual)==tie_groups(expected) and len(actual)==size,dict(actual=actual,exact=expected))
            reconcile(label,plan(label,query))
        text=plan('real_rescan',RESCAN)
        actual,expected=sql(RESCAN),sql(RESCAN,rc)
        check('real_rescan_original_rows',actual==expected and actual[0][2]==actual[2][2] and actual[3][2] is None,dict(actual=actual,exact=expected))
        reconcile('real_rescan',text,4)
        check('real_rescan_cache_does_not_cross_epoch',count(text,'Qualification Probes')==512 and count(text,'Probe Cache Hits')==0 and 'Ann Index Scan' in text and 'loops=4' in text,text)
        for metric in ('l2','cos'):
            query=QUERY if metric=='l2' else QUERY.replace(' <-> ',' <=> ')
            check(metric+'_original_distance_result',sql(query)==sql(query,rc),query)
        sql("UPDATE vf_in.documents SET tenant_id=3 WHERE id=7")
        query=QUERY.replace('tenant_id=3','id=7')
        text=plan('hot_visible_tid',query)
        check('hot_visible_tid_scan',sql(query)==sql(query,rc) and 'Tid Scan' in text and count(text,'Accepted Members')==1,text)
        sql('BEGIN ISOLATION LEVEL REPEATABLE READ')
        old=sql(QUERY,cur)
        writer=connect()
        with writer.cursor() as wc:
            sql("UPDATE vf_in.documents SET tenant_id=4 WHERE id=3",wc)
        writer.close()
        check('in_statement_snapshot_retained',sql(QUERY)==old,'same repeatable-read statement snapshot with external committed update')
        sql('COMMIT')
        check('new_epoch_new_snapshot',sql(QUERY)==sql(QUERY,rc),'new execution sees committed qualification change')
        sql('SET diskann_filter_mode=exact')
        sql("CREATE OR REPLACE FUNCTION vf_in.nested_check(int) RETURNS boolean LANGUAGE plpgsql STABLE AS $$ DECLARE eligible boolean; BEGIN SELECT tenant_id=3 INTO eligible FROM vf_in.documents WHERE id=$1; RETURN eligible; END $$")
        sql('SET diskann_filter_mode=\'in\'')
        nested=QUERY.replace('tenant_id=3','vf_in.nested_check(id)').replace('LIMIT 10','LIMIT 1000')
        text=plan('nested_same_table_read',nested)
        check('nested_probe_same_table_read',sql(nested)==sql(nested,rc) and 'nested_check' in text and 'Tid Scan' in text,text)
        reconcile('nested_probe',text)
        # Native legacy duplicate overflow can retain the sampled frozen TID
        # in another physical node. A fixed seed and small build beam construct that actual
        # condition without changing pages or adding any debug-only callback.
        sql('SET diskann_filter_mode=exact')
        sql('DROP TABLE IF EXISTS vf_in.duplicates')
        sql('CREATE TABLE vf_in.duplicates(id int PRIMARY KEY,tenant_id int NOT NULL,embedding vector(8) NOT NULL) WITH(storage_type=astore)')
        sql("INSERT INTO vf_in.duplicates SELECT i,i%5,'[9.5,1,2,3,4,5,6,7]'::vector FROM generate_series(1,240)i")
        sql('SELECT setseed(0.5)')
        sample=sql('SELECT floor(random()*2147483648)::bigint % 240')[0][0]
        sql('SELECT setseed(0.5)')
        sql('CREATE INDEX vf_in_duplicates_l2 ON vf_in.duplicates USING diskann(embedding vector_l2_ops) WITH(index_size=16)')
        sql('ANALYZE vf_in.duplicates')
        sql('SET diskann_filter_mode=\'in\'; SET diskann_filter_work_mem=32768; SET diskann_probes=512')
        duplicate_query=QUERY.replace('vf_in.documents','vf_in.duplicates').replace('LIMIT 10','LIMIT 1000')
        for label,predicate,size in [('cached_true','id>0',240),('cached_false','id<0',0),('member_qualification','tenant_id=1',48)]:
            query=duplicate_query.replace('tenant_id=3',predicate)
            text=plan(label,query)
            actual,expected=sql(query),sql(query,rc)
            check(label+'_all_member_rows',tie_groups(actual)==tie_groups(expected) and len(actual)==size and len({row[0] for row in actual})==size,dict(actual=actual,exact=expected))
            reconcile(label,text)
            check(label+'_real_same_epoch_cache_hit',count(text,'Probe Cache Hits')>0 and count(text,'Qualification Probes')==240 and count(text,'Candidate Callbacks')>240,dict(sample_position=sample,plan=text))
        sql('SET diskann_filter_work_mem=1')
        try:
            sql(duplicate_query)
        except psycopg2.Error as exc:
            check('in_cache_budget_failure',exc.pgcode=='54000' and 'diskann_filter_work_mem' in str(exc),str(exc))
        else:
            check('in_cache_budget_failure',False,'accepted incomplete query-owned memory budget')
        sql('SET diskann_filter_work_mem=0; SET diskann_probes=256')
        check('in_budget_same_connection_reuse',sql(QUERY)==sql(QUERY,rc),'fresh epoch after resource failure')
        sql('SET plan_cache_mode=force_generic_plan; SET diskann_filter_work_mem=32768')
        cached_body=QUERY.replace('tenant_id=3','tenant_id=$1').replace('LIMIT 10','LIMIT 1000')
        sql('PREPARE in_cached(int) AS '+cached_body)
        expected=sql(cached_body.replace('tenant_id=$1','tenant_id=3'),rc)
        check('generic_predicate_initial_rows',sql('EXECUTE in_cached(3)')==expected,'same prepared plan and WHERE parameter')
        sql('SET diskann_probes=2')
        text=plan('generic_budget_changed','EXECUTE in_cached(3)')
        check('generic_probes_change_replans',count(text,'Initial Search Nodes')==2 and count(text,'Search Rounds')==1 and len(sql('EXECUTE in_cached(3)'))<=2,text)
        sql('SET diskann_probes=256; SET diskann_filter_work_mem=1')
        try:
            sql('EXECUTE in_cached(3)')
        except psycopg2.Error as exc:
            check('generic_memory_change_replans',exc.pgcode=='54000',str(exc))
        else:
            check('generic_memory_change_replans',False,'cached plan retained the old 32MB resource limit')
        sql('SET diskann_filter_work_mem=0')
        check('generic_budget_restore_reuses_connection',sql('EXECUTE in_cached(3)')==expected,'original rows after budget restoration')
        sql('DEALLOCATE in_cached; SET plan_cache_mode=auto')
    finally:
        (args.output/'statements.json').write_text(json.dumps(statements,ensure_ascii=False,indent=2,default=str)+'\n')
        (args.output/'summary.json').write_text(json.dumps(dict(checks=checks,passed=sum(c['result']=='PASS' for c in checks),total=len(checks)),ensure_ascii=False,indent=2)+'\n')
        cur.close();rc.close();conn.close();reference.close()
    failed=[c['name'] for c in checks if c['result']!='PASS']
    print(json.dumps(dict(result='FAIL' if failed else 'PASS',checks=len(checks),failed=failed),ensure_ascii=False),flush=True)
    if failed:
        raise SystemExit(1)


if __name__=='__main__':
    main()
