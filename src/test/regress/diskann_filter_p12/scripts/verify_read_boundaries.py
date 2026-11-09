#!/usr/bin/env python3
"""Verify statement timeout cleanup and absence of query-created persistent state."""
import argparse
import json
from pathlib import Path
from verify_query_protocol import connect,QUERY


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args();args.output.mkdir(parents=True,exist_ok=False)
    conn=connect();cur=conn.cursor();checks,statements=[],[]
    def sql(q):
        cur.execute(q);rows=cur.fetchall() if cur.description else []
        statements.append(dict(sql=q,rows=rows));return rows
    def check(name,ok,detail):
        checks.append(dict(name=name,result='PASS' if ok else 'FAIL',details=detail))
        if not ok: print('FAIL '+name+': '+str(detail),flush=True)
    try:
        sql('SET enable_opfusion=off; SET enable_diskann_filter=on; SET diskann_filter_mode=exact')
        sql("CREATE OR REPLACE FUNCTION vf_protocol.slow_qual(int) RETURNS boolean LANGUAGE plpgsql STABLE AS $$ BEGIN PERFORM pg_sleep(0.05); RETURN $1=3; END $$")
        expected=sql(QUERY)
        catalog="SELECT c.oid,c.relname,c.relkind,c.relfilenode FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='vf_protocol' ORDER BY c.oid"
        before=sql(catalog)
        for mode in ('pre','in','post'):
            sql("SET diskann_filter_mode='"+mode+"'; SET diskann_probes=256; SET statement_timeout='15ms'")
            try: sql(QUERY.replace('tenant_id=3','vf_protocol.slow_qual(tenant_id)'))
            except Exception as exc:
                check(mode+'_statement_timeout',getattr(exc,'pgcode',None)=='57014' and 'statement timeout' in str(exc),str(exc))
            else: check(mode+'_statement_timeout',False,'timeout did not interrupt actual qualification')
            sql('SET statement_timeout=0')
            check(mode+'_same_connection_after_timeout',sql(QUERY)==expected,dict(rows=sql(QUERY),exact=expected))
            text='\n'.join(r[0] for r in sql('EXPLAIN (ANALYZE,COSTS OFF,TIMING OFF) '+QUERY))+'\n'
            (args.output/(mode+'.plan.txt')).write_text(text)
            check(mode+'_fresh_epoch_and_no_exact','Scan Epoch: 1' in text and 'Search Rounds: 1' in text and 'Executed Mode: '+mode.upper() in text and 'EXACT' not in text,text)
        check('read_scan_creates_no_catalog_or_relation_files',sql(catalog)==before,dict(before=before,after=sql(catalog)))
        sql('SET diskann_filter_mode=exact')
        check('ordinary_route_after_all_errors',sql(QUERY)==expected,expected)
    finally:
        (args.output/'statements.json').write_text(json.dumps(statements,ensure_ascii=False,indent=2)+'\n')
        (args.output/'summary.json').write_text(json.dumps(dict(checks=checks,passed=sum(c['result']=='PASS' for c in checks),total=len(checks)),ensure_ascii=False,indent=2)+'\n')
        cur.close();conn.close()
    failed=[c['name'] for c in checks if c['result']!='PASS']
    print(json.dumps(dict(result='FAIL' if failed else 'PASS',checks=len(checks),failed=failed),ensure_ascii=False),flush=True)
    if failed: raise SystemExit(1)
if __name__=='__main__': main()
