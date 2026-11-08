#!/usr/bin/env python3
"""Verify PRE consumes real qualification sources fully before graph search."""
import argparse
import json
import re
import threading
import time
from pathlib import Path
import psycopg2
from verify_query_protocol import connect

QUERY = "SELECT id,embedding <-> '[7.25,1,2,3,4,5,6,7]'::vector FROM vf_pre.documents WHERE tenant_id=3 ORDER BY embedding <-> '[7.25,1,2,3,4,5,6,7]'::vector LIMIT 10"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("output must not exist")
    args.output.mkdir(parents=True)
    checks, statements = [], []
    conn, reference = connect(), connect()
    cur, rc = conn.cursor(), reference.cursor()

    def sql(statement, cursor=None):
        cursor = cur if cursor is None else cursor
        cursor.execute(statement)
        rows = cursor.fetchall() if cursor.description else []
        statements.append(dict(sql=statement, rows=rows))
        return rows

    def check(label, condition, details):
        checks.append(dict(name=label, result="PASS" if condition else "FAIL", details=details))
        if not condition:
            print("FAIL " + label + ": " + str(details), flush=True)

    def plan(label, query=QUERY, analyze=True):
        prefix = "EXPLAIN (ANALYZE,COSTS OFF,TIMING OFF) " if analyze else "EXPLAIN (COSTS OFF) "
        text = "\n".join(row[0] for row in sql(prefix + query)) + "\n"
        (args.output / (label + ".plan.txt")).write_text(text)
        return text

    def count(text, label):
        match = re.search(r"^\s*" + re.escape(label) + r": (\d+)\s*$", text, re.M)
        return int(match.group(1)) if match else None

    def source(kind):
        sql("SET enable_seqscan=" + ("on" if kind == "seq" else "off"))
        sql("SET enable_indexscan=" + ("on" if kind == "index" else "off"))
        sql("SET enable_bitmapscan=" + ("on" if kind == "bitmap" else "off"))
        sql("SET enable_tidscan=on")

    def reference_rows(query):
        return sql(query, rc)

    try:
        sql("SET enable_diskann_filter=off; SET diskann_filter_mode=auto; SET enable_opfusion=off")
        sql("SET enable_diskann_filter=on; SET diskann_filter_mode=exact; SET enable_opfusion=off", rc)
        sql("CREATE SCHEMA IF NOT EXISTS vf_pre")
        sql("DROP TABLE IF EXISTS vf_pre.documents")
        sql("CREATE TABLE vf_pre.documents(id int PRIMARY KEY,tenant_id int NOT NULL,embedding vector(8) NOT NULL) WITH(storage_type=astore)")
        sql("INSERT INTO vf_pre.documents SELECT i,i%7,('['||i||',1,2,3,4,5,6,7]')::vector FROM generate_series(1,128)i")
        sql("CREATE INDEX vf_pre_l2 ON vf_pre.documents USING diskann(embedding vector_l2_ops)")
        sql("CREATE INDEX vf_pre_tenant ON vf_pre.documents(tenant_id)")
        sql("ANALYZE vf_pre.documents")
        sql("SET enable_diskann_filter=on; SET diskann_filter_mode=pre; SET diskann_probes=256")
        expected = reference_rows(QUERY)
        for kind, marker in (("seq", "Seq Scan"), ("index", "Index Scan using vf_pre_tenant"), ("bitmap", "Bitmap Heap Scan")):
            source(kind)
            text = plan(kind)
            check(kind + "_real_source", marker in text and "Ann Index Scan" in text and "Sort" in text, text)
            check(kind + "_result", sql(QUERY) == expected, expected)
            check(kind + "_full_build", count(text, "PRE Build Runs") == 1 and count(text, "PRE Seals") == 1 and count(text, "PRE Source Rows") == 18 and count(text, "Search Rounds") == 1 and count(text, "Scan Epoch") == 1, text)
        source("seq")
        for label, predicate, size in (("empty", "id=777", 0), ("small", "id IN(3,10)", 2), ("equal_k", "id<=10", 10), ("large", "tenant_id=3", 18)):
            query = QUERY.replace("tenant_id=3", predicate)
            text = plan(label, query)
            check(label + "_source_and_search", count(text, "PRE Source Rows") == size and count(text, "PRE Seals") == 1 and count(text, "Search Rounds") == 1 and "Executed Mode: PRE" in text and "EXACT" not in text, text)
            check(label + "_sql_rows", sql(query) == reference_rows(query), query)
        # Runtime keys are initialized by the ordinary source after binding
        # its external parameter. Repeated EXECUTE is not an Ann ReScan.
        source("index")
        sql("SET plan_cache_mode=force_generic_plan")
        sql("SET plan_cache_mode=force_generic_plan", rc)
        body = QUERY.replace("tenant_id=3", "tenant_id=$1")
        sql("PREPARE pre_runtime(int) AS " + body)
        sql("PREPARE pre_runtime(int) AS " + body, rc)
        for value in (3,4,3,777,1):
            query = "EXECUTE pre_runtime(" + str(value) + ")"
            text = plan("runtime_" + str(value) + "_" + str(len(checks)), query)
            check("runtime_key_" + str(value), sql(query) == reference_rows(query) and count(text, "PRE Build Runs") == 1 and count(text, "PRE Seals") == 1, text)
        sql("DEALLOCATE pre_runtime")
        sql("DEALLOCATE pre_runtime", rc)
        sql("SET plan_cache_mode=auto")
        sql("SET plan_cache_mode=auto", rc)
        # Unlike separate EXECUTE statements, this scalar SubPlan reuses one
        # AnnIndexScanState across four outer bindings in the same execution.
        rescan = """SELECT x.ord,x.tenant,
          (SELECT array_agg(s.id) FROM
             (SELECT d.id FROM vf_pre.documents d
              WHERE d.tenant_id=x.tenant
              ORDER BY d.embedding <-> '[7.25,1,2,3,4,5,6,7]'::vector
              LIMIT 10) s) ids
          FROM (VALUES(1,3),(2,4),(3,3),(4,777)) x(ord,tenant)
          ORDER BY x.ord"""
        text = plan("real_rescan", rescan)
        rows, exact = sql(rescan), reference_rows(rescan)
        check("real_ann_rescan_results", rows == exact and rows[0][2] == rows[2][2] and rows[3][2] is None, dict(actual=rows, exact=exact))
        check("real_ann_rescan_cycles", "Ann Index Scan" in text and "loops=4" in text and count(text, "Search Rounds") == 4 and count(text, "PRE Build Runs") == 4 and count(text, "PRE Seals") == 4 and count(text, "PRE Source Rows") == 54, text)
        # Force actual lossy bitmap pages using a wide, PLAIN row heap.
        sql("SET diskann_filter_mode=exact")
        sql("DROP TABLE IF EXISTS vf_pre.lossy")
        sql("CREATE TABLE vf_pre.lossy(id int PRIMARY KEY,tenant_id int NOT NULL,payload text,embedding vector(8) NOT NULL) WITH(storage_type=astore)")
        sql("ALTER TABLE vf_pre.lossy ALTER COLUMN payload SET STORAGE PLAIN")
        sql("INSERT INTO vf_pre.lossy SELECT i,i%2,repeat('x',4000),('['||i||',1,2,3,4,5,6,7]')::vector FROM generate_series(1,2000)i")
        sql("CREATE INDEX vf_pre_lossy_l2 ON vf_pre.lossy USING diskann(embedding vector_l2_ops)")
        sql("CREATE INDEX vf_pre_lossy_tenant ON vf_pre.lossy(tenant_id)")
        sql("ANALYZE vf_pre.lossy")
        sql("SET diskann_filter_mode=pre; SET work_mem='64kB'; SET diskann_filter_work_mem=32768; SET diskann_probes=4096")
        source("bitmap")
        lossy = QUERY.replace("vf_pre.documents", "vf_pre.lossy").replace("tenant_id=3", "tenant_id=1 AND id%7=3")
        text = plan("lossy", lossy)
        pages = re.search(r"lossy=(\d+)", text)
        check("actual_lossy_bitmap", pages is not None and int(pages.group(1)) > 0 and "Recheck Cond" in text and "Bitmap Heap Scan" in text, text)
        size = sql("SELECT count(*) FROM vf_pre.lossy WHERE tenant_id=1 AND id%7=3", rc)[0][0]
        check("lossy_exact_membership", sql(lossy) == reference_rows(lossy) and count(text, "PRE Source Rows") == size and count(text, "PRE Seals") == 1, text)
        combined = lossy.replace("id%7=3", "id BETWEEN 500 AND 1500")
        text = plan("bitmap_and", combined)
        check("owned_bitmap_and", "BitmapAnd" in text and sql(combined) == reference_rows(combined), text)
        disjunction = lossy.replace("tenant_id=1 AND id%7=3", "(tenant_id=1 OR id BETWEEN 500 AND 520) AND id%7=3")
        text = plan("bitmap_or", disjunction)
        check("owned_bitmap_or", "BitmapOr" in text and sql(disjunction) == reference_rows(disjunction), text)
        # Resource failure occurs while building, rather than sealing a
        # partial set or switching to Exact; the same connection recovers.
        source("seq")
        sql("SET work_mem='4MB'; SET diskann_probes=256; SET diskann_filter_work_mem=1")
        try:
            sql(QUERY)
        except psycopg2.Error as exc:
            check("pre_budget_error", exc.pgcode == '54000' and 'diskann_filter_work_mem' in str(exc), dict(sqlstate=exc.pgcode, error=str(exc)))
        else:
            check("pre_budget_error", False, "query accepted incomplete budget")
        sql("SET diskann_filter_work_mem=0")
        check("pre_budget_connection_reuse", sql("SELECT 1") == [(1,)] and sql(QUERY) == expected, "same connection")
        sql("SET diskann_filter_mode=exact")
        sql("CREATE OR REPLACE FUNCTION vf_pre.fail_last(int) RETURNS boolean LANGUAGE plpgsql IMMUTABLE AS $$ BEGIN IF $1=128 THEN RAISE EXCEPTION 'pre source incomplete'; END IF; RETURN TRUE; END $$")
        sql("SET diskann_filter_mode=pre")
        fault = QUERY.replace("tenant_id=3", "vf_pre.fail_last(id)")
        text = plan("source_error", fault, analyze=False)
        check("source_fault_real_plan", "Seq Scan" in text and "fail_last" in text, text)
        try:
            sql(fault)
        except psycopg2.Error as exc:
            check("pre_source_error_propagated", exc.pgcode == 'P0001' and 'pre source incomplete' in str(exc), str(exc))
        else:
            check("pre_source_error_propagated", False, "incomplete source was accepted")
        check("pre_source_error_reuse", sql(QUERY) == expected, "new PRE on same connection")
        worker = connect()
        wc = worker.cursor()
        wc.execute("SET enable_opfusion=off; SET enable_diskann_filter=on; SET diskann_filter_mode=pre; SET diskann_probes=256; SET enable_seqscan=on; SET enable_indexscan=off; SET enable_bitmapscan=off")
        wc.execute("SELECT pg_backend_pid()")
        pid = wc.fetchone()[0]
        slow = QUERY.replace("tenant_id=3", "length(md5(repeat(id::text,1000000)))=32")
        outcome = {}

        def run():
            try:
                wc.execute(slow)
                outcome['rows'] = wc.fetchall()
            except psycopg2.Error as exc:
                outcome.update(sqlstate=exc.pgcode, error=str(exc))

        thread = threading.Thread(target=run)
        thread.start()
        active = False
        deadline = time.monotonic()+5
        while time.monotonic()<deadline and thread.is_alive():
            observed = sql("SELECT state,query FROM pg_stat_activity WHERE pid="+str(pid))
            if observed and observed[0][0]=='active' and 'repeat' in observed[0][1]:
                active = True
                break
            time.sleep(0.01)
        if active:
            worker.cancel()
        thread.join(timeout=10)
        check("pre_build_cancel", active and not thread.is_alive() and outcome.get('sqlstate')=='57014', outcome)
        if not thread.is_alive():
            wc.execute(QUERY)
            check("pre_cancel_connection_reuse", wc.fetchall()==expected, "same worker connection")
            wc.close()
            worker.close()
    finally:
        (args.output/'statements.json').write_text(json.dumps(statements, ensure_ascii=False, indent=2, default=str)+'\n')
        (args.output/'summary.json').write_text(json.dumps(dict(checks=checks,passed=sum(c['result']=='PASS' for c in checks),total=len(checks)),ensure_ascii=False,indent=2)+'\n')
        cur.close()
        rc.close()
        conn.close()
        reference.close()
    failed = [c['name'] for c in checks if c['result']!='PASS']
    print(json.dumps(dict(result='FAIL' if failed else 'PASS',checks=len(checks),failed=failed,output=str(args.output)),ensure_ascii=False),flush=True)
    if failed:
        raise SystemExit(1)


if __name__=='__main__':
    main()
