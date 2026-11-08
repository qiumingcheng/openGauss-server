#!/usr/bin/env python3
"""Verify real DiskANN query execution, fixed beam, errors and cancellation."""
import argparse
import getpass
import json
import os
import re
import threading
import time
from pathlib import Path
import psycopg2

SOCKET = os.environ.get("DISKANN_FILTER_HOST", os.environ.get("PGHOST", "/tmp"))
QUERY = "SELECT id, embedding <-> '[7.25,1,2,3,4,5,6,7]'::vector AS distance FROM vf_protocol.documents WHERE tenant_id=3 ORDER BY embedding <-> '[7.25,1,2,3,4,5,6,7]'::vector LIMIT 10"


def connect():
    conn = psycopg2.connect(host=os.environ.get("DISKANN_FILTER_HOST", SOCKET),
                           port=int(os.environ.get("DISKANN_FILTER_PORT", os.environ.get("PGPORT", "55450"))),
                           dbname=os.environ.get("DISKANN_FILTER_DATABASE", "vf_p12_baseline"),
                           user=os.environ.get("DISKANN_FILTER_USER", os.environ.get("PGUSER", getpass.getuser())))
    conn.autocommit = True
    return conn


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("output must not exist")
    args.output.mkdir(parents=True)
    checks = []
    conn = connect()
    cur = conn.cursor()

    def sql(statement):
        cur.execute(statement)
        return cur.fetchall() if cur.description else []

    def check(name, condition, details):
        checks.append(dict(name=name, result="PASS" if condition else "FAIL", details=details))
        if not condition:
            print("FAIL " + name + ": " + str(details), flush=True)

    def plan(label, query, analyze=True):
        prefix = "EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF) " if analyze else "EXPLAIN (COSTS OFF) "
        text = "\n".join(row[0] for row in sql(prefix + query)) + "\n"
        (args.output / (label + ".plan.txt")).write_text(text)
        return text

    def count(text, label):
        matched = re.search(r"^\s*" + re.escape(label) + r": (\d+)\s*$", text, re.M)
        return int(matched.group(1)) if matched else None

    def mode(value):
        sql("SET diskann_filter_mode='" + value + "'")

    try:
        sql("SET enable_diskann_filter=off")
        mode("auto")
        sql("SET enable_opfusion=off")
        sql("CREATE SCHEMA IF NOT EXISTS vf_protocol")
        sql("DROP TABLE IF EXISTS vf_protocol.documents")
        sql("CREATE TABLE vf_protocol.documents (id int PRIMARY KEY, tenant_id int NOT NULL, embedding vector(8) NOT NULL CHECK(vector_norm(embedding)>0)) WITH (storage_type=astore)")
        sql("INSERT INTO vf_protocol.documents SELECT i, i%7, ('['||i||',1,2,3,4,5,6,7]')::vector FROM generate_series(1,128) i")
        sql("CREATE INDEX vf_protocol_l2 ON vf_protocol.documents USING diskann (embedding vector_l2_ops)")
        sql("CREATE INDEX vf_protocol_cos ON vf_protocol.documents USING diskann (embedding vector_cosine_ops)")
        sql("CREATE INDEX vf_protocol_tenant ON vf_protocol.documents (tenant_id)")
        sql("ANALYZE vf_protocol.documents")
        sql("SET enable_diskann_filter=on")
        sql("SET diskann_probes=128")
        for metric, query in (("l2", QUERY), ("cos", QUERY.replace(" <-> ", " <=> "))):
            mode("exact")
            expected = sql(query)
            check(metric + "_reference_rows", len(expected) == 10, expected)
            for current in ("pre", "in", "post"):
                mode(current)
                actual = sql(query)
                check(metric + "_" + current + "_original_sql_result", actual == expected, dict(expected=expected, actual=actual))
                text = plan(metric + "_" + current, query)
                check(metric + "_" + current + "_one_search", count(text, "Search Rounds") == 1 and count(text, "Scan Epoch") == 1 and "Sort" in text and "Executed Mode: " + current.upper() in text, text)
                if current == "in":
                    probe_loops = re.search(r"Tid Scan.*loops=(\d+)\)", text)
                    check(metric + "_in_local_probe", probe_loops is not None and int(probe_loops.group(1)) == count(text, "Qualification Probes") and count(text, "Qualification Probes") > 0, text)
                else:
                    check(metric + "_" + current + "_no_sql_probe", count(text, "Qualification Probes") == 0 and "SubPlan" not in text, text)
        all_query = QUERY.replace("LIMIT 10", "LIMIT 100")
        mode("exact")
        expected_all = sql(all_query)
        for current in ("pre", "in", "post"):
            mode(current)
            actual = sql(all_query)
            check(current + "_group_boundary_is_not_eof", len(expected_all) == 18 and actual == expected_all, dict(expected=expected_all, actual=actual))
            empty = all_query.replace("tenant_id=3", "tenant_id=777")
            check(current + "_empty_results", sql(empty) == [], empty)
            text = plan(current + "_empty", empty)
            check(current + "_empty_enters_ann", count(text, "Search Rounds") == 1 and "Executed Mode: " + current.upper() in text, text)
            if current == "pre":
                check("empty_pre_source_eof", "actual rows=0 loops=1" in text and count(text, "AStore Fetch Attempts") == 0, text)
            sql("SET diskann_probes=2")
            limited = sql(all_query)
            text = plan(current + "_fixed_small_beam", all_query)
            check(current + "_no_automatic_expansion", len(limited) <= 2 and len(limited) < len(expected_all) and count(text, "Search Rounds") == 1 and "EXACT" not in text.upper(), dict(rows=limited, plan=text))
            sql("SET diskann_probes=128")
        # Volatility remains a capability gate. This immutable fixture raises
        # on a row-dependent argument; planning must finish before the error.
        mode("exact")
        sql("CREATE OR REPLACE FUNCTION vf_protocol.raise_if(int) RETURNS boolean LANGUAGE plpgsql IMMUTABLE AS $$ BEGIN IF $1>0 THEN RAISE EXCEPTION 'vf protocol probe failure'; END IF; RETURN TRUE; END $$")
        mode("in")
        fault = QUERY.replace("tenant_id=3", "tenant_id=3 AND vf_protocol.raise_if(id)")
        text = plan("in_fault_planning", fault, analyze=False)
        check("fault_has_real_probe", "SubPlan" in text and "raise_if" in text and "Tid Scan" in text, text)
        try:
            sql(fault)
        except psycopg2.Error as exc:
            check("probe_error_propagated", exc.pgcode == "P0001" and "vf protocol probe failure" in str(exc), dict(sqlstate=exc.pgcode, error=str(exc)))
        else:
            check("probe_error_propagated", False, "expected SQL error")
        check("connection_after_probe_error", sql("SELECT 1") == [(1,)] and sql(QUERY) == expected_all[:10], "connection reuse and full query result")
        # Cancellation through an expensive immutable, row-dependent filter.
        worker = connect()
        worker_cur = worker.cursor()
        worker_cur.execute("SET enable_opfusion=off; SET enable_diskann_filter=on; SET diskann_filter_mode='in'; SET diskann_probes=128")
        worker_cur.execute("SELECT pg_backend_pid()")
        backend_pid = worker_cur.fetchone()[0]
        slow = QUERY.replace("tenant_id=3", "length(md5(repeat(id::text,1000000)))=32")
        result = {}

        def execute_slow():
            try:
                worker_cur.execute(slow)
                result["rows"] = worker_cur.fetchall()
            except psycopg2.Error as exc:
                result.update(sqlstate=exc.pgcode, error=str(exc))

        thread = threading.Thread(target=execute_slow)
        thread.start()
        active = False
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline and thread.is_alive():
            observed = sql("SELECT state, query FROM pg_stat_activity WHERE pid=" + str(backend_pid))
            if observed and observed[0][0] == "active" and "repeat" in observed[0][1]:
                active = True
                break
            time.sleep(0.01)
        if active:
            worker.cancel()
        thread.join(timeout=10)
        check("in_cancel_original_sqlstate", active and not thread.is_alive() and result.get("sqlstate") == "57014", result)
        if not thread.is_alive():
            worker_cur.execute("SELECT 1")
            reused = worker_cur.fetchall() == [(1,)]
            worker_cur.execute(QUERY)
            check("in_cancel_connection_reusable", reused and worker_cur.fetchall() == expected_all[:10], "same connection after cancel")
            worker_cur.close()
            worker.close()
        mode("exact")
        sql("SET enable_diskann_filter=off")
        mode("auto")
        check("legacy_mode_isolated", sql(QUERY) == expected_all[:10], "default disabled legacy route")
    finally:
        (args.output / "summary.json").write_text(json.dumps(dict(checks=checks, passed=sum(c["result"]=="PASS" for c in checks), total=len(checks)), ensure_ascii=False, indent=2) + "\n")
        cur.close()
        conn.close()
    failed = [c["name"] for c in checks if c["result"] != "PASS"]
    print(json.dumps(dict(result="FAIL" if failed else "PASS", checks=len(checks), failed=failed, output=str(args.output)), ensure_ascii=False), flush=True)
    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
