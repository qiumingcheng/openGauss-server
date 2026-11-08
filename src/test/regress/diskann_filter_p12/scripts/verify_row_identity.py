#!/usr/bin/env python3
"""Check candidate identities against the same-snapshot ordinary SQL result."""
import argparse
import getpass
import itertools
import json
import os
import time
from pathlib import Path
import psycopg2

SOCKET = os.environ.get("DISKANN_FILTER_HOST", os.environ.get("PGHOST", "/tmp"))
QUERY = "SELECT id,tenant_id,payload,embedding::text,ctid::text,embedding <-> '[7.25,1,2,3,4,5,6,7]'::vector AS distance FROM vf_identity.documents WHERE tenant_id=1 ORDER BY embedding <-> '[7.25,1,2,3,4,5,6,7]'::vector LIMIT 1000"


def connect():
    conn = psycopg2.connect(host=os.environ.get("DISKANN_FILTER_HOST", SOCKET),
                           port=int(os.environ.get("DISKANN_FILTER_PORT", os.environ.get("PGPORT", "55450"))),
                           dbname=os.environ.get("DISKANN_FILTER_DATABASE", "vf_p12_baseline"),
                           user=os.environ.get("DISKANN_FILTER_USER", os.environ.get("PGUSER", getpass.getuser())))
    conn.autocommit = True
    return conn


def tie_groups(rows):
    # SQL has no tie-break. Preserve the distance sequence and every row
    # within each equal-distance group; the original SQL/LIMIT stays intact.
    return [(distance, sorted(list(group))) for distance, group in itertools.groupby(rows, key=lambda row: row[-1])]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("output must not exist")
    args.output.mkdir(parents=True)
    checks, statements = [], []
    conn = connect()
    cur = conn.cursor()

    def sql(statement, cursor=None):
        if cursor is None:
            cursor = cur
        cursor.execute(statement)
        rows = cursor.fetchall() if cursor.description else []
        statements.append(dict(sql=statement, rows=rows, affected_rows=cursor.rowcount))
        return rows

    def check(name, condition, details):
        checks.append(dict(name=name, result="PASS" if condition else "FAIL", details=details))
        if not condition:
            print("FAIL " + name + ": " + str(details), flush=True)

    def mode(value, cursor=None):
        sql("SET diskann_filter_mode='" + value + "'", cursor)

    def compare(label, query=QUERY, cursor=None):
        mode("exact", cursor)
        expected = sql(query, cursor)
        for current in ("pre", "in", "post"):
            mode(current, cursor)
            actual = sql(query, cursor)
            check(label + "_" + current, tie_groups(actual) == tie_groups(expected), dict(expected=expected, actual=actual))
        return expected

    try:
        sql("SET enable_diskann_filter=off")
        mode("auto")
        sql("SET enable_opfusion=off")
        sql("CREATE SCHEMA IF NOT EXISTS vf_identity")
        sql("DROP TABLE IF EXISTS vf_identity.documents")
        sql("CREATE TABLE vf_identity.documents (id int PRIMARY KEY,tenant_id int NOT NULL,payload text,embedding vector(8) NOT NULL CHECK(vector_norm(embedding)>0)) WITH(storage_type=astore,fillfactor=60)")
        sql("INSERT INTO vf_identity.documents SELECT i,i%3,'row-'||i,('['||i||',1,2,3,4,5,6,7]')::vector FROM generate_series(1,128) i")
        sql("INSERT INTO vf_identity.documents SELECT i,i%3,'duplicate-'||i,'[9.5,1,2,3,4,5,6,7]'::vector FROM generate_series(1001,1032) i")
        sql("CREATE INDEX vf_identity_l2 ON vf_identity.documents USING diskann(embedding vector_l2_ops)")
        sql("CREATE INDEX vf_identity_cos ON vf_identity.documents USING diskann(embedding vector_cosine_ops)")
        sql("ANALYZE vf_identity.documents")
        sql("SET enable_diskann_filter=on")
        sql("SET diskann_probes=512")
        duplicates = QUERY.replace("tenant_id=1", "tenant_id=1 AND id>=1001")
        expected = compare("same_vector_members", duplicates)
        check("all_distinct_members_retained", len(expected) == 10 and len({row[0] for row in expected}) == 10 and len({row[4] for row in expected}) == 10, expected)
        compare("cosine_canonical_key", duplicates.replace(" <-> ", " <=> "))
        # A separate writer flushes statistics on exit, so an UPDATE is
        # verified as HOT rather than inferred from the unchanged vector.
        before = sql("SELECT ctid::text FROM vf_identity.documents WHERE id=7")[0][0]
        writer = connect()
        with writer.cursor() as wc:
            sql("UPDATE vf_identity.documents SET tenant_id=2,payload='hot-seven' WHERE id=7", wc)
        writer.close()
        after = sql("SELECT ctid::text FROM vf_identity.documents WHERE id=7")[0][0]
        hot = 0
        for _ in range(30):
            sql("SELECT pg_stat_clear_snapshot()")
            hot = sql("SELECT pg_stat_get_tuples_hot_updated('vf_identity.documents'::regclass)")[0][0]
            if hot > 0:
                break
            time.sleep(0.1)
        check("actual_hot_update", before != after and hot > 0, dict(before=before, after=after, hot_updates=hot))
        compare("hot_qualification_removed")
        compare("hot_visible_leaf", QUERY.replace("tenant_id=1", "id=7"))
        mode("exact")
        sql("BEGIN")
        sql("UPDATE vf_identity.documents SET embedding='[1000,1,2,3,4,5,6,7]'::vector,payload='vector-B' WHERE id=7")
        compare("own_non_hot_vector_B", QUERY.replace("tenant_id=1", "id=7"))
        sql("SAVEPOINT vector_b")
        sql("UPDATE vf_identity.documents SET embedding='[7,1,2,3,4,5,6,7]'::vector,payload='vector-A-again' WHERE id=7")
        compare("own_vector_A_again", QUERY.replace("tenant_id=1", "id=7"))
        sql("ROLLBACK TO SAVEPOINT vector_b")
        compare("savepoint_restores_B", QUERY.replace("tenant_id=1", "id=7"))
        sql("ROLLBACK")
        compare("rollback_restores_original", QUERY.replace("tenant_id=1", "id=7"))
        # Keep the old row versions visible while another session commits
        # vector updates and deletes, then exercises VACUUM concurrently.
        snapshot = connect()
        with snapshot.cursor() as sc:
            sql("SET enable_diskann_filter=on; SET enable_opfusion=off; SET diskann_probes=512", sc)
            sql("BEGIN ISOLATION LEVEL REPEATABLE READ", sc)
            mode("exact", sc)
            old = sql(QUERY, sc)
            writer = connect()
            with writer.cursor() as wc:
                sql("UPDATE vf_identity.documents SET embedding='[500,1,2,3,4,5,6,7]'::vector,payload='committed-B' WHERE id=4", wc)
                sql("DELETE FROM vf_identity.documents WHERE id IN (10,13,1003)", wc)
                sql("VACUUM vf_identity.documents", wc)
            writer.close()
            retained = compare("long_snapshot_update_delete_vacuum", cursor=sc)
            check("long_snapshot_rows_unchanged", retained == old, dict(before=old, after=retained))
            sql("COMMIT", sc)
        snapshot.close()
        compare("new_snapshot_after_commit")
        # Remove rows, reclaim heap slots using the existing maintenance
        # protocol, then insert new entities with different vector keys.
        mode("exact")
        old_file = sql("SELECT pg_relation_filepath('vf_identity.documents')")[0][0]
        removed = sql("SELECT ctid::text FROM vf_identity.documents")
        sql("DELETE FROM vf_identity.documents")
        # Native deferred xmin calculation can keep just-deleted versions
        # nonremovable until its one-second calculation window advances.
        # Finish readers, then create a committed xid after that window;
        # this exercises normal maintenance without changing server GUCs.
        cur.close()
        conn.close()
        conn = connect()
        cur = conn.cursor()
        sql("SET enable_opfusion=off; SET enable_diskann_filter=on; SET diskann_probes=512")
        mode("exact")
        # The ring snapshot's local xmin includes the preceding snapshot.
        # Wait for normal xmin advancement rather than forcing reclamation.
        for _ in range(4):
            time.sleep(1.1)
            sql("SELECT txid_current()")
            sql("VACUUM VERBOSE vf_identity.documents")
            statements.append(dict(vacuum_notices=list(conn.notices)))
            conn.notices.clear()
            if sql("SELECT pg_relation_size('vf_identity.documents')")[0][0] == 0:
                break
        sql("INSERT INTO vf_identity.documents SELECT i,1,'new-entity-'||i,('['||(i+10000)||',1,2,3,4,5,6,7]')::vector FROM generate_series(2001,2160) i")
        new = sql("SELECT ctid::text FROM vf_identity.documents WHERE id BETWEEN 2001 AND 2160")
        new_file = sql("SELECT pg_relation_filepath('vf_identity.documents')")[0][0]
        overlap = sorted({row[0] for row in removed} & {row[0] for row in new})
        check("actual_tid_slot_reuse", len(overlap) > 0 and old_file == new_file, dict(reused_tids=overlap, old_file=old_file, new_file=new_file))
        compare("tid_reuse_new_entities")
        sql("REINDEX INDEX vf_identity.vf_identity_l2")
        compare("reindex_read_compatibility")
        mode("in")
        text = "\n".join(row[0] for row in sql("EXPLAIN (ANALYZE,COSTS OFF,TIMING OFF) " + QUERY)) + "\n"
        (args.output / "final-in.plan.txt").write_text(text)
        check("end_connection_usable", sql("SELECT 1") == [(1,)], "same connection after all transactions")
    finally:
        (args.output / "statements.json").write_text(json.dumps(statements, ensure_ascii=False, indent=2, default=str) + "\n")
        (args.output / "summary.json").write_text(json.dumps(dict(checks=checks, passed=sum(c['result']=='PASS' for c in checks), total=len(checks)), ensure_ascii=False, indent=2) + "\n")
        cur.close()
        conn.close()
    failed = [c['name'] for c in checks if c['result'] != 'PASS']
    print(json.dumps(dict(result='FAIL' if failed else 'PASS', checks=len(checks), failed=failed, output=str(args.output)), ensure_ascii=False), flush=True)
    if failed:
        raise SystemExit(1)


if __name__ == '__main__':
    main()
