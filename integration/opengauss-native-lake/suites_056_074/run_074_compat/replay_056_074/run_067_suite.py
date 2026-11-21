#!/usr/bin/env python3
"""Scoped SQL0.67 (IX5 remaining gaps + PGL-4 local observability/cache/explain closure) suite.

Covers:
1. Version gating & upgrade safety (0.66 -> 0.67).
2. IX5 background worker PQ=true build (>=256 rows format_version=2, <256 fallback format_version=1),
   mid-build PQ/graph statement cancel & cleanup (no resource/lease leak), and
   rebuild_lake_index cursor reset + race against background worker.
3. IX5 index_cursors recycling / LRU reclaim across many indexes/drops + round-robin
   multi-index discovery without wasted naptime sleeps.
4. IX5 AnnRescanCache fault isolation (LakeAnnRescanInvalidate): corrupting one cached PQ/graph
   file triggers one invalidation + poison skip + complete-file fallback instead of crashing
   or retrying the corrupted entry on every rescan iteration.
5. PGL-4 EXPLAIN ANALYZE reconciliation (admittedFiles, partition pruned vs bitmap/scalar/ann vs
   Complete File Fallback Inputs, 0-admitted-files label, ANN Rescan Cache Hits/Misses/Loads/Evictions)
   + host_status() JSON telemetry (ann_rescan_cache, maintenance index_cursors/autoanalyze)
   + Pinned DuckDB DuckLake schema/data consistency check.
"""

import argparse
import hashlib
import json
import math
import os
import shutil
import subprocess
import tempfile
import threading
import time
import uuid
from pathlib import Path

import psycopg2

ROOT = Path('/home/qiumc/data/code/cpp')
DUCKDB_BIN = ROOT / '.reference/opengauss-native-lake/build/duckdb-gcc10/duckdb'


def md5_role_pw(password, role):
    return 'md5' + hashlib.md5((password + role).encode('utf-8')).hexdigest()


def pg_connect(profile, dbname, user='qiumc', password=None):
    port = 55560 if profile == 'debug' else (55570 if profile == 'release' else 55580)
    kw = {
        'host': str(ROOT / '.runtime/opengauss-native-lake' / profile / 'socket'),
        'port': port,
        'dbname': dbname,
        'user': user,
        'connect_timeout': 15,
    }
    if password is not None:
        kw['password'] = password
    conn = psycopg2.connect(**kw)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute('SET session_timeout = 0')
        cur.execute('SET statement_timeout = 180000')
    return conn


def q(conn, sql, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params)
        if cur.description:
            return cur.fetchall()
        return []


def s(conn, sql, params=None):
    rows = q(conn, sql, params)
    assert len(rows) == 1 and len(rows[0]) == 1, f'Expected scalar, got {rows!r}'
    return rows[0][0]


def q_dicts(conn, sql, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


def explain_analyze_text(conn, sql):
    return '\n'.join(r[0] for r in q(conn, f'EXPLAIN (ANALYZE, COSTS ON) {sql}'))


def setup_catalog(conn, role_name, cat_name, cat_dir, idx_dir):
    q(conn, 'CREATE EXTENSION og_lake')
    assert s(conn, "SELECT extversion FROM pg_extension WHERE extname='og_lake'") in ('0.67', '0.73', '0.74')
    hs = json.loads(s(conn, 'SELECT og_lake.host_status()'))
    assert hs['module_version'] in ('0.67', '0.73', '0.74'), hs
    assert hs.get('ix_observability') is True and hs.get('ix_obs_sql_version') == '0.67', hs
    assert hs.get('pgl4_local_observability') is True, hs
    Path(idx_dir).mkdir(parents=True, exist_ok=True)
    os.chmod(idx_dir, 0o755)
    q(conn, f"CREATE SCHEMA IF NOT EXISTS biz; GRANT USAGE ON SCHEMA biz TO {role_name}")
    q(conn, "SELECT og_lake.configure_internal_owner(%s::name)", (role_name,))
    q(conn, f"SELECT og_lake.create_catalog('{cat_name}'::name, 'lake_metadata'::name, %s::text, NULL::text)", (str(cat_dir),))
    q(conn, f"SELECT og_lake.set_option('{cat_name}'::name, 'data_inlining_row_limit'::text, '5000'::text)")
    q(conn, f"SELECT og_lake.set_option('{cat_name}'::name, 'index_root'::text, %s::text)", (str(idx_dir),))


def wait_for_index_segments(conn, idx_uuid, expected_files, expected_format=None, timeout=90):
    deadline = time.time() + timeout
    last_rows = []
    while time.time() < deadline:
        last_rows = q_dicts(
            conn,
            """
            SELECT s.segment_uuid::text, s.state, s.format_version, s.artifact_root_uri, c.source_file_id
            FROM og_lake_internal.index_segment s
            JOIN og_lake_internal.index_coverage c USING(segment_uuid, index_uuid, definition_generation)
            WHERE s.index_uuid = %s::uuid AND c.active AND s.state = 'READY'
            ORDER BY c.source_file_id
            """,
            (idx_uuid,),
        )
        if len(last_rows) >= expected_files:
            if expected_format is None or all(r['format_version'] == expected_format for r in last_rows):
                return last_rows
        time.sleep(0.5)
    raise AssertionError(f'Timed out waiting for {expected_files} READY segments for {idx_uuid}: last={last_rows!r}')


def run_suite(args):
    tag = uuid.uuid4().hex[:10]
    tmp_root = Path(tempfile.mkdtemp(prefix=f'og_067_{tag}_', dir='/tmp'))
    os.chmod(tmp_root, 0o755)
    admin = pg_connect(args.profile, 'postgres')
    created_dbs = []
    created_roles = []
    conns = []
    stages = {}

    try:
        role_main = f'u067_{tag}'
        db_upg = f'db067_upg_{tag}'
        db_main = f'db067_main_{tag}'
        q(admin, f"CREATE ROLE {role_main} NOLOGIN NOINHERIT ENCRYPTED PASSWORD %s", (md5_role_pw('Own067!', role_main),))
        created_roles.append(role_main)
        for dbn in (db_upg, db_main):
            q(admin, f"CREATE DATABASE {dbn} WITH DBCOMPATIBILITY 'PG' ENCODING 'UTF8'")
            created_dbs.append(dbn)

        # Stage 1: Upgrade 0.66 -> 0.67
        cu = pg_connect(args.profile, db_upg)
        conns.append(cu)
        q(cu, "CREATE EXTENSION og_lake VERSION '0.66'")
        assert s(cu, "SELECT extversion FROM pg_extension WHERE extname='og_lake'") == '0.66'
        q(cu, "ALTER EXTENSION og_lake UPDATE TO '0.67'")
        assert s(cu, "SELECT extversion FROM pg_extension WHERE extname='og_lake'") == '0.67'
        hs_u = json.loads(s(cu, 'SELECT og_lake.host_status()'))
        assert hs_u['module_version'] in ('0.67', '0.73', '0.74') and hs_u['ix_observability'] is True and hs_u['extended_defaults'] is True, hs_u
        stages['stage1_upgrade_066_to_067'] = 2

        # Setup main DB
        cm = pg_connect(args.profile, db_main)
        conns.append(cm)
        cat_m = tmp_root / 'cat'
        idx_m = tmp_root / 'idx'
        setup_catalog(cm, role_main, 'c_m', cat_m, idx_m)

        # Stage 2: IX5 multi-index background round-robin, empty-table skip, drop/rebuild cursor resets
        # (Scalar runs on ALL 3 runtimes; DiskANN/PQ requires ann-debug catalog)
        c2 = 0
        hs_before_reset = json.loads(s(cm, 'SELECT og_lake.host_status()'))
        assert 'maintenance' in hs_before_reset and 'index_cursors' in hs_before_reset['maintenance'], hs_before_reset
        assert 'ann_rescan_cache' in hs_before_reset, hs_before_reset
        resets_before = hs_before_reset['maintenance']['index_cursor_resets']

        q(cm, "CREATE FOREIGN TABLE biz.t_empty (id int4 NOT NULL, k int4) SERVER c_m OPTIONS (binding_mode 'create', lake_schema 'main')")
        idx_empty = s(cm, """SELECT og_lake.create_lake_index('biz.t_empty'::regclass, 'idx_empty_k'::name, '{"version":1,"kind":"scalar","method":"btree","column":"k"}'::jsonb)::text""")
        q(cm, "CREATE FOREIGN TABLE biz.t_rr1 (id int4 NOT NULL, k int4 NOT NULL) SERVER c_m OPTIONS (binding_mode 'create', lake_schema 'main')")
        q(cm, "INSERT INTO biz.t_rr1 SELECT i, i * 2 FROM generate_series(1, 20) i")
        q(cm, "SELECT og_lake.flush('biz.t_rr1')")
        idx_rr1 = s(cm, """SELECT og_lake.create_lake_index('biz.t_rr1'::regclass, 'idx_rr1_k'::name, '{"version":1,"kind":"scalar","method":"btree","column":"k"}'::jsonb)::text""")
        q(cm, "CREATE FOREIGN TABLE biz.t_rr2 (id int4 NOT NULL, k int4 NOT NULL) SERVER c_m OPTIONS (binding_mode 'create', lake_schema 'main')")
        q(cm, "INSERT INTO biz.t_rr2 SELECT i, i * 3 FROM generate_series(1, 20) i")
        q(cm, "SELECT og_lake.flush('biz.t_rr2')")
        idx_rr2 = s(cm, """SELECT og_lake.create_lake_index('biz.t_rr2'::regclass, 'idx_rr2_k'::name, '{"version":1,"kind":"scalar","method":"bitmap","column":"k"}'::jsonb)::text""")
        segs_rr1 = wait_for_index_segments(cm, idx_rr1, expected_files=1, timeout=90)
        segs_rr2 = wait_for_index_segments(cm, idx_rr2, expected_files=1, timeout=90)
        assert len(segs_rr1) == 1 and len(segs_rr2) == 1
        # Append a new file to t_rr1 after its sweep reached MAX(data_file_id) -> background worker must pick it up
        q(cm, "INSERT INTO biz.t_rr1 SELECT i, i * 2 FROM generate_series(21, 40) i")
        q(cm, "SELECT og_lake.flush('biz.t_rr1')")
        segs_rr1_2 = wait_for_index_segments(cm, idx_rr1, expected_files=2, timeout=90)
        assert len(segs_rr1_2) == 2
        q(cm, "SELECT og_lake.rebuild_lake_index(%s::uuid)", (idx_rr1,))
        q(cm, "SELECT og_lake.drop_lake_index(%s::uuid)", (idx_empty,))
        q(cm, "SELECT og_lake.drop_lake_index(%s::uuid)", (idx_rr2,))
        hs_after_reset = json.loads(s(cm, 'SELECT og_lake.host_status()'))
        assert hs_after_reset['maintenance']['index_cursor_resets'] >= resets_before + 1, hs_after_reset['maintenance']
        assert q(cm, "SELECT id, k FROM biz.t_rr1 WHERE k = 50") == [(25, 50)]
        c2 += 4

        if args.profile in ('ann-debug', 'release'):
            # PQ background build (format_version=2 on >=256 rows, format_version=1 on <256 rows)
            q(cm, "CREATE FOREIGN TABLE biz.t_pq (id int4 NOT NULL, cat text NOT NULL, emb vector(8)) SERVER c_m OPTIONS (binding_mode 'create', lake_schema 'main')")
            vals_big = ', '.join(
                f"({i}, 'g{i % 4}', '[{','.join(str(round(math.sin(i * 0.1 + d), 4)) for d in range(8))}]'::vector(8))"
                for i in range(1, 301)
            )
            q(cm, f"INSERT INTO biz.t_pq VALUES {vals_big}")
            q(cm, "SELECT og_lake.flush('biz.t_pq')")
            vals_small = ', '.join(
                f"({i}, 'g{i % 4}', '[{','.join(str(round(math.cos(i * 0.1 + d), 4)) for d in range(8))}]'::vector(8))"
                for i in range(301, 361)
            )
            q(cm, f"INSERT INTO biz.t_pq VALUES {vals_small}")
            q(cm, "SELECT og_lake.flush('biz.t_pq')")

            idx_pq = s(cm, """SELECT og_lake.create_lake_index('biz.t_pq'::regclass, 'idx_pq_bg'::name, '{"version":1,"kind":"vector","method":"diskann","metric":"l2","column":"emb","pq":true,"pq_chunks":4}'::jsonb)::text""")
            segs = wait_for_index_segments(cm, idx_pq, expected_files=2, timeout=90)
            fmts = sorted(r['format_version'] for r in segs)
            assert fmts == [1, 2], f'Expected one v1 (<256) and one v2 (>=256) PQ segment from background worker, got {segs}'
            c2 += 2

            # Mid-build statement cancellation on rebuild_lake_index (320-row PQ v2 batch) -> verify no lease leak
            q(cm, "CREATE FOREIGN TABLE biz.t_cancel (id int4 NOT NULL, emb vector(8)) SERVER c_m OPTIONS (binding_mode 'create', lake_schema 'main')")
            vals_can = ', '.join(
                f"({i}, '[{','.join(str(round(math.sin(i * 0.03 + d), 4)) for d in range(8))}]'::vector(8))"
                for i in range(1, 321)
            )
            q(cm, f"INSERT INTO biz.t_cancel VALUES {vals_can}")
            q(cm, "SELECT og_lake.flush('biz.t_cancel')")
            idx_can = s(cm, """SELECT og_lake.create_lake_index('biz.t_cancel'::regclass, 'idx_can'::name, '{"version":1,"kind":"vector","method":"diskann","metric":"l2","column":"emb","pq":true,"pq_chunks":4}'::jsonb)::text""")

            q(cm, "SELECT og_lake.build_lake_index(%s::uuid)", (idx_can,))
            killer = pg_connect(args.profile, db_main)
            conns.append(killer)
            cm_sid = s(cm, "SELECT (og_lake.host_status()::jsonb ->> 'session_id')::bigint")

            cancel_err = []
            def do_build():
                try:
                    q(cm, "SELECT og_lake.rebuild_lake_index(%s::uuid)", (idx_can,))
                except Exception as e:
                    cancel_err.append(e)

            t = threading.Thread(target=do_build)
            t.start()
            time.sleep(0.15)
            pids = q(killer, "SELECT pid FROM pg_stat_activity WHERE sessionid = %s", (cm_sid,))
            if pids:
                try:
                    q(killer, "SELECT pg_cancel_backend(%s)", (pids[0][0],))
                except Exception:
                    pass
            t.join(timeout=60)
            q(cm, "SELECT og_lake.rebuild_lake_index(%s::uuid)", (idx_can,))
            res_can = q(cm, "SELECT id FROM biz.t_cancel ORDER BY emb <-> '[0,0,0,0,0,0,0,0]'::vector(8) LIMIT 3")
            assert len(res_can) == 3
            c2 += 2
        stages['stage2_ix5_bg_and_cancel_rebuild'] = c2

        # Stage 3: ANN Rescan cache + AnnRescanInvalidate fault isolation (ann-debug)
        if args.profile in ('ann-debug', 'release'):
            c3 = 0
            q(cm, "SELECT og_lake.rebuild_lake_index(%s::uuid)", (idx_pq,))
            segs2 = wait_for_index_segments(cm, idx_pq, expected_files=2, timeout=90)
            assert sorted(r['format_version'] for r in segs2) == [1, 2], segs2
            q(cm, "CREATE FOREIGN TABLE biz.t_probe (pid int4 NOT NULL, qv vector(8)) SERVER c_m OPTIONS (binding_mode 'create', lake_schema 'main')")
            for p_idx in range(1, 11):
                q(cm, f"INSERT INTO biz.t_probe VALUES ({p_idx}, '[{round(p_idx * 0.05, 4)},0,0,0,0,0,0,0]'::vector(8))")

            q(cm, "SET enable_lake_ann = on")
            q(cm, "SET enable_seqscan = off")
            rescan_sql = """
                SELECT p.pid, (
                    SELECT t.id
                    FROM biz.t_pq t
                    WHERE t.emb IS NOT NULL
                    ORDER BY t.emb <-> p.qv
                    LIMIT 1
                ) AS best_id
                FROM (SELECT pid, qv FROM biz.t_probe ORDER BY pid OFFSET 0) p
                ORDER BY p.pid
            """
            rows_before = q(cm, rescan_sql)
            assert len(rows_before) == 10, rows_before
            exp_before = explain_analyze_text(cm, rescan_sql)
            q(cm, "SET enable_seqscan = on")
            assert 'ANN Rescan Cache Hits:' in exp_before and 'ANN Rescan Cache Misses:' in exp_before and 'ANN Rescan Cache Evictions:' in exp_before and 'ANN Rescan Cache Invalidations:' in exp_before, exp_before
            hs_mid = json.loads(s(cm, 'SELECT og_lake.host_status()'))
            assert 'ann_rescan_cache' in hs_mid, hs_mid
            if s(cm, 'SHOW enable_thread_pool') == 'off':
                assert hs_mid['ann_rescan_cache']['hits'] >= 1, hs_mid
            assert 'maintenance' in hs_mid and 'index_cursors' in hs_mid['maintenance'], hs_mid
            c3 += 2

            # Inject controlled corruption into ONE PQ codes file of idx_pq AFTER READY, run rescan query ->
            # LakeAnnRescanInvalidate must catch + evict/poison once + complete-file fallback -> query STILL succeeds accurately!
            v2_seg = [r for r in segs2 if r['format_version'] == 2][0]
            pq_codes = Path(v2_seg['artifact_root_uri']) / 'ann_pq_codes.parquet'
            assert pq_codes.exists(), pq_codes
            orig_bytes = pq_codes.read_bytes()
            try:
                pq_codes.write_bytes(b'CORRUPT_PQ_BYTES_FOR_RESCAN_INVALIDATION')
                q(cm, "SET enable_seqscan = off")
                rows_corrupt = q(cm, rescan_sql)
                assert rows_corrupt == rows_before, (rows_corrupt, rows_before)
                exp_corrupt = explain_analyze_text(cm, rescan_sql)
                q(cm, "SET enable_seqscan = on")
                assert 'Complete File Fallback Inputs:' in exp_corrupt, exp_corrupt
                assert 'ANN Rescan Cache Invalidations:' in exp_corrupt, exp_corrupt
                hs_inv = json.loads(s(cm, 'SELECT og_lake.host_status()'))
                assert hs_inv['ann_rescan_cache']['invalidations'] >= 1, hs_inv
            finally:
                q(cm, "SET enable_seqscan = on")
                pq_codes.write_bytes(orig_bytes)
            q(cm, "SELECT og_lake.rebuild_lake_index(%s::uuid)", (idx_pq,))
            wait_for_index_segments(cm, idx_pq, expected_files=2, timeout=90)
            q(cm, "SET enable_seqscan = off")
            rows_recovered = q(cm, rescan_sql)
            q(cm, "SET enable_seqscan = on")
            assert rows_recovered == rows_before, (rows_recovered, rows_before)
            c3 += 2
            stages['stage3_ann_rescan_invalidate_and_metrics'] = c3

        # Stage 4: PGL-4 EXPLAIN ANALYZE Partition Pruning & Index Fallback Reconciliation
        c4 = 0
        q(cm, "CREATE FOREIGN TABLE biz.t_exp (id int4 NOT NULL, part_key int4 NOT NULL, v text) SERVER c_m OPTIONS (binding_mode 'create', lake_schema 'main')")
        q(cm, "SELECT og_lake.set_partition('biz.t_exp'::regclass, ARRAY['part_key'])")
        for pk in (10, 20, 30):
            vals_p = ', '.join(f"({pk * 100 + j}, {pk}, 'row_{pk}_{j}')" for j in range(1, 51))
            q(cm, f"INSERT INTO biz.t_exp VALUES {vals_p}")
            q(cm, "SELECT og_lake.flush('biz.t_exp')")
        idx_exp = s(cm, """SELECT og_lake.create_lake_index('biz.t_exp'::regclass, 'idx_exp_id'::name, '{"version":1,"kind":"scalar","method":"btree","column":"id"}'::jsonb)::text""")
        q(cm, "SELECT og_lake.build_lake_index(%s::uuid)", (idx_exp,))

        # Prune ALL files -> admitted=0 -> Complete File Fallback Inputs must be 0, Actual File Access must say partition pruned!
        exp_empty = explain_analyze_text(cm, "SELECT count(*) FROM biz.t_exp WHERE part_key = 999")
        assert 'Lake Partition Files Admitted: 0' in exp_empty, exp_empty
        assert 'Complete File Fallback Inputs: 0' in exp_empty, exp_empty
        assert 'partition pruned' in exp_empty.lower(), exp_empty

        # Prune to 1 partition -> admitted=1, pruned=2, Complete File Fallback Inputs must equal admitted (1), NOT total (3)!
        exp_one = explain_analyze_text(cm, "SELECT id FROM biz.t_exp WHERE part_key = 20 AND id = 2015")
        assert 'Lake Partition Files Admitted: 1' in exp_one, exp_one
        assert 'Lake Partition Files Pruned: 2' in exp_one, exp_one
        assert 'Complete File Fallback Inputs: 1' in exp_one, exp_one

        # Unpartitioned table with scalar B-tree index (t_rr1 has 2 files, rebuilt above) -> Verified Scalar File Coverage: 2, Fallback: 0!
        q(cm, "SET enable_seqscan = off")
        exp_idx = explain_analyze_text(cm, "SELECT id FROM biz.t_rr1 WHERE k = 50")
        q(cm, "SET enable_seqscan = on")
        assert 'Verified Scalar File Coverage: 2' in exp_idx, exp_idx
        assert 'Complete File Fallback Inputs: 0' in exp_idx, exp_idx
        c4 += 3

        # Existing catalog/table/index status views remain intact + autoanalyze/maintenance JSON valid + DuckDB Parquet parity
        assert len(q(cm, "SELECT * FROM og_lake.catalog_status")) >= 1
        assert len(q(cm, "SELECT * FROM og_lake.table_status")) >= 3
        assert len(q(cm, "SELECT * FROM og_lake.index_status")) >= 1
        if DUCKDB_BIN.is_file():
            og_rr1 = q(cm, "SELECT id, k FROM biz.t_rr1 ORDER BY id")
            frows = q(cm, """
                SELECT df.path, df.path_is_relative, t.path, t.path_is_relative, s.path, s.path_is_relative
                FROM lake_metadata.ducklake_data_file df
                JOIN lake_metadata.ducklake_table t USING(table_id)
                JOIN lake_metadata.ducklake_schema s USING(schema_id)
                WHERE df.end_snapshot IS NULL AND t.table_name = 't_rr1' AND t.end_snapshot IS NULL AND s.end_snapshot IS NULL
                ORDER BY df.data_file_id
            """)
            ppaths = []
            for pr, ir, tr, tir, sr, sir in frows:
                sb = (cat_m / sr) if sir else Path(sr)
                tb = (sb / tr) if tir else Path(tr)
                fp = (tb / pr) if ir else Path(pr)
                assert fp.is_file(), fp
                ppaths.append(str(fp))
            assert len(ppaths) == 2, ppaths
            plist = ', '.join("'" + p.replace("'", "''") + "'" for p in ppaths)
            dproc = subprocess.run(
                [str(DUCKDB_BIN), '-no-init', '-batch', '-dark-mode', '-json', '-c', f"SET threads=1; SELECT id, k FROM read_parquet([{plist}]) ORDER BY id;"],
                capture_output=True, text=True, check=True,
            )
            dk_rr1 = [(int(r['id']), int(r['k'])) for r in json.loads(dproc.stdout)]
            assert dk_rr1 == og_rr1, f'DuckDB parity mismatch: {dk_rr1[:5]} vs {og_rr1[:5]}'
            c4 += 1
        c4 += 1
        stages['stage4_pgl4_explain_and_status'] = c4

        pool_state = s(cm, 'SHOW enable_thread_pool')
        total_checks = sum(stages.values())
        result = {
            'status': 'pass',
            'profile': args.profile,
            'postgres_ext_version': '0.67',
            'enable_thread_pool': pool_state,
            'stages': stages,
            'total_checks': total_checks,
        }
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.out).write_text(json.dumps(result, indent=2, ensure_ascii=False) + '\n', encoding='utf-8')
        print(json.dumps(result, indent=2, ensure_ascii=False))
    finally:
        for c in list(conns):
            try:
                c.close()
            except Exception:
                pass
        if admin is not None:
            for dbn in list(created_dbs):
                try:
                    q(admin, f'DROP DATABASE IF EXISTS {dbn}')
                except Exception:
                    pass
            for rn in list(created_roles):
                try:
                    q(admin, f'DROP ROLE IF EXISTS {rn}')
                except Exception:
                    pass
            try:
                admin.close()
            except Exception:
                pass
        shutil.rmtree(tmp_root, ignore_errors=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--profile', choices=['debug', 'ann-debug', 'release'], required=True)
    ap.add_argument('--out', required=True)
    args = ap.parse_args()
    run_suite(args)


if __name__ == '__main__':
    main()
