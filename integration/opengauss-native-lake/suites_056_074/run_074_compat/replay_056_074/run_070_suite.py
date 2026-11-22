#!/usr/bin/env python3
"""Scoped SQL0.70 (N6 widening ALTER COLUMN TYPE + OPS1 operational tools) suite.

Covers:
1. Extension upgrade 0.69 -> 0.70, fresh 0.70 install, host_status() & catalog_status() flags,
   and PUBLIC execution denial on admin OPS1 functions.
2. Widening ALTER COLUMN TYPE across:
   - int2 -> int4 -> int8 (chained)
   - int4 -> int8
   - float4 -> float8
   - numeric(8,2) -> numeric(14,4) (including DEFAULT values)
   - historical inline rows (pre-alter), flushed Parquet files (pre-alter), post-alter inline + Parquet
   - UPDATE & DELETE targeting pre-alter flushed/inline rows and post-alter rows
   - merge_adjacent_files over mixed pre-alter & post-alter Parquet files
   - time_travel() & bind_snapshot() on pre-alter and post-alter snapshots
   - table_changes() / table_insertions() across schema evolution
   - CREATE INDEX (lake_btree) after alter & query verification
   - DuckDB (ducklake + read_parquet) parity on widened metadata + Parquet + delete files.
3. Refusal guardrails:
   - narrowing int8 -> int4, int4 -> int2, float8 -> float4
   - numeric narrowing or integer-digit shrinkage: numeric(14,4)->numeric(10,2), numeric(8,2)->numeric(9,4)
   - incompatible families (int8 -> text, int8 -> numeric(20,0))
   - USING expression & COLLATE change
   - dependency checks against active lake index (and partition columns if tested)
   - READ ONLY transaction & read_only catalog refusal.
4. OPS1 operational functions:
   - og_lake.backup_manifest(catalog) JSONB structure, disk verification counts, zero secret leakage
   - og_lake.retention_status(catalog) telemetry + active_snapshot_pins/min_pinned_snapshot accuracy
   - og_lake.verify_catalog_integrity(catalog, repair): healthy -> corrupt index dir -> repair=true
     safely marks FAILED & clears active coverage -> index rebuilds cleanly;
     plus missing/corrupted Parquet data file reported unrepairable without fabricating user data.
"""

import argparse
import hashlib
import json
import os
import getpass
import shutil
import subprocess
import tempfile
import time
import uuid
from decimal import Decimal
from pathlib import Path

import psycopg2

ROOT = Path(os.environ.get('CPP_WORKSPACE', str(Path(__file__).resolve().parents[5]))).expanduser().resolve()
DUCKDB_BIN = ROOT / '.reference/opengauss-native-lake/build/duckdb-gcc10/duckdb'


def md5_role_pw(password, role):
    return 'md5' + hashlib.md5((password + role).encode('utf-8')).hexdigest()


def pg_connect(profile, dbname, user=None, password=None):
    port = 55560 if profile == 'debug' else (55570 if profile == 'release' else 55580)
    kw = {
        'host': str(ROOT / '.runtime/opengauss-native-lake' / profile / 'socket'),
        'port': port,
        'dbname': dbname,
        'user': user or os.environ.get('PGUSER') or getpass.getuser(),
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


def q_retry(conn, sql, params=None, tries=5):
    for attempt in range(tries):
        try:
            return q(conn, sql, params)
        except psycopg2.errors.SerializationFailure:
            if attempt + 1 == tries:
                raise
            time.sleep(0.05)


def s(conn, sql, params=None):
    rows = q(conn, sql, params)
    assert len(rows) == 1 and len(rows[0]) == 1, f'Expected scalar, got {rows!r}'
    return rows[0][0]


def expect_fail(conn, sql, substr, params=None):
    try:
        q(conn, sql, params)
    except Exception as exc:
        msg = str(exc)
        assert substr.lower() in msg.lower(), f'Expected {substr!r} in error, got: {msg}'
        return msg
    raise AssertionError(f'Expected SQL to fail ({substr}), but succeeded: {sql}')


def setup_catalog(conn, role_name, cat_name, cat_dir, idx_dir):
    q(conn, 'CREATE EXTENSION og_lake')
    assert s(conn, "SELECT extversion FROM pg_extension WHERE extname='og_lake'") in ('0.70', '0.73', '0.74')
    hs = json.loads(s(conn, 'SELECT og_lake.host_status()'))
    assert hs['module_version'] in ('0.70', '0.73', '0.74'), hs
    assert hs.get('alter_column_type_evolution') is True, hs
    assert hs.get('alter_type_ops_sql_version') == '0.70', hs
    assert hs.get('ops1_backup_integrity_retention') is True, hs
    assert hs.get('physical_gc') is False, hs
    Path(idx_dir).mkdir(parents=True, exist_ok=True)
    os.chmod(idx_dir, 0o755)
    q(conn, f"CREATE SCHEMA IF NOT EXISTS biz; GRANT USAGE ON SCHEMA biz TO {role_name}")
    q(conn, "SELECT og_lake.configure_internal_owner(%s::name)", (role_name,))
    q(conn, f"SELECT og_lake.create_catalog('{cat_name}'::name, 'lake_metadata'::name, %s::text, NULL::text)", (str(cat_dir),))
    q(conn, f"SELECT og_lake.set_option('{cat_name}'::name, 'data_inlining_row_limit'::text, '5000'::text)")
    q(conn, f"SELECT og_lake.set_option('{cat_name}'::name, 'index_root'::text, %s::text)", (str(idx_dir),))
    cs_raw = s(conn, "SELECT capabilities FROM og_lake.catalog_status WHERE server_name = %s", (cat_name,))
    cs = json.loads(cs_raw) if isinstance(cs_raw, str) else cs_raw
    assert cs.get('widening_alter_type') is True and cs.get('ops_tools') is True, cs


def duckdb_query(sql):
    assert DUCKDB_BIN.exists(), f'Missing DuckDB binary: {DUCKDB_BIN}'
    proc = subprocess.run(
        [str(DUCKDB_BIN), '-json', '-c', sql],
        capture_output=True, text=True, timeout=60,
    )
    assert proc.returncode == 0, f'DuckDB failed ({proc.returncode}):\nSQL:\n{sql}\nSTDERR:\n{proc.stderr}\nSTDOUT:\n{proc.stdout}'
    return json.loads(proc.stdout) if proc.stdout.strip() else []


def assert_rows_match(actual_rows, expected_map):
    got = {}
    for rid, sv, vi, fv, nv, tv in actual_rows:
        got[int(rid)] = (
            None if sv is None else int(sv),
            None if vi is None else int(vi),
            None if fv is None else float(fv),
            None if nv is None else Decimal(str(nv)),
            tv,
        )
    assert set(got.keys()) == set(expected_map.keys()), f'Key mismatch: got={sorted(got)} exp={sorted(expected_map)}'
    for k, exp in expected_map.items():
        g = got[k]
        assert g[0] == exp[0], f'id={k} s mismatch: {g[0]} vs {exp[0]}'
        assert g[1] == exp[1], f'id={k} v_int mismatch: {g[1]} vs {exp[1]}'
        if exp[2] is None:
            assert g[2] is None
        else:
            rel_err = abs(g[2] - exp[2]) / max(1.0, abs(exp[2]))
            assert rel_err < 1e-6, f'id={k} f mismatch: {g[2]} vs {exp[2]}'
        assert g[3] == exp[3], f'id={k} n mismatch: {g[3]} vs {exp[3]}'
        assert g[4] == exp[4], f'id={k} tag mismatch: {g[4]} vs {exp[4]}'


def run_suite(args):
    tag = uuid.uuid4().hex[:10]
    tmp_root = Path(tempfile.mkdtemp(prefix=f'og_070_{tag}_', dir='/tmp'))
    os.chmod(tmp_root, 0o755)
    admin = pg_connect(args.profile, 'postgres')
    created_dbs = []
    created_roles = []
    conns = []
    stages = {}

    try:
        tp_mode = s(admin, 'SHOW enable_thread_pool')
        role_main = f'u070_{tag}'
        role_unpriv = f'u070_np_{tag}'
        db_upg = f'db070_upg_{tag}'
        db_main = f'db070_main_{tag}'
        q(admin, f"CREATE ROLE {role_main} NOLOGIN NOINHERIT ENCRYPTED PASSWORD %s", (md5_role_pw('Own070!', role_main),))
        created_roles.append(role_main)
        q(admin, f"CREATE ROLE {role_unpriv} LOGIN ENCRYPTED PASSWORD %s", (md5_role_pw('Unp070!', role_unpriv),))
        created_roles.append(role_unpriv)
        for dbn in (db_upg, db_main):
            q(admin, f"CREATE DATABASE {dbn} WITH DBCOMPATIBILITY 'PG' ENCODING 'UTF8'")
            created_dbs.append(dbn)

        # Stage 1: Upgrade 0.69 -> 0.70 + PUBLIC permission restriction
        cu = pg_connect(args.profile, db_upg)
        conns.append(cu)
        q(cu, "CREATE EXTENSION og_lake VERSION '0.69'")
        q(cu, "ALTER EXTENSION og_lake UPDATE TO '0.70'")
        assert s(cu, "SELECT extversion FROM pg_extension WHERE extname='og_lake'") == '0.70'
        hs_u = json.loads(s(cu, 'SELECT og_lake.host_status()'))
        assert hs_u['module_version'] in ('0.70', '0.73', '0.74')
        assert hs_u.get('alter_column_type_evolution') is True
        assert hs_u.get('alter_type_ops_sql_version') == '0.70'
        assert hs_u.get('ops1_backup_integrity_retention') is True
        assert hs_u.get('physical_gc') is False
        cu_np = pg_connect(args.profile, db_upg, user=role_unpriv, password='Unp070!')
        conns.append(cu_np)
        expect_fail(cu_np, "SELECT og_lake.backup_manifest('no_such'::name)", 'permission denied')
        expect_fail(cu_np, "SELECT * FROM og_lake.verify_catalog_integrity('no_such'::name, false)", 'permission denied')
        expect_fail(cu_np, "SELECT * FROM og_lake.retention_status('no_such'::name)", 'permission denied')
        stages['stage1_upgrade_069_to_070_and_acl'] = 5

        # Setup main DB
        cm = pg_connect(args.profile, db_main)
        conns.append(cm)
        cat_m = tmp_root / 'cat'
        idx_m = tmp_root / 'idx'
        setup_catalog(cm, role_main, 'c_m', cat_m, idx_m)

        # Stage 2: Widening ALTER COLUMN TYPE across pre-alter inline + Parquet, post-alter DML, time travel, index, merge
        c2 = 0
        q(cm, """
            CREATE FOREIGN TABLE biz.t_wid (
                id int8 NOT NULL,
                s int2 DEFAULT 11,
                v_int int4 NOT NULL,
                f real,
                n numeric(8,2) DEFAULT 12.34,
                tag text
            ) SERVER c_m OPTIONS (binding_mode 'create', lake_schema 'main')
        """)
        q(cm, """
            INSERT INTO biz.t_wid VALUES
                (1, 7, 100, 1.5, 10.25, 'flushed_pre_1'),
                (2, 8, 200, 2.25, 20.50, 'flushed_pre_2'),
                (3, NULL, 300, NULL, NULL, 'flushed_pre_null')
        """)
        q(cm, "SELECT og_lake.flush('biz.t_wid')")
        snap_flushed_pre = s(cm, "SELECT og_lake.last_committed_snapshot('c_m')")

        q(cm, """
            INSERT INTO biz.t_wid VALUES
                (4, 9, 400, 3.5, 30.75, 'inline_pre_4'),
                (5, DEFAULT, 500, 4.75, DEFAULT, 'inline_pre_def')
        """)
        snap_pre_alter = s(cm, "SELECT og_lake.last_committed_snapshot('c_m')")

        exp_pre = {
            1: (7, 100, 1.5, Decimal('10.2500'), 'flushed_pre_1'),
            2: (8, 200, 2.25, Decimal('20.5000'), 'flushed_pre_2'),
            3: (None, 300, None, None, 'flushed_pre_null'),
            4: (9, 400, 3.5, Decimal('30.7500'), 'inline_pre_4'),
            5: (11, 500, 4.75, Decimal('12.3400'), 'inline_pre_def'),
        }

        # Execute chained & parallel widenings
        q(cm, "ALTER FOREIGN TABLE biz.t_wid ALTER COLUMN s TYPE int4")
        q(cm, "ALTER FOREIGN TABLE biz.t_wid ALTER COLUMN s TYPE int8")
        q(cm, "ALTER FOREIGN TABLE biz.t_wid ALTER COLUMN v_int TYPE int8")
        q(cm, "ALTER FOREIGN TABLE biz.t_wid ALTER COLUMN f TYPE double precision")
        q(cm, "ALTER FOREIGN TABLE biz.t_wid ALTER COLUMN n TYPE numeric(14,4)")
        c2 += 5

        assert_rows_match(q(cm, "SELECT id, s, v_int, f, n, tag FROM biz.t_wid ORDER BY id"), exp_pre)
        c2 += 1

        # Post-alter inserts using defaults + wide-range values overflowing pre-alter widths
        q(cm, """
            INSERT INTO biz.t_wid(id, v_int, f, tag) VALUES
                (6, 9000000001, 1e45, 'post_def')
        """)
        q(cm, """
            INSERT INTO biz.t_wid VALUES
                (7, 5000000002, 9000000002, 2.5e50, 1234567890.1234, 'post_wide')
        """)
        exp_mid = dict(exp_pre)
        exp_mid[6] = (11, 9000000001, 1e45, Decimal('12.3400'), 'post_def')
        exp_mid[7] = (5000000002, 9000000002, 2.5e50, Decimal('1234567890.1234'), 'post_wide')
        assert_rows_match(q(cm, "SELECT id, s, v_int, f, n, tag FROM biz.t_wid ORDER BY id"), exp_mid)
        c2 += 1

        # DML hitting pre-alter flushed row (2), pre-alter inline row (4), post-alter inline row (6)
        q_retry(cm, "UPDATE biz.t_wid SET s = 7000000002, n = 999999.9876, tag = 'upd_flushed_2' WHERE id = 2")
        q_retry(cm, "DELETE FROM biz.t_wid WHERE id = 4")
        q_retry(cm, "UPDATE biz.t_wid SET f = 3.25e60 WHERE id = 6")
        exp_now = dict(exp_mid)
        exp_now[2] = (7000000002, 200, 2.25, Decimal('999999.9876'), 'upd_flushed_2')
        del exp_now[4]
        exp_now[6] = (11, 9000000001, 3.25e60, Decimal('12.3400'), 'post_def')
        assert_rows_match(q(cm, "SELECT id, s, v_int, f, n, tag FROM biz.t_wid ORDER BY id"), exp_now)
        c2 += 1

        # Time travel & bind_snapshot back to snap_pre_alter
        tt_rows = q(cm, """
            SELECT id, s, v_int, f, n, tag
            FROM og_lake.time_travel('biz.t_wid'::regclass, %s::bigint)
                 AS t(id int8, s int2, v_int int4, f float4, n numeric(8,2), tag text)
            ORDER BY id
        """, (snap_pre_alter,))
        assert_rows_match(tt_rows, exp_pre)
        r_tt = pg_connect(args.profile, db_main)
        conns.append(r_tt)
        q(r_tt, "BEGIN READ ONLY")
        q(r_tt, "SELECT og_lake.bind_snapshot('c_m'::name, %s::bigint)", (snap_pre_alter,))
        assert_rows_match(q(r_tt, "SELECT id, s, v_int, f, n, tag FROM biz.t_wid ORDER BY id"), exp_pre)
        q(r_tt, "COMMIT")
        c2 += 2

        # Flush remaining inline rows, create lake btree indexes, rewrite delete files & merge adjacent files
        q_retry(cm, "INSERT INTO biz.t_wid VALUES (8, 12, 800, 5.5, 1.2500, 'post_rewrite')")
        exp_now[8] = (12, 800, 5.5, Decimal('1.2500'), 'post_rewrite')
        for _ in range(3):
            try:
                q(cm, "SELECT og_lake.flush('biz.t_wid')")
            except psycopg2.errors.SerializationFailure:
                pass
        for _ in range(4):
            rem = [r[0] for r in q(cm, """
                SELECT DISTINCT d.data_file_id
                FROM lake_metadata.ducklake_delete_file df
                JOIN lake_metadata.ducklake_data_file d USING(data_file_id, table_id)
                JOIN lake_metadata.ducklake_table t USING(table_id)
                WHERE t.table_name='t_wid' AND df.end_snapshot IS NULL AND d.end_snapshot IS NULL
                ORDER BY d.data_file_id
            """)]
            if not rem:
                break
            for i in range(0, len(rem), 2):
                q_retry(cm, "SELECT og_lake.rewrite_data_files('biz.t_wid'::regclass, %s::bigint[])", (rem[i:i + 2],))
        q_retry(cm, "SELECT og_lake.merge_adjacent_files('biz.t_wid')")
        ix_wid_s = s(cm, "SELECT og_lake.create_lake_index('biz.t_wid'::regclass, 'ix_wid_s'::name, '{\"version\":1,\"kind\":\"scalar\",\"method\":\"btree\",\"column\":\"s\"}'::jsonb)")
        ix_wid_v = s(cm, "SELECT og_lake.create_lake_index('biz.t_wid'::regclass, 'ix_wid_v'::name, '{\"version\":1,\"kind\":\"scalar\",\"method\":\"btree\",\"column\":\"v_int\"}'::jsonb)")
        q(cm, "SELECT og_lake.build_lake_index(%s::uuid)", (ix_wid_s,))
        q(cm, "SELECT og_lake.build_lake_index(%s::uuid)", (ix_wid_v,))
        q(cm, "SELECT og_lake.rebuild_lake_index(%s::uuid)", (ix_wid_s,))
        q(cm, "SELECT og_lake.build_lake_index(%s::uuid)", (ix_wid_s,))
        assert_rows_match(q(cm, "SELECT id, s, v_int, f, n, tag FROM biz.t_wid ORDER BY id"), exp_now)
        wide_hit = q(cm, "SELECT id, tag FROM biz.t_wid WHERE s >= 5000000000 ORDER BY id")
        assert wide_hit == [(2, 'upd_flushed_2'), (7, 'post_wide')], wide_hit
        null_hit = q(cm, "SELECT id FROM biz.t_wid WHERE s IS NULL")
        assert null_hit == [(3,)], null_hit
        c2 += 3

        # CDC table_changes & table_insertions across schema evolution (snap_flushed_pre -> head)
        head_snap = s(cm, "SELECT og_lake.last_committed_snapshot('c_m')")
        tc_rows = q(cm, """
            SELECT change_type, id, s
            FROM og_lake.table_changes('biz.t_wid'::regclass, %s::bigint, %s::bigint)
                 AS t(snapshot_id bigint, rowid bigint, change_type text, id int8, s int8, v_int int8, f float8, n numeric(14,4), tag text)
            ORDER BY snapshot_id, id
        """, (snap_flushed_pre, head_snap))
        assert len(tc_rows) >= 4, tc_rows
        ti_rows = q(cm, """
            SELECT id, s
            FROM og_lake.table_insertions('biz.t_wid'::regclass, %s::bigint, %s::bigint)
                 AS t(snapshot_id bigint, rowid bigint, id int8, s int8, v_int int8, f float8, n numeric(14,4), tag text)
            ORDER BY id
        """, (snap_flushed_pre, head_snap))
        assert len(ti_rows) >= 2, ti_rows
        c2 += 2

        # DuckDB read_parquet parity over active merged/non-deleted data files in cat_m
        data_files = q(cm, """
            SELECT d.path, d.path_is_relative, coalesce(t.path,''), coalesce(t.path_is_relative,true), coalesce(s.path,''), coalesce(s.path_is_relative,true)
            FROM lake_metadata.ducklake_data_file d
            LEFT JOIN lake_metadata.ducklake_table t ON t.table_id=d.table_id
            LEFT JOIN lake_metadata.ducklake_schema s ON s.schema_id=t.schema_id
            WHERE d.end_snapshot IS NULL
        """)
        assert len(data_files) >= 1
        del_files = q(cm, """
            SELECT d.path, d.path_is_relative, coalesce(t.path,''), coalesce(t.path_is_relative,true), coalesce(s.path,''), coalesce(s.path_is_relative,true)
            FROM lake_metadata.ducklake_delete_file d
            LEFT JOIN lake_metadata.ducklake_table t ON t.table_id=d.table_id
            LEFT JOIN lake_metadata.ducklake_schema s ON s.schema_id=t.schema_id
            WHERE d.end_snapshot IS NULL
        """)
        assert len(del_files) == 0, f'Expected merge_adjacent_files to materialize deletes, got {del_files}'
        # Query each active Parquet file with DuckDB read_parquet directly
        pq_paths = []
        for fpath, frel, tpath, trel, spath, srel in data_files:
            parts = []
            if trel and spath:
                parts.append(spath.strip('/'))
            if frel and tpath:
                parts.append(tpath.strip('/'))
            parts.append(fpath.lstrip('/'))
            pq_paths.append(str(cat_m / '/'.join(p for p in parts if p)))
        pq_list_sql = ', '.join("'" + p.replace("'", "''") + "'" for p in pq_paths)
        ddb_rows = duckdb_query(f"SELECT id, s, v_int, f, CAST(n AS VARCHAR) AS n_str, tag FROM read_parquet([{pq_list_sql}], union_by_name=true) ORDER BY id")
        assert len(ddb_rows) == len(exp_now), f'DuckDB count mismatch: {ddb_rows} vs {exp_now}'
        for dr in ddb_rows:
            rid = int(dr['id'])
            assert rid in exp_now
            exp = exp_now[rid]
            assert (None if dr['s'] is None else int(dr['s'])) == exp[0], (dr, exp)
            assert int(dr['v_int']) == exp[1], (dr, exp)
            assert (None if dr['n_str'] is None else Decimal(dr['n_str'])) == exp[3], (dr, exp)
            assert dr['tag'] == exp[4], (dr, exp)
        c2 += 2
        stages['stage2_widening_alter_type_and_duckdb'] = c2

        # Stage 3: Refusal guardrails
        c3 = 0
        # Active lake index depends on column k (widening int2->int4 blocked while index ACTIVE, succeeds after drop_lake_index)
        q(cm, "CREATE FOREIGN TABLE biz.t_idx_dep (id int8 NOT NULL, k int2) SERVER c_m OPTIONS (binding_mode 'create', lake_schema 'main')")
        q(cm, "INSERT INTO biz.t_idx_dep VALUES (1, 12)")
        ix_dep = s(cm, "SELECT og_lake.create_lake_index('biz.t_idx_dep'::regclass, 'ix_dep_k'::name, '{\"version\":1,\"kind\":\"scalar\",\"method\":\"btree\",\"column\":\"k\"}'::jsonb)")
        expect_fail(cm, "ALTER FOREIGN TABLE biz.t_idx_dep ALTER COLUMN k TYPE int4", 'depends on column')
        q(cm, "SELECT og_lake.drop_lake_index(%s::uuid)", (ix_dep,))
        q(cm, "ALTER FOREIGN TABLE biz.t_idx_dep ALTER COLUMN k TYPE int4")
        assert q(cm, "SELECT id, k FROM biz.t_idx_dep") == [(1, 12)]
        c3 += 2
        # Now s on t_wid (drop ix_wid_s first so dependency doesn't mask widening checks) -> test narrowing / USING / incompatible refusals
        q(cm, "SELECT og_lake.drop_lake_index(%s::uuid)", (ix_wid_s,))
        expect_fail(cm, "ALTER FOREIGN TABLE biz.t_wid ALTER COLUMN s TYPE int4", 'widening')
        expect_fail(cm, "ALTER FOREIGN TABLE biz.t_wid ALTER COLUMN v_int TYPE int2", 'widening')
        expect_fail(cm, "ALTER FOREIGN TABLE biz.t_wid ALTER COLUMN f TYPE real", 'widening')
        expect_fail(cm, "ALTER FOREIGN TABLE biz.t_wid ALTER COLUMN n TYPE numeric(10,2)", 'widening')
        expect_fail(cm, "ALTER FOREIGN TABLE biz.t_wid ALTER COLUMN n TYPE numeric(16,2)", 'widening')
        expect_fail(cm, "ALTER FOREIGN TABLE biz.t_wid ALTER COLUMN n TYPE numeric(15,6)", 'widening')  # int digits 15-6=9 < 14-4=10!
        expect_fail(cm, "ALTER FOREIGN TABLE biz.t_wid ALTER COLUMN s TYPE text", 'widening')
        expect_fail(cm, "ALTER FOREIGN TABLE biz.t_wid ALTER COLUMN s TYPE numeric(24,0)", 'widening')
        expect_fail(cm, "ALTER FOREIGN TABLE biz.t_wid ALTER COLUMN s TYPE int8 USING s + 1", 'without USING')
        expect_fail(cm, "ALTER FOREIGN TABLE biz.t_wid ALTER COLUMN tag TYPE text COLLATE \"C\"", 'without USING')
        c3 += 10
        # Read-only txn refusal
        q(cm, "CREATE FOREIGN TABLE biz.t_ro_chk (id int2 NOT NULL) SERVER c_m OPTIONS (binding_mode 'create', lake_schema 'main')")
        q(cm, "BEGIN READ ONLY")
        expect_fail(cm, "ALTER FOREIGN TABLE biz.t_ro_chk ALTER COLUMN id TYPE int4", 'read-only')
        q(cm, "ROLLBACK")
        assert_rows_match(q(cm, "SELECT id, s, v_int, f, n, tag FROM biz.t_wid ORDER BY id"), exp_now)
        c3 += 2
        stages['stage3_alter_type_refusals'] = c3

        # Stage 4: OPS1 operational suite (backup_manifest, retention_status, verify_catalog_integrity + safe repair)
        c4 = 0
        # Flush any metadata & verify backup_manifest
        manifest = s(cm, "SELECT og_lake.backup_manifest('c_m'::name)")
        assert isinstance(manifest, dict), type(manifest)
        assert manifest['module_version'] in ('0.70', '0.73', '0.74')
        assert manifest['server_name'] == 'c_m'
        assert manifest['read_only'] is False
        assert manifest['summary']['active_data_files'] >= 1
        assert manifest['summary']['verified_data_files'] == manifest['summary']['active_data_files']
        assert manifest['summary']['missing_or_mismatched_data_files'] == 0
        assert manifest['summary']['ready_index_segments'] >= 1
        raw_manifest = json.dumps(manifest).lower()
        assert 'password' not in raw_manifest and 'secret' not in raw_manifest
        c4 += 2

        # retention_status under an active pin
        r_pin = pg_connect(args.profile, db_main)
        conns.append(r_pin)
        q(r_pin, "BEGIN READ ONLY")
        q(r_pin, "SELECT og_lake.bind_snapshot('c_m'::name, %s::bigint)", (snap_pre_alter,))
        ret_rows = q(cm, "SELECT catalog_name, head_snapshot, total_snapshots, expired_scheduled_files, scheduled_bytes, active_pins, min_pinned_snapshot, delete_older_than, oldest_xmin, ready_index_segments, retired_index_segments, auto_compact, target_file_size FROM og_lake.retention_status('c_m'::name)")
        assert len(ret_rows) == 1
        rr = ret_rows[0]
        assert rr[0] == 'c_m'
        assert rr[1] >= head_snap
        assert rr[2] >= 2
        assert rr[5] >= 1 and rr[6] == snap_pre_alter, rr
        assert int(rr[8]) > 0, rr
        assert rr[9] >= 1, rr
        q(r_pin, "COMMIT")
        c4 += 2

        # verify_catalog_integrity(healthy)
        v_ok = q(cm, "SELECT object_kind, object_id, severity, status, detail FROM og_lake.verify_catalog_integrity('c_m'::name, false)")
        assert v_ok[-1][0] == 'summary' and v_ok[-1][2] == 'ok' and v_ok[-1][3] == 'healthy', v_ok

        # Corrupt index segment dir of ix_wid_v on disk -> verify detects -> repair=true marks FAILED -> rebuilds cleanly
        seg_info = q(cm, """
            SELECT s.segment_uuid::text, s.artifact_root_uri
            FROM og_lake_internal.index_segment s
            JOIN og_lake_internal.index_definition d USING(index_uuid)
            WHERE d.index_uuid=%s::uuid AND s.state='READY'
            LIMIT 1
        """, (ix_wid_v,))
        assert len(seg_info) == 1, seg_info
        seg_uuid, art_uri = seg_info[0]
        raw_uri = art_uri[7:] if art_uri.startswith('file://') else art_uri
        art_path = Path(raw_uri) if os.path.isabs(raw_uri) else (idx_m / raw_uri)
        assert art_path.exists(), f'Missing artifact path {art_path}'
        shutil.rmtree(art_path)
        v_bad = q(cm, "SELECT object_kind, object_id, severity, status, detail FROM og_lake.verify_catalog_integrity('c_m'::name, false)")
        assert any(r[0] == 'index_segment' and r[1] == seg_uuid and r[3] == 'corrupt_or_missing_index_segment' for r in v_bad), v_bad

        for _v_try in range(20):
            try:
                v_rep = q(cm, "SELECT object_kind, object_id, severity, status, detail FROM og_lake.verify_catalog_integrity('c_m'::name, true)")
                break
            except psycopg2.errors.LockNotAvailable:
                if _v_try == 19:
                    raise
                time.sleep(0.25)
        assert any(r[0] == 'index_segment' and r[1] == seg_uuid and r[3] == 'repaired_marked_failed' for r in v_rep), v_rep
        assert s(cm, "SELECT state FROM og_lake_internal.index_segment WHERE segment_uuid=%s::uuid", (seg_uuid,)) == 'FAILED'
        # Fallback query still matches expected rows!
        assert_rows_match(q(cm, "SELECT id, s, v_int, f, n, tag FROM biz.t_wid ORDER BY id"), exp_now)
        # Rebuild ix_wid_v & verify healthy again
        q(cm, "SELECT og_lake.rebuild_lake_index(%s::uuid)", (ix_wid_v,))
        q(cm, "SELECT og_lake.build_lake_index(%s::uuid)", (ix_wid_v,))
        v_healed = q(cm, "SELECT object_kind, object_id, severity, status, detail FROM og_lake.verify_catalog_integrity('c_m'::name, false)")
        assert v_healed[-1][0] == 'summary' and v_healed[-1][2] == 'ok' and v_healed[-1][3] == 'healthy', v_healed
        c4 += 4

        # Missing Parquet data file: repair=true must report unrepairable_business_data and never fabricate user file!
        q(cm, "CREATE FOREIGN TABLE biz.t_corrupt (id int4 NOT NULL, v text) SERVER c_m OPTIONS (binding_mode 'create', lake_schema 'main')")
        q(cm, "INSERT INTO biz.t_corrupt VALUES (1, 'important_user_row')")
        q(cm, "SELECT og_lake.flush('biz.t_corrupt')")
        corrupt_df = q(cm, """
            SELECT d.data_file_id, d.path, d.path_is_relative, coalesce(t.path,''), coalesce(t.path_is_relative,true), coalesce(s.path,''), coalesce(s.path_is_relative,true)
            FROM lake_metadata.ducklake_data_file d
            JOIN lake_metadata.ducklake_table t ON t.table_id=d.table_id
            JOIN lake_metadata.ducklake_schema s ON s.schema_id=t.schema_id
            WHERE d.end_snapshot IS NULL AND t.table_name='t_corrupt'
        """)
        assert len(corrupt_df) == 1, corrupt_df
        df_id, fpath, frel, tpath, trel, spath, srel = corrupt_df[0]
        parts = []
        if trel and spath:
            parts.append(spath.strip('/'))
        if frel and tpath:
            parts.append(tpath.strip('/'))
        parts.append(fpath.lstrip('/'))
        dead_pq = cat_m / '/'.join(p for p in parts if p)
        assert dead_pq.exists()
        dead_pq.unlink()
        for _v_try in range(20):
            try:
                v_unrep = q(cm, "SELECT object_kind, object_id, severity, status, detail FROM og_lake.verify_catalog_integrity('c_m'::name, true)")
                break
            except psycopg2.errors.LockNotAvailable:
                if _v_try == 19:
                    raise
                time.sleep(0.25)
        assert any(r[0] == 'data_file' and r[1] == str(df_id) and r[2] == 'error' and r[3] == 'unrepairable_business_data' for r in v_unrep), v_unrep
        assert not dead_pq.exists(), 'verify_catalog_integrity repair=true must never fabricate deleted user Parquet file!'
        assert s(cm, "SELECT count(*) FROM lake_metadata.ducklake_data_file WHERE data_file_id=%s AND end_snapshot IS NULL", (df_id,)) == 1
        c4 += 2
        stages['stage4_ops1_manifest_verify_retention'] = c4

        result = {
            'profile': args.profile,
            'enable_thread_pool': tp_mode,
            'postgres_ext_version': '0.70',
            'stages': stages,
            'total_checks': sum(stages.values()),
        }
        if args.out:
            Path(args.out).write_text(json.dumps(result, indent=2, ensure_ascii=False) + '\n', encoding='utf-8')
        print(json.dumps(result, indent=2, ensure_ascii=False))

    finally:
        for c in conns:
            try:
                c.close()
            except Exception:
                pass
        for dbn in created_dbs:
            try:
                q(admin, f"""
                    SELECT pg_terminate_session(pid, sessionid)
                    FROM pg_stat_activity
                    WHERE datname = '{dbn}' AND sessionid <> pg_current_sessionid()
                """)
            except Exception:
                pass
            try:
                q(admin, f"DROP DATABASE IF EXISTS {dbn}")
            except Exception:
                pass
        for rn in created_roles:
            try:
                q(admin, f"DROP ROLE IF EXISTS {rn}")
            except Exception:
                pass
        try:
            admin.close()
        except Exception:
            pass
        shutil.rmtree(tmp_root, ignore_errors=True)


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--profile', choices=['debug', 'ann-debug', 'release'], required=True)
    p.add_argument('--out')
    args = p.parse_args()
    run_suite(args)


if __name__ == '__main__':
    main()
