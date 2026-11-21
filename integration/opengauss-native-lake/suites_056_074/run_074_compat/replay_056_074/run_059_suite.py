#!/usr/bin/env python3
"""Comprehensive 12-group verification suite for og_lake SQL0.59:
- og_lake.bind_snapshot(server_name name, snapshot_id bigint) -> void
- og_lake.rebuild_lake_index(index_id uuid) -> void
- DuckLake-compatible read-only metadata functions: table_info, list_files, options
- Public privilege-filtered management status views: catalog_status, table_status, index_status
- Independent pinned DuckLake (0aaa8e10d4adfe2ca37da77134e991d20106d0e7) parity
"""
import argparse
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import secrets
import shutil
import sys
import time
import uuid

import psycopg2
from psycopg2 import sql

ROOT = Path('/home/qiumc/data/code/cpp')
TESTS_DIR = ROOT / 'integration/opengauss-native-lake'
sys.path.insert(0, str(TESTS_DIR))
sys.path.insert(0, str(TESTS_DIR / 'tests'))
from native_runtime import INSTALL, PORT, PROFILE, RUNTIME, SOURCE
from empty_catalog_oracle import read_native_catalog, transport_native
from reverse_reference import Reference


def sha_file(p):
    h = hashlib.sha256()
    with Path(p).open('rb') as f:
        for b in iter(lambda: f.read(1024 * 1024), b''):
            h.update(b)
    return h.hexdigest()


def connect(dbname='postgres', user=None, password=None, autocommit=True):
    kwargs = dict(
        host=str(RUNTIME / 'socket'),
        port=PORT,
        dbname=dbname,
        user=user or os.getenv('USER', 'qiumc'),
        connect_timeout=10,
        application_name='og_lake_test_059',
    )
    if password is not None:
        kwargs['password'] = password
    conn = psycopg2.connect(**kwargs)
    conn.autocommit = autocommit
    with conn.cursor() as cur:
        cur.execute('SET statement_timeout = 60000')
        cur.execute("SET behavior_compat_options = 'display_leading_zero'")
    return conn


def scalar(conn, stmt, params=()):
    with conn.cursor() as cur:
        cur.execute(stmt, params)
        rows = cur.fetchall()
    assert len(rows) == 1 and len(rows[0]) == 1, f'expected 1x1, got {rows!r} for {stmt}'
    return rows[0][0]


def fetchall(conn, stmt, params=()):
    with conn.cursor() as cur:
        cur.execute(stmt, params)
        return cur.fetchall()


def fetchdicts(conn, stmt, params=()):
    with conn.cursor() as cur:
        cur.execute(stmt, params)
        cols = [d.name for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


def exec_sql(conn, stmt, params=()):
    with conn.cursor() as cur:
        cur.execute(stmt, params)


def current_snap(conn, server):
    return scalar(conn, 'SELECT id FROM og_lake.current_snapshot(%s::name)', (server,))


def assert_raises(conn, stmt, params=(), expected_sqlstate=None):
    try:
        exec_sql(conn, stmt, params)
    except psycopg2.Error as exc:
        if expected_sqlstate is not None:
            assert exc.pgcode == expected_sqlstate, (
                f'expected SQLSTATE {expected_sqlstate}, got {exc.pgcode}: {exc} for {stmt}'
            )
        return exc
    raise AssertionError(f'expected SQL error ({expected_sqlstate}) for: {stmt}')


def create_empty_ducklake_root(workdir: Path, name: str) -> str:
    workdir.mkdir(parents=True, exist_ok=True)
    data_dir = workdir / (name + '_data')
    assert not data_dir.exists(), f'data_dir already exists: {data_dir}'
    return str(data_dir)


def run_suite(output_json: Path | None = None):
    started = datetime.now(timezone.utc).isoformat()
    run_id = uuid.uuid4().hex[:10]
    workdir = Path('/tmp') / f'og_lake_059_suite_{run_id}'
    workdir.mkdir(parents=True, exist_ok=True)
    db58 = f'og_lake_059_v58_{run_id}'
    db59 = f'og_lake_059_v59_{run_id}'
    owner_role = f'og_owner_{run_id}'
    tbl_owner_role = f'og_towner_{run_id}'
    u_none = f'og_unone_{run_id}'
    u_col = f'og_ucol_{run_id}'
    u_full = f'og_ufull_{run_id}'
    owner_pwd = 'Og1!' + secrets.token_hex(12)
    user_pwd = 'Og1!' + secrets.token_hex(12)

    def md5_pw(role, pw):
        return 'md5' + hashlib.md5((pw + role).encode('utf-8')).hexdigest()

    checks = []

    def record(name):
        checks.append(name)

    admin = connect('postgres')
    thread_pool_setting = scalar(admin, 'SHOW enable_thread_pool')
    postmaster_start = str(scalar(admin, 'SELECT pg_postmaster_start_time()'))
    try:
        exec_sql(admin, sql.SQL('CREATE ROLE {} NOLOGIN NOINHERIT PASSWORD {}').format(
            sql.Identifier(owner_role), sql.Literal(owner_pwd)))
        for role in (tbl_owner_role, u_none, u_col, u_full):
            exec_sql(admin, sql.SQL('CREATE ROLE {} LOGIN ENCRYPTED PASSWORD {}').format(
                sql.Identifier(role), sql.Literal(md5_pw(role, user_pwd))))
        exec_sql(admin, sql.SQL("CREATE DATABASE {} DBCOMPATIBILITY 'PG'").format(sql.Identifier(db58)))
        exec_sql(admin, sql.SQL("CREATE DATABASE {} DBCOMPATIBILITY 'PG'").format(sql.Identifier(db59)))

        # ------------------------------------------------------------------
        # Group 1: Pre-0.59 gate in 0.58 and ALTER EXTENSION UPDATE TO '0.59'
        # ------------------------------------------------------------------
        c58 = connect(db58, autocommit=True)
        try:
            exec_sql(c58, "CREATE EXTENSION og_lake VERSION '0.58'")
            hs58 = json.loads(scalar(c58, 'SELECT og_lake.host_status()'))
            assert hs58['module_version'] in ('0.59', '0.73', '0.74'), hs58
            assert hs58['bind_snapshot'] is True, hs58
            assert hs58['bind_snapshot_sql_version'] == '0.59', hs58
            assert hs58['management_status_views'] is True, hs58
            assert hs58['rebuild_lake_index'] is True, hs58
            assert hs58['table_changes'] is True, hs58
            assert hs58['time_travel'] is True, hs58
            exec_sql(c58, 'SELECT og_lake.configure_internal_owner(%s::name)', (owner_role,))
            root58 = create_empty_ducklake_root(workdir, 'cat58')
            exec_sql(c58, 'SELECT og_lake.create_catalog(%s::name, %s::name, %s, NULL)',
                     ('srv58', 'meta58', root58))
            exec_sql(c58, 'CREATE SCHEMA biz')
            exec_sql(c58, """
                CREATE FOREIGN TABLE biz.t58 (id int4, note text)
                SERVER srv58 OPTIONS (binding_mode 'create', lake_schema 'main')
            """)
            exec_sql(c58, "INSERT INTO biz.t58 VALUES (1, 'v1')")
            s58_1 = current_snap(c58, 'srv58')
            assert_raises(c58, "SELECT og_lake.bind_snapshot('srv58'::name, 1)", expected_sqlstate='42883')
            assert_raises(c58, 'SELECT * FROM og_lake.catalog_status', expected_sqlstate='42P01')
            # Existing 0.58 CDC + 0.57 time_travel continue to work under 0.58
            assert fetchall(
                c58,
                "SELECT id, note FROM og_lake.time_travel('biz.t58'::regclass, %s) AS t(id int4, note text)",
                (s58_1,),
            ) == [(1, 'v1')]
            assert fetchall(
                c58,
                "SELECT change_type, id, note FROM og_lake.table_changes('biz.t58'::regclass, %s, %s) "
                "AS c(snapshot_id bigint, rowid bigint, change_type text, id int4, note text)",
                (s58_1, s58_1),
            ) == [('insert', 1, 'v1')]

            exec_sql(c58, "ALTER EXTENSION og_lake UPDATE TO '0.59'")
            assert scalar(c58, "SELECT extversion FROM pg_extension WHERE extname='og_lake'") == '0.59'
            exec_sql(c58, "INSERT INTO biz.t58 VALUES (2, 'v2')")
            c58.autocommit = False
            try:
                exec_sql(c58, "SELECT og_lake.bind_snapshot('srv58'::name, %s)", (s58_1,))
                assert fetchall(c58, 'SELECT id, note FROM biz.t58 ORDER BY id') == [(1, 'v1')]
                cs = fetchdicts(c58, 'SELECT * FROM og_lake.catalog_status')
                assert len(cs) == 1 and cs[0]['bound_snapshot'] == s58_1
                c58.commit()
            finally:
                c58.autocommit = True
            assert fetchall(c58, 'SELECT id, note FROM biz.t58 ORDER BY id') == [(1, 'v1'), (2, 'v2')]
            record('pre_059_gate_and_extension_upgrade_058_to_059')
        finally:
            c58.close()

        # ------------------------------------------------------------------
        # Fresh 0.59 database for Groups 2..12
        # ------------------------------------------------------------------
        c = connect(db59, autocommit=True)
        try:
            exec_sql(c, 'CREATE EXTENSION og_lake')
            assert scalar(c, "SELECT extversion FROM pg_extension WHERE extname='og_lake'") in ('0.59', '0.73', '0.74')
            exec_sql(c, 'SELECT og_lake.configure_internal_owner(%s::name)', (owner_role,))
            root_main = create_empty_ducklake_root(workdir, 'cat_main')
            idx_root_main = workdir / 'idx_main'
            idx_root_main.mkdir(parents=True, exist_ok=True)
            exec_sql(c, 'SELECT og_lake.create_catalog(%s::name, %s::name, %s, NULL)',
                     ('srv_main', 'meta_main', root_main))
            exec_sql(c, "SELECT og_lake.set_option('srv_main'::name, 'index_root', %s)", (str(idx_root_main),))
            exec_sql(c, 'CREATE SCHEMA biz')
            exec_sql(c, 'CREATE FOREIGN DATA WRAPPER dummy_fdw_059')
            exec_sql(c, 'CREATE SERVER dummy_srv_059 FOREIGN DATA WRAPPER dummy_fdw_059')
            exec_sql(c, 'CREATE TABLE biz.heap_sink (id int4, label text)')

            # ------------------------------------------------------------------
            # Group 2: Transaction-block, isolation, and first-access preconditions
            # ------------------------------------------------------------------
            assert_raises(c, "SELECT og_lake.bind_snapshot('srv_main'::name, 0)", expected_sqlstate='25P01')
            c.autocommit = False
            try:
                assert_raises(c, 'SELECT og_lake.bind_snapshot(NULL, 0)', expected_sqlstate='22004')
                c.rollback()
                assert_raises(c, "SELECT og_lake.bind_snapshot('srv_main'::name, NULL)", expected_sqlstate='22004')
                c.rollback()
                assert_raises(c, "SELECT og_lake.bind_snapshot('no_such_srv'::name, 0)", expected_sqlstate='42704')
                c.rollback()
                assert_raises(c, "SELECT og_lake.bind_snapshot('dummy_srv_059'::name, 0)", expected_sqlstate='42809')
                c.rollback()
                assert_raises(c, "SELECT og_lake.bind_snapshot('srv_main'::name, -1)", expected_sqlstate='22023')
                c.rollback()
                assert_raises(c, "SELECT og_lake.bind_snapshot('srv_main'::name, 99999)", expected_sqlstate='22023')
                c.rollback()
                exec_sql(c, 'SET TRANSACTION ISOLATION LEVEL REPEATABLE READ')
                assert_raises(c, "SELECT og_lake.bind_snapshot('srv_main'::name, 0)", expected_sqlstate='0A000')
                c.rollback()
            finally:
                if c.status != psycopg2.extensions.STATUS_READY:
                    c.rollback()
                c.autocommit = True

            # Build multi-type table lifecycle across inline, flush, delete, rewrite
            u1 = str(uuid.UUID('11111111-2222-3333-4444-555555555551'))
            u2 = str(uuid.UUID('11111111-2222-3333-4444-555555555552'))
            u3 = str(uuid.UUID('11111111-2222-3333-4444-555555555553'))
            u4 = str(uuid.UUID('11111111-2222-3333-4444-555555555554'))
            exec_sql(c, """
                CREATE FOREIGN TABLE biz.t_all (
                    id int4 NOT NULL,
                    b bool,
                    i2 int2,
                    i8 int8,
                    f4 float4,
                    f8 float8,
                    label text,
                    raw bytea,
                    d date,
                    ts timestamp,
                    tstz timestamptz,
                    u uuid,
                    j json,
                    num numeric(18, 4),
                    emb vector(3)
                ) SERVER srv_main OPTIONS (binding_mode 'create', lake_schema 'main')
            """)
            s_created = current_snap(c, 'srv_main')

            exec_sql(c, """
                INSERT INTO biz.t_all VALUES
                (1, true,  10, 1000, 1.5, 10.25, 'alpha', E'\\\\x0001ff', DATE '2026-01-02',
                 TIMESTAMP '2026-01-02 03:04:05', TIMESTAMPTZ '2026-01-02 03:04:05+00',
                 %s::uuid, '{"k":1}', 12345.6789, '[1,2,3]'),
                (2, false, 20, 2000, 2.5, 20.50, 'beta',  E'\\\\xdeadbeef', DATE '2026-02-03',
                 TIMESTAMP '2026-02-03 04:05:06', TIMESTAMPTZ '2026-02-03 04:05:06+00',
                 %s::uuid, '[1,true,"x"]', -99.0001, '[4,5,6]'),
                (3, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
            """, (u1, u2))
            s1 = current_snap(c, 'srv_main')

            exec_sql(c, "UPDATE biz.t_all SET label = 'beta_inline_upd', num = 222.3333 WHERE id = 2")
            exec_sql(c, 'DELETE FROM biz.t_all WHERE id = 3')
            s2 = current_snap(c, 'srv_main')

            exec_sql(c, "SELECT og_lake.flush('biz.t_all'::regclass)")
            s3_flush = current_snap(c, 'srv_main')
            assert s3_flush > s2  # flush mints a new DuckLake snapshot when flushing inlined rows

            exec_sql(c, """
                INSERT INTO biz.t_all VALUES
                (4, true, 40, 4000, 4.5, 40.75, 'delta', E'\\\\x42', DATE '2026-04-05',
                 TIMESTAMP '2026-04-05 06:07:08', TIMESTAMPTZ '2026-04-05 06:07:08+00',
                 %s::uuid, '{"d":4}', 0.5000, '[7,8,9]')
            """, (u3,))
            exec_sql(c, "UPDATE biz.t_all SET label = 'alpha_file_upd', i8 = 1111 WHERE id = 1")
            s4 = current_snap(c, 'srv_main')

            exec_sql(c, "SELECT og_lake.flush('biz.t_all'::regclass)")
            file_ids = [
                r[0] for r in fetchall(
                    c,
                    'SELECT data_file_id FROM meta_main.ducklake_data_file '
                    'WHERE end_snapshot IS NULL ORDER BY data_file_id',
                )
            ]
            assert len(file_ids) >= 2, file_ids
            exec_sql(c, "SELECT og_lake.rewrite_data_files('biz.t_all'::regclass, %s::bigint[])", (file_ids[:2],))
            s5 = current_snap(c, 'srv_main')

            exec_sql(c, 'DELETE FROM biz.t_all WHERE id = 2')
            exec_sql(c, """
                INSERT INTO biz.t_all VALUES
                (5, false, 50, 5000, 5.5, 50.25, 'omega', E'\\\\x99', DATE '2026-05-06',
                 TIMESTAMP '2026-05-06 07:08:09', TIMESTAMPTZ '2026-05-06 07:08:09+00',
                 %s::uuid, '{"w":5}', 888.1250, '[0,1,0]')
            """, (u4,))
            s6 = current_snap(c, 'srv_main')

            # First-access & duplicate bind_snapshot rejections
            c.autocommit = False
            try:
                _ = fetchall(c, 'SELECT id FROM biz.t_all ORDER BY id')
                assert_raises(c, "SELECT og_lake.bind_snapshot('srv_main'::name, %s)", (s1,), expected_sqlstate='55000')
                c.rollback()

                exec_sql(c, "UPDATE biz.t_all SET label = 'tmp' WHERE id = 5")
                assert_raises(c, "SELECT og_lake.bind_snapshot('srv_main'::name, %s)", (s1,), expected_sqlstate='55000')
                c.rollback()

                exec_sql(c, "SELECT og_lake.bind_snapshot('srv_main'::name, %s)", (s1,))
                assert_raises(c, "SELECT og_lake.bind_snapshot('srv_main'::name, %s)", (s1,), expected_sqlstate='55000')
                c.rollback()

                # Table did not exist at snapshot 0 (seed snapshot before CREATE FOREIGN TABLE)
                exec_sql(c, "SELECT og_lake.bind_snapshot('srv_main'::name, 0)")
                assert_raises(c, 'SELECT * FROM biz.t_all', expected_sqlstate='22023')
                c.rollback()
            finally:
                if c.status != psycopg2.extensions.STATUS_READY:
                    c.rollback()
                c.autocommit = True
            record('bind_snapshot_transaction_and_first_access_preconditions')

            # ------------------------------------------------------------------
            # Group 3: Historical ordinary SELECT parity across snapshots, joins,
            # CTEs, subqueries, views, cursors, and INSERT INTO heap SELECT
            # ------------------------------------------------------------------
            exec_sql(c, 'CREATE VIEW biz.v_t_all AS SELECT id, label, num FROM biz.t_all WHERE id >= 1')
            tt_cols = (
                'id int4, b bool, i2 int2, i8 int8, f4 float4, f8 float8, label text, raw bytea, '
                'd date, ts timestamp, tstz timestamptz, u uuid, j json, num numeric(18,4), emb vector(3)'
            )
            for snap in (s_created, s1, s2, s3_flush, s4, s5, s6):
                tt_rows = fetchall(
                    c,
                    f"SELECT id, b, i2, i8, f4, f8, label, encode(raw,'hex'), d, ts, tstz, u::text, j::text, num, emb::text "
                    f"FROM og_lake.time_travel('biz.t_all'::regclass, %s) AS t({tt_cols}) ORDER BY id",
                    (snap,),
                )
                c.autocommit = False
                try:
                    exec_sql(c, "SELECT og_lake.bind_snapshot('srv_main'::name, %s)", (snap,))
                    bs_rows = fetchall(
                        c,
                        "SELECT id, b, i2, i8, f4, f8, label, encode(raw,'hex'), d, ts, tstz, u::text, j::text, num, emb::text "
                        "FROM biz.t_all ORDER BY id",
                    )
                    assert bs_rows == tt_rows, (snap, bs_rows, tt_rows)

                    # Self-join, CTE, view, cursor, and INSERT INTO ordinary heap table
                    join_rows = fetchall(
                        c,
                        'WITH base AS (SELECT id, label FROM biz.t_all) '
                        'SELECT a.id, b.label FROM base a JOIN biz.v_t_all b ON a.id = b.id ORDER BY a.id',
                    )
                    assert join_rows == [(r[0], r[6]) for r in tt_rows], (snap, join_rows)

                    exec_sql(c, 'DECLARE cur_059 CURSOR FOR SELECT id, label FROM biz.t_all ORDER BY id')
                    cur_rows = fetchall(c, 'FETCH ALL FROM cur_059')
                    exec_sql(c, 'CLOSE cur_059')
                    assert cur_rows == [(r[0], r[6]) for r in tt_rows]

                    exec_sql(c, 'DELETE FROM biz.heap_sink')
                    exec_sql(c, 'INSERT INTO biz.heap_sink SELECT id, label FROM biz.t_all ORDER BY id')
                    assert fetchall(c, 'SELECT id, label FROM biz.heap_sink ORDER BY id') == [(r[0], r[6]) for r in tt_rows]
                    c.commit()
                finally:
                    if c.status != psycopg2.extensions.STATUS_READY:
                        c.rollback()
                    c.autocommit = True
            assert [(r[0], r[1]) for r in fetchall(c, 'SELECT id, label FROM biz.t_all ORDER BY id')] == [
                (1, 'alpha_file_upd'),
                (4, 'delta'),
                (5, 'omega'),
            ]
            record('historical_ordinary_select_multi_type_join_cte_view_cursor_heap_sink')

            # ------------------------------------------------------------------
            # Group 4: Strict read-only enforcement across all write/DDL/maint APIs
            # ------------------------------------------------------------------
            exec_sql(c, """
                CREATE FOREIGN TABLE biz.t_ro_idx (id int4, i8 int8)
                SERVER srv_main OPTIONS (binding_mode 'create', lake_schema 'main')
            """)
            idx_ro_probe = scalar(
                c,
                """SELECT og_lake.create_lake_index(
                    'biz.t_ro_idx'::regclass, 'idx_ro_i8'::name,
                    '{"version":1,"kind":"scalar","method":"btree","column":"i8"}'::jsonb
                )""",
            )
            ro_stmts = [
                ("INSERT INTO biz.t_all(id, label) VALUES (99, 'bad')", '25006'),
                ("UPDATE biz.t_all SET label = 'bad' WHERE id = 1", '25006'),
                ('DELETE FROM biz.t_all WHERE id = 1', '25006'),
                ("CREATE FOREIGN TABLE biz.t_bad (id int4) SERVER srv_main OPTIONS (binding_mode 'create', lake_schema 'main')", '25006'),
                ('ALTER FOREIGN TABLE biz.t_all RENAME TO t_all_renamed', '25006'),
                ('ALTER FOREIGN TABLE biz.t_all ADD COLUMN bad_col int4', '25006'),
                ('DROP FOREIGN TABLE biz.t_all CASCADE', '25006'),
                ("SELECT og_lake.flush('biz.t_all'::regclass)", '25006'),
                ("SELECT og_lake.set_option('biz.t_all'::regclass, 'data_inlining_row_limit', '10')", '25006'),
                ("SELECT og_lake.set_option('srv_main'::name, 'data_inlining_row_limit', '10')", '25006'),
                ("SELECT og_lake.set_partition('biz.t_all'::regclass, ARRAY['id'])", '25006'),
                ("SELECT og_lake.reset_partition('biz.t_all'::regclass)", '25006'),
                ("SELECT og_lake.reclaim_row_heads('biz.t_all'::regclass, 10)", '25006'),
                ("SELECT * FROM og_lake.expire_snapshots('srv_main'::name, ARRAY[%s]::bigint[], NULL, false)" % s1, '25006'),
                (f"SELECT og_lake.build_lake_index('{idx_ro_probe}'::uuid)", '25006'),
                (f"SELECT og_lake.rebuild_lake_index('{idx_ro_probe}'::uuid)", '25006'),
                (f"SELECT og_lake.drop_lake_index('{idx_ro_probe}'::uuid)", '25006'),
                (f"SELECT og_lake.create_lake_index('biz.t_ro_idx'::regclass, 'idx_ro_bad'::name, '{{\"version\":1,\"kind\":\"scalar\",\"method\":\"btree\",\"column\":\"id\"}}'::jsonb)", '25006'),
                ("SELECT og_lake.rewrite_data_files('biz.t_all'::regclass, ARRAY[1]::bigint[])", '25006'),
                ('ANALYZE biz.t_all', '25001'),
            ]
            c.autocommit = False
            try:
                exec_sql(c, "SELECT og_lake.bind_snapshot('srv_main'::name, %s)", (s2,))
                for stmt, code in ro_stmts:
                    exec_sql(c, 'SAVEPOINT sp_ro')
                    assert_raises(c, stmt, expected_sqlstate=code)
                    exec_sql(c, 'ROLLBACK TO SAVEPOINT sp_ro')
                    exec_sql(c, 'RELEASE SAVEPOINT sp_ro')
                # Bound read remains intact after all rejected write attempts!
                assert fetchall(c, 'SELECT id, label FROM biz.t_all ORDER BY id') == [
                    (1, 'alpha'),
                    (2, 'beta_inline_upd'),
                ]
                c.commit()
            finally:
                if c.status != psycopg2.extensions.STATUS_READY:
                    c.rollback()
                c.autocommit = True
            exec_sql(c, 'SELECT og_lake.drop_lake_index(%s::uuid)', (idx_ro_probe,))
            exec_sql(c, 'DROP FOREIGN TABLE biz.t_ro_idx')
            record('strict_readonly_enforcement_all_dml_ddl_maintenance_index_analyze')

            # ------------------------------------------------------------------
            # Group 5: Schema evolution under bind_snapshot (RENAME, ADD DEFAULT,
            # DROP, DROP+RE-ADD same name, SET NOT NULL)
            # ------------------------------------------------------------------
            exec_sql(c, """
                CREATE FOREIGN TABLE biz.t_evo (
                    id int4,
                    label text,
                    legacy_col int4,
                    nullable_then_nn text
                ) SERVER srv_main OPTIONS (binding_mode 'create', lake_schema 'main')
            """)
            exec_sql(c, "INSERT INTO biz.t_evo VALUES (1, 'orig1', 100, 'n1'), (2, 'orig2', 200, 'n2')")
            se_base = current_snap(c, 'srv_main')

            exec_sql(c, 'ALTER FOREIGN TABLE biz.t_evo RENAME COLUMN label TO title')
            se_ren = current_snap(c, 'srv_main')

            exec_sql(c, 'ALTER FOREIGN TABLE biz.t_evo ADD COLUMN weight int8 DEFAULT 77')
            exec_sql(c, "INSERT INTO biz.t_evo(id, title, legacy_col, nullable_then_nn) VALUES (3, 'post_add', 300, 'n3')")
            se_add = current_snap(c, 'srv_main')

            exec_sql(c, 'ALTER FOREIGN TABLE biz.t_evo DROP COLUMN legacy_col')
            se_drop = current_snap(c, 'srv_main')

            exec_sql(c, 'ALTER FOREIGN TABLE biz.t_evo ADD COLUMN legacy_col int4 DEFAULT 999')
            exec_sql(c, "INSERT INTO biz.t_evo(id, title, nullable_then_nn) VALUES (4, 'readded', 'n4')")
            se_readd = current_snap(c, 'srv_main')

            c.autocommit = False
            try:
                # Bound to se_base (before RENAME, before ADD weight, before DROP+READD legacy_col)
                exec_sql(c, "SELECT og_lake.bind_snapshot('srv_main'::name, %s)", (se_base,))
                assert fetchall(c, 'SELECT id, title FROM biz.t_evo ORDER BY id') == [(1, 'orig1'), (2, 'orig2')]
                assert scalar(c, 'SELECT count(*) FROM biz.t_evo') == 2
                # Requesting weight (added later) -> 42703
                exec_sql(c, 'SAVEPOINT sp_col')
                assert_raises(c, 'SELECT id, weight FROM biz.t_evo', expected_sqlstate='42703')
                exec_sql(c, 'ROLLBACK TO SAVEPOINT sp_col')
                assert_raises(c, 'SELECT * FROM biz.t_evo', expected_sqlstate='42703')
                exec_sql(c, 'ROLLBACK TO SAVEPOINT sp_col')
                assert_raises(c, 'SELECT id FROM biz.t_evo WHERE weight = 77', expected_sqlstate='42703')
                exec_sql(c, 'ROLLBACK TO SAVEPOINT sp_col')
                # Whole-row reference includes post-snapshot columns -> 42703
                assert_raises(c, 'SELECT t_evo FROM biz.t_evo', expected_sqlstate='42703')
                exec_sql(c, 'ROLLBACK TO SAVEPOINT sp_col')
                # Re-added legacy_col has a NEW column_id not present at se_base -> 42703!
                assert_raises(c, 'SELECT id, legacy_col FROM biz.t_evo', expected_sqlstate='42703')
                exec_sql(c, 'ROLLBACK TO SAVEPOINT sp_col')
                c.commit()

                # Bound to se_add (weight exists with initial_default 77, new legacy_col still absent)
                exec_sql(c, "SELECT og_lake.bind_snapshot('srv_main'::name, %s)", (se_add,))
                assert fetchall(c, 'SELECT id, title, weight FROM biz.t_evo ORDER BY id') == [
                    (1, 'orig1', 77),
                    (2, 'orig2', 77),
                    (3, 'post_add', 77),
                ]
                exec_sql(c, 'SAVEPOINT sp_col2')
                assert_raises(c, 'SELECT id, legacy_col FROM biz.t_evo', expected_sqlstate='42703')
                exec_sql(c, 'ROLLBACK TO SAVEPOINT sp_col2')
                c.commit()

                # Bound to se_readd (all current live columns exist)
                exec_sql(c, "SELECT og_lake.bind_snapshot('srv_main'::name, %s)", (se_readd,))
                assert fetchall(c, 'SELECT id, title, weight, legacy_col FROM biz.t_evo ORDER BY id') == [
                    (1, 'orig1', 77, 999),
                    (2, 'orig2', 77, 999),
                    (3, 'post_add', 77, 999),
                    (4, 'readded', 77, 999),
                ]
                c.commit()
            finally:
                if c.status != psycopg2.extensions.STATUS_READY:
                    c.rollback()
                c.autocommit = True

            # SET NOT NULL added after se_readd -> reading se_readd rejects with 0A000
            exec_sql(c, 'ALTER FOREIGN TABLE biz.t_evo ALTER COLUMN nullable_then_nn SET NOT NULL')
            c.autocommit = False
            try:
                exec_sql(c, "SELECT og_lake.bind_snapshot('srv_main'::name, %s)", (se_readd,))
                assert_raises(c, 'SELECT id FROM biz.t_evo', expected_sqlstate='0A000')
                c.rollback()
            finally:
                if c.status != psycopg2.extensions.STATUS_READY:
                    c.rollback()
                c.autocommit = True
            record('schema_evolution_rename_add_drop_readd_notnull_under_bind_snapshot')

            # ------------------------------------------------------------------
            # Group 6: SAVEPOINT unwind/promote & PREPARE/EXECUTE plan-cache
            # transitions (including index/Vec/SMP fallback and recovery)
            # ------------------------------------------------------------------
            exec_sql(c, """
                CREATE FOREIGN TABLE biz.t_plan (
                    id int4,
                    k int4,
                    v text
                ) SERVER srv_main OPTIONS (binding_mode 'create', lake_schema 'main')
            """)
            exec_sql(c, "INSERT INTO biz.t_plan VALUES (1, 10, 'p_v1'), (2, 20, 'p_v2')")
            sp_s1 = current_snap(c, 'srv_main')
            idx_plan = scalar(
                c,
                """SELECT og_lake.create_lake_index(
                    'biz.t_plan'::regclass, 'idx_plan_k'::name,
                    '{"version":1,"kind":"scalar","method":"btree","column":"k"}'::jsonb
                )""",
            )
            exec_sql(c, 'SELECT og_lake.build_lake_index(%s::uuid)', (idx_plan,))
            exec_sql(c, "UPDATE biz.t_plan SET v = 'p_v1_live' WHERE id = 1")
            exec_sql(c, "INSERT INTO biz.t_plan VALUES (3, 10, 'p_v3_live')")
            sp_s2 = current_snap(c, 'srv_main')

            exec_sql(c, 'PREPARE p_lookup(int4) AS SELECT id, v FROM biz.t_plan WHERE k = $1 ORDER BY id')
            # Warm generic plan (>5 executions) before bind_snapshot
            for _ in range(6):
                assert fetchall(c, 'EXECUTE p_lookup(10)') == [(1, 'p_v1_live'), (3, 'p_v3_live')]

            c.autocommit = False
            try:
                exec_sql(c, 'SET LOCAL query_dop = 2')
                exec_sql(c, 'SAVEPOINT sp_bind')
                exec_sql(c, "SELECT og_lake.bind_snapshot('srv_main'::name, %s)", (sp_s1,))
                assert fetchall(c, 'EXECUTE p_lookup(10)') == [(1, 'p_v1')]

                # Roll back savepoint -> unwinds bind_snapshot, restores live view & writability!
                exec_sql(c, 'ROLLBACK TO SAVEPOINT sp_bind')
                assert fetchall(c, 'EXECUTE p_lookup(10)') == [(1, 'p_v1_live'), (3, 'p_v3_live')]
                exec_sql(c, "INSERT INTO biz.t_plan VALUES (4, 10, 'p_v4_after_sp')")
                c.commit()
            finally:
                if c.status != psycopg2.extensions.STATUS_READY:
                    c.rollback()
                c.autocommit = True
            sp_s3 = current_snap(c, 'srv_main')

            c.autocommit = False
            try:
                exec_sql(c, 'SAVEPOINT sp_promote')
                exec_sql(c, "SELECT og_lake.bind_snapshot('srv_main'::name, %s)", (sp_s2,))
                exec_sql(c, 'RELEASE SAVEPOINT sp_promote')
                assert fetchall(c, 'EXECUTE p_lookup(10)') == [(1, 'p_v1_live'), (3, 'p_v3_live')]
                c.commit()
            finally:
                if c.status != psycopg2.extensions.STATUS_READY:
                    c.rollback()
                c.autocommit = True
            assert fetchall(c, 'EXECUTE p_lookup(10)') == [
                (1, 'p_v1_live'),
                (3, 'p_v3_live'),
                (4, 'p_v4_after_sp'),
            ]
            exec_sql(c, 'DEALLOCATE p_lookup')
            record('savepoint_unwind_promote_and_prepared_plan_cache_transitions')

            # ------------------------------------------------------------------
            # Group 7: Concurrent expire_snapshots & concurrent writers while
            # bind_snapshot transaction holds pinned snapshot
            # ------------------------------------------------------------------
            c_reader = connect(db59, autocommit=False)
            try:
                exec_sql(c_reader, "SELECT og_lake.bind_snapshot('srv_main'::name, %s)", (sp_s1,))
                # Concurrent session attempts expire_snapshots while BOUND_SNAPSHOT pin is active
                ext_ver = scalar(c, "SELECT extversion FROM pg_extension WHERE extname='og_lake'")
                ver_tuple = tuple(int(x) for x in ext_ver.split('.')[:2])
                expired = fetchall(
                    c,
                    "SELECT snapshot_id FROM og_lake.expire_snapshots('srv_main'::name, ARRAY[%s]::bigint[], NULL, false)",
                    (sp_s1,),
                )
                if ver_tuple >= (0, 68):
                    # SQL0.68+ PGL-5: active BOUND_SNAPSHOT pin blocks expiry until c_reader commits
                    assert expired == [], expired
                else:
                    assert expired == [(sp_s1,)], expired
                exec_sql(c, "INSERT INTO biz.t_plan VALUES (5, 10, 'p_v5_concurrent')")
                # Pinned reader STILL reads sp_s1 cleanly!
                assert fetchall(c_reader, 'SELECT id, v FROM biz.t_plan ORDER BY id') == [
                    (1, 'p_v1'),
                    (2, 'p_v2'),
                ]
                c_reader.commit()
                if ver_tuple >= (0, 68):
                    # Once pin is released, expire_snapshots expires sp_s1 immediately
                    expired_after = fetchall(
                        c,
                        "SELECT snapshot_id FROM og_lake.expire_snapshots('srv_main'::name, ARRAY[%s]::bigint[], NULL, false)",
                        (sp_s1,),
                    )
                    assert expired_after == [(sp_s1,)], expired_after

                # New transaction attempting to bind expired sp_s1 now fails with 22023!
                assert_raises(c_reader, "SELECT og_lake.bind_snapshot('srv_main'::name, %s)", (sp_s1,), expected_sqlstate='22023')
                c_reader.rollback()
            finally:
                c_reader.close()
            record('concurrent_expire_snapshots_and_writer_isolation_under_pinned_bind_snapshot')

            # ------------------------------------------------------------------
            # Group 8: Multi-catalog isolation in a single transaction
            # ------------------------------------------------------------------
            root_second = create_empty_ducklake_root(workdir, 'cat_second')
            exec_sql(c, 'SELECT og_lake.create_catalog(%s::name, %s::name, %s, NULL)',
                     ('srv_second', 'meta_second', root_second))
            exec_sql(c, """
                CREATE FOREIGN TABLE biz.t_sec (id int4, tag text)
                SERVER srv_second OPTIONS (binding_mode 'create', lake_schema 'main')
            """)
            exec_sql(c, "INSERT INTO biz.t_sec VALUES (1, 'sec_v1')")
            sec_s1 = current_snap(c, 'srv_second')
            exec_sql(c, "UPDATE biz.t_sec SET tag = 'sec_v2' WHERE id = 1")

            c.autocommit = False
            try:
                # Bind srv_main to sp_s2 while srv_second stays live AND writable!
                exec_sql(c, "SELECT og_lake.bind_snapshot('srv_main'::name, %s)", (sp_s2,))
                exec_sql(c, "INSERT INTO biz.t_sec VALUES (3, 'sec_tx_new')")
                joined = fetchall(
                    c,
                    'SELECT m.id, m.v, s.tag FROM biz.t_plan m JOIN biz.t_sec s ON m.id = s.id ORDER BY m.id',
                )
                assert joined == [(1, 'p_v1_live', 'sec_v2'), (3, 'p_v3_live', 'sec_tx_new')], joined
                c.commit()

                # Bind BOTH catalogs to distinct historical snapshots in one transaction!
                exec_sql(c, "SELECT og_lake.bind_snapshot('srv_main'::name, %s)", (sp_s2,))
                exec_sql(c, "SELECT og_lake.bind_snapshot('srv_second'::name, %s)", (sec_s1,))
                joined_both = fetchall(
                    c,
                    'SELECT m.id, m.v, s.tag FROM biz.t_plan m JOIN biz.t_sec s ON m.id = s.id ORDER BY m.id',
                )
                assert joined_both == [(1, 'p_v1_live', 'sec_v1')], joined_both
                c.commit()
            finally:
                if c.status != psycopg2.extensions.STATUS_READY:
                    c.rollback()
                c.autocommit = True
            record('multi_catalog_independent_bind_snapshot_and_live_write_isolation')

            # ------------------------------------------------------------------
            # Group 9: og_lake.rebuild_lake_index generational rebuild, retention
            # of reader-pinned artifacts, and SAVEPOINT rollback
            # ------------------------------------------------------------------
            exec_sql(c, "SELECT og_lake.flush('biz.t_plan'::regclass)")
            exec_sql(c, 'SELECT og_lake.build_lake_index(%s::uuid)', (idx_plan,))
            st_before = fetchdicts(c, 'SELECT * FROM og_lake.index_status WHERE index_uuid = %s::uuid', (idx_plan,))
            assert len(st_before) == 1, st_before
            assert st_before[0]['definition_generation'] == 1
            assert st_before[0]['ready_segments'] >= 1
            assert st_before[0]['retired_segments'] == 0
            assert st_before[0]['usable'] is True

            # Record existing segment directory paths on disk to verify retention after rebuild
            seg_dirs_gen1 = [
                r[0] for r in fetchall(
                    c,
                    'SELECT artifact_root_uri FROM og_lake_internal.index_segment '
                    'WHERE index_uuid = %s::uuid AND definition_generation = 1',
                    (idx_plan,),
                )
            ]
            assert len(seg_dirs_gen1) >= 1
            for d in seg_dirs_gen1:
                assert Path(d).exists(), d

            exec_sql(c, 'SELECT og_lake.rebuild_lake_index(%s::uuid)', (idx_plan,))
            st_after = fetchdicts(c, 'SELECT * FROM og_lake.index_status WHERE index_uuid = %s::uuid', (idx_plan,))
            assert len(st_after) == 1
            assert st_after[0]['definition_generation'] == 2, st_after
            assert st_after[0]['ready_segments'] >= 1, st_after
            assert st_after[0]['retired_segments'] == len(seg_dirs_gen1), st_after
            assert st_after[0]['usable'] is True, st_after
            # Prior generation physical artifacts remain on disk (deferred GC protects pinned readers)
            for d in seg_dirs_gen1:
                assert Path(d).exists(), f'pinned artifact deleted prematurely: {d}'

            # SAVEPOINT rollback of rebuild_lake_index restores generation 2
            c.autocommit = False
            try:
                exec_sql(c, 'SAVEPOINT sp_reb')
                exec_sql(c, 'SELECT og_lake.rebuild_lake_index(%s::uuid)', (idx_plan,))
                assert scalar(
                    c,
                    'SELECT definition_generation FROM og_lake.index_status WHERE index_uuid = %s::uuid',
                    (idx_plan,),
                ) == 3
                exec_sql(c, 'ROLLBACK TO SAVEPOINT sp_reb')
                assert scalar(
                    c,
                    'SELECT definition_generation FROM og_lake.index_status WHERE index_uuid = %s::uuid',
                    (idx_plan,),
                ) == 2
                c.commit()
            finally:
                if c.status != psycopg2.extensions.STATUS_READY:
                    c.rollback()
                c.autocommit = True
            assert fetchall(c, 'SELECT id FROM biz.t_plan WHERE k = 10 ORDER BY id') == [(1,), (3,), (4,), (5,)]
            record('rebuild_lake_index_generational_advance_artifact_retention_and_savepoint_rollback')

            # ------------------------------------------------------------------
            # Group 10: Public management status views & metadata functions
            # (catalog_status, table_status, index_status, table_info, list_files, options)
            # ------------------------------------------------------------------
            exec_sql(c, "SELECT og_lake.set_option('biz.t_plan'::regclass, 'data_inlining_row_limit', '128')")
            exec_sql(c, "SELECT og_lake.set_partition('biz.t_sec'::regclass, ARRAY['id'])")
            exec_sql(c, 'ANALYZE biz.t_plan')

            cats = {r['server_name']: r for r in fetchdicts(c, 'SELECT * FROM og_lake.catalog_status')}
            assert 'srv_main' in cats and 'srv_second' in cats, cats
            cm = cats['srv_main']
            assert cm['format_version'] == '1.1'
            assert cm['read_only'] is False
            assert cm['credential_configured'] is False
            assert cm['index_root_configured'] is True
            assert cm['bound_snapshot'] is None
            assert cm['last_committed_snapshot'] == cm['visible_snapshot']
            assert cm['capabilities']['bind_snapshot'] is True
            ext_ver_cap = scalar(c, "SELECT extversion FROM pg_extension WHERE extname='og_lake'")
            cap_ver_tuple = tuple(int(x) for x in ext_ver_cap.split('.')[:2])
            if cap_ver_tuple >= (0, 69):
                assert cm['capabilities']['physical_gc'] is True, cm['capabilities']
            else:
                assert cm['capabilities']['physical_gc'] is False, cm['capabilities']

            tbls = {r['table_name']: r for r in fetchdicts(c, 'SELECT * FROM og_lake.table_status')}
            assert 't_all' in tbls and 't_plan' in tbls and 't_sec' in tbls, tbls
            tp = tbls['t_plan']
            assert tp['active_data_files'] >= 1 and tp['active_data_bytes'] > 0
            assert tp['inline_row_limit'] == 128
            assert tp['active_indexes'] == 1
            assert tp['analyze_committed_changes'] >= 5 and tp['analyze_analyzed_changes'] == 0, tp
            assert tbls['t_sec']['partition_keys'] == 1

            # Under bind_snapshot, views reflect bound snapshot & index fallback!
            c.autocommit = False
            try:
                exec_sql(c, "SELECT og_lake.bind_snapshot('srv_main'::name, %s)", (s_created,))
                cm_b = {r['server_name']: r for r in fetchdicts(c, 'SELECT * FROM og_lake.catalog_status')}['srv_main']
                assert cm_b['bound_snapshot'] == s_created and cm_b['read_only'] is True, cm_b
                ta_b = {r['table_name']: r for r in fetchdicts(c, 'SELECT * FROM og_lake.table_status')}['t_all']
                assert (
                    ta_b['effective_snapshot'] == s_created
                    and ta_b['active_data_files'] == 0
                    and ta_b['active_data_bytes'] == 0
                    and ta_b['read_only'] is True
                ), ta_b
                ix_b = fetchdicts(c, 'SELECT * FROM og_lake.index_status WHERE index_uuid = %s::uuid', (idx_plan,))[0]
                assert ix_b['usable'] is False and ix_b['reason'] == 'bound_snapshot_historical_scan_fallback', ix_b
                c.commit()
            finally:
                if c.status != psycopg2.extensions.STATUS_READY:
                    c.rollback()
                c.autocommit = True

            # Verify no raw root path or encryption key leaks in list_files / views
            lf_rows = fetchdicts(c, "SELECT * FROM og_lake.list_files('biz.t_all'::regclass)")
            assert len(lf_rows) >= 1, lf_rows
            for r in lf_rows:
                assert not r['data_file'].startswith('/'), r
                assert root_main not in r['data_file'], r
                assert r['data_file_encryption_key'] is None and r['delete_file_encryption_key'] is None
            assert_raises(
                c,
                "SELECT * FROM og_lake.list_files('biz.t_all'::regclass, 1, now())",
                expected_sqlstate='22023',
            )
            record('public_status_views_and_metadata_functions_accuracy_and_redaction')

            # ------------------------------------------------------------------
            # Group 11: Multi-role privilege filtering & zero-oracle security
            # ------------------------------------------------------------------
            exec_sql(c, sql.SQL('GRANT USAGE ON SCHEMA biz TO {}, {}, {}').format(
                sql.Identifier(tbl_owner_role), sql.Identifier(u_col), sql.Identifier(u_full)))
            exec_sql(c, sql.SQL('GRANT CREATE ON SCHEMA biz TO {}').format(sql.Identifier(tbl_owner_role)))
            exec_sql(c, sql.SQL('GRANT USAGE ON FOREIGN SERVER srv_main TO {}').format(sql.Identifier(tbl_owner_role)))
            exec_sql(c, sql.SQL('GRANT SELECT (id) ON biz.t_plan TO {}').format(sql.Identifier(u_col)))
            exec_sql(c, sql.SQL('GRANT SELECT ON biz.t_plan TO {}').format(sql.Identifier(u_full)))

            # u_none: 0 rows in all 3 views, 42501 on bind_snapshot/table_info/list_files/options (even bad snap ID!)
            cn = connect(db59, user=u_none, password=user_pwd, autocommit=True)
            try:
                assert fetchall(cn, 'SELECT * FROM og_lake.catalog_status') == []
                assert fetchall(cn, 'SELECT * FROM og_lake.table_status') == []
                assert fetchall(cn, 'SELECT * FROM og_lake.index_status') == []
                assert_raises(cn, "SELECT * FROM og_lake.table_info('srv_main'::name)", expected_sqlstate='42501')
                assert_raises(cn, "SELECT * FROM og_lake.options('srv_main'::name)", expected_sqlstate='42501')
                assert_raises(cn, "SELECT * FROM og_lake.list_files('biz.t_plan'::regclass)", expected_sqlstate='42501')
                cn.autocommit = False
                assert_raises(cn, "SELECT og_lake.bind_snapshot('srv_main'::name, 999999)", expected_sqlstate='42501')
                cn.rollback()
            finally:
                cn.close()

            # u_col: sees ONLY srv_main (with bound_table_count=1), ONLY t_plan, can bind_snapshot & SELECT id,
            # rejected on SELECT v (42501) and rejected on list_files(t_plan) (requires whole-table SELECT -> 42501)!
            cc = connect(db59, user=u_col, password=user_pwd, autocommit=True)
            try:
                cc_cats = fetchdicts(cc, 'SELECT * FROM og_lake.catalog_status')
                assert [r['server_name'] for r in cc_cats] == ['srv_main']
                assert cc_cats[0]['bound_table_count'] == 1
                cc_tbls = fetchdicts(cc, 'SELECT * FROM og_lake.table_status')
                assert [r['table_name'] for r in cc_tbls] == ['t_plan']
                cc_ti = fetchdicts(cc, "SELECT * FROM og_lake.table_info('srv_main'::name)")
                assert [r['table_name'] for r in cc_ti] == ['t_plan']
                cc_ix = fetchdicts(cc, 'SELECT * FROM og_lake.index_status')
                assert [str(r['index_uuid']) for r in cc_ix] == [str(idx_plan)]
                assert_raises(cc, "SELECT * FROM og_lake.list_files('biz.t_plan'::regclass)", expected_sqlstate='42501')
                assert_raises(cc, 'SELECT og_lake.rebuild_lake_index(%s::uuid)', (idx_plan,), expected_sqlstate='42501')

                cc.autocommit = False
                exec_sql(cc, "SELECT og_lake.bind_snapshot('srv_main'::name, %s)", (sp_s2,))
                assert fetchall(cc, 'SELECT id FROM biz.t_plan ORDER BY id') == [(1,), (2,), (3,)]
                exec_sql(cc, 'SAVEPOINT sp_acl')
                assert_raises(cc, 'SELECT id, v FROM biz.t_plan', expected_sqlstate='42501')
                exec_sql(cc, 'ROLLBACK TO SAVEPOINT sp_acl')
                cc.commit()
            finally:
                cc.close()

            # u_full: has whole-table SELECT on t_plan -> can call list_files(t_plan) and options(srv_main),
            # while data_path and options of unauthorized tables remain hidden!
            cf = connect(db59, user=u_full, password=user_pwd, autocommit=True)
            try:
                cf_lf = fetchdicts(cf, "SELECT * FROM og_lake.list_files('biz.t_plan'::regclass)")
                assert len(cf_lf) >= 1
                cf_opts = fetchdicts(cf, "SELECT * FROM og_lake.options('srv_main'::name)")
                opt_names = {r['option_name'] for r in cf_opts}
                assert 'data_path' not in opt_names, cf_opts
                assert 'data_inlining_row_limit' in opt_names, cf_opts
            finally:
                cf.close()
            record('multi_role_privilege_filtering_and_zero_oracle_security')

            # ------------------------------------------------------------------
            # Group 12: Independent pinned DuckLake (0aaa8e10d4...) parity for
            # bind_snapshot AT VERSION, table_info, list_files, and options
            # ------------------------------------------------------------------
            root_ref = create_empty_ducklake_root(workdir, 'cat_ref')
            exec_sql(c, 'SELECT og_lake.create_catalog(%s::name, %s::name, %s, NULL)',
                     ('srv_ref', 'meta_ref', root_ref))
            exec_sql(c, "SELECT og_lake.set_option('srv_ref'::name, 'data_inlining_row_limit', '64')")
            exec_sql(c, """
                CREATE FOREIGN TABLE biz.t_ref (
                    id int4,
                    name text,
                    score float8
                ) SERVER srv_ref OPTIONS (binding_mode 'create', lake_schema 'main')
            """)
            exec_sql(c, "INSERT INTO biz.t_ref VALUES (1, 'r1', 10.50), (2, 'r2', 20.75), (3, 'r3', 30.00)")
            rf1 = current_snap(c, 'srv_ref')
            exec_sql(c, "SELECT og_lake.flush('biz.t_ref'::regclass)")
            rf2 = current_snap(c, 'srv_ref')
            exec_sql(c, "UPDATE biz.t_ref SET name = 'r2_upd', score = 22.25 WHERE id = 2")
            exec_sql(c, 'DELETE FROM biz.t_ref WHERE id = 1')
            exec_sql(c, "INSERT INTO biz.t_ref VALUES (4, 'r4', 40.00)")
            rf3 = current_snap(c, 'srv_ref')
            exec_sql(c, "SELECT og_lake.flush('biz.t_ref'::regclass)")
            rf4 = current_snap(c, 'srv_ref')
            exec_sql(c, "UPDATE biz.t_ref SET score = 44.50 WHERE id = 4")
            rf5 = current_snap(c, 'srv_ref')

            ref_dir = workdir / 'ducklake_ref_059'
            ref_dir.mkdir(parents=True, exist_ok=True)
            names = fetchall(
                c,
                "SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
                "WHERE n.nspname = 'meta_ref' AND c.relkind = 'r' ORDER BY c.relname",
            )
            exported = read_native_catalog(
                c, 'meta_ref', {'tables': [{'table_name': n} for (n,) in names]}, ref_dir / 'native-catalog.json'
            )
            lock = json.loads((TESTS_DIR / 'source-lock.json').read_text(encoding='utf-8'))
            receipt = json.loads((ROOT / '.reference/opengauss-native-lake/build/receipt.json').read_text(encoding='utf-8'))
            ref = Reference(ref_dir, lock, receipt)
            transport = ref_dir / 'transport.ducklake'
            transport_native(ref, transport, exported)

            # 12a. bind_snapshot + SELECT parity vs DuckLake AT (VERSION => sid)
            for sid in (rf1, rf2, rf3, rf4, rf5):
                c.autocommit = False
                try:
                    exec_sql(c, "SELECT og_lake.bind_snapshot('srv_ref'::name, %s)", (sid,))
                    og_rows = fetchall(c, 'SELECT id, name, score FROM biz.t_ref ORDER BY id')
                    c.commit()
                finally:
                    if c.status != psycopg2.extensions.STATUS_READY:
                        c.rollback()
                    c.autocommit = True
                dk_rows = [
                    (r['id'], r['name'], float(r['score']))
                    for r in ref.run(
                        ref.attach(transport, True)
                        + f'SELECT id, name, score FROM lake.main.t_ref AT (VERSION => {sid}) ORDER BY id',
                        f'ref-at-{sid}',
                        result=True,
                    )
                ]
                assert og_rows == dk_rows, (sid, og_rows, dk_rows)

            # 12b. table_info parity vs ducklake_table_info
            og_ti = [
                (r[0], r[1], r[2], str(r[3]), r[4], r[5], r[6], r[7])
                for r in fetchall(c, "SELECT * FROM og_lake.table_info('srv_ref'::name)")
            ]
            dk_ti = [
                (
                    r['table_name'],
                    r['schema_id'],
                    r['table_id'],
                    str(r['table_uuid']),
                    r['file_count'],
                    r['file_size_bytes'],
                    r['delete_file_count'],
                    r['delete_file_size_bytes'],
                )
                for r in ref.run(
                    ref.attach(transport, True) + "SELECT * FROM ducklake_table_info('lake') ORDER BY table_id",
                    'ref-table-info',
                    result=True,
                )
            ]
            assert og_ti == dk_ti, (og_ti, dk_ti)

            # 12c. list_files parity vs ducklake_list_files (head + historical version)
            def norm_rel_path(p):
                if p is None:
                    return None
                s = str(p)
                prefix = str(root_ref).rstrip('/') + '/'
                if s.startswith(prefix):
                    return s[len(prefix):]
                return s

            for label_lf, ver_arg, dk_opt in [
                ('head', None, ''),
                ('rf2', rf2, f', snapshot_version => {rf2}'),
                ('rf4', rf4, f', snapshot_version => {rf4}'),
            ]:
                if ver_arg is None:
                    og_lf = fetchall(c, "SELECT * FROM og_lake.list_files('biz.t_ref'::regclass)")
                else:
                    og_lf = fetchall(c, "SELECT * FROM og_lake.list_files('biz.t_ref'::regclass, %s)", (ver_arg,))
                dk_lf = [
                    (
                        norm_rel_path(r['data_file']),
                        None if r['data_file_size_bytes'] is None else int(r['data_file_size_bytes']),
                        None if r['data_file_footer_size'] is None else int(r['data_file_footer_size']),
                        r['data_file_encryption_key'],
                        norm_rel_path(r['delete_file']),
                        None if r['delete_file_size_bytes'] is None else int(r['delete_file_size_bytes']),
                        None if r['delete_file_footer_size'] is None else int(r['delete_file_footer_size']),
                        r['delete_file_encryption_key'],
                    )
                    for r in ref.run(
                        ref.attach(transport, True)
                        + f"SELECT * FROM ducklake_list_files('lake', 't_ref', schema => 'main'{dk_opt})",
                        f'ref-list-files-{label_lf}',
                        result=True,
                    )
                ]
                assert og_lf == dk_lf, (label_lf, og_lf, dk_lf)

            # 12d. options parity vs ducklake_options
            og_opt = fetchall(
                c,
                "SELECT option_name, value, scope, scope_entry FROM og_lake.options('srv_ref'::name) ORDER BY option_name, scope",
            )
            dk_opt_rows = [
                (r['option_name'], r['value'], r['scope'], r['scope_entry'])
                for r in ref.run(
                    ref.attach(transport, True)
                    + "SELECT option_name, value, scope, scope_entry FROM ducklake_options('lake') ORDER BY option_name, scope",
                    'ref-options',
                    result=True,
                )
            ]
            assert og_opt == dk_opt_rows, (og_opt, dk_opt_rows)
            record('independent_pinned_ducklake_reference_parity_bind_snapshot_table_info_list_files_options')
        finally:
            c.close()

    finally:
        for db in (db58, db59):
            exec_sql(
                admin,
                'SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = %s AND pid <> pg_backend_pid()',
                (db,),
            )
            exec_sql(admin, sql.SQL('DROP DATABASE IF EXISTS {}').format(sql.Identifier(db)))
        for role in (tbl_owner_role, u_none, u_col, u_full, owner_role):
            exec_sql(admin, sql.SQL('DROP ROLE IF EXISTS {}').format(sql.Identifier(role)))
        admin.close()
        shutil.rmtree(workdir, ignore_errors=True)

    result = dict(
        suite='native_059_bind_snapshot_35',
        status='passed',
        profile=PROFILE,
        port=PORT,
        enable_thread_pool=thread_pool_setting,
        postmaster_start_time=postmaster_start,
        binary_path=str(INSTALL / 'bin/gaussdb'),
        binary_sha256=sha_file(INSTALL / 'bin/gaussdb'),
        started_at=started,
        completed_at=datetime.now(timezone.utc).isoformat(),
        check_count=len(checks),
        checks=checks,
    )
    if output_json is not None:
        output_json.parent.mkdir(parents=True, exist_ok=True)
        output_json.write_text(json.dumps(result, indent=2, ensure_ascii=False) + '\n', encoding='utf-8')
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return result


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--output', type=Path)
    args = ap.parse_args()
    run_suite(args.output)


if __name__ == '__main__':
    main()
