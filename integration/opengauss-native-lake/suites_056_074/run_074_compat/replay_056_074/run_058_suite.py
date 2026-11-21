#!/usr/bin/env python3
"""Comprehensive verification suite for og_lake 0.58 CDC functions:
table_insertions, table_deletions, and table_changes (bigint & timestamptz)."""
import argparse
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import secrets
import shutil
import sys
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
        application_name='og_lake_test_058',
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
    workdir = Path('/tmp') / f'og_lake_058_suite_{run_id}'
    workdir.mkdir(parents=True, exist_ok=True)
    db57 = f'og_lake_058_v57_{run_id}'
    db58 = f'og_lake_058_v58_{run_id}'
    owner_role = f'og_owner_{run_id}'
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
        for role in (u_none, u_col, u_full):
            exec_sql(admin, sql.SQL('CREATE ROLE {} LOGIN ENCRYPTED PASSWORD {}').format(
                sql.Identifier(role), sql.Literal(md5_pw(role, user_pwd))))
        exec_sql(admin, sql.SQL("CREATE DATABASE {} DBCOMPATIBILITY 'PG'").format(sql.Identifier(db57)))
        exec_sql(admin, sql.SQL("CREATE DATABASE {} DBCOMPATIBILITY 'PG'").format(sql.Identifier(db58)))

        # 1. Pre-0.58 gate in 0.57 and ALTER EXTENSION UPDATE TO '0.58'.
        c57 = connect(db57, autocommit=True)
        try:
            exec_sql(c57, "CREATE EXTENSION og_lake VERSION '0.57'")
            hs57 = json.loads(scalar(c57, 'SELECT og_lake.host_status()'))
            assert hs57['module_version'] in ('0.58', '0.73', '0.74'), hs57
            assert hs57['table_changes'] is True, hs57
            assert hs57['table_changes_sql_version'] == '0.58', hs57
            assert hs57['time_travel'] is True, hs57
            assert hs57['last_committed_snapshot'] is True, hs57
            assert hs57['snapshot_metadata'] is True, hs57
            assert scalar(
                c57,
                "SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace "
                "WHERE n.nspname = 'og_lake' AND p.proname IN ('table_insertions','table_deletions','table_changes')",
            ) == 0
            exec_sql(c57, 'SELECT og_lake.configure_internal_owner(%s::name)', (owner_role,))
            r57 = create_empty_ducklake_root(workdir, 'cat57')
            exec_sql(c57, 'SELECT og_lake.create_catalog(%s::name, %s::name, %s::text, NULL)',
                     ('srv57', 'meta57', r57))
            exec_sql(c57, """
                CREATE FOREIGN TABLE public.t57 (id int4, v text)
                SERVER srv57 OPTIONS (binding_mode 'create', lake_schema 'main')
            """)
            exec_sql(c57, "INSERT INTO public.t57 VALUES (1, 'v57')")
            s57 = current_snap(c57, 'srv57')
            assert fetchall(
                c57,
                "SELECT * FROM og_lake.time_travel('public.t57'::regclass, %s) AS h(id int4, v text)",
                (s57,),
            ) == [(1, 'v57')]

            exec_sql(c57, "ALTER EXTENSION og_lake UPDATE TO '0.58'")
            cdc_procs = fetchall(
                c57,
                "SELECT p.proname, p.prosecdef, p.provolatile, p.proisstrict, p.proretset, p.proconfig::text "
                "FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace "
                "WHERE n.nspname = 'og_lake' AND p.proname IN ('table_insertions','table_deletions','table_changes') "
                "ORDER BY p.proname, p.oid",
            )
            assert len(cdc_procs) == 6, cdc_procs
            for row in cdc_procs:
                assert row[1:] == (False, 's', False, True, '{"search_path=pg_catalog, pg_temp"}'), row
            assert fetchall(
                c57,
                "SELECT snapshot_id, rowid, change_type, id, v "
                "FROM og_lake.table_changes('public.t57'::regclass, 0, %s) "
                "AS c(snapshot_id bigint, rowid bigint, change_type text, id int4, v text)",
                (s57,),
            ) == [(s57, 0, 'insert', 1, 'v57')]
            record('sql057_pre_upgrade_gate_and_057_to_058_alter_extension')
        finally:
            c57.close()

        # 2. Fresh 0.58 installation + all 15 business types across inline, flush, inline-file-delete, delete-flush & rewrite.
        c = connect(db58, autocommit=True)
        try:
            exec_sql(c, "CREATE EXTENSION og_lake VERSION '0.58'")
            exec_sql(c, 'SELECT og_lake.configure_internal_owner(%s::name)', (owner_role,))
            root_main = create_empty_ducklake_root(workdir, 'cat_main')
            exec_sql(c, 'SELECT og_lake.create_catalog(%s::name, %s::name, %s::text, NULL)',
                     ('srv_main', 'meta_main', root_main))
            exec_sql(c, 'CREATE SCHEMA biz')

            exec_sql(c, """
                CREATE FOREIGN TABLE biz.t_rich (
                    id int4,
                    b bool,
                    i2 int2,
                    i8 int8,
                    f4 float4,
                    f8 float8,
                    txt text,
                    raw bytea,
                    dt date,
                    ts timestamp,
                    tstz timestamptz,
                    u uuid,
                    js json,
                    num numeric(12,2),
                    vec vector(3)
                ) SERVER srv_main OPTIONS (binding_mode 'create', lake_schema 'main')
            """)
            s_rich_create = current_snap(c, 'srv_main')
            exec_sql(c, "SET TIME ZONE 'UTC'")
            exec_sql(c, """
                INSERT INTO biz.t_rich VALUES
                (1, true, 12, 10000000001, 1.5, 2.25, 'alpha', E'\\\\x00ff41', DATE '2026-05-01',
                 TIMESTAMP '2026-05-01 10:20:30.123456', TIMESTAMPTZ '2026-05-01 10:20:30.123456+00',
                 '11111111-2222-3333-4444-555555555555'::uuid, '{"k":1}'::json, 12345.67, '[1,2,3]'::vector(3)),
                (2, false, -7, -99, NULL, -0.5, NULL, NULL, DATE '2025-12-31',
                 NULL, TIMESTAMPTZ '2025-12-31 23:59:59+00',
                 NULL, 'null'::json, -0.05, '[0,0,1]'::vector(3))
            """)
            s_rich_ins1 = current_snap(c, 'srv_main')

            exec_sql(c, "UPDATE biz.t_rich SET txt = 'alpha_v2', num = 999.99 WHERE id = 1")
            s_rich_upd1 = current_snap(c, 'srv_main')
            exec_sql(c, "DELETE FROM biz.t_rich WHERE id = 2")
            s_rich_del1 = current_snap(c, 'srv_main')

            exec_sql(c, "SELECT og_lake.flush('biz.t_rich'::regclass)")
            s_rich_flush1 = current_snap(c, 'srv_main')

            exec_sql(c, """
                INSERT INTO biz.t_rich VALUES
                (3, true, 300, 3000, 3.5, 6.75, 'gamma', E'\\\\xdeadbeef', DATE '2026-06-15',
                 TIMESTAMP '2026-06-15 08:00:00', TIMESTAMPTZ '2026-06-15 08:00:00+00',
                 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee'::uuid, '[1,true,"x"]'::json, 0.00, '[4,5,6]'::vector(3)),
                (4, false, 400, 4000, -1.25, 8.125, 'delta', E'\\\\x', DATE '2026-07-01',
                 TIMESTAMP '2026-07-01 00:00:00', TIMESTAMPTZ '2026-07-01 00:00:00+00',
                 'ffffffff-eeee-dddd-cccc-bbbbbbbbbbbb'::uuid, '{"ok":false}'::json, 88.80, '[7,8,9]'::vector(3))
            """)
            s_rich_ins2 = current_snap(c, 'srv_main')
            exec_sql(c, "SELECT og_lake.flush('biz.t_rich'::regclass)")
            s_rich_flush2 = current_snap(c, 'srv_main')

            # Position delete on flushed Parquet row 1 (UPDATE) + row 4 (DELETE) BEFORE flushing deletes!
            exec_sql(c, "UPDATE biz.t_rich SET num = 1001.50, txt = 'alpha_v3' WHERE id = 1")
            s_rich_upd2 = current_snap(c, 'srv_main')
            exec_sql(c, "DELETE FROM biz.t_rich WHERE id = 4")
            s_rich_del2 = current_snap(c, 'srv_main')

            rich_cols = """
                id int4, b bool, i2 int2, i8 int8, f4 float4, f8 float8,
                txt text, raw bytea, dt date, ts timestamp, tstz timestamptz,
                u uuid, js json, num numeric(12,2), vec vector(3)
            """
            # Verify mixed inline-delete + flushed-file CDC BEFORE delete-flush!
            pre_flush_chg = fetchall(
                c,
                f"SELECT snapshot_id, rowid, change_type, id, txt, num "
                f"FROM og_lake.table_changes('biz.t_rich'::regclass, %s::bigint, %s::bigint) "
                f"AS c(snapshot_id bigint, rowid bigint, change_type text, {rich_cols}) "
                f"ORDER BY snapshot_id, rowid, change_type",
                (s_rich_upd2, s_rich_del2),
            )
            assert pre_flush_chg == [
                (s_rich_upd2, 0, 'update_postimage', 1, 'alpha_v3', Decimal('1001.50')),
                (s_rich_upd2, 0, 'update_preimage', 1, 'alpha_v2', Decimal('999.99')),
                (s_rich_del2, 3, 'delete', 4, 'delta', Decimal('88.80')),
            ], pre_flush_chg

            # Flush deletes -> creates 3-column embedded-snapshot delete file, then rewrite_data_files, then post-rewrite UPDATE!
            exec_sql(c, "SELECT og_lake.flush('biz.t_rich'::regclass)")
            s_rich_flush3 = current_snap(c, 'srv_main')
            file_ids = [
                r[0] for r in fetchall(
                    c,
                    "SELECT data_file_id FROM meta_main.ducklake_data_file "
                    "WHERE end_snapshot IS NULL ORDER BY data_file_id",
                )
            ]
            assert len(file_ids) >= 2, file_ids
            exec_sql(c, "SELECT og_lake.rewrite_data_files('biz.t_rich'::regclass, %s::bigint[])", (file_ids,))
            s_rich_rew = current_snap(c, 'srv_main')
            exec_sql(c, "UPDATE biz.t_rich SET txt = 'gamma_v2', num = 333.33 WHERE id = 3")
            s_rich_upd3 = current_snap(c, 'srv_main')

            # Maintenance snapshots alone emit zero CDC events!
            for maint_s in (s_rich_flush1, s_rich_flush2, s_rich_flush3, s_rich_rew):
                assert fetchall(
                    c,
                    f"SELECT snapshot_id FROM og_lake.table_changes('biz.t_rich'::regclass, %s::bigint, %s::bigint) "
                    f"AS c(snapshot_id bigint, rowid bigint, change_type text, {rich_cols})",
                    (maint_s, maint_s),
                ) == [], maint_s

            full_chg = fetchall(
                c,
                f"SELECT snapshot_id, rowid, change_type, id, txt, encode(raw, 'hex'), u::text, js::text, num, vec::text "
                f"FROM og_lake.table_changes('biz.t_rich'::regclass, %s::bigint, %s::bigint) "
                f"AS c(snapshot_id bigint, rowid bigint, change_type text, {rich_cols}) "
                f"ORDER BY snapshot_id, rowid, change_type",
                (s_rich_create, s_rich_upd3),
            )
            assert full_chg == [
                (s_rich_ins1, 0, 'insert', 1, 'alpha', '00ff41', '11111111-2222-3333-4444-555555555555', '{"k":1}', Decimal('12345.67'), '[1,2,3]'),
                (s_rich_ins1, 1, 'insert', 2, None, None, None, 'null', Decimal('-0.05'), '[0,0,1]'),
                (s_rich_upd1, 0, 'update_postimage', 1, 'alpha_v2', '00ff41', '11111111-2222-3333-4444-555555555555', '{"k":1}', Decimal('999.99'), '[1,2,3]'),
                (s_rich_upd1, 0, 'update_preimage', 1, 'alpha', '00ff41', '11111111-2222-3333-4444-555555555555', '{"k":1}', Decimal('12345.67'), '[1,2,3]'),
                (s_rich_del1, 1, 'delete', 2, None, None, None, 'null', Decimal('-0.05'), '[0,0,1]'),
                (s_rich_ins2, 2, 'insert', 3, 'gamma', 'deadbeef', 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee', '[1,true,"x"]', Decimal('0.00'), '[4,5,6]'),
                (s_rich_ins2, 3, 'insert', 4, 'delta', '', 'ffffffff-eeee-dddd-cccc-bbbbbbbbbbbb', '{"ok":false}', Decimal('88.80'), '[7,8,9]'),
                (s_rich_upd2, 0, 'update_postimage', 1, 'alpha_v3', '00ff41', '11111111-2222-3333-4444-555555555555', '{"k":1}', Decimal('1001.50'), '[1,2,3]'),
                (s_rich_upd2, 0, 'update_preimage', 1, 'alpha_v2', '00ff41', '11111111-2222-3333-4444-555555555555', '{"k":1}', Decimal('999.99'), '[1,2,3]'),
                (s_rich_del2, 3, 'delete', 4, 'delta', '', 'ffffffff-eeee-dddd-cccc-bbbbbbbbbbbb', '{"ok":false}', Decimal('88.80'), '[7,8,9]'),
                (s_rich_upd3, 2, 'update_postimage', 3, 'gamma_v2', 'deadbeef', 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee', '[1,true,"x"]', Decimal('333.33'), '[4,5,6]'),
                (s_rich_upd3, 2, 'update_preimage', 3, 'gamma', 'deadbeef', 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee', '[1,true,"x"]', Decimal('0.00'), '[4,5,6]'),
            ], full_chg

            # Verify both business-only and (snapshot_id, rowid) lineage-prefixed forms of table_insertions & table_deletions!
            ins_with_prefix = fetchall(
                c,
                f"SELECT snapshot_id, rowid, id, txt, num "
                f"FROM og_lake.table_insertions('biz.t_rich'::regclass, %s::bigint, %s::bigint) "
                f"AS i(snapshot_id bigint, rowid bigint, {rich_cols}) ORDER BY snapshot_id, rowid",
                (s_rich_create, s_rich_upd3),
            )
            ins_biz_only = fetchall(
                c,
                f"SELECT id, txt, num "
                f"FROM og_lake.table_insertions('biz.t_rich'::regclass, %s::bigint, %s::bigint) "
                f"AS i({rich_cols}) ORDER BY id, num",
                (s_rich_create, s_rich_upd3),
            )
            assert [(r[2], r[3], r[4]) for r in sorted(ins_with_prefix, key=lambda x: (x[2], x[4]))] == ins_biz_only
            assert len(ins_with_prefix) == 7, ins_with_prefix

            del_with_prefix = fetchall(
                c,
                f"SELECT snapshot_id, rowid, id, txt, num "
                f"FROM og_lake.table_deletions('biz.t_rich'::regclass, %s::bigint, %s::bigint) "
                f"AS d(snapshot_id bigint, rowid bigint, {rich_cols}) ORDER BY snapshot_id, rowid",
                (s_rich_create, s_rich_upd3),
            )
            del_biz_only = fetchall(
                c,
                f"SELECT id, txt, num "
                f"FROM og_lake.table_deletions('biz.t_rich'::regclass, %s::bigint, %s::bigint) "
                f"AS d({rich_cols}) ORDER BY id, num",
                (s_rich_create, s_rich_upd3),
            )
            assert [(r[2], r[3], r[4]) for r in sorted(del_with_prefix, key=lambda x: (x[2], x[4]))] == del_biz_only
            assert len(del_with_prefix) == 5, del_with_prefix
            record('rich_15_types_inline_flush_delete_rewrite_cdc_version_and_lineage_forms')

            # 3. Timestamp bounds (LOWER_BOUND / UPPER_BOUND, empty tail, and start > end).
            exec_sql(c, 'SELECT pg_sleep(0.02)')
            ts_all = fetchall(
                c,
                f"SELECT snapshot_id, rowid, change_type, id, txt "
                f"FROM og_lake.table_changes('biz.t_rich'::regclass, TIMESTAMPTZ '1970-01-01 00:00:00+00', clock_timestamp()) "
                f"AS c(snapshot_id bigint, rowid bigint, change_type text, {rich_cols}) "
                f"ORDER BY snapshot_id, rowid, change_type",
            )
            assert ts_all == [(r[0], r[1], r[2], r[3], r[4]) for r in full_chg], ts_all

            # Empty tail where start_time > last snapshot_time <= clock_timestamp()
            ts_after = scalar(c, 'SELECT clock_timestamp()')
            exec_sql(c, 'SELECT pg_sleep(0.01)')
            assert fetchall(
                c,
                f"SELECT snapshot_id FROM og_lake.table_changes('biz.t_rich'::regclass, %s::timestamptz, clock_timestamp()) "
                f"AS c(snapshot_id bigint, rowid bigint, change_type text, {rich_cols})",
                (ts_after,),
            ) == []
            assert fetchall(
                c,
                f"SELECT id FROM og_lake.table_insertions('biz.t_rich'::regclass, %s::timestamptz, clock_timestamp()) "
                f"AS i({rich_cols})",
                (ts_after,),
            ) == []
            assert fetchall(
                c,
                f"SELECT id FROM og_lake.table_deletions('biz.t_rich'::regclass, %s::timestamptz, clock_timestamp()) "
                f"AS d({rich_cols})",
                (ts_after,),
            ) == []

            # start > end returns 0 rows after validating expectedDesc!
            assert fetchall(
                c,
                f"SELECT snapshot_id FROM og_lake.table_changes('biz.t_rich'::regclass, %s::bigint, %s::bigint) "
                f"AS c(snapshot_id bigint, rowid bigint, change_type text, {rich_cols})",
                (s_rich_upd3, s_rich_ins1),
            ) == []
            assert fetchall(
                c,
                f"SELECT snapshot_id FROM og_lake.table_changes('biz.t_rich'::regclass, clock_timestamp(), TIMESTAMPTZ '2026-11-18 00:00:00+00' + (SELECT snapshot_time - TIMESTAMPTZ '2026-11-18 00:00:00+00' FROM og_lake.snapshots('srv_main'::name) WHERE snapshot_id = %s)) "
                f"AS c(snapshot_id bigint, rowid bigint, change_type text, {rich_cols})",
                (s_rich_ins1,),
            ) == []
            assert_raises(
                c,
                "SELECT * FROM og_lake.table_changes('biz.t_rich'::regclass, %s::bigint, %s::bigint) "
                "AS c(snapshot_id bigint, rowid bigint, change_type text, wrong_only int4)",
                (s_rich_upd3, s_rich_ins1), '42804',
            )
            record('timestamp_bounds_lower_upper_empty_tail_and_start_gt_end')

            # 4. Schema evolution (RENAME COLUMN, ADD COLUMN with literal & NULL defaults, DROP COLUMN) across CDC windows.
            exec_sql(c, """
                CREATE FOREIGN TABLE biz.t_evo (
                    id int4,
                    v text
                ) SERVER srv_main OPTIONS (binding_mode 'create', lake_schema 'main')
            """)
            s_e1 = current_snap(c, 'srv_main')
            exec_sql(c, "INSERT INTO biz.t_evo VALUES (1, 'one'), (2, 'two')")
            s_e2 = current_snap(c, 'srv_main')
            exec_sql(c, "UPDATE biz.t_evo SET v = 'one_v2' WHERE id = 1")
            s_e3 = current_snap(c, 'srv_main')
            exec_sql(c, 'ALTER FOREIGN TABLE biz.t_evo RENAME COLUMN v TO label')
            s_e4 = current_snap(c, 'srv_main')
            exec_sql(c, 'ALTER FOREIGN TABLE biz.t_evo ADD COLUMN score int4 DEFAULT 42')
            s_e5 = current_snap(c, 'srv_main')
            exec_sql(c, 'ALTER FOREIGN TABLE biz.t_evo ADD COLUMN amt numeric(10,2)')
            s_e6 = current_snap(c, 'srv_main')
            exec_sql(c, "INSERT INTO biz.t_evo VALUES (3, 'three', 99, 12.34)")
            s_e7 = current_snap(c, 'srv_main')
            exec_sql(c, "SELECT og_lake.flush('biz.t_evo'::regclass)")
            s_e8 = current_snap(c, 'srv_main')
            exec_sql(c, 'DELETE FROM biz.t_evo WHERE id = 2')
            s_e9 = current_snap(c, 'srv_main')
            exec_sql(c, 'ALTER FOREIGN TABLE biz.t_evo DROP COLUMN label')
            s_e10 = current_snap(c, 'srv_main')
            exec_sql(c, 'INSERT INTO biz.t_evo VALUES (4, 100, 56.78)')
            s_e11 = current_snap(c, 'srv_main')

            # Window ending at s_e9 (before DROP COLUMN label): 4 business columns (id, label, score, amt)
            evo_pre_drop = fetchall(
                c,
                "SELECT snapshot_id, rowid, change_type, id, label, score, amt "
                "FROM og_lake.table_changes('biz.t_evo'::regclass, %s, %s) "
                "AS c(snapshot_id bigint, rowid bigint, change_type text, id int4, label text, score int4, amt numeric(10,2)) "
                "ORDER BY snapshot_id, rowid, change_type",
                (s_e1, s_e9),
            )
            assert evo_pre_drop == [
                (s_e2, 0, 'insert', 1, 'one', 42, None),
                (s_e2, 1, 'insert', 2, 'two', 42, None),
                (s_e3, 0, 'update_postimage', 1, 'one_v2', 42, None),
                (s_e3, 0, 'update_preimage', 1, 'one', 42, None),
                (s_e7, 2, 'insert', 3, 'three', 99, Decimal('12.34')),
                (s_e9, 1, 'delete', 2, 'two', 42, None),
            ], evo_pre_drop

            # Window ending at s_e11 (after DROP COLUMN label): 3 business columns (id, score, amt)
            evo_post_drop = fetchall(
                c,
                "SELECT snapshot_id, rowid, change_type, id, score, amt "
                "FROM og_lake.table_changes('biz.t_evo'::regclass, %s, %s) "
                "AS c(snapshot_id bigint, rowid bigint, change_type text, id int4, score int4, amt numeric(10,2)) "
                "ORDER BY snapshot_id, rowid, change_type",
                (s_e1, s_e11),
            )
            assert evo_post_drop == [
                (s_e2, 0, 'insert', 1, 42, None),
                (s_e2, 1, 'insert', 2, 42, None),
                (s_e3, 0, 'update_postimage', 1, 42, None),
                (s_e3, 0, 'update_preimage', 1, 42, None),
                (s_e7, 2, 'insert', 3, 99, Decimal('12.34')),
                (s_e9, 1, 'delete', 2, 42, None),
                (s_e11, 3, 'insert', 4, 100, Decimal('56.78')),
            ], evo_post_drop
            record('schema_evolution_rename_add_default_drop_column_cdc_projection')

            # 5. Partition evolution (identity & bucket) + CDC.
            exec_sql(c, """
                CREATE FOREIGN TABLE biz.t_part (
                    id int4,
                    grp text,
                    val int8
                ) SERVER srv_main OPTIONS (binding_mode 'create', lake_schema 'main')
            """)
            s_p0 = current_snap(c, 'srv_main')
            exec_sql(c, "SELECT og_lake.set_partition('biz.t_part'::regclass, ARRAY['grp'], ARRAY['identity'])")
            exec_sql(c, "INSERT INTO biz.t_part VALUES (1, 'east', 10), (2, 'west', 20)")
            s_p1 = current_snap(c, 'srv_main')
            exec_sql(c, "SELECT og_lake.flush('biz.t_part'::regclass)")
            exec_sql(c, "SELECT og_lake.set_partition('biz.t_part'::regclass, ARRAY['id'], ARRAY['bucket(4)'])")
            exec_sql(c, "INSERT INTO biz.t_part VALUES (3, 'north', 30)")
            exec_sql(c, "UPDATE biz.t_part SET val = 25 WHERE id = 2")
            s_p2 = current_snap(c, 'srv_main')
            exec_sql(c, "SELECT og_lake.flush('biz.t_part'::regclass)")
            exec_sql(c, "SELECT og_lake.reset_partition('biz.t_part'::regclass)")
            exec_sql(c, "DELETE FROM biz.t_part WHERE id = 1")
            s_p3 = current_snap(c, 'srv_main')

            part_chg = fetchall(
                c,
                "SELECT change_type, id, grp, val "
                "FROM og_lake.table_changes('biz.t_part'::regclass, %s, %s) "
                "AS c(snapshot_id bigint, rowid bigint, change_type text, id int4, grp text, val int8) "
                "ORDER BY snapshot_id, rowid, change_type",
                (s_p0, s_p3),
            )
            assert part_chg == [
                ('insert', 1, 'east', 10),
                ('insert', 2, 'west', 20),
                ('insert', 3, 'north', 30),
                ('update_postimage', 2, 'west', 25),
                ('update_preimage', 2, 'west', 20),
                ('delete', 1, 'east', 10),
            ], part_chg
            record('partition_evolution_identity_bucket_reset_cdc')

            # 6. Self-join, LATERAL, PREPARE/EXECUTE & strict descriptor negative validation.
            joined = fetchall(
                c,
                """
                SELECT i.rowid, i.id, i.amt AS inserted_amt, d.id AS deleted_id
                FROM og_lake.table_insertions('biz.t_evo'::regclass, %s, %s)
                     AS i(snapshot_id bigint, rowid bigint, id int4, score int4, amt numeric(10,2))
                LEFT JOIN og_lake.table_deletions('biz.t_evo'::regclass, %s, %s)
                     AS d(snapshot_id bigint, rowid bigint, id int4, score int4, amt numeric(10,2))
                ON i.rowid = d.rowid
                ORDER BY i.snapshot_id, i.rowid
                """,
                (s_e1, s_e11, s_e1, s_e11),
            )
            assert joined == [
                (0, 1, None, 1),
                (1, 2, None, 2),
                (0, 1, None, 1),
                (2, 3, Decimal('12.34'), None),
                (3, 4, Decimal('56.78'), None),
            ], joined

            lat = fetchall(
                c,
                """
                WITH w(s1, s2) AS (VALUES (%s::bigint, %s::bigint), (%s::bigint, %s::bigint))
                SELECT w.s2, c.change_type, c.id
                FROM w, LATERAL (
                    SELECT * FROM og_lake.table_changes('biz.t_part'::regclass, w.s1, w.s2)
                    AS t(snapshot_id bigint, rowid bigint, change_type text, id int4, grp text, val int8)
                ) AS c
                ORDER BY w.s2, c.change_type, c.id
                """,
                (s_p1, s_p1, s_p3, s_p3),
            )
            assert lat == [(s_p1, 'insert', 1), (s_p1, 'insert', 2), (s_p3, 'delete', 1)], lat

            exec_sql(c, """
                PREPARE p_cdc_058(bigint, bigint) AS
                SELECT change_type, id, v FROM og_lake.table_changes('biz.t_evo'::regclass, $1, $2)
                AS c(snapshot_id bigint, rowid bigint, change_type text, id int4, v text)
                ORDER BY snapshot_id, rowid, change_type
            """)
            assert fetchall(c, 'EXECUTE p_cdc_058(%s, %s)', (s_e3, s_e3)) == [
                ('update_postimage', 1, 'one_v2'),
                ('update_preimage', 1, 'one'),
            ]
            exec_sql(c, 'DEALLOCATE p_cdc_058')

            # Strict descriptor mismatches (42804 / 0A000), including pre-DROP end_snapshot given post-DROP AS list
            assert_raises(
                c,
                "SELECT * FROM og_lake.table_changes('biz.t_evo'::regclass, %s, %s) "
                "AS c(snapshot_id bigint, rowid bigint, change_type text, id int4, score int4, amt numeric(10,2))",
                (s_e9, s_e9), '42804',
            )
            assert_raises(
                c,
                "SELECT * FROM og_lake.table_changes('biz.t_evo'::regclass, %s, %s) "
                "AS c(snapshot_id int4, rowid bigint, change_type text, id int4, score int4, amt numeric(10,2))",
                (s_e11, s_e11), '42804',
            )
            assert_raises(
                c,
                "SELECT * FROM og_lake.table_changes('biz.t_evo'::regclass, %s, %s) "
                "AS c(snapshot_id bigint, rowid bigint, change_type text COLLATE \"C\", id int4, score int4, amt numeric(10,2))",
                (s_e11, s_e11), '42804',
            )
            assert_raises(
                c,
                "SELECT * FROM og_lake.table_insertions('biz.t_evo'::regclass, %s, %s) "
                "AS i(only_one bigint, id int4, score int4, amt numeric(10,2))",
                (s_e11, s_e11), '42804',
            )
            assert_raises(
                c,
                "SELECT * FROM og_lake.table_deletions('biz.t_evo'::regclass, %s, %s) "
                "AS d(id int4, score int4, amt numeric(12,2))",
                (s_e11, s_e11), '42804',
            )
            assert_raises(c, "SELECT og_lake.table_changes('biz.t_evo'::regclass, %s, %s)", (s_e2, s_e3), '0A000')
            record('self_join_lateral_prepared_and_strict_descriptor_validation')

            # 7. Own-transaction journal & uncommitted DDL isolation.
            ctx = connect(db58, autocommit=False)
            try:
                exec_sql(ctx, 'INSERT INTO biz.t_evo (id, score, amt) VALUES (99, 999, 1.11)')
                exec_sql(ctx, 'DELETE FROM biz.t_evo WHERE id = 1')
                exec_sql(ctx, 'UPDATE biz.t_evo SET score = -1 WHERE id = 3')
                assert [r[0] for r in fetchall(ctx, 'SELECT id FROM biz.t_evo ORDER BY id')] == [3, 4, 99]
                cdc_in_tx = fetchall(
                    ctx,
                    "SELECT change_type, id, score FROM og_lake.table_changes('biz.t_evo'::regclass, %s, %s) "
                    "AS c(snapshot_id bigint, rowid bigint, change_type text, id int4, score int4, amt numeric(10,2)) "
                    "ORDER BY snapshot_id, rowid, change_type",
                    (s_e9, s_e11),
                )
                assert cdc_in_tx == [('delete', 2, 42), ('insert', 4, 100)], cdc_in_tx
                assert [r[0] for r in fetchall(ctx, 'SELECT id FROM biz.t_evo ORDER BY id')] == [3, 4, 99]
                ctx.rollback()

                head_before = current_snap(c, 'srv_main')
                exec_sql(ctx, 'ALTER FOREIGN TABLE biz.t_evo ADD COLUMN uncommitted_c int4')
                exec_sql(ctx, 'SAVEPOINT sp_cdc_058')
                assert_raises(
                    ctx,
                    "SELECT * FROM og_lake.table_changes('biz.t_evo'::regclass, %s, %s) "
                    "AS c(snapshot_id bigint, rowid bigint, change_type text, id int4, score int4, amt numeric(10,2), uncommitted_c int4)",
                    (s_e11, head_before + 1), '22023',
                )
                exec_sql(ctx, 'ROLLBACK TO SAVEPOINT sp_cdc_058')
                assert fetchall(
                    ctx,
                    "SELECT change_type, id FROM og_lake.table_changes('biz.t_evo'::regclass, TIMESTAMPTZ '1970-01-01 00:00:00+00', clock_timestamp()) "
                    "AS c(snapshot_id bigint, rowid bigint, change_type text, id int4, score int4, amt numeric(10,2)) "
                    "WHERE snapshot_id = %s",
                    (s_e11,),
                ) == [('insert', 4)]
                ctx.rollback()

                exec_sql(ctx, """
                    CREATE FOREIGN TABLE biz.t_local_new (id int4)
                    SERVER srv_main OPTIONS (binding_mode 'create', lake_schema 'main')
                """)
                assert_raises(
                    ctx,
                    "SELECT * FROM og_lake.table_insertions('biz.t_local_new'::regclass, 0, %s) AS i(id int4)",
                    (head_before,), '55000',
                )
                ctx.rollback()
                record('own_transaction_journal_and_uncommitted_ddl_isolation')
            finally:
                ctx.close()

            # 8. Negative/boundary checks, REPEATABLE READ gate, and expire_snapshots interaction.
            assert_raises(c, "SELECT * FROM og_lake.table_changes(NULL::regclass, 1, 2) AS c(s bigint, r bigint, ct text, id int4)", (), '22004')
            assert_raises(c, "SELECT * FROM og_lake.table_changes('biz.t_evo'::regclass, NULL::bigint, 2) AS c(s bigint, r bigint, ct text, id int4)", (), '22004')
            assert_raises(c, "SELECT * FROM og_lake.table_changes('biz.t_evo'::regclass, 1, NULL::bigint) AS c(s bigint, r bigint, ct text, id int4)", (), '22004')
            assert_raises(c, "SELECT * FROM og_lake.table_insertions('biz.t_evo'::regclass, NULL::timestamptz, now()) AS i(id int4)", (), '22004')
            assert_raises(c, "SELECT * FROM og_lake.table_deletions('biz.t_evo'::regclass, -1, %s) AS d(id int4, score int4, amt numeric(10,2))", (s_e11,), '22023')
            assert_raises(c, "SELECT * FROM og_lake.table_deletions('biz.t_evo'::regclass, 1, -1) AS d(id int4, score int4, amt numeric(10,2))", (), '22023')
            assert_raises(c, "SELECT * FROM og_lake.table_insertions('biz.t_evo'::regclass, 1, 9999999) AS i(id int4)", (), '22023')
            assert_raises(c, "SELECT * FROM og_lake.table_insertions('biz.t_evo'::regclass, 0, 0) AS i(id int4, v text)", (), '22023')
            assert_raises(
                c,
                "SELECT * FROM og_lake.table_changes('biz.t_evo'::regclass, TIMESTAMPTZ '2000-01-01 00:00:00+00', TIMESTAMPTZ '2000-01-02 00:00:00+00') "
                "AS c(s bigint, r bigint, ct text, id int4)",
                (), '22023',
            )
            assert_raises(
                c,
                "SELECT * FROM og_lake.table_changes('biz.t_evo'::regclass, now() - interval '1 hour', now() + interval '1 hour') "
                "AS c(s bigint, r bigint, ct text, id int4)",
                (), '22023',
            )
            assert_raises(
                c,
                "SELECT * FROM og_lake.table_changes('biz.t_evo'::regclass, '-infinity'::timestamptz, now()) "
                "AS c(s bigint, r bigint, ct text, id int4)",
                (), '22023',
            )
            assert_raises(
                c,
                "SELECT * FROM og_lake.table_changes('biz.t_evo'::regclass, now(), 'infinity'::timestamptz) "
                "AS c(s bigint, r bigint, ct text, id int4)",
                (), '22023',
            )
            exec_sql(c, 'CREATE TABLE biz.t_heap (id int4)')
            assert_raises(
                c,
                "SELECT * FROM og_lake.table_insertions('biz.t_heap'::regclass, 0, 1) AS i(id int4)",
                (), '42809',
            )
            assert_raises(
                c,
                "SELECT * FROM og_lake.table_deletions(4294967290::oid::regclass, 0, 1) AS d(id int4)",
                (), '42P01',
            )

            crr = connect(db58, autocommit=False)
            try:
                exec_sql(crr, 'SET TRANSACTION ISOLATION LEVEL REPEATABLE READ')
                assert_raises(
                    crr,
                    "SELECT * FROM og_lake.table_changes('biz.t_evo'::regclass, %s, %s) "
                    "AS c(s bigint, r bigint, ct text, id int4, score int4, amt numeric(10,2))",
                    (s_e1, s_e11), '0A000',
                )
                crr.rollback()
            finally:
                crr.close()

            expired = fetchall(
                c,
                "SELECT snapshot_id FROM og_lake.expire_snapshots('srv_main'::name, ARRAY[%s]::bigint[], NULL, false)",
                (s_e2,),
            )
            assert expired == [(s_e2,)], expired
            assert_raises(
                c,
                "SELECT * FROM og_lake.table_changes('biz.t_evo'::regclass, %s, %s) "
                "AS c(s bigint, r bigint, ct text, id int4, score int4, amt numeric(10,2))",
                (s_e2, s_e11), '22023',
            )
            assert_raises(
                c,
                "SELECT * FROM og_lake.table_changes('biz.t_evo'::regclass, %s, %s) "
                "AS c(s bigint, r bigint, ct text, id int4, v text)",
                (s_e1, s_e2), '22023',
            )
            record('boundary_errors_isolation_gate_and_expire_snapshots_rejection')

            # 9. Ordinary-user schema, table & column ACL gates + dropped-column fence.
            exec_sql(c, """
                CREATE FOREIGN TABLE biz.t_colacl (
                    id int4,
                    pub text
                ) SERVER srv_main OPTIONS (binding_mode 'create', lake_schema 'main')
            """)
            s_acl_0 = current_snap(c, 'srv_main')
            exec_sql(c, "INSERT INTO biz.t_colacl VALUES (10, 'open_v1')")
            s_acl_a = current_snap(c, 'srv_main')
            exec_sql(c, "ALTER FOREIGN TABLE biz.t_colacl ADD COLUMN secret text DEFAULT 'top_secret'")
            s_acl_b = current_snap(c, 'srv_main')

            t_colacl_oid = scalar(c, "SELECT 'biz.t_colacl'::regclass::oid")
            exec_sql(c, sql.SQL('GRANT CONNECT ON DATABASE {} TO {}, {}, {}').format(
                sql.Identifier(db58), sql.Identifier(u_none), sql.Identifier(u_col), sql.Identifier(u_full)))
            exec_sql(c, sql.SQL('GRANT USAGE ON SCHEMA biz TO {}, {}').format(
                sql.Identifier(u_col), sql.Identifier(u_full)))
            exec_sql(c, sql.SQL('GRANT SELECT (id, pub) ON biz.t_colacl TO {}').format(sql.Identifier(u_col)))
            exec_sql(c, sql.SQL('GRANT SELECT ON biz.t_colacl TO {}').format(sql.Identifier(u_full)))

            cn = connect(db58, user=u_none, password=user_pwd, autocommit=True)
            cc = connect(db58, user=u_col, password=user_pwd, autocommit=True)
            cf = connect(db58, user=u_full, password=user_pwd, autocommit=True)
            try:
                assert_raises(
                    cn,
                    'SELECT * FROM og_lake.table_changes(%s::oid::regclass, %s, %s) '
                    'AS c(s bigint, r bigint, ct text, id int4, pub text)',
                    (t_colacl_oid, s_acl_0, s_acl_a), '42501',
                )
                assert fetchall(
                    cc,
                    "SELECT change_type, id, pub FROM og_lake.table_changes('biz.t_colacl'::regclass, %s, %s) "
                    "AS c(s bigint, r bigint, change_type text, id int4, pub text)",
                    (s_acl_0, s_acl_a),
                ) == [('insert', 10, 'open_v1')]
                assert_raises(
                    cc,
                    "SELECT * FROM og_lake.table_changes('biz.t_colacl'::regclass, %s, %s) "
                    "AS c(s bigint, r bigint, ct text, id int4, pub text, secret text)",
                    (s_acl_0, s_acl_b), '42501',
                )
                exec_sql(c, 'ALTER FOREIGN TABLE biz.t_colacl DROP COLUMN pub')
                assert_raises(
                    cc,
                    "SELECT * FROM og_lake.table_changes('biz.t_colacl'::regclass, %s, %s) "
                    "AS c(s bigint, r bigint, ct text, id int4, pub text)",
                    (s_acl_0, s_acl_a), '42501',
                )
                assert fetchall(
                    cf,
                    "SELECT change_type, id, pub FROM og_lake.table_changes('biz.t_colacl'::regclass, %s, %s) "
                    "AS c(s bigint, r bigint, change_type text, id int4, pub text)",
                    (s_acl_0, s_acl_a),
                ) == [('insert', 10, 'open_v1')]
                record('ordinary_user_schema_table_and_column_acl_with_dropped_column_fence')
            finally:
                cn.close()
                cc.close()
                cf.close()

            # 10. Independent pinned DuckLake (0aaa8e10d4...) offline CDC parity check (both rewrite & full column evolution)!
            root_ref = create_empty_ducklake_root(workdir, 'cat_ref')
            exec_sql(c, 'SELECT og_lake.create_catalog(%s::name, %s::name, %s::text, NULL)',
                     ('srv_ref', 'meta_ref', root_ref))

            # 10a. Rewrite + delete-file + post-rewrite DML table
            exec_sql(c, """
                CREATE FOREIGN TABLE biz.t_ref_rew (
                    id int4,
                    label text,
                    active bool
                ) SERVER srv_ref OPTIONS (binding_mode 'create', lake_schema 'main')
            """)
            rw1 = current_snap(c, 'srv_ref')
            exec_sql(c, "INSERT INTO biz.t_ref_rew VALUES (1, 'r1_a', true), (2, 'r1_b', false)")
            rw2 = current_snap(c, 'srv_ref')
            exec_sql(c, "SELECT og_lake.flush('biz.t_ref_rew'::regclass)")
            rw3 = current_snap(c, 'srv_ref')
            exec_sql(c, "INSERT INTO biz.t_ref_rew VALUES (3, 'r2_c', true), (4, 'r2_d', false)")
            rw4 = current_snap(c, 'srv_ref')
            exec_sql(c, "SELECT og_lake.flush('biz.t_ref_rew'::regclass)")
            rw5 = current_snap(c, 'srv_ref')
            exec_sql(c, "UPDATE biz.t_ref_rew SET label = 'r1_a_u1' WHERE id = 1")
            exec_sql(c, 'DELETE FROM biz.t_ref_rew WHERE id = 2')
            rw6 = current_snap(c, 'srv_ref')
            exec_sql(c, "SELECT og_lake.flush('biz.t_ref_rew'::regclass)")
            rw7 = current_snap(c, 'srv_ref')
            ref_files = [
                r[0] for r in fetchall(
                    c,
                    "SELECT data_file_id FROM meta_ref.ducklake_data_file "
                    "WHERE end_snapshot IS NULL ORDER BY data_file_id",
                )
            ]
            assert len(ref_files) >= 2, ref_files
            exec_sql(c, "SELECT og_lake.rewrite_data_files('biz.t_ref_rew'::regclass, %s::bigint[])", (ref_files,))
            rw8 = current_snap(c, 'srv_ref')
            exec_sql(c, "UPDATE biz.t_ref_rew SET label = 'r2_c_post_rew' WHERE id = 3")
            exec_sql(c, 'DELETE FROM biz.t_ref_rew WHERE id = 4')
            rw9 = current_snap(c, 'srv_ref')

            # 10b. Schema evolution (RENAME, ADD DEFAULT, flush, file UPDATE/DELETE, DROP) table
            exec_sql(c, """
                CREATE FOREIGN TABLE biz.t_ref_cdc (
                    id int4,
                    label text,
                    active bool
                ) SERVER srv_ref OPTIONS (binding_mode 'create', lake_schema 'main')
            """)
            rs1 = current_snap(c, 'srv_ref')
            exec_sql(c, "INSERT INTO biz.t_ref_cdc VALUES (1, 'v1_a', true), (2, 'v1_b', false)")
            rs2 = current_snap(c, 'srv_ref')
            exec_sql(c, "UPDATE biz.t_ref_cdc SET label = 'v1_a_u1' WHERE id = 1")
            rs3 = current_snap(c, 'srv_ref')
            exec_sql(c, 'ALTER FOREIGN TABLE biz.t_ref_cdc RENAME COLUMN label TO title')
            rs4 = current_snap(c, 'srv_ref')
            exec_sql(c, 'ALTER FOREIGN TABLE biz.t_ref_cdc ADD COLUMN weight int8 DEFAULT 77')
            rs5 = current_snap(c, 'srv_ref')
            exec_sql(c, "INSERT INTO biz.t_ref_cdc VALUES (3, 'v2_c', true, 300)")
            rs6 = current_snap(c, 'srv_ref')
            exec_sql(c, "SELECT og_lake.flush('biz.t_ref_cdc'::regclass)")
            rs7 = current_snap(c, 'srv_ref')
            exec_sql(c, 'UPDATE biz.t_ref_cdc SET weight = 888 WHERE id = 1')
            exec_sql(c, 'DELETE FROM biz.t_ref_cdc WHERE id = 2')
            rs8 = current_snap(c, 'srv_ref')
            exec_sql(c, "SELECT og_lake.flush('biz.t_ref_cdc'::regclass)")
            rs9 = current_snap(c, 'srv_ref')
            exec_sql(c, 'ALTER FOREIGN TABLE biz.t_ref_cdc DROP COLUMN active')
            rs12 = current_snap(c, 'srv_ref')
            exec_sql(c, "INSERT INTO biz.t_ref_cdc VALUES (4, 'v3_d', 400)")
            rs13 = current_snap(c, 'srv_ref')

            ref_dir = workdir / 'duck_ref'
            ref_dir.mkdir(parents=True, exist_ok=True)
            names = fetchall(
                c,
                "SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
                "WHERE n.nspname = 'meta_ref' AND c.relkind = 'r' ORDER BY c.relname",
            )
            native_cat = read_native_catalog(
                c, 'meta_ref', {'tables': [{'table_name': n} for (n,) in names]}, ref_dir / 'native-catalog.json'
            )
            lock = json.loads((TESTS_DIR / 'source-lock.json').read_text(encoding='utf-8'))
            receipt = json.loads((ROOT / '.reference/opengauss-native-lake/build/receipt.json').read_text(encoding='utf-8'))
            ref = Reference(ref_dir, lock, receipt)
            transport = ref_dir / 'transport.ducklake'
            transport_native(ref, transport, native_cat)

            windows = [
                ('t_ref_rew', rw1, rw9, 'id int4, label text, active bool', 'id, label, active'),
                ('t_ref_rew', rw5, rw9, 'id int4, label text, active bool', 'id, label, active'),
                ('t_ref_rew', rw8, rw8, 'id int4, label text, active bool', 'id, label, active'),
                ('t_ref_cdc', rs3, rs3, 'id int4, label text, active bool', 'id, label, active'),
                ('t_ref_cdc', rs1, rs8, 'id int4, title text, active bool, weight int8', 'id, title, active, weight'),
                ('t_ref_cdc', rs7, rs9, 'id int4, title text, active bool, weight int8', 'id, title, active, weight'),
                ('t_ref_cdc', rs1, rs13, 'id int4, title text, weight int8', 'id, title, weight'),
            ]
            for idx_w, (tbl_name, w_start, w_end, biz_defs, biz_proj) in enumerate(windows):
                og_chg = fetchall(
                    c,
                    f"SELECT snapshot_id, rowid, change_type, {biz_proj} "
                    f"FROM og_lake.table_changes('biz.{tbl_name}'::regclass, %s, %s) "
                    f"AS c(snapshot_id bigint, rowid bigint, change_type text, {biz_defs}) "
                    f"ORDER BY snapshot_id, rowid, change_type",
                    (w_start, w_end),
                )
                dk_chg = [
                    tuple(r.values())
                    for r in ref.run(
                        ref.attach(transport, True)
                        + f"SELECT snapshot_id, rowid, change_type, {biz_proj} "
                        + f"FROM ducklake_table_changes('lake', 'main', '{tbl_name}', {w_start}, {w_end}) "
                        + "ORDER BY snapshot_id, rowid, change_type",
                        f'ref-chg-{idx_w}',
                        result=True,
                    )
                ]
                assert og_chg == dk_chg, (tbl_name, w_start, w_end, og_chg, dk_chg)

                og_ins = fetchall(
                    c,
                    f"SELECT snapshot_id, rowid, {biz_proj} "
                    f"FROM og_lake.table_insertions('biz.{tbl_name}'::regclass, %s, %s) "
                    f"AS i(snapshot_id bigint, rowid bigint, {biz_defs}) "
                    f"ORDER BY snapshot_id, rowid",
                    (w_start, w_end),
                )
                dk_ins = [
                    tuple(r.values())
                    for r in ref.run(
                        ref.attach(transport, True)
                        + f"SELECT snapshot_id, rowid, {biz_proj} "
                        + f"FROM ducklake_table_insertions('lake', 'main', '{tbl_name}', {w_start}, {w_end}) "
                        + "ORDER BY snapshot_id, rowid",
                        f'ref-ins-{idx_w}',
                        result=True,
                    )
                ]
                assert og_ins == dk_ins, (tbl_name, w_start, w_end, og_ins, dk_ins)

                og_del = fetchall(
                    c,
                    f"SELECT snapshot_id, rowid, {biz_proj} "
                    f"FROM og_lake.table_deletions('biz.{tbl_name}'::regclass, %s, %s) "
                    f"AS d(snapshot_id bigint, rowid bigint, {biz_defs}) "
                    f"ORDER BY snapshot_id, rowid",
                    (w_start, w_end),
                )
                dk_del = [
                    tuple(r.values())
                    for r in ref.run(
                        ref.attach(transport, True)
                        + f"SELECT snapshot_id, rowid, {biz_proj} "
                        + f"FROM ducklake_table_deletions('lake', 'main', '{tbl_name}', {w_start}, {w_end}) "
                        + "ORDER BY snapshot_id, rowid",
                        f'ref-del-{idx_w}',
                        result=True,
                    )
                ]
                assert og_del == dk_del, (tbl_name, w_start, w_end, og_del, dk_del)
            record('independent_pinned_ducklake_reference_cdc_parity')
        finally:
            c.close()

    finally:
        for db in (db57, db58):
            exec_sql(admin, 'SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = %s AND pid <> pg_backend_pid()', (db,))
            exec_sql(admin, sql.SQL('DROP DATABASE IF EXISTS {}').format(sql.Identifier(db)))
        for role in (u_none, u_col, u_full, owner_role):
            exec_sql(admin, sql.SQL('DROP ROLE IF EXISTS {}').format(sql.Identifier(role)))
        admin.close()
        shutil.rmtree(workdir, ignore_errors=True)

    result = dict(
        suite='native_058_table_changes_22',
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
