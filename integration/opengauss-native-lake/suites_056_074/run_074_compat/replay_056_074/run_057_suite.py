#!/usr/bin/env python3
"""Comprehensive verification suite for og_lake 0.57 time_travel(regclass, bigint / timestamptz)."""
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
        application_name='og_lake_test_057',
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


def last_committed(conn, server):
    rows = fetchall(conn, 'SELECT id FROM og_lake.last_committed_snapshot(%s::name)', (server,))
    assert len(rows) == 1 and len(rows[0]) == 1, f'expected 1 row, got {rows!r}'
    return rows[0][0]


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
    workdir = Path('/tmp') / f'og_lake_057_suite_{run_id}'
    workdir.mkdir(parents=True, exist_ok=True)
    db56 = f'og_lake_057_v56_{run_id}'
    db57 = f'og_lake_057_v57_{run_id}'
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
        exec_sql(admin, sql.SQL("CREATE DATABASE {} DBCOMPATIBILITY 'PG'").format(sql.Identifier(db56)))
        exec_sql(admin, sql.SQL("CREATE DATABASE {} DBCOMPATIBILITY 'PG'").format(sql.Identifier(db57)))

        # 1. Pre-0.57 gate in 0.56 and ALTER EXTENSION UPDATE TO '0.57'.
        c56 = connect(db56, autocommit=True)
        try:
            exec_sql(c56, "CREATE EXTENSION og_lake VERSION '0.56'")
            hs56 = json.loads(scalar(c56, 'SELECT og_lake.host_status()'))
            assert hs56['module_version'] in ('0.57', '0.73', '0.74'), hs56
            assert hs56['time_travel'] is True, hs56
            assert hs56['time_travel_sql_version'] == '0.57', hs56
            assert hs56['last_committed_snapshot'] is True, hs56
            assert hs56['snapshot_metadata'] is True, hs56
            assert scalar(
                c56,
                "SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace "
                "WHERE n.nspname = 'og_lake' AND p.proname = 'time_travel'",
            ) == 0
            exec_sql(c56, 'SELECT og_lake.configure_internal_owner(%s::name)', (owner_role,))
            r56 = create_empty_ducklake_root(workdir, 'cat56')
            exec_sql(c56, 'SELECT og_lake.create_catalog(%s::name, %s::name, %s::text, NULL)',
                     ('srv56', 'meta56', r56))
            assert current_snap(c56, 'srv56') == 0
            assert last_committed(c56, 'srv56') is None
            exec_sql(c56, "ALTER EXTENSION og_lake UPDATE TO '0.57'")
            tt_procs = fetchall(
                c56,
                "SELECT p.prosecdef, p.provolatile, p.proisstrict, p.proretset, p.proconfig::text "
                "FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace "
                "WHERE n.nspname = 'og_lake' AND p.proname = 'time_travel' ORDER BY p.oid",
            )
            assert len(tt_procs) == 2, tt_procs
            for row in tt_procs:
                assert row == (False, 's', False, True, '{"search_path=pg_catalog, pg_temp"}'), row
            record('sql056_pre_upgrade_gate_and_056_to_057_alter_extension')
        finally:
            c56.close()

        # 2. Fresh 0.57 installation + all 15 business types across inline, flush, delete-flush & rewrite.
        c = connect(db57, autocommit=True)
        try:
            exec_sql(c, "CREATE EXTENSION og_lake VERSION '0.57'")
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

            # Position delete on flushed Parquet row 1 (UPDATE) + row 4 (DELETE), then flush deletes + rewrite files.
            exec_sql(c, "UPDATE biz.t_rich SET num = 1001.50, txt = 'alpha_v3' WHERE id = 1")
            s_rich_upd2 = current_snap(c, 'srv_main')
            exec_sql(c, "DELETE FROM biz.t_rich WHERE id = 4")
            s_rich_del2 = current_snap(c, 'srv_main')
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

            rich_as = """
                AS h(
                    id int4, b bool, i2 int2, i8 int8, f4 float4, f8 float8,
                    txt text, raw bytea, dt date, ts timestamp, tstz timestamptz,
                    u uuid, js json, num numeric(12,2), vec vector(3)
                )
            """
            def query_rich_ver(ver):
                rows = fetchall(
                    c,
                    f"SELECT id, b, i2, i8, f4, f8, txt, encode(raw, 'hex'), dt::text, ts::text, "
                    f"tstz::text, u::text, js::text, num, vec::text "
                    f"FROM og_lake.time_travel('biz.t_rich'::regclass, %s::bigint) {rich_as} ORDER BY id",
                    (ver,),
                )
                return rows

            assert query_rich_ver(s_rich_create) == []
            r_ins1 = query_rich_ver(s_rich_ins1)
            assert [(r[0], r[6], r[7], r[11], r[12], r[13], r[14]) for r in r_ins1] == [
                (1, 'alpha', '00ff41', '11111111-2222-3333-4444-555555555555', '{"k":1}', Decimal('12345.67'), '[1,2,3]'),
                (2, None, None, None, 'null', Decimal('-0.05'), '[0,0,1]'),
            ], r_ins1
            assert [(r[0], r[6], r[13]) for r in query_rich_ver(s_rich_upd1)] == [
                (1, 'alpha_v2', Decimal('999.99')),
                (2, None, Decimal('-0.05')),
            ]
            assert [(r[0], r[6], r[13]) for r in query_rich_ver(s_rich_del1)] == [
                (1, 'alpha_v2', Decimal('999.99')),
            ]
            assert query_rich_ver(s_rich_flush1) == query_rich_ver(s_rich_del1)
            assert [r[0] for r in query_rich_ver(s_rich_ins2)] == [1, 3, 4]
            assert query_rich_ver(s_rich_flush2) == query_rich_ver(s_rich_ins2)
            assert [(r[0], r[6], r[13]) for r in query_rich_ver(s_rich_upd2)] == [
                (1, 'alpha_v3', Decimal('1001.50')),
                (3, 'gamma', Decimal('0.00')),
                (4, 'delta', Decimal('88.80')),
            ]
            expected_final = [
                (1, 'alpha_v3', Decimal('1001.50')),
                (3, 'gamma', Decimal('0.00')),
            ]
            for sid in (s_rich_del2, s_rich_flush3, s_rich_rew):
                assert [(r[0], r[6], r[13]) for r in query_rich_ver(sid)] == expected_final
            record('rich_15_types_inline_flush_delete_rewrite_version_time_travel')

            # Verify timestamp overload at exact snapshot_time and +1us across snapshots.
            snap_times = dict(fetchall(
                c,
                "SELECT snapshot_id, snapshot_time FROM og_lake.snapshots('srv_main'::name) "
                "WHERE snapshot_id >= %s ORDER BY snapshot_id",
                (s_rich_create,),
            ))
            for sid in (s_rich_create, s_rich_ins1, s_rich_del1, s_rich_ins2, s_rich_rew):
                ts_val = snap_times[sid]
                # Ensure no later snapshot shares the exact same microsecond before asserting exact match
                later_same_ts = [k for k, v in snap_times.items() if k > sid and v == ts_val]
                if not later_same_ts:
                    rows_ts = fetchall(
                        c,
                        f"SELECT id, b, i2, i8, f4, f8, txt, encode(raw, 'hex'), dt::text, ts::text, "
                        f"tstz::text, u::text, js::text, num, vec::text "
                        f"FROM og_lake.time_travel('biz.t_rich'::regclass, %s::timestamptz) {rich_as} ORDER BY id",
                        (ts_val,),
                    )
                    assert rows_ts == query_rich_ver(sid), (sid, rows_ts)
            # Intermediate timestamp (+1us after s_rich_ins1, provided next snapshot is > ts + 1us)
            exec_sql(c, "SELECT pg_sleep(0.02)")
            exec_sql(c, "INSERT INTO biz.t_rich (id, txt, num) VALUES (5, 'epsilon', 5.55)")
            s_rich_ins3 = current_snap(c, 'srv_main')
            snap_times2 = dict(fetchall(
                c,
                "SELECT snapshot_id, snapshot_time FROM og_lake.snapshots('srv_main'::name) "
                "WHERE snapshot_id IN (%s, %s)",
                (s_rich_rew, s_rich_ins3),
            ))
            mid_ts = snap_times2[s_rich_rew] + timedelta(milliseconds=5)
            assert mid_ts < snap_times2[s_rich_ins3], snap_times2
            rows_mid = fetchall(
                c,
                f"SELECT id, txt, num FROM og_lake.time_travel('biz.t_rich'::regclass, %s::timestamptz) "
                f"{rich_as} ORDER BY id",
                (mid_ts,),
            )
            assert rows_mid == expected_final, rows_mid
            record('timestamptz_exact_and_intermediate_resolution')

            # 3. Schema evolution across history: RENAME, ADD (literal & NULL) + post-ADD flush (LakeReadFutureColumns!),
            #    SET/DROP DEFAULT, and DROP COLUMN!
            exec_sql(c, """
                CREATE FOREIGN TABLE biz.t_evo (
                    id int4,
                    name text
                ) SERVER srv_main OPTIONS (binding_mode 'create', lake_schema 'main')
            """)
            s_e1 = current_snap(c, 'srv_main')
            exec_sql(c, "INSERT INTO biz.t_evo VALUES (1, 'alice'), (2, 'bob')")
            s_e2 = current_snap(c, 'srv_main')
            exec_sql(c, "ALTER FOREIGN TABLE biz.t_evo RENAME COLUMN name TO full_name")
            s_e3 = current_snap(c, 'srv_main')
            exec_sql(c, "ALTER FOREIGN TABLE biz.t_evo ADD COLUMN score int4 DEFAULT 42")
            s_e4 = current_snap(c, 'srv_main')
            exec_sql(c, "ALTER FOREIGN TABLE biz.t_evo ADD COLUMN amt numeric(10,2)")
            s_e5 = current_snap(c, 'srv_main')
            exec_sql(c, "INSERT INTO biz.t_evo VALUES (3, 'carol', 99, 12.34)")
            s_e6 = current_snap(c, 'srv_main')

            # Flush AFTER ADD COLUMN: flushed Parquet file has begin_snapshot = s_e2 and physically contains future
            # columns `score` (s_e4) and `amt` (s_e5)!
            exec_sql(c, "SELECT og_lake.flush('biz.t_evo'::regclass)")
            s_e7 = current_snap(c, 'srv_main')

            assert fetchall(
                c, "SELECT * FROM og_lake.time_travel('biz.t_evo'::regclass, %s) AS h(id int4, name text)", (s_e1,)
            ) == []
            assert fetchall(
                c,
                "SELECT * FROM og_lake.time_travel('biz.t_evo'::regclass, %s) AS h(id int4, name text) ORDER BY id",
                (s_e2,),
            ) == [(1, 'alice'), (2, 'bob')]
            assert fetchall(
                c,
                "SELECT * FROM og_lake.time_travel('biz.t_evo'::regclass, %s) AS h(k int4, fn text) ORDER BY k",
                (s_e3,),
            ) == [(1, 'alice'), (2, 'bob')]
            assert fetchall(
                c,
                "SELECT * FROM og_lake.time_travel('biz.t_evo'::regclass, %s) "
                "AS h(id int4, full_name text, score int4) ORDER BY id",
                (s_e4,),
            ) == [(1, 'alice', 42), (2, 'bob', 42)]
            assert fetchall(
                c,
                "SELECT * FROM og_lake.time_travel('biz.t_evo'::regclass, %s) "
                "AS h(id int4, full_name text, score int4, amt numeric(10,2)) ORDER BY id",
                (s_e5,),
            ) == [(1, 'alice', 42, None), (2, 'bob', 42, None)]
            assert fetchall(
                c,
                "SELECT * FROM og_lake.time_travel('biz.t_evo'::regclass, %s) "
                "AS h(id int4, full_name text, score int4, amt numeric(10,2)) ORDER BY id",
                (s_e6,),
            ) == [(1, 'alice', 42, None), (2, 'bob', 42, None), (3, 'carol', 99, Decimal('12.34'))]
            record('schema_evolution_rename_add_default_and_post_add_flush_future_columns')

            # Alter current default & DROP COLUMN full_name: historical snapshots must still project full_name & score=42!
            exec_sql(c, "ALTER FOREIGN TABLE biz.t_evo ALTER COLUMN score SET DEFAULT 100")
            s_e8 = current_snap(c, 'srv_main')
            exec_sql(c, "ALTER FOREIGN TABLE biz.t_evo DROP COLUMN full_name")
            s_e9 = current_snap(c, 'srv_main')
            exec_sql(c, "INSERT INTO biz.t_evo (id, amt) VALUES (4, 56.78)")
            s_e10 = current_snap(c, 'srv_main')

            assert fetchall(
                c,
                "SELECT * FROM og_lake.time_travel('biz.t_evo'::regclass, %s) AS h(id int4, name text) ORDER BY id",
                (s_e2,),
            ) == [(1, 'alice'), (2, 'bob')]
            assert fetchall(
                c,
                "SELECT * FROM og_lake.time_travel('biz.t_evo'::regclass, %s) "
                "AS h(id int4, full_name text, score int4) ORDER BY id",
                (s_e4,),
            ) == [(1, 'alice', 42), (2, 'bob', 42)]
            assert fetchall(
                c,
                "SELECT * FROM og_lake.time_travel('biz.t_evo'::regclass, %s) "
                "AS h(id int4, score int4, amt numeric(10,2)) ORDER BY id",
                (s_e10,),
            ) == [
                (1, 42, None),
                (2, 42, None),
                (3, 99, Decimal('12.34')),
                (4, 100, Decimal('56.78')),
            ]
            record('alter_default_and_drop_column_historical_recovery')

            # Partition evolution across history: set_partition -> insert -> flush -> reset_partition -> insert
            exec_sql(c, """
                CREATE FOREIGN TABLE biz.t_part (
                    id int4,
                    region text,
                    v int8
                ) SERVER srv_main OPTIONS (binding_mode 'create', lake_schema 'main')
            """)
            exec_sql(c, "INSERT INTO biz.t_part VALUES (1, 'east', 10)")
            s_p1 = current_snap(c, 'srv_main')
            exec_sql(c, "SELECT og_lake.set_partition('biz.t_part'::regclass, ARRAY['region'])")
            exec_sql(c, "INSERT INTO biz.t_part VALUES (2, 'west', 20), (3, 'east', 30)")
            s_p2 = current_snap(c, 'srv_main')
            exec_sql(c, "SELECT og_lake.flush('biz.t_part'::regclass)")
            exec_sql(c, "SELECT og_lake.reset_partition('biz.t_part'::regclass)")
            exec_sql(c, "INSERT INTO biz.t_part VALUES (4, 'north', 40)")
            s_p3 = current_snap(c, 'srv_main')
            assert fetchall(
                c,
                "SELECT * FROM og_lake.time_travel('biz.t_part'::regclass, %s) AS h(id int4, region text, v int8) ORDER BY id",
                (s_p1,),
            ) == [(1, 'east', 10)]
            assert fetchall(
                c,
                "SELECT * FROM og_lake.time_travel('biz.t_part'::regclass, %s) AS h(id int4, region text, v int8) ORDER BY id",
                (s_p2,),
            ) == [(1, 'east', 10), (2, 'west', 20), (3, 'east', 30)]
            assert fetchall(
                c,
                "SELECT * FROM og_lake.time_travel('biz.t_part'::regclass, %s) AS h(id int4, region text, v int8) ORDER BY id",
                (s_p3,),
            ) == [(1, 'east', 10), (2, 'west', 20), (3, 'east', 30), (4, 'north', 40)]
            record('partition_set_flush_reset_evolution_time_travel')

            # 4. Self-join across two different schemas/snapshots, LATERAL, PREPARE/EXECUTE, and strict AS checking.
            joined = fetchall(
                c,
                """
                SELECT old_v.k, old_v.nm, new_v.sc, new_v.am
                FROM og_lake.time_travel('biz.t_evo'::regclass, %s) AS old_v(k int4, nm text)
                JOIN og_lake.time_travel('biz.t_evo'::regclass, %s) AS new_v(k int4, sc int4, am numeric(10,2))
                  ON old_v.k = new_v.k
                ORDER BY old_v.k
                """,
                (s_e2, s_e10),
            )
            assert joined == [(1, 'alice', 42, None), (2, 'bob', 42, None)], joined

            lat = fetchall(
                c,
                """
                SELECT s.sid, h.k, h.nm
                FROM (VALUES (%s::bigint), (%s::bigint)) AS s(sid),
                LATERAL (SELECT * FROM og_lake.time_travel('biz.t_evo'::regclass, s.sid) AS t(k int4, nm text)) AS h
                ORDER BY s.sid, h.k
                """,
                (s_e2, s_e3),
            )
            assert lat == [(s_e2, 1, 'alice'), (s_e2, 2, 'bob'), (s_e3, 1, 'alice'), (s_e3, 2, 'bob')], lat

            exec_sql(c, "PREPARE tt_plan_057(bigint) AS SELECT * FROM og_lake.time_travel('biz.t_evo'::regclass, $1) AS h(id int4, nm text) ORDER BY id")
            assert fetchall(c, 'EXECUTE tt_plan_057(%s)', (s_e1,)) == []
            assert fetchall(c, 'EXECUTE tt_plan_057(%s)', (s_e2,)) == [(1, 'alice'), (2, 'bob')]
            assert_raises(c, 'EXECUTE tt_plan_057(%s)', (s_e4,), '42804')
            exec_sql(c, 'DEALLOCATE tt_plan_057')

            # Strict AS checks EVEN ON EMPTY SNAPSHOT (s_e1 & s_rich_create)
            assert_raises(
                c,
                "SELECT * FROM og_lake.time_travel('biz.t_evo'::regclass, %s) AS h(id int4)",
                (s_e1,), '42804',
            )
            assert_raises(
                c,
                "SELECT * FROM og_lake.time_travel('biz.t_evo'::regclass, %s) AS h(id int8, name text)",
                (s_e1,), '42804',
            )
            assert_raises(
                c,
                "SELECT * FROM og_lake.time_travel('biz.t_evo'::regclass, %s) "
                "AS h(id int4, full_name text, score int4, amt numeric(10,3))",
                (s_e5,), '42804',
            )
            assert_raises(
                c,
                "SELECT * FROM og_lake.time_travel('biz.t_evo'::regclass, %s) "
                "AS h(id int4, full_name text, score int4, amt numeric)",
                (s_e5,), '42804',
            )
            # Wrong vector dim (4 vs 3) & wrong collation ("C" vs default) on empty s_rich_create
            assert_raises(
                c,
                """
                SELECT id FROM og_lake.time_travel('biz.t_rich'::regclass, %s) AS h(
                    id int4, b bool, i2 int2, i8 int8, f4 float4, f8 float8,
                    txt text, raw bytea, dt date, ts timestamp, tstz timestamptz,
                    u uuid, js json, num numeric(12,2), vec vector(4)
                )
                """,
                (s_rich_create,), '42804',
            )
            assert_raises(
                c,
                """
                SELECT id FROM og_lake.time_travel('biz.t_rich'::regclass, %s) AS h(
                    id int4, b bool, i2 int2, i8 int8, f4 float4, f8 float8,
                    txt text COLLATE "C", raw bytea, dt date, ts timestamp, tstz timestamptz,
                    u uuid, js json, num numeric(12,2), vec vector(3)
                )
                """,
                (s_rich_create,), '42804',
            )
            assert_raises(c, "SELECT og_lake.time_travel('biz.t_evo'::regclass, %s)", (s_e2,), '0A000')
            record('multi_snapshot_self_join_lateral_prepared_and_strict_descriptor_validation')

            # 5. Own-transaction journal & uncommitted DDL isolation.
            ctx = connect(db57, autocommit=False)
            try:
                exec_sql(ctx, "INSERT INTO biz.t_evo (id, score, amt) VALUES (99, 999, 1.11)")
                exec_sql(ctx, "DELETE FROM biz.t_evo WHERE id = 1")
                exec_sql(ctx, "UPDATE biz.t_evo SET score = -1 WHERE id = 2")
                live_ids = [r[0] for r in fetchall(ctx, 'SELECT id FROM biz.t_evo ORDER BY id')]
                assert live_ids == [2, 3, 4, 99], live_ids
                hist_s10 = fetchall(
                    ctx,
                    "SELECT * FROM og_lake.time_travel('biz.t_evo'::regclass, %s) "
                    "AS h(id int4, score int4, amt numeric(10,2)) ORDER BY id",
                    (s_e10,),
                )
                assert hist_s10 == [
                    (1, 42, None),
                    (2, 42, None),
                    (3, 99, Decimal('12.34')),
                    (4, 100, Decimal('56.78')),
                ], hist_s10
                # Confirm current transaction's private journal was NOT mutated by time_travel
                assert [r[0] for r in fetchall(ctx, 'SELECT id FROM biz.t_evo ORDER BY id')] == [2, 3, 4, 99]
                ctx.rollback()

                # Uncommitted DDL snapshot exclusion (64-bit own-XID)
                head_before_ddl = current_snap(c, 'srv_main')
                exec_sql(ctx, "ALTER FOREIGN TABLE biz.t_evo ADD COLUMN uncommitted_c int4")
                exec_sql(ctx, 'SAVEPOINT sp_tt_057')
                assert_raises(
                    ctx,
                    "SELECT * FROM og_lake.time_travel('biz.t_evo'::regclass, %s) "
                    "AS h(id int4, score int4, amt numeric(10,2), uncommitted_c int4)",
                    (head_before_ddl + 1,), '22023',
                )
                exec_sql(ctx, 'ROLLBACK TO SAVEPOINT sp_tt_057')
                hist_now = fetchall(
                    ctx,
                    "SELECT * FROM og_lake.time_travel('biz.t_evo'::regclass, clock_timestamp()) "
                    "AS h(id int4, score int4, amt numeric(10,2)) ORDER BY id",
                )
                assert [r[0] for r in hist_now] == [1, 2, 3, 4], hist_now
                ctx.rollback()

                # Uncommitted new foreign table rejected with 55000
                exec_sql(ctx, """
                    CREATE FOREIGN TABLE biz.t_local_new (id int4)
                    SERVER srv_main OPTIONS (binding_mode 'create', lake_schema 'main')
                """)
                assert_raises(
                    ctx,
                    "SELECT * FROM og_lake.time_travel('biz.t_local_new'::regclass, %s) AS h(id int4)",
                    (head_before_ddl,), '55000',
                )
                ctx.rollback()
                record('own_transaction_journal_and_uncommitted_ddl_isolation')
            finally:
                ctx.close()

            # 6. Negative/boundary checks, expire_snapshots interaction, isolation level, and ordinary-user ACLs.
            assert_raises(c, "SELECT * FROM og_lake.time_travel(NULL::regclass, 1) AS h(id int4)", (), '22004')
            assert_raises(c, "SELECT * FROM og_lake.time_travel('biz.t_evo'::regclass, NULL::bigint) AS h(id int4)", (), '22004')
            assert_raises(c, "SELECT * FROM og_lake.time_travel('biz.t_evo'::regclass, NULL::timestamptz) AS h(id int4)", (), '22004')
            assert_raises(c, "SELECT * FROM og_lake.time_travel('biz.t_evo'::regclass, -1) AS h(id int4)", (), '22023')
            assert_raises(c, "SELECT * FROM og_lake.time_travel('biz.t_evo'::regclass, 9999999) AS h(id int4)", (), '22023')
            assert_raises(c, "SELECT * FROM og_lake.time_travel('biz.t_evo'::regclass, 0) AS h(id int4, name text)", (), '22023')
            assert_raises(
                c,
                "SELECT * FROM og_lake.time_travel('biz.t_evo'::regclass, TIMESTAMPTZ '2000-01-01 00:00:00+00') AS h(id int4, name text)",
                (), '22023',
            )
            assert_raises(
                c,
                "SELECT * FROM og_lake.time_travel('biz.t_evo'::regclass, now() + interval '1 hour') AS h(id int4, score int4, amt numeric(10,2))",
                (), '22023',
            )
            assert_raises(
                c,
                "SELECT * FROM og_lake.time_travel('biz.t_evo'::regclass, 'infinity'::timestamptz) AS h(id int4)",
                (), '22023',
            )
            assert_raises(
                c,
                "SELECT * FROM og_lake.time_travel('biz.t_evo'::regclass, '-infinity'::timestamptz) AS h(id int4)",
                (), '22023',
            )
            exec_sql(c, 'CREATE TABLE biz.t_heap (id int4)')
            assert_raises(
                c,
                "SELECT * FROM og_lake.time_travel('biz.t_heap'::regclass, 1) AS h(id int4)",
                (), '42809',
            )
            assert_raises(
                c,
                "SELECT * FROM og_lake.time_travel(4294967290::oid::regclass, 1) AS h(id int4)",
                (), '42P01',
            )

            crr = connect(db57, autocommit=False)
            try:
                exec_sql(crr, 'SET TRANSACTION ISOLATION LEVEL REPEATABLE READ')
                assert_raises(
                    crr,
                    "SELECT * FROM og_lake.time_travel('biz.t_evo'::regclass, %s) AS h(id int4, score int4, amt numeric(10,2))",
                    (s_e10,), '0A000',
                )
                crr.rollback()
            finally:
                crr.close()

            # Expire snapshot s_e2 and verify s_e2 is rejected while s_e3 remains readable!
            expired = fetchall(
                c,
                "SELECT snapshot_id FROM og_lake.expire_snapshots('srv_main'::name, ARRAY[%s]::bigint[], NULL, false)",
                (s_e2,),
            )
            assert expired == [(s_e2,)], expired
            assert_raises(
                c,
                "SELECT * FROM og_lake.time_travel('biz.t_evo'::regclass, %s) AS h(id int4, name text)",
                (s_e2,), '22023',
            )
            assert fetchall(
                c,
                "SELECT * FROM og_lake.time_travel('biz.t_evo'::regclass, %s) AS h(id int4, fn text) ORDER BY id",
                (s_e3,),
            ) == [(1, 'alice'), (2, 'bob')]
            record('boundary_errors_isolation_gate_and_expire_snapshots_rejection')

            # Ordinary user table & column ACL gates (including dropped-column protection!)
            exec_sql(c, """
                CREATE FOREIGN TABLE biz.t_colacl (
                    id int4,
                    pub text
                ) SERVER srv_main OPTIONS (binding_mode 'create', lake_schema 'main')
            """)
            exec_sql(c, "INSERT INTO biz.t_colacl VALUES (10, 'open_v1')")
            s_acl_a = current_snap(c, 'srv_main')
            exec_sql(c, "ALTER FOREIGN TABLE biz.t_colacl ADD COLUMN secret text DEFAULT 'top_secret'")
            s_acl_b = current_snap(c, 'srv_main')

            t_colacl_oid = scalar(c, "SELECT 'biz.t_colacl'::regclass::oid")
            exec_sql(c, sql.SQL('GRANT CONNECT ON DATABASE {} TO {}, {}, {}').format(
                sql.Identifier(db57), sql.Identifier(u_none), sql.Identifier(u_col), sql.Identifier(u_full)))
            exec_sql(c, sql.SQL('GRANT USAGE ON SCHEMA biz TO {}, {}').format(
                sql.Identifier(u_col), sql.Identifier(u_full)))
            exec_sql(c, sql.SQL('GRANT SELECT (id, pub) ON biz.t_colacl TO {}').format(sql.Identifier(u_col)))
            exec_sql(c, sql.SQL('GRANT SELECT ON biz.t_colacl, biz.t_evo TO {}').format(sql.Identifier(u_full)))

            cn = connect(db57, user=u_none, password=user_pwd, autocommit=True)
            cc = connect(db57, user=u_col, password=user_pwd, autocommit=True)
            cf = connect(db57, user=u_full, password=user_pwd, autocommit=True)
            try:
                # u_none has no schema USAGE -> rejected even with numeric OID::regclass!
                assert_raises(
                    cn,
                    'SELECT * FROM og_lake.time_travel(%s::oid::regclass, %s) AS h(id int4, pub text)',
                    (t_colacl_oid, s_acl_a), '42501',
                )
                # u_col CAN read s_acl_a (only id, pub existed, and both are live & granted)
                assert fetchall(
                    cc,
                    "SELECT * FROM og_lake.time_travel('biz.t_colacl'::regclass, %s) AS h(id int4, pub text)",
                    (s_acl_a,),
                ) == [(10, 'open_v1')]
                # u_col CANNOT read s_acl_b (secret is not granted!)
                assert_raises(
                    cc,
                    "SELECT * FROM og_lake.time_travel('biz.t_colacl'::regclass, %s) AS h(id int4, pub text, secret text)",
                    (s_acl_b,), '42501',
                )
                # Now admin drops `pub`: u_col (column-only ACL) can NO LONGER read s_acl_a, whereas u_full (table SELECT) CAN!
                exec_sql(c, 'ALTER FOREIGN TABLE biz.t_colacl DROP COLUMN pub')
                assert_raises(
                    cc,
                    "SELECT * FROM og_lake.time_travel('biz.t_colacl'::regclass, %s) AS h(id int4, pub text)",
                    (s_acl_a,), '42501',
                )
                assert fetchall(
                    cf,
                    "SELECT * FROM og_lake.time_travel('biz.t_colacl'::regclass, %s) AS h(id int4, pub text)",
                    (s_acl_a,),
                ) == [(10, 'open_v1')]
                assert fetchall(
                    cf,
                    "SELECT * FROM og_lake.time_travel('biz.t_evo'::regclass, %s) AS h(id int4, fn text) ORDER BY id",
                    (s_e3,),
                ) == [(1, 'alice'), (2, 'bob')]
                record('ordinary_user_schema_table_and_column_acl_with_dropped_column_fence')
            finally:
                cn.close()
                cc.close()
                cf.close()

            # 7. Independent pinned DuckLake (0aaa8e10d4...) offline interread parity check across AT (VERSION => sid).
            root_ref = create_empty_ducklake_root(workdir, 'cat_ref')
            exec_sql(c, 'SELECT og_lake.create_catalog(%s::name, %s::name, %s::text, NULL)',
                     ('srv_ref', 'meta_ref', root_ref))
            exec_sql(c, """
                CREATE FOREIGN TABLE biz.t_ref (
                    id int4,
                    label text,
                    active bool
                ) SERVER srv_ref OPTIONS (binding_mode 'create', lake_schema 'main')
            """)
            r_s1 = current_snap(c, 'srv_ref')
            exec_sql(c, "INSERT INTO biz.t_ref VALUES (1, 'v1_a', true), (2, 'v1_b', false)")
            r_s2 = current_snap(c, 'srv_ref')
            exec_sql(c, "ALTER FOREIGN TABLE biz.t_ref RENAME COLUMN label TO title")
            r_s3 = current_snap(c, 'srv_ref')
            exec_sql(c, "ALTER FOREIGN TABLE biz.t_ref ADD COLUMN weight int8 DEFAULT 77")
            r_s4 = current_snap(c, 'srv_ref')
            exec_sql(c, "INSERT INTO biz.t_ref VALUES (3, 'v2_c', true, 300)")
            r_s5 = current_snap(c, 'srv_ref')
            exec_sql(c, "SELECT og_lake.flush('biz.t_ref'::regclass)")
            r_s6 = current_snap(c, 'srv_ref')
            exec_sql(c, "UPDATE biz.t_ref SET weight = 888 WHERE id = 1")
            exec_sql(c, "DELETE FROM biz.t_ref WHERE id = 2")
            r_s7 = current_snap(c, 'srv_ref')
            exec_sql(c, "ALTER FOREIGN TABLE biz.t_ref DROP COLUMN active")
            r_s8 = current_snap(c, 'srv_ref')

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

            for sid, as_clause, proj in [
                (r_s1, 'AS h(id int4, label text, active bool)', 'id, label, active'),
                (r_s2, 'AS h(id int4, label text, active bool)', 'id, label, active'),
                (r_s3, 'AS h(id int4, title text, active bool)', 'id, title, active'),
                (r_s4, 'AS h(id int4, title text, active bool, weight int8)', 'id, title, active, weight'),
                (r_s5, 'AS h(id int4, title text, active bool, weight int8)', 'id, title, active, weight'),
                (r_s6, 'AS h(id int4, title text, active bool, weight int8)', 'id, title, active, weight'),
                (r_s7, 'AS h(id int4, title text, active bool, weight int8)', 'id, title, active, weight'),
                (r_s8, 'AS h(id int4, title text, weight int8)', 'id, title, weight'),
            ]:
                og_rows = fetchall(
                    c,
                    f"SELECT {proj} FROM og_lake.time_travel('biz.t_ref'::regclass, %s) {as_clause} ORDER BY id",
                    (sid,),
                )
                dk_rows = [
                    tuple(r.values())
                    for r in ref.run(
                        ref.attach(transport, True)
                        + f'SELECT {proj} FROM lake.main.t_ref AT (VERSION => {sid}) ORDER BY id',
                        f'ref-t_ref-{sid}',
                        result=True,
                    )
                ]
                assert og_rows == dk_rows, (sid, og_rows, dk_rows)
            record('independent_pinned_ducklake_reference_interread_parity')
        finally:
            c.close()

    finally:
        for db in (db56, db57):
            exec_sql(admin, 'SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = %s AND pid <> pg_backend_pid()', (db,))
            exec_sql(admin, sql.SQL('DROP DATABASE IF EXISTS {}').format(sql.Identifier(db)))
        for role in (u_none, u_col, u_full, owner_role):
            exec_sql(admin, sql.SQL('DROP ROLE IF EXISTS {}').format(sql.Identifier(role)))
        admin.close()
        shutil.rmtree(workdir, ignore_errors=True)

    result = dict(
        suite='native_057_time_travel_22',
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
