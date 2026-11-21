#!/usr/bin/env python3
"""Comprehensive verification suite for og_lake 0.56 last_committed_snapshot + LakeValidationScope."""
import argparse
from datetime import datetime, timezone
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
from native_runtime import INSTALL, PORT, PROFILE, RUNTIME, SOURCE


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
        application_name='og_lake_test_056',
    )
    if password is not None:
        kwargs['password'] = password
    conn = psycopg2.connect(**kwargs)
    conn.autocommit = autocommit
    with conn.cursor() as cur:
        cur.execute('SET statement_timeout = 30000')
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


def last_committed(conn, server):
    rows = fetchall(conn, 'SELECT id FROM og_lake.last_committed_snapshot(%s::name)', (server,))
    assert len(rows) == 1 and len(rows[0]) == 1, f'expected exactly 1 row, got {rows!r}'
    return rows[0][0]


def current_snap(conn, server):
    return scalar(conn, 'SELECT id FROM og_lake.current_snapshot(%s::name)', (server,))


def assert_raises(conn, stmt, params=(), expected_sqlstate=None):
    try:
        exec_sql(conn, stmt, params)
    except psycopg2.Error as exc:
        if expected_sqlstate is not None:
            assert exc.pgcode == expected_sqlstate, f'expected SQLSTATE {expected_sqlstate}, got {exc.pgcode}: {exc}'
        return exc
    raise AssertionError(f'expected SQL error for: {stmt}')


def create_empty_ducklake_root(workdir: Path, name: str) -> str:
    workdir.mkdir(parents=True, exist_ok=True)
    data_dir = workdir / (name + '_data')
    assert not data_dir.exists(), f'data_dir already exists: {data_dir}'
    return str(data_dir)


def run_suite(output_json: Path | None = None):
    started = datetime.now(timezone.utc).isoformat()
    run_id = uuid.uuid4().hex[:10]
    workdir = Path('/tmp') / f'og_lake_056_suite_{run_id}'
    workdir.mkdir(parents=True, exist_ok=True)
    db55 = f'og_lake_056_v55_{run_id}'
    db56 = f'og_lake_056_v56_{run_id}'
    owner_role = f'og_owner_{run_id}'
    unpriv_role = f'og_user_{run_id}'
    owner_pwd = 'Og1!' + secrets.token_hex(12)
    unpriv_pwd = 'Og1!' + secrets.token_hex(12)
    unpriv_md5 = 'md5' + hashlib.md5((unpriv_pwd + unpriv_role).encode('utf-8')).hexdigest()

    checks = []
    def record(name):
        checks.append(name)

    admin = connect('postgres')
    thread_pool_setting = scalar(admin, 'SHOW enable_thread_pool')
    postmaster_start = str(scalar(admin, 'SELECT pg_postmaster_start_time()'))
    try:
        exec_sql(admin, sql.SQL('CREATE ROLE {} NOLOGIN NOINHERIT PASSWORD {}').format(
            sql.Identifier(owner_role), sql.Literal(owner_pwd)))
        exec_sql(admin, sql.SQL('CREATE ROLE {} LOGIN ENCRYPTED PASSWORD {}').format(
            sql.Identifier(unpriv_role), sql.Literal(unpriv_md5)))
        exec_sql(admin, sql.SQL('CREATE DATABASE {}').format(sql.Identifier(db55)))
        exec_sql(admin, sql.SQL('CREATE DATABASE {}').format(sql.Identifier(db56)))

        # 1. Verify 0.55 backward compatibility + pre-0.56 gate, then 0.55 -> 0.56 upgrade.
        c55 = connect(db55, autocommit=True)
        try:
            exec_sql(c55, "CREATE EXTENSION og_lake VERSION '0.55'")
            hs55 = json.loads(scalar(c55, 'SELECT og_lake.host_status()'))
            assert hs55['module_version'] in ('0.56', '0.73', '0.74'), hs55
            assert hs55['last_committed_snapshot'] is True, hs55
            assert hs55['last_committed_snapshot_sql_version'] == '0.56', hs55
            assert hs55['snapshot_metadata'] is True, hs55
            assert scalar(
                c55,
                "SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace "
                "WHERE n.nspname = 'og_lake' AND p.proname = 'last_committed_snapshot'",
            ) == 0
            exec_sql(c55, "ALTER EXTENSION og_lake UPDATE TO '0.56'")
            assert scalar(
                c55,
                "SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace "
                "WHERE n.nspname = 'og_lake' AND p.proname = 'last_committed_snapshot'",
            ) == 1
            record('sql055_pre_upgrade_gate_and_055_to_056_alter_extension')
        finally:
            c55.close()

        # 2. Fresh 0.56 installation + metadata / privilege checks.
        c1 = connect(db56, autocommit=True)
        c2 = connect(db56, autocommit=True)
        try:
            expected_tp = os.getenv('OG_LAKE_EXPECT_THREAD_POOL')
            if expected_tp:
                assert scalar(c1, 'SHOW enable_thread_pool') == expected_tp
            exec_sql(c1, 'CREATE EXTENSION og_lake')
            assert scalar(c1, "SELECT extversion FROM pg_extension WHERE extname = 'og_lake'") in ('0.56', '0.73', '0.74')
            pro = fetchall(
                c1,
                "SELECT provolatile, prosecdef, proretset, fencedmode, proconfig::text "
                "FROM pg_proc WHERE oid = 'og_lake.last_committed_snapshot(name)'::regprocedure",
            )
            assert pro in (
                [('s', False, True, False, '{search_path=pg_catalog,pg_temp}')],
                [('s', False, True, False, '{search_path="pg_catalog",pg_temp}')],
                [('s', False, True, False, '{"search_path=pg_catalog, pg_temp"}')],
            ), pro
            assert scalar(c1, "SELECT has_function_privilege(%s, 'og_lake.last_committed_snapshot(name)', 'EXECUTE')", (unpriv_role,)) is False
            cu = connect(db56, user=unpriv_role, password=unpriv_pwd, autocommit=True)
            try:
                assert_raises(cu, "SELECT * FROM og_lake.last_committed_snapshot('nonexistent')")
            finally:
                cu.close()
            record('fresh_056_pg_proc_signature_and_unprivileged_rejection')

            # Configure internal owner and create two independent catalogs cat1 & cat2.
            exec_sql(c1, 'SELECT og_lake.configure_internal_owner(%s::name)', (owner_role,))
            root1 = create_empty_ducklake_root(workdir, 'cat1')
            root2 = create_empty_ducklake_root(workdir, 'cat2')
            exec_sql(c1, 'SELECT og_lake.create_catalog(%s::name, %s::name, %s::text, NULL::text)', ('cat1', 'meta1', root1))
            exec_sql(c1, 'SELECT og_lake.create_catalog(%s::name, %s::name, %s::text, NULL::text)', ('cat2', 'meta2', root2))

            # Input guards
            assert_raises(c1, 'SELECT * FROM og_lake.last_committed_snapshot(NULL::name)', expected_sqlstate='22004')
            assert_raises(c1, "SELECT * FROM og_lake.last_committed_snapshot('no_such_server')")
            exec_sql(c1, 'CREATE SERVER dummy_srv_056 FOREIGN DATA WRAPPER log_fdw')
            assert_raises(c1, "SELECT * FROM og_lake.last_committed_snapshot('dummy_srv_056')", expected_sqlstate='22023')
            exec_sql(c1, 'DROP SERVER dummy_srv_056')
            record('input_guards_null_missing_and_non_lake_fdw')

            # Read-only txn allowed + seed0 returns single NULL row
            exec_sql(c1, 'BEGIN READ ONLY')
            assert last_committed(c1, 'cat1') is None
            assert last_committed(c1, 'cat2') is None
            exec_sql(c1, 'COMMIT')
            assert current_snap(c1, 'cat1') == 0
            record('readonly_txn_seed0_returns_single_null_row')

            # Publisher 1: CREATE FOREIGN TABLE inside explicit BEGIN -> NULL until COMMIT
            exec_sql(c1, 'BEGIN')
            exec_sql(c1, "CREATE FOREIGN TABLE t_rb (id bigint) SERVER cat1 OPTIONS (binding_mode 'create', lake_schema 'main')")
            assert last_committed(c1, 'cat1') is None
            exec_sql(c1, 'ROLLBACK')
            assert last_committed(c1, 'cat1') is None
            assert current_snap(c1, 'cat1') == 0

            exec_sql(c1, 'BEGIN')
            exec_sql(c1, "CREATE FOREIGN TABLE t1 (id bigint, val text) SERVER cat1 OPTIONS (binding_mode 'create', lake_schema 'main')")
            assert last_committed(c1, 'cat1') is None, 'uncommitted CREATE leaked into last_committed_snapshot'
            exec_sql(c1, 'COMMIT')
            s_create = current_snap(c1, 'cat1')
            assert s_create > 0 and last_committed(c1, 'cat1') == s_create
            assert last_committed(c1, 'cat2') is None, 'cat1 commit leaked to cat2 in same session'
            assert last_committed(c2, 'cat1') is None, 'other session saw session 1 commit'
            record('publisher_create_foreign_table_commit_and_session_catalog_isolation')

            # Combined CREATE + INSERT in single transaction commits once
            exec_sql(c1, 'BEGIN')
            exec_sql(c1, "CREATE FOREIGN TABLE t_combo (id bigint) SERVER cat1 OPTIONS (binding_mode 'create', lake_schema 'main')")
            exec_sql(c1, 'INSERT INTO t_combo VALUES (10), (20)')
            assert last_committed(c1, 'cat1') == s_create
            exec_sql(c1, 'COMMIT')
            s_combo = current_snap(c1, 'cat1')
            assert s_combo > s_create and last_committed(c1, 'cat1') == s_combo
            exec_sql(c1, 'DROP FOREIGN TABLE t_combo')
            s_create = current_snap(c1, 'cat1')
            assert s_create > s_combo and last_committed(c1, 'cat1') == s_create

            # Multi-catalog independence: commit on cat2 does not disturb cat1
            exec_sql(c1, "CREATE FOREIGN TABLE t_cat2 (id bigint) SERVER cat2 OPTIONS (binding_mode 'create', lake_schema 'main')")
            s_cat2 = current_snap(c1, 'cat2')
            assert s_cat2 > 0 and last_committed(c1, 'cat2') == s_cat2
            assert last_committed(c1, 'cat1') == s_create
            record('combined_create_insert_and_multicatalog_independence')

            # Publisher 2: INSERT / UPDATE / DELETE + ROLLBACK preservation + Session B isolation
            exec_sql(c1, 'BEGIN')
            exec_sql(c1, "INSERT INTO t1 VALUES (1, 'a'), (2, 'b')")
            assert last_committed(c1, 'cat1') == s_create
            exec_sql(c1, 'ROLLBACK')
            assert last_committed(c1, 'cat1') == s_create
            assert current_snap(c1, 'cat1') == s_create

            exec_sql(c1, 'BEGIN')
            exec_sql(c1, "INSERT INTO t1 VALUES (1, 'a'), (2, 'b')")
            exec_sql(c1, 'COMMIT')
            s_ins = current_snap(c1, 'cat1')
            assert s_ins > s_create and last_committed(c1, 'cat1') == s_ins

            exec_sql(c2, 'BEGIN')
            exec_sql(c2, "UPDATE t1 SET val = 'b2' WHERE id = 2")
            exec_sql(c2, 'COMMIT')
            s_upd = current_snap(c2, 'cat1')
            assert s_upd > s_ins
            assert last_committed(c2, 'cat1') == s_upd
            assert last_committed(c1, 'cat1') == s_ins
            record('publisher_inline_dml_rollback_and_two_session_isolation')

            # SAVEPOINT commit & abort semantics
            exec_sql(c1, 'BEGIN')
            exec_sql(c1, "INSERT INTO t1 VALUES (3, 'c')")
            exec_sql(c1, 'SAVEPOINT sp1')
            exec_sql(c1, "INSERT INTO t1 VALUES (4, 'd')")
            exec_sql(c1, 'ROLLBACK TO SAVEPOINT sp1')
            exec_sql(c1, 'SAVEPOINT sp2')
            exec_sql(c1, "DELETE FROM t1 WHERE id = 1")
            exec_sql(c1, 'RELEASE SAVEPOINT sp2')
            exec_sql(c1, 'COMMIT')
            s_sp = current_snap(c1, 'cat1')
            assert s_sp > s_upd and last_committed(c1, 'cat1') == s_sp
            assert fetchall(c1, 'SELECT id, val FROM t1 ORDER BY id') == [(2, 'b2'), (3, 'c')]

            exec_sql(c1, 'BEGIN')
            exec_sql(c1, 'SAVEPOINT sp_only')
            exec_sql(c1, "INSERT INTO t1 VALUES (5, 'e')")
            exec_sql(c1, 'ROLLBACK TO SAVEPOINT sp_only')
            exec_sql(c1, 'COMMIT')
            assert last_committed(c1, 'cat1') == s_sp
            record('savepoint_release_and_rollback_to_savepoint_semantics')

            # Publisher 5: flush + empty NOOP flush
            exec_sql(c1, "SELECT og_lake.flush('t1'::regclass)")
            s_flush1 = current_snap(c1, 'cat1')
            assert s_flush1 > s_sp and last_committed(c1, 'cat1') == s_flush1
            exec_sql(c1, "SELECT og_lake.flush('t1'::regclass)")
            assert current_snap(c1, 'cat1') == s_flush1
            assert last_committed(c1, 'cat1') == s_flush1

            # Publisher 6: second flush + rewrite_data_files (explicit active data_file_id[])
            exec_sql(c1, "INSERT INTO t1 VALUES (6, 'f')")
            exec_sql(c1, "SELECT og_lake.flush('t1'::regclass)")
            s_flush2 = current_snap(c1, 'cat1')
            assert last_committed(c1, 'cat1') == s_flush2
            file_ids = [r[0] for r in fetchall(c1, 'SELECT data_file_id FROM meta1.ducklake_data_file WHERE end_snapshot IS NULL ORDER BY data_file_id')]
            assert len(file_ids) >= 2, file_ids
            exec_sql(c1, "SELECT og_lake.rewrite_data_files('t1'::regclass, %s::bigint[])", (file_ids[:2],))
            s_rw = current_snap(c1, 'cat1')
            assert s_rw > s_flush2 and last_committed(c1, 'cat1') == s_rw
            record('publishers_flush_noop_flush_and_rewrite_data_files')

            # Immediate DDL Publishers 7, 8, 9, 10 + NOOP + failure/rollback-preservation + Partition DDL
            exec_sql(c1, 'DROP FOREIGN TABLE t1')
            exec_sql(c1, "CREATE FOREIGN TABLE t_ddl (id bigint, val text) SERVER cat1 OPTIONS (binding_mode 'create', lake_schema 'main')")
            exec_sql(c1, "INSERT INTO t_ddl VALUES (2, 'b2'), (3, 'c')")
            exec_sql(c1, "SELECT og_lake.flush('t_ddl'::regclass)")
            s_base_ddl = current_snap(c1, 'cat1')
            assert s_base_ddl > s_rw and last_committed(c1, 'cat1') == s_base_ddl

            exec_sql(c1, 'BEGIN')
            exec_sql(c1, 'ALTER FOREIGN TABLE t_ddl ADD COLUMN tag text')
            assert last_committed(c1, 'cat1') == s_base_ddl, 'uncommitted ADD COLUMN leaked before COMMIT'
            exec_sql(c1, 'ROLLBACK')
            assert last_committed(c1, 'cat1') == s_base_ddl, 'rolled-back ADD COLUMN mutated last_committed_snapshot'

            exec_sql(c1, 'BEGIN')
            exec_sql(c1, 'ALTER FOREIGN TABLE t_ddl ADD COLUMN note text')
            assert last_committed(c1, 'cat1') == s_base_ddl
            exec_sql(c1, 'COMMIT')
            s_add = current_snap(c1, 'cat1')
            assert s_add > s_base_ddl and last_committed(c1, 'cat1') == s_add

            exec_sql(c1, "ALTER FOREIGN TABLE t_ddl ALTER COLUMN note SET DEFAULT 'dx'")
            s_def = current_snap(c1, 'cat1')
            assert s_def > s_add and last_committed(c1, 'cat1') == s_def

            exec_sql(c1, "ALTER FOREIGN TABLE t_ddl ALTER COLUMN note SET DEFAULT 'dx'")
            assert last_committed(c1, 'cat1') == s_def

            assert_raises(c1, 'ALTER FOREIGN TABLE t_ddl ALTER COLUMN note SET NOT NULL')
            assert last_committed(c1, 'cat1') == s_def

            exec_sql(c1, "UPDATE t_ddl SET note = 'ok' WHERE note IS NULL")
            s_fill = current_snap(c1, 'cat1')
            assert s_fill > s_def and last_committed(c1, 'cat1') == s_fill

            exec_sql(c1, 'ALTER FOREIGN TABLE t_ddl ALTER COLUMN note SET NOT NULL')
            s_nn = current_snap(c1, 'cat1')
            assert s_nn > s_fill and last_committed(c1, 'cat1') == s_nn

            exec_sql(c1, 'ALTER FOREIGN TABLE t_ddl ALTER COLUMN note DROP NOT NULL')
            s_dnn = current_snap(c1, 'cat1')
            assert s_dnn > s_nn and last_committed(c1, 'cat1') == s_dnn

            exec_sql(c1, 'ALTER FOREIGN TABLE t_ddl ALTER COLUMN note DROP DEFAULT')
            s_ddef = current_snap(c1, 'cat1')
            assert s_ddef > s_dnn and last_committed(c1, 'cat1') == s_ddef

            exec_sql(c1, 'ALTER FOREIGN TABLE t_ddl DROP COLUMN note')
            s_dcol = current_snap(c1, 'cat1')
            assert s_dcol > s_ddef and last_committed(c1, 'cat1') == s_dcol

            exec_sql(c1, 'BEGIN ISOLATION LEVEL REPEATABLE READ')
            assert_raises(c1, "SELECT * FROM og_lake.last_committed_snapshot('cat1')", expected_sqlstate='0A000')
            exec_sql(c1, 'ROLLBACK')
            assert last_committed(c1, 'cat1') == s_dcol

            exec_sql(c1, "SELECT og_lake.set_partition('t_ddl'::regclass, ARRAY['id'])")
            s_part = current_snap(c1, 'cat1')
            assert s_part > s_dcol and last_committed(c1, 'cat1') == s_part
            exec_sql(c1, "SELECT og_lake.set_partition('t_ddl'::regclass, ARRAY['id'])")
            assert last_committed(c1, 'cat1') == s_part

            exec_sql(c1, "SELECT og_lake.reset_partition('t_ddl'::regclass)")
            s_rpart = current_snap(c1, 'cat1')
            assert s_rpart > s_part and last_committed(c1, 'cat1') == s_rpart
            record('publishers_schema_evolution_partition_ddl_and_repeatable_read')

            # Publisher 4 (RENAME column + RENAME table) + Publisher 3 (DROP TABLE)
            exec_sql(c1, 'ALTER FOREIGN TABLE t_ddl RENAME COLUMN val TO val_renamed')
            s_rcol = current_snap(c1, 'cat1')
            assert s_rcol > s_rpart and last_committed(c1, 'cat1') == s_rcol

            # Error inside subtransaction preserves parent txn slot + prior snapshot on ROLLBACK TO SAVEPOINT
            exec_sql(c1, 'BEGIN')
            exec_sql(c1, "INSERT INTO t_ddl (id, val_renamed) VALUES (88, 'sub_ok')")
            exec_sql(c1, 'SAVEPOINT sp_err')
            assert_raises(c1, 'ALTER FOREIGN TABLE t_ddl DROP COLUMN no_such_col_056')
            exec_sql(c1, 'ROLLBACK TO SAVEPOINT sp_err')
            exec_sql(c1, 'RELEASE SAVEPOINT sp_err')
            exec_sql(c1, 'COMMIT')
            s_suberr = current_snap(c1, 'cat1')
            assert s_suberr > s_rcol and last_committed(c1, 'cat1') == s_suberr

            exec_sql(c1, 'ALTER FOREIGN TABLE t_ddl RENAME TO t_ddl_renamed')
            s_ren = current_snap(c1, 'cat1')
            assert s_ren > s_suberr and last_committed(c1, 'cat1') == s_ren

            exec_sql(c1, 'DROP FOREIGN TABLE t_ddl_renamed')
            s_drop = current_snap(c1, 'cat1')
            assert s_drop > s_ren and last_committed(c1, 'cat1') == s_drop
            record('publishers_rename_drop_and_subtransaction_error_recovery')

            # 6-session interleaving check (exercises threadpool worker migration across idle sessions)
            exec_sql(c1, "CREATE FOREIGN TABLE t_pool (id bigint) SERVER cat1 OPTIONS (binding_mode 'create', lake_schema 'main')")
            s_pool_base = current_snap(c1, 'cat1')
            assert last_committed(c1, 'cat1') == s_pool_base
            pool_conns = [connect(db56, autocommit=True) for _ in range(6)]
            try:
                expected_pool = []
                for idx, pc in enumerate(pool_conns, 1):
                    assert last_committed(pc, 'cat1') is None
                    exec_sql(pc, 'BEGIN')
                    exec_sql(pc, 'INSERT INTO t_pool VALUES (%s)', (idx * 10,))
                    exec_sql(pc, 'SAVEPOINT sp_p')
                    exec_sql(pc, 'INSERT INTO t_pool VALUES (%s)', (idx * 10 + 1,))
                    if idx % 2 == 0:
                        exec_sql(pc, 'ROLLBACK TO SAVEPOINT sp_p')
                    else:
                        exec_sql(pc, 'RELEASE SAVEPOINT sp_p')
                    assert last_committed(pc, 'cat1') is None
                    exec_sql(pc, 'COMMIT')
                    expected_pool.append(current_snap(pc, 'cat1'))
                for idx, (pc, exp_snap) in enumerate(zip(pool_conns, expected_pool), 1):
                    assert last_committed(pc, 'cat1') == exp_snap, f'session {idx} lost committed snapshot'
                assert last_committed(c1, 'cat1') == s_pool_base
            finally:
                for pc in pool_conns:
                    pc.close()
            exec_sql(c1, 'DROP FOREIGN TABLE t_pool')
            s_drop = current_snap(c1, 'cat1')
            assert last_committed(c1, 'cat1') == s_drop
            record('six_session_interleaving_u_sess_isolation')

            # DROP SERVER rollback preserves; commit + recreate resets to NULL
            exec_sql(c1, 'BEGIN')
            exec_sql(c1, 'DROP SERVER cat1 CASCADE')
            exec_sql(c1, 'ROLLBACK')
            assert last_committed(c1, 'cat1') == s_drop

            exec_sql(c1, 'DROP SERVER cat1 CASCADE')
            assert last_committed(c1, 'cat2') == s_cat2, 'dropping cat1 disturbed cat2'
            root1b = create_empty_ducklake_root(workdir, 'cat1b')
            exec_sql(c1, 'SELECT og_lake.create_catalog(%s::name, %s::name, %s::text, NULL::text)', ('cat1', 'meta1b', root1b))
            assert last_committed(c1, 'cat1') is None

            # DROP EXTENSION rollback preserves; commit resets session state
            exec_sql(c1, "CREATE FOREIGN TABLE t2 (id bigint) SERVER cat1 OPTIONS (binding_mode 'create', lake_schema 'main')")
            assert last_committed(c1, 'cat1') is not None
            s_t2 = last_committed(c1, 'cat1')
            exec_sql(c1, 'BEGIN')
            exec_sql(c1, 'DROP EXTENSION og_lake CASCADE')
            exec_sql(c1, 'ROLLBACK')
            assert last_committed(c1, 'cat1') == s_t2
            assert last_committed(c1, 'cat2') == s_cat2

            exec_sql(c1, 'DROP FOREIGN TABLE t_cat2')
            s_cat2_drop = current_snap(c1, 'cat2')
            assert s_cat2_drop > s_cat2 and last_committed(c1, 'cat2') == s_cat2_drop
            exec_sql(c1, 'DROP SERVER cat2 CASCADE')
            exec_sql(c1, 'DROP EXTENSION og_lake CASCADE')
            exec_sql(c1, 'CREATE EXTENSION og_lake')
            exec_sql(c1, 'SELECT og_lake.configure_internal_owner(%s::name)', (owner_role,))
            root1c = create_empty_ducklake_root(workdir, 'cat1c')
            exec_sql(c1, 'SELECT og_lake.create_catalog(%s::name, %s::name, %s::text, NULL::text)', ('cat1', 'meta1c', root1c))
            assert last_committed(c1, 'cat1') is None
            record('drop_server_and_drop_extension_rollback_commit_reset')
        finally:
            c1.close()
            c2.close()
    finally:
        for d in (db55, db56):
            exec_sql(admin, f"DROP DATABASE IF EXISTS {d}")
        for r in (owner_role, unpriv_role):
            exec_sql(admin, f"DROP ROLE IF EXISTS {r}")
        admin.close()
        shutil.rmtree(workdir, ignore_errors=True)

    summary = {
        'schema': 'native-lake-056-last-committed-snapshot-verification-v1',
        'status': 'pass',
        'module_version': '0.56',
        'profile': PROFILE,
        'port': PORT,
        'enable_thread_pool': thread_pool_setting,
        'postmaster_start': postmaster_start,
        'binary_sha256': sha_file(Path(INSTALL) / 'bin/gaussdb'),
        'source': str(SOURCE),
        'check_groups': checks,
        'check_group_count': len(checks),
        'started_at_UTC': started,
        'completed_at_UTC': datetime.now(timezone.utc).isoformat(),
    }
    if output_json is not None:
        output_json.parent.mkdir(parents=True, exist_ok=True)
        output_json.write_text(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + '\n', encoding='utf-8')
    print(json.dumps(summary, ensure_ascii=False))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--output', type=Path)
    args = ap.parse_args()
    run_suite(args.output)


if __name__ == '__main__':
    main()
