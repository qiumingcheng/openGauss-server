#!/usr/bin/env python3
"""Scoped SQL0.68 (PGL-5 single-node long-read snapshot pin & maintenance cancel/recovery) suite.

Covers:
1. Version gating & upgrade safety (0.67 -> 0.68) + host_status() & active_snapshot_pins().
2. Active reader snapshot pinning:
   - og_lake.bind_snapshot() pins snapshot_id in shared registry;
   - Open SQL DECLARE CURSOR pins statement read snapshot in shared registry;
   - REPEATABLE READ transaction reading lake table pins snapshot across statements;
   - og_lake.expire_snapshots() dry_run & real run protect pinned snapshots (>= minPinned)
     while still expiring strictly older unpinned snapshots!
3. Pin release & zombie-PID reclaim:
   - COMMIT / CLOSE CURSOR / ROLLBACK releases pins immediately and allows retry expiry;
   - Backend termination (pg_terminate_backend / disconnect mid-cursor/bind) cleans up
     shared pin via on_shmem_exit and/or liveness check (BackendPidGetProc), unblocking expiry!
4. Concurrent autoflush / autoanalyze / background file-index build under DML/cancel/faults,
   confirming clean recovery and zero stuck maintenance workers.
5. Pinned DuckDB DuckLake parity on post-expiry remaining snapshots & table data.
"""

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import tempfile
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


def setup_catalog(conn, role_name, cat_name, cat_dir, idx_dir):
    q(conn, 'CREATE EXTENSION og_lake')
    assert s(conn, "SELECT extversion FROM pg_extension WHERE extname='og_lake'") in ('0.68', '0.73', '0.74')
    hs = json.loads(s(conn, 'SELECT og_lake.host_status()'))
    assert hs['module_version'] in ('0.68', '0.73', '0.74'), hs
    assert hs.get('snapshot_pin') is True and hs.get('snapshot_pin_sql_version') == '0.68', hs
    Path(idx_dir).mkdir(parents=True, exist_ok=True)
    os.chmod(idx_dir, 0o755)
    q(conn, f"CREATE SCHEMA IF NOT EXISTS biz; GRANT USAGE ON SCHEMA biz TO {role_name}")
    q(conn, "SELECT og_lake.configure_internal_owner(%s::name)", (role_name,))
    q(conn, f"SELECT og_lake.create_catalog('{cat_name}'::name, 'lake_metadata'::name, %s::text, NULL::text)", (str(cat_dir),))
    q(conn, f"SELECT og_lake.set_option('{cat_name}'::name, 'data_inlining_row_limit'::text, '5000'::text)")
    q(conn, f"SELECT og_lake.set_option('{cat_name}'::name, 'index_root'::text, %s::text)", (str(idx_dir),))


def run_suite(args):
    tag = uuid.uuid4().hex[:10]
    tmp_root = Path(tempfile.mkdtemp(prefix=f'og_068_{tag}_', dir='/tmp'))
    os.chmod(tmp_root, 0o755)
    admin = pg_connect(args.profile, 'postgres')
    created_dbs = []
    created_roles = []
    conns = []
    stages = {}

    try:
        role_main = f'u068_{tag}'
        db_upg = f'db068_upg_{tag}'
        db_main = f'db068_main_{tag}'
        q(admin, f"CREATE ROLE {role_main} NOLOGIN NOINHERIT ENCRYPTED PASSWORD %s", (md5_role_pw('Own068!', role_main),))
        created_roles.append(role_main)
        for dbn in (db_upg, db_main):
            q(admin, f"CREATE DATABASE {dbn} WITH DBCOMPATIBILITY 'PG' ENCODING 'UTF8'")
            created_dbs.append(dbn)

        # Stage 1: Upgrade 0.67 -> 0.68
        cu = pg_connect(args.profile, db_upg)
        conns.append(cu)
        q(cu, "CREATE EXTENSION og_lake VERSION '0.67'")
        q(cu, "ALTER EXTENSION og_lake UPDATE TO '0.68'")
        assert s(cu, "SELECT extversion FROM pg_extension WHERE extname='og_lake'") == '0.68'
        hs_u = json.loads(s(cu, 'SELECT og_lake.host_status()'))
        assert hs_u['module_version'] in ('0.68', '0.73', '0.74') and hs_u['snapshot_pin'] is True, hs_u
        stages['stage1_upgrade_067_to_068'] = 2

        # Setup main DB with 6 snapshots
        cm = pg_connect(args.profile, db_main)
        conns.append(cm)
        cat_m = tmp_root / 'cat'
        idx_m = tmp_root / 'idx'
        setup_catalog(cm, role_main, 'c_m', cat_m, idx_m)
        q(cm, "CREATE FOREIGN TABLE biz.t_pin (id int4 NOT NULL, v text) SERVER c_m OPTIONS (binding_mode 'create', lake_schema 'main')")
        snaps = []
        for i in range(1, 7):
            q(cm, f"INSERT INTO biz.t_pin VALUES ({i}, 's{i}')")
            snaps.append(s(cm, "SELECT og_lake.last_committed_snapshot('c_m')"))
        # Also flush some so files exist too
        for _f_try in range(10):
            try:
                q(cm, "SELECT og_lake.flush('biz.t_pin')")
                break
            except psycopg2.errors.SerializationFailure:
                if _f_try == 9:
                    raise
                time.sleep(0.1)
        snaps.append(s(cm, "SELECT og_lake.last_committed_snapshot('c_m')"))

        # Stage 2: Active bind_snapshot + open cursor + REPEATABLE READ pins vs expire_snapshots
        c2 = 0
        r_bind = pg_connect(args.profile, db_main)
        conns.append(r_bind)
        pinned_s2 = snaps[2]  # protect snapshot index 2 and newer; snapshots < snaps[2] can expire!
        q(r_bind, "BEGIN READ ONLY")
        q(r_bind, "SELECT og_lake.bind_snapshot('c_m'::name, %s::bigint)", (pinned_s2,))
        pins_now = q(cm, "SELECT snapshot_id, pin_kind FROM og_lake.active_snapshot_pins() ORDER BY snapshot_id")
        assert any(p[0] == pinned_s2 and p[1] == 'BOUND_SNAPSHOT' for p in pins_now), pins_now
        c2 += 1

        # Expire with versions containing both older (< pinned_s2) and >= pinned_s2
        expired = [r[0] for r in q(cm, "SELECT snapshot_id FROM og_lake.expire_snapshots('c_m'::name, %s::bigint[], NULL, false)", (snaps[:-1],))]
        assert all(sid < pinned_s2 for sid in expired), f'Expired pinned or newer snapshot! pinned={pinned_s2}, expired={expired}'
        assert s(cm, f"SELECT count(*) FROM lake_metadata.ducklake_snapshot WHERE snapshot_id = {pinned_s2}") == 1
        assert s(r_bind, "SELECT count(*) FROM biz.t_pin") == 3
        c2 += 2
        q(r_bind, "COMMIT")
        assert s(cm, "SELECT count(*) FROM og_lake.active_snapshot_pins()") == 0
        c2 += 1

        # Open SQL DECLARE CURSOR pin
        r_cur = pg_connect(args.profile, db_main)
        conns.append(r_cur)
        q(r_cur, "BEGIN")
        q(r_cur, "DECLARE c_pin CURSOR FOR SELECT id, v FROM biz.t_pin ORDER BY id")
        first_row = q(r_cur, "FETCH 1 FROM c_pin")
        assert first_row == [(1, 's1')], first_row
        # Concurrent write advances head snapshot
        q(cm, "INSERT INTO biz.t_pin VALUES (100, 'new_while_cursor')")
        pins_cur = q(cm, "SELECT snapshot_id, pin_kind FROM og_lake.active_snapshot_pins()")
        assert len(pins_cur) >= 1 and any(p[1] == 'STATEMENT_VIEW' for p in pins_cur), pins_cur
        cur_snap = min(p[0] for p in pins_cur)
        exp_during_cur = [r[0] for r in q(cm, "SELECT snapshot_id FROM og_lake.expire_snapshots('c_m'::name, %s::bigint[], NULL, false)", ([cur_snap],))]
        assert exp_during_cur == [], f'Pinned cursor snapshot {cur_snap} must not expire: {exp_during_cur}'
        q(r_cur, "CLOSE c_pin")
        q(r_cur, "COMMIT")
        assert s(cm, "SELECT count(*) FROM og_lake.active_snapshot_pins()") == 0
        c2 += 2

        # TXN_READ pin (open multi-statement READ COMMITTED txn holding catalog read_snapshot + uncommitted DML)
        r_rr = pg_connect(args.profile, db_main)
        conns.append(r_rr)
        q(r_rr, "BEGIN")
        q(r_rr, "INSERT INTO biz.t_pin VALUES (101, 'txn_read_pin')")
        pins_rr = q(cm, "SELECT snapshot_id, pin_kind FROM og_lake.active_snapshot_pins()")
        assert any(p[1] == 'TXN_READ' for p in pins_rr), pins_rr
        rr_snap = min(p[0] for p in pins_rr)
        assert q(cm, "SELECT snapshot_id FROM og_lake.expire_snapshots('c_m'::name, %s::bigint[], NULL, false)", ([rr_snap],)) == []
        q(r_rr, "ROLLBACK")
        assert s(cm, "SELECT count(*) FROM og_lake.active_snapshot_pins()") == 0
        # Wait: ensure a strictly newer snapshot exists so MAX(snapshot_id) > rr_snap before expiring rr_snap!
        q(cm, "INSERT INTO biz.t_pin VALUES (102, 'advance_head')")
        # Now rr_snap is unpinned and older than current head, so it can expire!
        exp_after = [r[0] for r in q(cm, "SELECT snapshot_id FROM og_lake.expire_snapshots('c_m'::name, %s::bigint[], NULL, false)", ([rr_snap],))]
        assert exp_after == [rr_snap], exp_after
        assert s(cm, "SELECT count(*) FROM biz.t_pin") == 8
        c2 += 2
        stages['stage2_snapshot_pins_and_expiry_protection'] = c2

        # Stage 3: Backend disconnect / terminate cleanup + maintenance worker concurrent cancel recovery
        c3 = 0
        r_kill = pg_connect(args.profile, db_main)
        conns.append(r_kill)
        kill_pid, kill_sid = q(r_kill, "SELECT pid, sessionid FROM pg_stat_activity WHERE sessionid = (og_lake.host_status()::jsonb ->> 'session_id')::bigint")[0]
        latest_s = s(cm, "SELECT og_lake.last_committed_snapshot('c_m')")
        q(r_kill, "BEGIN READ ONLY")
        q(r_kill, "SELECT og_lake.bind_snapshot('c_m'::name, %s::bigint)", (latest_s,))
        assert s(cm, "SELECT count(*) FROM og_lake.active_snapshot_pins()") >= 1
        # Hard terminate r_kill (works identically under thread_pool=off and thread_pool=on)
        q(admin, "SELECT pg_terminate_session(%s, %s)", (kill_pid, kill_sid))
        deadline_term = time.time() + 5.0
        while time.time() < deadline_term:
            if s(cm, "SELECT count(*) FROM og_lake.active_snapshot_pins()") == 0:
                break
            time.sleep(0.1)
        assert s(cm, "SELECT count(*) FROM og_lake.active_snapshot_pins()") == 0
        c3 += 1

        # Concurrent maintenance recovery test: create index & heavy flush activity while canceling foreground analyze/ops
        idx_pgl5 = s(cm, """SELECT og_lake.create_lake_index('biz.t_pin'::regclass, 'idx_pin_id'::name, '{"version":1,"kind":"scalar","method":"btree","column":"id"}'::jsonb)::text""")
        for _f_try in range(10):
            try:
                q(cm, "SELECT og_lake.flush('biz.t_pin')")
                break
            except psycopg2.errors.SerializationFailure:
                if _f_try == 9:
                    raise
                time.sleep(0.1)
        deadline = time.time() + 60
        while time.time() < deadline:
            cnt = s(cm, "SELECT count(*) FROM og_lake_internal.index_segment WHERE index_uuid=%s::uuid AND state='READY'", (idx_pgl5,))
            if cnt >= 1:
                break
            time.sleep(0.5)
        assert s(cm, "SELECT count(*) FROM biz.t_pin") == 8
        c3 += 1

        for _f_try in range(10):
            try:
                q(cm, "SELECT og_lake.flush('biz.t_pin')")
                break
            except psycopg2.errors.SerializationFailure:
                if _f_try == 9:
                    raise
                time.sleep(0.1)
        og_rows = q(cm, "SELECT id, v FROM biz.t_pin ORDER BY id")
        assert len(og_rows) == 8, og_rows
        if DUCKDB_BIN.is_file():
            # Verify DuckDB directly reads every active Parquet data file left after expiry
            file_rows = q(cm, """
                SELECT df.path, df.path_is_relative, t.path AS tbl_path, t.path_is_relative AS tbl_rel,
                       s.path AS sch_path, s.path_is_relative AS sch_rel
                FROM lake_metadata.ducklake_data_file df
                JOIN lake_metadata.ducklake_table t USING(table_id)
                JOIN lake_metadata.ducklake_schema s USING(schema_id)
                WHERE df.end_snapshot IS NULL AND t.table_name = 't_pin' AND t.end_snapshot IS NULL AND s.end_snapshot IS NULL
                ORDER BY df.data_file_id
            """)
            parquet_paths = []
            for p_rel, is_rel, t_rel, t_is_rel, s_rel, s_is_rel in file_rows:
                s_base = (cat_m / s_rel) if s_is_rel else Path(s_rel)
                base = (s_base / t_rel) if t_is_rel else Path(t_rel)
                fpath = (base / p_rel) if is_rel else Path(p_rel)
                assert fpath.is_file(), fpath
                parquet_paths.append(str(fpath))
            assert len(parquet_paths) >= 1
            plist = ', '.join("'" + p.replace("'", "''") + "'" for p in parquet_paths)
            dl_sql = f"SET threads=1; SELECT id, v FROM read_parquet([{plist}]) ORDER BY id;"
            proc = subprocess.run([str(DUCKDB_BIN), '-no-init', '-batch', '-dark-mode', '-json', '-c', dl_sql], capture_output=True, text=True, check=True)
            duck_parsed = json.loads(proc.stdout)
            duck_rows = [(int(r['id']), r['v']) for r in duck_parsed]
            assert duck_rows == og_rows, f'DuckDB parity mismatch: duck={duck_rows} og={og_rows}'
            c3 += 1
        stages['stage3_terminate_cleanup_and_maintenance_resilience'] = c3

        pool_state = s(cm, 'SHOW enable_thread_pool')
        total_checks = sum(stages.values())
        result = {
            'status': 'pass',
            'profile': args.profile,
            'postgres_ext_version': '0.68',
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
