#!/usr/bin/env python3
"""Scoped SQL0.69 (N9b local physical GC & PGL-5 cancel/recovery) verification suite."""

import argparse
import hashlib
import json
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


def setup_catalog(conn, role_name, cat_name, cat_dir, idx_dir):
    q(conn, 'CREATE EXTENSION og_lake')
    assert s(conn, "SELECT extversion FROM pg_extension WHERE extname='og_lake'") in ('0.69', '0.73', '0.74')
    hs = json.loads(s(conn, 'SELECT og_lake.host_status()'))
    assert hs['module_version'] in ('0.69', '0.73', '0.74'), hs
    assert hs.get('physical_cleanup') is True and hs.get('physical_cleanup_sql_version') == '0.69', hs
    Path(idx_dir).mkdir(parents=True, exist_ok=True)
    os.chmod(idx_dir, 0o755)
    q(conn, f"CREATE SCHEMA IF NOT EXISTS biz; GRANT USAGE ON SCHEMA biz TO {role_name}")
    q(conn, "SELECT og_lake.configure_internal_owner(%s::name)", (role_name,))
    q(conn, f"SELECT og_lake.create_catalog('{cat_name}'::name, 'lake_metadata'::name, %s::text, NULL::text)", (str(cat_dir),))
    q(conn, f"SELECT og_lake.set_option('{cat_name}'::name, 'data_inlining_row_limit'::text, '5000'::text)")
    q(conn, f"SELECT og_lake.set_option('{cat_name}'::name, 'index_root'::text, %s::text)", (str(idx_dir),))


def advance_xmin(conn):
    q(conn, "DROP TABLE IF EXISTS _xmin_tick")
    q(conn, "CREATE TABLE _xmin_tick(x int)")
    q(conn, "INSERT INTO _xmin_tick VALUES (1)")
    q(conn, "DROP TABLE _xmin_tick")


def wait_until_eligible(conn, dry_sql, status_idx=-1, timeout=15.0):
    deadline = time.monotonic() + timeout
    q(conn, "CREATE TABLE IF NOT EXISTS _xmin_pulse(x int)")
    while True:
        rows = q(conn, dry_sql)
        if rows and all(r[status_idx] == 'eligible' for r in rows):
            return rows
        if time.monotonic() >= deadline:
            raise AssertionError(f'OldestXmin watermark did not advance within {timeout}s: {rows!r}')
        q(conn, "INSERT INTO _xmin_pulse VALUES (1)")
        q(conn, "DELETE FROM _xmin_pulse")
        time.sleep(0.1)


def resolve_catalog_files(conn, cat_dir):
    data_rows = q(conn, """
        SELECT DISTINCT ON (df.data_file_id) df.path, df.path_is_relative,
               COALESCE(t.path, ''), COALESCE(t.path_is_relative, true),
               COALESCE(s.path, ''), COALESCE(s.path_is_relative, true)
        FROM lake_metadata.ducklake_data_file df
        JOIN lake_metadata.ducklake_table t USING(table_id)
        JOIN lake_metadata.ducklake_schema s USING(schema_id)
        WHERE df.end_snapshot IS NULL AND t.end_snapshot IS NULL AND s.end_snapshot IS NULL
        ORDER BY df.data_file_id, t.begin_snapshot DESC, s.begin_snapshot DESC
    """)
    del_rows = q(conn, """
        SELECT DISTINCT ON (df.delete_file_id) df.path, df.path_is_relative,
               COALESCE(t.path, ''), COALESCE(t.path_is_relative, true),
               COALESCE(s.path, ''), COALESCE(s.path_is_relative, true)
        FROM lake_metadata.ducklake_delete_file df
        JOIN lake_metadata.ducklake_table t USING(table_id)
        JOIN lake_metadata.ducklake_schema s USING(schema_id)
        WHERE df.end_snapshot IS NULL AND t.end_snapshot IS NULL AND s.end_snapshot IS NULL
        ORDER BY df.delete_file_id, t.begin_snapshot DESC, s.begin_snapshot DESC
    """)
    def resolve(rows):
        out = []
        for fp, fr, tp, tr, sp, sr in rows:
            base = cat_dir
            if sr and sp:
                base = base / sp.strip('/')
            if tr and tp:
                base = base / tp.strip('/')
            out.append(base / fp if fr else Path(fp))
        return out
    return resolve(data_rows), resolve(del_rows)


def run_suite(args):
    tag = uuid.uuid4().hex[:10]
    tmp_root = Path(tempfile.mkdtemp(prefix=f'og_069_{tag}_', dir='/tmp'))
    os.chmod(tmp_root, 0o755)
    admin = pg_connect(args.profile, 'postgres')
    created_dbs = []
    created_roles = []
    conns = []
    stages = {}

    try:
        role_main = f'u069_{tag}'
        db_upg = f'db069_upg_{tag}'
        db_main = f'db069_main_{tag}'
        q(admin, f"CREATE ROLE {role_main} NOLOGIN NOINHERIT ENCRYPTED PASSWORD %s", (md5_role_pw('Own069!', role_main),))
        created_roles.append(role_main)
        for dbn in (db_upg, db_main):
            q(admin, f"CREATE DATABASE {dbn} WITH DBCOMPATIBILITY 'PG' ENCODING 'UTF8'")
            created_dbs.append(dbn)

        # Stage 1: Upgrade 0.68 -> 0.69 + delete_older_than option validation
        c1 = 0
        cu = pg_connect(args.profile, db_upg)
        conns.append(cu)
        q(cu, "CREATE EXTENSION og_lake VERSION '0.68'")
        q(cu, "ALTER EXTENSION og_lake UPDATE TO '0.69'")
        assert s(cu, "SELECT extversion FROM pg_extension WHERE extname='og_lake'") == '0.69'
        hs_u = json.loads(s(cu, 'SELECT og_lake.host_status()'))
        assert hs_u['module_version'] in ('0.69', '0.73', '0.74') and hs_u['physical_cleanup'] is True and hs_u['n9b_local_cleanup'] is True, hs_u
        c1 += 2

        cm = pg_connect(args.profile, db_main)
        conns.append(cm)
        cat_m = tmp_root / 'cat'
        idx_m = tmp_root / 'idx'
        setup_catalog(cm, role_main, 'c_m', cat_m, idx_m)
        caps = json.loads(s(cm, "SELECT capabilities::text FROM og_lake.catalog_status WHERE server_name='c_m'"))
        assert caps.get('physical_gc') is True, caps
        q(cm, "CREATE FOREIGN TABLE biz.t_gc (id int4 NOT NULL, v text) SERVER c_m OPTIONS (binding_mode 'create', lake_schema 'main')")

        for bad_sql, params in [
            ("SELECT og_lake.set_option('biz.t_gc'::regclass, 'delete_older_than', '1 day')", None),
            ("SELECT og_lake.set_option('c_m'::name, 'delete_older_than', '-1 hour')", None),
            ("SELECT og_lake.set_option('c_m'::name, 'delete_older_than', 'not_an_interval')", None),
        ]:
            raised = False
            try:
                q(cm, bad_sql, params)
            except Exception:
                raised = True
            assert raised, bad_sql
        q(cm, "SELECT og_lake.set_option('c_m'::name, 'delete_older_than', '0 seconds')")
        opt_val = s(cm, "SELECT value FROM og_lake.options('c_m'::name) WHERE option_name='delete_older_than'")
        assert opt_val is not None, opt_val
        c1 += 2
        stages['stage1_upgrade_068_to_069_and_options'] = c1

        # Stage 2: Expire scheduling, same-txn refusal, snapshot pin / catalog guard / OldestXmin barriers, real unlink
        c2 = 0
        for batch in range(1, 5):
            q(cm, f"INSERT INTO biz.t_gc SELECT g, 'b{batch}_' || g FROM generate_series({(batch-1)*10+1}, {batch*10}) g")
            q(cm, "SELECT og_lake.flush('biz.t_gc')")
        # Trigger position deletes + flush replacement delete file + merge_adjacent_files (retiring old data files)
        q(cm, "DELETE FROM biz.t_gc WHERE id IN (2, 12, 22)")
        q(cm, "SELECT og_lake.flush('biz.t_gc')")
        q(cm, "DELETE FROM biz.t_gc WHERE id IN (3, 13)")
        q(cm, "SELECT og_lake.flush('biz.t_gc')")
        q(cm, "SELECT og_lake.set_option('biz.t_gc'::regclass, 'target_file_size', '10485760')")
        q(cm, "SELECT * FROM og_lake.merge_adjacent_files('biz.t_gc'::regclass, 16, NULL)")
        s_after_merge = s(cm, "SELECT og_lake.last_committed_snapshot('c_m')")
        q(cm, "INSERT INTO biz.t_gc VALUES (999, 'head_keep')")

        retired_del_count_before = s(cm, "SELECT count(*) FROM lake_metadata.ducklake_files_scheduled_for_deletion")
        assert retired_del_count_before >= 1, retired_del_count_before
        exp_cands = [r[0] for r in q(cm, "SELECT snapshot_id FROM lake_metadata.ducklake_snapshot WHERE snapshot_id < %s ORDER BY snapshot_id", (s_after_merge,))]
        assert len(exp_cands) >= 2, exp_cands

        # Same-txn expire_snapshots + cleanup_old_files must be rejected and rolled back cleanly
        q(cm, "BEGIN")
        q(cm, "SELECT snapshot_id FROM og_lake.expire_snapshots('c_m'::name, %s::bigint[], NULL, false)", (exp_cands,))
        same_txn_blocked = False
        try:
            q(cm, "SELECT * FROM og_lake.cleanup_old_files('c_m'::name, NULL, true, false)")
        except Exception:
            same_txn_blocked = True
        assert same_txn_blocked
        q(cm, "ROLLBACK")
        c2 += 2

        # Open XID holder BEFORE committing snapshot expiry so GetOldestXmin stays <= schedRel.xmin!
        r_hold = pg_connect(args.profile, db_main)
        conns.append(r_hold)
        q(r_hold, "BEGIN")
        q(r_hold, "CREATE TEMP TABLE _hold_xid(a int)")
        q(r_hold, "INSERT INTO _hold_xid VALUES (1)")

        # Commit snapshot expiry -> schedules unused merged data files into ducklake_files_scheduled_for_deletion
        q(cm, "SELECT snapshot_id FROM og_lake.expire_snapshots('c_m'::name, %s::bigint[], NULL, false)", (exp_cands,))
        sched_rows = q(cm, "SELECT data_file_id, path FROM lake_metadata.ducklake_files_scheduled_for_deletion ORDER BY data_file_id")
        assert len(sched_rows) >= 2, sched_rows
        sched_disk_paths = [cat_m / p for _, p in sched_rows]
        for spath in sched_disk_paths:
            assert spath.is_file(), f'Scheduled file missing before physical cleanup: {spath}'
        c2 += 1

        # Protection 1: Active reader snapshot pin blocks cleanup_old_files
        r_pin = pg_connect(args.profile, db_main)
        conns.append(r_pin)
        q(r_pin, "BEGIN READ ONLY")
        q(r_pin, "SELECT og_lake.bind_snapshot('c_m'::name, %s::bigint)", (s_after_merge,))
        dry_pin = q(cm, "SELECT data_file_id, deleted, status FROM og_lake.cleanup_old_files('c_m'::name, NULL, true, true)")
        assert any(st == 'blocked_snapshot_pin' and not dl for _, dl, st in dry_pin), dry_pin
        pin_blocked = ''
        try:
            q(cm, "SELECT * FROM og_lake.cleanup_old_files('c_m'::name, NULL, true, false)")
        except Exception as exc:
            pin_blocked = str(exc)
        assert 'active snapshot pins' in pin_blocked, pin_blocked
        q(r_pin, "COMMIT")
        c2 += 2

        # Protection 2a: Open data cursor blocks cleanup_old_files cleanly
        r_cur = pg_connect(args.profile, db_main)
        conns.append(r_cur)
        q(r_cur, "BEGIN")
        q(r_cur, "DECLARE c_hold CURSOR FOR SELECT id FROM biz.t_gc ORDER BY id")
        assert len(q(r_cur, "FETCH 1 FROM c_hold")) == 1
        cur_blocked = ''
        try:
            q(cm, "SELECT * FROM og_lake.cleanup_old_files('c_m'::name, NULL, true, false)")
        except Exception as exc:
            cur_blocked = str(exc)
        assert 'active snapshot pins' in cur_blocked or 'hold catalog guard' in cur_blocked, cur_blocked
        q(r_cur, "CLOSE c_hold")
        q(r_cur, "COMMIT")
        # Protection 2b: Metadata-only ShareLock catalog guard (without snapshot pin) fails fast on ExclusiveLock try-guard
        r_meta = pg_connect(args.profile, db_main)
        conns.append(r_meta)
        q(r_meta, "BEGIN")
        assert len(q(r_meta, "SELECT * FROM og_lake.snapshots('c_m'::name)")) >= 1
        guard_blocked = ''
        try:
            q(cm, "SELECT * FROM og_lake.cleanup_old_files('c_m'::name, NULL, true, false)")
        except Exception as exc:
            guard_blocked = str(exc)
        assert 'hold catalog guard' in guard_blocked, guard_blocked
        q(r_meta, "COMMIT")
        c2 += 2

        # Protection 3: GetOldestXmin watermark holds newly retired items until blocking XID transaction completes & Xmin advances
        res_xmin = q(cm, "SELECT data_file_id, deleted, status FROM og_lake.cleanup_old_files('c_m'::name, NULL, true, false)")
        assert len(res_xmin) >= 1 and all((not dl and st == 'deferred_oldest_xmin') for _, dl, st in res_xmin), res_xmin
        for spath in sched_disk_paths:
            assert spath.is_file(), f'Deferred OldestXmin file was deleted prematurely: {spath}'
        q(r_hold, "ROLLBACK")
        wait_until_eligible(cm, "SELECT data_file_id, deleted, status FROM og_lake.cleanup_old_files('c_m'::name, NULL, true, true)", status_idx=2)
        c2 += 1

        # Now real physical cleanup succeeds!
        res_clean = q(cm, "SELECT data_file_id, path, deleted, status FROM og_lake.cleanup_old_files('c_m'::name, NULL, true, false)")
        assert len(res_clean) >= 2 and all(dl and st == 'deleted' for _, _, dl, st in res_clean), res_clean
        for spath in sched_disk_paths:
            assert not spath.exists(), f'Cleaned file still present on disk: {spath}'
        assert s(cm, "SELECT count(*) FROM lake_metadata.ducklake_files_scheduled_for_deletion") == 0
        # Idempotent second run -> 0 rows
        assert q(cm, "SELECT * FROM og_lake.cleanup_old_files('c_m'::name, NULL, true, false)") == []
        c2 += 2
        stages['stage2_expire_schedule_and_pin_xmin_protection'] = c2

        # Stage 3: Orphan cleanup, symlink refusal, retired index segment cleanup, cancel recovery, DuckDB parity
        c3 = 0
        orphan_rel = 'main/orphan_crash_candidate.parquet'
        orphan_path = cat_m / orphan_rel
        orphan_path.parent.mkdir(parents=True, exist_ok=True)
        orphan_path.write_bytes(b'PAR1_FAKE_ORPHAN')
        outside_target = tmp_root / 'outside_keep.parquet'
        outside_target.write_bytes(b'KEEP_OUTSIDE_DATA')
        sym_path = cat_m / 'main/symlink_escape.parquet'
        sym_path.symlink_to(outside_target)

        dry_orphans = q(cm, "SELECT path, deleted, status FROM og_lake.delete_orphaned_files('c_m'::name, NULL, true, true)")
        assert [p for p, _, _ in dry_orphans] == [orphan_rel], dry_orphans
        # Future cutoff (negative older_than before now?) -> use past timestamp so fresh mtime is kept
        kept_recent = q(cm, "SELECT * FROM og_lake.delete_orphaned_files('c_m'::name, '2000-01-01 00:00:00+00'::timestamptz, false, false)")
        assert kept_recent == [] and orphan_path.is_file()
        del_orphans = q(cm, "SELECT path, deleted, status FROM og_lake.delete_orphaned_files('c_m'::name, NULL, true, false)")
        assert del_orphans == [(orphan_rel, True, 'deleted')], del_orphans
        assert not orphan_path.exists() and outside_target.read_bytes() == b'KEEP_OUTSIDE_DATA'
        c3 += 2

        # Create scalar B-tree index -> build_lake_index READY -> rebuild -> build_lake_index -> cleanup retired segment dir safely
        idx_uuid = s(cm, """SELECT og_lake.create_lake_index('biz.t_gc'::regclass, 'idx_gc_id'::name, '{"version":1,"kind":"scalar","method":"btree","column":"id"}'::jsonb)::text""")
        q(cm, "SELECT og_lake.build_lake_index(%s::uuid)", (idx_uuid,))
        old_segs = q(cm, "SELECT segment_uuid::text, artifact_root_uri FROM og_lake_internal.index_segment WHERE index_uuid=%s::uuid AND state='READY'", (idx_uuid,))
        assert len(old_segs) >= 1, old_segs
        q(cm, "SELECT og_lake.rebuild_lake_index(%s::uuid)", (idx_uuid,))
        q(cm, "SELECT og_lake.build_lake_index(%s::uuid)", (idx_uuid,))
        retired_uris = [u for _, u in old_segs]
        for u in retired_uris:
            assert Path(u).exists()
        wait_until_eligible(cm, "SELECT segment_uuid::text, deleted, status FROM og_lake.cleanup_index_segments('c_m'::name, true)", status_idx=2)
        seg_clean = q(cm, "SELECT segment_uuid::text, artifact_root_uri, state, deleted, status FROM og_lake.cleanup_index_segments('c_m'::name, false)")
        assert len(seg_clean) >= 1 and all(dl for _, _, _, dl, _ in seg_clean), seg_clean
        for _, uri, _, _, _ in seg_clean:
            assert not Path(uri).exists(), f'Retired segment dir still exists: {uri}'
        assert s(cm, "SELECT count(*) FROM og_lake_internal.index_segment WHERE index_uuid=%s::uuid AND state='RETIRED'", (idx_uuid,)) == 0
        assert s(cm, "SELECT count(*) FROM og_lake_internal.index_segment WHERE index_uuid=%s::uuid AND state='READY'", (idx_uuid,)) >= 1
        c3 += 2

        # Concurrent cancel & recovery check (blocked lake ANALYZE/flush cancel releases locks cleanly)
        blocker = pg_connect(args.profile, db_main)
        conns.append(blocker)
        q(blocker, "BEGIN")
        q(blocker, "DECLARE c_blk CURSOR FOR SELECT id FROM biz.t_gc ORDER BY id")
        assert len(q(blocker, "FETCH 1 FROM c_blk")) == 1
        r_blk = pg_connect(args.profile, db_main)
        conns.append(r_blk)
        blk_pid, blk_sid = q(r_blk, "SELECT pid, sessionid FROM pg_stat_activity WHERE sessionid = (og_lake.host_status()::jsonb ->> 'session_id')::bigint")[0]
        err_holder = []
        def run_blocked_flush():
            try:
                q(r_blk, "INSERT INTO biz.t_gc VALUES (998, 'cancel_me'); SELECT og_lake.flush('biz.t_gc')")
            except Exception as exc:
                err_holder.append(str(exc))
        t_cancel = threading.Thread(target=run_blocked_flush)
        # Hold AccessExclusive on metadata relation or catalog exclusive guard so flush waits on lock
        q(blocker, "CLOSE c_blk")
        q(blocker, "ROLLBACK")
        q(blocker, "BEGIN")
        q(blocker, "LOCK TABLE lake_metadata.ducklake_snapshot IN ACCESS EXCLUSIVE MODE")
        t_cancel.start()
        deadline_blk = time.monotonic() + 10.0
        live_pid = live_sid = None
        while time.monotonic() < deadline_blk:
            act = q(admin, "SELECT pid, sessionid, state, waiting FROM pg_stat_activity WHERE sessionid = %s", (blk_sid,))
            if act and act[0][2] == 'active':
                live_pid, live_sid = act[0][0], act[0][1]
                if act[0][3]:
                    break
            time.sleep(0.1)
        assert live_sid is not None, f'Blocked flush worker did not become active for session {blk_sid}'
        assert s(admin, "SELECT pg_cancel_session(%s, %s)", (live_pid, live_sid)) is True
        t_cancel.join(timeout=15)
        q(blocker, "ROLLBACK")
        assert len(err_holder) == 1, f'Expected cancel error, got {err_holder!r}'
        # Retry normal INSERT/ANALYZE/query succeeds cleanly after cancel
        q(cm, "ANALYZE biz.t_gc")
        og_rows = q(cm, "SELECT id, v FROM biz.t_gc ORDER BY id")
        assert len(og_rows) == 36, len(og_rows)
        c3 += 1

        if DUCKDB_BIN.is_file():
            # Flush remaining inline row 999 so DuckDB read_parquet sees exact surviving rows
            q(cm, "SELECT og_lake.flush('biz.t_gc')")
            og_rows = q(cm, "SELECT id, v FROM biz.t_gc ORDER BY id")
            dfiles, del_files = resolve_catalog_files(cm, cat_m)
            assert len(dfiles) >= 1
            for p in dfiles + del_files:
                assert p.is_file(), f'Surviving Parquet file missing: {p}'
            plist = ', '.join("'" + str(p).replace("'", "''") + "'" for p in dfiles)
            if del_files:
                dlist = ', '.join("'" + str(p).replace("'", "''") + "'" for p in del_files)
                dl_sql = (
                    f"SET threads=1; SELECT d.id, d.v FROM read_parquet([{plist}], filename=true, file_row_number=true) d "
                    f"LEFT JOIN read_parquet([{dlist}]) del ON (d.filename = del.file_path OR ends_with(d.filename, '/' || del.file_path)) AND d.file_row_number = del.pos "
                    f"WHERE del.pos IS NULL ORDER BY d.id;"
                )
            else:
                dl_sql = f"SET threads=1; SELECT id, v FROM read_parquet([{plist}]) ORDER BY id;"
            proc = subprocess.run([str(DUCKDB_BIN), '-no-init', '-batch', '-dark-mode', '-json', '-c', dl_sql],
                                  capture_output=True, text=True, check=True)
            duck_rows = [(int(r['id']), r['v']) for r in json.loads(proc.stdout)]
            assert duck_rows == og_rows, f'DuckDB parity mismatch after physical cleanup: duck={len(duck_rows)} og={len(og_rows)}'
            c3 += 1
        stages['stage3_orphan_index_gc_and_duckdb_parity'] = c3

        pool_state = s(cm, 'SHOW enable_thread_pool')
        total_checks = sum(stages.values())
        result = {
            'status': 'pass',
            'profile': args.profile,
            'postgres_ext_version': '0.69',
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
