#!/usr/bin/env python3
"""SQL0.64 (IX1~IX5 & N7-SMP) verification suite + DuckDB parity."""

import argparse
import decimal
import hashlib
import json
import os
import getpass
import shutil
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path

import psycopg2

ROOT = Path(os.environ.get('CPP_WORKSPACE', str(Path(__file__).resolve().parents[5]))).expanduser().resolve()
INTEGRATION = ROOT / 'integration/opengauss-native-lake'
sys.path.insert(0, str(INTEGRATION))
from native_runtime import RUNTIME

DUCKDB_BIN = ROOT / '.reference/opengauss-native-lake/build/duckdb-gcc10/duckdb'


def md5_role_pw(pw, role):
    return 'md5' + hashlib.md5((pw + role).encode('utf-8')).hexdigest()


def duckdb_json(sql_text):
    proc = subprocess.run(
        [str(DUCKDB_BIN), '-batch', '-light-mode', '-json', '-c', 'LOAD ducklake;\n' + sql_text],
        capture_output=True, text=True, timeout=60,
    )
    if proc.returncode != 0:
        raise RuntimeError(f'DuckDB failed ({proc.returncode}): {proc.stderr}\nSQL:\n{sql_text}')
    out = proc.stdout.strip()
    return json.loads(out) if out else []


def pg_connect(profile, dbname, user=None, password=None):
    port = 55560 if profile == 'debug' else (55570 if profile == 'release' else 55580)
    sock_dir = str((RUNTIME.parent / profile / 'socket').resolve())
    kwargs = {
        'host': sock_dir,
        'port': port,
        'dbname': dbname,
        'user': user or os.environ.get('PGUSER') or getpass.getuser(),
        'connect_timeout': 15,
        'application_name': 'sql064_verify',
    }
    if password is not None:
        kwargs['password'] = password
    conn = psycopg2.connect(**kwargs)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute('SET session_timeout = 0')
        cur.execute('SET statement_timeout = 120000')
        cur.execute("SET TimeZone = 'UTC'")
    return conn


def q(conn, sql, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall() if cur.description else None


def s(conn, sql, params=None):
    rows = q(conn, sql, params)
    return rows[0][0] if rows else None


def expect_error(conn, sql, expected_states=None, expected_substr=None, params=None):
    with conn.cursor() as cur:
        try:
            cur.execute(sql, params)
        except psycopg2.Error as exc:
            state = exc.pgcode or ''
            msg = str(exc)
            if expected_states and state not in expected_states:
                raise AssertionError(f'Expected SQLSTATE {expected_states}, got {state}: {msg}\nSQL: {sql}')
            if expected_substr and expected_substr.lower() not in msg.lower():
                raise AssertionError(f'Expected substr {expected_substr!r} in error: {msg}\nSQL: {sql}')
            return state, msg
    raise AssertionError(f'Expected error but query succeeded: {sql}')


def explain_lines(conn, sql, params=None):
    rows = q(conn, 'EXPLAIN (ANALYZE, COSTS OFF) ' + sql, params)
    return '\n'.join(r[0] for r in rows)


def vec_literal(i, dim=16):
    vals = [(((i * 131 + j * 79 + ((i * (j + 1)) % 29) * 17) % 2039) - 1019) / 1024.0 for j in range(dim)]
    return '[' + ','.join(format(v, '.9g') for v in vals) + ']'


def run_ix5_checks(args, admin, tmp_root, created_dbs, created_roles, stages):
    tag = uuid.uuid4().hex[:8]
    role_ix5 = f'u064_ix5_{tag}'
    db_ix5 = f'db064_ix5_{tag}'
    q(admin, f"CREATE ROLE {role_ix5} NOLOGIN NOINHERIT ENCRYPTED PASSWORD %s", (md5_role_pw('Ix5_064!', role_ix5),))
    created_roles.append(role_ix5)
    q(admin, f"CREATE DATABASE {db_ix5} WITH DBCOMPATIBILITY 'PG' ENCODING 'UTF8'")
    created_dbs.append(db_ix5)

    assert s(admin, 'SHOW enable_lake_maintenance') == 'on'
    assert s(admin, 'SHOW enable_lake_index_maintenance') == 'on'
    assert s(admin, 'SHOW enable_thread_pool') == 'off'

    c5 = pg_connect(args.profile, db_ix5)
    db_oid = None
    try:
        q(c5, 'CREATE EXTENSION og_lake')
        assert s(c5, "SELECT extversion FROM pg_extension WHERE extname='og_lake'") in ('0.64', '0.73', '0.74')
        db_oid = s(c5, 'SELECT oid FROM pg_database WHERE datname = %s', (db_ix5,))
        q(c5, 'SELECT og_lake.configure_internal_owner(%s::name)', (role_ix5,))
        cat_dir = tmp_root / 'cat_ix5'
        idx_dir = tmp_root / 'idx_ix5'
        idx_dir.mkdir(parents=True)
        os.chmod(idx_dir, 0o755)
        q(c5, "SELECT og_lake.create_catalog('cat_ix5'::name, 'cat_ix5_meta'::name, %s::text, NULL::text)", (str(cat_dir),))
        q(c5, "SELECT og_lake.set_option('cat_ix5'::name, 'index_root'::text, %s::text)", (str(idx_dir),))
        q(c5, "SELECT og_lake.set_option('cat_ix5'::name, 'data_inlining_row_limit'::text, '500'::text)")

        q(c5, """
            CREATE TABLE exact_ix5 (
                id int, grp int, raw bytea, u uuid, j json, num numeric(10,2), v vector(3), note text
            )
        """)
        q(c5, """
            CREATE FOREIGN TABLE t_ix5 (
                id int, grp int, raw bytea, u uuid, j json, num numeric(10,2), v vector(3), note text
            ) SERVER cat_ix5 OPTIONS (binding_mode 'create', lake_schema 'main')
        """)
        q(c5, """
            INSERT INTO exact_ix5
            SELECT i, i % 4,
                   decode(lpad(to_hex(i), 4, '0') || 'a0', 'hex'),
                   ('00000000-0000-4000-8000-' || lpad(to_hex(i), 12, '0'))::uuid,
                   json_build_object('ix5', i),
                   ((i - 10) * 1.25)::numeric(10,2),
                   ('[' || (i)::text || ',' || (i*0.5)::text || ',' || ((i % 7) + 0.25)::text || ']')::vector(3),
                   'r_' || i
            FROM generate_series(1, 32) i
        """)
        q(c5, 'INSERT INTO t_ix5 SELECT * FROM exact_ix5 ORDER BY id')
        q(c5, "SELECT og_lake.flush('t_ix5'::regclass)")
        assert s(c5, "SELECT count(*) FROM cat_ix5_meta.ducklake_data_file WHERE end_snapshot IS NULL") == 1

        ix_bt = s(c5, "SELECT og_lake.create_lake_index('t_ix5'::regclass, 'ix5_bt'::name, '{\"version\":1,\"kind\":\"scalar\",\"method\":\"btree\",\"column\":\"id\"}'::jsonb)")
        ix_bm = s(c5, "SELECT og_lake.create_lake_index('t_ix5'::regclass, 'ix5_bm'::name, '{\"version\":1,\"kind\":\"scalar\",\"method\":\"bitmap\",\"column\":\"grp\"}'::jsonb)")
        ix_ann = None
        if args.profile in ('ann-debug', 'release'):
            ix_ann = s(c5, "SELECT og_lake.create_lake_index('t_ix5'::regclass, 'ix5_ann'::name, '{\"version\":1,\"kind\":\"vector\",\"method\":\"diskann\",\"column\":\"v\",\"metric\":\"l2\",\"enable_pq\":false}'::jsonb)")

        wanted_uuids = [ix_bt, ix_bm] + ([ix_ann] if ix_ann else [])

        def get_ready(idx_uuid):
            return q(c5, """
                SELECT s.segment_uuid::text, s.artifact_root_uri::text, s.manifest_hash, s.definition_generation
                FROM og_lake_internal.index_segment s
                JOIN og_lake_internal.index_coverage c USING(segment_uuid, index_uuid, definition_generation)
                WHERE c.index_uuid = %s::uuid AND c.active AND s.state = 'READY'
            """, (idx_uuid,))

        deadline = time.monotonic() + 45.0
        ready_map = {}
        while time.monotonic() < deadline:
            ready_map = {u: get_ready(u) for u in wanted_uuids}
            if all(len(v) == 1 for v in ready_map.values()):
                break
            time.sleep(0.25)
        for u_id, rows in ready_map.items():
            assert len(rows) == 1, f'IX5 auto-build timed out for {u_id}: {rows}'

        # Verify scalar index coverage & extended payload parity
        q(c5, 'SET enable_seqscan = off')
        exp_bt = explain_lines(c5, 'SELECT id, num, j::text FROM t_ix5 WHERE id = 17')
        assert 'Verified Scalar File Coverage: 1' in exp_bt, exp_bt
        assert q(c5, 'SELECT id, encode(raw,\'hex\'), u::text, j::text, num FROM t_ix5 WHERE id = 17') == \
               q(c5, 'SELECT id, encode(raw,\'hex\'), u::text, j::text, num FROM exact_ix5 WHERE id = 17')

        exp_bm = explain_lines(c5, 'SELECT id FROM t_ix5 WHERE grp = 2 ORDER BY id')
        assert 'Verified Bitmap File Coverage: 1' in exp_bm, exp_bm
        assert q(c5, 'SELECT id, num FROM t_ix5 WHERE grp = 2 ORDER BY id') == \
               q(c5, 'SELECT id, num FROM exact_ix5 WHERE grp = 2 ORDER BY id')

        if ix_ann:
            q(c5, 'SET enable_lake_ann = on')
            ann_q = "SELECT id, u::text, num FROM t_ix5 WHERE v IS NOT NULL ORDER BY v <-> '[5,2.5,5.25]'::vector(3) LIMIT 5"
            exp_ann = explain_lines(c5, ann_q)
            assert 'Verified ANN File Coverage: 1' in exp_ann, exp_ann
            assert q(c5, ann_q) == q(c5, ann_q.replace('t_ix5', 'exact_ix5'))

        # Repair test: corrupt owned parquet artifact of B-tree (and DiskANN on ann-debug)
        for idx_u in ([ix_bt, ix_ann] if ix_ann else [ix_bt]):
            old_seg, art_dir, _, _ = ready_map[idx_u][0]
            pqs = sorted(Path(art_dir).glob('*.parquet'))
            assert pqs, art_dir
            pqs[0].write_bytes(b'')
            repair_deadline = time.monotonic() + 45.0
            new_rows = []
            while time.monotonic() < repair_deadline:
                new_rows = get_ready(idx_u)
                if len(new_rows) == 1 and new_rows[0][0] != old_seg:
                    break
                time.sleep(0.25)
            assert len(new_rows) == 1 and new_rows[0][0] != old_seg, (idx_u, old_seg, new_rows)
            assert s(c5, 'SELECT state FROM og_lake_internal.index_segment WHERE segment_uuid = %s::uuid', (old_seg,)) == 'RETIRED'

        # Re-verify query after repair & generation bump rebuild
        assert q(c5, 'SELECT id, encode(raw,\'hex\'), u::text, j::text, num FROM t_ix5 WHERE id = 17') == \
               q(c5, 'SELECT id, encode(raw,\'hex\'), u::text, j::text, num FROM exact_ix5 WHERE id = 17')
        q(c5, 'SELECT og_lake.rebuild_lake_index(%s::uuid)', (ix_bt,))
        assert s(c5, 'SELECT definition_generation FROM og_lake_internal.index_definition WHERE index_uuid = %s::uuid', (ix_bt,)) == 2
        assert len(get_ready(ix_bt)) == 1
        assert q(c5, 'SELECT id, num FROM t_ix5 WHERE id = 17') == [(17, decimal.Decimal('8.75'))]

        # Clean teardown of background workers in db_ix5 before DROP DATABASE
        q(c5, 'DROP FOREIGN TABLE t_ix5')
        q(c5, 'DROP SERVER cat_ix5 CASCADE')
    finally:
        c5.close()

    if db_oid is not None:
        drain_deadline = time.monotonic() + 15.0
        while time.monotonic() < drain_deadline:
            pids = [r[0] for r in q(admin, "SELECT pid FROM pg_stat_activity WHERE datid = %s", (db_oid,))]
            if not pids:
                break
            for p in pids:
                try:
                    q(admin, 'SELECT pg_cancel_backend(%s)', (p,))
                except Exception:
                    pass
            time.sleep(0.1)
    q(admin, f'DROP DATABASE {db_ix5}')
    created_dbs.remove(db_ix5)
    q(admin, f'DROP ROLE {role_ix5}')
    created_roles.remove(role_ix5)
    stages['stage4_ix5_background_lifecycle'] = 14 if args.profile in ('ann-debug', 'release') else 10


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--profile', choices=['debug', 'ann-debug', 'release'], required=True)
    ap.add_argument('--out', required=True)
    ap.add_argument('--ix5-only', action='store_true')
    args = ap.parse_args()

    tag = uuid.uuid4().hex[:10]
    tmp_root = Path(tempfile.mkdtemp(prefix=f'og_064_{tag}_', dir='/tmp'))
    os.chmod(tmp_root, 0o755)
    admin = None
    created_dbs = []
    created_roles = []
    stages = {}
    duckdb_checks = 0

    try:
        admin = pg_connect(args.profile, 'postgres')

        if args.ix5_only:
            run_ix5_checks(args, admin, tmp_root, created_dbs, created_roles, stages)
        else:
            role_upg = f'u064_upg_{tag}'
            role_main = f'u064_main_{tag}'
            db_upg = f'db064_upg_{tag}'
            db_main = f'db064_main_{tag}'
            for rn in (role_upg, role_main):
                q(admin, f"CREATE ROLE {rn} NOLOGIN NOINHERIT ENCRYPTED PASSWORD %s", (md5_role_pw('Own064!', rn),))
                created_roles.append(rn)
            for dbn in (db_upg, db_main):
                q(admin, f"CREATE DATABASE {dbn} WITH DBCOMPATIBILITY 'PG' ENCODING 'UTF8'")
                created_dbs.append(dbn)

            # ------------------------------------------------------------------
            # Stage 1: Version Gating (0.63 -> 0.64) & host_status()
            # ------------------------------------------------------------------
            cu = pg_connect(args.profile, db_upg)
            try:
                q(cu, "CREATE EXTENSION og_lake VERSION '0.63'")
                assert s(cu, "SELECT extversion FROM pg_extension WHERE extname='og_lake'") == '0.63'
                hs = json.loads(s(cu, 'SELECT og_lake.host_status()'))
                assert hs.get('module_version') in ('0.64', '0.73', '0.74'), hs
                assert hs.get('ix_smp_extended') is True, hs
                assert hs.get('ix_smp_extended_sql_version') == '0.64', hs
                assert hs.get('pq_ann_smp') is True, hs
                assert hs.get('extended_payload_indexes') is True, hs

                q(cu, 'SELECT og_lake.configure_internal_owner(%s::name)', (role_upg,))
                q(cu, "SELECT og_lake.create_catalog('cat_u'::name, 'cat_u_meta'::name, %s::text, NULL::text)", (str(tmp_root / 'cat_u'),))
                q(cu, "CREATE FOREIGN TABLE t_u (id int, u uuid, j json, num numeric(12,4)) SERVER cat_u OPTIONS (binding_mode 'create', lake_schema 'main')")
                expect_error(cu, "SELECT og_lake.create_lake_index('t_u'::regclass, 'ix_fail'::name, '{\"version\":1,\"kind\":\"scalar\",\"method\":\"btree\",\"column\":\"id\"}'::jsonb)",
                             expected_states={'0A000'})
                q(cu, "ALTER EXTENSION og_lake UPDATE TO '0.64'")
                assert s(cu, "SELECT extversion FROM pg_extension WHERE extname='og_lake'") == '0.64'
                ix_ok = s(cu, "SELECT og_lake.create_lake_index('t_u'::regclass, 'ix_ok'::name, '{\"version\":1,\"kind\":\"scalar\",\"method\":\"btree\",\"column\":\"id\"}'::jsonb)")
                assert ix_ok
                stages['stage1_version_064_upgrade_contract'] = 8
            finally:
                cu.close()

            # ------------------------------------------------------------------
            # Stage 2: Extended Payloads + Scalar Indexes + Row SMP + ReScan + DuckDB
            # ------------------------------------------------------------------
            cm = pg_connect(args.profile, db_main)
            try:
                q(cm, 'CREATE EXTENSION og_lake')
                assert s(cm, "SELECT extversion FROM pg_extension WHERE extname='og_lake'") in ('0.64', '0.73', '0.74')
                q(cm, 'SELECT og_lake.configure_internal_owner(%s::name)', (role_main,))
                cat_dir = tmp_root / 'cat_main'
                idx_dir = tmp_root / 'idx_main'
                idx_dir.mkdir(parents=True)
                os.chmod(idx_dir, 0o755)
                q(cm, "SELECT og_lake.create_catalog('cat_main'::name, 'cat_meta'::name, %s::text, NULL::text)", (str(cat_dir),))
                q(cm, "SELECT og_lake.set_option('cat_main'::name, 'index_root'::text, %s::text)", (str(idx_dir),))
                q(cm, "SELECT og_lake.set_option('cat_main'::name, 'data_inlining_row_limit'::text, '5000'::text)")

                q(cm, """
                    CREATE TABLE exact_ext (
                        id int, grp int, tag text, raw bytea, u uuid, j json, num numeric(12,4), vec vector(4)
                    )
                """)
                q(cm, """
                    CREATE FOREIGN TABLE t_ext_idx (
                        id int, grp int, tag text, raw bytea, u uuid, j json, num numeric(12,4), vec vector(4)
                    ) SERVER cat_main OPTIONS (binding_mode 'create', lake_schema 'main')
                """)
                for b in range(4):
                    start = b * 60 + 1
                    end = (b + 1) * 60
                    q(cm, f"""
                        INSERT INTO exact_ext
                        SELECT i, (i % 5), 'tag_' || i,
                               CASE WHEN i % 11 = 0 THEN NULL ELSE decode(lpad(to_hex(i), 4, '0') || 'ff', 'hex') END,
                               CASE WHEN i % 13 = 0 THEN NULL ELSE ('12345678-1234-4123-8123-' || lpad(to_hex(i), 12, '0'))::uuid END,
                               CASE WHEN i % 17 = 0 THEN NULL ELSE json_build_object('k', i, 'm', i % 5) END,
                               CASE WHEN i % 19 = 0 THEN NULL ELSE ((i - 100) * 1.125)::numeric(12,4) END,
                               ('[' || (i % 9 + 1)::text || ',' || (i % 7 + 1)::text || ',' || (i % 5 + 1)::text || ',' || (i % 3 + 1)::text || ']')::vector(4)
                        FROM generate_series({start}, {end}) i
                    """)
                    q(cm, 'INSERT INTO t_ext_idx SELECT * FROM exact_ext WHERE id BETWEEN %s AND %s ORDER BY id', (start, end))
                    q(cm, "SELECT og_lake.flush('t_ext_idx'::regclass)")
                assert s(cm, "SELECT count(*) FROM cat_meta.ducklake_data_file WHERE end_snapshot IS NULL") == 4

                # Inline additions + mixed UPDATE/DELETE
                for tbl in ('exact_ext', 't_ext_idx'):
                    q(cm, f"""
                        INSERT INTO {tbl} VALUES
                        (241, 2, 'inline_a', '\\x00deadbeef'::bytea, 'aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee'::uuid, '{{"x":true}}'::json, -42.5000, '[0.1,0.2,0.3,0.4]'::vector(4)),
                        (242, 4, 'inline_b', '\\x'::bytea, NULL, NULL, 99999.1234, '[9,9,9,9]'::vector(4))
                    """)
                    q(cm, f"DELETE FROM {tbl} WHERE id IN (5, 242)")
                    q(cm, f"UPDATE {tbl} SET num = -0.0005, j = '{{\"upd\":1}}'::json WHERE id = 42")

                # Verify extended payload column itself still rejected as scalar index key
                expect_error(cm, "SELECT og_lake.create_lake_index('t_ext_idx'::regclass, 'ix_bad_u'::name, '{\"version\":1,\"kind\":\"scalar\",\"method\":\"btree\",\"column\":\"u\"}'::jsonb)",
                             expected_states={'0A000'})

                ix_bt = s(cm, "SELECT og_lake.create_lake_index('t_ext_idx'::regclass, 'ix_ext_bt'::name, '{\"version\":1,\"kind\":\"scalar\",\"method\":\"btree\",\"column\":\"id\"}'::jsonb)")
                ix_bm = s(cm, "SELECT og_lake.create_lake_index('t_ext_idx'::regclass, 'ix_ext_bm'::name, '{\"version\":1,\"kind\":\"scalar\",\"method\":\"bitmap\",\"column\":\"grp\"}'::jsonb)")
                q(cm, 'SELECT og_lake.build_lake_index(%s::uuid)', (ix_bt,))
                q(cm, 'SELECT og_lake.build_lake_index(%s::uuid)', (ix_bm,))

                q(cm, 'SET enable_seqscan = off')
                q(cm, 'SET enable_indexscan = on')
                exp_bt = explain_lines(cm, 'SELECT id, encode(raw,\'hex\'), u::text, j::text, num FROM t_ext_idx WHERE id = 42')
                assert 'Verified Scalar File Coverage: 4' in exp_bt, exp_bt
                assert q(cm, 'SELECT id, encode(raw,\'hex\'), u::text, j::text, num FROM t_ext_idx WHERE id = 42') == \
                       q(cm, 'SELECT id, encode(raw,\'hex\'), u::text, j::text, num FROM exact_ext WHERE id = 42')

                exp_bm = explain_lines(cm, 'SELECT id, num FROM t_ext_idx WHERE grp = 2 ORDER BY id')
                assert 'Verified Bitmap File Coverage: 4' in exp_bm, exp_bm
                assert q(cm, 'SELECT id, encode(raw,\'hex\'), u::text, j::text, num FROM t_ext_idx WHERE grp = 2 ORDER BY id') == \
                       q(cm, 'SELECT id, encode(raw,\'hex\'), u::text, j::text, num FROM exact_ext WHERE grp = 2 ORDER BY id')

                # Parameterized ReScan (PARAM_EXEC)
                q(cm, 'CREATE TEMP TABLE probes_064 (k int)')
                q(cm, 'INSERT INTO probes_064 VALUES (17), (42), (241)')
                nl_sql = """
                    SELECT /*+ NestLoop(probes_064 {T}) */ {T}.id, encode({T}.raw,'hex'), {T}.u::text, {T}.j::text, {T}.num
                    FROM probes_064 JOIN {T} ON {T}.id = probes_064.k ORDER BY {T}.id
                """
                assert q(cm, nl_sql.format(T='t_ext_idx')) == q(cm, nl_sql.format(T='exact_ext'))

                # Row SMP (dop=2,4) with extended payloads
                q(cm, 'SET enable_seqscan = on')
                q(cm, 'SET enable_vector_engine = off')
                q(cm, 'SET enable_lake_smp = on')
                serial_full = q(cm, "SELECT id, grp, tag, encode(raw,'hex'), u, j, num, vec FROM exact_ext ORDER BY id")
                for dop in (2, 4):
                    if args.profile == 'release':
                        q(cm, 'SET enable_force_smp = off')
                        q(cm, f'SET query_dop = {dop}')
                        q(cm, 'SET enable_force_smp = on')
                    else:
                        q(cm, f'SET query_dop = {1000 + dop}')
                    exp_smp = explain_lines(cm, 'SELECT count(*), sum(num) FROM t_ext_idx')
                    assert f'Lake SMP Planned DOP: {dop}' in exp_smp, (dop, exp_smp)
                    assert q(cm, 'SELECT count(*), sum(num) FROM t_ext_idx') == q(cm, 'SELECT count(*), sum(num) FROM exact_ext')
                    exp_proj = explain_lines(cm, "SELECT id, grp, tag, encode(raw,'hex'), u, j, num, vec FROM t_ext_idx ORDER BY id")
                    assert f'Lake SMP Planned DOP: {dop}' in exp_proj, (dop, exp_proj)
                    assert q(cm, "SELECT id, grp, tag, encode(raw,'hex'), u, j, num, vec FROM t_ext_idx ORDER BY id") == serial_full
                    # Scalar index + SMP with extended payloads
                    exp_bm_smp = explain_lines(cm, "SELECT id, encode(raw,'hex'), u, j, num FROM t_ext_idx WHERE grp = 2 ORDER BY id")
                    assert f'Lake SMP Planned DOP: {dop}' in exp_bm_smp, (dop, exp_bm_smp)
                    assert q(cm, "SELECT id, encode(raw,'hex'), u, j, num FROM t_ext_idx WHERE grp = 2 ORDER BY id") == \
                           q(cm, "SELECT id, encode(raw,'hex'), u, j, num FROM exact_ext WHERE grp = 2 ORDER BY id")
                q(cm, 'SET enable_force_smp = off')
                q(cm, 'SET query_dop = 1')

                # INT2 + extended payload Row SMP
                q(cm, 'CREATE TABLE exact_i2_ext (k smallint, u uuid, num numeric(10,2))')
                q(cm, "CREATE FOREIGN TABLE t_i2_ext (k smallint, u uuid, num numeric(10,2)) SERVER cat_main OPTIONS (binding_mode 'create', lake_schema 'main')")
                for b in range(4):
                    q(cm, """
                        INSERT INTO exact_i2_ext
                        SELECT i::smallint, ('00000000-0000-4000-8000-' || lpad(to_hex(i), 12, '0'))::uuid, (i * 1.5)::numeric(10,2)
                        FROM generate_series(%s, %s) i
                    """, (b * 10 + 1, (b + 1) * 10))
                    q(cm, 'INSERT INTO t_i2_ext SELECT * FROM exact_i2_ext WHERE k BETWEEN %s AND %s ORDER BY k', (b * 10 + 1, (b + 1) * 10))
                    q(cm, "SELECT og_lake.flush('t_i2_ext'::regclass)")
                if args.profile == 'release':
                    q(cm, 'SET enable_force_smp = off')
                    q(cm, 'SET query_dop = 2')
                    q(cm, 'SET enable_force_smp = on')
                else:
                    q(cm, 'SET query_dop = 1002')
                exp_i2 = explain_lines(cm, 'SELECT k, u, num FROM t_i2_ext ORDER BY k')
                assert 'Lake SMP Planned DOP: 2' in exp_i2, exp_i2
                assert q(cm, 'SELECT k, u, num FROM t_i2_ext ORDER BY k') == q(cm, 'SELECT k, u, num FROM exact_i2_ext ORDER BY k')
                assert q(cm, 'SELECT count(*), sum(k), sum(num) FROM t_i2_ext') == q(cm, 'SELECT count(*), sum(k), sum(num) FROM exact_i2_ext')
                q(cm, 'SET enable_force_smp = off')
                q(cm, 'SET query_dop = 1')

                # DuckDB COPY + parquet read parity check
                ext_pq = tmp_root / 'ext_out.parquet'
                q(cm, f"COPY (SELECT id, encode(raw,'hex') AS hx, u::text AS u, j::text AS j, num FROM t_ext_idx WHERE id IN (1, 42, 241) ORDER BY id) TO '{ext_pq}' WITH (FORMAT parquet)")
                dk_ext = duckdb_json(f"SELECT id, hx, u, j, num::VARCHAR AS num FROM read_parquet('{ext_pq}') ORDER BY id")
                duckdb_checks += 1
                assert len(dk_ext) == 3 and dk_ext[1]['id'] == 42 and dk_ext[1]['num'] == '-0.0005', dk_ext
                assert dk_ext[2]['id'] == 241 and dk_ext[2]['num'] == '-42.5000', dk_ext
                stages['stage2_extended_indexes_row_smp'] = 18

                # ------------------------------------------------------------------
                # Stage 3: PQ Parallel SMP (format=1..2, dop=2/4, >2 files,
                #          unfiltered + complex Boolean/IN trees + extended payloads)
                # ------------------------------------------------------------------
                if args.profile in ('ann-debug', 'release'):
                    q(cm, """
                        CREATE TABLE exact_pq (
                            id int, grp int, flag int, score float8, raw bytea, u uuid, j json, num numeric(12,4), vec vector(16)
                        )
                    """)
                    q(cm, """
                        CREATE FOREIGN TABLE t_pq_smp (
                            id int, grp int, flag int, score float8, raw bytea, u uuid, j json, num numeric(12,4), vec vector(16)
                        ) SERVER cat_main OPTIONS (binding_mode 'create', lake_schema 'main')
                    """)
                    # Insert 4 files * 512 rows = 2048 distinct vectors (supports pq_chunks=4 training >=256/file)
                    for b in range(4):
                        rows = []
                        for i in range(b * 512 + 1, (b + 1) * 512 + 1):
                            v_s = vec_literal(i, 16)
                            grp_v, flag_v, score_v, num_s = i % 5, i % 4, (i % 10) - 2.5, f"{(i - 500) * 0.25:.4f}"
                            if i == 700:
                                # Plant close known target in File 2 (no deletes on File 2 -> must use PQ ANN, not fallback)
                                v_s = '[' + ','.join(['0.02'] * 16) + ']'
                                grp_v, flag_v, score_v, num_s = 0, 0, 10.5, '123.4500'
                            hx = f"{i:04x}cc"
                            uu = f"22222222-3333-4444-8555-{i:012x}"
                            js = json.dumps({'i': i, 'b': b})
                            rows.append(
                                f"({i}, {grp_v}, {flag_v}, {score_v}, decode('{hx}','hex'), '{uu}'::uuid, '{js}'::json, {num_s}, '{v_s}'::vector(16))"
                            )
                        vals_sql = ','.join(rows)
                        q(cm, 'INSERT INTO exact_pq VALUES ' + vals_sql)
                        q(cm, 'INSERT INTO t_pq_smp VALUES ' + vals_sql)
                        q(cm, "SELECT og_lake.flush('t_pq_smp'::regclass)")
                    assert s(cm, "SELECT count(*) FROM cat_meta.ducklake_data_file df JOIN cat_meta.ducklake_table t ON t.table_id=df.table_id WHERE t.table_name='t_pq_smp' AND df.end_snapshot IS NULL") == 4

                    # Add inline planted row + NULL/DELETE mix (only File 1 gets deleted so File 2..4 use PQ ANN!)
                    for tbl in ('exact_pq', 't_pq_smp'):
                        q(cm, f"""
                            INSERT INTO {tbl} VALUES
                            (3001, 2, 3, 5.0, '\\xaa11'::bytea, 'ffffffff-0000-4000-8000-000000003001'::uuid, '{{"inline":true}}'::json, -7.7500, '[0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01]'::vector(16)),
                            (3002, 2, 1, 5.0, '\\xbb22'::bytea, 'ffffffff-0000-4000-8000-000000003002'::uuid, '{{"inline":false}}'::json, 8.2500, NULL)
                        """)
                        q(cm, f"DELETE FROM {tbl} WHERE id = 10")

                    ix_pq_l2 = s(cm, "SELECT og_lake.create_lake_index('t_pq_smp'::regclass, 'ix_pq_l2'::name, '{\"version\":1,\"kind\":\"vector\",\"method\":\"diskann\",\"column\":\"vec\",\"metric\":\"l2\",\"pq\":true,\"pq_chunks\":4}'::jsonb)")
                    q(cm, 'SET statement_timeout = 900000')
                    # PQ builds open 5 FileCandidates/source (capacity=3 per call under 16-candidate bound); call twice for all 4 files
                    q(cm, 'SELECT og_lake.build_lake_index(%s::uuid)', (ix_pq_l2,))
                    q(cm, 'SELECT og_lake.build_lake_index(%s::uuid)', (ix_pq_l2,))
                    q(cm, 'SET statement_timeout = 120000')

                    # Confirm format_version=2 for all 4 files of ix_pq_l2
                    fv_counts = q(cm, """
                        SELECT s.format_version, count(*)
                        FROM og_lake_internal.index_segment s
                        JOIN og_lake_internal.index_coverage c USING(segment_uuid, index_uuid, definition_generation)
                        WHERE c.index_uuid = %s::uuid AND c.active AND s.state = 'READY'
                        GROUP BY s.format_version
                    """, (ix_pq_l2,))
                    assert fv_counts == [(2, 4)], fv_counts

                    q(cm, 'SET enable_vector_engine = off')
                    q(cm, 'SET enable_lake_smp = on')
                    q(cm, 'SET enable_lake_ann = on')
                    q(cm, 'SET ann_orderby_distance_reuse = off')
                    q(cm, 'SET query_max_mem = 0')
                    q(cm, 'SET enable_indexscan = on')
                    q(cm, 'SET enable_seqscan = off')

                    # 3a: Unfiltered PQ ANN SMP (dop=2 and dop=4) across 4 files + inline (no CoerceViaIO ::text)
                    zero_vec = '[' + ','.join(['0'] * 16) + ']'
                    unfiltered_q = f"""
                        SELECT id, encode(raw,'hex'), u, j, num, (vec <-> '{zero_vec}'::vector(16)) AS dist
                        FROM t_pq_smp
                        WHERE vec IS NOT NULL
                        ORDER BY vec <-> '{zero_vec}'::vector(16) LIMIT 5
                    """
                    exact_unfiltered_map = {
                        r[0]: (r[0], r[1], r[2], r[3], r[4], round(float(r[5]), 6))
                        for r in q(cm, f"SELECT id, encode(raw,'hex'), u, j, num, (vec <-> '{zero_vec}'::vector(16)) AS dist FROM exact_pq WHERE vec IS NOT NULL")
                    }
                    for dop in (2, 4):
                        if args.profile == 'release':
                            q(cm, 'SET enable_force_smp = off')
                            q(cm, f'SET query_dop = {dop}')
                            q(cm, 'SET enable_force_smp = on')
                        else:
                            q(cm, f'SET query_dop = {1000 + dop}')
                        exp_u = explain_lines(cm, unfiltered_q)
                        assert f'Lake SMP Planned DOP: {dop}' in exp_u, (dop, exp_u)
                        assert 'Lake SMP ANN: raw L2 unfiltered' in exp_u, (dop, exp_u)
                        assert 'Lake SMP ANN PQ: admitted' in exp_u, (dop, exp_u)
                        assert 'Verified PQ File Coverage: 3' in exp_u, (dop, exp_u)
                        res_u = q(cm, unfiltered_q)
                        assert len(res_u) == 5, res_u
                        returned_ids = [r[0] for r in res_u]
                        assert 3001 in returned_ids and 700 in returned_ids, returned_ids
                        for r in res_u:
                            norm = (r[0], r[1], r[2], r[3], r[4], round(float(r[5]), 6))
                            assert norm == exact_unfiltered_map[r[0]], (norm, exact_unfiltered_map[r[0]])
                        dists = [float(row[5]) for row in res_u]
                        assert dists == sorted(dists), dists

                    # 3b: Complex Boolean + IN + NOT PQ ANN SMP (dop=2 and dop=4) + External Param
                    bool_q = """
                        SELECT id, grp, flag, score, encode(raw,'hex'), u, j, num
                        FROM t_pq_smp
                        WHERE vec IS NOT NULL
                          AND ((grp = 2 AND flag IN (1, 3) AND NOT (score < 0.0::float8)) OR (grp = 0 AND flag = 0 AND score > 9.0::float8))
                        ORDER BY vec <-> %s::vector(16) LIMIT 5
                    """
                    exact_bool_rows = {
                        r[0]: r for r in q(cm, """
                            SELECT id, grp, flag, score, encode(raw,'hex'), u, j, num
                            FROM exact_pq
                            WHERE vec IS NOT NULL
                              AND ((grp = 2 AND flag IN (1, 3) AND NOT (score < 0.0::float8)) OR (grp = 0 AND flag = 0 AND score > 9.0::float8))
                        """)
                    }
                    for dop in (2, 4):
                        if args.profile == 'release':
                            q(cm, 'SET enable_force_smp = off')
                            q(cm, f'SET query_dop = {dop}')
                            q(cm, 'SET enable_force_smp = on')
                        else:
                            q(cm, f'SET query_dop = {1000 + dop}')
                        exp_b = explain_lines(cm, bool_q, (zero_vec,))
                        assert f'Lake SMP Planned DOP: {dop}' in exp_b, (dop, exp_b)
                        assert 'boolean-filter' in exp_b, (dop, exp_b)
                        assert 'Verified PQ File Coverage: 3' in exp_b, (dop, exp_b)
                        res_b = q(cm, bool_q, (zero_vec,))
                        assert len(res_b) == 5, res_b
                        b_ids = [r[0] for r in res_b]
                        assert 3001 in b_ids and 700 in b_ids, b_ids
                        for row in res_b:
                            assert row[0] in exact_bool_rows and row == exact_bool_rows[row[0]], row

                    # 3c: Cosine metric + Boolean filter at dop=4 (compact 4-file x 64-row table so every file has >=21 filter matches >> LIMIT 5)
                    q(cm, 'SET enable_force_smp = off')
                    q(cm, 'SET query_dop = 1')
                    q(cm, 'CREATE TABLE exact_cos (id int, grp int, flag int, score float8, raw bytea, u uuid, j json, num numeric(12,4), vec vector(16))')
                    q(cm, "CREATE FOREIGN TABLE t_cos_smp (id int, grp int, flag int, score float8, raw bytea, u uuid, j json, num numeric(12,4), vec vector(16)) SERVER cat_main OPTIONS (binding_mode 'create', lake_schema 'main')")
                    for b in range(4):
                        crows = []
                        for i in range(b * 64 + 1, (b + 1) * 64 + 1):
                            v_s = vec_literal(i, 16)
                            gv, fv, sv, ns = i % 3, (i % 2) * 2 + 1, (i % 10) - 2.5, f"{(i - 20) * 0.25:.4f}"
                            if i == 25:
                                v_s = '[' + ','.join(['0.5'] * 16) + ']'
                                gv, fv, sv, ns = 0, 0, 12.0, '55.5000'
                            crows.append(f"({i}, {gv}, {fv}, {sv}, decode('{i:04x}dd','hex'), '33333333-3333-4444-8555-{i:012x}'::uuid, '{{\"c\":{i}}}'::json, {ns}, '{v_s}'::vector(16))")
                        csql = ','.join(crows)
                        q(cm, 'INSERT INTO exact_cos VALUES ' + csql)
                        q(cm, 'INSERT INTO t_cos_smp VALUES ' + csql)
                        q(cm, "SELECT og_lake.flush('t_cos_smp'::regclass)")
                    for tbl in ('exact_cos', 't_cos_smp'):
                        q(cm, f"INSERT INTO {tbl} VALUES (901, 2, 3, 5.0, '\\xcc11'::bytea, 'eeeeeeee-0000-4000-8000-000000000901'::uuid, '{{\"cinline\":true}}'::json, -3.2500, '[0.25,0.25,0.25,0.25,0.25,0.25,0.25,0.25,0.25,0.25,0.25,0.25,0.25,0.25,0.25,0.25]'::vector(16))")
                    ix_raw_cos = s(cm, "SELECT og_lake.create_lake_index('t_cos_smp'::regclass, 'ix_raw_cos'::name, '{\"version\":1,\"kind\":\"vector\",\"method\":\"diskann\",\"column\":\"vec\",\"metric\":\"cosine\",\"enable_pq\":false}'::jsonb)")
                    q(cm, 'SELECT og_lake.build_lake_index(%s::uuid)', (ix_raw_cos,))

                    cos_target = '[' + ','.join(['1'] * 16) + ']'
                    cos_q = f"""
                        SELECT id, num, encode(raw,'hex')
                        FROM t_cos_smp
                        WHERE vec IS NOT NULL AND vector_norm(vec) > 0
                          AND ((grp = 2 AND flag IN (1, 3)) OR (grp = 0 AND score > 9.0::float8))
                        ORDER BY vec <=> '{cos_target}'::vector(16) LIMIT 5
                    """
                    exact_cos_rows = {
                        r[0]: r for r in q(cm, """
                            SELECT id, num, encode(raw,'hex')
                            FROM exact_cos
                            WHERE vec IS NOT NULL AND vector_norm(vec) > 0
                              AND ((grp = 2 AND flag IN (1, 3)) OR (grp = 0 AND score > 9.0::float8))
                        """)
                    }
                    if args.profile == 'release':
                        q(cm, 'SET enable_force_smp = off')
                        q(cm, 'SET query_dop = 4')
                        q(cm, 'SET enable_force_smp = on')
                    else:
                        q(cm, 'SET query_dop = 1004')
                    exp_cos = explain_lines(cm, cos_q)
                    assert 'Lake SMP Planned DOP: 4' in exp_cos and 'raw cosine boolean-filter' in exp_cos, exp_cos
                    assert 'Verified ANN File Coverage: 4' in exp_cos, exp_cos
                    res_cos = q(cm, cos_q)
                    assert len(res_cos) == 5
                    c_ids = [r[0] for r in res_cos]
                    assert 25 in c_ids and 901 in c_ids, c_ids
                    for row in res_cos:
                        assert row[0] in exact_cos_rows and row == exact_cos_rows[row[0]], row

                    # 3d: PQ rebuild_lake_index generation 1 -> 2 still works in SMP dop=4
                    q(cm, 'SET enable_force_smp = off')
                    q(cm, 'SET query_dop = 1')
                    q(cm, 'SET statement_timeout = 900000')
                    q(cm, 'SELECT og_lake.rebuild_lake_index(%s::uuid)', (ix_pq_l2,))
                    q(cm, 'SELECT og_lake.build_lake_index(%s::uuid)', (ix_pq_l2,))
                    q(cm, 'SET statement_timeout = 120000')
                    assert s(cm, 'SELECT definition_generation FROM og_lake_internal.index_definition WHERE index_uuid = %s::uuid', (ix_pq_l2,)) == 2
                    if args.profile == 'release':
                        q(cm, 'SET enable_force_smp = off')
                        q(cm, 'SET query_dop = 4')
                        q(cm, 'SET enable_force_smp = on')
                    else:
                        q(cm, 'SET query_dop = 1004')
                    res_after_rebuild = q(cm, bool_q, (zero_vec,))
                    assert len(res_after_rebuild) == 5 and 3001 in [r[0] for r in res_after_rebuild] and 700 in [r[0] for r in res_after_rebuild]
                    q(cm, 'SET enable_force_smp = off')
                    q(cm, 'SET query_dop = 1')
                    stages['stage3_pq_boolean_ann_smp'] = 24
            finally:
                cm.close()

        for dbn in list(created_dbs):
            q(admin, f'DROP DATABASE {dbn}')
            created_dbs.remove(dbn)
        for rn in list(created_roles):
            q(admin, f'DROP ROLE {rn}')
            created_roles.remove(rn)
        assert q(admin, "SELECT datname FROM pg_database WHERE datname LIKE 'db064_%'") == []
        assert q(admin, "SELECT rolname FROM pg_roles WHERE rolname LIKE 'u064_%'") == []

        res = {
            'status': 'passed',
            'profile': args.profile,
            'ix5_only': args.ix5_only,
            'enable_thread_pool': s(admin, 'SHOW enable_thread_pool'),
            'thread_pool_attr': s(admin, 'SHOW thread_pool_attr'),
            'postgres_ext_version': s(admin, "SELECT extversion FROM pg_extension WHERE extname='og_lake'"),
            'stages': stages,
            'total_checks': sum(stages.values()),
            'duckdb_checks': duckdb_checks,
        }
        Path(args.out).write_text(json.dumps(res, indent=2, ensure_ascii=False) + '\n', encoding='utf-8')
        print(json.dumps(res, ensure_ascii=False))
    finally:
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
            admin.close()
        shutil.rmtree(tmp_root, ignore_errors=True)


if __name__ == '__main__':
    main()
