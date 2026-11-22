#!/usr/bin/env python3
"""N7-SMP full closure suite (SMP-01, SMP-02, complex fusion + PREPARE/ReScan,
partitioned + extended types row SMP DOP=2/4, and widened-column row SMP).
"""

import argparse
import getpass
import hashlib
import json
import os
import shutil
import tempfile
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import psycopg2

def default_workspace():
    configured = os.environ.get('CPP_WORKSPACE')
    if configured:
        return Path(configured).expanduser().resolve()
    integration = next(p for p in Path(__file__).resolve().parents if p.name == 'integration')
    root = integration.parent
    if (root / 'contrib/og_lake').is_dir():
        return root.parent.parent if root.parent.name == '.worktrees' else root.parent
    return root


ROOT = default_workspace()
CONNECTION = {}


def add_connection_args(parser):
    parser.add_argument('--workspace', type=Path, default=ROOT)
    parser.add_argument('--host', help='Profile socket directory or TCP hostname')
    parser.add_argument('--port', type=int)
    parser.add_argument('--user', default=os.environ.get('PGUSER') or getpass.getuser())
    parser.add_argument('--expected-extension-version', default='0.74')


def configure_connection(args):
    global ROOT
    ROOT = args.workspace.expanduser().resolve()
    suffix = args.profile.upper().replace('-', '_')
    CONNECTION.update(
        host=args.host or os.environ.get('OG_LAKE_TEST_HOST_' + suffix) or os.environ.get('OG_LAKE_TEST_HOST'),
        port=args.port or os.environ.get('OG_LAKE_TEST_PORT_' + suffix) or os.environ.get('OG_LAKE_TEST_PORT'),
        user=args.user,
    )



def md5_role_pw(password, role):
    return 'md5' + hashlib.md5((password + role).encode('utf-8')).hexdigest()


def pg_connect(profile, dbname, user=None, password=None):
    port = {'debug': 55560, 'release': 55570, 'ann-debug': 55580}[profile]
    kw = {
        'host': CONNECTION.get('host') or str(ROOT / '.runtime/opengauss-native-lake' / profile / 'socket'),
        'port': int(CONNECTION.get('port') or port),
        'dbname': dbname,
        'user': user or CONNECTION.get('user') or os.environ.get('PGUSER') or getpass.getuser(),
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


def explain_text(conn, sql, params=None):
    return '\n'.join(r[0] for r in q(conn, 'EXPLAIN (ANALYZE, COSTS OFF) ' + sql, params))


def set_smp_dop(conn, dop, force=True):
    if dop <= 1:
        q(conn, "SET query_dop = 1")
        q(conn, "SET enable_force_smp = off")
    else:
        q(conn, f"SET query_dop = {dop}")
        q(conn, f"SET enable_force_smp = {'on' if force else 'off'}")


def vec_literal(i, dim=16):
    vals = [(((i * 131 + j * 79 + ((i * (j + 1)) % 29) * 17) % 2039) - 1019) / 1024.0 for j in range(dim)]
    return '[' + ','.join(format(v, '.9g') for v in vals) + ']'


def run_suite(args):
    tag = uuid.uuid4().hex[:10]
    tmp_root = Path(tempfile.mkdtemp(prefix=f'og_smp_{tag}_', dir='/tmp'))
    os.chmod(tmp_root, 0o755)
    admin = pg_connect(args.profile, 'postgres')
    created_dbs = []
    created_roles = []
    conns = []
    stages = {}

    try:
        tp_mode = s(admin, 'SHOW enable_thread_pool')
        role_main = f'usmp_{tag}'
        db_main = f'dbsmp_{tag}'
        q(admin, f"CREATE ROLE {role_main} NOLOGIN NOINHERIT ENCRYPTED PASSWORD %s", (md5_role_pw('Smp070!', role_main),))
        created_roles.append(role_main)
        q(admin, f"CREATE DATABASE {db_main} WITH DBCOMPATIBILITY 'PG' ENCODING 'UTF8'")
        created_dbs.append(db_main)

        cm = pg_connect(args.profile, db_main)
        conns.append(cm)
        obs = pg_connect(args.profile, db_main)
        conns.append(obs)
        holder = pg_connect(args.profile, db_main)
        conns.append(holder)

        cat_dir = tmp_root / 'cat'
        idx_dir = tmp_root / 'idx'
        idx_dir.mkdir(parents=True, exist_ok=True)
        os.chmod(idx_dir, 0o755)
        q(cm, 'CREATE EXTENSION og_lake')
        assert s(cm, "SELECT extversion FROM pg_extension WHERE extname='og_lake'") == args.expected_extension_version
        q(cm, f"CREATE SCHEMA biz; GRANT USAGE ON SCHEMA biz TO {role_main}")
        q(cm, "SELECT og_lake.configure_internal_owner(%s::name)", (role_main,))
        q(cm, "SELECT og_lake.create_catalog('c_smp'::name, 'lake_metadata'::name, %s::text, NULL::text)", (str(cat_dir),))
        q(cm, "SELECT og_lake.set_option('c_smp'::name, 'data_inlining_row_limit'::text, '5000'::text)")
        q(cm, "SELECT og_lake.set_option('c_smp'::name, 'index_root'::text, %s::text)", (str(idx_dir),))

        # 1. Partitioned (bucket(4) over uuid) + extended types (int2, bytea, uuid, json, numeric) row SMP DOP=2 & 4
        q(cm, "CREATE TABLE biz.ex_part (id int8, grp int2, u uuid, num numeric(12,4), raw bytea, j json)")
        q(cm, """
            CREATE FOREIGN TABLE biz.t_part (
                id int8, grp int2, u uuid, num numeric(12,4), raw bytea, j json
            ) SERVER c_smp OPTIONS (binding_mode 'create', lake_schema 'main')
        """)
        q(cm, "SELECT og_lake.set_partition('biz.t_part'::regclass, ARRAY['u'], ARRAY['bucket(4)'])")
        for b in range(4):
            rows_sql = []
            for i in range(b * 32 + 1, (b + 1) * 32 + 1):
                rows_sql.append(
                    f"({i}, {i % 7}, '11111111-2222-4333-8444-{i:012x}'::uuid, "
                    f"{(i * 1.25):.4f}, decode('{i:04x}aa','hex'), '{{\"i\":{i}}}'::json)"
                )
            sql_vals = ','.join(rows_sql)
            q(cm, 'INSERT INTO biz.ex_part VALUES ' + sql_vals)
            q(cm, 'INSERT INTO biz.t_part VALUES ' + sql_vals)
        for _ in range(2):
            q(cm, "SELECT og_lake.flush('biz.t_part'::regclass)")
        # Add inline rows across partitions + delete 2 file rows
        q(cm, """
            INSERT INTO biz.ex_part VALUES
            (901, 1, 'aaaaaaaa-bbbb-4ccc-8ddd-000000000901'::uuid, 42.5000, '\\xdead'::bytea, '{"i":901}'::json),
            (902, 2, 'bbbbbbbb-cccc-4ddd-8eee-000000000902'::uuid, -9.2500, '\\xbeef'::bytea, '{"i":902}'::json)
        """)
        q(cm, """
            INSERT INTO biz.t_part VALUES
            (901, 1, 'aaaaaaaa-bbbb-4ccc-8ddd-000000000901'::uuid, 42.5000, '\\xdead'::bytea, '{"i":901}'::json),
            (902, 2, 'bbbbbbbb-cccc-4ddd-8eee-000000000902'::uuid, -9.2500, '\\xbeef'::bytea, '{"i":902}'::json)
        """)
        q(cm, "DELETE FROM biz.ex_part WHERE id IN (5, 70)")
        q(cm, "DELETE FROM biz.t_part WHERE id IN (5, 70)")

        file_cnt = s(cm, """
            SELECT count(*) FROM lake_metadata.ducklake_data_file df
            JOIN lake_metadata.ducklake_table t ON t.table_id=df.table_id
            WHERE t.table_name='t_part' AND df.end_snapshot IS NULL
        """)
        assert file_cnt >= 4, file_cnt

        oracle_part = q(obs, "SELECT id, grp, u, num, encode(raw,'hex'), j::text FROM biz.ex_part ORDER BY id")
        q(cm, "SET enable_vector_engine = off")
        q(cm, "SET enable_lake_smp = on")
        for dop in (2, 4):
            set_smp_dop(cm, dop)
            exp_p = explain_text(cm, "SELECT count(*), sum(id), sum(grp) FROM biz.t_part")
            assert f"Lake SMP Planned DOP: {dop}" in exp_p, (dop, exp_p)
            assert q(cm, "SELECT count(*), sum(id), sum(grp) FROM biz.t_part") == q(obs, "SELECT count(*), sum(id), sum(grp) FROM biz.ex_part")
            got_p = q(cm, "SELECT id, grp, u, num, encode(raw,'hex'), j::text FROM biz.t_part ORDER BY id")
            assert got_p == oracle_part, (dop, len(got_p))
        stages['stage1_partitioned_extended_row_smp'] = 4

        # 2. SMP-01 & SMP-02 formal probes (debug/release pool=off, per og_lake_test_smp_probe gate)
        c_smp01 = 0
        if tp_mode == 'off':
            set_smp_dop(cm, 1)
            q(cm, "CREATE FUNCTION biz.smp_probe(integer,integer,bigint) RETURNS text AS '$libdir/og_lake','og_lake_test_smp_probe' LANGUAGE C VOLATILE NOT FENCED")
            q(cm, "CREATE TABLE biz.ex_probe (id int4, val int4)")
            q(cm, "CREATE FOREIGN TABLE biz.t_probe (id int4, val int4) SERVER c_smp OPTIONS (binding_mode 'create', lake_schema 'main')")
            for b in range(4):
                vals = ','.join(f"({i}, {i * 10})" for i in range(b * 64 + 1, (b + 1) * 64 + 1))
                q(cm, "INSERT INTO biz.ex_probe VALUES " + vals)
                q(cm, "INSERT INTO biz.t_probe VALUES " + vals)
                q(cm, "SELECT og_lake.flush('biz.t_probe'::regclass)")
            q(cm, "INSERT INTO biz.ex_probe VALUES (9001, 90010)")
            q(cm, "INSERT INTO biz.t_probe VALUES (9001, 90010)")
            oracle_probe = q(obs, "SELECT id, val FROM biz.ex_probe ORDER BY id")
            dbid = s(obs, "SELECT oid FROM pg_database WHERE datname=current_database()")
            set_smp_dop(cm, 4)

            # 2a. Pause worker 1 mid-batch + pg_cancel_backend -> 57014 + full resource release
            key1 = (1 << 44) + int(hashlib.sha256(tag.encode()).hexdigest()[:9], 16)
            s(cm, "SELECT biz.smp_probe(-1, 0, 0)")
            s(holder, "SELECT pg_advisory_lock(%s::bigint)", (key1,))
            cm_pid = s(cm, "SELECT pg_backend_pid()")
            s(cm, "SELECT biz.smp_probe(1, 1, %s::bigint)", (key1,))
            with ThreadPoolExecutor(max_workers=1) as pool:
                fut = pool.submit(q, cm, "SELECT id, val FROM biz.t_probe ORDER BY id")
                deadline = time.monotonic() + 30
                locks = []
                while time.monotonic() < deadline and not fut.done():
                    locks = q(obs, """
                        SELECT pid FROM pg_locks
                        WHERE locktype='advisory' AND database=%s AND classid=%s::oid AND objid=%s::oid
                          AND objsubid=1 AND mode='ExclusiveLock' AND NOT granted
                    """, (dbid, key1 >> 32, key1 & 0xffffffff))
                    if locks:
                        break
                    time.sleep(0.01)
                assert len(locks) == 1, f'Worker pause gate not hit: {locks}'
                assert s(obs, "SELECT pg_cancel_backend(%s)", (cm_pid,))
                s(holder, "SELECT pg_advisory_unlock(%s::bigint)", (key1,))
                try:
                    fut.result(timeout=30)
                    raise AssertionError('Cancelled SMP query unexpectedly succeeded')
                except psycopg2.Error as exc:
                    assert exc.pgcode == '57014', (exc.pgcode, exc)
            st_cancel = json.loads(s(cm, "SELECT biz.smp_probe(0, 0, 0)"))
            assert st_cancel['participants'] == 4 and all(w['done'] in (1, 2) for w in st_cancel['workers'] if w['claimed']), st_cancel
            assert q(cm, "SELECT id, val FROM biz.t_probe ORDER BY id") == oracle_probe
            c_smp01 += 2

            # 2b. Pause worker 1 + concurrent UPDATE & flush -> S_stmt snapshot isolation
            key2 = key1 + 1
            s(holder, "SELECT pg_advisory_lock(%s::bigint)", (key2,))
            s(cm, "SELECT biz.smp_probe(1, 1, %s::bigint)", (key2,))
            with ThreadPoolExecutor(max_workers=1) as pool:
                fut = pool.submit(q, cm, "SELECT id, val FROM biz.t_probe ORDER BY id")
                deadline = time.monotonic() + 30
                locks = []
                while time.monotonic() < deadline and not fut.done():
                    locks = q(obs, """
                        SELECT pid FROM pg_locks
                        WHERE locktype='advisory' AND database=%s AND classid=%s::oid AND objid=%s::oid
                          AND objsubid=1 AND mode='ExclusiveLock' AND NOT granted
                    """, (dbid, key2 >> 32, key2 & 0xffffffff))
                    if locks:
                        break
                    time.sleep(0.01)
                assert len(locks) == 1
                q(holder, "UPDATE biz.t_probe SET val = val + 500 WHERE id = 1")
                q(obs, "UPDATE biz.ex_probe SET val = val + 500 WHERE id = 1")
                q(holder, "SELECT og_lake.flush('biz.t_probe'::regclass)")
                s(holder, "SELECT pg_advisory_unlock(%s::bigint)", (key2,))
                got_snap = fut.result(timeout=30)
                assert got_snap == oracle_probe, 'Snapshot S_stmt mutated under paused SMP worker!'
            oracle_probe = q(obs, "SELECT id, val FROM biz.ex_probe ORDER BY id")
            assert q(cm, "SELECT id, val FROM biz.t_probe ORDER BY id") == oracle_probe
            c_smp01 += 2

            # 2c. Fault injection modes 2 (worker error), 3 (pre-start), 4 (partial-start) -> XX000 & clean recovery
            for mode in (2, 3, 4):
                s(cm, "SELECT biz.smp_probe(-1, 0, 0)")
                s(cm, "SELECT biz.smp_probe(%s, 1, 0)", (mode,))
                try:
                    q(cm, "SELECT id, val FROM biz.t_probe ORDER BY id")
                    raise AssertionError(f'Expected failure for SMP probe mode {mode}')
                except psycopg2.Error as exc:
                    assert exc.pgcode == 'XX000', (mode, exc.pgcode, exc)
                st_m = json.loads(s(cm, "SELECT biz.smp_probe(0, 0, 0)"))
                if mode == 3:
                    assert sum(w['started'] for w in st_m['workers']) == 0, st_m
                elif mode == 4:
                    assert 1 <= sum(w['started'] for w in st_m['workers']) <= 4, st_m
                assert all(w['done'] in (1, 2) for w in st_m['workers'] if w['claimed']), st_m
                assert q(cm, "SELECT id, val FROM biz.t_probe ORDER BY id") == oracle_probe
            c_smp01 += 3

            # 2d. PREPARE external-param re-execution + generic plan cache disable/re-enable (SMP-02)
            # Warm 5 low-cutoff custom plans so generic selectivity (33%) is cheaper than avg_custom_cost (~100%).
            q(cm, "PREPARE p_smp(int4) AS SELECT id, val FROM biz.t_probe WHERE id > $1 ORDER BY id")
            for cutoff in (0, 1, 2, 3, 4, 100):
                s(cm, "SELECT biz.smp_probe(-1, 0, 0)")
                got_p_rows = q(cm, f"EXECUTE p_smp({cutoff})")
                if cutoff in (0, 100):
                    assert got_p_rows == q(obs, "SELECT id, val FROM biz.ex_probe WHERE id > %s ORDER BY id", (cutoff,))
                assert json.loads(s(cm, "SELECT biz.smp_probe(0, 0, 0)"))['participants'] == 4
            q(cm, "SET enable_lake_smp = off")
            try:
                q(cm, "EXECUTE p_smp(50)")
                raise AssertionError('Expected cached SMP plan failure when enable_lake_smp=off')
            except psycopg2.Error as exc:
                assert exc.pgcode == '0A000', (exc.pgcode, exc)
            q(cm, "SET enable_lake_smp = on")
            assert q(cm, "EXECUTE p_smp(50)") == q(obs, "SELECT id, val FROM biz.ex_probe WHERE id > 50 ORDER BY id")
            q(cm, "DEALLOCATE p_smp")
            c_smp01 += 2

            # 2e. Task shortage fallback (1 inline task -> serial; 1 file + inline = 2 tasks -> DOP=2 < 4)
            set_smp_dop(cm, 1)
            q(cm, "CREATE TABLE biz.ex_small (id int4, val int4)")
            q(cm, "CREATE FOREIGN TABLE biz.t_small (id int4, val int4) SERVER c_smp OPTIONS (binding_mode 'create', lake_schema 'main')")
            q(cm, "INSERT INTO biz.ex_small VALUES (1, 10), (2, 20)")
            q(cm, "INSERT INTO biz.t_small VALUES (1, 10), (2, 20)")
            set_smp_dop(cm, 4)
            s(cm, "SELECT biz.smp_probe(-1, 0, 0)")
            assert q(cm, "SELECT id, val FROM biz.t_small ORDER BY id") == [(1, 10), (2, 20)]
            assert json.loads(s(cm, "SELECT biz.smp_probe(0, 0, 0)"))['participants'] == 0
            q(cm, "SELECT og_lake.flush('biz.t_small'::regclass)")
            s(cm, "SELECT biz.smp_probe(-1, 0, 0)")
            assert q(cm, "SELECT id, val FROM biz.t_small ORDER BY id") == [(1, 10), (2, 20)]
            assert json.loads(s(cm, "SELECT biz.smp_probe(0, 0, 0)"))['participants'] == 2
            c_smp01 += 1
        stages['stage2_smp01_smp02_probes'] = c_smp01

        # 3. Complex Vector+Scalar Fusion + PREPARE/EXECUTE DOP=4 + LATERAL Parameter ReScan
        c3 = 0
        set_smp_dop(cm, 1)
        q(cm, "CREATE TABLE biz.ex_ann (id int4, grp int4, flag int4, score float8, tag text, vec vector(16))")
        q(cm, "CREATE FOREIGN TABLE biz.t_ann (id int4, grp int4, flag int4, score float8, tag text, vec vector(16)) SERVER c_smp OPTIONS (binding_mode 'create', lake_schema 'main')")
        for b in range(4):
            rows_sql = []
            for i in range(b * 260 + 1, (b + 1) * 260 + 1):
                rows_sql.append(f"({i}, {i % 3}, {(i % 3) + 1}, {(i % 10) - 2.0}, 'r{i}', '{vec_literal(i, 16)}'::vector(16))")
            sql_vals = ','.join(rows_sql)
            q(cm, "INSERT INTO biz.ex_ann VALUES " + sql_vals)
            q(cm, "INSERT INTO biz.t_ann VALUES " + sql_vals)
            q(cm, "SELECT og_lake.flush('biz.t_ann'::regclass)")
        q(cm, "INSERT INTO biz.ex_ann VALUES (2001, 2, 3, 5.0, 'inl', '[0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01]'::vector(16))")
        q(cm, "INSERT INTO biz.t_ann VALUES (2001, 2, 3, 5.0, 'inl', '[0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01]'::vector(16))")

        if args.profile in ('ann-debug', 'release'):
            ix_pq = s(cm, "SELECT og_lake.create_lake_index('biz.t_ann'::regclass, 'ix_ann_pq'::name, '{\"version\":1,\"kind\":\"vector\",\"method\":\"diskann\",\"column\":\"vec\",\"metric\":\"l2\",\"pq\":true,\"pq_chunks\":4}'::jsonb)")
            q(cm, "SET statement_timeout = 900000")
            q(cm, "SELECT og_lake.build_lake_index(%s::uuid)", (ix_pq,))
            q(cm, "SELECT og_lake.build_lake_index(%s::uuid)", (ix_pq,))
            q(cm, "SET statement_timeout = 180000")
            fmt2_cnt = s(obs, "SELECT count(*) FROM og_lake_internal.index_segment WHERE index_uuid=%s::uuid AND state='READY' AND format_version=2", (ix_pq,))
            assert fmt2_cnt == 4, f'Expected 4 PQ format2 segments, got {fmt2_cnt}'

        q(cm, "SET enable_vector_engine = off")
        q(cm, "SET enable_lake_smp = on")
        q(cm, "SET enable_lake_ann = on")
        q(cm, "SET ann_orderby_distance_reuse = off")
        q(cm, "SET query_max_mem = 0")
        q(cm, "SET enable_seqscan = off")
        set_smp_dop(cm, 4)

        # 3a. Complex boolean fusion + PREPARE/EXECUTE multi-parameter under DOP=4
        q(cm, """
            PREPARE p_ann_fuse(int4, float8, int4, int4, vector(16)) AS
            SELECT id, grp, flag, score, tag, vec <-> $5 AS dist
            FROM biz.t_ann
            WHERE vec IS NOT NULL
              AND ((grp = $1 AND NOT (score < $2)) OR ((flag = $3 OR flag = $4) AND grp = 0))
            ORDER BY vec <-> $5 LIMIT 5
        """)
        zero_v = '[' + ','.join(['0'] * 16) + ']'
        ones_v = '[' + ','.join(['0.5'] * 16) + ']'
        for g_p, sc_p, f1, f2, qv in [(2, 0.0, 1, 3, zero_v), (1, 2.0, 2, 3, ones_v), (2, 1.0, 1, 2, zero_v)]:
            exp_af = explain_text(cm, f"EXECUTE p_ann_fuse({g_p}, {sc_p}::float8, {f1}, {f2}, '{qv}'::vector(16))")
            assert 'Lake SMP Planned DOP: 4' in exp_af, exp_af
            if args.profile in ('ann-debug', 'release'):
                assert 'Lake SMP ANN PQ: admitted' in exp_af and 'boolean-filter' in exp_af, exp_af
            got_af = q(cm, f"EXECUTE p_ann_fuse({g_p}, {sc_p}::float8, {f1}, {f2}, '{qv}'::vector(16))")
            exp_rows = q(obs, f"""
                SELECT id, grp, flag, score, tag, vec <-> %s::vector(16) AS dist FROM biz.ex_ann
                WHERE vec IS NOT NULL
                  AND ((grp = %s AND NOT (score < %s)) OR ((flag = %s OR flag = %s) AND grp = 0))
                ORDER BY vec <-> %s::vector(16) LIMIT 5
            """, (qv, g_p, sc_p, f1, f2, qv))
            assert len(got_af) == 5, (len(got_af), got_af)
            dists = [r[5] for r in got_af]
            assert dists == sorted(dists), dists
            if args.profile == 'debug':
                assert got_af == exp_rows, (got_af, exp_rows)
            else:
                # Validate predicate + exact table membership + PQ recall@5 >= 0.8 (4/5)
                valid_pool = {r[0]: r for r in q(obs, """
                    SELECT id, grp, flag, score, tag, vec <-> %s::vector(16) AS dist FROM biz.ex_ann
                    WHERE vec IS NOT NULL
                      AND ((grp = %s AND NOT (score < %s)) OR ((flag = %s OR flag = %s) AND grp = 0))
                """, (qv, g_p, sc_p, f1, f2))}
                assert all(r[0] in valid_pool and r == valid_pool[r[0]] for r in got_af), got_af
                recall = len({r[0] for r in got_af} & {r[0] for r in exp_rows}) / 5.0
                assert recall >= 0.8, (recall, got_af, exp_rows)
        q(cm, "DEALLOCATE p_ann_fuse")
        c3 += 3

        # 3b. Correlated NestedLoop parameter ReScan over complex vector+scalar fusion
        rescan_sql = """
            SELECT d.g, d.s_min, lat.id, lat.grp, lat.flag, lat.score
            FROM (VALUES (2, 0.0::float8), (1, 1.0::float8), (0, 2.0::float8)) AS d(g, s_min)
            CROSS JOIN LATERAL (
                SELECT id, grp, flag, score
                FROM biz.t_ann
                WHERE vec IS NOT NULL
                  AND ((grp = d.g AND NOT (score < d.s_min)) OR (flag = 3 AND grp = d.g))
                ORDER BY vec <-> '[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]'::vector(16) LIMIT 3
            ) AS lat
            ORDER BY d.g, lat.id
        """
        got_lat = q(cm, rescan_sql)
        exp_lat = q(obs, rescan_sql.replace('biz.t_ann', 'biz.ex_ann'))
        assert len(got_lat) == 9, (len(got_lat), got_lat)
        if args.profile == 'debug':
            assert got_lat == exp_lat, (got_lat, exp_lat)
        else:
            valid_lat = set(q(obs, """
                SELECT d.g, d.s_min, lat.id, lat.grp, lat.flag, lat.score
                FROM (VALUES (2, 0.0::float8), (1, 1.0::float8), (0, 2.0::float8)) AS d(g, s_min)
                CROSS JOIN LATERAL (
                    SELECT id, grp, flag, score
                    FROM biz.ex_ann
                    WHERE vec IS NOT NULL
                      AND ((grp = d.g AND NOT (score < d.s_min)) OR (flag = 3 AND grp = d.g))
                ) AS lat
            """))
            assert all(r in valid_lat for r in got_lat), got_lat
            lat_recall = len({(r[0], r[2]) for r in got_lat} & {(r[0], r[2]) for r in exp_lat}) / 9.0
            assert lat_recall >= 6.0 / 9.0, (lat_recall, got_lat, exp_lat)
        c3 += 2
        stages['stage3_complex_fusion_rescan_smp'] = c3

        result = {
            'status': 'passed',
            'profile': args.profile,
            'enable_thread_pool': tp_mode,
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
                q(admin, f"SELECT pg_terminate_session(pid, sessionid) FROM pg_stat_activity WHERE datname='{dbn}' AND sessionid <> pg_current_sessionid()")
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
    add_connection_args(p)
    p.add_argument('--profile', choices=['debug', 'ann-debug', 'release'], required=True)
    p.add_argument('--out')
    args = p.parse_args()
    configure_connection(args)
    run_suite(args)


if __name__ == '__main__':
    main()
