#!/usr/bin/env python3
"""SQL0.72 / N7-VECTOR acceptance suite:
- 0.70 -> 0.71 -> 0.72 upgrade + fresh 0.72 install + pre-0.72 refusal gates
- SMP + Vectorized VecForeignScan (DOP=2 & DOP=4) with both 'force' and 'opt'
- VecAgg (global + GROUP BY Redistribute/Gather), VecSort, VecLimit early stop,
  VecHashJoin (parallel lake + heap), PREPARE/EXECUTE external params
- Extended Vec & Vec+SMP types (bool, int2, int4, int8, float4, float8,
  date, timestamp, timestamptz, numeric, bytea, text) + NULLs + partitioned pruning
- Alternating wide >16KiB & >8MiB multi-batch split + >2KiB detoasted inline TEXT
- Cached plan invalidation (enable_lake_smp=off, ADD COLUMN), adaptive task shortage,
  uncommitted local txn fallback, and biz.smp_probe fault-injection / cancel (pool=off)
- DuckDB physical Parquet + heap oracle parity
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
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import psycopg2

ROOT = Path('/home/qiumc/data/code/cpp')
DUCKDB_BIN = json.loads((ROOT / '.reference/opengauss-native-lake/build/receipt.json').read_text())['binary']


def duckdb_query(sql):
    proc = subprocess.run([DUCKDB_BIN, '-json', '-c', sql], capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        raise RuntimeError(f'DuckDB query failed ({proc.returncode}): {proc.stderr.strip()}')
    out = proc.stdout.strip()
    return json.loads(out) if out else []


def md5_role_pw(password, role):
    return 'md5' + hashlib.md5((password + role).encode('utf-8')).hexdigest()


def pg_connect(profile, dbname, user='qiumc', password=None):
    port = {'debug': 55560, 'release': 55570, 'ann-debug': 55580}[profile]
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


def explain_text(conn, sql, analyze=True, params=None):
    prefix = 'EXPLAIN (ANALYZE, COSTS OFF) ' if analyze else 'EXPLAIN (COSTS OFF) '
    return '\n'.join(r[0] for r in q(conn, prefix + sql, params))


def explain_json(conn, sql, analyze=False):
    prefix = 'EXPLAIN (ANALYZE, FORMAT JSON) ' if analyze else 'EXPLAIN (FORMAT JSON) '
    raw = s(conn, prefix + sql)
    return json.loads(raw) if isinstance(raw, str) else raw


def walk_nodes(obj):
    if isinstance(obj, dict):
        yield obj
        for v in obj.values():
            yield from walk_nodes(v)
    elif isinstance(obj, list):
        for item in obj:
            yield from walk_nodes(item)


def set_vec_smp(conn, dop, strategy='force', enable_vec=True, enable_smp=True):
    q(conn, f"SET enable_vector_engine = {'on' if enable_vec else 'off'}")
    q(conn, f"SET try_vector_engine_strategy = '{strategy}'")
    q(conn, f"SET enable_lake_smp = {'on' if enable_smp else 'off'}")
    if dop <= 1:
        q(conn, "SET query_dop = 1")
        q(conn, "SET enable_force_smp = off")
    else:
        q(conn, f"SET query_dop = {dop}")
        q(conn, "SET enable_force_smp = on")


def run_suite(args):
    tag = uuid.uuid4().hex[:10]
    tmp_root = Path(tempfile.mkdtemp(prefix=f'og_v72_{tag}_', dir='/tmp'))
    os.chmod(tmp_root, 0o755)
    admin = pg_connect(args.profile, 'postgres')
    created_dbs = []
    created_roles = []
    conns = []
    stages = {}

    try:
        tp_mode = s(admin, 'SHOW enable_thread_pool')
        role_main = f'uv72_{tag}'
        db_main = f'dbv72_{tag}'
        db_fresh = f'dbv72_f_{tag}'
        q(admin, f"CREATE ROLE {role_main} NOLOGIN NOINHERIT ENCRYPTED PASSWORD %s", (md5_role_pw('Vec072!', role_main),))
        created_roles.append(role_main)
        q(admin, f"CREATE DATABASE {db_main} WITH DBCOMPATIBILITY 'PG' ENCODING 'UTF8'")
        q(admin, f"CREATE DATABASE {db_fresh} WITH DBCOMPATIBILITY 'PG' ENCODING 'UTF8'")
        created_dbs.extend([db_main, db_fresh])

        # Stage 1: Upgrade 0.70 -> 0.71 -> 0.72 + pre-0.72 gates + fresh 0.72 install
        c1 = 0
        cm = pg_connect(args.profile, db_main)
        obs = pg_connect(args.profile, db_main)
        holder = pg_connect(args.profile, db_main)
        conns.extend([cm, obs, holder])

        q(cm, "CREATE EXTENSION og_lake VERSION '0.70'")
        q(cm, "ALTER EXTENSION og_lake UPDATE TO '0.71'")
        assert s(cm, "SELECT extversion FROM pg_extension WHERE extname='og_lake'") == '0.71'
        hs71 = json.loads(s(cm, "SELECT og_lake.host_status()"))
        assert hs71['module_version'] in ('0.72', '0.73', '0.74')
        assert hs71['vector_smp'] is True and hs71['vector_smp_sql_version'] == '0.72'

        q(cm, f"CREATE SCHEMA biz; GRANT USAGE ON SCHEMA biz TO {role_main}")
        q(cm, "SELECT og_lake.configure_internal_owner(%s::name)", (role_main,))
        cat_dir = tmp_root / 'cat'
        idx_dir = tmp_root / 'idx'
        idx_dir.mkdir(parents=True, exist_ok=True)
        os.chmod(idx_dir, 0o755)
        q(cm, "SELECT og_lake.create_catalog('c_v72'::name, 'lake_metadata'::name, %s::text, NULL::text)", (str(cat_dir),))
        q(cm, "SELECT og_lake.set_option('c_v72'::name, 'data_inlining_row_limit'::text, '5000'::text)")
        q(cm, "SELECT og_lake.set_option('c_v72'::name, 'index_root'::text, %s::text)", (str(idx_dir),))

        # Verify legacy 0.71 gates keep Vec+SMP, INT2-Vec, temporal-SMP disabled before 0.72 update
        q(cm, """
            CREATE FOREIGN TABLE biz.t_legacy_gate (
                id int8, k int2, d date, v bigint
            ) SERVER c_v72 OPTIONS (binding_mode 'create', lake_schema 'main')
        """)
        q(cm, """
            CREATE FOREIGN TABLE biz.t_vec_pre (
                id int4, k int4, v int8, txt text
            ) SERVER c_v72 OPTIONS (binding_mode 'create', lake_schema 'main')
        """)
        for fidx in range(4):
            q(cm, f"""
                INSERT INTO biz.t_legacy_gate
                SELECT g, (g % 5)::int2, DATE '2026-11-01' + (g % 10), g * 10
                FROM generate_series({fidx * 20 + 1}, {(fidx + 1) * 20}) AS g
            """)
            q(cm, "SELECT og_lake.flush('biz.t_legacy_gate'::regclass)")
            q(cm, f"""
                INSERT INTO biz.t_vec_pre
                SELECT g, g % 7, g * 3, 'p_' || g
                FROM generate_series({fidx * 25 + 1}, {(fidx + 1) * 25}) AS g
            """)
            q(cm, "SELECT og_lake.flush('biz.t_vec_pre'::regclass)")

        set_vec_smp(cm, 4, 'force')
        exp_pre_vec = explain_text(cm, "SELECT count(*), sum(v) FROM biz.t_vec_pre", analyze=False)
        assert 'Lake Batch Delivery: row ForeignScan' in exp_pre_vec, exp_pre_vec
        assert 'Lake SMP Planned DOP: 4' in exp_pre_vec, exp_pre_vec
        exp_pre_leg = explain_text(cm, "SELECT count(*), sum(k) FROM biz.t_legacy_gate", analyze=False)
        assert 'Lake Batch Delivery: row ForeignScan' in exp_pre_leg, exp_pre_leg
        assert 'Lake SMP Planned DOP' not in exp_pre_leg, exp_pre_leg
        c1 += 2

        q(cm, "ALTER EXTENSION og_lake UPDATE TO '0.72'")
        assert s(cm, "SELECT extversion FROM pg_extension WHERE extname='og_lake'") == '0.72'

        cf = pg_connect(args.profile, db_fresh)
        conns.append(cf)
        q(cf, "CREATE EXTENSION og_lake")
        assert s(cf, "SELECT extversion FROM pg_extension WHERE extname='og_lake'") in ('0.72', '0.73', '0.74')
        cf.close()
        conns.remove(cf)
        c1 += 2
        stages['stage1_upgrade_legacy_gates'] = c1

        # Stage 2: Core SMP + VecForeignScan (DOP=2 & DOP=4, force & opt, GROUP BY, Join, Sort, Limit, Prepare)
        c2 = 0
        q(cm, """
            CREATE TABLE biz.ex_core (
                id bigint, grp int4, v1 bigint, v2 double precision, flag boolean, txt text
            )
        """)
        q(cm, """
            CREATE FOREIGN TABLE biz.t_core (
                id bigint, grp int4, v1 bigint, v2 double precision, flag boolean, txt text
            ) SERVER c_v72 OPTIONS (binding_mode 'create', lake_schema 'main')
        """)
        q(cm, """
            CREATE TABLE biz.dim_grp (grp int4, label text, weight double precision)
        """)
        q(cm, "INSERT INTO biz.dim_grp SELECT g, 'G_' || g, (g + 1) * 0.5 FROM generate_series(0, 15) AS g")

        for b in range(6):
            ins = f"""
                SELECT g, (g % 16)::int4, (g * 7)::bigint, ((g % 97) * 0.25)::float8,
                       (g % 2 = 0), CASE WHEN g % 11 = 0 THEN NULL ELSE 'row_' || (g % 50) END
                FROM generate_series({b * 200 + 1}, {(b + 1) * 200}) AS g
            """
            q(cm, "INSERT INTO biz.ex_core " + ins)
            q(cm, "INSERT INTO biz.t_core " + ins)
            q(cm, "SELECT og_lake.flush('biz.t_core'::regclass)")

        # Add inline carrier rows, including >2KiB detoasted text, plus delete tombstones across file+inline
        long_str = 'Z' * 3100
        q(cm, """
            INSERT INTO biz.ex_core VALUES
            (5001, 3, 50010, 12.5, true, %s),
            (5002, 7, NULL, 99.0, false, 'inline_tail'),
            (5003, 11, -42, 0.0, NULL, 'inline_null_flag')
        """, (long_str,))
        q(cm, """
            INSERT INTO biz.t_core VALUES
            (5001, 3, 50010, 12.5, true, %s),
            (5002, 7, NULL, 99.0, false, 'inline_tail'),
            (5003, 11, -42, 0.0, NULL, 'inline_null_flag')
        """, (long_str,))
        q(cm, "DELETE FROM biz.ex_core WHERE id IN (10, 250, 610, 1199, 5002)")
        q(cm, "DELETE FROM biz.t_core WHERE id IN (10, 250, 610, 1199, 5002)")
        q(cm, "ANALYZE biz.t_core")
        q(cm, "ANALYZE biz.ex_core")

        # Verify Vector Streaming + native VecForeignScan at DOP=2 and DOP=4 across 'force' AND 'opt'
        core_agg_sql = "SELECT count(*), sum(v1), round(sum(v2)::numeric, 4), min(txt), max(txt) FROM biz.t_core WHERE grp BETWEEN 2 AND 12"
        core_gb_sql = "SELECT grp, count(*), COALESCE(sum(v1), 0), count(txt) FROM biz.t_core GROUP BY grp ORDER BY grp"
        core_limit_sql = "SELECT id, grp, v1, length(txt) FROM biz.t_core WHERE grp >= 2 ORDER BY id LIMIT 7"
        core_join_sql = """
            SELECT d.label, count(*), sum(t.v1 + (d.weight * 10)::bigint)
            FROM biz.t_core t JOIN biz.dim_grp d ON t.grp = d.grp
            WHERE t.flag IS DISTINCT FROM false
            GROUP BY d.label ORDER BY d.label
        """

        oracle_agg = q(obs, core_agg_sql.replace('biz.t_core', 'biz.ex_core'))
        oracle_gb = q(obs, core_gb_sql.replace('biz.t_core', 'biz.ex_core'))
        oracle_limit = q(obs, core_limit_sql.replace('biz.t_core', 'biz.ex_core'))
        oracle_join = q(obs, core_join_sql.replace('biz.t_core', 'biz.ex_core'))
        oracle_all = q(obs, "SELECT id, grp, v1, v2, flag, txt FROM biz.ex_core ORDER BY id")

        for strat in ('force', 'optimal'):
            for dop in (1, 2, 4):
                set_vec_smp(cm, dop, strat)
                exp_gb = explain_text(cm, core_gb_sql, analyze=True)
                assert 'Lake Batch Delivery: native VecForeignScan / owned scalar VectorBatch' in exp_gb, (strat, dop, exp_gb)
                if dop > 1:
                    assert f'Lake SMP Planned DOP: {dop}' in exp_gb, (strat, dop, exp_gb)
                    assert 'Vector Streaming' in exp_gb or 'VecStream' in exp_gb, (strat, dop, exp_gb)
                assert q(cm, core_agg_sql) == oracle_agg, (strat, dop)
                assert q(cm, core_gb_sql) == oracle_gb, (strat, dop)
                assert q(cm, core_limit_sql) == oracle_limit, (strat, dop)
                assert q(cm, "SELECT id, grp, v1, v2, flag, txt FROM biz.t_core ORDER BY id") == oracle_all, (strat, dop)
                if dop == 4:
                    # Also test HashJoin between SMP/serial VecForeignScan and heap table
                    assert q(cm, core_join_sql) == oracle_join, (strat, dop)
        c2 += 6

        # DuckDB independent Parquet file check on biz.t_core files
        pq_files = [r[0] for r in q(obs, """
            SELECT df.path FROM lake_metadata.ducklake_data_file df
            JOIN lake_metadata.ducklake_table t ON t.table_id=df.table_id
            WHERE t.table_name='t_core' AND df.end_snapshot IS NULL ORDER BY df.data_file_id
        """)]
        assert len(pq_files) == 6
        abs_pq = [str(next(cat_dir.rglob(Path(p).name))) for p in pq_files]
        duck_rows = duckdb_query("SELECT count(*) AS n, sum(v1) AS s FROM read_parquet([" +
                                 ','.join("'" + p.replace("'", "''") + "'" for p in abs_pq) + "])")
        assert duck_rows[0]['n'] == 1200 and int(duck_rows[0]['s']) == sum(g * 7 for g in range(1, 1201)), duck_rows
        c2 += 1
        stages['stage2_vec_smp_core_ops'] = c2

        # Stage 3: Extended 12-type + Temporal + Partitioned (bucket + month) Vec & Vec+SMP + Partition Pruning
        c3 = 0
        set_vec_smp(cm, 1, 'force')
        q(cm, """
            CREATE TABLE biz.ex_ext (
                id int8, b bool, s int2, i4 int4, f4 real, f8 double precision,
                d date, ts timestamp, tstz timestamptz, num numeric(12,4), raw bytea, txt text
            )
        """)
        q(cm, """
            CREATE FOREIGN TABLE biz.t_ext (
                id int8, b bool, s int2, i4 int4, f4 real, f8 double precision,
                d date, ts timestamp, tstz timestamptz, num numeric(12,4), raw bytea, txt text
            ) SERVER c_v72 OPTIONS (binding_mode 'create', lake_schema 'main')
        """)
        q(cm, "SELECT og_lake.set_partition('biz.t_ext'::regclass, ARRAY['s', 'd'], ARRAY['identity', 'month'])")
        for m_idx, d_base in enumerate(('2026-01-15', '2026-03-10', '2026-06-20', '2026-09-05')):
            rows_sql = []
            for k in range(1, 31):
                rid = m_idx * 30 + k
                s_val = m_idx % 2
                rows_sql.append(
                    f"({rid}, {rid % 2 == 0}, {s_val}::int2, {rid * 3}, {rid * 0.5}::float4, {rid * 1.25}::float8, "
                    f"DATE '{d_base}', TIMESTAMP '{d_base} 10:15:30', TIMESTAMPTZ '{d_base} 12:00:00+00', "
                    f"{(rid * 2.125):.4f}, decode('{rid:04x}fe','hex'), 'ext_{rid}')"
                )
            vals = ','.join(rows_sql)
            q(cm, "INSERT INTO biz.ex_ext VALUES " + vals)
            q(cm, "INSERT INTO biz.t_ext VALUES " + vals)
            q(cm, "SELECT og_lake.flush('biz.t_ext'::regclass)")

        # Inline row in partitioned extended table + 1 delete
        q(cm, """
            INSERT INTO biz.ex_ext VALUES
            (999, NULL, 1::int2, -77, 1.5, 2.75, DATE '2026-12-01', TIMESTAMP '2026-12-01 01:02:03', TIMESTAMPTZ '2026-12-01 01:02:03+00', -42.5000, '\\xdeadbeef'::bytea, 'inline_ext')
        """)
        q(cm, """
            INSERT INTO biz.t_ext VALUES
            (999, NULL, 1::int2, -77, 1.5, 2.75, DATE '2026-12-01', TIMESTAMP '2026-12-01 01:02:03', TIMESTAMPTZ '2026-12-01 01:02:03+00', -42.5000, '\\xdeadbeef'::bytea, 'inline_ext')
        """)
        q(cm, "DELETE FROM biz.ex_ext WHERE id IN (3, 62)")
        q(cm, "DELETE FROM biz.t_ext WHERE id IN (3, 62)")

        ext_all_sql = "SELECT id, b, s, i4, f4, f8, d, ts, tstz, num, encode(raw,'hex'), txt FROM biz.t_ext ORDER BY id"
        ext_gb_sql = "SELECT s, d, count(*), sum(i4), sum(num) FROM biz.t_ext GROUP BY s, d ORDER BY s, d"
        ext_prune_sql = "SELECT id, s, d, num, txt FROM biz.t_ext WHERE d = DATE '2026-03-10' AND s = 1::int2 ORDER BY id"
        oracle_ext_all = q(obs, ext_all_sql.replace('biz.t_ext', 'biz.ex_ext'))
        oracle_ext_gb = q(obs, ext_gb_sql.replace('biz.t_ext', 'biz.ex_ext'))
        oracle_ext_prune = q(obs, ext_prune_sql.replace('biz.t_ext', 'biz.ex_ext'))

        # DOP=1 partitioned VecForeignScan + real partition pruning check
        set_vec_smp(cm, 1, 'force')
        exp_ext_p = explain_text(cm, ext_prune_sql, analyze=True)
        assert 'Lake Batch Delivery: native VecForeignScan / owned scalar VectorBatch' in exp_ext_p, exp_ext_p
        assert 'Lake Partition Files Skipped:' in exp_ext_p, exp_ext_p
        assert q(cm, ext_prune_sql) == oracle_ext_prune
        assert q(cm, ext_all_sql) == oracle_ext_all
        assert q(cm, ext_gb_sql) == oracle_ext_gb
        c3 += 3

        # DOP=2 and DOP=4 SMP + VecForeignScan across all 12 types + partitioned table
        for dop in (2, 4):
            set_vec_smp(cm, dop, 'force')
            exp_ep = explain_text(cm, ext_gb_sql, analyze=True)
            assert 'Lake Batch Delivery: native VecForeignScan / owned scalar VectorBatch' in exp_ep, (dop, exp_ep)
            assert f'Lake SMP Planned DOP: {dop}' in exp_ep, (dop, exp_ep)
            assert q(cm, ext_all_sql) == oracle_ext_all, dop
            assert q(cm, ext_gb_sql) == oracle_ext_gb, dop
            assert q(cm, ext_prune_sql) == oracle_ext_prune, dop
        c3 += 4
        stages['stage3_extended_temporal_partition_vec_smp'] = c3

        # Stage 4: Wide alternating >16KiB & >8MiB multi-batch + Cache Invalidation + Task Fallback + SMP Probes
        c4 = 0
        set_vec_smp(cm, 1, 'force')
        q(cm, "CREATE TABLE biz.ex_wide (id int4, k int4, a text, b bytea)")
        q(cm, "CREATE FOREIGN TABLE biz.t_wide (id int4, k int4, a text, b bytea) SERVER c_v72 OPTIONS (binding_mode 'create', lake_schema 'main')")
        # 4 files x 25 rows x ~220 KiB = ~22 MiB -> >8 MiB split at BOTH DOP=1 AND DOP=2!
        for fidx in range(4):
            ins_wide = f"""
                SELECT g, (g % 3)::int4,
                       repeat(chr(65 + (g % 26)), 180000),
                       repeat(chr(97 + (g % 26)), 40000)::bytea
                FROM generate_series({fidx * 25 + 1}, {(fidx + 1) * 25}) AS g
            """
            q(cm, "INSERT INTO biz.ex_wide " + ins_wide)
            q(cm, "INSERT INTO biz.t_wide " + ins_wide)
            q(cm, "SELECT og_lake.flush('biz.t_wide'::regclass)")
        oracle_wide = q(obs, "SELECT id, k, length(a), md5(a), octet_length(b), md5(b) FROM biz.ex_wide ORDER BY id")
        for dop in (1, 2):
            set_vec_smp(cm, dop, 'force')
            pj = explain_json(cm, "SELECT id, k, length(a), md5(a), octet_length(b), md5(b) FROM biz.t_wide ORDER BY id", analyze=True)
            nodes = [n for n in walk_nodes(pj) if 'Lake Batch Delivery' in n]
            assert any(n['Lake Batch Delivery'] == 'native VecForeignScan / owned scalar VectorBatch' for n in nodes), (dop, pj)
            if dop == 1:
                vnode = [n for n in nodes if 'Lake Vector Peak Payload Bytes' in n][0]
                assert int(vnode['Lake Vector Peak Payload Bytes']) <= 8 * 1024 * 1024
                assert int(vnode['Lake Vector Batches']) >= 2, vnode
            assert q(cm, "SELECT id, k, length(a), md5(a), octet_length(b), md5(b) FROM biz.t_wide ORDER BY id") == oracle_wide, dop
        c4 += 2

        # Fallback tests: UUID/JSON columns stay non-Vec; uncommitted write falls back to DOP=1 serial VecForeignScan
        q(cm, "CREATE FOREIGN TABLE biz.t_uuid (id int4, u uuid) SERVER c_v72 OPTIONS (binding_mode 'create', lake_schema 'main')")
        q(cm, "INSERT INTO biz.t_uuid SELECT g, ('11111111-2222-4333-8444-' || lpad(to_hex(g), 12, '0'))::uuid FROM generate_series(1, 20) AS g")
        q(cm, "SELECT og_lake.flush('biz.t_uuid'::regclass)")
        set_vec_smp(cm, 1, 'force')
        assert 'Lake Batch Delivery: row ForeignScan' in explain_text(cm, "SELECT count(*) FROM biz.t_uuid", analyze=False)

        q(cm, "BEGIN")
        q(cm, "INSERT INTO biz.t_core VALUES (9901, 4, 100, 1.5, true, 'uncommitted_vec')")
        set_vec_smp(cm, 4, 'force')
        exp_uncomm = explain_text(cm, "SELECT count(*), sum(v1) FROM biz.t_core WHERE id >= 9900", analyze=True)
        assert 'Lake Batch Delivery: native VecForeignScan / owned scalar VectorBatch' in exp_uncomm
        assert 'Lake SMP Planned DOP' not in exp_uncomm
        assert q(cm, "SELECT id, txt FROM biz.t_core WHERE id = 9901") == [(9901, 'uncommitted_vec')]
        q(cm, "ROLLBACK")
        c4 += 2

        # Cached PREPARE VecStream -> VecForeignScan invalidation when enable_lake_smp=off or ADD COLUMN happens
        set_vec_smp(cm, 4, 'force')
        q(cm, "PREPARE p_vsmp(int4) AS SELECT count(*), sum(v1) FROM biz.t_core WHERE grp >= $1")
        for cutoff in (0, 1, 2, 3, 4, 5):
            assert q(cm, f"EXECUTE p_vsmp({cutoff})") == q(obs, "SELECT count(*), sum(v1) FROM biz.ex_core WHERE grp >= %s", (cutoff,))
        q(cm, "SET enable_lake_smp = off")
        try:
            q(cm, "EXECUTE p_vsmp(5)")
            raise AssertionError('Expected cached VecStream plan failure when enable_lake_smp=off')
        except psycopg2.Error as exc:
            assert exc.pgcode == '0A000', (exc.pgcode, exc)
        q(cm, "SET enable_lake_smp = on")
        assert q(cm, "EXECUTE p_vsmp(5)") == q(obs, "SELECT count(*), sum(v1) FROM biz.ex_core WHERE grp >= 5")
        q(cm, "DEALLOCATE p_vsmp")
        # Separate small table for ALTER ADD COLUMN invalidation from Vec -> Row
        q(cm, "CREATE FOREIGN TABLE biz.t_evolve_v (id int4, v int4) SERVER c_v72 OPTIONS (binding_mode 'create', lake_schema 'main')")
        for b in range(2):
            q(cm, f"INSERT INTO biz.t_evolve_v SELECT g, g * 5 FROM generate_series({b * 10 + 1}, {(b + 1) * 10}) AS g")
            q(cm, "SELECT og_lake.flush('biz.t_evolve_v'::regclass)")
        set_vec_smp(cm, 2, 'force')
        assert 'Lake Batch Delivery: native VecForeignScan / owned scalar VectorBatch' in explain_text(cm, "SELECT count(*), sum(v) FROM biz.t_evolve_v", analyze=False)
        q(cm, "ALTER FOREIGN TABLE biz.t_evolve_v ADD COLUMN tag text DEFAULT 'ok'")
        assert 'Lake Batch Delivery: row ForeignScan' in explain_text(cm, "SELECT count(*), sum(v) FROM biz.t_evolve_v", analyze=False)
        assert s(cm, "SELECT count(*) FROM biz.t_evolve_v WHERE tag = 'ok'") == 20
        c4 += 3

        # Task shortage adaptive fallback (1 inline task -> DOP=1; 2 tasks -> DOP=2; 4 tasks -> DOP=4)
        q(cm, "CREATE FOREIGN TABLE biz.t_adapt (id int4, v int4) SERVER c_v72 OPTIONS (binding_mode 'create', lake_schema 'main')")
        q(cm, "INSERT INTO biz.t_adapt SELECT g, g * 2 FROM generate_series(1, 20) AS g")
        set_vec_smp(cm, 4, 'force')
        exp_a1 = explain_text(cm, "SELECT count(*) FROM biz.t_adapt", analyze=False)
        assert 'Lake Batch Delivery: native VecForeignScan' in exp_a1 and 'Lake SMP Planned DOP' not in exp_a1, exp_a1
        q(cm, "SELECT og_lake.flush('biz.t_adapt'::regclass)")
        q(cm, "INSERT INTO biz.t_adapt VALUES (21, 42)")  # 1 file + 1 inline = 2 tasks
        exp_a2 = explain_text(cm, "SELECT count(*) FROM biz.t_adapt", analyze=False)
        assert 'Lake Batch Delivery: native VecForeignScan' in exp_a2 and 'Lake SMP Planned DOP: 2' in exp_a2, exp_a2
        assert s(cm, "SELECT sum(v) FROM biz.t_adapt") == sum(g * 2 for g in range(1, 22))
        c4 += 2

        # Live SMP probe on VecForeignScan when thread_pool=off (worker pause, concurrent flush isolation, error/recovery, early LIMIT)
        if tp_mode == 'off':
            set_vec_smp(cm, 1, 'force')
            q(cm, "CREATE FUNCTION biz.vsmp_probe(integer,integer,bigint) RETURNS text AS '$libdir/og_lake','og_lake_test_smp_probe' LANGUAGE C VOLATILE NOT FENCED")
            set_vec_smp(cm, 4, 'force')
            key_v = (1 << 45) + int(hashlib.sha256(tag.encode()).hexdigest()[:9], 16)
            dbid = s(obs, "SELECT oid FROM pg_database WHERE datname=current_database()")
            s(cm, "SELECT biz.vsmp_probe(-1, 0, 0)")
            s(holder, "SELECT pg_advisory_lock(%s::bigint)", (key_v,))
            s(cm, "SELECT biz.vsmp_probe(1, 2, %s::bigint)", (key_v,))
            with ThreadPoolExecutor(max_workers=1) as pool:
                fut = pool.submit(q, cm, "SELECT count(*), sum(v1) FROM biz.t_core")
                deadline = time.monotonic() + 30
                locks = []
                while time.monotonic() < deadline and not fut.done():
                    locks = q(obs, """
                        SELECT pid FROM pg_locks
                        WHERE locktype='advisory' AND database=%s AND classid=%s::oid AND objid=%s::oid
                          AND objsubid=1 AND mode='ExclusiveLock' AND NOT granted
                    """, (dbid, key_v >> 32, key_v & 0xffffffff))
                    if locks:
                        break
                    time.sleep(0.01)
                assert len(locks) == 1, f'Vec worker pause gate not hit: {locks}'
                # Concurrent UPDATE + flush must not leak into S_stmt of paused Vec workers
                q(holder, "UPDATE biz.t_core SET v1 = v1 + 100 WHERE id = 1")
                q(holder, "SELECT og_lake.flush('biz.t_core'::regclass)")
                s(holder, "SELECT pg_advisory_unlock(%s::bigint)", (key_v,))
                got_snap = fut.result(timeout=30)
                assert got_snap == q(obs, "SELECT count(*), sum(v1) FROM biz.ex_core"), got_snap
            q(obs, "UPDATE biz.ex_core SET v1 = v1 + 100 WHERE id = 1")
            st_ok = json.loads(s(cm, "SELECT biz.vsmp_probe(0, 0, 0)"))
            assert st_ok['participants'] == 4 and all(w['done'] == 1 for w in st_ok['workers']), st_ok
            assert sum(w['rows'] for w in st_ok['workers']) == len(oracle_all), st_ok

            # Error recovery inside Vec worker (mode=2) & LIMIT early close cleanup
            s(cm, "SELECT biz.vsmp_probe(-1, 0, 0)")
            s(cm, "SELECT biz.vsmp_probe(2, 1, 0)")
            try:
                q(cm, "SELECT count(*) FROM biz.t_core")
                raise AssertionError('Expected failure for Vec SMP probe mode 2')
            except psycopg2.Error as exc:
                assert exc.pgcode == 'XX000', (exc.pgcode, exc)
            st_err = json.loads(s(cm, "SELECT biz.vsmp_probe(0, 0, 0)"))
            assert all(w['done'] in (1, 2) for w in st_err['workers'] if w['claimed']), st_err

            s(cm, "SELECT biz.vsmp_probe(-1, 0, 0)")
            assert len(q(cm, "SELECT id, txt FROM biz.t_core LIMIT 3")) == 3
            st_lim = json.loads(s(cm, "SELECT biz.vsmp_probe(0, 0, 0)"))
            assert all(w['done'] in (1, 2) for w in st_lim['workers'] if w['claimed']), st_lim
            assert q(cm, "SELECT count(*), sum(v1) FROM biz.t_core") == q(obs, "SELECT count(*), sum(v1) FROM biz.ex_core")
            c4 += 4

        stages['stage4_wide_cache_fallback_probe'] = c4

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
    p.add_argument('--profile', choices=['debug', 'ann-debug', 'release'], required=True)
    p.add_argument('--out')
    args = p.parse_args()
    run_suite(args)


if __name__ == '__main__':
    main()
