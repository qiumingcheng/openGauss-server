#!/usr/bin/env python3
"""Release (-O2) thorough closure validation:
1) Upstream openGauss datavec SQL regression (diskann_vector, hnsw_lsg_vector, hnsw_rabitq_vector)
2) Core SQL0.56..0.70 historical suites replayed on Release -O2
3) 1,000,000-row (1M) scale ingest, Row/Vec SMP DOP=1/4, truncate(1000)+bucket(4) pruning
4) 20k & 50k DiskANN+PQ dim=32 crossover benchmark (build_ms, exact vs PQ DOP=1/4, Recall@10)
5) Thread-pool=on (4 workers) soak: 80-table >64 SPI_keepplan LRU eviction + DDL invalidation,
   multi-writer DML/flush/GC + concurrent Vec DOP=4 readers + pg_cancel_backend + VmRSS monitor.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
import hashlib
import json
import math
import os
from pathlib import Path
import random
import statistics
import subprocess
import sys
import tempfile
import threading
import time

import psycopg2
from psycopg2 import sql

ROOT = Path('/home/qiumc/data/code/cpp')
INTEGRATION = ROOT / 'integration/opengauss-native-lake'
VERIF = ROOT / '公共文档/openGauss原生湖存储/验证'
REPLAY_DIR = Path('/home/qiumc/.gemini/antigravity-cli/brain/c42afb00-fe75-4503-b0b5-faefda85817d/scratch/run_074_compat/replay_056_074')

ROARING_BUILD = VERIF / 'native_bitmap_roaring_oom_dependency_1109_01/build.json'
BLAS_BUILD = VERIF / 'pq_openblas_locking_build_1109_01/build.json'

EXPECTED_SDK54 = 'b07ee911aaf81aa816c2c280bd1568871353bbd6f8af5792ac5cdf9010240c51'
EXPECTED_SDK6 = '4f2a2cd79d50f3f2c3b1350ac58b4868b20b727a5141ae49ec1033b4d3a20368'
EXPECTED_RELEASE_CONF = '9047aaa1c1379256ac625c3d6e761192c0dd8cb3999832a5ec210738f2003ee8'

REL_CONF = ROOT / '.runtime/opengauss-native-lake/release/data/postgresql.conf'
REL_SOCK = str(ROOT / '.runtime/opengauss-native-lake/release/socket')
REL_PORT = 55570
REL_INSTALL = ROOT / '.install/opengauss-native-lake-release'
REL_SDK54_SO = REL_INSTALL / 'lib/postgresql/og_lake_sdk/libog_lake_sdk_arrow.so'
REL_SDK6_SO = REL_INSTALL / 'lib/postgresql/og_lake_external_sdk/libog_lake_sdk_arrow.so'


def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def driver_env():
    env = os.environ.copy()
    env['OG_LAKE_SOURCE'] = str(ROOT / '.worktrees/opengauss-native-lake')
    env['OG_LAKE_TEST_PROFILE'] = 'release'
    return env


def run_cmd(cmd, label='', timeout=1200):
    print(f'[{time.strftime("%H:%M:%S")}] START {label}', flush=True)
    proc = subprocess.run(cmd, cwd=str(ROOT), env=driver_env(), capture_output=True, text=True, timeout=timeout)
    if proc.returncode != 0:
        raise RuntimeError(
            f'{label} failed ({proc.returncode}): {" ".join(map(str, cmd))}\n'
            f'STDOUT:\n{proc.stdout[-4000:]}\nSTDERR:\n{proc.stderr[-4000:]}'
        )
    print(f'[{time.strftime("%H:%M:%S")}] DONE  {label}', flush=True)
    return proc


def rel_driver(action, run_id, build_receipt=None):
    cmd = [
        sys.executable, str(INTEGRATION / 'build_driver.py'), 'release', action,
        '--run-id', run_id,
        '--roaring-build', str(ROARING_BUILD),
        '--pq-in-tree',
        '--blas-build', str(BLAS_BUILD),
    ]
    if action == 'restart' and build_receipt is not None:
        cmd.extend(['--reuse-build-receipt', str(build_receipt)])
    return cmd


def patch_conf(orig_bytes: bytes, overrides: dict) -> bytes:
    lines = orig_bytes.decode('utf-8').splitlines()
    rem = dict(overrides)
    out = []
    for ln in lines:
        s = ln.lstrip()
        matched = None
        if s and not s.startswith('#'):
            for k in list(rem.keys()):
                if s.startswith(k) and s[len(k):].lstrip().startswith('='):
                    matched = k
                    break
        if matched:
            out.append(f'{matched} = {rem.pop(matched)}')
        else:
            out.append(ln)
    for k, v in rem.items():
        out.append(f'{k} = {v}')
    return ('\n'.join(out) + '\n').encode('utf-8')


def connect(dbname='postgres', autocommit=True):
    conn = psycopg2.connect(host=REL_SOCK, port=REL_PORT, dbname=dbname)
    conn.autocommit = autocommit
    return conn


def q(conn, stmt, args=None):
    with conn.cursor() as cur:
        if args is None:
            cur.execute(stmt)
        else:
            cur.execute(stmt, args)
        return cur.fetchall() if cur.description else None


def s(conn, stmt, args=None):
    rows = q(conn, stmt, args)
    return rows[0][0] if rows else None


def explain_lines(conn, stmt):
    return [r[0] for r in q(conn, 'EXPLAIN (ANALYZE, COSTS OFF) ' + stmt)]


def md5_role_pw(password: str, role: str) -> str:
    return 'md5' + hashlib.md5((password + role).encode('utf-8')).hexdigest()


def setup_lake_catalog(admin, db_name: str, srv_name: str, meta_schema: str, root_dir: Path, inline_limit: int = 50000):
    """Create isolated database, private owner role, extension, catalog, and biz schema."""
    tag = db_name.replace('-', '_')[-10:]
    owner_role = f'u_{tag}_{os.getpid()}'
    q(admin, f"DROP ROLE IF EXISTS {owner_role}")
    q(admin, f"CREATE ROLE {owner_role} NOLOGIN NOINHERIT ENCRYPTED PASSWORD %s", (md5_role_pw('Lake073!', owner_role),))
    q(admin, f'DROP DATABASE IF EXISTS {db_name}')
    q(admin, f"CREATE DATABASE {db_name} WITH DBCOMPATIBILITY 'PG' ENCODING 'UTF8'")
    conn = connect(db_name)
    idx_dir = root_dir / 'idx'
    cat_dir = root_dir / 'cat'
    idx_dir.mkdir(parents=True, exist_ok=True)
    os.chmod(root_dir, 0o755)
    os.chmod(idx_dir, 0o755)
    q(conn, 'CREATE EXTENSION og_lake')
    q(conn, f'CREATE SCHEMA biz; GRANT USAGE ON SCHEMA biz TO {owner_role}')
    q(conn, 'SELECT og_lake.configure_internal_owner(%s::name)', (owner_role,))
    q(conn, f"SELECT og_lake.create_catalog('{srv_name}'::name, '{meta_schema}'::name, %s::text, NULL::text)", (str(cat_dir),))
    q(conn, f"SELECT og_lake.set_option('{srv_name}'::name, 'data_inlining_row_limit'::text, %s::text)", (str(inline_limit),))
    q(conn, f"SELECT og_lake.set_option('{srv_name}'::name, 'index_root'::text, %s::text)", (str(idx_dir),))
    return conn, owner_role


def run_datavec_regress(out_dir: Path):
    gsql = str(REL_INSTALL / 'bin/gsql')
    gsql_env = driver_env()
    gsql_env['GAUSSHOME'] = str(REL_INSTALL)
    gsql_env['LD_LIBRARY_PATH'] = f"{REL_INSTALL}/lib:{ROOT}/binarylibs/buildtools/gcc10.3/gcc/lib64"
    regress_dir = ROOT / '.worktrees/opengauss-native-lake/src/test/regress'
    results = {}
    for name in ('diskann_vector', 'hnsw_lsg_vector', 'hnsw_rabitq_vector'):
        sql_path = regress_dir / 'sql' / f'{name}.sql'
        lines = sql_path.read_text(encoding='utf-8').splitlines()
        body = [
            ln for ln in lines
            if ln.strip().lower() not in ('create database test;', '\\c test', '\\c regression', 'drop database test;')
        ]
        tmp_sql = out_dir / f'{name}.adapted.sql'
        tmp_sql.write_text('\n'.join(body) + '\n', encoding='utf-8')
        db_name = f'rel_dv_{name}_{os.getpid()}'
        with closing(connect()) as admin:
            q(admin, f'DROP DATABASE IF EXISTS {db_name}')
            q(admin, f"CREATE DATABASE {db_name} DBCOMPATIBILITY 'PG'")
        try:
            proc = subprocess.run(
                [gsql, '-h', REL_SOCK, '-p', str(REL_PORT), '-d', db_name, '-v', 'ON_ERROR_STOP=0', '-f', str(tmp_sql)],
                env=gsql_env, capture_output=True, text=True, timeout=300
            )
            assert proc.returncode == 0, f'{name} gsql failed: {proc.stderr}'
            out_txt = proc.stdout + '\n' + proc.stderr
            (out_dir / f'{name}.out').write_text(out_txt, encoding='utf-8')
            if name == 'diskann_vector':
                assert 'Ann Index Scan using idx_vectors_l2 on diskann_t1' in out_txt
                assert 'Ann Index Scan using idx_vectors_cosine on diskann_t1' in out_txt
                assert 'Ann Index Scan using idx_vectors_ip on diskann_t1' in out_txt
                assert 'Ann Index Scan using idx_vectors_10d_100 on diskann_t2' in out_txt
                assert 'column cannot have more than 1536 dimensions for diskann index' in out_txt
                assert 'value 15 out of bounds for option "index_size"' in out_txt
                assert 'value 1001 out of bounds for option "index_size"' in out_txt
                assert out_txt.count('ERROR:') == 3
                assert out_txt.count('(2 rows)') >= 5
                for row_tag in ('  8 |', '  7 |', '  6 |', ' 10 |', '  9 |'):
                    assert row_tag in out_txt
            elif name == 'hnsw_lsg_vector':
                assert out_txt.count('Order By: (val <=>') >= 2
                assert out_txt.count('Order By: (val <->') >= 2
                assert out_txt.count('Order By: (val <#>') >= 2
                assert 'create an hnsw lsg index requires inserting data first' in out_txt.lower()
                assert 'hnsw does not support the mixed use of the lsg and quantization methods' in out_txt.lower()
                assert out_txt.count('ERROR:') == 6
            else:
                assert out_txt.count('Order By: (val <=>') >= 2
                assert out_txt.count('Order By: (val <->') >= 2
                assert out_txt.count('Order By: (val <#>') >= 2
                assert 'ERROR:' not in out_txt
            results[name] = {'status': 'passed', 'stdout_lines': len(proc.stdout.splitlines())}
        finally:
            with closing(connect()) as admin:
                q(admin, f'DROP DATABASE IF EXISTS {db_name}')
    return results


def run_release_history_suites(build_receipt: Path, out_dir: Path, orig_conf: bytes):
    hist_dir = out_dir / 'history_suites'
    hist_dir.mkdir(parents=True, exist_ok=True)
    results = {}

    for name in ('run_056_suite.py', 'run_057_suite.py', 'run_058_suite.py', 'run_059_suite.py', 'run_061_suite.py'):
        out_json = hist_dir / f'{name}.json'
        run_cmd([sys.executable, str(REPLAY_DIR / name), '--output', str(out_json)], label=f'release:{name}')
        results[name] = json.loads(out_json.read_text(encoding='utf-8'))

    for name in ('run_062_suite.py', 'run_063_suite.py', 'run_064_suite.py', 'run_065_suite.py', 'run_066_suite.py', 'run_069_suite.py'):
        out_json = hist_dir / f'{name}.json'
        run_cmd([sys.executable, str(REPLAY_DIR / name), '--profile', 'release', '--out', str(out_json)], label=f'release:{name}')
        r = json.loads(out_json.read_text(encoding='utf-8'))
        assert r['status'] in ('pass', 'passed'), (name, r)
        results[name] = r

    # 064(--ix5-only) / 067 / 068 / 070 with exact maintenance GUC profiles under Release
    maint_groups = [
        ('ix5_064', {
            'enable_lake_maintenance': 'on', 'enable_lake_index_maintenance': 'on',
            'lake_flush_naptime': '200ms', 'lake_index_maintenance_naptime': '200ms',
            'lake_flush_max_delay': '3600000ms', 'lake_flush_min_rows': '1000', 'lake_index_build_budget': '600000ms',
        }, 'run_064_suite.py', ['--ix5-only']),
        ('m067', {
            'enable_lake_maintenance': 'on', 'enable_lake_index_maintenance': 'on',
            'enable_lake_autoanalyze': 'on', 'lake_flush_naptime': '250', 'lake_index_maintenance_naptime': '250',
        }, 'run_067_suite.py', []),
        ('m068', {
            'enable_lake_maintenance': 'on', 'enable_lake_index_maintenance': 'on',
            'enable_lake_autoanalyze': 'on', 'lake_flush_naptime': '250',
            'lake_flush_min_rows': '5', 'lake_flush_max_delay': '250', 'lake_index_maintenance_naptime': '250',
        }, 'run_068_suite.py', []),
        ('m070', {
            'enable_lake_maintenance': 'on', 'enable_lake_index_maintenance': 'on',
            'enable_lake_autoanalyze': 'on', 'lake_flush_naptime': '15000',
            'lake_flush_min_rows': '500', 'lake_flush_max_delay': '20000', 'lake_index_maintenance_naptime': '1000',
        }, 'run_070_suite.py', []),
    ]
    for tag, ov, sname, extra in maint_groups:
        REL_CONF.write_bytes(patch_conf(orig_conf, ov))
        os.chmod(REL_CONF, 0o600)
        run_cmd(rel_driver('restart', f'rel_hist_{tag}_{int(time.time())}', build_receipt), label=f'restart release {tag}')
        out_json = hist_dir / f'{tag}_{sname}.json'
        run_cmd([sys.executable, str(REPLAY_DIR / sname), '--profile', 'release', '--out', str(out_json), *extra], label=f'release:{tag}:{sname}')
        r = json.loads(out_json.read_text(encoding='utf-8'))
        assert r.get('status', 'passed') in ('pass', 'passed') and r.get('total_checks', 1) > 0, (sname, r)
        results[f'{tag}:{sname}'] = r

    # Restore baseline release conf & restart cleanly
    REL_CONF.write_bytes(orig_conf)
    os.chmod(REL_CONF, 0o600)
    run_cmd(rel_driver('restart', f'rel_hist_restore_{int(time.time())}', build_receipt), label='restart release baseline')
    return results


def bench_1m_scale(out_dir: Path):
    """1,000,000 rows (16 Parquet files) with truncate(125000) + bucket(2) partitioning."""
    db_name = f'rel_scale1m_{os.getpid()}'
    root_dir = Path(tempfile.mkdtemp(prefix='og_scale1m_', dir='/tmp'))
    owner_role = None
    try:
        with closing(connect()) as admin:
            conn, owner_role = setup_lake_catalog(admin, db_name, 'srv1m', 'meta1m', root_dir, inline_limit=150000)
        with closing(conn):
            q(conn, 'SET statement_timeout = 600000')
            q(conn, """
                CREATE FOREIGN TABLE biz.biz1m (
                    id bigint,
                    k bigint,
                    b integer,
                    grp integer,
                    v double precision,
                    num numeric(12,2),
                    d date,
                    tag text
                ) SERVER srv1m OPTIONS (binding_mode 'create', lake_schema 'main')
            """)
            q(conn, "SELECT og_lake.set_partition('biz.biz1m'::regclass, ARRAY['k', 'b'], ARRAY['truncate(125000)', 'bucket(2)'])")
            q(conn, 'CREATE TABLE biz.exact1m (id bigint, k bigint, b integer, grp integer, v double precision, num numeric(12,2), d date, tag text)')
            q(conn, """
                INSERT INTO biz.exact1m
                SELECT g, g - 1, CASE WHEN g % 2 = 0 THEN 0 ELSE 3 END, (g % 100), (g % 1000) * 0.25,
                       round(((g % 5000) / 100.0)::numeric, 2),
                       DATE '2025-01-01' + (g % 365),
                       'r_' || (g % 32)
                FROM generate_series(1, 1000000) g
            """)
            t0 = time.monotonic()
            for i in range(8):
                lo = i * 125000 + 1
                hi = (i + 1) * 125000
                q(conn, f'INSERT INTO biz.biz1m SELECT * FROM biz.exact1m WHERE id BETWEEN {lo} AND {hi}')
                for _ in range(3):
                    q(conn, "SELECT og_lake.flush('biz.biz1m'::regclass)")
            ingest_ms = round((time.monotonic() - t0) * 1000.0, 3)

            file_cnt = s(conn, 'SELECT count(*) FROM meta1m.ducklake_data_file WHERE end_snapshot IS NULL')
            assert int(file_cnt) == 16, file_cnt
            expected = q(conn, 'SELECT count(*), sum(k), round(sum(v)::numeric, 4), min(num), max(num) FROM biz.exact1m')[0]

            def measure_agg(setup_sql, agg_sql, repeats=4):
                times = []
                res = None
                for st in setup_sql:
                    q(conn, st)
                for _ in range(repeats):
                    ts = time.monotonic()
                    res = q(conn, agg_sql)[0]
                    times.append((time.monotonic() - ts) * 1000.0)
                return round(statistics.median(times[1:] if len(times) > 1 else times), 3), res

            agg_sql = 'SELECT count(*), sum(k), round(sum(v)::numeric, 4), min(num), max(num) FROM biz.biz1m'
            row_d1_ms, r1 = measure_agg([
                "SET try_vector_engine_strategy = 'off'",
                'SET enable_vector_engine = off',
                'SET enable_lake_smp = off',
                'SET enable_force_smp = off',
                'SET query_dop = 1',
            ], agg_sql)
            assert r1 == expected, (r1, expected)

            row_d4_ms, r2 = measure_agg([
                "SET try_vector_engine_strategy = 'off'",
                'SET enable_vector_engine = off',
                'SET enable_lake_smp = on',
                'SET enable_force_smp = off',
                'SET query_dop = 4',
                'SET enable_force_smp = on',
            ], agg_sql)
            assert r2 == expected, (r2, expected)

            vec_d1_ms, r3 = measure_agg([
                'SET enable_vector_engine = on',
                "SET try_vector_engine_strategy = 'force'",
                'SET enable_lake_smp = off',
                'SET enable_force_smp = off',
                'SET query_dop = 1',
            ], agg_sql)
            assert r3 == expected, (r3, expected)
            exp_vd1 = '\n'.join(explain_lines(conn, agg_sql))
            assert 'Lake Batch Delivery: native VecForeignScan / owned scalar VectorBatch' in exp_vd1, exp_vd1

            vec_d4_ms, r4 = measure_agg([
                'SET enable_vector_engine = on',
                "SET try_vector_engine_strategy = 'force'",
                'SET enable_lake_smp = on',
                'SET enable_force_smp = off',
                'SET query_dop = 4',
                'SET enable_force_smp = on',
            ], agg_sql)
            assert r4 == expected, (r4, expected)
            exp_vd4 = '\n'.join(explain_lines(conn, agg_sql))
            assert 'Lake Batch Delivery: native VecForeignScan / owned scalar VectorBatch' in exp_vd4, exp_vd4
            assert 'Lake SMP Planned DOP: 4' in exp_vd4, exp_vd4

            # Truncate + bucket pruning check on 1M rows under VecForeignScan DOP=1 (16 files -> 15 skipped, 1 admitted)
            prune_sql = 'SELECT count(*), sum(id) FROM biz.biz1m WHERE k BETWEEN 500000 AND 500999 AND b = 3'
            exp_prune = q(conn, 'SELECT count(*), sum(id) FROM biz.exact1m WHERE k BETWEEN 500000 AND 500999 AND b = 3')[0]
            prune_ms, r_prune = measure_agg([
                'SET enable_vector_engine = on',
                "SET try_vector_engine_strategy = 'force'",
                'SET enable_lake_smp = off',
                'SET enable_force_smp = off',
                'SET query_dop = 1',
            ], prune_sql, repeats=5)
            plan_txt = '\n'.join(explain_lines(conn, prune_sql))
            assert 'Lake Batch Delivery: native VecForeignScan / owned scalar VectorBatch' in plan_txt, plan_txt
            assert 'Lake Partition Files Skipped: 15' in plan_txt and 'Lake Partition Files Admitted: 1' in plan_txt, plan_txt
            assert r_prune == exp_prune, (r_prune, exp_prune)

            return {
                'rows': 1000000,
                'data_files': int(file_cnt),
                'ingest_8_batches_flush_ms': ingest_ms,
                'row_dop1_ms': row_d1_ms,
                'row_dop4_ms': row_d4_ms,
                'row_smp_speedup': round(row_d1_ms / row_d4_ms, 3),
                'vec_dop1_ms': vec_d1_ms,
                'vec_dop4_ms': vec_d4_ms,
                'vec_smp_speedup': round(vec_d1_ms / vec_d4_ms, 3),
                'vec_dop4_vs_row_dop1': round(row_d1_ms / vec_d4_ms, 3),
                'truncate_bucket_prune_1m_ms': prune_ms,
            }
    finally:
        with closing(connect()) as admin:
            q(admin, f'DROP DATABASE IF EXISTS {db_name}')
            if owner_role:
                q(admin, f'DROP ROLE IF EXISTS {owner_role}')
        subprocess.run(['rm', '-rf', str(root_dir)], check=False)


def make_vec_lit(seed: int, dim: int = 32) -> str:
    rng = random.Random(seed)
    return '[' + ','.join(f'{rng.uniform(-1.0, 1.0):.4f}' for _ in range(dim)) + ']'


def bench_pq_scale(conn_db_prefix: str, total_rows: int, n_files: int = 4, dim: int = 32):
    db_name = f'{conn_db_prefix}_{total_rows}_{os.getpid()}'
    root_dir = Path(tempfile.mkdtemp(prefix=f'og_pq{total_rows}_', dir='/tmp'))
    rows_per_file = total_rows // n_files
    owner_role = None
    try:
        with closing(connect()) as admin:
            conn, owner_role = setup_lake_catalog(admin, db_name, 'srvpq', 'metapq', root_dir, inline_limit=max(50000, rows_per_file + 100))
        with closing(conn):
            q(conn, 'SET statement_timeout = 900000')
            q(conn, f"""
                CREATE FOREIGN TABLE biz.t_pq (
                    id integer,
                    grp integer,
                    v vector({dim})
                ) SERVER srvpq OPTIONS (binding_mode 'create', lake_schema 'main')
            """)
            q(conn, f'CREATE TABLE biz.staging_pq (id integer, grp integer, v vector({dim}))')
            q(conn, f"""
                INSERT INTO biz.staging_pq
                SELECT g, g % 16,
                       ('[' || (SELECT string_agg((((g * 31 + d * 17 + (g % 19) * d) % 997) / 500.0 - 1.0)::text, ',')
                                FROM generate_series(1, {dim}) AS d) || ']')::vector({dim})
                FROM generate_series(1, {total_rows}) g
            """)
            for fi in range(n_files):
                lo = fi * rows_per_file + 1
                hi = (fi + 1) * rows_per_file
                q(conn, f'INSERT INTO biz.t_pq SELECT * FROM biz.staging_pq WHERE id BETWEEN {lo} AND {hi}')
                q(conn, "SELECT og_lake.flush('biz.t_pq'::regclass)")
            q(conn, 'ANALYZE biz.t_pq')

            idx_uuid = s(conn, """
                SELECT og_lake.create_lake_index(
                    'biz.t_pq'::regclass, 'idx_pq'::name,
                    '{"version":1,"kind":"vector","method":"diskann","column":"v","metric":"l2","pq":true,"pq_chunks":8}'::jsonb
                )
            """)
            t0 = time.monotonic()
            for _ in range(n_files + 2):
                q(conn, 'SELECT og_lake.build_lake_index(%s::uuid)', (idx_uuid,))
                done_cnt = s(conn, "SELECT count(*) FROM og_lake_internal.index_segment WHERE index_uuid=%s::uuid AND state='READY' AND format_version=2", (idx_uuid,))
                if int(done_cnt) >= n_files:
                    break
            build_ms = round((time.monotonic() - t0) * 1000.0, 3)
            done_cnt = s(conn, "SELECT count(*) FROM og_lake_internal.index_segment WHERE index_uuid=%s::uuid AND state='READY' AND format_version=2", (idx_uuid,))
            assert int(done_cnt) == n_files, f'Expected {n_files} READY format2 segments, got {done_cnt}'

            queries = [make_vec_lit(1000 + i, dim) for i in range(5)]
            recalls = []
            exact_times = []
            pq_d1_times = []
            pq_d4_times = []

            for qi, qv in enumerate(queries):
                q(conn, "SET enable_vector_engine = off; SET try_vector_engine_strategy = 'off'; SET enable_lake_smp = on; SET ann_orderby_distance_reuse = off; SET query_max_mem = 0")
                q(conn, 'SET enable_lake_ann = off; SET enable_seqscan = on; SET enable_force_smp = off; SET query_dop = 1')
                truth_rows = q(conn, f"SELECT id, (v <-> %s::vector({dim})) AS d FROM biz.staging_pq ORDER BY d, id LIMIT 10", (qv,))
                te = time.monotonic()
                _ = q(conn, f"SELECT id FROM biz.t_pq WHERE v IS NOT NULL ORDER BY v <-> %s::vector({dim}), id LIMIT 10", (qv,))
                exact_times.append((time.monotonic() - te) * 1000.0)
                k10_cutoff = truth_rows[-1][1] + 1e-6
                truth_set = {r[0] for r in q(conn, f"SELECT id FROM biz.staging_pq WHERE (v <-> %s::vector({dim})) <= %s", (qv, k10_cutoff))}

                q(conn, 'SET enable_lake_ann = on; SET enable_seqscan = off; SET enable_force_smp = off; SET query_dop = 1')
                if qi == 0:
                    q(conn, f"SELECT id FROM biz.t_pq WHERE v IS NOT NULL ORDER BY v <-> %s::vector({dim}) LIMIT 10", (qv,))
                t1 = time.monotonic()
                approx_d1 = q(conn, f"SELECT id FROM biz.t_pq WHERE v IS NOT NULL ORDER BY v <-> %s::vector({dim}) LIMIT 10", (qv,))
                pq_d1_times.append((time.monotonic() - t1) * 1000.0)
                hits = sum(1 for (aid,) in approx_d1 if aid in truth_set)
                recalls.append(hits / 10.0)

                q(conn, 'SET enable_force_smp = off; SET query_dop = 4; SET enable_force_smp = on')
                if qi == 0:
                    exp_pq = '\n'.join(explain_lines(conn, f"SELECT id FROM biz.t_pq WHERE v IS NOT NULL ORDER BY v <-> %s::vector({dim}) LIMIT 10".replace('%s', f"'{qv}'")))
                    assert 'Lake SMP Planned DOP: 4' in exp_pq and 'Lake SMP ANN PQ: admitted' in exp_pq, exp_pq
                t4 = time.monotonic()
                approx_d4 = q(conn, f"SELECT id FROM biz.t_pq WHERE v IS NOT NULL ORDER BY v <-> %s::vector({dim}) LIMIT 10", (qv,))
                pq_d4_times.append((time.monotonic() - t4) * 1000.0)
                assert len(approx_d4) == 10

            return {
                'rows': total_rows,
                'files': n_files,
                'dim': dim,
                'build_ms': build_ms,
                'exact_scan_median_ms': round(statistics.median(exact_times), 3),
                'pq_warm_dop1_median_ms': round(statistics.median(pq_d1_times), 3),
                'pq_warm_dop4_median_ms': round(statistics.median(pq_d4_times), 3),
                'mean_recall_at_10': round(sum(recalls) / len(recalls), 4),
                'min_recall_at_10': round(min(recalls), 4),
            }
    finally:
        with closing(connect()) as admin:
            q(admin, f'DROP DATABASE IF EXISTS {db_name}')
            if owner_role:
                q(admin, f'DROP ROLE IF EXISTS {owner_role}')
        subprocess.run(['rm', '-rf', str(root_dir)], check=False)


def total_gaussdb_rss_kb() -> int:
    proc = subprocess.run(['pgrep', '-x', 'gaussdb'], capture_output=True, text=True)
    if proc.returncode != 0:
        return 0
    total = 0
    for pid in proc.stdout.split():
        st = Path(f'/proc/{pid}/status')
        if st.exists():
            for ln in st.read_text().splitlines():
                if ln.startswith('VmRSS:'):
                    total += int(ln.split()[1])
    return total


def soak_threadpool_spi_cache_and_gc(build_receipt: Path, orig_conf: bytes):
    """Thread-pool=on (4w): 80-table >64 SPI_keepplan LRU eviction/invalidation +
    concurrent mixed DML/flush/GC/Vec-readers/pg_cancel + VmRSS drift audit.
    """
    REL_CONF.write_bytes(patch_conf(orig_conf, {
        'enable_thread_pool': 'on',
        'thread_pool_attr': "'4, 1, (nobind)'",
    }))
    os.chmod(REL_CONF, 0o600)
    run_cmd(rel_driver('restart', f'rel_soak_tpon_{int(time.time())}', build_receipt), label='restart release thread_pool=on')

    db_name = f'rel_soak_{os.getpid()}'
    root_dir = Path(tempfile.mkdtemp(prefix='og_soak_', dir='/tmp'))
    owner_role = None
    try:
        with closing(connect()) as admin:
            conn, owner_role = setup_lake_catalog(admin, db_name, 'srv_soak', 'meta_soak', root_dir, inline_limit=50000)
        with closing(conn):
            assert s(conn, 'SHOW enable_thread_pool') == 'on'

            # 5A. Create 80 distinct lake tables (>64 LRU cache limit), populate & flush, churn queries & DDL
            for i in range(80):
                q(conn, f"""
                    CREATE FOREIGN TABLE biz.lru_{i} (id int, k int, val text)
                    SERVER srv_soak OPTIONS (binding_mode 'create', lake_schema 'main')
                """)
                q(conn, f"INSERT INTO biz.lru_{i} VALUES ({i}, {i * 10}, 'v_{i}')")
                if i % 4 == 0:
                    q(conn, f"SELECT og_lake.flush('biz.lru_{i}'::regclass)")

            rss_after_setup_kb = total_gaussdb_rss_kb()

            # Evict/repopulate 64-entry SPI_keepplan LRU cache across 3 full sweeps (240+ queries)
            for sweep in range(3):
                idxs = list(range(80))
                random.Random(sweep).shuffle(idxs)
                for i in idxs:
                    cnt, sm = q(conn, f"SELECT count(*), sum(k) FROM biz.lru_{i}")[0]
                    assert cnt == 1 and sm == i * 10, (sweep, i, cnt, sm)

            # Invalidate plans via ALTER TYPE, RENAME, set_partition truncate, and DROP
            for i in range(0, 20, 2):
                q(conn, f"ALTER FOREIGN TABLE biz.lru_{i} ALTER COLUMN k TYPE bigint")
                q(conn, f"SELECT og_lake.set_partition('biz.lru_{i}'::regclass, ARRAY['k'], ARRAY['truncate(5)'])")
                q(conn, f"INSERT INTO biz.lru_{i} VALUES (1000 + {i}, 9999999999, 'wide')")
                res = q(conn, f"SELECT count(*), max(k) FROM biz.lru_{i}")[0]
                assert res == (2, 9999999999), (i, res)
            for i in range(1, 15, 2):
                q(conn, f"ALTER FOREIGN TABLE biz.lru_{i} RENAME TO lru_r_{i}")
                assert s(conn, f"SELECT k FROM biz.lru_r_{i}") == i * 10
                q(conn, f"DROP FOREIGN TABLE biz.lru_r_{i}")

            rss_after_lru_kb = total_gaussdb_rss_kb()

            # 5B. Concurrent writers + Vec DOP=4 readers + flush + GC + deliberate pg_cancel_backend
            q(conn, """
                CREATE FOREIGN TABLE biz.soak_mix (id integer, k integer, val text)
                SERVER srv_soak OPTIONS (binding_mode 'create', lake_schema 'main')
            """)
            q(conn, "SELECT og_lake.set_partition('biz.soak_mix'::regclass, ARRAY['k'], ARRAY['truncate(10)'])")
            q(conn, "INSERT INTO biz.soak_mix SELECT g, g % 100, 'init_' || g FROM generate_series(1, 2000) g")
            for _ in range(12):
                q(conn, "SELECT og_lake.flush('biz.soak_mix'::regclass)")

        cancel_hits = 0
        read_queries = 0
        occ_retries = 0
        committed_delta = 0
        stat_lock = threading.Lock()

        def writer_loop(worker_id: int):
            nonlocal occ_retries, committed_delta
            with closing(connect(db_name, autocommit=False)) as wc:
                for step in range(10):
                    row_id = 10000 + worker_id * 1000 + step * 20
                    for attempt in range(25):
                        try:
                            with wc.cursor() as cur:
                                vals = ','.join(f"({row_id + off}, {(row_id + off) % 100}, 'w{worker_id}_{step}')" for off in range(20))
                                cur.execute(f"INSERT INTO biz.soak_mix VALUES {vals}")
                                if step % 3 == 0:
                                    cur.execute("DELETE FROM biz.soak_mix WHERE id = %s", (row_id,))
                                    step_delta = 19
                                else:
                                    cur.execute("UPDATE biz.soak_mix SET val = 'upd' WHERE id = %s", (row_id,))
                                    step_delta = 20
                            wc.commit()
                            with stat_lock:
                                committed_delta += step_delta
                            break
                        except psycopg2.Error as exc:
                            wc.rollback()
                            if exc.pgcode in ('40001', '55P03') and attempt < 24:
                                with stat_lock:
                                    occ_retries += 1
                                time.sleep(0.015 * ((attempt % 4) + 1))
                                continue
                            raise
                    if worker_id == 0 and step % 4 == 3:
                        wc.autocommit = True
                        for attempt in range(10):
                            try:
                                q(wc, "SELECT og_lake.flush('biz.soak_mix'::regclass)")
                                break
                            except psycopg2.Error as exc:
                                if exc.pgcode in ('40001', '55P03') and attempt < 9:
                                    with stat_lock:
                                        occ_retries += 1
                                    time.sleep(0.02)
                                    continue
                                raise
                        wc.autocommit = False

        def reader_loop():
            nonlocal read_queries
            with closing(connect(db_name)) as rc:
                q(rc, "SET enable_vector_engine = on; SET try_vector_engine_strategy = 'force'; SET enable_lake_smp = on; SET enable_force_smp = off; SET query_dop = 4; SET enable_force_smp = on")
                for _ in range(25):
                    cnt, _ = q(rc, "SELECT count(*), sum(k) FROM biz.soak_mix WHERE k >= 10")[0]
                    assert cnt > 0
                    with stat_lock:
                        read_queries += 1

        def cancel_probe_loop():
            nonlocal cancel_hits
            with closing(connect(db_name)) as kc, closing(connect(db_name)) as target:
                t_pid, t_sid = q(target, 'SELECT pg_backend_pid(), pg_current_sessid()')[0]
                for _ in range(5):
                    with ThreadPoolExecutor(max_workers=1) as ex:
                        fut = ex.submit(q, target, "SELECT count(*) FROM biz.soak_mix a CROSS JOIN biz.soak_mix b WHERE (a.id + b.id) % 1009 = 0")
                        time.sleep(0.02)
                        q(kc, 'SELECT pg_cancel_session(%s, %s)', (t_pid, t_sid))
                        try:
                            fut.result(timeout=15)
                        except Exception:
                            cancel_hits += 1

        with ThreadPoolExecutor(max_workers=6) as pool:
            futs = [pool.submit(writer_loop, wid) for wid in range(3)]
            futs.append(pool.submit(reader_loop))
            futs.append(pool.submit(cancel_probe_loop))
            for f in futs:
                f.result()

        with closing(connect(db_name)) as conn:
            expected_rows = 2000 + committed_delta
            lake_cnt = s(conn, "SELECT count(*) FROM biz.soak_mix")
            assert lake_cnt == expected_rows, f'Expected {expected_rows} rows after concurrent writers, got {lake_cnt}'
            for _ in range(12):
                q(conn, "SELECT og_lake.flush('biz.soak_mix'::regclass)")
            head_snap = s(conn, "SELECT id FROM og_lake.current_snapshot('srv_soak'::name)")
            old_snaps = [r[0] for r in q(conn, 'SELECT snapshot_id FROM meta_soak.ducklake_snapshot WHERE snapshot_id < %s ORDER BY snapshot_id', (head_snap,))]
            if old_snaps:
                q(conn, "SELECT snapshot_id FROM og_lake.expire_snapshots('srv_soak'::name, %s::bigint[], NULL, false)", (old_snaps,))
            q(conn, "SELECT path, deleted, status FROM og_lake.cleanup_old_files('srv_soak'::name, NULL, true, true)")
            q(conn, "SELECT path, deleted, status FROM og_lake.delete_orphaned_files('srv_soak'::name, NULL, true, true)")
            assert s(conn, "SELECT count(*) FROM og_lake.active_snapshot_pins()") == 0
            assert s(conn, "SELECT count(*) FROM biz.soak_mix") == expected_rows

        rss_end_kb = total_gaussdb_rss_kb()
        return {
            'status': 'passed',
            'thread_pool': 'on',
            'lru_tables_churned': 80,
            'lru_query_sweeps': 3,
            'concurrent_vec_reads': read_queries,
            'occ_retries_handled': occ_retries,
            'cancel_probes_handled': cancel_hits,
            'final_soak_rows': expected_rows,
            'active_pins_after': 0,
            'rss_setup_kb': rss_after_setup_kb,
            'rss_after_lru_kb': rss_after_lru_kb,
            'rss_end_kb': rss_end_kb,
            'rss_delta_lru_to_end_kb': rss_end_kb - rss_after_lru_kb,
        }
    finally:
        with closing(connect()) as admin:
            q(admin, f'DROP DATABASE IF EXISTS {db_name}')
            if owner_role:
                q(admin, f'DROP ROLE IF EXISTS {owner_role}')
        subprocess.run(['rm', '-rf', str(root_dir)], check=False)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--build-receipt', type=Path, required=True)
    ap.add_argument('--out-dir', type=Path, default=VERIF / 'native_074_scale_soak_1121_01')
    args = ap.parse_args()

    args.out_dir.mkdir(parents=True, exist_ok=True)
    assert sha256_file(REL_SDK54_SO) == EXPECTED_SDK54
    assert sha256_file(REL_SDK6_SO) == EXPECTED_SDK6
    orig_conf = REL_CONF.read_bytes()
    assert hashlib.sha256(orig_conf).hexdigest() == EXPECTED_RELEASE_CONF

    build_meta = json.loads(args.build_receipt.read_text(encoding='utf-8'))
    run_stamp = int(time.time())
    try:
        subprocess.run(rel_driver('stop', f'{args.out_dir.name}_prestop_{run_stamp}'), cwd=str(ROOT), env=driver_env(), check=False)
        run_cmd(rel_driver('restart', f'{args.out_dir.name}_start_{run_stamp}', args.build_receipt), label='start release baseline')

        dv_res = run_datavec_regress(args.out_dir)
        hist_res = run_release_history_suites(args.build_receipt, args.out_dir, orig_conf)
        scale1m_res = bench_1m_scale(args.out_dir)
        pq20k_res = bench_pq_scale('rel_pq', 20000, n_files=4, dim=32)
        pq50k_res = bench_pq_scale('rel_pq', 50000, n_files=4, dim=32)
        soak_res = soak_threadpool_spi_cache_and_gc(args.build_receipt, orig_conf)
    finally:
        REL_CONF.write_bytes(orig_conf)
        os.chmod(REL_CONF, 0o600)
        assert sha256_file(REL_CONF) == EXPECTED_RELEASE_CONF
        subprocess.run(rel_driver('stop', f'{args.out_dir.name}_poststop_{int(time.time())}'), cwd=str(ROOT), env=driver_env(), check=False)

    assert sha256_file(REL_SDK54_SO) == EXPECTED_SDK54
    assert sha256_file(REL_SDK6_SO) == EXPECTED_SDK6
    pg = subprocess.run(['pgrep', '-xa', 'gaussdb'], capture_output=True, text=True)
    assert pg.returncode != 0, f'Leftover gaussdb:\n{pg.stdout}'

    summary = {
        'status': 'passed',
        'build_receipt': str(args.build_receipt),
        'build_receipt_sha256': sha256_file(args.build_receipt),
        'binary_sha256': build_meta['binary_sha256'],
        'datavec_regress': dv_res,
        'history_suites_on_release': list(hist_res.keys()),
        'scale_1m_rows': scale1m_res,
        'diskann_pq_20k_dim32': pq20k_res,
        'diskann_pq_50k_dim32': pq50k_res,
        'threadpool_soak_and_lru': soak_res,
    }
    idx = args.out_dir / 'index.json'
    idx.write_text(json.dumps(summary, indent=2, ensure_ascii=False) + '\n', encoding='utf-8')
    print(json.dumps(summary, indent=2, ensure_ascii=False))
    print(f'SAVED {idx} sha256={sha256_file(idx)}')


if __name__ == '__main__':
    main()
