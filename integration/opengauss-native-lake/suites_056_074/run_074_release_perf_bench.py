#!/usr/bin/env python3
"""Release performance, SMP scaling (DOP=1/2/4), DiskANN PQ recall@10, and flush timing benchmark."""

import argparse
import getpass
import hashlib
import json
import os
import shutil
import statistics
import tempfile
import threading
import time
import uuid
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
        cur.execute('SET statement_timeout = 300000')
    return conn


def q(conn, sql, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params)
        if cur.description:
            return cur.fetchall()
        return []


def s(conn, sql, params=None):
    rows = q(conn, sql, params)
    assert len(rows) == 1 and len(rows[0]) == 1
    return rows[0][0]


def time_query(conn, sql, runs=3, warmup=1):
    for _ in range(warmup):
        res = q(conn, sql)
    times = []
    for _ in range(runs):
        t0 = time.perf_counter()
        res = q(conn, sql)
        times.append((time.perf_counter() - t0) * 1000.0)
    return round(statistics.median(times), 3), res


def main():
    ap = argparse.ArgumentParser()
    add_connection_args(ap)
    ap.add_argument('--profile', default='release', choices=['release', 'debug', 'ann-debug'])
    ap.add_argument('--out-dir')
    args = ap.parse_args()
    configure_connection(args)
    args.out_dir = args.out_dir or str(ROOT / '公共文档/openGauss原生湖存储/验证' / ('native_074_release_perf_' + time.strftime('%Y%m%d_%H%M%S')))

    tag = uuid.uuid4().hex[:10]
    tmp_root = Path(tempfile.mkdtemp(prefix=f'og_perf_{tag}_', dir='/tmp'))
    os.chmod(tmp_root, 0o755)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    admin = pg_connect(args.profile, 'postgres')
    created_dbs = []
    created_roles = []
    conns = []

    try:
        role_main = f'uperf_{tag}'
        db_main = f'dbperf_{tag}'
        q(admin, f"CREATE ROLE {role_main} NOLOGIN NOINHERIT ENCRYPTED PASSWORD %s", (md5_role_pw('Perf071!', role_main),))
        created_roles.append(role_main)
        q(admin, f"CREATE DATABASE {db_main} WITH DBCOMPATIBILITY 'PG' ENCODING 'UTF8'")
        created_dbs.append(db_main)

        cm = pg_connect(args.profile, db_main)
        reader = pg_connect(args.profile, db_main)
        conns.extend([cm, reader])

        q(cm, 'CREATE EXTENSION og_lake')
        assert s(cm, "SELECT extversion FROM pg_extension WHERE extname='og_lake'") == args.expected_extension_version
        q(cm, f"CREATE SCHEMA biz; GRANT USAGE ON SCHEMA biz TO {role_main}")
        q(cm, "SELECT og_lake.configure_internal_owner(%s::name)", (role_main,))
        cat_dir = tmp_root / 'cat'
        idx_dir = tmp_root / 'idx'
        idx_dir.mkdir(parents=True, exist_ok=True)
        os.chmod(idx_dir, 0o755)
        q(cm, "SELECT og_lake.create_catalog('c_perf'::name, 'lake_metadata'::name, %s::text, NULL::text)", (str(cat_dir),))
        q(cm, "SELECT og_lake.set_option('c_perf'::name, 'data_inlining_row_limit'::text, '50000'::text)")
        q(cm, "SELECT og_lake.set_option('c_perf'::name, 'index_root'::text, %s::text)", (str(idx_dir),))

        # 1. COPY IO throughput & Inline vs Parquet vs Vectorized scan (60,000 rows in 4 files of 15,000)
        q(cm, """
            CREATE FOREIGN TABLE biz.t_scan (
                id bigint, grp int4, v1 bigint, v2 double precision, flag boolean, payload text
            ) SERVER c_perf OPTIONS (binding_mode 'create', lake_schema 'main')
        """)
        n_batch = 15000
        for b in range(4):
            q(cm, f"""
                INSERT INTO biz.t_scan
                SELECT g, (g % 16), (g * 7)::bigint, (g % 1000) * 0.25, (g % 2 = 0), 'item_' || (g % 100)
                FROM generate_series({b * n_batch + 1}, {(b + 1) * n_batch}) AS g
            """)
            if b < 3:
                q(cm, "SELECT og_lake.flush('biz.t_scan'::regclass)")
        # Inline batch (45k..60k) holds 15k inline rows alongside 45k in 3 Parquet files; measure flush too!
        t0 = time.perf_counter()
        q(cm, "SELECT og_lake.flush('biz.t_scan'::regclass)")
        flush_15k_ms = round((time.perf_counter() - t0) * 1000.0, 3)

        # Add another batch for inline + file mixed & pure-Parquet scans; measure COPY TO / FROM JSONL & Parquet
        scan_sql = "SELECT count(*), sum(v1), avg(v2) FROM biz.t_scan WHERE grp BETWEEN 2 AND 9 AND flag"
        q(cm, "SET query_dop = 1")
        q(cm, "SET enable_force_smp = off")
        q(cm, "SET enable_lake_smp = off")
        q(cm, "SET enable_vector_engine = off")
        q(cm, "SET try_vector_engine_strategy = 'off'")
        row_scan_ms, row_res = time_query(cm, scan_sql)
        q(cm, "SET enable_vector_engine = on")
        q(cm, "SET try_vector_engine_strategy = 'force'")
        exp_vec = '\n'.join(r[0] for r in q(cm, 'EXPLAIN (COSTS OFF) ' + scan_sql))
        assert 'Vector' in exp_vec or 'Vec' in exp_vec, exp_vec
        vec_scan_ms, vec_res = time_query(cm, scan_sql)
        q(cm, "SET try_vector_engine_strategy = 'off'")
        q(cm, "SET enable_vector_engine = off")
        assert row_res == vec_res, (row_res, vec_res)

        jsonl_path = tmp_root / 'bench_60k.jsonl'
        pq_path = tmp_root / 'bench_60k.parquet'
        t0 = time.perf_counter()
        q(cm, f"COPY biz.t_scan TO '{jsonl_path}' (FORMAT 'jsonl')")
        copy_to_jsonl_ms = round((time.perf_counter() - t0) * 1000.0, 3)

        t0 = time.perf_counter()
        q(cm, f"COPY biz.t_scan TO '{pq_path}' (FORMAT 'parquet', COMPRESSION 'zstd')")
        copy_to_pq_ms = round((time.perf_counter() - t0) * 1000.0, 3)

        q(cm, """
            CREATE FOREIGN TABLE biz.t_in_json (
                id bigint, grp int4, v1 bigint, v2 double precision, flag boolean, payload text
            ) SERVER c_perf OPTIONS (binding_mode 'create', lake_schema 'main')
        """)
        q(cm, """
            CREATE FOREIGN TABLE biz.t_in_pq (
                id bigint, grp int4, v1 bigint, v2 double precision, flag boolean, payload text
            ) SERVER c_perf OPTIONS (binding_mode 'create', lake_schema 'main')
        """)
        t0 = time.perf_counter()
        q(cm, f"COPY biz.t_in_json FROM '{jsonl_path}' (FORMAT 'json')")
        copy_from_jsonl_ms = round((time.perf_counter() - t0) * 1000.0, 3)
        t0 = time.perf_counter()
        q(cm, f"COPY biz.t_in_pq FROM '{pq_path}' (FORMAT 'parquet')")
        copy_from_pq_ms = round((time.perf_counter() - t0) * 1000.0, 3)
        assert s(cm, "SELECT count(*) FROM biz.t_in_json") == 60000
        assert s(cm, "SELECT count(*) FROM biz.t_in_pq") == 60000

        # 2. SMP Scaling Benchmark (8 x 30k = 240,000 rows in 8 Parquet files, natural cost DOP 1 vs 2 vs 4)
        q(cm, """
            CREATE FOREIGN TABLE biz.t_smp_scale (
                id bigint, k int4, a bigint, b double precision, tag text
            ) SERVER c_perf OPTIONS (binding_mode 'create', lake_schema 'main')
        """)
        for b in range(8):
            q(cm, f"""
                INSERT INTO biz.t_smp_scale
                SELECT g, (g % 32), (g * 13)::bigint, ((g * 17) % 997)::float8 / 3.0, md5(g::text)
                FROM generate_series({b * 30000 + 1}, {(b + 1) * 30000}) AS g
            """)
            q(cm, "SELECT og_lake.flush('biz.t_smp_scale'::regclass)")

        q(cm, "SET enable_vector_engine = off")
        q(cm, "SET enable_lake_smp = on")
        q(cm, "SET enable_force_smp = off")
        smp_sql = "SELECT count(*), sum(a), avg(b) FROM biz.t_smp_scale WHERE k < 16 AND length(tag) = 32"
        smp_timings = {}
        ref_smp_res = None
        for dop in (1, 2, 4):
            q(cm, f"SET query_dop = {dop}")
            ms, res = time_query(cm, smp_sql, runs=3, warmup=1)
            if ref_smp_res is None:
                ref_smp_res = res
            else:
                assert res == ref_smp_res, (dop, res, ref_smp_res)
            if dop > 1:
                exp = '\n'.join(r[0] for r in q(cm, 'EXPLAIN (ANALYZE, COSTS OFF) ' + smp_sql))
                assert f'Lake SMP Planned DOP: {dop}' in exp, (dop, exp)
            smp_timings[f'dop{dop}_ms'] = ms
        smp_timings['speedup_dop2'] = round(smp_timings['dop1_ms'] / smp_timings['dop2_ms'], 3)
        smp_timings['speedup_dop4'] = round(smp_timings['dop1_ms'] / smp_timings['dop4_ms'], 3)

        # Also benchmark Vectorized VecForeignScan + SMP (DOP 1, 2, 4) on exact same 240k 8-file table!
        q(cm, "SET enable_vector_engine = on")
        q(cm, "SET try_vector_engine_strategy = 'force'")
        q(cm, "SET enable_lake_smp = on")
        vec_smp_timings = {}
        for dop in (1, 2, 4):
            q(cm, f"SET query_dop = {dop}")
            q(cm, f"SET enable_force_smp = {'on' if dop > 1 else 'off'}")
            ms, res = time_query(cm, smp_sql, runs=3, warmup=1)
            assert res == ref_smp_res, (dop, res, ref_smp_res)
            exp = '\n'.join(r[0] for r in q(cm, 'EXPLAIN (ANALYZE, COSTS OFF) ' + smp_sql))
            assert 'Lake Batch Delivery: native VecForeignScan / owned scalar VectorBatch' in exp, (dop, exp)
            if dop > 1:
                assert f'Lake SMP Planned DOP: {dop}' in exp, (dop, exp)
            vec_smp_timings[f'vec_dop{dop}_ms'] = ms
        vec_smp_timings['vec_speedup_vs_row_dop1'] = round(smp_timings['dop1_ms'] / vec_smp_timings['vec_dop1_ms'], 3)
        vec_smp_timings['vec_speedup_dop2'] = round(vec_smp_timings['vec_dop1_ms'] / vec_smp_timings['vec_dop2_ms'], 3)
        vec_smp_timings['vec_speedup_dop4'] = round(vec_smp_timings['vec_dop1_ms'] / vec_smp_timings['vec_dop4_ms'], 3)
        vec_smp_timings['vec_dop4_vs_row_dop1'] = round(smp_timings['dop1_ms'] / vec_smp_timings['vec_dop4_ms'], 3)
        q(cm, "SET try_vector_engine_strategy = 'off'")
        q(cm, "SET enable_vector_engine = off")
        q(cm, "SET enable_force_smp = off")

        print('[stage 2] 240k 8-file SMP scaling (Row + Vec) done', smp_timings, vec_smp_timings, flush=True)

        # 3. DiskANN PQ Recall@10 vs Exact Brute-Force & ANN Latency (4 files x 300 = 1,200 vectors dim=32, >=256/file for real 256-centroid PQ)
        q(cm, "SET query_dop = 1")
        q(cm, """
            CREATE FOREIGN TABLE biz.t_pq_bench (
                id int4, grp int4, vec vector(32)
            ) SERVER c_perf OPTIONS (binding_mode 'create', lake_schema 'main')
        """)
        for b in range(4):
            q(cm, f"""
                INSERT INTO biz.t_pq_bench
                SELECT g, (g % 8),
                       ('[' || (SELECT string_agg((((g * 31 + d * 17 + (g % 19) * d) % 997) / 500.0 - 1.0)::text, ',')
                                FROM generate_series(1, 32) AS d) || ']')::vector(32)
                FROM generate_series({b * 300 + 1}, {(b + 1) * 300}) AS g
            """)
            q(cm, "SELECT og_lake.flush('biz.t_pq_bench'::regclass)")

        ix_pq = s(cm, "SELECT og_lake.create_lake_index('biz.t_pq_bench'::regclass, 'ix_pq_32'::name, '{\"version\":1,\"kind\":\"vector\",\"method\":\"diskann\",\"column\":\"vec\",\"metric\":\"l2\",\"pq\":true,\"pq_chunks\":8}'::jsonb)")
        q(cm, "SET statement_timeout = 900000")
        t0 = time.perf_counter()
        print('[stage 3] building DiskANN+PQ segments 1..3/4', flush=True)
        q(cm, "SELECT og_lake.build_lake_index(%s::uuid)", (ix_pq,))
        print('[stage 3] building DiskANN+PQ segment 4/4', flush=True)
        q(cm, "SELECT og_lake.build_lake_index(%s::uuid)", (ix_pq,))
        pq_build_ms = round((time.perf_counter() - t0) * 1000.0, 3)
        print(f'[stage 3] DiskANN+PQ build_ms={pq_build_ms}', flush=True)
        q(cm, "SET statement_timeout = 300000")
        fmt2_cnt = s(cm, "SELECT count(*) FROM og_lake_internal.index_segment WHERE index_uuid=%s::uuid AND state='READY' AND format_version=2", (ix_pq,))
        assert fmt2_cnt == 4, f'Expected 4 format2 segments, got {fmt2_cnt}'

        queries = []
        for qi in range(10):
            vec_q = '[' + ','.join(f'{(((qi * 73 + d * 29) % 401) / 200.0 - 1.0):.4f}' for d in range(1, 33)) + ']'
            queries.append(vec_q)

        recalls = []
        exact_times = []
        pq_dop1_times = []
        pq_dop4_times = []
        q(cm, "SET enable_vector_engine = off")
        q(cm, "SET enable_lake_smp = on")
        q(cm, "SET ann_orderby_distance_reuse = off")
        for qi, vec_q in enumerate(queries):
            q(cm, "SET query_dop = 1")
            q(cm, "SET enable_force_smp = off")
            q(cm, "SET enable_lake_ann = off")
            q(cm, "SET enable_seqscan = on")
            t0 = time.perf_counter()
            gt_ids = [r[0] for r in q(cm, f"SELECT id FROM biz.t_pq_bench WHERE vec IS NOT NULL ORDER BY vec <-> '{vec_q}'::vector(32) LIMIT 10")]
            exact_times.append((time.perf_counter() - t0) * 1000.0)

            q(cm, "SET enable_lake_ann = on")
            q(cm, "SET enable_seqscan = off")
            q(cm, "SET query_max_mem = 0")
            t0 = time.perf_counter()
            pq_ids = [r[0] for r in q(cm, f"SELECT id FROM biz.t_pq_bench WHERE vec IS NOT NULL ORDER BY vec <-> '{vec_q}'::vector(32) LIMIT 10")]
            pq_dop1_times.append((time.perf_counter() - t0) * 1000.0)
            recalls.append(len(set(gt_ids) & set(pq_ids)) / 10.0)

            q(cm, "SET query_dop = 4")
            q(cm, "SET enable_force_smp = on")
            if qi == 0:
                exp_pq = '\n'.join(r[0] for r in q(cm, f"EXPLAIN (ANALYZE, COSTS OFF) SELECT id FROM biz.t_pq_bench WHERE vec IS NOT NULL ORDER BY vec <-> '{vec_q}'::vector(32) LIMIT 10"))
                assert 'Lake SMP Planned DOP: 4' in exp_pq and 'Lake SMP ANN PQ: admitted' in exp_pq, exp_pq
            t0 = time.perf_counter()
            q(cm, f"SELECT id FROM biz.t_pq_bench WHERE vec IS NOT NULL ORDER BY vec <-> '{vec_q}'::vector(32) LIMIT 10")
            pq_dop4_times.append((time.perf_counter() - t0) * 1000.0)

        # 4. Concurrent Flush Read Latency Check (20k inline rows flushed while reader runs count queries)
        q(cm, "SET query_dop = 1")
        q(cm, "SET enable_force_smp = off")
        q(cm, """
            CREATE FOREIGN TABLE biz.t_flush_lock (id int4, val int4)
            SERVER c_perf OPTIONS (binding_mode 'create', lake_schema 'main')
        """)
        q(cm, "INSERT INTO biz.t_flush_lock SELECT g, g * 2 FROM generate_series(1, 20000) AS g")
        read_latencies = []
        stop_flag = threading.Event()
        read_errors = []

        def reader_loop():
            try:
                while not stop_flag.is_set():
                    rt0 = time.perf_counter()
                    _ = s(reader, "SELECT count(*) FROM biz.t_flush_lock")
                    read_latencies.append((time.perf_counter() - rt0) * 1000.0)
                    time.sleep(0.005)
            except Exception as exc:
                read_errors.append(str(exc))

        th = threading.Thread(target=reader_loop, daemon=True)
        th.start()
        t0 = time.perf_counter()
        q(cm, "SELECT og_lake.flush('biz.t_flush_lock'::regclass)")
        flush_20k_ms = round((time.perf_counter() - t0) * 1000.0, 3)
        stop_flag.set()
        th.join(timeout=5)
        assert not read_errors, read_errors

        bench = {
            'status': 'passed',
            'profile': args.profile,
            'scan_and_io_60k_rows': {
                'flush_15k_ms': flush_15k_ms,
                'row_store_scan_ms': row_scan_ms,
                'vectorized_scan_ms': vec_scan_ms,
                'vectorized_speedup': round(row_scan_ms / vec_scan_ms, 3),
                'copy_to_jsonl_ms': copy_to_jsonl_ms,
                'copy_to_jsonl_bytes': jsonl_path.stat().st_size,
                'copy_to_parquet_ms': copy_to_pq_ms,
                'copy_to_parquet_bytes': pq_path.stat().st_size,
                'copy_from_jsonl_ms': copy_from_jsonl_ms,
                'copy_from_parquet_ms': copy_from_pq_ms,
            },
            'smp_scaling_240k_rows_8files': smp_timings,
            'vec_smp_scaling_240k_rows_8files': vec_smp_timings,
            'diskann_pq_1200_dim32': {
                'build_ms': pq_build_ms,
                'mean_recall_at_10': round(statistics.mean(recalls), 4),
                'min_recall_at_10': round(min(recalls), 4),
                'exact_scan_median_ms': round(statistics.median(exact_times), 3),
                'pq_dop1_median_ms': round(statistics.median(pq_dop1_times), 3),
                'pq_dop4_median_ms': round(statistics.median(pq_dop4_times), 3),
            },
            'flush_lock_contention_20k_rows': {
                'flush_ms': flush_20k_ms,
                'concurrent_reads': len(read_latencies),
                'max_concurrent_read_ms': round(max(read_latencies), 3) if read_latencies else None,
                'median_concurrent_read_ms': round(statistics.median(read_latencies), 3) if read_latencies else None,
            },
        }
        out_path = out_dir / 'index.json'
        out_path.write_text(json.dumps(bench, indent=2, ensure_ascii=False) + '\n', encoding='utf-8')
        print(json.dumps(bench, indent=2, ensure_ascii=False))
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


if __name__ == '__main__':
    main()
