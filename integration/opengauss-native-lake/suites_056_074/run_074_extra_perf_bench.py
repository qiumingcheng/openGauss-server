#!/usr/bin/env python3
"""Release (-O2) extended Vec full-col/count(*) and 8,000-row (4x2000) DiskANN+PQ cold+warm benchmark."""

import argparse
import hashlib
import json
import os
import shutil
import statistics
import tempfile
import time
import uuid
from pathlib import Path

import psycopg2

ROOT = Path('/home/qiumc/data/code/cpp')


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
    ap.add_argument('--profile', default='release', choices=['release', 'debug', 'ann-debug'])
    ap.add_argument('--out-dir', default=str(ROOT / '公共文档/openGauss原生湖存储/验证/native_074_extra_perf_1121_01'))
    args = ap.parse_args()

    tag = uuid.uuid4().hex[:10]
    tmp_root = Path(tempfile.mkdtemp(prefix=f'og_eperf_{tag}_', dir='/tmp'))
    os.chmod(tmp_root, 0o755)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    admin = pg_connect(args.profile, 'postgres')
    created_dbs = []
    created_roles = []
    conns = []
    try:
        role_main = f'ueperf_{tag}'
        db_main = f'dbeperf_{tag}'
        q(admin, f"CREATE ROLE {role_main} NOLOGIN NOINHERIT ENCRYPTED PASSWORD %s", (md5_role_pw('Eperf073!', role_main),))
        created_roles.append(role_main)
        q(admin, f"CREATE DATABASE {db_main} WITH DBCOMPATIBILITY 'PG' ENCODING 'UTF8'")
        created_dbs.append(db_main)

        cm = pg_connect(args.profile, db_main)
        conns.append(cm)
        q(cm, 'CREATE EXTENSION og_lake')
        q(cm, f"CREATE SCHEMA biz; GRANT USAGE ON SCHEMA biz TO {role_main}")
        q(cm, "SELECT og_lake.configure_internal_owner(%s::name)", (role_main,))
        cat_dir = tmp_root / 'cat'
        idx_dir = tmp_root / 'idx'
        idx_dir.mkdir(parents=True, exist_ok=True)
        os.chmod(idx_dir, 0o755)
        q(cm, "SELECT og_lake.create_catalog('c_ep'::name, 'lake_metadata'::name, %s::text, NULL::text)", (str(cat_dir),))
        q(cm, "SELECT og_lake.set_option('c_ep'::name, 'data_inlining_row_limit'::text, '50000'::text)")
        q(cm, "SELECT og_lake.set_option('c_ep'::name, 'index_root'::text, %s::text)", (str(idx_dir),))

        # Part 1: 60k rows — row vs vec on count(*), single-col sum(v), and all-column aggregation (incl payload text)
        q(cm, "CREATE FOREIGN TABLE biz.t_scan60 (id int8, grp int4, v float8, payload text) SERVER c_ep OPTIONS (binding_mode 'create', lake_schema 'main')")
        for b in range(4):
            q(cm, f"""
                INSERT INTO biz.t_scan60
                SELECT g::int8, (g % 20)::int4, (g * 0.25)::float8, 'payload_row_' || (g % 97) || '_' || repeat('x', 24)
                FROM generate_series({b * 15000 + 1}, {(b + 1) * 15000}) AS g
            """)
            q(cm, "SELECT og_lake.flush('biz.t_scan60'::regclass)")

        queries_60k = {
            'count_star': 'SELECT count(*) FROM biz.t_scan60',
            'proj_sum_v': 'SELECT count(*), round(sum(v)::numeric, 2) FROM biz.t_scan60 WHERE grp < 10',
            'full_all_cols': 'SELECT count(*), sum(id), sum(grp), round(sum(v)::numeric, 2), sum(length(payload)) FROM biz.t_scan60',
        }
        vec_vs_row = {}
        for name, sql in queries_60k.items():
            q(cm, "SET enable_vector_engine = off; SET query_dop = 1; SET enable_force_smp = off;")
            r_ms, r_val = time_query(cm, sql, runs=3, warmup=1)
            q(cm, "SET enable_vector_engine = on; SET try_vector_engine_strategy = 'force'; SET query_dop = 1; SET enable_force_smp = off;")
            v_ms, v_val = time_query(cm, sql, runs=3, warmup=1)
            assert r_val == v_val, (name, r_val, v_val)
            vec_vs_row[name] = {
                'row_ms': r_ms,
                'vec_ms': v_ms,
                'speedup': round(r_ms / v_ms, 3),
            }

        # Part 2: Larger 8,000-row (4 x 2,000) dim=32 DiskANN+PQ build + cold & steady-state DOP=1 / DOP=4 query latency
        q(cm, "CREATE FOREIGN TABLE biz.t_pq8k (id int4, vec vector(32)) SERVER c_ep OPTIONS (binding_mode 'create', lake_schema 'main')")
        for b in range(4):
            q(cm, f"""
                INSERT INTO biz.t_pq8k
                SELECT g, ('[' || string_agg((((((g * 37 + k * 13) % 20000) - 10000) / 10000.0))::text, ',') || ']')::vector(32)
                FROM generate_series({b * 2000 + 1}, {(b + 1) * 2000}) AS g, generate_series(0, 31) AS k
                GROUP BY g
            """)
            q(cm, "SELECT og_lake.flush('biz.t_pq8k'::regclass)")
        ix8k = s(cm, "SELECT og_lake.create_lake_index('biz.t_pq8k'::regclass, 'ix_pq8k'::name, '{\"version\":1,\"kind\":\"vector\",\"method\":\"diskann\",\"column\":\"vec\",\"metric\":\"l2\",\"pq\":true,\"pq_chunks\":8}'::jsonb)")
        q(cm, "SET statement_timeout = 900000")
        t0 = time.perf_counter()
        for _ in range(8):
            q(cm, "SELECT og_lake.build_lake_index(%s::uuid)", (ix8k,))
            if s(cm, "SELECT count(*) FROM og_lake_internal.index_segment WHERE index_uuid=%s::uuid AND state='READY' AND format_version=2", (ix8k,)) >= 4:
                break
        pq8k_build_ms = round((time.perf_counter() - t0) * 1000.0, 3)
        q(cm, "SET statement_timeout = 300000")
        assert s(cm, "SELECT count(*) FROM og_lake_internal.index_segment WHERE index_uuid=%s::uuid AND state='READY' AND format_version=2", (ix8k,)) == 4
        q(cm, "ANALYZE biz.t_pq8k")
        q(cm, "SET enable_vector_engine = off; SET try_vector_engine_strategy = 'off';")

        exact_times = []
        pq_cold_dop1 = []
        pq_warm_dop1 = []
        pq_warm_dop4 = []
        recalls = []
        for qi in range(5):
            vec_q = '[' + ','.join(str(round((((qi * 23 + k * 7) % 100) - 50) / 50.0, 3)) for k in range(32)) + ']'
            sql_q = f"SELECT id FROM biz.t_pq8k WHERE vec IS NOT NULL ORDER BY vec <-> '{vec_q}'::vector(32) LIMIT 10"
            q(cm, "SET enable_lake_ann = off; SET enable_seqscan = on; SET query_dop = 1; SET enable_force_smp = off;")
            if qi == 0:
                _ = q(cm, sql_q)
            t0 = time.perf_counter()
            gt_ids = [r[0] for r in q(cm, sql_q)]
            exact_times.append((time.perf_counter() - t0) * 1000.0)

            q(cm, "SET enable_lake_ann = on; SET ann_orderby_distance_reuse = off; SET query_max_mem = 0; SET enable_seqscan = off; SET query_dop = 1; SET enable_force_smp = off;")
            if qi == 0:
                exp1 = '\n'.join(r[0] for r in q(cm, f"EXPLAIN (ANALYZE, COSTS OFF) {sql_q}"))
                assert 'Verified PQ File Coverage: 4' in exp1, exp1
            t0 = time.perf_counter()
            pq_ids = [r[0] for r in q(cm, sql_q)]
            pq_cold_dop1.append((time.perf_counter() - t0) * 1000.0)
            t0 = time.perf_counter()
            pq_ids2 = [r[0] for r in q(cm, sql_q)]
            pq_warm_dop1.append((time.perf_counter() - t0) * 1000.0)
            recalls.append(len(set(gt_ids) & set(pq_ids2)) / 10.0)

            q(cm, "SET query_dop = 4; SET enable_force_smp = on; SET enable_lake_smp = on;")
            if qi == 0:
                exp4 = '\n'.join(r[0] for r in q(cm, f"EXPLAIN (ANALYZE, COSTS OFF) {sql_q}"))
                assert 'Lake SMP Planned DOP: 4' in exp4 and 'Lake SMP ANN PQ: admitted' in exp4, exp4
                _ = q(cm, sql_q)
            t0 = time.perf_counter()
            _ = q(cm, sql_q)
            pq_warm_dop4.append((time.perf_counter() - t0) * 1000.0)

        bench = {
            'status': 'passed',
            'profile': args.profile,
            'vec_vs_row_60k': vec_vs_row,
            'diskann_pq_8000_4x2000_dim32': {
                'build_ms': pq8k_build_ms,
                'mean_recall_at_10': round(statistics.mean(recalls), 4),
                'min_recall_at_10': round(min(recalls), 4),
                'exact_scan_median_ms': round(statistics.median(exact_times), 3),
                'pq_cold_dop1_median_ms': round(statistics.median(pq_cold_dop1), 3),
                'pq_warm_dop1_median_ms': round(statistics.median(pq_warm_dop1), 3),
                'pq_warm_dop4_median_ms': round(statistics.median(pq_warm_dop4), 3),
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
