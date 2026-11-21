#!/usr/bin/env python3
"""Comprehensive SQL0.74 verification suite:
- Part A: High-dim (32D..1536D) DiskANN+PQ build, recall@10, warm/cold latency,
          auto pq_chunks, ANALYZE budget, and natural enable_seqscan=on plan selection.
- Part B: active-H empty-input cleanup recovery & concurrency stress.
- Part C: PGL-0 expanded native pg_partition (RANGE 4+ leaves, LIST, HASH,
          extended column/key types, DML, COPY, ANALYZE, row movement,
          0.73->0.74 upgrade, and pre-0.74 contract isolation).
"""
import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from copy import deepcopy
from pathlib import Path

import numpy as np
import psycopg2

ROOT = Path('/home/qiumc/data/code/cpp')
DRIVER = ROOT / 'integration/opengauss-native-lake'
sys.path.insert(0, str(DRIVER))
sys.path.insert(0, str(DRIVER / 'tests'))
from empty_catalog_oracle import canonical_shape, read_native_catalog, transport_native
from empty_reference_fixture import export_actual_tables
from inline_fixture import replace_catalog_values
from reverse_reference import literal


class _DuckRef:
    def __init__(self, out_dir: Path):
        self.binary = ROOT / '.reference/opengauss-native-lake/build/duckdb-gcc10/duckdb'
        self.output = out_dir
        self.counter = 0

    def run(self, sql_text, name, catalog=None, readonly=False, result=False):
        self.counter += 1
        argv = [str(self.binary), '-no-init', '-batch', '-json']
        if readonly:
            argv.append('-readonly')
        if catalog:
            argv.append(str(catalog))
        proc = subprocess.run(argv, input='SET threads=1;\n' + sql_text, text=True,
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=180)
        if proc.returncode != 0:
            raise RuntimeError(f'DuckDB failed ({name}): {proc.stderr}\nSQL:\n{sql_text}')
        if result:
            return json.loads(proc.stdout) if proc.stdout.strip() else []
        return None


def seed_unbound_duckdb_tables(profile: str, db: str, tmp_root: Path, table_specs):
    """Use pinned reference DuckDB DuckLake 1.1 + offline catalog transport to create
    genuine unbound DuckLake tables in lake_metadata (end_snapshot IS NULL).
    table_specs: list of (table_name, duckdb_columns_ddl)
    Returns {table_name: table_id}.
    """
    obs = connect_profile(profile, db)
    obs.autocommit = False
    try:
        with obs.cursor() as c:
            c.execute("SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='lake_metadata' AND c.relkind='r' ORDER BY c.relname")
            tnames = [r[0] for r in c.fetchall()]
        obs.rollback()
        ref_dir = tmp_root / f'duck_seed_{uuid.uuid4().hex[:6]}'
        ref_dir.mkdir(parents=True, exist_ok=True)
        expected = {'tables': [{'table_name': n} for n in tnames]}
        before = read_native_catalog(obs, 'lake_metadata', expected, ref_dir / 'before.json')
        ref = _DuckRef(ref_dir)
        cat_path = ref_dir / 'src.ducklake'
        transport_native(ref, cat_path, before)
        stmts = [
            f"ATTACH {literal('ducklake:' + str(cat_path))} AS lake (DUCKLAKE_VERSION '1.1', AUTOMATIC_MIGRATION FALSE, DATA_INLINING_ROW_LIMIT 0);",
            "BEGIN;"
        ]
        for tname, ddl in table_specs:
            stmts.append(f"CREATE TABLE lake.main.{tname}({ddl});")
        stmts.append("COMMIT;")
        ref.run('\n'.join(stmts), 'create-seed-tables')
        actual_tables = export_actual_tables(ref, cat_path)
        prev = {x['table_name']: x for x in before['tables']}
        adapted = []
        for x in actual_tables:
            old = prev[x['table_name']]
            assert canonical_shape(x) == canonical_shape(old), x['table_name']
            adapted.append(dict(x, columns=deepcopy(old['columns']), constraints=deepcopy(old['constraints'])))
        with obs.cursor() as c:
            c.execute('BEGIN')
            replace_catalog_values(obs, 'lake_metadata', before, {'tables': adapted})
            obs.commit()
        with obs.cursor() as c:
            c.execute("SELECT table_name, table_id FROM lake_metadata.ducklake_table WHERE end_snapshot IS NULL")
            id_map = {r[0]: r[1] for r in c.fetchall()}
        obs.rollback()
        return {name: id_map[name] for name, _ in table_specs}
    finally:
        obs.close()


def connect_profile(profile: str, dbname: str = 'postgres'):
    sock = str(ROOT / '.runtime/opengauss-native-lake' / profile / 'socket')
    port = {'debug': 55560, 'release': 55570, 'ann-debug': 55580}[profile]
    conn = psycopg2.connect(host=sock, port=port, dbname=dbname, user='qiumc', connect_timeout=15)
    conn.autocommit = True
    return conn


def setup_test_db(profile: str, ext_version: str = '0.74'):
    tag = uuid.uuid4().hex[:8]
    tmp_root = Path(tempfile.mkdtemp(prefix=f'og_v074_{tag}_', dir='/tmp'))
    os.chmod(tmp_root, 0o755)
    role = f'u074_{tag}'
    db = f'db074_{tag}'
    admin = connect_profile(profile, 'postgres')
    with admin.cursor() as cur:
        pw = 'md5' + hashlib.md5(f'Test074!{role}'.encode()).hexdigest()
        cur.execute(f"CREATE ROLE {role} NOLOGIN NOINHERIT ENCRYPTED PASSWORD %s", (pw,))
        cur.execute(f"CREATE DATABASE {db} WITH DBCOMPATIBILITY 'PG' ENCODING 'UTF8'")
    admin.close()

    conn = connect_profile(profile, db)
    with conn.cursor() as cur:
        cur.execute('SET statement_timeout=300000')
        cur.execute('SET session_timeout=0')
        cur.execute('SET query_dop=1')
        cur.execute(f"CREATE EXTENSION og_lake VERSION '{ext_version}'")
        cur.execute(f"CREATE SCHEMA biz; GRANT USAGE, CREATE ON SCHEMA biz TO {role}")
        cur.execute('SELECT og_lake.configure_internal_owner(%s::name)', (role,))
        cat_dir = tmp_root / 'cat'
        idx_dir = tmp_root / 'idx'
        idx_dir.mkdir(parents=True, exist_ok=True)
        os.chmod(idx_dir, 0o755)
        cur.execute("SELECT og_lake.create_catalog('c_test'::name, 'lake_metadata'::name, %s::text, NULL::text)", (str(cat_dir),))
        cur.execute("SELECT og_lake.set_option('c_test'::name, 'index_root'::text, %s::text)", (str(idx_dir),))
    return conn, db, role, tmp_root, idx_dir


def teardown_test_db(profile: str, conn, db: str, role: str, tmp_root: Path):
    if conn is not None:
        try:
            conn.close()
        except Exception:
            pass
    admin = connect_profile(profile, 'postgres')
    with admin.cursor() as cur:
        cur.execute(f'DROP DATABASE IF EXISTS {db}')
        cur.execute(f'DROP ROLE IF EXISTS {role}')
    admin.close()
    shutil.rmtree(tmp_root, ignore_errors=True)


def run_ann_suite(profile: str, dims=(32, 128, 384, 768, 1536)):
    print(f'\n=== [Part A] High-Dim DiskANN+PQ Suite ({profile}) ===', flush=True)
    results = []
    conn, db, role, tmp_root, idx_dir = setup_test_db(profile)
    try:
        rng = np.random.default_rng(2026)
        with conn.cursor() as cur:
            # 1. Verify tiny 100-row table stays on SeqScan naturally
            cur.execute("CREATE FOREIGN TABLE biz.tiny(id int, v vector(32)) SERVER c_test OPTIONS (binding_mode 'create', lake_schema 'main')")
            cur.execute("SELECT og_lake.set_option('biz.tiny'::regclass, 'data_inlining_row_limit', '100000')")
            vals = ','.join(f"({i},'[{','.join(f'{x:.4f}' for x in rng.normal(size=32))}]'::vector(32))" for i in range(100))
            cur.execute(f'INSERT INTO biz.tiny VALUES {vals}')
            cur.execute("SELECT og_lake.flush('biz.tiny'::regclass)")
            cur.execute("ANALYZE biz.tiny")
            cur.execute("""SELECT og_lake.create_lake_index('biz.tiny'::regclass, 'ix_tiny',
                '{"version":1,"kind":"vector","method":"diskann","column":"v","metric":"l2"}'::jsonb)""")
            tiny_u = cur.fetchone()[0]
            cur.execute('SELECT og_lake.build_lake_index(%s::uuid)', (tiny_u,))
            q0 = '[' + ','.join(f'{x:.4f}' for x in rng.normal(size=32)) + ']'
            cur.execute('SET enable_seqscan=on; SET enable_lake_ann=on')
            cur.execute(f"EXPLAIN (COSTS ON) SELECT id FROM biz.tiny WHERE v IS NOT NULL ORDER BY v <-> '{q0}'::vector(32) LIMIT 10")
            tiny_plan = '\n'.join(r[0] for r in cur.fetchall())
            print('Tiny 100-row EXPLAIN:\n' + tiny_plan, flush=True)
            assert 'Verified ANN File Coverage' not in tiny_plan and 'Verified PQ File Coverage' not in tiny_plan, f'Tiny 100-row unexpectedly used ANN!\n{tiny_plan}'

            # 2. Test 32D..1536D clustered 20k (4 files x 5k rows) + l2/cosine/ip
            for dim in dims:
                metric = 'cosine' if dim == 384 else 'l2'
                op = '<=>' if metric == 'cosine' else '<->'
                tname = f'biz.t_{dim}d'
                n_total = 12000 if dim == 1536 else 20000
                per_file = n_total // 4
                cur.execute(f"CREATE FOREIGN TABLE {tname}(id bigint, tag int, v vector({dim})) SERVER c_test OPTIONS (binding_mode 'create', lake_schema 'main')")
                cur.execute("SELECT og_lake.set_option(%s::regclass, 'data_inlining_row_limit', '100000')", (tname,))

                centers = rng.normal(0.0, 2.5, size=(32, dim)).astype(np.float32)
                cids = rng.integers(0, 32, size=n_total)
                vecs = centers[cids] + rng.normal(0.0, 0.35, size=(n_total, dim)).astype(np.float32)

                t_ins0 = time.monotonic()
                for file_idx in range(4):
                    s0 = file_idx * per_file
                    s1 = (file_idx + 1) * per_file
                    step = 500 if dim <= 384 else 200
                    cur.execute('BEGIN')
                    for st in range(s0, s1, step):
                        en = min(st + step, s1)
                        chunk_vals = ','.join(
                            f"({i},{i % 10},'[{','.join(f'{x:.5f}' for x in vecs[i])}]'::vector({dim}))"
                            for i in range(st, en)
                        )
                        cur.execute(f'INSERT INTO {tname} VALUES {chunk_vals}')
                    cur.execute('COMMIT')
                    cur.execute('SELECT og_lake.flush(%s::regclass)', (tname,))
                ins_s = time.monotonic() - t_ins0

                # ANALYZE check (previously crashed >= 768D!)
                t_ana0 = time.monotonic()
                cur.execute(f'ANALYZE {tname}')
                ana_ms = (time.monotonic() - t_ana0) * 1000.0

                cur.execute('SELECT count(*) FROM lake_metadata.ducklake_data_file df JOIN lake_metadata.ducklake_table dt USING(table_id) WHERE dt.table_name=%s AND df.end_snapshot IS NULL', (f't_{dim}d',))
                n_files = cur.fetchone()[0]
                assert n_files == 4, f'Expected 4 files for {tname}, got {n_files}'

                # Create PQ index WITHOUT explicit pq_chunks to verify smart auto-default (dim/4)
                spec_pq = json.dumps({'version': 1, 'kind': 'vector', 'method': 'diskann', 'column': 'v', 'metric': metric, 'pq': True})
                cur.execute('SELECT og_lake.create_lake_index(%s::regclass, %s, %s::jsonb)', (tname, f'ix_pq_{dim}', spec_pq))
                pq_uuid = cur.fetchone()[0]
                t_bld0 = time.monotonic()
                for _ in range(8):
                    cur.execute('SELECT og_lake.build_lake_index(%s::uuid)', (pq_uuid,))
                    cur.execute('SELECT og_lake.lake_index_status(%s::uuid)::text', (pq_uuid,))
                    st_json = json.loads(cur.fetchone()[0])
                    if st_json.get('file_uncovered_inputs', 0) == 0:
                        break
                pq_build_ms = (time.monotonic() - t_bld0) * 1000.0
                expected_chunks = dim // 4
                assert st_json['ready_segments'] == 4, st_json
                assert st_json['pq_ready_segments'] == 4, st_json
                cur.execute("SELECT definition->>'pq_chunks' FROM og_lake_internal.index_definition WHERE index_uuid=%s::uuid", (pq_uuid,))
                actual_chunks = int(cur.fetchone()[0])
                assert actual_chunks == expected_chunks, f'dim={dim}: got chunks={actual_chunks}, want={expected_chunks}'

                # Queries + exact ground truth
                qvecs = [centers[rng.integers(0, 32)] + rng.normal(0.0, 0.25, size=dim).astype(np.float32) for _ in range(15)]
                qlits = ['[' + ','.join(f'{x:.5f}' for x in q) + ']' for q in qvecs]
                if metric == 'cosine':
                    vnorm = vecs / np.maximum(np.linalg.norm(vecs, axis=1, keepdims=True), 1e-12)
                    truths = [
                        set(int(x) for x in np.argsort(-(vnorm @ (q / max(np.linalg.norm(q), 1e-12))))[:10])
                        for q in qvecs
                    ]
                else:
                    truths = [set(int(x) for x in np.argsort(np.sum((vecs - q) ** 2, axis=1))[:10]) for q in qvecs]

                where_base = 'v IS NOT NULL' + (' AND vector_norm(v)>0' if metric == 'cosine' else '')

                # Exact seq scan baseline
                cur.execute('SET enable_lake_ann=off; SET enable_seqscan=on')
                t0 = time.monotonic()
                for ql in qlits[:5]:
                    cur.execute(f"SELECT id FROM {tname} WHERE {where_base} ORDER BY v {op} '{ql}'::vector({dim}) LIMIT 10")
                    cur.fetchall()
                exact_ms = (time.monotonic() - t0) * 1000.0 / 5.0

                # Natural planner selection check (enable_seqscan=on, enable_lake_ann=on)
                cur.execute('SET enable_lake_ann=on; SET enable_seqscan=on')
                cur.execute(f"EXPLAIN (ANALYZE, COSTS ON) SELECT id FROM {tname} WHERE {where_base} ORDER BY v {op} '{qlits[0]}'::vector({dim}) LIMIT 10")
                nat_plan = '\n'.join(r[0] for r in cur.fetchall())
                assert 'Verified PQ File Coverage: 4' in nat_plan or 'Verified ANN File Coverage: 4' in nat_plan, f'Natural ANN not chosen for {tname}!\n{nat_plan}'

                # Cold & Warm PQ ANN + recall
                recalls = []
                lat_ms = []
                for qi, ql in enumerate(qlits):
                    t0 = time.monotonic()
                    cur.execute(f"SELECT id FROM {tname} WHERE {where_base} ORDER BY v {op} '{ql}'::vector({dim}) LIMIT 10")
                    got = set(r[0] for r in cur.fetchall())
                    lat_ms.append((time.monotonic() - t0) * 1000.0)
                    recalls.append(len(got & truths[qi]) / 10.0)

                # Filtered ANN check
                cur.execute(f"EXPLAIN (ANALYZE, COSTS OFF) SELECT id FROM {tname} WHERE {where_base} AND tag=7 ORDER BY v {op} '{qlits[0]}'::vector({dim}) LIMIT 10")
                flt_plan = '\n'.join(r[0] for r in cur.fetchall())
                assert 'Verified PQ File Coverage: 4' in flt_plan or 'Verified ANN File Coverage: 4' in flt_plan, f'Filtered ANN not active:\n{flt_plan}'
                cur.execute(f"SELECT id FROM {tname} WHERE {where_base} AND tag=7 ORDER BY v {op} '{qlits[0]}'::vector({dim}) LIMIT 10")
                flt_rows = [r[0] for r in cur.fetchall()]
                assert len(flt_rows) == 10 and all(rid % 10 == 7 for rid in flt_rows)

                mean_recall = float(np.mean(recalls))
                warm_ms = float(np.mean(lat_ms[1:]))
                cold_ms = lat_ms[0]
                rec = dict(
                    dim=dim, rows=n_total, files=4, metric=metric, pq_chunks=actual_chunks,
                    insert_flush_s=round(ins_s, 2), analyze_ms=round(ana_ms, 1),
                    pq_build_ms=round(pq_build_ms, 1), exact_ms=round(exact_ms, 2),
                    pq_cold_ms=round(cold_ms, 2), pq_warm_ms=round(warm_ms, 2),
                    mean_recall10=round(mean_recall, 4), min_recall10=round(min(recalls), 4),
                    natural_seqscan_on_chose_ann=True, filtered_ok=True,
                )
                print(json.dumps(rec), flush=True)
                assert mean_recall >= 0.88, f'Recall too low for {dim}D ({metric}): {mean_recall}'
                results.append(rec)

            # 3. Also test IP metric explicitly at 128D & raw DiskANN (pq=false) caching + recall
            cur.execute("CREATE FOREIGN TABLE biz.t_ip_128(id bigint, v vector(128)) SERVER c_test OPTIONS (binding_mode 'create', lake_schema 'main')")
            cur.execute("SELECT og_lake.set_option('biz.t_ip_128'::regclass, 'data_inlining_row_limit', '100000')")
            ip_vecs = rng.normal(0.0, 1.0, size=(5000, 128)).astype(np.float32)
            # rescale some directions to create varied norms for IP
            scales = np.linspace(0.5, 3.0, 5000).astype(np.float32)[:, None]
            ip_vecs = ip_vecs * scales
            cur.execute('BEGIN')
            for st in range(0, 5000, 500):
                vals = ','.join(f"({i},'[{','.join(f'{x:.5f}' for x in ip_vecs[i])}]'::vector(128))" for i in range(st, st + 500))
                cur.execute(f'INSERT INTO biz.t_ip_128 VALUES {vals}')
            cur.execute('COMMIT')
            cur.execute("SELECT og_lake.flush('biz.t_ip_128'::regclass)")
            cur.execute("ANALYZE biz.t_ip_128")
            cur.execute("""SELECT og_lake.create_lake_index('biz.t_ip_128'::regclass, 'ix_ip_raw',
                '{"version":1,"kind":"vector","method":"diskann","column":"v","metric":"ip","pq":false}'::jsonb)""")
            ip_uuid = cur.fetchone()[0]
            cur.execute('SELECT og_lake.build_lake_index(%s::uuid)', (ip_uuid,))
            ip_q = [rng.normal(0.0, 1.0, size=128).astype(np.float32) for _ in range(10)]
            ip_lits = ['[' + ','.join(f'{x:.5f}' for x in q) + ']' for q in ip_q]
            ip_truth = [set(int(x) for x in np.argsort(-(ip_vecs @ q))[:10]) for q in ip_q]
            cur.execute('SET enable_lake_ann=on; SET enable_seqscan=on')
            cur.execute(f"EXPLAIN (ANALYZE, COSTS ON) SELECT id FROM biz.t_ip_128 WHERE v IS NOT NULL ORDER BY v <#> '{ip_lits[0]}'::vector(128) LIMIT 10")
            ip_plan = '\n'.join(r[0] for r in cur.fetchall())
            assert 'Verified ANN File Coverage: 1' in ip_plan, f'Raw IP ANN not chosen naturally!\n{ip_plan}'
            ip_recalls = []
            ip_times = []
            for qi, ql in enumerate(ip_lits):
                t0 = time.monotonic()
                cur.execute(f"SELECT id FROM biz.t_ip_128 WHERE v IS NOT NULL ORDER BY v <#> '{ql}'::vector(128) LIMIT 10")
                got = set(r[0] for r in cur.fetchall())
                ip_times.append((time.monotonic() - t0) * 1000.0)
                ip_recalls.append(len(got & ip_truth[qi]) / 10.0)
            ip_rec = dict(dim=128, rows=5000, metric='ip_raw_cached', mean_recall10=round(float(np.mean(ip_recalls)), 4),
                          cold_ms=round(ip_times[0], 2), warm_ms=round(float(np.mean(ip_times[1:])), 2))
            print(json.dumps(ip_rec), flush=True)
            assert ip_rec['mean_recall10'] >= 0.88, ip_rec
            results.append(ip_rec)
        return dict(status='passed', cases=results)
    finally:
        teardown_test_db(profile, conn, db, role, tmp_root)


def run_active_h_suite(profile: str):
    print(f'\n=== [Part B] active-H Empty-Input Hardening & Concurrency ({profile}) ===', flush=True)
    conn, db, role, tmp_root, _ = setup_test_db(profile)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE FOREIGN TABLE biz.part_h(id bigint, p int, v int)
                SERVER c_test OPTIONS (binding_mode 'create', lake_schema 'main')
            """)
            cur.execute("SELECT og_lake.set_partition('biz.part_h'::regclass, ARRAY['p'])")
            cur.execute("SELECT og_lake.set_option('biz.part_h'::regclass, 'data_inlining_row_limit', '100000')")

            # Reference heap table for exact result check
            cur.execute("CREATE TABLE biz.ref_h(id bigint, p int, v int)")

            # Phase 1: Insert rows clustered by partition key across 4 partitions (LAKE_IDENTITY_FLUSH_GROUP_LIMIT=2 -> round 1 takes 2 groups and activates H; second flush closes H)
            cur.execute("INSERT INTO biz.part_h SELECT i, i % 4, i * 10 FROM generate_series(1, 200) i ORDER BY i % 4, i")
            cur.execute("INSERT INTO biz.ref_h SELECT i, i % 4, i * 10 FROM generate_series(1, 200) i ORDER BY i % 4, i")
            cur.execute("SELECT og_lake.flush('biz.part_h'::regclass)")
            cur.execute("SELECT flush_birth_cutoff FROM og_lake_internal.table_binding WHERE table_uuid=(SELECT table_uuid FROM lake_metadata.ducklake_table WHERE table_name='part_h' AND end_snapshot IS NULL)::uuid")
            assert cur.fetchone()[0] is not None, 'Expected active-H after flushing 2 of 4 groups'
            cur.execute("SELECT og_lake.flush('biz.part_h'::regclass)")
            cur.execute("SELECT flush_birth_cutoff FROM og_lake_internal.table_binding WHERE table_uuid=(SELECT table_uuid FROM lake_metadata.ducklake_table WHERE table_name='part_h' AND end_snapshot IS NULL)::uuid")
            assert cur.fetchone()[0] is None, 'Expected H closed after draining remaining 2 groups'

            # Phase 2: Concurrency - simultaneous writers + flush loops across threads
            errors = []
            stop_flag = threading.Event()

            def writer_worker(wid: int):
                wc = connect_profile(profile, db)
                try:
                    with wc.cursor() as wcur:
                        wcur.execute('SET statement_timeout=60000')
                        base = 1000 + wid * 1000
                        for step in range(8):
                            if stop_flag.is_set():
                                break
                            rid = base + step
                            try:
                                wcur.execute('BEGIN')
                                wcur.execute("INSERT INTO biz.part_h VALUES (%s, %s, %s)", (rid, rid % 4, rid))
                                wcur.execute("INSERT INTO biz.ref_h VALUES (%s, %s, %s)", (rid, rid % 4, rid))
                                wcur.execute('COMMIT')
                            except psycopg2.Error as exc:
                                wcur.execute('ROLLBACK')
                                # Serialization retries are expected under concurrent flush/write
                                if exc.pgcode not in ('40001', '55000'):
                                    errors.append((wid, exc.pgcode, str(exc)))
                                    break
                finally:
                    wc.close()

            t1 = threading.Thread(target=writer_worker, args=(1,))
            t2 = threading.Thread(target=writer_worker, args=(2,))
            t1.start()
            t2.start()
            for _ in range(6):
                cur.execute("SELECT og_lake.flush('biz.part_h'::regclass)")
                time.sleep(0.02)
            t1.join(timeout=30)
            t2.join(timeout=30)
            stop_flag.set()
            assert not errors, f'Unexpected worker errors: {errors}'

            for _ in range(64):
                cur.execute("SELECT og_lake.flush('biz.part_h'::regclass)")
                cur.execute("SELECT flush_birth_cutoff FROM og_lake_internal.table_binding WHERE table_uuid=(SELECT table_uuid FROM lake_metadata.ducklake_table WHERE table_name='part_h' AND end_snapshot IS NULL)::uuid")
                if cur.fetchone()[0] is None:
                    # Also ensure no un-flushed inline rows remain in active carrier
                    cur.execute("""
                        SELECT count(*) FROM lake_metadata.ducklake_inlined_data_tables
                        WHERE table_id=(SELECT table_id FROM lake_metadata.ducklake_table WHERE table_name='part_h' AND end_snapshot IS NULL)
                    """)
                    break
            cur.execute("SELECT flush_birth_cutoff FROM og_lake_internal.table_binding WHERE table_uuid=(SELECT table_uuid FROM lake_metadata.ducklake_table WHERE table_name='part_h' AND end_snapshot IS NULL)::uuid")
            assert cur.fetchone()[0] is None, 'H remained active after draining all groups'
            cur.execute("SELECT count(*) FROM (SELECT * FROM biz.part_h EXCEPT ALL SELECT * FROM biz.ref_h) x")
            assert cur.fetchone()[0] == 0
            cur.execute("SELECT count(*) FROM (SELECT * FROM biz.ref_h EXCEPT ALL SELECT * FROM biz.part_h) x")
            assert cur.fetchone()[0] == 0

            cur.execute("""
                SELECT t.binding_id, dt.table_id
                FROM og_lake_internal.table_binding t
                JOIN lake_metadata.ducklake_table dt ON dt.table_id = t.table_id
                WHERE dt.table_name='part_h' AND dt.end_snapshot IS NULL AND t.state='active'
            """)
            bid, tid = cur.fetchone()
            cur.execute("SELECT table_name, schema_version FROM lake_metadata.ducklake_inlined_data_tables WHERE table_id=%s ORDER BY schema_version DESC LIMIT 1", (tid,))
            carrier, sv = cur.fetchone()
            cur.execute("SELECT min(begin_snapshot) FROM lake_metadata.ducklake_schema_versions WHERE table_id=%s AND schema_version=%s", (tid, sv))
            osnap = cur.fetchone()[0]
            cur.execute(f"SELECT 'lake_metadata.{carrier}'::regclass::oid")
            carrier_oid = cur.fetchone()[0]

            # Drain any remaining interleaved inline groups completely before Phase 3a empty-input check
            for _ in range(64):
                cur.execute(f"SELECT count(*) FROM lake_metadata.{carrier}")
                rem = cur.fetchone()[0]
                cur.execute("SELECT flush_birth_cutoff FROM og_lake_internal.table_binding WHERE binding_id=%s::uuid", (bid,))
                cur_h = cur.fetchone()[0]
                if rem == 0 and cur_h is None:
                    break
                cur.execute("SELECT og_lake.flush('biz.part_h'::regclass)")
            cur.execute("SELECT max(snapshot_id) FROM lake_metadata.ducklake_snapshot")
            snap_before = cur.fetchone()[0]

            # 3a: Inject stale active H pointing to cutoff=snap_before while carrier genuinely has 0 eligible rows (rows=0)!
            # Exercises exact empty-input recovery branch in lake_publish.cpp.
            cur.execute("""
                UPDATE og_lake_internal.table_binding
                SET flush_birth_cutoff = %s, flush_carrier_oid = %s, flush_source_schema = %s, flush_source_origin = %s
                WHERE binding_id = %s::uuid
            """, (snap_before, carrier_oid, sv, osnap, bid))
            # Verify empty-input flush closes H to NULL cleanly without creating an empty DuckLake snapshot!
            cur.execute("SELECT og_lake.flush('biz.part_h'::regclass)")
            cur.execute("SELECT max(snapshot_id) FROM lake_metadata.ducklake_snapshot")
            snap_after = cur.fetchone()[0]
            assert snap_after == snap_before, f'Empty-input H recovery created spurious snapshot {snap_after} != {snap_before}'
            cur.execute("SELECT flush_birth_cutoff FROM og_lake_internal.table_binding WHERE binding_id=%s::uuid", (bid,))
            assert cur.fetchone()[0] is None, 'Stale empty H was not cleared'

            # 3b: Verify empty-input cleanup REFUSES if non-empty, and normal post-recovery insert+flush still works!
            cur.execute("INSERT INTO biz.part_h VALUES (9999, 1, 999)")
            cur.execute("INSERT INTO biz.ref_h VALUES (9999, 1, 999)")
            cur.execute("SELECT og_lake.flush('biz.part_h'::regclass)")
            cur.execute("SELECT flush_birth_cutoff FROM og_lake_internal.table_binding WHERE binding_id=%s::uuid", (bid,))
            assert cur.fetchone()[0] is None
            cur.execute("SELECT count(*) FROM (SELECT * FROM biz.part_h EXCEPT ALL SELECT * FROM biz.ref_h) x")
            assert cur.fetchone()[0] == 0

        print('active-H empty-input cleanup & concurrency PASSED', flush=True)
        return dict(status='passed')
    finally:
        teardown_test_db(profile, conn, db, role, tmp_root)


def run_pgl0_suite(profile: str):
    print(f'\n=== [Part C] PGL-0 Expanded Native pg_partition Suite ({profile}) ===', flush=True)

    # First check 0.73 contract isolation + ALTER EXTENSION UPDATE TO '0.74'
    conn73, db73, role73, tmp73, _ = setup_test_db(profile, ext_version='0.73')
    try:
        tids_73_map = seed_unbound_duckdb_tables(profile, db73, tmp73, [(f'b{i}', 'k INTEGER, v VARCHAR') for i in range(4)])
        with conn73.cursor() as cur:
            cur.execute("""
                CREATE TABLE biz.p4_old(k int, v text)
                PARTITION BY RANGE(k) (
                    PARTITION p0 VALUES LESS THAN (10),
                    PARTITION p1 VALUES LESS THAN (20),
                    PARTITION p2 VALUES LESS THAN (30),
                    PARTITION p3 VALUES LESS THAN (MAXVALUE)
                )
            """)
            cur.execute("SELECT array_agg(oid ORDER BY relname) FROM pg_partition WHERE parentid='biz.p4_old'::regclass AND parttype='p'")
            poids = cur.fetchone()[0]
            tids = [tids_73_map[f'b{i}'] for i in range(4)]
            try:
                cur.execute("SELECT og_lake.bind_native_partition_readonly('biz.p4_old'::regclass, 'c_test'::name, %s::oid[], %s::bigint[])", (poids, tids))
                raise AssertionError('0.73 unexpectedly allowed 4 RANGE leaves!')
            except psycopg2.Error as exc:
                assert exc.pgcode == '0A000', f'Expected 0A000 under 0.73, got {exc.pgcode}: {exc}'
            cur.execute("ALTER EXTENSION og_lake UPDATE TO '0.74'")
            cur.execute("SELECT extversion FROM pg_extension WHERE extname='og_lake'")
            assert cur.fetchone()[0] == '0.74'
            cur.execute("SELECT og_lake.host_status()::jsonb ->> 'native_partition_expanded'")
            assert cur.fetchone()[0] == 'true'
            cur.execute("SELECT og_lake.bind_native_partition_readonly('biz.p4_old'::regclass, 'c_test'::name, %s::oid[], %s::bigint[])", (poids, tids))
            cur.execute("SELECT og_lake.enable_native_partition_insert('biz.p4_old'::regclass)")
            cur.execute("INSERT INTO biz.p4_old VALUES (1,'a'),(15,'b'),(25,'c'),(99,'d')")
            cur.execute("SELECT k, v FROM biz.p4_old ORDER BY k")
            assert cur.fetchall() == [(1, 'a'), (15, 'b'), (25, 'c'), (99, 'd')]
        print('0.73 pre-upgrade gate & 0.73->0.74 upgrade + 4-leaf RANGE bind PASSED', flush=True)
    finally:
        teardown_test_db(profile, conn73, db73, role73, tmp73)

    # Full SQL0.74 suite: RANGE 4-leaf extended types, LIST 4-leaf, HASH 4-leaf
    conn, db, role, tmp_root, _ = setup_test_db(profile, ext_version='0.74')
    try:
        seed_specs = (
            [(f'rseed_{i}', 'k BIGINT, u UUID, amt DECIMAL(12,2), ts TIMESTAMPTZ, meta JSON, score DOUBLE') for i in range(4)] +
            [(f'lseed_{i}', 'id INTEGER, dt DATE, region VARCHAR, amt DECIMAL(10,2)') for i in range(4)] +
            [(f'hseed_{i}', 'id BIGINT, code VARCHAR, d DATE') for i in range(4)]
        )
        all_tids = seed_unbound_duckdb_tables(profile, db, tmp_root, seed_specs)
        with conn.cursor() as cur:
            def bind_and_verify(strategy_label, seed_prefix, parent_sql, pre_rows, copy_lines, update_sql, delete_sql, prune_sql, oracle_create_sql, cmp_cols='*'):
                p_tbl = f'biz.p_{strategy_label}'
                o_tbl = f'biz.oracle_{strategy_label}'
                t0 = time.monotonic()
                cur.execute(parent_sql.replace('biz.p', p_tbl))
                cur.execute(oracle_create_sql.replace('biz.oracle', o_tbl))
                cur.execute("SELECT array_agg(oid ORDER BY relname), count(*) FROM pg_partition WHERE parentid=%s::regclass AND parttype='p'", (p_tbl,))
                poids, nleaves = cur.fetchone()
                tids = [all_tids[f'{seed_prefix}{i}'] for i in range(nleaves)]

                cur.execute("SELECT og_lake.bind_native_partition_readonly(%s::regclass, 'c_test'::name, %s::oid[], %s::bigint[])", (p_tbl, poids, tids))
                cur.execute("SELECT og_lake.enable_native_partition_insert(%s::regclass)", (p_tbl,))
                print(f'[{strategy_label}] bind+enable: {time.monotonic()-t0:.2f}s', flush=True)

                # INSERT batch across all leaves
                t1 = time.monotonic()
                for vals_sql in pre_rows:
                    cur.execute(f"INSERT INTO {p_tbl} VALUES {vals_sql}")
                    cur.execute(f"INSERT INTO {o_tbl} VALUES {vals_sql}")

                # COPY FROM
                copy_file = tmp_root / f'copy_{strategy_label}.csv'
                copy_file.write_text('\n'.join(copy_lines) + '\n')
                with copy_file.open('r') as fh:
                    cur.copy_expert(f"COPY {p_tbl} FROM STDIN WITH (DELIMITER ',', NULL '\\N')", fh)
                with copy_file.open('r') as fh:
                    cur.copy_expert(f"COPY {o_tbl} FROM STDIN WITH (DELIMITER ',', NULL '\\N')", fh)
                print(f'[{strategy_label}] insert+copy: {time.monotonic()-t1:.2f}s', flush=True)

                # ANALYZE
                t2 = time.monotonic()
                cur.execute(f"ANALYZE {p_tbl}")
                cur.execute(f"ANALYZE {o_tbl}")
                print(f'[{strategy_label}] analyze: {time.monotonic()-t2:.2f}s', flush=True)

                # Rollback check
                t3 = time.monotonic()
                cur.execute("BEGIN")
                cur.execute(delete_sql.replace('biz.p', p_tbl))
                cur.execute("ROLLBACK")
                print(f'[{strategy_label}] rollback_del: {time.monotonic()-t3:.2f}s', flush=True)

                # UPDATE (including cross-partition row movement!) + DELETE
                t4 = time.monotonic()
                cur.execute(update_sql.replace('biz.p', p_tbl))
                cur.execute(update_sql.replace('biz.p', o_tbl))
                print(f'[{strategy_label}] update_move: {time.monotonic()-t4:.2f}s', flush=True)
                t5 = time.monotonic()
                cur.execute(delete_sql.replace('biz.p', p_tbl))
                cur.execute(delete_sql.replace('biz.p', o_tbl))
                print(f'[{strategy_label}] delete: {time.monotonic()-t5:.2f}s', flush=True)

                # Compare exact table equality against ordinary heap oracle
                cur.execute(f"SELECT count(*) FROM (SELECT {cmp_cols} FROM {p_tbl} EXCEPT ALL SELECT {cmp_cols} FROM {o_tbl}) x")
                d1 = cur.fetchone()[0]
                cur.execute(f"SELECT count(*) FROM (SELECT {cmp_cols} FROM {o_tbl} EXCEPT ALL SELECT {cmp_cols} FROM {p_tbl}) x")
                d2 = cur.fetchone()[0]
                assert d1 == 0 and d2 == 0, f'{strategy_label}: diff p-oracle={d1}, oracle-p={d2}'

                # Check EXPLAIN plan pruning / Native Lake Parent label
                cur.execute(f"EXPLAIN (ANALYZE, COSTS OFF) {prune_sql.replace('biz.p', p_tbl)}")
                plan_txt = '\n'.join(r[0] for r in cur.fetchall())
                cur.execute("SELECT * FROM (" + prune_sql.replace('biz.p', p_tbl) + ") a EXCEPT ALL SELECT * FROM (" + prune_sql.replace('biz.p', o_tbl) + ") b")
                assert cur.fetchall() == []
                print(f'[{strategy_label}] ({nleaves} leaves) DML/COPY/ANALYZE/RowMovement/Prune PASSED!', flush=True)
                return plan_txt

            # 1) RANGE 4-leaf with BIGINT key + extended UUID/NUMERIC/JSON/TIMESTAMPTZ/FLOAT8 columns!
            p_range = """
                CREATE TABLE biz.p(
                    k bigint, u uuid, amt numeric(12,2), ts timestamptz, meta json, score float8
                ) WITH (orientation=row)
                PARTITION BY RANGE(k) (
                    PARTITION r0 VALUES LESS THAN (100),
                    PARTITION r1 VALUES LESS THAN (200),
                    PARTITION r2 VALUES LESS THAN (300),
                    PARTITION r3 VALUES LESS THAN (MAXVALUE)
                ) ENABLE ROW MOVEMENT
            """
            o_range = "CREATE TABLE biz.oracle(k bigint, u uuid, amt numeric(12,2), ts timestamptz, meta json, score float8)"
            u1, u2, u3, u4 = [str(uuid.uuid4()) for _ in range(4)]
            plan_r = bind_and_verify(
                'range_4leaf_ext',
                'rseed_',
                p_range,
                [f"(10,'{u1}',12.34,'2026-01-01 00:00:00+00','{{\"a\":1}}',1.5),(150,'{u2}',56.78,'2026-02-01 00:00:00+00','{{\"a\":2}}',2.5)",
                 f"(250,'{u3}',99.99,'2026-03-01 00:00:00+00','{{\"a\":3}}',3.5),(350,'{u4}',0.01,'2026-04-01 00:00:00+00','{{\"a\":4}}',4.5)"],
                [f"55,{u1},11.11,2026-05-01 00:00:00+00,{{\"c\":1}},9.1",
                 f"275,{u3},22.22,2026-06-01 00:00:00+00,{{\"c\":2}},8.2"],
                "UPDATE biz.p SET k = k + 200, amt = amt + 1.00 WHERE k IN (10, 150)",
                "DELETE FROM biz.p WHERE k = 275",
                "SELECT k, u::text, amt::text FROM biz.p WHERE k >= 200 AND k < 300 ORDER BY k",
                o_range,
                cmp_cols='k, u, amt, ts, meta::text, score',
            )
            assert 'certified readonly native RANGE leaves' in plan_r, plan_r

            # 2) LIST 4-leaf on DATE key (+ DEFAULT leaf)
            p_list = """
                CREATE TABLE biz.p(id int, dt date, region text, amt numeric(10,2)) WITH (orientation=row)
                PARTITION BY LIST(dt) (
                    PARTITION pe VALUES ('2026-01-01', '2026-01-02'),
                    PARTITION pw VALUES ('2026-02-01'),
                    PARTITION pn VALUES ('2026-03-01'),
                    PARTITION pd VALUES (DEFAULT)
                ) ENABLE ROW MOVEMENT
            """
            o_list = "CREATE TABLE biz.oracle(id int, dt date, region text, amt numeric(10,2))"
            plan_l = bind_and_verify(
                'list_4leaf_date',
                'lseed_',
                p_list,
                ["(1,'2026-01-01','east',10.50),(2,'2026-02-01','west',20.50),(3,'2026-03-01','north',30.50),(4,'2026-04-01','south',40.50),(5,'2026-01-02','ne',50.00)"],
                ["6,2026-02-01,west,60.25", "7,2026-05-01,other,70.75"],
                "UPDATE biz.p SET dt = '2026-02-01', amt = amt + 5 WHERE id IN (1, 4)",
                "DELETE FROM biz.p WHERE id = 3",
                "SELECT id, region FROM biz.p WHERE dt = '2026-02-01' ORDER BY id",
                o_list,
            )
            assert 'certified native LIST leaves' in plan_l, plan_l

            # 3) HASH 4-leaf on TEXT key
            p_hash = """
                CREATE TABLE biz.p(id bigint, code text, d date) WITH (orientation=row)
                PARTITION BY HASH(code) (
                    PARTITION h0, PARTITION h1, PARTITION h2, PARTITION h3
                ) ENABLE ROW MOVEMENT
            """
            o_hash = "CREATE TABLE biz.oracle(id bigint, code text, d date)"
            plan_h = bind_and_verify(
                'hash_4leaf_text',
                'hseed_',
                p_hash,
                ["(1001,'alpha','2026-01-10'),(1002,'beta','2026-01-11'),(1003,'gamma','2026-01-12'),(1004,'delta','2026-01-13')"],
                ["1005,epsilon,2026-01-14", "1006,zeta,2026-01-15"],
                "UPDATE biz.p SET code = 'beta', id = id + 100 WHERE code IN ('alpha', 'gamma')",
                "DELETE FROM biz.p WHERE code = 'delta'",
                "SELECT id, code FROM biz.p WHERE code = 'beta' ORDER BY id",
                o_hash,
            )
            assert 'certified native HASH leaves' in plan_h, plan_h

        return dict(status='passed')
    finally:
        teardown_test_db(profile, conn, db, role, tmp_root)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--profile', choices=['debug', 'release', 'ann-debug'], default='release')
    ap.add_argument('--mode', choices=['full', 'ann', 'active-h', 'pgl0'], default='full')
    ap.add_argument('--dims', default='32,128,384,768,1536')
    args = ap.parse_args()

    out = {}
    dims = tuple(int(x) for x in args.dims.split(',') if x.strip())
    if args.mode in ('full', 'ann'):
        out['ann'] = run_ann_suite(args.profile, dims=dims)
    if args.mode in ('full', 'active-h'):
        out['active_h'] = run_active_h_suite(args.profile)
    if args.mode in ('full', 'pgl0'):
        out['pgl0'] = run_pgl0_suite(args.profile)
    print('\n=== ALL REQUESTED 0.74 VERIFICATIONS PASSED ===')
    print(json.dumps(out, indent=2))


if __name__ == '__main__':
    main()
