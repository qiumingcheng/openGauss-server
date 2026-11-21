#!/usr/bin/env python3
"""SQL0.63 (PGL-3) verification suite + DuckLake/DuckDB parity."""

import argparse
import datetime
import decimal
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import uuid
from pathlib import Path

import psycopg2

ROOT = Path('/home/qiumc/data/code/cpp')
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


def duckdb_exec(sql_text):
    proc = subprocess.run(
        [str(DUCKDB_BIN), '-batch', '-light-mode', '-c', 'LOAD ducklake;\n' + sql_text],
        capture_output=True, text=True, timeout=60,
    )
    if proc.returncode != 0:
        raise RuntimeError(f'DuckDB exec failed ({proc.returncode}): {proc.stderr}\nSQL:\n{sql_text}')


def pg_connect(profile, dbname, user='qiumc', password=None):
    port = 55560 if profile == 'debug' else (55570 if profile == 'release' else 55580)
    sock_dir = str((RUNTIME.parent / profile / 'socket').resolve())
    kwargs = {
        'host': sock_dir,
        'port': port,
        'dbname': dbname,
        'user': user,
        'connect_timeout': 15,
        'application_name': 'sql063_verify',
    }
    if password is not None:
        kwargs['password'] = password
    conn = psycopg2.connect(**kwargs)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute('SET statement_timeout = 60000')
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


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--profile', choices=['debug', 'ann-debug', 'release'], required=True)
    ap.add_argument('--out', required=True)
    args = ap.parse_args()

    tag = uuid.uuid4().hex[:10]
    tmp_root = Path(tempfile.mkdtemp(prefix=f'og_063_{tag}_', dir='/tmp'))
    os.chmod(tmp_root, 0o755)
    role_own_upg = f'u063_ownu_{tag}'
    role_own_main = f'u063_ownm_{tag}'
    db_upg = f'db063_upg_{tag}'
    db_main = f'db063_main_{tag}'
    admin = None
    created_dbs = []
    created_roles = []
    stages = {}
    duckdb_checks = 0

    try:
        admin = pg_connect(args.profile, 'postgres')
        for rn in (role_own_upg, role_own_main):
            q(admin, f"CREATE ROLE {rn} NOLOGIN NOINHERIT ENCRYPTED PASSWORD %s", (md5_role_pw('Own063!', rn),))
            created_roles.append(rn)
        for dbn in (db_upg, db_main):
            q(admin, f"CREATE DATABASE {dbn} WITH DBCOMPATIBILITY 'PG' ENCODING 'UTF8'")
            created_dbs.append(dbn)

        # ------------------------------------------------------------------
        # Stage 1: Version gating (0.62 -> 0.63) & explicit truncate boundary
        # ------------------------------------------------------------------
        cu = pg_connect(args.profile, db_upg)
        try:
            q(cu, "CREATE EXTENSION og_lake VERSION '0.62'")
            hs = json.loads(s(cu, 'SELECT og_lake.host_status()'))
            assert hs.get('module_version') in ('0.63', '0.73', '0.74'), hs
            assert hs.get('file_partition_combinations') is True, hs
            assert hs.get('file_partition_combinations_sql_version') == '0.63', hs
            assert hs.get('truncate_partition_supported') is (hs.get('module_version') in ('0.73', '0.74')), hs

            q(cu, 'SELECT og_lake.configure_internal_owner(%s::name)', (role_own_upg,))
            q(cu, "SELECT og_lake.create_catalog('cat_u'::name, 'cat_u_meta'::name, %s::text, NULL::text)",
              (str(tmp_root / 'cat_u'),))
            q(cu, "CREATE FOREIGN TABLE t_u (id int, u uuid, n numeric(18,4), d date, b bytea) SERVER cat_u OPTIONS (binding_mode 'create', lake_schema 'main')")

            # Under 0.62, extended partition keys and truncate must fail cleanly with 0A000
            expect_error(cu, "SELECT og_lake.set_partition('t_u'::regclass, ARRAY['u'])",
                         expected_states={'0A000'})
            expect_error(cu, "SELECT og_lake.set_partition('t_u'::regclass, ARRAY['n'])",
                         expected_states={'0A000'})
            expect_error(cu, "SELECT og_lake.set_partition('t_u'::regclass, ARRAY['id'], ARRAY['truncate(4)'])",
                         expected_states={'0A000'}, expected_substr='truncate')
            assert s(cu, 'SELECT count(*) FROM cat_u_meta.ducklake_partition_info') == 0
            assert s(cu, 'SELECT count(*) FROM cat_u_meta.ducklake_partition_column') == 0

            # Upgrade 0.62 -> 0.63
            q(cu, "ALTER EXTENSION og_lake UPDATE TO '0.63'")
            assert s(cu, "SELECT extversion FROM pg_extension WHERE extname='og_lake'") == '0.63'

            # Explicit truncate boundary MUST stay rejected in 0.63
            expect_error(cu, "SELECT og_lake.set_partition('t_u'::regclass, ARRAY['id'], ARRAY['truncate(4)'])",
                         expected_states={'0A000'}, expected_substr='truncate')
            expect_error(cu, "SELECT og_lake.set_partition('t_u'::regclass, ARRAY['id'], ARRAY['truncate'])",
                         expected_states={'0A000'}, expected_substr='truncate')
            assert s(cu, 'SELECT count(*) FROM cat_u_meta.ducklake_partition_info') == 0
            assert s(cu, 'SELECT count(*) FROM cat_u_meta.ducklake_partition_column') == 0
            stages['stage1_version_and_truncate_boundary'] = 10
        finally:
            cu.close()

        # ------------------------------------------------------------------
        # Stage 2: Extended Partition Keys + Composite + Pruning + DuckDB Parity
        # ------------------------------------------------------------------
        cm = pg_connect(args.profile, db_main)
        try:
            q(cm, 'CREATE EXTENSION og_lake')
            assert s(cm, "SELECT extversion FROM pg_extension WHERE extname='og_lake'") in ('0.63', '0.73', '0.74')
            q(cm, 'SELECT og_lake.configure_internal_owner(%s::name)', (role_own_main,))
            cat_dir = tmp_root / 'cat_main'
            idx_dir = tmp_root / 'idx_main'
            idx_dir.mkdir(parents=True)
            os.chmod(idx_dir, 0o755)
            q(cm, "SELECT og_lake.create_catalog('cat_main'::name, 'cat_meta'::name, %s::text, NULL::text)",
              (str(cat_dir),))
            q(cm, "SELECT og_lake.set_option('cat_main'::name, 'index_root'::text, %s::text)",
              (str(idx_dir),))
            q(cm, "SELECT og_lake.set_option('cat_main'::name, 'data_inlining_row_limit'::text, '500'::text)")

            # 2a: UUID identity + bucket(4)
            u1 = '01234567-89ab-cdef-0123-456789abcdef'
            u2 = 'ffffffff-ffff-ffff-ffff-ffffffffffff'
            q(cm, "CREATE FOREIGN TABLE t_uuid_id (id int, u uuid, tag text) SERVER cat_main OPTIONS (binding_mode 'create', lake_schema 'main')")
            q(cm, "SELECT og_lake.set_partition('t_uuid_id'::regclass, ARRAY['u'])")
            q(cm, "INSERT INTO t_uuid_id VALUES (1, %s::uuid, 'a'), (2, %s::uuid, 'b'), (3, NULL, 'n')", (u1, u2))
            q(cm, "SELECT og_lake.flush('t_uuid_id'::regclass)")
            q(cm, "SELECT og_lake.flush('t_uuid_id'::regclass)")
            pvals = q(cm, "SELECT partition_value FROM cat_meta.ducklake_file_partition_value v JOIN cat_meta.ducklake_table t ON t.table_id=v.table_id WHERE t.table_name='t_uuid_id' ORDER BY partition_value NULLS LAST")
            assert pvals == [(u1,), (u2,), (None,)], pvals
            exp_u = explain_lines(cm, f"SELECT id, tag FROM t_uuid_id WHERE u = '{u1}'::uuid ORDER BY id")
            assert 'Lake Partition Files Skipped: 2' in exp_u, exp_u
            assert q(cm, f"SELECT id, tag FROM t_uuid_id WHERE u = '{u1}'::uuid") == [(1, 'a')]
            assert q(cm, f"SELECT id, tag FROM t_uuid_id WHERE u IN ('{u1}'::uuid, '{u2}'::uuid) ORDER BY id") == [(1, 'a'), (2, 'b')]
            assert q(cm, "SELECT id, tag FROM t_uuid_id WHERE u IS NULL") == [(3, 'n')]

            q(cm, "CREATE FOREIGN TABLE t_uuid_bkt (id int, u uuid) SERVER cat_main OPTIONS (binding_mode 'create', lake_schema 'main')")
            q(cm, "SELECT og_lake.set_partition('t_uuid_bkt'::regclass, ARRAY['u'], ARRAY['bucket(4)'])")
            q(cm, "INSERT INTO t_uuid_bkt VALUES (1, %s::uuid), (2, %s::uuid)", (u1, u2))
            q(cm, "SELECT og_lake.flush('t_uuid_bkt'::regclass)")
            og_u_bkts = [r[0] for r in q(cm, "SELECT partition_value FROM cat_meta.ducklake_file_partition_value v JOIN cat_meta.ducklake_table t ON t.table_id=v.table_id WHERE t.table_name='t_uuid_bkt' ORDER BY partition_value")]
            dk_u_bkts = duckdb_json(f"""
                SELECT ((murmur3_32('{u1}'::UUID) & 2147483647) % 4)::VARCHAR AS b1,
                       ((murmur3_32('{u2}'::UUID) & 2147483647) % 4)::VARCHAR AS b2
            """)[0]
            duckdb_checks += 1
            assert sorted(og_u_bkts) == sorted({dk_u_bkts['b1'], dk_u_bkts['b2']}), (og_u_bkts, dk_u_bkts)
            exp_ub = explain_lines(cm, f"SELECT id FROM t_uuid_bkt WHERE u = '{u1}'::uuid")
            if dk_u_bkts['b1'] != dk_u_bkts['b2']:
                assert 'Lake Partition Files Skipped: 1' in exp_ub, exp_ub
            assert q(cm, f"SELECT id FROM t_uuid_bkt WHERE u = '{u1}'::uuid") == [(1,)]

            # 2b: NUMERIC(18,4) and NUMERIC(38,4) identity + bucket(4) + edge pruning
            q(cm, "CREATE FOREIGN TABLE t_num18 (id int, n numeric(18,4)) SERVER cat_main OPTIONS (binding_mode 'create', lake_schema 'main')")
            q(cm, "SELECT og_lake.set_partition('t_num18'::regclass, ARRAY['n'])")
            q(cm, "INSERT INTO t_num18 VALUES (1, -0.0001), (2, 0.0000), (3, 123.4500), (4, NULL)")
            q(cm, "SELECT og_lake.flush('t_num18'::regclass)")
            q(cm, "SELECT og_lake.flush('t_num18'::regclass)")
            n18_vals = q(cm, "SELECT partition_value FROM cat_meta.ducklake_file_partition_value v JOIN cat_meta.ducklake_table t ON t.table_id=v.table_id WHERE t.table_name='t_num18' ORDER BY partition_value NULLS LAST")
            assert n18_vals == [('-0.0001',), ('0.0000',), ('123.4500',), (None,)], n18_vals
            exp_n = explain_lines(cm, 'SELECT id FROM t_num18 WHERE n = (-0.0001)::numeric(18,4)')
            assert 'Lake Partition Files Skipped: 3' in exp_n, exp_n
            assert q(cm, 'SELECT id FROM t_num18 WHERE n = 123.450000::numeric') == [(3,)]
            assert q(cm, 'SELECT id FROM t_num18 WHERE n = 123.45678::numeric') == []
            assert q(cm, 'SELECT id FROM t_num18 WHERE n = 99999999999999999999.9::numeric') == []

            q(cm, "CREATE FOREIGN TABLE t_num38 (id int, n numeric(38,4)) SERVER cat_main OPTIONS (binding_mode 'create', lake_schema 'main')")
            q(cm, "SELECT og_lake.set_partition('t_num38'::regclass, ARRAY['n'], ARRAY['bucket(4)'])")
            q(cm, "INSERT INTO t_num38 VALUES (1, -0.0001), (2, 12345678901234567890.1234)")
            q(cm, "SELECT og_lake.flush('t_num38'::regclass)")
            og_d38 = [r[0] for r in q(cm, "SELECT partition_value FROM cat_meta.ducklake_file_partition_value v JOIN cat_meta.ducklake_table t ON t.table_id=v.table_id WHERE t.table_name='t_num38' ORDER BY partition_value")]
            dk_d38 = duckdb_json("""
                SELECT ((murmur3_32((-0.0001)::DECIMAL(38,4)) & 2147483647) % 4)::VARCHAR AS b1,
                       ((murmur3_32(12345678901234567890.1234::DECIMAL(38,4)) & 2147483647) % 4)::VARCHAR AS b2
            """)[0]
            duckdb_checks += 1
            assert sorted(og_d38) == sorted({dk_d38['b1'], dk_d38['b2']}), (og_d38, dk_d38)
            assert q(cm, 'SELECT id FROM t_num38 WHERE n = 12345678901234567890.123400::numeric') == [(2,)]
            assert q(cm, 'SELECT id FROM t_num38 WHERE n = 999999999999999999999999999999999999999.1::numeric') == []

            # 2c: DATE, TIMESTAMP, TIMESTAMPTZ, BYTEA, FLOAT8 identity/bucket(4) & TimeZone independence
            q(cm, "CREATE FOREIGN TABLE t_dt (id int, d date, ts timestamp, tz timestamptz, b bytea, f double precision) SERVER cat_main OPTIONS (binding_mode 'create', lake_schema 'main')")
            q(cm, "SELECT og_lake.set_partition('t_dt'::regclass, ARRAY['d','ts','tz','b','f'], ARRAY['identity','bucket(4)','identity','bucket(4)','bucket(4)'])")
            q(cm, "SET TimeZone = 'Asia/Shanghai'")
            q(cm, "INSERT INTO t_dt VALUES (1, DATE '2026-11-19', TIMESTAMP '2026-11-19 08:09:10.120000', TIMESTAMPTZ '2026-11-19 08:09:10.120000+00', decode('00deadbeef','hex'), 1.5)")
            q(cm, "SELECT og_lake.flush('t_dt'::regclass)")
            q(cm, "SET TimeZone = 'UTC'")
            dt_pvals = q(cm, "SELECT partition_key_index, partition_value FROM cat_meta.ducklake_file_partition_value v JOIN cat_meta.ducklake_table t ON t.table_id=v.table_id WHERE t.table_name='t_dt' ORDER BY partition_key_index")
            dk_dt = duckdb_json("""
                SELECT ((murmur3_32(TIMESTAMP '2026-11-19 08:09:10.120000') & 2147483647) % 4)::VARCHAR AS b_ts,
                       ((murmur3_32('\\x00\\xDE\\xAD\\xBE\\xEF'::BLOB) & 2147483647) % 4)::VARCHAR AS b_blob,
                       ((murmur3_32(1.5::DOUBLE) & 2147483647) % 4)::VARCHAR AS b_flt
            """)[0]
            duckdb_checks += 1
            assert dt_pvals == [
                (0, '2026-11-19'),
                (1, dk_dt['b_ts']),
                (2, '2026-11-19 08:09:10.12+00'),
                (3, dk_dt['b_blob']),
                (4, dk_dt['b_flt']),
            ], (dt_pvals, dk_dt)
            assert s(cm, "SELECT id FROM t_dt WHERE d IN (DATE '2026-11-19', DATE '1970-01-01') AND b = decode('00deadbeef','hex')") == 1
            assert q(cm, "SELECT id FROM t_dt WHERE f = 'NaN'::double precision") == []
            stages['stage2_partition_keys_pruning_duckdb'] = 15

            # ------------------------------------------------------------------
            # Stage 3: Partitioned Table + Extended Payloads + COPY + DML + Flush + Rewrite
            # ------------------------------------------------------------------
            q(cm, """
                CREATE FOREIGN TABLE t_part_payload (
                    id int, p int, raw bytea, u uuid, j json, num numeric(18,4),
                    d date, ts timestamp, tstz timestamptz, vec vector(3)
                ) SERVER cat_main OPTIONS (binding_mode 'create', lake_schema 'main')
            """)
            q(cm, "SELECT og_lake.set_partition('t_part_payload'::regclass, ARRAY['p'])")
            src_payload_pq = tmp_root / 'src_payload.parquet'
            duckdb_exec(f"""
                COPY (
                    SELECT i::INTEGER AS id, (i % 3)::INTEGER AS p,
                           CASE WHEN i % 2 = 0 THEN from_hex('DEADBEEF') ELSE ''::BLOB END AS raw,
                           CASE WHEN i % 2 = 0 THEN UUID '{u1}' ELSE UUID '{u2}' END AS u,
                           ('{{"k":' || i::VARCHAR || '}}')::JSON AS j,
                           (i * 0.25 - 1.0001)::DECIMAL(18,4) AS num,
                           DATE '2026-11-19' AS d,
                           TIMESTAMP '2026-11-19 01:02:03' AS ts,
                           TIMESTAMPTZ '2026-11-19 01:02:03+00' AS tstz,
                           [i::FLOAT, (i+1)::FLOAT, (i+2)::FLOAT]::FLOAT[] AS vec
                    FROM range(1, 31) t(i)
                    ORDER BY (i % 3), i
                ) TO '{src_payload_pq}' (FORMAT PARQUET);
            """)
            duckdb_checks += 1
            q(cm, f"COPY t_part_payload FROM '{src_payload_pq}' WITH (FORMAT parquet)")
            q(cm, "SELECT og_lake.flush('t_part_payload'::regclass)")
            q(cm, "SELECT og_lake.flush('t_part_payload'::regclass)")
            q(cm, f"COPY t_part_payload FROM '{src_payload_pq}' WITH (FORMAT parquet)")
            q(cm, "SELECT og_lake.flush('t_part_payload'::regclass)")
            q(cm, "SELECT og_lake.flush('t_part_payload'::regclass)")
            # Now each partition p IN (0,1,2) has 2 files -> test rewrite_data_files on p=1!
            f_p1 = [r[0] for r in q(cm, """
                SELECT df.data_file_id FROM cat_meta.ducklake_data_file df
                JOIN cat_meta.ducklake_table t ON t.table_id=df.table_id
                JOIN cat_meta.ducklake_file_partition_value fv ON fv.data_file_id=df.data_file_id AND fv.table_id=df.table_id
                WHERE t.table_name='t_part_payload' AND df.end_snapshot IS NULL AND fv.partition_value='1'
                ORDER BY df.data_file_id
            """)]
            assert len(f_p1) == 2, f_p1
            q(cm, "UPDATE t_part_payload SET num = -0.0001 WHERE id = 1 AND p = 1")
            q(cm, "DELETE FROM t_part_payload WHERE id = 4")
            q(cm, "SELECT og_lake.rewrite_data_files('t_part_payload'::regclass, %s::bigint[])", (f_p1,))
            assert s(cm, 'SELECT count(*) FROM t_part_payload') == 58
            assert s(cm, 'SELECT count(*) FROM t_part_payload WHERE p = 1') == 18
            out_payload_pq = tmp_root / 'out_payload.parquet'
            q(cm, f"COPY (SELECT id, p, encode(raw,'hex') AS hx, u::text AS u, j::text AS j, num FROM t_part_payload WHERE p = 1 ORDER BY id LIMIT 4) TO '{out_payload_pq}' WITH (FORMAT parquet)")
            dk_pay = duckdb_json(f"SELECT id, p, hx, u, j, num::VARCHAR AS num FROM read_parquet('{out_payload_pq}') ORDER BY id")
            duckdb_checks += 1
            assert len(dk_pay) == 4 and dk_pay[0]['num'] == '-0.0001' and dk_pay[1]['num'] == '-0.0001', dk_pay
            stages['stage3_partition_extended_payload_rewrite'] = 8

            # ------------------------------------------------------------------
            # Stage 4: Partition + Scalar B-tree/Bitmap + DiskANN + Pruning + ReScan + SMP
            # ------------------------------------------------------------------
            q(cm, "CREATE FOREIGN TABLE t_part_idx (id int, p int, flag int, tag text, vec vector(3)) SERVER cat_main OPTIONS (binding_mode 'create', lake_schema 'main')")
            q(cm, "SELECT og_lake.set_partition('t_part_idx'::regclass, ARRAY['p'])")
            # Insert >= 4 distinct partition files (p=0,1,2,3) and flush, plus inline rows (p=0,3)
            q(cm, """
                INSERT INTO t_part_idx
                SELECT i, i % 4, CASE WHEN i % 5 = 0 THEN 1 ELSE 0 END,
                       'k_' || i, ('[' || (1 + (i % 9))::text || ',' || (1 + ((i*2) % 9))::text || ',' || (1 + ((i*3) % 9))::text || ']')::vector(3)
                FROM generate_series(1, 200) s(i)
                ORDER BY (i % 4), i
            """)
            q(cm, "SELECT og_lake.flush('t_part_idx'::regclass)")
            q(cm, "SELECT og_lake.flush('t_part_idx'::regclass)")
            q(cm, """
                INSERT INTO t_part_idx VALUES
                (201, 2, 1, 'inline_hit', '[0,0,0.05]'::vector(3)),
                (202, 3, 1, 'inline_other', '[9,9,9]'::vector(3))
            """)

            ix_bt = s(cm, "SELECT og_lake.create_lake_index('t_part_idx'::regclass, 'ix_bt_id'::name, '{\"version\":1,\"kind\":\"scalar\",\"method\":\"btree\",\"column\":\"id\"}'::jsonb)")
            ix_bm = s(cm, "SELECT og_lake.create_lake_index('t_part_idx'::regclass, 'ix_bm_flag'::name, '{\"version\":1,\"kind\":\"scalar\",\"method\":\"bitmap\",\"column\":\"flag\"}'::jsonb)")
            q(cm, 'SELECT og_lake.build_lake_index(%s::uuid)', (ix_bt,))
            q(cm, 'SELECT og_lake.build_lake_index(%s::uuid)', (ix_bm,))
            if args.profile in ('ann-debug', 'release'):
                ix_ann = s(cm, "SELECT og_lake.create_lake_index('t_part_idx'::regclass, 'ix_ann_v'::name, '{\"version\":1,\"kind\":\"vector\",\"method\":\"diskann\",\"column\":\"vec\",\"metric\":\"l2\"}'::jsonb)")
                q(cm, 'SELECT og_lake.build_lake_index(%s::uuid)', (ix_ann,))

            # 4a: Scalar B-tree + Partition Pruning
            q(cm, 'SET enable_seqscan = off')
            q(cm, 'SET enable_indexscan = on')
            exp_bt = explain_lines(cm, 'SELECT id, p, tag FROM t_part_idx WHERE p = 2 AND id = 42')
            assert 'Lake Partition Files Skipped: 3' in exp_bt, exp_bt
            assert 'Verified Scalar File Coverage: 1' in exp_bt or 'sorted Parquet candidates' in exp_bt, exp_bt
            assert q(cm, 'SELECT id, p, tag FROM t_part_idx WHERE p = 2 AND id = 42') == [(42, 2, 'k_42')]

            # 4b: Bitmap + Partition Pruning
            exp_bm = explain_lines(cm, 'SELECT id FROM t_part_idx WHERE p = 2 AND flag = 1 ORDER BY id')
            assert 'Lake Partition Files Skipped: 3' in exp_bm, exp_bm
            bm_rows = [r[0] for r in q(cm, 'SELECT id FROM t_part_idx WHERE p = 2 AND flag = 1 ORDER BY id')]
            assert bm_rows == [i for i in range(1, 201) if i % 4 == 2 and i % 5 == 0] + [201], bm_rows

            # 4c: Parameterized NestedLoop ReScan combining Partition Pruning + Scalar Index
            q(cm, 'CREATE TEMP TABLE probes_063 (p_k int, id_k int)')
            q(cm, 'INSERT INTO probes_063 VALUES (1, 17), (2, 42), (3, 19), (2, 201)')
            nl_rows = q(cm, """
                SELECT /*+ NestLoop(probes_063 t_part_idx) */ t_part_idx.id, t_part_idx.p, t_part_idx.tag
                FROM probes_063 JOIN t_part_idx ON t_part_idx.p = probes_063.p_k AND t_part_idx.id = probes_063.id_k
                ORDER BY t_part_idx.id
            """)
            assert nl_rows == [(17, 1, 'k_17'), (19, 3, 'k_19'), (42, 2, 'k_42'), (201, 2, 'inline_hit')], nl_rows

            # 4d: DiskANN + Partition Pruning + Scalar Filter (ann-debug only)
            if args.profile in ('ann-debug', 'release'):
                q(cm, 'SET enable_lake_ann = on')
                q(cm, 'SET enable_seqscan = on')
                ann_sql = """
                    SELECT id FROM t_part_idx
                    WHERE p = 2 AND flag = 1 AND vec IS NOT NULL
                    ORDER BY vec <-> '[0,0,0]'::vector(3) LIMIT 3
                """
                exp_ann = explain_lines(cm, ann_sql)
                assert 'Lake Partition Files Skipped: 3' in exp_ann or 'Lake Partition Files Admitted: 1' in exp_ann, exp_ann
                ann_res = q(cm, ann_sql)
                assert len(ann_res) == 3 and (201,) in ann_res, ann_res

                # Compare pruned serial vs full oracle with ANN off
                q(cm, 'SET enable_lake_ann = off')
                oracle_ann = q(cm, ann_sql)
                assert ann_res == oracle_ann, (ann_res, oracle_ann)
                # Also verify flushed partition-file hit matches exact oracle
                file_ann_sql = """
                    SELECT id FROM t_part_idx
                    WHERE p = 2 AND flag = 1 AND vec IS NOT NULL
                    ORDER BY vec <-> '[2,3,4]'::vector(3) LIMIT 3
                """
                q(cm, 'SET enable_lake_ann = on')
                exp_file_ann = explain_lines(cm, file_ann_sql)
                assert 'Lake Partition Files Skipped: 3' in exp_file_ann or 'Lake Partition Files Admitted: 1' in exp_file_ann, exp_file_ann
                ann_file_res = q(cm, file_ann_sql)
                q(cm, 'SET enable_lake_ann = off')
                assert ann_file_res == q(cm, file_ann_sql), ann_file_res
                q(cm, 'SET enable_lake_ann = on')
            q(cm, 'SET enable_seqscan = on')

            # 4e: Row SMP (query_dop = 1002 and 1004) on File-Partitioned Table
            serial_all = q(cm, 'SELECT id, p, flag, tag FROM t_part_idx ORDER BY id')
            assert len(serial_all) == 202
            q(cm, 'SET enable_vector_engine = off')
            q(cm, 'SET enable_lake_smp = on')
            for dop in (2, 4):
                if args.profile == 'release':
                    q(cm, 'SET enable_force_smp = off')
                    q(cm, f'SET query_dop = {dop}')
                    q(cm, 'SET enable_force_smp = on')
                else:
                    q(cm, f'SET query_dop = {1000 + dop}')
                exp_smp = explain_lines(cm, 'SELECT count(*), sum(id) FROM t_part_idx')
                assert f'Lake SMP Planned DOP: {dop}' in exp_smp, (dop, exp_smp)
                smp_agg = q(cm, 'SELECT count(*), sum(id) FROM t_part_idx')
                assert smp_agg == [(202, 202 * 203 // 2)], (dop, smp_agg)
                smp_rows = q(cm, 'SELECT id, p, flag, tag FROM t_part_idx ORDER BY id')
                assert smp_rows == serial_all, (dop, len(smp_rows))
                # Scalar index + SMP on partitioned table
                exp_ix_smp = explain_lines(cm, 'SELECT id FROM t_part_idx WHERE flag = 1 ORDER BY id')
                if 'Lake SMP Planned DOP:' in exp_ix_smp:
                    ix_smp_rows = [r[0] for r in q(cm, 'SELECT id FROM t_part_idx WHERE flag = 1 ORDER BY id')]
                    assert ix_smp_rows == [i for i in range(1, 201) if i % 5 == 0] + [201, 202]
            q(cm, 'SET enable_force_smp = off')
            q(cm, 'SET query_dop = 1')

            # Also verify post-index DML (INSERT/UPDATE/DELETE) on partitioned indexed table stays exact!
            q(cm, 'DELETE FROM t_part_idx WHERE id = 42')
            q(cm, "UPDATE t_part_idx SET tag = 'u_201' WHERE id = 201")
            assert q(cm, 'SELECT id, tag FROM t_part_idx WHERE p = 2 AND id IN (42, 201) ORDER BY id') == [(201, 'u_201')]
            stages['stage4_partition_indexes_pruning_smp'] = 14 if args.profile in ('ann-debug', 'release') else 11
        finally:
            cm.close()

        for dbn in list(created_dbs):
            q(admin, f'DROP DATABASE {dbn}')
            created_dbs.remove(dbn)
        for rn in list(created_roles):
            q(admin, f'DROP ROLE {rn}')
            created_roles.remove(rn)
        assert q(admin, "SELECT datname FROM pg_database WHERE datname LIKE 'db063_%'") == []
        assert q(admin, "SELECT rolname FROM pg_roles WHERE rolname LIKE 'u063_%'") == []

        res = {
            'status': 'passed',
            'profile': args.profile,
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
            for dbn in created_dbs:
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
