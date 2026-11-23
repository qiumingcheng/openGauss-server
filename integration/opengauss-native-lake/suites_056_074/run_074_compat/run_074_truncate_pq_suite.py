#!/usr/bin/env python3
"""SQL0.73 truncate(N) partition transform + Vec projection/batch + DiskANN+PQ closure suite."""

import argparse
import hashlib
import getpass
import json
import os
import shutil
import subprocess
import tempfile
import uuid
from pathlib import Path

import psycopg2

ROOT = Path(os.getenv('CPP_WORKSPACE', str(Path(__file__).resolve().parents[4]))).expanduser().resolve()
DUCKDB_BIN = os.getenv('OG_LAKE_DUCKDB_BIN')


def duckdb_query(sql):
    binary = DUCKDB_BIN or json.loads((ROOT / '.reference/opengauss-native-lake/build/receipt.json').read_text())['binary']
    proc = subprocess.run([binary, '-json', '-c', sql], capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        raise RuntimeError(f'DuckDB query failed ({proc.returncode}): {proc.stderr.strip()}')
    out = proc.stdout.strip()
    return json.loads(out) if out else []


def md5_role_pw(password, role):
    return 'md5' + hashlib.md5((password + role).encode('utf-8')).hexdigest()


def pg_connect(profile, dbname, user=None, password=None):
    suffix = profile.upper().replace('-', '_')
    default_port = {'debug': 55560, 'release': 55570, 'ann-debug': 55580}[profile]
    port = int(os.getenv('OG_LAKE_TEST_PORT_' + suffix, os.getenv('OG_LAKE_TEST_PORT', str(default_port))))
    host = os.getenv('OG_LAKE_TEST_HOST_' + suffix, os.getenv('OG_LAKE_TEST_HOST', str(ROOT / '.runtime/opengauss-native-lake' / profile / 'socket')))
    if user is None:
        user = os.getenv('OG_LAKE_TEST_USER', os.getenv('PGUSER', getpass.getuser()))
    kw = {
        'host': host,
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


def must_fail(conn, sql, expected_codes=None, params=None):
    try:
        q(conn, sql, params)
    except psycopg2.Error as exc:
        if expected_codes and exc.pgcode not in expected_codes:
            raise AssertionError(f'Expected pgcode in {expected_codes}, got {exc.pgcode}: {exc}')
        return exc.pgcode, str(exc)
    raise AssertionError(f'Expected failure for SQL: {sql}')


def explain_text(conn, sql, analyze=True, params=None):
    prefix = 'EXPLAIN (ANALYZE, COSTS OFF) ' if analyze else 'EXPLAIN (COSTS OFF) '
    return '\n'.join(r[0] for r in q(conn, prefix + sql, params))


def flush_all(conn, tbl_name, expected_file_rows=None, max_rounds=16):
    short_name = tbl_name.split('.')[-1]
    tid = s(conn, "SELECT table_id FROM lake_metadata.ducklake_table WHERE table_name=%s AND end_snapshot IS NULL", (short_name,))
    for _ in range(max_rounds):
        flushed = s(conn, "SELECT COALESCE(SUM(record_count), 0) FROM lake_metadata.ducklake_data_file WHERE table_id=%s AND end_snapshot IS NULL", (tid,))
        if expected_file_rows is not None and flushed >= expected_file_rows:
            return tid
        before = flushed
        q(conn, f"SELECT og_lake.flush('{tbl_name}'::regclass)")
        after = s(conn, "SELECT COALESCE(SUM(record_count), 0) FROM lake_metadata.ducklake_data_file WHERE table_id=%s AND end_snapshot IS NULL", (tid,))
        if after == before:
            return tid
    return tid


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
    tmp_root = Path(tempfile.mkdtemp(prefix=f'og_v73_{tag}_', dir='/tmp'))
    os.chmod(tmp_root, 0o755)
    admin = pg_connect(args.profile, 'postgres')
    created_dbs = []
    created_roles = []
    conns = []
    stages = {}

    try:
        tp_mode = s(admin, 'SHOW enable_thread_pool')
        role_main = f'uv73_{tag}'
        db_main = f'dbv73_{tag}'
        db_fresh = f'dbv73_f_{tag}'
        q(admin, f"CREATE ROLE {role_main} NOLOGIN NOINHERIT ENCRYPTED PASSWORD %s", (md5_role_pw('Trun073!', role_main),))
        created_roles.append(role_main)
        q(admin, f"CREATE DATABASE {db_main} WITH DBCOMPATIBILITY 'PG' ENCODING 'UTF8'")
        q(admin, f"CREATE DATABASE {db_fresh} WITH DBCOMPATIBILITY 'PG' ENCODING 'UTF8'")
        created_dbs.extend([db_main, db_fresh])

        # Stage 1: Upgrade 0.72 -> 0.73, refusal gates, malformed/type validation
        c1 = 0
        cm = pg_connect(args.profile, db_main)
        conns.append(cm)
        q(cm, "CREATE EXTENSION og_lake VERSION '0.72'")
        assert s(cm, "SELECT extversion FROM pg_extension WHERE extname='og_lake'") == '0.72'
        hs = json.loads(s(cm, "SELECT og_lake.host_status()"))
        assert hs['module_version'] in ('0.73', '0.74')
        assert hs['truncate_partition_supported'] is True
        assert hs['truncate_partition_sql_version'] == '0.73'
        c1 += 2

        q(cm, f"CREATE SCHEMA biz; GRANT USAGE ON SCHEMA biz TO {role_main}")
        q(cm, "SELECT og_lake.configure_internal_owner(%s::name)", (role_main,))
        cat_dir = tmp_root / 'cat'
        idx_dir = tmp_root / 'idx'
        idx_dir.mkdir(parents=True, exist_ok=True)
        os.chmod(idx_dir, 0o755)
        q(cm, "SELECT og_lake.create_catalog('c_v73'::name, 'lake_metadata'::name, %s::text, NULL::text)", (str(cat_dir),))
        q(cm, "SELECT og_lake.set_option('c_v73'::name, 'data_inlining_row_limit'::text, '5000'::text)")
        q(cm, "SELECT og_lake.set_option('c_v73'::name, 'index_root'::text, %s::text)", (str(idx_dir),))

        q(cm, """
            CREATE FOREIGN TABLE biz.t_pre (
                id int8, k int4
            ) SERVER c_v73 OPTIONS (binding_mode 'create', lake_schema 'main')
        """)
        must_fail(cm, "SELECT og_lake.set_partition('biz.t_pre'::regclass, ARRAY['k'], ARRAY['truncate(10)'])", ('0A000',))
        q(cm, "ALTER EXTENSION og_lake UPDATE TO '0.73'")
        assert s(cm, "SELECT extversion FROM pg_extension WHERE extname='og_lake'") == '0.73'
        c1 += 2

        cf = pg_connect(args.profile, db_fresh)
        conns.append(cf)
        q(cf, "CREATE EXTENSION og_lake")
        assert s(cf, "SELECT extversion FROM pg_extension WHERE extname='og_lake'") == '0.74'
        cf.close()
        conns.remove(cf)
        c1 += 1

        # Parameter bounds and disallowed source types
        for bad_tr in ['truncate', 'truncate(0)', 'truncate(-1)', 'truncate(01)', 'truncate(2147483648)', 'truncate(abc)']:
            must_fail(cm, "SELECT og_lake.set_partition('biz.t_pre'::regclass, ARRAY['k'], ARRAY[%s])", ('22023',), (bad_tr,))
        q(cm, """
            CREATE FOREIGN TABLE biz.t_bad_types (
                n numeric(10,2), b boolean, f float8, d date, ts timestamp, u uuid, j json
            ) SERVER c_v73 OPTIONS (binding_mode 'create', lake_schema 'main')
        """)
        for col in ['n', 'b', 'f', 'd', 'ts', 'u', 'j']:
            must_fail(cm, "SELECT og_lake.set_partition('biz.t_bad_types'::regclass, ARRAY[%s], ARRAY['truncate(10)'])", ('0A000',), (col,))
        must_fail(cm, "SELECT og_lake.set_partition('biz.t_pre'::regclass, ARRAY['k','k'], ARRAY['truncate(10)','truncate(10)'])", ('42701',))
        c1 += 3
        stages['stage1_upgrade_and_gates'] = c1

        # Stage 2: INT2/INT4/INT8/TEXT/BYTEA truncate(W) semantics, overflow 22003, metadata & DuckDB parity
        c2 = 0
        # 2a: INT2 -> INT4 widened result
        q(cm, "CREATE TABLE biz.ex_i2 (id int4, k int2)")
        q(cm, "CREATE FOREIGN TABLE biz.t_i2 (id int4, k int2) SERVER c_v73 OPTIONS (binding_mode 'create', lake_schema 'main')")
        q(cm, "SELECT og_lake.set_partition('biz.t_i2'::regclass, ARRAY['k'], ARRAY['truncate(10)'])")
        assert q(cm, "SELECT partition_key_index, column_name, transform FROM og_lake.get_partition('biz.t_i2'::regclass)") == [(0, 'k', 'truncate(10)')]
        i2_vals = [(1, -32768), (2, -32761), (3, -11), (4, -10), (5, -1), (6, 0), (7, 9), (8, 10), (9, 32767), (10, None)]
        for rid, kv in i2_vals:
            q(cm, "INSERT INTO biz.ex_i2 VALUES (%s, %s)", (rid, kv))
            q(cm, "INSERT INTO biz.t_i2 VALUES (%s, %s)", (rid, kv))
        tid_i2 = flush_all(cm, 'biz.t_i2', len(i2_vals))
        pvals_i2 = [r[0] for r in q(cm, "SELECT DISTINCT partition_value FROM lake_metadata.ducklake_file_partition_value WHERE table_id=%s ORDER BY 1 NULLS LAST", (tid_i2,))]
        assert sorted([x for x in pvals_i2 if x is not None]) == sorted(['-32770', '-20', '-10', '0', '10', '32760']) and None in pvals_i2, pvals_i2
        for fpath, pval in q(cm, """
            SELECT d.path, fp.partition_value
            FROM lake_metadata.ducklake_data_file d
            JOIN lake_metadata.ducklake_file_partition_value fp ON fp.data_file_id=d.data_file_id AND fp.table_id=d.table_id
            WHERE d.table_id=%s AND d.end_snapshot IS NULL
        """, (tid_i2,)):
            full = fpath if os.path.isabs(fpath) else str(next(cat_dir.rglob(Path(fpath).name)))
            drows = duckdb_query(f"SELECT DISTINCT CASE WHEN k IS NULL THEN NULL ELSE CAST(floor(k / 10.0) * 10 AS BIGINT) END AS b FROM read_parquet('{full}')")
            assert len(drows) == 1 and (None if drows[0]['b'] is None else str(drows[0]['b'])) == pval, (drows, pval)
        c2 += 3

        # INT2 edge: truncate(2147483647) on -32768 fits int4 (-2147483647)
        q(cm, "CREATE FOREIGN TABLE biz.t_i2_max (id int4, k int2) SERVER c_v73 OPTIONS (binding_mode 'create', lake_schema 'main')")
        q(cm, "SELECT og_lake.set_partition('biz.t_i2_max'::regclass, ARRAY['k'], ARRAY['truncate(2147483647)'])")
        q(cm, "INSERT INTO biz.t_i2_max VALUES (1, -32768), (2, 0), (3, 32767)")
        q(cm, "SELECT og_lake.flush('biz.t_i2_max'::regclass)")
        tid_i2m = s(cm, "SELECT table_id FROM lake_metadata.ducklake_table WHERE table_name='t_i2_max' AND end_snapshot IS NULL")
        pvals_i2m = sorted([r[0] for r in q(cm, "SELECT DISTINCT partition_value FROM lake_metadata.ducklake_file_partition_value WHERE table_id=%s", (tid_i2m,))])
        assert pvals_i2m == ['-2147483647', '0'], pvals_i2m
        c2 += 1

        # 2b: INT4 -> INT8 widened result (INT32_MIN = -2147483648)
        q(cm, "CREATE TABLE biz.ex_i4 (id int4, k int4)")
        q(cm, "CREATE FOREIGN TABLE biz.t_i4 (id int4, k int4) SERVER c_v73 OPTIONS (binding_mode 'create', lake_schema 'main')")
        q(cm, "SELECT og_lake.set_partition('biz.t_i4'::regclass, ARRAY['k'], ARRAY['truncate(1000)'])")
        i4_vals = [(1, -2147483648), (2, -1001), (3, -1000), (4, -1), (5, 0), (6, 999), (7, 1000), (8, 2147483647), (9, None)]
        for rid, kv in i4_vals:
            q(cm, "INSERT INTO biz.ex_i4 VALUES (%s, %s)", (rid, kv))
            q(cm, "INSERT INTO biz.t_i4 VALUES (%s, %s)", (rid, kv))
        tid_i4 = flush_all(cm, 'biz.t_i4', len(i4_vals))
        pvals_i4 = sorted([r[0] for r in q(cm, "SELECT DISTINCT partition_value FROM lake_metadata.ducklake_file_partition_value WHERE table_id=%s AND partition_value IS NOT NULL", (tid_i4,))])
        assert pvals_i4 == sorted(['-2147484000', '-2000', '-1000', '0', '1000', '2147483000']), pvals_i4
        c2 += 2

        # 2c: INT8 + __int128 floor + overflow 22003 on PG_INT64_MIN + conservative pruning bail
        q(cm, "CREATE TABLE biz.ex_i8 (id int4, k int8)")
        q(cm, "CREATE FOREIGN TABLE biz.t_i8 (id int4, k int8) SERVER c_v73 OPTIONS (binding_mode 'create', lake_schema 'main')")
        q(cm, "SELECT og_lake.set_partition('biz.t_i8'::regclass, ARRAY['k'], ARRAY['truncate(10)'])")
        i8_vals = [(1, -9223372036854775800), (2, -11), (3, -1), (4, 0), (5, 9), (6, 10), (7, 9223372036854775800), (8, None)]
        for rid, kv in i8_vals:
            q(cm, "INSERT INTO biz.ex_i8 VALUES (%s, %s)", (rid, kv))
            q(cm, "INSERT INTO biz.t_i8 VALUES (%s, %s)", (rid, kv))
        flush_all(cm, 'biz.t_i8', len(i8_vals))
        # Attempting to flush or write PG_INT64_MIN must fail with 22003
        q(cm, "INSERT INTO biz.t_i8 VALUES (99, -9223372036854775808)")
        must_fail(cm, "SELECT og_lake.flush('biz.t_i8'::regclass)", ('22003',))
        q(cm, "DELETE FROM biz.t_i8 WHERE id = 99")
        # Querying PG_INT64_MIN must NOT error during pruning; returns exact 0 rows
        assert q(cm, "SELECT id FROM biz.t_i8 WHERE k = -9223372036854775808") == []
        assert q(cm, "SELECT id FROM biz.t_i8 WHERE k < -9223372036854775808") == []
        c2 += 3

        # 2d: TEXT UTF-8 multibyte character truncation (C collation DB + non-C DB test below)
        q(cm, 'CREATE TABLE biz.ex_txt (id int4, k text, note text)')
        q(cm, 'CREATE FOREIGN TABLE biz.t_txt (id int4, k text, note text) SERVER c_v73 OPTIONS (binding_mode \'create\', lake_schema \'main\')')
        q(cm, "SELECT og_lake.set_partition('biz.t_txt'::regclass, ARRAY['k'], ARRAY['truncate(2)'])")
        txt_vals = [
            (1, '你好世界', 'cn1'), (2, '你好中国', 'cn2'),
            (3, 'a你b', 'mix1'), (4, 'a你c', 'mix2'),
            (5, '€😀xyz', 'emo1'), (6, '€😀w', 'emo2'),
            (7, 'abcdef', 'asc1'), (8, 'ab123', 'asc2'),
            (9, 'x', 'short'), (10, '', 'empty'), (11, None, 'nullk'),
        ]
        for rid, kv, nv in txt_vals:
            q(cm, "INSERT INTO biz.ex_txt VALUES (%s, %s, %s)", (rid, kv, nv))
            q(cm, "INSERT INTO biz.t_txt VALUES (%s, %s, %s)", (rid, kv, nv))
        tid_txt = flush_all(cm, 'biz.t_txt', len(txt_vals))
        pvals_txt = [r[0] for r in q(cm, "SELECT DISTINCT partition_value FROM lake_metadata.ducklake_file_partition_value WHERE table_id=%s ORDER BY 1 NULLS LAST", (tid_txt,))]
        assert sorted([x for x in pvals_txt if x is not None]) == sorted(['你好', 'a你', '€😀', 'ab', 'x', '']) and None in pvals_txt, pvals_txt
        for fpath, pval in q(cm, """
            SELECT d.path, fp.partition_value
            FROM lake_metadata.ducklake_data_file d
            JOIN lake_metadata.ducklake_file_partition_value fp ON fp.data_file_id=d.data_file_id AND fp.table_id=d.table_id
            WHERE d.table_id=%s AND d.end_snapshot IS NULL
        """, (tid_txt,)):
            full = fpath if os.path.isabs(fpath) else str(next(cat_dir.rglob(Path(fpath).name)))
            drows = duckdb_query(f"SELECT DISTINCT left(k, 2) AS p FROM read_parquet('{full}')")
            assert len(drows) == 1 and drows[0]['p'] == pval, (drows, pval)
        c2 += 3

        # 2e: BYTEA truncate(3) including \x00, \xff, ASCII escapes (\\, ', "), SMP DOP=4
        q(cm, "CREATE TABLE biz.ex_bin (id int4, k bytea)")
        q(cm, "CREATE FOREIGN TABLE biz.t_bin (id int4, k bytea) SERVER c_v73 OPTIONS (binding_mode 'create', lake_schema 'main')")
        q(cm, "SELECT og_lake.set_partition('biz.t_bin'::regclass, ARRAY['k'], ARRAY['truncate(3)'])")
        bin_vals = [
            (1, b'\x00\xff\x01\x02'), (2, b'\x00\xff\x01\x99'),
            (3, b'\x00\xfe\x01\x02'), (4, b"ab\\'q"),
            (5, b"ab\\x"), (6, b'a'), (7, b''), (8, None),
        ]
        for rid, bv in bin_vals:
            q(cm, "INSERT INTO biz.ex_bin VALUES (%s, %s)", (rid, psycopg2.Binary(bv) if bv is not None else None))
            q(cm, "INSERT INTO biz.t_bin VALUES (%s, %s)", (rid, psycopg2.Binary(bv) if bv is not None else None))
        tid_bin = flush_all(cm, 'biz.t_bin', len(bin_vals))
        pvals_bin = sorted([r[0] for r in q(cm, "SELECT DISTINCT partition_value FROM lake_metadata.ducklake_file_partition_value WHERE table_id=%s AND partition_value IS NOT NULL", (tid_bin,))])
        assert pvals_bin == sorted(['\\x00\\xFF\\x01', '\\x00\\xFE\\x01', 'ab\\x5C', 'a', '']), pvals_bin
        for fpath, pval in q(cm, """
            SELECT d.path, fp.partition_value
            FROM lake_metadata.ducklake_data_file d
            JOIN lake_metadata.ducklake_file_partition_value fp ON fp.data_file_id=d.data_file_id AND fp.table_id=d.table_id
            WHERE d.table_id=%s AND d.end_snapshot IS NULL
        """, (tid_bin,)):
            full = fpath if os.path.isabs(fpath) else str(next(cat_dir.rglob(Path(fpath).name)))
            drows = duckdb_query(f"SELECT DISTINCT ((k::BLOB)[1:3])::VARCHAR AS p FROM read_parquet('{full}')")
            assert len(drows) == 1 and drows[0]['p'] == pval, (drows, pval)
        c2 += 3

        # 2f: Non-C UTF8 collation DB conservative non-pruning + exact heap parity
        db_nonc = f'dbv73_nc_{tag}'
        q(admin, f"CREATE DATABASE {db_nonc} WITH TEMPLATE template0 DBCOMPATIBILITY 'PG' ENCODING 'UTF8' LC_COLLATE 'en_US.utf8' LC_CTYPE 'en_US.utf8'")
        created_dbs.append(db_nonc)
        cn = pg_connect(args.profile, db_nonc)
        conns.append(cn)
        q(cn, "CREATE EXTENSION og_lake")
        q(cn, f"CREATE SCHEMA biz; GRANT USAGE ON SCHEMA biz TO {role_main}")
        q(cn, "SELECT og_lake.configure_internal_owner(%s::name)", (role_main,))
        nc_cat = tmp_root / 'cat_nonc'
        q(cn, "SELECT og_lake.create_catalog('c_nc'::name, 'lake_metadata'::name, %s::text, NULL::text)", (str(nc_cat),))
        q(cn, "SELECT og_lake.set_option('c_nc'::name, 'data_inlining_row_limit'::text, '5000'::text)")
        q(cn, "CREATE TABLE biz.ex_nonc (id int4, k text)")
        q(cn, "CREATE FOREIGN TABLE biz.t_nonc (id int4, k text) SERVER c_nc OPTIONS (binding_mode 'create', lake_schema 'main')")
        q(cn, "SELECT og_lake.set_partition('biz.t_nonc'::regclass, ARRAY['k'], ARRAY['truncate(2)'])")
        for rid, kv in [(1, 'apple'), (2, 'Banana'), (3, 'apricot'), (4, None)]:
            q(cn, "INSERT INTO biz.ex_nonc VALUES (%s, %s)", (rid, kv))
            q(cn, "INSERT INTO biz.t_nonc VALUES (%s, %s)", (rid, kv))
        flush_all(cn, 'biz.t_nonc', 4)
        set_vec_smp(cn, 1, 'off', enable_vec=False, enable_smp=False)
        exp_nonc = explain_text(cn, "SELECT id FROM biz.t_nonc WHERE k = 'apple'")
        assert 'Lake Partition Files Skipped: 0' in exp_nonc, exp_nonc
        exp_nonc_null = explain_text(cn, "SELECT id FROM biz.t_nonc WHERE k IS NULL")
        assert 'Lake Partition Files Skipped: 2' in exp_nonc_null, exp_nonc_null
        for pred in ["k = 'apple'", "k > 'apple'", "k < 'Banana'", "k IS NULL"]:
            assert q(cn, f"SELECT id FROM biz.t_nonc WHERE {pred} ORDER BY id") == q(cn, f"SELECT id FROM biz.ex_nonc WHERE {pred} ORDER BY id")
        cn.close()
        conns.remove(cn)
        c2 += 2
        stages['stage2_truncate_all_types_and_oracle'] = c2

        # Stage 3: Partition Pruning (=, <, <=, >, >=, IS NULL, PREPARE, multi-key) + DML/Flush/Rewrite/Merge/TimeTravel/CDC + Vec + SMP
        c3 = 0
        set_vec_smp(cm, 1, 'off', enable_vec=False, enable_smp=False)
        prune_cases = [
            ('t_i2', 'ex_i2', 'k = -5', 5),
            ('t_i2', 'ex_i2', 'k < -10', 4),
            ('t_i2', 'ex_i2', 'k >= 10', 5),
            ('t_i2', 'ex_i2', 'k IS NULL', 6),
            ('t_i4', 'ex_i4', 'k >= 1000', 4),
            ('t_i4', 'ex_i4', 'k <= -1', 3),
            ('t_i8', 'ex_i8', 'k = 5', 6),
            ('t_i8', 'ex_i8', 'k > 9', 4),
            ('t_i8', 'ex_i8', 'k >= 10', 5),
            ('t_txt', 'ex_txt', "k = '你好世界'", 6),
            ('t_txt', 'ex_txt', "k >= '€'", 5),
            ('t_txt', 'ex_txt', "k < 'ab'", 3),
            ('t_txt', 'ex_txt', "k IS NOT NULL", 1),
            ('t_bin', 'ex_bin', r"k = E'\\x00ff0102'::bytea", 5),
            ('t_bin', 'ex_bin', r"k >= E'\\x6162'::bytea", 3),
            ('t_bin', 'ex_bin', "k IS NULL", 5),
        ]
        for ft, ex, pred, min_skip in prune_cases:
            exp = explain_text(cm, f"SELECT id FROM biz.{ft} WHERE {pred}")
            assert 'Lake Partition Files Skipped:' in exp, (ft, pred, exp)
            skipped = int([ln.split(':')[1].strip() for ln in exp.splitlines() if 'Lake Partition Files Skipped:' in ln][0])
            assert skipped >= min_skip, (ft, pred, skipped, min_skip, exp)
            assert q(cm, f"SELECT id FROM biz.{ft} WHERE {pred} ORDER BY id") == q(cm, f"SELECT id FROM biz.{ex} WHERE {pred} ORDER BY id")
        c3 += 5

        # Prepared parameter pruning (PARAM_EXTERN)
        q(cm, "PREPARE p_trunc (text) AS SELECT id FROM biz.t_txt WHERE k = $1 ORDER BY id")
        for i in range(6):
            exp_p = explain_text(cm, "EXECUTE p_trunc('a你b')")
            assert 'Lake Partition Files Skipped: 6' in exp_p, (i, exp_p)
            assert q(cm, "EXECUTE p_trunc('a你b')") == [(3,)]
        q(cm, "DEALLOCATE p_trunc")
        c3 += 1

        # Vectorized VecForeignScan with partition pruning + SMP DOP=4 across all types (incl BYTEA)
        set_vec_smp(cm, 1, 'force', enable_vec=True, enable_smp=False)
        exp_vec_pr = explain_text(cm, "SELECT count(*), sum(id) FROM biz.t_txt WHERE k >= 'ab'")
        assert 'Lake Batch Delivery: native VecForeignScan' in exp_vec_pr and 'Lake Partition Files Skipped:' in exp_vec_pr, exp_vec_pr
        assert q(cm, "SELECT count(*), sum(id) FROM biz.t_txt WHERE k >= 'ab'") == q(cm, "SELECT count(*), sum(id) FROM biz.ex_txt WHERE k >= 'ab'")
        set_vec_smp(cm, 4, 'force', enable_vec=True, enable_smp=True)
        for ft, ex in [('t_i2', 'ex_i2'), ('t_i4', 'ex_i4'), ('t_i8', 'ex_i8'), ('t_txt', 'ex_txt'), ('t_bin', 'ex_bin')]:
            assert q(cm, f"SELECT id, k FROM biz.{ft} ORDER BY id") == q(cm, f"SELECT id, k FROM biz.{ex} ORDER BY id")
        c3 += 3

        # Multi-key (identity, truncate) + DML/Flush/Rewrite/Merge/TimeTravel/CDC
        set_vec_smp(cm, 1, 'off', enable_vec=False, enable_smp=False)
        q(cm, 'CREATE TABLE biz.ex_mk (id int4, grp int4, k text)')
        q(cm, 'CREATE FOREIGN TABLE biz.t_mk (id int4, grp int4, k text) SERVER c_v73 OPTIONS (binding_mode \'create\', lake_schema \'main\')')
        q(cm, "SELECT og_lake.set_partition('biz.t_mk'::regclass, ARRAY['grp','k'], ARRAY['identity','truncate(2)'])")
        mk_rows = [(1, 1, 'alpha1'), (2, 1, 'alpha2'), (3, 1, 'beta1'), (4, 2, 'alpha3'), (5, 2, 'gamma1')]
        for rid, grp, kv in mk_rows:
            q(cm, "INSERT INTO biz.ex_mk VALUES (%s, %s, %s)", (rid, grp, kv))
            q(cm, "INSERT INTO biz.t_mk VALUES (%s, %s, %s)", (rid, grp, kv))
        flush_all(cm, 'biz.t_mk', len(mk_rows))
        s_after_flush1 = s(cm, "SELECT id FROM og_lake.current_snapshot('c_v73'::name)")
        # Duplicate one group across a second flush, then rewrite & merge
        q(cm, "INSERT INTO biz.ex_mk VALUES (6, 1, 'alpha9')")
        q(cm, "INSERT INTO biz.t_mk VALUES (6, 1, 'alpha9')")
        q(cm, "SELECT og_lake.flush('biz.t_mk'::regclass)")
        # UPDATE & DELETE across files
        q(cm, "UPDATE biz.ex_mk SET k = 'omega1' WHERE id = 2")
        q(cm, "UPDATE biz.t_mk SET k = 'omega1' WHERE id = 2")
        q(cm, "DELETE FROM biz.ex_mk WHERE id = 3")
        q(cm, "DELETE FROM biz.t_mk WHERE id = 3")
        flush_all(cm, 'biz.t_mk', None)
        assert q(cm, "SELECT id, grp, k FROM biz.t_mk ORDER BY id") == q(cm, "SELECT id, grp, k FROM biz.ex_mk ORDER BY id")
        # Time travel + table changes
        tt_rows = q(cm, f"SELECT id, grp, k FROM og_lake.time_travel('biz.t_mk'::regclass, {s_after_flush1}::bigint) AS (id int4, grp int4, k text) ORDER BY id")
        assert tt_rows == mk_rows, tt_rows
        s_now = s(cm, "SELECT id FROM og_lake.current_snapshot('c_v73'::name)")
        cdc_rows = q(cm, f"SELECT change_type, id FROM og_lake.table_changes('biz.t_mk'::regclass, {s_after_flush1}::bigint, {s_now}::bigint) AS (snapshot_id bigint, rowid bigint, change_type text, id int4, grp int4, k text) ORDER BY snapshot_id, rowid, change_type")
        assert len(cdc_rows) >= 3, cdc_rows
        # Rewrite the two (1, 'al') files together
        al_files = [r[0] for r in q(cm, """
            SELECT d.data_file_id
            FROM lake_metadata.ducklake_data_file d
            JOIN lake_metadata.ducklake_table t ON t.table_id=d.table_id AND t.end_snapshot IS NULL
            JOIN lake_metadata.ducklake_file_partition_value fp0 ON fp0.data_file_id=d.data_file_id AND fp0.partition_key_index=0
            JOIN lake_metadata.ducklake_file_partition_value fp1 ON fp1.data_file_id=d.data_file_id AND fp1.partition_key_index=1
            WHERE t.table_name='t_mk' AND d.end_snapshot IS NULL AND fp0.partition_value='1' AND fp1.partition_value='al'
            ORDER BY d.data_file_id
        """)]
        assert len(al_files) == 2, al_files
        q(cm, "SELECT og_lake.rewrite_data_files('biz.t_mk'::regclass, %s::bigint[])", (al_files,))
        assert q(cm, "SELECT id, grp, k FROM biz.t_mk ORDER BY id") == q(cm, "SELECT id, grp, k FROM biz.ex_mk ORDER BY id")
        # Multi-key prune
        exp_mk = explain_text(cm, "SELECT id FROM biz.t_mk WHERE grp = 1 AND k = 'alpha9'")
        skipped_mk = int([ln.split(':')[1].strip() for ln in exp_mk.splitlines() if 'Lake Partition Files Skipped:' in ln][0])
        assert skipped_mk >= 3 and 'Lake Partition Files Admitted: 1' in exp_mk, exp_mk
        c3 += 5
        stages['stage3_pruning_dml_rewrite_vec_smp'] = c3

        # Stage 4: VecForeignScan projection pushdown & DiskANN+PQ functional closure
        c4 = 0
        q(cm, "CREATE TABLE biz.ex_proj (id int8, grp int4, v float8, flag bool, payload text)")
        q(cm, "CREATE FOREIGN TABLE biz.t_proj (id int8, grp int4, v float8, flag bool, payload text) SERVER c_v73 OPTIONS (binding_mode 'create', lake_schema 'main')")
        for b in range(4):
            ins = f"""
                SELECT g::int8, (g % 7)::int4, (g * 0.125)::float8, (g % 2 = 0),
                       CASE WHEN g % 13 = 0 THEN NULL ELSE repeat('p😀_' || (g % 19), 5) END
                FROM generate_series({b * 2000 + 1}, {(b + 1) * 2000}) AS g
            """
            q(cm, "INSERT INTO biz.ex_proj " + ins)
            q(cm, "INSERT INTO biz.t_proj " + ins)
            q(cm, "SELECT og_lake.flush('biz.t_proj'::regclass)")
        # Add inline tail + deleted file/inline rows + wide payload row (>16KB VarBuf threshold)
        q(cm, "INSERT INTO biz.ex_proj VALUES (9001, 3, 1.5, true, repeat('W', 4000)), (9002, 5, NULL, NULL, NULL), (9003, 3, 2.5, false, repeat('Z', 20000))")
        q(cm, "INSERT INTO biz.t_proj VALUES (9001, 3, 1.5, true, repeat('W', 4000)), (9002, 5, NULL, NULL, NULL), (9003, 3, 2.5, false, repeat('Z', 20000))")
        q(cm, "DELETE FROM biz.ex_proj WHERE id IN (5, 2005, 9002)")
        q(cm, "DELETE FROM biz.t_proj WHERE id IN (5, 2005, 9002)")
        q(cm, "ANALYZE biz.t_proj")
        q(cm, "ANALYZE biz.ex_proj")

        for dop in (1, 4):
            set_vec_smp(cm, dop, 'force', enable_vec=True, enable_smp=(dop > 1))
            # 0-col count(*)
            assert s(cm, "SELECT count(*) FROM biz.t_proj") == s(cm, "SELECT count(*) FROM biz.ex_proj")
            # Projected fixed cols, unprojected payload
            assert q(cm, "SELECT count(v), round(sum(v)::numeric, 3), min(id), max(id), count(flag) FROM biz.t_proj WHERE grp IN (1, 3)") == q(cm, "SELECT count(v), round(sum(v)::numeric, 3), min(id), max(id), count(flag) FROM biz.ex_proj WHERE grp IN (1, 3)")
            # Projected text only + ReScan nested loop join
            assert q(cm, "SELECT id, payload FROM biz.t_proj WHERE grp = 3 AND id > 7900 ORDER BY id") == q(cm, "SELECT id, payload FROM biz.ex_proj WHERE grp = 3 AND id > 7900 ORDER BY id")
            assert q(cm, "SELECT count(*), sum(length(payload)) FROM biz.t_proj GROUP BY grp ORDER BY grp") == q(cm, "SELECT count(*), sum(length(payload)) FROM biz.ex_proj GROUP BY grp ORDER BY grp")
        c4 += 4

        # Schema evolution with projection pushdown (ADD COLUMN DEFAULT + DROP COLUMN)
        set_vec_smp(cm, 1, 'force', enable_vec=True, enable_smp=False)
        q(cm, "ALTER TABLE biz.ex_proj ADD COLUMN extra text DEFAULT 'def_v73'")
        q(cm, "ALTER FOREIGN TABLE biz.t_proj ADD COLUMN extra text DEFAULT 'def_v73'")
        assert q(cm, "SELECT count(*), count(extra), min(extra) FROM biz.t_proj") == q(cm, "SELECT count(*), count(extra), min(extra) FROM biz.ex_proj")
        # Nested Loop ReScan over VecForeignScan
        q(cm, "SET enable_hashjoin = off; SET enable_mergejoin = off;")
        assert q(cm, "SELECT count(*), sum(t.id) FROM (VALUES (2), (5)) AS d(g) JOIN biz.t_proj t ON t.grp = d.g AND t.id < 50") == q(cm, "SELECT count(*), sum(t.id) FROM (VALUES (2), (5)) AS d(g) JOIN biz.ex_proj t ON t.grp = d.g AND t.id < 50")
        q(cm, "SET enable_hashjoin = on; SET enable_mergejoin = on;")
        c4 += 2

        # DiskANN+PQ Rescan Cache, SMP DOP=1 vs DOP=4, and Scalar Filter (requires DiskANN catalog: ann-debug or release)
        if args.profile in ('ann-debug', 'release'):
            set_vec_smp(cm, 1, 'off', enable_vec=False, enable_smp=False)
            q(cm, "CREATE TABLE biz.ex_ann (id int4, tag int4, vec vector(32))")
            q(cm, "CREATE FOREIGN TABLE biz.t_ann (id int4, tag int4, vec vector(32)) SERVER c_v73 OPTIONS (binding_mode 'create', lake_schema 'main')")
            for b in range(4):
                ins = f"""
                    SELECT g, (g % 4)::int4,
                           ('[' || string_agg((((((g * 31 + k * 17) % 20000) - 10000) / 10000.0))::text, ',') || ']')::vector(32)
                    FROM generate_series({b * 300 + 1}, {(b + 1) * 300}) AS g, generate_series(0, 31) AS k
                    GROUP BY g
                """
                q(cm, "INSERT INTO biz.ex_ann " + ins)
                q(cm, "INSERT INTO biz.t_ann " + ins)
                q(cm, "SELECT og_lake.flush('biz.t_ann'::regclass)")
            ix = s(cm, "SELECT og_lake.create_lake_index('biz.t_ann'::regclass, 'ix_ann_pq'::name, '{\"version\":1,\"kind\":\"vector\",\"method\":\"diskann\",\"column\":\"vec\",\"metric\":\"l2\",\"pq\":true,\"pq_chunks\":8}'::jsonb)")
            q(cm, "SELECT og_lake.build_lake_index(%s::uuid)", (ix,))
            q(cm, "SELECT og_lake.build_lake_index(%s::uuid)", (ix,))
            assert s(cm, "SELECT count(*) FROM og_lake_internal.index_segment WHERE index_uuid=%s::uuid AND state='READY' AND format_version=2", (ix,)) == 4
            q(cm, "ANALYZE biz.t_ann")
            qvec = '[' + ','.join('0.1' for _ in range(32)) + ']'
            q(cm, "SET enable_lake_ann = on; SET ann_orderby_distance_reuse = off; SET query_max_mem = 0; SET enable_seqscan = off;")
            exp_ann1 = explain_text(cm, f"SELECT id FROM biz.t_ann WHERE vec IS NOT NULL ORDER BY vec <-> '{qvec}'::vector(32) LIMIT 10")
            assert 'Verified PQ File Coverage: 4' in exp_ann1, exp_ann1
            res_dop1 = q(cm, f"SELECT id, round((vec <-> '{qvec}'::vector(32))::numeric, 6) FROM biz.t_ann WHERE vec IS NOT NULL ORDER BY vec <-> '{qvec}'::vector(32) LIMIT 10")
            # Correlated / Prepared PQ rescan stability (verifying AnnRescanCache / SharedAnnPool correctness across repeated runs)
            q(cm, "PREPARE p_pq (vector(32)) AS SELECT id FROM biz.t_ann WHERE vec IS NOT NULL ORDER BY vec <-> $1 LIMIT 10")
            for _ in range(4):
                assert [r[0] for r in q(cm, f"EXECUTE p_pq('{qvec}'::vector(32))")] == [r[0] for r in res_dop1]
            q(cm, "DEALLOCATE p_pq")
            # Compare filtered ANN + SMP DOP=4
            q(cm, "SET query_dop = 4; SET enable_force_smp = on; SET enable_lake_smp = on;")
            exp_ann4 = explain_text(cm, f"SELECT id FROM biz.t_ann WHERE vec IS NOT NULL ORDER BY vec <-> '{qvec}'::vector(32) LIMIT 10")
            assert 'Lake SMP Planned DOP: 4' in exp_ann4 and 'Lake SMP ANN PQ: admitted' in exp_ann4, exp_ann4
            res_dop4 = q(cm, f"SELECT id, round((vec <-> '{qvec}'::vector(32))::numeric, 6) FROM biz.t_ann WHERE vec IS NOT NULL ORDER BY vec <-> '{qvec}'::vector(32) LIMIT 10")
            assert len(res_dop4) == 10
            assert [r[1] for r in res_dop4] == [r[1] for r in res_dop1], (res_dop4, res_dop1)
            assert res_dop4[0][0] in {r[0] for r in res_dop1 if r[1] == res_dop1[0][1]}, (res_dop4, res_dop1)
            # Filtered ANN on PQ (tag = 2)
            res_filt = q(cm, f"SELECT id FROM biz.t_ann WHERE vec IS NOT NULL AND tag = 2 ORDER BY vec <-> '{qvec}'::vector(32) LIMIT 10")
            assert len(res_filt) == 10 and all(rid % 4 == 2 for (rid,) in res_filt), res_filt
            q(cm, "SET query_dop = 1; SET enable_force_smp = off; SET enable_lake_ann = off; SET enable_seqscan = on;")
            c4 += 5
        stages['stage4_vec_projection_and_pq_rescan'] = c4

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
