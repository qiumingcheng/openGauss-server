#!/usr/bin/env python3
"""Comprehensive SQL0.61 (PGL-2) external Parquet/CSV FDW & schema-inference verification suite."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import secrets
import shutil
import subprocess
import sys
import tempfile

import psycopg2
from psycopg2 import sql

ROOT = Path('/home/qiumc/data/code/cpp')
TESTS_DIR = ROOT / 'integration/opengauss-native-lake'
sys.path.insert(0, str(TESTS_DIR))
from native_runtime import PORT, PROFILE, RUNTIME

DUCKDB_BIN = ROOT / '.reference/opengauss-native-lake/build/duckdb-gcc10/duckdb'


def connect(dbname='postgres', user=None, password=None, autocommit=True):
    kwargs = dict(
        host=str(RUNTIME / 'socket'),
        port=PORT,
        dbname=dbname,
        user=user or os.getenv('USER', 'qiumc'),
        connect_timeout=15,
        application_name='og_lake_test_061',
    )
    if password is not None:
        kwargs['password'] = password
    conn = psycopg2.connect(**kwargs)
    conn.autocommit = autocommit
    with conn.cursor() as cur:
        cur.execute('SET statement_timeout = 30000')
        cur.execute("SET behavior_compat_options = 'display_leading_zero'")
    return conn


def scalar(conn, stmt, params=()):
    with conn.cursor() as cur:
        cur.execute(stmt, params)
        rows = cur.fetchall()
    assert len(rows) == 1 and len(rows[0]) == 1, f'expected 1x1, got {rows!r} for {stmt}'
    return rows[0][0]


def fetchall(conn, stmt, params=()):
    with conn.cursor() as cur:
        cur.execute(stmt, params)
        return cur.fetchall()


def exec_sql(conn, stmt, params=()):
    with conn.cursor() as cur:
        cur.execute(stmt, params)


def assert_raises(conn, stmt, params=(), expected_sqlstate=None, msg_substr=None):
    try:
        exec_sql(conn, stmt, params)
    except psycopg2.Error as exc:
        if expected_sqlstate is not None:
            assert exc.pgcode == expected_sqlstate, (
                f'expected SQLSTATE {expected_sqlstate}, got {exc.pgcode}: {exc}'
            )
        if msg_substr is not None:
            assert msg_substr.lower() in str(exc).lower(), (
                f'expected {msg_substr!r} in error message, got: {exc}'
            )
        return exc
    raise AssertionError(f'expected SQL error for: {stmt}')


def run_duckdb_json(sql_text: str):
    proc = subprocess.run(
        [str(DUCKDB_BIN), '-batch', '-light-mode', '-json', '-c', sql_text],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env={**os.environ, 'TERM': 'dumb'},
    )
    if proc.returncode != 0:
        raise RuntimeError(f'DuckDB JSON failed ({proc.returncode}): {proc.stderr}\nSQL:\n{sql_text}')
    out = proc.stdout.strip()
    return json.loads(out) if out else []


def run_duckdb_exec(sql_text: str):
    proc = subprocess.run(
        [str(DUCKDB_BIN), '-batch', '-light-mode', '-c', sql_text],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env={**os.environ, 'TERM': 'dumb'},
    )
    if proc.returncode != 0:
        raise RuntimeError(f'DuckDB failed ({proc.returncode}): {proc.stderr}\nSQL:\n{sql_text}')


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--output', type=Path, required=True)
    args = ap.parse_args()

    admin_user = os.getenv('USER', 'qiumc')
    suffix = secrets.token_hex(6)
    db_name = f'sql061_pgl2_{suffix}'
    db_old = f'sql061_old_{suffix}'
    ord_user = f'og_pgl2_u_{suffix}'
    copy_user = f'og_pgl2_c_{suffix}'
    meta_owner = f'og_pgl2_o_{suffix}'
    pwd = 'Og1!' + secrets.token_hex(12)

    workdir = Path(tempfile.mkdtemp(prefix=f'pgl2_suite_{suffix}_', dir='/tmp'))
    ext_root = workdir / 'ext_root'
    ext_sub = ext_root / 'nested'
    escape_dir = workdir / 'outside'
    lake_data = workdir / 'lake_data'
    ext_sub.mkdir(parents=True, exist_ok=True)
    escape_dir.mkdir(parents=True, exist_ok=True)
    os.chmod(workdir, 0o755)
    os.chmod(ext_root, 0o755)
    os.chmod(ext_sub, 0o755)
    os.chmod(escape_dir, 0o755)

    stages = []
    duckdb_checks = 0

    def rec(name, details=None):
        stages.append({'stage': name, 'status': 'passed', **(details or {})})

    conn_adm = connect('postgres')
    try:
        def md5_pw(role_name, raw_pw):
            return 'md5' + hashlib.md5((raw_pw + role_name).encode('utf-8')).hexdigest()

        # Prepare roles and isolated databases
        for role_sql in [
            sql.SQL('CREATE ROLE {} LOGIN ENCRYPTED PASSWORD {}').format(sql.Identifier(ord_user), sql.Literal(md5_pw(ord_user, pwd))),
            sql.SQL('CREATE ROLE {} LOGIN ENCRYPTED PASSWORD {}').format(sql.Identifier(copy_user), sql.Literal(md5_pw(copy_user, pwd))),
            sql.SQL('CREATE ROLE {} NOLOGIN NOINHERIT PASSWORD {}').format(sql.Identifier(meta_owner), sql.Literal(pwd)),
            sql.SQL('GRANT gs_role_copy_files TO {}').format(sql.Identifier(copy_user)),
            sql.SQL("CREATE DATABASE {} DBCOMPATIBILITY 'PG'").format(sql.Identifier(db_name)),
            sql.SQL("CREATE DATABASE {} DBCOMPATIBILITY 'PG'").format(sql.Identifier(db_old)),
        ]:
            exec_sql(conn_adm, role_sql)

        # Generate DuckDB reference Parquet & CSV/TSV datasets (no Iceberg field_ids, multi-row-group)
        pq_rich = ext_root / 'rich_types.parquet'
        pq_multi = ext_sub / 'multi_5000.parquet'
        pq_vec = ext_root / 'vectors.parquet'
        csv_hdr = ext_root / 'events.csv'
        csv_nohdr = ext_sub / 'raw_nohdr.csv'
        tsv_file = ext_root / 'tabbed.tsv'
        csv_empty = ext_root / 'empty.csv'
        outside_csv = escape_dir / 'secret.csv'

        run_duckdb_exec(f"""
COPY (
    SELECT
        i::INTEGER AS id,
        (i - 10)::SMALLINT AS s16,
        (i * 10000000000::BIGINT)::BIGINT AS b64,
        (i % 2 = 0) AS flag,
        (i::FLOAT / 4.0)::FLOAT AS f32,
        (i * 1.25)::DOUBLE AS f64,
        CASE WHEN i % 3 = 0 THEN NULL WHEN i = 2 THEN '' ELSE '湖/' || i::VARCHAR END AS txt,
        CASE WHEN i = 4 THEN NULL ELSE encode('bin-' || i::VARCHAR)::BLOB END AS payload,
        ('00000000-0000-4000-8000-' || lpad(to_hex(i), 12, '0'))::UUID AS uid,
        CASE WHEN i = 3 THEN NULL ELSE ('{{"k":' || i::VARCHAR || '}}')::JSON END AS js,
        (123.450 + i::DECIMAL(12, 3))::DECIMAL(12, 3) AS amt,
        (DATE '2026-01-01' + i::INTEGER)::DATE AS dt,
        (TIMESTAMP '2026-01-01 00:00:00' + INTERVAL (i::INTEGER * 37) SECOND)::TIMESTAMP AS ts,
        (TIMESTAMPTZ '2026-01-01 00:00:00+00' + INTERVAL (i::INTEGER * 91) SECOND)::TIMESTAMPTZ AS tstz
    FROM range(1, 13) tbl(i)
) TO '{pq_rich}' (FORMAT PARQUET, ROW_GROUP_SIZE 5);

COPY (
    SELECT
        i::INTEGER AS id,
        CASE WHEN i % 5 = 0 THEN NULL WHEN i % 7 = 0 THEN '' ELSE '湖/csv' END AS text_col
    FROM range(0, 5000) tbl(i)
) TO '{pq_multi}' (FORMAT PARQUET, ROW_GROUP_SIZE 1000);

COPY (
    SELECT
        i::INTEGER AS id,
        [i::FLOAT, (i + 0.5)::FLOAT, (-i)::FLOAT]::FLOAT[] AS emb
    FROM range(1, 6) tbl(i)
) TO '{pq_vec}' (FORMAT PARQUET);

COPY (
    SELECT * FROM (VALUES
        (1, 12.5, true, '2026-10-01', 'alpha,one'),
        (2, 0.25, false, '2026-10-02', 'quote "two"'),
        (3, NULL, true, '2026-10-03', '湖/utf8'),
        (4, -7.75, NULL, NULL, '')
    ) AS t(id, score, ok, day_col, note)
) TO '{csv_hdr}' (FORMAT CSV, HEADER true);

COPY (
    SELECT * FROM (VALUES
        (10, 'r10'),
        (20, ''),
        (30, 'r30')
    ) AS t(c0, c1)
) TO '{csv_nohdr}' (FORMAT CSV, HEADER false);

COPY (
    SELECT * FROM (VALUES
        (101, 't101'),
        (102, 't102')
    ) AS t(k, v)
) TO '{tsv_file}' (FORMAT CSV, HEADER true, DELIMITER E'\t');
""")
        csv_empty.write_text('', encoding='utf-8')
        outside_csv.write_text('secret_id\n999\n', encoding='utf-8')
        (ext_root / 'link_escape.csv').symlink_to(outside_csv)
        (ext_root / 'link_escape.parquet').symlink_to(pq_rich)
        sym_sub = ext_root / 'sym_dir'
        sym_sub.symlink_to(escape_dir)

        # Stage 1: Down-level SQL0.60 / SQL0.59 contract gate isolation & ALTER EXTENSION UPGRADE
        c_old = connect(db_old)
        try:
            exec_sql(c_old, "CREATE EXTENSION og_lake VERSION '0.59'")
            assert_raises(
                c_old,
                f"CREATE SERVER s_bad FOREIGN DATA WRAPPER og_lake_file_fdw OPTIONS (external_root {repr(str(ext_root))})",
                expected_sqlstate='42704',
            )
            assert_raises(
                c_old,
                "CREATE SERVER s_bad FOREIGN DATA WRAPPER og_lake_fdw OPTIONS (external_root '/tmp')",
                expected_sqlstate='HV00D',
            )
            exec_sql(c_old, "ALTER EXTENSION og_lake UPDATE TO '0.60'")
            assert_raises(
                c_old,
                "SELECT * FROM og_lake.external_file_info('s', 'a.parquet')",
                expected_sqlstate='42883',
            )
            exec_sql(c_old, "ALTER EXTENSION og_lake UPDATE TO '0.61'")
            hs_old = json.loads(scalar(c_old, "SELECT og_lake.host_status()"))
            assert hs_old['module_version'] in ('0.61', '0.73', '0.74') and hs_old['external_file_fdw'] is True
            exec_sql(
                c_old,
                sql.SQL("CREATE SERVER srv_up FOREIGN DATA WRAPPER og_lake_file_fdw OPTIONS (external_root {})").format(
                    sql.Literal(str(ext_root))
                ),
            )
            cnt_up = scalar(c_old, "SELECT count(*) FROM og_lake.describe_external_file('srv_up', 'rich_types.parquet')")
            assert cnt_up == 14
        finally:
            c_old.close()
        rec('upgrade_chain_059_060_061_and_isolation')

        # Main 0.61 test DB
        c_db = connect(db_name)
        c_ord = connect(db_name, user=ord_user, password=pwd)
        try:
            exec_sql(c_db, "CREATE EXTENSION og_lake VERSION '0.61'")
            hs = json.loads(scalar(c_db, "SELECT og_lake.host_status()"))
            assert hs['module_version'] in ('0.61', '0.73', '0.74')
            assert hs['external_file_fdw'] is True
            assert hs['external_file_sql_version'] == '0.61'
            assert hs['merge_adjacent_files'] is True

            # Stage 2: Security & path confinement matrix
            assert_raises(c_ord, "CREATE SERVER s_fail1 FOREIGN DATA WRAPPER og_lake_file_fdw OPTIONS (external_root '/tmp')", expected_sqlstate='42501')
            exec_sql(c_db, "GRANT USAGE ON FOREIGN DATA WRAPPER og_lake_file_fdw TO " + ord_user)
            assert_raises(c_ord, f"CREATE SERVER s_fail2 FOREIGN DATA WRAPPER og_lake_file_fdw OPTIONS (external_root '{ext_root}')", expected_sqlstate='42501')
            assert_raises(c_db, "CREATE SERVER s_badroot1 FOREIGN DATA WRAPPER og_lake_file_fdw OPTIONS (external_root '/')")
            assert_raises(c_db, "CREATE SERVER s_badroot2 FOREIGN DATA WRAPPER og_lake_file_fdw OPTIONS (external_root 'relative/dir')")
            assert_raises(c_db, f"CREATE SERVER s_badroot3 FOREIGN DATA WRAPPER og_lake_file_fdw OPTIONS (external_root '{sym_sub}')")
            assert_raises(c_db, "CREATE SERVER s_badroot4 FOREIGN DATA WRAPPER og_lake_file_fdw OPTIONS (external_root 's3://bucket/prefix')")
            assert_raises(c_db, f"CREATE SERVER s_badopt FOREIGN DATA WRAPPER og_lake_file_fdw OPTIONS (external_root '{ext_root}', extra '1')")
            assert_raises(c_db, "ALTER FOREIGN DATA WRAPPER og_lake_file_fdw OPTIONS (ADD foo 'bar')")

            exec_sql(
                c_db,
                sql.SQL("CREATE SERVER ext_srv FOREIGN DATA WRAPPER og_lake_file_fdw OPTIONS (external_root {})").format(
                    sql.Literal(str(ext_root))
                ),
            )
            assert_raises(c_db, "CREATE SERVER ext_empty_srv FOREIGN DATA WRAPPER og_lake_file_fdw", expected_sqlstate='HV00J')

            # Bad table options
            assert_raises(c_db, "CREATE FOREIGN TABLE ft_bad1 (id int) SERVER ext_srv OPTIONS (path '../outside/secret.csv', format 'csv')")
            assert_raises(c_db, "CREATE FOREIGN TABLE ft_bad2 (id int) SERVER ext_srv OPTIONS (path '/etc/passwd', format 'csv')")
            assert_raises(c_db, "CREATE FOREIGN TABLE ft_bad3 (id int) SERVER ext_srv OPTIONS (path 's3://b/a.parquet', format 'parquet')")
            assert_raises(c_db, "CREATE FOREIGN TABLE ft_bad4 (id int) SERVER ext_srv OPTIONS (path 'events.csv', format 'parquet')")
            assert_raises(c_db, "CREATE FOREIGN TABLE ft_bad5 (id int) SERVER ext_srv OPTIONS (path 'rich_types.parquet', format 'csv')")
            assert_raises(c_db, "CREATE FOREIGN TABLE ft_bad6 (id int) SERVER ext_srv OPTIONS (path 'rich_types.parquet', header 'true')")
            exec_sql(c_db, "CREATE FOREIGN TABLE ft_bad7 (id int) SERVER ext_srv OPTIONS (path 'missing.parquet')")
            assert_raises(c_db, "SELECT * FROM ft_bad7")

            # Symlink escape rejections at execution / schema-inference time
            exec_sql(c_db, "CREATE FOREIGN TABLE ft_sym_csv (secret_id int) SERVER ext_srv OPTIONS (path 'link_escape.csv', format 'csv', header 'true')")
            assert_raises(c_db, "SELECT * FROM ft_sym_csv")
            assert_raises(c_db, "SELECT * FROM og_lake.describe_external_file('ext_srv', 'link_escape.csv', 'csv')")
            assert_raises(c_db, "SELECT * FROM og_lake.describe_external_file('ext_srv', 'sym_dir/secret.csv', 'csv')")
            exec_sql(c_db, "CREATE FOREIGN TABLE ft_sym_pq (id int) SERVER ext_srv OPTIONS (path 'link_escape.parquet', format 'parquet')")
            assert_raises(c_db, "SELECT * FROM ft_sym_pq")
            assert_raises(c_db, "SELECT * FROM og_lake.describe_external_file('s3://bucket/a.parquet')")
            assert_raises(c_db, f"SELECT * FROM og_lake.describe_external_path({repr(str(sym_sub / 'secret.csv'))}, 'csv')")
            assert_raises(c_db, f"SELECT * FROM og_lake.describe_external_file({repr(str(sym_sub / 'secret.csv'))})")
            assert_raises(c_ord, "SELECT * FROM og_lake.describe_external_file('ext_srv', 'rich_types.parquet')", expected_sqlstate='42501')
            assert_raises(c_ord, f"SELECT * FROM og_lake.describe_external_file({repr(str(pq_rich))})", expected_sqlstate='42501')
            rec('security_openat2_symlink_acl_matrix')

            # Stage 3: Schema inference & metadata functions vs DuckDB
            desc_pq = fetchall(c_db, "SELECT column_name, column_type, nullable, physical_ordinal FROM og_lake.describe_external_file('ext_srv', 'rich_types.parquet') ORDER BY physical_ordinal")
            assert desc_pq == [
                ('id', 'integer', True, 1),
                ('s16', 'smallint', True, 2),
                ('b64', 'bigint', True, 3),
                ('flag', 'boolean', True, 4),
                ('f32', 'real', True, 5),
                ('f64', 'double precision', True, 6),
                ('txt', 'text', True, 7),
                ('payload', 'bytea', True, 8),
                ('uid', 'uuid', True, 9),
                ('js', 'json', True, 10),
                ('amt', 'numeric(12,3)', True, 11),
                ('dt', 'date', True, 12),
                ('ts', 'timestamp without time zone', True, 13),
                ('tstz', 'timestamp with time zone', True, 14),
            ], f'unexpected desc_pq: {desc_pq!r}'
            desc_path = fetchall(c_db, "SELECT column_name, column_type FROM og_lake.describe_external_file(%s) ORDER BY physical_ordinal", (f'file://{pq_rich}',))
            assert desc_path == [(r[0], r[1]) for r in desc_pq]

            desc_vec = fetchall(c_db, "SELECT column_name, column_type FROM og_lake.describe_external_file('ext_srv', 'vectors.parquet') ORDER BY physical_ordinal")
            assert desc_vec == [('id', 'integer'), ('emb', 'vector')]

            info_pq = fetchall(c_db, "SELECT format, file_bytes, row_count, row_group_count, column_count FROM og_lake.external_file_info('ext_srv', 'rich_types.parquet')")
            assert len(info_pq) == 1 and info_pq[0] == ('parquet', pq_rich.stat().st_size, 12, 1, 14), f'unexpected info_pq: {info_pq!r}'
            info_multi = fetchall(c_db, "SELECT format, file_bytes, row_count, row_group_count, column_count FROM og_lake.external_file_info('ext_srv', 'nested/multi_5000.parquet')")
            assert len(info_multi) == 1 and info_multi[0] == ('parquet', pq_multi.stat().st_size, 5000, 3, 2), f'unexpected info_multi: {info_multi!r}'

            desc_csv = fetchall(c_db, "SELECT column_name, column_type, nullable, physical_ordinal FROM og_lake.describe_external_file('ext_srv', 'events.csv') ORDER BY physical_ordinal")
            assert desc_csv == [
                ('id', 'bigint', True, 1),
                ('score', 'double precision', True, 2),
                ('ok', 'boolean', True, 3),
                ('day_col', 'date', True, 4),
                ('note', 'text', True, 5),
            ], f'unexpected desc_csv: {desc_csv!r}'
            desc_nohdr = fetchall(c_db, "SELECT column_name, column_type FROM og_lake.describe_external_file('ext_srv', 'nested/raw_nohdr.csv', 'csv', false, ',') ORDER BY physical_ordinal")
            assert desc_nohdr == [('column0', 'bigint'), ('column1', 'text')]
            desc_nohdr_path = fetchall(c_db, "SELECT column_name, column_type FROM og_lake.describe_external_path(%s, 'csv', false, ',') ORDER BY physical_ordinal", (str(csv_nohdr),))
            assert desc_nohdr_path == desc_nohdr
            assert_raises(c_db, "SELECT * FROM og_lake.describe_external_file('ext_srv', 'empty.csv', 'csv')")
            duckdb_checks += 4
            rec('schema_inference_and_metadata')

            # Stage 4: External Parquet Full Types + Projection / Reordering / Widening / COUNT(*) + DuckDB Parity
            exec_sql(c_db, """
CREATE FOREIGN TABLE ext_rich_all (
    id integer,
    s16 smallint,
    b64 bigint,
    flag boolean,
    f32 real,
    f64 double precision,
    txt text,
    payload bytea,
    uid uuid,
    js json,
    amt numeric(12,3),
    dt date,
    ts timestamp without time zone,
    tstz timestamp with time zone
) SERVER ext_srv OPTIONS (path 'rich_types.parquet');

CREATE FOREIGN TABLE ext_rich_sub (
    amt numeric,
    txt varchar,
    wide_id bigint,
    s16_i4 integer,
    f32_f8 double precision,
    flag boolean
) SERVER ext_srv OPTIONS (path 'rich_types.parquet');
""")
            # Exact type incompatibility detection
            exec_sql(c_db, "CREATE FOREIGN TABLE ext_bad_sub (id integer, txt integer) SERVER ext_srv OPTIONS (path 'rich_types.parquet')")
            assert_raises(c_db, "SELECT * FROM ext_bad_sub")
            exec_sql(c_db, "CREATE FOREIGN TABLE ext_bad_col (missing_col text) SERVER ext_srv OPTIONS (path 'rich_types.parquet')")
            assert_raises(c_db, "SELECT * FROM ext_bad_col")

            # Rename alias test on widening vs exact name mapping
            exec_sql(c_db, """
CREATE FOREIGN TABLE ext_rich_widen (
    amt numeric,
    txt varchar,
    id bigint,
    s16 integer,
    f32 double precision,
    flag boolean
) SERVER ext_srv OPTIONS (path 'rich_types.parquet');
""")
            og_all = fetchall(c_db, """
SELECT id, s16, b64, flag, round(f32::numeric, 3)::text, round(f64::numeric, 3)::text,
       txt, encode(payload, 'hex'), uid::text, js::text, amt::text, dt::text,
       to_char(ts, 'YYYY-MM-DD HH24:MI:SS'), to_char(tstz AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS')
FROM ext_rich_all ORDER BY id
""")
            dk_all = run_duckdb_json(f"""
SELECT id, s16, b64, flag,
       printf('%.3f', f32) AS f32s, printf('%.3f', f64) AS f64s,
       txt, CASE WHEN payload IS NULL THEN NULL ELSE lower(hex(payload)) END AS payhex,
       uid::VARCHAR AS uids, js::VARCHAR AS jss, printf('%.3f', amt) AS amts, dt::VARCHAR AS dts,
       strftime(ts, '%Y-%m-%d %H:%M:%S') AS tss,
       strftime(tstz AT TIME ZONE 'UTC', '%Y-%m-%d %H:%M:%S') AS tstzs
FROM read_parquet('{pq_rich}') ORDER BY id
""")
            assert len(og_all) == len(dk_all) == 12
            for og_r, dk_r in zip(og_all, dk_all):
                assert og_r[0] == dk_r['id']
                assert og_r[1] == dk_r['s16']
                assert og_r[2] == dk_r['b64']
                assert og_r[3] == dk_r['flag']
                assert og_r[4] == dk_r['f32s']
                assert og_r[5] == dk_r['f64s']
                assert og_r[6] == dk_r['txt']
                assert og_r[7] == dk_r['payhex']
                assert og_r[8] == dk_r['uids']
                if dk_r['jss'] is None:
                    assert og_r[9] is None
                else:
                    assert json.loads(og_r[9]) == json.loads(dk_r['jss'])
                assert og_r[10] == dk_r['amts']
                assert og_r[11] == dk_r['dts']
                assert og_r[12] == dk_r['tss']
                assert og_r[13] == dk_r['tstzs']
            duckdb_checks += 12

            # Zero-column COUNT(*) and subset projection + widening
            assert scalar(c_db, "SELECT count(*) FROM ext_rich_all") == 12
            assert scalar(c_db, "SELECT count(*) FROM ext_rich_widen WHERE id >= 10 AND flag") == 2
            widen_rows = fetchall(c_db, "SELECT id, s16, round(f32::numeric, 2)::text, txt, amt::text FROM ext_rich_widen ORDER BY id")
            assert len(widen_rows) == 12 and widen_rows[0] == (1, -9, '0.25', '湖/1', '124.450'), f'unexpected widen_rows[0]: {widen_rows[0]!r}'

            # Multi-row-group 5000-row external Parquet + ANALYZE + NestedLoop ReScan + Read-Only checks
            exec_sql(c_db, "CREATE FOREIGN TABLE ext_multi (text_col text, id int) SERVER ext_srv OPTIONS (path 'nested/multi_5000.parquet')")
            multi_agg = fetchall(c_db, "SELECT count(*), count(text_col), count(*) FILTER (WHERE text_col = ''), sum(id) FROM ext_multi")
            dk_multi = run_duckdb_json(f"SELECT count(*) AS n, count(text_col) AS nt, count(*) FILTER (WHERE text_col = '') AS ne, sum(id)::BIGINT AS s FROM read_parquet('{pq_multi}')")[0]
            assert multi_agg[0] == (dk_multi['n'], dk_multi['nt'], dk_multi['ne'], dk_multi['s'])
            duckdb_checks += 4

            # Force NestedLoop rescan across ext_rich_widen
            exec_sql(c_db, "SET enable_hashjoin = off; SET enable_mergejoin = off;")
            rescan_cnt = scalar(c_db, "SELECT count(*) FROM (VALUES (1),(5),(10)) v(k) JOIN ext_rich_widen e ON e.id = v.k")
            assert rescan_cnt == 3
            exec_sql(c_db, "RESET enable_hashjoin; RESET enable_mergejoin;")

            # Read-only enforcement on external tables
            assert_raises(c_db, "INSERT INTO ext_rich_widen(id) VALUES (99)")
            assert_raises(c_db, "UPDATE ext_rich_widen SET id = 0")
            assert_raises(c_db, "DELETE FROM ext_rich_widen")
            assert_raises(c_db, "COPY ext_rich_widen FROM STDIN")
            assert_raises(c_db, "TRUNCATE ext_rich_widen")
            assert_raises(c_db, "SELECT * FROM ext_rich_widen FOR UPDATE")

            # Vectors + ANALYZE + EXPLAIN
            exec_sql(c_db, "CREATE FOREIGN TABLE ext_vec (id int, emb vector(3)) SERVER ext_srv OPTIONS (path 'vectors.parquet')")
            vec_rows = fetchall(c_db, "SELECT id, emb::text FROM ext_vec ORDER BY id")
            assert vec_rows[0] == (1, '[1,1.5,-1]') and len(vec_rows) == 5
            exec_sql(c_db, "ANALYZE ext_rich_all")
            exec_sql(c_db, "ANALYZE ext_multi")
            assert scalar(c_db, "SELECT reltuples::bigint FROM pg_class WHERE relname = 'ext_multi'") == 5000
            plan_txt = "\n".join(r[0] for r in fetchall(c_db, "EXPLAIN (VERBOSE, COSTS) SELECT txt FROM ext_rich_all WHERE id > 5"))
            assert 'Foreign Format: parquet' in plan_txt and 'External Parquet Row Groups: 1' in plan_txt, plan_txt
            plan_multi = "\n".join(r[0] for r in fetchall(c_db, "EXPLAIN (VERBOSE, COSTS) SELECT id FROM ext_multi WHERE id > 10"))
            assert 'Foreign Format: parquet' in plan_multi and 'External Parquet Row Groups: 3' in plan_multi, plan_multi
            rec('external_parquet_types_projection_rescan_analyze')

            # Stage 5: CSV & TSV external foreign tables + DuckLake table interop + SQL0.60 sanity
            exec_sql(c_db, """
CREATE FOREIGN TABLE ext_events_csv (
    id int,
    score double precision,
    ok boolean,
    day_col date,
    note text
) SERVER ext_srv OPTIONS (path 'events.csv', format 'csv', header 'true', delimiter ',');

CREATE FOREIGN TABLE ext_nohdr_csv (
    a int,
    b text OPTIONS (force_not_null 'true')
) SERVER ext_srv OPTIONS (path 'nested/raw_nohdr.csv', format 'csv', header 'false');

CREATE FOREIGN TABLE ext_tsv (
    k int,
    v text
) SERVER ext_srv OPTIONS (path 'tabbed.tsv', format 'csv', header 'true', delimiter E'\t');
""")
            og_csv = fetchall(c_db, "SELECT id, score, ok, day_col::text, note FROM ext_events_csv ORDER BY id")
            dk_csv = run_duckdb_json(f"SELECT id, score, ok, day_col::VARCHAR AS d, note FROM read_csv('{csv_hdr}', header=true, force_not_null=['note']) ORDER BY id")
            assert len(og_csv) == len(dk_csv) == 4
            for o, d in zip(og_csv, dk_csv):
                assert (o[0], o[1], o[2], o[3], o[4]) == (d['id'], float(d['score']) if d['score'] is not None else None, d['ok'], d['d'], d['note']), f'csv mismatch: og={o!r} dk={d!r}'
            nohdr_rows = fetchall(c_db, "SELECT a, b FROM ext_nohdr_csv ORDER BY a")
            assert nohdr_rows == [(10, 'r10'), (20, ''), (30, 'r30')]
            assert fetchall(c_db, "SELECT k, v FROM ext_tsv ORDER BY k") == [(101, 't101'), (102, 't102')]
            exec_sql(c_db, "ANALYZE ext_events_csv;")
            assert scalar(c_db, "SELECT reltuples::bigint FROM pg_class WHERE relname = 'ext_events_csv'") == 4
            duckdb_checks += 4

            # Ordinary user with USAGE ON FOREIGN SERVER ext_srv can read & describe, but cannot insert or access without USAGE
            exec_sql(c_db, "GRANT USAGE ON FOREIGN SERVER ext_srv TO " + ord_user)
            exec_sql(c_db, f"CREATE SCHEMA ord_sc AUTHORIZATION {ord_user}")
            exec_sql(c_ord, "CREATE FOREIGN TABLE ord_sc.ord_ext_t (id int, txt text) SERVER ext_srv OPTIONS (path 'rich_types.parquet')")
            assert scalar(c_ord, "SELECT count(*) FROM ord_sc.ord_ext_t") == 12
            assert scalar(c_ord, "SELECT count(*) FROM og_lake.describe_external_file('ext_srv', 'events.csv')") == 5
            assert_raises(c_ord, "INSERT INTO ord_sc.ord_ext_t VALUES (1, 'x')")

            # DuckLake managed catalog integration + COPY (SELECT ... FROM ext) TO + SQL0.60 merge_adjacent_files sanity
            exec_sql(c_db, f"SELECT og_lake.configure_internal_owner('{meta_owner}')")
            exec_sql(c_db, f"SELECT og_lake.create_catalog('lake_srv', 'lake_meta', '{lake_data}', NULL)")
            exec_sql(c_db, "CREATE FOREIGN TABLE lake_orders (id int, amt numeric(12,3), txt text) SERVER lake_srv OPTIONS (binding_mode 'create', lake_schema 'main')")
            exec_sql(c_db, "INSERT INTO lake_orders SELECT id, amt, COALESCE(txt, '') FROM ext_rich_all WHERE id <= 6")
            exec_sql(c_db, "INSERT INTO lake_orders SELECT id, NULL, COALESCE(note, '') FROM ext_events_csv")
            joined = fetchall(c_db, "SELECT o.id, o.txt, e.note FROM lake_orders o JOIN ext_events_csv e ON o.id = e.id ORDER BY o.id, o.txt")
            assert len(joined) == 8
            exec_sql(c_db, "SELECT og_lake.flush('lake_orders'::regclass)")
            exec_sql(c_db, "INSERT INTO lake_orders VALUES (99, 1.000, 'second')")
            exec_sql(c_db, "SELECT og_lake.flush('lake_orders'::regclass)")
            merged = fetchall(c_db, "SELECT files_processed, files_created FROM og_lake.merge_adjacent_files('lake_orders'::regclass)")
            assert merged == [(2, 1)]
            out_export = workdir / 'copy_export.csv'
            exec_sql(c_db, sql.SQL("COPY (SELECT id, txt FROM ext_rich_all WHERE id <= 3 ORDER BY id) TO {} WITH (FORMAT csv, HEADER true)").format(sql.Literal(str(out_export))))
            assert out_export.read_text(encoding='utf-8').splitlines()[0] == 'id,txt'

            # SMP fallback clean when query_dop = 2
            exec_sql(c_db, "SET query_dop = 2")
            assert scalar(c_db, "SELECT count(*) FROM ext_multi") == 5000
            exec_sql(c_db, "SET query_dop = 1")
            duckdb_checks += 3
            rec('csv_tsv_acl_ducklake_interop_and_060_compat')

            # Stage 6: Cursor fd pinning across atomic file replacement + rollback cleanup
            mut_file = ext_root / 'mut.parquet'
            mut_tmp = ext_root / 'mut.parquet.tmp'
            shutil.copy2(pq_multi, mut_file)
            exec_sql(c_db, "CREATE FOREIGN TABLE ext_mut (id int, text_col text) SERVER ext_srv OPTIONS (path 'mut.parquet')")
            c_cur = connect(db_name, autocommit=False)
            try:
                cur = c_cur.cursor()
                cur.execute("DECLARE c_mut CURSOR FOR SELECT id FROM ext_mut ORDER BY id")
                cur.execute("FETCH 100 FROM c_mut")
                first_100 = cur.fetchall()
                assert len(first_100) == 100 and first_100[0][0] == 0
                shutil.copy2(pq_rich, mut_tmp)
                os.replace(mut_tmp, mut_file)
                cur.execute("FETCH ALL FROM c_mut")
                rest_rows = cur.fetchall()
                assert len(rest_rows) == 4900 and rest_rows[-1][0] == 4999
                cur.execute("CLOSE c_mut")
                cur.execute("ROLLBACK")
            finally:
                c_cur.close()
            assert scalar(c_db, "SELECT count(*) FROM ext_mut") == 12

            # Direct path inspect without enable_copy_server_files / safe_data_path is rejected for non-initialuser,
            # while server-scoped inspect works after GRANT USAGE ON FOREIGN SERVER.
            c_copy = connect(db_name, user=copy_user, password=pwd)
            try:
                assert_raises(c_copy, "SELECT count(*) FROM og_lake.describe_external_file(%s)", (str(pq_rich),), expected_sqlstate='42501')
                exec_sql(c_db, "GRANT USAGE ON FOREIGN SERVER ext_srv TO " + copy_user)
                assert scalar(c_copy, "SELECT count(*) FROM og_lake.describe_external_file('ext_srv', 'rich_types.parquet')") == 14
            finally:
                c_copy.close()
            rec('cursor_fd_pinning_atomic_replace_and_copy_role')
        finally:
            c_ord.close()
            c_db.close()

    finally:
        for stmt in [
            sql.SQL('DROP DATABASE IF EXISTS {}').format(sql.Identifier(db_name)),
            sql.SQL('DROP DATABASE IF EXISTS {}').format(sql.Identifier(db_old)),
            sql.SQL('DROP ROLE IF EXISTS {}').format(sql.Identifier(ord_user)),
            sql.SQL('DROP ROLE IF EXISTS {}').format(sql.Identifier(copy_user)),
            sql.SQL('DROP ROLE IF EXISTS {}').format(sql.Identifier(meta_owner)),
        ]:
            try:
                exec_sql(conn_adm, stmt)
            except Exception:
                pass
        conn_adm.close()
        shutil.rmtree(workdir, ignore_errors=True)

    c_chk = connect('postgres')
    try:
        tp = scalar(c_chk, 'SHOW enable_thread_pool')
        pid = scalar(c_chk, 'SELECT pg_backend_pid()')
        exe = os.path.realpath(f'/proc/{pid}/exe')
    finally:
        c_chk.close()

    report = {
        'status': 'passed',
        'sql_version': '0.61',
        'profile': PROFILE,
        'port': PORT,
        'enable_thread_pool': tp,
        'backend_exe': exe,
        'stage_count': len(stages),
        'duckdb_checks': duckdb_checks,
        'stages': stages,
        'finished_at': datetime.now(timezone.utc).isoformat(),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + '\n', encoding='utf-8')
    print(json.dumps({'status': 'passed', 'profile': PROFILE, 'enable_thread_pool': tp, 'stages': len(stages), 'duckdb_checks': duckdb_checks}), flush=True)


if __name__ == '__main__':
    main()
