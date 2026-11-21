#!/usr/bin/env python3
"""SQL0.62 (N7-COPY) comprehensive verification suite + DuckDB parity."""

import argparse
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


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        while True:
            chunk = f.read(65536)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def duckdb_json(sql_text):
    proc = subprocess.run(
        [str(DUCKDB_BIN), '-batch', '-light-mode', '-json', '-c', sql_text],
        capture_output=True, text=True, timeout=60,
    )
    if proc.returncode != 0:
        raise RuntimeError(f'DuckDB failed ({proc.returncode}): {proc.stderr}\nSQL:\n{sql_text}')
    out = proc.stdout.strip()
    return json.loads(out) if out else []


def duckdb_exec(sql_text):
    proc = subprocess.run(
        [str(DUCKDB_BIN), '-batch', '-light-mode', '-c', sql_text],
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
        'application_name': 'sql062_verify',
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


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--profile', choices=['debug', 'ann-debug', 'release'], required=True)
    ap.add_argument('--out', required=True)
    args = ap.parse_args()

    tag = uuid.uuid4().hex[:10]
    tmp_root = Path(tempfile.mkdtemp(prefix=f'og_062_{tag}_', dir='/tmp'))
    os.chmod(tmp_root, 0o755)
    role_unpriv = f'u062_unpriv_{tag}'
    role_own_upg = f'u062_ownu_{tag}'
    role_own_main = f'u062_ownm_{tag}'
    db_noext = f'db062_noext_{tag}'
    db_upg = f'db062_upg_{tag}'
    db_main = f'db062_main_{tag}'
    admin = None
    unpriv_pw = 'LakeCopy_062!'

    created_dbs = []
    created_roles = []
    stage_stats = {}
    duckdb_checks = 0

    try:
        admin = pg_connect(args.profile, 'postgres')
        # Ensure clean role creation
        q(admin, f"CREATE ROLE {role_unpriv} LOGIN ENCRYPTED PASSWORD %s",
          (md5_role_pw(unpriv_pw, role_unpriv),))
        created_roles.append(role_unpriv)
        for own_r in (role_own_upg, role_own_main):
            q(admin, f"CREATE ROLE {own_r} NOLOGIN NOINHERIT ENCRYPTED PASSWORD %s",
              (md5_role_pw('Own_062!', own_r),))
            created_roles.append(own_r)

        for dbn in (db_noext, db_upg, db_main):
            q(admin, f"CREATE DATABASE {dbn} WITH DBCOMPATIBILITY 'PG' ENCODING 'UTF8'")
            created_dbs.append(dbn)

        # Prepare fixture files using DuckDB in tmp_root
        in_dir = tmp_root / 'in'
        out_dir = tmp_root / 'out'
        in_dir.mkdir(parents=True)
        out_dir.mkdir(parents=True)

        src_types_pq = in_dir / 'src_types.parquet'
        src_bulk_pq = in_dir / 'src_bulk_5200.parquet'
        src_part_pq = in_dir / 'src_part.parquet'
        src_wide_pq = in_dir / 'src_widen.parquet'
        src_ext_pq = in_dir / 'src_ext.parquet'
        src_csv = in_dir / 'src_rows.csv'
        src_tsv = in_dir / 'src_rows.tsv'

        duckdb_exec(f"""
        COPY (
            SELECT
                1::INTEGER AS id,
                true::BOOLEAN AS b,
                (-12345)::SMALLINT AS i2,
                1000001::INTEGER AS i4,
                9876543210123::BIGINT AS i8,
                1.5::FLOAT AS f4,
                (-2.25)::DOUBLE AS f8,
                (-123456.7890)::DECIMAL(18,4) AS dec_b,
                99.5::DOUBLE AS num_u,
                'hello 湖仓'::VARCHAR AS txt,
                'abcd'::VARCHAR AS ch4,
                DATE '2026-11-19' AS d,
                TIMESTAMP '2026-11-19 08:09:10.123456' AS ts,
                TIMESTAMPTZ '2026-11-19 08:09:10.123456+00:00' AS tstz,
                UUID '01234567-89ab-cdef-0123-456789abcdef' AS u,
                '{{"k":[1,2]}}'::JSON AS j,
                from_hex('00DEADBEEF') AS bin,
                [1.5::FLOAT, -2.5::FLOAT, 3.0::FLOAT]::FLOAT[] AS vec
            UNION ALL SELECT
                2, false, NULL::SMALLINT, -42, 0::BIGINT, NULL::FLOAT, 3.5::DOUBLE, (-0.0001)::DECIMAL(18,4), 0.125::DOUBLE,
                '', 'xy  ', DATE '1970-01-01', TIMESTAMP '1970-01-01 00:00:00',
                TIMESTAMPTZ '2025-01-02 03:04:05+00:00',
                UUID 'ffffffff-ffff-ffff-ffff-ffffffffffff',
                '[10,true]'::JSON, ''::BLOB, [0.0::FLOAT, 1.0::FLOAT, 2.0::FLOAT]::FLOAT[]
            UNION ALL SELECT
                3, NULL::BOOLEAN, 32767::SMALLINT, NULL::INTEGER, NULL::BIGINT, 0.0::FLOAT, NULL::DOUBLE,
                NULL::DECIMAL(18,4), NULL::DOUBLE,
                NULL, NULL, NULL, NULL, NULL, NULL, NULL::JSON, NULL, NULL
        ) TO '{src_types_pq}' (FORMAT PARQUET, COMPRESSION ZSTD);

        COPY (
            SELECT
                i::INTEGER AS id,
                (i % 7)::INTEGER AS p,
                'row_' || i::VARCHAR || '_湖' AS payload,
                (i * 1.25)::DECIMAL(18,2) AS amt
            FROM range(1, 5201) t(i)
        ) TO '{src_bulk_pq}' (FORMAT PARQUET, COMPRESSION SNAPPY, ROW_GROUP_SIZE 1500);

        COPY (
            SELECT
                i::INTEGER AS id,
                CASE WHEN i % 10 = 0 THEN NULL ELSE (i % 5)::INTEGER END AS p,
                'pv_' || i::VARCHAR AS val
            FROM range(1, 301) t(i)
        ) TO '{src_part_pq}' (FORMAT PARQUET, COMPRESSION ZSTD);

        COPY (
            SELECT 10::INTEGER AS id_wide, 3.5::FLOAT AS score_wide, 'abc'::VARCHAR AS label_wide
            UNION ALL SELECT -9, -1.25::FLOAT, '湖'
        ) TO '{src_wide_pq}' (FORMAT PARQUET);

        COPY (
            SELECT
                1::INTEGER AS id,
                'alpha'::VARCHAR AS tag,
                12.5::DOUBLE AS score,
                [1.0::FLOAT, 2.0::FLOAT, 3.0::FLOAT]::FLOAT[] AS vec
            UNION ALL SELECT 2, 'beta', (-4.25)::DOUBLE, [4.0::FLOAT, 5.0::FLOAT, 6.0::FLOAT]::FLOAT[]
        ) TO '{src_ext_pq}' (FORMAT PARQUET);
        """)
        duckdb_checks += 5

        src_csv.write_text('id,label,val\n101,"csv,a",10\n102,"",20\n103,gamma,\n', encoding='utf-8')
        src_tsv.write_text('201\ttsv_a\t30\n202\t\\N\t40\n', encoding='utf-8')

        # ------------------------------------------------------------------
        # Stage 1: Version gating (no extension, 0.61 -> 0.62)
        # ------------------------------------------------------------------
        c_noext = pg_connect(args.profile, db_noext)
        try:
            q(c_noext, 'CREATE TABLE h_noext (id int, v text)')
            q(c_noext, "INSERT INTO h_noext VALUES (1, 'a')")
            noext_out = out_dir / 'noext_fail.parquet'
            expect_error(c_noext, f"COPY h_noext TO '{noext_out}' WITH (FORMAT parquet)",
                         expected_states={'0A000'}, expected_substr='0.62')
            assert not noext_out.exists()
        finally:
            c_noext.close()

        c_upg = pg_connect(args.profile, db_upg)
        try:
            q(c_upg, "CREATE EXTENSION og_lake VERSION '0.61'")
            hs = json.loads(s(c_upg, 'SELECT og_lake.host_status()'))
            assert hs.get('module_version') in ('0.62', '0.73', '0.74'), hs
            assert hs.get('copy_io') is True, hs
            assert hs.get('copy_io_sql_version') == '0.62', hs
            assert hs.get('copy_formats') == (['parquet', 'csv', 'text', 'json'] if hs.get('module_version') in ('0.73', '0.74') else ['parquet', 'csv', 'text']), hs

            q(c_upg, 'CREATE TABLE h_upg (id int, p int, val text)')
            q(c_upg, "INSERT INTO h_upg VALUES (1, 1, 'x')")
            cat_upg_dir = tmp_root / 'cat_upg'
            q(c_upg, 'SELECT og_lake.configure_internal_owner(%s::name)', (role_own_upg,))
            q(c_upg, "SELECT og_lake.create_catalog('cat_upg'::name, 'cat_upg_meta'::name, %s::text, NULL::text)",
              (str(cat_upg_dir),))
            q(c_upg, "CREATE FOREIGN TABLE l_upg (id int, label text, val int) SERVER cat_upg OPTIONS (binding_mode 'create', lake_schema 'main')")

            expect_error(c_upg, f"COPY h_upg FROM '{src_wide_pq}' WITH (FORMAT parquet)",
                         expected_states={'0A000'}, expected_substr='0.62')
            expect_error(c_upg, f"COPY h_upg TO '{out_dir}/upg_fail.parquet' WITH (FORMAT parquet)",
                         expected_states={'0A000'}, expected_substr='0.62')
            expect_error(c_upg, f"COPY l_upg FROM '{src_csv}' WITH (FORMAT csv, HEADER true)",
                         expected_states={'0A000'})
            expect_error(c_upg, f"COPY l_upg TO '{out_dir}/upg_fail.csv' WITH (FORMAT csv)",
                         expected_states={'42809', '0A000'})
            assert not (out_dir / 'upg_fail.parquet').exists()
            assert not (out_dir / 'upg_fail.csv').exists()

            q(c_upg, "ALTER EXTENSION og_lake UPDATE TO '0.62'")
            assert s(c_upg, "SELECT extversion FROM pg_extension WHERE extname='og_lake'") == '0.62'

            # Verify post-upgrade works!
            q(c_upg, f"COPY l_upg FROM '{src_csv}' WITH (FORMAT csv, HEADER true)")
            assert s(c_upg, 'SELECT count(*) FROM l_upg') == 3
            upg_pq = out_dir / 'upg_ok.parquet'
            q(c_upg, f"COPY l_upg TO '{upg_pq}' WITH (FORMAT parquet)")
            assert upg_pq.is_file()
            drows = duckdb_json(f"SELECT id, label, val FROM read_parquet('{upg_pq}') ORDER BY id")
            duckdb_checks += 1
            assert drows == [
                {'id': 101, 'label': 'csv,a', 'val': 10},
                {'id': 102, 'label': '', 'val': 20},
                {'id': 103, 'label': 'gamma', 'val': None},
            ], drows
        finally:
            c_upg.close()
        stage_stats['stage1_version_gating'] = 10

        # ------------------------------------------------------------------
        # Main DB for Stages 2..6
        # ------------------------------------------------------------------
        cm = pg_connect(args.profile, db_main)
        try:
            q(cm, 'CREATE EXTENSION og_lake')
            assert s(cm, "SELECT extversion FROM pg_extension WHERE extname='og_lake'") in ('0.62', '0.73', '0.74')

            # Stage 2: Security, Path Confinement, Privilege & Option validation
            q(cm, 'CREATE TABLE h_sec (id int, name text)')
            q(cm, "INSERT INTO h_sec VALUES (1, 'a')")
            existing_file = out_dir / 'already_exists.parquet'
            existing_file.write_bytes(b'PRESERVED_BYTES_062')
            orig_sha = sha256_file(existing_file)

            sym_dir = tmp_root / 'sym_dir'
            os.symlink(str(in_dir), str(sym_dir))
            sym_out_dir = tmp_root / 'sym_out_dir'
            os.symlink(str(out_dir), str(sym_out_dir))
            sym_file = in_dir / 'sym_file.parquet'
            os.symlink(str(src_wide_pq), str(sym_file))

            # Remote URLs
            expect_error(cm, "COPY h_sec FROM 's3://bucket/data.parquet' WITH (FORMAT parquet)",
                         expected_states={'0A000'})
            expect_error(cm, "COPY h_sec TO 'https://example.com/out.parquet' WITH (FORMAT parquet)",
                         expected_states={'0A000'})
            # Relative file:// URI
            expect_error(cm, "COPY h_sec TO 'file://rel/out.parquet' WITH (FORMAT parquet)",
                         expected_states={'22023'})
            # Symlink rejection (COPY FROM file & parent symlink, COPY TO parent symlink)
            expect_error(cm, f"COPY h_sec FROM '{sym_file}' WITH (FORMAT parquet)")
            expect_error(cm, f"COPY h_sec FROM '{sym_dir}/src_widen.parquet' WITH (FORMAT parquet)")
            expect_error(cm, f"COPY h_sec TO '{sym_out_dir}/sym_out.parquet' WITH (FORMAT parquet)")
            assert not (out_dir / 'sym_out.parquet').exists()
            # Existing file non-overwrite
            expect_error(cm, f"COPY h_sec TO '{existing_file}' WITH (FORMAT parquet)")
            assert sha256_file(existing_file) == orig_sha
            # Conflicting / invalid options & pipes
            expect_error(cm, f"COPY h_sec TO '{out_dir}/bad_opt1.parquet' WITH (FORMAT parquet, HEADER true)",
                         expected_states={'0A000'})
            expect_error(cm, f"COPY h_sec TO '{out_dir}/bad_opt2.parquet' WITH (FORMAT parquet, DELIMITER ',')",
                         expected_states={'0A000'})
            expect_error(cm, f"COPY h_sec TO '{out_dir}/bad_opt3.csv' WITH (FORMAT csv, COMPRESSION zstd)",
                         expected_states={'42601'})
            expect_error(cm, f"COPY h_sec FROM '{src_wide_pq}' WITH (FORMAT parquet, COMPRESSION zstd)",
                         expected_states={'42601'})
            expect_error(cm, f"COPY h_sec TO '{out_dir}/bad_opt4.parquet' WITH (FORMAT parquet, COMPRESSION brotli)",
                         expected_states={'0A000'})
            expect_error(cm, "COPY h_sec FROM STDIN WITH (FORMAT parquet)", expected_states={'0A000'})
            expect_error(cm, "COPY h_sec TO STDOUT WITH (FORMAT parquet)", expected_states={'0A000'})
            # Duplicate output column names
            expect_error(cm, f"COPY (SELECT 1 AS a, 2 AS a) TO '{out_dir}/dup_col.parquet' WITH (FORMAT parquet)",
                         expected_states={'42701'})
            assert not (out_dir / 'dup_col.parquet').exists()

            # Privilege check: ordinary unprivileged user cannot read/write server files
            q(cm, f'GRANT ALL ON h_sec TO {role_unpriv}')
            cu = pg_connect(args.profile, db_main, user=role_unpriv, password=unpriv_pw)
            try:
                expect_error(cu, f"COPY h_sec FROM '{src_wide_pq}' WITH (FORMAT parquet)",
                             expected_states={'42501'})
                expect_error(cu, f"COPY h_sec TO '{out_dir}/unpriv_fail.parquet' WITH (FORMAT parquet)",
                             expected_states={'42501'})
                assert not (out_dir / 'unpriv_fail.parquet').exists()
            finally:
                cu.close()

            # file:// normalization positive test
            uri_pq = out_dir / 'uri_ok.parquet'
            q(cm, f"COPY h_sec TO 'file://{uri_pq}' WITH (FORMAT parquet)")
            assert duckdb_json(f"SELECT id, name FROM read_parquet('{uri_pq}')") == [{'id': 1, 'name': 'a'}]
            duckdb_checks += 1
            stage_stats['stage2_security_and_options'] = 17

            # Stage 3: Heap table 15 DuckLake types + bounded/unbounded numeric + vector + defaults + trigger + index + bulk
            q(cm, """
                CREATE TABLE h_types (
                    id int,
                    b boolean,
                    i2 smallint,
                    i4 integer,
                    i8 bigint,
                    f4 real,
                    f8 double precision,
                    dec_b numeric(18,4),
                    num_u double precision,
                    txt text,
                    ch4 text,
                    d date,
                    ts timestamp,
                    tstz timestamptz,
                    u uuid,
                    j json,
                    bin bytea,
                    vec vector(3)
                )
            """)
            q(cm, f"COPY h_types FROM '{src_types_pq}' WITH (FORMAT parquet)")
            assert s(cm, 'SELECT count(*) FROM h_types') == 3
            assert s(cm, 'SELECT txt FROM h_types WHERE id = 2') == ''
            assert s(cm, 'SELECT txt FROM h_types WHERE id = 1') == 'hello 湖仓'
            assert s(cm, 'SELECT dec_b::text FROM h_types WHERE id = 1') == '-123456.7890'
            assert s(cm, 'SELECT octet_length(bin) FROM h_types WHERE id = 1') == 5

            # Export with zstd, snappy, uncompressed + subset/reordered + 0-row
            for codec, expected_codec in [('zstd', 'ZSTD'), ('snappy', 'SNAPPY'), ('uncompressed', 'UNCOMPRESSED')]:
                ep = out_dir / f'htypes_{codec}.parquet'
                q(cm, f"COPY h_types TO '{ep}' WITH (FORMAT parquet, COMPRESSION {codec})")
                meta = duckdb_json(f"SELECT DISTINCT compression FROM parquet_metadata('{ep}')")
                d_rows = duckdb_json(f"""
                    SELECT id, b, i2, i4, i8, dec_b::VARCHAR AS dec_b, txt,
                           d::VARCHAR AS d, u::VARCHAR AS u, vec::FLOAT[] AS vec
                    FROM read_parquet('{ep}') ORDER BY id
                """)
                duckdb_checks += 2
                assert meta[0]['compression'] == expected_codec, (codec, meta)
                assert len(d_rows) == 3
                assert d_rows[0]['dec_b'] == '-123456.7890'
                assert d_rows[1]['dec_b'] == '-0.0001'
                assert d_rows[0]['vec'] == [1.5, -2.5, 3.0]
                assert d_rows[1]['txt'] == ''

            # Narrowing check: INT4 -> SMALLINT must be rejected cleanly
            q(cm, 'CREATE TABLE h_narrow (id_wide smallint)')
            expect_error(cm, f"COPY h_narrow (id_wide) FROM '{src_wide_pq}' WITH (FORMAT parquet)",
                         expected_states={'42804'})

            # Unbounded NUMERIC, small positive decimal & VARCHAR/CHAR export cleanly to Parquet
            q(cm, 'CREATE TABLE h_unbounded_num (id int, v numeric, d4 numeric(10,4), vc varchar(8), ch char(4))')
            q(cm, "INSERT INTO h_unbounded_num VALUES (1, 99.5, 0.0001, 'vchar', 'xy'), (2, 0.125, -0.0001, '', 'abcd'), (3, NULL, NULL, NULL, NULL)")
            ub_pq = out_dir / 'unbounded_num.parquet'
            q(cm, f"COPY h_unbounded_num TO '{ub_pq}' WITH (FORMAT parquet)")
            assert duckdb_json(f"SELECT id, v, d4::VARCHAR AS d4, vc, ch FROM read_parquet('{ub_pq}') ORDER BY id") == [
                {'id': 1, 'v': 99.5, 'd4': '0.0001', 'vc': 'vchar', 'ch': 'xy  '},
                {'id': 2, 'v': 0.125, 'd4': '-0.0001', 'vc': '', 'ch': 'abcd'},
                {'id': 3, 'v': None, 'd4': None, 'vc': None, 'ch': None},
            ]
            duckdb_checks += 1

            # Zero-row Parquet export
            empty_pq = out_dir / 'empty_zero.parquet'
            q(cm, f"COPY (SELECT id, txt, dec_b FROM h_types WHERE id < 0) TO '{empty_pq}' WITH (FORMAT parquet)")
            assert duckdb_json(f"SELECT count(*) AS cnt FROM read_parquet('{empty_pq}')") == [{'cnt': 0}]
            assert duckdb_json(f"SELECT count(*) AS cols FROM parquet_schema('{empty_pq}') WHERE name IN ('id','txt','dec_b')") == [{'cols': 3}]
            duckdb_checks += 2

            # Subset columns + defaults + sequence + CHECK + trigger + B-tree index
            q(cm, 'CREATE SEQUENCE seq_062 START 100')
            q(cm, """
                CREATE TABLE h_def (
                    seq_id bigint DEFAULT nextval('seq_062'),
                    score_wide double precision CHECK (score_wide > -100),
                    id_wide bigint NOT NULL,
                    label_wide text,
                    note text DEFAULT 'auto_default',
                    trig_tag text
                )
            """)
            q(cm, 'CREATE INDEX idx_h_def_id ON h_def(id_wide)')
            q(cm, """
                CREATE OR REPLACE FUNCTION trg_h_def_fn() RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN
                    NEW.trig_tag := COALESCE(NEW.label_wide, 'nil') || '_tr';
                    RETURN NEW;
                END;
                $$;
                CREATE TRIGGER trg_h_def BEFORE INSERT ON h_def
                FOR EACH ROW EXECUTE PROCEDURE trg_h_def_fn();
            """)
            q(cm, f"COPY h_def (id_wide, label_wide, score_wide) FROM '{src_wide_pq}' WITH (FORMAT parquet)")
            rows_def = q(cm, 'SELECT seq_id, score_wide, id_wide, label_wide, note, trig_tag FROM h_def ORDER BY id_wide')
            assert rows_def == [
                (101, -1.25, -9, '湖', 'auto_default', '湖_tr'),
                (100, 3.5, 10, 'abc', 'auto_default', 'abc_tr'),
            ], rows_def
            # Force index lookup on copied heap rows
            q(cm, 'SET enable_seqscan = off')
            idx_hit = q(cm, 'SELECT label_wide, trig_tag FROM h_def WHERE id_wide = 10')
            q(cm, 'SET enable_seqscan = on')
            assert idx_hit == [('abc', 'abc_tr')]

            # 5,200 rows multi-row-group bulk Parquet import + export
            q(cm, 'CREATE TABLE h_bulk (id int, p int, payload text, amt numeric(18,2))')
            q(cm, f"COPY h_bulk FROM '{src_bulk_pq}' WITH (FORMAT parquet)")
            assert q(cm, 'SELECT count(*), min(id), max(id), sum(id) FROM h_bulk') == [(5200, 1, 5200, 5200 * 5201 // 2)]
            bulk_out = out_dir / 'h_bulk_out.parquet'
            q(cm, f"COPY h_bulk (payload, id, amt) TO '{bulk_out}' WITH (FORMAT parquet)")
            d_bulk = duckdb_json(f"SELECT count(*) AS cnt, min(id) AS mn, max(id) AS mx, sum(id)::BIGINT AS sm FROM read_parquet('{bulk_out}')")
            duckdb_checks += 1
            assert d_bulk == [{'cnt': 5200, 'mn': 1, 'mx': 5200, 'sm': 5200 * 5201 // 2}], d_bulk
            stage_stats['stage3_heap_parquet'] = 12

            # Stage 4: Native Lake Tables + Partitioned Lake Table + External File FDW -> COPY
            cat_main_dir = tmp_root / 'cat_main'
            q(cm, 'SELECT og_lake.configure_internal_owner(%s::name)', (role_own_main,))
            q(cm, "SELECT og_lake.create_catalog('cat_main'::name, 'cat_main_meta'::name, %s::text, NULL::text)",
              (str(cat_main_dir),))

            # 4a: Inline + Spill lake table (row limit 50 -> triggers BufFile/file flush on bulk, then small CSV & Parquet)
            q(cm, "CREATE FOREIGN TABLE biz_inline (id int, p int, payload text, amt numeric(18,2)) SERVER cat_main OPTIONS (binding_mode 'create', lake_schema 'main')")
            q(cm, "SELECT og_lake.set_option('biz_inline'::regclass, 'data_inlining_row_limit', '50')")
            q(cm, f"COPY biz_inline FROM '{src_bulk_pq}' WITH (FORMAT parquet)")
            assert s(cm, 'SELECT count(*) FROM biz_inline') == 5200
            q(cm, "CREATE FOREIGN TABLE biz_small (id int, label text, val int DEFAULT 77) SERVER cat_main OPTIONS (binding_mode 'create', lake_schema 'main')")
            snap_s0 = s(cm, "SELECT og_lake.current_snapshot('cat_main'::name)")
            q(cm, f"COPY biz_small (id, label, val) FROM 'file://{src_csv}' WITH (FORMAT csv, HEADER true)")
            q(cm, f"COPY biz_small (id, label, val) FROM '{src_tsv}' WITH (FORMAT text)")
            snap_s_ins = s(cm, "SELECT og_lake.current_snapshot('cat_main'::name)")
            assert q(cm, 'SELECT id, label, val FROM biz_small ORDER BY id') == [
                (101, 'csv,a', 10), (102, '', 20), (103, 'gamma', None),
                (201, 'tsv_a', 30), (202, None, 40),
            ]
            # CDC delete + time_travel / table_changes + export lake table
            q(cm, 'DELETE FROM biz_small WHERE id = 102')
            snap_s_del = s(cm, "SELECT og_lake.current_snapshot('cat_main'::name)")
            tt_rows = q(cm, "SELECT id, label, val FROM og_lake.time_travel('biz_small'::regclass, %s::bigint) AS t(id int, label text, val int) ORDER BY id", (snap_s_ins,))
            assert len(tt_rows) == 5 and any(r[0] == 102 for r in tt_rows), tt_rows
            chg_rows = q(cm, "SELECT change_type, id, label, val FROM og_lake.table_changes('biz_small'::regclass, %s::bigint, %s::bigint) AS c(snapshot_id bigint, rowid bigint, change_type text, id int, label text, val int) ORDER BY id, change_type", (snap_s0 + 1, snap_s_del))
            assert any(r[0] == 'delete' and r[1] == 102 for r in chg_rows) and any(r[0] == 'insert' and r[1] == 101 for r in chg_rows), chg_rows
            small_out = out_dir / 'biz_small_out.parquet'
            q(cm, f"COPY biz_small TO '{small_out}' WITH (FORMAT parquet)")
            assert duckdb_json(f"SELECT id, label, val FROM read_parquet('{small_out}') ORDER BY id") == [
                {'id': 101, 'label': 'csv,a', 'val': 10},
                {'id': 103, 'label': 'gamma', 'val': None},
                {'id': 201, 'label': 'tsv_a', 'val': 30},
                {'id': 202, 'label': None, 'val': 40},
            ]
            duckdb_checks += 1

            # 4b: Direct-file lake table (data_inlining_row_limit = 0)
            q(cm, "CREATE FOREIGN TABLE biz_direct (id int, p int, payload text, amt numeric(18,2)) SERVER cat_main OPTIONS (binding_mode 'create', lake_schema 'main')")
            q(cm, "SELECT og_lake.set_option('biz_direct'::regclass, 'data_inlining_row_limit', '0')")
            q(cm, f"COPY biz_direct FROM '{src_bulk_pq}' WITH (FORMAT parquet)")
            files_direct = q(cm, "SELECT data_file, data_file_size_bytes FROM og_lake.list_files('biz_direct'::regclass)")
            assert len(files_direct) >= 1
            total_direct_rows = 0
            for fp, fsz in files_direct:
                fp_abs = fp if fp.startswith('/') else str(cat_main_dir / fp)
                assert os.path.getsize(fp_abs) == fsz
                meta_cnt = duckdb_json(f"SELECT count(*) AS cnt FROM read_parquet('{fp_abs}')")[0]['cnt']
                duckdb_checks += 1
                total_direct_rows += meta_cnt
            assert total_direct_rows == 5200, (files_direct, total_direct_rows)
            direct_out = out_dir / 'biz_direct_out.parquet'
            q(cm, f"COPY (SELECT id, amt FROM biz_direct WHERE id BETWEEN 100 AND 105 ORDER BY id) TO '{direct_out}' WITH (FORMAT parquet)")
            assert duckdb_json(f"SELECT count(*) AS cnt, min(id) AS mn, max(id) AS mx FROM read_parquet('{direct_out}')") == [{'cnt': 6, 'mn': 100, 'mx': 105}]
            duckdb_checks += 1

            # 4c: File-partitioned native lake tables (identity & bucket(4)) + subset column list + post-COPY UPDATE/DELETE
            q(cm, "CREATE FOREIGN TABLE biz_part_id (id int, p int, val text) SERVER cat_main OPTIONS (binding_mode 'create', lake_schema 'main')")
            q(cm, "SELECT og_lake.set_partition('biz_part_id'::regclass, ARRAY['p'])")
            q(cm, f"COPY biz_part_id FROM '{src_part_pq}' WITH (FORMAT parquet)")
            assert s(cm, 'SELECT count(*) FROM biz_part_id') == 300
            assert s(cm, 'SELECT count(*) FROM biz_part_id WHERE p IS NULL') == 30
            assert s(cm, 'SELECT count(*) FROM biz_part_id WHERE p = 3') == 60
            part_sub_csv = in_dir / 'part_sub.csv'
            part_sub_csv.write_text('301,sub_null_p\n302,sub_null_p2\n', encoding='utf-8')
            q(cm, f"COPY biz_part_id (id, val) FROM '{part_sub_csv}' WITH (FORMAT csv)")
            q(cm, "UPDATE biz_part_id SET val = val || '_u' WHERE id IN (3, 10, 301)")
            q(cm, 'DELETE FROM biz_part_id WHERE id = 20')
            assert s(cm, 'SELECT count(*) FROM biz_part_id') == 301
            assert s(cm, 'SELECT count(*) FROM biz_part_id WHERE p IS NULL') == 31
            assert q(cm, 'SELECT id, p, val FROM biz_part_id WHERE id IN (3, 10, 301) ORDER BY id') == [
                (3, 3, 'pv_3_u'), (10, None, 'pv_10_u'), (301, None, 'sub_null_p_u')
            ]
            part_id_out = out_dir / 'biz_part_id_out.parquet'
            q(cm, f"COPY biz_part_id TO '{part_id_out}' WITH (FORMAT parquet)")
            assert duckdb_json(f"SELECT count(*) AS cnt, count(*) FILTER (WHERE p IS NULL) AS nc FROM read_parquet('{part_id_out}')") == [{'cnt': 301, 'nc': 31}]
            duckdb_checks += 1

            q(cm, "CREATE FOREIGN TABLE biz_part_bkt (id int, p int, val text) SERVER cat_main OPTIONS (binding_mode 'create', lake_schema 'main')")
            q(cm, "SELECT og_lake.set_partition('biz_part_bkt'::regclass, ARRAY['p'], ARRAY['bucket(4)'])")
            q(cm, "SELECT og_lake.set_option('biz_part_bkt'::regclass, 'data_inlining_row_limit', '0')")
            q(cm, f"COPY biz_part_bkt FROM '{src_part_pq}' WITH (FORMAT parquet)")
            assert s(cm, 'SELECT count(*) FROM biz_part_bkt WHERE p = 4') == 60
            part_bkt_files = q(cm, "SELECT data_file, data_file_size_bytes FROM og_lake.list_files('biz_part_bkt'::regclass)")
            bkt_rows = 0
            for fp, fsz in part_bkt_files:
                fp_abs = fp if fp.startswith('/') else str(cat_main_dir / fp)
                assert os.path.getsize(fp_abs) == fsz
                bkt_rows += duckdb_json(f"SELECT count(*) AS cnt FROM read_parquet('{fp_abs}')")[0]['cnt']
                duckdb_checks += 1
            assert bkt_rows == 300 and len(part_bkt_files) >= 4, (part_bkt_files, bkt_rows)

            # 4d: External file FDW (og_lake_file_fdw) COPY TO Parquet & CSV
            q(cm, f"CREATE SERVER ext_srv_062 FOREIGN DATA WRAPPER og_lake_file_fdw OPTIONS (external_root %s)", (str(in_dir),))
            q(cm, """
                CREATE FOREIGN TABLE ext_src_062 (
                    id int,
                    tag text,
                    score double precision,
                    vec vector(3)
                ) SERVER ext_srv_062 OPTIONS (path 'src_ext.parquet', format 'parquet')
            """)
            ext_out_pq = out_dir / 'ext_fdw_out.parquet'
            ext_out_csv = out_dir / 'ext_fdw_out.csv'
            q(cm, f"COPY ext_src_062 TO '{ext_out_pq}' WITH (FORMAT parquet)")
            q(cm, f"COPY ext_src_062 (id, tag) TO '{ext_out_csv}' WITH (FORMAT csv, HEADER true)")
            assert duckdb_json(f"SELECT id, tag, score, vec::FLOAT[] AS vec FROM read_parquet('{ext_out_pq}') ORDER BY id") == [
                {'id': 1, 'tag': 'alpha', 'score': 12.5, 'vec': [1.0, 2.0, 3.0]},
                {'id': 2, 'tag': 'beta', 'score': -4.25, 'vec': [4.0, 5.0, 6.0]},
            ]
            assert '1,alpha\n2,beta\n' in ext_out_csv.read_text(encoding='utf-8')
            duckdb_checks += 1
            stage_stats['stage4_lake_partition_and_ext_fdw'] = 14

            # Stage 5: Transaction, Savepoint & Mid-Query Error Atomicity (candidate unlink)
            tx_rb_pq = out_dir / 'tx_rb.parquet'
            sp_rb_pq = out_dir / 'sp_rb.parquet'
            div0_pq = out_dir / 'div0_fail.parquet'
            tx_ok_pq = out_dir / 'tx_commit.parquet'

            q(cm, 'BEGIN')
            q(cm, f"COPY (SELECT id, tag FROM ext_src_062 ORDER BY id) TO '{tx_rb_pq}' WITH (FORMAT parquet)")
            q(cm, 'SAVEPOINT sp1')
            q(cm, f"COPY (SELECT id FROM h_sec) TO '{sp_rb_pq}' WITH (FORMAT parquet)")
            q(cm, 'ROLLBACK TO SAVEPOINT sp1')
            assert not sp_rb_pq.exists(), f'Savepoint rollback left candidate file {sp_rb_pq}'
            q(cm, 'ROLLBACK')
            assert not tx_rb_pq.exists(), f'Transaction rollback left candidate file {tx_rb_pq}'

            # Mid-query failure inside savepoint + commit prior lake import
            before_cnt = s(cm, 'SELECT count(*) FROM biz_small')
            q(cm, 'BEGIN')
            q(cm, f"COPY biz_small (id, label, val) FROM '{src_tsv}' WITH (FORMAT text)")
            q(cm, 'SAVEPOINT sp2')
            expect_error(cm, f"COPY (SELECT 1 / (id - id) FROM h_sec) TO '{div0_pq}' WITH (FORMAT parquet)")
            assert not div0_pq.exists(), f'Mid-query error left candidate file {div0_pq}'
            q(cm, 'ROLLBACK TO SAVEPOINT sp2')
            q(cm, 'COMMIT')
            assert s(cm, 'SELECT count(*) FROM biz_small') == before_cnt + 2

            q(cm, 'BEGIN')
            q(cm, f"COPY (SELECT id, txt FROM h_types ORDER BY id) TO '{tx_ok_pq}' WITH (FORMAT parquet)")
            q(cm, 'COMMIT')
            assert tx_ok_pq.is_file()
            assert duckdb_json(f"SELECT id, txt FROM read_parquet('{tx_ok_pq}') ORDER BY id") == [
                {'id': 1, 'txt': 'hello 湖仓'},
                {'id': 2, 'txt': ''},
                {'id': 3, 'txt': None},
            ]
            duckdb_checks += 1
            stage_stats['stage5_xact_savepoint_atomicity'] = 6
        finally:
            cm.close()

        # Cleanup created DBs & roles explicitly
        for dbn in list(created_dbs):
            q(admin, f'DROP DATABASE {dbn}')
            created_dbs.remove(dbn)
        for rn in list(created_roles):
            q(admin, f'DROP ROLE {rn}')
            created_roles.remove(rn)

        # Verify zero leftover DBs / roles
        left_dbs = q(admin, "SELECT datname FROM pg_database WHERE datname LIKE 'db062_%'")
        left_roles = q(admin, "SELECT rolname FROM pg_roles WHERE rolname LIKE 'u062_%'")
        assert left_dbs == [] and left_roles == [], (left_dbs, left_roles)

        res = {
            'status': 'passed',
            'profile': args.profile,
            'enable_thread_pool': s(admin, 'SHOW enable_thread_pool'),
            'thread_pool_attr': s(admin, 'SHOW thread_pool_attr'),
            'postgres_ext_version': s(admin, "SELECT extversion FROM pg_extension WHERE extname='og_lake'"),
            'stages': stage_stats,
            'total_checks': sum(stage_stats.values()),
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
            for rn in created_roles:
                try:
                    q(admin, f'DROP ROLE IF EXISTS {rn}')
                except Exception:
                    pass
            admin.close()
        shutil.rmtree(tmp_root, ignore_errors=True)


if __name__ == '__main__':
    main()
