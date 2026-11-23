#!/usr/bin/env python3
"""SQL0.71 (PGL-2 / PGL-6) local JSON / JSONL verification suite."""

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


def must_fail(conn, sql, expected_codes=None, substr=None, params=None):
    try:
        q(conn, sql, params)
    except psycopg2.Error as exc:
        if expected_codes and exc.pgcode not in expected_codes:
            raise AssertionError(f'Expected pgcodes {expected_codes}, got {exc.pgcode}: {exc}') from exc
        if substr and substr.lower() not in str(exc).lower():
            raise AssertionError(f'Expected {substr!r} in error, got: {exc}') from exc
        return str(exc)
    raise AssertionError(f'Statement unexpectedly succeeded: {sql}')


def run_suite(args):
    tag = uuid.uuid4().hex[:10]
    tmp_root = Path(tempfile.mkdtemp(prefix=f'og_json_{tag}_', dir='/tmp'))
    os.chmod(tmp_root, 0o755)
    admin = pg_connect(args.profile, 'postgres')
    created_dbs = []
    created_roles = []
    conns = []
    stages = {}

    try:
        tp_mode = s(admin, 'SHOW enable_thread_pool')
        role_main = f'ujson_{tag}'
        role_other = f'ujson2_{tag}'
        db_main = f'dbjson_{tag}'
        db_fresh = f'dbjson_f_{tag}'
        q(admin, f"CREATE ROLE {role_main} NOLOGIN NOINHERIT ENCRYPTED PASSWORD %s", (md5_role_pw('Json071!', role_main),))
        q(admin, f"CREATE ROLE {role_other} LOGIN NOINHERIT ENCRYPTED PASSWORD %s", (md5_role_pw('Json071!', role_other),))
        created_roles.extend([role_main, role_other])
        q(admin, f"CREATE DATABASE {db_main} WITH DBCOMPATIBILITY 'PG' ENCODING 'UTF8'")
        q(admin, f"CREATE DATABASE {db_fresh} WITH DBCOMPATIBILITY 'PG' ENCODING 'UTF8'")
        created_dbs.extend([db_main, db_fresh])

        # Stage 1: Upgrade 0.70 -> 0.71, pre-upgrade refusal gate, fresh 0.71 install, 0.70 ops compat
        c1 = 0
        cm = pg_connect(args.profile, db_main)
        conns.append(cm)
        q(cm, "CREATE EXTENSION og_lake VERSION '0.70'")
        assert s(cm, "SELECT extversion FROM pg_extension WHERE extname='og_lake'") == '0.70'

        ext_dir = tmp_root / 'ext'
        ext_dir.mkdir(parents=True, exist_ok=True)
        os.chmod(ext_dir, 0o755)
        pre_jsonl = ext_dir / 'pre.jsonl'
        pre_jsonl.write_text('{"id": 1}\n', encoding='utf-8')
        os.chmod(pre_jsonl, 0o644)

        must_fail(cm, "SELECT * FROM og_lake.describe_external_path(%s::text)", ('0A000',), '0.71', (str(pre_jsonl),))
        q(cm, "CREATE TABLE public.pre_t (id int)")
        must_fail(cm, f"COPY public.pre_t FROM '{pre_jsonl}' (FORMAT 'json')", ('0A000',), '0.71')
        c1 += 2

        q(cm, "ALTER EXTENSION og_lake UPDATE TO '0.71'")
        assert s(cm, "SELECT extversion FROM pg_extension WHERE extname='og_lake'") == '0.71'
        hs = json.loads(s(cm, "SELECT og_lake.host_status()"))
        assert hs['module_version'] in ('0.71', '0.72', '0.73', '0.74')
        assert hs['json_format_io'] is True and hs['json_format_sql_version'] == '0.71'
        assert 'json' in hs['copy_formats'] and 'json' in hs['external_file_formats']
        c1 += 1

        cf = pg_connect(args.profile, db_fresh)
        conns.append(cf)
        q(cf, "CREATE EXTENSION og_lake")
        assert s(cf, "SELECT extversion FROM pg_extension WHERE extname='og_lake'") in ('0.71', '0.72', '0.73', '0.74')
        cf.close()
        conns.remove(cf)
        c1 += 1

        q(cm, f"CREATE SCHEMA biz; GRANT USAGE ON SCHEMA biz TO {role_main}")
        q(cm, "SELECT og_lake.configure_internal_owner(%s::name)", (role_main,))
        cat_dir = tmp_root / 'cat'
        idx_dir = tmp_root / 'idx'
        idx_dir.mkdir(parents=True, exist_ok=True)
        os.chmod(idx_dir, 0o755)
        q(cm, "SELECT og_lake.create_catalog('c_json'::name, 'lake_metadata'::name, %s::text, NULL::text)", (str(cat_dir),))
        q(cm, "SELECT og_lake.set_option('c_json'::name, 'data_inlining_row_limit'::text, '50'::text)")
        q(cm, "SELECT og_lake.set_option('c_json'::name, 'index_root'::text, %s::text)", (str(idx_dir),))
        manifest = s(cm, "SELECT og_lake.backup_manifest('c_json'::name)")
        assert isinstance(manifest, dict) and manifest['server_name'] == 'c_json' and manifest['module_version'] in ('0.71', '0.72', '0.73', '0.74'), manifest
        ret = q(cm, "SELECT catalog_name FROM og_lake.retention_status('c_json'::name)")
        assert ret == [('c_json',)], ret
        c1 += 1
        stages['stage1_upgrade_capabilities_ops'] = c1

        # Stage 2: describe_external_file / describe_external_path / external_file_info
        c2 = 0
        q(cm, f"CREATE SERVER ext_json_srv FOREIGN DATA WRAPPER og_lake_file_fdw OPTIONS (external_root %s)", (str(ext_dir),))
        sample_jsonl = ext_dir / 'sample.jsonl'
        sample_bytes = (
            '\ufeff'
            '{"id": 1, "active": true, "score": 10, "d": "2026-11-01", "meta": {"tags": ["a", "b"]}, "note": "first"}\r\n'
            '\n'
            '{"id": 2, "active": false, "score": 12.5, "d": "2026-11-02", "meta": [1, 2], "note": null}\r\n'
            '{"id": 3, "active": true, "score": null, "d": "2026-11-03", "note": "third", "extra": "tail"}\n'
        ).encode('utf-8')
        sample_jsonl.write_bytes(sample_bytes)
        os.chmod(sample_jsonl, 0o644)

        desc_srv = {r[0]: r[1] for r in q(cm, "SELECT column_name, column_type FROM og_lake.describe_external_file('ext_json_srv', 'sample.jsonl')")}
        desc_path = {r[0]: r[1] for r in q(cm, "SELECT column_name, column_type FROM og_lake.describe_external_path(%s::text)", (str(sample_jsonl),))}
        assert desc_srv == desc_path, (desc_srv, desc_path)
        assert desc_srv['id'] == 'bigint'
        assert desc_srv['active'] == 'boolean'
        assert desc_srv['score'] == 'double precision'
        assert desc_srv['d'] == 'date'
        assert desc_srv['meta'] == 'json'
        assert desc_srv['note'] == 'text'
        assert desc_srv['extra'] == 'text'
        info_jsonl = q(cm, "SELECT format, file_bytes, row_count, row_group_count, column_count FROM og_lake.external_file_info('ext_json_srv', 'sample.jsonl')")[0]
        assert info_jsonl[0] == 'json' and info_jsonl[1] == len(sample_bytes) and info_jsonl[2] == 3 and info_jsonl[3] is None and info_jsonl[4] >= 6, info_jsonl
        c2 += 3

        pretty_json = ext_dir / 'pretty.json'
        pretty_json.write_text(json.dumps([
            {'id': 10, 'price': 99, 'title': 'alpha'},
            {'id': 11, 'price': 10.25, 'title': None},
        ], indent=2), encoding='utf-8')
        os.chmod(pretty_json, 0o644)
        desc_arr = {r[0]: r[1] for r in q(cm, "SELECT column_name, column_type FROM og_lake.describe_external_file('ext_json_srv', 'pretty.json', 'json')")}
        assert desc_arr == {'id': 'bigint', 'price': 'double precision', 'title': 'text'}, desc_arr
        info_arr = q(cm, "SELECT format, row_count, row_group_count, column_count FROM og_lake.external_file_info('ext_json_srv', 'pretty.json')")[0]
        assert info_arr == ('json', 2, None, 3), info_arr
        c2 += 2

        empty_json = ext_dir / 'empty.jsonl'
        empty_json.write_text('  \n\n', encoding='utf-8')
        os.chmod(empty_json, 0o644)
        must_fail(cm, "SELECT * FROM og_lake.describe_external_file('ext_json_srv', 'empty.jsonl')", ('02000',), 'empty')

        bad_scalar_arr = ext_dir / 'scalar.json'
        bad_scalar_arr.write_text('[1, 2, 3]', encoding='utf-8')
        os.chmod(bad_scalar_arr, 0o644)
        must_fail(cm, "SELECT * FROM og_lake.external_file_info('ext_json_srv', 'scalar.json')", ('22P02',))

        sym = ext_dir / 'sym.jsonl'
        sym.symlink_to(sample_jsonl)
        must_fail(cm, "SELECT * FROM og_lake.describe_external_file('ext_json_srv', 'sym.jsonl')", ('42501', '58P01', 'HV000'))
        must_fail(cm, "SELECT * FROM og_lake.describe_external_path('s3://bucket/a.jsonl')", ('0A000',))
        must_fail(cm, "SELECT * FROM og_lake.describe_external_file('ext_json_srv', '../escape.jsonl')", substr='path')
        c2 += 5
        stages['stage2_describe_and_info_json'] = c2

        # Stage 3: og_lake_file_fdw JSON/JSONL foreign tables
        c3 = 0
        q(cm, """
            CREATE FOREIGN TABLE biz.ft_jsonl (
                id bigint,
                active boolean,
                score double precision,
                d date,
                meta json,
                note text,
                extra text
            ) SERVER ext_json_srv OPTIONS (path 'sample.jsonl')
        """)
        rows_jsonl = q(cm, "SELECT id, active, score, d::text, meta::text, note, extra FROM biz.ft_jsonl ORDER BY id")
        assert len(rows_jsonl) == 3
        assert rows_jsonl[0][:4] == (1, True, 10.0, '2026-11-01')
        assert json.loads(rows_jsonl[0][4]) == {'tags': ['a', 'b']}
        assert rows_jsonl[1][2] == 12.5 and rows_jsonl[1][5] is None
        assert rows_jsonl[2][2] is None and rows_jsonl[2][6] == 'tail'

        rescan_rows = q(cm, """
            SELECT v.k, f.note FROM (VALUES (3), (1), (2)) AS v(k)
            JOIN biz.ft_jsonl f ON f.id = v.k
            ORDER BY v.k
        """)
        assert rescan_rows == [(1, 'first'), (2, None), (3, 'third')], rescan_rows
        q(cm, "ANALYZE biz.ft_jsonl")
        assert s(cm, "SELECT reltuples FROM pg_class WHERE oid='biz.ft_jsonl'::regclass") == 3.0
        exp_fdw = '\n'.join(r[0] for r in q(cm, "EXPLAIN (COSTS OFF) SELECT id FROM biz.ft_jsonl"))
        assert 'Foreign Format: json' in exp_fdw, exp_fdw
        c3 += 4

        q(cm, """
            CREATE FOREIGN TABLE biz.ft_arr (
                id bigint,
                price double precision,
                title text
            ) SERVER ext_json_srv OPTIONS (path 'pretty.json', format 'json')
        """)
        assert q(cm, "SELECT id, price, title FROM biz.ft_arr ORDER BY id") == [
            (10, 99.0, 'alpha'),
            (11, 10.25, None),
        ]

        q(cm, "CREATE FOREIGN TABLE biz.ft_empty (id bigint) SERVER ext_json_srv OPTIONS (path 'empty.jsonl', format 'ndjson')")
        assert q(cm, "SELECT * FROM biz.ft_empty") == []

        q(cm, "CREATE FOREIGN TABLE biz.ft_nn (id bigint NOT NULL, title text NOT NULL) SERVER ext_json_srv OPTIONS (path 'pretty.json', format 'jsonl')")
        must_fail(cm, "SELECT * FROM biz.ft_nn", ('23502',))
        c3 += 3

        must_fail(cm, "CREATE FOREIGN TABLE biz.ft_bad1 (id bigint) SERVER ext_json_srv OPTIONS (path 'sample.jsonl', format 'json', header 'true')", ('HV00D', '42704', '0A000'))
        must_fail(cm, "CREATE FOREIGN TABLE biz.ft_bad2 (id bigint) SERVER ext_json_srv OPTIONS (path 'sample.jsonl', format 'json', delimiter ',')", ('HV00D', '42704', '0A000'))
        must_fail(cm, "INSERT INTO biz.ft_jsonl (id) VALUES (9)", ('0A000', '55000'))
        uo = pg_connect(args.profile, db_main, user=role_other, password='Json071!')
        conns.append(uo)
        must_fail(uo, "SELECT * FROM og_lake.external_file_info('ext_json_srv', 'sample.jsonl')", ('42501',))
        uo.close()
        conns.remove(uo)
        c3 += 4
        stages['stage3_external_fdw_json'] = c3

        # Stage 4: COPY FROM / TO (FORMAT 'json' | 'jsonl' | 'ndjson') + DuckDB parity
        c4 = 0
        cols_ddl = """
            id int4 NOT NULL,
            s2 int2,
            b8 bigint,
            f4 real,
            f8 double precision,
            dec numeric(14,4),
            flag boolean,
            txt text,
            vc text,
            dt date,
            ts timestamp,
            uid uuid,
            raw bytea,
            js json,
            vec vector(4),
            def_col text DEFAULT 'dft'
        """
        q(cm, f"CREATE TABLE biz.ex_copy ({cols_ddl})")
        q(cm, f"""
            CREATE FOREIGN TABLE biz.t_copy ({cols_ddl})
            SERVER c_json OPTIONS (binding_mode 'create', lake_schema 'main')
        """)
        q(cm, f"""
            CREATE FOREIGN TABLE biz.t_round ({cols_ddl})
            SERVER c_json OPTIONS (binding_mode 'create', lake_schema 'main')
        """)
        q(cm, "CREATE TABLE biz.ex_jb (id int4, vc varchar(24), jb jsonb)")

        ingest_jsonl = ext_dir / 'ingest.jsonl'
        ingest_lines = [
            json.dumps({
                'id': 1, 's2': 7, 'b8': 9000000001, 'f4': 1.25, 'f8': 2.5,
                'dec': 1234.5678, 'flag': True, 'txt': 'hello\nworld', 'vc': 'abc',
                'dt': '2026-11-19', 'ts': '2026-11-19 10:20:30',
                'UID': '11111111-2222-4333-8444-000000000001',
                'raw': '\\xdeadbeef', 'js': {'k': 1, 'arr': [True, None]},
                'jb': {'nested': 'ok'}, 'vec': [1.0, 2.0, 3.0, 4.0],
                'ignored_extra': 999
            }),
            json.dumps({
                'id': 2, 's2': None, 'b8': -42, 'f4': '3.5', 'f8': -7.25,
                'dec': '-0.0500', 'flag': False, 'txt': None, 'vc': 'xyz',
                'dt': '2026-11-20', 'ts': '2026-11-20 01:02:03',
                'uid': '11111111-2222-4333-8444-000000000002',
                'raw': '\\x00ff', 'js': [10, 20], 'jb': {'n': 2},
                'vec': '[0.5, 1.5, 2.5, 3.5]'
            }),
        ]
        ingest_jsonl.write_text('\n'.join(ingest_lines) + '\n', encoding='utf-8')
        os.chmod(ingest_jsonl, 0o644)

        col_list = "(id, s2, b8, f4, f8, dec, flag, txt, vc, dt, ts, uid, raw, js, vec)"
        q(cm, f"COPY biz.ex_copy {col_list} FROM 'file://{ingest_jsonl}' (FORMAT 'json')")
        q(cm, f"COPY biz.t_copy {col_list} FROM 'file://{ingest_jsonl}' (FORMAT 'jsonl')")
        q(cm, "SELECT og_lake.flush('biz.t_copy'::regclass)")
        q(cm, f"COPY biz.ex_jb (id, vc, jb) FROM 'file://{ingest_jsonl}' (FORMAT 'json')")
        assert q(cm, "SELECT id, vc, jb::text FROM biz.ex_jb ORDER BY id") == [
            (1, 'abc', '{"nested": "ok"}'),
            (2, 'xyz', '{"n": 2}'),
        ]

        verify_sql = """
            SELECT id, s2, b8, round(f4::numeric, 4), round(f8::numeric, 4), dec::text, flag, txt, vc,
                   dt::text, ts::text, uid::text, encode(raw, 'hex'), js::text, vec::text, def_col
            FROM {tbl} ORDER BY id
        """
        oracle_rows = q(cm, verify_sql.format(tbl='biz.ex_copy'))
        assert len(oracle_rows) == 2 and oracle_rows[0][-1] == 'dft' and oracle_rows[1][-1] == 'dft', oracle_rows
        assert q(cm, verify_sql.format(tbl='biz.t_copy')) == oracle_rows
        c4 += 2

        export_jsonl = ext_dir / 'export.jsonl'
        q(cm, f"COPY biz.t_copy TO 'file://{export_jsonl}' (FORMAT 'jsonl')")
        assert export_jsonl.is_file()

        query_ndjson = ext_dir / 'query_out.ndjson'
        q(cm, f"COPY (SELECT id, dec::double precision AS dec_f, flag, vc, dt::text AS dt_s FROM biz.t_copy WHERE id >= 2 ORDER BY id) TO '{query_ndjson}' (FORMAT 'ndjson')")

        ddb_rows = duckdb_query(
            f"SELECT id, s2, b8, round(f4, 4) AS f4r, round(f8, 4) AS f8r, round(dec, 4) AS decr, "
            f"flag, txt, vc, CAST(dt AS VARCHAR) AS dts, uid "
            f"FROM read_json_auto('{export_jsonl}') ORDER BY id"
        )
        assert len(ddb_rows) == 2
        assert int(ddb_rows[0]['id']) == 1 and int(ddb_rows[0]['s2']) == 7 and int(ddb_rows[0]['b8']) == 9000000001, ddb_rows
        assert float(ddb_rows[0]['decr']) == 1234.5678 and ddb_rows[0]['flag'] is True and ddb_rows[0]['txt'] == 'hello\nworld', ddb_rows
        assert int(ddb_rows[1]['id']) == 2 and ddb_rows[1]['s2'] is None and int(ddb_rows[1]['b8']) == -42 and float(ddb_rows[1]['decr']) == -0.05 and ddb_rows[1]['txt'] is None and ddb_rows[1]['vc'] == 'xyz', ddb_rows
        ddb_q = duckdb_query(
            f"SELECT id, round(dec_f, 4) AS decr, flag, vc, dt_s FROM read_json_auto('{query_ndjson}')"
        )
        assert len(ddb_q) == 1 and int(ddb_q[0]['id']) == 2 and float(ddb_q[0]['decr']) == -0.05 and ddb_q[0]['flag'] is False and ddb_q[0]['vc'] == 'xyz' and ddb_q[0]['dt_s'] == '2026-11-20', ddb_q

        q(cm, f"COPY biz.t_round FROM '{export_jsonl}' (FORMAT 'json')")
        assert q(cm, verify_sql.format(tbl='biz.t_round')) == oracle_rows
        c4 += 3

        q(cm, "CREATE TABLE biz.ex_empty_copy (id int4, txt text)")
        empty_out = ext_dir / 'empty_out.jsonl'
        q(cm, f"COPY biz.ex_empty_copy TO '{empty_out}' (FORMAT 'json')")
        assert empty_out.exists() and empty_out.stat().st_size == 0
        q(cm, f"COPY biz.ex_empty_copy FROM '{empty_out}' (FORMAT 'json')")
        assert s(cm, "SELECT count(*) FROM biz.ex_empty_copy") == 0
        c4 += 1

        must_fail(cm, f"COPY biz.t_copy TO '{export_jsonl}' (FORMAT 'json')", ('58P02',))
        aborted_out = ext_dir / 'aborted.jsonl'
        must_fail(cm, f"COPY (SELECT id, 1 / (id - 2) AS boom FROM biz.t_copy ORDER BY id) TO '{aborted_out}' (FORMAT 'json')", ('22012',))
        assert not aborted_out.exists(), f'Aborted COPY TO leaked file {aborted_out}'
        c4 += 2

        must_fail(cm, f"COPY biz.ex_copy FROM STDIN (FORMAT 'json')", ('0A000',))
        must_fail(cm, f"COPY biz.ex_copy TO STDOUT (FORMAT 'json')", ('0A000',))
        must_fail(cm, f"COPY biz.ex_copy TO '{ext_dir / 'comp.jsonl'}' (FORMAT 'json', COMPRESSION 'zstd')", ('42601', '0A000'))
        must_fail(cm, f"COPY biz.ex_copy FROM '{ingest_jsonl}' (FORMAT 'json', HEADER true)", ('0A000', '42601'))
        must_fail(cm, f"COPY biz.ex_copy FROM '{ingest_jsonl}' (FORMAT 'json', DELIMITER ',')", ('0A000', '42601'))

        bad_type_jsonl = ext_dir / 'bad_type.jsonl'
        bad_type_jsonl.write_text('{"id": true}\n', encoding='utf-8')
        os.chmod(bad_type_jsonl, 0o644)
        must_fail(cm, f"COPY biz.ex_copy (id) FROM '{bad_type_jsonl}' (FORMAT 'json')", ('42804', '22P02'))

        bad_obj_jsonl = ext_dir / 'bad_nest.jsonl'
        bad_obj_jsonl.write_text('{"id": {"k": 1}}\n', encoding='utf-8')
        os.chmod(bad_obj_jsonl, 0o644)
        must_fail(cm, f"COPY biz.ex_copy (id) FROM '{bad_obj_jsonl}' (FORMAT 'json')", ('42804',))
        c4 += 7
        stages['stage4_copy_from_to_duckdb_parity'] = c4

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
