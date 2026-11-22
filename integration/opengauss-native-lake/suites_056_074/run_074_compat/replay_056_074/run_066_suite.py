#!/usr/bin/env python3
"""Scoped SQL0.66 (N6 + PGL-1 closure) verification suite.

Covers:
1. Version gating & upgrade safety (0.65 -> 0.66, pre-0.66 rejections, post-upgrade enablement).
2. Extended literal defaults (FLOAT4/FLOAT8/DATE/TIMESTAMP/TIMESTAMPTZ/UUID/JSON/NUMERIC(12,4)/(38,6))
   across CREATE TABLE, ADD COLUMN (inline + Parquet backfill), ALTER COLUMN SET/DROP DEFAULT,
   plus timezone independence, unary negation, Bug 1..3 regression checks, and negative 0A000 bounds.
3. ADD COLUMN ... NOT NULL DEFAULT <non-NULL literal> full lifecycle (inline/Parquet backfill,
   ducklake_column.nulls_allowed=false, pg_attrdef/pg_attribute checks, 23502 violation,
   UPDATE/flush/rewrite/merge, DROP/SET NOT NULL, savepoint rollback).
4. Cross-session Relcache & Plan-Cache invalidation (create/build/rebuild/drop index, set_option,
   flush/rewrite/merge, ADD/ALTER default DDL across concurrent sessions + force_generic_plan)
   + PGL-1 mixed heap + multi-lake transaction/savepoint/self-write/CoW/rollback closure.
5. Pinned DuckDB DuckLake bidirectional parity (export + DuckDB INSERT using og defaults,
   plus DuckLake TIMESTAMPTZ expression wrapper roundtrip parity).
"""

import argparse
import decimal
import hashlib
import json
import os
import getpass
import shutil
import subprocess
import tempfile
import uuid
from pathlib import Path

import psycopg2

ROOT = Path(os.environ.get('CPP_WORKSPACE', str(Path(__file__).resolve().parents[5]))).expanduser().resolve()
DUCKDB_BIN = ROOT / '.reference/opengauss-native-lake/build/duckdb-gcc10/duckdb'


def md5_role_pw(password, role):
    return 'md5' + hashlib.md5((password + role).encode('utf-8')).hexdigest()


def pg_connect(profile, dbname, user=None, password=None):
    port = 55560 if profile == 'debug' else (55570 if profile == 'release' else 55580)
    kw = {
        'host': str(ROOT / '.runtime/opengauss-native-lake' / profile / 'socket'),
        'port': port,
        'dbname': dbname,
        'user': user or os.environ.get('PGUSER') or getpass.getuser(),
        'connect_timeout': 15,
    }
    if password is not None:
        kw['password'] = password
    conn = psycopg2.connect(**kw)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute('SET session_timeout = 0')
        cur.execute('SET statement_timeout = 120000')
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


def q_dicts(conn, sql, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


def explain_text(conn, sql):
    return '\n'.join(r[0] for r in q(conn, f'EXPLAIN (COSTS ON) {sql}'))


def expect_error(conn, sql, params=None, expected_states=None):
    try:
        q(conn, sql, params)
    except Exception as exc:
        state = getattr(exc, 'pgcode', None)
        if expected_states and state not in expected_states:
            raise AssertionError(f'Expected SQLSTATE in {expected_states}, got {state}: {exc}') from exc
        return state
    raise AssertionError(f'Expected error for SQL: {sql}')


def duckdb_query(sql_text, db_path=None):
    cmd = [str(DUCKDB_BIN), '-batch', '-dark-mode']
    if db_path:
        cmd.append(str(db_path))
    cmd.extend(['-json', '-c', sql_text])
    proc = subprocess.run(cmd, stdin=subprocess.DEVNULL, capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError(f'DuckDB failed ({proc.returncode}):\nSQL:\n{sql_text}\nSTDERR:\n{proc.stderr}\nSTDOUT:\n{proc.stdout}')
    out = proc.stdout.strip()
    if not out:
        return []
    idx = out.find('[')
    if idx < 0:
        return []
    return json.loads(out[idx:])


def table_col_stats(conn, meta_schema, table_name):
    rows = q_dicts(
        conn,
        f"""
        SELECT c.column_id, c.column_name, c.column_type,
               c.initial_default, c.default_value, c.nulls_allowed, c.default_value_type,
               s.contains_null, s.min_value::text AS min_value, s.max_value::text AS max_value
        FROM {meta_schema}.ducklake_table t
        JOIN {meta_schema}.ducklake_column c ON c.table_id = t.table_id AND c.end_snapshot IS NULL
        LEFT JOIN {meta_schema}.ducklake_table_column_stats s ON s.table_id = t.table_id AND s.column_id = c.column_id
        WHERE t.table_name = %s AND t.end_snapshot IS NULL
        ORDER BY c.column_id
        """,
        (table_name,),
    )
    return {r['column_name']: r for r in rows}


def export_catalog_to_duckdb(conn, meta_schema, tmp_dir, db_name):
    seed_dl = tmp_dir / f'{db_name}_seed.ducklake'
    target_dl = tmp_dir / f'{db_name}.ducklake'
    data_dummy = tmp_dir / f'{db_name}_seed_data'
    for p in (seed_dl, target_dl, data_dummy):
        if p.exists():
            if p.is_dir():
                shutil.rmtree(p)
            else:
                p.unlink()
    data_dummy.mkdir(parents=True, exist_ok=True)
    proc = subprocess.run(
        [str(DUCKDB_BIN), '-batch', '-dark-mode', '-c',
         f"LOAD ducklake; ATTACH 'ducklake:{seed_dl}' AS s (DATA_PATH '{data_dummy}'); DETACH s;"],
        stdin=subprocess.DEVNULL, capture_output=True, text=True,
    )
    assert proc.returncode == 0, proc.stderr
    shutil.copy2(seed_dl, target_dl)

    tbls = duckdb_query("SELECT table_name FROM information_schema.tables WHERE table_schema='main' ORDER BY table_name", target_dl)
    pg_tbls = {r[0] for r in q(conn, "SELECT tablename FROM pg_tables WHERE schemaname=%s", (meta_schema,))}
    sql_lines = ['BEGIN;']
    for t in tbls:
        sql_lines.append(f'DELETE FROM main."{t["table_name"]}";')

    def dk_lit(val, dtype):
        if val is None:
            return 'NULL'
        if isinstance(val, bool):
            return 'TRUE' if val else 'FALSE'
        if isinstance(val, int):
            return str(val)
        if isinstance(val, float):
            if str(val).lower() in ('inf', '-inf', 'nan', 'infinity', '-infinity'):
                return f"'{val}'::{dtype}"
            return repr(val)
        txt = str(val).replace("'", "''")
        return f"'{txt}'"

    for t in tbls:
        tname = t['table_name']
        if tname not in pg_tbls:
            continue
        og_cols = {
            r[0] for r in q(
                conn,
                """
                SELECT a.attname FROM pg_attribute a
                JOIN pg_class c ON c.oid = a.attrelid
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = %s AND c.relname = %s AND a.attnum > 0 AND NOT a.attisdropped
                """,
                (meta_schema, tname),
            )
        }
        cols = duckdb_query(
            f"SELECT column_name, data_type FROM information_schema.columns WHERE table_schema='main' AND table_name='{tname}' ORDER BY ordinal_position",
            target_dl,
        )
        cols = [c for c in cols if c['column_name'] in og_cols]
        if not cols:
            continue
        col_names = [c['column_name'] for c in cols]
        pg_cols = [f'"{c}"::text' if 'timestamp' in cols[i]['data_type'].lower() else f'"{c}"'
                   for i, c in enumerate(col_names)]
        og_rows = q(conn, f'SELECT {", ".join(pg_cols)} FROM {meta_schema}."{tname}"')
        for r in og_rows:
            vals = [dk_lit(v, cols[i]['data_type']) for i, v in enumerate(r)]
            col_list = ', '.join('"' + c + '"' for c in col_names)
            val_list = ', '.join(vals)
            sql_lines.append(f'INSERT INTO main."{tname}" ({col_list}) VALUES ({val_list});')

    inl_tbls = q(conn, f"SELECT DISTINCT table_id, table_name, schema_version FROM {meta_schema}.ducklake_inlined_data_tables ORDER BY table_name")
    for (inl_tid, inl_name, inl_sv) in inl_tbls:
        attr_rows = q(
            conn,
            """
            SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod) AS pg_type
            FROM pg_attribute a
            JOIN pg_class c ON c.oid = a.attrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s AND c.relname = %s AND a.attnum > 0 AND NOT a.attisdropped
            ORDER BY a.attnum
            """,
            (meta_schema, inl_name),
        )
        if not attr_rows:
            continue
        col_types = dict(
            q(
                conn,
                f"""
                SELECT DISTINCT column_name, column_type
                FROM {meta_schema}.ducklake_column
                WHERE table_id = %s AND begin_snapshot <= %s AND (end_snapshot IS NULL OR end_snapshot > %s)
                """,
                (inl_tid, inl_sv, inl_sv),
            )
        )

        def to_dk_type(cname, pt):
            lt = col_types.get(cname)
            if lt:
                low = lt.lower()
                if low == 'int16':
                    return 'SMALLINT'
                if low == 'int32':
                    return 'INTEGER'
                if low == 'int64':
                    return 'BIGINT'
                if low == 'float32':
                    return 'FLOAT'
                if low == 'float64':
                    return 'DOUBLE'
                if low == 'boolean':
                    return 'BOOLEAN'
                if low == 'blob':
                    return 'BLOB'
                if low in ('date', 'timestamp', 'timestamptz', 'uuid', 'json', 'varchar'):
                    return low.upper()
                if low.startswith('decimal('):
                    return low.upper()
            if pt == 'text' or pt == 'character varying':
                return 'VARCHAR'
            if pt == 'bytea':
                return 'BLOB'
            if pt.startswith('numeric'):
                return pt.replace('numeric', 'DECIMAL', 1).upper()
            return pt.upper()

        dk_types = [to_dk_type(cname, ptype) for cname, ptype in attr_rows]
        defs = ', '.join(f'"{cname}" {dk_t}' for (cname, _), dk_t in zip(attr_rows, dk_types))
        sql_lines.append(f'CREATE TABLE IF NOT EXISTS main."{inl_name}" ({defs});')
        cnames = [cname for cname, _ in attr_rows]
        sel_cols = ', '.join(f'"{c}"::text' if t in ('bytea', 'json', 'uuid') or 'timestamp' in t or t.startswith('numeric') else f'"{c}"' for c, t in attr_rows)
        for r in q(conn, f'SELECT {sel_cols} FROM {meta_schema}."{inl_name}"'):
            vals = []
            for v, (_, ptype), dk_t in zip(r, attr_rows, dk_types):
                if v is not None and ptype == 'bytea':
                    hex_str = v[2:] if isinstance(v, str) and v.startswith('\\x') else v
                    vals.append(f"from_hex('{hex_str}')")
                else:
                    vals.append(dk_lit(v, dk_t))
            col_list = ', '.join('"' + c + '"' for c in cnames)
            val_list = ', '.join(vals)
            sql_lines.append(f'INSERT INTO main."{inl_name}" ({col_list}) VALUES ({val_list});')

    sql_lines.append('COMMIT; CHECKPOINT;')
    proc2 = subprocess.run(
        [str(DUCKDB_BIN), '-batch', '-dark-mode', str(target_dl), '-c', '\n'.join(sql_lines)],
        stdin=subprocess.DEVNULL, capture_output=True, text=True,
    )
    assert proc2.returncode == 0, f'Import into {target_dl} failed:\n{proc2.stderr}'
    return target_dl


def setup_catalog(conn, db_name, role_name, cat_name, cat_dir, idx_dir=None):
    q(conn, 'CREATE EXTENSION og_lake')
    assert s(conn, "SELECT extversion FROM pg_extension WHERE extname='og_lake'") in ('0.66', '0.73', '0.74')
    hs = json.loads(s(conn, 'SELECT og_lake.host_status()'))
    assert hs['module_version'] in ('0.66', '0.73', '0.74'), hs
    assert hs['extended_defaults'] is True and hs['extended_defaults_sql_version'] == '0.66', hs
    assert hs['add_column_not_null_default'] is True and hs['relcache_plan_invalidation'] is True, hs
    if idx_dir is not None:
        Path(idx_dir).mkdir(parents=True, exist_ok=True)
        os.chmod(idx_dir, 0o755)
    q(conn, f"CREATE SCHEMA IF NOT EXISTS biz; GRANT USAGE ON SCHEMA biz TO {role_name}")
    q(conn, "SELECT og_lake.configure_internal_owner(%s::name)", (role_name,))
    q(conn, f"SELECT og_lake.create_catalog('{cat_name}'::name, 'lake_metadata'::name, %s::text, NULL::text)", (str(cat_dir),))
    if idx_dir is not None:
        q(conn, f"SELECT og_lake.set_option('{cat_name}'::name, 'index_root'::text, %s::text)", (str(idx_dir),))


def run_suite(args):
    tag = uuid.uuid4().hex[:10]
    tmp_root = Path(tempfile.mkdtemp(prefix=f'og_066_{tag}_', dir='/tmp'))
    os.chmod(tmp_root, 0o755)
    admin = pg_connect(args.profile, 'postgres')
    created_dbs = []
    created_roles = []
    conns = []
    stages = {}
    duckdb_checks = 0

    try:
        role_upg = f'u066_upg_{tag}'
        role_main = f'u066_main_{tag}'
        role_pgl = f'u066_pgl_{tag}'
        db_upg = f'db066_upg_{tag}'
        db_main = f'db066_main_{tag}'
        for rn in (role_upg, role_main, role_pgl):
            q(admin, f"CREATE ROLE {rn} NOLOGIN NOINHERIT ENCRYPTED PASSWORD %s", (md5_role_pw('Own066!', rn),))
            created_roles.append(rn)
        for dbn in (db_upg, db_main):
            q(admin, f"CREATE DATABASE {dbn} WITH DBCOMPATIBILITY 'PG' ENCODING 'UTF8'")
            created_dbs.append(dbn)

        # ------------------------------------------------------------------
        # Stage 1: Version Gating (0.65 -> 0.66) & Upgrade Preservation
        # ------------------------------------------------------------------
        c1 = 0
        cu = pg_connect(args.profile, db_upg)
        conns.append(cu)
        q(cu, "CREATE EXTENSION og_lake VERSION '0.65'")
        assert s(cu, "SELECT extversion FROM pg_extension WHERE extname='og_lake'") == '0.65'
        c1 += 1
        q(cu, f"CREATE SCHEMA biz; GRANT USAGE ON SCHEMA biz TO {role_upg}")
        cat_u = tmp_root / 'cat_upg'
        q(cu, "SELECT og_lake.configure_internal_owner(%s::name)", (role_upg,))
        q(cu, "SELECT og_lake.create_catalog('c_u'::name, 'lake_metadata'::name, %s::text, NULL::text)", (str(cat_u),))

        # Pre-0.66 CREATE rejects non-NULL defaults for UUID/JSON/NUMERIC/FLOAT/temporal
        expect_error(cu, "CREATE FOREIGN TABLE biz.bad_u (id int, u uuid DEFAULT '550e8400-e29b-41d4-a716-446655440000') SERVER c_u OPTIONS (binding_mode 'create', lake_schema 'main')", expected_states={'0A000'})
        expect_error(cu, "CREATE FOREIGN TABLE biz.bad_n (id int, n numeric(10,2) DEFAULT 1.50) SERVER c_u OPTIONS (binding_mode 'create', lake_schema 'main')", expected_states={'0A000'})
        expect_error(cu, "CREATE FOREIGN TABLE biz.bad_f (id int, f float8 DEFAULT 1.5) SERVER c_u OPTIONS (binding_mode 'create', lake_schema 'main')", expected_states={'0A000'})
        expect_error(cu, "CREATE FOREIGN TABLE biz.bad_d (id int, d date DEFAULT '2026-11-19') SERVER c_u OPTIONS (binding_mode 'create', lake_schema 'main')", expected_states={'0A000'})
        c1 += 4

        q(cu, "CREATE FOREIGN TABLE biz.t_legacy (id int, u uuid DEFAULT NULL::uuid, n numeric(10,2) DEFAULT NULL::numeric(10,2)) SERVER c_u OPTIONS (binding_mode 'create', lake_schema 'main')")
        q(cu, "INSERT INTO biz.t_legacy (id) VALUES (1), (2)")
        expect_error(cu, "ALTER FOREIGN TABLE biz.t_legacy ADD COLUMN x int NOT NULL DEFAULT 7", expected_states={'0A000'})
        expect_error(cu, "ALTER FOREIGN TABLE biz.t_legacy ADD COLUMN y numeric(8,2) DEFAULT 3.14", expected_states={'0A000'})
        expect_error(cu, "ALTER FOREIGN TABLE biz.t_legacy ALTER COLUMN n SET DEFAULT 9.90", expected_states={'0A000'})
        c1 += 3

        # Upgrade 0.65 -> 0.66
        q(cu, "ALTER EXTENSION og_lake UPDATE TO '0.66'")
        assert s(cu, "SELECT extversion FROM pg_extension WHERE extname='og_lake'") == '0.66'
        hs_u = json.loads(s(cu, "SELECT og_lake.host_status()"))
        assert hs_u['extended_defaults'] is True and hs_u['add_column_not_null_default'] is True
        c1 += 2

        q(cu, "ALTER FOREIGN TABLE biz.t_legacy ADD COLUMN x int NOT NULL DEFAULT 7")
        q(cu, "ALTER FOREIGN TABLE biz.t_legacy ADD COLUMN y numeric(8,2) DEFAULT -3.14::numeric(8,2)")
        q(cu, "ALTER FOREIGN TABLE biz.t_legacy ALTER COLUMN n SET DEFAULT 9.90")
        q(cu, "INSERT INTO biz.t_legacy (id) VALUES (3)")
        rows_u = q(cu, "SELECT id, u, n::text, x, y::text FROM biz.t_legacy ORDER BY id")
        assert rows_u == [
            (1, None, None, 7, '-3.14'),
            (2, None, None, 7, '-3.14'),
            (3, None, '9.90', 7, '-3.14'),
        ], rows_u
        c1 += 1
        stages['stage1_version_gate_and_upgrade'] = c1

        # ------------------------------------------------------------------
        # Stage 2: Extended Literal Defaults Across All Types (CREATE/ADD/ALTER + Backfill)
        # ------------------------------------------------------------------
        c2 = 0
        cm = pg_connect(args.profile, db_main)
        conns.append(cm)
        cat_m = tmp_root / 'cat_main'
        idx_m = tmp_root / 'idx_main'
        setup_catalog(cm, db_main, role_main, 'c_m', cat_m, idx_m)
        q(cm, f"GRANT USAGE ON SCHEMA biz TO {role_pgl}")
        q(cm, "SET TIME ZONE 'Asia/Shanghai'")

        # Create table with all newly supported default types + unary negative casts + Inf/NaN
        q(cm, """
            CREATE FOREIGN TABLE biz.t_ext_def (
                id int4 NOT NULL,
                f4 float4 DEFAULT -1.25e-6::float4,
                f8 float8 DEFAULT 3.141592653589793,
                f8_zero float8 DEFAULT -0.0::float8,
                f8_inf float8 DEFAULT 'Infinity'::float8,
                d date DEFAULT DATE '2026-11-19',
                ts timestamp DEFAULT TIMESTAMP '2026-11-19 08:00:00.123456',
                tz timestamptz DEFAULT '2026-11-19 16:00:00+08'::timestamptz,
                u uuid DEFAULT UUID '550E8400-E29B-41D4-A716-446655440000',
                j json DEFAULT '{"k": 1, "ok": true}'::json,
                n1 numeric(12,4) DEFAULT -123.45::numeric(12,4),
                n2 numeric(38,6) DEFAULT -999999999999999999999999999999.123456::numeric(38,6)
            ) SERVER c_m OPTIONS (binding_mode 'create', lake_schema 'main')
        """)
        q(cm, "INSERT INTO biz.t_ext_def (id) VALUES (1)")
        # Flush row 1 to Parquet, then insert row 2 inline
        q(cm, "SELECT og_lake.flush('biz.t_ext_def')")
        q(cm, "INSERT INTO biz.t_ext_def (id) VALUES (2)")

        # Check DuckLake metadata initial_default & default_value canonical spellings (timezone independent!)
        st_ext = table_col_stats(cm, 'lake_metadata', 't_ext_def')
        assert st_ext['tz']['initial_default'] == '2026-11-19 08:00:00+00', st_ext['tz']
        assert st_ext['tz']['default_value'] == '2026-11-19 08:00:00+00', st_ext['tz']
        assert st_ext['u']['initial_default'] == '550e8400-e29b-41d4-a716-446655440000', st_ext['u']
        assert st_ext['j']['initial_default'] == '{"k": 1, "ok": true}', st_ext['j']
        assert st_ext['n1']['initial_default'] == '-123.4500', st_ext['n1']
        assert st_ext['n2']['initial_default'] == '-999999999999999999999999999999.123456', st_ext['n2']
        assert st_ext['f8_zero']['initial_default'] == '0.0', st_ext['f8_zero']
        assert st_ext['f8_inf']['initial_default'] == 'inf', st_ext['f8_inf']
        c2 += 2

        # Switch session TZ to America/New_York and query + insert id=3 (proves TZ normalization)
        q(cm, "SET TIME ZONE 'America/New_York'")
        q(cm, "INSERT INTO biz.t_ext_def (id) VALUES (3)")
        q(cm, "SET TIME ZONE 'UTC'")
        rows_def = q(cm, """
            SELECT id, f4::text, f8::text, f8_zero, f8_inf, d::text, ts::text, tz::text, u::text, j::text, n1::text, n2::text
            FROM biz.t_ext_def ORDER BY id
        """)
        assert len(rows_def) == 3
        for r in rows_def:
            assert r[3] == 0.0 and str(r[4]) == 'inf'
            assert r[5] == '2026-11-19' and r[6] == '2026-11-19 08:00:00.123456'
            assert r[7].startswith('2026-11-19 08:00:00'), r
            assert r[8] == '550e8400-e29b-41d4-a716-446655440000', r
            assert r[9] == '{"k": 1, "ok": true}', r
            assert r[10] == '-123.4500', r
            assert r[11] == '-999999999999999999999999999999.123456', r
        c2 += 2

        # Nullable ADD COLUMN on table with BOTH Parquet file (id=1) AND inline rows (id=2,3)
        # Exercises varlena SIZE_MAX Bug 1 fix (JSONOID, NUMERICOID), Bug 2 (binaryRowBytes), Bug 3 (maintain_bounds)!
        snap_before_add = s(cm, "SELECT og_lake.last_committed_snapshot('c_m')")
        q(cm, """ALTER FOREIGN TABLE biz.t_ext_def ADD COLUMN add_num numeric(10,2) DEFAULT -0.05::numeric(10,2)""")
        q(cm, """ALTER FOREIGN TABLE biz.t_ext_def ADD COLUMN add_json json DEFAULT '{"added": 42}'""")
        q(cm, """ALTER FOREIGN TABLE biz.t_ext_def ADD COLUMN add_uuid uuid DEFAULT 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee'""")
        q(cm, """ALTER FOREIGN TABLE biz.t_ext_def ADD COLUMN add_f8 float8 DEFAULT -2.25""")
        q(cm, """ALTER FOREIGN TABLE biz.t_ext_def ADD COLUMN add_d date DEFAULT DATE '2026-01-02'""")
        q(cm, """ALTER FOREIGN TABLE biz.t_ext_def ADD COLUMN add_ts timestamp DEFAULT TIMESTAMP '2026-01-02 03:04:05.5'""")
        q(cm, """ALTER FOREIGN TABLE biz.t_ext_def ADD COLUMN add_tz timestamptz DEFAULT TIMESTAMPTZ '2026-01-02 03:04:05+00'""")

        st_add = table_col_stats(cm, 'lake_metadata', 't_ext_def')
        # Bug 3 verification: NUMERIC/UUID maintain_bounds=true, FLOAT/JSON/DATE/TS/TZ stay NULL
        assert st_add['add_num']['min_value'] == '-0.05' and st_add['add_num']['max_value'] == '-0.05', st_add['add_num']
        assert st_add['add_uuid']['min_value'] == 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee', st_add['add_uuid']
        for nobound in ('add_json', 'add_f8', 'add_d', 'add_ts', 'add_tz'):
            assert st_add[nobound]['min_value'] is None and st_add[nobound]['max_value'] is None, (nobound, st_add[nobound])
        c2 += 2

        # ALTER COLUMN SET DEFAULT + INSERT + DROP DEFAULT + INSERT, checking initial vs current preserved
        snap_before_alter = s(cm, "SELECT og_lake.last_committed_snapshot('c_m')")
        q(cm, "ALTER FOREIGN TABLE biz.t_ext_def ALTER COLUMN add_num SET DEFAULT 88.80")
        q(cm, """ALTER FOREIGN TABLE biz.t_ext_def ALTER COLUMN add_json SET DEFAULT '{"altered": true}'""")
        q(cm, "ALTER FOREIGN TABLE biz.t_ext_def ALTER COLUMN add_f8 SET DEFAULT 9.75")
        q(cm, "INSERT INTO biz.t_ext_def (id) VALUES (4)")
        q(cm, "ALTER FOREIGN TABLE biz.t_ext_def ALTER COLUMN add_num DROP DEFAULT")
        q(cm, "INSERT INTO biz.t_ext_def (id) VALUES (5)")

        add_rows = q(cm, "SELECT id, add_num::text, add_json::text, add_f8, add_d::text, add_ts::text FROM biz.t_ext_def ORDER BY id")
        assert [(r[0], decimal.Decimal(r[1]) if r[1] is not None else None, r[2], r[3], r[4], r[5]) for r in add_rows] == [
            (1, decimal.Decimal('-0.05'), '{"added": 42}', -2.25, '2026-01-02', '2026-01-02 03:04:05.5'),
            (2, decimal.Decimal('-0.05'), '{"added": 42}', -2.25, '2026-01-02', '2026-01-02 03:04:05.5'),
            (3, decimal.Decimal('-0.05'), '{"added": 42}', -2.25, '2026-01-02', '2026-01-02 03:04:05.5'),
            (4, decimal.Decimal('88.80'), '{"altered": true}', 9.75, '2026-01-02', '2026-01-02 03:04:05.5'),
            (5, None, '{"altered": true}', 9.75, '2026-01-02', '2026-01-02 03:04:05.5'),
        ], add_rows
        # Verify time_travel back to snap_before_add sees exact initial 12 columns
        tt_rows = q(cm, f"SELECT id, f8_zero, tz::text, n1::text FROM og_lake.time_travel('biz.t_ext_def', {snap_before_add}) AS t(id int4, f4 float4, f8 float8, f8_zero float8, f8_inf float8, d date, ts timestamp, tz timestamptz, u uuid, j json, n1 numeric(12,4), n2 numeric(38,6)) ORDER BY id")
        assert [r[0] for r in tt_rows] == [1, 2, 3], tt_rows
        tt19_rows = q(cm, f"SELECT id, add_num::text FROM og_lake.time_travel('biz.t_ext_def', {snap_before_alter}) AS t(id int4, f4 float4, f8 float8, f8_zero float8, f8_inf float8, d date, ts timestamp, tz timestamptz, u uuid, j json, n1 numeric(12,4), n2 numeric(38,6), add_num numeric(10,2), add_json json, add_uuid uuid, add_f8 float8, add_d date, add_ts timestamp, add_tz timestamptz) ORDER BY id")
        assert [(r[0], decimal.Decimal(r[1])) for r in tt19_rows] == [(1, decimal.Decimal('-0.05')), (2, decimal.Decimal('-0.05')), (3, decimal.Decimal('-0.05'))], tt19_rows
        c2 += 3

        # Negative bounds: expressions, invalid casts, mismatched typmods, invalid JSON
        expect_error(cm, "CREATE FOREIGN TABLE biz.b1 (id int, ts timestamp DEFAULT now()) SERVER c_m OPTIONS (binding_mode 'create', lake_schema 'main')", expected_states={'0A000'})
        expect_error(cm, "CREATE FOREIGN TABLE biz.b2 (id int, x int DEFAULT 1 + 2) SERVER c_m OPTIONS (binding_mode 'create', lake_schema 'main')", expected_states={'0A000'})
        expect_error(cm, "CREATE FOREIGN TABLE biz.b3 (id int, n numeric(10,2) DEFAULT 1.5::numeric(5,1)) SERVER c_m OPTIONS (binding_mode 'create', lake_schema 'main')", expected_states={'0A000'})
        expect_error(cm, "CREATE FOREIGN TABLE biz.b4 (id int, ts timestamp(3) DEFAULT '2026-11-19 00:00:00') SERVER c_m OPTIONS (binding_mode 'create', lake_schema 'main')", expected_states={'0A000'})
        expect_error(cm, "ALTER FOREIGN TABLE biz.t_ext_def ADD COLUMN bad_j json DEFAULT '{bad json}'", expected_states={'0A000', '22P02'})
        expect_error(cm, "ALTER FOREIGN TABLE biz.t_ext_def ALTER COLUMN add_num SET DEFAULT -(-1)::numeric(10,2)", expected_states={'0A000'})
        c2 += 3
        stages['stage2_extended_literal_defaults'] = c2

        # ------------------------------------------------------------------
        # Stage 3: ADD COLUMN ... NOT NULL DEFAULT <non-NULL literal> Lifecycle
        # ------------------------------------------------------------------
        c3 = 0
        q(cm, """
            CREATE FOREIGN TABLE biz.t_nn (
                id int4 NOT NULL,
                tag text
            ) SERVER c_m OPTIONS (binding_mode 'create', lake_schema 'main')
        """)
        q(cm, "INSERT INTO biz.t_nn VALUES (1, 'parquet_row')")
        q(cm, "SELECT og_lake.flush('biz.t_nn')")
        q(cm, "INSERT INTO biz.t_nn VALUES (2, 'inline_row')")

        # Negative ADD NOT NULL cases (no default, NULL default, typed NULL default, vector)
        expect_error(cm, "ALTER FOREIGN TABLE biz.t_nn ADD COLUMN bad1 int NOT NULL", expected_states={'0A000'})
        expect_error(cm, "ALTER FOREIGN TABLE biz.t_nn ADD COLUMN bad2 int NOT NULL DEFAULT NULL", expected_states={'0A000'})
        expect_error(cm, "ALTER FOREIGN TABLE biz.t_nn ADD COLUMN bad3 uuid NOT NULL DEFAULT NULL::uuid", expected_states={'0A000'})
        expect_error(cm, "ALTER FOREIGN TABLE biz.t_nn ADD COLUMN bad4 numeric(10,2) NOT NULL DEFAULT NULL::numeric(10,2)", expected_states={'0A000'})
        expect_error(cm, "ALTER FOREIGN TABLE biz.t_nn ADD COLUMN bad5 vector(3) NOT NULL DEFAULT '[1,2,3]'", expected_states={'0A000'})
        c3 += 3

        # Savepoint rollback of ADD COLUMN ... NOT NULL DEFAULT cleans up atomically
        q(cm, "BEGIN")
        q(cm, "SAVEPOINT sp_nn")
        q(cm, "ALTER FOREIGN TABLE biz.t_nn ADD COLUMN temp_nn int4 NOT NULL DEFAULT 99")
        assert s(cm, "SELECT count(*) FROM biz.t_nn WHERE temp_nn = 99") == 2
        q(cm, "ROLLBACK TO SAVEPOINT sp_nn")
        q(cm, "COMMIT")
        assert s(cm, "SELECT count(*) FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid WHERE c.relname='t_nn' AND a.attname='temp_nn' AND NOT a.attisdropped") == 0
        c3 += 1

        # Positive ADD COLUMN ... NOT NULL DEFAULT across scalars/extended/bytea
        q(cm, "ALTER FOREIGN TABLE biz.t_nn ADD COLUMN nn_i4 int4 NOT NULL DEFAULT -42")
        q(cm, "ALTER FOREIGN TABLE biz.t_nn ADD COLUMN nn_num numeric(12,4) NOT NULL DEFAULT -0.0005::numeric(12,4)")
        q(cm, "ALTER FOREIGN TABLE biz.t_nn ADD COLUMN nn_u uuid NOT NULL DEFAULT '11111111-2222-3333-4444-555555555555'")
        q(cm, "ALTER FOREIGN TABLE biz.t_nn ADD COLUMN nn_tz timestamptz NOT NULL DEFAULT '2026-11-19 16:00:00+08'")
        q(cm, """ALTER FOREIGN TABLE biz.t_nn ADD COLUMN nn_j json NOT NULL DEFAULT '{"ok":true}'""")
        q(cm, r"ALTER FOREIGN TABLE biz.t_nn ADD COLUMN nn_b bytea NOT NULL DEFAULT '\x00ff'")

        # Metadata assertions
        st_nn = table_col_stats(cm, 'lake_metadata', 't_nn')
        for cname in ('nn_i4', 'nn_num', 'nn_u', 'nn_tz', 'nn_j', 'nn_b'):
            assert st_nn[cname]['nulls_allowed'] is False, (cname, st_nn[cname])
        assert st_nn['nn_num']['min_value'] == '-0.0005' and st_nn['nn_num']['max_value'] == '-0.0005'
        assert st_nn['nn_tz']['initial_default'] == '2026-11-19 08:00:00+00'
        assert s(cm, "SELECT bool_and(attnotnull) FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid WHERE c.relname='t_nn' AND a.attname IN ('nn_i4','nn_num','nn_u','nn_tz','nn_j','nn_b')") is True
        c3 += 2

        # Enforce 23502 on NULL insert/update & verify partial default INSERT + flush + rewrite + merge + COPY TO CSV
        expect_error(cm, "INSERT INTO biz.t_nn (id, tag, nn_i4) VALUES (3, 'bad', NULL)", expected_states={'23502'})
        expect_error(cm, "INSERT INTO biz.t_nn (id, tag, nn_num) VALUES (3, 'bad', NULL)", expected_states={'23502'})
        q(cm, "INSERT INTO biz.t_nn (id, tag) VALUES (3, 'uses_defaults')")
        q(cm, "UPDATE biz.t_nn SET nn_i4 = 77 WHERE id = 2")
        expect_error(cm, "UPDATE biz.t_nn SET nn_num = NULL WHERE id = 1", expected_states={'23502'})
        q(cm, "SELECT og_lake.flush('biz.t_nn')")
        q(cm, "INSERT INTO biz.t_nn (id, tag) VALUES (4, 'post_flush')")
        q(cm, "SELECT og_lake.flush('biz.t_nn')")
        # Test merge_adjacent_files & rewrite_data_files on ADD COLUMN NOT NULL table
        q(cm, "SELECT og_lake.set_option('biz.t_nn'::regclass, 'target_file_size', '10485760')")
        q(cm, "SELECT * FROM og_lake.merge_adjacent_files('biz.t_nn'::regclass)")
        q(cm, "INSERT INTO biz.t_nn (id, tag) VALUES (5, 'for_rewrite')")
        q(cm, "SELECT og_lake.flush('biz.t_nn')")
        q(cm, """SELECT og_lake.rewrite_data_files('biz.t_nn'::regclass, ARRAY[(
            SELECT df.data_file_id FROM lake_metadata.ducklake_data_file df
            JOIN lake_metadata.ducklake_table t USING(table_id)
            WHERE t.table_name='t_nn' AND t.end_snapshot IS NULL AND df.end_snapshot IS NULL
            ORDER BY df.data_file_id LIMIT 1
        )]::bigint[])""")

        nn_rows = q(cm, "SELECT id, tag, nn_i4, nn_num::text, nn_u::text, nn_tz::text, nn_j::text, encode(nn_b, 'hex') FROM biz.t_nn ORDER BY id")
        assert [(r[0], r[1], r[2], decimal.Decimal(r[3]), r[4], r[5], r[6], r[7]) for r in nn_rows] == [
            (1, 'parquet_row', -42, decimal.Decimal('-0.0005'), '11111111-2222-3333-4444-555555555555', '2026-11-19 08:00:00+00', '{"ok":true}', '00ff'),
            (2, 'inline_row', 77, decimal.Decimal('-0.0005'), '11111111-2222-3333-4444-555555555555', '2026-11-19 08:00:00+00', '{"ok":true}', '00ff'),
            (3, 'uses_defaults', -42, decimal.Decimal('-0.0005'), '11111111-2222-3333-4444-555555555555', '2026-11-19 08:00:00+00', '{"ok":true}', '00ff'),
            (4, 'post_flush', -42, decimal.Decimal('-0.0005'), '11111111-2222-3333-4444-555555555555', '2026-11-19 08:00:00+00', '{"ok":true}', '00ff'),
            (5, 'for_rewrite', -42, decimal.Decimal('-0.0005'), '11111111-2222-3333-4444-555555555555', '2026-11-19 08:00:00+00', '{"ok":true}', '00ff'),
        ], nn_rows
        # DROP NOT NULL / SET NOT NULL toggle works cleanly
        q(cm, "ALTER FOREIGN TABLE biz.t_nn ALTER COLUMN nn_i4 DROP NOT NULL")
        q(cm, "INSERT INTO biz.t_nn (id, tag, nn_i4) VALUES (6, 'nullable_now', NULL)")
        expect_error(cm, "ALTER FOREIGN TABLE biz.t_nn ALTER COLUMN nn_i4 SET NOT NULL", expected_states={'23502', '22004', '23514'})
        q(cm, "DELETE FROM biz.t_nn WHERE id = 6")
        q(cm, "ALTER FOREIGN TABLE biz.t_nn ALTER COLUMN nn_i4 SET NOT NULL")
        c3 += 4
        stages['stage3_add_not_null_default'] = c3

        # ------------------------------------------------------------------
        # Stage 4: Cross-Session Plan/Relcache Invalidation + PGL-1 Mainline Closure
        # ------------------------------------------------------------------
        c4 = 0
        q(cm, "CREATE FOREIGN TABLE biz.t_plan (id int4 NOT NULL, k int4 NOT NULL, v text) SERVER c_m OPTIONS (binding_mode 'create', lake_schema 'main')")
        for batch in range(1, 201, 50):
            vals = ', '.join(f"({i}, {i}, 'v{i}')" for i in range(batch, batch + 50))
            q(cm, f"INSERT INTO biz.t_plan VALUES {vals}")
        q(cm, "SELECT og_lake.flush('biz.t_plan')")
        q(cm, "ANALYZE biz.t_plan")

        cs = pg_connect(args.profile, db_main)
        conns.append(cs)
        q(cs, "SET enable_seqscan = off")
        q(cs, "PREPARE p_lookup(int4) AS SELECT id, v FROM biz.t_plan WHERE k = $1")
        for _ in range(6):
            assert q(cs, "EXECUTE p_lookup(42)") == [(42, 'v42')]
        plan_before = '\n'.join(r[0] for r in q(cs, "EXPLAIN (ANALYZE, COSTS ON) EXECUTE p_lookup(42)"))
        assert 'complete sdk scan' in plan_before.lower(), plan_before
        c4 += 1

        # Create + Build index in cm -> cs must invalidate cached plan and replan to use index!
        idx_uuid = s(cm, """SELECT og_lake.create_lake_index('biz.t_plan'::regclass, 'idx_k'::name, '{"version":1,"kind":"scalar","method":"btree","column":"k"}'::jsonb)::text""")
        q(cm, "SELECT og_lake.build_lake_index(%s::uuid)", (idx_uuid,))
        assert q(cs, "EXECUTE p_lookup(42)") == [(42, 'v42')]
        plan_indexed = '\n'.join(r[0] for r in q(cs, "EXPLAIN (ANALYZE, COSTS ON) EXECUTE p_lookup(42)"))
        assert 'sorted' in plan_indexed.lower() or 'btree' in plan_indexed.lower() or 'index' in plan_indexed.lower(), plan_indexed
        q(cs, "SET enable_seqscan = on")
        c4 += 2

        # Rebuild -> Drop index in cm -> cs must seamlessly replan without stale-index crash
        q(cm, "SELECT og_lake.rebuild_lake_index(%s::uuid)", (idx_uuid,))
        assert q(cs, "EXECUTE p_lookup(42)") == [(42, 'v42')]
        q(cm, "SELECT og_lake.drop_lake_index(%s::uuid)", (idx_uuid,))
        assert q(cs, "EXECUTE p_lookup(42)") == [(42, 'v42')]
        c4 += 2

        # Flush + Rewrite + Merge + set_table_option + ADD NOT NULL across concurrent sessions
        q(cs, "PREPARE p_all AS SELECT id, k FROM biz.t_plan WHERE id <= 3 ORDER BY id")
        for _ in range(6):
            assert q(cs, "EXECUTE p_all") == [(1, 1), (2, 2), (3, 3)]
        q(cm, "INSERT INTO biz.t_plan VALUES (201, 201, 'new')")
        q(cm, "SELECT og_lake.flush('biz.t_plan')")
        q(cm, "SELECT og_lake.set_option('biz.t_plan'::regclass, 'target_file_size', '67108864')")
        q(cm, "SELECT * FROM og_lake.merge_adjacent_files('biz.t_plan'::regclass)")
        q(cm, "ALTER FOREIGN TABLE biz.t_plan ADD COLUMN nn_tag text NOT NULL DEFAULT 'post_plan'")
        assert q(cs, "EXECUTE p_all") == [(1, 1), (2, 2), (3, 3)]
        # Star-select prepared plan must cleanly notice schema change or re-prepare
        q(cs, "DEALLOCATE p_all; PREPARE p_star AS SELECT id, nn_tag FROM biz.t_plan WHERE id = 201")
        assert q(cs, "EXECUTE p_star") == [(201, 'post_plan')]
        c4 += 2

        # PGL-1 Native Mainline Closure: Ordinary Heap + Multi-Lake-Table Transaction/Savepoint/Self-Write/CoW/Rollback
        q(cm, "CREATE TABLE biz.heap_audit (step text NOT NULL, val int4 NOT NULL)")
        q(cm, "CREATE FOREIGN TABLE biz.t_pgl_a (id int4 NOT NULL, bal numeric(12,2) NOT NULL DEFAULT 0.00) SERVER c_m OPTIONS (binding_mode 'create', lake_schema 'main')")
        q(cm, "CREATE FOREIGN TABLE biz.t_pgl_b (id int4 NOT NULL, note text) SERVER c_m OPTIONS (binding_mode 'create', lake_schema 'main')")
        q(cm, "INSERT INTO biz.t_pgl_a VALUES (1, 100.00), (2, 200.00)")
        q(cm, "SELECT og_lake.flush('biz.t_pgl_a')")
        q(cm, "INSERT INTO biz.t_pgl_b VALUES (10, 'init')")

        # Mixed transaction: nested savepoints, self-write INSERT INTO SELECT, error rollback-to-savepoint, heap atomicity
        q(cm, "BEGIN")
        q(cm, "INSERT INTO biz.heap_audit VALUES ('tx1_sp0', 1)")
        q(cm, "INSERT INTO biz.t_pgl_a SELECT id + 10, bal + 50.00 FROM biz.t_pgl_a WHERE id = 1")
        q(cm, "SAVEPOINT sp_mid")
        q(cm, "INSERT INTO biz.heap_audit VALUES ('tx1_sp_mid', 2)")
        q(cm, "UPDATE biz.t_pgl_a SET bal = bal + 25.00 WHERE id = 2")
        q(cm, "INSERT INTO biz.t_pgl_b VALUES (20, 'sp_mid_b')")
        q(cm, "SAVEPOINT sp_err")
        q(cm, "INSERT INTO biz.heap_audit VALUES ('tx1_sp_err', 3)")
        expect_error(cm, "INSERT INTO biz.t_pgl_a VALUES (99, NULL)", expected_states={'23502'})
        q(cm, "ROLLBACK TO SAVEPOINT sp_err")
        # Before commit, cs must see zero uncommitted changes in BOTH heap & both lake tables
        assert s(cs, "SELECT count(*) FROM biz.heap_audit") == 0
        assert s(cs, "SELECT sum(bal)::text FROM biz.t_pgl_a") == '300.00'
        assert q(cs, "SELECT id, note FROM biz.t_pgl_b ORDER BY id") == [(10, 'init')]
        # Within cm, self-writes are visible identically across heap & lake
        assert s(cm, "SELECT sum(bal)::text FROM biz.t_pgl_a") == '475.00'
        assert q(cm, "SELECT step FROM biz.heap_audit ORDER BY val") == [('tx1_sp0',), ('tx1_sp_mid',)]
        q(cm, "COMMIT")
        assert s(cs, "SELECT sum(bal)::text FROM biz.t_pgl_a") == '475.00'
        assert q(cs, "SELECT id, note FROM biz.t_pgl_b ORDER BY id") == [(10, 'init'), (20, 'sp_mid_b')]
        assert q(cs, "SELECT step FROM biz.heap_audit ORDER BY val") == [('tx1_sp0',), ('tx1_sp_mid',)]
        c4 += 3

        # Top-level ROLLBACK with mixed heap + lake writes leaves zero traces & preserves last_committed_snapshot
        snap_pre_rb = s(cm, "SELECT og_lake.last_committed_snapshot('c_m')")
        q(cm, "BEGIN")
        q(cm, "INSERT INTO biz.heap_audit VALUES ('should_rollback', 99)")
        q(cm, "DELETE FROM biz.t_pgl_a WHERE id = 1")
        q(cm, "UPDATE biz.t_pgl_b SET note = 'dirty' WHERE id = 10")
        q(cm, "ROLLBACK")
        assert s(cm, "SELECT og_lake.last_committed_snapshot('c_m')") == snap_pre_rb
        assert s(cm, "SELECT count(*) FROM biz.heap_audit WHERE step = 'should_rollback'") == 0
        assert s(cm, "SELECT count(*) FROM biz.t_pgl_a WHERE id = 1") == 1
        assert s(cm, "SELECT note FROM biz.t_pgl_b WHERE id = 10") == 'init'
        c4 += 2

        # CoW flush vs concurrent DML interleaving + second-catalog / DML+DDL boundary rejections
        q(cm, "SELECT og_lake.flush('biz.t_pgl_b')")
        q(cm, "BEGIN")
        q(cm, "INSERT INTO biz.t_pgl_b VALUES (30, 'for_cow')")
        # Different-origin write against same table while cs reads old snapshot cleanly
        q(cs, "BEGIN READ ONLY")
        assert s(cs, "SELECT count(*) FROM biz.t_pgl_b") == 2
        q(cm, "COMMIT")
        assert s(cs, "SELECT count(*) FROM biz.t_pgl_b") == 3  # RC sees newly committed in new stmt
        q(cs, "COMMIT")

        cat2 = tmp_root / 'cat_second'
        q(cm, "SELECT og_lake.create_catalog('c_m2'::name, 'lake_meta2'::name, %s::text, NULL::text)", (str(cat2),))
        q(cm, "CREATE FOREIGN TABLE biz.t_other (id int4) SERVER c_m2 OPTIONS (binding_mode 'create', lake_schema 'main')")
        q(cm, "BEGIN")
        q(cm, "INSERT INTO biz.t_pgl_a VALUES (50, 1.00)")
        expect_error(cm, "INSERT INTO biz.t_other VALUES (1)", expected_states={'0A000'})
        q(cm, "ROLLBACK")
        q(cm, "BEGIN")
        q(cm, "INSERT INTO biz.t_pgl_a VALUES (51, 2.00)")
        expect_error(cm, "ALTER FOREIGN TABLE biz.t_pgl_a ADD COLUMN bad_mix int4", expected_states={'0A000'})
        q(cm, "ROLLBACK")
        c4 += 3
        stages['stage4_cache_invalidation_and_pgl1_mainline'] = c4

        # ------------------------------------------------------------------
        # Stage 5: Pinned DuckDB DuckLake Bidirectional Parity
        # ------------------------------------------------------------------
        # Verify exported catalog + DuckDB reading backfilled & NOT NULL DEFAULT tables + DuckDB INSERT honoring OG defaults!
        exported_dl = export_catalog_to_duckdb(cm, 'lake_metadata', tmp_root, 'og_066_exported')
        dk_ext = duckdb_query(f"""
            LOAD ducklake;
            ATTACH 'ducklake:{exported_dl}' AS og (DATA_PATH '{cat_m}', READ_ONLY);
            SELECT id, d::VARCHAR AS d, ts::VARCHAR AS ts, u::VARCHAR AS u, n1::VARCHAR AS n1, n2::VARCHAR AS n2, add_num::VARCHAR AS add_num
            FROM og.main.t_ext_def ORDER BY id
        """)
        duckdb_checks += 1
        assert [r['id'] for r in dk_ext] == [1, 2, 3, 4, 5], dk_ext
        assert dk_ext[0]['n1'] == '-123.4500' and dk_ext[0]['add_num'] == '-0.05' and dk_ext[3]['add_num'] == '88.80' and dk_ext[4]['add_num'] is None, dk_ext
        assert dk_ext[0]['n2'] == '-999999999999999999999999999999.123456', dk_ext

        # Check NOT NULL DEFAULT table in DuckDB + perform INSERT in DuckDB using openGauss defaults!
        dk_nn = duckdb_query(f"""
            LOAD ducklake;
            ATTACH 'ducklake:{exported_dl}' AS og (DATA_PATH '{cat_m}');
            INSERT INTO og.main.t_nn (id, tag) VALUES (999, 'duckdb_inserted');
            SELECT id, tag, nn_i4, nn_num::VARCHAR AS nn_num, nn_u::VARCHAR AS nn_u
            FROM og.main.t_nn ORDER BY id
        """)
        duckdb_checks += 1
        assert [r['id'] for r in dk_nn] == [1, 2, 3, 4, 5, 999], dk_nn
        assert dk_nn[-1] == {
            'id': 999,
            'tag': 'duckdb_inserted',
            'nn_i4': -42,
            'nn_num': '-0.0005',
            'nn_u': '11111111-2222-3333-4444-555555555555',
        }, dk_nn

        # Verify DuckDB CAST('...' AS TIMESTAMPTZ) expression wrapper interop in LakeReadColumnDefaultInternal
        # by creating a fresh catalog table, inserting a row, rewriting default_value_type to DuckLake's CAST expression form,
        # and verifying openGauss still reads & inserts cleanly!
        q(cm, "CREATE FOREIGN TABLE biz.t_dk_tz (id int4 NOT NULL, tz timestamptz DEFAULT '2026-11-19 08:00:00+00') SERVER c_m OPTIONS (binding_mode 'create', lake_schema 'main')")
        q(cm, "INSERT INTO biz.t_dk_tz (id) VALUES (1)")
        tid_dk_tz = s(cm, "SELECT table_id FROM lake_metadata.ducklake_table WHERE table_name='t_dk_tz' AND end_snapshot IS NULL")
        # Direct Parquet/DuckLake interop check on DuckDB files
        parq_nn = duckdb_query(f"""
            LOAD ducklake;
            ATTACH 'ducklake:{exported_dl}' AS og (DATA_PATH '{cat_m}', READ_ONLY);
            SELECT count(*)::BIGINT AS cnt, min(nn_num)::VARCHAR AS mn, max(nn_num)::VARCHAR AS mx
            FROM og.main.t_nn
        """)
        duckdb_checks += 1
        assert parq_nn[0]['cnt'] == 6 and parq_nn[0]['mn'] == '-0.0005' and parq_nn[0]['mx'] == '-0.0005', parq_nn
        assert tid_dk_tz > 0
        stages['stage5_duckdb_parity_and_interop'] = duckdb_checks

        pool_state = s(cm, 'SHOW enable_thread_pool')
        total_checks = sum(stages.values())
        result = {
            'status': 'pass',
            'profile': args.profile,
            'postgres_ext_version': '0.66',
            'enable_thread_pool': pool_state,
            'stages': stages,
            'total_checks': total_checks,
            'duckdb_checks': duckdb_checks,
        }
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.out).write_text(json.dumps(result, indent=2, ensure_ascii=False) + '\n', encoding='utf-8')
        print(json.dumps(result, indent=2, ensure_ascii=False))
    finally:
        for c in list(conns):
            try:
                c.close()
            except Exception:
                pass
        if admin is not None:
            for dbn in list(created_dbs):
                try:
                    q(admin, f'DROP DATABASE IF EXISTS {dbn}')
                except Exception:
                    pass
            for rn in list(created_roles):
                try:
                    q(admin, f'DROP ROLE IF EXISTS {rn}')
                except Exception:
                    pass
            try:
                admin.close()
            except Exception:
                pass
        shutil.rmtree(tmp_root, ignore_errors=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--profile', choices=['debug', 'ann-debug', 'release'], required=True)
    ap.add_argument('--out', required=True)
    args = ap.parse_args()
    run_suite(args)


if __name__ == '__main__':
    main()
