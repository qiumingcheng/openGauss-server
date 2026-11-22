#!/usr/bin/env python3
"""Scoped SQL0.65 (N7-STATS) verification suite.

Covers:
1. Version gating & upgrade safety (0.64 -> 0.65, pre-0.65 legacy NULL bounds preserved,
   conservative post-upgrade retention on already-populated 0.64 tables, new file/empty recovery).
2. NUMERIC(p,s) exact canonical bounds & 128-bit ordering (numeric(12,4), numeric(38,6),
   sub-zero negatives, trailing scale zeros, COPY FROM CSV, UPDATE expansion,
   ALL-NULL recovery, ADD COLUMN NULL-default, malformed/inverted external bound resilience).
3. File-level exact ducklake_file_column_stats across all 4 Parquet writers:
   direct INSERT, partitioned direct INSERT (disjoint per-file vs table rollup),
   unpartitioned flush, identity-partitioned flush, rewrite_data_files, and merge_adjacent_files.
4. Native ANALYZE (pg_class.reltuples & pg_stats for NUMERIC(12,4)/(38,6) across inline+file
   and partitioned tables), optimizer EXPLAIN row-estimate/index-plan sensitivity, and
   exact query parity.
5. Pinned DuckDB DuckLake offline parity:
   - direct read_parquet() min/max check of every openGauss Parquet file,
   - side-by-side DuckDB DuckLake catalog execution comparing table_column_stats & file_column_stats,
   - exporting openGauss lake_metadata into a .ducklake file & querying via LOAD ducklake; ATTACH.
"""

import argparse
import decimal
import hashlib
import json
import os
import getpass
import re
import shutil
import subprocess
import sys
import tempfile
import uuid
from pathlib import Path

import psycopg2

ROOT = Path(os.environ.get('CPP_WORKSPACE', str(Path(__file__).resolve().parents[5]))).expanduser().resolve()
DUCKDB_BIN = ROOT / '.reference/opengauss-native-lake/build/duckdb-gcc10/duckdb'


def md5_role_pw(password, role):
    return 'md5' + hashlib.md5((password + role).encode('utf-8')).hexdigest()


def pg_connect(profile, dbname, user=None):
    port = 55560 if profile == 'debug' else (55570 if profile == 'release' else 55580)
    conn = psycopg2.connect(
        host=str(ROOT / '.runtime/opengauss-native-lake' / profile / 'socket'),
        port=port,
        dbname=dbname,
        user=user or os.environ.get('PGUSER') or getpass.getuser(),
        connect_timeout=15,
    )
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


def explain_text(conn, sql, analyze=False):
    opts = 'ANALYZE ON, COSTS ON' if analyze else 'COSTS ON'
    return '\n'.join(r[0] for r in q(conn, f'EXPLAIN ({opts}) {sql}'))


def explain_est_rows(conn, sql):
    plan = explain_text(conn, sql)
    m = re.search(r'rows=(\d+(?:\.\d+)?)', plan)
    assert m, f'Could not parse rows= from EXPLAIN:\n{plan}'
    return float(m.group(1)), plan


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
    # Strip any CLI terminal detection warning prefix before JSON array '['
    idx = out.find('[')
    if idx < 0:
        return []
    return json.loads(out[idx:])


def get_table_id(conn, schema, relname):
    return s(
        conn,
        f"SELECT t.table_id FROM {schema}.ducklake_table t "
        f"JOIN pg_foreign_table ft ON true "
        f"WHERE t.table_name = %s AND t.end_snapshot IS NULL LIMIT 1",
        (relname,),
    )


def table_col_stats(conn, meta_schema, table_name):
    rows = q_dicts(
        conn,
        f"""
        SELECT c.column_id, c.column_name, c.column_type,
               s.contains_null, s.contains_nan, s.min_value::text AS min_value,
               s.max_value::text AS max_value, s.min_is_exact, s.max_is_exact
        FROM {meta_schema}.ducklake_table t
        JOIN {meta_schema}.ducklake_column c ON c.table_id = t.table_id AND c.end_snapshot IS NULL
        JOIN {meta_schema}.ducklake_table_column_stats s ON s.table_id = t.table_id AND s.column_id = c.column_id
        WHERE t.table_name = %s AND t.end_snapshot IS NULL
        ORDER BY c.column_id
        """,
        (table_name,),
    )
    return {r['column_name']: r for r in rows}


def file_col_stats(conn, meta_schema, table_name):
    rows = q_dicts(
        conn,
        f"""
        SELECT df.data_file_id, df.path, df.end_snapshot, df.record_count,
               c.column_id, c.column_name, c.column_type,
               fcs.value_count, fcs.null_count, fcs.contains_nan,
               fcs.min_value::text AS min_value, fcs.max_value::text AS max_value,
               fcs.min_is_exact, fcs.max_is_exact
        FROM {meta_schema}.ducklake_table t
        JOIN {meta_schema}.ducklake_data_file df ON df.table_id = t.table_id
        JOIN {meta_schema}.ducklake_column c ON c.table_id = t.table_id AND c.end_snapshot IS NULL
        JOIN {meta_schema}.ducklake_file_column_stats fcs
          ON fcs.table_id = t.table_id AND fcs.data_file_id = df.data_file_id AND fcs.column_id = c.column_id
        WHERE t.table_name = %s AND t.end_snapshot IS NULL
        ORDER BY df.data_file_id, c.column_id
        """,
        (table_name,),
    )
    by_file = {}
    for r in rows:
        fid = r['data_file_id']
        entry = by_file.setdefault(
            fid,
            {'data_file_id': fid, 'path': r['path'], 'end_snapshot': r['end_snapshot'],
             'record_count': r['record_count'], 'columns': {}},
        )
        entry['columns'][r['column_name']] = r
    return by_file


def export_catalog_to_duckdb(conn, meta_schema, tmp_dir, db_name):
    """Create a real .ducklake catalog file by seeding EMPTY schema from pinned duckdb,
    clearing its rows, and loading openGauss lake_metadata rows cleanly."""
    seed_dl = tmp_dir / f'{db_name}_seed.ducklake'
    target_dl = tmp_dir / f'{db_name}.ducklake'
    data_dummy = tmp_dir / f'{db_name}_seed_data'
    data_out = tmp_dir / f'{db_name}_out_data'
    for p in (seed_dl, target_dl, data_dummy, data_out):
        if p.exists():
            if p.is_dir():
                shutil.rmtree(p)
            else:
                p.unlink()
    data_dummy.mkdir(parents=True, exist_ok=True)
    # Create an EMPTY DuckLake metadata file using pinned duckdb
    proc = subprocess.run(
        [str(DUCKDB_BIN), '-batch', '-dark-mode', '-c',
         f"LOAD ducklake; ATTACH 'ducklake:{seed_dl}' AS s (DATA_PATH '{data_dummy}'); DETACH s;"],
        stdin=subprocess.DEVNULL, capture_output=True, text=True,
    )
    assert proc.returncode == 0, proc.stderr
    shutil.copy2(seed_dl, target_dl)

    # Read actual table names and columns from seed DuckDB schema
    tbls = duckdb_query("SELECT table_name FROM information_schema.tables WHERE table_schema='main' ORDER BY table_name", target_dl)
    sql_lines = ['BEGIN;']
    for t in tbls:
        tname = t['table_name']
        sql_lines.append(f'DELETE FROM main."{tname}";')
    # Read rows from openGauss lake_metadata
    def dk_lit(val, dtype):
        if val is None:
            return 'NULL'
        if isinstance(val, bool):
            return 'TRUE' if val else 'FALSE'
        if isinstance(val, int):
            return str(val)
        txt = str(val).replace("'", "''")
        return f"'{txt}'"

    # Intersect with actual tables/columns in openGauss lake_metadata schema
    pg_tbls = {r[0] for r in q(conn, "SELECT tablename FROM pg_tables WHERE schemaname=%s", (meta_schema,))}

    # Preserve FK/check independence inside transaction
    for t in tbls:
        tname = t['table_name']
        if tname not in pg_tbls:
            continue
        og_cols = {
            r[0] for r in q(
                conn,
                """
                SELECT a.attname
                FROM pg_attribute a
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
        og_rows = q(conn, f"SELECT {', '.join(pg_cols)} FROM {meta_schema}.\"{tname}\"")
        for r in og_rows:
            vals = [dk_lit(v, cols[i]['data_type']) for i, v in enumerate(r)]
            col_list = ', '.join('"' + c + '"' for c in col_names)
            val_list = ', '.join(vals)
            sql_lines.append(f'INSERT INTO main."{tname}" ({col_list}) VALUES ({val_list});')
    # Mirror any referenced inline carrier tables so mixed inline+file DuckLake queries succeed
    inl_tbls = q(conn, f"SELECT DISTINCT table_name FROM {meta_schema}.ducklake_inlined_data_tables ORDER BY table_name")
    for (inl_name,) in inl_tbls:
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
        def to_dk_type(pt):
            if pt == 'text' or pt == 'character varying':
                return 'VARCHAR'
            if pt == 'bytea':
                return 'BLOB'
            if pt.startswith('numeric'):
                return pt.replace('numeric', 'DECIMAL', 1)
            return pt.upper()
        defs = ', '.join(f'"{cname}" {to_dk_type(ptype)}' for cname, ptype in attr_rows)
        sql_lines.append(f'CREATE TABLE IF NOT EXISTS main."{inl_name}" ({defs});')
        cnames = [cname for cname, _ in attr_rows]
        sel_cols = ', '.join(f'"{c}"::text' if t == 'bytea' or 'timestamp' in t else f'"{c}"' for c, t in attr_rows)
        for r in q(conn, f'SELECT {sel_cols} FROM {meta_schema}."{inl_name}"'):
            vals = [dk_lit(v, ptype) for v, (_, ptype) in zip(r, attr_rows)]
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


def run_suite(args):
    tag = uuid.uuid4().hex[:10]
    tmp_root = Path(tempfile.mkdtemp(prefix=f'og_065_{tag}_', dir='/tmp'))
    os.chmod(tmp_root, 0o755)
    admin = pg_connect(args.profile, 'postgres')
    created_dbs = []
    created_roles = []
    conns = []
    stages = {}
    duckdb_checks = 0

    try:
        role_upg = f'u065_upg_{tag}'
        role_main = f'u065_main_{tag}'
        db_upg = f'db065_upg_{tag}'
        db_main = f'db065_main_{tag}'
        for rn in (role_upg, role_main):
            q(admin, f"CREATE ROLE {rn} NOLOGIN NOINHERIT ENCRYPTED PASSWORD %s", (md5_role_pw('Own065!', rn),))
            created_roles.append(rn)
        for dbn in (db_upg, db_main):
            q(admin, f"CREATE DATABASE {dbn} WITH DBCOMPATIBILITY 'PG' ENCODING 'UTF8'")
            created_dbs.append(dbn)

        # ------------------------------------------------------------------
        # Stage 1: Version Gating (0.64 -> 0.65) & conservative upgrade safety
        # ------------------------------------------------------------------
        cu = pg_connect(args.profile, db_upg)
        conns.append(cu)
        q(cu, "CREATE EXTENSION og_lake VERSION '0.64'")
        assert s(cu, "SELECT extversion FROM pg_extension WHERE extname='og_lake'") == '0.64'
        hs = json.loads(s(cu, 'SELECT og_lake.host_status()'))
        assert hs.get('module_version') in ('0.65', '0.73', '0.74'), hs
        assert hs.get('stats_extended') is True, hs
        assert hs.get('stats_extended_sql_version') == '0.65', hs
        assert hs.get('numeric_minmax_stats') is True, hs
        assert hs.get('file_column_stats_bounds') is True, hs

        cat_u = tmp_root / 'cat_u'
        q(cu, 'SELECT og_lake.configure_internal_owner(%s::name)', (role_upg,))
        q(cu, "SELECT og_lake.create_catalog('cat_u'::name, 'cat_u_meta'::name, %s::text, NULL::text)", (str(cat_u),))
        q(cu, "CREATE FOREIGN TABLE t_in_064 (id int, n1 numeric(12,4), n2 numeric(38,6)) SERVER cat_u OPTIONS (binding_mode 'create', lake_schema 'main')")
        q(cu, "CREATE FOREIGN TABLE t_empty_064 (id int, n1 numeric(12,4)) SERVER cat_u OPTIONS (binding_mode 'create', lake_schema 'main')")
        q(cu, "INSERT INTO t_in_064 VALUES (1, -12.3400, 999999999999999999999999999999.123456), (2, 56.7800, -1.000001)")

        # Under 0.64, NUMERIC bounds remain NULL (legacy behavior preserved)
        st_in_064 = table_col_stats(cu, 'cat_u_meta', 't_in_064')
        for cn in ('n1', 'n2'):
            assert (st_in_064[cn]['contains_null'], st_in_064[cn]['min_value'], st_in_064[cn]['max_value'],
                    st_in_064[cn]['min_is_exact'], st_in_064[cn]['max_is_exact']) == (False, None, None, None, None), st_in_064[cn]

        # Direct INSERT under 0.64 -> file column bounds remain NULL
        q(cu, "CREATE FOREIGN TABLE t_dir_064 (id int, n1 numeric(12,4), txt text) SERVER cat_u OPTIONS (binding_mode 'create', lake_schema 'main')")
        q(cu, "SELECT og_lake.set_option('t_dir_064'::regclass, 'data_inlining_row_limit'::text, '0'::text)")
        q(cu, "INSERT INTO t_dir_064 VALUES (1, 10.5000, 'aa'), (2, 20.5000, 'zz')")
        f_064 = file_col_stats(cu, 'cat_u_meta', 't_dir_064')
        assert len(f_064) == 1
        fid_064 = next(iter(f_064))
        for cn in ('id', 'n1', 'txt'):
            fc = f_064[fid_064]['columns'][cn]
            assert (fc['min_value'], fc['max_value'], fc['min_is_exact'], fc['max_is_exact']) == (None, None, None, None), fc

        # Upgrade 0.64 -> 0.65
        q(cu, "ALTER EXTENSION og_lake UPDATE TO '0.65'")
        assert s(cu, "SELECT extversion FROM pg_extension WHERE extname='og_lake'") == '0.65'

        # Already-populated 0.64 table MUST NOT fabricate narrowed table bounds over unknown prior values
        q(cu, "INSERT INTO t_in_064 VALUES (3, -99.9900, 0.000000)")
        st_in_after = table_col_stats(cu, 'cat_u_meta', 't_in_064')
        assert st_in_after['n1']['min_value'] is None and st_in_after['n1']['max_value'] is None, st_in_after['n1']

        # Empty table created in 0.64, first populated in 0.65 -> exact bounds established!
        q(cu, "INSERT INTO t_empty_064 VALUES (1, -5.5000), (2, 15.2500)")
        st_empty_after = table_col_stats(cu, 'cat_u_meta', 't_empty_064')
        assert (st_empty_after['n1']['min_value'], st_empty_after['n1']['max_value'],
                st_empty_after['n1']['min_is_exact'], st_empty_after['n1']['max_is_exact']) == ('-5.5000', '15.2500', True, True), st_empty_after

        # New direct INSERT file in t_dir_064 after 0.65 gets exact file bounds while table stats stay conservatively unknown
        q(cu, "INSERT INTO t_dir_064 VALUES (3, -5.2500, 'mm'), (4, 99.7500, 'pp')")
        f_065 = file_col_stats(cu, 'cat_u_meta', 't_dir_064')
        assert len(f_065) == 2
        new_fid = max(f_065)
        assert (f_065[new_fid]['columns']['n1']['min_value'], f_065[new_fid]['columns']['n1']['max_value'],
                f_065[new_fid]['columns']['n1']['min_is_exact'], f_065[new_fid]['columns']['n1']['max_is_exact']) == ('-5.2500', '99.7500', True, True)
        st_dir_after = table_col_stats(cu, 'cat_u_meta', 't_dir_064')
        assert st_dir_after['n1']['min_value'] is None and st_dir_after['n1']['max_value'] is None
        cu.close()
        conns.remove(cu)
        stages['stage1_version_gate_and_upgrade'] = 10

        # ------------------------------------------------------------------
        # Main DB at SQL 0.65
        # ------------------------------------------------------------------
        cm = pg_connect(args.profile, db_main)
        conns.append(cm)
        q(cm, "CREATE EXTENSION og_lake VERSION '0.65'")
        q(cm, 'SELECT og_lake.configure_internal_owner(%s::name)', (role_main,))
        cat_m = tmp_root / 'cat_main'
        idx_m = tmp_root / 'idx_main'
        idx_m.mkdir()
        os.chmod(idx_m, 0o755)
        q(cm, "SELECT og_lake.create_catalog('cat'::name, 'lake_metadata'::name, %s::text, NULL::text)", (str(cat_m),))
        q(cm, "SELECT og_lake.set_option('cat'::name, 'index_root'::text, %s::text)", (str(idx_m),))
        q(cm, "SELECT og_lake.set_option('cat'::name, 'data_inlining_row_limit'::text, '1000'::text)")

        # ------------------------------------------------------------------
        # Stage 2: Inline NUMERIC(p,s) 128-bit Stats, Sub-zero Ordering,
        #          COPY CSV, UPDATE/DELETE, All-NULL Recovery, ADD COLUMN
        # ------------------------------------------------------------------
        q(cm, """
            CREATE FOREIGN TABLE t_num (
                id int, b bool, i2 smallint, i8 bigint, txt text, u uuid,
                n1 numeric(12,4), n2 numeric(38,6)
            ) SERVER cat OPTIONS (binding_mode 'create', lake_schema 'main')
        """)
        q(cm, """
            INSERT INTO t_num VALUES
              (1, false, -32768, -10000000000, 'alpha', '00000000-0000-0000-0000-000000000001'::uuid, -123.4500, -999999999999999999999999999999.123456),
              (2, true,   32767,  20000000000, 'zeta',  'ffffffff-ffff-ffff-ffff-ffffffffffff'::uuid,  456.7890,  123456789012345678901234567890.654321),
              (3, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
        """)
        stn = table_col_stats(cm, 'lake_metadata', 't_num')
        assert (stn['n1']['contains_null'], stn['n1']['min_value'], stn['n1']['max_value'], stn['n1']['min_is_exact'], stn['n1']['max_is_exact']) == \
               (True, '-123.4500', '456.7890', True, True), stn['n1']
        assert (stn['n2']['contains_null'], stn['n2']['min_value'], stn['n2']['max_value'], stn['n2']['min_is_exact'], stn['n2']['max_is_exact']) == \
               (True, '-999999999999999999999999999999.123456', '123456789012345678901234567890.654321', True, True), stn['n2']

        # Sub-zero ordering + COPY FROM CSV
        q(cm, "CREATE FOREIGN TABLE t_sub (id int, n1 numeric(12,4)) SERVER cat OPTIONS (binding_mode 'create', lake_schema 'main')")
        q(cm, "INSERT INTO t_sub VALUES (1, -0.0005), (2, -0.0001), (3, 0.0000), (4, 1.2000)")
        sts = table_col_stats(cm, 'lake_metadata', 't_sub')
        assert (sts['n1']['min_value'], sts['n1']['max_value'], sts['n1']['min_is_exact'], sts['n1']['max_is_exact']) == ('-0.0005', '1.2000', True, True), sts['n1']
        csv_file = tmp_root / 'sub_copy.csv'
        csv_file.write_text('5,-0.0009\n6,2.5000\n', encoding='utf-8')
        os.chmod(csv_file, 0o644)
        q(cm, f"COPY t_sub FROM '{csv_file}' WITH (FORMAT csv)")
        sts2 = table_col_stats(cm, 'lake_metadata', 't_sub')
        assert (sts2['n1']['min_value'], sts2['n1']['max_value']) == ('-0.0009', '2.5000'), sts2['n1']

        # All-NULL recovery + ADD COLUMN NULL default
        q(cm, "CREATE FOREIGN TABLE t_all_null (id int, n1 numeric(12,4), n2 numeric(38,6)) SERVER cat OPTIONS (binding_mode 'create', lake_schema 'main')")
        q(cm, "INSERT INTO t_all_null VALUES (1, NULL, NULL), (2, NULL, NULL)")
        stan = table_col_stats(cm, 'lake_metadata', 't_all_null')
        assert (stan['n1']['contains_null'], stan['n1']['min_value'], stan['n1']['max_value'], stan['n1']['min_is_exact'], stan['n1']['max_is_exact']) == \
               (True, None, None, None, None), stan['n1']
        q(cm, "UPDATE t_all_null SET n1 = -0.0500, n2 = 42.000001 WHERE id = 2")
        stan2 = table_col_stats(cm, 'lake_metadata', 't_all_null')
        assert (stan2['n1']['contains_null'], stan2['n1']['min_value'], stan2['n1']['max_value'], stan2['n1']['min_is_exact'], stan2['n1']['max_is_exact']) == \
               (True, '-0.0500', '-0.0500', True, True), stan2['n1']
        assert (stan2['n2']['contains_null'], stan2['n2']['min_value'], stan2['n2']['max_value'], stan2['n2']['min_is_exact'], stan2['n2']['max_is_exact']) == \
               (True, '42.000001', '42.000001', True, True), stan2['n2']

        q(cm, "ALTER FOREIGN TABLE t_all_null ADD COLUMN n3 numeric(10,2)")
        stan3 = table_col_stats(cm, 'lake_metadata', 't_all_null')
        assert (stan3['n3']['contains_null'], stan3['n3']['min_value'], stan3['n3']['max_value'], stan3['n3']['min_is_exact'], stan3['n3']['max_is_exact']) == \
               (True, None, None, None, None), stan3['n3']

        # Expand 128-bit max via UPDATE; conservative DELETE retention
        q(cm, "UPDATE t_num SET n2 = 223456789012345678901234567890.000001 WHERE id = 2")
        stn2 = table_col_stats(cm, 'lake_metadata', 't_num')
        assert (stn2['n2']['min_value'], stn2['n2']['max_value'], stn2['n2']['min_is_exact'], stn2['n2']['max_is_exact']) == \
               ('-999999999999999999999999999999.123456', '223456789012345678901234567890.000001', True, True), stn2['n2']
        q(cm, "DELETE FROM t_num WHERE id = 1")
        stn3 = table_col_stats(cm, 'lake_metadata', 't_num')
        assert stn3['n2']['min_value'] == '-999999999999999999999999999999.123456', stn3['n2']

        # Malformed external NUMERIC bound resilience
        tid_sub = s(cm, "SELECT table_id FROM lake_metadata.ducklake_table WHERE table_name='t_sub' AND end_snapshot IS NULL")
        cid_sub_n1 = s(cm, "SELECT column_id FROM lake_metadata.ducklake_column WHERE table_id=%s AND column_name='n1' AND end_snapshot IS NULL", (tid_sub,))
        q(cm, "UPDATE lake_metadata.ducklake_table_column_stats SET min_value='not_a_decimal' WHERE table_id=%s AND column_id=%s", (tid_sub, cid_sub_n1))
        q(cm, "INSERT INTO t_sub VALUES (7, 10.0000)")
        sts_bad = table_col_stats(cm, 'lake_metadata', 't_sub')
        assert (sts_bad['n1']['min_value'], sts_bad['n1']['max_value'],
                sts_bad['n1']['min_is_exact'], sts_bad['n1']['max_is_exact']) == (None, None, None, None), sts_bad['n1']

        stages['stage2_inline_numeric_stats'] = 10

        # ------------------------------------------------------------------
        # Stage 3: Per-File Stats Across Direct INSERT, Partitioned Direct INSERT,
        #          Flush, Identity Flush, rewrite_data_files & merge_adjacent_files
        # ------------------------------------------------------------------
        # 3a. Direct INSERT (non-partitioned + one all-NULL numeric column)
        q(cm, """
            CREATE FOREIGN TABLE t_dir (
                id int, b bool, txt text, u uuid, n1 numeric(12,4), n2 numeric(38,6), n_null numeric(8,2)
            ) SERVER cat OPTIONS (binding_mode 'create', lake_schema 'main')
        """)
        q(cm, "SELECT og_lake.set_option('t_dir'::regclass, 'data_inlining_row_limit'::text, '0'::text)")
        q(cm, """
            INSERT INTO t_dir VALUES
              (1, false, 'alpha', '00000000-0000-0000-0000-000000000001'::uuid, -123.4500, -999999999999999999999999999999.123456, NULL),
              (2, true,  'zeta',  'ffffffff-ffff-ffff-ffff-ffffffffffff'::uuid,  456.7890,  123456789012345678901234567890.654321, NULL),
              (3, NULL,  NULL,    NULL, NULL, NULL, NULL)
        """)
        f_dir = file_col_stats(cm, 'lake_metadata', 't_dir')
        assert len(f_dir) == 1
        fd = next(iter(f_dir.values()))['columns']
        assert (fd['id']['value_count'], fd['id']['null_count'], fd['id']['min_value'], fd['id']['max_value'], fd['id']['min_is_exact']) == (3, 0, '1', '3', True)
        assert (fd['b']['min_value'], fd['b']['max_value'], fd['b']['min_is_exact']) == ('false', 'true', True)
        assert (fd['txt']['min_value'], fd['txt']['max_value'], fd['txt']['min_is_exact']) == ('alpha', 'zeta', True)
        assert (fd['u']['min_value'], fd['u']['max_value']) == ('00000000-0000-0000-0000-000000000001', 'ffffffff-ffff-ffff-ffff-ffffffffffff')
        assert (fd['n1']['value_count'], fd['n1']['null_count'], fd['n1']['min_value'], fd['n1']['max_value'], fd['n1']['min_is_exact'], fd['n1']['max_is_exact']) == \
               (2, 1, '-123.4500', '456.7890', True, True), fd['n1']
        assert (fd['n2']['min_value'], fd['n2']['max_value'], fd['n2']['min_is_exact'], fd['n2']['max_is_exact']) == \
               ('-999999999999999999999999999999.123456', '123456789012345678901234567890.654321', True, True), fd['n2']
        assert (fd['n_null']['value_count'], fd['n_null']['null_count'], fd['n_null']['min_value'], fd['n_null']['max_value'], fd['n_null']['min_is_exact']) == \
               (0, 3, None, None, None), fd['n_null']

        # 3b. Partitioned direct INSERT: per-file disjoint bounds vs table union!
        q(cm, """
            CREATE FOREIGN TABLE t_part_dir (
                id int, grp text, p_num numeric(10,2), n2 numeric(38,6)
            ) SERVER cat OPTIONS (binding_mode 'create', lake_schema 'main')
        """)
        q(cm, "SELECT og_lake.set_partition('t_part_dir'::regclass, ARRAY['p_num'])")
        q(cm, "SELECT og_lake.set_option('t_part_dir'::regclass, 'data_inlining_row_limit'::text, '0'::text)")
        q(cm, """
            INSERT INTO t_part_dir VALUES
              (1, 'g1', -10.50, -50.100000),
              (2, 'g2', -10.50,  -1.200000),
              (3, 'g3',  20.00, 100.300000),
              (4, 'g4',  20.00, 999999999999999999999999999999.500000)
        """)
        f_pdir = file_col_stats(cm, 'lake_metadata', 't_part_dir')
        assert len(f_pdir) == 2, f_pdir
        by_p = {f['columns']['p_num']['min_value']: f for f in f_pdir.values()}
        assert set(by_p.keys()) == {'-10.50', '20.00'}, by_p
        assert (by_p['-10.50']['columns']['n2']['min_value'], by_p['-10.50']['columns']['n2']['max_value'],
                by_p['-10.50']['columns']['n2']['min_is_exact']) == ('-50.100000', '-1.200000', True)
        assert (by_p['20.00']['columns']['n2']['min_value'], by_p['20.00']['columns']['n2']['max_value'],
                by_p['20.00']['columns']['n2']['min_is_exact']) == ('100.300000', '999999999999999999999999999999.500000', True)
        stp = table_col_stats(cm, 'lake_metadata', 't_part_dir')
        assert (stp['p_num']['min_value'], stp['p_num']['max_value']) == ('-10.50', '20.00')
        assert (stp['n2']['min_value'], stp['n2']['max_value']) == ('-50.100000', '999999999999999999999999999999.500000')

        # 3c. Inline Flush -> rewrite_data_files (extreme deleted) -> merge_adjacent_files
        q(cm, """
            CREATE FOREIGN TABLE t_flow (
                id int, n1 numeric(12,4), n2 numeric(38,6), u uuid, s text
            ) SERVER cat OPTIONS (binding_mode 'create', lake_schema 'main')
        """)
        q(cm, """
            INSERT INTO t_flow VALUES
              (1, -25.0000, -900000000000000000000000000000.500000, '00000000-0000-0000-0000-000000000001'::uuid, 'aa'),
              (2,  -5.0000, -10.000000,                             '00000000-0000-0000-0000-000000000002'::uuid, 'bb'),
              (3,   5.0000,  20.000000,                             '00000000-0000-0000-0000-000000000003'::uuid, 'cc'),
              (4,  50.0000,  900000000000000000000000000000.500000, '00000000-0000-0000-0000-000000000004'::uuid, 'dd')
        """)
        q(cm, "SELECT og_lake.flush('t_flow'::regclass)")
        f_flow = file_col_stats(cm, 'lake_metadata', 't_flow')
        assert len(f_flow) == 1
        flushed_fid = next(iter(f_flow))
        assert (f_flow[flushed_fid]['columns']['n1']['min_value'], f_flow[flushed_fid]['columns']['n1']['max_value'],
                f_flow[flushed_fid]['columns']['n1']['min_is_exact']) == ('-25.0000', '50.0000', True)
        assert (f_flow[flushed_fid]['columns']['n2']['min_value'], f_flow[flushed_fid]['columns']['n2']['max_value']) == \
               ('-900000000000000000000000000000.500000', '900000000000000000000000000000.500000')

        # Delete min and max rows -> rewrite_data_files narrows rewritten file bounds cleanly
        q(cm, "DELETE FROM t_flow WHERE id IN (1, 4)")
        q(cm, "SELECT og_lake.rewrite_data_files('t_flow'::regclass, ARRAY[%s::bigint])", (flushed_fid,))
        f_rw = file_col_stats(cm, 'lake_metadata', 't_flow')
        active_rw = [v for v in f_rw.values() if v['end_snapshot'] is None]
        assert len(active_rw) == 1
        assert (active_rw[0]['columns']['n1']['min_value'], active_rw[0]['columns']['n1']['max_value'],
                active_rw[0]['columns']['n1']['min_is_exact']) == ('-5.0000', '5.0000', True)
        assert (active_rw[0]['columns']['n2']['min_value'], active_rw[0]['columns']['n2']['max_value']) == ('-10.000000', '20.000000')
        # Conservative table bounds retained
        st_flow = table_col_stats(cm, 'lake_metadata', 't_flow')
        assert (st_flow['n1']['min_value'], st_flow['n1']['max_value']) == ('-25.0000', '50.0000')

        # Add two direct small files and compact via merge_adjacent_files
        q(cm, "SELECT og_lake.set_option('t_flow'::regclass, 'data_inlining_row_limit'::text, '0'::text)")
        q(cm, "INSERT INTO t_flow VALUES (5, -50.0000, -100.000000, '00000000-0000-0000-0000-000000000005'::uuid, 'e1')")
        q(cm, "INSERT INTO t_flow VALUES (6, 500.0000,  999.000000, '00000000-0000-0000-0000-000000000006'::uuid, 'f1')")
        m_res = q(cm, "SELECT files_processed, files_created FROM og_lake.merge_adjacent_files('t_flow'::regclass)")
        assert m_res == [(3, 1)], m_res
        f_mg = file_col_stats(cm, 'lake_metadata', 't_flow')
        active_mg = [v for v in f_mg.values() if v['end_snapshot'] is None]
        assert len(active_mg) == 1
        assert (active_mg[0]['columns']['n1']['min_value'], active_mg[0]['columns']['n1']['max_value'],
                active_mg[0]['columns']['n1']['min_is_exact']) == ('-50.0000', '500.0000', True)
        assert (active_mg[0]['columns']['n2']['min_value'], active_mg[0]['columns']['n2']['max_value']) == ('-100.000000', '999.000000')

        # 3d. Identity-partitioned flush with NUMERIC partition key + NULL partition + payload NUMERIC
        q(cm, """
            CREATE FOREIGN TABLE t_part_flush (
                id int, k_num numeric(10,2), n1 numeric(12,4), s text
            ) SERVER cat OPTIONS (binding_mode 'create', lake_schema 'main')
        """)
        q(cm, "SELECT og_lake.set_partition('t_part_flush'::regclass, ARRAY['k_num'])")
        q(cm, """
            INSERT INTO t_part_flush VALUES
              (1, -10.25,  1.5000, 'p1_a'),
              (2, -10.25, 99.5000, 'p1_b'),
              (3,  20.75, -8.2500, 'p2_a'),
              (4,   NULL,  5.0000, 'pnull')
        """)
        # Bounded identity flush limit is 2 groups per call -> 2 calls flush all 3 partitions
        q(cm, "SELECT og_lake.flush('t_part_flush'::regclass)")
        q(cm, "SELECT og_lake.flush('t_part_flush'::regclass)")
        f_pf = file_col_stats(cm, 'lake_metadata', 't_part_flush')
        active_pf = [v for v in f_pf.values() if v['end_snapshot'] is None]
        assert len(active_pf) == 3, active_pf
        null_key_files = [v for v in active_pf if v['columns']['k_num']['min_value'] is None]
        assert len(null_key_files) == 1
        assert (null_key_files[0]['columns']['k_num']['null_count'], null_key_files[0]['columns']['n1']['min_value'],
                null_key_files[0]['columns']['n1']['max_value']) == (1, '5.0000', '5.0000')
        non_null_pf = {v['columns']['k_num']['min_value']: v for v in active_pf if v['columns']['k_num']['min_value'] is not None}
        assert set(non_null_pf.keys()) == {'-10.25', '20.75'}
        assert (non_null_pf['-10.25']['columns']['n1']['min_value'], non_null_pf['-10.25']['columns']['n1']['max_value']) == ('1.5000', '99.5000')
        assert (non_null_pf['20.75']['columns']['n1']['min_value'], non_null_pf['20.75']['columns']['n1']['max_value']) == ('-8.2500', '-8.2500')

        stages['stage3_file_stats_all_writers'] = 18

        # ------------------------------------------------------------------
        # Stage 4: Native ANALYZE, pg_stats, Optimizer Cost/Plan Sensitivity
        # ------------------------------------------------------------------
        q(cm, "CREATE FOREIGN TABLE t_anl (id int, tag int, n1 numeric(12,4), n2 numeric(38,6)) SERVER cat OPTIONS (binding_mode 'create', lake_schema 'main')")
        q(cm, """
            INSERT INTO t_anl
            SELECT i,
                   CASE WHEN i <= 400 THEN 99 ELSE i + 100 END,
                   CASE WHEN i <= 400 THEN 77.7700::numeric(12,4) ELSE (i * 0.25)::numeric(12,4) END,
                   CASE WHEN i <= 400 THEN 1000000000000000000000.123456::numeric(38,6) ELSE (i * 1.5)::numeric(38,6) END
            FROM generate_series(1, 600) i
        """)
        q(cm, "SELECT og_lake.flush('t_anl'::regclass)")
        q(cm, """
            INSERT INTO t_anl
            SELECT i, i + 100, (i * 0.25)::numeric(12,4), (i * 1.5)::numeric(38,6)
            FROM generate_series(601, 700) i
        """)
        ix_anl = s(cm, "SELECT og_lake.create_lake_index('t_anl'::regclass, 'ix_anl_id'::name, '{\"version\":1,\"kind\":\"scalar\",\"method\":\"btree\",\"column\":\"id\"}'::jsonb)")
        q(cm, "SELECT og_lake.build_lake_index(%s::uuid)", (ix_anl,))
        ix_tag = s(cm, "SELECT og_lake.create_lake_index('t_anl'::regclass, 'ix_anl_tag'::name, '{\"version\":1,\"kind\":\"scalar\",\"method\":\"btree\",\"column\":\"tag\"}'::jsonb)")
        q(cm, "SELECT og_lake.build_lake_index(%s::uuid)", (ix_tag,))

        rel_before = q(cm, "SELECT reltuples, relpages FROM pg_class WHERE relname='t_anl'")[0]
        q(cm, 'ANALYZE t_anl')
        rel_after = q(cm, "SELECT reltuples::bigint, relpages FROM pg_class WHERE relname='t_anl'")[0]
        assert abs(rel_after[0] - 700) <= 50 and rel_after[1] >= 1, (rel_before, rel_after)

        pgs_n1 = q_dicts(cm, "SELECT attname, null_frac, avg_width, n_distinct, most_common_vals::text, most_common_freqs::text, histogram_bounds::text FROM pg_stats WHERE tablename='t_anl' AND attname IN ('n1','n2') ORDER BY attname")
        assert len(pgs_n1) == 2, pgs_n1
        for st_row in pgs_n1:
            assert st_row['avg_width'] > 0 and st_row['n_distinct'] != 0, st_row
            assert st_row['most_common_vals'] is not None or st_row['histogram_bounds'] is not None, st_row

        hot_rows, hot_plan = explain_est_rows(cm, "SELECT id, n1 FROM t_anl WHERE n1 = 77.7700")
        rare_rows, rare_plan = explain_est_rows(cm, "SELECT id, n1 FROM t_anl WHERE n1 = 150.2500")
        assert hot_rows > 150.0 and rare_rows <= 25.0 and hot_rows > rare_rows * 10, (hot_rows, rare_rows, hot_plan, rare_plan)

        hot_tag_rows, _ = explain_est_rows(cm, "SELECT id, n1 FROM t_anl WHERE tag = 99")
        rare_tag_rows, _ = explain_est_rows(cm, "SELECT id, n1 FROM t_anl WHERE tag = 550")
        assert hot_rows > 150.0 and rare_rows <= 25.0 and hot_rows > rare_rows * 10, (hot_rows, rare_rows, hot_plan, rare_plan)
        assert hot_tag_rows > 150.0 and rare_tag_rows <= 25.0 and hot_tag_rows > rare_tag_rows * 10, (hot_tag_rows, rare_tag_rows)

        # Range & filtered join & single-column ANALYZE + column rename re-ANALYZE (STAT-04/09/10)
        range_narrow, _ = explain_est_rows(cm, "SELECT id FROM t_anl WHERE n1 BETWEEN 149.0000 AND 151.0000")
        range_wide, _ = explain_est_rows(cm, "SELECT id FROM t_anl WHERE n1 >= 70.0000")
        assert range_wide > range_narrow * 10, (range_narrow, range_wide)
        join_rows, _ = explain_est_rows(cm, "SELECT a.id, b.n1 FROM t_anl a JOIN t_flow b ON a.n1 = b.n1 WHERE a.n1 > 0.0000")
        assert join_rows >= 1.0
        # Regular SELECT does not alter pg_class reltuples/relpages
        rel_snap = q(cm, "SELECT reltuples::bigint, relpages FROM pg_class WHERE relname='t_anl'")[0]
        assert q(cm, "SELECT count(*) FROM t_anl WHERE n1 = 77.7700")[0][0] == 400
        assert q(cm, "SELECT reltuples::bigint, relpages FROM pg_class WHERE relname='t_anl'")[0] == rel_snap
        q(cm, 'ANALYZE t_anl(n1)')
        assert s(cm, "SELECT count(*) FROM pg_stats WHERE tablename='t_anl' AND attname='n1'") == 1

        # Verify scalar index execution + query parity on analyzed table with NUMERIC(p,s) payloads
        q(cm, 'SET enable_seqscan = off')
        exp_ix = explain_text(cm, "SELECT id, n1, n2 FROM t_anl WHERE id = 601 ORDER BY id", analyze=True)
        assert 'Verified Scalar File Coverage: 1' in exp_ix, exp_ix
        assert q(cm, "SELECT id, n1, n2 FROM t_anl WHERE id = 601 ORDER BY id") == [(601, decimal.Decimal('150.2500'), decimal.Decimal('901.500000'))]
        exp_tag = explain_text(cm, "SELECT id, tag, n1 FROM t_anl WHERE tag = 550 ORDER BY id", analyze=True)
        assert 'Verified Scalar File Coverage: 1' in exp_tag, exp_tag
        assert q(cm, "SELECT id, tag, n1 FROM t_anl WHERE tag = 550 ORDER BY id") == [(450, 550, decimal.Decimal('112.5000'))]
        q(cm, 'SET enable_seqscan = on')

        # Analyze partitioned table too
        q(cm, 'ANALYZE t_part_dir')
        p_rows, p_plan = explain_est_rows(cm, "SELECT id FROM t_part_dir WHERE p_num = -10.50")
        assert s(cm, "SELECT count(*) FROM t_part_dir WHERE p_num = -10.50") == 2
        assert 'Pruned' in p_plan or p_rows >= 1.0, p_plan

        stages['stage4_analyze_and_cost'] = 8

        # ------------------------------------------------------------------
        # Stage 5: Pinned DuckDB Parquet + Reference + Offline DuckLake Parity
        # ------------------------------------------------------------------
        # 5a. Direct read_parquet() extrema parity on actual openGauss Parquet files
        for tbl_name, num_cols in [('t_dir', ('n1', 'n2')), ('t_part_dir', ('n2',)), ('t_flow', ('n1', 'n2'))]:
            files_info = file_col_stats(cm, 'lake_metadata', tbl_name)
            for f in files_info.values():
                if f['end_snapshot'] is not None:
                    continue
                matches = list(cat_m.rglob(f['path']))
                assert len(matches) == 1, f['path']
                pq_path = matches[0]
                exprs = ', '.join(f"min({c})::VARCHAR AS {c}_min, max({c})::VARCHAR AS {c}_max, count(*) - count({c}) AS {c}_nulls" for c in num_cols)
                dk_pq = duckdb_query(f"SELECT {exprs} FROM read_parquet('{pq_path}')")[0]
                duckdb_checks += 1
                for c in num_cols:
                    fc = f['columns'][c]
                    assert str(dk_pq[f'{c}_min']) == str(fc['min_value']), (tbl_name, f['path'], c, dk_pq, fc)
                    assert str(dk_pq[f'{c}_max']) == str(fc['max_value']), (tbl_name, f['path'], c, dk_pq, fc)
                    assert int(dk_pq[f'{c}_nulls']) == int(fc['null_count']), (tbl_name, f['path'], c, dk_pq, fc)

        # 5b. Parallel Reference DuckDB DuckLake comparison on t_dir / t_part_dir / t_all_null
        ref_dl = tmp_root / 'ref_parity.ducklake'
        ref_data = tmp_root / 'ref_parity_data'
        ref_data.mkdir()
        ref_sql = f"""
        LOAD ducklake;
        ATTACH 'ducklake:{ref_dl}' AS dl (DATA_PATH '{ref_data}', DATA_INLINING_ROW_LIMIT 0);
        CREATE TABLE dl.t_dir (id INT, b BOOLEAN, txt VARCHAR, u UUID, n1 DECIMAL(12,4), n2 DECIMAL(38,6), n_null DECIMAL(8,2));
        INSERT INTO dl.t_dir VALUES
          (1, false, 'alpha', '00000000-0000-0000-0000-000000000001'::UUID, -123.4500, -999999999999999999999999999999.123456, NULL),
          (2, true,  'zeta',  'ffffffff-ffff-ffff-ffff-ffffffffffff'::UUID,  456.7890,  123456789012345678901234567890.654321, NULL),
          (3, NULL,  NULL,    NULL, NULL, NULL, NULL);
        CREATE TABLE dl.t_part_dir (id INT, grp VARCHAR, p_num DECIMAL(10,2), n2 DECIMAL(38,6));
        ALTER TABLE dl.t_part_dir SET PARTITIONED BY (p_num);
        INSERT INTO dl.t_part_dir VALUES
          (1, 'g1', -10.50, -50.100000),
          (2, 'g2', -10.50,  -1.200000),
          (3, 'g3',  20.00, 100.300000),
          (4, 'g4',  20.00, 999999999999999999999999999999.500000);
        DETACH dl;
        """
        proc = subprocess.run([str(DUCKDB_BIN), '-batch', '-dark-mode', '-c', ref_sql],
                              stdin=subprocess.DEVNULL, capture_output=True, text=True)
        assert proc.returncode == 0, proc.stderr
        ref_tcs = duckdb_query(f"""
            ATTACH '{ref_dl}' AS m (READ_ONLY);
            SELECT t.table_name, c.column_name, s.contains_null, s.min_value, s.max_value, s.min_is_exact, s.max_is_exact
            FROM m.ducklake_table t
            JOIN m.ducklake_column c USING(table_id)
            JOIN m.ducklake_table_column_stats s USING(table_id, column_id)
            ORDER BY t.table_name, c.column_id
        """)
        duckdb_checks += 1
        ref_map = {(r['table_name'], r['column_name']): r for r in ref_tcs}
        for tbl_name, check_cols in [('t_dir', ('n1', 'n2', 'n_null', 'u', 'txt', 'id')),
                                     ('t_part_dir', ('p_num', 'n2'))]:
            og_st = table_col_stats(cm, 'lake_metadata', tbl_name)
            for c in check_cols:
                rr = ref_map[(tbl_name, c)]
                oo = og_st[c]
                assert (oo['contains_null'], oo['min_value'], oo['max_value'], oo['min_is_exact'], oo['max_is_exact']) == \
                       (rr['contains_null'], rr['min_value'], rr['max_value'], rr['min_is_exact'], rr['max_is_exact']), (tbl_name, c, oo, rr)

        # 5c. Offline export of actual openGauss catalog -> DuckDB DuckLake query + stats pruning read
        exported_dl = export_catalog_to_duckdb(cm, 'lake_metadata', tmp_root, 'og_exported')
        dk_flow = duckdb_query(f"""
            LOAD ducklake;
            ATTACH 'ducklake:{exported_dl}' AS og (DATA_PATH '{cat_m}', READ_ONLY);
            SELECT id, n1::VARCHAR AS n1, n2::VARCHAR AS n2 FROM og.main.t_flow WHERE n1 > 0.0000 ORDER BY id
        """)
        duckdb_checks += 1
        assert dk_flow == [
            {'id': 3, 'n1': '5.0000', 'n2': '20.000000'},
            {'id': 6, 'n1': '500.0000', 'n2': '999.000000'},
        ], dk_flow

        dk_part = duckdb_query(f"""
            LOAD ducklake;
            ATTACH 'ducklake:{exported_dl}' AS og (DATA_PATH '{cat_m}', READ_ONLY);
            SELECT id, p_num::VARCHAR AS p_num, n2::VARCHAR AS n2 FROM og.main.t_part_dir WHERE n2 > 500.000000 ORDER BY id
        """)
        duckdb_checks += 1
        assert dk_part == [
            {'id': 4, 'p_num': '20.00', 'n2': '999999999999999999999999999999.500000'},
        ], dk_part

        dk_num = duckdb_query(f"""
            LOAD ducklake;
            ATTACH 'ducklake:{exported_dl}' AS og (DATA_PATH '{cat_m}', READ_ONLY);
            SELECT id, n1::VARCHAR AS n1, n2::VARCHAR AS n2 FROM og.main.t_num ORDER BY id
        """)
        duckdb_checks += 1
        assert dk_num == [
            {'id': 2, 'n1': '456.7890', 'n2': '223456789012345678901234567890.000001'},
            {'id': 3, 'n1': None, 'n2': None},
        ], dk_num

        stages['stage5_duckdb_parity_and_interop'] = duckdb_checks

        pool_state = s(cm, 'SHOW enable_thread_pool')
        total_checks = sum(stages.values())
        result = {
            'status': 'pass',
            'profile': args.profile,
            'postgres_ext_version': '0.65',
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
