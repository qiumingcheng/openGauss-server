#!/usr/bin/env python3
"""Run SQL0.60 against an owned runtime; keep databases and files on failure."""

import argparse
from contextlib import closing, contextmanager
from datetime import datetime, timezone
from hashlib import file_digest
import hashlib
import json
import os
from pathlib import Path
import secrets
import sys
import time
import traceback
from uuid import uuid4

import psycopg2
from psycopg2 import sql

ROOT = Path('/home/qiumc/data/code/cpp')
DRIVER = ROOT / 'integration/opengauss-native-lake'
sys.path[:0] = [str(DRIVER), str(DRIVER / 'tests')]
from native_runtime import INSTALL, PORT, PROFILE, RUNTIME, SOURCE
from empty_catalog_oracle import read_native_catalog, transport_native
from reverse_reference import Reference

PINNED_DUCKLAKE = '0aaa8e10d4adfe2ca37da77134e991d20106d0e7'
EXT_VERSION = "SELECT extversion FROM pg_extension WHERE extname='og_lake'"
CATALOG = 'lake060'
META = 'meta060'
TARGET = 2**24
# The canonical Debug PGDATA contains preserved older failure fixtures. Only
# the pool-on phase may use an explicitly owned, fresh Debug cluster instead.
if os.environ.get('OG_LAKE_060_ISOLATED_RUNTIME'):
    isolated = Path(os.environ['OG_LAKE_060_ISOLATED_RUNTIME']).resolve(strict=True)
    base = ROOT / '.runtime/opengauss-native-lake'
    if PROFILE != 'debug' or isolated.parent != base or not isolated.name.startswith('sql060p-'):
        raise ValueError('Invalid isolated SQL0.60 Debug runtime')
    identity = json.loads((isolated / 'owner.json').read_text())
    if (identity.get('project') != 'opengauss-native-lake-sql060-pool' or
            identity.get('install') != str(INSTALL) or identity.get('data') != str(isolated / 'data') or
            identity.get('port') != PORT):
        raise ValueError('SQL0.60 pool cluster identity mismatch')
    RUNTIME = isolated


def sha(path):
    with path.open('rb') as stream:
        return file_digest(stream, 'sha256').hexdigest()


def now():
    return datetime.now(timezone.utc).isoformat()


def write_json(path, obj):
    tmp = path.with_suffix('.tmp')
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2, default=str) + '\n')
    tmp.replace(path)


def connect(database, user=None, password=None):
    conn = psycopg2.connect(host=str(RUNTIME / 'socket'), port=PORT, dbname=database,
                            user=user or os.environ['USER'], password=password, connect_timeout=10,
                            application_name='og_lake_sql060')
    conn.autocommit = True
    query(conn, "SET statement_timeout = '60s'")
    return conn


def query(conn, statement, args=()):
    with conn.cursor() as cur:
        cur.execute(statement, args)
        return cur.fetchall() if cur.description is not None else None


def scalar(conn, statement, args=()):
    result = query(conn, statement, args)
    assert len(result) == 1 and len(result[0]) == 1, (statement, result)
    return result[0][0]


def reject(conn, state, statement, args=()):
    try:
        query(conn, statement, args)
    except psycopg2.Error as error:
        assert error.pgcode == state, (state, error.pgcode, str(error), statement)
    else:
        raise AssertionError(f'Expected SQLSTATE {state} for {statement}')


@contextmanager
def transaction(conn):
    query(conn, 'BEGIN')
    try:
        yield
    finally:
        query(conn, 'ROLLBACK')


def make_catalog(conn, owner, data_root, version=None):
    assert not data_root.exists()
    query(conn, 'CREATE EXTENSION og_lake' if version is None else
          sql.SQL('CREATE EXTENSION og_lake VERSION {}').format(sql.Literal(version)))
    query(conn, 'SELECT og_lake.configure_internal_owner(%s::name)', (owner,))
    query(conn, 'SELECT og_lake.create_catalog(%s::name, %s::name, %s, NULL)',
          (CATALOG, META, str(data_root)))
    set_option(conn, CATALOG, 'data_inlining_row_limit', '128', catalog=True)
    query(conn, 'CREATE SCHEMA biz')


def set_option(conn, scope, key, value, *, catalog=False):
    kind = 'name' if catalog else 'regclass'
    query(conn, f'SELECT og_lake.set_option(%s::{kind}, %s, %s)', (scope, key, value))


def option_rows(conn):
    return query(conn, 'SELECT option_name, value, scope, scope_entry '
                 'FROM og_lake.options(%s::name) ORDER BY 1, 3, 4', (CATALOG,))


def make_table(conn, table, *, partitioned=False):
    query(conn, sql.SQL('CREATE FOREIGN TABLE biz.{} (id integer, part integer, val text) '
                       "SERVER lake060 OPTIONS (binding_mode 'create', lake_schema 'main')")
          .format(sql.Identifier(table)))
    if partitioned:
        scalar(conn, "SELECT og_lake.set_partition(%s::regclass, ARRAY['part'])", (f'biz.{table}',))


def flush_rows(conn, table, rows):
    with conn.cursor() as cur:
        cur.executemany(sql.SQL('INSERT INTO biz.{} (id, part, val) VALUES (%s, %s, %s)')
                        .format(sql.Identifier(table)), rows)
    scalar(conn, 'SELECT og_lake.flush(%s::regclass)', (f'biz.{table}',))


def files(conn, table, *, order='id'):
    sort = {'id': 'df.data_file_id', 'logical': 'COALESCE(df.file_order, df.data_file_id), df.data_file_id'}[order]
    return query(conn, f'SELECT df.data_file_id, df.path, df.file_size_bytes, df.record_count, df.partition_id '
                 f'FROM {META}.ducklake_data_file df JOIN {META}.ducklake_table t USING (table_id) '
                 f'WHERE t.table_name=%s AND t.end_snapshot IS NULL AND df.end_snapshot IS NULL ORDER BY {sort}',
                 (table,))


def rows(conn, table, projection='id, part, val'):
    return query(conn, sql.SQL('SELECT {} FROM biz.{} ORDER BY id').format(
        sql.SQL(projection), sql.Identifier(table)))


def snap(conn, *, last=False):
    fn = 'last_committed_snapshot' if last else 'current_snapshot'
    return scalar(conn, f'SELECT id FROM og_lake.{fn}(%s::name)', (CATALOG,))


def merge(conn, table, max_files=16, target=None):
    return query(conn, 'SELECT * FROM og_lake.merge_adjacent_files(%s::regclass, %s, %s)',
                 (f'biz.{table}', max_files, target))


def rewrite(conn, table, selected):
    scalar(conn, 'SELECT og_lake.rewrite_data_files(%s::regclass, %s::bigint[])', (f'biz.{table}', selected))


def tt(conn, table, version, columns):
    projection = ', '.join(column.split()[0] for column in columns.split(', '))
    return query(conn, f'SELECT {projection} FROM og_lake.time_travel(%s::regclass, %s) AS r({columns}) ORDER BY id',
                 (f'biz.{table}', version))


def changes(conn, table, start, end, columns, function='table_changes'):
    shape = 'snapshot_id bigint, rowid bigint, '
    if function == 'table_changes':
        shape += 'change_type text, '
    return query(conn, f'SELECT * FROM og_lake.{function}(%s::regclass, %s, %s) '
                 f'AS r({shape}{columns}) ORDER BY 1, 2, 3', (f'biz.{table}', start, end))


def gate_and_upgrade(conn):
    assert scalar(conn, EXT_VERSION) == '0.59'
    hs = json.loads(scalar(conn, 'SELECT og_lake.host_status()'))
    assert hs['module_version'] in ('0.60', '0.73', '0.74') and hs['merge_adjacent_sql_version'] == '0.60' and hs['merge_adjacent_files']
    make_table(conn, 'upgrade_lane')
    for i in range(3):
        flush_rows(conn, 'upgrade_lane', [(i, i, f'pre-{i}')])
    query(conn, 'ALTER FOREIGN TABLE biz.upgrade_lane ADD COLUMN added integer DEFAULT 7')
    ids = [f[0] for f in files(conn, 'upgrade_lane')]
    assert len(ids) == 3, ids
    for key, value in [('target_file_size', '1024'), ('auto_compact', 'true')]:
        reject(conn, '0A000', 'SELECT og_lake.set_option(%s::name, %s, %s)', (CATALOG, key, value))
        reject(conn, '0A000', 'SELECT og_lake.set_option(%s::regclass, %s, %s)', ('biz.upgrade_lane', key, value))
    reject(conn, '42883', "SELECT * FROM og_lake.merge_adjacent_files('biz.upgrade_lane'::regclass)")
    reject(conn, '0A000', "SELECT og_lake.rewrite_data_files('biz.upgrade_lane', %s::bigint[])", (ids,))
    query(conn, "ALTER EXTENSION og_lake UPDATE TO '0.60'")
    assert scalar(conn, EXT_VERSION) == '0.60'
    rewrite(conn, 'upgrade_lane', ids)
    assert len(files(conn, 'upgrade_lane')) == 1
    assert rows(conn, 'upgrade_lane', 'id, part, val, added') == [(i, i, f'pre-{i}', 7) for i in range(3)]


def check_options(conn):
    assert scalar(conn, EXT_VERSION) in ('0.60', '0.73', '0.74')
    make_table(conn, 'options_lane')
    set_option(conn, CATALOG, 'target_file_size', '00020000000', catalog=True)
    set_option(conn, CATALOG, 'auto_compact', 'OFF', catalog=True)
    set_option(conn, 'biz.options_lane', 'target_file_size', str(TARGET))
    set_option(conn, 'biz.options_lane', 'auto_compact', 'ON')
    observed = option_rows(conn)
    assert ('target_file_size', '20000000', 'GLOBAL', None) in observed, observed
    assert ('auto_compact', 'false', 'GLOBAL', None) in observed, observed
    assert ('target_file_size', str(TARGET), 'TABLE', 'main.options_lane') in observed, observed
    assert ('auto_compact', 'true', 'TABLE', 'main.options_lane') in observed, observed
    for key, val, code in (('target_file_size', '0', '22023'), ('target_file_size', '', '22023'),
                           ('target_file_size', '-1', '22023'), ('target_file_size', '1MB', '22023'),
                           ('target_file_size', str(2**63), '22003'), ('auto_compact', 'sometimes', '22023')):
        reject(conn, code, "SELECT og_lake.set_option('biz.options_lane'::regclass, %s, %s)", (key, val))
    with transaction(conn):
        query(conn, 'SAVEPOINT opts')
        set_option(conn, 'biz.options_lane', 'target_file_size', '1234')
        set_option(conn, 'biz.options_lane', 'auto_compact', '0')
        query(conn, 'ROLLBACK TO opts')
        assert option_rows(conn) == observed
    committed = snap(conn)
    with transaction(conn):
        scalar(conn, 'SELECT og_lake.bind_snapshot(%s::name, %s)', (CATALOG, committed))
        reject(conn, '25006', "SELECT og_lake.set_option('biz.options_lane'::regclass, 'target_file_size', '2000')")
    assert option_rows(conn) == observed
    return observed


def check_merge(conn, root):
    table = 'merge_lane'
    make_table(conn, table)
    for i in range(4):
        flush_rows(conn, table, [(i, 0, f'small-{i}')])
    original = files(conn, table, order='logical')
    assert len(original) == 4 and all(f[3] == 1 for f in original), original
    before = snap(conn)
    last = snap(conn, last=True)
    assert merge(conn, table, target=min(f[2] for f in original)) == []
    assert files(conn, table, order='logical') == original and snap(conn) == before and snap(conn, last=True) == last
    reject(conn, '22023', "SELECT * FROM og_lake.merge_adjacent_files('biz.merge_lane', 1)")
    reject(conn, '22023', "SELECT * FROM og_lake.merge_adjacent_files('biz.merge_lane', 17)")
    reject(conn, '22023', "SELECT * FROM og_lake.merge_adjacent_files('biz.merge_lane', 2, 0)")
    output = lambda: sorted(p.relative_to(root) for p in root.rglob('og-merge-*.parquet'))
    artifacts = output()
    with transaction(conn):
        assert merge(conn, table, 2, TARGET) == [('main', table, 2, 1)]
    assert output() == artifacts and snap(conn) == before and files(conn, table, order='logical') == original
    with transaction(conn):
        query(conn, 'SAVEPOINT merge_rollback')
        assert merge(conn, table, 2, TARGET) == [('main', table, 2, 1)]
        query(conn, 'ROLLBACK TO merge_rollback')
        query(conn, 'COMMIT')
    assert output() == artifacts and snap(conn) == before and files(conn, table, order='logical') == original
    # The first exact-size pair strictly improves distance; a third file cannot improve zero distance.
    exact = original[0][2] + original[1][2]
    assert merge(conn, table, 16, exact) == [('main', table, 2, 1)]
    after = snap(conn)
    retired = query(conn, f'SELECT data_file_id, end_snapshot FROM {META}.ducklake_data_file '
                    'WHERE data_file_id=ANY(%s::bigint[]) ORDER BY data_file_id', ([f[0] for f in original],))
    chosen = {f[0] for f in original[:2]}
    assert after > before and {i for i, end in retired if end == after} == chosen, retired
    assert len(files(conn, table)) == 3 and len(output()) == len(artifacts) + 1
    assert all(any(root.rglob(Path(f[1]).name)) for f in original), original
    expected = [(i, 0, f'small-{i}') for i in range(4)]
    assert rows(conn, table) == tt(conn, table, before, 'id integer, part integer, val text') == expected
    assert changes(conn, table, after, after, 'id integer, part integer, val text') == []
    return before, after


def check_permissions(conn, dbname, role, password):
    query(conn, sql.SQL('GRANT USAGE ON SCHEMA biz TO {}').format(sql.Identifier(role)))
    query(conn, sql.SQL('GRANT SELECT ON biz.merge_lane TO {}').format(sql.Identifier(role)))
    with closing(connect(dbname, role, password)) as other:
        assert scalar(other, 'SELECT current_user') == role
        assert rows(other, 'merge_lane') == rows(conn, 'merge_lane')
        reject(other, '42501', "SELECT * FROM og_lake.merge_adjacent_files('biz.merge_lane')")
    version = snap(conn)
    with transaction(conn):
        scalar(conn, 'SELECT og_lake.bind_snapshot(%s::name, %s)', (CATALOG, version))
        reject(conn, '25006', "SELECT * FROM og_lake.merge_adjacent_files('biz.merge_lane')")
    assert snap(conn) == version


def run(args):
    if not __debug__:
        raise RuntimeError('Optimized Python disables assertions; refusing to run')
    if args.maintenance == 'on' and (PROFILE != 'debug' or args.pool != 'on' or args.manual_only):
        raise ValueError('Automatic maintenance is tested only in the complete owned Debug pool-on run')
    output = args.output.resolve()
    output.mkdir(mode=0o700, parents=True, exist_ok=False)
    identity = uuid4().hex[:12]
    owner, reader = 'lake060_owner_' + identity, 'lake060_reader_' + identity
    db_old, db_new = 'lake060_up_' + identity, 'lake060_new_' + identity
    role_pwd = 'La1!' + secrets.token_hex(12)
    receipt = json.loads(args.build.read_text())
    qualified_hash = receipt.get('binary_sha256')
    result = {'status': 'running', 'started_at': now(), 'profile': PROFILE, 'port': PORT,
              'build_receipt': str(args.build.resolve()), 'build_receipt_sha256': sha(args.build),
              'runner_sha256': sha(Path(__file__)), 'stages': [], 'created': [],
              'candidate_databases': [db_old, db_new], 'candidate_roles': [owner, reader]}
    resultfile = output / 'result.json'
    write_json(resultfile, result)

    def stage(name, fn):
        result['active_stage'] = name
        write_json(resultfile, result)
        value = fn()
        result['stages'].append({'name': name, 'finished_at': now(), 'evidence': value})
        write_json(resultfile, result)
        return value

    try:
        if PROFILE not in ('debug', 'ann-debug') or receipt.get('status') != 'build_and_sql_passed':
            raise ValueError('An owned Debug runtime with a completed build is required')
        pid_lines = (RUNTIME / 'data/postmaster.pid').read_text().splitlines()
        assert receipt['profile'] == PROFILE and receipt['running_pid'] == int(pid_lines[0]), (PROFILE, pid_lines[0])
        assert receipt['data'] == str(RUNTIME / 'data') and int(receipt['port']) == PORT
        assert qualified_hash == sha(INSTALL / 'bin/gaussdb') == sha(Path('/proc') / pid_lines[0] / 'exe')
        result.update(binary_sha256=qualified_hash, source_head=receipt['source_head'], running_pid=int(pid_lines[0]))
        with closing(connect('postgres')) as admin:
            settings = {key: scalar(admin, 'SHOW ' + key) for key in ('enable_thread_pool', 'thread_pool_attr',
                                                                       'enable_lake_maintenance')}
            assert settings['enable_thread_pool'] == args.pool and settings['enable_lake_maintenance'] == args.maintenance, settings
            if args.pool == 'on':
                assert settings['thread_pool_attr'].split(',')[0].strip() == '4', settings
            result.update(settings=settings, postmaster_started=scalar(admin, 'SELECT pg_postmaster_start_time()'))
            query(admin, sql.SQL('CREATE ROLE {} NOLOGIN NOINHERIT PASSWORD {}').format(
                sql.Identifier(owner), sql.Literal('La1!' + secrets.token_hex(12))))
            result['created'].append(owner)
            encoded = 'md5' + hashlib.md5((role_pwd + reader).encode()).hexdigest()
            query(admin, sql.SQL('CREATE ROLE {} LOGIN ENCRYPTED PASSWORD {}').format(sql.Identifier(reader), sql.Literal(encoded)))
            result['created'].append(reader)
            for db in (db_old, db_new):
                query(admin, sql.SQL("CREATE DATABASE {} DBCOMPATIBILITY 'PG'").format(sql.Identifier(db)))
                result['created'].append(db)
            with closing(connect(db_old)) as older:
                stage('install_059', lambda: make_catalog(older, owner, output / 'data059', '0.59'))
                stage('upgrade_059_to_060', lambda: gate_and_upgrade(older))
            with closing(connect(db_new)) as conn:
                stage('fresh_install_060', lambda: make_catalog(conn, owner, output / 'data060'))
                if args.maintenance == 'on':
                    from worker_060 import check_worker
                    stage('maintenance_auto_compact', lambda: check_worker(conn, output / 'worker'))
                stage('persistent_options', lambda: check_options(conn))
                pair = stage('manual_merge_size_selection_rollback_history', lambda: check_merge(conn, output / 'data060'))
                stage('permission_and_bound_snapshot', lambda: check_permissions(conn, db_new, reader, role_pwd))
                if not args.manual_only:
                    from evolution_060 import check_delete_exclusion, check_evolution
                    stage('delete_exclusions', lambda: check_delete_exclusion(conn))
                    stage('partition_and_column_evolution', lambda: check_evolution(conn))
                    from oracle_060 import check_oracle
                    stage('pinned_ducklake', lambda: check_oracle(conn, output / 'reference', output / 'data060', {'merge_lane': pair}))
            # Only remove the two databases/roles actually created by this run.
            # A failed stage deliberately leaves its isolated database and files for diagnosis.
            for db in (db_old, db_new):
                query(admin, sql.SQL('DROP DATABASE {}').format(sql.Identifier(db)))
            for role in (reader, owner):
                query(admin, sql.SQL('DROP ROLE {}').format(sql.Identifier(role)))
            result.update(status='manual_subset_passed' if args.manual_only else 'passed',
                          cleanup='owned databases and roles removed; file/reference evidence retained')
    except BaseException as error:
        result.update(status='failed', error=repr(error), traceback=traceback.format_exc(),
                      cleanup='failure scene retained; connections closed')
        raise
    finally:
        result['finished_at'] = now()
        write_json(resultfile, result)


if __name__ == '__main__':
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--build', type=Path, required=True)
    ap.add_argument('--output', type=Path, required=True)
    ap.add_argument('--pool', choices=('on', 'off'), required=True)
    ap.add_argument('--maintenance', choices=('on', 'off'), required=True)
    ap.add_argument('--manual-only', action='store_true', help='Development smoke test; never qualifies the full suite')
    run(ap.parse_args())