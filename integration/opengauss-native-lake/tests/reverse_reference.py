#!/usr/bin/env python3
"""Read actual native INSERT publications with the exact standalone reference.

This is an offline test adapter, not a live external-writer connector. It reads
the host in one RR/read-only transaction, restores its exact values into an
owned copy of the genuine reference catalog, and never repairs protocol data.
"""
import argparse
from collections import Counter, defaultdict
import fcntl
import json
import math
import os
from pathlib import Path
import shutil
import subprocess
import sys
import traceback

DRIVER = Path(__file__).resolve().parents[1]
ROOT = DRIVER.parents[1]
sys.path.insert(0, str(DRIVER))
from inline_fixture import checked_export, multiset, normalize, query, scalar
from lake_harness import Blocked, read_json, sha256, write_json

HOST_OIDS = {'BOOLEAN': {16}, 'INTEGER': {23}, 'BIGINT': {20},
             'VARCHAR': {25, 1043}, 'UUID': {2950},
             'TIMESTAMP WITH TIME ZONE': {1184}, 'TIMESTAMP': {1114},
             'DOUBLE': {701}, 'FLOAT': {700}}


def qi(value):
    return '"' + str(value).replace('"', '""') + '"'


def literal(value, type_name=None):
    if value is None:
        return 'NULL'
    if isinstance(value, bool):
        return 'true' if value else 'false'
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if not math.isfinite(value):
            raise Blocked('Reverse adapter does not transport nonfinite floating values')
        return repr(value)
    if not isinstance(value, str) or '\0' in value:
        raise Blocked('Reverse adapter requires an explicitly supported scalar value')
    quoted = "'" + value.replace("'", "''") + "'"
    if type_name in ('UUID', 'TIMESTAMP', 'TIMESTAMP WITH TIME ZONE'):
        return 'CAST(' + quoted + ' AS ' + type_name + ')'
    return quoted


def normalized_rows(table):
    return [[normalize(row[c['column_name']], c['data_type']) for c in table['columns']]
            for row in table['rows']]


def table_map(exported):
    return {table['table_name']: table for table in exported['tables']}


def export_host(conn, schema, original, output):
    """Only begin from an idle connection; never commit the caller's work."""
    from psycopg2 import sql
    from psycopg2.extensions import TRANSACTION_STATUS_IDLE
    if conn.get_transaction_status() != TRANSACTION_STATUS_IDLE:
        raise ValueError('Commit the native INSERT before reverse export; connection must be idle')
    tables = []
    query(conn, 'BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY')
    try:
        status = json.loads(scalar(conn, 'SELECT og_lake.host_status()'))
        if status.get('inline_insert') is not True:
            raise Blocked('Installed product does not expose native inline INSERT')
        relations = query(conn, "SELECT c.relname,c.relkind FROM pg_class c JOIN pg_namespace n "
                          "ON n.oid=c.relnamespace WHERE n.nspname=%s AND c.relkind IN ('r','f','v','m','p')",
                          (schema,))
        if dict(relations) != {t['table_name']: 'r' for t in original['tables']}:
            raise ValueError('Host physical catalog table set differs from the genuine fixture')
        for source in original['tables']:
            columns = source['columns']
            attributes = query(conn, "SELECT a.attname,a.atttypid FROM pg_attribute a JOIN pg_class c "
                               "ON c.oid=a.attrelid JOIN pg_namespace n ON n.oid=c.relnamespace "
                               "WHERE n.nspname=%s AND c.relname=%s AND a.attnum>0 "
                               "AND NOT a.attisdropped ORDER BY a.attnum", (schema, source['table_name']))
            if len(attributes) != len(columns) or any(
                    name != col['column_name'] or oid not in HOST_OIDS[col['data_type']]
                    for (name, oid), col in zip(attributes, columns)):
                raise ValueError('Host catalog shape changed: ' + source['table_name'])
            rows = query(conn, sql.SQL('SELECT {} FROM {}.{}').format(
                sql.SQL(',').join(sql.Identifier(c['column_name']) for c in columns),
                sql.Identifier(schema), sql.Identifier(source['table_name'])))
            table = dict(source)
            table['rows'] = [{c['column_name']: normalize(v, c['data_type'])
                              for c, v in zip(columns, row)} for row in rows]
            tables.append(table)
        exported = {'scope': 'actual_committed_og_catalog_read_only_export',
                    'host_schema': schema, 'host_capabilities': status,
                    'host_version': scalar(conn, 'SELECT version()'),
                    'host_database': scalar(conn, 'SELECT current_database()'),
                    'isolation': 'REPEATABLE READ READ ONLY', 'tables': tables}
        query(conn, 'ROLLBACK')
    except BaseException:
        query(conn, 'ROLLBACK')
        raise
    write_json(output, exported)
    return exported


class Reference:
    def __init__(self, output, lock, receipt):
        self.output, self.lock, self.receipt = output, lock, receipt
        self.binary = ROOT / '.reference/opengauss-native-lake' / lock['reference_build']['build_subdir'] / 'duckdb'
        self.counter = 0

    def run(self, sql, name, catalog=None, readonly=False, result=False):
        self.counter += 1
        stem = self.output / ('%03d-' % self.counter + name)
        stem.with_suffix('.sql').write_text(sql + '\n')
        argv = [str(self.binary), '-no-init', '-batch', '-json']
        if readonly:
            argv.append('-readonly')
        if catalog:
            argv.append(str(catalog))
        process = subprocess.run(argv, input=sql, text=True, stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE, timeout=180)
        stem.with_suffix('.stdout').write_text(process.stdout)
        stem.with_suffix('.stderr').write_text(process.stderr)
        write_json(stem.with_suffix('.process.json'), {'argv': argv, 'returncode': process.returncode})
        if process.returncode:
            raise RuntimeError('Reference command failed: ' + str(stem.with_suffix('.stderr')))
        return (json.loads(process.stdout) if process.stdout.strip() else []) if result else None

    def attach(self, catalog, readonly=False):
        # Keep standard data_path unchanged. Admitted cases are entirely inline;
        # the reference runtime limit ensures the tiny continuation stays inline.
        options = "DUCKLAKE_VERSION '1.1', AUTOMATIC_MIGRATION FALSE"
        options += ', READ_ONLY' if readonly else ', DATA_INLINING_ROW_LIMIT 1000000'
        return 'ATTACH ' + literal('ducklake:' + str(catalog)) + ' AS lake (' + options + ');\n'

    def logical(self, catalog, table, name):
        return self.run(self.attach(catalog, True) + 'SELECT rowid AS logical_rowid,snapshot_id AS begin_snapshot,* FROM '
                        + table + ' ORDER BY rowid', name, result=True)

    def metadata(self, catalog, definitions, name):
        result = []
        for source in definitions:
            table = dict(source)
            table['rows'] = self.run('SELECT * FROM ' + qi(source['schema_name']) + '.' + qi(source['table_name']),
                                     name + '-' + source['table_name'], catalog, True, True)
            result.append(table)
        return {'tables': result}

    def insert(self, catalog, relation, schema, rows, name):
        if not rows or len(rows) > 1000000:
            raise Blocked('Reference control batch must contain 1..1000000 inline input rows')
        names = [c['name'] for c in schema]
        values = ','.join('(' + ','.join(literal(row[c['name']], c['type']) for c in schema) + ')' for row in rows)
        self.run(self.attach(catalog) + 'INSERT INTO ' + relation + '(' + ','.join(map(qi, names))
                 + ') VALUES ' + values + ';', name)

    def restore(self, catalog, exported):
        statements = ['BEGIN;']
        for table in exported['tables']:
            full = qi(table['schema_name']) + '.' + qi(table['table_name'])
            statements.append('DELETE FROM ' + full + ';')
            cols = table['columns']
            for offset in range(0, len(table['rows']), 250):
                values = ','.join('(' + ','.join(literal(row[c['column_name']], c['data_type']) for c in cols)
                                  + ')' for row in table['rows'][offset:offset + 250])
                statements.append('INSERT INTO ' + full + '(' + ','.join(qi(c['column_name']) for c in cols)
                                  + ') VALUES ' + values + ';')
        statements.append('COMMIT; CHECKPOINT;')
        self.run('\n'.join(statements), 'restore-host-values', catalog)


def reverse_reference(conn, schema, source_manifest_path, original_export_path,
                      expected_records, output_dir, *, expected_commit_count=None):
    """Return/write result.json; errors retain evidence and are re-raised.

    Caller must have committed real native INSERTs and own its temporary DB.
    The helper does not create/drop host objects, issue host DML, or change the
    source reference fixture. Only simple INSERT on its single table is admitted.
    """
    output = Path(output_dir).resolve()
    output.mkdir(parents=True, exist_ok=False)
    result = {'status': 'fail', 'scope': 'offline_native_insert_reference_interread',
              'assertions': [], 'limitations': [
                  'Not a concurrent DuckDB/openGauss shared-catalog connector.',
                  'No Parquet SDK, UPDATE/DELETE, flush, RAC, or production qualification is implied.',
                  'The reference copies are test-owned; standard row/ID/version/stat values are never repaired.']}

    def check(name, observed, expected=True):
        ok = observed == expected
        result['assertions'].append({'name': name, 'status': 'pass' if ok else 'fail',
                                     'observed': observed, 'expected': expected})
        if not ok:
            raise AssertionError(name)

    mutex = None
    source_files = []
    try:
        before, manifest = checked_export(original_export_path, source_manifest_path, DRIVER)
        if sha256(before['catalog']) != before['catalog_sha256'] or Path(before['catalog'] + '.wal').exists():
            raise ValueError('Reference source catalog changed or has an uncheckpointed WAL')
        lock_hash = sha256(DRIVER / 'source-lock.json')
        lock = read_json(DRIVER / 'source-lock.json')
        receipt = read_json(ROOT / '.reference/opengauss-native-lake/build/receipt.json')
        ref = Reference(output, lock, receipt)
        if receipt['source_lock_hash'] != lock_hash or sha256(ref.binary) != receipt['binary_sha256']:
            raise Blocked('Reference receipt/binary is stale')
        if manifest['reference_commit'] != lock['repositories']['ducklake']['commit'] or \
                manifest['duckdb_commit'] != lock['repositories']['duckdb']['commit']:
            raise ValueError('Reference manifest source commits differ from the lock')
        mutex = (ROOT / '.reference/opengauss-native-lake/.driver.lock').open('a+')
        try:
            fcntl.flock(mutex, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise Blocked('Another reference driver is running') from exc
        result['provenance'] = {'source_lock_hash': lock_hash, 'binary_sha256': sha256(ref.binary),
                                'manifest': str(Path(source_manifest_path).resolve()),
                                'manifest_sha256': sha256(source_manifest_path),
                                'original_export_sha256': sha256(original_export_path),
                                'helper_sha256': sha256(__file__)}
        versions = ref.run('PRAGMA version;', 'runtime-version', result=True)
        extensions = ref.run("SELECT extension_version,loaded,install_mode FROM duckdb_extensions() "
                             "WHERE extension_name='ducklake'", 'runtime-extension', result=True)
        check('runtime DuckDB source', versions[0]['source_id'], manifest['duckdb_commit'][:10])
        check('runtime DuckLake source', extensions[0], {'extension_version': manifest['reference_commit'][:10],
                                                       'loaded': True, 'install_mode': 'STATICALLY_LINKED'})
        originals = table_map(before)
        for name in ('ducklake_data_file', 'ducklake_delete_file'):
            if originals[name]['rows']:
                raise Blocked('Reverse first slice requires an entirely inline source catalog')
        source_files = [(item['path'], item['sha256']) for item in manifest['files']]
        host = export_host(conn, schema, before, output / 'host-catalog.json')
        host_export_sha = sha256(output / 'host-catalog.json')
        result['provenance']['host_export_sha256'] = host_export_sha
        actual = table_map(host)
        # A table/schema override could force Parquet into the unchanged source
        # data_path. This offline first-slice test refuses that configuration.
        for row in actual['ducklake_metadata']['rows']:
            if row['key'] == 'data_inlining_row_limit' and row['scope'] is not None:
                raise Blocked('Reverse inline adapter does not admit scoped data-inlining limits')
        table_id = manifest['table_id']
        names = [c['name'] for c in manifest['schema']]
        expected = [{name: row[name] for name in names} for row in expected_records]
        snapshots_before = originals['ducklake_snapshot']['rows']
        previous = max(snapshots_before, key=lambda row: row['snapshot_id'])
        snapshots_after = actual['ducklake_snapshot']['rows']
        head = max(snapshots_after, key=lambda row: row['snapshot_id'])
        prior_after = dict(actual['ducklake_snapshot'], rows=[r for r in snapshots_after
                           if r['snapshot_id'] <= previous['snapshot_id']])
        check('prior snapshot rows including timestamps are immutable',
              dict(multiset(normalized_rows(prior_after))),
              dict(multiset(normalized_rows(originals['ducklake_snapshot']))))
        additions = sorted((r for r in snapshots_after if r['snapshot_id'] > previous['snapshot_id']),
                           key=lambda r: r['snapshot_id'])
        check('at least one real native snapshot committed', bool(additions))
        check('native snapshots contain actual timestamps', all(r['snapshot_time'] is not None for r in additions))
        if expected_commit_count is not None:
            check('native commit count', len(additions), expected_commit_count)
        check('contiguous committed snapshot IDs', [r['snapshot_id'] for r in additions],
              list(range(previous['snapshot_id'] + 1, head['snapshot_id'] + 1)))
        mapping = [r for r in actual['ducklake_inlined_data_tables']['rows'] if r['table_id'] == table_id]
        check('one current inline carrier', len(mapping), 1)
        inline_name = mapping[0]['table_name']
        old_physical = originals[inline_name]['rows']
        new_physical = actual[inline_name]['rows']
        old_set = multiset(old_physical)
        check('prior inline versions preserved exactly', not bool(old_set - multiset(new_physical)))
        old_encoded = set(old_set)
        inserted = [r for r in new_physical if json.dumps(r, ensure_ascii=False, sort_keys=True,
                    separators=(',', ':')) not in old_encoded]
        groups = defaultdict(list)
        for row in inserted:
            check('new row is an unexpired INSERT version', row['_ducklake_end_snapshot'], None)
            groups[row['_ducklake_begin_snapshot']].append(row)
        check('every new snapshot owns a nonempty INSERT batch', sorted(groups), [r['snapshot_id'] for r in additions])
        prior_stats = [r for r in originals['ducklake_table_stats']['rows'] if r['table_id'] == table_id]
        check('one original row allocation frontier', len(prior_stats), 1)
        frontier = prior_stats[0]['next_row_id']
        check('native row IDs continue the original frontier without overlap',
              sorted(r['_ducklake_row_id'] for r in inserted), list(range(frontier, frontier + len(inserted))))
        live = [r for r in new_physical if r['_ducklake_begin_snapshot'] <= head['snapshot_id'] and
                (r['_ducklake_end_snapshot'] is None or r['_ducklake_end_snapshot'] > head['snapshot_id'])]
        check('host published business multiset equals caller expectation',
              dict(multiset([{name: r[name] for name in names} for r in live])), dict(multiset(expected)))
        check('visible logical row IDs are unique', len({r['_ducklake_row_id'] for r in live}), len(live))
        roundtrip, control = output / 'native-result.ducklake', output / 'reference-control.ducklake'
        for path in (roundtrip, control):
            shutil.copyfile(before['catalog'], path)
        ref.restore(roundtrip, host)
        restored = ref.metadata(roundtrip, before['tables'], 'restored')
        for table in restored['tables']:
            check('exact transport/' + table['table_name'], dict(multiset(normalized_rows(table))),
                  dict(multiset(normalized_rows(actual[table['table_name']]))))
        definition = next(r for r in originals['ducklake_table']['rows'] if r['table_id'] == table_id and r['end_snapshot'] is None)
        schema_row = next(r for r in originals['ducklake_schema']['rows'] if r['schema_id'] == definition['schema_id'] and r['end_snapshot'] is None)
        relation = 'lake.' + qi(schema_row['schema_name']) + '.' + qi(definition['table_name'])
        observed = ref.logical(roundtrip, relation, 'read-native-publication')
        expected_logical = [dict({name: r[name] for name in names}, logical_rowid=r['_ducklake_row_id'],
                                 begin_snapshot=r['_ducklake_begin_snapshot']) for r in live]
        check('DuckLake reads complete native business values and stable identities',
              dict(multiset(observed)), dict(multiset(expected_logical)))
        count = ref.run(ref.attach(roundtrip, True) + 'SELECT count(*) AS n FROM ' + relation,
                        'reference-visible-count', result=True)
        check('reference COUNT uses visible rows', count, [{'n': len(expected)}])
        # Retain the exact verified native publication before the later legal
        # reference INSERT changes the disposable roundtrip copy.
        native_publication = output / 'native-publication.ducklake'
        if Path(str(roundtrip) + '.wal').exists():
            raise ValueError('Native publication copy unexpectedly has an uncheckpointed WAL')
        shutil.copyfile(roundtrip, native_publication)
        native_publication_sha = sha256(native_publication)
        for snapshot, rows in sorted(groups.items()):
            ref.insert(control, relation, manifest['schema'], sorted(rows, key=lambda r: r['_ducklake_row_id']),
                       'control-insert-' + str(snapshot))
        control_meta = ref.metadata(control, before['tables'], 'control')
        for table in control_meta['tables']:
            name = table['table_name']
            cols = [c for c in table['columns'] if name != 'ducklake_snapshot' or c['column_name'] != 'snapshot_time']
            control_table = dict(table, columns=cols)
            host_table = dict(actual[name], columns=cols)
            check('standard catalog differential/' + name, dict(multiset(normalized_rows(host_table))),
                  dict(multiset(normalized_rows(control_table))))
        # A single deterministic business row is legal SQL, never a metadata edit.
        occupied = {r['id'] for r in expected}
        identifier = 1
        while identifier in occupied:
            identifier += 1
        if identifier > 2147483647:
            raise Blocked('No supported continuation integer ID')
        continuation = {}
        for col in manifest['schema']:
            kind = col['type']
            continuation[col['name']] = ('reference continuation 湖' if kind == 'VARCHAR' else
                                         True if kind == 'BOOLEAN' else 42)
        continuation['id'] = identifier
        ref.insert(roundtrip, relation, manifest['schema'], [continuation], 'reference-continue-native-result')
        after = ref.logical(roundtrip, relation, 'read-reference-continuation')
        stats = next(r for r in actual['ducklake_table_stats']['rows'] if r['table_id'] == table_id)
        expected_after = expected_logical + [dict(continuation, logical_rowid=stats['next_row_id'],
                                                  begin_snapshot=head['snapshot_id'] + 1)]
        check('reference can continue native allocation', dict(multiset(after)), dict(multiset(expected_after)))
        after_head = ref.run('SELECT * FROM ducklake_snapshot ORDER BY snapshot_id DESC LIMIT 1',
                             'continued-snapshot', roundtrip, True, True)[0]
        for field, increment in [('snapshot_id', 1), ('next_file_id', 1), ('next_catalog_id', 0), ('schema_version', 0)]:
            check('reference continuation/' + field, after_head[field], head[field] + increment)
        after_stats = ref.run('SELECT * FROM ducklake_table_stats WHERE table_id=' + str(table_id),
                              'continued-stats', roundtrip, True, True)[0]
        check('reference continuation/next_row_id', after_stats['next_row_id'], stats['next_row_id'] + 1)
        check('reference continuation/record_count', after_stats['record_count'],
              None if stats['record_count'] is None else stats['record_count'] + 1)
        check('reference continuation/file_size_bytes', after_stats['file_size_bytes'], stats['file_size_bytes'])
        check('source lock stayed frozen', sha256(DRIVER / 'source-lock.json'), lock_hash)
        check('reference binary stayed frozen', sha256(ref.binary), receipt['binary_sha256'])
        check('original host export was not rewritten', sha256(output / 'host-catalog.json'), host_export_sha)
        check('verified native publication was not rewritten', sha256(native_publication), native_publication_sha)
        for path, digest in source_files:
            check('source artifact unchanged/' + Path(path).name, sha256(path), digest)
        result.update(status='pass', inserted_rows=len(inserted), committed_snapshots=len(additions),
                      continuation=continuation, host_export_sha256=sha256(output / 'host-catalog.json'),
                      native_publication_catalog_sha256=native_publication_sha,
                      native_result_catalog_sha256=sha256(roundtrip), control_catalog_sha256=sha256(control))
        write_json(output / 'result.json', result)
        return result
    except BaseException as exc:
        result.update(status='blocked' if isinstance(exc, Blocked) else 'fail', reason=str(exc),
                      traceback=traceback.format_exc())
        write_json(output / 'result.json', result)
        raise
    finally:
        if mutex:
            mutex.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--manifest', required=True)
    parser.add_argument('--original-export', required=True)
    parser.add_argument('--expected', required=True, help='JSON array of expected committed business records')
    parser.add_argument('--output-dir', required=True, help='A new evidence subdirectory; never overwritten')
    parser.add_argument('--database', required=True)
    parser.add_argument('--schema', default='lake_metadata')
    parser.add_argument('--host', default=str(ROOT / '.runtime/opengauss-native-lake/debug/socket'))
    parser.add_argument('--port', type=int, default=55560)
    parser.add_argument('--user', default=os.getenv('USER', 'qiumc'))
    parser.add_argument('--expected-commit-count', type=int)
    args = parser.parse_args()
    connection = None
    try:
        import psycopg2
        connection = psycopg2.connect(host=args.host, port=args.port, dbname=args.database,
                                      user=args.user, connect_timeout=10, application_name='lake_reverse_reference')
        connection.autocommit = True
        reverse_reference(connection, args.schema, args.manifest, args.original_export,
                          read_json(args.expected), args.output_dir, expected_commit_count=args.expected_commit_count)
        return 0
    except Blocked as exc:
        print(str(exc), file=sys.stderr)
        return 2
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        return 1
    finally:
        if connection:
            connection.close()


if __name__ == '__main__':
    raise SystemExit(main())
