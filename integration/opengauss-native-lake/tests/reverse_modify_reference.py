#!/usr/bin/env python3
"""Staged N4 offline differential; this helper has not run product/reference SQL.

Reuses the frozen transport primitives without changing the N3 helper. The
control executes the caller's real business DML in independent transactions;
it never infers UPDATE/DELETE from, or repairs, native protocol records.
"""
import fcntl
import json
import os
from pathlib import Path
import re
import shutil
import sys
import traceback

DRIVER = Path(os.getenv('OG_LAKE_DRIVER', '/home/qiumc/data/code/cpp/integration/opengauss-native-lake')).resolve()
sys.path.insert(0, str(DRIVER / 'tests'))
from reverse_reference import (ROOT, Reference, checked_export, export_host, literal,
                               multiset, normalized_rows, qi, read_json, sha256,
                               table_map, write_json, Blocked)


def reference_returning_clause(statement):
    """Remove only a terminal, supported plain-column RETURNING projection.

    This scanner only recognizes standard SQL quotes and parentheses; escaped
    and dollar-quoted SQL literals are outside the fixture grammar (use params).
    A RETURNING word inside a quoted literal/identifier is never a clause.
    """
    returning = []
    depth = 0
    index = 0
    while index < len(statement):
        ch = statement[index]
        if ch in ("'", '"'):
            quote = ch
            index += 1
            while index < len(statement):
                if statement[index] == "\\":
                    raise ValueError('Use bound params instead of backslash-escaped SQL literals')
                if statement[index] == quote:
                    if index + 1 < len(statement) and statement[index + 1] == quote:
                        index += 2
                        continue
                    index += 1
                    break
                index += 1
            else:
                raise ValueError('Unclosed SQL quote is outside the fixture grammar')
            continue
        if ch == '$':
            raise ValueError('Use bound params instead of dollar-quoted SQL literals')
        if ch == '(':
            depth += 1
        elif ch == ')':
            depth -= 1
            if depth < 0:
                raise ValueError('Unbalanced SQL parentheses')
        elif ch.isalpha() or ch == '_':
            end = index + 1
            while end < len(statement) and (statement[end].isalnum() or statement[end] in '_$'):
                end += 1
            if statement[index:end].lower() == 'returning':
                returning.append((index, end, depth))
            index = end
            continue
        index += 1
    if depth:
        raise ValueError('Unbalanced SQL parentheses')
    if not returning:
        return statement, []
    if len(returning) != 1 or returning[0][2] != 0:
        raise ValueError('Only one top-level terminal RETURNING clause is supported')
    start, end, _ = returning[0]
    suffix = statement[end:].strip()
    if not re.fullmatch(r'id\s*,\s*val(?:\s*,\s*flag\s*,\s*bigval\s*,\s*note)?', suffix, re.I):
        raise ValueError('RETURNING supports only id,val or id,val,flag,bigval,note with no expressions/aliases')
    # Preserve the entire business-statement prefix exactly, including spaces.
    return statement[:start], [name.strip().lower() for name in suffix.split(',')]


def render_operation(operation, relation, index, audit=None):
    """One controlled DML statement; parameters are SQL literals for EXECUTE.

    sql uses {table} and DuckDB ? parameters, e.g.
    {'sql': 'UPDATE {table} SET val = ? WHERE id = ?', 'params': [15, 1]}.
    This deliberately supports a smaller grammar than arbitrary SQL scripts.
    All transaction control and attachment are owned by this helper.
    """
    if not isinstance(operation, dict) or set(operation) - {'sql', 'params'}:
        raise ValueError('Operation must contain only sql and optional params')
    statement = operation.get('sql')
    params = operation.get('params', [])
    if not isinstance(statement, str) or not isinstance(params, (list, tuple)):
        raise ValueError('Operation sql/params types are invalid')
    if statement.count('{table}') != 1 or any(t in statement for t in (';', '--', '/*', '*/')):
        raise ValueError('One {table} target and one comment-free statement are required')
    # A complete DELETE needs no token after its target. Keep the exact
    # single-target spelling; do not admit target suffixes or extra statements.
    plain_delete = re.fullmatch(r'\s*DELETE\s+FROM\s+\{table\}\s*', statement, re.I) is not None
    if not plain_delete and not re.match(r'^\s*(?:INSERT\s+INTO|UPDATE|DELETE\s+FROM)\s+\{table\}(?:\s|\()', statement, re.I):
        raise ValueError('Only business INSERT/UPDATE/DELETE against {table} is admitted')
    if re.search(r'\bducklake_[a-z_]+\b', statement, re.I):
        raise ValueError('Control operations cannot target standard metadata')
    reference_statement, returning = reference_returning_clause(statement)
    rendered = reference_statement.replace('{table}', relation)
    if audit is not None:
        audit.append({'operation_index': index, 'original_sql': statement,
                      'reference_sql': reference_statement, 'params': list(params),
                      'reference_target': relation, 'removed_returning_columns': returning,
                      'returning_validation': 'native oracle only; reference does not validate RETURNING'})
    # PREPARE binds actual DuckDB parameters. Do not substitute ? textually:
    # quoted string contents and identifiers must retain their exact meaning.
    name = 'n4_operation_' + str(index)
    invocation = name + ('(' + ','.join(literal(v) for v in params) + ')' if params else '')
    return 'PREPARE ' + name + ' AS ' + rendered + ';\nEXECUTE ' + invocation + ';\nDEALLOCATE ' + name + ';'


def reverse_modify_reference(conn, schema, source_manifest_path, original_export_path,
                             expected_records, output_dir, *, operations_by_commit,
                             expected_commit_snapshots=None):
    """Inspect a committed owned OG database; modify only owned reference copies.

    operations_by_commit is a list of operation lists, in actual native commit
    order. Net-empty commits are valid. expected_commit_snapshots optionally
    contains the observed native MAX(snapshot_id) after each group, including
    repeated IDs for commits that published nothing. These are checked against
    actual reference observations, never used to alter reference metadata.
    """
    output = Path(output_dir).resolve()
    output.mkdir(parents=True, exist_ok=False)
    result = {'status': 'fail', 'scope': 'offline_native_inline_modify_reference_interread',
              'assertions': [], 'limitations': [
                  'Independent offline copies, not concurrent shared-catalog interoperability.',
                  'Single existing inline table; no file, SDK, flush, schema evolution, or RAC qualification.',
                  'Reference replays actual business statements; native protocol values are never repaired.',
                  'Only snapshot_time is excluded from cross-engine comparison; transport preserves it.',
                  'Terminal plain-column RETURNING is omitted in reference DML; native oracle validates returned values.']}
    mutex = None

    def check(name, observed, expected=True):
        ok = observed == expected
        result['assertions'].append({'name': name, 'status': 'pass' if ok else 'fail',
                                     'observed': observed, 'expected': expected})
        if not ok:
            raise AssertionError(name)

    try:
        if not isinstance(operations_by_commit, list) or any(not isinstance(g, list) for g in operations_by_commit):
            raise ValueError('operations_by_commit must be a list of operation lists')
        if expected_commit_snapshots is not None and len(expected_commit_snapshots) != len(operations_by_commit):
            raise ValueError('Expected per-commit snapshots must cover every operation group')
        before, manifest = checked_export(original_export_path, source_manifest_path, DRIVER)
        if sha256(before['catalog']) != before['catalog_sha256'] or Path(before['catalog'] + '.wal').exists():
            raise ValueError('Original catalog is changed or uncheckpointed')
        lock_hash = sha256(DRIVER / 'source-lock.json')
        lock = read_json(DRIVER / 'source-lock.json')
        receipt = read_json(ROOT / '.reference/opengauss-native-lake/build/receipt.json')
        ref = Reference(output, lock, receipt)
        if receipt['source_lock_hash'] != lock_hash or sha256(ref.binary) != receipt['binary_sha256']:
            raise Blocked('Reference receipt/binary is stale')
        if manifest['reference_commit'] != lock['repositories']['ducklake']['commit'] or manifest['duckdb_commit'] != lock['repositories']['duckdb']['commit']:
            raise ValueError('Manifest source commits differ from the frozen lock')
        mutex = (ROOT / '.reference/opengauss-native-lake/.driver.lock').open('a+')
        try:
            fcntl.flock(mutex, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise Blocked('Another reference driver is running') from exc
        transport_path = DRIVER / 'tests/reverse_reference.py'
        result['provenance'] = {'source_lock_hash': lock_hash, 'binary_sha256': sha256(ref.binary),
                                'manifest_sha256': sha256(source_manifest_path),
                                'original_export_sha256': sha256(original_export_path),
                                'helper_sha256': sha256(__file__), 'transport_sha256': sha256(transport_path)}
        write_json(output / 'operations.json', operations_by_commit)
        versions = ref.run('PRAGMA version;', 'runtime-version', result=True)
        extensions = ref.run("SELECT extension_version,loaded,install_mode FROM duckdb_extensions() WHERE extension_name='ducklake'", 'runtime-extension', result=True)
        check('actual DuckDB source', versions[0]['source_id'], manifest['duckdb_commit'][:10])
        check('actual DuckLake source', extensions[0], {'extension_version': manifest['reference_commit'][:10], 'loaded': True, 'install_mode': 'STATICALLY_LINKED'})
        originals = table_map(before)
        host = export_host(conn, schema, before, output / 'host-catalog.json')
        host_hash = sha256(output / 'host-catalog.json')
        actual = table_map(host)
        for name in ('ducklake_data_file', 'ducklake_delete_file'):
            check('entirely inline source/' + name, originals[name]['rows'], [])
            check('entirely inline native result/' + name, actual[name]['rows'], [])
        for metadata in (originals, actual):
            for row in metadata['ducklake_metadata']['rows']:
                if row['key'] == 'data_inlining_row_limit' and row['scope'] is not None:
                    raise Blocked('Scoped inlining limits could write outside the owned reference copy')
        table_id = manifest['table_id']
        definition = next(r for r in originals['ducklake_table']['rows'] if r['table_id'] == table_id and r['end_snapshot'] is None)
        schema_row = next(r for r in originals['ducklake_schema']['rows'] if r['schema_id'] == definition['schema_id'] and r['end_snapshot'] is None)
        relation = 'lake.' + qi(schema_row['schema_name']) + '.' + qi(definition['table_name'])
        mapping = [r for r in actual['ducklake_inlined_data_tables']['rows'] if r['table_id'] == table_id]
        check('one current inline carrier', len(mapping), 1)
        inline_name = mapping[0]['table_name']
        names = [c['name'] for c in manifest['schema']]
        expected = [{name: row[name] for name in names} for row in expected_records]
        initial_head = max(r['snapshot_id'] for r in originals['ducklake_snapshot']['rows'])
        snapshots = actual['ducklake_snapshot']['rows']
        head = max(snapshots, key=lambda r: r['snapshot_id'])
        prior = dict(actual['ducklake_snapshot'], rows=[r for r in snapshots if r['snapshot_id'] <= initial_head])
        check('prior snapshots including timestamps immutable', dict(multiset(normalized_rows(prior))), dict(multiset(normalized_rows(originals['ducklake_snapshot']))))
        additions = sorted(r['snapshot_id'] for r in snapshots if r['snapshot_id'] > initial_head)
        check('published snapshot sequence has no gaps', additions, list(range(initial_head + 1, head['snapshot_id'] + 1)))
        check('new snapshots carry real timestamps', all(r['snapshot_time'] is not None for r in snapshots if r['snapshot_id'] > initial_head))
        physical = actual[inline_name]['rows']

        def visible(snapshot):
            return [r for r in physical if r['_ducklake_begin_snapshot'] <= snapshot and
                    (r['_ducklake_end_snapshot'] is None or snapshot < r['_ducklake_end_snapshot'])]

        def logical(rows):
            return [dict({n: r[n] for n in names}, logical_rowid=r['_ducklake_row_id'], begin_snapshot=r['_ducklake_begin_snapshot']) for r in rows]

        live = visible(head['snapshot_id'])
        check('native live business values', dict(multiset([{n: r[n] for n in names} for r in live])), dict(multiset(expected)))
        check('native live identities unique', len({r['_ducklake_row_id'] for r in live}), len(live))
        roundtrip, control = output / 'native-result.ducklake', output / 'reference-control.ducklake'
        for path in (roundtrip, control):
            shutil.copyfile(before['catalog'], path)
        ref.restore(roundtrip, host)
        restored = ref.metadata(roundtrip, before['tables'], 'restored')
        for table in restored['tables']:
            check('exact transport/' + table['table_name'], dict(multiset(normalized_rows(table))), dict(multiset(normalized_rows(actual[table['table_name']]))))
        for sid in sorted(r['snapshot_id'] for r in snapshots if r['snapshot_id'] >= definition['begin_snapshot']):
            rows = visible(sid)
            check('unique logical IDs at snapshot/' + str(sid), len({r['_ducklake_row_id'] for r in rows}), len(rows))
            observed = ref.run(ref.attach(roundtrip, True) + 'SELECT rowid AS logical_rowid,snapshot_id AS begin_snapshot,* FROM ' + relation + ' AT (VERSION => ' + str(sid) + ') ORDER BY rowid;', 'read-history-' + str(sid), result=True)
            check('DuckLake native history/' + str(sid), dict(multiset(observed)), dict(multiset(logical(rows))))
        check('reference visible COUNT', ref.run(ref.attach(roundtrip, True) + 'SELECT count(*) AS n FROM ' + relation, 'visible-count', result=True), [{'n': len(expected)}])
        native_publication = output / 'native-publication.ducklake'
        if Path(str(roundtrip) + '.wal').exists():
            raise ValueError('Restored native publication has an uncheckpointed WAL')
        shutil.copyfile(roundtrip, native_publication)
        publication_hash = sha256(native_publication)
        observations = []
        for group_index, group in enumerate(operations_by_commit):
            commands = [ref.attach(control), 'BEGIN;']
            operation_audit = []
            commands.extend(render_operation(operation, relation, index, operation_audit)
                            for index, operation in enumerate(group))
            write_json(output / ('replay-commit-' + str(group_index) + '-operations.json'), operation_audit)
            commands.append('COMMIT;')
            ref.run('\n'.join(commands), 'replay-commit-' + str(group_index))
            snapshot = ref.run('SELECT * FROM ducklake_snapshot ORDER BY snapshot_id DESC LIMIT 1', 'head-after-commit-' + str(group_index), control, True, True)[0]
            observations.append(snapshot)
            if expected_commit_snapshots is not None:
                check('actual native/reference commit head/' + str(group_index), snapshot['snapshot_id'], expected_commit_snapshots[group_index])
        write_json(output / 'reference-commit-observations.json', observations)
        control_meta = ref.metadata(control, before['tables'], 'control')
        for table in control_meta['tables']:
            name = table['table_name']
            cols = [c for c in table['columns'] if name != 'ducklake_snapshot' or c['column_name'] != 'snapshot_time']
            check('complete standard catalog differential/' + name,
                  dict(multiset(normalized_rows(dict(actual[name], columns=cols)))),
                  dict(multiset(normalized_rows(dict(table, columns=cols)))))
        occupied = {r['id'] for r in expected}
        identifier = 1
        while identifier in occupied:
            identifier += 1
        if identifier > 2147483647:
            raise Blocked('No supported continuation integer ID')
        continuation = {c['name']: ('reference continuation 湖' if c['type'] == 'VARCHAR' else True if c['type'] == 'BOOLEAN' else 42) for c in manifest['schema']}
        continuation['id'] = identifier
        ref.insert(roundtrip, relation, manifest['schema'], [continuation], 'reference-continues-after-modifications')
        stats = next(r for r in actual['ducklake_table_stats']['rows'] if r['table_id'] == table_id)
        check('reference continues native identities', dict(multiset(ref.logical(roundtrip, relation, 'read-continuation'))), dict(multiset(logical(live) + [dict(continuation, logical_rowid=stats['next_row_id'], begin_snapshot=head['snapshot_id'] + 1)])))
        after_stats = ref.run('SELECT * FROM ducklake_table_stats WHERE table_id=' + str(table_id), 'continued-stats', roundtrip, True, True)[0]
        check('continuation row frontier', after_stats['next_row_id'], stats['next_row_id'] + 1)
        check('continuation stored record count', after_stats['record_count'], None if stats['record_count'] is None else stats['record_count'] + 1)
        check('frozen lock', sha256(DRIVER / 'source-lock.json'), lock_hash)
        check('frozen binary', sha256(ref.binary), receipt['binary_sha256'])
        check('frozen transport helper', sha256(transport_path), result['provenance']['transport_sha256'])
        check('host export unchanged', sha256(output / 'host-catalog.json'), host_hash)
        check('native publication unchanged', sha256(native_publication), publication_hash)
        for item in manifest['files']:
            check('source unchanged/' + Path(item['path']).name, sha256(item['path']), item['sha256'])
        result.update(status='pass', committed_snapshots=len(additions), replayed_transactions=len(operations_by_commit),
                      per_commit_head_checks=expected_commit_snapshots is not None,
                      host_export_sha256=host_hash, native_publication_catalog_sha256=publication_hash,
                      native_result_catalog_sha256=sha256(roundtrip), control_catalog_sha256=sha256(control))
        write_json(output / 'result.json', result)
        return result
    except BaseException as exc:
        result.update(status='blocked' if isinstance(exc, Blocked) else 'fail', reason=str(exc), traceback=traceback.format_exc())
        write_json(output / 'result.json', result)
        raise
    finally:
        if mutex:
            mutex.close()
