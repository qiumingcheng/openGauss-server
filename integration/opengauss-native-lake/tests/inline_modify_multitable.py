#!/usr/bin/env python3
"""Staged genuine two-table N4 acceptance; not registered or executed.

Both physical carriers and their standard metadata must originate from actual
fixed-reference CREATE/INSERT operations. No test constructs a second table's
protocol records, guesses table IDs, or edits persistent reference values.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import copy
import json
import os
from pathlib import Path
import sys
import time
import traceback

DRIVER = Path(os.getenv('OG_LAKE_DRIVER_ROOT', '/home/qiumc/data/code/cpp/integration/opengauss-native-lake'))
sys.path.insert(0, str(DRIVER))
from inline_fixture import foreign_table_statement, multiset, query, scalar
from lake_harness import Blocked, validate_run_id, write_json
from native_test_context import OwnedDatabase, load_reference, reviewed_runtime_version


class TwoTables:
    def __init__(self, db, args, result):
        from psycopg2 import sql
        self.sql, self.result = sql, result
        self.a, self.b, self.c = [db.connect() for _ in range(3)]
        query(self.a, "CREATE EXTENSION og_lake VERSION '0.4'")
        capabilities = json.loads(scalar(self.a, 'SELECT og_lake.host_status()'))
        runtime_version = reviewed_runtime_version()
        if runtime_version not in ('0.4', '0.5', '0.6', '0.7', '0.8', '0.9', '0.10', '0.11') or capabilities.get('module_version') != runtime_version or not all(
                capabilities.get(key) is True for key in ('inline_insert', 'inline_update', 'inline_delete')):
            raise Blocked('Requires exact reviewed native DML runtime with installed SQL 0.4')
        self.check('legacy N4 installation remains SQL 0.4',
                   scalar(self.a, "SELECT extversion FROM pg_extension WHERE extname='og_lake'"), '0.4')
        result['runtime_version'] = runtime_version
        result['installed_extension_version'] = '0.4'
        manifest, self.exported, counts, artifacts = load_reference(
            self.a, args.manifest, args.run_id, args.output.parent)
        result['artifacts'].extend(artifacts)
        self.check('actual MULTITABLE profile', manifest['case'], 'MULTITABLE')
        self.check('complete genuine physical catalog imported', len(counts), len(self.exported['tables']))
        self.bindings = {item['alias']: item for item in manifest['business_tables']}
        self.check('exactly two declared business tables', sorted(self.bindings), ['t', 'u'])
        self.check('distinct actual standard table identities', len({b['table_id'] for b in self.bindings.values()}), 2)
        self.expected = {}
        self.carriers = {}
        physical = {table['table_name']: table for table in self.exported['tables']}
        current_tables = [row for row in physical['ducklake_table']['rows'] if row['end_snapshot'] is None]
        self.check('the actual standard catalog has exactly the two declared live tables',
                   sorted(row['table_id'] for row in current_tables),
                   sorted(item['table_id'] for item in self.bindings.values()))
        self.check('reference input contains no data files', physical['ducklake_data_file']['rows'], [])
        self.check('reference input contains no delete files', physical['ducklake_delete_file']['rows'], [])
        query(self.a, 'CREATE SCHEMA biz')
        query(self.a, "SELECT og_lake.attach_catalog('lake_reference','lake_metadata',NULL::text,false)")
        for alias, item in self.bindings.items():
            self.check(alias + ' schema is the declared two-column integer schema', item['schema'],
                       [{'name': 'id', 'type': 'INTEGER'}, {'name': 'val', 'type': 'INTEGER'}])
            rows = [row for row in physical['ducklake_inlined_data_tables']['rows']
                    if row['table_id'] == item['table_id']]
            self.check(alias + ' has one real inline carrier', len(rows), 1)
            self.carriers[alias] = rows[0]['table_name']
            query(self.a, foreign_table_statement('biz', alias, 'lake_reference', item['table_id'], item['schema']))
            self.expected[alias] = [{name: row[name] for name in ('id', 'val')} for row in item['expected']]
            self.expect(self.a, alias, self.expected[alias], 'initial reference values')
        query(self.a, 'CREATE TABLE biz.heap_marker(id integer PRIMARY KEY, val integer)')
        self.check('both bindings use the same catalog server',
                   scalar(self.a, "SELECT count(DISTINCT ftserver) FROM pg_foreign_table "
                                  "WHERE ftrelid IN ('biz.t'::regclass,'biz.u'::regclass)"), 1)
        result['actual_table_ids'] = {alias: item['table_id'] for alias, item in self.bindings.items()}
        self.baseline = self.image()

    def check(self, name, observed, expected=True):
        success = observed == expected
        self.result['assertions'].append({'name': name, 'status': 'pass' if success else 'fail',
                                         'details': {'observed': observed, 'expected': expected}})
        if not success:
            raise AssertionError(name)

    def expect(self, connection, alias, rows, label):
        actual = query(connection, self.sql.SQL('SELECT id,val FROM biz.{}').format(self.sql.Identifier(alias)))
        self.check(alias + ': ' + label, multiset(actual), multiset([[row['id'], row['val']] for row in rows]))

    def image(self):
        return {table['table_name']: sorted(repr(tuple(row)) for row in query(self.c,
            self.sql.SQL('SELECT * FROM lake_metadata.{}').format(self.sql.Identifier(table['table_name']))))
            for table in self.exported['tables']}

    def physical(self, alias):
        names = ['_ducklake_row_id', '_ducklake_begin_snapshot', '_ducklake_end_snapshot', 'id', 'val']
        rows = query(self.c, self.sql.SQL('SELECT {} FROM lake_metadata.{}').format(
            self.sql.SQL(',').join(map(self.sql.Identifier, names)), self.sql.Identifier(self.carriers[alias])))
        return [dict(zip(names, row)) for row in rows]

    def stats(self, alias):
        return query(self.c, 'SELECT record_count,next_row_id,file_size_bytes FROM lake_metadata.ducklake_table_stats '
                     'WHERE table_id=%s', (self.bindings[alias]['table_id'],))[0]

    def head(self):
        return query(self.c, 'SELECT snapshot_id,next_file_id FROM lake_metadata.ducklake_snapshot '
                     'ORDER BY snapshot_id DESC LIMIT 1')[0]

    def commit_conflict(self, connection, label, winner_image):
        import psycopg2
        try:
            query(connection, 'COMMIT')
        except psycopg2.Error as error:
            self.check(label + ' returns exact serialization failure', error.pgcode, '40001')
            self.result.setdefault('actual_conflicts', []).append(str(error))
        else:
            self.check(label + ' must reject the conflicting publication', 'accepted', '40001')
        query(connection, 'ROLLBACK')
        self.check(label + ' preserves every winning protocol value', self.image(), winner_image)
        self.check(label + ' aborts the ordinary heap together', query(self.c, 'SELECT * FROM biz.heap_marker'), [])
        self.check(label + ' same session remains usable', scalar(connection, 'SELECT 42'), 42)
        for alias in ('t', 'u'):
            self.expect(self.c, alias, self.expected[alias], label + ' complete visible values')


def attempts(t):
    # T's zero-match operation is the only T mutation attempt in A. A's sole
    # net lake write is in U, so iterating only net-changed tables misses T.
    for index, operation in enumerate(('update', 'delete', 'false_delete', 'released_update')):
        incoming, private = 900 + index, 1900 + index
        query(t.a, 'BEGIN')
        if operation == 'released_update':
            query(t.a, 'SAVEPOINT intent')
        statement = ('UPDATE biz.t SET val=0 WHERE id=%s RETURNING id' if operation in ('update', 'released_update')
                     else 'DELETE FROM biz.t WHERE id=%s RETURNING id' if operation == 'delete'
                     else 'DELETE FROM biz.t WHERE false RETURNING id')
        t.check(operation + ' truly matches no T source version',
                query(t.a, statement, None if operation == 'false_delete' else (incoming,)), [])
        if operation == 'released_update':
            query(t.a, 'RELEASE SAVEPOINT intent')
        query(t.a, 'INSERT INTO biz.u VALUES(%s,%s)', (private, private))
        query(t.a, 'INSERT INTO biz.heap_marker VALUES(%s,%s)', (private, private))
        t.expect(t.a, 'u', t.expected['u'] + [{'id': private, 'val': private}], operation + ' has a private U net write')
        t.expect(t.c, 'u', t.expected['u'], operation + ' U remains invisible externally')
        query(t.b, 'INSERT INTO biz.t VALUES(%s,%s)', (incoming, incoming))
        t.expected['t'].append({'id': incoming, 'val': incoming})
        t.commit_conflict(t.a, operation + ' on T with net publication only in U', t.image())

    # Negative control: the child attempt disappears on rollback. A retains a
    # real parent U write, while a concurrent T INSERT is mergeable for A.
    query(t.a, 'BEGIN')
    query(t.a, 'INSERT INTO biz.u VALUES(2000,2000)')
    query(t.a, 'SAVEPOINT discarded_intent')
    t.check('discarded child DELETE matches no T row',
            query(t.a, 'DELETE FROM biz.t WHERE id=999 RETURNING id'), [])
    query(t.a, 'ROLLBACK TO SAVEPOINT discarded_intent')
    query(t.a, 'RELEASE SAVEPOINT discarded_intent')
    query(t.b, 'INSERT INTO biz.t VALUES(999,999)')
    t.expected['t'].append({'id': 999, 'val': 999})
    query(t.a, 'COMMIT')
    t.expected['u'].append({'id': 2000, 'val': 2000})
    for alias in ('t', 'u'):
        t.expect(t.c, alias, t.expected[alias], 'rolled-back cross-table intent permits the valid parent commit')
    t.check('all failed cross-table attempts leave ordinary heap empty', query(t.c, 'SELECT * FROM biz.heap_marker'), [])


def publication(t):
    before_rows = {alias: t.physical(alias) for alias in ('t', 'u')}
    before_stats = {alias: t.stats(alias) for alias in ('t', 'u')}
    head, next_file = t.head()
    additions = {'t': [{'id': 501, 'val': 5010}, {'id': 502, 'val': 5020}],
                 'u': [{'id': 601, 'val': 6010}, {'id': 602, 'val': 6020}, {'id': 603, 'val': 6030}]}
    updated = {'t': (1, 11), 'u': (101, 101)}
    # Deliberately visit tables in descending actual table-ID order. The public
    # format sorts IDs within change groups independently of business order.
    order = sorted(t.bindings, key=lambda alias: t.bindings[alias]['table_id'], reverse=True)
    t.result['business_table_visit_order'] = order
    query(t.a, 'BEGIN')
    for alias in order:
        identity, value = updated[alias]
        t.check(alias + ' UPDATE returns its own new business value', query(t.a,
                t.sql.SQL('UPDATE biz.{} SET val=%s WHERE id=%s RETURNING id,val').format(t.sql.Identifier(alias)),
                (value, identity)), [(identity, value)])
        for row in additions[alias]:
            query(t.a, t.sql.SQL('INSERT INTO biz.{} VALUES(%s,%s)').format(t.sql.Identifier(alias)), (row['id'], row['val']))
        wanted = [dict(row, val=value) if row['id'] == identity else row for row in t.expected[alias]] + additions[alias]
        t.expect(t.a, alias, wanted, 'private multi-table values')
        t.expect(t.b, alias, t.expected[alias], 'uncommitted multi-table values remain private')
        t.expected[alias] = wanted
    query(t.a, 'INSERT INTO biz.heap_marker VALUES(1,100)')
    query(t.a, 'COMMIT')
    t.check('both tables publish one common snapshot', t.head(), (head + 1, next_file + 2))
    sorted_ids = sorted(item['table_id'] for item in t.bindings.values())
    expected_changes = ','.join('inlined_insert:' + str(i) for i in sorted_ids) + ',' + ','.join(
        'inlined_delete:' + str(i) for i in sorted_ids)
    t.check('standard changeset groups inserts before deletes and sorts IDs in each group',
            scalar(t.c, 'SELECT changes_made FROM lake_metadata.ducklake_snapshot_changes WHERE snapshot_id=%s', (head + 1,)),
            expected_changes)
    for alias in ('t', 'u'):
        t.expect(t.c, alias, t.expected[alias], 'both tables committed completely')
        physical = t.physical(alias)
        old_count, old_next, old_bytes = before_stats[alias]
        t.check(alias + ' advances only its own new-identity range', t.stats(alias),
                (old_count + len(additions[alias]) + 1, old_next + len(additions[alias]), old_bytes))
        new_ids = {row['_ducklake_row_id'] for row in physical if row['id'] in {r['id'] for r in additions[alias]}}
        t.check(alias + ' new IDs exactly cover its reserved half-open range', sorted(new_ids),
                list(range(old_next, old_next + len(additions[alias]))))
        old_identity, value = updated[alias]
        original = next(row for row in before_rows[alias] if row['id'] == old_identity)
        old_versions = [dict(row, _ducklake_end_snapshot=head + 1) if row['id'] == old_identity else row
                        for row in before_rows[alias]]
        existing = [row for row in physical if row['_ducklake_begin_snapshot'] <= head]
        t.check(alias + ' old complete versions only end the actual updated origin', multiset(existing), multiset(old_versions))
        replacement = [row for row in physical if row['id'] == old_identity and row['_ducklake_end_snapshot'] is None]
        wanted = dict(original, val=value, _ducklake_begin_snapshot=head + 1, _ducklake_end_snapshot=None)
        t.check(alias + ' replacement preserves its own original logical ID', replacement, [wanted])
        t.check(alias + ' every new physical version shares the common committed snapshot',
                all(row['_ducklake_begin_snapshot'] == head + 1 and row['_ducklake_end_snapshot'] is None
                    for row in physical if row['_ducklake_begin_snapshot'] > head))
    t.check('ordinary heap commits with both lakes', query(t.c, 'SELECT * FROM biz.heap_marker'), [(1, 100)])

    before = t.image()
    query(t.a, 'BEGIN')
    query(t.a, 'UPDATE biz.t SET val=-1 WHERE id=1')
    query(t.a, 'DELETE FROM biz.u WHERE id=101')
    query(t.a, 'INSERT INTO biz.t VALUES(701,701)')
    query(t.a, 'INSERT INTO biz.u VALUES(702,702)')
    query(t.a, 'INSERT INTO biz.heap_marker VALUES(2,200)')
    query(t.a, 'ROLLBACK')
    t.check('two-table rollback restores every complete standard record', t.image(), before)
    t.check('two-table rollback preserves only earlier committed ordinary heap', query(t.c, 'SELECT * FROM biz.heap_marker'), [(1, 100)])
    for alias in ('t', 'u'):
        t.expect(t.c, alias, t.expected[alias], 'two-table rollback preserves prior values')


def cancel(t):
    if scalar(t.a, 'SHOW enable_thread_pool') == 'on':
        raise Blocked('Exact backend wait/cancellation barrier requires dedicated mode')
    ordered = sorted(t.bindings, key=lambda alias: t.bindings[alias]['table_id'])
    late = ordered[-1]
    late_id = t.bindings[late]['table_id']
    publisher, holder = scalar(t.a, 'SELECT pg_backend_pid()'), scalar(t.b, 'SELECT pg_backend_pid()')
    query(t.a, 'SET statement_timeout=30000')
    query(t.a, 'BEGIN')
    query(t.a, 'UPDATE biz.t SET val=700 WHERE id=1')
    query(t.a, 'INSERT INTO biz.t VALUES(701,701)')
    query(t.a, 'UPDATE biz.u SET val=800 WHERE id=101')
    query(t.a, 'INSERT INTO biz.u VALUES(801,801)')
    query(t.a, 'INSERT INTO biz.heap_marker VALUES(1,999)')
    # The read and binding checks do not block on this version. The actual
    # second-table column-stats UPDATE waits after first-table publication and
    # second-table inline writes. The holder always aborts its equal-value write.
    query(t.b, 'BEGIN')
    locked = query(t.b, 'UPDATE lake_metadata.ducklake_table_column_stats SET contains_null=contains_null '
                       'WHERE table_id=%s AND column_id=(SELECT min(column_id) '
                       'FROM lake_metadata.ducklake_table_column_stats WHERE table_id=%s) RETURNING column_id',
                   (late_id, late_id))
    t.check('barrier locks one genuine late-table column statistic', len(locked), 1)
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(query, t.a, 'COMMIT')
        try:
            deadline = time.monotonic() + 12
            while True:
                waits = query(t.c, 'SELECT w.pid,w.transactionid,h.pid FROM pg_locks w JOIN pg_locks h '
                              'ON h.locktype=w.locktype AND h.transactionid=w.transactionid '
                              "WHERE w.pid=%s AND h.pid=%s AND w.locktype='transactionid' AND NOT w.granted AND h.granted",
                              (publisher, holder))
                if waits:
                    break
                if future.done():
                    raise AssertionError('Commit finished before the controlled late-table update')
                if time.monotonic() >= deadline:
                    raise AssertionError('Did not reach the actual late-table transaction lock')
                time.sleep(.02)
            t.result['late_table_barrier'] = {'table_id': late_id, 'column_id': locked[0][0], 'waits': [list(r) for r in waits]}
            t.check('neither table nor shared snapshot is partially visible', t.image(), t.baseline)
            for alias in ('t', 'u'):
                t.expect(t.c, alias, t.expected[alias], 'old business values during late-table wait')
            t.check('cancel only the exact owned publisher', scalar(t.c, 'SELECT pg_cancel_backend(%s)', (publisher,)), True)
            try:
                future.result(timeout=15)
            except Exception as error:
                t.check('cancelled multi-table commit returns cancellation', getattr(error, 'pgcode', None), '57014')
            else:
                t.check('commit must not ignore cancellation', 'accepted', '57014')
        finally:
            query(t.b, 'ROLLBACK')
    query(t.a, 'ROLLBACK')
    t.check('late-table cancellation rolls back all protocol tables and both carriers', t.image(), t.baseline)
    t.check('late-table cancellation rolls back ordinary heap', query(t.c, 'SELECT * FROM biz.heap_marker'), [])
    query(t.a, 'INSERT INTO biz.u VALUES(999,999)')
    t.expected['u'].append({'id': 999, 'val': 999})
    for alias in ('t', 'u'):
        t.expect(t.c, alias, t.expected[alias], 'same session recovers after multi-table cancellation')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--mode', required=True, choices=('attempts', 'publication', 'cancel'))
    parser.add_argument('--run-id', default=os.getenv('OG_LAKE_RUN_ID'))
    parser.add_argument('--manifest', default=os.getenv('OG_LAKE_FIXTURE_MANIFEST'))
    parser.add_argument('--output', default=os.getenv('OG_LAKE_RESULT_FILE'))
    args = parser.parse_args()
    if not all((args.run_id, args.manifest, args.output)):
        parser.error('run-id, genuine manifest and output are required')
    validate_run_id(args.run_id)
    args.output = Path(args.output).resolve()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    result = {'status': 'fail', 'scope': 'N4_staged_multitable/' + args.mode, 'assertions': [], 'artifacts': [],
              'notes': ['Staged only: not registered or run against a product binary.',
                        'Same catalog, two actual reference-created pure-inline tables; no SDK, partitions or RAC claim.',
                        'Equal-value lock-holder writes are rolled back and are not interoperability evidence.']}
    owned = None
    try:
        with OwnedDatabase(args.run_id, args.output.parent) as owned:
            tables = TwoTables(owned, args, result)
            globals()[args.mode](tables)
            result['status'] = 'pass'
    except Blocked as error:
        result.update(status='blocked', reason=str(error))
    except BaseException as error:
        result.update(status='fail', error=repr(error), traceback=traceback.format_exc())
    finally:
        if owned is not None:
            result['artifacts'].extend(owned.artifacts())
            result['cleanup'] = owned.cleanup_result
        write_json(args.output, result)
    return {'pass': 0, 'blocked': 2}.get(result['status'], 1)


if __name__ == '__main__':
    raise SystemExit(main())
