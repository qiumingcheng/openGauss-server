#!/usr/bin/env python3
"""N3 version admission and allocation boundaries; deliberate faults are labeled.

Never use these manually changed catalog states as interoperability fixtures.
Every mutation is confined to an owned test database; negative cases roll back.
"""
import argparse
import copy
import json
import os
from pathlib import Path
import sys
import traceback

DRIVER = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(DRIVER))
from inline_fixture import foreign_table_statement, normalize, query, scalar
from lake_harness import Blocked, read_json, validate_run_id, write_json
from native_test_context import OwnedDatabase, load_reference, reviewed_runtime_version

LOCAL_ID_START = 10 ** 18


def canonical(rows):
    return sorted(rows, key=lambda r: json.dumps(r, sort_keys=True, ensure_ascii=False, default=str))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--mode', required=True, choices=['version', 'rowid', 'metadata_faults'])
    parser.add_argument('--manifest', default=os.getenv('OG_LAKE_FIXTURE_MANIFEST'))
    parser.add_argument('--run-id', default=os.getenv('OG_LAKE_RUN_ID'))
    parser.add_argument('--output', default=os.getenv('OG_LAKE_RESULT_FILE'))
    args = parser.parse_args()
    if not args.manifest or not args.run_id or not args.output:
        parser.error('--manifest, --run-id and --output (or runner environment) are required')
    validate_run_id(args.run_id)
    output = Path(args.output).resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    result = {'status': 'fail', 'scope': 'native_inline_insert_edges/' + args.mode,
              'assertions': [], 'artifacts': [], 'fault_injection': True,
              'notes': ['Starts with an actual, source-locked reference I fixture.',
                        'Manual catalog/binding mutations are explicit boundary or fault injections in an owned database.',
                        'Injected states never count as genuine-reference or reverse-interread compatibility evidence.',
                        'Rejected commits must restore all standard catalog tables, all host binding tables, and the ordinary heap.']}
    owned = None

    def check(name, observed, expected=True):
        ok = observed == expected
        result['assertions'].append({'name': name, 'status': 'pass' if ok else 'fail',
                                     'details': {'observed': observed, 'expected': expected}})
        if not ok:
            raise AssertionError(name)

    try:
        import psycopg2
        from psycopg2 import sql
        manifest = read_json(args.manifest)
        if manifest.get('case') != 'I':
            raise Blocked('This boundary test requires the genuine simple I fixture')
        owned = OwnedDatabase(args.run_id, output.parent)
        with owned as db:
            a, observer = db.connect(), db.connect()
            extension_version = '0.2' if args.mode == 'version' else '0.3'
            query(a, sql.SQL('CREATE EXTENSION og_lake VERSION {}').format(sql.Literal(extension_version)))
            capabilities = json.loads(scalar(a, 'SELECT og_lake.host_status()'))
            result['actual_host_capabilities'] = capabilities
            runtime_version = reviewed_runtime_version()
            if capabilities.get('module_version') != runtime_version or capabilities.get('inline_insert') is not True:
                raise Blocked('Native INSERT runtime must exactly match the reviewed source control version')
            result['runtime_version'] = runtime_version
            result['installed_extension_version'] = extension_version
            old_version_guard = ('extension version 0.3' if runtime_version == '0.3'
                                 else ('native lake INSERT requires extension 0.3 or 0.4' if runtime_version == '0.4'
                                       else 'native lake INSERT requires extension 0.3 or later'))
            check('actual extension catalog version', scalar(a, "SELECT extversion FROM pg_extension WHERE extname='og_lake'"),
                  extension_version)
            query(a, 'CREATE SCHEMA biz')
            query(a, 'CREATE TABLE biz.atomic_heap(id integer PRIMARY KEY, note text)')
            manifest, exported, counts, artifacts = load_reference(a, args.manifest, args.run_id, output.parent)
            result['artifacts'].extend(artifacts)
            result['reference_import_counts'] = counts
            table_id = manifest['table_id']
            query(a, 'SELECT og_lake.attach_catalog(%s::name,%s::name,NULL::text,%s)',
                  ('lake_reference', 'lake_metadata', args.mode == 'version'))
            query(a, foreign_table_statement('biz', 'orders', 'lake_reference', table_id, manifest['schema']))
            check('genuine fixture reads before injection', query(a, 'SELECT id,val FROM biz.orders ORDER BY id'),
                  [(r['id'], r['val']) for r in manifest['expected']])
            definitions = {t['table_name']: t for t in exported['tables']}
            inline_names = [r['table_name'] for t in exported['tables'] if t['table_name'] == 'ducklake_inlined_data_tables'
                            for r in t['rows'] if r['table_id'] == table_id]
            check('one actual inline carrier', len(inline_names), 1)
            inline_name = inline_names[0]
            serial = 0

            def state(label):
                """Read a complete consistent ordinary-table oracle, never stats-only."""
                nonlocal serial
                serial += 1
                snapshot = {'standard': {}, 'bindings': {}, 'binding_and_metadata_constraints': [], 'ordinary_heap': []}
                query(observer, 'BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY')
                try:
                    for name, definition in definitions.items():
                        columns = definition['columns']
                        rows = query(observer, sql.SQL('SELECT {} FROM lake_metadata.{}').format(
                            sql.SQL(',').join(sql.Identifier(c['column_name']) for c in columns), sql.Identifier(name)))
                        snapshot['standard'][name] = canonical([
                            {c['column_name']: normalize(v, c['data_type']) for c, v in zip(columns, row)} for row in rows])
                    for name in ('catalog_binding', 'table_binding', 'relation_binding', 'column_binding'):
                        with observer.cursor() as cursor:
                            cursor.execute(sql.SQL('SELECT * FROM og_lake_internal.{}').format(sql.Identifier(name)))
                            names = [c.name for c in cursor.description]
                            snapshot['bindings'][name] = canonical([
                                {n: v if v is None or isinstance(v, (int, float, bool, str)) else str(v)
                                 for n, v in zip(names, row)} for row in cursor.fetchall()])
                    snapshot['binding_and_metadata_constraints'] = [list(row) for row in query(observer,
                        "SELECT n.nspname,c.relname,k.conname,k.contype,pg_get_constraintdef(k.oid) "
                        "FROM pg_constraint k JOIN pg_class c ON c.oid=k.conrelid "
                        "JOIN pg_namespace n ON n.oid=c.relnamespace "
                        "WHERE n.nspname IN ('og_lake_internal','lake_metadata') "
                        "ORDER BY n.nspname,c.relname,k.conname")]
                    snapshot['ordinary_heap'] = [list(r) for r in query(observer, 'SELECT id,note FROM biz.atomic_heap ORDER BY id')]
                    query(observer, 'ROLLBACK')
                except BaseException:
                    query(observer, 'ROLLBACK')
                    raise
                path = output.parent / ('%02d-' % serial + label + '-state.json')
                write_json(path, snapshot)
                result['artifacts'].append({'path': str(path)})
                return snapshot

            def reject_statement(name, statement, code, *, contains=None):
                try:
                    query(a, statement)
                except psycopg2.Error as exc:
                    check(name + '/sqlstate', exc.pgcode, code)
                    if contains:
                        check(name + '/native diagnostic', contains in str(exc))
                else:
                    raise AssertionError(name + ' unexpectedly succeeded')

            def reject_commit(name, before, code):
                try:
                    query(a, 'COMMIT')
                except psycopg2.Error as exc:
                    check(name + '/commit sqlstate', exc.pgcode, code)
                else:
                    raise AssertionError(name + ' unexpectedly committed')
                finally:
                    query(a, 'ROLLBACK')
                check(name + '/all catalogs bindings and heap roll back', state(name + '-after'), before)
                check(name + '/connection recovered', scalar(a, 'SELECT 6*7'), 42)

            def expected_insert(before, observed, *, rowid, record_count, marker, row):
                """Explicit simple-I oracle; only the declared publication fields change."""
                expected = copy.deepcopy(before)
                standard = expected['standard']
                old_head = max(standard['ducklake_snapshot'], key=lambda r: r['snapshot_id'])
                new_id = old_head['snapshot_id'] + 1
                heads = [r for r in observed['standard']['ducklake_snapshot'] if r['snapshot_id'] == new_id]
                check('one new committed snapshot', len(heads), 1)
                check('new snapshot time exists', heads[0]['snapshot_time'] is not None)
                new_head = dict(old_head, snapshot_id=new_id, snapshot_time=heads[0]['snapshot_time'],
                                next_file_id=old_head['next_file_id'] + 1)
                standard['ducklake_snapshot'].append(new_head)
                standard['ducklake_snapshot_changes'].append({'snapshot_id': new_id, 'changes_made': 'inlined_insert:' + str(table_id),
                    'author': None, 'commit_message': None, 'commit_extra_info': None})
                for stat in standard['ducklake_table_stats']:
                    if stat['table_id'] == table_id:
                        stat['next_row_id'] = rowid + 1
                        stat['record_count'] = record_count
                columns = {r['column_id']: r['column_name'] for r in standard['ducklake_column']
                           if r['table_id'] == table_id and r['end_snapshot'] is None}
                for stat in standard['ducklake_table_column_stats']:
                    if stat['table_id'] == table_id:
                        # I has known non-NULL integer bounds; the new test values
                        # are explicit larger maxima. Unknown-general merging is
                        # tested elsewhere against the actual reference engine.
                        value = row[columns[stat['column_id']]]
                        check('I new test value exceeds old bound', value > int(stat['max_value']))
                        stat['max_value'] = str(value)
                        stat['max_is_exact'] = True
                standard[inline_name].append(dict(row, _ducklake_row_id=rowid,
                    _ducklake_begin_snapshot=new_id, _ducklake_end_snapshot=None))
                for name in standard:
                    standard[name] = canonical(standard[name])
                expected['ordinary_heap'].append(list(marker))
                expected['ordinary_heap'].sort()
                return expected

            baseline = state('genuine-baseline')
            if args.mode == 'version':
                reject_statement('0.2 normal INSERT', 'INSERT INTO biz.orders VALUES (901,9001)', '0A000',
                                 contains=old_version_guard)
                reject_statement('0.2 writable attach', "SELECT og_lake.attach_catalog('blocked_rw'::name,'lake_metadata'::name,NULL::text,false)",
                                 '0A000', contains=old_version_guard)
                check('normal old-version rejection has no catalog effects', state('version-normal-rejected'), baseline)
                check('failed writable attach creates no server', scalar(observer,
                      "SELECT count(*) FROM pg_foreign_server WHERE srvname='blocked_rw'"), 0)
                query(a, 'BEGIN')
                try:
                    # Explicit host-binding fault injection, wholly rolled back.
                    query(a, 'ALTER TABLE og_lake_internal.catalog_binding DROP CONSTRAINT catalog_binding_read_only_check')
                    query(a, 'ALTER TABLE og_lake_internal.table_binding DROP CONSTRAINT table_binding_read_only_check')
                    query(a, 'UPDATE og_lake_internal.catalog_binding SET read_only=false')
                    query(a, 'UPDATE og_lake_internal.table_binding SET read_only=false')
                    query(a, "INSERT INTO biz.atomic_heap VALUES(1,'version bypass attempt')")
                    check('injected binding flags are actually writable',
                          scalar(a, 'SELECT bool_and(NOT read_only) FROM og_lake_internal.table_binding'))
                    reject_statement('0.2 cannot bypass version using writable flags',
                                     'INSERT INTO biz.orders VALUES(902,9002)', '0A000', contains=old_version_guard)
                finally:
                    query(a, 'ROLLBACK')
                check('binding fault and ordinary write fully roll back', state('version-fault-rolled-back'), baseline)
                check('0.2 remains readable after rejection', query(a, 'SELECT id,val FROM biz.orders ORDER BY id'),
                      [(r['id'], r['val']) for r in manifest['expected']])

            elif args.mode == 'rowid':
                query(a, 'BEGIN')
                query(a, 'UPDATE lake_metadata.ducklake_table_stats SET next_row_id=%s WHERE table_id=%s',
                      (LOCAL_ID_START - 1, table_id))
                query(a, "INSERT INTO biz.atomic_heap VALUES(1,'cross allocation boundary')")
                query(a, 'INSERT INTO biz.orders VALUES(901,9001),(902,9002)')
                reject_commit('two rows cross reserved-ID boundary', baseline, '22003')
                query(a, 'BEGIN')
                query(a, 'UPDATE lake_metadata.ducklake_table_stats SET next_row_id=%s WHERE table_id=%s',
                      (LOCAL_ID_START - 1, table_id))
                query(a, "INSERT INTO biz.atomic_heap VALUES(2,'last legal row ID')")
                query(a, 'INSERT INTO biz.orders VALUES(901,9001)')
                query(a, 'COMMIT')
                last = state('last-legal-ID-committed')
                check('last legal ID complete publication oracle', last,
                      expected_insert(baseline, last, rowid=LOCAL_ID_START - 1,
                                      record_count=4, marker=(2, 'last legal row ID'), row={'id': 901, 'val': 9001}))
                check('last legal row is visible', query(a, 'SELECT id,val FROM biz.orders ORDER BY id'),
                      [(1, 10), (2, 100), (3, 200), (901, 9001)])
                query(a, 'BEGIN')
                query(a, "INSERT INTO biz.atomic_heap VALUES(3,'exhausted row IDs')")
                query(a, 'INSERT INTO biz.orders VALUES(902,9002)')
                reject_commit('reserved-ID frontier exhausted', last, '22003')

            else:
                faults = [
                    ('missing-table-stats', 'DELETE FROM lake_metadata.ducklake_table_stats WHERE table_id=%s', (table_id,), 'XX001'),
                    ('duplicate-table-stats', 'INSERT INTO lake_metadata.ducklake_table_stats SELECT * FROM lake_metadata.ducklake_table_stats WHERE table_id=%s', (table_id,), 'XX001'),
                    ('NULL-next-row-id', 'UPDATE lake_metadata.ducklake_table_stats SET next_row_id=NULL WHERE table_id=%s', (table_id,), 'XX001'),
                    ('negative-next-row-id', 'UPDATE lake_metadata.ducklake_table_stats SET next_row_id=-1 WHERE table_id=%s', (table_id,), '22003'),
                    ('reserved-next-row-id', 'UPDATE lake_metadata.ducklake_table_stats SET next_row_id=%s WHERE table_id=%s', (LOCAL_ID_START + 1, table_id), '22003'),
                    ('unknown-file-bytes', 'UPDATE lake_metadata.ducklake_table_stats SET file_size_bytes=NULL WHERE table_id=%s', (table_id,), '0A000'),
                    ('negative-file-bytes', 'UPDATE lake_metadata.ducklake_table_stats SET file_size_bytes=-1 WHERE table_id=%s', (table_id,), '22003'),
                    ('negative-record-count', 'UPDATE lake_metadata.ducklake_table_stats SET record_count=-1 WHERE table_id=%s', (table_id,), '22003'),
                    ('record-count-overflow', 'UPDATE lake_metadata.ducklake_table_stats SET record_count=%s WHERE table_id=%s', (9223372036854775807, table_id), '22003'),
                ]
                for number, (name, statement, params, code) in enumerate(faults, 1):
                    query(a, 'BEGIN')
                    query(a, statement, params)
                    query(a, 'INSERT INTO biz.atomic_heap VALUES(%s,%s)', (number, name))
                    query(a, 'INSERT INTO biz.orders VALUES(901,9001)')
                    reject_commit(name, baseline, code)
                # NULL record_count is a legitimate UNKNOWN, unlike a missing
                # allocation frontier. Inject this state only to test admission.
                query(a, 'BEGIN')
                query(a, 'UPDATE lake_metadata.ducklake_table_stats SET record_count=NULL WHERE table_id=%s', (table_id,))
                query(a, "INSERT INTO biz.atomic_heap VALUES(100,'preserve unknown record count')")
                query(a, 'INSERT INTO biz.orders VALUES(901,9001)')
                query(a, 'COMMIT')
                observed = state('unknown-record-count-accepted')
                check('legitimate unknown count complete publication oracle', observed,
                      expected_insert(baseline, observed, rowid=3, record_count=None,
                                      marker=(100, 'preserve unknown record count'), row={'id': 901, 'val': 9001}))
                check('logical count remains exact with unknown table statistic', scalar(a, 'SELECT count(*) FROM biz.orders'), 4)
            check('ordinary SQL remains usable', scalar(a, 'SELECT 42'), 42)
            result['status'] = 'pass'
    except Blocked as exc:
        result.update(status='blocked', reason=str(exc))
    except BaseException as exc:
        result.update(status='fail', reason=str(exc), traceback=traceback.format_exc())
    finally:
        if owned:
            result['artifacts'].extend(owned.artifacts())
            result['cleanup'] = owned.cleanup_result
        write_json(output, result)
        print(json.dumps({'mode': args.mode, 'status': result['status'], 'output': str(output)}))
    return 0 if result['status'] == 'pass' else 2 if result['status'] == 'blocked' else 1


if __name__ == '__main__':
    raise SystemExit(main())
