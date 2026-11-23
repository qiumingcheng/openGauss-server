#!/usr/bin/env python3
"""Exercise the public SQL boundaries of the bounded native INSERT slice."""
import argparse
import io
import json
import os
from pathlib import Path
import sys
import traceback
import uuid

DRIVER = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(DRIVER))
from inline_fixture import foreign_table_statement, multiset, query, scalar
from lake_harness import Blocked, validate_run_id, write_json
from native_test_context import OwnedDatabase, load_reference, reviewed_runtime_version


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-id', default=os.getenv('OG_LAKE_RUN_ID'))
    parser.add_argument('--output', default=os.getenv('OG_LAKE_RESULT_FILE'))
    parser.add_argument('--manifest', default=os.getenv('OG_LAKE_FIXTURE_MANIFEST'))
    parser.add_argument('--mode', choices=['basic', 'ddl', 'prepare'], required=True)
    args = parser.parse_args()
    if not args.run_id or not args.output or not args.manifest:
        parser.error('run-id, output and genuine reference manifest are required')
    validate_run_id(args.run_id)
    output = Path(args.output).resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    result = {'status': 'fail', 'scope': 'native_inline_insert_admission/' + args.mode,
              'assertions': [], 'artifacts': [], 'rejections': [], 'notes': [
                  'All standard protocol rows originate from a real format 1.1 reference fixture.',
                  'This checks stated acceptance and rejection boundaries, not general DML or 2PC support.']}
    owned = None

    def check(name, actual, expected=True):
        passed = actual == expected
        result['assertions'].append({'name': name, 'status': 'pass' if passed else 'fail',
                                     'details': {'actual': actual, 'expected': expected}})
        if not passed:
            raise AssertionError(name)

    try:
        import psycopg2
        from psycopg2 import sql
        with OwnedDatabase(args.run_id, output.parent) as owned:
            a, observer = owned.connect(), owned.connect()
            query(a, "CREATE EXTENSION og_lake VERSION '0.3'")
            caps = json.loads(scalar(a, 'SELECT og_lake.host_status()'))
            runtime_version = reviewed_runtime_version()
            if caps.get('module_version') != runtime_version or caps.get('inline_insert') is not True:
                raise Blocked('Native INSERT runtime must exactly match the reviewed source control version')
            check('N3 admission keeps installed SQL version 0.3',
                  scalar(a, "SELECT extversion FROM pg_extension WHERE extname='og_lake'"), '0.3')
            result['runtime_version'] = runtime_version
            result['installed_extension_version'] = '0.3'
            rc_guard = 'native lake access requires READ COMMITTED'
            row_lock_guard = 'without row locks' if runtime_version == '0.3' else 'does not support row locks'
            modify_version_guard = None if runtime_version == '0.3' else 'UPDATE/DELETE require extension 0.4'
            manifest, exported, counts, artifacts = load_reference(a, args.manifest, args.run_id, output.parent)
            result['artifacts'].extend(artifacts)
            check('genuine I profile imported without missing tables',
                  (manifest['case'], len(counts)), ('I', len(exported['tables'])))
            initial = [[row['id'], row['val']] for row in manifest['expected']]
            query(a, 'CREATE SCHEMA biz')
            query(a, 'CREATE TABLE biz.heap_marker(id integer PRIMARY KEY,val integer)')
            query(a, 'INSERT INTO biz.heap_marker VALUES(1,10)')

            def bind(read_only=False):
                query(a, 'SELECT og_lake.attach_catalog(%s::name,%s::name,NULL::text,%s)',
                      ('lake_reference', 'lake_metadata', read_only))
                query(a, foreign_table_statement('biz', 'orders', 'lake_reference', manifest['table_id'], manifest['schema']))

            def image(conn):
                return {table['table_name']: sorted(repr(tuple(row)) for row in query(conn,
                        sql.SQL('SELECT * FROM lake_metadata.{}').format(sql.Identifier(table['table_name']))))
                        for table in exported['tables']}

            def business(conn, expected, label):
                check(label, multiset(query(conn, 'SELECT id,val FROM biz.orders')), multiset(expected))

            def binding(conn):
                return query(conn, 'SELECT r.relation_oid,t.binding_id::text,t.table_id,t.table_uuid::text '
                             'FROM og_lake_internal.relation_binding r '
                             'JOIN og_lake_internal.table_binding t USING(binding_id) ORDER BY r.relation_oid')

            def reject(label, operation, code, fragment=None):
                try:
                    operation()
                except psycopg2.Error as exc:
                    result['rejections'].append({'name': label, 'sqlstate': exc.pgcode, 'message': str(exc)})
                    check(label + ' SQLSTATE', exc.pgcode, code)
                    if fragment:
                        check(label + ' reaches its intended capability guard', fragment in str(exc))
                else:
                    check(label + ' must be rejected', 'accepted', code)

            bind(read_only=args.mode == 'basic')
            initial_image = image(observer)
            business(a, initial, 'initial read equals genuine reference values')

            if args.mode == 'basic':
                reject('read-only catalog INSERT', lambda: query(a, 'INSERT INTO biz.orders VALUES(101,1010)'),
                       '25006', 'binding is read-only')
                check('read-only catalog rejection preserves every protocol row', image(observer), initial_image)
                query(a, 'DROP SERVER lake_reference CASCADE')
                bind()
                query(a, 'BEGIN READ ONLY')
                business(a, initial, 'read-only transaction can read the lake')
                reject('read-only transaction INSERT', lambda: query(a, 'INSERT INTO biz.orders VALUES(102,1020)'), '25006')
                query(a, 'ROLLBACK')

                for isolation in ('REPEATABLE READ', 'SERIALIZABLE'):
                    query(a, 'BEGIN ISOLATION LEVEL ' + isolation)
                    result.setdefault('isolation_modes', {})[isolation] = scalar(a, 'SHOW transaction_isolation')
                    query(a, 'SAVEPOINT rejected_read')
                    reject(isolation + ' SELECT', lambda: business(a, initial, isolation + ' unsupported read'),
                           '0A000', rc_guard)
                    query(a, 'ROLLBACK TO rejected_read')
                    query(a, 'RELEASE rejected_read')
                    reject(isolation + ' INSERT', lambda: query(a, 'INSERT INTO biz.orders VALUES(103,1030)'),
                           '0A000', rc_guard)
                    query(a, 'ROLLBACK')

                statements = [
                    ('UPDATE', 'UPDATE biz.orders SET val=val+1', '0A000', modify_version_guard),
                    ('DELETE', 'DELETE FROM biz.orders', '0A000', modify_version_guard),
                    ('row locking', 'SELECT id FROM biz.orders FOR UPDATE', '0A000', row_lock_guard),
                    ('UPSERT host admission', 'INSERT INTO biz.orders VALUES(104,1040) ON CONFLICT DO NOTHING',
                     '0A000', 'INSERT ON CONFLICT DO UPDATE is not supported on foreign table'),
                ]
                for column in ('xmin','ctid','tableoid'):
                    statements.append(('system-column '+column+' host admission',
                        'INSERT INTO biz.orders VALUES(105,1050) RETURNING '+column,
                        '42703', 'column "'+column+'" does not exist'))
                for name, statement, code, fragment in statements:
                    query(a, 'BEGIN')
                    query(a, 'UPDATE biz.heap_marker SET val=20 WHERE id=1')
                    reject(name, lambda statement=statement: query(a, statement), code, fragment)
                    query(a, 'ROLLBACK')
                    check(name + ' leaves ordinary heap unchanged', scalar(observer, 'SELECT val FROM biz.heap_marker WHERE id=1'), 10)
                    check(name + ' leaves all protocol rows unchanged', image(observer), initial_image)
                    business(observer, initial, name + ' leaves lake business values unchanged')
                result['notes'].append('The legal ON CONFLICT syntax is rejected by the host foreign-table admission check before FDW planning; this does not exercise the module UPSERT callback guard. The host diagnostic says DO UPDATE even for DO NOTHING.')
                result['notes'].append('This host excludes foreign-table RTEs from system-attribute name lookup (parse_relation.cpp). xmin, ctid and tableoid therefore fail with exact undefined-column diagnostics before FDW planning; these cases do not claim coverage of the defensive module RETURNING-system-column walker.')

                def copy_input():
                    with a.cursor() as cursor:
                        cursor.copy_expert('COPY biz.orders(id,val) FROM STDIN', io.StringIO('106\t1060\n'))
                reject('COPY FROM', copy_input, '0A000', 'COPY into native lake tables is not enabled')
                check('COPY and isolation rejections publish no protocol changes', image(observer), initial_image)
                query(a, 'EXPLAIN INSERT INTO biz.orders VALUES(107,1070)')
                check('EXPLAIN INSERT creates no snapshot or inline value', image(observer), initial_image)
                query(a, 'INSERT INTO biz.orders VALUES(108,1080)')
                business(observer, initial + [[108,1080]], 'same connection commits a supported INSERT after all rejections')

            elif args.mode == 'ddl':
                original_binding = binding(observer)
                query(a, 'BEGIN')
                query(a, 'INSERT INTO biz.orders VALUES(201,2010)')
                query(a, 'SAVEPOINT forbidden_ddl')
                reject('write followed by DROP binding', lambda: query(a, 'DROP FOREIGN TABLE biz.orders'),
                       '0A000', 'binding DDL cannot follow a lake write')
                query(a, 'ROLLBACK TO SAVEPOINT forbidden_ddl')
                query(a, 'RELEASE SAVEPOINT forbidden_ddl')
                business(a, initial + [[201,2010]], 'rejected child DDL preserves the parent write set')
                query(a, 'COMMIT')
                business(observer, initial + [[201,2010]], 'parent write commits after child DDL rejection')
                check('rejected DROP preserves original host relation and binding identities', binding(observer), original_binding)
                before_ddl = image(observer)

                query(a, 'BEGIN')
                query(a, 'SAVEPOINT released_binding')
                query(a, 'DROP FOREIGN TABLE biz.orders')
                query(a, foreign_table_statement('biz', 'orders', 'lake_reference', manifest['table_id'], manifest['schema']))
                query(a, 'RELEASE SAVEPOINT released_binding')
                reject('released child binding DDL followed by INSERT', lambda: query(a, 'INSERT INTO biz.orders VALUES(202,2020)'),
                       '0A000', 'cannot follow binding DDL')
                query(a, 'ROLLBACK')
                check('failed DDL then write restores original binding', binding(observer), original_binding)
                check('failed DDL then write preserves every protocol row', image(observer), before_ddl)

                query(a, 'BEGIN')
                query(a, 'SAVEPOINT reverted_binding')
                query(a, 'DROP FOREIGN TABLE biz.orders')
                query(a, foreign_table_statement('biz', 'orders', 'lake_reference', manifest['table_id'], manifest['schema']))
                query(a, 'ROLLBACK TO SAVEPOINT reverted_binding')
                query(a, 'RELEASE SAVEPOINT reverted_binding')
                query(a, 'INSERT INTO biz.orders VALUES(203,2030)')
                query(a, 'COMMIT')
                business(observer, initial + [[201,2010],[203,2030]], 'rolled-back child DDL does not poison a later parent INSERT')
                check('rolled-back DDL keeps the original binding identity', binding(observer), original_binding)
                before_discard = image(observer)

                query(a, 'BEGIN')
                query(a, 'SAVEPOINT reverted_write')
                query(a, 'INSERT INTO biz.orders VALUES(204,2040)')
                query(a, 'ROLLBACK TO SAVEPOINT reverted_write')
                query(a, 'RELEASE SAVEPOINT reverted_write')
                query(a, 'DROP FOREIGN TABLE biz.orders')
                query(a, foreign_table_statement('biz', 'orders', 'lake_reference', manifest['table_id'], manifest['schema']))
                query(a, 'COMMIT')
                check('discarded child write permits parent binding DDL without a protocol publication', image(observer), before_discard)
                check('successful rebind creates a different host relation identity', binding(observer) != original_binding)
                business(observer, initial + [[201,2010],[203,2030]], 'rebound host table still reads exact durable rows')

            else:
                if int(scalar(a, 'SHOW max_prepared_transactions')) <= 0:
                    raise Blocked('The host disables PREPARE; that cannot prove the native lake rejection boundary')
                gid = 'lake_admission_' + uuid.uuid4().hex
                prepare = sql.SQL('PREPARE TRANSACTION {}').format(sql.Literal(gid))
                rollback = sql.SQL('ROLLBACK PREPARED {}').format(sql.Literal(gid))
                try:
                    query(a, 'BEGIN')
                    query(a, 'INSERT INTO biz.heap_marker VALUES(301,3010)')
                    query(a, prepare)
                    check('ordinary heap control really enters the prepared state',
                          scalar(observer, 'SELECT count(*) FROM pg_prepared_xacts WHERE gid=%s AND database=current_database()', (gid,)), 1)
                    check('prepared control has no visible heap row', scalar(observer, 'SELECT count(*) FROM biz.heap_marker WHERE id=301'), 0)
                    query(observer, rollback)
                    query(a, 'BEGIN')
                    query(a, 'INSERT INTO biz.heap_marker VALUES(302,3020)')
                    query(a, 'INSERT INTO biz.orders VALUES(302,3020)')
                    reject('lake PREPARE', lambda: query(a, prepare), '0A000', 'cannot prepare a transaction using native lake operations')
                    query(a, 'ROLLBACK')
                    check('lake PREPARE leaves no prepared state', scalar(observer, 'SELECT count(*) FROM pg_prepared_xacts WHERE gid=%s', (gid,)), 0)
                    check('lake PREPARE rolls back ordinary heap', scalar(observer, 'SELECT count(*) FROM biz.heap_marker WHERE id=302'), 0)
                    check('lake PREPARE rolls back every protocol row', image(observer), initial_image)
                    query(a, 'INSERT INTO biz.orders VALUES(303,3030)')
                    business(observer, initial + [[303,3030]], 'same session can commit normally after PREPARE rejection')
                finally:
                    query(a, 'ROLLBACK')
                    # Release only this attempt's exact prepared transaction in
                    # its owned database; never sweep other users' GIDs.
                    if scalar(observer, 'SELECT count(*) FROM pg_prepared_xacts WHERE gid=%s AND database=current_database()', (gid,)):
                        query(observer, rollback)
            result['status'] = 'pass'
    except Blocked as exc:
        result.update(status='blocked', reason=str(exc))
    except BaseException as exc:
        result.update(status='fail', error=repr(exc), traceback=traceback.format_exc())
        if not result['assertions'] or result['assertions'][-1]['status'] != 'fail':
            result['assertions'].append({'name': 'admission scenario completed', 'status': 'fail', 'details': repr(exc)})
    finally:
        if owned is not None:
            result['artifacts'].extend(owned.artifacts())
            result['cleanup'] = owned.cleanup_result
        write_json(output, result)
        print(json.dumps({'status': result['status'], 'mode': args.mode, 'output': str(output)}))
    return {'pass': 0, 'blocked': 2}.get(result['status'], 1)


if __name__ == '__main__':
    raise SystemExit(main())
