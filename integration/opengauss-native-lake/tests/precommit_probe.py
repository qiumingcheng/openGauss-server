#!/usr/bin/env python3
"""Exercise real C callbacks entering native INSERT during transaction finalization.

The probe performs SPI INSERT; only the production phase guard may reject it.
This test never replaces commit callbacks or invents standard catalog records.
"""
import argparse
import json
import os
from pathlib import Path
import sys
import traceback

DRIVER = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(DRIVER))
from inline_fixture import foreign_table_statement, normalize, query, scalar
from lake_harness import Blocked, validate_run_id
from native_test_context import OwnedDatabase, load_reference, reviewed_runtime_version


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-id', default=os.getenv('OG_LAKE_RUN_ID'))
    parser.add_argument('--output', default=os.getenv('OG_LAKE_RESULT_FILE'))
    parser.add_argument('--manifest', default=os.getenv('OG_LAKE_FIXTURE_MANIFEST'))
    args = parser.parse_args()
    if not args.run_id or not args.output:
        parser.error('--run-id and --output are required')
    validate_run_id(args.run_id)
    output = Path(args.output).resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    result = {'status': 'fail', 'scope': 'N3/late_precommit_callback',
              'assertions': [], 'artifacts': [], 'notes': [
                  'Actual superuser-only C probes invoke SPI INSERT during host transaction callbacks.',
                  'Only bounded inline INSERT is covered; this is not general DML or production acceptance.']}
    owned = None

    def check(name, observed, expected=True):
        passed = observed == expected
        result['assertions'].append({'name': name, 'status': 'pass' if passed else 'fail',
                                     'details': {'observed': observed, 'expected': expected}})
        if not passed:
            raise AssertionError(name)

    try:
        if not args.manifest:
            raise Blocked('A verified real I reference fixture is required')
        import psycopg2
        from psycopg2 import sql
        owned = OwnedDatabase(args.run_id, output.parent)
        with owned as db:
            observer = db.connect()
            query(observer, "CREATE EXTENSION og_lake VERSION '0.3'")
            capabilities = json.loads(scalar(observer, 'SELECT og_lake.host_status()'))
            runtime_version = reviewed_runtime_version()
            check('compiled runtime exactly matches reviewed source control', capabilities['module_version'], runtime_version)
            check('callback compatibility uses installed SQL version 0.3',
                  scalar(observer, "SELECT extversion FROM pg_extension WHERE extname='og_lake'"), '0.3')
            result['runtime_version'] = runtime_version
            result['installed_extension_version'] = '0.3'
            check('runtime enables bounded INSERT', capabilities.get('inline_insert'), True)
            manifest, exported, counts, artifacts = load_reference(
                observer, args.manifest, args.run_id, output.parent)
            result['artifacts'].extend(artifacts)
            check('reference profile is I', manifest['case'], 'I')
            check('reference schema matches the reviewed probe fixture', manifest['schema'],
                  [{'name': 'id', 'type': 'INTEGER'}, {'name': 'val', 'type': 'INTEGER'}])
            check('all real reference tables imported', len(counts), len(exported['tables']))
            query(observer, 'CREATE SCHEMA biz')
            query(observer, "SELECT og_lake.attach_catalog('lake_reference','lake_metadata',NULL::text,false)")
            query(observer, foreign_table_statement('biz', 'orders', 'lake_reference',
                                                    manifest['table_id'], manifest['schema']))
            query(observer, 'CREATE TABLE biz.heap_marker(id integer PRIMARY KEY)')

            def catalog_state():
                state = {}
                for table in exported['tables']:
                    columns = table['columns']
                    rows = query(observer, sql.SQL('SELECT {} FROM lake_metadata.{}').format(
                        sql.SQL(',').join(sql.Identifier(c['column_name']) for c in columns),
                        sql.Identifier(table['table_name'])))
                    encoded = [json.dumps([normalize(value, column['data_type'])
                                           for value, column in zip(row, columns)],
                                          ensure_ascii=False, separators=(',', ':')) for row in rows]
                    state[table['table_name']] = sorted(encoded)
                return state

            for mode in (0, 1, 2):
                # No lake operation on this connection before the probe, except
                # the deliberate early write in mode 1 (probe precedes publisher).
                session = db.connect()
                before = catalog_state()
                marker = 900000 + mode
                query(session, 'BEGIN')
                query(session, 'INSERT INTO biz.heap_marker VALUES (%s)', (marker,))
                if mode == 1:
                    query(session, 'INSERT INTO biz.orders VALUES (%s,%s)', (marker, 10))
                query(session, 'SELECT og_lake_internal.test_precommit_insert(%s::regclass,%s)',
                      ('biz.orders', mode))
                if mode == 2:
                    query(session, 'INSERT INTO biz.orders VALUES (%s,%s)', (marker, 20))
                try:
                    query(session, 'COMMIT')
                except psycopg2.Error as exc:
                    check('mode %d rejects callback INSERT with the production SQLSTATE' % mode,
                          exc.pgcode, '0A000')
                    check('mode %d rejection comes from finalization guard' % mode,
                          'inside pre-commit or pre-prepare callbacks' in str(exc))
                else:
                    check('mode %d must not commit callback INSERT' % mode, 'accepted', 'rejected')
                query(session, 'ROLLBACK')
                check('mode %d leaves all standard metadata and inline rows unchanged' % mode,
                      catalog_state(), before)
                check('mode %d also rolls back ordinary heap changes' % mode,
                      scalar(observer, 'SELECT count(*) FROM biz.heap_marker WHERE id=%s', (marker,)), 0)
                check('mode %d same session remains usable' % mode, scalar(session, 'SELECT 6*7'), 42)
                query(session, 'INSERT INTO biz.orders VALUES (%s,%s)', (marker + 100, 42))
                check('mode %d can commit a subsequent normal lake INSERT' % mode,
                      query(observer, 'SELECT id,val FROM biz.orders WHERE id=%s', (marker + 100,)),
                      [(marker + 100, 42)])
                session.close()

            # A rolled-back registration must not poison the outer transaction.
            session = db.connect()
            before = catalog_state()
            query(session, 'BEGIN')
            query(session, 'SAVEPOINT probe_registration')
            query(session, "SELECT og_lake_internal.test_precommit_insert('biz.orders'::regclass,0)")
            query(session, 'ROLLBACK TO SAVEPOINT probe_registration')
            query(session, 'INSERT INTO biz.heap_marker VALUES (900010)')
            query(session, 'COMMIT')
            check('rolled-back probe registration performs no lake write', catalog_state(), before)
            check('rolled-back probe registration allows ordinary commit',
                  scalar(observer, 'SELECT count(*) FROM biz.heap_marker WHERE id=900010'), 1)
            query(session, 'INSERT INTO biz.orders VALUES (900110,42)')
            check('rolled-back probe leaves subsequent lake commit usable',
                  query(observer, 'SELECT id,val FROM biz.orders WHERE id=900110'), [(900110, 42)])

            session = db.connect()
            before = catalog_state()
            query(session, 'BEGIN')
            query(session, 'SAVEPOINT release_registration')
            query(session, "SELECT og_lake_internal.test_precommit_insert('biz.orders'::regclass,0)")
            query(session, 'RELEASE SAVEPOINT release_registration')
            try:
                query(session, 'COMMIT')
            except psycopg2.Error as exc:
                check('released child probe is adopted by parent', exc.pgcode, '0A000')
                check('adopted probe hits production phase guard',
                      'inside pre-commit or pre-prepare callbacks' in str(exc))
            else:
                check('adopted callback must not publish', 'accepted', 'rejected')
            query(session, 'ROLLBACK')
            check('adopted callback error preserves the complete catalog', catalog_state(), before)

            # Even the first-ever lake access in a PRE_PREPARE callback must
            # be rejected; a lazily registered publisher cannot join late.
            session = db.connect()
            before = catalog_state()
            gid = 'lake_probe_' + args.run_id
            query(session, 'BEGIN')
            query(session, 'INSERT INTO biz.heap_marker VALUES (900020)')
            query(session, "SELECT og_lake_internal.test_precommit_insert('biz.orders'::regclass,0)")
            try:
                query(session, sql.SQL('PREPARE TRANSACTION {}').format(sql.Literal(gid)))
            except psycopg2.Error as exc:
                check('first lake access during PRE_PREPARE is rejected', exc.pgcode, '0A000')
                check('PRE_PREPARE rejection comes from the phase guard',
                      'inside pre-commit or pre-prepare callbacks' in str(exc))
            else:
                # Release only this test's exact prepared transaction before
                # failing; never leave a test-owned database locked forever.
                query(observer, sql.SQL('ROLLBACK PREPARED {}').format(sql.Literal(gid)))
                check('late PRE_PREPARE INSERT cannot be prepared', 'accepted', 'rejected')
            query(session, 'ROLLBACK')
            check('PRE_PREPARE failure leaves no prepared transaction',
                  scalar(observer, 'SELECT count(*) FROM pg_prepared_xacts WHERE gid=%s', (gid,)), 0)
            check('PRE_PREPARE failure preserves all lake metadata and data', catalog_state(), before)
            check('PRE_PREPARE failure rolls back ordinary heap changes',
                  scalar(observer, 'SELECT count(*) FROM biz.heap_marker WHERE id=900020'), 0)
            query(session, 'INSERT INTO biz.orders VALUES (900120,42)')
            check('same session can INSERT after PRE_PREPARE failure',
                  query(observer, 'SELECT id,val FROM biz.orders WHERE id=900120'), [(900120, 42)])
            result['status'] = 'pass'
    except Blocked as exc:
        result.update(status='blocked', reason=str(exc))
    except BaseException as exc:
        result.update(status='fail', reason=str(exc), traceback=traceback.format_exc())
    finally:
        if owned is not None:
            result['artifacts'].extend(owned.artifacts())
            result['database_cleanup'] = owned.cleanup_result
        output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n')
        print(json.dumps({'status': result['status'], 'assertions': len(result['assertions']),
                          'result': str(output)}))
    return {'pass': 0, 'fail': 1, 'blocked': 2}[result['status']]


if __name__ == '__main__':
    sys.exit(main())
