#!/usr/bin/env python3
"""Real reference-export tests for the native, serial, read-only inline slice."""
import argparse
import datetime
import json
import os
import re
from pathlib import Path
import subprocess
import sys
import time
import traceback
import uuid

DRIVER = Path(__file__).resolve().parents[1]
ROOT = DRIVER.parents[1]
sys.path.insert(0, str(DRIVER))
from inline_fixture import (export_reference, foreign_table_statement, import_catalog, multiset,
                            query, replace_catalog_values, scalar)
from lake_harness import Blocked, read_json, sha256, validate_run_id, write_json


class RejectionVerified(Exception):
    pass


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--run-id', default=os.getenv('OG_LAKE_RUN_ID'))
    parser.add_argument('--output', default=os.getenv('OG_LAKE_RESULT_FILE'))
    parser.add_argument('--manifest', default=os.getenv('OG_LAKE_FIXTURE_MANIFEST'))
    parser.add_argument('--test-hold-after-allocation', action='store_true',
                        help=argparse.SUPPRESS)
    parser.add_argument('--mode', required=True,
                        choices=['read', 'typed', 'long_text', 'empty', 'snapshot', 'reject_file', 'reject_schema', 'ddl', 'identity'])
    args = parser.parse_args()
    if not args.run_id or not args.output:
        parser.error('--run-id and --output (or runner environment) required')
    validate_run_id(args.run_id)
    output = Path(args.output).resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    result = {'status': 'fail', 'scope': 'native_inline/' + args.mode, 'assertions': [], 'artifacts': [],
              'notes': [
                  'Business input and protocol values come from a fixed real DuckLake 1.1 reference export.',
                  'The host importer only adapts physical SQL DDL spelling and enclosing schema, and checks every loaded value.',
                  'No SDK, lake writes/flush, production permissions, RAC, or shared external-writer compatibility is claimed.',
                  'Snapshot replay copies two genuine committed reference states into one host transaction; it invents no protocol IDs.']}
    connections = []
    admin = None
    database = None
    allocation = None

    def check(name, observed, expected=True):
        passed = observed == expected
        result['assertions'].append({'name': name, 'status': 'pass' if passed else 'fail',
                                     'details': {'observed': observed, 'expected': expected}})
        if not passed:
            raise AssertionError(name)

    def reject(name, conn, statement, states, params=None):
        if isinstance(states, str):
            states = [states]
        try:
            query(conn, statement, params)
        except psycopg2.Error as exc:
            check(name, exc.pgcode in states, True)
            result['assertions'][-1]['details']['sqlstate'] = exc.pgcode
        else:
            check(name, 'unexpectedly accepted', states)

    try:
        if not args.manifest:
            raise Blocked('The runner must provide a verified real reference fixture manifest')
        try:
            import psycopg2
            from psycopg2 import sql
        except ImportError as exc:
            raise Blocked('psycopg2 is required for real native SQL tests') from exc
        manifest_path = Path(args.manifest).resolve()
        manifest = read_json(manifest_path)
        profile = {'read': 'I', 'typed': 'TYPED', 'long_text': 'LONG_TEXT', 'empty': 'N', 'snapshot': 'INLINE_HISTORY',
                   'reject_file': 'F', 'reject_schema': 'SCHEMA', 'ddl': 'I', 'identity': 'I'}[args.mode]
        if manifest.get('case') != profile:
            raise ValueError('Wrong reference fixture profile for this test mode')
        if manifest.get('status') != 'verified_reference_only' or manifest.get('automatic_migration') is not False:
            raise ValueError('Fixture is not a verified, explicitly versioned reference result')
        result['artifacts'].append({'path': str(manifest_path)})
        result['fixture'] = {'profile': profile, 'manifest_sha256': sha256(manifest_path),
                             'reference_commit': manifest['reference_commit'], 'duckdb_commit': manifest['duckdb_commit']}

        def connect(db):
            conn = psycopg2.connect(host=str(ROOT / '.runtime/opengauss-native-lake/debug/socket'),
                                    port=55560, dbname=db, user=os.getenv('USER', 'qiumc'),
                                    connect_timeout=10, application_name='og_lake_native_inline')
            conn.autocommit = True
            connections.append(conn)
            query(conn, 'SET statement_timeout=15000')
            return conn

        admin = connect('postgres')
        database = 'lake_inline_' + datetime.datetime.now().strftime('%Y%m%d%H%M%S') + '_' + uuid.uuid4().hex[:8]
        allocation = {'schema_version': 1, 'run_id': args.run_id, 'attempt': str(output.parent),
                      'database': database, 'database_oid': None, 'phase': 'planned',
                      'owner_oid': scalar(admin, 'SELECT oid FROM pg_roles WHERE rolname=current_user'),
                      'postmaster_start': str(scalar(admin, 'SELECT pg_postmaster_start_time()')),
                      'marker': 'og_lake_inline_attempt:' + args.run_id + ':' + uuid.uuid4().hex}
        write_json(output.parent / 'database-owner.json', allocation)
        query(admin, sql.SQL('CREATE DATABASE {}').format(sql.Identifier(database)))
        allocation['database_oid'] = scalar(admin, 'SELECT oid FROM pg_database WHERE datname=%s', (database,))
        allocation['phase'] = 'identified'
        write_json(output.parent / 'database-owner.json', allocation)
        query(admin, sql.SQL('COMMENT ON DATABASE {} IS {}').format(
            sql.Identifier(database), sql.Literal(allocation['marker'])))
        allocation['phase'] = 'owned'
        write_json(output.parent / 'database-owner.json', allocation)
        a, b, c = [connect(database) for unused in range(3)]
        query(a, "CREATE EXTENSION og_lake VERSION '0.7'" if args.mode=='reject_file' else 'CREATE EXTENSION og_lake')
        if args.test_hold_after_allocation:
            write_json(output.parent / 'hold-ready.json',
                       {'run_id': args.run_id, 'database_oid': allocation['database_oid'],
                        'reason': 'Owned database and real target connections exist; awaiting controlled test termination.'})
            while True:
                time.sleep(0.1)
        capabilities = json.loads(scalar(a, 'SELECT og_lake.host_status()'))
        result['actual_host_capabilities'] = capabilities
        source = Path(os.getenv('OG_LAKE_SOURCE', str(ROOT / '.worktrees/opengauss-native-lake')))
        control = source / 'contrib/og_lake/og_lake.control'
        versions = re.findall(r"^default_version\s*=\s*'([^']+)'\s*$", control.read_text(), re.M)
        if len(versions) != 1:
            raise Blocked('Product control file does not declare exactly one default version')
        result['expected_module_version'] = versions[0]
        if capabilities.get('module_version') != versions[0] or capabilities.get('inline_scan') is not True:
            raise Blocked('Installed module version/capabilities do not match the product control file')
        check('slice advertises its actual capability boundary',
              {name: capabilities.get(name) for name in ('ordinary_user_access', 'file_scan', 'smp')},
              {'ordinary_user_access': versions[0] in ('0.5', '0.6', '0.7', '0.8', '0.9', '0.10', '0.11'), 'file_scan': False, 'smp': False})
        if versions[0] in ('0.5', '0.6', '0.7', '0.8', '0.9', '0.10', '0.11'):
            check('ordinary access is conditional, not granted by installation',
                  capabilities.get('ordinary_access_requires'),'extension-0.5-and-configured-private-owner')
        check('ordinary SQL works before attach', scalar(a, 'SELECT 6*7'), 42)
        metadata_schema = 'lake_metadata'
        server = 'lake_reference'
        query(a, 'CREATE SCHEMA biz')

        def export(label=None):
            export_path = output.parent / ('catalog-export-' + (label or profile) + '.json')
            exported, source_manifest = export_reference(DRIVER, manifest_path, export_path, args.run_id, label)
            result['artifacts'] += [{'path': str(export_path)}, {'path': str(export_path.with_suffix('.log'))}]
            return exported

        before_export = export('S10' if args.mode == 'snapshot' else None)
        after_export = export('S12') if args.mode == 'snapshot' else None
        query(a, 'BEGIN')
        try:
            loaded = import_catalog(a, metadata_schema, before_export)
            query(a, 'COMMIT')
        except BaseException:
            query(a, 'ROLLBACK')
            raise
        check('all reference physical tables and values imported exactly', len(loaded), len(before_export['tables']))
        result['imported_row_counts'] = loaded

        def attach(conn, server_name=server):
            query(conn, 'SELECT og_lake.attach_catalog(%s::name,%s::name,NULL::text,true)',
                  (server_name, metadata_schema))

        def bind(conn, table='orders'):
            query(conn, foreign_table_statement('biz', table, server, manifest['table_id'], manifest['schema']))

        # These are real DDL rollback tests, not inserts fabricated in host bindings.
        query(a, 'BEGIN')
        attach(a)
        check('attach writes a real catalog binding', scalar(a, 'SELECT count(*) FROM og_lake_internal.catalog_binding'), 1)
        query(a, 'ROLLBACK')
        check('attach rollback removes host server', scalar(a, 'SELECT count(*) FROM pg_foreign_server WHERE srvname=%s', (server,)), 0)
        check('attach rollback removes catalog binding', scalar(a, 'SELECT count(*) FROM og_lake_internal.catalog_binding'), 0)
        attach(a)
        if args.mode in ('reject_file', 'reject_schema'):
            reject('unsupported reference layout rejects before a bound table or delivered row', a,
                   foreign_table_statement('biz', 'orders', server, manifest['table_id'], manifest['schema']), '0A000')
            check('rejected layout leaves no relation binding', scalar(a, 'SELECT count(*) FROM og_lake_internal.relation_binding'), 0)
            check('ordinary SQL survives layout rejection', scalar(a, 'SELECT 42'), 42)
            query(a, 'DROP EXTENSION og_lake CASCADE')
            check('extension drops after rejected layout', scalar(a, "SELECT count(*) FROM pg_extension WHERE extname='og_lake'"), 0)
            check('rejected layout extension drop leaves no lake server',
                  scalar(a, 'SELECT count(*) FROM pg_foreign_server WHERE srvname=%s', (server,)), 0)
            check('rejected layout extension drop leaves no lake wrapper',
                  scalar(a, "SELECT count(*) FROM pg_foreign_data_wrapper WHERE fdwname='og_lake_fdw'"), 0)
            check('rejected layout extension drop removes its private schemas',
                  scalar(a, "SELECT count(*) FROM pg_namespace WHERE nspname IN ('og_lake','og_lake_internal')"), 0)
            result['status'] = 'pass'
            raise RejectionVerified()

        query(a, 'BEGIN')
        bind(a)
        check('foreign DDL writes persistent column mappings',
              scalar(a, 'SELECT count(*) FROM og_lake_internal.column_binding'), len(manifest['schema']))
        query(a, 'ROLLBACK')
        check('foreign DDL rollback leaves no table binding', scalar(a, 'SELECT count(*) FROM og_lake_internal.table_binding'), 0)
        bind(a)
        reject('same lake table cannot acquire duplicate host binding', a,
               foreign_table_statement('biz', 'duplicate_orders', server, manifest['table_id'], manifest['schema']), '23505')
        check('duplicate DDL failure preserved original binding', scalar(a, 'SELECT count(*) FROM og_lake_internal.table_binding'), 1)

        columns = [column['name'] for column in manifest['schema']]
        projection = sql.SQL(',').join(map(sql.Identifier, columns))
        select_all = sql.SQL('SELECT {} FROM biz.orders').format(projection)

        def expected(records):
            return [[record[column] for column in columns] for record in records]

        def actual(conn, suffix=None):
            return [list(row) for row in query(conn, select_all + (suffix or sql.SQL('')))]

        if args.mode == 'snapshot':
            states = {state['label']: state for state in manifest['catalog_states']}
            initial_records = states['S10']['expected']
        else:
            initial_records = manifest['expected']
        initial = expected(initial_records)
        check('native complete typed multiset matches reference', multiset(actual(a)), multiset(initial))
        if args.mode == 'ddl':
            # Every failed mutation must leave both native catalogs and host
            # bindings unchanged.  A subsequent successful read is necessary:
            # accepting ALTER and rejecting only the next scan is not enough.
            def catalog_state():
                statements = {
                    'relation': "SELECT oid,relname,relnamespace,relowner FROM pg_class WHERE oid='biz.orders'::regclass",
                    'attributes': "SELECT attnum,attname,atttypid,atttypmod,attcollation,attisdropped FROM pg_attribute WHERE attrelid='biz.orders'::regclass AND attnum>0 ORDER BY attnum",
                    'foreign_table': "SELECT ftserver,ftoptions FROM pg_foreign_table WHERE ftrelid='biz.orders'::regclass",
                    'server': "SELECT oid,srvname,srvowner,srvfdw,srvtype,srvversion,srvoptions FROM pg_foreign_server WHERE srvname='lake_reference'",
                    'wrapper': "SELECT oid,fdwname,fdwowner,fdwhandler,fdwvalidator,fdwoptions FROM pg_foreign_data_wrapper WHERE fdwname='og_lake_fdw'",
                }
                saved = {name: query(a, statement) for name, statement in statements.items()}
                for table in ('catalog_binding', 'table_binding', 'relation_binding', 'column_binding'):
                    saved[table] = sorted(query(a, sql.SQL('SELECT * FROM og_lake_internal.{}').format(
                        sql.Identifier(table))), key=repr)
                return saved

            original_catalogs = catalog_state()
            current_owner = sql.Identifier(scalar(a, 'SELECT current_user'))
            attempts = [
                ('column type', sql.SQL('ALTER FOREIGN TABLE biz.orders ALTER COLUMN id TYPE bigint')),
                ('foreign options', sql.SQL("ALTER FOREIGN TABLE biz.orders OPTIONS (SET table_id '999999')")),
                ('column rename', sql.SQL('ALTER FOREIGN TABLE biz.orders RENAME COLUMN id TO other_id')),
                ('relation rename', sql.SQL('ALTER FOREIGN TABLE biz.orders RENAME TO renamed_orders')),
                ('relation schema', sql.SQL('ALTER FOREIGN TABLE biz.orders SET SCHEMA public')),
                ('relation owner', sql.SQL('ALTER FOREIGN TABLE biz.orders OWNER TO {}').format(current_owner)),
                ('server version', sql.SQL("ALTER SERVER lake_reference VERSION 'changed'")),
                ('server owner', sql.SQL('ALTER SERVER lake_reference OWNER TO {}').format(current_owner)),
                ('server rename', sql.SQL('ALTER SERVER lake_reference RENAME TO renamed_reference')),
                ('wrapper options', sql.SQL("ALTER FOREIGN DATA WRAPPER og_lake_fdw OPTIONS (ADD lake_test 'changed')")),
                ('wrapper owner', sql.SQL('ALTER FOREIGN DATA WRAPPER og_lake_fdw OWNER TO {}').format(current_owner)),
                ('wrapper rename', sql.SQL('ALTER FOREIGN DATA WRAPPER og_lake_fdw RENAME TO renamed_lake_fdw')),
            ]
            for label, statement in attempts:
                reject('unsupported ALTER rejects ' + label, a, statement, '0A000')
                check('failed ALTER preserves catalogs and bindings: ' + label, catalog_state(), original_catalogs)
                check('native read survives rejected ALTER: ' + label, multiset(actual(a)), multiset(initial))
            query(a, 'CREATE TABLE biz.alter_control (id integer)')
            query(a, 'ALTER TABLE biz.alter_control RENAME COLUMN id TO renamed_id')
            query(a, 'ALTER TABLE biz.alter_control RENAME TO renamed_control')
            query(a, 'INSERT INTO biz.renamed_control VALUES (42)')
            check('native lake ALTER guard leaves ordinary tables usable',
                  scalar(a, 'SELECT renamed_id FROM biz.renamed_control'), 42)
            query(a, 'DROP TABLE biz.renamed_control')
        plan = '\n'.join(row[0] for row in query(a, sql.SQL('EXPLAIN ') + select_all))
        plan_path = output.parent / 'native-inline-explain.txt'
        plan_path.write_text(plan + '\n')
        result['artifacts'].append({'path': str(plan_path)})
        check('planner reaches a native Foreign Scan for legitimate inline inputs', 'Foreign Scan' in plan)
        check('count star without projected business columns', scalar(a, 'SELECT count(*) FROM biz.orders'), len(initial))
        old_dop = scalar(a, 'SHOW query_dop')
        try:
            query(a, 'SET query_dop=4')
            dop_plan = '\n'.join(row[0] for row in query(a, sql.SQL('EXPLAIN ') + select_all))
            dop_path = output.parent / 'query-dop-4-explain.txt'
            dop_path.write_text(dop_plan + '\n')
            result['artifacts'].append({'path': str(dop_path)})
            check('query_dop 4 does not introduce duplicate inline rows', multiset(actual(a)), multiset(initial))
            check('simple inline scan has no parallel Stream or Gather path',
                  not any(marker in dop_plan.lower() for marker in ('stream', 'gather', 'parallel foreign')))
        finally:
            query(a, 'SET query_dop=%s', (old_dop,))
        reversed_projection = sql.SQL(',').join(sql.Identifier(name) for name in reversed(columns))
        check('projection order is preserved',
              multiset([list(row) for row in query(a, sql.SQL('SELECT {} FROM biz.orders').format(reversed_projection))]),
              multiset([list(reversed(row)) for row in initial]))
        check('executor predicate is applied', multiset(actual(a, sql.SQL(' WHERE id >= 2'))),
              multiset(expected([record for record in initial_records if record['id'] >= 2])))
        query(a, 'PREPARE lake_inline_prepared(integer) AS SELECT count(*) FROM biz.orders WHERE id >= $1')
        for bound in (0, 2, 999):
            check('prepared predicate bound ' + str(bound), scalar(a, 'EXECUTE lake_inline_prepared(%s)', (bound,)),
                  sum(record['id'] >= bound for record in initial_records))
        query(a, 'DEALLOCATE lake_inline_prepared')
        rescan = query(a, 'SELECT x,(SELECT count(*) FROM biz.orders WHERE id<=x) FROM generate_series(0,4) t(x) ORDER BY x')
        check('correlated rescans retain the input snapshot', [list(row) for row in rescan],
              [[value, sum(record['id'] <= value for record in initial_records)] for value in range(5)])
        if initial:
            check('limit early stop returns one valid reference row', len(actual(a, sql.SQL(' LIMIT 1'))), 1)
            reject('executor error remains a real error', a, 'SELECT 1/(id-id) FROM biz.orders LIMIT 1', '22012')
            check('scan remains healthy after executor error', multiset(actual(a)), multiset(initial))
        if args.mode == 'typed':
            type_set = {column['type'] for column in manifest['schema']}
            check('typed fixture exercises all initial scalar adapters',
                  {'BOOLEAN', 'INTEGER', 'BIGINT', 'VARCHAR'}.issubset(type_set))
            text_columns = [column['name'] for column in manifest['schema'] if column['type'] == 'VARCHAR']
            check('typed fixture contains actual NULL values', any(value is None for row in initial for value in row))
            result['notes'].append('TYPED verifies only the actual exported text values; long text and TOAST require the separate inline_long_text case.')
            for name in text_columns:
                found = actual(a, sql.SQL(' WHERE {} IS NULL').format(sql.Identifier(name)))
                check('NULL predicate on ' + name, multiset(found),
                      multiset(expected([record for record in initial_records if record[name] is None])))

        if args.mode == 'long_text':
            text_columns = [column['name'] for column in manifest['schema'] if column['type'] == 'VARCHAR']
            check('real reference fixture contains long text',
                  any(isinstance(record[name], str) and len(record[name]) > 8192
                      for name in text_columns for record in initial_records))
            for name in text_columns:
                lengths = query(a, sql.SQL('SELECT id,length({}),octet_length({}) FROM biz.orders ORDER BY id').format(
                    sql.Identifier(name), sql.Identifier(name)))
                expected_lengths = [[row['id'], None if row[name] is None else len(row[name]),
                                     None if row[name] is None else len(row[name].encode('utf-8'))]
                                    for row in sorted(initial_records, key=lambda row: row['id'])]
                check('full text lengths and UTF-8 bytes: ' + name, [list(row) for row in lengths], expected_lengths)
            inline_names = [row['table_name'] for table in before_export['tables']
                            if table['table_name'] == 'ducklake_inlined_data_tables' for row in table['rows']]
            toast_chunks = {}
            for name in inline_names:
                toast = query(a, 'SELECT tn.nspname,t.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace '
                                'JOIN pg_class t ON t.oid=c.reltoastrelid JOIN pg_namespace tn ON tn.oid=t.relnamespace '
                                'WHERE n.nspname=%s AND c.relname=%s', (metadata_schema, name))
                for schema_name, table_name in toast:
                    toast_chunks[name] = scalar(a, sql.SQL('SELECT count(*) FROM {}.{}').format(
                        sql.Identifier(schema_name), sql.Identifier(table_name)))
            result['actual_toast_chunks'] = toast_chunks
            check('long text actually exercises host external TOAST storage', sum(toast_chunks.values()) > 0)
            check('complete long text remains exact on another native scan', multiset(actual(a)), multiset(initial))

        if args.mode == 'identity':
            metadata_oid = scalar(a, 'SELECT oid FROM pg_namespace WHERE nspname=%s', (metadata_schema,))
            check('catalog binding stores the physical schema OID',
                  scalar(a, 'SELECT metadata_schema_oid FROM og_lake_internal.catalog_binding'), metadata_oid)
            query(a, 'PREPARE lake_identity_read AS SELECT count(*) FROM biz.orders')
            check('prepared identity query initially reads the real fixture',
                  scalar(a, 'EXECUTE lake_identity_read'), len(initial))
            renamed_schema = 'lake_metadata_original'
            query(b, sql.SQL('ALTER SCHEMA {} RENAME TO {}').format(
                sql.Identifier(metadata_schema), sql.Identifier(renamed_schema)))
            query(b, sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(metadata_schema)))
            check('metadata original name was reused by a different empty schema',
                  scalar(a, 'SELECT oid FROM pg_namespace WHERE nspname=%s', (metadata_schema,)) != metadata_oid)
            check('renamed schema retains its original identity',
                  scalar(a, 'SELECT oid FROM pg_namespace WHERE nspname=%s', (renamed_schema,)), metadata_oid)
            check('binding does not drift to a reused schema name',
                  scalar(a, 'SELECT metadata_schema_oid FROM og_lake_internal.catalog_binding'), metadata_oid)
            check('new scan reads bound OID after metadata schema rename and name reuse',
                  multiset(actual(a)), multiset(initial))
            check('prepared query reads bound OID after metadata schema rename and name reuse',
                  scalar(a, 'EXECUTE lake_identity_read'), len(initial))
            query(a, 'DEALLOCATE lake_identity_read')
            query(b, sql.SQL('DROP SCHEMA {}').format(sql.Identifier(metadata_schema)))
            query(b, sql.SQL('ALTER SCHEMA {} RENAME TO {}').format(
                sql.Identifier(renamed_schema), sql.Identifier(metadata_schema)))
            check('restored metadata schema remains readable', multiset(actual(a)), multiset(initial))
            result['notes'].append('Schema rename/name reuse is tested between real SQL statements. This is not a deterministic pause between SPI_prepare and SPI_execute_snapshot and does not prove all concurrent rename interleavings.')

        if args.mode == 'snapshot':
            query(b, 'BEGIN ISOLATION LEVEL READ COMMITTED')
            check('retained RC lake reader sees initial reference state', multiset(actual(b)), multiset(initial))
            query(c, 'BEGIN')
            replace_catalog_values(c, metadata_schema, before_export, after_export)
            check('uncommitted replacement is invisible to another RC statement', multiset(actual(a)), multiset(initial))
            query(c, 'COMMIT')
            latest = expected(states['S12']['expected'])
            check('retained RC lake reader retains metadata and inline from its original host snapshot', multiset(actual(b)), multiset(initial))
            check('new RC sees the complete new reference state', multiset(actual(a)), multiset(latest))
            id_position = columns.index('id')
            two_scans = query(b, 'SELECT a.id,b.id FROM biz.orders a JOIN biz.orders b ON a.id=b.id ORDER BY a.id')
            ids = sorted(row[id_position] for row in initial)
            check('two lake scans in one retained RC statement agree', [list(row) for row in two_scans], [[value, value] for value in ids])
            query(b, 'COMMIT')
            check('new transaction after RC transaction end sees the new reference state', multiset(actual(b)), multiset(latest))
            initial = latest

        # Cancellation occurs in a real shared guard wait, with statement_timeout
        # as the bounded outcome; no sleeps or mocked lock ownership are used.
        server_oid = scalar(a, 'SELECT oid FROM pg_foreign_server WHERE srvname=%s', (server,))
        query(b, 'BEGIN')
        check('exclusive control guard acquired', scalar(b,
              'SELECT og_lake_internal.test_catalog_guard_acquire(%s::oid,true)', (server_oid,)))
        query(a, 'SET statement_timeout=250')
        try:
            reject('reader waits and can be cancelled rather than returning empty', a, select_all, '57014')
        finally:
            query(a, 'SET statement_timeout=15000')
            query(b, 'ROLLBACK')
        check('subsequent scan succeeds after cancelled guard wait', multiset(actual(a)), multiset(initial))
        check('reader leaves no orphan catalog guard',
              json.loads(scalar(a, 'SELECT og_lake_internal.test_xact_state()'))['catalog_guards'], 0)

        query(a, 'BEGIN')
        query(a, 'DROP FOREIGN TABLE biz.orders')
        check('drop removes table binding within transaction', scalar(a, 'SELECT count(*) FROM og_lake_internal.table_binding'), 0)
        query(a, 'ROLLBACK')
        check('drop rollback restores readable relation and binding', multiset(actual(a)), multiset(initial))
        query(a, 'DROP FOREIGN TABLE biz.orders')
        for table in ('table_binding', 'relation_binding', 'column_binding'):
            check('normal drop cleans ' + table, scalar(a, sql.SQL('SELECT count(*) FROM og_lake_internal.{}').format(sql.Identifier(table))), 0)
        bind(a)
        check('rebind after normal drop has no stale mapping', multiset(actual(a)), multiset(initial))
        metadata_count_before = scalar(a, 'SELECT count(*) FROM lake_metadata.ducklake_snapshot')
        query(a, 'DROP EXTENSION og_lake CASCADE')
        check('extension drop with attached server and bound table succeeds',
              scalar(a, "SELECT count(*) FROM pg_extension WHERE extname='og_lake'"), 0)
        check('extension cascade removes the bound foreign relation', scalar(a,
              "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='biz' AND c.relname='orders'"), 0)
        check('extension cascade leaves no lake server',
              scalar(a, 'SELECT count(*) FROM pg_foreign_server WHERE srvname=%s', (server,)), 0)
        check('extension cascade leaves no lake wrapper',
              scalar(a, "SELECT count(*) FROM pg_foreign_data_wrapper WHERE fdwname='og_lake_fdw'"), 0)
        check('extension cascade removes its private schemas',
              scalar(a, "SELECT count(*) FROM pg_namespace WHERE nspname IN ('og_lake','og_lake_internal')"), 0)
        check('extension drop preserves reference protocol data', scalar(a, 'SELECT count(*) FROM lake_metadata.ducklake_snapshot'), metadata_count_before)
        check('ordinary SQL remains usable after extension removal', scalar(a, 'SELECT 42'), 42)
        result['status'] = 'pass'
    except RejectionVerified:
        pass
    except Blocked as exc:
        result.update(status='blocked', reason=str(exc))
    except BaseException as exc:
        result['error'] = repr(exc)
        result['traceback'] = traceback.format_exc()
        if not result['assertions'] or result['assertions'][-1]['status'] != 'fail':
            result['assertions'].append({'name': 'native inline execution completed', 'status': 'fail', 'details': repr(exc)})
    finally:
        for conn in connections:
            if conn is not admin:
                conn.close()
        if admin is not None:
            admin.close()
        try:
            cleanup = subprocess.run([sys.executable, str(DRIVER / 'tests/inline_cleanup.py'),
                                      '--run-id', args.run_id, '--attempt', str(output.parent)],
                                     capture_output=True, text=True, timeout=30)
            result['cleanup_exit'] = cleanup.returncode
            if cleanup.returncode:
                result.update(status='fail', cleanup_error=cleanup.stderr + cleanup.stdout)
            if (output.parent / 'database-owner.json').is_file():
                result['artifacts'].append({'path': str(output.parent / 'database-owner.json')})
            result['artifacts'].extend({'path': str(path)} for path in output.parent.glob('database-cleanup-*.json'))
        except BaseException as exc:
            result.update(status='fail', cleanup_error=repr(exc))
        # Counters are a comparison convenience, not JSON object keys in evidence.
        output.write_text(json.dumps(result, ensure_ascii=False, indent=2, default=str) + '\n')
        print(json.dumps({'status': result['status'], 'mode': args.mode, 'output': str(output),
                          'assertions': len(result['assertions'])}, ensure_ascii=False))
    return {'pass': 0, 'blocked': 2}.get(result['status'], 1)


if __name__ == '__main__':
    raise SystemExit(main())
