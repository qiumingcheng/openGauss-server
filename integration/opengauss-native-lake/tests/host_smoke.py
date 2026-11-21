#!/usr/bin/env python3
"""Real openGauss sessions; no mocked locks, snapshots or server lifecycle."""
import argparse
import datetime
import json
import os
from pathlib import Path
import re
import secrets
import sys
import time


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--run-id', default=os.getenv('OG_LAKE_RUN_ID', 'manual'))
    parser.add_argument('--output', default=os.getenv('OG_LAKE_RESULT_FILE'))
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[3]
    control = root / '.worktrees/opengauss-native-lake/contrib/og_lake/og_lake.control'
    version_match = re.search(r"^default_version\s*=\s*'([^']+)'", control.read_text(), re.MULTILINE)
    if version_match is None or version_match.group(1) not in ('0.2', '0.3', '0.4', '0.5', '0.6', '0.7', '0.8', '0.9', '0.10', '0.11'):
        parser.error('host test requires a reviewed native-lake runtime version')
    runtime_version = version_match.group(1)
    output = Path(args.output) if args.output else root / '公共文档/openGauss原生湖存储/验证' / args.run_id / 'host.json'
    output.parent.mkdir(parents=True, exist_ok=True)
    result = {'status': 'fail', 'scope': 'ABI-01/host, JOB-02/local, GC-04/guard',
              'assertions': [], 'artifacts': [], 'notes': [
                  'No lake data scan, SDK, physical deletion or RAC capability is claimed.',
                  'Catalog guard release is tested with no in-flight SDK references.']}
    connections = []
    databases = []
    admin = None
    role = None
    try:
        import psycopg2
        from psycopg2 import sql
        host = str(root / '.runtime/opengauss-native-lake/debug/socket')
        port = int(os.getenv('OG_LAKE_TEST_PORT', '55560'))

        def connect(database, user=None, password=None):
            conn = psycopg2.connect(host=host, port=port, dbname=database,
                                    user=user or os.getenv('USER', 'qiumc'), password=password, connect_timeout=10,
                                    application_name='og_lake_host_smoke')
            conn.autocommit = True
            connections.append(conn)
            with conn.cursor() as cur:
                cur.execute('SET statement_timeout = 10000')
            return conn

        def query(conn, statement, params=None):
            with conn.cursor() as cur:
                cur.execute(statement, params)
                return cur.fetchall() if cur.description else None

        def scalar(conn, statement, params=None):
            return query(conn, statement, params)[0][0]

        def check(name, observed, expected=True):
            passed = observed == expected
            result['assertions'].append({'name': name, 'status': 'pass' if passed else 'fail',
                                         'details': {'observed': observed, 'expected': expected}})
            if not passed:
                raise AssertionError(name)

        def reject(name, conn, statement, state, params=None):
            try:
                query(conn, statement, params)
            except psycopg2.Error as exc:
                check(name, exc.pgcode, state)
            else:
                check(name, 'accepted', state)

        marker_count_sql = ("SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON p.pronamespace=n.oid "
                            "WHERE n.nspname='og_lake_internal' AND p.proname='row_locator'")

        def check_marker(conn, label):
            observed = query(conn, "SELECT p.pronargs,p.proargtypes::text,p.prorettype,p.proisstrict,p.provolatile,"
                             "p.prosrc,p.probin,p.prolang=(SELECT oid FROM pg_language WHERE lanname='c'),"
                             "EXISTS (SELECT 1 FROM pg_depend d JOIN pg_extension e ON e.oid=d.refobjid "
                             "WHERE d.classid='pg_proc'::regclass AND d.objid=p.oid "
                             "AND d.refclassid='pg_extension'::regclass AND d.deptype='e' AND e.extname='og_lake') "
                             "FROM pg_proc p JOIN pg_namespace n ON p.pronamespace=n.oid "
                             "WHERE n.nspname='og_lake_internal' AND p.proname='row_locator'")
            check(label + ' exact marker signature implementation and extension ownership', observed,
                  [(1,'2283',17,True,'s','og_lake_row_locator','$libdir/og_lake',True,True)])

        def claim(conn):
            return scalar(conn, 'SELECT og_lake_internal.test_maintenance_claim()')

        def release(conn):
            return scalar(conn, 'SELECT og_lake_internal.test_maintenance_release()')

        def guard(conn, oid, exclusive=False):
            return scalar(conn, 'SELECT og_lake_internal.test_catalog_guard_acquire(%s::oid,%s)', (oid, exclusive))

        def unguard(conn, oid):
            return scalar(conn, 'SELECT og_lake_internal.test_catalog_guard_release(%s::oid)', (oid,))

        def state(conn):
            return json.loads(scalar(conn, 'SELECT og_lake_internal.test_xact_state()'))

        admin = connect('postgres')
        if os.getenv('OG_LAKE_EXPECT_THREAD_POOL'):
            check('actual thread-pool mode', scalar(admin, 'SHOW enable_thread_pool'),
                  os.environ['OG_LAKE_EXPECT_THREAD_POOL'])
        suffix = f'{datetime.datetime.now():%Y%m%d%H%M%S}_{os.getpid()}'
        dbname = 'lake_host_' + suffix
        dbname2 = 'lake_host_other_' + suffix
        for db in [dbname, dbname2]:
            query(admin, sql.SQL('CREATE DATABASE {}').format(sql.Identifier(db)))
            databases.append(db)
        a, b, c = [connect(dbname) for _ in range(3)]
        check('ordinary SQL before installation', scalar(a, 'SELECT sum(x) FROM generate_series(1,10) x'), 55)
        query(a, 'BEGIN')
        query(a, 'CREATE EXTENSION og_lake')
        query(a, 'ROLLBACK')
        check('extension install rolls back', scalar(a, "SELECT count(*) FROM pg_extension WHERE extname='og_lake'"), 0)
        query(a, 'CREATE EXTENSION og_lake')
        check('default installation uses the reviewed runtime SQL version',
              scalar(a, "SELECT extversion FROM pg_extension WHERE extname='og_lake'"), runtime_version)
        if runtime_version in ('0.4', '0.5', '0.6', '0.7', '0.8', '0.9', '0.10', '0.11'):
            check_marker(a, 'default ' + runtime_version + ' install')
            reject('marker cannot be directly executed outside a native scan slot', a,
                   'SELECT og_lake_internal.row_locator(1)', '0A000')
        query(a, 'CREATE TABLE ordinary_tx(k integer PRIMARY KEY, v text)')
        query(a, "INSERT INTO ordinary_tx VALUES (1,'committed')")
        query(a, 'BEGIN')
        query(a, "UPDATE ordinary_tx SET v='rolled back'")
        query(a, 'ROLLBACK')
        check('ordinary DML rollback unchanged', scalar(b, 'SELECT v FROM ordinary_tx WHERE k=1'), 'committed')
        query(a, 'BEGIN')
        check('ordinary DELETE still returns the real heap row',
              query(a, 'DELETE FROM ordinary_tx WHERE k=1 RETURNING k,v'), [(1, 'committed')])
        query(a, 'ROLLBACK')
        check('ordinary DELETE rollback unchanged',
              query(b, 'SELECT k,v FROM ordinary_tx'), [(1, 'committed')])
        if not scalar(a, "SELECT count(*) FROM pg_extension WHERE extname='file_fdw'"):
            query(a, 'CREATE EXTENSION file_fdw')
        query(a, 'CREATE SERVER legacy_host_file FOREIGN DATA WRAPPER file_fdw')
        legacy = output.parent / 'legacy_fdw.csv'
        legacy.write_text('1,alpha\n2,beta\n')
        result['artifacts'].append({'path': str(legacy.resolve())})
        query(a, sql.SQL("CREATE FOREIGN TABLE legacy_probe(i integer, v text) SERVER legacy_host_file "
                         "OPTIONS (filename {}, format 'csv')").format(sql.Literal(str(legacy.resolve()))))
        check('existing static file_fdw still scans', query(a, 'SELECT * FROM legacy_probe ORDER BY i'),
              [(1, 'alpha'), (2, 'beta')])
        for name, conn in [('installing session', a), ('fresh session', b)]:
            status = json.loads(scalar(conn, 'SELECT og_lake.host_status()'))
            check(name + ' static ABI', status['host_abi'], 1)
            check(name + ' module version', status['module_version'], runtime_version)
            check(name + ' inline-only scan capability', status['inline_scan'], True)
            if runtime_version in ('0.3', '0.4', '0.5', '0.6', '0.7', '0.8', '0.9', '0.10', '0.11'):
                check(name + ' bounded inline INSERT capability', status['inline_insert'], True)
                check(name + ' INSERT requires upgraded SQL objects', status['write_requires_extension_version'], '0.3')
            if runtime_version in ('0.4', '0.5', '0.6', '0.7', '0.8', '0.9', '0.10', '0.11'):
                check(name + ' runtime capability scope is explicit', status['capability_scope'], 'runtime_binary')
                check(name + ' bounded inline UPDATE capability', status['inline_update'], True)
                check(name + ' bounded inline DELETE capability', status['inline_delete'], True)
                check(name + ' UPDATE/DELETE require upgraded SQL objects', status['update_delete_requires_extension_version'], '0.4')
            for capability in ['data_scan', 'writes', 'sdk', 'physical_gc', 'planner_bridge', 'stream_bridge']:
                check(name + ' reports unavailable ' + capability, status[capability], False)
            check(name + ' reports conditional ordinary-user implementation',
                  status['ordinary_user_access'],runtime_version in ('0.5', '0.6', '0.7', '0.8', '0.9', '0.10', '0.11'))
            if runtime_version in ('0.5', '0.6', '0.7', '0.8', '0.9', '0.10', '0.11'):
                check(name + ' ordinary access requires explicit configuration',
                      status.get('ordinary_access_requires'),'extension-0.5-and-configured-private-owner')
            if runtime_version in ('0.6', '0.7', '0.8', '0.9', '0.10', '0.11'):
                check(name + ' runtime includes catalog initialization', status.get('catalog_create'), True)
                check(name + ' catalog creation still needs explicit version/configuration',
                      status.get('catalog_create_requires'), 'extension-0.6-and-configured-private-owner')
                check(name + ' catalog initialization storage boundary',
                      status.get('catalog_create_storage'), 'local-absolute-root-without-credentials')
            for capability in ['file_scan', 'schema_evolution', 'smp']:
                check(name + ' reports unavailable ' + capability, status[capability], False)

        check('maintenance first owner', claim(a))
        check('maintenance excludes other session', claim(b), False)
        check('maintenance duplicate claim is idempotent', claim(a))
        query(a, 'SELECT pg_advisory_lock(4762381::bigint)')
        query(a, 'SELECT pg_advisory_unlock_all()')
        check('unlock_all releases application advisory locks',
              scalar(b, 'SELECT pg_try_advisory_lock(4762381::bigint)'))
        query(b, 'SELECT pg_advisory_unlock_all()')
        check('unlock_all preserves internal maintenance claim', claim(b), False)
        query(a, 'SELECT pg_advisory_lock(4762381::bigint)')
        query(a, 'SELECT pg_advisory_lock(4762,381)')
        discard_supported = True
        try:
            query(a, 'DISCARD ALL')
        except psycopg2.Error as exc:
            # This baseline rejects DISCARD in utility.cpp before DiscardAll.
            if exc.pgcode != '0A000' or 'DISCARD statement is not yet supported' not in str(exc):
                raise
            discard_supported = False
            result['notes'].append('Host rejects DISCARD SQL; its internal cleanup branch is not runtime-certified.')
            check('unsupported DISCARD leaves maintenance protected', claim(b), False)
            query(a, 'SELECT pg_advisory_unlock_all()')
        result['discard_sql_supported'] = discard_supported
        query(a, 'SET statement_timeout = 10000')
        cleanup_label = 'discard all' if discard_supported else 'unlock_all'
        check(cleanup_label + ' releases int8 application lock',
              scalar(b, 'SELECT pg_try_advisory_lock(4762381::bigint)'))
        check(cleanup_label + ' releases int4 application lock',
              scalar(b, 'SELECT pg_try_advisory_lock(4762,381)'))
        query(b, 'SELECT pg_advisory_unlock_all()')
        check(cleanup_label + ' preserves maintenance ownership', claim(b), False)
        query(a, 'BEGIN'); query(a, 'SELECT 1'); query(a, 'COMMIT')
        check('maintenance survives commit', claim(b), False)
        query(a, 'BEGIN'); query(a, 'SELECT 1'); query(a, 'ROLLBACK')
        check('maintenance survives abort', claim(b), False)
        check('maintenance release', release(a))
        check('maintenance repeated release false', release(a), False)
        check('single release balances duplicate claim', claim(b))
        check('second owner releases', release(b))

        other = connect(dbname2)
        query(other, 'CREATE EXTENSION og_lake')
        check('claim original database', claim(a))
        check('other database can maintain independently', claim(other))
        release(a); release(other)
        exiting = connect(dbname)
        check('disconnecting owner acquires', claim(exiting))
        exiting.close()
        deadline = time.monotonic() + 5
        acquired = claim(b)
        while not acquired and time.monotonic() < deadline:
            time.sleep(0.02)
            acquired = claim(b)
        check('disconnect releases session ownership', acquired)
        release(b)

        query(a, 'CREATE SERVER lake_guard_one FOREIGN DATA WRAPPER og_lake_fdw')
        query(a, 'CREATE SERVER lake_guard_two FOREIGN DATA WRAPPER og_lake_fdw')
        server1 = scalar(a, "SELECT oid FROM pg_foreign_server WHERE srvname='lake_guard_one'")
        server2 = scalar(a, "SELECT oid FROM pg_foreign_server WHERE srvname='lake_guard_two'")
        for conn in [a, b, c]:
            query(conn, 'BEGIN')
        check('reader shared guard', guard(a, server1))
        query(a, 'SELECT pg_advisory_unlock_all()')
        check('unlock_all preserves catalog protection', guard(c, server1, True), False)
        check('writer shared guard compatible', guard(b, server1))
        check('GC defers with readers', guard(c, server1, True), False)
        check('other catalog guard independent', guard(c, server2, True))
        query(a, 'COMMIT')
        check('remaining reader still protects', guard(c, server1, True), False)
        query(b, 'ROLLBACK')
        check('GC acquires after readers finish', guard(c, server1, True))
        query(a, 'BEGIN')
        check('new reader try is blocked by GC', guard(a, server1), False)
        query(c, 'COMMIT')
        check('reader resumes after GC commit', guard(a, server1))
        query(a, 'ROLLBACK')

        query(a, 'BEGIN')
        check('parent guard', guard(a, server1))
        query(a, 'SAVEPOINT child')
        check('child reuses parent guard', guard(a, server1))
        check('child new guard', guard(a, server2))
        query(a, 'ROLLBACK TO child')
        query(b, 'BEGIN')
        check('parent guard survives child rollback', guard(b, server1, True), False)
        check('child guard released by child rollback', guard(b, server2, True))
        query(b, 'ROLLBACK')
        query(a, 'SAVEPOINT release_parent')
        reject('child cannot explicitly release parent guard', a,
               'SELECT og_lake_internal.test_catalog_guard_release(%s::oid)', '55000', (server1,))
        query(a, 'ROLLBACK TO release_parent')
        query(a, 'SAVEPOINT upgrade')
        reject('shared to exclusive upgrade rejected', a,
               'SELECT og_lake_internal.test_catalog_guard_acquire(%s::oid,true)', '0A000', (server1,))
        query(a, 'ROLLBACK TO upgrade')
        query(a, 'COMMIT')
        query(b, 'BEGIN')
        check('top commit releases parent guard', guard(b, server1, True))
        query(b, 'ROLLBACK')
        query(a, 'BEGIN'); query(a, 'SAVEPOINT child_commit')
        check('child obtains guard before subcommit', guard(a, server1))
        query(a, 'RELEASE child_commit')
        query(b, 'BEGIN')
        check('subcommit retains guard in parent', guard(b, server1, True), False)
        query(a, 'ROLLBACK')
        check('parent abort releases adopted guard', guard(b, server1, True))
        query(b, 'ROLLBACK')
        check('no catalog guards leaked', state(a)['catalog_guards'], 0)
        check('callbacks registered once', state(a)['callbacks_registered'])
        query(a, 'BEGIN')
        check('prepared transaction starts with shared protection', guard(a, server1))
        reject('two phase prepare is explicitly unavailable', a,
               "PREPARE TRANSACTION 'native_lake_host_not_supported'", '0A000')
        query(a, 'ROLLBACK')
        query(b, 'BEGIN')
        check('failed prepare leaves no catalog guard', guard(b, server1, True))
        query(b, 'ROLLBACK')
        reject('unknown static symbol is rejected', a,
               "CREATE FUNCTION public.bad_lake_symbol() RETURNS void "
               "AS '$libdir/og_lake', 'unknown_lake_symbol' LANGUAGE C NOT FENCED", '42883')

        role = 'lake_host_role_' + suffix
        role_password = 'Og1!' + secrets.token_hex(12)
        query(admin, sql.SQL('CREATE ROLE {} LOGIN PASSWORD {}').format(
            sql.Identifier(role), sql.Literal(role_password)))
        if runtime_version in ('0.4', '0.5', '0.6', '0.7', '0.8', '0.9', '0.10', '0.11'):
            check('ordinary role has no locator function execution grant',
                  scalar(a, "SELECT has_function_privilege(%s,'og_lake_internal.row_locator(anyelement)','EXECUTE')", (role,)), False)
        query(c, sql.SQL('SET ROLE {} PASSWORD {}').format(
            sql.Identifier(role), sql.Literal(role_password)))
        try:
            check('non-superuser role is active', scalar(c, 'SELECT current_user'), role)
            reject('non-superuser cannot claim maintenance', c,
                   'SELECT og_lake_internal.test_maintenance_claim()', '42501')
        finally:
            query(c, 'RESET ROLE')
        reject('lake table creation without binding options fails explicitly', a,
               'CREATE FOREIGN TABLE host_unsupported(i integer) SERVER lake_guard_one', 'HV00J')
        reject('unsupported credential-backed catalog creation fails explicitly', a,
               "SELECT og_lake.create_catalog('unsupported_catalog','unsupported_schema','unused','1.1')", '0A000')
        reject('unknown server option fails', a,
               "CREATE SERVER bad_host_option FOREIGN DATA WRAPPER og_lake_fdw OPTIONS (unknown_option '1')", 'HV00D')
        query(a, 'DROP EXTENSION og_lake CASCADE')
        check('extension uninstalled', scalar(a, "SELECT count(*) FROM pg_extension WHERE extname='og_lake'"), 0)
        check('ordinary query after uninstall', scalar(a, 'SELECT 21*2'), 42)
        query(a, "CREATE EXTENSION og_lake VERSION '0.1'")
        check('original extension version can be installed',
              scalar(a, "SELECT extversion FROM pg_extension WHERE extname='og_lake'"), '0.1')
        query(a, 'BEGIN')
        query(a, "ALTER EXTENSION og_lake UPDATE TO '0.2'")
        query(a, 'ROLLBACK')
        check('extension upgrade is transactional',
              scalar(a, "SELECT extversion FROM pg_extension WHERE extname='og_lake'"), '0.1')
        query(a, "ALTER EXTENSION og_lake UPDATE TO '0.2'")
        check('extension upgrade completes',
              scalar(a, "SELECT extversion FROM pg_extension WHERE extname='og_lake'"), '0.2')
        if runtime_version in ('0.3', '0.4', '0.5', '0.6', '0.7', '0.8', '0.9', '0.10', '0.11'):
            checks_sql = ("SELECT count(*) FROM pg_constraint c JOIN pg_namespace n ON c.connamespace=n.oid "
                          "WHERE n.nspname='og_lake_internal' AND c.conname IN "
                          "('catalog_binding_read_only_check','table_binding_read_only_check')")
            check('0.2 retains both read-only binding constraints', scalar(a, checks_sql), 2)
            query(a, 'BEGIN')
            query(a, "ALTER EXTENSION og_lake UPDATE TO '0.3'")
            check('upgrade removes read-only constraints within the transaction', scalar(a, checks_sql), 0)
            query(a, 'ROLLBACK')
            check('0.3 upgrade rollback restores extension version',
                  scalar(a, "SELECT extversion FROM pg_extension WHERE extname='og_lake'"), '0.2')
            check('0.3 upgrade rollback restores both constraints', scalar(a, checks_sql), 2)
            check('0.3 upgrade rollback removes callback probe SQL entry',
                  scalar(a, "SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON p.pronamespace=n.oid "
                            "WHERE n.nspname='og_lake_internal' AND p.proname='test_precommit_insert'"), 0)
            query(a, "ALTER EXTENSION og_lake UPDATE TO '0.3'")
            check('0.3 upgrade completes',
                  scalar(a, "SELECT extversion FROM pg_extension WHERE extname='og_lake'"), '0.3')
            check('0.3 upgrade removes both read-only binding constraints', scalar(a, checks_sql), 0)
            check('0.3 upgrade installs callback probe SQL entry',
                  scalar(a, "SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON p.pronamespace=n.oid "
                            "WHERE n.nspname='og_lake_internal' AND p.proname='test_precommit_insert'"), 1)
        if runtime_version in ('0.4', '0.5', '0.6', '0.7', '0.8', '0.9', '0.10', '0.11'):
            check('installed 0.3 has no 0.4 row locator marker', scalar(a, marker_count_sql), 0)
            old_comment = scalar(a, "SELECT obj_description('og_lake.host_status()'::regprocedure,'pg_proc')")
            query(a, 'BEGIN')
            query(a, "ALTER EXTENSION og_lake UPDATE TO '0.4'")
            check('0.4 extension version visible within upgrading transaction',
                  scalar(a, "SELECT extversion FROM pg_extension WHERE extname='og_lake'"), '0.4')
            check_marker(a, 'transactional 0.4 upgrade')
            query(a, 'ROLLBACK')
            check('0.4 upgrade rollback restores installed 0.3 version',
                  scalar(a, "SELECT extversion FROM pg_extension WHERE extname='og_lake'"), '0.3')
            check('0.4 upgrade rollback removes marker SQL entry', scalar(a, marker_count_sql), 0)
            check('0.4 upgrade rollback restores previous host function comment',
                  scalar(a, "SELECT obj_description('og_lake.host_status()'::regprocedure,'pg_proc')"), old_comment)
            query(a, "ALTER EXTENSION og_lake UPDATE TO '0.4'")
            check('0.4 upgrade commits installed version',
                  scalar(a, "SELECT extversion FROM pg_extension WHERE extname='og_lake'"), '0.4')
            check_marker(a, 'committed 0.4 upgrade')
            check('upgraded marker remains unavailable to the ordinary role',
                  scalar(a, "SELECT has_function_privilege(%s,'og_lake_internal.row_locator(anyelement)','EXECUTE')", (role,)), False)
            reject('upgraded marker still rejects direct execution', a,
                   'SELECT og_lake_internal.row_locator(1)', '0A000')
        check('reinstall uses clean guard state', state(a)['catalog_guards'], 0)
        check('reinstalled maintenance claim works', claim(a))
        release(a)
        result['status'] = 'pass'
    except ImportError as exc:
        result.update(status='blocked', error=f'required real SQL client missing: {exc}')
    except Exception as exc:
        result['error'] = f'{type(exc).__name__}: {exc}'
        if not result['assertions'] or result['assertions'][-1]['status'] != 'fail':
            result['assertions'].append({'name': 'execution completed', 'status': 'fail', 'details': str(exc)})
    finally:
        for conn in connections:
            if conn is not admin:
                conn.close()
        if admin is not None:
            try:
                for db in reversed(databases):
                    query(admin, sql.SQL('DROP DATABASE {}').format(sql.Identifier(db)))
                if role:
                    query(admin, sql.SQL('DROP ROLE {}').format(sql.Identifier(role)))
            except Exception as exc:
                result['cleanup_error'] = str(exc)
                result['status'] = 'fail'
            admin.close()
        output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n')
        print(json.dumps({'status': result['status'], 'assertions': len(result['assertions']), 'result': str(output)}))
    return {'pass': 0, 'fail': 1, 'blocked': 2}[result['status']]


if __name__ == '__main__':
    sys.exit(main())
