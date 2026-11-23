#!/usr/bin/env python3
"""Transactional native persistent options, using real owned SQL/Parquet inputs."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import shutil
import subprocess
import sys
import time
import traceback
from types import SimpleNamespace

ROOT = Path('/home/qiumc/data/code/cpp')
D = ROOT / 'integration/opengauss-native-lake'
sys.path[:0] = [str(D), str(D / 'tests')]
from file_dml_acceptance import check_build
from inline_fixture import query, scalar
from lake_harness import sha256, validate_run_id, write_json
from native_auto_flush import connect_control
from native_test_context import OwnedDatabase
from native_table_create import Case
from permission_api import OwnedRoles, cleanup_roles
from empty_catalog_oracle import read_native_catalog, transport_native
from reverse_reference import Reference

MODES = ['transactions', 'concurrent', 'publication', 'permissions', 'upgrade']


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--run-id', required=True)
    p.add_argument('--build-receipt', type=Path, required=True)
    p.add_argument('--mode', choices=MODES, required=True)
    a = p.parse_args()
    validate_run_id(a.run_id)
    out = ROOT / '公共文档/openGauss原生湖存储/验证' / a.run_id
    out.mkdir(exist_ok=False)
    r = {'status': 'fail', 'scope': 'Native SQL0.12 persistent option ' + a.mode,
         'assertions': [], 'limitations': ['Positive inline limit only; native RC functional correctness, not complete DDL/configuration or commercial qualification.']}
    inputs = [Path(__file__), D / 'tests/native_table_create.py', D / 'tests/permission_api.py',
              D / 'native_test_context.py', D / 'native_runtime.py', D / 'tests/inline_cleanup.py',
              D / 'tests/file_dml_acceptance.py', D / 'tests/empty_catalog_oracle.py',
              D / 'tests/reverse_reference.py', D / 'source-lock.json']
    r['inputs'] = {str(f): sha256(f) for f in inputs}
    archive = out / 'executed-inputs'
    archive.mkdir()
    for f in inputs:
        shutil.copy2(f, archive / f.name)
    db = roles = control = t = pool = future = None

    def check(name, got, want=True):
        ok = got == want
        r['assertions'].append({'name': name, 'observed': got, 'expected': want, 'status': 'pass' if ok else 'fail'})
        if not ok:
            raise AssertionError(name)

    def error(conn, statement, args, code, label):
        import psycopg2
        try:
            query(conn, statement, args)
        except psycopg2.Error as exc:
            check(label, exc.pgcode, code)
            query(conn, 'ROLLBACK')
        else:
            raise AssertionError(label + ': unexpectedly succeeded')

    def options(conn=None):
        return query(conn or t.observer, "SELECT key,value,scope,scope_id FROM created_metadata.ducklake_metadata "
                     "WHERE key='data_inlining_row_limit' ORDER BY scope NULLS FIRST,scope_id")

    def setting(conn, value, catalog=False, key='data_inlining_row_limit'):
        statement = 'SELECT og_lake.set_option(%s::name,%s,%s)' if catalog else 'SELECT og_lake.set_option(%s::regclass,%s,%s)'
        return query(conn, statement, (t.server if catalog else 'biz.orders', key, value))

    try:
        b = check_build(a.build_receipt)
        r.update(source_head=b['source_head'], binary_sha256=b['binary_sha256'])
        control = connect_control()
        check('automatic maintenance stays disabled', scalar(control, 'SHOW enable_lake_maintenance'), 'off')
        with (out / 'fixture.log').open('w') as log:
            subprocess.run([sys.executable, str(D / 'tests/catalog_fixture_bundle.py'), '--run-id', a.run_id,
                            '--output', str(out / 'fixture/manifest.json')], stdout=log,
                           stderr=subprocess.STDOUT, check=True, timeout=300)
        db = OwnedDatabase(a.run_id, out).create()
        roles = OwnedRoles(control, a.run_id, out)
        t = Case(db, SimpleNamespace(mode='net_dml', sql_version='0.11' if a.mode == 'upgrade' else '0.12',
                                    manifest=out / 'fixture/manifest.json', output=out / 'result.json', run_id=a.run_id), r, roles)
        t.create()
        query(t.a, 'INSERT INTO biz.orders VALUES(1,10),(2,20),(3,30)')
        original = [(1, 10), (2, 20), (3, 30)]
        initial = options()
        before_head = t.head()
        if a.mode == 'upgrade':
            check('legacy SQL has no configuration API', scalar(t.a, "SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='og_lake' AND p.proname='set_option'"), 0)
            query(t.a, "ALTER EXTENSION og_lake UPDATE TO '0.12'")
            check('both overloads installed by upgrade', scalar(t.a, "SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='og_lake' AND p.proname='set_option'"), 2)
            check('upgrade preserves standard catalog snapshot', t.head(), before_head)
            setting(t.a, '25')
            check('upgraded API writes one standard override', options()[-1], ('data_inlining_row_limit', '25', 'table', 1))
            check('upgraded binding reads original business values', t.rows(), original)
            query(t.a, "SELECT og_lake.flush('biz.orders'::regclass)")
            query(t.a, 'UPDATE biz.orders SET val=11 WHERE id=1')
            check('upgraded file DML works', t.rows(), [(1, 11), (2, 20), (3, 30)])
        elif a.mode == 'transactions':
            query(t.a, 'BEGIN')
            setting(t.a, '00100', True, 'DATA_INLINING_ROW_LIMIT')
            query(t.a, 'INSERT INTO biz.heap_marker VALUES(77)')
            check('own transaction reads canonical global option', options(t.a)[0], ('data_inlining_row_limit', '100', None, None))
            check('other session cannot see uncommitted options', options(), initial)
            check('lake reader does not wait for option writer', t.rows(conn=t.b), original)
            query(t.a, 'ROLLBACK')
            check('rollback restores all option rows', options(), initial)
            check('same transaction heap write rolled back', query(t.observer, 'SELECT * FROM biz.heap_marker'), [])
            query(t.a, 'BEGIN')
            setting(t.a, '100', True)
            query(t.a, 'INSERT INTO biz.heap_marker VALUES(78)')
            query(t.a, 'COMMIT')
            check('heap marker commits with option', query(t.observer, 'SELECT * FROM biz.heap_marker'), [(78,)])
            query(t.a, 'BEGIN')
            query(t.a, 'SAVEPOINT child')
            setting(t.a, '20')
            query(t.a, 'ROLLBACK TO child')
            query(t.a, 'RELEASE SAVEPOINT child')
            check('savepoint restores table option absence', options(t.a), [('data_inlining_row_limit', '100', None, None)])
            query(t.a, 'INSERT INTO biz.orders VALUES(4,40)')
            query(t.a, 'COMMIT')
            check('rolled-back option DDL journal permits parent lake DML', t.rows(), original + [(4, 40)])
            before_head = t.head()
            query(t.a, 'PREPARE configured(text) AS SELECT og_lake.set_option(\'biz.orders\'::regclass,\'data_inlining_row_limit\',$1)')
            query(t.a, "EXECUTE configured('20')")
            query(t.a, "EXECUTE configured('21')")
            check('prepared API replaces same standard row once', options(), [('data_inlining_row_limit', '100', None, None), ('data_inlining_row_limit', '21', 'table', 1)])
            for value, code in [('', '22023'), ('-1', '22023'), ('1.5', '22023'), ('1; DROP TABLE biz.orders', '22023'), ('0', '0A000'), ('9223372036854775808', '22003'), (None, '22004')]:
                error(t.a, "SELECT og_lake.set_option('biz.orders'::regclass,'data_inlining_row_limit',%s)", (value,), code, 'invalid option rejected: ' + repr(value))
            error(t.a, "SELECT og_lake.set_option('biz.orders'::regclass,%s,'1')", ('unknown_option',), '22023', 'unknown option key rejected')
            error(t.a, "SELECT og_lake.set_option('biz.heap_marker'::regclass,'data_inlining_row_limit','1')", None, '42809', 'ordinary heap cannot become config target')
            query(t.a, 'BEGIN READ ONLY')
            error(t.a, "SELECT og_lake.set_option('biz.orders'::regclass,'data_inlining_row_limit','1')", None, '25006', 'read-only transaction rejected')
            query(t.a, 'BEGIN')
            t.create('local_only')
            error(t.a, "SELECT og_lake.set_option('biz.local_only'::regclass,'data_inlining_row_limit','1')", None, '0A000', 'transaction-local table config rejected atomically')
            check('rejected local config rolls back local CREATE', scalar(t.a, "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='biz' AND c.relname='local_only'"), 0)
            check('configuration and all rejected writes created no lake snapshot', t.head(), before_head)
        elif a.mode == 'concurrent':
            # Deliberately create the otherwise-absent override under an ordinary
            # uncommitted metadata RowExclusive writer. Native API must wait,
            # then see it and UPDATE rather than INSERT a duplicate.
            query(t.b, 'BEGIN')
            query(t.b, "INSERT INTO created_metadata.ducklake_metadata VALUES('data_inlining_row_limit','30','table',1)")
            pool = ThreadPoolExecutor(max_workers=1)
            pid = scalar(t.a, 'SELECT pg_backend_pid()')
            future = pool.submit(setting, t.a, '31')
            deadline = time.monotonic() + 10
            waits = []
            while time.monotonic() < deadline:
                waits = query(t.observer, "SELECT pid FROM pg_locks WHERE pid=%s AND relation='created_metadata.ducklake_metadata'::regclass AND mode='ShareRowExclusiveLock' AND NOT granted", (pid,))
                if waits or future.done():
                    break
                time.sleep(.01)
            check('native API genuinely waits for uncommitted standard metadata writer', len(waits), 1)
            check('ordinary DQL still runs during config lock wait', t.rows(conn=t.observer), original)
            query(t.b, 'COMMIT')
            future.result(timeout=15)
            pool.shutdown(wait=True)
            pool = None
            check('post-wait fresh snapshot avoids duplicate override', options()[-1:], [('data_inlining_row_limit', '31', 'table', 1)])
            check('configuration-only writes do not allocate snapshots or IDs', t.head(), before_head)
        elif a.mode == 'publication':
            query(t.a, "SELECT og_lake.flush('biz.orders'::regclass)")
            file_head = t.head()
            query(t.a, 'PREPARE change AS UPDATE biz.orders SET val=val+1 WHERE id<=2')
            query(t.a, 'BEGIN')
            query(t.a, 'EXECUTE change')
            query(t.a, 'INSERT INTO biz.heap_marker VALUES(77)')
            setting(t.b, '1')
            error(t.a, 'COMMIT', None, '0A000', 'staged file DML revalidates changed threshold at commit')
            check('rejected file DML does not change business data', t.rows(), original)
            check('rejected commit restores same-transaction heap data', query(t.observer, 'SELECT * FROM biz.heap_marker'), [])
            check('configuration and rejected DML do not allocate lake snapshot', t.head(), file_head)
            setting(t.b, '3')
            setting(t.b, '1', True)
            query(t.a, 'EXECUTE change')
            check('cached DML plan uses table override above lower catalog threshold', t.rows(), [(1, 11), (2, 21), (3, 30)])
            committed_head = t.head()
            query(t.a, 'BEGIN')
            setting(t.a, '5')
            error(t.a, 'INSERT INTO biz.orders VALUES(4,40)', None, '0A000', 'configuration then lake write rejected')
            check('rejected mixed transaction rolls back options', options()[-1][1], '3')
            query(t.a, 'BEGIN')
            query(t.a, 'UPDATE biz.orders SET val=99 WHERE id=1')
            error(t.a, "SELECT og_lake.set_option('biz.orders'::regclass,'data_inlining_row_limit','5')", None, '0A000', 'lake write then configuration rejected')
            query(t.a, 'BEGIN')
            setting(t.a, '5')
            error(t.a, "SELECT og_lake.flush('biz.orders'::regclass)", None, '0A000', 'configuration then flush rejected')
            query(t.a, 'BEGIN')
            query(t.a, "SELECT og_lake.flush('biz.orders'::regclass)")
            error(t.a, "SELECT og_lake.set_option('biz.orders'::regclass,'data_inlining_row_limit','5')", None, '0A000', 'flush then configuration rejected')
            check('all mixed failures preserve snapshot frontiers', t.head(), committed_head)
            check('all mixed failures preserve full values', t.rows(), [(1, 11), (2, 21), (3, 30)])
        elif a.mode == 'permissions':
            from psycopg2 import sql
            owner = roles.create('business')
            reader = roles.create('reader')
            query(t.a, sql.SQL('GRANT USAGE,CREATE ON SCHEMA biz TO {}').format(sql.Identifier(owner)))
            query(t.a, sql.SQL('GRANT USAGE ON FOREIGN SERVER created_lake TO {}').format(sql.Identifier(owner)))
            query(t.a, sql.SQL('GRANT USAGE ON SCHEMA biz TO {}').format(sql.Identifier(reader)))
            ordinary, identity = roles.connect(db, 'business')
            unprivileged, read_identity = roles.connect(db, 'reader')
            r['ordinary_identities'] = [identity, read_identity]
            t.create('owned', conn=ordinary)
            query(t.a, sql.SQL('GRANT SELECT ON biz.owned TO {}').format(sql.Identifier(reader)))
            query(ordinary, "SELECT og_lake.set_option('biz.owned'::regclass,'data_inlining_row_limit','50')")
            check('ordinary owner can configure its own table', query(t.observer, "SELECT value FROM created_metadata.ducklake_metadata WHERE scope='table' AND scope_id=2"), [('50',)])
            error(unprivileged, "SELECT og_lake.set_option('biz.owned'::regclass,'data_inlining_row_limit','60')", None, '42501', 'SELECT grantee cannot set table policy')
            error(ordinary, "SELECT og_lake.set_option('biz.orders'::regclass,'data_inlining_row_limit','60')", None, '42501', 'owner of another table cannot set target policy')
            error(ordinary, "SELECT og_lake.set_option('created_lake'::name,'data_inlining_row_limit','60')", None, '42501', 'table owner cannot change catalog policy')
            error(ordinary, 'SELECT * FROM created_metadata.ducklake_metadata', None, '42501', 'public API does not grant direct private metadata access')
            check('permission failures retain original override', query(t.observer, "SELECT value FROM created_metadata.ducklake_metadata WHERE scope='table' AND scope_id=2"), [('50',)])
            check('permission failures restore caller identity', scalar(ordinary, 'SELECT current_user'), owner)
        # Native-created standard state is interpreted by the pinned independent
        # DuckLake, without rewriting values, IDs, scopes or snapshots.
        names = query(t.observer, "SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=%s AND c.relkind='r' ORDER BY c.relname", (t.schema,))
        native = read_native_catalog(t.observer, t.schema, {'tables': [{'table_name': n} for n, in names]}, out / 'native-export.json')
        lock = json.loads((D / 'source-lock.json').read_text())
        receipt = json.loads((ROOT / '.reference/opengauss-native-lake/build/receipt.json').read_text())
        check('independent reference uses current source lock', receipt['source_lock_hash'], sha256(D / 'source-lock.json'))
        ref = Reference(out, lock, receipt)
        transport = out / 'transport.duckdb'
        transport_native(ref, transport, native)
        observed = ref.run(ref.attach(transport, True) + 'SELECT id,val FROM lake.main.orders ORDER BY id', 'current', result=True)
        check('pinned independent DuckLake reads complete native business values', [tuple(row.values()) for row in observed], t.rows())
        standard = ref.run("SELECT key,value,scope,scope_id FROM main.ducklake_metadata WHERE key='data_inlining_row_limit' ORDER BY scope NULLS FIRST,scope_id", 'standard-options', transport, True, True)
        check('independent metadata retains exact option scopes and values', [tuple(row.values()) for row in standard], options())
        check('executed input bytes unchanged', all(sha256(f) == h for f, h in r['inputs'].items()))
        check_build(a.build_receipt)
        r['status'] = 'pass'
    except BaseException as exc:
        r.update(reason=str(exc), traceback=traceback.format_exc())
    finally:
        if future is not None and not future.done() and t is not None:
            try:
                scalar(t.observer, 'SELECT pg_cancel_backend(%s)', (pid,))
                query(t.b, 'ROLLBACK')
            except BaseException:
                pass
        if pool is not None:
            pool.shutdown(wait=True)
        if db is not None:
            try:
                db.cleanup()
                r['database_cleanup'] = db.cleanup_result
            except BaseException as exc:
                r.update(status='fail', cleanup_error=str(exc))
        if roles is not None:
            try:
                if not r.get('database_cleanup') or r['database_cleanup']['returncode']:
                    raise RuntimeError('Owned database cleanup unproven; role retained')
                r['role_cleanup'] = cleanup_roles(control, roles.path, a.run_id)
                r = roles.redact(r)
            except BaseException as exc:
                r.update(status='fail', role_cleanup_error=str(exc))
        if control is not None:
            control.close()
        write_json(out / 'result.json', r)
    print(json.dumps({'status': r['status'], 'mode': a.mode, 'checks': len(r['assertions']), 'reason': r.get('reason'), 'result': str(out / 'result.json')}, ensure_ascii=False))
    return 0 if r['status'] == 'pass' else 1


if __name__ == '__main__':
    raise SystemExit(main())
