#!/usr/bin/env python3
"""RC-only admission and real reader protection across deletion-file replacement."""
import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import traceback
from types import SimpleNamespace

ROOT = Path('/home/qiumc/data/code/cpp')
DRIVER = ROOT / 'integration/opengauss-native-lake'
sys.path[:0] = [str(DRIVER), str(DRIVER / 'tests')]
from file_dml_acceptance import check_build
from inline_fixture import query, scalar
from lake_harness import sha256, validate_run_id, write_json
from native_table_create import Case
from native_test_context import OwnedDatabase
from permission_api import OwnedRoles, cleanup_roles


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--build-receipt', type=Path, required=True)
    args = parser.parse_args()
    validate_run_id(args.run_id)
    out = ROOT / '公共文档/openGauss原生湖存储/验证' / args.run_id
    out.mkdir(exist_ok=False)
    result = {'status': 'fail', 'scope': 'RC-only access admission and live RC cursor/catalog protection',
              'assertions': [], 'limitations': ['No physical GC, watermark, remote or target-RAC qualification.']}
    paths = [Path(__file__), DRIVER / 'native_test_context.py', DRIVER / 'tests/native_table_create.py',
             DRIVER / 'tests/permission_api.py', DRIVER / 'inline_fixture.py']
    result['inputs'] = {str(p): sha256(p) for p in paths}
    archive = out / 'executed-inputs'
    archive.mkdir()
    for p in paths:
        shutil.copy2(p, archive / p.name)
    db = roles = control = None

    def check(name, observed, expected=True):
        passed = observed == expected
        result['assertions'].append({'name': name, 'observed': observed, 'expected': expected,
                                     'status': 'pass' if passed else 'fail'})
        if not passed:
            raise AssertionError(name)

    try:
        import psycopg2
        build = check_build(args.build_receipt)
        result.update(source_head=build['source_head'], binary_sha256=build['binary_sha256'],
                      build=str(args.build_receipt), build_sha256=sha256(args.build_receipt))
        with (out / 'fixture.log').open('w') as log:
            subprocess.run([sys.executable, str(DRIVER / 'tests/catalog_fixture_bundle.py'),
                            '--run-id', args.run_id, '--output', str(out / 'fixture/manifest.json')],
                           stdout=log, stderr=subprocess.STDOUT, check=True, timeout=300)
        control = psycopg2.connect(host=str(ROOT / '.runtime/opengauss-native-lake/debug/socket'),
                                  port=55560, dbname='postgres', user=os.getenv('USER', 'qiumc'))
        control.autocommit = True
        db = OwnedDatabase(args.run_id, out).create()
        roles = OwnedRoles(control, args.run_id, out)
        test = Case(db, SimpleNamespace(mode='net_dml', sql_version='0.11', manifest=out / 'fixture/manifest.json',
                                       output=out / 'result.json', run_id=args.run_id), result, roles)
        test.create()
        check('host starts in READ COMMITTED', scalar(test.a, 'SHOW transaction_isolation'), 'read committed')
        query(test.a, 'INSERT INTO biz.orders VALUES(1,10),(2,20),(3,30)')
        flush = "SELECT og_lake.flush('biz.orders'::regclass)"
        query(test.a, flush)
        query(test.a, 'PREPARE cached_lake AS SELECT id,val FROM biz.orders ORDER BY id')
        check('cached plan first executes under RC', query(test.a, 'EXECUTE cached_lake'), [(1,10),(2,20),(3,30)])
        protocol = test.protocol()
        root = Path(test.empty_manifest['data_path'])
        files = {str(p): sha256(p) for p in root.rglob('*.parquet')}
        for level in ('REPEATABLE READ', 'SERIALIZABLE', 'READ UNCOMMITTED'):
            query(test.a, 'BEGIN ISOLATION LEVEL ' + level)
            check(level + ' still permits ordinary-table reads', scalar(test.a, 'SELECT count(*) FROM biz.heap_marker'), 0)
            statements = ['SELECT * FROM biz.orders', 'EXECUTE cached_lake',
                          'EXPLAIN SELECT * FROM biz.orders', 'INSERT INTO biz.orders VALUES(4,40)',
                          'UPDATE biz.orders SET val=11 WHERE id=1', 'DELETE FROM biz.orders WHERE id=1',
                          "CREATE FOREIGN TABLE biz.denied(id integer,val integer) SERVER created_lake "
                          "OPTIONS(binding_mode 'create',lake_schema 'main')", flush]
            for statement in statements:
                query(test.a, 'SAVEPOINT denied')
                try:
                    query(test.a, statement)
                except psycopg2.Error as exc:
                    check(level + ' rejects ' + statement[:60], exc.pgcode, '0A000')
                    check('rejection identifies RC requirement', 'READ COMMITTED' in str(exc))
                else:
                    raise AssertionError('Unsupported isolation accepted: ' + level + '/' + statement)
                query(test.a, 'ROLLBACK TO denied')
                query(test.a, 'RELEASE denied')
            query(test.a, 'ROLLBACK')
            check('failed ' + level + ' has no standard catalog effects', test.protocol(), protocol)
            check('failed ' + level + ' has no candidate file effects',
                  {str(p): sha256(p) for p in root.rglob('*.parquet')}, files)
            check('RC works after unsupported transaction rollback', test.rows(), [(1,10),(2,20),(3,30)])
        query(test.a, 'DEALLOCATE cached_lake')
        query(test.a, 'BEGIN ISOLATION LEVEL READ COMMITTED READ ONLY')
        check('RC read-only transaction can read lake files', test.rows(), [(1,10),(2,20),(3,30)])
        query(test.a, 'COMMIT')
        query(test.a, 'DELETE FROM biz.orders WHERE id=2')
        query(test.a, flush)
        path = scalar(test.a, 'SELECT path FROM created_metadata.ducklake_delete_file')
        old_files = list(root.rglob(path))
        check('original deletion object is real', len(old_files), 1)
        old_hash = sha256(old_files[0])
        server = scalar(test.a, "SELECT oid FROM pg_foreign_server WHERE srvname='created_lake'")
        query(test.b, 'BEGIN ISOLATION LEVEL READ COMMITTED')
        query(test.b, 'DECLARE retained NO SCROLL CURSOR FOR SELECT id,val FROM biz.orders ORDER BY id')
        check('live RC cursor starts at original lake state', query(test.b, 'FETCH 1 FROM retained'), [(1,10)])
        query(test.a, 'DELETE FROM biz.orders WHERE id=1')
        query(test.a, flush)
        check('retirement registered by genuine file consolidation',
              scalar(test.a, 'SELECT count(*) FROM created_metadata.ducklake_files_scheduled_for_deletion'), 1)
        check('new RC transaction sees current lake state', test.rows(), [(3,30)])

        def exclusive_available():
            query(test.observer, 'BEGIN')
            acquired = scalar(test.observer, 'SELECT og_lake_internal.test_catalog_guard_acquire(%s::oid,true)', (server,))
            query(test.observer, 'COMMIT')
            return acquired

        check('live RC reader blocks GC-exclusive admission', exclusive_available(), False)
        check('cursor completes its original valid result', query(test.b, 'FETCH ALL FROM retained'), [(3,30)])
        query(test.b, 'CLOSE retained')
        check('RC transaction still retains original lake context', test.rows(test.b), [(1,10),(3,30)])
        check('closing cursor cannot release transaction-owned protection', exclusive_available(), False)
        check('retired physical object remains unchanged', sha256(old_files[0]), old_hash)
        query(test.b, 'COMMIT')
        check('GC-exclusive becomes available only after reader ends', exclusive_available())
        check('new RC context sees current state after commit', test.rows(test.b), [(3,30)])
        query(test.a, 'BEGIN')
        query(test.a, 'INSERT INTO biz.heap_marker VALUES(1)')
        query(test.a, 'UPDATE biz.orders SET val=31 WHERE id=3')
        query(test.a, 'ROLLBACK')
        check('mixed RC heap rollback is atomic', scalar(test.a, 'SELECT count(*) FROM biz.heap_marker'), 0)
        check('mixed RC lake rollback is atomic', test.rows(), [(3,30)])
        check('test inputs unchanged', all(sha256(p) == h for p,h in result['inputs'].items()))
        check_build(args.build_receipt)
        result['status'] = 'pass'
    except BaseException as exc:
        result.update(reason=str(exc), traceback=traceback.format_exc())
    finally:
        if db is not None:
            try:
                db.cleanup()
                result['database_cleanup'] = db.cleanup_result
            except BaseException as exc:
                result.update(status='fail', cleanup_error=str(exc))
        if roles is not None:
            try:
                if not result.get('database_cleanup') or result['database_cleanup']['returncode']:
                    raise RuntimeError('Owned database cleanup unproven; roles retained')
                result['role_cleanup'] = cleanup_roles(control, roles.path, args.run_id)
                result = roles.redact(result)
            except BaseException as exc:
                result.update(status='fail', role_cleanup_error=str(exc))
        if control is not None:
            control.close()
        write_json(out / 'result.json', result)
    print(json.dumps({'status':result['status'], 'checks':len(result['assertions']),
                      'reason':result.get('reason'), 'result':str(out / 'result.json')}))
    return 0 if result['status'] == 'pass' else 1


if __name__ == '__main__':
    raise SystemExit(main())
