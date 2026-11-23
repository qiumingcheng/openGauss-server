#!/usr/bin/env python3
"""Actual typed multi-statement publication, savepoints and late cancellation."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
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
from empty_catalog_oracle import read_native_catalog, transport_native
from reverse_reference import Reference


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--build-receipt', type=Path, required=True)
    parser.add_argument('--mode', choices=['savepoints', 'cancel'], required=True)
    args = parser.parse_args()
    validate_run_id(args.run_id)
    out = ROOT / '公共文档/openGauss原生湖存储/验证' / args.run_id
    out.mkdir(exist_ok=False)
    result = {'status': 'fail', 'scope': 'Actual four-column 1537-row publication: ' + args.mode,
              'assertions': [], 'limitations': ['Local Debug correctness only; no throughput or remote/RAC qualification.']}
    inputs = [Path(__file__), DRIVER / 'native_test_context.py', DRIVER / 'tests/native_table_create.py',
              DRIVER / 'tests/permission_api.py', DRIVER / 'tests/empty_catalog_oracle.py',
              DRIVER / 'tests/reverse_reference.py', DRIVER / 'inline_fixture.py']
    result['input_hashes'] = {str(p): sha256(p) for p in inputs}
    archive = out / 'executed-inputs'
    archive.mkdir()
    for p in inputs:
        shutil.copy2(p, archive / p.name)
    db = roles = control = holder = executor = future = test = None
    publisher = None

    def check(name, observed, expected=True):
        ok = observed == expected
        result['assertions'].append({'name': name, 'observed': observed,
                                     'expected': expected, 'status': 'pass' if ok else 'fail'})
        if not ok:
            raise AssertionError(name)

    try:
        import psycopg2
        from psycopg2 import sql
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
        query(test.a, "CREATE FOREIGN TABLE biz.orders(id integer,val integer,b boolean,note text) "
              "SERVER created_lake OPTIONS(binding_mode 'create',lake_schema 'main')")
        query(test.a, "INSERT INTO biz.orders VALUES(1,10,true,'seed')")
        original_head = test.head()
        original_protocol = test.protocol()
        carrier = scalar(test.a, 'SELECT table_name FROM created_metadata.ducklake_inlined_data_tables')
        bulk = "INSERT INTO biz.orders SELECT s,CASE WHEN s%7=0 THEN NULL ELSE s*10 END," \
               "CASE WHEN s%5=0 THEN NULL ELSE s%2=0 END,CASE WHEN s%11=0 THEN NULL " \
               "ELSE 'λ湖-'||s::text END FROM generate_series(2,1538) s"
        expected = [(1, 10, True, 'seed')] + [
            (s, None if s % 7 == 0 else s * 10, None if s % 5 == 0 else s % 2 == 0,
             None if s % 11 == 0 else 'λ湖-' + str(s)) for s in range(2, 1539)]
        query(test.a, 'BEGIN')
        if args.mode == 'savepoints':
            query(test.a, 'SAVEPOINT discarded')
            query(test.a, 'INSERT INTO biz.orders SELECT s,s,true,\'discarded\' FROM generate_series(3000,4199) s')
            query(test.a, 'ROLLBACK TO discarded')
            query(test.a, 'RELEASE discarded')
        query(test.a, 'INSERT INTO biz.heap_marker VALUES(1)')
        returned = query(test.a, bulk + ' RETURNING id,val,b,note')
        check('all staged RETURNING values before publication', returned, expected[1:])
        check('own read sees every staged typed value', query(test.a, 'SELECT id,val,b,note FROM biz.orders ORDER BY id'), expected)
        check('other connection sees only original committed row',
              query(test.observer, 'SELECT id,val,b,note FROM biz.orders'), expected[:1])
        if args.mode == 'cancel':
            holder = db.connect()
            query(holder, 'BEGIN')
            query(holder, 'LOCK TABLE created_metadata.ducklake_table_column_stats IN SHARE MODE')
            publisher = scalar(test.a, 'SELECT pg_backend_pid()')

            def commit():
                try:
                    query(test.a, 'COMMIT')
                    return 'accepted'
                except psycopg2.Error as exc:
                    return exc.pgcode

            executor = ThreadPoolExecutor(max_workers=1)
            future = executor.submit(commit)
            deadline = time.monotonic() + 10
            waits = []
            while time.monotonic() < deadline:
                waits = query(test.observer, "SELECT pid,mode FROM pg_locks WHERE relation="
                              "'created_metadata.ducklake_table_column_stats'::regclass AND pid=%s "
                              "AND mode='RowExclusiveLock' AND NOT granted", (publisher,))
                if waits or future.done():
                    break
                time.sleep(.02)
            check('publisher reaches final column statistics after row batches', len(waits), 1)
            lock_count = scalar(test.observer, 'SELECT count(*) FROM pg_locks WHERE relation=%s::regclass '
                                "AND pid=%s AND mode='RowExclusiveLock' AND granted",
                                ('created_metadata.' + carrier, publisher))
            check('publisher already inserted into physical inline carrier', lock_count, 1)
            check('all staged protocol changes remain invisible', test.protocol(), original_protocol)
            check('cancel exact owned batch publisher', scalar(test.observer, 'SELECT pg_cancel_backend(%s)', (publisher,)), True)
            check('real batch COMMIT reports cancellation', future.result(timeout=10), '57014')
            query(test.a, 'ROLLBACK')
            query(holder, 'ROLLBACK')
            check('canceled batches restore complete protocol', test.protocol(), original_protocol)
            check('canceled batches restore logical counters', test.head(), original_head)
            check('ordinary heap write rolls back with lake batches', query(test.observer, 'SELECT * FROM biz.heap_marker'), [])
            check('all canceled typed rows invisible', query(test.observer, 'SELECT id,val,b,note FROM biz.orders'), expected[:1])
            query(test.a, bulk)
        else:
            query(test.a, 'COMMIT')
            check('ordinary heap write commits with lake batches', query(test.observer, 'SELECT * FROM biz.heap_marker'), [(1,)])
        check('every committed typed value across batch boundaries',
              query(test.observer, 'SELECT id,val,b,note FROM biz.orders ORDER BY id'), expected)
        check('exactly one successful publication snapshot', test.head()[0], original_head[0] + 1)
        check('final logical counters contain no canceled/savepoint IDs',
              query(test.observer, 'SELECT record_count,next_row_id FROM created_metadata.ducklake_table_stats'), [(1538, 1538)])
        physical = query(test.observer, sql.SQL('SELECT _ducklake_row_id,_ducklake_begin_snapshot,'
            '_ducklake_end_snapshot,id,val,b,note FROM created_metadata.{} ORDER BY id').format(sql.Identifier(carrier)))
        final_snapshot = test.head()[0]
        wanted = [(row[0] - 1, original_head[0] if row[0] == 1 else final_snapshot, None, *row) for row in expected]
        check('every physical identity birth and typed row is exact', physical, wanted)
        check('column NULL flags include rows from every batch', query(test.observer,
              'SELECT column_id,contains_null FROM created_metadata.ducklake_table_column_stats ORDER BY column_id'),
              [(1, False), (2, True), (3, True), (4, True)])
        names = query(test.observer, "SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
                      "WHERE n.nspname='created_metadata' AND c.relkind='r' ORDER BY c.relname")
        native = read_native_catalog(test.observer, 'created_metadata',
            {'tables': [{'table_name': name} for name, in names]}, out / 'native-catalog.json')
        lock = json.loads((DRIVER / 'source-lock.json').read_text())
        receipt = json.loads((ROOT / '.reference/opengauss-native-lake/build/receipt.json').read_text())
        check('independent reader uses exact source lock', receipt['source_lock_hash'], sha256(DRIVER / 'source-lock.json'))
        ref = Reference(out, lock, receipt)
        catalog = out / 'transport.ducklake'
        transport_native(ref, catalog, native)
        answer = ref.run(ref.attach(catalog, True) + 'SELECT rowid,id,val,b,note FROM lake.main.orders ORDER BY id',
                         'independent-all-typed-rows', result=True)
        check('independent DuckLake verifies every ID and typed value',
              [(x['rowid'], x['id'], x['val'], x['b'], x['note']) for x in answer],
              [(row[0] - 1, *row) for row in expected])
        check('executed inputs stayed fixed', all(sha256(p) == h for p, h in result['input_hashes'].items()))
        check_build(args.build_receipt)
        result['status'] = 'pass'
    except BaseException as exc:
        result.update(reason=str(exc), traceback=traceback.format_exc())
    finally:
        if future is not None and not future.done():
            try:
                scalar(test.observer, 'SELECT pg_cancel_backend(%s)', (publisher,))
            except BaseException:
                pass
        if holder is not None:
            try:
                query(holder, 'ROLLBACK')
            except BaseException:
                pass
        if executor is not None:
            executor.shutdown(wait=True)
        if db is not None:
            try:
                db.cleanup()
                result['database_cleanup'] = db.cleanup_result
            except BaseException as exc:
                result.update(status='fail', cleanup_error=str(exc))
        if roles is not None:
            try:
                if not result.get('database_cleanup') or result['database_cleanup']['returncode']:
                    raise RuntimeError('Owned DB cleanup unproven; role retained')
                result['role_cleanup'] = cleanup_roles(control, roles.path, args.run_id)
                result = roles.redact(result)
            except BaseException as exc:
                result.update(status='fail', role_cleanup_error=str(exc))
        if control is not None:
            control.close()
        write_json(out / 'result.json', result)
    print(json.dumps({'status': result['status'], 'reason': result.get('reason'),
                      'checks': len(result['assertions']), 'result': str(out / 'result.json')}))
    return 0 if result['status'] == 'pass' else 1


if __name__ == '__main__':
    raise SystemExit(main())
