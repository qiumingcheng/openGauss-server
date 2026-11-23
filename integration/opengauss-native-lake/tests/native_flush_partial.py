#!/usr/bin/env python3
"""Exercise an actual >262144-version inline input, never fabricated rows/IDs."""
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
from empty_catalog_oracle import read_native_catalog, transport_native
from reverse_reference import Reference


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--run-id', required=True)
    p.add_argument('--build-receipt', type=Path, required=True)
    p.add_argument('--expect', choices=['budget_refusal', 'partial'], required=True)
    args = p.parse_args()
    validate_run_id(args.run_id)
    out = ROOT / '公共文档/openGauss原生湖存储/验证' / args.run_id
    out.mkdir(exist_ok=False)
    result = {'status': 'fail', 'scope': 'Native >262144-version flush: ' + args.expect,
              'assertions': [], 'limitations': ['Debug local correctness only; no remote/RAC, performance or arbitrary-size row qualification.']}
    inputs = [Path(__file__), DRIVER / 'native_test_context.py', DRIVER / 'tests/native_table_create.py',
              DRIVER / 'tests/permission_api.py', DRIVER / 'tests/empty_catalog_oracle.py',
              DRIVER / 'tests/reverse_reference.py', DRIVER / 'inline_fixture.py']
    result['input_hashes'] = {str(x): sha256(x) for x in inputs}
    archive = out / 'executed-inputs'
    archive.mkdir()
    for x in inputs:
        shutil.copy2(x, archive / x.name)
    db = roles = control = None
    bulk = 262160
    boundary = 262144

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
                           stdout=log, stderr=subprocess.STDOUT, timeout=300, check=True)
        control = psycopg2.connect(host=str(ROOT / '.runtime/opengauss-native-lake/debug/socket'),
                                  port=55560, dbname='postgres', user=os.getenv('USER', 'qiumc'))
        control.autocommit = True
        db = OwnedDatabase(args.run_id, out).create()
        roles = OwnedRoles(control, args.run_id, out)
        case_args = SimpleNamespace(mode='net_dml', sql_version='0.11',
                                   manifest=out / 'fixture/manifest.json', output=out / 'result.json', run_id=args.run_id)
        t = Case(db, case_args, result, roles)
        t.create()
        for conn in [t.a, t.b, t.observer]:
            query(conn, 'SET statement_timeout=180000')
        # Seed the genuine inline carrier through the public DML path before
        # batching. No hidden business/version/identity records are inserted.
        query(t.a, 'INSERT INTO biz.orders VALUES(1,10)')
        for first in range(2, bulk + 1, 16384):
            last = min(first + 16383, bulk)
            query(t.a, 'INSERT INTO biz.orders SELECT s,s*10 FROM generate_series(%s,%s) s', (first, last))
        original_snapshot = t.head()[0]
        query(t.a, 'UPDATE biz.orders SET val=val+1 WHERE id=%s', (boundary,))
        first_update = t.head()[0]
        query(t.a, 'UPDATE biz.orders SET val=val+1 WHERE id=%s', (boundary,))
        second_update = t.head()[0]
        carrier = scalar(t.a, 'SELECT table_name FROM created_metadata.ducklake_inlined_data_tables')
        source = sql.SQL('created_metadata.{}').format(sql.Identifier(carrier))

        def inline_count():
            return scalar(t.observer, sql.SQL('SELECT count(*) FROM {}').format(source))

        def exact_current(conn, count):
            # A dense ID range, no duplicates, and an exact value predicate
            # jointly validate every row without a giant JSON transcript.
            observed = query(conn, 'SELECT count(*),count(DISTINCT id),min(id),max(id),'
                             'sum(CASE WHEN val=id*10+CASE WHEN id=%s THEN 2 ELSE 0 END '
                             'THEN 0 ELSE 1 END) FROM biz.orders', (boundary,))[0]
            check('every current value and dense unique identity range (' + str(count) + ')',
                  observed, (count, count, 1, count, 0))

        check('genuine native input exceeds one batch', inline_count(), bulk + 2)
        exact_current(t.observer, bulk)
        head = t.head()
        stats = query(t.observer, 'SELECT record_count,next_row_id FROM created_metadata.ducklake_table_stats')
        root = Path(t.empty_manifest['data_path'])
        if args.expect == 'budget_refusal':
            try:
                query(t.a, "SELECT og_lake.flush('biz.orders'::regclass)")
            except psycopg2.Error as exc:
                check('baseline refuses over-budget complete input', exc.pgcode, '54000')
                result['baseline_error'] = str(exc)
            else:
                raise AssertionError('Expected original single-output budget refusal')
            check('refused flush leaves all physical input versions', inline_count(), bulk + 2)
            check('refused flush leaves exact snapshot/frontiers', t.head(), head)
            check('refused flush removes its finished/unfinished candidates', list(root.rglob('*.parquet')), [])
            exact_current(t.observer, bulk)
        else:
            query(t.a, "SET work_mem='64kB'")
            query(t.b, 'BEGIN ISOLATION LEVEL READ COMMITTED')
            exact_current(t.b, bulk)
            query(t.a, 'BEGIN')
            query(t.a, "SELECT og_lake.flush('biz.orders'::regclass)")
            writer = db.connect()
            query(writer, 'INSERT INTO biz.orders VALUES(%s,%s)', (bulk + 1, (bulk + 1) * 10))
            query(t.a, 'COMMIT')
            check('first partial flush removes only 262144 transferred versions', inline_count(), bulk + 3 - 262144)
            check('first partial flush registers one bounded data file',
                  query(t.observer, 'SELECT record_count FROM created_metadata.ducklake_data_file'), [(262144,)])
            remaining_boundary = query(t.observer, sql.SQL('SELECT _ducklake_row_id,_ducklake_begin_snapshot,'
                '_ducklake_end_snapshot,id,val FROM {} WHERE id=%s ORDER BY _ducklake_begin_snapshot').format(source), (boundary,))
            check('same logical row crosses exact version-prefix boundary', remaining_boundary,
                  [(boundary - 1, first_update, second_update, boundary, boundary * 10 + 1),
                   (boundary - 1, second_update, None, boundary, boundary * 10 + 2)])
            exact_current(t.observer, bulk + 1)
            exact_current(t.b, bulk)
            check('logical statistics allocate only the real concurrent insert',
                  query(t.observer, 'SELECT record_count,next_row_id FROM created_metadata.ducklake_table_stats'),
                  [(stats[0][0] + 1, stats[0][1] + 1)])
            query(t.a, "SELECT og_lake.flush('biz.orders'::regclass)")
            check('second partial flush drains remaining precise input', inline_count(), 0)
            check('both data files together cover physical history exactly',
                  scalar(t.observer, 'SELECT sum(record_count)::bigint FROM created_metadata.ducklake_data_file'), bulk + 3)
            exact_current(t.observer, bulk + 1)
            exact_current(t.b, bulk)
            query(t.b, 'ROLLBACK')
            after = t.head()
            disk = {str(p): sha256(p) for p in root.rglob('*.parquet')}
            query(t.a, "SELECT og_lake.flush('biz.orders'::regclass)")
            check('drained flush is a metadata no-op', t.head(), after)
            check('drained flush emits no files', {str(p): sha256(p) for p in root.rglob('*.parquet')}, disk)
            names = query(t.observer, "SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
                          "WHERE n.nspname='created_metadata' AND c.relkind='r' ORDER BY c.relname")
            native = read_native_catalog(t.observer, 'created_metadata',
                {'tables': [{'table_name': name} for name, in names]}, out / 'native-catalog.json')
            lock = json.loads((DRIVER / 'source-lock.json').read_text())
            receipt = json.loads((ROOT / '.reference/opengauss-native-lake/build/receipt.json').read_text())
            check('independent reader uses exact source lock', receipt['source_lock_hash'], sha256(DRIVER / 'source-lock.json'))
            ref = Reference(out, lock, receipt)
            catalog = out / 'transport.ducklake'
            transport_native(ref, catalog, native)
            for sid, delta, count in [(original_snapshot, 0, bulk), (first_update, 1, bulk),
                                      (second_update, 2, bulk), (after[0], 2, bulk + 1)]:
                answer = ref.run(ref.attach(catalog, True) +
                    'SELECT count(*) AS n,count(DISTINCT id) AS nd,min(id) AS lo,max(id) AS hi,'
                    'count(*) FILTER(WHERE rowid<>id-1 OR val<>id*10+CASE WHEN id=' + str(boundary) +
                    ' THEN ' + str(delta) + ' ELSE 0 END) AS invalid FROM lake.main.orders AT (VERSION => ' +
                    str(sid) + ')', 'independent-complete-version-' + str(sid), result=True)
                check('independent reader validates every logical ID and value at snapshot ' + str(sid),
                      answer, [{'n': count, 'nd': count, 'lo': 1, 'hi': count, 'invalid': 0}])
        check('executed inputs stayed fixed', all(sha256(x) == h for x, h in result['input_hashes'].items()))
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
