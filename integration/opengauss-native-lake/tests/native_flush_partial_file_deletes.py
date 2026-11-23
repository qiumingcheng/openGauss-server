#!/usr/bin/env python3
"""Drain more marked data files than one transaction's candidate-file budget."""
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
    result = {'status': 'fail', 'scope': 'Actual eighteen-file deletion flush: ' + args.expect,
              'assertions': [], 'limitations': ['Local bounded correctness; no physical GC or remote/RAC qualification.']}
    inputs = [Path(__file__), DRIVER / 'native_test_context.py', DRIVER / 'tests/native_table_create.py',
              DRIVER / 'tests/permission_api.py', DRIVER / 'tests/empty_catalog_oracle.py',
              DRIVER / 'tests/reverse_reference.py', DRIVER / 'inline_fixture.py']
    result['input_hashes'] = {str(x): sha256(x) for x in inputs}
    archive = out / 'executed-inputs'
    archive.mkdir()
    for x in inputs:
        shutil.copy2(x, archive / x.name)
    db = roles = control = None

    def check(name, observed, expected=True):
        ok = observed == expected
        result['assertions'].append({'name': name, 'observed': observed,
                                     'expected': expected, 'status': 'pass' if ok else 'fail'})
        if not ok:
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
        t = Case(db, SimpleNamespace(mode='net_dml', sql_version='0.11', manifest=out / 'fixture/manifest.json',
                                    output=out / 'result.json', run_id=args.run_id), result, roles)
        t.create()
        for group in range(18):
            first = group * 3 + 1
            query(t.a, 'INSERT INTO biz.orders SELECT s,s*10 FROM generate_series(%s,%s) s', (first, first + 2))
            query(t.a, "SELECT og_lake.flush('biz.orders'::regclass)")
        check('eighteen genuine immutable native data files',
              scalar(t.a, 'SELECT count(*) FROM created_metadata.ducklake_data_file'), 18)
        pre_delete = t.head()[0]
        root = Path(t.empty_manifest['data_path'])
        original_files = {str(path): sha256(path) for path in root.rglob('*.parquet')}
        # Nine origins per transaction remain below the unchanged DML limit.
        query(t.a, 'DELETE FROM biz.orders WHERE id%3=2 AND id<=27')
        query(t.a, 'DELETE FROM biz.orders WHERE id%3=2 AND id>27')
        tid = scalar(t.a, 'SELECT table_id FROM created_metadata.ducklake_table')
        carrier = 'created_metadata.ducklake_inlined_delete_' + str(tid)

        def marks():
            return query(t.observer, 'SELECT file_id,row_id,begin_snapshot FROM ' + carrier + ' ORDER BY file_id,row_id')

        before_marks = marks()
        check('eighteen distinct data-file mark populations', len({r[0] for r in before_marks}), 18)
        check('all marks intentionally share physical position one', sorted({r[1] for r in before_marks}), [1])
        expected = [(i, i * 10) for i in range(1, 55) if i % 3 != 2]
        check('native deletes leave exact live rows', t.rows(t.observer), expected)
        protocol = t.protocol()
        if args.expect == 'budget_refusal':
            try:
                query(t.a, "SELECT og_lake.flush('biz.orders'::regclass)")
            except psycopg2.Error as exc:
                check('baseline refuses more than sixteen outputs', exc.pgcode, '54000')
                result['baseline_error'] = str(exc)
            else:
                raise AssertionError('Expected original candidate-budget refusal')
            check('refusal keeps complete catalog unchanged', t.protocol(), protocol)
            check('refusal keeps every exact input marker', marks(), before_marks)
            check('refusal creates no candidate residue', {str(p): sha256(p) for p in root.rglob('*.parquet')}, original_files)
        else:
            query(t.b, 'BEGIN ISOLATION LEVEL READ COMMITTED')
            check('old reader pins all eighteen inline mark populations', t.rows(t.b), expected)
            query(t.a, "SELECT og_lake.flush('biz.orders'::regclass)")
            remaining = marks()
            check('first call retains two complete original populations', len(remaining), 2)
            check('retained marks are exact original tuples', all(row in before_marks for row in remaining))
            check('first call registers sixteen immutable delete files',
                  scalar(t.observer, 'SELECT count(*) FROM created_metadata.ducklake_delete_file'), 16)
            check('native mixed inline/file delete sources cover all rows', t.rows(t.observer), expected)
            check('old reader survives first partial marker transfer', t.rows(t.b), expected)
            query(t.a, "SELECT og_lake.flush('biz.orders'::regclass)")
            check('second call drains remaining file populations', marks(), [])
            check('each original data file has one deletion file',
                  query(t.observer, 'SELECT count(*),count(DISTINCT data_file_id),sum(delete_count)::bigint '
                        'FROM created_metadata.ducklake_delete_file'), [(18, 18, 18)])
            check('all current values survive complete drain', t.rows(t.observer), expected)
            check('old reader still sees all old inline markers', t.rows(t.b), expected)
            query(t.b, 'ROLLBACK')
            check('original data files never rewritten', {name: sha256(name) for name in original_files}, original_files)
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
            answer = ref.run(ref.attach(catalog, True) + 'SELECT id,val FROM lake.main.orders ORDER BY id',
                             'independent-current', result=True)
            check('independent reader sees all current logical rows', [(x['id'], x['val']) for x in answer], expected)
            answer = ref.run(ref.attach(catalog, True) + 'SELECT id,val FROM lake.main.orders AT (VERSION => ' +
                             str(pre_delete) + ') ORDER BY id', 'independent-before-deletes', result=True)
            check('independent reader preserves complete original history', [(x['id'], x['val']) for x in answer],
                  [(i, i * 10) for i in range(1, 55)])
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
                      'checks': len(result['assertions']), 'result': str(out / 'result.json')}, default=str))
    return 0 if result['status'] == 'pass' else 1


if __name__ == '__main__':
    raise SystemExit(main())
