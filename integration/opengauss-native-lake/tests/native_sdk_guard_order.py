#!/usr/bin/env python3
"""Observe actual native guard ownership during delayed real SDK abort.

Requires the separately built test-only SDK adapter already installed by an
exclusive, reversible restart controller. No file/protocol data is simulated.
"""
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
from reverse_reference import Reference, literal


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--run-id', required=True)
    p.add_argument('--build-receipt', type=Path, required=True)
    p.add_argument('--barrier-build', type=Path, required=True)
    p.add_argument('--mode', choices=['transaction', 'savepoint'], required=True)
    p.add_argument('--expect', choices=['premature', 'protected'], required=True)
    args = p.parse_args()
    validate_run_id(args.run_id)
    out = ROOT / '公共文档/openGauss原生湖存储/验证' / args.run_id
    out.mkdir(exist_ok=False)
    result = {'status': 'fail', 'scope': 'Actual candidate abort protection order: ' + args.mode + '/' + args.expect,
              'assertions': [], 'limitations': ['Lifecycle-only delay adapter delegates all Parquet/abort work to the real provider.',
                                             'No physical GC, remote asynchronous SDK or target RAC qualification.']}
    inputs = [Path(__file__), DRIVER / 'native_test_context.py', DRIVER / 'tests/native_table_create.py',
              DRIVER / 'tests/permission_api.py', DRIVER / 'tests/reverse_reference.py', DRIVER / 'inline_fixture.py']
    result['inputs'] = {str(x): sha256(x) for x in inputs}
    archive = out / 'executed-inputs'
    archive.mkdir()
    for x in inputs:
        shutil.copy2(x, archive / x.name)
    db = roles = control = executor = future = test = None
    barrier = None

    def check(name, observed, expected=True):
        ok = observed == expected
        result['assertions'].append({'name': name, 'observed': observed,
                                     'expected': expected, 'status': 'pass' if ok else 'fail'})
        if not ok:
            raise AssertionError(name)

    def release_barrier():
        if barrier is not None and (barrier / 'arm').exists() and not (barrier / 'continue').exists():
            (barrier / 'continue').write_text(args.run_id + '\n')

    try:
        import psycopg2
        build = check_build(args.build_receipt)
        result.update(source_head=build['source_head'], binary_sha256=build['binary_sha256'])
        adapter = json.loads(args.barrier_build.read_text())
        check('reviewed adapter was built without installation', adapter['status'], 'built_unloaded')
        check('genuine provider copy unchanged', sha256(adapter['real_provider']), adapter['real_provider_sha256'])
        check('adapter binary unchanged', sha256(adapter['library']), adapter['library_sha256'])
        check('SDK ABI header matches actual native source', sha256(adapter['header']), adapter['header_sha256'])
        installed = ROOT / '.install/opengauss-native-lake-debug/lib/postgresql/og_lake_sdk/libog_lake_sdk_arrow.so'
        check('dedicated runtime uses exact lifecycle delay adapter', sha256(installed), adapter['library_sha256'])
        barrier = Path(adapter['barrier_directory'])
        owner = json.loads((barrier / 'owner.json').read_text())
        check('barrier directory has exact project ownership',
              (owner['project'], owner['path']), ('opengauss-native-lake', str(barrier)))
        check('barrier markers initially absent',
              [x for x in ['arm', 'arrived', 'continue', 'completed', 'timeout'] if (barrier / x).exists()], [])
        result['adapter'] = {'receipt': str(args.barrier_build), 'sha256': sha256(args.barrier_build),
                             'library_sha256': adapter['library_sha256'], 'real_provider_sha256': adapter['real_provider_sha256']}
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
        query(test.a, 'INSERT INTO biz.orders VALUES(1,10),(2,20),(3,30)')
        query(test.a, 'UPDATE biz.orders SET val=11 WHERE id=1')
        original = test.protocol()
        server = scalar(test.observer, "SELECT oid FROM pg_foreign_server WHERE srvname='created_lake'")

        def exclusive_probe():
            # Autocommit closes and releases any successful diagnostic guard.
            return scalar(test.b, 'SELECT og_lake_internal.test_catalog_guard_acquire(%s::oid,true)', (server,))

        query(test.a, 'BEGIN')
        if args.mode == 'savepoint':
            query(test.a, 'SAVEPOINT child')
        query(test.a, "SELECT og_lake.flush('biz.orders'::regclass)")
        root = Path(test.empty_manifest['data_path'])
        candidates = sorted(root.rglob('*.parquet'))
        check('real immutable data plus history-delete candidates exist', len(candidates), 2)
        lock = json.loads((DRIVER / 'source-lock.json').read_text())
        reference_receipt = json.loads((ROOT / '.reference/opengauss-native-lake/build/receipt.json').read_text())
        check('real independent reader matches exact source lock', reference_receipt['source_lock_hash'], sha256(DRIVER / 'source-lock.json'))
        ref = Reference(out, lock, reference_receipt)
        counts = []
        for i, path in enumerate(candidates):
            answer = ref.run('SELECT count(*) AS n FROM read_parquet(' + literal(str(path)) + ')',
                             'actual-candidate-' + str(i), result=True)
            counts.append(answer[0]['n'])
        check('independent reader verifies real Parquet populations', sorted(counts), [1, 4])
        check('native catalog shared protection is held before abort', exclusive_probe(), False)
        (barrier / 'arm').write_text(args.run_id + '\n')

        def rollback():
            try:
                query(test.a, 'ROLLBACK' if args.mode == 'transaction' else 'ROLLBACK TO child')
                return 'completed'
            except psycopg2.Error as exc:
                return exc.pgcode

        executor = ThreadPoolExecutor(max_workers=1)
        future = executor.submit(rollback)
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline and not (barrier / 'arrived').exists() and not future.done():
            time.sleep(.01)
        check('actual native cleanup entered real-provider abort barrier', (barrier / 'arrived').exists())
        check('real SDK abort has not completed', (barrier / 'completed').exists(), False)
        check('native rollback still waiting on real candidate cleanup', future.done(), False)
        check('candidate objects still exist while abort is paused', all(p.is_file() for p in candidates))
        acquired = exclusive_probe()
        result['exclusive_during_live_sdk_abort'] = acquired
        check('observed exact expected protection ownership during live SDK abort', acquired, args.expect == 'premature')
        release_barrier()
        check('real rollback finishes after SDK release', future.result(timeout=15), 'completed')
        if args.mode == 'savepoint':
            query(test.a, 'ROLLBACK')
        check('real SDK abort completed', (barrier / 'completed').exists())
        check('barrier had no fallback timeout', (barrier / 'timeout').exists(), False)
        check('real SDK removed every unpublished candidate', list(root.rglob('*.parquet')), [])
        check('complete committed lake protocol preserved', test.protocol(), original)
        check('all committed business values preserved', test.rows(test.observer), [(1, 11), (2, 20), (3, 30)])
        check('exclusive protection becomes available after SDK cleanup', exclusive_probe(), True)
        check('all input bytes remain fixed', all(sha256(p) == h for p, h in result['inputs'].items()))
        check_build(args.build_receipt)
        result['status'] = 'pass'
    except BaseException as exc:
        result.update(reason=str(exc), traceback=traceback.format_exc())
    finally:
        release_barrier()
        if executor is not None:
            executor.shutdown(wait=True)
        if barrier is not None:
            markers = out / 'barrier-markers'
            markers.mkdir(exist_ok=True)
            for name in ['owner.json', 'arm', 'arrived', 'continue', 'completed', 'timeout']:
                if (barrier / name).is_file():
                    shutil.copy2(barrier / name, markers / name)
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
    print(json.dumps({'status': result['status'], 'mode': args.mode, 'expect': args.expect,
                      'checks': len(result['assertions']), 'reason': result.get('reason'), 'result': str(out / 'result.json')}))
    return 0 if result['status'] == 'pass' else 1


if __name__ == '__main__':
    raise SystemExit(main())
