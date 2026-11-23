#!/usr/bin/env python3
"""Qualify native SQL session-lock primitives needed by approved GC design.

This checks host primitives only. It implements neither lake protection hooks
nor file deletion, and supplies no cross-instance or target-RAC proof.
"""
import argparse
import json
from pathlib import Path
import shutil
import sys
import time
import traceback

ROOT = Path('/home/qiumc/data/code/cpp')
DRIVER = ROOT / 'integration/opengauss-native-lake'
sys.path[:0] = [str(DRIVER), str(DRIVER / 'tests')]
from file_dml_acceptance import check_build
from inline_fixture import query, scalar
from lake_harness import sha256, validate_run_id, write_json
from native_test_context import OwnedDatabase


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--build-receipt', type=Path, required=True)
    args = parser.parse_args()
    validate_run_id(args.run_id)
    out = ROOT / '公共文档/openGauss原生湖存储/验证' / args.run_id
    out.mkdir(exist_ok=False)
    result = {'status': 'fail', 'scope': 'Single-instance native session advisory-lock contract only',
              'assertions': [], 'limitations': ['No lake GC/protection implementation or remote/RAC qualification.']}
    inputs = [Path(__file__), DRIVER / 'native_test_context.py', DRIVER / 'inline_fixture.py']
    result['inputs'] = {str(p): sha256(p) for p in inputs}
    archive = out / 'executed-inputs'
    archive.mkdir()
    for p in inputs:
        shutil.copy2(p, archive / p.name)
    db = None

    def check(name, observed, expected=True):
        ok = observed == expected
        result['assertions'].append({'name': name, 'observed': observed,
                                     'expected': expected, 'status': 'pass' if ok else 'fail'})
        if not ok:
            raise AssertionError(name)

    try:
        build = check_build(args.build_receipt)
        result.update(source_head=build['source_head'], binary_sha256=build['binary_sha256'])
        db = OwnedDatabase(args.run_id, out).create()
        a, b, g = [db.connect() for _ in range(3)]
        key = (0x4f474c47, db.allocation['database_oid'])

        def lock(conn, exclusive=False):
            name = 'pg_try_advisory_lock' + ('' if exclusive else '_shared')
            return scalar(conn, 'SELECT ' + name + '(%s::integer,%s::integer)', key)

        def unlock(conn, exclusive=False):
            name = 'pg_advisory_unlock' + ('' if exclusive else '_shared')
            return scalar(conn, 'SELECT ' + name + '(%s::integer,%s::integer)', key)

        check('specific host test runs with dedicated backend sessions', scalar(a, 'SHOW enable_thread_pool'), 'off')
        check('first reader obtains shared session lock', lock(a))
        check('second reader obtains compatible shared session lock', lock(b))
        check('GC exclusive try fails immediately while readers exist', lock(g, True), False)
        query(a, 'BEGIN')
        query(a, 'COMMIT')
        check('shared protection survives transaction COMMIT', lock(g, True), False)
        query(b, 'BEGIN')
        query(b, 'SAVEPOINT child')
        query(b, 'ROLLBACK TO child')
        query(b, 'ROLLBACK')
        check('shared protection survives savepoint and transaction rollback', lock(g, True), False)
        check('same-session shared acquisition is reentrant', lock(a))
        check('first release consumes one reentrant ownership', unlock(a))
        check('remaining shared ownership still protects reader', lock(g, True), False)
        check('release second ownership of first reader', unlock(a))
        check('independent second reader still protects objects', lock(g, True), False)
        check('second reader releases exact session ownership', unlock(b))
        check('GC obtains exclusive protection after last reader', lock(g, True))
        check('new reader try fails during exclusive phase', lock(a), False)
        query(g, 'BEGIN')
        query(g, 'COMMIT')
        check('exclusive protection survives worker transaction boundary', lock(b), False)
        # The gate is separate from ordinary database locking. The future lake
        # hooks must explicitly participate; these statements prove no GC hook.
        query(a, 'CREATE TABLE heap_probe(id integer)')
        query(a, 'INSERT INTO heap_probe VALUES(1)')
        check('ordinary heap DML remains independent of the advisory gate', query(b, 'SELECT * FROM heap_probe'), [(1,)])
        check('GC explicitly releases session protection', unlock(g, True))
        check('new reader can acquire after GC releases', lock(a))
        a.close()
        deadline = time.monotonic() + 5
        acquired = False
        while time.monotonic() < deadline:
            acquired = lock(g, True)
            if acquired:
                break
            time.sleep(.01)
        check('backend exit releases remaining session ownership', acquired)
        check('final GC release is balanced', unlock(g, True))
        check('no unintended reentrant exclusive ownership remains', unlock(g, True), False)
        check('all executed inputs remained fixed', all(sha256(p) == h for p, h in result['inputs'].items()))
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
        write_json(out / 'result.json', result)
    print(json.dumps({'status': result['status'], 'checks': len(result['assertions']),
                      'reason': result.get('reason'), 'result': str(out / 'result.json')}))
    return 0 if result['status'] == 'pass' else 1


if __name__ == '__main__':
    raise SystemExit(main())
