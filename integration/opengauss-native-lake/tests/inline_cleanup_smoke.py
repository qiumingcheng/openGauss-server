#!/usr/bin/env python3
"""Real owned-DB cleanup after SIGKILL of this test's own Python subprocess.

This checks harness cleanup, not a database crash or product durability.
"""
import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

DRIVER = Path(__file__).resolve().parents[1]
ROOT = DRIVER.parents[1]
sys.path.insert(0, str(DRIVER))
from inline_fixture import query, scalar
from lake_harness import read_json, validate_run_id, write_json


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--run-id', default=os.getenv('OG_LAKE_RUN_ID'))
    parser.add_argument('--output', default=os.getenv('OG_LAKE_RESULT_FILE'))
    parser.add_argument('--manifest', default=os.getenv('OG_LAKE_FIXTURE_MANIFEST'))
    args = parser.parse_args()
    if not args.run_id or not args.output or not args.manifest:
        parser.error('run-id, output and real fixture manifest required')
    validate_run_id(args.run_id)
    output = Path(args.output).resolve()
    child_dir = output.parent / 'controlled-child'
    child_dir.mkdir()
    result = {'status': 'fail', 'scope': 'test_harness_owned_database_cleanup',
              'assertions': [], 'artifacts': [], 'notes': [
                  'SIGKILL targets only the Python subprocess started with a new process group by this test.',
                  'No server process or database backend is killed. This does not test database crash recovery.',
                  'Rejected ownership receipts are deliberate negative test inputs, not standard lake protocol rows.']}
    child = None
    admin = None
    log = None
    receipt = None

    def check(name, actual, expected=True):
        result['assertions'].append({'name': name, 'status': 'pass' if actual == expected else 'fail',
                                     'details': {'actual': actual, 'expected': expected}})
        if actual != expected:
            raise AssertionError(name)

    def cleanup(directory):
        completed = subprocess.run([sys.executable, str(DRIVER / 'tests/inline_cleanup.py'),
                                    '--run-id', args.run_id, '--attempt', str(directory)],
                                   capture_output=True, text=True, timeout=30)
        (directory / ('cleanup-driver-' + str(time.time_ns()) + '.log')).write_text(completed.stdout + completed.stderr)
        return completed

    try:
        import psycopg2
        admin = psycopg2.connect(host=str(ROOT / '.runtime/opengauss-native-lake/debug/socket'),
                                port=55560, dbname='postgres', user=os.getenv('USER', 'qiumc'),
                                connect_timeout=5, application_name='og_lake_cleanup_test_controller')
        admin.autocommit = True
        query(admin, 'SET statement_timeout=10000')
        postgres_oid = scalar(admin, "SELECT oid FROM pg_database WHERE datname='postgres'")
        log = (child_dir / 'child.log').open('w')
        child = subprocess.Popen([sys.executable, str(DRIVER / 'tests/inline_scan.py'),
                                  '--run-id', args.run_id, '--output', str(child_dir / 'test-result.json'),
                                  '--manifest', str(Path(args.manifest).resolve()), '--mode', 'read',
                                  '--test-hold-after-allocation'], stdout=log, stderr=subprocess.STDOUT,
                                 start_new_session=True)
        deadline = time.monotonic() + 40
        while not (child_dir / 'hold-ready.json').is_file():
            if child.poll() is not None:
                raise RuntimeError('Controlled child exited before real allocation; inspect child.log')
            if time.monotonic() >= deadline:
                raise RuntimeError('Timed out waiting for actual owned database and target connections')
            time.sleep(0.05)
        receipt = read_json(child_dir / 'database-owner.json')
        check('child completed a real OID and ownership marker receipt', receipt['phase'], 'owned')
        check('real allocated database exists before termination', scalar(admin,
              'SELECT count(*) FROM pg_database WHERE oid=%s AND datname=%s',
              (receipt['database_oid'], receipt['database'])), 1)
        check('child created actual target database sessions', scalar(admin,
              'SELECT count(*) FROM pg_stat_activity WHERE datid=%s', (receipt['database_oid'],)) > 0)
        check('controlled child has its own process group', os.getpgid(child.pid), child.pid)
        os.killpg(child.pid, signal.SIGKILL)
        check('controlled Python child was killed', child.wait(timeout=5), -signal.SIGKILL)
        check('hard termination bypassed child finally cleanup', (child_dir / 'test-result.json').exists(), False)
        check('database is still present before independent cleanup', scalar(admin,
              'SELECT count(*) FROM pg_database WHERE oid=%s', (receipt['database_oid'],)), 1)

        for name, change in (('wrong-oid', {'database_oid': postgres_oid}),
                             ('wrong-marker', {'marker': 'not-this-test-owner'})):
            negative = output.parent / name
            negative.mkdir()
            rejected = dict(receipt, attempt=str(negative), **change)
            write_json(negative / 'database-owner.json', rejected)
            completed = cleanup(negative)
            check('mismatched receipt is refused: ' + name, completed.returncode, 1)
            check('refusal preserves the test database: ' + name, scalar(admin,
                  'SELECT count(*) FROM pg_database WHERE oid=%s', (receipt['database_oid'],)), 1)
            check('refusal preserves unrelated postgres database: ' + name,
                  scalar(admin, "SELECT oid FROM pg_database WHERE datname='postgres'"), postgres_oid)

        completed = cleanup(child_dir)
        check('correct independent cleanup succeeds after hard termination', completed.returncode, 0)
        check('owned database is absent by both original OID and name', scalar(admin,
              'SELECT count(*) FROM pg_database WHERE oid=%s OR datname=%s',
              (receipt['database_oid'], receipt['database'])), 0)
        check('postgres identity is unchanged by successful cleanup',
              scalar(admin, "SELECT oid FROM pg_database WHERE datname='postgres'"), postgres_oid)
        check('repeated cleanup is harmless', cleanup(child_dir).returncode, 0)
        result['status'] = 'pass'
    except BaseException as exc:
        result['error'] = repr(exc)
        if not result['assertions'] or result['assertions'][-1]['status'] != 'fail':
            result['assertions'].append({'name': 'cleanup fault test completed', 'status': 'fail', 'details': repr(exc)})
    finally:
        if child is not None and child.poll() is None:
            # This PID is the subprocess this function created, never a server PID.
            os.killpg(child.pid, signal.SIGKILL)
            child.wait(timeout=5)
        if log is not None:
            log.close()
        if admin is not None:
            admin.close()
        try:
            final = cleanup(child_dir)
            if final.returncode:
                result.update(status='fail', cleanup_error=final.stdout + final.stderr)
        except BaseException as exc:
            result.update(status='fail', cleanup_error=repr(exc))
        result['artifacts'] = [{'path': str(path)} for path in output.parent.rglob('*')
                               if path.is_file() and path != output and path.name != 'database-cleanup.json']
        write_json(output, result)
        print(json.dumps({'status': result['status'], 'assertions': len(result['assertions']),
                          'output': str(output)}, ensure_ascii=False))
    return 0 if result['status'] == 'pass' else 1


if __name__ == '__main__':
    raise SystemExit(main())
