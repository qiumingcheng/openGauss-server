#!/usr/bin/env python3
"""Exclusively install a test lifecycle adapter, test, restore and restart.

Uses the official project restart entry, atomic SDK replacements and precise
owned-instance checks. It never changes product source or unrelated services.
"""
import argparse
import datetime
import fcntl
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time
import traceback

ROOT = Path('/home/qiumc/data/code/cpp')
DRIVER = ROOT / 'integration/opengauss-native-lake'
RUNTIME = ROOT / '.runtime/opengauss-native-lake/debug'
sys.path[:0] = [str(DRIVER), str(DRIVER / 'tests')]
from file_dml_acceptance import check_build
from inline_fixture import query, scalar
from lake_harness import sha256, validate_run_id, write_json


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--build-receipt', type=Path, required=True)
    parser.add_argument('--barrier-build', type=Path, required=True)
    parser.add_argument('--expect', choices=['premature', 'protected'], required=True)
    parser.add_argument('--wait-for', type=Path)
    args = parser.parse_args()
    validate_run_id(args.run_id)
    out = ROOT / '公共文档/openGauss原生湖存储/验证' / args.run_id
    out.mkdir(exist_ok=False)
    report = {'status': 'waiting_prior_tests' if args.wait_for else 'preparing', 'jobs': [],
              'scope': 'Real native SDK/guard cleanup order, expected=' + args.expect,
              'limitations': ['Test lifecycle delay only; no physical GC or remote/RAC qualification.']}
    installed = ROOT / '.install/opengauss-native-lake-debug/lib/postgresql/og_lake_sdk/libog_lake_sdk_arrow.so'
    changed = False
    adapter = None
    mutex = None
    restart_count = 0

    def save():
        report['updated_at'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        write_json(out / 'index.json', report)

    def check_exclusive():
        import psycopg2
        conn = psycopg2.connect(host=str(RUNTIME / 'socket'), port=55560, dbname='postgres',
                                user=os.getenv('USER', 'qiumc'), application_name='native_sdk_barrier_controller',
                                connect_timeout=5)
        conn.autocommit = True
        try:
            if Path(scalar(conn, 'SHOW data_directory')).resolve() != RUNTIME / 'data':
                raise RuntimeError('Controller reached another instance')
            owner = json.loads((RUNTIME / 'owner.json').read_text())
            if owner['project'] != 'opengauss-native-lake' or owner['data'] != str(RUNTIME / 'data'):
                raise RuntimeError('Runtime ownership changed')
            pid = int((RUNTIME / 'data/postmaster.pid').read_text().splitlines()[0])
            expected = ROOT / '.install/opengauss-native-lake-debug/bin/gaussdb'
            if (Path('/proc') / str(pid) / 'exe').resolve() != expected:
                raise RuntimeError('Process is not this installation')
            own = scalar(conn, 'SELECT pg_backend_pid()')
            deadline = time.monotonic() + 5
            while True:
                other = query(conn, 'SELECT pid,datname,application_name FROM pg_stat_activity '
                              'WHERE usename IS NOT NULL AND client_port IS NOT NULL AND pid<>%s', (own,))
                if not other:
                    break
                if time.monotonic() > deadline:
                    raise RuntimeError('Unrelated or surviving frontend sessions prevent restart: ' + repr(other))
                time.sleep(.05)
        finally:
            conn.close()

    def replace(source):
        descriptor, name = tempfile.mkstemp(prefix='.' + installed.name + '.' + args.run_id + '.', dir=installed.parent)
        temporary = Path(name)
        try:
            with os.fdopen(descriptor, 'wb') as destination, Path(source).open('rb') as origin:
                shutil.copyfileobj(origin, destination)
                os.fchmod(destination.fileno(), Path(source).stat().st_mode & 0o777)
                destination.flush()
                os.fsync(destination.fileno())
            os.replace(temporary, installed)
            directory = os.open(installed.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        finally:
            if temporary.exists():
                temporary.unlink()

    def restart(label):
        nonlocal restart_count
        restart_count += 1
        run_id = args.run_id + '_' + label + '_' + str(restart_count)
        command = ['bash', str(DRIVER / 'build.sh'), 'debug', 'restart', '--run-id', run_id]
        with (out / (label + '-' + str(restart_count) + '.log')).open('w') as log:
            subprocess.run(command, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT, check=True, timeout=240)
        receipt = out.parent / run_id / 'build.json'
        build = check_build(receipt)
        if build['binary_sha256'] != report['binary_sha256']:
            raise RuntimeError('Restart changed native binary')
        report.setdefault('restarts', []).append({'label': label, 'receipt': str(receipt), 'sha256': sha256(receipt)})
        save()

    def clear_markers():
        barrier = Path(adapter['barrier_directory'])
        owner = json.loads((barrier / 'owner.json').read_text())
        if owner['project'] != 'opengauss-native-lake' or owner['path'] != str(barrier):
            raise RuntimeError('Refusing to clear unowned lifecycle barrier')
        for name in ['arm', 'arrived', 'continue', 'completed', 'timeout']:
            path = barrier / name
            if path.exists():
                path.unlink()

    save()
    try:
        if args.wait_for:
            deadline = time.monotonic() + 4200
            while True:
                try:
                    prior = json.loads(args.wait_for.read_text()) if args.wait_for.exists() else {}
                except json.JSONDecodeError:
                    prior = {}  # Official build receipts may still be being written.
                if prior.get('status') in ['passed', 'build_and_sql_passed']:
                    break
                if prior.get('status') in ['failed', 'incomplete']:
                    raise RuntimeError('Prior tests failed; SDK was not changed')
                if time.monotonic() > deadline:
                    raise TimeoutError('Prior tests did not finish')
                time.sleep(3)
        mutex = (RUNTIME.parent / 'sdk-install.lock').open('a+')
        fcntl.flock(mutex, fcntl.LOCK_EX | fcntl.LOCK_NB)
        build = check_build(args.build_receipt)
        adapter = json.loads(args.barrier_build.read_text())
        if adapter['status'] != 'built_unloaded':
            raise RuntimeError('No successful unloaded barrier build receipt')
        for field, digest in [('library', 'library_sha256'), ('real_provider', 'real_provider_sha256'), ('header', 'header_sha256')]:
            if sha256(adapter[field]) != adapter[digest]:
                raise RuntimeError('Adapter input changed: ' + field)
        if sha256(installed) != adapter['real_provider_sha256']:
            raise RuntimeError('Original SDK is not the immutable genuine provider copy')
        report.update(source_head=build['source_head'], binary_sha256=build['binary_sha256'],
                      original_sdk_sha256=sha256(installed), adapter=str(args.barrier_build),
                      adapter_sha256=sha256(args.barrier_build), executed_driver_sha256=sha256(__file__))
        shutil.copy2(__file__, out / 'executed-controller.py')
        check_exclusive()
        clear_markers()
        changed = True
        replace(adapter['library'])
        report['status'] = 'testing_cleanup_protection_order'
        save()
        for mode in ['transaction', 'savepoint']:
            check_exclusive()
            clear_markers()
            restart('adapter_' + mode)
            run_id = args.run_id + '_' + mode
            command = [sys.executable, str(DRIVER / 'tests/native_sdk_guard_order.py'),
                       '--run-id', run_id, '--build-receipt', str(args.build_receipt),
                       '--barrier-build', str(args.barrier_build), '--expect', args.expect, '--mode', mode]
            with (out / (mode + '.log')).open('w') as log:
                process = subprocess.run(command, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT,
                                         check=False, timeout=600)
            result_path = out.parent / run_id / 'result.json'
            report['jobs'].append({'mode': mode, 'command': command, 'returncode': process.returncode,
                                   'result': str(result_path), 'result_sha256': sha256(result_path) if result_path.exists() else None})
            save()
            process.check_returncode()
            result = json.loads(result_path.read_text())
            if result['status'] != 'pass':
                raise RuntimeError('Nonpassing cleanup order case')
        report['status'] = 'passed'
        report['negative_premature_release_proven'] = args.expect == 'premature'
    except BaseException as exc:
        report.update(status='failed', error=str(exc), traceback=traceback.format_exc())
    finally:
        if changed:
            try:
                replace(adapter['real_provider'])
                report['restored_sdk_sha256'] = sha256(installed)
                check_exclusive()
                restart('restore_genuine_sdk')
                clear_markers()
                report['genuine_sdk_runtime_restored'] = True
            except BaseException as exc:
                report.update(status='failed', restore_error=str(exc), genuine_sdk_runtime_restored=False)
        save()
        if mutex is not None:
            mutex.close()
    print(json.dumps({'status': report['status'], 'error': report.get('error'),
                      'restored': report.get('genuine_sdk_runtime_restored'), 'index': str(out / 'index.json')}))
    return 0 if report['status'] == 'passed' else 1


if __name__ == '__main__':
    raise SystemExit(main())
