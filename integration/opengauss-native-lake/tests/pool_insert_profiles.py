#!/usr/bin/env python3
"""Independent inline lifecycle mode orchestration; no product edits or build.

Uses the official harness for existing cases, official build.sh restart for mode
changes, and restores the exact original configuration in finally.
"""
import argparse
import contextlib
import fcntl
import hashlib
import json
import os
from pathlib import Path
import signal
import stat
import subprocess
import sys
import traceback

CASES = ['N3/insert_commands', 'N3/insert_savepoints', 'N3/insert_cursors',
         'N3/late_precommit_callback', 'N3/insert_memory']
MODIFY_CASES = ['N3/insert_commands', 'N4/modify_chains', 'N4/modify_savepoints',
                'N4/modify_join_returning', 'N4/modify_conflicts', 'N4/modify_journal_memory']


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--workspace', type=Path, default=Path('/home/qiumc/data/code/cpp'))
    p.add_argument('--run-id', required=True)
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--build-receipt', type=Path, required=True,
                   help='Official successful full/incremental build.json for this source and binary')
    p.add_argument('--modes', nargs='+', choices=['dedicated', 'thread_pool'], default=['thread_pool'])
    p.add_argument('--suite', choices=['insert', 'modify'], default='insert')
    p.add_argument('--dedicated-receipt', type=Path, action='append', default=[],
                   help='Attach historical dedicated-mode receipts; never reinterpret as this run/current binary.')
    args = p.parse_args()
    root = args.workspace.resolve()
    driver = root / 'integration/opengauss-native-lake'
    sys.path.insert(0, str(driver))
    from lake_harness import (Context, checked_artifacts, execute, fingerprint, read_json,
                              run_case, sha256, validate_run_id, write_json)
    validate_run_id(args.run_id)
    if len(args.run_id) > 60 or len(args.modes) != len(set(args.modes)):
        p.error('run-id must be at most 60 characters; modes cannot repeat')
    output = args.output.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    if output.exists():
        p.error('output already exists; use a fresh evidence directory/run-id')
    runtime = root / '.runtime/opengauss-native-lake/debug'
    source = root / '.worktrees/opengauss-native-lake'
    install = root / '.install/opengauss-native-lake-debug'
    conf = runtime / 'data/postgresql.conf'
    evidence = output.parent
    cases = CASES if args.suite == 'insert' else MODIFY_CASES
    result = {'status': 'fail', 'scope': 'N3/local_insert_host_modes' if args.suite == 'insert'
              else 'N4/local_inline_modify_host_modes', 'suite': args.suite, 'run_id': args.run_id,
              'modes_requested': args.modes, 'assertions': [], 'children': [], 'artifacts': [],
              'notes': ['Correctness/lifecycle of the listed cases only; no performance, RAC, SDK or complete N3/N4 qualification.',
                        'The orchestrator lock coordinates only this script. The root operator must serialize other tests/builds.',
                        'Historical dedicated receipts are attached separately, not counted as current-binary runs.',
                        'Host migration is observed between transactions; active transactions must retain their worker.',
                        'Missing cross-transaction migration in the bounded host probe remains an open proof.']}
    original = None
    original_stat = None
    changed = False
    lock = None
    initial_runtime = None
    blocked = False
    old_env = os.environ.get('OG_LAKE_EXPECT_THREAD_POOL')
    signal_handlers = {}
    before = None
    scripts_before = None

    def assertion(name, success, details=None):
        result['assertions'].append({'name': name, 'status': 'pass' if success else 'fail', 'details': details})
        if not success:
            raise RuntimeError(name)

    def artifact(path):
        result['artifacts'].append({'path': str(Path(path).resolve()), 'sha256': sha256(path)})

    def runtime_state():
        import psycopg2
        pid = int((runtime / 'data/postmaster.pid').read_text().splitlines()[0])
        executable = Path('/proc', str(pid), 'exe')
        if executable.resolve() != install / 'bin/gaussdb':
            raise RuntimeError('runtime is not the owned project binary')
        conn = psycopg2.connect(host=str(runtime / 'socket'), port=55560, dbname='postgres',
                               user=os.getenv('USER', 'qiumc'), connect_timeout=5,
                               application_name='og_lake_pool_profile_observer')
        try:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute('SET statement_timeout=10000')
                cur.execute('SHOW data_directory'); data = str(Path(cur.fetchone()[0]).resolve())
                cur.execute('SHOW enable_thread_pool'); mode = cur.fetchone()[0]
                cur.execute('SHOW thread_pool_attr'); pool = cur.fetchone()[0]
                cur.execute('SELECT pg_postmaster_start_time()'); started = str(cur.fetchone()[0])
            if data != str(runtime / 'data'):
                raise RuntimeError('SQL reached another data directory')
            return {'pid': pid, 'postmaster_start': started, 'data': data,
                    'thread_pool': mode, 'thread_pool_attr': pool,
                    'binary_sha256': sha256(executable)}
        finally:
            conn.close()

    def script_hashes():
        files = [Path(__file__).resolve(), Path(__file__).with_name('private_write_migration.py').resolve(),
                 driver / 'source-lock.json', driver / 'state/cases.json',
                 root / 'deliverables/multimodal_optimizer/opengauss_build_install_restart.sh']
        files += sorted(driver.glob('*.py')) + sorted(driver.glob('*.sh')) + sorted((driver / 'tests').glob('*.py'))
        return {str(path): sha256(path) for path in files}

    def write_conf(content):
        # Writing in place preserves ownership/mode of the existing owned file.
        with conf.open('r+b') as stream:
            stream.seek(0); stream.write(content); stream.truncate(); stream.flush(); os.fsync(stream.fileno())

    def restart(label):
        run_id = args.run_id + '_' + label
        attempt = evidence / ('restart_' + label)
        attempt.mkdir(exist_ok=False)
        print('Restart:', label, flush=True)
        process = execute([str(driver / 'build.sh'), 'debug', 'restart', '--run-id', run_id],
                          attempt, 240, os.environ.copy())
        record = root / '公共文档/openGauss原生湖存储/验证' / run_id / 'build.json'
        if record.exists():
            artifact(record)
            build = read_json(record)
        else:
            build = {}
        result.setdefault('restarts', []).append({'label': label, 'process': process, 'receipt': str(record)})
        for path in attempt.iterdir():
            if path.is_file(): artifact(path)
        assertion(label + ' official restart and SQL passed', process['returncode'] == 0 and
                  not process['timed_out'] and not process['cancelled'] and
                  build.get('status') == 'build_and_sql_passed', process)

    def standalone(label, script, argv, mode, timeout=240):
        nonlocal blocked
        attempt = evidence / label
        attempt.mkdir(exist_ok=False)
        child_output = attempt / 'test-result.json'
        child_id = args.run_id + '_' + label
        process = execute([sys.executable, str(script), '--run-id', child_id,
                           '--output', str(child_output)] + argv, attempt, timeout, os.environ.copy())
        # A timed-out private probe may not reach its with/finally. Use only the
        # existing exact OID/owner/marker cleanup; never DROP by a name prefix.
        if (attempt / 'database-owner.json').exists():
            cleanup = execute([sys.executable, str(driver / 'tests/inline_cleanup.py'),
                               '--run-id', child_id, '--attempt', str(attempt)],
                              attempt, 40, os.environ.copy(), 'fallback-cleanup')
            assertion(label + ' ownership-verified cleanup', cleanup['returncode'] == 0 and
                      not cleanup['timed_out'] and not cleanup['cancelled'], cleanup)
        payload = read_json(child_output) if child_output.exists() else {}
        result['children'].append({'label': label, 'mode': mode, 'status': payload.get('status', 'fail'),
                                   'result': str(child_output), 'process': process})
        assertion(label + ' completed without crash/timeout', process['returncode'] in (0, 2) and
                  not process['timed_out'] and not process['cancelled'], process)
        assertions = payload.get('assertions', [])
        assertion(label + ' concrete functional assertions', bool(assertions) and
                  all(item.get('status') == 'pass' for item in assertions), payload.get('error'))
        assertion(label + ' cleanup reported no failure', not payload.get('cleanup_errors') and
                  payload.get('cleanup', {}).get('returncode', 0) == 0)
        if payload.get('status') == 'blocked':
            assertion(label + ' only missing migration observation is blocked',
                      payload.get('functional_status') == 'pass' and
                      payload.get('migration_observed_while_staged') is False)
            blocked = True
        else:
            assertion(label + ' test passed', payload.get('status') == 'pass')
        result['artifacts'].extend(checked_artifacts(payload.get('artifacts', [])))
        for path in sorted(attempt.rglob('*')):
            if path.is_file(): artifact(path)
        return payload

    def interrupt(signum, frame):
        raise KeyboardInterrupt('signal ' + str(signum))

    try:
        lock = (runtime / 'native-insert-profiles.lock').open('a+')
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        owner = read_json(runtime / 'owner.json')
        assertion('owned source/install/runtime identity', owner == {
            'project': 'opengauss-native-lake', 'source': str(source), 'data': str(runtime / 'data'),
            'install': str(install), 'profile': 'debug', 'port': 55560})
        initial_runtime = runtime_state()
        result['runtime_before'] = initial_runtime
        before = fingerprint(driver, source, [install / 'bin/gaussdb'])
        build = read_json(args.build_receipt)
        current_diff = hashlib.sha256(subprocess.check_output(['git', '-C', str(source), 'diff', 'HEAD'])).hexdigest()
        assertion('official build receipt binds current source and installed/running binary',
            build.get('mode') in ('full', 'incremental') and build.get('status') == 'build_and_sql_passed' and
            build.get('source_head') == before['source']['head'] and build.get('source_diff_sha256') == current_diff and
            build.get('binary_sha256') == initial_runtime['binary_sha256'] == sha256(install / 'bin/gaussdb') and
            build.get('source') == str(source) and build.get('install') == str(install) and
            build.get('data') == str(runtime / 'data'))
        artifact(args.build_receipt)
        result['build_receipt'] = str(args.build_receipt.resolve())
        scripts_before = script_hashes()
        result['provenance_before'] = before
        result['script_hashes_before'] = scripts_before
        for path in args.dedicated_receipt:
            receipt = read_json(path)
            assertion('historical baseline receipt passed: ' + str(path), receipt.get('status') == 'pass')
            result.setdefault('historical_dedicated_baseline', []).append({
                'path': str(path.resolve()), 'sha256': sha256(path), 'id': receipt.get('id'),
                'provenance': receipt.get('provenance'), 'runtime': receipt.get('runtime')})
        original_stat = conf.stat()
        original = conf.read_bytes()
        backup = evidence / 'postgresql.conf.original'
        with backup.open('xb') as stream:
            os.fchmod(stream.fileno(), 0o600); stream.write(original); stream.flush(); os.fsync(stream.fileno())
        artifact(backup)
        result['configuration_before'] = {'sha256': hashlib.sha256(original).hexdigest(),
            'mode': stat.S_IMODE(original_stat.st_mode), 'uid': original_stat.st_uid, 'gid': original_stat.st_gid}
        for sig in (signal.SIGINT, signal.SIGTERM):
            signal_handlers[sig] = signal.signal(sig, interrupt)
        for mode in args.modes:
            setting = 'on' if mode == 'thread_pool' else 'off'
            changed = True
            write_conf(original + ('\n# Temporary native inline lifecycle acceptance.\n'
                'enable_thread_pool = ' + setting + "\nthread_pool_attr = '4, 1, (nobind)'\n").encode())
            restart(mode)
            os.environ['OG_LAKE_EXPECT_THREAD_POOL'] = setting
            state = runtime_state()
            assertion(mode + ' actually active', state['thread_pool'] == setting, state)
            assertion(mode + ' installed binary unchanged', state['binary_sha256'] == initial_runtime['binary_sha256'])
            result.setdefault('mode_runtime', {})[mode] = state
            manifest = None
            for case_id in cases:
                assertion(mode + ' remains active before ' + case_id, runtime_state()['thread_pool'] == setting)
                print('Case:', mode, case_id, flush=True)
                child, path = run_case(case_id, Context(args.run_id + '_' + mode,
                    driver, evidence / 'children', source))
                result['children'].append({'id': case_id, 'mode': mode, 'result': str(path),
                                           'sha256': sha256(path), 'status': child['status']})
                artifact(path)
                result['artifacts'].extend(child.get('artifacts', []))
                assertion(mode + ' ' + case_id, child['status'] == 'pass', {'result': str(path)})
                if case_id == 'N3/insert_commands': manifest = child['fixture_manifest']
            host = standalone(mode + '_host_migration', driver / 'tests/host_concurrency.py', [], mode)
            assertion(mode + ' host probe used requested mode', host.get('thread_pool_mode') == setting)
            if mode == 'thread_pool':
                if host.get('thread_migration_observed') is not True:
                    blocked = True
                    result.setdefault('open_proofs', []).append('host thread migration not observed in bounded stress')
                probe = standalone(mode + '_private_migration', Path(__file__).with_name('private_write_migration.py'),
                    ['--workspace', str(root), '--manifest', manifest], mode)
                if probe.get('status') == 'blocked':
                    result.setdefault('open_proofs', []).append(probe['reason'])
            assertion(mode + ' runtime survived all cases', runtime_state()['pid'] == state['pid'])
        result['status'] = 'blocked' if blocked else 'pass'
    except BaseException as exc:
        result.update(status='fail', error=repr(exc), traceback=traceback.format_exc())
    finally:
        # Bounded restoration must not be skipped by a second Ctrl-C/SIGTERM.
        for sig in signal_handlers:
            signal.signal(sig, signal.SIG_IGN)
        if changed and original is not None:
            try:
                write_conf(original)
                assertion('original configuration bytes restored', conf.read_bytes() == original)
                current_stat = conf.stat()
                assertion('configuration ownership/mode preserved',
                    (current_stat.st_uid, current_stat.st_gid, stat.S_IMODE(current_stat.st_mode)) ==
                    (original_stat.st_uid, original_stat.st_gid, stat.S_IMODE(original_stat.st_mode)))
                restart('restore')
                restored = runtime_state()
                result['runtime_restored'] = restored
                assertion('original effective configuration restored',
                    restored['thread_pool'] == initial_runtime['thread_pool'] and
                    restored['thread_pool_attr'] == initial_runtime['thread_pool_attr'])
                assertion('restored runtime binary unchanged', restored['binary_sha256'] == initial_runtime['binary_sha256'])
                assertion('configuration remains byte-exact after restart', conf.read_bytes() == original)
            except BaseException as exc:
                result.update(status='fail', restore_error=repr(exc))
        if old_env is None: os.environ.pop('OG_LAKE_EXPECT_THREAD_POOL', None)
        else: os.environ['OG_LAKE_EXPECT_THREAD_POOL'] = old_env
        # Include partial stdout/results even when an assertion or cleanup failed
        # before the child reached its normal evidence-return path.
        try:
            known_paths = {item['path'] for item in result['artifacts']}
            for path in sorted(evidence.rglob('*')):
                if path.is_file() and path != output and str(path.resolve()) not in known_paths:
                    artifact(path)
        except BaseException as exc:
            result.update(status='fail', artifact_collection_error=repr(exc))
        if before is not None and scripts_before is not None:
            try:
                result['provenance_after'] = fingerprint(driver, source, [install / 'bin/gaussdb'])
                result['script_hashes_after'] = script_hashes()
                assertion('source/lock/binary frozen for the complete suite', result['provenance_after'] == before)
                assertion('all suite scripts/helpers frozen', result['script_hashes_after'] == scripts_before)
                # Recheck hashes of nested receipts after all cleanup. A changed
                # artifact cannot silently retain an old passing outer result.
                for item in result['artifacts']:
                    if sha256(item['path']) != item['sha256']:
                        raise RuntimeError('nested artifact changed: ' + item['path'])
            except BaseException as exc:
                result.update(status='fail', provenance_error=repr(exc))
        if lock is not None:
            lock.close()
        write_json(output, result)
        for sig, handler in signal_handlers.items(): signal.signal(sig, handler)
        print(json.dumps({'status': result['status'], 'result': str(output)}), flush=True)
    return {'pass': 0, 'blocked': 2}.get(result['status'], 1)


if __name__ == '__main__':
    raise SystemExit(main())
