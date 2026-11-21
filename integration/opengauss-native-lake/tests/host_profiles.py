#!/usr/bin/env python3
"""Exercise host state in dedicated-thread and native thread-pool modes.

Only the owned native-lake debug instance is restarted. Exact configuration is
restored even on failure; this is not a performance benchmark.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--run-id', default=os.getenv('OG_LAKE_RUN_ID'))
    parser.add_argument('--output', default=os.getenv('OG_LAKE_RESULT_FILE'))
    args = parser.parse_args()
    if not args.run_id or not args.output:
        parser.error('--run-id and --output (or runner environment) required')
    root = Path(__file__).resolve().parents[3]
    driver = root / 'integration/opengauss-native-lake'
    runtime = root / '.runtime/opengauss-native-lake/debug'
    conf = runtime / 'data/postgresql.conf'
    output = Path(args.output).resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    evidence = output.parent
    result = {'status': 'fail', 'scope': 'ABI-01/local_host_modes',
              'assertions': [], 'artifacts': []}
    original = None
    original_digest = None
    def assertion(name, value, details=None):
        result['assertions'].append({'name': name, 'status': 'pass' if value else 'fail', 'details': details})
        if not value:
            raise RuntimeError(name)
    def restart(suffix):
        command = [str(driver / 'build.sh'), 'debug', 'restart', '--run-id', args.run_id + suffix]
        log = evidence / (suffix.strip('_') + '.log')
        with log.open('w') as stream:
            completed = subprocess.run(command, stdout=stream, stderr=subprocess.STDOUT, timeout=180)
        result['artifacts'].append({'path': str(log)})
        if completed.returncode:
            raise RuntimeError(f'restart failed: {log}')
    try:
        owner = json.loads((runtime / 'owner.json').read_text())
        assertion('owned project instance', owner['project'] == 'opengauss-native-lake' and
                  owner['profile'] == 'debug' and owner['data'] == str(runtime / 'data'))
        original = conf.read_bytes()
        original_digest = hashlib.sha256(original).hexdigest()
        for mode, setting in [('dedicated', 'off'), ('thread_pool', 'on')]:
            conf.write_bytes(original + (
                f"\n# Temporary native lake lifecycle acceptance.\nenable_thread_pool = {setting}\n"
                "thread_pool_attr = '4, 1, (nobind)'\n").encode())
            restart('_' + mode + '_restart')
            env = os.environ.copy()
            env.update(OG_LAKE_EXPECT_THREAD_POOL=setting)
            for label, script in [('lifecycle', 'host_smoke.py'), ('concurrency', 'host_concurrency.py')]:
                child_name = mode + '_' + label
                # Child tests use fixed artifact basenames. Give each child
                # its own directory so a later mode cannot overwrite evidence.
                child_dir = evidence / child_name
                child_dir.mkdir()
                child_output = child_dir / 'test-result.json'
                child = subprocess.run([sys.executable, str(driver / 'tests' / script),
                                        '--run-id', args.run_id + '_' + child_name,
                                        '--output', str(child_output)], env=env, timeout=180,
                                       capture_output=True, text=True)
                child_log = evidence / (child_name + '.stdout')
                child_log.write_text(child.stdout + child.stderr)
                child_result = json.loads(child_output.read_text()) if child_output.exists() else {}
                result['artifacts'].extend([{'path': str(child_output)}, {'path': str(child_log)}])
                # Bind nested audit artifacts too, so changing a stress event
                # log invalidates the outer acceptance record.
                result['artifacts'].extend(child_result.get('artifacts', []))
                assertions = child_result.get('assertions', [])
                assertion(child_name + ' real sessions', child.returncode == 0 and
                          child_result.get('status') == 'pass' and bool(assertions) and
                          all(x.get('status') == 'pass' for x in assertions),
                          {'result': str(child_output), 'exit': child.returncode})
        result['status'] = 'pass'
    except (OSError, ValueError, KeyError, RuntimeError, subprocess.TimeoutExpired) as exc:
        result['error'] = str(exc)
        if not result['assertions'] or result['assertions'][-1]['status'] != 'fail':
            result['assertions'].append({'name': 'profile execution', 'status': 'fail', 'details': str(exc)})
    finally:
        if original is not None:
            try:
                conf.write_bytes(original)
                restart('_restore')
                assertion('configuration restored exactly', hashlib.sha256(conf.read_bytes()).hexdigest() == original_digest)
            except Exception as exc:
                result['status'] = 'fail'
                result['restore_error'] = str(exc)
        output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n')
        print(json.dumps({'status': result['status'], 'result': str(output)}))
    return 0 if result['status'] == 'pass' else 1


if __name__ == '__main__':
    sys.exit(main())
