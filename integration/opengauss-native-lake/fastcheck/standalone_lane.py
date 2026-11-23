"""Run the published native lake scripts outside the formal case registry."""

from contextlib import nullcontext
from copy import copy
from hashlib import sha256
import json
from pathlib import Path
import subprocess
from uuid import uuid4


CASES = ('standalone', 'standalone_smoke')
CATALOGUE = Path(__file__).with_name('standalone_cases.json')
EVIDENCE = '公共文档/openGauss原生湖存储/验证'
PARTIAL_OPTIONS = {
    'file_dml_acceptance': '--only',
    'run_native_index_store': '--only',
    'run_scalar_file_query': '--build-only',
}


class Blocked(ValueError):
    """A driver cannot be run with the available configuration."""


def expand(value, templates):
    for token, replacement in templates.items():
        value = value.replace('{' + token + '}', str(replacement))
    return value


def plans(case, ctx, catalogue):
    if case == 'standalone_smoke':
        return {'reference_flush_history_oracle': [{
            'argv': ['--run-id', '{run_id}'],
            'result': '{workspace}/' + EVIDENCE + '/{run_id}/result.json',
        }]}
    options = ctx.config.get('standalone', {})
    configured = options.get('cases', {}) if isinstance(options, dict) else None
    if not isinstance(configured, dict):
        raise Blocked('standalone.cases must be an object keyed by driver name')
    unknown = sorted(configured.keys() - set(catalogue['test_scripts']))
    if unknown:
        raise Blocked('Unknown standalone scripts: ' + ', '.join(unknown))
    return {name: configured.get(name) for name in catalogue['test_scripts']}


def native_receipt(result, expected, row):
    if result.is_symlink() or result.resolve() != result or not result.is_file():
        raise RuntimeError(f'No fresh native result at {result}')
    data = result.read_bytes()
    row['receipt_sha256'] = sha256(data).hexdigest()
    receipt = json.loads(data)
    row['native_status'] = receipt.get('status') if isinstance(receipt, dict) else None
    if row['native_status'] in ('blocked', 'prepared_not_executed', 'skipped'):
        raise Blocked(f'Native result reported {row["native_status"]!r}: {result}')
    if row['native_status'] != expected:
        raise RuntimeError(f'Expected native status {expected!r}, got {row["native_status"]!r}: {result}')
    for field in ('assertions', 'cases'):
        if isinstance(receipt.get(field), list):
            row[field] = len(receipt[field])


def run_job(ctx, name, number, job, catalogue, row):
    if not isinstance(job, dict) or set(job) - {'argv', 'result', 'profile', 'timeout'}:
        raise Blocked('Each standalone job must specify argv and result with optional profile and timeout')
    argv = job.get('argv')
    if not isinstance(argv, list) or any(not isinstance(arg, str) for arg in argv):
        raise Blocked('argv must be an array of command-line arguments')
    if any(arg.startswith('--_') for arg in argv):
        raise Blocked('Use the public entry point of the native standalone driver')
    partial = PARTIAL_OPTIONS.get(name)
    if partial and any(arg.startswith('--') and partial.startswith(arg.split('=', 1)[0])
                       for arg in argv):
        raise Blocked(f'Omit {partial} to run the entire native driver')
    if name in catalogue['explicit_execute'] and '--execute' not in argv:
        raise Blocked('This driver requires --execute to run its assertions')
    if not isinstance(job.get('result'), str) or not job['result']:
        raise Blocked('Specify the native JSON result path')
    profile_name = job.get('profile')
    if profile_name not in (None, 'debug', 'release', 'ann-debug'):
        raise Blocked('profile must be debug, release, or ann-debug when specified')
    timeout = job.get('timeout')
    if timeout is not None and (type(timeout) is not int or timeout <= 0):
        raise Blocked('timeout must be a positive number of seconds')

    job_ctx = copy(ctx)
    job_ctx.out = ctx.out / 'standalone' / name / str(number)
    run_id = 'fastcheck_' + uuid4().hex[:16]
    templates = {'run_id': run_id, 'workspace': ctx.workspace, 'source': ctx.source,
                 'out': job_ctx.out, 'driver': ctx.driver,
                 'config': ctx.config_path.parent}
    args = [expand(arg, templates) for arg in argv]
    result = ctx.path(expand(job['result'], templates))
    if name not in catalogue['explicit_receipts']:
        filename = 'index.json' if name in catalogue['index_receipts'] else 'result.json'
        if result.name != filename:
            raise Blocked(f'The native driver writes its terminal result to {filename}')
    expected_status = 'passed' if name in catalogue['passed_receipts'] else 'pass'
    legacy_output = (ctx.workspace / EVIDENCE).resolve()
    local_result = result.is_relative_to(job_ctx.out)
    isolated_legacy_result = (
        result.is_relative_to(legacy_output)
        and any(run_id in part for part in result.relative_to(legacy_output).parts[:-1])
    )
    if not local_result and not isolated_legacy_result:
        raise Blocked('Native result must be under {out} or the workspace evidence directory with {run_id}')
    if result.exists() or result.is_symlink():
        raise Blocked(f'Native output already exists: {result}')
    row.update(run_id=run_id, receipt=str(result))

    script = ctx.driver / 'tests' / f'{name}.py'
    original = ctx.repo_driver / 'tests' / script.name
    if (script.is_symlink() or not script.is_file() or not original.is_file()
            or script.read_bytes() != original.read_bytes()):
        raise Blocked(f'Workspace driver differs from the published test: {script}')
    job_ctx.out.mkdir(parents=True)
    if profile_name:
        from suites_lane import profile
        runtime = profile(job_ctx, profile_name)
    else:
        runtime = nullcontext()
    with runtime as env:
        try:
            job_ctx.run([ctx.python, script, *args], name, env=env, timeout=timeout)
        except subprocess.CalledProcessError as exc:
            row['returncode'] = exc.returncode
            if result.exists():
                native_receipt(result, expected_status, row)
            raise
        row['returncode'] = 0
        native_receipt(result, expected_status, row)


def run(case, ctx):
    catalogue = json.loads(CATALOGUE.read_text())
    if catalogue['schema_version'] != 1:
        raise RuntimeError('Unsupported standalone receipt catalogue version')
    reports = []
    for name, jobs in plans(case, ctx, catalogue).items():
        if not isinstance(jobs, list) or not jobs:
            reports.append(dict(script=name, status='blocked',
                                reason=f'Configure standalone.cases.{name} as a nonempty list'))
            continue
        for number, job in enumerate(jobs, 1):
            row = dict(script=name, job=number)
            reports.append(row)
            try:
                run_job(ctx, name, number, job, catalogue, row)
            except Blocked as exc:
                row.update(status='blocked', reason=str(exc))
            except Exception as exc:
                row.update(status='failed', reason=str(exc))
            else:
                row['status'] = 'passed'

    counts = {status: sum(row['status'] == status for row in reports)
              for status in ('passed', 'failed', 'blocked')}
    index = ctx.out / 'standalone.json'
    status = 'passed' if not (counts['failed'] or counts['blocked']) else 'failed'
    summary = dict(status=status, counts=counts, scripts=len({row['script'] for row in reports}),
                   receipt=str(index))
    index.write_text(json.dumps(dict(summary, jobs=reports), indent=2, ensure_ascii=False) + '\n')
    if status == 'failed':
        raise RuntimeError(f'Standalone jobs failed={counts["failed"]}, blocked={counts["blocked"]}; {index}')
    return summary
