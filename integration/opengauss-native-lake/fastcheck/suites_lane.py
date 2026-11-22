"""Schedule the published SQL056–074 entrypoints through their original drivers."""

import contextlib
import hashlib
import json
import os
import re
import shutil
import stat
import subprocess
import uuid
from pathlib import Path

CASES = ('main_debug', 'main_release', 'compat', 'debug_history',
         'release_campaign', 'release_truncate_pq', 'extra_perf', 'scale')
PORTS = {'debug': 55560, 'ann-debug': 55580, 'release': 55570}
SDK_FILES = {'SDK54_SO': 'og_lake_sdk', 'SDK6_SO': 'og_lake_external_sdk'}
IMMUTABLES = set(SDK_FILES) | {'DEBUG_CONF', 'ANN_DEBUG_CONF'}
DATAVEC = {'diskann_vector', 'hnsw_lsg_vector', 'hnsw_rabitq_vector'}
HISTORY_BASE = ({f'run_{n:03d}_suite.py' for n in range(56, 71)} -
                {'run_060_suite.py'}) | {'run_060_manual'}
RELEASE_HISTORY = ({f'run_{n:03d}_suite.py' for n in range(56, 67)} -
                   {'run_060_suite.py'}) | {
    'run_069_suite.py', 'ix5_064:run_064_suite.py',
    'm067:run_067_suite.py', 'm068:run_068_suite.py', 'm070:run_070_suite.py',
}


def _sha(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as f:
        for block in iter(lambda: f.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def _read(path):
    return json.loads(Path(path).read_text(encoding='utf-8'))


def _check_sha(path, expected):
    if not isinstance(expected, str) or not re.fullmatch(r'[0-9a-f]{64}', expected):
        raise ValueError(f'Expected a recorded SHA256 for {path}')
    if _sha(path) != expected:
        raise RuntimeError(f'Input changed or build identity mismatch: {path}')


def _settings(ctx):
    settings = ctx.config.get('suites', {})
    if not isinstance(settings, dict):
        raise ValueError('suites configuration must be an object')
    return settings


def _require(ctx, key):
    try:
        return _settings(ctx)[key]
    except KeyError as exc:
        raise ValueError(f'Missing suites.{key} configuration') from exc


def _immutable(ctx):
    path = Path(ctx.path(_require(ctx, 'immutable_manifest'))).resolve(strict=True)
    manifest = _read(path)
    if set(manifest) != IMMUTABLES:
        raise ValueError(f'Expected the four original immutable identities in {path}')
    return path, manifest


def _run(ctx, script, args, label, *, env, timeout):
    return ctx.run([ctx.python, str(script), *map(str, args)], label,
                   cwd=ctx.workspace, env=env, timeout=timeout)


class _Runtime:
    def __init__(self, ctx, name, immutable):
        self.ctx = ctx
        self.name = name
        install_profile = 'debug' if name == 'ann-debug' else name
        self.receipt = Path(ctx.path(_require(ctx, f'{install_profile}_build_receipt'))).resolve(strict=True)
        self.build = _read(self.receipt)
        if self.build.get('status') != 'build_and_sql_passed' or self.build.get('profile') != install_profile:
            raise RuntimeError(f'Need a qualified {install_profile} build receipt: {self.receipt}')
        self.source = Path(self.build['source']).resolve(strict=True)
        self.install = ctx.workspace / '.install' / f'opengauss-native-lake-{install_profile}'
        self.runtime = ctx.workspace / '.runtime/opengauss-native-lake' / name
        self.data = self.runtime / 'data'
        self.pidfile = self.data / 'postmaster.pid'
        self.conf = self.data / 'postgresql.conf'
        self.binary = self.install / 'bin/gaussdb'
        self.port = PORTS[name]
        owner = {
            'project': 'opengauss-native-lake', 'source': str(self.source),
            'data': str(self.data), 'install': str(self.install),
            'profile': name, 'port': self.port,
        }
        if Path(self.build['install']).resolve() != self.install.resolve():
            raise RuntimeError(f'Build receipt does not own {self.install}')
        if _read(self.runtime / 'owner.json') != owner:
            raise RuntimeError(f'Refuse to use a runtime without the matching owner: {self.runtime}')
        if self.pidfile.exists():
            raise RuntimeError(f'Refuse to take over an existing instance: {self.pidfile}')
        if name == 'ann-debug':
            head = subprocess.check_output(['git', '-C', str(self.source), 'rev-parse', 'HEAD'],
                                           text=True).strip()
            if self.build.get('source_head') != head:
                raise RuntimeError(f'ann-debug requires a Debug build receipt from source HEAD {head}')
        self.entry = Path(self.build['build_entry']).resolve(strict=True)
        _check_sha(self.entry, self.build['build_entry_sha256'])
        self.expected = {self.binary: self.build['binary_sha256']}
        if self.conf.is_symlink() or self.data.is_symlink() or self.runtime.is_symlink():
            raise RuntimeError(f'Refuse a managed runtime with symlinked PGDATA or configuration: {self.runtime}')
        self.expected[self.conf] = (_require(ctx, 'release_conf_sha256') if name == 'release' else
                                    immutable['DEBUG_CONF' if name == 'debug' else 'ANN_DEBUG_CONF'])
        self.expected.update({
            self.install / 'lib/postgresql' / folder / 'libog_lake_sdk_arrow.so': immutable[key]
            for key, folder in SDK_FILES.items()
        })
        for path, expected in self.expected.items():
            _check_sha(path, expected)
        # The providers are built separately and checked above by installed library SHA.
        product_inputs = self.build['native_source_inputs_after']
        recorded_inputs = {
            path: digest for path, digest in product_inputs.items()
            if path.startswith(('src/', 'contrib/')) and not path.startswith(
                ('src/test/', 'contrib/og_lake/sdk/', 'contrib/og_lake/sdk_external/'))
        }
        if not recorded_inputs:
            raise RuntimeError(f'Missing native build input evidence: {self.receipt}')
        for path, expected in recorded_inputs.items():
            build_input = (self.source / path).resolve(strict=True)
            if not build_input.is_relative_to(self.source):
                raise RuntimeError(f'Build receipt input is outside its source: {build_input}')
            _check_sha(build_input, expected)
        ctx.runtimes.append({
            'profile': name, 'build_receipt': str(self.receipt),
            'source': str(self.source), 'source_head_at_build': self.build['source_head'],
            'source_diff_sha256_at_build': self.build['source_diff_sha256'],
            'binary_sha256': self.expected[self.binary],
            'config_sha256': self.expected[self.conf],
            'sdk54_sha256': immutable['SDK54_SO'], 'sdk6_sha256': immutable['SDK6_SO'],
        })
        self.original_conf = self.conf.read_bytes()
        self.conf_mode = stat.S_IMODE(self.conf.stat().st_mode)

    def environment(self):
        env = {
            k: v for k, v in self.ctx.env.items()
            if not k.startswith(('PG', 'OG_LAKE_TEST_HOST', 'OG_LAKE_TEST_PORT'))
            and k != 'OG_LAKE_060_ISOLATED_RUNTIME'
        }
        user = self.ctx.env['PGUSER']
        return dict(env, CPP_WORKSPACE=str(self.ctx.workspace), PGUSER=user, USER=user,
                    OG_LAKE_TEST_USER=user, OG_LAKE_SOURCE=str(self.source),
                    OG_LAKE_TEST_PROFILE=self.name, OG_LAKE_TEST_HOST=str(self.runtime / 'socket'),
                    OG_LAKE_TEST_PORT=str(self.port))

    def dependencies(self):
        roaring = self.build['roaring_inputs_after']
        pq = self.build['pq_inputs_after']
        args = ['--roaring-build', roaring['receipt']]
        if pq.get('dependency_kind') == 'pq_in_tree_v1':
            blas = pq['blas_inputs']
            args += ['--pq-in-tree', '--blas-build', blas['receipt'],
                     '--blas-build-sha256', blas['receipt_sha256']]
        else:
            selection = pq['decimal54_selection']
            args += ['--pq-build', pq['receipt'], '--pq-sdk-selection', selection['path'],
                     '--pq-sdk-selection-sha256', selection['sha256']]
        return args

    def build_command(self, action):
        args = [self.name, action, '--run-id',
                f'fastcheck_{self.name}_{action}_{uuid.uuid4().hex[:12]}',
                '--build-entry', self.entry, *self.dependencies()]
        if self.name == 'ann-debug' and action == 'restart':
            args += ['--reuse-build-receipt', self.receipt]
        _run(self.ctx, self.ctx.driver / 'build_driver.py', args, f'{self.name}_{action}',
             env=self.environment(), timeout=300)

    def verify_started(self):
        if not self.pidfile.is_file():
            raise RuntimeError(f'Missing postmaster PID for {self.runtime}')
        pid = int(self.pidfile.read_text().splitlines()[0])
        proc = Path(f'/proc/{pid}')
        executable = (proc / 'exe').resolve(strict=True)
        argv = (proc / 'cmdline').read_bytes().split(b'\0')
        pgdata = os.fsencode(self.data.resolve())
        if (executable != self.binary.resolve(strict=True) or
                not any(a == b'-D' and b == pgdata for a, b in zip(argv, argv[1:]))):
            raise RuntimeError(f'Refuse to manage a different postmaster PID {pid}: {self.runtime}')
        return pid

    def finish(self):
        # A pre-existing PID is refused at admission. Only stop instances
        # with this job's exact owned binary and PGDATA.
        if self.pidfile.exists():
            self.verify_started()
            self.build_command('stop')
        if self.pidfile.exists():
            raise RuntimeError(f'Postmaster PID still present after cleanup: {self.pidfile}')
        if self.conf.read_bytes() != self.original_conf:
            self.conf.write_bytes(self.original_conf)
            self.conf.chmod(self.conf_mode)
            raise RuntimeError(f'Profile did not restore its configuration; restored {self.conf}')
        if stat.S_IMODE(self.conf.stat().st_mode) != self.conf_mode:
            self.conf.chmod(self.conf_mode)
            raise RuntimeError(f'Profile changed configuration mode; restored {self.conf}')
        for path, expected in self.expected.items():
            _check_sha(path, expected)


@contextlib.contextmanager
def _owned(ctx, *profiles):
    path, immutable = _immutable(ctx)
    runtimes = [_Runtime(ctx, name, immutable) for name in profiles]
    try:
        yield path, immutable, runtimes
    finally:
        errs = []
        for item in reversed(runtimes):
            try:
                item.finish()
            except (OSError, RuntimeError, ValueError, subprocess.SubprocessError) as exc:
                errs.append(str(exc))
        if errs:
            raise RuntimeError('; '.join(errs))


@contextlib.contextmanager
def profile(ctx, name):
    """Run one owned native test instance and return its explicit connection env."""
    with _owned(ctx, name) as (_, _, [runtime]):
        runtime.build_command('restart')
        runtime.verify_started()
        yield runtime.environment()


def _require_quiet_hosts():
    running = subprocess.run(['pgrep', '-x', 'gaussdb'], capture_output=True, check=False, text=True)
    if running.returncode != 1:
        raise RuntimeError(f'Original exclusive-host campaign requires no live gaussdb: {running.stdout.strip()}')


def _result(path, status):
    result = _read(path)
    if result.get('status') != status:
        raise RuntimeError(f'Original suite did not pass: {path}')
    return result


def _campaign_id():
    return 'fastcheck_' + uuid.uuid4().hex


def _run_main(ctx, directory, name):
    result = ctx.out / 'main.json'
    args = ['--profile', name, '--mode', 'full', '--out', result]
    if name == 'debug':
        timeout = _settings(ctx).get('debug_index_fixture_timeout_ms', 300000)
        if not isinstance(timeout, int) or not 1 <= timeout <= 2147483647:
            raise ValueError('Invalid suites.debug_index_fixture_timeout_ms')
        args += ['--index-fixture-timeout-ms', timeout]
    with profile(ctx, name) as env:
        _run(ctx, directory / 'verify_074_suite.py', args, f'main_{name}', env=env,
             timeout=4 * 3600)
    results = _read(result)
    if set(results) != {'ann', 'active_h', 'pgl0', 'files'} or any(
            item.get('status') != 'passed' for item in results.values()):
        raise RuntimeError('Incomplete final SQL074 main verification')
    ann = results['ann']['cases']
    observed_dims = {item.get('dim') for item in ann}
    if (observed_dims != {32, 128, 384, 768, 1536} or len(ann) != 6 or
            sum(item.get('metric') == 'ip_raw_cached' and item.get('dim') == 128 for item in ann) != 1):
        raise RuntimeError(f'Incomplete SQL074 ANN dimension or metric coverage: {observed_dims}')
    return {'profile': name, 'receipt': str(result), 'ann_dimensions': sorted(observed_dims),
            'ann_checks': len(ann)}


def _run_release_case(ctx, directory, case):
    out = ctx.out / ('truncate_pq.json' if case == 'release_truncate_pq' else 'index.json')
    if case == 'release_truncate_pq':
        script = directory / 'run_074_compat/run_074_truncate_pq_suite.py'
        args = ['--profile', 'release', '--out', out]
    else:
        script = directory / 'run_074_extra_perf_bench.py'
        args = ['--profile', 'release', '--out-dir', ctx.out]
    with profile(ctx, 'release') as env:
        _run(ctx, script, args, case, env=env, timeout=1800)
    result = _result(out, 'passed')
    summary = {'profile': 'release', 'receipt': str(out)}
    if case == 'release_truncate_pq':
        if result['total_checks'] <= 0:
            raise RuntimeError('Missing Release truncate/PQ checks')
        summary['total_checks'] = result['total_checks']
    else:
        for key in ('vec_vs_row_60k', 'diskann_pq_8000_4x2000_dim32'):
            if not result[key]:
                raise RuntimeError(f'Missing extra Release performance evidence: {key}')
    return summary


def _verify_debug_results(case, result):
    if case == 'compat':
        groups = result['runs']
        if set(groups) != {'debug_pool_off', 'debug_pool_on', 'anndebug_pool_off'}:
            raise RuntimeError('Incomplete compatibility profile matrix')
        names = {'trunc_pq_073', 'vec_smp_072', 'smp_closure', 'json_071'}
        for row in groups.values():
            if any(row[name].get('status') != 'passed' or row[name]['total_checks'] <= 0
                   for name in names):
                raise RuntimeError('Incomplete original compatibility checks')
    else:
        matrix = result['suites_per_target']
        expected = {
            'debug_pool_off': HISTORY_BASE | {'run_064_ix5'},
            'debug_pool_on': HISTORY_BASE | {'run_060_full_worker'},
            'ann-debug_pool_off': HISTORY_BASE | {'run_064_ix5', 'datavec_regress'},
        }
        if {name: set(suites) for name, suites in matrix.items()} != expected:
            raise RuntimeError('Incomplete SQL056–070 Debug replay matrix')


def _run_debug_campaign(ctx, directory, case):
    if case == 'debug_history':
        _require_quiet_hosts()
        release_pid = ctx.workspace / '.runtime/opengauss-native-lake/release/data/postmaster.pid'
        if release_pid.exists():
            raise RuntimeError(f'Refuse original history pre-stop with existing {release_pid}')
    runid = _campaign_id()
    artifact = ctx.workspace / '公共文档/openGauss原生湖存储/验证' / runid
    (ctx.out / 'campaign_location.txt').write_text(str(artifact) + '\n', encoding='utf-8')
    with _owned(ctx, 'debug', 'ann-debug') as (manifest_path, immutable, runtimes):
        dbg = runtimes[0]
        args = ['--workspace', ctx.workspace, '--user', ctx.env['PGUSER'],
                '--build-entry', dbg.entry, '--build-receipt', dbg.receipt, '--campaign-id', runid]
        if case == 'compat':
            script = 'run_074_compat_campaign.py'
            args += ['--immutable-manifest', manifest_path]
            status = 'passed_SQL074_COMPAT_SQL073_TRUNC_PQVEC_SQL072_N7_SMP_SQL071'
        else:
            script = 'run_074_debug_history_campaign.py'
            deps = dbg.dependencies()
            if '--pq-in-tree' not in deps:
                raise RuntimeError('Original Debug history requires the in-tree PQ profile')
            args += ['--source', dbg.source, '--expected-sdk54', immutable['SDK54_SO'],
                     '--expected-sdk6', immutable['SDK6_SO'], '--expected-debug-conf',
                     immutable['DEBUG_CONF'], '--expected-ann-debug-conf', immutable['ANN_DEBUG_CONF'],
                     *(arg for arg in deps if arg != '--pq-in-tree')]
            status = 'passed_SQL056_TO_SQL070_AND_DATAVEC_REPLAY_ON_SQL074'
        _run(ctx, directory / script, args, case, env=dbg.environment(), timeout=4 * 3600)
    receipt = artifact / 'index.json'
    result = _result(receipt, status)
    _verify_debug_results(case, result)
    # Preserve per-suite JSON, fixture evidence and logs beside FastCheck's own output.
    shutil.copytree(artifact, ctx.out / 'campaign', symlinks=True)
    summary = {'receipt': str(ctx.out / 'campaign/index.json'), 'campaign_dir': str(artifact)}
    if case == 'compat':
        summary['total_checks'] = result['grand_total_checks']
    else:
        summary['suite_invocations'] = {
            name: len(suites) - ('datavec_regress' in suites)
            for name, suites in result['suites_per_target'].items()
        }
        summary['datavec_cases'] = sorted(DATAVEC)
    return summary


def _run_release_campaign(ctx, directory, case):
    if case == 'scale':
        _require_quiet_hosts()
    with _owned(ctx, 'release') as (_, immutable, [release]):
        args = ['--build-receipt', release.receipt, '--build-entry', release.entry, '--out-dir', ctx.out]
        if case == 'release_campaign':
            script = 'run_074_release_campaign.py'
            args += ['--workspace', ctx.workspace, '--user', ctx.env['PGUSER'],
                     '--expected-sdk54-sha256', immutable['SDK54_SO'],
                     '--expected-sdk6-sha256', immutable['SDK6_SO']]
            status = 'passed_release_full'
        else:
            script = 'run_074_compat/run_074_scale_soak_and_datavec.py'
            deps = release.dependencies()
            if '--pq-in-tree' not in deps:
                raise RuntimeError('Original Release scale requires the in-tree PQ profile')
            args += ['--source', release.source, '--release-conf-sha256', release.expected[release.conf],
                     '--sdk54-sha256', immutable['SDK54_SO'], '--sdk6-sha256', immutable['SDK6_SO'],
                     *(arg for arg in deps if arg != '--pq-in-tree')]
            status = 'passed'
        _run(ctx, directory / script, args, case, env=release.environment(), timeout=4 * 3600)
    out = ctx.out / 'index.json'
    result = _result(out, status)
    if case == 'release_campaign':
        checks = {'release_070_ops_alter', 'release_vec_smp_072', 'release_071_json',
                  'release_smp_closure', 'release_perf'}
        if any(result[key].get('status') not in ('passed', 'pass') for key in checks):
            raise RuntimeError('Incomplete original Release compatibility/performance checks')
    else:
        if set(result['history_suites_on_release']) != RELEASE_HISTORY or set(result['datavec_regress']) != DATAVEC:
            raise RuntimeError('Incomplete original Release history or native datavec results')
        for key in ('scale_1m_rows', 'diskann_pq_8k_dim32', 'diskann_pq_20k_dim32', 'diskann_pq_50k_dim32',
                    'threadpool_soak_and_lru'):
            if not result[key]:
                raise RuntimeError(f'Missing original scale/soak evidence: {key}')
    return {'receipt': str(out), 'profile': 'release'}


def run(case, ctx):
    """Run the original archived case or campaign; incomplete evidence fails."""
    directory = ctx.repo_driver / 'suites_056_074'
    if case in ('main_debug', 'main_release'):
        return _run_main(ctx, directory, case.split('_', 1)[1])
    if case in ('release_truncate_pq', 'extra_perf'):
        return _run_release_case(ctx, directory, case)
    if case in ('compat', 'debug_history'):
        return _run_debug_campaign(ctx, directory, case)
    if case in ('release_campaign', 'scale'):
        return _run_release_campaign(ctx, directory, case)
    raise ValueError(f'Unknown archived suite case: {case}')
