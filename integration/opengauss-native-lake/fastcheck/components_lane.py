"""Run lake SDK and native component checks outside the SQL server."""

import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import sys
import tempfile
import uuid

DRIVER = Path('integration/opengauss-native-lake')
EVIDENCE = Path('公共文档/openGauss原生湖存储/验证')
DUCKDB = Path('.reference/opengauss-native-lake/build/duckdb-gcc10/duckdb')
ARROW = Path('.reference/opengauss-native-lake/sdk/install/arrow-20.0.0')
GCC = Path('binarylibs/buildtools/gcc10.3/gcc')
OPENSSL = Path('binarylibs/kernel/dependency/openssl/comm')

SDK54 = {
    'sdk54_reader': 'reader',
    'sdk54_reader_budget': 'reader_budget',
    'sdk54_writer': 'writer',
    'sdk54_writer_faults': 'writer_faults',
    'sdk54_codecs': 'codecs',
    'sdk54_delete': 'delete',
    'sdk54_ducklake_files': 'ducklake_files',
    'sdk54_object_version': 'object_version',
    'sdk54_versioned_files': 'versioned_files',
    'sdk54_candidate': 'candidate',
}
READER_FIXTURE = ('writer', 'writer_faults', 'codecs', 'delete')
SDK54_SCRIPT = {'versioned_files': 'check_versioned_files.py', 'candidate': 'qualify_candidate.py'}
BITMAP = {'bitmap_codec': 'run_bitmap_codec.py', 'bitmap_allocator': 'run_bitmap_codec_alloc.py'}

# Original archived component tests consume specific historical baselines and build
# receipts. Keep their entrypoints distinct from the checks of current native code.
LEGACY = {
    'sdk54_binary': ('sdk/test_binary.py', ('provider_build', 'source'), ()),
    'sdk54_floats': ('sdk/test_floats.py', ('provider_build', 'source', 'old_provider'), ()),
    'sdk54_lists': ('sdk/test_lists.py',
                    ('provider_build', 'source', 'old_provider', 'legacy_input'), ('prefix_provider',)),
    'sdk020_binary': ('sdk/sql020_qa/test_binary.py', ('provider_build', 'source', 'old_provider'), ()),
    'sdk020_floats': ('sdk/sql020_qa/test_floats.py',
                     ('provider_build', 'source', 'old_provider', 'legacy_source'), ()),
    'sdk020_lists': ('sdk/sql020_qa/test_lists.py',
                    ('provider_build', 'source', 'old_provider', 'legacy_input', 'legacy_source'), ('prefix_provider',)),
    'sdk020_qualification': ('sdk/qualify_sql020_provider.py', ('provider_build', 'source', 'legacy_source'), ()),
    'pq_cpu': ('indexes/pq_cpu_component.py', (), ()),
    'pq_cpu_locking': ('indexes/pq_cpu_locking_thread_component.py', ('blas_build',), ()),
    'pq_cpu_stream': ('indexes/pq_cpu_stream_component.py', ('blas_build',), ()),
    'pq_parquet': ('indexes/pq_parquet_component.py', (), ()),
    'pq_sql020_parquet': ('indexes/pq_sql020_parquet_component.py', ('sdk_index', 'sdk_index_sha256'), ()),
}
SANITIZERS = {
    'pq_cpu': '--sanitize',
    'pq_cpu_stream': '--asan',
    'pq_parquet': '--asan',
    'pq_sql020_parquet': '--asan',
}
CODEC_PROBES = {
    'binary': ('provider_binary_test.c',),
    'floats': ('provider_float_test.c', 'provider_reader_test.c'),
    'lists': ('provider_list_test.c', 'provider_reader_test.c', 'provider_writer_test.c'),
}
PQ_PROBES = {
    'pq_cpu': ('pq_cpu_probe.cpp',),
    'pq_cpu_locking': ('pq_cpu_probe.cpp', 'pq_cpu_locking_thread_probe.cpp'),
    'pq_cpu_stream': ('pq_cpu_probe.cpp', 'pq_cpu_locking_thread_probe.cpp', 'pq_cpu_stream_probe.cpp'),
    'pq_parquet': ('pq_parquet_probe.cpp',),
    'pq_sql020_parquet': ('pq_parquet_probe.cpp',),
}
CASES = ('sdk6_probes', *SDK54, 'sdk54_local_qualification',
         'pq_stream_shapes', 'pq_parquet_native', *BITMAP, *LEGACY, 'sql020_pipeline')


def sha256(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def check_digest(path, expected):
    if not isinstance(expected, str) or not re.fullmatch(r'[0-9a-f]{64}', expected):
        raise ValueError(f'expected a lowercase SHA256 for {path}')
    if sha256(path) != expected:
        raise ValueError(f'input SHA256 differs from its recorded value: {path}')


def load_json(path):
    data = json.loads(path.read_text())
    if not isinstance(data, dict):
        raise ValueError(f'expected a JSON object in {path}')
    return data


def passed_json(path):
    data = load_json(path)
    status = data.get('status')
    if not isinstance(status, str) or not (status in ('pass', 'passed') or status.startswith('passed_')):
        raise ValueError(f'check failed in {path}: {status!r}')
    return data


def require(cfg, key, section='components'):
    value = cfg.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f'configure {section}.{key} to run this case')
    return value


def support_path(workspace):
    gcc = workspace / GCC
    return os.pathsep.join(str(path) for path in
                           [gcc / 'lib64', *(gcc.parent / name / 'lib' for name in ('isl', 'mpc', 'mpfr', 'gmp'))])


def source_inputs(source):
    return {item.name: sha256(item) for item in source.iterdir()
            if item.is_file() and (item.suffix in ('.h', '.cpp', '.map') or item.name == 'CMakeLists.txt')}


def provider(ctx, cfg, key, sdk=None):
    path = ctx.path(require(cfg, key))
    receipt = load_json(path)
    if receipt.get('status') != 'provider_built_unqualified':
        raise ValueError(f'provider build did not succeed: {path}')
    if (receipt.get('sources_before') != receipt.get('sources_after') or
            receipt.get('dependencies_before') != receipt.get('dependencies_after')):
        raise ValueError(f'provider inputs changed during build: {path}')
    if sdk is not None:
        inputs = source_inputs(ctx.source / 'contrib/og_lake' / sdk)
        if receipt.get('sources_after') != inputs:
            raise ValueError(f'provider receipt does not match current {sdk} source: {path}')
    library = Path(receipt['library']).resolve(strict=True)
    installed = Path(receipt['provider_prefix']) / 'lib/libog_lake_sdk_arrow.so'
    if library != installed.resolve(strict=True) or library.name != installed.name:
        raise ValueError(f'provider library is outside its recorded installation: {path}')
    check_digest(library, receipt.get('sha256'))
    shutil.copy2(path, ctx.out / f'{key}.json')
    return library, receipt['sha256']


def reference_duckdb(ctx, cfg, workspace=None, configurable=False):
    duckdb = ctx.path(cfg['duckdb']) if configurable and cfg.get('duckdb') else (workspace or ctx.workspace) / DUCKDB
    check_digest(duckdb, require(cfg, 'duckdb_sha256'))
    return duckdb


def sdk6_probes(ctx, cfg):
    library, sha = provider(ctx, cfg, 'sdk6_build', 'sdk_external')
    duckdb = reference_duckdb(ctx, cfg, configurable=True)
    ctx.run(['bash', ctx.repo_driver / 'sdk_external_tests/run_probe.sh', 'probes'], 'sdk6_probes',
            env=dict(CODE_BASE=str(ctx.source), DUCKDB=str(duckdb), DUCKDB_SHA256=cfg['duckdb_sha256'],
                     SDK6_LIBRARY=str(library), SDK6_SHA256=sha, CC=str(ctx.workspace / GCC / 'bin/gcc'),
                     LD_LIBRARY_PATH='', PROBE_LD_LIBRARY_PATH=support_path(ctx.workspace),
                     PROBE_OUTPUT_ROOT=str(ctx.out)),
            timeout=180)
    return {name: passed_json(ctx.out / 'probes' / f'{name}-result.json')
            for name in ('ordinal', 'fd', 'anchor', 'host')}


def sdk54_cases(case, ctx, cfg):
    library, sha = provider(ctx, cfg, 'sdk54_build', 'sdk')
    case_name = SDK54[case]
    source = ctx.source / 'contrib/og_lake/sdk'
    workspace = ctx.workspace
    if case_name == 'object_version':
        workspace = ctx.path(require(cfg, 'object_version_legacy_workspace'))
        legacy = workspace / '.install/opengauss-native-lake-debug/lib/postgresql/og_lake_sdk/libog_lake_sdk_arrow.so'
        legacy_sha = require(cfg, 'object_version_legacy_sha256')
        check_digest(legacy, legacy_sha)
        if legacy_sha == sha:
            raise ValueError('object-version check requires the pre-capability provider and its historical header')
    if case_name != 'candidate':
        reference_duckdb(ctx, cfg, workspace)

    def run_driver(name, label, args):
        script = ctx.driver / 'sdk' / SDK54_SCRIPT.get(name, f'test_{name}.py')
        ctx.run([ctx.python, __file__, 'sdk54', script, workspace, source, library, *args], label,
                timeout=1200)

    out = ctx.out / case_name
    args = ['--evidence', out]
    if case_name in READER_FIXTURE:
        fixture_dir = ctx.out / 'reader_fixture'
        run_driver('reader', 'reader_fixture', ['--evidence', fixture_dir])
        passed_json(fixture_dir / 'result.json')
        args.extend(['--input', fixture_dir / 'typed.parquet'])
    elif case_name == 'reader_budget':
        # The original budget driver takes its ABI header from the workspace tree.
        local_source = ctx.workspace / '.worktrees/opengauss-native-lake/contrib/og_lake/sdk'
        if source_inputs(local_source) != source_inputs(source):
            raise ValueError(f'SDK54 reader-budget headers differ at {local_source}')
        args = ['--output', out, '--library', library]
        if cfg.get('previous_sdk54_build'):
            old, _ = provider(ctx, cfg, 'previous_sdk54_build')
            args.extend(['--previous-library', old])
    elif case_name == 'object_version':
        args.extend(['--provider', library, '--source', ctx.source])
    elif case_name == 'versioned_files':
        fixture_dir = ctx.out / 'ducklake_fixture'
        run_driver('ducklake_files', 'ducklake_fixture', ['--evidence', fixture_dir])
        passed_json(fixture_dir / 'result.json')
        args.extend(['--fixture', fixture_dir])
    elif case_name == 'candidate':
        candidate = cfg.get('sdk54_candidate_inputs', {})
        if not isinstance(candidate, dict):
            raise ValueError('components.sdk54_candidate_inputs must be a JSON object')
        args.extend(['--build', ctx.path(cfg['sdk54_build'])])
        for name in ('list', 'float', 'faults', 'lifecycle'):
            value = require(candidate, name + '_result', section='components.sdk54_candidate_inputs')
            args.extend(['--' + name + '-result', ctx.path(value).resolve(strict=True)])
    run_driver(case_name, case, args)
    receipt = out / ('index.json' if case_name == 'candidate' else 'result.json')
    result = passed_json(receipt)
    return {'receipt': str(receipt), 'provider_sha256': sha, 'status': result['status']}


def legacy_inputs(case, cfg):
    inputs = cfg.get('legacy_inputs', {})
    if not isinstance(inputs, dict) or not isinstance(inputs.get(case, {}), dict):
        raise ValueError('components.legacy_inputs must map case names to input objects')
    return inputs.get(case, {})


def original_asset(ctx, workspace, relative):
    asset = workspace / DRIVER / relative
    if asset.resolve() != asset:
        raise ValueError(f'historical driver assets must be staged as real files: {asset}')
    check_digest(asset, sha256(ctx.repo_driver / relative))
    return asset


def sdk54_local_qualification(ctx, cfg):
    workspace = ctx.path(require(cfg, 'legacy_workspace'))
    section = 'components.legacy_inputs.sdk54_local_qualification'
    fixture = legacy_inputs('sdk54_local_qualification', cfg)
    build_path = ctx.path(require(fixture, 'provider_build', section))
    build = load_json(build_path)
    prefix = workspace / ARROW
    library = prefix / 'lib/libog_lake_sdk_arrow.so'
    source = workspace / '.worktrees/opengauss-native-lake/contrib/og_lake/sdk'
    if (build.get('status') != 'provider_built_unqualified' or
            Path(build['library']).resolve(strict=True) != library.resolve(strict=True) or
            build.get('sources_before') != build.get('sources_after') or
            build.get('sources_after') != source_inputs(source) or
            build.get('dependencies_before') != build.get('dependencies_after')):
        raise ValueError(f'historical v1 provider build does not match its original source/layout: {build_path}')
    check_digest(library, build.get('sha256'))
    check_digest(workspace / DUCKDB, require(fixture, 'duckdb_sha256', section))
    for name in ('build_provider.py', 'test_reader.py', 'test_writer.py', 'test_writer_faults.py',
                 'test_ducklake_files.py', 'provider_reader_test.c', 'provider_writer_test.c',
                 'provider_writer_fault_test.c', 'provider_io_faults.c', 'provider_dump_test.c'):
        original_asset(ctx, workspace, Path('sdk') / name)
    script = original_asset(ctx, workspace, 'sdk/qualify_local.py')
    out = ctx.out / 'qualification'
    ctx.run([ctx.python, script, '--evidence', out], 'sdk54_local_qualification',
            cwd=workspace, env={'CPP_WORKSPACE': str(workspace)}, timeout=1800)
    receipt = out / 'index.json'
    result = passed_json(receipt)
    if (result.get('library_sha256') != build['sha256'] or
            result.get('dependencies_after') != build['dependencies_after'] or
            len(result.get('cases', [])) != 4):
        raise ValueError(f'historical v1 provider qualification does not match its build: {receipt}')
    return {'receipt': str(receipt), 'status': result['status'], 'cases': len(result['cases'])}


def legacy_case(case, ctx, cfg):
    workspace = ctx.path(require(cfg, 'legacy_workspace'))
    path, required, optional = LEGACY[case]
    script = original_asset(ctx, workspace, path)
    codec = CODEC_PROBES.get(case.split('_', 1)[1])
    if codec:
        probes = [Path(path).with_name(codec[0]), *(Path('sdk') / name for name in codec[1:])]
    else:
        probes = [Path(path).with_name(name) for name in PQ_PROBES.get(case, ())]
    for relative in probes:
        original_asset(ctx, workspace, relative)
    if case == 'sdk020_qualification':
        for asset in (ctx.repo_driver / 'sdk/sql020_qa').iterdir():
            if asset.is_file() and asset.suffix in ('.py', '.c', '.cpp'):
                original_asset(ctx, workspace, asset.relative_to(ctx.repo_driver))
    case_cfg = legacy_inputs(case, cfg)
    unexpected = set(case_cfg) - set(required) - set(optional)
    if unexpected:
        raise ValueError(f'unexpected input for components.legacy_inputs.{case}: {sorted(unexpected)}')
    args = []
    for key in (*required, *(key for key in optional if key in case_cfg)):
        value = require(case_cfg, key, section=f'components.legacy_inputs.{case}')
        if key.endswith('_sha256'):
            check_digest(ctx.path(case_cfg[key.removesuffix('_sha256')]), value)
        else:
            value = ctx.path(value).resolve(strict=True)
        args.extend(['--' + key.replace('_', '-'), value])
    if case in ('pq_sql020_parquet', 'sdk020_qualification'):
        args.extend(['--workspace', workspace])
    if case == 'sdk020_qualification':
        build = load_json(ctx.path(case_cfg['provider_build']))
        prefix = Path(build['provider_prefix']).resolve(strict=True)
        if not (prefix / '.og-lake-sdk-owner.json').is_file():
            raise ValueError(f'SQL020 SDK qualification needs an owned historical provider prefix: {prefix}')
        args.append('--execute')

    variants = {'normal': []}
    if case in SANITIZERS:
        variants['sanitized'] = [SANITIZERS[case]]
    receipts = {}
    for name, flags in variants.items():
        run_id = f'fastcheck_{case}_{name}_{uuid.uuid4().hex[:16]}'
        out = workspace / EVIDENCE / run_id
        try:
            ctx.run([ctx.python, __file__, 'legacy', script, workspace, '--run-id', run_id, *args, *flags],
                    name, cwd=workspace, env={'CPP_WORKSPACE': str(workspace)}, timeout=1800)
        finally:
            for filename in ('result.json', 'index.json', 'failed.json'):
                if (out / filename).is_file():
                    shutil.copy2(out / filename, ctx.out / f'{name}-{filename}')
        if case == 'sdk020_qualification':
            index = load_json(out / 'index.json')
            status = index.get('status')
            cases = index.get('cases', {})
            if (status != 'verified_independent_sdk_sql020_int16_only' or len(cases) != 12 or
                    any(result.get('status') != 'pass' for result in cases.values())):
                raise ValueError(f'SQL020 SDK qualification did not pass 12 cases: {out}')
        else:
            status = passed_json(out / 'result.json')['status']
        receipts[name] = {'status': status, 'evidence': str(out)}
    return receipts


def probe_json(ctx, argv, label, checks, env):
    ctx.run(argv, label, env=env, timeout=120)
    log = ctx.out / (label + '.log')
    lines = log.read_text().splitlines()
    data = json.loads(lines[-1]) if lines else {}
    if data.get('status') != 'pass' or data.get('checks') != checks:
        raise ValueError(f'probe did not record {checks} passing checks: {log}')
    return data


def sql020_pipeline(ctx, cfg):
    workspace = ctx.path(require(cfg, 'legacy_workspace'))
    section = 'components.legacy_inputs.sql020_pipeline'
    inputs = legacy_inputs('sql020_pipeline', cfg)
    source = ctx.path(require(inputs, 'source', section)).resolve(strict=True)
    legacy_source = ctx.path(require(inputs, 'legacy_source', section)).resolve(strict=True)
    assets = ('sdk/build_provider.py', 'sdk/qualify_sql020_provider.py',
              'pq_native_build_inputs.py', 'pq_sql020_build_inputs.py',
              'indexes/pq_sql020_parquet_component.py', 'indexes/pq_sql020_dependency_build.py',
              'indexes/pq_parquet_format.h', 'indexes/pq_parquet_format.cpp',
              'indexes/pq_parquet_probe.cpp', 'indexes/pq_cpu_stream_probe.cpp',
              'indexes/pq_cpu.h', 'indexes/pq_cpu.cpp', 'indexes/pq_cpu_internal.h',
              'indexes/pq_cpu_ms.cpp', 'indexes/pq_cpu_locking_blas_meta.h',
              'indexes/pq_cpu_locking_blas_meta.cpp')
    for asset in assets:
        original_asset(ctx, workspace, asset)
    for asset in (ctx.repo_driver / 'sdk/sql020_qa').iterdir():
        if asset.is_file() and asset.suffix in ('.py', '.c', '.cpp'):
            original_asset(ctx, workspace, asset.relative_to(ctx.repo_driver))
    script = original_asset(ctx, workspace, 'indexes/qualify_sql020_pipeline.py')
    run_id = 'fastcheck_sql020_' + uuid.uuid4().hex[:16]
    receipt = workspace / EVIDENCE / run_id / 'result.json'
    try:
        ctx.run([ctx.python, script, '--execute', '--workspace', workspace, '--run-id', run_id,
                 '--source', source, '--legacy-source', legacy_source], 'sql020_pipeline',
                cwd=workspace, env={'CPP_WORKSPACE': str(workspace)})
    finally:
        if receipt.is_file():
            shutil.copy2(receipt, ctx.out / 'pipeline-result.json')
    result = load_json(receipt)
    phases = [('provider', 0), ('sdkqual', 0), ('pq', 0), ('pqasan', 0), ('static', 0)]
    if (result.get('status') != 'qualified_components_only' or
            [(row.get('name'), row.get('returncode')) for row in result.get('commands', [])] != phases):
        raise ValueError(f'SQL020 SDK/PQ pipeline did not complete qualification: {receipt}')
    check_digest(Path(result['pq_build']), result.get('pq_build_sha256'))
    return {'receipt': str(receipt), 'status': result['status'], 'phases': len(phases)}


def current_pq(case, ctx, cfg):
    pq_src = ctx.source / 'contrib/og_lake/pq'
    sdk = None
    sdk_sha = None
    if case == 'pq_parquet_native':
        sdk, sdk_sha = provider(ctx, cfg, 'sdk54_build', 'sdk')
    sys.path.insert(0, str(ctx.repo_driver))
    try:
        from pq_native_build_inputs import PUBLIC_PREFIXES, blas_inputs, global_symbols
    finally:
        sys.path.pop(0)
    blas = blas_inputs(ctx.workspace, receipt=ctx.path(require(cfg, 'blas_build')),
                       receipt_sha256=require(cfg, 'blas_build_sha256'))
    compiler = ctx.workspace / GCC / 'bin/g++'
    ssl = ctx.workspace / OPENSSL
    env = {'LD_LIBRARY_PATH': support_path(ctx.workspace), 'OMP_NUM_THREADS': '1',
           'OPENBLAS_NUM_THREADS': '1'}
    with tempfile.TemporaryDirectory(prefix='og_lake_fastcheck_pq_') as directory:
        build = Path(directory)
        archive = build / 'libog_lake_pq_cpu.a'
        ctx.run(['make', '-C', build, '-f', pq_src / 'Makefile', '-j1', 'all',
                 f'srcdir={pq_src}', f'top_builddir={build}', f'PQ_CXX={compiler}', 'PQ_OPT_FLAGS=-O2',
                 f"PQ_BLAS_INCLUDE={Path(blas['prefix']) / 'include'}",
                 f"PQ_BLAS_ARCHIVE={blas['library']}", f'PQ_OPENSSL_INCLUDE={ssl / "include"}'],
                'build', env=env, timeout=300)
        shutil.copy2(build / 'pq-symbol-map.log', ctx.out / 'pq-symbol-map.log')
        exported = global_symbols(archive)
        public = sorted(name for name in exported if name.startswith(PUBLIC_PREFIXES))
        leaked = sorted(name for name in exported if not name.startswith(PUBLIC_PREFIXES + ('oglpqcpu_',)))
        if leaked or not public:
            raise ValueError(f'PQ archive exports unexpected symbols: {leaked}')
        probe = 'pq_cpu_stream_probe.cpp' if case == 'pq_stream_shapes' else 'pq_parquet_probe.cpp'
        executable = build / 'probe'
        ctx.run([compiler, '-std=c++14', '-O2', '-fPIC', '-I' + str(pq_src),
                 '-I' + str(pq_src.parent), ctx.repo_driver / 'indexes' / probe, archive,
                 blas['library'], ssl / 'lib/libcrypto_static.a', '-pthread', '-lm', '-ldl', '-o', executable],
                'compile_probe', env=env, timeout=300)
        if case == 'pq_stream_shapes':
            result = probe_json(ctx, [executable, 'shapes'], 'shapes', 83, env)
            if result.get('dimensions') != [3, 1536] or result.get('chunks') != [1, 16]:
                raise ValueError('PQ stream shape coverage differs from the original checks')
            return {'probe': result, 'public_symbols': public, 'blas': blas}

        out = ctx.out / 'd1536'
        out.mkdir()
        writer_out = ctx.out / 'writer_negative'
        writer_out.mkdir()
        produce = probe_json(ctx, [executable, sdk, out, 'produce', 'd1536', '1'], 'produce', 29, env)
        manifest = produce.get('manifest')
        if not isinstance(manifest, str) or not manifest:
            raise ValueError('PQ parquet producer did not return a manifest')
        consume = probe_json(ctx, [executable, sdk, out, 'consume', 'd1536', '1', manifest], 'consume', 48, env)
        negative = probe_json(ctx, [executable, sdk, out, 'negative', 'd1536', '1', manifest], 'negative', 76, env)
        writer = probe_json(ctx, [executable, sdk, writer_out, 'writer-negative', 'metric-1', '1'],
                            'writer_negative', 117, env)
        if writer.get('cleanup_failures') != 0:
            raise ValueError('PQ parquet writer left failed output published')
        return {'produce': produce, 'consume': consume, 'negative': negative, 'writer_negative': writer,
                'provider_sha256': sdk_sha, 'blas': blas}


def bitmap_case(case, ctx, cfg):
    dependency = ctx.path(require(cfg, 'roaring_build')).resolve(strict=True)
    out = ctx.out / 'probe'
    ctx.run([ctx.python, ctx.driver / 'indexes' / BITMAP[case], '--run-id', case,
             '--source', ctx.source / 'contrib/og_lake', '--dependency-build', dependency, '--evidence', out],
            case, timeout=600)
    receipt = out / 'result.json'
    result = passed_json(receipt)
    return {'receipt': str(receipt), 'status': result['status'],
            'checks': result.get('probe_checks', result.get('case_count'))}


def run(case, ctx):
    cfg = ctx.config.get('components', {})
    if not isinstance(cfg, dict):
        raise ValueError('components must be a JSON object')
    if case == 'sdk6_probes':
        return sdk6_probes(ctx, cfg)
    if case in SDK54:
        return sdk54_cases(case, ctx, cfg)
    if case == 'sdk54_local_qualification':
        return sdk54_local_qualification(ctx, cfg)
    if case in ('pq_stream_shapes', 'pq_parquet_native'):
        return current_pq(case, ctx, cfg)
    if case in BITMAP:
        return bitmap_case(case, ctx, cfg)
    if case == 'sql020_pipeline':
        return sql020_pipeline(ctx, cfg)
    if case in LEGACY:
        return legacy_case(case, ctx, cfg)
    raise ValueError(f'unknown lake component case: {case}')


def load_driver(name, script):
    spec = importlib.util.spec_from_file_location(name, script)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def worker():
    # Each invocation gets its own interpreter and calls the original driver main.
    # Repoint its module-level paths without modifying any of its assertions.
    mode, script, workspace, *args = sys.argv[1:]
    script, workspace = Path(script), Path(workspace)
    sys.path[:0] = [str(script.parent), str(workspace / DRIVER)]
    if mode == 'sdk54':
        source, library, *args = args
        build = load_driver('build_provider', script.parent / 'build_provider.py')
        build.ROOT, build.SOURCE, build.GCC = workspace, Path(source), workspace / GCC
        build.SDK, build.PREFIX = workspace / ARROW.parent.parent, workspace / ARROW
        os.environ['C_INCLUDE_PATH'] = str(build.PREFIX / 'include')
        module = load_driver('_test_driver', script)
        module.PREFIX = Path(library).parent.parent
    elif mode == 'legacy':
        # Several archived drivers used a fixed machine root.
        module = load_driver('_test_driver', script)
        module.D = workspace / DRIVER
    else:
        raise ValueError(f'unknown component worker: {mode}')
    module.ROOT = workspace
    sys.argv = [str(script), *args]
    return module.main()


if __name__ == '__main__':
    if not __debug__:
        raise RuntimeError('component assertions must be enabled')
    sys.exit(worker())
