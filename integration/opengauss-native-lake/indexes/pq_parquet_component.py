#!/usr/bin/env python3
"""Real SDK5ae PQ Parquet sidecars, independent processes and DuckDB oracle.

No Native/SDK/wrapper/source-lock modification, server, fixture or integration.
The frozen stream CPU source is linked unchanged to the private locking BLAS.
"""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import resource
import shutil
import struct
import subprocess

ROOT = Path('/home/qiumc/data/code/cpp')


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--asan', action='store_true')
    args = parser.parse_args()
    if not re.fullmatch(r'[A-Za-z0-9_.-]+', args.run_id) or args.run_id in ['.', '..']:
        parser.error('new simple run-id required')
    source = Path(__file__).resolve().parent
    sdk_source = ROOT / '.worktrees/opengauss-native-lake-inline-index/contrib/og_lake'
    evidence = ROOT / '公共文档/openGauss原生湖存储/验证'
    out = evidence / args.run_id
    build = ROOT / '.build/opengauss-native-lake/pq-parquet-component' / args.run_id
    out.mkdir(exist_ok=False)
    build.mkdir(parents=True, exist_ok=False)
    blas_manifest = evidence / 'pq_openblas_locking_build_1109_01/build.json'
    blas = json.loads(blas_manifest.read_text())
    cpu_index = evidence / 'pq_cpu_stream_restore_scoped_verified_index.json'
    frozen = json.loads(cpu_index.read_text())
    sdk_index = evidence / 'sdk_binary_transport_scoped_verified_index.json'
    sdk = json.loads(sdk_index.read_text())
    provider = Path(json.loads(Path(sdk['build']).read_text())['library'])
    duckdb = ROOT / '.reference/opengauss-native-lake/build/duckdb-gcc10/duckdb'
    inputs = [source / n for n in ['pq_parquet_format.h', 'pq_parquet_format.cpp', 'pq_parquet_probe.cpp', 'pq_parquet_component.py',
                                  'pq_cpu.h', 'pq_cpu.cpp', 'pq_cpu_internal.h', 'pq_cpu_ms.cpp', 'pq_cpu_locking_blas_meta.h', 'pq_cpu_locking_blas_meta.cpp']]
    inputs += [sdk_source / n for n in ['sdk/lake_parquet_sdk.h', 'vendor/arrow20/abi.h']]
    inputs += [blas_manifest, Path(blas['library']), cpu_index, sdk_index, provider, duckdb]
    result = dict(status='failed', scope=__doc__, assertions=[], commands=[], inputs={str(p): sha(p) for p in inputs},
                  started_at=datetime.datetime.now(datetime.timezone.utc).isoformat(), sanitizer=args.asan,
                  native_integration=False, source_lock_updated=False, sdk_pool_reservation_bytes=8*1024*1024,
                  unqualified=['Native graph/PQ/cache/ResourceOwner/MemoryContext/EXPLAIN integration.',
                    'Production SMP, RAC, remote storage, I/O optimization, recall or performance.',
                    'Whole-query allocation limits; budgets apply to participating component reservations and capped SDK pool.',
                    'TSan unavailable; SDK/BLAS shared C/assembly libraries not ASan-instrumented.',
                    'Publication is caller-owned: all three SDK candidates must finish/fsync before catalog atomic publish.'])
    archive = out / 'executed-inputs'
    archive.mkdir()
    for p in inputs:
        shutil.copy2(p, archive / p.name)
    gcc = ROOT / 'binarylibs/buildtools/gcc10.3'
    ssl = ROOT / 'binarylibs/kernel/dependency/openssl/comm'
    env = dict(os.environ)
    env['LD_LIBRARY_PATH'] = ':'.join(map(str, [ssl / 'lib', ROOT / '.reference/opengauss-native-lake/sdk/install/arrow-20.0.0/lib'] +
        [gcc / p for p in ['gcc/lib64', 'isl/lib', 'mpc/lib', 'mpfr/lib', 'gmp/lib']]))
    env['OMP_NUM_THREADS'] = env['OPENBLAS_NUM_THREADS'] = '1'
    if args.asan:
        env['ASAN_OPTIONS'] = 'detect_leaks=1:halt_on_error=1:abort_on_error=1'
        env['UBSAN_OPTIONS'] = 'halt_on_error=1:print_stacktrace=1'

    def check(name, actual, expected=True):
        ok = actual == expected
        result['assertions'].append(dict(name=name, observed=actual, expected=expected, status='pass' if ok else 'fail'))
        if not ok:
            raise AssertionError(name)

    def command(name, argv, timeout=180, parse=True):
        result['commands'].append(dict(name=name, argv=list(map(str, argv))))
        def no_core():
            resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
        with (out / (name + '.stdout.log')).open('w') as so, (out / (name + '.stderr.log')).open('w') as se:
            child = subprocess.Popen(list(map(str, argv)), stdout=so, stderr=se, env=env, preexec_fn=no_core)
            try:
                code = child.wait(timeout=timeout)
            except BaseException:
                child.kill()
                child.wait()
                raise
        result['commands'][-1].update(pid=child.pid, returncode=code, waited=True)
        if code:
            raise RuntimeError(name + ' failed with ' + str(code))
        output = (out / (name + '.stdout.log')).read_text()
        return json.loads(output) if parse and output.strip() else None

    def literal(value):
        return "'" + str(value).replace("'", "''") + "'"

    def sql(name, query):
        return command(name, [duckdb, '-no-init', '-batch', '-json', '-c', 'SET threads=1;' + query])

    try:
        check('actual SDK5ae provider identity', sha(provider), sdk['provider_sha256'])
        check('private locking BLAS identity', sha(blas['library']), blas['library_sha256'])
        for name in ['pq_cpu.h', 'pq_cpu.cpp', 'pq_cpu_internal.h', 'pq_cpu_ms.cpp', 'pq_cpu_locking_blas_meta.h', 'pq_cpu_locking_blas_meta.cpp']:
            check('frozen stream CPU input unchanged/' + name, sha(source / name), frozen['current_inputs'][str(source / name)])
        common = [gcc / 'gcc/bin/g++', '-std=c++14', '-O0', '-g', '-Wall', '-Wextra', '-Werror', '-Wno-sign-compare',
                  '-Wno-unused-parameter', '-Wno-unknown-pragmas', '-Wno-deprecated-declarations', '-Wno-misleading-indentation',
                  '-fPIC', '-I', source, '-I', sdk_source, '-I', Path(blas['prefix']) / 'include', '-I', ssl / 'include',
                  '-include', source / 'pq_cpu_locking_blas_meta.h', '-Dcblas_sgemm=oglpq_cblas_sgemm', '-Dcblas_snrm2=oglpq_cblas_snrm2',
                  '-Dopenblas_get_config=pq_cpu_blas_immutable_config', '-Dopenblas_get_parallel=pq_cpu_blas_parallel_checked',
                  '-Dopenblas_get_num_threads=pq_cpu_blas_num_threads_checked']
        if args.asan:
            common += ['-fsanitize=address,undefined', '-fno-omit-frame-pointer', '-fno-pie', '-no-pie']
        exe = build / 'pq-parquet-probe'
        command('compile', common + [source / 'pq_parquet_probe.cpp', source / 'pq_parquet_format.cpp', source / 'pq_cpu.cpp', source / 'pq_cpu_ms.cpp',
            source / 'pq_cpu_locking_blas_meta.cpp', blas['library'], '-Wl,--exclude-libs,ALL', '-L', ssl / 'lib', '-lcrypto', '-ldl', '-pthread', '-lm', '-o', exe])
        result['consumer_binary_sha256'] = sha(exe)
        artifacts = build / 'test-only-pq-artifacts'
        artifacts.mkdir()
        result['cases'] = []
        for tag, metric, dimension, chunks, nodes in [('metric-1', 1, 8, 2, 2057), ('metric-2', 2, 8, 2, 2057), ('metric-3', 3, 8, 2, 2057), ('empty', 1, 8, 2, 0), ('d3', 1, 3, 1, 17), ('d1536', 1, 1536, 16, 17)]:
            produced = command('producer-' + tag, [exe, provider, artifacts, 'produce', tag, metric])
            check('producer exited before independent consumer/' + tag, result['commands'][-1]['returncode'], 0)
            consumed = command('consumer-' + tag, [exe, provider, artifacts, 'consume', tag, metric, produced['manifest']])
            check('model/codes/LUT reopen across processes/' + tag, consumed['status'], 'pass')
            oracle = (artifacts / (tag + '.oracle')).read_bytes()
            model = sql('duck-model-' + tag, 'SELECT centroid_id,hex(centers) AS payload FROM read_parquet(' + literal(artifacts / (tag + '-model.parquet')) + ') ORDER BY centroid_id')
            check('DuckDB every centroid ID/dense coverage/' + tag, [x['centroid_id'] for x in model], list(range(256)))
            check('DuckDB every persisted float32 model bit/' + tag, b''.join(bytes.fromhex(x['payload']) for x in model) == oracle[:256*dimension*4])
            codes = sql('duck-codes-' + tag, 'SELECT node_id,hex(codes) AS payload FROM read_parquet(' + literal(artifacts / (tag + '-codes.parquet')) + ') ORDER BY node_id') or []
            check('DuckDB dense node IDs are graph nodes/' + tag, [x['node_id'] for x in codes], list(range(nodes)))
            check('DuckDB every persisted code byte/' + tag, b''.join(bytes.fromhex(x['payload']) for x in codes) == oracle[256*dimension*4:256*dimension*4+nodes*chunks])
            manifest = sql('duck-manifest-' + tag, 'SELECT field_id,hex(value) AS payload FROM read_parquet(' + literal(artifacts / (tag + '-manifest.parquet')) + ') ORDER BY field_id')
            check('DuckDB manifest fixed ordered keys/' + tag, [x['field_id'] for x in manifest], list(range(1, 21)))
            digest = hashlib.sha256(b'og_lake_pq_parquet_artifact_v1')
            for row in manifest:
                data = bytes.fromhex(row['payload'])
                digest.update(struct.pack('>Q', row['field_id']))
                digest.update(struct.pack('>Q', len(data)))
                digest.update(data)
            check('independent full manifest digest/' + tag, digest.hexdigest(), produced['manifest'])
            result['cases'].append(dict(tag=tag, metric=metric, dimension=dimension, chunks=chunks, nodes=nodes, produced=produced, consumed=consumed,
                files={p.name: sha(p) for p in artifacts.glob(tag + '*')}))
        first = result['cases'][0]['produced']
        negative = command('negative-matrix', [exe, provider, artifacts, 'negative', 'metric-1', 1, first['manifest']])
        check('legal schema corruption and wrong expected identity rejected', negative['status'], 'pass')
        result['negative_matrix'] = negative
        writer = command('writer-errors', [exe, provider, artifacts, 'writer-negative', 'unused', 1])
        check('writer errors poison without publishing', [writer['status'], writer['cleanup_failures']], ['pass', 0])
        check('failed SDK candidates actually removed', not list(artifacts.glob('writer-invalid-*.parquet')))
        result['writer_errors'] = writer
        baseline = command('allocation-reader-baseline', [exe, provider, artifacts, 'fault', 'metric-1', 1, first['manifest'], 0])
        result['allocation_baseline'] = baseline
        selected = sorted({1, 2, 3, baseline['calls']//2, baseline['calls']-1, baseline['calls']})
        selected = [n for n in selected if n > 0]
        result['allocation_selected_points'] = selected
        for point in selected:
            item = command('allocation-reader-' + str(point), [exe, provider, artifacts, 'fault', 'metric-1', 1, first['manifest'], point])
            check('real C++ allocator fault/' + str(point), [item['code'], item['outstanding_scalar'], item['outstanding_array']], [4, 0, 0])
        polls = sorted({1, 2, baseline['polls']//2, baseline['polls']-1, baseline['polls']})
        result['cancellation_selected_polls'] = polls
        for point in polls:
            item = command('cancel-reader-' + str(point), [exe, provider, artifacts, 'cancel', 'metric-1', 1, first['manifest'], point])
            check('poll cancellation refuses partial reader/' + str(point), [item['code'], item['outstanding_scalar'], item['outstanding_array']], [5, 0, 0])
        command('dependencies', ['ldd', exe], parse=False)
        command('dynamic-symbols', ['nm', '-D', '--defined-only', exe], parse=False)
        check('private BLAS dynamic symbols hidden', 'oglpq_' not in (out / 'dynamic-symbols.stdout.log').read_text())
        check('frozen and compiled inputs unchanged after all cases', all(sha(p) == h for p, h in result['inputs'].items()))
        result['status'] = 'passed_independent_pq_parquet_component_only'
    except BaseException as error:
        result['reason'] = str(error)
    result['finished_at'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    (out / 'result.json').write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps(dict(status=result['status'], assertions=len(result['assertions']), reason=result.get('reason'), result=str(out / 'result.json')), ensure_ascii=False))
    return 0 if result['status'].startswith('passed_') else 1


if __name__ == '__main__':
    raise SystemExit(main())
