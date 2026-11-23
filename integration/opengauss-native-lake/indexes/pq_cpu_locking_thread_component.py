#!/usr/bin/env python3
"""Private serial+USE_LOCKING OpenBLAS / immutable-metadata PQ build variant.

Links frozen PQ v1 files unchanged with explicit private CBLAS symbol mappings.
Concurrent tests do not hold a global lock around PQ or BLAS operations. This
does not qualify Native SMP, whole-query memory accounting or persistent cache.
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
import subprocess

ROOT = Path('/home/qiumc/data/code/cpp')


def sha(p):
    return hashlib.sha256(Path(p).read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--blas-build', type=Path, default=ROOT / '公共文档/openGauss原生湖存储/验证/pq_openblas_locking_build_1109_01/build.json')
    args = parser.parse_args()
    if not re.fullmatch(r'[A-Za-z0-9_.-]+', args.run_id) or args.run_id in ['.', '..']:
        parser.error('new simple run-id required')
    out = ROOT / '公共文档/openGauss原生湖存储/验证' / args.run_id
    build = ROOT / '.build/opengauss-native-lake/pq-locking-thread-component' / args.run_id
    out.mkdir(exist_ok=False)
    build.mkdir(parents=True, exist_ok=False)
    source = Path(__file__).resolve().parent
    blas = json.loads(args.blas_build.read_text())
    library, prefix = Path(blas['library']), Path(blas['prefix'])
    frozen_path = ROOT / '公共文档/openGauss原生湖存储/验证/pq_cpu_c_boundary_scoped_verified_index.json'
    frozen = json.loads(frozen_path.read_text())
    files = [source / n for n in ['pq_cpu.h', 'pq_cpu_internal.h', 'pq_cpu.cpp', 'pq_cpu_ms.cpp', 'pq_cpu_probe.cpp',
                                 'pq_cpu_locking_blas_meta.h', 'pq_cpu_locking_blas_meta.cpp', 'pq_cpu_locking_thread_probe.cpp', 'pq_cpu_locking_thread_component.py']]
    files += [args.blas_build, library, prefix / 'include/cblas.h', prefix / 'include/openblas_config.h', frozen_path]
    result = dict(status='failed', scope=__doc__, assertions=[], inputs={str(p): sha(p) for p in files},
                  frozen_pq_v1_index=str(frozen_path), private_blas_manifest=str(args.blas_build),
                  started_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
                  build_variant='frozen PQ v1 numerical code + prefixed serial-locking OpenBLAS + pthread_once immutable config bridge',
                  unqualified=['Native SMP and process/threadpool lifecycle integration', 'Deterministic k-means seeds',
                               'All BLAS C/assembly memory accesses under TSan: static archive is not TSan instrumented',
                               'Whole-query/global-cache allocator budgets and cancellation inside BLAS calls'])
    archive = out / 'executed-inputs'
    archive.mkdir()
    for p in files:
        name = p.name if p.parent == source else 'dependency-' + p.name
        shutil.copy2(p, archive / name)
    gcc = ROOT / 'binarylibs/buildtools/gcc10.3'
    ssl = ROOT / 'binarylibs/kernel/dependency/openssl/comm'
    env = dict(os.environ)
    env['LD_LIBRARY_PATH'] = ':'.join([str(ssl / 'lib')] + [str(gcc / p) for p in ['gcc/lib64', 'isl/lib', 'mpc/lib', 'mpfr/lib', 'gmp/lib']])
    env['OMP_NUM_THREADS'] = '1'
    env['OPENBLAS_NUM_THREADS'] = '1'

    def check(name, actual, expected=True):
        ok = actual == expected
        result['assertions'].append(dict(name=name, observed=actual, expected=expected, status='pass' if ok else 'fail'))
        if not ok:
            raise AssertionError(name)

    def command(name, argv, expected=True, timeout=120):
        result.setdefault('commands', []).append(dict(name=name, argv=list(map(str, argv))))
        def no_core():
            resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
        with (out / (name + '.stdout.log')).open('w') as so, (out / (name + '.stderr.log')).open('w') as se:
            p = subprocess.run(list(map(str, argv)), env=env, stdout=so, stderr=se, preexec_fn=no_core, timeout=timeout)
        if expected and p.returncode:
            raise RuntimeError(name + ' failed with ' + str(p.returncode) + ', see stderr log')
        return p.returncode

    try:
        check('explicit locking dependency completed', blas['status'], 'built_provisional_unlinked')
        check('private static archive exactly matches provisional manifest', sha(library), blas['library_sha256'])
        check('frozen v1 input set has not changed', all(sha(p) == h for p, h in frozen['inputs'].items()))
        common = [gcc / 'gcc/bin/g++', '-std=c++14', '-O0', '-g', '-Wall', '-Wextra', '-Werror', '-Wno-sign-compare',
                  '-Wno-unused-parameter', '-Wno-unknown-pragmas', '-Wno-deprecated-declarations', '-Wno-misleading-indentation',
                  '-fPIC', '-I', source, '-I', prefix / 'include', '-I', ssl / 'include', '-include', source / 'pq_cpu_locking_blas_meta.h',
                  '-Dcblas_sgemm=oglpq_cblas_sgemm', '-Dcblas_snrm2=oglpq_cblas_snrm2',
                  '-Dopenblas_get_config=pq_cpu_blas_immutable_config', '-Dopenblas_get_parallel=pq_cpu_blas_parallel_checked',
                  '-Dopenblas_get_num_threads=pq_cpu_blas_num_threads_checked', '-DPQ_CPU_METADATA_TEST_FAULTS']
        implementation = [source / 'pq_cpu.cpp', source / 'pq_cpu_ms.cpp', source / 'pq_cpu_locking_blas_meta.cpp',
                          library, '-Wl,--exclude-libs,ALL', '-L', ssl / 'lib', '-lcrypto', '-pthread', '-lm']
        exe = build / 'locking-thread-probe'
        command('compile-thread-probe', common + [source / 'pq_cpu_locking_thread_probe.cpp'] + implementation + ['-o', exe])
        result['consumer_binary_sha256'] = sha(exe)
        command('cold-once-config', [exe, 'cold-metadata'])
        cold = json.loads((out / 'cold-once-config.stdout.log').read_text())
        check('concurrent first use executes one serial raw-config read', [cold['status'], cold['initializers'], cold['raw_config_reads']], ['pass', 1, 1])
        result['backend_metadata'] = cold
        for fault in [1, 2, 3]:
            name = 'metadata-failure-' + str(fault)
            command(name, [exe, 'metadata-failure', fault])
            observed = json.loads((out / (name + '.stdout.log')).read_text())
            check('metadata NULL/length/allocation-exception fails closed/' + str(fault), [observed['status'], observed['initializer_code'], observed['train_code']], ['pass', 2 if fault == 3 else 1, 4])
        command('shared-model-serial-parallel', [exe, 'shared'])
        shared = json.loads((out / 'shared-model-serial-parallel.stdout.log').read_text())
        check('concurrent immutable-model encode/query equals same-codebook serial result', [shared['status'], shared['parallel_encode_query_runs'], shared['wrong_poll_owners']], ['pass', 64, 0])
        check('numeric API scopes truly overlap across workers', shared['peak_overlapping_api_operations'] >= 2)
        result['shared_model'] = shared
        result['concurrent_cases'] = []
        for operation in ['train', 'encode', 'query', 'distance']:
            for mode in ['success', 'cancel', 'fault', 'mixed']:
                name = operation + '-' + mode
                command(name, [exe, operation, mode])
                observed = json.loads((out / (name + '.stdout.log')).read_text())
                check('concurrent status/poll/cleanup isolation/' + name, [observed['status'], observed['raw_config_reads']], ['pass', 1])
                check('all concurrent scalar/array allocations released/' + name,
                      [[w['outstanding_scalar'], w['outstanding_array'], w['wrong_poll_owners']] for w in observed['workers']], [[0, 0, 0]] * 4)
                result['concurrent_cases'].append(observed)
        command('raw-config-risk-negative', [exe, 'raw-config-risk'], expected=False)
        raw = json.loads((out / 'raw-config-risk-negative.stdout.log').read_text())
        result['raw_upstream_config_negative'] = raw
        check('negative uses actual raw upstream mutable-config function', raw['calls'], 80000)
        # Keep old suite source intact; relink an independent consumer against
        # this new variant for every real scalar/array allocation failure.
        serial = build / 'frozen-v1-new-blas-probe'
        command('compile-frozen-source-serial-probe', common + [source / 'pq_cpu_probe.cpp'] + implementation + ['-o', serial])
        for mode in [[], ['extremes'], ['million']]:
            name = 'serial-' + (mode[0] if mode else 'numeric')
            command(name, [serial] + mode)
            check('unchanged v1 consumer under new backend/' + name, json.loads((out / (name + '.stdout.log')).read_text())['status'], 'pass')
        result['fault_injection'] = {}
        for operation in ['train', 'encode', 'query', 'distance']:
            name = 'allocation-' + operation + '-baseline'
            command(name, [serial, 'fault', operation, 0])
            baseline = json.loads((out / (name + '.stdout.log')).read_text())
            result['fault_injection'][operation] = baseline
            for point in range(1, baseline['calls'] + 1):
                name = 'allocation-' + operation + '-' + str(point)
                command(name, [serial, 'fault', operation, point])
                observed = json.loads((out / (name + '.stdout.log')).read_text())
                check('actual C++ allocation failure with locking backend/' + operation + '/' + str(point),
                      [observed['code'], observed['outstanding_scalar'], observed['outstanding_array']], [2, 0, 0])
        # TSan's runtime availability is tested independently; never substitute
        # a functional concurrency pass for a successful TSan run.
        smoke = build / 'tsan-smoke.cpp'
        smoke.write_text('#include <thread>\nint main(){int x=0;std::thread t([&]{x=1;});t.join();return x==1?0:1;}\n')
        rc = command('compile-tsan-smoke', [gcc / 'gcc/bin/g++', '-std=c++14', '-g', '-O0', '-fsanitize=thread', '-fno-pie', '-no-pie', smoke, '-pthread', '-o', build / 'tsan-smoke'], expected=False)
        result['tsan'] = dict(scope='C++ consumer/runtime only; OpenBLAS archive C/assembly is uninstrumented', compile_returncode=rc)
        if rc == 0:
            rc = command('run-tsan-smoke', [build / 'tsan-smoke'], expected=False)
            result['tsan']['smoke_returncode'] = rc
            if rc == 0:
                tsan = build / 'locking-thread-probe-tsan'
                command('compile-tsan-thread-probe', common + ['-fsanitize=thread', '-fno-omit-frame-pointer', '-fno-pie', '-no-pie', source / 'pq_cpu_locking_thread_probe.cpp'] + implementation + ['-o', tsan])
                result['tsan']['shared_returncode'] = command('tsan-shared-model', [tsan, 'shared'], expected=False)
                result['tsan']['raw_negative_returncode'] = command('tsan-raw-config-negative', [tsan, 'raw-config-risk'], expected=False)
            else:
                result['tsan']['unavailable_reason'] = (out / 'run-tsan-smoke.stderr.log').read_text()[:2000]
        command('dynamic-dependencies', ['ldd', exe])
        dependencies = (out / 'dynamic-dependencies.stdout.log').read_text()
        check('no system/shared BLAS or OpenMP backend is loaded', 'openblas' not in dependencies.lower() and 'libgomp' not in dependencies and 'libiomp' not in dependencies)
        command('dynamic-symbols', ['nm', '-D', '--defined-only', exe])
        check('private prefixed archive symbols hidden at final link', 'oglpq_' not in (out / 'dynamic-symbols.stdout.log').read_text())
        check('frozen v1 input set is still unchanged', all(sha(p) == h for p, h in frozen['inputs'].items()))
        check('all compiled inputs remain unchanged', all(sha(p) == h for p, h in result['inputs'].items()))
        result['status'] = 'passed_provisional_thread_dependency_variant_only'
    except BaseException as error:
        result['reason'] = str(error)
    result['finished_at'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    (out / 'result.json').write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps(dict(status=result['status'], assertions=len(result['assertions']), reason=result.get('reason'), result=str(out / 'result.json')), ensure_ascii=False))
    return 0 if result['status'].startswith('passed_') else 1


if __name__ == '__main__':
    raise SystemExit(main())
