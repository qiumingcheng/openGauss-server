#!/usr/bin/env python3
"""Independent bounded range encoder / primitive model-codes reopen checks.

Uses the private serial+USE_LOCKING OpenBLAS and immutable metadata shim.
Produces a test-only primitive transport, never a Native lake artifact. Old
575/602 receipts and archived inputs retain their historical identities.
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


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--asan', action='store_true')
    parser.add_argument('--blas-build', type=Path, default=ROOT / '公共文档/openGauss原生湖存储/验证/pq_openblas_locking_build_1109_01/build.json')
    args = parser.parse_args()
    if not re.fullmatch(r'[A-Za-z0-9_.-]+', args.run_id) or args.run_id in ['.', '..']:
        parser.error('new simple run-id required')
    source = Path(__file__).resolve().parent
    evidence = ROOT / '公共文档/openGauss原生湖存储/验证'
    out = evidence / args.run_id
    build = ROOT / '.build/opengauss-native-lake/pq-stream-component' / args.run_id
    out.mkdir(exist_ok=False)
    build.mkdir(parents=True, exist_ok=False)
    blas = json.loads(args.blas_build.read_text())
    library, prefix = Path(blas['library']), Path(blas['prefix'])
    names = ['pq_cpu.h', 'pq_cpu_internal.h', 'pq_cpu.cpp', 'pq_cpu_ms.cpp', 'pq_cpu_probe.cpp',
             'pq_cpu_locking_blas_meta.h', 'pq_cpu_locking_blas_meta.cpp', 'pq_cpu_locking_thread_probe.cpp',
             'pq_cpu_stream_probe.cpp', 'pq_cpu_stream_component.py']
    inputs = [source / n for n in names] + [args.blas_build, library, prefix / 'include/cblas.h', prefix / 'include/openblas_config.h']
    historical = [evidence / name / 'result.json' for name in ['pq_cpu_c_boundary_component_1109_03',
        'pq_cpu_c_boundary_asan_component_1109_01', 'pq_cpu_locking_thread_component_1109_02']]
    inputs += historical
    result = dict(status='failed', scope=__doc__, sanitizer='ASan/UBSan/LSan consumer, uninstrumented private BLAS' if args.asan else 'debug',
                  inputs={str(p): sha(p) for p in inputs}, assertions=[], commands=[], started_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
                  original_receipts_unchanged=True, source_lock_updated=False, native_integration=False,
                  limitations=['Not a Native artifact protocol, cache, ResourceOwner, SMP or whole-query allocator qualification.',
                    'Builder stores full immutable N*M codes but borrows no full N*d input matrix; each append is <=1024 rows.',
                    'Actual BLAS binary identity remains pinned by external manifest; runtime backend fingerprint binds algorithm source/version and config.',
                    'BLAS numeric calls remain internally noncancellable. No PostgreSQL longjmp crosses C++.',
                    'Hashes detect accidental mismatch/corruption, not authentication: host must provide expected catalog identity.',
                    'No TSan qualification; previous standalone TSan smoke failed before tests.'])
    archive = out / 'executed-inputs'
    archive.mkdir()
    for p in inputs:
        name = p.name if p.parent == source else (p.parent.name + '-' + p.name)
        shutil.copy2(p, archive / name)
    gcc = ROOT / 'binarylibs/buildtools/gcc10.3'
    ssl = ROOT / 'binarylibs/kernel/dependency/openssl/comm'
    env = dict(os.environ)
    env['LD_LIBRARY_PATH'] = ':'.join([str(ssl / 'lib')] + [str(gcc / p) for p in ['gcc/lib64', 'isl/lib', 'mpc/lib', 'mpfr/lib', 'gmp/lib']])
    env['OMP_NUM_THREADS'] = env['OPENBLAS_NUM_THREADS'] = '1'
    if args.asan:
        env['ASAN_OPTIONS'] = 'detect_leaks=1:halt_on_error=1:abort_on_error=1'
        env['UBSAN_OPTIONS'] = 'halt_on_error=1:print_stacktrace=1'

    def check(name, actual, expected=True):
        ok = actual == expected
        result['assertions'].append(dict(name=name, observed=actual, expected=expected, status='pass' if ok else 'fail'))
        if not ok:
            raise AssertionError(name)

    def command(name, argv, expected=0, timeout=120):
        result['commands'].append(dict(name=name, argv=list(map(str, argv))))
        def no_core():
            resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
        with (out / (name + '.stdout.log')).open('w') as so, (out / (name + '.stderr.log')).open('w') as se:
            child = subprocess.Popen(list(map(str, argv)), env=env, stdout=so, stderr=se, preexec_fn=no_core)
            try:
                rc = child.wait(timeout=timeout)
            except BaseException:
                child.kill()
                child.wait()
                raise
        result['commands'][-1].update(pid=child.pid, returncode=rc, waited=True)
        if (expected == 0 and rc != 0) or (expected == 'nonzero' and rc == 0):
            raise RuntimeError(name + ' unexpected status ' + str(rc))
        return rc

    def observed(name):
        return json.loads((out / (name + '.stdout.log')).read_text())

    try:
        check('locked private library unchanged', sha(library), blas['library_sha256'])
        # Before evolving v1, both parent and independent reviewer verified all
        # archived input hashes. Recheck historical copies rather than compare
        # old h/cpp inputs to deliberately evolved current h/cpp.
        archive_counts = {}
        for receipt in historical:
            old = json.loads(receipt.read_text())
            old_archive = receipt.parent / 'executed-inputs'
            hashes = {sha(p) for p in old_archive.iterdir() if p.is_file()}
            check('historical executed-inputs remain complete/' + receipt.parent.name, all(h in hashes for h in old['inputs'].values()))
            archive_counts[receipt.parent.name] = len(old['inputs'])
            for name in ['pq_cpu_probe.cpp', 'pq_cpu_ms.cpp', 'pq_cpu_internal.h']:
                old_value = old['inputs'].get(str(source / name))
                if old_value:
                    check('unchanged historical numerical/probe input/' + receipt.parent.name + '/' + name, sha(source / name), old_value)
        result['historical_input_counts'] = archive_counts
        common = [gcc / 'gcc/bin/g++', '-std=c++14', '-O0', '-g', '-Wall', '-Wextra', '-Werror', '-Wno-sign-compare',
                  '-Wno-unused-parameter', '-Wno-unknown-pragmas', '-Wno-deprecated-declarations', '-Wno-misleading-indentation',
                  '-fPIC', '-I', source, '-I', prefix / 'include', '-I', ssl / 'include', '-include', source / 'pq_cpu_locking_blas_meta.h',
                  '-Dcblas_sgemm=oglpq_cblas_sgemm', '-Dcblas_snrm2=oglpq_cblas_snrm2',
                  '-Dopenblas_get_config=pq_cpu_blas_immutable_config', '-Dopenblas_get_parallel=pq_cpu_blas_parallel_checked',
                  '-Dopenblas_get_num_threads=pq_cpu_blas_num_threads_checked']
        if args.asan:
            common += ['-fsanitize=address,undefined', '-fno-omit-frame-pointer', '-fno-pie', '-no-pie']
        implementation = [source / 'pq_cpu.cpp', source / 'pq_cpu_ms.cpp', source / 'pq_cpu_locking_blas_meta.cpp', library,
                          '-Wl,--exclude-libs,ALL', '-L', ssl / 'lib', '-lcrypto', '-pthread', '-lm']
        exe = build / 'stream-probe'
        command('compile-stream-probe', common + [source / 'pq_cpu_stream_probe.cpp'] + implementation + ['-o', exe])
        result['consumer_binary_sha256'] = sha(exe)
        fixtures = build / 'test-only-primitives'
        fixtures.mkdir()
        command('producer', [exe, 'produce', fixtures])
        producer = observed('producer')
        check('producer exited successfully before any consumer is launched', result['commands'][-1]['returncode'], 0)
        result['producer'] = producer
        result['consumers'] = []
        for case in producer['cases']:
            metric = case['metric']
            primitive = fixtures / ('metric-' + str(metric) + '.primitive')
            name = 'consumer-' + str(metric)
            command(name, [exe, 'consume', primitive, case['model_hash'], case['payload_hash']])
            item = observed(name)
            check('separate process reopens model/codes/' + str(metric), item['status'], 'pass')
            if metric == 3:
                check('cosine centers are not incorrectly required to be normalized', item['nonunit_cosine_centers'] > 0)
            item.update(primitive=str(primitive), primitive_sha256=sha(primitive))
            result['consumers'].append(item)
        fixture = fixtures / 'metric-1.primitive'
        command('primitive-negative-matrix', [exe, 'negatives', fixture])
        result['negative_matrix'] = observed('primitive-negative-matrix')
        check('all primitive/builder negative cases rejected', result['negative_matrix']['status'], 'pass')
        # The producer is gone. Corrupt the actual persisted primitive payload
        # and feed it to another consumer with the original external root.
        roots = producer['cases'][0]
        for mode in ['code-bit', 'center-bit', 'truncated', 'extra']:
            data = bytearray(fixture.read_bytes())
            if mode == 'code-bit':
                data[-1] ^= 1
            elif mode == 'center-bit':
                data[8 + 10 * 8 + 5 * 64] ^= 1
            elif mode == 'truncated':
                data.pop()
            else:
                data.append(0)
            bad = fixtures / ('corrupt-' + mode + '.primitive')
            bad.write_bytes(data)
            command('corrupt-file-' + mode, [exe, 'consume', bad, roots['model_hash'], roots['payload_hash']], expected='nonzero')
            check('persisted primitive corruption rejected/' + mode, 'PQ range/persistence probe failed:' in (out / ('corrupt-file-' + mode + '.stderr.log')).read_text())
        result['new_fault_injection'] = {}
        result['new_cancellation'] = {}
        for stage in ['restore', 'open', 'append-first', 'append-middle', 'append-last', 'raw-first', 'raw-middle', 'raw-last', 'finish']:
            name = 'new-allocation-' + stage + '-baseline'
            command(name, [exe, 'fault', stage, 0])
            baseline = observed(name)
            result['new_fault_injection'][stage] = baseline
            for point in range(1, baseline['calls'] + 1):
                name = 'new-allocation-' + stage + '-' + str(point)
                command(name, [exe, 'fault', stage, point])
                o = observed(name)
                check('real range/restore allocation failure/' + stage + '/' + str(point), [o['code'], o['outstanding_scalar'], o['outstanding_array']], [2, 0, 0])
            # First, middle, last poll per stage, with all polls for short
            # stages. This includes finish cancellation during full hashing.
            polls = baseline['polls']
            selected = list(range(1, polls + 1)) if polls <= 16 else sorted({1, 2, polls // 2, polls - 1, polls})
            result['new_cancellation'][stage] = dict(baseline_polls=polls, selected=selected)
            for point in selected:
                name = 'new-cancel-' + stage + '-' + str(point)
                command(name, [exe, 'cancel', stage, point])
                o = observed(name)
                check('range/restore cancellation does not publish/' + stage + '/' + str(point), [o['code'], o['outstanding_scalar'], o['outstanding_array']], [3, 0, 0])
        command('stream-million', [exe, 'million'])
        result['stream_million'] = observed('stream-million')
        check('million nodes encoded without borrowing a full input matrix', [result['stream_million']['nodes'], result['stream_million']['max_input_batch_rows'], result['stream_million']['full_input_matrix_allocated']], [1048576, 1024, False])
        check('cancel after multiple large payload hash blocks cannot publish', result['stream_million']['partial_large_hash_cancelled'])
        command('shape-boundaries', [exe, 'shapes'])
        result['shape_boundaries'] = observed('shape-boundaries')
        check('range/model reopening supports common3 and maximum1536 dimensions', result['shape_boundaries']['status'], 'pass')
        legacy = build / 'unchanged-legacy-probe'
        command('compile-unchanged-legacy-probe', common + [source / 'pq_cpu_probe.cpp'] + implementation + ['-o', legacy])
        result['legacy_consumer'] = {}
        for mode in ['numeric', 'extremes', 'million']:
            name = 'legacy-' + mode
            command(name, [legacy] + ([] if mode == 'numeric' else [mode]))
            result['legacy_consumer'][mode] = observed(name)
            check('unchanged legacy source rerun against evolved C boundary/' + mode, result['legacy_consumer'][mode]['status'], 'pass')
        result['legacy_fault_injection'] = {}
        for operation in ['train', 'encode', 'query', 'distance']:
            name = 'legacy-allocation-' + operation + '-baseline'
            command(name, [legacy, 'fault', operation, 0])
            baseline = observed(name)
            result['legacy_fault_injection'][operation] = baseline
            for point in range(1, baseline['calls'] + 1):
                name = 'legacy-allocation-' + operation + '-' + str(point)
                command(name, [legacy, 'fault', operation, point])
                o = observed(name)
                check('legacy actual allocation failure on evolved source/' + operation + '/' + str(point), [o['code'], o['outstanding_scalar'], o['outstanding_array']], [2, 0, 0])
        # Frozen concurrency probe, fresh binary and hashes. Does not mutate
        # or reattribute the accepted 602 historical suite.
        threaded = build / 'unchanged-thread-probe'
        command('compile-unchanged-thread-probe', common + [source / 'pq_cpu_locking_thread_probe.cpp'] + implementation + ['-o', threaded])
        command('shared-model-concurrency', [threaded, 'shared'])
        result['shared_model_concurrency'] = observed('shared-model-concurrency')
        check('same-codebook parallel results remain equal on evolved source', result['shared_model_concurrency']['status'], 'pass')
        check('API operations truly overlap', result['shared_model_concurrency']['peak_overlapping_api_operations'] >= 2)
        # C11 sees the additive public declarations with the old structs intact.
        client = build / 'c-client.c'
        client.write_text('#include "pq_cpu.h"\nint main(void){PqCpuBackendInfo b={0};PqCpuError e={0};PqCpuCodesBuilder *p=0;b.struct_size=sizeof(b);pq_cpu_codes_builder_close(&p);return pq_cpu_backend_info(&b,&e);}\n')
        obj = build / 'c-client.o'
        command('compile-c11-client', [gcc / 'gcc/bin/gcc', '-std=c11', '-Wall', '-Wextra', '-Werror', '-I', source, '-c', client, '-o', obj])
        command('link-c11-client', common + [obj] + implementation + ['-o', build / 'c-client'])
        command('run-c11-client', [build / 'c-client'])
        command('dynamic-dependencies', ['ldd', exe])
        deps = (out / 'dynamic-dependencies.stdout.log').read_text()
        check('no shared system BLAS or OpenMP loaded', 'openblas' not in deps.lower() and 'libgomp' not in deps and 'libiomp' not in deps)
        command('dynamic-symbols', ['nm', '-D', '--defined-only', exe])
        check('private BLAS symbols hidden', 'oglpq_' not in (out / 'dynamic-symbols.stdout.log').read_text())
        check('compiled inputs and historical receipts did not change during test', all(sha(p) == h for p, h in result['inputs'].items()))
        result['status'] = 'passed_independent_stream_restore_component_only'
    except BaseException as error:
        result['reason'] = str(error)
    result['finished_at'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    (out / 'result.json').write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps(dict(status=result['status'], assertions=len(result['assertions']), reason=result.get('reason'), result=str(out / 'result.json')), ensure_ascii=False))
    return 0 if result['status'].startswith('passed_') else 1


if __name__ == '__main__':
    raise SystemExit(main())
