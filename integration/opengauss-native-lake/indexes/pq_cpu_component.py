#!/usr/bin/env python3
"""Isolated C boundary for actual locked Microsoft CPU PQ numerical kernels.

Never compiles/starts Native or changes SDK/reference/wrapper inputs. Tests are
serialized, single-process consumers with serial OpenBLAS. Thread safety, BLAS
allocator accounting and whole-query budgets are not production-qualified.
"""
import argparse
import ctypes
import datetime
import difflib
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
    parser.add_argument('--sanitize', action='store_true')
    args = parser.parse_args()
    if not re.fullmatch(r'[A-Za-z0-9_.-]+', args.run_id):
        parser.error('new simple run-id required')
    out = ROOT / '公共文档/openGauss原生湖存储/验证' / args.run_id
    out.mkdir(exist_ok=False)
    build = ROOT / '.build/opengauss-native-lake/pq-cpu-boundary' / args.run_id
    build.mkdir(exist_ok=False)
    source = Path(__file__).resolve().parent
    original = ROOT / '.reference/opengauss-native-lake/diskann-pq-cpu-reference'
    baseline = ROOT / '.build/opengauss-native-lake/pq-cpu-component/pq_cpu_openblas_component_1109_01/diskann_pq_extract.cpp'
    extraction = baseline.with_name('extraction.json')
    files = [source / name for name in ['pq_cpu.h', 'pq_cpu_internal.h', 'pq_cpu.cpp', 'pq_cpu_ms.cpp', 'pq_cpu_probe.cpp', 'pq_cpu_component.py']]
    files += [original / name for name in ['LICENSE', 'src/pq.cpp', 'src/math_utils.cpp', 'include/math_utils.h', 'include/utils.h']]
    files += [baseline, extraction, Path('/usr/lib64/libopenblas-r0.3.18.so'), Path('/usr/include/openblas/cblas.h'), Path('/usr/include/openblas/openblas_config.h')]
    gcc = ROOT / 'binarylibs/buildtools/gcc10.3'
    ssl = ROOT / 'binarylibs/kernel/dependency/openssl/comm'
    env = dict(os.environ)
    env['LD_LIBRARY_PATH'] = ':'.join([str(ssl / 'lib')] + [str(gcc / p) for p in ['gcc/lib64', 'isl/lib', 'mpc/lib', 'mpfr/lib', 'gmp/lib']])
    env['OMP_NUM_THREADS'] = '1'
    env['OPENBLAS_NUM_THREADS'] = '1'
    if args.sanitize:
        env['ASAN_OPTIONS'] = 'detect_leaks=1:halt_on_error=1'
        env['UBSAN_OPTIONS'] = 'halt_on_error=1:print_stacktrace=1'
    result = dict(status='failed', scope=__doc__, assertions=[], sanitizer=args.sanitize,
                  inputs={str(p): sha(p) for p in files}, build=str(build),
                  started_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
                  limitations=['Serial isolated component only; no Native graph/query, Parquet model persistence or production thread safety.',
                               'Original training matrix N*256 remains and is explicitly charged; training cap256..1024.',
                               'Borrowed input, allocator rounding/bookkeeping and BLAS allocations are outside the component workspace estimate.',
                               'Original random_device seed remains nondeterministic; immutable model/code hashes bind actual output.',
                               'BLAS calls cannot be interrupted internally; cancellation polls surround bounded work.'])
    archive = out / 'executed-inputs'
    archive.mkdir()
    for p in files:
        label = p.name if p.parent == source else 'original-' + p.name if original in p.parents else 'dependency-' + p.name
        shutil.copy2(p, archive / label)
    (out / 'microsoft-raii-poll.patch').write_text(''.join(difflib.unified_diff(baseline.read_text().splitlines(True), (source / 'pq_cpu_ms.cpp').read_text().splitlines(True),
                                                                            fromfile='locked-Microsoft-original-extract.cpp', tofile='pq_cpu_ms.cpp')))

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
            p = subprocess.run(list(map(str, argv)), env=env, stdout=so, stderr=se, timeout=timeout, preexec_fn=no_core)
        if expected and p.returncode:
            raise RuntimeError(name + ' failed, see ' + str(out / (name + '.stderr.log')))
        return p.returncode

    try:
        check('original reference remains exact locked commit', subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=original, text=True).strip(), '78256bbab4685e1774e78d331e081a153be26823')
        manifest = json.loads(extraction.read_text())
        check('exact original function extraction identity', sha(baseline), manifest['output_sha256'])
        check('all upstream files equal recorded primary source SHA', all(sha(p) == h for p, h in manifest['source_files_sha256'].items()))
        exe = build / 'pq-cpu-probe'
        flags = ['-std=c++14', '-O0', '-g', '-Wall', '-Wextra', '-Werror', '-Wno-sign-compare', '-Wno-unused-parameter',
                 '-Wno-unknown-pragmas', '-Wno-deprecated-declarations', '-Wno-misleading-indentation']
        if args.sanitize:
            flags += ['-fsanitize=address,undefined', '-fno-omit-frame-pointer', '-fno-pie', '-no-pie']
        common = [gcc / 'gcc/bin/g++'] + flags + ['-I', source, '-I/usr/include/openblas', '-I', ssl / 'include']
        implementation = [source / 'pq_cpu.cpp', source / 'pq_cpu_ms.cpp', '/usr/lib64/libopenblas-r0.3.18.so', '-L', ssl / 'lib', '-lcrypto']
        command('compile', common + [source / 'pq_cpu_probe.cpp'] + implementation + ['-o', exe])
        result['executable_sha256'] = sha(exe)
        command('numeric-boundaries-cancellation', [exe])
        observed = json.loads((out / 'numeric-boundaries-cancellation.stdout.log').read_text())
        check('C-boundary numeric/error/identity/cancellation consumer', observed['status'], 'pass')
        check('supports d1, ordinary d3, max d1536 and L2/IP/cosine', [(c['dimension'], c['metric']) for c in observed['cases']], [(1, 1), (3, 1), (3, 1), (8, 2), (32, 3), (1536, 1)])
        check('upper training sample cap exercised', max(c['training_rows'] for c in observed['cases']), 1024)
        result['consumer_checks'] = observed['checks']
        result['numeric'] = observed
        command('million-node-low-M', [exe, 'million'])
        million = json.loads((out / 'million-node-low-M.stdout.log').read_text())
        check('maximum ANN node count actually encodes with low-M budget', [million['status'], million['nodes']], ['pass', 1048576])
        result['million_node_checks'] = million['checks']
        result['million_node'] = million
        command('finite-extremes-degenerate', [exe, 'extremes'])
        extremes = json.loads((out / 'finite-extremes-degenerate.stdout.log').read_text())
        check('finite1e30 rejected before float overflow', extremes['finite_1e30_training'], 'INVALID')
        check('all-zero/subnormal/duplicate inputs complete for L2/IP', len(extremes['cases']), 6)
        check('zero cosine explicitly rejected', extremes['zero_cosine_training'], 'INVALID')
        result['extreme_checks'] = extremes['checks']
        result['extremes'] = extremes
        c = build / 'c-consumer.c'
        c.write_text('#include "pq_cpu.h"\nint main(void){PqCpuModel* p=0;PqCpuError e={0};PqCpuLimits l={sizeof(l),67108864,0,0};PqCpuTrainSpec s={sizeof(s),1,3,0,1,256,"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"};int r=pq_cpu_train(&s,&l,0,0,&p,&e);pq_cpu_model_close(&p);pq_cpu_model_close(&p);return r==PQ_CPU_INVALID&&e.code==r&&p==0?0:1;}\n')
        command('compile-c11-consumer', [gcc / 'gcc/bin/gcc', '-std=c11', '-Wall', '-Wextra', '-Werror', '-I', source, '-c', c, '-o', build / 'c-consumer.o'])
        command('link-c11-consumer', common + [build / 'c-consumer.o'] + implementation + ['-o', build / 'c-consumer'])
        command('run-c11-consumer', [build / 'c-consumer'])
        check('actual separately compiled C11 caller links/calls C++ guarded implementation', True)
        shutil.copy2(c, archive / c.name)
        result['c_consumer_sha256'] = sha(c)
        command('dynamic-dependencies', ['ldd', exe])
        dependencies = (out / 'dynamic-dependencies.stdout.log').read_text()
        check('OpenMP runtime not linked', 'libgomp' not in dependencies and 'libiomp' not in dependencies)
        result['fault_injection'] = {}
        for operation in ['train', 'encode', 'query', 'distance']:
            name = 'fault-' + operation + '-baseline'
            command(name, [exe, 'fault', operation, 0])
            baseline_result = json.loads((out / (name + '.stdout.log')).read_text())
            check('all baseline allocations released/' + operation, baseline_result['outstanding'], 0)
            result['fault_injection'][operation] = dict(allocation_points=baseline_result['calls'], baseline=baseline_result)
            for point in range(1, baseline_result['calls'] + 1):
                name = 'fault-' + operation + '-' + str(point)
                command(name, [exe, 'fault', operation, point])
                fault = json.loads((out / (name + '.stdout.log')).read_text())
                check('real C++ allocation failure/' + operation + '/' + str(point), [fault['status'], fault['failures'], fault['code'], fault['outstanding'], fault['outstanding_scalar'], fault['outstanding_array']], ['pass', 1, 2, 0, 0, 0])
            check('scalar/array allocation kinds counted/' + operation, baseline_result['scalar_calls'] + baseline_result['array_calls'], baseline_result['calls'])
        check('all compiled source/dependency inputs unchanged', all(sha(p) == h for p, h in result['inputs'].items()))
        check('reference tracked files remain clean', subprocess.check_output(['git', 'status', '--porcelain', '--untracked-files=no'], cwd=original, text=True).strip(), '')
        result.update(status='passed_isolated_c_boundary_only', patch_sha256=sha(out / 'microsoft-raii-poll.patch'))
    except BaseException as error:
        result['reason'] = str(error)
    result['finished_at'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    (out / 'result.json').write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps(dict(status=result['status'], assertions=len(result['assertions']), consumer_checks=result.get('consumer_checks'), reason=result.get('reason'), result=str(out / 'result.json')), ensure_ascii=False))
    return 0 if result['status'].startswith('passed_') else 1


if __name__ == '__main__':
    raise SystemExit(main())
