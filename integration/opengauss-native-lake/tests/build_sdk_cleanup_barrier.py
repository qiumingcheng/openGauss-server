#!/usr/bin/env python3
"""Build, never install, a lifecycle-only barrier forwarding real SDK calls."""
import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

ROOT = Path('/home/qiumc/data/code/cpp')
DRIVER = ROOT / 'integration/opengauss-native-lake'
SOURCE = ROOT / '.worktrees/opengauss-native-lake'
sys.path[:0] = [str(DRIVER), str(DRIVER / 'tests')]
from file_dml_acceptance import check_build
from lake_harness import sha256, validate_run_id, write_json


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--run-id', required=True)
    p.add_argument('--build-receipt', type=Path, required=True)
    p.add_argument('--provider-sha256', required=True)
    args = p.parse_args()
    validate_run_id(args.run_id)
    out = ROOT / '公共文档/openGauss原生湖存储/验证' / args.run_id
    out.mkdir(exist_ok=False)
    result = {'status': 'failed', 'scope': 'Test-only barrier; all data, footer and actual abort calls delegate to the genuine SDK; not installed'}
    try:
        build = check_build(args.build_receipt)
        provider = ROOT / '.install/opengauss-native-lake-debug/lib/postgresql/og_lake_sdk/libog_lake_sdk_arrow.so'
        if sha256(provider) != args.provider_sha256:
            raise RuntimeError('Installed SDK is not the explicitly expected genuine provider')
        real = out / 'real-provider.so'
        shutil.copy2(provider, real)
        barrier = ROOT / '.runtime/opengauss-native-lake' / ('sdk-cleanup-barrier-' + args.run_id)
        barrier.mkdir(mode=0o700, exist_ok=False)
        write_json(barrier / 'owner.json', {'project': 'opengauss-native-lake', 'run_id': args.run_id,
                                          'build_receipt': str(out / 'build.json'), 'path': str(barrier)})
        source = Path(__file__).with_name('sdk_cleanup_barrier.c')
        shutil.copy2(source, out / source.name)
        shutil.copy2(__file__, out / 'executed-build.py')
        header = SOURCE / 'contrib/og_lake/sdk/lake_parquet_sdk.h'
        (out / 'barrier_config.h').write_text(
            '#define OG_LAKE_REAL_PROVIDER ' + json.dumps(str(real)) + '\n' +
            '#define OG_LAKE_BARRIER_DIR ' + json.dumps(str(barrier)) + '\n')
        compiler = ROOT / 'binarylibs/buildtools/gcc10.3/gcc/bin/gcc'
        gcc = compiler.parent.parent
        env = os.environ.copy()
        env['LD_LIBRARY_PATH'] = ':'.join(map(str, [gcc / 'lib64'] +
                                           [gcc.parent / name / 'lib' for name in ['isl', 'mpc', 'mpfr', 'gmp']]))
        library = out / 'libog_lake_sdk_cleanup_barrier.so'
        command = [str(compiler), '-std=c11', '-O2', '-fPIC', '-shared', '-fvisibility=hidden',
                   '-Wall', '-Wextra', '-I' + str(SOURCE / 'contrib/og_lake'), '-I' + str(out),
                   str(out / source.name), '-o', str(library), '-ldl', '-lpthread']
        with (out / 'compile.log').open('w') as log:
            subprocess.run(command, env=env, stdout=log, stderr=subprocess.STDOUT, check=True, timeout=90)
        exports = subprocess.check_output(['nm', '-D', '--defined-only', str(library)], text=True)
        if any(name not in exports for name in ['og_lake_parquet_sdk_v1', 'og_lake_parquet_sdk_v2']):
            raise RuntimeError('Lifecycle barrier must export both ABI entry points')
        (out / 'exports.txt').write_text(exports)
        if sha256(real) != args.provider_sha256:
            raise RuntimeError('Genuine provider copy differs')
        check_build(args.build_receipt)
        result.update(status='built_unloaded', library=str(library), library_sha256=sha256(library),
                      real_provider=str(real), real_provider_sha256=sha256(real), barrier_directory=str(barrier),
                      source=str(source), source_sha256=sha256(source), header=str(header), header_sha256=sha256(header),
                      driver_sha256=sha256(__file__), compiler_sha256=sha256(compiler), command=command,
                      native_source_head=build['source_head'], native_binary_sha256=build['binary_sha256'])
    except BaseException as exc:
        result['error'] = str(exc)
    finally:
        write_json(out / 'build.json', result)
    print(json.dumps({'status': result['status'], 'error': result.get('error'), 'receipt': str(out / 'build.json')}))
    return 0 if result['status'] == 'built_unloaded' else 1


if __name__ == '__main__':
    raise SystemExit(main())
