#!/usr/bin/env python3
"""Build an unloaded test wrapper importing actual native watermark symbols."""
import argparse
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import sys

ROOT = Path('/home/qiumc/data/code/cpp')
DRIVER = ROOT / 'integration/opengauss-native-lake'
SOURCE = ROOT / '.worktrees/opengauss-native-lake'
PREFIX = ROOT / '.install/opengauss-native-lake-debug'
sys.path[:0] = [str(DRIVER), str(DRIVER / 'tests')]
from file_dml_acceptance import check_build
from lake_harness import sha256, validate_run_id, write_json
from build_native_arrow_probe import builder_symbols


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--run-id', required=True)
    p.add_argument('--build-receipt', type=Path, required=True)
    args = p.parse_args()
    validate_run_id(args.run_id)
    out = ROOT / '公共文档/openGauss原生湖存储/验证' / args.run_id
    out.mkdir(exist_ok=False)
    result = {'status': 'failed', 'scope': 'Actual native GetOldestXmin/TransactionIdPrecedes wrapper only; not loaded; no GC'}
    try:
        build = check_build(args.build_receipt)
        compiler = ROOT / 'binarylibs/buildtools/gcc10.3/gcc/bin/g++'
        gcc = compiler.parent.parent
        env = os.environ.copy()
        env['GAUSSHOME'] = str(PREFIX)
        env['LD_LIBRARY_PATH'] = ':'.join(map(str, [PREFIX / 'lib', gcc / 'lib64'] +
                                           [gcc.parent / name / 'lib' for name in ['isl', 'mpc', 'mpfr', 'gmp']]))

        def config(option):
            return subprocess.check_output([str(PREFIX / 'bin/pg_config'), option], text=True, env=env).strip()

        source = Path(__file__).with_name('native_gc_xmin_probe.cpp')
        shutil.copy2(source, out / source.name)
        shutil.copy2(__file__, out / 'executed-build.py')
        library = out / 'native_gc_xmin_probe.so'
        command = [str(compiler), *shlex.split(config('--cppflags')), *shlex.split(config('--cflags')),
                   '-fPIC', '-shared', '-I' + config('--includedir-server'),
                   '-I' + str(SOURCE / 'src/include'), '-MD', '-MF', str(out / 'dependencies.d'),
                   str(out / source.name), '-o', str(library)]
        with (out / 'compile.log').open('w') as log:
            subprocess.run(command, env=env, stdout=log, stderr=subprocess.STDOUT, check=True, timeout=120)
        # This fork defines TransactionIdPrecedes as a full-width comparison
        # macro in native c.h, not an exported function. The watermark must
        # import real backend code; comparison comes from the hashed header.
        names = ('GetOldestXmin(',)
        imports = builder_symbols(library, names)
        exports = builder_symbols(PREFIX / 'bin/gaussdb', names)
        if any(not any(' U ' in (' ' + line) and name in line for line in imports) for name in names):
            raise RuntimeError('Probe does not import every real native function')
        if any(' T ' in (' ' + line) for line in imports):
            raise RuntimeError('Probe may not contain a copied native watermark implementation')
        if any(not any(' T ' in (' ' + line) and name in line for line in exports) for name in names):
            raise RuntimeError('Actual native binary does not export the required functions')
        dependencies = (out / 'dependencies.d').read_text().replace('\\\n', ' ')
        headers = {str(Path(name).resolve()): sha256(name)
                   for name in shlex.split(dependencies.split(':', 1)[1]) if Path(name).suffix in ['.h', '.hpp']}
        check_build(args.build_receipt)
        result.update(status='built_unloaded', source_head=build['source_head'], native_binary_sha256=build['binary_sha256'],
                      library=str(library), library_sha256=sha256(library), source_sha256=sha256(source),
                      driver_sha256=sha256(__file__), headers=headers, command=command,
                      comparison_contract='Native c.h TransactionIdPrecedes macro; no copied comparator implementation',
                      actual_native_imports=imports, actual_native_exports=exports)
    except BaseException as exc:
        result['error'] = str(exc)
    finally:
        write_json(out / 'build.json', result)
    print(json.dumps({'status': result['status'], 'error': result.get('error'), 'receipt': str(out / 'build.json')}))
    return 0 if result['status'] == 'built_unloaded' else 1


if __name__ == '__main__':
    raise SystemExit(main())
