#!/usr/bin/env python3
"""Build a test wrapper that imports the actual native Arrow implementation.

The wrapper contains no Arrow builder implementation. A matching official
native build receipt and exported builder symbols are mandatory.
"""
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
from lake_harness import sha256, write_json


def builder_symbols(path, prefixes=('LakeArrowBuilder',)):
    process = subprocess.Popen(['nm', '-D', '-C', str(path)], text=True,
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    selected = [line.strip() for line in process.stdout if any(name in line for name in prefixes)]
    error = process.stderr.read()
    if process.wait():
        raise RuntimeError('Cannot inspect native builder symbols: ' + error)
    return selected


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--build-receipt', type=Path, required=True)
    parser.add_argument('--evidence', type=Path, required=True)
    parser.add_argument('--kind', choices=['arrow', 'candidate'], default='arrow')
    args = parser.parse_args()
    output = args.evidence.resolve()
    output.mkdir(exist_ok=False)
    result = {'status': 'failed', 'scope': 'Test-only wrapper imports actual product Arrow builder; no product SQL API'}
    try:
        build = check_build(args.build_receipt)
        compiler = ROOT / 'binarylibs/buildtools/gcc10.3/gcc/bin/g++'
        gcc = compiler.parent.parent
        env = os.environ.copy()
        env['GAUSSHOME'] = str(PREFIX)
        env['LD_LIBRARY_PATH'] = ':'.join(map(str, [PREFIX / 'lib', gcc / 'lib64'] +
                                              [gcc.parent / p / 'lib' for p in ['isl', 'mpc', 'mpfr', 'gmp']]))

        def config(option):
            return subprocess.check_output([str(PREFIX / 'bin/pg_config'), option], env=env, text=True).strip()

        stem = 'native_arrow_writer_probe' if args.kind == 'arrow' else 'native_candidate_probe'
        source = Path(__file__).with_name(stem + '.cpp')
        shutil.copy2(source, output / source.name)
        shutil.copy2(__file__, output / 'executed-build.py')
        flags = shlex.split(config('--cppflags')) + shlex.split(config('--cflags'))
        library = output / (stem + '.so')
        command = [str(compiler), *flags, '-fPIC', '-shared', '-I' + config('--includedir-server'),
                   '-I' + str(SOURCE / 'contrib/og_lake'), '-I' + str(SOURCE / 'src/include'),
                   '-MD', '-MF', str(output / 'dependencies.d'), str(output / source.name), '-o', str(library)]
        result.update(command=command, build_receipt=str(args.build_receipt.resolve()),
                      build_receipt_sha256=sha256(args.build_receipt), source_head=build['source_head'],
                      native_binary_sha256=build['binary_sha256'], source_sha256=sha256(source),
                      compiler_sha256=sha256(compiler), driver_sha256=sha256(__file__))
        with (output / 'compile.log').open('w') as log:
            subprocess.run(command, env=env, stdout=log, stderr=subprocess.STDOUT, check=True, timeout=120)
        dependencies = (output / 'dependencies.d').read_text().replace('\\\n', ' ')
        result['headers'] = {str(Path(p).resolve()): sha256(p)
                             for p in shlex.split(dependencies.split(':', 1)[1])
                             if Path(p).suffix in ['.h', '.hpp']}
        prefixes = ('LakeArrowBuilder', 'LakeFileCandidate')
        imports = builder_symbols(library, prefixes)
        exports = builder_symbols(PREFIX / 'bin/gaussdb', prefixes)
        names = ['LakeArrowBuilder' + name for name in ['Create(', 'Append(', 'Write(', 'Destroy(']]
        if args.kind == 'arrow':
            names.append('LakeArrowBuilderReset(')
        else:
            names += ['LakeFileCandidateOpen(', 'LakeFileCandidateFinish(', 'LakeFileCandidatePublish(']
        if (any(' T ' in line for line in imports) or
                any(not any(' U ' in (' ' + line) and name in line for line in imports)
                    for name in names) or
                any(not any(' T ' in line and name in line for line in exports)
                    for name in names)):
            raise RuntimeError('Wrapper must import every builder operation from the actual native binary')
        check_build(args.build_receipt)
        result.update(status='built_unloaded', library=str(library), library_sha256=sha256(library),
                      wrapper_imports=imports, native_exports=exports)
    except BaseException as exc:
        result['error'] = str(exc)
    finally:
        write_json(output / 'result.json', result)
    print(json.dumps({'status': result['status'], 'error': result.get('error'),
                      'receipt': str(output / 'result.json')}))
    return 0 if result['status'] == 'built_unloaded' else 1


if __name__ == '__main__':
    raise SystemExit(main())
