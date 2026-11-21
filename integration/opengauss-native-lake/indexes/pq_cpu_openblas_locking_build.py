#!/usr/bin/env python3
"""Provisional isolated OpenBLAS0.3.18 serial+locking static PIC dependency.

Does not update source-lock, system packages, frozen PQ v1, Native or SDK.
An upstream checkout is archived into an owned build tree before compilation.
"""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tarfile

ROOT = Path('/home/qiumc/data/code/cpp')
COMMIT = 'efe42481e2c85e8197bdf533078bf89bdf6eabf4'
PREFIX = 'oglpq_'


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-id', required=True)
    args = parser.parse_args()
    if not re.fullmatch(r'[A-Za-z0-9_.-]+', args.run_id) or args.run_id in ['.', '..']:
        parser.error('new simple run-id required')
    source = ROOT / '.reference/opengauss-native-lake/openblas-pq-locking-v0.3.18'
    out = ROOT / '公共文档/openGauss原生湖存储/验证' / args.run_id
    build = ROOT / '.build/opengauss-native-lake/openblas-locking' / args.run_id
    prefix = ROOT / '.install/opengauss-native-lake-pq-openblas' / args.run_id
    out.mkdir(exist_ok=False)
    build.mkdir(parents=True, exist_ok=False)
    prefix.mkdir(parents=True, exist_ok=False)
    owner = dict(task='native-lake-pq-openblas-serial-locking', run_id=args.run_id,
                 source=str(source), source_commit=COMMIT, build=str(build), prefix=str(prefix),
                 created_at=datetime.datetime.now(datetime.timezone.utc).isoformat())
    for folder in [build, prefix]:
        (folder / '.og-lake-owner.json').write_text(json.dumps(owner, ensure_ascii=False, indent=2) + '\n')
    gcc = ROOT / 'binarylibs/buildtools/gcc10.3'
    env = dict(os.environ)
    env['LD_LIBRARY_PATH'] = ':'.join(str(gcc / p) for p in ['gcc/lib64', 'isl/lib', 'mpc/lib', 'mpfr/lib', 'gmp/lib'])
    env['OMP_NUM_THREADS'] = '1'
    env['OPENBLAS_NUM_THREADS'] = '1'
    result = dict(status='failed', scope=__doc__, owner=owner, driver_sha256=sha(__file__),
                  assertions=[], commands=[], source=str(source), source_commit=COMMIT,
                  prefix=str(prefix), build=str(build), symbol_prefix=PREFIX,
                  source_lock_updated=False, provisional=True)

    def check(name, actual, expected=True):
        ok = actual == expected
        result['assertions'].append(dict(name=name, observed=actual, expected=expected, status='pass' if ok else 'fail'))
        if not ok:
            raise AssertionError(name)

    def command(name, argv, cwd=build, timeout=1800):
        result['commands'].append(dict(name=name, cwd=str(cwd), argv=list(map(str, argv))))
        with (out / (name + '.log')).open('w') as log:
            subprocess.run(list(map(str, argv)), cwd=cwd, env=env, stdout=log, stderr=subprocess.STDOUT, timeout=timeout, check=True)

    try:
        check('official source commit matches v0.3.18 tag', subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=source, text=True).strip(), COMMIT)
        check('source tracked clean before build', subprocess.check_output(['git', 'status', '--porcelain', '--untracked-files=no'], cwd=source, text=True).strip(), '')
        archive = build / 'OpenBLAS-v0.3.18-source.tar'
        command('source-archive', ['git', 'archive', '--format=tar', '-o', archive, COMMIT], cwd=source)
        result['source_archive_sha256'] = sha(archive)
        copied = build / 'source'
        copied.mkdir()
        with tarfile.open(archive) as tar:
            for member in tar.getmembers():
                if Path(member.name).is_absolute() or '..' in Path(member.name).parts:
                    raise ValueError('unsafe source archive member')
            tar.extractall(copied)
        variables = ['CC=' + str(gcc / 'gcc/bin/gcc'), 'HOSTCC=' + str(gcc / 'gcc/bin/gcc'),
                     'AR=/usr/bin/ar', 'RANLIB=/usr/bin/ranlib', 'OBJCOPY=/usr/bin/objcopy',
                     'TARGET=PRESCOTT', 'BINARY=64', 'DYNAMIC_ARCH=0', 'USE_THREAD=0', 'USE_LOCKING=1',
                     'USE_OPENMP=0', 'NUM_THREADS=1', 'NUM_PARALLEL=1', 'NO_AFFINITY=1',
                     'NOFORTRAN=1', 'ONLY_CBLAS=1', 'NO_LAPACK=1', 'NO_LAPACKE=1',
                     'NO_SHARED=1', 'NO_STATIC=0', 'NEED_PIC=1', 'BUILD_SINGLE=1', 'BUILD_DOUBLE=0',
                     'BUILD_COMPLEX=0', 'BUILD_COMPLEX16=0', 'BUILD_BFLOAT16=0',
                     'SYMBOLPREFIX=' + PREFIX, 'COMMON_OPT=-O0 -g -fPIC -fvisibility=hidden']
        result['make_variables'] = variables
        result['compiler'] = str(gcc / 'gcc/bin/gcc')
        result['compiler_sha256'] = sha(gcc / 'gcc/bin/gcc')
        command('build-libs', ['make', '-j1', 'libs'] + variables, cwd=copied)
        stage = build / 'stage'
        command('install-staging', ['make', '-j1', 'install', 'PREFIX=' + str(stage)] + variables, cwd=copied)
        make_log = (out / 'build-libs.log').read_text()
        check('actual compile command enables locking', '-DUSE_LOCKING' in make_log)
        check('actual compile command emits PIC', '-fPIC' in make_log)
        check('actual compile command hides C symbols', '-fvisibility=hidden' in make_log)
        check('no OpenMP compiler option', '-fopenmp' not in make_log)
        check('no internal SMP_SERVER compiler option', '-DSMP_SERVER' not in make_log)
        raw_archives = [p for p in (stage / 'lib').glob('*.a') if not p.is_symlink()]
        check('one actual static library installed to staging', len(raw_archives), 1)
        raw = raw_archives[0]
        symbols = subprocess.check_output(['/usr/bin/nm', '-g', '--defined-only', '--format=posix', raw], text=True)
        (out / 'raw-defined-symbols.txt').write_text(symbols)
        owned = set()
        for line in symbols.splitlines():
            parts = line.split()
            if len(parts) >= 2 and re.fullmatch(r'[A-Za-z_.$][A-Za-z0-9_.$@]*', parts[0]) and re.fullmatch(r'[A-Za-z]', parts[1]):
                owned.add(parts[0])
        check('raw archive defines actual CBLAS kernels', {'cblas_sgemm', 'cblas_snrm2', 'openblas_get_config'}.issubset(owned))
        mapping = build / 'owned-symbols.map'
        mapping.write_text(''.join(s + ' ' + PREFIX + s + '\n' for s in sorted(owned) if not s.startswith(PREFIX)))
        libdir = prefix / 'lib'
        libdir.mkdir()
        library = libdir / 'libog_lake_pq_openblas.a'
        command('prefix-owned-symbols', ['/usr/bin/objcopy', '--redefine-syms=' + str(mapping), raw, library])
        command('archive-index', ['/usr/bin/ranlib', library])
        shutil.copytree(stage / 'include', prefix / 'include')
        names = subprocess.check_output(['/usr/bin/nm', '-g', '--defined-only', '--format=posix', library], text=True)
        (out / 'prefixed-defined-symbols.txt').write_text(names)
        remaining = []
        for line in names.splitlines():
            parts = line.split()
            if len(parts) >= 2 and re.fullmatch(r'[A-Za-z]', parts[1]) and not parts[0].startswith(PREFIX):
                remaining.append(parts[0])
        check('every owned global definition has private prefix', sorted(set(remaining)), [])
        check('installed header uses actual prefixed CBLAS API', PREFIX + 'cblas_sgemm' in (prefix / 'include/cblas.h').read_text())
        check('BLAS is LP64', '#define OPENBLAS_USE64BITINT' not in (prefix / 'include/openblas_config.h').read_text())
        licenses = prefix / 'licenses'
        licenses.mkdir()
        license_paths = subprocess.check_output(['git', 'ls-files', '*LICENSE*', '*COPYING*'], cwd=source, text=True).splitlines()
        for name in license_paths:
            target = licenses / name.replace('/', '__')
            shutil.copy2(source / name, target)
        common = (source / 'common.h').read_text()
        (licenses / 'GotoBLAS-common-header-notice.txt').write_text(common[:common.index('#ifndef COMMON_H')])
        # A link projection verifies static PIC and final archive hiding. It is
        # an audit artifact, not an installed shared provider or a fake backend.
        projection = build / 'hidden-link-projection.so'
        command('pic-hidden-link-projection', [gcc / 'gcc/bin/gcc', '-shared', '-Wl,--whole-archive', library,
                                                '-Wl,--no-whole-archive', '-Wl,--exclude-libs,ALL', '-pthread', '-lm', '-o', projection])
        exported = subprocess.check_output(['/usr/bin/nm', '-D', '--defined-only', projection], text=True)
        (out / 'projection-dynamic-symbols.txt').write_text(exported)
        check('projection exposes no private BLAS dynamic symbols', PREFIX not in exported)
        dynamic = subprocess.check_output(['readelf', '-d', projection], text=True)
        check('PIC link projection has no TEXTREL', 'TEXTREL' not in dynamic)
        check('reference still tracked clean', subprocess.check_output(['git', 'status', '--porcelain', '--untracked-files=no'], cwd=source, text=True).strip(), '')
        result.update(status='built_provisional_unlinked', library=str(library), library_sha256=sha(library),
                      owned_symbols=len(owned), symbol_map_sha256=sha(mapping),
                      files={str(p.relative_to(prefix)): sha(p) for p in prefix.rglob('*') if p.is_file()},
                      source_license=dict(file='LICENSE', sha256=sha(source / 'LICENSE'), spdx='BSD-3-Clause; GotoBLAS component notices preserved'),
                      hidden_link_policy='C compilation visibility hidden; every owned global is prefixed; final link must retain --exclude-libs,ALL as audited by the PIC projection',
                      caveats=['OpenBLAS0.3.18 raw get_config returns a mutable static buffer; numeric USE_LOCKING does not protect it.',
                               'Concurrency requires the separate once-initialized immutable-config shim variant, not old serial PQ v1 unchanged behavior.',
                               'No Native SMP or whole-query allocator qualification.'])
    except BaseException as error:
        result['reason'] = str(error)
    result['finished_at'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    (out / 'build.json').write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps(dict(status=result['status'], reason=result.get('reason'), manifest=str(out / 'build.json')), ensure_ascii=False))
    return 0 if result['status'] == 'built_provisional_unlinked' else 1


if __name__ == '__main__':
    raise SystemExit(main())
