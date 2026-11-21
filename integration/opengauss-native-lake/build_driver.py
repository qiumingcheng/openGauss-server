#!/usr/bin/env python3
"""Isolated lake build using the workspace's authoritative build scripts."""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import socket
import stat
import subprocess
import sys
import tempfile
from native_runtime import source_for_profile
from pq_native_build_inputs import blas_inputs, global_symbols, pq_inputs, verify_pq_symbols

SDK_HEADER_SHA256 = {
    'sdk/lake_parquet_sdk.h': '6dbe78174680de7830ed45cfee957fefa38fc3d5dda0429b04e99436da28e759',
    'vendor/arrow20/abi.h': '1828f1ce60368cd590d19595910fa19db09b30728f8a43b5f89933623f7a7276',
}
MS_DISKANN_LICENSE_SHA256 = 'c7d6393d0618d992594942d9bc91b520cb1a4d92ea2bbd26b31f1eae56f87ce0'
IN_TREE_PQ_OWNED_FILES = (
    'pq_cpu.h', 'pq_cpu_internal.h', 'pq_cpu.cpp',
    'pq_cpu_locking_blas_meta.h', 'pq_cpu_locking_blas_meta.cpp',
    'pq_parquet_format.h', 'pq_parquet_format.cpp',
    'pq_parquet_frontier_draft.h', 'pq_parquet_frontier_draft.cpp',
)


def pq_in_tree_inputs(workspace, source, blas_receipt=None, blas_receipt_sha256=None):
    """Validate in-tree contrib/og_lake/{pq,vendor/diskann_pq} + private OpenBLAS."""
    blas = blas_inputs(workspace, receipt=blas_receipt, receipt_sha256=blas_receipt_sha256) if blas_receipt_sha256 else blas_inputs(workspace)
    if blas_receipt is not None and Path(blas_receipt).resolve(strict=True) != Path(blas['receipt']).resolve(strict=True):
        raise RuntimeError('In-tree PQ --blas-build must match pinned serial locking OpenBLAS receipt')
    pq_dir = source / 'contrib/og_lake/pq'
    vendor_dir = source / 'contrib/og_lake/vendor/diskann_pq'
    for name in IN_TREE_PQ_OWNED_FILES + ('Makefile', 'CMakeLists.txt', 'isolate_pq_symbols.sh'):
        if not (pq_dir / name).is_file():
            raise RuntimeError('Missing in-tree PQ file: contrib/og_lake/pq/' + name)
    for name in ('pq_cpu_ms.cpp', 'LICENSE', 'NOTICE.txt', 'README.md'):
        if not (vendor_dir / name).is_file():
            raise RuntimeError('Missing in-tree vendor PQ file: contrib/og_lake/vendor/diskann_pq/' + name)
    if digest(vendor_dir / 'LICENSE') != MS_DISKANN_LICENSE_SHA256:
        raise RuntimeError('Microsoft DiskANN LICENSE mismatch in contrib/og_lake/vendor/diskann_pq/LICENSE')
    sdk_headers = {}
    for rel, expected_sha in SDK_HEADER_SHA256.items():
        p = source / 'contrib/og_lake' / rel
        if not p.is_file() or digest(p) != expected_sha:
            raise RuntimeError('In-tree SDK ABI header mismatch: ' + rel)
        sdk_headers[rel] = digest(p)
    owned_sources = {name: digest(pq_dir / name) for name in IN_TREE_PQ_OWNED_FILES}
    vendor_sources = {name: digest(vendor_dir / name) for name in ('pq_cpu_ms.cpp', 'LICENSE', 'NOTICE.txt')}
    blas_prefix = Path(blas['prefix']).resolve(strict=True)
    return dict(
        dependency_kind='pq_in_tree_v1',
        source_dir=str(pq_dir),
        include=str(pq_dir),
        library=str(pq_dir / 'libog_lake_pq_cpu.a'),
        blas_library=blas['library'],
        blas_library_sha256=blas['library_sha256'],
        blas_include=str(blas_prefix / 'include'),
        blas_licenses=str(blas_prefix / 'licenses'),
        blas_inputs=blas,
        owned_sources=owned_sources,
        vendor_sources=vendor_sources,
        sdk_headers=sdk_headers,
    )

ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT / '.worktrees/opengauss-native-lake'
BASELINE = 'ba1e4ad2f99295b1b0f65e3880a05cb75a1121d4'
ROARING_COMMIT = '2e8395f1dbf286d7944a7276195a7c40cbcbfd4a'
ROARING_PATCH = '291aca7744394c6658dd2316972f6cc465f04461c0dec7c4a5c5885ee4e2441f'
ROARING_PATCHED_SOURCE = '73e4b686e9f9f8de0be37f38c6224c382e78d013d74c47ab84dc78798710b3f8'
ROARING_LICENSE = 'f8e1f16a3f22e001b44ae9b12625420b9a0c4dbb7ec0e9f83b6a3d38a9525411'


def digest(path):
    h = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def roaring_inputs(receipt):
    """Admit the qualified run-preallocation variant, not SDK-owned symbols.

    Re-read this complete identity after the build.  A manifest alone does not
    prove that its archive, installed headers or source patch stayed unchanged.
    """
    if receipt is None:
        return None
    receipt = receipt.resolve(strict=True)
    dep = json.loads(receipt.read_text())
    if (dep.get('status') != 'built_unlinked' or
            dep.get('source_commit') != ROARING_COMMIT or
            dep.get('source_patch_sha256') != ROARING_PATCH or
            dep.get('patched_source_sha256') != ROARING_PATCHED_SOURCE or
            dep.get('source_license', {}).get('sha256') != ROARING_LICENSE):
        raise RuntimeError('CRoaring requires the qualified pinned portable-run OOM fix')
    prefix = Path(dep['prefix']).resolve(strict=True)
    library = Path(dep['library']).resolve(strict=True)
    include = prefix / 'include'
    # These paths become Make flags/recipe inputs. Do not interpret shell/Make
    # metacharacters supplied through an otherwise syntactically valid JSON.
    for path in (prefix, library, include):
        if not re.fullmatch(r'/[A-Za-z0-9_./+-]+', str(path)):
            raise RuntimeError('CRoaring build paths must be absolute Make-safe paths')
    if library.name != 'libroaring.a' or library.parent.parent != prefix:
        raise RuntimeError('CRoaring must be a private static archive inside its owned prefix')
    owner_path = prefix / 'owner.json'
    if digest(owner_path) != dep['owner_sha256']:
        raise RuntimeError('CRoaring owner identity mismatch')
    owner = json.loads(owner_path.read_text())
    files = dep['files']
    if owner.get('prefix') != str(prefix) or owner.get('files') != files:
        raise RuntimeError('CRoaring owner/file manifest mismatch')
    actual_names = {str(p.relative_to(prefix)) for p in prefix.rglob('*') if p.is_file()}
    if actual_names != set(files) | {'owner.json'}:
        raise RuntimeError('CRoaring installed file inventory mismatch')
    for name, expected in files.items():
        path = prefix / name
        if path.is_symlink() or prefix not in path.resolve().parents or digest(path) != expected:
            raise RuntimeError('CRoaring installed file identity mismatch: ' + name)
    for name in ('include/roaring/roaring.h', 'include/roaring/roaring_version.h',
                 str(library.relative_to(prefix)), 'licenses/CRoaring-LICENSE'):
        if name not in files:
            raise RuntimeError('CRoaring required file absent: ' + name)
    if (files[str(library.relative_to(prefix))] != dep['library_sha256'] or
            files['licenses/CRoaring-LICENSE'] != ROARING_LICENSE):
        raise RuntimeError('CRoaring archive/license identity mismatch')
    patch = Path(dep['source_patch']).resolve(strict=True)
    base = Path(dep['base_build']).resolve(strict=True)
    source = Path(dep['source']).resolve(strict=True)
    source_file = source / 'src/roaring_array.c'
    if (digest(patch) != ROARING_PATCH or digest(base) != dep['base_build_sha256'] or
            digest(source_file) != dep['patched_source_sha256'] or
            digest(source / 'LICENSE') != ROARING_LICENSE):
        raise RuntimeError('CRoaring patch/source provenance mismatch')
    original = json.loads(base.read_text())
    if (original.get('status') != 'built_unlinked' or
            original.get('source_commit') != ROARING_COMMIT or
            original.get('library_sha256') != dep['base_library_sha256']):
        raise RuntimeError('CRoaring base build provenance mismatch')
    return dict(receipt=str(receipt), receipt_sha256=digest(receipt),
                prefix=str(prefix), include=str(include), library=str(library),
                library_sha256=dep['library_sha256'], owner_sha256=digest(owner_path),
                files=files, source_commit=ROARING_COMMIT, source_patch_sha256=ROARING_PATCH,
                patched_source_sha256=digest(source_file), base_build_sha256=digest(base))


def roaring_config_path(profile):
    source_key = hashlib.sha256(str(SOURCE).encode()).hexdigest()
    return ROOT / '.build/opengauss-native-lake/config' / source_key / (profile + '.json')


def native_build_config_inputs():
    """Freeze inputs whose changes require a clean native rebuild.

    This tree need not enable automatic header dependencies. A CPP-only repair
    after a failed full build can still reuse the correctly configured objects.
    """
    return {name: value for name, value in native_source_inputs().items()
            if Path(name).suffix in {'.h', '.hpp', '.inc', '.in', '.cmake', '.sh'} or
            Path(name).name in {'Makefile', 'CMakeLists.txt', 'configure'}}


def check_roaring_config(mode, profile, dependency, bitmap_sources, pq_dependency=None):
    """Full is required when enabling/disabling/changing compiler inputs."""
    path = roaring_config_path(profile)
    identity = dict(source=str(SOURCE), profile=profile, roaring_inputs=dependency,
                    pq_inputs=pq_dependency,
                    compiler_inputs=native_build_config_inputs())
    if mode in ('incremental', 'restart'):
        if path.exists():
            previous = json.loads(path.read_text())
            state = previous.pop('state', None)
            if previous != identity or state not in (None, 'building'):
                raise RuntimeError('native header/configuration or CRoaring/PQ inputs changed; use full')
            if state == 'building' and mode == 'restart':
                raise RuntimeError('unfinished native build cannot restart; complete the build first')
        elif dependency is not None or bitmap_sources or pq_dependency is not None:
            raise RuntimeError('no recorded native bitmap build configuration; use full')
    return path, identity


def verify_roaring_symbols(binary, library, required_symbols=()):
    """Check all archive globals, including helpers without roaring_ prefixes."""
    def names(path, dynamic=False):
        cmd = ['nm', '--defined-only', '--extern-only']
        if dynamic:
            cmd.append('--dynamic')
        output = subprocess.check_output(cmd + [str(path)], text=True, stderr=subprocess.STDOUT)
        return {parts[-1].split('@')[0] for line in output.splitlines()
                if len(parts := line.split()) >= 3 and re.fullmatch(r'[A-Za-z]', parts[-2])}
    archive = names(library)
    leaked = sorted(archive & names(binary, dynamic=True))
    if not archive or leaked:
        raise RuntimeError('private CRoaring symbol visibility check failed: ' + repr(leaked[:20]))
    if required_symbols:
        defined = subprocess.check_output(['nm', '--defined-only', str(binary)], text=True)
        present = {line.split()[-1] for line in defined.splitlines() if line.split()}
        missing = sorted(set(required_symbols) - present)
        if missing:
            raise RuntimeError('native bitmap implementation was not linked: ' + repr(missing))
    return dict(status='hidden', archive_global_symbol_count=len(archive),
                dynamic_overlap_count=0,
                required_defined_symbols=list(required_symbols),
                archive_symbols_sha256=hashlib.sha256('\n'.join(sorted(archive)).encode()).hexdigest())


def selected_pq_inputs(receipt, workspace, *, selection=None, selection_sha256=None):
    """Only the explicit new kind selects additive admission; legacy is exact."""
    if receipt is None:
        if selection is not None or selection_sha256 is not None:
            raise RuntimeError('SDK54 selection requires a decimal54 PQ receipt')
        return pq_inputs(None, workspace)
    kind = json.loads(Path(receipt).read_text()).get('dependency_kind')
    if kind == 'pq_decimal54_v1':
        from sdk_decimal54_native_inputs import pq_inputs as decimal54_pq_inputs
        return decimal54_pq_inputs(receipt, workspace, selection=selection, selection_sha256=selection_sha256)
    if selection is not None or selection_sha256 is not None:
        raise RuntimeError('Explicit SDK54 selection cannot accompany a legacy PQ kind')
    if kind is None:
        return pq_inputs(receipt, workspace)
    if kind == 'pq_sql020_int16_v1':
        from pq_sql020_build_inputs import pq_inputs as sql020_pq_inputs
        return sql020_pq_inputs(receipt, workspace)
    if kind == 'pq_temporal_v1':
        from pq_temporal_build_inputs import pq_inputs as temporal_pq_inputs
        return temporal_pq_inputs(receipt, workspace)
    raise RuntimeError('Unknown optional PQ dependency kind: ' + repr(kind))


def native_source_inputs():
    """Include new module files before git add, and omit generated objects.

    git diff HEAD alone misses an untracked new .cpp. Freeze actual input bytes
    so different candidate builds cannot receive the same source identity.
    """
    names = set(subprocess.check_output(
        ['git', '-C', str(SOURCE), 'diff', '--name-only', BASELINE, '--'],
        text=True).splitlines())
    module = SOURCE / 'contrib/og_lake'
    for path in module.rglob('*'):
        # Make writes this object/link-input list during a fresh build. Keep
        # tracked changes above, but do not treat the generated list as source.
        if path == module / 'objfiles.txt':
            continue
        if path.is_file() and (path.suffix in {'.cpp', '.c', '.h', '.hpp', '.sql',
                '.control', '.inc', '.md', '.sh', '.txt'} or path.name in {'Makefile', 'CMakeLists.txt', 'LICENSE', 'LICENSE.ducklake'}):
            names.add(str(path.relative_to(SOURCE)))
    return {name: digest(SOURCE / name) for name in sorted(names)
            if (SOURCE / name).is_file()}


def build_wrapper_inputs():
    return {str(path): digest(path) for path in
            (Path(__file__).resolve(), Path(__file__).with_name('native_runtime.py').resolve(),
             Path(__file__).with_name('pq_native_build_inputs.py').resolve(),
             Path(__file__).parent / 'indexes/pq_native_dependency_build.py',
             Path(__file__).with_name('pq_sql020_build_inputs.py').resolve(),
             Path(__file__).parent / 'indexes/pq_sql020_dependency_build.py',
             Path(__file__).with_name('sdk_temporal_inputs.py').resolve(),
             Path(__file__).with_name('pq_temporal_build_inputs.py').resolve(),
             Path(__file__).with_name('sdk_decimal54_native_inputs.py').resolve())}


def run(argv, env, log):
    log.write('$ ' + json.dumps([str(a) for a in argv], ensure_ascii=False) + '\n')
    log.flush()
    proc = subprocess.run([str(a) for a in argv], env=env, stdout=log,
                          stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL)
    if proc.returncode:
        raise RuntimeError(f'{argv[0]} exited {proc.returncode}; see {log.name}')


def port_check(port):
    for current in range(port, port + 3):
        with socket.socket() as s:
            try:
                s.bind(('127.0.0.1', current))
            except OSError as exc:
                raise RuntimeError(f'port {current} unavailable: {exc}') from exc


def bootstrap(data, runtime, evidence, prefix=None):
    if (data / 'PG_VERSION').is_file():
        return
    if data.exists() and any(data.iterdir()):
        raise RuntimeError(f'refusing to initialize nonempty directory: {data}')
    candidates = [ROOT / '.install/diskann-vector-filter-before-p12-release',
                  ROOT / '.install/hybrid-recall-pure-validation']
    if prefix is None:
        prefix = next((p for p in candidates if (p / 'bin/gs_initdb').is_file()), None)
    else:
        prefix = Path(prefix).resolve()
    if prefix is None:
        raise RuntimeError('no compatible bootstrap gs_initdb; no fake PG_VERSION permitted')
    if not (prefix / 'bin/gs_initdb').is_file() or not (prefix / 'bin/gaussdb').is_file():
        raise RuntimeError('bootstrap prefix must contain both actual native executables')
    env = os.environ.copy()
    gcc = ROOT / 'binarylibs/buildtools/gcc10.3/gcc/lib64'
    env.update(GAUSSHOME=str(prefix), LD_LIBRARY_PATH=f'{prefix}/lib:{gcc}')
    sock = runtime / 'socket'
    sock.mkdir(exist_ok=True)
    with (evidence / 'bootstrap.log').open('w') as log:
        run([prefix / 'bin/gs_initdb', '--version'], env, log)
        with tempfile.NamedTemporaryFile(mode='w', dir=runtime, prefix='.init-password-') as pw:
            # openGauss bootstrap passwords are limited to 32 characters.
            pw.write('Og1!' + secrets.token_hex(12) + '\n')
            pw.flush()
            run([prefix / 'bin/gs_initdb', '-D', data, '--nodename=og_native_lake',
                 '--dbcompatibility=PG', '--locale=C', '-E', 'UTF8', '-A', 'trust',
                 '--pwfile', pw.name], env, log)
    with (data / 'postgresql.conf').open('a') as conf:
        conf.write("\n# Isolated native lake development instance.\n"
                   "listen_addresses = '127.0.0.1'\n"
                   f"unix_socket_directory = '{sock}'\n"
                   "shared_buffers = '128MB'\nmax_connections = 40\n"
                   "max_prepared_transactions = 10\nwork_mem = '16MB'\n"
                   "enable_thread_pool = off\n")
    (evidence / 'bootstrap.json').write_text(json.dumps({
        'bootstrap_prefix': str(prefix), 'initdb_sha256': digest(prefix / 'bin/gs_initdb'),
        'gaussdb_sha256': digest(prefix / 'bin/gaussdb'),
        'bootstrap_bki_sha256': digest(prefix / 'share/postgresql/postgres.bki'),
        'data': str(data), 'compatibility': 'PG', 'status': 'initialized_only',
    }, indent=2) + '\n')


def main():
    global SOURCE
    ap = argparse.ArgumentParser()
    ap.add_argument('profile', choices=['debug', 'release', 'ann-debug'])
    ap.add_argument('mode', choices=['init', 'full', 'incremental', 'restart', 'stop'])
    ap.add_argument('--run-id', default=datetime.datetime.now().strftime('build_%Y%m%d_%H%M%S'))
    ap.add_argument('--reuse-build-receipt', type=Path, help='Required compiled native identity for an ann-debug deployment')
    ap.add_argument('--roaring-build', type=Path, help='Qualified private CRoaring OOM-fix build.json; required by bitmap builds')
    ap.add_argument('--pq-build', type=Path, help='Optional owned PQ static dependency build.json; omitted keeps raw graph')
    ap.add_argument('--pq-in-tree', action='store_true', help='Build and link in-tree contrib/og_lake/{pq,vendor/diskann_pq} with private OpenBLAS')
    ap.add_argument('--blas-build', type=Path, default=None, help='Private locking OpenBLAS build.json used with --pq-in-tree')
    ap.add_argument('--blas-build-sha256', help='Exact SHA256 of a newly rebuilt private BLAS receipt; legacy admission remains the default')
    ap.add_argument('--build-entry', type=Path, help='Explicit build/restart entry for a reconstructed environment; recorded in build identity')
    ap.add_argument('--pq-sdk-selection', type=Path, help='Exact full SDK54/PQ source and actual proof selection; decimal54 kind only')
    ap.add_argument('--pq-sdk-selection-sha256', help='SHA256 of the explicit decimal54 SDK selection')
    ap.add_argument('--jobs', type=int, default=3)
    args = ap.parse_args()
    if args.pq_in_tree and (args.pq_build is not None or args.pq_sdk_selection is not None or args.pq_sdk_selection_sha256 is not None):
        ap.error('--pq-in-tree cannot be combined with --pq-build or --pq-sdk-selection')
    if args.blas_build is not None and not args.pq_in_tree:
        ap.error('--blas-build requires --pq-in-tree')
    if args.blas_build_sha256 and (args.blas_build is None or not args.pq_in_tree):
        ap.error('--blas-build-sha256 requires --pq-in-tree and --blas-build')
    SOURCE = source_for_profile(args.profile)
    if not re.fullmatch(r'[A-Za-z0-9_.-]+', args.run_id) or args.jobs < 1:
        ap.error('invalid run id or jobs')
    runtime = ROOT / '.runtime/opengauss-native-lake' / args.profile
    data = runtime / 'data'
    install_profile = 'debug' if args.profile=='ann-debug' else args.profile
    install = ROOT / f'.install/opengauss-native-lake-{install_profile}'
    port = {'debug':55560,'release':55570,'ann-debug':55580}[args.profile]
    evidence = ROOT / '公共文档/openGauss原生湖存储/验证' / args.run_id
    evidence.mkdir(parents=True, exist_ok=False)
    runtime.mkdir(parents=True, exist_ok=True)
    identity = {'project': 'opengauss-native-lake', 'source': str(SOURCE),
                'data': str(data), 'install': str(install), 'profile': args.profile, 'port': port}
    owner = runtime / 'owner.json'
    if owner.exists():
        if json.loads(owner.read_text()) != identity:
            raise RuntimeError('runtime ownership mismatch')
    elif data.exists():
        raise RuntimeError('existing data without project ownership marker')
    else:
        owner.write_text(json.dumps(identity, indent=2) + '\n')
    env = os.environ.copy()
    # An inherited path must never bypass --roaring-build admission.
    env.pop('OG_LAKE_ROARING_INCLUDE', None)
    env.pop('OG_LAKE_ROARING_ARCHIVE', None)
    for name in ('OG_LAKE_PQ_IN_TREE', 'OG_LAKE_PQ_INCLUDE', 'OG_LAKE_PQ_ARCHIVE',
                 'OG_LAKE_PQ_BLAS_INCLUDE', 'OG_LAKE_PQ_BLAS_ARCHIVE',
                 'OG_LAKE_PQ_BLAS_LICENSES', 'OG_LAKE_PQ_MANIFEST'):
        env.pop(name, None)
    env.update(CODE_BASE=str(SOURCE), BINARYLIBS=str(ROOT / 'binarylibs'),
               OG_LAKE_BUILD_PROFILE=install_profile,
               GAUSSHOME=str(install), PGDATA=str(data), PGPORT=str(port),
               PGHOST=str(runtime / 'socket'), PGUSER=env.get('USER', 'qiumc'),
               DB_USER=env.get('USER', 'qiumc'), JOBS=str(args.jobs),
               LOG_DIR=str(evidence), SERVER_LOG=str(runtime / 'server.log'))
    env['LD_LIBRARY_PATH'] = f"{install}/lib:{ROOT}/binarylibs/buildtools/gcc10.3/gcc/lib64"
    summary = dict(identity, mode=args.mode, status='failed', evidence=str(evidence))
    try:
        baseline = subprocess.check_output(['git', '-C', str(SOURCE), 'rev-parse', 'HEAD'], text=True).strip()
        subprocess.run(['git', '-C', str(SOURCE), 'merge-base', '--is-ancestor', BASELINE, baseline], check=True)
        summary['source_head'] = baseline
        summary['source_diff_sha256'] = hashlib.sha256(subprocess.check_output(
            ['git', '-C', str(SOURCE), 'diff', 'HEAD'])).hexdigest()
        summary['native_source_inputs_before'] = native_source_inputs()
        summary['build_wrapper_inputs_before'] = build_wrapper_inputs()
        if args.mode == 'stop':
            with (evidence / 'stop.log').open('w') as log:
                run([install / 'bin/gs_ctl', 'stop', '-D', data, '-m', 'fast', '-w'], env, log)
            summary['status'] = 'stopped'
            return 0
        bitmap_sources = any((SOURCE / 'contrib/og_lake' / name).exists()
                             for name in ('lake_bitmap_codec.cpp', 'lake_bitmap_format.cpp'))
        if bitmap_sources and args.mode in ('full', 'incremental') and args.roaring_build is None:
            raise RuntimeError('native bitmap source build requires explicit --roaring-build')
        dependency = roaring_inputs(args.roaring_build)
        summary['roaring_inputs_before'] = dependency
        if dependency is not None:
            if not bitmap_sources:
                raise RuntimeError('CRoaring supplied to a source tree without native bitmap implementation')
            env.update(OG_LAKE_ROARING_INCLUDE=dependency['include'],
                       OG_LAKE_ROARING_ARCHIVE=dependency['library'])
        if args.pq_in_tree:
            pq_dependency = pq_in_tree_inputs(ROOT, SOURCE, args.blas_build, args.blas_build_sha256)
            if not (SOURCE / 'contrib/og_lake/lake_file_pq.cpp').is_file():
                raise RuntimeError('In-tree PQ supplied to a source tree without Native PQ glue')
            env.update(OG_LAKE_PQ_IN_TREE='1',
                       OG_LAKE_PQ_BLAS_INCLUDE=pq_dependency['blas_include'],
                       OG_LAKE_PQ_BLAS_ARCHIVE=pq_dependency['blas_library'],
                       OG_LAKE_PQ_BLAS_LICENSES=pq_dependency['blas_licenses'])
        else:
            pq_dependency = selected_pq_inputs(args.pq_build, ROOT, selection=args.pq_sdk_selection, selection_sha256=args.pq_sdk_selection_sha256)
            if pq_dependency is not None:
                if not (SOURCE / 'contrib/og_lake/lake_file_pq.cpp').is_file():
                    raise RuntimeError('PQ supplied to a source tree without Native PQ glue')
                for public in ('sdk/lake_parquet_sdk.h', 'vendor/arrow20/abi.h'):
                    if digest(SOURCE / 'contrib/og_lake' / public) != pq_dependency['files']['include/' + public]:
                        raise RuntimeError('Native/PQ public SDK ABI header identity mismatch: ' + public)
                # The receipt itself stays in the evidence tree. Only validated
                # include/archive paths enter compiler/linker flags.
                env.update(OG_LAKE_PQ_INCLUDE=pq_dependency['include'],
                           OG_LAKE_PQ_ARCHIVE=pq_dependency['library'],
                           OG_LAKE_PQ_BLAS_ARCHIVE=pq_dependency['blas_library'],
                           OG_LAKE_PQ_MANIFEST=pq_dependency['receipt'])
        summary['pq_inputs_before'] = pq_dependency
        config_path, config_identity = check_roaring_config(
            args.mode, install_profile, dependency, bitmap_sources, pq_dependency)
        if not (data / 'postmaster.pid').exists():
            port_check(port)
        else:
            pid = int((data / 'postmaster.pid').read_text().splitlines()[0])
            if not Path(f'/proc/{pid}').exists():
                # Leave stale pidfile recovery to gs_ctl/gaussdb itself.
                port_check(port)
                summary['stale_pid_before_recovery'] = pid
            elif Path(f'/proc/{pid}/exe').resolve() != install / 'bin/gaussdb':
                raise RuntimeError('running instance binary is not this project installation')
        if args.profile=='ann-debug':
            if args.mode not in ('restart','init') or args.reuse_build_receipt is None:
                raise RuntimeError('ann-debug only deploys an exact compiled build through restart/init and --reuse-build-receipt')
            previous=json.loads(args.reuse_build_receipt.read_text())
            if(previous.get('status')!='build_and_sql_passed' or previous.get('source_head')!=baseline or
               previous.get('native_source_inputs_after')!=summary['native_source_inputs_before'] or
               previous.get('roaring_inputs_after')!=dependency or
               previous.get('pq_inputs_after')!=pq_dependency or
               previous.get('binary_sha256')!=digest(install/'bin/gaussdb')):
                raise RuntimeError('ann-debug compiled source/binary identity mismatch')
            summary['compiled_build_receipt']=str(args.reuse_build_receipt.resolve())
            summary['compiled_build_receipt_sha256']=digest(args.reuse_build_receipt)
            bootstrap(data,runtime,evidence,prefix=install)
        elif not (args.build_entry is not None and args.mode in ('full', 'incremental')):
            bootstrap(data, runtime, evidence)
        if args.mode == 'init':
            summary['status'] = 'initialized_only'
            return 0
        if args.mode == 'incremental':
            if not (SOURCE / 'config.status').is_file():
                raise RuntimeError('first build requires full')
            config = (SOURCE / 'src/Makefile.global').read_text()
            expected_debug = args.profile != 'release'
            if ('--enable-debug' in config) != expected_debug or str(install) not in config:
                raise RuntimeError('configuration/profile differs; use full')
        entry = args.build_entry.resolve(strict=True) if args.build_entry else ROOT / ('deliverables/multimodal_optimizer/opengauss_build_install_restart.sh'
                        if args.profile != 'release' else 'deliverables/hybrid_vs_milvus_bench/build_release.sh')
        summary['build_entry'] = str(entry)
        summary['build_entry_sha256'] = digest(entry)
        # The authoritative Debug installer uses cp -r for JRE files. Its
        # upstream archive contains read-only files, so subsequent installs
        # otherwise fail. Touch only this owned project's installed JRE.
        if args.mode in ('full', 'incremental') and args.profile != 'release':
            adjusted = []
            jre = install / 'jre'
            if jre.is_dir():
                for path in jre.rglob('*'):
                    if path.is_file() and not path.is_symlink():
                        mode = path.stat().st_mode
                        if not mode & stat.S_IWUSR:
                            path.chmod(mode | stat.S_IWUSR)
                            adjusted.append(str(path.relative_to(install)))
            summary['owned_jre_write_permissions_fixed'] = adjusted
        print(f'Build log: {evidence / "build.log"}', flush=True)
        if args.mode in ('full', 'incremental'):
            # Retain the header/configuration identity through a failed build.
            # CPP-only repairs may continue incrementally; restart must wait
            # until the full binary and running process have been verified.
            config_path.parent.mkdir(parents=True, exist_ok=True)
            config_path.write_text(json.dumps(dict(config_identity, state='building'), indent=2) + '\n')
            summary['native_build_configuration'] = str(config_path)
        with (evidence / 'build.log').open('w') as log:
            run(['bash', entry, args.mode], env, log)
        summary['native_source_inputs_after'] = native_source_inputs()
        if summary['native_source_inputs_after'] != summary['native_source_inputs_before']:
            raise RuntimeError('native lake source inputs changed during the build')
        summary['build_wrapper_inputs_after'] = build_wrapper_inputs()
        if summary['build_wrapper_inputs_after'] != summary['build_wrapper_inputs_before']:
            raise RuntimeError('native lake build/profile adapter changed during the build')
        summary['roaring_inputs_after'] = roaring_inputs(args.roaring_build)
        if summary['roaring_inputs_after'] != dependency:
            raise RuntimeError('CRoaring build inputs changed during the build')
        if dependency is not None:
            summary['roaring_symbol_visibility'] = verify_roaring_symbols(
                install / 'bin/gaussdb', dependency['library'], required_symbols=(
                    'og_lake_bitmap_decode', 'og_lake_bitmap_writer_open',
                    'og_lake_bitmap_select', 'roaring_bitmap_portable_deserialize_safe'))
        if args.pq_in_tree:
            summary['pq_inputs_after'] = pq_in_tree_inputs(ROOT, SOURCE, args.blas_build, args.blas_build_sha256)
            if summary['pq_inputs_after'] != pq_dependency:
                raise RuntimeError('In-tree PQ inputs changed during the build')
            pq_lib = Path(pq_dependency['library'])
            if not pq_lib.is_file():
                raise RuntimeError('In-tree PQ static archive was not built: ' + str(pq_lib))
            pq_syms = sorted(global_symbols(pq_lib))
            leaked_in_archive = [s for s in pq_syms if not s.startswith(('pq_cpu_', 'pq_parquet_', 'oglpqcpu_'))]
            if not pq_syms or leaked_in_archive:
                raise RuntimeError('In-tree PQ private C++ globals were not isolated: ' + repr(leaked_in_archive[:20]))
            summary['pq_in_tree_archive'] = {
                'path': str(pq_lib),
                'sha256': digest(pq_lib),
                'global_symbol_count': len(pq_syms),
            }
        else:
            summary['pq_inputs_after'] = selected_pq_inputs(args.pq_build, ROOT, selection=args.pq_sdk_selection, selection_sha256=args.pq_sdk_selection_sha256)
            if summary['pq_inputs_after'] != pq_dependency:
                raise RuntimeError('PQ static dependency inputs changed during the build')
        if pq_dependency is not None:
            summary['pq_symbol_visibility'] = verify_pq_symbols(install / 'bin/gaussdb', pq_dependency)
        with (evidence / 'sql.log').open('w') as log:
            run([install / 'bin/gsql', '-X', '-v', 'ON_ERROR_STOP=1', '-h', runtime / 'socket',
                 '-p', str(port), '-d', 'postgres', '-c',
                 'SELECT version(), current_database(), current_user; SELECT 6 * 7;'], env, log)
        summary['binary_sha256'] = digest(install / 'bin/gaussdb')
        pid = int((data / 'postmaster.pid').read_text().splitlines()[0])
        summary['running_pid'] = pid
        summary['running_binary_sha256'] = digest(Path(f'/proc/{pid}/exe'))
        if summary['binary_sha256'] != summary['running_binary_sha256']:
            raise RuntimeError('running binary differs from installed binary')
        if args.mode in ('full', 'incremental'):
            config_path.parent.mkdir(parents=True, exist_ok=True)
            config_path.write_text(json.dumps(config_identity, indent=2) + '\n')
            summary['native_build_configuration'] = str(config_path)
        summary['status'] = 'build_and_sql_passed'
        return 0
    except Exception as exc:
        summary['error'] = str(exc)
        print(str(exc), file=sys.stderr)
        return 1
    finally:
        if 'roaring_inputs_before' in summary and 'roaring_inputs_after' not in summary:
            try:
                summary['roaring_inputs_after'] = roaring_inputs(args.roaring_build)
            except Exception as exc:
                summary['roaring_inputs_after_error'] = str(exc)
        if 'pq_inputs_before' in summary and 'pq_inputs_after' not in summary:
            try:
                summary['pq_inputs_after'] = (
                    pq_in_tree_inputs(ROOT, SOURCE, args.blas_build, args.blas_build_sha256)
                    if args.pq_in_tree else
                    selected_pq_inputs(args.pq_build, ROOT, selection=args.pq_sdk_selection, selection_sha256=args.pq_sdk_selection_sha256)
                )
            except Exception as exc:
                summary['pq_inputs_after_error'] = str(exc)
        (evidence / 'build.json').write_text(json.dumps(summary, indent=2) + '\n')
        print(json.dumps(summary), flush=True)


if __name__ == '__main__':
    sys.exit(main())
