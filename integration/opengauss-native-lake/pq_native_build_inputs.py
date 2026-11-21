"""Strict optional Native PQ dependency admission; no environment-only enablement.

This helper verifies the frozen independent component identities. It does not
turn those component receipts into Native/SMP/recall qualification.
"""
import hashlib
import json
from pathlib import Path
import re
import subprocess

CPU_INDEX = ('pq_cpu_stream_restore_scoped_verified_index.json',
             'dc7f4cd2c5fd3752a3cf1731b30631dba49ddee2d751cbb61d79de9f12b1f06a',
             'verified_independent_stream_restore_component_only')
PARQUET_INDEX = ('pq_parquet_scoped_verified_index.json',
                 '6d44e6c159e46337d693869550be01ed062c7a5080596da83367aa1610d36871',
                 'verified_independent_pq_parquet_component_only')
BLAS_MANIFEST = ('pq_openblas_locking_build_1109_01/build.json',
                 '4752daabd6161d45a4520d21750a8d50f238d8104c95ae85038263281e629c3d')
MS_COMMIT = '78256bbab4685e1774e78d331e081a153be26823'
BLAS_COMMIT = 'efe42481e2c85e8197bdf533078bf89bdf6eabf4'
FRONTIER = {'pq_parquet_frontier_draft.h': '0862fb82e2b81ebea3bec6285a9d5895871d45d218bcbf21eb782eac6394cc70',
            'pq_parquet_frontier_draft.cpp': 'f567c955077b51b5430551804938e31d7f0b8441a12c86a549049c453758109b'}
SOURCES = ('pq_cpu.h', 'pq_cpu_internal.h', 'pq_cpu.cpp', 'pq_cpu_ms.cpp',
           'pq_cpu_locking_blas_meta.h', 'pq_cpu_locking_blas_meta.cpp',
           'pq_parquet_format.h', 'pq_parquet_format.cpp')
PUBLIC_PREFIXES = ('pq_cpu_', 'pq_parquet_')


def digest(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def read(path):
    return json.loads(Path(path).read_text())


def safe(path):
    path = Path(path)
    if not path.is_absolute() or not re.fullmatch(r'/[A-Za-z0-9_./+-]+', str(path)):
        raise RuntimeError('PQ build path must be absolute and Make-safe: ' + str(path))
    if path.is_symlink():
        raise RuntimeError('PQ build paths must not be symlinks: ' + str(path))
    return path.resolve(strict=True)


def verify_files(prefix, files):
    names = {str(p.relative_to(prefix)) for p in prefix.rglob('*') if p.is_file()}
    if names != set(files):
        raise RuntimeError('PQ installed file inventory mismatch')
    for name, expected in files.items():
        item = prefix / name
        if (Path(name).is_absolute() or '..' in Path(name).parts or
                item.is_symlink() or prefix not in item.resolve(strict=True).parents or
                digest(item) != expected):
            raise RuntimeError('PQ installed file mismatch: ' + name)


def qualified_sources(workspace):
    """Resolve by workspace-relative suffix; frozen receipts retain old paths."""
    workspace = Path(workspace).resolve(strict=True)
    evidence = workspace / '公共文档/openGauss原生湖存储/验证'
    records = {}
    inputs = {}
    for name, expected, status in (CPU_INDEX, PARQUET_INDEX):
        path = evidence / name
        if digest(path) != expected:
            raise RuntimeError('PQ qualified index identity mismatch: ' + name)
        record = read(path)
        if record.get('status') != status or record.get('native_integration') is not False:
            raise RuntimeError('PQ index scope mismatch: ' + name)
        records[name] = record
        inputs[str(path)] = expected
    source = workspace / 'integration/opengauss-native-lake/indexes'
    for name in SOURCES:
        candidates = [value for record in records.values()
                      for old, value in record['current_inputs'].items()
                      if old.endswith('/integration/opengauss-native-lake/indexes/' + name)]
        path = source / name
        if not candidates or len(set(candidates)) != 1 or digest(path) != candidates[0]:
            raise RuntimeError('PQ qualified component changed: ' + name)
        inputs[str(path)] = candidates[0]
    for name, expected in FRONTIER.items():
        path = source / name
        if digest(path) != expected:
            raise RuntimeError('PQ isolated frontier source identity mismatch: ' + name)
        inputs[str(path)] = expected
    sdk = workspace / '.worktrees/opengauss-native-lake-inline-index/contrib/og_lake'
    for name in ('sdk/lake_parquet_sdk.h', 'vendor/arrow20/abi.h'):
        expected = [value for old, value in records[PARQUET_INDEX[0]]['current_inputs'].items()
                    if old.endswith('/contrib/og_lake/' + name)]
        path = sdk / name
        if len(expected) != 1 or digest(path) != expected[0]:
            raise RuntimeError('PQ qualified SDK header mismatch: ' + name)
        inputs[str(path)] = expected[0]
    license_path = workspace / '.reference/opengauss-native-lake/diskann-pq-cpu-reference/LICENSE'
    commit = subprocess.check_output(['git', '-C', str(license_path.parent), 'rev-parse', 'HEAD'], text=True).strip()
    if commit != MS_COMMIT:
        raise RuntimeError('PQ Microsoft source commit mismatch')
    if digest(license_path) != 'c7d6393d0618d992594942d9bc91b520cb1a4d92ea2bbd26b31f1eae56f87ce0':
        raise RuntimeError('PQ Microsoft license identity mismatch')
    inputs[str(license_path)] = digest(license_path)
    return dict(workspace=str(workspace), source=str(source), sdk_include=str(sdk),
                source_commit=MS_COMMIT, inputs=inputs,
                frontier_scope='isolated adapter only; not independent Native qualification')


def blas_inputs(workspace):
    workspace = Path(workspace).resolve(strict=True)
    path = workspace / '公共文档/openGauss原生湖存储/验证' / BLAS_MANIFEST[0]
    if digest(path) != BLAS_MANIFEST[1]:
        raise RuntimeError('PQ private locking BLAS manifest mismatch')
    dep = read(path)
    if (dep.get('status') != 'built_provisional_unlinked' or
            dep.get('source_commit') != BLAS_COMMIT or dep.get('symbol_prefix') != 'oglpq_'):
        raise RuntimeError('PQ requires pinned private locking OpenBLAS')
    variables = dict(item.split('=', 1) for item in dep['make_variables'])
    expected = dict(USE_THREAD='0', USE_LOCKING='1', USE_OPENMP='0', NOFORTRAN='1',
                    ONLY_CBLAS='1', NO_LAPACK='1', NO_LAPACKE='1', NEED_PIC='1',
                    NO_SHARED='1', NO_STATIC='0', SYMBOLPREFIX='oglpq_', NUM_THREADS='1')
    if any(variables.get(k) != v for k, v in expected.items()):
        raise RuntimeError('PQ BLAS threading/PIC/symbol build contract mismatch')
    prefix = safe(dep['prefix'])
    verify_files(prefix, dep['files'])
    if read(prefix / '.og-lake-owner.json') != dep['owner']:
        raise RuntimeError('PQ BLAS ownership mismatch')
    library = safe(dep['library'])
    if (library != prefix / 'lib/libog_lake_pq_openblas.a' or
            digest(library) != dep['library_sha256']):
        raise RuntimeError('PQ private BLAS archive mismatch')
    return dict(receipt=str(path), receipt_sha256=digest(path), prefix=str(prefix),
                library=str(library), library_sha256=digest(library), files=dep['files'],
                source_commit=BLAS_COMMIT)


def global_symbols(path, dynamic=False):
    command = ['nm', '--defined-only', '--extern-only']
    if dynamic:
        command.append('--dynamic')
    output = subprocess.check_output(command + [str(path)], text=True, stderr=subprocess.STDOUT)
    return {parts[-1].split('@')[0] for line in output.splitlines()
            if len(parts := line.split()) >= 3 and re.fullmatch(r'[A-Za-z]', parts[-2])}


def pq_inputs(receipt, workspace):
    if receipt is None:
        return None
    # Receipt path need not be Make-safe (evidence is in the Chinese document
    # tree); only installed paths enter Make/CMake flags. Driver never shells it.
    receipt = Path(receipt).resolve(strict=True)
    dep = read(receipt)
    if (dep.get('status') != 'pq_static_built_unlinked' or dep.get('manifest_version') != 1 or
            dep.get('native_integration') is not False):
        raise RuntimeError('PQ requires the owned static dependency build receipt')
    qualified = qualified_sources(workspace)
    blas = blas_inputs(workspace)
    if dep.get('qualified_sources') != qualified or dep.get('blas_inputs') != blas:
        raise RuntimeError('PQ frozen source/dependency identity changed')
    prefix = safe(dep['prefix'])
    include = safe(dep['include'])
    library = safe(dep['library'])
    blas_library = safe(dep['blas_library'])
    if (include != prefix / 'include' or library != prefix / 'lib/libog_lake_pq_cpu.a' or
            blas_library != prefix / 'lib/libog_lake_pq_openblas.a'):
        raise RuntimeError('PQ archive/header paths must stay in owned prefix')
    verify_files(prefix, dep['files'])
    owner = prefix / '.og-lake-pq-owner.json'
    if digest(owner) != dep['owner_sha256'] or read(owner) != dep['owner']:
        raise RuntimeError('PQ owner marker mismatch')
    if dep['owner'].get('prefix') != str(prefix):
        raise RuntimeError('PQ owner prefix mismatch')
    for path, expected in dep['helper_inputs'].items():
        if digest(Path(path)) != expected:
            raise RuntimeError('PQ dependency builder/admission helper changed')
    for key, file in dep['build_tools'].items():
        if digest(Path(file['path'])) != file['sha256']:
            raise RuntimeError('PQ compiler/tool identity mismatch: ' + key)
    required = ('include/pq_cpu.h', 'include/pq_parquet_format.h',
                'include/pq_parquet_frontier_draft.h', 'include/sdk/lake_parquet_sdk.h',
                'include/vendor/arrow20/abi.h', 'licenses/Microsoft-DiskANN-LICENSE',
                'licenses/OpenBLAS/LICENSE', str(library.relative_to(prefix)),
                str(blas_library.relative_to(prefix)), 'symbol-map.txt')
    if any(name not in dep['files'] for name in required):
        raise RuntimeError('PQ installed public ABI/license/archive is incomplete')
    if (digest(library) != dep['library_sha256'] or digest(blas_library) != blas['library_sha256'] or
            sorted(global_symbols(library)) != dep['owned_symbols']):
        raise RuntimeError('PQ archive symbol/byte identity mismatch')
    if any(not name.startswith(PUBLIC_PREFIXES + ('oglpqcpu_',)) for name in dep['owned_symbols']):
        raise RuntimeError('PQ private C++ globals were not isolated')
    return dict(receipt=str(receipt), receipt_sha256=digest(receipt), prefix=str(prefix),
                include=str(include), library=str(library), blas_library=str(blas_library),
                library_sha256=digest(library), blas_library_sha256=digest(blas_library),
                owner_sha256=digest(owner), files=dep['files'], qualified_sources=qualified,
                blas_inputs=blas, build_tools=dep['build_tools'],
                symbol_isolation=dep['symbol_isolation'])


def verify_pq_symbols(binary, dependency):
    """All owned archive globals must remain absent from gaussdb's dynamic ABI."""
    owned = global_symbols(dependency['library']) | global_symbols(dependency['blas_library'])
    leaked = sorted(owned & global_symbols(binary, dynamic=True))
    if not owned or leaked:
        raise RuntimeError('private PQ symbol visibility failed: ' + repr(leaked[:20]))
    output = subprocess.check_output(['nm', '--defined-only', str(binary)], text=True)
    present = {line.split()[-1] for line in output.splitlines() if line.split()}
    required = {'pq_cpu_model_restore', 'pq_parquet_reader_open', 'pq_parquet_graph_context',
                'pq_cpu_blas_immutable_config', 'oglpq_cblas_sgemm'}
    if not required <= present:
        raise RuntimeError('optional Native PQ implementation was not linked: ' + repr(sorted(required - present)))
    return dict(status='hidden', archive_global_symbol_count=len(owned), dynamic_overlap_count=0,
                required_defined_symbols=sorted(required),
                archive_symbols_sha256=hashlib.sha256('\n'.join(sorted(owned)).encode()).hexdigest())
