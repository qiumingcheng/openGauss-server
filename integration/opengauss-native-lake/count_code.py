#!/usr/bin/env python3
"""Count native-lake product C/C++ lines separately from build/test assets.

Physical lines include comments and blanks. A line count is not a capability or
test result; each receipt explicitly states the source revision it measured.
"""
import argparse
import datetime
import hashlib
import json
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT / '.worktrees/opengauss-native-lake'
BASELINE = 'ba1e4ad2f99295b1b0f65e3880a05cb75a1121d4'
SUFFIXES = {'.c', '.cc', '.cpp', '.h', '.hpp'}
TEST_FILES = {'contrib/og_lake/lake_test_hooks.cpp', 'contrib/og_lake/lake_pq_test.h'}
PQ_OWNED_FILES = (
    'pq_cpu.h', 'pq_cpu_internal.h', 'pq_cpu.cpp',
    'pq_cpu_locking_blas_meta.h', 'pq_cpu_locking_blas_meta.cpp',
    'pq_parquet_format.h', 'pq_parquet_format.cpp',
    'pq_parquet_frontier_draft.h', 'pq_parquet_frontier_draft.cpp',
)


def product_source(name):
    path = Path(name)
    return (path.suffix in SUFFIXES and name not in TEST_FILES and
            not name.startswith('src/test/') and '/tests/' not in name and
            '/new/' not in name and '/vendor/' not in name)


def git(*args):
    return subprocess.check_output(['git', '-C', str(SOURCE), *args], text=True)


def main():
    global SOURCE
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, default=SOURCE,
                        help='product worktree to measure; defaults to the main native-lake tree')
    parser.add_argument('--revision', help='committed revision; omit to include current worktree')
    parser.add_argument('--pq-build', type=Path,
                        help='optional legacy private PQ dependency receipt when PQ adapter still lived out-of-tree')
    parser.add_argument('--pq-sdk-selection', type=Path,
                        help='optional decimal54 SDK/PQ selection descriptor when using --pq-build')
    parser.add_argument('--pq-sdk-selection-sha256',
                        help='optional decimal54 selection SHA256 when using --pq-build')
    parser.add_argument('--output', type=Path, help='optional new JSON receipt under project evidence directory')
    args = parser.parse_args()
    SOURCE = args.source.resolve()
    if not (SOURCE / '.git').exists():
        parser.error('--source must identify an existing product Git worktree')
    revision = git('rev-parse', '--verify', args.revision + '^{commit}').strip() if args.revision else None
    diff_args = ['diff', '--numstat', BASELINE] + ([revision] if revision else [])
    rows = []
    for record in git(*diff_args).splitlines():
        added, deleted, name = record.split('\t', 2)
        if not product_source(name):
            continue
        rows.append({'path': name, 'added': int(added), 'deleted': int(deleted)})
    if revision is None:
        for name in git('ls-files', '--others', '--exclude-standard', 'contrib/og_lake').splitlines():
            if product_source(name):
                rows.append({'path': name, 'added': len((SOURCE / name).read_text().splitlines()), 'deleted': 0})
    for row in rows:
        row['scope'] = ('lake_module' if row['path'].startswith('contrib/og_lake/') or
                        row['path'] == 'src/include/access/og_lake.h' else 'existing_kernel')
    pq = None
    in_tree_pq_files = [r for r in rows if r['path'].startswith('contrib/og_lake/pq/')]
    if in_tree_pq_files and args.pq_build is not None:
        parser.error('--pq-build cannot be used when PQ adapter files are already in-tree under contrib/og_lake/pq/')
    if in_tree_pq_files:
        vendor_ms = SOURCE / 'contrib/og_lake/vendor/diskann_pq/pq_cpu_ms.cpp'
        pq = {
            'location': 'in_tree (contrib/og_lake/pq/ already counted once in totals.lake_module)',
            'owned_adapter_files': len(in_tree_pq_files),
            'owned_adapter_lines': sum(r['added'] for r in in_tree_pq_files),
            'excluded_dependencies': ['OpenBLAS source/archive', 'Microsoft DiskANN vendored numerical subset (/vendor/)'],
        }
        if vendor_ms.is_file():
            pq['excluded_reused_algorithm'] = {
                'path': 'contrib/og_lake/vendor/diskann_pq/pq_cpu_ms.cpp',
                'lines': len(vendor_ms.read_text().splitlines()),
                'sha256': hashlib.sha256(vendor_ms.read_bytes()).hexdigest(),
            }
    elif args.pq_build:
        if revision is not None:
            parser.error('--pq-build measures current adapter bytes, not a historical committed revision')
        from build_driver import selected_pq_inputs as pq_inputs
        dependency = pq_inputs(args.pq_build, ROOT, selection=args.pq_sdk_selection,
                               selection_sha256=args.pq_sdk_selection_sha256)
        pq_source = ROOT / 'integration/opengauss-native-lake/indexes'
        if not (SOURCE / 'contrib/og_lake/lake_file_pq.cpp').is_file():
            parser.error('PQ count requires the Native PQ integration source')
        for name in PQ_OWNED_FILES:
            path = pq_source / name
            rows.append({'path': str(path.relative_to(ROOT)),
                         'added': len(path.read_text().splitlines()), 'deleted': 0,
                         'scope': 'lake_module', 'storage_root': 'workspace',
                         'linkage': 'private static PQ archive linked into gaussdb'})
        reused = pq_source / 'pq_cpu_ms.cpp'
        pq = {'dependency_receipt': str(args.pq_build.resolve()),
              'dependency_receipt_sha256': dependency['receipt_sha256'],
              'scope': 'owned adapter sources counted once; build/runtime qualification is separate',
              'excluded_reused_algorithm': {'path': str(reused.relative_to(ROOT)),
                  'lines': len(reused.read_text().splitlines()),
                  'sha256': hashlib.sha256(reused.read_bytes()).hexdigest()},
              'excluded_dependencies': ['OpenBLAS source/archive', 'duplicate installed headers',
                                        'SDK ABI headers already counted in the Native module']}
    result = {
        'measured_at': datetime.datetime.now(datetime.timezone.utc).isoformat(),
        'baseline': BASELINE,
        'source_worktree': str(SOURCE),
        'revision': revision,
        'head': git('rev-parse', 'HEAD').strip(),
        'includes_uncommitted_work': revision is None,
        'unit': 'physical C/C++ and header lines, including comments and blanks',
        'excluded': ['SQL/control', 'build rules', 'tests (including lake_test_hooks.cpp and lake_pq_test.h)',
                     'duplicate staging directory', 'generated build files', 'vendored third-party headers'],
        'totals': {},
        'files': sorted(rows, key=lambda row: row['path']),
    }
    for scope in ('existing_kernel', 'lake_module'):
        selected = [row for row in rows if row['scope'] == scope]
        result['totals'][scope] = {key: sum(row[key] for row in selected) for key in ('added', 'deleted')}
        result['totals'][scope]['files'] = len(selected)
    result['added_total'] = sum(row['added'] for row in rows)
    result['deleted_total'] = sum(row['deleted'] for row in rows)
    if pq is not None:
        result['pq_static_adapter'] = pq
    # Commit identity covers immutable measurements. A worktree receipt also
    # identifies the exact current contents rather than just its base HEAD.
    if revision is None:
        result['worktree_file_sha256'] = {
            row['path']: hashlib.sha256(((ROOT if row.get('storage_root') == 'workspace' else SOURCE)
                                         / row['path']).read_bytes()).hexdigest()
            for row in rows
        }
    payload = json.dumps(result, ensure_ascii=False, indent=2) + '\n'
    if args.output:
        output = args.output.resolve()
        evidence = (ROOT / '公共文档/openGauss原生湖存储/验证').resolve()
        if evidence not in output.parents:
            parser.error('--output must be within the project evidence directory')
        output.parent.mkdir(parents=True, exist_ok=True)
        with output.open('x') as stream:
            stream.write(payload)
    print(payload, end='')


if __name__ == '__main__':
    main()
