#!/usr/bin/env python3
"""Qualify optional synchronous local deletion and original ABI1/2 lifetimes."""
import argparse
import datetime
import json
import os
from pathlib import Path
import shutil
import subprocess
import traceback
from build_provider import ROOT, PREFIX, GCC, SOURCE, sha, inputs, dependencies


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--evidence', type=Path, required=True)
    parser.add_argument('--input', type=Path, required=True)
    parser.add_argument('--provider', type=Path, default=PREFIX / 'lib/libog_lake_sdk_arrow.so')
    args = parser.parse_args()
    evidence = args.evidence.resolve()
    evidence.mkdir(parents=True, exist_ok=False)
    test = Path(__file__).with_name('provider_delete_test.c')
    faults = test.with_name('provider_io_faults.c')
    life = test.with_name('provider_lifecycle_test.c')
    genuine = args.input.resolve()
    library = args.provider.resolve()
    files = [Path(__file__), test, faults, life, life.with_name('provider_reader_test.c'),
             SOURCE / 'lake_parquet_sdk.h', genuine, library]
    result = {'status': 'failed', 'scope': 'Standalone real local maintenance deletion and original candidate lifecycle; no native catalog GC, remote or RAC qualification',
              'started_at': datetime.datetime.now(datetime.timezone.utc).isoformat(),
              'inputs': {str(p): sha(p) for p in files},
              'provider_sources_before': inputs(), 'dependencies_before': dependencies()}
    archive = evidence / 'executed-inputs'
    archive.mkdir()
    for source in files[:-2]:
        shutil.copy2(source, archive / source.name)
    support = [GCC / 'lib64'] + [GCC.parent / name / 'lib' for name in ['isl', 'mpc', 'mpfr', 'gmp']]
    env = os.environ.copy()
    env['LD_LIBRARY_PATH'] = ':'.join(map(str, [PREFIX / 'lib'] + support))
    flags = [str(GCC / 'bin/gcc'), '-std=c11', '-O2', '-Wall', '-Wextra', '-Werror']
    result['commands'] = []

    def command(name, argv, runtime=env):
        result['commands'].append(list(map(str, argv)))
        with (evidence / (name + '.log')).open('w') as log:
            subprocess.run(list(map(str, argv)), env=runtime, stdout=log, stderr=subprocess.STDOUT,
                           check=True, timeout=120)

    try:
        command('compile-faults', flags + ['-fPIC', '-shared', faults, '-ldl', '-o', evidence / 'faults.so'])
        command('compile-delete', flags + ['-I' + str(SOURCE), test, '-ldl', '-o', evidence / 'delete-consumer'])
        owned = evidence / 'owned-files'
        owned.mkdir()
        runtime = env.copy()
        runtime['LD_PRELOAD'] = str(evidence / 'faults.so')
        command('delete', [evidence / 'delete-consumer', library, owned, genuine], runtime)
        result['delete'] = json.loads((evidence / 'delete.log').read_text())
        command('compile-lifecycle', flags + ['-I' + str(SOURCE), '-I' + str(PREFIX / 'include'), life,
                                             '-ldl', '-o', evidence / 'lifecycle-consumer'])
        owned_life = evidence / 'lifecycle-files'
        owned_life.mkdir()
        command('lifecycle', [evidence / 'lifecycle-consumer', library, genuine, owned_life], runtime)
        result['original_lifecycle_cases'] = [json.loads(line) for line in (evidence / 'lifecycle.log').read_text().splitlines()]
        if result['delete']['status'] != 'pass' or len(result['original_lifecycle_cases']) != 6:
            raise AssertionError('Delete or original lifecycle consumer did not pass')
        result['provider_sources_after'] = inputs()
        result['dependencies_after'] = dependencies()
        if result['provider_sources_before'] != result['provider_sources_after'] or result['dependencies_before'] != result['dependencies_after']:
            raise RuntimeError('Provider source or Arrow dependency changed')
        if any(sha(p) != h for p,h in result['inputs'].items()):
            raise RuntimeError('Executed test, genuine input or provider changed')
        result['status'] = 'passed'
    except BaseException as exc:
        result.update(reason=str(exc), traceback=traceback.format_exc())
    (evidence / 'result.json').write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps({'status': result['status'], 'reason': result.get('reason'),
                      'delete_checks': result.get('delete', {}).get('checks')}))
    return 0 if result['status'] == 'passed' else 1


if __name__ == '__main__':
    raise SystemExit(main())
