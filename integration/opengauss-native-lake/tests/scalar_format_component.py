#!/usr/bin/env python3
"""Real Parquet sorted-page roundtrip with independent DuckDB SQL results.

This qualifies a portable component only, not native index build/publication,
file coverage, query integration, the private SDK, RAC or production readiness.
"""
import argparse
import csv
import datetime
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import traceback

ROOT = Path('/home/qiumc/data/code/cpp')


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--source', type=Path, default=ROOT / '.worktrees/opengauss-native-lake-inline-index')
    parser.add_argument('--provider', type=Path, default=ROOT / '.install/opengauss-native-lake-debug/lib/postgresql/og_lake_sdk/libog_lake_sdk_arrow.so')
    args = parser.parse_args()
    if not args.run_id or any(c not in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.-' for c in args.run_id):
        parser.error('run-id must be a simple new identifier')
    out = ROOT / '公共文档/openGauss原生湖存储/验证' / args.run_id
    out.mkdir(exist_ok=False)
    source = args.source.resolve() / 'contrib/og_lake'
    probe = Path(__file__).with_name('scalar_format_probe.cpp')
    provider = args.provider.resolve()
    duckdb = ROOT / '.reference/opengauss-native-lake/build/duckdb-gcc10/duckdb'
    gcc = ROOT / 'binarylibs/buildtools/gcc10.3'
    openssl = ROOT / 'binarylibs/kernel/dependency/openssl/comm'
    files = [Path(__file__), probe, source / 'lake_scalar_format.cpp', source / 'lake_scalar_format.h',
             source / 'sdk/lake_parquet_sdk.h', source / 'vendor/arrow20/abi.h']
    result = {'status': 'failed', 'scope': __doc__, 'assertions': [],
              'inputs': {str(p): sha(p) for p in files},
              'provider': str(provider), 'provider_sha256': sha(provider), 'reference_binary_sha256': sha(duckdb),
              'compiler_sha256': sha(gcc / 'gcc/bin/g++')}
    archive = out / 'executed-inputs'
    archive.mkdir()
    for path in files:
        shutil.copy2(path, archive / path.name)
    env = dict(os.environ)
    libraries = [ROOT / '.install/opengauss-native-lake-debug/lib',
                 ROOT / '.reference/opengauss-native-lake/sdk/install/arrow-20.0.0/lib']
    libraries += [gcc / part for part in ['gcc/lib64', 'isl/lib', 'mpc/lib', 'mpfr/lib', 'gmp/lib']]
    env['LD_LIBRARY_PATH'] = ':'.join(map(str, libraries))

    def check(name, observed, expected=True):
        ok = observed == expected
        result['assertions'].append({'name': name, 'observed': observed, 'expected': expected,
                                     'status': 'pass' if ok else 'fail'})
        if not ok:
            raise AssertionError(name)

    def command(name, argv):
        result.setdefault('commands', []).append({'name': name, 'argv': list(map(str, argv))})
        with (out / (name + '.log')).open('w') as log:
            subprocess.run(list(map(str, argv)), env=env, stdout=log, stderr=subprocess.STDOUT,
                           check=True, timeout=300)

    def literal(value):
        return "'" + str(value).replace("'", "''") + "'"

    def query(sql):
        process = subprocess.run([str(duckdb), '-no-init', '-batch', '-json', '-c', 'SET threads=1;' + sql],
                                 text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True, timeout=30)
        return json.loads(process.stdout) if process.stdout.strip() else []

    def position_image(rows):
        data = ''.join(str(row['file_id']) + ',' + str(row['row_position']) + '\n' for row in rows)
        return {'rows': len(rows), 'sha256': hashlib.sha256(data.encode()).hexdigest()}

    try:
        exe = out / 'scalar-format-probe'
        command('compile', [gcc / 'gcc/bin/g++', '-std=c++14', '-O0', '-g', '-D_GLIBCXX_USE_CXX11_ABI=0',
                           '-Wall', '-Wextra', '-Werror', '-Wno-deprecated-declarations', '-Wno-misleading-indentation',
                           '-I', source, '-I', openssl / 'include', probe, source / 'lake_scalar_format.cpp',
                           '-L', openssl / 'lib', '-lcrypto', '-ldl', '-o', exe])
        check('strict standalone Debug compilation produced an executable', exe.is_file())
        result['consumer_binary_sha256'] = sha(exe)
        data = out / 'artifacts'
        data.mkdir()
        command('probe', [exe, provider, data])
        probe_result = json.loads((out / 'probe.log').read_text())
        check('real provider C++ consumer succeeds', probe_result['status'], 'pass')
        check('roundtrip includes multiple-page int64/int32, all-NULL and empty artifacts',
              len(probe_result['artifacts']), 4)
        result['native_checks'] = probe_result['checks']
        for artifact in probe_result['artifacts']:
            with Path(artifact['input']).open(newline='') as handle:
                expected = [{'values': None if row['values'] == 'NULL' else int(row['values']),
                             'file_id': int(row['file_id']), 'row_position': int(row['row_position'])}
                            for row in csv.DictReader(handle)]
            expected.sort(key=lambda row: (row['file_id'], row['row_position']))
            actual = query('SELECT "values",file_id,row_position FROM read_parquet(' + literal(artifact['data']) +
                           ') ORDER BY file_id,row_position')
            check('independent full Parquet decode equals every original physical input: ' + Path(artifact['data']).parent.name,
                  {'rows': len(actual), 'sha256': hashlib.sha256(repr(actual).encode()).hexdigest()},
                  {'rows': len(expected), 'sha256': hashlib.sha256(repr(expected).encode()).hexdigest()})
            check('physical input addresses are unique', len({(r['file_id'], r['row_position']) for r in expected}), len(expected))
            for number, item in enumerate(artifact['queries']):
                kind, keys = item['kind'], item['keys']
                if kind in (6, 7):
                    predicate = '"values" IS ' + ('NOT ' if kind == 7 else '') + 'NULL'
                elif kind == 8:
                    array = 'NULL::BIGINT[]' if item['null_argument'] else '[' + ','.join(literal(v) + '::BIGINT' for v in keys) + ']::BIGINT[]'
                    predicate = '"values"=ANY(' + array + ')'
                else:
                    operator = {1: '<', 2: '<=', 3: '=', 4: '>=', 5: '>'}[kind]
                    value = 'NULL' if item['null_argument'] else literal(keys[0])
                    predicate = '"values"' + operator + value + '::BIGINT'
                selected = query('SELECT file_id,row_position FROM read_parquet(' + literal(artifact['data']) +
                                 ') WHERE ' + predicate + ' ORDER BY file_id,row_position')
                check('independent SQL positions ' + Path(artifact['data']).parent.name + '/' + str(number),
                      {'rows': item['rows'], 'sha256': item['sha256']}, position_image(selected))
        check('all compiled/executed inputs unchanged', all(sha(Path(p)) == h for p, h in result['inputs'].items()))
        check('genuine provider unchanged', sha(provider), result['provider_sha256'])
        check('pinned independent SQL engine unchanged', sha(duckdb), result['reference_binary_sha256'])
        result.update(status='passed', probe=probe_result,
                      artifacts={str(p): sha(p) for p in data.rglob('*') if p.is_file()})
    except BaseException as exc:
        result.update(reason=str(exc), traceback=traceback.format_exc())
    result['finished_at'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    (out / 'result.json').write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps({'status': result['status'], 'checks': len(result['assertions']),
                      'native_checks': result.get('native_checks'), 'reason': result.get('reason'),
                      'result': str(out / 'result.json')}, ensure_ascii=False))
    return 0 if result['status'] == 'passed' else 1


if __name__ == '__main__':
    raise SystemExit(main())
