#!/usr/bin/env python3
"""C ABI regression for retry after a non-aligned delivered Parquet prefix."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[3]


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--library', type=Path, required=True)
    ap.add_argument('--previous-library', type=Path)
    ap.add_argument('--output', type=Path, required=True)
    args = ap.parse_args()
    out = args.output.resolve()
    out.mkdir(parents=True, exist_ok=False)
    duckdb = ROOT / '.reference/opengauss-native-lake/build/duckdb-gcc10/duckdb'
    gcc = ROOT / 'binarylibs/buildtools/gcc10.3/gcc'
    source = ROOT / '.worktrees/opengauss-native-lake/contrib/og_lake/sdk'
    arrow = ROOT / '.reference/opengauss-native-lake/sdk/install/arrow-20.0.0/include'
    test = Path(__file__).with_name('provider_reader_budget_test.c')
    library = args.library.resolve(strict=True)
    fixture = out / 'wide.parquet'
    exe = out / 'reader-budget-c-host'
    env = os.environ.copy()
    env['LD_LIBRARY_PATH'] = ':'.join(str(p) for p in [gcc / 'lib64'] +
                                    [gcc.parent / p / 'lib' for p in ('isl', 'mpc', 'mpfr', 'gmp')])
    result = {'status': 'running', 'library': str(library), 'library_sha256': sha(library),
              'test_sha256': sha(test), 'driver_sha256': sha(Path(__file__)), 'commands': []}

    def run(name, command):
        with (out / (name + '.log')).open('w') as log:
            process = subprocess.run(list(map(str, command)), env=env, stdout=log,
                                     stderr=subprocess.STDOUT, timeout=180)
        result['commands'].append({'name': name, 'argv': list(map(str, command)),
                                   'exit_code': process.returncode})
        return process.returncode

    try:
        sql = """SET threads=1;
CREATE TABLE input AS SELECT i::BIGINT AS id,
 'row-'||printf('%06d',i)||'-'||CASE WHEN i<65 THEN '' ELSE repeat(chr((65+i%26)::INTEGER),65536) END AS payload
 FROM range(320) t(i);
COPY input TO '""" + str(fixture).replace("'", "''") + """'
 (FORMAT PARQUET, COMPRESSION UNCOMPRESSED, ROW_GROUP_SIZE 2048,
  DICTIONARY_SIZE_LIMIT 0, DATA_PAGE_SIZE_LIMIT 131072, FIELD_IDS {id:11,payload:21});
"""
        (out / 'fixture.sql').write_text(sql)
        assert run('fixture', [duckdb, '-no-init', '-batch', '-c', sql]) == 0
        assert run('compile', [gcc / 'bin/gcc', '-std=c11', '-O2', '-Wall', '-Wextra', '-Werror',
                              '-I' + str(source), '-I' + str(arrow), test, '-ldl', '-o', exe]) == 0
        if args.previous_library:
            old = args.previous_library.resolve(strict=True)
            code = run('previous-provider', [exe, old, fixture])
            log = (out / 'previous-provider.log').read_text()
            assert code != 0 and 'SDK error 4:' in log, 'Previous provider did not reproduce the budget failure'
            result['previous_provider'] = {'sha256': sha(old), 'exit_code': code,
                                           'outcome': 'expected memory-budget regression reproduced'}
        assert run('repaired-provider', [exe, library, fixture]) == 0
        result['consumer'] = json.loads((out / 'repaired-provider.log').read_text())
        assert sha(library) == result['library_sha256'] and sha(test) == result['test_sha256']
        result.update(status='passed', fixture_sha256=sha(fixture), consumer_sha256=sha(exe))
    except BaseException as error:
        result.update(status='failed', error=repr(error))
        raise
    finally:
        (out / 'result.json').write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps(result['consumer']))


if __name__ == '__main__':
    main()
