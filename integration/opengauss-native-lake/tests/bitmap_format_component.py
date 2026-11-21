#!/usr/bin/env python3
"""Bounded bitmap Parquet dictionary component over the qualified actual SDK.

Independent DuckDB reads every dictionary payload. A test-only standard portable
format parser checks all stored physical positions against original source CSV;
it is not linked into the implementation. Also checks cross-process reopening,
corruption and every real CRoaring allocation-failure point in the builder.
No native query/planner/coverage/READY integration or production claim.
"""
import argparse
import csv
import datetime
import hashlib
import json
import os
from pathlib import Path
import shutil
import struct
import subprocess
import traceback

ROOT = Path('/home/qiumc/data/code/cpp')


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def portable_positions(raw):
    """Independent oracle for this builder's standard no-run array/bitset form."""
    cookie, count = struct.unpack_from('<II', raw)
    assert cookie == 12346 and count <= 16
    headers = [struct.unpack_from('<HH', raw, 8 + i * 4) for i in range(count)]
    offsets = [struct.unpack_from('<I', raw, 8 + count * 4 + i * 4)[0] for i in range(count)]
    result, previous, end = [], -1, 8 + count * 8
    for i, (high, card_minus_one) in enumerate(headers):
        card = card_minus_one + 1
        assert high > previous and offsets[i] == end
        previous = high
        if card <= 4096:
            values = list(struct.unpack_from('<' + 'H' * card, raw, end))
            end += card * 2
            assert values == sorted(set(values))
        else:
            words = struct.unpack_from('<1024Q', raw, end)
            values = []
            for word_index, word in enumerate(words):
                while word:
                    low = word & -word
                    values.append(word_index * 64 + low.bit_length() - 1)
                    word ^= low
            end += 8192
            assert len(values) == card
        result.extend((high << 16) + v for v in values)
    assert end == len(raw)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--source', type=Path, default=ROOT / '.worktrees/opengauss-native-lake-inline-index')
    parser.add_argument('--roaring-receipt', type=Path, default=ROOT / '公共文档/openGauss原生湖存储/验证/native_bitmap_roaring_oom_dependency_1109_01/build.json')
    parser.add_argument('--sanitize', action='store_true', help='instrument adapter/probe with ASan+UBSan; provider and locked archive are not instrumented')
    args = parser.parse_args()
    if not args.run_id or any(c not in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.-' for c in args.run_id):
        parser.error('run-id must be a new simple identifier')
    out = ROOT / '公共文档/openGauss原生湖存储/验证' / args.run_id
    out.mkdir(exist_ok=False)
    source = args.source.resolve() / 'contrib/og_lake'
    qualification = ROOT / '公共文档/openGauss原生湖存储/验证/sdk_binary_transport_scoped_verified_index.json'
    qualified = json.loads(qualification.read_text())
    provider = Path(json.loads(Path(qualified['build']).read_text())['library'])
    roaring = json.loads(args.roaring_receipt.read_text())
    prefix, library = Path(roaring['prefix']), Path(roaring['library'])
    probe = Path(__file__).with_name('bitmap_format_probe.cpp')
    duckdb = ROOT / '.reference/opengauss-native-lake/build/duckdb-gcc10/duckdb'
    gcc = ROOT / 'binarylibs/buildtools/gcc10.3'
    openssl = ROOT / 'binarylibs/kernel/dependency/openssl/comm'
    inputs = [Path(__file__), probe, source / 'lake_bitmap_format.h', source / 'lake_bitmap_format.cpp',
              source / 'lake_bitmap_codec.h', source / 'lake_bitmap_codec.cpp', source / 'lake_scalar_format.h',
              source / 'sdk/lake_parquet_sdk.h', source / 'vendor/arrow20/abi.h', args.roaring_receipt, qualification]
    result = dict(status='failed', scope=__doc__, assertions=[], inputs={str(p): sha(p) for p in inputs},
                  provider=str(provider), provider_sha256=sha(provider), roaring_library=str(library),
                  roaring_sha256=sha(library), reference_sha256=sha(duckdb), sanitizer=args.sanitize)
    archive = out / 'executed-inputs'
    archive.mkdir()
    for p in inputs:
        shutil.copy2(p, archive / p.name)
    env = dict(os.environ)
    dependencies = [ROOT / '.install/opengauss-native-lake-debug/lib',
                    ROOT / '.reference/opengauss-native-lake/sdk/install/arrow-20.0.0/lib']
    dependencies += [gcc / part for part in ['gcc/lib64', 'isl/lib', 'mpc/lib', 'mpfr/lib', 'gmp/lib']]
    env['LD_LIBRARY_PATH'] = ':'.join(map(str, dependencies))

    def check(name, value, expected=True):
        ok = value == expected
        result['assertions'].append(dict(name=name, observed=value, expected=expected, status='pass' if ok else 'fail'))
        if not ok:
            raise AssertionError(name)

    def command(name, argv, timeout=300):
        result.setdefault('commands', []).append(dict(name=name, argv=list(map(str, argv))))
        with (out / (name + '.log')).open('w') as log:
            subprocess.run(list(map(str, argv)), stdout=log, stderr=subprocess.STDOUT, env=env, timeout=timeout, check=True)
        return (out / (name + '.log')).read_text()

    def literal(s):
        return "'" + str(s).replace("'", "''") + "'"

    def sql(query):
        p = subprocess.run([str(duckdb), '-no-init', '-batch', '-json', '-c', 'SET threads=1;' + query],
                           text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env, timeout=60, check=True)
        return json.loads(p.stdout) if p.stdout.strip() else []

    def validate(exe, artifact, name, **changes):
        a = dict(artifact, **changes)
        return json.loads(command(name, [exe, provider, out, 'validate', a['path'], a['context'], a['manifest'],
                                        a['type'], a['physical_rows'], a['file_id']]))

    try:
        check('actual provider matches qualified BINARY SDK', sha(provider), qualified['provider_sha256'])
        check('private OOM-fixed CRoaring dependency identity', sha(library), roaring['library_sha256'])
        exe = out / 'bitmap-format-probe'
        common = [gcc / 'gcc/bin/g++', '-std=c++14', '-O0', '-g', '-D_GLIBCXX_USE_CXX11_ABI=0',
                  '-Wall', '-Wextra', '-Werror', '-Wno-deprecated-declarations', '-Wno-misleading-indentation',
                  '-I', source, '-I', prefix / 'include', '-I', openssl / 'include', probe,
                  source / 'lake_bitmap_format.cpp', source / 'lake_bitmap_codec.cpp', library,
                  '-L', openssl / 'lib', '-lcrypto', '-ldl', '-o']
        if args.sanitize:
            common[1:1] = ['-fsanitize=address,undefined', '-fno-omit-frame-pointer', '-fno-pie', '-no-pie']
            env['ASAN_OPTIONS'] = 'detect_leaks=1:halt_on_error=1'
            env['UBSAN_OPTIONS'] = 'halt_on_error=1:print_stacktrace=1'
        command('compile', common + [exe])
        result['consumer_sha256'] = sha(exe)
        artifacts_dir = out / 'artifacts'
        artifacts_dir.mkdir()
        observed = json.loads(command('roundtrip', [exe, provider, artifacts_dir, 'roundtrip']))
        check('real SDK roundtrip consumer', observed['status'], 'pass')
        check('int32/int64, all-domain NULL, empty, maximum NDV cases', len(observed['artifacts']), 5)
        check('SDK abort removed all failed/cancelled candidates', not list(artifacts_dir.glob('invalid*.parquet')) and not (artifacts_dir / 'over-limit.parquet').exists())
        result['native_checks'] = observed['checks']
        for index, a in enumerate(observed['artifacts']):
            with Path(a['input']).open() as f:
                source_rows = [(None if r['key'] == 'NULL' else int(r['key']), int(r['row_position'])) for r in csv.DictReader(f)]
            expected = {position: key for key, position in source_rows}
            check('source physical population unique/' + str(index), len(expected), a['physical_rows'])
            dictionary = sql('SELECT key,hex(bitmap) AS payload,cardinality FROM read_parquet(' + literal(a['path']) + ') ORDER BY key NULLS LAST')
            check('independent DuckDB dictionary row count/' + str(index), len(dictionary), a['dictionary_rows'])
            decoded = {}
            for row in dictionary:
                positions = portable_positions(bytes.fromhex(row['payload']))
                if len(positions) != row['cardinality']:
                    raise AssertionError('independent portable cardinality')
                for position in positions:
                    if position in decoded:
                        raise AssertionError('independent dictionary overlap')
                    decoded[position] = row['key']
            check('every independently decoded key/physical position equals input/' + str(index), decoded == expected)
            for qindex, q in enumerate(a['queries']):
                kind, values = q['kind'], set(q['keys'])
                def match(key):
                    if kind in (6, 7):
                        return (key is None) == (kind == 6)
                    if q['null_argument'] or key is None or not values:
                        return False
                    if kind == 8:
                        return key in values
                    argument = q['keys'][0]
                    return {1: key < argument, 2: key <= argument, 3: key == argument,
                            4: key >= argument, 5: key > argument}[kind]
                selected = sorted(p for key, p in source_rows if match(key))
                image = ''.join(str(a['file_id']) + ',' + str(p) + '\n' for p in selected)
                check('independent predicate positions/' + str(index) + '/' + str(qindex),
                      [q['rows'], q['sha256']], [len(selected), hashlib.sha256(image.encode()).hexdigest()])
            reopened = validate(exe, a, 'reopen-' + str(index))
            check('fresh process validates persisted artifact/' + str(index), reopened['code'], 0)
            rejected = validate(exe, a, 'context-' + str(index), context='b' * 64)
            check('context change publishes no handle/' + str(index), [rejected['code'], rejected['partial']], [2, False])
        a = observed['artifacts'][0]
        truncated = artifacts_dir / 'truncated.parquet'
        raw = Path(a['path']).read_bytes()
        truncated.write_bytes(raw[:-11])
        bad = validate(exe, a, 'truncated', path=str(truncated))
        check('truncated Parquet rejected before handle', bad['code'] != 0 and not bad['partial'])
        bad = validate(exe, a, 'wrong-population', physical_rows=a['physical_rows'] + 1)
        check('missing source coverage rejected before handle', bad['code'] != 0 and not bad['partial'])
        wrong = artifacts_dir / 'schema.parquet'
        sql('COPY (SELECT 1::BIGINT AS bad) TO ' + literal(wrong) + ' (FORMAT PARQUET)')
        bad = validate(exe, a, 'wrong-schema', path=str(wrong))
        check('wrong dictionary schema rejected before handle', bad['code'] != 0 and not bad['partial'])
        damaged_dir = out / 'malformed'
        damaged_dir.mkdir()
        damaged = json.loads(command('malformed', [exe, provider, damaged_dir, 'malformed']))
        for case in damaged['cases']:
            check('correct-schema damage/' + case['name'], [case['code'], case['partial']], [2, False])
        result['malformed'] = damaged
        baseline_dir = out / 'fault-baseline'
        baseline_dir.mkdir()
        baseline = json.loads(command('fault-baseline', [exe, provider, baseline_dir, 'fault', 0]))
        check('baseline builder C allocations released', baseline['outstanding'], 0)
        result['fault_allocation_points'] = baseline['calls']
        for point in range(1, baseline['calls'] + 1):
            directory = out / ('fault-' + str(point))
            directory.mkdir()
            fault = json.loads(command('fault-' + str(point), [exe, provider, directory, 'fault', point]))
            check('builder real C allocation failure/' + str(point),
                  [fault['status'], fault['failures'], fault['outstanding'], fault['code']], ['pass', 1, 0, 4])
        check('all executed source inputs unchanged', all(sha(p) == h for p, h in result['inputs'].items()))
        check('provider unchanged', sha(provider), result['provider_sha256'])
        check('CRoaring private dependency unchanged', sha(library), result['roaring_sha256'])
        result.update(status='passed', probe=observed, artifact_sha256={str(p): sha(p) for p in artifacts_dir.glob('*.parquet')})
    except BaseException as e:
        result.update(reason=str(e), traceback=traceback.format_exc())
    result['finished_at'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    (out / 'result.json').write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps(dict(status=result['status'], assertions=len(result['assertions']), native_checks=result.get('native_checks'),
                          reason=result.get('reason'), result=str(out / 'result.json')), ensure_ascii=False))
    return 0 if result['status'] == 'passed' else 1


if __name__ == '__main__':
    raise SystemExit(main())
