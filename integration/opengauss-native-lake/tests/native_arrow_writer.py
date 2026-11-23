#!/usr/bin/env python3
"""Test-only native TableAM/Arrow/Parquet conversion, with no lake publication."""
import argparse
from collections import Counter
import hashlib
import json
import os
import shutil
import subprocess
from pathlib import Path
import sys
import traceback
from types import SimpleNamespace

ROOT = Path('/home/qiumc/data/code/cpp')
DRIVER = ROOT / 'integration/opengauss-native-lake'
sys.path[:0] = [str(DRIVER), str(DRIVER / 'tests')]
from inline_fixture import query, scalar
from lake_harness import sha256, write_json
from native_test_context import OwnedDatabase
from native_table_create import Case
from permission_api import OwnedRoles, cleanup_roles
from reverse_reference import Reference, literal


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--manifest', type=Path, help='Unused genuine EMPTY bundle; omit to generate a new owned fixture')
    parser.add_argument('--probe-receipt', type=Path, required=True)
    parser.add_argument('--storage', choices=['heap','ustore'], default='heap')
    args = parser.parse_args()
    output = ROOT / '公共文档/openGauss原生湖存储/验证' / args.run_id
    output.mkdir(exist_ok=False)
    result = {'status': 'fail', 'scope': 'Actual native TableAM -> Arrow C Data -> Parquet writer only',
              'assertions': [], 'artifacts': [], 'limitations': [
                  'Test-only loadable probe; not a product flush or catalog/file publication.',
                  'No remote, crash, orphan cleanup or RAC qualification.']}
    control = roles = db = None
    inputs = [Path(__file__), DRIVER / 'native_test_context.py', DRIVER / 'tests/native_table_create.py',
              DRIVER / 'tests/permission_api.py', DRIVER / 'tests/reverse_reference.py',
              DRIVER / 'tests/catalog_fixture_bundle.py', DRIVER / 'tests/empty_reference_fixture.py',
              DRIVER / 'inline_fixture.py']
    frozen = {str(p): sha256(p) for p in inputs}
    archive = output / 'executed-inputs'
    archive.mkdir(exist_ok=False)
    for i, path in enumerate(inputs):
        shutil.copy2(path, archive / (str(i) + '-' + path.name))
    result['input_hashes'] = frozen

    def check(name, actual, expected=True):
        passed = actual == expected
        result['assertions'].append({'name': name, 'status': 'pass' if passed else 'fail',
                                     'observed': actual, 'expected': expected})
        if not passed:
            raise AssertionError(name)
    try:
        import psycopg2
        from psycopg2 import sql
        if args.manifest is None:
            args.manifest = output / 'fixture/manifest.json'
            with (output / 'fixture-generation.log').open('w') as log:
                subprocess.run([sys.executable, str(DRIVER / 'tests/catalog_fixture_bundle.py'),
                                '--run-id', args.run_id, '--output', str(args.manifest)],
                               stdout=log, stderr=subprocess.STDOUT, check=True, timeout=900)
        receipt = json.loads(args.probe_receipt.read_text())
        shutil.copy2(args.probe_receipt, output / 'probe-receipt.json')
        result['probe_receipt_sha256'] = sha256(args.probe_receipt)
        native = ROOT / '.install/opengauss-native-lake-debug/bin/gaussdb'
        check('probe matches exact native runtime', sha256(native), receipt['native_binary_sha256'])
        check('installed test library matches compiled bytes', sha256(receipt['library']), receipt['library_sha256'])
        check('all actual compile headers still match', all(sha256(p) == h for p, h in receipt['headers'].items()))
        control = psycopg2.connect(host=str(ROOT / '.runtime/opengauss-native-lake/debug/socket'),
                                  port=55560, dbname='postgres', user=os.getenv('USER', 'qiumc'))
        control.autocommit = True
        roles = OwnedRoles(control, args.run_id, output)
        case_args = SimpleNamespace(mode='net_dml', sql_version='0.10', manifest=args.manifest,
                                    output=output / 'result.json')
        with OwnedDatabase(args.run_id, output) as db:
            t = Case(db, case_args, result, roles)
            if args.storage == 'ustore':
                query(t.a, 'SET enable_default_ustore_table=on')
            query(t.a, "CREATE FOREIGN TABLE biz.orders(id integer,val integer,flag boolean,bigval bigint,note text) "
                  "SERVER created_lake OPTIONS(binding_mode 'create',lake_schema 'main')")
            query(t.a, 'SET enable_default_ustore_table=off')
            long_text = ''.join(hashlib.sha256(str(i).encode()).hexdigest() + '湖' for i in range(512))
            rows = [(1, 2147483647, True, 9223372036854775807, long_text),
                    (2, -2147483648, False, -9223372036854775808, None),
                    (3, None, None, None, '')] + [(i, i, i % 2 == 0, i * 10000000000, long_text) for i in range(4, 44)]
            query(t.a, 'INSERT INTO biz.orders VALUES ' + ','.join(['(%s,%s,%s,%s,%s)'] * len(rows)),
                  tuple(value for row in rows for value in row))
            query(t.a, "UPDATE biz.orders SET val=7,flag=false,note=note||'首次'WHERE id=1")
            query(t.a, "UPDATE biz.orders SET val=8,flag=true,note=note||'再次'WHERE id=1")
            carrier = scalar(t.a, 'SELECT table_name FROM created_metadata.ducklake_inlined_data_tables')
            options = scalar(t.a, 'SELECT reloptions FROM pg_class WHERE oid=(%s)::regclass',
                             ('created_metadata.' + carrier,)) or []
            check('actual native carrier TableAM matches requested storage',
                  'storage_type=ustore' in [str(v).lower() for v in options], args.storage == 'ustore')
            raw = query(t.a, sql.SQL('SELECT _ducklake_row_id,_ducklake_begin_snapshot,_ducklake_end_snapshot,'
                                    'id,val,flag,bigval,note FROM created_metadata.{}').format(sql.Identifier(carrier)))
            check('normal native INSERT/UPDATE generated all physical versions', len(raw), 45)
            by_version = {(r[0], r[1]): r for r in raw}
            check('native versions have distinct identity/birth pairs', len(by_version), len(raw))
            write_json(output / 'actual-native-inline-versions.json', {'columns': [
                'logical_row_id', 'birth', 'end', 'id', 'val', 'flag', 'bigval', 'note'], 'rows': raw})
            columns = query(t.a, 'SELECT column_id,column_name FROM created_metadata.ducklake_column ORDER BY column_order')
            query(t.a, 'CREATE SCHEMA writer_probe')
            query(t.a, sql.SQL('CREATE FUNCTION writer_probe.write_versions(regclass,text,integer) RETURNS text '
                               'AS {},{} LANGUAGE C STRICT').format(sql.Literal(receipt['sql_module_path']),
                                                                    sql.Literal('lake_test_write_inline_versions')))
            original = digest(t.protocol())
            lock = json.loads((DRIVER / 'source-lock.json').read_text())
            reference_receipt = json.loads((ROOT / '.reference/opengauss-native-lake/build/receipt.json').read_text())
            check('reference receipt matches active source lock', reference_receipt['source_lock_hash'], sha256(DRIVER / 'source-lock.json'))
            ref = Reference(output, lock, reference_receipt)
            for capacity in [1, 2, 4096]:
                path = output / ('batch-' + str(capacity) + '.parquet')
                written = json.loads(scalar(t.a, "SELECT writer_probe.write_versions('biz.orders'::regclass,%s,%s)",
                                            (str(path), capacity)))
                check('writer preserved all versions: ' + str(capacity), written['rows'], len(raw))
                check('writer confirmed local file/directory fsync: ' + str(capacity), written['local_fsync_complete'], 1)
                check('writer byte count equals actual immutable output: ' + str(capacity), written['bytes'], path.stat().st_size)
                found = ref.run('SELECT file_row_number,* FROM read_parquet(' + literal(str(path)) +
                                ',file_row_number=true) ORDER BY file_row_number', 'read-native-writer-' + str(capacity), result=True)
                actual = Counter(json.dumps([r[n] for n in ['_ducklake_internal_row_id', '_ducklake_internal_snapshot_id',
                                  'id', 'val', 'flag', 'bigval', 'note']], ensure_ascii=False) for r in found)
                expected = Counter(json.dumps([r[0], r[1], *r[3:]], ensure_ascii=False) for r in raw)
                check('independent Parquet reader matches every native value/identity: ' + str(capacity), actual == expected)
                mapping = [[r['file_row_number'], r['_ducklake_internal_row_id'], r['_ducklake_internal_snapshot_id'],
                            by_version[(r['_ducklake_internal_row_id'], r['_ducklake_internal_snapshot_id'])][2]] for r in found]
                check('output positions map back to exact original version/end: ' + str(capacity), written['mapping'], mapping)
                schema = ref.run('SELECT name,field_id FROM parquet_schema(' + literal(str(path)) + ') WHERE field_id IS NOT NULL',
                                 'schema-native-writer-' + str(capacity), result=True)
                check('standard field IDs preserved: ' + str(capacity), [(r['field_id'], r['name']) for r in schema],
                      columns + [(2147483540, '_ducklake_internal_row_id'), (2147483539, '_ducklake_internal_snapshot_id')])
                groups = ref.run('SELECT num_row_groups FROM parquet_file_metadata(' + literal(str(path)) + ')',
                                 'groups-native-writer-' + str(capacity), result=True)[0]['num_row_groups']
                check('row/byte capacity caused real multiple output groups: ' + str(capacity), groups > 1)
                check('conversion did not publish or alter source catalog: ' + str(capacity), digest(t.protocol()), original)
                result.setdefault('outputs', []).append({'path': str(path), 'sha256': sha256(path), 'written': written, 'row_groups': groups})
            check('native runtime stayed fixed during conversion', sha256(native), receipt['native_binary_sha256'])
            check('executed helper inputs stayed fixed', {str(p): sha256(p) for p in inputs}, frozen)
        result['status'] = 'pass'
    except BaseException as exc:
        result.update(reason=str(exc), traceback=traceback.format_exc())
    finally:
        if db is not None:
            result['database_cleanup'] = db.cleanup_result
            result['artifacts'].extend(db.artifacts())
        if roles is not None:
            try:
                if db is None or not db.cleanup_result or db.cleanup_result['returncode'] != 0:
                    raise RuntimeError('database cleanup unproven; roles retained')
                result['role_cleanup'] = cleanup_roles(control, roles.path, args.run_id)
            except BaseException as exc:
                result.update(status='fail', cleanup_error=str(exc))
            result = roles.redact(result)
        if control is not None:
            control.close()
        write_json(output / 'result.json', result)
    print(json.dumps({'status': result['status'], 'reason': result.get('reason'),
                      'assertions': len(result['assertions']), 'result': str(output / 'result.json')}))
    return 0 if result['status'] == 'pass' else 1


if __name__ == '__main__':
    raise SystemExit(main())
