#!/usr/bin/env python3
"""Native bigint/FLOAT4/FLOAT8 bitmap files with heap or Ustore inline input.

Every result is compared with native heap SQL. Independent DuckDB reads the
real Parquet source and every portable bitmap; stored key order is checked
against native SQL total ordering. RC Debug correctness only, not performance,
PQ, SMP, remote/RAC, automatic repair, physical GC or commercial acceptance.
Run the six type/storage combinations serially with fresh fixture run IDs.
"""
import argparse
import hashlib
import json
import math
from pathlib import Path
import re
import shutil
import struct
import subprocess
import sys
import traceback
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[3]
D = ROOT / 'integration/opengauss-native-lake'
sys.path[:0] = [str(D), str(D / 'tests')]
from bitmap_format_component import portable_positions
from file_dml_acceptance import check_build
from inline_fixture import query, scalar
from lake_harness import sha256, validate_run_id, write_json
from native_auto_flush import connect_control
from native_runtime import INSTALL
from native_table_create import Case
from native_test_context import OwnedDatabase
from permission_api import OwnedRoles, cleanup_roles


def encoded_key(text, kind):
    """Test-only IEEE order oracle, additionally checked against native SQL."""
    if text is None:
        return None
    if kind == 'bigint':
        return int(text)
    value = float(text)
    if kind == 'real':
        value = struct.unpack('>f', struct.pack('>f', value))[0]
    if math.isnan(value):
        return (1 << 63) - 1
    if value == 0:
        return 0
    if math.isinf(value):
        return -(1 << 63) if value < 0 else (1 << 63) - 2
    bits = struct.unpack('>Q', struct.pack('>d', value))[0]
    if bits & (1 << 63):
        bits = ((~bits) & ((1 << 64) - 1)) ^ (1 << 63)
    return bits - (1 << 64) if bits & (1 << 63) else bits


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--build-receipt', required=True, type=Path)
    parser.add_argument('--type', choices=['bigint', 'real', 'double'], required=True)
    parser.add_argument('--storage', choices=['heap', 'ustore'], required=True)
    args = parser.parse_args()
    validate_run_id(args.run_id)
    out = ROOT / '公共文档/openGauss原生湖存储/验证' / args.run_id
    out.mkdir(exist_ok=False)
    result = dict(status='fail', scope=__doc__, type=args.type, storage=args.storage, assertions=[])
    control = db = roles = case = None
    paths = [Path(__file__), D / 'tests/bitmap_format_component.py',
             D / 'tests/native_table_create.py', D / 'tests/permission_api.py',
             D / 'native_test_context.py', D / 'native_runtime.py',
             D / 'tests/inline_cleanup.py', D / 'tests/file_dml_acceptance.py',
             D / 'inline_fixture.py', D / 'tests/native_auto_flush.py',
             D / 'tests/catalog_fixture_bundle.py', D / 'tests/empty_reference_fixture.py',
             D / 'tests/empty_manifest.py', D / 'tests/empty_catalog_oracle.py',
             D / 'tests/create_reference_oracle.py', D / 'tests/reverse_reference.py',
             D / 'reference.sh', D / 'fixture.py', D / 'lake_harness.py', D / 'source-lock.json']
    result['inputs'] = {str(path): sha256(path) for path in paths}
    archive = out / 'executed-inputs'
    archive.mkdir()
    for path in paths:
        shutil.copy2(path, archive / path.name)
    provider = INSTALL / 'lib/postgresql/og_lake_sdk/libog_lake_sdk_arrow.so'
    duckdb = ROOT / '.reference/opengauss-native-lake/build/duckdb-gcc10/duckdb'

    def image(value):
        if isinstance(value, list) and len(value) > 30:
            return dict(rows=len(value), sha256=hashlib.sha256(repr(value).encode()).hexdigest())
        return value

    def check(name, observed, expected=True):
        good = observed == expected
        result['assertions'].append(dict(name=name, observed=image(observed), expected=image(expected),
                                         status='pass' if good else 'fail'))
        if not good:
            raise AssertionError(name + ': ' + repr(image(observed)))

    def literal(value):
        return "'" + str(value).replace("'", "''") + "'"

    def independent(statement, label):
        (out / (label + '.sql')).write_text(statement + '\n')
        proc = subprocess.run([str(duckdb), '-no-init', '-batch', '-json', '-c', 'SET threads=1;' + statement],
                              capture_output=True, text=True, timeout=90)
        (out / (label + '.stdout')).write_text(proc.stdout)
        (out / (label + '.stderr')).write_text(proc.stderr)
        if proc.returncode:
            raise RuntimeError('Independent DuckDB failed: ' + label)
        return json.loads(proc.stdout) if proc.stdout.strip() else []

    try:
        built = check_build(args.build_receipt)
        result.update(source_head=built['source_head'], binary_sha256=built['binary_sha256'],
                      provider_sha256=sha256(provider), reference_binary_sha256=sha256(duckdb),
                      build_receipt=str(args.build_receipt.resolve()),
                      build_receipt_sha256=sha256(args.build_receipt))
        control = connect_control()
        check('automatic maintenance remains off', scalar(control, 'SHOW enable_lake_maintenance'), 'off')
        with (out / 'fixture.log').open('w') as log:
            subprocess.run([sys.executable, str(D / 'tests/catalog_fixture_bundle.py'), '--run-id', args.run_id,
                            '--output', str(out / 'fixture/manifest.json')], stdout=log,
                           stderr=subprocess.STDOUT, check=True, timeout=300)
        db = OwnedDatabase(args.run_id, out).create()
        roles = OwnedRoles(control, args.run_id, out)
        case = Case(db, SimpleNamespace(mode='net_dml', sql_version='0.16',
                    manifest=out / 'fixture/manifest.json', output=out / 'result.json', run_id=args.run_id), result, roles)
        conn = case.a
        sql_type = {'bigint': 'bigint', 'real': 'real', 'double': 'double precision'}[args.type]
        send = {'bigint': 'int8send', 'real': 'float4send', 'double': 'float8send'}[args.type]
        algorithm = 1 if args.type == 'bigint' else 2
        query(conn, 'CREATE TABLE biz.exact(id integer,k ' + sql_type + ',note text)')
        query(conn, 'SET enable_default_ustore_table=' + ('on' if args.storage == 'ustore' else 'off'))
        query(conn, 'CREATE FOREIGN TABLE biz.keys(id integer,k ' + sql_type
              + ",note text) SERVER created_lake OPTIONS(binding_mode 'create',lake_schema 'main')")
        query(conn, 'RESET enable_default_ustore_table')
        query(conn, "SELECT og_lake.set_option('biz.keys'::regclass,'data_inlining_row_limit','50000')")
        carrier = query(conn, 'SELECT c.reloptions FROM created_metadata.ducklake_inlined_data_tables t '
                        'JOIN pg_class c ON c.relname=t.table_name AND c.relnamespace='
                        "(SELECT oid FROM pg_namespace WHERE nspname='created_metadata')")
        check('actual inline carrier uses requested storage', len(carrier) == 1
              and ('storage_type=ustore' in (carrier[0][0] or [])) == (args.storage == 'ustore'))

        def both(statement):
            query(conn, 'BEGIN')
            try:
                for table in ['biz.keys', 'biz.exact']:
                    query(conn, statement.format(table=table))
                query(conn, 'COMMIT')
            except BaseException:
                query(conn, 'ROLLBACK')
                raise

        finite = '(i%127-63)::bigint' if args.type == 'bigint' else '((i-256)::double precision/4)::' + sql_type
        both('INSERT INTO {table} SELECT i,CASE WHEN i%19=0 THEN NULL ELSE ' + finite
             + " END,'row/'||i::text FROM generate_series(1,511)i")
        if args.type == 'bigint':
            special = ['-9223372036854775808', '9223372036854775807', '-2147483649', '2147483648',
                       '-32769', '32768', '0', '-1', '1', '9223372036854775807']
        else:
            tiny = '1e-30' if args.type == 'real' else '1e-300'
            huge = '1e30' if args.type == 'real' else '1e300'
            special = ['0', '-0', 'NaN', 'Infinity', '-Infinity', tiny, '-' + tiny, huge, '-' + huge,
                       '1.00000011920928955078125', '1.000000059604644775390625']
        both('INSERT INTO {table} VALUES' + ','.join('(' + str(1001 + i) + ',' + literal(value)
             + '::' + sql_type + ',' + literal('special/' + value) + ')' for i, value in enumerate(special)))
        query(conn, 'ANALYZE biz.keys')
        query(conn, "SELECT og_lake.flush('biz.keys'::regclass)")
        store = out / 'index-store'
        store.mkdir()
        query(conn, "SELECT og_lake.set_option('created_lake'::name,'index_root',%s::text)", (str(store),))
        index_uuid = str(scalar(conn, "SELECT og_lake.create_lake_index('biz.keys'::regclass,'typed_bitmap',%s::jsonb)",
                               (json.dumps(dict(version=1, kind='scalar', method='bitmap', column='k')),)))

        def build():
            query(conn, 'SELECT og_lake.build_lake_index(%s::uuid)', (index_uuid,))

        def segments():
            return query(conn, 'SELECT segment_uuid::text,state,artifact_root_uri,manifest_hash,algorithm_version,format_version '
                         'FROM og_lake_internal.index_segment WHERE index_uuid=%s::uuid ORDER BY segment_uuid', (index_uuid,))

        def equal(predicate='true', values=None, label=None):
            statement = 'SELECT id,encode(' + send + "(k),'hex'),note FROM {} WHERE " + predicate + ' ORDER BY id'
            check(label or 'native heap differential: ' + predicate, query(conn, statement.format('biz.keys'), values),
                  query(conn, statement.format('biz.exact'), values))

        def plan(label):
            text = '\n'.join(row[0] for row in query(conn,
                'EXPLAIN (ANALYZE TRUE,COSTS FALSE) SELECT id,k FROM biz.keys WHERE k=0::'
                + ('bigint' if args.type == 'bigint' else 'double precision')))
            path = out / (label + '-plan.txt')
            path.write_text(text + '\n')
            result.setdefault('actual_plans', []).append(dict(label=label, path=str(path), text=text))
            return text

        def bitmap_actual(label):
            text = plan(label)
            check('actual verified bitmap access: ' + label, 'Actual File Access: verified bitmap Parquet candidates' in text)
            count = re.findall(r'^\s*Verified Bitmap File Coverage:\s*(\d+)\s*$', text, re.M)
            check('actual positive bitmap coverage: ' + label, len(count) == 1 and int(count[0]) > 0)
            return text

        build()
        initial = segments()
        check('typed bitmap has correct algorithm and format2', bool(initial)
              and all(row[4:] == (algorithm, 2) for row in initial))
        build()
        check('repeat build reuses healthy typed bitmap bytes and segments', segments(), initial)
        coverage = query(conn, 'SELECT c.physical_row_count,c.source_path,s.artifact_root_uri '
                         'FROM og_lake_internal.index_coverage c JOIN og_lake_internal.index_segment s '
                         'USING(segment_uuid,index_uuid,definition_generation) WHERE c.index_uuid=%s::uuid AND c.active', (index_uuid,))
        check('small typed source has exactly one physical coverage', len(coverage), 1)
        population, source, directory = coverage[0]
        directory = Path(directory).resolve()
        check('bitmap artifact belongs to exact attempt', directory.is_relative_to(store.resolve()))
        dictionaries = independent('SELECT key,hex(bitmap) AS payload,cardinality FROM read_parquet('
                                   + literal(directory / 'bitmap_data.parquet') + ') ORDER BY key NULLS LAST', 'dictionary')
        physical = independent('SELECT id,CAST(k AS VARCHAR) AS key_text,file_row_number AS row_position FROM read_parquet('
                               + literal(source) + ',file_row_number=true) ORDER BY file_row_number', 'source')
        decoded = []
        position_to_id = {row['row_position']: row['id'] for row in physical}
        for row in dictionaries:
            positions = portable_positions(bytes.fromhex(row['payload']))
            check('independent bitmap cardinality for key ' + str(row['key']), len(positions), row['cardinality'])
            decoded.extend((row['key'], position) for position in positions)
        order = lambda pair: (pair[0] is None, pair[0] or 0, pair[1])
        expected = [(encoded_key(row['key_text'], args.type), row['row_position']) for row in physical]
        check('every typed encoded key maps to exact physical source positions', sorted(decoded, key=order), sorted(expected, key=order))
        check('bitmap population is full physical source count', len(decoded), population)
        check('coverage never confuses dictionary and physical row counts', len(dictionaries) < population)
        native_order = [row[0] for row in query(conn, 'SELECT id FROM biz.exact ORDER BY k NULLS LAST,id')]
        encoded_order = [position_to_id[position] for key, position in sorted(decoded,
                         key=lambda pair: (pair[0] is None, pair[0] or 0, position_to_id[pair[1]]))]
        check('bitmap encoded key order equals original native SQL total order', encoded_order, native_order)
        both('INSERT INTO {table} VALUES(2001,0::' + sql_type + ",'inline/zero'),(2002,NULL,'inline/null')")
        query(conn, 'SET enable_seqscan=off')
        text = bitmap_actual('mixed-typed-inputs')
        expected_inline = 'native UB-tree' if args.storage == 'ustore' else 'native B-tree'
        check('actual inline IndexAM uses requested native storage', 'Actual Inline Access: ' + expected_inline in text)
        if args.type == 'bigint':
            predicates = ['k=0', 'k<0', 'k<=0', 'k>0', 'k>=0', '0=k', 'k=7::smallint',
                          "k='-9223372036854775808'::bigint", "k='9223372036854775807'::bigint",
                          'k>2147483647::integer', 'k<(-2147483647-1)::integer',
                          "k=ANY(ARRAY['-9223372036854775808'::bigint,9223372036854775807::bigint,0,NULL,0])",
                          'k=ANY(ARRAY[7,0,NULL,7]::integer[])', 'k=ANY(ARRAY[]::bigint[])',
                          'k=ANY(NULL::bigint[])', 'k=ANY(ARRAY[NULL]::bigint[])']
            parameter_type = 'bigint'
            parameters = ['0', '-9223372036854775808', '9223372036854775807', '-2147483649', '2147483648', None, '0']
        else:
            predicates = ['k=0', 'k=-0.0::double precision', 'k<0', 'k<=0', 'k>0', 'k>=0',
                          'k>=1.00000006::double precision', '1.00000006::double precision<k',
                          "k='NaN'::double precision", "k<'NaN'::double precision", "k>'Infinity'::double precision",
                          "k='-Infinity'::double precision", "k=ANY(ARRAY['NaN'::double precision,-0.0::double precision,1.25::double precision,NULL])",
                          'k=ANY(ARRAY[]::real[])', 'k=ANY(NULL::double precision[])', 'k=ANY(ARRAY[NULL]::double precision[])',
                          "k<='1e-30'::double precision"]
            parameter_type = 'double precision'
            parameters = ['0', '-1.25', '1.25', '1.00000006', 'NaN', 'Infinity', '-Infinity', None, '0']
        for predicate in predicates + ['k IS NULL', 'k IS NOT NULL']:
            equal(predicate)
        query(conn, 'SET plan_cache_mode=force_generic_plan')
        projection = 'SELECT id,encode(' + send + "(k),'hex'),note FROM {} WHERE k>=$1 ORDER BY id"
        query(conn, 'PREPARE bitmap_bound(' + parameter_type + ') AS ' + projection.format('biz.keys'))
        for value in parameters:
            check('generic typed bound ' + str(value), query(conn, 'EXECUTE bitmap_bound(%s)', (value,)),
                  query(conn, projection.replace('$1', '%s::' + parameter_type).format('biz.exact'), (value,)))
        query(conn, 'DEALLOCATE bitmap_bound')
        query(conn, 'RESET plan_cache_mode')
        if args.type == 'bigint':
            query(conn, 'SET plan_cache_mode=force_generic_plan')
            query(conn, 'PREPARE narrow_bound(integer) AS SELECT id,note FROM biz.keys WHERE k=$1 ORDER BY id')
            for value in ['0', '32768', '2147483647', '-2147483648', None, '0']:
                check('generic bigint versus int4 argument ' + str(value), query(conn, 'EXECUTE narrow_bound(%s)', (value,)),
                      query(conn, 'SELECT id,note FROM biz.exact WHERE k=%s::integer ORDER BY id', (value,)))
            query(conn, 'DEALLOCATE narrow_bound')
            query(conn, 'RESET plan_cache_mode')
        query(conn, 'BEGIN')
        for table in ['biz.keys', 'biz.exact']:
            query(conn, "UPDATE " + table + " SET k=NULL,note='private' WHERE id=1")
        equal(label='private NULL replacement hides original file version')
        query(conn, 'SAVEPOINT typed_change')
        for table in ['biz.keys', 'biz.exact']:
            query(conn, 'DELETE FROM ' + table + ' WHERE id=1')
        equal(label='savepoint private deletion remains exact')
        query(conn, 'ROLLBACK TO typed_change')
        query(conn, 'RELEASE SAVEPOINT typed_change')
        equal(label='savepoint rollback restores typed private replacement')
        query(conn, 'ROLLBACK')
        equal(label='whole rollback restores original typed file row')
        replacements = ['9223372036854775807', '-9223372036854775808'] if args.type == 'bigint' else ['-0', 'NaN']
        for value in replacements:
            both('UPDATE {table} SET k=' + literal(value) + '::' + sql_type + ",note='updated' WHERE id=1")
            equal(label='two committed updates retain exact replacement ' + value)
        head, protocol = case.head(), case.protocol()
        for field, wrong in [('format_version', 1), ('algorithm_version', 2 if algorithm == 1 else 1)]:
            query(conn, 'BEGIN')
            query(conn, 'UPDATE og_lake_internal.index_segment SET ' + field + '=%s WHERE index_uuid=%s::uuid', (wrong, index_uuid))
            equal(label='obsolete ' + field + ' retains complete visible data')
            status = json.loads(scalar(conn, 'SELECT og_lake.lake_index_status(%s::uuid)', (index_uuid,)))
            check('obsolete ' + field + ' is excluded from READY status', status['ready_segments'], 0)
            text = plan('obsolete-' + field)
            check('obsolete ' + field + ' never authorizes bitmap pruning', 'Verified Bitmap File Coverage: 0' in text)
            check('obsolete ' + field + ' uses full source fallback', 'Actual File Access: complete SDK scan' in text)
            query(conn, 'ROLLBACK')
            bitmap_actual('restored-' + field)
        check('format guards never allocate business snapshots or row IDs', case.head(), head)
        check('format guards leave complete standard protocol unchanged', case.protocol(), protocol)
        equal(label='final full native heap differential')
        check('visible row population unchanged by index routes', scalar(conn, 'SELECT count(*) FROM biz.keys'),
              scalar(conn, 'SELECT count(*) FROM biz.exact'))
        check('executed inputs remain frozen', all(sha256(Path(path)) == digest for path, digest in result['inputs'].items()))
        check('SDK provider remains exact', sha256(provider), result['provider_sha256'])
        check('independent reference remains exact', sha256(duckdb), result['reference_binary_sha256'])
        check('selected official build receipt remains exact', sha256(args.build_receipt), result['build_receipt_sha256'])
        check_build(args.build_receipt)
        result['status'] = 'pass'
    except BaseException as exc:
        result.update(reason=str(exc), traceback=traceback.format_exc())
    finally:
        if db is not None:
            try:
                db.cleanup()
                result['database_cleanup'] = db.cleanup_result
            except BaseException as exc:
                result.update(status='fail', cleanup_error=str(exc))
        if roles is not None:
            try:
                if not result.get('database_cleanup') or result['database_cleanup']['returncode']:
                    raise RuntimeError('Owned DB cleanup unproven; roles retained')
                result['role_cleanup'] = cleanup_roles(control, roles.path, args.run_id)
                result = roles.redact(result)
            except BaseException as exc:
                result.update(status='fail', role_cleanup_error=str(exc))
        if control is not None:
            control.close()
        result['artifacts'] = {str(path): sha256(path) for path in out.rglob('*')
                               if path.is_file() and path.suffix in ['.parquet', '.sql', '.stdout', '.stderr', '.txt']}
        write_json(out / 'result.json', result)
    print(json.dumps(dict(status=result['status'], checks=len(result['assertions']),
                          reason=result.get('reason'), result=str(out / 'result.json')), ensure_ascii=False))
    return 0 if result['status'] == 'pass' else 1


if __name__ == '__main__':
    raise SystemExit(main())
