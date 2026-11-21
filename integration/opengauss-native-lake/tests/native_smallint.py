#!/usr/bin/env python3
"""Prepared SQL0.20 signed SMALLINT qualification; default executes nothing.

Requires independently qualified optional INT16 SDK and a new official Native
build. No provider installation, GUC/service mutation, fixture concurrency,
index/Vec/SMP enablement, temporal support or performance claim is made here.
Root must run each --execute child with an owned-processgroup hard deadline.
"""
import argparse
from copy import deepcopy
import ctypes
import json
import os
import re
from pathlib import Path
import shutil
import subprocess
import sys
import traceback
from types import SimpleNamespace

ROOT = Path(os.getenv('CPP_WORKSPACE', '/home/qiumc/data/code/cpp')).resolve()
D = ROOT / 'integration/opengauss-native-lake'
MODES = ['basic', 'interop', 'upgrade', 'legacy', 'old-sdk']
ROWS = [(1, -32768), (2, -32767), (3, -1), (4, 0), (5, 1), (6, 32767),
        (7, None), (8, -2), (9, 1024), (10, -1024), (11, None), (12, 32766)]


def installation(mode, target):
    if mode == 'legacy':
        if target not in ['0.17', '0.18', '0.19']:
            raise ValueError('legacy requires explicitly retained SQL17/18/19')
        return target
    if target not in ['0.20','0.21']:
        raise ValueError('current SMALLINT cases require explicit SQL20/21')
    return '0.19' if mode == 'upgrade' else target


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--execute', action='store_true')
    parser.add_argument('--run-id')
    parser.add_argument('--build-receipt', type=Path)
    parser.add_argument('--sdk-index', type=Path, help='Exact qualified signed16 provider index; not used for old-sdk')
    parser.add_argument('--sdk-index-sha256', help='Exact SHA256 selected with --sdk-index')
    parser.add_argument('--mode', required=True, choices=MODES)
    parser.add_argument('--sql-version', choices=['0.17', '0.18', '0.19', '0.20', '0.21'])
    args = parser.parse_args()
    target = args.sql_version or ('0.19' if args.mode == 'legacy' else '0.20')
    try:
        initial = installation(args.mode, target)
    except ValueError as exc:
        parser.error(str(exc))
    if not args.execute:
        print(json.dumps(dict(status='prepared_not_executed', mode=args.mode, initial_sql=initial,
            target_sql=target, native_executed=0, golden_rows=ROWS,
            sdk_admission='genuine5ae exact provider' if args.mode == 'old-sdk' else 'strict signed16 SDK index/hash and installed library SHA equality',
            prerequisite='Exact reviewed source module20/21 and qualified SDK; old-sdk instead requires the genuine old provider; root external hard deadline1200s.',
            limitations=__doc__), ensure_ascii=False, indent=2))
        return 0
    if not args.run_id or args.build_receipt is None:
        parser.error('execute requires run-id/build-receipt')
    if args.mode == 'old-sdk':
        if args.sdk_index is not None or args.sdk_index_sha256 is not None:
            parser.error('old-sdk uses the exact genuine5ae authority, not a new provider index')
    elif args.sdk_index is None or not re.fullmatch(r'[0-9a-f]{64}', args.sdk_index_sha256 or ''):
        parser.error('execute requires exact --sdk-index/--sdk-index-sha256 for the qualified signed16 SDK')
    sys.path[:0] = [str(D), str(D / 'tests')]
    from file_dml_acceptance import check_build
    from inline_fixture import query, scalar, replace_catalog_values
    from lake_harness import sha256, validate_run_id, write_json
    from native_auto_flush import connect_control
    from native_runtime import PROFILE, SOURCE, INSTALL
    from native_test_context import OwnedDatabase, reviewed_runtime_version
    from native_table_create import Case
    from permission_api import OwnedRoles, cleanup_roles
    from empty_catalog_oracle import canonical_shape, transport_native
    from empty_reference_fixture import export_actual_tables
    from reverse_reference import Reference, literal

    validate_run_id(args.run_id)
    if PROFILE != 'debug':
        parser.error('Only the independently owned main Debug runtime is admitted')
    out = ROOT / '公共文档/openGauss原生湖存储/验证' / args.run_id
    out.mkdir(exist_ok=False)
    result = dict(status='fail', mode=args.mode, installed_target=target, assertions=[], executed=False)
    control = db = roles = case = None

    def check(label, got, want=True):
        ok = got == want
        result['assertions'].append(deepcopy(dict(name=label, observed=got, expected=want,
                                                status='pass' if ok else 'fail')))
        write_json(out / 'result.json', result)
        if not ok:
            raise AssertionError(label)

    def reject(sql, state, params=None):
        try:
            query(case.a, sql, params)
        except BaseException as exc:
            check('actual rejection SQLSTATE/' + sql[:80], getattr(exc, 'pgcode', None), state)
            query(case.a, 'ROLLBACK')
        else:
            raise AssertionError('Unexpectedly accepted ' + sql)

    def session(conn):
        for sql in ['SET session_timeout=0', 'SET statement_timeout=180000', 'SET query_dop=1',
                    'SET enable_vector_engine=off', 'SET enable_lake_smp=off', 'SET enable_default_ustore_table=off']:
            query(conn, sql)

    def rows():
        return query(case.a, 'SELECT id,val FROM biz.orders ORDER BY id')

    def both(statement):
        query(case.b, 'BEGIN')
        try:
            for table in ['biz.orders', 'biz.exact']:
                query(case.b, statement.format(table=table))
            query(case.b, 'COMMIT')
        except BaseException:
            query(case.b, 'ROLLBACK')
            raise

    def parity(label):
        check(label + ' exact heap values and NULLs', rows(), query(case.observer, 'SELECT id,val FROM biz.exact ORDER BY id'))

    def parameters(label):
        query(case.a, 'SET plan_cache_mode=force_generic_plan')
        query(case.a, 'PREPARE narrow_parameter(smallint) AS SELECT id,val FROM biz.orders WHERE val=$1 ORDER BY id')
        try:
            for value in [-32768, -1, 0, 32767, None, -32768]:
                check(label + ' native generic parameter/' + str(value),
                      query(case.a, 'EXECUTE narrow_parameter(%s)', (value,)),
                      query(case.observer, 'SELECT id,val FROM biz.exact WHERE val=(%s)::smallint ORDER BY id', (value,)))
        finally:
            query(case.a, 'DEALLOCATE narrow_parameter')
            query(case.a, 'SET plan_cache_mode=auto')

    def flush():
        query(case.a, "SELECT og_lake.flush('biz.orders'::regclass)")

    def interread(label):
        exported = case.export(label + '-native.json')
        catalog = out / (label + '-transport.ducklake')
        transport_native(reference, catalog, exported)
        found = reference.run(reference.attach(catalog, True) + 'SELECT id,val FROM lake.main.orders ORDER BY id', label, result=True)
        check(label + ' standard DuckLake reads true SMALLINT values', [(x['id'], x['val']) for x in found], rows())
        if 'initial_business_snapshot' in result:
            found = reference.run(reference.attach(catalog, True) +
                'SELECT id,val FROM lake.main.orders AT(VERSION => ' + str(result['initial_business_snapshot']) + ') ORDER BY id',
                label + '-history', result=True)
            check(label + ' standard history keeps initial narrow values and identities', [(x['id'], x['val']) for x in found], ROWS)

    try:
        build = check_build(args.build_receipt)
        result.update(build=str(args.build_receipt.resolve()), build_sha256=sha256(args.build_receipt),
            source_head=build['source_head'], binary_sha256=build['binary_sha256'])
        sdk_qualification = None
        if args.mode != 'old-sdk':
            from pq_sql020_build_inputs import sdk_index
            sdk_qualification = sdk_index(args.sdk_index, args.sdk_index_sha256, ROOT)
            result['sdk_qualification'] = deepcopy(sdk_qualification)
            check('Native full SDK public header equals qualified signed16 header', sha256(SOURCE / 'contrib/og_lake/sdk/lake_parquet_sdk.h'), sdk_qualification['header_sha256'])
        else:
            old_authority = ROOT / '公共文档/openGauss原生湖存储/验证/sdk_binary_transport_scoped_verified_index.json'
            old_provider = ROOT / '.reference/opengauss-native-lake/sdk/install/binary-transport/lib/libog_lake_sdk_arrow.so'
            check('genuine5ae old-provider authority', sha256(old_authority), 'fbde68f0b5620d9b4f2f0fe1c4c948616db3570352feac26769403ce5b63fb05')
            check('genuine5ae immutable origin', sha256(old_provider), '5ae9f8fecba4b0565b414e766cfad4dbc62e7f8714326f7610c8a2e51e3c083a')
            result['old_sdk_authority'] = dict(path=str(old_authority), sha256=sha256(old_authority), provider=str(old_provider), provider_sha256=sha256(old_provider))
        control = connect_control()
        session(control)
        check('maintenance remains off', scalar(control, 'SHOW enable_lake_maintenance'), 'off')
        check('first slice uses dedicated backends', scalar(control, 'SHOW enable_thread_pool'), 'off')
        check('test source declares the explicit SMALLINT capability',
              'OG_LAKE_SDK_INT16_VALUES' in (SOURCE / 'contrib/og_lake/lake_types.h').read_text())
        files = [Path(__file__), args.build_receipt, D / 'native_test_context.py', D / 'native_runtime.py',
            D / 'tests/native_table_create.py', D / 'tests/permission_api.py', D / 'tests/inline_cleanup.py',
            D / 'inline_fixture.py', D / 'lake_harness.py', D / 'tests/file_dml_acceptance.py',
            D / 'tests/native_auto_flush.py', D / 'tests/catalog_fixture_bundle.py', D / 'tests/empty_reference_fixture.py',
            D / 'tests/empty_manifest.py', D / 'tests/empty_catalog_oracle.py', D / 'tests/reverse_reference.py',
            D / 'tests/create_reference_oracle.py', D / 'fixture.py', D / 'reference.sh', D / 'source-lock.json']
        files += [SOURCE / name for name in build['native_source_inputs_after']]
        files += [Path(name) for name in build['build_wrapper_inputs_after']]
        provider = INSTALL / 'lib/postgresql/og_lake_sdk/libog_lake_sdk_arrow.so'
        files += [INSTALL / 'bin/gaussdb'] + [p for p in provider.parent.rglob('*') if p.is_file()]
        if sdk_qualification is not None:
            check('actual installed SDK is the independently qualified signed16 library', sha256(provider), sdk_qualification['library_sha256'])
            files += [args.sdk_index.resolve(), D / 'pq_sql020_build_inputs.py', D / 'pq_native_build_inputs.py']
        else:
            check('actual installed old-sdk is genuine5ae', sha256(provider), result['old_sdk_authority']['provider_sha256'])
            files += [old_authority, old_provider]
        files += list((INSTALL / 'share/postgresql/extension').glob('og_lake*.sql'))
        lock = json.loads((D / 'source-lock.json').read_text())
        reference_receipt = ROOT / '.reference/opengauss-native-lake/build/receipt.json'
        receipt = json.loads(reference_receipt.read_text())
        reference_dir = out / 'reference'
        reference_dir.mkdir()
        reference = Reference(reference_dir, lock, receipt)
        check('genuine fixed reference matches current lock', receipt['source_lock_hash'], sha256(D / 'source-lock.json'))
        check('genuine fixed reference binary', sha256(reference.binary), receipt['binary_sha256'])
        files += [reference_receipt, reference.binary]
        archive = out / 'executed-inputs'
        archive.mkdir()
        result['inputs'] = {}
        for n, path in enumerate(dict.fromkeys(p.resolve() for p in files)):
            target_path = archive / (str(n) + '-' + path.name)
            shutil.copy2(path, target_path)
            result['inputs'][str(path)] = dict(sha256=sha256(path), archive=str(target_path))
        # Inspect only the stable public C ABI prefix; no C++/OG object crosses
        # into Python, and no SDK file operation is called by this capability probe.
        class ApiPrefix(ctypes.Structure):
            _fields_ = [('abi', ctypes.c_uint32), ('size', ctypes.c_uint32),
                        ('name', ctypes.c_char_p), ('version', ctypes.c_char_p),
                        ('capabilities', ctypes.c_uint64)]
        library = ctypes.CDLL(str(provider))
        library.og_lake_parquet_sdk_v1.restype = ctypes.POINTER(ApiPrefix)
        api = library.og_lake_parquet_sdk_v1().contents
        check('actual C provider ABI', api.abi, 1)
        check('actual prefix struct size is sufficient', api.size >= ctypes.sizeof(ApiPrefix))
        check('actual provider capability matches selected mode', bool(api.capabilities & (1 << 14)), args.mode != 'old-sdk')
        result['sdk_capabilities'] = int(api.capabilities)
        with (out / 'fixture.log').open('w') as log:
            subprocess.run([sys.executable, str(D / 'tests/catalog_fixture_bundle.py'), '--run-id', args.run_id,
                '--output', str(out / 'fixture/manifest.json')], stdout=log, stderr=subprocess.STDOUT, check=True, timeout=300)
        db = OwnedDatabase(args.run_id, out)
        db.create()
        session(db.admin)
        roles = OwnedRoles(control, args.run_id, out)
        case = Case(db, SimpleNamespace(mode='net_dml', sql_version=initial, manifest=out / 'fixture/manifest.json',
                    output=out / 'result.json', run_id=args.run_id), result, roles)
        for conn in [case.a, case.b, case.observer]:
            session(conn)
        runtime_default=reviewed_runtime_version()
        check('exact official SMALLINT source runtime',json.loads(scalar(case.a,'SELECT og_lake.host_status()'))['module_version'],runtime_default)
        check('source runtime supports SMALLINT target',runtime_default in ['0.20','0.21'])
        result['runtime_default_version']=runtime_default
        if initial != '0.21':
            check('retained SQL never installs public rewrite entry', scalar(case.a,
                "SELECT NOT EXISTS(SELECT 1 FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_namespace n ON n.oid=p.pronamespace "
                "WHERE n.nspname='og_lake' AND p.proname='rewrite_data_files')"))
            if runtime_default == '0.21':
                # A private host probe reaches the actual installed-version
                # guard with nonNULL inputs before any SDK/source scan. The
                # public API remains absent from retainedSQL19/20.
                query(case.a, "CREATE FUNCTION biz.rewrite_version_guard(regclass,bigint[]) RETURNS void "
                    "AS '$libdir/og_lake','og_lake_rewrite_data_files' LANGUAGE C VOLATILE NOT FENCED")
                protocol = case.protocol()
                reject("SELECT biz.rewrite_version_guard('biz.heap_marker'::regclass,ARRAY[0]::bigint[])", '0A000')
                check('retained rewrite rejection has no standard catalog side effects', case.protocol(), protocol)
        result['executed'] = True
        create = "CREATE FOREIGN TABLE biz.orders(id integer,val smallint) SERVER created_lake OPTIONS(binding_mode 'create',lake_schema 'main')"
        if args.mode in ['legacy', 'upgrade']:
            before = case.protocol()
            reject(create, '0A000')
            check('retained SQL has no catalog/identity side effects', case.protocol(), before)
            if args.mode == 'legacy':
                check('legacy initial SQL remains selected', scalar(case.a, "SELECT extversion FROM pg_extension WHERE extname='og_lake'"), initial)
            else:
                query(case.a, "ALTER EXTENSION og_lake UPDATE TO '"+target+"'")
                check('upgrade reaches explicit selected target',scalar(case.a,"SELECT extversion FROM pg_extension WHERE extname='og_lake'"),target)
                check('capability-only upgrade changes no standard catalog row', case.protocol(), before)
        if args.mode != 'legacy':
            if args.mode == 'interop':
                empty = case.export('empty-native.json')
                catalog = reference_dir / 'producer.ducklake'
                transport_native(reference, catalog, empty)
                attach = 'ATTACH ' + literal('ducklake:' + str(catalog)) + " AS lake (DUCKLAKE_VERSION '1.1',AUTOMATIC_MIGRATION FALSE,DATA_INLINING_ROW_LIMIT 0);\n"
                values = ','.join('(%d,%s)' % (key, 'NULL' if val is None else str(val)) for key, val in ROWS)
                reference.run(attach + 'CREATE TABLE lake.main.orders(id INTEGER,val SMALLINT); INSERT INTO lake.main.orders VALUES' + values, 'real-file-only-smallint')
                actual = export_actual_tables(reference, catalog)
                old = {x['table_name']: x for x in empty['tables']}
                check('actual external catalog table set', sorted(x['table_name'] for x in actual), sorted(old))
                adapted = []
                for table in actual:
                    check('real external table shape/' + table['table_name'], canonical_shape(table), canonical_shape(old[table['table_name']]))
                    adapted.append(dict(table, columns=deepcopy(old[table['table_name']]['columns']), constraints=deepcopy(old[table['table_name']]['constraints'])))
                check('DDL adaptation changes no real value or ID', [(x['table_name'], x['rows']) for x in adapted], [(x['table_name'], x['rows']) for x in actual])
                table_id = next(row['table_id'] for table in actual if table['table_name'] == 'ducklake_table' for row in table['rows'] if row['table_name'] == 'orders')
                query(case.a, 'BEGIN')
                replace_catalog_values(case.a, case.schema, empty, dict(tables=adapted))
                query(case.a, 'COMMIT')
                query(case.a, "CREATE FOREIGN TABLE biz.orders(id integer,val smallint) SERVER created_lake OPTIONS(binding_mode 'attach',table_id %s)" % literal(str(table_id)))
                check('genuine DuckLake SMALLINT Parquet reads through Native', rows(), ROWS)
            else:
                query(case.a, create)
            query(case.a, 'CREATE TABLE biz.exact(id integer,val smallint)')
            values = ','.join('(%d,%s)' % (key, 'NULL' if val is None else str(val)) for key, val in ROWS)
            query(case.a, 'INSERT INTO biz.exact VALUES' + values)
            query(case.a, "SELECT og_lake.set_option('biz.orders'::regclass,'data_inlining_row_limit','1000')")
            if args.mode != 'interop':
                query(case.a, 'INSERT INTO biz.orders VALUES' + values)
            parity('initial exact signed16 domain and NULL values')
            parameters('initial signed16 values')
            result['initial_business_snapshot'] = case.head()[0]
            initial_allocator = query(case.observer, 'SELECT table_id,next_row_id FROM created_metadata.ducklake_table_stats ORDER BY table_id')
            check('logical catalog persists int16, never int32', scalar(case.a, "SELECT column_type FROM created_metadata.ducklake_column WHERE column_name='val' AND parent_column IS NULL AND end_snapshot IS NULL"), 'int16')
            if args.mode == 'old-sdk':
                before, head = case.protocol(), case.head()
                reject("SELECT og_lake.flush('biz.orders'::regclass)", '0A000')
                check('old SDK refusal preserves all protocol data', case.protocol(), before)
                check('old SDK refusal allocates no snapshot/IDs', case.head(), head)
                check('old SDK produces no candidate Parquet file', [str(p) for p in Path(case.empty_manifest['data_path']).rglob('*.parquet')], [])
                parity('old SDK refusal preserves inline values')
                query(case.a, "SELECT og_lake.set_option('biz.orders'::regclass,'data_inlining_row_limit','0')")
                before, head = case.protocol(), case.head()
                query(case.a, 'BEGIN')
                query(case.a, 'INSERT INTO biz.heap_marker VALUES(55)')
                query(case.a, 'INSERT INTO biz.orders VALUES(201,-5)')
                reject('COMMIT', '0A000')
                check('old SDK direct-file refusal rolls ordinary heap back', query(case.observer, 'SELECT * FROM biz.heap_marker'), [])
                check('old SDK direct-file refusal leaves protocol/frontiers unchanged', (case.protocol(), case.head()), (before, head))
                check('old SDK direct-file refusal produces no Parquet', [str(p) for p in Path(case.empty_manifest['data_path']).rglob('*.parquet')], [])
                interread('old-sdk-inline-only')
            else:
                for value in [32768, -32769]:
                    reject('INSERT INTO biz.orders VALUES(99,%s)', '22003', (value,))
                    parity('range refusal is atomic/' + str(value))
                if args.mode != 'interop':
                    stats = query(case.a,
                        'SELECT s.min_value,s.max_value,s.contains_null '
                        'FROM created_metadata.ducklake_table_column_stats s '
                        'JOIN created_metadata.ducklake_column c ON c.table_id=s.table_id AND c.column_id=s.column_id '
                        'JOIN created_metadata.ducklake_table t ON t.table_id=c.table_id '
                        'JOIN created_metadata.ducklake_schema n ON n.schema_id=t.schema_id '
                        "WHERE n.schema_name='main' AND n.end_snapshot IS NULL "
                        "AND t.table_name='orders' AND t.end_snapshot IS NULL "
                        "AND c.column_name='val' AND c.parent_column IS NULL AND c.end_snapshot IS NULL")
                    check('signed16 min/max and NULL standard stats', stats, [('-32768', '32767', True)])
                query(case.a, 'ANALYZE biz.orders')
                check('native ANALYZE sees complete visible row count', int(scalar(case.a, "SELECT reltuples FROM pg_class WHERE oid='biz.orders'::regclass")), 12)
                query(case.a, 'BEGIN')
                query(case.a, 'SAVEPOINT narrow')
                query(case.a, 'UPDATE biz.orders SET val=7 WHERE id=1')
                query(case.a, 'ROLLBACK TO narrow')
                query(case.a, 'RELEASE narrow')
                query(case.a, 'COMMIT')
                parity('savepoint rollback preserves narrow Datum')
                both('UPDATE {table} SET val=val+1 WHERE id=1')
                both('UPDATE {table} SET val=val+1 WHERE id=1')
                both('DELETE FROM {table} WHERE id=2')
                check('two UPDATEs and DELETE do not allocate new stable row IDs',
                      query(case.observer, 'SELECT table_id,next_row_id FROM created_metadata.ducklake_table_stats ORDER BY table_id'), initial_allocator)
                flush()
                parity('first historical flush preserves signed16 versions')
                both('UPDATE {table} SET val=-17 WHERE id=3')
                flush()
                for key in [101, 102, 103]:
                    both('INSERT INTO {table} VALUES(' + str(key) + ',-123)')
                    flush()
                check('enough real sources to make SMP rejection meaningful', scalar(case.a, 'SELECT count(*) FROM created_metadata.ducklake_data_file WHERE end_snapshot IS NULL') >= 4)
                parity('second file-origin update and subsequent flush')
                query(case.a, "SELECT og_lake.set_option('biz.orders'::regclass,'data_inlining_row_limit','0')")
                file_count = scalar(case.a, 'SELECT count(*) FROM created_metadata.ducklake_data_file')
                both('INSERT INTO {table} VALUES(201,-32768),(202,32767)')
                check('pure-new SMALLINT INSERT writes one actual direct file', scalar(case.a, 'SELECT count(*) FROM created_metadata.ducklake_data_file'), file_count + 1)
                parity('actual direct-file signed16 boundary payload')
                field_ids = query(case.observer, 'SELECT column_id,column_type FROM created_metadata.ducklake_column WHERE end_snapshot IS NULL ORDER BY column_id')
                rename_allocator = query(case.observer, 'SELECT table_id,next_row_id FROM created_metadata.ducklake_table_stats ORDER BY table_id')
                query(case.a, 'ALTER FOREIGN TABLE biz.orders RENAME COLUMN val TO amount')
                check('renamed narrow field reads original physical carrier/file IDs', query(case.a, 'SELECT id,amount FROM biz.orders ORDER BY id'), query(case.observer, 'SELECT id,val FROM biz.exact ORDER BY id'))
                query(case.a, 'ALTER FOREIGN TABLE biz.orders RENAME COLUMN amount TO val')
                check('rename preserves SMALLINT field IDs/logical type', query(case.observer, 'SELECT column_id,column_type FROM created_metadata.ducklake_column WHERE end_snapshot IS NULL ORDER BY column_id'), field_ids)
                check('rename allocates no new stable row IDs', query(case.observer, 'SELECT table_id,next_row_id FROM created_metadata.ducklake_table_stats ORDER BY table_id'), rename_allocator)
                parameters('file and direct-file signed16 values')
                interread('smallint-current')
                for method in ['btree', 'bitmap']:
                    reject("SELECT og_lake.create_lake_index('biz.orders'::regclass,'narrow_%s',%%s::jsonb)" % method,
                           '0A000', (json.dumps(dict(version=1, kind='scalar', method=method, column='val')),))
                query(case.a, 'SET enable_vector_engine=on')
                query(case.a, "SET try_vector_engine_strategy='force'")
                plan = '\n'.join(x[0] for x in query(case.a, 'EXPLAIN ANALYZE SELECT id,val FROM biz.orders'))
                (out / 'smallint-vec-refusal-plan.txt').write_text(plan + '\n')
                check('unqualified SMALLINT Vec stays ordinary row ForeignScan', 'Lake Batch Delivery: row ForeignScan' in plan)
                query(case.a, 'SET enable_vector_engine=off')
                query(case.a, 'SET enable_lake_smp=on')
                query(case.a, 'SET query_dop=1004')
                plan = '\n'.join(x[0] for x in query(case.a, 'EXPLAIN ANALYZE SELECT id,val FROM biz.orders'))
                (out / 'smallint-smp-refusal-plan.txt').write_text(plan + '\n')
                # LakeStreamExplain emits Planned DOP only for dop>1. The
                # serial branch has its own explicit marker; require it and
                # reject both a parallel lake marker and a real Stream node.
                check('unqualified SMALLINT Stream keeps explicit serial plan',
                      'Lake SMP: serial path;' in plan and 'Lake SMP Planned DOP:' not in plan
                      and 'Stream' not in plan)
                session(case.a)
                parity('after explicitly refused combinations')
        check('frozen actual input/archive bytes', all(sha256(path) == item['sha256'] == sha256(item['archive']) for path, item in result['inputs'].items()))
        check_build(args.build_receipt)
        if sdk_qualification is not None:
            check('qualified signed16 SDK identity remains exact', sdk_index(args.sdk_index, args.sdk_index_sha256, ROOT), sdk_qualification)
        else:
            check('genuine5ae authority remains exact', sha256(old_authority), result['old_sdk_authority']['sha256'])
            check('genuine5ae immutable origin remains exact', sha256(old_provider), result['old_sdk_authority']['provider_sha256'])
        result['status'] = 'pass'
    except BaseException as exc:
        result.update(reason=str(exc), sqlstate=getattr(exc, 'pgcode', None), traceback=traceback.format_exc())
    finally:
        if db is not None:
            try:
                db.cleanup()
                result['database_cleanup'] = db.cleanup_result
            except BaseException as exc:
                result.update(status='fail', database_cleanup_error=str(exc))
        if roles is not None:
            try:
                if not result.get('database_cleanup') or result['database_cleanup']['returncode']:
                    raise RuntimeError('Owned database cleanup unproven; preserve roles')
                result['role_cleanup'] = cleanup_roles(control, roles.path, args.run_id)
                result = roles.redact(result)
            except BaseException as exc:
                result.update(status='fail', role_cleanup_error=str(exc))
        if control is not None:
            control.close()
        write_json(out / 'result.json', result)
    return 0 if result['status'] == 'pass' else 1


if __name__ == '__main__':
    raise SystemExit(main())
