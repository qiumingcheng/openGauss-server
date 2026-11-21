#!/usr/bin/env python3
"""Unexecuted SQL0.18 COPY STDIN journal bridge acceptance.

Requires a new exact build receipt, the installed SDK and an exclusively owned
real DuckLake fixture. Uses the host COPY protocol, never SQL INSERT expansion.
Basic covers text/CSV parsing, mapped/omitted columns, caller transaction,
savepoint recovery, line diagnostics, ordinary-heap parity and column ACLs.
Limits requires explicit rejection of the current inline/64MiB journal bounds.
Legacy checks that newer runtime does not open retained SQL's COPY contract.
No case qualifies direct bulk-file COPY, remote object storage, SMP or RAC.
"""
import argparse
import io
from copy import deepcopy
import json
from pathlib import Path
import shutil
import subprocess
import sys
import traceback

ROOT = Path(__file__).resolve().parents[3]
D = ROOT / 'integration/opengauss-native-lake'
sys.path[:0] = [str(D), str(D / 'tests')]
from empty_catalog_oracle import compare_initialized
from empty_manifest import validate_empty_bundle, validate_empty_manifest
from create_reference_oracle import export_created_catalog
from file_dml_acceptance import check_build
from inline_fixture import query, scalar
from lake_harness import sha256, validate_run_id, write_json
from native_auto_flush import connect_control
from native_runtime import INSTALL, PROFILE
from native_test_context import OwnedDatabase
from permission_api import OwnedRoles, cleanup_roles


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--build-receipt', required=True, type=Path)
    parser.add_argument('--mode', required=True, choices=['basic', 'limits', 'legacy'])
    parser.add_argument('--legacy-version', choices=['0.7', '0.16', '0.17'], default='0.17')
    args = parser.parse_args()
    validate_run_id(args.run_id)
    if PROFILE != 'debug':
        parser.error('Requires the separately owned main Debug runtime')
    version = args.legacy_version if args.mode == 'legacy' else '0.18'
    out = ROOT / '公共文档/openGauss原生湖存储/验证' / args.run_id
    out.mkdir(exist_ok=False)
    result = dict(status='fail', scope=__doc__, mode=args.mode, installed_sql=version, assertions=[], copies=[])
    inputs = [Path(__file__), D / 'tests/file_dml_acceptance.py', D / 'tests/permission_api.py',
              D / 'native_test_context.py', D / 'native_runtime.py', D / 'inline_fixture.py',
              D / 'tests/inline_cleanup.py', D / 'tests/native_auto_flush.py', D / 'tests/catalog_fixture_bundle.py',
              D / 'tests/empty_reference_fixture.py', D / 'tests/empty_manifest.py',
              D / 'tests/empty_catalog_oracle.py', D / 'tests/create_reference_oracle.py',
              D / 'tests/reverse_reference.py', D / 'reference.sh', D / 'fixture.py',
              D / 'lake_harness.py', D / 'source-lock.json']
    result['inputs'] = {str(p): sha256(p) for p in inputs}
    archive = out / 'executed-inputs'
    archive.mkdir()
    for p in inputs:
        shutil.copy2(p, archive / p.name)
    control = db = roles = connection = observer = None
    committed = []
    markers = []
    provider = INSTALL / 'lib/postgresql/og_lake_sdk/libog_lake_sdk_arrow.so'

    def check(name, observed, expected=True):
        passed = observed == expected
        result['assertions'].append(dict(name=name, observed=deepcopy(observed), expected=deepcopy(expected),
                                         status='pass' if passed else 'fail'))
        if not passed:
            raise AssertionError(name + ': ' + repr(observed))

    def copy(conn, table, payload, options='', columns='id,val', label='copy', expected_rows=None):
        statement = 'COPY ' + table + '(' + columns + ') FROM STDIN' + (' WITH (' + options + ')' if options else '')
        record = dict(label=label, statement=statement, payload_bytes=len(payload.encode()),
                      payload_sha256=__import__('hashlib').sha256(payload.encode()).hexdigest())
        result['copies'].append(record)
        try:
            with conn.cursor() as cursor:
                cursor.copy_expert(statement, io.StringIO(payload))
                record.update(state='completed', statusmessage=cursor.statusmessage, rowcount=cursor.rowcount)
                if expected_rows is not None:
                    check(label + ' reports the exact COPY command count', cursor.rowcount, expected_rows)
        except BaseException as exc:
            record.update(state='error', sqlstate=getattr(exc, 'pgcode', None), message=str(exc),
                          context=getattr(getattr(exc, 'diag', None), 'context', None))
            raise
        return record

    def reject_copy(conn, payload, options, expected, label, columns='id,val'):
        try:
            copy(conn, 'biz.orders', payload, options, columns, label)
        except Exception as exc:
            check(label + ' SQLSTATE', getattr(exc, 'pgcode', None), expected)
            return exc
        raise AssertionError(label + ' unexpectedly accepted')

    def rows(conn, table='biz.orders'):
        return query(conn, 'SELECT id,val FROM ' + table + ' ORDER BY id,val NULLS FIRST')

    def state():
        return dict(rows=rows(observer), heap=rows(observer, 'biz.reference'),
                    snapshots=query(observer, 'SELECT snapshot_id FROM created_metadata.ducklake_snapshot ORDER BY 1'),
                    stats=query(observer, 'SELECT table_id,record_count,next_row_id,file_size_bytes '
                                'FROM created_metadata.ducklake_table_stats ORDER BY table_id'))

    try:
        from psycopg2 import sql
        built = check_build(args.build_receipt)
        result.update(source_head=built['source_head'], binary_sha256=built['binary_sha256'],
                      build=str(args.build_receipt.resolve()), build_sha256=sha256(args.build_receipt),
                      provider_sha256=sha256(provider))
        control = connect_control()
        query(control, 'SET session_timeout=0')
        check('automatic maintenance remains disabled', scalar(control, 'SHOW enable_lake_maintenance'), 'off')
        with (out / 'fixture.log').open('w') as log:
            subprocess.run([sys.executable, str(D / 'tests/catalog_fixture_bundle.py'), '--run-id', args.run_id,
                            '--output', str(out / 'fixture/manifest.json')], stdout=log,
                           stderr=subprocess.STDOUT, check=True, timeout=300)
        bundle = validate_empty_bundle(out / 'fixture/manifest.json')
        entry = bundle['entries']['same']
        check('real EMPTY manifest is immutable', sha256(entry['manifest']), entry['sha256'])
        empty, exported_empty = validate_empty_manifest(entry['manifest'])
        check('fixture has no invented business table', empty['expected_business_tables'], [])
        db = OwnedDatabase(args.run_id, out)
        db.create()
        connection, observer = db.connect(), db.connect()
        for conn in [connection, observer]:
            query(conn, 'SET session_timeout=0')
            query(conn, 'SET statement_timeout=30000')
        query(connection, sql.SQL('CREATE EXTENSION og_lake VERSION {}').format(sql.Literal(version)))
        check('exact new runtime capability', json.loads(scalar(connection, 'SELECT og_lake.host_status()'))['module_version'], '0.18')
        check('exact requested installed SQL', scalar(connection,
              "SELECT extversion FROM pg_extension WHERE extname='og_lake'"), version)
        roles = OwnedRoles(control, args.run_id, out)
        owner = roles.create('private', False)
        query(connection, 'SELECT og_lake.configure_internal_owner(%s::name)', (owner,))
        query(connection, 'SELECT og_lake.create_catalog(%s::name,%s::name,%s::text,NULL::text)',
              ('created_lake', 'created_metadata', empty['data_path']))
        initial = export_created_catalog(observer, 'created_metadata', exported_empty, out / 'native-initial.json')
        result['initial_independent_comparison'] = compare_initialized(exported_empty, initial, empty['data_path'],
                                                                      'openGauss og_lake 0.18')
        query(connection, 'CREATE SCHEMA biz')
        query(connection, 'SET enable_default_ustore_table=off')
        query(connection, 'CREATE FOREIGN TABLE biz.orders(id integer,val text) SERVER created_lake '
                          "OPTIONS(binding_mode 'create',lake_schema 'main')")
        query(connection, 'CREATE TABLE biz.reference(id integer,val text)')
        query(connection, 'CREATE TABLE biz.heap_marker(id integer PRIMARY KEY)')
        if args.mode == 'legacy':
            before = state()
            reject_copy(connection, '1\tlegacy\n', '', '0A000', 'retained SQL explicitly keeps COPY disabled')
            check('legacy rejection changes no lake/heap/allocator/snapshot state', state(), before)
        else:
            text = '1\thello\n2\t\\N\n3\tline\\nnext\n4\tback\\\\slash\n'
            first = [(1, 'hello'), (2, None), (3, 'line\nnext'), (4, 'back\\slash')]
            query(connection, 'BEGIN')
            query(connection, 'INSERT INTO biz.heap_marker VALUES(1)')
            for table in ['biz.orders', 'biz.reference']:
                copy(connection, table, text, label='native text input into ' + table, expected_rows=4)
                check('current command result visible on next statement ' + table, rows(connection, table), first)
            check('uncommitted COPY is invisible in another session', rows(observer), [])
            query(connection, 'COMMIT')
            committed = first
            markers = [(1,)]
            check('committed text agrees with ordinary heap', rows(observer), rows(observer, 'biz.reference'))
            empty_before = state()
            for table in ['biz.orders', 'biz.reference']:
                copy(connection, table, '', label='empty input into ' + table, expected_rows=0)
            check('empty COPY creates no snapshot or row allocation', state(), empty_before)
            if args.mode == 'limits':
                query(connection, "SELECT og_lake.set_option('biz.orders'::regclass,'data_inlining_row_limit','10')")
                query(connection, "SELECT og_lake.flush('biz.orders'::regclass)")
                check('real file source exists before bounded file/inline COPY', scalar(observer,
                      'SELECT count(*) FROM created_metadata.ducklake_data_file WHERE end_snapshot IS NULL') > 0)
                before = state()
                payload = ''.join(str(i) + '\tbounded\n' for i in range(100, 111))
                reject_copy(connection, payload, '', '0A000', 'file/inline row limit stays explicit')
                check('row-limit failure allocates/publishes nothing', state(), before)
                # Isolate journal memory from the preceding 10-row refusal.
                # All68 input rows fit this independently configured row cap.
                query(connection, "SELECT og_lake.set_option('biz.orders'::regclass,'data_inlining_row_limit','1000')")
                before = state()
                query(connection, 'BEGIN')
                query(connection, 'INSERT INTO biz.heap_marker VALUES(2)')
                query(connection, 'SAVEPOINT large_copy')
                # Bounded client allocation; Native must reject the current
                # 64MiB journal rather than split/commit behind the caller.
                payload = ''.join(str(i) + '\t' + 'x' * (1024 * 1024) + '\n' for i in range(200, 268))
                reject_copy(connection, payload, '', '54000', '64MiB journal memory bound stays explicit')
                query(connection, 'ROLLBACK TO large_copy')
                query(connection, 'RELEASE SAVEPOINT large_copy')
                check('memory failure removes every partial journal row', rows(connection), committed)
                query(connection, 'ROLLBACK')
                check('memory failure and caller rollback preserve complete state', state(), before)
            else:
                csv = 'val,id\n"逗号,引号""",5\n"multi\nline",6\nNULL,7\n"",8\n'
                for table in ['biz.orders', 'biz.reference']:
                    copy(connection, table, csv, "FORMAT csv,HEADER true,NULL 'NULL'", 'val,id', 'mapped CSV into ' + table, expected_rows=4)
                committed += [(5, '逗号,引号"'), (6, 'multi\nline'), (7, None), (8, '')]
                for table in ['biz.orders', 'biz.reference']:
                    copy(connection, table, '9\n', columns='id', label='omitted nullable/default column into ' + table, expected_rows=1)
                committed += [(9, None)]
                check('text/CSV/omitted columns agree with full expected values', rows(observer), committed)
                before = state()
                query(connection, 'BEGIN')
                query(connection, 'INSERT INTO biz.heap_marker VALUES(101)')
                for table in ['biz.orders', 'biz.reference']:
                    query(connection, 'SAVEPOINT bad_copy')
                    query(connection, 'INSERT INTO biz.heap_marker VALUES(102)')
                    try:
                        copy(connection, table, '10\tpartial\ninvalid_int\tbad\n', label='bad second line into ' + table)
                    except Exception as exc:
                        check('invalid scalar keeps host input SQLSTATE ' + table, getattr(exc, 'pgcode', None), '22P02')
                        context = getattr(getattr(exc, 'diag', None), 'context', '') or ''
                        check('host reports exact failing COPY line2 ' + table, 'line 2' in context)
                    else:
                        raise AssertionError('invalid COPY input unexpectedly accepted')
                    query(connection, 'ROLLBACK TO bad_copy')
                    query(connection, 'RELEASE SAVEPOINT bad_copy')
                    check('savepoint removes partial COPY rows ' + table, rows(connection, table), committed)
                    check('savepoint rolls back ordinary child heap work', query(connection,
                          'SELECT id FROM biz.heap_marker ORDER BY id'), [(1,), (101,)])
                for table in ['biz.orders', 'biz.reference']:
                    copy(connection, table, '11\trecovered\n', label='COPY after error recovery into ' + table, expected_rows=1)
                check('failed child COPY cannot publish to observer', state(), before)
                query(connection, 'COMMIT')
                committed += [(11, 'recovered')]
                markers += [(101,)]
                before = state()
                query(connection, 'BEGIN')
                query(connection, 'INSERT INTO biz.heap_marker VALUES(201)')
                for table in ['biz.orders', 'biz.reference']:
                    copy(connection, table, '12\trollback\n', label='outer rollback into ' + table, expected_rows=1)
                query(connection, 'ROLLBACK')
                check('outer rollback is atomic with heap writes', state(), before)
                for option in ['FORMAT binary', 'FORMAT fixed', 'FREEZE true', 'ignore_extra_data true',
                               'log_errors true', 'reject_limit 1', 'loader true']:
                    reject_copy(connection, '', option, '0A000', 'unsupported ' + option)
                    check('unsupported mode has no committed side effects ' + option, state(), before)
                writer = roles.create('writer')
                query(connection, sql.SQL('GRANT USAGE ON SCHEMA biz TO {}').format(sql.Identifier(writer)))
                for table in ['biz.orders', 'biz.reference']:
                    query(connection, sql.SQL('GRANT INSERT(id) ON {} TO {}').format(sql.Identifier(*table.split('.')), sql.Identifier(writer)))
                login, identity = roles.connect(db, 'writer')
                result['ordinary_writer'] = identity
                query(login, 'SET session_timeout=0')
                for table in ['biz.orders', 'biz.reference']:
                    copy(login, table, '13\n', columns='id', label='column-INSERT-only login into ' + table, expected_rows=1)
                committed += [(13, None)]
                reject_copy(login, '14\tforbidden\n', '', '42501', 'COPY cannot widen column INSERT privilege')
                check('denied column leaves no partial row', rows(observer), committed)
        check('complete lake values match explicit committed model', rows(observer), committed)
        check('complete ordinary heap values match', rows(observer, 'biz.reference'), committed)
        check('ordinary heap markers track only confirmed commits', query(observer, 'SELECT id FROM biz.heap_marker ORDER BY id'), markers)
        if args.mode != 'legacy':
            check('COPY allocates exactly committed permanent identities', scalar(observer,
                  'SELECT next_row_id FROM created_metadata.ducklake_table_stats'), len(committed))
        check('input source files stayed frozen', all(sha256(Path(p)) == h for p, h in result['inputs'].items()))
        check('SDK provider stayed frozen', sha256(provider), result['provider_sha256'])
        check_build(args.build_receipt)
        result['status'] = 'pass'
    except BaseException as exc:
        result.update(reason=str(exc), traceback=traceback.format_exc())
    finally:
        if connection is not None and not connection.closed:
            try:
                query(connection, 'ROLLBACK')
            except BaseException as exc:
                result.update(status='fail', rollback_error=str(exc))
        if db is not None:
            try:
                db.cleanup()
                result['database_cleanup'] = db.cleanup_result
            except BaseException as exc:
                result.update(status='fail', cleanup_error=str(exc))
        if roles is not None:
            try:
                if not result.get('database_cleanup') or result['database_cleanup']['returncode']:
                    raise RuntimeError('Owned DB cleanup unproven; exact roles retained')
                result['role_cleanup'] = cleanup_roles(control, roles.path, args.run_id)
                result = roles.redact(result)
            except BaseException as exc:
                result.update(status='fail', role_cleanup_error=str(exc))
        if control is not None:
            control.close()
        write_json(out / 'result.json', result)
    print(json.dumps(dict(status=result['status'], mode=args.mode, installed_sql=version,
                          assertions=len(result['assertions']), reason=result.get('reason'), result=str(out / 'result.json')),
                     ensure_ascii=False))
    return 0 if result['status'] == 'pass' else 1


if __name__ == '__main__':
    raise SystemExit(main())
