#!/usr/bin/env python3
"""Unexecuted explicitly selected SQL0.18/0.19 DOP1 native VecForeignScan acceptance.

This is scalar VectorBatch delivery, not vector(n), ANN, SMP, zero-copy or
performance qualification. Requires a new exact official build containing this
candidate. Serialize fixture generation. Source/SDK/build identities and owned
DB/role cleanup use the existing Native harness without changing its helpers.
"""
from copy import deepcopy
import argparse
import hashlib
import json
import math
import os
import re
from pathlib import Path
import shutil
import subprocess
import sys
import threading
import time
import traceback
from types import SimpleNamespace

ROOT = Path(os.getenv('CPP_WORKSPACE', '/home/qiumc/data/code/cpp')).resolve()
D = ROOT / 'integration/opengauss-native-lake'
sys.path[:0] = [str(D), str(D / 'tests')]
from file_dml_acceptance import check_build
from inline_fixture import query, scalar
from lake_harness import sha256, validate_run_id, write_json
from native_auto_flush import connect_control
from native_runtime import INSTALL, PROFILE, SOURCE
from native_table_create import Case
from native_test_context import OwnedDatabase
from permission_api import OwnedRoles, cleanup_roles


def walk(value):
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from walk(child)
    elif isinstance(value, list):
        for child in value:
            yield from walk(child)


def normalized(rows):
    """Preserve every scalar and text byte while making NaN/inf JSON-safe."""
    def value(item):
        if isinstance(item, float):
            if math.isnan(item):
                return ('float', 'NaN')
            return ('float', item.hex())
        return item
    return [tuple(value(item) for item in row) for row in rows]


def selected_sql(mode,requested):
    installed=requested or ('0.17'if mode=='legacy'else'0.18')
    if mode=='legacy'and installed!='0.17':raise ValueError('legacy is explicitly the SQL17 capability rejection case; current SQL18/19 use other modes')
    if mode!='legacy'and installed not in ['0.18','0.19','0.20','0.21']:raise ValueError('current Vec modes require explicitly installed18/19/20/21')
    return installed


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--build-receipt', required=True, type=Path)
    parser.add_argument('--mode', required=True, choices=['basic', 'rescan', 'lifecycle', 'fallback', 'legacy'])
    parser.add_argument('--storage', choices=['heap', 'ustore'], default='heap')
    parser.add_argument('--sql-version', choices=['0.17','0.18','0.19','0.20','0.21'],default=None,
                        help='Defaults: legacy uses17; current modes use18. Select19/20/21 explicitly for current modes.')
    args = parser.parse_args()
    validate_run_id(args.run_id)
    try:installed_sql=selected_sql(args.mode,args.sql_version)
    except ValueError as exc:parser.error(str(exc))
    if PROFILE != 'debug':
        parser.error('Requires the separately owned main Debug profile')
    out = ROOT / '公共文档/openGauss原生湖存储/验证' / args.run_id
    out.mkdir(exist_ok=False)
    result = dict(status='fail', scope=__doc__, mode=args.mode, storage=args.storage, installed_sql=installed_sql, requested_sql=args.sql_version,
                  assertions=[], plans=[], executed=False)
    dependencies = ['tests/native_table_create.py', 'tests/file_dml_acceptance.py', 'tests/permission_api.py',
                    'native_test_context.py', 'native_runtime.py', 'tests/inline_cleanup.py',
                    'inline_fixture.py', 'tests/native_auto_flush.py', 'tests/catalog_fixture_bundle.py',
                    'tests/empty_reference_fixture.py', 'tests/empty_manifest.py', 'tests/empty_catalog_oracle.py',
                    'tests/create_reference_oracle.py', 'tests/reverse_reference.py', 'reference.sh',
                    'fixture.py', 'lake_harness.py', 'source-lock.json']
    files = [Path(__file__).resolve(), args.build_receipt.resolve()] + [D / name for name in dependencies]
    result['inputs'] = {str(path): sha256(path) for path in files}
    archive = out / 'executed-inputs'
    archive.mkdir()
    for path in files:
        shutil.copy2(path, archive / path.name)
    provider = INSTALL / 'lib/postgresql/og_lake_sdk/libog_lake_sdk_arrow.so'
    duckdb = ROOT / '.reference/opengauss-native-lake/build/duckdb-gcc10/duckdb'
    control = db = roles = case = None
    active = []

    def image(value):
        if isinstance(value, (list, tuple)) and len(value) > 20:
            return dict(rows=len(value), sha256=hashlib.sha256(repr(value).encode()).hexdigest())
        return value

    def check(name, observed, expected=True):
        passed = observed == expected
        result['assertions'].append(dict(name=name, observed=deepcopy(image(observed)), expected=deepcopy(image(expected)),
                                         status='pass' if passed else 'fail'))
        if not passed:
            raise AssertionError(name)

    def rows(conn, table='biz.orders', clause=''):
        return normalized(query(conn, 'SELECT * FROM ' + table + (' ' + clause if clause else '') + ' ORDER BY id'))

    def parity(label, conn=None, clause=''):
        conn = conn or case.a
        check(label + ' every value/NULL/text byte equals ordinary heap', rows(conn, clause=clause),
              rows(conn, 'biz.reference', clause))

    def explain(statement, label, vector=True, analyze=True, conn=None):
        raw = scalar(conn or case.a, 'EXPLAIN (' + ('ANALYZE,' if analyze else '') + 'FORMAT JSON) ' + statement)
        plan = json.loads(raw) if isinstance(raw, str) else raw
        result['plans'].append(dict(label=label, analyze=analyze, plan=plan))
        nodes = [node for node in walk(plan) if 'Lake Batch Delivery' in node]
        check(label + ' contains a real lake scan node', len(nodes) > 0)
        actual = [node for node in nodes if node['Lake Batch Delivery'] ==
                  'native VecForeignScan / owned scalar VectorBatch']
        check(label + ' direct VecForeignScan admission', bool(actual), vector)
        if vector and analyze:
            check(label + ' actual callback emitted batches', sum(int(node['Lake Vector Batches']) for node in actual) > 0)
            for node in actual:
                check(label + ' native row capacity', int(node['Lake Vector Row Capacity']), 1000)
                check(label + ' bounded payload capacity', int(node['Lake Vector Payload Capacity']), 8 * 1024 * 1024)
                check(label + ' actual peak respects payload bound', int(node['Lake Vector Peak Payload Bytes']) <= 8 * 1024 * 1024)
        return nodes

    def engine(enabled):
        query(case.a, "SET try_vector_engine_strategy='force'" if enabled else
                      "SET try_vector_engine_strategy='off'")

    def flush():
        query(case.a, "SELECT og_lake.flush('biz.orders'::regclass)")
        check('flush produced genuine committed Parquet', scalar(case.observer,
              'SELECT count(*) FROM created_metadata.ducklake_data_file WHERE end_snapshot IS NULL') > 0)

    def sdk_snapshot():
        directory = provider.parent.resolve()
        owner_path = directory / 'owner.json'
        owner = json.loads(owner_path.read_text())
        check('SDK owner has exact installation target', str(Path(owner['target']).resolve()), str(directory))
        check('SDK owner is the real installed provider', owner['project'], 'opengauss-native-lake-local-provider')
        inventory = {str(path.relative_to(directory)): sha256(path) for path in directory.rglob('*')
                     if path.is_file() and path != owner_path}
        check('whole SDK owned file/license inventory is exact', inventory, owner['files'])
        return dict(owner_sha256=sha256(owner_path), owner=owner, files=inventory)

    def physical_oracle(label):
        root = Path(case.empty_manifest['data_path']).resolve()
        sources = query(case.observer, 'SELECT data_file_id,path,record_count FROM created_metadata.ducklake_data_file '
                        'WHERE end_snapshot IS NULL ORDER BY data_file_id')
        evidence = []
        for file_id, name, count in sources:
            matches = [p for p in root.rglob('*.parquet') if p.name == Path(name).name]
            check(label + ' unique owned data object ' + str(file_id), len(matches), 1)
            path = matches[0].resolve()
            check(label + ' data object remains within owned root', path.is_relative_to(root))
            literal = "'" + str(path).replace("'", "''") + "'"
            statement = ('SELECT count(*) AS physical,count(DISTINCT (_ducklake_internal_row_id,_ducklake_internal_snapshot_id)) AS identities,'
                         'min(_ducklake_internal_snapshot_id) AS first_birth FROM read_parquet(' + literal + ')')
            proc = subprocess.run([str(duckdb), '-no-init', '-batch', '-json', '-c', 'SET threads=1;' + statement],
                                  capture_output=True, text=True, timeout=60)
            (out / (label + '-file-' + str(file_id) + '.stdout')).write_text(proc.stdout)
            (out / (label + '-file-' + str(file_id) + '.stderr')).write_text(proc.stderr)
            check(label + ' independent actual Parquet read succeeds', proc.returncode, 0)
            record = json.loads(proc.stdout)[0]
            check(label + ' immutable physical count matches catalog', record['physical'], count)
            check(label + ' real source retains stable row-version identity', record['identities'], count)
            evidence.append(dict(file_id=file_id, path=str(path), sha256=sha256(path), observed=record))
        result.setdefault('independent_sources', []).append(dict(label=label, sources=evidence))

    try:
        built = check_build(args.build_receipt)
        result.update(source_head=built['source_head'], binary_sha256=built['binary_sha256'],
                      build=str(args.build_receipt.resolve()), build_sha256=sha256(args.build_receipt),
                      provider_sha256=sha256(provider), reference_binary_sha256=sha256(duckdb))
        result['sdk_identity_before'] = sdk_snapshot()
        check('build includes exact batch converter source', 'contrib/og_lake/lake_inline_scan.cpp' in built['native_source_inputs_after'])
        control = connect_control()
        query(control, 'SET session_timeout=0')
        check('automatic maintenance is disabled', scalar(control, 'SHOW enable_lake_maintenance'), 'off')
        with (out / 'fixture.log').open('w') as log:
            subprocess.run([sys.executable, str(D / 'tests/catalog_fixture_bundle.py'), '--run-id', args.run_id,
                            '--output', str(out / 'fixture/manifest.json')], stdout=log,
                           stderr=subprocess.STDOUT, check=True, timeout=300)
        db = OwnedDatabase(args.run_id, out)
        db.create()
        roles = OwnedRoles(control, args.run_id, out)
        case = Case(db, SimpleNamespace(mode='net_dml', sql_version=installed_sql, manifest=out / 'fixture/manifest.json',
                    output=out / 'result.json', run_id=args.run_id), result, roles)
        defaults=re.findall(r"^default_version\s*=\s*'([^']+)'",(SOURCE/'contrib/og_lake/og_lake.control').read_text(),re.M)
        check('one exact source runtime default',len(defaults),1)
        module_version=json.loads(scalar(case.a,'SELECT og_lake.host_status()'))['module_version']
        actual_sql=scalar(case.a,"SELECT extversion FROM pg_extension WHERE extname='og_lake'")
        check('actual module matches source default',module_version,defaults[0])
        check('actual Vec installed SQL matches selected mode',actual_sql,installed_sql)
        check('reviewed inherited Vec source runtime',defaults[0] in ['0.18','0.19','0.20','0.21'])
        result.update(runtime_default_version=defaults[0],runtime_module_version=module_version,installed_sql=actual_sql)
        for conn in (case.a, case.b, case.observer):
            query(conn, 'SET session_timeout=0')
            query(conn, 'SET statement_timeout=30000')
            query(conn, 'SET query_dop=1')
            query(conn, 'SET enable_lake_smp=off')
            query(conn, 'SET enable_vector_engine=on')
            query(conn, "SET try_vector_engine_strategy='off'")
        query(case.a, 'SET enable_default_ustore_table=' + ('on' if args.storage == 'ustore' else 'off'))
        definitions = 'id integer,b boolean,s integer,n integer,big bigint,f4 real,f8 double precision,txt text'
        query(case.a, 'CREATE FOREIGN TABLE biz.orders(' + definitions + ') SERVER created_lake '
                      "OPTIONS(binding_mode 'create',lake_schema 'main')")
        query(case.a, 'SET enable_default_ustore_table=off')
        query(case.a, 'CREATE TABLE biz.reference(' + definitions + ')')
        query(case.a, "SELECT og_lake.set_option('biz.orders'::regclass,'data_inlining_row_limit','10000')")
        expression = ("SELECT i,CASE WHEN i%7=0 THEN NULL ELSE i%2=0 END,(i%32767)::integer,"
                      "CASE WHEN i%11=0 THEN NULL ELSE -i END,-9223372036854775807::bigint+i,"
                      "CASE WHEN i%29=0 THEN 'NaN'::real WHEN i%23=0 THEN 'Infinity'::real WHEN i%37=0 THEN '-0'::real ELSE i*.125::real END,"
                      "CASE WHEN i%31=0 THEN '-Infinity'::double precision WHEN i%38=0 THEN '-0'::double precision ELSE i*.0625::double precision END,"
                      "CASE WHEN i%13=0 THEN NULL ELSE repeat(md5(i::text),CASE WHEN i<=24 THEN 16384 ELSE 32 END) END "
                      "FROM generate_series(1,4507) AS x(i)")
        for table in ['biz.orders', 'biz.reference']:
            query(case.a, 'SET enable_default_ustore_table=' +
                  ('on' if table == 'biz.orders' and args.storage == 'ustore' else 'off'))
            query(case.a, 'INSERT INTO ' + table + ' ' + expression)
        query(case.a, 'SET enable_default_ustore_table=off')
        carrier = query(case.observer, "SELECT c.reloptions FROM created_metadata.ducklake_inlined_data_tables t "
                        "JOIN pg_class c ON c.relname=t.table_name "
                        "AND c.relnamespace=(SELECT oid FROM pg_namespace WHERE nspname='created_metadata')")
        check('one real inline carrier before any auxiliary table', len(carrier), 1)
        check('real requested inline TableAM', 'storage_type=ustore' in (carrier[0][0] or []), args.storage == 'ustore')
        reference_options = scalar(case.observer, "SELECT reloptions FROM pg_class WHERE oid='biz.reference'::regclass")
        check('ordinary reference really remains heap', 'storage_type=ustore' in (reference_options or []), False)
        result['executed'] = True
        engine(True)
        parity('all scalar types inline, including TOAST, NaN/inf and NULL')
        full = explain('SELECT * FROM biz.orders', 'inline complete batch scan', args.mode != 'legacy')
        if args.mode != 'legacy':
            check('inline exact callback source cardinality', sum(int(n['Lake Vector Source Rows']) for n in full), 4507)
            check('inline uses multiple bounded batches', sum(int(n['Lake Vector Batches']) for n in full) >= 5)
        if args.mode == 'legacy':
            flush()
            parity('retained SQL0.17 keeps exact row semantics after flush')
            explain('SELECT * FROM biz.orders', 'retained SQL0.17 file path remains row callback', False)
        elif args.mode == 'fallback':
            engine(False)
            explain('SELECT * FROM biz.orders', 'explicit vector-off keeps row callback', False)
            parity('row callback remains exact')
            engine(True)
            explain('SELECT ROW(id,n) FROM biz.orders', 'native unsupported RowExpr falls back whole plan', False)
            # DML's hidden locator/whole-row layout must never enter rd_att batch.
            query(case.a, 'BEGIN')
            explain('UPDATE biz.orders SET n=n+1 WHERE id=1 RETURNING id,n', 'locator DML keeps row callback', False)
            query(case.a, 'ROLLBACK')
            query(case.a, 'CREATE FOREIGN TABLE biz.vector_values(id integer,v vector(3)) SERVER created_lake '
                          "OPTIONS(binding_mode 'create',lake_schema 'main')")
            query(case.a, "INSERT INTO biz.vector_values VALUES(1,'[1,2,3]')")
            explain('SELECT * FROM biz.vector_values', 'vector(n) business type is outside scalar batch scope', False)
        else:
            flush()
            physical_oracle('first-flush')
            parity('actual files through batch path')
            nodes = explain('SELECT * FROM biz.orders', 'actual file batch scan')
            check('actual batch reader opens genuine file inputs', sum(int(n['Local File Inputs']) for n in nodes) > 0)
            # SQL DML is intentionally row based; subsequent SELECT returns
            # visible file+inline+private versions through the batch reader.
            for table in ['biz.orders', 'biz.reference']:
                query(case.a, 'UPDATE ' + table + ' SET n=n+1,txt=txt||\'first\' WHERE id<=17')
                query(case.a, 'UPDATE ' + table + ' SET n=n+1,txt=txt||\'second\' WHERE id<=9')
                query(case.a, 'DELETE FROM ' + table + ' WHERE id IN (20,21,4507)')
                query(case.a, "INSERT INTO " + table + " VALUES(5000,true,1,5000,5000,0,0,'inline-tail')")
            parity('mixed files/inline after two updates and delete')
            mixed = explain('SELECT * FROM biz.orders', 'mixed version batch scan')
            check('mixed reader uses inline plus files', any(int(n['Inline Inputs']) == 1 and int(n['Local File Inputs']) > 0 for n in mixed))
            if args.mode == 'basic':
                parity('native batch qual with many entirely rejected early rows', clause='WHERE id>4400 AND b IS NOT NULL')
                explain('SELECT id,txt FROM biz.orders WHERE id>4400 AND b IS NOT NULL', 'native vector filter')
                query(case.a, 'BEGIN')
                for table in ['biz.orders', 'biz.reference']:
                    query(case.a, 'UPDATE ' + table + ' SET n=n+100 WHERE id=10')
                parity('private own-write overlay shares identical visibility')
                query(case.a, 'SAVEPOINT child')
                query(case.a, 'DELETE FROM biz.orders WHERE id=10')
                query(case.a, 'ROLLBACK TO child')
                parity('savepoint restores private visible row')
                query(case.a, 'ROLLBACK')
                parity('outer rollback restores committed file/inline image')
                flush()
                parity('second flush keeps row identities/delete visibility')
                physical_oracle('second-flush')
                explain('SELECT * FROM biz.orders', 'second flushed batch scan')
            elif args.mode == 'rescan':
                query(case.a, 'CREATE TABLE biz.outer_keys(id integer)')
                query(case.a, 'INSERT INTO biz.outer_keys VALUES(1),(2),(3)')
                for guc in ['enable_hashjoin', 'enable_mergejoin', 'enable_material']:
                    query(case.a, 'SET ' + guc + '=off')
                query(case.a, 'SET join_collapse_limit=1')
                statement = 'SELECT o.id,l.id,l.txt FROM biz.outer_keys o LEFT JOIN biz.orders l ON l.id=o.id ORDER BY o.id'
                check('rescanned full text equals ordinary heap join', normalized(query(case.a, statement)),
                      normalized(query(case.a, statement.replace('biz.orders', 'biz.reference'))))
                rescan = explain(statement, 'actual nested-loop batch rescan')
                check('actual FDW ReScan callback was exercised', sum(int(n['Lake Vector Rescans']) for n in rescan) > 0)
                query(case.a, 'PREPARE batch_generic(integer) AS SELECT id,txt FROM biz.orders WHERE id>$1 ORDER BY id')
                query(case.a, 'SET plan_cache_mode=force_generic_plan')
                for minimum in [4505, 0, 4500, 2, 5000, 4400]:
                    got = normalized(query(case.a, 'EXECUTE batch_generic(%s)', (minimum,)))
                    expected = normalized(query(case.a, 'SELECT id,txt FROM biz.reference WHERE id>%s ORDER BY id', (minimum,)))
                    check('generic batch parameter epoch ' + str(minimum), got, expected)
                    explain('EXECUTE batch_generic(' + str(minimum) + ')', 'generic actual epoch ' + str(minimum), analyze=False)
            elif args.mode == 'lifecycle':
                limit = explain('SELECT * FROM biz.orders LIMIT 1', 'partial consumer stops before source EOF')
                check('LIMIT consumed bounded prefix, not full lake', 1 <= sum(int(n['Lake Vector Source Rows']) for n in limit) <= 1000)
                query(case.a, 'BEGIN')
                query(case.a, 'DECLARE batch_cursor CURSOR FOR SELECT * FROM biz.orders')
                check('first cursor batch can be partly consumed', len(query(case.a, 'FETCH 1 FROM batch_cursor')), 1)
                query(case.a, 'CLOSE batch_cursor')
                query(case.a, 'COMMIT')
                parity('closing partial batch cursor leaves no stale pending row')
                statement = 'SELECT count(*) FROM biz.orders a CROSS JOIN biz.orders b CROSS JOIN biz.orders c'
                explain(statement, 'planned cancellable batch query', analyze=False)
                worker = db.connect()
                query(worker, 'SET session_timeout=0')
                query(worker, 'SET statement_timeout=20000')
                query(worker, 'SET query_dop=1')
                query(worker, 'SET enable_vector_engine=on')
                query(worker, "SET try_vector_engine_strategy='force'")
                app = args.run_id + '_batch_cancel'
                query(worker, 'SET application_name=%s', (app,))
                pid = scalar(worker, 'SELECT pg_backend_pid()')
                done, reply = threading.Event(), {}
                def execute():
                    try:
                        reply.update(state='completed', value=query(worker, statement))
                    except BaseException as exc:
                        reply.update(state='error', sqlstate=getattr(exc, 'pgcode', None), message=str(exc))
                    finally:
                        done.set()
                thread = threading.Thread(target=execute, daemon=True)
                active.append(dict(connection=worker, done=done, pid=pid, application_name=app))
                thread.start()
                deadline = time.monotonic() + 8
                while True:
                    identity = query(case.observer, 'SELECT datid,application_name,state FROM pg_stat_activity WHERE pid=%s', (pid,))
                    locks = query(case.observer, 'SELECT relation,mode,granted FROM pg_locks WHERE pid=%s AND relation=%s',
                                  (pid, scalar(case.observer, "SELECT 'biz.orders'::regclass::oid")))
                    if identity == [(db.allocation['database_oid'], app, 'active')] and any(r[2] for r in locks) and not done.is_set():
                        result['cancellation_proof'] = dict(pid=pid, identity=identity, locks=locks)
                        break
                    if done.is_set() or time.monotonic() >= deadline:
                        raise AssertionError('No live exact-owned batch scan observed before cancel')
                    time.sleep(.02)
                check('cancel exact owned backend', scalar(case.observer, 'SELECT pg_cancel_backend(%s)', (pid,)))
                check('cancel drains within bounded timeout', done.wait(25))
                result['cancellation_reply'] = reply
                check('native cancellation reports 57014', reply.get('sqlstate'), '57014')
                query(worker, 'ROLLBACK')
                parity('cancelled backend can read exact values again', conn=worker)
                explain('SELECT * FROM biz.orders', 'same backend batch scan after cancellation', conn=worker)
                parity('cancelled scan releases per-batch/source state')
                explain('SELECT * FROM biz.orders', 'scan after cancellation')
                # This is the private-journal -> Vec source-row limit. Native
                # flush has its separate 1 MiB Arrow input-batch budget, so a
                # 9 MiB row cannot be a Parquet-source fixture via local flush.
                # This case makes no Parquet-oversize-row coverage claim.
                query(case.a, 'BEGIN')
                query(case.a, "INSERT INTO biz.orders VALUES(6000,true,1,1,1,0,0,repeat('x',9*1024*1024))")
                query(case.a, 'SAVEPOINT oversize_read')
                try:
                    query(case.a, 'SELECT * FROM biz.orders')
                except Exception as exc:
                    check('oversize batch source row has explicit 54000', getattr(exc, 'pgcode', None), '54000')
                    check('error is the Vec source-row payload gate',
                          getattr(getattr(exc, 'diag', None), 'message_primary', None),
                          'native lake batch source row exceeds 8 MiB payload limit')
                else:
                    raise AssertionError('Oversize batch source row silently bypassed its payload bound')
                query(case.a, 'ROLLBACK TO oversize_read')
                engine(False)
                check('row fallback can read the intact oversize value', scalar(case.a,
                      'SELECT octet_length(txt) FROM biz.orders WHERE id=6000'), 9*1024*1024)
                query(case.a, 'ROLLBACK')
                engine(True)
                parity('oversize error cleanup cannot leak private rows or pending batches')
                explain('SELECT * FROM biz.orders', 'scan after batch payload error')
        check('all archived inputs remained frozen', all(sha256(Path(p)) == h for p, h in result['inputs'].items()))
        check('SDK provider remained frozen', sha256(provider), result['provider_sha256'])
        result['sdk_identity_after'] = sdk_snapshot()
        check('whole SDK/license ownership remained frozen', result['sdk_identity_after'], result['sdk_identity_before'])
        check('final installed Vec SQL version',scalar(case.a,"SELECT extversion FROM pg_extension WHERE extname='og_lake'"),installed_sql)
        check_build(args.build_receipt)
        result['status'] = 'pass'
    except BaseException as exc:
        result.update(reason=str(exc), traceback=traceback.format_exc())
    finally:
        for item in active:
            if not item['done'].is_set():
                try:
                    item['connection'].cancel()
                except BaseException as exc:
                    result.setdefault('cancel_errors', []).append(str(exc))
                item['done'].wait(25)
        unfinished = [dict(pid=x['pid'], application_name=x['application_name']) for x in active if not x['done'].is_set()]
        if unfinished:
            result.update(status='fail', preserved_database='busy owned backend; cleanup cannot be proved', unfinished=unfinished)
        else:
            if case is not None:
                for conn in [case.a, case.b, case.observer]:
                    if not conn.closed:
                        try:
                            query(conn, 'ROLLBACK')
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
                except BaseException as exc:
                    result.update(status='fail', role_cleanup_error=str(exc))
        if roles is not None:
            result = roles.redact(result)
        if control is not None:
            control.close()
        write_json(out / 'result.json', result)
    print(json.dumps(dict(status=result['status'], mode=args.mode, assertions=len(result['assertions']),
                          reason=result.get('reason'), result=str(out / 'result.json')), ensure_ascii=False))
    return 0 if result['status'] == 'pass' else 1


if __name__ == '__main__':
    raise SystemExit(main())
