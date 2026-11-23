#!/usr/bin/env python3
"""Prepared real Native cost-ledger correctness slice; default performs no SQL.

Two fresh cases (btree and bitmap) use the existing Case/OwnedDatabase fixture.
Standard EXPLAIN startup/total are read without accepting duplicate JSON keys.
Small unequal files, ended physical versions, missing coverage and actual native
Stream ownership are checked against ordinary heap results. No natural CBO win,
latency, ANN/PQ, RAC or commercial qualification is inferred.
"""
import argparse
import ast
from copy import deepcopy
import json
import math
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys
import traceback
from types import SimpleNamespace

ROOT = Path(os.getenv('CPP_WORKSPACE', '/home/qiumc/data/code/cpp')).resolve()
D = ROOT / 'integration/opengauss-native-lake'


def unique_object(pairs):
    result = {}
    for name, value in pairs:
        if name in result:
            raise ValueError('Duplicate EXPLAIN JSON key: ' + name)
        result[name] = value
    return result


def cost_pair(node):
    startup, total = float(node['Startup Cost']), float(node['Total Cost'])
    if not all(math.isfinite(x) and x >= 0 for x in (startup, total)) or total < startup:
        raise ValueError('Non-finite, negative or reversed actual plan costs')
    return startup, total


def prepare(method,sql_version="0.19"):
    return dict(status='prepared_not_executed', method=method, sql_version=sql_version,
        scope=__doc__, native_cases_executed=0, fixture_file_rows=[8, 32, 12, 20],
        initial_deleted_rows=5, final_inline_rows=3,
        prerequisites=['New official Debug receipt containing cost02 plus the startup-label fix and repaired scalar SMP.',
            'Exact source, binary, SDK, reference and dependency identities; one serialized fixture at a time.',
            'Launcher maintenance off; dedicated backend/thread pool off; bitmap requires qualified private Roaring.',
            'Run btree and bitmap with different run IDs; outer deadline kills only its Python process group.'],
        oracle=['Real standard Startup Cost/Total Cost, duplicate-key rejection and distinct Base Planner Startup Cost.',
            'ANALYZE reports35 visible rows after deleting5 from40 immutable physical rows.',
            'Covered zero-result probes and LIMIT1 keep full candidate preparation in scan startup.',
            'Mixed2 covered/2 fallback files; native DOP2/4 exact tasks and per-lane coverage logs, not SQL concurrency.',
            'Result multiset equals heap, source/index bytes unchanged, exact owned OID/marker cleanup.'],
        limitations=['No calibrated cost or natural index winner assertion.',
            'Startup mapping inequality is checked; hidden per-source estimated work is not reconstructed from JSON.',
            'No cancellation/fault injection here; existing scalar SMP qualification remains required.'])


def supervise(args):
    """Same bounded owned-process pattern as native_scalar_smp; no SQL cleanup."""
    out = ROOT / '公共文档/openGauss原生湖存储/验证' / args.run_id
    if out.exists():
        raise FileExistsError('Refusing an existing evidence directory: ' + str(out))
    env = dict(os.environ, CPP_WORKSPACE=str(ROOT), PYTHONDONTWRITEBYTECODE='1',
               OG_LAKE_COST_CHILD_PARENT=str(os.getpid()))
    argv = [sys.executable, str(Path(__file__).resolve()), *sys.argv[1:], '--_isolated-child']
    child = subprocess.Popen(argv, env=env, start_new_session=True,
                             stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    reason = None
    try:
        output, _ = child.communicate(timeout=args.case_timeout)
    except (subprocess.TimeoutExpired, KeyboardInterrupt) as exc:
        reason = type(exc).__name__ + ': owned Python process deadline/interruption'
        try:
            os.killpg(child.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        try:
            output, _ = child.communicate(timeout=10)
        except subprocess.TimeoutExpired as drain:
            output = drain.output or ''
            if isinstance(output, bytes):
                output = output.decode('utf-8', 'replace')
            if child.stdout is not None:
                child.stdout.close()
    out.mkdir(exist_ok=True)
    (out / 'isolated-child.stdout').write_text(output or '')
    child_result = out / 'result.json'
    try:
        data = json.loads(child_result.read_text()) if child_result.is_file() else dict(status='fail', assertions=[])
    except (ValueError, OSError):
        data = dict(status='fail', assertions=[], reason='Child result is incomplete/unreadable')
    if child_result.is_file():
        shutil.copy2(child_result, out / 'isolated-child-result.json')
    data['supervision'] = dict(argv=argv, pid=child.pid, returncode=child.returncode,
        deadline_seconds=args.case_timeout, owned_process_group_killed=reason is not None,
        controller_sql_cleanup_attempted=False)
    if reason or child.returncode != 0:
        data['status'] = 'fail'
    if reason:
        data.update(reason=reason, preserve_owned_scene=True)
    child_result.write_text(json.dumps(data, ensure_ascii=False, indent=2, allow_nan=False) + '\n')
    print(json.dumps(dict(status=data['status'], result=str(child_result), reason=data.get('reason'))))
    return 0 if data['status'] == 'pass' else 1


def execute(args):
    sys.path[:0] = [str(D), str(D / 'tests')]
    from psycopg2.extras import register_default_json
    from file_dml_acceptance import check_build
    from inline_fixture import query, scalar
    from lake_harness import sha256, write_json
    from native_auto_flush import connect_control
    from native_direct_campaign import assertions, verify_archive, verify_cleanup
    from native_runtime import PROFILE, RUNTIME, SOURCE, INSTALL
    from native_runtime_log import NativeLogSource
    from native_scalar_smp import TASK, INDEX
    from native_table_create import Case
    from native_test_context import OwnedDatabase, reviewed_runtime_version
    from native_vector_batch_scan import walk
    from permission_api import OwnedRoles, cleanup_roles

    if PROFILE != 'debug':
        raise RuntimeError('This scoped candidate admits the owned main Debug profile only')
    out = ROOT / '公共文档/openGauss原生湖存储/验证' / args.run_id
    out.mkdir(exist_ok=False)
    result = dict(status='fail', assertions=[], executed=False, plan=prepare(args.method,args.sql_version), plans=[])
    control = db = roles = case = None

    def check(name, observed, expected=True):
        good = observed == expected
        result['assertions'].append(dict(name=name, observed=deepcopy(observed),
            expected=deepcopy(expected), status='pass' if good else 'fail'))
        write_json(out / 'result.json', result)
        if not good:
            raise AssertionError(name)

    def session(conn, dop=1, indexed=False):
        for sql in ['SET session_timeout=0', 'SET statement_timeout=180000',
                    'SET enable_vector_engine=off', 'SET enable_lake_smp=on',
                    'SET enable_indexscan=' + ('on' if indexed else 'off'),
                    'SET enable_seqscan=' + ('off' if indexed else 'on'),
                    'SET query_dop=' + str(1000 + dop if dop > 1 else 1)]:
            query(conn, sql)

    def parity(label, predicate='true'):
        sql = 'SELECT id,val FROM {table} WHERE ' + predicate + ' ORDER BY id,val'
        check(label + ' exact complete ordinary heap result',
              query(case.a, sql.format(table='biz.orders')),
              query(case.observer, sql.format(table='biz.exact')))

    def explain(label, predicate='true', limit=False, analyze=True):
        sql = 'SELECT id,val FROM biz.orders WHERE ' + predicate + (' LIMIT 1' if limit else '')
        with case.a.cursor() as cursor:
            # Cursor-local raw text: ordinary dict decoding would silently erase
            # duplicate custom/core labels before this test could reject them.
            register_default_json(cursor, loads=lambda raw: raw)
            cursor.execute('EXPLAIN (' + ('ANALYZE,' if analyze else '') + 'FORMAT JSON) ' + sql)
            raw = cursor.fetchone()[0]
        check(label + ' raw JSON preserved before duplicate-key detection', isinstance(raw, str))
        (out / (label + '-explain.json')).write_text(raw + '\n')
        plan = json.loads(raw, object_pairs_hook=unique_object)
        all_nodes = [node for node in walk(plan) if 'Node Type' in node]
        for i, node in enumerate(all_nodes):
            startup, total = cost_pair(node)
            check(label + ' real finite nonnegative ordered costs/' + str(i), total >= startup >= 0)
        scans = [node for node in all_nodes if 'Native Lake' in node]
        check(label + ' one actual business lake scan', len(scans), 1)
        scan = scans[0]
        check(label + ' distinct custom planner baseline label', 'Base Planner Startup Cost' in scan)
        result['plans'].append(dict(label=label, sql=sql, analyze=analyze, plan=plan))
        return scan

    def files():
        return query(case.observer, 'SELECT data_file_id,path,record_count,file_size_bytes '
            'FROM created_metadata.ducklake_data_file WHERE end_snapshot IS NULL ORDER BY data_file_id')

    def physical(label, expected):
        actual = files()
        check(label + ' exact immutable per-file physical rows', [int(row[2]) for row in actual], expected)
        root = Path(case.empty_manifest['data_path']).resolve()
        evidence = []
        for file_id, name, count, size in actual:
            found = [path for path in root.rglob('*.parquet') if path.name == Path(name).name]
            check(label + ' unique owned physical file/' + str(file_id), len(found), 1)
            path = found[0].resolve(strict=True)
            check(label + ' file stays inside owned root/' + str(file_id), path.is_relative_to(root))
            literal = "'" + str(path).replace("'", "''") + "'"
            sql = 'SELECT count(*) AS physical FROM read_parquet(' + literal + ')'
            proc = subprocess.run([str(reference), '-no-init', '-batch', '-json', '-c',
                'SET threads=1;' + sql], capture_output=True, text=True, timeout=60)
            (out / (label + '-physical-' + str(file_id) + '.stdout')).write_text(proc.stdout)
            (out / (label + '-physical-' + str(file_id) + '.stderr')).write_text(proc.stderr)
            check(label + ' independent Parquet reader succeeds/' + str(file_id), proc.returncode, 0)
            check(label + ' independently counted physical rows/' + str(file_id),
                  json.loads(proc.stdout)[0]['physical'], int(count))
            target = out / ('source-' + str(file_id) + '.parquet')
            if not target.exists():
                shutil.copy2(path, target)
            check(label + ' physical source byte archive/' + str(file_id), sha256(target), sha256(path))
            evidence.append(dict(file_id=file_id, path=str(path), physical=count, bytes=size,
                                 sha256=sha256(path), archive=str(target)))
        result.setdefault('physical_sources', []).append(dict(label=label, files=evidence))
        return actual

    def both(statement):
        query(case.b, 'BEGIN')
        try:
            for table in ('biz.orders', 'biz.exact'):
                query(case.b, statement.format(table=table))
            query(case.b, 'COMMIT')
        except BaseException:
            query(case.b, 'ROLLBACK')
            raise

    def batch(first, last):
        both('INSERT INTO {table} SELECT i,repeat(md5(i::text),i%7+1) FROM generate_series('
             + str(first) + ',' + str(last) + ')i')
        query(case.b, "SELECT og_lake.flush('biz.orders'::regclass)")

    def stats(mode=0):
        return json.loads(scalar(case.a, 'SELECT biz.smp_probe(%s,0,0)', (mode,)))

    def stream(label, dop, covered, fallback, predicate='id>=6'):
        session(case.a, dop, indexed=True)
        stats(-1)
        mark = logs.mark()
        scan = explain(label, predicate)
        state = stats()
        result.setdefault('stream_rounds', []).append(dict(label=label, state=deepcopy(state)))
        check(label + ' real Stream operator exists', any(node.get('Node Type', '').startswith('Streaming')
            or 'Stream' in node.get('Node Type', '') for node in walk(result['plans'][-1]['plan'])))
        check(label + ' actual producer count', state['participants'], dop)
        check(label + ' four genuine file inputs', len(state['files']), 4)
        check(label + ' exact original physical file identities', sorted(state['files']), sorted(row[0] for row in files()))
        check(label + ' complete independent inline task', state['inline_task'], True)
        for field in ('started', 'claimed', 'done', 'reader_ready'):
            check(label + ' producer completion/' + field, [w[field] for w in state['workers']], [1] * dop)
        check(label + ' actual verified scalar sources', sum(w['scalar_files'] for w in state['workers']), covered)
        check(label + ' actual complete fallback sources', sum(w['fallback_files'] for w in state['workers']), fallback)
        check(label + ' actual bitmap backend count', sum(w['bitmap_files'] for w in state['workers']),
              covered if args.method == 'bitmap' else 0)
        expected = [(state['query'], state['scan'], state['snapshot'], 0, dop, 0, -1, 1)]
        for at, file_id in enumerate(state['files']):
            expected.append((state['query'], state['scan'], state['snapshot'], (at + 1) % dop, dop, at + 1, file_id, 0))
        try:
            text = mark.wait(lambda text: len([m for m in INDEX.finditer(text)
                if int(m[1]) == state['query'] and int(m[2]) == state['scan']]) >= dop, 5)
        finally:
            write_json(out / (label + '-log-source.json'), mark.receipt())
        (out / (label + '-server.log')).write_text(text)
        tasks = [tuple(map(int, m.groups(default='-1'))) for m in TASK.finditer(text)
                 if int(m[1]) == state['query'] and int(m[2]) == state['scan']]
        check(label + ' each file and inline source assigned exactly once', sorted(tasks), sorted(expected))
        actual = [tuple(map(int, m.groups())) for m in INDEX.finditer(text)
                  if int(m[1]) == state['query'] and int(m[2]) == state['scan']]
        wanted = [(state['query'], state['scan'], state['snapshot'], w['participant'], dop,
                   w['scalar_files'], w['bitmap_files'], w['fallback_files']) for w in state['workers']]
        check(label + ' exact actual index/fallback report per lane', sorted(actual), sorted(wanted))
        for worker in state['workers']:
            lane = worker['participant']
            owned = [file_id for at, file_id in enumerate(state['files']) if (at + 1) % dop == lane]
            indexed = sum(file_id in covered_ids for file_id in owned)
            check(label + ' real coverage follows assigned source identities/' + str(lane),
                  (worker['scalar_files'], worker['fallback_files'], worker['bitmap_files']),
                  (indexed, len(owned) - indexed, indexed if args.method == 'bitmap' else 0))
        if dop == 2:
            check(label + ' two files consumed per actual lane',
                  [w['scalar_files'] + w['fallback_files'] for w in state['workers']], [2, 2])
        # Run the same logical predicate separately to compare every output.
        # Do not treat EXPLAIN ANALYZE output cardinality alone as row equality.
        parity(label, predicate)
        return scan

    try:
        build = check_build(args.build_receipt)
        result.update(build_receipt=str(args.build_receipt.resolve()), build_sha256=sha256(args.build_receipt),
            source_head=build['source_head'], source_diff_sha256=build['source_diff_sha256'],
            binary_sha256=build['binary_sha256'])
        check('official source includes the real cost ledger', 'contrib/og_lake/lake_cost.cpp' in build['native_source_inputs_after'])
        check('compiled EXPLAIN no longer shadows real core startup',
              '"Base Planner Startup Cost"' in (SOURCE / 'contrib/og_lake/lake_inline_scan.cpp').read_text())
        check('compiled Stream has scalar worker interface',
              'LakeStreamWorkerScalarSources' in (SOURCE / 'contrib/og_lake/lake_stream.cpp').read_text())
        if args.method == 'bitmap':
            check('bitmap has an explicitly qualified dependency receipt', bool(build.get('roaring_inputs_after')))
            check('bitmap dependency remains qualified and identical', build.get('roaring_inputs_before'), build.get('roaring_inputs_after'))
            check('bitmap archive symbols remain hidden', build['roaring_symbol_visibility']['dynamic_overlap_count'], 0)
        # Archive all actual Native/build/SDK/dependency inputs plus recursively
        # imported test helpers, using the existing receipt inventories.
        pending = [Path(__file__).resolve(), args.build_receipt.resolve(), D / 'reference.sh', D / 'source-lock.json',
                   D / 'tests/native_direct_insert.py', D / 'tests/native_direct_matrix.json', D / 'tests/inline_cleanup.py']
        local = set(pending)
        for name in ['native_table_create', 'native_test_context', 'native_scalar_smp', 'native_vector_batch_scan',
                     'native_direct_campaign', 'file_dml_acceptance', 'catalog_fixture_bundle']:
            path = D / (name + '.py')
            pending.append(path if path.is_file() else D / 'tests' / (name + '.py'))
        while pending:
            path = pending.pop()
            if path in local and path.suffix != '.py':
                continue
            local.add(path)
            if path.suffix != '.py':
                continue
            for node in ast.walk(ast.parse(path.read_text())):
                names = [item.name.split('.')[0] for item in node.names] if isinstance(node, ast.Import) else [
                    node.module.split('.')[0]] if isinstance(node, ast.ImportFrom) and node.module else []
                for name in names:
                    for directory in (D, D / 'tests'):
                        candidate = directory / (name + '.py')
                        if candidate.is_file() and candidate not in local:
                            local.add(candidate)
                            pending.append(candidate)
        local.update(SOURCE / name for name in build['native_source_inputs_after'])
        local.update(Path(name) for name in build['build_wrapper_inputs_after'])
        local.add(INSTALL / 'bin/gaussdb')
        local.update(path for path in (INSTALL / 'share/postgresql/extension').glob('og_lake*') if path.is_file())
        sdk = INSTALL / 'lib/postgresql/og_lake_sdk'
        sdk_owner = json.loads((sdk / 'owner.json').read_text())
        check('SDK owner target is the actual loaded installation', str(Path(sdk_owner['target']).resolve()), str(sdk.resolve()))
        check('SDK provider has the qualified Native project owner', sdk_owner['project'], 'opengauss-native-lake-local-provider')
        check('SDK owner file inventory equals actual provider bytes',
              {str(path.relative_to(sdk)): sha256(path) for path in sdk.rglob('*')
               if path.is_file() and path != sdk / 'owner.json'}, sdk_owner['files'])
        local.update(path for path in sdk.rglob('*') if path.is_file())
        reference_receipt = ROOT / '.reference/opengauss-native-lake/build/receipt.json'
        reference_info = json.loads(reference_receipt.read_text())
        lock = json.loads((D / 'source-lock.json').read_text())
        reference = ROOT / '.reference/opengauss-native-lake' / lock['reference_build']['build_subdir'] / 'duckdb'
        check('genuine reference binary matches receipt', sha256(reference), reference_info['binary_sha256'])
        check('genuine reference uses exact source lock', reference_info['source_lock_hash'], sha256(D / 'source-lock.json'))
        local.update([reference, reference_receipt])
        for key in ('roaring', 'pq'):
            dependency = build.get(key + '_inputs_after')
            if not dependency:
                continue
            check(key + ' dependency frozen during official build', build[key + '_inputs_before'], dependency)
            local.add(Path(dependency['receipt']))
            check(key + ' dependency receipt identity', sha256(dependency['receipt']), dependency['receipt_sha256'])
            for name, digest in dependency['files'].items():
                path = Path(dependency['prefix']) / name
                check(key + ' dependency file identity/' + name, sha256(path), digest)
                local.add(path)
        archive = out / 'executed-inputs'
        archive.mkdir()
        result['inputs'] = {}
        def archive_one(path):
            path = path.resolve()
            if str(path) in result['inputs']:
                return
            target = archive / ('%04d-' % len(result['inputs']) + path.name)
            shutil.copy2(path, target)
            result['inputs'][str(path)] = dict(sha256=sha256(path), archive=str(target), bytes=path.stat().st_size)
        for path in sorted(local, key=str):
            archive_one(path)
        control = connect_control()
        for sql in ('SET session_timeout=0', 'SET statement_timeout=30000'):
            query(control, sql)
        check('launcher maintenance remains disabled', scalar(control, 'SHOW enable_lake_maintenance'), 'off')
        check('dedicated backend for real producer diagnostics', scalar(control, 'SHOW enable_thread_pool'), 'off')
        logs = NativeLogSource.from_connection(control, RUNTIME)
        with (out / 'fixture.log').open('w') as log:
            subprocess.run([sys.executable, str(D / 'tests/catalog_fixture_bundle.py'), '--run-id', args.run_id,
                '--output', str(out / 'fixture/manifest.json')], stdout=log, stderr=subprocess.STDOUT, timeout=300, check=True)
        bundle = json.loads((out / 'fixture/manifest.json').read_text())
        empty = json.loads(Path(bundle['entries']['same']['manifest']).read_text())
        data_root = Path(empty['data_path']).resolve()
        check('own fresh EMPTY still has no materialized data root', not data_root.exists())
        check('fresh data root is within this exact fixture owner',
              data_root.is_relative_to(Path(bundle['cleanup_owner']).resolve()))
        check('fresh EMPTY belongs to the exact child run', empty['run_id'], args.run_id + '-same')
        for path in sorted((out / 'fixture').rglob('*')):
            if path.is_file():
                archive_one(path)
        db = OwnedDatabase(args.run_id, out)
        db.create()
        result['postmaster_start'] = db.allocation['postmaster_start']
        query(db.admin, 'SET session_timeout=0')
        roles = OwnedRoles(control, args.run_id, out)
        case = Case(db, SimpleNamespace(mode='net_dml', sql_version=args.sql_version, manifest=out / 'fixture/manifest.json',
                    output=out / 'result.json', run_id=args.run_id), result, roles)
        check('actual explicitly installed SQL19', scalar(case.a, "SELECT extversion FROM pg_extension WHERE extname='og_lake'"), '0.19')
        check('actual runtime matches exact source control',json.loads(scalar(case.a,'SELECT og_lake.host_status()'))['module_version'],reviewed_runtime_version())
        check('actual installed cost SQL matches selection',scalar(case.a,"SELECT extversion FROM pg_extension WHERE extname='og_lake'"),args.sql_version)
        for conn in (case.a, case.b, case.observer):
            session(conn)
        query(case.a, 'SET enable_default_ustore_table=off')
        query(case.a, "CREATE FOREIGN TABLE biz.orders(id integer,val text) SERVER created_lake OPTIONS(binding_mode 'create',lake_schema 'main')")
        query(case.a, 'CREATE TABLE biz.exact(id integer,val text)')
        query(case.a, "SELECT og_lake.set_option('biz.orders'::regclass,'data_inlining_row_limit','10000')")
        query(case.a, "CREATE FUNCTION biz.smp_probe(integer,integer,bigint) RETURNS text AS '$libdir/og_lake','og_lake_test_smp_probe' LANGUAGE C VOLATILE NOT FENCED")
        query(case.a, 'REVOKE ALL ON FUNCTION biz.smp_probe(integer,integer,bigint) FROM PUBLIC')
        result['executed'] = True
        batch(1, 8)
        batch(9, 40)
        physical('initial', [8, 32])
        both('DELETE FROM {table} WHERE id<=5')
        query(case.a, 'ANALYZE biz.orders')
        check('ANALYZE visible cardinality excludes ended physical versions',
              float(scalar(case.a, "SELECT reltuples FROM pg_class WHERE oid='biz.orders'::regclass")), 35.0)
        base = explain('base-after-delete')
        check('base has complete file access', base['Planned File Access'], 'complete SDK scan')
        check('cost visible baseline is ANALYZE visible cardinality', float(base['Visible Row Baseline']), 35.0)
        check('physical estimate still includes immutable deleted rows', float(base['Stored Version Estimate']) >= 40.0)
        check('visible and stored-version estimates remain distinct', float(base['Stored Version Estimate']) > float(base['Visible Row Baseline']))
        parity('base-after-delete')
        physical('after-delete', [8, 32])
        store = out / 'index-store'
        store.mkdir()
        query(case.a, "SELECT og_lake.set_option('created_lake'::name,'index_root',%s::text)", (str(store),))
        index = str(scalar(case.a, "SELECT og_lake.create_lake_index('biz.orders'::regclass,'cost_id',%s::jsonb)",
              (json.dumps(dict(version=1, kind='scalar', method=args.method, column='id')),)))
        query(case.a, 'SELECT og_lake.build_lake_index(%s::uuid)', (index,))
        covered_ids = {int(row[0]) for row in query(case.a, 'SELECT DISTINCT source_file_id '
            'FROM og_lake_internal.index_coverage WHERE index_uuid=%s::uuid AND active', (index,))}
        check('derived coverage records the two original source identities', sorted(covered_ids), sorted(row[0] for row in files()))
        session(case.a, indexed=True)
        complete = explain('covered-candidates', 'id>=6')
        check('actual scalar index covers both files', complete['Verified Scalar File Coverage'], 2)
        check('actual bitmap backend agrees with selected method', complete['Verified Bitmap File Coverage'], 2 if args.method == 'bitmap' else 0)
        check('candidate preparation precedes first scan output',
              float(complete['Startup Cost']) > float(complete['Base Planner Startup Cost']))
        zero = explain('zero-candidates', 'id=-1000')
        check('zero matching keys retain actual full source coverage', zero['Verified Scalar File Coverage'], 2)
        check('zero matching keys still pay candidate startup', float(zero['Startup Cost']) > float(zero['Base Planner Startup Cost']))
        parity('zero-candidates', 'id=-1000')
        limit = explain('limit-one-startup', 'id>=6', limit=True, analyze=False)
        check('LIMIT1 does not prorate preparation out of child scan startup', limit['Startup Cost'], complete['Startup Cost'])
        batch(41, 52)
        partial = explain('partial-coverage', 'id>=6')
        check('uncovered new file remains a complete input', partial['Complete File Fallback Inputs'], 1)
        check('covered sources remain exactly the original two', partial['Verified Scalar File Coverage'], 2)
        parity('partial-coverage', 'id>=6')
        batch(53, 72)
        both("INSERT INTO {table} VALUES(1001,'inline-a'),(1002,'inline-b'),(1003,'inline-c')")
        query(case.a, 'ANALYZE biz.orders')
        actual_files = physical('final', [8, 32, 12, 20])
        check('actual file bytes are unequal and positive', len({int(row[3]) for row in actual_files}) > 1 and all(row[3] > 0 for row in actual_files))
        final = explain('serial-mixed', 'id>=6')
        check('mixed coverage keeps exactly two indexed sources', final['Verified Scalar File Coverage'], 2)
        check('mixed coverage keeps exactly two complete fallback sources', final['Complete File Fallback Inputs'], 2)
        check('current complete visible count', scalar(case.a, 'SELECT count(*) FROM biz.orders'), 70)
        parity('serial-mixed', 'id>=6')
        head, protocol = case.head(), case.protocol()
        source_hashes = {row['path']: row['sha256'] for phase in result['physical_sources'] for row in phase['files']}
        artifact_hashes = {str(path): sha256(path) for path in store.rglob('*.parquet')}
        serial_preparation = float(final['Startup Cost']) - float(final['Base Planner Startup Cost'])
        for dop in (2, 4):
            scan = stream('mixed-dop' + str(dop), dop, 2, 2)
            preparation = float(scan['Startup Cost']) - float(scan['Base Planner Startup Cost'])
            check('DOP' + str(dop) + ' startup contains nonzero actual-lane candidate preparation', preparation > 0)
            check('DOP' + str(dop) + ' preparation respects serial upper envelope at zero native thread cost', preparation <= serial_preparation + .03)
        check('read-only cost/index/SMP creates no snapshots or IDs', case.head(), head)
        check('read-only cost/index/SMP mutates no standard catalog or carrier', case.protocol(), protocol)
        check('all immutable source bytes unchanged', {path: sha256(path) for path in source_hashes}, source_hashes)
        check('all derived index bytes unchanged', {str(path): sha256(path) for path in store.rglob('*.parquet')}, artifact_hashes)
        check('all archived executed inputs still exact', verify_archive(result), len(result['inputs']))
        check_build(args.build_receipt)
        result['status'] = 'pass'
    except BaseException as exc:
        result.update(reason=str(exc), sqlstate=getattr(exc, 'pgcode', None), traceback=traceback.format_exc())
    finally:
        if db is not None:
            try:
                db.cleanup()
                result['database_cleanup'] = db.cleanup_result
                owner_path = out / 'database-owner.json'
                result['database_cleanups'] = [dict(attempt=str(out), owner=dict(path=str(owner_path),
                    sha256=sha256(owner_path), receipt=json.loads(owner_path.read_text())), cleanup=db.cleanup_result)]
            except BaseException as exc:
                result.update(status='fail', database_cleanup_error=str(exc))
        if roles is not None:
            try:
                if not result.get('database_cleanup') or result['database_cleanup']['returncode']:
                    raise RuntimeError('Exact owned DB cleanup is unproven; roles retained')
                result['role_cleanup'] = cleanup_roles(control, roles.path, args.run_id)
                result = roles.redact(result)
                result['strict_cleanup'] = verify_cleanup(result, args.run_id, out, result['postmaster_start'])
            except BaseException as exc:
                result.update(status='fail', role_cleanup_error=str(exc))
        if control is not None:
            control.close()
        if result['status'] == 'pass':
            try:
                result['strict_assertion_count'] = assertions(result)
            except BaseException as exc:
                result.update(status='fail', assertion_review_error=str(exc))
        write_json(out / 'result.json', result)
    return 0 if result['status'] == 'pass' else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--execute', action='store_true')
    parser.add_argument('--method', choices=['btree', 'bitmap'], required=True)
    parser.add_argument('--sql-version',choices=['0.19','0.20','0.21'],default='0.19')
    parser.add_argument('--run-id')
    parser.add_argument('--build-receipt', type=Path)
    parser.add_argument('--case-timeout', type=int, default=1800)
    parser.add_argument('--prepare-output', type=Path)
    parser.add_argument('--_isolated-child', action='store_true', help=argparse.SUPPRESS)
    args = parser.parse_args()
    if not 300 <= args.case_timeout <= 3600:
        parser.error('case-timeout must be300..3600 seconds')
    if not args.execute:
        plan = prepare(args.method,args.sql_version)
        if args.prepare_output:
            if args.prepare_output.exists():
                parser.error('Never overwrite prior evidence')
            args.prepare_output.write_text(json.dumps(plan, ensure_ascii=False, indent=2) + '\n')
        print(json.dumps(plan, ensure_ascii=False, indent=2))
        return 0
    if not args.run_id or args.build_receipt is None:
        parser.error('execute requires run-id and build-receipt')
    if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,119}', args.run_id):
        parser.error('Invalid run ID')
    if not args._isolated_child:
        return supervise(args)
    if os.getenv('OG_LAKE_COST_CHILD_PARENT') != str(os.getppid()):
        parser.error('Unsupervised private child')
    return execute(args)


if __name__ == '__main__':
    raise SystemExit(main())
