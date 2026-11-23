#!/usr/bin/env python3
"""Positive0.17 RC row-head/tuple-lock/EPQ and statement-overlay contracts.

Draft executable cases for a NEW exact native build. Never interprets a038 as
implementing these targets. Core modes cover first simultaneous row-head
creation for a flushed source (stable row_id != physical position), an already
committed inline row head, and per-statement committed reads plus own writes.
Both commit and rollback outcomes are required for each core wait mode.
WHERE-false and update/delete modes also retain the exact latest-row RETURNING
and skip contracts. Explicit cancellation and subtransaction rollback prove
waiter/holder resource release. flush-during-wait requires a committed inline
source to move into Parquet while a writer holds its logical head and a second
writer waits; no optimistic-conflict allowance weakens that target. Passing
this bounded subset cannot qualify all RC statements, cold-head GC or RAC.
Do not run during active native compilation or another reference fixture.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeout
import datetime
import json
from pathlib import Path
import shutil
import subprocess
import sys
import time
import traceback
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[3]
D = ROOT / 'integration/opengauss-native-lake'
TARGET = 3
sys.path[:0] = [str(D), str(D / 'tests')]
from file_dml_acceptance import check_build
from inline_fixture import query, scalar
from lake_harness import sha256, validate_run_id, write_json
from native_auto_flush import connect_control
from native_runtime import INSTALL, PROFILE
from native_table_create import Case
from native_test_context import OwnedDatabase, reviewed_runtime_version
from permission_api import OwnedRoles, cleanup_roles


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--build-receipt', type=Path, required=True)
    parser.add_argument('--mode', choices=['flushed-first', 'inline-existing', 'statement-overlay',
                        'where-false', 'delete-update', 'update-delete', 'delete-delete',
                        'wait-cancel', 'savepoint-release', 'flush-during-wait'], required=True)
    parser.add_argument('--first-outcome', choices=['commit', 'rollback'], default='commit')
    parser.add_argument('--sql-version', choices=['0.17', '0.18'], default='0.17',
                        help='explicit installed SQL contract; runtime identity is checked separately')
    args = parser.parse_args()
    validate_run_id(args.run_id)
    if PROFILE != 'debug':
        parser.error('Requires the separately owned main Debug runtime')
    if args.mode not in ['flushed-first', 'inline-existing'] and args.first_outcome != 'commit':
        parser.error('This mode has a fixed committed/own-write timeline')
    flushed = args.mode in ['flushed-first', 'where-false', 'delete-update', 'update-delete', 'delete-delete',
                           'wait-cancel', 'savepoint-release']
    history = flushed or args.mode == 'flush-during-wait'
    out = ROOT / '公共文档/openGauss原生湖存储/验证' / args.run_id
    out.mkdir(exist_ok=False)
    result = dict(status='fail', scope=__doc__, mode=args.mode, first_outcome=args.first_outcome,
                  installed_sql=args.sql_version,
                  assertions=[], events=[], limitations=['Core positive targets only; no general RC/RAC/performance qualification.'])
    inputs = [Path(__file__), D / 'tests/file_dml_acceptance.py', D / 'tests/native_table_create.py',
              D / 'tests/permission_api.py', D / 'native_test_context.py', D / 'native_runtime.py',
              D / 'tests/inline_cleanup.py', D / 'inline_fixture.py', D / 'tests/native_auto_flush.py',
              D / 'tests/catalog_fixture_bundle.py', D / 'tests/empty_reference_fixture.py',
              D / 'tests/empty_manifest.py', D / 'tests/empty_catalog_oracle.py',
              D / 'tests/create_reference_oracle.py', D / 'tests/reverse_reference.py',
              D / 'reference.sh', D / 'fixture.py', D / 'lake_harness.py', D / 'source-lock.json']
    result['inputs'] = {str(path): sha256(path) for path in inputs}
    archive = out / 'executed-inputs'
    archive.mkdir()
    for path in inputs:
        shutil.copy2(path, archive / path.name)
    provider = INSTALL / 'lib/postgresql/og_lake_sdk/libog_lake_sdk_arrow.so'
    duckdb = ROOT / '.reference/opengauss-native-lake/build/duckdb-gcc10/duckdb'
    control = db = roles = case = None
    workers, transactions = [], []
    committed_model = None
    committed_markers = set()

    def check(name, observed, expected=True):
        good = observed == expected
        result['assertions'].append(dict(name=name, observed=observed, expected=expected,
                                         status='pass' if good else 'fail'))
        if not good:
            raise AssertionError(name + ': ' + repr(observed))

    def event(name, **values):
        result['events'].append(dict(name=name, at=datetime.datetime.now(datetime.timezone.utc).isoformat(), **values))

    def configure(conn):
        query(conn, 'SET session_timeout=0')
        query(conn, 'SET statement_timeout=30000')

    def begin(conn):
        query(conn, 'BEGIN ISOLATION LEVEL READ COMMITTED')
        transactions.append(conn)

    def lock_rows(conn, pid, waiting=False):
        return query(conn, 'SELECT pid,locktype,database,relation,page,tuple,transactionid::text,mode,granted '
                     'FROM pg_locks WHERE pid=%s' + (' AND NOT granted' if waiting else '')
                     + ' ORDER BY locktype,relation,page,tuple,transactionid,mode,granted', (pid,))

    def execute_action(conn, table, action):
        if action == 'delete':
            return query(conn, 'DELETE FROM ' + table + ' WHERE id=%s RETURNING id,val', (TARGET,))
        where = ' AND val=10' if action == 'where10' else ''
        value = '20' if action == 'set20' else 'val+1'
        return query(conn, 'UPDATE ' + table + ' SET val=' + value + ' WHERE id=%s' + where + ' RETURNING id,val', (TARGET,))

    def model_action(model, action):
        expected = dict(model)
        if TARGET not in expected or (action == 'where10' and expected[TARGET] != 10):
            return expected, []
        if action == 'delete':
            removed = expected.pop(TARGET)
            return expected, [(TARGET, removed)]
        expected[TARGET] = 20 if action == 'set20' else expected[TARGET] + 1
        return expected, [(TARGET, expected[TARGET])]

    def async_update(conn, table, label, action='increment'):
        pid = scalar(conn, 'SELECT pg_backend_pid()')
        app = scalar(conn, "SELECT current_setting('application_name')")
        def execute():
            try:
                rows = execute_action(conn, table, action)
                return dict(state='completed', action=action, rows=rows)
            except Exception as exc:
                return dict(state='error', sqlstate=getattr(exc, 'pgcode', None), message=str(exc))
        pool = ThreadPoolExecutor(max_workers=1)
        item = dict(conn=conn, pid=pid, app=app, label=label, pool=pool, future=pool.submit(execute))
        workers.append(item)
        return item

    def wait_on_holder(worker, holder_pid, relation_oid, label):
        deadline = time.monotonic() + 10
        while True:
            waits = lock_rows(case.observer, worker['pid'], True)
            held = lock_rows(case.observer, holder_pid)
            matching = []
            for wait in waits:
                if wait[1] in ('transactionid', 'subtransactionid') and wait[6] is not None:
                    if any(row[1] == wait[1] and row[6] == wait[6] and row[8] for row in held):
                        matching.append(wait)
                elif wait[1] == 'tuple' and wait[3] == relation_oid:
                    if any(row[1:7] == wait[1:7] and row[8] for row in held):
                        matching.append(wait)
            if matching and not worker['future'].done():
                proof = dict(label=label, state='waiting', waiter_pid=worker['pid'], holder_pid=holder_pid,
                             required_relation_oid=relation_oid, matching_waits=matching, all_waits=waits, holder_locks=held)
                result.setdefault('real_lock_waits', []).append(proof)
                check(label + ' is still executing while the first transaction holds its lock', worker['future'].done(), False)
                return proof
            if worker['future'].done():
                result.setdefault('unexpected_early_updates', []).append(dict(label=label, reply=worker['future'].result(),
                    waits=waits, holder_locks=held))
                raise AssertionError(label + ' returned before its holder ended')
            if time.monotonic() >= deadline:
                result.setdefault('missing_lock_waits', []).append(dict(label=label, waits=waits, holder_locks=held))
                raise AssertionError(label + ' has no observed tuple/transaction wait on its holder')
            time.sleep(.02)  # A poll interval, never the evidence of blocking.

    def cancel_owned_worker(worker):
        identity = query(case.observer, 'SELECT datid,application_name FROM pg_stat_activity WHERE pid=%s', (worker['pid'],))
        check(worker['label'] + ' cancellation targets the exact owned backend', identity,
              [(db.allocation['database_oid'], worker['app'])])
        check(worker['label'] + ' explicit cancellation was delivered',
              scalar(case.observer, 'SELECT pg_cancel_backend(%s)', (worker['pid'],)))

    try:
        from psycopg2 import sql
        built = check_build(args.build_receipt)
        result.update(source_head=built['source_head'], binary_sha256=built['binary_sha256'],
                      build=str(args.build_receipt.resolve()), build_sha256=sha256(args.build_receipt),
                      provider_sha256=sha256(provider), reference_binary_sha256=sha256(duckdb))
        control = connect_control()
        configure(control)
        check('automatic maintenance is disabled', scalar(control, 'SHOW enable_lake_maintenance'), 'off')
        check('lock PID oracle requires dedicated backends', scalar(control, 'SHOW enable_thread_pool'), 'off')
        with (out / 'fixture.log').open('w') as log:
            subprocess.run([sys.executable, str(D / 'tests/catalog_fixture_bundle.py'), '--run-id', args.run_id,
                            '--output', str(out / 'fixture/manifest.json')], stdout=log,
                           stderr=subprocess.STDOUT, check=True, timeout=300)
        db = OwnedDatabase(args.run_id, out)
        db.create()
        configure(db.admin)
        roles = OwnedRoles(control, args.run_id, out)
        case = Case(db, SimpleNamespace(mode='net_dml', sql_version=args.sql_version, manifest=out / 'fixture/manifest.json',
                    output=out / 'result.json', run_id=args.run_id), result, roles)
        for conn in [case.a, case.b, case.observer]:
            configure(conn)
        check('RC suite installs exact selected SQL contract', scalar(case.observer,
              "SELECT extversion FROM pg_extension WHERE extname='og_lake'"), args.sql_version)
        check('RC suite runs exact reviewed module', json.loads(scalar(case.observer,
              'SELECT og_lake.host_status()'))['module_version'], reviewed_runtime_version())
        query(case.a, 'SET enable_default_ustore_table=off')
        case.create()
        host_oid = scalar(case.observer, "SELECT 'biz.orders'::regclass::oid")
        mapping = query(case.observer, 'SELECT h.binding_id::text,h.relation_oid,h.row_head_oid,n.nspname,c.relname,'
                        'c.relowner,c.relkind,c.reloptions FROM og_lake_internal.relation_binding r '
                        'JOIN og_lake_internal.row_lock_binding h ON h.binding_id=r.binding_id '
                        'JOIN pg_class c ON c.oid=h.row_head_oid JOIN pg_namespace n ON n.oid=c.relnamespace '
                        'WHERE r.relation_oid=%s AND h.relation_oid=r.relation_oid', (host_oid,))
        check('exact business binding resolves one permanent row-head OID', len(mapping), 1)
        binding_id, registered_host, head_oid, head_schema, head_name, head_owner, head_kind, head_options = mapping[0]
        result['row_head_binding'] = dict(binding_id=binding_id, business_oid=registered_host, row_head_oid=head_oid,
            regclass=scalar(case.observer, 'SELECT %s::oid::regclass::text', (head_oid,)), options=head_options)
        check('row-head is a private plain heap relation', (head_schema, head_kind), ('og_lake_internal', 'r'))
        check('row-head owner is the configured private role', head_owner,
              scalar(case.observer, 'SELECT oid FROM pg_roles WHERE rolname=%s', (case.owner,)))
        check('row-head explicitly uses astore without tuple UIDs',
              'storage_type=astore' in (head_options or []) and 'hasuids=false' in (head_options or []))
        columns = query(case.observer, 'SELECT attname,atttypid,attnotnull FROM pg_attribute '
                        'WHERE attrelid=%s AND attnum>0 AND NOT attisdropped ORDER BY attnum', (head_oid,))
        check('row-head stores only a nonnull bigint logical ID', columns, [('row_id', 20, True)])
        indexes = query(case.observer, 'SELECT i.indisprimary,i.indisunique,i.indisvalid,i.indkey::text,a.amname '
                        'FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid JOIN pg_am a ON a.oid=c.relam '
                        'WHERE i.indrelid=%s', (head_oid,))
        check('row-head identity has a real native unique primary B-tree',
              any(row == (True, True, True, '1', 'btree') for row in indexes))
        head_sql = sql.SQL('SELECT row_id,xmax::text FROM {} ORDER BY row_id').format(sql.Identifier(head_schema, head_name))
        def head_rows(conn=None):
            return query(conn or case.observer, head_sql)
        check('business CREATE prepares an empty row-head carrier', head_rows(), [])
        query(case.a, 'INSERT INTO biz.orders VALUES(1,100),(2,200),(3,10)')
        check('new INSERT identities do not eagerly materialize row heads', head_rows(), [])
        table_id = scalar(case.observer, "SELECT table_id FROM created_metadata.ducklake_table WHERE table_name='orders' AND end_snapshot IS NULL")
        carrier = scalar(case.observer, 'SELECT table_name FROM created_metadata.ducklake_inlined_data_tables WHERE table_id=%s', (table_id,))
        raw_sql = sql.SQL('SELECT _ducklake_row_id,_ducklake_begin_snapshot,_ducklake_end_snapshot,id,val '
                         'FROM created_metadata.{} ORDER BY _ducklake_row_id,_ducklake_begin_snapshot').format(sql.Identifier(carrier))
        if history:
            query(case.a, 'UPDATE biz.orders SET val=val+1 WHERE id=1')
        elif args.mode == 'inline-existing':
            for amount in [1, -1]:
                query(case.a, 'UPDATE biz.orders SET val=val+(%s) WHERE id=%s', (amount, TARGET))
        before = query(case.observer, raw_sql)
        target = [row for row in before if row[3] == TARGET and row[2] is None]
        check('one live committed target is available', len(target), 1)
        logical_id, birth, _, _, value = target[0]
        check('target begins at10', value, 10)
        initial_next = scalar(case.observer, 'SELECT next_row_id FROM created_metadata.ducklake_table_stats WHERE table_id=%s', (table_id,))
        committed_model = {1: 101 if history else 100, 2: 200, TARGET: 10}
        check('baseline business rows match explicit input model', case.rows(), sorted(committed_model.items()))
        initial_heads = head_rows()
        check('target head state matches the selected setup', sum(row[0] == logical_id for row in initial_heads),
              1 if args.mode == 'inline-existing' else 0)
        result['origin'] = dict(logical_id=logical_id, birth=birth, initial_next_row_id=initial_next,
                                initial_row_heads=initial_heads, inline_versions_before_flush=before)
        source = source_hash = original_file = original_position = None
        delete_name = 'ducklake_inlined_delete_' + str(table_id)
        delete_sql = sql.SQL('SELECT file_id,row_id,begin_snapshot FROM created_metadata.{} ORDER BY 1,2,3').format(sql.Identifier(delete_name))
        def marks():
            present = scalar(case.observer, 'SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace '
                             "WHERE n.nspname='created_metadata' AND c.relname=%s", (delete_name,))
            return query(case.observer, delete_sql) if present else []
        def observe_flushed_origin():
            nonlocal source, source_hash, original_file, original_position
            check('flushed source has no delta/inline physical rows', query(case.observer, raw_sql), [])
            check('flushed target begins with no inline position-delete marker', marks(), [])
            files = query(case.observer, 'SELECT data_file_id,path,record_count FROM created_metadata.ducklake_data_file '
                          'WHERE table_id=%s AND end_snapshot IS NULL', (table_id,))
            check('one complete historical prefix became a real data file', len(files), 1)
            original_file, path, population = files[0]
            candidates = [p for p in Path(case.empty_manifest['data_path']).rglob('*.parquet') if p.name == Path(path).name]
            check('catalog source resolves to one owned Parquet file', len(candidates), 1)
            source = candidates[0]
            source_hash = sha256(source)
            statement = ('SELECT file_row_number AS position,_ducklake_internal_row_id AS logical_id,'
                         '_ducklake_internal_snapshot_id AS birth,id,val FROM read_parquet('
                         + "'" + str(source).replace("'", "''") + "',file_row_number=true) ORDER BY file_row_number")
            (out / 'flushed-physical.sql').write_text(statement + '\n')
            proc = subprocess.run([str(duckdb), '-no-init', '-batch', '-json', '-c', 'SET threads=1;' + statement],
                                  capture_output=True, text=True, timeout=60)
            (out / 'flushed-physical.stdout').write_text(proc.stdout)
            (out / 'flushed-physical.stderr').write_text(proc.stderr)
            check('independent Parquet decoder succeeds', proc.returncode, 0)
            physical = json.loads(proc.stdout)
            check('flush preserves the full original identity/birth prefix', physical,
                  [dict(position=i, logical_id=row[0], birth=row[1], id=row[3], val=row[4]) for i, row in enumerate(before)])
            check('catalog physical population equals real source', population, len(physical))
            found = [row for row in physical if row['id'] == TARGET]
            check('one original target exists in Parquet', len(found), 1)
            original_position = found[0]['position']
            check('target stable R differs from physical P', logical_id != original_position)
            result['origin'].update(file_id=original_file, position=original_position, source=str(source), source_sha256=source_hash)
        if flushed:
            query(case.a, "SELECT og_lake.flush('biz.orders'::regclass)")
            check('flush retains row-head records and their OID', head_rows(), initial_heads)
            observe_flushed_origin()
        query(case.a, 'CREATE TABLE biz.reference(id integer PRIMARY KEY,val integer)')
        for business_id, val in sorted(committed_model.items()):
            query(case.a, 'INSERT INTO biz.reference VALUES(%s,%s)', (business_id, val))

        def equal_own(conn, expected, label):
            check(label + ' lake values', case.rows(conn), sorted(expected.items()))
            check(label + ' independent heap values', query(conn, 'SELECT id,val FROM biz.reference ORDER BY id'), sorted(expected.items()))

        if args.mode == 'statement-overlay':
            begin(case.a)
            equal_own(case.a, committed_model, 'initial explicitRC statement')
            for marker, business_id, increment in [(201, TARGET, 1), (202, 2, 100)]:
                begin(case.b)
                for table in ['biz.orders', 'biz.reference']:
                    query(case.b, 'UPDATE ' + table + ' SET val=val+(%s) WHERE id=%s', (increment, business_id))
                query(case.b, 'INSERT INTO biz.heap_marker VALUES(%s)', (marker,))
                query(case.b, 'COMMIT')
                committed_model[business_id] += increment
                committed_markers.add(marker)
                expected = dict(committed_model)
                if marker == 201:
                    equal_own(case.a, expected, 'next statement sees a new external commit')
                    for table in ['biz.orders', 'biz.reference']:
                        check('first own increment on ' + table, query(case.a,
                              'UPDATE ' + table + ' SET val=val+1 WHERE id=%s RETURNING id,val', (TARGET,)), [(TARGET, 12)])
                    query(case.a, 'INSERT INTO biz.heap_marker VALUES(101)')
                expected[TARGET] = 12
                equal_own(case.a, expected, 'new committed inputs plus own overlay after marker' + str(marker))
            query(case.a, 'SAVEPOINT private_change')
            for table in ['biz.orders', 'biz.reference']:
                check('child increment on ' + table, query(case.a,
                      'UPDATE ' + table + ' SET val=val+1 WHERE id=%s RETURNING id,val', (TARGET,)), [(TARGET, 13)])
            child = dict(committed_model)
            child[TARGET] = 13
            equal_own(case.a, child, 'child command sees exactly its own latest version')
            query(case.a, 'ROLLBACK TO private_change')
            query(case.a, 'RELEASE SAVEPOINT private_change')
            parent = dict(committed_model)
            parent[TARGET] = 12
            equal_own(case.a, parent, 'savepoint rollback restores stable-ID overlay')
            for table in ['biz.orders', 'biz.reference']:
                check('second parent increment on ' + table, query(case.a,
                      'UPDATE ' + table + ' SET val=val+1 WHERE id=%s RETURNING id,val', (TARGET,)), [(TARGET, 13)])
            query(case.a, 'COMMIT')
            committed_model[TARGET] = 13
            committed_markers.add(101)
        elif args.mode in ['wait-cancel', 'savepoint-release']:
            # Exercise the ordinary heap and lake independently. A successful
            # finally/ROLLBACK is not a substitute for these positive contracts.
            ha, hb = db.connect(), db.connect()
            configure(ha)
            configure(hb)
            heap_oid = scalar(case.observer, "SELECT 'biz.reference'::regclass::oid")
            baseline = dict(committed_model)
            for label, table, first, second, lock_relation in [('ordinary-heap', 'biz.reference', ha, hb, heap_oid),
                    ('lake', 'biz.orders', case.a, case.b, head_oid)]:
                is_lake = label == 'lake'
                begin(first)
                begin(second)
                check(label + ' resource-case starts at10',
                      (scalar(first, 'SELECT val FROM ' + table + ' WHERE id=3'),
                       scalar(second, 'SELECT val FROM ' + table + ' WHERE id=3')), (10, 10))
                if is_lake:
                    query(first, 'INSERT INTO biz.heap_marker VALUES(101)')
                    query(second, 'INSERT INTO biz.heap_marker VALUES(202)')
                if args.mode == 'savepoint-release':
                    query(first, 'SAVEPOINT holder_child')
                    if is_lake:
                        query(first, 'INSERT INTO biz.heap_marker VALUES(102)')
                check(label + ' holder stages11', execute_action(first, table, 'increment'), [(TARGET, 11)])
                worker = async_update(second, table, label + '-resource-waiter')
                wait_on_holder(worker, scalar(first, 'SELECT pg_backend_pid()'), lock_relation, label + ' resource wait')
                if args.mode == 'wait-cancel':
                    cancel_owned_worker(worker)
                    reply = worker['future'].result(timeout=15)
                    check(label + ' cancelled UPDATE has real query-cancelled SQLSTATE',
                          (reply['state'], reply.get('sqlstate')), ('error', '57014'))
                    query(second, 'ROLLBACK')
                    query(first, 'COMMIT')
                    if is_lake:
                        committed_model = dict(baseline)
                        committed_model[TARGET] = 11
                        committed_markers.add(101)
                    begin(second)
                    check(label + ' cancelled backend can start fresh and see11',
                          scalar(second, 'SELECT val FROM ' + table + ' WHERE id=3'), 11)
                    if is_lake:
                        query(second, 'INSERT INTO biz.heap_marker VALUES(203)')
                    check(label + ' fresh UPDATE after cancellation reaches12',
                          execute_action(second, table, 'increment'), [(TARGET, 12)])
                    query(second, 'COMMIT')
                    if is_lake:
                        committed_model[TARGET] = 12
                        committed_markers.add(203)
                else:
                    query(first, 'ROLLBACK TO holder_child')
                    query(first, 'RELEASE SAVEPOINT holder_child')
                    reply = worker['future'].result(timeout=15)
                    check(label + ' waiter wakes after child rollback before parent ends', reply['state'], 'completed')
                    check(label + ' rolled-back holder value is not reused', reply['rows'], [(TARGET, 11)])
                    query(second, 'COMMIT')
                    if is_lake:
                        committed_model = dict(baseline)
                        committed_model[TARGET] = 11
                        committed_markers.add(202)
                    check(label + ' live parent sees newly committed waiter value',
                          scalar(first, 'SELECT val FROM ' + table + ' WHERE id=3'), 11)
                    check(label + ' parent reacquires after child rollback and reaches12',
                          execute_action(first, table, 'increment'), [(TARGET, 12)])
                    query(first, 'COMMIT')
                    if is_lake:
                        committed_model[TARGET] = 12
                        committed_markers.add(101)
                result.setdefault('resource_waiter_replies', []).append(dict(label=label, mode=args.mode, reply=reply))
                worker['pool'].shutdown(wait=True)
        else:
            # Separate heap transactions prove actual lock waiting and the
            # same commit/rollback arithmetic before exercising the lake.
            action_a, action_b = {'where-false': ('set20', 'where10'), 'delete-update': ('delete', 'increment'),
                                  'update-delete': ('increment', 'delete'), 'delete-delete': ('delete', 'delete')}.get(
                                      args.mode, ('increment', 'increment'))
            after_a, returning_a = model_action(committed_model, action_a)
            after_holder = after_a if args.first_outcome == 'commit' else dict(committed_model)
            after_b, returning_b = model_action(after_holder, action_b)
            result['actions'] = dict(first=action_a, second=action_b, first_returning=returning_a,
                                     second_returning=returning_b, final_model=sorted(after_b.items()))
            ha, hb = db.connect(), db.connect()
            configure(ha)
            configure(hb)
            begin(ha)
            begin(hb)
            check('heap sessions initially see10', (scalar(ha, 'SELECT val FROM biz.reference WHERE id=3'),
                  scalar(hb, 'SELECT val FROM biz.reference WHERE id=3')), (10, 10))
            check('heap first action exact RETURNING', execute_action(ha, 'biz.reference', action_a), returning_a)
            heap_pid = scalar(ha, 'SELECT pg_backend_pid()')
            heap_oid = scalar(case.observer, "SELECT 'biz.reference'::regclass::oid")
            heap_worker = async_update(hb, 'biz.reference', 'ordinary-heap', action_b)
            wait_on_holder(heap_worker, heap_pid, heap_oid, 'ordinary heap second UPDATE')
            query(ha, 'COMMIT' if args.first_outcome == 'commit' else 'ROLLBACK')
            heap_reply = heap_worker['future'].result(timeout=15)
            check('heap waiter resumes successfully', heap_reply['state'], 'completed')
            check('heap waiter uses latest row/qualification after holder outcome', heap_reply['rows'], returning_b)
            query(hb, 'COMMIT')
            heap_worker['pool'].shutdown(wait=True)
            begin(case.a)
            begin(case.b)
            check('lake sessions initially see same committed10', (case.rows(case.a), case.rows(case.b)),
                  (sorted(committed_model.items()), sorted(committed_model.items())))
            query(case.a, 'INSERT INTO biz.heap_marker VALUES(101)')
            query(case.b, 'INSERT INTO biz.heap_marker VALUES(202)')
            first_pid = scalar(case.a, 'SELECT pg_backend_pid()')
            check('lake first action exact RETURNING', execute_action(case.a, 'biz.orders', action_a), returning_a)
            result['holder_visible_heads'] = head_rows(case.a)
            check('holder uses exactly one target row head', sum(row[0] == logical_id for row in head_rows(case.a)), 1)
            worker = async_update(case.b, 'biz.orders', 'lake-waiter', action_b)
            wait_on_holder(worker, first_pid, head_oid, 'lake second UPDATE')
            if args.mode == 'flush-during-wait':
                # Neither writer has published its journal. The independent
                # flusher therefore transfers the committed original prefix.
                # UPDATE must later resolve the same stable ID in its new file.
                query(case.observer, "SELECT og_lake.flush('biz.orders'::regclass)")
                observe_flushed_origin()
                wait_on_holder(worker, first_pid, head_oid, 'lake waiter remains on its logical head after source relocation')
                check('source relocation keeps the exact mapped row-head OID', scalar(case.observer,
                      'SELECT row_head_oid FROM og_lake_internal.row_lock_binding WHERE binding_id=%s::uuid', (binding_id,)), head_oid)
                event('committed source flushed while the holder and waiter transactions are still active',
                      file_id=original_file, position=original_position, logical_id=logical_id)
            query(case.a, 'COMMIT' if args.first_outcome == 'commit' else 'ROLLBACK')
            if args.first_outcome == 'commit':
                committed_model = dict(after_a)
                committed_markers.add(101)
            event('first lake transaction ended', outcome=args.first_outcome)
            reply = worker['future'].result(timeout=15)
            result['waiter_reply'] = reply
            check('lake waiter resumes successfully', reply['state'], 'completed')
            check('lake EPQ uses latest row/qualification after holder outcome', reply['rows'], returning_b)
            check('next waiter statement overlays its own result exactly once', case.rows(case.b), sorted(after_b.items()))
            query(case.b, 'COMMIT')
            committed_model = dict(after_b)
            committed_markers.add(202)
            worker['pool'].shutdown(wait=True)
        check('complete committed lake values equal explicit model', case.rows(case.observer), sorted(committed_model.items()))
        check('complete ordinary heap reference matches committed model', query(case.observer,
              'SELECT id,val FROM biz.reference ORDER BY id'), sorted(committed_model.items()))
        live = [row for row in query(case.observer, raw_sql) if row[3] == TARGET and row[2] is None]
        check('live target existence matches UPDATE/DELETE outcome', len(live), 1 if TARGET in committed_model else 0)
        if live:
            check('live target retains stable logical identity', (live[0][0], live[0][4]), (logical_id, committed_model[TARGET]))
        check('prototype retains one target head through this tested timeline', sum(row[0] == logical_id for row in head_rows()), 1)
        check('head binding identity survives the whole transaction flow', scalar(case.observer,
              'SELECT row_head_oid FROM og_lake_internal.row_lock_binding WHERE binding_id=%s::uuid', (binding_id,)), head_oid)
        check('row allocation frontier unchanged by UPDATE/head creation', scalar(case.observer,
              'SELECT next_row_id FROM created_metadata.ducklake_table_stats WHERE table_id=%s', (table_id,)), initial_next)
        check('ordinary heap markers follow lake commit outcomes', query(case.observer,
              'SELECT id FROM biz.heap_marker ORDER BY id'), [(value,) for value in sorted(committed_markers)])
        if source is not None:
            deleted = marks()
            check('one original file-position marker survives both updates', len(deleted), 1)
            check('delete marker uses original(F,P), not row_id', deleted[0][:2], (original_file, original_position))
            check('flushed source bytes remain immutable', sha256(source), source_hash)
        check('all frozen inputs stayed unchanged', all(sha256(Path(p)) == h for p, h in result['inputs'].items()))
        check('installed provider stayed identical', sha256(provider), result['provider_sha256'])
        check_build(args.build_receipt)
        result['status'] = 'pass'
    except BaseException as exc:
        result.update(reason=str(exc), traceback=traceback.format_exc())
    finally:
        errors = []
        busy = {id(w['conn']) for w in workers if not w['future'].done()}
        for conn in transactions:
            if id(conn) in busy or conn.closed:
                continue
            try:
                query(conn, 'ROLLBACK')
            except BaseException as exc:
                errors.append('holder rollback: ' + str(exc))
        for worker in workers:
            future = worker['future']
            try:
                if not future.done():
                    identity = query(case.observer, 'SELECT datid,application_name FROM pg_stat_activity WHERE pid=%s', (worker['pid'],))
                    if identity != [(db.allocation['database_oid'], worker['app'])]:
                        raise RuntimeError('Refuse cancellation of changed worker identity')
                    scalar(case.observer, 'SELECT pg_cancel_backend(%s)', (worker['pid'],))
                try:
                    reply = future.result(timeout=10)
                except FutureTimeout:
                    identity = query(case.observer, 'SELECT datid,application_name FROM pg_stat_activity WHERE pid=%s', (worker['pid'],))
                    if identity != [(db.allocation['database_oid'], worker['app'])]:
                        raise RuntimeError('Refuse termination of changed worker identity')
                    scalar(case.observer, 'SELECT pg_terminate_backend(%s)', (worker['pid'],))
                    reply = future.result(timeout=10)
                result.setdefault('worker_cleanup', []).append(dict(label=worker['label'], pid=worker['pid'], reply=reply))
                if not worker['conn'].closed:
                    query(worker['conn'], 'ROLLBACK')
            except BaseException as exc:
                errors.append('worker cleanup: ' + str(exc))
            finally:
                worker['pool'].shutdown(wait=future.done(), cancel_futures=True)
        if case is not None and committed_model is not None:
            try:
                result['after_cleanup_rows'] = case.rows(case.observer)
                check('cleanup retains exactly confirmed lake commits', result['after_cleanup_rows'], sorted(committed_model.items()))
                check('cleanup retains only confirmed heap markers', query(case.observer,
                      'SELECT id FROM biz.heap_marker ORDER BY id'), [(v,) for v in sorted(committed_markers)])
            except BaseException as exc:
                errors.append('post-rollback observation: ' + str(exc))
        if errors:
            result.update(status='fail', transaction_cleanup_errors=errors)
        if db is not None:
            try:
                db.cleanup()
                result['database_cleanup'] = db.cleanup_result
            except BaseException as exc:
                result.update(status='fail', cleanup_error=str(exc))
        if roles is not None:
            try:
                if not result.get('database_cleanup') or result['database_cleanup']['returncode']:
                    raise RuntimeError('Owned database cleanup unproven; roles retained')
                result['role_cleanup'] = cleanup_roles(control, roles.path, args.run_id)
                result = roles.redact(result)
            except BaseException as exc:
                result.update(status='fail', role_cleanup_error=str(exc))
        if control is not None:
            control.close()
        result['artifacts'] = {str(p): sha256(p) for p in out.rglob('*')
                               if p.is_file() and p.suffix in ['.parquet', '.sql', '.stdout', '.stderr']}
        write_json(out / 'result.json', result)
    print(json.dumps(dict(status=result['status'], mode=args.mode, first_outcome=args.first_outcome,
                          assertions=len(result['assertions']), reason=result.get('reason'), result=str(out / 'result.json')), ensure_ascii=False))
    return 0 if result['status'] == 'pass' else 1


if __name__ == '__main__':
    raise SystemExit(main())
