#!/usr/bin/env python3
"""First concurrent UPDATE of an already flushed logical row, with RC oracle.

current-journal records the existing optimistic publication contract: both
UPDATEs return11, one COMMIT fails40001, and only one replacement survives.
ordinary-rc requires a real target UID wait and native latest-version EPQ:
the second UPDATE returns12 after the first commits. Current a038 should fail
this second contract; that negative is retained, never promoted to a fix.
Fresh real DuckLake fixture, installed SDK, single-instance RC Debug only.
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
TARGET_ID = 3
sys.path[:0] = [str(D), str(D / 'tests')]
from file_dml_acceptance import check_build
from inline_fixture import query, scalar
from lake_harness import sha256, validate_run_id, write_json
from native_auto_flush import connect_control
from native_runtime import INSTALL, PROFILE
from native_table_create import Case
from native_test_context import OwnedDatabase
from permission_api import OwnedRoles, cleanup_roles


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--build-receipt', type=Path, required=True)
    parser.add_argument('--mode', choices=['current-journal', 'ordinary-rc'], required=True)
    args = parser.parse_args()
    validate_run_id(args.run_id)
    if PROFILE != 'debug':
        parser.error('This bounded concurrency case requires the owned main Debug runtime')
    out = ROOT / '公共文档/openGauss原生湖存储/验证' / args.run_id
    out.mkdir(exist_ok=False)
    result = dict(status='fail', scope=__doc__, mode=args.mode, assertions=[], events=[],
                  contract_expectation=(dict(second_update_waits=True, second_returning=[(TARGET_ID, 12)], commits=2)
                    if args.mode == 'ordinary-rc' else
                    dict(second_update_waits=False, both_returning=[(TARGET_ID, 11)], loser_commit_sqlstate='40001')))
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
    control = db = roles = case = None
    workers, holders, transaction_connections = [], [], []
    committed_value = 10
    provider = INSTALL / 'lib/postgresql/og_lake_sdk/libog_lake_sdk_arrow.so'
    duckdb = ROOT / '.reference/opengauss-native-lake/build/duckdb-gcc10/duckdb'

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
        query(conn, 'SET statement_timeout=20000')

    def begin(conn):
        query(conn, 'BEGIN ISOLATION LEVEL READ COMMITTED')
        transaction_connections.append(conn)

    def lock_rows(observer, pid, waiting=False):
        return query(observer, 'SELECT pid,locktype,database,relation,transactionid::text,mode,granted '
                     'FROM pg_locks WHERE pid=%s' + (' AND NOT granted' if waiting else '')
                     + ' ORDER BY locktype,relation,transactionid,mode,granted', (pid,))

    def async_update(conn, table, label):
        pid = scalar(conn, 'SELECT pg_backend_pid()')
        app = scalar(conn, "SELECT current_setting('application_name')")
        def execute():
            try:
                rows = query(conn, 'UPDATE ' + table + ' SET val=val+1 WHERE id=%s RETURNING id,val', (TARGET_ID,))
                return dict(state='updated', rows=rows, finished_at=datetime.datetime.now(datetime.timezone.utc).isoformat())
            except Exception as exc:
                return dict(state='error', sqlstate=getattr(exc, 'pgcode', None), message=str(exc))
        pool = ThreadPoolExecutor(max_workers=1)
        future = pool.submit(execute)
        item = dict(label=label, conn=conn, pid=pid, app=app, pool=pool, future=future)
        workers.append(item)
        return item

    def observe_wait(worker, holder, observer, locktype):
        deadline = time.monotonic() + 8
        while True:
            waiting = [row for row in lock_rows(observer, worker['pid'], True) if row[1] == locktype]
            if waiting and not worker['future'].done():
                return dict(state='waiting', waiting=waiting, holder=lock_rows(observer, holder), waiter_pid=worker['pid'])
            if worker['future'].done():
                return dict(state='returned_before_first_commit', reply=worker['future'].result(),
                            waiting=waiting, holder=lock_rows(observer, holder), waiter_pid=worker['pid'])
            if time.monotonic() >= deadline:
                return dict(state='no_matching_lock_wait_observed', waiting=lock_rows(observer, worker['pid'], True),
                            holder=lock_rows(observer, holder), waiter_pid=worker['pid'])
            time.sleep(.02)  # Poll real lock evidence, never infer a wait from elapsed time.

    try:
        from psycopg2 import sql
        import psycopg2
        built = check_build(args.build_receipt)
        result.update(source_head=built['source_head'], binary_sha256=built['binary_sha256'],
                      build=str(args.build_receipt.resolve()), build_sha256=sha256(args.build_receipt),
                      provider_sha256=sha256(provider), reference_binary_sha256=sha256(duckdb))
        control = connect_control()
        configure(control)
        check('automatic scheduler stays off', scalar(control, 'SHOW enable_lake_maintenance'), 'off')
        check('PID lock oracle uses dedicated backend mode', scalar(control, 'SHOW enable_thread_pool'), 'off')
        with (out / 'fixture.log').open('w') as log:
            subprocess.run([sys.executable, str(D / 'tests/catalog_fixture_bundle.py'), '--run-id', args.run_id,
                            '--output', str(out / 'fixture/manifest.json')], stdout=log,
                           stderr=subprocess.STDOUT, check=True, timeout=300)
        db = OwnedDatabase(args.run_id, out).create()
        configure(db.admin)
        roles = OwnedRoles(control, args.run_id, out)
        case = Case(db, SimpleNamespace(mode='net_dml', sql_version='0.16', manifest=out / 'fixture/manifest.json',
                    output=out / 'result.json', run_id=args.run_id), result, roles)
        for conn in [case.a, case.b, case.observer]:
            configure(conn)
        query(case.a, 'SET enable_default_ustore_table=off')
        case.create()
        query(case.a, 'INSERT INTO biz.orders VALUES(1,100),(2,200),(3,10)')
        query(case.a, 'UPDATE biz.orders SET val=val+1 WHERE id=1')
        table_id = scalar(case.observer, "SELECT table_id FROM created_metadata.ducklake_table WHERE table_name='orders' AND end_snapshot IS NULL")
        carrier = scalar(case.observer, 'SELECT table_name FROM created_metadata.ducklake_inlined_data_tables WHERE table_id=%s', (table_id,))
        raw_sql = sql.SQL('SELECT _ducklake_row_id,_ducklake_begin_snapshot,_ducklake_end_snapshot,id,val '
                         'FROM created_metadata.{} ORDER BY _ducklake_row_id,_ducklake_begin_snapshot').format(sql.Identifier(carrier))
        before = query(case.observer, raw_sql)
        target_before = [row for row in before if row[3] == TARGET_ID]
        check('one target committed inline origin before first flush', len(target_before), 1)
        check('non-target update creates extra retained physical history', len(before) > 3)
        other_history = [row for row in before if row[3] == 1]
        check('non-target has one ended and one live inline version',
              (len(other_history), sum(row[2] is None for row in other_history)), (2, 1))
        row_id, birth, ended, business_id, initial = target_before[0]
        check('target origin starts live with business value10', (ended, business_id, initial), (None, TARGET_ID, 10))
        result['preflush_inline_versions'] = before
        initial_next = scalar(case.observer, 'SELECT next_row_id FROM created_metadata.ducklake_table_stats WHERE table_id=%s', (table_id,))
        query(case.a, "SELECT og_lake.flush('biz.orders'::regclass)")
        check('flush removes target and every physical inline/delta version', query(case.observer, raw_sql), [])
        check('flush preserves row allocation frontier', scalar(case.observer,
              'SELECT next_row_id FROM created_metadata.ducklake_table_stats WHERE table_id=%s', (table_id,)), initial_next)
        delete_name = 'ducklake_inlined_delete_' + str(table_id)
        delete_sql = sql.SQL('SELECT file_id,row_id,begin_snapshot FROM created_metadata.{} ORDER BY 1,2,3').format(sql.Identifier(delete_name))
        def marks():
            exists = scalar(case.observer, 'SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace '
                            "WHERE n.nspname='created_metadata' AND c.relname=%s AND c.relkind='r'", (delete_name,))
            return query(case.observer, delete_sql) if exists else []
        check('fresh file has no inline file-position delete markers', marks(), [])
        initial_remote_deletes = query(case.observer, 'SELECT delete_file_id,data_file_id,begin_snapshot,path,delete_count '
                                       'FROM created_metadata.ducklake_delete_file WHERE table_id=%s ORDER BY delete_file_id', (table_id,))
        result['initial_remote_delete_history'] = initial_remote_deletes
        file_sql = ('SELECT data_file_id,path,record_count,begin_snapshot,end_snapshot FROM created_metadata.ducklake_data_file '
                    'WHERE table_id=%s AND end_snapshot IS NULL ORDER BY data_file_id')
        data_files = query(case.observer, file_sql, (table_id,))
        check('first flush has exactly one live physical file', len(data_files), 1)
        file_id, stored_path, physical_count, file_birth, _ = data_files[0]
        check('data file retains the complete original physical prefix', physical_count, len(before))
        data_root = Path(case.empty_manifest['data_path'])
        matches = [path for path in data_root.rglob('*.parquet') if path.name == Path(stored_path).name]
        check('catalog file resolves to one owned real Parquet object', len(matches), 1)
        source = matches[0]
        source_hash = sha256(source)
        statement = ('SELECT file_row_number AS position,_ducklake_internal_row_id AS logical_id,'
                     '_ducklake_internal_snapshot_id AS birth,id,val FROM read_parquet('
                     + "'" + str(source).replace("'", "''") + "',file_row_number=true) ORDER BY file_row_number")
        (out / 'physical-origin.sql').write_text(statement + '\n')
        proc = subprocess.run([str(duckdb), '-no-init', '-batch', '-json', '-c', 'SET threads=1;' + statement],
                              capture_output=True, text=True, timeout=60)
        (out / 'physical-origin.stdout').write_text(proc.stdout)
        (out / 'physical-origin.stderr').write_text(proc.stderr)
        check('independent real Parquet read succeeds', proc.returncode, 0)
        physical = json.loads(proc.stdout)
        check('real Parquet preserves all original identities, values and order', physical,
              [dict(position=i, logical_id=row[0], birth=row[1], id=row[3], val=row[4]) for i, row in enumerate(before)])
        target_physical = [row for row in physical if row['id'] == TARGET_ID]
        check('one physical target version exists in real Parquet', len(target_physical), 1)
        target = target_physical[0]
        check('flush retains target stable logical ID/birth and value10',
              (target['logical_id'], target['birth'], target['id'], target['val']), (row_id, birth, TARGET_ID, 10))
        position = target['position']
        check('test distinguishes logical row ID from file position', row_id != position)
        result['origin'] = dict(table_id=table_id, logical_row_id=row_id, birth_snapshot=birth,
                                file_id=file_id, physical_position=position, file_path=str(source),
                                source_sha256=source_hash, initial_next_row_id=initial_next)

        # Ordinary heap is a separate semantic reference, not a lake patch.
        query(case.a, 'CREATE TABLE biz.heap_reference(id integer PRIMARY KEY,val integer)')
        query(case.a, 'INSERT INTO biz.heap_reference VALUES(1,101),(2,200),(3,10)')
        heap_options = scalar(case.observer, "SELECT reloptions FROM pg_class WHERE oid='biz.heap_reference'::regclass")
        result['heap_reference_reloptions'] = heap_options
        check('semantic reference is actual ordinary heap storage', 'storage_type=ustore' not in (heap_options or []))
        ha, hb = db.connect(), db.connect()
        configure(ha)
        configure(hb)
        begin(ha)
        begin(hb)
        holders.append(ha)
        check('heap reference sessions both initially read10',
              (scalar(ha, 'SELECT val FROM biz.heap_reference WHERE id=3'), scalar(hb, 'SELECT val FROM biz.heap_reference WHERE id=3')), (10, 10))
        check('heap first increment RETURNING11', query(ha, 'UPDATE biz.heap_reference SET val=val+1 WHERE id=3 RETURNING id,val'), [(TARGET_ID, 11)])
        heap_pid = scalar(ha, 'SELECT pg_backend_pid()')
        heap_worker = async_update(hb, 'biz.heap_reference', 'heap-reference')
        heap_phase = observe_wait(heap_worker, heap_pid, case.observer, 'transactionid')
        result['heap_reference_wait'] = heap_phase
        check('ordinary heap second UPDATE really waits in pg_locks', heap_phase['state'], 'waiting')
        heap_xids = {row[4] for row in heap_phase['waiting']}
        check('heap wait names a transaction held by first updater',
              any(row[1] == 'transactionid' and row[4] in heap_xids and row[6] for row in heap_phase['holder']))
        query(ha, 'COMMIT')
        heap_reply = heap_worker['future'].result(timeout=12)
        result['heap_reference_reply'] = heap_reply
        check('ordinary heap waiter resumes successfully', heap_reply['state'], 'updated')
        check('ordinary heap re-evaluates increment on latest value', heap_reply['rows'], [(TARGET_ID, 12)])
        query(hb, 'COMMIT')
        heap_worker['pool'].shutdown(wait=True)
        check('ordinary heap commits both increments', query(case.observer, 'SELECT id,val FROM biz.heap_reference ORDER BY id'),
              [(1, 101), (2, 200), (TARGET_ID, 12)])

        begin(case.a)
        begin(case.b)
        holders.append(case.a)
        check('both lake transactions initially read the same flushed value10',
              (case.rows(case.a), case.rows(case.b)),
              ([(1, 101), (2, 200), (TARGET_ID, 10)], [(1, 101), (2, 200), (TARGET_ID, 10)]))
        check('both sessions identify the same sole live file version',
              (query(case.a, file_sql, (table_id,)), query(case.b, file_sql, (table_id,))), (data_files, data_files))
        check('origin is still file-only before concurrent updates', query(case.observer, raw_sql), [])
        check('origin still unmarked before concurrent updates', marks(), [])
        result['session_origins'] = [dict(session=name, logical_row_id=row_id, birth_snapshot=birth,
                                         file_id=file_id, physical_position=position, initial_target=(TARGET_ID, 10),
                                         proof='sole live file + empty inline/deletes + independently decoded unique Parquet origin')
                                     for name in ['A', 'B']]
        first_pid = scalar(case.a, 'SELECT pg_backend_pid()')
        db_oid = scalar(case.observer, 'SELECT oid FROM pg_database WHERE datname=current_database()')
        relation_oid = scalar(case.observer, "SELECT 'biz.orders'::regclass::oid")
        query(case.a, 'INSERT INTO biz.heap_marker VALUES(101)')
        query(case.b, 'INSERT INTO biz.heap_marker VALUES(202)')
        first = query(case.a, 'UPDATE biz.orders SET val=val+1 WHERE id=%s RETURNING id,val', (TARGET_ID,))
        event('lake-A-UPDATE-returned', rows=first)
        check('lake first UPDATE RETURNING11 from original10', first, [(TARGET_ID, 11)])
        worker = async_update(case.b, 'biz.orders', 'lake-second-update')
        if args.mode == 'ordinary-rc':
            phase = observe_wait(worker, first_pid, case.observer, 'tuple_uid')
            result['ordinary_rc_observation'] = phase
            if 'reply' in phase:
                result['second_update_reply'] = phase['reply']
                result['ordinary_rc_difference'] = dict(expected_state='waiting', observed_state=phase['state'],
                    expected_second_returning=[(TARGET_ID, 12)], observed_second_returning=phase['reply'].get('rows'))
            check('ordinary RC lake second UPDATE must wait before first COMMIT', phase['state'], 'waiting')
            key_matches = lambda row: row[1] == 'tuple_uid' and row[2] == db_oid and row[3] == relation_oid and row[4] == str(row_id) and row[5] == 'ExclusiveLock'
            check('waiter blocks on exact stable logical UID key', any(key_matches(row) and not row[6] for row in phase['waiting']))
            check('first updater holds that same UID key', any(key_matches(row) and row[6] for row in phase['holder']))
            query(case.a, 'COMMIT')
            committed_value = 11
            event('lake-A-COMMIT-confirmed')
            reply = worker['future'].result(timeout=12)
            result['second_update_reply'] = reply
            check('ordinary RC waiter resumes with a successful UPDATE', reply['state'], 'updated')
            check('ordinary RC EPQ recomputes val+1 on latest11', reply['rows'], [(TARGET_ID, 12)])
            query(case.b, 'COMMIT')
            committed_value = 12
            event('lake-B-COMMIT-confirmed')
            result['publication_outcome'] = 'both_committed'
        else:
            reply = worker['future'].result(timeout=8)
            result['second_update_reply'] = reply
            event('lake-B-UPDATE-returned-before-commits', reply=reply)
            check('current journal second UPDATE also returns before COMMIT', reply['state'], 'updated')
            check('current journal second UPDATE still computes from10', reply['rows'], [(TARGET_ID, 11)])
            check('both UPDATE transactions remain uncommitted at RETURNING',
                  (case.a.get_transaction_status(), case.b.get_transaction_status()), (2, 2))
            result['precommit_locks'] = dict(holder=lock_rows(case.observer, first_pid), waiter=lock_rows(case.observer, worker['pid']))
            query(case.a, 'COMMIT')
            committed_value = 11
            event('lake-A-COMMIT-confirmed')
            outcome = 'committed'
            try:
                query(case.b, 'COMMIT')
            except psycopg2.Error as exc:
                outcome = exc.pgcode
                result['publication_error'] = str(exc)
                query(case.b, 'ROLLBACK')
            result['publication_outcome'] = outcome
            check('current journal loser COMMIT rejects overlapping file origin', outcome, '40001')
        worker['pool'].shutdown(wait=True)
        check('committed business rows have no duplicate, missing row or lost increment', case.rows(),
              [(1, 101), (2, 200), (TARGET_ID, committed_value)])
        current = query(case.observer, raw_sql)
        live = [row for row in current if row[2] is None]
        check('exactly one current inline/delta replacement survives', len(live), 1)
        check('live replacement keeps the original logical row ID', (live[0][0], live[0][3], live[0][4]), (row_id, TARGET_ID, committed_value))
        check('UPDATE never allocates a new logical identity', scalar(case.observer,
              'SELECT next_row_id FROM created_metadata.ducklake_table_stats WHERE table_id=%s', (table_id,)), initial_next)
        deleted = marks()
        check('only one exact original file-position delete marker exists', len(deleted), 1)
        check('marker refers to original(F,P), not logical row ID', deleted[0][:2], (file_id, position))
        check('new delete marker position cannot equal target logical row ID', deleted[0][1] != row_id)
        check('original remote delete history is preserved independently', query(case.observer,
              'SELECT delete_file_id,data_file_id,begin_snapshot,path,delete_count FROM created_metadata.ducklake_delete_file '
              'WHERE table_id=%s ORDER BY delete_file_id', (table_id,)), initial_remote_deletes)
        check('heap marker commits atomically with corresponding lake transaction', query(case.observer,
              'SELECT id FROM biz.heap_marker ORDER BY id'), [(101,)] if args.mode == 'current-journal' else [(101,), (202,)])
        if args.mode == 'current-journal':
            check('failed second publish leaves no ended/intermediate delta row', len(current), 1)
        check('original Parquet bytes and logical IDs never change', sha256(source), source_hash)
        check('all executed input bytes unchanged', all(sha256(Path(path)) == digest for path, digest in result['inputs'].items()))
        check('provider identity unchanged', sha256(provider), result['provider_sha256'])
        check_build(args.build_receipt)
        result['status'] = 'pass'
    except BaseException as exc:
        result.update(reason=str(exc), traceback=traceback.format_exc())
    finally:
        cleanup_errors = []
        # Release holders first. Never issue another command on a connection
        # still executing in the worker thread.
        for conn in holders:
            try:
                if not conn.closed:
                    query(conn, 'ROLLBACK')
            except BaseException as exc:
                cleanup_errors.append('holder rollback: ' + str(exc))
        for item in workers:
            future = item['future']
            try:
                if not future.done() and case is not None:
                    owned = query(case.observer, 'SELECT datid,application_name FROM pg_stat_activity WHERE pid=%s', (item['pid'],))
                    if owned != [(db.allocation['database_oid'], item['app'])]:
                        raise RuntimeError('Refuse cancellation: worker PID/DB/application identity changed')
                    scalar(case.observer, 'SELECT pg_cancel_backend(%s)', (item['pid'],))
                try:
                    reply = future.result(timeout=10)
                except FutureTimeout:
                    owned = query(case.observer, 'SELECT datid,application_name FROM pg_stat_activity WHERE pid=%s', (item['pid'],))
                    if owned != [(db.allocation['database_oid'], item['app'])]:
                        raise RuntimeError('Refuse termination: worker identity changed')
                    scalar(case.observer, 'SELECT pg_terminate_backend(%s)', (item['pid'],))
                    reply = future.result(timeout=10)
                result.setdefault('worker_cleanup', []).append(dict(label=item['label'], pid=item['pid'], reply=reply))
                if not item['conn'].closed:
                    query(item['conn'], 'ROLLBACK')
            except BaseException as exc:
                cleanup_errors.append('worker cleanup: ' + str(exc))
            finally:
                item['pool'].shutdown(wait=future.done(), cancel_futures=True)
        still_busy = {id(item['conn']) for item in workers if not item['future'].done()}
        for conn in transaction_connections:
            try:
                if id(conn) not in still_busy and not conn.closed:
                    query(conn, 'ROLLBACK')
            except BaseException as exc:
                cleanup_errors.append('transaction rollback: ' + str(exc))
        if case is not None:
            try:
                result['after_transaction_cleanup_rows'] = case.rows()
                check('transaction cleanup preserves only confirmed lake commits', result['after_transaction_cleanup_rows'],
                      [(1, 101), (2, 200), (TARGET_ID, committed_value)])
                result['after_transaction_cleanup_heap_markers'] = query(case.observer, 'SELECT id FROM biz.heap_marker ORDER BY id')
                check('transaction cleanup preserves only confirmed heap markers', result['after_transaction_cleanup_heap_markers'],
                      [] if committed_value == 10 else [(101,)] if committed_value == 11 else [(101,), (202,)])
                if 'raw_sql' in locals() and 'initial_next' in locals():
                    result['after_transaction_cleanup_delta'] = query(case.observer, raw_sql)
                    result['after_transaction_cleanup_deletes'] = marks() if 'marks' in locals() else None
                    check('transaction cleanup never reallocates row IDs', scalar(case.observer,
                          'SELECT next_row_id FROM created_metadata.ducklake_table_stats WHERE table_id=%s', (table_id,)), initial_next)
                    if committed_value == 10:
                        check('negative contract rollback restores empty delta', result['after_transaction_cleanup_delta'], [])
                        check('negative contract rollback restores unmarked file', result['after_transaction_cleanup_deletes'], [])
            except BaseException as exc:
                cleanup_errors.append('post-rollback observation: ' + str(exc))
        try:
            check('all frozen inputs stay identical after cleanup', all(sha256(Path(path)) == digest for path, digest in result['inputs'].items()))
            check('actual official binary identity survives the scenario', check_build(args.build_receipt)['binary_sha256'],
                  result.get('binary_sha256'))
            check('full scenario leaves the same installed provider', sha256(provider), result.get('provider_sha256'))
        except BaseException as exc:
            cleanup_errors.append('post-scenario identity: ' + str(exc))
        if cleanup_errors:
            result.update(status='fail', transaction_cleanup_errors=cleanup_errors)
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
        result['artifacts'] = {str(path): sha256(path) for path in out.rglob('*')
                               if path.is_file() and path.suffix in ['.parquet', '.sql', '.stdout', '.stderr']}
        write_json(out / 'result.json', result)
    print(json.dumps(dict(status=result['status'], mode=args.mode, checks=len(result['assertions']),
                          outcome=result.get('publication_outcome'), reason=result.get('reason'),
                          result=str(out / 'result.json')), ensure_ascii=False))
    return 0 if result['status'] == 'pass' else 1


if __name__ == '__main__':
    raise SystemExit(main())
