#!/usr/bin/env python3
"""Crash the exclusively owned native instance and verify flush WAL atomicity.

Only this project's instance is eligible. This is a local crash/recovery test,
not a power-loss, orphan-GC, remote-storage or multi-instance qualification.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import time
import traceback
from types import SimpleNamespace

ROOT = Path('/home/qiumc/data/code/cpp')
DRIVER = ROOT / 'integration/opengauss-native-lake'
RUNTIME = ROOT / '.runtime/opengauss-native-lake/debug'
sys.path[:0] = [str(DRIVER), str(DRIVER / 'tests')]
from file_dml_acceptance import check_build
from inline_fixture import query, scalar
from lake_harness import sha256, validate_run_id, write_json
from native_table_create import Case
from native_test_context import OwnedDatabase
from permission_api import OwnedRoles, cleanup_roles
from empty_catalog_oracle import read_native_catalog, transport_native
from reverse_reference import Reference


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--build-receipt', type=Path, required=True)
    parser.add_argument('--mode', choices=['prepared', 'publishing', 'committed'], required=True)
    args = parser.parse_args()
    validate_run_id(args.run_id)
    out = ROOT / '公共文档/openGauss原生湖存储/验证' / args.run_id
    out.mkdir(exist_ok=False)
    result = {'status': 'fail', 'scope': 'Actual native flush process crash: ' + args.mode,
              'assertions': [], 'limitations': [
                  'Unregistered candidates remain after a process crash; durable orphan GC is not implemented.',
                  'Local SIGKILL and WAL recovery only; no machine power-loss, private SDK, remote or RAC qualification.']}
    inputs = [Path(__file__), DRIVER / 'native_test_context.py',
              DRIVER / 'tests/native_table_create.py', DRIVER / 'tests/permission_api.py',
              DRIVER / 'tests/empty_catalog_oracle.py', DRIVER / 'tests/reverse_reference.py',
              DRIVER / 'inline_fixture.py', DRIVER / 'build_driver.py']
    result['input_hashes'] = {str(p): sha256(p) for p in inputs}
    archive = out / 'executed-inputs'
    archive.mkdir()
    for path in inputs:
        shutil.copy2(path, archive / path.name)
    db = roles = control = test = holder = executor = future = None
    crashed = False
    restarted = False
    cleanup_db = None
    restart_attempt = 0

    def check(name, observed, expected=True):
        ok = observed == expected
        result['assertions'].append({'name': name, 'observed': observed,
                                     'expected': expected, 'status': 'pass' if ok else 'fail'})
        if not ok:
            raise AssertionError(name)

    def connect_control():
        import psycopg2
        conn = psycopg2.connect(host=str(RUNTIME / 'socket'), port=55560,
                                dbname='postgres', user=os.getenv('USER', 'qiumc'),
                                application_name='og_lake_flush_crash_control', connect_timeout=5)
        conn.autocommit = True
        return conn

    def restart():
        nonlocal restart_attempt
        restart_attempt += 1
        restart_id = args.run_id + '_restart_' + str(restart_attempt)
        command = ['bash', str(DRIVER / 'build.sh'), 'debug', 'restart', '--run-id', restart_id]
        with (out / 'restart.log').open('w') as log:
            subprocess.run(command, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT,
                           check=True, timeout=180)
        receipt = out.parent / restart_id / 'build.json'
        restarted_build = check_build(receipt)
        result['restart'] = {'command': command, 'receipt': str(receipt), 'sha256': sha256(receipt)}
        check('recovery uses exact pre-crash native binary', restarted_build['binary_sha256'],
              result['binary_sha256'])
        check('new process performs crash recovery', restarted_build['running_pid'] != result['crashed_pid'])

    try:
        import psycopg2
        from psycopg2 import sql
        build = check_build(args.build_receipt)
        result.update(source_head=build['source_head'], binary_sha256=build['binary_sha256'],
                      build=str(args.build_receipt), build_sha256=sha256(args.build_receipt))
        provider = ROOT / '.install/opengauss-native-lake-debug/lib/postgresql/og_lake_sdk/libog_lake_sdk_arrow.so'
        result['provider'] = {'path': str(provider), 'sha256': sha256(provider)}
        with (out / 'fixture.log').open('w') as log:
            subprocess.run([sys.executable, str(DRIVER / 'tests/catalog_fixture_bundle.py'),
                            '--run-id', args.run_id, '--output', str(out / 'fixture/manifest.json')],
                           stdout=log, stderr=subprocess.STDOUT, check=True, timeout=300)
        control = connect_control()
        db = OwnedDatabase(args.run_id, out).create()
        roles = OwnedRoles(control, args.run_id, out)
        case_args = SimpleNamespace(mode='net_dml', sql_version='0.11',
                                   manifest=out / 'fixture/manifest.json',
                                   output=out / 'result.json', run_id=args.run_id)
        test = Case(db, case_args, result, roles)
        check('host file synchronization is enabled', scalar(test.a, 'SHOW fsync'), 'on')
        check('COMMIT waits for synchronous WAL completion', scalar(test.a, 'SHOW synchronous_commit'), 'on')
        test.create()
        query(test.a, 'INSERT INTO biz.orders VALUES(1,10),(2,20),(3,30)')
        initial_snapshot = test.head()[0]
        query(test.a, 'UPDATE biz.orders SET val=11 WHERE id=1')
        original = test.protocol()
        carrier = scalar(test.a, 'SELECT table_name FROM created_metadata.ducklake_inlined_data_tables')
        source = sql.SQL('created_metadata.{}').format(sql.Identifier(carrier))
        physical_sql = sql.SQL('SELECT * FROM {} ORDER BY _ducklake_row_id,_ducklake_begin_snapshot').format(source)
        physical_before = query(test.observer, physical_sql)
        check('source includes all four historical versions', len(physical_before), 4)
        root = Path(test.empty_manifest['data_path'])
        if args.mode == 'publishing':
            holder = db.connect()
            query(holder, 'BEGIN')
            query(holder, 'LOCK TABLE created_metadata.ducklake_snapshot_changes IN SHARE MODE')
        query(test.a, 'BEGIN')
        query(test.a, 'INSERT INTO biz.heap_marker VALUES(1)')
        publisher = scalar(test.a, 'SELECT pg_backend_pid()')
        query(test.a, "SELECT og_lake.flush('biz.orders'::regclass)")
        candidates = {str(p): sha256(p) for p in root.rglob('*.parquet')}
        check('both candidate files finished before crash', len(candidates), 2)
        if args.mode == 'committed':
            query(test.a, 'COMMIT')
            expected_protocol = test.protocol()
            check('COMMIT acknowledged complete physical transfer', query(test.observer, physical_sql), [])
            check('ordinary heap committed with lake metadata', query(test.observer, 'SELECT * FROM biz.heap_marker'), [(1,)])
        else:
            expected_protocol = original
        if args.mode == 'publishing':
            def commit():
                try:
                    query(test.a, 'COMMIT')
                    return 'accepted'
                except psycopg2.Error as exc:
                    return {'sqlstate': exc.pgcode, 'message': str(exc)}
            executor = ThreadPoolExecutor(max_workers=1)
            future = executor.submit(commit)
            deadline = time.monotonic() + 10
            waiting = []
            while time.monotonic() < deadline:
                waiting = query(test.observer, "SELECT pid,mode FROM pg_locks WHERE relation="
                                "'created_metadata.ducklake_snapshot_changes'::regclass AND pid=%s "
                                "AND mode='RowExclusiveLock' AND NOT granted", (publisher,))
                if waiting or future.done():
                    break
                time.sleep(.02)
            check('publisher reached final changeset barrier', len(waiting), 1)
            check('real heap DELETE already executed before crash',
                  scalar(test.observer, sql.SQL("SELECT count(*) FROM {} WHERE xmax::text<>'0'").format(source)), 4)
            check('outside reader sees complete committed catalog', test.protocol(), original)
        owner = json.loads((RUNTIME / 'owner.json').read_text())
        check('runtime belongs to native lake project',
              (owner['project'], owner['profile'], owner['data'], owner['port']),
              ('opengauss-native-lake', 'debug', str(RUNTIME / 'data'), 55560))
        check('SQL connection targets exact owned data directory',
              str(Path(scalar(control, 'SHOW data_directory')).resolve()), str(RUNTIME / 'data'))
        pid = int((RUNTIME / 'data/postmaster.pid').read_text().splitlines()[0])
        executable = Path('/proc') / str(pid) / 'exe'
        check('running process is exact owned installation', str(executable.resolve()),
              str(ROOT / '.install/opengauss-native-lake-debug/bin/gaussdb'))
        check('running executable has qualified hash', sha256(executable), build['binary_sha256'])
        command = (Path('/proc') / str(pid) / 'cmdline').read_bytes().split(b'\0')
        check('running command names exact owned data directory', os.fsencode(RUNTIME / 'data') in command)
        known_pids = {scalar(conn, 'SELECT pg_backend_pid()') for conn in
                      [control, db.admin, test.b, test.observer] + ([holder] if holder else [])}
        known_pids.add(publisher)
        # openGauss also exposes native maintenance threads as database users.
        # Frontend Unix sockets have client_port=-1, TCP has a real port;
        # internal engine threads have NULL and belong to this owned instance.
        activity = query(test.observer, 'SELECT pid,datname,usename,application_name,client_port '
                         'FROM pg_stat_activity WHERE usename IS NOT NULL AND client_port IS NOT NULL')
        foreign = [row for row in activity if row[0] not in known_pids]
        check('no unrelated frontend SQL sessions will be interrupted', foreign, [])
        result.update(crashed_pid=pid, candidate_hashes=candidates, pre_crash_activity=activity)
        write_json(out / 'pre-crash.json', roles.redact(result))
        server_log = RUNTIME / 'server.log'
        log_offset = server_log.stat().st_size
        fd = os.pidfd_open(pid)
        try:
            # pidfd prevents a recycled numeric PID from becoming a kill target.
            signal.pidfd_send_signal(fd, signal.SIGKILL)
            crashed = True
        finally:
            os.close(fd)
        deadline = time.monotonic() + 10
        while executable.exists() and time.monotonic() < deadline:
            time.sleep(.02)
        check('owned process exited without transaction cleanup callbacks', not executable.exists())
        if future is not None:
            result['interrupted_commit'] = future.result(timeout=10)
            check('unacknowledged COMMIT did not report success', result['interrupted_commit'] != 'accepted')
        restart()
        restarted = True
        with server_log.open('rb') as stream:
            stream.seek(log_offset)
            (out / 'crash-recovery-server.log').write_bytes(stream.read())
        for conn in db.connections:
            conn.close()
        db.connections = []
        control.close()
        control = connect_control()
        test.a, test.b, test.observer = [db.connect() for _ in range(3)]
        expected_identity = (db.allocation['database_oid'], db.database,
                             db.allocation['owner_oid'], db.allocation['marker'])
        observed = query(control, "SELECT oid,datname,datdba,shobj_description(oid,'pg_database') "
                         'FROM pg_database WHERE datname=%s OR oid=%s',
                         (db.database, db.allocation['database_oid']))
        check('recovery preserves exact database OID owner and marker', observed, [expected_identity])
        check('recovered business values', test.rows(test.observer), [(1, 11), (2, 20), (3, 30)])
        check('recovered complete catalog is transaction atomic', test.protocol(), expected_protocol)
        check('recovered inline source transfer is transaction atomic',
              query(test.observer, physical_sql), [] if args.mode == 'committed' else physical_before)
        check('ordinary heap follows same recovery decision', query(test.observer, 'SELECT * FROM biz.heap_marker'),
              [(1,)] if args.mode == 'committed' else [])
        after = {str(p): sha256(p) for p in root.rglob('*.parquet')}
        check('durably prepared objects retain exact bytes across process crash', after, candidates)
        check('recovered data-file registration count',
              scalar(test.observer, 'SELECT count(*) FROM created_metadata.ducklake_data_file'),
              1 if args.mode == 'committed' else 0)
        check('recovered deletion-file registration count',
              scalar(test.observer, 'SELECT count(*) FROM created_metadata.ducklake_delete_file'),
              1 if args.mode == 'committed' else 0)
        result['unregistered_candidate_count_after_recovery'] = 0 if args.mode == 'committed' else len(after)
        names = query(test.observer, "SELECT c.relname FROM pg_class c JOIN pg_namespace n "
                      "ON n.oid=c.relnamespace WHERE n.nspname='created_metadata' AND c.relkind='r' ORDER BY c.relname")
        native = read_native_catalog(test.observer, 'created_metadata',
                                     {'tables': [{'table_name': name} for name, in names]}, out / 'native-catalog.json')
        lock = json.loads((DRIVER / 'source-lock.json').read_text())
        receipt = json.loads((ROOT / '.reference/opengauss-native-lake/build/receipt.json').read_text())
        check('independent reader matches exact source lock', receipt['source_lock_hash'], sha256(DRIVER / 'source-lock.json'))
        ref = Reference(out, lock, receipt)
        catalog = out / 'transport.ducklake'
        transport_native(ref, catalog, native)
        current = ref.run(ref.attach(catalog, True) + 'SELECT id,val FROM lake.main.orders ORDER BY id',
                          'independent-recovered-current', result=True)
        check('independent DuckLake reads recovered state', [(r['id'], r['val']) for r in current],
              [(1, 11), (2, 20), (3, 30)])
        old = ref.run(ref.attach(catalog, True) + 'SELECT id,val FROM lake.main.orders AT (VERSION => ' +
                      str(initial_snapshot) + ') ORDER BY id', 'independent-recovered-history', result=True)
        check('independent DuckLake reads retained pre-update snapshot', [(r['id'], r['val']) for r in old],
              [(1, 10), (2, 20), (3, 30)])
        check('all executed inputs stayed fixed', all(sha256(p) == h for p, h in result['input_hashes'].items()))
        check('actual SDK bytes stayed fixed across recovery', sha256(provider), result['provider']['sha256'])
        check_build(args.build_receipt)
        result['status'] = 'pass'
    except BaseException as exc:
        result.update(reason=str(exc), traceback=traceback.format_exc())
    finally:
        if crashed and not restarted:
            try:
                restart()
                restarted = True
            except BaseException as exc:
                result.update(status='fail', restart_error=str(exc))
        if future is not None and not future.done() and not crashed:
            try:
                scalar(test.observer, 'SELECT pg_cancel_backend(%s)', (publisher,))
            except BaseException:
                pass
        if holder is not None:
            holder.close()
        if executor is not None:
            executor.shutdown(wait=True)
        if db is not None:
            try:
                if crashed:
                    if not restarted:
                        raise RuntimeError('Instance recovery unproven; retain owned database')
                    if control is not None:
                        control.close()
                    control = connect_control()
                    expected = (db.allocation['database_oid'], db.database,
                                db.allocation['owner_oid'], db.allocation['marker'])
                    observed = query(control, "SELECT oid,datname,datdba,shobj_description(oid,'pg_database') "
                                     'FROM pg_database WHERE oid=%s', (db.allocation['database_oid'],))
                    if observed != [expected]:
                        raise RuntimeError('Recovery cleanup cannot verify original OID owner and marker')
                    # Preserve original pre-crash ownership evidence. The normal
                    # strict cleanup gets a new receipt backed by recovery proof.
                    cleanup_dir = out / 'recovery-cleanup'
                    cleanup_db = OwnedDatabase(args.run_id, cleanup_dir)
                    cleanup_db.database = db.database
                    cleanup_db.allocation = dict(db.allocation, attempt=str(cleanup_dir),
                        postmaster_start=str(scalar(control, 'SELECT pg_postmaster_start_time()')),
                        original_owner_receipt_sha256=sha256(out / 'database-owner.json'),
                        restart_receipt=result.get('restart'))
                    cleanup_db._save_allocation()
                    cleanup_db.connections = db.connections
                    cleanup_db.cleanup()
                    result['database_cleanup'] = cleanup_db.cleanup_result
                else:
                    db.cleanup()
                    result['database_cleanup'] = db.cleanup_result
            except BaseException as exc:
                result.update(status='fail', cleanup_error=str(exc))
        if roles is not None:
            try:
                if not result.get('database_cleanup') or result['database_cleanup']['returncode']:
                    raise RuntimeError('Database cleanup unproven; role retained')
                role_receipt = roles.path
                if crashed:
                    original_roles = json.loads(roles.path.read_text())
                    for item in original_roles['roles']:
                        identity = query(control, "SELECT oid,rolname,shobj_description(oid,'pg_authid') "
                                         'FROM pg_roles WHERE rolname=%s OR oid=%s',
                                         (item['name'], item['oid']))
                        if identity != [(item['oid'], item['name'], item['marker'])]:
                            raise RuntimeError('Recovery cleanup cannot verify original role OID and marker')
                    role_receipt = out / 'recovery-cleanup/permission-roles.json'
                    recovered_roles = dict(original_roles,
                        postmaster_start=str(scalar(control, 'SELECT pg_postmaster_start_time()')),
                        original_receipt_sha256=sha256(roles.path), restart_receipt=result['restart'])
                    write_json(role_receipt, recovered_roles)
                result['role_cleanup'] = cleanup_roles(control, role_receipt, args.run_id)
                result = roles.redact(result)
            except BaseException as exc:
                result.update(status='fail', role_cleanup_error=str(exc))
        if control is not None:
            control.close()
        write_json(out / 'result.json', result)
    print(json.dumps({'status': result['status'], 'mode': args.mode, 'reason': result.get('reason'),
                      'checks': len(result['assertions']), 'result': str(out / 'result.json')}))
    return 0 if result['status'] == 'pass' else 1


if __name__ == '__main__':
    raise SystemExit(main())
