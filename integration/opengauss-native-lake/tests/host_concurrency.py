#!/usr/bin/env python3
"""Exercise real local openGauss maintenance claims; no server lifecycle or mock locks."""
import argparse
import datetime
import json
import os
from pathlib import Path
import threading
import time
import traceback


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--run-id', default=os.getenv('OG_LAKE_RUN_ID', 'manual'))
    parser.add_argument('--output', default=os.getenv('OG_LAKE_RESULT_FILE'))
    parser.add_argument('--sessions', type=int, default=8)
    parser.add_argument('--rounds', type=int, default=int(os.getenv('OG_LAKE_STRESS_ROUNDS', '40')),
                        help='successful ownership rounds per session (default: 8 * 40 = 320 total)')
    parser.add_argument('--deadline', type=float, default=120.0)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[3]
    output = Path(args.output) if args.output else root / '公共文档/openGauss原生湖存储/验证' / args.run_id / 'host-concurrency.json'
    output.parent.mkdir(parents=True, exist_ok=True)
    result = {
        'status': 'fail', 'scope': 'JOB-02/local_stress', 'assertions': [], 'artifacts': [],
        'notes': ['Local openGauss sessions only; does not verify RAC, launcher scheduling or lake DML.',
                  'Claims execute concurrently in real SQL. A Python observer only records entry/exit; it does not grant ownership.',
                  'Retries use bounded cancellation-aware 1 ms backoff; no long sleep is used as a concurrency oracle.'],
        'parameters': {'sessions': args.sessions, 'rounds_per_session': args.rounds,
                       'expected_critical_sections': args.sessions * args.rounds, 'deadline_seconds': args.deadline}}
    connections = []
    connections_guard = threading.Lock()
    admin = None
    dbname = None
    threads = []
    stop = threading.Event()
    round_barrier = None
    started = time.monotonic()

    def check(name, observed, expected=True):
        passed = observed == expected
        result['assertions'].append({'name': name, 'status': 'pass' if passed else 'fail',
                                     'details': {'observed': observed, 'expected': expected}})
        if not passed:
            raise AssertionError(name)

    try:
        if not 2 <= args.sessions <= 32 or not 1 <= args.rounds <= 10000 or not 5 <= args.deadline <= 3600:
            result['status'] = 'blocked'
            raise ValueError('Require 2..32 sessions, 1..10000 rounds and 5..3600 second deadline.')
        try:
            import psycopg2
            from psycopg2 import sql
        except ImportError:
            result['status'] = 'blocked'
            raise RuntimeError('psycopg2 is required for real host tests.')
        host = str(root / '.runtime/opengauss-native-lake/debug/socket')
        port = int(os.getenv('OG_LAKE_TEST_PORT', '55560'))

        def query(conn, statement, parameters=None):
            with conn.cursor() as cursor:
                cursor.execute(statement, parameters)
                return cursor.fetchall() if cursor.description else None

        def scalar(conn, statement, parameters=None):
            return query(conn, statement, parameters)[0][0]

        def connect(database):
            conn = psycopg2.connect(host=host, port=port, dbname=database,
                                    user=os.getenv('USER', 'qiumc'), connect_timeout=5,
                                    application_name='og_lake_host_concurrency')
            conn.autocommit = True
            with connections_guard:
                connections.append(conn)
            query(conn, 'SET statement_timeout = 10000')
            return conn

        def claim(conn):
            return scalar(conn, 'SELECT og_lake_internal.test_maintenance_claim()')

        def release(conn):
            return scalar(conn, 'SELECT og_lake_internal.test_maintenance_release()')

        thread_observations = []

        def held(conn, worker_index=None):
            state = json.loads(scalar(conn, 'SELECT og_lake_internal.test_xact_state()'))
            if worker_index is not None:
                observed = thread_observations[worker_index]
                if thread_pool_mode == 'on' and state.get('logical_session_id') != observed['logical_session_id']:
                    raise AssertionError('logical session identity changed across worker transactions')
                thread_id = state.get('thread_id')
                if isinstance(thread_id, int) and thread_id > 0 and thread_id not in observed['thread_ids']:
                    observed['thread_ids'].append(thread_id)
                    observed['first_seen'].append({'thread_id': thread_id, 'monotonic_ns': time.monotonic_ns()})
            return state['maintenance_held']

        admin = connect('postgres')
        if os.getenv('OG_LAKE_EXPECT_THREAD_POOL'):
            check('actual thread pool mode', scalar(admin, 'SHOW enable_thread_pool'),
                  os.environ['OG_LAKE_EXPECT_THREAD_POOL'])
        dbname = 'lake_stress_' + datetime.datetime.now().strftime('%Y%m%d%H%M%S') + '_' + str(os.getpid())
        query(admin, sql.SQL('CREATE DATABASE {}').format(sql.Identifier(dbname)))
        control = connect(dbname)
        query(control, 'CREATE EXTENSION og_lake')
        workers = [connect(dbname) for _ in range(args.sessions)]
        thread_pool_mode = scalar(control, 'SHOW enable_thread_pool')
        session_identity_records = [
            {'backend_pid': scalar(conn, 'SELECT pg_backend_pid()'),
             'host': json.loads(scalar(conn, 'SELECT og_lake_internal.test_xact_state()'))}
            for conn in workers]
        if thread_pool_mode == 'off':
            # Dedicated sessions have distinct backend threads; u_sess session_id
            # is legitimately zero and is not a connection-identity oracle.
            session_ids = [item['backend_pid'] for item in session_identity_records]
        elif thread_pool_mode == 'on':
            session_ids = [item['host'].get('logical_session_id') for item in session_identity_records]
            check('thread pool exposes positive logical session identities',
                  all(isinstance(value, int) and value > 0 for value in session_ids))
        else:
            raise RuntimeError('unknown actual thread pool mode: ' + str(thread_pool_mode))
        result['thread_pool_mode'] = thread_pool_mode
        result['session_identity_records'] = session_identity_records
        thread_observations.extend({'logical_session_id': item['host'].get('logical_session_id'),
                                    'thread_ids': [item['host']['thread_id']] if item['host'].get('thread_id') else [],
                                    'first_seen': []}
                                   for item in session_identity_records)
        check('independent real backend sessions', len(set(session_ids)), args.sessions)
        check('control session owns deterministic contention window', claim(control))
        check('all competing sessions rejected while owner held', [claim(conn) for conn in workers],
              [False] * args.sessions)
        check('control releases before stress', release(control))

        observer = threading.Lock()
        active_owner = None
        events = []
        errors = []
        counters = [{'acquired': 0, 'rejected': 0, 'commit': 0, 'rollback': 0,
                     'unlock_all': 0, 'duplicate_claim': 0} for _ in workers]
        round_barrier = threading.Barrier(args.sessions)
        deadline = time.monotonic() + args.deadline

        def worker(index, conn):
            nonlocal active_owner
            owns = False
            entered = False
            try:
                for round_number in range(args.rounds):
                    if stop.is_set() or time.monotonic() >= deadline:
                        raise TimeoutError('stress stopped or exceeded its deadline')
                    round_barrier.wait(timeout=max(0.1, deadline - time.monotonic()))
                    while not stop.is_set():
                        if time.monotonic() >= deadline:
                            raise TimeoutError('maintenance claim starvation/deadline')
                        if claim(conn):
                            owns = True
                            break
                        counters[index]['rejected'] += 1
                        stop.wait(0.001)
                    if not owns:
                        raise RuntimeError('peer failed before ownership')
                    # This observer does not surround SQL claim or the critical section.
                    with observer:
                        if active_owner is not None:
                            raise AssertionError(f'overlapping real claims: owner={active_owner}, entrant={index}')
                        active_owner = index
                        entered = True
                        events.append({'event': 'enter', 'worker': index, 'round': round_number,
                                       'monotonic_ns': time.monotonic_ns()})
                    if not claim(conn):
                        raise AssertionError('duplicate owner claim rejected')
                    counters[index]['duplicate_claim'] += 1
                    query(conn, 'BEGIN')
                    if scalar(conn, 'SELECT 1') != 1:
                        raise AssertionError('ordinary SQL failed within owner transaction')
                    transaction_action = 'COMMIT' if (round_number + index) % 2 == 0 else 'ROLLBACK'
                    query(conn, transaction_action)
                    counters[index][transaction_action.lower()] += 1
                    if not held(conn, index):
                        raise AssertionError('maintenance ownership lost across transaction boundary')
                    if (round_number + index) % 3 == 0:
                        query(conn, 'SELECT pg_advisory_lock(%s::bigint)', (782300000 + index,))
                        query(conn, 'SELECT pg_advisory_unlock_all()')
                        counters[index]['unlock_all'] += 1
                        if not held(conn, index):
                            raise AssertionError('application unlock_all lost internal maintenance claim')
                    if not claim(conn):
                        raise AssertionError('owner cannot reclaim after transaction/unlock_all')
                    with observer:
                        if active_owner != index:
                            raise AssertionError('critical section observer owner changed')
                        events.append({'event': 'exit', 'worker': index, 'round': round_number,
                                       'monotonic_ns': time.monotonic_ns()})
                        active_owner = None
                        entered = False
                    if not release(conn):
                        raise AssertionError('owner release failed')
                    owns = False
                    if release(conn):
                        raise AssertionError('duplicate claim leaked an extra ownership reference')
                    if held(conn, index):
                        raise AssertionError('maintenance state remained held after release')
                    counters[index]['acquired'] += 1
            except BaseException as exc:
                with observer:
                    errors.append({'worker': index, 'error': repr(exc), 'traceback': traceback.format_exc()})
                    if entered and active_owner == index:
                        active_owner = None
                stop.set()
                round_barrier.abort()
            finally:
                try:
                    if not conn.closed:
                        query(conn, 'ROLLBACK')
                        if owns:
                            release(conn)
                except BaseException as exc:
                    with observer:
                        errors.append({'worker': index, 'cleanup_error': repr(exc)})

        threads = [threading.Thread(target=worker, args=(index, conn), name=f'lake-stress-{index}', daemon=True)
                   for index, conn in enumerate(workers)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=max(0.1, deadline + 12 - time.monotonic()))
        check('all stress threads ended within deadline', any(thread.is_alive() for thread in threads), False)
        result['counters'] = counters
        result['errors'] = errors
        result['thread_observations'] = thread_observations
        result['thread_migration_observed'] = thread_pool_mode == 'on' and any(
            len(item['thread_ids']) > 1 for item in thread_observations)
        if thread_pool_mode == 'on' and not result['thread_migration_observed']:
            result['notes'].append('No thread migration was observed; this run makes no migration-coverage claim.')
        audit_path = output.with_name('host-concurrency-events.json')
        audit_path.write_text(json.dumps({'session_ids': session_ids, 'thread_pool_mode': thread_pool_mode,
                                         'session_identity_records': session_identity_records,
                                         'thread_observations': thread_observations,
                                         'events': events, 'errors': errors,
                                         'counters': counters}, indent=2) + '\n')
        result['artifacts'].append({'path': str(audit_path)})
        check('no critical section overlap or SQL/lifecycle errors', len(errors), 0)
        check('every session completed every round', [counter['acquired'] for counter in counters],
              [args.rounds] * args.sessions)
        check('all critical sections have paired observer records', len(events), args.sessions * args.rounds * 2)
        check('observer empty after competition', active_owner, None)
        check('both commit and rollback interleavings executed',
              all(sum(counter[action] for counter in counters) > 0 for action in ('commit', 'rollback')))
        check('unlock_all interleavings executed', sum(counter['unlock_all'] for counter in counters) > 0)
        check('duplicate claims exercised on every ownership round',
              sum(counter['duplicate_claim'] for counter in counters), args.sessions * args.rounds)
        check('stress produced real rejected claims', sum(counter['rejected'] for counter in counters) > 0)
        check('all workers report no maintenance residue', [held(conn) for conn in workers],
              [False] * args.sessions)

        # Disconnect recovery is separate from the repeated explicit-release workload.
        check('disconnect recovery holder acquires', claim(control))
        check('disconnect recovery waiter initially blocked', claim(workers[0]), False)
        control.close()
        recovery_deadline = time.monotonic() + 10
        recovered = claim(workers[0])
        recovery_attempts = 1
        while not recovered and time.monotonic() < recovery_deadline:
            stop.wait(0.001)
            recovered = claim(workers[0])
            recovery_attempts += 1
        result['disconnect_recovery_attempts'] = recovery_attempts
        check('session close releases ownership without manual unlock', recovered)
        check('recovered owner releases exactly once', release(workers[0]))
        check('no residue after close recovery', release(workers[0]), False)
        check('another session can acquire after all tests', claim(workers[-1]))
        check('final owner releases', release(workers[-1]))
        result['status'] = 'pass'
    except BaseException as exc:
        result['error'] = repr(exc)
        result['traceback'] = traceback.format_exc()
    finally:
        stop.set()
        if round_barrier:
            round_barrier.abort()
        cleanup_errors = []
        for conn in connections:
            if conn is admin:
                continue
            try:
                if not conn.closed:
                    conn.cancel()
                    conn.close()
            except BaseException as exc:
                cleanup_errors.append(repr(exc))
        for thread in threads:
            thread.join(timeout=2)
        if any(thread.is_alive() for thread in threads):
            cleanup_errors.append('worker thread remains alive after cancellation')
        if admin is not None:
            try:
                if dbname:
                    query(admin, sql.SQL('DROP DATABASE {}').format(sql.Identifier(dbname)))
            except BaseException as exc:
                cleanup_errors.append(repr(exc))
            finally:
                admin.close()
        result['cleanup_errors'] = cleanup_errors
        if cleanup_errors:
            result['status'] = 'fail'
        result['elapsed_seconds'] = round(time.monotonic() - started, 3)
        output.write_text(json.dumps(result, indent=2) + '\n')
        print(json.dumps({'status': result['status'], 'scope': result['scope'], 'output': str(output),
                          'assertions': len(result['assertions']), 'elapsed_seconds': result['elapsed_seconds']}))
    return {'pass': 0, 'blocked': 2}.get(result['status'], 1)


if __name__ == '__main__':
    raise SystemExit(main())
