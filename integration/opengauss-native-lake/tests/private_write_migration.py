#!/usr/bin/env python3
"""Bounded host transaction-pinning and private lake write-set cleanup checks."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import sys
import traceback


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--workspace', type=Path, default=Path('/home/qiumc/data/code/cpp'))
    p.add_argument('--run-id', required=True)
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--manifest', type=Path, required=True)
    p.add_argument('--rounds', type=int, default=24)
    args = p.parse_args()
    if not 1 <= args.rounds <= 100:
        p.error('rounds must be 1..100; no retry-until-migration loop exists')
    driver = args.workspace.resolve() / 'integration/opengauss-native-lake'
    sys.path.insert(0, str(driver))
    from inline_fixture import foreign_table_statement, multiset, query, scalar
    from native_test_context import OwnedDatabase, load_reference, reviewed_runtime_version
    from lake_harness import validate_run_id, write_json
    validate_run_id(args.run_id)
    output = args.output.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    result = {'status': 'fail', 'scope': 'N3/private_write_transaction_pin',
              'assertions': [], 'artifacts': [], 'parameters': {'writers': 3, 'churn_sessions': 4,
              'rounds': args.rounds}, 'observations': [], 'notes': [
                  'Real dedicated/thread-pool identifiers come from the native diagnostic, never Python thread IDs.',
                  'Host xact.cpp WorkerThreadCanSeekAnotherMission pins non-TBLOCK_DEFAULT transactions.',
                  'The live write-set transaction must retain one worker, including after child rollback.',
                  'Top-level COMMIT/ROLLBACK clears private state before later session scheduling.',
                  'Cross-transaction worker migration is a separate host-case proof, outside this case.'],
              'host_contract': {'source_baseline': '8365eecf99a9f9b7623cb6920a2e13663e9829ff',
                  'function': 'WorkerThreadCanSeekAnotherMission',
                  'condition': 'blockState != TBLOCK_DEFAULT => TWORKER_STILLINTRANS / false',
                  'source': 'src/gausskernel/storage/access/transam/xact.cpp:516'},
              'completion_observations': []}
    owned = None

    def check(name, observed, expected=True):
        result['assertions'].append({'name': name, 'status': 'pass' if observed == expected else 'fail',
                                     'details': {'observed': observed, 'expected': expected}})
        if observed != expected:
            raise AssertionError(name)

    try:
        owned = OwnedDatabase(args.run_id, output.parent)
        with owned as db:
            admin = db.connect()
            check('actual native thread-pool mode is enabled', scalar(admin, 'SHOW enable_thread_pool'), 'on')
            query(admin, "CREATE EXTENSION og_lake VERSION '0.3'")
            caps = json.loads(scalar(admin, 'SELECT og_lake.host_status()'))
            runtime_version = reviewed_runtime_version()
            check('native INSERT runtime exactly matches reviewed source control',
                  (caps['module_version'], caps['inline_insert']), (runtime_version, True))
            check('migration compatibility keeps installed SQL objects at 0.3',
                  scalar(admin, "SELECT extversion FROM pg_extension WHERE extname='og_lake'"), '0.3')
            result['runtime_version'] = runtime_version
            result['installed_extension_version'] = '0.3'
            manifest, exported, counts, artifacts = load_reference(admin, args.manifest, args.run_id, output.parent)
            result['artifacts'].extend(artifacts)
            check('genuine fixture profile is I', manifest['case'], 'I')
            check('all genuine fixture tables imported', len(counts), len(exported['tables']))
            query(admin, 'CREATE SCHEMA biz')
            query(admin, "SELECT og_lake.attach_catalog('lake_reference','lake_metadata',NULL::text,false)")
            query(admin, foreign_table_statement('biz', 'orders', 'lake_reference', manifest['table_id'], manifest['schema']))
            initial = [[row['id'], row['val']] for row in manifest['expected']]
            writers = [db.connect() for _ in range(3)]
            churn = [db.connect() for _ in range(4)]
            sessions = [None] * len(writers)
            threads = [set() for _ in writers]
            pinned_threads = [None] * len(writers)
            staged = [None] * len(writers)

            def observe(index, phase):
                state = json.loads(scalar(writers[index], 'SELECT og_lake_internal.test_xact_state()'))
                logical = state.get('logical_session_id')
                thread = state.get('thread_id')
                if not isinstance(logical, int) or logical <= 0 or not isinstance(thread, int) or thread <= 0:
                    raise AssertionError('native logical/thread identities must be positive integers')
                if sessions[index] is None:
                    sessions[index] = logical
                elif sessions[index] != logical:
                    raise AssertionError('logical session changed while holding private write state')
                if state['staged_bytes'] <= 0:
                    raise AssertionError('private rows disappeared before transaction completion')
                if staged[index] is None:
                    staged[index] = state['staged_bytes']
                elif staged[index] != state['staged_bytes']:
                    raise AssertionError('private write budget changed across a read-only round')
                if pinned_threads[index] is None:
                    pinned_threads[index] = thread
                check(f'writer {index} keeps its transaction worker at {phase}',
                      thread, pinned_threads[index])
                threads[index].add(thread)
                result['observations'].append({'writer': index, 'phase': phase,
                    'logical_session_id': logical, 'thread_id': thread, 'staged_bytes': state['staged_bytes']})

            for i, conn in enumerate(writers):
                query(conn, 'BEGIN')
                query(conn, 'INSERT INTO biz.orders VALUES(%s,%s)', (1000001 + i, 500 + i))
                observe(i, 'after_insert')
            check('writers are distinct logical sessions', len(set(sessions)), len(writers))
            with ThreadPoolExecutor(max_workers=4) as pool:
                for iteration in range(args.rounds):
                    # All calls use different connections. Churn only ordinary
                    # SQL; it cannot alter the lake oracle or grant any lock.
                    futures = [pool.submit(scalar, conn, 'SELECT 40+2') for conn in churn]
                    for i, conn in enumerate(writers):
                        wanted = initial + [[1000001 + i, 500 + i]]
                        check(f'round {iteration} writer {i} retains only its own private row',
                              multiset(query(conn, 'SELECT id,val FROM biz.orders')), multiset(wanted))
                        observe(i, 'round_' + str(iteration))
                    check(f'round {iteration} ordinary SQL churn remains correct',
                          [future.result(timeout=20) for future in futures], [42] * len(churn))
                    check(f'round {iteration} private writes remain invisible to observer',
                          scalar(admin, 'SELECT count(*) FROM biz.orders WHERE id>=1000000'), 0)
            check('writers retain nonempty budgets before savepoint rollback checks',
                  all(value > 0 for value in staged))
            for i, conn in enumerate(writers):
                query(conn, 'SAVEPOINT migration_child')
                query(conn, 'INSERT INTO biz.orders VALUES(%s,%s)', (2000001 + i, 900 + i))
                query(conn, 'ROLLBACK TO SAVEPOINT migration_child')
                observe(i, 'after_child_rollback')
                query(conn, 'COMMIT' if i != 1 else 'ROLLBACK')
                state = json.loads(scalar(conn, 'SELECT og_lake_internal.test_xact_state()'))
                check(f'writer {i} releases its private state at transaction end', state['staged_bytes'], 0)
                check(f'writer {i} retains its logical session after transaction end',
                      state['logical_session_id'], sessions[i])
                cleared = ('transaction_memory_bytes', 'transaction_catalogs', 'transaction_tables',
                           'transaction_rows', 'catalog_guards')
                check(f'writer {i} releases its transaction resources before later scheduling',
                      {name: state[name] for name in cleared}, {name: 0 for name in cleared})
                result['completion_observations'].append({'writer': i,
                    'ending': 'COMMIT' if i != 1 else 'ROLLBACK',
                    'logical_session_id': state['logical_session_id'], 'thread_id': state['thread_id'],
                    'staged_bytes': state['staged_bytes'],
                    'cleared_state': {name: state[name] for name in cleared}})
            check('commit and rollback retain exactly their intended rows',
                  multiset(query(admin, 'SELECT id,val FROM biz.orders')),
                  multiset(initial + [[1000001, 500], [1000003, 502]]))
            result['writers'] = [{'logical_session_id': sessions[i], 'thread_ids_while_staged': sorted(threads[i])}
                                 for i in range(len(writers))]
            result['migration_observed_while_staged'] = any(len(value) > 1 for value in threads)
            result['functional_status'] = 'pass'
            result['transaction_pinning_verified'] = True
            result['status'] = 'pass'
            result['reason'] = ('All live write-set observations retained their transaction worker; '
                                'commit/rollback cleared private state and preserved exact intended rows. '
                                'This case does not require or claim cross-transaction migration.')
    except BaseException as exc:
        result.update(status='fail', error=repr(exc), traceback=traceback.format_exc())
    finally:
        if owned is not None:
            result['artifacts'].extend(owned.artifacts())
            result['cleanup'] = owned.cleanup_result
        write_json(output, result)
        print(json.dumps({'status': result['status'], 'result': str(output)}), flush=True)
    return {'pass': 0, 'blocked': 2}.get(result['status'], 1)


if __name__ == '__main__':
    raise SystemExit(main())
