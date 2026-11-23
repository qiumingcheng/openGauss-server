#!/usr/bin/env python3
"""Cancel genuine first inline publication after data writes, before column stats.

Uses only real relation locks and the production commit path. Preparation of
this file does not run SQL or alter the existing fixture/helper/registry.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import os
from pathlib import Path
import secrets
import shutil
import sys
import time
import traceback

WORKSPACE = Path('/home/qiumc/data/code/cpp')
DRIVER = Path(os.getenv('OG_LAKE_DRIVER_ROOT', str(WORKSPACE / 'integration/opengauss-native-lake')))
sys.path.insert(0, str(DRIVER))
sys.path.insert(0, str(DRIVER / 'tests'))
import empty_inline_insert
from inline_fixture import query, scalar
from lake_harness import Blocked, validate_case_id, validate_run_id, write_json
from native_test_context import OwnedDatabase

CASE_ID = 'N3/empty_insert_publish_cancel'


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def scenario(t):
    a, b, holder = t.a, t.b, t.c
    observer = t.db.connect()
    app = 'lake_first_cancel_' + secrets.token_hex(10)
    query(a, 'SET application_name=%s', (app,))
    query(a, 'SET statement_timeout=30000')
    dbid = scalar(observer, 'SELECT oid FROM pg_database WHERE datname=current_database()')
    relations = {
        'columns': 'lake_metadata.ducklake_table_column_stats',
        'stats': 'lake_metadata.ducklake_table_stats',
        'snapshot': 'lake_metadata.ducklake_snapshot',
        'carrier': 'lake_metadata.' + t.carrier,
        'heap': 'biz.heap_marker',
    }
    oids = {name:scalar(observer,'SELECT %s::regclass::oid',(relation,))
            for name,relation in relations.items()}
    t.result['barrier_relation_oids'] = oids
    baseline = t.image(observer)
    t.check('first-publication cancellation starts with genuine missing table stats', t.stats(), [])
    t.check('first-publication cancellation starts with genuine missing column stats', t.column_stats(), [])
    t.check('ordinary heap starts empty', query(observer,'SELECT * FROM biz.heap_marker'), [])

    future = executor = None
    locked = False
    writer_pid = None
    verified_waiter = False
    try:
        query(holder, 'BEGIN')
        query(holder, 'LOCK TABLE lake_metadata.ducklake_table_column_stats IN SHARE MODE')
        locked = True
        holder_pid = scalar(holder, 'SELECT pg_backend_pid()')
        t.check('holder owns the exact column-statistics SHARE lock', scalar(observer,
            "SELECT count(*) FROM pg_locks WHERE locktype='relation' AND database=%s AND relation=%s "
            "AND pid=%s AND mode='ShareLock' AND granted", (dbid,oids['columns'],holder_pid)), 1)

        query(a, 'BEGIN')
        # The explicit user transaction pins the worker in the native pool
        # contract. Capture its actual PID after BEGIN, never before it.
        writer_pid = scalar(a, 'SELECT pg_backend_pid()')
        identity = query(a, 'SELECT session_user,current_user')
        t.check('publisher and lock holder are separate actual workers', writer_pid != holder_pid)
        query(a, 'INSERT INTO biz.heap_marker VALUES(1,111)')
        returned = query(a, 'INSERT INTO biz.orders VALUES(101,1010),(102,NULL) RETURNING id,val')
        t.check('first private INSERT returns exact uncommitted values', returned, [(101,1010),(102,None)])
        t.expect(a,[{'id':101,'val':1010},{'id':102,'val':None}], 'publisher sees its private first rows')
        t.check('staging does not expose shared metadata', t.image(observer), baseline)

        def commit():
            import psycopg2
            try:
                query(a,'COMMIT')
                return {'state':'accepted'}
            except psycopg2.Error as exc:
                return {'state':exc.pgcode, 'message':str(exc)}

        executor = ThreadPoolExecutor(max_workers=1)
        future = executor.submit(commit)
        deadline = time.monotonic() + 12
        waits = []
        while time.monotonic() < deadline:
            waits = query(observer,
                "SELECT pid,relation,mode,granted,database FROM pg_locks WHERE locktype='relation' "
                "AND database=%s AND relation=%s AND pid=%s AND mode='RowExclusiveLock' AND NOT granted",
                (dbid,oids['columns'],writer_pid))
            if waits or future.done():
                break
            time.sleep(0.02)  # bounded observation, never the synchronization proof
        t.check('publisher reaches the exact first column-statistics INSERT lock', len(waits), 1)
        activity = query(observer, 'SELECT application_name FROM pg_stat_activity WHERE pid=%s', (writer_pid,))
        t.check('actual waiter belongs to the owned publisher request', activity, [(app,)])
        verified_waiter = True
        granted = query(observer,
            "SELECT relation,mode FROM pg_locks WHERE locktype='relation' AND database=%s "
            "AND pid=%s AND relation=ANY(%s::oid[]) AND mode='RowExclusiveLock' AND granted",
            (dbid,writer_pid,[oids[k] for k in ('snapshot','stats','carrier','heap')]))
        t.check('publisher already owns write locks for claim, seeded stats, carrier and ordinary heap',
                sorted({row[0] for row in granted}), sorted(oids[k] for k in ('snapshot','stats','carrier','heap')))
        observation = {'writer_pid':writer_pid, 'holder_pid':holder_pid, 'application_name':app,
            'requested_column_stats_lock':[list(row) for row in waits],
            'already_granted_write_locks':[list(row) for row in granted],
            'interpretation':'Production source order places this first column-statistics RowExclusive request after seed, allocation, carrier INSERTs and table-stats UPDATE. Locks alone are not an uncommitted-row read.'}
        path = t.args.output.parent / 'publication-lock-observation.json'
        write_json(path,observation)
        t.result['artifacts'].append({'path':str(path)})
        t.result['publication_lock_observation'] = observation
        t.check('no attempted protocol write is visible before outer commit', t.image(observer), baseline)
        t.check('ordinary heap is not visible before outer commit', query(observer,'SELECT * FROM biz.heap_marker'), [])
        t.expect(observer, [], 'another transaction still sees original empty business table')
        t.check('precise cancel reaches the verified publisher waiter',
                scalar(observer,'SELECT pg_cancel_backend(%s)',(writer_pid,)), True)
        outcome = future.result(timeout=10)
        t.result['cancelled_commit_outcome'] = outcome
        t.check('actual COMMIT cancellation SQLSTATE', outcome['state'], '57014')
        query(a,'ROLLBACK')
        t.check('every standard catalog and carrier row returns to original visible state', t.image(observer), baseline)
        t.check('ordinary heap rolls back with partial lake publication', query(observer,'SELECT * FROM biz.heap_marker'), [])
        t.check('rollback removes the new table statistics seed', t.stats(), [])
        t.check('rollback leaves no initial column statistics', t.column_stats(), [])
        t.check('rollback removes all visible new inline physical versions', t.physical(), [])
        t.check('rollback restores all original snapshot frontiers', t.snapshot(), t.start_snapshot)
        t.check('publication error restores request identity', query(a,'SELECT session_user,current_user'), identity)
        t.check('cancelled publisher session is reusable', scalar(a,'SELECT 6*7'), 42)
        t.check('cancelled request holds no residual write locks on publication relations', scalar(observer,
            "SELECT count(*) FROM pg_locks WHERE locktype='relation' AND database=%s AND pid=%s "
            "AND relation=ANY(%s::oid[]) AND mode='RowExclusiveLock'",
            (dbid,writer_pid,list(oids.values()))), 0)
    finally:
        try:
            if future is not None and not future.done() and verified_waiter:
                # Cancel only a PID whose exact owned relation wait and app
                # identity were observed, never an unrelated backend.
                scalar(observer,'SELECT pg_cancel_backend(%s)',(writer_pid,))
        finally:
            try:
                if locked:
                    query(holder,'ROLLBACK')
                    locked = False
            finally:
                if executor is not None:
                    executor.shutdown(wait=True)  # publisher has a 30s SQL timeout
                query(a,'ROLLBACK')
    t.check('barrier holder releases column-statistics SHARE lock', scalar(observer,
        "SELECT count(*) FROM pg_locks WHERE locktype='relation' AND database=%s AND relation=%s "
        "AND pid=%s AND mode='ShareLock' AND granted",(dbid,oids['columns'],holder_pid)), 0)

    # Retry on a different connection, after the controlled failed transaction
    # is fully rolled back. No fake stats, repaired metadata, or manual ID reset.
    query(b,'BEGIN')
    query(b,'INSERT INTO biz.heap_marker VALUES(2,222)')
    statement = 'INSERT INTO biz.orders VALUES(201,2010),(202,NULL) RETURNING id,val'
    t.check('other session begins genuine first publication after cancellation',
            query(b,statement), [(201,2010),(202,None)])
    query(b,'COMMIT')
    t.groups = [[{'sql':statement.replace('biz.orders','{table}'),'params':[]}]]
    t.heads = [t.snapshot()['snapshot_id']]
    expected = [{'id':201,'val':2010},{'id':202,'val':None}]
    t.publication(expected,2,2,1)
    t.check('cancelled attempt consumes no permanent identity range',
            sorted(row['_ducklake_row_id'] for row in t.physical()), [0,1])
    t.check('only the successful retry has an ordinary heap marker',
            query(observer,'SELECT * FROM biz.heap_marker ORDER BY id'), [(2,222)])
    t.expect(a, expected, 'previously cancelled connection reads the new committed first batch')
    t.reverse(expected, 'first-success-after-cancel')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-id',default=os.getenv('OG_LAKE_RUN_ID'))
    parser.add_argument('--output',default=os.getenv('OG_LAKE_RESULT_FILE'))
    parser.add_argument('--manifest',default=os.getenv('OG_LAKE_FIXTURE_MANIFEST'))
    args = parser.parse_args()
    if not all((args.run_id,args.output,args.manifest)):
        parser.error('run-id, output and genuine EMPTY_TABLE manifest are required')
    validate_run_id(args.run_id);validate_case_id(CASE_ID)
    args.output=Path(args.output).resolve();args.output.parent.mkdir(parents=True,exist_ok=True)
    args.manifest=str(Path(args.manifest).resolve())
    result={'status':'fail','scope':CASE_ID,'assertions':[],'artifacts':[],'notes':[
        'Actual production SQL and relation-lock barrier only; no extra product hook or pre-created statistics.',
        'Rollback compares committed/MVCC-visible protocol state, not physical page bytes or vacuum reclamation.',
        'Only an existing pure-inline EMPTY_TABLE; no file SDK, RAC, multi-table or simultaneous waiting-successor qualification.']}
    owned = None
    try:
        imported = Path(empty_inline_insert.__file__).resolve()
        expected_import = (DRIVER/'tests/empty_inline_insert.py').resolve()
        if imported != expected_import:
            raise RuntimeError('Case helper must be imported from the frozen formal driver path')
        publisher = Path(os.getenv('OG_LAKE_SOURCE',str(WORKSPACE/'.worktrees/opengauss-native-lake'))) / 'contrib/og_lake/lake_publish.cpp'
        if not publisher.is_file():
            raise Blocked('Actual publisher source required for the lock-boundary evidence')
        result['input_hashes']={str(imported):digest(imported),str(publisher):digest(publisher)}
        archived = args.output.parent/'cancel-inputs'
        archived.mkdir(exist_ok=False)
        for original in (imported,publisher):
            copy = archived/original.name
            shutil.copyfile(original,copy)
            if digest(copy)!=result['input_hashes'][str(original)]:
                raise RuntimeError('Input source changed during evidence archiving')
            result['artifacts'].append({'path':str(copy)})
        with OwnedDatabase(args.run_id,args.output.parent) as owned:
            case = empty_inline_insert.Case(owned,args,result)
            scenario(case)
            for path,value in result['input_hashes'].items():
                case.check('imported helper and publisher source remain frozen/'+Path(path).name,digest(path),value)
            result['status']='pass'
    except Blocked as exc:
        result.update(status='blocked',reason=str(exc))
    except BaseException as exc:
        result.update(status='fail',error=repr(exc),traceback=traceback.format_exc())
        if not result['assertions'] or result['assertions'][-1]['status']!='fail':
            result['assertions'].append({'name':'partial first-publication cancellation scenario completed','status':'fail','details':repr(exc)})
    finally:
        if owned is not None:
            result['artifacts'].extend(owned.artifacts());result['cleanup']=owned.cleanup_result
        write_json(args.output,result)
        print(json.dumps({'status':result['status'],'output':str(args.output)}))
    return {'pass':0,'blocked':2}.get(result['status'],1)


if __name__=='__main__':
    raise SystemExit(main())
