#!/usr/bin/env python3
"""Real native INSERT acceptance for the bounded 0.3 implementation."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import sys
import threading
import time
import traceback

DRIVER = Path(os.getenv('OG_LAKE_DRIVER_ROOT', str(Path(__file__).resolve().parents[1])))
sys.path.insert(0, str(DRIVER))
sys.path.insert(0, str(DRIVER / 'tests'))
from inline_fixture import foreign_table_statement, multiset, query, scalar
from native_test_context import OwnedDatabase, load_reference, reviewed_runtime_version
from lake_harness import Blocked, read_json, validate_run_id, write_json


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--run-id', default=os.getenv('OG_LAKE_RUN_ID'))
    p.add_argument('--output', default=os.getenv('OG_LAKE_RESULT_FILE'))
    p.add_argument('--manifest', default=os.getenv('OG_LAKE_FIXTURE_MANIFEST'))
    p.add_argument('--mode', choices=['commands', 'savepoints', 'atomic', 'concurrency', 'contention', 'publish_cancel', 'visibility', 'zero', 'memory', 'cursors', 'interread', 'typed'], required=True)
    args = p.parse_args()
    if not args.run_id or not args.output or not args.manifest:
        p.error('run-id, output and genuine reference manifest are required')
    validate_run_id(args.run_id)
    output = Path(args.output).resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    result = {'status': 'fail', 'scope': 'native_inline_insert/' + args.mode, 'assertions': [], 'artifacts': [],
              'notes': ['Business expected rows are maintained independently of catalog publication SQL.',
                        'Only INSERT is exercised. No UPDATE/DELETE, file writes, SDK or RAC support is implied.',
                        'Read-committed lake reads pin their first catalog snapshot inside an explicit transaction as required by section 4.8.1.']}
    db = None

    def check(name, actual, expected=True):
        result['assertions'].append({'name': name, 'status': 'pass' if actual == expected else 'fail',
                                     'details': {'actual': actual, 'expected': expected}})
        if actual != expected:
            raise AssertionError(name)

    try:
        from psycopg2 import sql
        raw = read_json(args.manifest)
        expected_profile = 'LONG_TEXT' if args.mode == 'memory' else 'TYPED' if args.mode == 'typed' else 'I'
        if raw['case'] != expected_profile:
            raise Blocked('This mode requires its exact real reference profile')
        with OwnedDatabase(args.run_id, output.parent) as db:
            a, b, c = [db.connect() for unused in range(3)]
            query(a, 'CREATE EXTENSION og_lake VERSION \'0.3\'')
            caps = json.loads(scalar(a, 'SELECT og_lake.host_status()'))
            runtime_version = reviewed_runtime_version()
            if caps.get('module_version') != runtime_version or caps.get('inline_insert') is not True:
                raise Blocked('Native INSERT runtime must exactly match the reviewed source control version')
            check('N3 tests keep installed SQL objects at 0.3 for compatibility',
                  scalar(a, "SELECT extversion FROM pg_extension WHERE extname='og_lake'"), '0.3')
            result['runtime_version'] = runtime_version
            result['installed_extension_version'] = '0.3'
            manifest, exported, counts, artifacts = load_reference(a, args.manifest, args.run_id, output.parent)
            result['artifacts'] += artifacts
            check('every genuine catalog table was imported', len(counts), len(exported['tables']))
            query(a, 'CREATE SCHEMA biz')
            query(a, 'SELECT og_lake.attach_catalog(%s::name,%s::name,NULL::text,false)', ('lake_reference', 'lake_metadata'))
            query(a, foreign_table_statement('biz', 'orders', 'lake_reference', manifest['table_id'], manifest['schema']))
            query(a, 'CREATE TABLE biz.heap_control (id integer PRIMARY KEY,val integer)')
            query(a, 'INSERT INTO biz.heap_control VALUES (1,10)')
            initial = [[r['id'], r['val']] for r in manifest['expected']]

            def rows(conn):
                return [list(row) for row in query(conn, 'SELECT id,val FROM biz.orders')]

            def expect(conn, wanted, name):
                check(name, multiset(rows(conn)), multiset(wanted))

            def catalog_image(conn):
                # This is a full physical-value oracle for no-op/rollback checks,
                # not an assumed count/statistics formula.
                return {t['table_name']: sorted([repr(tuple(row)) for row in query(conn,
                        sql.SQL('SELECT * FROM lake_metadata.{}').format(sql.Identifier(t['table_name'])))])
                        for t in exported['tables']}

            def counter(conn):
                return scalar(conn, 'SELECT next_row_id FROM lake_metadata.ducklake_table_stats WHERE table_id=%s', (manifest['table_id'],))

            initial_catalog = catalog_image(a)
            initial_counter = counter(a)
            expect(a, initial, 'initial native result equals the actual reference')

            if args.mode in ('commands', 'interread'):
                query(a, 'BEGIN')
                first = [[101, 1001], [102, 1002]]
                returning = query(a, 'INSERT INTO biz.orders(id,val) VALUES(101,1001),(102,1002) RETURNING id,val')
                check('VALUES RETURNING exposes actual business values', multiset(returning), multiset(first))
                current = initial + first
                expect(a, current, 'next command reads its own previous writes')
                expect(b, initial, 'another transaction cannot see staged rows')
                second = [[r[0] + 1000, r[1] + 7] for r in first]
                returning = query(a, 'INSERT INTO biz.orders(id,val) SELECT id+1000,val+7 FROM biz.orders WHERE id>=100 RETURNING id,val')
                check('INSERT SELECT consumes previous command writes', multiset(returning), multiset(second))
                current += second
                source = [list(r) for r in current]
                third = [[r[0] + 10000, r[1]] for r in source]
                returning = query(a, 'INSERT INTO biz.orders(id,val) SELECT id+10000,val FROM biz.orders RETURNING id,val')
                check('self INSERT SELECT consumes each pre-command row once without feedback', multiset(returning), multiset(third))
                current += third
                expect(a, current, 'complete staged command result is correct')
                query(a, 'COMMIT')
                expect(b, current, 'all business rows become visible together after commit')
                check('one permanent identity per newly inserted row', counter(b), initial_counter + len(current) - len(initial))
                if args.mode == 'interread':
                    from reverse_reference import reverse_reference
                    reference = reverse_reference(c, 'lake_metadata', args.manifest,
                        output.parent / 'reference-export-current.json',
                        [{'id': row[0], 'val': row[1]} for row in current],
                        output.parent / 'reverse-reference', expected_commit_count=1)
                    check('fixed real reference reads and continues native publications', reference['status'], 'pass')
                    result['reference_assertions'] = reference['assertions']
                    result['artifacts'].extend({'path': str(path)} for path in (output.parent / 'reverse-reference').rglob('*') if path.is_file())

            elif args.mode == 'typed':
                names = [column['name'] for column in manifest['schema']]
                additions = [
                    {'id':1001,'val':2147483647,'flag':True,'bigval':9223372036854775807,'note':"English\n中文 'quote' \\backslash"},
                    {'id':1002,'val':-2147483648,'flag':False,'bigval':-9223372036854775808,'note':''},
                    {'id':1003,'val':None,'flag':None,'bigval':None,'note':None},
                ]
                projection = sql.SQL(',').join(map(sql.Identifier,names))
                select = sql.SQL('SELECT {} FROM biz.orders').format(projection)
                old_values = [[row[name] for name in names] for row in manifest['expected']]
                new_values = [[row[name] for name in names] for row in additions]
                query(a,'BEGIN')
                returned = []
                for row in additions:
                    returned += query(a,'INSERT INTO biz.orders(id,val,flag,bigval,note) VALUES(%s,%s,%s,%s,%s) RETURNING id,val,flag,bigval,note',
                                      (row['id'],row['val'],row['flag'],row['bigval'],row['note']))
                check('typed RETURNING preserves integer limits bool NULL empty text and UTF-8',multiset(returned),multiset(new_values))
                check('typed prior-command overlay values are exact',multiset(query(a,select)),multiset(old_values+new_values))
                check('typed overlay remains private until commit',multiset(query(b,select)),multiset(old_values))
                query(a,'COMMIT')
                check('typed committed values are exact',multiset(query(c,select)),multiset(old_values+new_values))
                from reverse_reference import reverse_reference
                reference=reverse_reference(c,'lake_metadata',args.manifest,output.parent/'reference-export-current.json',
                    manifest['expected']+additions,output.parent/'reverse-reference',expected_commit_count=1)
                check('fixed reference accepts typed native values and statistics',reference['status'],'pass')
                result['reference_assertions']=reference['assertions']
                result['artifacts'].extend({'path':str(path)} for path in (output.parent/'reverse-reference').rglob('*') if path.is_file())

            elif args.mode == 'savepoints':
                query(a, 'BEGIN')
                query(a, 'INSERT INTO biz.orders(id,val) VALUES(201,2010)')
                query(a, 'SAVEPOINT outer_sp')
                query(a, 'INSERT INTO biz.orders(id,val) VALUES(202,2020)')
                query(a, 'SAVEPOINT inner_sp')
                query(a, 'INSERT INTO biz.orders(id,val) VALUES(203,2030)')
                query(a, 'RELEASE SAVEPOINT inner_sp')
                expect(a, initial + [[201,2010],[202,2020],[203,2030]], 'released child writes remain visible in parent')
                query(a, 'ROLLBACK TO SAVEPOINT outer_sp')
                expect(a, initial + [[201,2010]], 'outer rollback removes released nested child writes')
                query(a, 'INSERT INTO biz.orders(id,val) VALUES(204,2040)')
                query(a, 'ROLLBACK TO SAVEPOINT outer_sp')
                expect(a, initial + [[201,2010]], 'reused savepoint removes only its replacement child writes')
                query(a, 'RELEASE SAVEPOINT outer_sp')
                expect(b, initial, 'savepoint release does not publish')
                query(a, 'COMMIT')
                expect(b, initial + [[201,2010]], 'only surviving parent change commits')
                check('rolled back child rows do not consume permanent identities', counter(b), initial_counter + 1)
                before = catalog_image(b)
                query(a, 'BEGIN')
                query(a, 'INSERT INTO biz.orders(id,val) VALUES(205,2050)')
                query(a, 'ROLLBACK')
                expect(b, initial + [[201,2010]], 'top-level rollback removes its business changes')
                check('top-level rollback preserves all protocol rows', catalog_image(b), before)

            elif args.mode == 'atomic':
                query(a, 'BEGIN')
                query(a, 'UPDATE biz.heap_control SET val=20 WHERE id=1')
                query(a, 'INSERT INTO biz.orders(id,val) VALUES(301,3010)')
                check('other connection retains committed heap value', scalar(b, 'SELECT val FROM biz.heap_control WHERE id=1'), 10)
                expect(b, initial, 'other connection retains committed lake value')
                query(a, 'ROLLBACK')
                check('heap rollback is atomic with lake rollback', scalar(b, 'SELECT val FROM biz.heap_control WHERE id=1'), 10)
                expect(b, initial, 'lake rows rolled back with heap')
                check('mixed rollback changes no protocol records', catalog_image(b), initial_catalog)
                query(a, 'BEGIN')
                query(a, 'UPDATE biz.heap_control SET val=30 WHERE id=1')
                query(a, 'INSERT INTO biz.orders(id,val) VALUES(302,3020)')
                query(a, 'COMMIT')
                query(b, 'BEGIN ISOLATION LEVEL READ COMMITTED')
                check('one observer snapshot sees committed heap', scalar(b, 'SELECT val FROM biz.heap_control WHERE id=1'), 30)
                expect(b, initial + [[302,3020]], 'same observer snapshot sees committed lake')
                query(b, 'COMMIT')

            elif args.mode in ('concurrency','contention'):
                observer = db.connect() if args.mode=='contention' else c
                writer_pids = [scalar(conn,'SELECT pg_backend_pid()') for conn in (a,b)]
                snapshot_oid = None
                if args.mode=='contention':
                    if scalar(a,'SHOW enable_thread_pool')=='on':
                        raise Blocked('The deterministic pg_locks PID barrier is currently validated only for dedicated sessions')
                    snapshot_oid=scalar(observer,"SELECT 'lake_metadata.ducklake_snapshot'::regclass::oid")
                    query(c,'BEGIN')
                    query(c,'LOCK TABLE lake_metadata.ducklake_snapshot IN SHARE MODE')
                barrier = threading.Barrier(2)
                def worker(conn, identifier):
                    query(conn, 'BEGIN')
                    query(conn, 'INSERT INTO biz.orders(id,val) VALUES(%s,%s)', (identifier, identifier*10))
                    barrier.wait(timeout=10)
                    query(conn, 'COMMIT')
                    return identifier
                with ThreadPoolExecutor(max_workers=2) as pool:
                    future1 = pool.submit(worker, a, 501)
                    future2 = pool.submit(worker, b, 502)
                    if args.mode=='contention':
                        try:
                            deadline=time.monotonic()+12
                            while True:
                                waiting=query(observer,"SELECT pid,mode FROM pg_locks WHERE relation=%s "
                                    "AND pid IN (%s,%s) AND NOT granted AND mode='RowExclusiveLock'",
                                    (snapshot_oid,writer_pids[0],writer_pids[1]))
                                if {row[0] for row in waiting}==set(writer_pids):break
                                if future1.done() or future2.done():
                                    raise AssertionError('A publisher finished before the controlled claim barrier')
                                if time.monotonic()>=deadline:
                                    raise AssertionError('Both publishers did not reach the actual snapshot INSERT relation lock')
                                time.sleep(0.02)
                            result['observed_claim_waiters']=[list(row) for row in waiting]
                            check('both publishers reached the real snapshot claim lock',len(waiting),2)
                            check('no publisher became visible before releasing the shared relation lock',catalog_image(observer),initial_catalog)
                        finally:
                            query(c,'ROLLBACK')
                    check('both real concurrent writers commit', sorted([future1.result(timeout=45), future2.result(timeout=45)]), [501,502])
                expect(observer, initial + [[501,5010],[502,5020]], 'both concurrent committed business rows survive')
                check('concurrent commits allocate distinct permanent identities', counter(observer), initial_counter + 2)
                names = [r['table_name'] for t in exported['tables'] if t['table_name']=='ducklake_inlined_data_tables' for r in t['rows']]
                ids = []
                for name in names:
                    ids += [r[0] for r in query(observer, sql.SQL('SELECT _ducklake_row_id FROM lake_metadata.{}').format(sql.Identifier(name)))]
                check('physical inline logical IDs remain unique in this insert-only fixture', len(ids), len(set(ids)))
                if args.mode=='contention':
                    original_snapshots=next(len(t['rows']) for t in exported['tables'] if t['table_name']=='ducklake_snapshot')
                    check('both controlled claim contenders publish exactly one snapshot',
                          scalar(observer,'SELECT count(*) FROM lake_metadata.ducklake_snapshot'),original_snapshots+2)
                    result['notes'].append('Both RowExclusive waits follow reading the old snapshot in the pinned publisher code; the SHARE lock blocks both claims while leaving those SELECTs available. This is a real lock barrier, not a sleep-based race.')

            elif args.mode == 'publish_cancel':
                if scalar(a,'SHOW enable_thread_pool')=='on':
                    raise Blocked('This exact transaction-lock/PID cancellation barrier currently requires dedicated sessions')
                publisher_pid=scalar(a,'SELECT pg_backend_pid()')
                holder_pid=scalar(b,'SELECT pg_backend_pid()')
                column_id=scalar(b,'SELECT min(column_id) FROM lake_metadata.ducklake_table_column_stats WHERE table_id=%s',(manifest['table_id'],))
                check('genuine fixture contains a column-statistics row for the lock barrier',column_id is not None)
                query(a,'SET statement_timeout=30000')
                query(a,'BEGIN')
                query(a,'UPDATE biz.heap_control SET val=50 WHERE id=1')
                query(a,'INSERT INTO biz.orders(id,val) VALUES(951,9510)')
                query(b,'BEGIN')
                locked=query(b,'UPDATE lake_metadata.ducklake_table_column_stats SET min_value=min_value '
                               'WHERE table_id=%s AND column_id=%s RETURNING column_id',(manifest['table_id'],column_id))
                check('barrier locks one real existing statistics row without changing its value',locked,[(column_id,)])
                with ThreadPoolExecutor(max_workers=1) as pool:
                    future=pool.submit(query,a,'COMMIT')
                    try:
                        deadline=time.monotonic()+12
                        while True:
                            waits=query(c,"SELECT w.pid,w.transactionid,h.pid FROM pg_locks w JOIN pg_locks h "
                                "ON h.locktype=w.locktype AND h.transactionid=w.transactionid "
                                "WHERE w.pid=%s AND h.pid=%s AND w.locktype='transactionid' AND NOT w.granted AND h.granted",
                                (publisher_pid,holder_pid))
                            if waits:break
                            if future.done():
                                raise AssertionError('Publisher finished before reaching the locked column-statistics update')
                            if time.monotonic()>=deadline:
                                raise AssertionError('Publisher was not observed waiting on the exact holder transaction')
                            time.sleep(0.02)
                        result['observed_statistics_row_wait']=[list(row) for row in waits]
                        check('uncommitted partial publication remains invisible in every protocol table',catalog_image(c),initial_catalog)
                        expect(c,initial,'partial native publication exposes no business row')
                        check('ordinary heap change is also still invisible',scalar(c,'SELECT val FROM biz.heap_control WHERE id=1'),10)
                        check('cancel targets the proven publisher backend',scalar(c,'SELECT pg_cancel_backend(%s)',(publisher_pid,)),True)
                        try:
                            future.result(timeout=15)
                        except Exception as exc:
                            check('publication fails with actual cancellation',getattr(exc,'pgcode',None),'57014')
                            result['publication_cancel_message']=str(exc)
                        else:
                            check('publication cancellation is not ignored','COMMIT unexpectedly succeeded','57014')
                    finally:
                        query(b,'ROLLBACK')
                check('all protocol values roll back after mid-publication cancellation',catalog_image(c),initial_catalog)
                expect(c,initial,'inline business write rolls back after cancellation')
                check('ordinary heap rolls back with the lake publication',scalar(c,'SELECT val FROM biz.heap_control WHERE id=1'),10)
                check('same publisher session remains usable',scalar(a,'SELECT 42'),42)
                query(a,'BEGIN')
                query(a,'UPDATE biz.heap_control SET val=60 WHERE id=1')
                query(a,'INSERT INTO biz.orders(id,val) VALUES(952,9520)')
                query(a,'COMMIT')
                expect(c,initial+[[952,9520]],'same session publishes a subsequent valid transaction')
                check('subsequent ordinary heap update commits',scalar(c,'SELECT val FROM biz.heap_control WHERE id=1'),60)
                check('cancelled publication consumed no permanent identity',counter(c),initial_counter+1)
                result['notes'].append('Pinned publisher order inserts inline rows and updates table statistics before updating column statistics. The observed holder-transaction wait therefore pauses an actual partial publication; the holder always rolls back its same-value UPDATE.')

            elif args.mode == 'visibility':
                query(a, 'BEGIN ISOLATION LEVEL READ COMMITTED')
                expect(a, initial, 'explicit RC binds its first lake business snapshot')
                query(b, 'BEGIN')
                query(b, 'UPDATE biz.heap_control SET val=40 WHERE id=1')
                query(b, 'INSERT INTO biz.orders(id,val) VALUES(401,4010)')
                query(b, 'COMMIT')
                check('ordinary heap uses the next RC statement snapshot', scalar(a, 'SELECT val FROM biz.heap_control WHERE id=1'), 40)
                expect(a, initial, 'same explicit RC lake transaction retains its fixed S_read')
                query(a, 'INSERT INTO biz.orders(id,val) VALUES(402,4020)')
                expect(a, initial + [[402,4020]], 'fixed lake base combines with this transaction own previous command')
                query(a, 'COMMIT')
                expect(c, initial + [[401,4010],[402,4020]], 'new transaction sees both independent committed inserts')

            elif args.mode == 'zero':
                query(a, 'INSERT INTO biz.orders(id,val) SELECT 999,9990 WHERE false')
                check('zero-row autocommit creates no snapshot or protocol change', catalog_image(b), initial_catalog)
                query(a, 'BEGIN')
                query(a, 'SAVEPOINT discarded')
                query(a, 'INSERT INTO biz.orders(id,val) VALUES(601,6010)')
                query(a, 'ROLLBACK TO SAVEPOINT discarded')
                query(a, 'RELEASE SAVEPOINT discarded')
                query(a, 'COMMIT')
                check('fully rolled back write-set creates no snapshot or protocol change', catalog_image(b), initial_catalog)
                expect(b, initial, 'zero-row and discarded writes preserve business rows')

            elif args.mode == 'cursors':
                query(a, 'BEGIN')
                parent_rows = [[801,8010],[802,8020]]
                query(a, 'INSERT INTO biz.orders(id,val) VALUES(801,8010),(802,8020)')
                query(a, 'DECLARE parent_cursor NO SCROLL CURSOR FOR SELECT id,val FROM biz.orders')
                query(a, 'SAVEPOINT child_rows')
                query(a, 'INSERT INTO biz.orders(id,val) VALUES(803,8030)')
                prefix = [list(r) for r in query(a, 'FETCH FORWARD %s FROM parent_cursor', (len(initial)+2,))]
                check('parent cursor returns only its command-visible rows before rollback',
                      multiset(prefix), multiset(initial + parent_rows))
                query(a, 'ROLLBACK TO SAVEPOINT child_rows')
                query(a, 'RELEASE SAVEPOINT child_rows')
                check('parent cursor safely resumes after its cached child successor was freed',
                      query(a, 'FETCH ALL FROM parent_cursor'), [])
                query(a, 'CLOSE parent_cursor')
                expect(a, initial + parent_rows, 'rollback and cursor close preserve parent writes')
                query(a, 'ROLLBACK')
                # VOLATILE SQL functions avoid normal SQL-function inlining.
                # The target-list SRF is eligible for lazy value-per-call work.
                query(a, 'CREATE TABLE biz.srf_heap(id integer)')
                for row in initial:
                    query(a, 'INSERT INTO biz.srf_heap VALUES(%s)', (row[0],))
                query(a, 'CREATE FUNCTION biz.lake_srf() RETURNS SETOF integer LANGUAGE SQL VOLATILE AS $$ SELECT id FROM biz.orders $$')
                query(a, 'CREATE FUNCTION biz.heap_srf() RETURNS SETOF integer LANGUAGE SQL VOLATILE AS $$ SELECT id FROM biz.srf_heap $$')
                observed = {}
                lifecycle = {}
                def table_count():
                    return json.loads(scalar(a,'SELECT og_lake.host_status()'))['transaction_tables']
                for source in ('heap','lake'):
                    query(a, 'BEGIN')
                    query(a, sql.SQL('DECLARE lazy_cursor NO SCROLL CURSOR FOR SELECT biz.{}_srf()').format(sql.SQL(source)))
                    counts = {'after_declare':table_count()}
                    check(source+' lazy SRF has not begun a lake reader at DECLARE',counts['after_declare'],0)
                    query(a, 'SAVEPOINT first_fetch')
                    first = query(a, 'FETCH 1 FROM lazy_cursor')
                    counts['after_child_fetch']=table_count()
                    if source=='lake':
                        check('lake lazy SRF first initializes its table inside the child',counts['after_child_fetch']>0)
                    query(a, 'ROLLBACK TO SAVEPOINT first_fetch')
                    query(a, 'RELEASE SAVEPOINT first_fetch')
                    counts['after_child_rollback']=table_count()
                    check(source+' lazy SRF has no retained child lake table after rollback',counts['after_child_rollback'],0)
                    remainder = query(a, 'FETCH ALL FROM lazy_cursor')
                    counts['after_continued_fetch']=table_count()
                    if source=='lake':
                        check('continued parent FETCH reconstructs the lake reader after fcache invalidation',counts['after_continued_fetch']>0)
                    observed[source] = {'first':[list(r) for r in first], 'remainder':[list(r) for r in remainder],
                                        'complete':[list(r) for r in first+remainder]}
                    lifecycle[source]=counts
                    query(a, 'CLOSE lazy_cursor')
                    query(a, 'ROLLBACK')
                result['lazy_srf_cursor_rows'] = observed
                result['lazy_srf_transaction_table_lifecycle']=lifecycle
                check('lake lazy SRF cursor matches the ordinary heap lifecycle control',
                      observed['lake'], observed['heap'])
                check('lake and heap lazy SRF complete multisets including repeats agree',
                      multiset(observed['lake']['complete']),multiset(observed['heap']['complete']))
                check('first FETCH delivers one actual initial row',
                      len(observed['lake']['first'])==1 and observed['lake']['first'][0] in [[r[0]] for r in initial])
                check('after child rollback native SQL fcache restarts the complete SRF input',
                      multiset(observed['lake']['remainder']),multiset([[r[0]] for r in initial]))
                result['notes'].append('This actual host invalidates the SQL-function fcache when its initializing subtransaction aborts. Continued FETCH restarts that SRF input, including the previously returned row; heap and lake sequences must agree. This case does not claim generic exactly-once cursor semantics.')
                for source in ('heap','lake'):
                    query(a, 'BEGIN')
                    query(a, sql.SQL('DECLARE lazy_cursor NO SCROLL CURSOR FOR SELECT biz.{}_srf()').format(sql.SQL(source)))
                    check(source+' close-only DECLARE has no lake reader',table_count(),0)
                    query(a, 'SAVEPOINT close_after_rollback')
                    query(a, 'FETCH 1 FROM lazy_cursor')
                    if source=='lake':
                        check('close-only lake reader is initialized in child',table_count()>0)
                    query(a, 'ROLLBACK TO SAVEPOINT close_after_rollback')
                    check(source+' close-only child lake table is reclaimed',table_count(),0)
                    query(a, 'CLOSE lazy_cursor')
                    check(source+' closing the surviving parent cursor retains no child lake table',table_count(),0)
                    query(a, 'ROLLBACK')
                check('session remains healthy after lazy SRF close paths', scalar(a, 'SELECT 42'), 42)
                check('cursor tests publish no lake changes', catalog_image(b), initial_catalog)

            else:
                pool_enabled = scalar(a,'SHOW enable_thread_pool') == 'on'
                def memory(require_context=False):
                    if pool_enabled:
                        # The host view formats a seconds-based start time;
                        # pg_current_sessionid() embeds the raw TimestampTz.
                        # Match both the native logical ID and currently
                        # attached worker inside this same SQL statement.
                        captured = query(a,"SELECT sessid,threadid,contextname,usedsize,"
                                           "pg_current_sessionid(),pg_current_sessid(),pg_backend_pid() "
                                           "FROM pv_session_memory_detail() "
                                           "WHERE split_part(sessid,'.',2)::bigint=pg_current_sessid() "
                                           "AND threadid=pg_backend_pid() AND contextname LIKE 'Native lake%'")
                        observed = [(row[2],row[3]) for row in captured]
                        if require_context:
                            result.setdefault('session_memory_identity_samples',[]).append([
                                {'view_sessid':row[0],'view_threadid':row[1],'contextname':row[2],
                                 'usedsize':row[3],'raw_current_sessionid':row[4],
                                 'logical_session_id':row[5],'current_thread_id':row[6]}
                                for row in captured])
                    else:
                        # The view's threadid is a start-time/id composite; tid
                        # is the exact native backend identity in dedicated mode.
                        observed = query(a,"SELECT contextname,usedsize FROM pv_thread_memory_detail() "
                                          "WHERE tid=pg_backend_pid() AND contextname LIKE 'Native lake%'")
                    total = sum(int(row[1]) for row in observed)
                    if require_context:
                        check('independent host view actually observes native lake contexts',len(observed)>0 and total>0)
                    return total
                def diagnostic():
                    return json.loads(scalar(a,'SELECT og_lake.host_status()'))
                query(a, 'BEGIN')
                samples = []
                for index in range(200):
                    query(a, 'SAVEPOINT discarded')
                    expect(a, initial, 'savepoint reader business values ' + str(index))
                    if index == 0:
                        memory(True)
                        check('reader diagnostic confirms a live table binding',diagnostic()['transaction_tables']>0)
                    query(a, 'ROLLBACK TO SAVEPOINT discarded')
                    query(a, 'RELEASE SAVEPOINT discarded')
                    if index in (9,99,199): samples.append(int(memory()))
                result['savepoint_read_context_bytes'] = samples
                check('savepoint read rollback does not accumulate private catalog memory', samples[-1] <= samples[0]+65536)
                query(a, 'ROLLBACK')
                note = max((r.get('note') or '' for r in manifest['expected']), key=len) * 64
                query(a, 'BEGIN')
                for index in range(40):
                    query(a, 'SAVEPOINT discarded')
                    query(a, 'INSERT INTO biz.orders(id,val,note) VALUES(%s,%s,%s)', (700+index,index,note))
                    if index == 0:
                        actual_bytes=memory(True)
                        live=diagnostic()
                        result['active_large_row_observation']={'independent_used_bytes':actual_bytes,'host':live}
                        check('independent context usage includes actual staged text',actual_bytes>=len(note.encode('utf-8')))
                        check('active large row is charged and counted',live['transaction_rows']==1 and live['staged_bytes']>=len(note.encode('utf-8')))
                        check('host total allocation is consistent with charged row bytes',live['transaction_memory_bytes']>=live['staged_bytes'])
                    query(a, 'ROLLBACK TO SAVEPOINT discarded')
                    query(a, 'RELEASE SAVEPOINT discarded')
                    after=diagnostic()
                    check('rollback restores charged bytes and row count '+str(index),
                          [after['staged_bytes'],after['transaction_rows']],[0,0])
                check('repeated discarded large rows restore the active business state', multiset(rows(a)), multiset(initial))
                result['discarded_large_rows_context_bytes'] = int(memory())
                check('large discarded row contexts are reclaimed', result['discarded_large_rows_context_bytes'] < 1024*1024)
                query(a,"INSERT INTO biz.orders(id,val,note) VALUES(901,9010,'valid after repeated rollback')")
                query(a, 'COMMIT')
                expect(b,initial+[[901,9010]],'same outer transaction can commit a small write after repeated rollback')
                check('discarded large rows do not consume identities',counter(b),initial_counter+1)
                before_rejection=catalog_image(b)
                query(a,'BEGIN')
                query(a,'SAVEPOINT over_limit')
                try:
                    query(a,'INSERT INTO biz.orders(id,val,note) SELECT 1000+g,g,%s FROM generate_series(1,40) t(g)',(note,))
                except Exception as exc:
                    check('actual active write-set above 64 MiB is rejected',getattr(exc,'pgcode',None),'54000')
                else:
                    check('actual active write-set above 64 MiB is rejected','unexpected success','54000')
                query(a,'ROLLBACK TO SAVEPOINT over_limit')
                query(a,'RELEASE SAVEPOINT over_limit')
                after=diagnostic()
                check('failed oversized statement restores charged bytes and rows',[after['staged_bytes'],after['transaction_rows']],[0,0])
                check('oversized failure is invisible in all protocol tables',catalog_image(b),before_rejection)
                check('oversized failure private memory is reclaimed',memory()<1024*1024)
                query(a,"INSERT INTO biz.orders(id,val,note) VALUES(902,9020,'valid after over-limit error')")
                query(a,'COMMIT')
                expect(b,initial+[[901,9010],[902,9020]],'same session commits after the real memory-limit error')
                check('only two surviving small rows receive permanent identities',counter(b),initial_counter+2)
            result['status'] = 'pass'
    except Blocked as exc:
        result.update(status='blocked', reason=str(exc))
    except BaseException as exc:
        result.update(status='fail', error=repr(exc), traceback=traceback.format_exc())
        if not result['assertions'] or result['assertions'][-1]['status']!='fail':
            result['assertions'].append({'name':'native INSERT scenario completed','status':'fail','details':repr(exc)})
    finally:
        if db is not None:
            result['artifacts'] += db.artifacts()
            result['cleanup'] = db.cleanup_result
        write_json(output,result)
        print(json.dumps({'status':result['status'],'mode':args.mode,'output':str(output)},ensure_ascii=False))
    return {'pass':0,'blocked':2}.get(result['status'],1)


if __name__=='__main__':
    raise SystemExit(main())
