#!/usr/bin/env python3
"""Real EMPTY_TABLE first-publication and rollback tests.

Positive inputs come only from fixed DuckLake CREATE + complete catalog export.
The first mode's explicitly separated fault section follows genuine interread;
it does not seed statistics to make initialization succeed.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import sys
import threading
import time
import traceback

WORKSPACE = Path('/home/qiumc/data/code/cpp')
DRIVER = Path(os.getenv('OG_LAKE_DRIVER_ROOT', str(WORKSPACE / 'integration/opengauss-native-lake')))
sys.path.insert(0, str(DRIVER))
sys.path.insert(0, str(DRIVER / 'tests'))
from inline_fixture import foreign_table_statement, multiset, query, scalar
from lake_harness import Blocked, validate_case_id, validate_run_id, write_json
from native_test_context import OwnedDatabase, load_reference, reviewed_runtime_version

MODES = ('first', 'net', 'rollback', 'concurrency')


class Case:
    def __init__(self, db, args, result):
        from psycopg2 import sql
        self.sql, self.db, self.args, self.result = sql, db, args, result
        self.a, self.b, self.c = [db.connect() for _ in range(3)]
        runtime = reviewed_runtime_version()
        if runtime not in ('0.6', '0.7', '0.8', '0.9', '0.10', '0.11'):
            raise Blocked('This staged contract is reviewed against runtime 0.6/0.7/0.8/0.9 with installed SQL0.6')
        query(self.a, "CREATE EXTENSION og_lake VERSION '0.6'")
        caps = json.loads(scalar(self.a, 'SELECT og_lake.host_status()'))
        self.check('runtime is exactly the reviewed source version', caps.get('module_version'), runtime)
        for capability in ('inline_insert', 'inline_update', 'inline_delete'):
            self.check('required runtime capability/' + capability, caps.get(capability), True)
        result['runtime_version'] = runtime
        result['installed_extension_version'] = scalar(self.a,
            "SELECT extversion FROM pg_extension WHERE extname='og_lake'")
        self.check('exact SQL extension installation', result['installed_extension_version'], '0.6')
        self.manifest, self.exported, counts, artifacts = load_reference(
            self.a, args.manifest, args.run_id, args.output.parent)
        result['artifacts'].extend(artifacts)
        self.check('all real reference relations imported', len(counts), len(self.exported['tables']))
        self.check('genuine never-inserted reference profile', self.manifest['case'], 'EMPTY_TABLE')
        self.check('reference business rows are empty', self.manifest['expected'], [])
        self.check('inlining enabled by actual reference option',
                   self.manifest['layout_settings']['data_inlining_row_limit'], 100)
        self.check('reviewed two-column business schema', self.manifest['schema'],
                   [{'name': 'id', 'type': 'INTEGER'}, {'name': 'val', 'type': 'INTEGER'}])
        self.table_id = self.manifest['table_id']
        self.names = ['id', 'val']
        mapping = [r for t in self.exported['tables'] if t['table_name'] == 'ducklake_inlined_data_tables'
                   for r in t['rows'] if r['table_id'] == self.table_id]
        self.check('exactly one real empty carrier', len(mapping), 1)
        self.carrier = mapping[0]['table_name']
        self.column_ids = sorted(r['column_id'] for t in self.exported['tables']
                                 if t['table_name'] == 'ducklake_column' for r in t['rows']
                                 if r['table_id'] == self.table_id and r['end_snapshot'] is None)
        query(self.a, 'CREATE SCHEMA biz')
        query(self.a, "SELECT og_lake.attach_catalog('lake_reference','lake_metadata',NULL::text,false)")
        query(self.a, foreign_table_statement('biz', 'orders', 'lake_reference', self.table_id, self.manifest['schema']))
        query(self.a, 'CREATE TABLE biz.heap_marker(id integer PRIMARY KEY,val integer)')
        self.check('all carrier versions initially absent', self.physical(), [])
        self.check('table statistics genuinely absent', self.stats(), [])
        self.check('column statistics genuinely absent', self.column_stats(), [])
        for name in ('ducklake_data_file', 'ducklake_delete_file'):
            self.check('no historical file records/' + name,
                       query(self.c, sql.SQL('SELECT * FROM lake_metadata.{}').format(sql.Identifier(name))), [])
        self.expect(self.a, [], 'native scan accepts genuine uninitialized empty table')
        self.start_snapshot = self.snapshot()
        self.baseline = self.image()
        self.groups, self.heads, self.operations = [], [], None

    def check(self, name, actual, expected=True):
        ok = actual == expected
        self.result['assertions'].append({'name': name, 'status': 'pass' if ok else 'fail',
                                         'details': {'actual': actual, 'expected': expected}})
        if not ok:
            raise AssertionError(name)

    def image(self, conn=None):
        return {t['table_name']: sorted(repr(tuple(row)) for row in query(conn or self.c,
                self.sql.SQL('SELECT * FROM lake_metadata.{}').format(self.sql.Identifier(t['table_name']))))
                for t in self.exported['tables']}

    def snapshot(self):
        row = query(self.c, 'SELECT snapshot_id,schema_version,next_catalog_id,next_file_id '
                           'FROM lake_metadata.ducklake_snapshot ORDER BY snapshot_id DESC LIMIT 1')[0]
        return dict(zip(('snapshot_id', 'schema_version', 'next_catalog_id', 'next_file_id'), row))

    def stats(self):
        return query(self.c, 'SELECT record_count,next_row_id,file_size_bytes '
                            'FROM lake_metadata.ducklake_table_stats WHERE table_id=%s', (self.table_id,))

    def column_stats(self):
        return query(self.c, 'SELECT column_id,min_value,max_value,contains_null,contains_nan '
                            'FROM lake_metadata.ducklake_table_column_stats WHERE table_id=%s ORDER BY column_id',
                     (self.table_id,))

    def physical(self):
        names = ['_ducklake_row_id', '_ducklake_begin_snapshot', '_ducklake_end_snapshot', 'id', 'val']
        rows = query(self.c, self.sql.SQL('SELECT {} FROM lake_metadata.{}').format(
            self.sql.SQL(',').join(map(self.sql.Identifier, names)), self.sql.Identifier(self.carrier)))
        return [dict(zip(names, row)) for row in rows]

    def expect(self, conn, expected, name):
        rows = [dict(zip(self.names, row)) for row in query(conn, 'SELECT id,val FROM biz.orders')]
        self.check(name, multiset(rows), multiset(expected))

    def begin(self):
        query(self.a, 'BEGIN')
        self.operations = []

    def mutate(self, statement, params=None):
        rows = query(self.a, statement, params)
        self.operations.append({'sql': statement.replace('biz.orders', '{table}').replace('%s', '?'),
                                'params': list(params or [])})
        return rows

    def commit(self):
        query(self.a, 'COMMIT')
        self.groups.append(self.operations)
        self.heads.append(self.snapshot()['snapshot_id'])
        self.operations = None

    def publication(self, expected, versions, frontier, commits):
        """Independent counters plus all-version identities; interread checks full metadata."""
        self.expect(self.b, expected, 'committed business rows exact')
        now = self.snapshot()
        self.check('one snapshot per nonempty lake commit', now['snapshot_id'],
                   self.start_snapshot['snapshot_id'] + commits)
        self.check('inline batches advance file allocation frontier', now['next_file_id'],
                   self.start_snapshot['next_file_id'] + commits)
        for key in ('schema_version', 'next_catalog_id'):
            self.check('DML leaves ' + key + ' unchanged', now[key], self.start_snapshot[key])
        rows = self.stats()
        self.check('initialization creates only one table statistics record', len(rows), 1)
        self.check('stored versions and permanent ID frontier', list(rows[0]), [versions, frontier, 0])
        cols = self.column_stats()
        self.check('exactly one statistics record for each genuine column', [r[0] for r in cols], self.column_ids)
        self.check('integer columns have unknown not false NaN flags', [r[4] for r in cols], [None] * len(cols))
        self.check('actual physical stored version count', len(self.physical()), versions)
        live = [r for r in self.physical() if r['_ducklake_end_snapshot'] is None]
        self.check('live row IDs never overlap', len({r['_ducklake_row_id'] for r in live}), len(live))
        self.check('no replacement carrier was created', query(self.c,
            'SELECT table_name FROM lake_metadata.ducklake_inlined_data_tables WHERE table_id=%s',
            (self.table_id,)), [(self.carrier,)])

    def reverse(self, expected, checkpoint='final'):
        from reverse_modify_reference import reverse_modify_reference
        path = self.args.output.parent / ('reverse-reference-' + checkpoint)
        try:
            reply = reverse_modify_reference(self.c, 'lake_metadata', self.args.manifest,
                self.args.output.parent / 'reference-export-current.json', expected, path,
                operations_by_commit=self.groups, expected_commit_snapshots=self.heads)
            self.check('real fixed DuckLake reads and independently replays complete native publication', reply['status'], 'pass')
            self.result.setdefault('reference_assertions', []).extend(reply['assertions'])
            self.result.setdefault('reference_checkpoints', []).append(
                {'name':checkpoint, 'path':str(path), 'assertion_count':len(reply['assertions'])})
        finally:
            # This subdirectory contains immutable per-attempt transport artifacts.
            if path.exists():
                self.result['artifacts'].extend({'path': str(p)} for p in path.rglob('*') if p.is_file())


def first(t):
    t.begin()
    rows = t.mutate('INSERT INTO biz.orders VALUES(11,NULL),(22,NULL),(33,NULL) RETURNING id,val')
    t.check('first INSERT RETURNING preserves NULL and all input values', multiset(rows),
            multiset([(11,None), (22,None), (33,None)]))
    t.expect(t.a, [{'id':11,'val':None}, {'id':22,'val':None}, {'id':33,'val':None}], 'own first write visible before commit')
    t.check('private first INSERT has not synthesized visible statistics', t.image(), t.baseline)
    t.commit()
    expected = [{'id':11,'val':None}, {'id':22,'val':None}, {'id':33,'val':None}]
    t.publication(expected, 3, 3, 1)
    initial_physical = t.physical()
    t.check('first identities start at zero and are contiguous',
            sorted(r['_ducklake_row_id'] for r in initial_physical), [0,1,2])
    t.check('initial physical versions start at the actual first commit',
            sorted({r['_ducklake_begin_snapshot'] for r in initial_physical}), [t.heads[-1]])
    t.check('NULL detection describes the real first batch', [r[3] for r in t.column_stats()], [False,True])
    t.check('first batch populates every column statistics field including NULL-only flags',
            query(t.c,'SELECT table_id,column_id,contains_null,contains_nan,min_value::text,max_value::text,'
                      'extra_stats::text,min_is_exact,max_is_exact FROM lake_metadata.ducklake_table_column_stats '
                      'WHERE table_id=%s ORDER BY column_id',(t.table_id,)),
            [(t.table_id,t.column_ids[0],False,None,'11','33',None,True,True),
             (t.table_id,t.column_ids[1],True,None,None,None,None,None,None)])
    t.reverse(expected, 'first-publication')
    identity = next(r['_ducklake_row_id'] for r in initial_physical if r['id'] == 11)
    t.begin()
    t.check('UPDATE of first-published row returns its new value',
            t.mutate('UPDATE biz.orders SET val=111 WHERE id=11 RETURNING id,val'), [(11,111)])
    t.commit()
    expected[0]['val'] = 111
    t.publication(expected, 4, 3, 2)
    pair = [r for r in t.physical() if r['id'] == 11]
    t.check('UPDATE keeps first-published permanent identity', sorted(r['_ducklake_row_id'] for r in pair), [identity,identity])
    t.check('old version ends exactly when replacement begins',
            sorted(r['_ducklake_end_snapshot'] for r in pair if r['_ducklake_end_snapshot'] is not None), [t.heads[-1]])
    t.reverse(expected, 'after-update')

    # Explicit corruption/admission section AFTER genuine complete interread.
    # Nothing here is used as the positive first-publication input or reference oracle.
    import psycopg2
    baseline = t.image()
    faults = [
        ('missing statistics after real publication', 'DELETE FROM lake_metadata.ducklake_table_stats WHERE table_id=%s', 'XX001'),
        ('NULL frontier is unknown, not zero', 'UPDATE lake_metadata.ducklake_table_stats SET next_row_id=NULL WHERE table_id=%s', 'XX001'),
        ('NULL byte count is unknown, not zero', 'UPDATE lake_metadata.ducklake_table_stats SET file_size_bytes=NULL WHERE table_id=%s', '0A000'),
    ]
    for number, (name, fault, code) in enumerate(faults, 1):
        query(t.a, 'BEGIN')
        query(t.a, fault, (t.table_id,))
        query(t.a, 'INSERT INTO biz.heap_marker VALUES(%s,99)', (number,))
        query(t.a, 'INSERT INTO biz.orders VALUES(99,990)')
        try:
            query(t.a, 'COMMIT')
        except psycopg2.Error as exc:
            t.check(name + ' exact SQLSTATE', exc.pgcode, code)
        else:
            t.check(name + ' must reject publication', 'accepted', code)
        query(t.a, 'ROLLBACK')
        t.check(name + ' rolls back every protocol row', t.image(), baseline)
        t.check(name + ' rolls back ordinary heap atomically', query(t.c, 'SELECT * FROM biz.heap_marker'), [])
        t.check(name + ' session reusable', scalar(t.a, 'SELECT 42'), 42)
    query(t.a, 'BEGIN')
    query(t.a, 'UPDATE lake_metadata.ducklake_table_stats SET record_count=NULL WHERE table_id=%s', (t.table_id,))
    query(t.a, 'INSERT INTO biz.orders VALUES(44,440)')
    query(t.a, 'COMMIT')
    t.check('legitimate UNKNOWN stored count stays UNKNOWN rather than becoming one', t.stats(), [(None,4,0)])
    t.check('actual SQL COUNT remains exact despite UNKNOWN metadata', scalar(t.c,'SELECT count(*) FROM biz.orders'), 4)
    t.check('UNKNOWN count cannot reset the permanent ID frontier',
            [r['_ducklake_row_id'] for r in t.physical() if r['id'] == 44], [3])
    t.result['notes'].append('Full reference interread precedes explicit metadata-fault tests. NULL record_count acceptance is a separate admission test, not a reference-generated protocol state.')


def net(t):
    t.begin()
    t.mutate('INSERT INTO biz.orders VALUES(1,10)')
    t.check('first private UPDATE sees inserted value', t.mutate('UPDATE biz.orders SET val=val+1 WHERE id=1 RETURNING id,val'), [(1,11)])
    t.check('second private UPDATE sees previous command', t.mutate('UPDATE biz.orders SET val=val*2 WHERE id=1 RETURNING id,val'), [(1,22)])
    t.check('DELETE returns the final private value', t.mutate('DELETE FROM biz.orders WHERE id=1 RETURNING id,val'), [(1,22)])
    query(t.a, 'INSERT INTO biz.heap_marker VALUES(1,1)')
    t.expect(t.a, [], 'insert-update-update-delete is already empty within transaction')
    t.commit()
    t.check('net-empty first write changes no protocol row including missing stats', t.image(), t.baseline)
    t.check('ordinary heap still commits in a lake net-empty transaction', query(t.c, 'SELECT * FROM biz.heap_marker'), [(1,1)])
    t.check('net-empty commit publishes no snapshot', t.heads, [t.start_snapshot['snapshot_id']])

    t.begin()
    t.mutate('INSERT INTO biz.orders VALUES(2,20)')
    t.mutate('UPDATE biz.orders SET val=val+1 WHERE id=2')
    t.check('surviving first identity has only the final new value',
            t.mutate('UPDATE biz.orders SET val=val*2 WHERE id=2 RETURNING id,val'), [(2,42)])
    t.commit()
    expected = [{'id':2,'val':42}]
    t.publication(expected, 1, 1, 1)
    t.check('aborted private identity consumed no permanent row ID',
            t.physical(), [{'_ducklake_row_id':0, '_ducklake_begin_snapshot':t.heads[-1],
                            '_ducklake_end_snapshot':None, 'id':2, 'val':42}])
    t.reverse(expected)


def rollback(t):
    for release in (False, True):
        query(t.a, 'BEGIN')
        if release:
            query(t.a, 'SAVEPOINT first_child')
        query(t.a, 'INSERT INTO biz.orders VALUES(1,10)')
        query(t.a, 'INSERT INTO biz.heap_marker VALUES(1,10)')
        if release:
            query(t.a, 'RELEASE SAVEPOINT first_child')
        query(t.a, 'ROLLBACK')
        t.check('top abort restores complete never-inserted metadata/' + str(release), t.image(), t.baseline)
        t.check('top abort restores ordinary heap/' + str(release), query(t.c, 'SELECT * FROM biz.heap_marker'), [])
    t.begin()
    query(t.a, 'SAVEPOINT first_child')
    query(t.a, 'INSERT INTO biz.orders VALUES(2,20)')
    query(t.a, 'INSERT INTO biz.heap_marker VALUES(2,20)')
    query(t.a, 'ROLLBACK TO SAVEPOINT first_child')
    query(t.a, 'RELEASE SAVEPOINT first_child')
    t.expect(t.a, [], 'savepoint abort removes all first private rows')
    t.commit()
    t.check('savepoint-only aborted first write leaves no stats or snapshot', t.image(), t.baseline)
    t.check('savepoint rollback includes heap writes', query(t.c, 'SELECT * FROM biz.heap_marker'), [])

    t.begin()
    t.mutate('INSERT INTO biz.orders VALUES(3,30)')
    query(t.a, 'INSERT INTO biz.heap_marker VALUES(3,30)')
    query(t.a, 'SAVEPOINT changes')
    query(t.a, 'UPDATE biz.orders SET val=300 WHERE id=3')
    query(t.a, 'INSERT INTO biz.orders VALUES(4,40)')
    query(t.a, 'UPDATE biz.heap_marker SET val=300 WHERE id=3')
    query(t.a, 'ROLLBACK TO SAVEPOINT changes')
    query(t.a, 'RELEASE SAVEPOINT changes')
    t.expect(t.a, [{'id':3,'val':30}], 'child abort restores surviving parent private version')
    t.commit()
    expected = [{'id':3,'val':30}]
    t.publication(expected, 1, 1, 1)
    t.check('rollback attempts did not consume row ID zero', t.physical()[0]['_ducklake_row_id'], 0)
    t.check('parent heap and lake both commit original values', query(t.c, 'SELECT * FROM biz.heap_marker'), [(3,30)])
    t.reverse(expected)


def concurrency(t):
    if scalar(t.a, 'SHOW enable_thread_pool') == 'on':
        raise Blocked('Exact PID-based first-claim lock barrier is scoped to dedicated sessions')
    observer = t.db.connect()
    pids = []
    batches = [[(101,1010),(102,1020)], [(201,2010),(202,2020),(203,2030)]]
    operations = []
    for conn, batch in zip((t.a,t.b), batches):
        query(conn, 'SET statement_timeout=45000')
        query(conn, 'BEGIN')
        t.expect(conn, [], 'both first writers acquire the same empty lake read state')
        t.check('both writers see the original catalog snapshot',
                scalar(conn, 'SELECT max(snapshot_id) FROM lake_metadata.ducklake_snapshot'), t.start_snapshot['snapshot_id'])
        pids.append(scalar(conn, 'SELECT pg_backend_pid()'))
        group = []
        for identifier, value in batch:
            query(conn, 'INSERT INTO biz.orders VALUES(%s,%s)', (identifier,value))
            group.append({'sql':'INSERT INTO {table} VALUES(?,?)', 'params':[identifier,value]})
        operations.append(group)
    t.check('writers are distinct actual dedicated sessions', len(set(pids)), 2)
    oid = scalar(observer, "SELECT 'lake_metadata.ducklake_snapshot'::regclass::oid")
    query(t.c, 'BEGIN')
    query(t.c, 'LOCK TABLE lake_metadata.ducklake_snapshot IN SHARE MODE')
    start = threading.Barrier(2)
    def commit(conn):
        start.wait(timeout=10)
        query(conn, 'COMMIT')
        return True
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(commit, conn) for conn in (t.a,t.b)]
        try:
            deadline = time.monotonic() + 12
            while True:
                waits = query(observer, 'SELECT pid,mode FROM pg_locks WHERE relation=%s '
                    "AND pid IN (%s,%s) AND NOT granted AND mode='RowExclusiveLock'", (oid,*pids))
                if {r[0] for r in waits} == set(pids):
                    break
                if any(f.done() for f in futures):
                    raise AssertionError('A publisher returned before both real snapshot INSERT lock waits')
                if time.monotonic() >= deadline:
                    raise AssertionError('Both first publishers failed to reach the exact snapshot claim lock barrier')
                time.sleep(0.02)  # bounded observation polling, never a timing proof
            t.result['observed_claim_waiters'] = [list(r) for r in waits]
            t.check('both claims stop after reading the old head', len({r[0] for r in waits}), 2)
            t.check('no seed stats or data leaks before claim succeeds', t.image(observer), t.baseline)
        finally:
            query(t.c, 'ROLLBACK')
        t.check('both genuine first publishers commit', [f.result(timeout=45) for f in futures], [True,True])
    expected = [{'id':i,'val':v} for batch in batches for i,v in batch]
    t.publication(expected, 5, 5, 2)
    physical = t.physical()
    t.check('retry allocates disjoint contiguous permanent IDs',
            sorted(r['_ducklake_row_id'] for r in physical), [0,1,2,3,4])
    order = []
    for index,batch in enumerate(batches):
        identifiers = {r[0] for r in batch}
        actual = [r for r in physical if r['id'] in identifiers]
        snapshots = {r['_ducklake_begin_snapshot'] for r in actual}
        t.check('each batch publishes under one actual snapshot/' + str(index), len(snapshots), 1)
        order.append((next(iter(snapshots)), index, actual))
    order.sort()
    t.check('two first writers publish consecutive snapshots', [entry[0] for entry in order],
            [t.start_snapshot['snapshot_id']+1, t.start_snapshot['snapshot_id']+2])
    offset = 0
    for snapshot,index,actual in order:
        t.check('batch reserves one contiguous range/' + str(index),
                sorted(r['_ducklake_row_id'] for r in actual), list(range(offset,offset+len(actual))))
        offset += len(actual)
        t.groups.append(operations[index])
        t.heads.append(snapshot)
    t.result['observed_commit_order'] = [{'snapshot_id':s, 'writer_index':i} for s,i,_ in order]
    t.reverse(expected)
    t.result['notes'].append('The observer requires both real RowExclusiveLock waits behind a SHARE lock on ducklake_snapshot. Publisher order must remain latest-head read, snapshot claim, then initialization. This covers both successful first commits, not cancellation or rollback of the winner.')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-id', default=os.getenv('OG_LAKE_RUN_ID'))
    parser.add_argument('--output', default=os.getenv('OG_LAKE_RESULT_FILE'))
    parser.add_argument('--manifest', default=os.getenv('OG_LAKE_FIXTURE_MANIFEST'))
    parser.add_argument('--mode', required=True, choices=MODES)
    args = parser.parse_args()
    if not all((args.run_id,args.output,args.manifest)):
        parser.error('run-id, output and genuine reference manifest are required')
    validate_run_id(args.run_id)
    case_id = 'N3/empty_insert_' + args.mode
    validate_case_id(case_id)
    args.manifest = str(Path(args.manifest).resolve())
    args.output = Path(args.output).resolve()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    result = {'status':'fail', 'scope':case_id, 'assertions':[], 'artifacts':[], 'notes':[
        'Only existing pure-inline EMPTY_TABLE with an actual carrier and no statistics rows is admitted.',
        'No pre-created statistics, file SDK, native business CREATE, RAC, or production qualification.',
        'Preparation is static only; a successful invocation must execute real host SQL and complete fixed-reference differential.']}
    owned = None
    try:
        with OwnedDatabase(args.run_id,args.output.parent) as owned:
            case = Case(owned,args,result)
            globals()[args.mode](case)
            result['status'] = 'pass'
    except Blocked as exc:
        result.update(status='blocked', reason=str(exc))
    except BaseException as exc:
        result.update(status='fail', error=repr(exc), traceback=traceback.format_exc())
        if not result['assertions'] or result['assertions'][-1]['status'] != 'fail':
            result['assertions'].append({'name':'first-publication scenario completed', 'status':'fail', 'details':repr(exc)})
    finally:
        if owned is not None:
            result['artifacts'].extend(owned.artifacts())
            result['cleanup'] = owned.cleanup_result
        write_json(args.output,result)
        print(json.dumps({'status':result['status'], 'mode':args.mode, 'output':str(args.output)}))
    return {'pass':0,'blocked':2}.get(result['status'],1)


if __name__ == '__main__':
    raise SystemExit(main())
