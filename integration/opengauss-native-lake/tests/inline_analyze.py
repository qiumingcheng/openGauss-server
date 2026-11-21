#!/usr/bin/env python3
"""Real pure-inline ANALYZE acceptance; staging only, no fabricated protocol rows.

Each invocation owns one database. Genuine reference export values are preserved.
Successful SQL without actual business-OID statistics never passes this test.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import os
import re
from pathlib import Path
import secrets
import sys
import time
import traceback

WORKSPACE = Path('/home/qiumc/data/code/cpp')
DRIVER = Path(os.getenv('OG_LAKE_DRIVER_ROOT', str(WORKSPACE / 'integration/opengauss-native-lake')))
sys.path.insert(0, str(DRIVER)); sys.path.insert(0, str(DRIVER / 'tests'))
from inline_fixture import (TYPE_SQL, BUSINESS_TYPES, foreign_table_statement, insert_rows,
                            normalize, query, scalar, verify_loaded_rows)
from lake_harness import Blocked, validate_run_id, write_json
from native_test_context import OwnedDatabase, load_reference, reviewed_runtime_version

PROFILES = {'basic': 'I', 'columns': 'TYPED', 'versions': 'I', 'private': 'I',
            'snapshot': 'I', 'empty': 'N', 'all_null': 'ALL_NULL', 'permissions': 'I',
            'ustore': 'TYPED', 'cancel': 'I', 'admission': 'I',
            'empty_create': 'EMPTY_TABLE', 'empty_no_inline': 'EMPTY_NO_INLINE',
            'distribution': 'ANALYZE_DISTRIBUTION'}


class Case:
    def __init__(self, db, args, result, roles=None):
        from psycopg2 import sql
        self.sql, self.db, self.args, self.result = sql, db, args, result
        self.conn, self.observer = db.connect(), db.connect()
        # Fixed SQL API remains independent of the source/runtime version.
        query(self.conn, "CREATE EXTENSION og_lake VERSION '0.5'")
        self.check('installed SQL permission API', scalar(self.conn,
            "SELECT extversion FROM pg_extension WHERE extname='og_lake'"), '0.5')
        runtime = reviewed_runtime_version()
        caps = json.loads(scalar(self.conn, 'SELECT og_lake.host_status()'))
        self.check('runtime exactly matches reviewed source', caps['module_version'], runtime)
        result.update(runtime_version=runtime, installed_extension_version='0.5')
        self.manifest, self.exported, counts, artifacts = load_reference(
            self.conn, args.manifest, args.run_id, args.output.parent)
        result['artifacts'].extend(artifacts)
        self.check('genuine reference profile', self.manifest['case'], PROFILES[args.mode])
        self.check('all actual catalog tables imported', len(counts), len(self.exported['tables']))
        self.names = [c['name'] for c in self.manifest['schema']]
        self.initial = sorted([tuple(row[n] for n in self.names) for row in self.manifest['expected']], key=lambda r: r[0])
        self.carriers = [r['table_name'] for t in self.exported['tables']
                         if t['table_name'] == 'ducklake_inlined_data_tables'
                         for r in t['rows'] if r['table_id'] == self.manifest['table_id']]
        if args.mode == 'distribution':
            self.check('real distribution population', len(self.initial), 12000)
        else:
            self.check('bounded full sample profile has at most 1000 rows', len(self.initial) <= 1000)
        if args.mode == 'ustore':
            self.convert_carriers_to_ustore()
        self.roles = roles
        if roles is not None:
            from permission_api import adopt_private_owner
            self.private_owner = roles.create('private', False)
            for alias in ('reader', 'maintainer'): roles.create(alias)
            query(self.conn, 'BEGIN')
            try:
                adopt_private_owner(self.conn, self.exported, self.private_owner)
                query(self.conn, 'COMMIT')
            except BaseException:
                query(self.conn, 'ROLLBACK'); raise
        query(self.conn, 'CREATE SCHEMA biz')
        query(self.conn, "SELECT og_lake.attach_catalog('lake_reference','lake_metadata',NULL::text,false)")
        query(self.conn, foreign_table_statement('biz', 'orders', 'lake_reference',
            self.manifest['table_id'], self.manifest['schema']))
        definitions = sql.SQL(',').join(sql.SQL('{} {}').format(sql.Identifier(c['name']),
                    sql.SQL(BUSINESS_TYPES[c['type']])) for c in self.manifest['schema'])
        query(self.conn, sql.SQL('CREATE TABLE biz.heap_expected ({})').format(definitions))
        if self.initial:
            with self.conn.cursor() as cur:
                cur.executemany(sql.SQL('INSERT INTO biz.heap_expected VALUES ({})').format(
                    sql.SQL(',').join(sql.Placeholder() for _ in self.names)), self.initial)
        for conn in (self.conn, self.observer):
            query(conn, 'SET default_statistics_target=100')
            query(conn, 'SET query_dop=1')
        self.oid = scalar(self.conn, "SELECT 'biz.orders'::regclass::oid")
        self.carrier_oids = [scalar(self.conn, 'SELECT %s::regclass::oid',
                                  ('lake_metadata.' + n,)) for n in self.carriers]
        self.check('real native initial rows equal independent fixture', self.rows(), self.initial)
        self.check('new foreign table has no column statistics', self.raw_stats(), [])
        self.baseline = self.catalog()

    def check(self, name, actual, expected=True):
        good = actual == expected
        self.result['assertions'].append({'name': name, 'status': 'pass' if good else 'fail',
                                        'details': {'observed': actual, 'expected': expected}})
        if not good: raise AssertionError(name)

    def rows(self, conn=None, relation='orders'):
        return query(conn or self.conn, self.sql.SQL('SELECT {} FROM biz.{} ORDER BY id').format(
            self.sql.SQL(',').join(map(self.sql.Identifier, self.names)), self.sql.Identifier(relation)))

    def catalog(self, conn=None):
        conn = conn or self.conn
        return {t['table_name']: sorted(json.dumps([normalize(v, c['data_type'])
                    for v, c in zip(row, t['columns'])], ensure_ascii=False, separators=(',', ':'))
                for row in query(conn, self.sql.SQL('SELECT {} FROM lake_metadata.{}').format(
                    self.sql.SQL(',').join(self.sql.Identifier(c['column_name']) for c in t['columns']),
                    self.sql.Identifier(t['table_name'])))) for t in self.exported['tables']}

    def relstats(self, conn=None, relation='orders'):
        return query(conn or self.conn, 'SELECT reltuples,relpages,relallvisible FROM pg_class WHERE oid=%s::regclass',
                     ('biz.' + relation,))[0]

    def raw_stats(self, conn=None, relation='orders'):
        return query(conn or self.conn,
            "SELECT staattnum,s::text FROM pg_statistic s WHERE starelid=%s::regclass "
            "AND starelkind='c' AND NOT stainherit ORDER BY staattnum", ('biz.' + relation,))

    def distribution(self, relation='orders', conn=None):
        # Correlation is not compared across separately populated physical layouts.
        return dict((row[0], tuple(row[1:])) for row in query(conn or self.conn,
            'SELECT attname,null_frac,avg_width,n_distinct,most_common_vals::text,'
            'most_common_freqs::text,histogram_bounds::text FROM pg_stats '
            "WHERE schemaname='biz' AND tablename=%s AND NOT inherited ORDER BY attname", (relation,)))

    def analyze(self, relation='orders', conn=None, columns=None):
        statement = self.sql.SQL('ANALYZE biz.{}').format(self.sql.Identifier(relation))
        if columns:
            statement += self.sql.SQL(' ({})').format(self.sql.SQL(',').join(map(self.sql.Identifier, columns)))
        query(conn or self.conn, statement)

    def compare(self, label, expected_rows=None, columns=None):
        conn = self.conn
        before = self.catalog()
        carrier_before = query(conn, 'SELECT starelid,staattnum,s::text FROM pg_statistic s '
                               'WHERE starelid=ANY(%s::oid[]) ORDER BY starelid,staattnum', (self.carrier_oids,))
        if expected_rows is None: expected_rows = self.rows(relation='heap_expected')
        self.check(label + ' lake rows equal independent heap', self.rows(), expected_rows)
        self.analyze(columns=columns)
        self.analyze('heap_expected', columns=columns)
        self.check(label + ' exact visible population on business OID', self.relstats()[0], float(len(expected_rows)))
        self.check(label + ' native heap population', self.relstats(relation='heap_expected')[0], float(len(expected_rows)))
        self.check(label + ' analyzed foreign pages use positive sentinel', self.relstats()[1] >= 1)
        self.check(label + ' foreign all-visible pages remain zero', self.relstats()[2], 0)
        actual, expected = self.distribution(), self.distribution('heap_expected')
        names = columns or self.names
        if expected_rows:
            self.check(label + ' each requested business column has stats', all(n in actual for n in names))
            self.check(label + ' full small sample matches heap distributions',
                       {n: actual[n] for n in names}, {n: expected[n] for n in names})
        self.check(label + ' complete standard catalog unchanged by ANALYZE', self.catalog(), before)
        self.check(label + ' carrier stats not written instead of business OID', query(conn,
            'SELECT starelid,staattnum,s::text FROM pg_statistic s WHERE starelid=ANY(%s::oid[]) '
            'ORDER BY starelid,staattnum', (self.carrier_oids,)), carrier_before)
        self.result.setdefault('observations', []).append({'label': label,
            'business_oid': self.oid, 'rows': len(expected_rows), 'relstats': self.relstats(),
            'distribution': actual, 'heap_distribution': expected})

    def both(self, statement, params=None):
        for name in ('orders', 'heap_expected'):
            query(self.conn, self.sql.SQL(statement).format(self.sql.Identifier(name)), params)

    def reject(self, label, statement, state, conn=None):
        import psycopg2
        conn = conn or self.conn
        try:
            query(conn, statement)
        except psycopg2.Error as exc:
            self.check(label + ' exact SQLSTATE', exc.pgcode, state)
        else:
            self.check(label + ' must reject', 'accepted', state)

    def convert_carriers_to_ustore(self):
        # Test-import representation only: exact exported protocol rows/schema;
        # before attach/OID registration, no catalog value synthesis or remapping.
        self.check('Ustore test requires real carrier', bool(self.carriers))
        query(self.conn, 'BEGIN')
        try:
            for name in self.carriers:
                table = next(t for t in self.exported['tables'] if t['table_name'] == name)
                definitions = []
                for column in table['columns']:
                    definitions.append(self.sql.SQL('{} {}{}').format(self.sql.Identifier(column['column_name']),
                        self.sql.SQL(TYPE_SQL[column['data_type']]), self.sql.SQL('' if column['is_nullable'] else ' NOT NULL')))
                for constraint in table['constraints']:
                    kind = constraint['constraint_type']
                    if kind != 'NOT NULL':
                        definitions.append(self.sql.SQL('{} ({})').format(self.sql.SQL(kind), self.sql.SQL(',').join(
                            map(self.sql.Identifier, constraint['constraint_column_names']))))
                relation = self.sql.SQL('lake_metadata.{}').format(self.sql.Identifier(name))
                query(self.conn, self.sql.SQL('DROP TABLE {}').format(relation))
                query(self.conn, self.sql.SQL('CREATE TABLE {} ({}) WITH (storage_type=ustore)').format(
                    relation, self.sql.SQL(',').join(definitions)))
                insert_rows(self.conn, 'lake_metadata', table)
                options = scalar(self.conn, 'SELECT reloptions::text FROM pg_class WHERE oid=%s::regclass',
                                 ('lake_metadata.' + name,))
                self.check('actual Ustore carrier ' + name, 'storage_type=ustore' in options)
            verify_loaded_rows(self.conn, 'lake_metadata', self.exported)
            query(self.conn, 'COMMIT')
        except BaseException:
            query(self.conn, 'ROLLBACK'); raise


def basic(t):
    t.compare('baseline genuine I')
    t.check('unique id has native negative NDV representation', t.distribution()['id'][2], -1.0)
    t.check('no lake publication from statistics', t.catalog(), t.baseline)


def columns(t):
    t.compare('initial typed all columns')
    old = dict(t.raw_stats())
    query(t.conn, 'BEGIN')
    t.both('UPDATE biz.{} SET val=77,note=\'changed after first ANALYZE\' WHERE id=1')
    query(t.conn, 'COMMIT')
    t.compare('only requested id and val', columns=['id', 'val'])
    current = dict(t.raw_stats())
    for attnum, name in enumerate(t.names, 1):
        if name not in ('id', 'val'):
            t.check('unspecified column stats retained: ' + name, current[attnum], old[attnum])
    t.compare('all columns refreshed afterward')
    before = t.raw_stats()
    t.reject('unknown requested column', 'ANALYZE biz.orders (does_not_exist)', '42703')
    t.check('bad column preserves existing stats', t.raw_stats(), before)


def versions(t):
    for step, statement in enumerate(('UPDATE biz.{} SET val=110 WHERE id=1',
                                      'UPDATE biz.{} SET val=220 WHERE id=1',
                                      'DELETE FROM biz.{} WHERE id=2'), 1):
        query(t.conn, 'BEGIN'); t.both(statement); query(t.conn, 'COMMIT')
        t.compare('committed version step ' + str(step))
    physical = sum(scalar(t.conn, t.sql.SQL('SELECT count(*) FROM lake_metadata.{}').format(t.sql.Identifier(n)))
                   for n in t.carriers)
    t.check('real history contains more physical than visible rows', physical > len(t.rows()))
    t.check('reltuples does not use physical version count', t.relstats()[0], float(len(t.rows())))


def private(t):
    t.compare('committed baseline before private writes')
    stats, heap_stats, directory = t.raw_stats(), t.raw_stats(relation='heap_expected'), t.catalog()
    rel, heap_rel = t.relstats(), t.relstats(relation='heap_expected')

    def visible(label, expected):
        t.check(label + ' lake private net values', t.rows(), expected)
        t.check(label + ' independent heap private net values', t.rows(relation='heap_expected'), expected)
        t.check(label + ' observer still sees committed lake', t.rows(t.observer), t.initial)
        t.check(label + ' observer still sees committed heap', t.rows(t.observer,'heap_expected'), t.initial)

    def deny_analysis(label, expected):
        # DoVacuumMppTable applies the same transaction-block guard to both
        # persistent relations, before reaching either sampler. Recover through
        # a real savepoint so the previous business writes remain testable.
        for relation in ('orders', 'heap_expected'):
            query(t.conn, 'SAVEPOINT rejected_analyze')
            t.reject(label + '/' + relation, 'ANALYZE biz.' + relation, '25001')
            query(t.conn, 'ROLLBACK TO rejected_analyze')
            query(t.conn, 'RELEASE rejected_analyze')
            visible(label + '/after rejected ' + relation, expected)
        t.check(label + ' lake column statistics unchanged', t.raw_stats(), stats)
        t.check(label + ' heap column statistics unchanged', t.raw_stats(relation='heap_expected'), heap_stats)
        t.check(label + ' lake relstats unchanged before sampler admission', t.relstats(), rel)
        t.check(label + ' heap relstats unchanged before sampler admission', t.relstats(relation='heap_expected'), heap_rel)
        t.check(label + ' no journal publication through rejected ANALYZE', t.catalog(t.observer), directory)

    expected = sorted([(identifier,222 if identifier==1 else value)
                       for identifier,value in t.initial if identifier!=2] + [(50,500)])
    query(t.conn, 'BEGIN')
    t.both('INSERT INTO biz.{} VALUES (50,500)')
    t.both('UPDATE biz.{} SET val=111 WHERE id=1')
    t.both('UPDATE biz.{} SET val=222 WHERE id=1')
    t.both('DELETE FROM biz.{} WHERE id=2')
    visible('own previous-command net versions', expected)
    deny_analysis('persistent ANALYZE inside outer transaction', expected)
    query(t.conn, 'SAVEPOINT child')
    t.both('INSERT INTO biz.{} VALUES (60,600)')
    t.both('DELETE FROM biz.{} WHERE id=3')
    child_expected = sorted([row for row in expected if row[0]!=3] + [(60,600)])
    visible('child private net versions', child_expected)
    deny_analysis('persistent ANALYZE inside child transaction', child_expected)
    query(t.conn, 'ROLLBACK TO child'); query(t.conn, 'RELEASE child')
    visible('after child rollback', expected)
    query(t.conn, 'ROLLBACK')
    t.check('rejected analysis never changes column stats', t.raw_stats(), stats)
    t.check('outer rollback leaves complete standard catalog unchanged', t.catalog(), directory)
    t.check('outer rollback restores initial rows', t.rows(), t.initial)
    t.check('outer rollback restores independent heap', t.rows(relation='heap_expected'), t.initial)
    t.compare('top-level analysis works after rollback')
    query(t.conn, 'BEGIN')
    t.both('INSERT INTO biz.{} VALUES (50,500)')
    t.both('UPDATE biz.{} SET val=111 WHERE id=1')
    t.both('UPDATE biz.{} SET val=222 WHERE id=1')
    t.both('DELETE FROM biz.{} WHERE id=2')
    visible('private net versions before successful commit', expected)
    deny_analysis('same host guard before successful business commit', expected)
    query(t.conn, 'COMMIT')
    t.compare('top-level analysis gathers only the committed net versions', expected_rows=expected)
    t.result['limitations'].append('This persistent-table host rejects ANALYZE inside a user transaction with 25001. Private versions are verified by SELECT and rollback; their eventual committed net result is analyzed. No private-write ANALYZE capability is claimed.')


class SamplePause:
    """Arm one next-transaction real sample and observe its exact lock waiter.

    The arm SELECT commits independently; its immediately following request is
    the top-level ANALYZE. No BEGIN or intervening sampler-session statement.
    """
    def __init__(self, t):
        self.t = t
        self.holder, self.observer = t.db.connect(), t.db.connect()
        self.key = secrets.randbits(62) + 1
        self.application = 'lake_sample_' + secrets.token_hex(12)
        self.pid = None
        self.future = self.executor = None
        self.held = False
        self.armed = False
        self.identity = None

    def exact_locks(self):
        return query(self.observer, "SELECT pid,mode,granted,classid,objid,objsubid,database FROM pg_locks "
            "WHERE locktype='advisory' AND database=%s AND classid=%s::oid AND objid=%s::oid "
            "AND objsubid=1 AND mode='ExclusiveLock' AND NOT granted",
            (self.dbid,self.key >> 32,self.key & 0xffffffff))

    def __enter__(self):
        t = self.t
        source = Path(os.getenv('OG_LAKE_SOURCE', str(WORKSPACE / '.worktrees/opengauss-native-lake')))
        producer = source / 'contrib/og_lake/lake_analyze.cpp'
        if not producer.is_file() or 'LakeTestAnalyzeAfterRow' not in producer.read_text():
            raise Blocked('Real sampler pause probe is not integrated into the producer')
        query(t.conn, "CREATE FUNCTION biz.analyze_pause(oid,bigint,bigint) RETURNS void "
              "AS '$libdir/og_lake','og_lake_test_analyze_pause' LANGUAGE C VOLATILE NOT FENCED")
        query(t.conn, 'REVOKE ALL ON FUNCTION biz.analyze_pause(oid,bigint,bigint) FROM PUBLIC')
        self.dbid = scalar(self.observer, 'SELECT oid FROM pg_database WHERE datname=current_database()')
        query(t.conn, 'SET statement_timeout=45000')
        query(t.conn, 'SET application_name=%s', (self.application,))
        t.check('sampler connection uses independent top-level transactions', t.conn.autocommit, True)
        try:
            query(self.holder, 'SELECT pg_advisory_lock(%s::bigint)', (self.key,))
            self.held = True
            arm = query(t.conn, 'SELECT pg_current_sessid(),session_user,current_user,'
                'biz.analyze_pause(%s::oid,1,%s::bigint)', (t.oid,self.key))[0]
            self.armed = True
            self.identity = [(arm[1],arm[2])]
            t.result['sampler_arm_identity'] = {'logical_session_id':arm[0],
                'session_user':arm[1], 'current_user':arm[2], 'application_name':self.application}
            def run():
                import psycopg2
                try:
                    t.analyze()
                    return {'state':'accepted'}
                except psycopg2.Error as exc:
                    return {'state':exc.pgcode, 'message':str(exc)}
            self.executor = ThreadPoolExecutor(max_workers=1)
            self.future = self.executor.submit(run)
            deadline = time.monotonic() + 12
            waiting = []
            while time.monotonic() < deadline:
                waiting = self.exact_locks()
                if waiting or self.future.done():
                    break
                time.sleep(0.02)  # bounded observation, never a timing proof
            t.check('real top-level sampler reaches exactly one owned advisory wait', len(waiting), 1)
            candidate = waiting[0][0]
            activity = query(self.observer, 'SELECT application_name FROM pg_stat_activity WHERE pid=%s', (candidate,))
            t.check('waiter belongs to the exact sampler request', activity, [(self.application,)])
            self.pid = candidate
            t.result['sampler_wait_lock'] = [list(row) for row in waiting]
            return self
        except BaseException:
            self.close()
            raise

    def release(self):
        if self.held:
            self.t.check('holder releases the exact real sample barrier',
                scalar(self.holder,'SELECT pg_advisory_unlock(%s::bigint)',(self.key,)), True)
            self.held = False

    def finish(self, expected):
        outcome = self.future.result(timeout=15)
        self.t.check('top-level sampler completion SQLSTATE', outcome['state'], expected)
        self.t.result['sampler_outcome'] = outcome
        return outcome

    def close(self):
        try:
            if self.future is not None and not self.future.done() and self.pid is not None:
                scalar(self.observer, 'SELECT pg_cancel_backend(%s)', (self.pid,))
        finally:
            try:
                self.release()
            finally:
                if self.executor is not None:
                    self.executor.shutdown(wait=True)
                if self.armed:
                    # Explicit disarm after the running request is finished.
                    query(self.t.conn, 'SELECT biz.analyze_pause(0::oid,0::bigint,0::bigint)')
                    self.armed = False
        self.t.check('no remaining exact advisory key after sampler completion', scalar(self.observer,
            "SELECT count(*) FROM pg_locks WHERE locktype='advisory' AND database=%s "
            'AND classid=%s::oid AND objid=%s::oid AND objsubid=1',
            (self.dbid,self.key >> 32,self.key & 0xffffffff)), 0)

    def __exit__(self, kind, value, tb):
        self.close()
        return False


def snapshot(t):
    a, b = t.conn, t.db.connect()
    t.compare('committed old distribution before concurrent sample')
    old_distribution, old_stats, old_rel = t.distribution(), t.raw_stats(), t.relstats()
    changed = sorted([(i,909 if i==1 else v) for i,v in t.initial if i!=2] + [(9,90),(10,100)])
    with SamplePause(t) as pause:
        t.check('paused old sample has not partially published statistics', t.raw_stats(pause.observer), old_stats)
        query(b, 'BEGIN')
        query(b, 'UPDATE biz.orders SET val=909 WHERE id=1')
        query(b, 'DELETE FROM biz.orders WHERE id=2')
        query(b, 'INSERT INTO biz.orders VALUES (9,90),(10,100)')
        query(b, 'COMMIT')
        t.check('concurrent writer really commits before the sample resumes', t.rows(b), changed)
        t.check('concurrent commit changes actual population', len(changed), len(t.initial)+1)
        committed_directory = t.catalog(b)
        t.check('old sample remains blocked while writer publishes', len(pause.exact_locks()), 1)
        t.check('no statistics half publication during concurrent commit', t.raw_stats(pause.observer), old_stats)
        t.check('no relstats half publication during concurrent commit', t.relstats(pause.observer), old_rel)
        pause.release()
        pause.finish('accepted')
    t.check('completed sample keeps its original visible population', t.relstats()[0], float(len(t.initial)))
    t.check('completed sample keeps the whole original distribution', t.distribution(), old_distribution)
    t.check('old distribution independently matches unchanged heap control', t.distribution(), t.distribution('heap_expected'))
    t.check('sampling does not undo or republish concurrent lake changes', t.catalog(), committed_directory)
    t.check('new transaction sees B commit', t.rows(), changed)
    query(a, 'TRUNCATE biz.heap_expected')
    with a.cursor() as cur:
        cur.executemany('INSERT INTO biz.heap_expected VALUES (%s,%s)', changed)
    t.compare('next independent top-level ANALYZE sees newer snapshot', expected_rows=changed)


def empty(t):
    t.check('real N profile is logically empty', t.initial, [])
    t.compare('never analyzed empty')
    t.check('no invented column distribution for empty table', t.raw_stats(), [])
    query(t.conn, 'BEGIN'); t.both('INSERT INTO biz.{} VALUES (1,10),(2,20),(3,30)'); query(t.conn, 'COMMIT')
    t.compare('nonempty before complete deletion')
    old_lake, old_heap = t.raw_stats(), t.raw_stats(relation='heap_expected')
    query(t.conn, 'BEGIN'); t.both('DELETE FROM biz.{}'); query(t.conn, 'COMMIT')
    t.compare('all deleted')
    t.check('empty lake retains old column stats according to native core', t.raw_stats(), old_lake)
    t.check('ordinary heap has the same retain-old-stats behavior', t.raw_stats(relation='heap_expected'), old_heap)
    # Logical emptiness must not erase the physical cost of retained versions.
    actual_bytes = sum(scalar(t.conn, 'SELECT pg_relation_size(%s::oid)', (oid,))
                       for oid in t.carrier_oids)
    t.check('all-deleted carrier still occupies real data pages', actual_bytes > 0)
    pages = actual_bytes / int(scalar(t.conn, "SELECT current_setting('block_size')"))
    lines = [row[0] for row in query(t.conn, 'EXPLAIN SELECT * FROM biz.orders')]
    estimated = [float(line.split(':', 1)[1]) for line in lines if 'Inline Data Pages:' in line]
    t.check('logical empty still charges the retained carrier pages', estimated, [float(pages)])


def all_null(t):
    t.compare('genuine nonempty ALL_NULL')
    expected = dict(zip(t.names, zip(*t.initial)))
    for name, values in expected.items():
        if all(v is None for v in values):
            t.check(name + ' all NULL fraction', t.distribution()[name][0], 1.0)
            t.check(name + ' no fake MCV values', t.distribution()[name][3], None)


def permissions(t):
    roles = t.roles
    sessions = {}
    for alias in ('reader', 'maintainer'):
        role = t.sql.Identifier(roles.name(alias))
        query(t.conn, t.sql.SQL('GRANT USAGE ON SCHEMA biz TO {}').format(role))
        query(t.conn, t.sql.SQL('GRANT USAGE ON FOREIGN SERVER lake_reference TO {}').format(role))
        conn, identity = roles.connect(t.db, alias); sessions[alias] = conn
        t.result.setdefault('real_logins', []).append(identity)
        query(conn, 'SET default_statistics_target=100')
        for schema in ('lake_metadata', 'og_lake_internal'):
            t.check(alias + ' has no internal USAGE ' + schema, scalar(t.conn,
                'SELECT has_schema_privilege(%s,%s,\'USAGE\')', (roles.name(alias), schema)), False)
    query(t.conn, t.sql.SQL('GRANT SELECT ON biz.orders TO {}').format(t.sql.Identifier(roles.name('reader'))))
    query(t.conn, t.sql.SQL('GRANT VACUUM ON biz.orders TO {}').format(t.sql.Identifier(roles.name('maintainer'))))
    for alias in sessions:
        t.check(alias + ' maintenance ACL', scalar(t.conn, "SELECT has_table_privilege(%s,'biz.orders','VACUUM')",
            (roles.name(alias),)), alias == 'maintainer')
        t.check(alias + ' SELECT ACL', scalar(t.conn, "SELECT has_table_privilege(%s,'biz.orders','SELECT')",
            (roles.name(alias),)), alias == 'reader')
    reader, maintainer = sessions['reader'], sessions['maintainer']
    before_stats, before_relation = t.raw_stats(), t.relstats()
    reader.notices.clear(); t.analyze(conn=reader)
    t.check('SELECT-only ANALYZE did not create column stats', t.raw_stats(), before_stats)
    t.check('SELECT-only ANALYZE did not change relstats', t.relstats(), before_relation)
    t.check('SELECT-only skip is explicit notice', any('skipping' in n.lower() for n in reader.notices))
    t.check('SELECT-only still reads expected rows', t.rows(reader), t.initial)
    t.reject('maintenance privilege does not confer SELECT', 'SELECT * FROM biz.orders', '42501', maintainer)
    t.analyze(conn=maintainer)
    t.check('maintenance-without-SELECT actually gathered business stats', len(t.raw_stats()), len(t.names))
    t.check('maintenance population is accurate', t.relstats()[0], float(len(t.initial)))
    for alias, conn in sessions.items():
        t.check(alias + ' session/current identity restored', query(conn, 'SELECT session_user,current_user'),
                [(roles.name(alias), roles.name(alias))])
        t.reject(alias + ' cannot read private protocol table',
                 'SELECT * FROM lake_metadata.ducklake_snapshot', '42501', conn)
    t.compare('administrator/owner and ordinary maintenance agree')
    t.check('permission tests caused no protocol publication', t.catalog(), t.baseline)


def ustore(t):
    t.check('Ustore fixture has multiple same-page candidates', len(t.initial) >= 3)
    for i in range(3):
        t.check('Ustore complete business scan ' + str(i), t.rows(), t.initial)
        t.compare('Ustore multirow ANALYZE ' + str(i))
    query(t.conn, 'BEGIN')
    query(t.conn, 'SAVEPOINT unsupported_scroll')
    t.reject('host explicitly rejects SCROLL cursor',
             'DECLARE unsupported_u_cursor SCROLL CURSOR FOR SELECT * FROM biz.orders ORDER BY id', '0A000')
    query(t.conn, 'ROLLBACK TO unsupported_scroll'); query(t.conn, 'RELEASE unsupported_scroll')
    query(t.conn, 'DECLARE u_cursor CURSOR FOR SELECT * FROM biz.orders ORDER BY id')
    t.check('Ustore forward cursor first batch', query(t.conn, 'FETCH FORWARD 2 FROM u_cursor'), t.initial[:2])
    t.check('Ustore forward cursor remaining batch', query(t.conn, 'FETCH FORWARD ALL FROM u_cursor'), t.initial[2:])
    t.check('Ustore forward cursor exact EOF', query(t.conn, 'FETCH FORWARD 1 FROM u_cursor'), [])
    query(t.conn, 'CLOSE u_cursor'); query(t.conn, 'COMMIT')
    query(t.conn, 'BEGIN'); t.both('UPDATE biz.{} SET val=123 WHERE id=1'); query(t.conn, 'COMMIT')
    t.compare('Ustore with real committed ended versions')
    t.result['limitations'].append('This host does not support SCROLL CURSOR. Forward FETCH validates cursor delivery only; actual ForeignScan rescan is covered separately by the USTORE tests requiring NestedLoop ForeignScan loops >= 4.')


def cancel(t):
    t.compare('baseline before real sampler cancellation')
    stats, rel = t.raw_stats(), t.relstats()
    # A successful new analysis would now produce a different population and
    # distribution. Keeping the old statistics after cancel is meaningful.
    query(t.conn, 'BEGIN')
    t.both('UPDATE biz.{} SET val=808 WHERE id=1')
    t.both('INSERT INTO biz.{} VALUES(90,900)')
    query(t.conn, 'COMMIT')
    expected = sorted([(i,808 if i==1 else v) for i,v in t.initial] + [(90,900)])
    directory = t.catalog()
    t.check('cancel input is genuinely newer than baseline statistics', t.rows(), expected)
    with SamplePause(t) as pause:
        t.check('observer sees no partially written statistics', t.raw_stats(pause.observer), stats)
        t.check('observer sees unchanged relstats before cancellation', t.relstats(pause.observer), rel)
        t.check('cancel targets the actual owned sampler waiter', scalar(pause.observer,
            'SELECT pg_cancel_backend(%s)', (pause.pid,)), True)
        pause.finish('57014')
        identity = pause.identity
        t.check('cancelled statistics stay unchanged', t.raw_stats(pause.observer), stats)
        t.check('cancelled relstats stay unchanged', t.relstats(pause.observer), rel)
        t.check('cancelled scan did not publish protocol values', t.catalog(pause.observer), directory)
        # No intervening sampler-session request and no explicit disarm: with
        # the same key still held, the next real ANALYZE must not pause again.
        t.analyze()
        t.check('cancel consumes the one-shot probe before explicit cleanup', pause.exact_locks(), [])
        t.check('immediate top-level retry publishes the newer population',
                t.relstats(pause.observer)[0], float(len(expected)))
    t.check('sampler ERROR restores request identity after private SPI', query(t.conn, 'SELECT session_user,current_user'), identity)
    t.check('same session remains usable', scalar(t.conn, 'SELECT 6*7'), 42)
    t.compare('same session top-level retry sees all committed newer rows', expected_rows=expected)
    probe_lifecycle(t, len(expected))


def probe_lifecycle(t, population):
    """Negative admission/expiry checks against a held real advisory key.

    A stale arm cannot silently pass: ANALYZE would wait on the holder and its
    short statement timeout would raise instead of reporting success.
    """
    holder = t.db.connect()
    key = secrets.randbits(62) + 1
    dbid = scalar(holder, 'SELECT oid FROM pg_database WHERE datname=current_database()')
    statement = 'SELECT biz.analyze_pause(%d::oid,1,%d::bigint)' % (t.oid,key)
    clear = 'SELECT biz.analyze_pause(0::oid,0::bigint,0::bigint)'
    before = t.catalog()
    query(t.conn, 'SET statement_timeout=3000')
    query(holder, 'SELECT pg_advisory_lock(%s::bigint)', (key,))
    try:
        query(t.conn, 'BEGIN')
        t.reject('arm inside BEGIN follows native 25001 guard', statement, '25001')
        query(t.conn, 'ROLLBACK')
        t.analyze()
        t.check('rejected top-level arm leaves no pending probe', t.relstats()[0], float(population))

        query(t.conn, 'BEGIN'); query(t.conn, 'SAVEPOINT forbidden_arm')
        t.reject('arm inside SAVEPOINT follows native 25001 guard', statement, '25001')
        query(t.conn, 'ROLLBACK TO forbidden_arm'); query(t.conn, 'RELEASE forbidden_arm')
        query(t.conn, 'COMMIT')
        t.analyze()
        t.check('rejected nested arm leaves no pending probe', t.relstats()[0], float(population))

        query(t.conn, statement)
        t.check('unrelated next transaction succeeds normally', scalar(t.conn,'SELECT 41+1'), 42)
        t.analyze()
        t.check('unrelated next transaction expires the reservation', t.relstats()[0], float(population))

        query(t.conn, statement)
        t.reject('ordinary next-transaction ERROR consumes reservation', 'SELECT 1/0', '22012')
        t.analyze()
        t.check('ERROR does not leak reservation to a later ANALYZE', t.relstats()[0], float(population))

        query(t.conn, 'SELECT biz.analyze_pause(%s::oid,%s::bigint,%s::bigint)',
              (t.oid,population+10,key))
        t.analyze()
        # Re-arming must succeed immediately: another unrelated SELECT could
        # otherwise erase a stale pending state and hide failed expiry.
        query(t.conn, statement)
        query(t.conn, clear)
        t.analyze()
        t.check('unreached threshold expires and explicit disarm cancels next arm',
                t.relstats()[0], float(population))
        t.check('probe lifecycle tests never publish lake metadata', t.catalog(), before)
    finally:
        query(t.conn, 'ROLLBACK')
        query(t.conn, clear)
        t.check('lifecycle holder unlocks its exact key',
                scalar(holder,'SELECT pg_advisory_unlock(%s::bigint)',(key,)), True)
        query(t.conn, 'SET statement_timeout=15000')
    t.check('lifecycle tests leave no owned advisory key', scalar(holder,
        "SELECT count(*) FROM pg_locks WHERE locktype='advisory' AND database=%s "
        'AND classid=%s::oid AND objid=%s::oid AND objsubid=1',
        (dbid,key >> 32,key & 0xffffffff)), 0)


def empty_create(t):
    t.check('actual CREATE-only reference has no table_stats', t.manifest['table_stats'], [])
    no_inline = t.args.mode == 'empty_no_inline'
    t.check('CREATE-only actual inline option', t.manifest['layout_settings']['data_inlining_row_limit'], 0 if no_inline else 100)
    t.check('CREATE-only carrier follows actual inline option', len(t.manifest['inline_tables']), 0 if no_inline else 1)
    t.check('actual CREATE-only reference has no identities', t.manifest['row_identity_map'], [])
    before_plan = query(t.conn, 'EXPLAIN SELECT * FROM biz.orders WHERE id>0')
    t.result['plans'] = [{'phase': 'never inserted before ANALYZE', 'text': before_plan}]
    t.compare('actual never-inserted empty layout')
    t.result['plans'].append({'phase': 'never inserted after ANALYZE',
                              'text': query(t.conn, 'EXPLAIN SELECT * FROM biz.orders WHERE id>0')})
    actual_bytes = sum(scalar(t.conn, 'SELECT pg_relation_size(%s::oid)', (oid,))
                       for oid in t.carrier_oids)
    t.check('never-inserted physical layout has no allocated data bytes', actual_bytes, 0)
    for plan in t.result['plans']:
        lines = [row[0] for row in plan['text']]
        for label in ('Inline Data Pages:', 'Inline Read Cost:'):
            values = [float(line.split(':', 1)[1]) for line in lines if label in line]
            t.check(plan['phase'] + ' zero actual ' + label, values, [0.0])
    t.check('pg_class analyzed-empty sentinel is bookkeeping only', t.relstats()[1], 1)
    t.check('empty ANALYZE creates no fake column stats', t.raw_stats(), [])
    t.check('empty ANALYZE never creates carrier/statistics protocol rows', t.catalog(), t.baseline)
    if no_inline:
        t.reject('existing no-carrier write admission is not widened',
                 'INSERT INTO biz.orders VALUES (1,10)', '0A000')
        t.check('rejected first write retains true empty layout', t.catalog(), t.baseline)
        t.check('same-session empty read after rejection', t.rows(), [])


def empty_no_inline(t):
    empty_create(t)


def distribution(t):
    source = Path(os.getenv('OG_LAKE_SOURCE', str(WORKSPACE / '.worktrees/opengauss-native-lake')))
    scan = source / 'contrib/og_lake/lake_inline_scan.cpp'
    if 'set_baserel_size_estimates(root, baserel)' not in scan.read_text():
        raise Blocked('Distribution/plan case requires the separately reviewed statistics-consumer patch')
    expected = { 'val=1': (6000, 480), 'id>9600': (2400, 360),
                 'flag IS NULL': (3000, 360), 'note IS NULL': (2400, 360) }
    for predicate, (count, unused) in expected.items():
        for relation in ('orders', 'heap_expected'):
            t.check('independent exact population ' + relation + ' ' + predicate,
                scalar(t.conn, 'SELECT count(*) FROM biz.' + relation + ' WHERE ' + predicate), count)
    t.result['plans'] = [{'phase': 'before ANALYZE',
                         'text': query(t.conn, 'EXPLAIN SELECT * FROM biz.orders WHERE val=1')}]
    for run in range(3):
        query(t.conn, 'SET default_statistics_target=10')
        directory = t.catalog()
        t.conn.notices.clear()
        query(t.conn, 'ANALYZE VERBOSE biz.orders')
        notices = list(t.conn.notices)
        samples = [tuple(map(int, match)) for notice in notices
                   for match in re.findall(r'native lake analyzed ([0-9]+) visible rows, ([0-9]+) rows in sample', notice)]
        t.check('actual producer reports full stream and bounded sample run ' + str(run), samples, [(12000, 3000)])
        t.check('sample size is smaller than population', samples[0][1] < samples[0][0])
        t.check('sampled reltuples still counts full visible stream', t.relstats()[0], 12000.0)
        dist = t.distribution()
        t.check('sampled all four business columns have stats', sorted(dist), sorted(t.names))
        t.check('sampled NULL boolean fraction within preregistered 3 percentage points',
                abs(dist['flag'][0] - 0.25) <= 0.03)
        t.check('sampled NULL text fraction within preregistered 3 percentage points',
                abs(dist['note'][0] - 0.20) <= 0.03)
        t.check('unique sampled ID uses native NDV extrapolation', dist['id'][2], -1.0)
        mcv, freqs = query(t.conn,
            "SELECT most_common_vals::text::integer[],most_common_freqs FROM pg_stats "
            "WHERE schemaname='biz' AND tablename='orders' AND attname='val' AND NOT inherited")[0]
        t.check('known hotspot appears in actual MCV', mcv is not None and 1 in mcv)
        t.check('hotspot frequency within preregistered 4 percentage points', abs(freqs[mcv.index(1)] - 0.5) <= 0.04)
        bounds = scalar(t.conn,
            "SELECT histogram_bounds::text::integer[] FROM pg_stats "
            "WHERE schemaname='biz' AND tablename='orders' AND attname='id' AND NOT inherited")
        t.check('actual histogram spans data range', bool(bounds) and 1 <= bounds[0] < 1200 and 10800 < bounds[-1] <= 12000)
        t.check('actual unique ID histogram strictly increases', all(a < b for a, b in zip(bounds, bounds[1:])))
        t.analyze('heap_expected')
        data_pages = []
        for predicate, (count, tolerance) in expected.items():
            for relation in ('orders', 'heap_expected'):
                lines = [r[0] for r in query(t.conn, 'EXPLAIN SELECT * FROM biz.' + relation + ' WHERE ' + predicate)]
                scans = [line for line in lines if ('Foreign Scan' if relation == 'orders' else 'Seq Scan') in line]
                t.check('one real scan plan ' + relation + ' ' + predicate, len(scans), 1)
                match = re.search(r'\brows=([0-9]+)', scans[0])
                t.check('scan plan exposes row estimate', match is not None)
                estimate = int(match.group(1))
                t.check('sampled plan estimate within preregistered error ' + relation + ' ' + predicate,
                        abs(estimate - count) <= tolerance)
                if relation == 'orders':
                    t.check('planner actually consumes ANALYZE source', any('pg_class / ANALYZE' in line for line in lines))
                    pages = [float(line.split(':', 1)[1]) for line in lines if 'Inline Data Pages:' in line]
                    t.check('planner exposes physical read estimate', len(pages), 1)
                    data_pages.extend(pages)
                t.result['plans'].append({'run': run, 'relation': relation, 'predicate': predicate,
                    'actual_count': count, 'allowed_absolute_error': tolerance, 'estimated_count': estimate, 'text': lines})
        t.check('different selectivities do not pretend to scan fewer physical pages', len(set(data_pages)), 1)
        t.check('sampling and planning do not change protocol directory', t.catalog(), directory)
        t.result.setdefault('sample_observations', []).append({'run': run, 'notices': notices,
            'distribution': dist, 'histogram_bounds': bounds, 'sample_size': samples[0][1]})
    # Post-analysis stats can become stale. Row correctness must not depend on them.
    query(t.conn, 'BEGIN')
    t.both('UPDATE biz.{} SET val=99999 WHERE id<=100')
    query(t.conn, 'COMMIT')
    for relation in ('orders', 'heap_expected'):
        t.check('stale histogram cannot suppress changed business values ' + relation,
                scalar(t.conn, 'SELECT count(*) FROM biz.' + relation + ' WHERE val=99999'), 100)
    t.result['limitations'].append('Three statistical trials validate this fixed distribution only, not universal probability or measured memory upper bounds.')


def admission(t):
    t.compare('baseline before unsupported statistics targets')
    for target in (0, -2):
        before, directory = t.raw_stats(), t.catalog()
        query(t.conn, 'SET default_statistics_target=' + str(target))
        try:
            t.reject('lake unsupported target ' + str(target), 'ANALYZE biz.orders', '0A000')
        finally:
            query(t.conn, 'SET default_statistics_target=100')
        t.check('unsupported target preserves column stats', t.raw_stats(), before)
        t.check('unsupported target preserves directory', t.catalog(), directory)
    t.compare('positive target remains usable')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--mode', required=True, choices=sorted(PROFILES))
    parser.add_argument('--manifest', default=os.getenv('OG_LAKE_FIXTURE_MANIFEST'))
    parser.add_argument('--run-id', default=os.getenv('OG_LAKE_RUN_ID'))
    parser.add_argument('--output', default=os.getenv('OG_LAKE_RESULT_FILE'))
    args = parser.parse_args()
    if not all((args.manifest, args.run_id, args.output)):
        parser.error('genuine manifest, run-id and output are required')
    validate_run_id(args.run_id); args.output = Path(args.output).resolve()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    result = {'status': 'fail', 'scope': 'STAT-INLINE/' + args.mode, 'assertions': [], 'artifacts': [],
              'limitations': [('ANALYZE_DISTRIBUTION samples 3000 of 12000 real rows; bounded distribution-specific acceptance only.'
                 if args.mode == 'distribution' else 'Small genuine reference fixtures use a full sample, not large-table sampling-quality acceptance.'),
                'No file/Parquet/SDK pruning, autoanalyze, SMP, RAC or calibrated cost acceptance.',
                'Successful outer rollback need not undo native inplace pg_class estimates.',
                'Heap distribution comparison excludes correlation because physical layouts differ.']}
    db = roles = control = None
    try:
        if args.mode in ('permissions', 'cancel'):
            import psycopg2
            from permission_api import OwnedRoles, cleanup_roles
            control = psycopg2.connect(host=str(WORKSPACE / '.runtime/opengauss-native-lake/debug/socket'),
                port=55560, dbname='postgres', user=os.getenv('USER', 'qiumc'), connect_timeout=10)
            control.autocommit = True
            query(control, 'SET statement_timeout=15000')
            roles = OwnedRoles(control, args.run_id, args.output.parent)
        with OwnedDatabase(args.run_id, args.output.parent) as db:
            case = Case(db, args, result, roles)
            globals()[args.mode](case)
            result['status'] = 'pass'
    except Blocked as exc:
        result.update(status='blocked', reason=str(exc))
    except BaseException as exc:
        result.update(status='fail', reason=str(exc), traceback=traceback.format_exc())
    finally:
        if db is not None:
            result['artifacts'].extend(db.artifacts()); result['database_cleanup'] = db.cleanup_result
        if roles is not None:
            try:
                if db is not None and db.cleanup_result and db.cleanup_result['returncode'] == 0:
                    result['role_cleanup'] = cleanup_roles(control, roles.path, args.run_id)
                elif not roles.items:
                    result['role_cleanup'] = {'status': 'pass', 'reason': 'No roles allocated'}
                else:
                    raise RuntimeError('Database cleanup unproven; role cleanup intentionally deferred')
            except BaseException as exc:
                result.update(status='fail', cleanup_error=str(exc))
            result['artifacts'].append({'path': str(roles.path)}); result = roles.redact(result)
        if control is not None: control.close()
        write_json(args.output, result)
        print(json.dumps({'status': result['status'], 'mode': args.mode, 'output': str(args.output)}))
    return {'pass': 0, 'blocked': 2}.get(result['status'], 1)


if __name__ == '__main__':
    raise SystemExit(main())
