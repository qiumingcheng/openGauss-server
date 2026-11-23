#!/usr/bin/env python3
"""N4 acceptance: genuine reference input, real host DML, independent values.

Each run records its own result; registration alone does not imply acceptance.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import copy
import json
import os
from pathlib import Path
import sys
import time
import traceback

WORKSPACE = Path('/home/qiumc/data/code/cpp')
DRIVER = Path(os.getenv('OG_LAKE_DRIVER_ROOT', str(WORKSPACE / 'integration/opengauss-native-lake')))
sys.path.insert(0, str(DRIVER))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from inline_fixture import foreign_table_statement, multiset, query, scalar
from lake_harness import Blocked, validate_run_id, write_json
from native_test_context import OwnedDatabase, load_reference, reviewed_runtime_version


class Case:
    def __init__(self, db, args, result):
        from psycopg2 import sql
        self.sql, self.db, self.args, self.result = sql, db, args, result
        self.a, self.b, self.c = [db.connect() for _ in range(3)]
        query(self.a, "CREATE EXTENSION og_lake VERSION '0.4'")
        caps = json.loads(scalar(self.a, 'SELECT og_lake.host_status()'))
        runtime_version = reviewed_runtime_version()
        if runtime_version not in ('0.4', '0.5', '0.6', '0.7', '0.8', '0.9', '0.10', '0.11') or caps.get('module_version') != runtime_version or not all(caps.get(k) is True for k in ('inline_insert','inline_update','inline_delete')):
            raise Blocked('Requires exact reviewed native DML runtime with installed SQL 0.4')
        self.check('legacy N4 installation remains SQL 0.4',
                   scalar(self.a, "SELECT extversion FROM pg_extension WHERE extname='og_lake'"), '0.4')
        result['runtime_version'] = runtime_version
        result['installed_extension_version'] = '0.4'
        self.manifest, self.exported, counts, artifacts = load_reference(self.a, args.manifest, args.run_id, args.output.parent)
        result['artifacts'].extend(artifacts)
        self.check('all real reference tables imported', len(counts), len(self.exported['tables']))
        required = 'LONG_TEXT' if args.mode == 'typed' else 'I'
        self.check('exact reference profile', self.manifest['case'], required)
        self.names = [c['name'] for c in self.manifest['schema']]
        self.initial = [{name:row[name] for name in self.names} for row in self.manifest['expected']]
        self.table_id = self.manifest['table_id']
        carriers = [r['table_name'] for t in self.exported['tables'] if t['table_name']=='ducklake_inlined_data_tables'
                    for r in t['rows'] if r['table_id']==self.table_id]
        self.check('one actual inline carrier', len(carriers), 1)
        self.carrier = carriers[0]
        query(self.a, 'CREATE SCHEMA biz')
        query(self.a, "SELECT og_lake.attach_catalog('lake_reference','lake_metadata',NULL::text,false)")
        query(self.a, foreign_table_statement('biz','orders','lake_reference',self.table_id,self.manifest['schema']))
        query(self.a, 'CREATE TABLE biz.heap_marker(id integer PRIMARY KEY,val integer)')
        self.groups, self.group_heads, self.operations = [], [], None
        self.expect(self.a,self.initial,'initial native values equal the actual reference')
        self.baseline = self.image(self.c)
        self.start_counter = self.counter()

    def check(self, name, actual, expected=True):
        good = actual == expected
        self.result['assertions'].append({'name':name,'status':'pass' if good else 'fail',
                                         'details':{'actual':actual,'expected':expected}})
        if not good:
            raise AssertionError(name)

    def records(self, conn):
        statement = self.sql.SQL('SELECT {} FROM biz.orders').format(self.sql.SQL(',').join(map(self.sql.Identifier,self.names)))
        return [dict(zip(self.names,row)) for row in query(conn,statement)]

    def expect(self, conn, expected, label):
        self.check(label,multiset(self.records(conn)),multiset(expected))

    def image(self, conn):
        # No concurrent visible mutation is allowed during this rollback/no-op oracle.
        return {t['table_name']:sorted(repr(tuple(row)) for row in query(conn,
            self.sql.SQL('SELECT * FROM lake_metadata.{}').format(self.sql.Identifier(t['table_name']))))
            for t in self.exported['tables']}

    def physical(self, conn=None):
        columns = ['_ducklake_row_id','_ducklake_begin_snapshot','_ducklake_end_snapshot'] + self.names
        stmt = self.sql.SQL('SELECT {} FROM lake_metadata.{}').format(
            self.sql.SQL(',').join(map(self.sql.Identifier,columns)),self.sql.Identifier(self.carrier))
        return [dict(zip(columns,row)) for row in query(conn or self.c,stmt)]

    def head(self):
        return scalar(self.c,'SELECT max(snapshot_id) FROM lake_metadata.ducklake_snapshot')

    def counter(self):
        return scalar(self.c,'SELECT next_row_id FROM lake_metadata.ducklake_table_stats WHERE table_id=%s',(self.table_id,))

    def stats(self):
        return query(self.c,'SELECT record_count,next_row_id,file_size_bytes FROM lake_metadata.ducklake_table_stats WHERE table_id=%s',(self.table_id,))[0]

    def begin(self):
        self.operations=[]
        query(self.a,'BEGIN')

    def mutate(self, statement, params=None):
        returned=query(self.a,statement,params)
        if self.operations is not None:
            self.operations.append({'sql':statement.replace('biz.orders','{table}').replace('%s','?'),
                                    'params':list(params or [])})
        return returned

    def commit(self):
        query(self.a,'COMMIT')
        self.groups.append(self.operations or [])
        self.group_heads.append(self.head())
        self.operations=None

    def reject_commit(self, conn, label, before):
        import psycopg2
        try:
            query(conn,'COMMIT')
        except psycopg2.Error as exc:
            self.check(label+' SQLSTATE',exc.pgcode,'40001')
            self.result.setdefault('conflict_messages',[]).append(str(exc))
        else:
            self.check(label+' rejects lost update','accepted','40001')
        query(conn,'ROLLBACK')
        self.check(label+' preserves the winning complete protocol state',self.image(self.c),before)
        self.check(label+' session recovered',scalar(conn,'SELECT 42'),42)

    def new_version(self, before_rows, identity, value, snapshot):
        matches=[r for r in before_rows if r['id']==identity and r['_ducklake_end_snapshot'] is None]
        self.check('exactly one initial source version for id '+str(identity),len(matches),1)
        expected=copy.deepcopy(before_rows)
        source=next(r for r in expected if r['id']==identity and r['_ducklake_end_snapshot'] is None)
        source['_ducklake_end_snapshot']=snapshot
        replacement=dict(source,**value)
        replacement['_ducklake_begin_snapshot']=snapshot
        replacement['_ducklake_end_snapshot']=None
        expected.append(replacement)
        self.check('one final version keeps the original logical identity',multiset(self.physical()),multiset(expected))

    def reverse(self, expected):
        from reverse_modify_reference import reverse_modify_reference
        path=self.args.output.parent/'reverse-reference'
        reply=reverse_modify_reference(self.c,'lake_metadata',self.args.manifest,
            self.args.output.parent/'reference-export-current.json',expected,path,
            operations_by_commit=self.groups,expected_commit_snapshots=self.group_heads)
        self.check('fixed actual DuckLake reads and independently replays native U/D',reply['status'],'pass')
        self.result['reference_assertions']=reply['assertions']
        self.result['artifacts'].extend({'path':str(p)} for p in path.rglob('*') if p.is_file())


def chains(t):
    initial=t.initial
    source=t.physical()
    start=t.head()
    t.begin()
    t.check('first UPDATE returns its new value',t.mutate('UPDATE biz.orders SET val=val+1 WHERE id=1 RETURNING id,val'),[(1,11)])
    wanted=[dict(r,val=11) if r['id']==1 else r for r in initial]
    t.expect(t.a,wanted,'next command sees first UPDATE')
    t.expect(t.b,initial,'another transaction cannot see the first UPDATE')
    t.check('second UPDATE uses the first private version',t.mutate('UPDATE biz.orders SET val=val*2 WHERE id=1 RETURNING id,val'),[(1,22)])
    wanted=[dict(r,val=22) if r['id']==1 else r for r in initial]
    t.commit()
    t.check('two UPDATEs publish one snapshot',t.head(),start+1)
    t.check('UPDATE consumes no new identity',t.counter(),t.start_counter)
    t.new_version(source,1,{'val':22},t.head())
    t.expect(t.b,wanted,'two UPDATEs expose only the final value')

    before=t.physical(); start=t.head(); counter=t.counter()
    t.begin()
    t.mutate('INSERT INTO biz.orders VALUES(101,1000)')
    t.check('UPDATE sees a previously inserted private row',t.mutate('UPDATE biz.orders SET val=val+5 WHERE id=101 RETURNING id,val'),[(101,1005)])
    t.check('second UPDATE still uses the same private identity',t.mutate('UPDATE biz.orders SET val=val*2 WHERE id=101 RETURNING id,val'),[(101,2010)])
    t.commit()
    wanted=wanted+[{'id':101,'val':2010}]
    expected=before+[{'id':101,'val':2010,'_ducklake_row_id':counter,'_ducklake_begin_snapshot':start+1,'_ducklake_end_snapshot':None}]
    t.check('INSERT then UPDATE twice creates only the final physical version',multiset(t.physical()),multiset(expected))
    t.check('INSERT then UPDATE twice allocates one permanent identity',t.counter(),counter+1)
    t.expect(t.b,wanted,'INSERT/UPDATE chain commits the expected values')

    before=t.image(t.c)
    t.begin()
    t.mutate('INSERT INTO biz.orders VALUES(102,1020)')
    t.mutate('UPDATE biz.orders SET val=val+1 WHERE id=102')
    t.check('DELETE RETURNING returns the latest private value',t.mutate('DELETE FROM biz.orders WHERE id=102 RETURNING id,val'),[(102,1021)])
    t.commit()
    t.check('INSERT then UPDATE then DELETE is a complete lake no-op',t.image(t.c),before)

    before=t.physical(); start=t.head(); stats=t.stats()
    t.begin()
    t.mutate('UPDATE biz.orders SET val=999 WHERE id=2')
    t.check('DELETE after UPDATE returns that private replacement',t.mutate('DELETE FROM biz.orders WHERE id=2 RETURNING id,val'),[(2,999)])
    t.commit()
    wanted=[r for r in wanted if r['id']!=2]
    expected=copy.deepcopy(before)
    next(r for r in expected if r['id']==2 and r['_ducklake_end_snapshot'] is None)['_ducklake_end_snapshot']=start+1
    t.check('UPDATE then DELETE only ends the original physical version',multiset(t.physical()),multiset(expected))
    t.check('pure final deletion retains stored statistics and allocation frontier',t.stats(),stats)
    t.expect(t.b,wanted,'UPDATE/DELETE final business values are exact')
    t.reverse(wanted)


def commands(t):
    expected=copy.deepcopy(t.initial)
    t.begin()
    actual=t.mutate('UPDATE biz.orders SET val=val+1 WHERE val<1000 RETURNING id,val')
    expected=[dict(r,val=r['val']+1) for r in expected]
    t.check('qualifying UPDATE processes each old version once',multiset(actual),multiset([[r['id'],r['val']] for r in expected]))
    actual=t.mutate('UPDATE biz.orders o SET val=s.val+2 FROM biz.orders s WHERE o.id=s.id RETURNING o.id,o.val')
    expected=[dict(r,val=r['val']+2) for r in expected]
    t.check('self-join UPDATE reads previous-command values without feedback',multiset(actual),multiset([[r['id'],r['val']] for r in expected]))
    t.expect(t.a,expected,'following SELECT sees final command versions')
    t.expect(t.b,t.initial,'all updates remain private')
    t.commit()
    t.expect(t.b,expected,'all final updated values publish together')
    t.check('multi-command UPDATE preserves permanent identity frontier',t.counter(),t.start_counter)
    before=t.image(t.c)
    t.begin()
    t.check('zero-match UPDATE returns no affected rows',t.mutate('UPDATE biz.orders SET val=0 WHERE id=9999 RETURNING id'),[])
    t.check('zero-match DELETE returns no affected rows',t.mutate('DELETE FROM biz.orders WHERE id=9999 RETURNING id'),[])
    t.commit()
    t.check('isolated zero-match U/D publishes no snapshot',t.image(t.c),before)
    for exclusion in ('off', 'on'):
        query(t.a,'SET constraint_exclusion='+exclusion)
        for predicate in ('false', 'id<0 AND id>0'):
            for operation in ('UPDATE biz.orders SET val=0', 'DELETE FROM biz.orders'):
                statement=operation+' WHERE '+predicate+' RETURNING id'
                plan='\n'.join(r[0] for r in query(t.a,'EXPLAIN ANALYZE '+statement))
                label=exclusion+'/'+predicate+'/'+operation.split()[0]
                t.result.setdefault('empty_plans',[]).append({'case':label,'plan':plan})
                # This host can keep a ForeignScan below a false one-time gate.
                # Constraint exclusion of contradictory column bounds instead
                # exercises the leaf dummy Result with no locator-producing scan.
                if exclusion=='on' and predicate!='false':
                    t.check(label+' really uses a leaf dummy Result',
                            'Result' in plan and 'Foreign Scan' not in plan)
                if predicate=='false' and 'Foreign Scan' in plan:
                    t.check(label+' gated ForeignScan is never executed','never executed' in plan)
                t.check(label+' returns no rows and never executes the marker',query(t.a,statement),[])
    query(t.a,'RESET constraint_exclusion')
    query(t.a,'PREPARE empty_update(integer,boolean) AS UPDATE biz.orders SET val=0 WHERE id=$1 AND $2 RETURNING id')
    query(t.a,'PREPARE empty_delete(integer,boolean) AS DELETE FROM biz.orders WHERE id=$1 AND $2 RETURNING id')
    for attempt in range(8):
        t.check('prepared empty UPDATE remains valid across plan reuse '+str(attempt),query(t.a,'EXECUTE empty_update(1,false)'),[])
        t.check('prepared empty DELETE remains valid across plan reuse '+str(attempt),query(t.a,'EXECUTE empty_delete(1,false)'),[])
    query(t.a,'DEALLOCATE empty_update'); query(t.a,'DEALLOCATE empty_delete')
    t.check('constant and prepared empty plans never publish protocol changes',t.image(t.c),before)


def savepoints(t):
    before=t.physical()
    query(t.a,'BEGIN')
    query(t.a,'UPDATE biz.orders SET val=11 WHERE id=1')
    query(t.a,'SAVEPOINT outer_sp')
    query(t.a,'UPDATE biz.orders SET val=12 WHERE id=1')
    query(t.a,'SAVEPOINT inner_sp')
    query(t.a,'DELETE FROM biz.orders WHERE id=1')
    query(t.a,'RELEASE SAVEPOINT inner_sp')
    t.expect(t.a,[r for r in t.initial if r['id']!=1],'released nested delete is visible privately')
    query(t.a,'ROLLBACK TO SAVEPOINT outer_sp')
    query(t.a,'RELEASE SAVEPOINT outer_sp')
    wanted=[dict(r,val=11) if r['id']==1 else r for r in t.initial]
    t.expect(t.a,wanted,'outer rollback restores the parent private update')
    query(t.a,'INSERT INTO biz.orders VALUES(201,2010)')
    query(t.a,'SAVEPOINT private_delete')
    query(t.a,'DELETE FROM biz.orders WHERE id=201')
    query(t.a,'ROLLBACK TO SAVEPOINT private_delete')
    query(t.a,'RELEASE SAVEPOINT private_delete')
    wanted.append({'id':201,'val':2010})
    t.expect(t.a,wanted,'rolled-back private delete restores its parent INSERT')
    query(t.a,'COMMIT')
    t.expect(t.b,wanted,'only surviving parent states commit')
    t.check('only the surviving new logical identity is allocated',t.counter(),t.start_counter+1)
    expected_physical=copy.deepcopy(before)
    source=next(r for r in expected_physical if r['id']==1 and r['_ducklake_end_snapshot'] is None)
    source['_ducklake_end_snapshot']=t.head()
    expected_physical.append(dict(source,val=11,_ducklake_begin_snapshot=t.head(),_ducklake_end_snapshot=None))
    expected_physical.append({'id':201,'val':2010,'_ducklake_row_id':t.start_counter,
                              '_ducklake_begin_snapshot':t.head(),'_ducklake_end_snapshot':None})
    t.check('savepoint publication keeps the original identity and only two final physical versions',
            multiset(t.physical()),multiset(expected_physical))
    saved=t.image(t.c)
    query(t.a,'BEGIN')
    query(t.a,'UPDATE biz.orders SET val=13 WHERE id=1')
    query(t.a,'DELETE FROM biz.orders WHERE id=3')
    query(t.a,'INSERT INTO biz.heap_marker VALUES(1,10)')
    query(t.a,'ROLLBACK')
    t.check('top abort restores every standard protocol value',t.image(t.c),saved)
    t.check('top abort also restores ordinary heap',scalar(t.c,'SELECT count(*) FROM biz.heap_marker'),0)
    t.expect(t.c,wanted,'top abort preserves previously committed versions')


def joins(t):
    query(t.a,'CREATE TABLE biz.drive(id integer)')
    query(t.a,'INSERT INTO biz.drive VALUES(1),(1),(1),(2),(2)')
    expected=copy.deepcopy(t.initial)
    for method,enabled in [('Nested Loop','enable_nestloop'),('Hash Join','enable_hashjoin'),('Merge Join','enable_mergejoin')]:
        for setting in ('enable_nestloop','enable_hashjoin','enable_mergejoin'):
            query(t.a,'SET '+setting+'='+('on' if setting==enabled else 'off'))
        statement='UPDATE biz.orders o SET val=o.val+5 FROM biz.drive d WHERE o.id=d.id RETURNING o.id,o.val'
        before=t.image(t.c)
        plan='\n'.join(r[0] for r in query(t.a,'EXPLAIN '+statement))
        t.result.setdefault('actual_plans',{})[method]=plan
        t.check(method+' was actually selected for the locator path',method in plan)
        t.check('EXPLAIN '+method+' changes no protocol value',t.image(t.c),before)
        returned=query(t.a,statement)
        expected=[dict(r,val=r['val']+5) if r['id'] in (1,2) else r for r in expected]
        t.check(method+' duplicate matches update and RETURN exactly once per old identity',
                multiset(returned),multiset([[r['id'],r['val']] for r in expected if r['id'] in (1,2)]))
        t.expect(t.c,expected,method+' business result is exact')
    for setting in ('enable_nestloop','enable_hashjoin','enable_mergejoin'): query(t.a,'RESET '+setting)
    deleted=query(t.a,'DELETE FROM biz.orders o USING biz.drive d WHERE o.id=d.id RETURNING o.id,o.val')
    t.check('DELETE USING duplicate matches return each old value once',multiset(deleted),
            multiset([[r['id'],r['val']] for r in expected if r['id'] in (1,2)]))
    expected=[r for r in expected if r['id'] not in (1,2)]
    query(t.a,'INSERT INTO biz.orders VALUES(77,700),(77,701)')
    returned=query(t.a,'UPDATE biz.orders SET val=val+1 WHERE id=77 RETURNING id,val')
    t.check('equal business IDs remain two distinct logical rows',multiset(returned),multiset([[77,701],[77,702]]))
    expected.extend([{'id':77,'val':701},{'id':77,'val':702}])
    t.expect(t.c,expected,'logical-identity deduplication never deduplicates equal business keys')
    live=[r['_ducklake_row_id'] for r in t.physical() if r['_ducklake_end_snapshot'] is None]
    t.check('distinct logical rows have distinct live permanent identities',len(set(live)),len(expected))


def conflicts(t):
    query(t.a,'BEGIN'); query(t.b,'BEGIN')
    query(t.a,'UPDATE biz.orders SET val=501 WHERE id=1')
    query(t.b,'UPDATE biz.orders SET val=502 WHERE id=1')
    query(t.b,'INSERT INTO biz.heap_marker VALUES(1,502)')
    query(t.a,'COMMIT')
    winner=t.image(t.c)
    t.reject_commit(t.b,'concurrent updates of the same source version',winner)
    t.check('losing UPDATE rolls back ordinary heap',scalar(t.c,'SELECT count(*) FROM biz.heap_marker'),0)
    expected=[dict(r,val=501) if r['id']==1 else r for r in t.initial]
    t.expect(t.c,expected,'winning UPDATE is not overwritten')
    for offset,operation in enumerate(('UPDATE','DELETE')):
        incoming=930+offset
        query(t.a,'BEGIN')
        statement=('UPDATE biz.orders SET val=600 WHERE id=1 RETURNING id' if operation=='UPDATE'
                   else 'DELETE FROM biz.orders WHERE id=1 RETURNING id')
        t.check('real '+operation+' matched an existing source version',query(t.a,statement),[(1,)])
        query(t.a,'INSERT INTO biz.heap_marker VALUES(%s,%s)',(incoming,incoming))
        query(t.b,'INSERT INTO biz.orders VALUES(%s,%s)',(incoming,incoming))
        expected.append({'id':incoming,'val':incoming})
        winner=t.image(t.c)
        t.reject_commit(t.a,'real '+operation+' intent conflicts with a later same-table INSERT',winner)
        t.check('real '+operation+' conflict rolls back ordinary heap',scalar(t.c,'SELECT count(*) FROM biz.heap_marker'),0)
        t.expect(t.c,expected,'real '+operation+' conflict restores its original business version')
    for offset,operation in enumerate(('UPDATE','DELETE','DELETE FALSE')):
        own_id=810+offset; incoming=910+offset
        query(t.a,'BEGIN')
        query(t.a,'INSERT INTO biz.orders VALUES(%s,%s)',(own_id,own_id))
        query(t.a,'INSERT INTO biz.heap_marker VALUES(%s,%s)',(own_id,own_id))
        statement=('UPDATE biz.orders SET val=0 WHERE id=%s' if operation=='UPDATE' else
                   'DELETE FROM biz.orders WHERE id=%s' if operation=='DELETE' else 'DELETE FROM biz.orders WHERE false')
        query(t.a,statement,None if operation=='DELETE FALSE' else (incoming,))
        query(t.b,'INSERT INTO biz.orders VALUES(%s,%s)',(incoming,incoming))
        expected.append({'id':incoming,'val':incoming})
        winner=t.image(t.c)
        t.reject_commit(t.a,'zero-match '+operation+' intent with a nonempty lake publication',winner)
        t.check('conflicting zero-match '+operation+' also rolls back ordinary heap',scalar(t.c,'SELECT count(*) FROM biz.heap_marker'),0)
        t.expect(t.c,expected,'conflicting zero-match '+operation+' preserves only winning inserts')
    query(t.a,'BEGIN')
    query(t.a,'INSERT INTO biz.orders VALUES(821,821)')
    query(t.a,'SAVEPOINT retained_intent')
    query(t.a,'UPDATE biz.orders SET val=0 WHERE id=921')
    query(t.a,'RELEASE SAVEPOINT retained_intent')
    query(t.b,'INSERT INTO biz.orders VALUES(921,921)')
    expected.append({'id':921,'val':921})
    t.reject_commit(t.a,'RELEASE adopts the child zero-match intent into parent publication',t.image(t.c))
    t.expect(t.c,expected,'released child intent conflicts and rolls back its parent INSERT')
    query(t.a,'BEGIN')
    query(t.a,'INSERT INTO biz.orders VALUES(820,820)')
    query(t.a,'SAVEPOINT discarded_intent')
    query(t.a,'DELETE FROM biz.orders WHERE id=920')
    query(t.a,'ROLLBACK TO SAVEPOINT discarded_intent')
    query(t.a,'RELEASE SAVEPOINT discarded_intent')
    query(t.b,'INSERT INTO biz.orders VALUES(920,920)')
    query(t.a,'COMMIT')
    expected.extend([{'id':820,'val':820},{'id':920,'val':920}])
    t.expect(t.c,expected,'rolled-back zero-match intent does not poison a real parent INSERT publication')


def cancel(t):
    if scalar(t.a,'SHOW enable_thread_pool')=='on':
        raise Blocked('The exact backend transaction-lock barrier currently requires dedicated sessions')
    carrier=t.sql.SQL('lake_metadata.{}').format(t.sql.Identifier(t.carrier))
    publisher=scalar(t.a,'SELECT pg_backend_pid()'); holder=scalar(t.b,'SELECT pg_backend_pid()')
    query(t.a,'SET statement_timeout=30000')
    query(t.a,'BEGIN')
    query(t.a,'UPDATE biz.orders SET val=777 WHERE id=1')
    query(t.a,'INSERT INTO biz.heap_marker VALUES(1,777)')
    # No protocol-value alteration: lock one genuine source version by an
    # equal-value update, then always abort that holder transaction.
    query(t.b,'BEGIN')
    locked=query(t.b,t.sql.SQL('UPDATE {} SET _ducklake_end_snapshot=_ducklake_end_snapshot WHERE id=1 AND _ducklake_end_snapshot IS NULL RETURNING id').format(carrier))
    t.check('barrier locked exactly the genuine source version',locked,[(1,)])
    with ThreadPoolExecutor(max_workers=1) as pool:
        future=pool.submit(query,t.a,'COMMIT')
        try:
            deadline=time.monotonic()+12
            while True:
                waits=query(t.c,'SELECT w.pid,w.transactionid,h.pid FROM pg_locks w JOIN pg_locks h '
                    'ON h.locktype=w.locktype AND h.transactionid=w.transactionid '
                    "WHERE w.pid=%s AND h.pid=%s AND w.locktype='transactionid' AND NOT w.granted AND h.granted",(publisher,holder))
                if waits: break
                if future.done(): raise AssertionError('Publisher finished before the controlled source-version wait')
                if time.monotonic()>=deadline: raise AssertionError('Publisher did not reach the actual holder transaction lock')
                time.sleep(.02)
            t.result['observed_source_version_wait']=[list(r) for r in waits]
            t.check('uncommitted publication exposes no partial standard state',t.image(t.c),t.baseline)
            t.expect(t.c,t.initial,'old business version stays visible during source update wait')
            t.check('cancel only the known owned publisher',scalar(t.c,'SELECT pg_cancel_backend(%s)',(publisher,)),True)
            try: future.result(timeout=15)
            except Exception as exc: t.check('cancelled native UPDATE commit returns actual cancellation',getattr(exc,'pgcode',None),'57014')
            else: t.check('UPDATE cancellation cannot be ignored','accepted','57014')
        finally:
            query(t.b,'ROLLBACK')
    query(t.a,'ROLLBACK')
    t.check('cancel restores every standard protocol value',t.image(t.c),t.baseline)
    t.check('cancel restores ordinary heap with lake state',scalar(t.c,'SELECT count(*) FROM biz.heap_marker'),0)
    query(t.a,'UPDATE biz.orders SET val=778 WHERE id=1')
    t.expect(t.c,[dict(r,val=778) if r['id']==1 else r for r in t.initial],'same session commits a later supported UPDATE')


def typed(t):
    replacement='后继版本\n'+('x'*35000)
    t.begin()
    changed=t.mutate('UPDATE biz.orders SET val=%s,flag=%s,bigval=%s,note=%s WHERE id=1 RETURNING id,val,flag,bigval,note',
                     (-2147483648,False,-9223372036854775808,replacement))
    expected=[dict(r,val=-2147483648,flag=False,bigval=-9223372036854775808,note=replacement) if r['id']==1 else r for r in t.initial]
    t.check('typed UPDATE RETURNING preserves boundaries NULL and long UTF-8 values',multiset(changed),
            multiset([[r[name] for name in t.names] for r in expected if r['id']==1]))
    t.expect(t.a,expected,'private typed replacement is exact')
    t.expect(t.b,t.initial,'old long text stays visible to another transaction')
    t.commit()
    t.expect(t.c,expected,'committed long-text replacement is exact')
    t.begin()
    returned=t.mutate('UPDATE biz.orders SET val=NULL,flag=NULL,bigval=NULL,note=NULL WHERE id=2 RETURNING id,val,flag,bigval,note')
    t.check('UPDATE can assign actual NULL to all admitted value types',returned,[(2,None,None,None,None)])
    expected=[dict(r,val=None,flag=None,bigval=None,note=None) if r['id']==2 else r for r in expected]
    returned=t.mutate('UPDATE biz.orders SET val=%s,flag=%s,bigval=%s,note=%s WHERE id=3 RETURNING id,val,flag,bigval,note',
                     (2147483647,True,9223372036854775807,''))
    t.check('UPDATE preserves positive boundaries boolean and empty text',returned,[(3,2147483647,True,9223372036854775807,'')])
    expected=[dict(r,val=2147483647,flag=True,bigval=9223372036854775807,note='') if r['id']==3 else r for r in expected]
    t.commit()
    t.expect(t.c,expected,'NULL and non-NULL typed updates publish exactly')
    old=next(r for r in expected if r['id']==1)
    t.begin()
    returned=t.mutate('DELETE FROM biz.orders WHERE id=1 RETURNING id,val,flag,bigval,note')
    t.check('DELETE RETURNING contains the complete old business value including long text',multiset(returned),multiset([[old[n] for n in t.names]]))
    t.commit()
    expected=[r for r in expected if r['id']!=1]
    t.expect(t.c,expected,'typed DELETE leaves every untouched value exact')
    t.check('typed UPDATE and DELETE allocate no new identity',t.counter(),t.start_counter)
    t.reverse(expected)


def journal_memory(t):
    pool_enabled=scalar(t.a,'SHOW enable_thread_pool')=='on'
    def diagnostic(): return json.loads(scalar(t.a,'SELECT og_lake.host_status()'))
    def measured():
        if pool_enabled:
            rows=query(t.a,"SELECT contextname,usedsize FROM pv_session_memory_detail() "
                           "WHERE split_part(sessid,'.',2)::bigint=pg_current_sessid() "
                           "AND threadid=pg_backend_pid() AND contextname LIKE 'Native lake%'")
        else:
            rows=query(t.a,"SELECT contextname,usedsize FROM pv_thread_memory_detail() WHERE tid=pg_backend_pid() AND contextname LIKE 'Native lake%'")
        t.check('independent host memory view actually finds native lake contexts',len(rows)>0 and sum(int(r[1]) for r in rows)>0)
        return sum(int(r[1]) for r in rows)
    query(t.a,'BEGIN')
    query(t.a,'UPDATE biz.orders SET val=11 WHERE id=1')
    parent=diagnostic(); parent_memory=measured()
    t.check('parent owns one surviving private replacement',parent['transaction_rows'],1)
    samples=[]
    for attempt in range(12):
        query(t.a,'SAVEPOINT many_short_versions')
        query(t.a,'INSERT INTO biz.orders SELECT 10000+g,g FROM generate_series(1,10000) t(g)')
        query(t.a,'UPDATE biz.orders SET val=val+1 WHERE id>10000')
        query(t.a,'DELETE FROM biz.orders WHERE id>10000 AND id%2=0')
        if attempt==0:
            live=diagnostic(); actual_memory=measured()
            t.result['active_short_version_observation']={'host':live,'independent_used_bytes':actual_memory}
            t.check('actual short-row versions produce measurable allocations',actual_memory>parent_memory+1024*1024)
            t.check('staged rows count only final live private versions',live['transaction_rows'],5001)
            t.check('short-row version journal is charged',live['staged_bytes']>parent['staged_bytes'])
        query(t.a,'ROLLBACK TO SAVEPOINT many_short_versions')
        query(t.a,'RELEASE SAVEPOINT many_short_versions')
        after=diagnostic()
        t.check('rollback restores parent active rows '+str(attempt),after['transaction_rows'],1)
        t.check('rollback restores parent journal budget '+str(attempt),after['staged_bytes'],parent['staged_bytes'])
        samples.append({'round':attempt,'independent_used_bytes':measured(),
                        'host_allocated_bytes':after['transaction_memory_bytes']})
    t.result['short_version_rollback_samples']=samples
    # Hash bucket/freelist capacity may retain the first peak. Subsequent equal
    # batches must reuse it, rather than accumulate another peak every round.
    t.check('repeated short-row rollback memory stabilizes after warmup',
            samples[-1]['independent_used_bytes']<=samples[1]['independent_used_bytes']+1024*1024)
    t.check('host total allocation also stabilizes after warmup',
            samples[-1]['host_allocated_bytes']<=samples[1]['host_allocated_bytes']+1024*1024)
    wanted=[dict(r,val=11) if r['id']==1 else r for r in t.initial]
    t.expect(t.a,wanted,'all discarded versions disappear while parent replacement survives')
    query(t.a,'INSERT INTO biz.orders VALUES(101,1010)')
    query(t.a,'COMMIT')
    t.expect(t.c,wanted+[{'id':101,'val':1010}],'same outer transaction remains able to publish supported writes')
    t.check('discarded short-row batches consumed no permanent identities',t.counter(),t.start_counter+1)
    t.result['notes'].append('The 64 MiB limit charges the journal, not total memory-context allocation. Hash capacity retained at the first peak is measured; equal later batches must stabilize.')


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-id',default=os.getenv('OG_LAKE_RUN_ID'))
    parser.add_argument('--output',default=os.getenv('OG_LAKE_RESULT_FILE'))
    parser.add_argument('--manifest',default=os.getenv('OG_LAKE_FIXTURE_MANIFEST'))
    parser.add_argument('--mode',required=True,choices=['chains','commands','savepoints','joins','conflicts','cancel','typed','journal_memory'])
    args=parser.parse_args()
    if not all((args.run_id,args.output,args.manifest)): parser.error('run-id, output and actual reference manifest required')
    validate_run_id(args.run_id)
    args.output=Path(args.output).resolve(); args.output.parent.mkdir(parents=True,exist_ok=True)
    result={'status':'fail','scope':'N4/'+args.mode,'assertions':[],'artifacts':[],
            'notes':['This receipt covers only the selected mode on its recorded source and binary.',
                     'Only actual administrator-owned pure-inline format 1.1 inputs are admitted; no file SDK, RAC, partition or general SQL support is claimed.']}
    owned=None
    try:
        with OwnedDatabase(args.run_id,args.output.parent) as owned:
            case=Case(owned,args,result)
            globals()[args.mode](case)
            result['status']='pass'
    except Blocked as exc: result.update(status='blocked',reason=str(exc))
    except BaseException as exc:
        result.update(status='fail',error=repr(exc),traceback=traceback.format_exc())
        if not result['assertions'] or result['assertions'][-1]['status']!='fail':
            result['assertions'].append({'name':'N4 scenario completed','status':'fail','details':repr(exc)})
    finally:
        if owned is not None:
            result['artifacts'].extend(owned.artifacts()); result['cleanup']=owned.cleanup_result
        write_json(args.output,result)
        print(json.dumps({'status':result['status'],'mode':args.mode,'output':str(args.output)}))
    return {'pass':0,'blocked':2}.get(result['status'],1)


if __name__=='__main__':
    raise SystemExit(main())
