#!/usr/bin/env python3
"""SQL0.19 bounded pure-new-identity direct INSERT qualification draft.

Default is prepare: no Native import, connection, fixture, process or build.
--execute requires an official build and an explicit reviewed source contract.
Every case owns its database and real EMPTY fixture. This is a bounded journal
path, not streaming bulk COPY, a live external catalog connector or RAC proof.
"""
import argparse
from collections import Counter
from copy import deepcopy
import hashlib
import json
import math
import os
from pathlib import Path
import shutil
import subprocess
import sys
import threading
import time
import traceback

ROOT = Path('/home/qiumc/data/code/cpp')
D = ROOT / 'integration/opengauss-native-lake'
HERE = Path(__file__).resolve().parent
STAGE = HERE.parent
MODES = ('boundary', 'journal', 'cancel', 'claim', 'schema', 'carrier', 'limits', 'interop', 'types', 'legacy', 'carrier-legacy', 'callbacks')
COMBINED = '538c03cf68410df0b39a45f3668777a209998790f591db8c500d445f9b16652d'
SCOPE = 'SQL019 bounded direct INSERT; actual SDK Parquet, host RC journal and standard DuckLake offline interread'
LIMITATIONS = [
    'Local Debug, serial fixtures and heap baseline; no production/RAC or Native SMP qualification.',
    'At most512 ordinary rows here;64MiB journal/16MiB Arrow batch/16 candidates remain hard bounds.',
    'Unknown external DDL and reverse reads replay unmodified real reference rows in an owned DB; not a live connector.',
    'No prewrite SDK fault-point injection is claimed; actual post-finish SPI cancellation is deterministic.',
    'Payload spill and transaction-private files are future work, not passing cases in this suite.',
]


def digest(path):
    with Path(path).open('rb') as stream:
        h = hashlib.sha256()
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            h.update(block)
        return h.hexdigest()


def dump(path, value):
    Path(path).write_text(json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False) + '\n')


def canon(value):
    if isinstance(value, float):
        if math.isnan(value): return 'NaN'
        if math.isinf(value): return 'Infinity' if value > 0 else '-Infinity'
    if isinstance(value, dict): return {str(k): canon(v) for k, v in value.items()}
    if isinstance(value, (set, frozenset)): return sorted(canon(v) for v in value)
    if isinstance(value, (list, tuple)): return [canon(v) for v in value]
    return value


def jsonsafe(value):
    if isinstance(value, bytes):
        import base64
        return {'encoding': 'base64', 'bytes': len(value), 'value': base64.b64encode(value).decode()}
    if isinstance(value, Path): return str(value)
    if isinstance(value, dict): return {str(k): jsonsafe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)): return [jsonsafe(v) for v in value]
    return canon(value)


def source_contract(path, build):
    """Reviewed final overlay, not the known-bad538c snapshot, admits execution."""
    contract = json.loads(path.read_text())
    if (contract.get('schema_version') != 1 or contract.get('purpose') != 'sql019_direct_insert_qualification'
            or contract.get('ready_for_native_qualification') is not True or contract.get('known_blockers') != []
            or contract.get('base_patch_sha256') != COMBINED):
        raise RuntimeError('A reviewed final SQL019 source contract including carrier/schema fix is required')
    required = {'contrib/og_lake/lake_publish.cpp', 'contrib/og_lake/lake_catalog.cpp',
                'contrib/og_lake/lake_create_publish.cpp', 'contrib/og_lake/og_lake--0.19.sql',
                'contrib/og_lake/og_lake--0.18--0.19.sql'}
    inputs = contract.get('native_inputs', {})
    if not required <= inputs.keys() or not contract.get('review_evidence'):
        raise RuntimeError('Final source contract lacks core source hashes/review provenance')
    for name, sha in inputs.items():
        if Path(name).is_absolute() or '..' in Path(name).parts:
            raise RuntimeError('Unsafe source contract input')
        if build['native_source_inputs_after'].get(name) != sha:
            raise RuntimeError('Official build differs from reviewed SQL019 candidate: ' + name)
    helper_inputs=contract.get('shared_helper_inputs',{})
    if set(helper_inputs)!={'native_test_context.py','tests/native_table_create.py'}:
        raise RuntimeError('Reviewed commonSQL19 helper identities are missing')
    for name,sha in helper_inputs.items():
        if digest(D/name)!=sha:
            raise RuntimeError('SharedSQL19 test helper differs from reviewed candidate: '+name)
    for name, sha in contract['review_evidence'].items():
        if digest(name) != sha: raise RuntimeError('Reviewed provenance changed: ' + name)
    return contract


def archive_inputs(out, build, contract_path):
    inputs = [Path(__file__), 
              HERE/'native_direct_matrix.json', contract_path, D/'tests/file_dml_acceptance.py',
              D/'native_test_context.py', D/'tests/native_table_create.py', D/'tests/permission_api.py',
              D/'tests/inline_cleanup.py', D/'tests/catalog_fixture_bundle.py', D/'tests/empty_manifest.py',
              D/'tests/empty_reference_fixture.py', D/'tests/empty_catalog_oracle.py',
              D/'tests/create_reference_oracle.py', D/'tests/reverse_reference.py', D/'tests/precommit_probe.py',
              D/'tests/native_auto_flush.py', D/'inline_fixture.py', D/'native_runtime.py',
              D/'lake_harness.py', D/'fixture.py', D/'reference.sh', D/'source-lock.json']
    inputs += [Path(build['source']) / name for name in build['native_source_inputs_after']]
    inputs += [Path(name) for name in build.get('build_wrapper_inputs_after', {})]
    prefix = Path(build['install'])
    inputs += [prefix/'bin/gaussdb', prefix/'lib/postgresql/og_lake_sdk/libog_lake_sdk_arrow.so',
               prefix/'share/postgresql/extension/og_lake.control']
    inputs += list((prefix/'share/postgresql/extension').glob('og_lake--*.sql'))
    ref_receipt = ROOT/'.reference/opengauss-native-lake/build/receipt.json'
    inputs.append(ref_receipt)
    ref = json.loads(ref_receipt.read_text())
    lock = json.loads((D/'source-lock.json').read_text())
    ref_binary = ROOT/'.reference/opengauss-native-lake'/lock['reference_build']['build_subdir']/'duckdb'
    if digest(ref_binary) != ref['binary_sha256'] or ref['source_lock_hash'] != digest(D/'source-lock.json'):
        raise RuntimeError('Pinned genuine DuckLake runtime is stale')
    inputs.append(ref_binary)
    frozen = {}
    arc = out/'executed-inputs'; arc.mkdir()
    for i, path in enumerate(dict.fromkeys(p.resolve() for p in inputs)):
        target = arc / ('%04d-' % i + path.name)
        shutil.copy2(path, target)
        sha = digest(path)
        if digest(target) != sha: raise RuntimeError('Input archive differs: ' + str(path))
        frozen[str(path)] = {'sha256': sha, 'archive': str(target), 'bytes': path.stat().st_size}
    dump(arc/'index.json', frozen)
    return frozen


class Direct:
    def __init__(self, args, out, result, db, roles):
        from types import SimpleNamespace
        from native_table_create import Case
        self.args, self.out, self.r, self.db, self.roles = args, out, result, db, roles
        self.t = Case(db, SimpleNamespace(mode='net_dml', sql_version=args.sql_version,
                    manifest=out/'fixture/manifest.json', output=out/'result.json', run_id=args.run_id), result, roles)
        self.a, self.b, self.o = self.t.a, self.t.b, self.t.observer
        for c in [self.a, self.b, self.o, db.admin]: self.session(c)
        self.root = Path(self.t.empty_manifest['data_path']).resolve()
        self.holders, self.threads = [], []
        self.ref_count = 0
        self.check('source runtime is explicitly reviewed19/20/21',reviewed_runtime_version() in ['0.19','0.20','0.21'])
        self.check('installed exact SQL version', scalar(self.a,"SELECT extversion FROM pg_extension WHERE extname='og_lake'"), args.sql_version)
        query(self.a, 'SET enable_default_ustore_table=off')

    def check(self, name, got, want=True):
        a, b = canon(got), canon(want)
        passed = a == b
        self.r['assertions'].append({'name': name, 'observed': deepcopy(a), 'expected': deepcopy(b),
                                      'status': 'pass' if passed else 'fail'})
        if not passed: raise AssertionError(name + ': ' + repr(a)[:400])

    def session(self, c):
        query(c, 'SET session_timeout=0'); query(c, 'SET statement_timeout=30000')
        query(c, 'SET query_dop=1');query(c, 'SET enable_lake_smp=off')
        query(c, "SET try_vector_engine_strategy='off'")

    def create(self, name='orders', columns='id integer,val integer'):
        from psycopg2 import sql
        query(self.a, sql.SQL("CREATE FOREIGN TABLE biz.{}({}) SERVER created_lake "
              "OPTIONS(binding_mode 'create',lake_schema 'main')").format(sql.Identifier(name), sql.SQL(columns)))
        self.check('permanent empty relation '+name, self.rows(name), [])

    def rows(self, name='orders', conn=None, val='val'):
        from psycopg2 import sql
        return query(conn or self.o, sql.SQL('SELECT id,{} FROM biz.{} ORDER BY id').format(sql.Identifier(val),sql.Identifier(name)))

    def limit(self, n, name='orders'):
        query(self.a,'SELECT og_lake.set_option(%s::regclass,%s,%s)', ('biz.'+name,'data_inlining_row_limit',str(n)))

    def tid(self, name):
        return scalar(self.o, 'SELECT table_id FROM created_metadata.ducklake_table WHERE table_name=%s AND end_snapshot IS NULL',(name,))

    def stats(self, name):
        rows = query(self.o, 'SELECT record_count,next_row_id,file_size_bytes FROM created_metadata.ducklake_table_stats WHERE table_id=%s',(self.tid(name),))
        return rows[0] if rows else (0,0,0)

    def count(self, name):
        return scalar(self.o, 'SELECT count(*) FROM created_metadata.ducklake_data_file WHERE table_id=%s AND end_snapshot IS NULL',(self.tid(name),))

    def changes(self, name):
        return scalar(self.o, 'SELECT COALESCE(max(s.committed_changes),0) FROM og_lake_internal.analyze_state s '
            'WHERE s.relation_oid=%s::regclass', ('biz.'+name,)) if self.args.sql_version in ['0.19','0.20','0.21'] else None

    def disk(self):
        return {str(p.resolve()): digest(p) for p in sorted(self.root.rglob('*.parquet')) if p.is_file()}

    def state(self):
        return {'standard': self.t.protocol(), 'host': self.t.host_image(), 'heap': query(self.o,'SELECT * FROM biz.heap_marker ORDER BY id'),
                'autoanalyze': query(self.o,'SELECT binding_id::text,relation_oid,table_uuid::text,committed_changes,analyzed_changes '
                                   'FROM og_lake_internal.analyze_state ORDER BY binding_id')
                                 if self.args.sql_version in ['0.19','0.20','0.21'] else [], 'files': self.disk()}

    def insert(self, start, count, name='orders', conn=None):
        from psycopg2 import sql
        query(conn or self.a, sql.SQL('INSERT INTO biz.{} SELECT i,i*10 FROM generate_series(%s,%s)i').format(sql.Identifier(name)),(start,start+count-1))

    def reject(self, conn, statement, state, params=None):
        try: query(conn, statement, params)
        except Exception as exc:
            self.check('rejection '+str(statement)[:90], getattr(exc,'pgcode',None), state)
            return {'sqlstate': getattr(exc,'pgcode',None), 'message': str(exc)}
        raise AssertionError('Unexpected success: '+str(statement))

    def replay_actual(self, conn, before, actual):
        # DDL-description adaptation only: all column/constraint semantics must
        # match. The actual reference row dictionaries are passed unchanged.
        from empty_catalog_oracle import canonical_shape
        from inline_fixture import replace_catalog_values
        previous={x['table_name']:x for x in before['tables']}
        self.check('real reference replay retains exact standard table set',sorted(x['table_name'] for x in actual['tables']),sorted(previous))
        adapted=[]
        for table in actual['tables']:
            old=previous[table['table_name']]
            self.check('real physical shape before replay '+table['table_name'],canonical_shape(table),canonical_shape(old))
            adapted.append(dict(table,columns=deepcopy(old['columns']),constraints=deepcopy(old['constraints'])))
        return replace_catalog_values(conn,'created_metadata',before,{'tables':adapted})

    def reference(self, label):
        from reverse_reference import Reference
        lock = json.loads((D/'source-lock.json').read_text())
        receipt = json.loads((ROOT/'.reference/opengauss-native-lake/build/receipt.json').read_text())
        directory = self.out/label; directory.mkdir()
        return Reference(directory, lock, receipt)

    def file_rows(self, name):
        return query(self.o,'SELECT data_file_id,begin_snapshot,path,record_count,file_size_bytes,row_id_start '
            'FROM created_metadata.ducklake_data_file WHERE table_id=%s AND end_snapshot IS NULL ORDER BY data_file_id',(self.tid(name),))

    def path(self, relative):
        matches = [p.resolve() for p in self.root.rglob(relative) if p.is_file()]
        self.check('metadata output path resolves uniquely '+relative, len(matches), 1)
        self.check('actual file remains inside owned root', matches[0].is_relative_to(self.root))
        return matches[0]

    def physical(self, name, expected, label):
        """Derive R and birth only from actual file registration and originalP."""
        from reverse_reference import literal, qi
        ref = self.reference(label)
        cols = query(self.o,'SELECT column_id,column_name FROM created_metadata.ducklake_column '
                     'WHERE table_id=%s AND end_snapshot IS NULL AND parent_column IS NULL ORDER BY column_order',(self.tid(name),))
        observations, logical = [], []
        for fid, birth, rel, n, size, start in self.file_rows(name):
            path = self.path(rel)
            schema = ref.run('SELECT name,type,field_id,num_children FROM parquet_schema('+literal(str(path))+')',
                             'schema-'+str(fid), result=True)
            leaves = [x for x in schema if int(x.get('num_children') or 0)==0]
            self.check('business field-ID set file'+str(fid), sorted(int(x['field_id']) for x in leaves), sorted(c[0] for c in cols))
            self.check('no physical virtual identity columns file'+str(fid),
                       any(x['name'] in ('_ducklake_internal_row_id','_ducklake_internal_snapshot_id') or
                           x['field_id'] in (2147483540,2147483539) for x in leaves), False)
            by_id={int(x['field_id']): x['name'] for x in leaves}
            projection=','.join(qi(by_id[cid])+' AS '+qi(col) for cid,col in cols)
            raw = ref.run('SELECT file_row_number,'+projection+' FROM read_parquet('+literal(str(path))+',file_row_number=true) ORDER BY file_row_number',
                          'physical-'+str(fid), result=True)
            file_stats=query(self.o,'SELECT column_id,column_size_bytes,value_count,null_count,min_value,max_value,min_is_exact,max_is_exact '
                         'FROM created_metadata.ducklake_file_column_stats WHERE table_id=%s AND data_file_id=%s ORDER BY column_id',(self.tid(name),fid))
            self.check('exactly one file-statistics row per business column file'+str(fid),[x[0] for x in file_stats],sorted(c[0] for c in cols))
            names_by_id=dict(cols)
            for cid,byte_count,non_null,null_count,minimum,maximum,min_exact,max_exact in file_stats:
                actual_nulls=sum(x[names_by_id[cid]] is None for x in raw)
                self.check('file exact null/nonNULL counts '+str(fid)+'/'+str(cid),(non_null,null_count),(n-actual_nulls,actual_nulls))
                self.check('column compressed bytes are nonnegative',isinstance(byte_count,int) and byte_count>=0)
                self.check('uncollected file bounds stay unknown',(minimum,maximum,min_exact,max_exact),(None,None,None,None))
            self.check('physical count matches exact metadata file'+str(fid), len(raw), n)
            self.check('file physical positions are dense', [x['file_row_number'] for x in raw], list(range(n)))
            self.check('file size is actual bytes', path.stat().st_size, size)
            self.check('implicit row ID start is present', isinstance(start,int))
            for row in raw:
                logical.append({'logical_rowid': start+row['file_row_number'], 'begin_snapshot': birth,
                                **{c: row[c] for _,c in cols}})
            observations.append({'file_id':fid, 'path':str(path), 'sha256':digest(path),'rows':n,
                                 'bytes':size,'row_id_start':start,'birth':birth,'schema':schema,'physical':raw})
        self.check('all direct physical values match final survivors', sorted([[x['id'], x[cols[1][1]]] for x in logical]), expected)
        self.r.setdefault('physical', {})[label]=observations
        return logical

    def verify(self, name, expected, files, allocated=None, changes=None, val='val', stored_records=None):
        self.check('business rows '+name, self.rows(name,val=val), expected)
        self.check('registered data file count '+name, self.count(name), files)
        stat = self.stats(name)
        self.check('stored record_count '+name, stat[0], len(expected) if stored_records is None else stored_records)
        if allocated is not None: self.check('single logical row allocation '+name, stat[1], allocated)
        if changes is not None: self.check('single committed changed-R count '+name,self.changes(name),changes)
        self.check('registered byte total equals actual immutable files '+name,stat[2],
                   sum(self.path(x[2]).stat().st_size for x in self.file_rows(name)))

    def start_commit(self, conn):
        pid = scalar(conn,'SELECT pg_backend_pid()')
        outcome = {'pid':pid, 'done':False}
        def run():
            try: query(conn,'COMMIT'); outcome['sqlstate']='00000'
            except BaseException as exc: outcome.update(sqlstate=getattr(exc,'pgcode',None),message=str(exc))
            finally: outcome['done']=True
        thread = threading.Thread(target=run,daemon=True,name='owned-direct-publisher-'+str(pid))
        self.threads.append((thread,conn,outcome));thread.start()
        return outcome

    def wait_lock(self, outcomes, relation, seconds=12, mode='RowExclusiveLock'):
        pids = [x['pid'] for x in outcomes]
        deadline = time.monotonic()+seconds; found=[]
        while time.monotonic()<deadline:
            found=query(self.o,'SELECT pid,relation,mode FROM pg_locks WHERE database=(SELECT oid FROM pg_database '
                'WHERE datname=current_database()) AND relation=%s::regclass AND pid=ANY(%s::bigint[]) '
                "AND mode=%s AND NOT granted ORDER BY pid",(relation,pids,mode))
            if len(found)==len(pids) or any(x['done'] for x in outcomes): break
            time.sleep(.02)
        self.check('actual owned publishers wait at '+relation, [x[0] for x in found], sorted(pids))
        self.r.setdefault('barriers',[]).append({'relation':relation,'pids':pids,'waits':found,
                                                'postmaster_start':str(scalar(self.o,'SELECT pg_postmaster_start_time()'))})

    def wait_done(self, outcomes, seconds=20):
        deadline=time.monotonic()+seconds
        for thread,_,outcome in self.threads:
            if any(outcome is x for x in outcomes): thread.join(max(0,deadline-time.monotonic()))
        self.check('all publisher clients drained before cleanup',all(x['done'] for x in outcomes))
        return [x.get('sqlstate') for x in outcomes]

    def holder(self, relation):
        c=self.db.connect();self.session(c);query(c,'BEGIN');query(c,'LOCK TABLE '+relation+' IN SHARE MODE')
        self.holders.append(c);return c

    def finish_threads(self):
        # Never issue a second operation on a connection whose commit is active.
        for _,_,o in self.threads:
            if not o['done']:
                try: scalar(self.o,'SELECT pg_cancel_backend(%s)',(o['pid'],))
                except BaseException: pass
        for c in self.holders:
            try: query(c,'ROLLBACK')
            except BaseException: pass
        deadline=time.monotonic()+12
        for thread,_,_ in self.threads: thread.join(max(0,deadline-time.monotonic()))
        pending=[o['pid'] for _,_,o in self.threads if not o['done']]
        if pending: raise RuntimeError('Commit drain exceeded bound; preserve exact owned DB and PIDs '+repr(pending))


def boundary(t):
    t.create();t.limit(2)
    t.insert(1,2);t.verify('orders',[(1,10),(2,20)],0,2,2)
    t.insert(3,3);expected=[(i,i*10) for i in range(1,6)]
    t.verify('orders',expected,1,5,5)
    logical=t.physical('orders',[(i,i*10) for i in range(3,6)],'above-threshold')
    t.check('R is not mistaken for local physicalP', sorted(x['logical_rowid'] for x in logical), [2,3,4])
    t.create('zero');t.limit(0,'zero');t.insert(10,1,'zero')
    t.verify('zero',[(10,100)],1,1,1);t.physical('zero',[(10,100)],'zero-limit')
    # Catalog0 permits CREATE alone but does not permit unpublished-table DML.
    query(t.a,"SELECT og_lake.set_option('created_lake'::name,'data_inlining_row_limit','0')")
    before=t.state();query(t.a,'BEGIN')
    query(t.a,"CREATE FOREIGN TABLE biz.local_rejected(id integer,val integer) SERVER created_lake OPTIONS(binding_mode 'create',lake_schema 'main')")
    t.reject(t.a,'INSERT INTO biz.local_rejected VALUES(1,10)','0A000');query(t.a,'ROLLBACK')
    t.check('CREATE-local zero-limit failure leaves no provisional publication',t.state(),before)
    t.create('committed_zero');t.insert(70,1,'committed_zero')
    t.verify('committed_zero',[(70,700)],1,1,1)
    t.create('fresh');t.limit(100,'fresh')
    before=t.state();query(t.a,'BEGIN');t.insert(1,3,'fresh');query(t.a,'ROLLBACK')
    t.check('outer rollback before PRE_COMMIT publishes no candidate or metadata',t.state(),before)
    # A committed source identity in the same table deliberately disqualifies pure-new path.
    t.limit(100);query(t.a,'BEGIN');query(t.a,'UPDATE biz.orders SET val=11 WHERE id=1');t.insert(6,1);query(t.a,'COMMIT')
    t.verify('orders',[(1,11)]+[(i,i*10) for i in range(2,7)],1,6,7,stored_records=7)


def journal(t):
    t.create();t.limit(0)
    before=t.state();query(t.a,'BEGIN');t.insert(1,12)
    t.check('own first next-statement journal read',t.rows(conn=t.a),[(i,i*10) for i in range(1,13)])
    t.check('no candidate is written before PRE_COMMIT',t.disk(),before['files'])
    query(t.a,'UPDATE biz.orders SET val=val+1 WHERE id<=4')
    query(t.a,'UPDATE biz.orders SET val=val+100 WHERE id<=4')
    query(t.a,'DELETE FROM biz.orders WHERE id IN (5,6)')
    query(t.a,'SAVEPOINT child');t.insert(100,2)
    query(t.a,'UPDATE biz.orders SET val=-1 WHERE id=1');query(t.a,'DELETE FROM biz.orders WHERE id=2')
    query(t.a,'INSERT INTO biz.heap_marker VALUES(99)');query(t.a,'ROLLBACK TO child');query(t.a,'RELEASE child')
    expected=[(i,i*10+101 if i<=4 else i*10) for i in range(1,13) if i not in (5,6)]
    t.check('savepoint restores final private versions',t.rows(conn=t.a),expected)
    t.check('observer sees no private journal or heap marker',t.rows(),[])
    t.check('child heap write is gone',query(t.a,'SELECT * FROM biz.heap_marker'),[])
    query(t.a,'INSERT INTO biz.heap_marker VALUES(1)');query(t.a,'COMMIT')
    t.verify('orders',expected,1,len(expected),len(expected));t.physical('orders',expected,'journal-final')
    t.create('empty');t.limit(0,'empty');empty=t.state()
    query(t.a,'BEGIN');t.insert(1,3,'empty');query(t.a,'DELETE FROM biz.empty');query(t.a,'COMMIT')
    t.check('INSERT then DELETE all creates no snapshot/allocation/candidate',t.state(),empty)


def cancel(t):
    query(t.a,"SELECT og_lake.set_option('created_lake'::name,'data_inlining_row_limit','0')")
    t.create();t.create('inline');t.create('third');t.limit(0);t.limit(100,'inline');t.limit(0,'third')
    t.check('catalog0 empty CREATE leaves all three carriers absent',
            scalar(t.o,'SELECT count(*) FROM created_metadata.ducklake_inlined_data_tables'),0)
    before=t.state();holder=t.holder('created_metadata.ducklake_file_column_stats')
    query(t.a,'BEGIN');t.insert(1,32);t.insert(80,2,'inline');t.insert(200,3,'third');query(t.a,'INSERT INTO biz.heap_marker VALUES(1)')
    outcome=t.start_commit(t.a);t.wait_lock([outcome],'created_metadata.ducklake_file_column_stats')
    staged=t.disk();added=set(staged)-set(before['files'])
    t.check('both finished fileA/fileC candidates exist at registration barrier',len(added),2)
    ref=t.reference('cancel-prepared')
    from reverse_reference import literal
    prepared_counts=[]
    for p in sorted(added):
        physical=ref.run('SELECT count(*) AS n FROM read_parquet('+literal(p)+')','independent-finished-file',result=True)
        t.check('independent DuckDB reads exactly one finished-file count',len(physical),1)
        prepared_counts.append(physical[0]['n'])
    t.check('prepared candidates cover fileA and fileC exact row counts',sorted(prepared_counts),[3,32])
    t.r['cancel_candidate_files']=staged
    t.check('partial lake/heap/metadata/AA publication is still invisible',t.state()['standard'],before['standard'])
    t.check('cancel only the observed owned publisher',scalar(t.o,'SELECT pg_cancel_backend(%s)',(outcome['pid'],)),True)
    t.check('actual COMMIT interruption keeps57014',t.wait_done([outcome]),['57014'])
    query(t.a,'ROLLBACK');query(holder,'ROLLBACK')
    t.check('candidate/heap/inline/file/stats/snapshot/AA roll back atomically',t.state(),before)
    t.check('cancelled connection remains usable',scalar(t.a,'SELECT 6*7'),42)
    query(t.a,'BEGIN');t.insert(1,32);t.insert(80,2,'inline');t.insert(200,3,'third');query(t.a,'INSERT INTO biz.heap_marker VALUES(1)');query(t.a,'COMMIT')
    t.verify('orders',[(i,i*10) for i in range(1,33)],1,32,32)
    t.verify('inline',[(80,800),(81,810)],0,2,2)
    t.verify('third',[(200,2000),(201,2010),(202,2020)],1,3,3)


def claim(t):
    t.create();t.limit(0);t.create('inline');t.limit(100,'inline')
    before=t.state();frontier=t.t.head();holder=t.holder('created_metadata.ducklake_snapshot')
    query(t.a,'BEGIN');query(t.b,'BEGIN');t.insert(1,16,conn=t.a);t.insert(101,16,conn=t.b)
    t.insert(50,1,'inline',t.a);query(t.a,'INSERT INTO biz.heap_marker VALUES(1)');query(t.b,'INSERT INTO biz.heap_marker VALUES(2)')
    one=t.start_commit(t.a);two=t.start_commit(t.b)
    t.wait_lock([one,two],'created_metadata.ducklake_snapshot')
    prepared=t.disk();added=set(prepared)-set(before['files'])
    t.check('both publishers finish their own immutable payload before PK claim',len(added),2)
    t.r['claim_prepared_candidates']={x:prepared[x] for x in sorted(added)}
    query(holder,'ROLLBACK')
    t.check('known concurrent direct appends both commit after real claim race',t.wait_done([one,two]),['00000','00000'])
    after=t.disk()
    t.check('retry reuses same candidate paths/bytes', {x:after.get(x) for x in sorted(added)}, {x:prepared[x] for x in sorted(added)})
    t.check('claim race creates exactly two data objects',set(after)-set(before['files']),added)
    expected=[(i,i*10) for i in list(range(1,17))+list(range(101,117))]
    t.verify('orders',expected,2,32,32);t.verify('inline',[(50,500)],0,1,1)
    t.check('snapshot claim consumes two committed snapshots',t.t.head()[0]-frontier[0],2)
    t.check('file frontier reserves one slot per changed table including inline placeholder',t.t.head()[3]-frontier[3],3)
    regs=t.file_rows('orders');ranges=sorted((r[5],r[5]+r[3]) for r in regs)
    t.check('disjoint ranges cover every finalR exactly once',ranges,[(0,16),(16,32)])
    t.physical('orders',expected,'claim-final')


def schema(t):
    from inline_fixture import replace_catalog_values
    from empty_catalog_oracle import transport_native
    from empty_reference_fixture import export_actual_tables
    t.create();t.limit(0)
    # A real options-only commit after first DML is caught before publication.
    query(t.a,'BEGIN');t.insert(1,4)
    query(t.b,"SELECT og_lake.set_option('biz.orders'::regclass,'data_inlining_row_limit','10')")
    # PRE_COMMIT chooses the current effective threshold; this is safe, not an error.
    query(t.a,'COMMIT');t.verify('orders',[(i,i*10) for i in range(1,5)],0,4,4)
    t.limit(0)
    # The option writer already owns SRE before publication. Old committed0
    # remains visible to preparation; ShareLock waits after candidate finish.
    before=t.state();query(t.b,'BEGIN');t.holders.append(t.b)
    query(t.b,"SELECT og_lake.set_option('biz.orders'::regclass,'data_inlining_row_limit','10')")
    query(t.a,'BEGIN');t.insert(20,4);query(t.a,'INSERT INTO biz.heap_marker VALUES(55)')
    outcome=t.start_commit(t.a)
    t.wait_lock([outcome],'created_metadata.ducklake_metadata',mode='ShareLock')
    t.check('policy race occurs after actual candidate finish',len(set(t.disk())-set(before['files'])),1)
    query(t.b,'COMMIT')
    t.check('fresh policy rejects stale prepared decision',t.wait_done([outcome]),['40001'])
    query(t.a,'ROLLBACK')
    t.check('policy refusal removes candidate/heap/AA changes',
            {'files':t.disk(),'heap':query(t.o,'SELECT * FROM biz.heap_marker'),'stats':t.stats('orders'),'aa':t.changes('orders')},
            {'files':before['files'],'heap':before['heap'],'stats':(4,4,0),'aa':4})
    t.limit(0)
    # Produce actual unsupported created_schema via the pinned implementation.
    # Replay whole real rows under host MVCC, with no computed snapshot/IDs.
    native=t.t.export('unknown-ddl-before.json');ref=t.reference('unknown-ddl-reference')
    transported=ref.output/'source.ducklake';transport_native(ref,transported,native)
    query(t.a,'BEGIN');t.insert(20,4)
    ref.run(ref.attach(transported)+'CREATE SCHEMA lake.external_schema;','real-external-created-schema')
    actual={'tables':export_actual_tables(ref,transported)}
    dump(t.out/'unknown-ddl-actual-reference.json',actual)
    changes=[x for table in actual['tables'] if table['table_name']=='ducklake_snapshot_changes' for x in table['rows']]
    baseline_ids={x['snapshot_id'] for table in native['tables'] if table['table_name']=='ducklake_snapshot_changes' for x in table['rows']}
    late=[x for x in changes if x['snapshot_id'] not in baseline_ids]
    t.check('real externalDDL emits exactly one newly committed changeset',len(late),1)
    t.check('unsupported change names the actual new schema',late[0]['changes_made'],'created_schema:"external_schema"')
    schema_rows=[x for table in actual['tables'] if table['table_name']=='ducklake_schema' for x in table['rows'] if x['schema_name']=='external_schema']
    t.check('real schema definition and changeset share the producer snapshot',[(x['begin_snapshot'],x['end_snapshot']) for x in schema_rows],[(late[0]['snapshot_id'],None)])
    query(t.b,'BEGIN');t.replay_actual(t.b,native,actual);query(t.b,'COMMIT')
    committed=t.state()
    t.reject(t.a,'COMMIT','40001');query(t.a,'ROLLBACK')
    t.check('unknown real DDL prevents all pending publication/candidate effects',t.state(),committed)
    t.check('prior committed business rows survive unknown DDL refusal',t.rows(),[(i,i*10) for i in range(1,5)])


def carrier(t):
    """file-only A, rename, unrelated B DDL, first carrier must use A schema."""
    from empty_catalog_oracle import transport_native
    from empty_reference_fixture import export_actual_tables
    from inline_fixture import replace_catalog_values, foreign_table_statement
    ref=t.reference('file-only-producer')
    empty=t.t.export('file-only-native-empty.json');catalog=ref.output/'source.ducklake'
    transport_native(ref,catalog,empty)
    # Different attach options deliberately force genuine identity-free files.
    from reverse_reference import literal
    attach='ATTACH '+literal('ducklake:'+str(catalog))+" AS lake (DUCKLAKE_VERSION '1.1',AUTOMATIC_MIGRATION FALSE,DATA_INLINING_ROW_LIMIT 0);\n"
    ref.run(attach+'CREATE TABLE lake.main.orders(id INTEGER,val INTEGER); INSERT INTO lake.main.orders VALUES(1,10),(2,20);','create-real-file-only-A')
    actual={'tables':export_actual_tables(ref,catalog)}
    table=next(x for x in actual['tables'] if x['table_name']=='ducklake_table')['rows']
    tid=next(x['table_id'] for x in table if x['table_name']=='orders' and x['end_snapshot'] is None)
    t.check('genuine file-only source has no inline carrier registration',
            next(x for x in actual['tables'] if x['table_name']=='ducklake_inlined_data_tables')['rows'],[])
    query(t.a,'BEGIN');t.replay_actual(t.a,empty,actual);query(t.a,'COMMIT')
    query(t.a,foreign_table_statement('biz','orders','created_lake',tid,[{'name':'id','type':'INTEGER'},{'name':'val','type':'INTEGER'}]))
    t.check('Native initially reads standard file-only source',t.rows(),[(1,10),(2,20)])
    field='val' if t.args.mode=='carrier-legacy' else 'amount'
    if field=='amount': query(t.a,'ALTER FOREIGN TABLE biz.orders RENAME COLUMN val TO amount')
    versions=query(t.o,'SELECT schema_version FROM created_metadata.ducklake_schema_versions WHERE table_id=%s ORDER BY begin_snapshot DESC',(tid,))
    schema_a=max((x[0] for x in versions),default=0)
    t.r['carrier_initial_table_versions']=versions
    t.create('unrelated')
    t.check('unrelated CREATE advances global schema frontier',t.t.head()[1]>schema_a)
    t.limit(100)
    before=t.state();query(t.a,'BEGIN');query(t.a,'SAVEPOINT first_carrier')
    query(t.a,'INSERT INTO biz.heap_marker VALUES(101)')
    query(t.a,'UPDATE biz.orders SET '+field+'='+field+'+1 WHERE id=1')
    t.check('private lazy-carrier update is visible to its own next statement',t.rows(conn=t.a,val=field),[(1,11),(2,20)])
    query(t.a,'ROLLBACK TO first_carrier');query(t.a,'RELEASE first_carrier');query(t.a,'COMMIT')
    t.check('savepoint rollback of first carrier leaves complete committed state',t.state(),before)
    query(t.a,'BEGIN');query(t.a,'INSERT INTO biz.heap_marker VALUES(102)')
    query(t.a,'UPDATE biz.orders SET '+field+'='+field+'+1 WHERE id=1');query(t.a,'COMMIT')
    t.check('first carrier resolves renamed file-only table after unrelatedDDL',t.rows(val=field),[(1,11),(2,20)])
    registered=query(t.o,'SELECT table_name,schema_version FROM created_metadata.ducklake_inlined_data_tables WHERE table_id=%s',(tid,))
    t.check('exactly one lazily allocated carrier',len(registered),1)
    t.check('carrier registration uses table-local schema version',registered[0][1],schema_a)
    columns=query(t.o,'SELECT a.attname FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid JOIN pg_namespace n ON n.oid=c.relnamespace '
                  "WHERE n.nspname='created_metadata' AND c.relname=%s AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attnum",(registered[0][0],))
    t.check('first carrier business field uses current correct logical name',columns[-2:],[('id',),(field,)])
    fresh=t.db.connect();t.session(fresh)
    t.check('reconnected reader resolves lazy-carrier mapping',t.rows(conn=fresh,val=field),[(1,11),(2,20)])
    query(fresh,'UPDATE biz.orders SET '+field+'='+field+'+1 WHERE id=1')
    t.check('second same-R update resolves lazy carrier safely',t.rows(val=field),[(1,12),(2,20)])
    t.check('committed-origin updates do not consume newR',t.stats('orders')[1],2)
    query(t.a,"SELECT og_lake.flush('biz.orders'::regclass)")
    t.check('flush after cross-table schema/lazy carrier preserves values',t.rows(val=field),[(1,12),(2,20)])


def limits(t):
    import io
    t.create('orders','id integer,val text');t.limit(0)
    before=t.state();query(t.a,'BEGIN');query(t.a,'INSERT INTO biz.heap_marker VALUES(1)');query(t.a,'SAVEPOINT large')
    # Each1MiB cell fits Arrow per-row cap; total68MiB must fail the journal.
    payload=''.join(str(i)+'\t'+'x'*(1024*1024)+'\n' for i in range(68))
    try:
        with t.a.cursor() as c: c.copy_expert('COPY biz.orders(id,val) FROM STDIN',io.StringIO(payload))
    except Exception as exc: t.check('real COPY retains64MiB journal54000',getattr(exc,'pgcode',None),'54000')
    else: raise AssertionError('Bounded journal unexpectedly admitted68MiB')
    del payload
    query(t.a,'ROLLBACK TO large');query(t.a,'RELEASE large')
    t.check('COPY limit failure removes partial private rows',t.rows(conn=t.a),[])
    query(t.a,'ROLLBACK');t.check('journal limit and caller rollback are atomic',t.state(),before)
    payload='1\tbounded\n2\t\\N\n3\tlast\n'
    with t.a.cursor() as c:
        c.copy_expert('COPY biz.orders(id,val) FROM STDIN',io.StringIO(payload))
        t.check('bounded real COPY command count',c.rowcount,3)
    t.verify('orders',[(1,'bounded'),(2,None),(3,'last')],1,3,3)
    t.physical('orders',[(1,'bounded'),(2,None),(3,'last')],'bounded-copy-direct')
    # A single17MiB text value fits64MiB journal but cannot fit16MiB Arrow batch.
    before=t.state();query(t.a,'BEGIN');query(t.a,'INSERT INTO biz.heap_marker VALUES(2)')
    query(t.a,'INSERT INTO biz.orders VALUES(4,%s)',('z'*(17*1024*1024),))
    t.reject(t.a,'COMMIT','54000');query(t.a,'ROLLBACK')
    t.check('single Arrow-row limit removes partial candidate and all metadata',t.state(),before)
    # All17 tables/identities fit the journal; the independent candidate cap
    # must reject the17th source and clean the previous16 outputs.
    for i in range(17):
        t.create('cap'+str(i));t.limit(0,'cap'+str(i))
    before=t.state();query(t.a,'BEGIN');query(t.a,'INSERT INTO biz.heap_marker VALUES(3)')
    for i in range(17):t.insert(1,1,'cap'+str(i))
    t.reject(t.a,'COMMIT','54000');query(t.a,'ROLLBACK')
    t.check('17-candidate refusal removes every owned partial output and table publication',t.state(),before)


def interop(t):
    from empty_catalog_oracle import transport_native
    from empty_reference_fixture import export_actual_tables
    from inline_fixture import import_catalog, foreign_table_statement
    from permission_api import adopt_private_owner
    from native_test_context import OwnedDatabase
    t.create();t.limit(0);t.insert(1,32)
    expected=[(i,i*10) for i in range(1,33)]
    logical=t.physical('orders',expected,'interop-physical')
    native=t.t.export('interop-native.json');ref=t.reference('interop-reference')
    transported=ref.output/'source.ducklake';transport_native(ref,transported,native)
    old_hash=digest(transported)
    observed=ref.logical(transported,'lake.main.orders','read-native-implicit-identities')
    t.check('standard DuckLake reconstructs R=start+P and birth=commit',observed,sorted(logical,key=lambda x:x['logical_rowid']))
    t.check('readonly reference does not mutate transported standard catalog',digest(transported),old_hash)
    # Continue only the test-owned offline transport, never the shared realEMPTY.
    ref.run(ref.attach(transported)+'INSERT INTO lake.main.orders VALUES(1001,10010); '
            'UPDATE lake.main.orders SET val=11 WHERE id=1;','standard-continuation')
    actual={'tables':export_actual_tables(ref,transported)}
    dump(t.out/'interop-actual-reference-continuation.json',actual)
    t.check('independent continuation maintains unique stableR',
            len({x['logical_rowid'] for x in ref.logical(transported,'lake.main.orders','continued-identities')}),33)
    reverse=OwnedDatabase(t.args.run_id+'-reverse',t.out/'reverse-db')
    t.r.setdefault('_extra_databases',[]).append(reverse) # removed before serializing
    reverse.create();c=reverse.connect();t.session(c)
    query(c,"CREATE EXTENSION og_lake VERSION '0.19'")
    query(c,'BEGIN');import_catalog(c,'lake_metadata',actual);adopt_private_owner(c,actual,t.t.owner);query(c,'COMMIT')
    query(c,"SELECT og_lake.attach_catalog('continued','lake_metadata',NULL::text,true)")
    query(c,'CREATE SCHEMA reverse_business')
    query(c,foreign_table_statement('reverse_business','orders','continued',t.tid('orders'),
                                   [{'name':'id','type':'INTEGER'},{'name':'val','type':'INTEGER'}]))
    want=[(1,11)]+expected[1:]+[(1001,10010)]
    t.check('Native reads genuine standard DuckLake continuation unchanged',
            query(c,'SELECT id,val FROM reverse_business.orders ORDER BY id'),want)
    t.check('offline continuation never changes originalNative rows',t.rows(),expected)


def types(t):
    import struct
    before=t.state()
    t.reject(t.a,"CREATE FOREIGN TABLE biz.unsupported_smallint(s smallint) SERVER created_lake OPTIONS(binding_mode 'create',lake_schema 'main')",'0A000')
    t.check('unregistered smallint remains rejected without metadata or file changes',t.state(),before)
    definition='id integer,val text,b boolean,s integer,l bigint,f real,d double precision'
    t.create('orders',definition);t.limit(0)
    query(t.a,'CREATE TABLE biz.exact('+definition+')')
    values=[(1,'中文𐐀',True,-2147483648,-9223372036854775808,1.5,2.25),
            (2,None,False,2147483647,9223372036854775807,-0.0,-0.0),
            (3,'',None,None,None,None,None),(4,'long'*32768,True,0,0,float('nan'),float('inf'))]
    for name in ['orders','exact']:
        for row in values: query(t.a,'INSERT INTO biz.'+name+' VALUES(%s,%s,%s,%s,%s,%s,%s)',row)
    t.check('all admitted scalar/null/UTF8/TOAST/nonfinite values agree with native heap',
            query(t.o,'SELECT * FROM biz.orders ORDER BY id'),query(t.o,'SELECT * FROM biz.exact ORDER BY id'))
    t.check('small separate transactions above zero each publishonefile',t.count('orders'),4)
    t.check('exact cumulative AA changed-R count across tiny direct commits',t.changes('orders'),4)
    # Physical checker handles id/val for equality and verifies every businessfieldID.
    t.physical('orders',[(x[0],x[1]) for x in values],'types-file-fields')
    from reverse_reference import literal
    ref=t.reference('types-full-payload')
    full=[]
    for file in t.file_rows('orders'):
        full+=ref.run('SELECT id,val,b,s,l,f,d FROM read_parquet('+literal(str(t.path(file[2])))+')','file-'+str(file[0]),result=True)
    got=[[r[k] for k in ['id','val','b','s','l','f','d']] for r in sorted(full,key=lambda x:x['id'])]
    t.check('independent DuckDB reads every scalar payload',got,values)
    # openGauss float4in/float8in deliberately normalize textual -0 to +0
    # before either the ordinary heap or the lake writer receives the Datum.
    # Compare physical bytes against the actual native heap input contract.
    heap_zero=query(t.o,"SELECT encode(float4send(f),'hex'),encode(float8send(d),'hex') FROM biz.exact WHERE id=2")[0]
    t.check('host SQL input normalizes textual signedzero',heap_zero,('00000000','0000000000000000'))
    for row in full:
        if row['id']==2:
            t.check('Parquet double bytes equal actual native heap value',struct.pack('!d',row['d']).hex(),heap_zero[1])
            t.check('Parquet float4 bytes equal actual native heap value',struct.pack('!f',row['f']).hex(),heap_zero[0])


def legacy(t):
    t.create();before=t.state()
    t.reject(t.a,"SELECT og_lake.set_option('biz.orders'::regclass,'data_inlining_row_limit','0')",'0A000')
    t.check('retained SQL18 staysclosed to disabling inline',t.state(),before)
    t.limit(10);t.insert(1,1)
    t.verify('orders',[(1,10)],0,1,None)


def callbacks(t):
    """Real SQL19 callback guards, including a prepared/privately-published file.

    Reuse the product's existing three-mode probe and Direct barriers/cleanup.
    No new callback hook, fake error, owner or metadata record is introduced.
    """
    t.create();t.limit(0)
    t.r['callback_probe_source']=dict(path=str(D/'tests/precommit_probe.py'),
        sha256=digest(D/'tests/precommit_probe.py'), purpose='existing callback-order logic reference; not imported/executed')
    for mode in (0,1,2):
        # Modes0/2 must have no prior lake operation on this connection; SETs
        # and ordinary heap DML do not allocate a lake host transaction state.
        session=t.db.connect();t.session(session)
        before=t.state();marker=900000+mode
        query(session,'BEGIN');query(session,'INSERT INTO biz.heap_marker VALUES(%s)',(marker,))
        if mode==1:query(session,'INSERT INTO biz.orders VALUES(%s,10)',(marker,))
        query(session,'SELECT og_lake_internal.test_precommit_insert(%s::regclass,%s)',('biz.orders',mode))
        if mode==2:
            query(session,'INSERT INTO biz.orders VALUES(%s,20)',(marker,))
            holder=t.holder('created_metadata.ducklake_snapshot_changes')
            outcome=t.start_commit(session)
            t.wait_lock([outcome],'created_metadata.ducklake_snapshot_changes')
            # This last SPI write follows the snapshot claim, actual file and
            # column statistics, range allocation and the common AA UPSERT.
            pending=t.disk();added=set(pending)-set(before['files'])
            t.check('mode2 owns one finished candidate before late callback',len(added),1)
            from reverse_reference import literal
            ref=t.reference('callback2-private-payload')
            for path in sorted(added):
                actual=ref.run('SELECT id,val FROM read_parquet('+literal(path)+')','finished-candidate',result=True)
                t.check('mode2 independent reader sees actual direct payload',actual,[{'id':marker,'val':20}])
            relations=['created_metadata.ducklake_snapshot','created_metadata.ducklake_data_file',
                       'created_metadata.ducklake_file_column_stats','created_metadata.ducklake_table_stats',
                       'og_lake_internal.analyze_state','biz.heap_marker']
            oids=[scalar(t.o,'SELECT %s::regclass::oid',(name,)) for name in relations]
            locks=query(t.o,"SELECT DISTINCT relation FROM pg_locks WHERE database=(SELECT oid FROM pg_database "
                        "WHERE datname=current_database()) AND pid=%s AND granted AND mode='RowExclusiveLock' "
                        "AND relation=ANY(%s::oid[]) ORDER BY relation",(outcome['pid'],oids))
            t.check('mode2 has actual private publication writes before probe', [x[0] for x in locks],sorted(oids))
            t.check('mode2 partial publication remains invisible',t.t.protocol(),before['standard'])
            t.r['callback2_private_publication']=dict(pid=outcome['pid'],finished_candidates={x:pending[x] for x in added},
                                                      granted_relations=relations,granted_oids=locks)
            query(holder,'ROLLBACK')
            t.check('mode2 late callback still rejects public INSERT',t.wait_done([outcome]),['0A000'])
            t.check('mode2 failure comes from production callback guard',
                    'inside pre-commit or pre-prepare callbacks' in outcome.get('message',''))
            t.r['callback2_error']=deepcopy(outcome)
        else:
            outcome=t.reject(session,'COMMIT','0A000')
            t.check('mode'+str(mode)+' failure comes from production callback guard',
                    'inside pre-commit or pre-prepare callbacks' in outcome['message'])
        query(session,'ROLLBACK')
        t.check('mode'+str(mode)+' rolls back complete metadata/heap/AA/disk',t.state(),before)
        t.check('mode'+str(mode)+' same session remains usable',scalar(session,'SELECT 6*7'),42)
        query(session,'INSERT INTO biz.orders VALUES(%s,42)',(marker+100,))
        expected=[(900100+i,42) for i in range(mode+1)]
        t.verify('orders',expected,mode+1,mode+1,mode+1)
        session.close()
    # Preserve the old probe's savepoint/prepare closure checks without its
    # unrelated fixture framework. Registration rollback must disarm it.
    session=t.db.connect();t.session(session);before=t.state()
    query(session,'BEGIN');query(session,'SAVEPOINT probe_registration')
    query(session,"SELECT og_lake_internal.test_precommit_insert('biz.orders'::regclass,0)")
    query(session,'ROLLBACK TO probe_registration');query(session,'RELEASE probe_registration')
    query(session,'COMMIT')
    t.check('rolled-back callback registration permits empty commit unchanged',t.state(),before)
    session.close()
    session=t.db.connect();t.session(session);before=t.state()
    query(session,'BEGIN');query(session,'SAVEPOINT adopted_probe')
    query(session,"SELECT og_lake_internal.test_precommit_insert('biz.orders'::regclass,0)")
    query(session,'RELEASE adopted_probe');t.reject(session,'COMMIT','0A000');query(session,'ROLLBACK')
    t.check('released callback registration is adopted and cannot publish',t.state(),before)
    session.close()
    session=t.db.connect();t.session(session);before=t.state()
    gid='lake_callback_'+t.args.run_id
    from psycopg2 import sql
    query(session,'BEGIN');query(session,'INSERT INTO biz.heap_marker VALUES(900020)')
    query(session,"SELECT og_lake_internal.test_precommit_insert('biz.orders'::regclass,0)")
    try:query(session,sql.SQL('PREPARE TRANSACTION {}').format(sql.Literal(gid)))
    except Exception as exc:
        t.check('PRE_PREPARE first-access guard remains0A000',getattr(exc,'pgcode',None),'0A000')
        t.check('PRE_PREPARE uses production phase guard','inside pre-commit or pre-prepare callbacks' in str(exc))
    else:
        # Unexpected success must not leave this test's exact GID blocking DB
        # cleanup. This is the only prepared transaction this runner may touch.
        query(t.o,sql.SQL('ROLLBACK PREPARED {}').format(sql.Literal(gid)))
        raise AssertionError('Callback INSERT unexpectedly prepared')
    query(session,'ROLLBACK')
    t.check('PRE_PREPARE leaves no exact test GID',scalar(t.o,'SELECT count(*) FROM pg_prepared_xacts WHERE gid=%s',(gid,)),0)
    t.check('PRE_PREPARE rolls back all ordinary and lake work',t.state(),before)
    session.close()


CASES={'boundary':boundary,'journal':journal,'cancel':cancel,'claim':claim,'schema':schema,
       'carrier':carrier,'carrier-legacy':carrier,'callbacks':callbacks,'limits':limits,'interop':interop,'types':types,'legacy':legacy}


def execute(args):
    # Lazy imports keep prepare entirely stdlib/noNative. Explicit private variants
    # leave the frozen legacy helpers untouched.
    sys.path[:0]=[str(D),str(D/'tests'),str(HERE)]
    global query,scalar,reviewed_runtime_version
    from inline_fixture import query,scalar
    from native_test_context import OwnedDatabase, reviewed_runtime_version
    from native_auto_flush import connect_control
    from file_dml_acceptance import check_build
    from native_runtime import PROFILE,INSTALL
    from permission_api import OwnedRoles,cleanup_roles
    from lake_harness import validate_run_id
    validate_run_id(args.run_id)
    if PROFILE!='debug': raise RuntimeError('This first qualification explicitly requires mainDebug')
    allowed_sql=['0.18']if args.mode in ('legacy','carrier-legacy')else(['0.19']if args.mode=='types'else['0.19','0.20','0.21'])
    if args.sql_version not in allowed_sql:
        raise RuntimeError('Explicit mode/installed SQL mismatch; types retains SQL19 SMALLINT refusal')
    out=ROOT/'公共文档/openGauss原生湖存储/验证'/args.run_id;out.mkdir(exist_ok=False)
    r={'status':'fail','executed':False,'scope':SCOPE,'mode':args.mode,'sql_version':args.sql_version,
       'run_id':args.run_id,'assertions':[],'limitations':LIMITATIONS}
    db=roles=control=t=None;frozen={};drained=True;extra=[];reference_mutex=None
    try:
        build=check_build(args.build_receipt)
        contract=source_contract(args.source_contract,build)
        if reviewed_runtime_version()not in ['0.19','0.20','0.21']:
            raise RuntimeError('Source.control must be exact reviewed19/20/21; no fixture/database allocation attempted')
        sql19=Path(build['install'])/('share/postgresql/extension/og_lake--'+args.sql_version+'.sql')
        if not sql19.is_file() or digest(sql19)!=build['native_source_inputs_after'].get('contrib/og_lake/og_lake--'+args.sql_version+'.sql'):
            raise RuntimeError('Exact selected SQL installation is missing/stale before fixture allocation')
        frozen=archive_inputs(out,build,args.source_contract)
        shutil.copy2(args.build_receipt,out/'official-build.json')
        r.update(build_receipt=str(args.build_receipt.resolve()),build_sha256=digest(args.build_receipt),
                 binary_sha256=build['binary_sha256'],source_head=build['source_head'],
                 source_diff_sha256=build['source_diff_sha256'],source_contract=contract,inputs=frozen)
        control=connect_control();query(control,'SET session_timeout=0');query(control,'SET statement_timeout=30000')
        if scalar(control,'SHOW enable_lake_maintenance')!='off':
            raise RuntimeError('Automatic maintenance must already beoff; runner never reconfigures/restarts')
        r['postmaster_start']=str(scalar(control,'SELECT pg_postmaster_start_time()'))
        with (out/'fixture.log').open('w') as log:
            subprocess.run([sys.executable,str(D/'tests/catalog_fixture_bundle.py'),'--run-id',args.run_id,
                            '--output',str(out/'fixture/manifest.json')],stdout=log,stderr=subprocess.STDOUT,
                           check=True,timeout=300)
        db=OwnedDatabase(args.run_id,out);db.create()
        # db.admin can live throughout independent reference calls, so disable idle timeout too.
        query(db.admin,'SET session_timeout=0');query(db.admin,'SET statement_timeout=30000')
        roles=OwnedRoles(control,args.run_id,out);t=Direct(args,out,r,db,roles)
        # Acquire only AFTER the child fixture generator released this lock.
        # Never hold an outer reference mutex while launching that generator.
        import fcntl
        reference_mutex=(ROOT/'.reference/opengauss-native-lake/.driver.lock').open('a+')
        fcntl.flock(reference_mutex,fcntl.LOCK_EX|fcntl.LOCK_NB)
        r['executed']=True
        CASES[args.mode](t)
        t.finish_threads()
        t.check('all frozen execution inputs unchanged',{x:digest(x) for x in frozen},
                {x:y['sha256'] for x,y in frozen.items()})
        t.check('same owned runtime postmaster atcaseend',str(scalar(control,'SELECT pg_postmaster_start_time()')),r['postmaster_start'])
        check_build(args.build_receipt)
        r['status']='pass'
    except BaseException as exc:
        r.update(reason=str(exc),traceback=traceback.format_exc())
    finally:
        extra=r.pop('_extra_databases',[])
        if reference_mutex is not None:reference_mutex.close()
        if t is not None:
            try:t.finish_threads()
            except BaseException as exc:
                drained=False;r.update(status='fail',drain_error=str(exc),retained_database=db.database)
        clean=[]
        if drained:
            for owned in list(reversed(extra))+([db] if db is not None else []):
                try:
                    owner=owned.attempt/'database-owner.json'
                    ownership={'path':str(owner),'sha256':digest(owner),'receipt':json.loads(owner.read_text())} if owner.is_file() else None
                    owned.cleanup();clean.append({'database':owned.database,'attempt':str(owned.attempt),
                                                  'owner':ownership,'cleanup':owned.cleanup_result})
                except subprocess.TimeoutExpired as exc:
                    owner=owned.attempt/'database-owner.json'
                    clean.append({'database':owned.database,'status':'timeout','argv':jsonsafe(exc.cmd),
                                  'timeout':exc.timeout,'owner_receipt':str(owner),
                                  'owner_receipt_sha256':digest(owner) if owner.is_file() else None,
                                  'stdout':jsonsafe(exc.stdout),'stderr':jsonsafe(exc.stderr)})
                    r.update(status='fail',cleanup_error='Exact owned cleanup timedout; retain scene, no automatic cross-restart retry')
                except BaseException as exc:
                    clean.append({'database':owned.database,'status':'fail','reason':str(exc)})
                    r.update(status='fail',cleanup_error=str(exc))
        r['database_cleanups']=clean
        if roles is not None:
            try:
                if not clean or any(x.get('cleanup',{}).get('returncode')!=0 for x in clean) or not drained:
                    raise RuntimeError('Database cleanup unproven; exact private roles retained')
                r['role_cleanup']=cleanup_roles(control,roles.path,args.run_id)
            except BaseException as exc:r.update(status='fail',role_cleanup_error=str(exc))
            r=roles.redact(r)
        if control is not None:control.close()
        dump(out/'result.json',jsonsafe(r))
        print(json.dumps({'status':r['status'],'executed':r['executed'],'output':str(out/'result.json'),'reason':r.get('reason')},ensure_ascii=False))
        if not drained:
            # A daemon client may still own a libpq operation. Preserve DB/PIDs;
            # no process-global threadpool atexit join and no SQL cleanup racing it.
            sys.stdout.flush();sys.stderr.flush();os._exit(1)
    return 0 if r['status']=='pass' else 1


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--mode',choices=MODES,default='boundary')
    p.add_argument('--execute',action='store_true')
    p.add_argument('--run-id')
    p.add_argument('--build-receipt',type=Path)
    p.add_argument('--source-contract',type=Path)
    p.add_argument('--sql-version',choices=['0.18','0.19','0.20','0.21'],default='0.19')
    p.add_argument('--prepare-output',type=Path)
    args=p.parse_args()
    if not args.execute:
        report={'status':'prepared_not_executed','mode':args.mode,'scope':SCOPE,'modes':list(MODES),
                'runner_sha256':digest(__file__),'matrix':str(HERE/'native_direct_matrix.json'),
                'historical_blocked_contract':str(HERE/'native_direct_source_contract.pending.json'),
                'reviewed_future_source_contract':str(HERE/'native_direct_source_contract.sql019-candidate.json'),
                'limitations':LIMITATIONS,'native_connections':0,'native_builds':0,'native_cases_executed':0}
        if args.prepare_output:
            if args.prepare_output.exists():p.error('Never overwrite prior preparation receipt')
            dump(args.prepare_output,report)
        print(json.dumps(report,ensure_ascii=False,indent=2));return 0
    if not all([args.run_id,args.build_receipt,args.source_contract]):
        p.error('--execute requires run-id/build-receipt/source-contract')
    return execute(args)

if __name__=='__main__':raise SystemExit(main())
