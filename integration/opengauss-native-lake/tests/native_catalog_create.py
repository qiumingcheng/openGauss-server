#!/usr/bin/env python3
"""Native 0.6 catalog initialization and registration acceptance tests.

Input is a genuine EMPTY_BUNDLE. The root runner must validate the native build
receipt/source fingerprint and serialize this case with all other test work.
No product function, hook, or protocol initialization is registered by this test.
"""
import argparse,fcntl,json,os,shutil,sys,time,traceback
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from empty_reference_fixture import ROOT,DRIVER,export_actual_tables
from empty_manifest import validate_empty_manifest,validate_empty_bundle
from empty_catalog_oracle import read_native_catalog,compare_initialized,transport_native
sys.path.insert(0,str(DRIVER));sys.path.insert(0,str(DRIVER/'tests'))
from inline_fixture import import_catalog,query,scalar,foreign_table_statement
from lake_harness import Blocked,sha256,validate_run_id,write_json
from native_test_context import OwnedDatabase,load_reference
from permission_api import OwnedRoles,cleanup_roles
from reverse_reference import Reference


class Case:
    def __init__(self,db,args,result,roles):
        from psycopg2 import sql
        self.db,self.args,self.result,self.roles,self.sql=db,args,result,roles,sql
        self.a,self.b,self.observer=[db.connect() for _ in range(3)]
        self.version=args.legacy_version if args.mode=='legacy' else ('0.5' if args.prepared_legacy or args.mode=='upgrade' else '0.6')
        query(self.a,sql.SQL('CREATE EXTENSION og_lake VERSION {}').format(sql.Literal(self.version)))
        from native_test_context import reviewed_runtime_version
        self.runtime=reviewed_runtime_version()
        self.check('runtime supports retained SQL0.6 API',self.runtime in ('0.6','0.7', '0.8', '0.9', '0.10', '0.11'))
        self.check('actual runtime matches reviewed source',json.loads(scalar(self.a,'SELECT og_lake.host_status()'))['module_version'],self.runtime)
        self.check('installed exact requested SQL version',scalar(self.a,"SELECT extversion FROM pg_extension WHERE extname='og_lake'"),self.version)
        self.owner=None
        if self.version in ('0.5','0.6'):
            self.owner=roles.create('private',False)
            if args.mode!='configure_lock':query(self.a,'SELECT og_lake.configure_internal_owner(%s::name)',(self.owner,))
        query(self.a,'CREATE TABLE public.catalog_heap_marker(id integer PRIMARY KEY)')
        bundle=validate_empty_bundle(args.manifest);self.bundle=bundle
        self.fixtures={}
        for alias,item in bundle['entries'].items():
            if sha256(item['manifest'])!=item['sha256']:raise ValueError('EMPTY bundle manifest changed')
            m,e=validate_empty_manifest(item['manifest']);self.fixtures[alias]=(m,e)
        self.check('all required real root inputs present',set(self.fixtures),{'same','ancestor','child','sibling'})

    def check(self,name,actual,expected=True):
        ok=actual==expected
        self.result['assertions'].append({'name':name,'status':'pass' if ok else 'fail',
            'details':{'actual':actual,'expected':expected}})
        if not ok:raise AssertionError(name)

    def root(self,alias):return self.fixtures[alias][0]['data_path']

    def create(self,conn,server,schema,alias,root=None,credential=None):
        query(conn,'SELECT og_lake.create_catalog(%s::name,%s::name,%s::text,%s::text)',
              (server,schema,self.root(alias) if root is None else root,credential))

    def attach(self,conn,server,schema,credential=None):
        query(conn,'SELECT og_lake.attach_catalog(%s::name,%s::name,%s::text,true)',(server,schema,credential))

    def import_empty(self,schema,alias):
        query(self.a,'BEGIN')
        try:
            counts=import_catalog(self.a,schema,self.fixtures[alias][1])
            self.check('actual EMPTY imported '+schema,(len(counts),sum(counts.values())),(29,7))
            if self.owner is not None:
                query(self.a,self.sql.SQL('REVOKE ALL ON SCHEMA {} FROM PUBLIC').format(self.sql.Identifier(schema)))
                query(self.a,self.sql.SQL('ALTER SCHEMA {} OWNER TO {}').format(self.sql.Identifier(schema),self.sql.Identifier(self.owner)))
                for table in self.fixtures[alias][1]['tables']:
                    relation=self.sql.SQL('{}.{}').format(self.sql.Identifier(schema),self.sql.Identifier(table['table_name']))
                    query(self.a,self.sql.SQL('REVOKE ALL ON {} FROM PUBLIC').format(relation))
                    query(self.a,self.sql.SQL('ALTER TABLE {} OWNER TO {}').format(relation,self.sql.Identifier(self.owner)))
            query(self.a,'COMMIT')
        except BaseException:query(self.a,'ROLLBACK');raise

    def image(self,conn=None):
        conn=conn or self.observer
        # All rows are inside this exact owned database; no prefix based cleanup.
        image={'namespaces':query(conn,"SELECT oid,nspname,nspowner,nspacl::text FROM pg_namespace "
                "WHERE nspname NOT LIKE 'pg_%' AND nspname NOT IN ('information_schema','cstore') ORDER BY oid"),
            'servers':query(conn,'SELECT oid,srvname,srvowner,srvfdw,srvoptions::text,srvacl::text FROM pg_foreign_server ORDER BY oid'),
            'relations':query(conn,"SELECT c.oid,n.nspname,c.relname,c.relkind,c.relowner,c.relacl::text FROM pg_class c "
                "JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname NOT LIKE 'pg_%' "
                "AND n.nspname NOT IN ('information_schema','cstore') ORDER BY c.oid"),
            'heap':query(conn,'SELECT * FROM public.catalog_heap_marker ORDER BY id')}
        for (name,) in query(conn,"SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
                                   "WHERE n.nspname='og_lake_internal' AND c.relkind='r' ORDER BY c.relname"):
            rows=query(conn,self.sql.SQL('SELECT * FROM og_lake_internal.{}').format(self.sql.Identifier(name)))
            image[name]=sorted(repr(tuple(row)) for row in rows)
        return image

    def absent(self,conn,server,schema):
        return query(conn,'SELECT (SELECT count(*) FROM pg_namespace WHERE nspname=%s),'
                          '(SELECT count(*) FROM pg_foreign_server WHERE srvname=%s)',(schema,server))==[(0,0)]

    def visible_registration(self,conn=None):
        """MVCC-only observer: do not open the OID registry while A holds its
        AccessExclusiveLock. Full image comparison is for after transaction end.
        """
        conn=conn or self.observer
        return {'namespaces':query(conn,"SELECT oid,nspname FROM pg_namespace WHERE nspname NOT LIKE 'pg_%' ORDER BY oid"),
                'servers':query(conn,'SELECT oid,srvname FROM pg_foreign_server ORDER BY oid'),
                'bindings':sorted(repr(tuple(row)) for row in query(conn,'SELECT * FROM og_lake_internal.catalog_binding')),
                'heap':query(conn,'SELECT * FROM public.catalog_heap_marker ORDER BY id')}

    def reject(self,label,conn,fn,state):
        import psycopg2
        try:fn()
        except psycopg2.Error as exc:self.check(label+' SQLSTATE',exc.pgcode,state)
        else:self.check(label+' rejects','accepted',state)

    def wait_registration(self,future,holder=None,waiter=None):
        if scalar(self.observer,'SHOW enable_thread_pool')=='on':raise Blocked('This deterministic relation-lock PID oracle currently requires dedicated mode')
        relation=scalar(self.observer,"SELECT 'og_lake_internal.catalog_binding'::regclass::oid")
        holder=self.a_pid if holder is None else holder;waiter=self.b_pid if waiter is None else waiter
        deadline=time.monotonic()+8
        while True:
            rows=query(self.observer,"SELECT pid,mode,granted FROM pg_locks WHERE relation=%s "
                       "AND pid IN (%s,%s) AND mode='ShareRowExclusiveLock' ORDER BY pid,granted",(relation,holder,waiter))
            expected={(holder,True),(waiter,False)}
            if {(pid,granted) for pid,mode,granted in rows}==expected:
                self.result.setdefault('registration_waits',[]).append([list(row) for row in rows]);return
            if future.done():raise AssertionError('Second registration finished before the exact catalog relation-lock barrier')
            if time.monotonic()>deadline:raise AssertionError('Did not observe the exact native registration lock conflict')
            time.sleep(.02)


def initial(t):
    root=Path(t.root('same'));t.check('native initial root absent',root.exists() or root.is_symlink(),False)
    query(t.a,'BEGIN');tx_time=scalar(t.a,'SELECT transaction_timestamp()')
    t.create(t.a,'created_lake','created_metadata','same')
    t.check('local initial snapshot uses calling transaction time',scalar(t.a,'SELECT snapshot_time FROM created_metadata.ducklake_snapshot'),tx_time)
    t.check('observer cannot see uncommitted native catalog',t.absent(t.observer,'created_lake','created_metadata'))
    query(t.a,'COMMIT')
    m,e=t.fixtures['same'];native=read_native_catalog(t.observer,'created_metadata',e,t.args.output.parent/'native-created-export.json')
    comparison=compare_initialized(e,native,t.root('same'),'openGauss og_lake '+t.runtime)
    t.result['independent_empty_comparison']=comparison
    t.check('native initialize does not create data root',root.exists() or root.is_symlink(),False)
    schema_oid=scalar(t.observer,"SELECT oid FROM pg_namespace WHERE nspname='created_metadata'")
    owner_oid=scalar(t.observer,'SELECT oid FROM pg_roles WHERE rolname=%s',(t.owner,))
    t.check('native private schema owner',scalar(t.observer,'SELECT nspowner FROM pg_namespace WHERE oid=%s',(schema_oid,)),owner_oid)
    t.check('all actual tables owned by configured private owner',query(t.observer,
        "SELECT DISTINCT relowner FROM pg_class WHERE relnamespace=%s AND relkind='r'",(schema_oid,)),[(owner_oid,)])
    binding=query(t.observer,'SELECT b.metadata_schema_oid,b.data_root,b.credential_ref,b.format_version,b.read_only '
        'FROM og_lake_internal.catalog_binding b JOIN pg_foreign_server s ON b.server_oid=s.oid WHERE s.srvname=%s',('created_lake',))
    t.check('native server/catalog binding',binding,[(schema_oid,t.root('same').rstrip('/')+'/',None,'1.1',False)])
    t.check('actual relation OIDs registered once',scalar(t.observer,
        'SELECT count(*) FROM og_lake_internal.metadata_relation m JOIN pg_class c ON m.relation_oid=c.oid '
        'WHERE m.metadata_schema_oid=%s AND c.relnamespace=%s AND m.relation_name=c.relname',(schema_oid,schema_oid)),29)
    t.check('no business table binding synthesized',scalar(t.observer,'SELECT count(*) FROM og_lake_internal.table_binding'),0)
    # Exact offline transport followed by real DuckLake attachment, never merely
    # a SELECT from copied SQL tables. No source protocol value is repaired.
    output=t.args.output.parent/'native-reference-roundtrip';output.mkdir()
    lock=json.loads((DRIVER/'source-lock.json').read_text());receipt=json.loads((ROOT/'.reference/opengauss-native-lake/build/receipt.json').read_text())
    if receipt['source_lock_hash']!=sha256(DRIVER/'source-lock.json'):raise Blocked('Reference receipt stale')
    ref=Reference(output,lock,receipt)
    if sha256(ref.binary)!=receipt['binary_sha256']:raise Blocked('Reference binary changed')
    with (ROOT/'.reference/opengauss-native-lake/.driver.lock').open('a+') as guard:
        fcntl.flock(guard,fcntl.LOCK_EX|fcntl.LOCK_NB)
        target=output/'native-transport.ducklake';transport_native(ref,target,native);before=sha256(target)
        schemas=ref.run(ref.attach(target,True)+"SELECT schema_name FROM duckdb_schemas() WHERE database_name='lake' ORDER BY schema_name",'native-schemas',result=True)
        t.check('real reference sees logical main',schemas,[{'schema_name':'main'}])
        tables=ref.run(ref.attach(target,True)+"SELECT table_name FROM duckdb_tables() WHERE database_name='lake'",'native-tables',result=True)
        t.check('real reference sees zero business tables',tables,[])
        snapshots=ref.run(ref.attach(target,True)+"SELECT snapshot_id,schema_version FROM ducklake_snapshots('lake') ORDER BY snapshot_id",'native-snapshots',result=True)
        t.check('real reference interprets native initial snapshot',snapshots,[{'snapshot_id':0,'schema_version':0}])
        t.check('readonly reference leaves actual transport bytes unchanged',sha256(target),before)
        # Normal continuation on two disposable copies. Original OG catalog and
        # original EMPTY files remain untouched. New independent table UUID/time
        # fields are recorded, not declared equal across separate executions.
        observed=[]
        for label,source in [('native',target),('reference',Path(m['catalog']))]:
            copy=output/(label+'-continued.ducklake');shutil.copy2(source,copy)
            ref.run(ref.attach(copy)+'CREATE TABLE lake.main.probe(id INTEGER,val INTEGER); INSERT INTO lake.main.probe VALUES(1,10),(2,20);',label+'-normal-continuation')
            logical=ref.logical(copy,'lake.main.probe',label+'-logical')
            stats=ref.run('SELECT table_id,record_count,next_row_id,file_size_bytes FROM ducklake_table_stats ORDER BY table_id',label+'-stats',copy,True,True)
            frontier=ref.run('SELECT snapshot_id,schema_version,next_catalog_id,next_file_id FROM ducklake_snapshot ORDER BY snapshot_id',label+'-frontier',copy,True,True)
            changes=ref.run('SELECT snapshot_id,changes_made FROM ducklake_snapshot_changes ORDER BY snapshot_id',label+'-changes',copy,True,True)
            observed.append({'logical':logical,'stats':stats,'frontiers':frontier,'changes':changes})
        t.check('normal reference continuation identities/counts/frontiers/changes agree',observed[0],observed[1])


def atomic(t):
    baseline=t.image();visible=t.visible_registration();query(t.a,'BEGIN');query(t.a,'INSERT INTO public.catalog_heap_marker VALUES(1)')
    t.create(t.a,'rollback_lake','rollback_metadata','same')
    t.check('other connection cannot observe uncommitted objects',t.visible_registration(),visible)
    query(t.a,'ROLLBACK');t.check('full rollback restores exact host/heap bindings',t.image(),baseline)
    query(t.a,'BEGIN');query(t.a,'SAVEPOINT before_create')
    t.create(t.a,'save_lake','save_metadata','same');query(t.a,'INSERT INTO public.catalog_heap_marker VALUES(2)')
    query(t.a,'ROLLBACK TO before_create');query(t.a,'RELEASE SAVEPOINT before_create')
    t.check('savepoint rollback removes all local created identities',t.image(t.a),baseline)
    # The same connection and names/root must be reusable after subabort.
    query(t.a,'SAVEPOINT retry_create');t.create(t.a,'save_lake','save_metadata','same');query(t.a,'RELEASE SAVEPOINT retry_create')
    query(t.a,'SAVEPOINT duplicate_root')
    t.reject('same transaction sees its prior registered root',t.a,
             lambda:t.create(t.a,'duplicate_lake','duplicate_metadata','same'),'55000')
    query(t.a,'ROLLBACK TO duplicate_root');query(t.a,'RELEASE SAVEPOINT duplicate_root')
    t.check('failed child leaves parent created catalog intact',t.absent(t.a,'save_lake','save_metadata'),False)
    query(t.a,'ROLLBACK');t.check('released child remains part of outer rollback',t.image(),baseline)
    t.check('rollback did not materialize any data leaf',Path(t.root('same')).exists(),False)


def collision(t):
    if scalar(t.a,'SHOW enable_thread_pool')=='on':raise Blocked('Dedicated backend PID lock oracle required')
    a_kind,b_kind=t.args.operations.split('-')
    aliases=('child','ancestor') if t.args.roots=='ancestor' else (('same','sibling') if t.args.roots=='sibling' else ('same','same'))
    if t.args.roots=='ancestor' and (a_kind,b_kind)==('attach','create'):aliases=('ancestor','child')
    if t.args.roots=='ancestor' and (a_kind,b_kind)==('create','create'):
        raise ValueError('CREATE ancestor leaf must not exist; use genuine ATTACH ancestor to test overlap independently of path admission')
    if a_kind=='attach':t.import_empty('metadata_a',aliases[0])
    if b_kind=='attach':t.import_empty('metadata_b',aliases[1])
    if t.args.prepared_legacy:
        if b_kind!='attach':raise ValueError('Prepared legacy path needs ATTACH as B')
        query(t.b,"PREPARE inherited_attach(name,name) AS SELECT og_lake.attach_catalog($1,$2,NULL::text,true)")
        t.result['legacy_attach_function_oid']=scalar(t.b,"SELECT oid FROM pg_proc WHERE pronamespace=(SELECT oid FROM pg_namespace WHERE nspname='og_lake') "
            "AND proname='attach_catalog' AND proargtypes::text='19 19 25 16'")
        query(t.a,"ALTER EXTENSION og_lake UPDATE TO '0.6'");t.version='0.6'
        t.check('attach SQL function OID survives upgrade',scalar(t.a,"SELECT oid FROM pg_proc WHERE pronamespace=(SELECT oid FROM pg_namespace WHERE nspname='og_lake') "
            "AND proname='attach_catalog' AND proargtypes::text='19 19 25 16'"),t.result['legacy_attach_function_oid'])
    before=t.visible_registration();t.a_pid=scalar(t.a,'SELECT pg_backend_pid()');t.b_pid=scalar(t.b,'SELECT pg_backend_pid()')
    query(t.b,'BEGIN ISOLATION LEVEL REPEATABLE READ');query(t.b,'SELECT count(*) FROM og_lake_internal.catalog_binding')
    query(t.a,'BEGIN');query(t.a,'INSERT INTO public.catalog_heap_marker VALUES(1)')
    (t.create(t.a,'server_a','metadata_a',aliases[0]) if a_kind=='create' else t.attach(t.a,'server_a','metadata_a'))
    def second():
        import psycopg2
        try:
            query(t.b,'INSERT INTO public.catalog_heap_marker VALUES(2)')
            if t.args.prepared_legacy:query(t.b,"EXECUTE inherited_attach('server_b','metadata_b')")
            elif b_kind=='create':t.create(t.b,'server_b','metadata_b',aliases[1],root=t.root(aliases[1])+'///')
            else:t.attach(t.b,'server_b','metadata_b')
            query(t.b,'COMMIT');return {'state':'committed'}
        except psycopg2.Error as exc:
            query(t.b,'ROLLBACK');return {'state':'rejected','sqlstate':exc.pgcode,'message':str(exc)}
    with ThreadPoolExecutor(max_workers=1) as pool:
        future=pool.submit(second)
        try:
            t.wait_registration(future);t.check('pending A remains invisible in another connection',t.visible_registration(),before)
            query(t.a,'COMMIT' if t.args.first_outcome=='commit' else 'ROLLBACK')
            reply=future.result(timeout=20);t.result['second_registration']=reply
        finally:
            if not future.done():
                query(t.a,'ROLLBACK');t.b.cancel()
    if t.args.first_outcome=='commit' and t.args.roots=='sibling':
        t.check('component boundary is not an overlapping prefix',reply,{'state':'committed'})
        t.check('both independent roots commit heap atomically',query(t.observer,'SELECT * FROM public.catalog_heap_marker ORDER BY id'),[(1,),(2,)])
        t.check('both nonoverlapping roots registered',scalar(t.observer,'SELECT count(*) FROM og_lake_internal.catalog_binding'),2)
    elif t.args.first_outcome=='commit':
        t.check('fresh lock-after-wait admission detects committed collision',reply.get('sqlstate'),'55000')
        t.check('failed B also rolls back its ordinary heap row',query(t.observer,'SELECT * FROM public.catalog_heap_marker ORDER BY id'),[(1,)])
        t.check('only first catalog is registered',query(t.observer,'SELECT s.srvname FROM pg_foreign_server s JOIN og_lake_internal.catalog_binding b ON s.oid=b.server_oid ORDER BY s.srvname'),[('server_a',)])
        if b_kind=='create':t.check('failed B creates no namespace',scalar(t.observer,"SELECT count(*) FROM pg_namespace WHERE nspname='metadata_b'"),0)
    else:
        t.check('A rollback releases exact root claim and B succeeds',reply,{'state':'committed'})
        t.check('only B ordinary heap row commits',query(t.observer,'SELECT * FROM public.catalog_heap_marker ORDER BY id'),[(2,)])
        t.check('only second catalog is registered',query(t.observer,'SELECT s.srvname FROM pg_foreign_server s JOIN og_lake_internal.catalog_binding b ON s.oid=b.server_oid ORDER BY s.srvname'),[('server_b',)])


def legacy(t):
    # Each old SQL version is installed in its own owned DB on the new binary.
    # No old C symbol wrapper is fabricated to fake a mixed-version database.
    t.import_empty('metadata_a','same');t.import_empty('metadata_b','same')
    before=t.image()
    t.reject('old installed SQL create remains unavailable',t.a,
             lambda:t.create(t.a,'new_server','new_metadata','sibling'),'0A000')
    t.check('old create refusal has no changes',t.image(),before)
    t.reject('new legacy ATTACH cannot smuggle unsupported credential',t.a,
             lambda:t.attach(t.a,'credential_server','metadata_a','credential:test'),'0A000')
    t.check('credential refusal leaves standard catalog and binding intact',t.image(),before)
    # Collision implementation uses already imported catalogs; its duplicate
    # preparation is intentionally skipped by this scoped adapter.
    original=t.import_empty;t.import_empty=lambda schema,alias:None
    try:collision(t)
    finally:t.import_empty=original


def admission(t):
    before=t.image();parent=Path(t.root('same')).parent
    cases=[('NULL root',None,'22004'),('empty root','','22023'),('relative root','relative/lake','0A000'),
           ('remote URI','s3://native-catalog-test/no-io','0A000'),('filesystem root','/','22023'),
           ('dot component',str(parent)+'/./base','22023'),('dotdot component',str(parent)+'/../base','22023'),
           ('existing directory',str(parent),'55000')]
    for label,root,state in cases:
        t.reject(label,t.a,lambda root=root:query(t.a,
            'SELECT og_lake.create_catalog(%s::name,%s::name,%s::text,NULL::text)',('rejected_server','rejected_metadata',root)),state)
        t.check(label+' preserves exact host state',t.image(),before)
    t.reject('unimplemented credential',t.a,lambda:t.create(t.a,'rejected_server','rejected_metadata','same',credential='owned:test'),'0A000')
    t.check('credential failure preserves all host state',t.image(),before)
    # Explicitly owned filesystem sentinels: never touch or remove user paths.
    owned=t.args.output.parent/'path-admission';owned.mkdir()
    file=owned/'sentinel';file.write_text('preserve-this-owned-sentinel\n');file_sha=sha256(file)
    link=owned/'alias';link.symlink_to(parent,target_is_directory=True)
    for label,root in [('existing file',file),('symlink root',link),('symlink parent',link/'new-leaf')]:
        t.reject(label,t.a,lambda root=root:t.create(t.a,'rejected_server','rejected_metadata','same',root=str(root)),'55000')
        t.check(label+' leaves all SQL state unchanged',t.image(),before)
    t.check('existing sentinel untouched',sha256(file),file_sha)
    t.check('symlink not replaced or removed',link.is_symlink())
    # Remove exactly the symlink made above; retain sentinel and evidence. No
    # recursive deletion may follow it into the real fixture parent.
    link.unlink()
    query(t.a,'CREATE SCHEMA occupied_metadata');occupied=t.image()
    t.reject('even an empty existing schema cannot be claimed',t.a,
             lambda:t.create(t.a,'rejected_server','occupied_metadata','same'),'42P06')
    t.check('schema collision preserves the original OID/owner',t.image(),occupied)
    role=t.roles.create('stranger');conn,identity=t.roles.connect(t.db,'stranger');t.result['real_login']=identity
    t.reject('real ordinary LOGIN cannot create a catalog',conn,
             lambda:t.create(conn,'ordinary_server','ordinary_metadata','same'),'42501')
    # A quoted identifier and quoted/Unicode root must be data, not executable
    # SQL. Success is rolled back and cannot consume the real same-root fixture.
    query(t.a,'BEGIN')
    t.create(t.a,'server "湖','schema "湖','same',root=str(parent/"root '湖"))
    t.check('quoted native identifiers are actual distinct objects',t.absent(t.a,'server "湖','schema "湖'),False)
    query(t.a,'ROLLBACK');t.check('quoted initialization rollback preserves all prior host state',t.image(),occupied)


def cancellation(t):
    if scalar(t.a,'SHOW enable_thread_pool')=='on':raise Blocked('Dedicated backend PID lock oracle required')
    baseline=t.image();t.a_pid=scalar(t.a,'SELECT pg_backend_pid()');t.b_pid=scalar(t.b,'SELECT pg_backend_pid()')
    query(t.a,'BEGIN');query(t.a,'LOCK TABLE og_lake_internal.catalog_binding IN SHARE ROW EXCLUSIVE MODE')
    query(t.b,'BEGIN');query(t.b,'INSERT INTO public.catalog_heap_marker VALUES(1)')
    def worker():
        import psycopg2
        try:t.create(t.b,'cancelled_server','cancelled_metadata','same');return {'state':'accepted'}
        except psycopg2.Error as exc:return {'state':'rejected','sqlstate':exc.pgcode}
    with ThreadPoolExecutor(max_workers=1) as pool:
        future=pool.submit(worker)
        try:
            t.wait_registration(future);t.b.cancel();reply=future.result(timeout=20)
            t.check('cancellation interrupts actual native lock wait',reply,{'state':'rejected','sqlstate':'57014'})
        finally:
            if not future.done():t.b.cancel()
            query(t.a,'ROLLBACK')
            # Never send ROLLBACK on B while its query is executing in the
            # worker thread. Finish/cancel that exact invocation first.
            future.result(timeout=20);query(t.b,'ROLLBACK')
    t.check('cancelled registration and heap restore atomically',t.image(),baseline)
    query(t.b,'BEGIN');t.create(t.b,'cancelled_server','cancelled_metadata','same');query(t.b,'ROLLBACK')
    t.check('same cancelled connection can initialize and roll back cleanly',t.image(),baseline)


def prepare_business(t):
    manifest=Path(t.bundle['business_fixture']['manifest'])
    m,e,counts,artifacts=load_reference(t.a,manifest,t.args.run_id,t.args.output.parent/'business-import',metadata_schema='business_metadata')
    t.result['artifacts'].extend(artifacts)
    query(t.a,'BEGIN')
    try:
        query(t.a,t.sql.SQL('REVOKE ALL ON SCHEMA business_metadata FROM PUBLIC'))
        query(t.a,t.sql.SQL('ALTER SCHEMA business_metadata OWNER TO {}').format(t.sql.Identifier(t.owner)))
        for table in e['tables']:
            relation=t.sql.SQL('business_metadata.{}').format(t.sql.Identifier(table['table_name']))
            query(t.a,t.sql.SQL('REVOKE ALL ON {} FROM PUBLIC').format(relation))
            query(t.a,t.sql.SQL('ALTER TABLE {} OWNER TO {}').format(relation,t.sql.Identifier(t.owner)))
        query(t.a,'COMMIT')
    except BaseException:query(t.a,'ROLLBACK');raise
    t.attach(t.a,'business_lake','business_metadata');query(t.a,'CREATE SCHEMA biz')
    query(t.a,foreign_table_statement('biz','orders','business_lake',m['table_id'],m['schema']))
    return [(row['id'],row['val']) for row in m['expected']]


def reader_then_registration(t):
    """A real lake reader must not invert registry-AS and registration locks."""
    if scalar(t.a,'SHOW enable_thread_pool')=='on':raise Blocked('Dedicated backend PID lock oracle required')
    expected=prepare_business(t)
    if t.args.reader_operation=='attach':t.import_empty('reader_b_metadata','same')
    t.a_pid=scalar(t.a,'SELECT pg_backend_pid()');t.b_pid=scalar(t.b,'SELECT pg_backend_pid()')
    query(t.a,'BEGIN');t.check('A reads genuine existing lake',query(t.a,'SELECT id,val FROM biz.orders ORDER BY id'),expected)
    registry=scalar(t.observer,"SELECT 'og_lake_internal.metadata_relation'::regclass::oid")
    t.check('actual reader holds registry AccessShare',scalar(t.observer,
        "SELECT count(*) FROM pg_locks WHERE pid=%s AND relation=%s AND mode='AccessShareLock' AND granted",(t.a_pid,registry))>0)
    query(t.b,'BEGIN');query(t.b,'INSERT INTO public.catalog_heap_marker VALUES(2)')
    # This is intentionally synchronous: old registry AccessExclusive behavior
    # would time out here while A is a valid idle reader. Such a failure must be
    # reported, not worked around by dropping A's lock or allowing a deadlock.
    if t.args.reader_operation=='attach':t.attach(t.b,'reader_b_server','reader_b_metadata')
    else:t.create(t.b,'reader_b_server','reader_b_metadata','same')
    t.check('B registry registration uses a reader-compatible lock',scalar(t.observer,
        "SELECT count(*) FROM pg_locks WHERE pid=%s AND relation=%s AND mode='RowExclusiveLock' AND granted",(t.b_pid,registry))>0)
    query(t.a,'INSERT INTO public.catalog_heap_marker VALUES(1)')
    def a_then_create():
        t.create(t.a,'reader_a_server','reader_a_metadata','sibling');query(t.a,'COMMIT')
        return 'committed'
    with ThreadPoolExecutor(max_workers=1) as pool:
        future=pool.submit(a_then_create)
        try:
            t.wait_registration(future,holder=t.b_pid,waiter=t.a_pid)
            query(t.b,'COMMIT');t.check('read then later registration completes after B',future.result(timeout=20),'committed')
        finally:
            if not future.done():t.a.cancel()
            query(t.b,'ROLLBACK')
    t.check('both user transactions retain their heap effects',query(t.observer,'SELECT * FROM public.catalog_heap_marker ORDER BY id'),[(1,),(2,)])
    t.check('old existing lake still reads after concurrent new registrations',query(t.observer,'SELECT id,val FROM biz.orders ORDER BY id'),expected)
    t.check('old and two new catalogs remain distinct',scalar(t.observer,'SELECT count(*) FROM og_lake_internal.catalog_binding'),3)


def drop_under_registry_reader(t):
    prepare_business(t)
    query(t.a,'BEGIN');query(t.a,'LOCK TABLE og_lake_internal.metadata_relation IN ACCESS SHARE MODE')
    # A holds the same registry mode as an admitted reader but deliberately no
    # business-table lock: normal DROP's AccessExclusive relation wait is not a
    # lake registry bug and must not be confused with the lock-order test.
    query(t.b,'DROP SERVER business_lake CASCADE')
    t.check('DROP removes binding despite compatible registry reader',scalar(t.observer,'SELECT count(*) FROM og_lake_internal.catalog_binding'),0)
    t.check('DROP leaves actual standard metadata intact',scalar(t.observer,'SELECT count(*) FROM business_metadata.ducklake_snapshot')>0)
    t.create(t.a,'drop_retry_server','drop_retry_metadata','same');query(t.a,'ROLLBACK')
    t.check('registry reader can register after DROP without a lock cycle',scalar(t.observer,'SELECT count(*) FROM og_lake_internal.catalog_binding'),0)


def configure_lock(t):
    baseline=t.image()
    for target in ['access_policy','catalog_binding','table_binding','relation_binding','column_binding','metadata_relation']:
        query(t.b,'BEGIN');query(t.b,t.sql.SQL('SELECT count(*) FROM og_lake_internal.{}').format(t.sql.Identifier(target)))
        query(t.a,'BEGIN');query(t.a,'INSERT INTO public.catalog_heap_marker VALUES(1)')
        try:
            t.reject('first owner handover with active '+target+' reader',t.a,
                lambda:query(t.a,'SELECT og_lake.configure_internal_owner(%s::name)',(t.owner,)),'55P03')
        finally:query(t.a,'ROLLBACK');query(t.b,'ROLLBACK')
        t.check(target+' handover refusal restores all ownership/ACL/binding/heap state',t.image(),baseline)
    query(t.a,'SELECT og_lake.configure_internal_owner(%s::name)',(t.owner,))
    t.check('retry after actual readers end succeeds',scalar(t.observer,'SELECT internal_owner_oid FROM og_lake_internal.access_policy'),
            scalar(t.observer,'SELECT oid FROM pg_roles WHERE rolname=%s',(t.owner,)))
    query(t.a,'SELECT og_lake.configure_internal_owner(%s::name)',(t.owner,))
    t.check('same owner remains idempotent',scalar(t.observer,'SELECT count(*) FROM og_lake_internal.access_policy'),1)


def upgrade(t):
    """Independent SQL0.5→0.6 upgrade; never replace ACL's0.4→0.5 oracle."""
    version_sql="SELECT extversion FROM pg_extension WHERE extname='og_lake'"
    descriptor_sql=("SELECT p.oid,p.prosrc,p.probin,p.proacl::text,p.proowner,p.provolatile,p.proisstrict,"
        "obj_description(p.oid,'pg_proc') FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace "
        "WHERE n.nspname='og_lake' AND p.proname='create_catalog' AND p.proargtypes::text='19 19 25 25'")
    old_descriptor=query(t.a,descriptor_sql);t.check('one real existing create API',len(old_descriptor),1)
    t.check('SQL0.5 create retains unavailable C entry',old_descriptor[0][1],'og_lake_unavailable')
    owner=scalar(t.a,'SELECT internal_owner_oid FROM og_lake_internal.access_policy')
    before=t.image()
    t.reject('runtime0.6 alone cannot activate SQL0.5 create',t.a,
             lambda:t.create(t.a,'upgrade_server','upgrade_metadata','same'),'0A000')
    t.check('old create refusal changes nothing',t.image(),before)

    query(t.a,'BEGIN');query(t.a,'INSERT INTO public.catalog_heap_marker VALUES(1)')
    query(t.a,"ALTER EXTENSION og_lake UPDATE TO '0.6'")
    t.check('upgrading session sees SQL0.6',scalar(t.a,version_sql),'0.6')
    t.check('other session still sees committed SQL0.5',scalar(t.observer,version_sql),'0.5')
    t.create(t.a,'upgrade_server','upgrade_metadata','same')
    t.check('upgraded API is usable inside the same transaction',t.absent(t.a,'upgrade_server','upgrade_metadata'),False)
    t.check('new catalog remains uncommitted to observer',t.absent(t.observer,'upgrade_server','upgrade_metadata'))
    query(t.a,'ROLLBACK')
    t.check('upgrade rollback restores installed SQL0.5',scalar(t.observer,version_sql),'0.5')
    t.check('upgrade rollback restores exact function OID/body/ACL/comment',query(t.observer,descriptor_sql),old_descriptor)
    t.check('upgrade+creation+heap rollback is atomic',t.image(),before)
    t.reject('rolled back upgrade cannot leave activated create',t.a,
             lambda:t.create(t.a,'upgrade_server','upgrade_metadata','same'),'0A000')

    query(t.a,'BEGIN');query(t.a,'INSERT INTO public.catalog_heap_marker VALUES(2)')
    query(t.a,'SAVEPOINT before_upgrade');query(t.a,"ALTER EXTENSION og_lake UPDATE TO '0.6'")
    t.create(t.a,'upgrade_server','upgrade_metadata','same')
    query(t.a,'ROLLBACK TO before_upgrade');query(t.a,'RELEASE SAVEPOINT before_upgrade')
    t.check('subabort restores SQL0.5 in the parent',scalar(t.a,version_sql),'0.5')
    t.check('subabort restores exact function entry',query(t.a,descriptor_sql),old_descriptor)
    t.check('subabort removes created names',t.absent(t.a,'upgrade_server','upgrade_metadata'))
    query(t.a,'COMMIT')
    expected=dict(before);expected['heap']=[(2,)]
    t.check('parent heap survives rollback of only the upgrade child',t.image(),expected)

    query(t.a,"ALTER EXTENSION og_lake UPDATE TO '0.6'")
    t.check('upgrade COMMIT activates installed SQL0.6',scalar(t.observer,version_sql),'0.6')
    descriptor=query(t.observer,descriptor_sql)
    t.check('upgrade preserves public API identity',descriptor[0][0],old_descriptor[0][0])
    t.check('upgrade installs the actual native create symbol',descriptor[0][1],'og_lake_create_catalog')
    t.check('upgrade keeps create ACL and function owner',descriptor[0][3:5],old_descriptor[0][3:5])
    t.check('upgrade retains the configured private-owner identity',scalar(t.observer,
        'SELECT internal_owner_oid FROM og_lake_internal.access_policy'),owner)
    query(t.a,'SELECT og_lake.configure_internal_owner(%s::name)',(t.owner,))
    query(t.a,'BEGIN');t.create(t.a,'upgrade_server','upgrade_metadata','same');query(t.a,'COMMIT')
    t.check('committed upgraded API creates the catalog',t.absent(t.observer,'upgrade_server','upgrade_metadata'),False)
    m,e=t.fixtures['same']
    native=read_native_catalog(t.observer,'upgrade_metadata',e,t.args.output.parent/'upgraded-created-export.json')
    t.result['upgraded_empty_comparison']=compare_initialized(e,native,t.root('same'),'openGauss og_lake '+t.runtime)
    t.check('upgrade path also creates no data directory',Path(t.root('same')).exists(),False)


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--run-id',default=os.getenv('OG_LAKE_RUN_ID'))
    p.add_argument('--output',type=Path,default=os.getenv('OG_LAKE_RESULT_FILE'))
    p.add_argument('--manifest',type=Path,default=os.getenv('OG_LAKE_FIXTURE_MANIFEST'))
    p.add_argument('--mode',choices=['initial','atomic','collision','legacy','admission','cancel','reader_register','drop_registry','configure_lock','upgrade'],required=True)
    p.add_argument('--legacy-version',choices=['0.2','0.3','0.4','0.5'],default='0.5')
    p.add_argument('--operations',choices=['create-create','create-attach','attach-create','attach-attach'],default='create-attach')
    p.add_argument('--roots',choices=['same','ancestor','sibling'],default='same');p.add_argument('--first-outcome',choices=['commit','rollback'],default='commit')
    p.add_argument('--reader-operation',choices=['create','attach'],default='create')
    p.add_argument('--prepared-legacy',action='store_true');args=p.parse_args()
    if not all((args.run_id,args.output,args.manifest)):p.error('run-id/output/real EMPTY_BUNDLE manifest required')
    validate_run_id(args.run_id)
    if args.mode=='legacy' and (args.operations!='attach-attach' or args.prepared_legacy):p.error('Legacy mode requires --operations attach-attach without --prepared-legacy')
    args.output=args.output.resolve();args.manifest=args.manifest.resolve();args.output.parent.mkdir(parents=True,exist_ok=True)
    result={'status':'fail','run_id':args.run_id,'mode':args.mode,'assertions':[],'artifacts':[]}
    roles=None;db=None;control=None
    try:
        import psycopg2
        control=psycopg2.connect(host=str(ROOT/'.runtime/opengauss-native-lake/debug/socket'),port=55560,
            dbname='postgres',user=os.getenv('USER','qiumc'),connect_timeout=10);control.autocommit=True
        roles=OwnedRoles(control,args.run_id,args.output.parent)
        with OwnedDatabase(args.run_id,args.output.parent) as db:
            t=Case(db,args,result,roles)
            {'initial':initial,'atomic':atomic,'collision':collision,'legacy':legacy,'admission':admission,'cancel':cancellation,
             'reader_register':reader_then_registration,'drop_registry':drop_under_registry_reader,
             'configure_lock':configure_lock,'upgrade':upgrade}[args.mode](t)
        result['status']='pass'
    except Blocked as exc:result.update(status='blocked',reason=str(exc))
    except BaseException as exc:result.update(reason=str(exc),traceback=traceback.format_exc())
    finally:
        if db is not None:result['database_cleanup']=db.cleanup_result;result['artifacts'].extend(db.artifacts())
        if roles is not None:
            try:
                if db is not None and db.cleanup_result and db.cleanup_result['returncode']==0:
                    result['role_cleanup']=cleanup_roles(control,roles.path,args.run_id)
                elif roles.items:raise RuntimeError('Owned DB cleanup unproven; exact role cleanup deferred')
            except BaseException as exc:result.update(status='fail',cleanup_error=str(exc))
            result=roles.redact(result)
        if control is not None:control.close()
        # The outer harness repeats owned cleanup after this process exits.
        # Keep every immutable cleanup report, but let that harness capture the
        # final mutable alias after its last cleanup; an earlier alias hash would
        # otherwise conflict with the final report in the same result.
        result['artifacts'].extend({'path':str(path)} for path in args.output.parent.rglob('*')
            if path.is_file() and path!=args.output and path.name!='database-cleanup.json')
        write_json(args.output,result);print(json.dumps({'status':result['status'],'output':str(args.output)}))
    return {'pass':0,'blocked':2}.get(result['status'],1)


if __name__=='__main__':raise SystemExit(main())
