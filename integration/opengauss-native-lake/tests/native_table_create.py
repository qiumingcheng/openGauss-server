#!/usr/bin/env python3
"""First executable 0.7 CREATE slice. Draft; never run during preparation."""
import argparse,json,os,re,shutil,sys,time,traceback,uuid
from copy import deepcopy
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
ROOT=Path('/home/qiumc/data/code/cpp');DRIVER=ROOT/'integration/opengauss-native-lake'
sys.path.insert(0,str(DRIVER));sys.path.insert(0,str(DRIVER/'tests'))
from lake_harness import Blocked,sha256,validate_run_id,write_json
from inline_fixture import query,scalar
from native_test_context import OwnedDatabase
from native_runtime import RUNTIME, PORT
from empty_manifest import validate_empty_bundle,validate_empty_manifest
from empty_catalog_oracle import compare_initialized
from permission_api import OwnedRoles,cleanup_roles
from create_reference_oracle import export_created_catalog,check_reference_continuation


class Case:
    def __init__(self,db,args,result,roles):
        from psycopg2 import sql
        self.db,self.args,self.result,self.roles,self.sql=db,args,result,roles,sql
        self.a,self.b,self.observer=[db.connect() for _ in range(3)]
        source=Path(os.getenv('OG_LAKE_SOURCE',str(ROOT/'.worktrees/opengauss-native-lake')))
        versions=re.findall(r"^default_version\s*=\s*'([^']+)'",(source/'contrib/og_lake/og_lake.control').read_text(),re.M)
        if versions not in (['0.7'],['0.8'],['0.9'],['0.10'],['0.11'],['0.12'],['0.13'],['0.14'],['0.15'],['0.16'],['0.17'],['0.18'],['0.19'],['0.20'],['0.21'],['0.22'],['0.23'],['0.24'],['0.25'],['0.26'],['0.29'],['0.30'],['0.31'],['0.32'],['0.33'],['0.34'],['0.35'],['0.36'],['0.37'],['0.38'],['0.39'],['0.40'],['0.41'],['0.42'],['0.43'],['0.49'],['0.50'],['0.51'],['0.52'],['0.53'],['0.54'],['0.55']):raise Blocked('This case requires compatible runtime0.7/0.8/0.9 and retained SQL0.7')
        installed=getattr(args,'sql_version',None) or ('0.6' if args.mode=='upgrade' else '0.7')
        if args.mode=='upgrade'and getattr(args,'sql_version',None) is not None:raise Blocked('Upgrade case requires its original SQL0.6 starting version')
        query(self.a,"CREATE EXTENSION og_lake VERSION '"+installed+"'")
        host=json.loads(scalar(self.a,'SELECT og_lake.host_status()'))
        self.check('exact reviewed runtime for retained SQL0.7',host['module_version'],versions[0])
        self.check('runtime declares business CREATE',host.get('business_table_create'),True)
        self.check('runtime declares locator V2',host.get('locator_wire_version'),2)
        self.check('exact installed SQL version',scalar(self.a,"SELECT extversion FROM pg_extension WHERE extname='og_lake'"),installed)
        bundle=validate_empty_bundle(args.manifest)
        entry=bundle['entries']['same']
        if sha256(entry['manifest'])!=entry['sha256']:raise AssertionError('Real EMPTY child manifest changed')
        self.empty_manifest,self.empty_export=validate_empty_manifest(entry['manifest'])
        self.check('real EMPTY has no precreated business definitions',self.empty_manifest['expected_business_tables'],[])
        self.owner=roles.create('private',False)
        query(self.a,'SELECT og_lake.configure_internal_owner(%s::name)',(self.owner,))
        self.schema='created_metadata';self.server='created_lake';self.name='orders'
        query(self.a,'SELECT og_lake.create_catalog(%s::name,%s::name,%s::text,NULL::text)',
              (self.server,self.schema,self.empty_manifest['data_path']))
        query(self.a,'CREATE SCHEMA biz')
        query(self.a,'CREATE TABLE biz.heap_marker(id integer PRIMARY KEY)')
        initial=self.export('native-initial.json')
        self.result['initial_independent_comparison']=compare_initialized(self.empty_export,initial,
            self.empty_manifest['data_path'],'openGauss og_lake '+versions[0])
        self.initial=self.protocol();self.initial_host=self.host_image()
        self.initial_head=self.head();self.groups=[]
        self.check('native initial snapshot/frontiers',self.initial_head,(0,0,1,0))

    def check(self,name,actual,expected=True):
        ok=actual==expected
        self.result['assertions'].append({'name':name,'status':'pass' if ok else 'fail','observed':deepcopy(actual),'expected':deepcopy(expected)})
        if not ok:raise AssertionError(name)

    def relation(self,name=None):return self.sql.SQL('biz.{}').format(self.sql.Identifier(name or self.name))

    def create(self,name=None,conn=None):
        query(conn or self.a,self.sql.SQL("CREATE FOREIGN TABLE {} (id integer,val integer) SERVER {} "
            "OPTIONS(binding_mode 'create',lake_schema 'main')").format(self.relation(name),self.sql.Identifier(self.server)))

    def rows(self,conn=None,name=None):
        return query(conn or self.a,self.sql.SQL('SELECT id,val FROM {} ORDER BY id').format(self.relation(name)))

    def head(self,conn=None):
        return query(conn or self.observer,self.sql.SQL('SELECT snapshot_id,schema_version,next_catalog_id,next_file_id FROM {}.ducklake_snapshot ORDER BY snapshot_id DESC LIMIT 1').format(self.sql.Identifier(self.schema)))[0]

    def protocol(self,conn=None):
        conn=conn or self.observer
        names=query(conn,'SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=%s AND c.relkind=\'r\' ORDER BY c.relname',(self.schema,))
        return {name:sorted(repr(tuple(row)) for row in query(conn,self.sql.SQL('SELECT * FROM {}.{}').format(self.sql.Identifier(self.schema),self.sql.Identifier(name)))) for (name,) in names}

    def host_image(self,conn=None):
        conn=conn or self.observer
        out={'relations':query(conn,"SELECT c.oid,n.nspname,c.relname,c.relkind,c.relowner FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname IN ('biz',%s) ORDER BY c.oid",(self.schema,)),
             'heap':query(conn,'SELECT * FROM biz.heap_marker ORDER BY id')}
        for table in ['catalog_binding','table_binding','relation_binding','column_binding','metadata_relation','access_policy']:
            out[table]=sorted(repr(tuple(row)) for row in query(conn,self.sql.SQL('SELECT * FROM og_lake_internal.{}').format(self.sql.Identifier(table))))
        return out

    def export(self,name):return export_created_catalog(self.observer,self.schema,self.empty_export,self.args.output.parent/name)

    def no_published_local_identity(self,label):
        self.check(label+' standard table count',scalar(self.a,self.sql.SQL('SELECT count(*) FROM {}.ducklake_table').format(self.sql.Identifier(self.schema))),0)
        for name in ['table_binding','relation_binding','column_binding']:
            self.check(label+' no provisional '+name,scalar(self.a,self.sql.SQL('SELECT count(*) FROM og_lake_internal.{}').format(self.sql.Identifier(name))),0)
        self.check(label+' other session sees exact committed catalog',self.protocol(),self.initial)
        self.check(label+' other session sees exact host identities',self.host_image(),self.initial_host)

    def published(self,expected,allocation_order=None):
        self.allocated_rows=expected if allocation_order is None else allocation_order
        self.check('allocation order has exactly final business rows',sorted(self.allocated_rows,key=lambda r:r['id']),expected)
        self.check('one snapshot and schema/catalog allocation',self.head(),(1,1,2,1 if expected else 0))
        self.check('native published business rows',self.rows(),[(r['id'],r['val']) for r in expected])
        table=query(self.a,self.sql.SQL('SELECT table_id,table_uuid::text,begin_snapshot,end_snapshot,schema_id,table_name,path,path_is_relative FROM {}.ducklake_table').format(self.sql.Identifier(self.schema)))
        self.check('exactly one native table definition',len(table),1)
        tid,table_uuid,begin,end,schema_id,name,path,relative=table[0]
        self.check('first permanent table ID allocated from catalog',tid,1)
        self.check('table definition fields',(begin,end,schema_id,name,path,relative),(1,None,0,self.name,self.name+'/',True))
        self.check('real nonzero UUID',uuid.UUID(table_uuid).int!=0)
        registrations=query(self.a,self.sql.SQL('SELECT table_id,table_name,schema_version FROM {}.ducklake_inlined_data_tables').format(self.sql.Identifier(self.schema)))
        self.carrier='ducklake_inlined_'+uuid.UUID(table_uuid).hex
        self.check('explicit UUID carrier registered',(len(registrations),registrations), (1,[(tid,self.carrier,1)]))
        carrier_oid=scalar(self.a,'SELECT c.oid FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=%s AND c.relname=%s',(self.schema,self.carrier))
        host_oid=scalar(self.a,'SELECT c.oid FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=\'biz\' AND c.relname=%s',(self.name,))
        self.check('committed carrier no longer internally owned by host',scalar(self.a,"SELECT count(*) FROM pg_depend WHERE classid='pg_class'::regclass AND objid=%s AND refclassid='pg_class'::regclass AND refobjid=%s AND deptype='i'",(carrier_oid,host_oid)),0)
        binding=query(self.a,'SELECT b.table_id,b.schema_version,b.table_uuid::text FROM og_lake_internal.table_binding b JOIN og_lake_internal.relation_binding r ON r.binding_id=b.binding_id WHERE r.relation_oid=%s',(host_oid,))
        self.check('final host binding uses only permanent identity',binding,[(tid,1,table_uuid)])
        physical=query(self.a,self.sql.SQL('SELECT _ducklake_row_id,_ducklake_begin_snapshot,_ducklake_end_snapshot,id,val FROM {}.{} ORDER BY _ducklake_row_id').format(self.sql.Identifier(self.schema),self.sql.Identifier(self.carrier)))
        self.check('only final private versions reach carrier',physical,[(i,1,None,r['id'],r['val']) for i,r in enumerate(self.allocated_rows)])
        stats=query(self.a,self.sql.SQL('SELECT table_id,record_count,next_row_id,file_size_bytes FROM {}.ducklake_table_stats').format(self.sql.Identifier(self.schema)))
        self.check('real empty/no-stats or first-batch frontiers',stats,[(tid,len(expected),len(expected),0)] if expected else [])
        self.check('snapshot changes use standard grouped encoding',query(self.a,self.sql.SQL('SELECT changes_made FROM {}.ducklake_snapshot_changes WHERE snapshot_id=1').format(self.sql.Identifier(self.schema))),
                   [('created_table:"main"."'+self.name+'"'+(',inlined_insert:1' if expected else ''),)])
        self.check('schema version record exact',query(self.a,self.sql.SQL('SELECT begin_snapshot,schema_version,table_id FROM {}.ducklake_schema_versions').format(self.sql.Identifier(self.schema))),[(1,1,tid)])
        cols=query(self.a,self.sql.SQL('SELECT column_id,begin_snapshot,end_snapshot,table_id,column_order,column_name,column_type,initial_default,default_value,nulls_allowed,parent_column,default_value_type,default_value_dialect FROM {}.ducklake_column ORDER BY column_id').format(self.sql.Identifier(self.schema)))
        self.check('all13 standard column fields exact',cols,[(i,1,None,tid,i,n,'int32',None,'NULL',True,None,'literal','duckdb') for i,n in enumerate(['id','val'],1)])
        self.check('all live bindings have configured private storage owner',scalar(self.a,'SELECT relowner FROM pg_class WHERE oid=%s',(carrier_oid,)),scalar(self.a,'SELECT oid FROM pg_roles WHERE rolname=%s',(self.owner,)))

    def interop(self,expected,label):
        native=self.export(label+'-native.json')
        check_reference_continuation(self,native,self.groups,expected,self.args.output.parent/(label+'-reference'))


CREATE_REFERENCE='CREATE TABLE lake.main.orders(id INTEGER,val INTEGER)'


def create_empty(t):
    query(t.a,'BEGIN');t.create()
    t.check('new local table immediately readable as empty',t.rows(),[])
    t.no_published_local_identity('uncommitted empty CREATE')
    query(t.a,'COMMIT');t.groups=[[CREATE_REFERENCE]]
    t.published([])
    before=t.protocol()
    query(t.a,'DROP FOREIGN TABLE biz.orders')
    t.check('administrator host DROP preserves full standard catalog and UUID carrier',t.protocol(),before)
    t.check('host relation alone removed',scalar(t.a,"SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='biz' AND c.relname='orders'"),0)
    t.check('host binding removed without deleting standard lake table',scalar(t.a,'SELECT count(*) FROM og_lake_internal.table_binding'),0)
    t.interop([],'after-host-drop')


def create_net_dml(t):
    query(t.a,'BEGIN');t.create();query(t.a,'INSERT INTO biz.heap_marker VALUES(1)')
    t.check('local INSERT RETURNING business-only columns',query(t.a,'INSERT INTO biz.orders VALUES(1,10),(2,20),(3,NULL) RETURNING id,val'),[(1,10),(2,20),(3,None)])
    t.check('local read sees completed INSERT',t.rows(),[(1,10),(2,20),(3,None)])
    t.check('first local UPDATE modifies distinct identities',sorted(query(t.a,'UPDATE biz.orders SET val=val+1 RETURNING id,val')),[(1,11),(2,21),(3,None)])
    t.check('second local UPDATE uses previous version',query(t.a,'UPDATE biz.orders SET val=val+100 WHERE id=1 RETURNING id,val'),[(1,111)])
    t.check('local DELETE RETURNING returns exact removed version',query(t.a,'DELETE FROM biz.orders WHERE id=2 RETURNING id,val'),[(2,21)])
    t.check('private final business state',t.rows(),[(1,111),(3,None)])
    t.no_published_local_identity('uncommitted CREATE and I/U/U/D')
    t.check('ordinary heap own write present',query(t.a,'SELECT * FROM biz.heap_marker'),[(1,)])
    query(t.a,'COMMIT')
    t.groups=[[CREATE_REFERENCE,'INSERT INTO lake.main.orders VALUES(1,10),(2,20),(3,NULL)',
        'UPDATE lake.main.orders SET val=val+1','UPDATE lake.main.orders SET val=val+100 WHERE id=1','DELETE FROM lake.main.orders WHERE id=2']]
    expected=[{'id':1,'val':111},{'id':3,'val':None}]
    t.published(expected,[expected[1],expected[0]]);t.check('heap and lake both commit',query(t.observer,'SELECT * FROM biz.heap_marker'),[(1,)])
    before=t.protocol();query(t.a,'DROP EXTENSION og_lake CASCADE')
    t.check('extension DROP preserves complete standard catalog and UUID carrier',t.protocol(),before)
    t.check('extension SQL object was actually removed',scalar(t.a,"SELECT count(*) FROM pg_extension WHERE extname='og_lake'"),0)
    t.interop(expected,'after-extension-drop')


def create_savepoints(t):
    query(t.a,'BEGIN');t.create('outer_rollback');query(t.a,'INSERT INTO biz.outer_rollback VALUES(8,80)');query(t.a,'INSERT INTO biz.heap_marker VALUES(8)')
    query(t.a,'ROLLBACK')
    t.check('whole transaction restores standard state',t.protocol(),t.initial)
    t.check('whole transaction restores physical/registry/binding/heap state',t.host_image(),t.initial_host)
    query(t.a,'BEGIN');query(t.a,'SAVEPOINT child_create');t.create('discarded')
    query(t.a,'INSERT INTO biz.discarded VALUES(9,90)');query(t.a,'ROLLBACK TO child_create');query(t.a,'RELEASE SAVEPOINT child_create')
    t.check('child CREATE rollback removes carrier and OID registry',t.host_image(t.a),t.initial_host)
    t.create();query(t.a,'INSERT INTO biz.orders VALUES(1,10),(2,NULL)')
    original_oid=scalar(t.a,"SELECT 'biz.orders'::regclass::oid")
    query(t.a,'SAVEPOINT child_drop');query(t.a,'DROP FOREIGN TABLE biz.orders')
    t.check('child DROP actually hides parent local relation',scalar(t.a,"SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='biz' AND c.relname='orders'"),0)
    query(t.a,'ROLLBACK TO child_drop');query(t.a,'RELEASE SAVEPOINT child_drop')
    t.check('child DROP rollback restores exact parent host OID',scalar(t.a,"SELECT 'biz.orders'::regclass::oid"),original_oid)
    t.check('child DROP rollback restores parent private rows',t.rows(),[(1,10),(2,None)])
    t.check('child DROP rollback restores the exact temporary ownership edge',scalar(t.a,
        "SELECT count(*) FROM pg_depend d JOIN pg_class c ON c.oid=d.objid JOIN pg_namespace n ON n.oid=c.relnamespace "
        "WHERE d.classid='pg_class'::regclass AND d.refclassid='pg_class'::regclass AND d.refobjid=%s AND d.deptype='i' AND n.nspname=%s",
        (original_oid,t.schema)),1)
    query(t.a,'SAVEPOINT child_update');query(t.a,'UPDATE biz.orders SET val=999');query(t.a,'ROLLBACK TO child_update');query(t.a,'RELEASE SAVEPOINT child_update')
    t.check('child UPDATE rollback restores both prior LOCAL versions',t.rows(),[(1,10),(2,None)])
    query(t.a,'SAVEPOINT released_child');t.create('discarded');query(t.a,'INSERT INTO biz.discarded VALUES(9,90)');query(t.a,'DROP FOREIGN TABLE biz.discarded');query(t.a,'RELEASE SAVEPOINT released_child')
    t.no_published_local_identity('parent CREATE survives child cleanup')
    query(t.a,'COMMIT');t.groups=[[CREATE_REFERENCE,'INSERT INTO lake.main.orders VALUES(1,10),(2,NULL)']]
    expected=[{'id':1,'val':10},{'id':2,'val':None}];t.published(expected)
    before=t.protocol();host_before=t.host_image()
    query(t.a,'BEGIN');t.create('net_empty');query(t.a,'INSERT INTO biz.net_empty VALUES(7,70)');query(t.a,'UPDATE biz.net_empty SET val=71');query(t.a,'DELETE FROM biz.net_empty');query(t.a,'DROP FOREIGN TABLE biz.net_empty');query(t.a,'COMMIT')
    t.groups.append(['CREATE TABLE lake.main.net_empty(id INTEGER,val INTEGER)','INSERT INTO lake.main.net_empty VALUES(7,70)',
        'UPDATE lake.main.net_empty SET val=71','DELETE FROM lake.main.net_empty','DROP TABLE lake.main.net_empty'])
    t.check('CREATE-I-U-D-DROP net empty leaves every standard row unchanged',t.protocol(),before)
    t.check('CREATE-DROP cleans local carrier/registry with no host residue',t.host_image(),host_before)
    t.interop(expected,'after-savepoints-and-net-empty')


def reject(t,conn,label,statement,state,params=None):
    import psycopg2
    try:query(conn,statement,params)
    except psycopg2.Error as exc:t.check(label+' SQLSTATE',exc.pgcode,state)
    else:t.check(label+' rejected','accepted',state)


def create_permissions(t):
    name=t.roles.create('creator')
    query(t.a,t.sql.SQL('GRANT USAGE,CREATE ON SCHEMA biz TO {}').format(t.sql.Identifier(name)))
    user,identity=t.roles.connect(t.db,'creator');t.result['ordinary_login']=identity
    stmt=t.sql.SQL("CREATE FOREIGN TABLE {} (id integer,val integer) SERVER {} OPTIONS(binding_mode 'create',lake_schema 'main')").format(t.relation(),t.sql.Identifier(t.server))
    reject(t,user,'server usage is required before private DDL',stmt,'42501')
    t.check('denied CREATE leaves all host and carrier objects unchanged',t.host_image(),t.initial_host)
    query(t.a,t.sql.SQL('GRANT USAGE ON FOREIGN SERVER {} TO {}').format(t.sql.Identifier(t.server),t.sql.Identifier(name)))
    reject(t,user,'ordinary creator cannot read private catalog',t.sql.SQL('SELECT * FROM {}.ducklake_table').format(t.sql.Identifier(t.schema)),'42501')
    query(user,'BEGIN');t.create(conn=user)
    query(user,'INSERT INTO biz.orders VALUES(1,10),(2,NULL)')
    query(user,'UPDATE biz.orders SET val=val+1 WHERE id=1')
    query(user,'SAVEPOINT removed');query(user,'DELETE FROM biz.orders WHERE id=2');query(user,'ROLLBACK TO removed');query(user,'RELEASE SAVEPOINT removed')
    t.check('actual ordinary owner reads its private versions',t.rows(user),[(1,11),(2,None)])
    t.check('private owner SPI restores actual caller identity',scalar(user,'SELECT current_user'),name)
    query(user,'COMMIT');expected=[{'id':1,'val':11},{'id':2,'val':None}]
    t.groups=[[CREATE_REFERENCE,'INSERT INTO lake.main.orders VALUES(1,10),(2,NULL)','UPDATE lake.main.orders SET val=val+1 WHERE id=1']]
    t.published(expected,[expected[1],expected[0]])
    t.check('ordinary owner reads committed permanent binding',t.rows(user),[(1,11),(2,None)])
    before=t.protocol()
    reject(t,user,'committed host unbind retains explicit admin guard','DROP FOREIGN TABLE biz.orders','42501')
    t.check('denied unbind preserves every standard row',t.protocol(),before)
    reject(t,user,'ordinary owner still cannot read private carrier',t.sql.SQL('SELECT * FROM {}.{}').format(t.sql.Identifier(t.schema),t.sql.Identifier(t.carrier)),'42501')
    t.interop(expected,'ordinary-owner')


def create_upgrade(t):
    reject(t,t.a,'installed SQL0.6 rejects new CREATE API',t.sql.SQL("CREATE FOREIGN TABLE {} (id integer,val integer) SERVER {} OPTIONS(binding_mode 'create',lake_schema 'main')").format(t.relation(),t.sql.Identifier(t.server)),'0A000')
    t.check('old version rejection is atomic',t.host_image(),t.initial_host)
    before=t.protocol();query(t.a,"ALTER EXTENSION og_lake UPDATE TO '0.7'")
    t.check('SQL upgrade installs exact0.7',scalar(t.a,"SELECT extversion FROM pg_extension WHERE extname='og_lake'"),'0.7')
    t.check('SQL upgrade leaves all DuckLake standard rows unchanged',t.protocol(),before)
    query(t.a,'BEGIN');t.create();query(t.a,'INSERT INTO biz.orders VALUES(1,10)');query(t.a,'COMMIT')
    t.groups=[[CREATE_REFERENCE,'INSERT INTO lake.main.orders VALUES(1,10)']]
    expected=[{'id':1,'val':10}];t.published(expected);t.interop(expected,'after-upgrade')


def create_publish_cancel(t):
    a,holder,observer=t.a,t.b,t.observer
    app='lake_create_cancel_'+uuid.uuid4().hex[:16]
    query(a,'SET application_name=%s',(app,));query(a,'SET statement_timeout=30000')
    columns=scalar(observer,'SELECT %s::regclass::oid',(t.schema+'.ducklake_table_column_stats',))
    dbid=scalar(observer,'SELECT oid FROM pg_database WHERE datname=current_database()')
    existing_carriers=set(row[0] for row in query(a,"SELECT c.oid FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=%s AND c.relkind='r'",(t.schema,)))
    verified=False;future=None;executor=None;pid=None
    try:
        query(holder,'BEGIN');query(holder,t.sql.SQL('LOCK TABLE {}.ducklake_table_column_stats IN SHARE MODE').format(t.sql.Identifier(t.schema)))
        query(a,'BEGIN');pid=scalar(a,'SELECT pg_backend_pid()');t.create()
        query(a,'INSERT INTO biz.orders VALUES(1,10),(2,NULL)');query(a,'INSERT INTO biz.heap_marker VALUES(1)')
        # LIKE ducklake_inlined_% also matches standard metadata relations.
        # Locate the independently observed newly-created table, then retain
        # the original ownership-edge assertion against that exact OID.
        observed_carriers=query(a,"SELECT c.oid,c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=%s AND c.relkind='r'",(t.schema,))
        created_carriers=[row for row in observed_carriers if row[0] not in existing_carriers]
        t.check('one independently observed new physical carrier',len(created_carriers),1)
        carrier=created_carriers[0][0]
        t.result['carrier_selection']={'new_relation':created_carriers[0],
            'previous_ambiguous_pattern_matches':query(a,'SELECT c.oid,c.relname,c.relkind FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=%s AND c.relname LIKE %s ORDER BY c.relname',(t.schema,'ducklake_inlined_%'))}
        host=scalar(a,"SELECT 'biz.orders'::regclass::oid")
        t.check('unpublished CREATE has one temporary carrier ownership edge',scalar(a,"SELECT count(*) FROM pg_depend WHERE classid='pg_class'::regclass AND objid=%s AND refclassid='pg_class'::regclass AND refobjid=%s AND deptype='i'",(carrier,host)),1)
        def commit():
            import psycopg2
            try:query(a,'COMMIT');return {'state':'accepted'}
            except psycopg2.Error as exc:return {'state':exc.pgcode,'message':str(exc)}
        executor=ThreadPoolExecutor(max_workers=1);future=executor.submit(commit)
        deadline=time.monotonic()+12;waits=[]
        while time.monotonic()<deadline:
            waits=query(observer,"SELECT pid,database,relation,mode FROM pg_locks WHERE locktype='relation' AND database=%s AND relation=%s AND pid=%s AND mode='RowExclusiveLock' AND NOT granted",(dbid,columns,pid))
            if waits or future.done():break
            time.sleep(.02)
        t.check('publisher waits at actual column-stats INSERT after detach',len(waits),1)
        t.check('wait belongs to this owned publisher',query(observer,'SELECT application_name FROM pg_stat_activity WHERE pid=%s',(pid,)),[(app,)])
        verified=True
        preceding=[carrier,scalar(observer,"SELECT 'pg_catalog.pg_depend'::regclass::oid"),
            scalar(observer,"SELECT 'og_lake_internal.table_binding'::regclass::oid"),
            scalar(observer,'SELECT %s::regclass::oid',(t.schema+'.ducklake_table',)),
            scalar(observer,"SELECT 'biz.heap_marker'::regclass::oid")]
        granted=query(observer,"SELECT relation,mode FROM pg_locks WHERE locktype='relation' AND database=%s AND pid=%s AND relation=ANY(%s::oid[]) AND mode='RowExclusiveLock' AND granted",(dbid,pid,preceding))
        t.check('publisher already holds definition, binding, carrier, dependency and heap write locks',sorted({row[0] for row in granted}),sorted(preceding))
        t.result['publication_barrier']={'database_oid':dbid,'pid':pid,'relation_oid':columns,'waits':waits,'source_order':'column-stats INSERT follows winning claim, definitions, host binding, pg_depend detach, carrier INSERT and table_stats UPDATE'}
        t.result['publication_barrier']['already_granted_write_locks']=granted
        t.check('partial publication remains invisible',t.protocol(),t.initial)
        t.check('cancel actual publisher',scalar(observer,'SELECT pg_cancel_backend(%s)',(pid,)),True)
        outcome=future.result(timeout=10);t.result['cancelled_commit']=outcome
        t.check('COMMIT cancellation SQLSTATE',outcome['state'],'57014')
        query(a,'ROLLBACK')
        t.check('cancel restores all standard rows',t.protocol(),t.initial)
        t.check('cancel removes host, carrier, registry and ordinary heap writes',t.host_image(),t.initial_host)
        t.check('cancelled connection is reusable',scalar(a,'SELECT 6*7'),42)
    finally:
        if future is not None and not future.done() and verified:scalar(observer,'SELECT pg_cancel_backend(%s)',(pid,))
        query(holder,'ROLLBACK')
        if executor is not None:executor.shutdown(wait=True)
        query(a,'ROLLBACK')
    query(t.b,'BEGIN');t.create(conn=t.b);query(t.b,'INSERT INTO biz.orders VALUES(1,10),(2,NULL)');query(t.b,'COMMIT')
    expected=[{'id':1,'val':10},{'id':2,'val':None}]
    t.groups=[[CREATE_REFERENCE,'INSERT INTO lake.main.orders VALUES(1,10),(2,NULL)']]
    t.published(expected);t.interop(expected,'retry-after-cancel')


def create_prepared(t):
    query(t.a,'BEGIN');t.create()
    query(t.a,'PREPARE lake_read(integer) AS SELECT id,val FROM biz.orders WHERE id >= $1 ORDER BY id')
    query(t.a,'PREPARE lake_update(integer,integer) AS UPDATE biz.orders SET val=$1 WHERE id=$2 RETURNING id,val')
    query(t.a,'INSERT INTO biz.orders VALUES(1,10),(2,NULL)')
    t.check('prepared scan resolves real local objects at execution',query(t.a,'EXECUTE lake_read(1)'),[(1,10),(2,None)])
    t.check('prepared modify resolves the local locator at execution',query(t.a,'EXECUTE lake_update(11,1)'),[(1,11)])
    query(t.a,'SAVEPOINT change');query(t.a,'EXECUTE lake_update(99,1)');query(t.a,'ROLLBACK TO change');query(t.a,'RELEASE SAVEPOINT change')
    t.check('same prepared plan observes restored local version after savepoint',query(t.a,'EXECUTE lake_read(1)'),[(1,11),(2,None)])
    query(t.a,'COMMIT');expected=[{'id':1,'val':11},{'id':2,'val':None}]
    t.groups=[[CREATE_REFERENCE,'INSERT INTO lake.main.orders VALUES(1,10),(2,NULL)','UPDATE lake.main.orders SET val=11 WHERE id=1']]
    t.published(expected,[expected[1],expected[0]]);t.interop(expected,'prepared-local-publication')
    t.check('prepared scan reuses plan after LOCAL becomes committed',query(t.a,'EXECUTE lake_read(1)'),[(1,11),(2,None)])
    t.check('prepared modify reuses plan with a fresh permanent locator',query(t.a,'EXECUTE lake_update(12,1)'),[(1,12)])
    t.check('old prepared scan sees new committed versions',query(t.a,'EXECUTE lake_read(1)'),[(1,12),(2,None)])
    t.check('second commit uses existing table ID and schema version',t.head(),(2,1,2,2))
    t.check('other session sees prepared statement commit',t.rows(t.observer),[(1,12),(2,None)])


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--mode',choices=['empty','net_dml','savepoints','permissions','upgrade','publish_cancel','prepared'],required=True)
    p.add_argument('--sql-version',choices=['0.7','0.10','0.11'],help='Explicit tested SQL contract; omitted retains historical case behavior')
    p.add_argument('--run-id',default=os.getenv('OG_LAKE_RUN_ID'))
    p.add_argument('--manifest',type=Path,default=os.getenv('OG_LAKE_FIXTURE_MANIFEST'))
    p.add_argument('--output',type=Path,default=os.getenv('OG_LAKE_RESULT_FILE'))
    args=p.parse_args()
    if not all([args.run_id,args.manifest,args.output]):p.error('run-id/real EMPTY_BUNDLE manifest/output required')
    validate_run_id(args.run_id);args.output=args.output.resolve();args.manifest=args.manifest.resolve()
    args.output.parent.mkdir(parents=True,exist_ok=True)
    result={'status':'fail','scope':'native_business_create_'+args.mode,'run_id':args.run_id,'assertions':[],'artifacts':[],
        'limitations':['Scoped business CREATE qualification; does not imply the full feature is accepted.',
        'No file SDK, physical carrier GC, crash, RAC or live external connector is covered.']}
    roles=None;db=None;control=None
    # Archive exact script/helper bytes; never leave a mutable live source path
    # as the only evidence of what executed.
    inputs=[Path(__file__).resolve(),Path(__file__).with_name('create_reference_oracle.py'),
        DRIVER/'native_test_context.py',DRIVER/'tests/permission_api.py',DRIVER/'tests/empty_catalog_oracle.py',
        DRIVER/'tests/empty_manifest.py',DRIVER/'tests/empty_reference_fixture.py',DRIVER/'tests/reverse_reference.py',DRIVER/'inline_fixture.py']
    frozen={str(x):sha256(x) for x in inputs};archive=args.output.parent/'executed-inputs';archive.mkdir(exist_ok=False)
    for i,path in enumerate(inputs):shutil.copy2(path,archive/('%02d-'%i+path.name))
    write_json(archive/'input-hashes.json',frozen)
    try:
        import psycopg2
        control=psycopg2.connect(host=str(RUNTIME/'socket'),port=PORT,
            dbname='postgres',user=os.getenv('USER','qiumc'),connect_timeout=10);control.autocommit=True
        roles=OwnedRoles(control,args.run_id,args.output.parent)
        with OwnedDatabase(args.run_id,args.output.parent) as db:
            t=Case(db,args,result,roles)
            {'empty':create_empty,'net_dml':create_net_dml,'savepoints':create_savepoints,
             'permissions':create_permissions,'upgrade':create_upgrade,'publish_cancel':create_publish_cancel,
             'prepared':create_prepared}[args.mode](t)
            t.check('all executed input files unchanged',{str(x):sha256(x) for x in inputs},frozen)
        result['status']='pass'
    except Blocked as exc:result.update(status='blocked',reason=str(exc))
    except BaseException as exc:result.update(reason=str(exc),traceback=traceback.format_exc())
    finally:
        if db is not None:result['database_cleanup']=db.cleanup_result;result['artifacts'].extend(db.artifacts())
        if roles is not None:
            try:
                if db is not None and db.cleanup_result and db.cleanup_result['returncode']==0:
                    result['role_cleanup']=cleanup_roles(control,roles.path,args.run_id)
                elif roles.items:raise RuntimeError('Database cleanup unproven; exact role cleanup deferred')
            except BaseException as exc:result.update(status='fail',cleanup_error=str(exc))
            result=roles.redact(result)
        if control is not None:control.close()
        seen={item['path'] for item in result['artifacts']}
        for path in args.output.parent.rglob('*'):
            if path.is_file() and path!=args.output and path.name!='database-cleanup.json' and str(path) not in seen:
                result['artifacts'].append({'path':str(path)});seen.add(str(path))
        write_json(args.output,result);print(json.dumps({'status':result['status'],'output':str(args.output)}))
    return {'pass':0,'blocked':2}.get(result['status'],1)

if __name__=='__main__':raise SystemExit(main())
