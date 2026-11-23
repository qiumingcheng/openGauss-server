#!/usr/bin/env python3
"""Draft real-login 0.5 ACL acceptance. No product API is mocked or installed here."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import os
import re
from pathlib import Path
import sys
import time
import traceback

from permission_api import (WORKSPACE,DRIVER,API_VERSION,OwnedRoles,cleanup_roles,
                            configure_and_attach,adopt_private_owner,reviewed_permission_runtime)
sys.path.insert(0,str(DRIVER)); sys.path.insert(0,str(DRIVER/'tests'))
from inline_fixture import multiset,query,scalar
from lake_harness import Blocked,validate_run_id,write_json
from native_test_context import OwnedDatabase,load_reference
from reverse_modify_reference import reverse_modify_reference


class Case:
    def __init__(self,db,args,result,roles):
        from psycopg2 import sql
        self.sql,self.db,self.args,self.result,self.roles=sql,db,args,result,roles
        self.admin,self.observer,self.holder=[db.connect() for _ in range(3)]
        query(self.admin,"CREATE EXTENSION og_lake VERSION '0.5'")
        self.check('installed exact permission extension',scalar(self.admin,"SELECT extversion FROM pg_extension WHERE extname='og_lake'"),API_VERSION)
        runtime_version=reviewed_permission_runtime()
        self.check('actual native runtime version',json.loads(scalar(self.admin,'SELECT og_lake.host_status()'))['module_version'],runtime_version)
        result['runtime_version']=runtime_version
        result['installed_extension_version']=API_VERSION
        self.manifest,self.exported,counts,artifacts=load_reference(self.admin,args.manifest,args.run_id,args.output.parent)
        result['artifacts'].extend(artifacts)
        self.check('genuine simple reference input',self.manifest['case'],'I')
        self.check('complete reference import',len(counts),len(self.exported['tables']))
        self.table_id=self.manifest['table_id']
        self.initial=[(r['id'],r['val']) for r in self.manifest['expected']]
        self.carrier=next(r['table_name'] for t in self.exported['tables'] if t['table_name']=='ducklake_inlined_data_tables'
                          for r in t['rows'] if r['table_id']==self.table_id)
        before=self.image()
        private=roles.create('private',False)
        for alias in ('reader','writer','stranger','columns','viewowner'): roles.create(alias)
        configure_and_attach(self.admin,self.exported,private,self.table_id)
        self.check('explicit ownership setup does not alter standard data',self.image(),before)
        self.check('registered private owner is exact role OID',
            scalar(self.admin,'SELECT internal_owner_oid FROM og_lake_internal.access_policy'),
            scalar(self.admin,'SELECT oid FROM pg_roles WHERE rolname=%s',(private,)))
        self.check('policy retains extension administrator owner',
            scalar(self.admin,"SELECT c.relowner=e.extowner FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
                              "CROSS JOIN pg_extension e WHERE n.nspname='og_lake_internal' AND c.relname='access_policy' AND e.extname='og_lake'"),True)
        query(self.admin,'CREATE TABLE biz.heap_marker(id integer PRIMARY KEY,val integer)')
        for alias in ('reader','writer','stranger','columns','viewowner'):
            role=sql.Identifier(roles.name(alias))
            query(self.admin,sql.SQL('GRANT USAGE ON SCHEMA biz TO {}').format(role))
            query(self.admin,sql.SQL('GRANT USAGE ON FOREIGN SERVER lake_reference TO {}').format(role))
        for alias in ('reader','writer','viewowner'):
            query(self.admin,sql.SQL('GRANT SELECT ON biz.orders TO {}').format(sql.Identifier(roles.name(alias))))
        query(self.admin,sql.SQL('GRANT INSERT,UPDATE,DELETE ON biz.orders TO {}').format(sql.Identifier(roles.name('writer'))))
        query(self.admin,sql.SQL('GRANT INSERT,SELECT ON biz.heap_marker TO {}').format(sql.Identifier(roles.name('writer'))))
        query(self.admin,sql.SQL('GRANT SELECT(id),UPDATE(val),INSERT(id,val) ON biz.orders TO {}').format(sql.Identifier(roles.name('columns'))))
        self.connections={}
        for alias in ('reader','writer','stranger','columns','viewowner'):
            conn,identity=roles.connect(db,alias); self.connections[alias]=conn
            result.setdefault('real_logins',[]).append(identity)
            self.identity(conn,alias)
            self.check(alias+' has no metadata schema USAGE',scalar(self.admin,
                "SELECT has_schema_privilege(%s,'lake_metadata','USAGE')",(roles.name(alias),)),False)
            self.check(alias+' has no private schema USAGE',scalar(self.admin,
                "SELECT has_schema_privilege(%s,'og_lake_internal','USAGE')",(roles.name(alias),)),False)
        self.baseline=self.image()

    def check(self,label,observed,expected=True):
        good=observed==expected
        self.result['assertions'].append({'name':label,'status':'pass' if good else 'fail',
                                         'details':{'observed':observed,'expected':expected}})
        if not good: raise AssertionError(label)

    def identity(self,conn,alias):
        self.check(alias+' real session/current identity',query(conn,'SELECT session_user,current_user'),
                   [(self.roles.name(alias),self.roles.name(alias))])

    def image(self):
        return {t['table_name']:sorted(repr(tuple(row)) for row in query(self.observer,
            self.sql.SQL('SELECT * FROM lake_metadata.{}').format(self.sql.Identifier(t['table_name']))))
            for t in self.exported['tables']}

    def reject(self,label,conn,statement,state='42501',params=None,savepoint=False):
        import psycopg2
        if savepoint: query(conn,'SAVEPOINT permission_denial')
        try:
            query(conn,statement,params)
        except psycopg2.Error as exc:
            if savepoint: query(conn,'ROLLBACK TO permission_denial')
            self.check(label+' exact SQLSTATE',exc.pgcode,state)
        else:
            if savepoint: query(conn,'ROLLBACK TO permission_denial')
            self.check(label+' must reject','accepted',state)
        finally:
            if savepoint: query(conn,'RELEASE SAVEPOINT permission_denial')

    def unchanged(self,label):
        self.check(label+' full standard directory unchanged',self.image(),self.baseline)
        self.check(label+' ordinary heap empty',scalar(self.observer,'SELECT count(*) FROM biz.heap_marker'),0)

    def read(self,conn): return query(conn,'SELECT id,val FROM biz.orders ORDER BY id')


def acl(t):
    reader,writer,stranger=[t.connections[a] for a in ('reader','writer','stranger')]
    t.check('actual reader reads reference values',t.read(reader),t.initial)
    t.reject('reader INSERT',reader,'INSERT INTO biz.orders VALUES(99,990)')
    t.reject('reader UPDATE',reader,'UPDATE biz.orders SET val=11 WHERE id=1')
    t.reject('reader DELETE',reader,'DELETE FROM biz.orders WHERE id=2')
    for name,statement in [('SELECT','SELECT * FROM biz.orders'),('INSERT','INSERT INTO biz.orders VALUES(99,990)'),
                           ('UPDATE','UPDATE biz.orders SET val=11 WHERE id=1'),('DELETE','DELETE FROM biz.orders WHERE id=2')]:
        t.reject('stranger '+name,stranger,statement)
    t.unchanged('ordinary ACL refusals')
    query(writer,'BEGIN')
    t.check('writer INSERT RETURNING',query(writer,'INSERT INTO biz.orders VALUES(99,990) RETURNING id,val'),[(99,990)])
    t.check('writer UPDATE RETURNING',query(writer,'UPDATE biz.orders SET val=11 WHERE id=1 RETURNING id,val'),[(1,11)])
    t.check('writer DELETE RETURNING',query(writer,'DELETE FROM biz.orders WHERE id=2 RETURNING id,val'),[(2,100)])
    query(writer,'INSERT INTO biz.heap_marker VALUES(1,1)'); query(writer,'COMMIT')
    t.identity(writer,'writer')
    expected=[(1,11),(3,200),(99,990)]
    t.check('committed ordinary DML is visible to authorized reader',t.read(reader),expected)
    t.check('ordinary heap commits with the lake',query(t.observer,'SELECT * FROM biz.heap_marker'),[(1,1)])
    t.reject('writer still cannot read private metadata',writer,'SELECT * FROM lake_metadata.ducklake_snapshot')
    output=t.args.output.parent/'reference-ordinary-dml'
    try:
        reply=reverse_modify_reference(t.observer,'lake_metadata',t.args.manifest,
            t.args.output.parent/'reference-export-current.json',[{'id':i,'val':v} for i,v in expected],output,
            operations_by_commit=[[{'sql':'INSERT INTO {table} VALUES(99,990)'},
                                   {'sql':'UPDATE {table} SET val=11 WHERE id=1'},
                                   {'sql':'DELETE FROM {table} WHERE id=2'}]])
        t.check('actual ordinary-user publication matches complete fixed reference',reply['status'],'pass')
    finally:
        if output.exists(): t.result['artifacts'].extend({'path':str(p)} for p in output.rglob('*') if p.is_file())


def columns(t):
    conn=t.connections['columns']
    t.check('single-column SELECT',query(conn,'SELECT id FROM biz.orders ORDER BY id'),[(1,),(2,),(3,)])
    query(conn,'BEGIN')
    t.check('hidden locator does not force SELECT of all business columns',
        query(conn,'UPDATE biz.orders SET val=15 WHERE id=1 RETURNING id'),[(1,)])
    for name,statement in [('read expression','UPDATE biz.orders SET val=val+1 WHERE id=1'),
                           ('predicate','UPDATE biz.orders SET val=15 WHERE val=10'),
                           ('RETURNING','UPDATE biz.orders SET val=15 WHERE id=1 RETURNING val'),
                           ('whole-row SELECT','SELECT orders FROM biz.orders'),
                           ('unauthorized SELECT','SELECT val FROM biz.orders')]:
        t.reject('column ACL '+name,conn,statement,savepoint=True)
    t.check('column INSERT without unauthorized RETURNING',
        query(conn,'INSERT INTO biz.orders(id,val) VALUES(99,990) RETURNING id'),[(99,)])
    t.reject('column INSERT with unauthorized RETURNING',conn,'INSERT INTO biz.orders(id,val) VALUES(100,1000) RETURNING val',savepoint=True)
    query(conn,'ROLLBACK'); t.identity(conn,'columns'); t.unchanged('column ACL and private write rollback')
    # COUNT(*) has no explicit column reference; use identical heap ACL as the
    # actual host oracle rather than imposing a guessed column-permission rule.
    query(t.admin,'CREATE TABLE biz.column_heap(id integer,val integer)')
    query(t.admin,'INSERT INTO biz.column_heap VALUES(1,10),(2,100),(3,200)')
    query(t.admin,t.sql.SQL('GRANT SELECT(id) ON biz.column_heap TO {}').format(t.sql.Identifier(t.roles.name('columns'))))
    t.check('COUNT(*) matches native heap column-ACL semantics',query(conn,'SELECT count(*) FROM biz.orders'),query(conn,'SELECT count(*) FROM biz.column_heap'))
    # A constant, unqualified UPDATE needs UPDATE(val), not any SELECT grant.
    # The hidden whole-row locator must preserve this native heap ACL contract.
    role=t.sql.Identifier(t.roles.name('columns'))
    query(t.admin,t.sql.SQL('REVOKE SELECT(id),INSERT(id,val) ON biz.orders FROM {}').format(role))
    query(t.admin,t.sql.SQL('REVOKE SELECT(id) ON biz.column_heap FROM {}').format(role))
    query(t.admin,t.sql.SQL('GRANT UPDATE(val) ON biz.column_heap TO {}').format(role))
    for relation in ('biz.orders','biz.column_heap'):
        t.check(relation+' has no table/id/val SELECT grants',query(t.admin,
            "SELECT has_table_privilege(%s,%s,'SELECT'),has_column_privilege(%s,%s,'id','SELECT'),"
            "has_column_privilege(%s,%s,'val','SELECT')",
            (t.roles.name('columns'),relation)*3),[(False,False,False)])
        t.reject('COUNT(*) without any SELECT matches heap '+relation,conn,'SELECT count(*) FROM '+relation,'42501')
    query(conn,'BEGIN')
    affected={}
    for relation in ('biz.column_heap','biz.orders'):
        with conn.cursor() as cursor:
            cursor.execute('UPDATE '+relation+' SET val=777')
            affected[relation]=cursor.rowcount
    t.check('pure UPDATE(val) affected rows match heap',affected,
            {'biz.column_heap':len(t.initial),'biz.orders':len(t.initial)})
    query(conn,'COMMIT'); t.identity(conn,'columns')
    expected=[(row[0],777) for row in t.initial]
    t.check('constant UPDATE without SELECT commits all lake values',t.read(t.observer),expected)
    t.check('same transaction commits matching ordinary heap values',
            query(t.observer,'SELECT id,val FROM biz.column_heap ORDER BY id'),expected)
    t.reject('pure UPDATE grants no subsequent SELECT',conn,'SELECT count(*) FROM biz.orders','42501')
    t.reject('pure UPDATE grants no internal access',conn,'SELECT * FROM lake_metadata.ducklake_snapshot','42501')
    output=t.args.output.parent/'reference-update-without-select'
    try:
        reply=reverse_modify_reference(t.observer,'lake_metadata',t.args.manifest,
            t.args.output.parent/'reference-export-current.json',[{'id':i,'val':v} for i,v in expected],output,
            operations_by_commit=[[{'sql':'UPDATE {table} SET val=777'}]])
        t.check('pure UPDATE column ACL publishes complete reference-compatible protocol',reply['status'],'pass')
    finally:
        if output.exists(): t.result['artifacts'].extend({'path':str(p)} for p in output.rglob('*') if p.is_file())


def views(t):
    owner=t.roles.name('viewowner'); stranger=t.connections['stranger']; reader=t.connections['reader']
    query(t.admin,'CREATE VIEW biz.public_orders AS SELECT id,val FROM biz.orders')
    query(t.admin,t.sql.SQL('ALTER VIEW biz.public_orders OWNER TO {}').format(t.sql.Identifier(owner)))
    query(t.admin,t.sql.SQL('GRANT SELECT ON biz.public_orders TO {}').format(t.sql.Identifier(t.roles.name('stranger'))))
    t.check('ordinary view uses its owner checkAsUser for base ACL',query(stranger,'SELECT id,val FROM biz.public_orders ORDER BY id'),t.initial)
    t.check('user projection remains caller identity',query(stranger,'SELECT DISTINCT current_user FROM biz.public_orders'),[(t.roles.name('stranger'),)])
    t.reject('base ACL does not grant outer view access',reader,'SELECT * FROM biz.public_orders')
    t.reject('view grant does not grant base relation access',stranger,'SELECT * FROM biz.orders')
    query(t.admin,t.sql.SQL('REVOKE SELECT ON biz.orders FROM {}').format(t.sql.Identifier(owner)))
    t.reject('view owner base revocation is rechecked',stranger,'SELECT * FROM biz.public_orders')
    query(t.admin,t.sql.SQL('GRANT SELECT ON biz.orders TO {}').format(t.sql.Identifier(owner)))
    t.check('same connection recovers after restored view ACL',query(stranger,'SELECT id,val FROM biz.public_orders ORDER BY id'),t.initial)
    query(reader,'PREPARE permitted_query AS SELECT id FROM biz.orders ORDER BY id')
    query(t.admin,t.sql.SQL('REVOKE SELECT ON biz.orders FROM {}').format(t.sql.Identifier(t.roles.name('reader'))))
    t.reject('PREPARE does not retain revoked authorization',reader,'EXECUTE permitted_query')
    query(t.admin,t.sql.SQL('GRANT SELECT ON biz.orders TO {}').format(t.sql.Identifier(t.roles.name('reader'))))
    t.check('prepared query recovers with actual restored grant',query(reader,'EXECUTE permitted_query'),[(1,),(2,),(3,)])
    query(reader,'DEALLOCATE permitted_query'); t.identity(stranger,'stranger'); t.unchanged('view and plan ACL checks')


def internal(t):
    writer=t.connections['writer']; stranger=t.connections['stranger']
    objects=['lake_metadata.ducklake_snapshot','lake_metadata.'+t.sql.Identifier(t.carrier).as_string(t.admin),
             'og_lake_internal.catalog_binding','og_lake_internal.table_binding',
             'og_lake_internal.relation_binding','og_lake_internal.column_binding','og_lake_internal.metadata_relation',
             'og_lake_internal.access_policy']
    for relation in objects:
        # Confirm every negative query targets a real object, not a missing-name error.
        t.check('admin confirms protected object '+relation,scalar(t.admin,'SELECT count(*) FROM pg_class WHERE oid=%s::regclass',(relation,)),1)
        t.reject('ordinary direct SELECT '+relation,writer,'SELECT * FROM '+relation)
        t.reject('ordinary direct DELETE '+relation,writer,'DELETE FROM '+relation+' WHERE false')
    t.reject('private marker invocation',writer,'SELECT og_lake_internal.row_locator(1::integer)')
    t.reject('private diagnostics invocation',writer,'SELECT og_lake_internal.test_xact_state()')
    t.reject('ordinary owner registration',writer,'SELECT og_lake.configure_internal_owner(%s::name)',params=(t.roles.name('private'),))
    t.reject('ordinary catalog attach',writer,"SELECT og_lake.attach_catalog('stolen','lake_metadata',NULL::text,false)")
    query(t.admin,t.sql.SQL('GRANT CREATE ON SCHEMA biz TO {}').format(t.sql.Identifier(t.roles.name('stranger'))))
    from inline_fixture import foreign_table_statement
    t.reject('server USAGE cannot bind victim table',stranger,
             foreign_table_statement('biz','stolen','lake_reference',t.table_id,t.manifest['schema']))
    t.check('no unauthorized host binding was created',scalar(t.admin,"SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='biz' AND c.relname='stolen'"),0)
    t.identity(writer,'writer'); t.unchanged('private API and object refusals')



def tamper(t):
    # Only this injection mode uses an administrator session with SET ROLE, so
    # it can observe uncommitted hostile DDL and roll the entire injection back.
    # ACL/read/write positives still use distinct real LOGIN connections.
    role=t.roles.name('reader'); private=t.roles.name('private')
    reader=t.sql.Identifier(role); owner=t.sql.Identifier(private)
    carrier=t.sql.SQL('lake_metadata.{}').format(t.sql.Identifier(t.carrier))
    cases=[
        ('private role LOGIN capability',t.sql.SQL('ALTER ROLE {} LOGIN').format(owner),'42501'),
        ('private role inbound membership',t.sql.SQL('GRANT {} TO {}').format(owner,t.sql.Identifier(t.roles.name('stranger'))),'42501'),
        ('metadata namespace grant',t.sql.SQL('GRANT USAGE ON SCHEMA lake_metadata TO {}').format(reader),'42501'),
        ('metadata table grant',t.sql.SQL('GRANT SELECT ON lake_metadata.ducklake_snapshot TO {}').format(reader),'42501'),
        ('metadata column grant',t.sql.SQL('GRANT SELECT(value) ON lake_metadata.ducklake_metadata TO {}').format(reader),'42501'),
        ('metadata namespace owner replacement',t.sql.SQL('ALTER SCHEMA lake_metadata OWNER TO {}').format(reader),'42501'),
        ('inline owner replacement',t.sql.SQL('ALTER TABLE {} OWNER TO {}').format(carrier,reader),'42501'),
        ('inline INSERT rule',t.sql.SQL('CREATE RULE denied_rule AS ON INSERT TO {} DO INSTEAD NOTHING').format(carrier),'0A000'),
        ('inline default expression',t.sql.SQL('ALTER TABLE {} ALTER COLUMN val SET DEFAULT 77').format(carrier),'0A000'),
        ('inline expression index',t.sql.SQL('CREATE INDEX denied_expression ON {} ((id+0))').format(carrier),'0A000'),
        ('inline partial index',t.sql.SQL('CREATE INDEX denied_partial ON {} (id) WHERE id>0').format(carrier),'0A000'),
        ('inline RLS enable',t.sql.SQL('ALTER TABLE {} ENABLE ROW LEVEL SECURITY').format(carrier),'0A000')]
    for label,injection,state in cases:
        query(t.admin,'BEGIN')
        try:
            # Construction must succeed; a host syntax/DDL rejection is a failed
            # setup, not proof that the module checked the hostile object.
            query(t.admin,injection)
            query(t.admin,t.sql.SQL('SET ROLE {} PASSWORD {}').format(reader,t.sql.Literal(t.roles.passwords['reader'])))
            t.reject(label+' ordinary lake access',t.admin,'SELECT * FROM biz.orders',state,savepoint=True)
            t.check(label+' error restores effective caller',scalar(t.admin,'SELECT current_user'),role)
        finally:
            query(t.admin,'ROLLBACK'); query(t.admin,'RESET ROLE')
        t.unchanged(label+' rollback')
        t.check(label+' original real reader recovers',t.read(t.connections['reader']),t.initial)
    # A real user trigger is injected with a deliberately unmistakable runtime
    # exception; the module must reject its presence before any invocation.
    query(t.admin,'BEGIN')
    try:
        query(t.admin,"CREATE FUNCTION biz.denied_trigger() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'forbidden trigger executed'; RETURN NEW; END $$")
        query(t.admin,t.sql.SQL('CREATE TRIGGER denied_trigger BEFORE INSERT OR UPDATE ON {} FOR EACH ROW EXECUTE PROCEDURE biz.denied_trigger()').format(carrier))
        query(t.admin,t.sql.SQL('SET ROLE {} PASSWORD {}').format(t.sql.Identifier(t.roles.name('writer')),t.sql.Literal(t.roles.passwords['writer'])))
        t.reject('private carrier user trigger cannot reach execution',t.admin,'UPDATE biz.orders SET val=77 WHERE id=1','0A000',savepoint=True)
        t.check('trigger rejection leaves writer context',scalar(t.admin,'SELECT current_user'),t.roles.name('writer'))
    finally:
        query(t.admin,'ROLLBACK'); query(t.admin,'RESET ROLE')
    t.unchanged('trigger injection rollback')



def binding_identity(t):
    # Explicit hostile admin DDL, isolated in a rollback-only transaction.
    # Copying already-real records here is attack construction, never a fixture
    # or a reference-equivalence oracle. Identity must reject the replacement.
    reader=t.roles.name('reader'); private=t.roles.name('private')
    for name in ('ducklake_snapshot',t.carrier):
        old_oid=scalar(t.admin,'SELECT %s::regclass::oid',('lake_metadata.'+name,))
        replacement=t.sql.SQL('lake_metadata.{}').format(t.sql.Identifier(name))
        previous=t.sql.SQL('lake_metadata.{}').format(t.sql.Identifier(name+'_saved'))
        query(t.admin,'BEGIN')
        try:
            query(t.admin,t.sql.SQL('ALTER TABLE {} RENAME TO {}').format(replacement,t.sql.Identifier(name+'_saved')))
            query(t.admin,t.sql.SQL('CREATE TABLE {} (LIKE {} INCLUDING ALL)').format(replacement,previous))
            query(t.admin,t.sql.SQL('INSERT INTO {} SELECT * FROM {}').format(replacement,previous))
            query(t.admin,t.sql.SQL('ALTER TABLE {} OWNER TO {}').format(replacement,t.sql.Identifier(private)))
            t.check('hostile replacement has a distinct OID '+name,
                    scalar(t.admin,'SELECT %s::regclass::oid',('lake_metadata.'+name,))!=old_oid,True)
            query(t.admin,t.sql.SQL('SET ROLE {} PASSWORD {}').format(t.sql.Identifier(reader),t.sql.Literal(t.roles.passwords['reader'])))
            t.reject('same name shape and owner do not trust replacement '+name,t.admin,'SELECT * FROM biz.orders','55000',savepoint=True)
            t.check('replacement rejection restores caller',scalar(t.admin,'SELECT current_user'),reader)
        finally:
            query(t.admin,'ROLLBACK'); query(t.admin,'RESET ROLE')
        t.check('rollback restores exact original relation OID '+name,scalar(t.admin,'SELECT %s::regclass::oid',('lake_metadata.'+name,)),old_oid)
        t.unchanged('replacement '+name+' rollback')
        t.check('actual reader recovers original binding '+name,t.read(t.connections['reader']),t.initial)


def execution_identity(t):
    writer=t.connections['writer']; stranger=t.connections['stranger']
    query(writer,'CREATE TEMP TABLE ducklake_snapshot(snapshot_id bigint)')
    query(writer,'INSERT INTO ducklake_snapshot VALUES(-999)')
    query(writer,'SET search_path=pg_temp,biz,public,pg_catalog')
    t.check('same-name temp metadata never substitutes registered relation',t.read(writer),t.initial)
    query(t.admin,"CREATE FUNCTION biz.caller_identity() RETURNS text LANGUAGE sql SECURITY INVOKER AS $$ SELECT current_user::text $$")
    t.check('user target expression executes as caller',
            query(writer,'SELECT DISTINCT biz.caller_identity() FROM biz.orders'),[(t.roles.name('writer'),)])
    query(writer,'BEGIN')
    rows=query(writer,'UPDATE biz.orders SET val=CASE WHEN biz.caller_identity()=%s THEN val+1 ELSE -999 END WHERE id=1 RETURNING id,val,biz.caller_identity()',
               (t.roles.name('writer'),))
    t.check('user DML expression and RETURNING remain caller',rows,[(1,11,t.roles.name('writer'))])
    query(writer,'ROLLBACK'); t.unchanged('user expressions rollback')
    # Ordinary SECURITY DEFINER follows host effective identity; the extension
    # must restore that identity, not substitute session_user or private owner.
    query(t.admin,"CREATE FUNCTION biz.definer_orders() RETURNS TABLE(id integer,val integer,actor text) LANGUAGE sql SECURITY DEFINER AS $$ SELECT id,val,current_user::text FROM biz.orders ORDER BY id $$")
    query(t.admin,t.sql.SQL('ALTER FUNCTION biz.definer_orders() OWNER TO {}').format(t.sql.Identifier(t.roles.name('viewowner'))))
    query(t.admin,'REVOKE ALL ON FUNCTION biz.definer_orders() FROM PUBLIC')
    query(t.admin,t.sql.SQL('GRANT EXECUTE ON FUNCTION biz.definer_orders() TO {}').format(t.sql.Identifier(t.roles.name('stranger'))))
    t.check('security definer preserves host effective owner semantics',
            query(stranger,'SELECT * FROM biz.definer_orders()'),[(i,v,t.roles.name('viewowner')) for i,v in t.initial])
    t.identity(stranger,'stranger'); t.reject('definer return does not retain elevated identity',stranger,'SELECT * FROM biz.orders')
    t.reject('definer does not grant direct metadata access',stranger,'SELECT * FROM lake_metadata.ducklake_snapshot')
    # The first permission slice explicitly rejects security-barrier plans.
    query(t.admin,'CREATE VIEW biz.barrier_orders WITH (security_barrier=true) AS SELECT * FROM biz.orders')
    query(t.admin,t.sql.SQL('GRANT SELECT ON biz.barrier_orders TO {}').format(t.sql.Identifier(t.roles.name('reader'))))
    t.reject('security barrier unsupported combination is explicit',t.connections['reader'],'SELECT * FROM biz.barrier_orders','0A000')
    t.unchanged('expression and view identity checks')


def role_transition(t):
    writer=t.connections['writer']; reader_name=t.roles.name('reader')
    # Use an explicit membership solely for the business-role transition. The
    # private owner has no member on either side and is never granted.
    query(t.admin,t.sql.SQL('GRANT {} TO {}').format(t.sql.Identifier(reader_name),t.sql.Identifier(t.roles.name('writer'))))
    query(writer,'BEGIN')
    query(writer,'UPDATE biz.orders SET val=55 WHERE id=1')
    query(writer,'INSERT INTO biz.heap_marker VALUES(1,55)')
    query(writer,t.sql.SQL('SET ROLE {} PASSWORD {}').format(t.sql.Identifier(reader_name),t.sql.Literal(t.roles.passwords['reader'])))
    t.check('real writer session adopts business reader role',query(writer,'SELECT session_user,current_user'),[(t.roles.name('writer'),reader_name)])
    t.reject('new command uses reader role privileges',writer,'UPDATE biz.orders SET val=66 WHERE id=2',savepoint=True)
    query(writer,'COMMIT')
    t.check('already authorized DML survives business role change',t.read(t.connections['reader']),[(1,55),(2,100),(3,200)])
    t.check('ordinary heap participates in same role-change commit',query(t.observer,'SELECT * FROM biz.heap_marker'),[(1,55)])
    t.check('COMMIT does not leak private identity',scalar(writer,'SELECT current_user'),reader_name)
    t.reject('post-COMMIT reader cannot access metadata',writer,'SELECT * FROM lake_metadata.ducklake_snapshot')
    query(writer,'RESET ROLE'); t.identity(writer,'writer')
    query(writer,'BEGIN'); query(writer,'SAVEPOINT caller_scope')
    query(writer,t.sql.SQL('SET ROLE {} PASSWORD {}').format(t.sql.Identifier(reader_name),t.sql.Literal(t.roles.passwords['reader'])))
    t.reject('savepoint role uses new statement ACL',writer,'INSERT INTO biz.orders VALUES(88,88)',savepoint=True)
    query(writer,'ROLLBACK TO caller_scope')
    t.identity(writer,'writer'); query(writer,'ROLLBACK'); t.identity(writer,'writer')


def cancel(t):
    writer=t.connections['writer']; holder=t.holder
    if scalar(writer,'SHOW enable_thread_pool')=='on':
        raise Blocked('Exact source-version transaction-lock barrier currently requires dedicated mode')
    publisher=scalar(writer,'SELECT pg_backend_pid()'); blocker=scalar(holder,'SELECT pg_backend_pid()')
    query(writer,'SET statement_timeout=30000'); query(writer,'BEGIN')
    query(writer,'UPDATE biz.orders SET val=777 WHERE id=1')
    query(writer,'INSERT INTO biz.heap_marker VALUES(1,777)')
    query(holder,'BEGIN')
    target=t.sql.SQL('lake_metadata.{}').format(t.sql.Identifier(t.carrier))
    query(holder,t.sql.SQL('UPDATE {} SET _ducklake_end_snapshot=_ducklake_end_snapshot WHERE id=1 AND _ducklake_end_snapshot IS NULL').format(target))
    with ThreadPoolExecutor(max_workers=1) as pool:
        future=pool.submit(query,writer,'COMMIT')
        try:
            deadline=time.monotonic()+12
            while True:
                waits=query(t.observer,"SELECT w.pid,w.transactionid,h.pid FROM pg_locks w JOIN pg_locks h ON h.locktype=w.locktype AND h.transactionid=w.transactionid WHERE w.pid=%s AND h.pid=%s AND w.locktype='transactionid' AND NOT w.granted AND h.granted",(publisher,blocker))
                if waits: break
                if future.done(): raise AssertionError('Publication ended before reaching the controlled SPI source lock')
                if time.monotonic()>=deadline: raise AssertionError('Expected actual publication wait was not reached')
                time.sleep(.02)
            t.result['observed_publication_wait']=waits
            t.check('uncommitted publisher has no visible directory effects',t.image(),t.baseline)
            expected_error='YY003' if t.args.mode=='spi_error' else '57014'
            if t.args.mode!='spi_error':
                t.check('cancel exact owned ordinary backend',scalar(t.observer,'SELECT pg_cancel_backend(%s)',(publisher,)),True)
            try: future.result(timeout=15)
            except Exception as exc: t.check('ordinary publication actual SPI error',getattr(exc,'pgcode',None),expected_error)
            else: t.check('controlled SPI failure must terminate publication','accepted',expected_error)
        finally:
            query(holder,'ROLLBACK')
    query(writer,'ROLLBACK'); t.identity(writer,'writer'); t.unchanged('cancelled ordinary publication')
    t.reject('cancel does not leak internal identity',writer,'SELECT * FROM lake_metadata.ducklake_snapshot')
    query(writer,'UPDATE biz.orders SET val=778 WHERE id=1')
    t.check('same ordinary session subsequently publishes valid data',t.read(t.connections['reader']),[(1,778),(2,100),(3,200)])
    t.identity(writer,'writer')




def spi_error(t):
    # Distinct from pg_cancel_backend: the real lock manager raises YY003 inside
    # SPI's source-version UPDATE. GUCs are scoped to an owned disposable role;
    # they are SUSET, so only the administrator prepares them, before true login.
    role=t.sql.Identifier(t.roles.name('writer'))
    for option in ('lockwait_timeout','update_lockwait_timeout'):
        query(t.admin,t.sql.SQL('ALTER ROLE {} SET {}=2000').format(role,t.sql.Identifier(option)))
    t.connections['writer'].close()
    conn,identity=t.roles.connect(t.db,'writer'); t.connections['writer']=conn
    t.result['real_logins'].append(identity)
    for option in ('lockwait_timeout','update_lockwait_timeout'):
        t.check('owned-role timeout is active for real LOGIN '+option,
                int(scalar(conn,'SELECT setting FROM pg_settings WHERE name=%s',(option,))),2000)
    cancel(t)


def upgrade_scenario(db,args,result,roles):
    """Real existing 0.4 binding -> transactional 0.5 adoption, not fresh-only."""
    from psycopg2 import sql
    from inline_fixture import foreign_table_statement
    import psycopg2
    admin,observer=db.connect(),db.connect()
    def check(name,actual,expected=True):
        ok=actual==expected
        result['assertions'].append({'name':name,'status':'pass' if ok else 'fail','details':{'observed':actual,'expected':expected}})
        if not ok: raise AssertionError(name)
    def reject(conn,name,statement,params=None):
        try: query(conn,statement,params)
        except psycopg2.Error as exc: check(name,exc.pgcode,'42501')
        else: check(name,'accepted','42501')
    query(admin,"CREATE EXTENSION og_lake VERSION '0.4'")
    check('legacy upgrade uses exact reviewed runtime',json.loads(scalar(admin,'SELECT og_lake.host_status()'))['module_version'],reviewed_permission_runtime())
    manifest,exported,counts,artifacts=load_reference(admin,args.manifest,args.run_id,args.output.parent)
    result['artifacts'].extend(artifacts); check('actual upgrade I fixture',manifest['case'],'I')
    query(admin,'CREATE SCHEMA biz')
    query(admin,"SELECT og_lake.attach_catalog('lake_reference','lake_metadata',NULL::text,false)")
    query(admin,foreign_table_statement('biz','orders','lake_reference',manifest['table_id'],manifest['schema']))
    private=roles.create('private',False); reader=roles.create('reader')
    query(admin,sql.SQL('GRANT USAGE ON SCHEMA biz TO {}').format(sql.Identifier(reader)))
    query(admin,sql.SQL('GRANT USAGE ON FOREIGN SERVER lake_reference TO {}').format(sql.Identifier(reader)))
    query(admin,sql.SQL('GRANT SELECT ON biz.orders TO {}').format(sql.Identifier(reader)))
    login,identity=roles.connect(db,'reader'); result.setdefault('real_logins',[]).append(identity)
    expected=[(row['id'],row['val']) for row in manifest['expected']]
    check('original admin binding reads genuine values',query(admin,'SELECT id,val FROM biz.orders ORDER BY id'),expected)
    reject(login,'installed 0.4 still refuses ordinary access','SELECT * FROM biz.orders')
    def image():
        return {table['table_name']:sorted(repr(tuple(row)) for row in query(observer,
            sql.SQL('SELECT * FROM lake_metadata.{}').format(sql.Identifier(table['table_name'])))) for table in exported['tables']}
    def owners():
        return {'namespace':query(observer,"SELECT oid,nspname,nspowner,nspacl::text FROM pg_namespace WHERE nspname IN ('lake_metadata','og_lake_internal') ORDER BY oid"),
                'relation':query(observer,"SELECT c.oid,c.relname,c.relowner,c.relacl::text FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname IN ('lake_metadata','og_lake_internal') ORDER BY c.oid")}
    initial=image(); original_owners=owners()
    query(admin,'BEGIN'); query(admin,"ALTER EXTENSION og_lake UPDATE TO '0.5'")
    check('upgrade visible within owning transaction',scalar(admin,"SELECT extversion FROM pg_extension WHERE extname='og_lake'"),'0.5')
    query(admin,'ROLLBACK')
    check('upgrade rollback restores 0.4',scalar(observer,"SELECT extversion FROM pg_extension WHERE extname='og_lake'"),'0.4')
    check('upgrade rollback restores namespace/relation owners ACLs and OIDs',owners(),original_owners)
    check('upgrade rollback preserves complete standard data',image(),initial)
    query(admin,"ALTER EXTENSION og_lake UPDATE TO '0.5'")
    reject(login,'upgrade without owner configuration remains closed','SELECT * FROM biz.orders')
    unconfigured_owners=owners()
    query(admin,'BEGIN')
    reject(admin,'configure does not silently take metadata ownership',
           'SELECT og_lake.configure_internal_owner(%s::name)',(private,))
    query(admin,'ROLLBACK')
    check('failed unprepared configure leaves empty policy',scalar(observer,'SELECT count(*) FROM og_lake_internal.access_policy'),0)
    check('failed configure preserves ownership',owners(),unconfigured_owners)
    query(admin,'BEGIN')
    try:
        adopt_private_owner(admin,exported,private)
        check('owner handoff is visible in owning transaction',scalar(admin,'SELECT count(*) FROM og_lake_internal.access_policy'),1)
    finally: query(admin,'ROLLBACK')
    check('owner handoff rollback restores complete old ownership',owners(),unconfigured_owners)
    check('owner handoff rollback leaves policy empty',scalar(observer,'SELECT count(*) FROM og_lake_internal.access_policy'),0)
    reject(login,'rolled-back owner adoption did not enable access','SELECT * FROM biz.orders')
    check('owner adoption rollback never edits standard data',image(),initial)
    configure_and_attach(admin,exported,private,manifest['table_id'],existing_binding=True)
    check('existing bound table works for real ordinary login after adoption',query(login,'SELECT id,val FROM biz.orders ORDER BY id'),expected)
    policy=query(observer,'SELECT * FROM og_lake_internal.access_policy'); configured_owners=owners()
    query(admin,'SELECT og_lake.configure_internal_owner(%s::name)',(private,))
    check('same owner configure is idempotent',query(observer,'SELECT * FROM og_lake_internal.access_policy'),policy)
    check('same owner configure preserves ownership',owners(),configured_owners)
    other_private=roles.create('replacement',False)
    try: query(admin,'SELECT og_lake.configure_internal_owner(%s::name)',(other_private,))
    except psycopg2.Error as exc: check('different internal owner OID is refused',exc.pgcode,'55000')
    else: check('different internal owner must not replace registration','accepted','55000')
    check('failed owner replacement preserves registered policy',query(observer,'SELECT * FROM og_lake_internal.access_policy'),policy)
    query(admin,sql.SQL('GRANT UPDATE(val) ON biz.orders TO {}').format(sql.Identifier(reader)))
    check('upgraded binding executes ordinary UPDATE RETURNING',query(login,'UPDATE biz.orders SET val=55 WHERE id=1 RETURNING id,val'),[(1,55)])
    check('upgrade ordinary publication completes',query(observer,'SELECT id,val FROM biz.orders ORDER BY id'),[(1,55),(2,100),(3,200)])
    reject(login,'upgraded session still cannot read standard internals','SELECT * FROM lake_metadata.ducklake_snapshot')
    check('upgrade caller remains its real login',query(login,'SELECT session_user,current_user'),[(reader,reader)])


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--run-id',default=os.getenv('OG_LAKE_RUN_ID'))
    p.add_argument('--output',default=os.getenv('OG_LAKE_RESULT_FILE'))
    p.add_argument('--manifest',default=os.getenv('OG_LAKE_FIXTURE_MANIFEST'))
    p.add_argument('--mode',required=True,choices=['acl','columns','views','internal','tamper','binding_identity','execution_identity','role_transition','cancel','spi_error','upgrade'])
    args=p.parse_args()
    if not all((args.run_id,args.output,args.manifest)): p.error('run-id/output/real manifest required')
    validate_run_id(args.run_id); args.output=Path(args.output).resolve(); args.output.parent.mkdir(parents=True,exist_ok=True)
    result={'status':'fail','scope':'ordinary_permissions/'+args.mode,'assertions':[],'artifacts':[],
            'limitations':['Requires exact reviewed runtime0.5/0.6 and SQL0.5 permission API; no missing functions are registered.',
                           'No RLS support, arbitrary external writer, file/SDK or RAC qualification.']}
    db=None; roles=None; control=None
    try:
        control_file=WORKSPACE/'.worktrees/opengauss-native-lake/contrib/og_lake/og_lake.control'
        declared=re.findall(r"^default_version\s*=\s*'([^']+)'",control_file.read_text(),re.MULTILINE)
        if declared!=[reviewed_permission_runtime()]: raise Blocked('Declared source differs from reviewed permission runtime')
        # A separate administrator control connection outlives OwnedDatabase so
        # precise role cleanup occurs only after the owned database is removed.
        import psycopg2
        control=psycopg2.connect(host=str(WORKSPACE/'.runtime/opengauss-native-lake/debug/socket'),
                                port=55560,dbname='postgres',user=os.getenv('USER','qiumc'),connect_timeout=10)
        control.autocommit=True; query(control,'SET statement_timeout=15000')
        roles=OwnedRoles(control,args.run_id,args.output.parent)
        with OwnedDatabase(args.run_id,args.output.parent) as db:
            if args.mode=='upgrade': upgrade_scenario(db,args,result,roles)
            else:
                case=Case(db,args,result,roles); globals()[args.mode](case)
            result['status']='pass'
    except Blocked as exc: result.update(status='blocked',reason=str(exc))
    except BaseException as exc: result.update(status='fail',reason=str(exc),traceback=traceback.format_exc())
    finally:
        if db is not None:
            result['artifacts'].extend(db.artifacts()); result['database_cleanup']=db.cleanup_result
        if roles is not None and control is not None:
            try:
                # A failed DB cleanup leaves dependencies for an operator; do
                # not use DROP OWNED to remove objects from a surviving DB.
                if db is not None and db.cleanup_result and db.cleanup_result['returncode']==0:
                    result['role_cleanup']=cleanup_roles(control,roles.path,args.run_id)
                elif not roles.items: result['role_cleanup']={'status':'pass','reason':'No roles allocated'}
                else: raise RuntimeError('Owned database cleanup not proven; role cleanup deliberately deferred')
            except BaseException as exc: result.update(status='fail',cleanup_error=str(exc))
            result['artifacts'].append({'path':str(roles.path)})
            result=roles.redact(result)
        if control is not None: control.close()
        write_json(args.output,result)
        print(json.dumps({'status':result['status'],'mode':args.mode,'output':str(args.output)}))
    return {'pass':0,'blocked':2}.get(result['status'],1)


if __name__=='__main__': raise SystemExit(main())
