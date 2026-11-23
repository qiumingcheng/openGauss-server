#!/usr/bin/env python3
"""N6 single-column Native RENAME candidate: stable field IDs and old inline schema.

Uses actual public host DDL, Native DML/flush, immutable-file hashes, and the
locked DuckLake reader. No metadata records or execution results are fabricated.
Preparation alone is not a runtime qualification.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from copy import deepcopy
import fcntl
import json
from pathlib import Path
import shutil
import subprocess
import sys
import time
import traceback
from types import SimpleNamespace

ROOT=Path('/home/qiumc/data/code/cpp');D=ROOT/'integration/opengauss-native-lake'
sys.path[:0]=[str(D),str(D/'tests')]
from file_dml_acceptance import check_build
from inline_fixture import query,scalar
from lake_harness import sha256,validate_run_id,write_json
from native_auto_flush import connect_control
from native_test_context import OwnedDatabase
from native_table_create import Case
from permission_api import OwnedRoles,cleanup_roles
from empty_catalog_oracle import read_native_catalog,transport_native,canonical_shape
from reverse_reference import Reference,qi,literal
from empty_reference_fixture import export_actual_tables
from inline_fixture import replace_catalog_values,foreign_table_statement

MODES=['inline','file','savepoints','cached','concurrent','cancel','guards','permissions','indexes','vector','upgrade','first-carrier']


def sql_version_arg(value):
    aliases={str(v):'0.'+str(v)for v in[18,19,20,21]}
    aliases.update({'0.'+str(v):'0.'+str(v)for v in[18,19,20,21]})
    if value not in aliases:raise argparse.ArgumentTypeError('SQL version must be18/19/20/21 or its0.x spelling')
    return aliases[value]


def parse_args(argv=None):
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--run-id',required=True)
    p.add_argument('--build-receipt',type=Path,required=True);p.add_argument('--mode',required=True,choices=MODES)
    p.add_argument('--sql-version',type=sql_version_arg,choices=['0.18','0.19','0.20','0.21'],default='0.18',
                   help='Target installed SQL (18..21 aliases); upgrade starts at17; default retains17→18')
    return p.parse_args(argv)


def sql_installation(mode,target):
    return '0.17' if mode=='upgrade' else target


def main():
    a=parse_args();validate_run_id(a.run_id)
    out=ROOT/'公共文档/openGauss原生湖存储/验证'/a.run_id;out.mkdir(exist_ok=False)
    r=dict(status='fail',scope=__doc__,mode=a.mode,sql_version=a.sql_version,initial_sql_version=sql_installation(a.mode,a.sql_version),assertions=[],plans=[],limitations=[
        'Only one column rename per transaction, explicitly selected installed SQL'+a.sql_version+', one structurally identical historical inline source.',
        'No ADD/DROP/type/default/constraint evolution, live external catalog writers, RAC or commercial qualification.',
        'Small exact result checks do not establish ANN recall/performance.'])
    files=[Path(__file__),a.build_receipt,D/'tests/native_table_create.py',D/'tests/permission_api.py',D/'native_test_context.py',
           D/'native_runtime.py',D/'tests/inline_cleanup.py',D/'tests/file_dml_acceptance.py',D/'inline_fixture.py',D/'lake_harness.py',
           D/'tests/native_auto_flush.py',D/'tests/empty_catalog_oracle.py',D/'tests/reverse_reference.py',D/'tests/create_reference_oracle.py',
           D/'tests/catalog_fixture_bundle.py',D/'tests/empty_manifest.py',D/'tests/empty_reference_fixture.py',D/'source-lock.json']
    r['inputs']={str(f.resolve()):sha256(f)for f in files};archive=out/'executed-inputs';archive.mkdir()
    for i,f in enumerate(files):shutil.copy2(f,archive/(str(i)+'-'+f.name))
    control=db=roles=t=reservation=pool=future=holder=None;actors=[];waiter_pid=None

    def check(name,got,want=True):
        ok=got==want;r['assertions'].append(deepcopy(dict(name=name,observed=got,expected=want,status='pass'if ok else'fail')))
        if not ok:raise AssertionError(name+': '+repr(got))
    def configure(c):query(c,'SET session_timeout=0');query(c,'SET statement_timeout=120000');query(c,'SET query_dop=1')
    def rejected(c,statement,code,args=None,rollback=True):
        import psycopg2
        try:query(c,statement,args)
        except psycopg2.Error as exc:
            check('rejected '+str(statement)[:160],exc.pgcode,code)
            if rollback:query(c,'ROLLBACK')
        else:raise AssertionError('Unexpectedly accepted '+str(statement))
    def rename(old,new,c=None,table='orders'):
        query(c or t.a,sql.SQL('ALTER FOREIGN TABLE biz.{} RENAME COLUMN {} TO {}').format(
            sql.Identifier(table),sql.Identifier(old),sql.Identifier(new)))
    def values(name='val',c=None):return query(c or t.a,sql.SQL('SELECT id,{} FROM biz.orders ORDER BY id').format(sql.Identifier(name)))
    def columns(c=None):return query(c or t.observer,'SELECT column_id,begin_snapshot,end_snapshot,table_id,column_order,column_name,column_type,initial_default,default_value,nulls_allowed,parent_column,default_value_type,default_value_dialect FROM created_metadata.ducklake_column ORDER BY table_id,column_id,begin_snapshot')
    def file_bytes():return {str(f):sha256(f)for f in Path(t.empty_manifest['data_path']).rglob('*.parquet')}
    def image():
        return dict(head=t.head(),columns=columns(),protocol=t.protocol(),files=file_bytes(),
            mapping=query(t.observer,'SELECT * FROM og_lake_internal.column_binding ORDER BY column_id'),
            carriers=query(t.observer,'SELECT table_id,table_name,schema_version FROM created_metadata.ducklake_inlined_data_tables ORDER BY table_id,schema_version'),
            physical=query(t.observer,'SELECT * FROM created_metadata.'+qi(carrier)+' ORDER BY _ducklake_row_id,_ducklake_begin_snapshot'),
            attrs=query(t.observer,'SELECT attnum,attname::text FROM pg_attribute WHERE attrelid=%s AND attnum>0 ORDER BY attnum',(carrier_oid,)))
    def published(before,old,new):
        after=image();head=after['head'];check('one snapshot/schema, unchanged ID counters',head,(before['head'][0]+1,before['head'][1]+1,before['head'][2],before['head'][3]))
        roots=[row for row in before['columns']if row[5]==old and row[2]is None and row[10]is None]
        check('one exact live root source',len(roots),1);source=roots[0]
        expected=[tuple(row[:2])+(head[0],)+tuple(row[3:])if row==source else row for row in before['columns']]
        added=list(source);added[1]=head[0];added[5]=new;expected.append(tuple(added));expected.sort(key=lambda row:(row[0],row[1]))
        check('complete13-field column version changes only name and validity',after['columns'],expected)
        for key in ['mapping','carriers','physical','attrs','files']:check('rename preserves '+key,after[key],before[key])
        ignore={'ducklake_column','ducklake_schema_versions','ducklake_snapshot','ducklake_snapshot_changes'}
        check('all unrelated standard rows unchanged',{k:v for k,v in after['protocol'].items()if k not in ignore},{k:v for k,v in before['protocol'].items()if k not in ignore})
        check('standard altered_table changeset',query(t.observer,'SELECT changes_made FROM created_metadata.ducklake_snapshot_changes WHERE snapshot_id=%s',(head[0],)),[('altered_table:1',)])
        check('current host name keeps original attnum',query(t.observer,"SELECT attnum FROM pg_attribute WHERE attrelid='biz.orders'::regclass AND attname=%s",(new,)),[(source[4],)])
        check('binding schema version advances without replacing column mapping',scalar(t.observer,'SELECT schema_version FROM og_lake_internal.table_binding WHERE table_id=1'),head[1])
    def wait_locked(pid,oid):
        deadline=time.monotonic()+15
        while time.monotonic()<deadline:
            if future.done():raise AssertionError('DDL completed before observed relation wait: '+repr(future.result()))
            waits=query(t.observer,"SELECT pid,mode FROM pg_locks WHERE locktype='relation' AND relation=%s AND pid=%s AND NOT granted",(oid,pid))
            holders=query(t.observer,"SELECT pid,mode FROM pg_locks WHERE locktype='relation' AND relation=%s AND pid=%s AND granted",(oid,scalar(holder,'SELECT pg_backend_pid()')))
            if waits and holders:r['ddl_wait']=dict(waiter=waits,holder=holders);return
            time.sleep(.01)
        raise AssertionError('No exact host relation lock wait observed')
    def commit_rename():
        import psycopg2
        try:rename('val','amount',t.b);return 'committed'
        except psycopg2.Error as exc:query(t.b,'ROLLBACK');return exc.pgcode

    try:
        from psycopg2 import sql
        b=check_build(a.build_receipt);r.update(source_head=b['source_head'],binary_sha256=b['binary_sha256'],build_sha256=sha256(a.build_receipt))
        control=connect_control();configure(control)
        check('automatic scheduler stays disabled',scalar(control,'SHOW enable_lake_maintenance'),'off')
        with(out/'fixture.log').open('w')as log:subprocess.run([sys.executable,str(D/'tests/catalog_fixture_bundle.py'),'--run-id',a.run_id,'--output',str(out/'fixture/manifest.json')],stdout=log,stderr=subprocess.STDOUT,check=True,timeout=300)
        reservation=(ROOT/'.reference/opengauss-native-lake/.driver.lock').open('a+');fcntl.flock(reservation.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB)
        db=OwnedDatabase(a.run_id,out);db.create();configure(db.admin);roles=OwnedRoles(control,a.run_id,out)
        t=Case(db,SimpleNamespace(mode='net_dml',sql_version=sql_installation(a.mode,a.sql_version),manifest=out/'fixture/manifest.json',output=out/'result.json',run_id=a.run_id),r,roles)
        for c in [t.a,t.b,t.observer]:configure(c)
        check('selected initial installed SQL version',scalar(t.a,"SELECT extversion FROM pg_extension WHERE extname='og_lake'"),sql_installation(a.mode,a.sql_version))
        compiled_module=json.loads(scalar(t.a,'SELECT og_lake.host_status()'))['module_version']
        r['compiled_module_version']=compiled_module
        original=[(1,10),(2,20),(3,30)]
        if a.mode=='first-carrier':
            # Genuine fixed DuckLake writer creates an immutable file without
            # any inline registration. Only DDL description spellings are
            # adapted after exact shape comparison; real catalog rows/IDs are
            # replayed unchanged into this exclusively owned EMPTY catalog.
            producer=out/'file-only-producer';producer.mkdir()
            lock=json.loads((D/'source-lock.json').read_text())
            receipt=json.loads((ROOT/'.reference/opengauss-native-lake/build/receipt.json').read_text())
            check('file-only producer uses exact locked source',receipt['source_lock_hash'],sha256(D/'source-lock.json'))
            ref=Reference(producer,lock,receipt);empty=t.export('file-only-native-empty.json')
            catalog=producer/'source.ducklake';transport_native(ref,catalog,empty)
            attach='ATTACH '+literal('ducklake:'+str(catalog))+" AS lake (DUCKLAKE_VERSION '1.1',AUTOMATIC_MIGRATION FALSE,DATA_INLINING_ROW_LIMIT 0);\n"
            ref.run(attach+'CREATE TABLE lake.main.orders(id INTEGER,val INTEGER); INSERT INTO lake.main.orders VALUES(1,10),(2,20),(3,30);','actual-file-only-orders')
            actual={'tables':export_actual_tables(ref,catalog)}
            previous={x['table_name']:x for x in empty['tables']};adapted=[]
            check('file-only actual catalog retains complete standard table set',sorted(x['table_name']for x in actual['tables']),sorted(previous))
            for table in actual['tables']:
                old=previous[table['table_name']]
                check('file-only real physical shape/'+table['table_name'],canonical_shape(table),canonical_shape(old))
                adapted.append(dict(table,columns=deepcopy(old['columns']),constraints=deepcopy(old['constraints'])))
            check('DDL adaptation preserves every real catalog row',[(x['table_name'],x['rows'])for x in adapted],[(x['table_name'],x['rows'])for x in actual['tables']])
            actual_tables=next(x['rows']for x in actual['tables']if x['table_name']=='ducklake_table')
            table_id=next(x['table_id']for x in actual_tables if x['table_name']=='orders'and x['end_snapshot']is None)
            check('actual file-only table has no carrier registration',next(x['rows']for x in actual['tables']if x['table_name']=='ducklake_inlined_data_tables'),[])
            query(t.a,'BEGIN');replace_catalog_values(t.a,t.schema,empty,{'tables':adapted});query(t.a,'COMMIT')
            query(t.a,foreign_table_statement('biz','orders','created_lake',table_id,[{'name':'id','type':'INTEGER'},{'name':'val','type':'INTEGER'}]))
            check('attached file-only source starts with exact business rows',values(),original)
            check('native loader also sees no carrier for A',query(t.observer,'SELECT table_name FROM created_metadata.ducklake_inlined_data_tables WHERE table_id=%s',(table_id,)),[])
        else:
            t.create();query(t.a,'INSERT INTO biz.orders VALUES(1,10),(2,20),(3,30)')
        start=t.head()[0];carrier=None if a.mode=='first-carrier'else scalar(t.observer,'SELECT table_name FROM created_metadata.ducklake_inlined_data_tables WHERE table_id=1')
        carrier_oid=None if carrier is None else scalar(t.observer,'SELECT c.oid FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=%s AND c.relname=%s',('created_metadata',carrier))
        host_oid=scalar(t.observer,"SELECT 'biz.orders'::regclass::oid");current='val'
        if a.mode=='file':query(t.a,"SELECT og_lake.flush('biz.orders'::regclass)");check('actual file source exists',scalar(t.observer,'SELECT count(*) FROM created_metadata.ducklake_data_file')>0)
        before=None if a.mode=='first-carrier'else image()
        if a.mode in ['inline','file','upgrade']:
            if a.mode=='upgrade':
                rejected(t.a,'ALTER FOREIGN TABLE biz.orders RENAME COLUMN val TO amount','0A000');check('retained SQL17 rejects without changes',image(),before)
                query(t.a,sql.SQL('ALTER EXTENSION og_lake UPDATE TO {}').format(sql.Literal(a.sql_version)))
                check('SQL migration reaches explicitly selected version',scalar(t.a,"SELECT extversion FROM pg_extension WHERE extname='og_lake'"),a.sql_version)
                check('SQL migration keeps the same compiled module',json.loads(scalar(t.a,'SELECT og_lake.host_status()'))['module_version'],compiled_module)
                check('SQL migration changes no standard data',image(),before)
            rename('val','amount');current='amount';published(before,'val',current);check('read renamed business rows',values(current),original)
            query(t.a,'UPDATE biz.orders SET amount=amount+1 WHERE id=1');query(t.a,'UPDATE biz.orders SET amount=amount+1 WHERE id=1')
            query(t.a,'DELETE FROM biz.orders WHERE id=2');query(t.a,'INSERT INTO biz.orders(id,amount)VALUES(4,40)')
            query(t.a,"SELECT og_lake.flush('biz.orders'::regclass)");check('two UPDATEs/delete/insert and flush through physical old name',values(current),[(1,12),(3,30),(4,40)])
            before=image();rename('amount','total');current='total';published(before,'amount',current)
            query(t.a,'UPDATE biz.orders SET total=total+1 WHERE id=3');check('second committed rename still uses first carrier schema',values(current),[(1,12),(3,31),(4,40)])
        elif a.mode=='first-carrier':
            def a_state():
                return dict(head=t.head(),standard=t.protocol(),columns=columns(),files=file_bytes(),
                    registrations=query(t.observer,'SELECT table_id,table_name,schema_version FROM created_metadata.ducklake_inlined_data_tables ORDER BY table_id,schema_version'),
                    heap=query(t.observer,'SELECT * FROM biz.heap_marker ORDER BY id'))
            field_ids=query(t.observer,'SELECT column_id,column_order,column_type FROM created_metadata.ducklake_column WHERE table_id=%s AND end_snapshot IS NULL ORDER BY column_order',(table_id,))
            files=file_bytes();next_row_id=scalar(t.observer,'SELECT next_row_id FROM created_metadata.ducklake_table_stats WHERE table_id=%s',(table_id,))
            rename('val','amount');current='amount'
            check('file-only rename preserves stable column identities',query(t.observer,'SELECT column_id,column_order,column_type FROM created_metadata.ducklake_column WHERE table_id=%s AND end_snapshot IS NULL ORDER BY column_order',(table_id,)),field_ids)
            check('file-only rename rewrites no Parquet bytes',file_bytes(),files)
            versions=query(t.observer,'SELECT schema_version,begin_snapshot FROM created_metadata.ducklake_schema_versions WHERE table_id=%s AND begin_snapshot<=%s ORDER BY begin_snapshot',(table_id,t.head()[0]))
            check('renamed A has a visible table-specific schema record',len(versions)>0)
            schema_a=max(x[0]for x in versions);r['file_only_a_visible_schema_versions']=versions
            t.create('unrelated')
            check('unrelated B DDL advances global schema beyond A',t.head()[1]>schema_a)
            check('B DDL leaves A table-specific schema unchanged',scalar(t.observer,'SELECT max(schema_version) FROM created_metadata.ducklake_schema_versions WHERE table_id=%s',(table_id,)),schema_a)
            check('A still has no carrier before first UPDATE',query(t.observer,'SELECT table_name FROM created_metadata.ducklake_inlined_data_tables WHERE table_id=%s',(table_id,)),[])
            query(t.a,"SELECT og_lake.set_option('biz.orders'::regclass,'data_inlining_row_limit','100')")
            before_first=a_state();query(t.a,'BEGIN');query(t.a,'SAVEPOINT first_carrier')
            query(t.a,'INSERT INTO biz.heap_marker VALUES(101)');query(t.a,'UPDATE biz.orders SET amount=amount+1 WHERE id=1')
            check('own pending first-carrier update is visible',values(current),[(1,11),(2,20),(3,30)])
            query(t.a,'ROLLBACK TO first_carrier');query(t.a,'RELEASE first_carrier');query(t.a,'COMMIT')
            check('savepoint rollback of first-carrier update is completely atomic',a_state(),before_first)
            query(t.a,'BEGIN');query(t.a,'INSERT INTO biz.heap_marker VALUES(102)')
            query(t.a,'UPDATE biz.orders SET amount=amount+1 WHERE id=1');query(t.a,'COMMIT')
            registration=query(t.observer,'SELECT table_name,schema_version FROM created_metadata.ducklake_inlined_data_tables WHERE table_id=%s',(table_id,))
            check('first UPDATE allocates one genuine A carrier',len(registration),1)
            carrier,registered_version=registration[0]
            check('first carrier uses A own schema version, not global B version',registered_version,schema_a)
            check('A carrier version differs from advanced global frontier',registered_version<t.head()[1])
            carrier_oid=scalar(t.observer,'SELECT c.oid FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=%s AND c.relname=%s',(t.schema,carrier))
            attrs=query(t.observer,'SELECT attname::text FROM pg_attribute WHERE attrelid=%s AND attnum>0 AND NOT attisdropped ORDER BY attnum',(carrier_oid,))
            check('first carrier uses renamed physical business fields',attrs[-2:],[('id',),('amount',)])
            fresh=db.connect();actors.append(fresh);configure(fresh)
            check('fresh session resolves persisted first-carrier schema',values(current,fresh),[(1,11),(2,20),(3,30)])
            row_ids=query(t.observer,'SELECT DISTINCT _ducklake_row_id FROM created_metadata.'+qi(carrier)+' WHERE id=1 ORDER BY _ducklake_row_id')
            check('one stable R in first after-image',len(row_ids),1)
            query(fresh,'UPDATE biz.orders SET amount=amount+1 WHERE id=1')
            check('second same-R update survives reconnect',values(current,fresh),[(1,12),(2,20),(3,30)])
            check('same-R update keeps physical logical row ID',query(t.observer,'SELECT DISTINCT _ducklake_row_id FROM created_metadata.'+qi(carrier)+' WHERE id=1 ORDER BY _ducklake_row_id'),row_ids)
            check('two updates allocate no new logical row IDs',scalar(t.observer,'SELECT next_row_id FROM created_metadata.ducklake_table_stats WHERE table_id=%s',(table_id,)),next_row_id)
            query(fresh,"SELECT og_lake.flush('biz.orders'::regclass)")
            check('flush after cross-table schema drift preserves renamed rows',values(current,fresh),[(1,12),(2,20),(3,30)])
            check('fresh original session also reloads flushed schema',values(current),[(1,12),(2,20),(3,30)])
        elif a.mode=='savepoints':
            query(t.a,'BEGIN');check('closed read creates old readonly Root',values(),original);query(t.a,'SAVEPOINT child')
            rename('val','amount');check('own read sees name before publication',values('amount'),original)
            query(t.a,'ROLLBACK TO child');query(t.a,'RELEASE child');check('rollback reconstructs old descriptor after child view closes',values(),original)
            query(t.a,'COMMIT');check('savepoint rollback restores standard metadata',image(),before)
            query(t.a,'BEGIN');query(t.a,'SAVEPOINT child');rename('val','amount');check('child renamed reader',values('amount'),original)
            query(t.a,'RELEASE child');check('promoted reader uses owned descriptor',values('amount'),original);query(t.a,'COMMIT');current='amount';published(before,'val',current)
        elif a.mode=='cached':
            query(t.a,'CREATE TABLE biz.exact(id integer,val integer)');query(t.a,'INSERT INTO biz.exact SELECT * FROM biz.orders')
            for tag,table in [('lake','orders'),('heap','exact')]:
                query(t.a,'PREPARE '+tag+'_read AS SELECT id,val FROM biz.'+table+' ORDER BY id')
                query(t.a,'PREPARE '+tag+'_write AS UPDATE biz.'+table+' SET val=val+1 WHERE id=1 RETURNING id,val')
                check(tag+' prepared old read',query(t.a,'EXECUTE '+tag+'_read'),original)
            rename('val','amount');query(t.a,'ALTER TABLE biz.exact RENAME COLUMN val TO amount');current='amount';published(before,'val',current)
            def outcome(statement):
                import psycopg2
                try:return dict(rows=query(t.a,statement))
                except psycopg2.Error as exc:query(t.a,'ROLLBACK');return dict(sqlstate=exc.pgcode)
            for kind in ['read','write']:check('prepared invalidation follows ordinary heap '+kind,outcome('EXECUTE lake_'+kind),outcome('EXECUTE heap_'+kind))
            check('fresh names resolve after plan invalidation',values(current),query(t.a,'SELECT id,amount FROM biz.exact ORDER BY id'))
        elif a.mode in ['concurrent','cancel']:
            holder=t.a;query(holder,'BEGIN');query(holder,'UPDATE biz.orders SET val=val+1 WHERE id=1')
            waiter_pid=scalar(t.b,'SELECT pg_backend_pid()');pool=ThreadPoolExecutor(max_workers=1);future=pool.submit(commit_rename);wait_locked(waiter_pid,host_oid)
            if a.mode=='cancel':
                check('cancel exact waiting rename',scalar(t.observer,'SELECT pg_cancel_backend(%s)',(waiter_pid,)))
                check('cancelled public DDL result',future.result(timeout=20),'57014');query(holder,'ROLLBACK');holder=None
                check('cancel restores both lake metadata and data',image(),before);rename('val','amount');current='amount';published(before,'val',current)
            else:
                query(holder,'COMMIT');holder=None;check('waiting DDL then commits',future.result(timeout=20),'committed')
                current='amount';check('DDL sees committed same-row update',values(current),[(1,11),(2,20),(3,30)])
                check('rename preserves writer row IDs and columns',columns(),[tuple(x[:2])+(t.head()[0],)+tuple(x[3:])if x[5]=='val'and x[2]is None else x for x in before['columns']]+[(2,t.head()[0],None,1,2,'amount','int32',None,'NULL',True,None,'literal','duckdb')])
            pool.shutdown(wait=True);pool=None;future=None
        elif a.mode=='guards':
            for statement,code in [('ALTER FOREIGN TABLE biz.orders RENAME COLUMN val TO id','42701'),('ALTER FOREIGN TABLE biz.orders RENAME COLUMN val TO "ID"','42701'),('ALTER FOREIGN TABLE biz.orders RENAME COLUMN val TO _ducklake_hidden','0A000'),('ALTER FOREIGN TABLE biz.orders ADD COLUMN extra integer','0A000'),('ALTER FOREIGN TABLE biz.orders ALTER COLUMN val TYPE bigint','0A000')]:
                rejected(t.a,statement,code);check('unsupported command leaves exact protocol',image(),before)
            query(t.a,'BEGIN READ ONLY');rejected(t.a,'ALTER FOREIGN TABLE biz.orders RENAME COLUMN val TO amount','25006')
            for first,second in [('UPDATE biz.orders SET val=11 WHERE id=1','ALTER FOREIGN TABLE biz.orders RENAME COLUMN val TO amount'),('ALTER FOREIGN TABLE biz.orders RENAME COLUMN val TO amount','UPDATE biz.orders SET amount=11 WHERE id=1'),('ALTER FOREIGN TABLE biz.orders RENAME COLUMN val TO amount','ALTER FOREIGN TABLE biz.orders RENAME COLUMN id TO id_new'),('ALTER FOREIGN TABLE biz.orders RENAME COLUMN val TO amount','ALTER FOREIGN TABLE biz.orders RENAME TO renamed')]:
                query(t.a,'BEGIN');query(t.a,first);rejected(t.a,second,'0A000');check('mixed operation rolls back all metadata',image(),before)
            query(t.a,'BEGIN');query(t.a,'DECLARE held NO SCROLL CURSOR FOR SELECT * FROM biz.orders');query(t.a,'FETCH 1 FROM held');query(t.a,'SAVEPOINT child')
            rejected(t.a,'ALTER FOREIGN TABLE biz.orders RENAME COLUMN val TO amount','55006',rollback=False);query(t.a,'ROLLBACK TO child');query(t.a,'CLOSE held');query(t.a,'RELEASE child')
            rename('val','amount');query(t.a,'COMMIT');current='amount';published(before,'val',current)
        elif a.mode=='permissions':
            owner=roles.create('business');reader=roles.create('reader')
            for role in [owner,reader]:query(t.a,sql.SQL('GRANT USAGE ON SCHEMA biz TO {}').format(sql.Identifier(role)))
            query(t.a,sql.SQL('GRANT CREATE ON SCHEMA biz TO {}').format(sql.Identifier(owner)));query(t.a,sql.SQL('GRANT USAGE ON FOREIGN SERVER created_lake TO {}').format(sql.Identifier(owner)))
            ordinary,_=roles.connect(db,'business');unprivileged,_=roles.connect(db,'reader');actors.extend([ordinary,unprivileged]);configure(ordinary);configure(unprivileged)
            t.create('owned',conn=ordinary);query(ordinary,'INSERT INTO biz.owned VALUES(8,80)');query(ordinary,sql.SQL('GRANT SELECT ON biz.owned TO {}').format(sql.Identifier(reader)))
            rejected(unprivileged,'ALTER FOREIGN TABLE biz.owned RENAME COLUMN val TO amount','42501');rejected(ordinary,'ALTER FOREIGN TABLE biz.orders RENAME COLUMN val TO amount','42501')
            rename('val','amount',ordinary,table='owned');check('owner rename preserves SELECT grant',query(unprivileged,'SELECT id,amount FROM biz.owned'),[(8,80)])
            rejected(ordinary,'SELECT * FROM created_metadata.ducklake_column','42501');rename('val','amount');current='amount'
        elif a.mode=='indexes':
            spec=dict(version=1,kind='scalar',method='btree',column='val');uuid=str(scalar(t.a,"SELECT og_lake.create_lake_index('biz.orders'::regclass,'ix_val',%s::jsonb)",(json.dumps(spec),)))
            query(t.a,'SELECT og_lake.build_lake_index(%s::uuid)',(uuid,));definition=query(t.observer,'SELECT index_uuid::text,column_id,definition_hash,definition::text,definition_generation FROM og_lake_internal.index_definition')
            before=image();rename('val','amount');current='amount';published(before,'val',current)
            check('index identity and definition stay bound to field ID',query(t.observer,'SELECT index_uuid::text,column_id,definition_hash,definition::text,definition_generation FROM og_lake_internal.index_definition'),definition)
            query(t.a,'SET enable_seqscan=off');statement='SELECT id,amount FROM biz.orders WHERE amount=20';plan='\n'.join(row[0]for row in query(t.a,'EXPLAIN ANALYZE '+statement));r['plans'].append(plan)
            check('actual inline IndexAM follows old physical name','Actual Inline Access: native B-tree'in plan);check('indexed renamed column exact values',query(t.a,statement),[(2,20)])
            late=str(scalar(t.a,"SELECT og_lake.create_lake_index('biz.orders'::regclass,'ix_after',%s::jsonb)",(json.dumps({**spec,'column':'amount'}),)));query(t.a,'SELECT og_lake.build_lake_index(%s::uuid)',(late,))
            query(t.a,"SELECT og_lake.flush('biz.orders'::regclass)");query(t.a,'UPDATE biz.orders SET amount=amount+1 WHERE id=2');check('index plus file/inline updates do not misbind renamed field',values(current),[(1,10),(2,21),(3,30)])
        elif a.mode=='vector':
            query(t.a,"CREATE FOREIGN TABLE biz.vectors(id integer,v vector(3),note text) SERVER created_lake OPTIONS(binding_mode 'create',lake_schema 'main')")
            query(t.a,"INSERT INTO biz.vectors SELECT i,('['||i::text||','||(i*i/100.0)::text||','||(i%13+.125)::text||']')::vector,'n/'||i::text FROM generate_series(1,64)i")
            spec=dict(version=1,kind='vector',method='diskann',column='v',metric='l2',enable_pq=False);uuid=str(scalar(t.a,"SELECT og_lake.create_lake_index('biz.vectors'::regclass,'ix_v',%s::jsonb)",(json.dumps(spec),)));query(t.a,'SELECT og_lake.build_lake_index(%s::uuid)',(uuid,))
            defs=query(t.observer,'SELECT index_uuid::text,column_id,definition_hash,definition::text,definition_generation FROM og_lake_internal.index_definition')
            cols=columns();exact=query(t.a,"SELECT id,v::text FROM biz.vectors ORDER BY id");rename('v','embedding',table='vectors')
            check('vector parent and LIST child IDs preserved',[(x[0],x[6],x[10])for x in columns()if x[2]is None],[(x[0],x[6],x[10])for x in cols if x[2]is None])
            check('renamed vector values use old inline TEXT physical column',query(t.a,'SELECT id,embedding::text FROM biz.vectors ORDER BY id'),exact)
            check('vector normalized definition generation stays bound to ID',query(t.observer,'SELECT index_uuid::text,column_id,definition_hash,definition::text,definition_generation FROM og_lake_internal.index_definition'),defs)
            query(t.a,'SET enable_seqscan=off');q="'[4.1,3.7,2.4]'::vector";statement='SELECT id FROM biz.vectors ORDER BY embedding <-> '+q+' LIMIT 10';plan='\n'.join(row[0]for row in query(t.a,'EXPLAIN ANALYZE '+statement));r['plans'].append(plan)
            check('actual DiskANN follows renamed field','Actual Inline Access: native DiskANN'in plan)
            actual=query(t.a,statement);query(t.a,'SET enable_seqscan=on');query(t.a,'CREATE TABLE biz.vector_exact AS SELECT * FROM biz.vectors');check('small renamed ANN result equals independent ordinary heap distance',actual,query(t.a,'SELECT id FROM biz.vector_exact ORDER BY embedding <-> '+q+' LIMIT 10'))
            query(t.a,"SELECT og_lake.flush('biz.vectors'::regclass)");check('LIST field-ID flush survives rename',query(t.a,'SELECT id,embedding::text FROM biz.vectors ORDER BY id'),exact)
        check('final installed SQL version matches explicit selection',scalar(t.a,"SELECT extversion FROM pg_extension WHERE extname='og_lake'"),a.sql_version)
        check('selected SQL mode does not replace compiled module',json.loads(scalar(t.a,'SELECT og_lake.host_status()'))['module_version'],compiled_module)
        # Independent standard reader always sees original table history; current
        # logical name may differ from the physical inline registration schema.
        names=query(t.observer,"SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=%s AND c.relkind='r' ORDER BY c.relname",(t.schema,))
        native=read_native_catalog(t.observer,t.schema,{'tables':[{'table_name':n}for n,in names]},out/'native-export.json')
        lock=json.loads((D/'source-lock.json').read_text());receipt=json.loads((ROOT/'.reference/opengauss-native-lake/build/receipt.json').read_text())
        check('reference uses exact locked source',receipt['source_lock_hash'],sha256(D/'source-lock.json'))
        ref=Reference(out,lock,receipt);transport=out/'transport.ducklake';transport_native(ref,transport,native)
        found=ref.run(ref.attach(transport,True)+'SELECT id,'+qi(current)+' AS val FROM lake.main.orders ORDER BY id','current',result=True)
        check('DuckLake reads name-only old-schema inline projection',[(row['id'],row['val'])for row in found],values(current))
        found=ref.run(ref.attach(transport,True)+'SELECT id,val FROM lake.main.orders AT(VERSION => '+str(start)+') ORDER BY id','original-history',result=True)
        check('DuckLake time travel retains old column name and values',[(row['id'],row['val'])for row in found],original)
        if a.mode in ['inline','file']:
            ref.run(ref.attach(transport)+'INSERT INTO lake.main.orders VALUES(5,50)','reference-new-inline-schema')
            ref.run(ref.attach(transport)+"CALL ducklake_flush_inlined_data('lake')",'reference-flush-both-schemas')
            found=ref.run(ref.attach(transport,True)+'SELECT id,'+qi(current)+' AS val FROM lake.main.orders ORDER BY id','reference-after-flush',result=True)
            check('standard client writes and flushes both historical/current inline schemas',[(row['id'],row['val'])for row in found],values(current)+[(5,50)])
        check('all executed input bytes unchanged',all(sha256(f)==h for f,h in r['inputs'].items()));check_build(a.build_receipt);r['status']='pass'
    except BaseException as exc:r.update(reason=str(exc),traceback=traceback.format_exc())
    finally:
        # Release a non-busy holder before draining a blocked DDL. Cancellation
        # failure never makes cleanup wait on its own still-open holder.
        if holder is not None:
            try:query(holder,'ROLLBACK')
            except BaseException:pass
        if future is not None and not future.done()and t is not None:
            try:scalar(t.observer,'SELECT pg_cancel_backend(%s)',(waiter_pid,))
            except BaseException:pass
            try:future.result(timeout=20)
            except BaseException:pass
        if pool is not None:pool.shutdown(wait=False)
        for actor in actors:
            try:actor.close()
            except BaseException:pass
        if db is not None:
            try:db.cleanup();r['database_cleanup']=db.cleanup_result
            except BaseException as exc:r.update(status='fail',cleanup_error=str(exc))
        if roles is not None:
            try:
                if not r.get('database_cleanup')or r['database_cleanup']['returncode']:raise RuntimeError('Exact owned database cleanup unproven; roles retained')
                r['role_cleanup']=cleanup_roles(control,roles.path,a.run_id);r=roles.redact(r)
            except BaseException as exc:r.update(status='fail',role_cleanup_error=str(exc))
        if control is not None:control.close()
        if reservation is not None:reservation.close()
        write_json(out/'result.json',r)
    print(json.dumps(dict(status=r['status'],mode=a.mode,checks=len(r['assertions']),reason=r.get('reason'),result=str(out/'result.json')),ensure_ascii=False))
    return 0 if r['status']=='pass'else 1


if __name__=='__main__':raise SystemExit(main())
