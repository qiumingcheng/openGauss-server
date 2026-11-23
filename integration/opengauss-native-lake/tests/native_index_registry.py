#!/usr/bin/env python3
"""Real private index-definition catalog lifecycle; no built index is implied."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
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
from empty_catalog_oracle import read_native_catalog,transport_native
from reverse_reference import Reference

MODES=['definition','transactions','names','permissions','unbind','upgrade','guards','tamper','concurrency']
SPEC={'version':1,'kind':'scalar','method':'btree','column':'id'}


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--run-id',required=True)
    p.add_argument('--build-receipt',type=Path,required=True);p.add_argument('--mode',choices=MODES,required=True)
    a=p.parse_args();validate_run_id(a.run_id)
    out=ROOT/'公共文档/openGauss原生湖存储/验证'/a.run_id;out.mkdir(exist_ok=False)
    r={'status':'fail','scope':'Actual native SQL0.14 index-definition registry '+a.mode,'assertions':[],
       'limitations':['Metadata registry only: no segment builder, READY backend or indexed query path. No index-performance, remote/RAC or commercial qualification.']}
    files=[Path(__file__),D/'tests/native_table_create.py',D/'tests/permission_api.py',D/'native_test_context.py',D/'native_runtime.py',
           D/'tests/file_dml_acceptance.py',D/'tests/inline_cleanup.py',D/'tests/native_auto_flush.py',D/'inline_fixture.py',
           D/'tests/empty_catalog_oracle.py',D/'tests/reverse_reference.py',D/'source-lock.json']
    r['inputs']={str(f):sha256(f)for f in files};archive=out/'executed-inputs';archive.mkdir()
    for f in files:shutil.copy2(f,archive/f.name)
    db=roles=control=t=pool=future=None;pid=None

    def check(name,got,want=True):
        ok=got==want;r['assertions'].append({'name':name,'observed':got,'expected':want,'status':'pass'if ok else'fail'})
        if not ok:raise AssertionError(name)

    def rejected(conn,statement,code,args=None):
        import psycopg2
        try:query(conn,statement,args)
        except psycopg2.Error as exc:
            check('rejected '+str(statement)[:130],exc.pgcode,code);query(conn,'ROLLBACK')
        else:raise AssertionError('Unexpectedly accepted '+str(statement))

    try:
        import psycopg2
        from psycopg2 import sql
        built=check_build(a.build_receipt);r.update(source_head=built['source_head'],binary_sha256=built['binary_sha256'])
        control=connect_control();check('automatic scheduler stays disabled',scalar(control,'SHOW enable_lake_maintenance'),'off')
        with (out/'fixture.log').open('w')as log:
            subprocess.run([sys.executable,str(D/'tests/catalog_fixture_bundle.py'),'--run-id',a.run_id,
                            '--output',str(out/'fixture/manifest.json')],stdout=log,stderr=subprocess.STDOUT,check=True,timeout=300)
        db=OwnedDatabase(a.run_id,out).create();roles=OwnedRoles(control,a.run_id,out)
        t=Case(db,SimpleNamespace(mode='net_dml',sql_version='0.13'if a.mode=='upgrade'else'0.14',
            manifest=out/'fixture/manifest.json',output=out/'result.json',run_id=a.run_id),r,roles)
        t.create();query(t.a,'INSERT INTO biz.orders VALUES(1,10),(2,20),(3,30)')
        expected=[(1,10),(2,20),(3,30)];protocol=t.protocol();head=t.head()

        def create(name='ix_id',relation='biz.orders',spec=None,conn=None):
            return str(scalar(conn or t.a,'SELECT og_lake.create_lake_index(%s::regclass,%s::name,%s::jsonb)',
                              (relation,name,json.dumps(SPEC if spec is None else spec))))

        def status(uuid,conn=None):return json.loads(scalar(conn or t.a,'SELECT og_lake.lake_index_status(%s::uuid)',(uuid,)))

        def drop(uuid,conn=None):return query(conn or t.a,'SELECT og_lake.drop_lake_index(%s::uuid)',(uuid,))

        def counts(conn=None):
            return tuple(scalar(conn or t.observer,'SELECT count(*) FROM og_lake_internal.'+name)
                         for name in ['index_definition','index_segment','index_coverage'])

        def check_status(uuid,state='ACTIVE',conn=None):
            check('definition status is explicit about missing backend',status(uuid,conn),
                  {'index_uuid':uuid,'state':state,'ready_segments':0,'usable':False,'reason':'index_backend_not_implemented'})

        def check_owners():
            check('new catalogs share the registered private owner',query(t.observer,
                "SELECT c.relname,c.relowner=(SELECT internal_owner_oid FROM og_lake_internal.access_policy) "
                "FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='og_lake_internal' "
                "AND c.relname IN ('index_definition','index_segment','index_coverage') ORDER BY c.relname"),
                [('index_coverage',True),('index_definition',True),('index_segment',True)])

        if a.mode=='upgrade':
            check('SQL0.13 has no index API',scalar(t.a,"SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='og_lake' AND p.proname='create_lake_index'"),0)
            query(t.a,"ALTER EXTENSION og_lake UPDATE TO '0.14'")
            check('upgrade preserves all standard metadata and data',t.protocol(),protocol)
            check('upgrade creates no business snapshots or IDs',t.head(),head)
        check_owners();check('index catalogs initially empty',counts(),(0,0,0))

        if a.mode in ['definition','upgrade']:
            uuid=create();check_status(uuid);check('a definition is not a built segment or coverage',counts(),(1,0,0))
            row=query(t.observer,'SELECT table_uuid::text,column_id,type_oid,collation_oid,definition_generation,definition_hash,definition::text,registered_snapshot,state FROM og_lake_internal.index_definition WHERE index_uuid=%s::uuid',(uuid,))[0]
            normalized='{"version":1,"kind":"scalar","method":"btree","column_id":1,"type_oid":23,"collation_oid":0}'
            check('definition resolves permanent column and type identity',row[1:5],(1,23,0,1))
            check('definition JSON normalized to stable identities',json.loads(row[6]),json.loads(normalized))
            check('SHA256 covers table UUID and exact normalized definition',row[5],hashlib.sha256(('og_lake_index_definition_v1|'+row[0]+'|'+normalized).encode()).hexdigest())
            check('definition records existing snapshot without allocation',row[7:],(head[0],'ACTIVE'))
            check('registration does not change standard lake state',t.protocol(),protocol)
            check('registration creates no lake snapshot or IDs',t.head(),head)
            if a.mode=='definition':
                plan='\n'.join(x[0]for x in query(t.a,'EXPLAIN SELECT id,val FROM biz.orders WHERE id=2'))
                check('ordinary query retains actual native Foreign Scan','Foreign Scan'in plan)
                check('definition does not change predicate results',query(t.a,'SELECT id,val FROM biz.orders WHERE id=2'),[(2,20)])
                query(t.a,'UPDATE biz.orders SET val=21 WHERE id=2');expected=[(1,10),(2,21),(3,30)]
                check('pending definition never hides new DML',t.rows(),expected);check_status(uuid)
            drop(uuid);check_status(uuid,'DROPPING');drop(uuid);check_status(uuid,'DROPPING')
        elif a.mode=='transactions':
            query(t.a,'BEGIN');uuid=create();query(t.a,'INSERT INTO biz.heap_marker VALUES(77)');check_status(uuid)
            check('other session cannot see uncommitted index rows',counts(),(0,0,0));query(t.a,'ROLLBACK')
            check('rollback removes definition and ordinary heap data',(counts(),query(t.observer,'SELECT * FROM biz.heap_marker')),((0,0,0),[]))
            query(t.a,'BEGIN');parent=create('parent');query(t.a,'SAVEPOINT child');create('child',spec={**SPEC,'column':'val'})
            query(t.a,'ROLLBACK TO child');query(t.a,'RELEASE SAVEPOINT child')
            check('child registration rollback preserves parent only',counts(t.a),(1,0,0))
            query(t.a,'SAVEPOINT retired');drop(parent);check_status(parent,'DROPPING')
            query(t.a,'ROLLBACK TO retired');query(t.a,'RELEASE SAVEPOINT retired');check_status(parent)
            query(t.a,'INSERT INTO biz.heap_marker VALUES(78)');query(t.a,'COMMIT')
            check('parent definition and ordinary heap commit atomically',(counts(),query(t.observer,'SELECT * FROM biz.heap_marker')),((1,0,0),[(78,)]))
            query(t.a,'BEGIN');query(t.a,'SAVEPOINT only_child');create('discarded');query(t.a,'ROLLBACK TO only_child');query(t.a,'RELEASE SAVEPOINT only_child')
            query(t.a,'INSERT INTO biz.orders VALUES(4,40)');query(t.a,'COMMIT');expected.append((4,40))
            check('rolled-back index DDL guard permits parent lake DML',t.rows(),expected)
        elif a.mode=='names':
            first=create();rejected(t.a,'SELECT og_lake.create_lake_index(%s::regclass,%s::name,%s::jsonb)','42710',('biz.orders','ix_id',json.dumps(SPEC)))
            drop(first);second=create();check('dropped name can be reused with a fresh UUID',first!=second)
            complex_uuid=create('ix,"湖');check_status(complex_uuid)
            hashes=query(t.observer,"SELECT definition_hash FROM og_lake_internal.index_definition WHERE state='ACTIVE' ORDER BY index_name")
            check('index names do not change semantic definition hash',len(set(x[0]for x in hashes)),1)
            query(t.a,'ALTER FOREIGN TABLE biz.orders RENAME TO renamed');t.name='renamed'
            check_status(second);check('host rename preserves index identity and original table rows',t.rows(),expected)
            query(t.a,"CREATE FOREIGN TABLE biz.big(id bigint,val bigint) SERVER created_lake OPTIONS(binding_mode 'create',lake_schema 'main')")
            big=create('ix_id',relation='biz.big');check_status(big)
            check('same index name is scoped to table binding',scalar(t.observer,"SELECT count(*) FROM og_lake_internal.index_definition WHERE state='ACTIVE' AND index_name='ix_id'"),2)
            check('bigint definition stores its actual type',scalar(t.observer,'SELECT type_oid FROM og_lake_internal.index_definition WHERE index_uuid=%s::uuid',(big,)),20)
        elif a.mode=='permissions':
            owner=roles.create('business');reader=roles.create('reader')
            query(t.a,sql.SQL('GRANT USAGE,CREATE ON SCHEMA biz TO {}').format(sql.Identifier(owner)))
            query(t.a,sql.SQL('GRANT USAGE ON FOREIGN SERVER created_lake TO {}').format(sql.Identifier(owner)))
            query(t.a,sql.SQL('GRANT USAGE ON SCHEMA biz TO {}').format(sql.Identifier(reader)))
            ordinary,identity=roles.connect(db,'business');unprivileged,other=roles.connect(db,'reader');r['ordinary_identities']=[identity,other]
            t.create('owned',conn=ordinary);query(t.a,sql.SQL('GRANT SELECT ON biz.owned TO {}').format(sql.Identifier(reader)))
            uuid=create(relation='biz.owned',conn=ordinary);check_status(uuid,conn=ordinary)
            rejected(unprivileged,'SELECT og_lake.create_lake_index(%s::regclass,%s::name,%s::jsonb)','42501',('biz.owned','not_owned',json.dumps(SPEC)))
            rejected(unprivileged,'SELECT og_lake.lake_index_status(%s::uuid)','42501',(uuid,))
            rejected(unprivileged,'SELECT og_lake.drop_lake_index(%s::uuid)','42501',(uuid,))
            rejected(ordinary,'SELECT * FROM og_lake_internal.index_definition','42501')
            rejected(ordinary,'SELECT og_lake_internal._index_catalog_upgrade()','42501')
            check('private failures restore caller identity',scalar(ordinary,'SELECT current_user'),owner)
            check_status(uuid,conn=ordinary);drop(uuid,conn=ordinary);check_status(uuid,'DROPPING',conn=ordinary)
        elif a.mode=='unbind':
            uuid=create();before=t.protocol();query(t.a,'DROP FOREIGN TABLE biz.orders')
            check('host unbind preserves full standard data and history',t.protocol(),before)
            check_status(uuid,'DROPPING')
            check('retired definition retains table identity with detached binding',query(t.observer,'SELECT binding_id,table_id,table_uuid IS NOT NULL,state FROM og_lake_internal.index_definition WHERE index_uuid=%s::uuid',(uuid,)),[(None,1,True,'DROPPING')])
            query(t.a,"CREATE FOREIGN TABLE biz.rebound(id integer,val integer) SERVER created_lake OPTIONS(binding_mode 'attach',table_id '1')");t.name='rebound'
            check('reattached data is unchanged',t.rows(),expected)
            fresh=create(relation='biz.rebound');check('new binding can register same name without reviving tombstone',fresh!=uuid)
            check_status(fresh);check_status(uuid,'DROPPING')
        elif a.mode=='guards':
            invalid=[([], '22023'), ({'version':1},'22023'), ({**SPEC,'other':True},'22023'), ({**SPEC,'version':2},'0A000'),
                     ({**SPEC,'kind':'diskann'},'0A000'), ({**SPEC,'method':'bitmap'},'0A000'), ({**SPEC,'column':'missing'},'42703')]
            for spec,code in invalid:
                rejected(t.a,'SELECT og_lake.create_lake_index(%s::regclass,%s::name,%s::jsonb)',code,('biz.orders','bad',json.dumps(spec)))
            rejected(t.a,'SELECT og_lake.create_lake_index(%s::regclass,%s::name,%s::jsonb)','42809',('biz.heap_marker','bad',json.dumps(SPEC)))
            query(t.a,'BEGIN READ ONLY');rejected(t.a,'SELECT og_lake.create_lake_index(%s::regclass,%s::name,%s::jsonb)','25006',('biz.orders','bad',json.dumps(SPEC)))
            query(t.a,'BEGIN');t.create('local_only');rejected(t.a,'SELECT og_lake.create_lake_index(%s::regclass,%s::name,%s::jsonb)','0A000',('biz.local_only','bad',json.dumps(SPEC)))
            query(t.a,'BEGIN');query(t.a,'UPDATE biz.orders SET val=99 WHERE id=1')
            rejected(t.a,'SELECT og_lake.create_lake_index(%s::regclass,%s::name,%s::jsonb)','0A000',('biz.orders','bad',json.dumps(SPEC)))
            query(t.a,'BEGIN');create();rejected(t.a,'INSERT INTO biz.orders VALUES(4,40)','0A000')
            query(t.a,'UPDATE og_lake_internal.table_binding SET read_only=true WHERE table_id=1')
            rejected(t.a,'SELECT og_lake.create_lake_index(%s::regclass,%s::name,%s::jsonb)','25006',('biz.orders','bad',json.dumps(SPEC)))
            query(t.a,'UPDATE og_lake_internal.table_binding SET read_only=false WHERE table_id=1')
            check('all unsupported requests leave standard metadata and data intact',t.protocol(),protocol)
            check('no rejected definition persists',counts(),(0,0,0));check_status(create())
        elif a.mode=='tamper':
            uuid=create();check_status(uuid)
            for statement,code in [('DROP INDEX og_lake_internal.og_lake_index_definition_live_name','55000'),
                                   ('ALTER TABLE og_lake_internal.index_definition DISABLE TRIGGER ALL','55000'),
                                   ("DROP INDEX og_lake_internal.og_lake_index_definition_live_name; CREATE UNIQUE INDEX og_lake_index_definition_live_name ON og_lake_internal.index_definition(binding_id,index_name) WHERE state='DROPPING'",'55000'),
                                   ('DROP INDEX og_lake_internal.og_lake_index_coverage_one_live_source; CREATE UNIQUE INDEX og_lake_index_coverage_one_live_source ON og_lake_internal.index_coverage(index_uuid,definition_generation,source_file_id) WHERE NOT active','55000')]:
                query(t.a,'BEGIN');query(t.a,statement)
                rejected(t.a,'SELECT og_lake.lake_index_status(%s::uuid)',code,(uuid,));check_status(uuid)
            rejected(t.a,'SELECT og_lake_internal._index_catalog_upgrade()','0A000')
            rejected(t.a,'SELECT og_lake_internal._index_catalog_validate()','0A000')
            check('trust failures preserve business data',t.rows(),expected)
        elif a.mode=='concurrency':
            query(t.a,'BEGIN');uuid=create();query(t.a,'INSERT INTO biz.heap_marker VALUES(77)')
            query(t.b,'INSERT INTO biz.orders VALUES(4,40)');expected.append((4,40))
            check('uncommitted index registration does not block ordinary DML',t.rows(conn=t.b),expected)
            after_dml=t.head();pid=scalar(t.b,'SELECT pg_backend_pid()');pool=ThreadPoolExecutor(max_workers=1)
            def duplicate():
                try:return create(conn=t.b)
                except psycopg2.Error as exc:query(t.b,'ROLLBACK');return exc.pgcode
            future=pool.submit(duplicate);deadline=time.monotonic()+15;waiting=[]
            while time.monotonic()<deadline:
                waiting=query(t.observer,'SELECT pid FROM pg_locks WHERE pid=%s AND NOT granted',(pid,))
                if waiting or future.done():break
                time.sleep(.01)
            check('actual concurrent same-name registration waits',len(waiting)>0)
            check('other session sees no uncommitted index rows',counts(),(0,0,0));query(t.a,'COMMIT')
            check('waited writer sees committed name and rejects duplicate',future.result(timeout=20),'42710')
            pool.shutdown(wait=True);pool=None;check_status(uuid)
            check('registration commit allocates no lake snapshot',t.head(),after_dml)
            check('ordinary heap transaction marker commits with definition',query(t.observer,'SELECT * FROM biz.heap_marker'),[(77,)])

        check('current native business values remain exact',t.rows(),expected)
        names=query(t.observer,"SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=%s AND c.relkind='r' ORDER BY c.relname",(t.schema,))
        native=read_native_catalog(t.observer,t.schema,{'tables':[{'table_name':n}for n,in names]},out/'native-export.json')
        lock=json.loads((D/'source-lock.json').read_text());receipt=json.loads((ROOT/'.reference/opengauss-native-lake/build/receipt.json').read_text())
        check('independent reference source lock remains exact',receipt['source_lock_hash'],sha256(D/'source-lock.json'))
        ref=Reference(out,lock,receipt);transport=out/'transport.ducklake';transport_native(ref,transport,native)
        # A native host rename changes the standard name, while host-only rebind does not.
        reference_table='renamed'if a.mode=='names'else'orders'
        found=ref.run(ref.attach(transport,True)+'SELECT id,val FROM lake.main.'+reference_table+' ORDER BY id','current',result=True)
        check('pinned DuckLake still reads standard data without index extension',[tuple(x.values())for x in found],expected)
        check('all executed inputs unchanged',all(sha256(f)==h for f,h in r['inputs'].items()));check_build(a.build_receipt);r['status']='pass'
    except BaseException as exc:r.update(reason=str(exc),traceback=traceback.format_exc())
    finally:
        if future is not None and not future.done() and t is not None:
            try:scalar(t.observer,'SELECT pg_cancel_backend(%s)',(pid,));query(t.a,'ROLLBACK')
            except BaseException:pass
        if pool is not None:pool.shutdown(wait=True)
        if db is not None:
            try:db.cleanup();r['database_cleanup']=db.cleanup_result
            except BaseException as exc:r.update(status='fail',cleanup_error=str(exc))
        if roles is not None:
            try:
                if not r.get('database_cleanup')or r['database_cleanup']['returncode']:raise RuntimeError('Owned DB cleanup unproven; role retained')
                r['role_cleanup']=cleanup_roles(control,roles.path,a.run_id);r=roles.redact(r)
            except BaseException as exc:r.update(status='fail',role_cleanup_error=str(exc))
        if control is not None:control.close()
        write_json(out/'result.json',r)
    print(json.dumps({'status':r['status'],'mode':a.mode,'checks':len(r['assertions']),'reason':r.get('reason'),'result':str(out/'result.json')},ensure_ascii=False))
    return 0 if r['status']=='pass'else 1


if __name__=='__main__':raise SystemExit(main())
