#!/usr/bin/env python3
"""Real native table RENAME: atomic catalog publication, stable data, host plans."""
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
from reverse_reference import Reference,qi

MODES=['inline','file','savepoints','batch','names','conflicts','cached','claim_retry','cancel','permissions','guards','upgrade']


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--run-id',required=True)
    p.add_argument('--build-receipt',type=Path,required=True);p.add_argument('--mode',choices=MODES,required=True)
    a=p.parse_args();validate_run_id(a.run_id)
    out=ROOT/'公共文档/openGauss原生湖存储/验证'/a.run_id;out.mkdir(exist_ok=False)
    r={'status':'fail','scope':'Native SQL0.13 RENAME '+a.mode,'assertions':[],
       'limitations':['Permanent primitive tables in one native catalog; local RC functional correctness. Other ALTER, live external writers and commercial qualification remain separate.']}
    files=[Path(__file__),D/'tests/native_table_create.py',D/'tests/permission_api.py',D/'native_test_context.py',
           D/'native_runtime.py',D/'tests/inline_cleanup.py',D/'tests/file_dml_acceptance.py',D/'inline_fixture.py',
           D/'tests/native_auto_flush.py',D/'tests/empty_catalog_oracle.py',D/'tests/reverse_reference.py',D/'source-lock.json']
    r['inputs']={str(f):sha256(f)for f in files};archive=out/'executed-inputs';archive.mkdir()
    for f in files:shutil.copy2(f,archive/f.name)
    db=roles=control=t=holder=pool=None;futures=[];pids=[]

    def check(name,got,want=True):
        ok=got==want;r['assertions'].append({'name':name,'observed':got,'expected':want,'status':'pass'if ok else'fail'})
        if not ok:raise AssertionError(name)

    def rejected(conn,statement,code,args=None):
        import psycopg2
        try:query(conn,statement,args)
        except psycopg2.Error as exc:
            check('rejected '+str(statement)[:140],exc.pgcode,code);query(conn,'ROLLBACK')
        else:raise AssertionError('Unexpectedly accepted '+str(statement))

    try:
        import psycopg2
        from psycopg2 import sql
        b=check_build(a.build_receipt);r.update(source_head=b['source_head'],binary_sha256=b['binary_sha256'])
        control=connect_control();check('automatic scheduler stays disabled',scalar(control,'SHOW enable_lake_maintenance'),'off')
        with (out/'fixture.log').open('w')as log:
            subprocess.run([sys.executable,str(D/'tests/catalog_fixture_bundle.py'),'--run-id',a.run_id,
                            '--output',str(out/'fixture/manifest.json')],stdout=log,stderr=subprocess.STDOUT,check=True,timeout=300)
        db=OwnedDatabase(a.run_id,out).create();roles=OwnedRoles(control,a.run_id,out)
        t=Case(db,SimpleNamespace(mode='net_dml',sql_version='0.12'if a.mode=='upgrade'else'0.13',
            manifest=out/'fixture/manifest.json',output=out/'result.json',run_id=a.run_id),r,roles)
        t.create();query(t.a,'INSERT INTO biz.orders VALUES(1,10),(2,20),(3,30)')
        original=[(1,10),(2,20),(3,30)];old_snapshot=t.head()[0]
        history=[(old_snapshot,'orders',original)]

        def rename(old,new,conn=None,schema='biz'):
            query(conn or t.a,sql.SQL('ALTER FOREIGN TABLE {}.{} RENAME TO {}').format(sql.Identifier(schema),sql.Identifier(old),sql.Identifier(new)))

        def definitions():
            return query(t.observer,'SELECT table_id,table_uuid::text,begin_snapshot,end_snapshot,schema_id,table_name,path,path_is_relative FROM created_metadata.ducklake_table ORDER BY table_id,begin_snapshot')

        def commit(conn):
            try:query(conn,'COMMIT');return 'committed'
            except psycopg2.Error as exc:
                outcome=exc.pgcode;query(conn,'ROLLBACK');return outcome

        def wait_for(predicate,label):
            deadline=time.monotonic()+15;seen=None
            while time.monotonic()<deadline:
                seen=predicate()
                if seen:return seen
                if futures and all(f.done()for f in futures):break
                time.sleep(.01)
            raise AssertionError(label+': actual wait not reached; last='+repr(seen))

        def check_published(before,old_defs,targets,protocol=None):
            now=t.head();check('one late snapshot/schema version without ID allocation',now,(before[0]+1,before[1]+1,before[2],before[3]))
            rows=definitions()
            for tid,new in targets.items():
                old=[x for x in old_defs if x[0]==tid and x[3]is None]
                check('exact one old live definition for table'+str(tid),len(old),1)
                prior=old[0];versions=[x for x in rows if x[0]==tid]
                ended=tuple(prior[:3])+(now[0],)+tuple(prior[4:])
                added=(tid,prior[1],now[0],None,prior[4],new,prior[6],prior[7])
                check('versioned name preserves ID UUID path and previous versions for table'+str(tid),versions,[ended,added])
                check('host binding version updated without changing permanent identity',query(t.observer,
                    'SELECT table_id,table_uuid::text,schema_version FROM og_lake_internal.table_binding WHERE table_id=%s',(tid,)),[(tid,prior[1],now[1])])
                check('new standard schema version recorded once',query(t.observer,
                    'SELECT begin_snapshot,schema_version,table_id FROM created_metadata.ducklake_schema_versions WHERE begin_snapshot=%s AND table_id=%s',(now[0],tid)),[(now[0],now[1],tid)])
            expected=','.join('created_table:"main".'+qi(targets[tid])for tid in sorted(targets))
            check('reference rename changeset encoding',query(t.observer,'SELECT changes_made FROM created_metadata.ducklake_snapshot_changes WHERE snapshot_id=%s',(now[0],)),[(expected,)])
            if protocol is not None:
                after=t.protocol();ignore={'ducklake_table','ducklake_schema_versions','ducklake_snapshot','ducklake_snapshot_changes'}
                check('all data carriers, file references, options, columns and statistics unchanged',
                      {k:v for k,v in after.items()if k not in ignore},{k:v for k,v in protocol.items()if k not in ignore})

        if a.mode in ['inline','file','names']:
            if a.mode=='file':
                query(t.a,"SELECT og_lake.flush('biz.orders'::regclass)");history.append((t.head()[0],'orders',original))
            before=t.head();old_defs=definitions();protocol=t.protocol();oid=scalar(t.a,"SELECT 'biz.orders'::regclass::oid")
            root=Path(t.empty_manifest['data_path']);disk={str(p):sha256(p)for p in root.rglob('*.parquet')}
            new='x,"湖表'if a.mode=='names'else'renamed'
            query(t.a,'BEGIN');rename('orders',new);query(t.a,'INSERT INTO biz.heap_marker VALUES(77)')
            check('no standard snapshot claimed before user COMMIT',t.head(),before)
            check('rename uses same host OID immediately in its own transaction',scalar(t.a,'SELECT %s::regclass::oid',('biz.'+qi(new),)),oid)
            check('own renamed table reads exact data before publication',t.rows(name=new),original)
            check('standard metadata remains unchanged before COMMIT',t.protocol(),protocol)
            query(t.a,'COMMIT');t.name=new
            check_published(before,old_defs,{1:new},protocol)
            check('rename preserves current complete native data',t.rows(),original)
            check('ordinary heap marker and rename commit together',query(t.observer,'SELECT * FROM biz.heap_marker'),[(77,)])
            check('rename performs no file mutation or movement',{str(p):sha256(p)for p in root.rglob('*.parquet')},disk)
            if a.mode=='file':
                query(t.a,sql.SQL('UPDATE {} SET val=11 WHERE id=1').format(t.relation()))
                query(t.a,'SELECT og_lake.flush(%s::regclass)',('biz.'+qi(new),))
                check('native file DML and flush work through renamed binding',t.rows(),[(1,11),(2,20),(3,30)])
        elif a.mode=='savepoints':
            protocol=t.protocol();host=t.host_image()
            query(t.a,'BEGIN');rename('orders','discarded');query(t.a,'INSERT INTO biz.heap_marker VALUES(77)');query(t.a,'ROLLBACK')
            check('whole rollback restores all metadata and data',t.protocol(),protocol)
            check('whole rollback restores host names OIDs bindings and heap',t.host_image(),host)
            query(t.a,'BEGIN');query(t.a,'SAVEPOINT child');rename('orders','discarded')
            query(t.a,'ROLLBACK TO child');query(t.a,'RELEASE SAVEPOINT child')
            query(t.a,'INSERT INTO biz.orders VALUES(4,40)');query(t.a,'COMMIT')
            check('aborted rename event permits parent DML',t.rows(),original+[(4,40)])
            before=t.head();old_defs=definitions();protocol=t.protocol()
            query(t.a,'BEGIN');rename('orders','parent');query(t.a,'SAVEPOINT child');rename('parent','discarded')
            query(t.a,'ROLLBACK TO child');query(t.a,'RELEASE SAVEPOINT child')
            check('child rollback preserves parent renamed data',t.rows(name='parent'),original+[(4,40)])
            rename('parent','renamed');query(t.a,'COMMIT');t.name='renamed'
            check_published(before,old_defs,{1:'renamed'},protocol)
        elif a.mode=='batch':
            t.create('other');query(t.a,'INSERT INTO biz.other VALUES(9,90)')
            before=t.head();old_defs=definitions();protocol=t.protocol()
            query(t.a,'BEGIN');rename('orders','temporary');rename('other','orders');rename('temporary','other')
            check('batch does not publish intermediate names',t.protocol(),protocol)
            query(t.a,'COMMIT');check_published(before,old_defs,{1:'other',2:'orders'},protocol)
            check('first stable table now at other name',t.rows(name='other'),original)
            check('second stable table now at orders name',t.rows(name='orders'),[(9,90)])
        elif a.mode=='conflicts':
            query(t.a,'CREATE TABLE biz.occupied(id integer)');before=t.protocol()
            query(t.a,'BEGIN');query(t.a,'INSERT INTO biz.heap_marker VALUES(77)')
            rejected(t.a,'ALTER FOREIGN TABLE biz.orders RENAME TO occupied','42P07')
            check('host name collision rolls back journal and standard data',t.protocol(),before)
            query(t.a,'CREATE SCHEMA otherbiz')
            query(t.a,'CREATE FOREIGN TABLE otherbiz."TaRgEt"(id integer,val integer) SERVER created_lake OPTIONS(binding_mode \'create\',lake_schema \'main\')')
            before=t.protocol();host=t.host_image()
            query(t.a,'BEGIN');query(t.a,'INSERT INTO biz.heap_marker VALUES(77)');rename('orders','target')
            rejected(t.a,'COMMIT','40001')
            check('lake ASCII name collision atomically restores full standard state',t.protocol(),before)
            check('failed publication restores host identity and ordinary heap',t.host_image(),host)
            query(t.a,'BEGIN');rename('orders','same');rename('TaRgEt','same',schema='otherbiz')
            rejected(t.a,'COMMIT','42P07')
            check('duplicate final lake names restore both sources',t.protocol(),before)
            check('source remains readable after both conflicts',t.rows(),original)
        elif a.mode=='cached':
            query(t.a,'CREATE TABLE biz.heap_source(id integer,val integer)');query(t.a,'INSERT INTO biz.heap_source VALUES(1,10),(2,20),(3,30)')
            for kind,table in [('lake','orders'),('heap','heap_source')]:
                query(t.a,'PREPARE '+kind+'_read AS SELECT id,val FROM biz.'+table+' ORDER BY id')
                query(t.a,'PREPARE '+kind+'_write AS UPDATE biz.'+table+' SET val=val+1 WHERE id=1 RETURNING id,val')
                check(kind+' prepared initial read',query(t.a,'EXECUTE '+kind+'_read'),original)
            query(t.a,'BEGIN');rename('orders','renamed');query(t.a,'ALTER TABLE biz.heap_source RENAME TO heap_renamed');query(t.a,'COMMIT');t.name='renamed'
            def execute(name):
                try:return {'rows':query(t.a,'EXECUTE '+name)}
                except psycopg2.Error as exc:
                    query(t.a,'ROLLBACK');return {'sqlstate':exc.pgcode}
            for suffix in ['read','write']:
                observed=execute('lake_'+suffix);expected=execute('heap_'+suffix)
                check('native cached '+suffix+' follows actual host heap rename semantics',observed,expected)
            check('fresh renamed query matches actual heap oracle',t.rows(),query(t.a,'SELECT id,val FROM biz.heap_renamed ORDER BY id'))
            query(t.a,'PREPARE fresh_read AS SELECT id,val FROM biz.renamed ORDER BY id')
            check('new prepared plan resolves renamed binding',query(t.a,'EXECUTE fresh_read'),t.rows())
        elif a.mode in ['claim_retry','cancel']:
            if a.mode=='claim_retry':
                t.create('other');query(t.a,'INSERT INTO biz.other VALUES(9,90)')
            before=t.head();old_defs=definitions();protocol=t.protocol()
            query(t.a,'BEGIN');rename('orders','renamed');query(t.a,'INSERT INTO biz.heap_marker VALUES(77)')
            holder=db.connect();query(holder,'BEGIN');query(holder,'LOCK TABLE created_metadata.ducklake_snapshot_changes IN SHARE MODE')
            pool=ThreadPoolExecutor(max_workers=2);pid_a=scalar(t.a,'SELECT pg_backend_pid()');pids=[pid_a]
            if a.mode=='claim_retry':
                query(t.b,'BEGIN');query(t.b,'UPDATE biz.other SET val=91 WHERE id=9');query(t.b,'INSERT INTO biz.heap_marker VALUES(78)')
                pid_b=scalar(t.b,'SELECT pg_backend_pid()');pids.append(pid_b)
                fb=pool.submit(commit,t.b);futures.append(fb)
                wait_for(lambda:query(t.observer,"SELECT pid FROM pg_locks WHERE pid=%s AND relation='created_metadata.ducklake_snapshot_changes'::regclass AND mode='RowExclusiveLock' AND NOT granted",(pid_b,)),'DML winner reaches changeset barrier')
                fa=pool.submit(commit,t.a);futures.append(fa)
                wait_for(lambda:query(t.observer,'SELECT pid FROM pg_locks WHERE pid=%s AND NOT granted',(pid_a,)),'rename waits for actual competing snapshot claim')
                check('competing publication remains invisible before release',t.protocol(),protocol)
                query(holder,'COMMIT');check('real DML winner commits',fb.result(timeout=20),'committed')
                check('rename retries losing claim and commits',fa.result(timeout=20),'committed')
                current=t.head();check('two real publications advance snapshots without rename ID allocation',current[:3],(before[0]+2,before[1]+1,before[2]))
                check('rename itself consumes no file IDs',current[3],before[3]+1)
                check('other table DML survives rename retry',t.rows(name='other'),[(9,91)])
                check('both ordinary heap transaction markers commit',query(t.observer,'SELECT * FROM biz.heap_marker ORDER BY id'),[(77,),(78,)])
            else:
                fa=pool.submit(commit,t.a);futures.append(fa)
                wait_for(lambda:query(t.observer,"SELECT pid FROM pg_locks WHERE pid=%s AND relation='created_metadata.ducklake_snapshot_changes'::regclass AND mode='RowExclusiveLock' AND NOT granted",(pid_a,)),'rename writes definitions before actual changeset barrier')
                check('uncommitted rename publication leaves exact old standard data',t.protocol(),protocol)
                check('cancel actual rename publisher',scalar(t.observer,'SELECT pg_cancel_backend(%s)',(pid_a,)))
                check('cancelled rename returns actual cancellation',fa.result(timeout=20),'57014');query(holder,'ROLLBACK')
                check('cancel restores entire standard catalog',t.protocol(),protocol)
                check('cancel restores host name and business rows',t.rows(),original)
                check('cancel rolls back ordinary heap marker',query(t.observer,'SELECT * FROM biz.heap_marker'),[])
                rename('orders','renamed');check_published(before,old_defs,{1:'renamed'},protocol)
            pool.shutdown(wait=True);pool=None;t.name='renamed'
            check('renamed source has original complete rows',t.rows(),original)
        elif a.mode=='permissions':
            owner=roles.create('business');reader=roles.create('reader')
            query(t.a,sql.SQL('GRANT USAGE,CREATE ON SCHEMA biz TO {}').format(sql.Identifier(owner)))
            query(t.a,sql.SQL('GRANT USAGE ON FOREIGN SERVER created_lake TO {}').format(sql.Identifier(owner)))
            query(t.a,sql.SQL('GRANT USAGE ON SCHEMA biz TO {}').format(sql.Identifier(reader)))
            ordinary,identity=roles.connect(db,'business');unprivileged,reader_identity=roles.connect(db,'reader')
            r['ordinary_identities']=[identity,reader_identity];t.create('owned',conn=ordinary)
            query(ordinary,'INSERT INTO biz.owned VALUES(8,80)')
            query(t.a,sql.SQL('GRANT SELECT ON biz.owned TO {}').format(sql.Identifier(reader)))
            rejected(unprivileged,'ALTER FOREIGN TABLE biz.owned RENAME TO stolen','42501')
            rejected(ordinary,'ALTER FOREIGN TABLE biz.orders RENAME TO stolen','42501')
            before=t.head();old_defs=definitions();protocol=t.protocol()
            rename('owned','owned_new',conn=ordinary);check_published(before,old_defs,{2:'owned_new'},protocol)
            check('ordinary owner reads renamed table',query(ordinary,'SELECT id,val FROM biz.owned_new'),[(8,80)])
            check('SELECT grant remains attached to same object',query(unprivileged,'SELECT id,val FROM biz.owned_new'),[(8,80)])
            rejected(ordinary,'SELECT * FROM created_metadata.ducklake_table','42501')
            check('failed internal access restores ordinary identity',scalar(ordinary,'SELECT current_user'),owner)
        elif a.mode=='guards':
            base=t.protocol()
            pairs=[('UPDATE biz.orders SET val=11 WHERE id=1','ALTER FOREIGN TABLE biz.orders RENAME TO renamed'),
                   ('ALTER FOREIGN TABLE biz.orders RENAME TO renamed','INSERT INTO biz.renamed VALUES(4,40)'),
                   ("SELECT og_lake.set_option('biz.orders'::regclass,'data_inlining_row_limit','50')",'ALTER FOREIGN TABLE biz.orders RENAME TO renamed'),
                   ('ALTER FOREIGN TABLE biz.orders RENAME TO renamed',"SELECT og_lake.set_option('biz.renamed'::regclass,'data_inlining_row_limit','50')"),
                   ("SELECT og_lake.flush('biz.orders'::regclass)",'ALTER FOREIGN TABLE biz.orders RENAME TO renamed'),
                   ('ALTER FOREIGN TABLE biz.orders RENAME TO renamed',"SELECT og_lake.flush('biz.renamed'::regclass)"),
                   ('ALTER FOREIGN TABLE biz.orders RENAME TO renamed','DROP FOREIGN TABLE biz.renamed'),
                   ('ALTER FOREIGN TABLE biz.orders RENAME TO renamed',"CREATE FOREIGN TABLE biz.extra(id integer,val integer) SERVER created_lake OPTIONS(binding_mode 'create',lake_schema 'main')"),
                   ("CREATE FOREIGN TABLE biz.extra(id integer,val integer) SERVER created_lake OPTIONS(binding_mode 'create',lake_schema 'main')",'ALTER FOREIGN TABLE biz.orders RENAME TO renamed')]
            for first,second in pairs:
                query(t.a,'BEGIN');query(t.a,first);rejected(t.a,second,'0A000')
                check('rejected mixed command preserves standard protocol',t.protocol(),base)
                check('rejected mixed command preserves host/data source',t.rows(),original)
            for statement in ['ALTER FOREIGN TABLE biz.orders RENAME COLUMN id TO id_new','ALTER FOREIGN TABLE biz.orders ADD COLUMN extra integer','ALTER FOREIGN TABLE biz.orders SET SCHEMA public']:
                rejected(t.a,statement,'0A000')
            query(t.a,'BEGIN');t.create('local_only')
            rejected(t.a,'ALTER FOREIGN TABLE biz.local_only RENAME TO local_new','0A000')
            check('local rename rejection rolls back local CREATE and carrier',t.protocol(),base)
            rename('orders','renamed');t.name='renamed';check('valid rename works after every rejected form',t.rows(),original)
        elif a.mode=='upgrade':
            before=t.protocol();rejected(t.a,'ALTER FOREIGN TABLE biz.orders RENAME TO renamed','0A000')
            check('old SQL contract remains rejected without mutations',t.protocol(),before)
            head=t.head();query(t.a,"ALTER EXTENSION og_lake UPDATE TO '0.13'")
            check('SQL upgrade does not allocate data IDs or snapshots',t.head(),head)
            old_defs=definitions();protocol=t.protocol();rename('orders','renamed');t.name='renamed'
            check_published(head,old_defs,{1:'renamed'},protocol)
            check('upgraded original table reads complete data',t.rows(),original)

        names=query(t.observer,"SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=%s AND c.relkind='r' ORDER BY c.relname",(t.schema,))
        native=read_native_catalog(t.observer,t.schema,{'tables':[{'table_name':n}for n,in names]},out/'native-export.json')
        lock=json.loads((D/'source-lock.json').read_text());receipt=json.loads((ROOT/'.reference/opengauss-native-lake/build/receipt.json').read_text())
        check('independent reference uses exact current lock',receipt['source_lock_hash'],sha256(D/'source-lock.json'))
        ref=Reference(out,lock,receipt);transport=out/'transport.ducklake';transport_native(ref,transport,native)
        bindings=query(t.observer,'SELECT b.table_id,n.nspname,c.relname FROM og_lake_internal.table_binding b JOIN og_lake_internal.relation_binding r USING(binding_id) JOIN pg_class c ON c.oid=r.relation_oid JOIN pg_namespace n ON n.oid=c.relnamespace ORDER BY b.table_id')
        live={tid:name for tid,name in query(t.observer,'SELECT table_id,table_name FROM created_metadata.ducklake_table WHERE end_snapshot IS NULL')}
        for tid,schema,name in bindings:
            expected=query(t.observer,sql.SQL('SELECT id,val FROM {}.{} ORDER BY id').format(sql.Identifier(schema),sql.Identifier(name)))
            found=ref.run(ref.attach(transport,True)+'SELECT rowid,id,val FROM lake.main.'+qi(live[tid])+' ORDER BY id','current-'+str(tid),result=True)
            check('pinned DuckLake reads all current values for table'+str(tid),[(row['id'],row['val'])for row in found],expected)
        for snap,name,expected in history:
            found=ref.run(ref.attach(transport,True)+'SELECT id,val FROM lake.main.'+qi(name)+' AT(VERSION => '+str(snap)+') ORDER BY id','history-'+str(snap),result=True)
            check('original name and rows remain visible at old snapshot'+str(snap),[tuple(row.values())for row in found],expected)
        if a.mode in ['inline','file','names']:
            relation='lake.main.'+qi(live[1]);expected=t.rows()+[(4,40)]
            ref.run(ref.attach(transport)+'INSERT INTO '+relation+' VALUES(4,40)','independent-continuation')
            found=ref.run(ref.attach(transport,True)+'SELECT id,val FROM '+relation+' ORDER BY id','continued',result=True)
            check('pinned DuckLake continues writing renamed native catalog',[tuple(row.values())for row in found],expected)
            ref.run(ref.attach(transport)+"CALL ducklake_flush_inlined_data('lake')",'continued-flush')
            found=ref.run(ref.attach(transport,True)+'SELECT id,val FROM '+relation+' ORDER BY id','continued-after-flush',result=True)
            check('pinned DuckLake flushes preserved original carrier layout',[tuple(row.values())for row in found],expected)
            for snap,name,old in history:
                found=ref.run(ref.attach(transport,True)+'SELECT id,val FROM lake.main.'+qi(name)+' AT(VERSION => '+str(snap)+') ORDER BY id','continued-old-'+str(snap),result=True)
                check('independent continuation/flush preserves old name history'+str(snap),[tuple(row.values())for row in found],old)
        check('all executed input bytes unchanged',all(sha256(f)==h for f,h in r['inputs'].items()));check_build(a.build_receipt);r['status']='pass'
    except BaseException as exc:r.update(reason=str(exc),traceback=traceback.format_exc())
    finally:
        if t is not None:
            for f,pid in zip(futures,pids[::-1]if a.mode=='claim_retry'else pids):
                if not f.done():
                    try:scalar(t.observer,'SELECT pg_cancel_backend(%s)',(pid,))
                    except BaseException:pass
        if holder is not None:
            try:query(holder,'ROLLBACK')
            except BaseException:pass
        if pool is not None:pool.shutdown(wait=True)
        if db is not None:
            try:db.cleanup();r['database_cleanup']=db.cleanup_result
            except BaseException as exc:r.update(status='fail',cleanup_error=str(exc))
        if roles is not None:
            try:
                if not r.get('database_cleanup')or r['database_cleanup']['returncode']:raise RuntimeError('Owned database cleanup unproven; role retained')
                r['role_cleanup']=cleanup_roles(control,roles.path,a.run_id);r=roles.redact(r)
            except BaseException as exc:r.update(status='fail',role_cleanup_error=str(exc))
        if control is not None:control.close()
        write_json(out/'result.json',r)
    print(json.dumps({'status':r['status'],'mode':a.mode,'checks':len(r['assertions']),'reason':r.get('reason'),'result':str(out/'result.json')},ensure_ascii=False))
    return 0 if r['status']=='pass'else 1


if __name__=='__main__':raise SystemExit(main())
