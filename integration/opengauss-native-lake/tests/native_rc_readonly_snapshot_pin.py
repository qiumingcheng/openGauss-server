#!/usr/bin/env python3
"""Compare ordinary heap and lake RC snapshot lifetime using registered heads.

AST/preparation only until the root serial fixture coordinator authorizes run.
No XID-producing function is called in the readonly transaction. This is not
a GC implementation, throughput result or target RAC qualification.
"""
import argparse
import fcntl
import json
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time
import traceback
from types import SimpleNamespace

ROOT=Path(__file__).resolve().parents[3];D=ROOT/'integration/opengauss-native-lake'
sys.path[:0]=[str(D),str(D/'tests')]
from file_dml_acceptance import check_build
from inline_fixture import query,scalar
from lake_harness import sha256,validate_run_id,write_json
from native_auto_flush import connect_control
from native_runtime import PROFILE
from native_table_create import Case
from native_test_context import OwnedDatabase
from permission_api import OwnedRoles,cleanup_roles


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--run-id',required=True);p.add_argument('--build-receipt',required=True,type=Path)
    p.add_argument('--lake-expectation',choices=['released-at-statement-end','retained-until-transaction-end'],
        default='released-at-statement-end')
    p.add_argument('--horizon-budget-seconds',type=int,default=20)
    p.add_argument('--sql-version',choices=['0.17','0.18'],default='0.17')
    a=p.parse_args();validate_run_id(a.run_id)
    if PROFILE!='debug':p.error('Requires owned main Debug runtime')
    if not 5<=a.horizon_budget_seconds<=30:p.error('Horizon budget must be 5..30 seconds')
    out=ROOT/'公共文档/openGauss原生湖存储/验证'/a.run_id;out.mkdir(exist_ok=False)
    r=dict(status='fail',scope=__doc__,lake_expectation=a.lake_expectation,assertions=[],phases=[],
        readonly_xid_functions_called=False)
    files=[Path(__file__),D/'tests/file_dml_acceptance.py',D/'inline_fixture.py',D/'lake_harness.py',
        D/'tests/native_auto_flush.py',D/'native_runtime.py',D/'native_test_context.py',D/'tests/inline_cleanup.py',
        D/'tests/native_table_create.py',D/'tests/permission_api.py',D/'tests/catalog_fixture_bundle.py',
        D/'tests/empty_manifest.py',D/'tests/empty_reference_fixture.py',D/'tests/empty_catalog_oracle.py',
        D/'tests/create_reference_oracle.py',D/'tests/reverse_reference.py',D/'reference.sh',D/'fixture.py',D/'source-lock.json']
    archive=out/'executed-inputs';archive.mkdir();r['inputs']={str(f):sha256(f)for f in files}
    for f in files:shutil.copy2(f,archive/f.name)
    shutil.copy2(a.build_receipt,archive/'build.json')
    control=db=roles=reservation=None

    def check(name,got,want=True):
        ok=got==want;r['assertions'].append(dict(name=name,observed=got,expected=want,status='pass'if ok else'fail'))
        write_json(out/'result.json',r)
        if not ok:raise AssertionError(name+': '+repr(got))

    def configure(c):
        query(c,'SET session_timeout=0');query(c,'SET statement_timeout=60000')

    def cutoff_probe(admin,head,delete_xid,label):
        started=time.monotonic();deadline=started+a.horizon_budget_seconds;observations=[];ready=False
        while True:
            admin.notices.clear();query(admin,'VACUUM VERBOSE '+head)
            notices=list(admin.notices);text='\n'.join(notices)
            dead=re.search(r'(\d+) dead row versions cannot be removed yet',text)
            cutoff=re.search(r'OldestXid (\d+)',text)
            ready=dead is not None and int(dead.group(1))==0 and cutoff is not None and int(cutoff.group(1))>delete_xid
            observations.append(dict(elapsed_seconds=time.monotonic()-started,notices=notices,ready=ready))
            if ready or time.monotonic()>=deadline:break
            time.sleep(.5)  # Real vacuum cutoff/dead-row observations, not guessed sleep duration.
        return dict(label=label,ready=ready,observations=observations)

    try:
        built=check_build(a.build_receipt)
        r.update(source_head=built['source_head'],binary_sha256=built['binary_sha256'],
            build=str(a.build_receipt.resolve()),build_sha256=sha256(a.build_receipt))
        control=connect_control();configure(control)
        check('automatic worker stays off during controlled snapshot experiment',scalar(control,'SHOW enable_lake_maintenance'),'off')
        check('backend PID oracle uses dedicated backend mode',scalar(control,'SHOW enable_thread_pool'),'off')
        # The real reference builder owns its own global lock. Acquire the same
        # lock for the SQL phase only after the builder has exited.
        with(out/'fixture.log').open('w')as log:
            subprocess.run([sys.executable,str(D/'tests/catalog_fixture_bundle.py'),'--run-id',a.run_id,
                '--output',str(out/'fixture/manifest.json')],stdout=log,stderr=subprocess.STDOUT,check=True,timeout=300)
        reservation=(ROOT/'.reference/opengauss-native-lake/.driver.lock').open('a+')
        fcntl.flock(reservation.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB)
        db=OwnedDatabase(a.run_id,out).create();configure(db.admin);roles=OwnedRoles(control,a.run_id,out)
        case=Case(db,SimpleNamespace(mode='net_dml',sql_version=a.sql_version,manifest=out/'fixture/manifest.json',
            output=out/'result.json',run_id=a.run_id),r,roles)
        for c in [case.a,case.b,case.observer]:configure(c)
        case.create('heap_anchor');case.create('lake_anchor')
        query(case.a,'INSERT INTO biz.heap_anchor VALUES(1,10),(2,20),(3,30)')
        query(case.a,'INSERT INTO biz.lake_anchor VALUES(1,10),(2,20),(3,30)')
        query(case.a,'CREATE TABLE biz.heap_reference(id integer,val integer) WITH(storage_type=astore,hasuids=false)')
        query(case.a,'INSERT INTO biz.heap_reference VALUES(1,10),(2,20),(3,30)')
        mappings={}
        for name in ['heap_anchor','lake_anchor']:
            row=query(case.observer,"SELECT h.row_head_oid,h.binding_id::text,c.relowner,c.relkind,n.nspname,c.relname "
                "FROM og_lake_internal.row_lock_binding h JOIN og_lake_internal.relation_binding r "
                "ON r.relation_oid=h.relation_oid AND r.binding_id=h.binding_id "
                "JOIN pg_class c ON c.oid=h.row_head_oid JOIN pg_namespace n ON n.oid=c.relnamespace "
                "WHERE h.relation_oid=%s::regclass",('biz.'+name,))
            check('one exact registered row-head owner '+name,len(row),1)
            oid,binding,owner,kind,ns,relname=row[0]
            check('registered head lives in private schema '+name,(kind,ns),('r','og_lake_internal'))
            check('registered head is empty before controlled ordinary seed '+name,scalar(case.observer,
                'SELECT count(*) FROM '+case.sql.Identifier(ns).as_string(case.observer)+'.'+case.sql.Identifier(relname).as_string(case.observer)),0)
            mappings[name]=dict(oid=oid,binding_id=binding,owner_oid=owner,
                sql=case.sql.Identifier(ns).as_string(case.observer)+'.'+case.sql.Identifier(relname).as_string(case.observer))
        # No setup actor remains in a business transaction during either phase.
        for c in [case.a,case.b,case.observer]:query(c,'ROLLBACK');c.close()
        for kind,name,read_sql in [('ordinary','heap_anchor','SELECT sum(val) FROM biz.heap_reference'),
                                   ('lake','lake_anchor','SELECT sum(val) FROM biz.lake_anchor')]:
            admin,reader,observer=[db.connect()for _ in range(3)]
            for c in [admin,reader,observer]:configure(c)
            query(admin,'SET client_min_messages=info')
            pid=scalar(reader,'SELECT pg_backend_pid()')
            phase=dict(kind=kind,head=mappings[name],reader_pid=pid);r['phases'].append(phase)
            query(reader,'BEGIN ISOLATION LEVEL READ COMMITTED READ ONLY')
            check(kind+' readonly statement result',scalar(reader,read_sql),60)
            check(kind+' transaction remains open after statement completion',scalar(observer,
                'SELECT state FROM pg_stat_activity WHERE pid=%s',(pid,)),'idle in transaction')
            phase['reader_locks']=query(observer,'SELECT locktype,mode,granted,transactionid::text FROM pg_locks WHERE pid=%s ORDER BY locktype,mode',(pid,))
            check(kind+' readonly session has no observed assigned-XID lock',
                [x for x in phase['reader_locks']if x[0]=='transactionid'and x[1]=='ExclusiveLock'],[])
            head=mappings[name]['sql']
            # Administrative ordinary writes only; no lake row, snapshot or
            # logical-row identity is fabricated by an extension callback.
            snapshot_before=scalar(admin,'SELECT max(snapshot_id) FROM created_metadata.ducklake_snapshot')
            query(admin,'INSERT INTO '+head+'(row_id) SELECT i FROM generate_series(10000000,10019999)i')
            query(admin,'BEGIN');delete_xid=int(scalar(admin,'SELECT txid_current()'))
            query(admin,'DELETE FROM '+head+' WHERE row_id%2=0');query(admin,'COMMIT')
            phase['delete_xid']=delete_xid
            admin.close();admin=db.connect();configure(admin);query(admin,'SET client_min_messages=info')
            phase['host_settings']=query(admin,"SELECT name,setting FROM pg_settings WHERE name IN('vacuum_defer_cleanup_age','defer_csn_cleanup_time','enable_defer_calculate_snapshot','enable_vacuum_extreme_xmin') ORDER BY name")
            phase['other_transactions']=[[str(v)if v is not None else None for v in row]for row in query(observer,
                'SELECT datname,pid,application_name,state,xact_start FROM pg_stat_activity WHERE xact_start IS NOT NULL AND pid<>pg_backend_pid() ORDER BY xact_start')]
            phase['while_readonly_open']=cutoff_probe(admin,head,delete_xid,'statement finished; readonly transaction remains open')
            # Always complete the second half before asserting the requested
            # contract, so a failing current binary leaves the release evidence.
            query(reader,'COMMIT');reader.close()
            phase['after_readonly_end']=cutoff_probe(admin,head,delete_xid,'readonly transaction ended')
            check(kind+' ending readonly transaction permits eventual head vacuum',phase['after_readonly_end']['ready'])
            check(kind+' ordinary head writes do not publish lake snapshots',
                scalar(admin,'SELECT max(snapshot_id) FROM created_metadata.ducklake_snapshot'),snapshot_before)
            admin.close();observer.close()
        ordinary,lake=r['phases']
        check('ordinary RC statement releases its vacuum horizon while transaction stays open',ordinary['while_readonly_open']['ready'])
        check('lake snapshot lifetime matches explicit requested contract',lake['while_readonly_open']['ready'],
            a.lake_expectation=='released-at-statement-end')
        check('all executed inputs unchanged',all(sha256(Path(f))==h for f,h in r['inputs'].items()))
        check_build(a.build_receipt);r['status']='pass'
    except BaseException as exc:r.update(reason=str(exc),traceback=traceback.format_exc())
    finally:
        if db is not None:
            try:db.cleanup();r['database_cleanup']=db.cleanup_result
            except BaseException as exc:r.update(status='fail',cleanup_error=str(exc))
        if roles is not None:
            try:r['role_cleanup']=cleanup_roles(control,roles.path,a.run_id)
            except BaseException as exc:r.update(status='fail',role_cleanup_error=str(exc))
        if control is not None:control.close()
        if reservation is not None:reservation.close()
        write_json(out/'result.json',r)
    print(str(out/'result.json')+': '+r['status']);return 0 if r['status']=='pass'else 1


if __name__=='__main__':sys.exit(main())
