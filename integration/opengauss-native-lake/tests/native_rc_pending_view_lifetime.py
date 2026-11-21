#!/usr/bin/env python3
"""Pending CREATE views, cursor lifetime and savepoint snapshot ownership.

Requires the internal test_xact_state registered_snapshots diagnostic. A real
CREATE assigns a parent XID even after child rollback, so vacuum blockage alone
cannot distinguish an extra snapshot reference from legitimate write lifetime.
Preparation/AST only until the serial Native coordinator authorizes execution.
"""
import argparse
import fcntl
import json
from pathlib import Path
import shutil
import subprocess
import sys
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
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--run-id',required=True)
    p.add_argument('--build-receipt',required=True,type=Path)
    p.add_argument('--sql-version',choices=['0.17','0.18'],default='0.17');a=p.parse_args();validate_run_id(a.run_id)
    if PROFILE!='debug':p.error('Requires owned main Debug runtime')
    out=ROOT/'公共文档/openGauss原生湖存储/验证'/a.run_id;out.mkdir(exist_ok=False)
    r=dict(status='fail',scope=__doc__,assertions=[],snapshot_observations=[])
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

    def counter(conn,label):
        state=json.loads(scalar(conn,'SELECT og_lake_internal.test_xact_state()'))
        if not isinstance(state.get('registered_snapshots'),int):
            raise RuntimeError('Build lacks the internal registered_snapshots diagnostic; do not infer refs from assigned XID')
        value=state['registered_snapshots'];r['snapshot_observations'].append(dict(label=label,count=value,
            transaction_catalogs=state.get('transaction_catalogs'),transaction_tables=state.get('transaction_tables')))
        return value

    try:
        b=check_build(a.build_receipt);r.update(source_head=b['source_head'],binary_sha256=b['binary_sha256'],
            build=str(a.build_receipt.resolve()),build_sha256=sha256(a.build_receipt))
        control=connect_control();configure(control)
        check('maintenance remains off in owned lifecycle test',scalar(control,'SHOW enable_lake_maintenance'),'off')
        with(out/'fixture.log').open('w')as log:
            subprocess.run([sys.executable,str(D/'tests/catalog_fixture_bundle.py'),'--run-id',a.run_id,
                '--output',str(out/'fixture/manifest.json')],stdout=log,stderr=subprocess.STDOUT,check=True,timeout=300)
        reservation=(ROOT/'.reference/opengauss-native-lake/.driver.lock').open('a+')
        fcntl.flock(reservation.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB)
        db=OwnedDatabase(a.run_id,out).create();configure(db.admin);roles=OwnedRoles(control,a.run_id,out)
        case=Case(db,SimpleNamespace(mode='net_dml',sql_version=a.sql_version,manifest=out/'fixture/manifest.json',
            output=out/'result.json',run_id=a.run_id),r,roles)
        for c in [case.a,case.b,case.observer]:configure(c)
        case.create();query(case.a,'INSERT INTO biz.orders VALUES(1,10),(2,20),(3,30)')
        holder=case.b;pid=scalar(holder,'SELECT pg_backend_pid()')
        outside=counter(holder,'outside transaction baseline')
        query(holder,'BEGIN ISOLATION LEVEL READ COMMITTED')
        check('first parent command reads permanent lake',case.rows(holder),[(1,10),(2,20),(3,30)])
        base=counter(holder,'parent readonly catalog after statement')
        check('pure parent read leaves no extra registered refs',base,outside)

        query(holder,'SAVEPOINT child')
        case.create('pending_child',holder)
        counter(holder,'after child CREATE command')
        check('pending child empty read works',case.rows(holder,'pending_child'),[])
        query(holder,'INSERT INTO biz.pending_child VALUES(1,10),(2,20)')
        check('pending own rows are visible through view',case.rows(holder,'pending_child'),[(1,10),(2,20)])
        query(holder,'UPDATE biz.pending_child SET val=val+1 WHERE id=1')
        check('pending UPDATE resolves private journal',case.rows(holder,'pending_child'),[(1,11),(2,20)])
        query(holder,'DECLARE pending_cursor NO SCROLL CURSOR FOR SELECT id,val FROM biz.pending_child ORDER BY id')
        check('cursor reads first private row',query(holder,'FETCH 1 FROM pending_cursor'),[(1,11)])
        held=counter(holder,'pending cursor legitimately holds snapshot')
        check('live cursor owns refs above command baseline',held>base)
        query(holder,'CLOSE pending_cursor')
        after_close=counter(holder,'after pending cursor close')
        query(holder,'SAVEPOINT inner_change');query(holder,'UPDATE biz.pending_child SET val=val+100')
        check('nested update visible',case.rows(holder,'pending_child'),[(1,111),(2,120)])
        query(holder,'ROLLBACK TO inner_change');query(holder,'RELEASE SAVEPOINT inner_change')
        check('nested rollback restores private versions',case.rows(holder,'pending_child'),[(1,11),(2,20)])
        query(holder,'ROLLBACK TO child');query(holder,'RELEASE SAVEPOINT child')
        check('child relation really rolled back',scalar(holder,"SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='biz' AND c.relname='pending_child'"),0)
        check('parent journal survives child CREATE abort',case.rows(holder),[(1,10),(2,20),(3,30)])
        after_abort=counter(holder,'after child CREATE rollback')
        # Defer ref assertions until after rollback so an old promotion build
        # records the precise child-owner leak, not merely its creation point.
        check('closing pending cursor releases its own refs',after_close,base)
        check('child CREATE rollback leaves no promoted catalog ref',after_abort,base)
        xid_locks=query(case.observer,"SELECT transactionid::text FROM pg_locks WHERE pid=%s AND locktype='transactionid' AND mode='ExclusiveLock' AND granted",(pid,))
        r['assigned_parent_xid_locks_after_child_abort']=xid_locks
        check('real CREATE assigned an XID independently of snapshot refs',bool(xid_locks))

        # A released child owns legal pending definitions/versions in its
        # parent. New per-command views must neither free them nor pin forever.
        query(holder,'SAVEPOINT child_commit');case.create('pending_kept',holder)
        query(holder,'INSERT INTO biz.pending_kept VALUES(7,70),(8,80)')
        query(holder,'RELEASE SAVEPOINT child_commit')
        check('subcommit keeps pending rows',case.rows(holder,'pending_kept'),[(7,70),(8,80)])
        check('subcommit owns no stale read view',counter(holder,'after pending subcommit'),base)
        query(holder,'SAVEPOINT drop_pending');query(holder,'DROP FOREIGN TABLE biz.pending_kept')
        query(holder,'ROLLBACK TO drop_pending');query(holder,'RELEASE SAVEPOINT drop_pending')
        check('rollback of pending DROP restores descriptor and rows',case.rows(holder,'pending_kept'),[(7,70),(8,80)])
        for i in range(4):
            query(holder,'SAVEPOINT transient');case.create('pending_transient',holder)
            query(holder,'INSERT INTO biz.pending_transient VALUES(9,90)')
            check('transient pending row '+str(i),case.rows(holder,'pending_transient'),[(9,90)])
            query(holder,'ROLLBACK TO transient');query(holder,'RELEASE SAVEPOINT transient')
            check('no snapshot reference accumulation after child abort '+str(i),counter(holder,'transient child abort '+str(i)),base)
        query(holder,'UPDATE biz.pending_kept SET val=val+1 WHERE id=7')
        query(holder,'COMMIT')
        check('committed pending object publishes final rows',case.rows(case.observer,'pending_kept'),[(7,71),(8,80)])
        check('aborted pending objects were never published',query(case.observer,
            "SELECT table_name FROM created_metadata.ducklake_table WHERE table_name IN('pending_child','pending_transient') AND end_snapshot IS NULL"),[])
        check('new transaction has baseline references only',counter(holder,'after outer COMMIT'),outside)
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
