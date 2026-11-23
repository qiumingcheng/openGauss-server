#!/usr/bin/env python3
"""Controlled ordinary heap row-head reuse diagnostic; not Native RC/GC qualification."""
import argparse
import fcntl
import json
import re
import time
from pathlib import Path
import shutil
import sys
import traceback

ROOT=Path(__file__).resolve().parents[3];D=ROOT/'integration/opengauss-native-lake'
sys.path[:0]=[str(D),str(D/'tests')]
from inline_fixture import query,scalar
from lake_harness import sha256,validate_run_id,write_json
from native_auto_flush import connect_control
from native_runtime import PROFILE,PORT,RUNTIME,INSTALL
from native_test_context import OwnedDatabase
from permission_api import OwnedRoles,cleanup_roles


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--run-id',required=True)
    p.add_argument('--binary-receipt',required=True,type=Path)
    p.add_argument('--wait-horizon-seconds',type=int,default=0)
    a=p.parse_args();validate_run_id(a.run_id)
    if a.wait_horizon_seconds<0 or a.wait_horizon_seconds>30:p.error('Horizon wait must be between 0 and 30 seconds')
    if PROFILE!='debug':p.error('Requires the owned main Debug endpoint')
    out=ROOT/'公共文档/openGauss原生湖存储/验证'/a.run_id;out.mkdir(exist_ok=False)
    r=dict(status='fail',scope=__doc__,assertions=[],space_samples=[],vacuum_notices=[],native_source_qualified=False)
    files=[Path(__file__),D/'inline_fixture.py',D/'lake_harness.py',D/'tests/native_auto_flush.py',D/'native_runtime.py',
        D/'native_test_context.py',D/'tests/permission_api.py',D/'tests/inline_cleanup.py']
    archive=out/'executed-inputs';archive.mkdir();r['inputs']={str(f):sha256(f)for f in files}
    for f in files:shutil.copy2(f,archive/f.name)
    shutil.copy2(a.binary_receipt,archive/'build.json')
    control=db=roles=reservation=None

    def check(name,got,want=True):
        ok=got==want;r['assertions'].append(dict(name=name,observed=got,expected=want,status='pass'if ok else'fail'))
        write_json(out/'result.json',r)
        if not ok:raise AssertionError(name+': '+repr(got))

    def identity():
        b=json.loads(a.binary_receipt.read_text());binary=INSTALL/'bin/gaussdb'
        pid=int((RUNTIME/'data/postmaster.pid').read_text().splitlines()[0]);exe=Path('/proc')/str(pid)/'exe'
        if b['status']!='build_and_sql_passed' or b['profile']!=PROFILE or b['port']!=PORT or exe.resolve()!=binary.resolve():raise RuntimeError('Wrong runtime')
        if sha256(exe)!=b['binary_sha256']or sha256(binary)!=b['binary_sha256']:raise RuntimeError('Runtime binary changed')
        return dict(binary_sha256=b['binary_sha256'],source_head=b['source_head'],postmaster_pid=pid,
            receipt_sha256=sha256(a.binary_receipt))

    def configure(c):
        query(c,'SET session_timeout=0');query(c,'SET statement_timeout=45000')

    def diagnostics(c,stage):
        attrs=[x[0]for x in query(c,"SELECT attname FROM pg_attribute WHERE attrelid='pg_stat_activity'::regclass AND attnum>0 AND NOT attisdropped")]
        cols=['datname','pid','application_name','state','xact_start']+[x for x in ['backend_xmin','backend_xid']if x in attrs]
        active=query(c,'SELECT '+','.join(cols)+' FROM pg_stat_activity WHERE xact_start IS NOT NULL AND pid<>pg_backend_pid() ORDER BY xact_start')
        image=dict(stage=stage,activity_columns=cols,activity=[[str(v)if v is not None else None for v in row]for row in active],
            unavailable_columns=[x for x in ['backend_xmin','backend_xid']if x not in attrs],
            settings=query(c,"SELECT name,setting,context FROM pg_settings WHERE name LIKE '%retention%' OR name LIKE '%csn%' OR name LIKE '%snapshot%' OR name LIKE 'vacuum%age' OR name='enable_vacuum_extreme_xmin' ORDER BY name"),
            snapshot=query(c,'SELECT txid_current_snapshot()::text'))
        r.setdefault('diagnostics',[]).append(image)

    def sizes(c,stage):
        count=int(scalar(c,'SELECT count(*) FROM biz.head'))
        heap,index,total=query(c,"SELECT pg_relation_size('biz.head'),pg_indexes_size('biz.head'),pg_total_relation_size('biz.head')")[0]
        image=dict(stage=stage,rows=count,heap_bytes=int(heap),index_bytes=int(index),total_bytes=int(total),total_bytes_per_live_row=float(total)/count)
        r['space_samples'].append(image);return image

    try:
        reservation=(ROOT/'.reference/opengauss-native-lake/.driver.lock').open('a+')
        fcntl.flock(reservation.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB)
        before=identity();r.update(binary_before=before,source_head=before['source_head'],binary_sha256=before['binary_sha256'])
        control=connect_control();configure(control)
        db=OwnedDatabase(a.run_id,out).create();configure(db.admin);roles=OwnedRoles(control,a.run_id,out)
        actor=db.connect();configure(actor);query(actor,'SET client_min_messages=info')
        query(actor,'CREATE SCHEMA biz');query(actor,'CREATE TABLE biz.head(row_id bigint PRIMARY KEY) WITH(storage_type=astore,hasuids=false)')
        query(actor,'INSERT INTO biz.head SELECT i FROM generate_series(1,20000)i')
        seed=sizes(actor,'20k committed heads')
        query(actor,'BEGIN');r['deleting_xid']=str(scalar(actor,'SELECT txid_current()'))
        query(actor,'DELETE FROM biz.head WHERE row_id%2=0');query(actor,'COMMIT')
        deleted=sizes(actor,'10k logical deletes committed');check('exactly half removed',deleted['rows'],10000)
        # All business actors are now rolled back and closed. A newly opened
        # autocommit observer/vacuum backend cannot retain an earlier snapshot.
        query(actor,'ROLLBACK');actor.close()
        vacuum=db.connect();configure(vacuum);query(vacuum,'SET client_min_messages=info')
        check('new vacuum connection is autocommit',vacuum.autocommit)
        diagnostics(vacuum,'before vacuum; all former actors closed')
        vacuum.notices.clear();query(vacuum,'VACUUM VERBOSE biz.head');r['vacuum_notices'].append(dict(stage='first vacuum',notices=list(vacuum.notices)))
        if a.wait_horizon_seconds:
            started=time.monotonic();deadline=started+a.wait_horizon_seconds
            while True:
                text='\n'.join(vacuum.notices)
                dead=re.search(r'(\d+) dead row versions cannot be removed yet',text)
                cutoff=re.search(r'OldestXid (\d+)',text)
                ready=dead is not None and int(dead.group(1))==0 and cutoff is not None and int(cutoff.group(1))>int(r['deleting_xid'])
                if ready or time.monotonic()>=deadline:break
                time.sleep(.5)  # Poll the actual safe-removal cutoff, not assumed elapsed retention.
                vacuum.notices.clear();query(vacuum,'VACUUM VERBOSE biz.head')
                r['vacuum_notices'].append(dict(stage='bounded horizon polling',elapsed_seconds=time.monotonic()-started,notices=list(vacuum.notices)))
                write_json(out/'result.json',r)
            check('bounded native vacuum horizon actually passed the delete XID',ready)
        sizes(vacuum,'after first vacuum')
        diagnostics(vacuum,'after first vacuum')
        query(vacuum,'INSERT INTO biz.head SELECT i FROM generate_series(1,20000)i WHERE i%2=0')
        reused=sizes(vacuum,'same 10k stable keys recreated')
        check('20k unique keys recreated',reused['rows'],20000)
        r['binary_after']=identity();check('binary and postmaster unchanged',r['binary_after'],before)
        check('all inputs unchanged',all(sha256(Path(f))==h for f,h in r['inputs'].items()))
        check('original 20k heap reuse bound after vacuum',reused['heap_bytes']<=seed['heap_bytes']+8192)
        r['status']='pass'
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
