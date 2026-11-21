#!/usr/bin/env python3
"""Reclaimable ordinary heap row-head primitives, not a lake RC/GC fix.

Runs only host heap SQL on the exact previously built Debug binary while newer
Native sources can be compiling. No new-source qualification is inferred.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import fcntl
import json
import re
from pathlib import Path
import shutil
import sys
import time
import traceback

ROOT=Path(__file__).resolve().parents[3]
D=ROOT/'integration/opengauss-native-lake'
sys.path[:0]=[str(D),str(D/'tests')]
from inline_fixture import query,scalar
from lake_harness import sha256,validate_run_id,write_json
from native_auto_flush import connect_control
from native_runtime import PROFILE,PORT,RUNTIME,INSTALL
from native_test_context import OwnedDatabase
from permission_api import OwnedRoles,cleanup_roles


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--run-id',required=True)
    p.add_argument('--binary-receipt',type=Path,required=True)
    a=p.parse_args();validate_run_id(a.run_id)
    if PROFILE!='debug':p.error('Requires the owned main Debug runtime')
    out=ROOT/'公共文档/openGauss原生湖存储/验证'/a.run_id;out.mkdir(exist_ok=False)
    r=dict(status='fail',scope=__doc__,assertions=[],events=[],native_source_qualified=False)
    files=[Path(__file__),D/'inline_fixture.py',D/'lake_harness.py',D/'tests/native_auto_flush.py',
        D/'native_runtime.py',D/'native_test_context.py',D/'tests/permission_api.py',D/'tests/inline_cleanup.py']
    archive=out/'executed-inputs';archive.mkdir()
    r['inputs']={str(f):sha256(f)for f in files}
    for f in files:shutil.copy2(f,archive/f.name)
    shutil.copy2(a.binary_receipt,archive/'build.json')
    control=db=roles=reservation=None;workers=[]

    def check(name,got,want=True):
        ok=got==want;r['assertions'].append(dict(name=name,observed=got,expected=want,status='pass'if ok else'fail'))
        write_json(out/'result.json',r)
        if not ok:raise AssertionError(name+': '+repr(got))

    def identity():
        b=json.loads(a.binary_receipt.read_text())
        if b.get('status')!='build_and_sql_passed' or b['profile']!=PROFILE or b['port']!=PORT:
            raise RuntimeError('Wrong successful runtime binary receipt')
        binary=INSTALL/'bin/gaussdb';pid=int((RUNTIME/'data/postmaster.pid').read_text().splitlines()[0])
        running=Path('/proc')/str(pid)/'exe'
        if running.resolve()!=binary.resolve() or sha256(running)!=b['binary_sha256'] or sha256(binary)!=b['binary_sha256']:
            raise RuntimeError('Installed or running binary changed; stop primitive before deployment')
        return dict(binary_sha256=b['binary_sha256'],source_head=b['source_head'],
            receipt=str(a.binary_receipt.resolve()),receipt_sha256=sha256(a.binary_receipt),postmaster_pid=pid)

    def configure(c):
        for sql in ['SET session_timeout=0','SET statement_timeout=45000','SET deadlock_timeout=100']:
            query(c,sql)

    def begin(c):query(c,'BEGIN ISOLATION LEVEL READ COMMITTED')
    def ensure(c,key):return query(c,'INSERT INTO biz.head(row_id) VALUES(%s) ON CONFLICT DO NOTHING RETURNING ctid::text',(key,))
    def lock(c,key):return query(c,'SELECT ctid::text FROM biz.head WHERE row_id=%s FOR UPDATE',(key,))
    def held(c):return query(o,'SELECT locktype,mode,granted,relation,transactionid::text FROM pg_locks WHERE pid=%s ORDER BY locktype,mode',(pids[id(c)],))

    def async_sql(c,sql,args=None):
        def execute():
            try:return dict(state='ok',rows=sql() if callable(sql) else query(c,sql,args))
            except Exception as exc:
                reply=dict(state='error',sqlstate=getattr(exc,'pgcode',None),message=str(exc))
                query(c,'ROLLBACK');return reply
        pool=ThreadPoolExecutor(max_workers=1);job=dict(conn=c,pool=pool,future=pool.submit(execute));workers.append(job);return job

    def wait(job,label):
        until=time.monotonic()+8
        while time.monotonic()<until:
            rows=held(job['conn'])
            if any(not x[2]for x in rows) and not job['future'].done():
                r['events'].append(dict(name=label,locks=rows));check(label+' real lock wait',True);return
            if job['future'].done():raise AssertionError(label+' returned without waiting: '+repr(job['future'].result()))
            time.sleep(.02)
        raise AssertionError(label+' no wait seen')

    def reply(job):
        value=job['future'].result(timeout=10);r['events'].append(dict(name='worker reply',reply=value));return value

    def reset(key):
        for c in [x,y,z,g]:query(c,'ROLLBACK')
        query(x,'DELETE FROM biz.head WHERE row_id=%s',(key,));ensure(x,key)
        return scalar(x,'SELECT ctid::text FROM biz.head WHERE row_id=%s',(key,))

    try:
        reservation=(ROOT/'.reference/opengauss-native-lake/.driver.lock').open('a+')
        fcntl.flock(reservation.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB)
        r['binary_before']=identity();r.update(source_head=r['binary_before']['source_head'],binary_sha256=r['binary_before']['binary_sha256'])
        control=connect_control();configure(control)
        check('dedicated backend lock oracle',scalar(control,'SHOW enable_thread_pool'),'off')
        db=OwnedDatabase(a.run_id,out).create();configure(db.admin);roles=OwnedRoles(control,a.run_id,out)
        x,y,z,g,o=[db.connect()for _ in range(5)]
        for c in [x,y,z,g,o]:configure(c)
        pids={id(c):scalar(c,'SELECT pg_backend_pid()')for c in [x,y,z,g,o]}
        query(x,'CREATE SCHEMA biz')
        query(x,'CREATE TABLE biz.head(row_id bigint PRIMARY KEY) WITH(storage_type=astore,hasuids=false)')
        ensure(x,1);ensure(x,2)

        # A holder makes nonblocking GC skip; queued writers remain serialized.
        begin(x);lock(x,1)
        begin(g)
        check('GC SKIP LOCKED excludes an active writer head',query(g,'SELECT row_id FROM biz.head WHERE row_id=1 FOR UPDATE SKIP LOCKED'),[])
        query(g,'COMMIT')
        begin(y)
        b=async_sql(y,lambda:(ensure(y,1),lock(y,1))[1]);wait(b,'B ensure or tuple lock waits on A')
        query(x,'COMMIT');check('B becomes one holder',reply(b)['state'],'ok')
        begin(z)
        c=async_sql(z,lambda:(ensure(z,1),lock(z,1))[1]);wait(c,'C ensure or tuple lock waits on B')
        query(y,'COMMIT');check('C becomes holder only after B ends',reply(c)['state'],'ok');query(z,'COMMIT')

        # B ensured the old head. GC deletes it; B must not use the old ctid.
        old=reset(1);begin(y);ensure(y,1)
        begin(g);query(g,'DELETE FROM biz.head WHERE row_id=1')
        b=async_sql(y,'SELECT ctid::text FROM biz.head WHERE row_id=1 FOR UPDATE');wait(b,'B waits on GC deletion')
        begin(z);c=async_sql(z,'INSERT INTO biz.head VALUES(1) ON CONFLICT DO NOTHING RETURNING ctid::text');wait(c,'C creation waits on GC deletion')
        query(g,'COMMIT')
        check('native TM_Deleted path yields no locked head',reply(b),dict(state='ok',rows=[]))
        made=reply(c);check('C creates one new physical head',len(made['rows']),1)
        fresh=made['rows'][0][0];check('new head has a different ctid',fresh!=old)
        check('C locks its own newly inserted head',lock(z,1),[(fresh,)])
        b=async_sql(y,'INSERT INTO biz.head VALUES(1) ON CONFLICT DO NOTHING RETURNING ctid::text');wait(b,'B re-ensure waits for new creator C')
        query(z,'COMMIT');check('B re-ensure accepts the unique existing head',reply(b),dict(state='ok',rows=[]))
        check('B refreshes and locks the new head rather than old ctid',lock(y,1),[(fresh,)])
        begin(x);c=async_sql(x,'SELECT ctid::text FROM biz.head WHERE row_id=1 FOR UPDATE');wait(c,'A cannot lock the recreated head while B holds it')
        query(y,'COMMIT');check('A receives same recreated head',reply(c),dict(state='ok',rows=[(fresh,)]));query(x,'COMMIT')

        # GC may also commit entirely between ensure and a fresh lookup.
        old=reset(1);begin(y);ensure(y,1)
        query(g,'DELETE FROM biz.head WHERE row_id=1')
        begin(z);made=ensure(z,1);fresh=made[0][0];lock(z,1)
        check('fresh lookup cannot use an uncommitted replacement head',lock(y,1),[])
        b=async_sql(y,lambda:(ensure(y,1),lock(y,1))[1]);wait(b,'retry ensure waits on replacement creator C')
        query(z,'COMMIT');check('retry returns committed replacement without stale address',reply(b),dict(state='ok',rows=[(fresh,)]));query(y,'COMMIT')

        for action in ['ROLLBACK','ROLLBACK TO child']:
            old=reset(1);begin(y);ensure(y,1);begin(g)
            if action.endswith('child'):query(g,'SAVEPOINT child')
            query(g,'DELETE FROM biz.head WHERE row_id=1')
            b=async_sql(y,'SELECT ctid::text FROM biz.head WHERE row_id=1 FOR UPDATE');wait(b,'GC '+action)
            query(g,action)
            check('delete rollback restores the original lock carrier '+action,reply(b),dict(state='ok',rows=[(old,)]))
            query(y,'COMMIT');query(g,'ROLLBACK')

        reset(1);begin(x);lock(x,1);begin(g)
        deletion=async_sql(g,'DELETE FROM biz.head WHERE row_id=1');wait(deletion,'cancelable native delete waiter')
        g.cancel();check('canceled delete uses native SQLSTATE',reply(deletion).get('sqlstate'),'57014')
        check('canceled deletion leaves no outstanding wait',[row for row in held(g)if not row[2]],[])
        query(x,'COMMIT');check('canceled deletion preserves one head',scalar(o,'SELECT count(*) FROM biz.head WHERE row_id=1'),1)

        reset(1);reset(2);begin(x);begin(g);lock(x,1);query(g,'DELETE FROM biz.head WHERE row_id=2')
        b=async_sql(x,'SELECT ctid::text FROM biz.head WHERE row_id=2 FOR UPDATE');wait(b,'writer/collector first deadlock edge')
        c=async_sql(g,'DELETE FROM biz.head WHERE row_id=1')
        replies=[reply(j)for j in [b,c]]
        check('native deadlock detector aborts one participant',sorted(v.get('sqlstate','ok')for v in replies),['40P01','ok'])
        query(x,'ROLLBACK');query(g,'ROLLBACK')
        check('deadlock rollback restores both head keys',query(o,'SELECT row_id FROM biz.head WHERE row_id IN(1,2) ORDER BY row_id'),[(1,),(2,)])

        # Logical reclamation and physical reuse are distinct. Vacuum the
        # owned ordinary heap; do not call lake flush or physical-file GC.
        query(x,'INSERT INTO biz.head SELECT i FROM generate_series(100001,120000)i')
        def sizes(stage):
            count=int(scalar(o,'SELECT count(*) FROM biz.head WHERE row_id BETWEEN 100001 AND 120000'))
            heap,index,total=query(o,"SELECT pg_relation_size('biz.head'),pg_indexes_size('biz.head'),pg_total_relation_size('biz.head')")[0]
            image=dict(stage=stage,rows=count,heap_bytes=int(heap),index_bytes=int(index),total_bytes=int(total),
                total_bytes_per_live_row=(float(total)/count if count else None))
            r.setdefault('space_samples',[]).append(image);return image
        seed=sizes('20000 hot heads plus two control keys')
        begin(x);r['deleting_xid']=str(scalar(x,'SELECT txid_current()'))
        query(x,'DELETE FROM biz.head WHERE row_id BETWEEN 100001 AND 120000 AND row_id%2=0');query(x,'COMMIT')
        deleted=sizes('logical deletion committed; dead physical versions remain')
        check('bounded batch removed exactly half the hot heads',deleted['rows'],10000)
        for actor in [x,y,z,g,o]:
            query(actor,'ROLLBACK');actor.close()
        x=db.connect();configure(x);o=x
        query(x,'SET client_min_messages=info')
        r['space_settings']=query(x,"SELECT name,setting,context FROM pg_settings WHERE name IN('vacuum_defer_cleanup_age','defer_csn_cleanup_time','enable_defer_calculate_snapshot','enable_vacuum_extreme_xmin') ORDER BY name")
        r['vacuum_notices']=[];started=time.monotonic();ready=False
        while True:
            x.notices.clear();query(x,'VACUUM VERBOSE biz.head');text='\n'.join(x.notices)
            dead=re.search(r'(\d+) dead row versions cannot be removed yet',text)
            cutoff=re.search(r'OldestXid (\d+)',text)
            r['vacuum_notices'].append(dict(elapsed_seconds=time.monotonic()-started,notices=list(x.notices)))
            ready=dead is not None and int(dead.group(1))==0 and cutoff is not None and int(cutoff.group(1))>int(r['deleting_xid'])
            write_json(out/'result.json',r)
            if ready or time.monotonic()-started>=20:break
            time.sleep(.5)
        check('actual native reclamation horizon passed deletion XID',ready)
        vacuum=sizes('ordinary heap vacuum after actual safe-removal cutoff')
        query(x,'INSERT INTO biz.head SELECT i FROM generate_series(100001,120000)i WHERE i%2=0')
        reused=sizes('same stable keys recreated after reclamation')
        check('all stable keys recreated uniquely',reused['rows'],20000)
        check('vacuumed heap pages are reused by the next hot batch',reused['heap_bytes']<=seed['heap_bytes']+8192)
        check('all executed probe inputs unchanged',all(sha256(Path(f))==h for f,h in r['inputs'].items()))
        r['binary_after']=identity();check('binary and postmaster unchanged during primitive',r['binary_after'],r['binary_before'])
        r['status']='pass'
    except BaseException as exc:r.update(reason=str(exc),traceback=traceback.format_exc())
    finally:
        for j in workers:
            if not j['future'].done():
                try:j['conn'].cancel()
                except Exception:pass
        for j in workers:j['pool'].shutdown(wait=True)
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
