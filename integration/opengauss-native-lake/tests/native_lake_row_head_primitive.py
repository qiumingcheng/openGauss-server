#!/usr/bin/env python3
"""Ordinary heap row-head locking primitives; not a lake RC/EPQ implementation.

Single-instance Debug only. No lake DML, flush, target RAC or performance claim.
The candidate design preserves these row heads across flush; that lifecycle is
not implemented or tested by this independent storage primitive test.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import fcntl
from pathlib import Path
import shutil
import sys
import time
import traceback

ROOT = Path(__file__).resolve().parents[3]
D = ROOT / 'integration/opengauss-native-lake'
sys.path[:0] = [str(D), str(D / 'tests')]
from file_dml_acceptance import check_build
from inline_fixture import query, scalar
from lake_harness import sha256, validate_run_id, write_json
from native_auto_flush import connect_control
from native_runtime import PROFILE
from native_test_context import OwnedDatabase
from permission_api import OwnedRoles, cleanup_roles


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--run-id', required=True)
    p.add_argument('--build-receipt', required=True, type=Path)
    a = p.parse_args()
    validate_run_id(a.run_id)
    if PROFILE != 'debug':
        p.error('Owned main Debug endpoint required')
    out = ROOT / '公共文档/openGauss原生湖存储/验证' / a.run_id
    out.mkdir(exist_ok=False)
    r = dict(status='fail', scope=__doc__, assertions=[], events=[])
    inputs = [Path(__file__), D / 'tests/file_dml_acceptance.py', D / 'inline_fixture.py',
              D / 'lake_harness.py', D / 'tests/native_auto_flush.py', D / 'native_runtime.py',
              D / 'native_test_context.py', D / 'tests/permission_api.py', D / 'tests/inline_cleanup.py']
    r['inputs'] = {str(f): sha256(f) for f in inputs}
    archive = out / 'executed-inputs'
    archive.mkdir()
    for f in inputs:
        shutil.copy2(f, archive / f.name)
    shutil.copy2(a.build_receipt, archive / 'build.json')
    control = db = roles = reservation = None
    workers = []

    def check(name, got, want=True):
        ok = got == want
        r['assertions'].append(dict(name=name, observed=got, expected=want,
                                    status='pass' if ok else 'fail'))
        write_json(out / 'result.json', r)
        if not ok:
            raise AssertionError(name + ': ' + repr(got))

    def configure(c):
        query(c, 'SET session_timeout=0')
        query(c, 'SET statement_timeout=60000')
        query(c, 'SET deadlock_timeout=100')

    def begin(c):
        query(c, 'BEGIN ISOLATION LEVEL READ COMMITTED')

    def locks(c):
        return query(observer, 'SELECT locktype,mode,granted,relation,transactionid::text '
                     'FROM pg_locks WHERE pid=%s ORDER BY locktype,mode,granted',
                     (pids[id(c)],))

    def asynchronous(c, statement):
        def run():
            try:
                return dict(state='ok', rows=query(c, statement))
            except Exception as exc:
                code = getattr(exc, 'pgcode', None)
                query(c, 'ROLLBACK')
                return dict(state='error', sqlstate=code, message=str(exc))
        pool = ThreadPoolExecutor(max_workers=1)
        item = dict(conn=c, pool=pool, future=pool.submit(run))
        workers.append(item)
        return item

    def wait(item, name):
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            rows = locks(item['conn'])
            waiting = [row for row in rows if not row[2]]
            if waiting and not item['future'].done():
                r['events'].append(dict(name=name, waiting=waiting, all_locks=rows))
                check(name + ' has a real ungranted lock', bool(waiting))
                return
            if item['future'].done():
                raise AssertionError(name + ' returned before release: ' + repr(item['future'].result()))
            time.sleep(.02)
        raise AssertionError(name + ' no lock wait observed')

    def finish(item, expected):
        got = item['future'].result(timeout=12)
        check('worker result', got, expected)
        return got

    try:
        reservation = (ROOT / '.reference/opengauss-native-lake/.driver.lock').open('a+')
        fcntl.flock(reservation.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        built = check_build(a.build_receipt)
        r.update(source_head=built['source_head'], binary_sha256=built['binary_sha256'],
                 build=str(a.build_receipt.resolve()), build_sha256=sha256(a.build_receipt))
        control = connect_control()
        configure(control)
        check('dedicated backend lock oracle', scalar(control, 'SHOW enable_thread_pool'), 'off')
        check('automatic scheduler unchanged and off', scalar(control, 'SHOW enable_lake_maintenance'), 'off')
        db = OwnedDatabase(a.run_id, out).create()
        configure(db.admin)
        roles = OwnedRoles(control, a.run_id, out)
        x, y, z, observer = [db.connect() for _ in range(4)]
        for c in [x, y, z, observer]:
            configure(c)
        pids = {id(c): scalar(c, 'SELECT pg_backend_pid()') for c in [x, y, z, observer]}
        r['owned_backend_pids'] = list(pids.values())
        query(x, 'CREATE SCHEMA biz')
        query(x, 'CREATE TABLE biz.row_head(row_id bigint PRIMARY KEY) WITH(storage_type=astore,hasuids=false)')
        options = scalar(x, "SELECT reloptions FROM pg_class WHERE oid='biz.row_head'::regclass")
        check('heap row heads explicitly disable UIDs', 'hasuids=false' in options)
        check('row heads use ordinary heap', 'storage_type=astore' in options)

        for key, release, expected in [(1, 'COMMIT', []), (2, 'ROLLBACK', [(2,)])]:
            begin(x)
            query(x, 'INSERT INTO biz.row_head VALUES(%s)', (key,))
            begin(y)
            job = asynchronous(y, 'INSERT INTO biz.row_head VALUES(' + str(key) + ') ON CONFLICT DO NOTHING RETURNING row_id')
            wait(job, 'lazy unique creation ' + release)
            query(x, release)
            finish(job, dict(state='ok', rows=expected))
            check('fresh statement sees exactly one head', query(y, 'SELECT row_id FROM biz.row_head WHERE row_id=%s FOR UPDATE', (key,)), [(key,)])
            query(y, 'COMMIT')
        for release in ['COMMIT', 'ROLLBACK']:
            begin(x)
            query(x, 'SELECT row_id FROM biz.row_head WHERE row_id=1 FOR UPDATE')
            query(x, 'SELECT row_id FROM biz.row_head WHERE row_id=1 FOR UPDATE')
            begin(y)
            job = asynchronous(y, 'SELECT row_id FROM biz.row_head WHERE row_id=1 FOR UPDATE')
            wait(job, 'existing tuple ' + release)
            query(x, release)
            finish(job, dict(state='ok', rows=[(1,)]))
            query(y, 'COMMIT')

        begin(x)
        query(x, 'SELECT row_id FROM biz.row_head WHERE row_id=1 FOR UPDATE')
        query(x, 'SAVEPOINT child')
        query(x, 'SELECT row_id FROM biz.row_head WHERE row_id=2 FOR UPDATE')
        begin(y)
        child = asynchronous(y, 'SELECT row_id FROM biz.row_head WHERE row_id=2 FOR UPDATE')
        wait(child, 'savepoint child tuple')
        query(x, 'ROLLBACK TO child')
        finish(child, dict(state='ok', rows=[(2,)]))
        query(y, 'COMMIT')
        begin(z)
        parent = asynchronous(z, 'SELECT row_id FROM biz.row_head WHERE row_id=1 FOR UPDATE')
        wait(parent, 'parent tuple survives child rollback')
        query(x, 'COMMIT')
        finish(parent, dict(state='ok', rows=[(1,)]))
        query(z, 'COMMIT')

        begin(x)
        query(x, 'SELECT row_id FROM biz.row_head WHERE row_id=1 FOR UPDATE')
        begin(y)
        job = asynchronous(y, 'SELECT row_id FROM biz.row_head WHERE row_id=1 FOR UPDATE')
        wait(job, 'cancel waiter')
        y.cancel()
        reply = job['future'].result(timeout=12)
        r['events'].append(dict(name='cancel result', reply=reply))
        check('cancel preserves standard SQLSTATE', reply.get('sqlstate'), '57014')
        check('canceled transaction releases all waits', [row for row in locks(y) if not row[2]], [])
        begin(z)
        job = asynchronous(z, 'SELECT row_id FROM biz.row_head WHERE row_id=1 FOR UPDATE')
        wait(job, 'cancel leaves holder intact')
        query(x, 'COMMIT')
        finish(job, dict(state='ok', rows=[(1,)]))
        query(z, 'COMMIT')

        begin(x)
        begin(y)
        query(x, 'SELECT row_id FROM biz.row_head WHERE row_id=1 FOR UPDATE')
        query(y, 'SELECT row_id FROM biz.row_head WHERE row_id=2 FOR UPDATE')
        one = asynchronous(x, 'SELECT row_id FROM biz.row_head WHERE row_id=2 FOR UPDATE')
        wait(one, 'first edge of deadlock')
        two = asynchronous(y, 'SELECT row_id FROM biz.row_head WHERE row_id=1 FOR UPDATE')
        replies = [j['future'].result(timeout=12) for j in [one, two]]
        r['events'].append(dict(name='deadlock replies', replies=replies))
        check('native detector chooses exactly one deadlock victim', sorted(v.get('sqlstate', 'ok') for v in replies), ['40P01', 'ok'])
        query(x, 'ROLLBACK')
        query(y, 'ROLLBACK')

        query(x, 'INSERT INTO biz.row_head SELECT i FROM generate_series(100001,120000)i')
        begin(x)
        before = locks(x)
        count = len(query(x, 'SELECT row_id FROM biz.row_head WHERE row_id BETWEEN 100001 AND 120000 FOR UPDATE'))
        after = locks(x)
        r['events'].append(dict(name='20k heap header locks', before=before, after=after))
        check('all 20k stable row heads locked', count, 20000)
        check('20k row locks leave bounded shared lock entries', len(after) < 64)
        check('no permanent heavyweight tuple or UID locks after batch',
              [row for row in after if row[0] in ['tuple', 'uid']], [])
        check('all 20k row headers contain the holder XID', scalar(x,
              'SELECT count(*) FROM biz.row_head WHERE row_id BETWEEN 100001 AND 120000 AND xmax::text=txid_current()::text'), 20000)
        query(x, 'ROLLBACK')
        begin(y)
        check('batch abort releases tuple headers', query(y, 'SELECT row_id FROM biz.row_head WHERE row_id IN(100001,120000) ORDER BY row_id FOR UPDATE'), [(100001,), (120000,)])
        query(y, 'COMMIT')
        check('executed input hashes unchanged', all(sha256(Path(f)) == h for f, h in r['inputs'].items()))
        check_build(a.build_receipt)
        r['status'] = 'pass'
    except BaseException as exc:
        r.update(reason=str(exc), traceback=traceback.format_exc())
    finally:
        for job in workers:
            if not job['future'].done():
                try:
                    job['conn'].cancel()
                except Exception:
                    pass
        for job in workers:
            job['pool'].shutdown(wait=True)
        if db is not None:
            try:
                db.cleanup()
                r['database_cleanup'] = db.cleanup_result
            except BaseException as exc:
                r.update(status='fail', cleanup_error=str(exc))
        if roles is not None:
            try:
                r['role_cleanup'] = cleanup_roles(control, roles.path, a.run_id)
            except BaseException as exc:
                r.update(status='fail', role_cleanup_error=str(exc))
        if control is not None:
            control.close()
        if reservation is not None:
            reservation.close()
        write_json(out / 'result.json', r)
    print(str(out / 'result.json') + ': ' + r['status'])
    return 0 if r['status'] == 'pass' else 1


if __name__ == '__main__':
    sys.exit(main())
