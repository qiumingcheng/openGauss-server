#!/usr/bin/env python3
"""Actual native candidate commit/abort/savepoint lifecycle, no lake publication.

The test wrapper only imports gaussdb implementations. Marking a test candidate
published is an explicit simulation of the mark, never evidence of catalog flush.
"""
import argparse
import json
import os
from pathlib import Path
import shutil
import sys
import threading
import time
import traceback
import uuid

ROOT = Path('/home/qiumc/data/code/cpp')
DRIVER = ROOT / 'integration/opengauss-native-lake'
sys.path[:0] = [str(DRIVER), str(DRIVER / 'tests')]
from file_dml_acceptance import check_build
from inline_fixture import query, scalar
from lake_harness import sha256, write_json
from native_test_context import OwnedDatabase


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--build-receipt', type=Path, required=True)
    parser.add_argument('--probe-receipt', type=Path, required=True)
    parser.add_argument('--old-provider', action='store_true')
    args = parser.parse_args()
    out = ROOT / '公共文档/openGauss原生湖存储/验证' / args.run_id
    out.mkdir(exist_ok=False)
    result = {'status': 'fail', 'scope': 'Native finished-candidate lifecycle; no catalog flush/publication',
              'assertions': [], 'limitations': ['Published mark is test-only; no catalog publication.',
              'No crash, durable orphan recovery, remote or RAC qualification.']}
    sources = [Path(__file__), DRIVER / 'native_test_context.py', DRIVER / 'inline_fixture.py',
               DRIVER / 'tests/build_native_arrow_probe.py', DRIVER / 'tests/native_candidate_probe.cpp']
    result['input_hashes'] = {str(p): sha256(p) for p in sources}
    archive = out / 'executed-inputs'
    archive.mkdir()
    for p in sources:
        shutil.copy2(p, archive / p.name)
    db = None
    probe_install = None

    def check(name, observed, expected=True):
        ok = observed == expected
        result['assertions'].append({'name': name, 'status': 'pass' if ok else 'fail',
                                     'observed': observed, 'expected': expected})
        if not ok:
            raise AssertionError(name)

    def error(conn, sql, code, params=None, message=None):
        try:
            query(conn, sql, params)
        except Exception as exc:
            check('expected SQLSTATE: ' + sql[:65], getattr(exc, 'pgcode', None), code)
            if message:
                check('exact rejection reason: ' + message, message in str(exc))
            return
        raise AssertionError('Expected SQL failure: ' + sql)

    try:
        from psycopg2 import sql
        build = check_build(args.build_receipt)
        receipt = json.loads(args.probe_receipt.read_text())
        check('probe belongs to actual built gaussdb', receipt['native_binary_sha256'], build['binary_sha256'])
        check('compiled wrapper bytes match', sha256(receipt['library']), receipt['library_sha256'])
        check('all wrapper headers match', all(sha256(p) == h for p, h in receipt['headers'].items()))
        result.update(source_head=build['source_head'], binary_sha256=build['binary_sha256'],
                      build=str(args.build_receipt.resolve()), build_sha256=sha256(args.build_receipt),
                      probe_receipt=str(args.probe_receipt.resolve()), probe_receipt_sha256=sha256(args.probe_receipt))
        # Never overwrite a loaded DSO. The small owned installation is retained
        # until a later postmaster restart; its path, identity and bytes are saved.
        name = 'lake_candidate_probe_' + uuid.uuid4().hex
        probe_install = ROOT / '.install/opengauss-native-lake-debug/lib/postgresql' / name
        probe_install.mkdir(mode=0o750, exist_ok=False)
        target = probe_install / 'native_candidate_probe.so'
        shutil.copy2(receipt['library'], target)
        owner = {'owner': args.run_id, 'path': str(probe_install), 'library_sha256': sha256(target),
                 'sql_module_path': '$libdir/' + name + '/native_candidate_probe'}
        write_json(probe_install / 'owner.json', owner)
        write_json(out / 'probe-installation.json', owner)
        provider = ROOT / '.install/opengauss-native-lake-debug/lib/postgresql/og_lake_sdk/libog_lake_sdk_arrow.so'
        result['provider_sha256'] = sha256(provider)
        db = OwnedDatabase(args.run_id, out).create()
        conn = db.connect()
        query(conn, 'CREATE SCHEMA candidate_probe')
        query(conn, sql.SQL('CREATE FUNCTION candidate_probe.write(text,boolean,boolean,integer) RETURNS bigint '
                           'AS {},{} LANGUAGE C STRICT').format(sql.Literal(owner['sql_module_path']),
                                                               sql.Literal('lake_test_candidate_write')))
        files = out / 'candidates'
        files.mkdir()
        postmaster = int((ROOT / '.runtime/opengauss-native-lake/debug/data/postmaster.pid').read_text().splitlines()[0])

        def held():
            found = []
            for fd in Path('/proc/' + str(postmaster) + '/fd').iterdir():
                try:
                    value = os.readlink(fd)
                except FileNotFoundError:
                    continue
                if value == str(files) or value.startswith(str(files) + '/'):
                    found.append(value)
            return sorted(found)

        def path(name):
            return files / (name + '.parquet')

        def prepare(name, finish=True, published=False, fail=0, connection=conn):
            return scalar(connection, 'SELECT candidate_probe.write(%s,%s,%s,%s)',
                          (str(path(name)), finish, published, fail))

        check('no initial owned candidate descriptors', held(), [])
        if args.old_provider:
            error(conn, 'SELECT candidate_probe.write(%s,true,false,0)', '0A000',
                  (str(path('unsupported')),), 'finished-candidate abort support')
            check('old provider rejected before file creation', path('unsupported').exists(), False)
            check('old provider rejection leaked no descriptors', held(), [])
        else:
            check('finished unpublished statement returned rows', prepare('unpublished'), 2)
            check('commit discards unpublished finished candidate', path('unpublished').exists(), False)
            prepare('unfinished', finish=False)
            check('commit discards unfinished candidate', path('unfinished').exists(), False)
            prepare('published', published=True)
            check('commit retains marked finished candidate', path('published').is_file())
            check('commit releases every candidate descriptor', held(), [])
            before = sha256(path('published'))
            error(conn, 'SELECT candidate_probe.write(%s,true,false,0)', 'HV000', (str(path('published')),))
            check('exclusive-open error preserves existing file', sha256(path('published')), before)
            query(conn, 'BEGIN')
            prepare('rollback', published=True)
            check('finished candidate retains inode and parent until outcome', len(held()), 2)
            query(conn, 'ROLLBACK')
            check('rollback discards even marked finished candidate', path('rollback').exists(), False)
            check('rollback releases every descriptor', held(), [])
            query(conn, 'BEGIN')
            prepare('parent', published=True)
            query(conn, 'SAVEPOINT child')
            prepare('child', published=True)
            query(conn, 'ROLLBACK TO child')
            check('child rollback discards child candidate', path('child').exists(), False)
            check('child rollback preserves parent candidate', path('parent').is_file())
            check('only parent inode and directory descriptors remain', len(held()), 2)
            query(conn, 'COMMIT')
            check('parent candidate retained at commit', path('parent').is_file())
            query(conn, 'BEGIN')
            query(conn, 'SAVEPOINT outer_child')
            query(conn, 'SAVEPOINT inner_child')
            prepare('adopted', published=True)
            query(conn, 'RELEASE inner_child')
            query(conn, 'RELEASE outer_child')
            query(conn, 'ROLLBACK')
            check('nested released candidate adopted into parent abort', path('adopted').exists(), False)
            error(conn, 'SELECT candidate_probe.write(%s,true,true,1)', 'P0001', (str(path('error')),))
            check('statement error after finish discards candidate', path('error').exists(), False)
            error(conn, 'SELECT candidate_probe.write(%s,false,true,0)', '55000', (str(path('invalid_mark')),))
            check('unfinished publication rejected and candidate removed', path('invalid_mark').exists(), False)
            query(conn, 'BEGIN')
            for i in range(16):
                prepare('bounded-' + str(i), published=True)
            check('16 finished candidates retain bounded 32 descriptors', len(held()), 32)
            error(conn, 'SELECT candidate_probe.write(%s,true,true,0)', '54000', (str(path('over_limit')),))
            query(conn, 'ROLLBACK')
            check('limit failure removes all earlier candidates',
                  [path('bounded-' + str(i)).exists() for i in range(16)], [False] * 16)
            check('limit rejected before seventeenth file created', path('over_limit').exists(), False)
            check('limit rollback released all descriptors', held(), [])
            query(conn, 'BEGIN')
            prepare('cancelled', published=True)
            failures = []
            started = threading.Event()

            def wait_query():
                started.set()
                try:
                    query(conn, 'SELECT pg_sleep(20)')
                except Exception as exc:
                    failures.append(getattr(exc, 'pgcode', None))

            worker = threading.Thread(target=wait_query)
            worker.start()
            if not started.wait(2):
                raise AssertionError('sleep worker did not start')
            observer = db.connect()
            pid = scalar(observer, 'SELECT pg_backend_pid()')
            # The connection already prepared a candidate. Cancel a real running
            # SQL command; do not replace cancellation with an invented exception.
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                active = scalar(observer, "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() "
                                          "AND pid<>%s AND state='active' AND query='SELECT pg_sleep(20)'", (pid,))
                if active:
                    break
                time.sleep(.02)
            check('actual cancellable query became active', bool(active))
            conn.cancel()
            worker.join(5)
            check('real query cancellation returned', worker.is_alive(), False)
            check('real query cancellation SQLSTATE', failures, ['57014'])
            query(conn, 'ROLLBACK')
            check('cancel rollback discards finished candidate', path('cancelled').exists(), False)
            check('cancel rollback releases all descriptors', held(), [])
            # Close our own backend normally with a transaction still open.
            quitter = db.connect()
            query(quitter, 'BEGIN')
            prepare('disconnect', published=True, connection=quitter)
            quitter.close()
            deadline = time.monotonic() + 5
            while path('disconnect').exists() and time.monotonic() < deadline:
                time.sleep(.02)
            check('backend disconnect rolls candidate back', path('disconnect').exists(), False)
            check('backend disconnect releases descriptors', held(), [])
            check('ordinary native SQL remains available', scalar(observer, 'SELECT 6*7'), 42)
        check('all executed test inputs remain unchanged', all(sha256(p) == h for p, h in result['input_hashes'].items()))
        check_build(args.build_receipt)
        result['status'] = 'pass'
    except BaseException as exc:
        result.update(reason=str(exc), traceback=traceback.format_exc())
    finally:
        if db is not None:
            try:
                db.cleanup()
                result['database_cleanup'] = db.cleanup_result
            except BaseException as exc:
                result.update(status='fail', cleanup_error=str(exc))
        write_json(out / 'result.json', result)
    print(json.dumps({'status': result['status'], 'reason': result.get('reason'),
                      'checks': len(result['assertions']), 'result': str(out / 'result.json')}))
    return 0 if result['status'] == 'pass' else 1


if __name__ == '__main__':
    raise SystemExit(main())
