#!/usr/bin/env python3
"""Real retirement xmin versus a late first lake access within one RC statement.

Uses genuine native file-delete replacement and the actual native VACUUM
watermark entry. Performs no physical deletion and supplies no RAC proof.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
import traceback
import uuid
from types import SimpleNamespace

ROOT = Path('/home/qiumc/data/code/cpp')
DRIVER = ROOT / 'integration/opengauss-native-lake'
sys.path[:0] = [str(DRIVER), str(DRIVER / 'tests')]
from file_dml_acceptance import check_build
from inline_fixture import query, scalar
from lake_harness import sha256, validate_run_id, write_json
from native_table_create import Case
from native_test_context import OwnedDatabase
from permission_api import OwnedRoles, cleanup_roles


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--run-id', required=True)
    p.add_argument('--build-receipt', type=Path, required=True)
    p.add_argument('--probe-build', type=Path, required=True)
    p.add_argument('--wait-for', type=Path)
    args = p.parse_args()
    validate_run_id(args.run_id)
    out = ROOT / '公共文档/openGauss原生湖存储/验证' / args.run_id
    out.mkdir(exist_ok=False)
    result = {'status': 'fail', 'scope': 'RC statement paused inside STABLE function before first lake access, with real retirement xmin',
              'assertions': [], 'limitations': ['No physical deletion, backup/PITR pin or global target-RAC qualification.']}
    inputs = [Path(__file__), DRIVER / 'native_test_context.py', DRIVER / 'tests/native_table_create.py',
              DRIVER / 'tests/permission_api.py', DRIVER / 'inline_fixture.py']
    result['inputs'] = {str(x): sha256(x) for x in inputs}
    archive = out / 'executed-inputs'
    archive.mkdir()
    for x in inputs:
        shutil.copy2(x, archive / x.name)
    db = roles = control = holder = executor = future = test = None

    def check(name, observed, expected=True):
        ok = observed == expected
        result['assertions'].append({'name': name, 'observed': observed,
                                     'expected': expected, 'status': 'pass' if ok else 'fail'})
        if not ok:
            raise AssertionError(name)

    try:
        if args.wait_for:
            deadline = time.monotonic() + 3600
            while True:
                state = json.loads(args.wait_for.read_text())
                if state['status'] == 'passed':
                    break
                if state['status'] == 'failed':
                    raise RuntimeError('Prior native regression failed; no watermark test started')
                if time.monotonic() > deadline:
                    raise TimeoutError('Waiting exclusive native recovery/regression window')
                time.sleep(3)
        import psycopg2
        from psycopg2 import sql
        build = check_build(args.build_receipt)
        result.update(source_head=build['source_head'], binary_sha256=build['binary_sha256'])
        probe = json.loads(args.probe_build.read_text())
        check('real native watermark wrapper built unloaded', probe['status'], 'built_unloaded')
        check('wrapper targets exact native binary', probe['native_binary_sha256'], build['binary_sha256'])
        check('wrapper library unchanged', sha256(probe['library']), probe['library_sha256'])
        check('all compiled native header inputs unchanged', all(sha256(p) == h for p, h in probe['headers'].items()))
        with (out / 'fixture.log').open('w') as log:
            subprocess.run([sys.executable, str(DRIVER / 'tests/catalog_fixture_bundle.py'),
                            '--run-id', args.run_id, '--output', str(out / 'fixture/manifest.json')],
                           stdout=log, stderr=subprocess.STDOUT, check=True, timeout=300)
        control = psycopg2.connect(host=str(ROOT / '.runtime/opengauss-native-lake/debug/socket'),
                                  port=55560, dbname='postgres', user=os.getenv('USER', 'qiumc'))
        control.autocommit = True
        db = OwnedDatabase(args.run_id, out).create()
        roles = OwnedRoles(control, args.run_id, out)
        test = Case(db, SimpleNamespace(mode='net_dml', sql_version='0.11', manifest=out / 'fixture/manifest.json',
                                       output=out / 'result.json', run_id=args.run_id), result, roles)
        install = ROOT / '.install/opengauss-native-lake-debug/lib/postgresql' / ('lake_gc_xmin_probe_' + uuid.uuid4().hex)
        install.mkdir(mode=0o750, exist_ok=False)
        target = install / 'native_gc_xmin_probe.so'
        shutil.copy2(probe['library'], target)
        owner = {'owner': args.run_id, 'path': str(install), 'library_sha256': sha256(target),
                 'sql_module_path': '$libdir/' + install.name + '/native_gc_xmin_probe'}
        write_json(install / 'owner.json', owner)
        write_json(out / 'probe-installation.json', owner)
        result['probe_installation'] = owner
        query(test.a, 'CREATE SCHEMA gc_probe')
        for name, signature, output, entry in [
                ('oldest', 'regclass', 'bigint', 'lake_test_gc_oldest_xmin'),
                ('precedes', 'xid,xid', 'boolean', 'lake_test_gc_xid_precedes')]:
            query(test.a, sql.SQL('CREATE FUNCTION gc_probe.{}(' + signature + ') RETURNS ' + output +
                  ' AS {},{} LANGUAGE C STRICT VOLATILE NOT FENCED').format(
                      sql.Identifier(name), sql.Literal(owner['sql_module_path']), sql.Literal(entry)))
            query(test.a, 'REVOKE ALL ON FUNCTION gc_probe.' + name + '(' + signature + ') FROM PUBLIC')
        test.create()
        query(test.a, 'INSERT INTO biz.orders VALUES(1,10),(2,20),(3,30)')
        flush = "SELECT og_lake.flush('biz.orders'::regclass)"
        query(test.a, flush)
        query(test.a, 'DELETE FROM biz.orders WHERE id=2')
        query(test.a, flush)
        old_path = scalar(test.a, 'SELECT path FROM created_metadata.ducklake_delete_file')
        root = Path(test.empty_manifest['data_path'])
        old_files = list(root.rglob(old_path))
        check('genuine first deletion object exists', len(old_files), 1)
        old_hash = sha256(old_files[0])
        # A STABLE function reuses the caller's RC statement snapshot, but
        # prepares its lake query only after the first PERFORM lock finishes.
        key = 781346700123
        query(test.a, "CREATE FUNCTION biz.late_rc_read() RETURNS TABLE(id integer,val integer,delete_path text) "
              "LANGUAGE plpgsql STABLE AS $$ BEGIN PERFORM pg_catalog.pg_advisory_lock(781346700123); "
              "RETURN QUERY SELECT l.id,l.val,(SELECT d.path::text FROM created_metadata.ducklake_delete_file d) "
              "FROM biz.orders l ORDER BY l.id; END $$")
        holder = db.connect()
        query(holder, 'SELECT pg_advisory_lock(%s)', (key,))
        query(test.b, 'BEGIN ISOLATION LEVEL READ COMMITTED')
        reader_pid = scalar(test.b, 'SELECT pg_backend_pid()')
        query(test.b, 'SET statement_timeout=30000')
        executor = ThreadPoolExecutor(max_workers=1)
        future = executor.submit(query, test.b, 'SELECT * FROM biz.late_rc_read()')
        deadline = time.monotonic() + 10
        while True:
            waiting = query(test.observer, "SELECT pid,mode FROM pg_locks WHERE pid=%s "
                            "AND locktype='advisory' AND NOT granted", (reader_pid,))
            if waiting or future.done(): break
            if time.monotonic() > deadline: raise TimeoutError('RC statement never reached its real lock barrier')
            time.sleep(.02)
        check('actual RC statement paused before first lake query', waiting, [(reader_pid, 'ExclusiveLock')])
        query(test.a, 'DELETE FROM biz.orders WHERE id=1')
        query(test.a, flush)
        retire = query(test.observer, 'SELECT data_file_id,path,xmin::text FROM '
                       'created_metadata.ducklake_files_scheduled_for_deletion')
        check('one real standard retirement row', len(retire), 1)
        retirement_xid = int(retire[0][2])
        server = scalar(test.observer, "SELECT oid FROM pg_foreign_server WHERE srvname='created_lake'")
        query(test.observer, 'BEGIN')
        check('catalog-exclusive alone cannot detect RC statement before its lake access',
              scalar(test.observer, 'SELECT og_lake_internal.test_catalog_guard_acquire(%s::oid,true)', (server,)))
        def oldest(conn):
            return scalar(conn, "SELECT gc_probe.oldest('created_metadata.ducklake_files_scheduled_for_deletion'::regclass)")
        def precedes(conn, first, second):
            return scalar(conn, 'SELECT gc_probe.precedes(%s::xid,%s::xid)', (str(first), str(second)))
        water = oldest(test.observer)
        result.update(retirement_xid=retirement_xid, oldest_with_paused_rc_statement=water)
        check('real host watermark defers retirement while RC statement is paused',
              precedes(test.observer, retirement_xid, water), False)
        check('old object remains intact before deferred GC releases protection', sha256(old_files[0]), old_hash)
        query(test.observer, 'COMMIT')
        check('release only the owned statement barrier', scalar(holder, 'SELECT pg_advisory_unlock(%s)', (key,)))
        check('first lake access uses legitimate old RC statement state and old file metadata',
              future.result(timeout=10), [(1,10,old_path),(3,30,old_path)])
        query(test.b, 'SELECT pg_advisory_unlock(%s)', (key,))
        check('fresh RC reader sees the new committed lake state', test.rows(test.observer), [(3,30)])
        check('needed physical old deletion object still intact', sha256(old_files[0]), old_hash)
        query(test.b, 'COMMIT')
        for first, second, expected in [(4294967299, 4294967300, True),
                                         (4294967300, 4294967299, False),
                                         (4294967300, 4294967300, False),
                                         (3, 4294967300, True), (2, 4294967300, True)]:
            check('native full-width xid comparison ' + str((first, second)), precedes(test.observer, first, second), expected)
        check('no physical deletion performed by this primitive test', sha256(old_files[0]), old_hash)
        check('retirement row remains for actual guarded GC implementation',
              query(test.observer, 'SELECT data_file_id,path,xmin::text FROM created_metadata.ducklake_files_scheduled_for_deletion'), retire)
        check('test input bytes remain fixed', all(sha256(p) == h for p, h in result['inputs'].items()))
        check_build(args.build_receipt)
        result['status'] = 'pass'
    except BaseException as exc:
        result.update(reason=str(exc), traceback=traceback.format_exc())
    finally:
        if holder is not None:
            try: query(holder, 'SELECT pg_advisory_unlock_all()')
            except BaseException: pass
            holder.close()
        if future is not None and not future.done() and test is not None:
            try: test.b.cancel()
            except BaseException: pass
        if executor is not None: executor.shutdown(wait=True)
        if db is not None:
            try:
                db.cleanup()
                result['database_cleanup'] = db.cleanup_result
            except BaseException as exc:
                result.update(status='fail', cleanup_error=str(exc))
        if roles is not None:
            try:
                if not result.get('database_cleanup') or result['database_cleanup']['returncode']:
                    raise RuntimeError('Owned DB cleanup unproven; role retained')
                result['role_cleanup'] = cleanup_roles(control, roles.path, args.run_id)
                result = roles.redact(result)
            except BaseException as exc:
                result.update(status='fail', role_cleanup_error=str(exc))
        if control is not None:
            control.close()
        write_json(out / 'result.json', result)
    print(json.dumps({'status': result['status'], 'checks': len(result['assertions']),
                      'reason': result.get('reason'), 'result': str(out / 'result.json')}))
    return 0 if result['status'] == 'pass' else 1


if __name__ == '__main__':
    raise SystemExit(main())
