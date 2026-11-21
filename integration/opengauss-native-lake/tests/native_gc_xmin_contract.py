#!/usr/bin/env python3
"""Real retirement xmin versus an active RC reader's native host watermark.

Uses genuine native file-delete replacement and the actual native VACUUM
watermark entry. Performs no physical deletion and supplies no RAC proof.
"""
import argparse
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
    result = {'status': 'fail', 'scope': 'Native safe xmin with a real retired deletion file and retained RC lake context',
              'assertions': [], 'limitations': ['No physical deletion, backup/PITR pin or global target-RAC qualification.']}
    inputs = [Path(__file__), DRIVER / 'native_test_context.py', DRIVER / 'tests/native_table_create.py',
              DRIVER / 'tests/permission_api.py', DRIVER / 'inline_fixture.py']
    result['inputs'] = {str(x): sha256(x) for x in inputs}
    archive = out / 'executed-inputs'
    archive.mkdir()
    for x in inputs:
        shutil.copy2(x, archive / x.name)
    db = roles = control = None

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
        query(test.b, 'BEGIN ISOLATION LEVEL READ COMMITTED')
        check('RC reader establishes and retains actual lake context', test.rows(test.b), [(1, 10), (3, 30)])
        query(test.a, 'DELETE FROM biz.orders WHERE id=1')
        query(test.a, flush)
        retire = query(test.observer, 'SELECT data_file_id,path,xmin::text FROM '
                       'created_metadata.ducklake_files_scheduled_for_deletion')
        check('one real standard retirement row', len(retire), 1)
        retirement_xid = int(retire[0][2])
        check('retirement uses a real normal full host xid', retirement_xid >= 3)
        server = scalar(test.observer, "SELECT oid FROM pg_foreign_server WHERE srvname='created_lake'")
        query(test.observer, 'BEGIN')
        check('actual RC retained lake context denies GC-exclusive admission',
              scalar(test.observer, 'SELECT og_lake_internal.test_catalog_guard_acquire(%s::oid,true)', (server,)), False)

        def oldest(conn):
            return scalar(conn, "SELECT gc_probe.oldest('created_metadata.ducklake_files_scheduled_for_deletion'::regclass)")

        def precedes(conn, first, second):
            return scalar(conn, 'SELECT gc_probe.precedes(%s::xid,%s::xid)', (str(first), str(second)))

        water = oldest(test.observer)
        result['retirement_xid'] = retirement_xid
        result['oldest_with_retained_rc'] = water
        check('actual native watermark prevents premature physical eligibility', precedes(test.observer, retirement_xid, water), False)
        query(test.observer, 'COMMIT')
        check('retained RC lake context still reads its original valid state', test.rows(test.b), [(1, 10), (3, 30)])
        check('new reader sees committed replacement state', test.rows(test.observer), [(3, 30)])
        check('old physical object unchanged while RC reader needs it', sha256(old_files[0]), old_hash)
        query(test.b, 'COMMIT')
        deadline = time.monotonic() + 15
        samples = []
        eligible = False
        progress_writes = 0
        while time.monotonic() < deadline:
            # The native deferred snapshot ring recalculates at transaction
            # completion. Read-only polling need not advance its cached safe
            # minimum. Drive ordinary committed host work, without bypassing
            # the native watermark or changing any cleanup settings.
            query(test.a, 'INSERT INTO biz.heap_marker VALUES(1)')
            query(test.a, 'DELETE FROM biz.heap_marker WHERE id=1')
            progress_writes += 2
            # A fresh connection avoids confusing a deliberately conservative
            # per-session cached minimum with a newly checked GC transaction.
            fresh = db.connect()
            try:
                water = oldest(fresh)
                samples.append(water)
                eligible = precedes(fresh, retirement_xid, water)
            finally:
                fresh.close()
            if eligible:
                break
            time.sleep(.1)
        result['oldest_after_rc_ends'] = samples
        result['ordinary_committed_progress_writes'] = progress_writes
        check('conservative host watermark advances after the reader ends', eligible)
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
