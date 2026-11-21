#!/usr/bin/env python3
"""Clean only the database whose OID and ownership receipt belong to this attempt.

No backend is terminated. A surviving connection or an uncertain identity makes
cleanup fail with evidence instead of deleting another database by name prefix.
"""
import argparse
import json
import os
from pathlib import Path
import re
import sys
import time

DRIVER = Path(__file__).resolve().parents[1]
ROOT = DRIVER.parents[1]
sys.path.insert(0, str(DRIVER))
from inline_fixture import query, scalar
from lake_harness import read_json, validate_run_id, write_json
from native_runtime import PROFILE, RUNTIME, PORT, INSTALL


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--run-id', default=os.getenv('OG_LAKE_RUN_ID'))
    parser.add_argument('--attempt', default=os.getenv('OG_LAKE_EVIDENCE_DIR'))
    args = parser.parse_args()
    if not args.run_id or not args.attempt:
        parser.error('run-id and attempt required')
    validate_run_id(args.run_id)
    attempt = Path(args.attempt).resolve()
    receipt_path = attempt / 'database-owner.json'
    outcome = {'status': 'fail', 'run_id': args.run_id, 'attempt': str(attempt), 'terminated_backends': []}
    conn = None
    try:
        # Absence means the test never allocated a database. A malformed or
        # half-written receipt is not treated as this harmless case.
        if not receipt_path.exists():
            outcome.update(status='pass', reason='No database allocation receipt; no cleanup action.')
            return 0
        receipt = read_json(receipt_path)
        if (receipt.get('schema_version') != 1 or receipt.get('run_id') != args.run_id or
                Path(receipt.get('attempt', '')).resolve() != attempt):
            raise RuntimeError('Database allocation receipt belongs to another attempt')
        database = receipt.get('database', '')
        if receipt.get('runtime_profile', 'debug') != PROFILE:
            raise RuntimeError('Database allocation profile does not match selected runtime')
        if receipt.get('runtime_data', str(RUNTIME / 'data')) != str(RUNTIME / 'data'):
            raise RuntimeError('Database allocation belongs to another data directory')
        if not re.fullmatch(r'lake_inline_[0-9]{14}_[0-9a-f]{8}', database):
            raise RuntimeError('Invalid allocated database name')
        runtime = RUNTIME
        owner = read_json(runtime / 'owner.json')
        if (owner.get('project') != 'opengauss-native-lake' or owner.get('profile') != PROFILE or
                owner.get('data') != str(runtime / 'data') or owner.get('port') != PORT or
                owner.get('install') != str(INSTALL)):
            raise RuntimeError('Runtime is not the selected owned instance')
        pid = int((runtime / 'data/postmaster.pid').read_text().splitlines()[0])
        installed = (INSTALL / 'bin/gaussdb').resolve()
        if Path('/proc', str(pid), 'exe').resolve() != installed:
            raise RuntimeError('Owned postmaster is not running the expected installation')
        import psycopg2
        from psycopg2 import sql
        conn = psycopg2.connect(host=str(runtime / 'socket'), port=PORT, dbname='postgres',
                                user=os.getenv('USER', 'qiumc'), connect_timeout=5,
                                application_name='og_lake_inline_cleanup')
        conn.autocommit = True
        query(conn, 'SET statement_timeout=10000')
        if Path(scalar(conn, 'SHOW data_directory')).resolve() != runtime / 'data':
            raise RuntimeError('SQL connection reached another data directory')
        start = str(scalar(conn, 'SELECT pg_postmaster_start_time()'))
        if start != receipt.get('postmaster_start'):
            raise RuntimeError('Postmaster identity changed after database allocation')
        observed = query(conn, 'SELECT oid,datname,datdba,shobj_description(oid,\'pg_database\') '
                               'FROM pg_database WHERE datname=%s OR oid=%s',
                         (database, receipt.get('database_oid', 0)))
        if not observed:
            outcome.update(status='pass', reason='Allocated database is already absent.')
            return 0
        if receipt.get('phase') not in ('owned', 'dropped') or not receipt.get('database_oid'):
            raise RuntimeError('Allocation stopped before a complete OID/ownership receipt; no DROP attempted')
        expected = (receipt['database_oid'], database, receipt['owner_oid'], receipt['marker'])
        if observed != [expected]:
            outcome['observed_database_identity'] = observed
            raise RuntimeError('Database OID, name, owner or marker no longer matches this attempt')
        if scalar(conn, 'SELECT oid FROM pg_roles WHERE rolname=current_user') != receipt['owner_oid']:
            raise RuntimeError('Cleanup connection is not the recorded database owner')
        deadline = time.monotonic() + 8
        while scalar(conn, 'SELECT count(*) FROM pg_stat_activity WHERE datid=%s', (receipt['database_oid'],)):
            if time.monotonic() >= deadline:
                raise RuntimeError('Allocated database still has connections; no session termination or DROP attempted')
            time.sleep(0.1)
        # Recheck immediately before DROP. A race remains fail-safe: DROP refuses
        # a database with active sessions, and this test never terminates them.
        check = query(conn, 'SELECT oid,datname,datdba,shobj_description(oid,\'pg_database\') '
                            'FROM pg_database WHERE oid=%s', (receipt['database_oid'],))
        if check != [expected]:
            raise RuntimeError('Database identity changed before cleanup')
        query(conn, sql.SQL('DROP DATABASE {}').format(sql.Identifier(database)))
        if scalar(conn, 'SELECT count(*) FROM pg_database WHERE datname=%s OR oid=%s',
                  (database, receipt['database_oid'])):
            raise RuntimeError('Allocated database remains after DROP')
        outcome.update(status='pass', reason='Only the OID- and marker-verified database was removed.',
                       dropped_database_oid=receipt['database_oid'])
        return 0
    except BaseException as exc:
        outcome['error'] = repr(exc)
        return 1
    finally:
        if conn is not None:
            conn.close()
        # Keep every invocation: normal child cleanup, runner fallback, and
        # idempotence checks must not overwrite the original DROP evidence.
        report = attempt / ('database-cleanup-' + str(time.time_ns()) + '-' + str(os.getpid()) + '.json')
        outcome['immutable_report'] = str(report)
        write_json(report, outcome)
        write_json(attempt / 'database-cleanup.json', outcome)
        print(json.dumps(outcome, ensure_ascii=False, default=str))


if __name__ == '__main__':
    raise SystemExit(main())
