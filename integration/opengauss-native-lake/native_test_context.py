"""Owned SQL fixture lifecycle for native lake write/callback integration tests.

This is test infrastructure. It creates no lake protocol records of its own.
"""
import datetime
import json
import os
import re
from pathlib import Path
import subprocess
import sys
import uuid

from inline_fixture import export_reference, import_catalog, query, scalar
from lake_harness import Blocked, read_json, validate_run_id, write_json
from native_runtime import PROFILE, RUNTIME, PORT

DRIVER = Path(__file__).resolve().parent
ROOT = DRIVER.parents[1]


def reviewed_runtime_version():
    """Exact source/runtime contract, independent of the installed SQL version."""
    source = Path(os.getenv('OG_LAKE_SOURCE', str(ROOT / '.worktrees/opengauss-native-lake')))
    control = source / 'contrib/og_lake/og_lake.control'
    versions = re.findall(r"^default_version\s*=\s*'([^']+)'", control.read_text(), re.MULTILINE)
    # Recognition permits the real legacy/0.17 assertions to execute; it does
    # not change their installed SQL versions or claim a compatibility pass.
    if len(versions) != 1 or versions[0] not in ('0.3', '0.4', '0.5', '0.6', '0.7', '0.8', '0.9', '0.10', '0.11', '0.17', '0.18', '0.19', '0.20', '0.21', '0.22', '0.23', '0.24', '0.25', '0.26', '0.29', '0.30', '0.31', '0.32', '0.33', '0.34', '0.35', '0.36', '0.37', '0.38', '0.39', '0.40', '0.41', '0.42', '0.43', '0.49', '0.50', '0.51', '0.52', '0.53', '0.54', '0.55'):
        raise Blocked('Native write tests require one reviewed source control version (0.3 through0.11, or0.17/0.18/0.19/0.20/0.21/0.22/0.23/0.24/0.25/0.26/0.29/0.30/0.31/0.32/0.33/0.34/0.35/0.36/0.37/0.38/0.39/0.40/0.41/0.42/0.43/0.49/0.50/0.51/0.52/0.53/0.54/0.55)')
    return versions[0]


class OwnedDatabase:
    """with OwnedDatabase(run_id, attempt) as db: conn = db.connect()."""
    def __init__(self, run_id, attempt):
        validate_run_id(run_id)
        self.run_id = run_id
        self.attempt = Path(attempt).resolve()
        self.attempt.mkdir(parents=True, exist_ok=True)
        self.database = None
        self.admin = None
        self.connections = []
        self.allocation = None
        self.cleanup_result = None

    def connect(self, database=None):
        import psycopg2
        conn = psycopg2.connect(host=str(RUNTIME / 'socket'),
                                port=PORT, dbname=database or self.database,
                                user=os.getenv('USER', 'qiumc'), connect_timeout=10,
                                application_name='og_lake_owned_' + self.run_id[:40])
        conn.autocommit = True
        self.connections.append(conn)
        query(conn, 'SET statement_timeout=15000')
        return conn

    def create(self):
        from psycopg2 import sql
        if self.database is not None:
            raise RuntimeError('OwnedDatabase cannot allocate a second database')
        self.admin = self.connect('postgres')
        self.database = 'lake_inline_' + datetime.datetime.now().strftime('%Y%m%d%H%M%S') + '_' + uuid.uuid4().hex[:8]
        self.allocation = {'schema_version': 1, 'run_id': self.run_id, 'attempt': str(self.attempt),
                           'runtime_profile': PROFILE, 'runtime_data': str(RUNTIME / 'data'),
                           'database': self.database, 'database_oid': None, 'phase': 'planned',
                           'owner_oid': scalar(self.admin, 'SELECT oid FROM pg_roles WHERE rolname=current_user'),
                           'postmaster_start': str(scalar(self.admin, 'SELECT pg_postmaster_start_time()')),
                           'marker': 'og_lake_inline_attempt:' + self.run_id + ':' + uuid.uuid4().hex}
        self._save_allocation()
        query(self.admin, sql.SQL('CREATE DATABASE {}').format(sql.Identifier(self.database)))
        self.allocation['database_oid'] = scalar(self.admin, 'SELECT oid FROM pg_database WHERE datname=%s', (self.database,))
        self.allocation['phase'] = 'identified'
        self._save_allocation()
        query(self.admin, sql.SQL('COMMENT ON DATABASE {} IS {}').format(
            sql.Identifier(self.database), sql.Literal(self.allocation['marker'])))
        self.allocation['phase'] = 'owned'
        self._save_allocation()
        return self

    def _save_allocation(self):
        write_json(self.attempt / 'database-owner.json', self.allocation)

    def cleanup(self):
        for conn in reversed(self.connections):
            conn.close()
        self.connections = []
        completed = subprocess.run([sys.executable, str(DRIVER / 'tests/inline_cleanup.py'),
                                    '--run-id', self.run_id, '--attempt', str(self.attempt)],
                                   capture_output=True, text=True, timeout=30)
        self.cleanup_result = {'returncode': completed.returncode, 'stdout': completed.stdout,
                               'stderr': completed.stderr}
        if completed.returncode:
            raise RuntimeError('Owned database cleanup failed: ' + completed.stdout + completed.stderr)

    def artifacts(self):
        # The latest pointer is mutable when the runner repeats cleanup.
        return [{'path': str(path)} for path in self.attempt.rglob('*') if path.is_file()
                and (path.name == 'database-owner.json' or path.name.startswith('database-cleanup-'))]

    def __enter__(self):
        try:
            return self.create()
        except BaseException:
            self.cleanup()
            raise

    def __exit__(self, kind, value, traceback):
        self.cleanup()
        return False


def load_reference(conn, manifest_path, run_id, output_dir, metadata_schema='lake_metadata', state=None):
    """Export/import a real profile; returns manifest, exported, counts, artifacts.

    Caller creates the extension/business schema and chooses read-only or writable
    attachment. Only enclosing SQL schema/type spellings are adapted by the importer.
    """
    output_dir = Path(output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    export_path = output_dir / ('reference-export-' + (state or 'current') + '.json')
    exported, manifest = export_reference(DRIVER, Path(manifest_path).resolve(), export_path, run_id, state)
    query(conn, 'BEGIN')
    try:
        counts = import_catalog(conn, metadata_schema, exported)
        query(conn, 'COMMIT')
    except BaseException:
        query(conn, 'ROLLBACK')
        raise
    artifacts = [{'path': str(Path(manifest_path).resolve())}, {'path': str(export_path)},
                 {'path': str(export_path.with_suffix('.log'))}]
    return manifest, exported, counts, artifacts
