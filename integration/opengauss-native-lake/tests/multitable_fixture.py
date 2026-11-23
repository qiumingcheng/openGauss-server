#!/usr/bin/env python3
"""Generate MULTITABLE only by actual pinned DuckLake 1.1 business DDL/DML.

Staged independent adapter. Preparing/importing this module performs no SQL.
No protocol rows are synthesized; standard IDs, carriers and stats are queried.
"""
import argparse
import datetime
import fcntl
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys
import traceback
import uuid


class Blocked(RuntimeError):
    pass


def sha(path):
    result = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            result.update(block)
    return result.hexdigest()


def q(value):
    return "'" + str(value).replace("'", "''") + "'"


def qi(value):
    return '"' + value.replace('"', '""') + '"'


def write_json(path, value):
    Path(path).write_text(json.dumps(value, ensure_ascii=False, indent=2) + '\n')


class Generator:
    def __init__(self, args):
        self.root = args.workspace.resolve()
        self.output = args.output
        if not self.output.is_absolute():
            raise ValueError('--output must be an absolute manifest path')
        self.output = self.output.resolve()
        self.run_id = args.run_id
        if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]*', self.run_id):
            raise ValueError('run-id must be a simple identifier')
        self.owned = self.output.parent / 'multitable-reference'
        if self.output.exists() or self.owned.exists():
            raise ValueError('Refuse to overwrite the manifest or owned fixture directory')
        self.driver = self.root / 'integration/opengauss-native-lake'
        self.reference = self.root / '.reference/opengauss-native-lake'
        self.lock_path = self.driver / 'source-lock.json'
        self.receipt_path = self.reference / 'build/receipt.json'
        self.lock = json.loads(self.lock_path.read_text())
        self.receipt = json.loads(self.receipt_path.read_text())
        self.lock_sha = sha(self.lock_path)
        self.receipt_sha = sha(self.receipt_path)
        self.binary = self.reference / self.lock['reference_build']['build_subdir'] / 'duckdb'
        self.binary_sha = sha(self.binary)
        if self.receipt['source_lock_hash'] != self.lock_sha or self.receipt['binary_sha256'] != self.binary_sha:
            raise Blocked('Reference receipt is stale for current source-lock/binary')
        self.driver_sha = sha(self.driver / 'reference.sh')
        if self.driver_sha != self.lock['reference_build']['script_sha256']:
            raise Blocked('Frozen formal reference driver differs from its locked hash')
        if self.receipt.get('build_origin') != 'exact_pinned_source':
            raise Blocked('An exact pinned-source reference build is required')
        self.script = Path(__file__).resolve()
        self.script_sha = sha(self.script)
        self.catalog = self.owned / 'metadata.ducklake'
        self.data = self.owned / 'data'
        self.counter = 0
        self.mutex = None
        self.created = False

    def command(self, sql, label, *, raw_catalog=False, readonly=False):
        self.counter += 1
        stem = self.owned / ('%03d-' % self.counter + label)
        stem.with_suffix('.sql').write_text(sql + '\n')
        argv = [str(self.binary), '-no-init', '-batch', '-bail', '-json']
        if readonly:
            argv.append('-readonly')
        if raw_catalog:
            argv.append(str(self.catalog))
        started = datetime.datetime.now(datetime.timezone.utc).isoformat()
        process = subprocess.run(argv, input=sql, text=True, capture_output=True, timeout=180)
        stem.with_suffix('.stdout').write_text(process.stdout)
        stem.with_suffix('.stderr').write_text(process.stderr)
        write_json(stem.with_suffix('.process.json'),
                   {'argv': argv, 'returncode': process.returncode, 'started_at': started})
        if process.returncode:
            raise RuntimeError('Actual reference command failed: ' + str(stem.with_suffix('.stderr')))
        return json.loads(process.stdout) if process.stdout.strip() else []

    def attach(self, readonly=False):
        options = "DUCKLAKE_VERSION '1.1', AUTOMATIC_MIGRATION FALSE"
        options += ', READ_ONLY' if readonly else ', DATA_INLINING_ROW_LIMIT 100'
        # All generated Parquet (if ever produced unexpectedly) is owned here.
        options += ', DATA_PATH ' + q(self.data)
        return 'ATTACH ' + q('ducklake:' + str(self.catalog)) + ' AS lake (' + options + ');\n'

    def sql(self, sql, label, readonly=False):
        return self.command(self.attach(readonly) + sql, label)

    def verify_sources(self):
        verified = {}
        for name, repo in self.lock['repositories'].items():
            if repo['role'] == 'production_baseline':
                continue  # Product full build is independent and may be in progress.
            path = self.root / repo['path']
            actual = subprocess.check_output(['git', '-C', str(path), 'rev-parse', 'HEAD'], text=True).strip()
            if actual != repo['commit']:
                raise Blocked('Reference checkout differs: ' + name)
            for command in (['diff', '--quiet', '--ignore-submodules'], ['diff', '--cached', '--quiet']):
                if subprocess.run(['git', '-C', str(path)] + command).returncode:
                    raise Blocked('Reference checkout has unrecorded tracked edits: ' + name)
            license = repo['license']
            if license['file'] and license['sha256'] and sha(path / license['file']) != license['sha256']:
                raise Blocked('Reference license hash differs: ' + name)
            verified[name] = actual
        return verified

    def generate(self):
        self.mutex = (self.reference / '.driver.lock').open('a+')
        try:
            fcntl.flock(self.mutex, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise Blocked('Another reference driver is active; no wait/retry or lock bypass') from exc
        verified = self.verify_sources()
        self.owned.mkdir(parents=True, exist_ok=False)
        self.created = True
        self.data.mkdir()
        write_json(self.owned / 'owner.json', {'run_id': self.run_id, 'case': 'MULTITABLE',
                   'token': uuid.uuid4().hex, 'cleanup_owner': str(self.owned),
                   'purpose': 'independent actual reference fixture; never an openGauss data directory'})
        (self.owned / 'source-lock.json').write_bytes(self.lock_path.read_bytes())
        (self.owned / 'build-receipt.json').write_bytes(self.receipt_path.read_bytes())
        (self.owned / 'multitable_fixture.py').write_bytes(self.script.read_bytes())
        write_json(self.owned / 'source-verification.json', verified)
        versions = self.command('PRAGMA version;', 'runtime-version')
        extensions = self.command("SELECT extension_version,loaded,install_mode FROM duckdb_extensions() WHERE extension_name='ducklake';", 'runtime-ducklake')
        if len(versions) != 1 or versions[0]['source_id'] != self.lock['repositories']['duckdb']['commit'][:10]:
            raise Blocked('Actual DuckDB source does not match frozen commit')
        expected_extension = {'extension_version': self.lock['repositories']['ducklake']['commit'][:10],
                              'loaded': True, 'install_mode': 'STATICALLY_LINKED'}
        if extensions != [expected_extension]:
            raise Blocked('Actual statically linked DuckLake differs from frozen commit')
        # Individual autocommit statements, exactly like normal user DDL/DML.
        self.sql('CREATE SCHEMA lake.biz;', 'create-schema')
        self.sql('CREATE TABLE lake.biz.t(id INTEGER,val INTEGER);', 'create-t')
        self.sql('CREATE TABLE lake.biz.u(id INTEGER,val INTEGER);', 'create-u')
        self.sql('INSERT INTO lake.biz.t VALUES(1,10),(2,20),(3,30);', 'insert-t')
        self.sql('INSERT INTO lake.biz.u VALUES(101,100),(102,200);', 'insert-u')
        meta = qi('__ducklake_metadata_lake') + '.main.'
        version = self.sql('SELECT value FROM ' + meta + "ducklake_metadata WHERE key='version';", 'format-version', True)
        if version != [{'value': '1.1'}]:
            raise RuntimeError('Actual metadata is not explicit format 1.1')
        snapshots = self.sql('SELECT * FROM ' + meta + 'ducklake_snapshot ORDER BY snapshot_id;', 'snapshots', True)
        head = max(row['snapshot_id'] for row in snapshots)
        changes = self.sql('SELECT * FROM ' + meta + 'ducklake_snapshot_changes ORDER BY snapshot_id;', 'snapshot-changes', True)
        business_tables = []
        expected_by_alias = {'t': [{'id': 1, 'val': 10}, {'id': 2, 'val': 20}, {'id': 3, 'val': 30}],
                             'u': [{'id': 101, 'val': 100}, {'id': 102, 'val': 200}]}
        for alias, intended in expected_by_alias.items():
            binding = self.sql('SELECT t.* FROM ' + meta + 'ducklake_table t JOIN ' + meta +
                "ducklake_schema s ON s.schema_id=t.schema_id WHERE s.schema_name='biz' AND s.end_snapshot IS NULL " +
                'AND t.table_name=' + q(alias) + ' AND t.end_snapshot IS NULL;', 'binding-' + alias, True)
            if len(binding) != 1:
                raise RuntimeError('Business binding is not unique: ' + alias)
            table_id = binding[0]['table_id']
            relation = 'lake.biz.' + qi(alias)
            described = self.sql('DESCRIBE ' + relation + ';', 'schema-' + alias, True)
            schema = [{'name': col['column_name'], 'type': col['column_type']} for col in described]
            if schema != [{'name': 'id', 'type': 'INTEGER'}, {'name': 'val', 'type': 'INTEGER'}]:
                raise RuntimeError('Actual business schema differs: ' + alias)
            rows = self.sql('SELECT rowid AS logical_rowid,snapshot_id AS begin_snapshot,* FROM ' + relation +
                            ' ORDER BY id;', 'business-' + alias, True)
            if [{k: row[k] for k in ('id', 'val')} for row in rows] != intended:
                raise RuntimeError('Actual business values differ: ' + alias)
            if len({row['logical_rowid'] for row in rows}) != len(rows):
                raise RuntimeError('Reference logical IDs are not unique within table: ' + alias)
            stats = self.sql('SELECT * FROM ' + meta + 'ducklake_table_stats WHERE table_id=' + str(table_id) + ';', 'stats-' + alias, True)
            column_stats = self.sql('SELECT * FROM ' + meta + 'ducklake_table_column_stats WHERE table_id=' + str(table_id) + ' ORDER BY column_id;', 'column-stats-' + alias, True)
            files = self.sql('SELECT * FROM ' + meta + 'ducklake_data_file WHERE table_id=' + str(table_id) + ';', 'files-' + alias, True)
            carriers = self.sql('SELECT * FROM ' + meta + 'ducklake_inlined_data_tables WHERE table_id=' + str(table_id) + ';', 'carriers-' + alias, True)
            physical = {row['table_name']: self.sql('SELECT * FROM ' + meta + qi(row['table_name']) +
                ' ORDER BY _ducklake_row_id,_ducklake_begin_snapshot;', 'physical-' + alias + '-' + str(n), True)
                for n, row in enumerate(carriers)}
            if files or len(carriers) != 1 or sum(map(len, physical.values())) != len(intended):
                raise RuntimeError('Expected one entirely inline carrier: ' + alias)
            actual_physical = next(iter(physical.values()))
            expected_physical = [dict(_ducklake_row_id=row['logical_rowid'],
                _ducklake_begin_snapshot=row['begin_snapshot'], _ducklake_end_snapshot=None,
                id=row['id'], val=row['val']) for row in rows]
            if actual_physical != expected_physical:
                raise RuntimeError('Actual inline carrier differs from actual logical read: ' + alias)
            if (len(stats) != 1 or stats[0]['record_count'] != len(rows) or
                    stats[0]['next_row_id'] != len(rows) or stats[0]['file_size_bytes'] != 0):
                raise RuntimeError('Fresh legal INSERT statistics/frontier differ: ' + alias)
            business_tables.append({'alias': alias, 'schema_name': 'biz', 'table_name': alias,
                'table_id': table_id, 'table_definition': binding[0], 'schema': schema,
                'expected': [{k:v for k,v in row.items() if k != 'begin_snapshot'} for row in rows],
                'row_identity_map': rows, 'visible_count_expected': len(rows),
                'table_stats': stats, 'column_stats': column_stats, 'data_files': files,
                'inline_tables': carriers, 'inline_rows': physical})
        if len({t['table_id'] for t in business_tables}) != 2:
            raise RuntimeError('T and U must be distinct actual lake tables')
        all_files = self.sql('SELECT * FROM ' + meta + 'ducklake_data_file;', 'all-data-files', True)
        all_deletes = self.sql('SELECT * FROM ' + meta + 'ducklake_delete_file;', 'all-delete-files', True)
        if all_files or all_deletes:
            raise RuntimeError('MULTITABLE generated unexpected external data/delete files')
        # Produce a standalone checkpointed catalog accepted by formal export.
        self.command('CHECKPOINT;', 'checkpoint', raw_catalog=True)
        wal = Path(str(self.catalog) + '.wal')
        if wal.exists() and wal.stat().st_size:
            raise RuntimeError('Checkpoint left a nonempty WAL')
        if sha(self.lock_path) != self.lock_sha or sha(self.receipt_path) != self.receipt_sha or sha(self.binary) != self.binary_sha or sha(self.script) != self.script_sha:
            raise Blocked('A frozen build/generator input changed during fixture generation')
        first = business_tables[0]
        provenance = {'source_lock_hash': self.lock_sha, 'receipt_sha256': self.receipt_sha,
                      'binary_sha256': self.binary_sha, 'formal_driver_sha256': self.driver_sha,
                      'generator_path': str(self.script), 'generator_sha256': self.script_sha,
                      'runtime_version': versions, 'runtime_ducklake': extensions}
        write_json(self.owned / 'generation-provenance.json', provenance)
        artifacts = [{'path': str(p), 'sha256': sha(p), 'size_bytes': p.stat().st_size}
                     for p in sorted(self.owned.rglob('*')) if p.is_file()]
        manifest = {'schema_version': 1, 'run_id': self.run_id, 'case': 'MULTITABLE',
            'status': 'verified_reference_only', 'source_lock_hash': self.lock_sha,
            'reference_commit': self.lock['repositories']['ducklake']['commit'],
            'duckdb_commit': self.lock['repositories']['duckdb']['commit'],
            'binary_sha256': self.binary_sha, 'catalog': str(self.catalog),
            'catalog_engine': 'DuckDB-local metadata (not openGauss shared catalog)',
            'format_version': '1.1', 'automatic_migration': False, 'table_id': first['table_id'],
            'snapshot_aliases': {'S10': head}, 'row_identity_map': first['row_identity_map'],
            'file_aliases': {}, 'physical_positions': [], 'physical_schema': [],
            'schema': first['schema'], 'layout_settings': {'data_inlining_row_limit': 100},
            'expected': first['expected'], 'visible_count_expected': first['visible_count_expected'],
            'history_expected': {}, 'catalog_states': [], 'table_stats': first['table_stats'],
            'data_files': [], 'inline_tables': first['inline_tables'], 'inline_rows': first['inline_rows'],
            'business_tables': business_tables, 'snapshots': snapshots, 'snapshot_changes': changes,
            'generation_provenance': provenance, 'files': artifacts, 'cleanup_owner': str(self.owned),
            'limitations': ['Actual fixed-reference catalog only; no native execution has been performed.',
                'Formal fixture validator requires an explicit MULTITABLE profile addition; never relabel as I.',
                'No shared-catalog concurrency, external SDK, flush, or RAC interoperability qualification.']}
        self.output.parent.mkdir(parents=True, exist_ok=True)
        write_json(self.output, manifest)
        print(json.dumps({'status': 'verified_reference_only', 'case': 'MULTITABLE',
              'manifest': str(self.output), 'source_lock_hash': self.lock_sha,
              'business_table_ids': {t['alias']: t['table_id'] for t in business_tables}}))
        return 0

    def close(self):
        if self.mutex is not None:
            self.mutex.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--workspace', type=Path, default=Path('/home/qiumc/data/code/cpp'))
    parser.add_argument('--case', choices=['MULTITABLE'], default='MULTITABLE')
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    generator = None
    try:
        generator = Generator(args)
        return generator.generate()
    except BaseException as exc:
        status = 'blocked' if isinstance(exc, (Blocked, FileNotFoundError)) else 'fail'
        if generator is not None and generator.created:
            write_json(generator.owned / 'generation-failure.json',
                       {'status': status, 'error': repr(exc), 'traceback': traceback.format_exc()})
        print(json.dumps({'status': status, 'error': repr(exc)}), file=sys.stderr)
        return 2 if status == 'blocked' else 1
    finally:
        if generator is not None:
            generator.close()


if __name__ == '__main__':
    raise SystemExit(main())
