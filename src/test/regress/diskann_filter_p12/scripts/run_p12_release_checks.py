#!/usr/bin/env python3
"""Run the preserved P1/P2 checks on an explicitly selected Release instance."""
import argparse
import getpass
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def verify_files(root, manifest, key, expected_count):
    """Check immutable inputs without trusting a historical commit identifier."""
    files = json.loads(manifest.read_text())['files']
    require(len(files) == expected_count, 'unexpected input count: ' + str(manifest))
    seen = set()
    for entry in files:
        relative = Path(entry[key])
        path = (root / relative).resolve()
        require(not relative.is_absolute() and root in path.parents,
                'input path leaves its root: ' + str(relative))
        require(str(relative) not in seen, 'duplicate input: ' + str(relative))
        seen.add(str(relative))
        require(path.is_file(), 'missing input: ' + str(path))
        content = path.read_bytes()
        require(hashlib.sha256(content).hexdigest() == entry['sha256'],
                'input hash mismatch: ' + str(path))
        if 'bytes' in entry:
            require(len(content) == entry['bytes'], 'input size mismatch: ' + str(path))
    return files


def main():
    pack = Path(__file__).resolve().parent.parent
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--source', type=Path, default=pack.parents[3])
    p.add_argument('--install', type=Path, required=True)
    p.add_argument('--runtime', type=Path, required=True)
    p.add_argument('--host', type=Path, help='local Unix socket directory; default: runtime/socket')
    p.add_argument('--port', type=int, default=int(os.environ.get('DISKANN_FILTER_PORT', '55450')))
    p.add_argument('--user', default=os.environ.get('DISKANN_FILTER_USER', os.environ.get('PGUSER', getpass.getuser())))
    p.add_argument('--database', default=os.environ.get('DISKANN_FILTER_DATABASE', 'vf_p12_baseline'))
    p.add_argument('--gcc-lib-dir', type=Path, help='optional compiler runtime directory for native child processes only')
    p.add_argument('--inputs', type=Path, help='source file hash manifest; default: bundled inputs/source-files.json')
    p.add_argument('--start-at', help='continue from a group; does not certify the complete suite')
    a = p.parse_args()
    a.output = a.output.resolve()
    a.output.mkdir(parents=True, exist_ok=False)
    source, install, runtime = a.source.resolve(), a.install.resolve(), a.runtime.resolve()
    data = runtime / 'data'
    host = (a.host or runtime / 'socket').resolve()
    report = dict(result='FAIL', scope='continuation' if a.start_at else 'all groups',
                  historical_core_check_count=1439, checks=[], prerequisites=[], skipped_groups=[])
    try:
        require(1 <= a.port <= 65535, 'invalid port')
        require('diskann-vector-filter' in runtime.name and data.is_dir(),
                'use an existing dedicated diskann-vector-filter runtime/data directory')
        require(host.is_dir(), 'Unix socket directory is missing: ' + str(host))
        require(shutil.which('git') is not None, 'git is required')
        for dependency in ('psycopg2', 'numpy'):
            require(importlib.util.find_spec(dependency) is not None,
                    'missing Python dependency: ' + dependency)
        for name in ('gaussdb', 'gsql'):
            require((install / 'bin' / name).is_file(), 'missing installed executable: ' + name)
        require((source / 'src/test/regress/pg_regress').is_file(),
                'build the native src/test/regress/pg_regress driver before running this suite')
        if a.gcc_lib_dir:
            require(a.gcc_lib_dir.is_dir(), '--gcc-lib-dir does not exist')
        # Python/psycopg2 use system libraries. Native programs receive their own
        # library path inside run_diskann_filter_tests.py, never in this process.
        require(not os.environ.get('LD_LIBRARY_PATH'),
                'invoke Python with env -u LD_LIBRARY_PATH; pass --gcc-lib-dir for the native driver')
        top = Path(subprocess.check_output(['git', '-C', str(source), 'rev-parse', '--show-toplevel'], text=True).strip()).resolve()
        require(top == source, '--source must be the product repository root')
        source_manifest = (a.inputs or pack / 'inputs/source-files.json').resolve()
        files = verify_files(source, source_manifest, 'file', 40)
        verify_files(pack / 'inputs', pack / 'inputs/oracle-files.json', 'path', 6)
        current = subprocess.check_output(['git', '-C', str(source), 'rev-parse', 'HEAD'], text=True).strip()
        snapshot = dict(head=current, files=files, source_manifest_sha256=hashlib.sha256(source_manifest.read_bytes()).hexdigest(),
                        scope='Current source file identity only; does not certify binary build provenance.')
        (a.output / 'runtime-source.json').write_text(json.dumps(snapshot, ensure_ascii=False, indent=2) + '\n')
        report['head'] = current
        os.environ.update(DISKANN_FILTER_HOST=str(host), DISKANN_FILTER_PORT=str(a.port),
                          DISKANN_FILTER_USER=a.user, DISKANN_FILTER_DATABASE=a.database,
                          PGHOST=str(host), PGPORT=str(a.port), PGUSER=a.user)
        # Import after setting the environment shared by every child verifier.
        from verify_query_protocol import connect
        import psycopg2
        conn = connect()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT current_setting('data_directory'), version()")
                actual_data, version = cur.fetchone()
        finally:
            conn.close()
        require(Path(actual_data).resolve() == data, 'test connection uses a different data directory')
        require('debug on' not in version, 'a Release installation is required')
        pid = int((data / 'postmaster.pid').read_text().splitlines()[0])
        executable = (Path('/proc') / str(pid) / 'exe').resolve()
        require(executable == (install / 'bin/gaussdb').resolve(), 'running binary differs from --install')
        # Native prerequisites use regression and temporarily create/drop test.
        conn = psycopg2.connect(host=str(host), port=a.port, user=a.user, dbname='regression')
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT current_setting('data_directory')")
                require(Path(cur.fetchone()[0]).resolve() == data, 'native connection uses a different instance')
        finally:
            conn.close()
        report.update(version=version, executable=str(executable), data=str(data))
        jobs = []
        def add(label, script, *args):
            jobs.append((label, [sys.executable, str(pack / 'scripts' / script), *map(str, args), '--output', str(a.output / label)]))
        native = [sys.executable, str(source / 'src/test/regress/run_diskann_filter_tests.py'),
                  '--install', str(install), '--data', str(data), '--host', str(host),
                  '--port', str(a.port), '--user', a.user, '--output', str(a.output / 'native-eight')]
        if a.gcc_lib_dir:
            native.extend(['--gcc-lib-dir', str(a.gcc_lib_dir.resolve())])
        jobs.append(('native-eight', native))
        for label, script in [('p1-protocol', 'verify_query_protocol.py'),
                              ('p1-identities', 'verify_row_identity.py'),
                              ('p1-pre', 'verify_pre_source.py'), ('p1-in', 'verify_in_probe.py'),
                              ('p1-post', 'verify_post_branch.py'), ('p1-boundaries', 'verify_read_boundaries.py'),
                              ('join-redlines', 'verify_join_redline.py'), ('join-routes', 'verify_join_routes.py'),
                              ('join-costs', 'verify_join_costs_p2.py')]:
            add(label, script)
        oracle = pack / 'inputs/oracle'
        add('oracle-semantics', 'verify_oracle_join_semantics.py', '--oracle', oracle / 'oracle-semantic-original.json',
            '--oracle-post', oracle / 'oracle-post-join-back-original.json', '--probes', '2048')
        add('oracle-multisets', 'verify_join_multisets.py', '--oracle', oracle / 'oracle-multisets-original.json')
        add('oracle-nl', 'verify_join_nl.py', '--oracle', oracle / 'oracle-nl-original.json')
        add('external-bindings', 'verify_join_external_bindings.py')
        add('hash-lifecycle', 'verify_vector_hash_lifecycle.py')
        add('concurrency', 'verify_join_concurrency.py', '--rounds', '24')
        if a.start_at:
            labels = [label for label, command in jobs]
            require(a.start_at in labels, 'unknown --start-at label')
            begin = labels.index(a.start_at)
            report['skipped_groups'] = labels[:begin]
            jobs = jobs[begin:]
        def run(label, command, records):
            print('BEGIN ' + label, flush=True)
            with (a.output / (label + '.log')).open('x') as log:
                completed = subprocess.run(command, cwd=pack, stdout=log, stderr=subprocess.STDOUT)
            record = dict(label=label, command=command, exit_code=completed.returncode)
            records.append(record)
            require(completed.returncode == 0, 'check failed: ' + label)
            return record
        if not a.start_at:
            prerequisites = [
                ('oracle-fixture', 'prepare_oracle_join_fixture.py', '--documents', oracle / 'documents.csv', '--business', oracle / 'business.csv'),
                ('join-registration', 'verify_join_registration.py', '--routes-connected'),
                ('join-cost-fixture', 'inspect_join_width.py'),
            ]
            for label, script, *args in prerequisites:
                run(label, [sys.executable, str(pack / 'scripts' / script), *map(str, args),
                            '--output', str(a.output / label)], report['prerequisites'])
        passed = total = groups = 0
        for label, command in jobs:
            record = run(label, command, report['checks'])
            if label != 'native-eight':
                summary = json.loads((a.output / label / 'summary.json').read_text())
                record.update(passed=summary['passed'], total=summary['total'])
                require(isinstance(summary['total'], int) and summary['total'] > 0
                        and summary['passed'] == summary['total'], 'incomplete checks: ' + label)
                passed += summary['passed']
                total += summary['total']
                groups += 1
        report.update(core_passed=passed, core_total=total, core_groups=groups,
                      result='PARTIAL_PASS' if a.start_at else 'PASS')
    except Exception as exc:
        report['error'] = str(exc)
        raise
    finally:
        (a.output / 'summary.json').write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps(dict(result=report['result'], groups=len(report['checks']),
                         core_passed=report['core_passed'], core_total=report['core_total'])), flush=True)


if __name__ == '__main__':
    main()
