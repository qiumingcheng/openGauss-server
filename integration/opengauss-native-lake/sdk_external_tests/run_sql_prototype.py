#!/usr/bin/env python3
"""Run a private SDK6 SQL materialization probe in a new, owned Debug cluster."""
import argparse
from contextlib import closing
from datetime import datetime, timezone
from hashlib import file_digest, sha256
import getpass
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import subprocess
import sys
import traceback

import psycopg2
from psycopg2 import sql

HERE = Path(__file__).resolve().parent


def sha(p):
    with p.open('rb') as f:
        return file_digest(f, 'sha256').hexdigest()


def query(db, text, params=()):
    with db.cursor() as cursor:
        cursor.execute(text, params)
        return cursor.fetchall() if cursor.description else None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--module-build', type=Path, required=True)
    parser.add_argument('--module-build-sha256', required=True)
    parser.add_argument('--workspace', type=Path, default=os.environ.get('CPP_WORKSPACE'))
    parser.add_argument('--prefix', type=Path, required=True, help='matching Debug GAUSSHOME')
    parser.add_argument('--fixture', type=Path, required=True, help='data.parquet from run_probe.sh')
    parser.add_argument('--fixture-sha256', required=True)
    parser.add_argument('--out-dir', type=Path)
    parser.add_argument('--runtime-root', type=Path)
    parser.add_argument('--port', type=int, default=55760)
    parser.add_argument('--user', default=os.environ.get('PGUSER') or getpass.getuser())
    args = parser.parse_args()
    if not __debug__ or not args.run_id.isascii() or not args.run_id.replace('_', '').isalnum():
        parser.error('use enabled assertions and a new ASCII run id')
    if args.workspace is None:
        parser.error('set CPP_WORKSPACE or --workspace')
    ROOT = args.workspace.expanduser().resolve()
    # Resolve the migrated driver beside these probes, in either supported layout.
    sys.path.insert(0, str(HERE.parent))
    import build_driver
    build_driver.ROOT = ROOT
    bootstrap, port_check = build_driver.bootstrap, build_driver.port_check
    out = (args.out_dir or ROOT / '公共文档/openGauss原生湖存储/验证' / args.run_id).resolve()
    runtime = ((args.runtime_root or ROOT / '.runtime/opengauss-native-lake') /
               ('pgl2-' + sha256(args.run_id.encode()).hexdigest()[:12])).resolve()
    data, socket_dir = runtime / 'data', runtime / 'socket'
    installed_prefix = args.prefix.expanduser().resolve()
    prefix = runtime / 'install'
    if prefix.is_relative_to(installed_prefix):
        parser.error('the private runtime must be outside the supplied installation')
    assert sha(args.module_build) == args.module_build_sha256, 'module build receipt digest'
    qualified = json.loads(args.module_build.read_text())
    assert qualified['status'] == 'built_unloaded'
    assert sha(Path(qualified['sdk_library'])) == qualified['sdk_sha256'], 'SDK6 library digest'
    fixture = args.fixture.expanduser().resolve()
    assert sha(fixture) == args.fixture_sha256, 'fixture digest'
    module = args.module_build.resolve().with_name('lake_external_sql_probe.so')
    assert sha(module) == qualified['module_sha256']
    assert sha(installed_prefix / 'bin/gaussdb') == qualified['gaussdb_sha256']
    assert not runtime.exists() and not out.exists()
    port = args.port
    assert 1024 <= port <= 65533
    port_check(port)
    out.parent.mkdir(parents=True, exist_ok=True)
    runtime.parent.mkdir(parents=True, exist_ok=True)
    out.mkdir(mode=0o700)
    runtime.mkdir(mode=0o700)
    state = {'status': 'running', 'runtime': str(runtime), 'port': port, 'owned': True,
             'installed_prefix': str(installed_prefix), 'private_prefix': str(prefix),
             'module_sha256': sha(module), 'module_build_sha256': sha(args.module_build),
             'fixture_sha256': args.fixture_sha256, 'sdk_sha256': qualified['sdk_sha256'],
             'binary_sha256': qualified['gaussdb_sha256'], 'commands': [], 'checks': [],
             'started_at': datetime.now(timezone.utc).isoformat()}
    (runtime / 'owner.json').write_text(json.dumps(state, indent=2) + '\n')
    env = {**os.environ, 'GAUSSHOME': str(prefix), 'PGDATA': str(data), 'PGHOST': str(socket_dir),
           'PGPORT': str(port), 'LD_LIBRARY_PATH': f'{prefix}/lib:{ROOT}/binarylibs/buildtools/gcc10.3/gcc/lib64:' + os.environ.get('LD_LIBRARY_PATH', '')}
    ctl = prefix / 'bin/gs_ctl'

    def service(label, cmd):
        converted = [str(p) for p in cmd]
        state['commands'].append(converted)
        with (out / (label + '.log')).open('x') as log:
            subprocess.run(converted, env=env, stdout=log, stderr=subprocess.STDOUT, check=True, timeout=80)

    def check(condition, name):
        state['checks'].append({'test': name, 'passed': bool(condition)})
        assert condition, name

    def reject(db, code, statement, params, label):
        try:
            query(db, statement, params)
        except psycopg2.Error as err:
            check(err.pgcode == code, (label, code, err.pgcode, str(err)))
        else:
            check(False, label)

    try:
        # CREATE FUNCTION copies C libraries from proc_srclib into pg_plugin.
        # Keep both directories private to this run and preserve the original install.
        shutil.copytree(installed_prefix, prefix, symlinks=True)
        assert sha(prefix / 'bin/gaussdb') == qualified['gaussdb_sha256'], 'private Debug ELF digest'
        pkglib = prefix / 'lib/postgresql'
        source_lib, plugin_lib = pkglib / 'proc_srclib', pkglib / 'pg_plugin'
        for directory in (source_lib, plugin_lib):
            assert directory.is_dir() and directory.resolve().is_relative_to(prefix)
        staged_module = source_lib / module.name
        with module.open('rb') as src, staged_module.open('xb') as dst:
            shutil.copyfileobj(src, dst)
        assert sha(staged_module) == qualified['module_sha256'], 'staged private module digest'
        state['staged_module'] = str(staged_module)
        bootstrap(data, runtime, out, prefix=prefix)
        with (data / 'postgresql.conf').open('a') as conf:
            conf.write('\nenable_lake_maintenance = off\n')
        service('start', [ctl, 'start', '-w', '-t', '60', '-D', data, '-Z', 'single_node',
                          '-l', runtime/'server.log', '-o', f'-p {port}'])
        pid = int((data / 'postmaster.pid').read_text().splitlines()[0])
        running_binary = Path('/proc') / str(pid) / 'exe'
        check(running_binary.resolve() == (prefix / 'bin/gaussdb').resolve() and
              sha(running_binary) == qualified['gaussdb_sha256'], 'actual owned Debug ELF')
        state['pid'] = pid
        root = out / 'external'
        (root / 'nested').mkdir(parents=True)
        (out / 'outside').mkdir()
        shutil.copy2(fixture, root / 'nested/data.parquet')
        shutil.copy2(fixture, out / 'outside/data.parquet')
        (root / 'alias').symlink_to(out / 'outside', target_is_directory=True)
        (root / 'link.parquet').symlink_to(out / 'outside/data.parquet')
        (root / 'broken.parquet').write_bytes(b'test-invalid-parquet')
        os.mkfifo(root / 'pipe.parquet', 0o600)
        dbargs = dict(host=str(socket_dir), port=port, dbname='postgres', user=args.user, connect_timeout=10)
        with closing(psycopg2.connect(**dbargs)) as db:
            db.autocommit = True
            check(query(db, 'SHOW data_directory') == [(str(data),)], 'same owned PGDATA')
            query(db, "SET statement_timeout = '10s'")
            query(db, 'SET query_dop = 1')
            query(db, 'CREATE SCHEMA lab_external')
            query(db, sql.SQL("CREATE FUNCTION lab_external.read_it(text,text) RETURNS SETOF record "
                              "AS {}, 'lake_external_parquet_probe' LANGUAGE C VOLATILE NOT FENCED SECURITY INVOKER")
                  .format(sql.Literal('$libdir/proc_srclib/' + staged_module.name)))
            query(db, 'REVOKE ALL ON FUNCTION lab_external.read_it(text,text) FROM PUBLIC')
            pwd = 'Pg6!' + secrets.token_hex(12)
            query(db, sql.SQL('CREATE ROLE lab_external_reader LOGIN PASSWORD {}').format(sql.Literal(pwd)))
            query(db, 'GRANT USAGE ON SCHEMA lab_external TO lab_external_reader')
            query(db, 'GRANT EXECUTE ON FUNCTION lab_external.read_it(text,text) TO lab_external_reader')
            scan = 'SELECT text_col,id FROM lab_external.read_it(%s,%s) AS r(text_col text,id integer) ORDER BY id'
            params = (str(root), 'nested/data.parquet')
            expected = [(None if i%5 == 0 else '' if i%7 == 0 else '湖/csv', i) for i in range(5000)]
            observed = query(db, scan, params)
            check(observed == expected, 'all 5000 rows: exact names/types, reordered columns, NULL/empty/UTF-8')
            (out / 'observed.json').write_text(json.dumps(observed, ensure_ascii=False) + '\n')
            for name in ('../outside/data.parquet', 'alias/data.parquet', 'link.parquet', 'pipe.parquet', 'broken.parquet'):
                reject(db, 'HV000', scan, (str(root), name), name)
            reject(db, '42703', 'SELECT * FROM lab_external.read_it(%s,%s) AS r(no_such_field text)', params, 'missing field')
            reject(db, '42804', 'SELECT * FROM lab_external.read_it(%s,%s) AS r(id bigint)', params, 'physical type mismatch')
            reject(db, '0A000', 'SELECT * FROM lab_external.read_it(%s,%s) AS r(id numeric)', params, 'unsupported type')
            # Use the matching openGauss client for the server's noninitial-user
            # password authentication exchange.
            with db.cursor() as cursor:
                nonadmin_scan = cursor.mogrify(scan, params).decode('utf-8')
            nonadmin_file = out / 'nonadmin.sql'
            nonadmin_file.write_text(f'SELECT current_user;\n{nonadmin_scan};\n', encoding='utf-8')
            nonadmin_cmd = [str(prefix / 'bin/gsql'), '-X', '-A', '-t', '-q',
                            '-v', 'ON_ERROR_STOP=1', '-v', 'VERBOSITY=verbose',
                            '-h', str(socket_dir), '-p', str(port), '-d', 'postgres',
                            '-U', 'lab_external_reader', '-2', '-f', str(nonadmin_file)]
            state['commands'].append(nonadmin_cmd)
            nonadmin = subprocess.run(
                nonadmin_cmd, env={**env, 'LC_ALL': 'C'}, input=pwd + '\n',
                text=True, encoding='utf-8', capture_output=True, timeout=20)
            (out / 'nonadmin.stdout').write_text(nonadmin.stdout)
            (out / 'nonadmin.stderr').write_text(nonadmin.stderr)
            state['nonadmin_client_returncode'] = nonadmin.returncode
            rows = nonadmin.stdout.splitlines()
            if rows and re.fullmatch(r'total time:\s+[0-9]+\s+ms', rows[-1]):
                rows.pop()
            check(rows == ['lab_external_reader'], 'real nonadmin session')
            codes = re.findall(r'^SQLSTATE:\s+([0-9A-Z]{5})$', nonadmin.stderr, re.MULTILINE)
            message = 'ERROR:  external Parquet probe requires the invoking initial administrator'
            check(nonadmin.returncode != 0 and codes == ['42501'] and
                  any(line.endswith(message) for line in nonadmin.stderr.splitlines()),
                  'admin-only guard even with SQL EXECUTE grant')
            for attempt in range(3):
                check(query(db, scan, params) == expected, f'same connection after errors, scan {attempt}')
            check(query(db, "SELECT count(*) FROM pg_extension WHERE extname='og_lake'") == [(0,)],
                  'probe needs no managed lake catalog or publication')
        state['status'] = 'passed_private_sql_probe_only'
    except BaseException:
        state.update(status='failed', traceback=traceback.format_exc())
        raise
    finally:
        try:
            if (data / 'postmaster.pid').exists():
                owned_pid = int((data / 'postmaster.pid').read_text().splitlines()[0])
                assert sha(Path('/proc')/str(owned_pid)/'exe') == qualified['gaussdb_sha256']
                service('stop', [ctl, 'stop', '-D', data, '-m', 'fast', '-w'])
            state['stopped'] = not (data / 'postmaster.pid').exists()
            assert state['stopped']
        finally:
            state['finished_at'] = datetime.now(timezone.utc).isoformat()
            (out / 'result.json').write_text(json.dumps(state, ensure_ascii=False, indent=2) + '\n')


if __name__ == '__main__':
    main()