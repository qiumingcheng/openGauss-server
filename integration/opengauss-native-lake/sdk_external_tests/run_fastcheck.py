#!/usr/bin/env python3
"""Prepare and run the existing private SDK6 SQL probe for pg_regress."""

import json
import os
from pathlib import Path
import pwd
import shutil
import socket
import subprocess
import sys
import tempfile
from contextlib import ExitStack
from hashlib import file_digest

HERE = Path(__file__).resolve().parent
FIXTURE = HERE / 'fixtures/external_ordinal_5000.parquet'
FIXTURE_SHA256 = '454b450b7d9770406dc14c0005c9f7f36cfb96fdcc1d7ccd50d020e17279d7f4'


def sha256(path):
    with path.open('rb') as f:
        return file_digest(f, 'sha256').hexdigest()


def available_port():
    for _ in range(32):
        with ExitStack() as stack:
            listener = stack.enter_context(socket.socket())
            listener.bind(('127.0.0.1', 0))
            port = listener.getsockname()[1]
            if port > 65533:
                continue
            try:
                for extra in (1, 2):
                    neighbor = stack.enter_context(socket.socket())
                    neighbor.bind(('127.0.0.1', port + extra))
            except OSError:
                continue
            return port
    raise RuntimeError('cannot find three consecutive local ports for the isolated cluster')


def main():
    artifact = scratch = None
    try:
        if not __debug__:
            raise RuntimeError('SDK6 FastCheck requires Python assertions')
        config_path = os.environ.get('OG_LAKE_SDK6_FASTCHECK_CONFIG')
        if not config_path:
            raise ValueError('set OG_LAKE_SDK6_FASTCHECK_CONFIG; see sdk_external_tests/README.md')
        config_path = Path(config_path).resolve(strict=True)
        config = json.loads(config_path.read_text())
        required = {'prefix', 'gaussdb_sha256', 'sdk_library', 'sdk_sha256', 'compile_commands', 'cc'}
        optional = {'library_path', 'port', 'runtime_root'}
        if not isinstance(config, dict) or not required <= config.keys() <= required | optional:
            raise ValueError('invalid SDK6 FastCheck config keys; see sdk_external_tests/README.md')

        def configured_path(key):
            return str((config_path.parent / config[key]).resolve(strict=True))

        prefix = configured_path('prefix')
        compiler = configured_path('cc')
        sdk = configured_path('sdk_library')
        compile_db = configured_path('compile_commands')
        if sha256(FIXTURE) != FIXTURE_SHA256:
            raise ValueError('SDK6 FastCheck fixture digest mismatch')
        port = config.get('port')
        if port is None:
            port = available_port()
        elif type(port) is not int or not 1024 <= port <= 65533:
            raise ValueError('SDK6 FastCheck port must be between 1024 and 65533')

        artifact_root = Path(os.environ.get('OG_LAKE_SDK6_FASTCHECK_ARTIFACTS', 'results')).resolve()
        artifact_root.mkdir(parents=True, exist_ok=True)
        artifact = Path(tempfile.mkdtemp(prefix='og_lake_sdk6_', dir=artifact_root))
        runtime_root = configured_path('runtime_root') if 'runtime_root' in config else '/tmp'
        scratch = Path(tempfile.mkdtemp(prefix='og-sdk6-', dir=runtime_root))
        socket_path = scratch / f'pgl2-000000000000/socket/.s.PGSQL.{port}'
        if len(os.fsencode(socket_path)) >= 108:
            raise ValueError('choose a shorter SDK6 FastCheck runtime_root for Unix sockets')
        # FastCheck sets PGHOST, PGPORT and PGOPTIONS for its own instance.
        # The private test uses only its explicit Unix socket and fresh PGDATA.
        env = {k: v for k, v in os.environ.items() if not k.startswith('PG')}
        if 'library_path' in config:
            library_path = config['library_path']
            if not isinstance(library_path, str) or any(not Path(p).is_absolute() for p in library_path.split(':')):
                raise ValueError('SDK6 FastCheck library_path needs colon-separated absolute paths')
            env['LD_LIBRARY_PATH'] = library_path

        def run(script, args, log_name):
            with (artifact / log_name).open('x') as log:
                subprocess.run([sys.executable, str(HERE / script), *map(str, args)],
                               env=env, stdout=log, stderr=subprocess.STDOUT, check=True)

        module_dir = artifact / 'module'
        run('build_sql_prototype.py', [
            '--code-base', HERE.parents[2], '--compile-commands', compile_db,
            '--prefix', prefix, '--gaussdb-sha256', config['gaussdb_sha256'],
            '--sdk-library', sdk, '--sdk-sha256', config['sdk_sha256'],
            '--cc', compiler, '--out-dir', module_dir,
        ], 'build.log')
        module_receipt = module_dir / 'module-build.json'
        sql_out = artifact / 'sql'
        run('run_sql_prototype.py', [
            '--run-id', 'fastcheck', '--workspace', HERE.parents[2],
            '--module-build', module_receipt, '--module-build-sha256', sha256(module_receipt),
            '--prefix', prefix, '--fixture', FIXTURE, '--fixture-sha256', FIXTURE_SHA256,
            '--out-dir', sql_out, '--runtime-root', scratch, '--port', port,
            '--user', pwd.getpwuid(os.getuid()).pw_name,
        ], 'probe.log')
        result = json.loads((sql_out / 'result.json').read_text())
        checks = result['checks']
        if (result['status'] != 'passed_private_sql_probe_only' or result['stopped'] is not True or
                len(checks) != 17 or any(check['passed'] is not True for check in checks)):
            raise RuntimeError('SDK6 SQL receipt does not show all 17 checks and a stopped instance')
        shutil.rmtree(scratch)
    except (OSError, ValueError, RuntimeError, KeyError, TypeError, subprocess.CalledProcessError) as exc:
        print(f'SDK6 isolated SQL FAILED: {exc}', file=sys.stderr)
        if artifact is not None:
            print(f'Evidence: {artifact}', file=sys.stderr)
        if scratch is not None:
            print(f'Private runtime: {scratch}', file=sys.stderr)
        return 1

    print('SDK6 isolated SQL: 17/17 passed')
    return 0


if __name__ == '__main__':
    sys.exit(main())
