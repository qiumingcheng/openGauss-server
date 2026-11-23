#!/usr/bin/env python3
"""Build (without loading) the private SQL probe using an actual Debug compile entry."""
import argparse
from datetime import datetime, timezone
from hashlib import file_digest
import json
import os
from pathlib import Path
import shlex
import subprocess

HERE = Path(__file__).resolve().parent


def digest(path):
    with path.open('rb') as stream:
        return file_digest(stream, 'sha256').hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--code-base', type=Path, required=True)
    parser.add_argument('--compile-commands', type=Path, required=True,
                        help='compile database entry from the matching configured Debug build')
    parser.add_argument('--prefix', type=Path, required=True)
    parser.add_argument('--gaussdb-sha256', required=True)
    parser.add_argument('--sdk-library', type=Path, required=True)
    parser.add_argument('--sdk-sha256', required=True)
    parser.add_argument('--cc', required=True, help='matching C compiler, e.g. GCC10.3 gcc')
    parser.add_argument('--out-dir', type=Path, required=True, help='new, nonexistent build directory')
    args = parser.parse_args()
    if not __debug__:
        parser.error('assertions must be enabled')
    source = args.code_base.expanduser().resolve()
    prefix = args.prefix.expanduser().resolve()
    sdk = args.sdk_library.expanduser().resolve()
    database = prefix / 'bin/gaussdb'
    if digest(sdk) != args.sdk_sha256 or digest(database) != args.gaussdb_sha256:
        parser.error('SDK6 or gaussdb digest differs from the supplied build identity')
    compile_commands = args.compile_commands.resolve()
    entries = json.loads(compile_commands.read_text())
    def source_path(entry):
        return (Path(entry['directory']) / entry['file']).resolve()
    wanted = source / 'contrib/og_lake/lake_file_sdk.cpp'
    entries = [entry for entry in entries if source_path(entry) == wanted]
    if len(entries) != 1:
        parser.error('need exactly one lake_file_sdk.cpp entry from the matching source tree')
    entry = entries[0]
    argv = entry.get('arguments') or shlex.split(entry['command'])
    if not argv or any(token in {'&&', '||', '|', ';'} or token.startswith('@') for token in argv):
        parser.error('use a plain compiler entry without shell commands or response files')
    if Path(argv[0]).name in {'ccache', 'sccache', 'env'}:
        parser.error('export a compile entry with the actual compiler as argv[0]')
    directory = Path(entry['directory']).resolve()
    flags = []
    index = 1
    while index < len(argv):
        item = argv[index]
        if item in {'-o', '-MF', '-MT', '-MQ'}:
            index += 2
            continue
        if item in {'-c', '-MD', '-MMD', '-MP'}:
            index += 1
            continue
        if not item.startswith('-') and (directory / item).resolve() == wanted:
            index += 1
            continue
        flags.append(item)
        index += 1
    out = args.out_dir.expanduser().resolve()
    out.mkdir(parents=True, mode=0o700, exist_ok=False)
    state = {'status': 'building_unloaded', 'started_at': datetime.now(timezone.utc).isoformat(),
             'sdk_library': str(sdk), 'sdk_sha256': args.sdk_sha256,
             'gaussdb': str(database), 'gaussdb_sha256': args.gaussdb_sha256,
             'compile_commands': str(compile_commands),
             'compile_commands_sha256': digest(compile_commands), 'commands': []}
    state['sources'] = {name: digest(HERE / name) for name in
                        ('lake_external_path.c', 'lake_external_path.h', 'lake_external_sql_probe.cpp')}
    worktree = source / 'contrib/og_lake'
    c_object, cpp_object = out / 'path.o', out / 'probe.o'
    module = out / 'lake_external_sql_probe.so'
    commands = [
        [args.cc, '-std=c11', '-fPIC', '-Wall', '-Wextra', '-Werror', '-I', str(HERE),
         '-c', str(HERE / 'lake_external_path.c'), '-o', str(c_object)],
        [argv[0], *flags, '-fPIC', '-I', str(HERE), '-I', str(worktree / 'sdk_external'),
         '-I', str(worktree), '-DLAKE_PRIVATE_EXTERNAL_SDK_PATH=' + json.dumps(str(sdk)),
         '-c', str(HERE / 'lake_external_sql_probe.cpp'), '-o', str(cpp_object)],
        [argv[0], '-shared', '-o', str(module), str(cpp_object), str(c_object), '-ldl'],
    ]
    try:
        for index, command in enumerate(commands):
            state['commands'].append(command)
            with (out / f'build-{index}.log').open('x') as log:
                subprocess.run(command, cwd=directory, env=os.environ.copy(), stdout=log,
                               stderr=subprocess.STDOUT, timeout=300, check=True)
        assert digest(sdk) == args.sdk_sha256 and digest(database) == args.gaussdb_sha256
        state.update(status='built_unloaded', module_sha256=digest(module))
    except BaseException:
        state['status'] = 'build_failed'
        raise
    finally:
        state['finished_at'] = datetime.now(timezone.utc).isoformat()
        (out / 'module-build.json').write_text(json.dumps(state, indent=2) + '\n')


if __name__ == '__main__':
    main()
