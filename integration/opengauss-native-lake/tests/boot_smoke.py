#!/usr/bin/env python3
"""BOOT-01: validate real full build/initialization evidence, restart and SQL failure propagation."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import traceback
import uuid

DRIVER = Path(__file__).resolve().parents[1]
ROOT = DRIVER.parents[1]
sys.path.insert(0, str(DRIVER))
import build_driver
from lake_harness import Blocked, sha256, source_identity, validate_run_id


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--run-id', default=os.getenv('OG_LAKE_RUN_ID'))
    parser.add_argument('--output', default=os.getenv('OG_LAKE_RESULT_FILE'))
    parser.add_argument('--build-evidence', default=os.getenv('OG_LAKE_BOOT_BUILD_EVIDENCE'))
    parser.add_argument('--full-build-evidence', default=os.getenv('OG_LAKE_BOOT_FULL_BUILD_EVIDENCE'))
    parser.add_argument('--bootstrap-evidence', default=os.getenv('OG_LAKE_BOOT_BOOTSTRAP_EVIDENCE'))
    args = parser.parse_args()
    if not args.run_id or not args.output:
        parser.error('--run-id and --output (or harness environment) required')
    validate_run_id(args.run_id)
    output = Path(args.output).resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    runtime = ROOT / '.runtime/opengauss-native-lake/debug'
    data = runtime / 'data'
    install = ROOT / '.install/opengauss-native-lake-debug'
    source = ROOT / '.worktrees/opengauss-native-lake'
    binary = install / 'bin/gaussdb'
    evidence_root = ROOT / '公共文档/openGauss原生湖存储/验证'
    result = {'status': 'fail', 'scope': 'BOOT-01', 'assertions': [], 'artifacts': [],
              'notes': [
                  'Accepts a real isolated Debug/full receipt, or a successful cpp-only incremental descendant plus its full receipt, and a real initialization receipt.',
                  'Verifies tracked commit/diff plus installed/running binary hashes; does not claim a reproducible build of every transitive dependency.',
                  'Uncompiled N1 staging files in contrib/og_lake/new are not claimed as part of the recorded full build.',
                  'No per-file initial hashes of the pre-existing dirty user trees were captured; only branch/HEAD/status equality over this test is asserted.',
                  'The successful restart leaves only the already-running owned Debug instance running.']}

    def assertion(name, observed, expected=True):
        passed = observed == expected
        result['assertions'].append({'name': name, 'status': 'pass' if passed else 'fail',
                                     'details': {'observed': observed, 'expected': expected}})
        if not passed:
            raise AssertionError(name)

    def artifact(path):
        path = Path(path).resolve()
        if not path.is_file():
            raise Blocked(f'Required evidence artifact is missing: {path}')
        result['artifacts'].append({'path': str(path)})

    def git(tree, *arguments):
        return subprocess.check_output(['git', '-C', str(tree), *arguments], timeout=30)

    def worktree_state(tree):
        return {'path': str(tree), 'branch': git(tree, 'branch', '--show-current').decode().strip(),
                'head': git(tree, 'rev-parse', 'HEAD').decode().strip(),
                'status': git(tree, 'status', '--porcelain=v1', '--untracked-files=all').decode().splitlines()}

    def running():
        pid = int((data / 'postmaster.pid').read_text().splitlines()[0])
        actual = Path(f'/proc/{pid}/exe')
        if actual.resolve() != binary:
            raise Blocked('Running executable is outside the owned Debug installation')
        return {'pid': pid, 'binary_path': str(actual.resolve()), 'binary_sha256': sha256(actual)}

    def foreign_database_processes():
        observed = []
        for proc in Path('/proc').iterdir():
            if not proc.name.isdigit():
                continue
            try:
                exe = (proc / 'exe').resolve(strict=True)
                if exe.name not in ('gaussdb', 'postgres', 'duckdb') or exe == binary:
                    continue
                observed.append({'pid': int(proc.name), 'exe': str(exe)})
            except (OSError, RuntimeError):
                pass
        return sorted(observed, key=lambda item: item['pid'])

    def containers():
        try:
            response = subprocess.run(['podman', 'ps', '--format', '{{.ID}} {{.Names}} {{.State}}'],
                                      capture_output=True, text=True, timeout=15)
        except (OSError, subprocess.TimeoutExpired):
            return None
        return sorted(response.stdout.splitlines()) if response.returncode == 0 else None

    before_trees = None
    before_processes = None
    before_containers = None
    before_conf = None
    try:
        if not args.build_evidence or not args.bootstrap_evidence:
            raise Blocked('Set OG_LAKE_BOOT_BUILD_EVIDENCE and OG_LAKE_BOOT_BOOTSTRAP_EVIDENCE to real completed receipts.')
        build_path = Path(args.build_evidence).resolve()
        bootstrap_path = Path(args.bootstrap_evidence).resolve()
        artifact(build_path)
        artifact(bootstrap_path)
        build = json.loads(build_path.read_text())
        bootstrap = json.loads(bootstrap_path.read_text())
        if build.get('mode') == 'incremental':
            if not args.full_build_evidence:
                raise Blocked('Incremental acceptance requires OG_LAKE_BOOT_FULL_BUILD_EVIDENCE for the successful full ancestor.')
            full_path = Path(args.full_build_evidence).resolve()
            artifact(full_path)
            full = json.loads(full_path.read_text())
        else:
            full_path, full = build_path, build
        expected_identity = {'project': 'opengauss-native-lake', 'source': str(source), 'data': str(data),
                             'install': str(install), 'profile': 'debug', 'port': 55560}
        owner = json.loads((runtime / 'owner.json').read_text())
        assertion('owned Debug runtime marker', owner, expected_identity)
        assertion('latest compile succeeded', build.get('mode') in ('full', 'incremental') and
                  build.get('status') == 'build_and_sql_passed')
        assertion('complete full ancestor build succeeded', [full.get('mode'), full.get('status')],
                  ['full', 'build_and_sql_passed'])
        assertion('latest build used isolated registered paths',
                  {key: build.get(key) for key in expected_identity}, expected_identity)
        assertion('full ancestor used identical registered configuration paths',
                  {key: full.get(key) for key in expected_identity}, expected_identity)
        assertion('real bootstrap initialized this data directory',
                  [bootstrap.get('status'), bootstrap.get('data'), bootstrap.get('compatibility')],
                  ['initialized_only', str(data), 'PG'])
        for path in (build_path.parent / 'build.log', build_path.parent / 'sql.log',
                     full_path.parent / 'build.log', full_path.parent / 'sql.log',
                     bootstrap_path.parent / 'bootstrap.log'):
            artifact(path)
        assertion('data is initialized', (data / 'PG_VERSION').is_file())
        assertion('isolated source branch', git(source, 'branch', '--show-current').decode().strip(),
                  'feature/native-lake')
        assertion('latest build HEAD matches current tracked source', build.get('source_head'),
                  git(source, 'rev-parse', 'HEAD').decode().strip())
        assertion('latest build tracked diff matches current source', build.get('source_diff_sha256'),
                  hashlib.sha256(git(source, 'diff', 'HEAD')).hexdigest())
        if build.get('mode') == 'incremental':
            if full.get('source_diff_sha256') != hashlib.sha256(b'').hexdigest():
                raise Blocked('Cannot reconstruct an incremental ancestry from a dirty full build using only its diff hash.')
            ancestry = subprocess.run(['git', '-C', str(source), 'merge-base', '--is-ancestor',
                                       full['source_head'], build['source_head']], capture_output=True)
            assertion('full build commit is an ancestor of latest incremental', ancestry.returncode, 0)
            changed = set(git(source, 'diff', '--name-only', full['source_head'], build['source_head']).decode().splitlines())
            changed.update(git(source, 'diff', '--name-only', 'HEAD').decode().splitlines())
            result['incremental_changed_files'] = sorted(changed)
            if any(Path(name).suffix != '.cpp' for name in changed):
                raise Blocked('Incremental build chain contains non-cpp files; require a new full build for headers/build rules/configuration changes.')
            assertion('incremental ancestry contains only cpp edits and no header/build-rule changes',
                      all(Path(name).suffix == '.cpp' for name in changed))
        assertion('installed binary matches latest successful compile', sha256(binary), build.get('binary_sha256'))
        assertion('latest compile verified installed and running identity', build.get('running_binary_sha256'),
                  build.get('binary_sha256'))
        current = running()
        assertion('currently running binary matches latest successful compile', current['binary_sha256'],
                  build['binary_sha256'])
        build_entry = ROOT / 'deliverables/multimodal_optimizer/opengauss_build_install_restart.sh'
        assertion('latest build used current authoritative Debug entry', build.get('build_entry_sha256'), sha256(build_entry))
        assertion('full ancestor used the same authoritative Debug configuration', full.get('build_entry_sha256'), sha256(build_entry))
        config = (source / 'src/Makefile.global').read_text()
        assertion('Debug source configuration targets isolated install', '--enable-debug' in config and str(install) in config)
        result['source_identity_at_acceptance'] = source_identity(source)
        result['running_before'] = current
        tracked_sources = git(source, 'ls-files', 'contrib/og_lake', 'src/include/access/og_lake.h').decode().splitlines()
        assertion('host module implementation was tracked before acceptance',
                  all(name in tracked_sources for name in ('contrib/og_lake/lake_host.cpp',
                                                          'contrib/og_lake/og_lake.cpp',
                                                          'src/include/access/og_lake.h')))
        result['tracked_lake_sources'] = tracked_sources
        before_trees = [worktree_state(ROOT / name) for name in
                        ('openGauss-server', 'openGauss-hybrid-recall-consolidate') if (ROOT / name / '.git').exists()]
        before_processes = foreign_database_processes()
        before_containers = containers()
        before_conf = sha256(data / 'postgresql.conf')
        restart_id = args.run_id[:64] + '-boot-restart-' + uuid.uuid4().hex[:10]
        restart_log = output.parent / 'restart.log'
        with restart_log.open('w') as log:
            process = subprocess.run([str(DRIVER / 'build.sh'), 'debug', 'restart', '--run-id', restart_id],
                                     stdout=log, stderr=subprocess.STDOUT, timeout=210)
        artifact(restart_log)
        assertion('actual owned restart command succeeds', process.returncode, 0)
        restart_receipt = evidence_root / restart_id / 'build.json'
        artifact(restart_receipt)
        restart = json.loads(restart_receipt.read_text())
        assertion('restart receipt reports actual SQL success', [restart.get('mode'), restart.get('status')],
                  ['restart', 'build_and_sql_passed'])
        artifact(restart_receipt.parent / 'build.log')
        artifact(restart_receipt.parent / 'sql.log')
        after = running()
        result['running_after'] = after
        assertion('restart produced a new owned server process', after['pid'] != current['pid'])
        assertion('restart preserved latest accepted compile binary', after['binary_sha256'], build['binary_sha256'])

        env = os.environ.copy()
        env.update(GAUSSHOME=str(install), PGHOST=str(runtime / 'socket'), PGPORT='55560',
                   PGUSER=env.get('USER', 'qiumc'),
                   LD_LIBRARY_PATH=f'{install}/lib:{ROOT}/binarylibs/buildtools/gcc10.3/gcc/lib64')
        common = [install / 'bin/gsql', '-X', '-A', '-t', '-v', 'ON_ERROR_STOP=1',
                  '-h', runtime / 'socket', '-p', '55560']
        positive_log = output.parent / 'post-restart-sql.log'
        with positive_log.open('w') as log:
            build_driver.run(common + ['-d', 'postgres', '-c',
                             "SELECT 6 * 7, current_database(), current_setting('port');"], env, log)
        artifact(positive_log)
        assertion('real SQL result after restart', '42|postgres|55560' in positive_log.read_text().splitlines())
        missing_db = 'lake_boot_missing_' + uuid.uuid4().hex[:16]
        existence = subprocess.run([str(item) for item in common] + ['-d', 'postgres', '-c',
                                    "SELECT count(*) FROM pg_database WHERE datname = '" + missing_db + "';"],
                                   env=env, capture_output=True, text=True, timeout=15)
        assertion('negative test database is absent', existence.returncode == 0 and '0' in existence.stdout.splitlines())
        failed_log = output.parent / 'intentional-sql-connect-failure.log'
        caught = None
        with failed_log.open('w') as log:
            try:
                build_driver.run(common + ['-d', missing_db, '-c', 'SELECT 42;'], env, log)
            except RuntimeError as exc:
                caught = str(exc)
        artifact(failed_log)
        result['expected_connection_failure'] = caught
        assertion('build_driver.run raises on real nonzero SQL connection failure',
                  caught is not None and re.search(r'exited [1-9][0-9]*;', caught) is not None)
        assertion('failure log identifies deliberately missing database', missing_db in failed_log.read_text())
        assertion('database remains healthy after rejected SQL connection', running()['binary_sha256'], build['binary_sha256'])
        assertion('current source remains unchanged over restart acceptance', source_identity(source),
                  result['source_identity_at_acceptance'])
        result['status'] = 'pass'
    except Blocked as exc:
        result['status'] = 'blocked'
        result['reason'] = str(exc)
    except BaseException as exc:
        result['error'] = repr(exc)
        result['traceback'] = traceback.format_exc()
    finally:
        try:
            if before_trees is not None:
                after_trees = [worktree_state(Path(item['path'])) for item in before_trees]
                tree_artifact = output.parent / 'user-worktree-status.json'
                tree_artifact.write_text(json.dumps({'before': before_trees, 'after': after_trees,
                    'boundary': 'Branch/HEAD/status only, no original dirty-file byte hashes.'}, indent=2) + '\n')
                artifact(tree_artifact)
                assertion('user worktree branch HEAD status unchanged during this test', after_trees, before_trees)
            if before_processes is not None:
                assertion('unrelated database processes unchanged during this test', foreign_database_processes(), before_processes)
            if before_containers is not None:
                assertion('running container membership/state unchanged during this test', containers(), before_containers)
            if before_conf is not None:
                assertion('owned Debug configuration unchanged', sha256(data / 'postgresql.conf'), before_conf)
        except BaseException as exc:
            result['status'] = 'fail'
            result['preservation_error'] = repr(exc)
        output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n')
        print(json.dumps({'status': result['status'], 'output': str(output),
                          'assertions': len(result['assertions'])}, ensure_ascii=False))
    return {'pass': 0, 'blocked': 2}.get(result['status'], 1)


if __name__ == '__main__':
    raise SystemExit(main())
