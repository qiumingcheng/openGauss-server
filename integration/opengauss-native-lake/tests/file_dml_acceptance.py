#!/usr/bin/env python3
"""Run bounded file DML and affected regressions against an exact native build.

This suite does not promote N4, SDK, flush, RAC or commercial readiness. Each
case uses the existing ownership, runtime, fixture and artifact verifiers.
"""
import argparse
import json
import shutil
import sys
from pathlib import Path

DRIVER = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(DRIVER))
from lake_harness import (Blocked, Context, SOURCE, fingerprint, now, read_json,
                          run_case, sha256, source_identity, verify_result, write_json)
from native_runtime import PROFILE, RUNTIME, PORT, INSTALL

CURRENT = [
    'N4/file_stats_unknown', 'N4/file_commands', 'N4/file_delete', 'N4/file_insert',
    'N4/file_repeat', 'N4/file_rollback', 'N4/file_savepoints', 'N4/file_update_delete',
    'N4/file_versioned', 'N4/file_limits', 'N4/file_conflict', 'N4/file_cancel',
    'N4/file_acl', 'N4/file_interop', 'N4/file_schema_gap',
    'CREATE/current_net_dml', 'CREATE/current_savepoints', 'CREATE/current_publish_cancel',
]
COMPAT = [
    'N2/local_file_versions_inline_update', 'N2/local_file_versions_inline_delete',
    'N2/local_file_versions_explicit', 'N2/local_file_versions_delete_errors',
    'N2/local_file_versions_rescan', 'N2/local_file_versions_acl',
    'N2/local_file_versions_upgrade', 'N2/local_file_inline_update',
    'N2/local_file_inline_delete', 'N2/local_file_legacy', 'N2/local_file_read',
    'N2/local_file_rescan', 'N2/local_file_acl', 'N2/local_file_errors',
    'N2/local_file_lifetime', 'N2/local_file_types', 'N2/local_file_batch',
    'N2/local_file_mixed', 'N2/local_file_cancel', 'N2/local_file_terminate',
    'CREATE/savepoints', 'CREATE/permissions', 'CREATE/upgrade',
    'CREATE/publish_cancel', 'CREATE/prepared', 'CREATE/empty', 'CREATE/net_dml',
    'ABI-01/host', 'N3/empty_insert_first', 'N3/empty_insert_rollback',
    'N3/late_precommit_callback', 'N4/modify_commands', 'N4/modify_savepoints',
    'N4/locator_codec_marker', 'N2/inline_typed', 'ACL/columns',
    'STAT-01/inline_private', 'CATALOG/initial', 'N3/empty_insert_concurrency',
    'N3/insert_concurrency', 'ACL/views', 'CREATE/typed', 'CREATE/multiple',
    'CREATE/names', 'CREATE/merged_create', 'CREATE/conflicting_create',
    'CREATE/merged_insert', 'N2/local_file_versions_batch',
]


def check_build(path):
    build = read_json(path)
    if build.get('status') != 'build_and_sql_passed':
        raise Blocked('An official successful build and SQL receipt is required')
    if (build.get('profile') != PROFILE or build.get('port') != PORT or
            Path(build.get('source', '')).resolve() != SOURCE.resolve() or
            Path(build.get('install', '')).resolve() != INSTALL.resolve() or
            Path(build.get('data', '')).resolve() != (RUNTIME / 'data').resolve()):
        raise Blocked('Build receipt belongs to another runtime profile or endpoint')
    if build.get('native_source_inputs_before') != build.get('native_source_inputs_after'):
        raise Blocked('Native build inputs changed during compilation')
    if ('build_wrapper_inputs_before' in build and
            build['build_wrapper_inputs_before'] != build.get('build_wrapper_inputs_after')):
        raise Blocked('Build/profile adapter changed during compilation')
    identity = source_identity(SOURCE)
    if (identity['head'] != build['source_head'] or
            identity['tracked_diff_sha256'] != build['source_diff_sha256']):
        raise Blocked('Product source no longer matches the official build')
    for name, expected in build['native_source_inputs_after'].items():
        if sha256(Path(SOURCE) / name) != expected:
            raise Blocked('Built native input changed: ' + name)
    binary = Path(build['install']) / 'bin/gaussdb'
    if sha256(binary) != build['binary_sha256']:
        raise Blocked('Installed native binary differs from the official build')
    try:
        pid = int((RUNTIME / 'data/postmaster.pid').read_text().splitlines()[0])
        running = Path('/proc') / str(pid) / 'exe'
        if running.resolve() != binary.resolve() or sha256(running) != build['binary_sha256']:
            raise Blocked('Running native binary differs from the selected official build')
    except (OSError, ValueError, IndexError) as exc:
        raise Blocked('Selected native runtime is not verifiably running') from exc
    return build


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--build-receipt', required=True, type=Path)
    parser.add_argument('--suite', choices=['current', 'compat'], default='current')
    parser.add_argument('--start-at', help='Run an explicit suffix; previous cases require separate strict verification')
    parser.add_argument('--only', action='append', help='Run only named cases in this suite; omitted cases require separate evidence')
    args = parser.parse_args()
    ctx = Context(args.run_id)
    ctx.run_dir.mkdir(parents=True, exist_ok=False)
    index = ctx.run_dir / 'index.json'
    report = {'status': 'running', 'scope': 'bounded file DML ' + args.suite,
              'started_at': now(), 'cases': [], 'case_count': 0, 'assertions': 0,
              'limitations': ['Not full N4/SDK/flush/remote/RAC/commercial qualification.']}
    try:
        build = check_build(args.build_receipt)
        report.update(source_head=build['source_head'], binary_sha256=build['binary_sha256'],
                      build=str(args.build_receipt.resolve()), build_sha256=sha256(args.build_receipt))
        shutil.copy2(__file__, ctx.run_dir / 'executed-suite.py')
        report['executed_suite_sha256'] = sha256(__file__)
        frozen = fingerprint()
        selected = list(CURRENT if args.suite == 'current' else COMPAT)
        if args.start_at is not None:
            if args.start_at not in selected:
                raise Blocked('start-at must name a case in the selected suite')
            selected = selected[selected.index(args.start_at):]
            report['scope'] += ' suffix from ' + args.start_at
        if args.only is not None:
            if args.start_at is not None or any(name not in selected for name in args.only):
                raise Blocked('only must select suite members and cannot combine with start-at')
            selected = [name for name in selected if name in args.only]
            report['scope'] += ' explicit subset'
        report['selected_cases'] = selected
        for case_id in selected:
            if fingerprint() != frozen:
                raise Blocked('Suite source/lock/harness changed before ' + case_id)
            result, path = run_case(case_id, ctx)
            item = {'case': case_id, 'status': result['status'], 'reason': result.get('reason'),
                    'result': str(path), 'result_sha256': sha256(path),
                    'assertions': len(result.get('assertions', []))}
            report['cases'].append(item)
            report['case_count'] = len(report['cases'])
            report['assertions'] += item['assertions']
            write_json(index, report)
            print(json.dumps(item, ensure_ascii=False), flush=True)
            if result['status'] != 'pass':
                raise Blocked('Stopped on first non-passing case: ' + case_id)
            verify_result(path, ctx)
        for item in report['cases']:
            verify_result(item['result'], ctx)
        check_build(args.build_receipt)
        if sha256(__file__) != report['executed_suite_sha256']:
            raise Blocked('Acceptance suite changed during execution')
        report.update(status='passed', all_results_strictly_verified=True)
    except BaseException as exc:
        report.update(status='incomplete', reason=str(exc))
        raise
    finally:
        report['finished_at'] = now()
        write_json(index, report)
    print(json.dumps({'status': report['status'], 'index': str(index),
                      'case_count': report['case_count'], 'assertions': report['assertions']}))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
