"""Private typed campaign adapter. No database/runtime imports or services.

Native admission intentionally closed until a separately sealed SQL26 source,
helper, SDK, binary and shared timing composition exists. This module implements
only receipt schema adaptation and external single-supervisor identity proof.
"""
import hashlib
import json
import os
import re
from pathlib import Path


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def require_typed_campaign_admission(args, built):
    # Exact full26/source/SDK/QA validation occurs before configuration mutation.
    from native_parent_sql026_admission import require_campaign_admission
    return require_campaign_admission(args, built)


def external_identity():
    """Attest this direct child; controller independently proves the whole group."""
    nonce = os.environ.get('OG_LAKE_CACHE_OWNER_NONCE', '')
    parent = os.environ.get('OG_LAKE_PARENT_TEST_PARENT', '')
    if not re.fullmatch(r'[0-9a-f]{64}', nonce) or parent != str(os.getppid()):
        raise AssertionError('Missing exact campaign parent/nonce contract')
    pid = os.getpid()
    path = Path('/proc')/str(pid)
    stat = (path/'stat').read_text()
    tail = stat[stat.rfind(')')+2:].split()
    identity = dict(pid=pid, parent_pid=os.getppid(), pgid=int(tail[2]),
                    sid=int(tail[3]), start_ticks=int(tail[19]), uid=path.stat().st_uid,
                    nonce_sha256=hashlib.sha256(nonce.encode()).hexdigest(),
                    supervisor_kind='ordinary_rowhead_campaign_direct_child')
    if identity['pgid'] != pid or identity['sid'] != pid or identity['uid'] != os.getuid():
        raise AssertionError('Typed auto child is not the campaign-owned session leader')
    return identity


def verify_typed_child(path, run, built, enabled_receipt, installed_sql, runtime, ownership):
    """Verify real typed result and owned campaign journal, never synthesize PASS."""
    path, enabled_receipt = Path(path), Path(enabled_receipt)
    result = json.loads(path.read_text())
    def require(value, reason):
        if not value:
            raise AssertionError(reason)
    require(installed_sql == runtime == '0.26', 'Typed auto requires26/26')
    require(result.get('status') == 'pass' and result.get('executed') is True,
            'Original typed child did not execute and pass')
    require(result.get('mode') in ('rowhead-auto', 'rowhead-vacuum-reuse'), 'Wrong typed auto mode')
    require(result['mode'] == ownership.get('case'), 'Typed child mode differs from selected campaign case')
    require(result.get('installed_sql') == installed_sql and
            result.get('runtime_module_version') == runtime, 'Wrong actual runtime/SQL')
    require(result.get('source_head') == built['source_head'] and
            result.get('binary_sha256') == built['binary_sha256'], 'Wrong child product binary')
    require(result.get('build_sha256') == digest(enabled_receipt) and
            Path(result['build']).resolve() == enabled_receipt.resolve(), 'Wrong enabled receipt')
    require(bool(result.get('assertions')), 'No actual assertions')
    for item in result['assertions']:
        require(item['status'] == 'pass' and item['observed'] == item['expected'], 'Failed assertion')
    require(bool(result.get('inputs')), 'Missing executed inputs')
    archive_root = (path.parent/'executed-inputs').resolve()
    archived = {}
    for name, wanted in result['inputs'].items():
        require(isinstance(wanted, dict) and set(wanted) == {'sha256', 'archive'}, 'Wrong typed input schema')
        copy = Path(wanted['archive'])
        require(copy.resolve().parent == archive_root and not copy.is_symlink(), 'Input archive escaped attempt')
        require(digest(name) == wanted['sha256'] == digest(copy), 'Input/archive changed: '+name)
        require(str(copy.resolve()) not in archived, 'Input archives collide')
        archived[str(copy.resolve())] = name
    build_key = str(enabled_receipt.resolve())
    require(build_key in result['inputs'] and
            result['inputs'][build_key]['sha256'] == digest(enabled_receipt), 'Official build not archived')
    require(bool(result.get('hash_only_inputs')), 'Missing executable/SDK hashes')
    for name, wanted in result['hash_only_inputs'].items():
        require(digest(name) == wanted, 'Executable/SDK changed: '+name)
    # Campaign supervisor owns all processes, not the fixture's nested supervisor.
    require(ownership.get('status') == 'drained' and ownership.get('drained') is True and
            ownership.get('returncode') == 0 and ownership.get('remaining') == [] and
            ownership.get('timed_out') is False, 'Campaign child supervision did not pass')
    require('ownership_refused' not in ownership, 'Campaign ownership proof refused')
    require(not ownership.get('retirement', {}).get('signals'), 'Signalled child is not a passing case')
    identity = ownership['identity']
    actual = result.get('external_supervision', {})
    require(actual.get('supervisor_kind') == 'ordinary_rowhead_campaign_direct_child', 'Nested/unknown supervisor')
    for field in ('pid', 'pgid', 'sid', 'start_ticks', 'uid'):
        require(actual.get(field) == identity[field], 'Child ownership identity changed/'+field)
    require(actual.get('nonce_sha256') == ownership['nonce_sha256'] and
            actual.get('nonce_sha256') == identity['nonce_sha256'], 'Child nonce digest mismatch')
    require(actual.get('parent_pid') == ownership['campaign_pid'], 'Child parent mismatch')
    require(actual['pid'] == actual['pgid'] == actual['sid'], 'Direct child session mismatch')
    require(result.get('typed_claim_released_before_cleanup') is True and
            'typed_claim_release_error' not in result, 'Owned maintenance claim release failed')
    case = result.get('typed_automatic_rowhead', {})
    require(case.get('status') == 'case_checks_completed' and case.get('mode') == result['mode'],
            'Typed automatic case checks missing')
    require(case.get('manual_reclaim_calls') == 0 and case.get('mapping_mutations') == 0 and
            bool(case.get('worker_identity_proofs')), 'Real automatic worker proof missing')
    if result['mode'] == 'rowhead-vacuum-reuse':
        require(case.get('vacuum_and_reuse_case_checks_completed') is True and
                case.get('space_reuse', {}).get('rows_per_leaf') == 20000,
                'Actual two-leaf vacuum and page reuse checks missing')
    cleanup = result.get('database_cleanup', {})
    require(cleanup.get('returncode') == 0, 'Database cleanup failed')
    summary = json.loads(cleanup['stdout'])
    require(summary.get('status') == 'pass', 'Cleanup summary failed')
    owner = json.loads((path.parent/'database-owner.json').read_text())
    require(owner['run_id'] == run == summary['run_id'], 'Exact owned run mismatch')
    require(Path(owner['attempt']).resolve() == path.parent.resolve(), 'Owned attempt mismatch')
    require(owner['database_oid'] == summary.get('dropped_database_oid'), 'Exact DB OID cleanup missing')
    report = Path(summary['immutable_report'])
    require(report.resolve().parent == path.parent.resolve() and not report.is_symlink(), 'Cleanup report escaped attempt')
    detail = json.loads(report.read_text())
    require(detail['status'] == 'pass' and detail['dropped_database_oid'] == owner['database_oid'],
            'Immutable owned cleanup mismatch')
    require(result.get('role_cleanup', {}).get('status') == 'pass', 'Role cleanup failed')
    return dict(result_sha256=digest(path), assertions=len(result['assertions']),
                cleanup_report=str(report), cleanup_report_sha256=digest(report))
