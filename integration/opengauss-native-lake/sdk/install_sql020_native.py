#!/usr/bin/env python3
"""Prepare by default; install an exactly qualified signed16 or temporal SDK only while both
Debug and ann-debug sharing its prefix are stopped. Never stops/starts services,
builds Native, accepts a legacy/unknown qualification kind or removes old SDKs.
The unchanged install_native.py remains the separate strict legacy entry.
"""
import argparse
import ctypes
import datetime
import fcntl
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import sys
import uuid

SDK_KIND = 'verified_independent_sdk_sql020_int16_only'
TEMPORAL_SDK_KIND = 'verified_independent_sdk_temporal_only'
DECIMAL54_SDK_KIND = 'verified_independent_sdk_decimal128_only'
DECIMAL54_HELPER_SHA256 = '9693ac2530288cfb2641631c385ce45e5d832972772152d43b738e7f1051eb06'
# Exact control of the independently reviewed public-snapshot55 Native Source.
NATIVE55_CONTROL_SHA256 = '19efbf80fc20d9c6b037d7d99be068efa8ec1e51cc33aaf9346b143148922f30'
TEMPORAL_HELPER_SHA256 = '84b07ae870143000a94a49de592c9c8dda642e7f09f0508080031fb4e4778e13'
HELPER_SHA256 = '8922e0d18c53879102988318cab7ec82787dd64af5ce7668502a94fedeecf87a'
LEGACY_HELPER_SHA256 = 'bf8a6480827df6ae8b5aa22171eb177759318a60b9c09e0a55a09ea6b54beab0'
OWNER_PROJECT = 'opengauss-native-lake-local-provider'


def sha(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def read(path):
    return json.loads(Path(path).read_text())


def durable(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def save(path, result):
    temporary = path.with_name(path.name + '.tmp')
    with temporary.open('w') as stream:
        json.dump(result, stream, ensure_ascii=False, indent=2)
        stream.write('\n')
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)
    durable(path.parent)


def canonical_directory(path):
    path = Path(path)
    if path.is_symlink() or not path.is_dir() or path.resolve(strict=True) != path:
        raise RuntimeError('Expected an existing canonical directory: ' + str(path))
    return path


def inventory(path):
    canonical_directory(path)
    files = {}
    for entry in sorted(path.rglob('*')):
        if entry.is_symlink() or not (entry.is_file() or entry.is_dir()):
            raise RuntimeError('SDK inventory contains an alias or special entry: ' + str(entry))
        if entry.is_file() and entry != path / 'owner.json':
            files[str(entry.relative_to(path))] = sha(entry)
    return files


def owned(path, target):
    marker = path / 'owner.json'
    files = inventory(path)
    owner = read(marker)
    if (owner.get('project') != OWNER_PROJECT or owner.get('target') != str(target)
            or owner.get('phase') != 'prepared-for-atomic-install'
            or owner.get('files') != files or not files.get('libog_lake_sdk_arrow.so')
            or not re.fullmatch(r'[0-9a-f]{64}', owner.get('qualification_sha256', ''))):
        raise RuntimeError('Existing SDK prefix ownership or exact inventory mismatch')
    # Absence is the exact old owner's schema, not an unknown qualification kind.
    if owner.get('qualification_kind', SDK_KIND) not in (SDK_KIND, TEMPORAL_SDK_KIND, DECIMAL54_SDK_KIND):
        raise RuntimeError('Unknown installed SDK qualification kind')
    return dict(owner=owner, owner_sha256=sha(marker), files=files)


def stopped(root, source, prefix):
    observations = {}
    for profile, port in [('debug', 55560), ('ann-debug', 55580)]:
        runtime = root / '.runtime/opengauss-native-lake' / profile
        if not runtime.exists():
            if profile == 'debug':
                raise RuntimeError('Owned Debug runtime must already exist')
            continue
        canonical_directory(runtime)
        expected = dict(project='opengauss-native-lake', source=str(source),
                        data=str(runtime / 'data'), install=str(prefix), profile=profile, port=port)
        owner = runtime / 'owner.json'
        if owner.is_symlink() or read(owner) != expected:
            raise RuntimeError('Shared installation runtime ownership mismatch: ' + profile)
        pidfile = runtime / 'data/postmaster.pid'
        if pidfile.exists():
            if pidfile.is_symlink():
                raise RuntimeError('Aliased postmaster PID file')
            pid = int(pidfile.read_text().splitlines()[0])
            if pid <= 0 or (Path('/proc') / str(pid)).exists():
                raise RuntimeError('Both shared-prefix instances must be stopped: ' + profile)
        observations[profile] = sha(owner)
    expected_binary = str(prefix / 'bin/gaussdb')
    sdk = str(prefix / 'lib/postgresql/og_lake_sdk/libog_lake_sdk_arrow.so')
    for process in Path('/proc').iterdir():
        if not process.name.isdigit():
            continue
        try:
            if process.stat().st_uid != os.geteuid():
                continue
            binary = os.readlink(process / 'exe').removesuffix(' (deleted)')
            if binary == expected_binary:
                raise RuntimeError('A process still uses the shared Native installation: PID ' + process.name)
            if any(line.rstrip().removesuffix(' (deleted)').endswith(' ' + sdk)
                   for line in (process / 'maps').read_text().splitlines()):
                raise RuntimeError('A process still maps the installed SDK: PID ' + process.name)
        except (FileNotFoundError, ProcessLookupError):
            continue
        except PermissionError as exc:
            raise RuntimeError('Cannot prove owned-process mappings are stopped: PID ' + process.name) from exc
    return observations


def admit(root, source, index, selected_sha):
    index = index.resolve(strict=True)
    kind = read(index).get('status')
    if sha(index) != selected_sha or kind not in (SDK_KIND, TEMPORAL_SDK_KIND, DECIMAL54_SDK_KIND):
        raise RuntimeError('Unknown/legacy kind or wrong selected SDK index SHA; use its original installer')
    if kind == DECIMAL54_SDK_KIND:
        return admit_decimal54(root, source, index, selected_sha)
    temporal = kind == TEMPORAL_SDK_KIND
    helper = root / 'integration/opengauss-native-lake' / (
        'sdk_temporal_inputs.py' if temporal else 'pq_sql020_build_inputs.py')
    helper_sha = TEMPORAL_HELPER_SHA256 if temporal else HELPER_SHA256
    legacy = helper.with_name('pq_native_build_inputs.py')
    if sha(helper) != helper_sha or sha(legacy) != LEGACY_HELPER_SHA256:
        raise RuntimeError('Exact new SDK admission helpers differ from this installer')
    sys.path.insert(0, str(helper.parent))
    spec = importlib.util.spec_from_file_location('og_lake_sdk_install_checks', helper)
    checks = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(checks)
    qualified = checks.sdk_index(index, selected_sha, root)
    control = source / 'contrib/og_lake/og_lake.control'
    match = re.search(r"(?m)^default_version\s*=\s*'([^']+)'", control.read_text())
    # SQL52 changes the public codec header. Temporal-only qualification cannot
    # authenticate UUID; retain a precise closed boundary until its new SDK proof.
    if match and match.group(1) in ('0.54', '0.55'):
        raise RuntimeError('SQL0.54/0.55 requires a separately qualified ABI5 decimal SDK and matching full public header/PQ; all new qualification remains pending')
    if match and match.group(1) == '0.53':
        raise RuntimeError('SQL0.53 requires a separately qualified physical JSON SDK and matching full public header; UUID-only SDK proofs are insufficient')
    if match and match.group(1) == '0.52':
        raise RuntimeError('SQL0.52 requires a separately qualified UUID SDK and matching full public header; temporal-only SDK proofs are insufficient')
    versions = ['0.31', '0.32', '0.33', '0.34', '0.35', '0.36', '0.37', '0.38', '0.39', '0.40', '0.41', '0.42', '0.43', '0.49', '0.50', '0.51'] if temporal else ['0.20', '0.21', '0.22', '0.23', '0.24', '0.25', '0.26', '0.29', '0.30']
    if not match or match.group(1) not in versions:
        raise RuntimeError('SDK kind does not match its reviewed Native source version')
    native = {str(control): sha(control)}
    public = ['sdk/lake_parquet_sdk.h', 'vendor/arrow20/abi.h']
    if temporal:
        public.append('sdk/lake_temporal_codec.h')
    for relative in public:
        path = source / 'contrib/og_lake' / relative
        if sha(path) != checks.SDK_SOURCES[relative]:
            raise RuntimeError('Native and qualified SDK full public headers differ: ' + relative)
        native[str(path)] = sha(path)
    frozen = {str(Path(__file__).resolve()): sha(__file__), str(helper): sha(helper),
              str(legacy): sha(legacy), str(index): selected_sha, **native}
    return qualified, frozen


def admit_decimal54(root, source, index, selected_sha):
    helper = root / 'integration/opengauss-native-lake/sdk_decimal54_native_inputs.py'
    if sha(helper) != DECIMAL54_HELPER_SHA256:
        raise RuntimeError('Exact reviewed decimal54 metadata bridge differs')
    spec = importlib.util.spec_from_file_location('og_lake_sdk_decimal54_install_checks', helper)
    checks = importlib.util.module_from_spec(spec)
    exec(compile(helper.read_bytes(), str(helper), 'exec'), checks.__dict__)
    qualified = checks.sdk_index(index, selected_sha, root)
    control = source / 'contrib/og_lake/og_lake.control'
    match = re.search(r"(?m)^default_version\s*=\s*'([^']+)'", control.read_text())
    if not match or match.group(1) not in ('0.54', '0.55'):
        raise RuntimeError('The full decimal128 SDK requires reviewed Native source0.54 or0.55')
    if match.group(1) == '0.55' and sha(control) != NATIVE55_CONTROL_SHA256:
        raise RuntimeError('Native55 control differs from reviewed public-snapshot55 Source')
    native = {str(control): sha(control)}
    for relative in ('sdk/lake_parquet_sdk.h', 'vendor/arrow20/abi.h', 'sdk/lake_temporal_codec.h'):
        path = source / 'contrib/og_lake' / relative
        if sha(path) != checks.SDK_SOURCES[relative]:
            raise RuntimeError('Native54/55 and qualified full SDK public headers differ: ' + relative)
        native[str(path)] = sha(path)
    frozen = {str(Path(__file__).resolve()): sha(__file__), str(helper): sha(helper),
              str(index): selected_sha, **native, **checks.control_inputs()}
    return qualified, frozen


def rename(source, target, flags):
    libc = ctypes.CDLL(None, use_errno=True)  # libc only; never loads a provider.
    call = libc.renameat2
    call.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
    call.restype = ctypes.c_int
    if call(-100, os.fsencode(source), -100, os.fsencode(target), flags):
        code = ctypes.get_errno()
        raise OSError(code, os.strerror(code))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--workspace', type=Path, required=True)
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--sdk-index', type=Path)
    parser.add_argument('--sdk-index-sha256')
    parser.add_argument('--native-source', type=Path)
    parser.add_argument('--replace-owned', action='store_true', help='Atomic exchange only for an exactly owned existing SDK')
    parser.add_argument('--execute', action='store_true')
    args = parser.parse_args()
    root = args.workspace.resolve(strict=True)
    if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,59}', args.run_id):
        parser.error('Safe fresh run-id max60 required')
    source = (args.native_source or root / '.worktrees/opengauss-native-lake').resolve()
    prefix = root / '.install/opengauss-native-lake-debug'
    target = prefix / 'lib/postgresql/og_lake_sdk'
    if not args.execute:
        print(json.dumps(dict(status='prepared_not_executed', target=str(target),
            qualification_kinds={SDK_KIND: 12, TEMPORAL_SDK_KIND: 15, DECIMAL54_SDK_KIND: 'complete original15 plus UUID15/JSON8 and NUM9/ABI5'}, native_source=str(source),
            stopped_profiles=['debug', 'ann-debug'], replace_owned=args.replace_owned,
            sdk_loads=0, installs=0, service_calls=0,
            prerequisite='Root serializes all build/service actions, stops both shared-prefix instances, and supplies actual qualified SDK index+SHA; this command never stops/starts either instance.'), indent=2))
        return 0
    if args.sdk_index is None or not re.fullmatch(r'[0-9a-f]{64}', args.sdk_index_sha256 or ''):
        parser.error('execute requires --sdk-index and exact --sdk-index-sha256')
    source = source.resolve(strict=True)
    evidence = root / '公共文档/openGauss原生湖存储/验证' / args.run_id
    evidence.mkdir(exist_ok=False)
    result = dict(status='failed', target=str(target), qualification_kind=None,
                  started_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
                  script_sha256=sha(__file__), published=False, committed=False,
                  scope='Provider deployment only; no Native build/runtime, SDK operation or gate promotion')
    path = evidence / 'result.json'
    lock = None
    staging = old_location = None
    previous = None
    ever_published = publication_attempted = committed = False
    try:
        lock = (root / '.reference/opengauss-native-lake/.driver.lock').open('a+')
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        canonical_directory(prefix)
        canonical_directory(target.parent)
        qualified, frozen = admit(root, source, args.sdk_index, args.sdk_index_sha256)
        result.update(qualification=qualified, qualification_kind=read(args.sdk_index)['status'], frozen_inputs=frozen,
                      stopped_runtime_owners=stopped(root, source, prefix))
        if target.exists() or target.is_symlink():
            if not args.replace_owned:
                raise RuntimeError('Target exists; explicit --replace-owned required, no unknown overwrite')
            previous = owned(target, target)
        result['previous'] = previous
        staging = target.with_name('.og_lake_sdk.staging.' + uuid.uuid4().hex)
        retired = target.with_name('og_lake_sdk.retired.' + uuid.uuid4().hex)
        result.update(staging=str(staging), retired=str(retired), phase='preparing')
        save(path, result)
        staging.mkdir(mode=0o700)
        provider_prefix = Path(qualified['prefix'])
        shutil.copy2(qualified['library'], staging / 'libog_lake_sdk_arrow.so')
        licenses_src = provider_prefix / 'share/licenses'
        if not licenses_src.is_dir():
            licenses_src = provider_prefix / 'licenses'
        shutil.copytree(licenses_src, staging / 'licenses', symlinks=True)
        header = staging / 'include/og_lake/lake_parquet_sdk.h'
        header.parent.mkdir(parents=True)
        shutil.copy2(provider_prefix / 'include/og_lake/lake_parquet_sdk.h', header)
        expected_files = {'libog_lake_sdk_arrow.so': qualified['library_sha256'],
                          'include/og_lake/lake_parquet_sdk.h': qualified['header_sha256']}
        for name, expected in read(args.sdk_index)['prefix_files'].items():
            if name.startswith('share/licenses/'):
                expected_files['licenses/' + name[len('share/licenses/'):]] = expected
            elif name.startswith('licenses/'):
                expected_files[name] = expected
        files = inventory(staging)
        if files != expected_files:
            raise RuntimeError('Staged SDK differs from the full qualified payload inventory')
        owner = dict(project=OWNER_PROJECT, phase='prepared-for-atomic-install',
                     target=str(target), qualification_kind=result['qualification_kind'],
                     qualification_sha256=args.sdk_index_sha256, files=files,
                     native_public_headers={k: v for k, v in frozen.items() if k.endswith(('.h', '.control')) and Path(k).is_relative_to(source / 'contrib/og_lake')})
        (staging / 'owner.json').write_text(json.dumps(owner, indent=2) + '\n')
        for item in staging.rglob('*'):
            if item.is_file():
                item.chmod(0o644)
                with item.open('rb') as stream:
                    os.fsync(stream.fileno())
        for directory in sorted([p for p in staging.rglob('*') if p.is_dir()], key=lambda p: len(p.parts), reverse=True):
            directory.chmod(0o755)
            durable(directory)
        staging.chmod(0o755)
        durable(staging)
        intended = owned(staging, target)
        result.update(intended=intended, phase='prepared')
        save(path, result)
        if admit(root, source, args.sdk_index, args.sdk_index_sha256) != (qualified, frozen):
            raise RuntimeError('SDK/source/index/helper identity changed before publication')
        stopped(root, source, prefix)
        if previous is not None and owned(target, target) != previous:
            raise RuntimeError('Existing owned SDK changed while preparing replacement')
        publication_attempted = True
        rename(staging, target, 2 if previous is not None else 1)  # EXCHANGE / NOREPLACE.
        ever_published = True
        result.update(published=True, phase='published_verifying')
        old_location = staging if previous is not None else None
        save(path, result)
        durable(target.parent)
        if owned(target, target) != intended:
            raise RuntimeError('Published SDK identity differs from prepared inventory')
        if admit(root, source, args.sdk_index, args.sdk_index_sha256) != (qualified, frozen):
            raise RuntimeError('SDK/source/index/helper identity changed during publication')
        stopped(root, source, prefix)
        if previous is not None:
            if owned(old_location, target) != previous:
                raise RuntimeError('Preserved old SDK identity differs')
            rename(old_location, retired, 1)
            old_location = retired
            durable(target.parent)
        result.update(status='installed', committed=True, phase='committed',
                      preserved_previous=str(old_location) if old_location else None)
        save(path, result)
        committed = True
        print(json.dumps(dict(status='installed', result=str(path), target=str(target))))
        return 0
    except BaseException as exc:
        result.update(status='failed', error=str(exc), committed=committed)
        # Discover syscall completion as well: a signal can interrupt Python
        # after renameat2 succeeds but before the following assignment executes.
        if publication_attempted and not committed:
            try:
                if owned(target, target) == result['intended']:
                    ever_published = True
                    result['published'] = True
            except BaseException as discovery_error:
                result.update(recovery_required=True, publication_discovery_error=str(discovery_error))
        if ever_published and not committed:
            try:
                stopped(root, source, prefix)
                if owned(target, target) != result['intended']:
                    raise RuntimeError('Refuse rollback over a changed/unowned published SDK')
                if previous is not None:
                    found = []
                    for candidate in [staging, retired]:
                        if candidate is not None and candidate.exists() and owned(candidate, target) == previous:
                            found.append(candidate)
                    if len(found) != 1:
                        raise RuntimeError('Cannot prove the exact preserved old SDK for rollback')
                    old_location = found[0]
                    rename(old_location, target, 2)
                    durable(target.parent)
                    if owned(target, target) != previous:
                        raise RuntimeError('Rollback did not restore the exact old SDK')
                    result['failed_candidate'] = str(old_location)
                else:
                    rename(target, staging, 1)
                    durable(target.parent)
                    result['failed_candidate'] = str(staging)
                result.update(published=False, rollback='restored_previous_or_absent_target')
            except BaseException as rollback_error:
                result['rollback_error'] = str(rollback_error)
                result['recovery_required'] = True
        print(json.dumps(dict(status='failed', error=str(exc), result=str(path))))
        return 1
    finally:
        if not publication_attempted and staging is not None and staging.exists():
            try:
                shutil.rmtree(staging)  # Only this invocation's random prepublish directory.
            except BaseException as cleanup_error:
                result['staging_cleanup_error'] = str(cleanup_error)
                result['recovery_required'] = True
        result['finished_at'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        try:
            save(path, result)
        finally:
            if lock is not None:
                lock.close()


if __name__ == '__main__':
    raise SystemExit(main())
