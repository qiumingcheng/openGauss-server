#!/usr/bin/env python3
"""SQL0.74 three-runtime campaign preserving SQL071-073 compatibility and N7-SMP checks."""

import argparse
import hashlib
import getpass
import os
import json
import re
import subprocess
import sys
import uuid
from pathlib import Path

import psycopg2

ROOT = Path(os.getenv('CPP_WORKSPACE', str(Path(__file__).resolve().parents[3]))).resolve()
SUITES = Path(__file__).resolve().parent
BUILD_DRIVER = ROOT / 'integration/opengauss-native-lake/build_driver.py'
BUILD_ENTRY = None
DB_USER = os.getenv('OG_LAKE_TEST_USER', os.getenv('PGUSER', getpass.getuser()))


def endpoint(profile):
    suffix = profile.upper().replace('-', '_')
    host = os.getenv('OG_LAKE_TEST_HOST_' + suffix, str(ROOT / '.runtime/opengauss-native-lake' / profile / 'socket'))
    port = int(os.getenv('OG_LAKE_TEST_PORT_' + suffix, str({'debug': 55560, 'ann-debug': 55580, 'release': 55570}[profile])))
    expected_host = str(ROOT / '.runtime/opengauss-native-lake' / profile / 'socket')
    expected_port = {'debug': 55560, 'ann-debug': 55580, 'release': 55570}[profile]
    if host != expected_host or port != expected_port:
        raise ValueError('Managed campaigns use build_driver owned sockets/ports; use standalone suites for custom endpoints')
    return host, port


def child_env(profile, source=None):
    env = os.environ.copy()
    host, port = endpoint(profile)
    env.update(CPP_WORKSPACE=str(ROOT), PGUSER=DB_USER, OG_LAKE_TEST_USER=DB_USER,
               OG_LAKE_TEST_HOST=host, OG_LAKE_TEST_PORT=str(port))
    if source:
        env['OG_LAKE_SOURCE'] = str(source)
    return env


def dependency_args(receipt):
    args = ['--roaring-build', receipt['roaring_inputs_after']['receipt']]
    pq = receipt['pq_inputs_after']
    if pq.get('dependency_kind') == 'pq_in_tree_v1':
        blas = pq['blas_inputs']
        args += ['--pq-in-tree', '--blas-build', blas['receipt'],
                 '--blas-build-sha256', blas['receipt_sha256']]
    else:
        selection = pq['decimal54_selection']
        args += ['--pq-build', pq['receipt'], '--pq-sdk-selection', selection['path'],
                 '--pq-sdk-selection-sha256', selection['sha256']]
    return args


def resolve_build_entry(args, receipt):
    # build_driver.py derives its workspace from its own location; use the
    # documented real directory copy under CPP_WORKSPACE, never a symlink.
    assert BUILD_DRIVER.is_file(), f'Missing workspace build driver: {BUILD_DRIVER}'
    assert BUILD_DRIVER.resolve().parents[2] == ROOT, 'Copy integration into CPP_WORKSPACE; do not symlink it'
    path = Path(args.build_entry or receipt.get('build_entry') or
                (BUILD_DRIVER.parent / 'native_build_entry.sh')).resolve(strict=True)
    if receipt.get('build_entry_sha256'):
        assert sha256_file(path) == receipt['build_entry_sha256'], 'build-entry differs from the supplied build receipt'
    return path

EXPECTED_IMMUTABLE = {}


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda: f.read(65536), b''):
            h.update(chunk)
    return h.hexdigest()


def check_immutables():
    out = {}
    for name, (p, expected) in EXPECTED_IMMUTABLE.items():
        actual = sha256_file(p)
        assert actual == expected, f'{name} immutable SHA mismatch: actual={actual} expected={expected}'
        out[name] = actual
    return out


def set_thread_pool(conf_path: Path, enabled: bool):
    lines = conf_path.read_text(encoding='utf-8').splitlines()
    out = []
    saw_pool = False
    saw_attr = False
    for line in lines:
        if re.match(r'^\s*#?\s*enable_thread_pool\s*=', line):
            out.append(f"enable_thread_pool = {'on' if enabled else 'off'}")
            saw_pool = True
        elif re.match(r'^\s*#?\s*thread_pool_attr\s*=', line):
            out.append("thread_pool_attr = '4,1,(nobind)'" if enabled else "#thread_pool_attr = '4,1,(nobind)'")
            saw_attr = True
        else:
            out.append(line)
    if not saw_pool:
        out.append(f"enable_thread_pool = {'on' if enabled else 'off'}")
    if enabled and not saw_attr:
        out.append("thread_pool_attr = '4,1,(nobind)'")
    conf_path.write_text('\n'.join(out) + '\n', encoding='utf-8')


def run_cmd(cmd, ignore_error=False, env=None):
    proc = subprocess.run(cmd, check=False, env=env)
    if proc.returncode != 0 and not ignore_error:
        raise RuntimeError(f'Command failed ({proc.returncode}): {" ".join(str(x) for x in cmd)}')


def build_driver_cmd(profile, mode, receipt_path, run_tag):
    receipt = json.loads(Path(receipt_path).read_text(encoding='utf-8'))
    cmd = [sys.executable, str(BUILD_DRIVER), profile, mode,
           '--run-id', f'{run_tag}_{uuid.uuid4().hex[:8]}',
           '--build-entry', str(BUILD_ENTRY)] + dependency_args(receipt)
    if profile == 'ann-debug' and mode in ('restart', 'init'):
        cmd.extend(['--reuse-build-receipt', str(receipt_path)])
    return cmd


def show_thread_pool(profile):
    host, port = endpoint(profile)
    conn = psycopg2.connect(
        host=host,
        port=port,
        dbname='postgres',
        user=DB_USER,
        connect_timeout=15,
    )
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute('SHOW enable_thread_pool')
            return cur.fetchone()[0]
    finally:
        conn.close()


def main():
    global ROOT, BUILD_DRIVER, BUILD_ENTRY, DB_USER, EXPECTED_IMMUTABLE
    ap = argparse.ArgumentParser()
    ap.add_argument('--workspace', default=str(ROOT))
    ap.add_argument('--user', default=DB_USER)
    ap.add_argument('--build-entry', help='Native build/restart entry matching the build receipt')
    ap.add_argument('--immutable-manifest', required=True,
                    help='JSON mapping SDK54_SO, SDK6_SO, DEBUG_CONF, ANN_DEBUG_CONF to expected SHA256 values')
    ap.add_argument('--build-receipt', required=True)
    ap.add_argument('--campaign-id', default='native_074_compat_campaign_01')
    args = ap.parse_args()
    ROOT = Path(args.workspace).expanduser().resolve()
    BUILD_DRIVER = ROOT / 'integration/opengauss-native-lake/build_driver.py'
    DB_USER = args.user
    os.environ['CPP_WORKSPACE'] = str(ROOT)
    os.environ['PGUSER'] = DB_USER
    expected = json.loads(Path(args.immutable_manifest).read_text(encoding='utf-8'))
    paths = {
        'SDK54_SO': ROOT / '.install/opengauss-native-lake-debug/lib/postgresql/og_lake_sdk/libog_lake_sdk_arrow.so',
        'SDK6_SO': ROOT / '.install/opengauss-native-lake-debug/lib/postgresql/og_lake_external_sdk/libog_lake_sdk_arrow.so',
        'DEBUG_CONF': ROOT / '.runtime/opengauss-native-lake/debug/data/postgresql.conf',
        'ANN_DEBUG_CONF': ROOT / '.runtime/opengauss-native-lake/ann-debug/data/postgresql.conf',
    }
    assert set(expected) == set(paths), 'immutable manifest must contain exactly the four documented identities'
    assert all(re.fullmatch('[0-9a-f]{64}', v) for v in expected.values()), 'Invalid expected SHA256'
    EXPECTED_IMMUTABLE = {key: (path, expected[key]) for key, path in paths.items()}

    receipt_path = Path(args.build_receipt).resolve()
    build_receipt = json.loads(receipt_path.read_text(encoding='utf-8'))
    assert build_receipt.get('status') == 'build_and_sql_passed', build_receipt.get('status')
    BUILD_ENTRY = resolve_build_entry(args, build_receipt)
    for profile in ('debug', 'ann-debug'):
        endpoint(profile)
    os.environ['OG_LAKE_SOURCE'] = build_receipt['source']

    ev_dir = ROOT / '公共文档/openGauss原生湖存储/验证' / args.campaign_id
    ev_dir.mkdir(parents=True, exist_ok=True)

    debug_conf = EXPECTED_IMMUTABLE['DEBUG_CONF'][0]
    ann_conf = EXPECTED_IMMUTABLE['ANN_DEBUG_CONF'][0]
    orig_debug_conf = debug_conf.read_bytes()
    orig_ann_conf = ann_conf.read_bytes()

    immut_before = check_immutables()
    runs = {}

    try:
        for tag, profile, pool_on in [
            ('debug_pool_off', 'debug', False),
            ('debug_pool_on', 'debug', True),
            ('anndebug_pool_off', 'ann-debug', False),
        ]:
            print(f'=== Running 0.74 campaign target: {tag} (profile={profile}, pool={pool_on}) ===', flush=True)
            if profile == 'debug':
                run_cmd(build_driver_cmd('debug', 'stop', receipt_path, f'{args.campaign_id}_{tag}_stop0'), ignore_error=True)
                if pool_on:
                    set_thread_pool(debug_conf, True)
                else:
                    debug_conf.write_bytes(orig_debug_conf)
                run_cmd(build_driver_cmd('debug', 'restart', receipt_path, f'{args.campaign_id}_{tag}_start'))
            else:
                run_cmd(build_driver_cmd('debug', 'stop', receipt_path, f'{args.campaign_id}_{tag}_stopd'), ignore_error=True)
                run_cmd(build_driver_cmd('ann-debug', 'stop', receipt_path, f'{args.campaign_id}_{tag}_stopa'), ignore_error=True)
                ann_conf.write_bytes(orig_ann_conf)
                run_cmd(build_driver_cmd('ann-debug', 'restart', receipt_path, f'{args.campaign_id}_{tag}_start'))

            actual_tp = show_thread_pool(profile)
            expected_tp = 'on' if pool_on else 'off'
            assert actual_tp == expected_tp, f'{tag}: expected thread_pool={expected_tp}, got {actual_tp}'

            tp_out = ev_dir / f'{tag}_trunc_pq_073.json'
            vec_out = ev_dir / f'{tag}_vec_smp_072.json'
            smp_out = ev_dir / f'{tag}_smp_closure.json'
            json_out = ev_dir / f'{tag}_json_071.json'
            run_cmd([sys.executable, str(SUITES / 'run_074_compat' / 'run_074_truncate_pq_suite.py'), '--profile', profile, '--out', str(tp_out)], env=child_env(profile))
            run_cmd([sys.executable, str(SUITES / 'run_074_compat' / 'run_074_vec_smp_compat_suite.py'), '--profile', profile, '--out', str(vec_out)], env=child_env(profile))
            run_cmd([sys.executable, str(SUITES / 'run_074_compat' / 'run_074_smp_closure_suite.py'), '--profile', profile, '--out', str(smp_out)], env=child_env(profile))
            run_cmd([sys.executable, str(SUITES / 'run_074_compat' / 'run_074_json_compat_suite.py'), '--profile', profile, '--out', str(json_out)], env=child_env(profile))
            tr = json.loads(tp_out.read_text(encoding='utf-8'))
            vr = json.loads(vec_out.read_text(encoding='utf-8'))
            sr = json.loads(smp_out.read_text(encoding='utf-8'))
            jr = json.loads(json_out.read_text(encoding='utf-8'))
            runs[tag] = {
                'profile': profile,
                'enable_thread_pool': actual_tp,
                'trunc_pq_073': tr,
                'vec_smp_072': vr,
                'smp_closure': sr,
                'json_071': jr,
                'total_checks': tr['total_checks'] + vr['total_checks'] + sr['total_checks'] + jr['total_checks'],
            }

            if profile == 'debug':
                run_cmd(build_driver_cmd('debug', 'stop', receipt_path, f'{args.campaign_id}_{tag}_stop1'), ignore_error=True)
                debug_conf.write_bytes(orig_debug_conf)
            else:
                run_cmd(build_driver_cmd('ann-debug', 'stop', receipt_path, f'{args.campaign_id}_{tag}_stop1'), ignore_error=True)
                ann_conf.write_bytes(orig_ann_conf)
    finally:
        for prof in ('debug', 'ann-debug'):
            run_cmd(build_driver_cmd(prof, 'stop', receipt_path, f'{args.campaign_id}_{prof}_fin'), ignore_error=True)
        debug_conf.write_bytes(orig_debug_conf)
        ann_conf.write_bytes(orig_ann_conf)

    immut_after = check_immutables()
    index = {
        'campaign_id': args.campaign_id,
        'status': 'passed_SQL074_COMPAT_SQL073_TRUNC_PQVEC_SQL072_N7_SMP_SQL071',
        'build_receipt': str(receipt_path),
        'build_receipt_sha256': sha256_file(receipt_path),
        'binary_sha256': build_receipt['binary_sha256'],
        'source_head': build_receipt['source_head'],
        'immutable_before': immut_before,
        'immutable_after': immut_after,
        'runs': runs,
        'grand_total_checks': sum(r['total_checks'] for r in runs.values()),
    }
    index_path = ev_dir / 'index.json'
    index_path.write_text(json.dumps(index, indent=2, ensure_ascii=False) + '\n', encoding='utf-8')
    print(f'Saved {index_path} (sha256={sha256_file(index_path)})', flush=True)
    print(json.dumps({k: {'total_checks': v['total_checks'], 'tp': v['enable_thread_pool']} for k, v in runs.items()}, indent=2))


if __name__ == '__main__':
    main()
