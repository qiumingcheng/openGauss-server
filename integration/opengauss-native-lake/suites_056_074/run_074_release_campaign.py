#!/usr/bin/env python3
"""Release (-O2) correctness, SDK deployment check, N7-SMP, SQL0.70/0.71, and performance runner."""

import argparse
import hashlib
import getpass
import re
import json
import os
import shutil
import subprocess
import sys
import tempfile
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

EXPECTED_SDK54_SHA = None
EXPECTED_SDK6_SHA = None
EXPECTED_EXTENSION_VERSION = '0.74'


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda: f.read(65536), b''):
            h.update(chunk)
    return h.hexdigest()


def ensure_release_sdks():
    debug_pg = ROOT / '.install/opengauss-native-lake-debug/lib/postgresql'
    rel_pg = ROOT / '.install/opengauss-native-lake-release/lib/postgresql'
    copied = []
    for sub, expected in [('og_lake_sdk', EXPECTED_SDK54_SHA), ('og_lake_external_sdk', EXPECTED_SDK6_SHA)]:
        dst = rel_pg / sub / 'libog_lake_sdk_arrow.so'
        if not dst.is_file() or sha256_file(dst) != expected:
            src_dir = debug_pg / sub
            src = src_dir / 'libog_lake_sdk_arrow.so'
            assert sha256_file(src) == expected, f'{sub} source SDK SHA mismatch; destination was not changed'
            dst_dir = rel_pg / sub
            dst_dir.mkdir(parents=True, exist_ok=True)
            shutil.copytree(src_dir, dst_dir, dirs_exist_ok=True)
            assert sha256_file(dst) == expected, f'{sub} sha mismatch after copy: {sha256_file(dst)}'
            copied.append(sub)
    return {
        'copied_dirs': copied,
        'sdk54_sha256': sha256_file(rel_pg / 'og_lake_sdk/libog_lake_sdk_arrow.so'),
        'sdk6_sha256': sha256_file(rel_pg / 'og_lake_external_sdk/libog_lake_sdk_arrow.so'),
    }


def verify_070_ops_and_alter_release():
    tag = uuid.uuid4().hex[:10]
    tmp_root = Path(tempfile.mkdtemp(prefix=f'og_rel070_{tag}_', dir='/tmp'))
    os.chmod(tmp_root, 0o755)
    sock, port = endpoint('release')
    admin = psycopg2.connect(host=sock, port=port, dbname='postgres', user=DB_USER, connect_timeout=15)
    admin.autocommit = True
    role = f'urel_{tag}'
    db = f'dbrel070_{tag}'
    cm = None
    try:
        with admin.cursor() as cur:
            cur.execute('SELECT version(), current_setting(%s)', ('enable_thread_pool',))
            ver, tp = cur.fetchone()
            assert 'debug on' not in ver.lower(), f'Unexpected debug binary in release: {ver}'
            pw = 'md5' + hashlib.md5((f'Rel071!{role}').encode('utf-8')).hexdigest()
            cur.execute(f"CREATE ROLE {role} NOLOGIN NOINHERIT ENCRYPTED PASSWORD %s", (pw,))
            cur.execute(f"CREATE DATABASE {db} WITH DBCOMPATIBILITY 'PG' ENCODING 'UTF8'")

        cm = psycopg2.connect(host=sock, port=port, dbname=db, user=DB_USER, connect_timeout=15)
        cm.autocommit = True
        with cm.cursor() as cur:
            cur.execute('CREATE EXTENSION og_lake')
            cur.execute(f"CREATE SCHEMA biz; GRANT USAGE ON SCHEMA biz TO {role}")
            cur.execute('SELECT og_lake.configure_internal_owner(%s::name)', (role,))
            cat_dir = tmp_root / 'cat'
            idx_dir = tmp_root / 'idx'
            idx_dir.mkdir(parents=True, exist_ok=True)
            os.chmod(idx_dir, 0o755)
            cur.execute("SELECT og_lake.create_catalog('c_rel'::name, 'lake_metadata'::name, %s::text, NULL::text)", (str(cat_dir),))
            cur.execute("SELECT og_lake.set_option('c_rel'::name, 'index_root'::text, %s::text)", (str(idx_dir),))
            cur.execute("""
                CREATE FOREIGN TABLE biz.t_evolve (
                    id int4, k int2, d numeric(8,2) DEFAULT 12.50
                ) SERVER c_rel OPTIONS (binding_mode 'create', lake_schema 'main')
            """)
            cur.execute("INSERT INTO biz.t_evolve (id, k) VALUES (1, 100)")
            cur.execute("SELECT og_lake.flush('biz.t_evolve'::regclass)")
            cur.execute("INSERT INTO biz.t_evolve VALUES (2, 200, 99.25)")
            cur.execute("ALTER FOREIGN TABLE biz.t_evolve ALTER COLUMN k TYPE int8")
            cur.execute("ALTER FOREIGN TABLE biz.t_evolve ALTER COLUMN d TYPE numeric(14,4)")
            cur.execute("INSERT INTO biz.t_evolve VALUES (3, 9000000000, 123456.7890)")
            cur.execute("SELECT id, k, d::text FROM biz.t_evolve ORDER BY id")
            rows = cur.fetchall()
            assert rows == [(1, 100, '12.5000'), (2, 200, '99.2500'), (3, 9000000000, '123456.7890')], rows
            cur.execute("SELECT status FROM og_lake.verify_catalog_integrity('c_rel'::name, false) WHERE severity = 'ERROR'")
            assert cur.fetchall() == []
            cur.execute("SELECT og_lake.backup_manifest('c_rel'::name)::text")
            manifest = json.loads(cur.fetchone()[0])
            assert manifest['module_version'] == EXPECTED_EXTENSION_VERSION and manifest['summary']['missing_or_mismatched_data_files'] == 0
            cur.execute("SELECT catalog_name, expired_scheduled_files FROM og_lake.retention_status('c_rel'::name)")
            assert cur.fetchone()[0] == 'c_rel'
            cur.execute("SELECT id FROM og_lake.current_snapshot('c_rel'::name)")
            snap_id = cur.fetchone()[0]
            cur.execute("BEGIN")
            cur.execute("SELECT og_lake.bind_snapshot('c_rel'::name, %s::bigint)", (snap_id,))
            cur.execute("SELECT count(*) FROM biz.t_evolve")
            assert cur.fetchone()[0] == 3
            cur.execute("COMMIT")
        return {'status': 'passed', 'version': ver, 'enable_thread_pool': tp, 'checks': 6}
    finally:
        if cm:
            cm.close()
        with admin.cursor() as cur:
            try:
                cur.execute(f"DROP DATABASE IF EXISTS {db}")
            except Exception:
                pass
            try:
                cur.execute(f"DROP ROLE IF EXISTS {role}")
            except Exception:
                pass
        admin.close()
        shutil.rmtree(tmp_root, ignore_errors=True)


def main():
    global ROOT, BUILD_DRIVER, BUILD_ENTRY, DB_USER, EXPECTED_SDK54_SHA, EXPECTED_SDK6_SHA
    ap = argparse.ArgumentParser()
    ap.add_argument('--workspace', default=str(ROOT))
    ap.add_argument('--user', default=DB_USER)
    ap.add_argument('--build-entry', help='Native build/restart entry matching the build receipt')
    ap.add_argument('--expected-sdk54-sha256', required=True)
    ap.add_argument('--expected-sdk6-sha256', required=True)
    ap.add_argument('--build-receipt', required=True)
    ap.add_argument('--out-dir')
    args = ap.parse_args()
    ROOT = Path(args.workspace).expanduser().resolve()
    BUILD_DRIVER = ROOT / 'integration/opengauss-native-lake/build_driver.py'
    DB_USER = args.user
    os.environ['CPP_WORKSPACE'] = str(ROOT)
    os.environ['PGUSER'] = DB_USER
    EXPECTED_SDK54_SHA = args.expected_sdk54_sha256
    EXPECTED_SDK6_SHA = args.expected_sdk6_sha256
    assert all(re.fullmatch('[0-9a-f]{64}', x) for x in [EXPECTED_SDK54_SHA, EXPECTED_SDK6_SHA]), 'Invalid expected SDK SHA256'

    out_dir = Path(args.out_dir) if args.out_dir else ROOT / '公共文档/openGauss原生湖存储/验证/native_074_release_campaign_01'
    out_dir.mkdir(parents=True, exist_ok=True)
    receipt = json.loads(Path(args.build_receipt).read_text(encoding='utf-8'))
    assert receipt.get('status') == 'build_and_sql_passed', receipt.get('status')
    BUILD_ENTRY = resolve_build_entry(args, receipt)

    endpoint('release')
    sdk_info = ensure_release_sdks()

    env = child_env('release', receipt['source'])
    common_cmd = ['--build-entry', str(BUILD_ENTRY)] + dependency_args(receipt)
    subprocess.run([
        sys.executable, str(BUILD_DRIVER), 'release', 'restart',
        '--run-id', f'release_verify_restart_{uuid.uuid4().hex[:8]}',
    ] + common_cmd, check=True, env=env)

    try:
        rel_070 = verify_070_ops_and_alter_release()
        json_out = out_dir / 'release_json_071.json'
        smp_out = out_dir / 'release_smp_closure.json'
        vec_out = out_dir / 'release_vec_smp_072.json'
        subprocess.run([sys.executable, str(SUITES / 'run_074_compat/run_074_vec_smp_compat_suite.py'), '--profile', 'release', '--out', str(vec_out)], check=True, env=env)
        subprocess.run([sys.executable, str(SUITES / 'run_074_compat/run_074_json_compat_suite.py'), '--profile', 'release', '--out', str(json_out)], check=True, env=env)
        subprocess.run([sys.executable, str(SUITES / 'run_074_compat/run_074_smp_closure_suite.py'), '--profile', 'release', '--out', str(smp_out)], check=True, env=env)
        perf_dir = out_dir / 'performance'
        subprocess.run([sys.executable, str(SUITES / 'run_074_release_perf_bench.py'), '--profile', 'release', '--out-dir', str(perf_dir)], check=True, env=env)

        summary = {
            'status': 'passed_release_full',
            'build_receipt': str(Path(args.build_receipt).resolve()),
            'build_receipt_sha256': sha256_file(Path(args.build_receipt)),
            'binary_sha256': receipt['binary_sha256'],
            'sdk_deploy': sdk_info,
            'release_070_ops_alter': rel_070,
            'release_vec_smp_072': json.loads(vec_out.read_text(encoding='utf-8')),
            'release_071_json': json.loads(json_out.read_text(encoding='utf-8')),
            'release_smp_closure': json.loads(smp_out.read_text(encoding='utf-8')),
            'release_perf': json.loads((perf_dir / 'index.json').read_text(encoding='utf-8')),
        }
        idx = out_dir / 'index.json'
        idx.write_text(json.dumps(summary, indent=2, ensure_ascii=False) + '\n', encoding='utf-8')
        print(f'Saved Release campaign {idx} (sha256={sha256_file(idx)})')
    finally:
        subprocess.run([
            sys.executable, str(BUILD_DRIVER), 'release', 'stop',
            '--run-id', f'release_verify_stop_{uuid.uuid4().hex[:8]}',
        ] + common_cmd, check=False, env=env)


if __name__ == '__main__':
    main()
