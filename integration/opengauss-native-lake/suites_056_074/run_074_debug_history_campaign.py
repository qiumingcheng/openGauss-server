#!/usr/bin/env python3
"""Replay ALL SQL0.56..SQL0.70 regression suites + native datavec tests
against an explicitly identified SQL0.74 Debug build
across debug(pool=off), debug(pool=on 4w), and ann-debug(pool=off).
"""
import argparse
import getpass
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time

import psycopg2

def default_workspace():
    configured = os.environ.get('CPP_WORKSPACE')
    if configured:
        return Path(configured).expanduser().resolve()
    root = next(p for p in Path(__file__).resolve().parents if p.name == 'integration').parent
    if (root / 'contrib/og_lake').is_dir():
        return root.parent.parent if root.parent.name == '.worktrees' else root.parent
    return root


ROOT = default_workspace()
INTEGRATION = ROOT / 'integration/opengauss-native-lake'
VERIF = ROOT / '公共文档/openGauss原生湖存储/验证'
REPLAY_DIR = Path(__file__).resolve().parent / 'run_074_compat/replay_056_074'
CONFIG = None
SOURCE = None
ROARING_BUILD = None
BLAS_BUILD = None
BUILD_ENTRY = None
EXPECTED_SDK54 = None
EXPECTED_SDK6 = None
EXPECTED_DEBUG_CONF = None
EXPECTED_ANN_DEBUG_CONF = None
DEBUG_CONF = None
ANN_DEBUG_CONF = None
SDK54_SO = None
SDK6_SO = None
INSTALL = None


def sha256_argument(value):
    if not re.fullmatch(r'[0-9a-fA-F]{64}', value):
        raise argparse.ArgumentTypeError('expected an explicit 64-digit SHA256')
    return value.lower()


def configure(args):
    global ROOT, INTEGRATION, VERIF, REPLAY_DIR, CONFIG, SOURCE
    global ROARING_BUILD, BLAS_BUILD, BUILD_ENTRY, INSTALL, SDK54_SO, SDK6_SO
    global DEBUG_CONF, ANN_DEBUG_CONF
    global EXPECTED_SDK54, EXPECTED_SDK6, EXPECTED_DEBUG_CONF, EXPECTED_ANN_DEBUG_CONF
    CONFIG = args
    ROOT = args.workspace.expanduser().resolve()
    INTEGRATION = ROOT / 'integration/opengauss-native-lake'
    # The existing build_driver and helpers locate cpp with parents[2].
    # Use the real workspace copy documented in README_MIGRATION.md, not a symlink.
    driver = INTEGRATION / 'build_driver.py'
    if not driver.is_file() or driver.resolve().parents[2] != ROOT:
        raise RuntimeError('Copy integration/opengauss-native-lake into CPP_WORKSPACE/integration (not a symlink) before running this campaign')
    VERIF = ROOT / '公共文档/openGauss原生湖存储/验证'
    REPLAY_DIR = args.replay_dir.expanduser().resolve()
    inferred_source = next((p for p in Path(__file__).resolve().parents if (p / 'contrib/og_lake').is_dir()), ROOT / '.worktrees/opengauss-native-lake')
    SOURCE = (args.source or Path(os.environ.get('OG_LAKE_SOURCE', str(inferred_source)))).expanduser().resolve()
    ROARING_BUILD = args.roaring_build.expanduser().resolve(strict=True)
    BLAS_BUILD = args.blas_build.expanduser().resolve(strict=True)
    BUILD_ENTRY = (args.build_entry or INTEGRATION / 'native_build_entry.sh').expanduser().resolve(strict=True)
    INSTALL = ROOT / '.install/opengauss-native-lake-debug'
    DEBUG_CONF = ROOT / '.runtime/opengauss-native-lake/debug/data/postgresql.conf'
    ANN_DEBUG_CONF = ROOT / '.runtime/opengauss-native-lake/ann-debug/data/postgresql.conf'
    SDK54_SO = INSTALL / 'lib/postgresql/og_lake_sdk/libog_lake_sdk_arrow.so'
    SDK6_SO = INSTALL / 'lib/postgresql/og_lake_external_sdk/libog_lake_sdk_arrow.so'
    EXPECTED_SDK54 = args.expected_sdk54
    EXPECTED_SDK6 = args.expected_sdk6
    EXPECTED_DEBUG_CONF = args.expected_debug_conf
    EXPECTED_ANN_DEBUG_CONF = args.expected_ann_debug_conf
    if not re.fullmatch(r'[A-Za-z0-9_.-]+', args.campaign_id):
        raise RuntimeError('campaign-id must be a single safe directory name')
    if not SOURCE.is_dir():
        raise RuntimeError('Source checkout not found: ' + str(SOURCE))
    assert sha256_file(BLAS_BUILD) == args.blas_build_sha256
    meta = json.loads(args.build_receipt.read_text(encoding='utf-8'))
    assert meta.get('status') == 'build_and_sql_passed', meta.get('status')
    assert sha256_file(INSTALL / 'bin/gaussdb') == meta['binary_sha256']
    required = ['run_%03d_suite.py' % version for version in range(56, 71)]
    required += ['oracle_060.py', 'worker_060.py', 'evolution_060.py']
    for name in required:
        if not (REPLAY_DIR / name).is_file():
            raise RuntimeError('Missing published replay dependency: ' + name)


def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def driver_env():
    env = os.environ.copy()
    env['OG_LAKE_SOURCE'] = str(SOURCE)
    env['CPP_WORKSPACE'] = str(ROOT)
    env['PGUSER'] = CONFIG.user
    return env


def run_cmd(cmd, env=None, label='', timeout=900):
    print(f'[{time.strftime("%H:%M:%S")}] START {label}', flush=True)
    merged_env = driver_env()
    if env:
        merged_env.update(env)
    proc = subprocess.run(cmd, cwd=str(ROOT), env=merged_env, capture_output=True, text=True, timeout=timeout)
    if proc.returncode != 0:
        so = proc.stdout if len(proc.stdout) <= 4000 else (proc.stdout[:1800] + '\n...[truncated]...\n' + proc.stdout[-1800:])
        se = proc.stderr if len(proc.stderr) <= 4000 else (proc.stderr[:1800] + '\n...[truncated]...\n' + proc.stderr[-1800:])
        raise RuntimeError(
            f'{label} failed ({proc.returncode}): {" ".join(map(str, cmd))}\n'
            f'STDOUT:\n{so}\nSTDERR:\n{se}'
        )
    print(f'[{time.strftime("%H:%M:%S")}] DONE  {label}', flush=True)
    return proc


def patch_conf(orig_bytes: bytes, overrides: dict) -> bytes:
    lines = orig_bytes.decode('utf-8').splitlines()
    rem = dict(overrides)
    out = []
    for ln in lines:
        s = ln.lstrip()
        matched = None
        if s and not s.startswith('#'):
            for k in list(rem.keys()):
                if s.startswith(k) and s[len(k):].lstrip().startswith('='):
                    matched = k
                    break
        if matched:
            out.append(f'{matched} = {rem.pop(matched)}')
        else:
            out.append(ln)
    for k, v in rem.items():
        out.append(f'{k} = {v}')
    return ('\n'.join(out) + '\n').encode('utf-8')


def driver_cmd(profile, action, run_id, build_receipt=None):
    cmd = [
        sys.executable, str(INTEGRATION / 'build_driver.py'), profile, action,
        '--run-id', run_id,
        '--roaring-build', str(ROARING_BUILD),
        '--pq-in-tree',
        '--blas-build', str(BLAS_BUILD),
        '--blas-build-sha256', CONFIG.blas_build_sha256,
        '--build-entry', str(BUILD_ENTRY),
    ]
    if action == 'restart' and profile == 'ann-debug' and build_receipt is not None:
        cmd.extend(['--reuse-build-receipt', str(build_receipt)])
    return cmd


def make_live_receipt(base_receipt_path: Path, profile: str, out_path: Path) -> Path:
    data_dir = ROOT / '.runtime/opengauss-native-lake' / profile / 'data'
    pid = int((data_dir / 'postmaster.pid').read_text().splitlines()[0])
    port = 55560 if profile == 'debug' else 55580
    meta = json.loads(base_receipt_path.read_text(encoding='utf-8'))
    meta['profile'] = profile
    meta['data'] = str(data_dir)
    meta['port'] = port
    meta['running_pid'] = pid
    meta['running_binary_sha256'] = sha256_file(INSTALL / 'bin/gaussdb')
    out_path.write_text(json.dumps(meta, indent=2), encoding='utf-8')
    return out_path


def run_output_suite(script_name: str, profile: str, out_json: Path):
    env = os.environ.copy()
    env['OG_LAKE_TEST_PROFILE'] = profile
    run_cmd([sys.executable, str(REPLAY_DIR / script_name), '--output', str(out_json)],
            env=env, label=f'{profile}:{script_name}')
    return json.loads(out_json.read_text(encoding='utf-8'))


def run_profile_suite(script_name: str, profile: str, out_json: Path, extra=()):
    run_cmd([sys.executable, str(REPLAY_DIR / script_name), '--profile', profile, '--out', str(out_json), *extra],
            label=f'{profile}:{script_name}{" " + " ".join(extra) if extra else ""}')
    return json.loads(out_json.read_text(encoding='utf-8'))


def run_060_isolated_pool_worker(base_receipt: Path, p_out: Path, stamp: str):
    """Run SQL0.60 pool=on + maintenance=on in a clean fresh Debug cluster so preserved DBs in canonical PGDATA cannot emit unrelated failed_batches."""
    sys.path.insert(0, str(INTEGRATION))
    import build_driver
    out_dir = p_out / 'run_060_full_worker'
    if out_dir.exists():
        raise FileExistsError('Refusing to replace prior campaign evidence: ' + str(out_dir))
    iso_path = ROOT / '.runtime/opengauss-native-lake' / f'sql060p-{stamp}_{os.getpid()}'
    iso_data = iso_path / 'data'
    iso_boot = p_out / f'iso_boot_{stamp}'
    iso_boot.mkdir(parents=True, exist_ok=True)
    iso_path.mkdir(mode=0o700, parents=True, exist_ok=True)
    base_meta = json.loads(base_receipt.read_text(encoding='utf-8'))
    (iso_path / 'owner.json').write_text(json.dumps({
        'project': 'opengauss-native-lake-sql060-pool',
        'data': str(iso_data),
        'port': 55560,
        'main_runtime': str(ROOT / '.runtime/opengauss-native-lake/debug'),
        'install': str(INSTALL),
        'build_sha256': sha256_file(base_receipt),
    }, indent=2) + '\n', encoding='utf-8')
    build_driver.bootstrap(iso_data, iso_path, iso_boot, prefix=INSTALL)
    iso_conf = iso_data / 'postgresql.conf'
    iso_conf.write_bytes(patch_conf(iso_conf.read_bytes(), POOL_060))
    os.chmod(iso_conf, 0o600)

    gcc = ROOT / 'binarylibs/buildtools/gcc10.3/gcc/lib64'
    env = driver_env()
    env.update(GAUSSHOME=str(INSTALL), PGDATA=str(iso_data), PGPORT='55560',
               PGHOST=str(iso_path / 'socket'), LD_LIBRARY_PATH=f'{INSTALL}/lib:{gcc}',
               OG_LAKE_TEST_PROFILE='debug', OG_LAKE_060_ISOLATED_RUNTIME=str(iso_path))
    ctl = INSTALL / 'bin/gs_ctl'
    try:
        run_cmd([str(ctl), 'start', '-w', '-t', '60', '-D', str(iso_data), '-Z', 'single_node',
                 '-l', str(iso_path / 'server.log'), '-o', '-p 55560'],
                env=env, label='start isolated sql060p pool+maint debug')
        pid = int((iso_data / 'postmaster.pid').read_text().splitlines()[0])
        live_rc = p_out / 'live_receipt_060_debug_on_on_iso.json'
        meta = dict(base_meta)
        meta.update(profile='debug', data=str(iso_data), port=55560, running_pid=pid,
                    running_binary_sha256=sha256_file(INSTALL / 'bin/gaussdb'))
        live_rc.write_text(json.dumps(meta, indent=2), encoding='utf-8')
        run_cmd([
            sys.executable, str(REPLAY_DIR / 'run_060_suite.py'),
            '--build', str(live_rc),
            '--output', str(out_dir),
            '--pool', 'on',
            '--maintenance', 'on',
        ], env=env, label='debug:run_060_suite(pool=on,maint=on,isolated)', timeout=900)
        return json.loads((out_dir / 'result.json').read_text(encoding='utf-8'))
    finally:
        if (iso_data / 'postmaster.pid').exists():
            subprocess.run([str(ctl), 'stop', '-D', str(iso_data), '-m', 'fast', '-w'],
                           env=env, check=False, capture_output=True, text=True)
        shutil.rmtree(iso_path, ignore_errors=True)


def run_060_suite(base_receipt: Path, profile: str, pool: str, maint: str, out_dir: Path):
    if out_dir.exists():
        raise FileExistsError('Refusing to replace prior campaign evidence: ' + str(out_dir))
    live_rc = make_live_receipt(base_receipt, profile, out_dir.parent / f'live_receipt_060_{profile}_{pool}_{maint}.json')
    env = os.environ.copy()
    env['OG_LAKE_TEST_PROFILE'] = profile
    run_cmd([
        sys.executable, str(REPLAY_DIR / 'run_060_suite.py'),
        '--build', str(live_rc),
        '--output', str(out_dir),
        '--pool', pool,
        '--maintenance', maint,
    ], env=env, label=f'{profile}:run_060_suite(pool={pool},maint={maint})', timeout=900)
    return json.loads((out_dir / 'result.json').read_text(encoding='utf-8'))


def run_datavec_regress(profile: str, out_dir: Path):
    """Run upstream diskann_vector.sql, hnsw_lsg_vector.sql, hnsw_rabitq_vector.sql cleanly."""
    port = 55560 if profile == 'debug' else (55570 if profile == 'release' else 55580)
    sock = str(ROOT / '.runtime/opengauss-native-lake' / profile / 'socket')
    inst_dir = ROOT / ('.install/opengauss-native-lake-release' if profile == 'release' else '.install/opengauss-native-lake-debug')
    gsql = str(inst_dir / 'bin/gsql')
    gsql_env = driver_env()
    gsql_env['GAUSSHOME'] = str(inst_dir)
    gsql_env['LD_LIBRARY_PATH'] = f"{inst_dir}/lib:{ROOT}/binarylibs/buildtools/gcc10.3/gcc/lib64"
    regress_dir = SOURCE / 'src/test/regress'

    results = {}
    for idx, name in enumerate(['diskann_vector', 'hnsw_lsg_vector', 'hnsw_rabitq_vector']):
        sql_path = regress_dir / 'sql' / f'{name}.sql'
        lines = sql_path.read_text(encoding='utf-8').splitlines()
        # Filter out CREATE DATABASE test / \c test / \c regression / DROP DATABASE test so we run in our own DB
        body = []
        for ln in lines:
            st = ln.strip().lower()
            if st in ('create database test;', '\\c test', '\\c regression', 'drop database test;'):
                continue
            body.append(ln)
        test_sql = '\n'.join(body) + '\n'
        tmp_sql = out_dir / f'{name}.adapted.sql'
        tmp_sql.write_text(test_sql, encoding='utf-8')
        db_name = f'dv_{name}_{os.getpid()}'

        c = psycopg2.connect(host=sock, port=port, dbname='postgres', user=CONFIG.user)
        c.autocommit = True
        with c.cursor() as cur:
            cur.execute(f'DROP DATABASE IF EXISTS {db_name}')
            cur.execute(f"CREATE DATABASE {db_name} DBCOMPATIBILITY 'PG'")
        c.close()
        try:
            proc = subprocess.run(
                [gsql, '-h', sock, '-p', str(port), '-U', CONFIG.user, '-d', db_name, '-v', 'ON_ERROR_STOP=0', '-f', str(tmp_sql)],
                env=gsql_env, capture_output=True, text=True, timeout=300
            )
            assert proc.returncode == 0, f'{name} gsql failed ({proc.returncode}): {proc.stderr}'
            out_txt = proc.stdout + '\n' + proc.stderr
            (out_dir / f'{name}.out').write_text(out_txt, encoding='utf-8')
            # Verify key expected behavior from upstream .out
            if name == 'diskann_vector':
                assert 'Ann Index Scan using idx_vectors_l2 on diskann_t1' in out_txt
                assert 'Ann Index Scan using idx_vectors_cosine on diskann_t1' in out_txt
                assert 'Ann Index Scan using idx_vectors_ip on diskann_t1' in out_txt
                assert 'Ann Index Scan using idx_vectors_10d_100 on diskann_t2' in out_txt
                assert 'column cannot have more than 1536 dimensions for diskann index' in out_txt
                assert 'value 15 out of bounds for option "index_size"' in out_txt
                assert 'value 1001 out of bounds for option "index_size"' in out_txt
                assert out_txt.count('ERROR:') == 3, out_txt[-2000:]
                assert out_txt.count('(2 rows)') >= 5, out_txt[-2000:]
                for expected_id_row in ('  8 |', '  7 |', '  6 |', ' 10 |', '  9 |'):
                    assert expected_id_row in out_txt, (expected_id_row, out_txt[-2000:])
            elif name == 'hnsw_lsg_vector':
                assert out_txt.count('Order By: (val <=>') >= 2, out_txt[-2000:]
                assert out_txt.count('Order By: (val <->') >= 2, out_txt[-2000:]
                assert out_txt.count('Order By: (val <#>') >= 2, out_txt[-2000:]
                assert 'create an hnsw lsg index requires inserting data first' in out_txt.lower()
                assert 'hnsw does not support the mixed use of the lsg and quantization methods' in out_txt.lower()
                assert out_txt.count('ERROR:') == 6, out_txt[-2000:]
            elif name == 'hnsw_rabitq_vector':
                assert out_txt.count('Order By: (val <=>') >= 2, out_txt[-2000:]
                assert out_txt.count('Order By: (val <->') >= 2, out_txt[-2000:]
                assert out_txt.count('Order By: (val <#>') >= 2, out_txt[-2000:]
                assert 'ERROR:' not in out_txt, out_txt[-2000:]
            results[name] = {'status': 'passed', 'stdout_lines': len(proc.stdout.splitlines())}
        finally:
            c = psycopg2.connect(host=sock, port=port, dbname='postgres', user=CONFIG.user)
            c.autocommit = True
            with c.cursor() as cur:
                cur.execute(f'DROP DATABASE IF EXISTS {db_name}')
            c.close()
    return results


MAINT_IX5_064 = {
    'enable_lake_maintenance': 'on',
    'enable_lake_index_maintenance': 'on',
    'lake_flush_naptime': '200ms',
    'lake_index_maintenance_naptime': '200ms',
    'lake_flush_max_delay': '3600000ms',
    'lake_flush_min_rows': '1000',
    'lake_index_build_budget': '600000ms',
}
MAINT_067 = {
    'enable_lake_maintenance': 'on',
    'enable_lake_index_maintenance': 'on',
    'enable_lake_autoanalyze': 'on',
    'lake_flush_naptime': '250',
    'lake_index_maintenance_naptime': '250',
}
MAINT_068 = {
    'enable_lake_maintenance': 'on',
    'enable_lake_index_maintenance': 'on',
    'enable_lake_autoanalyze': 'on',
    'lake_flush_naptime': '250',
    'lake_flush_min_rows': '5',
    'lake_flush_max_delay': '250',
    'lake_index_maintenance_naptime': '250',
}
MAINT_070 = {
    'enable_lake_maintenance': 'on',
    'enable_lake_index_maintenance': 'on',
    'enable_lake_autoanalyze': 'on',
    'lake_flush_naptime': '15000',
    'lake_flush_min_rows': '500',
    'lake_flush_max_delay': '20000',
    'lake_index_maintenance_naptime': '1000',
}
POOL_060 = {
    'enable_thread_pool': 'on',
    'thread_pool_attr': "'4, 1, (nobind)'",
    'enable_lake_maintenance': 'on',
    'lake_flush_naptime': '200',
    'lake_flush_worker_budget': '10000',
    'lake_flush_min_rows': '1000000',
    'lake_flush_max_delay': '3600000',
}


def run_profile_suite_batch(profile, base_receipt, conf_path, orig_conf_bytes, is_pool_on, out_dir, results):
    pool_label = 'pool_on' if is_pool_on else 'pool_off'
    base_ov = {'enable_thread_pool': 'on', 'thread_pool_attr': "'4, 1, (nobind)'"} if is_pool_on else {}
    stamp = time.strftime('%H%M%S')

    # Phase 1: Baseline conf (pool=off or pool=on)
    if base_ov:
        conf_path.write_bytes(patch_conf(orig_conf_bytes, base_ov))
    else:
        conf_path.write_bytes(orig_conf_bytes)
    os.chmod(conf_path, 0o600)
    run_cmd(driver_cmd(profile, 'restart', f'hist074_{profile}_{pool_label}_base_{stamp}', base_receipt),
            label=f'restart {profile} {pool_label} base')

    admin = psycopg2.connect(
        host=str(ROOT / '.runtime/opengauss-native-lake' / profile / 'socket'),
        port=55560 if profile == 'debug' else 55580, dbname='postgres', user=CONFIG.user,
    )
    try:
        with admin.cursor() as cur:
            cur.execute("SELECT default_version FROM pg_available_extensions WHERE name='og_lake'")
            row = cur.fetchone()
            assert row and row[0] == CONFIG.expected_extension_version, row
    finally:
        admin.close()

    p_out = out_dir / f'{profile}_{pool_label}'
    p_out.mkdir(parents=True, exist_ok=True)
    target_res = {}

    for s in ('run_056_suite.py', 'run_057_suite.py', 'run_058_suite.py', 'run_059_suite.py', 'run_061_suite.py'):
        r = run_output_suite(s, profile, p_out / f'{s}.json')
        target_res[s] = r

    target_res['run_060_manual'] = run_060_suite(
        base_receipt, profile, 'on' if is_pool_on else 'off', 'off', p_out / 'run_060_manual'
    )

    for s in ('run_062_suite.py', 'run_063_suite.py', 'run_064_suite.py', 'run_065_suite.py', 'run_066_suite.py', 'run_069_suite.py'):
        r = run_profile_suite(s, profile, p_out / f'{s}.json')
        assert r['status'] in ('pass', 'passed'), (s, r)
        target_res[s] = r

    if profile == 'ann-debug':
        target_res['datavec_regress'] = run_datavec_regress(profile, p_out)

    # Phase 2: 060 pool_on + maint (only when debug + pool_on; isolated clean cluster avoids spurious failed_batches on preserved debug DBs)
    if profile == 'debug' and is_pool_on:
        run_cmd(driver_cmd(profile, 'stop', f'hist074_{profile}_{pool_label}_preiso_{stamp}', base_receipt),
                label=f'stop {profile} before isolated 060 pool+maint')
        target_res['run_060_full_worker'] = run_060_isolated_pool_worker(base_receipt, p_out, stamp)

    # Phase 3: 064 --ix5-only (only pool=off as originally designed)
    if not is_pool_on:
        ov = dict(base_ov)
        ov.update(MAINT_IX5_064)
        conf_path.write_bytes(patch_conf(orig_conf_bytes, ov))
        os.chmod(conf_path, 0o600)
        run_cmd(driver_cmd(profile, 'restart', f'hist074_{profile}_{pool_label}_ix5_{stamp}', base_receipt),
                label=f'restart {profile} {pool_label} ix5')
        r_ix5 = run_profile_suite('run_064_suite.py', profile, p_out / 'run_064_ix5.json', extra=('--ix5-only',))
        assert r_ix5['status'] in ('pass', 'passed'), r_ix5
        target_res['run_064_ix5'] = r_ix5

    # Phase 4: 067 (250ms maint)
    ov = dict(base_ov)
    ov.update(MAINT_067)
    conf_path.write_bytes(patch_conf(orig_conf_bytes, ov))
    os.chmod(conf_path, 0o600)
    run_cmd(driver_cmd(profile, 'restart', f'hist074_{profile}_{pool_label}_m067_{stamp}', base_receipt),
            label=f'restart {profile} {pool_label} m067')
    r_067 = run_profile_suite('run_067_suite.py', profile, p_out / 'run_067.json')
    assert r_067['status'] in ('pass', 'passed'), r_067
    target_res['run_067_suite.py'] = r_067

    # Phase 5: 068 (min_rows=5 maint)
    ov = dict(base_ov)
    ov.update(MAINT_068)
    conf_path.write_bytes(patch_conf(orig_conf_bytes, ov))
    os.chmod(conf_path, 0o600)
    run_cmd(driver_cmd(profile, 'restart', f'hist074_{profile}_{pool_label}_m068_{stamp}', base_receipt),
            label=f'restart {profile} {pool_label} m068')
    r_068 = run_profile_suite('run_068_suite.py', profile, p_out / 'run_068.json')
    assert r_068['status'] in ('pass', 'passed'), r_068
    target_res['run_068_suite.py'] = r_068

    # Phase 6: 070
    ov = dict(base_ov)
    ov.update(MAINT_070)
    conf_path.write_bytes(patch_conf(orig_conf_bytes, ov))
    os.chmod(conf_path, 0o600)
    run_cmd(driver_cmd(profile, 'restart', f'hist074_{profile}_{pool_label}_m070_{stamp}', base_receipt),
            label=f'restart {profile} {pool_label} m070')
    r_070 = run_profile_suite('run_070_suite.py', profile, p_out / 'run_070.json')
    assert r_070.get('status', 'passed') in ('pass', 'passed') and r_070.get('total_checks', 0) > 0, r_070
    target_res['run_070_suite.py'] = r_070

    results[f'{profile}_{pool_label}'] = target_res


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--build-receipt', type=Path, required=True)
    ap.add_argument('--workspace', type=Path, default=ROOT)
    ap.add_argument('--source', type=Path)
    ap.add_argument('--replay-dir', type=Path, default=REPLAY_DIR)
    ap.add_argument('--build-entry', type=Path)
    ap.add_argument('--roaring-build', type=Path, required=True)
    ap.add_argument('--blas-build', type=Path, required=True)
    ap.add_argument('--blas-build-sha256', type=sha256_argument, required=True)
    ap.add_argument('--expected-sdk54', type=sha256_argument, required=True)
    ap.add_argument('--expected-sdk6', type=sha256_argument, required=True)
    ap.add_argument('--expected-debug-conf', type=sha256_argument, required=True)
    ap.add_argument('--expected-ann-debug-conf', type=sha256_argument, required=True)
    ap.add_argument('--user', default=os.environ.get('PGUSER') or getpass.getuser())
    ap.add_argument('--expected-extension-version', default='0.74')
    ap.add_argument('--campaign-id', default='native_074_debug_history_' + time.strftime('%Y%m%d_%H%M%S'))
    args = ap.parse_args()
    configure(args)

    out_dir = VERIF / args.campaign_id
    out_dir.mkdir(parents=True, exist_ok=False)

    assert sha256_file(SDK54_SO) == EXPECTED_SDK54
    assert sha256_file(SDK6_SO) == EXPECTED_SDK6
    orig_dbg = DEBUG_CONF.read_bytes()
    orig_ann = ANN_DEBUG_CONF.read_bytes()
    assert hashlib.sha256(orig_dbg).hexdigest() == EXPECTED_DEBUG_CONF
    assert hashlib.sha256(orig_ann).hexdigest() == EXPECTED_ANN_DEBUG_CONF

    run_stamp = int(time.time())
    for p in ('debug', 'ann-debug', 'release'):
        subprocess.run(driver_cmd(p, 'stop', f'{args.campaign_id}_prestop_{p}_{run_stamp}'), cwd=str(ROOT), env=driver_env(), check=False)

    results = {}
    try:
        try:
            run_profile_suite_batch('debug', args.build_receipt, DEBUG_CONF, orig_dbg, False, out_dir, results)
            run_profile_suite_batch('debug', args.build_receipt, DEBUG_CONF, orig_dbg, True, out_dir, results)
        finally:
            DEBUG_CONF.write_bytes(orig_dbg)
            os.chmod(DEBUG_CONF, 0o600)
            subprocess.run(driver_cmd('debug', 'stop', f'{args.campaign_id}_poststop_debug_{int(time.time())}'), cwd=str(ROOT), env=driver_env(), check=False)

        try:
            run_profile_suite_batch('ann-debug', args.build_receipt, ANN_DEBUG_CONF, orig_ann, False, out_dir, results)
        finally:
            ANN_DEBUG_CONF.write_bytes(orig_ann)
            os.chmod(ANN_DEBUG_CONF, 0o600)
            subprocess.run(driver_cmd('ann-debug', 'stop', f'{args.campaign_id}_poststop_anndebug_{int(time.time())}'), cwd=str(ROOT), env=driver_env(), check=False)
    finally:
        assert sha256_file(SDK54_SO) == EXPECTED_SDK54
        assert sha256_file(SDK6_SO) == EXPECTED_SDK6
        assert sha256_file(DEBUG_CONF) == EXPECTED_DEBUG_CONF
        assert sha256_file(ANN_DEBUG_CONF) == EXPECTED_ANN_DEBUG_CONF

    pg = subprocess.run(['pgrep', '-xa', 'gaussdb'], capture_output=True, text=True)
    assert pg.returncode != 0, f'Leftover gaussdb:\n{pg.stdout}'

    build_meta = json.loads(args.build_receipt.read_text(encoding='utf-8'))
    summary = {
        'campaign_id': args.campaign_id,
        'status': 'passed_SQL056_TO_SQL070_AND_DATAVEC_REPLAY_ON_SQL074',
        'build_receipt': str(args.build_receipt),
        'build_receipt_sha256': sha256_file(args.build_receipt),
        'binary_sha256': build_meta['binary_sha256'],
        'source_head': build_meta['source_head'],
        'immutable_verified': {
            'SDK54_SO': EXPECTED_SDK54,
            'SDK6_SO': EXPECTED_SDK6,
            'DEBUG_CONF': EXPECTED_DEBUG_CONF,
            'ANN_DEBUG_CONF': EXPECTED_ANN_DEBUG_CONF,
        },
        'suites_per_target': {k: list(v.keys()) for k, v in results.items()},
    }
    idx_path = out_dir / 'index.json'
    idx_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False) + '\n', encoding='utf-8')
    print(json.dumps(summary, indent=2, ensure_ascii=False))
    print(f'SAVED {idx_path} sha256={sha256_file(idx_path)}')


if __name__ == '__main__':
    main()
