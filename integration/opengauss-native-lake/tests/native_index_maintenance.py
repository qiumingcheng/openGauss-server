#!/usr/bin/env python3
"""IX5 candidate: actual worker prepare/IO/publish and durable source reservations.

Requires an official future Debug build and explicitly enabled owned launcher
plus index maintenance. Does not edit configuration, restart a postmaster or
pretend small deterministic ANN samples establish general recall/performance.
The worker-restart mode exits only the diagnostic worker after real prepare;
a postmaster crash/restart and target RAC remain separate acceptance items.
"""
from copy import deepcopy
import argparse
import base64
from collections import Counter
import fcntl
import hashlib
import json
import os
import re
from pathlib import Path
import secrets
import shutil
import subprocess
import sys
import time
import traceback
from types import SimpleNamespace

ROOT = Path(os.getenv('CPP_WORKSPACE', str(Path(__file__).resolve().parents[3]))).resolve()
D = ROOT / 'integration/opengauss-native-lake'
sys.path[:0] = [str(D), str(D / 'tests')]
from file_dml_acceptance import check_build
from inline_fixture import query, scalar
from lake_harness import sha256, validate_run_id, write_json
from native_auto_flush import connect_control
from native_runtime import PROFILE, RUNTIME, SOURCE
from native_table_create import Case
from native_test_context import OwnedDatabase
from permission_api import OwnedRoles, cleanup_roles


def cleanup_timeout_record(exc, database, folder):
    def safe(value):
        if isinstance(value,bytes):return dict(encoding='base64',data=base64.b64encode(value).decode('ascii'),bytes=len(value))
        if isinstance(value,(list,tuple)):return [safe(item)for item in value]
        return value
    owner=Path(database.attempt)/'database-owner.json'
    record=dict(status='fail',kind='owned_cleanup_timeout',run_id=database.run_id,
        argv=safe(exc.cmd),timeout_seconds=exc.timeout,owner_receipt=str(owner),
        owner_receipt_sha256=sha256(owner)if owner.exists()else None,
        partial_stdout=safe(exc.stdout),partial_stderr=safe(exc.stderr),cleanup_proven=False,
        cross_restart_cleanup_attempted=False)
    path=folder/('database-cleanup-timeout-'+str(time.time_ns())+'.json')
    write_json(path,record)
    return dict(record,diagnostic_report=str(path),diagnostic_report_sha256=sha256(path))


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--run-id', required=True)
    p.add_argument('--build-receipt', required=True, type=Path)
    p.add_argument('--method', choices=['btree', 'bitmap', 'diskann'], default='btree')
    p.add_argument('--mode', choices=['build', 'repair', 'cancel', 'fault-after-prepare', 'worker-restart', 'drop', 'rename', 'cleanup-blocked'], required=True)
    p.add_argument('--budget-seconds', type=int, default=900)
    p.add_argument('--sql-version', choices=['0.18','0.19','0.20','0.21'], default='0.18')
    a = p.parse_args()
    validate_run_id(a.run_id)
    if PROFILE not in ['debug', 'ann-debug'] or not 60 <= a.budget_seconds <= 1800:
        p.error('Requires matching owned Debug profile and bounded observation60..1800 seconds')
    if a.method == 'diskann' and PROFILE != 'ann-debug':
        p.error('Native DiskANN test requires separately owned ann-debug reference catalogs')
    out = ROOT / '公共文档/openGauss原生湖存储/验证' / a.run_id
    out.mkdir(exist_ok=False)
    r = dict(status='fail', scope=__doc__, method=a.method, mode=a.mode, installed_sql=a.sql_version, assertions=[], gates=[],
             limitations=['AST preparation is not Native execution or qualification.',
                          'Single worker/segment, local immutable Parquet, default-off scheduling only.',
                          'Worker-exit recovery is distinct from postmaster crash/restart.',
                          'No real RAC, remote IO, unlimited scheduling scale, old-reader physical GC or performance qualification.'])
    files = [Path(__file__), D / 'tests/file_dml_acceptance.py', D / 'tests/native_table_create.py',
             D / 'tests/native_auto_flush.py', D / 'tests/permission_api.py', D / 'native_test_context.py',
             D / 'native_runtime.py', D / 'inline_fixture.py', D / 'lake_harness.py', D / 'tests/inline_cleanup.py',
             D / 'tests/catalog_fixture_bundle.py', D / 'tests/empty_manifest.py', D / 'tests/empty_reference_fixture.py',
             D / 'tests/empty_catalog_oracle.py', D / 'tests/create_reference_oracle.py', D / 'tests/reverse_reference.py',
             D / 'reference.sh', D / 'fixture.py', D / 'source-lock.json']
    archive = out / 'executed-inputs'
    archive.mkdir()
    r['inputs'] = {str(f): sha256(f) for f in files}
    for f in files:
        shutil.copy2(f, archive / f.name)
    shutil.copy2(a.build_receipt, archive / 'build.json')
    control = database = roles = reservation = None
    actors = []
    claimed = held = catalog_hold = False
    admin = observer = gate = None
    target = 'biz.orders'
    worker = None
    log = RUNTIME / 'server.log'
    log_offset = log.stat().st_size if log.exists() else 0

    def check(label, got, want=True):
        ok = got == want
        r['assertions'].append(dict(name=label, observed=deepcopy(got), expected=deepcopy(want), status='pass' if ok else 'fail'))
        write_json(out / 'result.json', r)
        if not ok:
            raise AssertionError(label + ': ' + repr(got))

    def configure(c):
        query(c, 'SET session_timeout=0')
        query(c, 'SET statement_timeout=30000')
        query(c, 'SET query_dop=1')
        query(c, 'SET enable_default_ustore_table=off')

    def conn():
        c = database.connect()
        actors.append(c)
        configure(c)
        return c

    def wait(label, fn, timeout=None):
        deadline = time.monotonic() + (timeout or a.budget_seconds)
        while True:
            value = fn()
            if value:
                return value
            if time.monotonic() >= deadline:
                r.setdefault('timeouts', []).append(dict(label=label, activity=query(observer,
                    'SELECT pid,application_name,state,query FROM pg_stat_activity WHERE datid=%s',
                    (database.allocation['database_oid'],))))
                raise AssertionError('Bounded wait expired: ' + label)
            time.sleep(.05)

    def claim():
        nonlocal claimed
        wait('acquire exact database maintenance claim', lambda: scalar(admin, 'SELECT og_lake_internal.test_maintenance_claim()'), 60)
        claimed = True

    def release_claim():
        nonlocal claimed
        check('release exact database native maintenance claim', scalar(admin, 'SELECT og_lake_internal.test_maintenance_release()'))
        claimed = False

    def probe(phase=0, action=0):
        return json.loads(scalar(admin, 'SELECT biz.index_gate(%s::uuid,%s::bigint,%s::integer,%s::bigint,%s::integer)',
                                 (uuid, source, phase, key, action)))

    def arm(phase, action=0):
        nonlocal held
        query(gate, 'SELECT pg_advisory_lock(%s::bigint)', (key,))
        held = True
        probe(phase, action)

    def release_gate():
        nonlocal held
        check('release only exact owned advisory gate', scalar(gate, 'SELECT pg_advisory_unlock(%s::bigint)', (key,)))
        held = False

    def actual_gate():
        nonlocal worker
        state = probe()
        pid = int(state['worker_pid'])
        if not state['hits'] or not pid:
            return None
        wanted = (database.allocation['database_oid'], key >> 32, key & 0xffffffff)
        locks = query(observer, "SELECT pid,granted FROM pg_locks WHERE locktype='advisory' AND database=%s "
                      "AND classid=%s AND objid=%s AND objsubid=1 AND mode='ExclusiveLock' AND pid=%s AND NOT granted", wanted + (pid,))
        owner = query(observer, "SELECT pid,granted FROM pg_locks WHERE locktype='advisory' AND database=%s "
                      "AND classid=%s AND objid=%s AND objsubid=1 AND mode='ExclusiveLock' AND pid=%s AND granted", wanted + (gate_pid,))
        if not locks or not owner:
            return None
        worker = pid
        r['gates'].append(dict(probe=state, waiter=locks, holder=owner))
        return state

    def ready():
        return query(observer, "SELECT s.segment_uuid::text,s.artifact_root_uri::text,s.manifest_hash,s.build_token::text "
                     "FROM og_lake_internal.index_segment s JOIN og_lake_internal.index_coverage c "
                     "USING(segment_uuid,index_uuid,definition_generation) WHERE c.index_uuid=%s::uuid AND c.source_file_id=%s "
                     "AND c.active AND s.state='READY'", (uuid, source))

    def segment_state(segment):
        return query(observer, 'SELECT state,manifest_hash FROM og_lake_internal.index_segment WHERE segment_uuid=%s::uuid', (segment,))

    def pin(segment):
        return query(observer, 'SELECT source_file_id,source_checksum,schema_mapping_hash,physical_row_count,active,source_file_bytes,source_path '
                     'FROM og_lake_internal.index_coverage WHERE segment_uuid=%s::uuid', (segment,))

    def equivalent(label):
        actual = query(observer, 'SELECT id,encode(vector_send(v),\'hex\'),note FROM ' + target + ' ORDER BY id')
        expected = query(observer, "SELECT id,encode(vector_send(v),'hex'),note FROM biz.exact ORDER BY id")
        check(label + ' full original rows', actual, expected)
        if a.method == 'diskann':
            query(observer, 'SET enable_lake_ann=on')
            query(observer, 'SET enable_seqscan=off')
            q = "SELECT id,note,v <-> '[5.5,2.25,3.125]'::vector AS d FROM {} WHERE v IS NOT NULL ORDER BY d LIMIT 5"
            got = query(observer, q.format(target))
            want = query(observer, q.format('biz.exact'))
            check(label + ' original-vector SQL rerank', got, want)
            text = '\n'.join(x[0] for x in query(observer, 'EXPLAIN (ANALYZE,COSTS OFF) ' + q.format(target)))
            (out / (label.replace(' ', '-') + '-plan.txt')).write_text(text + '\n')
            check(label + ' actual verified graph coverage', 'Verified ANN File Coverage: 1' in text)
        else:
            query(observer, 'SET enable_seqscan=off')
            q = 'SELECT id,note FROM {} WHERE id>=3 ORDER BY id'
            check(label + ' scalar predicate exact SQL', query(observer, q.format(target)), query(observer, q.format('biz.exact')))
            text = '\n'.join(x[0] for x in query(observer, 'EXPLAIN (ANALYZE,COSTS OFF) ' + q.format(target)))
            (out / (label.replace(' ', '-') + '-plan.txt')).write_text(text + '\n')
            check(label + ' actual verified scalar file coverage', 'Verified Scalar File Coverage: 1' in text)

    try:
        built = check_build(a.build_receipt)
        r.update(source_head=built['source_head'], binary_sha256=built['binary_sha256'],
                 build=str(a.build_receipt.resolve()), build_sha256=sha256(a.build_receipt))
        control = connect_control()
        configure(control)
        for name in ['enable_lake_maintenance', 'enable_lake_index_maintenance']:
            check('explicit owned scheduler enabled ' + name, scalar(control, 'SHOW ' + name), 'on')
        check('index IO has independent complete-segment budget', int(scalar(control, "SELECT setting FROM pg_settings WHERE name='lake_index_build_budget'")) >= 600000)
        check('setup cannot trigger ordinary auto flush by age', int(scalar(control, "SELECT setting FROM pg_settings WHERE name='lake_flush_max_delay'")), 3600000)
        # 32 initial versions + two after-images + one file deletion marker.
        # A low row threshold may flush the concurrent DML before index checks.
        check('ordinary auto flush threshold exceeds complete fixture bound',
              int(scalar(control, "SELECT setting FROM pg_settings WHERE name='lake_flush_min_rows'")) > 35)
        with (out / 'fixture.log').open('w') as output:
            subprocess.run([sys.executable, str(D / 'tests/catalog_fixture_bundle.py'), '--run-id', a.run_id,
                            '--output', str(out / 'fixture/manifest.json')], stdout=output, stderr=subprocess.STDOUT,
                           check=True, timeout=300)
        reservation = (ROOT / '.reference/opengauss-native-lake/.driver.lock').open('a+')
        fcntl.flock(reservation.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        database = OwnedDatabase(a.run_id, out)
        database.create()
        configure(database.admin)
        roles = OwnedRoles(control, a.run_id, out)
        t = Case(database, SimpleNamespace(mode='net_dml', sql_version=a.sql_version, manifest=out / 'fixture/manifest.json',
                                          output=out / 'result.json', run_id=a.run_id), r, roles)
        defaults=re.findall(r"^default_version\s*=\s*'([^']+)'",(SOURCE/'contrib/og_lake/og_lake.control').read_text(),re.M)
        check('one reviewed source runtime default',len(defaults),1)
        check('IX5 runtime explicitly reviewed',defaults[0] in ['0.18','0.19','0.20','0.21'])
        module_version=json.loads(scalar(t.a,'SELECT og_lake.host_status()'))['module_version']
        installed_version=scalar(t.a,"SELECT extversion FROM pg_extension WHERE extname='og_lake'")
        check('actual IX5 module matches exact source',module_version,defaults[0])
        check('actual IX5 installed SQL matches selection',installed_version,a.sql_version)
        check('selected installed capability is retained by source runtime',int(defaults[0].split('.')[1])>=int(a.sql_version.split('.')[1]))
        r.update(runtime_default_version=defaults[0],runtime_module_version=module_version,installed_sql=installed_version)
        for c in [t.a, t.b, t.observer]:
            configure(c)
        admin, observer, gate = t.a, t.observer, conn()
        gate_pid = scalar(gate, 'SELECT pg_backend_pid()')
        claim()
        query(admin, 'CREATE TABLE biz.exact(id integer,v vector(3),note text)')
        query(admin, "CREATE FOREIGN TABLE biz.orders(id integer,v vector(3),note text) SERVER created_lake OPTIONS(binding_mode 'create',lake_schema 'main')")
        query(admin, "SELECT og_lake.set_option('biz.orders'::regclass,'data_inlining_row_limit','50000')")
        for table in ['biz.orders', 'biz.exact']:
            query(admin, 'INSERT INTO ' + table + " SELECT i,('['||i::text||','||(i*i/100.0)::text||','||(i%13+.125)::text||']')::vector,'row/'||i::text FROM generate_series(1,32)i")
        query(admin, "SELECT og_lake.flush('biz.orders'::regclass)")
        check('one actual immutable physical source', scalar(observer, 'SELECT count(*) FROM created_metadata.ducklake_data_file WHERE end_snapshot IS NULL'), 1)
        source = scalar(observer, 'SELECT data_file_id FROM created_metadata.ducklake_data_file WHERE end_snapshot IS NULL')
        host = scalar(observer, "SELECT 'biz.orders'::regclass::oid")
        spec = dict(version=1, kind='vector' if a.method == 'diskann' else 'scalar', method=a.method,
                    column='v' if a.method == 'diskann' else 'id')
        if a.method == 'diskann':
            spec.update(metric='l2', enable_pq=False)
        uuid = str(scalar(admin, "SELECT og_lake.create_lake_index('biz.orders'::regclass,'ix_background',%s::jsonb)", (json.dumps(spec),)))
        store = out / 'index-store'
        store.mkdir()
        query(admin, "SELECT og_lake.set_option('created_lake'::name,'index_root',%s::text)", (str(store),))
        query(admin, "CREATE FUNCTION biz.index_gate(uuid,bigint,integer,bigint,integer) RETURNS text AS '$libdir/og_lake','og_lake_test_index_maintenance_gate' LANGUAGE C VOLATILE NOT FENCED")
        query(admin, 'REVOKE ALL ON FUNCTION biz.index_gate(uuid,bigint,integer,bigint,integer) FROM PUBLIC')
        key = (0x4f47 << 32) + secrets.randbelow(1 << 24)
        r['target'] = dict(index_uuid=uuid, source_file=source, host_oid=host, gate_key=key)
        before_snapshot = t.head()
        if a.mode == 'repair':
            release_claim()
            old = wait('initial automatic segment', ready)
            check('one initial automatic coverage', len(old), 1)
            claim()
            # Target one actual artifact from this exact owned segment only.
            candidates = [x for x in Path(old[0][1]).iterdir() if x.suffix == '.parquet']
            check('owned repair target has actual artifact files', bool(candidates))
            path = sorted(candidates)[0]
            check('tamper target remains in exact owned store', path.resolve().is_relative_to(store.resolve()))
            shutil.copy2(path, out / ('original-' + path.name))
            r['corrupt_artifact'] = dict(path=str(path), before_sha256=sha256(path), old_segment=old[0][0])
            path.write_bytes(b'')
        phase = 2 if a.mode == 'drop' else 1
        action = 1 if a.mode in ['fault-after-prepare', 'cleanup-blocked'] else 2 if a.mode == 'worker-restart' else 0
        arm(phase, action)
        release_claim()
        state = wait('actual phase gate from Native worker', actual_gate)
        segment = state['segment']
        check('exact worker executed gate once', state['hits'], 1)
        check('prepare committed actual BUILDING state', segment_state(segment), [('BUILDING', None)])
        pins = pin(segment)
        check('exact inactive committed source reservation', len(pins), 1)
        check('reserved exact physical source with no active coverage', (pins[0][0], pins[0][3], pins[0][4]), (source, 32, False))
        check('reservation has provider object token and schema identity', pins[0][1].startswith('object-version-v1:') and len(pins[0][2]) == 64)
        locks = query(observer, "SELECT relation::oid,mode FROM pg_locks WHERE pid=%s AND locktype='relation' AND relation IN "
                      "(SELECT oid FROM pg_class WHERE oid=%s OR relnamespace=(SELECT oid FROM pg_namespace WHERE nspname='og_lake_internal'))", (worker, host))
        check('long IO phase holds no business or host-private metadata relation locks', locks, [])
        if a.mode in ['build', 'repair', 'rename']:
            query(admin, 'BEGIN')
            for table in [target, 'biz.exact']:
                query(admin, 'UPDATE ' + table + " SET note='new/'||id::text WHERE id=1")
                query(admin, 'INSERT INTO ' + table + " VALUES(33,'[33,10.89,7.125]'::vector,'inline/33')")
            query(admin, 'COMMIT')
            check('DML and DQL proceed while the actual worker build gate is held', scalar(observer, 'SELECT count(*) FROM ' + target), 33)
            before_snapshot = t.head()
            if a.mode == 'rename':
                query(admin, 'ALTER FOREIGN TABLE biz.orders RENAME TO renamed_orders')
                target = 'biz.renamed_orders'
        if a.mode == 'drop':
            query(admin, 'DROP FOREIGN TABLE biz.orders')
            check('host DROP completes during live candidate IO phase', scalar(observer, 'SELECT count(*) FROM pg_class WHERE oid=%s', (host,)), 0)
        if a.mode in ['worker-restart', 'cleanup-blocked']:
            query(gate, 'BEGIN')
            query(gate, 'LOCK TABLE og_lake_internal.' + ('catalog_binding' if a.mode == 'cleanup-blocked' else 'index_coverage') + ' IN SHARE MODE')
            catalog_hold = True
        if a.mode == 'cancel':
            check('cancel actual exact worker at post-prepare gate', scalar(observer, 'SELECT pg_cancel_backend(%s)', (worker,)))
        released_at = time.monotonic()
        release_gate()
        if a.mode == 'cleanup-blocked':
            # Real private registration lock in cleanup, blocked by our exact
            # relation SHARE owner. No artificial result/status substitution.
            registration = scalar(observer, "SELECT 'og_lake_internal.catalog_binding'::regclass::oid")
            waiting = wait('actual cleanup registration wait', lambda: query(observer,
                "SELECT pid,mode,granted,relation FROM pg_locks WHERE pid=%s AND relation=%s AND mode='ShareRowExclusiveLock' AND NOT granted",
                (worker, registration)), 5)
            r['cleanup_lock_wait'] = dict(worker=worker, holder=gate_pid, relation=registration, wait=waiting,
                holder_locks=query(observer, "SELECT pid,mode,granted,relation FROM pg_locks WHERE pid=%s AND relation=%s AND mode='ShareLock' AND granted", (gate_pid, registration)))
            check('cleanup registration blocker is exact owned SHARE', bool(r['cleanup_lock_wait']['holder_locks']))
            wait('cleanup deadline releases old worker and DB claim', lambda: not scalar(observer,
                'SELECT count(*) FROM pg_stat_activity WHERE pid=%s', (worker,)), 12)
            elapsed = time.monotonic() - released_at
            r['cleanup_elapsed_seconds'] = elapsed
            check('cleanup finishes within bounded observation under held DDL lock', elapsed < 12)
            check('failed bounded cleanup retains durable unpublished state', segment_state(segment), [('BUILDING', None)])
            check('failed bounded cleanup retains exact source pin', pin(segment), pins)
            check('worker rethrows original phase fault after cleanup lock error', probe()['error_sqlstate'], 'XX000')
            query(gate, 'COMMIT')
            catalog_hold = False
        if a.mode == 'worker-restart':
            wait('prepared worker exited', lambda: not scalar(observer, 'SELECT count(*) FROM pg_stat_activity WHERE pid=%s', (worker,)), 30)
            check('durable BUILDING pin survives diagnostic worker exit', segment_state(segment), [('BUILDING', None)])
            check('recovery cannot silently discard source pin before old worker ends', len(pin(segment)), 1)
            query(gate, 'COMMIT')
            catalog_hold = False
        if a.mode in ['cancel', 'fault-after-prepare', 'worker-restart', 'drop', 'cleanup-blocked']:
            wait('exact failed reservation terminal cleanup', lambda: segment_state(segment) == [('FAILED', None)] and not pin(segment))
            check('unpublished failed segment has no active query coverage', pin(segment), [])
            if a.mode in ['cancel', 'fault-after-prepare']:
                check('exact worker preserves original cancellation/fault SQLSTATE', probe()['error_sqlstate'], '57014' if a.mode == 'cancel' else 'XX000')
            if a.mode == 'drop':
                check('late candidate never resurrects dropped definition', ready(), [])
                check('definition remains detached DROPPING', scalar(observer, 'SELECT state FROM og_lake_internal.index_definition WHERE index_uuid=%s::uuid', (uuid,)), 'DROPPING')
            else:
                live = wait('subsequent worker rebuilds persisted ACTIVE definition', ready)
                check('replacement uses another immutable segment/token', live[0][0] != segment)
                equivalent('recovered')
        else:
            live = wait('same prepared segment short-publishes READY', lambda: [x for x in ready() if x[0] == segment])
            check('actual candidate published with authoritative manifest', bool(live[0][2]))
            check('index build publishes no business snapshot', t.head(), before_snapshot)
            equivalent('published')
            if a.mode == 'repair':
                check('old corrupt artifact remains retained inactive metadata', scalar(observer, 'SELECT state FROM og_lake_internal.index_segment WHERE segment_uuid=%s::uuid', (old[0][0],)), 'RETIRED')
                check('repair retained exact source checksum', pin(segment)[0][1], pins[0][1])
        check('final actual IX5 installed SQL version',scalar(t.a,"SELECT extversion FROM pg_extension WHERE extname='og_lake'"),a.sql_version)
        check('all executed inputs remain unchanged', all(sha256(Path(f)) == h for f, h in r['inputs'].items()))
        check_build(a.build_receipt)
        r['status'] = 'pass'
    except BaseException as exc:
        r.update(reason=str(exc), traceback=traceback.format_exc())
    finally:
        if gate is not None:
            if catalog_hold:
                try:
                    query(gate, 'ROLLBACK')
                except Exception:
                    pass
            if held:
                try:
                    query(gate, 'SELECT pg_advisory_unlock(%s::bigint)', (key,))
                except Exception:
                    pass
        if admin is not None:
            try:
                query(admin, 'ROLLBACK')
                if 'uuid' in locals():
                    probe(-1)
                if claimed:
                    query(admin, 'SELECT og_lake_internal.test_maintenance_release()')
                    claimed = False
            except Exception as exc:
                r.update(status='fail', claim_cleanup_error=str(exc))
        # Reacquire the same DB claim only after the worker is cancelled/drained
        # before DROP DATABASE. A running worker is never terminated by prefix.
        if admin is not None and database is not None and database.allocation is not None:
            try:
                deadline = time.monotonic() + 30
                while not scalar(admin, 'SELECT og_lake_internal.test_maintenance_claim()'):
                    exact = query(admin, "SELECT pid FROM pg_stat_activity WHERE datid=%s AND application_name='og_lake flush worker'", (database.allocation['database_oid'],))
                    for (pid,) in exact:
                        scalar(admin, 'SELECT pg_cancel_backend(%s)', (pid,))
                    if time.monotonic() >= deadline:
                        raise AssertionError('Owned maintenance worker did not release database claim')
                    time.sleep(.05)
                # DROP the exact owned server while holding the claim prevents
                # a new worker from beginning another index before DB cleanup.
                query(admin, 'DROP SERVER IF EXISTS created_lake CASCADE')
                query(admin, 'SELECT og_lake_internal.test_maintenance_release()')
            except Exception as exc:
                r.update(status='fail', worker_cleanup_error=str(exc))
        if database is not None:
            try:
                database.cleanup()
                r['database_cleanup'] = database.cleanup_result
            except subprocess.TimeoutExpired as exc:
                r.update(status='fail',database_cleanup_error=str(exc),database_cleanup_timeout=cleanup_timeout_record(exc,database,out))
            except Exception as exc:
                r.update(status='fail', database_cleanup_error=str(exc))
        if roles is not None and control is not None:
            try:
                if not r.get('database_cleanup') or r['database_cleanup'].get('returncode')!=0:
                    raise RuntimeError('Exact owned database cleanup unproven; roles retained')
                r['role_cleanup'] = cleanup_roles(control, roles.path, a.run_id)
            except Exception as exc:
                r.update(status='fail', role_cleanup_error=str(exc))
        if roles is not None:r=roles.redact(r)
        if control is not None:
            control.close()
        if reservation is not None:
            reservation.close()
        if log.exists():
            with log.open('rb') as stream:
                stream.seek(log_offset)
                data = stream.read(8 * 1024 * 1024)
            (out / 'server-log-slice.log').write_bytes(data)
            r['server_log_slice'] = dict(offset=log_offset, bytes=len(data), sha256=hashlib.sha256(data).hexdigest())
        write_json(out / 'result.json', r)
    print(json.dumps(dict(status=r['status'], result=str(out / 'result.json'), assertions=len(r['assertions']))))
    return 0 if r['status'] == 'pass' else 1


if __name__ == '__main__':
    raise SystemExit(main())
