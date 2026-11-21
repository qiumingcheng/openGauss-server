#!/usr/bin/env python3
"""Real native launcher/worker flush, with exact owned-instance restoration.

No call to the manual flush SQL entry is made. Configuration restart receipts,
original ownership records and both success/failure outputs are retained.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time
import traceback
import threading
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

ROOT = Path('/home/qiumc/data/code/cpp')
D = ROOT / 'integration/opengauss-native-lake'
sys.path[:0] = [str(D), str(D / 'tests')]
from native_runtime import PROFILE, RUNTIME, PORT
from file_dml_acceptance import check_build
from inline_fixture import query, scalar
from lake_harness import sha256, validate_run_id, write_json
from native_table_create import Case
from native_test_context import OwnedDatabase
from permission_api import OwnedRoles, cleanup_roles
from reverse_reference import Reference, literal


def atomic_bytes(path, contents, mode):
    fd, name = tempfile.mkstemp(prefix='.lake-auto-', dir=path.parent)
    try:
        with os.fdopen(fd, 'wb') as stream:
            os.fchmod(stream.fileno(), mode)
            stream.write(contents)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(name, path)
        directory = os.open(str(path.parent), os.O_RDONLY | os.O_DIRECTORY)
        try: os.fsync(directory)
        finally: os.close(directory)
    finally:
        if os.path.exists(name): os.unlink(name)


def connect_control():
    import psycopg2
    c = psycopg2.connect(host=str(RUNTIME / 'socket'), port=PORT, dbname='postgres',
                         user=os.getenv('USER', 'qiumc'), connect_timeout=5,
                         application_name='og_lake_auto_flush_controller')
    c.autocommit = True
    return c



def cleanup_owned_after_restart(db, roles, run_id, owner_out, control, restart_receipt):
    expected = (db.allocation['database_oid'], db.database, db.allocation['owner_oid'], db.allocation['marker'])
    if query(control, "SELECT oid,datname,datdba,shobj_description(oid,'pg_database') FROM pg_database WHERE oid=%s",
             (db.allocation['database_oid'],)) != [expected]:
        raise RuntimeError('Owned database identity changed across controlled restart')
    cleanup_dir = owner_out / 'restart-cleanup'
    cleanup_db = OwnedDatabase(run_id, cleanup_dir)
    cleanup_db.database = db.database
    cleanup_db.allocation = dict(db.allocation, attempt=str(cleanup_dir),
        postmaster_start=str(scalar(control, 'SELECT pg_postmaster_start_time()')),
        original_owner_receipt_sha256=sha256(owner_out / 'database-owner.json'), restart_receipt=restart_receipt)
    cleanup_db._save_allocation()
    cleanup_db.connections=db.connections; db.connections=[]
    cleanup_db.cleanup()
    result = {'database_cleanup':cleanup_db.cleanup_result}
    if roles is not None:
        original_roles = json.loads(roles.path.read_text())
        for item in original_roles['roles']:
            if query(control, "SELECT oid,rolname,shobj_description(oid,'pg_authid') FROM pg_roles WHERE oid=%s OR rolname=%s",
                     (item['oid'],item['name'])) != [(item['oid'],item['name'],item['marker'])]:
                raise RuntimeError('Owned role identity changed across restart')
        recovered = dict(original_roles, postmaster_start=str(scalar(control, 'SELECT pg_postmaster_start_time()')),
                         original_receipt_sha256=sha256(roles.path), restart_receipt=restart_receipt)
        role_path=cleanup_dir/'permission-roles.json'; write_json(role_path,recovered)
        result['role_cleanup']=cleanup_roles(control,role_path,run_id)
    return result


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--run-id', required=True)
    p.add_argument('--build-receipt', type=Path, required=True)
    p.add_argument('--mode', choices=['rows', 'age', 'cancel', 'fairness', 'shutdown', 'cross_database', 'budget'], required=True)
    a = p.parse_args()
    validate_run_id(a.run_id)
    out = ROOT / '公共文档/openGauss原生湖存储/验证' / a.run_id
    out.mkdir(exist_ok=False)
    result = {'status': 'fail', 'scope': 'Actual native automatic flush: ' + a.mode,
              'assertions': [], 'limitations': ['Local '+PROFILE+' correctness only; no GC, long soak, private SDK or target RAC qualification.']}
    inputs = [Path(__file__), D / 'tests/native_table_create.py', D / 'tests/permission_api.py',
              D / 'native_test_context.py', D / 'inline_fixture.py', D / 'source-lock.json']
    inputs += [D / 'native_runtime.py', D / 'build_driver.py', D / 'tests/inline_cleanup.py']
    result['inputs'] = {str(x): sha256(x) for x in inputs}
    archive = out / 'executed-inputs'
    archive.mkdir()
    for x in inputs: shutil.copy2(x, archive / x.name)
    db = roles = control = t = None
    extra_databases = []
    changed = restored = False
    conf = RUNTIME / 'data/postgresql.conf'
    original = installed = None
    mode = None

    def check(name, got, want=True):
        ok = got == want
        result['assertions'].append({'name': name, 'observed': got, 'expected': want,
                                     'status': 'pass' if ok else 'fail'})
        write_json(out / 'result.json', result)
        if not ok: raise AssertionError(name)

    def restart(suffix):
        run = a.run_id + '_' + suffix
        with (out / (suffix + '.log')).open('w') as log:
            subprocess.run(['bash', str(D / 'build.sh'), PROFILE, 'restart', '--run-id', run],
                           stdout=log, stderr=subprocess.STDOUT, check=True, timeout=180)
        receipt = ROOT / '公共文档/openGauss原生湖存储/验证' / run / 'build.json'
        found = check_build(receipt)
        check(suffix + ' keeps exact product binary', found['binary_sha256'], result['binary_sha256'])
        result[suffix + '_receipt'] = str(receipt)
        return receipt

    def auto_state():
        return json.loads(scalar(t.observer, 'SELECT og_lake.host_status()'))['automatic_flush']

    def wait_for(label, predicate, seconds=25):
        deadline = time.monotonic() + seconds
        samples = []
        while True:
            state = auto_state()
            if not samples or state != samples[-1]: samples.append(state)
            if predicate(state): break
            if time.monotonic() >= deadline:
                result[label + '_samples'] = samples
                raise TimeoutError(label)
            time.sleep(.1)
        result[label + '_samples'] = samples
        return state

    try:
        from psycopg2 import sql
        built = check_build(a.build_receipt)
        result.update(source_head=built['source_head'], binary_sha256=built['binary_sha256'])
        with (out / 'fixture.log').open('w') as log:
            subprocess.run([sys.executable, str(D / 'tests/catalog_fixture_bundle.py'), '--run-id', a.run_id,
                            '--output', str(out / 'fixture/manifest.json')], stdout=log,
                           stderr=subprocess.STDOUT, check=True, timeout=300)
        control = connect_control()
        check('dedicated owned data directory', scalar(control, 'SHOW data_directory'), str(RUNTIME / 'data'))
        check('automatic flush initially disabled', scalar(control, 'SHOW enable_lake_maintenance'), 'off')
        check('no unrelated databases can be modified by the scheduler',
              query(control, 'SELECT datname FROM pg_database WHERE NOT datistemplate ORDER BY datname'), [('postgres',)])
        own = scalar(control, 'SELECT pg_backend_pid()')
        check('no unrelated frontend sessions at restart', query(control,
              'SELECT pid FROM pg_stat_activity WHERE client_port IS NOT NULL AND pid<>%s', (own,)), [])
        control.close(); control = None
        original = conf.read_bytes(); mode = conf.stat().st_mode & 0o777
        (out / 'original-postgresql.conf').write_bytes(original)
        minimum, delay = (3, 60000) if a.mode in ('rows', 'cancel', 'fairness', 'shutdown', 'cross_database', 'budget') else (100000, 1500)
        worker_budget=1500 if a.mode=='budget' else 5000
        installed = original + ('\n# owned automatic flush test ' + a.run_id + '\n' +
            'enable_lake_maintenance = on\nlake_flush_naptime = 100\n' +
            'lake_flush_min_rows = ' + str(minimum) + '\nlake_flush_max_delay = ' + str(delay) +
            '\nlake_flush_worker_budget = '+str(worker_budget)+'\nlake_flush_max_batches = 1\n').encode()
        atomic_bytes(conf, installed, mode); changed = True
        restart('enable')
        control = connect_control()
        check('automatic flush enabled after native restart', scalar(control, 'SHOW enable_lake_maintenance'), 'on')
        db = OwnedDatabase(a.run_id, out).create()
        roles = OwnedRoles(control, a.run_id, out)
        t = Case(db, SimpleNamespace(mode='net_dml', sql_version='0.11', manifest=out / 'fixture/manifest.json',
                                     output=out / 'result.json', run_id=a.run_id), result, roles)
        check('owned session obtains maintenance claim', scalar(t.a, 'SELECT og_lake_internal.test_maintenance_claim()'))
        t.create()
        carrier = scalar(t.a, 'SELECT table_name FROM created_metadata.ducklake_inlined_data_tables')
        raw_sql = sql.SQL('SELECT _ducklake_row_id,_ducklake_begin_snapshot,id,val FROM created_metadata.{} ORDER BY 1,2').format(sql.Identifier(carrier))
        query(t.a, 'INSERT INTO biz.orders VALUES(1,10),(2,20),(3,30)')
        initial = query(t.observer, raw_sql)
        check('all three committed rows initially inline', len(initial), 3)
        generation = auto_state()['generation']
        wait_for('worker tried while a different session held maintenance claim', lambda s: s['generation'] >= generation + 3)
        check('claim blocks automatic publication across commits', scalar(t.observer, 'SELECT count(*) FROM created_metadata.ducklake_data_file'), 0)
        check('claim preserves exact inline versions', query(t.observer, raw_sql), initial)
        check('launcher exists', auto_state()['launcher_pid'] > 0)
        check('release only the owned maintenance claim', scalar(t.a, 'SELECT og_lake_internal.test_maintenance_release()'))
        started = time.monotonic()
        state = wait_for('actual automatic data file publication', lambda s:
            scalar(t.observer, 'SELECT count(*) FROM created_metadata.ducklake_data_file') == 1)
        result['automatic_publication_seconds'] = time.monotonic() - started
        check('automatic data flush leaves exact business rows', t.rows(), [(1,10),(2,20),(3,30)])
        check('automatic data flush consumes the transferred inline versions', query(t.observer, raw_sql), [])
        wait_for('worker releases session claim after its committed batch',
                 lambda state: scalar(t.a, 'SELECT og_lake_internal.test_maintenance_claim()'), seconds=5)
        check('owned session can claim maintenance again', scalar(t.a, 'SELECT og_lake_internal.test_maintenance_claim()'))
        check('worker counted a successful committed batch after claim release', auto_state()['completed_batches'] > 0)
        # Freeze subsequent maintenance only for exact output inspection.
        file_rows = query(t.observer, 'SELECT path,record_count FROM created_metadata.ducklake_data_file')
        root = Path(t.empty_manifest['data_path'])
        paths = list(root.rglob(file_rows[0][0]))
        check('automatic output is one actual immutable Parquet object', len(paths), 1)
        check('registered physical output row count exact', file_rows[0][1], 3)
        receipt = json.loads((ROOT / '.reference/opengauss-native-lake/build/receipt.json').read_text())
        check('independent reference lock current', receipt['source_lock_hash'], sha256(D / 'source-lock.json'))
        ref = Reference(out, json.loads((D / 'source-lock.json').read_text()), receipt)
        observed = ref.run('SELECT _ducklake_internal_row_id,_ducklake_internal_snapshot_id,id,val FROM read_parquet(' +
                           literal(str(paths[0])) + ') ORDER BY 1,2', 'automatic-physical-rows', result=True)
        check('automatic flush preserves permanent row IDs, births and complete values',
              [tuple(x.values()) for x in observed], initial)
        if a.mode == 'budget':
            names=['budget_'+str(i)for i in range(3)]
            for name in names:
                t.create(name)
                query(t.a,sql.SQL('INSERT INTO {} VALUES(1,10),(2,20),(3,30)').format(t.relation(name)))
            initial_protocol=t.protocol(); initial_disk=sorted(str(x)for x in root.rglob('*.parquet'))
            query(t.b,'BEGIN'); query(t.b,'LOCK TABLE created_metadata.ducklake_table_stats IN SHARE MODE')
            failed_before=auto_state()['failed_batches']
            check('configured whole-worker budget is1500ms',scalar(t.observer,"SELECT setting::integer FROM pg_settings WHERE name='lake_flush_worker_budget'"),1500)
            check('release claimed work for actual resource-budget test',scalar(t.a,'SELECT og_lake_internal.test_maintenance_release()'))
            deadline=time.monotonic()+15; blocked=[]
            while not blocked:
                blocked=query(t.observer,"SELECT pid FROM pg_locks WHERE relation='created_metadata.ducklake_table_stats'::regclass "
                              "AND mode='RowExclusiveLock' AND NOT granted")
                if time.monotonic()>deadline: raise TimeoutError('Budget worker never reached real metadata lock barrier')
                if not blocked: time.sleep(.005)
            budget_generation=auto_state()['generation']
            result['measured_worker_generation']=budget_generation
            started_wait=time.monotonic()
            wait_for('budgeted worker leaves after bounded failed batches',
                     lambda state:state['failed_batches']>=failed_before+2 and
                         (state['worker_pid']==0 or state['generation']!=budget_generation),seconds=10)
            elapsed=time.monotonic()-started_wait
            result['whole_worker_budget_seconds']=worker_budget/1000.; result['elapsed_from_first_actual_spi_wait']=elapsed
            query(t.b,'ROLLBACK')
            check('budget exhaustion preserves all original committed catalog state',t.protocol(),initial_protocol)
            check('budget exhaustion removes unpublished candidate files',sorted(str(x)for x in root.rglob('*.parquet')),initial_disk)
            for name in names:check('budget exhaustion preserves pending business rows '+name,t.rows(name=name),[(1,10),(2,20),(3,30)])
            check('whole worker respects1500ms budget with350ms test tolerance',elapsed<=worker_budget/1000.+.35)
        if a.mode == 'cross_database':
            cold_id=a.run_id+'_cold'; cold_out=out/'cold-database'; cold_out.mkdir()
            with (cold_out/'fixture.log').open('w') as log:
                subprocess.run([sys.executable,str(D/'tests/catalog_fixture_bundle.py'),'--run-id',cold_id,
                                '--output',str(cold_out/'fixture/manifest.json')],stdout=log,stderr=subprocess.STDOUT,check=True,timeout=300)
            cold_db=OwnedDatabase(cold_id,cold_out).create()
            extra={'database':cold_db,'roles':None,'run_id':cold_id,'output':cold_out}
            extra_databases.append(extra)
            cold_roles=OwnedRoles(control,cold_id,cold_out); extra['roles']=cold_roles
            cold_result={'assertions':[]}; result['cold_database_setup']=cold_result
            cold=Case(cold_db,SimpleNamespace(mode='net_dml',sql_version='0.11',manifest=cold_out/'fixture/manifest.json',
                      output=cold_out/'result.json',run_id=cold_id),cold_result,cold_roles)
            check('second database has an independent native OID',cold_db.allocation['database_oid']!=db.allocation['database_oid'])
            check('second catalog has a separate genuine storage root',cold.empty_manifest['data_path']!=t.empty_manifest['data_path'])
            check('claim cold database independently',scalar(cold.a,'SELECT og_lake_internal.test_maintenance_claim()'))
            cold.create(); query(cold.a,'INSERT INTO biz.orders VALUES(101,1010),(102,1020),(103,1030)')
            cold_carrier=scalar(cold.observer,'SELECT table_name FROM created_metadata.ducklake_inlined_data_tables')
            cold_raw_sql=sql.SQL('SELECT _ducklake_row_id,_ducklake_begin_snapshot,id,val FROM created_metadata.{} ORDER BY 1,2').format(sql.Identifier(cold_carrier))
            cold_initial=query(cold.observer,cold_raw_sql)
            t.create('hot'); query(t.a,'INSERT INTO biz.hot VALUES(1,10),(2,20),(3,30)')
            writer=db.connect(); writer_pid=scalar(writer,'SELECT pg_backend_pid()')
            stop=threading.Event(); ready=threading.Event(); committed=[]; retries=[]
            writer_started=time.monotonic(); result['hot_writer_pid']=writer_pid; result['hot_progress_samples']=[]
            def keep_hot_database_busy():
                import psycopg2
                identifier=1000; deadline=time.monotonic()+90
                while not stop.is_set() and time.monotonic()<deadline:
                    for attempt in range(20):
                        try:
                            query(writer,'INSERT INTO biz.hot VALUES(%s,%s)',(identifier,identifier*10))
                            committed.append(identifier); identifier+=1;
                            if len(committed)>=20: ready.set()
                            break
                        except psycopg2.Error as exc:
                            if exc.pgcode!='40001': raise
                            retries.append(exc.pgcode)
                            if attempt==19: raise
                            time.sleep(.005)
                    stop.wait(.01)
            executor=ThreadPoolExecutor(max_workers=1); future=executor.submit(keep_hot_database_busy)
            maximum_workers=[0]
            try:
                check('hot database writer committed while maintenance was claimed',ready.wait(20))
                check('release hot database maintenance claim',scalar(t.a,'SELECT og_lake_internal.test_maintenance_release()'))
                check('release cold database maintenance claim',scalar(cold.a,'SELECT og_lake_internal.test_maintenance_release()'))
                def cold_flushed_while_hot_is_busy(state):
                    active=scalar(control,"SELECT count(*) FROM pg_stat_activity WHERE application_name='og_lake flush worker'")
                    maximum_workers[0]=max(maximum_workers[0],active)
                    if active>1: raise AssertionError('Duplicate native workers across databases')
                    if future.done(): future.result(); raise AssertionError('Hot writer ended before cold database flush')
                    return scalar(cold.observer,'SELECT count(*) FROM created_metadata.ducklake_data_file')==1
                wait_for('cold database progresses under continuing hot-database commits',cold_flushed_while_hot_is_busy,seconds=20)
                at_publication=len(committed)
                result['hot_progress_observation_window_seconds']=20
                result['performance_scope']='Debug liveness under15s statement timeout plus5s observation allowance; original20commits/5s throughput gate remains unqualified'
                result['hot_commits_at_cold_publication']=at_publication
                def hot_progress_after_cold(state):
                    status=query(t.observer,'SELECT state,waiting,query FROM pg_stat_activity WHERE pid=%s',(writer_pid,))
                    locks=query(t.observer,'SELECT locktype,relation::regclass::text,mode,granted,transactionid::text '
                                'FROM pg_locks WHERE pid=%s ORDER BY locktype,relation,mode',(writer_pid,))
                    result['hot_progress_samples'].append({'seconds':time.monotonic()-writer_started,
                        'commits':len(committed),'serialization_retries':len(retries),'writer_status':status,
                        'writer_locks':locks,'automatic':state})
                    if future.done(): future.result(); raise AssertionError('Hot writer stopped before requested continued commits')
                    return len(committed)>=at_publication+20
                wait_for('hot writer continues committing after cold publication',hot_progress_after_cold,seconds=20)
                check('cold database row values exact',cold.rows(),[(101,1010),(102,1020),(103,1030)])
                check('cold database transferred its complete inline input',query(cold.observer,cold_raw_sql),[])
                result['hot_commits_at_cold_publication']=at_publication
            finally:
                stop.set(); executor.shutdown(wait=True)
                result['confirmed_hot_inserts']=list(committed); result['hot_serialization_retries']=list(retries)
                try: result['hot_rows_after_producer_stop']=t.rows(name='hot')
                except BaseException as diagnostic: result['hot_rows_diagnostic_error']=str(diagnostic)
                future.result()
            wait_for('hot worker releases claim for exact final validation',lambda state:scalar(t.a,'SELECT og_lake_internal.test_maintenance_claim()'),seconds=10)
            wait_for('cold worker releases claim for exact final validation',lambda state:scalar(cold.a,'SELECT og_lake_internal.test_maintenance_claim()'),seconds=10)
            check('all acknowledged hot writes remain exactly once',t.rows(name='hot'),[(1,10),(2,20),(3,30)]+[(i,i*10)for i in committed])
            check('at most one real maintenance worker across both databases',maximum_workers[0]<=1)
            cold_root=Path(cold.empty_manifest['data_path']); cold_path=scalar(cold.observer,'SELECT path FROM created_metadata.ducklake_data_file')
            cold_files=list(cold_root.rglob(cold_path)); check('cold output exists in its own storage root',len(cold_files),1)
            physical=ref.run('SELECT _ducklake_internal_row_id,_ducklake_internal_snapshot_id,id,val FROM read_parquet('+literal(str(cold_files[0]))+') ORDER BY 1,2','cold-physical-rows',result=True)
            check('cold automatic flush preserves original permanent identities and births',[tuple(x.values())for x in physical],cold_initial)
            result['confirmed_hot_inserts']=committed; result['hot_serialization_retries']=retries
            result['maximum_observed_active_workers']=maximum_workers[0]
        if a.mode == 'fairness':
            names = ['queued_' + str(i) for i in range(4)]
            for name in names:
                t.create(name)
                query(t.a, sql.SQL('INSERT INTO {} VALUES(1,10),(2,20),(3,30)').format(t.relation(name)))
            check('queued tables cannot bypass another session claim',
                  scalar(t.observer, 'SELECT count(*) FROM created_metadata.ducklake_data_file'), 1)
            previous_launcher = auto_state()['launcher_pid']
            postmaster = int((RUNTIME / 'data/postmaster.pid').read_text().splitlines()[0])
            def launcher_os_threads():
                found = []
                for task in (Path('/proc') / str(postmaster) / 'task').iterdir():
                    try: name = (task / 'comm').read_text().strip()
                    except FileNotFoundError: continue
                    if name == 'lakelauncher': found.append(int(task.name))
                return sorted(found)
            previous_os_threads = launcher_os_threads()
            check('one actual native launcher OS thread before termination', len(previous_os_threads), 1)
            result['launcher_before'] = {'postmaster_pid':postmaster,'native_thread_id':previous_launcher,
                                         'linux_thread_ids':previous_os_threads}
            check('terminate only the dedicated owned native launcher',
                  scalar(t.observer, 'SELECT pg_terminate_backend(%s)', (previous_launcher,)))
            replacement = wait_for('postmaster restarts terminated native launcher',
                                   lambda state: state['launcher_pid'] > 0 and len(launcher_os_threads()) == 1 and
                                       launcher_os_threads() != previous_os_threads, seconds=20)
            result['launcher_after'] = {'postmaster_pid':int((RUNTIME / 'data/postmaster.pid').read_text().splitlines()[0]),
                                        'native_thread_id':replacement['launcher_pid'],'linux_thread_ids':launcher_os_threads()}
            check('launcher replacement keeps the original postmaster alive', result['launcher_after']['postmaster_pid'], postmaster)
            check('launcher replacement retains committed lake rows', t.rows(), [(1,10),(2,20),(3,30)])
            check('replacement launcher still respects maintenance claim',
                  scalar(t.observer, 'SELECT count(*) FROM created_metadata.ducklake_data_file'), 1)
            baseline_batches = replacement['completed_batches']
            check('release queued bounded tasks', scalar(t.a, 'SELECT og_lake_internal.test_maintenance_release()'))
            maximum_workers = [0]
            def all_queued_tables_flushed(state):
                active = scalar(t.observer, "SELECT count(*) FROM pg_stat_activity WHERE application_name='og_lake flush worker'")
                maximum_workers[0] = max(maximum_workers[0], active)
                if active > 1: raise AssertionError('More than one real maintenance worker active')
                return scalar(t.observer, 'SELECT count(*) FROM created_metadata.ducklake_data_file') == 5
            wait_for('relation cursor drains four tables with max_batches one', all_queued_tables_flushed, seconds=20)
            wait_for('bounded workers release claim after queued publications',
                     lambda state: scalar(t.a, 'SELECT og_lake_internal.test_maintenance_claim()'), seconds=5)
            check('four independent committed batches were counted',
                  auto_state()['completed_batches'] >= baseline_batches + 4)
            check('actual pg_stat_activity never showed duplicate workers', maximum_workers[0] <= 1)
            for name in names:
                check('bounded fair automatic rows ' + name, t.rows(name=name), [(1,10),(2,20),(3,30)])
                registered = scalar(t.observer, 'SELECT i.table_name FROM created_metadata.ducklake_inlined_data_tables i '
                                    'JOIN created_metadata.ducklake_table t ON t.table_id=i.table_id WHERE t.table_name=%s', (name,))
                check('bounded fair automatic carrier empty ' + name,
                      scalar(t.observer, sql.SQL('SELECT count(*) FROM created_metadata.{}').format(sql.Identifier(registered))), 0)
            result['maximum_observed_active_workers'] = maximum_workers[0]
            check('each permanent allocation frontier unchanged by automatic queue draining',
                  query(t.observer, 'SELECT record_count,next_row_id FROM created_metadata.ducklake_table_stats ORDER BY table_id'),
                  [(3,3)] * 5)
        if a.mode in ('cancel', 'shutdown'):
            query(t.a, 'UPDATE biz.orders SET val=11 WHERE id=1')
            query(t.a, 'DELETE FROM biz.orders WHERE id=2')
            expected_rows = [(1,11),(3,30)]
            check('automatic cancellation case starts with file-origin DML', t.rows(), expected_rows)
            initial_protocol = t.protocol()
            initial_disk = sorted(str(x) for x in root.rglob('*.parquet'))
            failed_before = auto_state()['failed_batches']
            query(t.b, 'BEGIN')
            query(t.b, 'LOCK TABLE created_metadata.ducklake_table_stats IN SHARE MODE')
            check('release claim for exact blocked automatic publisher', scalar(t.a, 'SELECT og_lake_internal.test_maintenance_release()'))
            deadline = time.monotonic() + 15
            blocked = []
            while not blocked:
                blocked = query(t.observer, "SELECT pid FROM pg_locks WHERE relation='created_metadata.ducklake_table_stats'::regclass "
                                "AND mode='RowExclusiveLock' AND NOT granted")
                if time.monotonic() > deadline: raise TimeoutError('Actual automatic publisher did not reach catalog barrier')
                if not blocked: time.sleep(.01)
            check('one automatic publisher reached final metadata barrier', len(blocked), 1)
            check('blocked publisher is the actual registered worker', blocked[0][0], auto_state()['worker_pid'])
            check('uncommitted automatic publication invisible', t.protocol(), initial_protocol)
            if a.mode == 'shutdown':
                result['shutdown_expected_protocol'] = initial_protocol
                result['shutdown_expected_rows'] = expected_rows
                result['shutdown_expected_disk'] = initial_disk
                stop_run = a.run_id + '_stop_in_spi'
                with (out / 'stop-in-spi.log').open('w') as log:
                    subprocess.run(['bash',str(D/'build.sh'),PROFILE,'stop','--run-id',stop_run],
                                   stdout=log,stderr=subprocess.STDOUT,check=True,timeout=120)
                stop_receipt = ROOT/'公共文档/openGauss原生湖存储/验证'/stop_run/'build.json'
                stopped = json.loads(stop_receipt.read_text())
                check('native fast shutdown completes while worker holds SPI error scope', stopped['status'], 'stopped')
                result['shutdown_receipt'] = str(stop_receipt)
                result['shutdown_receipt_sha256'] = sha256(stop_receipt)
                check('stopped process keeps exact product binary', stopped['source_head'], result['source_head'])
            else:
                check('cancel exact owned automatic worker', scalar(t.observer, 'SELECT pg_cancel_backend(%s)', (blocked[0][0],)))
                wait_for('cancelled automatic worker exited and recorded error',
                         lambda state: state['failed_batches'] > failed_before and state['worker_pid'] == 0)
                query(t.b, 'ROLLBACK')
                check('cancelled automatic publication fully rolls back catalog', t.protocol(), initial_protocol)
                check('cancelled automatic worker discards completed candidates', sorted(str(x) for x in root.rglob('*.parquet')), initial_disk)
                check('cancelled automatic flush retains committed user updates', t.rows(), expected_rows)
                query(t.a, 'INSERT INTO biz.orders VALUES(4,40)')
                wait_for('fresh committed work retries failed automatic maintenance', lambda state:
                    scalar(t.observer, 'SELECT count(*) FROM created_metadata.ducklake_data_file') == 2 and
                    scalar(t.observer, 'SELECT count(*) FROM created_metadata.ducklake_delete_file') == 1, seconds=40)
                wait_for('retry worker releases its session claim after commit',
                         lambda state: scalar(t.a, 'SELECT og_lake_internal.test_maintenance_claim()'), seconds=5)
                check('claim maintenance after successful automatic retry', scalar(t.a, 'SELECT og_lake_internal.test_maintenance_claim()'))
                check('automatic retry preserves each final business version exactly once', t.rows(), [(1,11),(3,30),(4,40)])
                check('automatic retry transferred pending inline data', query(t.observer, raw_sql), [])
                tid = scalar(t.observer, 'SELECT table_id FROM created_metadata.ducklake_table')
                marks = sql.SQL('SELECT count(*) FROM created_metadata.{}').format(sql.Identifier('ducklake_inlined_delete_' + str(tid)))
                check('automatic retry transferred file-origin deletion marks', scalar(t.observer, marks), 0)
        check('every frozen executed input unchanged', all(sha256(p) == h for p,h in result['inputs'].items()))
        check_build(a.build_receipt)
        result['status'] = 'pass'
    except BaseException as exc:
        result.update(reason=str(exc), traceback=traceback.format_exc())
    finally:
        if db is not None:
            for c in db.connections:
                try: c.close()
                except BaseException: pass
            db.connections = []
        for extra in extra_databases:
            for c in extra['database'].connections:
                try: c.close()
                except BaseException: pass
            extra['database'].connections=[]
        if control is not None:
            control.close(); control = None
        if changed:
            try:
                if conf.read_bytes() != installed:
                    raise RuntimeError('Configuration changed externally; original bytes retained and no overwrite attempted')
                atomic_bytes(conf, original, mode)
                restart('restore'); restored = True
                check('configuration restored byte for byte', sha256(conf), hashlib.sha256(original).hexdigest())
            except BaseException as exc: result.update(status='fail', restoration_error=str(exc))
        if db is not None:
            try:
                if changed and not restored: raise RuntimeError('Controlled restart restoration unproven; retain owned objects')
                control = connect_control()
                check('automatic flush disabled before owned cleanup', scalar(control, 'SHOW enable_lake_maintenance'), 'off')
                if a.mode == 'shutdown' and result.get('shutdown_receipt'):
                    recovered = db.connect()
                    check('FATAL shutdown preserves committed user updates after restart', t.rows(conn=recovered), result['shutdown_expected_rows'])
                    check('FATAL shutdown rolls back uncommitted automatic directory', t.protocol(conn=recovered), result['shutdown_expected_protocol'])
                    check('FATAL shutdown aborts all finished unpublished files', sorted(str(x) for x in root.rglob('*.parquet')), result['shutdown_expected_disk'])
                    recovered.close()
                result.update(cleanup_owned_after_restart(db,roles,a.run_id,out,control,result.get('restore_receipt')))
                if roles is not None: result=roles.redact(result)
            except BaseException as exc: result.update(status='fail', cleanup_error=str(exc), cleanup_traceback=traceback.format_exc())
        for extra in extra_databases:
            try:
                if changed and not restored: raise RuntimeError('Controlled restart restoration unproven; retain extra owned database')
                if control is None: control=connect_control()
                payload=cleanup_owned_after_restart(extra['database'],extra['roles'],extra['run_id'],extra['output'],control,result.get('restore_receipt'))
                result.setdefault('extra_database_cleanup',[]).append(payload)
                if extra['roles'] is not None: result=extra['roles'].redact(result)
            except BaseException as exc: result.update(status='fail',extra_cleanup_error=str(exc),extra_cleanup_traceback=traceback.format_exc())
        if control is not None: control.close()
        # Do not serialize raw configuration bytes in the assertion list.
        write_json(out / 'result.json', result)
    print(json.dumps({'status': result['status'], 'checks': len(result['assertions']),
                      'reason': result.get('reason'), 'restoration_error': result.get('restoration_error'),
                      'result': str(out / 'result.json')}, ensure_ascii=False))
    return 0 if result['status'] == 'pass' else 1

if __name__ == '__main__': raise SystemExit(main())
