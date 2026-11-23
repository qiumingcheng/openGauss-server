#!/usr/bin/env python3
"""Prepared SQL19 auto-ANALYZE tests; requires a future official Debug receipt.

Uses existing real EMPTY fixture, exact owned database/roles and native worker.
Does not configure/restart the instance. Root must run cases serially with the
launcher+autoanalyze enabled, index maintenance off and a30s analysis budget.
No test has been executed by preparing/AST-checking this script.
"""
from copy import deepcopy
import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import secrets
import shutil
import subprocess
import sys
import time
import traceback
from types import SimpleNamespace
from psycopg2 import sql

ROOT = Path(os.getenv('CPP_WORKSPACE', '/home/qiumc/data/code/cpp')).resolve()
DRIVER = ROOT / 'integration/opengauss-native-lake'
sys.path[:0] = [str(DRIVER), str(DRIVER / 'tests')]
from file_dml_acceptance import check_build
from inline_fixture import query, scalar
from lake_harness import sha256, validate_run_id, write_json
from empty_manifest import validate_empty_bundle, validate_empty_manifest
from native_auto_flush import connect_control
from native_runtime import PROFILE,INSTALL,SOURCE,RUNTIME
from native_test_context import OwnedDatabase, reviewed_runtime_version
from permission_api import OwnedRoles, cleanup_roles


def image(value):
    if isinstance(value, bytes):
        return dict(encoding='base64', value=base64.b64encode(value).decode('ascii'), bytes=len(value))
    if isinstance(value, (tuple, list)):
        return [image(x) for x in value]
    if isinstance(value, dict):
        return {k: image(v) for k, v in value.items()}
    return value


def decode_sqlstate(code):
    # Same five PGSIXBIT fields as native elog.h, not an inferred error class.
    return None if code is None else ''.join(chr(((int(code)>>(6*i))&0x3f)+ord('0'))for i in range(5))


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--run-id', required=True)
    p.add_argument('--build-receipt', required=True, type=Path)
    p.add_argument('--manifest',type=Path,help='Optional exact fresh EMPTY/EMPTY_BUNDLE; omitted generates one genuine owned EMPTY for this case')
    p.add_argument('--campaign-frozen',type=Path,help='Read-only frozen campaign identity/archive; controller owns no DB cleanup')
    p.add_argument('--mode', required=True, choices=['counters', 'automatic', 'mvcc', 'cancel', 'refusal', 'upgrade', 'legacy', 'lifecycle'])
    p.add_argument('--budget-seconds', type=int, default=90)
    p.add_argument('--sql-version',choices=['0.19','0.20','0.21'],default='0.19',help='Positive/upgrade target; legacy remains installed18')
    a = p.parse_args()
    validate_run_id(a.run_id)
    if PROFILE != 'debug' or not 30 <= a.budget_seconds <= 180:
        p.error('Only exact Debug with bounded observation30..180 seconds')
    out = ROOT / '公共文档/openGauss原生湖存储/验证' / a.run_id
    out.mkdir(exist_ok=False)
    r = dict(status='fail', scope=__doc__, mode=a.mode, assertions=[], executed=False,
             limitations=['No Native execution by AST preparation.', 'Single-node RC; real RAC and performance remain pending.',
                          'Actual real-sampler pause tests pre-publication cancellation, not arbitrary late pg_class inplace rollback.',
                          'Does not test every contract.json case; file budget/readonly/fairness/DDL/error-cleanup need additional qualification.'])
    if a.manifest is None:
        a.manifest=out/'fixture/manifest.json'
        data_root=(a.manifest.parent/'empty-reference-generation/data').resolve()
        command=[sys.executable,str(DRIVER/'tests/empty_reference_fixture.py'),'--run-id',a.run_id,
                 '--output',str(a.manifest),'--data-root',str(data_root)]
        r['fixture_generation']=dict(command=command,independent_owned_data_root=str(data_root))
        try:
            check_build(a.build_receipt)
            with(out/'fixture-generator.log').open('w')as log:
                subprocess.run(command,stdout=log,stderr=subprocess.STDOUT,timeout=300,check=True)
        except BaseException as exc:
            r.update(reason=str(exc),exception_type=type(exc).__name__,sqlstate=getattr(exc,'pgcode',None),
                     fixture_generation_failed=True,traceback=traceback.format_exc())
            write_json(out/'result.json',r);return 1
    files = [Path(__file__), a.build_receipt, a.manifest,
             DRIVER/'tests/file_dml_acceptance.py', DRIVER/'inline_fixture.py', DRIVER/'lake_harness.py',
             DRIVER/'native_test_context.py', DRIVER/'native_runtime.py', DRIVER/'tests/native_auto_flush.py',
             DRIVER/'tests/permission_api.py', DRIVER/'tests/inline_cleanup.py', DRIVER/'tests/empty_manifest.py',
             DRIVER/'tests/empty_reference_fixture.py', DRIVER/'tests/empty_catalog_oracle.py', DRIVER/'source-lock.json']
    if a.campaign_frozen:files.append(a.campaign_frozen)
    r['inputs'] = {str(f): sha256(f) for f in files}
    archive = out/'executed-inputs'
    archive.mkdir()
    r['archived_inputs']={}
    for i,f in enumerate(files):
        archived=archive/(str(i)+'-'+f.name);shutil.copy2(f,archived)
        r['archived_inputs'][str(Path(f).resolve())]=dict(sha256=sha256(f),archive=str(archived.resolve()))
    control = database = roles = admin = observer = writer = gate = reservation = None
    claimed = gate_held = probe_ready = claim_ready = False
    refusal_setting = False
    cleanup_safe=True
    key = secrets.randbits(60) + 1
    target = None
    def check(label, observed, expected=True):
        ok = observed == expected
        r['assertions'].append(dict(name=label, status='pass' if ok else 'fail',
                                    observed=deepcopy(image(observed)), expected=deepcopy(image(expected))))
        write_json(out/'result.json', r)
        if not ok:
            raise AssertionError(label)
    def wait(label, predicate, seconds=None):
        end = time.monotonic() + (seconds or a.budget_seconds)
        last = None
        while time.monotonic() < end:
            last = predicate()
            if last:
                return last
            time.sleep(0.1)
        r.setdefault('wait_failures', []).append(dict(name=label, last=image(last)))
        raise TimeoutError(label)
    def claim():
        nonlocal claimed
        wait('exact DB native maintenance claim', lambda: scalar(reservation, 'SELECT og_lake_internal.test_maintenance_claim()'))
        claimed = True
    def release_claim():
        nonlocal claimed
        check('release exact DB native maintenance claim', scalar(reservation, 'SELECT og_lake_internal.test_maintenance_release()'))
        claimed = False
    def state():
        rows=query(observer, 'SELECT s.committed_changes,s.analyzed_changes,s.analyzed_snapshot, '
                   's.last_success IS NOT NULL,s.retry_after IS NOT NULL,s.last_error FROM og_lake_internal.analyze_state s '
                   'JOIN og_lake_internal.relation_binding r USING(binding_id) WHERE r.relation_oid=%s', (target,))
        return rows[0] if rows else None
    def stat_image():
        return query(observer, 'SELECT s::text FROM pg_statistic s WHERE starelid=%s ORDER BY staattnum', (target,)), \
               query(observer, 'SELECT reltuples,relpages,relallvisible FROM pg_class WHERE oid=%s', (target,))
    def gate_state(phase=0):
        return json.loads(scalar(admin, 'SELECT biz.autoanalyze_gate(%s::oid,%s::bigint,%s::integer)', (target,key,phase)))
    try:
        build=check_build(a.build_receipt)
        check('official map contains automatic ANALYZE CPP', 'contrib/og_lake/lake_autoanalyze.cpp' in build['native_source_inputs_after'])
        check('official map contains exact new migration', 'contrib/og_lake/og_lake--0.18--0.19.sql' in build['native_source_inputs_after'])
        r.update(binary_sha256=build['binary_sha256'],source_head=build['source_head'])
        control=connect_control()
        for s in ['SET session_timeout=0','SET statement_timeout=30000']:
            query(control,s)
        check('existing launcher enabled',scalar(control,'SHOW enable_lake_maintenance'),'on')
        check('automatic analysis explicitly enabled',scalar(control,'SHOW enable_lake_autoanalyze'),'on')
        check('long index work disabled for this scoped test',scalar(control,'SHOW enable_lake_index_maintenance'),'off')
        check('worker budget explicitly30s',scalar(control,"SELECT setting::int FROM pg_settings WHERE name='lake_autoanalyze_budget'"),30000)
        # Bind every fixture to this leaf's exact run ID. A real EMPTY catalog
        # remains immutable after writes, so its data root must independently
        # still be absent here; an old successful manifest is not a fresh root.
        fixture=json.loads(a.manifest.read_text());ctx=SimpleNamespace(run_id=a.run_id)
        if fixture.get('case')=='EMPTY_BUNDLE':
            bundle=validate_empty_bundle(a.manifest,ctx)
            empty_path=Path(bundle['entries']['same']['manifest'])
            empty,_=validate_empty_manifest(empty_path,SimpleNamespace(run_id=a.run_id+'-same'));fixture_owner=Path(bundle['cleanup_owner']).resolve()
        else:
            empty_path=a.manifest;empty,_=validate_empty_manifest(empty_path,ctx);fixture_owner=Path(empty['cleanup_owner']).resolve()
        data_root=Path(empty['data_path']).resolve()
        check('fresh owned EMPTY data root was not materialized by any previous case',not data_root.exists()and not Path(empty['data_path']).is_symlink())
        check('data root stays within exact fixture cleanup owner',data_root.is_relative_to(fixture_owner))
        r['fixture_identity']=dict(manifest=str(a.manifest.resolve()),manifest_sha256=sha256(a.manifest),
            empty_manifest=str(empty_path.resolve()),empty_sha256=sha256(empty_path),run_id=empty['run_id'],data_root=str(data_root),cleanup_owner=str(fixture_owner),empty_cleanup_owner=empty['cleanup_owner'])
        extra=[empty_path]+[Path(x['path'])for x in empty['files']]
        for i,f in enumerate(extra):
            archive_key=str(f.resolve())
            if archive_key in r['archived_inputs']:continue
            archived=archive/('empty-'+str(i)+'-'+f.name);shutil.copy2(f,archived)
            r['inputs'][archive_key]=sha256(f);r['archived_inputs'][archive_key]=dict(sha256=sha256(f),archive=str(archived.resolve()))
        if a.campaign_frozen:
            frozen=json.loads(a.campaign_frozen.read_text())
            check('campaign frozen source/binary matches actual build',frozen['binary_sha256'],build['binary_sha256'])
            check('campaign frozen Native source identity',frozen['source_inputs'],build['native_source_inputs_after'])
            for path,item in frozen['files'].items():
                check('campaign frozen original and archive/'+Path(path).name,(sha256(path),sha256(item['archive'])),(item['sha256'],item['sha256']))
            r['campaign_frozen']=dict(path=str(a.campaign_frozen.resolve()),sha256=sha256(a.campaign_frozen))
        database=OwnedDatabase(a.run_id,out)
        database.create()
        roles=OwnedRoles(control,a.run_id,out)
        admin,observer,writer,gate,reservation=[database.connect() for _ in range(5)]
        for c in [admin,observer,writer,gate,reservation]:
            for s in ['SET session_timeout=0','SET statement_timeout=30000','SET query_dop=1','SET default_statistics_target=100']:
                query(c,s)
        r['postmaster_start']=database.allocation['postmaster_start']
        version='0.18' if a.mode in ('upgrade','legacy') else a.sql_version
        query(admin,"CREATE EXTENSION og_lake VERSION '"+version+"'")
        r['initial_installed_sql']=scalar(admin,"SELECT extversion FROM pg_extension WHERE extname='og_lake'")
        check('exact initial installed version',r['initial_installed_sql'],version)
        r['runtime_module_version']=json.loads(scalar(admin,'SELECT og_lake.host_status()'))['module_version']
        check('automatic-analysis runtime matches exact official source control',r['runtime_module_version'],reviewed_runtime_version())
        owner=roles.create('private',False)
        query(admin,'SELECT og_lake.configure_internal_owner(%s::name)',(owner,))
        claim_ready=True;claim()
        query(admin,'SELECT og_lake.create_catalog(%s::name,%s::name,%s::text,NULL::text)',
              ('created_lake','created_metadata',empty['data_path']))
        query(admin,'CREATE SCHEMA biz')
        query(admin,"CREATE FOREIGN TABLE biz.orders(id integer,val integer) SERVER created_lake OPTIONS(binding_mode 'create',lake_schema 'main')")
        target=scalar(admin,"SELECT 'biz.orders'::regclass::oid")
        query(admin,'INSERT INTO biz.orders SELECT i,i FROM generate_series(1,100)i')
        r['executed']=True
        if version=='0.18':
            check('original18 has no analyze_state',scalar(admin,"SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='og_lake_internal' AND c.relname='analyze_state'"),0)
            if a.mode=='legacy':
                release_claim();time.sleep(2)
                check('old SQL relation remains unanalyzed',scalar(observer,'SELECT relpages FROM pg_class WHERE oid=%s',(target,)),0)
                claim()
            else:
                query(admin,"ALTER EXTENSION og_lake UPDATE TO '"+a.sql_version+"'")
                check('explicit migration installed selected target',scalar(admin,"SELECT extversion FROM pg_extension WHERE extname='og_lake'"),a.sql_version)
                check('private analyze state ownership transferred',scalar(admin,"SELECT c.relowner=r.oid FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace JOIN pg_roles r ON r.rolname=%s WHERE n.nspname='og_lake_internal' AND c.relname='analyze_state'",(owner,)),True)
                query(admin,'UPDATE biz.orders SET val=val+1 WHERE id=1')
                check('post-migration counter starts1; old data requires initial analysis',state()[:2],(1,0))
                release_claim();wait('upgrade initial analysis complete',lambda: state() and state()[3]);claim()
                check('initial analyze includes all100 preexisting rows',scalar(observer,'SELECT reltuples FROM pg_class WHERE oid=%s',(target,)),100)
        elif a.mode=='lifecycle':
            original=state()
            query(admin,'BEGIN');query(admin,'DROP FOREIGN TABLE biz.orders')
            check('own DDL transaction removes only exact analysis state',scalar(admin,'SELECT count(*) FROM og_lake_internal.analyze_state'),0)
            query(admin,'ROLLBACK')
            check('DDL rollback restores automatic state',state(),original)
            query(admin,"CREATE FOREIGN TABLE biz.extra(id integer,val integer) SERVER created_lake OPTIONS(binding_mode 'create',lake_schema 'main')")
            query(admin,'INSERT INTO biz.extra VALUES(1,2)')
            check('two exact business bindings have two scheduling states',scalar(observer,'SELECT count(*) FROM og_lake_internal.analyze_state'),2)
            query(admin,'DROP SERVER created_lake CASCADE')
            check('server deletion leaves no orphan host analysis states',scalar(observer,'SELECT count(*) FROM og_lake_internal.analyze_state'),0)
        elif a.mode=='counters':
            check('committed INSERT identities count100',state()[:2],(100,0))
            query(writer,'UPDATE biz.orders SET val=val+1000 WHERE id<=10')
            query(writer,'DELETE FROM biz.orders WHERE id BETWEEN 20 AND 24')
            check('update/delete changes despite net growth not positive',state()[:2],(115,0))
            query(writer,'BEGIN');query(writer,'UPDATE biz.orders SET val=0 WHERE id=1');query(writer,'ROLLBACK')
            check('top rollback count unchanged',state()[0],115)
            query(writer,'BEGIN');query(writer,'SAVEPOINT s');query(writer,'DELETE FROM biz.orders WHERE id=1');query(writer,'ROLLBACK TO s');query(writer,'COMMIT')
            check('savepoint rollback count unchanged',state()[0],115)
            query(writer,'BEGIN');query(writer,'UPDATE biz.orders SET val=7 WHERE id=1');query(writer,'UPDATE biz.orders SET val=8 WHERE id=1');query(writer,'COMMIT')
            check('same logical identity coalesced once per committed TX',state()[0],116)
            query(writer,'BEGIN');query(writer,'INSERT INTO biz.orders VALUES(1001,1)');query(writer,'DELETE FROM biz.orders WHERE id=1001');query(writer,'COMMIT')
            check('private insert-delete has no surviving change',state()[0],116)
            query(admin,'ANALYZE biz.orders')
            check('manual sample never claims automatic completion',state()[1:4],(0,None,False))
            before=state()[0];query(admin,"SELECT og_lake.flush('biz.orders'::regclass)")
            check('flush storage movement adds no logical change count',state()[0],before)
        else:
            query(admin,'ANALYZE biz.orders')
            check('manual ANALYZE leaves automatic watermark untouched',state()[1:4],(0,None,False))
            baseline=deepcopy(stat_image())
            if a.mode=='refusal':
                # Native lake column ALTER is intentionally closed. A normal
                # per-database GUC is loaded by each new real worker through
                # InitAutoVacWorker/InitSettings, without editing statistics.
                # Existing foreground sessions retain their explicit target100.
                query(admin,sql.SQL('ALTER DATABASE {} SET default_statistics_target=-1').format(sql.Identifier(database.database)))
                refusal_setting=True
                check('existing baseline session keeps target100',scalar(admin,'SHOW default_statistics_target'),'100')
                fresh=database.connect()
                try:check('new backend receives owned database percentage target',scalar(fresh,'SHOW default_statistics_target'),'-1')
                finally:fresh.close()
                r['refusal_scope']='database default percentage target; column SET STATISTICS is not qualified'
                release_claim();wait('unsupported default percentage recorded as failed attempt',lambda: state() and state()[5] is not None);claim()
                r['native_worker_sqlstate']=decode_sqlstate(state()[5])
                check('percentage refusal is exact Native0A000, not OOM/timeout',r['native_worker_sqlstate'],'0A000')
                check('failed native admission retains unadvanced coverage',state()[1:4],(0,None,False))
                check('failed native admission retains prior distributions',stat_image(),baseline)
            elif a.mode=='automatic':
                release_claim();wait('actual worker completes analysis',lambda: state() and state()[3]);claim()
                check('worker covers initial committed identities',state()[:2],(100,100))
                check('worker registered logical table scale',scalar(observer,'SELECT reltuples FROM pg_class WHERE oid=%s',(target,)),100)
            else:
                query(admin,"CREATE FUNCTION biz.autoanalyze_gate(oid,bigint,integer) RETURNS text AS '$libdir/og_lake','og_lake_test_autoanalyze_gate' LANGUAGE C VOLATILE NOT FENCED")
                query(admin,'REVOKE ALL ON FUNCTION biz.autoanalyze_gate(oid,bigint,integer) FROM PUBLIC');probe_ready=True
                query(gate,'SELECT pg_advisory_lock(%s::bigint)',(key,));gate_held=True
                gate_state(1);release_claim()
                def actual_gate():
                    s=gate_state()
                    if not s['hits'] or not s['live'] or not s['worker_pid']:return None
                    pid=int(s['worker_pid']);dbid=database.allocation['database_oid']
                    locks=query(observer,"SELECT pid,granted FROM pg_locks WHERE locktype='advisory' AND database=%s AND classid=%s AND objid=%s AND objsubid=1 AND mode='ExclusiveLock'",(dbid,key>>32,key&0xffffffff))
                    if (pid,False) not in locks:return None
                    identity=query(observer,'SELECT datid,pid FROM pg_stat_activity WHERE datid=%s AND pid=%s',(dbid,pid))
                    if identity!=[(dbid,pid)]:return None
                    r['real_sample_gate']=deepcopy(dict(probe=s,locks=locks,identity=identity))
                    return s
                s=wait('actual worker read sample under captured snapshot then blocks before stats writes',actual_gate)
                check('captured exact committed input counter',s['covered'],100)
                check('paused worker has not published any distribution',stat_image(),baseline)
                if a.mode=='mvcc':
                    query(writer,'UPDATE biz.orders SET val=999 WHERE id=1')
                    check('concurrent DML is not blocked by sampling state row',state()[:2],(101,0))
                    query(gate,'SELECT pg_advisory_unlock(%s::bigint)',(key,));gate_held=False
                    wait('old snapshot analysis completes',lambda: state() and state()[3]);claim()
                    check('new committed update remains pending',state()[:2],(101,100))
                    bounds=scalar(observer,"SELECT histogram_bounds::text FROM pg_stats WHERE schemaname='biz' AND tablename='orders' AND attname='val'")
                    check('post-S value not mixed into old analysis',bounds is not None and '999' not in bounds)
                else:
                    check('cancel exact owned worker during actual held sample',scalar(observer,'SELECT pg_cancel_backend(%s)',(int(s['worker_pid']),)))
                    wait('original57014 retained by actual worker',lambda: gate_state().get('error_sqlstate')=='57014')
                    claim()
                    r['native_worker_sqlstate']=decode_sqlstate(state()[5])
                    check('persisted cancellation keeps exact original57014',r['native_worker_sqlstate'],'57014')
                    check('cancelled analysis preserves old distributions and scale',stat_image(),baseline)
                    check('cancelled analysis did not consume coverage',state()[1:4],(0,None,False))
        if a.mode!='lifecycle':
            check('exact100-row logical content preserved apart from explicit test DML',scalar(observer,'SELECT count(*) FROM biz.orders'),95 if a.mode=='counters' else 100)
        r['installed_sql']=scalar(admin,"SELECT extversion FROM pg_extension WHERE extname='og_lake'")
        check('final actual installed SQL',r['installed_sql'],'0.18'if a.mode=='legacy'else a.sql_version)
        check('executed input identities remain fixed',all(sha256(Path(f))==h for f,h in r['inputs'].items()))
        check_build(a.build_receipt);r['status']='pass'
    except BaseException as exc:
        r.update(reason=str(exc),sqlstate=getattr(exc,'pgcode',None),exception_type=type(exc).__name__,traceback=traceback.format_exc())
    finally:
        if admin is not None:
            try:query(admin,'ROLLBACK')
            except BaseException:pass
        if gate_held:
            try:
                query(gate,'SELECT pg_advisory_unlock(%s::bigint)',(key,));gate_held=False
                held=query(observer,"SELECT pid FROM pg_locks WHERE locktype='advisory' AND database=%s AND classid=%s AND objid=%s AND objsubid=1 AND mode='ExclusiveLock' AND granted AND pid=%s",(database.allocation['database_oid'],key>>32,key&0xffffffff,scalar(gate,'SELECT pg_backend_pid()')))
                check('exact owned diagnostic gate released before cleanup',held,[])
            except BaseException as exc:
                cleanup_safe=False;r.update(status='fail',gate_cleanup_error=str(exc),gate_cleanup_sqlstate=getattr(exc,'pgcode',None))
        if claim_ready and not claimed:
            try:claim()
            except BaseException as exc:
                cleanup_safe=False;r.update(status='fail',worker_retirement_error=str(exc),worker_retirement_sqlstate=getattr(exc,'pgcode',None))
        if probe_ready:
            try:
                # The session-level native DB claim excludes the active worker
                # for its whole prepare/sample/cleanup interval. Reacquire it
                # after releasing the gate before claiming retirement.
                if not claimed:raise RuntimeError('Native worker retirement unproven; gate cannot be declared retired')
                retired=gate_state(-1);r['native_gate_terminal']=deepcopy(retired);check('native sample gate retired before owned cleanup',retired['live'],False)
            except BaseException as exc:
                cleanup_safe=False;r.update(status='fail',disarm_error=str(exc),disarm_sqlstate=getattr(exc,'pgcode',None))
        if refusal_setting:
            try:
                if not claimed:raise RuntimeError('Worker retirement unproven before resetting owned DB statistics target')
                query(admin,sql.SQL('ALTER DATABASE {} RESET default_statistics_target').format(sql.Identifier(database.database)))
                refusal_setting=False
                check('owned database target override removed',scalar(admin,
                    "SELECT count(*) FROM pg_db_role_setting WHERE setdatabase=%s AND setrole=0 AND 'default_statistics_target=-1'=ANY(setconfig)",
                    (database.allocation['database_oid'],)),0)
                r['refusal_database_setting_reset']=True
            except BaseException as exc:
                cleanup_safe=False;r.update(status='fail',refusal_setting_cleanup_error=str(exc),refusal_setting_cleanup_sqlstate=getattr(exc,'pgcode',None))
        if claimed:
            try:release_claim()
            except BaseException as exc:r.update(status='fail',claim_cleanup_error=str(exc))
        if not cleanup_safe:r.update(status='fail',preserve_owned_scene=True)
        if database is not None and cleanup_safe:
            try:database.cleanup();r['database_cleanup']=database.cleanup_result
            except subprocess.TimeoutExpired as exc:
                owner=Path(database.attempt)/'database-owner.json'
                diagnostic=dict(status='fail',kind='owned_cleanup_timeout',argv=image(exc.cmd),timeout=exc.timeout,
                    owner_receipt=str(owner),owner_sha256=sha256(owner) if owner.exists() else None,
                    stdout=image(exc.stdout),stderr=image(exc.stderr),cleanup_proven=False,cross_restart_cleanup_attempted=False)
                write_json(out/'cleanup-timeout.json',diagnostic)
                r.update(status='fail',cleanup_error=str(exc),cleanup_timeout=diagnostic)
            except BaseException as exc:r.update(status='fail',cleanup_error=str(exc))
        if roles is not None and cleanup_safe:
            try:
                if not r.get('database_cleanup') or r['database_cleanup']['returncode']:
                    raise RuntimeError('Exact database cleanup unproven; roles preserved')
                r['role_cleanup']=cleanup_roles(control,roles.path,a.run_id)
            except BaseException as exc:r.update(status='fail',role_cleanup_error=str(exc))
        if roles is not None:r=roles.redact(r)
        if control is not None:control.close()
        write_json(out/'result.json',r)
    print(str(out/'result.json')+': '+r['status'])
    return 0 if r['status']=='pass' else 1


if __name__=='__main__':
    raise SystemExit(main())
