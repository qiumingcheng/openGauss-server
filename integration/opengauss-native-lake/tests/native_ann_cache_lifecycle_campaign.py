#!/usr/bin/env python3
"""Prepare or serially supervise eight real ann-debug cache lifecycle cases.

Default prepares only. Execution requires an exact official SQL22 deployment,
qualified SDK4dd and original PQ/CRoaring receipts. No build, service, config,
backend termination or controller database cleanup is performed. A failed child
stops the matrix; hard timeout signals only its nonce-verified Python group and
preserves its scene. The frozen leaf retains two512-row files and600000ms SQL.
"""
import argparse,ast,fcntl,hashlib,json,os,re,secrets,shutil,signal,subprocess,sys,time,traceback
from pathlib import Path
ROOT=Path(os.getenv('CPP_WORKSPACE','/home/qiumc/data/code/cpp')).resolve()
D=ROOT/'integration/opengauss-native-lake';T=D/'tests'
LEAF=T/'native_ann_cache_lifecycle.py'
LEAF_SHA='9c1e915322f355fdb30453c6059569fd4d525a565cba9b0c811cf033660aa5ae'
SDK_SHA='4dd3353cf533cfaf812420e9337f6daf64acc1a2f85ffa64a6dd4f1680276f11'
SDK_INDEX_SHA='6ce416840e82aadba40c409bbc8fbc95abca9b20b7515a84b2382ec9f4154974'
MODES=['cursor','subxact','multi-budget','concat','concat-error','concat-cancel','target-miss','abort-before-load']
HARD_SECONDS=1200


def sha(path):
    h=hashlib.sha256()
    with Path(path).open('rb')as stream:
        for chunk in iter(lambda:stream.read(1024*1024),b''):h.update(chunk)
    return h.hexdigest()


def dump(path,value):
    path=Path(path);temporary=path.with_name(path.name+'.new')
    with temporary.open('w')as stream:
        json.dump(value,stream,ensure_ascii=False,indent=2);stream.write('\n');stream.flush();os.fsync(stream.fileno())
    os.replace(temporary,path)


def require(ok,reason):
    if not ok:raise RuntimeError(reason)


def prepare(modes,sql_version):
    require(bool(modes)and len(modes)==len(set(modes))and all(m in MODES for m in modes),'Duplicate/unknown/empty mode selection')
    require(sql_version in ['0.19','0.20','0.21'],'Only reviewed retained SQL19/20/21')
    require(sha(LEAF)==LEAF_SHA,'Frozen lifecycle leaf changed; independent review required')
    tree=ast.parse(LEAF.read_text());declared=next(ast.literal_eval(n.value)for n in tree.body if isinstance(n,ast.Assign)and any(isinstance(t,ast.Name)and t.id=='MODES'for t in n.targets))
    require(declared==MODES,'Frozen leaf mode contract changed')
    return dict(status='prepared_not_executed',strict_serial=True,native_cases_executed=0,profile='ann-debug',
        runtime_default='0.22',installed_sql=sql_version,controller_sha256=sha(__file__),leaf=str(LEAF),leaf_sha256=LEAF_SHA,
        cases=[dict(id=m,mode=m,sql_version=sql_version,hard_seconds=HARD_SECONDS)for m in modes],
        fixture_contract='Each leaf generates its unique genuine EMPTY_BUNDLE, owned database/role and two512-row vector16 files; never reuse another leaf fixture.',
        requirements=['Exact official successful ann-debug deployment and its original compiled receipt; current source/binary/6+wrappers checked by receipt, no restart during campaign.',
            'Complete installed SDK owner/inventory/header/control binding; original strict12-case index plus normal+ASan PQ/source and private roaring receipts revalidated before/after each case.',
            'Maintenance off; dedicated backends. Root serializes other profiles and all reference/fixture workloads; existing shared campaign locks protect this profile.',
            'The frozen leaf owns its diagnostic gates, original errors, database/roles and normal cleanup. Controller validates profile-aware exact cleanup without cleanup repair.'],
        boundaries=['No original512-row/600000ms SQL gate reduced; each leaf external owned-processgroup deadline is exactly1200s.',
            'Covers only listed cursor/savepoint/global32MiB/Concat/target-miss/preload cases on retained installed SQL. No SMP/threadpool/RAC/recall/performance/commercial claim.',
            'Large binary/data payloads may be hash-only references to immutable official/component archives; archived source inputs are identified separately, never labelled copied.'])


def sdk_inventory(install,source,selected,index_sha):
    raw=install/'lib/postgresql/og_lake_sdk';root=raw.resolve();owner_path=root/'owner.json';owner=json.loads(owner_path.read_text())
    require(root==raw.absolute()and not raw.is_symlink()and not owner_path.is_symlink(),'SDK owner/prefix alias or symlink forbidden')
    require(owner.get('project')=='opengauss-native-lake-local-provider'and owner.get('phase')=='prepared-for-atomic-install'and Path(owner.get('target','')).resolve()==root,'SDK project/phase/target differs')
    actual={}
    for path in root.rglob('*'):
        require(not path.is_symlink()and(path.is_dir()or path.is_file()),'SDK symlink or special payload')
        if path.is_file()and path!=owner_path:actual[str(path.relative_to(root))]=sha(path)
    require(bool(actual)and actual==owner.get('files'),'Complete SDK payload differs from owner inventory')
    require(owner.get('qualification_kind')=='verified_independent_sdk_sql020_int16_only'and owner.get('qualification_sha256')==index_sha==SDK_INDEX_SHA,'SDK owner lacks exact original12-case qualification')
    require(selected['library_sha256']==SDK_SHA==sha(root/'libog_lake_sdk_arrow.so'),'Not exact qualified4dd provider')
    expected={str((source/'contrib/og_lake'/p).resolve()):sha(source/'contrib/og_lake'/p)for p in ['og_lake.control','sdk/lake_parquet_sdk.h','vendor/arrow20/abi.h']}
    require(owner.get('native_public_headers')==expected,'SDK full Native header/control binding differs')
    require(sha(source/'contrib/og_lake/sdk/lake_parquet_sdk.h')==selected['header_sha256']==sha(root/'include/og_lake/lake_parquet_sdk.h'),'Native/installed/qualified full SDK header differs')
    return dict(owner=str(owner_path),owner_sha256=sha(owner_path),files=actual,library_sha256=SDK_SHA,native_public_headers=expected)


def leaf_contract(data,case,build,runtime):
    require(data.get('status')=='pass'and data.get('mode')==case['mode']and data.get('sql_version')==case['sql_version'],'Leaf status/mode/installed selection differs')
    require(data.get('build_sha256')==build['_selected_receipt_sha256']and data.get('binary_sha256')==build['binary_sha256'],'Leaf build/binary does not match exact deployment')
    # This immutable leaf has no executed/runtime-version output fields. Its
    # real Case assertions prove those values; never fabricate absent fields.
    for label,want in [('exact reviewed runtime for retained SQL0.7',runtime),('exact installed SQL version',case['sql_version']),('two actual512row sources',[512,512]),('both sources have real format2',[[2]])]:
        rows=[r for r in data.get('assertions',[])if r.get('name')==label]
        if label=='both sources have real format2':
            # Leaf JSON serializes the original SQL tuple [(2,)] as [[2]].
            want=[[2]]
        require(len(rows)==1 and rows[0].get('status')=='pass'and rows[0].get('observed')==want and rows[0].get('expected')==want,'Missing/mismatched actual leaf contract: '+label)
    return dict(mode=case['mode'],runtime_module=runtime,installed_sql=case['sql_version'],rows_per_file=[512,512])


def members(record,nonce_digest,proc=Path('/proc')):
    """Prove all living members before group signals; never persist the nonce.

    A dead/reaped leader does not prove its children drained. Unknown nonce,
    UID or reused leader identity refuses signals and preserves the scene.
    """
    found=[]
    for path in proc.iterdir():
        if not path.name.isdigit():continue
        try:
            stat=(path/'stat').read_text();tail=stat[stat.rfind(')')+2:].split()
            if int(tail[2])!=record['pgid']or int(tail[3])!=record['sid']or tail[0]=='Z':continue
            require(path.stat().st_uid==record['uid'],'Owned Python group UID changed; refuse signal')
            environment=(path/'environ').read_bytes().split(b'\0')
            nonce=next((x.split(b'=',1)[1]for x in environment if x.startswith(b'OG_LAKE_CACHE_OWNER_NONCE=')),None)
            require(nonce is not None and hashlib.sha256(nonce).hexdigest()==nonce_digest,'Unknown group member nonce; refuse signal')
            ticks=int(tail[19])
            require(int(path.name)!=record['pid']or ticks==record['start_ticks'],'Leader PID reused; refuse signal')
            found.append(dict(pid=int(path.name),start_ticks=ticks))
        except FileNotFoundError:continue
    return sorted(found,key=lambda item:item['pid'])


def spawn_identity(child,nonce_digest):
    path=Path('/proc')/str(child.pid);stat=(path/'stat').read_text();tail=stat[stat.rfind(')')+2:].split()
    record=dict(pid=child.pid,pgid=int(tail[2]),sid=int(tail[3]),start_ticks=int(tail[19]),uid=path.stat().st_uid,nonce_sha256=nonce_digest)
    require(record['pgid']==child.pid and record['sid']==child.pid and record['uid']==os.getuid(),'New Python session ownership differs')
    record['initial_members']=members(record,nonce_digest);return record


def stop_owned(child,record,nonce_digest,evidence,progress=lambda:None):
    """At most5+10s grace; signals require a complete current membership proof."""
    evidence.update(signals=[],membership_checks=[],drained=False)
    for sig,seconds in [(signal.SIGTERM,5),(signal.SIGKILL,10)]:
        live=members(record,nonce_digest);evidence['membership_checks'].append(dict(before_signal=int(sig),members=live));progress()
        if not live:break
        try:os.killpg(record['pgid'],sig);evidence['signals'].append(int(sig));progress()
        except ProcessLookupError:break
        until=time.monotonic()+seconds
        while time.monotonic()<until:
            child.poll()
            if not members(record,nonce_digest):break
            time.sleep(.05)
    remaining=members(record,nonce_digest);evidence.update(remaining=remaining,drained=not remaining);progress()
    require(not remaining,'Owned Python group did not drain within5+10s; preserve scene')
    require(child.poll()is not None,'Leader is still alive outside its proved group; preserve scene')
    return evidence


def execute(args,plan):
    # Runtime modules and all Native/reference actions are unreachable in prepare.
    os.environ.update(CPP_WORKSPACE=str(ROOT),OG_LAKE_TEST_PROFILE='ann-debug',PYTHONDONTWRITEBYTECODE='1')
    sys.path[:0]=[str(D),str(T)]
    from file_dml_acceptance import check_build
    from inline_fixture import query,scalar
    from native_auto_flush import connect_control
    from native_runtime import PROFILE,RUNTIME,SOURCE,INSTALL
    from lake_harness import validate_run_id
    from pq_sql020_build_inputs import sdk_index
    from build_driver import selected_pq_inputs,roaring_inputs
    from empty_manifest import validate_empty_bundle
    from native_ix5_campaign import cleanup_receipt,checked_assertions
    validate_run_id(args.run_id);require(len(args.run_id)<=48,'Campaign ID max48 reserves fixture descendant suffixes')
    require(PROFILE=='ann-debug','Only the exact owned ann-debug profile')
    out=ROOT/'公共文档/openGauss原生湖存储/验证'/args.run_id;out.mkdir(exist_ok=False)
    result=dict(status='running',plan=plan,cases=[],runtime_executed=False,controller_database_cleanup_attempted=False)
    locks=[];control=None;frozen={};followed=set();fixture_roots=[]
    child=record=nonce_digest=None;retirement_attempted=False;ownership_refused=False;active_row=None
    def write():dump(out/'index.json',result)
    try:
        for name in ['.n7-qualification.lock','.ix5-qualification.lock','native-ann-cache-lifecycle-campaign.lock']:
            f=(RUNTIME/name).open('a+');fcntl.flock(f,fcntl.LOCK_EX|fcntl.LOCK_NB);locks.append(f)
        # Do not hold .driver.lock here: the frozen real fixture generator needs
        # that existing nonblocking lock itself. Root owns cross-profile order.
        build=check_build(args.build_receipt);build['_selected_receipt_sha256']=sha(args.build_receipt)
        require(build.get('mode')=='restart'and build.get('compiled_build_receipt'),'Requires official separate ann-debug deployment receipt')
        compiled=Path(build['compiled_build_receipt']);require(sha(compiled)==build['compiled_build_receipt_sha256'],'Original compiled receipt changed')
        original=json.loads(compiled.read_text())
        require(original.get('status')=='build_and_sql_passed'and original.get('profile')=='debug','Original compiled Native receipt not successful Debug')
        for key in ['binary_sha256','source_head','source_diff_sha256','native_source_inputs_after','build_wrapper_inputs_after','pq_inputs_after','roaring_inputs_after']:
            require(original.get(key)==build.get(key),'Deployment no longer matches compiled receipt '+key)
        versions=re.findall(r"^default_version\s*=\s*'([^']+)'",(SOURCE/'contrib/og_lake/og_lake.control').read_text(),re.M)
        require(versions==['0.22'],'Requires reviewed exact SQL22 source, installed21 remains distinct')
        required={'contrib/og_lake/lake_ann_rescan.cpp','contrib/og_lake/lake_test_hooks.cpp','contrib/og_lake/lake_file_ann.cpp','contrib/og_lake/lake_inline_scan.cpp'}
        require(required<=set(build['native_source_inputs_after']),'Official source inventory lacks cache/diagnostic/defer modules')
        for key in ['pq_inputs','roaring_inputs']:
            require(build.get(key+'_before')==build.get(key+'_after')and bool(build.get(key+'_after')),'Missing/changed qualified dependency '+key)
        selected=sdk_index(args.sdk_index,args.sdk_index_sha256,ROOT)
        installed=sdk_inventory(INSTALL,SOURCE,selected,args.sdk_index_sha256)
        # The pinned SDK proof includes an observation of the installation that
        # existed BEFORE that provider was installed. Archive this exact result;
        # its observed paths are not dependencies of today's deployment.
        sdk_proof=json.loads(args.sdk_index.read_text())
        historical=[row for row in sdk_proof['assertions']if row.get('name')=='current installed SDK/native preserved']
        require(len(historical)==1,'Pinned SDK historical observation missing/ambiguous')
        historical_install=historical[0]
        require(historical_install.get('status')=='pass'and historical_install.get('observed')==historical_install.get('expected')and
            set(historical_install['observed'])=={str(INSTALL/'bin/gaussdb'),str(INSTALL/'lib/postgresql/og_lake_sdk/libog_lake_sdk_arrow.so')},
            'Pinned SDK historical observation malformed')
        def dependencies():
            pq=selected_pq_inputs(Path(build['pq_inputs_after']['receipt']),ROOT)
            roaring=roaring_inputs(Path(build['roaring_inputs_after']['receipt']))
            require(pq==build['pq_inputs_after']and roaring==build['roaring_inputs_after'],'Actual PQ/roaring identity differs from deployment')
            require(pq['qualified_sources']['sdk_index']==selected,'PQ component qualified a different SDK index')
            return dict(pq=pq,roaring=roaring)
        deps=dependencies();control=connect_control()
        query(control,'SET session_timeout=0');query(control,'SET statement_timeout=30000')
        def identity():
            return dict(postmaster_start=str(scalar(control,'SELECT pg_postmaster_start_time()')),data=str(Path(scalar(control,'SHOW data_directory')).resolve()),
                administrator_oid=scalar(control,'SELECT oid FROM pg_roles WHERE rolname=current_user'),binary_sha256=sha(INSTALL/'bin/gaussdb'),
                pidfile=(RUNTIME/'data/postmaster.pid').read_text().splitlines()[:3])
        initial=identity()
        for name,want in [('enable_lake_maintenance','off'),('enable_thread_pool','off')]:require(scalar(control,'SHOW '+name)==want,'Required runtime prerequisite differs: '+name)
        require(initial['data']==str((RUNTIME/'data').resolve()),'Control reached another runtime')
        def quiet():
            others=query(control,'SELECT pid,datname FROM pg_stat_activity WHERE client_port IS NOT NULL AND pid<>pg_backend_pid()')
            owned=query(control,"SELECT oid,datname FROM pg_database WHERE datname LIKE 'lake_inline_%%'")
            require(not others and not owned,'Retained fixture or unrelated frontend; no cleanup attempted: '+repr((others,owned)))
        quiet();archive=out/'frozen';archive.mkdir()
        def add(path,expected=None):
            path=Path(path).resolve(strict=True);require(path.is_file(),'Input is not regular file')
            digest=sha(path)
            require(expected is None or digest==expected,'Declared input hash mismatch: '+str(path))
            if str(path)in frozen:
                require(frozen[str(path)]['sha256']==digest,'Input changed while freezing');return
            row=dict(sha256=digest,bytes=path.stat().st_size)
            # Native/DuckDB binaries and large component data are immutable
            # hash-only inputs, explicitly separate from archived source files.
            if row['bytes']>16*1024*1024:
                row.update(archive=None,storage='hash_only_immutable_official_or_component_input')
            else:
                destination=archive/(digest+'-'+path.name)
                if not destination.exists():shutil.copy2(path,destination)
                require(sha(destination)==digest,'Copied input hash mismatch')
                row.update(archive=str(destination),storage='archived_bytes')
            frozen[str(path)]=row
        def follow(value):
            if isinstance(value,dict):
                if value==historical_install:
                    result['historical_sdk_install_observation']=dict(receipt=str(args.sdk_index.resolve()),
                        receipt_sha256=SDK_INDEX_SHA,observation=value,
                        interpretation='Archived historical result only; current SDK/native checked independently')
                    return
                # Qualified receipts name their complete original sources and
                # proofs as absolute-path→SHA maps and path/sha256 records.
                for name,h in value.items():
                    if isinstance(name,str)and name.startswith('/')and isinstance(h,str)and re.fullmatch('[0-9a-f]{64}',h):add(name,h)
                for pk,hk in [('path','sha256'),('receipt','receipt_sha256')]:
                    if isinstance(value.get(pk),str)and Path(value[pk]).is_absolute()and isinstance(value.get(hk),str):
                        path=Path(value[pk]);add(path,value[hk])
                        if path.suffix=='.json'and str(path)not in followed:
                            followed.add(str(path));follow(json.loads(path.read_text()))
                for child in value.values():follow(child)
            elif isinstance(value,list):
                for child in value:follow(child)
        for path in [Path(__file__),LEAF,args.build_receipt,compiled,args.sdk_index,INSTALL/'bin/gaussdb',Path(installed['owner'])]:add(path)
        for name,h in build['native_source_inputs_after'].items():add(SOURCE/name,h)
        for name,h in build['build_wrapper_inputs_after'].items():add(name,h)
        for path in D.rglob('*'):
            if path.is_file()and'__pycache__'not in path.parts and'state'not in path.relative_to(D).parts and(path.suffix in ['.py','.sh','.sql','.cpp','.h','.hpp','.inc','.control']or path.name=='source-lock.json'):add(path)
        for path in (INSTALL/'lib/postgresql/og_lake_sdk').rglob('*'):
            if path.is_file():add(path)
        sql_names=['og_lake.control','og_lake--0.22.sql','og_lake--0.21--0.22.sql','og_lake--'+args.sql_version+'.sql']
        for name in sql_names:add(INSTALL/'share/postgresql/extension'/name,sha(SOURCE/'contrib/og_lake'/name))
        for kind,dep in deps.items():
            raw=json.loads(Path(dep['receipt']).read_text());add(dep['receipt'],dep['receipt_sha256']);follow(raw)
            for name,h in dep['files'].items():add(Path(dep['prefix'])/name,h)
            add(Path(dep['prefix'])/('owner.json'if kind=='roaring'else'.og-lake-pq-owner.json'),dep['owner_sha256'])
            if kind=='roaring':
                add(Path(raw['source'])/'src/roaring_array.c',raw['patched_source_sha256'])
                add(Path(raw['source'])/raw['source_license']['file'],raw['source_license']['sha256'])
                add(raw['source_patch'],raw['source_patch_sha256']);add(raw['base_build'],raw['base_build_sha256'])
                follow(json.loads(Path(raw['base_build']).read_text()))
        add(Path(selected['prefix'])/'.og-lake-sdk-owner.json')
        follow(json.loads(args.sdk_index.read_text()));follow(build['pq_inputs_after']['qualified_sources'])
        reference_path=ROOT/'.reference/opengauss-native-lake/build/receipt.json';rr=json.loads(reference_path.read_text())
        add(reference_path);require(rr['source_lock_hash']==sha(D/'source-lock.json'),'Independent reference source-lock differs');add(rr['binary'],rr['binary_sha256'])
        manifest=out/'frozen-inputs.json';dump(manifest,dict(profile=PROFILE,files=frozen,identity=initial,compiled_receipt=str(compiled),dependencies=deps,sdk=installed))
        result.update(frozen_manifest=str(manifest),frozen_sha256=sha(manifest),runtime_identity=initial,binary_sha256=build['binary_sha256'],sdk=installed,official_deployment=str(args.build_receipt.resolve()))
        def verify():
            check_build(args.build_receipt);require(identity()==initial,'Runtime restarted/changed; no cleanup across restart')
            require(sha(LEAF)==LEAF_SHA,'Frozen lifecycle leaf changed')
            require(sdk_index(args.sdk_index,args.sdk_index_sha256,ROOT)==selected,'SDK original strict index changed')
            require(sdk_inventory(INSTALL,SOURCE,selected,args.sdk_index_sha256)==installed,'Installed SDK complete owner/payload changed')
            require(dependencies()==deps,'PQ/roaring qualification changed')
            for p,v in frozen.items():
                require(sha(p)==v['sha256'],'Frozen original input changed '+p)
                if v['archive']is not None:require(sha(v['archive'])==v['sha256'],'Frozen archive changed '+p)
        for case in plan['cases']:
            verify();quiet();child_id=args.run_id+'-'+case['mode'];validate_run_id(child_id)
            leafout=out.parent/child_id;require(not leafout.exists(),'Prior child evidence exists')
            command=[sys.executable,str(LEAF),'--execute','--mode',case['mode'],'--sql-version',case['sql_version'],'--run-id',child_id,'--build-receipt',str(args.build_receipt.resolve())]
            row=dict(mode=case['mode'],run_id=child_id,command=command,status='running',hard_seconds=HARD_SECONDS,preserve_owned_scene=True)
            result['cases'].append(row);result['runtime_executed']=True;write();started=time.monotonic();timed=False
            nonce=secrets.token_hex(24);nonce_digest=hashlib.sha256(nonce.encode()).hexdigest()
            env=dict(os.environ,CPP_WORKSPACE=str(ROOT),OG_LAKE_TEST_PROFILE='ann-debug',PYTHONDONTWRITEBYTECODE='1',OG_LAKE_CACHE_OWNER_NONCE=nonce)
            active_row=row;retirement_attempted=False;ownership_refused=False;row['nonce_sha256']=nonce_digest
            with(out/(case['mode']+'.stdout')).open('wb')as stream:
                child=subprocess.Popen(command,stdout=stream,stderr=subprocess.STDOUT,env=env,start_new_session=True)
                row['python_pid']=child.pid;row['started_monotonic']=started;write()
                try:record=spawn_identity(child,nonce_digest)
                except BaseException:ownership_refused=True;raise
                row['process_identity']=record;write()
                try:child.wait(timeout=max(0,HARD_SECONDS-(time.monotonic()-started)))
                except(subprocess.TimeoutExpired,KeyboardInterrupt):
                    timed=True;row['hard_timeout']=True;retirement_attempted=True
                    row['process_retirement']={};write()
                    stop_owned(child,record,nonce_digest,row['process_retirement'],write)
            row.update(returncode=child.returncode,hard_timeout=timed,elapsed_seconds=round(time.monotonic()-started,3))
            try:live=members(record,nonce_digest)
            except BaseException:ownership_refused=True;raise
            row['process_group_exit']=dict(phase='timeout_retirement'if timed else'normal',remaining=live,drained=not live);write()
            require(not live,'Leader exited but owned group helper remains; fail and preserve scene')
            child=record=nonce_digest=None;active_row=None
            path=leafout/'result.json'
            if path.is_file():row['result_sha256']=sha(path);shutil.copy2(path,out/(case['mode']+'-result.json'))
            write();require(not timed,'Owned child process group exceeded1200s; stop and preserve scene')
            require(row['returncode']==0 and path.is_file(),'Actual lifecycle child failed; preserve original result and stop')
            data=json.loads(path.read_text());row['actual_contract']=leaf_contract(data,case,build,versions[0]);row['assertions']=checked_assertions(data)
            inputs=data.get('inputs');require(bool(inputs),'Leaf executed input archive absent')
            for p,v in inputs.items():require(sha(p)==v['sha256']==sha(v['archive']),'Leaf original/archive bytes differ')
            require(str(LEAF.resolve())in inputs and inputs[str(LEAF.resolve())]['sha256']==LEAF_SHA,'Leaf archive is not selected frozen script')
            row['archived_inputs']=len(inputs)
            bundle=validate_empty_bundle(leafout/'fixture/manifest.json');require(bundle['run_id']==child_id,'Genuine fixture belongs to another child')
            fixture_root=Path(bundle['cleanup_owner']).resolve();require(fixture_root.is_relative_to(leafout),'Fixture generation escapes its unique leaf')
            require(not any(fixture_root==p or fixture_root.is_relative_to(p)or p.is_relative_to(fixture_root)for p in fixture_roots),'Fixture roots reused/overlap across children');fixture_roots.append(fixture_root)
            row['fixture']=dict(owner=str(fixture_root),bundle_sha256=sha(leafout/'fixture/manifest.json'))
            # This existing helper takes the actual profile argument. Never
            # mutate the Debug-only older verify_cleanup helper or fake Debug.
            row['cleanup']=cleanup_receipt(leafout.resolve(),data,child_id,'ann-debug',out,initial)
            verify();quiet();row.update(status='pass',preserve_owned_scene=False);write()
        result.update(status='pass',assertions=sum(r['assertions']for r in result['cases']),completed_cases=len(result['cases']))
    except BaseException as exc:
        result.update(status='fail',reason=str(exc),exception_type=type(exc).__name__,traceback=traceback.format_exc())
        if result['cases']and result['cases'][-1]['status']=='running':result['cases'][-1].update(status='fail',reason=str(exc),preserve_owned_scene=True)
    finally:
        if child is not None:
            try:
                # Never retry the15s retirement window, including if a proof
                # failed halfway. Unknown ownership refuses all group signals.
                require(not ownership_refused,'Prior group ownership proof failed; refuse signals and preserve scene')
                if record is None and child.poll()is None:record=spawn_identity(child,nonce_digest)
                require(record is not None,'Child identity was not captured; refuse signals and preserve scene')
                live=members(record,nonce_digest)
                if live and not retirement_attempted:
                    retirement_attempted=True;active_row['process_retirement']={};write()
                    stop_owned(child,record,nonce_digest,active_row['process_retirement'],write)
                remaining=members(record,nonce_digest)
                if active_row is not None:active_row['final_group_exit']=dict(remaining=remaining,drained=not remaining)
            except BaseException as exc:
                result['process_retirement_error']=str(exc)
                if active_row is not None:active_row['process_retirement_error']=str(exc)
        if control is not None:control.close()
        for f in reversed(locks):f.close()
        dump(out/'index.json',result)
    return 0 if result['status']=='pass'else 1


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--execute',action='store_true');parser.add_argument('--run-id');parser.add_argument('--build-receipt',type=Path)
    parser.add_argument('--sdk-index',type=Path);parser.add_argument('--sdk-index-sha256')
    parser.add_argument('--modes',nargs='+',choices=MODES,default=MODES)
    parser.add_argument('--sql-version',choices=['0.19','0.20','0.21'],default='0.21')
    parser.add_argument('--prepare-output',type=Path)
    args=parser.parse_args()
    plan=prepare(args.modes,args.sql_version)
    if not args.execute:
        if args.prepare_output:require(not args.prepare_output.exists(),'Never overwrite prior prepare evidence');dump(args.prepare_output,plan)
        print(json.dumps(plan,ensure_ascii=False,indent=2));return 0
    if not args.run_id or not args.build_receipt or not args.sdk_index or args.sdk_index_sha256!=SDK_INDEX_SHA:
        parser.error('execute requires run-id, exact ann-debug deployment, original12-case SDK index and its exact SHA256')
    return execute(args,plan)
if __name__=='__main__':raise SystemExit(main())
