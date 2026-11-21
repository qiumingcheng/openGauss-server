#!/usr/bin/env python3
"""Serial eight-case auto-ANALYZE qualification; default only prepares a plan.

An executed leaf generates its own genuine EMPTY and absent private data root.
No instance configuration/service changes or cross-restart cleanup. A timeout
signals only the owned Python process group, preserves DB/roles and stops.
"""
import argparse,ast,fcntl,hashlib,json,os,re,shutil,signal,subprocess,sys,time,traceback
from pathlib import Path
ROOT=Path(os.getenv('CPP_WORKSPACE','/home/qiumc/data/code/cpp')).resolve()
D=ROOT/'integration/opengauss-native-lake';HERE=Path(__file__).resolve().parent
LEAF=HERE/'native_autoanalyze.py'
MODES=['counters','automatic','mvcc','cancel','refusal','upgrade','legacy','lifecycle']

def sha(path):
    h=hashlib.sha256()
    with Path(path).open('rb')as f:
        for block in iter(lambda:f.read(1024*1024),b''):h.update(block)
    return h.hexdigest()

def dump(path,value):Path(path).write_text(json.dumps(value,ensure_ascii=False,indent=2)+'\n')

def plan(modes,budget,timeout,sql_version="0.19"):
    choices=[]
    for node in ast.walk(ast.parse(LEAF.read_text())):
        if isinstance(node,ast.Call)and isinstance(node.func,ast.Attribute)and node.func.attr=='add_argument'and node.args and isinstance(node.args[0],ast.Constant)and node.args[0].value=='--mode':
            choices=[ast.literal_eval(k.value)for k in node.keywords if k.arg=='choices'][0]
    if choices!=MODES:raise RuntimeError('Leaf modes changed; independent review required')
    if len(set(modes))!=len(modes):raise ValueError('Duplicate mode forbidden')
    return dict(status='prepared_not_executed',strict_serial=True,native_cases_executed=0,
        leaf_sha256=sha(LEAF),controller_sha256=sha(__file__),cases=[dict(mode=m,
            initial_sql='0.18'if m in ['upgrade','legacy']else sql_version,
            final_sql='0.18'if m=='legacy'else sql_version,budget_seconds=budget,hard_seconds=timeout,
            fixture='leaf generates one genuine EMPTY with exact child run ID; unique absent owner/data root')for m in modes],
        requirements=['official successful exact Debug receipt; module matches reviewed source.control19/20/21, installed target recorded separately',
            'launcher+autoanalyze on, index maintenance off, autoanalyze budget30000ms, dedicated backend; root configures externally',
            'no unrelated frontend or retained owned DB; strict serial fixture ownership',
            'source/ref/provider/dependency archives checked before and after every case'],
        limits=['No injected Native OOM proof; actual observed errors retain SQLSTATE and fail unless exact expected0A000/57014.',
            'No RAC, performance, fairness/readonly/file-budget matrix or commercial qualification.'])

def execute(args,prepared):
    sys.path[:0]=[str(D),str(D/'tests')]
    from file_dml_acceptance import check_build
    from inline_fixture import query,scalar
    from native_auto_flush import connect_control
    from native_runtime import PROFILE,RUNTIME,SOURCE,INSTALL
    from lake_harness import validate_run_id
    # Reuse existing independently reviewed receipt checkers; adapt only the
    # one actual AA OwnedDatabase receipt into their multiple-DB envelope.
    from native_direct_campaign import assertions,verify_archive,verify_cleanup
    validate_run_id(args.run_id)
    if PROFILE!='debug':raise RuntimeError('Only owned Debug profile')
    out=ROOT/'公共文档/openGauss原生湖存储/验证'/args.run_id;out.mkdir(exist_ok=False)
    result=dict(status='fail',plan=prepared,cases=[],runtime_executed=False,controller_database_cleanup_attempted=False)
    locks=[];control=None
    try:
        for name in ['.n7-qualification.lock','.ix5-qualification.lock','native-autoanalyze-campaign.lock']:
            f=(RUNTIME/name).open('a+');fcntl.flock(f,fcntl.LOCK_EX|fcntl.LOCK_NB);locks.append(f)
        build=check_build(args.build_receipt);control=connect_control();query(control,'SET session_timeout=0');query(control,'SET statement_timeout=30000')
        def identity():return dict(postmaster_start=str(scalar(control,'SELECT pg_postmaster_start_time()')),
            data=str(Path(scalar(control,'SHOW data_directory')).resolve()),administrator_oid=scalar(control,'SELECT oid FROM pg_roles WHERE rolname=current_user'),binary_sha256=sha(INSTALL/'bin/gaussdb'))
        initial=identity()
        for name,want in [('enable_lake_maintenance','on'),('enable_lake_autoanalyze','on'),('enable_lake_index_maintenance','off'),('enable_thread_pool','off')]:
            if scalar(control,'SHOW '+name)!=want:raise RuntimeError('Missing explicit runtime prerequisite '+name)
        if scalar(control,"SELECT setting::int FROM pg_settings WHERE name='lake_autoanalyze_budget'")!=30000:raise RuntimeError('Requires exact30s autoanalyze budget')
        defaults=re.findall(r"^default_version\s*=\s*'([^']+)'",(SOURCE/'contrib/og_lake/og_lake.control').read_text(),re.M)
        if len(defaults)!=1 or defaults[0]not in ['0.19','0.20','0.21']:raise RuntimeError('Requires exact reviewed source runtime19/20/21; legacy18 remains scoped per leaf')
        if int(defaults[0].split('.')[1])<int(args.sql_version.split('.')[1]):raise RuntimeError('Selected installed capability exceeds source runtime')
        def quiet():
            owned=query(control,"SELECT oid,datname FROM pg_database WHERE datname LIKE 'lake_inline_%%'")
            others=query(control,'SELECT pid,datname FROM pg_stat_activity WHERE client_port IS NOT NULL AND pid<>pg_backend_pid()')
            if owned or others:raise RuntimeError('Other/retained fixture/frontend; no cleanup attempted: '+repr((owned,others)))
        quiet();frozen=out/'frozen';frozen.mkdir();files={}
        def add(path,relative,expected=None):
            path=Path(path).resolve()
            if str(path)in files:return
            digest=sha(path)
            if expected is not None and digest!=expected:raise RuntimeError('Declared source hash mismatch '+str(path))
            relative=Path(relative)
            if relative.is_absolute()or'..'in relative.parts:raise ValueError('Unsafe archive path')
            target=frozen/relative;target.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(path,target)
            if sha(target)!=digest:raise RuntimeError('Archive mismatch '+str(path))
            files[str(path)]=dict(sha256=digest,archive=str(target.resolve()))
        add(args.build_receipt,'official-build.json')
        for name,digest in build['native_source_inputs_after'].items():add(SOURCE/name,Path('native-source')/name,digest)
        for path,digest in build.get('build_wrapper_inputs_after',{}).items():add(path,Path('wrappers')/Path(path).relative_to(ROOT),digest)
        for path in D.rglob('*'):
            if path.is_file()and'__pycache__'not in path.parts and'state'not in path.relative_to(D).parts and(path.suffix in ['.py','.sh','.sql','.cpp','.h','.hpp','.inc','.control']or path.name=='source-lock.json'):
                add(path,Path('integration')/path.relative_to(D))
        for p in [Path(__file__),LEAF]:add(p,Path('qualification')/p.name)
        add(INSTALL/'bin/gaussdb','binary/gaussdb',build['binary_sha256'])
        add(INSTALL/'lib/postgresql/og_lake_sdk/libog_lake_sdk_arrow.so','binary/provider.so')
        for name in ['og_lake.control','og_lake--0.18.sql','og_lake--0.19.sql','og_lake--0.18--0.19.sql']+(['og_lake--0.20.sql','og_lake--0.19--0.20.sql']if defaults[0]in ['0.20','0.21']else[])+(['og_lake--0.21.sql','og_lake--0.20--0.21.sql']if defaults[0]=='0.21'else[]):
            add(INSTALL/'share/postgresql/extension'/name,Path('installed-sql')/name,sha(SOURCE/'contrib/og_lake'/name))
        ref=ROOT/'.reference/opengauss-native-lake/build/receipt.json';reference=json.loads(ref.read_text());add(ref,'reference/build.json')
        if reference['source_lock_hash']!=sha(D/'source-lock.json'):raise RuntimeError('Reference source-lock mismatch')
        add(reference['binary'],'reference/duckdb',reference['binary_sha256'])
        for key in ['roaring_inputs_after','pq_inputs_after']:
            dependency=build.get(key)
            if dependency:
                add(dependency['receipt'],Path('dependencies')/key/'build.json',dependency['receipt_sha256'])
                for name,digest in dependency.get('files',{}).items():add(Path(dependency['prefix'])/name,Path('dependencies')/key/name,digest)
        manifest=out/'frozen-inputs.json';dump(manifest,dict(files=files,identity=initial,binary_sha256=build['binary_sha256'],source_inputs=build['native_source_inputs_after']))
        result.update(frozen_manifest=str(manifest),frozen_sha256=sha(manifest),identity=initial)
        def verify():
            check_build(args.build_receipt)
            if identity()!=initial:raise RuntimeError('Runtime changed/restarted; no cross-restart cleanup')
            return verify_archive(dict(inputs=files))
        roots=[]
        for spec in prepared['cases']:
            verify();quiet();child_id=args.run_id+'_'+spec['mode'];validate_run_id(child_id)
            leaf=ROOT/'公共文档/openGauss原生湖存储/验证'/child_id
            if leaf.exists():raise FileExistsError('Prior child evidence exists')
            command=[sys.executable,str(LEAF),'--run-id',child_id,'--build-receipt',str(args.build_receipt.resolve()),'--mode',spec['mode'],
                     '--budget-seconds',str(args.budget_seconds),'--sql-version',args.sql_version,'--campaign-frozen',str(manifest)]
            env=dict(os.environ);env.update(CPP_WORKSPACE=str(ROOT),OG_LAKE_TEST_PROFILE='debug',PYTHONDONTWRITEBYTECODE='1')
            log=out/(spec['mode']+'.stdout');timed=False;signals=[];started=time.monotonic();result['runtime_executed']=True
            with log.open('wb')as stream:
                child=subprocess.Popen(command,stdout=stream,stderr=subprocess.STDOUT,env=env,start_new_session=True)
                try:child.wait(timeout=spec['hard_seconds'])
                except(subprocess.TimeoutExpired,KeyboardInterrupt):
                    timed=True
                    for sig,grace in [(signal.SIGTERM,5),(signal.SIGKILL,10)]:
                        if child.poll()is not None:break
                        try:os.killpg(child.pid,sig);signals.append(int(sig))
                        except ProcessLookupError:break
                        try:child.wait(timeout=grace)
                        except subprocess.TimeoutExpired:pass
            row=dict(mode=spec['mode'],command=command,python_pid=child.pid,returncode=child.returncode,hard_timeout=timed,signals_to_owned_python_group=signals,
                elapsed_seconds=round(time.monotonic()-started,3),stdout=str(log),status='fail',preserve_owned_scene=True)
            result['cases'].append(row);dump(out/'index.json',result)
            if timed:raise RuntimeError('Owned Python group timed out; preserve DB/roles/fixture and stop')
            data=json.loads((leaf/'result.json').read_text());row['result_sha256']=sha(leaf/'result.json');row['native_sqlstate']=data.get('sqlstate')or data.get('native_worker_sqlstate')
            if child.returncode!=0 or data.get('status')!='pass'or data.get('executed')is not True or data.get('mode')!=spec['mode']:raise RuntimeError('Actual AA child failed')
            if data.get('binary_sha256')!=build['binary_sha256']or data.get('source_head')!=build['source_head']or data.get('runtime_module_version')!=defaults[0]:raise RuntimeError('Leaf Native source/module/binary differs')
            if data.get('initial_installed_sql')!=spec['initial_sql']or data.get('installed_sql')!=spec['final_sql']:raise RuntimeError('Leaf actual installed SQL differs')
            if data.get('campaign_frozen')!={'path':str(manifest.resolve()),'sha256':sha(manifest)}:raise RuntimeError('Leaf shared archive binding differs')
            fixture=data['fixture_identity'];root=Path(fixture['data_root']).resolve()
            if fixture['run_id']!=child_id or any(root==p or root.is_relative_to(p)or p.is_relative_to(root)for p in roots):raise RuntimeError('Fixture ID/root reused/overlapping')
            roots.append(root);row['fixture_identity']=fixture;row['assertions']=assertions(data);row['archived_inputs']=verify_archive(dict(inputs=data['archived_inputs']))
            owner_path=leaf/'database-owner.json';owner=json.loads(owner_path.read_text())
            if owner.get('runtime_data')!=initial['data']or owner.get('owner_oid')!=initial['administrator_oid']:raise RuntimeError('Actual owned allocation administrator/runtime mismatch')
            adapted=dict(database_cleanups=[dict(attempt=str(leaf),owner=dict(path=str(owner_path),sha256=sha(owner_path),receipt=owner),cleanup=data['database_cleanup'])],role_cleanup=data['role_cleanup'])
            row['cleanup']=verify_cleanup(adapted,child_id,leaf,initial['postmaster_start'])
            verify();quiet();shutil.copy2(leaf/'result.json',out/(spec['mode']+'-result.json'))
            row.update(status='pass',preserve_owned_scene=False);dump(out/'index.json',result)
        result.update(status='pass',independent_fixture_roots=[str(p)for p in roots])
    except BaseException as exc:result.update(reason=str(exc),exception_type=type(exc).__name__,sqlstate=getattr(exc,'pgcode',None),traceback=traceback.format_exc())
    finally:
        if control is not None:control.close()
        for f in reversed(locks):f.close()
        dump(out/'index.json',result)
    return 0 if result['status']=='pass'else 1

def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--execute',action='store_true');p.add_argument('--run-id');p.add_argument('--build-receipt',type=Path)
    p.add_argument('--sql-version',choices=['0.19','0.20','0.21'],default='0.19')
    p.add_argument('--modes',nargs='+',choices=MODES,default=MODES);p.add_argument('--budget-seconds',type=int,default=90);p.add_argument('--case-timeout',type=int)
    a=p.parse_args()
    if not 30<=a.budget_seconds<=180:p.error('budget30..180 seconds')
    timeout=a.case_timeout or 300+6*a.budget_seconds+180
    if not 300+5*a.budget_seconds+120<=timeout<=3600:p.error('case timeout lacks fixture/phase/cleanup bounds')
    prepared=plan(a.modes,a.budget_seconds,timeout,a.sql_version)
    if not a.execute:print(json.dumps(prepared,ensure_ascii=False,indent=2));return 0
    if not a.run_id or not a.build_receipt:p.error('execute requires run-id and official build receipt')
    return execute(a,prepared)
if __name__=='__main__':raise SystemExit(main())
