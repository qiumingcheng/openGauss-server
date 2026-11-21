#!/usr/bin/env python3
"""Explicit serial N7 qualification campaign; default prepares only.

Never builds/starts a service. --execute requires one successful official Debug
source/binary receipt. Each leaf runs in its own owned test process group;
timeout kills only that group, preserves the database/roles, and stops. A root
coordinator must not concurrently run GC/COPY/Boolean on the same runtime.
"""
import argparse,ast,fcntl,hashlib,json,os
from pathlib import Path
import shutil,signal,subprocess,sys,time,traceback
ROOT=Path(os.getenv('CPP_WORKSPACE','/home/qiumc/data/code/cpp')).resolve();D=ROOT/'integration/opengauss-native-lake'
sys.path[:0]=[str(D),str(D/'tests')]
from file_dml_acceptance import check_build
from inline_fixture import query,scalar
from lake_harness import sha256,validate_run_id,write_json
from native_auto_flush import connect_control
from native_runtime import PROFILE,RUNTIME,SOURCE,INSTALL
HERE=Path(__file__).resolve().parent
CASES=[
 dict(id='smp-first',group='smp',script='native_smp_scan_qualification.py',args=['--sql-version','0.18','--_isolated-child'],seconds=900,
      scope='real DOP2/4; files/inline0/multirowgroup/null/duplicates/deletes; native aggregate/filter; static params/GUC; fixed S_stmt under UPDATE+flush; live SDK cancel; worker/partial-start error; LIMIT; cached on-off; joins/recursive/rescan/own-writes explicit serial or prestart refusal'),
 dict(id='stats-small',group='stats',script='native_file_analyze.py',args=['--mode','small'],seconds=600,scope='p1 complete small mixed history+delete source; independent Parquet position/sample and pg_stats oracle'),
 *[dict(id='stats-sampling-'+str(i),group='stats',script='native_file_analyze.py',args=['--mode','sampling'],seconds=600,scope='independent random-seed position/group oracle and native sample statistics; not general variance qualification')for i in range(1,4)],
 dict(id='stats-refusal',group='stats',script='native_file_analyze.py',args=['--mode','refusal'],seconds=600,scope='insufficient visible survivors fail55000 and retain old pg_class/pg_statistic'),
 *[dict(id='stats-'+mode,group='stats',script='native_file_analyze_edges.py',args=['--mode',mode],seconds=900 if mode=='precision' else 360,scope={'empty':'exact zero live count with nonempty physical sources; CBO live/physical separation','null_extremes':'all-NULL and int32-extreme MCV/full histogram comparison','admission':'zero target and persistent transaction guards preserve stats','precision':'oversized requested precision fails54000 before statistics publication','mvcc':'paused complete-sample snapshot survives committed UPDATE/DELETE','cancel':'real sampled-row-held cancel; old statistics and same-backend reader reuse'}[mode])for mode in ['empty','null_extremes','admission','precision','mvcc','cancel']]
]


def safe_relative(name):
    p=Path(name)
    if p.is_absolute()or'..'in p.parts:raise RuntimeError('Unsafe input path '+name)
    return p


def prepare():
    for case in CASES:
        source=HERE/case['script'];ast.parse(source.read_text());case['script_sha256']=sha256(source)
    return dict(status='prepared_not_executed',strict_serial=True,cases=CASES,
        excluded_root_groups=['GC physical/row-head maintenance acceptance','COPY','complex Boolean fusion'],
        unsupported_as_parallel=['joins','parameterized rescans','recursive input','own uncommitted lake writes','DML/maintenance','vector batch output'],
        pending_design_cases=['genuine zero-row Parquet artifact import (empty-table/zero-visible inputs covered separately)','SDK transport failure during an active worker batch (PG worker error and missing-source prelaunch covered)','prototype source-count/delete-budget refusal scale gates'],
        remaining_qualification=['actual OS thread/resource exhaustion','Native ASan/TSan long stress','remote object store and RAC','all-type/schema evolution','autoanalyze scheduling','calibrated CBO/performance','commercial readiness'])


def freeze_inputs(out,receipt):
    frozen=out/'frozen';frozen.mkdir();files={}
    # Shared immutable archive is copied once; every leaf records and rechecks
    # this entire map, rather than recopying a large Debug binary per case.
    def add(source,destination):
        source=Path(source).resolve()
        if not source.is_file():raise RuntimeError('Missing frozen input '+str(source))
        target=frozen/destination;target.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(source,target)
        h=sha256(source)
        if sha256(target)!=h:raise RuntimeError('Freeze copy mismatch '+str(source))
        files[str(source)]=dict(sha256=h,archive=str(target.relative_to(out)));return h
    add(receipt,'official-build.json');build=check_build(receipt)
    for name in build['native_source_inputs_after']:add(SOURCE/safe_relative(name),Path('native-source')/safe_relative(name))
    for path in sorted(D.rglob('*')):
        if path.is_file()and'__pycache__'not in path.parts and'state'not in path.relative_to(D).parts and (path.suffix in {'.py','.sh','.sql','.cpp','.h','.hpp','.control','.inc'}or path.name=='source-lock.json'):
            add(path,Path('integration')/path.relative_to(D))
    for path in [Path(__file__),*(HERE/c['script']for c in CASES)]:add(path,Path('qualification')/path.name)
    add(INSTALL/'bin/gaussdb','binary/gaussdb')
    add(INSTALL/'lib/postgresql/og_lake_sdk/libog_lake_sdk_arrow.so','binary/libog_lake_sdk_arrow.so')
    ref=ROOT/'.reference/opengauss-native-lake/build/receipt.json';add(ref,'reference/build.json');rb=json.loads(ref.read_text());add(Path(rb['binary']),'reference/duckdb')
    meta=dict(native_binary_sha256=build['binary_sha256'],native_input_count=len(build['native_source_inputs_after']),files=files)
    write_json(out/'frozen-inputs.json',meta);return meta


def verify_inputs(meta,receipt,out):
    check_build(receipt)
    for source,item in meta['files'].items():
        if sha256(source)!=item['sha256']or sha256(out/item['archive'])!=item['sha256']:raise RuntimeError('Input/archive changed: '+source)
    return dict(verified=True,files=len(meta['files']),binary_sha256=meta['native_binary_sha256'],native_source_input_count=meta['native_input_count'],frozen_manifest_sha256=sha256(out/'frozen-inputs.json'))


def no_other_fixture(control):
    rows=query(control,"SELECT datname,pid,application_name,state FROM pg_stat_activity WHERE datname LIKE 'lake_inline_%%' AND pid<>pg_backend_pid() ORDER BY datname,pid")
    if rows:raise RuntimeError('Another owned fixture or retained scene is connected: '+repr(rows))


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--execute',action='store_true');p.add_argument('--run-id');p.add_argument('--build-receipt',type=Path);p.add_argument('--group',choices=['all','smp','stats'],default='all');p.add_argument('--cases',nargs='+',choices=[case['id']for case in CASES]);a=p.parse_args()
    plan=prepare()
    eligible=[case['id']for case in CASES if a.group=='all'or case['group']==a.group]
    if a.cases and(len(a.cases)!=len(set(a.cases))or not set(a.cases)<=set(eligible)):
        p.error('--cases must be unique cases in the chosen group')
    selected=[name for name in eligible if not a.cases or name in a.cases]
    plan['selected_cases']=selected
    plan['selection_preserves_canonical_order']=True
    if not a.execute:
        print(json.dumps(plan,indent=2));return 0
    if not a.run_id or not a.build_receipt:p.error('--execute needs --run-id and --build-receipt')
    validate_run_id(a.run_id)
    if len(a.run_id)>64:p.error('Campaign run-id must leave room for unique case suffixes (max64 chars)')
    if PROFILE!='debug':p.error('Only exact owned Debug profile admitted')
    out=ROOT/'公共文档/openGauss原生湖存储/验证'/a.run_id;out.mkdir(exist_ok=False);result=dict(status='fail',scope=__doc__,cases=[],plan=plan,strict_serial=True)
    lock=control=None
    try:
        lock=(RUNTIME/'.n7-qualification.lock').open('a+');fcntl.flock(lock.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB)
        build=check_build(a.build_receipt)
        required={'contrib/og_lake/lake_sample.cpp','contrib/og_lake/lake_stream.cpp'}
        if not required<=set(build['native_source_inputs_after']):raise RuntimeError('Official receipt lacks real sampler/Stream modules')
        control=connect_control();query(control,'SET session_timeout=0');query(control,'SET statement_timeout=30000')
        if scalar(control,'SHOW enable_lake_maintenance')!='off':raise RuntimeError('Controlled qualification requires automatic maintenance off')
        if scalar(control,'SHOW enable_thread_pool')!='off':raise RuntimeError('Controlled PID gates require ordinary backend mode')
        no_other_fixture(control);meta=freeze_inputs(out,a.build_receipt)
        for spec in CASES:
            if spec['id']not in selected:continue
            before=verify_inputs(meta,a.build_receipt,out);no_other_fixture(control)
            child_id=a.run_id+'_'+spec['id'].replace('-','_');validate_run_id(child_id)
            leaf=ROOT/'公共文档/openGauss原生湖存储/验证'/child_id
            if leaf.exists():raise FileExistsError('Refusing to overwrite prior case '+str(leaf))
            command=[sys.executable,str(HERE/spec['script']),'--run-id',child_id,'--build-receipt',str(a.build_receipt.resolve()),*spec['args']]
            env=dict(os.environ);env['CPP_WORKSPACE']=str(ROOT);env['OG_LAKE_SMP_ISOLATED_CHILD_PARENT']=str(os.getpid());logpath=out/(spec['id']+'.stdout');started=time.monotonic();timeout=False
            with logpath.open('w')as stream:
                child=subprocess.Popen(command,stdout=stream,stderr=subprocess.STDOUT,env=env,start_new_session=True)
                try:child.wait(timeout=spec['seconds'])
                except (subprocess.TimeoutExpired,KeyboardInterrupt):
                    timeout=True
                    try:os.killpg(child.pid,signal.SIGKILL)
                    except ProcessLookupError:pass
                    try:child.wait(timeout=10)
                    except subprocess.TimeoutExpired:pass
            row=dict(id=spec['id'],command=command,process_id=child.pid,returncode=child.returncode,hard_timeout=timeout,seconds=round(time.monotonic()-started,3),source_binary_before=before,result=str(leaf/'result.json'),status='fail',automatic_controller_database_cleanup=False)
            result['cases'].append(row);write_json(out/'index.json',result)
            if timeout:row['preserve_owned_scene']=True;raise RuntimeError('Case timeout; exact owned scene retained: '+child_id)
            data=json.loads((leaf/'result.json').read_text());row['leaf_result_sha256']=sha256(leaf/'result.json')
            shutil.copy2(leaf/'result.json',out/(spec['id']+'-result.json'))
            row['source_binary_after']=verify_inputs(meta,a.build_receipt,out)
            row['frozen_evidence_manifest']=str(out/'frozen-inputs.json')
            write_json(leaf/'campaign-frozen-reference.json',dict(campaign=str(out/'index.json'),frozen_manifest=str(out/'frozen-inputs.json'),frozen_manifest_sha256=sha256(out/'frozen-inputs.json'),source_binary_before=row['source_binary_before'],source_binary_after=row['source_binary_after']))
            row['assertions']=len(data.get('assertions',[]))
            if child.returncode or data.get('status')!='pass'or not data.get('assertions')or any(x.get('status')!='pass'for x in data['assertions']):raise RuntimeError('Case failed: '+spec['id'])
            if data.get('preserve_owned_scene')or data.get('database_cleanup',{}).get('returncode')!=0 or data.get('role_cleanup',{}).get('status')!='pass':raise RuntimeError('Owned cleanup is incomplete: '+spec['id'])
            no_other_fixture(control);row['status']='pass';write_json(out/'index.json',result)
        result['status']='pass';result['completed_scope']='Only selected N7 groups and exact cases listed; remaining plan limitations retained'
    except BaseException as exc:result.update(reason=str(exc),traceback=traceback.format_exc())
    finally:
        if control is not None:control.close()
        if lock is not None:lock.close()
        write_json(out/'index.json',result)
    print(json.dumps(dict(status=result['status'],cases=len(result['cases']),reason=result.get('reason'),index=str(out/'index.json'))));return 0 if result['status']=='pass'else 1


if __name__=='__main__':raise SystemExit(main())
