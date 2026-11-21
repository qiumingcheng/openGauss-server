#!/usr/bin/env python3
"""Exact-build real file-query and affected functional campaign, serialized."""
import argparse
import datetime
import json
from pathlib import Path
import shutil
import subprocess
import sys
import time
import traceback

ROOT=Path('/home/qiumc/data/code/cpp');D=ROOT/'integration/opengauss-native-lake'
sys.path[:0]=[str(D),str(D/'tests')]
from file_dml_acceptance import check_build
from lake_harness import sha256,validate_run_id,write_json

def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--run-id',required=True)
    p.add_argument('--build-receipt',type=Path,required=True)
    p.add_argument('--provider-qualification',type=Path,
        help='Exact local provider qualification; required for a provider newer than the historical142b baseline')
    p.add_argument('--build-only',action='store_true');p.add_argument('--registry-index',type=Path)
    a=p.parse_args();validate_run_id(a.run_id)
    if a.build_only!=(a.registry_index is not None):p.error('--build-only requires exactly one --registry-index')
    out=ROOT/'公共文档/openGauss原生湖存储/验证'/a.run_id;out.mkdir(exist_ok=False)
    files=[Path(__file__),D/'tests/native_file_scalar_query.py',D/'tests/native_file_scalar_build.py',
        D/'tests/run_native_index_store.py',D/'tests/run_index_affected.py',D/'tests/native_table_create.py',
        D/'tests/permission_api.py',D/'native_runtime.py',D/'native_test_context.py',D/'source-lock.json']
    if a.provider_qualification:files.extend([a.provider_qualification.resolve(),D/'sdk/build_provider.py'])
    r={'status':'waiting_build','scope':'Actual scalar file candidate/fallback/visibility/rescan/lifetime plus native build and affected Debug correctness; no Ustore/DiskANN/fusion/remote/RAC/commercial qualification',
        'inputs':{str(x):sha256(x)for x in files},'cases':[]}
    if a.build_only:r['scope']='Four native scalar builds plus eleven affected Debug cases only; query/root/inline evidence supplied separately'
    archive=out/'executed-inputs';archive.mkdir()
    for x in files:shutil.copy2(x,archive/x.name)
    def save():r['updated_at']=datetime.datetime.now(datetime.timezone.utc).isoformat();write_json(out/'index.json',r)
    def command(name,args,timeout=1800):
        with(out/(name+'.log')).open('w')as log:
            return subprocess.run(list(map(str,args)),stdout=log,stderr=subprocess.STDOUT,timeout=timeout)
    def child(kind,mode,number):
        run=a.run_id+'_'+str(number)+'_'+kind+'_'+mode
        result=command(kind+'_'+mode,[sys.executable,D/'tests'/('native_file_scalar_'+kind+'.py'),
            '--run-id',run,'--build-receipt',a.build_receipt.resolve(),'--mode',mode],900)
        path=ROOT/'公共文档/openGauss原生湖存储/验证'/run/'result.json';d=json.loads(path.read_text())if path.exists()else{}
        item=dict(kind=kind,mode=mode,status=d.get('status','missing'),returncode=result.returncode,
            assertions=len(d.get('assertions',[])),result=str(path),result_sha256=sha256(path)if path.exists()else None,reason=d.get('reason'))
        r['cases'].append(item);save();print(json.dumps(item),flush=True)
        if result.returncode or d.get('status')!='pass':raise RuntimeError('First non-passing file scalar case: '+kind+'/'+mode)
        check_build(a.build_receipt)
    save()
    try:
        deadline=time.monotonic()+7200
        while not a.build_receipt.exists():
            if time.monotonic()>deadline:raise TimeoutError('Official Debug full')
            time.sleep(3)
        b=check_build(a.build_receipt);r.update(source_head=b['source_head'],binary_sha256=b['binary_sha256'],status='query_cases')
        provider=ROOT/'.install/opengauss-native-lake-debug/lib/postgresql/og_lake_sdk/libog_lake_sdk_arrow.so'
        r['provider_sha256']=sha256(provider)
        if a.provider_qualification:
            sys.path.insert(0,str(D/'sdk'))
            from build_provider import inputs as sdk_inputs,dependencies as sdk_dependencies
            qualification=a.provider_qualification.resolve();q=json.loads(qualification.read_text())
            if(q.get('status')!='passed'or q.get('library_sha256')!=r['provider_sha256']or
                q.get('provider_sources_after')!=sdk_inputs()or q.get('dependencies_after')!=sdk_dependencies()or
                q.get('object_version',{}).get('status')!='pass'):
                raise RuntimeError('Provider qualification does not identify current source-version SDK bytes')
            for item in q.get('cases',[]):
                if sha256(item['path'])!=item['sha256']or json.loads(Path(item['path']).read_text()).get('status')!='passed':
                    raise RuntimeError('Qualified provider child receipt changed')
            if not q.get('cases'):raise RuntimeError('Provider qualification has no executed cases')
            r['provider_qualification']=str(qualification);r['provider_qualification_sha256']=sha256(qualification)
        elif r['provider_sha256']!='142b59847b047bd1dd988122d36e5d09335e430ec60314f357aa0c7760413529':
            raise RuntimeError('Exact qualified source-version provider is required')
        save()
        if a.build_only:
            registry=a.registry_index.resolve();deadline=time.monotonic()+7200
            while True:
                value=json.loads(registry.read_text())if registry.exists()else{}
                if value.get('status')=='failed':raise RuntimeError('Selected registry campaign failed')
                if value.get('status')=='passed':break
                if time.monotonic()>deadline:raise TimeoutError('Waiting exact-build registry campaign')
                time.sleep(3)
            if value.get('source_head')!=b['source_head']or value.get('binary_sha256')!=b['binary_sha256']:
                raise RuntimeError('Selected registry belongs to another native build')
            r['registry_prerequisite']=str(registry);r['registry_prerequisite_sha256']=sha256(registry);save()
        else:
            for number,mode in enumerate(['predicates','visibility','fallback','lifetime'],1):child('query',mode,number)
            compatibility=a.run_id+'_compatibility'
            process=command('compatibility',[sys.executable,D/'tests/run_native_index_store.py','--run-id',compatibility,'--build-receipt',a.build_receipt.resolve()])
            registry=ROOT/'公共文档/openGauss原生湖存储/验证'/compatibility/'index.json'
            if process.returncode or json.loads(registry.read_text())['status']!='passed':raise RuntimeError('Affected root/inline compatibility failed')
            r['compatibility_index']=str(registry);r['compatibility_index_sha256']=sha256(registry);save()
        for number,mode in enumerate(['publication','transactions','guards','permissions'],5):child('build',mode,number)
        affected=a.run_id+'_affected'
        process=command('affected',[sys.executable,D/'tests/run_index_affected.py','--run-id',affected,'--build-receipt',a.build_receipt.resolve(),'--registry-index',registry])
        path=ROOT/'公共文档/openGauss原生湖存储/验证'/affected/'index.json'
        if process.returncode or json.loads(path.read_text())['status']!='passed':raise RuntimeError('Affected functional cases failed')
        r['affected_index']=str(path);r['affected_index_sha256']=sha256(path)
        if not all(sha256(x)==h for x,h in r['inputs'].items()):raise RuntimeError('Campaign inputs changed')
        if sha256(provider)!=r['provider_sha256']:raise RuntimeError('Provider changed during campaign')
        check_build(a.build_receipt);r['status']='passed'
    except BaseException as exc:r.update(status='failed',reason=str(exc),traceback=traceback.format_exc())
    save();print(json.dumps(dict(status=r['status'],reason=r.get('reason'),index=str(out/'index.json'))),flush=True)
    return 0 if r['status']=='passed'else 1

if __name__=='__main__':raise SystemExit(main())
