#!/usr/bin/env python3
"""Serialize genuine Ustore index cases and affected heap/file correctness."""
import argparse
import datetime
import hashlib
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
    p.add_argument('--provider-qualification',type=Path);a=p.parse_args();validate_run_id(a.run_id)
    out=ROOT/'公共文档/openGauss原生湖存储/验证'/a.run_id;out.mkdir(exist_ok=False)
    paths=[Path(__file__),D/'tests/native_inline_ubtree.py',D/'tests/ustore_fixture.py',
        D/'tests/permission_api.py',D/'tests/run_scalar_file_query.py',D/'source-lock.json']
    if a.provider_qualification:paths.append(a.provider_qualification.resolve())
    r={'status':'waiting_build','scope':'Native Ustore UB-tree plus affected heap/file/root/build Debug correctness only; no DiskANN/fusion/remote/RAC/commercial qualification',
       'inputs':{str(x):sha256(x)for x in paths},'cases':[]}
    archive=out/'executed-inputs';archive.mkdir()
    for x in paths:shutil.copy2(x,archive/x.name)
    def save():
        r['updated_at']=datetime.datetime.now(datetime.timezone.utc).isoformat();write_json(out/'index.json',r)
    def frozen():
        if not all(sha256(Path(x))==h for x,h in r['inputs'].items()):raise RuntimeError('Frozen campaign inputs changed')
        return check_build(a.build_receipt)
    def child_name(suffix,budget):
        value=a.run_id+suffix
        return value if len(value)<=budget else'ubt_'+hashlib.sha256(value.encode()).hexdigest()[:20]
    save()
    try:
        deadline=time.monotonic()+7200
        while not a.build_receipt.exists():
            if time.monotonic()>deadline:raise TimeoutError('Official Debug incremental')
            time.sleep(3)
        b=frozen();r.update(source_head=b['source_head'],binary_sha256=b['binary_sha256'],status='ustore_cases');save()
        for number,mode in enumerate(['old_build_transfer','old_snapshot','integer','bigint','rescan','transactions','long_text'],1):
            frozen();run=child_name('_'+str(number)+'_'+mode,64)
            with(out/(str(number)+'-'+mode+'.log')).open('w')as log:
                child=subprocess.run([sys.executable,str(D/'tests/native_inline_ubtree.py'),'--run-id',run,
                    '--build-receipt',str(a.build_receipt.resolve()),'--mode',mode],stdout=log,stderr=subprocess.STDOUT,timeout=900)
            path=ROOT/'公共文档/openGauss原生湖存储/验证'/run/'result.json';v=json.loads(path.read_text())if path.exists()else{}
            item=dict(mode=mode,status=v.get('status','missing'),returncode=child.returncode,assertions=len(v.get('assertions',[])),
                reason=v.get('reason'),result=str(path),result_sha256=sha256(path)if path.exists()else None)
            r['cases'].append(item);save();print(json.dumps(item,ensure_ascii=False),flush=True)
            if child.returncode or v.get('status')!='pass':raise RuntimeError('First failed Ustore case: '+mode)
        frozen();r['status']='affected_cases';save()
        run=child_name('_affected',24);r['affected_run_id']=run;save()
        with(out/'affected.log').open('w')as log:
            child=subprocess.run([sys.executable,str(D/'tests/run_scalar_file_query.py'),'--run-id',run,
                '--build-receipt',str(a.build_receipt.resolve())]+(['--provider-qualification',str(a.provider_qualification.resolve())]if a.provider_qualification else[]),stdout=log,stderr=subprocess.STDOUT,timeout=3600)
        path=ROOT/'公共文档/openGauss原生湖存储/验证'/run/'index.json'
        if child.returncode or not path.is_file()or json.loads(path.read_text()).get('status')!='passed':
            raise RuntimeError('Affected heap/file/root/build cases failed')
        r['affected_index']=str(path);r['affected_index_sha256']=sha256(path)
        frozen();r['status']='passed'
    except BaseException as exc:r.update(status='failed',reason=str(exc),traceback=traceback.format_exc())
    save();print(json.dumps(dict(status=r['status'],reason=r.get('reason'),index=str(out/'index.json')),ensure_ascii=False),flush=True)
    return 0 if r['status']=='passed'else 1

if __name__=='__main__':raise SystemExit(main())
