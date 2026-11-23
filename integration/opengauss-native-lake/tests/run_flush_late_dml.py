#!/usr/bin/env python3
"""Serial real prepared-flush/late-DML acceptance followed by timed progress."""
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
from native_flush_late_dml import MODES
from lake_harness import sha256,validate_run_id,write_json


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--run-id',required=True)
    p.add_argument('--build-receipt',type=Path,required=True);p.add_argument('--reference-result',type=Path,required=True)
    p.add_argument('--timed',action='store_true');a=p.parse_args();validate_run_id(a.run_id)
    base=ROOT/'公共文档/openGauss原生湖存储/验证';out=base/a.run_id;out.mkdir(exist_ok=False)
    paths=[Path(__file__),D/'tests/native_flush_late_dml.py',D/'tests/native_auto_workload.py',D/'tests/native_auto_flush.py',
           D/'tests/native_table_create.py',D/'tests/permission_api.py',D/'tests/empty_catalog_oracle.py',
           D/'tests/reverse_reference.py',D/'native_test_context.py',D/'source-lock.json']
    r={'status':'waiting_build','scope':'Native SQL0.11 prepared flush/late DML; current/history/position/cancel and optional120s automatic progress, no commercial claim','cases':[],'assertions':0,'inputs':{str(p):sha256(p)for p in paths}}
    archive=out/'executed-inputs';archive.mkdir()
    for p in paths:shutil.copy2(p,archive/p.name)
    def save():r['updated_at']=datetime.datetime.now(datetime.timezone.utc).isoformat();write_json(out/'index.json',r)
    save()
    try:
        deadline=time.monotonic()+1200
        while not a.build_receipt.exists()or not a.reference_result.exists():
            if time.monotonic()>deadline:raise TimeoutError('Waiting official build/reference refresh')
            time.sleep(3)
        b=check_build(a.build_receipt);ref=json.loads(a.reference_result.read_text());assert ref['status']=='passed'
        assert ref['source_lock_hash']==sha256(D/'source-lock.json')
        r.update(status='running',source_head=b['source_head'],binary_sha256=b['binary_sha256']);save()
        jobs=[(mode,D/'tests/native_flush_late_dml.py',['--mode',mode])for mode in MODES]
        if a.timed:jobs.append(('timed_on',D/'tests/native_auto_workload.py',['--maintenance','on','--seconds','120']))
        for i,(mode,script,args)in enumerate(jobs,1):
            if not all(sha256(p)==h for p,h in r['inputs'].items()):raise RuntimeError('Frozen campaign input changed')
            run=a.run_id+'_'+str(i)+'_'+mode
            with (out/(str(i)+'-'+mode+'.log')).open('w')as log:
                process=subprocess.run([sys.executable,str(script),'--run-id',run,'--build-receipt',str(a.build_receipt.resolve()),*args],stdout=log,stderr=subprocess.STDOUT,timeout=900)
            path=base/run/'result.json';v=json.loads(path.read_text())if path.exists()else{}
            item={'mode':mode,'returncode':process.returncode,'status':v.get('status','missing'),'assertions':len(v.get('assertions',[])),
                  'reason':v.get('reason'),'result':str(path),'result_sha256':sha256(path)if path.exists()else None}
            r['cases'].append(item);r['assertions']+=item['assertions'];save();print(json.dumps(item,ensure_ascii=False),flush=True)
            if process.returncode or v.get('status')!='pass':raise RuntimeError('Real late-DML acceptance failed: '+mode)
            check_build(a.build_receipt)
        if not all(sha256(p)==h for p,h in r['inputs'].items()):raise RuntimeError('Frozen campaign inputs changed')
        r['status']='passed'
    except BaseException as exc:r.update(status='failed',reason=str(exc),traceback=traceback.format_exc())
    save();print(json.dumps({'status':r['status'],'assertions':r['assertions'],'reason':r.get('reason'),'index':str(out/'index.json')},ensure_ascii=False))
    return 0 if r['status']=='passed'else 1

if __name__=='__main__':raise SystemExit(main())
