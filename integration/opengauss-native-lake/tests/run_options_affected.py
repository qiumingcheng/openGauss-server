#!/usr/bin/env python3
"""Affected native functional regressions after persistent option admission changes."""
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
JOBS = [
    ('late_mixed', 'native_flush_late_dml.py', 'mixed_sources'),
    ('late_cancel', 'native_flush_late_dml.py', 'cancel'),
    ('file_delete', 'native_flush_file_deletes.py', 'only'),
    ('legacy_rc', 'native_rc_row_conflicts.py', 'legacy_inline_disjoint'),
    ('auto_rows', 'native_auto_flush.py', 'rows'),
]
from file_dml_acceptance import check_build
from lake_harness import sha256,validate_run_id,write_json


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--run-id',required=True)
    p.add_argument('--build-receipt',type=Path,required=True);p.add_argument('--wait-build',action='store_true')
    p.add_argument('--options-index',type=Path,required=True);a=p.parse_args();validate_run_id(a.run_id)
    out=ROOT/'公共文档/openGauss原生湖存储/验证'/a.run_id;out.mkdir(exist_ok=False)
    r={'status':'waiting_build'if a.wait_build else'preparing','cases':[],'assertions':0,
       'scope':'Affected Debug functional regressions; no commercial gate'}
    paths=[Path(__file__),D/'tests/native_table_create.py',D/'native_test_context.py',D/'tests/permission_api.py',D/'native_runtime.py',D/'source-lock.json'] + [D/'tests'/name for _,name,_ in JOBS]
    r['inputs']={str(p):sha256(p)for p in paths};archive=out/'executed-inputs';archive.mkdir()
    for p in paths:shutil.copy2(p,archive/p.name)
    def save():r['updated_at']=datetime.datetime.now(datetime.timezone.utc).isoformat();write_json(out/'index.json',r)
    save()
    try:
        if a.wait_build:
            deadline=time.monotonic()+7200
            while not a.build_receipt.exists():
                if time.monotonic()>deadline:raise TimeoutError('Waiting official RC row merge build')
                time.sleep(3)
        b=check_build(a.build_receipt)
        deadline=time.monotonic()+7200
        while True:
            v=json.loads(a.options_index.read_text()) if a.options_index.exists() else {}
            if v.get('status')=='passed':break
            if v.get('status')=='failed':raise RuntimeError('Configuration cases failed; affected campaign not started')
            if time.monotonic()>deadline:raise TimeoutError('Waiting configuration cases')
            time.sleep(3)
        r.update(status='running',source_head=b['source_head'],binary_sha256=b['binary_sha256']);save()
        for number,(mode,script,case_mode) in enumerate(JOBS,1):
            if not all(sha256(p)==h for p,h in r['inputs'].items()):raise RuntimeError('Frozen acceptance inputs changed')
            run=a.run_id+'_'+str(number)+'_'+mode
            with (out/(str(number)+'-'+mode+'.log')).open('w')as log:
                process=subprocess.run([sys.executable,str(D/'tests'/script),'--run-id',run,'--mode',case_mode,
                    '--build-receipt',str(a.build_receipt.resolve())],stdout=log,stderr=subprocess.STDOUT,timeout=600)
            path=ROOT/'公共文档/openGauss原生湖存储/验证'/run/'result.json';v=json.loads(path.read_text())if path.exists()else{}
            item={'mode':mode,'status':v.get('status','missing'),'returncode':process.returncode,
                  'assertions':len(v.get('assertions',[])),'outcome':v.get('publication_outcome'),
                  'reason':v.get('reason'),'result':str(path),'result_sha256':sha256(path)if path.exists()else None}
            r['cases'].append(item);r['assertions']+=item['assertions'];save();print(json.dumps(item,ensure_ascii=False),flush=True)
            if process.returncode or v.get('status')!='pass':raise RuntimeError('Affected native case failed: '+mode)
            check_build(a.build_receipt)
        if not all(sha256(p)==h for p,h in r['inputs'].items()):raise RuntimeError('Acceptance inputs changed')
        r['status']='passed'
    except BaseException as exc:r.update(status='failed',reason=str(exc),traceback=traceback.format_exc())
    save();print(json.dumps({'status':r['status'],'assertions':r['assertions'],'reason':r.get('reason'),'index':str(out/'index.json')},ensure_ascii=False))
    return 0 if r['status']=='passed'else 1

if __name__=='__main__':raise SystemExit(main())
