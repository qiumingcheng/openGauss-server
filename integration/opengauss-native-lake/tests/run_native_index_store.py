#!/usr/bin/env python3
"""Serialize exact-build root and affected SQL0.16/legacy correctness cases."""
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

JOBS=[
    ('root_configuration','native_index_root.py',['--mode','configuration']),
    ('root_upgrade','native_index_root.py',['--mode','upgrade']),
    ('root_permissions','native_index_root.py',['--mode','permissions']),
    ('root_concurrency','native_index_root.py',['--mode','concurrency']),
    ('current_native_index_files','native_inline_btree_array.py',['--sql-version','0.16']),
    ('retained_inline_null','native_inline_btree_null.py',[]),
    ('retained_registry','native_index_registry.py',['--mode','definition']),
    ('retained_registry_upgrade','native_index_registry.py',['--mode','upgrade']),
    ('retained_registry_tamper','native_index_registry.py',['--mode','tamper']),
]

def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--run-id',required=True)
    p.add_argument('--build-receipt',type=Path,required=True);p.add_argument('--wait-build',action='store_true')
    p.add_argument('--only',action='append',choices=[x[0]for x in JOBS]);a=p.parse_args();validate_run_id(a.run_id)
    out=ROOT/'公共文档/openGauss原生湖存储/验证'/a.run_id;out.mkdir(exist_ok=False)
    r={'status':'waiting_build'if a.wait_build else'preparing','cases':[],'assertions':0,
       'scope':'SQL0.16 independent root, actual heap-index/file fallback and retained registry only; no native file-index backend/READY/query or production qualification'}
    paths=[Path(__file__),D/'tests/native_table_create.py',D/'native_test_context.py',D/'tests/permission_api.py',D/'source-lock.json']
    paths += [D/'tests'/name for name in sorted({x[1]for x in JOBS})]
    r['inputs']={str(path):sha256(path)for path in paths};archive=out/'executed-inputs';archive.mkdir()
    for path in paths:shutil.copy2(path,archive/path.name)
    def save():r['updated_at']=datetime.datetime.now(datetime.timezone.utc).isoformat();write_json(out/'index.json',r)
    save()
    try:
        if a.wait_build:
            deadline=time.monotonic()+7200
            while not a.build_receipt.exists():
                if time.monotonic()>deadline:raise TimeoutError('Official native Debug full')
                time.sleep(3)
        b=check_build(a.build_receipt);r.update(status='running',source_head=b['source_head'],binary_sha256=b['binary_sha256']);save()
        for number,(mode,script,args)in enumerate([x for x in JOBS if not a.only or x[0]in a.only],1):
            if not all(sha256(Path(path))==h for path,h in r['inputs'].items()):raise RuntimeError('Frozen native store inputs changed')
            run=a.run_id+'_'+str(number)+'_'+mode
            with(out/(str(number)+'-'+mode+'.log')).open('w')as log:
                process=subprocess.run([sys.executable,str(D/'tests'/script),'--run-id',run,'--build-receipt',str(a.build_receipt.resolve())]+args,
                                       stdout=log,stderr=subprocess.STDOUT,timeout=600)
            path=ROOT/'公共文档/openGauss原生湖存储/验证'/run/'result.json';value=json.loads(path.read_text())if path.exists()else{}
            item={'mode':mode,'status':value.get('status','missing'),'returncode':process.returncode,
                  'assertions':len(value.get('assertions',[])),'reason':value.get('reason'),
                  'result':str(path),'result_sha256':sha256(path)if path.exists()else None}
            r['cases'].append(item);r['assertions']+=item['assertions'];save();print(json.dumps(item,ensure_ascii=False),flush=True)
            if process.returncode or value.get('status')!='pass':raise RuntimeError('Index store case failed: '+mode)
            check_build(a.build_receipt)
        if not all(sha256(Path(path))==h for path,h in r['inputs'].items()):raise RuntimeError('Native store inputs changed')
        r['status']='passed'
    except BaseException as exc:r.update(status='failed',reason=str(exc),traceback=traceback.format_exc())
    save();print(json.dumps({'status':r['status'],'assertions':r['assertions'],'reason':r.get('reason'),'index':str(out/'index.json')},ensure_ascii=False))
    return 0 if r['status']=='passed'else 1

if __name__=='__main__':raise SystemExit(main())
