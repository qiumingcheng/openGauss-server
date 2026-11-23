#!/usr/bin/env python3
"""Real flush component regression, optional crash recovery and default workload.

The 23 component inputs retain genuine public DML/Parquet reference validation.
Late-DML cases now require exact safe merge; old-locator rejection stays tested.
"""
import argparse
import datetime
import json
from pathlib import Path
import shutil
import subprocess
import sys
import traceback
ROOT=Path('/home/qiumc/data/code/cpp');D=ROOT/'integration/opengauss-native-lake'
sys.path[:0]=[str(D),str(D/'tests')]
from file_dml_acceptance import check_build
from lake_harness import sha256,validate_run_id,write_json


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--run-id',required=True)
    p.add_argument('--build-receipt',required=True,type=Path);p.add_argument('--with-crash',action='store_true');p.add_argument('--default-workload',action='store_true')
    a=p.parse_args();validate_run_id(a.run_id);base=ROOT/'公共文档/openGauss原生湖存储/验证';out=base/a.run_id;out.mkdir(exist_ok=False)
    paths=sorted(set(list(D.glob('*.py'))+list((D/'tests').glob('*.py'))+[D/'source-lock.json']))
    r={'status':'preparing','scope':'23 actual affected flush components, optional3 local SIGKILL recovery and120s default scheduling. Not commercial/Release/remote/RAC/GC acceptance.','cases':[],'assertions':0,'inputs':{str(p):sha256(p)for p in paths}}
    archive=out/'executed-inputs';archive.mkdir()
    for p in paths:
        target=archive/p.relative_to(D);target.parent.mkdir(exist_ok=True,parents=True);shutil.copy2(p,target)
    def save():r['updated_at']=datetime.datetime.now(datetime.timezone.utc).isoformat();write_json(out/'index.json',r)
    def mode(script,mode):return(D/'tests'/script,['--mode',mode])
    inline=['history','typed','old_writer','concurrent_insert','rollback','savepoints','publish_cancel','repeat_file_dml']
    file=['only','replace_history','mixed','rollback','cancel','concurrent_insert','nonzero_row_ids']
    jobs=[('data_concurrent_update',*mode('native_flush_inline.py','concurrent_update')),
          ('file_deletes_concurrent_delete',*mode('native_flush_file_deletes.py','concurrent_delete'))]
    jobs += [('data_'+m,*mode('native_flush_inline.py',m))for m in inline]
    jobs += [('file_deletes_'+m,*mode('native_flush_file_deletes.py',m))for m in file]
    jobs += [('inline_batches_'+m,*mode('native_inline_publish_batches.py',m))for m in ['savepoints','cancel']]
    jobs += [('partial_large_versions',D/'tests/native_flush_partial.py',['--expect','partial']),
             ('partial_file_populations',D/'tests/native_flush_partial_file_deletes.py',['--expect','partial']),
             ('failed_prepare_memory',D/'tests/native_flush_failed_prepare_memory.py',[]),
             ('cancel_after_inline_delete',D/'tests/native_flush_cancel_after_delete.py',[])]
    assert len(jobs)==23
    if a.with_crash:jobs += [('crash_'+m,*mode('native_flush_crash_recovery.py',m))for m in ['prepared','publishing','committed']]
    if a.default_workload:jobs.append(('default_workload',D/'tests/native_auto_workload.py',['--maintenance','on','--schedule','default','--seconds','120']))
    r['planned_cases']=[name for name, _, _ in jobs];save()
    try:
        b=check_build(a.build_receipt);r.update(status='running',source_head=b['source_head'],binary_sha256=b['binary_sha256']);save()
        for number,(name,script,args)in enumerate(jobs,1):
            if not all(sha256(p)==h for p,h in r['inputs'].items()):raise RuntimeError('Frozen component input changed')
            run=a.run_id+'_'+str(number)+'_'+name
            with (out/(str(number)+'-'+name+'.log')).open('w')as log:
                process=subprocess.run([sys.executable,str(script),'--run-id',run,'--build-receipt',str(a.build_receipt.resolve()),*args],stdout=log,stderr=subprocess.STDOUT,timeout=3600)
            target=base/run/'result.json';v=json.loads(target.read_text())if target.exists()else{}
            item={'name':name,'returncode':process.returncode,'status':v.get('status','missing'),'assertions':len(v.get('assertions',[])),
                  'reason':v.get('reason'),'result':str(target),'result_sha256':sha256(target)if target.exists()else None}
            r['cases'].append(item);r['assertions']+=item['assertions'];save();print(json.dumps(item,ensure_ascii=False),flush=True)
            if process.returncode or v.get('status')!='pass' or any(x['status']!='pass'for x in v.get('assertions',[])):raise RuntimeError('Actual component failed: '+name)
            if v['source_head']!=r['source_head']or v['binary_sha256']!=r['binary_sha256']:raise RuntimeError('Component binary/source mismatch')
            check_build(a.build_receipt)
        if not all(sha256(p)==h for p,h in r['inputs'].items()):raise RuntimeError('Component inputs changed')
        r['status']='passed'
    except BaseException as exc:r.update(status='failed',reason=str(exc),traceback=traceback.format_exc())
    save();print(json.dumps({'status':r['status'],'assertions':r['assertions'],'reason':r.get('reason'),'index':str(out/'index.json')},ensure_ascii=False))
    return 0 if r['status']=='passed'else 1

if __name__=='__main__':raise SystemExit(main())
