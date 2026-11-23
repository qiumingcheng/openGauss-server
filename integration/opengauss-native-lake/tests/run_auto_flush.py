#!/usr/bin/env python3
"""Serialize real automatic maintenance and directly affected manual regressions."""
import argparse
import datetime
import json
from pathlib import Path
import shutil
import subprocess
import sys
import time
import traceback

ROOT = Path('/home/qiumc/data/code/cpp')
D = ROOT / 'integration/opengauss-native-lake'
sys.path[:0] = [str(D), str(D / 'tests')]
from lake_harness import sha256, validate_run_id, write_json
from file_dml_acceptance import check_build


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--run-id', required=True)
    p.add_argument('--build-receipt', required=True, type=Path)
    p.add_argument('--wait-build', action='store_true')
    p.add_argument('--auto', action='append', choices=['rows', 'age', 'cancel', 'fairness', 'shutdown', 'cross_database', 'budget'])
    p.add_argument('--manual', action='append', choices=['history', 'concurrent_update', 'repeat_file_dml'])
    a = p.parse_args(); validate_run_id(a.run_id)
    out = ROOT / '公共文档/openGauss原生湖存储/验证' / a.run_id
    out.mkdir(exist_ok=False)
    result = {'status': 'waiting_build' if a.wait_build else 'preparing', 'cases': [], 'assertions': 0,
              'scope': 'Scoped real native automatic flush and affected manual behavior',
              'limitations': ['No cross-instance target-RAC, physical GC, backup/PITR, long soak or private remote SDK qualification.']}
    inputs = [Path(__file__), D/'tests/native_auto_flush.py', D/'tests/native_flush_inline.py',
              D/'tests/native_table_create.py', D/'native_test_context.py', D/'source-lock.json']
    result['inputs'] = {str(x):sha256(x) for x in inputs}
    archive = out/'executed-inputs'; archive.mkdir()
    for x in inputs: shutil.copy2(x, archive/x.name)
    def save():
        result['updated_at'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        write_json(out/'index.json', result)
    save()
    try:
        if a.wait_build:
            deadline = time.monotonic()+900
            while not a.build_receipt.exists():
                if time.monotonic() > deadline: raise TimeoutError('Waiting official build receipt')
                time.sleep(3)
        built = check_build(a.build_receipt)
        result.update(status='running', source_head=built['source_head'], binary_sha256=built['binary_sha256'],
                      build_receipt=str(a.build_receipt.resolve()), build_receipt_sha256=sha256(a.build_receipt))
        save()
        jobs = [('automatic',mode,'native_auto_flush.py') for mode in (a.auto or ['rows','age','cancel','fairness'])]
        jobs += [('manual',mode,'native_flush_inline.py') for mode in (a.manual or [])]
        for number,(kind,mode,script) in enumerate(jobs,1):
            if not all(sha256(p)==h for p,h in result['inputs'].items()):
                raise RuntimeError('Qualification inputs changed before '+mode)
            check_build(a.build_receipt)
            run = a.run_id + '_' + str(number) + '_' + mode
            child = ROOT/'公共文档/openGauss原生湖存储/验证'/run/'result.json'
            command = [sys.executable,str(D/'tests'/script),'--run-id',run,'--mode',mode,
                       '--build-receipt',str(a.build_receipt.resolve())]
            with (out/(str(number)+'-'+mode+'.log')).open('w') as log:
                completed = subprocess.run(command,stdout=log,stderr=subprocess.STDOUT,timeout=600)
            observed = json.loads(child.read_text()) if child.exists() else {}
            item = {'kind':kind,'mode':mode,'returncode':completed.returncode,'result':str(child),
                    'status':observed.get('status','missing'),'assertions':len(observed.get('assertions',[])),
                    'reason':observed.get('reason'),'result_sha256':sha256(child) if child.exists() else None}
            result['cases'].append(item); result['assertions'] += item['assertions']; save()
            print(json.dumps(item,ensure_ascii=False),flush=True)
            if completed.returncode or observed.get('status') != 'pass' or any(x['status']!='pass' for x in observed.get('assertions',[])):
                raise RuntimeError('Real native qualification failed: '+kind+'/'+mode)
            if not all(sha256(p)==h for p,h in result['inputs'].items()):
                raise RuntimeError('Qualification inputs changed during '+mode)
        check_build(a.build_receipt)
        result['status'] = 'passed'
    except BaseException as exc:
        result.update(status='failed',reason=str(exc),traceback=traceback.format_exc())
    save()
    print(json.dumps({'status':result['status'],'cases':len(result['cases']),'assertions':result['assertions'],
                      'reason':result.get('reason'),'index':str(out/'index.json')},ensure_ascii=False),flush=True)
    return 0 if result['status']=='passed' else 1

if __name__ == '__main__': raise SystemExit(main())
