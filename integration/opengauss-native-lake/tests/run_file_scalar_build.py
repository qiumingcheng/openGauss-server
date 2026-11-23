#!/usr/bin/env python3
"""Serialize old-provider compatibility, owned SDK switch and real file builds."""
import argparse
import datetime
import json
import os
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
    p.add_argument('--build-receipt',type=Path,required=True);p.add_argument('--qualification',type=Path,required=True)
    p.add_argument('--legacy-index',type=Path);p.add_argument('--installed-switch',type=Path)
    a=p.parse_args();validate_run_id(a.run_id)
    if bool(a.legacy_index)!=bool(a.installed_switch):p.error('legacy-index and installed-switch must be supplied together')
    out=ROOT/'公共文档/openGauss原生湖存储/验证'/a.run_id;out.mkdir(exist_ok=False)
    files=[Path(__file__),D/'tests/native_file_scalar_build.py',D/'tests/run_native_index_store.py',D/'tests/run_index_affected.py',
        D/'sdk/archive_native.py',D/'sdk/install_native.py',D/'sdk/build_provider.py',D/'source-lock.json',a.qualification.resolve()]
    if a.legacy_index:files += [a.legacy_index.resolve(),a.installed_switch.resolve()]
    r={'status':'waiting_build','scope':'Native physical scalar build/READY/lifecycle and old-provider/affected correctness only; no file query, remote/RAC/commercial qualification',
       'inputs':{str(f):sha256(f)for f in files},'cases':[]}
    archive=out/'executed-inputs';archive.mkdir()
    for f in files:shutil.copy2(f,archive/f.name)
    def save():r['updated_at']=datetime.datetime.now(datetime.timezone.utc).isoformat();write_json(out/'index.json',r)
    def command(name,args,timeout=1800):
        r.setdefault('commands',[]).append(dict(name=name,argv=list(map(str,args))));save()
        with(out/(name+'.log')).open('w')as log:
            subprocess.run(list(map(str,args)),stdout=log,stderr=subprocess.STDOUT,check=True,timeout=timeout)
    def native_run(mode,runid):
        try:command(mode,[sys.executable,D/'tests/native_file_scalar_build.py','--run-id',runid,'--build-receipt',a.build_receipt.resolve(),'--mode',mode],900)
        except subprocess.CalledProcessError:pass # Record the real child result before stopping.
        path=ROOT/'公共文档/openGauss原生湖存储/验证'/runid/'result.json';value=json.loads(path.read_text())
        item=dict(mode=mode,result=str(path),result_sha256=sha256(path),status=value['status'],assertions=len(value['assertions']))
        r['cases'].append(item);save();print(json.dumps(item),flush=True)
        if value['status']!='pass':raise RuntimeError('Native scalar build case failed: '+mode)
    save()
    try:
        deadline=time.monotonic()+7200
        while not a.build_receipt.exists():
            if time.monotonic()>deadline:raise TimeoutError('Official native full build')
            time.sleep(3)
        built=check_build(a.build_receipt);r.update(source_head=built['source_head'],binary_sha256=built['binary_sha256'],status='legacy_provider_cases');save()
        target=ROOT/'.install/opengauss-native-lake-debug/lib/postgresql/og_lake_sdk'
        original=target/'libog_lake_sdk_arrow.so'
        expected=json.loads(a.qualification.read_text())['library_sha256']
        if a.legacy_index:
            legacy=a.legacy_index.resolve();prior=json.loads(legacy.read_text())
            switch=json.loads(a.installed_switch.read_text())
            if prior['status']!='passed' or len(prior['cases'])!=9 or prior['source_head']!=built['source_head'] or prior['binary_sha256']!=built['binary_sha256']:
                raise RuntimeError('Reused legacy evidence belongs to another build or incomplete scope')
            for case in prior['cases']:
                result=Path(case['result']);value=json.loads(result.read_text())
                if sha256(result)!=case['result_sha256'] or value['status']!='pass':raise RuntimeError('Legacy child receipt changed')
                for assertion in value['assertions']:
                    if assertion['status']!='pass' or assertion['observed']!=assertion['expected']:raise RuntimeError('Legacy assertion failed')
                for path,h in value['inputs'].items():
                    if sha256(result.parent/'executed-inputs'/Path(path).name)!=h:raise RuntimeError('Legacy input archive changed')
            if switch['status']!='installed' or switch['provider_sha256']!=expected or sha256(original)!=expected:
                raise RuntimeError('Reused SDK switch does not identify the installed qualified provider')
            if switch['qualification_sha256']!=sha256(a.qualification):raise RuntimeError('SDK switch qualification changed')
            r.update(legacy_provider_index=str(legacy),legacy_provider_index_sha256=sha256(legacy),
                original_provider_sha256=switch['old_provider_sha256'],new_provider_sha256=expected,
                reused_installed_switch=str(a.installed_switch.resolve()),reused_installed_switch_sha256=sha256(a.installed_switch))
            save()
        else:
            original=target/'libog_lake_sdk_arrow.so';r['original_provider_sha256']=sha256(original)
            legacy_id=a.run_id+'_legacy';command('legacy-provider',[sys.executable,D/'tests/run_native_index_store.py','--run-id',legacy_id,'--build-receipt',a.build_receipt.resolve()])
            legacy=ROOT/'公共文档/openGauss原生湖存储/验证'/legacy_id/'index.json'
            if json.loads(legacy.read_text())['status']!='passed':raise RuntimeError('Legacy provider cases incomplete')
            r['legacy_provider_index']=str(legacy);r['legacy_provider_index_sha256']=sha256(legacy);save()
            qualification=json.loads(a.qualification.read_text());expected=qualification['library_sha256']
            r['status']='switching_owned_provider';save()
            command('stop-owned',['bash',D/'build.sh','debug','stop','--run-id',a.run_id+'_provider_stop'])
            stopped=True;retired=None
            try:
                preserve=out/'provider-preservation'
                command('preserve-provider',[sys.executable,D/'sdk/archive_native.py','--evidence',preserve])
                archived=json.loads((preserve/'result.json').read_text());retired=Path(archived['archive'])
                r['provider_preservation']=str(preserve/'result.json');save()
                if archived['status']!='preserved' or sha256(retired/'libog_lake_sdk_arrow.so')!=r['original_provider_sha256']:
                    raise RuntimeError('Original SDK was not preserved exactly')
                install=out/'provider-installation'
                command('install-provider',[sys.executable,D/'sdk/install_native.py','--qualification',a.qualification.resolve(),'--evidence',install])
                installed=json.loads((install/'result.json').read_text())
                if installed['status']!='installed' or sha256(original)!=expected:raise RuntimeError('Installed SDK differs from qualified provider')
                r['provider_installation']=str(install/'result.json');r['new_provider_sha256']=sha256(original);save()
            except BaseException:
                # archive_native moved only a marker/hash-verified prefix. Restore
                # that exact prefix if installation failed before a new publication.
                if retired is not None and retired.is_dir() and not target.exists():
                    retired.rename(target)
                    fd=os.open(target.parent,os.O_RDONLY|os.O_DIRECTORY|os.O_CLOEXEC)
                    try:os.fsync(fd)
                    finally:os.close(fd)
                    r['original_provider_restored']=True
                raise
            finally:
                if stopped:command('restart-owned',['bash',D/'build.sh','debug','restart','--run-id',a.run_id+'_provider_restart'])
        check_build(a.build_receipt);r['status']='native_file_cases';save()
        for mode in ['publication','transactions','guards','permissions']:native_run(mode,a.run_id+'_'+mode)
        affected_id=a.run_id+'_affected'
        command('affected',[sys.executable,D/'tests/run_index_affected.py','--run-id',affected_id,'--build-receipt',a.build_receipt.resolve(),'--registry-index',legacy])
        affected=ROOT/'公共文档/openGauss原生湖存储/验证'/affected_id/'index.json'
        if json.loads(affected.read_text())['status']!='passed':raise RuntimeError('Affected regressions incomplete')
        r['affected_index']=str(affected);r['affected_index_sha256']=sha256(affected)
        if not all(sha256(f)==h for f,h in r['inputs'].items()):raise RuntimeError('Campaign inputs changed')
        if sha256(original)!=expected:raise RuntimeError('Native provider changed during campaign')
        check_build(a.build_receipt);r['status']='passed'
    except BaseException as exc:r.update(status='failed',reason=str(exc),traceback=traceback.format_exc())
    save();print(json.dumps(dict(status=r['status'],reason=r.get('reason'),index=str(out/'index.json'))),flush=True)
    return 0 if r['status']=='passed'else 1

if __name__=='__main__':raise SystemExit(main())
