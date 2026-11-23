#!/usr/bin/env python3
"""Collect exact native vector evidence; the strict reviewer checks cleanup.

Child inputs come from their executed archives, including older driver versions.
The caller specifies the tested scope; ANN, SMP, remote and commercial gates
remain separate. This collector does not promote task state.
"""
import argparse
import datetime
import json
from pathlib import Path
import shutil
import sys

ROOT=Path('/home/qiumc/data/code/cpp');D=ROOT/'integration/opengauss-native-lake'
sys.path[:0]=[str(D),str(D/'tests')]
from file_dml_acceptance import check_build
from lake_harness import sha256,write_json


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--output',type=Path,required=True)
    p.add_argument('--build-receipt',type=Path,required=True)
    p.add_argument('--kind',default='native-vector-qualification',choices=['native-vector-qualification','native-inline-ann-qualification'])
    p.add_argument('--scope',help='Exact read/write/statistic capability scope of these selected children')
    p.add_argument('--case',action='append',required=True,help='unique logical mode=expected provider SHA256=result path')
    p.add_argument('--provider-qualification',type=Path,required=True)
    a=p.parse_args();out=a.output.resolve();out.parent.mkdir(exist_ok=False);b=check_build(a.build_receipt)
    q=json.loads(a.provider_qualification.read_text());assert q['status']=='passed'
    installed=ROOT/'.install/opengauss-native-lake-debug/lib/postgresql/og_lake_sdk/libog_lake_sdk_arrow.so'
    assert q['library_sha256']==sha256(installed)
    files=[Path(__file__),D/'source-lock.json',a.provider_qualification.resolve()]
    archive=out.parent/'executed-inputs';archive.mkdir()
    r={'status':'passed','scope':a.scope or __doc__,'source_head':b['source_head'],'binary_sha256':b['binary_sha256'],
       'inputs':{str(f):sha256(f)for f in files},'cases':[],
       'provider_qualification':str(a.provider_qualification.resolve()),'provider_qualification_sha256':sha256(a.provider_qualification)}
    for f in files:shutil.copy2(f,archive/f.name)
    known={q['library_sha256'],'8a6fcf7fafbb737f80de3d3ce39a579b2ff124a8c06cd89c85c81c5a686f07f6'}
    modes=set();paths=set()
    for raw in a.case:
        mode,provider,path=raw.split('=',2);path=Path(path).resolve()
        assert mode not in modes and path not in paths and provider in known
        modes.add(mode);paths.add(path);v=json.loads(path.read_text())
        assert v['status']=='pass'and v['cleanup']=='pass'
        assert v['source_head']==b['source_head']and v['binary_sha256']==b['binary_sha256']and v['provider_sha256']==provider
        assert v['assertions']and all(c['status']=='pass'and c['observed']==c['expected']for c in v['assertions'])
        for f,digest in v['inputs'].items():assert sha256(path.parent/'executed-inputs'/Path(f).name)==digest
        for f,digest in v.get('artifacts',{}).items():assert sha256(f)==digest
        r['cases'].append(dict(kind=a.kind,mode=mode,status='pass',returncode=0,
            assertions=len(v['assertions']),result=str(path),result_sha256=sha256(path),provider_sha256=provider,artifact_hashes_verified=True))
    check_build(a.build_receipt);r['collected_at']=datetime.datetime.now(datetime.timezone.utc).isoformat()
    write_json(out,r);print(out)

if __name__=='__main__':main()
