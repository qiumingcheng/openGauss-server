#!/usr/bin/env python3
"""Collect completed native float evidence, checking source/binary/artifacts."""
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
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--output',type=Path,required=True);p.add_argument('--build-receipt',type=Path,required=True)
    for name in ['old','full','heap','ustore']:p.add_argument('--'+name+'-result',type=Path,required=True)
    a=p.parse_args();out=a.output.resolve();out.parent.mkdir(exist_ok=False);b=check_build(a.build_receipt)
    files=[Path(__file__),D/'tests/native_float.py',D/'tests/native_float_interop.py',D/'source-lock.json'];archive=out.parent/'executed-inputs';archive.mkdir()
    r={'status':'passed','scope':'Native six-scalar registry float CREATE/DML/ANALYZE, old-provider atomic refusal, local Parquet data/delete flush and genuine DuckLake heap/Ustore readback only. No float indexes, vectors, DiskANN/fusion, SMP, remote/RAC or commercial qualification.','source_head':b['source_head'],'binary_sha256':b['binary_sha256'],'inputs':{str(f):sha256(f)for f in files},'cases':[]}
    for f in files:shutil.copy2(f,archive/f.name)
    for name,mode in [('old','old-provider'),('full','full'),('heap','interop-heap'),('ustore','interop-ustore')]:
        path=getattr(a,name+'_result').resolve();v=json.loads(path.read_text());assert v['status']=='pass'and v['cleanup']=='pass'
        assert v['source_head']==b['source_head']and v['binary_sha256']==b['binary_sha256']
        for artifact,digest in v.get('artifacts',{}).items():assert sha256(artifact)==digest
        for check in v['assertions']:assert check['status']=='pass'and check['observed']==check['expected']
        expected='142b59847b047bd1dd988122d36e5d09335e430ec60314f357aa0c7760413529'if name=='old'else'8a6fcf7fafbb737f80de3d3ce39a579b2ff124a8c06cd89c85c81c5a686f07f6'
        assert v['provider_sha256']==expected
        r['cases'].append(dict(kind='float',mode=mode,status='pass',returncode=0,assertions=len(v['assertions']),result=str(path),result_sha256=sha256(path),provider_sha256=expected,artifact_hashes_verified=True))
    r['collected_at']=datetime.datetime.now(datetime.timezone.utc).isoformat();write_json(out,r);print(out)
if __name__=='__main__':main()
