#!/usr/bin/env python3
"""Collect explicitly selected current-build regressions for strict review.

No capability is inferred from a case name. Frozen child scripts and their
individual scopes remain authoritative; provider hashes are kept only when
the child actually recorded them.
"""
import argparse
import json
from pathlib import Path
import shutil
import sys

D=Path(__file__).resolve().parents[1];sys.path[:0]=[str(D),str(D/'tests')]
from file_dml_acceptance import check_build
from lake_harness import sha256,write_json

def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--build-receipt',type=Path,required=True)
    p.add_argument('--output',type=Path,required=True);p.add_argument('--scope',required=True)
    p.add_argument('--case',action='append',required=True,help='kind=unique-mode=existing-result-path')
    a=p.parse_args();b=check_build(a.build_receipt);out=a.output.resolve();out.parent.mkdir(exist_ok=False)
    files=[Path(__file__),D/'source-lock.json'];archive=out.parent/'executed-inputs';archive.mkdir()
    for f in files:shutil.copy2(f,archive/f.name)
    r=dict(status='passed',scope=a.scope,source_head=b['source_head'],binary_sha256=b['binary_sha256'],
           inputs={str(f):sha256(f)for f in files},cases=[])
    seen=set()
    for raw in a.case:
        kind,mode,path=raw.split('=',2);path=Path(path).resolve();v=json.loads(path.read_text())
        assert (kind,mode)not in seen;seen.add((kind,mode))
        assert v['status']=='pass'and v['source_head']==b['source_head']and v['binary_sha256']==b['binary_sha256']
        assert v['assertions']and all(c['status']=='pass'and c['observed']==c['expected']for c in v['assertions'])
        for f,h in v['inputs'].items():assert sha256(path.parent/'executed-inputs'/Path(f).name)==h
        for f,h in v.get('artifacts',{}).items():assert sha256(f)==h
        item=dict(kind=kind,mode=mode,status='pass',returncode=0,assertions=len(v['assertions']),
                  result=str(path),result_sha256=sha256(path),child_scope=v['scope'])
        if 'provider_sha256'in v:item['provider_sha256']=v['provider_sha256']
        r['cases'].append(item)
    check_build(a.build_receipt);write_json(out,r);print(out)

if __name__=='__main__':main()
