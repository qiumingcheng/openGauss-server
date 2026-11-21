#!/usr/bin/env python3
"""Independently check selected transport/format receipts; no host promotion."""
import argparse
import datetime
import hashlib
import json
from pathlib import Path
import shutil

def sha(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()
def read(path):return json.loads(Path(path).read_text())

def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--build',type=Path,required=True)
    p.add_argument('--output',type=Path,required=True);p.add_argument('--scope',required=True)
    p.add_argument('--case',type=Path,action='append',required=True);a=p.parse_args()
    assert not a.output.exists();archive=a.output.with_suffix('.reviewer.py');assert not archive.exists()
    b=read(a.build);assert b['status']=='provider_built_unqualified'and b['sources_before']==b['sources_after']and b['dependencies_before']==b['dependencies_after']
    assert sha(b['library'])==b['sha256'];seen=set();cases=[];total=0
    for path in a.case:
        path=path.resolve();assert path not in seen;seen.add(path);v=read(path)
        assert v['status']=='passed'and v['provider_sha256']==b['sha256']
        assert v['assertions']and all(x['status']=='pass'and x['observed']==x['expected']for x in v['assertions'])
        for source,digest in v['inputs'].items():
            name=v.get('archive_index',{}).get(source,Path(source).name)
            assert sha(path.parent/'executed-inputs'/name)==digest
        for artifact,digest in v['artifacts'].items():assert sha(artifact)==digest
        assert v['consumer']['status']=='pass'and v['consumer']['checks']>0
        for key in ['external_consumer','reopen_consumer','old_consumer']:
            if key in v:assert v[key]['status']=='pass'and v[key]['checks']>0
        # Exact built header/provider inputs are verified only where archived.
        for source,digest in v['inputs'].items():
            name=Path(source).name
            if Path(source).parent.resolve()==Path(b['provider_source']).resolve():
                assert name in b['sources_after']and b['sources_after'][name]==digest
        total+=len(v['assertions']);cases.append(dict(result=str(path),sha256=sha(path),assertions=len(v['assertions']),consumer_checks=v['consumer']['checks'],scope=v['scope']))
    r=dict(status='passed',scope=a.scope,reviewed_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
           provider_sha256=b['sha256'],build=str(a.build.resolve()),build_sha256=sha(a.build),cases=cases,assertions=total,
           exclusions='No native installation eligibility, native DiskANN graph build/search, lake file publication/query, recall, PQ, SMP, remote or OGRAC/commercial acceptance.')
    shutil.copy2(__file__,archive);a.output.write_text(json.dumps(r,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps(dict(status=r['status'],cases=len(cases),assertions=total,output=str(a.output)),ensure_ascii=False))

if __name__=='__main__':main()
