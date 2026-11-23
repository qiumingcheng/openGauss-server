#!/usr/bin/env python3
"""Review executed index campaign evidence without modifying original receipts.

Checks exact native identity, every archived input, each observed/expected
assertion and the separately written OID/marker-owned cleanup reports. This
does not enlarge the campaign's tested capability scope.
"""
import argparse
import datetime
import json
from pathlib import Path
import sys

DRIVER=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(DRIVER),str(DRIVER/'tests')]
from file_dml_acceptance import check_build
from lake_harness import sha256,write_json

def require(condition,message):
    if not condition:raise ValueError(message)

def read(path):return json.loads(Path(path).read_text())

def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--campaign',type=Path,action='append',required=True)
    p.add_argument('--build-receipt',type=Path,required=True);p.add_argument('--output',type=Path,required=True)
    p.add_argument('--allow-failed-parents',action='store_true',help='Retain failed child exclusions explicitly; successful reruns require separate campaigns')
    p.add_argument('--scope');p.add_argument('--expected-cases',type=int)
    a=p.parse_args();require(not a.output.exists(),'Refuse to overwrite evidence')
    reviewer_archive=a.output.with_suffix('.reviewer.py')
    require(not reviewer_archive.exists(),'Refuse to overwrite archived reviewer')
    build=check_build(a.build_receipt);seen=set();campaigns=[];cases=[];excluded=[]
    def inputs(path,value):
        require(bool(value.get('inputs')),'No frozen inputs: '+str(path))
        for original,digest in value['inputs'].items():
            archived=path.parent/'executed-inputs'/Path(original).name
            require(archived.is_file()and sha256(archived)==digest,'Archived input mismatch: '+str(archived))
    def identity(path,value):
        require(value.get('source_head')==build['source_head'],'Wrong source: '+str(path))
        require(value.get('binary_sha256')==build['binary_sha256'],'Wrong binary: '+str(path))
    def leaf(item):
        path=Path(item['result']).resolve();require(path not in seen,'Repeated result: '+str(path));seen.add(path)
        require(item.get('returncode')==0 and item.get('status')=='pass','Child failed: '+str(path))
        require(sha256(path)==item.get('result_sha256'),'Child receipt changed: '+str(path))
        value=read(path);require(value.get('status')=='pass','Result failed: '+str(path));identity(path,value);inputs(path,value)
        assertions=value.get('assertions',[]);require(bool(assertions),'No assertions: '+str(path))
        for check in assertions:
            require(check.get('status')=='pass'and 'observed'in check and 'expected'in check and check['observed']==check['expected'],
                'Invalid assertion '+str(check.get('name'))+' in '+str(path))
        require(len(assertions)==item.get('assertions'),'Wrong assertion count: '+str(path))
        cleanup=value.get('database_cleanup',{});require(cleanup.get('returncode')==0,'DB cleanup failed: '+str(path))
        summary=json.loads(cleanup['stdout']);require(summary.get('status')=='pass','DB cleanup summary failed')
        immutable=Path(summary['immutable_report']).resolve()
        attempt=Path(summary['attempt']).resolve()
        require(attempt.is_relative_to(path.parent)and immutable.parent==attempt and immutable.is_file(),
            'Missing separate owned-attempt cleanup report')
        report=read(immutable);require(report.get('status')=='pass','Separate cleanup report failed')
        for key in ['run_id','dropped_database_oid','attempt']:
            require(report.get(key)==summary.get(key),'Cleanup mismatch: '+key)
        require(summary.get('run_id')==path.parent.name and isinstance(summary.get('dropped_database_oid'),int),
            'Cleanup ownership mismatch')
        require(value.get('role_cleanup',{}).get('status')=='pass','Role cleanup failed: '+str(path))
        cases.append(dict(**item,archived_inputs_verified=True,assertions_verified=True,owned_cleanup_verified=True,
            cleanup_report=str(immutable),cleanup_report_sha256=sha256(immutable)))
    def campaign(path,expected_hash=None):
        path=Path(path).resolve();require(path not in seen,'Repeated campaign: '+str(path));seen.add(path)
        if expected_hash:require(sha256(path)==expected_hash,'Campaign changed: '+str(path))
        value=read(path);parent_status=value.get('status')
        require(parent_status=='passed'or(a.allow_failed_parents and parent_status=='failed'),'Campaign not complete: '+str(path));identity(path,value);inputs(path,value)
        require(bool(value.get('cases')),'Empty campaign: '+str(path))
        for item in value['cases']:
            if item.get('status')=='pass'and item.get('returncode')==0:leaf(item)
            else:
                require(a.allow_failed_parents and parent_status=='failed','Child failed: '+str(item))
                failed=Path(item['result']);require(failed.is_file()and sha256(failed)==item['result_sha256'],'Failed child receipt changed')
                excluded.append(dict(parent=str(path),**item))
        campaigns.append(dict(path=str(path),sha256=sha256(path),original_status=parent_status,archived_inputs_verified=True))
        for key in ['compatibility_index','affected_index']:
            if key in value:campaign(value[key],value.get(key+'_sha256'))
    for selected in a.campaign:campaign(selected)
    check_build(a.build_receipt)
    require(len(a.campaign)==1 or bool(a.scope),'Multiple campaigns require an explicit combined scope')
    keys=[(item.get('kind','native'),item.get('mode'))for item in cases]
    require(len(keys)==len(set(keys)),'Duplicate logical cases in the aggregate')
    if a.expected_cases is not None:require(len(cases)==a.expected_cases,'Missing required case count')
    if excluded:
        for failed in excluded:
            require((failed.get('kind','native'),failed.get('mode'))in keys,'Excluded failure has no successful replacement')
    result=dict(status='passed',scope=a.scope or read(a.campaign[0])['scope'],reviewed_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
        source_head=build['source_head'],binary_sha256=build['binary_sha256'],native_inputs_verified=len(build['native_source_inputs_after']),
        build=str(a.build_receipt.resolve()),build_sha256=sha256(a.build_receipt),campaigns=campaigns,cases=cases,
        case_count=len(cases),assertions=sum(x['assertions']for x in cases),excluded_failures=excluded,
        reviewer=str(Path(__file__).resolve()),reviewer_sha256=sha256(Path(__file__)))
    reviewer_archive.write_bytes(Path(__file__).read_bytes())
    result['reviewer_archive']=str(reviewer_archive.resolve())
    require(sha256(reviewer_archive)==result['reviewer_sha256'],'Reviewer changed during review')
    write_json(a.output,result);print(json.dumps(dict(status='passed',cases=result['case_count'],assertions=result['assertions'],output=str(a.output))))
    return 0

if __name__=='__main__':raise SystemExit(main())
