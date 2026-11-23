#!/usr/bin/env python3
"""Audit old-binary heap primitives without qualifying frozen new Native code."""
import argparse
import datetime
import json
from pathlib import Path
import shutil
import sys

D=Path(__file__).resolve().parents[1];sys.path.insert(0,str(D))
from lake_harness import sha256,write_json


def require(value,message):
    if not value:raise ValueError(message)


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--binary-receipt',type=Path,required=True)
    p.add_argument('--case',type=Path,action='append',required=True)
    p.add_argument('--preserved-failure',type=Path,action='append',default=[])
    p.add_argument('--output',type=Path,required=True)
    a=p.parse_args();require(not a.output.exists(),'Refuse to overwrite review')
    build=json.loads(a.binary_receipt.read_text())
    require(build.get('status')=='build_and_sql_passed' and build['profile']=='debug','Wrong successful Debug binary receipt')
    binary=Path(build['install'])/'bin/gaussdb'
    require(sha256(binary)==build['binary_sha256'],'Installed binary changed during primitive audit')
    seen=set();cases=[];failures=[]
    def verify(path,passed):
        path=path.resolve();require(path not in seen,'Repeated result');seen.add(path)
        r=json.loads(path.read_text());require(r['status']==('pass'if passed else'fail'),'Wrong original result status')
        require(r.get('native_source_qualified')is False,'Primitive must not claim new Native qualification')
        require(r['source_head']==build['source_head']and r['binary_sha256']==build['binary_sha256'],'Wrong recorded executed binary')
        require(bool(r.get('inputs')),'Missing executed inputs')
        for f,h in r['inputs'].items():require(sha256(path.parent/'executed-inputs'/Path(f).name)==h,'Archived input changed: '+f)
        require(sha256(path.parent/'executed-inputs/build.json')==sha256(a.binary_receipt),'Archived binary receipt changed')
        if passed:
            require(r['binary_before']==r['binary_after'],'Before/after binary identity changed')
            for v in r['assertions']:require(v['status']=='pass'and v['observed']==v['expected'],'Invalid passing assertion')
        else:require(bool(r.get('reason')),'Missing preserved failure reason')
        cleanup=r.get('database_cleanup',{});require(cleanup.get('returncode')==0,'Database cleanup failed')
        summary=json.loads(cleanup['stdout']);require(summary['status']=='pass','Cleanup summary failed')
        report=Path(summary['immutable_report']);detail=json.loads(report.read_text())
        require(report.parent==path.parent and Path(summary['attempt'])==path.parent,'Wrong owned cleanup path')
        for key in ['status','run_id','dropped_database_oid','attempt']:require(summary[key]==detail[key],'Cleanup mismatch')
        owner=json.loads((path.parent/'database-owner.json').read_text())
        require(owner['run_id']==path.parent.name==summary['run_id'],'Wrong run ownership')
        require(owner['database_oid']==summary['dropped_database_oid'],'Wrong removed database OID')
        require(r.get('role_cleanup',{}).get('status')=='pass','Role cleanup failed')
        return dict(result=str(path),sha256=sha256(path),status=r['status'],assertions=len(r['assertions']),
            cleanup_report=str(report),cleanup_report_sha256=sha256(report),archived_inputs_verified=True,
            reason=r.get('reason'),scope=r['scope'])
    for path in a.case:cases.append(verify(path,True))
    for path in a.preserved_failure:failures.append(verify(path,False))
    archive=a.output.with_suffix('.reviewer.py');require(not archive.exists(),'Refuse to overwrite archived reviewer')
    shutil.copy2(__file__,archive)
    r=dict(status='passed',scope=__doc__,native_source_qualified=False,target_rac_qualified=False,
        reviewed_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
        binary_receipt=str(a.binary_receipt.resolve()),binary_receipt_sha256=sha256(a.binary_receipt),
        executed_binary_sha256=build['binary_sha256'],source_head=build['source_head'],cases=cases,preserved_failures=failures,
        case_count=len(cases),assertions=sum(c['assertions']for c in cases),reviewer_archive=str(archive.resolve()),
        reviewer_sha256=sha256(archive),limitations=[
            'Current frozen extension0.17 inputs were not compiled or exercised by these primitives.',
            'One bounded 20k-head workload and one vacuum safe-horizon interval do not establish an absolute capacity bound.',
            'Long snapshots/retention can postpone reuse; progress, budgets and foreground admission remain required.'])
    write_json(a.output,r);print(json.dumps(dict(status=r['status'],cases=r['case_count'],assertions=r['assertions'])))


if __name__=='__main__':main()
