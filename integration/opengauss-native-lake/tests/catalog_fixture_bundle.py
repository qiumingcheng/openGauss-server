#!/usr/bin/env python3
"""Draft real EMPTY fixtures for same/ancestor/child/sibling registration tests.

Not run. Each catalog is created by the fixed reference's ordinary ATTACH.
Only owned parent directories are mkdir'ed; no protocol value is rewritten.
"""
import argparse,json,subprocess,sys,traceback
from pathlib import Path
from types import SimpleNamespace
from empty_reference_fixture import DRIVER
from empty_manifest import validate_empty_manifest
from lake_harness import sha256,validate_run_id,write_json
from fixture import validate_manifest


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--run-id',required=True);p.add_argument('--output',type=Path,required=True)
    args=p.parse_args();validate_run_id(args.run_id)
    out=args.output.resolve();base=out.parent/'empty-root-bundle'
    if out.exists() or base.exists(): p.error('Refusing to overwrite an existing fixture attempt')
    base.mkdir(parents=True);parents=base/'data-parents';parents.mkdir()
    roots={'same':parents/'base','ancestor':parents/'ancestor','child':parents/'ancestor'/'child','sibling':parents/'base-other'}
    lock_hash=sha256(DRIVER/'source-lock.json');entries={};result={'status':'fail','scope':'actual_empty_reference_bundle'}
    try:
        for alias,root in roots.items():
            # The ancestor was absent during its normal reference initialization.
            # Making that test-owned directory now lets both ancestor/child
            # registration inputs pass their independent filesystem admission.
            if alias=='child': roots['ancestor'].mkdir()
            folder=base/alias;folder.mkdir();manifest=folder/'manifest.json'
            command=[sys.executable,str(Path(__file__).with_name('empty_reference_fixture.py')),
                     '--run-id',args.run_id+'-'+alias,'--output',str(manifest),'--data-root',str(root)]
            proc=subprocess.run(command,text=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=300)
            (folder/'generator.stdout').write_text(proc.stdout);(folder/'generator.stderr').write_text(proc.stderr)
            write_json(folder/'generator-process.json',{'argv':command,'returncode':proc.returncode})
            if proc.returncode: raise RuntimeError('Actual EMPTY generator failed: '+alias)
            m,_=validate_empty_manifest(manifest)
            if sha256(DRIVER/'source-lock.json')!=lock_hash: raise RuntimeError('Source-lock changed during bundle generation')
            entries[alias]={'manifest':str(manifest),'sha256':sha256(manifest),'data_root':m['data_path']}
        # A separate ordinary I fixture supports tests that must hold real lake
        # reader locks. It is not derived by editing an EMPTY protocol image.
        folder=base/'business';folder.mkdir();business=folder/'manifest.json'
        command=['bash',str(DRIVER/'reference.sh'),'fixture','--case','I','--run-id',args.run_id+'-business','--output',str(business)]
        proc=subprocess.run(command,text=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=300)
        (folder/'generator.stdout').write_text(proc.stdout);(folder/'generator.stderr').write_text(proc.stderr)
        write_json(folder/'generator-process.json',{'argv':command,'returncode':proc.returncode})
        if proc.returncode:raise RuntimeError('Actual I generator failed')
        actual=json.loads(business.read_text());validate_manifest(actual,SimpleNamespace(driver=DRIVER,run_id=args.run_id+'-business'))
        if sha256(DRIVER/'source-lock.json')!=lock_hash:raise RuntimeError('Source-lock changed during business fixture generation')
        bundle={'case':'EMPTY_BUNDLE','status':'verified_reference_only','run_id':args.run_id,
                'source_lock_hash':lock_hash,'cleanup_owner':str(base),'entries':entries,
                'generation_provenance':{'generator_path':str(Path(__file__).resolve()),'generator_sha256':sha256(__file__)},
                'business_fixture':{'manifest':str(business),'sha256':sha256(business)},
                'filesystem_setup':{'created_ancestor_parent':str(roots['ancestor']),
                                    'protocol_mutation':'none; all roots come from normal independent reference ATTACH'}}
        write_json(out,bundle);result.update(status='pass',manifest=str(out),manifest_sha256=sha256(out))
    except BaseException as exc: result.update(reason=str(exc),traceback=traceback.format_exc())
    finally:write_json(base/'generation-result.json',result);print(json.dumps(result,ensure_ascii=False))
    return 0 if result['status']=='pass' else 1


if __name__=='__main__':raise SystemExit(main())
