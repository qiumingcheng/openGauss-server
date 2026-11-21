#!/usr/bin/env python3
"""Preserve an exactly owned SDK prefix only while the owned instance is down."""
import argparse,json,os,uuid
from pathlib import Path
from build_provider import ROOT,sha

def main():
    parser=argparse.ArgumentParser();parser.add_argument('--evidence',type=Path,required=True)
    args=parser.parse_args();evidence=args.evidence.resolve();evidence.mkdir(parents=True,exist_ok=False)
    prefix=ROOT/'.install/opengauss-native-lake-debug/lib/postgresql/og_lake_sdk'
    result={'status':'failed','action':'preserve-owned-provider-prefix','source':str(prefix),'script_sha256':sha(__file__)}
    try:
        ownership=json.loads((ROOT/'.runtime/opengauss-native-lake/debug/owner.json').read_text())
        if ownership['project']!='opengauss-native-lake' or ownership['install']!=str(ROOT/'.install/opengauss-native-lake-debug'):
            raise RuntimeError('instance ownership mismatch')
        pidfile=ROOT/'.runtime/opengauss-native-lake/debug/data/postmaster.pid'
        if pidfile.exists():
            pid=int(pidfile.read_text().splitlines()[0])
            if Path('/proc/'+str(pid)).exists():raise RuntimeError('owned instance must be stopped before changing its provider prefix')
        if prefix.is_symlink() or not prefix.is_dir():raise RuntimeError('provider prefix is missing or aliased')
        owner=json.loads((prefix/'owner.json').read_text())
        if owner['project']!='opengauss-native-lake-local-provider' or owner['target']!=str(prefix):
            raise RuntimeError('provider ownership marker mismatch')
        actual={str(p.relative_to(prefix)):sha(p) for p in prefix.rglob('*') if p.is_file() and not p.is_symlink() and p.name!='owner.json'}
        if actual!=owner['files']:raise RuntimeError('provider prefix contains unowned or changed file bytes')
        aliases={str(p.relative_to(prefix)):os.readlink(p) for p in prefix.rglob('*') if p.is_symlink()}
        for name in aliases:
            if prefix not in (prefix/name).resolve().parents:raise RuntimeError('provider alias escapes owned prefix')
        destination=prefix.with_name('og_lake_sdk.retired.'+uuid.uuid4().hex)
        if destination.exists():raise RuntimeError('random preservation target already exists')
        prefix.rename(destination)
        descriptor=os.open(destination.parent,os.O_RDONLY|os.O_DIRECTORY|os.O_CLOEXEC)
        try:os.fsync(descriptor)
        finally:os.close(descriptor)
        result.update(status='preserved',archive=str(destination),files=actual,aliases=aliases,
                      qualification_sha256=owner['qualification_sha256'])
        print(str(destination),flush=True);return 0
    except Exception as exc:result['error']=str(exc);print(str(exc),flush=True);return 1
    finally:(evidence/'result.json').write_text(json.dumps(result,indent=2)+'\n')

if __name__=='__main__':raise SystemExit(main())
