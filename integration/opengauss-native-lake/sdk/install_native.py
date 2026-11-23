#!/usr/bin/env python3
"""Install only the qualified local provider into this project's owned prefix."""
import argparse,ctypes,datetime,errno,json,os,shutil,uuid
from pathlib import Path
from build_provider import ROOT,PREFIX,sha,inputs,dependencies

def main():
    parser=argparse.ArgumentParser();parser.add_argument('--qualification',type=Path,required=True)
    parser.add_argument('--evidence',type=Path,required=True)
    parser.add_argument('--provider-prefix',type=Path,default=PREFIX);args=parser.parse_args()
    provider_prefix=args.provider_prefix.resolve()
    evidence=args.evidence.resolve();evidence.mkdir(parents=True,exist_ok=False)
    target_prefix=ROOT/'.install/opengauss-native-lake-debug'
    target=target_prefix/'lib/postgresql/og_lake_sdk';staging=None;published=False
    result={'status':'failed','scope':'owned local provider installation; no native/runtime gate promotion',
            'target':str(target),'started_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),
            'script_sha256':sha(__file__),'qualification_path':str(args.qualification.resolve())}
    def durable_directory(path):
        fd=os.open(path,os.O_RDONLY|os.O_DIRECTORY|os.O_CLOEXEC)
        try:os.fsync(fd)
        finally:os.close(fd)
    try:
        qualification=json.loads(args.qualification.read_text())
        result['qualification_sha256']=sha(args.qualification)
        library=provider_prefix/'lib/libog_lake_sdk_arrow.so'
        if 'provider_prefix' in qualification and Path(qualification['provider_prefix']).resolve()!=provider_prefix:
            raise RuntimeError('qualified provider prefix mismatch')
        if qualification['status']!='passed' or qualification['provider_sources_after']!=inputs() or qualification['dependencies_after']!=dependencies() or qualification['library_sha256']!=sha(library):
            raise RuntimeError('qualification does not identify the current provider source and dependency bytes')
        ownership=json.loads((ROOT/'.runtime/opengauss-native-lake/debug/owner.json').read_text())
        if ownership['install']!=str(target_prefix) or ownership['project']!='opengauss-native-lake':
            raise RuntimeError('native installation ownership mismatch')
        pidfile=ROOT/'.runtime/opengauss-native-lake/debug/data/postmaster.pid'
        if pidfile.exists() and Path('/proc/'+pidfile.read_text().splitlines()[0]).exists():
            raise RuntimeError('owned native instance must be stopped before SDK installation')
        if target.exists() or target.is_symlink():raise RuntimeError('provider target already exists; no replacement of loaded or unknown libraries')
        parent=target.parent
        if not parent.is_dir() or parent.is_symlink() or parent.resolve()!=parent:
            raise RuntimeError('native provider parent must be an existing canonical directory')
        libc=ctypes.CDLL(None,use_errno=True)
        rename=libc.renameat2;rename.argtypes=[ctypes.c_int,ctypes.c_char_p,ctypes.c_int,ctypes.c_char_p,ctypes.c_uint]
        rename.restype=ctypes.c_int
        staging=parent/('.og_lake_sdk.staging.'+uuid.uuid4().hex);staging.mkdir(mode=0o700)
        for stem in ('libog_lake_sdk_arrow.so',):
            original=provider_prefix/'lib'/stem;real=original.resolve();shutil.copy2(real,staging/real.name)
            for alias in (provider_prefix/'lib').glob(stem+'*'):
                if alias.is_symlink():(staging/alias.name).symlink_to(os.readlink(alias))
        shutil.copytree(provider_prefix/'share/licenses',staging/'licenses')
        result['files']={str(p.relative_to(staging)):sha(p) for p in sorted(staging.rglob('*')) if p.is_file() and not p.is_symlink()}
        descriptor={'project':'opengauss-native-lake-local-provider','phase':'prepared-for-atomic-install',
                    'target':str(target),'qualification_sha256':result['qualification_sha256'],'files':result['files']}
        (staging/'owner.json').write_text(json.dumps(descriptor,indent=2)+'\n')
        for path in staging.rglob('*'):
            if path.is_file() and not path.is_symlink():
                path.chmod(0o644)
                with path.open('rb') as stream:os.fsync(stream.fileno())
        for path in sorted([p for p in staging.rglob('*') if p.is_dir()],key=lambda p:len(p.parts),reverse=True):
            durable_directory(path)
        staging.chmod(0o755);durable_directory(staging)
        # RENAME_NOREPLACE cannot overwrite even an empty competing directory.
        if rename(-100,os.fsencode(staging),-100,os.fsencode(target),1)!=0:
            code=ctypes.get_errno();raise OSError(code,os.strerror(code))
        published=True;durable_directory(parent)
        result['status']='installed'
        if result['files']['libog_lake_sdk_arrow.so']!=qualification['library_sha256']:
            raise RuntimeError('installed provider differs from qualified library')
        print('qualified_local_provider_installed',flush=True);return 0
    except Exception as exc:
        result['error']=str(exc);print(str(exc),flush=True);return 1
    finally:
        result['published']=published
        if staging is not None and not published and staging.is_dir():
            # The random directory was created by this invocation only.
            shutil.rmtree(staging)
        (evidence/'result.json').write_text(json.dumps(result,indent=2)+'\n')

if __name__=='__main__':raise SystemExit(main())
