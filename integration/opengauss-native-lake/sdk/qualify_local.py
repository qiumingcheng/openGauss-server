#!/usr/bin/env python3
"""Scoped standalone qualification; never promotes native SDK/commercial gates."""
import argparse,datetime,json,os,re,shutil,subprocess,sys
from pathlib import Path
from build_provider import ROOT,PREFIX,GCC,SOURCE,sha,inputs,dependencies

def main():
    parser=argparse.ArgumentParser();parser.add_argument('--evidence',type=Path,required=True)
    args=parser.parse_args();evidence=args.evidence.resolve();evidence.mkdir(parents=True,exist_ok=False)
    scripts=Path(__file__).resolve().parent;library=PREFIX/'lib/libog_lake_sdk_arrow.so'
    receipt={'status':'failed','scope':'standalone local C ABI real Parquet read/write only',
             'limitations':['No native FDW file scan or flush qualification','No deletion visibility or catalog publication qualification',
                            'No remote/shared-RAC durability or crash-recovery qualification','No private SDK contract qualification'],
             'started_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),
             'provider_sources_before':inputs(),'dependencies_before':dependencies(),'library_sha256':sha(library),'cases':[]}
    archived=evidence/'executed-inputs';archived.mkdir()
    frozen=[]
    for root,label in ((scripts,'driver'),(SOURCE,'provider')):
        destination=archived/label;destination.mkdir()
        for path in sorted(root.iterdir()):
            if path.is_file():
                shutil.copy2(path,destination/path.name)
                frozen.append({'path':str(path),'sha256':sha(path),'archive':str(destination/path.name)})
    receipt['executed_inputs']=frozen
    try:
        for name,script in [('reader','test_reader.py'),('writer','test_writer.py'),
                            ('faults','test_writer_faults.py'),('ducklake_files','test_ducklake_files.py')]:
            directory=evidence/name;command=[sys.executable,str(scripts/script),'--evidence',str(directory)]
            if name in ('writer','faults'):command+=['--input',str(evidence/'reader/typed.parquet')]
            process=subprocess.run(command,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True)
            (evidence/(name+'.log')).write_text(process.stdout)
            result=json.loads((directory/'result.json').read_text())
            receipt['cases'].append({'name':name,'returncode':process.returncode,'result':str(directory/'result.json'),
                                     'sha256':sha(directory/'result.json')})
            if process.returncode or result['status']!='passed':raise RuntimeError(name+' failed')
            if result['library_sha256']!=receipt['library_sha256'] or result['provider_sources_before']!=receipt['provider_sources_before']:
                raise RuntimeError('case ran against another provider build')
        # Copy actual shared libraries, retaining SONAME aliases, into an owned
        # independent prefix. No original Arrow library path is in this process.
        relocated=evidence/'relocated/lib';relocated.mkdir(parents=True)
        for stem in ('libog_lake_sdk_arrow.so',):
            original=PREFIX/'lib'/stem;real=original.resolve()
            shutil.copy2(real,relocated/real.name)
            for alias in (PREFIX/'lib').glob(stem+'*'):
                if alias.is_symlink():(relocated/alias.name).symlink_to(os.readlink(alias))
        license_dir=evidence/'relocated/share/licenses';shutil.copytree(PREFIX/'share/licenses',license_dir)
        env=os.environ.copy();support=[GCC/'lib64']+[GCC.parent/p/'lib' for p in ('isl','mpc','mpfr','gmp')]
        env['LD_LIBRARY_PATH']=':'.join(map(str,support))
        library2=relocated/'libog_lake_sdk_arrow.so'
        command=[str(evidence/'reader/reader-c-host'),str(library2)]+[str(evidence/'reader'/name) for name in
            ('typed.parquet','no_ids.parquet','bad_magic.parquet','truncated.parquet','missing.parquet')]
        process=subprocess.run(command,env=env,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True)
        (evidence/'relocated-reader.log').write_text(process.stdout)
        if process.returncode:raise RuntimeError('relocated provider C consumer failed')
        receipt['relocated_reader']=json.loads(process.stdout)
        exports=subprocess.check_output(['nm','-D','--defined-only',str(library)],text=True)
        (evidence/'exports.log').write_text(exports)
        names=[line.split()[-1].split('@')[0] for line in exports.splitlines() if line.strip()]
        if set(names)!={'OG_LAKE_PARQUET_1','og_lake_parquet_sdk_v1'}:raise RuntimeError('unexpected C++ exports')
        dynamic=subprocess.check_output(['readelf','-d',str(library)],text=True)
        (evidence/'dynamic.log').write_text(dynamic)
        rpaths=re.findall(r'\((?:RUNPATH|RPATH)\).*?\[(.*?)\]',dynamic)
        if rpaths!=['$ORIGIN']:raise RuntimeError('provider has unexpected runtime search path')
        links=subprocess.check_output(['ldd',str(library2)],env=env,text=True)
        (evidence/'relocated-ldd.log').write_text(links)
        if 'libarrow.so' in links or 'libparquet.so' in links or 'not found' in links:
            raise RuntimeError('provider must seal its Arrow/Parquet dependencies inside the C ABI library')
        receipt['dependency_isolation']='PIC static archives, hidden by the version script and exclude-libs; no Arrow/Parquet shared dependency'
        for entry in frozen:
            if sha(entry['path'])!=entry['sha256'] or sha(entry['archive'])!=entry['sha256']:
                raise RuntimeError('qualification inputs changed')
        receipt['provider_sources_after']=inputs();receipt['dependencies_after']=dependencies()
        if receipt['provider_sources_before']!=receipt['provider_sources_after'] or receipt['dependencies_before']!=receipt['dependencies_after'] or sha(library)!=receipt['library_sha256']:
            raise RuntimeError('provider changed during qualification')
        receipt['status']='passed';print('standalone_local_provider_passed',flush=True);return 0
    except Exception as exc:
        receipt['error']=str(exc);print(str(exc),flush=True);return 1
    finally:(evidence/'index.json').write_text(json.dumps(receipt,indent=2)+'\n')

if __name__=='__main__':raise SystemExit(main())
