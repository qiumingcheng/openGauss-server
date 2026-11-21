#!/usr/bin/env python3
"""Verify exact isolated codec evidence and local C ABI lifetimes before installation."""
import argparse
import datetime
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import traceback
from build_provider import ROOT,SOURCE,PREFIX,GCC,sha,inputs,dependencies

def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--evidence',type=Path,required=True)
    p.add_argument('--build',type=Path,required=True)
    for name in ['list','float','faults','lifecycle']:p.add_argument('--'+name+'-result',type=Path,required=True)
    a=p.parse_args()
    for key in ['build','list_result','float_result','faults_result','lifecycle_result']:setattr(a,key,getattr(a,key).resolve())
    out=a.evidence.resolve();out.mkdir(parents=True,exist_ok=False)
    r={'status':'failed','scope':'Local provider installation eligibility only; no native SQL, remote SDK, catalog GC, RAC or commercial qualification','started_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'script_sha256':sha(__file__),'cases':[]}
    def command(label,args,env=None):
        with(out/(label+'.log')).open('w')as log:subprocess.run(list(map(str,args)),env=env,stdout=log,stderr=subprocess.STDOUT,check=True,timeout=300)
        return(out/(label+'.log')).read_text()
    try:
        b=json.loads(a.build.read_text());library=Path(b['library']);prefix=Path(b['provider_prefix'])
        assert b['status']=='provider_built_unqualified'and sha(library)==b['sha256']
        assert b['sources_before']==b['sources_after']==inputs()
        assert b['dependencies_before']==b['dependencies_after']==dependencies()
        r.update(provider_sources_before=inputs(),dependencies_before=dependencies(),library_sha256=sha(library),provider_prefix=str(prefix),build=str(a.build.resolve()),build_sha256=sha(a.build))
        for name in ['list','float','faults','lifecycle']:
            path=getattr(a,name+'_result').resolve();d=json.loads(path.read_text());assert d['status']=='passed'
            if name in ['list','float']:
                assert d['provider_sha256']==sha(library)
                assert d['assertions']and all(v['status']=='pass'and v['observed']==v['expected']for v in d['assertions'])
                for f,h in d['inputs'].items():assert sha(path.parent/'executed-inputs'/d['archive_index'][f])==h
                for f,h in d['artifacts'].items():assert sha(f)==h
                assert d['consumer']['status']=='pass'and d['consumer']['checks']>0
                if name=='list':
                    assert d['pre_list_writer']['status']=='passed'and sha(path.parent/'old-writer')==d['pre_list_writer_sha256']
                else:assert d['pre_float_consumer']['status']=='passed'and sha(path.parent/'pre-float-consumer')==d['pre_float_consumer_sha256']
            elif name=='faults':
                assert d['library_sha256']==sha(library)and d['provider_sources_after']==inputs()and d['dependencies_after']==dependencies()
                assert d['faults']['status']=='passed'and d['faults']['assertions']==55 and d['borrowed_reference_differences']==0
                assert sha(path.parent/'writer-fault-c-host')==d['consumer_sha256']and sha(path.parent/'io-faults.so')==d['shim_sha256']
                for key,filename in [('test_sha256','provider_writer_fault_test.c'),('faults_sha256','provider_io_faults.c'),('common_test_sha256','provider_reader_test.c'),('driver_sha256','test_writer_faults.py')]:assert sha(Path(__file__).with_name(filename))==d[key]
            else:
                assert d['provider_sources_after']==inputs()and d['dependencies_after']==dependencies()
                assert d['delete']['status']=='pass'and d['delete']['checks']==340 and len(d['original_lifecycle_cases'])==6
                assert all(v['status']=='pass'for v in d['original_lifecycle_cases'])
                for f,h in d['inputs'].items():assert sha(f)==h
            r['cases'].append({'name':name,'path':str(path),'sha256':sha(path)})
        # Runtime identity and immutable-object replacement remain required after
        # extending the schema reader, in addition to original ABI1/2/3 lifetimes.
        env=dict(os.environ);env['LD_LIBRARY_PATH']=':'.join(map(str,[GCC/'lib64']+[GCC.parent/x/'lib'for x in ['isl','mpc','mpfr','gmp']]))
        test=Path(__file__).with_name('provider_object_version_test.c');exe=out/'object-version-consumer'
        command('compile-object-version',[GCC/'bin/gcc','-std=c11','-O0','-g','-Wall','-Wextra','-Werror','-Wno-misleading-indentation','-I',SOURCE,test,'-ldl','-o',exe],env)
        f=json.loads(a.float_result.read_text());typed=a.float_result.parent/'typed.parquet'
        first=out/'mutable.parquet';replacement=out/'replacement.parquet';shutil.copy2(typed,first);shutil.copy2(typed,replacement)
        v=json.loads(command('object-version',[exe,library,first,replacement,'version'],env));assert v['status']=='pass';r['object_version']=v
        r['object_version_consumer_sha256']=sha(exe);r['object_version_test_sha256']=sha(test)
        exports=command('exports',['nm','-D','--defined-only',library]);names={line.split()[-1].split('@')[0]for line in exports.splitlines()if line.strip()}
        assert names=={v for n in range(1,5)for v in ['OG_LAKE_PARQUET_'+str(n),'og_lake_parquet_sdk_v'+str(n)]}
        dynamic=command('dynamic',['readelf','-d',library]);assert re.findall(r'\((?:RUNPATH|RPATH)\).*?\[(.*?)\]',dynamic)==['$ORIGIN']
        relocated=out/'relocated';relocated.mkdir();copy=relocated/library.name;shutil.copy2(library,copy)
        links=command('relocated-links',['ldd',copy],env);assert 'libarrow.so'not in links and'libparquet.so'not in links and'not found'not in links
        reader=a.float_result.parent/'pre-float-consumer';args=[a.float_result.parent/n for n in ['typed.parquet','no_ids.parquet','bad_magic.parquet','truncated.parquet','missing.parquet']]
        value=json.loads(command('relocated-reader',[reader,copy,*args],env));assert value['status']=='passed';r['relocated_reader']=value
        r.update(provider_sources_after=inputs(),dependencies_after=dependencies())
        assert r['provider_sources_after']==r['provider_sources_before']and r['dependencies_after']==r['dependencies_before']and sha(library)==r['library_sha256']
        archive=out/'executed-inputs';archive.mkdir();r['archive_index']={}
        for i,pth in enumerate([Path(__file__),test,*SOURCE.iterdir()]):
            if pth.is_file():
                name=str(i)+'-'+pth.name;shutil.copy2(pth,archive/name);r['archive_index'][str(pth)]={'archive':name,'sha256':sha(pth)}
        r.update(status='passed',dependency_isolation='PIC static Arrow/Parquet archives, hidden C++ symbols; relocatable C ABI library')
    except BaseException as exc:r.update(reason=str(exc),traceback=traceback.format_exc())
    r['finished_at']=datetime.datetime.now(datetime.timezone.utc).isoformat();(out/'index.json').write_text(json.dumps(r,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps(dict(status=r['status'],reason=r.get('reason'),result=str(out/'index.json')),ensure_ascii=False));return 0 if r['status']=='passed'else 1
if __name__=='__main__':raise SystemExit(main())
