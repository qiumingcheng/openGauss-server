#!/usr/bin/env python3
"""Standalone actual SQL020 signed16 SDK qualification; prepare unless --execute.
Never installs a live provider, builds Native, or opens a database session.
"""
import argparse,fcntl,hashlib,json,os,re,shutil,subprocess,sys,traceback
from copy import deepcopy
from pathlib import Path
D=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(D),str(Path(os.getenv('CPP_WORKSPACE','/home/qiumc/data/code/cpp'))/'integration/opengauss-native-lake')]
from pq_sql020_build_inputs import SDK_SOURCES,SDK_KIND,assert_records,digest,verify_map,sdk_index


def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--execute',action='store_true');p.add_argument('--workspace',type=Path,required=True);p.add_argument('--run-id',required=True)
 p.add_argument('--provider-build',type=Path);p.add_argument('--source',type=Path);p.add_argument('--legacy-source',type=Path)
 a=p.parse_args();root=a.workspace.resolve(strict=True)
 if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,59}',a.run_id):p.error('safe new run-id max60')
 if not a.execute:print(json.dumps(dict(status='prepared_not_executed',kind=SDK_KIND,cases=12,sdk_loads=0,builds=0)));return 0
 if not a.provider_build or not a.source or not a.legacy_source:p.error('--execute requires provider-build/source/legacy-source')
 src=a.source.resolve(strict=True)/'contrib/og_lake';legacy=a.legacy_source.resolve(strict=True)/'contrib/og_lake';qa=Path(__file__).with_name('sql020_qa')
 out=root/'公共文档/openGauss原生湖存储/验证'/a.run_id;out.mkdir(exist_ok=False)
 report=dict(status='running',schema_version=1,native_integration=False,source_lock_updated=False,assertions=[],cases={},input_files={},commands=[])
 reservation=None;provider=None
 def check(name,observed,expected=True):
  ok=observed==expected;report['assertions'].append(dict(name=name,observed=deepcopy(observed),expected=deepcopy(expected),status='pass'if ok else'fail'))
  (out/'working.json').write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
  if not ok:raise RuntimeError(name+': '+repr(observed))
 env=dict(os.environ,CPP_WORKSPACE=str(root),OMP_NUM_THREADS='1',OPENBLAS_NUM_THREADS='1',PYTHONDONTWRITEBYTECODE='1')
 gcc=root/'binarylibs/buildtools/gcc10.3/gcc';arrow=root/'.reference/opengauss-native-lake/sdk/install/arrow-20.0.0'
 env['LD_LIBRARY_PATH']=':'.join(map(str,[gcc/'lib64']+[gcc.parent/x/'lib'for x in ['isl','mpc','mpfr','gmp']]))
 def run(case,name,argv,extra=None,parse=False):
  row=dict(name=name,argv=list(map(str,argv)),returncode=None);report['commands'].append(row);report['cases'].setdefault(case,dict(status='running',commands=[]))['commands'].append(row)
  with(out/(name+'.stdout')).open('w')as so,(out/(name+'.stderr')).open('w')as se:
   done=subprocess.run(row['argv'],stdin=subprocess.DEVNULL,stdout=so,stderr=se,env=dict(env,**(extra or{})),timeout=600)
  row['returncode']=done.returncode
  if done.returncode:raise RuntimeError(name+' failed; original stdout/stderr retained')
  return json.loads((out/(name+'.stdout')).read_text())if parse else(out/(name+'.stdout')).read_text()
 def passed(case):report['cases'][case]['status']='pass'
 try:
  reservation=(root/'.reference/opengauss-native-lake/.driver.lock').open('a+');fcntl.flock(reservation.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB)
  for name,h in SDK_SOURCES.items():check('exact signed16 source '+name,digest(src/name),h)
  check('old full header snapshot',digest(legacy/'sdk/lake_parquet_sdk.h'),'6b579aedb46200133ab607d109d02ef492f3339eced418fbfe5b7bc8f3052ea0')
  build=json.loads(a.provider_build.read_text());check('actual SDK build completed',build['status'],'provider_built_unqualified')
  check('sources stable during SDK build',build['sources_before'],build['sources_after']);check('dependencies stable during SDK build',build['dependencies_before'],build['dependencies_after'])
  import importlib.util
  builder_path=root/'integration/opengauss-native-lake/sdk/build_provider.py'
  spec=importlib.util.spec_from_file_location('qualified_sdk_builder',builder_path);builder=importlib.util.module_from_spec(spec);spec.loader.exec_module(builder)
  check('actual provider builder identity',build['script_sha256'],digest(builder_path))
  check('actual fixed Arrow/toolchain dependencies',build['dependencies_after'],builder.dependencies())
  check('provider build source path',str(Path(build['provider_source']).resolve()),str(src/'sdk'))
  prefix=Path(build['provider_prefix']).resolve(strict=True);provider=Path(build['library']).resolve(strict=True)
  check('new isolated provider namespace',prefix.is_relative_to(root/'.install/opengauss-native-lake-sdk-smallint'))
  check('exact actual library location',str(provider),str(prefix/'lib/libog_lake_sdk_arrow.so'));check('actual built library',digest(provider),build['sha256'])
  for name,h in SDK_SOURCES.items():
   if name.startswith('sdk/'):check('actually compiled provider '+name,build['sources_after'][name.split('/',1)[1]],h)
  origin=root/'.reference/opengauss-native-lake/sdk/install/binary-transport/lib/libog_lake_sdk_arrow.so';precap=root/'.reference/opengauss-native-lake/sdk/install/object-version-candidate/lib/libog_lake_sdk_arrow.so'
  check('genuine5ae origin',digest(origin),'5ae9f8fecba4b0565b414e766cfad4dbc62e7f8714326f7610c8a2e51e3c083a');check('genuine pre-codec origin',digest(precap),'142b59847b047bd1dd988122d36e5d09335e430ec60314f357aa0c7760413529')
  sdkq=root/'公共文档/openGauss原生湖存储/验证/sdk_binary_transport_scoped_verified_index.json';check('old provider authority',digest(sdkq),'fbde68f0b5620d9b4f2f0fe1c4c948616db3570352feac26769403ce5b63fb05')
  duckreceipt=root/'.reference/opengauss-native-lake/build/receipt.json';dr=json.loads(duckreceipt.read_text());duck=Path(dr['binary']);check('fixed independent DuckDB',digest(duck),dr['binary_sha256'])
  files=[Path(__file__),D/'pq_sql020_build_inputs.py',root/'integration/opengauss-native-lake/pq_native_build_inputs.py',a.provider_build.resolve(),sdkq,duckreceipt,duck,origin,precap,gcc/'bin/gcc',gcc/'bin/g++',builder_path]
  files +=[x for x in (arrow/'include').rglob('*')if x.is_file()and'og_lake'not in x.parts]
  files +=[arrow/'lib'/name for name in ['libarrow.a','libparquet.a','libarrow_bundled_dependencies.a','libarrow.so','libparquet.so']]
  files+=[src/x for x in SDK_SOURCES]+[legacy/'sdk/lake_parquet_sdk.h',legacy/'vendor/arrow20/abi.h']+[x for x in qa.iterdir()if x.is_file()and x.suffix in ['.py','.c','.cpp']]
  frozen=root/'公共文档/openGauss原生湖存储/验证/sdk_binary_float_1109_01';files +=[frozen/x for x in ['typed.parquet','no_ids.parquet','bad_magic.parquet','truncated.parquet']]
  oldinput=root/'公共文档/openGauss原生湖存储/验证/sdk_list_ids_float_1109_01/typed.parquet';files.append(oldinput)
  report['input_files']={str(x.resolve()):digest(x)for x in files};archive=out/'inputs';archive.mkdir()
  for i,x in enumerate(files):shutil.copy2(x,archive/(str(i)+'-'+x.name))
  guard=[root/'.install/opengauss-native-lake-debug/lib/postgresql/og_lake_sdk/libog_lake_sdk_arrow.so',root/'.install/opengauss-native-lake-debug/bin/gaussdb'];before={str(x):digest(x)for x in guard}
  layout=[]
  for tag,header in [('old',legacy/'sdk'),('new',src/'sdk')]:
   exe=out/('layout-'+tag);run('layout','compile-layout-'+tag,[gcc/'bin/g++','-std=c++14','-Wall','-Wextra','-Werror','-I',header,qa/'sdk_layout.cpp','-o',exe]);layout.append(run('layout','run-layout-'+tag,[exe],parse=True))
  check('all real C ABI layout values identical',layout[0],layout[1]);check('capabilities actual offset',layout[1]['OgLakeParquetSdkApi.capabilities'],24);passed('layout')
  exe=out/'signed16-consumer';run('signed16','compile-signed16',[gcc/'bin/g++','-std=c++14','-O0','-g','-Wall','-Wextra','-Werror','-I',src/'sdk','-I',src,qa/'sdk_int16_probe.cpp','-ldl','-o',exe])
  data=out/'signed16.parquet';got=run('signed16','signed16-full-domain',[exe,provider,data],parse=True);check('actual signed16 consumer',got['status'],'pass');check('all two-column domain cells checked',got['checked_cells'],131072);passed('signed16')
  old=run('old-provider-refusal','old-capability-refusal',[exe,origin,out/'must-not-exist.parquet','--expect-missing'],parse=True);check('old refusal exact',old,{'status':'pass_old_capability_refusal','file_calls':0});check('no unsupported artifact',(out/'must-not-exist.parquet').exists(),False);passed('old-provider-refusal')
  literal="'"+str(data).replace("'","''")+"'"
  schema=run('signed16-independent','duck-int16-schema',[duck,'-no-init','-batch','-json','-c','SET threads=1;SELECT name,type,converted_type,logical_type,field_id FROM parquet_schema('+literal+');'],parse=True)
  fields={x['name']:x for x in schema if x['name']in ['whole','maybe']};check('both actual INT32 signed16 fields',sorted(fields),['maybe','whole'])
  for name,fid in [('whole',73),('maybe',9)]:
   x=fields[name];check('actual standard signed16 field/'+name,x['type']=='INT32'and x['field_id']==fid and(x['converted_type']=='INT_16'or('16'in str(x['logical_type'])and re.search(r'isSigned\s*=\s*(?:1|true)',str(x['logical_type']),re.I)is not None)))
  desc=run('signed16-independent','duck-int16-types',[duck,'-no-init','-batch','-json','-c','DESCRIBE SELECT * FROM read_parquet('+literal+');'],parse=True);check('independent SQL SMALLINT types',[x['column_type']for x in desc],['SMALLINT','SMALLINT'])
  sql='SELECT count(*)::BIGINT AS rows,min(whole)::BIGINT AS lo,max(whole)::BIGINT AS hi,sum(whole)::BIGINT AS total,count(maybe)::BIGINT AS n,min(maybe)::BIGINT AS nlo,max(maybe)::BIGINT AS nhi,sum(maybe)::BIGINT AS ntotal FROM read_parquet('+literal+');'
  observed=run('signed16-independent','duck-int16-values',[duck,'-no-init','-batch','-json','-c',sql],parse=True)[0];check('independent whole signed16/NULL domain',observed,{'rows':65536,'lo':-32768,'hi':32767,'total':-32768,'n':61680,'nlo':-32767,'nhi':32766,'ntotal':-30840});report['int16_observed']=observed;passed('signed16-independent')
  for case,source in [('old-reader','provider_reader_test.c'),('old-writer','provider_writer_test.c'),('writer-io-faults','provider_writer_fault_test.c')]:
   binary=out/case;run(case,'compile-'+case,[gcc/'bin/gcc','-std=c11','-O2','-Wall','-Wextra','-Werror','-I',legacy/'sdk','-I',arrow/'include',qa/source,'-ldl','-o',binary])
   if case=='old-reader':argv=[binary,provider,*[frozen/x for x in ['typed.parquet','no_ids.parquet','bad_magic.parquet','truncated.parquet']],out/'missing.parquet'];extra=None
   elif case=='old-writer':argv=[binary,provider,frozen/'typed.parquet',*[out/x for x in ['written.parquet','abort.parquet','cancel.parquet','replaced.parquet','empty.parquet']]];extra=None
   else:
    shim=out/'io-faults.so';run(case,'compile-io-shim',[gcc/'bin/gcc','-std=c11','-O2','-Wall','-Wextra','-Werror','-fPIC','-shared',qa/'provider_io_faults.c','-ldl','-o',shim]);folder=out/'io-faults';folder.mkdir();argv=[binary,provider,frozen/'typed.parquet',folder];extra={'LD_PRELOAD':str(shim)}
   run(case,'run-'+case,argv,extra,parse=True)
   if case=='old-writer':
    def lit(x):return "'"+str(x).replace("'","''")+"'"
    original='SELECT * FROM read_parquet('+lit(frozen/'typed.parquet')+')';actual='SELECT * FROM read_parquet('+lit(out/'written.parquet')+')'
    diff=run(case,'independent-old-writer',[duck,'-no-init','-batch','-json','-c','SELECT count(*)::BIGINT AS n FROM (('+actual+' EXCEPT ALL '+original+') UNION ALL ('+original+' EXCEPT ALL '+actual+'));'],parse=True)
    check('old ABI writer full independent multiset',diff,[{'n':0}])
   passed(case)
  case='lifecycle';binary=out/'old-lifecycle';folder=out/'lifecycle-files';folder.mkdir()
  run(case,'compile-old-lifecycle',[gcc/'bin/gcc','-std=c11','-O2','-Wall','-Wextra','-Werror','-I',legacy/'sdk','-I',arrow/'include',qa/'provider_lifecycle_test.c','-ldl','-o',binary])
  lifecycle=[json.loads(line)for line in run(case,'run-old-lifecycle',[binary,provider,frozen/'typed.parquet',folder],{'LD_PRELOAD':str(out/'io-faults.so')}).splitlines()if line.strip()]
  check('actual original lifecycle six modes',[x['mode']for x in lifecycle],['close-retains','abort-finished','abort-unfinished','replace-finished','abort-fsync-error','abort-cancel-pending'])
  check('all original lifecycle modes passed',[x['status']for x in lifecycle],['pass']*6)
  check('original lifecycle FD baseline preserved',len({x['fds']for x in lifecycle})==1 and lifecycle[0]['fds']>=0)
  report['lifecycle_observed']=lifecycle;passed(case)
  for case in ['float','list','binary']:
   script=qa/('test_'+('floats'if case=='float'else'lists'if case=='list'else'binary')+'.py');child_id=a.run_id+'_'+case
   argv=[sys.executable,script,'--run-id',child_id,'--provider-build',a.provider_build.resolve(),'--source',a.source.resolve(),'--old-provider',precap]
   if case!='binary':argv+=['--legacy-source',a.legacy_source.resolve()]
   if case=='list':argv+=['--legacy-input',oldinput]
   run(case,'run-'+case,argv);child=root/'公共文档/openGauss原生湖存储/验证'/child_id/'result.json';r=json.loads(child.read_text());check('actual old codec case '+case,r['status'],'passed');assert_records(r['assertions'])
   report['input_files'].update(r['inputs']);report.setdefault('child_results',[]).append(str(child));passed(case)
  text=run('symbols','dynamic-symbols',['nm','-D','--defined-only',provider]);names={x.split()[-1].split('@')[0]for x in text.splitlines()if x.split()};names={x for x in names if not x.startswith('OG_LAKE_PARQUET_')};check('only exact four C entrypoints dynamically exported',sorted(names),['og_lake_parquet_sdk_v1','og_lake_parquet_sdk_v2','og_lake_parquet_sdk_v3','og_lake_parquet_sdk_v4']);passed('symbols')
  check('current installed SDK/native preserved',{str(x):digest(x)for x in guard},before);verify_map(report['input_files'])
  owner=prefix/'.og-lake-sdk-owner.json';ownerdata=dict(project='og-lake-sdk-sql020-int16',prefix=str(prefix),provider_build_sha256=digest(a.provider_build),source_header_sha256=SDK_SOURCES['sdk/lake_parquet_sdk.h'])
  if owner.exists():check('existing same SDK build owner',json.loads(owner.read_text()),ownerdata)
  else:owner.write_text(json.dumps(ownerdata,indent=2)+'\n')
  report.update(qualifier={'path':str(Path(__file__).resolve()),'sha256':digest(Path(__file__).resolve())},status=SDK_KIND,sdk_source=str(src),prefix=str(prefix),library=str(provider),library_sha256=digest(provider),old_provider_sha256=digest(origin),provider_build={'path':str(a.provider_build.resolve()),'sha256':digest(a.provider_build)},prefix_files={str(x.relative_to(prefix)):digest(x)for x in prefix.rglob('*')if x.is_file()})
  proof={str(x.resolve()):digest(x)for x in out.rglob('*')if x.is_file()and x.name!='index.json'}
  for child in report.get('child_results',[]):
   for x in Path(child).parent.rglob('*'):
    if x.is_file():proof[str(x.resolve())]=digest(x)
  report['proof_files']=proof;indexpath=out/'candidate-index.json';indexpath.write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n');sdk_index(indexpath,digest(indexpath),root);indexpath.rename(out/'index.json')
 except BaseException as e:
  report.update(status='failed',reason=str(e),traceback=traceback.format_exc());(out/'failed.json').write_text(json.dumps(report,ensure_ascii=False,indent=2,default=lambda x:sorted(x))+'\n');print(json.dumps(dict(status='failed',reason=str(e),out=str(out))));return 1
 finally:
  if reservation is not None:reservation.close()
 print(json.dumps(dict(status=report['status'],index=str(out/'index.json'),sha256=digest(out/'index.json'))));return 0
if __name__=='__main__':raise SystemExit(main())
