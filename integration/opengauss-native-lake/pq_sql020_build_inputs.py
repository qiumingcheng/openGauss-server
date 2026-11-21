"""Exact additive SQL020 SDK/PQ identity. Legacy admission stays untouched.

A matching C layout is insufficient: both actual SDK and PQ component results,
all full public headers, sources, tools, helpers, prefix inventories and symbol
isolation must remain exact. No build/SDK/Native operation occurs on import.
"""
import hashlib,json
from pathlib import Path
from pq_native_build_inputs import (digest,read,safe,verify_files,qualified_sources as legacy_sources,
    blas_inputs,global_symbols,PUBLIC_PREFIXES)
KIND='pq_sql020_int16_v1'
SDK_SOURCES={
 'sdk/lake_parquet_sdk.h':'25782cbea04aa2cb8934b5a84c34e7bb3df3188a5fc9264b1c17a10b5e62babd',
 'sdk/arrow_parquet_provider.cpp':'57e0901f4b574ed9833b667d0940dbc2b01d3532497b5ecacc172d151e998091',
 'sdk/CMakeLists.txt':'b0390e048c5b2287fd95fce5f37de41a7bacda075a8b8e77d2b32fef5c862707',
 'sdk/exports.map':'bf1cd5a31bbefdd4fb00b566f79bc276b54313fe2a29243a182be9ef6a709d72',
 'vendor/arrow20/abi.h':'1828f1ce60368cd590d19595910fa19db09b30728f8a43b5f89933623f7a7276'}
SDK_CASES={'layout','signed16','old-provider-refusal','signed16-independent','old-reader','old-writer','writer-io-faults','lifecycle','float','list','binary','symbols'}
PQ_TAGS={'metric-1','metric-2','metric-3','empty','d3','d1536'}
SDK_QUALIFIER_SHA256='b8b95257af3dd5ee1d0814ae2d8eb0bdd8eb1b9b63a45a3ef399a135a2b5220f'
PQ_QUALIFIER_SHA256='6816387ec1123a8390abd6fc9ef217a92edf463e21b62adbf1d8b59a53110987'
SDK_KIND='verified_independent_sdk_sql020_int16_only'
PQ_KIND='verified_independent_pq_sql020_parquet_only'


def verify_map(files):
 if not isinstance(files,dict)or not files:raise RuntimeError('Missing exact immutable input/proof files')
 for name,expected in files.items():
  p=Path(name)
  if not p.is_absolute()or not p.is_file()or digest(p)!=expected:raise RuntimeError('Qualification input/proof changed: '+name)


def assert_records(rows):
 if not rows or any(x.get('status')!='pass'or'expected'not in x or'observed'not in x or x['observed']!=x['expected']for x in rows):
  raise RuntimeError('Actual assertion records absent, failed or mutable')
 return len(rows)


def index(path,expected,kind):
 path=Path(path).resolve(strict=True)
 if digest(path)!=expected:raise RuntimeError('Exact selected component index hash mismatch')
 d=read(path)
 if d.get('status')!=kind or d.get('native_integration')is not False or d.get('source_lock_updated')is not False or d.get('schema_version')!=1:
  raise RuntimeError('SQL020 independent component scope mismatch')
 verify_map(d['input_files']);verify_map(d['proof_files']);assert_records(d['assertions'])
 return d


def sdk_index(path,expected,workspace):
 d=index(path,expected,SDK_KIND);root=Path(workspace).resolve(strict=True)
 if d.get('qualifier',{}).get('sha256')!=SDK_QUALIFIER_SHA256 or digest(Path(d['qualifier']['path']))!=SDK_QUALIFIER_SHA256:raise RuntimeError('SDK qualification runner identity mismatch')
 src=Path(d['sdk_source']).resolve(strict=True)
 for name,h in SDK_SOURCES.items():
  if digest(src/name)!=h or d['input_files'].get(str(src/name))!=h:raise RuntimeError('SQL020 exact SDK public/source identity mismatch: '+name)
 prefix=safe(d['prefix']);library=safe(d['library'])
 if not prefix.is_relative_to(root/'.install/opengauss-native-lake-sdk-smallint')or library!=prefix/'lib/libog_lake_sdk_arrow.so':
  raise RuntimeError('SQL020 provider must use its new private SDK prefix')
 verify_files(prefix,d['prefix_files'])
 if d['prefix_files'].get('include/og_lake/lake_parquet_sdk.h')!=SDK_SOURCES['sdk/lake_parquet_sdk.h']:raise RuntimeError('SDK installed a different public header')
 owner=read(prefix/'.og-lake-sdk-owner.json')
 if owner!={'project':'og-lake-sdk-sql020-int16','prefix':str(prefix),'provider_build_sha256':d['provider_build']['sha256'],'source_header_sha256':SDK_SOURCES['sdk/lake_parquet_sdk.h']}:raise RuntimeError('SDK owner marker differs')
 if digest(library)!=d['library_sha256']:raise RuntimeError('Qualified SQL020 provider changed')
 buildpath=Path(d['provider_build']['path']);build=read(buildpath)
 if digest(buildpath)!=d['provider_build']['sha256']or build.get('status')!='provider_built_unqualified'or build['sha256']!=d['library_sha256']:
  raise RuntimeError('SDK provider build receipt mismatch')
 if build['sources_before']!=build['sources_after']or build['dependencies_before']!=build['dependencies_after']:
  raise RuntimeError('SDK build input transition')
 for name,h in SDK_SOURCES.items():
  if name.startswith('sdk/')and build['sources_after'].get(name.split('/',1)[1])!=h:raise RuntimeError('SDK built a different source')
 if set(d['cases'])!=SDK_CASES:raise RuntimeError('SDK ABI/old-type/full-domain cases incomplete')
 for case in d['cases'].values():
  if case.get('status')!='pass'or not case.get('commands')or any(c.get('returncode')!=0 for c in case['commands']):
   raise RuntimeError('SDK required actual command did not pass')
 if d['int16_observed']!={'rows':65536,'lo':-32768,'hi':32767,'total':-32768,'n':61680,'nlo':-32767,'nhi':32766,'ntotal':-30840}:
  raise RuntimeError('Signed16 independent whole-domain oracle absent')
 if d.get('old_provider_sha256')!='5ae9f8fecba4b0565b414e766cfad4dbc62e7f8714326f7610c8a2e51e3c083a':raise RuntimeError('Not genuine qualified5ae old-provider negative')
 return dict(path=str(Path(path).resolve()),sha256=expected,sdk_source=str(src),prefix=str(prefix),library=str(library),library_sha256=d['library_sha256'],header_sha256=SDK_SOURCES['sdk/lake_parquet_sdk.h'])


def pq_component(path,expected,workspace):
 d=index(path,expected,PQ_KIND)
 if d.get('qualifier',{}).get('sha256')!=PQ_QUALIFIER_SHA256 or digest(Path(d['qualifier']['path']))!=PQ_QUALIFIER_SHA256:raise RuntimeError('PQ qualification runner identity mismatch')
 sdk=sdk_index(d['sdk_index']['path'],d['sdk_index']['sha256'],workspace)
 report=read(d['report']['path'])
 if digest(Path(d['report']['path']))!=d['report']['sha256']or report.get('status')!='passed_independent_pq_parquet_component_only':raise RuntimeError('PQ actual report mismatch')
 assert_records(report['assertions'])
 if {x['tag']for x in report['cases']}!=PQ_TAGS:raise RuntimeError('PQ numerical/schema/empty/large-dim case omitted')
 names={c['name']for c in report['commands']}
 required={p+tag for tag in PQ_TAGS for p in ['producer-','consumer-','duck-model-','duck-codes-','duck-manifest-']}
 required|={'negative-matrix','writer-errors','allocation-reader-baseline','compile','dynamic-symbols'}
 baseline=report['allocation_baseline'];points=sorted({x for x in [1,2,3,baseline['calls']//2,baseline['calls']-1,baseline['calls']]if x>0})
 polls=sorted({x for x in [1,2,baseline['polls']//2,baseline['polls']-1,baseline['polls']]if x>0})
 required|={'allocation-reader-'+str(x)for x in points}|{'cancel-reader-'+str(x)for x in polls}
 if not required<=names or any(c.get('returncode')!=0 for c in report['commands']):raise RuntimeError('PQ original probe/oracle/fault matrix incomplete')
 if points!=report['allocation_selected_points']or polls!=report['cancellation_selected_polls']:raise RuntimeError('PQ fault points not from actual new baseline')
 if report['writer_errors']['cleanup_failures']!=0 or report['negative_matrix']['status']!='pass':raise RuntimeError('PQ failure/partial-publication oracle absent')
 if report['provider_sha256']!=sdk['library_sha256']or report['sdk_source']!=sdk['sdk_source']:raise RuntimeError('PQ used different SDK')
 return dict(path=str(Path(path).resolve()),sha256=expected,sdk_index=sdk,asan=bool(report.get('sanitizer')))


def qualified_sources(workspace,component,component_sha256,asan_component,asan_sha256):
 legacy=legacy_sources(workspace)
 normal=pq_component(component,component_sha256,workspace);asan=pq_component(asan_component,asan_sha256,workspace)
 if normal['asan']or not asan['asan']or normal['sdk_index']!=asan['sdk_index']:raise RuntimeError('Require separate normal and ASan/UBSan new-header component qualification')
 return dict(base_qualified_sources=legacy,source=legacy['source'],sdk_include=normal['sdk_index']['sdk_source'],source_commit=legacy['source_commit'],
  sql020_component=normal,sql020_asan_component=asan,sdk_index=normal['sdk_index'])


def pq_inputs(receipt,workspace):
 if receipt is None:return None
 receipt=Path(receipt).resolve(strict=True);d=read(receipt)
 if d.get('dependency_kind')!=KIND or d.get('status')!='pq_static_built_unlinked'or d.get('native_integration')is not False or d.get('manifest_version')!=1:
  raise RuntimeError('SQL020 requires exact independently-qualified dependency kind')
 if digest(Path(__file__).resolve())!=d['admission_helper_sha256']:raise RuntimeError('SQL020 admission implementation differs from built receipt')
 q=d['qualification'];qualified=qualified_sources(workspace,q['normal']['path'],q['normal']['sha256'],q['asan']['path'],q['asan']['sha256']);blas=blas_inputs(workspace)
 if d['qualified_sources']!=qualified or d['blas_inputs']!=blas:raise RuntimeError('SQL020 PQ source/SDK/component qualification changed')
 prefix=safe(d['prefix']);include=safe(d['include']);library=safe(d['library']);bl=safe(d['blas_library'])
 if not prefix.is_relative_to(Path(workspace).resolve()/'.install/opengauss-native-lake-pq-static')or include!=prefix/'include'or library!=prefix/'lib/libog_lake_pq_cpu.a'or bl!=prefix/'lib/libog_lake_pq_openblas.a':raise RuntimeError('SQL020 archives escaped owned prefix')
 verify_files(prefix,d['files']);owner=prefix/'.og-lake-pq-owner.json'
 if digest(owner)!=d['owner_sha256']or read(owner)!=d['owner']or d['owner'].get('prefix')!=str(prefix)or d['owner'].get('dependency_kind')!=KIND:raise RuntimeError('SQL020 PQ owner identity mismatch')
 verify_map(d['helper_inputs'])
 for name,item in d['build_tools'].items():
  if digest(Path(item['path']))!=item['sha256']:raise RuntimeError('SQL020 compiler/tool identity changed: '+name)
 for name in ['include/pq_cpu.h','include/pq_parquet_format.h','include/pq_parquet_frontier_draft.h','include/sdk/lake_parquet_sdk.h','include/vendor/arrow20/abi.h','licenses/Microsoft-DiskANN-LICENSE','licenses/OpenBLAS/LICENSE','symbol-map.txt']:
  if name not in d['files']:raise RuntimeError('Incomplete SQL020 installed public archive/ABI/license')
 for name,h in [('sdk/lake_parquet_sdk.h',SDK_SOURCES['sdk/lake_parquet_sdk.h']),('vendor/arrow20/abi.h',SDK_SOURCES['vendor/arrow20/abi.h'])]:
  if d['files']['include/'+name]!=h:raise RuntimeError('New archive does not carry exact new SDK header')
 owned=sorted(global_symbols(library))
 if digest(library)!=d['library_sha256']or digest(bl)!=blas['library_sha256']or owned!=d['owned_symbols']or any(not x.startswith(PUBLIC_PREFIXES+('oglpqcpu_',))for x in owned):raise RuntimeError('SQL020 static symbol/byte identity mismatch')
 smoke=d.get('standalone_receipt',{})
 if smoke.get('returncode')!=0 or smoke.get('mode')!='shapes'or d['symbol_isolation'].get('standalone_dynamic_overlap_count')!=0:raise RuntimeError('Archive smoke/symbol isolation unqualified')
 archive=d.get('archive_parquet_receipt',{})
 if archive.get('returncodes')!=[0,0]or not archive.get('manifest'):raise RuntimeError('New archive Parquet producer/consumer not actually linked/run')
 for name in ['pq_parquet_format','pq_parquet_frontier_draft']:
  entry=d.get('sdk_header_dependencies',{}).get(name,{})
  if entry.get('header_sha256')!=SDK_SOURCES['sdk/lake_parquet_sdk.h']or digest(Path(entry['path']))!=entry.get('sha256'):raise RuntimeError('New archive actual SDK include dependency unproven')
 return dict(dependency_kind=KIND,receipt=str(receipt),receipt_sha256=digest(receipt),prefix=str(prefix),include=str(include),library=str(library),blas_library=str(bl),
  library_sha256=digest(library),blas_library_sha256=digest(bl),owner_sha256=digest(owner),files=d['files'],qualified_sources=qualified,blas_inputs=blas,build_tools=d['build_tools'],symbol_isolation=d['symbol_isolation'])


def write_pq_index(out,build,sdk,sdk_sha256,workspace):
 """Collect original actual report plus all original logs/artifacts, no mocks."""
 out=Path(out);build=Path(build);report=read(out/'result.json')
 assert_records(report['assertions']);verify_map(report['inputs'])
 if report.get('status')!='passed_independent_pq_parquet_component_only':raise RuntimeError('Cannot index failed PQ component')
 selected=sdk_index(sdk,sdk_sha256,workspace)
 d=dict(status=PQ_KIND,schema_version=1,native_integration=False,source_lock_updated=False,qualifier=report['qualifier'],
  input_files=report['inputs'],proof_files={},assertions=report['assertions'],sdk_index={'path':str(Path(sdk).resolve()),'sha256':sdk_sha256},
  report={'path':str((out/'result.json').resolve()),'sha256':digest(out/'result.json')})
 for parent in [out,build]:
  for f in parent.rglob('*'):
   if f.is_file()and f.name!='index.json':d['proof_files'][str(f.resolve())]=digest(f)
 path=out/'candidate-index.json';path.write_text(json.dumps(d,ensure_ascii=False,indent=2)+'\n');pq_component(path,digest(path),workspace)
 target=out/'index.json';path.rename(target);return target
