"""Private new-temporal-header PQ qualification; existing temporal-SDK helper unchanged."""
import hashlib,json
from pathlib import Path
from pq_native_build_inputs import (digest,read,safe,verify_files,qualified_sources as legacy_sources,
    blas_inputs,global_symbols,PUBLIC_PREFIXES)
from sdk_temporal_inputs import SDK_SOURCES,SDK_KIND,SDK_CASES,SDK_QUALIFIER_SHA256,verify_map,assert_records,index,sdk_index
KIND='pq_temporal_v1'
PQ_KIND='verified_independent_pq_temporal_parquet_only'
PQ_TAGS={'metric-1','metric-2','metric-3','empty','d3','d1536'}
PQ_QUALIFIER_SHA256='23b2b0f56eb17bcaa1ad0b8c747cc036b4340845c6ed666fc9e1c1b08df1a6b8'

def pq_component(path,expected,workspace):
 d=index(path,expected,PQ_KIND)
 if d.get('qualifier',{}).get('sha256')!=PQ_QUALIFIER_SHA256 or digest(Path(d['qualifier']['path']))!=PQ_QUALIFIER_SHA256:raise RuntimeError('PQ qualification runner identity mismatch')
 sdk=sdk_index(d['sdk_index']['path'],d['sdk_index']['sha256'],workspace)
 report=read(d['report']['path'])
 trace=report.get('compiler_include_trace',{});verify_map({trace.get('path',''):trace.get('sha256')})
 text=Path(trace['path']).read_text()
 if any(str(Path(sdk['sdk_source'])/n)not in text for n in ('sdk/lake_parquet_sdk.h','vendor/arrow20/abi.h')):raise RuntimeError('Actual component new-header compiler trace absent')
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
  raise RuntimeError('temporal-SDK requires exact independently-qualified dependency kind')
 if digest(Path(__file__).resolve())!=d['admission_helper_sha256']:raise RuntimeError('temporal-SDK admission implementation differs from built receipt')
 q=d['qualification'];qualified=qualified_sources(workspace,q['normal']['path'],q['normal']['sha256'],q['asan']['path'],q['asan']['sha256']);blas=blas_inputs(workspace)
 if d['qualified_sources']!=qualified or d['blas_inputs']!=blas:raise RuntimeError('temporal-SDK PQ source/SDK/component qualification changed')
 prefix=safe(d['prefix']);include=safe(d['include']);library=safe(d['library']);bl=safe(d['blas_library'])
 if not prefix.is_relative_to(Path(workspace).resolve()/'.install/opengauss-native-lake-pq-temporal-static')or include!=prefix/'include'or library!=prefix/'lib/libog_lake_pq_cpu.a'or bl!=prefix/'lib/libog_lake_pq_openblas.a':raise RuntimeError('temporal-SDK archives escaped owned prefix')
 verify_files(prefix,d['files']);owner=prefix/'.og-lake-pq-owner.json'
 if digest(owner)!=d['owner_sha256']or read(owner)!=d['owner']or d['owner'].get('prefix')!=str(prefix)or d['owner'].get('dependency_kind')!=KIND:raise RuntimeError('temporal-SDK PQ owner identity mismatch')
 verify_map(d['helper_inputs'])
 for name,item in d['build_tools'].items():
  if digest(Path(item['path']))!=item['sha256']:raise RuntimeError('temporal-SDK compiler/tool identity changed: '+name)
 for name in ['include/pq_cpu.h','include/pq_parquet_format.h','include/pq_parquet_frontier_draft.h','include/sdk/lake_parquet_sdk.h','include/vendor/arrow20/abi.h','licenses/Microsoft-DiskANN-LICENSE','licenses/OpenBLAS/LICENSE','symbol-map.txt']:
  if name not in d['files']:raise RuntimeError('Incomplete temporal-SDK installed public archive/ABI/license')
 for name,h in [('sdk/lake_parquet_sdk.h',SDK_SOURCES['sdk/lake_parquet_sdk.h']),('vendor/arrow20/abi.h',SDK_SOURCES['vendor/arrow20/abi.h'])]:
  if d['files']['include/'+name]!=h:raise RuntimeError('New archive does not carry exact new SDK header')
 owned=sorted(global_symbols(library))
 if digest(library)!=d['library_sha256']or digest(bl)!=blas['library_sha256']or owned!=d['owned_symbols']or any(not x.startswith(PUBLIC_PREFIXES+('oglpqcpu_',))for x in owned):raise RuntimeError('temporal-SDK static symbol/byte identity mismatch')
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
