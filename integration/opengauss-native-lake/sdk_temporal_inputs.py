"""Private temporal SDK qualification only; no import-time product operations."""
import hashlib,json
from pathlib import Path
from pq_native_build_inputs import digest,read,safe,verify_files
SDK_KIND='verified_independent_sdk_temporal_only'
SDK_SOURCES={'sdk/arrow_parquet_provider.cpp': 'f14e0b99e33d1a0fffb34fa9d75f818fa09316da0f4f8085a0dc524e88bc7b44', 'sdk/exports.map': 'bf1cd5a31bbefdd4fb00b566f79bc276b54313fe2a29243a182be9ef6a709d72', 'sdk/CMakeLists.txt': 'b0390e048c5b2287fd95fce5f37de41a7bacda075a8b8e77d2b32fef5c862707', 'sdk/lake_temporal_codec.h': '07879fc21faf4d6fd4c7e3dab7f531f7050b7216c5774841016a9024c2a43a87', 'sdk/lake_parquet_sdk.h': '8f7ac7c9671561e74b8f8d87023cc761e52e5a545d24a41527dadc8921cd73ca', 'vendor/arrow20/abi.h': '1828f1ce60368cd590d19595910fa19db09b30728f8a43b5f89933623f7a7276'}
SDK_CASES={'layout','signed16','old-provider-refusal','signed16-independent','old-reader','old-writer','writer-io-faults','lifecycle','float','list','binary','symbols','temporal-roundtrip','temporal-independent','temporal-refusals'}
SDK_QUALIFIER_SHA256='87c4cd853fc84fde0c29978d01320d6f44f038099e2bca50f9f3416ce0930e7e'

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
 if not prefix.is_relative_to(root/'.install/opengauss-native-lake-sdk-temporal')or library!=prefix/'lib/libog_lake_sdk_arrow.so':
  raise RuntimeError('SQL020 provider must use its new private SDK prefix')
 verify_files(prefix,d['prefix_files'])
 if d['prefix_files'].get('include/og_lake/lake_parquet_sdk.h')!=SDK_SOURCES['sdk/lake_parquet_sdk.h']:raise RuntimeError('SDK installed a different public header')
 owner=read(prefix/'.og-lake-sdk-owner.json')
 if owner!={'project':'og-lake-sdk-temporal','prefix':str(prefix),'provider_build_sha256':d['provider_build']['sha256'],'source_header_sha256':SDK_SOURCES['sdk/lake_parquet_sdk.h']}:raise RuntimeError('SDK owner marker differs')
 if digest(library)!=d['library_sha256']:raise RuntimeError('Qualified SQL020 provider changed')
 buildpath=Path(d['provider_build']['path']);build=read(buildpath)
 if digest(buildpath)!=d['provider_build']['sha256']or build.get('status')!='provider_built_unqualified'or build['sha256']!=d['library_sha256']:
  raise RuntimeError('SDK provider build receipt mismatch')
 if build['sources_before']!=build['sources_after']or build['dependencies_before']!=build['dependencies_after']:
  raise RuntimeError('SDK build input transition')
 for name,h in SDK_SOURCES.items():
  if name.startswith('sdk/')and build['sources_after'].get(name.split('/',1)[1])!=h:raise RuntimeError('SDK built a different source')
 if build.get('source_closure_before')!=SDK_SOURCES or build.get('source_closure_after')!=SDK_SOURCES:raise RuntimeError('Complete six-source actual build closure absent')
 for name,h in SDK_SOURCES.items():
  archive=buildpath.parent/'executed-source-closure'/name
  if digest(archive)!=h or d['input_files'].get(str(archive.resolve()))!=h:raise RuntimeError('Actual build closure archive absent: '+name)
 if d.get('temporal_observed')!={'cells':70119,'rows':10017,'writer_refusals':9,'reader_refusals':13,'plain_int64_cells':3,'independent_raw_cells':70119}:raise RuntimeError('Temporal actual independent full-cell/refusal oracle incomplete')
 if set(d['cases'])!=SDK_CASES:raise RuntimeError('SDK ABI/old-type/full-domain cases incomplete')
 for case in d['cases'].values():
  if case.get('status')!='pass'or not case.get('commands')or any(c.get('returncode')!=0 for c in case['commands']):
   raise RuntimeError('SDK required actual command did not pass')
 if d['int16_observed']!={'rows':65536,'lo':-32768,'hi':32767,'total':-32768,'n':61680,'nlo':-32767,'nhi':32766,'ntotal':-30840}:
  raise RuntimeError('Signed16 independent whole-domain oracle absent')
 if d.get('old_provider_sha256')!='5ae9f8fecba4b0565b414e766cfad4dbc62e7f8714326f7610c8a2e51e3c083a':raise RuntimeError('Not genuine qualified5ae old-provider negative')
 return dict(path=str(Path(path).resolve()),sha256=expected,sdk_source=str(src),prefix=str(prefix),library=str(library),library_sha256=d['library_sha256'],header_sha256=SDK_SOURCES['sdk/lake_parquet_sdk.h'])
