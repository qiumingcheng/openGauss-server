"""Draft EMPTY validator; no business table identity is invented."""
import json
from pathlib import Path
from types import SimpleNamespace
from empty_reference_fixture import DRIVER,check_empty_initial
from lake_harness import Blocked,read_json,sha256


def validate_empty_manifest(path,ctx=None):
    m=dict(path) if isinstance(path,dict) else read_json(Path(path).resolve())
    lock=read_json(DRIVER/'source-lock.json')
    if m.get('case')!='EMPTY' or m.get('status')!='verified_reference_only':
        raise ValueError('A genuine standalone EMPTY fixture is required')
    if ctx is not None and m['run_id']!=ctx.run_id and m.get('parent_run_id')!=ctx.run_id:
        raise ValueError('EMPTY fixture belongs to another run')
    if m['source_lock_hash']!=sha256(DRIVER/'source-lock.json'):
        raise Blocked('EMPTY fixture source-lock is stale')
    if m.get('format_version')!='1.1' or m.get('automatic_migration') is not False:
        raise ValueError('EMPTY must use explicit unmigrated format1.1')
    for name,key in [('ducklake','reference_commit'),('duckdb','duckdb_commit')]:
        if m.get(key)!=lock['repositories'][name]['commit']: raise ValueError('EMPTY reference pin mismatch')
    owner=Path(m['cleanup_owner']).resolve(); files=m.get('files',[])
    if not files: raise ValueError('EMPTY has no actual immutable artifacts')
    for item in files:
        p=Path(item['path'])
        if not p.is_absolute() or not p.resolve().is_relative_to(owner) or not p.is_file() or sha256(p)!=item['sha256']:
            raise ValueError('EMPTY artifact changed or escapes exact owned directory')
    artifacts={item['path'] for item in files}
    if m['catalog'] not in artifacts or m['catalog_export'] not in artifacts:
        raise ValueError('Actual EMPTY catalog/export is not fingerprinted')
    export_path=Path(m['catalog_export']); exported=read_json(export_path)
    if sha256(export_path)!=m['catalog_export_sha256']: raise ValueError('EMPTY export changed')
    provenance=m['generation_provenance']; generator=Path(provenance['generator_path'])
    if not generator.is_absolute() or not generator.is_file() or sha256(generator)!=provenance['generator_sha256']:
        raise ValueError('EMPTY generator provenance changed')
    if exported.get('scope')!='actual_empty_reference_catalog_export' or exported['source_lock_hash']!=m['source_lock_hash']:
        raise ValueError('EMPTY export is not the genuine generation result')
    observed=check_empty_initial(exported['tables'],m['data_path'])
    if observed!=m['initial_observations'] or observed['created_by']!='DuckDB '+m['duckdb_commit'][:10]:
        raise ValueError('EMPTY recorded observations differ from actual catalog rows')
    if m['expected_schemas']!=[{'schema_name':'main'}] or m['expected_business_tables']:
        raise ValueError('EMPTY virtual lake catalog is not empty')
    if len(m['observed_snapshots'])!=1 or m['observed_snapshots'][0]['snapshot_id']!=0:
        raise ValueError('EMPTY virtual lake snapshot was not observed')
    if any(key in m for key in ('table_id','row_identity_map','file_aliases')):
        raise ValueError('EMPTY must not fabricate business identities')
    return m,exported


def validate_empty_bundle(value,ctx=None):
    m=dict(value) if isinstance(value,dict) else read_json(Path(value).resolve())
    if m.get('case')!='EMPTY_BUNDLE' or m.get('status')!='verified_reference_only':raise ValueError('Invalid EMPTY_BUNDLE provenance')
    if ctx is not None and m['run_id']!=ctx.run_id and m.get('parent_run_id')!=ctx.run_id:raise ValueError('EMPTY bundle belongs to another run')
    if m['source_lock_hash']!=sha256(DRIVER/'source-lock.json'):raise Blocked('EMPTY bundle source-lock is stale')
    owner=Path(m['cleanup_owner']).resolve();entries=m.get('entries',{})
    if set(entries)!={'same','ancestor','child','sibling'}:raise ValueError('EMPTY bundle has unexpected root cases')
    roots={}
    for alias,item in entries.items():
        p=Path(item['manifest'])
        if not p.is_absolute() or not p.resolve().is_relative_to(owner) or sha256(p)!=item['sha256']:
            raise ValueError('EMPTY child manifest is not exact owned evidence')
        child,_=validate_empty_manifest(p)
        if child['run_id']!=m['run_id']+'-'+alias or child['data_path']!=item['data_root']:
            raise ValueError('EMPTY child identity/root differs from generation')
        roots[alias]=Path(child['data_path'])
        if not roots[alias].is_relative_to(owner):raise ValueError('EMPTY data root escapes exact owned test geometry')
    if roots['child'].parent!=roots['ancestor'] or roots['same'].parent!=roots['sibling'].parent:
        raise ValueError('EMPTY root overlap geometry differs')
    producer=m['generation_provenance'];p=Path(producer['generator_path'])
    if not p.is_absolute() or not p.is_file() or sha256(p)!=producer['generator_sha256']:
        raise ValueError('EMPTY bundle generator provenance changed')
    business=m['business_fixture'];p=Path(business['manifest'])
    if not p.resolve().is_relative_to(owner) or sha256(p)!=business['sha256']:
        raise ValueError('Real business fixture manifest escapes bundle or changed')
    from fixture import validate_manifest
    actual=read_json(p)
    if actual.get('case')!='I':raise ValueError('Read-lock tests require a genuine ordinary I fixture')
    validate_manifest(actual,SimpleNamespace(driver=DRIVER,run_id=m['run_id']+'-business'))
    return m
