#!/usr/bin/env python3
"""Generate a true empty DuckLake 1.1 through normal ATTACH, never protocol DDL.

Draft: not run. Formal registration/EMPTY validator/source-lock review must
precede use in the official harness. Does not build or change source-lock.
"""
import argparse
import fcntl
import json
import os
from pathlib import Path
import subprocess
import sys
import traceback
import uuid

ROOT=Path('/home/qiumc/data/code/cpp')
DRIVER=Path(os.getenv('OG_LAKE_DRIVER_ROOT',str(ROOT/'integration/opengauss-native-lake')))
sys.path.insert(0,str(DRIVER)); sys.path.insert(0,str(DRIVER/'tests'))
from lake_harness import Blocked,read_json,sha256,validate_run_id,write_json
from reverse_reference import Reference,literal,qi


def export_actual_tables(ref,catalog):
    """Read genuine physical DDL, constraints and rows, including empty tables."""
    tables=ref.run('SELECT schema_name,table_name,sql FROM duckdb_tables() '
        'WHERE database_name=current_database() AND NOT internal ORDER BY schema_name,table_name',
        'export-table-list',catalog,True,True)
    for table in tables:
        predicate='database_name=current_database() AND schema_name='+literal(table['schema_name'])+' AND table_name='+literal(table['table_name'])
        table['columns']=ref.run('SELECT column_index,column_name,data_type,is_nullable,column_default FROM duckdb_columns() WHERE '+predicate+' ORDER BY column_index',
            'columns-'+table['table_name'],catalog,True,True)
        table['constraints']=ref.run('SELECT constraint_type,constraint_text,expression,constraint_column_names FROM duckdb_constraints() WHERE '+predicate+' ORDER BY constraint_index',
            'constraints-'+table['table_name'],catalog,True,True)
        table['rows']=ref.run('SELECT * FROM '+qi(table['schema_name'])+'.'+qi(table['table_name']),
            'rows-'+table['table_name'],catalog,True,True)
    return tables


def check_empty_initial(tables,expected_root):
    """Source assertions checked against observations; never manufacture rows."""
    by_name={table['table_name']:table for table in tables}
    if len(by_name)!=len(tables) or len(tables)!=29: raise AssertionError('Actual EMPTY must expose all 29 distinct standard tables')
    if any(not name.startswith('ducklake_') for name in by_name): raise AssertionError('Unexpected physical catalog object')
    if 'ducklake_snapshot_tag' in by_name: raise AssertionError('A 2.0-only table entered the 1.1 catalog')
    snapshots=by_name['ducklake_snapshot']['rows']; changes=by_name['ducklake_snapshot_changes']['rows']
    if len(snapshots)!=1 or len(changes)!=1: raise AssertionError('Actual EMPTY must have exactly its initialization snapshot/change')
    first=snapshots[0]
    if {k:first[k] for k in ('snapshot_id','schema_version','next_catalog_id','next_file_id')}!={'snapshot_id':0,'schema_version':0,'next_catalog_id':1,'next_file_id':0}:
        raise AssertionError('Initial frontiers differ from pinned initializer')
    if first['snapshot_time'] is None: raise AssertionError('Missing real snapshot time')
    if changes!=[{'snapshot_id':0,'changes_made':'created_schema:"main"','author':None,'commit_message':None,'commit_extra_info':None}]:
        raise AssertionError('Initial actual changeset differs')
    metadata=by_name['ducklake_metadata']['rows']
    if len(metadata)!=4 or any(r['scope'] is not None or r['scope_id'] is not None for r in metadata):
        raise AssertionError('Initial metadata has unexpected scoped/duplicate settings')
    values={r['key']:r['value'] for r in metadata}
    if set(values)!={'version','created_by','data_path','encrypted'} or values['version']!='1.1' or values['encrypted']!='false':
        raise AssertionError('Initial metadata keys/version/encryption differ')
    if values['data_path']!=expected_root.rstrip('/')+'/': raise AssertionError('Real data root was not retained with one trailing slash')
    schemas=by_name['ducklake_schema']['rows']
    if len(schemas)!=1: raise AssertionError('Actual EMPTY must have one logical default schema')
    schema=dict(schemas[0]); identity=uuid.UUID(str(schema.pop('schema_uuid')))
    if identity.int==0: raise AssertionError('Initial schema UUID is zero')
    if schema!={'schema_id':0,'begin_snapshot':0,'end_snapshot':None,'schema_name':'main','path':'main/','path_is_relative':True,'parent_schema_id':None}:
        raise AssertionError('Logical main differs from actual pinned initializer')
    if sum(len(t['rows']) for t in tables)!=7: raise AssertionError('Actual EMPTY must have seven rows in total')
    nonempty={'ducklake_snapshot','ducklake_snapshot_changes','ducklake_metadata','ducklake_schema'}
    if any(t['rows'] for name,t in by_name.items() if name not in nonempty): raise AssertionError('Business/inline/stats rows found in EMPTY')
    return {'schema_uuid':str(identity),'uuid_version':identity.version,'snapshot_time':first['snapshot_time'],
            'created_by':values['created_by'],'data_path':values['data_path']}


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--run-id',required=True); p.add_argument('--output',type=Path,required=True)
    p.add_argument('--data-root',type=Path,required=True)
    args=p.parse_args(); validate_run_id(args.run_id)
    output=args.output.resolve(); data_root=args.data_root.absolute()
    if not args.data_root.is_absolute(): p.error('data root must be an explicit absolute path')
    if data_root.exists() or data_root.is_symlink(): p.error('data root must not exist; generator does not mkdir it')
    if output.exists(): p.error('output already exists')
    output.parent.mkdir(parents=True,exist_ok=True)
    attempt=output.parent/'empty-reference-generation'
    attempt.mkdir(exist_ok=False)
    catalog=attempt/'metadata.ducklake'
    result={'status':'fail','scope':'actual_empty_reference_catalog_initialization','run_id':args.run_id,
            'assertions':[],'artifacts':[]}; mutex=None
    try:
        lock_path=DRIVER/'source-lock.json'; lock=read_json(lock_path)
        receipt_path=ROOT/'.reference/opengauss-native-lake/build/receipt.json'; receipt=read_json(receipt_path)
        ref=Reference(attempt,lock,receipt)
        lock_hash=sha256(lock_path)
        if receipt['source_lock_hash']!=lock_hash or sha256(ref.binary)!=receipt['binary_sha256']:
            raise Blocked('Reference binary/receipt does not match current source-lock')
        if sha256(DRIVER/'reference.sh')!=lock['reference_build']['script_sha256']:
            raise Blocked('Reference driver differs from reviewed lock')
        for name in ('ducklake','duckdb'):
            entry=lock['repositories'][name]; repository=ROOT/entry['path']
            actual=subprocess.check_output(['git','-C',str(repository),'rev-parse','HEAD'],text=True).strip()
            if actual!=entry['commit']: raise Blocked('Reference source checkout differs: '+name)
        mutex=(ROOT/'.reference/opengauss-native-lake/.driver.lock').open('a+')
        try: fcntl.flock(mutex,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError as exc: raise Blocked('Another actual reference driver is running') from exc
        versions=ref.run('PRAGMA version;','version',result=True)
        extensions=ref.run("SELECT extension_version,loaded,install_mode FROM duckdb_extensions() WHERE extension_name='ducklake'",'extension',result=True)
        if len(versions)!=1 or versions[0]['source_id']!=lock['repositories']['duckdb']['commit'][:10]:
            raise AssertionError('Actual reference DuckDB source mismatch')
        if extensions!=[{'extension_version':lock['repositories']['ducklake']['commit'][:10],'loaded':True,'install_mode':'STATICALLY_LINKED'}]:
            raise AssertionError('Actual reference DuckLake extension mismatch')
        attach='ATTACH '+literal('ducklake:'+str(catalog))+" AS lake (DATA_PATH "+literal(str(data_root))+", DUCKLAKE_VERSION '1.1', AUTOMATIC_MIGRATION FALSE, DATA_INLINING_ROW_LIMIT 100);\n"
        schemas=ref.run(attach+"SELECT schema_name FROM duckdb_schemas() WHERE database_name='lake' ORDER BY schema_name;",
                        'normal-empty-attach',result=True)
        if schemas!=[{'schema_name':'main'}]: raise AssertionError('Real attached lake has unexpected schemas')
        ref.run('CHECKPOINT;','checkpoint',catalog)
        initial_hash=sha256(catalog)
        if Path(str(catalog)+'.wal').exists(): raise AssertionError('Reference catalog has uncheckpointed WAL')
        tables=export_actual_tables(ref,catalog)
        observed=check_empty_initial(tables,str(data_root))
        if observed['created_by']!='DuckDB '+lock['repositories']['duckdb']['commit'][:10]:
            raise AssertionError('Reference producer did not match its actual runtime source')
        attached=ref.run(ref.attach(catalog,True)+"SELECT snapshot_id,schema_version,changes::VARCHAR AS changes FROM ducklake_snapshots('lake') ORDER BY snapshot_id;",
                         'readonly-lake-snapshots',result=True)
        user_tables=ref.run(ref.attach(catalog,True)+"SELECT table_name FROM duckdb_tables() WHERE database_name='lake' ORDER BY table_name;",
                           'readonly-lake-tables',result=True)
        if user_tables: raise AssertionError('Reference EMPTY contains business tables')
        if sha256(catalog)!=initial_hash: raise AssertionError('Readonly export/ATTACH changed original reference artifact')
        if data_root.exists() or data_root.is_symlink(): raise AssertionError('Empty initialization unexpectedly materialized data root')
        exported=attempt/'catalog-export.json'
        write_json(exported,{'scope':'actual_empty_reference_catalog_export','format_version':'1.1','tables':tables,
            'catalog':str(catalog),'catalog_sha256':initial_hash,'source_lock_hash':lock_hash,
            'binary_sha256':receipt['binary_sha256'],'generation_provenance':{'generator_path':str(Path(__file__).resolve()),
                'generator_sha256':sha256(__file__)},'initial_observations':observed})
        files=[{'path':str(path.resolve()),'sha256':sha256(path)} for path in sorted(attempt.rglob('*')) if path.is_file()]
        write_json(output,{'case':'EMPTY','run_id':args.run_id,'status':'verified_reference_only','format_version':'1.1',
            'automatic_migration':False,'source_lock_hash':lock_hash,'binary_sha256':receipt['binary_sha256'],
            'reference_commit':lock['repositories']['ducklake']['commit'],'duckdb_commit':lock['repositories']['duckdb']['commit'],
            'catalog':str(catalog),'data_path':str(data_root),'catalog_export':str(exported),'catalog_export_sha256':sha256(exported),
            'expected_schemas':schemas,'expected_business_tables':user_tables,'observed_snapshots':attached,
            'initial_observations':observed,'files':files,'cleanup_owner':str(attempt),
            'generation_provenance':{'generator_path':str(Path(__file__).resolve()),'generator_sha256':sha256(__file__)}})
        result.update(status='pass',manifest=str(output),manifest_sha256=sha256(output),initial_observations=observed)
    except Blocked as exc: result.update(status='blocked',reason=str(exc))
    except BaseException as exc: result.update(reason=str(exc),traceback=traceback.format_exc())
    finally:
        if mutex is not None: mutex.close()
        write_json(attempt/'generation-result.json',result)
        print(json.dumps(result,ensure_ascii=False),flush=True)
    return {'pass':0,'blocked':2}.get(result['status'],1)


if __name__=='__main__': raise SystemExit(main())
