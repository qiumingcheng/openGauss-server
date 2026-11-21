#!/usr/bin/env python3
"""Native vector CREATE/DML/flush and exact heap wire-value differential.

Core single-instance RC only. This is not ANN/DiskANN, SMP, remote
SDK or commercial qualification.
"""
import argparse
import fcntl
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import struct
import subprocess
import sys
import traceback
from types import SimpleNamespace
ROOT=Path('/home/qiumc/data/code/cpp');D=ROOT/'integration/opengauss-native-lake'
sys.path[:0]=[str(D),str(D/'tests')]
from file_dml_acceptance import check_build
from inline_fixture import query,scalar
from lake_harness import sha256,validate_run_id,write_json
from native_auto_flush import connect_control
from native_test_context import OwnedDatabase
from native_table_create import Case
from permission_api import OwnedRoles,cleanup_roles
from empty_catalog_oracle import read_native_catalog,transport_native,canonical_shape
from reverse_reference import Reference,qi


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--run-id',required=True)
    p.add_argument('--build-receipt',type=Path,required=True);p.add_argument('--mode',choices=['full'],default='full')
    a=p.parse_args();validate_run_id(a.run_id);out=ROOT/'公共文档/openGauss原生湖存储/验证'/a.run_id;out.mkdir(exist_ok=False)
    r={'status':'fail','scope':__doc__+' '+a.mode,'assertions':[]}
    files=[Path(__file__),D/'tests/native_table_create.py',D/'tests/permission_api.py',D/'native_test_context.py',
        D/'native_runtime.py',D/'tests/inline_cleanup.py',D/'tests/file_dml_acceptance.py',D/'inline_fixture.py',
        D/'tests/native_auto_flush.py',D/'tests/empty_catalog_oracle.py',D/'tests/create_reference_oracle.py',D/'tests/reverse_reference.py',D/'source-lock.json']
    r['inputs']={str(f):sha256(f)for f in files};archive=out/'executed-inputs';archive.mkdir()
    for f in files:shutil.copy2(f,archive/f.name)
    provider=ROOT/'.install/opengauss-native-lake-debug/lib/postgresql/og_lake_sdk/libog_lake_sdk_arrow.so'
    duckdb=ROOT/'.reference/opengauss-native-lake/build/duckdb-gcc10/duckdb'
    r.update(provider_sha256=sha256(provider),reference_binary_sha256=sha256(duckdb));control=db=roles=t=None
    def image(v):return {'rows':len(v),'sha256':hashlib.sha256(repr(v).encode()).hexdigest()}if isinstance(v,list)and len(v)>40 else v
    def check(name,got,want=True):
        good=got==want;r['assertions'].append(dict(name=name,observed=image(got),expected=image(want),status='pass'if good else'fail'))
        if not good:raise AssertionError(name)
    def literal(s):return "'"+str(s).replace("'","''")+"'"
    def independent(sql,label):
        (out/(label+'.sql')).write_text(sql+'\n');v=subprocess.run([str(duckdb),'-no-init','-batch','-json','-c','SET threads=1;'+sql],text=True,capture_output=True,timeout=90)
        (out/(label+'.stdout')).write_text(v.stdout);(out/(label+'.stderr')).write_text(v.stderr)
        if v.returncode:raise RuntimeError('independent SQL failed: '+label)
        return json.loads(v.stdout)if v.stdout.strip()else[]
    try:
        import psycopg2
        from psycopg2 import sql
        b=check_build(a.build_receipt);r.update(source_head=b['source_head'],binary_sha256=b['binary_sha256'])
        control=connect_control();check('automatic maintenance remains off',scalar(control,'SHOW enable_lake_maintenance'),'off')
        with(out/'fixture.log').open('w')as log:
            subprocess.run([sys.executable,str(D/'tests/catalog_fixture_bundle.py'),'--run-id',a.run_id,'--output',str(out/'fixture/manifest.json')],stdout=log,stderr=subprocess.STDOUT,check=True,timeout=300)
        db=OwnedDatabase(a.run_id,out).create();roles=OwnedRoles(control,a.run_id,out)
        t=Case(db,SimpleNamespace(mode='net_dml',sql_version='0.16',manifest=out/'fixture/manifest.json',output=out/'result.json',run_id=a.run_id),r,roles)
        c=t.a
        shape='(id integer,v vector(3),note text)'
        query(c,'CREATE TABLE biz.vector_heap '+shape)
        query(c,'CREATE FOREIGN TABLE biz.vectors '+shape+" SERVER created_lake OPTIONS(binding_mode 'create',lake_schema 'main')")
        query(c,"SELECT og_lake.set_option('biz.vectors'::regclass,'data_inlining_row_limit','50000')")
        query(c,"INSERT INTO biz.vector_heap SELECT i,CASE WHEN i%17=0 THEN NULL ELSE ('['||i::text||','||(-i)::text||',0.25]')::vector END,'vector/'||i::text FROM generate_series(1,1025)i")
        query(c,"INSERT INTO biz.vector_heap VALUES(2001,'[0,-0,1]'::vector,'zero'),(2002,'[-1,1e-45,-1e-45]'::vector,'subnormal')")
        query(c,'BEGIN');query(c,'INSERT INTO biz.vectors SELECT * FROM biz.vector_heap')
        check('private vector values before commit',query(c,"SELECT id,encode(vector_send(v),'hex'),note FROM biz.vectors ORDER BY id"),query(c,"SELECT id,encode(vector_send(v),'hex'),note FROM biz.vector_heap ORDER BY id"))
        query(c,'COMMIT')
        def wire(table='biz.vectors',conn=c):return query(conn,"SELECT id,encode(vector_send(v),'hex'),note FROM "+table+' ORDER BY id')
        def equal(label):check(label,wire(),wire('biz.vector_heap'))
        equal('committed inline vector bytes')
        tid=scalar(c,"SELECT table_id FROM created_metadata.ducklake_table WHERE table_name='vectors'")
        carrier=scalar(c,'SELECT table_name FROM created_metadata.ducklake_inlined_data_tables WHERE table_id=%s',(tid,))
        check('native vector root/element/catalog order',query(c,'SELECT column_id,column_name,column_type,parent_column FROM created_metadata.ducklake_column WHERE table_id=%s ORDER BY column_id',(tid,)),[(1,'id','int32',None),(2,'v','list',None),(3,'element','float32',2),(4,'note','varchar',None)])
        check('standard nested inline uses text',scalar(c,'SELECT a.atttypid FROM pg_attribute a JOIN pg_class r ON r.oid=a.attrelid JOIN pg_namespace n ON n.oid=r.relnamespace WHERE n.nspname=%s AND r.relname=%s AND a.attname=%s',('created_metadata',carrier,'v')),25)
        def both(statement):
            query(c,'BEGIN')
            for table in ['biz.vectors','biz.vector_heap']:query(c,statement.format(table=table))
            query(c,'COMMIT')
        both("UPDATE {table} SET v='[7,8,9]'::vector WHERE id BETWEEN 1 AND 10")
        both("UPDATE {table} SET v='[70,80,90]'::vector WHERE id BETWEEN 1 AND 10")
        equal('two updates retain final native vector version')
        query(c,'BEGIN');query(c,'SAVEPOINT vector_child')
        query(c,"UPDATE biz.vectors SET v='[9,9,9]'::vector WHERE id=3");query(c,'ROLLBACK TO SAVEPOINT vector_child');query(c,'COMMIT')
        equal('vector savepoint rollback')
        query(c,'ANALYZE biz.vectors');query(c,'ANALYZE biz.vector_heap')
        check('native vector statistics use host type analyzer',query(c,"SELECT attname,null_frac,avg_width,n_distinct FROM pg_stats WHERE schemaname='biz' AND tablename='vectors' ORDER BY attname"),query(c,"SELECT attname,null_frac,avg_width,n_distinct FROM pg_stats WHERE schemaname='biz' AND tablename='vector_heap' ORDER BY attname"))
        query(t.b,'BEGIN');before=wire(conn=t.b)
        both("UPDATE {table} SET v='[100,200,300]'::vector WHERE id=3")
        before_versions=query(c,sql.SQL('SELECT _ducklake_row_id,_ducklake_begin_snapshot,id FROM created_metadata.{} ORDER BY 1,2').format(sql.Identifier(carrier)))
        query(c,"SELECT og_lake.flush('biz.vectors'::regclass)");equal('first vector flush exact visible values')
        check('old vector reader survives flush',wire(conn=t.b),before);query(t.b,'COMMIT')
        roots=query(c,'SELECT path,path_is_relative FROM created_metadata.ducklake_data_file WHERE table_id=%s ORDER BY data_file_id',(tid,))
        root=scalar(c,"SELECT value FROM created_metadata.ducklake_metadata WHERE key='data_path'")
        table_path,table_relative,schema_path,schema_relative=query(c,'SELECT t.path,t.path_is_relative,s.path,s.path_is_relative FROM created_metadata.ducklake_table t JOIN created_metadata.ducklake_schema s ON s.schema_id=t.schema_id WHERE t.table_id=%s AND t.end_snapshot IS NULL AND s.end_snapshot IS NULL',(tid,))[0]
        schema_root=Path(root)/schema_path if schema_relative else Path(schema_path)
        table_root=schema_root/table_path if table_relative else Path(table_path)
        paths=[str(table_root/path)if relative else path for path,relative in roots]
        check('vector flush emits a real Parquet file',len(paths),1)
        physical=independent('SELECT _ducklake_internal_row_id AS rid,_ducklake_internal_snapshot_id AS birth,id FROM read_parquet('+literal(paths[0])+') ORDER BY 1,2','decode-identities')
        check('flush preserves every row ID and birth snapshot',[(x['rid'],x['birth'],x['id'])for x in physical],before_versions)
        schema=independent('SELECT name,field_id FROM parquet_schema('+literal(paths[0])+') WHERE field_id IS NOT NULL','decode-schema')
        check('standard parent/element and hidden field IDs',[(x['name'],x['field_id'])for x in schema],[('id',1),('v',2),('element',3),('note',4),('_ducklake_internal_row_id',2147483540),('_ducklake_internal_snapshot_id',2147483539)])
        both("UPDATE {table} SET v='[1000,2000,3000]'::vector WHERE id BETWEEN 1 AND 10")
        both('DELETE FROM {table} WHERE id IN(7,8,9)');equal('file-origin vector UPDATE/DELETE overlay')
        query(c,"SELECT og_lake.flush('biz.vectors'::regclass)");equal('second vector data/delete flush')
        query(c,"CREATE FOREIGN TABLE biz.wide(id integer,v vector(16000)) SERVER created_lake OPTIONS(binding_mode 'create',lake_schema 'main')")
        query(c,"INSERT INTO biz.wide SELECT 1,('['||string_agg((i%19)::text,',' ORDER BY i)||']')::vector FROM generate_series(1,16000)i")
        wide=scalar(c,"SELECT encode(vector_send(v),'hex') FROM biz.wide")
        query(c,"SELECT og_lake.flush('biz.wide'::regclass)")
        check('wide TOAST value crosses bounded list batches',scalar(c,"SELECT encode(vector_send(v),'hex') FROM biz.wide"),wide)
        query(c,'BEGIN');query(c,"INSERT INTO biz.vectors VALUES(9999,'[1,2,3]'::vector,'rollback')");query(c,'SAVEPOINT flush_refusal')
        try:query(c,"SELECT og_lake.flush('biz.vectors'::regclass)")
        except Exception as exc:check('same-transaction flush refusal',exc.pgcode,'0A000')
        else:check('same-transaction flush refusal',False)
        query(c,'ROLLBACK TO SAVEPOINT flush_refusal');query(c,'ROLLBACK');equal('vector write rollback after refused flush')
        # Offline host transport: retain actual standard definitions, versions,
        # paths and IDs. Only empty nested physical carriers change TEXT -> LIST
        # to match DuckDB's metadata manager; no protocol row is manufactured.
        interop=out/'interop';interop.mkdir();mutex=(ROOT/'.reference/opengauss-native-lake/.driver.lock').open('a+')
        fcntl.flock(mutex,fcntl.LOCK_EX|fcntl.LOCK_NB)
        try:
            lock=json.loads((D/'source-lock.json').read_text());receipt=json.loads((ROOT/'.reference/opengauss-native-lake/build/receipt.json').read_text())
            check('interop reference source-lock',receipt['source_lock_hash'],sha256(D/'source-lock.json'))
            ref=Reference(interop,lock,receipt);check('interop exact reference binary',sha256(ref.binary),receipt['binary_sha256'])
            names=[x[0]for x in query(t.observer,"SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='created_metadata' AND c.relkind='r' ORDER BY c.relname")]
            actual=read_native_catalog(t.observer,t.schema,{'tables':[{'table_name':n}for n in names]},interop/'native-catalog.json')
            actual_map={v['table_name']:v for v in actual['tables']}
            for standard in t.empty_export['tables']:
                check('actual standard physical definition '+standard['table_name'],canonical_shape(actual_map[standard['table_name']]),canonical_shape(standard))
            carriers={x['table_name']:x['table_id']for x in actual_map['ducklake_inlined_data_tables']['rows']}
            allowed={x['table_name']for x in t.empty_export['tables']}|set(carriers)|{'ducklake_inlined_delete_'+str(x['table_id'])for x in actual_map['ducklake_table']['rows']}
            check('only standard and registered native objects',set(actual_map)<=allowed)
            check('complete small flush leaves no inline versions',all(not actual_map[name]['rows']for name in carriers))
            transported=interop/'transported.ducklake';transport_native(ref,transported,actual)
            nested=[x for x in actual_map['ducklake_column']['rows']if x['column_type']=='list'and x['parent_column']is None]
            for name,table in carriers.items():
                for column in nested:
                    if column['table_id']==table:
                        ref.run('ALTER TABLE '+qi(name)+' ALTER COLUMN '+qi(column['column_name'])+' TYPE FLOAT[] USING CAST('+qi(column['column_name'])+' AS FLOAT[])','empty-carrier-list-type-'+name,transported)
            before_catalog=sha256(transported);decoded=ref.logical(transported,'lake.main.vectors','native-vector-flush-readback')
            wire_columns="SELECT id,substring(encode(vector_send(v),'hex') from 9),note FROM biz.vectors ORDER BY id"
            decoded_wire=[(v['id'],None if v['v']is None else b''.join(struct.pack('>f',x)for x in v['v']).hex(),v['note'])for v in sorted(decoded,key=lambda v:v['id'])]
            check('independent DuckLake reads every native vector bit and deletion',decoded_wire,query(c,wire_columns))
            identities={identity:rowid for rowid,birth,identity in before_versions}
            check('independent DuckLake preserves logical row IDs',sorted((v['id'],v['logical_rowid'])for v in decoded),sorted((v['id'],identities[v['id']])for v in decoded))
            check('readonly reference preserves catalog publications',sha256(transported),before_catalog)
        finally:fcntl.flock(mutex,fcntl.LOCK_UN);mutex.close()
        check('every frozen input unchanged',all(sha256(f)==h for f,h in r['inputs'].items()))
        check('same installed provider at completion',sha256(provider),r['provider_sha256'])
        r['artifacts']={str(f):sha256(f)for f in Path(root).rglob('*.parquet')};r['status']='pass'
    except BaseException as exc:r.update(reason=str(exc),traceback=traceback.format_exc())
    finally:
        if db is not None:
            for c in list(db.connections):
                if not c.closed:
                    try:query(c,'ROLLBACK')
                    except Exception:pass
        try:
            if db is not None:db.cleanup();r['database_cleanup']=db.cleanup_result
            if roles is not None:
                if not r.get('database_cleanup')or r['database_cleanup']['returncode']:raise RuntimeError('Owned DB cleanup not proven; roles retained')
                r['role_cleanup']=cleanup_roles(control,roles.path,a.run_id);r=roles.redact(r)
            r['cleanup']='pass'
        except BaseException as exc:r.update(cleanup='fail',cleanup_error=str(exc),status='fail')
        if control is not None and not control.closed:control.close()
        write_json(out/'result.json',r)
    print(json.dumps(dict(status=r['status'],assertions=len(r['assertions']),reason=r.get('reason'),result=str(out/'result.json')),ensure_ascii=False));return 0 if r['status']=='pass'else 1
if __name__=='__main__':raise SystemExit(main())
