#!/usr/bin/env python3
"""Native float CREATE/DML/ANALYZE/flush and exact heap wire-value differential.

Core single-instance RC only. This is not vector, float indexing, SMP, remote
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
from empty_catalog_oracle import read_native_catalog,transport_native
from reverse_reference import Reference


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--run-id',required=True)
    p.add_argument('--build-receipt',type=Path,required=True);p.add_argument('--mode',choices=['old-provider','full'],required=True)
    p.add_argument('--check-file-nan-stats',action='store_true')
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
        shape='(id integer,f4 real,f8 double precision,note text,flag boolean)'
        query(c,'CREATE TABLE biz.float_heap '+shape)
        query(c,'CREATE FOREIGN TABLE biz.floating '+shape+" SERVER created_lake OPTIONS(binding_mode 'create',lake_schema 'main')")
        query(c,"SELECT og_lake.set_option('biz.floating'::regclass,'data_inlining_row_limit','50000')")
        query(c,"INSERT INTO biz.float_heap SELECT i,CASE i%11 WHEN 0 THEN NULL WHEN 1 THEN '-0'::real WHEN 2 THEN 0::real WHEN 3 THEN 'NaN'::real WHEN 4 THEN 'Infinity'::real WHEN 5 THEN '-Infinity'::real ELSE (i::real/7) END,CASE i%11 WHEN 0 THEN NULL WHEN 1 THEN '-0'::double precision WHEN 2 THEN 0::double precision WHEN 3 THEN 'NaN'::double precision WHEN 4 THEN 'Infinity'::double precision WHEN 5 THEN '-Infinity'::double precision ELSE (i::double precision/11) END,CASE WHEN i%13=0 THEN NULL ELSE '浮点/'||i::text END,CASE WHEN i%17=0 THEN NULL ELSE i%2=0 END FROM generate_series(1,1001)i")
        # Native float text input deliberately normalizes -0 (float.cpp:204,
        # 396). Use ordinary heap binary COPY to exercise exact IEEE values
        # without changing that host SQL policy or fabricating lake metadata.
        payload=bytearray(b'PGCOPY\n\xff\r\n\x00'+struct.pack('>ii',0,0))
        patterns=[(2001,0x80000000,0x8000000000000000),(2002,1,1),
                  (2003,0x80000001,0x8000000000000001),(2004,0x7fc12345,0x7ff8123456789abc)]
        for identity,f32,f64 in patterns:
            fields=[struct.pack('>i',identity),struct.pack('>I',f32),struct.pack('>Q',f64),('binary/'+str(identity)).encode(),b'\x01']
            payload.extend(struct.pack('>h',len(fields)))
            for field in fields:payload.extend(struct.pack('>i',len(field)));payload.extend(field)
        payload.extend(struct.pack('>h',-1));binary_input=out/'ordinary-heap-float.copy';binary_input.write_bytes(payload)
        with c.cursor()as cursor:cursor.copy_expert('COPY biz.float_heap FROM STDIN BINARY',io.BytesIO(payload))
        check('ordinary heap binary input retains all special IEEE payloads',query(c,"SELECT id,encode(float4send(f4),'hex'),encode(float8send(f8),'hex') FROM biz.float_heap WHERE id>=2001 ORDER BY id"),[(identity,format(f32,'08x'),format(f64,'016x'))for identity,f32,f64 in patterns])
        r['artifacts']={str(binary_input):sha256(binary_input)}
        query(c,'INSERT INTO biz.floating SELECT * FROM biz.float_heap')
        columns='id,encode(float4send(f4),\'hex\'),encode(float8send(f8),\'hex\'),note,flag'
        def wire(table,conn=c,where='TRUE'):return query(conn,'SELECT '+columns+' FROM '+table+' WHERE '+where+' ORDER BY id')
        def equal(label,conn=c,where='TRUE'):check(label,wire('biz.floating',conn,where),wire('biz.float_heap',conn,where))
        equal('initial exact native IEEE wire values, NULLs and companions')
        check('six-scalar catalog admission remains standard',query(c,"SELECT column_name,column_type FROM created_metadata.ducklake_column WHERE parent_column IS NULL ORDER BY column_order"),[('id','int32'),('f4','float32'),('f8','float64'),('note','varchar'),('flag','boolean')])
        check('signed zeros survive native inline writes',query(c,"SELECT encode(float4send(f4),'hex'),encode(float8send(f8),'hex') FROM biz.floating WHERE id=2001"),[('80000000','8000000000000000')])
        for predicate in ['f4 IS NULL','f8 IS NOT NULL','f4>0','f8<0',"f4='NaN'::real", "f8='Infinity'::double precision", "f4=ANY(ARRAY['NaN'::real,0::real,NULL])"]:
            equal('native floating predicate '+predicate,where=predicate)
        query(c,'ANALYZE biz.float_heap');query(c,'ANALYZE biz.floating')
        stats="SELECT attname,null_frac,avg_width,n_distinct,most_common_vals::text,most_common_freqs::text FROM pg_stats WHERE schemaname='biz' AND tablename=%s AND attname IN ('f4','f8') ORDER BY attname"
        check('typed native ANALYZE equals full heap sample',query(c,stats,('floating',)),query(c,stats,('float_heap',)))
        check('floating catalog bounds/nan remain conservative unknown',query(c,'SELECT column_id,min_value,max_value,contains_nan FROM created_metadata.ducklake_table_column_stats WHERE column_id IN (2,3) ORDER BY column_id'),[(2,None,None,None),(3,None,None,None)])
        def both(statement):
            query(c,'BEGIN')
            for table in ['biz.floating','biz.float_heap']:query(c,statement.format(table=table))
            query(c,'COMMIT')
        both('UPDATE {table} SET f4=f4*2,f8=f8/2 WHERE id BETWEEN 10 AND 40')
        both('UPDATE {table} SET f4=f4-1,f8=f8+1 WHERE id BETWEEN 10 AND 40')
        equal('two committed float updates preserve exact values')
        baseline=wire('biz.floating');before=t.head();protocol=t.protocol()
        query(c,'BEGIN');query(c,'SAVEPOINT nested')
        query(c,"INSERT INTO biz.floating VALUES(2000,'-0'::real,'-0'::double precision,'child',true)")
        query(c,'UPDATE biz.floating SET f4=7,f8=9 WHERE id=6')
        check('private after-image exposes typed floats',query(c,'SELECT f4::text,f8::text FROM biz.floating WHERE id=6'),[('7','9')])
        query(c,'ROLLBACK TO nested');query(c,'RELEASE SAVEPOINT nested');query(c,'COMMIT')
        check('float savepoint restores exact wire image',wire('biz.floating'),baseline)
        check('float savepoint changes no standard catalog',(t.head(),t.protocol()),(before,protocol))
        query(c,'PREPARE float_key(real) AS SELECT '+columns+' FROM biz.floating WHERE f4=$1 ORDER BY id')
        for value in ['0','NaN','Infinity','-Infinity',None,'7']:
            check('prepared typed parameter '+str(value),query(c,'EXECUTE float_key(%s)',(value,)),query(c,'SELECT '+columns+' FROM biz.float_heap WHERE f4=%s::real ORDER BY id',(value,)))
        query(c,'DEALLOCATE float_key')
        if a.mode=='old-provider':
            check('old SDK exact identity',r['provider_sha256'],'142b59847b047bd1dd988122d36e5d09335e430ec60314f357aa0c7760413529')
            protocol=t.protocol();head=t.head();paths=list(Path(t.empty_manifest['data_path']).rglob('*.parquet'))
            try:query(c,"SELECT og_lake.flush('biz.floating'::regclass)")
            except psycopg2.Error as exc:check('missing float SDK capability explicitly refused',exc.pgcode,'0A000')
            else:raise AssertionError('unadvertised float writer succeeded')
            check('refused flush leaves catalog and snapshot intact',(t.head(),t.protocol()),(head,protocol))
            check('refused flush creates no data candidates',list(Path(t.empty_manifest['data_path']).rglob('*.parquet')),paths)
            equal('old-provider refusal preserves every business row')
        else:
            query(c,"SELECT og_lake.flush('biz.floating'::regclass)");equal('native floating flush exact wire roundtrip')
            outputs=query(c,'SELECT path,record_count FROM created_metadata.ducklake_data_file ORDER BY data_file_id');check('float flush generated real files',len(outputs)>0)
            paths=[]
            for relative,count in outputs:
                found=[v for v in Path(t.empty_manifest['data_path']).rglob('*.parquet')if v.name==Path(relative).name]
                check('exact published data-file path resolves once',len(found),1);paths.append(found[0])
            check('published physical row population includes old versions',sum(int(v[1])for v in outputs)>1001)
            check('real published files exist',len(paths)>0)
            data_files='['+','.join(literal(v)for v in paths)+']'
            decoded=independent('SELECT id,f4,f8,note,flag FROM read_parquet('+data_files+') WHERE _ducklake_internal_snapshot_id=(SELECT max(_ducklake_internal_snapshot_id) FROM read_parquet('+data_files+')) ORDER BY id','decode-latest-input')
            check('independent reader decodes actual float columns',len(decoded)>0)
            expected_by_id={row[0]:row for row in wire('biz.float_heap')}
            def floating_hex(value,wide):
                return None if value is None else struct.pack('>d'if wide else'>f',float(value)).hex()
            independent_wire=[(row['id'],floating_hex(row['f4'],False),floating_hex(row['f8'],True),row['note'],row['flag'])for row in decoded]
            check('independent latest physical versions match every native IEEE value',independent_wire,[expected_by_id[row['id']]for row in decoded])
            types=independent('SELECT typeof(f4)AS f4,typeof(f8)AS f8 FROM read_parquet('+literal(paths[0])+') LIMIT 1','decode-types')
            check('standard Parquet float types',types,[{'f4':'FLOAT','f8':'DOUBLE'}])
            both('DELETE FROM {table} WHERE id IN(8,9,10)');both('UPDATE {table} SET f4=f4+3,f8=f8-2 WHERE id BETWEEN 100 AND 130')
            equal('file-origin float changes overlay exact old files')
            query(c,"SELECT og_lake.flush('biz.floating'::regclass)");equal('second float data/delete flush exact values')
            if a.check_file_nan_stats:
                flags=query(c,"SELECT s.data_file_id,s.column_id,s.contains_nan FROM created_metadata.ducklake_file_column_stats s JOIN created_metadata.ducklake_table t ON t.table_id=s.table_id WHERE t.table_name='floating' AND s.column_id IN(2,3) ORDER BY 1,2")
                check('floating file statistics exist',bool(flags))
                check('unmeasured floating file NaN flags stay unknown',flags,[(file,column,None)for file,column,value in flags])
                interop=out/'nan-interop';interop.mkdir();mutex=(ROOT/'.reference/opengauss-native-lake/.driver.lock').open('a+')
                fcntl.flock(mutex,fcntl.LOCK_EX|fcntl.LOCK_NB)
                try:
                    lock=json.loads((D/'source-lock.json').read_text());receipt=json.loads((ROOT/'.reference/opengauss-native-lake/build/receipt.json').read_text())
                    check('NaN interop exact reference source-lock',receipt['source_lock_hash'],sha256(D/'source-lock.json'))
                    ref=Reference(interop,lock,receipt);check('NaN interop exact binary',sha256(ref.binary),receipt['binary_sha256'])
                    names=[x[0]for x in query(t.observer,"SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='created_metadata' AND c.relkind='r' ORDER BY c.relname")]
                    actual=read_native_catalog(t.observer,t.schema,{'tables':[{'table_name':n}for n in names]},interop/'native-catalog.json')
                    actual_map={x['table_name']:x for x in actual['tables']}
                    check('special float inline values were transferred completely',all(not actual_map[x['table_name']]['rows']for x in actual_map['ducklake_inlined_data_tables']['rows']))
                    transported=interop/'transported.ducklake';transport_native(ref,transported,actual);fingerprint=sha256(transported)
                    for field,cast in [('f4','real'),('f8','double precision')]:
                        values=ref.run(ref.attach(transported,True)+'SELECT count(*) AS n FROM lake.main.floating WHERE isnan('+field+')','count-nan-'+field,result=True)
                        check('independent DuckLake NaN predicate '+field,int(values[0]['n']),int(scalar(c,"SELECT count(*) FROM biz.floating WHERE "+field+"='NaN'::"+cast)))
                    check('NaN predicate readback preserves the native catalog',sha256(transported),fingerprint)
                    r.setdefault('artifacts',{}).update({str(transported):fingerprint})
                finally:fcntl.flock(mutex,fcntl.LOCK_UN);mutex.close()
            for predicate in ['f4 IS NULL','f4>0',"f4='NaN'::real"]:equal('mixed floating predicate '+predicate,where=predicate)
            r.setdefault('artifacts',{}).update({str(f):sha256(f)for f in paths})
        check('every frozen input unchanged',all(sha256(f)==h for f,h in r['inputs'].items()))
        check('same installed provider at completion',sha256(provider),r['provider_sha256'])
        r['status']='pass'
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
