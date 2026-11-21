#!/usr/bin/env python3
"""Genuine DuckLake LIST<float32> -> native vector(n), without ANN or writes.

Reference DuckDB inline arrays are read as canonical VARCHAR, matching the
standard PostgreSQL metadata adapter. Catalog identities and all list values
are preserved; this is an offline physical test transport, not a connector.
"""
import argparse
import copy
import fcntl
import hashlib
import json
from pathlib import Path
import shutil
import struct
import sys
import traceback

ROOT=Path('/home/qiumc/data/code/cpp');D=ROOT/'integration/opengauss-native-lake'
sys.path[:0]=[str(D),str(D/'tests')]
from file_dml_acceptance import check_build
from inline_fixture import query,scalar,import_catalog
from lake_harness import sha256,validate_run_id,write_json
from native_auto_flush import connect_control
from native_test_context import OwnedDatabase
from permission_api import OwnedRoles,cleanup_roles,adopt_private_owner
from ustore_fixture import import_catalog_ustore
from reverse_reference import Reference,literal,qi
from empty_reference_fixture import export_actual_tables


def physical_inline_export(ref,catalog,exported):
    result=copy.deepcopy(exported)
    carriers={v['table_name']for t in result['tables']if t['table_name']=='ducklake_inlined_data_tables'for v in t['rows']}
    for t in result['tables']:
        arrays=[c for c in t['columns']if c['data_type'].endswith('[]')]
        if not arrays:continue
        if t['table_name']not in carriers or any(c['data_type']!='FLOAT[]'for c in arrays):
            raise ValueError('Unreviewed nested physical transport')
        names={c['column_name']for c in arrays}
        expressions=[('CAST('+qi(c['column_name'])+' AS VARCHAR)'if c['column_name']in names else qi(c['column_name']))+' AS '+qi(c['column_name'])for c in t['columns']]
        t['rows']=ref.run('SELECT '+','.join(expressions)+' FROM '+qi(t['schema_name'])+'.'+qi(t['table_name']),
            'canonical-inline-'+t['table_name'],catalog,True,True)
        for c in arrays:c['data_type']='VARCHAR'
    return result


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--run-id',required=True)
    p.add_argument('--build-receipt',type=Path,required=True)
    p.add_argument('--storage',choices=['file','heap','ustore'],required=True)
    p.add_argument('--old-provider',action='store_true',help='Expect explicit refusal of vector files before element-ID capability')
    p.add_argument('--writes-enabled',action='store_true',help='Later source implements writes; this suite remains read-only and skips the obsolete write-refusal checks')
    a=p.parse_args();validate_run_id(a.run_id);out=ROOT/'公共文档/openGauss原生湖存储/验证'/a.run_id;out.mkdir(exist_ok=False)
    r={'status':'fail','scope':__doc__+' '+a.storage,'assertions':[]};control=db=roles=mutex=None
    files=[Path(__file__),D/'tests/ustore_fixture.py',D/'tests/permission_api.py',D/'native_test_context.py',D/'native_runtime.py',
        D/'tests/inline_cleanup.py',D/'tests/file_dml_acceptance.py',D/'inline_fixture.py',D/'tests/native_auto_flush.py',D/'source-lock.json',
        D/'tests/reverse_reference.py',D/'tests/empty_reference_fixture.py']
    r['inputs']={str(f):sha256(f)for f in files};archive=out/'executed-inputs';archive.mkdir()
    for f in files:shutil.copy2(f,archive/f.name)
    provider=ROOT/'.install/opengauss-native-lake-debug/lib/postgresql/og_lake_sdk/libog_lake_sdk_arrow.so';r['provider_sha256']=sha256(provider)
    def image(v):return {'rows':len(v),'sha256':hashlib.sha256(repr(v).encode()).hexdigest()}if isinstance(v,list)and len(v)>20 else v
    def check(name,got,want=True):
        good=got==want;r['assertions'].append(dict(name=name,observed=image(got),expected=image(want),status='pass'if good else'fail'))
        if not good:raise AssertionError(name)
    def rejected(c,name,statement,states,values=None,message=None):
        try:query(c,statement,values)
        except Exception as exc:
            check(name,exc.pgcode in states);r['assertions'][-1]['sqlstate']=exc.pgcode
            if message is not None:check(name+' exact rejection reason',message in str(exc))
        else:check(name,False)
    try:
        from psycopg2 import sql
        b=check_build(a.build_receipt);r.update(source_head=b['source_head'],binary_sha256=b['binary_sha256'])
        lock=json.loads((D/'source-lock.json').read_text());receipt=json.loads((ROOT/'.reference/opengauss-native-lake/build/receipt.json').read_text())
        check('exact reference source-lock',receipt['source_lock_hash'],sha256(D/'source-lock.json'))
        refout=out/'reference';refout.mkdir();ref=Reference(refout,lock,receipt)
        check('exact reference binary',sha256(ref.binary),receipt['binary_sha256']);r['reference_binary_sha256']=sha256(ref.binary)
        mutex=(ROOT/'.reference/opengauss-native-lake/.driver.lock').open('a+');fcntl.flock(mutex,fcntl.LOCK_EX|fcntl.LOCK_NB)
        catalog=out/'genuine.ducklake';data=out/'data';data.mkdir()
        limit=0 if a.storage=='file'else 1000000
        attach='ATTACH '+literal('ducklake:'+str(catalog))+" AS lake (DATA_PATH "+literal(str(data))+",DUCKLAKE_VERSION '1.1',AUTOMATIC_MIGRATION FALSE,DATA_INLINING_ROW_LIMIT "+str(limit)+');'
        ref.run(attach+"CREATE TABLE lake.main.vectors(id INTEGER,v FLOAT[],note VARCHAR); INSERT INTO lake.main.vectors SELECT i::INTEGER, CASE WHEN i%17=0 THEN NULL ELSE [i::FLOAT,(-i)::FLOAT,0.25::FLOAT] END,'row-'||i FROM range(1,1026) t(i); INSERT INTO lake.main.vectors VALUES(2001,[0.0::FLOAT,'-0.0'::FLOAT,1.0::FLOAT],'signed zero'),(2002,[-1.0::FLOAT,'1e-45'::FLOAT,'-1e-45'::FLOAT],'subnormal'); UPDATE lake.main.vectors SET v=[7.0::FLOAT,8.0::FLOAT,9.0::FLOAT] WHERE id=3; DELETE FROM lake.main.vectors WHERE id=7;",'genuine-create')
        for name,value in [('wrong_dim','[1.0::FLOAT,2.0::FLOAT]'),('null_element','[1.0::FLOAT,NULL::FLOAT,3.0::FLOAT]'),('nan_element',"[1.0::FLOAT,'NaN'::FLOAT,3.0::FLOAT]"),('inf_element',"[1.0::FLOAT,'Infinity'::FLOAT,3.0::FLOAT]"),('binding_dimension','[1.0::FLOAT,2.0::FLOAT,3.0::FLOAT]')]:
            ref.run(ref.attach(catalog)+"CALL lake.set_option('data_inlining_row_limit',"+str(limit)+"); CREATE TABLE lake.main."+name+'(id INTEGER,v FLOAT[]); INSERT INTO lake.main.'+name+' VALUES(1,'+value+');','genuine-'+name)
        # A large legal vector exercises native varlena / inline TOAST copying.
        ref.run(ref.attach(catalog)+"CALL lake.set_option('data_inlining_row_limit',"+str(limit)+"); CREATE TABLE lake.main.wide(id INTEGER,v FLOAT[]); INSERT INTO lake.main.wide SELECT 1,list((i%19)::FLOAT ORDER BY i) FROM range(16000)t(i);",'genuine-wide')
        expected=ref.logical(catalog,'lake.main.vectors','expected-good')
        exported={'scope':'actual_reference_catalog_export_not_host_compatibility','tables':export_actual_tables(ref,catalog)}
        write_json(out/'genuine-export.json',exported);transport=physical_inline_export(ref,catalog,exported);write_json(out/'physical-export.json',transport)
        by_name={t['table_name']:t for t in exported['tables']};tables={v['table_name']:v['table_id']for v in by_name['ducklake_table']['rows']}
        columns=by_name['ducklake_column']['rows'];children=[v for v in columns if v['parent_column']is not None]
        check('genuine LIST has independent float32 element IDs',bool(children)and all(v['column_type']=='float32'and v['column_id']!=v['parent_column']for v in children))
        check('actual file/inline storage selected',bool(by_name['ducklake_data_file']['rows']),a.storage=='file')
        control=connect_control();check('automatic maintenance off',scalar(control,'SHOW enable_lake_maintenance'),'off')
        db=OwnedDatabase(a.run_id,out).create();roles=OwnedRoles(control,a.run_id,out);c=db.connect()
        query(c,"CREATE EXTENSION og_lake VERSION '0.16'");query(c,'BEGIN')
        (import_catalog_ustore if a.storage=='ustore'else import_catalog)(c,'lake_metadata',transport);query(c,'COMMIT')
        owner=roles.create('private',False);query(c,'BEGIN');adopt_private_owner(c,transport,owner);query(c,'COMMIT')
        query(c,'CREATE SCHEMA biz');query(c,"SELECT og_lake.attach_catalog('lake_reference','lake_metadata',NULL::text,false)")
        def bind(name,dimension=3,target=None):
            query(c,sql.SQL("CREATE FOREIGN TABLE biz.{}(id integer,v vector({}){}) SERVER lake_reference OPTIONS(binding_mode 'attach',table_id {})").format(
                sql.Identifier(target or name),sql.SQL(str(dimension)),sql.SQL(',note text'if name=='vectors'else''),sql.Literal(str(tables[name]))))
        bind('vectors');bind('wide',16000)
        r['metadata_before']=query(c,'SELECT snapshot_id,next_catalog_id,next_file_id FROM lake_metadata.ducklake_snapshot ORDER BY snapshot_id')
        statement="SELECT id,substring(encode(vector_send(v),'hex') from 9),note FROM biz.vectors ORDER BY id"
        if a.old_provider and a.storage=='file':
            rejected(c,'old provider refuses missing element identity capability',statement,{'0A000'},message='LIST values and element identity capabilities')
        else:
            want=[(v['id'],None if v['v']is None else b''.join(struct.pack('>f',x)for x in v['v']).hex(),v['note'])for v in sorted(expected,key=lambda v:v['id'])]
            check('all genuine values/deletions/NULL and IEEE element bits',query(c,statement),want)
            query(c,'CREATE TABLE biz.reference(id integer,v vector(3),note text)')
            with c.cursor()as cur:cur.executemany('INSERT INTO biz.reference VALUES(%s,%s::vector,%s)',[(v['id'],None if v['v']is None else '['+','.join(map(str,v['v']))+']',v['note'])for v in expected])
            for label,suffix in [('L2','v <-> %s::vector'),('inner product','v <#> %s::vector'),('cosine','v <=> %s::vector')]:
                q='SELECT id,'+suffix+' AS distance FROM {} WHERE v IS NOT NULL ORDER BY distance,id LIMIT 25'
                check('native '+label+' exact filter/order',query(c,q.format('biz.vectors'),('[1,2,3]',)),query(c,q.format('biz.reference'),('[1,2,3]',)))
            check('correlated parameter rescans',query(c,'SELECT x.id,(SELECT vector_dims(v) FROM biz.vectors t WHERE t.id=x.id) FROM (SELECT 3 AS id UNION ALL SELECT 17 UNION ALL SELECT 2001) x ORDER BY x.id'),[(3,3),(17,None),(2001,3)])
            check('max-dimension native value',query(c,"SELECT id,vector_dims(v),octet_length(vector_send(v)) FROM biz.wide"),[(1,16000,64004)])
            query(c,"SET statement_timeout=20")
            rejected(c,'cancellation releases active vector scan','SELECT pg_sleep(0.01),v FROM biz.vectors',{'57014'})
            query(c,'SET statement_timeout=15000');check('same session reusable after cancellation',int(scalar(c,'SELECT count(*) FROM biz.vectors')),len(want))
            # Public vector input rejects text NULL as invalid syntax (22P02);
            # binary Arrow NULL elements use the native array null code 22004.
            null_state={'22004'}if a.storage=='file'else{'22P02'}
            for name,state in [('wrong_dim',{'22000'}),('null_element',null_state),('nan_element',{'22000'}),('inf_element',{'22000'})]:
                bind(name);rejected(c,'reject '+name,'SELECT * FROM biz.'+name,state)
            # Host bindings are intentionally one-to-one per server/table ID.
            # Use a separate genuine definition to test the incompatible typmod.
            bind('binding_dimension',4);rejected(c,'reject incompatible bound dimension','SELECT * FROM biz.binding_dimension',{'22000'})
            query(c,'BEGIN');query(c,'SAVEPOINT identity_fault')
            query(c,'UPDATE lake_metadata.ducklake_column SET column_id=column_id+100000 WHERE table_id=%s AND parent_column IS NOT NULL',(tables['vectors'],))
            if a.storage=='file':rejected(c,'reject mismatched element field ID',statement,{'XX001'})
            query(c,'ROLLBACK TO SAVEPOINT identity_fault');query(c,'COMMIT')
            check('identity fault rollback restores original vector read',query(c,statement),want)
        # Current slice must refuse every write before publishing any state.
        if not a.writes_enabled:
            for label,q in [('INSERT',"INSERT INTO biz.vectors VALUES(9999,'[1,2,3]'::vector,'blocked')"),('UPDATE',"UPDATE biz.vectors SET v='[3,2,1]'::vector WHERE id=3"),('DELETE','DELETE FROM biz.vectors WHERE id=3'),('flush',"SELECT og_lake.flush('biz.vectors'::regclass)")]:
                rejected(c,'explicit vector '+label+' gate',q,{'0A000'})
        check('read/error/cancellation/write refusal never publishes snapshots',query(c,'SELECT snapshot_id,next_catalog_id,next_file_id FROM lake_metadata.ducklake_snapshot ORDER BY snapshot_id'),r['metadata_before'])
        check('executed inputs unchanged',all(sha256(f)==h for f,h in r['inputs'].items()))
        check('provider identity unchanged',sha256(provider),r['provider_sha256'])
        r['artifacts']={str(f):sha256(f)for f in data.rglob('*.parquet')};r['status']='pass'
    except BaseException as exc:r.update(reason=str(exc),traceback=traceback.format_exc())
    finally:
        if mutex is not None:mutex.close()
        if db is not None:
            for c in list(db.connections):
                if not c.closed:
                    try:query(c,'ROLLBACK')
                    except Exception:pass
        try:
            if db is not None:db.cleanup();r['database_cleanup']=db.cleanup_result
            if roles is not None:
                if not r.get('database_cleanup')or r['database_cleanup']['returncode']:raise RuntimeError('Owned database cleanup unproven; retain roles')
                r['role_cleanup']=cleanup_roles(control,roles.path,a.run_id);r=roles.redact(r)
            r['cleanup']='pass'
        except BaseException as exc:r.update(cleanup='fail',cleanup_error=str(exc),status='fail')
        if control is not None and not control.closed:control.close()
        write_json(out/'result.json',r)
    print(json.dumps(dict(status=r['status'],assertions=len(r['assertions']),reason=r.get('reason'),result=str(out/'result.json')),ensure_ascii=False));return 0 if r['status']=='pass'else 1

if __name__=='__main__':raise SystemExit(main())
