#!/usr/bin/env python3
"""Genuine DuckLake float catalog -> native heap/Ustore -> flush -> DuckLake readback.

Finite ordinary float values exercise catalog bounds without inventing metadata.
No concurrent external writer, vector/ANN, remote SDK or RAC qualification.
"""
import argparse
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
from reverse_reference import Reference,literal
from empty_reference_fixture import export_actual_tables
from empty_catalog_oracle import read_native_catalog,transport_native

def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--run-id',required=True)
    p.add_argument('--build-receipt',type=Path,required=True);p.add_argument('--storage',choices=['heap','ustore'],required=True)
    a=p.parse_args();validate_run_id(a.run_id);out=ROOT/'公共文档/openGauss原生湖存储/验证'/a.run_id;out.mkdir(exist_ok=False)
    r={'status':'fail','scope':__doc__+' '+a.storage,'assertions':[]};control=db=roles=None;mutex=None
    files=[Path(__file__),D/'tests/ustore_fixture.py',D/'tests/permission_api.py',D/'native_test_context.py',D/'native_runtime.py',
        D/'tests/inline_cleanup.py',D/'tests/file_dml_acceptance.py',D/'inline_fixture.py',D/'tests/native_auto_flush.py',D/'source-lock.json',
        D/'tests/reverse_reference.py',D/'tests/empty_reference_fixture.py',D/'tests/empty_catalog_oracle.py']
    r['inputs']={str(f):sha256(f)for f in files};archive=out/'executed-inputs';archive.mkdir()
    for f in files:shutil.copy2(f,archive/f.name)
    provider=ROOT/'.install/opengauss-native-lake-debug/lib/postgresql/og_lake_sdk/libog_lake_sdk_arrow.so';r['provider_sha256']=sha256(provider)
    def image(v):return {'rows':len(v),'sha256':hashlib.sha256(repr(v).encode()).hexdigest()}if isinstance(v,list)and len(v)>50 else v
    def check(name,got,want=True):
        good=got==want;r['assertions'].append(dict(name=name,observed=image(got),expected=image(want),status='pass'if good else'fail'))
        if not good:raise AssertionError(name)
    try:
        from psycopg2 import sql
        b=check_build(a.build_receipt);r.update(source_head=b['source_head'],binary_sha256=b['binary_sha256'])
        lock=json.loads((D/'source-lock.json').read_text());receipt=json.loads((ROOT/'.reference/opengauss-native-lake/build/receipt.json').read_text())
        check('reference exact source-lock receipt',receipt['source_lock_hash'],sha256(D/'source-lock.json'))
        refout=out/'reference';refout.mkdir();ref=Reference(refout,lock,receipt)
        check('actual fixed reference binary',sha256(ref.binary),receipt['binary_sha256']);r['reference_binary_sha256']=sha256(ref.binary)
        mutex=(ROOT/'.reference/opengauss-native-lake/.driver.lock').open('a+');fcntl.flock(mutex,fcntl.LOCK_EX|fcntl.LOCK_NB)
        catalog=out/'genuine.ducklake';data=out/'data';data.mkdir()
        ref.run('SET threads=1; ATTACH '+literal('ducklake:'+str(catalog))+" AS lake (DATA_PATH "+literal(str(data))+",DUCKLAKE_VERSION '1.1',AUTOMATIC_MIGRATION FALSE,DATA_INLINING_ROW_LIMIT 1000000); CREATE TABLE lake.main.floating(id INTEGER,f4 FLOAT,f8 DOUBLE); INSERT INTO lake.main.floating VALUES(1,1.25,2.5),(2,-3.5,-4.25),(3,NULL,NULL);",'genuine-create')
        exported={'scope':'actual_reference_catalog_export_not_host_compatibility','tables':export_actual_tables(ref,catalog)};write_json(out/'genuine-export.json',exported)
        by_name={t['table_name']:t for t in exported['tables']};check('genuine nonempty float bounds exist',all(v['min_value']is not None and v['max_value']is not None for v in by_name['ducklake_table_column_stats']['rows']if v['column_id']in[2,3]))
        native_tid=by_name['ducklake_table']['rows'][0]['table_id'];carrier=by_name['ducklake_inlined_data_tables']['rows'][0]['table_name']
        control=connect_control();check('automatic maintenance remains off',scalar(control,'SHOW enable_lake_maintenance'),'off')
        db=OwnedDatabase(a.run_id,out).create();roles=OwnedRoles(control,a.run_id,out);c,observer,old=[db.connect()for _ in range(3)]
        query(c,"CREATE EXTENSION og_lake VERSION '0.16'");query(c,'BEGIN')
        (import_catalog_ustore if a.storage=='ustore'else import_catalog)(c,'lake_metadata',exported);query(c,'COMMIT')
        owner=roles.create('private',False);query(c,'BEGIN');adopt_private_owner(c,exported,owner);query(c,'COMMIT')
        options=scalar(c,'SELECT reloptions FROM pg_class WHERE oid=%s::regclass',('lake_metadata.'+carrier,))
        check('actual requested carrier TableAM',('storage_type=ustore'in(options or[])),a.storage=='ustore')
        query(c,'CREATE SCHEMA biz');query(c,"SELECT og_lake.attach_catalog('lake_reference','lake_metadata',NULL::text,false)")
        query(c,sql.SQL("CREATE FOREIGN TABLE biz.floating(id integer,f4 real,f8 double precision) SERVER lake_reference OPTIONS(binding_mode 'attach',table_id {})").format(sql.Literal(str(native_tid))))
        query(c,'CREATE TABLE biz.reference(id integer,f4 real,f8 double precision)');query(c,'INSERT INTO biz.reference SELECT * FROM biz.floating')
        columns="id,encode(float4send(f4),'hex'),encode(float8send(f8),'hex')"
        def wire(table='biz.floating',conn=c):return query(conn,'SELECT '+columns+' FROM '+table+' ORDER BY id')
        def equal(label):check(label,wire(),wire('biz.reference'))
        equal('native decodes all genuine float inline values')
        def both(statement):
            query(c,'BEGIN')
            for table in ['biz.floating','biz.reference']:query(c,statement.format(table=table))
            query(c,'COMMIT')
        both('INSERT INTO {table} VALUES(5,999.125,-999.5)');equal('new values outside genuine external range remain visible')
        check('native write invalidates both old exact float ranges',query(c,'SELECT column_id,min_value,max_value,contains_nan FROM lake_metadata.ducklake_table_column_stats WHERE column_id IN(2,3) ORDER BY column_id'),[(2,None,None,None),(3,None,None,None)])
        both('UPDATE {table} SET f4=f4*2,f8=f8*(-3) WHERE id=1');both('UPDATE {table} SET f4=f4*2,f8=f8*(-3) WHERE id=1');equal('two native updates of reference identity retain typed values')
        query(c,'ANALYZE biz.floating');query(c,'ANALYZE biz.reference')
        stats="SELECT attname,null_frac,avg_width,n_distinct FROM pg_stats WHERE schemaname='biz' AND tablename=%s AND attname IN('f4','f8') ORDER BY attname"
        check('native floating ANALYZE statistics',query(c,stats,('floating',)),query(c,stats,('reference',)))
        query(old,'BEGIN');before=wire(conn=old)
        both('UPDATE {table} SET f4=f4+1,f8=f8-1 WHERE id=2')
        query(c,"SELECT og_lake.flush('biz.floating'::regclass)");equal('genuine-source native float flush exact wire roundtrip')
        check('retained float reader survives physical transfer',wire(conn=old),before);query(old,'COMMIT')
        check('native flush produced real standard files',int(scalar(c,'SELECT count(*) FROM lake_metadata.ducklake_data_file'))>0)
        # Read-only transport preserves every observed catalog value and path;
        # only SQL physical storage differs between test hosts.
        actual=read_native_catalog(observer,'lake_metadata',exported,out/'native-export.json')
        transported=out/'transported.ducklake';transport_native(ref,transported,actual);fingerprint=sha256(transported)
        decoded=ref.logical(transported,'lake.main.floating','native-flush-readback')
        decoded_wire=[(v['id'],None if v['f4']is None else struct.pack('>f',v['f4']).hex(),None if v['f8']is None else struct.pack('>d',v['f8']).hex())for v in sorted(decoded,key=lambda v:v['id'])]
        check('independent DuckLake resolves native snapshots/deletes and every float bit',decoded_wire,wire())
        check('read-only DuckLake does not alter transported metadata',sha256(transported),fingerprint)
        check('logical row IDs remain unique across source/native updates',len({v['logical_rowid']for v in decoded}),len(decoded))
        check('all executed inputs remain unchanged',all(sha256(f)==h for f,h in r['inputs'].items()))
        check('same installed provider at completion',sha256(provider),r['provider_sha256'])
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
                if not r.get('database_cleanup')or r['database_cleanup']['returncode']:raise RuntimeError('Owned DB cleanup not proven; roles retained')
                r['role_cleanup']=cleanup_roles(control,roles.path,a.run_id);r=roles.redact(r)
            r['cleanup']='pass'
        except BaseException as exc:r.update(cleanup='fail',cleanup_error=str(exc),status='fail')
        if control is not None and not control.closed:control.close()
        write_json(out/'result.json',r)
    print(json.dumps(dict(status=r['status'],assertions=len(r['assertions']),reason=r.get('reason'),result=str(out/'result.json')),ensure_ascii=False));return 0 if r['status']=='pass'else 1
if __name__=='__main__':raise SystemExit(main())
