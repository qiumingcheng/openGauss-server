#!/usr/bin/env python3
"""Real native physical-source scalar build/publication/lifetime cases.

READY artifacts and coverage are verified independently. Query activation still
requires separate runtime content/source validation and actual query evidence.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
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

def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--run-id',required=True)
    p.add_argument('--build-receipt',type=Path,required=True)
    p.add_argument('--mode',choices=['publication','transactions','guards','permissions','repair'],required=True)
    a=p.parse_args();validate_run_id(a.run_id)
    out=ROOT/'公共文档/openGauss原生湖存储/验证'/a.run_id;out.mkdir(exist_ok=False)
    r={'status':'fail','scope':__doc__+' '+a.mode,'assertions':[],
       'limitations':['No file query activation, Ustore index, DiskANN/fusion, performance, physical GC, remote/RAC or commercial qualification.']}
    paths=[Path(__file__),D/'tests/native_table_create.py',D/'tests/permission_api.py',D/'native_test_context.py',
        D/'native_runtime.py',D/'tests/inline_cleanup.py',D/'tests/file_dml_acceptance.py',D/'inline_fixture.py',
        D/'tests/native_auto_flush.py',D/'source-lock.json']
    r['inputs']={str(f):sha256(f)for f in paths};archive=out/'executed-inputs';archive.mkdir()
    for f in paths:shutil.copy2(f,archive/f.name)
    duckdb=ROOT/'.reference/opengauss-native-lake/build/duckdb-gcc10/duckdb'
    provider=ROOT/'.install/opengauss-native-lake-debug/lib/postgresql/og_lake_sdk/libog_lake_sdk_arrow.so'
    r.update(provider_sha256=sha256(provider),reference_binary_sha256=sha256(duckdb))
    control=db=roles=t=None
    def image(value):
        return {'rows':len(value),'sha256':hashlib.sha256(repr(value).encode()).hexdigest()}if isinstance(value,list)and len(value)>30 else value
    def check(name,got,want=True):
        good=got==want;r['assertions'].append(dict(name=name,observed=image(got),expected=image(want),status='pass'if good else'fail'))
        if not good:raise AssertionError(name)
    def literal(value):return "'"+str(value).replace("'","''")+"'"
    def independent(sql,label):
        (out/(label+'.sql')).write_text(sql)
        value=subprocess.run([str(duckdb),'-no-init','-batch','-json','-c','SET threads=1;'+sql],
            text=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=60)
        (out/(label+'.stdout')).write_text(value.stdout);(out/(label+'.stderr')).write_text(value.stderr)
        if value.returncode:raise RuntimeError('Independent SQL failed: '+label)
        return json.loads(value.stdout)if value.stdout.strip()else[]
    try:
        from psycopg2 import sql
        import psycopg2
        built=check_build(a.build_receipt);r.update(source_head=built['source_head'],binary_sha256=built['binary_sha256'])
        control=connect_control();check('automatic scheduler remains off',scalar(control,'SHOW enable_lake_maintenance'),'off')
        with(out/'fixture.log').open('w')as log:
            subprocess.run([sys.executable,str(D/'tests/catalog_fixture_bundle.py'),'--run-id',a.run_id,'--output',str(out/'fixture/manifest.json')],
                stdout=log,stderr=subprocess.STDOUT,check=True,timeout=300)
        db=OwnedDatabase(a.run_id,out).create();roles=OwnedRoles(control,a.run_id,out)
        t=Case(db,SimpleNamespace(mode='net_dml',sql_version='0.16',manifest=out/'fixture/manifest.json',output=out/'result.json',run_id=a.run_id),r,roles)
        owner_conn=t.a
        if a.mode=='permissions':
            owner=roles.create('business');other=roles.create('other')
            for role in [owner,other]:query(t.a,sql.SQL('GRANT USAGE ON SCHEMA biz TO {}').format(sql.Identifier(role)))
            query(t.a,sql.SQL('GRANT CREATE ON SCHEMA biz TO {}').format(sql.Identifier(owner)))
            query(t.a,sql.SQL('GRANT USAGE ON FOREIGN SERVER created_lake TO {}').format(sql.Identifier(owner)))
            owner_conn,identity=roles.connect(db,'business');unprivileged,identity2=roles.connect(db,'other');r['ordinary_identities']=[identity,identity2]
        t.create(conn=owner_conn)
        query(t.a,"SELECT og_lake.set_option('biz.orders'::regclass,'data_inlining_row_limit','50000')")
        query(owner_conn,'INSERT INTO biz.orders SELECT CASE WHEN i%17=0 THEN NULL ELSE i%101 END,i FROM generate_series(1,12000)i')
        for operation in ['UPDATE biz.orders SET val=val+20000 WHERE id=7','UPDATE biz.orders SET val=val+20000 WHERE id=7','DELETE FROM biz.orders WHERE id=11']:
            query(owner_conn,operation)
        expected=query(owner_conn,'SELECT id,val FROM biz.orders ORDER BY id NULLS LAST,val')
        query(t.a,"SELECT og_lake.flush('biz.orders'::regclass)")
        physical=int(scalar(t.a,'SELECT sum(record_count) FROM created_metadata.ducklake_data_file'))
        check('fixture has ended and current physical file versions',physical>len(expected))
        uuid=str(scalar(owner_conn,"SELECT og_lake.create_lake_index('biz.orders'::regclass,'ix_id',%s::jsonb)",
            (json.dumps(dict(version=1,kind='scalar',method='btree',column='id')),)))
        store=out/'index-store';store.mkdir();other_store=out/'another-index-store';other_store.mkdir()
        def configure(path=store):return query(t.a,"SELECT og_lake.set_option('created_lake'::name,'index_root',%s::text)",(str(path),))
        def build(conn=owner_conn):return query(conn,'SELECT og_lake.build_lake_index(%s::uuid)',(uuid,))
        def segments(conn=t.observer):return query(conn,'SELECT segment_uuid::text,state,artifact_root_uri,source_manifest_hash,manifest_hash,build_token::text FROM og_lake_internal.index_segment WHERE index_uuid=%s::uuid ORDER BY segment_uuid',(uuid,))
        def coverage(conn=t.observer):return query(conn,'SELECT segment_uuid::text,source_file_id,source_path,physical_row_count,source_checksum,schema_mapping_hash,active FROM og_lake_internal.index_coverage WHERE index_uuid=%s::uuid ORDER BY segment_uuid,source_file_id',(uuid,))
        def rejected(conn,statement,code,args=None):
            try:query(conn,statement,args)
            except psycopg2.Error as exc:check('rejected '+statement[:80],exc.pgcode,code);query(conn,'ROLLBACK')
            else:raise AssertionError('Unexpected success: '+statement)
        if a.mode=='guards':
            query(t.a,'BEGIN');configure();rejected(t.a,'SELECT og_lake.build_lake_index(%s::uuid)','55000',(uuid,))
            check('uncommitted root admission prepares no external files',list(store.rglob('*.parquet')),[])
            check('rejected configuration rolls back',scalar(t.a,'SELECT index_root FROM og_lake_internal.catalog_binding'),None)
        configure();head=t.head();protocol=t.protocol()
        if a.mode=='transactions':
            for child in [False,True]:
                query(owner_conn,'BEGIN')
                if child:query(owner_conn,'SAVEPOINT derived')
                build();pending=segments(owner_conn);paths=[Path(s[2])/name for s in pending for name in ['page_data.parquet','page_lookup.parquet']]
                check('native build creates genuine finished candidates',len(paths)>0 and all(x.is_file()for x in paths))
                check('other session cannot see uncommitted READY coverage',(segments(),coverage()),([],[]))
                if child:
                    query(owner_conn,'ROLLBACK TO derived');query(owner_conn,'RELEASE SAVEPOINT derived')
                    query(owner_conn,'INSERT INTO biz.heap_marker VALUES(77)');query(owner_conn,'COMMIT')
                else:
                    query(owner_conn,'INSERT INTO biz.heap_marker VALUES(66)');query(owner_conn,'ROLLBACK')
                check('aborted finished candidates are physically discarded',all(not x.exists()for x in paths))
                check('index publication and native inline binding roll back',(segments(),coverage(),scalar(t.a,'SELECT inline_index_oid FROM og_lake_internal.index_definition WHERE index_uuid=%s::uuid',(uuid,))),([],[],None))
            check('parent ordinary heap survives only the released child rollback',query(t.a,'SELECT * FROM biz.heap_marker'),[(77,)])
            rejected(t.a,"SELECT og_lake.set_option('created_lake'::name,'index_root',%s::text)",'55000',(str(other_store),))
            check('orphan segment directories retain their discoverable root',len(list(store.iterdir()))>0)
        if a.mode=='permissions':
            rejected(unprivileged,'SELECT og_lake.build_lake_index(%s::uuid)','42501',(uuid,))
            query(owner_conn,'BEGIN READ ONLY');rejected(owner_conn,'SELECT og_lake.build_lake_index(%s::uuid)','25006',(uuid,))
            check('refused builds produce no files',list(store.rglob('*.parquet')),[])
        build();ready=segments();covered=coverage();r['built_segments']=ready;r['built_coverage']=covered
        check('every current physical source gets one active coverage',len(covered),int(scalar(t.a,'SELECT count(*) FROM created_metadata.ducklake_data_file')))
        check('all built segments have guarded READY manifests',len(ready)>0 and all(s[1]=='READY'and len(s[3])==64 and len(s[4])==64 for s in ready))
        check('coverage includes ended physical versions',sum(c[3]for c in covered),physical)
        check('opaque object identities are explicitly tagged',all(c[4].startswith('object-version-v1:')and len(c[5])==64 and c[6]for c in covered))
        for number,c in enumerate(covered):
            directory=Path(next(s[2]for s in ready if s[0]==c[0]));data=directory/'page_data.parquet';lookup=directory/'page_lookup.parquet'
            check('genuine data and lookup artifacts exist',data.is_file()and lookup.is_file())
            source=independent('SELECT id AS key,file_row_number AS pos FROM read_parquet('+literal(c[2])+',file_row_number=true) ORDER BY file_row_number','physical-source-'+str(number))
            postings=independent('SELECT "values" AS key,file_id,row_position AS pos FROM read_parquet('+literal(data)+') ORDER BY row_position','index-postings-'+str(number))
            check('every raw source key/NULL/address matches independently decoded artifact',postings,[dict(key=x['key'],file_id=c[1],pos=x['pos'])for x in source])
            bounds=independent('SELECT count(*) AS pages,sum(row_count)::BIGINT AS rows FROM read_parquet('+literal(lookup)+')','lookup-'+str(number))
            check('page directory covers exact raw population',bounds[0]['rows'],c[3])
            r.setdefault('artifacts',{}).update({str(data):sha256(data),str(lookup):sha256(lookup)})
        build();check('repeating build skips unchanged READY physical sources',(segments(),coverage()),(ready,covered))
        check('builder allocates no business snapshot or IDs',t.head(),head)
        check('complete standard catalog and inline data unchanged',t.protocol(),protocol)
        check('business file reads remain exactly equal',query(owner_conn,'SELECT id,val FROM biz.orders ORDER BY id NULLS LAST,val'),expected)
        index_status=json.loads(scalar(owner_conn,'SELECT og_lake.lake_index_status(%s::uuid)',(uuid,)))
        check('READY current-input coverage is reported',index_status['file_index_ready'],True)
        check('coverage count matches current physical inputs',index_status['file_covered_inputs'],len(covered))
        check('all current inputs have metadata coverage',index_status['file_uncovered_inputs'],0)
        check('metadata coverage explicitly requires runtime validation',index_status['file_runtime_validation'],True)
        if a.mode=='repair':
            def active():return [c for c in coverage()if c[6]]
            def directory(c):return Path(next(s[2]for s in segments()if s[0]==c[0]))
            def unchanged_source(previous,current):
                return previous[1:6]==current[1:6]
            def exact(label):
                check(label,query(t.a,'SELECT id,val FROM biz.orders ORDER BY id NULLS LAST,val'),expected)
            old=active()[0];original_directory=directory(old)
            lookup=original_directory/'page_lookup.parquet'
            shutil.copy2(lookup,out/'removed-lookup-original.parquet');lookup.unlink()
            exact('missing lookup keeps complete business reads')
            build();fresh=next(c for c in active()if c[1]==old[1])
            check('same immutable source receives a new segment',fresh[0]!=old[0])
            check('repair preserves source version, schema, population and path',unchanged_source(old,fresh))
            check('only replaced coverage is retired',next(s[1]for s in segments()if s[0]==old[0]),'RETIRED')
            check('repair never overwrites a missing old artifact',not lookup.exists())
            check('old companion remains for protected cleanup',(original_directory/'page_data.parquet').is_file())
            stable=(segments(),coverage());build()
            check('verified healthy segments retain their original identity',(segments(),coverage()),stable)
            exact('missing-artifact repair preserves every value')

            current=next(c for c in active()if c[1]==old[1]);data=directory(current)/'page_data.parquet'
            groups=independent('SELECT row_group_id,data_page_offset FROM parquet_metadata('+literal(data)+') '
                'WHERE column_id=0 ORDER BY row_group_id DESC LIMIT 1','last-leaf-offset')
            check('repair test includes a real later leaf',groups[0]['row_group_id']>0)
            offset=int(groups[0]['data_page_offset'])+8
            with data.open('r+b')as stream:
                stream.seek(offset);byte=stream.read(1);check('corruption changes an actual data page byte',len(byte),1)
                stream.seek(offset);stream.write(bytes([byte[0]^0x5a]));stream.flush();os.fsync(stream.fileno())
            before_repair=(segments(),coverage())
            query(t.a,'BEGIN');build();own=coverage(t.a)
            pending=next(c for c in own if c[6]and c[1]==current[1])
            own_segments=segments(t.a);pending_directory=Path(next(s[2]for s in own_segments if s[0]==pending[0]))
            check('streaming validation detects the damaged later leaf',pending[0]!=current[0])
            check('private repaired files actually exist',all((pending_directory/name).is_file()for name in ['page_data.parquet','page_lookup.parquet']))
            query(t.a,'ROLLBACK')
            check('repair rollback restores exact prior metadata',(segments(),coverage()),before_repair)
            check('repair rollback removes its completed candidate files',not list(pending_directory.glob('*.parquet')))
            check('repair rollback retains discoverable directory',pending_directory.is_dir())
            exact('damaged index and aborted repair cannot change business results')
            build();repaired=next(c for c in active()if c[1]==current[1])
            check('retry publishes fresh immutable coverage',repaired[0]not in [current[0],pending[0]])
            check('repaired leaf keeps the same source object identity',unchanged_source(current,repaired))
            check('damaged prior segment retires after committed replacement',next(s[1]for s in segments()if s[0]==current[0]),'RETIRED')
            exact('streaming leaf repair preserves the full multiset')
            check('all repairs allocate no business snapshot or IDs',t.head(),head)
            check('all repairs preserve the entire standard protocol',t.protocol(),protocol)
        if a.mode=='guards':
            old=covered[0];path=Path(old[2]);st=path.stat()
            fd=os.open(path,os.O_RDWR)
            try:
                byte=os.pread(fd,1,4);os.pwrite(fd,bytes([byte[0]^1]),4);os.fsync(fd);os.pwrite(fd,byte,4);os.fsync(fd)
                os.utime(fd,ns=(st.st_atime_ns,st.st_mtime_ns))
            finally:os.close(fd)
            check('same-size restored bytes differ only in object history',path.stat().st_size,st.st_size)
            build();current=[c for c in coverage()if c[6]]
            check('changed object identity gets fresh active coverage',next(c[4]for c in current if c[1]==old[1])!=old[4])
            check('replaced immutable source retires its old segment',next(s[1]for s in segments()if s[0]==old[0]),'RETIRED')
            check('same physical bytes preserve all business results',query(t.a,'SELECT id,val FROM biz.orders ORDER BY id NULLS LAST,val'),expected)
        if a.mode=='permissions':check('builder restores real LOGIN identity',scalar(owner_conn,'SELECT current_user'),owner)
        if a.mode=='publication':
            query(owner_conn,'SELECT og_lake.drop_lake_index(%s::uuid)',(uuid,))
            check('retirement disables all coverage',all(not c[6]for c in coverage()))
            check('retirement retains derivative files for protected cleanup',all(Path(f).is_file()for f in r['artifacts']))
        check('executed inputs remain exact',all(sha256(f)==h for f,h in r['inputs'].items()))
        check('native provider remains the measured genuine version',sha256(provider),r['provider_sha256'])
        check_build(a.build_receipt);r['status']='pass'
    except BaseException as exc:r.update(reason=str(exc),traceback=traceback.format_exc())
    finally:
        if db is not None:
            try:db.cleanup();r['database_cleanup']=db.cleanup_result
            except BaseException as exc:r.update(status='fail',cleanup_error=str(exc))
        if roles is not None:
            try:
                if not r.get('database_cleanup')or r['database_cleanup']['returncode']:raise RuntimeError('Owned DB cleanup unproven; roles retained')
                r['role_cleanup']=cleanup_roles(control,roles.path,a.run_id);r=roles.redact(r)
            except BaseException as exc:r.update(status='fail',role_cleanup_error=str(exc))
        if control is not None:control.close()
        write_json(out/'result.json',r)
    print(json.dumps(dict(status=r['status'],checks=len(r['assertions']),reason=r.get('reason'),result=str(out/'result.json')),ensure_ascii=False))
    return 0 if r['status']=='pass'else 1

if __name__=='__main__':raise SystemExit(main())
