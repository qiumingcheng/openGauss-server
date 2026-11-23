#!/usr/bin/env python3
"""Real non-PQ native DiskANN build/export and immutable file publication.

Independent DuckDB decodes physical source rows and every graph posting. This
is a build/publication gate, not lake file ANN query, recall, PQ or SMP proof.
"""
import argparse
import hashlib
import json
from pathlib import Path
import shutil
import struct
import subprocess
import sys
import traceback
from types import SimpleNamespace
ROOT=Path(__file__).resolve().parents[3];D=ROOT/'integration/opengauss-native-lake'
sys.path[:0]=[str(D),str(D/'tests')]
from file_dml_acceptance import check_build
from inline_fixture import query,scalar
from lake_harness import sha256,validate_run_id,write_json
from native_auto_flush import connect_control
from native_runtime import PROFILE
from native_test_context import OwnedDatabase
from native_table_create import Case
from permission_api import OwnedRoles,cleanup_roles

def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--run-id',required=True)
    p.add_argument('--build-receipt',type=Path,required=True);p.add_argument('--metric',choices=['l2','ip','cosine'],default='l2')
    p.add_argument('--mode',choices=['publication','transactions','repair','empty','cancel','subnormal'],default='publication')
    p.add_argument('--build-timeout-ms',type=int,choices=[15000,60000,120000],default=15000)
    a=p.parse_args();validate_run_id(a.run_id)
    if PROFILE!='ann-debug':p.error('Use matching ann-debug catalogs; never edit DiskANN callbacks')
    out=ROOT/'公共文档/openGauss原生湖存储/验证'/a.run_id;out.mkdir(exist_ok=False)
    r=dict(status='fail',scope=__doc__,metric=a.metric,mode=a.mode,statement_timeout_ms=a.build_timeout_ms,assertions=[])
    files=[Path(__file__),D/'tests/native_table_create.py',D/'tests/permission_api.py',D/'native_test_context.py',
        D/'native_runtime.py',D/'tests/inline_cleanup.py',D/'tests/file_dml_acceptance.py',D/'inline_fixture.py',
        D/'tests/native_auto_flush.py',D/'source-lock.json']
    r['inputs']={str(f):sha256(f)for f in files};archive=out/'executed-inputs';archive.mkdir()
    for f in files:shutil.copy2(f,archive/f.name)
    duckdb=ROOT/'.reference/opengauss-native-lake/build/duckdb-gcc10/duckdb'
    provider=ROOT/'.install/opengauss-native-lake-debug/lib/postgresql/og_lake_sdk/libog_lake_sdk_arrow.so'
    r.update(provider_sha256=sha256(provider),reference_binary_sha256=sha256(duckdb));control=db=roles=t=None
    def image(v):return dict(rows=len(v),sha256=hashlib.sha256(repr(v).encode()).hexdigest())if isinstance(v,list)and len(v)>30 else v
    def check(name,got,want=True):
        good=got==want;r['assertions'].append(dict(name=name,observed=image(got),expected=image(want),status='pass'if good else'fail'))
        if not good:raise AssertionError(name+': '+repr(got))
    def literal(v):return "'"+str(v).replace("'","''")+"'"
    def independent(statement,label):
        (out/(label+'.sql')).write_text(statement+'\n')
        v=subprocess.run([str(duckdb),'-no-init','-batch','-json','-c','SET threads=1;'+statement],text=True,capture_output=True,timeout=90)
        (out/(label+'.stdout')).write_text(v.stdout);(out/(label+'.stderr')).write_text(v.stderr)
        if v.returncode:raise RuntimeError('Independent decode failed: '+label)
        return json.loads(v.stdout)if v.stdout.strip()else[]
    try:
        b=check_build(a.build_receipt);r.update(source_head=b['source_head'],binary_sha256=b['binary_sha256'])
        control=connect_control();check('automatic maintenance stays off',scalar(control,'SHOW enable_lake_maintenance'),'off')
        with(out/'fixture.log').open('w')as log:
            subprocess.run([sys.executable,str(D/'tests/catalog_fixture_bundle.py'),'--run-id',a.run_id,'--output',str(out/'fixture/manifest.json')],stdout=log,stderr=subprocess.STDOUT,check=True,timeout=300)
        db=OwnedDatabase(a.run_id,out).create();roles=OwnedRoles(control,a.run_id,out)
        t=Case(db,SimpleNamespace(mode='net_dml',sql_version='0.16',manifest=out/'fixture/manifest.json',output=out/'result.json',run_id=a.run_id),r,roles);c=t.a
        query(c,'SET statement_timeout='+str(a.build_timeout_ms))
        query(c,"CREATE FOREIGN TABLE biz.vectors(id integer,v vector(3),note text) SERVER created_lake OPTIONS(binding_mode 'create',lake_schema 'main')")
        query(c,"SELECT og_lake.set_option('biz.vectors'::regclass,'data_inlining_row_limit','50000')")
        if a.mode=='subnormal':
            if a.metric!='cosine':raise ValueError('This precision case requires the cosine opclass')
            query(c,"INSERT INTO biz.vectors VALUES(1,'[1e-45,0,0]'::vector,'subnormal'),(2,'[-1e-45,0,0]'::vector,'negative subnormal'),(3,'[1e-30,2e-30,-3e-30]'::vector,'tiny normal'),(4,NULL,'null'),(5,'[0,0,0]'::vector,'zero')")
            check('native double-precision norm recognizes all tiny nonzero vectors',scalar(c,'SELECT count(*) FROM biz.vectors WHERE v IS NOT NULL AND vector_norm(v)>0'),3)
        elif a.mode=='empty':
            query(c,"INSERT INTO biz.vectors VALUES(1,NULL,'null/1'),(2,NULL,'null/2'),(3,NULL,'null/3')")
            if a.metric=='cosine':query(c,"INSERT INTO biz.vectors VALUES(4,'[0,0,0]'::vector,'zero/1'),(5,'[0,0,0]'::vector,'zero/2')")
        else:
            query(c,"INSERT INTO biz.vectors SELECT i,('['||i::text||','||(i%11)::text||',0.25]')::vector,'row/'||i::text FROM generate_series(1,143)i")
            query(c,"INSERT INTO biz.vectors SELECT 1000+i,'[7,8,9]'::vector,'duplicate/'||i::text FROM generate_series(1,23)i")
            query(c,"INSERT INTO biz.vectors VALUES(2001,NULL,'null'),(2002,'[0,0,0]'::vector,'zero')")
            query(c,"UPDATE biz.vectors SET v='[70,80,90]'::vector WHERE id=1")
            query(c,"UPDATE biz.vectors SET v='[700,800,900]'::vector WHERE id=1")
            query(c,'DELETE FROM biz.vectors WHERE id=2')
        expected=query(c,"SELECT id,encode(vector_send(v),'hex'),note FROM biz.vectors ORDER BY id")
        query(c,"SELECT og_lake.flush('biz.vectors'::regclass)")
        spec=dict(version=1,kind='vector',method='diskann',column='v',metric=a.metric,enable_pq=False)
        uuid=str(scalar(c,"SELECT og_lake.create_lake_index('biz.vectors'::regclass,'ix_file_ann',%s::jsonb)",(json.dumps(spec),)))
        store=out/'index-store';store.mkdir();query(c,"SELECT og_lake.set_option('created_lake'::name,'index_root',%s::text)",(str(store),))
        head=t.head();protocol=t.protocol()
        def build(conn=c):return query(conn,'SELECT og_lake.build_lake_index(%s::uuid)',(uuid,))
        def segments(conn=t.observer):return query(conn,'SELECT segment_uuid::text,state,artifact_root_uri,manifest_hash,build_token::text FROM og_lake_internal.index_segment WHERE index_uuid=%s::uuid ORDER BY segment_uuid',(uuid,))
        def coverage(conn=t.observer):return query(conn,'SELECT segment_uuid::text,source_file_id,source_path,physical_row_count,source_checksum,schema_mapping_hash,active FROM og_lake_internal.index_coverage WHERE index_uuid=%s::uuid ORDER BY segment_uuid,source_file_id',(uuid,))
        def staging():return scalar(c,"SELECT count(*) FROM pg_class WHERE relnamespace=pg_my_temp_schema() AND relname LIKE '_og_lake_ann_stage_%'")
        if a.mode=='cancel':
            from concurrent.futures import ThreadPoolExecutor
            import time
            backend=scalar(c,'SELECT pg_backend_pid()')
            with ThreadPoolExecutor(max_workers=1)as pool:
                future=pool.submit(build)
                deadline=time.monotonic()+10
                while not list(store.iterdir())and not future.done()and time.monotonic()<deadline:time.sleep(.01)
                check('cancel reaches a newly registered immutable build directory',bool(list(store.iterdir())))
                time.sleep(.2)
                check('native build is still executing when cancellation is sent',not future.done())
                check('real backend cancellation is accepted',scalar(t.observer,'SELECT pg_cancel_backend(%s)',(backend,)))
                try:future.result(timeout=25)
                except Exception as exc:check('real file index build cancellation SQLSTATE',getattr(exc,'pgcode',None),'57014')
                else:check('build raises cancellation',False)
            query(c,'ROLLBACK')
            check('cancelled graph and coverage never publish',(segments(),coverage()),([],[]))
            check('cancelled staging and physical inline registration roll back',(staging(),scalar(c,'SELECT inline_index_oid FROM og_lake_internal.index_definition WHERE index_uuid=%s::uuid',(uuid,))),(0,None))
            check('cancelled build leaves no finished or temporary Parquet artifacts',list(store.rglob('*.parquet')),[])
            check('same connection retains exact original business rows',query(c,"SELECT id,encode(vector_send(v),'hex'),note FROM biz.vectors ORDER BY id"),expected)
            query(c,'INSERT INTO biz.heap_marker VALUES(88)')
            check('same connection ordinary writes recover',query(c,'SELECT * FROM biz.heap_marker'),[(88,)])
            check('cancel allocates no lake snapshot or IDs',t.head(),head)
            check('cancel preserves complete standard protocol',t.protocol(),protocol)
        if a.mode=='transactions':
            for child in [False,True]:
                query(c,'BEGIN')
                if child:query(c,'SAVEPOINT ann_build')
                build();pending=segments(c);paths=[Path(s[2])/name for s in pending for name in ['ann_nodes.parquet','ann_lookup.parquet']]
                check('real candidate graph files finished before rollback',bool(paths)and all(x.is_file()for x in paths))
                check('other reader cannot see pending graph publication',(segments(),coverage()),([],[]))
                check('successful staging relations are explicitly deleted',staging(),0)
                if child:
                    query(c,'ROLLBACK TO ann_build');query(c,'RELEASE SAVEPOINT ann_build');query(c,'INSERT INTO biz.heap_marker VALUES(77)');query(c,'COMMIT')
                else:query(c,'ROLLBACK')
                check('rollback discards finished graph candidates',all(not x.exists()for x in paths))
                check('rollback restores graph and inline registration',(segments(),coverage(),scalar(c,'SELECT inline_index_oid FROM og_lake_internal.index_definition WHERE index_uuid=%s::uuid',(uuid,))),([],[],None))
            check('parent ordinary write survives child rollback',query(c,'SELECT * FROM biz.heap_marker'),[(77,)])
        build();ready=segments();covered=coverage();r.update(built_segments=ready,built_coverage=covered)
        check('all graph segments are READY',bool(ready)and all(s[1]=='READY'and len(s[3])==64 for s in ready))
        check('one active full coverage per physical source',len(covered),scalar(c,'SELECT count(*) FROM created_metadata.ducklake_data_file'))
        check('every coverage has immutable version and schema identity',all(x[6]and x[4].startswith('object-version-v1:')and len(x[5])==64 for x in covered))
        check('build leaves no staging relation',staging(),0)
        all_artifacts={};expected_addresses={}
        for number,cov in enumerate(covered):
            directory=Path(next(s[2]for s in ready if s[0]==cov[0]));nodes=directory/'ann_nodes.parquet';lookup=directory/'ann_lookup.parquet'
            source=independent('SELECT file_row_number AS pos,_ducklake_internal_row_id AS rid,_ducklake_internal_snapshot_id AS birth,v FROM read_parquet('+literal(cov[2])+',file_row_number=true) ORDER BY pos','source-'+str(number))
            rows=independent('SELECT node_id,flags,hex(vector_le) AS v,hex(neighbors_le) AS n,hex(postings_le) AS p FROM read_parquet('+literal(nodes)+') ORDER BY node_id','graph-'+str(number))
            check('source physical population includes historical versions',len(source),cov[3])
            check('portable node identifiers are dense',[x['node_id']for x in rows],list(range(len(rows))))
            wanted={(cov[1],x['pos'],x['rid'],x['birth'])for x in source if x['v']is not None and(a.metric!='cosine'or any(v!=0 for v in x['v']))}
            expected_addresses[cov[1]]=wanted
            observed=set();edges_ok=True;values_ok=True;entries=0;postings=0
            for node in rows:
                entries+=bool(node['flags']&1)
                v=struct.unpack('<3f',bytes.fromhex(node['v']));neighbors=bytes.fromhex(node['n']);posts=bytes.fromhex(node['p'])
                edges_ok=edges_ok and len(neighbors)%4==0 and all(x[0]<len(rows)for x in struct.iter_unpack('<I',neighbors))
                for address in struct.iter_unpack('<qqqq',posts):
                    observed.add(address);postings+=1
                    original=source[address[1]]['v'];values_ok=values_ok and address in wanted
                    if a.metric=='cosine':
                        norm=sum(float(x)*float(x)for x in original)**0.5;original=[x/norm for x in original]
                    values_ok=values_ok and all(abs(x-y)<=2e-6*max(1,abs(y))for x,y in zip(v,original))
            check('export restores every original file/position/row/version address',sorted(observed),sorted(wanted))
            check('native topology remains inside the segment',edges_ok)
            check('native stored vectors preserve metric representation',values_ok)
            check('native graph entry presence matches its indexable source',entries>0,bool(wanted))
            page=independent('SELECT coalesce(sum(row_count),0)::BIGINT AS nodes,coalesce(sum(entry_count),0)::BIGINT AS entries,coalesce(sum(posting_count),0)::BIGINT AS postings FROM read_parquet('+literal(lookup)+')','directory-'+str(number))[0]
            check('independent directory populations match graph',page,dict(nodes=len(rows),entries=entries,postings=postings))
            all_artifacts.update({str(nodes):sha256(nodes),str(lookup):sha256(lookup),str(Path(cov[2])):sha256(cov[2])})
        r['artifacts']=all_artifacts
        build();check('verified repeated build preserves exact graph metadata',(segments(),coverage()),(ready,covered))
        check('derived graph allocates no logical snapshot or row IDs',t.head(),head)
        check('complete standard protocol unchanged by graph build',t.protocol(),protocol)
        check('original business values remain exact',query(c,"SELECT id,encode(vector_send(v),'hex'),note FROM biz.vectors ORDER BY id"),expected)
        if a.mode=='repair':
            old=covered[0];directory=Path(next(s[2]for s in ready if s[0]==old[0]));bad=directory/'ann_lookup.parquet'
            shutil.copy2(bad,out/'missing-lookup-original.parquet');bad.unlink()
            # Preserve evidence of deliberate damage without falsely claiming
            # that the deliberately removed object still matches its digest.
            r['artifacts'].pop(str(bad));r['removed_artifacts']={str(bad):sha256(out/'missing-lookup-original.parquet')}
            build();new=next(x for x in coverage()if x[1]==old[1]and x[6])
            check('missing graph directory repaired with new immutable segment',new[0]!=old[0])
            check('repair retains exact original source identity',new[1:6],old[1:6])
            check('damaged predecessor becomes RETIRED',next(x[1]for x in segments()if x[0]==old[0]),'RETIRED')
            check('repair never overwrites missing old object',bad.exists(),False)
            stable=(segments(),coverage());build();check('repaired graph reuse is idempotent',(segments(),coverage()),stable)
            for fresh in [x for x in coverage()if x[6]]:
                fresh_dir=Path(next(x[2]for x in segments()if x[0]==fresh[0]))
                decoded=independent('SELECT hex(postings_le) AS p FROM read_parquet('+literal(fresh_dir/'ann_nodes.parquet')+')','repaired-postings-'+str(fresh[1]))
                addresses={address for node in decoded for address in struct.iter_unpack('<qqqq',bytes.fromhex(node['p']))}
                check('repaired graph independently preserves all original version addresses',sorted(addresses),sorted(expected_addresses[fresh[1]]))
                for stem in ['ann_nodes.parquet','ann_lookup.parquet']:r['artifacts'][str(fresh_dir/stem)]=sha256(fresh_dir/stem)
        check('provider bytes remain fixed',sha256(provider),r['provider_sha256'])
        check('all executed inputs remain fixed',all(sha256(f)==h for f,h in r['inputs'].items()))
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
