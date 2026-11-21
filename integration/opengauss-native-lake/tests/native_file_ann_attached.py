#!/usr/bin/env python3
"""Real DuckLake-created file-only ANN, then native inline after-image transition.

No native direct-file INSERT is assumed. Catalog/file identities come from the
locked reference and are transported unchanged. Small correctness or explicitly
bounded recall samples are not general recall, performance, PQ or RAC proof.
"""
import argparse
from collections import Counter
import fcntl
import hashlib
import json
from pathlib import Path
import re
import shutil
import sys
import traceback
ROOT=Path(__file__).resolve().parents[3];D=ROOT/'integration/opengauss-native-lake'
sys.path[:0]=[str(D),str(D/'tests')]
from file_dml_acceptance import check_build
from inline_fixture import query,scalar,import_catalog
from lake_harness import sha256,validate_run_id,write_json
from native_auto_flush import connect_control
from native_runtime import PROFILE
from native_test_context import OwnedDatabase
from permission_api import OwnedRoles,cleanup_roles,adopt_private_owner
from reverse_reference import Reference,literal
from empty_reference_fixture import export_actual_tables

def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--run-id',required=True);p.add_argument('--build-receipt',type=Path,required=True)
    p.add_argument('--fusion-ann',action='store_true');p.add_argument('--metric',choices=['l2','ip','cosine'],default='l2');p.add_argument('--rows',type=int,choices=[64,2048],default=64);p.add_argument('--build-timeout-ms',type=int,choices=[180000,600000],default=180000)
    a=p.parse_args();validate_run_id(a.run_id)
    if PROFILE!='ann-debug':p.error('Requires separately owned matching ann-debug catalogs')
    out=ROOT/'公共文档/openGauss原生湖存储/验证'/a.run_id;out.mkdir(exist_ok=False)
    r=dict(status='fail',scope=__doc__,metric=a.metric,rows=a.rows,fusion_ann=a.fusion_ann,statement_timeout_ms=a.build_timeout_ms,assertions=[]);control=db=roles=mutex=None
    files=[Path(__file__),D/'tests/file_dml_acceptance.py',D/'inline_fixture.py',D/'tests/native_auto_flush.py',D/'native_runtime.py',D/'native_test_context.py',D/'tests/permission_api.py',D/'tests/inline_cleanup.py',D/'tests/reverse_reference.py',D/'tests/empty_reference_fixture.py',D/'source-lock.json']
    r['inputs']={str(f):sha256(f)for f in files};archive=out/'executed-inputs';archive.mkdir()
    for f in files:shutil.copy2(f,archive/f.name)
    provider=ROOT/'.install/opengauss-native-lake-debug/lib/postgresql/og_lake_sdk/libog_lake_sdk_arrow.so';r['provider_sha256']=sha256(provider)
    def image(v):return dict(rows=len(v),sha256=hashlib.sha256(repr(v).encode()).hexdigest())if isinstance(v,list)and len(v)>30 else v
    def check(name,got,want=True):
        ok=got==want;r['assertions'].append(dict(name=name,observed=image(got),expected=image(want),status='pass'if ok else'fail'))
        if not ok:raise AssertionError(name+': '+repr(got))
    try:
        b=check_build(a.build_receipt);r.update(source_head=b['source_head'],binary_sha256=b['binary_sha256'])
        lock=json.loads((D/'source-lock.json').read_text());receipt=json.loads((ROOT/'.reference/opengauss-native-lake/build/receipt.json').read_text())
        check('exact reference source-lock',receipt['source_lock_hash'],sha256(D/'source-lock.json'))
        mutex=(ROOT/'.reference/opengauss-native-lake/.driver.lock').open('a+');fcntl.flock(mutex,fcntl.LOCK_EX|fcntl.LOCK_NB)
        refout=out/'reference';refout.mkdir();ref=Reference(refout,lock,receipt);r['reference_binary_sha256']=sha256(ref.binary)
        check('exact reference binary',sha256(ref.binary),receipt['binary_sha256'])
        catalog=out/'genuine.ducklake';data=out/'data';data.mkdir()
        attach='ATTACH '+literal('ducklake:'+str(catalog))+" AS lake (DATA_PATH "+literal(str(data))+",DUCKLAKE_VERSION '1.1',AUTOMATIC_MIGRATION FALSE,DATA_INLINING_ROW_LIMIT 0);"
        ref.run(attach+'CREATE TABLE lake.main.vectors(id INTEGER,v FLOAT[],note VARCHAR);','create')
        ranges=[(1,32),(33,64)]if a.rows==64 else[(1,2048)]
        for first,last in ranges:
            ref.run(ref.attach(catalog)+"CALL lake.set_option('data_inlining_row_limit',0); INSERT INTO lake.main.vectors SELECT i::INTEGER,[i::FLOAT,(i*i/100.0)::FLOAT,(i%13+.125)::FLOAT],'row/'||i::VARCHAR FROM range("+str(first)+','+str(last+1)+')t(i);','insert-'+str(first))
        expected=ref.logical(catalog,'lake.main.vectors','expected')
        exported=dict(scope='actual_reference_catalog_export_not_host_compatibility',tables=export_actual_tables(ref,catalog));write_json(out/'genuine-export.json',exported)
        byname={t['table_name']:t for t in exported['tables']};table=next(x for x in byname['ducklake_table']['rows']if x['table_name']=='vectors')
        check('genuine initial catalog has no inline data registry',byname['ducklake_inlined_data_tables']['rows'],[])
        check('genuine initial source files',len(byname['ducklake_data_file']['rows']),len(ranges))
        control=connect_control();query(control,'SET session_timeout=0');check('long-build controller idle timeout disabled',scalar(control,"SELECT setting::integer FROM pg_settings WHERE name='session_timeout'"),0);check('automatic maintenance remains off',scalar(control,'SHOW enable_lake_maintenance'),'off')
        db=OwnedDatabase(a.run_id,out).create();query(db.admin,'SET session_timeout=0');roles=OwnedRoles(control,a.run_id,out);c=db.connect()
        query(c,"CREATE EXTENSION og_lake VERSION '0.16'");query(c,'BEGIN');import_catalog(c,'lake_metadata',exported);query(c,'COMMIT')
        owner=roles.create('private',False);query(c,'BEGIN');adopt_private_owner(c,exported,owner);query(c,'COMMIT')
        query(c,'CREATE SCHEMA biz');query(c,"SELECT og_lake.attach_catalog('lake_reference','lake_metadata',NULL::text,false)")
        query(c,"CREATE FOREIGN TABLE biz.vectors(id integer,v vector(3),note text) SERVER lake_reference OPTIONS(binding_mode 'attach',table_id %s)",(str(table['table_id']),))
        query(c,'CREATE TABLE biz.exact(id integer,v vector(3),note text)')
        with c.cursor()as cur:cur.executemany('INSERT INTO biz.exact VALUES(%s,%s::vector,%s)',[(x['id'],'['+','.join(map(str,x['v']))+']',x['note'])for x in expected])
        query(c,"SELECT og_lake.set_option('biz.vectors'::regclass,'data_inlining_row_limit','50000')")
        check('positive after-image policy does not manufacture an inline carrier',scalar(c,'SELECT count(*) FROM lake_metadata.ducklake_inlined_data_tables'),0)
        store=out/'index-store';store.mkdir();query(c,"SELECT og_lake.set_option('lake_reference'::name,'index_root',%s::text)",(str(store),))
        spec=dict(version=1,kind='vector',method='diskann',column='v',metric=a.metric,enable_pq=False)
        uuid=str(scalar(c,"SELECT og_lake.create_lake_index('biz.vectors'::regclass,'attached_ix',%s::jsonb)",(json.dumps(spec),)))
        query(c,'SET statement_timeout='+str(a.build_timeout_ms));before=scalar(c,'SELECT max(snapshot_id) FROM lake_metadata.ducklake_snapshot')
        query(c,'SELECT og_lake.build_lake_index(%s::uuid)',(uuid,))
        check('build leaves business snapshot unchanged',scalar(c,'SELECT max(snapshot_id) FROM lake_metadata.ducklake_snapshot'),before)
        check('file-only build leaves native inline index absent',scalar(c,'SELECT inline_index_oid IS NULL FROM og_lake_internal.index_definition WHERE index_uuid=%s::uuid',(uuid,)))
        query(c,'SET enable_lake_ann=on');query(c,'SET enable_seqscan=off')
        operator={'l2':'<->','ip':'<#>','cosine':'<=>'}[a.metric];where='v IS NOT NULL'+(' AND vector_norm(v)>0'if a.metric=='cosine'else'')
        if a.fusion_ann:where+=' AND id>='+str(a.rows//2)
        def statement(table='biz.vectors'):
            return 'SELECT id,encode(vector_send(v),\'hex\'),note,v '+operator+' %s::vector AS d FROM '+table+' WHERE '+where+' ORDER BY d LIMIT 10'
        def equivalent(label,value='[5,3,7]'):
            got=query(c,statement(),(value,));want=query(c,statement('biz.exact'),(value,))
            if a.rows==64:check(label+' exact small-case distances',[x[3]for x in got],[x[3]for x in want])
            else:
                recall=len({x[0]for x in got}&{x[0]for x in want})/len(want);r.setdefault('bounded_recall_samples',[]).append(dict(label=label,recall=recall,returned=[x[0]for x in got],exact=[x[0]for x in want]));check(label+' bounded deterministic recall >=0.9',recall>=.9)
            for x in got:check(label+' original tuple and native SQL distance '+str(x[0]),query(c,'SELECT id,encode(vector_send(v),\'hex\'),note,v '+operator+' %s::vector FROM biz.exact WHERE id=%s',(value,x[0])),[x])
        def plan(label,expected_count):
            text='\n'.join(x[0]for x in query(c,'EXPLAIN (ANALYZE,COSTS OFF) '+statement(),('[5,3,7]',)));(out/(label+'-plan.txt')).write_text(text+'\n')
            match=re.search(r'Verified ANN File Coverage: (\d+)',text);check(label+' actual verified file coverage',int(match.group(1))if match else-1,expected_count);return text
        equivalent('file-only initial');text=plan('initial',len(ranges));check('original file-only scan has no inline data input','Inline Inputs: 0'in text)
        if a.fusion_ann:check('actual scalar-constrained ANN path','ANN Scalar Filter:'in text)
        if a.rows==2048:
            match=re.search(r'Foreign Scan[^\n]*rows=(\d+)',text);check('bounded ANN actually prunes physical population',bool(match)and int(match.group(1))<a.rows)
            for value in ['[19,6,2]','[40,20,10]','[500,2500,6]','[1100,12100,9]']:equivalent('bounded query '+value,value)
        query(c,'BEGIN');query(c,"UPDATE biz.vectors SET v='[5,3,7.25]'::vector,note='rolled back' WHERE id=1");query(c,'ROLLBACK')
        equivalent('private rollback');check('rollback does not create committed carrier',scalar(c,'SELECT count(*) FROM lake_metadata.ducklake_inlined_data_tables'),0)
        for label,value in [('first','[5,3,7.5]'),('second','[5,3,7.75]')]:
            query(c,'BEGIN')
            for target in ['biz.vectors','biz.exact']:query(c,'UPDATE '+target+' SET v=%s::vector,note=%s WHERE id=1',(value,label))
            query(c,'COMMIT');equivalent(label+' native after-image')
        check('native after-images create one registered inline carrier',scalar(c,'SELECT count(*) FROM lake_metadata.ducklake_inlined_data_tables'),1)
        plan('after-updates',max(0,len(ranges)-1));query(c,"SELECT og_lake.flush('biz.vectors'::regclass)");equivalent('native flush with uncovered after-image source');plan('after-flush',max(0,len(ranges)-1))
        check('final visible row population is preserved',scalar(c,'SELECT count(*) FROM biz.vectors'),a.rows)
        check('frozen test input bytes unchanged',all(sha256(f)==h for f,h in r['inputs'].items()));check('provider identity unchanged',sha256(provider),r['provider_sha256'])
        r['artifacts']={str(f):sha256(f)for f in out.rglob('*.parquet')};check_build(a.build_receipt);r['status']='pass'
    except BaseException as exc:r.update(reason=str(exc),traceback=traceback.format_exc())
    finally:
        if mutex is not None:mutex.close()
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
