#!/usr/bin/env python3
"""Actual native shared DiskANN frontier over genuine Parquet graph pages.

Graph topology is a test chain, produced by a diagnostic using the native
format writer and cosine normalizer. This is no native graph-build/export,
lake FDW query, visibility/coverage publication, recall or PQ qualification.
"""
import argparse
import json
from pathlib import Path
import shutil
import sys
import traceback
import uuid

ROOT=Path('/home/qiumc/data/code/cpp');D=ROOT/'integration/opengauss-native-lake';sys.path[:0]=[str(D),str(D/'tests')]
from file_dml_acceptance import check_build
from inline_fixture import query,scalar
from lake_harness import sha256,validate_run_id,write_json
from native_auto_flush import connect_control
from native_test_context import OwnedDatabase
from permission_api import OwnedRoles,cleanup_roles

def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--run-id',required=True)
    p.add_argument('--build-receipt',type=Path,required=True);p.add_argument('--probe-build',type=Path,required=True)
    a=p.parse_args();validate_run_id(a.run_id);out=ROOT/'公共文档/openGauss原生湖存储/验证'/a.run_id;out.mkdir(exist_ok=False)
    r=dict(status='fail',scope=__doc__,assertions=[],graphs=[]);control=db=roles=None;module_prefix=None
    inputs=[Path(__file__),D/'tests/native_file_ann_probe.cpp',D/'tests/build_native_file_ann_probe.py',D/'tests/file_dml_acceptance.py',
            D/'tests/native_probe_abi.py',D/'native_test_context.py',D/'native_runtime.py',D/'tests/inline_cleanup.py',D/'inline_fixture.py',D/'tests/native_auto_flush.py',D/'tests/permission_api.py',D/'source-lock.json']
    r['inputs']={str(f):sha256(f)for f in inputs};archive=out/'executed-inputs';archive.mkdir()
    for f in inputs:shutil.copy2(f,archive/f.name)
    def check(name,got,want=True):
        good=got==want;r['assertions'].append(dict(name=name,observed=got,expected=want,status='pass'if good else'fail'))
        if not good:raise AssertionError(name)
    def reject(conn,name,statement,code,values=None):
        try:query(conn,statement,values)
        except Exception as exc:check(name,exc.pgcode,code)
        else:check(name,False)
    try:
        b=check_build(a.build_receipt);r.update(source_head=b['source_head'],binary_sha256=b['binary_sha256'])
        pb=json.loads(a.probe_build.read_text());check('diagnostic compiled with actual current native identity',pb['status']=='built_unloaded'and pb['source_head']==b['source_head']and pb['native_binary_sha256']==b['binary_sha256'])
        check('exact importing diagnostic module',sha256(pb['library']),pb['library_sha256'])
        check('module layout is proven against actual native DWARF',pb.get('abi_layout',{}).get('status'),'pass')
        check('exact ABI gate used',sha256(D/'tests/native_probe_abi.py'),pb['abi_gate_sha256'])
        check('genuine isolated BINARY provider intact',sha256(pb['provider']),pb['provider_sha256']);r['provider_sha256']=pb['provider_sha256']
        check('probe uses exact source',sha256(D/'tests/native_file_ann_probe.cpp'),pb['source_sha256'])
        installed=ROOT/'.install/opengauss-native-lake-debug/lib/postgresql/og_lake_sdk/libog_lake_sdk_arrow.so';r['installed_provider_sha256']=sha256(installed)
        r.update(probe_build=str(a.probe_build.resolve()),probe_build_sha256=sha256(a.probe_build))
        control=connect_control();query(control,'SET session_timeout=0')
        db=OwnedDatabase(a.run_id,out).create();roles=OwnedRoles(control,a.run_id,out);c=db.connect()
        query(db.admin,'SET session_timeout=0');query(c,'SET statement_timeout=600000')
        from psycopg2 import sql
        module_prefix=ROOT/'.install/opengauss-native-lake-debug/lib/postgresql'/('lake_file_ann_probe_'+uuid.uuid4().hex)
        module_prefix.mkdir(mode=0o750,exist_ok=False);module=module_prefix/'native_file_ann_probe.so';shutil.copy2(pb['library'],module)
        owner=dict(run_id=a.run_id,path=str(module_prefix),library_sha256=sha256(module),sql_module_path='$libdir/'+module_prefix.name+'/native_file_ann_probe')
        write_json(module_prefix/'owner.json',owner);write_json(out/'probe-installation.json',owner)
        for name,signature,result in [('lake_test_ann_graph_make','text,integer,integer,boolean','text'),
                ('lake_test_ann_graph_search','text,text,integer,integer,boolean,vector,integer,boolean','text')]:
            query(c,sql.SQL('CREATE FUNCTION public.{}('+signature+') RETURNS '+result+' AS {},{} LANGUAGE C STRICT NOT FENCED').format(sql.Identifier(name),sql.Literal(owner['sql_module_path']),sql.Literal(name)))
        query(c,sql.SQL('CREATE FUNCTION public.lake_test_ann_graph_masked_search(text,text,integer,integer,boolean,vector,integer,boolean,integer) RETURNS text AS {},{} LANGUAGE C STRICT NOT FENCED').format(sql.Literal(owner['sql_module_path']),sql.Literal('lake_test_ann_graph_search')))
        query(c,'CREATE TABLE public.exact(id integer,v vector(3))')
        query(c,"INSERT INTO public.exact SELECT i,('['||(i+.25)::text||','||((i*i%19)+.5)::text||','||((i%13)+.125)::text||']')::vector FROM generate_series(0,142)i")
        call='SELECT public.lake_test_ann_graph_search(%s,%s,%s,%s,%s,%s::vector,%s,%s)'
        graphroot=Path(pb['artifacts']);query_vectors=['[5,3,7]','[120,15,4]','[-2,8,-7]']
        def create(metric,count=143,bridge=False):
            path=graphroot/f'm{metric}_n{count}_b{int(bridge)}';manifest=scalar(c,'SELECT public.lake_test_ann_graph_make(%s,%s,%s,%s)',(str(path),metric,count,bridge))
            graph=dict(path=str(path),manifest=manifest,metric=metric,nodes=count,bridge=bridge);r['graphs'].append(graph);return graph
        def search(graph,q,beam=300,forced_cancel=False,conn=c):
            return json.loads(scalar(conn,call,(graph['path'],graph['manifest'],graph['metric'],graph['nodes'],graph['bridge'],q,beam,forced_cancel)))
        def compare(graph,q,label,beam=300):
            got=search(graph,q,beam);ids=[row[0]for row in got]
            eligible=set(range(graph['nodes']))-({1}if graph['bridge']else set())
            check(label+' unique candidates',len(ids),len(set(ids)))
            check(label+' valid native addresses',all(row[0]in eligible and row[1:5]==[0,row[0],100000+row[0],17]for row in got))
            expression='vector_l2_squared_distance(v,%s::vector)'if graph['metric']==1 else'vector_negative_inner_product(v,%s::vector)'if graph['metric']==2 else'vector_negative_inner_product(l2_normalize(v),l2_normalize(%s::vector))'
            expected=dict(query(c,'SELECT id,'+expression+' FROM public.exact',(q,)))
            check(label+' exact native internal score',all(float(row[5])==expected[row[0]]for row in got))
            if beam>=graph['nodes']:check(label+' complete reachable population',set(ids)==eligible)
            else:check(label+' bounded nonempty approximate beam',0<len(ids)<=beam)
            return got
        for metric in [1,2,3]:
            graph=create(metric)
            masked='SELECT public.lake_test_ann_graph_masked_search(%s,%s,%s,%s,%s,%s::vector,%s,%s,%s)'
            args=(graph['path'],graph['manifest'],metric,143,False,'[140.25,12.5,10.125]',3,False)
            selected=json.loads(scalar(c,masked,(*args,140)))
            check(f'metric{metric} scalar-rejected entry and139 bridges remain navigable',set(row[0]for row in selected)=={140,141,142})
            check(f'metric{metric} mask admission preserves original physical addresses',all(row[1:5]==[0,row[0],100000+row[0],17]for row in selected))
            check(f'metric{metric} empty scalar mask invents no row',json.loads(scalar(c,masked,(*args,143))),[])
            for i,q in enumerate(query_vectors):compare(graph,q,f'metric{metric} query{i}')
            compare(graph,query_vectors[0],f'metric{metric} narrow',16)
            values=(graph['path'],graph['manifest'],metric,143,False,'[1,2]',300,False)
            reject(c,f'metric{metric} wrong query dimension',call,'22000',values)
            reject(c,f'metric{metric} callback cancellation',call,'57014',(*values[:5],query_vectors[0],300,True))
            compare(graph,query_vectors[0],f'metric{metric} same connection after errors')
            if metric==3:reject(c,'zero cosine query requires full SQL route',call,'22023',(*values[:5],'[0,0,0]',300,False))
            else:compare(graph,'[0,0,0]',f'metric{metric} zero query')
            bridge=create(metric,bridge=True);compare(bridge,query_vectors[1],f'metric{metric} deleted bridge remains navigable')
        empty=create(1,0);check('empty immutable graph produces no invented row',search(empty,query_vectors[0]),[])
        budget=create(1,40000)
        reject(c,'sparse navigation budget is signalled for complete source fallback',masked,'54000',
            (budget['path'],budget['manifest'],1,40000,False,query_vectors[0],1,False,39999))
        graph=r['graphs'][0]
        compare(graph,query_vectors[0],'same session after sparse navigation budget')
        for beam in [0,1001,-1]:reject(c,'invalid beam '+str(beam),call,'22023',(graph['path'],graph['manifest'],1,143,False,query_vectors[0],beam,False))
        reject(c,'wrong source generation hash',call,'HV000',(graph['path'],'0'*64,1,143,False,query_vectors[0],300,False))
        role=roles.create('reader');ordinary,_=roles.connect(db,'reader')
        reject(ordinary,'ordinary role cannot invoke diagnostic filesystem access',call,'42501',(graph['path'],graph['manifest'],1,143,False,query_vectors[0],300,False))
        check('no extension or control catalog was installed by test',scalar(c,"SELECT count(*) FROM pg_extension WHERE extname='og_lake'"),0)
        check('no synthetic graph is a business table',scalar(c,'SELECT count(*) FROM public.exact'),143)
        check('all executed inputs unchanged',all(sha256(f)==h for f,h in r['inputs'].items()))
        check('native installed provider untouched',sha256(installed),r['installed_provider_sha256'])
        check_build(a.build_receipt);r.update(status='pass',artifacts={str(f):sha256(f)for f in graphroot.rglob('*.parquet')})
    except BaseException as exc:r.update(reason=str(exc),traceback=traceback.format_exc())
    finally:
        if db is not None:
            try:db.cleanup();r['database_cleanup']=db.cleanup_result
            except BaseException as exc:r.update(status='fail',cleanup_error=str(exc))
        if roles is not None:
            try:
                if not r.get('database_cleanup')or r['database_cleanup']['returncode']:raise RuntimeError('Owned DB cleanup unproven; roles retained')
                r['role_cleanup']=cleanup_roles(control,roles.path,a.run_id)
            except BaseException as exc:r.update(status='fail',role_cleanup_error=str(exc))
            r=roles.redact(r)
        if module_prefix is not None:
            try:
                if not r.get('database_cleanup')or r['database_cleanup']['returncode']:raise RuntimeError('Owned DB cleanup unproven; diagnostic module retained')
                owned=json.loads((module_prefix/'owner.json').read_text());assert owned['run_id']==a.run_id and owned['path']==str(module_prefix)
                module=module_prefix/'native_file_ann_probe.so';assert sha256(module)==owned['library_sha256']
                assert {p.name for p in module_prefix.iterdir()}=={'owner.json','native_file_ann_probe.so'}
                module.unlink();(module_prefix/'owner.json').unlink();module_prefix.rmdir();r['module_cleanup']=dict(status='pass',path=str(module_prefix))
            except BaseException as exc:r.update(status='fail',module_cleanup_error=str(exc))
        if control is not None:control.close()
        write_json(out/'result.json',r)
    print(json.dumps(dict(status=r['status'],checks=len(r['assertions']),reason=r.get('reason'),result=str(out/'result.json')),ensure_ascii=False))
    return 0 if r['status']=='pass'else 1

if __name__=='__main__':raise SystemExit(main())
