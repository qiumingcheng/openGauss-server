#!/usr/bin/env python3
"""Candidate floating key header versus real native IEEE comparison operators.

A test-only ABI-checked module transports raw IEEE bits and invokes the actual
candidate header. It does not implement any comparison oracle or qualify
product index/catalog/SDK integration.
"""
import argparse
import hashlib
import json
from pathlib import Path
import random
import shutil
import sys
import traceback
import uuid
ROOT=Path(__file__).resolve().parents[3];D=ROOT/'integration/opengauss-native-lake'
sys.path[:0]=[str(D),str(D/'tests')]
from file_dml_acceptance import check_build
from inline_fixture import query,scalar
from lake_harness import sha256,validate_run_id,write_json
from native_auto_flush import connect_control
from native_test_context import OwnedDatabase
from permission_api import OwnedRoles,cleanup_roles

def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--run-id',required=True);p.add_argument('--build-receipt',type=Path,required=True);p.add_argument('--probe-build',type=Path,required=True)
    a=p.parse_args();validate_run_id(a.run_id);out=ROOT/'公共文档/openGauss原生湖存储/验证'/a.run_id;out.mkdir(exist_ok=False)
    r=dict(status='fail',scope=__doc__,assertions=[]);control=db=roles=module_prefix=None;module_owner=None
    files=[Path(__file__),D/'tests/file_dml_acceptance.py',D/'inline_fixture.py',D/'tests/native_auto_flush.py',D/'native_test_context.py',D/'native_runtime.py',D/'tests/permission_api.py',D/'tests/inline_cleanup.py',D/'source-lock.json',a.probe_build.resolve()]
    r['inputs']={str(f):sha256(f)for f in files};archive=out/'executed-inputs';archive.mkdir()
    for f in files:shutil.copy2(f,archive/f.name)
    def image(v):return dict(rows=len(v),sha256=hashlib.sha256(repr(v).encode()).hexdigest())if isinstance(v,list)and len(v)>30 else v
    def check(name,got,want=True):
        ok=got==want;r['assertions'].append(dict(name=name,observed=image(got),expected=image(want),status='pass'if ok else'fail'))
        if not ok:raise AssertionError(name+': '+repr(got))
    try:
        b=check_build(a.build_receipt);r.update(source_head=b['source_head'],binary_sha256=b['binary_sha256'])
        pb=json.loads(a.probe_build.read_text());check('component built but never previously loaded',pb['status'],'built_unloaded');check('same native binary',pb['native_binary_sha256'],b['binary_sha256']);check('mandatory DWARF layout gate passed',pb['abi_layout']['status'],'pass')
        lib=Path(pb['library']);check('immutable component library identity',sha256(lib),pb['library_sha256']);header=a.probe_build.parent/'candidate_scalar_key.h';check('exact candidate header bytes',sha256(header),pb['candidate_header_sha256']);r['candidate_header_sha256']=sha256(header)
        prefix=ROOT/'.install/opengauss-native-lake-debug/lib/postgresql'
        module_prefix=prefix/('lake_key_probe_'+uuid.uuid4().hex);module_prefix.mkdir(mode=0o750,exist_ok=False)
        module=module_prefix/'native_scalar_key_probe.so';shutil.copy2(lib,module)
        module_owner=dict(run_id=a.run_id,path=str(module_prefix),library_sha256=sha256(module),sql_module_path='$libdir/'+module_prefix.name+'/native_scalar_key_probe')
        write_json(module_prefix/'owner.json',module_owner);write_json(out/'module-owner.json',module_owner)
        control=connect_control();db=OwnedDatabase(a.run_id,out).create();roles=OwnedRoles(control,a.run_id,out);c=db.connect();query(c,'SET statement_timeout=60000');query(c,'CREATE SCHEMA probe')
        for name,args,returns in [('float4_key','real','bigint'),('float8_key','double precision','bigint'),('float4_bits','integer','real'),('float8_bits','bigint','double precision')]:
            query(c,'CREATE FUNCTION probe.'+name+'('+args+') RETURNS '+returns+' AS %s,%s LANGUAGE C IMMUTABLE STRICT NOT FENCED',(module_owner['sql_module_path'],'lake_test_'+name))
            query(c,'REVOKE ALL ON FUNCTION probe.'+name+'('+args+') FROM PUBLIC')
        rng=random.Random(20261109)
        for width,typ in [(32,'real'),(64,'double precision')]:
            n='4'if width==32 else'8';sign=1<<(width-1)
            samples=([0,sign,1,sign+1,0x007fffff,0x00800000,0x7f7fffff,0xff7fffff,0x7f800000,0xff800000,0x7fc00000,0xffc00000,0x7f800001,0xffffffff]if width==32 else[0,sign,1,sign+1,0xfffffffffffff,0x10000000000000,0x7fefffffffffffff,0xffefffffffffffff,0x7ff0000000000000,0xfff0000000000000,0x7ff8000000000000,0xfff8000000000000,0x7ff0000000000001,0xffffffffffffffff])
            samples +=[rng.getrandbits(width)for _ in range(1024)];samples += samples[:14]
            query(c,'CREATE TABLE probe.t'+n+'(id integer,bits '+('integer'if width==32 else'bigint')+',v '+typ+')')
            signed=[x-(1<<width)if x>=sign else x for x in samples]
            with c.cursor()as cur:cur.executemany('INSERT INTO probe.t'+n+' VALUES(%s,%s,probe.float'+n+'_bits(%s))',[(i,x,x)for i,x in enumerate(signed)])
            check('float'+n+' full original native order with all IEEE classes',query(c,'SELECT id FROM probe.t'+n+' ORDER BY probe.float'+n+'_key(v),id'),query(c,'SELECT id FROM probe.t'+n+' ORDER BY v,id'))
            check('float'+n+' native equality classes include NaNs and both zeros',scalar(c,'SELECT count(DISTINCT probe.float'+n+'_key(v)) FROM probe.t'+n),scalar(c,'SELECT count(DISTINCT v) FROM probe.t'+n))
            for op in ['<','<=','=','>=','>']:
                sql='SELECT count(*) FROM probe.t'+n+' a CROSS JOIN probe.t'+n+' b WHERE a.id<128 AND b.id<128 AND ((a.v '+op+' b.v)<>(probe.float'+n+'_key(a.v) '+op+' probe.float'+n+'_key(b.v)))'
                check('float'+n+' native pairwise '+op,scalar(c,sql),0)
            check('float'+n+' encoded NaN is above positive infinity',scalar(c,"SELECT probe.float"+n+"_key('NaN'::"+typ+")>probe.float"+n+"_key('Infinity'::"+typ+')'))
            check('float'+n+' signed zero keys are equal',scalar(c,'SELECT probe.float'+n+'_key(probe.float'+n+'_bits(0))=probe.float'+n+'_key(probe.float'+n+'_bits(%s))',(-sign,)))
        check('float4 widening to float8 preserves every encoded key',scalar(c,'SELECT count(*) FROM probe.t4 WHERE probe.float4_key(v)<>probe.float8_key(v::double precision)'),0)
        for op in ['<','<=','=','>=','>']:
            check('native cross-float operand comparison '+op,scalar(c,'SELECT count(*) FROM probe.t4 a CROSS JOIN probe.t8 b WHERE a.id<128 AND b.id<128 AND ((a.v '+op+' b.v)<>(probe.float4_key(a.v) '+op+' probe.float8_key(b.v)))'),0)
        check('test input bytes unchanged',all(sha256(f)==h for f,h in r['inputs'].items()));check('probe library bytes unchanged',sha256(lib),pb['library_sha256']);check_build(a.build_receipt);r['status']='pass'
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
        if module_prefix is not None:
            try:
                if not r.get('database_cleanup')or r['database_cleanup']['returncode']:raise RuntimeError('Owned DB not removed; test module retained')
                assert json.loads((module_prefix/'owner.json').read_text())==module_owner
                assert {f.name for f in module_prefix.iterdir()}=={'owner.json','native_scalar_key_probe.so'}
                assert sha256(module_prefix/'native_scalar_key_probe.so')==module_owner['library_sha256']
                (module_prefix/'native_scalar_key_probe.so').unlink();(module_prefix/'owner.json').unlink();module_prefix.rmdir()
                r['module_cleanup']=dict(status='pass',exact_owned_path=str(module_prefix))
            except BaseException as exc:r.update(status='fail',module_cleanup_error=str(exc))
        if control is not None:control.close()
        write_json(out/'result.json',r)
    print(json.dumps(dict(status=r['status'],checks=len(r['assertions']),reason=r.get('reason'),result=str(out/'result.json')),ensure_ascii=False));return 0 if r['status']=='pass'else 1
if __name__=='__main__':raise SystemExit(main())
