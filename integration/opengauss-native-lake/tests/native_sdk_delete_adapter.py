#!/usr/bin/env python3
"""Exercise the actual native optional SDK loader and deletion/error adapters.

Only test-owned copies are removed. This supplies no catalog-GC eligibility.
"""
import argparse
import json
import os
from pathlib import Path
import shutil
import sys
import traceback
import uuid

ROOT = Path('/home/qiumc/data/code/cpp')
DRIVER = ROOT / 'integration/opengauss-native-lake'
sys.path[:0] = [str(DRIVER), str(DRIVER / 'tests')]
from file_dml_acceptance import check_build
from inline_fixture import query, scalar
from lake_harness import sha256, validate_run_id, write_json
from native_test_context import OwnedDatabase


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--run-id', required=True)
    p.add_argument('--build-receipt', type=Path, required=True)
    p.add_argument('--probe-build', type=Path, required=True)
    p.add_argument('--input', type=Path, required=True)
    p.add_argument('--expect', choices=['unavailable', 'available'], required=True)
    p.add_argument('--provider-sha256', required=True)
    args = p.parse_args()
    validate_run_id(args.run_id)
    out = ROOT / '公共文档/openGauss原生湖存储/验证' / args.run_id
    out.mkdir(exist_ok=False)
    result = {'status':'fail', 'scope':'Actual native SDK optional deletion adapter: ' + args.expect,
              'assertions':[], 'limitations':['No lake eligibility, backup, physical catalog GC, remote or RAC qualification.']}
    files = [Path(__file__), DRIVER / 'native_test_context.py', DRIVER / 'inline_fixture.py',
             DRIVER / 'tests/native_sdk_delete_probe.cpp', args.input.resolve()]
    result['inputs'] = {str(f):sha256(f) for f in files}
    archive = out / 'executed-inputs'
    archive.mkdir()
    for f in files[:-1]: shutil.copy2(f, archive / f.name)
    db = None

    def check(name, observed, expected=True):
        ok = observed == expected
        result['assertions'].append({'name':name,'observed':observed,'expected':expected,'status':'pass' if ok else 'fail'})
        if not ok: raise AssertionError(name)

    def error(conn, statement, parameters, code):
        try: query(conn, statement, parameters)
        except Exception as exc:
            check('exact native SQLSTATE for rejected SDK operation', getattr(exc,'pgcode',None), code)
            result.setdefault('errors',[]).append(str(exc))
        else: raise AssertionError('Expected native SDK rejection')

    try:
        from psycopg2 import sql
        build = check_build(args.build_receipt)
        probe = json.loads(args.probe_build.read_text())
        result.update(source_head=build['source_head'],binary_sha256=build['binary_sha256'])
        check('probe built unloaded',probe['status'],'built_unloaded')
        check('probe matches actual native binary',probe['native_binary_sha256'],build['binary_sha256'])
        check('probe library unchanged',sha256(probe['library']),probe['library_sha256'])
        check('all native probe headers unchanged',all(sha256(f)==h for f,h in probe['headers'].items()))
        provider=ROOT/'.install/opengauss-native-lake-debug/lib/postgresql/og_lake_sdk/libog_lake_sdk_arrow.so'
        check('installed SDK is exact expected phase',sha256(provider),args.provider_sha256)
        prefix=ROOT/'.install/opengauss-native-lake-debug/lib/postgresql'/('lake_sdk_delete_probe_'+uuid.uuid4().hex)
        prefix.mkdir(mode=0o750,exist_ok=False)
        library=prefix/'native_sdk_delete_probe.so';shutil.copy2(probe['library'],library)
        owner={'owner':args.run_id,'path':str(prefix),'library_sha256':sha256(library),
               'sql_module_path':'$libdir/'+prefix.name+'/native_sdk_delete_probe'}
        write_json(prefix/'owner.json',owner);write_json(out/'probe-installation.json',owner)
        db=OwnedDatabase(args.run_id,out).create();conn=db.connect()
        query(conn,'CREATE SCHEMA sdk_probe')
        query(conn,sql.SQL('CREATE FUNCTION sdk_probe.remove(text,text) RETURNS bigint AS {},{} '
                           'LANGUAGE C STRICT VOLATILE NOT FENCED').format(
                               sql.Literal(owner['sql_module_path']),sql.Literal('lake_test_sdk_delete_probe')))
        query(conn,'REVOKE ALL ON FUNCTION sdk_probe.remove(text,text) FROM PUBLIC')
        owned=out/'owned-files';owned.mkdir()
        genuine=owned/'genuine.parquet';shutil.copy2(args.input,genuine);original=sha256(genuine)
        statement='SELECT sdk_probe.remove(%s,%s)'
        if args.expect=='unavailable':
            error(conn,statement,(str(owned),'genuine.parquet'),'0A000')
            check('older SDK rejects before any file removal',sha256(genuine),original)
            check('capability rejection explains its requirement','explicit durable deletion support' in result['errors'][-1])
        else:
            check('native adapter removes genuine owned Parquet',scalar(conn,statement,(str(owned),'genuine.parquet')),1)
            check('physical object really gone',genuine.exists(),False)
            check('native retry of missing immutable object succeeds',scalar(conn,statement,(str(owned),'genuine.parquet')),0)
            outside=out/'outside.parquet';shutil.copy2(args.input,outside)
            error(conn,statement,(str(owned),'../outside.parquet'),'HV000')
            check('rejected traversal preserves outside object',sha256(outside),original)
            alias=owned/'alias';alias.symlink_to(out,True)
            for i in range(8): error(conn,statement,(str(owned),'alias/outside.parquet'),'HV000')
            check('native repeated SDK errors preserve unrelated object',sha256(outside),original)
            rollback=owned/'rollback.parquet';shutil.copy2(args.input,rollback)
            query(conn,'BEGIN')
            check('physical deletion reports completion before SQL rollback',scalar(conn,statement,(str(owned),'rollback.parquet')),1)
            query(conn,'ROLLBACK')
            check('filesystem deletion has explicit nontransactional semantics',rollback.exists(),False)
            check('next transaction can idempotently complete a retained queue item',scalar(conn,statement,(str(owned),'rollback.parquet')),0)
        check('genuine source object never changed',sha256(args.input),result['inputs'][str(args.input.resolve())])
        check('executed test inputs unchanged',all(sha256(f)==h for f,h in result['inputs'].items()))
        check('installed SDK unchanged within this phase',sha256(provider),args.provider_sha256)
        check_build(args.build_receipt)
        result['status']='pass'
    except BaseException as exc: result.update(reason=str(exc),traceback=traceback.format_exc())
    finally:
        if db is not None:
            try: db.cleanup();result['database_cleanup']=db.cleanup_result
            except BaseException as exc: result.update(status='fail',cleanup_error=str(exc))
        write_json(out/'result.json',result)
    print(json.dumps({'status':result['status'],'checks':len(result['assertions']),'reason':result.get('reason')}))
    return 0 if result['status']=='pass' else 1


if __name__=='__main__': raise SystemExit(main())
