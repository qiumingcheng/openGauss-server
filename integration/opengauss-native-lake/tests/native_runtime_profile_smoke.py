#!/usr/bin/env python3
"""Real ownership lifecycle plus wrong-profile build/cleanup rejection.

The negative test reuses an actual owned database receipt. It does not invent
lake protocol rows, terminate sessions or initialize the other profile.
"""
import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import traceback
ROOT=Path('/home/qiumc/data/code/cpp');D=ROOT/'integration/opengauss-native-lake'
sys.path[:0]=[str(D),str(D/'tests')]
from file_dml_acceptance import check_build
from inline_fixture import scalar
from lake_harness import sha256,validate_run_id,write_json
from native_runtime import PROFILE,RUNTIME,PORT
from native_test_context import OwnedDatabase


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--run-id',required=True)
    p.add_argument('--build-receipt',type=Path,required=True);a=p.parse_args();validate_run_id(a.run_id)
    out=ROOT/'公共文档/openGauss原生湖存储/验证'/a.run_id;out.mkdir(exist_ok=False)
    r={'status':'fail','scope':'Actual Debug owned database and explicit Release rejection boundaries','assertions':[]}
    paths=[Path(__file__),D/'native_runtime.py',D/'native_test_context.py',D/'build_driver.py',D/'lake_harness.py',D/'tests/inline_cleanup.py',D/'tests/file_dml_acceptance.py',D/'tests/permission_api.py']
    r['inputs']={str(p):sha256(p)for p in paths};archive=out/'executed-inputs';archive.mkdir()
    for p in paths:shutil.copy2(p,archive/p.name)
    db=None
    def check(name,got,want=True):
        ok=got==want;r['assertions'].append({'name':name,'observed':got,'expected':want,'status':'pass'if ok else'fail'})
        if not ok:raise AssertionError(name)
    try:
        b=check_build(a.build_receipt);r.update(source_head=b['source_head'],binary_sha256=b['binary_sha256'])
        check('positive fixture runs only in Debug',PROFILE,'debug');check('fixed Debug endpoint',PORT,55560)
        db=OwnedDatabase(a.run_id,out).create()
        check('allocation records exact runtime profile',db.allocation['runtime_profile'],'debug')
        check('allocation records dedicated data directory',db.allocation['runtime_data'],str(RUNTIME/'data'))
        env=os.environ.copy();env['OG_LAKE_TEST_PROFILE']='release';env['OG_LAKE_SOURCE']=str(ROOT/'.worktrees/opengauss-native-lake-release')
        process=subprocess.run([sys.executable,str(D/'tests/inline_cleanup.py'),'--run-id',a.run_id,'--attempt',str(out)],env=env,capture_output=True,text=True,timeout=30)
        (out/'wrong-profile-cleanup.stdout').write_text(process.stdout);(out/'wrong-profile-cleanup.stderr').write_text(process.stderr)
        check('Release cleanup rejects a genuine Debug ownership receipt',process.returncode,1)
        negative=json.loads(process.stdout.strip().splitlines()[-1]);r['negative_cleanup']=negative
        check('rejection specifically precedes other-runtime connection',negative['error'],"RuntimeError('Database allocation profile does not match selected runtime')")
        check('real allocated Debug database remains present',scalar(db.admin,'SELECT count(*) FROM pg_database WHERE oid=%s AND datname=%s',(db.allocation['database_oid'],db.database)),1)
        code="import sys,os;from pathlib import Path;sys.path[:0]=[sys.argv[1],sys.argv[1]+'/tests'];import psycopg2;from permission_api import OwnedRoles;from lake_harness import Blocked;c=psycopg2.connect(host=sys.argv[4],port=55560,dbname='postgres',user=os.getenv('USER','qiumc'));c.autocommit=True\ntry:\n OwnedRoles(c,sys.argv[2],Path(sys.argv[3]))\nexcept Blocked as exc:\n print(str(exc));c.close();raise SystemExit(0 if 'another runtime profile' in str(exc) else 2)\nc.close();raise SystemExit(1)"
        process=subprocess.run([sys.executable,'-c',code,str(D),a.run_id,str(out/'wrong-role-controller'),str(RUNTIME/'socket')],env=env,capture_output=True,text=True,timeout=30)
        (out/'wrong-profile-role.stdout').write_text(process.stdout);(out/'wrong-profile-role.stderr').write_text(process.stderr)
        check('Release role controller refuses an actual Debug SQL connection before writes',process.returncode,0)
        code="import sys;from pathlib import Path;sys.path[:0]=[sys.argv[1],sys.argv[1]+'/tests'];from file_dml_acceptance import check_build;from lake_harness import Blocked\ntry:\n check_build(Path(sys.argv[2]))\nexcept Blocked as exc:\n print(str(exc));raise SystemExit(0 if 'another runtime profile or endpoint' in str(exc) else 2)\nraise SystemExit(1)"
        process=subprocess.run([sys.executable,'-c',code,str(D),str(a.build_receipt.resolve())],env=env,capture_output=True,text=True,timeout=30)
        (out/'wrong-profile-build.stdout').write_text(process.stdout);(out/'wrong-profile-build.stderr').write_text(process.stderr)
        check('Release cannot accept a genuine Debug build receipt',process.returncode,0)
        code="import sys;sys.path[:0]=[sys.argv[1],sys.argv[1]+'/tests'];from lake_harness import Context,run_case;r,p=run_case('RUN-01',Context(sys.argv[2]));print(r['status'],r.get('reason'));raise SystemExit(0 if r['status']=='blocked' and 'Debug-only' in r.get('reason','') else 1)"
        process=subprocess.run([sys.executable,'-c',code,str(D),a.run_id+'_unsupported_release'],env=env,capture_output=True,text=True,timeout=30)
        (out/'unsupported-generic-release.stdout').write_text(process.stdout);(out/'unsupported-generic-release.stderr').write_text(process.stderr)
        check('unadapted generic cases fail before any Debug execution in Release mode',process.returncode,0)
        check('all executed infrastructure input bytes unchanged',all(sha256(p)==h for p,h in r['inputs'].items()))
        check_build(a.build_receipt);r['status']='pass'
    except BaseException as exc:r.update(reason=str(exc),traceback=traceback.format_exc())
    finally:
        if db is not None:
            try:db.cleanup();r['database_cleanup']=db.cleanup_result
            except BaseException as exc:r.update(status='fail',cleanup_error=str(exc))
        write_json(out/'result.json',r)
    print(json.dumps({'status':r['status'],'checks':len(r['assertions']),'reason':r.get('reason'),'result':str(out/'result.json')},ensure_ascii=False))
    return 0 if r['status']=='pass'else 1

if __name__=='__main__':raise SystemExit(main())
