#!/usr/bin/env python3
"""Actual RC publication conflicts: disjoint rows may merge; same origins must fail.

The native heap oracle receives the same successful business changes. Held RC
lake readers retain the declared old lake view through native publication.
"""
import argparse
import json
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

MODES=['inline_disjoint_update','inline_update_insert','inline_delete_insert','inline_zero_delete_insert',
       'inline_same_update','file_disjoint_update','file_update_insert','file_same_update','old_writer_flush',
       'legacy_inline_disjoint']


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--run-id',required=True)
    p.add_argument('--build-receipt',type=Path,required=True);p.add_argument('--mode',choices=MODES,required=True)
    a=p.parse_args();validate_run_id(a.run_id)
    out=ROOT/'公共文档/openGauss原生湖存储/验证'/a.run_id;out.mkdir(exist_ok=False)
    result={'status':'fail','scope':'Actual native RC row conflict '+a.mode,'assertions':[],
            'limitations':['Closed native catalog, SQL0.11 RC semantics; no external concurrent writer, target RAC or performance qualification.']}
    inputs=[Path(__file__),D/'tests/native_table_create.py',D/'tests/permission_api.py',D/'native_test_context.py',D/'source-lock.json']
    result['inputs']={str(x):sha256(x)for x in inputs};archive=out/'executed-inputs';archive.mkdir()
    for f in inputs:shutil.copy2(f,archive/f.name)
    db=roles=control=t=None
    def check(name,got,want=True):
        ok=got==want;result['assertions'].append({'name':name,'observed':got,'expected':want,'status':'pass'if ok else'fail'})
        if not ok:raise AssertionError(name)
    try:
        import psycopg2
        b=check_build(a.build_receipt);result.update(source_head=b['source_head'],binary_sha256=b['binary_sha256'])
        control=connect_control();check('automatic scheduling disabled for deterministic transaction order',scalar(control,'SHOW enable_lake_maintenance'),'off')
        with (out/'fixture.log').open('w')as log:
            subprocess.run([sys.executable,str(D/'tests/catalog_fixture_bundle.py'),'--run-id',a.run_id,
                            '--output',str(out/'fixture/manifest.json')],stdout=log,stderr=subprocess.STDOUT,check=True,timeout=300)
        db=OwnedDatabase(a.run_id,out).create();roles=OwnedRoles(control,a.run_id,out)
        version='0.10'if a.mode.startswith('legacy_')else'0.11'
        t=Case(db,SimpleNamespace(mode='net_dml',sql_version=version,manifest=out/'fixture/manifest.json',
                                 output=out/'result.json',run_id=a.run_id),result,roles)
        t.create();query(t.a,'CREATE TABLE biz.reference(id integer PRIMARY KEY,val integer)')
        query(t.a,'BEGIN');query(t.a,'INSERT INTO biz.orders VALUES(1,10),(2,20),(3,30)')
        query(t.a,'INSERT INTO biz.reference VALUES(1,10),(2,20),(3,30)');query(t.a,'COMMIT')
        if a.mode.startswith('file_'):query(t.a,"SELECT og_lake.flush('biz.orders'::regclass)")
        old=db.connect();query(old,'BEGIN ISOLATION LEVEL READ COMMITTED')
        original=[(1,10),(2,20),(3,30)]
        check('held native RC lake reader fixes original data',t.rows(conn=old),original)
        overlap='same_update'in a.mode
        reject=overlap or a.mode in ['old_writer_flush','legacy_inline_disjoint']
        query(t.a,'BEGIN');query(t.a,'INSERT INTO biz.heap_marker VALUES(77)')
        if 'zero_delete'in a.mode:
            query(t.a,'DELETE FROM biz.orders WHERE id=99');query(t.a,'DELETE FROM biz.reference WHERE id=99')
            query(t.a,'INSERT INTO biz.orders VALUES(5,50)');query(t.a,'INSERT INTO biz.reference VALUES(5,50)')
        elif 'delete_insert'in a.mode:
            query(t.a,'DELETE FROM biz.orders WHERE id=1');query(t.a,'DELETE FROM biz.reference WHERE id=1')
        else:
            query(t.a,'UPDATE biz.orders SET val=11 WHERE id=1')
            if not overlap:query(t.a,'UPDATE biz.reference SET val=11 WHERE id=1')
        if a.mode=='old_writer_flush':
            query(t.b,"SELECT og_lake.flush('biz.orders'::regclass)")
        else:
            query(t.b,'BEGIN')
            if overlap:
                query(t.b,'UPDATE biz.orders SET val=12 WHERE id=1');query(t.b,'UPDATE biz.reference SET val=12 WHERE id=1')
            elif 'zero_delete'in a.mode:
                query(t.b,'INSERT INTO biz.orders VALUES(99,990)');query(t.b,'INSERT INTO biz.reference VALUES(99,990)')
            elif 'insert'in a.mode:
                query(t.b,'INSERT INTO biz.orders VALUES(4,40)');query(t.b,'INSERT INTO biz.reference VALUES(4,40)')
            else:
                query(t.b,'UPDATE biz.orders SET val=22 WHERE id=2');query(t.b,'UPDATE biz.reference SET val=22 WHERE id=2')
            query(t.b,'COMMIT')
        outcome='committed'
        try:query(t.a,'COMMIT')
        except psycopg2.Error as exc:
            outcome=exc.pgcode;result['publication_error']=str(exc);query(t.a,'ROLLBACK')
        result['publication_outcome']=outcome
        # Capture complete committed evidence even when the old broad guard rejects.
        result['actual_lake_rows']=t.rows();result['actual_heap_rows']=query(t.observer,'SELECT id,val FROM biz.reference ORDER BY id')
        check('same-origin or retained legacy/flush conflict policy'if reject else'independent RC business changes both commit',
              outcome,'40001'if reject else'committed')
        check('complete lake business rows equal transactional heap oracle',result['actual_lake_rows'],result['actual_heap_rows'])
        check('ordinary heap marker follows the same commit outcome',query(t.observer,'SELECT * FROM biz.heap_marker'),[]if reject else[(77,)])
        check('old retained lake view preserved across both publications',t.rows(conn=old),original)
        query(old,'COMMIT')
        check('no duplicate application IDs',scalar(t.observer,'SELECT count(*)-count(DISTINCT id) FROM biz.orders'),0)
        if 'zero_delete'in a.mode:expected=[(1,10),(2,20),(3,30),(5,50),(99,990)]
        elif 'delete_insert'in a.mode:expected=[(2,20),(3,30),(4,40)]
        elif overlap:expected=[(1,12),(2,20),(3,30)]
        elif a.mode=='old_writer_flush':expected=original
        elif a.mode=='legacy_inline_disjoint':expected=[(1,10),(2,22),(3,30)]
        elif 'insert'in a.mode:expected=[(1,11),(2,20),(3,30),(4,40)]
        else:expected=[(1,11),(2,22),(3,30)]
        check('complete expected serially committed values',t.rows(),expected)
        next_id=scalar(t.observer,'SELECT next_row_id FROM created_metadata.ducklake_table_stats')
        check('row allocation counts only genuinely new identities',next_id,5 if'zero_delete'in a.mode else 4 if 'insert'in a.mode else 3)
        check('all executed input bytes unchanged',all(sha256(f)==v for f,v in result['inputs'].items()))
        check_build(a.build_receipt);result['status']='pass'
    except BaseException as exc:result.update(reason=str(exc),traceback=traceback.format_exc())
    finally:
        if db is not None:
            try:db.cleanup();result['database_cleanup']=db.cleanup_result
            except BaseException as exc:result.update(status='fail',cleanup_error=str(exc))
        if roles is not None:
            try:
                if not result.get('database_cleanup')or result['database_cleanup']['returncode']:
                    raise RuntimeError('Owned database cleanup unproven; role retained')
                result['role_cleanup']=cleanup_roles(control,roles.path,a.run_id);result=roles.redact(result)
            except BaseException as exc:result.update(status='fail',role_cleanup_error=str(exc))
        if control is not None:control.close()
        write_json(out/'result.json',result)
    print(json.dumps({'status':result['status'],'mode':a.mode,'checks':len(result['assertions']),
                      'outcome':result.get('publication_outcome'),'reason':result.get('reason'),'result':str(out/'result.json')},ensure_ascii=False))
    return 0 if result['status']=='pass'else 1

if __name__=='__main__':raise SystemExit(main())
