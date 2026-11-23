#!/usr/bin/env python3
"""Idempotent owned DB then exact OID+marker role cleanup; no session killing."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import traceback
from permission_api import WORKSPACE,DRIVER,cleanup_roles
sys.path.insert(0,str(DRIVER))
from inline_fixture import query
from lake_harness import validate_run_id,write_json


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-id',default=os.getenv('OG_LAKE_RUN_ID'))
    parser.add_argument('--attempt',default=os.getenv('OG_LAKE_EVIDENCE_DIR'))
    args=parser.parse_args()
    if not args.run_id or not args.attempt: parser.error('run-id and attempt required')
    validate_run_id(args.run_id); attempt=Path(args.attempt).resolve()
    result={'status':'fail','run_id':args.run_id,'attempt':str(attempt)}; conn=None
    try:
        completed=subprocess.run([sys.executable,str(DRIVER/'tests/inline_cleanup.py'),'--run-id',args.run_id,'--attempt',str(attempt)],capture_output=True,text=True,timeout=45)
        result['database_cleanup']={'returncode':completed.returncode,'stdout':completed.stdout,'stderr':completed.stderr}
        if completed.returncode: raise RuntimeError('Owned database cleanup failed; no role cleanup attempted')
        receipt=attempt/'permission-roles.json'
        if receipt.exists():
            import psycopg2
            conn=psycopg2.connect(host=str(WORKSPACE/'.runtime/opengauss-native-lake/debug/socket'),port=55560,
                                 dbname='postgres',user=os.getenv('USER','qiumc'),connect_timeout=10)
            conn.autocommit=True; query(conn,'SET statement_timeout=15000')
            result['role_cleanup']=cleanup_roles(conn,receipt,args.run_id)
        else: result['role_cleanup']={'status':'pass','reason':'No role allocation receipt'}
        result['status']='pass'
    except BaseException as exc: result.update(reason=str(exc),traceback=traceback.format_exc())
    finally:
        if conn is not None: conn.close()
        write_json(attempt/'permission-cleanup-latest.json',result)
        print(json.dumps(result,ensure_ascii=False))
    return 0 if result['status']=='pass' else 1


if __name__=='__main__': raise SystemExit(main())
