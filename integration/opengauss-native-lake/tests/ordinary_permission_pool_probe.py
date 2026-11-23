#!/usr/bin/env python3
"""Real ordinary LOGIN identities, native private writes and bounded pool reuse.

Requires installed SQL0.5 and exact reviewed runtime0.5/0.6 permission adapters.
No grants to internal diagnostics or private owner membership are made here.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import re
import sys
import threading
import traceback

DRIVER=Path(os.getenv('OG_LAKE_DRIVER_ROOT','/home/qiumc/data/code/cpp/integration/opengauss-native-lake'))
sys.path.insert(0,str(DRIVER)); sys.path.insert(0,str(DRIVER/'tests'))
sys.path.insert(0,str(Path(__file__).resolve().parent))
from permission_api import WORKSPACE,API_VERSION,OwnedRoles,cleanup_roles,reviewed_permission_runtime
from ordinary_permissions import Case
from inline_fixture import multiset,query,scalar
from lake_harness import Blocked,validate_run_id,write_json
from native_test_context import OwnedDatabase
from reverse_modify_reference import reverse_modify_reference


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--run-id',required=True); p.add_argument('--output',type=Path,required=True)
    p.add_argument('--manifest',type=Path,required=True)
    p.add_argument('--mode',choices=['dedicated','thread_pool'],required=True)
    p.add_argument('--rounds',type=int,default=16)
    args=p.parse_args(); validate_run_id(args.run_id)
    if not 1<=args.rounds<=48: p.error('rounds must be 1..48; no repeat-until-reuse loop')
    args.output=args.output.resolve(); args.output.parent.mkdir(parents=True,exist_ok=True)
    if args.output.exists(): p.error('result already exists; use a fresh attempt')
    result={'status':'fail','scope':'ACL/ordinary_login_pool_reuse','assertions':[],'artifacts':[],
        'mode':args.mode,'rounds':args.rounds,'identity_observations':[],
        'limits':['True ordinary LOGIN connections, no diagnostic EXECUTE or internal schema grants.',
            'Active transactions must retain their worker; active transaction migration is not expected.',
            'Cross-role worker reuse is observed, never manufactured or inferred from Python thread IDs.',
            'No private-state memory counter proof, RAC, SDK, or arbitrary security policy qualification.']}
    control=None; roles=None; db=None
    try:
        declared=re.findall(r"^default_version\s*=\s*'([^']+)'",(WORKSPACE/'.worktrees/opengauss-native-lake/contrib/og_lake/og_lake.control').read_text(),re.M)
        if declared!=[reviewed_permission_runtime()]: raise Blocked('Reviewed source runtime0.5/0.6 required with installed SQL0.5')
        import psycopg2
        from psycopg2 import sql
        control=psycopg2.connect(host=str(WORKSPACE/'.runtime/opengauss-native-lake/debug/socket'),port=55560,
            dbname='postgres',user=os.getenv('USER','qiumc'),connect_timeout=10)
        control.autocommit=True; query(control,'SET statement_timeout=15000')
        roles=OwnedRoles(control,args.run_id,args.output.parent)
        with OwnedDatabase(args.run_id,args.output.parent) as db:
            t=Case(db,args,result,roles)
            setting='on' if args.mode=='thread_pool' else 'off'
            t.check('actual requested host mode',scalar(t.admin,'SHOW enable_thread_pool'),setting)
            writer2=roles.create('writer2')
            for command in ('GRANT USAGE ON SCHEMA biz TO {}',
                            'GRANT USAGE ON FOREIGN SERVER lake_reference TO {}',
                            'GRANT SELECT,INSERT,UPDATE,DELETE ON biz.orders TO {}',
                            'GRANT SELECT,INSERT ON biz.heap_marker TO {}'):
                query(t.admin,sql.SQL(command).format(sql.Identifier(writer2)))
            second,identity=roles.connect(db,'writer2'); t.connections['writer2']=second
            result['real_logins'].append(identity)
            aliases=['writer','writer2','reader','stranger','columns','viewowner']
            sessions={}; workers={alias:set() for alias in aliases}; worker_roles={}
            obs_lock=threading.Lock(); pinned={}

            def observe(alias,phase,pin=False):
                conn=t.connections[alias]
                # All identity values are from one actual SQL statement.
                row=query(conn,'SELECT session_user,current_user,pg_current_sessid(),pg_current_sessionid(),pg_backend_pid()')[0]
                expected=roles.name(alias)
                if row[0:2]!=(expected,expected): raise AssertionError('caller identity leaked at '+phase+': '+alias)
                logical=int(row[2]); thread=int(row[4])
                if logical<=0 or thread<=0: raise AssertionError('nonpositive native session/thread identity')
                with obs_lock:
                    if alias in sessions and sessions[alias]!=(logical,row[3]):
                        raise AssertionError('logical session changed for persistent real LOGIN '+alias)
                    sessions[alias]=(logical,row[3]); workers[alias].add(thread)
                    worker_roles.setdefault(thread,set()).add(alias)
                    if pin:
                        if alias in pinned and pinned[alias]!=thread:
                            raise AssertionError('active transaction changed its native worker: '+alias)
                        pinned[alias]=thread
                    result['identity_observations'].append({'alias':alias,'phase':phase,
                        'session_user':row[0],'current_user':row[1],'logical_session_id':logical,
                        'host_session_string':row[3],'thread_id':thread,'active_transaction':pin})
                return row

            def no_internal(alias,in_transaction=False):
                conn=t.connections[alias]
                t.reject(alias+' direct metadata remains denied',conn,
                    'SELECT * FROM lake_metadata.ducklake_snapshot','42501',savepoint=in_transaction)
                observe(alias,'after_metadata_denial',pin=in_transaction)

            writers=['writer','writer2']; rowids={'writer':100001,'writer2':100002}
            for alias in aliases: observe(alias,'initial')
            t.check('all persistent logins have distinct logical sessions',len({v[0] for v in sessions.values()}),len(aliases))
            for index,alias in enumerate(writers):
                conn=t.connections[alias]; query(conn,'BEGIN')
                query(conn,'INSERT INTO biz.orders VALUES(%s,%s)',(rowids[alias],700+index))
                query(conn,'UPDATE biz.orders SET val=val+1 WHERE id=%s',(rowids[alias],))
                query(conn,'INSERT INTO biz.heap_marker VALUES(%s,%s)',(rowids[alias],index))
                observe(alias,'live_private_write',True)
                query(conn,'SAVEPOINT acl_child')
                query(conn,'INSERT INTO biz.orders VALUES(%s,999)',(rowids[alias]+1000,))
                query(conn,'ROLLBACK TO acl_child'); query(conn,'RELEASE acl_child')
                no_internal(alias,True)
                t.reject(alias+' SQL error uses host subtransaction rollback',conn,'SELECT 1/0','22012',savepoint=True)
                observe(alias,'after_child_error_rollback',True)

            def churn(alias,iteration):
                conn=t.connections[alias]
                if alias=='stranger':
                    t.reject('stranger remains unauthorized in scheduling round '+str(iteration),conn,'SELECT id FROM biz.orders')
                elif alias=='columns':
                    t.check('column-only reader remains scoped '+str(iteration),
                        query(conn,'SELECT id FROM biz.orders ORDER BY id'),[(r[0],) for r in t.initial])
                else:
                    t.check(alias+' reads only committed fixture in scheduling round '+str(iteration),t.read(conn),t.initial)
                observe(alias,'concurrent_read_'+str(iteration))
                no_internal(alias)

            with ThreadPoolExecutor(max_workers=4) as executor:
                for iteration in range(args.rounds):
                    futures=[executor.submit(churn,alias,iteration) for alias in aliases if alias not in writers]
                    for index,alias in enumerate(writers):
                        t.check(alias+' keeps only own private row '+str(iteration),
                            t.read(t.connections[alias]),t.initial+[(rowids[alias],701+index)])
                        observe(alias,'live_round_'+str(iteration),True)
                    for future in futures: future.result(timeout=30)
                    t.check('observer cannot see private writes '+str(iteration),
                        scalar(t.observer,'SELECT count(*) FROM biz.orders WHERE id>=100000'),0)

            query(t.connections['writer'],'COMMIT'); query(t.connections['writer2'],'ROLLBACK')
            expected=t.initial+[(100001,701)]
            t.check('ordinary lake commit/rollback exact final rows',t.read(t.observer),expected)
            t.check('ordinary heap is atomic with lake writes',query(t.observer,'SELECT * FROM biz.heap_marker'),[(100001,0)])
            for alias in writers:
                observe(alias,'after_transaction_end')
                t.check(alias+' new command has no aborted child or peer overlay',t.read(t.connections[alias]),expected)
                no_internal(alias)
            result['active_transaction_pinning_verified']=True
            # All subsequent writer transactions roll back: no new standard
            # snapshot or permanent IDs can obscure a leaked previous write set.
            committed_image=t.image()
            pinned.clear()

            def recycle(alias,iteration):
                conn=t.connections[alias]; query(conn,'BEGIN'); observe(alias,'recycle_begin_'+str(iteration),True)
                try:
                    if alias in writers:
                        k=200000+(0 if alias=='writer' else 1000)+iteration
                        query(conn,'INSERT INTO biz.orders VALUES(%s,%s)',(k,iteration))
                        t.check(alias+' sees this new transaction only '+str(iteration),
                            query(conn,'SELECT id,val FROM biz.orders WHERE id=%s',(k,)),[(k,iteration)])
                    elif alias=='stranger':
                        t.reject(alias+' denied after worker reuse '+str(iteration),conn,'SELECT id FROM biz.orders',savepoint=True)
                    elif alias=='columns':
                        t.check(alias+' column ACL after worker reuse '+str(iteration),
                            query(conn,'SELECT id FROM biz.orders ORDER BY id'),[(r[0],) for r in expected])
                        t.reject(alias+' val column still denied '+str(iteration),conn,'SELECT val FROM biz.orders',savepoint=True)
                    else: t.check(alias+' reader after worker reuse '+str(iteration),t.read(conn),expected)
                    no_internal(alias,True)
                    observe(alias,'recycle_end_'+str(iteration),True)
                    query(conn,'ROLLBACK' if alias in writers else 'COMMIT')
                except BaseException:
                    query(conn,'ROLLBACK'); raise
                finally:
                    # Transaction pin is a per-transaction invariant only.
                    with obs_lock: pinned.pop(alias,None)
                observe(alias,'outside_transaction_'+str(iteration))

            with ThreadPoolExecutor(max_workers=len(aliases)) as executor:
                for iteration in range(args.rounds):
                    futures=[executor.submit(recycle,alias,iteration) for alias in aliases]
                    for future in futures: future.result(timeout=45)
            t.check('rolled-back reuse rounds preserve all standard metadata',t.image(),committed_image)
            t.check('rolled-back reuse rounds leave no lake rows',t.read(t.observer),expected)
            reply=reverse_modify_reference(t.observer,'lake_metadata',args.manifest,
                args.output.parent/'reference-export-current.json',[{'id':i,'val':v} for i,v in expected],
                args.output.parent/'reference-pool-commit',operations_by_commit=[[
                    {'sql':'INSERT INTO {table} VALUES(100001,700)'},
                    {'sql':'UPDATE {table} SET val=val+1 WHERE id=100001'}]])
            result['reference_comparison']=reply
            result['artifacts'].extend(reply.get('artifacts',[]))
            t.check('real reference matches ordinary-user committed protocol',reply['status'],'pass')
            result['session_workers']={alias:sorted(values) for alias,values in workers.items()}
            result['worker_roles']={str(thread):sorted(values) for thread,values in worker_roles.items()}
            reused=any(len(values)>1 for values in worker_roles.values())
            result['cross_role_worker_reuse_observed']=reused
            result['cross_transaction_session_migration_observed']=any(len(values)>1 for values in workers.values())
            result['functional_status']='pass'
            if args.mode=='thread_pool' and not reused:
                result.update(status='blocked',reason='Functional checks passed; bounded probe did not observe one worker serving different ordinary roles.')
            else: result['status']='pass'
    except Blocked as exc: result.update(status='blocked',reason=str(exc))
    except BaseException as exc: result.update(status='fail',reason=str(exc),traceback=traceback.format_exc())
    finally:
        if db is not None:
            result['artifacts'].extend(db.artifacts()); result['database_cleanup']=db.cleanup_result
        if roles is not None and control is not None:
            try:
                if db is not None and db.cleanup_result and db.cleanup_result['returncode']==0:
                    result['role_cleanup']=cleanup_roles(control,roles.path,args.run_id)
                elif not roles.items: result['role_cleanup']={'status':'pass','reason':'No roles allocated'}
                else: raise RuntimeError('Owned DB cleanup not proven; preserving dependent roles')
            except BaseException as exc: result.update(status='fail',cleanup_error=str(exc))
            result['artifacts'].append({'path':str(roles.path)})
            result=roles.redact(result)
        if control is not None: control.close()
        write_json(args.output,result)
        print(json.dumps({'status':result['status'],'result':str(args.output)}),flush=True)
    return {'pass':0,'blocked':2}.get(result['status'],1)


if __name__=='__main__': raise SystemExit(main())
