#!/usr/bin/env python3
"""Owned-role trust-boundary attacks; never grants a membership in real admins.

All injected ACL/policy changes roll back. Configured-owner reauthorization is
an explicit administrator trust change: test lake fail-closed, not a new global
prohibition on ordinary heap access that the administrator has just authorized.
"""
import argparse
import json
import os
from pathlib import Path
import re
import sys
import traceback

DRIVER=Path(os.getenv('OG_LAKE_DRIVER_ROOT','/home/qiumc/data/code/cpp/integration/opengauss-native-lake'))
sys.path.insert(0,str(DRIVER)); sys.path.insert(0,str(DRIVER/'tests'))
sys.path.insert(0,str(Path(__file__).resolve().parent))
from permission_api import (WORKSPACE,API_VERSION,OwnedRoles,ordinary_connection,reviewed_permission_runtime,
                            cleanup_roles,adopt_private_owner)
from inline_fixture import foreign_table_statement,query,scalar
from lake_harness import Blocked,validate_run_id,write_json
from native_test_context import OwnedDatabase,load_reference


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--run-id',default=os.getenv('OG_LAKE_RUN_ID'))
    p.add_argument('--output',default=os.getenv('OG_LAKE_RESULT_FILE'))
    p.add_argument('--manifest',default=os.getenv('OG_LAKE_FIXTURE_MANIFEST'))
    args=p.parse_args()
    if not all((args.run_id,args.output,args.manifest)): p.error('run-id/output/real I manifest required')
    validate_run_id(args.run_id); args.output=Path(args.output).resolve()
    args.output.parent.mkdir(parents=True,exist_ok=True)
    if args.output.exists(): p.error('use a fresh result path')
    result={'status':'fail','scope':'ACL/owned_administrator_inheritance','assertions':[],'artifacts':[],
        'limits':['Only randomly named OID/comment-owned roles are altered; no real administrator memberships.',
                  'Owned SYSADMIN role is test setup, never represented as an ordinary business identity.',
                  'Post-config administrator GRANT changes trust: the acceptance promise is lake fail-closed.',
                  'Does not require a global GrantRole or all-heap DML hook.']}
    control=None; roles=None; db=None

    def check(label,observed,expected=True):
        result['assertions'].append({'name':label,'status':'pass' if observed==expected else 'fail',
                                    'details':{'observed':observed,'expected':expected}})
        if observed!=expected: raise AssertionError(label)

    def reject(label,conn,statement,params=None):
        import psycopg2
        try: query(conn,statement,params)
        except psycopg2.Error as exc: check(label+' SQLSTATE',exc.pgcode,'42501')
        else: check(label+' must reject','accepted','42501')

    try:
        control_file=WORKSPACE/'.worktrees/opengauss-native-lake/contrib/og_lake/og_lake.control'
        if re.findall(r"^default_version\s*=\s*'([^']+)'",control_file.read_text(),re.M)!=[reviewed_permission_runtime()]:
            raise Blocked('Reviewed actual runtime0.5/0.6 required with installed SQL0.5')
        import psycopg2
        from psycopg2 import sql
        control=psycopg2.connect(host=str(WORKSPACE/'.runtime/opengauss-native-lake/debug/socket'),port=55560,
                                dbname='postgres',user=os.getenv('USER','qiumc'),connect_timeout=10)
        control.autocommit=True; query(control,'SET statement_timeout=15000')
        roles=OwnedRoles(control,args.run_id,args.output.parent)
        with OwnedDatabase(args.run_id,args.output.parent) as db:
            # CREATE ROLE initially uses the normal owned-role receipt. Elevate
            # only these exact newly-created identities, never the environment user.
            extadmin=roles.create('extadmin'); acladmin=roles.create('acladmin',False)
            member=roles.create('member'); indirect=roles.create('indirect')
            private=roles.create('private',False)
            for role in (extadmin,acladmin):
                query(control,sql.SQL('ALTER ROLE {} SYSADMIN').format(sql.Identifier(role)))
                check(role+' is the newly allocated test system administrator',
                      scalar(control,'SELECT rolsystemadmin FROM pg_roles WHERE rolname=%s',(role,)),True)
            owner=ordinary_connection(db.database,extadmin,roles.passwords['extadmin']); db.connections.append(owner)
            observer=db.connect()
            check('extension is created by actual separate admin LOGIN',query(owner,'SELECT session_user,current_user'),[(extadmin,extadmin)])
            query(owner,"CREATE EXTENSION og_lake VERSION '0.5'")
            check('actual runtime matches reviewed source',json.loads(scalar(owner,'SELECT og_lake.host_status()'))['module_version'],reviewed_permission_runtime())
            check('extension owner is owned temporary admin',scalar(observer,"SELECT extowner FROM pg_extension WHERE extname='og_lake'"),
                  scalar(control,'SELECT oid FROM pg_roles WHERE rolname=%s',(extadmin,)))
            manifest,exported,counts,artifacts=load_reference(owner,args.manifest,args.run_id,args.output.parent)
            result['artifacts'].extend(artifacts); check('actual I fixture',manifest['case'],'I')
            check('complete reference import',len(counts),len(exported['tables']))
            expected=[(r['id'],r['val']) for r in manifest['expected']]
            query(owner,'CREATE SCHEMA biz')
            query(owner,"SELECT og_lake.attach_catalog('lake_reference','lake_metadata',NULL::text,false)")
            query(owner,foreign_table_statement('biz','orders','lake_reference',manifest['table_id'],manifest['schema']))
            for alias in ('member','indirect'):
                role=sql.Identifier(roles.name(alias))
                query(owner,sql.SQL('GRANT USAGE ON SCHEMA biz TO {}').format(role))
                query(owner,sql.SQL('GRANT USAGE ON FOREIGN SERVER lake_reference TO {}').format(role))
                query(owner,sql.SQL('GRANT SELECT ON biz.orders TO {}').format(role))
            ordinary,identity=roles.connect(db,'member'); result['ordinary_login']=identity
            other,identity=roles.connect(db,'indirect'); result['indirect_login']=identity

            def image():
                return {t['table_name']:sorted(repr(tuple(r)) for r in query(observer,
                    sql.SQL('SELECT * FROM lake_metadata.{}').format(sql.Identifier(t['table_name'])))) for t in exported['tables']}
            baseline=image()
            # Cache a real policy plan under the owner while the relation is
            # empty. Later changing CURRENT_USER must still check actual ACL.
            query(owner,'PREPARE policy_before_configuration(oid) AS UPDATE og_lake_internal.access_policy SET internal_owner_oid=$1 RETURNING internal_owner_oid')
            check('preconfiguration policy is empty',query(owner,'EXECUTE policy_before_configuration(%s)',
                  (scalar(control,'SELECT oid FROM pg_roles WHERE rolname=%s',(private,)),)),[])
            query(control,sql.SQL('GRANT {} TO {}').format(sql.Identifier(extadmin),sql.Identifier(member)))
            check('test member has no administrative flags',query(control,
                  'SELECT rolsuper,rolsystemadmin,rolcreaterole,rolcreatedb FROM pg_roles WHERE rolname=%s',(member,)),[(False,False,False,False)])
            query(owner,'BEGIN')
            try:
                # No ownership side effect may survive a rejected configure.
                try:
                    adopt_private_owner(owner,exported,private)
                except psycopg2.Error as exc:
                    check('administrator owner incoming membership blocks configure',exc.pgcode,'42501')
                else: check('unsafe incoming membership configure must reject','accepted','42501')
            finally: query(owner,'ROLLBACK')
            check('rejected configure leaves policy empty',scalar(observer,'SELECT count(*) FROM og_lake_internal.access_policy'),0)
            check('rejected configure preserves standard data',image(),baseline)
            reject('unconfigured member cannot use lake through inherited owner',ordinary,'SELECT id,val FROM biz.orders')
            query(control,sql.SQL('REVOKE {} FROM {}').format(sql.Identifier(extadmin),sql.Identifier(member)))
            query(owner,'BEGIN')
            try: adopt_private_owner(owner,exported,private); query(owner,'COMMIT')
            except BaseException: query(owner,'ROLLBACK'); raise
            check('revoking membership enables explicit configure and ordinary read',query(ordinary,'SELECT id,val FROM biz.orders ORDER BY id'),expected)
            policy=query(observer,'SELECT * FROM og_lake_internal.access_policy')
            query(owner,'BEGIN')
            try:
                query(owner,sql.SQL('SET ROLE {} PASSWORD {}').format(sql.Identifier(member),sql.Literal(roles.passwords['member'])))
                reject('old prepared policy plan cannot write after owner membership revoke',owner,
                       'EXECUTE policy_before_configuration(%s)',(scalar(control,'SELECT oid FROM pg_roles WHERE rolname=%s',(member,)),))
            finally: query(owner,'ROLLBACK'); query(owner,'RESET ROLE')
            check('cached policy denial preserves configured policy',query(observer,'SELECT * FROM og_lake_internal.access_policy'),policy)

            # Materialize a genuine cached lake plan before trust is changed.
            query(ordinary,'PREPARE lake_trust_cache AS SELECT id,val FROM biz.orders ORDER BY id')
            check('healthy cached lake plan',query(ordinary,'EXECUTE lake_trust_cache'),expected)
            query(control,sql.SQL('GRANT {} TO {}').format(sql.Identifier(extadmin),sql.Identifier(member)))
            try:
                reject('new lake access rejects later administrator-owner member',ordinary,'SELECT id,val FROM biz.orders')
                reject('cached lake access rejects later administrator-owner member',ordinary,'EXECUTE lake_trust_cache')
            finally: query(control,sql.SQL('REVOKE {} FROM {}').format(sql.Identifier(extadmin),sql.Identifier(member)))
            check('same cached lake plan recovers after restoring trust',query(ordinary,'EXECUTE lake_trust_cache'),expected)

            # Explicit ACL to another temporary SYSADMIN is unsafe if a normal
            # INHERIT member can obtain it. Check the actual privilege chain.
            query(control,sql.SQL('GRANT {} TO {}').format(sql.Identifier(acladmin),sql.Identifier(indirect)))
            attacks=[('namespace','GRANT USAGE ON SCHEMA lake_metadata TO {}',
                      "SELECT has_schema_privilege(%s,'lake_metadata','USAGE')"),
                     ('table','GRANT SELECT ON lake_metadata.ducklake_snapshot TO {}',
                      "SELECT has_table_privilege(%s,'lake_metadata.ducklake_snapshot','SELECT')"),
                     ('column','GRANT SELECT(snapshot_id) ON lake_metadata.ducklake_snapshot TO {}',
                      "SELECT has_column_privilege(%s,'lake_metadata.ducklake_snapshot','snapshot_id','SELECT')")]
            for label,grant,privilege in attacks:
                query(owner,'BEGIN')
                try:
                    query(owner,sql.SQL(grant).format(sql.Identifier(acladmin)))
                    check(label+' explicit admin grant really inherits to ordinary role',scalar(owner,privilege,(indirect,)),True)
                    query(owner,sql.SQL('SET ROLE {} PASSWORD {}').format(sql.Identifier(member),sql.Literal(roles.passwords['member'])))
                    reject(label+' ACL may name only exact private owner',owner,'SELECT id,val FROM biz.orders')
                finally: query(owner,'ROLLBACK'); query(owner,'RESET ROLE')
                check(label+' rollback restores real ordinary reader',query(ordinary,'SELECT id,val FROM biz.orders ORDER BY id'),expected)
            # Deliberate owner-authored policy corruption is not a valid new
            # configuration. No caller should gain lake access from it.
            query(owner,'BEGIN')
            try:
                query(owner,'UPDATE og_lake_internal.access_policy SET internal_owner_oid=%s',
                      (scalar(control,'SELECT oid FROM pg_roles WHERE rolname=%s',(member,)),))
                query(owner,sql.SQL('SET ROLE {} PASSWORD {}').format(sql.Identifier(member),sql.Literal(roles.passwords['member'])))
                reject('policy pointing at ordinary LOGIN cannot elevate lake access',owner,'SELECT id,val FROM biz.orders')
            finally: query(owner,'ROLLBACK'); query(owner,'RESET ROLE')
            check('all injected policy changes rolled back',query(observer,'SELECT * FROM og_lake_internal.access_policy'),policy)
            check('inheritance/ACL attacks preserve complete standard data',image(),baseline)
            check('real ordinary login remains ordinary after all refusals',query(ordinary,'SELECT session_user,current_user'),[(member,member)])
            query(ordinary,'DEALLOCATE lake_trust_cache'); query(owner,'DEALLOCATE policy_before_configuration')
            result['status']='pass'
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
                else: raise RuntimeError('Owned DB cleanup not proven; roles preserved')
            except BaseException as exc: result.update(status='fail',cleanup_error=str(exc))
            result['artifacts'].append({'path':str(roles.path)}); result=roles.redact(result)
        if control is not None: control.close()
        write_json(args.output,result)
        print(json.dumps({'status':result['status'],'result':str(args.output)}))
    return {'pass':0,'blocked':2}.get(result['status'],1)


if __name__=='__main__': raise SystemExit(main())
