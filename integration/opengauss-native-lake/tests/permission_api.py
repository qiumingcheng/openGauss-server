"""Proposed 0.5 test adapter. Calls real SQL; never grants internal data access.

No API registration or product fallback lives here. The reviewed implementation
must provide the named function and extension version before the test can run.
"""
import datetime
import hashlib
import json
import os
from pathlib import Path
import secrets
import sys
import uuid

WORKSPACE=Path('/home/qiumc/data/code/cpp')
DRIVER=Path(os.getenv('OG_LAKE_DRIVER_ROOT',str(WORKSPACE/'integration/opengauss-native-lake')))
sys.path.insert(0,str(DRIVER))
from inline_fixture import foreign_table_statement, query, scalar
from lake_harness import Blocked, write_json
from native_runtime import RUNTIME, PORT

API_VERSION='0.5'
INTERNAL_OWNER_FUNCTION='og_lake.configure_internal_owner'


def reviewed_permission_runtime():
    """Runtime compatibility is separate from the intentionally fixed SQL0.5 API."""
    from native_test_context import reviewed_runtime_version
    version=reviewed_runtime_version()
    if version not in ('0.5','0.6','0.7', '0.8', '0.9', '0.10', '0.11'):
        raise Blocked('Ordinary-user tests require an exact reviewed 0.5/0.6/0.7/0.8/0.9 runtime with installed SQL0.5')
    return version


def ordinary_connection(database,role,password):
    import psycopg2
    conn=psycopg2.connect(host=str(RUNTIME/'socket'),
                         port=PORT,dbname=database,user=role,password=password,connect_timeout=10,
                         application_name='og_lake_permissions_login')
    conn.autocommit=True
    query(conn,'SET statement_timeout=15000')
    return conn


class OwnedRoles:
    """Precise OID+comment ownership receipt; passwords stay only in memory."""
    def __init__(self,admin,run_id,output):
        if Path(scalar(admin,'SHOW data_directory')).resolve() != (RUNTIME/'data').resolve():
            raise Blocked('Role controller belongs to another runtime profile')
        self.admin,self.run_id,self.output=admin,run_id,Path(output)
        self.path=self.output/'permission-roles.json'
        if self.path.exists(): raise RuntimeError('Refusing to overwrite an existing owned-role receipt')
        self.passwords={}; self.password_digests=[]; self.items=[]
        self.start=str(scalar(admin,'SELECT pg_postmaster_start_time()'))
        self.administrator=scalar(admin,'SELECT oid FROM pg_roles WHERE rolname=current_user')
        self.write()

    def write(self):
        write_json(self.path,{'run_id':self.run_id,'postmaster_start':self.start,
                             'administrator_oid':self.administrator,'roles':self.items})

    def create(self,alias,login=True):
        from psycopg2 import sql
        name='lake_acl_'+uuid.uuid4().hex[:16]+'_'+alias
        marker='og_lake_role_attempt:'+self.run_id+':'+uuid.uuid4().hex
        item={'alias':alias,'name':name,'oid':None,'marker':marker,'phase':'planned','login':login}
        self.items.append(item); self.write()
        query(self.admin,'BEGIN')
        try:
            # openGauss requires a non-NULL password even for a NOLOGIN role.
            # Keep it only in memory for error redaction; NOLOGIN stays enforced.
            password='Og1!'+secrets.token_hex(12)
            self.passwords[alias]=password
            if login:
                # The installed psycopg2 uses PostgreSQL libpq, which cannot
                # perform openGauss SHA256 authentication. Only these owned,
                # short-lived test logins use its supported MD5 credential.
                # Do not change HBA or the server-wide password configuration.
                digest='md5'+hashlib.md5((password+name).encode('utf-8')).hexdigest()
                self.password_digests.append(digest)
                item['test_password_format']='md5_for_postgresql_libpq'
                query(self.admin,sql.SQL('CREATE ROLE {} LOGIN ENCRYPTED PASSWORD {}').format(sql.Identifier(name),sql.Literal(digest)))
            else:
                query(self.admin,sql.SQL('CREATE ROLE {} NOLOGIN NOINHERIT PASSWORD {}').format(sql.Identifier(name),sql.Literal(password)))
            item['oid']=scalar(self.admin,'SELECT oid FROM pg_roles WHERE rolname=%s',(name,))
            query(self.admin,sql.SQL('COMMENT ON ROLE {} IS {}').format(sql.Identifier(name),sql.Literal(marker)))
            item['phase']='prepared'; self.write()
            query(self.admin,'COMMIT')
            item['phase']='owned'; self.write()
        except BaseException:
            query(self.admin,'ROLLBACK'); raise
        return name

    def name(self,alias): return next(i['name'] for i in self.items if i['alias']==alias)

    def connect(self,db,alias):
        conn=ordinary_connection(db.database,self.name(alias),self.passwords[alias])
        db.connections.append(conn)
        identity=query(conn,'SELECT session_user,current_user,pg_backend_pid()')[0]
        if identity[0]!=self.name(alias) or identity[1]!=self.name(alias):
            raise AssertionError('Actual login did not use the requested ordinary identity')
        flags=query(self.admin,'SELECT rolsuper,rolsystemadmin,rolcreaterole,rolcreatedb FROM pg_roles WHERE rolname=%s',(self.name(alias),))[0]
        if any(flags): raise AssertionError('Ordinary test login unexpectedly has administrative privileges')
        return conn,{'alias':alias,'role':identity[0],'current_user':identity[1],
                     'backend_pid_at_login':identity[2],'administrative_flags':list(flags)}

    def redact(self,value):
        text=json.dumps(value,ensure_ascii=False,default=str)
        for password in list(self.passwords.values())+self.password_digests:
            text=text.replace(password,'<ephemeral-password-redacted>')
        return json.loads(text)


def adopt_private_owner(admin,exported,owner):
    """Explicit preparation in the caller's transaction; never commits here."""
    from psycopg2 import sql
    registered=scalar(admin,"SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace "
                            "WHERE n.nspname='og_lake' AND p.proname='configure_internal_owner' "
                            "AND p.proargtypes::text='19' AND p.prorettype=2278")
    if registered!=1: raise Blocked('Reviewed 0.5 configure_internal_owner(name) RETURNS void is not registered')
    query(admin,'REVOKE ALL ON SCHEMA lake_metadata FROM PUBLIC')
    query(admin,sql.SQL('ALTER SCHEMA lake_metadata OWNER TO {}').format(sql.Identifier(owner)))
    for table in exported['tables']:
        relation=sql.SQL('lake_metadata.{}').format(sql.Identifier(table['table_name']))
        query(admin,sql.SQL('REVOKE ALL ON {} FROM PUBLIC').format(relation))
        query(admin,sql.SQL('ALTER TABLE {} OWNER TO {}').format(relation,sql.Identifier(owner)))
    query(admin,'SELECT og_lake.configure_internal_owner(%s::name)',(owner,))


def configure_and_attach(admin,exported,owner,table_id,existing_binding=False):
    """Explicit admin adoption is visible and rollbackable; it is not query-time."""
    query(admin,'BEGIN')
    try:
        adopt_private_owner(admin,exported,owner)
        query(admin,'COMMIT')
    except BaseException:
        query(admin,'ROLLBACK'); raise
    if not existing_binding:
        query(admin,'CREATE SCHEMA biz')
        query(admin,"SELECT og_lake.attach_catalog('lake_reference','lake_metadata',NULL::text,false)")
        query(admin,foreign_table_statement('biz','orders','lake_reference',table_id,
                                          [{'name':'id','type':'INTEGER'},{'name':'val','type':'INTEGER'}]))


def cleanup_roles(admin,receipt_path,run_id):
    """Call only after owned DB cleanup; never DROP OWNED or delete by prefix."""
    from psycopg2 import sql
    receipt_path=Path(receipt_path)
    if not receipt_path.exists(): return {'status':'pass','reason':'No role allocation receipt.'}
    receipt=json.loads(receipt_path.read_text())
    if receipt['run_id']!=run_id: raise RuntimeError('Role receipt belongs to another run')
    if str(scalar(admin,'SELECT pg_postmaster_start_time()'))!=receipt['postmaster_start']:
        raise RuntimeError('Refusing role cleanup after an unreviewed postmaster change')
    if scalar(admin,'SELECT oid FROM pg_roles WHERE rolname=current_user')!=receipt['administrator_oid']:
        raise RuntimeError('Role cleanup requires the recorded administrator')
    removed=[]
    for item in reversed(receipt['roles']):
        rows=query(admin,"SELECT oid,rolname,shobj_description(oid,'pg_authid') FROM pg_roles "
                         "WHERE rolname=%s OR oid=%s",(item['name'],item['oid'] or 0))
        if not rows:
            if item['phase'] not in ('dropped','absent'): item['phase']='absent'
            continue
        if item['phase'] not in ('prepared','owned','dropped') or rows!=[(item['oid'],item['name'],item['marker'])]:
            raise RuntimeError('Role OID/name/marker no longer matches owned allocation')
        query(admin,sql.SQL('DROP ROLE {}').format(sql.Identifier(item['name'])))
        item['phase']='dropped'; removed.append(item['name'])
        write_json(receipt_path,receipt)
    write_json(receipt_path,receipt)
    return {'status':'pass','removed':removed}
