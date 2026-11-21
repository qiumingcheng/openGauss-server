#!/usr/bin/env python3
"""Actual native local-file scans of a genuine fixed DuckLake F export."""
import argparse,json,os,shutil,sys,traceback
from pathlib import Path
ROOT=Path('/home/qiumc/data/code/cpp');DRIVER=ROOT/'integration/opengauss-native-lake'
sys.path[:0]=[str(DRIVER),str(DRIVER/'tests')]
from inline_fixture import query,scalar,foreign_table_statement
from native_test_context import OwnedDatabase,load_reference,reviewed_runtime_version
from permission_api import OwnedRoles,adopt_private_owner,cleanup_roles
from lake_harness import Blocked,sha256,validate_run_id,write_json

PROVIDER=ROOT/'.install/opengauss-native-lake-debug/lib/postgresql/og_lake_sdk'

class Case:
    def __init__(self,db,args,result,roles):
        from psycopg2 import sql
        self.sql,self.db,self.args,self.result,self.roles=sql,db,args,result,roles
        self.a,self.observer=[db.connect() for _ in range(2)]
        installed=getattr(args,'installed_version',None) or ('0.9' if args.mode.startswith('versions_') and args.mode!='versions_upgrade' else ('0.7' if args.mode=='legacy' else '0.8'))
        query(self.a,"CREATE EXTENSION og_lake VERSION '"+installed+"'")
        runtime=reviewed_runtime_version()
        if runtime not in ('0.8','0.9', '0.10', '0.11'):raise Blocked('Local files require reviewed runtime0.8/0.9')
        self.check('exact reviewed native runtime',json.loads(scalar(self.a,'SELECT og_lake.host_status()'))['module_version'],runtime)
        manifest,exported,counts,artifacts=load_reference(self.a,args.manifest,args.run_id,args.output.parent)
        profile={'types':'FILE_TYPED','batch':'FILE_BATCH','mixed':'FILE_MIXED','inline_update':'FILE_INLINE_UPDATE','inline_delete':'FILE_INLINE_DELETE','versions_inline_update':'FILE_INLINE_UPDATE','versions_inline_delete':'FILE_INLINE_DELETE','versions_batch':'FILE_VERSIONED_BATCH','versions_explicit':'FILE_VERSIONED','versions_delete_errors':'FILE_VERSIONED','versions_rescan':'FILE_VERSIONED','versions_acl':'FILE_VERSIONED','versions_upgrade':'FILE_VERSIONED'}.get(args.mode,'F')
        profile=getattr(args,'fixture_profile',None) or profile
        if manifest.get('case')!=profile:raise Blocked('Local-file case requires the genuine '+profile+' fixture')
        self.exported=exported
        self.manifest=manifest;self.result['artifacts'].extend(artifacts)
        self.check('complete real catalog imported',len(counts),len(exported['tables']))
        self.expected=sorted((row['id'],row['val']) for row in manifest['expected'])
        self.paths={str(Path(item['path']).resolve()) for item in manifest['files'] if item['path'].endswith('.parquet')}
        self.check('real F contains data files',len(self.paths)>0,True)
        self.original_hashes={path:sha256(path) for path in self.paths}
        self.owner=roles.create('private',False)
        query(self.a,'BEGIN');adopt_private_owner(self.a,exported,self.owner);query(self.a,'COMMIT')
        query(self.a,'CREATE SCHEMA biz')
        query(self.a,"SELECT og_lake.attach_catalog('lake_reference','lake_metadata',NULL::text,%s)",(getattr(args,'read_only',True),))

    def check(self,name,actual,expected=True):
        status='pass' if actual==expected else 'fail'
        self.result['assertions'].append({'name':name,'status':status,'observed':actual,'expected':expected})
        if status!='pass':raise AssertionError(name)

    def reject(self,name,statement,states,conn=None):
        import psycopg2
        try:query(conn or self.a,statement)
        except psycopg2.Error as exc:
            self.check(name,exc.pgcode in ([states] if isinstance(states,str) else states),True)
            self.result['assertions'][-1]['sqlstate']=exc.pgcode
        else:self.check(name,'unexpectedly accepted',states)

    def bind(self):
        query(self.a,foreign_table_statement('biz','orders','lake_reference',self.manifest['table_id'],self.manifest['schema']))

    def rows(self,conn=None):return query(conn or self.a,'SELECT id,val FROM biz.orders ORDER BY id')

    def descriptors(self):
        pid=int((ROOT/'.runtime/opengauss-native-lake/debug/data/postmaster.pid').read_text().splitlines()[0])
        found=[]
        for fd in Path('/proc/'+str(pid)+'/fd').iterdir():
            try:target=os.readlink(fd)
            except FileNotFoundError:continue
            if target in self.paths:found.append(str(fd))
        return found

    def finish(self):self.check('all real source Parquet bytes unchanged',{p:sha256(p) for p in self.paths},self.original_hashes)

def execute(t):
    mode=t.args.mode
    if mode.startswith('versions_'):
        from native_file_versions import execute_versions
        execute_versions(t);return
    if mode in ('inline_update','inline_delete'):
        import psycopg2
        t.check('genuine inline file position deletion exists',len(t.manifest['inline_file_deletes']),1)
        try:t.bind()
        except psycopg2.Error as exc:
            t.check('unsupported inline file deletion rejected before rows',exc.pgcode,'0A000')
            t.check('failed binding leaves no business object',scalar(t.a,"SELECT count(*) FROM og_lake_internal.table_binding"),0)
            t.check('host remains responsive after rejection',scalar(t.a,'SELECT 42'),42)
        else:
            t.result['unexpected_native_rows']=t.rows()
            t.result['genuine_reference_rows']=t.expected
            t.check('unsupported inline file deletion rejected before rows','accepted','0A000')
        return
    if mode in ('missing','legacy'):
        if mode=='missing' and (PROVIDER/'libog_lake_sdk_arrow.so').exists():
            raise Blocked('Missing-provider test must precede installing the provider in this owned prefix')
        t.reject('unsupported/missing provider rejects binding before rows',
            foreign_table_statement('biz','orders','lake_reference',t.manifest['table_id'],t.manifest['schema']),'0A000')
        t.check('no failed table binding',scalar(t.a,'SELECT count(*) FROM og_lake_internal.table_binding'),0)
        t.check('host remains responsive',scalar(t.a,'SELECT 42'),42);return
    t.bind();t.check('native file scan equals genuine reference rows',t.rows(),t.expected)
    if mode=='read':
        t.check('native qual/aggregate reads file inputs',query(t.a,'SELECT count(*),sum(val) FROM biz.orders WHERE id>=2'),
            [(sum(row[0]>=2 for row in t.expected),sum(row[1] for row in t.expected if row[0]>=2))])
        query(t.a,'PREPARE read_file(integer) AS SELECT id,val FROM biz.orders WHERE id>=$1 ORDER BY id')
        for limit in (1,2,3,4):t.check('prepared file scan '+str(limit),query(t.a,'EXECUTE read_file('+str(limit)+')'),[row for row in t.expected if row[0]>=limit])
        query(t.a,'DEALLOCATE read_file')
        plan='\n'.join(row[0] for row in query(t.a,'EXPLAIN (ANALYZE,COSTS OFF) SELECT id,val FROM biz.orders'))
        (t.args.output.parent/'file-plan.txt').write_text(plan+'\n')
        t.check('native executor plan contains local file input count','Local File Inputs' in plan,True)
        t.check('file input has no external executor','DuckDB' in plan,False)
    elif mode=='rescan':
        query(t.a,'SET enable_material=off');query(t.a,'SET enable_hashjoin=off');query(t.a,'SET enable_mergejoin=off')
        statement='SELECT g.n,o.id,o.val FROM generate_series(1,3) g(n) CROSS JOIN LATERAL (SELECT id,val FROM biz.orders WHERE id>=g.n OFFSET 0) o ORDER BY 1,2'
        t.check('parameterized repeated local-file scan',query(t.a,statement),[(n,i,v) for n in range(1,4) for i,v in t.expected if i>=n])
        plan='\n'.join(row[0] for row in query(t.a,'EXPLAIN (ANALYZE,COSTS OFF) '+statement))
        (t.args.output.parent/'rescan-plan.txt').write_text(plan+'\n')
        t.check('plan actually exercised executor rescans','loops=3' in plan,True)
    elif mode=='acl':
        name=t.roles.create('reader',True)
        query(t.a,t.sql.SQL('GRANT USAGE ON SCHEMA biz TO {}').format(t.sql.Identifier(name)))
        query(t.a,t.sql.SQL('GRANT USAGE ON FOREIGN SERVER lake_reference TO {}').format(t.sql.Identifier(name)))
        query(t.a,t.sql.SQL('GRANT SELECT(id) ON biz.orders TO {}').format(t.sql.Identifier(name)))
        login,identity=t.roles.connect(t.db,'reader');t.result['ordinary_identity']=identity
        t.check('ordinary column grant reads Parquet',query(login,'SELECT id FROM biz.orders ORDER BY id'),[(i,) for i,v in t.expected])
        t.reject('ungranted file column rejected','SELECT val FROM biz.orders','42501',login)
        query(t.a,t.sql.SQL('GRANT SELECT ON biz.orders TO {}').format(t.sql.Identifier(name)))
        t.check('ordinary full SELECT equals reference',t.rows(login),t.expected)
        t.reject('ordinary reader cannot access directory','SELECT * FROM lake_metadata.ducklake_data_file','42501',login)
    elif mode=='errors':
        for name,assignment,codes in [
            ('invalid file size','file_size_bytes=1','XX001'),
            ('footer count mismatch','record_count=record_count+1','XX001'),
            ('path traversal',"path='../outside.parquet'",'XX001'),
            ('unimplemented mapping','mapping_id=1000','0A000'),
            ('unimplemented partial versions','partial_max=begin_snapshot','0A000'),
            ('missing file',"path='does-not-exist.parquet'",['58P01','58030'])]:
            query(t.a,'BEGIN');query(t.a,'UPDATE lake_metadata.ducklake_data_file SET '+assignment)
            t.reject(name+' rejected before successful result','SELECT * FROM biz.orders',codes)
            query(t.a,'ROLLBACK');t.check(name+' rollback restores readable binding',t.rows(),t.expected)
        for statement in ['INSERT INTO biz.orders VALUES(99,99)','UPDATE biz.orders SET val=val+1','DELETE FROM biz.orders WHERE false']:
            t.reject('file DML remains explicitly gated: '+statement,statement,'0A000')
            t.check('failed DML preserves original rows',t.rows(),t.expected)
    elif mode=='lifetime':
        t.check('ordinary complete SELECT closes all owned file descriptors',len(t.descriptors()),0)
        for i in range(4):
            t.check('LIMIT reads a prefix '+str(i),query(t.a,'SELECT id FROM biz.orders LIMIT 1'),[(t.expected[0][0],)])
            t.check('LIMIT closes its file reader '+str(i),len(t.descriptors()),0)
            t.reject('parent executor error releases file reader '+str(i),'SELECT 10/(id-2) FROM biz.orders','22012')
            t.check('error closes owned file descriptors '+str(i),len(t.descriptors()),0)
        for action in ['CLOSE held','ROLLBACK']:
            query(t.a,'BEGIN');query(t.a,'DECLARE held NO SCROLL CURSOR FOR SELECT id,val FROM biz.orders')
            t.check('cursor fetch retains exactly its row values',query(t.a,'FETCH 1 FROM held'),t.expected[:1])
            t.check('cursor keeps the actual file handle alive',len(t.descriptors()),1)
            query(t.a,action)
            if action.startswith('CLOSE'):query(t.a,'COMMIT')
            t.check('cursor cleanup closes actual file descriptor: '+action,len(t.descriptors()),0)
    elif mode in ('types','batch'):
        names=[column['name'] for column in t.manifest['schema']]
        expected=[tuple(row[name] for name in names) for row in t.manifest['expected']]
        statement=t.sql.SQL('SELECT {} FROM biz.orders ORDER BY id').format(t.sql.SQL(',').join(map(t.sql.Identifier,names)))
        t.check('all native types, NULLs and UTF8 equal the independent reference',query(t.a,statement),expected)
        if mode=='types':
            t.check('large native UTF8 value remains complete',scalar(t.a,'SELECT octet_length(note) FROM biz.orders WHERE id=1'),34304)
        else:
            t.check('fixture crosses three actual SDK batch boundaries',len(expected),12288)
            query(t.a,'BEGIN');query(t.a,'DECLARE batches NO SCROLL CURSOR FOR SELECT * FROM biz.orders')
            collected=[]
            for fetch in (5000,5000,5000):collected.extend(query(t.a,'FETCH '+str(fetch)+' FROM batches'))
            query(t.a,'CLOSE batches');query(t.a,'COMMIT')
            t.check('successive fetches preserve all row/type values across batches',sorted(collected,key=lambda row:row[0]),expected)
            t.check('wide cursor closes SDK input',len(t.descriptors()),0)
    elif mode=='mixed':
        t.check('fixture has exactly one standard inline row',sum(map(len,t.manifest['inline_rows'].values())),1)
        t.check('file and TableAM streams form one complete relation',query(t.a,'SELECT count(*),count(DISTINCT id),sum(val) FROM biz.orders'),
            [(4,4,sum(row[1] for row in t.expected))])
        t.check('late inline row is visible alongside immutable file',query(t.a,'SELECT id,val FROM biz.orders WHERE id=4'),[(4,400)])

def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--mode',choices=['missing','legacy','read','rescan','acl','errors','lifetime','types','batch','mixed','inline_update','inline_delete','versions_inline_update','versions_inline_delete','versions_explicit','versions_batch','versions_delete_errors','versions_rescan','versions_acl','versions_upgrade'],required=True)
    p.add_argument('--run-id',default=os.getenv('OG_LAKE_RUN_ID'));p.add_argument('--manifest',type=Path,default=os.getenv('OG_LAKE_FIXTURE_MANIFEST'))
    p.add_argument('--output',type=Path,default=os.getenv('OG_LAKE_RESULT_FILE'));args=p.parse_args()
    if not all([args.run_id,args.manifest,args.output]):p.error('run-id, manifest and output required')
    validate_run_id(args.run_id);args.output=args.output.resolve();args.manifest=args.manifest.resolve();args.output.parent.mkdir(parents=True,exist_ok=True)
    result={'status':'fail','scope':'native_local_file_'+args.mode,'run_id':args.run_id,'assertions':[],'artifacts':[],
        'limitations':['Local bounded reader only; no file DML, deletion vectors, schema evolution, flush, SMP, RAC or commercial qualification']}
    inputs=[Path(__file__),DRIVER/'inline_fixture.py',DRIVER/'native_test_context.py',DRIVER/'tests/permission_api.py',DRIVER/'tests/reverse_reference.py',DRIVER/'tests/native_file_versions.py']
    frozen={str(p):sha256(p) for p in inputs};archive=args.output.parent/'executed-inputs';archive.mkdir(exist_ok=False)
    for n,path in enumerate(inputs):shutil.copy2(path,archive/(str(n)+'-'+path.name))
    write_json(archive/'input-hashes.json',frozen)
    roles=None;db=None;control=None
    try:
        import psycopg2
        control=psycopg2.connect(host=str(ROOT/'.runtime/opengauss-native-lake/debug/socket'),port=55560,dbname='postgres',user=os.getenv('USER','qiumc'),connect_timeout=10)
        control.autocommit=True;roles=OwnedRoles(control,args.run_id,args.output.parent)
        with OwnedDatabase(args.run_id,args.output.parent) as db:
            t=Case(db,args,result,roles);execute(t);t.finish()
            t.check('test inputs remain unchanged',{str(p):sha256(p) for p in inputs},frozen)
        result['status']='pass'
    except Blocked as exc:result.update(status='blocked',reason=str(exc))
    except BaseException as exc:result.update(reason=str(exc),traceback=traceback.format_exc())
    finally:
        if db is not None:result['database_cleanup']=db.cleanup_result;result['artifacts'].extend(db.artifacts())
        if roles is not None:
            try:
                if db is not None and db.cleanup_result and db.cleanup_result['returncode']==0:result['role_cleanup']=cleanup_roles(control,roles.path,args.run_id)
                elif roles.items:raise RuntimeError('owned database cleanup unproven; role cleanup deferred')
            except BaseException as exc:result.update(status='fail',cleanup_error=str(exc))
            result=roles.redact(result)
        if control is not None:control.close()
        seen={entry['path'] for entry in result['artifacts']}
        for path in args.output.parent.rglob('*'):
            if path.is_file() and path!=args.output and path.name!='database-cleanup.json' and str(path) not in seen:
                result['artifacts'].append({'path':str(path)});seen.add(str(path))
        write_json(args.output,result);print(json.dumps({'status':result['status'],'output':str(args.output)}))
    return {'pass':0,'blocked':2}.get(result['status'],1)

if __name__=='__main__':raise SystemExit(main())
