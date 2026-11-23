"""Native0.9 read checks against genuinely generated DuckLake file versions.

The old SQL0.8 rejection cases remain in native_file_read.py. These cases opt
into SQL0.9 and require its reviewed runtime capability declaration.
"""
import json
from inline_fixture import query,scalar,foreign_table_statement
from native_test_context import reviewed_runtime_version

def execute_versions(t):
    mode=t.args.mode
    caps=json.loads(scalar(t.a,'SELECT og_lake.host_status()'))
    t.check('exact reviewed file-version runtime',caps['module_version'],reviewed_runtime_version())
    t.check('native version/position capability is declared',
            [caps.get('local_file_versions'),caps.get('local_file_position_deletes')],[True,True])
    if mode=='versions_upgrade':
        t.check('retained SQL0.8 before upgrade',scalar(t.a,"SELECT extversion FROM pg_extension WHERE extname='og_lake'"),'0.8')
        t.reject('old SQL still rejects actual version/delete input',
                 foreign_table_statement('biz','orders','lake_reference',t.manifest['table_id'],t.manifest['schema']),'0A000')
        query(t.a,'BEGIN');query(t.a,"ALTER EXTENSION og_lake UPDATE TO '0.9'");t.bind()
        t.check('transactional upgrade reads exact current rows',t.rows(),t.expected)
        query(t.a,'ROLLBACK')
        t.check('rollback restores old SQL gate',scalar(t.a,"SELECT extversion FROM pg_extension WHERE extname='og_lake'"),'0.8')
        t.check('rolled back host binding does not remain',scalar(t.a,'SELECT count(*) FROM og_lake_internal.table_binding'),0)
        query(t.a,"ALTER EXTENSION og_lake UPDATE TO '0.9'")
    t.bind()
    t.check('native versioned rows equal independent reference',t.rows(),t.expected)
    t.check('no stale duplicate business IDs',query(t.a,'SELECT count(*),count(DISTINCT id) FROM biz.orders'),[(len(t.expected),len(t.expected))])
    t.check('reader closes all data and delete inputs',len(t.descriptors()),0)
    if mode=='versions_inline_update':
        t.check('updated inline version replaces deleted file position',query(t.a,'SELECT id,val FROM biz.orders WHERE id=1'),[(1,11)])
        marker=t.sql.SQL('lake_metadata.{}').format(t.sql.Identifier('ducklake_inlined_delete_'+str(t.manifest['table_id'])))
        for assignment in ['row_id=999999','row_id=NULL','file_id=NULL','begin_snapshot=NULL']:
            query(t.a,'BEGIN');query(t.a,t.sql.SQL('UPDATE {} SET '+assignment).format(marker))
            t.reject('corrupt inline file deletion rejects before rows: '+assignment,'SELECT * FROM biz.orders','XX001')
            query(t.a,'ROLLBACK');t.check('rollback restores exact deletion visibility: '+assignment,t.rows(),t.expected)
    elif mode=='versions_inline_delete':
        t.check('deleted file row is absent',query(t.a,'SELECT * FROM biz.orders WHERE id=1'),[])
    elif mode=='versions_explicit':
        t.check('actual physical file contains five versions',len(t.manifest['physical_positions']),5)
        t.check('actual standard position deletes retain old identity versions',
                [r['_ducklake_internal_row_id']for r in t.manifest['physical_positions']],[0,0,0,1,2])
        t.check('two stale physical versions are excluded',query(t.a,'SELECT val FROM biz.orders WHERE id=1'),[(30,)])
    elif mode=='versions_batch':
        t.check('actual flushed physical population spans seven batches',len(t.manifest['physical_positions']),28672)
        t.check('actual position deletes span four batches',len(t.manifest['versioned_delete_rows']),16384)
        query(t.a,'BEGIN');query(t.a,'DECLARE versions NO SCROLL CURSOR FOR SELECT id,val FROM biz.orders')
        found=[]
        for size in [5000,5000,5000]:found.extend(query(t.a,'FETCH '+str(size)+' FROM versions'))
        query(t.a,'CLOSE versions');query(t.a,'COMMIT')
        t.check('cursor filters versions and deletes across actual batch boundaries',sorted(found),t.expected)
        t.check('versioned cursor releases every SDK input',len(t.descriptors()),0)
    elif mode=='versions_delete_errors':
        for name,table,assignment,state in [
            ('zero count must still validate footer','ducklake_delete_file','delete_count=0','XX001'),
            ('wrong delete count','ducklake_delete_file','delete_count=delete_count+1','XX001'),
            ('invalid delete size','ducklake_delete_file','file_size_bytes=1','XX001'),
            ('wrong deletion snapshot maximum','ducklake_delete_file','partial_max=0','XX001'),
            ('Puffin remains explicitly unsupported','ducklake_delete_file',"format='puffin'",'0A000'),
            ('absolute delete path remains unsupported','ducklake_delete_file','path_is_relative=false','0A000'),
            ('wrong data birth maximum','ducklake_data_file','partial_max=0','XX001')]:
            query(t.a,'BEGIN');query(t.a,'UPDATE lake_metadata.'+table+' SET '+assignment)
            t.reject(name,'SELECT * FROM biz.orders',state)
            query(t.a,'ROLLBACK');t.check(name+' rollback preserves visible versions',t.rows(),t.expected)
            t.check(name+' closes every actual input',len(t.descriptors()),0)
        for statement in ['INSERT INTO biz.orders VALUES(99,99)','UPDATE biz.orders SET val=val+1','DELETE FROM biz.orders WHERE false']:
            t.reject('file-table DML remains gated: '+statement,statement,'0A000')
    elif mode=='versions_rescan':
        query(t.a,'SET enable_material=off');query(t.a,'SET enable_hashjoin=off');query(t.a,'SET enable_mergejoin=off')
        statement='SELECT g.n,o.id,o.val FROM generate_series(1,3) g(n) CROSS JOIN LATERAL (SELECT id,val FROM biz.orders WHERE id>=g.n OFFSET 0) o ORDER BY 1,2'
        t.check('parameterized version/deletion rescans',query(t.a,statement),[(n,i,v)for n in range(1,4)for i,v in t.expected if i>=n])
        plan='\n'.join(row[0]for row in query(t.a,'EXPLAIN (ANALYZE,COSTS OFF) '+statement))
        (t.args.output.parent/'versioned-rescan-plan.txt').write_text(plan+'\n')
        t.check('actual versioned reader rescans occurred','loops=3' in plan,True)
        t.check('rescans release owned source descriptors',len(t.descriptors()),0)
    elif mode=='versions_acl':
        name=t.roles.create('version_reader',True)
        for statement in ['GRANT USAGE ON SCHEMA biz TO {}','GRANT USAGE ON FOREIGN SERVER lake_reference TO {}','GRANT SELECT ON biz.orders TO {}']:
            query(t.a,t.sql.SQL(statement).format(t.sql.Identifier(name)))
        login,identity=t.roles.connect(t.db,'version_reader');t.result['ordinary_identity']=identity
        t.check('ordinary LOGIN sees exact filtered versions',t.rows(login),t.expected)
        t.reject('ordinary LOGIN cannot read private delete metadata','SELECT * FROM lake_metadata.ducklake_delete_file','42501',login)
