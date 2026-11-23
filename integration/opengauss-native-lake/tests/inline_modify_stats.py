#!/usr/bin/env python3
"""Targeted pure-inline statistics, with independent actual reference at every COMMIT.

No standard statistics/snapshot row is fabricated or patched. ALL_NULL is a
fixed-reference normal CREATE + nonempty INSERT fixture, not an empty catalog.
"""
import argparse
import copy
import json
import os
from pathlib import Path
import sys
import traceback

WORKSPACE=Path('/home/qiumc/data/code/cpp')
DRIVER=Path(os.getenv('OG_LAKE_DRIVER_ROOT',str(WORKSPACE/'integration/opengauss-native-lake')))
sys.path.insert(0,str(DRIVER)); sys.path.insert(0,str(DRIVER/'tests'))
from inline_fixture import foreign_table_statement, multiset, query, scalar
from lake_harness import Blocked, validate_run_id, write_json
from native_test_context import OwnedDatabase, load_reference, reviewed_runtime_version
from reverse_modify_reference import reverse_modify_reference


class StatsCase:
    def __init__(self, db, args, result):
        from psycopg2 import sql
        self.sql,self.args,self.result=sql,args,result
        self.a,self.observer=db.connect(),db.connect()
        query(self.a,"CREATE EXTENSION og_lake VERSION '0.4'")
        caps=json.loads(scalar(self.a,'SELECT og_lake.host_status()'))
        runtime_version=reviewed_runtime_version()
        self.check('reviewed runtime supports existing N4',runtime_version in ('0.4','0.5','0.6'),True)
        self.check('runtime matches reviewed source',caps['module_version'],runtime_version)
        self.check('legacy N4 installation remains SQL 0.4',
                   scalar(self.a,"SELECT extversion FROM pg_extension WHERE extname='og_lake'"),'0.4')
        result['runtime_version']=runtime_version
        result['installed_extension_version']='0.4'
        self.manifest,self.exported,counts,artifacts=load_reference(self.a,args.manifest,args.run_id,args.output.parent)
        result['artifacts'].extend(artifacts)
        self.check('real fixture profile',self.manifest['case'],'ALL_NULL' if args.mode in ('all_null','all_null_insert') else 'UNKNOWN_BOUNDS' if args.mode=='unknown_bounds' else 'I')
        self.check('all standard reference tables imported',len(counts),len(self.exported['tables']))
        self.names=[c['name'] for c in self.manifest['schema']]
        self.expected=[{name:r[name] for name in self.names} for r in self.manifest['expected']]
        self.table_id=self.manifest['table_id']
        carriers=[r['table_name'] for t in self.exported['tables'] if t['table_name']=='ducklake_inlined_data_tables'
                  for r in t['rows'] if r['table_id']==self.table_id]
        self.check('one real inline carrier',len(carriers),1); self.carrier=carriers[0]
        query(self.a,'CREATE SCHEMA biz')
        query(self.a,"SELECT og_lake.attach_catalog('lake_reference','lake_metadata',NULL::text,false)")
        query(self.a,foreign_table_statement('biz','orders','lake_reference',self.table_id,self.manifest['schema']))
        self.groups=[]; self.heads=[]; self.checkpoints=[]
        self.initial=self.capture()
        self.check('actual fixture business values',multiset(self.initial['business']),multiset(self.expected))
        self.check('actual fixture is nonempty',self.initial['table']['record_count'],3)
        self.check('actual initial ID frontier',self.initial['table']['next_row_id'],3)
        if args.mode in ('all_null','all_null_insert','unknown_bounds'):
            if args.mode=='unknown_bounds':
                first=next(r for r in self.initial['business'] if r['id']==1)
                self.check('unknown bounds coexist with genuine false/zero/empty old values',
                           [first[n] for n in ('val','flag','bigval','note')],[0,False,0,''])
                self.check('effective skip_stats_columns has been normally cleared',
                           scalar(self.observer,"SELECT count(*) FROM lake_metadata.ducklake_metadata WHERE key='skip_stats_columns' AND scope='table' AND scope_id=%s AND value IS NOT NULL AND value<>''",(self.table_id,)),0)
            for name in ('val','flag','bigval','note'):
                actual=self.initial['columns'][name]
                self.check('actual initial '+name+' has NULL bounds',[actual['min_value'],actual['max_value']],[None,None])
                self.check('actual initial '+name+' contains NULL',actual['contains_null'],True)
        else:
            self.check('actual initial val bounds',self.bounds(self.initial,'val'),['10','200'])
            self.check('actual initial val has no NULL',self.initial['columns']['val']['contains_null'],False)
        write_json(args.output.parent/'initial-statistics.json',self.initial)
        result['artifacts'].append({'path':str(args.output.parent/'initial-statistics.json')})

    def check(self,label,actual,expected=True):
        ok=actual==expected
        self.result['assertions'].append({'name':label,'status':'pass' if ok else 'fail',
                                         'details':{'observed':actual,'expected':expected}})
        if not ok: raise AssertionError(label)

    def capture(self):
        names=['record_count','next_row_id','file_size_bytes']
        table=dict(zip(names,query(self.observer,'SELECT record_count,next_row_id,file_size_bytes '
                                  'FROM lake_metadata.ducklake_table_stats WHERE table_id=%s',(self.table_id,))[0]))
        snames=['snapshot_id','schema_version','next_catalog_id','next_file_id']
        head=dict(zip(snames,query(self.observer,'SELECT snapshot_id,schema_version,next_catalog_id,next_file_id '
                                  'FROM lake_metadata.ducklake_snapshot ORDER BY snapshot_id DESC LIMIT 1')[0]))
        fields=['column_name','contains_null','contains_nan','min_value','max_value','min_is_exact','max_is_exact']
        rows=query(self.observer,'SELECT c.column_name,s.contains_null,s.contains_nan,s.min_value::text,s.max_value::text,'
                   's.min_is_exact,s.max_is_exact FROM lake_metadata.ducklake_column c '
                   'JOIN lake_metadata.ducklake_table_column_stats s ON s.table_id=c.table_id AND s.column_id=c.column_id '
                   'WHERE c.table_id=%s AND c.end_snapshot IS NULL ORDER BY c.column_id',(self.table_id,))
        columns={row[0]:dict(zip(fields,row)) for row in rows}
        business=[dict(zip(self.names,row)) for row in query(self.observer,self.sql.SQL('SELECT {} FROM biz.orders ORDER BY id').format(
            self.sql.SQL(',').join(map(self.sql.Identifier,self.names))))]
        physical_names=['_ducklake_row_id','_ducklake_begin_snapshot','_ducklake_end_snapshot']+self.names
        physical=[dict(zip(physical_names,row)) for row in query(self.observer,self.sql.SQL('SELECT {} FROM lake_metadata.{}').format(
            self.sql.SQL(',').join(map(self.sql.Identifier,physical_names)),self.sql.Identifier(self.carrier)))]
        return {'table':table,'head':head,'columns':columns,'business':business,'physical':physical}

    @staticmethod
    def bounds(capture,name):
        return [capture['columns'][name]['min_value'],capture['columns'][name]['max_value']]

    def physical_oracle(self,before,expected,head):
        old_by_id={r['id']:r for r in before['business']}; new_by_id={r['id']:r for r in expected}
        physical=copy.deepcopy(before['physical']); frontier=before['table']['next_row_id']
        for identifier,old in old_by_id.items():
            if identifier not in new_by_id or new_by_id[identifier]!=old:
                source=next(r for r in physical if r['id']==identifier and r['_ducklake_end_snapshot'] is None)
                source['_ducklake_end_snapshot']=head
                if identifier in new_by_id:
                    physical.append(dict(new_by_id[identifier],_ducklake_row_id=source['_ducklake_row_id'],
                                         _ducklake_begin_snapshot=head,_ducklake_end_snapshot=None))
        # These controlled scenarios retain at most one fresh identity per
        # commit. Do not assume planner order for multi-row permanent-ID tests.
        additions=[r for r in expected if r['id'] not in old_by_id]
        if len(additions)>1: raise ValueError('Statistics fixture supports at most one surviving fresh identity per commit')
        for row in additions:
            physical.append(dict(row,_ducklake_row_id=frontier,_ducklake_begin_snapshot=head,_ducklake_end_snapshot=None))
            frontier+=1
        return physical

    def commit(self,label,operations,expected,new_versions,new_identities,publishes=True):
        before=self.capture()
        native_trace=[]
        query(self.a,'BEGIN')
        try:
            for operation in operations:
                # Controlled templates have parameters only at placeholders;
                # text payload is bound, never interpolated into SQL.
                statement=operation['sql'].replace('{table}','biz.orders').replace('?','%s')
                params=operation.get('params',[])
                returned=query(self.a,statement,params if params else None)
                native_trace.append({'sql':statement,'params':params,'returned':returned})
            query(self.a,'COMMIT')
        except BaseException:
            query(self.a,'ROLLBACK'); raise
        self.groups.append(operations); self.expected=copy.deepcopy(expected)
        observed=self.capture(); self.heads.append(observed['head']['snapshot_id'])
        directory=self.args.output.parent/('commit-%02d'%len(self.groups)); directory.mkdir()
        write_json(directory/'native-checkpoint.json',{'label':label,'before':before,'after':observed,'native_trace':native_trace,
                                                      'expected_business':expected,'new_versions':new_versions,'new_identities':new_identities})
        self.result['artifacts'].append({'path':str(directory/'native-checkpoint.json')})
        self.check(label+' business values',multiset(observed['business']),multiset(expected))
        self.check(label+' stored versions increment once',observed['table']['record_count'],before['table']['record_count']+new_versions)
        self.check(label+' only surviving new identities allocated',observed['table']['next_row_id'],before['table']['next_row_id']+new_identities)
        self.check(label+' one file identity per nonempty new inline batch',observed['head']['next_file_id'],before['head']['next_file_id']+(1 if new_versions else 0))
        self.check(label+' publication snapshot count',observed['head']['snapshot_id'],before['head']['snapshot_id']+(1 if publishes else 0))
        self.check(label+' schema identity unchanged',observed['head']['schema_version'],before['head']['schema_version'])
        self.check(label+' catalog identity frontier unchanged',observed['head']['next_catalog_id'],before['head']['next_catalog_id'])
        self.check(label+' physical versions are exactly the final business changes',multiset(observed['physical']),
                   multiset(self.physical_oracle(before,expected,observed['head']['snapshot_id'])))
        # Rebuild an independent reference copy from the real original, replay
        # the entire business transaction prefix, and compare the full catalog
        # NOW. A later commit cannot conceal this checkpoint's mismatch.
        reference=directory/'reference'
        try:
            reply=reverse_modify_reference(self.observer,'lake_metadata',self.args.manifest,
                self.args.output.parent/'reference-export-current.json',expected,reference,
                operations_by_commit=self.groups,expected_commit_snapshots=self.heads)
            self.check(label+' independent actual reference full-catalog differential',reply['status'],'pass')
            self.checkpoints.append({'label':label,'reference':str(reference/'result.json'),'status':reply['status']})
        finally:
            if reference.exists():
                self.result['artifacts'].extend({'path':str(p)} for p in reference.rglob('*') if p.is_file())
                if (reference/'result.json').exists():
                    self.result.setdefault('per_commit_reference_results',[]).append(
                        {'label':label,'path':str(reference/'result.json'),'status':json.loads((reference/'result.json').read_text())['status']})
        return observed


def op(statement,*params): return {'sql':statement,'params':list(params)}
def update(rows,identifier,**values): return [dict(r,**values) if r['id']==identifier else dict(r) for r in rows]


def all_null(t):
    wanted=update(t.expected,1,val=7,flag=True,bigval=9,note='甲')
    state=t.commit('all NULL old columns first gain real values',[
        op('UPDATE {table} SET val=?,flag=?,bigval=?,note=? WHERE id=1',7,True,9,'甲')],wanted,1,0)
    for name,value in [('val','7'),('flag','true'),('bigval','9'),('note','甲')]:
        t.check('restored '+name+' exact bounds',t.bounds(state,name),[value,value])
        t.check('restored '+name+' exactness',[state['columns'][name]['min_is_exact'],state['columns'][name]['max_is_exact']],[True,True])
        t.check('restored '+name+' keeps historical NULL presence',state['columns'][name]['contains_null'],True)
    wanted=update(wanted,1,val=None,flag=None,bigval=None,note=None)
    next_state=t.commit('nonNULL row returns to NULL',[
        op('UPDATE {table} SET val=NULL,flag=NULL,bigval=NULL,note=NULL WHERE id=1')],wanted,1,0)
    t.check('returning to NULL retains wider stored column bounds',next_state['columns'],state['columns'])
    deleted=t.commit('delete all remaining NULL rows',[op('DELETE FROM {table}')],[],0,0)
    t.check('pure delete retains all column statistics',deleted['columns'],next_state['columns'])
    wanted=[{'id':8,'val':8,'flag':False,'bigval':10,'note':''}]
    final=t.commit('new identity after last-row deletion',[
        op('INSERT INTO {table} VALUES(?,?,?,?,?)',8,8,False,10,'')],wanted,1,1)
    for name,bounds in [('val',['7','8']),('flag',['false','true']),('bigval',['9','10']),('note',['','甲'])]:
        t.check('reinsertion safely merges '+name+' bounds',t.bounds(final,name),bounds)



def all_null_insert(t):
    # A genuinely new identity tests exclusion of this commit's begin=S rows:
    # counting them as predecessor values would wrongly keep all bounds NULL.
    wanted=copy.deepcopy(t.expected)+[{'id':4,'val':7,'flag':True,'bigval':9,'note':'甲'}]
    state=t.commit('INSERT first nonNULL values into the nonempty all-NULL table',[
        op('INSERT INTO {table} VALUES(?,?,?,?,?)',4,7,True,9,'甲')],wanted,1,1)
    inserted=next(row for row in state['physical'] if row['id']==4)
    t.check('first nonNULL INSERT uses the original fresh identity frontier',
            inserted['_ducklake_row_id'],t.initial['table']['next_row_id'])
    for name,value in [('val','7'),('flag','true'),('bigval','9'),('note','甲')]:
        t.check('INSERT restores '+name+' bounds without counting its new row as old',
                t.bounds(state,name),[value,value])
        t.check('INSERT restores '+name+' exactness',
                [state['columns'][name]['min_is_exact'],state['columns'][name]['max_is_exact']],[True,True])
        t.check('INSERT preserves actual old NULL presence for '+name,state['columns'][name]['contains_null'],True)
    # A different pre-existing identity changes in a later transaction. Real
    # zero/false/empty values must merge with, rather than replace, known bounds.
    wanted=update(wanted,2,val=0,flag=False,bigval=0,note='')
    state=t.commit('old NULL identity becomes false zero and empty text',[
        op('UPDATE {table} SET val=?,flag=?,bigval=?,note=? WHERE id=2',0,False,0,'')],wanted,1,0)
    for name,bounds in [('val',['0','7']),('flag',['false','true']),('bigval',['0','9']),('note',['','甲'])]:
        t.check('later UPDATE merges '+name+' real nonNULL extremes',t.bounds(state,name),bounds)
        t.check('merged '+name+' bounds stay exact',
                [state['columns'][name]['min_is_exact'],state['columns'][name]['max_is_exact']],[True,True])
        t.check('merged '+name+' retains earlier NULL evidence',state['columns'][name]['contains_null'],True)
    t.check('later UPDATE preserves the old identity rather than consuming another ID',
            next(row['_ducklake_row_id'] for row in state['physical'] if row['id']==2 and row['_ducklake_end_snapshot'] is None),
            next(row['_ducklake_row_id'] for row in t.initial['physical'] if row['id']==2))


def unknown_bounds(t):
    wanted=update(t.expected,1,val=7,flag=True,bigval=9,note='甲')
    state=t.commit('unique old nonNULL ended by this UPDATE still forbids false recovery',[
        op('UPDATE {table} SET val=?,flag=?,bigval=?,note=? WHERE id=1',7,True,9,'甲')],wanted,1,0)
    for name in ('val','flag','bigval','note'):
        t.check('real old '+name+' unknown bounds are not replaced by the new batch',t.bounds(state,name),[None,None])
    wanted=[r for r in wanted if r['id']!=1]
    state=t.commit('remove the only live nonNULL row',[op('DELETE FROM {table} WHERE id=1')],wanted,0,0)
    for name in ('val','flag','bigval','note'):
        t.check('pure DELETE keeps '+name+' unknown',t.bounds(state,name),[None,None])
    wanted=update(wanted,2,val=7,flag=True,bigval=9,note='甲')
    state=t.commit('later predecessor truly contains only NULL values',[
        op('UPDATE {table} SET val=?,flag=?,bigval=?,note=? WHERE id=2',7,True,9,'甲')],wanted,1,0)
    for name,value in [('val','7'),('flag','true'),('bigval','9'),('note','甲')]:
        t.check('ended history does not block proven '+name+' NULL-only recovery',t.bounds(state,name),[value,value])


def private_extrema(t):
    wanted=update(t.expected,2,val=150)
    state=t.commit('overwritten private extremes are not published',[
        op('UPDATE {table} SET val=? WHERE id=2',2147483647),
        op('UPDATE {table} SET val=? WHERE id=2',-2147483648),
        op('UPDATE {table} SET val=150 WHERE id=2')],wanted,1,0)
    t.check('temporary numeric extremes do not broaden bounds',t.bounds(state,'val'),['10','200'])
    wanted=update(wanted,2,val=160)
    state=t.commit('cancelled new extreme alongside a surviving UPDATE',[
        op('INSERT INTO {table} VALUES(?,?)',9999,-2147483648),
        op('UPDATE {table} SET val=? WHERE id=9999',2147483647),
        op('DELETE FROM {table} WHERE id=9999'),op('UPDATE {table} SET val=160 WHERE id=2')],wanted,1,0)
    t.check('cancelled identity does not broaden ID bounds',t.bounds(state,'id'),['1','3'])
    t.check('cancelled identity does not broaden value bounds',t.bounds(state,'val'),['10','200'])
    wanted=update([r for r in wanted if r['id']!=2],1,val=80)
    state=t.commit('ended committed identity discards its private extreme',[
        op('UPDATE {table} SET val=? WHERE id=2',-2147483648),
        op('DELETE FROM {table} WHERE id=2'),op('UPDATE {table} SET val=80 WHERE id=1')],wanted,1,0)
    t.check('update then delete does not leak intermediate extreme',t.bounds(state,'val'),['10','200'])


def extrema_delete(t):
    wanted=update(t.expected,1,val=80)
    state=t.commit('unique live minimum moves inward',[op('UPDATE {table} SET val=80 WHERE id=1')],wanted,1,0)
    t.check('inward live minimum does not narrow stored bounds',t.bounds(state,'val'),['10','200'])
    wanted=update(wanted,3,val=180)
    state=t.commit('unique live maximum moves inward',[op('UPDATE {table} SET val=180 WHERE id=3')],wanted,1,0)
    t.check('inward live maximum does not narrow stored bounds',t.bounds(state,'val'),['10','200'])
    before=state
    wanted=[r for r in wanted if r['id']==2]
    state=t.commit('delete both former extremes',[op('DELETE FROM {table} WHERE id IN (1,3)')],wanted,0,0)
    t.check('deleting former extremes retains all stats',state['columns'],before['columns'])
    before=state
    state=t.commit('delete last visible row',[op('DELETE FROM {table} WHERE id=2')],[],0,0)
    t.check('last-row DELETE retains nonempty historical column stats',state['columns'],before['columns'])
    state=t.commit('reinsert after logical empty',[op('INSERT INTO {table} VALUES(8,80)')],[{'id':8,'val':80}],1,1)
    t.check('new row consumes old frontier rather than recycled zero',next(r['_ducklake_row_id'] for r in state['physical'] if r['id']==8),t.initial['table']['next_row_id'])
    t.check('reinsertion still preserves historical value bounds',t.bounds(state,'val'),['10','200'])


def null_direction(t):
    wanted=update(t.expected,2,val=150)
    state=t.commit('private NULL overwritten before commit',[
        op('UPDATE {table} SET val=NULL WHERE id=2'),op('UPDATE {table} SET val=150 WHERE id=2')],wanted,1,0)
    t.check('discarded private NULL does not set contains_null',state['columns']['val']['contains_null'],False)
    wanted=update(wanted,2,val=None)
    state=t.commit('first committed NULL replacement',[op('UPDATE {table} SET val=NULL WHERE id=2')],wanted,1,0)
    t.check('committed NULL sets contains_null',state['columns']['val']['contains_null'],True)
    wanted=update(wanted,2,val=160)
    state=t.commit('NULL replaced by nonNULL',[op('UPDATE {table} SET val=160 WHERE id=2')],wanted,1,0)
    t.check('historical NULL evidence is not lost',state['columns']['val']['contains_null'],True)
    t.check('NULL versions do not narrow stored bounds',t.bounds(state,'val'),['10','200'])


def netzero(t):
    state=t.commit('new identity fully cancelled',[
        op('INSERT INTO {table} VALUES(?,?)',9999,-2147483648),
        op('UPDATE {table} SET val=? WHERE id=9999',2147483647),op('DELETE FROM {table} WHERE id=9999')],t.expected,0,0,False)
    t.check('net-empty transaction preserves all column stats',state['columns'],t.initial['columns'])
    wanted=[r for r in t.expected if r['id']!=2]
    state=t.commit('committed UPDATE then DELETE has no stored replacement',[
        op('UPDATE {table} SET val=? WHERE id=2',-2147483648),op('DELETE FROM {table} WHERE id=2')],wanted,0,0)
    t.check('delete-only final state preserves column stats',state['columns'],t.initial['columns'])
    wanted=update(wanted,1,val=50)+[{'id':8,'val':80}]
    state=t.commit('mixed surviving INSERT and UPDATE with cancelled identity',[
        op('INSERT INTO {table} VALUES(8,80)'),op('INSERT INTO {table} VALUES(?,?)',9,-2147483648),
        op('UPDATE {table} SET val=? WHERE id=9',2147483647),op('DELETE FROM {table} WHERE id=9'),
        op('UPDATE {table} SET val=40 WHERE id=1'),op('UPDATE {table} SET val=50 WHERE id=1')],wanted,2,1)
    t.check('mixed final state excludes cancelled ID from max',t.bounds(state,'id'),['1','8'])
    t.check('mixed final state excludes cancelled value extremes',t.bounds(state,'val'),['10','200'])


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-id',default=os.getenv('OG_LAKE_RUN_ID'))
    parser.add_argument('--output',default=os.getenv('OG_LAKE_RESULT_FILE'))
    parser.add_argument('--manifest',default=os.getenv('OG_LAKE_FIXTURE_MANIFEST'))
    parser.add_argument('--mode',required=True,choices=['all_null','all_null_insert','unknown_bounds','private_extrema','extrema_delete','null_direction','netzero'])
    args=parser.parse_args()
    if not all((args.run_id,args.output,args.manifest)): parser.error('run-id/output/real manifest required')
    validate_run_id(args.run_id)
    args.output=Path(args.output).resolve(); args.output.parent.mkdir(parents=True,exist_ok=True)
    result={'status':'fail','scope':'N4/per_commit_statistics/'+args.mode,'assertions':[],'artifacts':[],
            'notes':['Every COMMIT is compared with a fresh independent fixed-reference replay of the transaction prefix.',
                     'Only snapshot_time is excluded by the existing differential helper; no statistics fields are repaired or omitted.',
                     'No file, schema evolution, floating point/NaN or RAC coverage is claimed.']}
    owned=None
    try:
        with OwnedDatabase(args.run_id,args.output.parent) as owned:
            case=StatsCase(owned,args,result); globals()[args.mode](case)
            result['status']='pass'
    except Blocked as exc: result.update(status='blocked',reason=str(exc))
    except BaseException as exc: result.update(status='fail',reason=str(exc),traceback=traceback.format_exc())
    finally:
        if owned is not None:
            result['artifacts'].extend(owned.artifacts()); result['database_cleanup']=owned.cleanup_result
        write_json(args.output,result)
        print(json.dumps({'status':result['status'],'mode':args.mode,'assertions':len(result['assertions']),'output':str(args.output)}))
    return {'pass':0,'blocked':2}.get(result['status'],1)


if __name__=='__main__': raise SystemExit(main())
