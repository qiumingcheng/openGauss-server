#!/usr/bin/env python3
"""Test native post-reduction join request metadata, without claiming join routes."""
import argparse
import json
from pathlib import Path
import re
import psycopg2
from verify_query_protocol import connect

SCHEMA='vf_join_registration'
MARKER='Owned by DiskANN query filter J-02 metadata tests'
ORDER=" ORDER BY d.embedding <-> '[7.25,1,2,3,4,5,6,7]'::vector LIMIT 10"


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--routes-connected',action='store_true',help='J-03+: validate metadata with real PRE routes rather than the historical registration-only capability gate')
    args=parser.parse_args();args.output.mkdir(parents=True,exist_ok=False)
    class Notices:
        def __init__(self):self.values=[]
        def append(self,value):self.values.append(value)
        def clear(self):self.values.clear()
        def __iter__(self):return iter(self.values)
    conn=connect();conn.notices=Notices();cur=conn.cursor();commands=[];checks=[]
    def sql(statement):
        conn.notices.clear()
        try:
            cur.execute(statement)
            rows=cur.fetchall() if cur.description else []
            commands.append(dict(sql=statement,rows=rows,notices=list(conn.notices)))
            return rows
        except psycopg2.Error as exc:
            commands.append(dict(sql=statement,error=str(exc),sqlstate=exc.pgcode,notices=list(conn.notices)))
            raise
    def check(name,condition,details):
        checks.append(dict(name=name,result='PASS' if condition else 'FAIL',details=details))
        if not condition:print('FAIL '+name+': '+str(details),flush=True)
    def metadata():
        messages=''.join(conn.notices)
        found=re.search(r'DiskANN filter request: target_rti=(\d+) vector_attno=(\d+) qualification_relids=\(i ([\d ]+)\) special_joins=(\d+) lateral_dependencies=(\S+)',messages)
        return dict(target_rti=int(found[1]),vector_attno=int(found[2]),relations=list(map(int,found[3].split())),special_joins=int(found[4]),lateral_dependencies=found[5]) if found else None
    def plan(label,query):
        rows=sql('EXPLAIN(COSTS OFF) '+query)
        text='\n'.join(row[0] for row in rows)+'\n'
        (args.output/(label+'.plan.txt')).write_text(text)
        return text
    def registered(label,query,target,domain,special=None):
        sql("SET diskann_filter_mode='auto'")
        text=plan(label,query);actual=metadata()
        check(label+'_domain',actual is not None and actual['target_rti']==target and actual['relations']==domain and (special is None or actual['special_joins']==special),dict(metadata=actual,query=query))
        # This task only registers metadata. Forced joins must report the
        # pending derivation capability, rather than publish a false scan plan.
        sql("SET diskann_filter_mode='pre'")
        if args.routes_connected:
            text=plan(label+'_forced',query)
            actual=metadata()
            check(label+'_connected_route',actual is not None and actual['target_rti']==target and actual['relations']==domain and 'Filter Mode: PRE' in text,dict(metadata=actual,plan=text))
            return text
        try:
            plan(label+'_forced',query)
        except psycopg2.Error as exc:
            actual=metadata()
            check(label+'_pending_route',exc.pgcode=='0A000' and 'owned join route derivation is not connected yet' in str(exc) and actual is not None,dict(error=str(exc),metadata=actual))
        else:
            check(label+'_pending_route',False,'join route unexpectedly claimed by registration task')
        return text
    def rejected(label,query,reason):
        sql("SET diskann_filter_mode='pre'")
        try:plan(label,query)
        except psycopg2.Error as exc:
            check(label+'_forced',exc.pgcode=='0A000' and reason in str(exc) and metadata() is None,str(exc))
        else:check(label+'_forced',False,'unsafe shape accepted')
        sql("SET diskann_filter_mode='auto'")
        text=plan(label+'_auto',query)
        check(label+'_ordinary_auto','Filter Mode: PRE' not in text and 'Filter Mode: IN' not in text and 'Filter Mode: POST' not in text,text)
    try:
        sql('SET enable_diskann_filter=off; SET diskann_filter_mode=exact; SET enable_opfusion=off; SET query_dop=1')
        owner=sql(f"SELECT obj_description(oid,'pg_namespace') FROM pg_namespace WHERE nspname='{SCHEMA}'")
        if owner and owner!=[(MARKER,)]:raise RuntimeError('refuse unrelated existing fixture')
        if not owner:
            sql('CREATE SCHEMA '+SCHEMA)
            sql(f"COMMENT ON SCHEMA {SCHEMA} IS '{MARKER}'")
            sql(f'CREATE TABLE {SCHEMA}.documents(id int PRIMARY KEY,bucket int NOT NULL,tenant int NOT NULL,embedding vector(8) NOT NULL CHECK(vector_norm(embedding)>0)) WITH(storage_type=astore)')
            sql(f"INSERT INTO {SCHEMA}.documents SELECT i,mod(i,16),mod(i,7),('['||i||',1,2,3,4,5,6,7]')::vector FROM generate_series(1,256)i")
            sql(f'CREATE INDEX registration_diskann ON {SCHEMA}.documents USING diskann(embedding vector_l2_ops) WITH(index_size=16)')
            sql(f'CREATE INDEX registration_bucket ON {SCHEMA}.documents(bucket)')
            sql(f'CREATE TABLE {SCHEMA}.acl(acl_group int,bucket int,enabled boolean NOT NULL,PRIMARY KEY(acl_group,bucket)) WITH(storage_type=astore)')
            sql(f'INSERT INTO {SCHEMA}.acl SELECT g,b,mod(g+b,3)<>0 FROM generate_series(1,3)g,generate_series(0,31)b')
            sql(f'CREATE INDEX registration_acl_bucket ON {SCHEMA}.acl(bucket)')
            sql(f'CREATE TABLE {SCHEMA}.unique_acl(bucket int PRIMARY KEY) WITH(storage_type=astore)')
            sql(f'INSERT INTO {SCHEMA}.unique_acl SELECT i FROM generate_series(0,31)i')
            sql(f'CREATE TABLE {SCHEMA}.flags(bucket int PRIMARY KEY,enabled boolean NOT NULL) WITH(storage_type=astore)')
            sql(f'INSERT INTO {SCHEMA}.flags SELECT i,mod(i,3)<>0 FROM generate_series(0,31)i')
        for table in ('documents','acl','unique_acl','flags'):sql('ANALYZE '+SCHEMA+'.'+table)
        for table,count in [('documents',256),('acl',96),('unique_acl',32),('flags',32)]:
            if sql('SELECT count(*) FROM '+SCHEMA+'.'+table)!=[(count,)]:
                raise RuntimeError('owned fixed fixture changed; do not overwrite')
        if sql(f"SELECT count(*) FROM {SCHEMA}.documents WHERE bucket<>mod(id,16) OR tenant<>mod(id,7) OR embedding<>('['||id||',1,2,3,4,5,6,7]')::vector")!=[(0,)]:
            raise RuntimeError('owned fixed target values changed')
        sql('SET enable_diskann_filter=on; SET diskann_probes=256; SET client_min_messages=debug2')
        doc=SCHEMA+'.documents';acl=SCHEMA+'.acl';unique=SCHEMA+'.unique_acl';flags=SCHEMA+'.flags'
        for mode in ('pre','in','post'):
            sql(f"SET diskann_filter_mode='{mode}'")
            text=plan('single_'+mode,'SELECT d.id FROM '+doc+' d WHERE d.tenant=3'+ORDER)
            actual=metadata()
            check('single_'+mode,actual is not None and actual['target_rti']==1 and actual['relations']==[1] and 'Filter Mode: '+mode.upper() in text,dict(metadata=actual,plan=text))
        registered('unique_inner',f'SELECT d.id FROM {doc} d JOIN {unique} a ON d.bucket=a.bucket'+ORDER,1,[1,2],0)
        registered('nonunique_inner',f'SELECT d.id FROM {doc} d JOIN {acl} a ON d.bucket=a.bucket'+ORDER,1,[1,2],0)
        registered('business_projection',f'SELECT d.id,a.acl_group FROM {doc} d JOIN {acl} a ON d.bucket=a.bucket WHERE d.tenant=3 AND a.enabled'+ORDER,1,[1,2],0)
        outer=f'SELECT d.id,a.acl_group FROM {doc} d LEFT JOIN {acl} a ON d.bucket=a.bucket AND a.enabled WHERE a.bucket>=3 OR a.bucket IS NULL'+ORDER
        text=registered('preserved_outer_on_where',outer,1,[1,2],1)
        check('outer_on_where_sites','Left Join' in text and 'a.bucket IS NULL' in text and 'enabled' in text,text)
        registered('preserved_right',f'SELECT d.id,a.acl_group FROM {acl} a RIGHT JOIN {doc} d ON d.bucket=a.bucket'+ORDER,2,[1,2],1)
        registered('semi',f'SELECT d.id FROM {doc} d WHERE EXISTS(SELECT 1 FROM {acl} a WHERE d.bucket=a.bucket AND a.enabled)'+ORDER,1,[1,2],1)
        registered('anti',f'SELECT d.id FROM {doc} d WHERE NOT EXISTS(SELECT 1 FROM {acl} a WHERE d.bucket=a.bucket AND a.enabled)'+ORDER,1,[1,2],1)
        registered('three_relations',f'SELECT d.id,a.acl_group FROM {doc} d JOIN {acl} a ON d.bucket=a.bucket JOIN {flags} b ON b.bucket=a.bucket WHERE b.enabled'+ORDER,1,[1,2,4],0)
        registered('target_last',f'SELECT d.id FROM {acl} a JOIN {flags} b ON b.bucket=a.bucket JOIN {doc} d ON d.bucket=b.bucket WHERE b.enabled'+ORDER,4,[1,2,4],0)
        registered('same_table_alias',f'SELECT d.id FROM {doc} x JOIN {doc} d ON d.bucket=x.bucket WHERE x.tenant=3'+ORDER,2,[1,2],0)
        rejected('nullable_target',f'SELECT d.id FROM {acl} a LEFT JOIN {doc} d ON d.bucket=a.bucket'+ORDER,'without a real output vector target TID')
        rejected('full_target',f'SELECT d.id FROM {doc} d FULL JOIN {acl} a ON d.bucket=a.bucket'+ORDER,'without a real output vector target TID')
        registered('outer_reduced_inner',f'SELECT d.id FROM {acl} a LEFT JOIN {doc} d ON d.bucket=a.bucket WHERE d.id IS NOT NULL'+ORDER,2,[1,2],0)
        sql("SET diskann_filter_mode='pre'")
        removed=plan('unused_unique_outer',f'SELECT d.id FROM {doc} d LEFT JOIN {unique} a ON d.bucket=a.bucket'+ORDER)
        actual=metadata()
        check('removed_business_not_in_domain',actual is not None and actual['relations']==[1] and 'Filter Mode: PRE' in removed and 'Join' not in removed,dict(metadata=actual,plan=removed))
        registered('native_not_in_conversion',f'SELECT d.id FROM {doc} d WHERE d.bucket NOT IN(SELECT a.bucket FROM {acl} a)'+ORDER,1,[1,3],1)
        rejected('residual_correlated_subplan',f'SELECT d.id FROM {doc} d WHERE EXISTS(SELECT 1 FROM {acl} a WHERE a.bucket=d.bucket OFFSET d.tenant LIMIT 1)'+ORDER,'query shape')
        registered('external_parameter',f'SELECT ARRAY(SELECT d.id FROM {doc} d JOIN {acl} a ON d.bucket=a.bucket WHERE d.tenant=x.n'+ORDER+') FROM (VALUES(1),(3),(1))x(n)',1,[1,2],0)
        sql("SET diskann_filter_mode='auto'")
        illegal=f'SELECT d.id FROM LATERAL(SELECT d.bucket) a CROSS JOIN {doc} d'+ORDER
        try:plan('native_invalid_cyclic_reference',illegal)
        except psycopg2.Error as exc:
            check('native_invalid_cyclic_reference',exc.pgcode=='42P01',dict(sqlstate=exc.pgcode,error=str(exc)))
        else:check('native_invalid_cyclic_reference',False,'forward cyclic reference accepted')
    finally:
        (args.output/'commands.json').write_text(json.dumps(commands,ensure_ascii=False,indent=2)+'\n')
        (args.output/'summary.json').write_text(json.dumps(dict(passed=sum(c['result']=='PASS' for c in checks),total=len(checks),checks=checks,scope='request registration only; join mode derivation still pending J-03/J-04'),ensure_ascii=False,indent=2)+'\n')
        cur.close();conn.close()
    failed=[c['name'] for c in checks if c['result']!='PASS']
    print(json.dumps(dict(result='FAIL' if failed else 'PASS',checks=len(checks),failed=failed),ensure_ascii=False))
    if failed:raise SystemExit(1)


if __name__=='__main__':main()
