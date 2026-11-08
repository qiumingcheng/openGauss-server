#!/usr/bin/env python3
import argparse,json
from pathlib import Path
from verify_query_protocol import connect
from verify_join_route_comparison import Notices
p=argparse.ArgumentParser();p.add_argument('--output',type=Path,required=True);a=p.parse_args();a.output.mkdir(parents=True,exist_ok=False)
c=connect();s=c.cursor();n=Notices();c.notices=n;commands=[]
def sql(q):
 s.execute(q);r=s.fetchall() if s.description else [];commands.append(dict(sql=q,rows=r));return r
try:
 sql('SET enable_diskann_filter=on; SET diskann_filter_mode=exact; SET enable_opfusion=off; SET query_dop=1')
 owner='Owned by DiskANN query filter Q-01-P2 cost diagnostics'
 found=sql("SELECT obj_description(oid,'pg_namespace') FROM pg_namespace WHERE nspname='vf_join_cost_p2'")
 if found and found!=[(owner,)]:raise RuntimeError('unrelated schema')
 if not found:
  sql('CREATE SCHEMA vf_join_cost_p2');sql("COMMENT ON SCHEMA vf_join_cost_p2 IS '"+owner+"'")
 if not sql("SELECT oid FROM pg_class WHERE relnamespace=(SELECT oid FROM pg_namespace WHERE nspname='vf_join_cost_p2') AND relname='documents'"):
  sql('CREATE TABLE vf_join_cost_p2.documents(id int PRIMARY KEY,bucket int NOT NULL,tenant int NOT NULL,embedding vector(8) NOT NULL,body text) WITH(storage_type=astore)')
  sql('ALTER TABLE vf_join_cost_p2.documents ALTER COLUMN body SET STORAGE PLAIN')
  sql("INSERT INTO vf_join_cost_p2.documents SELECT id,bucket,tenant,embedding,repeat('x',4096) FROM vf_join_registration.documents")
  sql('CREATE INDEX vf_join_cost_diskann ON vf_join_cost_p2.documents USING diskann(embedding vector_l2_ops)')
  sql('CREATE TABLE vf_join_cost_p2.acl(acl_group int,bucket int,enabled bool,payload text,PRIMARY KEY(acl_group,bucket)) WITH(storage_type=astore)')
  sql('ALTER TABLE vf_join_cost_p2.acl ALTER COLUMN payload SET STORAGE PLAIN')
  sql("INSERT INTO vf_join_cost_p2.acl SELECT acl_group,bucket,enabled,repeat('y',4096) FROM vf_join_registration.acl")
  sql('CREATE INDEX vf_join_cost_bucket ON vf_join_cost_p2.acl(bucket)')
 sql('ANALYZE vf_join_cost_p2.documents');sql('ANALYZE vf_join_cost_p2.acl')
 sql('SET diskann_probes=32; SET client_min_messages=debug2; SET work_mem=64; SET diskann_filter_work_mem=8192')
 q="SELECT /*+ HashJoin(d a) */ d.id,a.acl_group,d.body,a.payload FROM vf_join_cost_p2.documents d JOIN vf_join_cost_p2.acl a ON d.bucket=a.bucket WHERE a.enabled ORDER BY d.embedding <-> '[7.25,1,2,3,4,5,6,7]'::vector LIMIT 10"
 for m in ['exact','pre','in','post']:
  sql("SET diskann_filter_mode='"+m+"'");n.items.clear();t='\n'.join(r[0] for r in sql('EXPLAIN (ANALYZE,VERBOSE,COSTS ON,TIMING OFF) '+q));(a.output/(m+'.plan.txt')).write_text(t+'\n');(a.output/(m+'.planner.log')).write_text(''.join(n.items))
finally:
 (a.output/'commands.json').write_text(json.dumps(commands,ensure_ascii=False,indent=2)+'\n');s.close();c.close()
