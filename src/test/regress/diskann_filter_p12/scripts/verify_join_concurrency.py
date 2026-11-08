#!/usr/bin/env python3
"""Check joined projections under real AStore versions, errors and budgets."""
import argparse,hashlib,json,re,threading,time
from collections import Counter
from pathlib import Path
from verify_query_protocol import connect
p=argparse.ArgumentParser(description=__doc__);p.add_argument('--output',type=Path,required=True);p.add_argument('--rounds',type=int,default=24);a=p.parse_args();a.output.mkdir(parents=True,exist_ok=False)
sc='vf_p2_concurrent_'+hashlib.sha256(str(a.output.resolve()).encode()).hexdigest()[:8]
c=connect();cur=c.cursor();commands=[];checks=[];errors=[];writes=[]
def sql(q,cc=cur):
 cc.execute(q);r=cc.fetchall() if cc.description else [];commands.append(dict(sql=q,rows=r));return r
def check(name,ok,details):
 checks.append(dict(name=name,result='PASS' if ok else 'FAIL',details=details))
 if not ok:print('FAIL '+name,flush=True)
def setup(cc):sql("SET enable_diskann_filter=on; SET diskann_filter_mode=exact; SET enable_opfusion=off; SET query_dop=1; SET diskann_probes=2048; SET work_mem='16MB'; SET diskann_filter_work_mem=32768; SET statement_timeout=30000",cc)
def mode(m,cc=cur):sql("SET diskann_filter_mode='"+m+"'",cc)
order=" ORDER BY d.v <-> '[16.25,1,2,3,4,5,6,7]'::vector LIMIT 12"
qs={
 'inner':f'SELECT /*+ Leading((d a)) HashJoin(d a) */ d.id,d.bucket,d.payload,a.acl_group,a.payload FROM {sc}.documents d JOIN {sc}.acl a ON d.bucket=a.bucket WHERE a.enabled'+order,
 'semi':f'SELECT d.id,d.bucket,d.payload FROM {sc}.documents d WHERE EXISTS(SELECT 1 FROM {sc}.acl a WHERE a.bucket=d.bucket AND a.enabled)'+order,
 'anti':f'SELECT d.id,d.bucket,d.payload FROM {sc}.documents d WHERE NOT EXISTS(SELECT 1 FROM {sc}.acl a WHERE a.bucket=d.bucket AND a.acl_group=1 AND a.bucket<6 AND a.enabled)'+order,
 'far_semi':f"SELECT d.id,d.bucket,d.payload FROM {sc}.documents d WHERE EXISTS(SELECT 1 FROM {sc}.acl a WHERE a.bucket=d.bucket AND a.acl_group=3 AND a.enabled)"+order.replace('[16.25,','[505.25,'),
 'outer':f'SELECT /*+ Leading((d a)) HashJoin(d a) */ d.id,d.bucket,d.payload,a.acl_group,a.payload FROM {sc}.documents d LEFT JOIN {sc}.acl a ON d.bucket=a.bucket AND a.acl_group=3 AND a.enabled WHERE a.bucket>=5 OR a.bucket IS NULL'+order}
def compare(label,cc=cur,plans=False):
 for name,q in qs.items():
  mode('exact',cc);ref=sql(q,cc)
  for m in ['pre','in','post','auto']:
   mode(m,cc);got=sql(q,cc);check(label+'_'+name+'_'+m,Counter(got)==Counter(ref) and [x[0] for x in got]==[x[0] for x in ref],dict(reference=ref,actual=got))
   if plans and m!='auto':
    t='\n'.join(x[0] for x in sql('EXPLAIN(ANALYZE,COSTS OFF,TIMING OFF) '+q,cc))+'\n';(a.output/(label+'_'+name+'_'+m+'.plan.txt')).write_text(t);check(label+'_'+name+'_'+m+'_real_ann',t.count('Ann Index Scan using')==1 and 'Executed Mode: '+m.upper() in t and 'Search Rounds: 1' in t,t)
try:
 setup(cur)
 sql('CREATE SCHEMA '+sc);sql(f"COMMENT ON SCHEMA {sc} IS 'Owned by DiskANN query filter V-01-P2 concurrent test'")
 sql(f'CREATE TABLE {sc}.documents(id int PRIMARY KEY,bucket int NOT NULL,payload text NOT NULL,v vector(8) NOT NULL) WITH(storage_type=astore,fillfactor=60)')
 sql(f"INSERT INTO {sc}.documents SELECT i,mod(i,17),'original-'||i,('['||i||',1,2,3,4,5,6,7]')::vector FROM generate_series(1,1024)i")
 sql(f'CREATE INDEX p2_concurrent_diskann ON {sc}.documents USING diskann(v vector_l2_ops) WITH(index_size=16)')
 sql(f'CREATE TABLE {sc}.acl(acl_group int,bucket int,enabled bool,payload text,PRIMARY KEY(acl_group,bucket)) WITH(storage_type=astore,fillfactor=60)')
 sql(f"INSERT INTO {sc}.acl SELECT g,b,true,'business-'||g||'-'||b FROM generate_series(1,3)g,generate_series(0,16)b")
 sql(f'CREATE INDEX p2_concurrent_bucket ON {sc}.acl(bucket)');sql('ANALYZE '+sc+'.documents');sql('ANALYZE '+sc+'.acl')
 sql('BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY');compare('initial',plans=True);sql('COMMIT')
 # Pin an old statement view while a real second session changes both sides,
 # indexes new/old vector identities, deletes a near target and vacuums.
 sql('BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY');mode('exact');old=sql(qs['inner'])
 w=connect();wc=w.cursor();setup(wc)
 try:
  for statement in [f"UPDATE {sc}.documents SET payload='hot-sixteen' WHERE id=16",f"UPDATE {sc}.documents SET v='[1000.25,1,2,3,4,5,6,7]'::vector,payload='vector-changed' WHERE id=16",f'DELETE FROM {sc}.documents WHERE id=17',f"INSERT INTO {sc}.documents VALUES(5000,1,'new-near','[16.1,1,2,3,4,5,6,7]'::vector)",f"UPDATE {sc}.acl SET enabled=false,payload='changed-business' WHERE acl_group=3 AND bucket=16",f'VACUUM {sc}.documents',f'VACUUM {sc}.acl']:
   sql(statement,wc);writes.append(statement)
 finally:wc.close();w.close()
 compare('old_snapshot_after_both_sides_dml',plans=True);mode('exact');check('pinned_original_full_rows',sql(qs['inner'])==old,old);sql('COMMIT')
 sql('BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY');compare('new_snapshot',plans=True);mode('exact');new=sql(qs['inner']);check('new_view_changes_actual_projection',Counter(new)!=Counter(old) and any(x[0]==5000 for x in new),dict(old=old,new=new));sql('COMMIT')
 stop=threading.Event()
 def writer():
  w=connect();cc=w.cursor();i=0
  try:
   setup(cc)
   while not stop.is_set():
    i+=1;statements=[f"UPDATE {sc}.acl SET enabled={'true' if i%2 else 'false'},payload='concurrent-{i}' WHERE acl_group=3 AND bucket={8+i%5}",f"UPDATE {sc}.acl SET payload='near-business-{i}' WHERE acl_group=2 AND bucket=1",f"UPDATE {sc}.documents SET bucket=mod(bucket+1,17),payload='far-target-{i}' WHERE id={500+i%20}",f"UPDATE {sc}.documents SET payload='target-{i}' WHERE id={10+i%20}"]
    if i%4==0:statements.append(f"UPDATE {sc}.documents SET v=('[{35+i%20}.125,1,2,3,4,5,6,7]')::vector WHERE id={40+i%20}")
    if i%5==0:statements.extend([f'VACUUM {sc}.documents',f'VACUUM {sc}.acl'])
    for statement in statements:cc.execute(statement);writes.append(statement)
    stop.wait(.02)
  except Exception as exc:errors.append(str(exc))
  finally:cc.close();w.close()
 thread=threading.Thread(target=writer,daemon=True);thread.start()
 try:
  for i in range(a.rounds):
   sql('BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY');compare('concurrent_'+str(i));sql('COMMIT')
 finally:stop.set();thread.join(timeout=30)
 check('concurrent_writer_finished',not thread.is_alive() and not errors and len(writes)>7,dict(errors=errors,writes=len(writes)))
 sql('VACUUM '+sc+'.documents');sql('VACUUM '+sc+'.acl');sql('BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY');compare('after_vacuum');sql('COMMIT')
 mode('exact');sql(f"CREATE FUNCTION {sc}.fail_if(int) RETURNS boolean LANGUAGE plpgsql STABLE AS $$BEGIN IF $1>0 THEN RAISE EXCEPTION 'p2 controlled qualification failure'; END IF; RETURN true; END$$")
 sql(f"CREATE FUNCTION {sc}.slow(int) RETURNS boolean LANGUAGE plpgsql STABLE AS $$BEGIN PERFORM pg_sleep(.001); RETURN $1>0; END$$")
 for m in ['pre','in','post']:
  mode(m);fault=qs['inner'].replace('WHERE a.enabled',f'WHERE a.enabled AND {sc}.fail_if(d.id)')
  try:sql(fault)
  except Exception as exc:check(m+'_error_propagated',getattr(exc,'pgcode',None)=='P0001' and 'controlled qualification failure' in str(exc),str(exc))
  else:check(m+'_error_propagated',False,'query succeeded')
  check(m+'_error_connection_reuse',sql('SELECT 1')==[(1,)],'SELECT 1')
  sql('SET statement_timeout=100');slow=qs['inner'].replace('WHERE a.enabled',f'WHERE a.enabled AND {sc}.slow(d.id)')
  try:sql(slow)
  except Exception as exc:check(m+'_timeout',getattr(exc,'pgcode',None)=='57014',str(exc))
  else:check(m+'_timeout',False,'query succeeded')
  sql('SET statement_timeout=30000');check(m+'_timeout_reuse',sql('SELECT 1')==[(1,)],'SELECT 1')
  sql('SET diskann_filter_work_mem=16')
  try:sql(qs['inner'])
  except Exception as exc:check(m+'_bounded_filter_memory',getattr(exc,'pgcode',None)=='54000',str(exc))
  else:check(m+'_bounded_filter_memory',False,'accepted incomplete budget')
  sql('SET diskann_filter_work_mem=32768');check(m+'_budget_reuse',sql('SELECT 1')==[(1,)],'SELECT 1')
finally:
 (a.output/'commands.json').write_text(json.dumps(commands,ensure_ascii=False,indent=2)+'\n');(a.output/'writes.json').write_text(json.dumps(writes,indent=2)+'\n');(a.output/'summary.json').write_text(json.dumps(dict(schema=sc,passed=sum(x['result']=='PASS' for x in checks),total=len(checks),checks=checks,writer_errors=errors),ensure_ascii=False,indent=2)+'\n');cur.close();c.close()
failed=[x['name'] for x in checks if x['result']!='PASS'];print(json.dumps(dict(result='FAIL' if failed else 'PASS',checks=len(checks),failed=failed)),flush=True)
if failed:raise SystemExit(1)
