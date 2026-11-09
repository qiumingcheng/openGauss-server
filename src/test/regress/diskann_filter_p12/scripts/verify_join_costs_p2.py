#!/usr/bin/env python3
"""Audit native Join cost ownership using actual Path traces and EXPLAIN work."""
import argparse,json,math,re
from collections import Counter
from pathlib import Path
from verify_query_protocol import connect
from verify_join_route_comparison import Notices
p=argparse.ArgumentParser(description=__doc__);p.add_argument('--output',type=Path,required=True);a=p.parse_args();a.output.mkdir(parents=True,exist_ok=False)
c=connect();cur=c.cursor();notices=Notices();c.notices=notices;commands=[];checks=[]
def sql(q):
 cur.execute(q);r=cur.fetchall() if cur.description else [];commands.append(dict(sql=q,rows=r));return r
def check(name,ok,details):
 checks.append(dict(name=name,result='PASS' if ok else 'FAIL',details=details))
 if not ok:print('FAIL '+name+': '+str(details),flush=True)
def values(line):return {k:float(v) for k,v in re.findall(r'(\w+)=([-+0-9.eE]+)',line)}
base="SELECT /*+ HashJoin(d a) */ d.id,a.acl_group,d.body,a.payload FROM vf_join_cost_p2.documents d JOIN vf_join_cost_p2.acl a ON d.bucket=a.bucket WHERE a.enabled ORDER BY d.embedding <-> '[7.25,1,2,3,4,5,6,7]'::vector LIMIT 10"
def estimate(label,q,mode,analyze=True):
 sql("SET diskann_filter_mode='"+mode+"'");notices.items.clear()
 plan='\n'.join(r[0] for r in sql('EXPLAIN('+('ANALYZE,TIMING OFF,' if analyze else '')+'VERBOSE,COSTS ON'+(',BUFFERS' if analyze else '')+') '+q))+'\n';log=''.join(notices.items)
 (a.output/(label+'.plan.txt')).write_text(plan);(a.output/(label+'.planner.log')).write_text(log)
 routes=[values(l) for l in log.splitlines() if 'DiskANN join route:' in l]
 costs=[values(l) for l in log.splitlines() if 'DiskANN complete route cost:' in l]
 return dict(plan=plan,routes=routes,costs=costs)
def counter(t,label):
 x=re.search(r'^\s*'+re.escape(label)+r': (\d+)',t,re.M);return int(x[1]) if x else None
try:
 sql('SET enable_diskann_filter=on; SET diskann_filter_mode=exact; SET enable_opfusion=off; SET query_dop=1; SET diskann_probes=32; SET client_min_messages=debug2; SET work_mem=64; SET diskann_filter_work_mem=8192')
 check('owned_wide_fixture',sql("SELECT obj_description(oid,'pg_namespace') FROM pg_namespace WHERE nspname='vf_join_cost_p2'")==[('Owned by DiskANN query filter Q-01-P2 cost diagnostics',)],'separate owned 256 target/96 business rows; 4096-byte PLAIN columns')
 exact=estimate('exact_wide',base,'exact');reference=sql(base)
 check('ordinary_cost_is_independent',not exact['routes'] and not exact['costs'] and 'Filter Mode:' not in exact['plan'],exact['plan'])
 full=sql('SELECT count(*),count(DISTINCT d.id) FROM vf_join_cost_p2.documents d JOIN vf_join_cost_p2.acl a ON d.bucket=a.bucket WHERE a.enabled')[0]
 check('eligibility_vs_full_match_cardinality',full==(512,256),dict(F_exact=full[0],N_s=full[1]))
 for mode in ['pre','in','post']:
  e=estimate(mode+'_wide',base,mode);r=e['routes'];t=e['plan'];actual=sql(base)
  check(mode+'_strict_full_projection_bag',Counter(actual)==Counter(reference),dict(expected=reference,actual=actual))
  check(mode+'_finite_full_routes',bool(r) and all(all(math.isfinite(v) and v>=0 for v in x.values()) for x in r+e['costs']),e['routes'])
  check(mode+'_members_probes_not_k',bool(r) and all(x['members']==256 and x['probes']==(256 if mode=='in' else 0) for x in r),r)
  check(mode+'_all_matches_costed',bool(r) and all(abs(x['output_rows']-2*x['candidate_rows'])<1e-6 and x['output_rows']>10 for x in r),r)
  check(mode+'_native_sort_projection_once',bool(e['costs']) and all(x['projection']>0 and x['sort_limit']>0 and abs(x['complete']-x['input']-x['projection']-x['sort_limit'])<1e-5 for x in e['costs']),e['costs'])
  check(mode+'_actual_match_rows_independent',counter(t,'Qualified Target Rows')==32 and re.search(r'Hash Join.*actual rows=64 loops=1',t) and counter(t,'Search Rounds')==1,t)
  if mode=='pre':
   suffix=t.split('Search End Reason: configured beam completed',1)[1]
   lines=suffix.splitlines();first=next(i for i,l in enumerate(lines) if 'Hash Join' in l);depth=len(lines[first])-len(lines[first].lstrip());end=next((i for i in range(first+1,len(lines)) if lines[i].strip() and len(lines[i])-len(lines[i].lstrip())<depth),len(lines));subtree='\n'.join(lines[first:end])
   check('pre_private_inputs_are_narrow','d.body' not in subtree and 'a.payload' not in subtree and 'width=6)' in subtree and 'width=10)' in subtree and 'width=4)' in subtree,subtree)
   check('pre_qualification_hash_reduces_wide_spill',counter(t,'PRE Source Rows')==512 and re.search(r'Batches: ([12]) ',subtree) and 'Batches: 8' not in subtree,subtree)
   m=re.search(r'Hash Join\s+\(cost=([\d.]+)\.\.([\d.]+) rows=(\d+).*width=6',subtree)
   unit=float(sql('SHOW cpu_tuple_cost')[0][0]);expected=float(m[2])+int(m[3])*unit if m else None
   check('pre_native_source_once_and_all_insert_attempts',m and any(abs(x['qualification']-expected)<.02 for x in r),dict(expected_from_real_source=expected,routes=r))
   check('pre_membership_and_second_read_observable',counter(t,'Candidate Callbacks')>0 and counter(t,'AStore Fetch Attempts')==counter(t,'Visible Rows') and counter(t,'Qualification Probes')==0,t)
  elif mode=='in':
   subtree=t.split('SubPlan ',1)[1]
   check('in_probe_private_inputs_are_narrow','d.body' not in subtree and 'a.payload' not in subtree and re.search(r'Tid Scan.*width=4',subtree) and re.search(r'Index Scan.*width=4',subtree),subtree)
   m=re.search(r'Nested Loop\s+\(cost=([\d.]+)\.\.([\d.]+) rows=(\d+)',subtree)
   expected=256*(float(m[1])+(float(m[2])-float(m[1]))/max(int(m[3]),1)) if m else None
   check('in_cost_is_actual_exists_call_rule',m and any(abs(x['qualification']-expected)<2 for x in r),dict(expected_from_real_probe=expected,routes=r))
   loops=re.search(r'Tid Scan.*loops=(\d+)\)',subtree)
   check('in_probe_actual_loops_not_success_or_k',loops and int(loops[1])==counter(t,'Qualification Probes') and int(loops[1])>32 and counter(t,'AStore Fetch Attempts')>=int(loops[1]),t)
   check('in_no_unmeasured_cache_discount',all(x['probes']==x['members'] for x in r) and counter(t,'Probe Cache Hits')==0,r)
  else:
   check('post_no_private_qualification_bill',all(x['qualification']==0 and x['qualification_rows']==0 for x in r) and 'SubPlan' not in t and counter(t,'Qualification Probes')==0,r)
  big=estimate(mode+'_huge_k',base.replace('LIMIT 10','LIMIT 9223372036854775807'),mode,False)
  check(mode+'_k_does_not_change_qualification_budget',[(x['members'],x['probes'],x['qualification'],x['candidate_rows'],x['output_rows']) for x in big['routes']]==[(x['members'],x['probes'],x['qualification'],x['candidate_rows'],x['output_rows']) for x in r],big['routes'])
  empty=estimate(mode+'_empty',base.replace('WHERE a.enabled','WHERE a.enabled AND d.tenant=777'),mode)
  check(mode+'_zero_selectivity_finite_and_real_search',all(all(math.isfinite(v) and v>=0 for v in x.values()) for x in empty['routes']+empty['costs']) and 'Search Rounds: 1' in empty['plan'] and sql(base.replace('WHERE a.enabled','WHERE a.enabled AND d.tenant=777'))==[],empty['routes'])
 sql('SET diskann_filter_mode=exact')
 sql('CREATE OR REPLACE FUNCTION vf_join_cost_p2.allowed(boolean) RETURNS boolean LANGUAGE plpgsql STABLE COST 1 AS $$BEGIN RETURN $1; END$$')
 query=base.replace('WHERE a.enabled','WHERE vf_join_cost_p2.allowed(a.enabled)')
 cheap={m:estimate(m+'_cheap_function',query,m,False) for m in ['pre','in','post']}
 sql('ALTER FUNCTION vf_join_cost_p2.allowed(boolean) COST 1000')
 for m in ['pre','in','post']:
  high=estimate(m+'_expensive_function',query,m,False)
  check(m+'_predicate_cost_propagates_through_native_tree',min(x['complete'] for x in high['costs'])>min(x['complete'] for x in cheap[m]['costs']),dict(cheap=cheap[m]['costs'],high=high['costs']))
 sql('ALTER FUNCTION vf_join_cost_p2.allowed(boolean) COST 1')
 sql('SET diskann_filter_mode=exact; SET work_mem=8192')
 roomy=estimate('exact_wide_roomy',base,'exact')
 check('native_wide_output_hash_spill_cost',float(re.search(r'Limit\s+\(cost=([0-9]+\.[0-9]+)',exact['plan'])[1])>float(re.search(r'Limit\s+\(cost=([0-9]+\.[0-9]+)',roomy['plan'])[1]) and 'Batches: 8' in exact['plan'] and 'Batches: 1' in roomy['plan'],dict(low=exact['plan'],high=roomy['plan']))
 check('connection_reusable',sql('SELECT 1')==[(1,)],'SELECT 1')
finally:
 (a.output/'commands.json').write_text(json.dumps(commands,ensure_ascii=False,indent=2)+'\n')
 (a.output/'summary.json').write_text(json.dumps(dict(passed=sum(x['result']=='PASS' for x in checks),total=len(checks),checks=checks),ensure_ascii=False,indent=2)+'\n');cur.close();c.close()
failed=[x['name'] for x in checks if x['result']!='PASS'];print(json.dumps(dict(result='FAIL' if failed else 'PASS',checks=len(checks),failed=failed)),flush=True)
if failed:raise SystemExit(1)
