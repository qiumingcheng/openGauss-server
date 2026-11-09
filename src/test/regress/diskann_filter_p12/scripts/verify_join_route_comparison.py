#!/usr/bin/env python3
"""Check private route enumeration and complete native Sort/Limit cost comparison."""
import argparse,json,math,re
from pathlib import Path
from verify_query_protocol import connect

class Notices:
    def __init__(self):self.items=[]
    def append(self,value):self.items.append(value)

def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--output',type=Path,required=True)
    a=p.parse_args();a.output.mkdir(parents=True,exist_ok=False)
    conn=connect();cur=conn.cursor();notices=Notices();conn.notices=notices
    commands=[];checks=[]
    def sql(q):
        cur.execute(q);rows=cur.fetchall() if cur.description else []
        commands.append(dict(sql=q,rows=rows));return rows
    def check(name,ok,details):
        checks.append(dict(name=name,result='PASS' if ok else 'FAIL',details=details))
        if not ok:print('FAIL '+name+': '+str(details),flush=True)
    q="SELECT d.id,a.acl_group FROM vf_join_registration.documents d JOIN vf_join_registration.acl a ON d.bucket=a.bucket WHERE a.enabled ORDER BY d.embedding <-> '[7.25,1,2,3,4,5,6,7]'::vector LIMIT 10"
    try:
        sql('SET enable_diskann_filter=on; SET enable_opfusion=off; SET query_dop=1; SET diskann_probes=32; SET client_min_messages=debug2')
        for mode in ('auto','pre','in','post','exact'):
            sql("SET diskann_filter_mode='"+mode+"'");notices.items.clear()
            text='\n'.join(row[0] for row in sql('EXPLAIN(COSTS ON) '+q))+'\n'
            debug=''.join(notices.items)
            (a.output/(mode+'.plan.txt')).write_text(text);(a.output/(mode+'.planner.log')).write_text(debug)
            route_lines=[line for line in debug.splitlines() if 'DiskANN join route:' in line]
            cost_lines=[line for line in debug.splitlines() if 'DiskANN complete route cost:' in line]
            costs=[{k:float(v) for k,v in re.findall(r'(\w+)=([-+0-9.eE]+)',line)} for line in cost_lines]
            if mode=='exact':
                check('exact_independent_ordinary',not route_lines and not cost_lines and 'Filter Mode:' not in text,text)
                continue
            check(mode+'_complete_cost_per_private_route',len(costs)>=len(route_lines)>0,dict(routes=route_lines,costs=costs))
            check(mode+'_finite_complete_cost',all(all(math.isfinite(v) and v>=0 for v in row.values()) and row.get('complete',-1)>=row.get('input',0) for row in costs),costs)
            check(mode+'_real_sort_projection_cost',all(row.get('projection',0)>0 and row.get('sort_limit',0)>0 for row in costs),costs)
            check(mode+'_selected_plan_has_global_sort_limit',text.lstrip().startswith('Limit') and 'Sort Key:' in text and 'Custom Scan' not in text,text)
            if mode=='auto':
                check('auto_all_three_modes_enumerated',all(any('mode='+str(value)+' ' in line for line in route_lines) for value in (1,2,3)),route_lines)
                check('auto_exact_competes_on_full_cost',len(costs)>len(route_lines) and 'Filter Mode:' not in text,dict(costs=costs,plan=text))
            else:
                check(mode+'_forced_costs_remain_nonnegative',all(row['complete']>0 for row in costs) and 'Filter Mode: '+mode.upper() in text,costs)
    finally:
        result=dict(passed=sum(c['result']=='PASS' for c in checks),total=len(checks),checks=checks)
        (a.output/'summary.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
        (a.output/'commands.json').write_text(json.dumps(commands,ensure_ascii=False,indent=2)+'\n')
        cur.close();conn.close()
    failed=[c['name'] for c in checks if c['result']!='PASS']
    print(json.dumps(dict(result='FAIL' if failed else 'PASS',checks=len(checks),failed=failed)))
    if failed:raise SystemExit(1)

if __name__=='__main__':main()
