#!/usr/bin/env python3
"""Review bounded ordinary IndexAM probes; this never qualifies lake ANN or PQ."""
import argparse
import datetime
import json
from pathlib import Path
import sys

D=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(D),str(D/'tests')]
from file_dml_acceptance import check_build
from lake_harness import sha256,write_json

LIFECYCLE={'actual_native_ann_plan','ten_ordered_candidates','reference_is_exact_scan',
    'initial_topk_distances','index_update_new_version','deleted_tuple_not_returned',
    'own_insert_visible','rolled_back_insert_absent','prepared_parameter_changes',
    'prepared_reuse_restores_result','vacuum_keeps_visible_population'}
CANCELLATION={'native_serial_default','timeout_returns_before_wall_budget',
    'cancel_keeps_committed_rows','cancel_rolls_back_index','same_connection_build_after_cancel',
    'large_build_was_actually_cancelled','actual_native_ann_plan','ten_ordered_candidates'}


ZERO={'actual_native_ann_plan','zero_norm_query_returns_no_rows','zero_norm_table_population','zero_norm_index_population','null_insert_skips_graph','zero_insert_skips_graph','first_nonzero_insert_seeds_graph','deleted_seed_is_not_a_result','unindexed_null_and_zero_rows_preserved'}


def require(value,reason):
    if not value:raise ValueError(reason)


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--build-receipt',type=Path,required=True)
    p.add_argument('--result',type=Path,action='append',required=True)
    p.add_argument('--negative',type=Path,action='append',default=[])
    p.add_argument('--output',type=Path,required=True);a=p.parse_args()
    require(not a.output.exists(),'Refuse to overwrite evidence')
    b=check_build(a.build_receipt);children=[];negatives=[];total=0
    seen=set()
    for path in a.result:
        path=path.resolve();r=json.loads(path.read_text())
        require(path not in seen,'Repeated evidence');seen.add(path)
        require(r['status']=='passed'and r['completed']and not r['timed_out'],'Probe did not finish successfully')
        require(r['process_exited']and r['shutdown_mode']=='fast','Probe did not stop normally')
        require(r['source_head']==b['source_head']and r['binary_sha256']==b['binary_sha256'],'Probe source/binary identity mismatch')
        require(r['main_binary_unchanged']and not r['pq'],'This reviewer qualifies only non-PQ reuse')
        require('Ann Index Scan using reused_diskann'in r['plan'],'Missing actual native ANN plan')
        require(len(r['rows'])==(0 if r.get('zero_only')else 10) and [x[1]for x in r['rows']]==sorted(x[1]for x in r['rows']),'Wrong result population/order')
        archive=path.parent/'executed-inputs';names=[]
        for name,digest in r['inputs'].items():
            stored=archive/Path(name).name
            require(stored.is_file()and sha256(stored)==digest,'Executed input archive missing/changed: '+name)
            require(stored.name not in names,'Ambiguous input archive');names.append(stored.name)
        copied=Path(r['install'])/'bin/gaussdb'
        require(copied.is_file()and sha256(copied)==r['binary_sha256'],'Isolated executable changed')
        owner=json.loads((Path(r['runtime'])/'owner.json').read_text())
        require(owner['project']=='opengauss-native-lake-isolated-probe'and owner['binary_sha256']==r['binary_sha256']and
                owner['port']==r['port']and owner['install']==r['install'],'Isolated ownership mismatch')
        checks=r['assertions'];actual={v['name']for v in checks}
        expected=ZERO if r.get('zero_only')else CANCELLATION if r['expect_cancel']else LIFECYCLE
        require(len(checks)==len(actual)and actual==expected and all(v['passed']for v in checks),'Missing/duplicate/failed assertions')
        if r['expect_cancel']:
            require(r['build_cancelled']and r['cancel_sqlstate']=='57014'and
                    r['cancel_elapsed_seconds']<r['wall_budget_seconds']-2,'Missing bounded real SQL cancellation')
        elif r.get('zero_only'):require(r['metric']=='cosine','Zero-only scope requires cosine')
        else:require(r['lifecycle'],'Lifecycle assertions need the executed lifecycle mode')
        children.append(dict(result=str(path),sha256=sha256(path),metric=r['metric'],rows=r['rows'],
            assertions=len(checks),cancelled=r['expect_cancel'],normal_cleanup=True))
        total+=len(checks)
    for path in a.negative:
        path=path.resolve();r=json.loads(path.read_text())
        require(r['status']=='failed'and r['process_exited'],'Negative run/cleanup not preserved')
        negatives.append(dict(result=str(path),sha256=sha256(path),source_head=r['source_head'],
            binary_sha256=r['binary_sha256'],timed_out=r.get('timed_out'),reason=r.get('error',r.get('reason'))))
    result=dict(status='passed',scope=__doc__,reviewed_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
        source_head=b['source_head'],binary_sha256=b['binary_sha256'],cases=len(children),assertions=total,
        native_inputs_verified=len(b['native_source_inputs_after']),build_receipt=str(a.build_receipt.resolve()),
        build_receipt_sha256=sha256(a.build_receipt),children=children,retained_negative_evidence=negatives,
        reviewer=str(Path(__file__).resolve()),reviewer_sha256=sha256(__file__),lake_ann_qualified=False,pq_qualified=False)
    archive=a.output.with_suffix('.reviewer.py');require(not archive.exists(),'Refuse to overwrite reviewer archive')
    archive.write_bytes(Path(__file__).read_bytes());result['reviewer_archive']=str(archive.resolve())
    write_json(a.output,result);print(json.dumps({k:result[k]for k in ['status','cases','assertions','source_head']},ensure_ascii=False))


if __name__=='__main__':main()
