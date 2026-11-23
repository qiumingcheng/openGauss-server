"""Exact ordinary row-head SQL26 regression admission; no Native execution here.

Fresh26 covers manual/worker/VAC-reuse/policy/relations. Historical18 and23
fixtures remain separate; the17-to18 migration is never credited to runtime26.
"""
import argparse
import json
from pathlib import Path
import shutil
import sys

from native_parent_sql026_admission import (D, expected_inputs, build_shape,
    verify_actual, pinned, digest, read, require)

MODES=('manual','auto','vacuum','policy','relations')
COMMON_QA=('native_parent_partition_mutation.py','native_row_head_reclaim_campaign.py',
    'native_parent_rowhead_adapter.py','native_parent_sql026_admission.py',
    'native_parent_sql026_expected.json','native_parent_rowhead_cases.py',
    'native_parent_rowhead_policy_cases.py','native_parent_rowhead_auto_cases.py',
    'native_parent_multirow_epq_case.py')
ORDINARY_QA=('native_row_head_reclaim.py','native_row_head_reclaim_policy.py',
    'native_row_head_reclaim_relations.py','native_row_head_sql026_admission.py')
FIELDS26=['binding_id','relation_oid','row_head_oid','reclaim_cursor','reclaim_round_end','reclaim_scanned',
    'reclaimed_rows','reclaim_rounds','last_reclaim_at','last_vacuum_at','live_rows_estimate','dead_rows_estimate',
    'head_bytes','soft_budget_exceeded','last_delete_xid','oldest_xmin_observed','leaf_partition_oid','partition_no']


def require_contract(c,mode,installed='0.26'):
    require(c.get('ready_for_native_qualification') is True and c.get('status')=='ready_for_tests_only',
        'Ordinary26 source/build/QA contract remains closed')
    e=expected_inputs()
    require(c.get('purpose')=='ordinary_rowhead_runtime26_regression','Wrong ordinary contract purpose')
    require(mode in MODES and mode in c.get('allowed_modes',[]),'Ordinary26 mode not admitted; migration is not26')
    require(installed==c.get('partition_sql_version')==c.get('runtime_version')=='0.26','Exact fresh26/runtime26 required')
    require(c.get('ordinary_rowhead_qualified_to_test') is True and c.get('known_blockers')==[],
        'Ordinary26 capability or actual inputs remain unqualified')
    for name in ('native_inputs','build_wrapper_inputs','native_contract_versions','product_composition_inputs',
                 'retained_native_inputs','shared_helper_inputs'):
        require(c.get(name)==e[name],'Exact ordinary26 source projection differs/'+name)
    require(c.get('product_composition_manifest_sha256')==e['product_manifest_sha256'] and
            c.get('product_composition_patch_sha256')==e['product_patch_sha256'],'Wrong reviewed product composition')
    require(c.get('review_evidence'),'Independent ordinary26 review required')
    for path,h in c['review_evidence'].items():require(digest(path)==h,'Independent review changed')
    names={'tests/'+n for n in COMMON_QA+ORDINARY_QA}
    require(set(c.get('qa_inputs',{}))==names,'Exact complete13 ordinary/typed QA inputs required')
    for name,h in c['qa_inputs'].items():require(digest(D/name)==h,'Actual admitted QA changed/'+name)
    return e


def early_gate(default_mode=None):
    """Before database-helper imports: prepare is pure, closed26 cannot run."""
    p=argparse.ArgumentParser(add_help=False)
    p.add_argument('--prepare',action='store_true');p.add_argument('--execute',action='store_true')
    p.add_argument('--mode',default=default_mode);p.add_argument('--sql-version',default='0.18')
    p.add_argument('--runtime-version',default='0.18');p.add_argument('--source-contract',type=Path)
    a,unknown=p.parse_known_args()
    if a.prepare:
        if a.execute or unknown or (a.mode is not None and a.mode not in MODES):p.error('Prepare only admits ordinary26 cases without runtime execution arguments')
        print(json.dumps(dict(status='prepared_not_executed',Native=0,SDK=0,fixtures=0,
            modes=[a.mode]if a.mode else list(MODES),ready_for_native_qualification=False)))
        raise SystemExit(0)
    if '0.26' not in (a.sql_version,a.runtime_version):return
    if not a.execute or a.sql_version!=a.runtime_version or not a.source_contract:p.error('Ordinary26 requires explicit --execute,26/26 and source contract')
    require_contract(read(a.source_contract),a.mode,a.sql_version)


def add_arguments(p):
    p.add_argument('--execute',action='store_true')
    p.add_argument('--source-contract',type=Path)


def require_actual(a,mode,built=None,campaign=False):
    c=read(a.source_contract);e=require_contract(c,mode,a.sql_version)
    require(a.execute and a.runtime_version=='0.26','Ordinary26 requires explicit execution/runtime26')
    full=pinned(c.get('actual_full_build'));build_shape(full,e,True)
    require(c.get('binary_sha256')==full['binary_sha256'] and c.get('source_diff_sha256')==full['source_diff_sha256'],
        'Contract actual full-build identity differs')
    sdk=c.get('actual_sdk_install');pinned(sdk)
    current=read(a.build_receipt);build_shape(current,e,campaign)
    for k in ('source_head','source_diff_sha256','binary_sha256','native_source_inputs_before','native_source_inputs_after',
              'build_wrapper_inputs_before','build_wrapper_inputs_after','roaring_inputs_before','roaring_inputs_after','pq_inputs_before','pq_inputs_after'):
        require(current.get(k)==full.get(k),'Ordinary child restart changed actual full-build input/'+k)
    if campaign:require(Path(a.build_receipt).resolve()==Path(c['actual_full_build']['path']).resolve(),'Campaign must start from exact actual full26 receipt')
    if built is not None:require(current==built,'Caller supplied different active receipt')
    actual=verify_actual(a.build_receipt,sdk['path'],full_required=campaign)
    return c,actual


def require_campaign(a,built):
    require(a.cases and len(a.cases)==len(set(a.cases)) and set(a.cases)<=set(MODES),'Explicit ordinary26 affected subset required')
    for mode in a.cases:require_contract(read(a.source_contract),mode,a.sql_version)
    return require_actual(a,a.cases[0],built,campaign=True)


def extra_inputs(a):
    c=read(a.source_contract)
    require_contract(c,getattr(a,'mode',None) or a.cases[0],a.sql_version)
    return [D/name for name in sorted(c['qa_inputs'])]+[Path(a.source_contract)]


def archive_receipts(out,c,actual):
    """Distinct filenames preserve full-build and restarted-build evidence."""
    directory=out/'ordinary-admission';directory.mkdir()
    result={'actual':actual,'receipts':{}}
    for name,key in (('full-build.json','actual_full_build'),('sdk-install.json','actual_sdk_install')):
        ref=c[key];pinned(ref);dest=directory/name;shutil.copy2(ref['path'],dest)
        require(digest(dest)==ref['sha256'],'Archived actual receipt changed')
        result['receipts'][key]={'path':str(dest),'sha256':digest(dest)}
    return result


def check_ordinary_layout(connection,query,check,relations):
    fields=[x[0]for x in query(connection,"SELECT attname FROM pg_attribute WHERE attrelid='og_lake_internal.row_lock_binding'::regclass AND attnum>0 AND NOT attisdropped ORDER BY attnum")]
    check('fresh SQL26 exact18 head fields',fields,FIELDS26)
    for relation in relations:
        check('ordinary mapping retains zero typed identity '+str(relation),query(connection,
            'SELECT leaf_partition_oid::bigint,partition_no FROM og_lake_internal.row_lock_binding WHERE relation_oid=%s',
            (relation,)),[(0,0)])


def verify_child_admission(result,contract_path,folder):
    """Used alongside the unchanged child assertion/DB/role cleanup checks."""
    c=read(contract_path);require_contract(c,result.get('mode'),result.get('sql_version'))
    evidence=result.get('ordinary26_admission',{});actual=evidence.get('actual',{})
    require(actual.get('binary_sha256')==c['binary_sha256'] and actual.get('source_diff_sha256')==c['source_diff_sha256'],
        'Ordinary child actual binary/source evidence differs')
    require(actual.get('actual_build')=={'path':result['build'],'sha256':result['build_sha256']},
        'Ordinary child actual build evidence differs from its executed receipt')
    require(actual.get('actual_sdk')==c['actual_sdk_install'],'Ordinary child SDK receipt differs')
    for key,name in (('actual_full_build','full-build.json'),('actual_sdk_install','sdk-install.json')):
        ref=evidence.get('receipts',{}).get(key);pinned(ref)
        require(Path(ref['path']).resolve()==(Path(folder)/'ordinary-admission'/name).resolve(),
            'Ordinary child receipt archive escaped exact owned folder')
        require(ref['sha256']==c[key]['sha256'],'Ordinary child archived wrong full-build/SDK receipt')
    return True
