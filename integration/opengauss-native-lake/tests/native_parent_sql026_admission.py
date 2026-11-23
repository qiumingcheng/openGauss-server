"""Exact SQL26 typed QA admission; stdlib until receipt shapes reject old builds.

No SQL, fixture, service, configuration or SDK installation is performed here.
Actual checks read the official receipt, source, installed/running ELF and SDK.
A closed pending contract always rejects before importing runtime helpers.
"""
import hashlib
import json
import os
from pathlib import Path
import re
import sys

EXPECTED_SHA256='5bb72503d15b637291b04f7f70d6856df8fcddcb4bdac82bc911945085498d06'
MANUAL=('rowhead-identity','rowhead-busy-fairness','rowhead-rollback','rowhead-bounded')
POLICY=('rowhead-policy-legacy25','rowhead-policy-access','rowhead-policy-readonly-binding','rowhead-policy-dml','rowhead-policy-flush','rowhead-policy-create')
AUTO=('rowhead-auto','rowhead-vacuum-reuse')
ROOT=Path(os.getenv('CPP_WORKSPACE','/home/qiumc/data/code/cpp')).resolve()
D=ROOT/'integration/opengauss-native-lake'
SOURCE=ROOT/'.worktrees/opengauss-native-lake'


def require(value,message):
    if not value:raise RuntimeError(message)

def digest(path):
    h=hashlib.sha256()
    with Path(path).open('rb')as f:
        for block in iter(lambda:f.read(1048576),b''):h.update(block)
    return h.hexdigest()

def read(path):return json.loads(Path(path).read_text())
def pinned(record):
    require(isinstance(record,dict) and set(record)=={'path','sha256'},'Missing exact actual receipt reference')
    require(digest(record['path'])==record['sha256'],'Referenced receipt bytes changed')
    return read(record['path'])

def expected_inputs():
    path=Path(__file__).with_name('native_parent_sql026_expected.json')
    require(digest(path)==EXPECTED_SHA256,'Reviewed exact SQL26 projection changed')
    return read(path)

def build_shape(build,expected,full_required):
    require((build.get('status'),build.get('profile'))==('build_and_sql_passed','debug'),'Successful official Debug build required')
    require(build.get('mode') in (('full',) if full_required else ('full','restart')),'SQL26 needs a full build or its actual unchanged official restart')
    require(build.get('source_head')==expected['source_head'],'Wrong source branch')
    require(re.fullmatch('[0-9a-f]{64}',build.get('binary_sha256','')) is not None,'Missing actual binary hash')
    for suffix in ('before','after'):
        require(build.get('native_source_inputs_'+suffix)==expected['native_inputs'],'Entire actual233 Native inventory differs/'+suffix)
        require(build.get('build_wrapper_inputs_'+suffix)==expected['build_wrapper_inputs'],'Entire actual6 wrappers differ/'+suffix)


def verify_sdk_install(build,sdk_receipt_path,expected):
    """Read-only SDK owner/payload verification; reject owner25 for target26."""
    install=read(sdk_receipt_path);sdk=Path(build['install'])/'lib/postgresql/og_lake_sdk'
    want=expected['sdk_expected']
    require(install['script_sha256']==expected['shared_helper_inputs']['sdk/install_sql020_native.py'],'Wrong SQL26 SDK installer')
    require((install['status'],install['phase'],install['committed'],install['qualification_kind'])==('installed','committed',True,want['kind']),'Actual committed SDKowner26 required')
    require(install['target']==str(sdk),'SDK install belongs to another target')
    require(not sdk.is_symlink()and not any(p.is_symlink()for p in sdk.rglob('*')),'SDK tree must not contain symlinks')
    ownerpath=sdk/'owner.json';require(digest(ownerpath)==install['intended']['owner_sha256'],'Actual SDK owner bytes differ')
    owner=read(ownerpath);require(owner==install['intended']['owner'],'Actual SDK owner differs from atomic install intent')
    require((owner['target'],owner['project'],owner['phase'],owner['qualification_kind'])==(str(sdk),'opengauss-native-lake-local-provider','prepared-for-atomic-install',want['kind']),'Unexpected SDK owner fields')
    files={str(p.relative_to(sdk)):digest(p)for p in sdk.rglob('*')if p.is_file()and p!=ownerpath}
    require(owner['files']==files==install['intended']['files'],'SDK full payload differs')
    require(owner['native_public_headers']==want['native_public_headers'],'SDK owner must bind actual control26 and public headers')
    for path,h in want['native_public_headers'].items():require(digest(path)==h==install['frozen_inputs'][path],'SDK public input differs/'+path)
    from pq_sql020_build_inputs import sdk_index
    qual=sdk_index(Path(want['qualification_index']),want['qualification_index_sha256'],ROOT)
    require(install['qualification']==qual,'SDK qualified provider receipt differs')
    require((owner['qualification_sha256'],files['libog_lake_sdk_arrow.so'],files['include/og_lake/lake_parquet_sdk.h'])==(qual['sha256'],want['library_sha256'],want['header_sha256']),'SDK qualified payload identity differs')
    return dict(actual_sdk=dict(path=str(Path(sdk_receipt_path).resolve()),sha256=digest(sdk_receipt_path)),
                actual_owner=dict(path=str(ownerpath),sha256=digest(ownerpath)))


def verify_installed_sql(build,expected):
    """Check the actual extension files used by fresh26 and legacy25 fixtures."""
    installed={}
    directory=Path(build['install'])/'share/postgresql/extension'
    for name in ('og_lake.control','og_lake--0.25.sql','og_lake--0.26.sql'):
        path=directory/name;wanted=expected['native_inputs']['contrib/og_lake/'+name]
        require(digest(path)==wanted,'Actual installed extension input differs/'+name)
        installed[str(path)]=wanted
    return installed


def verify_actual(build_path,sdk_receipt_path,full_required=True):
    """Reject SQL25 and projected-only inputs before importing active helpers."""
    expected=expected_inputs();build=read(build_path)
    build_shape(build,expected,full_required)
    for name,wanted in (expected['admission_support_inputs']|expected['shared_helper_inputs']).items():
        require(not Path(name).is_absolute()and '..'not in Path(name).parts,'Unsafe helper path')
        require(digest(D/name)==wanted,'Required applied helper differs/'+name)
    sys.path[:0]=[str(D),str(D/'tests')]
    from file_dml_acceptance import check_build
    from build_driver import native_source_inputs,build_wrapper_inputs,roaring_inputs,selected_pq_inputs
    require(check_build(Path(build_path))==build,'Official running ELF/runtime verification differs')
    installed_sql=verify_installed_sql(build,expected)
    require(native_source_inputs()==expected['native_inputs'],'Actual discovered233 source inventory differs')
    require(build_wrapper_inputs()==expected['build_wrapper_inputs'],'Actual discovered6 wrappers differ')
    require(build.get('running_binary_sha256')==build['binary_sha256'],'Running ELF receipt differs')
    baseline=pinned(expected['source_baseline_build'])
    require(build['build_entry_sha256']==baseline['build_entry_sha256'],'Official build entry changed')
    require(digest(ROOT/'deliverables/multimodal_optimizer/opengauss_build_install_restart.sh')==build['build_entry_sha256'],'Actual official entry changed')
    for name,loader in [('roaring',lambda path:roaring_inputs(path)),('pq',lambda path:selected_pq_inputs(path,ROOT))]:
        dependency=baseline[name+'_inputs_after'];actual=loader(Path(dependency['receipt']))
        require(actual==dependency,'Qualified dependency changed/'+name)
        for suffix in ('before','after'):require(build[name+'_inputs_'+suffix]==actual,'Build dependency differs/'+name+'/'+suffix)
    sdk_identity=verify_sdk_install(build,sdk_receipt_path,expected)
    return dict(status='actual_identity_verified_tests_not_run',actual_build=dict(path=str(Path(build_path).resolve()),sha256=digest(build_path)),
                **sdk_identity,installed_sql_inputs=installed_sql,source_head=build['source_head'],
                source_diff_sha256=build['source_diff_sha256'],binary_sha256=build['binary_sha256'],Native=0)


def require_contract(contract,mode,installed):
    # This is deliberately the first check: false-ready never reads runtime or
    # imports database helpers, even if future source/hash fields are populated.
    require(contract.get('ready_for_native_qualification') is True and contract.get('status')=='ready_for_tests_only','SQL26 source/build/QA contract remains closed')
    expected=expected_inputs()
    require(contract.get('purpose')=='native_parent_rowhead_maintenance_qualification','Wrong typed contract purpose')
    family='manual' if mode in MANUAL else 'policy' if mode in POLICY else 'auto' if mode in AUTO else None
    require(family is not None and contract.get('case_family')==family,'Wrong typed case family')
    require(mode in contract.get('allowed_modes',[]),'Typed mode not admitted by contract')
    target='0.25'if mode=='rowhead-policy-legacy25'else'0.26'
    require(installed==target==contract.get('partition_sql_version') and contract.get('runtime_version')=='0.26','Exact runtime26 / installed SQL mode mismatch')
    require(contract.get('known_blockers')==[],'Typed contract has unresolved admission blockers')
    for name in ['native_inputs','build_wrapper_inputs','native_contract_versions','product_composition_inputs','retained_native_inputs','shared_helper_inputs']:
        require(contract.get(name)==expected[name],'Exact typed contract projection differs/'+name)
    require(contract.get('product_composition_manifest_sha256')==expected['product_manifest_sha256'] and contract.get('product_composition_patch_sha256')==expected['product_patch_sha256'],'Wrong reviewed23 product composition')
    require(contract.get('epq_context_lifetime_fix_patch_sha256')=='7ce63fef4c75c12096367b763ae41ee25df8663215af184ced0fdc5d0e9de215','EPQ repair provenance missing')
    require(contract.get('row_movement_fixture_fix_patch_sha256')=='4ea97d41326cc7674683c9f17cd7f518facba37bd84a41b6b9f192e916027782','Explicit fixture row movement fix provenance missing')
    flags=['waiter_absolute_deadline_qualified_to_test']
    flags+=['typed_rowhead_policy_qualified_to_test']if family=='policy'else['typed_rowhead_qualified_to_test']
    if family=='auto':flags+=['typed_worker_qualified_to_test','typed_worker_head_only']
    require(all(contract.get(flag)is True for flag in flags),'Typed case capability remains unqualified')
    require(contract.get('review_evidence') and contract.get('qa_inputs'),'Exact independent review/QA seals required')
    required_qa={'tests/'+n for n in ['native_parent_partition_mutation.py','native_row_head_reclaim_campaign.py','native_parent_rowhead_adapter.py','native_parent_sql026_admission.py','native_parent_sql026_expected.json','native_parent_rowhead_cases.py','native_parent_rowhead_policy_cases.py','native_parent_rowhead_auto_cases.py','native_parent_multirow_epq_case.py']}
    require(set(contract['qa_inputs'])==required_qa,'Exact complete9 QA inputs required')
    for path,h in contract['review_evidence'].items():require(digest(path)==h,'Independent review changed')
    for relative,h in contract['qa_inputs'].items():
        require(not Path(relative).is_absolute()and '..'not in Path(relative).parts,'Unsafe QA path')
        require(digest(D/relative)==h,'Actual admitted QA input changed/'+relative)
    for name,key in [('native_parent_rowhead_cases.py','typed_rowhead_sidecar_sha256'),('native_parent_rowhead_policy_cases.py','typed_rowhead_policy_sidecar_sha256'),('native_parent_rowhead_auto_cases.py','typed_rowhead_auto_sidecar_sha256')]:
        require(contract.get(key)==contract['qa_inputs'].get('tests/'+name),'Sidecar contract hash differs/'+name)
    return expected


def require_fixture_admission(args,built=None):
    contract=read(args.source_contract);expected=require_contract(contract,args.mode,args.sql_version)
    full=pinned(contract.get('actual_full_build'));build_shape(full,expected,True)
    require(contract.get('binary_sha256')==full['binary_sha256'] and contract.get('source_diff_sha256')==full['source_diff_sha256'],'Contract actual full-build identity differs')
    sdk=contract.get('actual_sdk_install');pinned(sdk)
    current=read(args.build_receipt);build_shape(current,expected,args.mode not in AUTO)
    # Automatic children use an official restart receipt emitted after enabling
    # maintenance; its source, dependencies and ELF must equal the full build.
    for key in ['source_head','source_diff_sha256','binary_sha256','native_source_inputs_before','native_source_inputs_after','build_wrapper_inputs_before','build_wrapper_inputs_after','roaring_inputs_before','roaring_inputs_after','pq_inputs_before','pq_inputs_after']:
        require(current.get(key)==full.get(key),'Child restart changed full-build input/'+key)
    if built is not None:require(current==built,'Caller passed a different active receipt')
    require(str(Path(args.sdk_qualification).resolve())==str(Path(expected['sdk_expected']['qualification_index']).resolve()) and args.sdk_index_sha256==expected['sdk_expected']['qualification_index_sha256'],'Wrong SDK qualification arguments')
    actual=verify_actual(args.build_receipt,sdk['path'],full_required=args.mode not in AUTO)
    return contract,actual


def require_campaign_admission(args,built):
    require(args.sql_version==args.runtime_version=='0.26' and args.cases and set(args.cases)<=set(AUTO),'Only exact typed26 automatic subset admitted')
    contract=read(args.source_contract)
    for mode in args.cases:require_contract(contract,mode,args.sql_version)
    expected=expected_inputs();full=pinned(contract.get('actual_full_build'));build_shape(full,expected,True)
    require(Path(args.build_receipt).resolve()==Path(contract['actual_full_build']['path']).resolve() and built==full,'Automatic campaign starts from its exact actual full26 receipt')
    require(contract.get('binary_sha256')==full['binary_sha256'] and contract.get('source_diff_sha256')==full['source_diff_sha256'],'Contract full-build identity differs')
    sdk=contract.get('actual_sdk_install');pinned(sdk)
    require(str(Path(args.sdk_qualification).resolve())==str(Path(expected['sdk_expected']['qualification_index']).resolve()) and args.sdk_index_sha256==expected['sdk_expected']['qualification_index_sha256'],'Wrong SDK qualification arguments')
    return verify_actual(args.build_receipt,sdk['path'],full_required=True)
