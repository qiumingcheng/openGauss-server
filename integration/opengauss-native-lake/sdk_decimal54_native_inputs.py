"""Additive exact SDK54/PQ54 metadata admission; no provider load or build.

The reviewed checker modules keep their original __file__ and receipt SHA.
Only their import names are temporarily bound during this serial CLI call;
original modules are restored even on failure. No successful proof is cached.
"""
import contextlib,hashlib,importlib.util,json,os,sys,threading
from pathlib import Path
ROOT=Path('/home/qiumc/data/code/cpp')
SDK_KIND='verified_independent_sdk_decimal128_only'
PQ_KIND='pq_decimal54_v1'
SDK_SOURCES={'sdk/lake_parquet_sdk.h':'6dbe78174680de7830ed45cfee957fefa38fc3d5dda0429b04e99436da28e759','vendor/arrow20/abi.h':'1828f1ce60368cd590d19595910fa19db09b30728f8a43b5f89933623f7a7276','sdk/lake_temporal_codec.h':'07879fc21faf4d6fd4c7e3dab7f531f7050b7216c5774841016a9024c2a43a87'}
SDK_DIR=ROOT/'.codex_tmp/native_lake_decimal54_reader_repair_full_qualification_source_1115_01'
PQ_DIR=ROOT/'.codex_tmp/native_lake_decimal54_reader_repair_PQ_source_1115_01'
O=ROOT/'.codex_tmp/native_lake_create_literal_default49_acceptance_review_1113_01'
SDK_MANIFEST={'path':str(SDK_DIR/'manifest.json'),'sha256':'e2ba141225cb050b0ebff23186b64f61bd903d5106262a5f0213c4990a7d09f5'}
SDK_READER={'path':str(SDK_DIR/'sdk_decimal54_inputs.py'),'sha256':'d8fbf014a25b2e44797e9ac73f116578dd6cbcad89bd0f0e0844a03b413714fb'}
SDK_REVIEW={'path':str(O/'Decimal54-rfix-qualifier-source-review.json'),'sha256':'326c45bc28c2525d4ed5c03b28b43eef08718f164b3d70d78e667f1548e03120'}
PQ_MANIFEST={'path':str(PQ_DIR/'manifest.json'),'sha256':'3ea6c761835004f95b49596718838af239ff64becfb4f202cb2b81330fcd5e2e'}
PQ_READER={'path':str(PQ_DIR/'stage_guard.py'),'sha256':'ce6c69e7dd3ad8d9a6638292fc140c8fb2f2e8c20727f76c95d6acee0bf46906'}
PQ_REVIEW={'path':str(O/'Decimal54-rfix-PQ54-stage-review.json'),'sha256':'f6961011b061d53cee06937aa8af8850879034ac87f972fdc9cac19543b32d04'}
_PQ_SELECTION=None


def digest(path):
    h=hashlib.sha256()
    with Path(path).open('rb')as f:
        for block in iter(lambda:f.read(1024*1024),b''):h.update(block)
    return h.hexdigest()


def exact(value,pins=None):
    if type(value)is not dict or set(value)!={'path','sha256'}:raise RuntimeError('Exact decimal54 descriptor required')
    p=Path(value['path'])
    if not p.is_absolute()or p.resolve(strict=True)!=p or p.is_symlink()or not p.is_file()or digest(p)!=value['sha256']:raise RuntimeError('Decimal54 Source/evidence changed/'+str(p))
    if pins is not None:
        if str(p)in pins and pins[str(p)]!=value['sha256']:raise RuntimeError('Conflicting decimal54 proof source')
        pins[str(p)]=value['sha256']
    return p


def _package(manifest,review,reader,pins):
    m=json.loads(exact(manifest,pins).read_text());peer=json.loads(exact(review,pins).read_text());exact(reader,pins)
    if m.get('reader')!=reader or peer.get('status')!='pass'or peer.get('blocking_findings')!=[]or peer.get('reviewed_manifest')!=manifest or peer.get('reviewed_reader')!=reader:raise RuntimeError('Exact independent complete decimal54 Source approval required')
    for name,h in m['artifacts'].items():
        rel=Path(name)
        if rel.is_absolute()or '..'in rel.parts:raise RuntimeError('Unsafe reviewed decimal54 artifact')
        exact(dict(path=str(Path(manifest['path']).parent/rel),sha256=h),pins)
    return m


def control_inputs():
    pins={};_package(SDK_MANIFEST,SDK_REVIEW,SDK_READER,pins);_package(PQ_MANIFEST,PQ_REVIEW,PQ_READER,pins)
    return pins


def _load(value,name):
    path=exact(value);spec=importlib.util.spec_from_file_location(name,path)
    if spec is None or spec.loader is None:raise RuntimeError('Cannot load exact decimal54 metadata checker')
    module=importlib.util.module_from_spec(spec)
    exec(compile(path.read_bytes(),str(path),'exec'),module.__dict__)
    exact(value);return module


def sdk_index(path,selected_sha,workspace):
    if Path(workspace).resolve(strict=True)!=ROOT:raise RuntimeError('Decimal54 proof workspace differs')
    before=control_inputs();api=_load(SDK_READER,'og_lake_exact_decimal54_full_sdk_metadata')
    result=api.sdk_index(path,selected_sha,workspace)
    if result['header_sha256']!=SDK_SOURCES['sdk/lake_parquet_sdk.h']:raise RuntimeError('Decimal54 full public SDK header mismatch')
    if control_inputs()!=before:raise RuntimeError('Decimal54 controlling Source changed during SDK admission')
    return result


@contextlib.contextmanager
def _module_aliases(mapping):
    # Original mature build/installer CLIs are serial. Do not mutate Python's
    # import names from a caller thread or leave an alias after any exception.
    if threading.current_thread()is not threading.main_thread():raise RuntimeError('Decimal54 imports require the serial original CLI thread')
    previous={name:sys.modules.get(name)for name in mapping}
    try:
        sys.modules.update(mapping);yield
    finally:
        for name,old in previous.items():
            if old is None:sys.modules.pop(name,None)
            else:sys.modules[name]=old


def pq_inputs(receipt,workspace,*,selection=None,selection_sha256=None):
    global _PQ_SELECTION
    if selection is None or selection_sha256 is None:raise RuntimeError('Decimal54 PQ requires explicit full-SDK selection path and SHA')
    if Path(workspace).resolve(strict=True)!=ROOT:raise RuntimeError('Decimal54 PQ proof workspace differs')
    descriptor=dict(path=str(Path(selection)),sha256=selection_sha256);exact(descriptor)
    if _PQ_SELECTION is not None and _PQ_SELECTION!=(os.getpid(),descriptor):raise RuntimeError('One immutable decimal54 selection per process; no inherited cross-process grant')
    _PQ_SELECTION=(os.getpid(),descriptor)
    before=control_inputs();c=json.loads(Path(selection).read_text())
    for key,wanted in [('stage_review',PQ_REVIEW),('SDK_checker_manifest',SDK_MANIFEST),('SDK_checker_reader',SDK_READER),('SDK_checker_review',SDK_REVIEW)]:
        if c.get(key)!=wanted:raise RuntimeError('Selected SDK/PQ Source differs from reviewed decimal54 integration/'+key)
    stage=_load(PQ_READER,'og_lake_decimal54_stage_metadata')
    manifest=json.loads(Path(PQ_MANIFEST['path']).read_text())
    bridge_ref=dict(path=str(PQ_DIR/'sdk_decimal54_inputs.py'),sha256=manifest['artifacts']['sdk_decimal54_inputs.py'])
    with _module_aliases({'stage_guard':stage}):bridge=_load(bridge_ref,'og_lake_decimal54_pq_sdk_bridge')
    admission_ref=dict(path=str(PQ_DIR/'pq_decimal54_build_inputs.py'),sha256=manifest['artifacts']['pq_decimal54_build_inputs.py'])
    # Both same-name mature imports must resolve to the exact existing main
    # helpers already covered by the original PQ frozen-shared.json closure.
    frozen=json.loads((PQ_DIR/'frozen-shared.json').read_text())
    base=ROOT/'integration/opengauss-native-lake'
    legacy_ref=dict(path=str(base/'pq_native_build_inputs.py'),sha256=frozen[str(base/'pq_native_build_inputs.py')])
    temporal_ref=dict(path=str(base/'sdk_temporal_inputs.py'),sha256=frozen[str(base/'sdk_temporal_inputs.py')])
    legacy=_load(legacy_ref,'og_lake_decimal54_original_legacy_pq_metadata')
    with _module_aliases({'pq_native_build_inputs':legacy}):
        temporal=_load(temporal_ref,'og_lake_decimal54_original_temporal_metadata')
    with _module_aliases({'stage_guard':stage,'sdk_decimal54_inputs':bridge,'pq_native_build_inputs':legacy,'sdk_temporal_inputs':temporal}):
        selected=stage.verify_stage(selection=selection,selection_sha=selection_sha256)
        api=_load(admission_ref,'og_lake_decimal54_original_static_admission')
        result=api.pq_inputs(receipt,workspace)
        if result['qualified_sources']['sdk_index']!=selected:raise RuntimeError('Static PQ consumed a different full decimal54 SDK')
        if stage.verify_stage(selection=selection,selection_sha=selection_sha256)!=selected:raise RuntimeError('Full SDK54 selection changed during static admission')
    exact(descriptor)
    if control_inputs()!=before:raise RuntimeError('Decimal54 SDK/PQ controlling Source changed during admission')
    return dict(result,decimal54_selection=descriptor)
