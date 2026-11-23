#!/usr/bin/env python3
"""Build the optional, owned Native PQ static dependency, one compiler at a time.

Does not build/start Native or change source-lock. Frozen component numerical
sources are compiled unchanged. This is packaging evidence, not Native pass.
"""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys

sys.path[:0]=[str(Path(__file__).resolve().parents[1]),str(Path(os.getenv('CPP_WORKSPACE','/home/qiumc/data/code/cpp'))/'integration/opengauss-native-lake')]
from pq_native_build_inputs import blas_inputs, digest, global_symbols
from pq_sql020_build_inputs import pq_inputs,qualified_sources,KIND


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--workspace', type=Path, required=True)
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--component-index',type=Path,required=True)
    parser.add_argument('--component-index-sha256',required=True)
    parser.add_argument('--asan-index',type=Path,required=True)
    parser.add_argument('--asan-index-sha256',required=True)
    args = parser.parse_args()
    if not re.fullmatch(r'[A-Za-z0-9_-][A-Za-z0-9_.-]*', args.run_id):
        parser.error('simple new run-id required')
    root = args.workspace.resolve(strict=True)
    qualified = qualified_sources(root,args.component_index,args.component_index_sha256,args.asan_index,args.asan_index_sha256)
    blas = blas_inputs(root)
    source = Path(qualified['source'])
    build = root / '.build/opengauss-native-lake/pq-static' / args.run_id
    prefix = root / '.install/opengauss-native-lake-pq-static' / args.run_id
    evidence = root / '公共文档/openGauss原生湖存储/验证' / args.run_id
    # Never clean or reuse old build/evidence/installed assets.
    build.mkdir(parents=True, exist_ok=False)
    prefix.mkdir(parents=True, exist_ok=False)
    evidence.mkdir(parents=True, exist_ok=False)
    for subdir in ('lib', 'include/sdk', 'include/vendor/arrow20', 'licenses/OpenBLAS'):
        (prefix / subdir).mkdir(parents=True, exist_ok=True)
    owner = dict(project='opengauss-native-lake-pq-static', run_id=args.run_id,
                 prefix=str(prefix), build=str(build), source_commit=qualified['source_commit'],dependency_kind=KIND)
    (prefix / '.og-lake-pq-owner.json').write_text(json.dumps(owner, indent=2) + '\n')
    gcc = root / 'binarylibs/buildtools/gcc10.3/gcc'
    ssl = root / 'binarylibs/kernel/dependency/openssl/comm'
    tools = dict(cxx=gcc / 'bin/g++', ar=Path('/usr/bin/ar'),
                 ranlib=Path('/usr/bin/ranlib'), objcopy=Path('/usr/bin/objcopy'),
                 nm=Path('/usr/bin/nm'), readelf=Path('/usr/bin/readelf'))
    env = dict(os.environ)
    # Changes are confined to child compiler/probe processes.
    env.update(OMP_NUM_THREADS='1', OPENBLAS_NUM_THREADS='1',
               LD_LIBRARY_PATH=':'.join([str(ssl/'lib'), str(gcc/'lib64')]+[str(gcc.parent/name/'lib') for name in ('isl','mpc','mpfr','gmp')]))
    receipt = evidence / 'build.json'
    result = dict(status='failed', manifest_version=1, native_integration=False,dependency_kind=KIND,
                  qualification={'normal':{'path':str(args.component_index.resolve()),'sha256':args.component_index_sha256},'asan':{'path':str(args.asan_index.resolve()),'sha256':args.asan_index_sha256}},
                  source_lock_updated=False, qualified_sources=qualified, blas_inputs=blas,
                  prefix=str(prefix), include=str(prefix / 'include'), owner=owner,
                  owner_sha256=digest(prefix / '.og-lake-pq-owner.json'),
                  commands=[], build_tools={k:dict(path=str(v.resolve()), sha256=digest(v)) for k,v in tools.items()},
                  helper_inputs={str(Path(__file__).resolve()):digest(Path(__file__).resolve()),
                                 str(Path(__file__).resolve().parents[1]/'pq_sql020_build_inputs.py'):
                                 digest(Path(__file__).resolve().parents[1]/'pq_sql020_build_inputs.py')},
                  started_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
                  scope='Owned static dependency packaging only; Native not built or tested',
                  limitations=['Native integration/SQL/ResourceOwner/SMP/recall/performance are unqualified.',
                               'Frozen CPU/MS/Parquet numeric sources are unmodified; frontier draft is adapter-only.',
                               'OpenBLAS is private serial USE_LOCKING; immutable once-only metadata shim is mandatory.',
                               'No TSan qualification. BLAS inner numeric calls are not interruptible.'])

    def command(name, argv):
        result['commands'].append(dict(name=name, argv=list(map(str, argv))))
        with (evidence / (name+'.log')).open('w') as log:
            child = subprocess.run(list(map(str, argv)), env=env, stdout=log, stderr=subprocess.STDOUT,
                                   stdin=subprocess.DEVNULL, timeout=300)
        result['commands'][-1]['returncode'] = child.returncode
        if child.returncode:
            raise RuntimeError(name+' failed; see '+str(evidence / (name+'.log')))

    try:
        import pq_sql020_build_inputs as admission_inputs
        result['admission_helper_sha256']=digest(Path(admission_inputs.__file__).resolve())
        import pq_native_build_inputs as legacy_inputs
        result['helper_inputs'][str(Path(legacy_inputs.__file__).resolve())]=digest(Path(legacy_inputs.__file__).resolve())
        common = [tools['cxx'], '-std=c++14', '-O0', '-g', '-fPIC', '-fvisibility=hidden',
                  '-fvisibility-inlines-hidden', '-I', source, '-I', Path(qualified['sdk_include']),
                  '-I', Path(blas['prefix'])/'include', '-I', ssl/'include',
                  '-include', source/'pq_cpu_locking_blas_meta.h',
                  '-Dcblas_sgemm=oglpq_cblas_sgemm', '-Dcblas_snrm2=oglpq_cblas_snrm2',
                  '-Dopenblas_get_config=pq_cpu_blas_immutable_config',
                  '-Dopenblas_get_parallel=pq_cpu_blas_parallel_checked',
                  '-Dopenblas_get_num_threads=pq_cpu_blas_num_threads_checked']
        objects = []
        for name in ('pq_cpu', 'pq_cpu_ms', 'pq_cpu_locking_blas_meta',
                     'pq_parquet_format', 'pq_parquet_frontier_draft'):
            obj = build/(name+'.o')
            depfile=build/(name+'.d')
            command('compile-'+name, common+['-MD','-MF',depfile,'-c', source/(name+'.cpp'), '-o', obj])
            if name in ('pq_parquet_format','pq_parquet_frontier_draft'):
                header=Path(qualified['sdk_include'])/'sdk/lake_parquet_sdk.h'
                if str(header) not in depfile.read_text():raise RuntimeError('Compiler did not use exact candidate SDK header')
                result.setdefault('sdk_header_dependencies',{})[name]={'path':str(depfile),'sha256':digest(depfile),'header_sha256':digest(header)}
            objects.append(obj)
        library = prefix/'lib/libog_lake_pq_cpu.a'
        command('archive', [tools['ar'], 'rcs', library]+objects)
        # Isolate every defined C++/weak helper, not just namespace prefixes.
        # Public C ABI stays exact; undefined libstdc++/crypto/libc symbols stay
        # unchanged. objcopy applies the SAME map to definitions and references
        # in every archive member; no source or numerical-loop modifications.
        original = global_symbols(library)
        groups = subprocess.check_output([str(tools['readelf']), '--section-groups', str(library)], text=True)
        signatures = set(re.findall(r"COMDAT group section .*?\[([^]\n]+)\] contains", groups))
        # GCC's local C5/D5 group signature symbols are not externs. Rename
        # them too, otherwise a client may discard this private std::vector
        # group against its own group and leave a renamed weak call at zero.
        original |= signatures
        rename = {name:'oglpqcpu_'+name for name in original
                  if not name.startswith(('pq_cpu_', 'pq_parquet_'))}
        mapping = prefix/'symbol-map.txt'
        mapping.write_text(''.join(k+' '+v+'\n' for k,v in sorted(rename.items())))
        command('isolate-symbols', [tools['objcopy'], '--redefine-syms='+str(mapping), library])
        command('ranlib', [tools['ranlib'], library])
        blas_library = prefix/'lib/libog_lake_pq_openblas.a'
        shutil.copy2(blas['library'], blas_library)
        for name in ('pq_cpu.h', 'pq_parquet_format.h', 'pq_parquet_frontier_draft.h'):
            shutil.copy2(source/name, prefix/'include'/name)
        for name in ('sdk/lake_parquet_sdk.h', 'vendor/arrow20/abi.h'):
            shutil.copy2(Path(qualified['sdk_include'])/name, prefix/'include'/name)
        shutil.copy2(root/'.reference/opengauss-native-lake/diskann-pq-cpu-reference/LICENSE',
                     prefix/'licenses/Microsoft-DiskANN-LICENSE')
        for name in (Path(blas['prefix'])/'licenses').iterdir():
            shutil.copy2(name, prefix/'licenses/OpenBLAS'/name.name)
        # Archive-link smoke uses the frozen original probe unchanged. This
        # also catches bad symbol rewriting or missing transitive dependencies.
        probe_source = source/'pq_cpu_stream_probe.cpp'
        if digest(probe_source) != 'a830625a11615df23a2bd72dc99c1fd45b4daf9711301c4fbe26c1017efc5dc4':
            raise RuntimeError('frozen archive smoke probe changed')
        probe = build/'archive-stream-probe'
        command('link-archive-probe', [tools['cxx'], '-std=c++14', '-O0', '-g', '-I', source,
                   '-I', Path(blas['prefix'])/'include', probe_source, library, blas_library,
                   '-Wl,--exclude-libs,libog_lake_pq_cpu.a', '-Wl,--exclude-libs,libog_lake_pq_openblas.a',
                   '-L', ssl/'lib', '-lcrypto', '-ldl', '-pthread', '-lm', '-o', probe])
        # Inspect all symbol identities even before Native consumes the archive.
        dynamic = global_symbols(probe, dynamic=True)
        owned = global_symbols(library)|global_symbols(blas_library)
        if dynamic & owned:
            raise RuntimeError('private archive globals leaked from standalone link')
        command('archive-probe-shapes', [probe, 'shapes'])
        result['standalone_receipt']={'mode':'shapes','returncode':result['commands'][-1]['returncode'],'probe_sha256':digest(probe)}
        result['standalone_check'] = 'frozen stream probe shapes: d3/M1 and d1536/M16; not Native qualification'
        # Also exercise the SDK Parquet facade THROUGH the isolated archive,
        # not only from the unrenamed G2 component sources.
        parquet_probe=build/'archive-parquet-probe'
        command('link-archive-parquet-probe',[tools['cxx'],'-std=c++14','-O0','-g','-I',prefix/'include',source/'pq_parquet_probe.cpp',library,blas_library,
            '-Wl,--exclude-libs,libog_lake_pq_cpu.a','-Wl,--exclude-libs,libog_lake_pq_openblas.a','-L',ssl/'lib','-lcrypto','-ldl','-pthread','-lm','-o',parquet_probe])
        artifacts=build/'archive-parquet-artifacts';artifacts.mkdir()
        provider=qualified['sdk_index']['library']
        command('archive-parquet-produce',[parquet_probe,provider,artifacts,'produce','d3',1])
        producer=json.loads((evidence/'archive-parquet-produce.log').read_text())
        command('archive-parquet-consume',[parquet_probe,provider,artifacts,'consume','d3',1,producer['manifest']])
        result['archive_parquet_receipt']={'returncodes':[result['commands'][-2]['returncode'],result['commands'][-1]['returncode']],'manifest':producer['manifest'],'probe_sha256':digest(parquet_probe)}
        result['library'] = str(library)
        result['blas_library'] = str(blas_library)
        result['library_sha256'] = digest(library)
        result['blas_library_sha256'] = digest(blas_library)
        result['owned_symbols'] = sorted(global_symbols(library))
        result['symbol_isolation'] = dict(method='objcopy-all-defined-nonpublic-C-symbols + final exclude-libs',
                     map_sha256=digest(mapping), renamed_count=len(rename),
                     public_prefixes=['pq_cpu_', 'pq_parquet_'], comdat_signature_count=len(signatures),
                     standalone_dynamic_overlap_count=0)
        result['files'] = {str(p.relative_to(prefix)):digest(p) for p in sorted(prefix.rglob('*')) if p.is_file()}
        result['probe_inputs'] = {str(probe_source):digest(probe_source)}
        if qualified_sources(root,args.component_index,args.component_index_sha256,args.asan_index,args.asan_index_sha256)!=qualified or blas_inputs(root)!=blas:
            raise RuntimeError('frozen component/dependency changed while building')
        result['status'] = 'pq_static_built_unlinked'
        receipt.write_text(json.dumps(result, indent=2)+'\n')
        pq_inputs(receipt, root)
        return 0
    except Exception as exc:
        result['status'] = 'failed'
        result['error'] = str(exc)
        print(str(exc), file=sys.stderr)
        return 1
    finally:
        result['finished_at'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        receipt.write_text(json.dumps(result, indent=2)+'\n')
        print(json.dumps(dict(status=result['status'], receipt=str(receipt))), flush=True)


if __name__ == '__main__':
    sys.exit(main())
