#!/usr/bin/env python3
"""Standalone build-adapter checks; does not build or start openGauss."""
import argparse
import copy
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / 'integration/opengauss-native-lake'))
import build_driver as driver


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--source', type=Path, required=True)
    ap.add_argument('--dependency-build', type=Path, required=True)
    ap.add_argument('--run-id', required=True)
    args = ap.parse_args()
    assert re.fullmatch(r'[A-Za-z0-9_.-]+', args.run_id)
    out = ROOT / '公共文档/openGauss原生湖存储/验证' / args.run_id
    out.mkdir(exist_ok=False)
    scratch = ROOT / '.build/opengauss-native-lake/bitmap-build-admission' / args.run_id
    scratch.mkdir(parents=True, exist_ok=False)
    result = dict(status='failed', scope=__doc__, source=str(args.source.resolve()), cases=[],
                  limitations=['No native kernel build/install/runtime/SMP/query qualification',
                               'CMake/Make use an isolated minimal harness, not full project configuration'])
    inputs = [Path(__file__).resolve(), Path(driver.__file__).resolve()] + [args.source / p for p in (
        'contrib/og_lake/Makefile', 'contrib/og_lake/CMakeLists.txt',
        'src/gausskernel/Makefile', 'src/gausskernel/CMakeLists.txt')]
    result['inputs_before'] = {str(p): driver.digest(p) for p in inputs}
    dep = json.loads(args.dependency_build.read_text())
    def case(name, fn, reject=False):
        try:
            value = fn()
            if reject:
                raise AssertionError('unexpected acceptance')
            result['cases'].append(dict(name=name, status='passed', value=value))
            return value
        except (RuntimeError, ValueError, KeyError, FileNotFoundError) as exc:
            if not reject:
                raise
            result['cases'].append(dict(name=name, status='passed', expected_rejection=str(exc)))
    def run(name, cmd, env=None):
        with (out / (name + '.log')).open('x') as log:
            log.write(json.dumps(list(map(str, cmd))) + '\n'); log.flush()
            subprocess.run(list(map(str, cmd)), env=env, stdout=log, stderr=subprocess.STDOUT,
                           check=True, timeout=60)
        return str(out / (name + '.log'))
    try:
        shutil.copy2(__file__, out / 'executed-probe.py')
        shutil.copy2(driver.__file__, out / 'executed-build-driver.py')
        actual = case('qualified_dependency', lambda: driver.roaring_inputs(args.dependency_build))
        case('legacy_no_dependency', lambda: driver.roaring_inputs(None))
        # Mutate only an independently owned copy, preserving locked real assets.
        prefix = scratch / 'prefix'; shutil.copytree(dep['prefix'], prefix)
        source = scratch / 'source'; (source / 'src').mkdir(parents=True)
        for name in ('src/roaring_array.c', 'LICENSE'):
            shutil.copy2(Path(dep['source']) / name, source / name)
        fixture = copy.deepcopy(dep)
        fixture.update(prefix=str(prefix), source=str(source), library=str(prefix / 'lib64/libroaring.a'))
        owner = json.loads((prefix / 'owner.json').read_text()); owner['prefix'] = str(prefix)
        (prefix / 'owner.json').write_text(json.dumps(owner))
        fixture['owner_sha256'] = driver.digest(prefix / 'owner.json')
        receipt = scratch / 'fixture.json'
        def admit(value):
            receipt.write_text(json.dumps(value)); return driver.roaring_inputs(receipt)
        case('copied_owned_dependency', lambda: admit(fixture))
        for key, value in [('status', 'failed'), ('source_commit', '0'*40),
                           ('source_patch_sha256', '0'*64), ('patched_source_sha256', '0'*64),
                           ('owner_sha256', '0'*64), ('library_sha256', '0'*64),
                           ('base_build_sha256', '0'*64)]:
            bad = copy.deepcopy(fixture); bad[key] = value
            case('reject_' + key, lambda bad=bad: admit(bad), reject=True)
        for name in ('include/roaring/roaring.h', 'lib64/libroaring.a'):
            path = prefix / name; content = path.read_bytes(); path.write_bytes(content + b'corrupt')
            case('reject_changed_' + path.name, lambda: admit(fixture), reject=True)
            path.write_bytes(content)
        extra = prefix / 'unexpected.so'; extra.write_bytes(b'not admitted')
        case('reject_unlisted_file', lambda: admit(fixture), reject=True); extra.unlink()
        patch = scratch / 'wrong.patch'; patch.write_text('different patch\n')
        bad = copy.deepcopy(fixture); bad['source_patch'] = str(patch)
        case('reject_actual_patch_bytes', lambda: admit(bad), reject=True)
        path = source / 'src/roaring_array.c'; content = path.read_bytes(); path.write_bytes(content + b'\n')
        case('reject_actual_patched_source', lambda: admit(fixture), reject=True); path.write_bytes(content)
        case('original_dependency_unchanged', lambda: driver.roaring_inputs(args.dependency_build) == actual)
        original_root, original_source = driver.ROOT, driver.SOURCE
        driver.ROOT, driver.SOURCE = scratch, args.source.resolve()
        stamp, identity = driver.check_roaring_config('full', 'debug', actual, True)
        case('reject_incremental_without_stamp', lambda: driver.check_roaring_config('incremental', 'debug', actual, True), True)
        stamp.parent.mkdir(parents=True); stamp.write_text(json.dumps(identity))
        case('same_dependency_incremental', lambda: bool(driver.check_roaring_config('incremental', 'debug', actual, True)))
        case('same_dependency_restart', lambda: bool(driver.check_roaring_config('restart', 'debug', actual, True)))
        case('reject_disabling_in_incremental', lambda: driver.check_roaring_config('incremental', 'debug', None, True), True)
        changed = copy.deepcopy(actual); changed['receipt_sha256'] = '0'*64
        for mode in ('incremental', 'restart'):
            case('reject_changed_dependency_' + mode, lambda mode=mode: driver.check_roaring_config(mode, 'debug', changed, True), True)
        stamp.write_text(json.dumps(dict(identity, state='building')))
        case('unfinished_same_config_incremental', lambda: bool(driver.check_roaring_config('incremental', 'debug', actual, True)))
        case('reject_unfinished_build_restart', lambda: driver.check_roaring_config('restart', 'debug', actual, True), True)
        original_source_inputs = driver.native_source_inputs
        simulated = original_source_inputs()
        driver.native_source_inputs = lambda: simulated
        simulated['contrib/og_lake/only_cpp_repair.cpp'] = 'new CPP bytes'
        case('unfinished_cpp_only_repair_incremental', lambda: bool(driver.check_roaring_config('incremental', 'debug', actual, True)))
        simulated['contrib/og_lake/new_header.h'] = 'changed header'
        case('reject_unfinished_changed_header_incremental', lambda: driver.check_roaring_config('incremental', 'debug', actual, True), True)
        del simulated['contrib/og_lake/new_header.h']
        simulated['contrib/og_lake/Makefile'] = 'changed Make configuration'
        case('reject_unfinished_changed_make_incremental', lambda: driver.check_roaring_config('incremental', 'debug', actual, True), True)
        driver.native_source_inputs = original_source_inputs
        inactive = dict(identity, roaring_inputs=None)
        stamp.write_text(json.dumps(inactive))
        case('reject_enabling_in_incremental', lambda: driver.check_roaring_config('incremental', 'debug', actual, True), True)
        stamp.unlink()
        case('legacy_no_bitmap_incremental', lambda: bool(driver.check_roaring_config('incremental', 'debug', None, False)))
        driver.ROOT, driver.SOURCE = original_root, original_source
        # Parse the actual module Makefile with harmless stand-ins for project includes.
        make_root = scratch / 'make'; (make_root / 'src/gausskernel').mkdir(parents=True)
        (make_root / 'src/Makefile.global').write_text('')
        (make_root / 'src/gausskernel/common.mk').write_text('')
        show = make_root / 'show.mk'; show.write_text("show:\n\t@echo '$(OBJS)'\n\t@echo '$(CPPFLAGS)'\n")
        module = args.source.resolve() / 'contrib/og_lake'
        base = ['make', '-s', '-f', str(module / 'Makefile'), '-f', str(show),
                'top_builddir=' + str(make_root), 'top_srcdir=' + str(make_root),
                'srcdir=' + str(module), 'show']
        env = dict(os.environ); env.pop('OG_LAKE_ROARING_INCLUDE', None); env.pop('OG_LAKE_ROARING_ARCHIVE', None)
        def module_make(enabled):
            cmd = base + (['OG_LAKE_ROARING_INCLUDE=' + actual['include'], 'OG_LAKE_ROARING_ARCHIVE=' + actual['library']] if enabled else [])
            value = subprocess.check_output(cmd, env=env, text=True)
            (out / ('make_enabled.txt' if enabled else 'make_disabled.txt')).write_text(value)
            assert ('lake_bitmap_codec.o' in value) == enabled
            assert ('lake_bitmap_format.o' in value) == enabled
            assert ('USE_LAKE_BITMAP' in value) == enabled
            return True
        case('make_disabled_omits_bitmap', lambda: module_make(False))
        case('make_enabled_compiles_both_with_macro', lambda: module_make(True))
        for missing in ('OG_LAKE_ROARING_INCLUDE', 'OG_LAKE_ROARING_ARCHIVE'):
            def reject_make(missing=missing):
                cmd = base + [key+'='+value for key,value in (
                    ('OG_LAKE_ROARING_INCLUDE', actual['include']),
                    ('OG_LAKE_ROARING_ARCHIVE', actual['library'])) if key != missing]
                proc = subprocess.run(cmd, env=env, text=True, capture_output=True, timeout=30)
                assert proc.returncode != 0
                (out / ('make_missing_' + missing + '.log')).write_text(proc.stdout + proc.stderr)
                return True
            case('make_reject_missing_' + missing, reject_make)
        # Execute the actual module CMake file with its object-target helper stubbed:
        # inspect sources/defines/includes without compiling any native C++.
        cm_root = scratch / 'cmake'; cm_root.mkdir()
        (cm_root / 'CMakeLists.txt').write_text('''cmake_minimum_required(VERSION 3.16)
project(bitmap_adapter LANGUAGES C)
function(add_static_objtarget target src inc defs flags links)
    add_library(${target} OBJECT "${CMAKE_SOURCE_DIR}/main.c")
    set_property(TARGET ${target} PROPERTY EXCLUDE_FROM_ALL TRUE)
    file(WRITE "${CMAKE_BINARY_DIR}/observed.txt" "sources=${${src}}\\nincludes=${${inc}}\\ndefines=${defs}\\n")
endfunction()
add_subdirectory("'''+str(module)+'''" module)
get_property(OG_LAKE_ROARING_ARCHIVE GLOBAL PROPERTY OG_LAKE_ROARING_ARCHIVE)
'''+(args.source / 'src/gausskernel/CMakeLists.txt').read_text().split('# contrib/og_lake is configured first;')[1].split('if(NOT "${ENABLE_UT}"')[0].replace(' link its exact private archive into both\n# final targets. Object-library link options alone do not reach gaussdb.\n','')+'''
add_executable(probe main.c)
target_link_libraries(probe PRIVATE ${gaussdb_LINK_LIBS})
target_link_options(probe PRIVATE -Wl,--export-dynamic ${gaussdb_LINK_OPTIONS})
''')
        (cm_root / 'main.c').write_text('''extern void *roaring_bitmap_create(void); extern void roaring_bitmap_free(void*);
int main(void) { void *p=roaring_bitmap_create(); if (!p) return 1; roaring_bitmap_free(p); return 0; }
''')
        for enabled in (False, True):
            cm_env = dict(env)
            if enabled:
                cm_env.update(OG_LAKE_ROARING_INCLUDE=actual['include'], OG_LAKE_ROARING_ARCHIVE=actual['library'])
            build = cm_root / ('enabled' if enabled else 'disabled')
            run('cmake_config_' + str(enabled), ['cmake', '-S', cm_root, '-B', build], cm_env)
            value = (build / 'observed.txt').read_text()
            assert ('lake_bitmap_codec.cpp' in value) == enabled
            assert ('lake_bitmap_format.cpp' in value) == enabled
            assert ('USE_LAKE_BITMAP' in value) == enabled
            result['cases'].append(dict(name='cmake_config_' + str(enabled), status='passed'))
            if enabled:
                run('cmake_link', ['cmake', '--build', build, '--verbose'], cm_env)
                run('cmake_link_execute', [build / 'probe'], cm_env)
                case('cmake_real_archive_hidden', lambda: driver.verify_roaring_symbols(build / 'probe', actual['library']))
                case('reject_unlinked_native_bitmap_implementation', lambda: driver.verify_roaring_symbols(
                    build / 'probe', actual['library'], ('og_lake_bitmap_decode',)), True)
                case('verify_required_defined_symbol', lambda: driver.verify_roaring_symbols(
                    build / 'probe', actual['library'], ('roaring_bitmap_portable_deserialize_safe',)))
        for missing in ('OG_LAKE_ROARING_INCLUDE', 'OG_LAKE_ROARING_ARCHIVE'):
            cm_env = dict(env)
            cm_env.update(OG_LAKE_ROARING_INCLUDE=actual['include'], OG_LAKE_ROARING_ARCHIVE=actual['library'])
            cm_env.pop(missing)
            proc = subprocess.run(['cmake', '-S', str(cm_root), '-B', str(cm_root / ('missing_' + missing))],
                                  env=cm_env, text=True, capture_output=True, timeout=30)
            assert proc.returncode != 0
            (out / ('cmake_missing_' + missing + '.log')).write_text(proc.stdout + proc.stderr)
            result['cases'].append(dict(name='cmake_reject_missing_' + missing, status='passed'))
        # Link using the precise Make final-link conditional, independent of the
        # CMake path. This is a tiny executable, never the Native server.
        block = (args.source / 'src/gausskernel/Makefile').read_text().split('# Keep the lake decoder independent')[1].split('all:')[0]
        block = block[block.index('ifneq'):]
        link_make = make_root / 'link.mk'
        link_make.write_text(block + '\nall:\n\t$(CC) -Wl,--export-dynamic ' + str(cm_root / 'main.c') + ' $(LIBS) -o ' + str(make_root / 'probe') + '\n')
        run('make_link', ['make', '-f', link_make, 'OG_LAKE_ROARING_ARCHIVE=' + actual['library'], 'all'])
        run('make_link_execute', [make_root / 'probe'])
        case('make_real_archive_hidden', lambda: driver.verify_roaring_symbols(make_root / 'probe', actual['library']))
        # Negative control: prove symbol admission really rejects a public global.
        public = scratch / 'public'; public.mkdir()
        (public / 'lib.c').write_text('int roaring_probe_global(void) { return 0; }\n')
        (public / 'main.c').write_text('extern int roaring_probe_global(void); int main(void) { return roaring_probe_global(); }\n')
        run('public_compile', ['cc', '-c', public / 'lib.c', '-o', public / 'lib.o'])
        run('public_archive', ['ar', 'rcs', public / 'libroaring.a', public / 'lib.o'])
        run('public_link', ['cc', '-Wl,--export-dynamic', public / 'main.c', public / 'libroaring.a', '-o', public / 'probe'])
        case('reject_exported_archive_global', lambda: driver.verify_roaring_symbols(public / 'probe', public / 'libroaring.a'), True)
        run('hidden_link', ['cc', '-Wl,--export-dynamic', '-Wl,--exclude-libs,libroaring.a', public / 'main.c', public / 'libroaring.a', '-o', public / 'hidden'])
        case('exclude_libs_hides_public_archive', lambda: driver.verify_roaring_symbols(public / 'hidden', public / 'libroaring.a'))
        result['inputs_after'] = {str(p): driver.digest(p) for p in inputs}
        assert result['inputs_after'] == result['inputs_before'], 'build adapter inputs changed during probe'
        assert driver.roaring_inputs(args.dependency_build) == actual, 'qualified dependency changed'
        result['status'] = 'passed'
    except BaseException as exc:
        result['error'] = repr(exc)
    result['passed'] = sum(c['status'] == 'passed' for c in result['cases'])
    result['artifacts'] = {str(p.relative_to(out)): driver.digest(p) for p in out.iterdir() if p.is_file()}
    (out / 'result.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(dict(status=result['status'], passed=result['passed'], error=result.get('error'), evidence=str(out))))
    return 0 if result['status'] == 'passed' else 1


if __name__ == '__main__':
    raise SystemExit(main())
