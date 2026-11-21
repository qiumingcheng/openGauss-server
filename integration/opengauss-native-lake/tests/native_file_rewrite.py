#!/usr/bin/env python3
"""Prepared SQL21 standard interval rewrite qualification; no default execution.

Uses genuine Native CREATE/INSERT/flush files, ordinary heap results, actual
Parquet positions and an independently executed fixed DuckLake history/CDC
reader. This script holds the shared fixture reservation; its caller must
provide a whole-processgroup hard deadline (1200s for the first campaign).
Not physical GC, backdated metadata merge, ANN activation, RAC or performance.
"""
import argparse
import fcntl
from copy import deepcopy
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import traceback
from types import SimpleNamespace

ROOT = Path(os.getenv('CPP_WORKSPACE', '/home/qiumc/data/code/cpp')).resolve()
D = ROOT / 'integration/opengauss-native-lake'
MODES = ('scalar', 'vector', 'unknown-stats', 'zero', 'conflict', 'old-writer', 'rollback', 'coverage', 'upgrade')


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--execute', action='store_true')
    p.add_argument('--mode', choices=MODES, required=True)
    p.add_argument('--run-id')
    p.add_argument('--source-count', type=int, choices=[1, 2], default=2)
    p.add_argument('--build-receipt', type=Path)
    p.add_argument('--sdk-index',type=Path)
    p.add_argument('--sdk-index-sha256',help='Exact approved immutable new-kind SDK component index SHA256')
    args = p.parse_args()
    if not args.execute:
        print(json.dumps(dict(status='prepared_not_executed', mode=args.mode, source_count=args.source_count, sql_version='0.21',
            initial_sql='0.20' if args.mode == 'upgrade' else '0.21', native_runs=0,
            requires='Reviewed20+21/helper union, b68+scalar-lists+unknown-global-bytes fixes, exact new official receipt and independently-qualified new-kind SDK index+SHA; root serial fixture controller/deadline1200s.'), indent=2))
        return 0
    if not args.run_id or args.build_receipt is None or args.sdk_index is None or not args.sdk_index_sha256:
        p.error('execute needs run-id, exact build-receipt and immutable new-kind SDK index+SHA')
    if len(args.sdk_index_sha256)!=64 or any(c not in '0123456789abcdef'for c in args.sdk_index_sha256):
        p.error('sdk-index-sha256 must be an exact lowercase SHA256')
    sys.path[:0] = [str(D), str(D / 'tests')]
    from file_dml_acceptance import check_build
    from inline_fixture import query, scalar
    from lake_harness import sha256, validate_run_id, write_json
    from native_auto_flush import connect_control
    from native_runtime import PROFILE, SOURCE, INSTALL
    from native_test_context import OwnedDatabase
    from native_table_create import Case
    from permission_api import OwnedRoles, cleanup_roles
    from empty_catalog_oracle import read_native_catalog, canonical_shape, transport_native
    from reverse_reference import Reference, literal, qi

    validate_run_id(args.run_id)
    if len(args.run_id) > 90:
        p.error('run-id max90 leaves fixture-child identity budget')
    if PROFILE != 'debug':
        p.error('First rewrite qualification requires owned main Debug profile')
    out = ROOT / '公共文档/openGauss原生湖存储/验证' / args.run_id
    out.mkdir(exist_ok=False)
    r = dict(status='fail', scope=__doc__, mode=args.mode, source_count=args.source_count, assertions=[], executed=False)
    db = roles = control = case = reservation = None

    def check(name, observed, expected=True):
        ok = observed == expected
        r['assertions'].append(deepcopy(dict(name=name, observed=observed, expected=expected,
                                             status='pass' if ok else 'fail')))
        write_json(out / 'result.json', r)
        if not ok:
            raise AssertionError(name)

    def session(c):
        for text in ['SET session_timeout=0', 'SET statement_timeout=180000', 'SET query_dop=1',
                     'SET enable_lake_smp=off', 'SET enable_vector_engine=off', 'SET enable_default_ustore_table=off']:
            query(c, text)

    def both(text, conn=None):
        c = conn or case.b
        query(c, 'BEGIN')
        try:
            for table in ['biz.orders', 'biz.exact']:
                query(c, text.format(table=table))
            query(c, 'COMMIT')
        except BaseException:
            query(c, 'ROLLBACK')
            raise

    def parity(label):
        fields = 'id,val,v::text' if args.mode == 'vector' else 'id,val'
        check(label, query(case.observer, 'SELECT ' + fields + ' FROM biz.orders ORDER BY id'),
              query(case.observer, 'SELECT ' + fields + ' FROM biz.exact ORDER BY id'))

    def active_files():
        return query(case.observer, 'SELECT data_file_id,path,record_count,file_size_bytes FROM created_metadata.ducklake_data_file '
                     'WHERE table_id=%s AND end_snapshot IS NULL ORDER BY data_file_id', (table_id,))

    def file_inventory():
        return {str(x): sha256(x) for x in sorted(Path(case.empty_manifest['data_path']).rglob('*.parquet'))}

    def rewrite(c, ids):
        query(c, "SELECT og_lake.rewrite_data_files('biz.orders'::regclass,%s::bigint[])", (ids,))

    def reject(c, sql, code, params=None):
        try:
            query(c, sql, params)
        except BaseException as exc:
            check('actual SQLSTATE/' + sql[:80], getattr(exc, 'pgcode', None), code)
            query(c, 'ROLLBACK')
        else:
            raise AssertionError('unexpectedly accepted ' + sql)

    def capture_reference(label):
        # The legacy CREATE helper only enumerates data carriers. Read every
        # actual owned metadata relation so file-delete carriers cannot vanish.
        names = query(case.observer, "SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
                      "WHERE n.nspname='created_metadata' AND c.relkind='r' ORDER BY c.relname")
        exported = read_native_catalog(case.observer, case.schema,
                     {'tables': [{'table_name': name} for name, in names]}, out / (label + '-native.json'))
        actual = {table['table_name']: table for table in exported['tables']}
        standard_names = {table['table_name'] for table in case.empty_export['tables']}
        for table in case.empty_export['tables']:
            check(label + ' actual standard definition/' + table['table_name'],
                  canonical_shape(actual[table['table_name']]), canonical_shape(table))
        carriers = {row['table_name']: row['table_id'] for row in actual['ducklake_inlined_data_tables']['rows']}
        delete_names = {'ducklake_inlined_delete_' + str(row['table_id']) for row in actual['ducklake_table']['rows']}
        check(label + ' only genuine standard/registered native carriers', set(actual) <= standard_names | set(carriers) | delete_names)
        catalog = out / (label + '.ducklake')
        transport_native(reference, catalog, exported)
        if args.mode == 'vector':
            # The host stores the vector carrier as TEXT; the reference LIST
            # adaptation is confined to this owned offline transport. Preserve
            # all identity/version/value rows and archive the unmodified export.
            nested = [row for row in actual['ducklake_column']['rows']
                      if row['column_type'] == 'list' and row['parent_column'] is None]
            for name, tid in carriers.items():
                for column in nested:
                    if column['table_id'] == tid:
                        reference.run('ALTER TABLE ' + qi(name) + ' ALTER COLUMN ' + qi(column['column_name']) +
                            ' TYPE FLOAT[] USING CAST(' + qi(column['column_name']) + ' AS FLOAT[])',
                            label + '-offline-list-carrier-' + name, catalog)
            r['reference_list_adaptation'] = 'Only registered host TEXT carrier columns cast to FLOAT[] in owned offline copies; native exports, standard metadata and identity/version values unchanged.'
        before_hash = sha256(catalog)
        current = reference.logical(catalog, 'lake.main.orders', label + '-logical')
        check(label + ' reference readonly access preserves transported catalog', sha256(catalog), before_hash)
        return catalog, current

    def history(catalog, snapshots, label):
        return {str(s): reference.run(reference.attach(catalog, True) +
            'SELECT rowid AS logical_rowid,snapshot_id AS begin_snapshot,* FROM lake.main.orders AT(VERSION => ' + str(s) + ') ORDER BY rowid',
            label + '-' + str(s), result=True) for s in snapshots}

    def cdc(catalog, first, last, label):
        columns = 'snapshot_id,rowid AS logical_rowid,change_type,id,val' + (',v' if args.mode == 'vector' else '')
        return reference.run(reference.attach(catalog, True) + 'SELECT ' + columns +
            " FROM ducklake_table_changes('lake','main','orders'," + str(first) + ',' + str(last) +
            ') ORDER BY snapshot_id,logical_rowid,change_type', label, result=True)

    def expected_cdc(identities):
        # Snapshot numbers are observed immediately after the original business
        # commits. Row IDs come from actual physical/inline identity records;
        # payloads and operations follow the fixture's ordinary DML log.
        rowids = {}
        for row in before_physical + identities:
            if row['id'] in rowids:
                check('one stable fixture row ID/' + str(row['id']), row['logical_rowid'], rowids[row['id']])
            rowids[row['id']] = row['logical_rowid']
        check('independent event-log identities cover all five inserts', sorted(rowids), [1, 2, 3, 4, 5])
        check('all fixture inserts have distinct stable row IDs', len(set(rowids.values())), 5)
        events = []
        def add(snapshot, kind, identity, value):
            row = dict(snapshot_id=snapshot, logical_rowid=rowids[identity], change_type=kind, id=identity, val=value)
            if args.mode == 'vector':
                row['v'] = {1: [1,2,3], 2: None, 3: [4,5,6], 4: [7,8,9], 5: None}[identity]
            events.append(row)
        for identity, value in [(1,100), (2,200), (3,10)]:
            add(fixture_commits['insert123'], 'insert', identity, value)
        add(fixture_commits['update1'], 'update_preimage', 1, 100)
        add(fixture_commits['update1'], 'update_postimage', 1, 101)
        add(fixture_commits['delete2'], 'delete', 2, 200)
        add(fixture_commits['insert4'], 'insert', 4, 20)
        add(fixture_commits['insert5'], 'insert', 5, 50)
        if args.mode == 'zero':
            for identity, value in [(1,101), (3,10), (4,20)]:
                add(fixture_commits['delete_files'], 'delete', identity, value)
        return sorted(events, key=lambda row: (row['snapshot_id'], row['logical_rowid'], row['change_type']))

    def expected_history(events, snapshots):
        # Replay original business commits, not file-layout metadata or SDK
        # reader predicates. UPDATE pre/post pairs are atomic at one snapshot.
        state, expected = {}, {}
        for snapshot in snapshots:
            batch = [row for row in events if row['snapshot_id'] == snapshot]
            for row in batch:
                if row['change_type'] in ['delete', 'update_preimage']:
                    old = state.pop(row['logical_rowid'], None)
                    if old is None or any(old.get(key) != value for key, value in row.items()
                                          if key not in ['snapshot_id', 'change_type']):
                        raise AssertionError('Independent original DML preimage is inconsistent')
            for row in batch:
                if row['change_type'] in ['insert', 'update_postimage']:
                    if row['logical_rowid'] in state:
                        raise AssertionError('Independent original DML adds an already-live row ID')
                    state[row['logical_rowid']] = dict(
                        {key: value for key, value in row.items() if key not in ['snapshot_id', 'change_type']},
                        begin_snapshot=snapshot)
            expected[str(snapshot)] = sorted(deepcopy(list(state.values())), key=lambda row: row['logical_rowid'])
        return expected

    def physical(catalog_rows, label):
        found = []
        for file_id, stored_path, population, size in catalog_rows:
            paths = [x for x in Path(case.empty_manifest['data_path']).rglob(Path(stored_path).name) if x.is_file()]
            check(label + ' exact owned source path/' + str(file_id), len(paths), 1)
            path = paths[0]
            sql = ('SELECT file_row_number AS position,_ducklake_internal_row_id AS logical_rowid,'
                   '_ducklake_internal_snapshot_id AS begin_snapshot,' + ('id,val,v' if args.mode == 'vector' else 'id,val') +
                   ' FROM read_parquet(' + literal(str(path)) +
                   ',file_row_number=true) ORDER BY file_row_number')
            actual = reference.run(sql, label + '-physical-' + str(file_id), result=True)
            check(label + ' actual physical row count/' + str(file_id), len(actual), population)
            check(label + ' metadata bytes equal real object/' + str(file_id), path.stat().st_size, size)
            for row in actual:
                found.append(dict(row, file_id=file_id))
            r.setdefault('physical_artifacts', []).append(dict(label=label, file_id=file_id, path=str(path), sha256=sha256(path)))
        return found

    try:
        build = check_build(args.build_receipt)
        r.update(source_head=build['source_head'], binary_sha256=build['binary_sha256'],
                 build=str(args.build_receipt.resolve()), build_sha256=sha256(args.build_receipt))
        r['executed'] = True
        control = connect_control()
        session(control)
        check('background fixture work remains disabled', scalar(control, 'SHOW enable_lake_maintenance'), 'off')
        check('owned dedicated backend profile', scalar(control, 'SHOW enable_thread_pool'), 'off')
        files = [Path(__file__), args.build_receipt, args.sdk_index, D / 'native_test_context.py', D / 'native_runtime.py', D / 'lake_harness.py',
                 D / 'inline_fixture.py', D / 'source-lock.json', D / 'fixture.py', D / 'reference.sh']
        files += [D / 'tests' / name for name in ['native_table_create.py', 'permission_api.py', 'inline_cleanup.py',
                  'file_dml_acceptance.py', 'native_auto_flush.py', 'catalog_fixture_bundle.py', 'empty_reference_fixture.py',
                  'empty_manifest.py', 'empty_catalog_oracle.py', 'reverse_reference.py', 'create_reference_oracle.py']]
        files += [SOURCE / name for name in build['native_source_inputs_after']]
        files += [Path(name) for name in build['build_wrapper_inputs_after']]
        # Archive code/SQL/receipts only. Large immutable executable/library
        # bytes are separately identity checked, never copied for every case.
        # The new-kind component helper verifies all12 real SDK cases,
        # source/proof files, whole private-prefix/license inventory and the
        # genuine old-provider negative. Never treat it as oldstatus=passed.
        from pq_sql020_build_inputs import sdk_index
        selected_sdk=sdk_index(args.sdk_index,args.sdk_index_sha256,ROOT)
        sdk_root = INSTALL / 'lib/postgresql/og_lake_sdk'
        owner_path = sdk_root / 'owner.json'
        owner = json.loads(owner_path.read_text())
        check('installed SDK owner belongs to expected provider',owner['project'],'opengauss-native-lake-local-provider')
        check('installed SDK owner reached atomic install preparation',owner['phase'],'prepared-for-atomic-install')
        check('installed SDK directory is canonical',not sdk_root.is_symlink() and sdk_root.resolve()==sdk_root)
        check('installed owner names exact SDK target',owner['target'],str(sdk_root))
        check('installed SDK uses exact new-kind component contract',owner.get('qualification_kind'),'verified_independent_sdk_sql020_int16_only')
        check('installed SDK links immutable selected component index',owner['qualification_sha256'],args.sdk_index_sha256)
        native_header=build['native_source_inputs_after']['contrib/og_lake/sdk/lake_parquet_sdk.h']
        check('qualified complete SDK header equals Native compiled header',selected_sdk['header_sha256'],native_header)
        expected_headers={str(SOURCE/name):build['native_source_inputs_after'][name] for name in
            ['contrib/og_lake/og_lake.control','contrib/og_lake/sdk/lake_parquet_sdk.h','contrib/og_lake/vendor/arrow20/abi.h']}
        check('installed owner binds complete Native control/public headers',owner.get('native_public_headers'),expected_headers)
        sdk_files=sorted(x for x in sdk_root.rglob('*')if x.is_file()and x!=owner_path)
        check('SDK prefix contains only ordinary owned files/directories',
              all(not x.is_symlink()and(x.is_file()or x.is_dir())for x in sdk_root.rglob('*')))
        actual_sdk={str(x.relative_to(sdk_root)):sha256(x)for x in sdk_files}
        check('whole installed SDK prefix matches owner manifest',actual_sdk,owner['files'])
        check('actual installed provider equals fully qualified component',actual_sdk['libog_lake_sdk_arrow.so'],selected_sdk['library_sha256'])
        check('actual installed complete public SDK header equals Native',actual_sdk.get('include/og_lake/lake_parquet_sdk.h'),native_header)
        if build.get('pq_inputs_after'):
            pq=build['pq_inputs_after']
            check('present PQ archive uses exact new-header dependency kind',pq.get('dependency_kind'),'pq_sql020_int16_v1')
            check('present private PQ complete SDK header matches Native',pq['files'].get('include/sdk/lake_parquet_sdk.h'),native_header)
        r['sdk_identity']=dict(component=selected_sdk,installed_owner_sha256=sha256(owner_path),
                               installed_files=actual_sdk,native_public_headers=expected_headers)
        files += [owner_path,D/'pq_sql020_build_inputs.py',D/'pq_native_build_inputs.py']
        for key in ['pq_inputs_after', 'roaring_inputs_after']:
            if key in build:
                receipt = build[key]
                check('exact dependency receipt/' + key, sha256(receipt['receipt']), receipt['receipt_sha256'])
                files.append(Path(receipt['receipt']))
        files += list((INSTALL / 'share/postgresql/extension').glob('og_lake*.sql'))
        hash_only = [INSTALL / 'bin/gaussdb'] + sdk_files
        lock = json.loads((D / 'source-lock.json').read_text())
        receipt_path = ROOT / '.reference/opengauss-native-lake/build/receipt.json'
        receipt = json.loads(receipt_path.read_text())
        ref_out = out / 'reference'
        ref_out.mkdir()
        reference = Reference(ref_out, lock, receipt)
        check('fixed reference lock', receipt['source_lock_hash'], sha256(D / 'source-lock.json'))
        check('fixed reference driver script', sha256(D / 'reference.sh'), lock['reference_build']['script_sha256'])
        check('fixed reference binary', sha256(reference.binary), receipt['binary_sha256'])
        files += [receipt_path]
        hash_only.append(reference.binary)
        r['hash_only_inputs'] = {str(x.resolve()): sha256(x) for x in hash_only}
        r['archive_scope'] = 'Code/helpers/SQL/official and SDK/dependency receipts are archived; Native/reference binaries and SDK prefix payloads are hash-only inputs, not archived-source claims.'
        archive = out / 'executed-inputs'
        archive.mkdir()
        r['inputs'] = {}
        for n, path in enumerate(dict.fromkeys(x.resolve() for x in files)):
            saved = archive / (str(n) + '-' + path.name)
            shutil.copy2(path, saved)
            r['inputs'][str(path)] = dict(sha256=sha256(path), archive=str(saved))
        with (out / 'fixture.log').open('w') as log:
            subprocess.run([sys.executable, str(D / 'tests/catalog_fixture_bundle.py'), '--run-id', args.run_id,
                '--output', str(out / 'fixture/manifest.json')], stdout=log, stderr=subprocess.STDOUT, check=True, timeout=300)
        # Generator owns this same lock itself. Acquire only after it exits,
        # then keep it through every reference call and owned DB/role cleanup.
        reservation = (ROOT / '.reference/opengauss-native-lake/.driver.lock').open('a+')
        r['reference_reservation'] = dict(path=str(reservation.name), mode='exclusive_nonblocking', acquired=False)
        fcntl.flock(reservation.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        r['reference_reservation']['acquired'] = True
        db = OwnedDatabase(args.run_id, out)
        db.create()
        session(db.admin)
        roles = OwnedRoles(control, args.run_id, out)
        initial = '0.20' if args.mode == 'upgrade' else '0.21'
        case = Case(db, SimpleNamespace(mode='net_dml', sql_version=initial, manifest=out / 'fixture/manifest.json',
                    output=out / 'result.json', run_id=args.run_id), r, roles)
        for c in [case.a, case.b, case.observer]:
            session(c)
        check('actual future module21', json.loads(scalar(case.a, 'SELECT og_lake.host_status()'))['module_version'], '0.21')
        if args.mode == 'upgrade':
            before = case.protocol()
            check('retained20 does not expose rewrite SQL entry', scalar(case.a,
                  "SELECT NOT EXISTS(SELECT 1 FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='og_lake' AND p.proname='rewrite_data_files')"))
            query(case.a, "ALTER EXTENSION og_lake UPDATE TO '0.21'")
            check('explicit upgrade installed21', scalar(case.a, "SELECT extversion FROM pg_extension WHERE extname='og_lake'"), '0.21')
            check('upgrade changes no standard data/catalog identities', case.protocol(), before)
        check('exact rewrite C entry executes in host rather than fenced UDF', query(case.a,
            "SELECT p.fencedmode,l.lanname,p.provolatile,p.prosrc FROM pg_catalog.pg_proc p "
            "JOIN pg_catalog.pg_namespace n ON n.oid=p.pronamespace JOIN pg_catalog.pg_language l ON l.oid=p.prolang "
            "WHERE n.nspname='og_lake' AND p.proname='rewrite_data_files' AND p.proargtypes::text='2205 1016' AND p.prorettype=2278"),
            [(False, 'c', 'v', 'og_lake_rewrite_data_files')])
        if args.mode == 'vector':
            query(case.a, "CREATE FOREIGN TABLE biz.orders(id integer,val integer,v vector(3)) SERVER created_lake OPTIONS(binding_mode 'create',lake_schema 'main')")
            query(case.a, 'CREATE TABLE biz.exact(id integer,val integer,v vector(3))')
            insert = "INSERT INTO {table} VALUES(1,100,'[1,2,3]'),(2,200,NULL),(3,10,'[4,5,6]')"
        else:
            case.create()
            query(case.a, 'CREATE TABLE biz.exact(id integer,val integer)')
            insert = 'INSERT INTO {table} VALUES(1,100),(2,200),(3,10)'
        table_id = scalar(case.observer, "SELECT table_id FROM created_metadata.ducklake_table WHERE table_name='orders' AND end_snapshot IS NULL")
        query(case.a, "SELECT og_lake.set_option('biz.orders'::regclass,'data_inlining_row_limit','1000')")
        fixture_commits = {}
        both(insert)
        fixture_commits['insert123'] = case.head()[0]
        both('UPDATE {table} SET val=101 WHERE id=1')
        fixture_commits['update1'] = case.head()[0]
        both('DELETE FROM {table} WHERE id=2')
        fixture_commits['delete2'] = case.head()[0]
        query(case.a, "SELECT og_lake.flush('biz.orders'::regclass)")
        both("INSERT INTO {table} VALUES(4,20,'[7,8,9]')" if args.mode == 'vector' else 'INSERT INTO {table} VALUES(4,20)')
        fixture_commits['insert4'] = case.head()[0]
        query(case.a, "SELECT og_lake.flush('biz.orders'::regclass)")
        original_files = active_files()
        check('two real complete source files', len(original_files), 2)
        original_ids = [x[0] for x in original_files]
        selected = original_files[:args.source_count]
        selected_ids = [x[0] for x in selected]
        retained_ids = [x for x in original_ids if x not in selected_ids]
        original_objects = file_inventory()
        before_physical = physical(original_files, 'before')
        original_deletes = query(case.observer,
            'SELECT delete_file_id,data_file_id,path,delete_count,file_size_bytes FROM created_metadata.ducklake_delete_file '
            'WHERE table_id=%s AND end_snapshot IS NULL ORDER BY delete_file_id', (table_id,))
        check('first historical flush produced a real active delete Parquet', len(original_deletes), 1)
        deletion_id, deletion_source, deletion_path, deletion_count, deletion_bytes = original_deletes[0]
        check('historical delete Parquet belongs to first selected source', deletion_source, selected_ids[0])
        check('historical old-version and DELETE produce two physical marks', deletion_count, 2)
        paths = [x for x in Path(case.empty_manifest['data_path']).rglob(Path(deletion_path).name) if x.is_file()]
        check('exact owned historical delete Parquet path', len(paths), 1)
        delete_positions = reference.run('SELECT file_path,pos AS position,_ducklake_internal_snapshot_id AS end_snapshot FROM read_parquet(' +
            literal(str(paths[0])) + ') ORDER BY pos', 'before-physical-delete-positions', result=True)
        check('real historical delete Parquet bytes', paths[0].stat().st_size, deletion_bytes)
        check('real historical deletion positions retain ended versions', [row['position'] for row in delete_positions],
              sorted([min((row for row in before_physical if row['id'] == 1), key=lambda row: row['begin_snapshot'])['position'],
                      next(row['position'] for row in before_physical if row['id'] == 2)]))
        check('real historical delete paths identify source object',
              all(Path(row['file_path']).name == Path(original_files[0][1]).name for row in delete_positions))
        r['source_delete_parquet'] = dict(metadata=original_deletes, positions=delete_positions, path=str(paths[0]), sha256=sha256(paths[0]))
        target = [x for x in before_physical if x['id'] == 3]
        check('one physical target version', len(target), 1)
        check('target distinguishes stable R and original P', target[0]['logical_rowid'] != target[0]['position'])
        both('INSERT INTO {table} VALUES(5,50,NULL)' if args.mode == 'vector' else 'INSERT INTO {table} VALUES(5,50)')
        fixture_commits['insert5'] = case.head()[0]
        if args.mode == 'zero':
            both('DELETE FROM {table} WHERE id<>5')
            fixture_commits['delete_files'] = case.head()[0]
        if args.mode == 'unknown-stats':
            query(case.a, 'UPDATE created_metadata.ducklake_table_stats SET file_size_bytes=NULL WHERE table_id=%s', (table_id,))
        index_uuid = None
        if args.mode == 'coverage':
            query(case.a, 'SET enable_indexscan=on')
            query(case.a, 'SET enable_seqscan=off')
            store = out / 'index-store'
            store.mkdir(mode=0o700)
            query(case.a, "SELECT og_lake.set_option('created_lake'::name,'index_root',%s::text)", (str(store),))
            index_uuid = str(scalar(case.a, "SELECT og_lake.create_lake_index('biz.orders'::regclass,'ix_rewrite',%s::jsonb)",
                (json.dumps(dict(version=1, kind='scalar', method='btree', column='id')),)))
            query(case.a, 'SELECT og_lake.build_lake_index(%s::uuid)', (index_uuid,))
            check('both original sources actually have active coverage', scalar(case.observer,
                  'SELECT count(*) FROM og_lake_internal.index_coverage WHERE index_uuid=%s::uuid AND active', (index_uuid,)), 2)
        before_head = case.head()
        before_stats = query(case.observer, 'SELECT record_count,next_row_id,file_size_bytes FROM created_metadata.ducklake_table_stats WHERE table_id=%s', (table_id,))[0]
        birth = scalar(case.observer, 'SELECT begin_snapshot FROM created_metadata.ducklake_table WHERE table_id=%s AND end_snapshot IS NULL', (table_id,))
        snapshots = [x[0] for x in query(case.observer, 'SELECT snapshot_id FROM created_metadata.ducklake_snapshot WHERE snapshot_id>=%s ORDER BY snapshot_id', (birth,))]
        before_catalog, before_identity = capture_reference('before')
        before_history = history(before_catalog, snapshots, 'before-history')
        expected_events = expected_cdc(before_identity)
        before_cdc = cdc(before_catalog, birth, before_head[0], 'before-complete-cdc')
        check('before rewrite CDC equals independent committed business log', before_cdc, expected_events)
        independent_history = expected_history(expected_events, snapshots)
        check('reference original current values/R/birth equal original committed business log', before_identity, independent_history[str(before_head[0])])
        check('reference every original historical snapshot equals original business log', before_history, independent_history)
        r['fixture_commits'] = fixture_commits
        r['independent_expected_cdc'] = expected_events
        check('reference history actually includes pre-file and current snapshots', bool(snapshots) and snapshots[0] == birth and snapshots[-1] == before_head[0])
        selected_versions = {(row['logical_rowid'], row['begin_snapshot']) for row in before_physical if row['file_id'] in selected_ids}
        expected_output_identity = [row for row in before_identity if (row['logical_rowid'], row['begin_snapshot']) in selected_versions]
        r['selected_sources'] = dict(original_ids=original_ids, selected_ids=selected_ids, retained_ids=retained_ids, expected_output=expected_output_identity)
        check('independent selected visible input cardinality', len(expected_output_identity), 0 if args.mode == 'zero' else args.source_count + 1)
        parity('initial genuine sources + inline equal ordinary heap')
        r['executed'] = True
        if args.mode == 'conflict':
            query(case.a, 'BEGIN')
            query(case.a, 'INSERT INTO biz.heap_marker VALUES(90)')
            rewrite(case.a, selected_ids)
            both('UPDATE {table} SET val=val+1 WHERE id=3')
            reject(case.a, 'COMMIT', '40001')
            check('conflict rolls ordinary heap marker back', query(case.observer, 'SELECT * FROM biz.heap_marker'), [])
            check('failed rewrite retires no selected source', [x[0] for x in active_files()], original_ids)
            check('failed candidate is discarded; original source bytes unchanged', file_inventory(), original_objects)
            parity('selected-source winner retained exactly once')
        elif args.mode == 'rollback':
            protocol = case.protocol()
            query(case.a, 'BEGIN')
            query(case.a, 'SAVEPOINT rewrite_one')
            query(case.a, 'INSERT INTO biz.heap_marker VALUES(91)')
            rewrite(case.a, selected_ids)
            query(case.a, 'ROLLBACK TO rewrite_one')
            query(case.a, 'RELEASE rewrite_one')
            for table in ['biz.orders', 'biz.exact']:
                query(case.a, 'UPDATE ' + table + ' SET val=val+1 WHERE id=5')
            query(case.a, 'COMMIT')
            check('savepoint restores DML eligibility and heap atomicity', query(case.observer, 'SELECT * FROM biz.heap_marker'), [])
            check('rollback leaves original files current', [x[0] for x in active_files()], original_ids)
            check('rollback candidate absent and source bytes unchanged', file_inventory(), original_objects)
            protocol = case.protocol()
            query(case.a, 'BEGIN')
            rewrite(case.a, selected_ids)
            query(case.a, 'ROLLBACK')
            check('whole rollback preserves complete protocol', case.protocol(), protocol)
            parity('savepoint and whole rollback preserve exact values')
        else:
            if args.mode == 'old-writer':
                query(case.a, 'BEGIN')
                check('old writer computes once from10', query(case.a, 'UPDATE biz.orders SET val=val+1 WHERE id=3 RETURNING val'), [(11,)])
                query(case.a, 'UPDATE biz.exact SET val=val+1 WHERE id=3')
            query(case.b, 'BEGIN')
            query(case.b, 'INSERT INTO biz.heap_marker VALUES(92)')
            rewrite(case.b, selected_ids)
            query(case.b, 'COMMIT')
            published = case.head()[0]
            check('rewrite allocates one layout snapshot', published, before_head[0] + 1)
            check('ordinary heap write commits atomically with layout', query(case.observer, 'SELECT * FROM biz.heap_marker'), [(92,)])
            after_files = active_files()
            new_files = [row for row in after_files if row[0] not in original_ids]
            check('exact new output population', len(new_files), 0 if args.mode == 'zero' else 1)
            check('unselected source intervals remain current', [row[0] for row in after_files if row[0] in original_ids], retained_ids)
            check('no extra or missing current sources', len(after_files), len(retained_ids) + len(new_files))
            ended = query(case.observer, 'SELECT data_file_id,end_snapshot FROM created_metadata.ducklake_data_file WHERE data_file_id=ANY(%s::bigint[]) ORDER BY data_file_id', (selected_ids,))
            check('all source intervals retired atomically at publication', ended, [(x, published) for x in selected_ids])
            check('associated historical delete interval ends at same publication', query(case.observer,
                'SELECT end_snapshot FROM created_metadata.ducklake_delete_file WHERE delete_file_id=%s', (deletion_id,)), [(published,)])
            check('next_file counter advances even zero-output cache generation', case.head()[3], before_head[3] + 1)
            after_stats = query(case.observer, 'SELECT record_count,next_row_id,file_size_bytes FROM created_metadata.ducklake_table_stats WHERE table_id=%s', (table_id,))[0]
            check('pure layout keeps logical count and row allocator', after_stats[:2], before_stats[:2])
            check('unknown global size remains unknown rather than invented', after_stats[2], None if before_stats[2] is None else before_stats[2] - sum(x[3] for x in selected) + sum(x[3] for x in new_files))
            if new_files:
                check('standard replacement begins now with NULL partial/range', query(case.observer,
                    'SELECT begin_snapshot,partial_max,row_id_start FROM created_metadata.ducklake_data_file WHERE data_file_id=%s', (new_files[0][0],)), [(published, None, None)])
                new_physical = physical(new_files, 'after')
                output_identity = [{key: value for key, value in row.items() if key not in ['file_id', 'position']}
                                   for row in new_physical]
                check('complete physical output is exactly the independent visible file versions',
                      sorted(output_identity, key=lambda x: x['logical_rowid']), expected_output_identity)
                moved = [x for x in new_physical if x['id'] == 3]
                check('target remains once with actual moved address', len(moved), 1)
                check('new target R is not its new physical P', moved[0]['logical_rowid'] != moved[0]['position'])
                check('layout preserves target R and birth', (moved[0]['logical_rowid'], moved[0]['begin_snapshot']),
                      (target[0]['logical_rowid'], target[0]['begin_snapshot']))
            if args.mode == 'zero':
                check('zero output creates no orphan candidate object', file_inventory(), original_objects)
            after_catalog, after_identity = capture_reference('after')
            check('standard current R/birth/value multiset unchanged', after_identity, before_identity)
            check('every retained historical snapshot remains exact', history(after_catalog, snapshots, 'after-history'), before_history)
            check('standard rewrite creates no logical CDC insertion/deletion',
                  cdc(after_catalog, published, published, 'rewrite-cdc'), [])
            check('old CDC interval remains exact after source retirement',
                  cdc(after_catalog, birth, before_head[0], 'after-old-interval-cdc'), expected_events)
            check('complete CDC through rewrite keeps every original event once',
                  cdc(after_catalog, birth, published, 'after-complete-cdc'), expected_events)
            if args.mode == 'old-writer':
                query(case.a, 'COMMIT')
                marks = query(case.observer, 'SELECT file_id,row_id FROM created_metadata.' + case.sql.Identifier('ducklake_inlined_delete_' + str(table_id)).as_string(case.observer) + ' ORDER BY 1,2')
                check('old journal retargets marker to actual new(F,P)', marks, [(new_files[0][0], moved[0]['position'])])
                check('old SET was not lost or repeated', scalar(case.observer, 'SELECT val FROM biz.orders WHERE id=3'), 11)
                writer_snapshot = case.head()[0]
                writer_catalog, writer_identity = capture_reference('after-old-writer')
                expected_writer_identity = deepcopy(before_identity)
                for row in expected_writer_identity:
                    if row['id'] == 3:
                        row.update(begin_snapshot=writer_snapshot, val=11)
                check('reference reads old writer new birth/value at same stable R', writer_identity, expected_writer_identity)
                check('old UPDATE never allocates a new stable row ID', scalar(case.observer,
                      'SELECT next_row_id FROM created_metadata.ducklake_table_stats WHERE table_id=%s', (table_id,)), before_stats[1])
                check('post-remap deletion leaves every original historical snapshot exact',
                      history(writer_catalog, snapshots + [published], 'post-writer-history'),
                      dict(before_history, **{str(published): before_identity}))
                writer_events = [dict(snapshot_id=writer_snapshot, logical_rowid=target[0]['logical_rowid'], change_type=kind,
                                      id=3, val=value) for kind, value in [('update_postimage',11), ('update_preimage',10)]]
                check('reference old-writer CDC contains one genuine update pair',
                      cdc(writer_catalog, writer_snapshot, writer_snapshot, 'old-writer-cdc'), writer_events)

            for path, expected in original_objects.items():
                check('historical source kept without mutation/' + Path(path).name, sha256(path), expected)
            if index_uuid:
                check('old-source coverage atomically inactive', scalar(case.observer,
                    'SELECT count(*) FROM og_lake_internal.index_coverage WHERE index_uuid=%s::uuid AND source_file_id=ANY(%s::bigint[]) AND active', (index_uuid, selected_ids)), 0)
                check('index definition stays ACTIVE', scalar(case.observer,
                    'SELECT state FROM og_lake_internal.index_definition WHERE index_uuid=%s::uuid', (index_uuid,)), 'ACTIVE')
                plan = '\n'.join(x[0] for x in query(case.a, 'EXPLAIN ANALYZE SELECT id,val FROM biz.orders WHERE id=3'))
                r['before_rebuild_plan'] = plan
                check('new uncovered file reports full fallback', ('Verified Scalar File Coverage: ' + str(len(retained_ids))) in plan and 'Complete File Fallback Inputs: 1' in plan)
                query(case.a, 'SELECT og_lake.build_lake_index(%s::uuid)', (index_uuid,))
                plan = '\n'.join(x[0] for x in query(case.a, 'EXPLAIN ANALYZE SELECT id,val FROM biz.orders WHERE id=3'))
                r['after_rebuild_plan'] = plan
                check('explicit scalar rebuild actually covers new file', 'Actual File Access: verified sorted Parquet candidates' in plan and ('Verified Scalar File Coverage: ' + str(len(after_files))) in plan)
            parity('committed rewrite and allowed old writer match ordinary heap')
        check('executed inputs remain byte-identical', all(sha256(path) == item['sha256'] == sha256(item['archive']) for path, item in r['inputs'].items()))
        check('hash-only immutable runtime/reference/SDK identities unchanged',
              all(sha256(path) == digest for path, digest in r['hash_only_inputs'].items()))
        check('new-kind SDK component original proofs remain exact',sdk_index(args.sdk_index,args.sdk_index_sha256,ROOT),selected_sdk)
        check_build(args.build_receipt)
        r['status'] = 'pass'
    except BaseException as exc:
        r.update(reason=str(exc), sqlstate=getattr(exc, 'pgcode', None), traceback=traceback.format_exc())
    finally:
        if case is not None:
            for c in [case.a, case.b, case.observer]:
                try:
                    if not c.closed:
                        query(c, 'ROLLBACK')
                except BaseException as exc:
                    r.setdefault('rollback_errors', []).append(str(exc))
        if db is not None:
            try:
                db.cleanup()
                r['database_cleanup'] = db.cleanup_result
            except BaseException as exc:
                r.update(status='fail', database_cleanup_error=str(exc))
        if roles is not None:
            try:
                if not r.get('database_cleanup') or r['database_cleanup']['returncode']:
                    raise RuntimeError('Owned DB cleanup unproven; preserve roles')
                r['role_cleanup'] = cleanup_roles(control, roles.path, args.run_id)
                r = roles.redact(r)
            except BaseException as exc:
                r.update(status='fail', role_cleanup_error=str(exc))
        if control is not None:
            try:
                control.close()
            except BaseException as exc:
                r.update(status='fail', control_cleanup_error=str(exc))
        if reservation is not None:
            held = r.get('reference_reservation', {}).get('acquired', False)
            if held:
                fcntl.flock(reservation.fileno(), fcntl.LOCK_UN)
            reservation.close()
            r.setdefault('reference_reservation', {})['released_after_cleanup_attempt'] = held
        write_json(out / 'result.json', r)
    return 0 if r['status'] == 'pass' else 1


if __name__ == '__main__':
    raise SystemExit(main())
