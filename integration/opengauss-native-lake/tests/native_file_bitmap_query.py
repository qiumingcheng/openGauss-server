#!/usr/bin/env python3
"""Owned native int4 bitmap Parquet correctness and publication cases.

Use one fresh run ID per mode and serialize fixture-producing cases. This uses
the genuine DuckLake EMPTY fixture and installed SDK, compares every query to
an ordinary heap, and independently decodes stored bitmaps through DuckDB and
a test-only portable-format oracle. RC Debug only: no performance, PQ, SMP,
automatic maintenance, remote/RAC, physical GC or production qualification.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import traceback
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[3]
D = ROOT / 'integration/opengauss-native-lake'
sys.path[:0] = [str(D), str(D / 'tests')]
from bitmap_format_component import portable_positions
from file_dml_acceptance import check_build
from inline_fixture import query, scalar
from lake_harness import sha256, validate_run_id, write_json
from native_auto_flush import connect_control
from native_runtime import INSTALL
from native_test_context import OwnedDatabase
from native_table_create import Case
from permission_api import OwnedRoles, cleanup_roles


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--build-receipt', type=Path, required=True)
    parser.add_argument('--mode', required=True, choices=[
        'predicates', 'visibility', 'repair', 'transactions', 'isolation'])
    args = parser.parse_args()
    validate_run_id(args.run_id)
    out = ROOT / '公共文档/openGauss原生湖存储/验证' / args.run_id
    out.mkdir(exist_ok=False)
    result = dict(status='fail', scope=__doc__, mode=args.mode, assertions=[])
    control = db = roles = case = None
    inputs = [Path(__file__), D / 'tests/bitmap_format_component.py',
              D / 'tests/native_table_create.py', D / 'tests/permission_api.py',
              D / 'native_test_context.py', D / 'native_runtime.py',
              D / 'tests/inline_cleanup.py', D / 'tests/file_dml_acceptance.py',
              D / 'inline_fixture.py', D / 'tests/native_auto_flush.py',
              D / 'tests/catalog_fixture_bundle.py', D / 'tests/empty_reference_fixture.py',
              D / 'tests/empty_manifest.py', D / 'tests/empty_catalog_oracle.py',
              D / 'tests/create_reference_oracle.py', D / 'reference.sh',
              D / 'tests/reverse_reference.py', D / 'fixture.py', D / 'lake_harness.py',
              D / 'source-lock.json']
    result['inputs'] = {str(path): sha256(path) for path in inputs}
    archive = out / 'executed-inputs'
    archive.mkdir()
    for path in inputs:
        shutil.copy2(path, archive / path.name)
    provider = INSTALL / 'lib/postgresql/og_lake_sdk/libog_lake_sdk_arrow.so'
    duckdb = ROOT / '.reference/opengauss-native-lake/build/duckdb-gcc10/duckdb'

    def image(value):
        if isinstance(value, list) and len(value) > 30:
            return dict(rows=len(value), sha256=hashlib.sha256(repr(value).encode()).hexdigest())
        return value

    def check(name, observed, expected=True):
        good = observed == expected
        result['assertions'].append(dict(name=name, observed=image(observed),
                                         expected=image(expected), status='pass' if good else 'fail'))
        if not good:
            raise AssertionError(name + ': ' + repr(image(observed)))

    def literal(value):
        return "'" + str(value).replace("'", "''") + "'"

    def independent(statement, label):
        (out / (label + '.sql')).write_text(statement + '\n')
        proc = subprocess.run([str(duckdb), '-no-init', '-batch', '-json', '-c',
                               'SET threads=1;' + statement], capture_output=True,
                              text=True, timeout=90)
        (out / (label + '.stdout')).write_text(proc.stdout)
        (out / (label + '.stderr')).write_text(proc.stderr)
        if proc.returncode:
            raise RuntimeError('Independent DuckDB read failed: ' + label)
        return json.loads(proc.stdout) if proc.stdout.strip() else []

    try:
        built = check_build(args.build_receipt)
        result.update(source_head=built['source_head'], binary_sha256=built['binary_sha256'],
                      provider_sha256=sha256(provider), reference_binary_sha256=sha256(duckdb),
                      build_receipt=str(args.build_receipt.resolve()),
                      build_receipt_sha256=sha256(args.build_receipt))
        control = connect_control()
        check('automatic scheduler remains off', scalar(control, 'SHOW enable_lake_maintenance'), 'off')
        with (out / 'fixture.log').open('w') as log:
            subprocess.run([sys.executable, str(D / 'tests/catalog_fixture_bundle.py'),
                            '--run-id', args.run_id, '--output', str(out / 'fixture/manifest.json')],
                           stdout=log, stderr=subprocess.STDOUT, check=True, timeout=300)
        db = OwnedDatabase(args.run_id, out).create()
        roles = OwnedRoles(control, args.run_id, out)
        case = Case(db, SimpleNamespace(mode='net_dml', sql_version='0.16',
                    manifest=out / 'fixture/manifest.json', output=out / 'result.json',
                    run_id=args.run_id), result, roles)
        conn = case.a
        case.create()
        query(conn, 'CREATE TABLE biz.reference(id integer,val integer)')
        query(conn, "SELECT og_lake.set_option('biz.orders'::regclass,'data_inlining_row_limit','50000')")

        def both(statement):
            query(conn, 'BEGIN')
            try:
                for table in ['biz.orders', 'biz.reference']:
                    query(conn, statement.format(table=table))
                query(conn, 'COMMIT')
            except BaseException:
                query(conn, 'ROLLBACK')
                raise

        both('INSERT INTO {table} SELECT CASE WHEN i%17=0 THEN NULL ELSE i%101-50 END,i '
             'FROM generate_series(1,12000)i')
        both('INSERT INTO {table} VALUES(7,70007),(7,70008)')
        both('UPDATE {table} SET val=val+20000 WHERE id=7')
        both('UPDATE {table} SET val=val+20000 WHERE id=7')
        both('DELETE FROM {table} WHERE id=11')
        query(conn, 'ANALYZE biz.orders')
        query(conn, "SELECT og_lake.flush('biz.orders'::regclass)")
        store = out / 'index-store'
        store.mkdir()
        query(conn, "SELECT og_lake.set_option('created_lake'::name,'index_root',%s::text)", (str(store),))

        def create_index(name, method='bitmap'):
            return str(scalar(conn,
                "SELECT og_lake.create_lake_index('biz.orders'::regclass,%s::name,%s::jsonb)",
                (name, json.dumps(dict(version=1, kind='scalar', method=method, column='id')))))

        index_uuid = create_index('bitmap_id')

        def build(uuid=index_uuid):
            return query(conn, 'SELECT og_lake.build_lake_index(%s::uuid)', (uuid,))

        def segments(uuid=index_uuid, connection=None):
            return query(connection or case.observer,
                'SELECT segment_uuid::text,state,artifact_root_uri,manifest_hash,algorithm_version,format_version '
                'FROM og_lake_internal.index_segment WHERE index_uuid=%s::uuid ORDER BY segment_uuid', (uuid,))

        def coverage(uuid=index_uuid, connection=None):
            return query(connection or case.observer,
                'SELECT c.segment_uuid::text,c.source_file_id,c.source_path,c.physical_row_count,'
                'c.source_checksum,c.schema_mapping_hash,c.active,s.artifact_root_uri '
                'FROM og_lake_internal.index_coverage c JOIN og_lake_internal.index_segment s '
                'USING(segment_uuid,index_uuid,definition_generation) WHERE c.index_uuid=%s::uuid '
                'ORDER BY c.source_file_id,c.segment_uuid', (uuid,))

        def status(uuid=index_uuid):
            return json.loads(scalar(conn, 'SELECT og_lake.lake_index_status(%s::uuid)', (uuid,)))

        def owned_directory(path):
            path = Path(path).resolve()
            check('derived directory belongs to this exact attempt', path.is_relative_to(store.resolve()))
            return path

        def equal(predicate='true', values=None, label=None):
            statement = 'SELECT id,val FROM {} WHERE ' + predicate + ' ORDER BY id NULLS LAST,val'
            got = query(conn, statement.format('biz.orders'), values)
            expected = query(conn, statement.format('biz.reference'), values)
            check(label or 'heap differential: ' + predicate, got, expected)
            return got

        def plan(label, predicate='id=7'):
            text = '\n'.join(row[0] for row in query(conn,
                'EXPLAIN (ANALYZE TRUE,COSTS FALSE) SELECT id,val FROM biz.orders WHERE ' + predicate))
            path = out / (label + '-plan.txt')
            path.write_text(text + '\n')
            result.setdefault('actual_plans', []).append(dict(label=label, path=str(path), text=text))
            return text

        def count_label(text, label):
            found = re.findall(r'^\s*' + re.escape(label) + r':\s*(\d+)\s*$', text, re.M)
            check('actual EXPLAIN contains one ' + label, len(found), 1)
            return int(found[0])

        def actual(label='bitmap-active', predicate='id=7'):
            text = plan(label, predicate)
            check('executor actually uses bitmap candidates: ' + label,
                  'Actual File Access: verified bitmap Parquet candidates' in text)
            check('verified bitmap coverage is positive: ' + label,
                  count_label(text, 'Verified Bitmap File Coverage') > 0)
            return text

        def verify_artifacts(label):
            live = [row for row in coverage() if row[6]]
            check('bitmap has active physical coverage: ' + label, bool(live))
            for ordinal, row in enumerate(live):
                directory = owned_directory(row[7])
                data = directory / 'bitmap_data.parquet'
                check('one bitmap artifact replaces sorted data/lookup: ' + label,
                      data.is_file() and not (directory / 'page_data.parquet').exists()
                      and not (directory / 'page_lookup.parquet').exists())
                dictionaries = independent('SELECT key,hex(bitmap) AS payload,cardinality FROM read_parquet('
                    + literal(data) + ') ORDER BY key NULLS LAST', label + '-dictionary-' + str(ordinal))
                physical = independent('SELECT id,file_row_number AS row_position FROM read_parquet('
                    + literal(row[2]) + ',file_row_number=true) ORDER BY file_row_number',
                    label + '-source-' + str(ordinal))
                decoded = []
                for entry in dictionaries:
                    positions = portable_positions(bytes.fromhex(entry['payload']))
                    check('independent bitmap cardinality: ' + label + '/' + str(ordinal) + '/' + str(entry['key']),
                          len(positions), entry['cardinality'])
                    decoded.extend((entry['key'], position) for position in positions)
                order = lambda value: (value[0] is None, value[0] or 0, value[1])
                check('every stored key/physical position matches real source: ' + label + '/' + str(ordinal),
                      sorted(decoded, key=order), sorted([(x['id'], x['row_position']) for x in physical], key=order))
                check('coverage counts physical rows, not dictionary rows: ' + label + '/' + str(ordinal),
                      len(decoded), row[3])
                check('low NDV remains distinct from source population: ' + label + '/' + str(ordinal),
                      len(dictionaries) < row[3])

        build()
        initial_segments = segments()
        check('bitmap segments use explicit integer algorithm and format2',
              bool(initial_segments) and all(row[4:] == (1, 2) for row in initial_segments))
        check('READY status advertises runtime validation only', status().get('file_runtime_validation'), True)
        verify_artifacts('initial')
        build()
        check('healthy rebuild reuses immutable bitmap segments', segments(), initial_segments)
        both('INSERT INTO {table} VALUES(7,-70),(NULL,-71),(20000,-72)')
        query(conn, 'SET enable_seqscan=off')
        actual('mixed-inline-file')
        equal('id=7')

        if args.mode == 'predicates':
            for label, predicate in [('range', 'id<0'), ('null', 'id IS NULL'),
                                     ('in', 'id=ANY(ARRAY[7,-1,NULL]::integer[])')]:
                actual('bitmap-' + label, predicate)
            for predicate in ['id<0', 'id<=0', 'id>0', 'id>=0', '7=id', 'id IS NULL', 'id IS NOT NULL',
                              'id=7::smallint', "id>'2147483647'::bigint", "id<'-2147483649'::bigint",
                              'id=ANY(ARRAY[7,-1,7,NULL,20000]::bigint[])', 'id=ANY(ARRAY[]::integer[])',
                              'id=ANY(NULL::integer[])', 'id=ANY(ARRAY[NULL]::integer[])',
                              'id<>7', 'val=1 OR id=7']:
                equal(predicate)
            query(conn, 'SET plan_cache_mode=force_generic_plan')
            query(conn, 'PREPARE bitmap_key(bigint) AS SELECT id,val FROM biz.orders WHERE id=$1 ORDER BY val')
            for value in [7, 20000, -1, None, 2147483648, -2147483649, 7, 0, 7]:
                sent = None if value is None else str(value)
                check('generic cross-integer parameter: ' + str(value),
                      query(conn, 'EXECUTE bitmap_key(%s)', (sent,)),
                      query(conn, 'SELECT id,val FROM biz.reference WHERE id=%s::bigint ORDER BY val', (sent,)))
            query(conn, 'DEALLOCATE bitmap_key')
            query(conn, 'RESET plan_cache_mode')
            query(conn, 'CREATE TEMP TABLE probe_keys(id integer)')
            query(conn, 'INSERT INTO probe_keys VALUES(7),(-1),(20000),(7),(NULL)')
            statement = ('SELECT k.id,(SELECT min(o.val) FROM {} o WHERE o.id=k.id) '
                         'FROM probe_keys k ORDER BY k.id NULLS LAST')
            check('correlated parameter rescans retain all matching rows',
                  query(conn, statement.format('biz.orders')), query(conn, statement.format('biz.reference')))
            query(conn, 'SET enable_indexscan=off')
            equal('id=7', label='disabled native index path preserves bitmap query result')
            query(conn, 'RESET enable_indexscan')
            actual('after-rescans')

        elif args.mode == 'visibility':
            both('UPDATE {table} SET id=19001,val=val+1 WHERE id=7')
            both('UPDATE {table} SET val=val+1 WHERE id=19001')
            equal('id=7')
            equal('id=19001', label='two updates replace every original file/inline version exactly once')
            query(conn, 'BEGIN')
            for table in ['biz.orders', 'biz.reference']:
                query(conn, 'UPDATE ' + table + ' SET id=19002 WHERE id=19001')
            equal('id=19001')
            equal('id=19002')
            query(conn, 'SAVEPOINT changed')
            for table in ['biz.orders', 'biz.reference']:
                query(conn, 'DELETE FROM ' + table + ' WHERE id=19002')
            equal('id=19002', label='private delete hides every updated version')
            query(conn, 'ROLLBACK TO changed')
            query(conn, 'RELEASE SAVEPOINT changed')
            equal('id=19002', label='savepoint rollback restores exact private after-images')
            query(conn, 'ROLLBACK')
            equal('id=19001', label='whole rollback restores both committed updates')
            query(conn, "SELECT og_lake.flush('biz.orders'::regclass)")
            equal(label='flush preserves entire heap multiset before rebuilding bitmap')
            text = plan('uncovered-new-file')
            check('flush leaves new sources in complete fallback', count_label(text, 'Complete File Fallback Inputs') > 0)
            build()
            actual('after-flush-rebuild')
            equal('id=19001', label='new bitmap coverage cannot resurrect ended versions')
            verify_artifacts('after-flush')

        elif args.mode == 'repair':
            old = next(row for row in coverage() if row[6])
            directory = owned_directory(old[7])
            data = directory / 'bitmap_data.parquet'
            original = data.read_bytes()
            (out / 'original-bitmap-data.parquet').write_bytes(original)
            check('damage target contains a real Parquet footer', len(original) > 16)
            head, protocol = case.head(), case.protocol()
            with data.open('r+b') as stream:
                stream.truncate(len(original) - 16)
                stream.flush()
                os.fsync(stream.fileno())
            equal('id=7', label='corrupt bitmap falls back without partial query output')
            equal('id IS NULL', label='corrupt NULL bitmap cannot lose rows')
            text = plan('corrupt-bitmap')
            check('damaged bitmap has no verified runtime coverage', count_label(text, 'Verified Bitmap File Coverage'), 0)
            check('damaged artifact uses complete source scan', 'Actual File Access: complete SDK scan' in text)
            before = segments(), coverage()
            query(conn, 'BEGIN')
            query(conn, 'SAVEPOINT repair_attempt')
            build()
            pending = next(row for row in coverage(connection=conn) if row[6] and row[1] == old[1])
            pending_directory = owned_directory(pending[7])
            check('private repair creates a fresh segment', pending[0] != old[0])
            check('private repair completes a real bitmap artifact', (pending_directory / 'bitmap_data.parquet').is_file())
            query(conn, 'ROLLBACK TO repair_attempt')
            query(conn, 'RELEASE SAVEPOINT repair_attempt')
            query(conn, 'COMMIT')
            check('savepoint restores exact old coverage and segment state', (segments(), coverage()), before)
            check('savepoint removes completed repaired candidates', list(pending_directory.glob('*.parquet')), [])
            equal('id=7', label='aborted repair preserves complete fallback')
            build()
            repaired = next(row for row in coverage() if row[6] and row[1] == old[1])
            check('committed repair publishes another fresh segment', repaired[0] not in [old[0], pending[0]])
            check('repair preserves source identity and population', repaired[1:6], old[1:6])
            check('damaged former segment becomes retired', next(row[1] for row in segments() if row[0] == old[0]), 'RETIRED')
            actual('repaired-bitmap')
            equal(label='repair preserves full result including inline rows')
            verify_artifacts('repaired')
            check('repair never allocates business snapshot or row IDs', case.head(), head)
            check('repair leaves all standard protocol/carrier records unchanged', case.protocol(), protocol)

        elif args.mode == 'transactions':
            before = segments(), coverage()
            head, protocol = case.head(), case.protocol()
            prior_files = set(store.rglob('*.parquet'))
            for label, savepoint in [('savepoint', True), ('transaction', False)]:
                query(conn, 'BEGIN')
                if savepoint:
                    query(conn, 'SAVEPOINT build_attempt')
                temporary = create_index('bitmap_rollback_' + label)
                build(temporary)
                private = coverage(temporary, conn)
                check(label + ' build has private complete physical coverage', bool(private) and all(row[6] for row in private))
                paths = [owned_directory(row[7]) / 'bitmap_data.parquet' for row in private]
                check(label + ' candidate artifacts really exist', all(path.is_file() for path in paths))
                if savepoint:
                    query(conn, 'ROLLBACK TO build_attempt')
                    query(conn, 'RELEASE SAVEPOINT build_attempt')
                    query(conn, 'COMMIT')
                else:
                    query(conn, 'ROLLBACK')
                check(label + ' rollback removes new definition', scalar(conn,
                    'SELECT count(*) FROM og_lake_internal.index_definition WHERE index_uuid=%s::uuid', (temporary,)), 0)
                check(label + ' rollback removes completed candidate bytes', all(not path.exists() for path in paths))
                check(label + ' rollback retains every prior artifact', set(store.rglob('*.parquet')), prior_files)
                check(label + ' rollback preserves original publication', (segments(), coverage()), before)
                equal(label=label + ' rollback preserves every business row')
            check('index-only transactions allocate no business identity', case.head(), head)
            check('index-only transactions preserve standard protocol', case.protocol(), protocol)
            actual('after-build-rollbacks')

        else:
            sorted_uuid = create_index('sorted_id', 'btree')
            build(sorted_uuid)
            check('same-column sorted artifact retains format1',
                  bool(segments(sorted_uuid)) and all(row[4:] == (1, 1) for row in segments(sorted_uuid)))
            definitions = query(conn, "SELECT definition->>'method',definition_hash::text,column_id "
                'FROM og_lake_internal.index_definition WHERE index_uuid=ANY(%s::uuid[]) ORDER BY definition->>\'method\'',
                ([index_uuid, sorted_uuid],))
            check('both logical methods coexist on one stable column',
                  len(definitions) == 2 and definitions[0][0] == 'bitmap' and definitions[1][0] == 'btree'
                  and definitions[0][1] != definitions[1][1] and definitions[0][2] == definitions[1][2])
            bitmap_ready, sorted_ready = status()['ready_segments'], status(sorted_uuid)['ready_segments']
            check('both methods independently report READY segments', bitmap_ready > 0 and sorted_ready > 0)
            for uuid, wrong_format, blocked_label, other_uuid, other_ready in [
                    (index_uuid, 1, 'Verified Bitmap File Coverage', sorted_uuid, sorted_ready),
                    (sorted_uuid, 2, 'Verified Scalar File Coverage', index_uuid, bitmap_ready)]:
                query(conn, 'BEGIN')
                query(conn, 'UPDATE og_lake_internal.index_segment SET format_version=%s WHERE index_uuid=%s::uuid',
                      (wrong_format, uuid))
                check('wrong method format never qualifies as READY: ' + uuid, status(uuid)['ready_segments'], 0)
                check('wrong format does not affect the other method: ' + uuid, status(other_uuid)['ready_segments'], other_ready)
                equal('id=7', label='cross-method format mismatch retains complete result: ' + uuid)
                text = plan('wrong-format-' + str(wrong_format))
                # Scalar coverage counts all scalar methods in the current
                # public EXPLAIN. Verify sorted exclusion by its access label.
                if uuid == index_uuid:
                    check('sorted bytes are not decoded as bitmap', count_label(text, blocked_label), 0)
                else:
                    check('bitmap bytes are not decoded as sorted pages',
                          'Actual File Access: verified sorted Parquet candidates' not in text)
                query(conn, 'ROLLBACK')
            check('rollback restores bitmap READY count', status()['ready_segments'], bitmap_ready)
            check('rollback restores sorted READY count', status(sorted_uuid)['ready_segments'], sorted_ready)
            query(conn, 'SELECT og_lake.drop_lake_index(%s::uuid)', (sorted_uuid,))
            actual('bitmap-after-sorted-drop')
            equal('id IS NULL', label='retiring the other method cannot hide NULL bitmap positions')

        equal(label='final complete heap multiset differential')
        check('executed test inputs remain frozen', all(sha256(Path(path)) == digest for path, digest in result['inputs'].items()))
        check('genuine provider identity remains unchanged', sha256(provider), result['provider_sha256'])
        check('independent reference binary remains unchanged', sha256(duckdb), result['reference_binary_sha256'])
        check('selected official receipt remains unchanged', sha256(args.build_receipt), result['build_receipt_sha256'])
        check_build(args.build_receipt)
        result['status'] = 'pass'
    except BaseException as exc:
        result.update(reason=str(exc), traceback=traceback.format_exc())
    finally:
        if db is not None:
            try:
                db.cleanup()
                result['database_cleanup'] = db.cleanup_result
            except BaseException as exc:
                result.update(status='fail', cleanup_error=str(exc))
        if roles is not None:
            try:
                if not result.get('database_cleanup') or result['database_cleanup']['returncode']:
                    raise RuntimeError('Owned DB cleanup unproven; roles retained')
                result['role_cleanup'] = cleanup_roles(control, roles.path, args.run_id)
                result = roles.redact(result)
            except BaseException as exc:
                result.update(status='fail', role_cleanup_error=str(exc))
        if control is not None:
            control.close()
        result['artifacts'] = {str(path): sha256(path) for path in out.rglob('*')
                               if path.is_file() and path.suffix in ['.parquet', '.sql', '.stdout', '.stderr', '.txt']}
        write_json(out / 'result.json', result)
    print(json.dumps(dict(status=result['status'], checks=len(result['assertions']),
                          reason=result.get('reason'), result=str(out / 'result.json')), ensure_ascii=False))
    return 0 if result['status'] == 'pass' else 1


if __name__ == '__main__':
    raise SystemExit(main())
