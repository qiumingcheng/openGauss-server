#!/usr/bin/env python3
"""Statement-cache qualification over the frozen full Boolean/PQ512 matrix.
Bounded native file ANN boolean-filter integration against original heap SQL.

Raw mode uses two64-row vector(3) files. Optional PQ mode requires SQL0.18/0.19,
two 512-row vector(16) files and the qualified CPU-PQ build. Actual EXPLAIN,
per-file mask counts and same-query parameter epochs must prove activation;
PQ artifacts alone cannot prove that PQ scored a frontier. Neither mode is an
ANN recall, SMP, remote, RAC, performance or commercial qualification.
This new draft has not run. Serialize all fixture generation and database use.
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
import threading
import time
import traceback
from types import SimpleNamespace

ROOT = Path(os.getenv('CPP_WORKSPACE', str(Path(__file__).resolve().parents[3]))).resolve()
D = ROOT / 'integration/opengauss-native-lake'
sys.path[:0] = [str(D), str(D / 'tests')]
from file_dml_acceptance import check_build
from empty_catalog_oracle import compare_initialized
from empty_manifest import validate_empty_bundle, validate_empty_manifest
from create_reference_oracle import export_created_catalog
from inline_fixture import query, scalar
from lake_harness import sha256, validate_run_id, write_json
from native_auto_flush import connect_control
from native_runtime import PROFILE, INSTALL
from native_test_context import OwnedDatabase
from permission_api import OwnedRoles, cleanup_roles


class NoticeCapture:
    """Keep mask/frontier evidence independently of unrelated DEBUG volume."""
    TARGETS = ('lake filtered ANN candidates', 'lake file ANN candidates')

    def __init__(self, target_records=8192, target_bytes=8 * 1024 * 1024):
        self.target_record_limit, self.target_byte_limit = target_records, target_bytes
        self.clear()

    def clear(self):
        self.items, self.discarded = [], 0  # discarded counts target records only
        self.target_seen = self.target_bytes_seen = self.target_bytes_retained = 0
        self.dropped_target_bytes = self.noise_count = 0
        self.noise_tail = []

    def append(self, message):
        if not any(label in message for label in self.TARGETS):
            self.noise_count += 1
            self.noise_tail.append(dict(text=message[:1024], truncated=len(message) > 1024))
            if len(self.noise_tail) > 16:
                del self.noise_tail[0]
            return
        self.target_seen += 1
        size = len(message.encode('utf-8', errors='replace'))
        self.target_bytes_seen += size
        if (len(self.items) >= self.target_record_limit or
                size > self.target_byte_limit - self.target_bytes_retained):
            self.discarded += 1
            self.dropped_target_bytes += size
            return
        self.items.append(message)
        self.target_bytes_retained += size

    def summary(self):
        return dict(target_record_limit=self.target_record_limit, target_byte_limit=self.target_byte_limit,
                    target_seen=self.target_seen, target_retained=len(self.items),
                    target_bytes_seen=self.target_bytes_seen, target_bytes_retained=self.target_bytes_retained,
                    dropped_target_records=self.discarded, dropped_target_bytes=self.dropped_target_bytes,
                    noise_count=self.noise_count, noise_tail_limit=16, noise_prefix_char_limit=1024,
                    noise_tail=list(self.noise_tail))

    def __iter__(self):
        return iter(self.items)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--build-receipt', type=Path, required=True)
    parser.add_argument('--metric', choices=['l2', 'ip', 'cosine'], default='l2')
    parser.add_argument('--storage', choices=['heap', 'ustore'], default='heap')
    parser.add_argument('--sql-version', choices=['0.16', '0.18', '0.19'], default='0.19')
    parser.add_argument('--pq', choices=['off', 'on'], default='on')
    args = parser.parse_args()
    validate_run_id(args.run_id)
    if PROFILE != 'ann-debug':
        parser.error('Requires the matching separately owned ann-debug runtime')
    if args.pq == 'on' and args.sql_version not in ('0.18', '0.19'):
        parser.error('PQ + boolean integration requires the explicit SQL0.18 or0.19 capability')
    if args.pq != 'on':
        parser.error('This cache qualification requires the real PQ512-file path; no raw-only cache PASS')
    pq = args.pq == 'on'
    dimension, file_rows = (16, 512) if pq else (3, 64)
    file_ranges = [(1, file_rows), (file_rows + 1, file_rows * 2)]
    note_width = len(str(file_rows * 2 + 16))
    def vector_literal(first_three):
        return '[' + ','.join(map(str, first_three + ([.019 * (i + 1) for i in range(13)] if pq else []))) + ']'
    vectors = [vector_literal([.37, 1.73, -.11]), vector_literal([.91, -.41, 2.17]), vector_literal([7.23, 1.19, .53])]
    out = ROOT / '公共文档/openGauss原生湖存储/验证' / args.run_id
    out.mkdir(exist_ok=False)
    result = dict(status='fail', scope=__doc__, metric=args.metric, storage=args.storage,
                  sql_version=args.sql_version, pq_requested=pq, dimension=dimension, physical_rows_per_file=file_rows,
                  assertions=[], actual_plans=[], statement_timeout_ms=600000,
                  correlated_rows=512, memory_samples=[])
    control = database = roles = case = connection = None
    worker = None
    dependencies = ['tests/file_dml_acceptance.py',
                    'tests/permission_api.py', 'native_test_context.py', 'native_runtime.py',
                    'tests/inline_cleanup.py', 'inline_fixture.py', 'tests/native_auto_flush.py',
                    'tests/catalog_fixture_bundle.py', 'tests/empty_manifest.py',
                    'tests/empty_catalog_oracle.py', 'tests/create_reference_oracle.py',
                    'tests/empty_reference_fixture.py', 'tests/reverse_reference.py',
                    'reference.sh', 'fixture.py', 'lake_harness.py', 'source-lock.json']
    files = [Path(__file__).resolve(), args.build_receipt.resolve()] + [D / name for name in dependencies]
    result['inputs'] = {str(path): sha256(path) for path in files}
    archive = out / 'executed-inputs'
    archive.mkdir()
    for path in files:
        shutil.copy2(path, archive / path.name)
    notices = NoticeCapture()
    latest_plan_notices = []

    def image(value):
        if isinstance(value, set):
            return sorted(value)
        if isinstance(value, list) and len(value) > 40:
            return dict(rows=len(value), sha256=hashlib.sha256(repr(value).encode()).hexdigest())
        return value

    def check(name, observed, expected=True):
        passed = observed == expected
        result['assertions'].append(dict(name=name, observed=image(observed), expected=image(expected),
                                         status='pass' if passed else 'fail'))
        if not passed:
            raise AssertionError(name + ': ' + repr(observed))

    def plan(label, statement, values=None, analyze=True):
        nonlocal latest_plan_notices
        notices.clear()
        prefix = 'EXPLAIN (ANALYZE,COSTS OFF) ' if analyze else 'EXPLAIN (COSTS OFF) '
        text = '\n'.join(row[0] for row in query(connection, prefix + statement, values))
        path = out / (label + '-plan.txt')
        path.write_text(text + '\n')
        write_json(out / (label + '-notice-capture.json'), notices.summary())
        check(label + ': bounded target diagnostic capture did not overflow', notices.discarded, 0)
        latest_plan_notices = list(notices)
        notice_path = out / (label + '-notices.txt')
        notice_path.write_text(''.join(latest_plan_notices))
        result['actual_plans'].append(dict(label=label, path=str(path), text=text, notices=str(notice_path)))
        return text

    def count_label(text, label):
        counts = re.findall(r'^\s*' + re.escape(label) + r':\s*(\d+)\s*$', text, re.M)
        check('one actual EXPLAIN label: ' + label, len(counts), 1)
        return int(counts[0])

    try:
        build = check_build(args.build_receipt)
        result.update(source_head=build['source_head'], binary_sha256=build['binary_sha256'],
                      build_receipt=str(args.build_receipt.resolve()), build_receipt_sha256=sha256(args.build_receipt))
        source_root = Path(build['source'])
        for name in ['lake_ann_rescan.cpp', 'lake_ann_rescan.h', 'lake_file_ann.cpp', 'lake_file_ann_catalog.cpp',
                     'lake_file_reader.cpp', 'lake_inline_scan.cpp', 'lake_test_hooks.cpp', 'lake_pq_test.h']:
            relative = 'contrib/og_lake/' + name
            actual = source_root / relative
            check('official frozen build includes cache integration/' + name, build['native_source_inputs_after'].get(relative), sha256(actual))
            result['inputs'][str(actual)] = sha256(actual)
            shutil.copy2(actual, archive / name)
        provider = INSTALL / 'lib/postgresql/og_lake_sdk/libog_lake_sdk_arrow.so'
        result['provider_sha256'] = sha256(provider)
        control = connect_control()
        query(control, 'SET session_timeout=0')
        query(control, 'SET statement_timeout=600000')
        check('automatic maintenance remains off', scalar(control, 'SHOW enable_lake_maintenance'), 'off')
        check('thread memory observations use the ordinary dedicated backend', scalar(control, 'SHOW enable_thread_pool'), 'off')
        with (out / 'fixture.log').open('w') as log:
            subprocess.run([sys.executable, str(D / 'tests/catalog_fixture_bundle.py'), '--run-id', args.run_id,
                            '--output', str(out / 'fixture/manifest.json')], stdout=log, stderr=subprocess.STDOUT,
                           check=True, timeout=300)
        database = OwnedDatabase(args.run_id, out)
        database.create()
        query(database.admin, 'SET session_timeout=0')
        roles = OwnedRoles(control, args.run_id, out)
        # This draft owns its selected SQL setup; do not alter or bypass the
        # frozen Case runtime whitelist used by historical executed tests.
        case = SimpleNamespace(a=database.connect(), b=database.connect(), observer=database.connect())
        query(case.a, "CREATE EXTENSION og_lake VERSION '" + args.sql_version + "'")
        version_header = Path(build['source']) / 'contrib/og_lake/lake_version.h'
        result['inputs'][str(version_header)] = sha256(version_header)
        shutil.copy2(version_header, archive / version_header.name)
        runtime = re.findall(r'^#define OG_LAKE_MODULE_VERSION "([^"]+)"', version_header.read_text(), re.M)
        check('one frozen source declares the Native runtime version', len(runtime), 1)
        check('actual runtime matches selected source', json.loads(scalar(case.a, 'SELECT og_lake.host_status()'))['module_version'], runtime[0])
        if args.sql_version in ('0.18', '0.19'):
            check('selected SQL capability runs its module or explicit retained19 on reviewed20/21/22',
                  runtime[0] == args.sql_version or (args.sql_version == '0.19' and runtime[0] in ('0.20', '0.21', '0.22')))
        check('exact installed SQL capability', scalar(case.a, "SELECT extversion FROM pg_extension WHERE extname='og_lake'"), args.sql_version)
        bundle = validate_empty_bundle(out / 'fixture/manifest.json')
        entry = bundle['entries']['same']
        check('actual EMPTY manifest has its recorded SHA', sha256(entry['manifest']), entry['sha256'])
        empty, empty_export = validate_empty_manifest(entry['manifest'])
        owner = roles.create('private', False)
        query(case.a, 'SELECT og_lake.configure_internal_owner(%s::name)', (owner,))
        query(case.a, 'SELECT og_lake.create_catalog(%s::name,%s::name,%s::text,NULL::text)',
              ('created_lake', 'created_metadata', empty['data_path']))
        initial = export_created_catalog(case.observer, 'created_metadata', empty_export, out / 'native-initial.json')
        result['initial_independent_comparison'] = compare_initialized(empty_export, initial, empty['data_path'],
                                                                      'openGauss og_lake ' + runtime[0])
        query(case.a, 'CREATE SCHEMA biz')
        def catalog_head():
            return query(case.observer, 'SELECT snapshot_id,schema_version,next_catalog_id,next_file_id '
                         'FROM created_metadata.ducklake_snapshot ORDER BY snapshot_id DESC LIMIT 1')[0]
        case.head = catalog_head
        connection = case.a
        connection.notices = notices
        for owned in (case.a, case.b, case.observer):
            query(owned, 'SET session_timeout=0')
            query(owned, 'SET statement_timeout=600000')
        shape = '(id integer,k bigint,f double precision,v vector(' + str(dimension) + '),note text)'
        query(connection, 'CREATE TABLE biz.exact' + shape)
        if args.storage == 'ustore':
            query(connection, 'SET enable_default_ustore_table=on')
        query(connection, 'CREATE FOREIGN TABLE biz.vectors' + shape +
              " SERVER created_lake OPTIONS(binding_mode 'create',lake_schema 'main')")
        query(connection, 'SET enable_default_ustore_table=off')
        query(connection, "SELECT og_lake.set_option('biz.vectors'::regclass,'data_inlining_row_limit','50000')")
        carriers = query(connection, "SELECT c.reloptions FROM created_metadata.ducklake_inlined_data_tables t "
            'JOIN created_metadata.ducklake_table d USING(table_id) JOIN pg_class c ON c.relname=t.table_name '
            "AND c.relnamespace=(SELECT oid FROM pg_namespace WHERE nspname='created_metadata') WHERE d.table_name='vectors' AND d.end_snapshot IS NULL")
        check('one actual inline carrier', len(carriers), 1)
        check('actual requested carrier AM', 'storage_type=ustore' in (carriers[0][0] or []), args.storage == 'ustore')

        def both(statement):
            query(connection, 'BEGIN')
            for table in ('biz.exact', 'biz.vectors'):
                query(connection, statement.format(table=table))
            query(connection, 'COMMIT')

        def insert_range(first, last):
            if pq:
                terms = ['(sin(i*.37+' + str(j * .19) + ')*(1+i/512.0)+cos(i*.017+' + str(j * .071) + '))::text'
                         for j in range(dimension)]
                vector_sql = "('['||" + "||','||".join(terms) + "||']')::vector,"
            else:
                vector_sql = "('['||(i+.125)::text||','||(i*i/257.0+.33)::text||','||(i*i*i/20000.0+.71)::text||']')::vector,"
            both("INSERT INTO {table} SELECT CASE WHEN i%13=0 THEN NULL ELSE i END,"
                 'CASE WHEN i%9=0 THEN NULL ELSE i::bigint*10000000000+7 END,'
                 "CASE WHEN i%11=0 THEN NULL WHEN i%17=0 THEN 'NaN'::double precision "
                 "WHEN i%23=0 THEN 'Infinity'::double precision WHEN i%29=0 THEN '-Infinity'::double precision "
                 "WHEN i%31=0 THEN '-0'::double precision WHEN i%32=0 THEN 0::double precision ELSE i::double precision/7::double precision END,"
                 + vector_sql + "'row/'||lpad(i::text," + str(note_width) + ",'0') FROM generate_series(" + str(first) + ',' + str(last) + ')i')

        for first, last in file_ranges:
            insert_range(first, last)
            query(connection, "SELECT og_lake.flush('biz.vectors'::regclass)")
        insert_range(file_rows * 2 + 1, file_rows * 2 + 16)
        zero = '[' + ','.join(['0'] * dimension) + ']'
        both("INSERT INTO {table} VALUES(" + str(file_rows * 2 + 17) + ",NULL,NULL,NULL,'null-vector'),(" +
             str(file_rows * 2 + 18) + ",NULL,NULL,'" + zero + "'::vector,'zero-vector')")
        check('heap and lake complete initial population', scalar(connection, 'SELECT count(*) FROM biz.vectors'),
              scalar(connection, 'SELECT count(*) FROM biz.exact'))
        store = out / 'index-store'
        store.mkdir()
        query(connection, "SELECT og_lake.set_option('created_lake'::name,'index_root',%s::text)", (str(store),))
        spec = dict(version=1, kind='vector', method='diskann', column='v', metric=args.metric)
        spec.update(dict(pq=True, pq_chunks=4) if pq else dict(enable_pq=False))
        index_uuid = str(scalar(connection, "SELECT og_lake.create_lake_index('biz.vectors'::regclass,'ix_boolean_ann',%s::jsonb)",
                                (json.dumps(spec),)))
        head = case.head()
        query(connection, 'SELECT og_lake.build_lake_index(%s::uuid)', (index_uuid,))
        check('graph build allocates no business snapshot', case.head(), head)
        coverage = query(connection, 'SELECT c.source_file_id,c.physical_row_count FROM og_lake_internal.index_coverage c '
                         'JOIN og_lake_internal.index_segment s USING(segment_uuid,index_uuid,definition_generation) '
                         "WHERE c.index_uuid=%s::uuid AND c.active AND s.state='READY' ORDER BY c.source_file_id", (index_uuid,))
        check('two actually covered bounded physical files', [row[1] for row in coverage], [file_rows, file_rows])
        check('only Heap registers inline ANN', bool(scalar(connection,
              'SELECT inline_index_oid IS NOT NULL FROM og_lake_internal.index_definition WHERE index_uuid=%s::uuid',
              (index_uuid,))), args.storage == 'heap')
        reference = ROOT / '.reference/opengauss-native-lake/build/duckdb-gcc10/duckdb'
        result['reference_binary_sha256'] = sha256(reference)
        graph_roots = query(connection, 'SELECT s.artifact_root_uri,s.algorithm_version,s.format_version,s.manifest_hash FROM og_lake_internal.index_segment s '
            'JOIN og_lake_internal.index_coverage c USING(segment_uuid,index_uuid,definition_generation) '
            "WHERE c.index_uuid=%s::uuid AND c.active AND s.state='READY' ORDER BY c.source_file_id", (index_uuid,))
        for ordinal, (directory, algorithm, format_version, manifest) in enumerate(graph_roots):
            check('actual graph algorithm/format ' + str(ordinal), (algorithm, format_version), (2, 2 if pq else 1))
            source_path = scalar(connection, 'SELECT path FROM created_metadata.ducklake_data_file WHERE data_file_id=%s',
                                 (coverage[ordinal][0],))
            source_files = [p for p in Path(empty['data_path']).rglob('*.parquet') if p.name == Path(source_path).name]
            check('coverage resolves to one immutable owned source ' + str(ordinal), len(source_files), 1)
            statement = "SELECT note FROM read_parquet('" + str(source_files[0]).replace("'", "''") + "') ORDER BY note"
            decoded = subprocess.run([str(reference), '-no-init', '-batch', '-json', '-c', 'SET threads=1;' + statement],
                                     text=True, capture_output=True, timeout=90)
            (out / ('physical-source-domain-' + str(ordinal) + '.sql')).write_text(statement + ';\n')
            (out / ('physical-source-domain-' + str(ordinal) + '.stdout')).write_text(decoded.stdout)
            (out / ('physical-source-domain-' + str(ordinal) + '.stderr')).write_text(decoded.stderr)
            check('independent physical source domain decoder succeeds ' + str(ordinal), decoded.returncode, 0)
            first, last = file_ranges[ordinal]
            check('physical file domain exactly matches the heap truth-oracle slice ' + str(ordinal),
                  [row['note'] for row in json.loads(decoded.stdout)],
                  ['row/' + str(i).zfill(note_width) for i in range(first, last + 1)])
            graph = Path(directory).resolve() / 'ann_nodes.parquet'
            check('graph artifact belongs to this owned attempt ' + str(ordinal), graph.is_relative_to(store.resolve()))
            sql = "SELECT count(*) AS nodes FROM read_parquet('" + str(graph).replace("'", "''") + "')"
            (out / ('graph-node-count-' + str(ordinal) + '.sql')).write_text(sql + ';\n')
            counted = subprocess.run([str(reference), '-no-init', '-batch', '-json', '-c', 'SET threads=1;' + sql],
                                     text=True, capture_output=True, timeout=90)
            (out / ('graph-node-count-' + str(ordinal) + '.stdout')).write_text(counted.stdout)
            (out / ('graph-node-count-' + str(ordinal) + '.stderr')).write_text(counted.stderr)
            check('independent graph-node count exit ' + str(ordinal), counted.returncode, 0)
            count = json.loads(counted.stdout)[0]['nodes']
            check('actual per-file graph is below beam, no recall extrapolation ' + str(ordinal), 0 < count < 1000)
            if pq:
                check('PQ fixture supplies at least512 real graph nodes ' + str(ordinal), count >= 512)
                descriptor = re.fullmatch(r'og_lake_ann_pq_v2\|([0-9a-f]{64})\|([0-9a-f]{64})\|(\d+)\|(\d+)\|(\d+)', manifest)
                check('PQ roots and dimensions are bound by a real format2 descriptor ' + str(ordinal), descriptor is not None)
                check('PQ descriptor binds four chunks and this real node domain ' + str(ordinal),
                      tuple(map(int, descriptor.groups()[2:])), (4, min(count, 1024), count))
                for filename, expected_rows in [('ann_pq_model.parquet', 256), ('ann_pq_codes.parquet', count),
                                                 ('ann_pq_manifest.parquet', 20)]:
                    sidecar = Path(directory).resolve() / filename
                    check('PQ sidecar stays in the exact owned graph directory ' + filename, sidecar.is_relative_to(store.resolve()))
                    statement = "SELECT count(*) AS n FROM read_parquet('" + str(sidecar).replace("'", "''") + "')"
                    decoded = subprocess.run([str(reference), '-no-init', '-batch', '-json', '-c', 'SET threads=1;' + statement],
                                             text=True, capture_output=True, timeout=90)
                    (out / ('pq-' + str(ordinal) + '-' + filename + '.stdout')).write_text(decoded.stdout)
                    (out / ('pq-' + str(ordinal) + '-' + filename + '.stderr')).write_text(decoded.stderr)
                    check('independent PQ sidecar decoder exit ' + filename, decoded.returncode, 0)
                    check('independent PQ sidecar row domain ' + filename, json.loads(decoded.stdout)[0]['n'], expected_rows)
        query(connection, 'SET enable_lake_ann=on')
        query(connection, 'SET enable_seqscan=off')
        query(connection, 'SET client_min_messages=debug1')
        operator = {'l2': '<->', 'ip': '<#>', 'cosine': '<=>'}[args.metric]
        base = 'v IS NOT NULL' + (' AND vector_norm(v)>0' if args.metric == 'cosine' else '')

        def statement(table, predicate, argument='%s::vector', limit=3):
            return ("SELECT id,k,encode(float8send(f),'hex'),encode(vector_send(v),'hex'),note,v " + operator + ' ' + argument +
                    ' AS d FROM ' + table + ' WHERE ' + base + ' AND (' + predicate + ') ORDER BY d LIMIT ' + str(limit))

        def no_ties(vector):
            sql = 'SELECT count(*),count(DISTINCT d) FROM (SELECT v ' + operator + ' %s::vector AS d FROM biz.exact WHERE ' + base + ')s'
            population, distinct = query(connection, sql, (vector,))[0]
            check('deterministic original distances have no ties: ' + vector, distinct, population)

        def equivalent(label, predicate, parameters=(), vector=None, limit=3):
            vector = vector or vectors[0]
            values = (vector,) + tuple(parameters)
            got = query(connection, statement('biz.vectors', predicate, limit=limit), values)
            expected = query(connection, statement('biz.exact', predicate, limit=limit), values)
            check(label + ': exact original rows and SQL distances', got, expected)
            return got

        def source_counts(predicate, parameters=()):
            return {coverage[index][0]: scalar(connection, 'SELECT count(*) FROM biz.exact WHERE ' + base + ' AND (' + predicate +
                    ") AND note>=%s AND note<=%s", tuple(parameters) +
                    ('row/' + str(first).zfill(note_width), 'row/' + str(last).zfill(note_width)))
                    for index, (first, last) in enumerate(file_ranges)}

        def mask_records(messages):
            pattern = r'file=(\d+) scalar_matches=(\d+) admitted_rows=(\d+) usable=(\d+)'
            return [dict(file=int(f), matches=int(n), admitted=int(a), usable=int(u))
                    for f, n, a, u in re.findall(pattern, ''.join(messages))]

        def verify_masks(label, text, messages, predicate, parameters=(), exact_files=False):
            # Comparing final rows alone would permit a stale/all-TRUE mask:
            # the original SQL qual could still remove those extra candidates.
            expected = source_counts(predicate, parameters)
            actual = mask_records(messages)
            check(label + ': no file is diagnosed twice in one parameter epoch',
                  len({row['file'] for row in actual}), len(actual))
            if exact_files:
                check(label + ': both actual file masks executed', {row['file'] for row in actual}, set(expected))
            for row in actual:
                check(label + ': diagnosed file is in the real covered source domain', row['file'] in expected)
                check(label + ': exact SQL TRUE physical-mask count for file' + str(row['file']),
                      row['matches'], expected[row['file']])
            result.setdefault('mask_epochs', []).append(dict(label=label, expected=expected, actual=actual,
                                                             verified=count_label(text, 'Verified ANN File Coverage')))
            return actual

        def active(label, predicate, parameters=(), expected=None, vector=None, limit=3):
            vector = vector or vectors[0]
            text = plan(label, statement('biz.vectors', predicate, limit=limit), (vector,) + tuple(parameters))
            check(label + ': actual scalar mask plan', 'ANN Scalar Filter:' in text)
            check(label + ': inline/private remains complete', 'Actual Inline Access: TableAM sequential' in text)
            check(label + ': original native SQL rerank and quals retained',
                  'Sort Key:' in text and operator in text and 'Filter:' in text)
            if expected is None:
                counts = source_counts(predicate, parameters).values()
                expected = sum(number == 0 or number >= limit for number in counts)
            check(label + ': verified file coverage, not inferred activation', count_label(text, 'Verified ANN File Coverage'), expected)
            verify_masks(label, text, latest_plan_notices, predicate, parameters, exact_files=expected == 2)
            return text

        for vector in vectors:
            no_ties(vector)
        core = 'id>=1 AND k>=7::bigint AND f IS NOT NULL'
        predicates = [
            ('multiple-and', core),
            ('multiple-or', '(id<20 OR k>1000000000007::bigint) AND f IS NOT NULL'),
            ('nested-not', 'NOT(id<10 OR f IS NULL) AND k IS NOT NULL'),
            ('not-equal', 'id<>7 AND k IS NOT NULL'),
            ('not-in', 'id NOT IN(7,13,19) AND f IS NOT NULL'),
            ('all-not-equal', 'id<>ALL(ARRAY[7,13,19]::bigint[]) AND k IS NOT NULL'),
            ('in-with-null', 'id=ANY(ARRAY[1,2,3,65,66,67,NULL]::bigint[])'),
            ('nested-three-valued', 'NOT((id<10 OR k IS NULL) AND (f<0 OR NOT(f IS NULL))) OR (id>=120 AND f IS NULL)'),
            ('cross-float-array', "f=ANY(ARRAY[0,'NaN',NULL]::real[]) OR k>=900000000007::bigint"),
        ]
        for label, predicate in predicates:
            equivalent(label, predicate)
            active(label, predicate)
        if pq:
            pq_plan = active('pq-cpu-frontier-activation', core, expected=2)
            check('actual EXPLAIN qualifies both PQ-scored files', count_label(pq_plan, 'Verified PQ File Coverage'), 2)
            used = {int(file_id) for file_id in re.findall(r'file=(\d+) nodes=\d+ visited=\d+ unique_rows=\d+ requested=\d+ usable=1 pq_used=1',
                                                          ''.join(latest_plan_notices))}
            check('both actual filtered frontiers used CPU PQ scores, not only READY sidecars', used, {row[0] for row in coverage})

            # LakePqOpen(mode1) pauses only after a real frontier opens and the
            # scalar mask has been constructed. Wait for this exact owned
            # advisory lock; timing or pg_sleep cannot stand in for the lease.
            query(connection, "CREATE FUNCTION biz.pq_cancel_probe(integer,bigint,bigint) RETURNS text AS '$libdir/og_lake','og_lake_test_pq_probe' LANGUAGE C VOLATILE NOT FENCED")
            query(connection, 'REVOKE ALL ON FUNCTION biz.pq_cancel_probe(integer,bigint,bigint) FROM PUBLIC')
            pq_source = coverage[0][0]
            pq_key = (1 << 44) + int(hashlib.sha256((args.run_id + '/pq-cancel').encode()).hexdigest()[:9], 16)
            pq_dbid = database.allocation['database_oid']
            pq_pid = scalar(connection, 'SELECT pg_backend_pid()')
            pq_holder = scalar(case.b, 'SELECT pg_backend_pid()')
            pq_outcome = {}
            pq_gate_held = False
            pq_unlock_ok = False
            pq_remaining = None
            def pq_probe(mode=0, gate=0):
                return json.loads(scalar(connection, 'SELECT biz.pq_cancel_probe(%s,%s,%s)', (mode, pq_source, gate)))
            def pq_cancel_query():
                try:
                    pq_outcome['rows'] = query(connection, statement('biz.vectors', core), (vectors[0],))
                except BaseException as exc:
                    pq_outcome.update(sqlstate=getattr(exc, 'pgcode', None), error=str(exc))
            pq_predicate = ("locktype='advisory' AND database=%s AND classid=%s::oid AND objid=%s::oid "
                            "AND objsubid=1 AND mode='ExclusiveLock'")
            pq_tag = (pq_dbid, pq_key >> 32, pq_key & 0xffffffff)
            try:
                query(case.b, 'SELECT pg_advisory_lock(%s::bigint)', (pq_key,))
                pq_gate_held = True
                pq_probe(-1)
                pq_probe(1, pq_key)
                notices.clear()
                worker = threading.Thread(target=pq_cancel_query, daemon=True)
                worker.start()
                pq_deadline = time.monotonic() + 30
                pq_wait, pq_owned = [], []
                while time.monotonic() < pq_deadline and worker.is_alive():
                    pq_wait = query(case.observer, 'SELECT pid,granted FROM pg_locks WHERE ' + pq_predicate +
                                    ' AND pid=%s AND NOT granted', pq_tag + (pq_pid,))
                    pq_owned = query(case.observer, 'SELECT pid,granted FROM pg_locks WHERE ' + pq_predicate +
                                     ' AND pid=%s AND granted', pq_tag + (pq_holder,))
                    if pq_wait and pq_owned:
                        break
                    time.sleep(.01)
                result['pq_live_cancel_gate'] = dict(database_oid=pq_dbid, source_file_id=pq_source,
                    consumer_pid=pq_pid, holder_pid=pq_holder, key=pq_key, waiter=pq_wait, holder=pq_owned)
                check('PQ boolean query reached exact live-frontier gate', pq_wait, [(pq_pid, False)])
                check('PQ gate belongs to exact controller backend', pq_owned, [(pq_holder, True)])
                check('PQ query remains live before cancellation', worker.is_alive())
                check('cancel exact live PQ boolean backend', scalar(case.observer, 'SELECT pg_cancel_backend(%s)', (pq_pid,)))
                worker.join(timeout=30)
                check('canceled live PQ query drains before cleanup', worker.is_alive(), False)
                check('actual live PQ boolean cancellation SQLSTATE', pq_outcome.get('sqlstate'), '57014')
            finally:
                # Release the exact controller gate before any extra drain.
                # A busy actor is preserved by the existing outer finally.
                if pq_gate_held:
                    try:
                        pq_unlock_ok = bool(scalar(case.b, 'SELECT pg_advisory_unlock(%s::bigint)', (pq_key,)))
                        pq_gate_held = not pq_unlock_ok
                    except BaseException as exc:
                        result['pq_gate_unlock_error'] = str(exc)
                if worker is not None and worker.is_alive():
                    try:
                        connection.cancel()
                    except BaseException as exc:
                        result['pq_gate_cancel_cleanup_error'] = str(exc)
                    worker.join(timeout=30)
                try:
                    pq_remaining = query(case.observer, 'SELECT pid FROM pg_locks WHERE ' + pq_predicate +
                                         ' AND pid=%s AND granted', pq_tag + (pq_holder,))
                except BaseException as exc:
                    result['pq_gate_remaining_error'] = str(exc)
                result['pq_live_cancel_cleanup'] = dict(unlocked=pq_unlock_ok, remaining=pq_remaining,
                                                      worker_alive=worker is not None and worker.is_alive())
                write_json(out / 'pq-live-cancel-notice-capture.json', notices.summary())
            check('live PQ controller gate released before reuse', (pq_unlock_ok, pq_remaining), (True, []))
            pq_balanced = pq_probe()
            result['pq_live_cancel_probe'] = pq_balanced
            check('live PQ leases all closed and destroyed exactly once',
                  1 <= pq_balanced['opened'] <= len(coverage) and
                  pq_balanced['opened'] == pq_balanced['closed'] == pq_balanced['destroyed'] and
                  pq_balanced['active'] == pq_balanced['violations'] == pq_balanced['armed'] == 0)
            check('one closing path per actual PQ lease',
                  pq_balanced['owner'] + pq_balanced['context'] + pq_balanced['explicit'], pq_balanced['opened'])
            pq_contexts = ['Lake ANN scalar parameters', 'Lake ANN scalar leaf', 'Lake ANN scalar file positions',
                           'Lake file index candidates', 'Lake file DiskANN search']
            check('live cancellation releases mask/candidate/PQ query contexts', query(case.observer,
                  'SELECT contextname FROM pg_catalog.pv_thread_memory_detail() WHERE tid=%s AND contextname=ANY(%s::text[])',
                  (pq_pid, pq_contexts)), [])
            check('same backend survives live PQ cancellation', scalar(connection, 'SELECT pg_backend_pid()'), pq_pid)
            equivalent('same session after live PQ cancel', core)
            active('after-live-pq-cancel', core, expected=2)
            pq_probe(-1)

        # Runtime parameters keep empty/NULL arrays visible to the executor,
        # rather than relying on a particular constant-folding transformation.
        query(connection, 'SET plan_cache_mode=force_generic_plan')
        for negate in (False, True):
            name = 'bool_not_array' if negate else 'bool_array'
            predicate = ('NOT(id=ANY($2))' if negate else 'id=ANY($2)')
            query(connection, 'PREPARE ' + name + '(vector,bigint[]) AS ' + statement('biz.vectors', predicate, '$1'))
            for ordinal, array in enumerate(('{1,2,3,65,66,67,NULL}', '{}', None, '{NULL,NULL}',
                                             '{7,7,65,66,67}', '{{1,2,3},{65,66,67}}',
                                             '{-9223372036854775808,9223372036854775807,1,65}')):
                got = query(connection, 'EXECUTE ' + name + '(%s::vector,%s::bigint[])', (vectors[ordinal % 3], array))
                direct = 'NOT(id=ANY(%s::bigint[]))' if negate else 'id=ANY(%s::bigint[])'
                want = query(connection, statement('biz.exact', direct), (vectors[ordinal % 3], array))
                check(name + ': generic NULL/empty/wide/repeated array ' + str(ordinal), got, want)
                text = plan(name + '-' + str(ordinal), 'EXECUTE ' + name + '(%s::vector,%s::bigint[])', (vectors[ordinal % 3], array))
                check(name + ': actual generic mask ' + str(ordinal), 'ANN Scalar Filter:' in text and '$2' in text)
                check(name + ': filtered inline stays complete ' + str(ordinal), 'Actual Inline Access: TableAM sequential' in text)
                verify_masks(name + '-' + str(ordinal), text, latest_plan_notices, direct, (array,), exact_files=True)
            huge = '{' + ','.join(map(str, range(1, 65538))) + '}'
            (out / (name + '-oversize-parameter.json')).write_text(json.dumps(dict(array=huge)) + '\n')
            got = query(connection, 'EXECUTE ' + name + '(%s::vector,%s::bigint[])', (vectors[0], huge))
            direct = 'NOT(id=ANY(%s::bigint[]))' if negate else 'id=ANY(%s::bigint[])'
            check(name + ': oversized array complete heap differential', got,
                  query(connection, statement('biz.exact', direct), (vectors[0], huge)))
            text = plan(name + '-oversize', 'EXECUTE ' + name + '(%s::vector,%s::bigint[])', (vectors[0], huge))
            check(name + ': unsupported input keeps plan but executes full files',
                  'ANN Scalar Filter:' in text and count_label(text, 'Verified ANN File Coverage') == 0
                  and count_label(text, 'Complete File Fallback Inputs') == 2)
            query(connection, 'DEALLOCATE ' + name)

        mixed = 'NOT(id=ANY($2)) OR (k<>$3 AND f=ANY($4))'
        query(connection, 'PREPARE bool_mixed(vector,bigint[],bigint,real[]) AS ' + statement('biz.vectors', mixed, '$1'))
        parameters = [('{7,13,NULL}', 70000000007, '{0,NaN,NULL}'), ('{}', None, '{}'),
                      (None, 7, '{NaN}'), ('{NULL}', None, None), ('{1,2,65,66}', 7, '{0,Infinity,NULL}'),
                      ('{7,13,NULL}', 70000000007, '{0,NaN,NULL}')]
        direct = 'NOT(id=ANY(%s::bigint[])) OR (k<>%s::bigint AND f=ANY(%s::real[]))'
        for ordinal, values in enumerate(parameters):
            vector = vectors[ordinal % 3]
            got = query(connection, 'EXECUTE bool_mixed(%s::vector,%s::bigint[],%s::bigint,%s::real[])', (vector,) + values)
            check('mixed generic parameter epoch ' + str(ordinal), got,
                  query(connection, statement('biz.exact', direct), (vector,) + values))
            text = plan('mixed-generic-' + str(ordinal), 'EXECUTE bool_mixed(%s::vector,%s::bigint[],%s::bigint,%s::real[])',
                        (vector,) + values)
            check('mixed generic actually executes a boolean mask each epoch', 'ANN Scalar Filter: native scalar boolean tree' in text)
            verify_masks('mixed-generic-' + str(ordinal), text, latest_plan_notices, direct, values, exact_files=True)
        text = plan('mixed-generic', 'EXECUTE bool_mixed(%s::vector,%s::bigint[],%s::bigint,%s::real[])',
                    (vectors[0],) + parameters[0])
        check('mixed generic actual boolean tree', 'ANN Scalar Filter: native scalar boolean tree' in text)
        query(connection, 'SET enable_lake_ann=off')
        got = query(connection, 'EXECUTE bool_mixed(%s::vector,%s::bigint[],%s::bigint,%s::real[])', (vectors[0],) + parameters[0])
        check('cached boolean plan honors ANN off', got,
              query(connection, statement('biz.exact', direct), (vectors[0],) + parameters[0]))
        text = plan('mixed-off', 'EXECUTE bool_mixed(%s::vector,%s::bigint[],%s::bigint,%s::real[])', (vectors[0],) + parameters[0])
        check('ANN off has complete source coverage', count_label(text, 'Verified ANN File Coverage'), 0)
        query(connection, 'SET enable_lake_ann=on')
        query(connection, 'DEALLOCATE bool_mixed')
        query(connection, 'SET plan_cache_mode=auto')

        unsupported = 'id>=1 OR length(note)>0'
        equivalent('unsupported OR branch', unsupported)
        text = plan('unsupported-or', statement('biz.vectors', unsupported), (vectors[0],))
        check('unsupported OR never claims filtered ANN', 'ANN Scalar Filter:' not in text and 'ANN Semantics:' not in text)
        check('unsupported OR executes both complete files', count_label(text, 'Verified ANN File Coverage') == 0
              and count_label(text, 'Complete File Fallback Inputs') == 2)

        # One query owns one filter tree through 512 actual correlated rescans.
        query(connection, 'CREATE TABLE biz.bool_keys(seq integer,lo bigint,hi bigint,keys bigint[],cut double precision,qv vector(' + str(dimension) + '))')
        query(connection, "INSERT INTO biz.bool_keys SELECT i,CASE WHEN i%11=0 THEN NULL ELSE (i%110+1)::bigint END,"
              '(i%110+22)::bigint,CASE WHEN i%13=0 THEN NULL::bigint[] WHEN i%17=0 THEN ARRAY[]::bigint[] '
              'ELSE ARRAY[(i%' + str(file_rows * 2) + '+1)::bigint*10000000000+7,((i+53)%' + str(file_rows * 2) + '+1)::bigint*10000000000+7,NULL] END,'
              "(i%19)::double precision,CASE WHEN i%3=0 THEN '" + vectors[0] + "'::vector "
              "WHEN i%3=1 THEN '" + vectors[1] + "'::vector ELSE '" + vectors[2] + "'::vector END FROM generate_series(1,512)i")
        check('independent correlated source row count', scalar(connection, 'SELECT count(*) FROM biz.bool_keys'), 512)
        correlated = '((id>=q.lo AND id<=q.hi) OR (k=ANY(q.keys) AND NOT(f<q.cut)))'
        stress = ('SELECT q.seq,(SELECT note FROM biz.vectors WHERE ' + base + ' AND ' + correlated +
                  ' ORDER BY v ' + operator + ' q.qv LIMIT 1),pg_sleep(0.004) '
                  'FROM (SELECT * FROM biz.bool_keys ORDER BY seq OFFSET 0) q ORDER BY q.seq')
        target_pid = scalar(connection, 'SELECT pg_backend_pid()')
        names = ['Lake ANN scalar parameters', 'Lake ANN scalar leaf']
        memory_sql = ('SELECT contextname,parent,count(*),sum(totalsize)::bigint,sum(usedsize)::bigint '
                      'FROM pg_catalog.pv_thread_memory_detail() WHERE tid=%s AND contextname=ANY(%s::text[]) '
                      'GROUP BY contextname,parent ORDER BY contextname,parent')
        captured = {}

        def cancel_stress(label):
            try:
                connection.cancel()
            except BaseException as exc:
                result.setdefault('cancellation_errors', []).append(dict(label=label, error=str(exc)))
                result['status'] = 'fail'

        def execute_stress():
            try:
                captured['rows'] = query(connection, stress)
            except BaseException as exc:
                captured['error'] = repr(exc)

        cache_before = pq_probe()['ann_rescan_cache']
        check('fresh statement cache baseline is empty', (cache_before['live'], cache_before['bytes']), (0, 0))
        worker = threading.Thread(target=execute_stress, daemon=True)
        started = time.monotonic()
        notices.clear()
        worker.start()
        try:
            while worker.is_alive():
                if time.monotonic() - started > 660:
                    cancel_stress('bounded rescan timeout')
                    raise RuntimeError('bounded 512-rescan memory observation timed out')
                rows = query(case.observer, memory_sql, (target_pid, names))
                if rows:
                    result['memory_samples'].append(dict(elapsed=round(time.monotonic() - started, 6), contexts=rows))
                time.sleep(0.02)
        finally:
            if worker.is_alive():
                cancel_stress('rescan observation exit')
            worker.join(timeout=30)
        check('correlated worker finishes before cleanup', worker.is_alive(), False)
        check('correlated worker has no SQL error', captured.get('error'), None)
        write_json(out / 'correlated-512-notice-capture.json', notices.summary())
        check('all rescan target notices fit the bounded diagnostic sink', notices.discarded, 0)
        rescan_notices = list(notices)
        (out / 'correlated-512-actual-mask-notices.txt').write_text(''.join(rescan_notices))
        mask_epochs = mask_records(rescan_notices)
        initial_masks = []
        # Native BeginForeignScan configures its reader once. Depending on
        # when the host binds outer EXEC Params this can emit an initial pair
        # before the512 data rescans. Preserve it separately, never count it
        # as a qualified parameter epoch or silently discard other extras.
        if len(mask_epochs) == 513 * len(coverage):
            initial_masks = mask_epochs[:len(coverage)]
            check('one initialization-only mask group names both source files',
                  {item['file'] for item in initial_masks}, {item[0] for item in coverage})
            result['correlated_initialization_masks'] = initial_masks
            mask_epochs = mask_epochs[len(coverage):]
        check('one actual physical-mask evaluation per file in all512 rescans', len(mask_epochs), 512 * len(coverage))
        bounds = tuple(value for first, last in file_ranges for value in
                       ('row/' + str(first).zfill(note_width), 'row/' + str(last).zfill(note_width)))
        count_exprs = ['(SELECT count(*) FROM biz.exact WHERE ' + base + ' AND ' + correlated +
                      ' AND note>=%s AND note<=%s)' for _ in file_ranges]
        truth = query(connection, 'SELECT q.seq,' + ','.join(count_exprs) + ' FROM biz.bool_keys q ORDER BY q.seq', bounds)
        check('independent per-epoch truth oracle covers all512 outer parameters', [row[0] for row in truth], list(range(1, 513)))
        for ordinal, row in enumerate(truth):
            actual = mask_epochs[ordinal * len(coverage):(ordinal + 1) * len(coverage)]
            expected = {coverage[index][0]: row[index + 1] for index in range(len(coverage))}
            check('rescan epoch' + str(row[0]) + ' evaluates both exact source files', {item['file'] for item in actual}, set(expected))
            check('rescan epoch' + str(row[0]) + ' refreshes its full boolean/ANY/NULL TRUE counts',
                  {item['file']: item['matches'] for item in actual}, expected)
        result['correlated_mask_epochs'] = dict(actual=mask_epochs, expected=truth)
        got = sorted((row[0], row[1]) for row in captured['rows'])
        want = sorted((row[0], row[1]) for row in query(connection, stress.replace('biz.vectors', 'biz.exact')))
        check('all 512 correlated changed parameter results equal heap SQL', got, want)
        cache_after = pq_probe()['ann_rescan_cache']
        result['correlated_cache_probe'] = dict(before=cache_before, after=cache_after)
        check('both physical512-node files loaded once in the actual correlated statement', cache_after['loads'] - cache_before['loads'], len(coverage))
        check('actual immutable cache reuse is observed, not inferred from elapsed time', cache_after['hits'] > cache_before['hits'])
        parameter_luts = sum(value > 0 for row in truth for value in row[1:])
        initial_luts = sum(item['matches'] > 0 for item in initial_masks)
        expected_luts = parameter_luts + initial_luts
        result['correlated_expected_luts'] = dict(parameter_epochs=parameter_luts, initialization=initial_luts, total=expected_luts)
        check('every nonempty parameter/file mask builds its own fresh query LUT', cache_after['queries'] - cache_before['queries'], expected_luts)
        check('all statement caches end and release retained allocations after512 rescans', (cache_after['live'], cache_after['bytes']), (0, 0))
        check('statement cache opens and closes balance exactly', cache_after['opened'], cache_after['closed'])
        check('observed cache retention peak stays within32MiB across all readers', 0 < cache_after['peak_bytes'] <= 32 * 1024 * 1024)

        # A ready image may be built by a miss that itself falls back for K.
        # The later cache hits must preserve the same posting_count/minimum
        # decision. One nonempty scalar match cannot satisfy per-file K=3.
        pq_probe(-1)
        small = ('SELECT q.seq,(SELECT note FROM biz.vectors WHERE ' + base + ' AND id=q.seq '
                 'ORDER BY v ' + operator + ' q.qv LIMIT 3) '
                 'FROM (SELECT * FROM biz.bool_keys WHERE seq<=3 ORDER BY seq OFFSET 0) q ORDER BY q.seq')
        small_plan = plan('cached-minimum-fallback', small)
        small_probe = pq_probe()['ann_rescan_cache']
        check('minimum fallback still exercises actual cache hits', small_probe['hits'] > 0)
        check('all cached small-candidate epochs retain nonzero graph population and full fallback',
              len(re.findall(r'nodes=[1-9][0-9]* visited=[0-9]+ unique_rows=1 requested=3 usable=0', ''.join(latest_plan_notices))) >= 3)
        check('minimum failure leaves a complete source fallback in the real plan', count_label(small_plan, 'Complete File Fallback Inputs') >= 1)
        check('cache minimum fallback rows match heap SQL', query(connection, small), query(connection, small.replace('biz.vectors', 'biz.exact')))

        # The division's nonconstant input prevents planning-time folding. It
        # raises only after earlier correlated searches built/reused the image.
        pq_probe(-1)
        broken = ('SELECT q.seq,CASE WHEN q.seq=4 THEN (q.seq/(q.seq-q.seq))::text ELSE '
                  '(SELECT note FROM biz.vectors WHERE ' + base + ' AND ' + correlated +
                  ' ORDER BY v ' + operator + ' q.qv LIMIT 1) END '
                  'FROM (SELECT * FROM biz.bool_keys WHERE seq<=8 ORDER BY seq OFFSET 0) q ORDER BY q.seq')
        try:
            query(connection, broken)
        except BaseException as exc:
            check('actual executor error after cached searches', getattr(exc, 'pgcode', None), '22012')
        else:
            check('controlled nonconstant executor error is required', False)
        cache_error = pq_probe()['ann_rescan_cache']
        result['cache_executor_error_probe'] = cache_error
        check('ERROR proof really loaded and reused immutable images', cache_error['loads'] > 0 and cache_error['hits'] > 0)
        check('ERROR releases immutable cache and fresh LUT resources', (cache_error['live'], cache_error['bytes']), (0, 0))
        check('ERROR statement owner closes each cache once', cache_error['opened'], cache_error['closed'])

        # The SQL gate argument depends on the correlated search result. Wait
        # for its exact native advisory lock, then cancel with immutable images
        # still owned by this live statement. Existing live-frontier cancel
        # above remains the separate query/LUT-in-flight diagnostic proof.
        pq_probe(-1)
        cache_gate = pq_key + 37
        cache_gate_tag = (pq_dbid, cache_gate >> 32, cache_gate & 0xffffffff)
        cache_inner = ('SELECT q.seq,(SELECT note FROM biz.vectors WHERE ' + base + ' AND ' + correlated +
                       ' ORDER BY v ' + operator + ' q.qv LIMIT 1) AS note '
                       'FROM (SELECT * FROM biz.bool_keys WHERE seq=1 OFFSET 0) q OFFSET 0')
        cache_gate_sql = ('SELECT q.seq,q.note,pg_advisory_lock(%s::bigint+coalesce((length(q.note)-length(q.note))::bigint,0)) '
                          'FROM (' + cache_inner + ') q')
        cache_cancel_outcome = {}
        def cache_cancel_query():
            try:
                cache_cancel_outcome['rows'] = query(connection, cache_gate_sql, (cache_gate,))
            except BaseException as exc:
                cache_cancel_outcome.update(sqlstate=getattr(exc, 'pgcode', None), error=str(exc))
        gate_held = False
        try:
            query(case.b, 'SELECT pg_advisory_lock(%s::bigint)', (cache_gate,))
            gate_held = True
            worker = threading.Thread(target=cache_cancel_query, daemon=True)
            worker.start()
            deadline = time.monotonic() + 30
            waiter = []
            while time.monotonic() < deadline and worker.is_alive():
                waiter = query(case.observer, 'SELECT pid,granted FROM pg_locks WHERE ' + pq_predicate +
                               ' AND pid=%s AND NOT granted', cache_gate_tag + (pq_pid,))
                if waiter:
                    break
                time.sleep(.01)
            check('cached search reaches the exact SQL result-dependent gate', waiter, [(pq_pid, False)])
            check('cache gate belongs to the owned controller', query(case.observer, 'SELECT pid,granted FROM pg_locks WHERE ' + pq_predicate +
                  ' AND pid=%s AND granted', cache_gate_tag + (pq_holder,)), [(pq_holder, True)])
            check('cancel the exact cached-statement backend', scalar(case.observer, 'SELECT pg_cancel_backend(%s)', (pq_pid,)))
        finally:
            if gate_held:
                check('release cache gate before draining worker', scalar(case.b, 'SELECT pg_advisory_unlock(%s::bigint)', (cache_gate,)))
            if worker is not None and worker.is_alive():
                connection.cancel()
                worker.join(timeout=30)
        check('canceled cached statement drains before cleanup', worker.is_alive(), False)
        check('cached statement cancellation is real57014', cache_cancel_outcome.get('sqlstate'), '57014')
        cache_canceled = pq_probe()['ann_rescan_cache']
        result['cache_statement_cancel'] = dict(probe=cache_canceled, outcome=cache_cancel_outcome, gate=cache_gate)
        check('cancel proof really loaded an immutable image before its gate', cache_canceled['loads'] > 0)
        check('cancel releases every statement cache and retained byte', (cache_canceled['live'], cache_canceled['bytes']), (0, 0))
        check('canceled statement cache opens/closes balance', cache_canceled['opened'], cache_canceled['closed'])
        check('correlated results prove parameter values change', len({row[1] for row in got}) > 8)
        samples = result['memory_samples']
        def context_totals(name):
            totals = []
            for sample in samples:
                rows = [row for row in sample['contexts'] if row[0] == name]
                if rows:
                    totals.append(dict(count=sum(row[2] for row in rows), total=sum(row[3] for row in rows),
                                       used=sum(row[4] for row in rows)))
            return totals
        parameter = context_totals(names[0])
        source = context_totals(names[1])
        result['memory_high_water'] = {name: dict(contexts=max(row['count'] for row in values),
                total=max(row['total'] for row in values), used=max(row['used'] for row in values))
                for name, values in ((names[0], parameter), (names[1], source)) if values}
        check('observer actually saw repeated live parameter/tree contexts', len(parameter) >= 3)
        check('observer actually saw live source-leaf work contexts', bool(source))
        check('one parameter/tree context instead of one per rescan', max(row['count'] for row in parameter), 1)
        check('parameter/tree high water remains bounded', max(row['total'] for row in parameter) <= 256 * 1024)
        check('one source-leaf work context at a time', max(row['count'] for row in source), 1)
        check('source-leaf high water remains bounded for small arrays', max(row['total'] for row in source) <= 4 * 1024 * 1024)
        check('finished query releases parameter/source contexts', query(case.observer, memory_sql, (target_pid, names)), [])
        text = plan('correlated-512-rescans', stress)
        check('EXPLAIN independently proves 512 executions', bool(re.search(r'loops=512(?:\D|$)', text)))
        check('correlated plan actually contains boolean mask and final SQL sort',
              'ANN Scalar Filter: native scalar boolean tree' in text and 'Sort Key:' in text)

        # This statement-level timeout checks recovery for an eligible mask
        # shape. It does not prove interruption after a live mask/PQ frontier
        # was opened; that requires a separate exact native lease gate.
        cancel_sql = ('SELECT pg_sleep(0.03),id,v ' + operator + ' %s::vector AS d FROM biz.vectors WHERE ' +
                      base + ' AND (' + core + ') ORDER BY d LIMIT 3')
        text = plan('cancel-planned', cancel_sql, (vectors[0],), analyze=False)
        check('cancel query retains the supported boolean plan', 'ANN Scalar Filter:' in text)
        query(connection, 'SET statement_timeout=20')
        try:
            query(connection, cancel_sql, (vectors[0],))
        except Exception as exc:
            check('actual boolean ANN cancellation', exc.pgcode, '57014')
        else:
            check('cancellation is observed', False)
        query(connection, 'SET statement_timeout=600000')
        equivalent('same session after cancellation', core)
        active('after-cancel', core, expected=2)
        check('cancel releases parameter/source contexts', query(case.observer, memory_sql, (target_pid, names)), [])

        query(connection, 'BEGIN')
        for table in ('biz.exact', 'biz.vectors'):
            query(connection, 'UPDATE ' + table + " SET v='" + vector_literal([.43, 1.79, -.13]) + "'::vector,note='private update' WHERE id=5")
        equivalent('private after-image', core)
        active('private-complete-sources', core, expected=0)
        query(connection, 'ROLLBACK')
        equivalent('private rollback', core)
        active('rollback-restores-masks', core, expected=2)
        both("UPDATE {table} SET v='" + vector_literal([.47, 1.83, -.17]) + "'::vector,note='first committed update' WHERE id=5")
        both("UPDATE {table} SET v='" + vector_literal([.53, 1.89, -.19]) + "'::vector,note='second committed update' WHERE id=5")
        check('two updates leave exactly one current row', query(connection, 'SELECT id,note FROM biz.vectors WHERE id=5'),
              [(5, 'second committed update')])
        equivalent('committed second after-image', core)
        active('ended-first-source-complete', core, expected=1)
        both("INSERT INTO {table} VALUES(9001,90010000000007,1.25,'" + vector_literal([.57, 1.97, -.23]) + "'::vector,'uncovered new source')")
        query(connection, "SELECT og_lake.flush('biz.vectors'::regclass)")
        equivalent('flush plus uncovered source participates in global rerank', core)
        text = active('uncovered-full-source', core, expected=1)
        check('uncovered and ended sources stay complete', count_label(text, 'Complete File Fallback Inputs') >= 2)
        check('flush preserves the full original current population', scalar(connection, 'SELECT count(*) FROM biz.vectors'),
              scalar(connection, 'SELECT count(*) FROM biz.exact'))
        for vector in vectors:
            no_ties(vector)
            equivalent('post-flush distance ' + vector, core, vector=vector)
        check('all executed inputs unchanged', all(sha256(path) == value for path, value in result['inputs'].items()))
        check('SDK unchanged', sha256(provider), result['provider_sha256'])
        check('reference binary unchanged', sha256(reference), result['reference_binary_sha256'])
        result['artifacts'] = {str(path): sha256(path) for path in out.rglob('*') if path.is_file()
                               and (path.suffix in ('.parquet', '.txt', '.sql', '.stdout', '.stderr') or path.name.endswith('-parameter.json'))}
        check_build(args.build_receipt)
        result['status'] = 'pass'
    except BaseException as exc:
        result.update(reason=str(exc), traceback=traceback.format_exc())
    finally:
        if worker is not None and worker.is_alive() and connection is not None:
            try:
                connection.cancel()
            except BaseException as exc:
                result.update(status='fail', final_cancel_error=str(exc))
            worker.join(timeout=30)
        if connection is not None:
            result['final_notice_capture'] = notices.summary()
            result['native_notices'] = list(connection.notices)[-50:]
        unfinished = worker is not None and worker.is_alive()
        if unfinished:
            result.update(status='fail', cleanup_error='Owned correlated query did not stop; preserve DB and roles without closing a busy connection')
        if database is not None and not unfinished:
            try:
                database.cleanup()
                result['database_cleanup'] = database.cleanup_result
            except BaseException as exc:
                result.update(status='fail', cleanup_error=str(exc))
        if roles is not None:
            try:
                if not result.get('database_cleanup') or result['database_cleanup']['returncode']:
                    raise RuntimeError('Owned DB cleanup unproven; owned roles retained')
                result['role_cleanup'] = cleanup_roles(control, roles.path, args.run_id)
                result = roles.redact(result)
            except BaseException as exc:
                result.update(status='fail', role_cleanup_error=str(exc))
        if control is not None:
            control.close()
        write_json(out / 'result.json', result)
    print(json.dumps(dict(status=result['status'], checks=len(result['assertions']), reason=result.get('reason'),
                          result=str(out / 'result.json')), ensure_ascii=False))
    return 0 if result['status'] == 'pass' else 1


if __name__ == '__main__':
    raise SystemExit(main())
