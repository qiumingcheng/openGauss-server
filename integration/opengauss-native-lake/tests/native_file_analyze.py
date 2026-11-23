#!/usr/bin/env python3
"""Unexecuted N7-STATS acceptance: real Native file ANALYZE and sample oracle.

Debug, local files, heap inline only. Not autoanalyze/SMP/remote/RAC qualification.
Reader bytes exclude independent footer_open I/O, which remains unobserved.
"""
from copy import deepcopy
from fractions import Fraction
import math
import argparse
import hashlib
import json
from pathlib import Path
import re
import shutil
import subprocess
import sys
import traceback
from types import SimpleNamespace

ROOT = Path('/home/qiumc/data/code/cpp')
D = ROOT / 'integration/opengauss-native-lake'
sys.path[:0] = [str(D), str(D / 'tests')]
from file_dml_acceptance import check_build
from inline_fixture import query, scalar
from lake_harness import sha256, validate_run_id, write_json
from native_auto_flush import connect_control
from native_runtime import INSTALL
from native_table_create import Case
from native_test_context import OwnedDatabase
from permission_api import OwnedRoles, cleanup_roles


class Notices:
    """Bounded target evidence; unrelated server DEBUG cannot evict it."""
    TARGET = 'native lake ANALYZE file sampling evidence'

    def __init__(self, target_records=8, target_bytes=64 * 1024):
        self.items = []
        self.target_record_limit, self.target_byte_limit = target_records, target_bytes
        self.target_seen = self.target_bytes_seen = self.target_bytes_retained = 0
        self.dropped_target_records = self.dropped_target_bytes = 0
        self.noise_count, self.noise_tail = 0, []

    def append(self, text):
        if self.TARGET not in text:
            self.noise_count += 1
            # At most16 short notices, regardless of total DEBUG traffic.
            self.noise_tail.append(dict(text=text[:1024], truncated=len(text) > 1024))
            if len(self.noise_tail) > 16:
                del self.noise_tail[0]
            return
        self.target_seen += 1
        size = len(text.encode('utf-8', errors='replace'))
        self.target_bytes_seen += size
        if (len(self.items) >= self.target_record_limit or
                size > self.target_byte_limit - self.target_bytes_retained):
            self.dropped_target_records += 1
            self.dropped_target_bytes += size
            return
        self.items.append(text)
        self.target_bytes_retained += size

    def evidence(self):
        return dict(target_record_limit=self.target_record_limit, target_byte_limit=self.target_byte_limit,
                    target_seen=self.target_seen, target_retained=len(self.items),
                    target_bytes_seen=self.target_bytes_seen, target_bytes_retained=self.target_bytes_retained,
                    dropped_target_records=self.dropped_target_records, dropped_target_bytes=self.dropped_target_bytes,
                    noise_count=self.noise_count, noise_tail_limit=16, noise_prefix_char_limit=1024,
                    noise_tail=list(self.noise_tail), targets=list(self.items))


class HostRandom:
    """Independent arithmetic oracle for OG's fixed pg_lrand48 recurrence."""
    def __init__(self, seed):
        self.state = sum(int(word) << (16 * i) for i, word in enumerate(seed))

    def fraction(self):
        self.state = (self.state * 0x5DEECE66D + 11) & ((1 << 48) - 1)
        return ((self.state >> 17) + 1) / 2147483649.0

    def sample(self, size, count):
        cursor, selected = 0, 0
        while cursor < size and selected < count:
            remaining, wanted = size - cursor, count - selected
            if wanted < remaining:
                draw, cutoff = self.fraction(), 1.0 - wanted / remaining
                while draw < cutoff:
                    cursor += 1
                    remaining -= 1
                    cutoff *= 1.0 - wanted / remaining
            yield cursor
            selected += 1
            cursor += 1


def selected_positions(evidence):
    population, length, blocks = (int(evidence[k]) for k in ('physical', 'L', 'M'))
    if length == 1:
        return list(range(population))
    rng = HostRandom(evidence['seed'].split(','))
    result = []
    for block in rng.sample(blocks, int(evidence['B'])):
        for position in rng.sample(length, int(evidence['r'])):
            absolute = block * length + position
            if absolute < population:
                result.append(absolute)
    return result


def requested_int4_sample_rows(default_target, columns):
    """Host std_typanalyze minrows for this two-int4, no-index fixture.

    Derive from actual GUC/pg_attribute requests, never sampler log values.
    OG single-node compute_attr_target uses300*effective per-column target;
    do_analyze_calculate_sample_target_rows takes their maximum and at least100.
    """
    if default_target <= 0 or not columns or any(oid != 23 or target < -1 for _, oid, target in columns):
        raise ValueError('Independent request oracle requires ordinary int4 statistics targets')
    return max([100] + [300 * (default_target if target == -1 else target)
                       for _, _, target in columns if target != 0])


def verified_sample_design(evidence, physical, requested_rows):
    """Check design parameters and first-order p; not a variance/NDV proof."""
    if physical < 0 or requested_rows <= 0:
        raise ValueError('Invalid independent physical population or sample request')
    if physical <= requested_rows:
        length, blocks, sampled, per_block = 1, physical, physical, 1
        probability = Fraction(1)
    else:
        length = 4096
        blocks = (physical + length - 1) // length
        sampled = min(8, max(1, requested_rows // 32), blocks)
        per_block = min(length, requested_rows // sampled)
        if sampled * per_block < min(requested_rows, max(100, requested_rows // 4)):
            raise ValueError('Independent requested precision exceeds the bounded design')
        probability = Fraction(sampled, blocks) * Fraction(per_block, length)
    expected = dict(physical=physical, L=length, M=blocks, B=sampled, r=per_block)
    if any(int(evidence[key]) != value for key, value in expected.items()):
        raise AssertionError('Sampler domain/design differs from independent population/request: ' + repr(expected))
    if not math.isclose(float(evidence['p']), float(probability), rel_tol=4e-15, abs_tol=0.0):
        raise AssertionError('Sampler p differs from exact independent B/M times r/L')
    return probability, expected


def fixture_parameters(mode):
    """Only reduce expensive RC preparation, preserving each sampling domain."""
    if mode == 'small':
        return dict(count=1000, default_target=10, update_modulus=97, update_residue=0)
    if mode == 'sampling':
        #40 uniformly spaced non-NULL rows,20 in each real50000-row file.
        return dict(count=100000, default_target=10, update_modulus=2500, update_residue=1)
    if mode == 'refusal':
        return dict(count=320, default_target=10, update_modulus=97, update_residue=0, refusal_default_target=1)
    raise ValueError('Unknown bounded statistics fixture mode')


def fixture_survivor_floor(physical, requested_rows):
    """Conservative bound for every allowed block/position sample, any seed.

    For a selected full/padded block, at most L-visible positions can be
    invalid; hence at least max(0,r-(L-visible)) survive. The sum of the B
    smallest block floors is a guaranteed lower bound over all selections.
    This is a fixture proof, not an empirical variance or selectivity claim.
    """
    if requested_rows <= 0:
        raise ValueError('Positive sample request required')
    population = len(physical)
    if population <= requested_rows:
        return dict(minimum_kept=sum(row is not None for row in physical),
                    L=1, M=population, B=population, r=1, probability=Fraction(1))
    length = 4096
    blocks = (population + length - 1) // length
    sampled = min(8, max(1, requested_rows // 32), blocks)
    per_block = min(length, requested_rows // sampled)
    visible = [sum(row is not None for row in physical[first:first+length])
               for first in range(0, population, length)]
    floors = sorted(max(0, per_block - (length - count)) for count in visible)
    return dict(minimum_kept=sum(floors[:sampled]), L=length, M=blocks, B=sampled, r=per_block,
                probability=Fraction(sampled, blocks) * Fraction(per_block, length))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--build-receipt', required=True, type=Path)
    parser.add_argument('--mode', required=True, choices=['small', 'sampling', 'refusal'])
    args = parser.parse_args()
    validate_run_id(args.run_id)
    out = ROOT / '公共文档/openGauss原生湖存储/验证' / args.run_id
    out.mkdir(exist_ok=False)
    result = dict(status='fail', scope=__doc__, mode=args.mode, assertions=[], executed=False)
    fixture = fixture_parameters(args.mode)
    result['fixture_parameters'] = fixture
    result['scale_limit_not_resolved'] = ('Per-target RC latest-row lookup still scans the complete lake. '
        'This fixture bounds DML preparation; no batch-RC performance improvement or qualification is claimed.')
    control = db = roles = case = None
    sources = [Path(__file__), D / 'tests/native_table_create.py', D / 'tests/permission_api.py',
               D / 'native_test_context.py', D / 'native_runtime.py', D / 'tests/inline_cleanup.py',
               D / 'tests/file_dml_acceptance.py', D / 'inline_fixture.py', D / 'tests/native_auto_flush.py',
               D / 'tests/catalog_fixture_bundle.py', D / 'tests/empty_reference_fixture.py',
               D / 'tests/empty_manifest.py', D / 'tests/empty_catalog_oracle.py',
               D / 'tests/create_reference_oracle.py', D / 'tests/reverse_reference.py', D / 'reference.sh',
               D / 'fixture.py', D / 'lake_harness.py', D / 'source-lock.json']
    result['inputs'] = {str(path): sha256(path) for path in sources}
    archive = out / 'executed-inputs'
    archive.mkdir()
    for path in sources:
        shutil.copy2(path, archive / path.name)
    duckdb = ROOT / '.reference/opengauss-native-lake/build/duckdb-gcc10/duckdb'
    provider = INSTALL / 'lib/postgresql/og_lake_sdk/libog_lake_sdk_arrow.so'

    def image(value):
        if isinstance(value, list) and len(value) > 30:
            return dict(rows=len(value), sha256=hashlib.sha256(repr(value).encode()).hexdigest())
        return value

    def check(name, observed, expected=True):
        passed = observed == expected
        result['assertions'].append(dict(name=name, status='pass' if passed else 'fail',
                                         observed=deepcopy(image(observed)), expected=deepcopy(image(expected))))
        if not passed:
            raise AssertionError(name)

    def literal(value):
        return "'" + str(value).replace("'", "''") + "'"

    def independent(statement, label):
        (out / (label + '.sql')).write_text(statement + '\n')
        proc = subprocess.run([str(duckdb), '-no-init', '-batch', '-json', '-c', 'SET threads=1;' + statement],
                              capture_output=True, text=True, timeout=90)
        (out / (label + '.stdout')).write_text(proc.stdout)
        (out / (label + '.stderr')).write_text(proc.stderr)
        if proc.returncode:
            raise RuntimeError('Independent DuckDB failed: ' + label)
        return json.loads(proc.stdout) if proc.stdout.strip() else []

    try:
        from psycopg2 import sql
        import psycopg2
        built = check_build(args.build_receipt)
        check('receipt includes actual new sampler input', 'contrib/og_lake/lake_sample.cpp' in built['native_source_inputs_after'])
        result.update(source_head=built['source_head'], binary_sha256=built['binary_sha256'],
                      build=str(args.build_receipt.resolve()), build_sha256=sha256(args.build_receipt),
                      provider_sha256=sha256(provider), reference_binary_sha256=sha256(duckdb))
        control = connect_control();query(control,'SET session_timeout=0');query(control,'SET statement_timeout=30000')
        check('automatic maintenance remains off', scalar(control, 'SHOW enable_lake_maintenance'), 'off')
        with (out / 'fixture.log').open('w') as log:
            subprocess.run([sys.executable, str(D / 'tests/catalog_fixture_bundle.py'), '--run-id', args.run_id,
                            '--output', str(out / 'fixture/manifest.json')], stdout=log,
                           stderr=subprocess.STDOUT, check=True, timeout=300)
        db = OwnedDatabase(args.run_id, out)
        db.create()
        roles = OwnedRoles(control, args.run_id, out)
        case = Case(db, SimpleNamespace(mode='net_dml', sql_version='0.18', manifest=out / 'fixture/manifest.json',
                    output=out / 'result.json', run_id=args.run_id), result, roles)
        for conn in (case.a, case.b, case.observer):
            query(conn, 'SET statement_timeout=90000')
            query(conn, 'SET session_timeout=0')
            query(conn, 'SET query_dop=1')
            query(conn, 'SET default_statistics_target=' + str(fixture['default_target']))
            query(conn, 'SET enable_default_ustore_table=off')
        check('runtime advertises bounded file ANALYZE', json.loads(scalar(case.a, 'SELECT og_lake.host_status()'))['file_analyze'], True)
        case.create()
        query(case.a, 'CREATE TABLE biz.reference(id integer,val integer)')
        query(case.a, "SELECT og_lake.set_option('biz.orders'::regclass,'data_inlining_row_limit','200000')")

        def both(statement):
            query(case.a, 'BEGIN')
            for relation in ('biz.orders', 'biz.reference'):
                query(case.a, statement.format(table=relation))
            query(case.a, 'COMMIT')

        def stats(relation):
            return query(case.a, 'SELECT attname,null_frac,avg_width,n_distinct,most_common_vals::text,'
                         'most_common_freqs::text,histogram_bounds::text FROM pg_stats '
                         "WHERE schemaname='biz' AND tablename=%s AND NOT inherited ORDER BY attname", (relation,))

        def raw_stats():
            return query(case.a, "SELECT staattnum,s::text FROM pg_statistic s WHERE starelid='biz.orders'::regclass "
                         "AND starelkind='c' AND NOT stainherit ORDER BY staattnum")

        def relstats():
            return query(case.a, "SELECT reltuples,relpages,relallvisible FROM pg_class WHERE oid='biz.orders'::regclass")[0]

        def analyze():
            notices = Notices()
            case.a.notices = notices
            try:
                query(case.a, 'SET client_min_messages=debug1')
                query(case.a, 'ANALYZE biz.orders')
                query(case.a, 'RESET client_min_messages')
            finally:
                capture = notices.evidence()
                capture_path = out / ('sampling-notices-' + str(len(result.get('notice_captures', []))) + '.json')
                write_json(capture_path, capture)
                result.setdefault('notice_captures', []).append(dict(
                    artifact=str(capture_path), target_seen=capture['target_seen'],
                    target_retained=capture['target_retained'], target_bytes_seen=capture['target_bytes_seen'],
                    target_bytes_retained=capture['target_bytes_retained'],
                    dropped_target_records=capture['dropped_target_records'],
                    dropped_target_bytes=capture['dropped_target_bytes'], noise_count=capture['noise_count']))
            check('ANALYZE target evidence collection is bounded and complete', notices.dropped_target_records, 0)
            relevant = list(notices.items)
            check('actual new sampler emitted exactly one evidence record', len(relevant), 1)
            evidence = dict(re.findall(r'(\w+)=([^\s;]+)', relevant[0]))
            result.setdefault('sampling_evidence', []).append(dict(text=relevant[0], parsed=evidence))
            check('independent footer opens are explicitly unobserved', int(evidence['unobserved_footer_opens']) > 0)
            check('reader-only byte counter is present', int(evidence['reader_bytes']) > 0)
            return evidence

        count = fixture['count']
        for first in range(1, count + 1, 50000):
            last = min(count, first + 49999)
            both('INSERT INTO {table} SELECT i,CASE WHEN i%10=0 THEN NULL ELSE i%20 END '
                 'FROM generate_series(' + str(first) + ',' + str(last) + ')i')
            if args.mode == 'small':
                both('UPDATE {table} SET val=val+30 WHERE id=1')
                both('UPDATE {table} SET val=val+30 WHERE id=1')
                both('DELETE FROM {table} WHERE id=2')
            query(case.a, "SELECT og_lake.flush('biz.orders'::regclass)")
        predicate = 'id%' + str(fixture['update_modulus']) + '=' + str(fixture['update_residue'])
        updated_ids = [i for i in range(1, count+1) if i % fixture['update_modulus'] == fixture['update_residue']]
        check('bounded repeated-UPDATE target set equals independent heap',
              query(case.a, 'SELECT id FROM biz.reference WHERE ' + predicate + ' ORDER BY id'),
              [(i,) for i in updated_ids])
        result['fixture_updated_ids'] = updated_ids
        both('UPDATE {table} SET val=val+1000 WHERE ' + predicate)
        both('UPDATE {table} SET val=val+1000 WHERE ' + predicate)
        both('INSERT INTO {table} SELECT i,9999 FROM generate_series(' + str(count+1) + ',' + str(count+18) + ')i')
        check('complete visible lake equals independent ordinary heap',
              query(case.a, 'SELECT * FROM biz.orders ORDER BY id'), query(case.a, 'SELECT * FROM biz.reference ORDER BY id'))
        table_id = scalar(case.a, "SELECT table_id FROM created_metadata.ducklake_table WHERE table_name='orders' AND end_snapshot IS NULL")
        carrier = scalar(case.a, 'SELECT table_name FROM created_metadata.ducklake_inlined_data_tables WHERE table_id=%s', (table_id,))
        raw_inline = query(case.a, sql.SQL('SELECT _ducklake_row_id,_ducklake_begin_snapshot,_ducklake_end_snapshot,id,val '
            'FROM created_metadata.{} ORDER BY ctid').format(sql.Identifier(carrier)))
        snapshot = case.head()[0]
        # The row list's ordinal is the physical candidate position used by
        # the sampler. Preserve ORDER BY ctid before a protocol export sorts
        # tuple strings; replay needs this order even after owned DB cleanup.
        inline_oracle_path = out / 'physical-inline-position-oracle.json'
        write_json(inline_oracle_path, dict(lake_snapshot=snapshot, order_by='ctid ASC',
            position='zero-based list ordinal',
            columns=['_ducklake_row_id', '_ducklake_begin_snapshot', '_ducklake_end_snapshot', 'id', 'val'],
            rows=raw_inline))
        result['physical_inline_oracle'] = dict(path=str(inline_oracle_path), sha256=sha256(inline_oracle_path),
                                                candidate_positions=len(raw_inline), lake_snapshot=snapshot)
        delete_name = 'ducklake_inlined_delete_' + str(table_id)
        deletes = set(query(case.a, sql.SQL('SELECT file_id,row_id FROM created_metadata.{} WHERE begin_snapshot<=%s').format(
            sql.Identifier(delete_name)), (snapshot,)))
        file_rows = query(case.a, 'SELECT data_file_id,path,record_count FROM created_metadata.ducklake_data_file '
                          'WHERE table_id=%s AND end_snapshot IS NULL ORDER BY data_file_id', (table_id,))
        physical, group_domains, original_hashes = [], [], {}
        data_root = Path(case.empty_manifest['data_path'])
        remote_deletes = query(case.a, 'SELECT data_file_id,path FROM created_metadata.ducklake_delete_file '
                               'WHERE table_id=%s AND end_snapshot IS NULL ORDER BY data_file_id', (table_id,))
        for ordinal, (file_id, name) in enumerate(remote_deletes):
            matches = [path for path in data_root.rglob('*.parquet') if path.name == Path(name).name]
            check('one genuine owned versioned delete object ' + str(ordinal), len(matches), 1)
            path = matches[0]
            check('delete object stays inside owned data root', path.resolve().is_relative_to(data_root.resolve()))
            original_hashes[str(path)] = sha256(path)
            marks = independent('SELECT pos AS position,_ducklake_internal_snapshot_id AS deleted_at FROM read_parquet('
                                + literal(path) + ') ORDER BY pos', 'deletes-' + str(ordinal))
            deletes.update((file_id, row['position']) for row in marks if row['deleted_at'] <= snapshot)
        for ordinal, (file_id, name, record_count) in enumerate(file_rows):
            matches = [path for path in data_root.rglob('*.parquet') if path.name == Path(name).name]
            check('one genuine owned immutable data source ' + str(ordinal), len(matches), 1)
            path = matches[0]
            check('source stays inside owned data root', path.resolve().is_relative_to(data_root.resolve()))
            original_hashes[str(path)] = sha256(path)
            rows = independent('SELECT file_row_number AS position,_ducklake_internal_row_id AS logical_id,'
                               '_ducklake_internal_snapshot_id AS birth,id,val FROM read_parquet('
                               + literal(path) + ',file_row_number=true) ORDER BY file_row_number', 'physical-' + str(ordinal))
            check('real source population matches standard metadata ' + str(ordinal), len(rows), record_count)
            if args.mode == 'sampling':
                check('sampling retains two50000-row genuine files', (len(file_rows), record_count), (2, 50000))
                check('uniform UPDATE targets cover both real files equally ' + str(ordinal),
                      sum(row['id'] in updated_ids for row in rows), 20)
            start = len(physical)
            for row in rows:
                visible = row['birth'] <= snapshot and (file_id, row['position']) not in deletes
                physical.append((row['id'], row['val']) if visible else None)
            groups = independent('SELECT row_group_id,max(row_group_num_rows) AS rows FROM parquet_metadata('
                                 + literal(path) + ') GROUP BY row_group_id ORDER BY row_group_id', 'groups-' + str(ordinal))
            first = start
            for group in groups:
                group_domains.append((first, first + int(group['rows'])))
                first += int(group['rows'])
            check('real footer row groups cover exact file population ' + str(ordinal), first, len(physical))
        for row_id, birth, ended, business_id, value in raw_inline:
            physical.append((business_id, value) if birth <= snapshot and (ended is None or ended > snapshot) else None)
        before, head = case.protocol(), case.head()
        default_target = int(scalar(case.a, 'SHOW default_statistics_target'))
        column_targets = query(case.a, "SELECT attname,atttypid::integer,attstattarget FROM pg_attribute "
                               "WHERE attrelid='biz.orders'::regclass AND attnum>0 AND NOT attisdropped ORDER BY attnum")
        requested_rows = requested_int4_sample_rows(default_target, column_targets)
        result['sample_request'] = dict(default_target=default_target, columns=column_targets, requested_rows=requested_rows)
        floor = fixture_survivor_floor(physical, requested_rows)
        result['fixture_survivor_floor'] = dict(floor, probability=str(floor['probability']))
        if args.mode != 'small':
            check('bounded UPDATE histories preserve exact physical candidate count',
                  len(physical), count + 2*len(updated_ids) + 18)
            check('two UPDATE rounds preserve old-file and ended-inline candidates',
                  sum(row is None for row in physical), 2*len(updated_ids))
            if args.mode == 'sampling':
                check('fixture guarantees adequate visible sample for every random seed',
                      floor['minimum_kept'] >= min(requested_rows, max(100, requested_rows//4)))
                check('large fixture has genuinely partial physical inclusion', floor['probability'] < 1)
            else:
                check('refusal baseline uses a complete p-one sample', str(floor['probability']), '1')
                check('complete baseline covers every visible row', floor['minimum_kept'], count+18)
        evidence = analyze()
        check('logged candidate population equals independent file+inline physical domain', int(evidence['physical']), len(physical))
        probability, design = verified_sample_design(evidence, len(physical), requested_rows)
        result['independent_sample_design'] = dict(design, probability_numerator=probability.numerator,
                                                   probability_denominator=probability.denominator)
        check('sampling design and probability match independent population/request', True)
        positions = selected_positions(evidence)
        expected = [physical[position] for position in positions if physical[position] is not None]
        check('every selected physical position occurs once', len(set(positions)), len(positions))
        check('visible survivor count matches independent physical-position oracle', int(evidence['kept']), len(expected))
        expected_groups = sum(any(first <= position < end for position in positions) for first, end in group_domains)
        check('only predicted source row groups were decoded', int(evidence['groups']), expected_groups)
        query(case.a, 'CREATE TABLE biz.sample_expected(id integer,val integer)')
        if expected:
            query(case.a, 'INSERT INTO biz.sample_expected VALUES ' + ','.join(['(%s,%s)'] * len(expected)),
                  [value for row in expected for value in row])
        query(case.a, 'ANALYZE biz.sample_expected')
        check('business pg_stats distributions equal independently reconstructed selected visible sample', stats('orders'), stats('sample_expected'))
        estimated = float(Fraction(len(expected), 1) / probability)
        check('host visible population uses sample inclusion probability', abs(float(relstats()[0]) - estimated) <= max(.01, estimated * 1e-6))
        check('business statistics have both actual columns', len(stats('orders')), 2)
        check('ordinary host all-visible-page semantics remain zero', relstats()[2], 0)
        plan = '\n'.join(row[0] for row in query(case.a, 'EXPLAIN SELECT * FROM biz.orders WHERE val=1'))
        result['actual_plan'] = plan
        check('optimizer consumes analyzed visible baseline', 'Row Estimate: pg_class / ANALYZE visible file+inline estimate' in plan)
        check('ANALYZE does not publish DuckLake metadata', case.protocol(), before)
        check('ANALYZE does not allocate snapshot/row IDs', case.head(), head)
        check('sampling preserves source Parquet bytes', {name: sha256(name) for name in original_hashes}, original_hashes)
        if args.mode == 'small':
            query(case.a, 'ANALYZE biz.reference')
            check('small complete logical sample equals full independent heap statistics', stats('orders'), stats('reference'))
            check('small physical domain has inclusion probability one', float(evidence['p']), 1.0)
        elif args.mode == 'sampling':
            check('large file-backed table uses less than full physical population', len(positions) < len(physical))
            check('actual SDK decoder did not scan all physical source rows', int(evidence['reader_decoded']) < sum(row[2] for row in file_rows))
            check('sample has enough visible survivors for column statistics', len(expected) >= 750)
            check('actual sample respects independently proven fixture lower bound', len(expected) >= floor['minimum_kept'])
            query(case.a, 'BEGIN')
            query(case.a, 'UPDATE biz.orders SET val=12345 WHERE id=1')
            query(case.a, 'SAVEPOINT deny_stats')
            try:
                query(case.a, 'ANALYZE biz.orders')
            except psycopg2.Error as exc:
                check('persistent ANALYZE inside private-write transaction retains host guard', exc.pgcode, '25001')
            else:
                check('persistent ANALYZE inside private-write transaction rejects', 'accepted', '25001')
            query(case.a, 'ROLLBACK TO deny_stats')
            query(case.a, 'ROLLBACK')
        else:
            # Keep the complete p1 baseline BEFORE deleting business rows.
            # The later negative request is deliberately different (target1).
            old_stats, old_relation = raw_stats(), relstats()
            for first in range(1, count + 19, 5000):
                both('DELETE FROM {table} WHERE id BETWEEN ' + str(first) + ' AND ' + str(first + 4999))
            check('true business population is now empty', scalar(case.a, 'SELECT count(*) FROM biz.orders'), 0)
            check('independent ordinary heap is empty too', scalar(case.a, 'SELECT count(*) FROM biz.reference'), 0)
            # Native DELETE ends logical versions; it must not turn this test
            # into an exact p1 empty-table sample. Derive actual retained N from
            # current metadata and raw inline rows, without inventing tombstones.
            retained_files = scalar(case.a, 'SELECT COALESCE(sum(record_count),0) FROM created_metadata.ducklake_data_file '
                                    'WHERE table_id=%s AND end_snapshot IS NULL', (table_id,))
            retained_inline = scalar(case.a, sql.SQL('SELECT count(*) FROM created_metadata.{}').format(sql.Identifier(carrier)))
            retained = int(retained_files) + int(retained_inline)
            check('DELETE preserves physical versions needed for negative sampling domain', retained, len(physical))
            query(case.a, 'SET default_statistics_target=' + str(fixture['refusal_default_target']))
            refusal_default = int(scalar(case.a, 'SHOW default_statistics_target'))
            refusal_columns = query(case.a, "SELECT attname,atttypid::integer,attstattarget FROM pg_attribute "
                                    "WHERE attrelid='biz.orders'::regclass AND attnum>0 AND NOT attisdropped ORDER BY attnum")
            refusal_request = requested_int4_sample_rows(refusal_default, refusal_columns)
            check('negative case actually lowers requested sample to300', refusal_request, 300)
            # No directory/delete record is synthesized: all business rows were
            # really deleted, so every retained candidate is logically invisible.
            refused_design = fixture_survivor_floor([None] * retained, refusal_request)
            result['refusal_design'] = dict(refused_design, probability=str(refused_design['probability']), physical=retained,
                                            file_positions=int(retained_files), inline_positions=int(retained_inline),
                                            default_target=refusal_default, columns=refusal_columns, requested_rows=refusal_request)
            check('empty business relation still requires true p-less-than-one sampling',
                  retained > refusal_request and refused_design['probability'] < 1 and refused_design['minimum_kept'] == 0)
            check('negative design has exact independently derived probability',str(refused_design['probability']),str(Fraction(300,4096)))
            check('real DELETE keeps complete baseline pg_statistic',raw_stats(),old_stats)
            check('real DELETE keeps complete baseline relation scale',relstats(),old_relation)
            unchanged_catalog = case.protocol()
            try:
                query(case.a, 'ANALYZE biz.orders')
            except psycopg2.Error as exc:
                check('thin visible sample is refused before pg_stats publication', exc.pgcode, '55000')
                check('failure is the actual sampler survivor guard', 'insufficient visible survivors' in str(exc))
            else:
                check('thin sample must not publish arbitrary zero/biased distributions', 'accepted', '55000')
            check('sample refusal preserves old pg_statistic exactly', raw_stats(), old_stats)
            check('sample refusal preserves old pg_class scale exactly', relstats(), old_relation)
            check('sample refusal leaves DuckLake catalog untouched', case.protocol(), unchanged_catalog)
        check('genuine independent reference binary unchanged', sha256(duckdb), result['reference_binary_sha256'])
        check('provider unchanged', sha256(provider), result['provider_sha256'])
        check('all archived executed inputs unchanged', all(sha256(path) == digest for path, digest in result['inputs'].items()))
        check_build(args.build_receipt)
        result.update(status='pass', executed=True)
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
                    raise RuntimeError('Owned database cleanup unproven; roles retained')
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
