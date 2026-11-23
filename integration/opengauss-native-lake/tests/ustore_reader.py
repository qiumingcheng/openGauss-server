#!/usr/bin/env python3
"""Actual Ustore page-cache, rescan, private-version and optional ANALYZE test.

Draft only. It does not generate data by inserting protocol records. The full
reference export is imported unchanged except for the host carrier TableAM.
"""
import argparse
import copy
import json
import os
from pathlib import Path
import re
import sys
import traceback

ROOT = Path('/home/qiumc/data/code/cpp')
DRIVER = Path(os.getenv('OG_LAKE_DRIVER_ROOT', str(ROOT / 'integration/opengauss-native-lake')))
sys.path.insert(0, str(DRIVER))
sys.path.insert(0, str(DRIVER / 'tests'))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from inline_fixture import export_reference, foreign_table_statement, multiset, query, scalar
from lake_harness import Blocked, sha256, validate_run_id, write_json
from native_test_context import OwnedDatabase, reviewed_runtime_version
from inline_modify import Case as ModifyCase
from ustore_fixture import import_catalog_ustore


class UstoreCase(ModifyCase):
    def __init__(self, db, args, result):
        from psycopg2 import sql
        self.sql, self.db, self.args, self.result = sql, db, args, result
        self.a, self.b, self.c = [db.connect() for _ in range(3)]
        # Test the current runtime against an already supported SQL API. No
        # private-owner grant or metadata DML is used by ordinary business SQL.
        query(self.a, "CREATE EXTENSION og_lake VERSION '0.4'")
        caps = json.loads(scalar(self.a, 'SELECT og_lake.host_status()'))
        self.check('runtime matches current reviewed source', caps.get('module_version'), reviewed_runtime_version())
        self.check('native inline read/update/delete are present',
                   all(caps.get(k) is True for k in ('inline_scan', 'inline_update', 'inline_delete')))
        result['capabilities'] = caps
        export_path = args.output.parent / 'reference-export-current.json'
        self.exported, self.manifest = export_reference(DRIVER, args.manifest, export_path, args.run_id)
        required = {'same_page': 'I', 'long_text': 'LONG_TEXT', 'multipage': 'USTORE_STREAM'}[args.mode]
        self.check('actual requested reference profile', self.manifest['case'], required)
        result['artifacts'] += [{'path': str(Path(args.manifest).resolve())}, {'path': str(export_path)},
                                {'path': str(export_path.with_suffix('.log'))}]
        result['fixture_manifest_sha256'] = sha256(args.manifest)
        query(self.a, 'BEGIN')
        try:
            counts = import_catalog_ustore(self.a, 'lake_metadata', self.exported)
            query(self.a, 'COMMIT')
        except BaseException:
            query(self.a, 'ROLLBACK')
            raise
        self.check('all reference tables and every value survived host import', len(counts), len(self.exported['tables']))
        self.names = [c['name'] for c in self.manifest['schema']]
        self.initial = [{n: row[n] for n in self.names} for row in self.manifest['expected']]
        self.table_id = self.manifest['table_id']
        carriers = [r['table_name'] for t in self.exported['tables'] if t['table_name'] == 'ducklake_inlined_data_tables'
                    for r in t['rows'] if r['table_id'] == self.table_id]
        self.check('one actual target carrier', len(carriers), 1)
        self.carrier = carriers[0]
        query(self.a, 'CREATE SCHEMA biz')
        query(self.a, "SELECT og_lake.attach_catalog('lake_reference','lake_metadata',NULL::text,false)")
        query(self.a, foreign_table_statement('biz', 'orders', 'lake_reference', self.table_id, self.manifest['schema']))
        self.groups, self.group_heads, self.operations = [], [], None
        self.start_counter = self.counter()
        self.baseline = self.image(self.c)

    def layout(self):
        # ctid is inspected only on the real local Ustore carrier, never on the
        # lake table and never encoded as a logical lake identity.
        rows = query(self.a, self.sql.SQL('SELECT ctid::text FROM lake_metadata.{}').format(
            self.sql.Identifier(self.carrier)))
        blocks = []
        for (tid,) in rows:
            match = re.fullmatch(r'\((\d+),(\d+)\)', tid)
            if not match:
                raise AssertionError('Unexpected actual local tuple location: ' + tid)
            blocks.append(int(match.group(1)))
        counts = {block: blocks.count(block) for block in sorted(set(blocks))}
        self.result['actual_carrier_block_counts'] = counts
        self.check('at least two actual tuples share a Ustore page', max(counts.values()) >= 2)
        if self.args.mode == 'same_page':
            self.check('the three source rows actually share one page', len(counts), 1)
        if self.args.mode == 'multipage':
            self.check('real reference rows actually cross Ustore pages', len(counts) >= 2)
            self.check('large profile contains all 1024 original business rows', len(rows), 1024)
        if self.args.mode == 'long_text':
            notes = [r['note'] for r in self.initial if r.get('note') is not None]
            self.check('fixture actually includes a large nontruncated text value', max(len(n.encode('utf8')) for n in notes) > 8192)
            storage = query(self.a, 'SELECT c.reltoastrelid,pg_relation_size(c.reltoastrelid) '
                'FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace '
                "WHERE n.nspname='lake_metadata' AND c.relname=%s", (self.carrier,))
            self.result['actual_toast_storage'] = storage
            self.check('real Ustore long-text carrier has allocated TOAST storage', len(storage) == 1 and storage[0][0] != 0 and storage[0][1] > 0)

    def rescan(self):
        query(self.a, 'CREATE TABLE biz.ustore_driver(k integer)')
        query(self.a, 'INSERT INTO biz.ustore_driver VALUES(1),(2),(3),(1000000)')
        query(self.a, 'ANALYZE biz.ustore_driver')
        settings = ('enable_material', 'enable_hashjoin', 'enable_mergejoin')
        try:
            for name in settings:
                query(self.a, 'SET ' + name + '=off')
            # LEFT join preserves the small outer relation and its unmatched
            # row. The actual plan must prove a genuine inner ForeignScan rescan.
            statement = 'SELECT d.k,o.id,o.val FROM biz.ustore_driver d LEFT JOIN biz.orders o ON o.id>=d.k'
            plan = '\n'.join(row[0] for row in query(self.a, 'EXPLAIN ANALYZE ' + statement))
            self.result['actual_rescan_plan'] = plan
            loops = [int(m.group(1)) for line in plan.splitlines() if 'Foreign Scan' in line
                     for m in [re.search(r'loops=(\d+)', line)] if m]
            self.check('actual ForeignScan executes repeatedly, not a materialized cached result',
                       bool(loops) and max(loops) >= 4)
            expected = []
            for key in (1, 2, 3, 1000000):
                matches = [r for r in self.initial if r['id'] >= key]
                expected += [[key, r['id'], r['val']] for r in matches] if matches else [[key, None, None]]
            self.check('all rescans return the exact joined values', multiset(query(self.a, statement)), multiset(expected))
        finally:
            for name in settings:
                query(self.a, 'RESET ' + name)

    def mutations(self):
        self.begin()
        self.mutate('UPDATE biz.orders SET val=17 WHERE id=1')
        self.mutate('UPDATE biz.orders SET val=val+1 WHERE id=1')
        wanted = [dict(r, val=18) if r['id'] == 1 else dict(r) for r in self.initial]
        self.expect(self.a, wanted, 'Ustore old source is hidden by its latest private replacement')
        self.expect(self.b, self.initial, 'other session still reads original committed Ustore versions')
        query(self.a, 'SAVEPOINT reader_child')
        query(self.a, 'DELETE FROM biz.orders WHERE id=1')
        self.expect(self.a, [r for r in wanted if r['id'] != 1], 'child tombstone hides parent replacement')
        query(self.a, 'ROLLBACK TO SAVEPOINT reader_child')
        query(self.a, 'RELEASE SAVEPOINT reader_child')
        self.expect(self.a, wanted, 'child rollback restores the parent replacement')
        self.mutate('DELETE FROM biz.orders WHERE id=2')
        wanted = [r for r in wanted if r['id'] != 2]
        self.commit()
        self.expect(self.c, wanted, 'committed Ustore scan excludes ended versions')
        self.check('UPDATE/DELETE allocate no new logical identities', self.counter(), self.start_counter)
        self.reverse(wanted)
        return wanted

    def analyze(self, wanted):
        before = self.image(self.c)
        query(self.a, 'SET default_statistics_target=100')
        try:
            query(self.a, 'ANALYZE biz.orders')
        finally:
            query(self.a, 'RESET default_statistics_target')
        self.check('ANALYZE never mutates standard lake records', self.image(self.c), before)
        self.check('ANALYZE counts visible rows after U/D, not physical stored versions',
            scalar(self.a, "SELECT reltuples::bigint FROM pg_class WHERE oid='biz.orders'::regclass"), len(wanted))
        stats = query(self.a, "SELECT attname FROM pg_stats WHERE schemaname='biz' AND tablename='orders' ORDER BY attname")
        self.check('ANALYZE produces each supported business column', sorted(r[0] for r in stats), sorted(self.names))
        self.expect(self.a, wanted, 'normal query remains correct after sampling cleanup')


def run_scenario(t):
    t.layout()
    for index in range(4):
        t.expect(t.a, t.initial, 'complete normal scan, including same-page cache, round ' + str(index))
    t.check('read-only scans preserve every protocol record', t.image(t.c), t.baseline)
    t.rescan()
    wanted = t.mutations()
    if t.args.analyze:
        t.analyze(wanted)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-id', default=os.getenv('OG_LAKE_RUN_ID'))
    parser.add_argument('--output', default=os.getenv('OG_LAKE_RESULT_FILE'))
    parser.add_argument('--manifest', default=os.getenv('OG_LAKE_FIXTURE_MANIFEST'))
    parser.add_argument('--mode', required=True, choices=['same_page', 'long_text', 'multipage'])
    parser.add_argument('--analyze', action='store_true')
    args = parser.parse_args()
    if not all((args.run_id, args.output, args.manifest)):
        parser.error('run-id/output/verified fixture manifest are required')
    validate_run_id(args.run_id)
    args.output = Path(args.output).resolve()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    result = {'status': 'fail', 'scope': 'ustore_reader/' + args.mode, 'assertions': [], 'artifacts': [],
              'notes': ['Physical TableAM adaptation only; all standard input values come from the fixed reference.',
                        'No ASAN, SDK, RAC, long-duration stability, or unobserved cross-thread behavior is implied.']}
    owned = None
    try:
        with OwnedDatabase(args.run_id, args.output.parent) as owned:
            run_scenario(UstoreCase(owned, args, result))
            result['status'] = 'pass'
    except Blocked as exc:
        result.update(status='blocked', reason=str(exc))
    except BaseException as exc:
        result.update(status='fail', error=repr(exc), traceback=traceback.format_exc())
    finally:
        if owned is not None:
            result['artifacts'].extend(owned.artifacts())
            result['cleanup'] = owned.cleanup_result
        write_json(args.output, result)
        print(json.dumps({'status': result['status'], 'output': str(args.output)}))
    return {'pass': 0, 'blocked': 2}.get(result['status'], 1)


if __name__ == '__main__':
    raise SystemExit(main())
