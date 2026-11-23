#!/usr/bin/env python3
"""Real modifying-CTE admission: ordinary heap positive controls and lake refusal.

Runs only in an owned database imported from the fixed real I reference fixture.
Every probing transaction rolls back, including an unexpectedly accepted lake
CTE on the old binary. No manual DuckLake snapshot/data record is synthesized.
"""
import argparse
import json
import os
from pathlib import Path
import sys
import traceback

WORKSPACE = Path('/home/qiumc/data/code/cpp')
DRIVER = Path(os.getenv('OG_LAKE_DRIVER_ROOT', str(WORKSPACE / 'integration/opengauss-native-lake')))
sys.path.insert(0, str(DRIVER))
from inline_fixture import foreign_table_statement, normalize, query, scalar
from lake_harness import Blocked, validate_run_id
from native_test_context import OwnedDatabase, load_reference, reviewed_runtime_version


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-id', default=os.getenv('OG_LAKE_RUN_ID'))
    parser.add_argument('--output', default=os.getenv('OG_LAKE_RESULT_FILE'))
    parser.add_argument('--manifest', default=os.getenv('OG_LAKE_FIXTURE_MANIFEST'))
    args = parser.parse_args()
    if not args.run_id or not args.output or not args.manifest:
        parser.error('--run-id, --output and --manifest are required')
    validate_run_id(args.run_id)
    output = Path(args.output).resolve(); output.parent.mkdir(parents=True, exist_ok=True)
    result = {'status': 'fail', 'scope': 'N4/modifying_cte_admission', 'assertions': [],
              'observations': [], 'artifacts': [], 'notes': [
                  'Real host parser/planner/executor only; no mocked hasModifyingCTE or result-relation list.',
                  'Every lake probing transaction rolls back even on an unpatched binary.',
                  'Pure-heap modifying CTE remains supported; ordinary read-only lake CTE remains supported.']}
    owned = None

    def check(label, actual, expected=True, fatal=True):
        ok = actual == expected
        result['assertions'].append({'name': label, 'status': 'pass' if ok else 'fail',
                                     'details': {'observed': actual, 'expected': expected}})
        if not ok and fatal:
            raise AssertionError(label)
        return ok

    try:
        import psycopg2
        from psycopg2 import sql
        owned = OwnedDatabase(args.run_id, output.parent)
        with owned as db:
            a, observer = db.connect(), db.connect()
            query(a, "CREATE EXTENSION og_lake VERSION '0.4'")
            caps = json.loads(scalar(a, 'SELECT og_lake.host_status()'))
            runtime_version=reviewed_runtime_version()
            check('reviewed runtime supports existing N4',runtime_version in ('0.4','0.5','0.6'),True)
            check('runtime matches reviewed source',caps['module_version'],runtime_version)
            check('legacy CTE admission keeps SQL 0.4',
                  scalar(a,"SELECT extversion FROM pg_extension WHERE extname='og_lake'"),'0.4')
            result['runtime_version']=runtime_version
            result['installed_extension_version']='0.4'
            manifest, exported, counts, artifacts = load_reference(a, args.manifest, args.run_id, output.parent)
            result['artifacts'].extend(artifacts)
            check('actual reference I profile', manifest['case'], 'I')
            check('actual reference schema', manifest['schema'], [{'name':'id','type':'INTEGER'},{'name':'val','type':'INTEGER'}])
            check('all standard reference tables imported', len(counts), len(exported['tables']))
            query(a, 'CREATE SCHEMA biz')
            query(a, "SELECT og_lake.attach_catalog('lake_reference','lake_metadata',NULL::text,false)")
            query(a, foreign_table_statement('biz','orders','lake_reference',manifest['table_id'],manifest['schema']))
            query(a, 'CREATE TABLE biz.heap_source(id integer PRIMARY KEY,val integer)')
            query(a, 'INSERT INTO biz.heap_source SELECT id,val FROM biz.orders')
            query(a, 'CREATE TABLE biz.heap_sink(id integer PRIMARY KEY,val integer)')
            query(a, 'CREATE TABLE biz.heap_marker(id integer PRIMARY KEY)')
            original = query(observer, 'SELECT id,val FROM biz.orders ORDER BY id')
            check('genuine I values match the test oracle', original, [(1,10),(2,100),(3,200)])

            def image():
                state = {}
                for table in exported['tables']:
                    cols = table['columns']
                    rows = query(observer, sql.SQL('SELECT {} FROM lake_metadata.{}').format(
                        sql.SQL(',').join(sql.Identifier(c['column_name']) for c in cols),
                        sql.Identifier(table['table_name'])))
                    state[table['table_name']] = sorted(json.dumps(
                        [normalize(v,c['data_type']) for v,c in zip(row,cols)],
                        ensure_ascii=False,separators=(',',':')) for row in rows)
                return state

            baseline = image()
            children = [
                ('UPDATE', 'UPDATE {table} SET val=val+1 WHERE id=1 RETURNING id,val', [(1,11)]),
                ('DELETE', 'DELETE FROM {table} WHERE id=1 RETURNING id,val', [(1,10)]),
                ('INSERT', 'INSERT INTO {table} VALUES(7001,7002) RETURNING id,val', [(7001,7002)]),
            ]

            def parent_sql(parent, child):
                if parent == 'SELECT':
                    return 'WITH changed AS ('+child+') SELECT id,val FROM changed'
                return 'WITH changed AS ('+child+') INSERT INTO biz.heap_sink SELECT id,val FROM changed RETURNING id,val'

            # These are positive controls, not assertions about hypothetical
            # syntax support. A parser/runtime limitation fails visibly here.
            for parent in ('SELECT','INSERT'):
                for operation, template, expected in children:
                    label = 'heap child '+operation+' under parent '+parent
                    statement = parent_sql(parent, template.format(table='biz.heap_source'))
                    query(a,'BEGIN')
                    try:
                        result['observations'].append({'kind':'heap_control','sql':statement,
                            'plan':'\n'.join(row[0] for row in query(a,'EXPLAIN '+statement))})
                        check(label+' real RETURNING values', query(a,statement), expected)
                        check(label+' parent sink effects', query(a,'SELECT id,val FROM biz.heap_sink ORDER BY id'),
                              expected if parent=='INSERT' else [])
                        if operation == 'UPDATE': wanted=[(1,11),(2,100),(3,200)]
                        elif operation == 'DELETE': wanted=[(2,100),(3,200)]
                        else: wanted=original+[(7001,7002)]
                        check(label+' real child effects', query(a,'SELECT id,val FROM biz.heap_source ORDER BY id'), wanted)
                    finally:
                        query(a,'ROLLBACK')
                    check(label+' rollback restores ordinary heap', query(observer,'SELECT id,val FROM biz.heap_source ORDER BY id'), original)
                    check(label+' changes no lake protocol record', image(), baseline)

            bad = False
            cases = [(parent, operation, parent_sql(parent, template.format(table='biz.orders')))
                     for parent in ('SELECT','INSERT') for operation,template,_ in children]
            cases += [
                ('SELECT','UPDATE FALSE','WITH changed AS (UPDATE biz.orders SET val=0 WHERE false RETURNING id,val) SELECT * FROM changed'),
                ('SELECT','DELETE FALSE','WITH changed AS (DELETE FROM biz.orders WHERE false RETURNING id,val) SELECT * FROM changed'),
                ('SELECT','INSERT EMPTY','WITH changed AS (INSERT INTO biz.orders SELECT 8,8 WHERE false RETURNING id,val) SELECT * FROM changed'),
                ('SELECT','UNREFERENCED UPDATE','WITH changed AS (UPDATE biz.orders SET val=val+1 WHERE id=1 RETURNING id,val) SELECT 42'),
            ]
            for number,(parent,operation,statement) in enumerate(cases):
                label = 'lake child '+operation+' under parent '+parent
                query(a,'BEGIN')
                query(a,'INSERT INTO biz.heap_marker VALUES(%s)',(number,))
                observation={'kind':'lake_guard','parent':parent,'child':operation,'sql':statement}
                try:
                    try:
                        values=query(a,statement)
                    except psycopg2.Error as exc:
                        observation.update(outcome='rejected',sqlstate=exc.pgcode,message=str(exc))
                        ok=check(label+' rejected with 0A000',exc.pgcode,'0A000',fatal=False)
                        # Assert the module's actual broad-scope admission gate,
                        # not an incidental syntax error or unrelated permission.
                        at_gate=('no modifying CTE' in str(exc))
                        ok=check(label+' actual lake admission gate',at_gate,True,fatal=False) and ok
                    else:
                        observation.update(outcome='unexpectedly_accepted',returned=values)
                        ok=check(label+' must be refused before execution','accepted','0A000',fatal=False)
                    bad = not ok or bad
                    result['observations'].append(observation)
                finally:
                    query(a,'ROLLBACK')
                check(label+' rolls back prior ordinary heap marker',scalar(observer,'SELECT count(*) FROM biz.heap_marker'),0)
                check(label+' leaves parent heap sink unchanged',scalar(observer,'SELECT count(*) FROM biz.heap_sink'),0)
                check(label+' leaves complete standard catalog/inline unchanged',image(),baseline)
                check(label+' leaves business values unchanged',query(observer,'SELECT id,val FROM biz.orders ORDER BY id'),original)
                check(label+' same connection recovered',scalar(a,'SELECT 42'),42)

            check('ordinary read-only CTE over a lake table stays allowed',
                  query(a,'WITH selected AS (SELECT id,val FROM biz.orders) SELECT id,val FROM selected ORDER BY id'),original)
            query(a,'BEGIN')
            try:
                check('ordinary top-level lake UPDATE remains supported',
                      query(a,'UPDATE biz.orders SET val=val+1 WHERE id=1 RETURNING id,val'),[(1,11)])
            finally:
                query(a,'ROLLBACK')
            check('final protocol state is exact baseline',image(),baseline)
            if bad:
                raise AssertionError('One or more unsupported lake modifying CTE shapes passed admission or failed at the wrong gate')
            result['status']='pass'
    except Blocked as exc:
        result.update(status='blocked',reason=str(exc))
    except BaseException as exc:
        result.update(status='fail',reason=str(exc),traceback=traceback.format_exc())
    finally:
        if owned is not None:
            result['artifacts'].extend(owned.artifacts())
            result['database_cleanup']=owned.cleanup_result
        output.write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
        print(json.dumps({'status':result['status'],'assertions':len(result['assertions']),'result':str(output)}))
    return {'pass':0,'fail':1,'blocked':2}[result['status']]


if __name__=='__main__':
    sys.exit(main())
