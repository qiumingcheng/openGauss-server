#!/usr/bin/env python3
"""LOC-01/LOC-02: exercise production C++ codec/binding checks and marker admission.

Python mutates bytes emitted by the actual C++ encoder. It does not implement
another decoder/validator. The diagnostic seed is a runtime fixture, not proof
that its source row exists; real ForeignModify tests check version ownership.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import secrets
import sys
import traceback

WORKSPACE = Path('/home/qiumc/data/code/cpp')
DRIVER = Path(os.getenv('OG_LAKE_DRIVER_ROOT', str(WORKSPACE / 'integration/opengauss-native-lake')))
sys.path.insert(0, str(DRIVER))
from inline_fixture import foreign_table_statement, normalize, query, scalar
from lake_harness import Blocked, validate_run_id
from native_test_context import OwnedDatabase, load_reference, reviewed_runtime_version


# These are explicit wire-layout mutations, not an independent codec oracle.
def changed(blob, offset, width, value):
    result = bytearray(blob)
    result[offset:offset + width] = value.to_bytes(width, 'little')
    return bytes(result)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-id', default=os.getenv('OG_LAKE_RUN_ID'))
    parser.add_argument('--output', default=os.getenv('OG_LAKE_RESULT_FILE'))
    parser.add_argument('--manifest', default=os.getenv('OG_LAKE_FIXTURE_MANIFEST'))
    args = parser.parse_args()
    if not args.run_id or not args.output:
        parser.error('--run-id and --output are required')
    validate_run_id(args.run_id)
    output = Path(args.output).resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    result = {'status': 'fail', 'scope': 'N4/LOC-01_LOC-02_native_codec_marker',
              'assertions': [], 'artifacts': [], 'notes': [
                  'All validation decisions execute the production C++ decoder and binding validator.',
                  'A runtime diagnostic seed is not a source-row existence/ownership proof.',
                  'No manual snapshot or DuckLake protocol record is fabricated.',
                  'Real DML coverage uses administrator pure-inline access with installed SQL 0.4.']}
    owned = None

    def report_value(value):
        if isinstance(value, (bytes, bytearray, memoryview)):
            return {'bytea_hex': bytes(value).hex()}
        if isinstance(value, (list, tuple)):
            return [report_value(item) for item in value]
        if isinstance(value, dict):
            return {key: report_value(item) for key, item in value.items()}
        return value

    def check(name, observed, expected=True):
        passed = observed == expected
        result['assertions'].append({'name': name, 'status': 'pass' if passed else 'fail',
                                     'details': {'observed': report_value(observed), 'expected': report_value(expected)}})
        if not passed:
            raise AssertionError(name)

    try:
        if not args.manifest:
            raise Blocked('A verified real I reference fixture is required')
        import psycopg2
        from psycopg2 import sql
        owned = OwnedDatabase(args.run_id, output.parent)
        with owned as db:
            a, observer = db.connect(), db.connect()
            query(a, "CREATE EXTENSION og_lake VERSION '0.4'")
            capabilities = json.loads(scalar(a, 'SELECT og_lake.host_status()'))
            runtime_version=reviewed_runtime_version()
            check('reviewed runtime supports existing locator codec',runtime_version in ('0.4','0.5','0.6','0.7', '0.8', '0.9', '0.10', '0.11'),True)
            check('runtime matches reviewed source',capabilities['module_version'],runtime_version)
            check('locator codec keeps installed SQL 0.4',
                  scalar(a,"SELECT extversion FROM pg_extension WHERE extname='og_lake'"),'0.4')
            result['runtime_version']=runtime_version
            result['installed_extension_version']='0.4'
            manifest, exported, counts, artifacts = load_reference(a, args.manifest, args.run_id, output.parent)
            result['artifacts'].extend(artifacts)
            check('reference profile is I', manifest['case'], 'I')
            check('all reference tables imported', len(counts), len(exported['tables']))
            query(a, 'CREATE SCHEMA biz')
            query(a, "SELECT og_lake.attach_catalog('lake_reference','lake_metadata',NULL::text,false)")
            query(a, foreign_table_statement('biz', 'orders', 'lake_reference', manifest['table_id'], manifest['schema']))
            query(a, 'CREATE TABLE biz.heap_marker(id integer PRIMARY KEY)')

            # Test registration is transactional and local to this owned database;
            # it is deliberately absent from the normal extension SQL API.
            query(a, 'BEGIN')
            try:
                query(a, 'CREATE SCHEMA locator_probe')
                query(a, 'REVOKE ALL ON SCHEMA locator_probe FROM PUBLIC')
                try:
                    query(a, "CREATE FUNCTION locator_probe.check_locator(regclass,bytea) RETURNS bytea "
                             "AS '$libdir/og_lake','og_lake_test_locator' LANGUAGE C VOLATILE NOT FENCED")
                except psycopg2.Error as exc:
                    if exc.pgcode == '42883' and 'unknown statically linked' in str(exc):
                        raise Blocked('The reviewed actual C++ locator diagnostic has not been built/installed') from exc
                    raise
                query(a, 'REVOKE ALL ON FUNCTION locator_probe.check_locator(regclass,bytea) FROM PUBLIC')
                query(a, 'COMMIT')
            except BaseException:
                query(a, 'ROLLBACK')
                raise
            check('test function is non-STRICT so NULL reaches the C++ seed branch',
                  scalar(a, "SELECT proisstrict FROM pg_proc WHERE oid='locator_probe.check_locator(regclass,bytea)'::regprocedure"), False)

            def image():
                state = {}
                for table in exported['tables']:
                    columns = table['columns']
                    rows = query(observer, sql.SQL('SELECT {} FROM lake_metadata.{}').format(
                        sql.SQL(',').join(sql.Identifier(c['column_name']) for c in columns),
                        sql.Identifier(table['table_name'])))
                    state[table['table_name']] = sorted(json.dumps(
                        [normalize(value, column['data_type']) for value, column in zip(row, columns)],
                        ensure_ascii=False, separators=(',', ':')) for row in rows)
                return state

            baseline = image()
            before_rows = query(observer, 'SELECT id,val FROM biz.orders ORDER BY id')

            def native(blob):
                param = None if blob is None else psycopg2.Binary(blob)
                return bytes(scalar(a, 'SELECT locator_probe.check_locator(%s::regclass,%s::bytea)', ('biz.orders', param)))

            def reject(label, statement, state, parameters=None, message=None):
                query(a, 'SAVEPOINT locator_bad_input')
                try:
                    query(a, statement, parameters)
                except psycopg2.Error as exc:
                    check(label + ' SQLSTATE', exc.pgcode, state)
                    if message:
                        check(label + ' actual rejection site', message in str(exc))
                else:
                    check(label + ' must fail', 'accepted', state)
                finally:
                    query(a, 'ROLLBACK TO SAVEPOINT locator_bad_input')
                    query(a, 'RELEASE SAVEPOINT locator_bad_input')
                check(label + ' connection recovered', scalar(a, 'SELECT 6*7'), 42)

            query(a, 'BEGIN')
            seed = native(None)
            check('C++ encoder returns the reviewed 112-byte payload', len(seed), 112)
            check('C++ row_id=0 seed round-trips under the real transaction binding', native(seed), seed)
            check('seed uses zero row identity bytes', seed[40:48].hex(), '0000000000000000')
            check('actual wire version matches reviewed runtime',int.from_bytes(seed[4:6],'little'),2 if runtime_version in ('0.7','0.8', '0.9', '0.10', '0.11') else 1)
            check('actual permanent object tag',seed[9],1 if runtime_version in ('0.7','0.8', '0.9', '0.10', '0.11') else 0)
            result['seed_sha256'] = hashlib.sha256(seed).hexdigest()
            result['seed_layout_scope'] = 'synthetic runtime locator; real binding/nonce; no existence oracle'
            # A heap-stored bytea exercises packed/short-varlena input as well as
            # libpq parameters. No standard lake metadata/data is inserted.
            query(a, 'CREATE TABLE locator_probe.payload(value bytea)')
            query(a, 'INSERT INTO locator_probe.payload VALUES (%s)', (psycopg2.Binary(seed),))
            check('stored bytea runs through the actual detoast-aware decoder',
                  bytes(scalar(a, "SELECT locator_probe.check_locator('biz.orders'::regclass,value) FROM locator_probe.payload")), seed)

            corrupt = []
            for length in (0, 1, 7, 111, 113, 512):
                blob = seed[:length] if length < len(seed) else seed + bytes(length - len(seed))
                corrupt.append(('length ' + str(length), blob, '22P03', 'locator header'))
            if runtime_version in ('0.7','0.8', '0.9', '0.10', '0.11'):
                corrupt.append(('prior V1 is explicitly rejected',changed(seed,4,2,1),'22P03','locator header'))
            corrupt.extend([
                ('magic', changed(seed, 0, 1, ord('X')), '22P03', 'locator header'),
                ('unknown version', changed(seed, 4, 2, 65535), '22P03', 'locator header'),
                ('embedded length', changed(seed, 6, 2, 113), '22P03', 'locator header'),
                ('unknown object kind', changed(seed, 9, 1, 255), '22P03', 'locator header'),
                ('reserved header', changed(seed, 10, 2, 1), '22P03', 'locator header'),
                ('reserved trailer', changed(seed, 100, 4, 1), '22P03', 'locator header'),
                ('source zero', changed(seed, 8, 1, 0), '22P03', 'locator fields'),
                ('unknown source', changed(seed, 8, 1, 4), '22P03', 'locator fields'),
                ('row_id high bit', changed(seed, 40, 8, 1 << 63), '22P03', 'row ID is out of range'),
                ('negative row_id encoding', changed(seed, 40, 8, (1 << 64) - 1), '22P03', 'row ID is out of range'),
                ('zero nonce', changed(seed, 88, 8, 0), '22P03', 'locator fields'),
                ('invalid command ID', changed(seed, 96, 4, (1 << 32) - 1), '22P03', 'locator fields'),
                ('INLINE mixed file field', changed(seed, 64, 8, 1), '22P03', 'inline locator'),
                ('INLINE mixed local token', changed(seed, 104, 8, 1), '22P03', 'inline locator'),
                ('leaf partition unsupported', changed(seed, 84, 4, 1), '0A000', 'partitioned targets are not enabled'if runtime_version in ('0.10','0.11')else'nonpartitioned inline rows only'),
            ])
            # FILE must have shape-correct fields so rejection proves the policy
            # gate, not a malformed INLINE field caught earlier.
            file_blob = changed(changed(seed, 8, 1, 2), 80, 4, 0)
            corrupt.append(('shape-valid FILE source unsupported', file_blob, '0A000', 'file modification requires extension 0.10'if runtime_version in ('0.10','0.11')else'nonpartitioned inline rows only'))
            for label, offset, width in [('database', 12, 4), ('relation', 16, 4), ('server', 20, 4),
                                          ('schema version', 24, 8), ('table ID', 32, 8),
                                          ('read snapshot', 56, 8), ('inline OID', 80, 4), ('nonce', 88, 8)]:
                current = int.from_bytes(seed[offset:offset + width], 'little')
                corrupt.append(('wrong ' + label, changed(seed, offset, width, current + 1),
                                '55000', 'does not match the active transaction binding'))
            corrupt.append(('future producer command', changed(seed, 96, 4, (1 << 32) - 2),
                            '55000', 'does not match the active transaction binding'))
            read_snapshot = int.from_bytes(seed[56:64], 'little')
            corrupt.append(('begin after read snapshot', changed(seed, 48, 8, read_snapshot + 1),
                            '22P03', 'locator fields'))
            for label, blob, state, message in corrupt:
                reject(label, 'SELECT locator_probe.check_locator(%s::regclass,%s::bytea)', state,
                       ('biz.orders', psycopg2.Binary(blob)), message)
                check(label + ' valid seed still accepted', native(seed), seed)
            reject('NULL target rejected', 'SELECT locator_probe.check_locator(NULL::regclass,NULL::bytea)', '22004')
            reject('ordinary heap target rejected',
                   "SELECT locator_probe.check_locator('biz.heap_marker'::regclass,NULL::bytea)", '42809')
            query(a, 'COMMIT')
            check('codec probes do not publish or change any standard record', image(), baseline)
            query(a, 'BEGIN')
            reject('actual prior transaction locator is stale',
                   'SELECT locator_probe.check_locator(%s::regclass,%s::bytea)', '55000',
                   ('biz.orders', psycopg2.Binary(seed)), 'does not match the active transaction binding')
            new_seed = native(None)
            check('new transaction obtains its own accepted C++ seed', native(new_seed), new_seed)
            check('new transaction nonce differs', new_seed[88:96] != seed[88:96])
            query(a, 'ROLLBACK')

            # Grant the test wrapper deliberately, then prove the C function's
            # own first-line superuser guard still rejects an ordinary role.
            role = 'lake_loc_' + secrets.token_hex(8)
            password = 'Og1!' + secrets.token_hex(12)
            query(a, 'BEGIN')
            try:
                query(a, sql.SQL('CREATE ROLE {} PASSWORD {}').format(sql.Identifier(role), sql.Literal(password)))
                check('wrapper has no implicit PUBLIC execution privilege',
                      scalar(a, 'SELECT has_function_privilege(%s,%s,%s)',
                             (role, 'locator_probe.check_locator(regclass,bytea)', 'EXECUTE')), False)
                query(a, sql.SQL('GRANT USAGE ON SCHEMA locator_probe TO {}').format(sql.Identifier(role)))
                query(a, sql.SQL('GRANT EXECUTE ON FUNCTION locator_probe.check_locator(regclass,bytea) TO {}').format(sql.Identifier(role)))
                query(a, sql.SQL('SET ROLE {} PASSWORD {}').format(sql.Identifier(role), sql.Literal(password)))
                # NULL target is intentional: privilege validation must happen
                # before target/binding checks, not be masked by schema ACLs.
                reject('C++ privilege check despite explicit SQL execute grant',
                       'SELECT locator_probe.check_locator(NULL::regclass,NULL::bytea)', '42501',
                       message='locator diagnostics require superuser privileges')
            finally:
                query(a, 'ROLLBACK')
                query(a, 'RESET ROLE')
            check('temporary ordinary role was transactionally removed',
                  scalar(observer, 'SELECT count(*) FROM pg_roles WHERE rolname=%s', (role,)), 0)

            # This is the real installed marker and real DML planning. Rollback
            # each catalog mutation and verify subsequent native UPDATE works.
            query(a, 'BEGIN')
            reject('marker cannot be called directly', 'SELECT og_lake_internal.row_locator(1)',
                   '0A000', message='must be supplied by a lake scan slot')
            reject('user marker in executable qual is not treated as a locator slot',
                   'UPDATE biz.orders SET val=val WHERE og_lake_internal.row_locator(orders) IS NOT NULL',
                   '0A000', message='locator was not rewritten to a scan slot')
            query(a, 'ROLLBACK')
            marker_attacks = [
                ('forged volatility', ['ALTER FUNCTION og_lake_internal.row_locator(anyelement) VOLATILE']),
                ('forged C entrypoint', ["CREATE OR REPLACE FUNCTION og_lake_internal.row_locator(anyelement) RETURNS bytea "
                                       "AS '$libdir/og_lake','og_lake_unavailable' LANGUAGE C STRICT STABLE NOT FENCED"]),
                # This host intentionally rejects ALTER EXTENSION ADD/DROP.
                # Rename the authentic member, then create an exact-name C
                # function with no extension dependency inside this transaction.
                ('forged extension ownership', [
                    'ALTER FUNCTION og_lake_internal.row_locator(anyelement) RENAME TO authentic_locator',
                    "CREATE FUNCTION og_lake_internal.row_locator(anyelement) RETURNS bytea "
                    "AS '$libdir/og_lake','og_lake_row_locator' LANGUAGE C STRICT STABLE NOT FENCED",
                    'REVOKE ALL ON FUNCTION og_lake_internal.row_locator(anyelement) FROM PUBLIC']),
            ]
            for label, mutations in marker_attacks:
                query(a, 'BEGIN')
                try:
                    for mutation in mutations:
                        query(a, mutation)
                    reject(label, 'UPDATE biz.orders SET val=val+1 WHERE id=1', '55000',
                           message='invalid native lake row locator marker binding')
                finally:
                    query(a, 'ROLLBACK')
                query(a, 'BEGIN')
                try:
                    check(label + ' rollback restores real planner/locator path',
                          query(a, 'UPDATE biz.orders SET val=val+1 WHERE id=1 RETURNING id,val'), [(1,11)])
                finally:
                    query(a, 'ROLLBACK')
                check(label + ' leaves all standard records unchanged', image(), baseline)
            check('all locator and marker tests preserve business values',
                  query(observer, 'SELECT id,val FROM biz.orders ORDER BY id'), before_rows)
            check('no standard protocol changes after all diagnostics', image(), baseline)
            query(a, 'DROP SCHEMA locator_probe CASCADE')
            check('test-only wrapper and schema are removed',
                  scalar(a, "SELECT count(*) FROM pg_namespace WHERE nspname='locator_probe'"), 0)
            result['status'] = 'pass'
    except Blocked as exc:
        result.update(status='blocked', reason=str(exc))
    except BaseException as exc:
        result.update(status='fail', reason=str(exc), traceback=traceback.format_exc())
    finally:
        if owned is not None:
            result['artifacts'].extend(owned.artifacts())
            result['database_cleanup'] = owned.cleanup_result
        output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n')
        print(json.dumps({'status': result['status'], 'assertions': len(result['assertions']), 'result': str(output)}))
    return {'pass': 0, 'fail': 1, 'blocked': 2}[result['status']]


if __name__ == '__main__':
    sys.exit(main())
