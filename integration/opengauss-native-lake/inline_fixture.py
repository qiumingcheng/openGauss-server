"""Explicit DuckDB-catalog to openGauss test import; never invent protocol rows.

This is a test adapter for the reference export, not an external-writer connector.
Only physical SQL DDL spelling and the enclosing host schema are adapted.
"""
from collections import Counter
import datetime
import json
from pathlib import Path
import subprocess
import uuid

from lake_harness import Blocked, read_json, sha256


TYPE_SQL = {
    'BOOLEAN': 'boolean', 'INTEGER': 'integer', 'BIGINT': 'bigint',
    'VARCHAR': 'text', 'UUID': 'uuid', 'TIMESTAMP WITH TIME ZONE': 'timestamptz',
    'TIMESTAMP': 'timestamp', 'DOUBLE': 'double precision', 'FLOAT': 'real',
}
BUSINESS_TYPES = {key: TYPE_SQL[key] for key in ('BOOLEAN', 'INTEGER', 'BIGINT', 'VARCHAR')}


def query(conn, statement, values=None):
    with conn.cursor() as cursor:
        cursor.execute(statement, values)
        return cursor.fetchall() if cursor.description else None


def scalar(conn, statement, values=None):
    return query(conn, statement, values)[0][0]


def normalize(value, type_name):
    if value is None:
        return None
    if type_name == 'UUID':
        return str(value).lower()
    if type_name.startswith('TIMESTAMP'):
        if isinstance(value, str):
            value = datetime.datetime.fromisoformat(value.replace('Z', '+00:00'))
        if type_name == 'TIMESTAMP WITH TIME ZONE':
            if value.tzinfo is None:
                raise ValueError('Reference timestamptz value lacks a timezone')
            value = value.astimezone(datetime.timezone.utc)
        return value.isoformat()
    return value


def multiset(rows):
    return Counter(json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(',', ':')) for row in rows)


def checked_export(path, manifest_path, driver):
    """Verify immutable provenance before executing any import DDL."""
    path, manifest_path, driver = Path(path), Path(manifest_path), Path(driver)
    data, manifest = read_json(path), read_json(manifest_path)
    if data.get('scope') != 'actual_reference_catalog_export_not_host_compatibility':
        raise ValueError('Input is not the real reference exporter format')
    if Path(data['source_manifest']).resolve() != manifest_path.resolve():
        raise ValueError('Export points to another reference manifest')
    if data['source_manifest_sha256'] != sha256(manifest_path):
        raise ValueError('Export source manifest changed')
    if data['source_lock_hash'] != sha256(driver / 'source-lock.json'):
        raise Blocked('Reference export source lock is stale')
    if data['format_version'] != '1.1' or manifest['format_version'] != '1.1':
        raise ValueError('Only actual format 1.1 exports are accepted')
    if data['binary_sha256'] != manifest['binary_sha256']:
        raise ValueError('Export uses another reference binary')
    for item in manifest['files']:
        if not Path(item['path']).is_file() or sha256(item['path']) != item['sha256']:
            raise ValueError('A real reference artifact changed after fixture generation')
    catalog_hashes = {sha256(manifest['catalog'])}
    for state in manifest.get('catalog_states', []):
        if not Path(state['catalog']).is_file() or sha256(state['catalog']) != state['path_sha256']:
            raise ValueError('A historical reference catalog copy changed')
        catalog_hashes.add(state['path_sha256'])
    if data['catalog_sha256'] not in catalog_hashes:
        raise ValueError('Export catalog is not a recorded reference artifact')
    if not data.get('tables'):
        raise ValueError('Reference export has no physical tables')
    source_schemas = {table['schema_name'] for table in data['tables']}
    if len(source_schemas) != 1:
        raise Blocked('Test import currently requires one metadata schema per reference catalog')
    names = set()
    for table in data['tables']:
        if table['table_name'] in names:
            raise ValueError('Duplicate physical table in export')
        names.add(table['table_name'])
        if not table['table_name'].startswith('ducklake_'):
            raise Blocked('Reference export contains a table outside the supported catalog namespace')
        columns = table['columns']
        if not columns or len({column['column_name'] for column in columns}) != len(columns):
            raise ValueError('Missing or duplicate physical columns')
        for column in columns:
            if column['data_type'] not in TYPE_SQL:
                raise Blocked('No explicit host export type adapter for ' + column['data_type'])
            if column.get('column_default') is not None:
                raise Blocked('Export has an unreviewed physical column default')
            if not isinstance(column['is_nullable'], bool):
                raise ValueError('Physical nullability must be explicit boolean')
        column_names = {column['column_name'] for column in columns}
        for row in table['rows']:
            if set(row) != column_names:
                raise ValueError('Physical export row differs from declared column set')
        for constraint in table['constraints']:
            if constraint['constraint_type'] not in ('NOT NULL', 'PRIMARY KEY', 'UNIQUE'):
                raise Blocked('Unreviewed physical constraint type: ' + constraint['constraint_type'])
            if any(name not in column_names for name in constraint['constraint_column_names']):
                raise ValueError('Constraint references an unknown physical column')
    required = {'ducklake_metadata', 'ducklake_snapshot', 'ducklake_table', 'ducklake_column',
                'ducklake_table_stats', 'ducklake_inlined_data_tables', 'ducklake_data_file', 'ducklake_delete_file'}
    if not required.issubset(names):
        raise ValueError('Reference export lacks required standard catalog tables')
    return data, manifest


def export_reference(driver, manifest_path, output, run_id, state=None):
    command = [str(Path(driver) / 'reference.sh'), 'export', '--manifest', str(Path(manifest_path).resolve()),
               '--output', str(Path(output).resolve()), '--run-id', run_id[:70] + '-export-' + uuid.uuid4().hex[:10]]
    if state:
        command += ['--state', state]
    log_path = Path(output).with_suffix('.log')
    with log_path.open('w') as log:
        response = subprocess.run(command, stdout=log, stderr=subprocess.STDOUT, timeout=180)
    if response.returncode == 2:
        raise Blocked('Real reference export prerequisite is unavailable: ' + str(log_path))
    if response.returncode:
        raise RuntimeError('Real reference export failed: ' + str(log_path))
    return checked_export(output, manifest_path, driver)


def insert_rows(conn, schema, table):
    from psycopg2 import sql
    columns = [column['column_name'] for column in table['columns']]
    statement = sql.SQL('INSERT INTO {}.{} ({}) VALUES ({})').format(
        sql.Identifier(schema), sql.Identifier(table['table_name']),
        sql.SQL(',').join(map(sql.Identifier, columns)),
        sql.SQL(',').join(sql.Placeholder() for unused in columns))
    with conn.cursor() as cursor:
        if table['rows']:
            cursor.executemany(statement, [[row[name] for name in columns] for row in table['rows']])


def verify_loaded_rows(conn, schema, exported):
    from psycopg2 import sql
    counts = {}
    for table in exported['tables']:
        columns = table['columns']
        rows = query(conn, sql.SQL('SELECT {} FROM {}.{}').format(
            sql.SQL(',').join(sql.Identifier(column['column_name']) for column in columns),
            sql.Identifier(schema), sql.Identifier(table['table_name'])))
        actual = [[normalize(value, column['data_type']) for value, column in zip(row, columns)] for row in rows]
        expected = [[normalize(row[column['column_name']], column['data_type']) for column in columns]
                    for row in table['rows']]
        if multiset(actual) != multiset(expected):
            raise AssertionError('Imported protocol values changed in ' + table['table_name'])
        counts[table['table_name']] = len(rows)
    return counts


def import_catalog(conn, schema, exported):
    from psycopg2 import sql
    query(conn, sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(schema)))
    for table in exported['tables']:
        definitions = []
        for column in table['columns']:
            definitions.append(sql.SQL('{} {}{}').format(sql.Identifier(column['column_name']),
                sql.SQL(TYPE_SQL[column['data_type']]), sql.SQL('' if column['is_nullable'] else ' NOT NULL')))
        for constraint in table['constraints']:
            kind = constraint['constraint_type']
            if kind == 'NOT NULL':
                continue
            definitions.append(sql.SQL('{} ({})').format(sql.SQL(kind),
                sql.SQL(',').join(map(sql.Identifier, constraint['constraint_column_names']))))
        query(conn, sql.SQL('CREATE TABLE {}.{} ({})').format(
            sql.Identifier(schema), sql.Identifier(table['table_name']), sql.SQL(',').join(definitions)))
        insert_rows(conn, schema, table)
    return verify_loaded_rows(conn, schema, exported)


def replace_catalog_values(conn, schema, before, after):
    """Replay a real second catalog state inside the caller's host transaction.

    No snapshot IDs, row IDs or version bounds are computed here. This tests
    native host MVCC; it is not a concurrent external-DuckDB write adapter.
    """
    from psycopg2 import sql
    identity = lambda exported: {table['table_name']: (table['columns'], table['constraints'])
                                 for table in exported['tables']}
    if identity(before) != identity(after):
        raise Blocked('MVCC replay requires identical physical table shapes in both real reference states')
    for table in after['tables']:
        query(conn, sql.SQL('DELETE FROM {}.{}').format(sql.Identifier(schema), sql.Identifier(table['table_name'])))
        insert_rows(conn, schema, table)
    return verify_loaded_rows(conn, schema, after)


def foreign_table_statement(schema, table_name, server, table_id, logical_schema):
    from psycopg2 import sql
    definitions = []
    for column in logical_schema:
        if column['type'] not in BUSINESS_TYPES:
            raise Blocked('No native business type support for ' + column['type'])
        definitions.append(sql.SQL('{} {}').format(sql.Identifier(column['name']), sql.SQL(BUSINESS_TYPES[column['type']])))
    return sql.SQL("CREATE FOREIGN TABLE {}.{} ({}) SERVER {} OPTIONS (binding_mode 'attach', table_id {})").format(
        sql.Identifier(schema), sql.Identifier(table_name), sql.SQL(',').join(definitions),
        sql.Identifier(server), sql.Literal(str(table_id)))
