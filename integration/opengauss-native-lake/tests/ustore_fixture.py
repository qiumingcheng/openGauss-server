"""Test-only physical carrier adapter; standard values remain reference-owned."""
from inline_fixture import TYPE_SQL, insert_rows, query, verify_loaded_rows


def import_catalog_ustore(conn, schema, exported):
    """Same reviewed export contract as import_catalog; only carrier AM differs.

    The caller must obtain exported via checked_export/export_reference first.
    Do not infer carriers by a name prefix: use the actual standard registry.
    No identifiers, snapshots, values, statistics, or logical types are changed.
    """
    from psycopg2 import sql
    registries = [t for t in exported['tables'] if t['table_name'] == 'ducklake_inlined_data_tables']
    if len(registries) != 1:
        raise ValueError('Expected one actual inline carrier registry')
    carriers = {row['table_name'] for row in registries[0]['rows']}
    names = {table['table_name'] for table in exported['tables']}
    if not carriers or not carriers.issubset(names):
        raise ValueError('Actual carrier relations must be present in the real export')
    query(conn, sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(schema)))
    for table in exported['tables']:
        definitions = []
        for column in table['columns']:
            definitions.append(sql.SQL('{} {}{}').format(
                sql.Identifier(column['column_name']), sql.SQL(TYPE_SQL[column['data_type']]),
                sql.SQL('' if column['is_nullable'] else ' NOT NULL')))
        for constraint in table['constraints']:
            kind = constraint['constraint_type']
            if kind == 'NOT NULL':
                continue
            if kind not in ('PRIMARY KEY', 'UNIQUE'):
                raise ValueError('Unreviewed physical constraint')
            definitions.append(sql.SQL('{} ({})').format(sql.SQL(kind), sql.SQL(',').join(
                map(sql.Identifier, constraint['constraint_column_names']))))
        suffix = sql.SQL(' WITH (storage_type=ustore)') if table['table_name'] in carriers else sql.SQL('')
        query(conn, sql.SQL('CREATE TABLE {}.{} ({}){}').format(sql.Identifier(schema),
            sql.Identifier(table['table_name']), sql.SQL(',').join(definitions), suffix))
        insert_rows(conn, schema, table)
    counts = verify_loaded_rows(conn, schema, exported)
    for carrier in sorted(carriers):
        options = query(conn, 'SELECT c.reloptions FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace '
                        'WHERE n.nspname=%s AND c.relname=%s', (schema, carrier))
        if len(options) != 1 or 'storage_type=ustore' not in [str(x).lower() for x in options[0][0] or []]:
            raise AssertionError('Host did not actually create a Ustore carrier: ' + carrier)
    return counts
