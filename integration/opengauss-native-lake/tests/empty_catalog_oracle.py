"""Draft independent host-shape export and empty-catalog differential helpers.

Not executed. Does not initialize either engine or invent missing protocol data.
The test caller must verify reference/source-lock provenance before comparison.
"""
from collections import Counter
import json
import sys
from pathlib import Path
from empty_reference_fixture import DRIVER,check_empty_initial
sys.path.insert(0,str(DRIVER)); sys.path.insert(0,str(DRIVER/'tests'))
from inline_fixture import normalize,query,scalar
from lake_harness import write_json
from reverse_reference import literal,qi

OID_TYPES={16:'BOOLEAN',20:'BIGINT',21:'SMALLINT',23:'INTEGER',25:'VARCHAR',1043:'VARCHAR',2950:'UUID',1184:'TIMESTAMP WITH TIME ZONE',700:'FLOAT',701:'DOUBLE'}


def canonical_shape(table):
    columns=[]
    for index,column in enumerate(table['columns'],start=1):
        # Pinned duckdb_columns.cpp emits i+1; retain that observed ordinal.
        if column['column_index']!=index or column.get('column_default') is not None:
            raise AssertionError('Unexpected column index/default: '+table['table_name'])
        if type(column['is_nullable']) is not bool: raise AssertionError('Nullability must be explicit')
        columns.append((column['column_name'],column['data_type'],column['is_nullable']))
    keys=[]
    for item in table['constraints']:
        kind=item['constraint_type']
        if kind=='NOT NULL': continue # independently represented by actual column nullability
        if kind not in ('PRIMARY KEY','UNIQUE'): raise AssertionError('Unreviewed metadata constraint: '+kind)
        keys.append((kind,tuple(item['constraint_column_names'])))
    return {'columns':columns,'keys':sorted(keys)}


def read_native_catalog(conn,schema,reference_export,output):
    """Export actual PG definitions and rows; never copy reference column DDL."""
    from psycopg2 import sql
    from psycopg2.extensions import TRANSACTION_STATUS_IDLE
    if conn.get_transaction_status()!=TRANSACTION_STATUS_IDLE: raise ValueError('Native export requires idle observer; it never commits user work')
    tables=[]
    query(conn,'BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY')
    try:
        relations=query(conn,"SELECT c.oid,c.relname,c.relkind,c.relpersistence,c.relhassubclass "
            "FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
            "WHERE n.nspname=%s AND c.relkind IN ('r','f','v','m','p','S') ORDER BY c.relname",(schema,))
        wanted={t['table_name'] for t in reference_export['tables']}
        if len(relations)!=len(wanted) or {r[1] for r in relations}!=wanted: raise AssertionError('Native table/object set differs from real EMPTY')
        for oid,name,kind,persistence,children in relations:
            if (kind,persistence,children)!=('r','p',False): raise AssertionError('Native metadata is not an independent persistent heap relation')
            attrs=query(conn,'SELECT attnum,attname,atttypid,atttypmod,attnotnull,atthasdef,attisdropped '
                        'FROM pg_attribute WHERE attrelid=%s AND attnum>0 ORDER BY attnum',(oid,))
            columns=[]
            for index,(number,column,type_oid,typmod,notnull,hasdefault,dropped) in enumerate(attrs):
                if number!=index+1 or dropped or hasdefault or type_oid not in OID_TYPES or typmod!=-1:
                    raise AssertionError('Native physical column differs from supported standard contract: '+name+'.'+column)
                columns.append({'column_index':number,'column_name':column,'data_type':OID_TYPES[type_oid],
                                'is_nullable':not notnull,'column_default':None})
            constraints=[]
            for kind,key,deferrable,deferred,validated in query(conn,
                    'SELECT contype,conkey,condeferrable,condeferred,convalidated FROM pg_constraint WHERE conrelid=%s ORDER BY oid',(oid,)):
                if kind not in ('p','u') or deferrable or deferred or not validated:
                    raise AssertionError('Native metadata has a nonstandard/unchecked constraint')
                if not key or any(not 1<=int(k)<=len(columns) for k in key): raise AssertionError('Invalid native key column indexes')
                constraints.append({'constraint_type':'PRIMARY KEY' if kind=='p' else 'UNIQUE',
                    'constraint_column_names':[columns[int(k)-1]['column_name'] for k in key]})
            rows=query(conn,sql.SQL('SELECT {} FROM {}.{}').format(
                sql.SQL(',').join(sql.Identifier(c['column_name']) for c in columns),sql.Identifier(schema),sql.Identifier(name)))
            tables.append({'schema_name':schema,'table_name':name,'columns':columns,'constraints':constraints,
                'rows':[{c['column_name']:normalize(value,c['data_type']) for c,value in zip(columns,row)} for row in rows]})
        query(conn,'COMMIT')
    except BaseException:
        query(conn,'ROLLBACK'); raise
    exported={'scope':'actual_native_created_catalog_export','format_version':'1.1','tables':tables}
    write_json(output,exported)
    return exported


def compare_initialized(reference,native,data_root,native_created_by):
    """Only independent producer/time/schema UUID fields may differ here."""
    observed_reference=check_empty_initial(reference['tables'],data_root)
    observed_native=check_empty_initial(native['tables'],data_root)
    if observed_native['created_by']!=native_created_by: raise AssertionError('Native created_by must honestly identify the declared implementation')
    a={t['table_name']:t for t in reference['tables']}; b={t['table_name']:t for t in native['tables']}
    if set(a)!=set(b): raise AssertionError('Standard table set differs')
    for name in sorted(a):
        if canonical_shape(a[name])!=canonical_shape(b[name]): raise AssertionError('Actual physical DDL differs: '+name)
        def values(table):
            result=[]
            for raw in table['rows']:
                if set(raw)!={c['column_name'] for c in table['columns']}: raise AssertionError('Actual row has missing/extra fields')
                row={c['column_name']:normalize(raw[c['column_name']],c['data_type']) for c in table['columns']}
                if name=='ducklake_snapshot': row['snapshot_time']='<independent-tx-time>'
                elif name=='ducklake_schema': row['schema_uuid']='<independent-default-schema-uuid>'
                elif name=='ducklake_metadata' and row['key']=='created_by': row['value']='<declared-producer>'
                result.append(json.dumps(row,ensure_ascii=False,sort_keys=True,separators=(',',':')))
            return Counter(result)
        if values(a[name])!=values(b[name]): raise AssertionError('Initial standard rows differ: '+name)
    return {'independent_initialization':'equal_except_declared_time_uuid_producer',
            'reference_observations':observed_reference,'native_observations':observed_native}


def transport_native(ref,catalog,native):
    """Create only a test-owned offline transport from actual native definitions.

    Caller must first compare actual shapes to the genuine EMPTY oracle. This
    is not a product initializer or a reference-generated EMPTY fixture.
    """
    catalog=Path(catalog)
    if catalog.exists() or Path(str(catalog)+'.wal').exists(): raise ValueError('Transport destination exists')
    statements=['BEGIN;']
    for table in native['tables']:
        canonical_shape(table)
        full=qi('main')+'.'+qi(table['table_name'])
        definitions=[qi(c['column_name'])+' '+c['data_type']+(' NOT NULL' if not c['is_nullable'] else '') for c in table['columns']]
        definitions += [c['constraint_type']+' ('+','.join(map(qi,c['constraint_column_names']))+')'
                        for c in table['constraints'] if c['constraint_type']!='NOT NULL']
        statements.append('CREATE TABLE '+full+' ('+','.join(definitions)+');')
        for row in table['rows']:
            statements.append('INSERT INTO '+full+' ('+','.join(qi(c['column_name']) for c in table['columns'])+') VALUES ('+
                ','.join(literal(row[c['column_name']],c['data_type']) for c in table['columns'])+');')
    statements.append('COMMIT; CHECKPOINT;')
    ref.run('\n'.join(statements),'transport-exact-native-catalog',catalog)
    # No normalizer is used on this identity-preserving round trip: UUID,
    # snapshot time, producer and root must all retain their native values.
    for table in native['tables']:
        rows=ref.run('SELECT * FROM '+qi('main')+'.'+qi(table['table_name']),
            'check-native-transport-'+table['table_name'],catalog,True,True)
        def exact(values):
            return Counter(json.dumps({c['column_name']:normalize(r[c['column_name']],c['data_type'])
                for c in table['columns']},sort_keys=True,ensure_ascii=False,separators=(',',':')) for r in values)
        if exact(rows)!=exact(table['rows']): raise AssertionError('Native transport changed actual values: '+table['table_name'])
