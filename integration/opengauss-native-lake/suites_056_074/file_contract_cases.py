"""SQL 0.74 external file and maintenance contracts on an owned test catalog."""
import json
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from pathlib import Path

import psycopg2


def external_files(cur, observer, root):
    files = root / 'external'
    files.mkdir()
    cur.execute("CREATE SERVER csv_files FOREIGN DATA WRAPPER og_lake_file_fdw OPTIONS (external_root %s)", (str(files),))
    describe = "SELECT column_name,column_type FROM og_lake.describe_external_file('csv_files',%s) ORDER BY physical_ordinal"
    (files / 'records.csv').write_bytes(b'id,note\r\n1,"one,\r\ntwo ""words"""\r\n2,plain\r\n')
    cur.execute(describe, ('records.csv',))
    assert cur.fetchall() == [('id', 'bigint'), ('note', 'text')]
    cur.execute("CREATE FOREIGN TABLE biz.csv_rows(id int,note text) SERVER csv_files OPTIONS(path 'records.csv', format 'csv', header 'true')")
    cur.execute('SELECT * FROM biz.csv_rows ORDER BY id')
    assert cur.fetchall() == [(1, 'one,\r\ntwo "words"'), (2, 'plain')]
    for filename, dates, kind in [('leap.csv', ['2000-02-29', '2024-02-29'], 'date'),
                                  ('mixed.csv', ['1900-02-29', '2024-02-29'], 'text')]:
        (files / filename).write_text('d\n' + '\n'.join(dates))
        cur.execute(describe, (filename,))
        assert cur.fetchall() == [('d', kind)]
    # Stop at the last complete record when the bounded sample ends inside a quoted field.
    (files / 'bounded.csv').write_text('id,note\n1,"ok"\n2,"' + ('a\n' * 40000) + '"\n')
    cur.execute(describe, ('bounded.csv',))
    assert cur.fetchall() == [('id', 'bigint'), ('note', 'text')]

    identity = 'SELECT pg_postmaster_start_time(),pg_backend_pid()'
    cur.execute(identity)
    writer_identity = cur.fetchone()
    observer.execute(identity)
    observer_identity = observer.fetchone()
    assert writer_identity[0] == observer_identity[0]
    try:
        cur.execute('SELECT 1 / (id - id) FROM biz.csv_rows')
    except psycopg2.Error as exc:
        assert exc.pgcode == '22012', exc
    else:
        raise AssertionError('expected division by zero during projection')
    # A typed reader reports ordinary input errors and both sessions stay usable.
    (files / 'typed.csv').write_text('id\n1\nunknown\n')
    cur.execute("CREATE FOREIGN TABLE biz.typed(id int) SERVER csv_files OPTIONS(path 'typed.csv',format 'csv',header 'true')")
    try:
        cur.execute('SELECT id FROM biz.typed ORDER BY id')
    except psycopg2.Error as exc:
        assert exc.pgcode == '22P02', exc
    else:
        raise AssertionError('typed CSV accepted an invalid integer')
    cur.execute('SELECT count(*) FROM biz.csv_rows')
    assert cur.fetchone() == (2,)
    cur.execute(identity)
    assert cur.fetchone() == writer_identity
    observer.execute(identity)
    assert observer.fetchone() == observer_identity

    (files / 'ordered.csv').write_text('id\n' + ''.join(f'{i}\n' for i in range(30000)))
    cur.execute("CREATE FOREIGN TABLE biz.ordered(id int) SERVER csv_files OPTIONS(path 'ordered.csv', format 'csv', header 'true')")
    cur.execute('SET default_statistics_target=10')
    (files / 'sorted.jsonl').write_text(''.join(json.dumps({'id': i}) + '\n' for i in range(30000)))
    cur.execute("CREATE FOREIGN TABLE biz.ordered_json(id int) SERVER csv_files OPTIONS(path 'sorted.jsonl',format 'jsonl')")
    cur.execute("COPY (SELECT generate_series(0,29999)::integer AS id) TO %s (FORMAT 'parquet')",
                (str(files / 'sorted.parquet'),))
    cur.execute("CREATE FOREIGN TABLE biz.ordered_parquet(id int) SERVER csv_files OPTIONS(path 'sorted.parquet',format 'parquet')")
    correlations = {}
    for table in ('ordered', 'ordered_json', 'ordered_parquet'):
        cur.execute('ANALYZE biz.' + table)
        cur.execute("SELECT correlation FROM pg_stats WHERE schemaname='biz' AND tablename=%s AND attname='id'", (table,))
        correlations[table], = cur.fetchone()
    assert all(value == 1.0 for value in correlations.values()), correlations

    value = {'multi': ['spaces in text', 'line\nbreak', 'slash\\quote"']}
    payload = json.dumps(value, indent=2)
    destination = files / 'result.jsonl'
    write = "COPY (SELECT 1 AS id,%s::json AS payload) TO %s (FORMAT 'jsonl')"
    cur.execute(write, (payload, str(destination)))
    output = destination.read_bytes()
    assert len(output.splitlines()) == 1 and json.loads(output)['payload'] == value
    try:
        observer.execute(write, ('{}', str(destination)))
    except psycopg2.Error as exc:
        assert exc.pgcode == '58P02', exc
    else:
        raise AssertionError('existing JSONL target accepted')
    assert destination.read_bytes() == output

    # Two normal exporters can share a directory. Exactly one owns this name.
    simultaneous = files / 'simultaneous.jsonl'
    start = Barrier(2, timeout=15)

    def export_marker(client, marker):
        start.wait()
        try:
            client.execute(write, (json.dumps(marker), str(simultaneous)))
            return marker
        except psycopg2.Error as exc:
            if exc.pgcode != '58P02':
                raise
            return None

    with ThreadPoolExecutor(max_workers=2) as pool:
        attempts = [pool.submit(export_marker, cur, 'first'), pool.submit(export_marker, observer, 'second')]
        outcomes = [attempt.result(timeout=30) for attempt in attempts]
    winners = [marker for marker in outcomes if marker is not None]
    assert len(winners) == 1 and json.loads(simultaneous.read_bytes())['payload'] == winners[0]

    nested = files / 'nested.jsonl'
    cur.execute("COPY (SELECT ARRAY[%s::json] AS items, ROW(%s::json) AS rec) TO %s (FORMAT 'jsonl')",
                (payload, payload, str(nested)))
    assert len(nested.read_bytes().splitlines()) == 1
    assert json.loads(nested.read_bytes()) == {'items': [value], 'rec': {'f1': value}}
    cur.execute('CREATE TABLE biz.readback(id integer,payload json)')
    cur.execute("COPY biz.readback FROM %s (FORMAT 'jsonl')", (str(destination),))
    cur.execute('SELECT id,payload::text FROM biz.readback')
    rows = cur.fetchall()
    assert len(rows) == 1 and rows[0][0] == 1 and json.loads(rows[0][1]) == value
    return {'correlations': correlations, 'csv_error_sqlstates': ['22012', '22P02'],
            'sessions_preserved': True, 'jsonl_rows': len(rows), 'concurrent_winners': len(winners)}


def managed_files(cur, observer, root):
    cur.execute("CREATE FOREIGN TABLE biz.files(id int,p int,val text) SERVER c_test OPTIONS (binding_mode 'create',lake_schema 'main')")

    def snapshot():
        cur.execute("SELECT id FROM og_lake.current_snapshot('c_test'::name)")
        return cur.fetchone()[0]

    def changes(lo, hi):
        cur.execute("""SELECT change_type,id FROM og_lake.table_changes('biz.files',%s::bigint,%s::bigint)
                       AS r(snapshot_id bigint,rowid bigint,change_type text,id int,p int,val text) ORDER BY id,change_type""", (lo, hi))
        return cur.fetchall()

    initial_snap = snapshot()
    for i in range(4):
        cur.execute('INSERT INTO biz.files VALUES (%s,0,%s)', (i, 'small-' + str(i)))
        cur.execute("SELECT og_lake.flush('biz.files')")
    cur.execute("""SELECT df.file_size_bytes FROM lake_metadata.ducklake_data_file df
        JOIN lake_metadata.ducklake_table t USING(table_id) WHERE t.table_name='files' AND df.end_snapshot IS NULL
        ORDER BY coalesce(df.file_order,df.data_file_id),df.data_file_id""")
    sizes = [row[0] for row in cur.fetchall()]
    assert len(sizes) == 4
    before = snapshot()
    cur.execute("SELECT * FROM og_lake.merge_adjacent_files('biz.files',16,%s)", (sum(sizes[:2]),))
    assert [row[2:] for row in cur.fetchall()] == [(2, 1)]
    merged = snapshot()
    assert merged > before and changes(merged, merged) == []
    assert changes(initial_snap, merged) == [('insert', i) for i in range(4)]
    cur.execute("""SELECT id FROM og_lake.time_travel('biz.files',%s::bigint)
                   AS r(id int,p int,val text) ORDER BY id""", (before,))
    assert cur.fetchall() == [(i,) for i in range(4)]
    original = [(i, 'small-' + str(i)) for i in range(4)]
    after_update = [(0, 'after-merge'), *original[1:]]
    current_rows = 'SELECT id,val FROM biz.files ORDER BY id'
    cur.execute('BEGIN')
    try:
        cur.execute('UPDATE biz.files SET val=%s WHERE id=0', ('after-merge',))
        cur.execute('SAVEPOINT before_delete')
        cur.execute('DELETE FROM biz.files WHERE id=1')
        cur.execute(current_rows)
        assert cur.fetchall() == [after_update[0], *after_update[2:]]
        observer.execute(current_rows)
        assert observer.fetchall() == original
        cur.execute('ROLLBACK TO SAVEPOINT before_delete')
        cur.execute(current_rows)
        assert cur.fetchall() == after_update
        cur.execute('COMMIT')
    except BaseException:
        cur.execute('ROLLBACK')
        raise
    observer.execute(current_rows)
    assert observer.fetchall() == after_update
    updated = snapshot()
    assert changes(updated, updated) == [('update_postimage', 0), ('update_preimage', 0)]
    cur.execute("SELECT og_lake.create_lake_index('biz.files','ix_f','{\"version\":1,\"kind\":\"scalar\",\"method\":\"btree\",\"column\":\"id\"}')")
    index_id, = cur.fetchone()
    cur.execute("SELECT og_lake.build_lake_index(%s::uuid)", (index_id,))
    cur.execute("SELECT segment_uuid,artifact_root_uri FROM og_lake_internal.index_segment WHERE index_uuid=%s AND state='READY' LIMIT 1", (index_id,))
    segment_id, artifact_path = cur.fetchone()
    artifact = Path(artifact_path)
    cur.execute("SELECT og_lake.create_lake_index('biz.files','ix_p',%s::jsonb)",
                (json.dumps(dict(version=1, kind='scalar', method='bitmap', column='p')),))
    bitmap_id, = cur.fetchone()
    cur.execute("SELECT og_lake.build_lake_index(%s::uuid)", (bitmap_id,))
    cur.execute("SELECT count(*) FROM og_lake_internal.index_segment WHERE index_uuid=%s AND state='READY' AND format_version=2", (bitmap_id,))
    assert cur.fetchone()[0] > 0
    member = artifact / 'page_data.parquet'
    assert (artifact / 'page_lookup.parquet').is_file()
    check = "SELECT object_kind,status,detail FROM og_lake.verify_catalog_integrity('c_test',false)"
    cur.execute(check)
    assert [(row[0], row[1]) for row in cur.fetchall()] == [('summary', 'healthy')]
    held = root / 'saved-page-data'
    member.rename(held)
    try:
        cur.execute(check)
        assert any(row[0] == 'index_segment' and member.name in row[2] for row in cur.fetchall())
    finally:
        held.rename(member)
    cur.execute(check)
    assert [(row[0], row[1]) for row in cur.fetchall()] == [('summary', 'healthy')]

    cur.execute('SELECT snapshot_id FROM lake_metadata.ducklake_snapshot WHERE snapshot_id<%s ORDER BY snapshot_id', (merged,))
    old = [row[0] for row in cur.fetchall()]
    cur.execute("SELECT snapshot_id FROM og_lake.expire_snapshots('c_test',%s::bigint[],NULL,false)", (old,))
    assert sorted(row[0] for row in cur.fetchall()) == old
    cur.execute('SELECT DISTINCT path FROM lake_metadata.ducklake_files_scheduled_for_deletion')
    queued = [root / 'cat' / row[0] for row in cur.fetchall()]
    assert len(queued) == 2
    physical = sum(path.stat().st_size for path in queued)
    retention = "SELECT expired_scheduled_files,scheduled_bytes FROM og_lake.retention_status('c_test')"
    cur.execute(retention)
    assert cur.fetchone() == (2, physical)
    held = root / 'saved-queued-file'
    queued[0].rename(held)
    try:
        cur.execute(retention)
        assert cur.fetchone() == (2, None)
    finally:
        held.rename(queued[0])
    cur.execute('SELECT count(*) FROM og_lake_internal.index_coverage WHERE segment_uuid=%s AND active', (segment_id,))
    assert cur.fetchone()[0] > 0
    held = root / 'saved-page-data'
    member.rename(held)
    try:
        cur.execute("SELECT object_kind,status,detail FROM og_lake.verify_catalog_integrity('c_test',true)")
        assert any(kind == 'index_segment' and status == 'repaired_marked_failed' and member.name in detail
                   for kind, status, detail in cur.fetchall())
        cur.execute('SELECT state FROM og_lake_internal.index_segment WHERE segment_uuid=%s', (segment_id,))
        assert cur.fetchone() == ('FAILED',)
        cur.execute('SELECT count(*) FROM og_lake_internal.index_coverage WHERE segment_uuid=%s AND active', (segment_id,))
        assert cur.fetchone() == (0,)
        cur.execute(current_rows)
        assert cur.fetchall() == after_update
    finally:
        held.rename(member)
    return {'original_data_files': len(sizes), 'scheduled_files': len(queued), 'scheduled_bytes': physical,
            'savepoint_and_visibility': True, 'btree_repair': True, 'bitmap_verified': True}
