"""SQL0.60 file/column evolution and delete exclusion in the suite's owned DB."""

from psycopg2 import sql

from run_060_suite import (
    CATALOG, META, TARGET, changes, files, flush_rows, make_table, merge,
    query, reject, rewrite, rows, scalar, snap, transaction, tt,
)

BASE = 'id integer, part integer, val text'
RENAMED = 'id integer, part integer, label text'
WIDE = RENAMED + ', score integer, note integer'
DROPPED = RENAMED + ', note integer'


def _table_id(conn, table):
    return scalar(conn, f'SELECT table_id FROM {META}.ducklake_table '
                  'WHERE table_name=%s AND end_snapshot IS NULL', (table,))


def _heads(conn):
    return snap(conn), snap(conn, last=True)


def _one_file(conn, table, expected_count, write_and_flush):
    before = files(conn, table)
    write_and_flush()
    active = files(conn, table)
    assert active[:-1] == before and len(active) == len(before) + 1, (table, before, active)
    result = active[-1]
    assert 0 < result[2] < TARGET and result[3] == expected_count, (table, result)
    return result


def _flush(conn, table):
    scalar(conn, 'SELECT og_lake.flush(%s::regclass)', (f'biz.{table}',))


def _partition_values(conn, table, descriptors):
    return query(conn, f'SELECT data_file_id, partition_key_index, partition_value '
                 f'FROM {META}.ducklake_file_partition_value '
                 'WHERE table_id=%s AND data_file_id=ANY(%s::bigint[]) '
                 'ORDER BY data_file_id, partition_key_index',
                 (_table_id(conn, table), [f[0] for f in descriptors]))


def _no_merge(conn, table):
    before, active = _heads(conn), files(conn, table)
    result = merge(conn, table, target=TARGET)
    assert result == [], (table, result, active)
    assert _heads(conn) == before and files(conn, table) == active, (table, before, active)
    return {'result': result, 'current_snapshot': before[0], 'last_committed_snapshot': before[1],
            'active_files': active}


def _publication(conn, table, before, original, selected, operation, record_count):
    """Check exact retired/retained IDs and the new file's committed interval."""
    after = snap(conn)
    active = files(conn, table)
    chosen = set(selected)
    assert len(chosen) == len(selected) and chosen <= {f[0] for f in original}, (table, selected, original)
    retained = [f for f in original if f[0] not in chosen]
    assert after > before and snap(conn, last=True) == after, (table, before, after)
    assert active[:-1] == retained and len(active) == len(retained) + 1, (table, original, selected, active)
    output = active[-1]
    assert output[0] > max(f[0] for f in original), (table, original, output)
    assert output[1].startswith(f'og-{operation}-') and output[2] > 0 and output[3] == record_count, (table, output)
    tid = _table_id(conn, table)
    intervals = query(conn, f'SELECT data_file_id, begin_snapshot, end_snapshot, row_id_start, partial_max '
                      f'FROM {META}.ducklake_data_file WHERE table_id=%s AND data_file_id=ANY(%s::bigint[]) '
                      'ORDER BY data_file_id', (tid, [f[0] for f in original] + [output[0]]))
    assert [(i, end) for i, _, end, _, _ in intervals] == [
        (f[0], after if f[0] in chosen else None) for f in original
    ] + [(output[0], None)], (table, intervals)
    assert intervals[-1] == (output[0], after, None, None, None), (table, intervals[-1])
    description = scalar(conn, f'SELECT changes_made FROM {META}.ducklake_snapshot_changes '
                         'WHERE snapshot_id=%s', (after,))
    assert description == f'rewrite_delete:{tid}', (table, after, description)
    return {'before_snapshot': before, 'snapshot': after, 'input_files': original,
            'selected_ids': selected, 'retained_files': retained, 'output_file': output,
            'file_intervals': intervals, 'changes_made': description}


def _check_views(conn, table, publication, columns, expected):
    before, after = publication['before_snapshot'], publication['snapshot']
    preimage = tt(conn, table, before, columns)
    postimage = tt(conn, table, after, columns)
    assert preimage == postimage == expected, (table, before, after, preimage, postimage)
    cdc = {fn: changes(conn, table, after, after, columns, function=fn)
           for fn in ('table_changes', 'table_insertions', 'table_deletions')}
    assert all(result == [] for result in cdc.values()), (table, after, cdc)
    publication.update(time_travel_before=preimage, time_travel_after=postimage, maintenance_cdc=cdc)


def _bound_rows(conn, table, version, projection, expected, *, missing=None):
    with transaction(conn):
        scalar(conn, 'SELECT og_lake.bind_snapshot(%s::name, %s)', (CATALOG, version))
        observed = rows(conn, table, projection)
        assert observed == expected, (table, version, observed, expected)
        if missing is not None:
            query(conn, 'SAVEPOINT missing_evolution_column')
            reject(conn, '42703', sql.SQL('SELECT {} FROM biz.{}').format(
                sql.Identifier(missing), sql.Identifier(table)))
            query(conn, 'ROLLBACK TO SAVEPOINT missing_evolution_column')
            assert rows(conn, table, projection) == observed, (table, version)
    return {'snapshot': version, 'rows': observed, 'missing_column': missing}


def _analyze(conn, table, projection, expected):
    before = _heads(conn)
    query(conn, sql.SQL('ANALYZE biz.{}').format(sql.Identifier(table)))
    reltuples = scalar(conn, 'SELECT reltuples::bigint FROM pg_class WHERE oid=%s::regclass', (f'biz.{table}',))
    stats = query(conn, "SELECT attname, null_frac::float8 FROM pg_stats WHERE schemaname='biz' "
                  'AND tablename=%s ORDER BY attname', (table,))
    wanted = sorted((name, sum(row[i] is None for row in expected) / len(expected))
                    for i, name in enumerate(projection.split(', ')))
    assert reltuples == len(expected), (table, reltuples, len(expected))
    assert [r[0] for r in stats] == [r[0] for r in wanted], (table, stats, wanted)
    assert all(abs(a[1] - b[1]) < 1e-6 for a, b in zip(stats, wanted)), (table, stats, wanted)
    assert _heads(conn) == before, (table, before)
    return {'snapshot': before[0], 'reltuples': reltuples, 'column_null_fractions': stats}


def _partition_groups(conn):
    table = 'evo060_groups'
    make_table(conn, table, partitioned=True)
    ledger = [(1, None, 'old-null-1'), (2, 0, 'old-zero'), (3, None, 'old-null-2'),
              (4, None, 'old-null-3'), (5, None, 'new-null')]
    for row in ledger[:4]:
        _one_file(conn, table, 1, lambda: flush_rows(conn, table, [row]))
    # Publish a new spec with the original shape, so its NULL is equal in value
    # but not in partition_id to the old NULL files. The old spec remains readable.
    for key in ('id', 'part'):
        scalar(conn, 'SELECT og_lake.set_partition(%s::regclass, %s::text[])', (f'biz.{table}', [key]))
    _one_file(conn, table, 1, lambda: flush_rows(conn, table, [ledger[-1]]))
    original = files(conn, table, order='logical')
    ids = [f[0] for f in original]
    old_spec, new_spec = original[0][4], original[4][4]
    assert old_spec is not None and new_spec is not None and old_spec != new_spec, original
    assert [f[4] for f in original] == [old_spec] * 4 + [new_spec], original
    specs = query(conn, f'SELECT partition_id, end_snapshot FROM {META}.ducklake_partition_info '
                  'WHERE table_id=%s AND partition_id=ANY(%s::bigint[]) ORDER BY partition_id',
                  (_table_id(conn, table), [old_spec, new_spec]))
    assert len(specs) == 2 and specs[0][0] == old_spec and specs[0][1] is not None, specs
    assert specs[1] == (new_spec, None), specs
    values = _partition_values(conn, table, original)
    assert values == list(zip(ids, [0] * 5, [None, '0', None, None, None])), values

    before = _heads(conn)
    mixed_groups = [[ids[0], ids[1], ids[2]], [ids[0], ids[4], ids[2]]]
    for selected in mixed_groups:
        reject(conn, '0A000', 'SELECT og_lake.rewrite_data_files(%s::regclass, %s::bigint[])',
               (f'biz.{table}', selected))
        assert _heads(conn) == before and files(conn, table, order='logical') == original, (table, selected)

    result = merge(conn, table, target=TARGET)
    assert result == [('main', table, 3, 1)], result
    publication = _publication(conn, table, before[0], original, [ids[0], *ids[2:4]], 'merge', 3)
    output = publication['output_file']
    output_values = _partition_values(conn, table, [output])
    assert output[4] == old_spec and output_values == [(output[0], 0, None)], (output, output_values)
    assert rows(conn, table) == ledger, table
    _check_views(conn, table, publication, BASE, ledger)
    return {'table': table, 'partition_specs': specs, 'source_partition_values': values,
            'rejected_group_ids': mixed_groups, 'merge_result': result, 'merge': publication,
            'output_partition_values': output_values, 'no_cross_group_retry': _no_merge(conn, table)}


def _column_evolution(conn):
    table = 'evo060_columns'
    make_table(conn, table, partitioned=True)
    created = snap(conn)
    base_rows = [(i, None, f'old-{i}') for i in range(1, 18)]
    for row in base_rows:
        _one_file(conn, table, 1, lambda: flush_rows(conn, table, [row]))
    original = files(conn, table)
    original_spec = original[0][4]
    assert original_spec is not None and all(f[4] == original_spec for f in original), original
    before = snap(conn)
    baseline_cdc = changes(conn, table, created, before, BASE)
    assert [r[3:] for r in baseline_cdc] == base_rows, (table, baseline_cdc)
    assert all(created < r[0] <= before and r[2] == 'insert' for r in baseline_cdc), baseline_cdc
    assert len({r[1] for r in baseline_cdc}) == len(base_rows), baseline_cdc

    # Deliberately unsorted IDs with genuine files, leaving 15 active files.
    selected = [original[i][0] for i in (2, 0, 1)]
    rewrite(conn, table, selected)
    first = _publication(conn, table, before, original, selected, 'rewrite', 3)
    assert rows(conn, table) == base_rows, table

    query(conn, f'ALTER FOREIGN TABLE biz.{table} ADD COLUMN score integer DEFAULT 7')
    query(conn, f'ALTER FOREIGN TABLE biz.{table} ADD COLUMN note integer DEFAULT NULL')
    query(conn, f'ALTER FOREIGN TABLE biz.{table} RENAME COLUMN val TO label')
    added = snap(conn)
    widened = [r + (7, None) for r in base_rows]
    assert rows(conn, table, 'id, part, label, score, note') == widened, table
    assert files(conn, table) == first['retained_files'] + [first['output_file']], table

    # The sixteenth file carries genuinely post-ADD values, not just defaults.
    post_add = (18, None, 'new-explicit', 99, 42)
    query(conn, f'INSERT INTO biz.{table}(id, part, label, score, note) VALUES (%s, %s, %s, %s, %s)', post_add)
    inserted = snap(conn)
    _one_file(conn, table, 1, lambda: _flush(conn, table))
    wide_rows = widened + [post_add]
    stats_added = _analyze(conn, table, 'id, part, label, score, note', wide_rows)
    original = files(conn, table)
    assert len(original) == 16 and all(f[4] == original_spec for f in original), original
    before = snap(conn)
    selected = [f[0] for f in original[::-1]]
    rewrite(conn, table, selected)
    second = _publication(conn, table, before, original, selected, 'rewrite', len(wide_rows))
    assert rows(conn, table, 'id, part, label, score, note') == wide_rows, table

    # DROP the field physically materialized by the second rewrite for older
    # rows. Its field birth is later than those rows' original birth snapshots.
    query(conn, f'ALTER FOREIGN TABLE biz.{table} DROP COLUMN score')
    dropped = snap(conn)
    narrow_rows = [r[:3] + r[4:] for r in wide_rows]
    reject(conn, '42703', f'SELECT score FROM biz.{table}')
    assert rows(conn, table, 'id, part, label, note') == narrow_rows, table
    assert files(conn, table) == [second['output_file']], table
    stats_dropped = _analyze(conn, table, 'id, part, label, note', narrow_rows)

    # Add two more tiny files to exercise automatic selection on an evolved
    # table, including the rewritten file that still contains retired score.
    tail = [(19, None, 'after-drop-1'), (20, None, 'after-drop-2')]
    for row in tail:
        query(conn, f'INSERT INTO biz.{table}(id, part, label) VALUES (%s, %s, %s)', row)
        _one_file(conn, table, 1, lambda: _flush(conn, table))
    final_rows = narrow_rows + [r + (None,) for r in tail]
    original = files(conn, table)
    before = snap(conn)
    result = merge(conn, table, target=TARGET)
    assert result == [('main', table, 3, 1)], result
    third = _publication(conn, table, before, original, [f[0] for f in original], 'merge', len(final_rows))
    assert rows(conn, table, 'id, part, label, note') == final_rows, table
    for pub in (first, second, third):
        output = pub['output_file']
        values = _partition_values(conn, table, [output])
        assert output[4] == original_spec and values == [(output[0], 0, None)], (output, values)
        pub['partition_values'] = values

    _check_views(conn, table, first, BASE, base_rows)
    _check_views(conn, table, second, WIDE, wide_rows)
    _check_views(conn, table, third, DROPPED, final_rows)
    historical_added = tt(conn, table, added, WIDE)
    assert historical_added == widened, (table, added, historical_added)
    historical_dropped = tt(conn, table, dropped, DROPPED)
    assert historical_dropped == narrow_rows, (table, dropped, historical_dropped)
    bound = [
        _bound_rows(conn, table, first['snapshot'], 'id, part, label', base_rows, missing='note'),
        _bound_rows(conn, table, added, 'id, part, label, note', [r + (None,) for r in base_rows]),
        _bound_rows(conn, table, dropped, 'id, part, label, note', narrow_rows),
    ]
    cdc_added = changes(conn, table, created, added, WIDE)
    cdc_dropped = changes(conn, table, created, dropped, DROPPED)
    assert cdc_added == [r + (7, None) for r in baseline_cdc], (table, cdc_added)
    assert cdc_dropped[:-1] == [r + (None,) for r in baseline_cdc] and len(cdc_dropped) == len(wide_rows), cdc_dropped
    assert cdc_dropped[-1][0] == inserted and cdc_dropped[-1][2:] == ('insert',) + narrow_rows[-1], cdc_dropped
    assert cdc_dropped[-1][1] not in {r[1] for r in baseline_cdc}, cdc_dropped
    return {'table': table, 'partition_id': original_spec, 'first_rewrite': first,
            'added_snapshot': added, 'added_time_travel': historical_added,
            'second_rewrite': second, 'dropped_snapshot': dropped, 'dropped_time_travel': historical_dropped,
            'post_drop_merge_result': result, 'post_drop_merge': third, 'bound_reads': bound,
            'baseline_cdc': baseline_cdc, 'post_add_cdc': cdc_added, 'post_drop_cdc': cdc_dropped,
            'analyze_after_add': stats_added, 'analyze_after_drop': stats_dropped,
            'live_rows': final_rows, 'consolidated_noop': _no_merge(conn, table)}


def check_evolution(conn):
    """Return observed partition grouping and schema lifecycle evidence, or fail."""
    return {'partition_groups': _partition_groups(conn), 'columns': _column_evolution(conn)}


def check_delete_exclusion(conn):
    """Reject both real delete carriers in merge; explicit rewrite can compact them."""
    table = 'evo060_deletes'
    make_table(conn, table)
    created = snap(conn)
    ledger = [(i, 0, f'row-{i}') for i in range(1, 6)]
    for group in (ledger[:2], ledger[2:4], ledger[4:]):
        _one_file(conn, table, len(group), lambda: flush_rows(conn, table, group))
    original = files(conn, table)
    file_deleted, inline_deleted, clean = original
    pre_delete = snap(conn)
    inserts = changes(conn, table, created, pre_delete, BASE)
    assert [e[2:] for e in inserts] == [('insert',) + r for r in ledger], inserts
    rowids = {event[3]: event[1] for event in inserts}
    assert len(set(rowids.values())) == len(ledger), inserts

    query(conn, f'DELETE FROM biz.{table} WHERE id=2')
    first_delete = snap(conn)
    _flush(conn, table)  # Transfer only this position to a real delete Parquet.
    flushed_delete = snap(conn)
    assert pre_delete < first_delete < flushed_delete, (pre_delete, first_delete, flushed_delete)
    query(conn, f'DELETE FROM biz.{table} WHERE id=4')
    inline_delete = snap(conn)  # Deliberately do not flush this mark.
    assert flushed_delete < inline_delete and files(conn, table) == original, (flushed_delete, inline_delete)

    tid = _table_id(conn, table)
    mark_sql = sql.SQL('SELECT file_id, row_id, begin_snapshot FROM {}.{} ORDER BY 1, 2, 3').format(
        sql.Identifier(META), sql.Identifier(f'ducklake_inlined_delete_{tid}'))
    delete_sql = (f'SELECT delete_file_id, data_file_id, path, delete_count, file_size_bytes, '
                  f'begin_snapshot, partial_max, end_snapshot '
                  f'FROM {META}.ducklake_delete_file WHERE table_id=%s ORDER BY delete_file_id')
    parquet = query(conn, delete_sql, (tid,))
    markers = query(conn, mark_sql)
    assert len(parquet) == 1, parquet
    mark = parquet[0]
    # The Parquet is registered by flush, but its interval covers the original
    # delete snapshot rather than moving the logical DELETE to the flush commit.
    assert (mark[1], mark[3], *mark[5:]) == (file_deleted[0], 1, first_delete, first_delete, None), parquet
    assert mark[2].endswith('.parquet') and mark[4] > 0, parquet
    assert markers == [(inline_deleted[0], 1, inline_delete)], markers  # row_id is physical position here.

    visible = ledger[::2]
    assert rows(conn, table) == visible, table
    expected_cdc = inserts + [(first_delete, rowids[2], 'delete') + ledger[1],
                              (inline_delete, rowids[4], 'delete') + ledger[3]]
    cdc_before = changes(conn, table, created, inline_delete, BASE)
    assert cdc_before == expected_cdc, (table, cdc_before, expected_cdc)
    # If *either* type of delete-bearing file is admitted, it can pair with the
    # clean file (all are below TARGET), so a successful no-op proves both exclusions.
    excluded = _no_merge(conn, table)
    assert excluded['active_files'] == original, excluded
    assert query(conn, delete_sql, (tid,)) == parquet and query(conn, mark_sql) == markers, table

    selected = [inline_deleted[0], file_deleted[0]]
    rewrite(conn, table, selected)
    compacted = _publication(conn, table, inline_delete, original, selected, 'rewrite', 2)
    retired_mark = query(conn, delete_sql, (tid,))
    assert retired_mark == [mark[:-1] + (compacted['snapshot'],)], retired_mark
    assert query(conn, mark_sql) == markers, 'old inline marks must remain available to historical readers'
    assert compacted['retained_files'] == [clean] and rows(conn, table) == visible, compacted
    _check_views(conn, table, compacted, BASE, visible)
    before_delete_rows = tt(conn, table, pre_delete, BASE)
    assert before_delete_rows == ledger, (table, before_delete_rows)

    # Historical marks on retired inputs must not block their new, clean output.
    after_rewrite = files(conn, table)
    result = merge(conn, table, target=TARGET)
    assert result == [('main', table, 2, 1)], result
    merged = _publication(conn, table, compacted['snapshot'], after_rewrite,
                          [f[0] for f in after_rewrite], 'merge', len(visible))
    assert rows(conn, table) == visible, table
    _check_views(conn, table, merged, BASE, visible)
    cdc_after = changes(conn, table, created, merged['snapshot'], BASE)
    assert cdc_after == expected_cdc, (table, cdc_after, expected_cdc)
    assert query(conn, delete_sql, (tid,)) == retired_mark and query(conn, mark_sql) == markers, table
    return {'table': table, 'pre_delete_snapshot': pre_delete, 'flushed_delete_snapshot': flushed_delete,
            'delete_parquet': parquet, 'inline_delete_marks': markers, 'excluded': excluded,
            'rewrite': compacted, 'retired_delete_parquet': retired_mark, 'clean_merge_result': result,
            'clean_merge': merged, 'live_rows': visible, 'historical_pre_delete_rows': before_delete_rows,
            'cdc_before': cdc_before, 'cdc_after': cdc_after}