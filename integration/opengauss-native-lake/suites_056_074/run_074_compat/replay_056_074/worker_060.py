"""Verify SQL0.60 background compaction in the caller's owned Debug database."""

import json
from pathlib import Path
from threading import Event
import time

from psycopg2.extensions import TRANSACTION_STATUS_IDLE

from run_060_suite import (EXT_VERSION, META, PROFILE, RUNTIME, changes,
                           files, flush_rows, make_table, now, option_rows, query, rows,
                           scalar, set_option, sha, snap, tt, write_json)


UNSET = 'worker_unset'
DISABLED = 'worker_false'
ENABLED = 'worker_true'
TABLES = (UNSET, DISABLED, ENABLED)  # Give both controls lower scheduling OIDs.
COLUMNS = 'id integer, part integer, val text'
WAIT_SECONDS = 100  # A worker that saw the database before CREATE EXTENSION can defer it for 60s.
POLL_SECONDS = 0.1
FOLLOWUP_ROUNDS = 2  # One round can merely reset the worker's relation cursor.
COUNTERS = ('generation', 'completed_batches', 'failed_batches', 'merge_batches', 'merge_failed')
FILE_FIELDS = ('data_file_id', 'begin_snapshot', 'end_snapshot', 'file_order', 'path',
               'path_is_relative', 'file_format', 'record_count', 'file_size_bytes', 'footer_size',
               'row_id_start', 'partition_id', 'encryption_key', 'mapping_id', 'partial_max', 'row_group_count')


def _require(ok, reason):
    if not ok:
        raise AssertionError(reason)


def _observe(conn):
    database, oid, session, postmaster, backend, raw = query(
        conn, "SELECT current_database()::text, (SELECT oid::bigint FROM pg_database "
              "WHERE datname=current_database()), pg_current_sessionid(), "
              "pg_postmaster_start_time()::text, pg_backend_pid(), og_lake.host_status()"
    )[0]
    host = json.loads(raw)
    return {'identity': [database, oid, session, postmaster], 'backend_pid': backend,
            'module_version': host['module_version'], 'automatic_flush': host['automatic_flush']}


def _status(observation, original):
    state = observation['automatic_flush']
    _require(observation['identity'] == original['identity'] and observation['module_version'] in ('0.60', '0.73', '0.74'),
             'Maintenance check changed database, SQL session, postmaster, or loaded version')
    _require(state['initialized'] is True and state['enabled'] is True and
             type(state['launcher_pid']) is int and state['launcher_pid'] > 0 and
             state['launcher_pid'] == original['automatic_flush']['launcher_pid'],
             'No stable enabled lake maintenance launcher')
    _require(all(type(state[key]) is int and state[key] >= 0 for key in COUNTERS),
             'Missing or invalid lake maintenance counters')
    return state


def _metadata(conn):
    records = query(conn, f'SELECT t.table_name, {", ".join("df." + x for x in FILE_FIELDS)} '
                          f'FROM {META}.ducklake_table t JOIN {META}.ducklake_schema s USING (schema_id) '
                          f'JOIN {META}.ducklake_data_file df ON df.table_id=t.table_id '
                          "WHERE s.schema_name='main' AND s.end_snapshot IS NULL "
                          'AND t.end_snapshot IS NULL AND t.table_name=ANY(%s::text[]) '
                          'ORDER BY t.table_name, df.data_file_id', (list(TABLES),))
    result = {table: [] for table in TABLES}
    for table, *values in records:
        result[table].append(dict(zip(FILE_FIELDS, values)))
    return result


def _local_path(path):
    _require(path.is_absolute() and path.resolve(strict=True) == path, f'Invalid owned local path: {path}')
    return path


def _relative(raw):
    path = Path(raw)
    _require(not path.is_absolute() and ':' not in raw and
             all(part not in ('', '.', '..') for part in raw.rstrip('/').split('/')),
             f'Invalid metadata-relative path: {raw!r}')
    return path


def _disk(conn, root, inventory):
    result = {}
    for table, records in inventory.items():
        directories = query(
            conn, f'SELECT s.path, s.path_is_relative, t.path, t.path_is_relative '
                  f'FROM {META}.ducklake_schema s JOIN {META}.ducklake_table t USING (schema_id) '
                  "WHERE s.schema_name='main' AND s.end_snapshot IS NULL AND t.end_snapshot IS NULL "
                  'AND t.table_name=%s', (table,))
        _require(len(directories) == 1, f'No unique metadata directory for {table}')
        spath, srel, tpath, trel = directories[0]
        _require(srel is True and trel is True, f'Nonrelative table directory for {table}')
        directory = _local_path(root / _relative(spath) / _relative(tpath))
        signatures = {}
        for f in records:
            _require(f['path_is_relative'] is True and f['file_format'].lower() == 'parquet', f)
            path = _local_path(directory / _relative(f['path']))
            size = path.stat().st_size
            _require(path.suffix == '.parquet' and path.is_file() and size == f['file_size_bytes'] > 8,
                     f'Parquet path or size differs from metadata: {path}')
            with path.open('rb') as stream:
                head = stream.read(4)
                stream.seek(-4, 2)
                _require(head == stream.read(4) == b'PAR1', f'Invalid local Parquet envelope: {path}')
            signatures[str(path)] = {'size': size, 'sha256': sha(path)}
        _require(len(signatures) == len(records) and set(signatures) ==
                 {str(path) for path in directory.rglob('*.parquet')},
                 f'Missing, aliased, or untracked Parquet in {directory}')
        result[table] = signatures
    return result


def _merge_snapshot(before, observed, baseline_snapshot):
    for table in (UNSET, DISABLED):
        _require(observed[table] == before[table], f'Background compaction changed control {table}')
    source, actual = before[ENABLED], observed[ENABLED]
    if actual == source:
        return None
    live = [row for row in actual if row['end_snapshot'] is None]
    _require(len(actual) == 3 and len(live) == 1, f'Not a single two-to-one publication: {actual!r}')
    merged = live[0]
    version = merged['begin_snapshot']
    _require(version > baseline_snapshot and merged['record_count'] == sum(f['record_count'] for f in source) and
             merged['partition_id'] is None and Path(merged['path']).name.startswith('og-merge-') and
             merged['data_file_id'] not in {f['data_file_id'] for f in source} and
             merged['path'] not in {f['path'] for f in source}, f'Unexpected compacted output: {merged!r}')
    retired = [row for row in actual if row['end_snapshot'] is not None]
    _require(retired == [dict(f, end_snapshot=version) for f in source], f'Invalid retirement: {retired!r}')
    return version


def check_worker(conn, outdir: Path):
    """Return acceptance evidence; persist every poll and preserve fixtures on error."""
    outdir.mkdir(mode=0o700, exist_ok=False)
    result = {'started_at': now(), 'status': 'running', 'profile': PROFILE,
              'wait_seconds': WAIT_SECONDS, 'poll_seconds': POLL_SECONDS,
              'required_followup_rounds': FOLLOWUP_ROUNDS, 'poll_count': 0}
    poll_path = outdir / 'polls.jsonl'
    try:
        _require(PROFILE == 'debug' and conn.autocommit and
                 conn.get_transaction_status() == TRANSACTION_STATUS_IDLE,
                 'Need an idle autocommit session on the owned Debug profile')
        settings = {name: scalar(conn, 'SHOW ' + name) for name in (
            'enable_thread_pool', 'thread_pool_attr', 'enable_lake_maintenance', 'data_directory',
            'transaction_isolation', 'transaction_read_only')}
        result['settings'] = settings
        _require(settings['enable_thread_pool'] == settings['enable_lake_maintenance'] == 'on' and
                 settings['thread_pool_attr'].split(',')[0].strip() == '4' and
                 Path(settings['data_directory']) == RUNTIME / 'data' and
                 settings['transaction_isolation'] == 'read committed' and
                 settings['transaction_read_only'] == 'off', 'Wrong maintenance acceptance runtime')
        timing = dict(query(conn, 'SELECT name, setting FROM pg_settings WHERE name::text=ANY(%s::text[])', ([
            'lake_flush_naptime', 'lake_flush_min_rows', 'lake_flush_max_delay', 'lake_flush_worker_budget',
            'lake_flush_max_batches', 'statement_timeout'],)))
        result['timing_settings'] = timing
        _require(int(timing['lake_flush_min_rows']) > 1 and int(timing['lake_flush_max_delay']) >= 180000 and
                 0 < int(timing['lake_flush_naptime']) <= 1000 and int(timing['lake_flush_worker_budget']) >= 5000 and
                 int(timing['lake_flush_max_batches']) >= len(TABLES) and
                 0 < int(timing['statement_timeout']) <= 60000,
                 'Need bounded queries and automatic flush thresholds above the staged inserts')
        _require(scalar(conn, EXT_VERSION) in ('0.60', '0.73', '0.74') and
                 not query(conn, f'SELECT 1 FROM {META}.ducklake_table LIMIT 1') and
                 not [r for r in option_rows(conn) if r[0] == 'auto_compact'],
                 'Need a fresh installed SQL0.60 catalog with no inherited auto_compact setting')
        # CREATE CATALOG registers a non-existing path; the first flush makes it.
        root = Path(scalar(conn, f"SELECT value FROM {META}.ducklake_metadata "
                                 "WHERE key='data_path' AND scope IS NULL"))
        result['data_root'] = str(root)
        _require(root.is_absolute() and _local_path(root.parent) == root.parent and not root.exists() and
                 not root.is_symlink(), f'Expected a fresh catalog data root: {root}')

        initial = result['before_fixtures'] = _observe(conn)
        original = _status(initial, initial)
        expected_rows, targets = {}, {}
        for table in TABLES:
            make_table(conn, table)
            expected_rows[table] = [(i, i * 10, f'{table}-row-{i}') for i in (1, 2)]
            for row in expected_rows[table]:
                flush_rows(conn, table, [row])
            active = files(conn, table, order='logical')
            _require(len(active) == 2 and all(f[2] > 8 and f[3] == 1 and f[4] is None for f in active), active)
            targets[table] = sum(f[2] for f in active)
            set_option(conn, f'biz.{table}', 'target_file_size', str(targets[table]))
        set_option(conn, f'biz.{DISABLED}', 'auto_compact', 'false')

        oids = [scalar(conn, 'SELECT %s::regclass::oid::bigint', (f'biz.{table}',)) for table in TABLES]
        result['table_oids_in_scan_order'] = list(zip(TABLES, oids))
        _require(oids[0] < oids[1] < oids[2], 'Control tables must precede the enabled table in worker scan order')
        labels = [r for r in option_rows(conn) if r[0] in ('auto_compact', 'target_file_size')]
        expected_options = {('target_file_size', str(target), 'TABLE', f'main.{table}')
                            for table, target in targets.items()}
        expected_options.add(('auto_compact', 'false', 'TABLE', f'main.{DISABLED}'))
        result['options_before'] = labels
        _require(len(labels) == len(expected_options) and set(labels) == expected_options,
                 f'Unexpected inherited or table options: {labels!r}')
        before = result['files_before'] = _metadata(conn)
        for table in TABLES:
            _require(len(before[table]) == 2 and all(f['end_snapshot'] is None and f['record_count'] == 1
                     and f['partition_id'] is None and not Path(f['path']).name.startswith('og-merge-')
                     and 8 < f['file_size_bytes'] < targets[table] and f['encryption_key'] is None
                     and f['mapping_id'] is None for f in before[table]), before[table])
            _require(rows(conn, table) == expected_rows[table], f'Bad fixture rows in {table}')
        disk_before = result['physical_before'] = _disk(conn, root, before)
        cutoff = result['snapshot_before'] = snap(conn)
        baseline = result['before_opt_in'] = _observe(conn)
        base = _status(baseline, initial)
        _require(all(base[key] == original[key] for key in COUNTERS if key != 'generation'),
                 'Unexpected maintenance work or failure while staging fixtures')
        write_json(outdir / 'worker.json', result)

        # This is the sole activation. There are no foreground merges or writes during observation.
        set_option(conn, f'biz.{ENABLED}', 'auto_compact', 'true')
        started = time.monotonic()
        deadline = started + WAIT_SECONDS
        result['enabled_at'] = now()
        expected_options.add(('auto_compact', 'true', 'TABLE', f'main.{ENABLED}'))
        result['options_enabled'] = option_rows(conn)
        labels = [r for r in result['options_enabled'] if r[0] in ('auto_compact', 'target_file_size')]
        _require(len(labels) == len(expected_options) and set(labels) == expected_options,
                 'Auto-compact activation or control options are wrong')

        waiter = Event()
        previous = base
        confirmed_generation = None
        seen_rounds, finished_rounds = set(), set()
        with poll_path.open('x', encoding='utf-8', buffering=1) as trace:
            while time.monotonic() < deadline:
                result['poll_count'] += 1
                poll = {'ordinal': result['poll_count'], 'time': now(), 'elapsed_s': time.monotonic() - started}
                try:
                    poll['before'] = _observe(conn)
                    poll['files'] = _metadata(conn)
                    poll['after'] = _observe(conn)
                except BaseException as exc:
                    poll['error'] = repr(exc)
                    raise
                finally:
                    trace.write(json.dumps(poll, ensure_ascii=False, allow_nan=False) + '\n')

                version = _merge_snapshot(before, poll['files'], cutoff)
                for key in ('before', 'after'):
                    state = _status(poll[key], initial)
                    _require(all(state[k] >= previous[k] for k in COUNTERS), 'Maintenance counters regressed')
                    _require(all(state[k] == base[k] for k in ('failed_batches', 'merge_failed', 'completed_batches')),
                             'Automatic maintenance failed or unexpectedly published a flush')
                    _require(state['merge_batches'] - base['merge_batches'] in (0, 1), 'Unexpected extra merge batch')
                    previous = state
                    if confirmed_generation is not None and state['generation'] > confirmed_generation:
                        generation = state['generation']
                        if state['database_oid'] == initial['identity'][1]:
                            seen_rounds.add(generation)
                            if state['phase'] == 0:
                                finished_rounds.add(generation)
                        finished_rounds.update(g for g in seen_rounds if g < generation)

                # Commit publishes files before incrementing the shared counter. It is also possible
                # that the file query preceded the commit and the second counter sample followed it.
                # Only accept the joint observation, not either signal in isolation.
                if version is not None and previous['merge_batches'] == base['merge_batches'] + 1:
                    if confirmed_generation is None:
                        confirmed_generation = previous['generation']
                        result['first_joint_observation'] = poll['ordinal']
                        result['merge_snapshot'] = version
                    _require(version == result['merge_snapshot'], 'Published compaction changed during follow-up')
                    if len(finished_rounds) >= FOLLOWUP_ROUNDS and time.monotonic() < deadline:
                        result['files_after'] = poll['files']
                        result['after_followup'] = poll['after']
                        break
                elif confirmed_generation is not None:
                    raise AssertionError('Confirmed merge is no longer visible')
                if time.monotonic() < deadline:
                    waiter.wait(max(0, min(POLL_SECONDS, deadline - time.monotonic())))
            else:
                raise TimeoutError('No exact two-to-one merge plus two completed same-database follow-up rounds')

        after = result['files_after']
        disk_after = result['physical_after'] = _disk(conn, root, after)
        for table in TABLES:
            _require(all(disk_after[table].get(p) == receipt for p, receipt in disk_before[table].items()),
                     f'Missing or modified input Parquet for {table}')
        result['row_checks'] = {table: {'expected': expected_rows[table], 'current': rows(conn, table),
                                        'historical': tt(conn, table, cutoff, COLUMNS)} for table in TABLES}
        _require(all(values['expected'] == values['current'] == values['historical']
                     for values in result['row_checks'].values()), 'Compaction altered current or historical rows')
        version = result['merge_snapshot']
        result['merge_changes'] = changes(conn, ENABLED, version, version, COLUMNS)
        _require(result['merge_changes'] == [] and option_rows(conn) == result['options_enabled'] and
                 _metadata(conn) == after, 'Follow-up changes, options, or file publication did not stay stable')
        final = result['final'] = _observe(conn)
        final_state = _status(final, initial)
        result['counter_deltas'] = {k: final_state[k] - base[k] for k in COUNTERS}
        _require(final_state['generation'] >= previous['generation'] and result['counter_deltas']['merge_batches'] == 1 and
                 all(result['counter_deltas'][k] == 0 for k in ('completed_batches', 'failed_batches', 'merge_failed')),
                 'Final maintenance counters do not attest exactly one successful merge')
        result['status'] = 'passed'
    except BaseException as exc:
        result.update(status='failed', error=repr(exc))
        raise
    finally:
        result['finished_at'] = now()
        if 'finished_rounds' in locals():
            result['completed_followup_generations'] = sorted(finished_rounds)
        if poll_path.exists():
            result['poll_evidence'] = {'path': str(poll_path), 'sha256': sha(poll_path)}
        write_json(outdir / 'worker.json', result)
    return result