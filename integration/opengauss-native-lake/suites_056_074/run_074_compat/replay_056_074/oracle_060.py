"""SQL0.60 differential against the pinned standalone DuckLake 1.1 reader.

No service lifecycle or cleanup here. Replay the actual native catalog and read
its retained Parquet files, not a re-created dataset or independent writer run.
"""

from contextlib import contextmanager, nullcontext
from hashlib import file_digest
import json
import os
import getpass
from pathlib import Path
import subprocess
import sys
from uuid import UUID

from psycopg2 import sql
from psycopg2.extensions import TRANSACTION_STATUS_IDLE

ROOT = Path(os.environ.get('CPP_WORKSPACE', str(Path(__file__).resolve().parents[5]))).expanduser().resolve()
DRIVER = ROOT / 'integration/opengauss-native-lake'
sys.path[:0] = [str(DRIVER), str(DRIVER / 'tests')]
from empty_catalog_oracle import read_native_catalog, transport_native
from reverse_reference import literal, qi

DUCKLAKE = '0aaa8e10d4adfe2ca37da77134e991d20106d0e7'
DUCKDB = 'd54cbbe3df17e8e5ce68312f483b4155836112aa'
BUSINESS = ('id', 'part', 'val')
BIZ_TYPES = 'id integer, part integer, val text'
INFO = ('table_name', 'schema_id', 'table_id', 'table_uuid', 'file_count',
        'file_size_bytes', 'delete_file_count', 'delete_file_size_bytes')
FILES = ('data_file', 'data_file_size_bytes', 'data_file_footer_size',
         'data_file_encryption_key', 'delete_file', 'delete_file_size_bytes',
         'delete_file_footer_size', 'delete_file_encryption_key')
OPTIONS = ('option_name', 'description', 'value', 'scope', 'scope_entry')
FEEDS = ('table_changes', 'table_insertions', 'table_deletions')


def require(condition, detail):
    if not condition:
        raise AssertionError(detail)


def encode(value):
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, (bytes, memoryview)):
        return {'hex': bytes(value).hex()}
    raise TypeError(f'Unexpected SQL value: {type(value)}')


def dump(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False,
                               default=encode) + '\n', encoding='utf-8')


def sha(path):
    with path.open('rb') as stream:
        return file_digest(stream, 'sha256').hexdigest()


class Native:
    def __init__(self, conn, output):
        self.conn, self.output = conn, output
        self.count = 0

    def run(self, statement, args=(), columns=None):
        self.count += 1
        stem = self.output / f'{self.count:03d}'
        with self.conn.cursor() as cursor:
            rendered = cursor.mogrify(statement, args)
            stem.with_suffix('.sql').write_bytes(rendered + b'\n')
            try:
                cursor.execute(rendered)
                names = tuple(c.name for c in cursor.description) if cursor.description else ()
                result = cursor.fetchall() if cursor.description else []
                dump(stem.with_suffix('.json'), {'columns': names, 'rows': result,
                                                 'rowcount': cursor.rowcount})
            except Exception as exc:
                dump(stem.with_suffix('.error.json'), {'type': type(exc).__name__,
                     'sqlstate': getattr(exc, 'pgcode', None), 'message': str(exc)})
                raise
        if columns is not None:
            require(names == columns, (statement, names, columns))
        # All assertions use the same strictly typed, JSON-serializable rows as evidence.
        return json.loads(json.dumps(result, default=encode, allow_nan=False))

    def scalar(self, statement, args=()):
        result = self.run(statement, args)
        require(len(result) == 1 and len(result[0]) == 1, (statement, result))
        return result[0][0]

    def snapshot(self, catalog):
        return self.scalar('SELECT id FROM og_lake.current_snapshot(%s::name)', (catalog,))

    @contextmanager
    def bound(self, catalog, snapshot):
        self.run('BEGIN ISOLATION LEVEL READ COMMITTED')
        try:
            self.run('SELECT og_lake.bind_snapshot(%s::name, %s::bigint)', (catalog, snapshot))
            yield
        finally:
            self.run('ROLLBACK')


class Reference:
    """Same transport interface as 0.58/0.59, with exact CLI receipts."""

    def __init__(self, output, binary):
        self.output, self.binary = output, binary
        self.count = 0

    def run(self, statement, name, catalog=None, readonly=False, result=False):
        self.count += 1
        stem = self.output / f'{self.count:03d}-{name}'
        statement = ('SET autoinstall_known_extensions=false;\n'
                     'SET autoload_known_extensions=false;\n' + statement.rstrip() + '\n')
        stem.with_suffix('.sql').write_text(statement, encoding='utf-8')
        argv = [str(self.binary), '-no-init', '-batch', '-bail', '-json']
        if readonly:
            argv.append('-readonly')
        if catalog is not None:
            argv.append(str(catalog))
        with stem.with_suffix('.stdout').open('wb') as out, stem.with_suffix('.stderr').open('wb') as err:
            try:
                proc = subprocess.run(argv, input=statement.encode('utf-8'), stdout=out, stderr=err,
                                      timeout=180, check=False)
            except (OSError, subprocess.TimeoutExpired) as exc:
                dump(stem.with_suffix('.process.json'), {'argv': argv, 'error': repr(exc)})
                raise
        dump(stem.with_suffix('.process.json'), {'argv': argv, 'returncode': proc.returncode})
        if proc.returncode:
            raise RuntimeError(f'DuckLake reference failed: {stem.with_suffix(".stderr")}')
        if result:
            stdout = stem.with_suffix('.stdout').read_text(encoding='utf-8')
            return json.loads(stdout) if stdout.strip() else []

    def read(self, catalog, statement, name, *, snapshot=None):
        opts = "DUCKLAKE_VERSION '1.1', AUTOMATIC_MIGRATION FALSE, READ_ONLY"
        if snapshot is not None:
            opts += f', SNAPSHOT_VERSION {snapshot}'
        return self.run(f'ATTACH {literal("ducklake:" + str(catalog))} AS lake ({opts});\n'
                        + statement, name, result=True)


def reference_runtime(output):
    lock_path = DRIVER / 'source-lock.json'
    ref_root = ROOT / '.reference/opengauss-native-lake'
    receipt_path = ref_root / 'build/receipt.json'
    lock_bytes, receipt_bytes = lock_path.read_bytes(), receipt_path.read_bytes()
    (output / 'source-lock.json').write_bytes(lock_bytes)
    (output / 'reference-build-receipt.json').write_bytes(receipt_bytes)
    lock, receipt = json.loads(lock_bytes), json.loads(receipt_bytes)
    build = lock['reference_build']
    repos = lock['repositories']
    require(lock['target_format'] == build['ducklake_version'] == '1.1', 'not a DuckLake 1.1 lock')
    require(repos['ducklake']['commit'] == DUCKLAKE and repos['duckdb']['commit'] == DUCKDB
            and repos['ducklake']['submodules']['duckdb'] == DUCKDB,
            'reference source-lock does not name the pinned commits')
    require(build['automatic_migration'] is False and sha(DRIVER / 'reference.sh') == build['script_sha256'],
            'source-lock reference driver or migration policy mismatch')
    lock_sha = sha(output / 'source-lock.json')
    binary = (ref_root / build['build_subdir'] / 'duckdb').resolve(strict=True)
    require(receipt['source_lock_hash'] == lock_sha and receipt['build_origin'] == 'exact_pinned_source'
            and Path(receipt['binary']).resolve(strict=True) == binary, 'stale reference build receipt')
    with binary.open('rb') as stream:
        require(stream.read(4) == b'\x7fELF', f'not an ELF binary: {binary}')
    elf_sha = sha(binary)
    require(elf_sha == receipt['binary_sha256'], 'reference ELF SHA-256 mismatch')
    ref = Reference(output, binary)
    versions = ref.run('PRAGMA version;', 'runtime-version', result=True)
    extensions = ref.run("SELECT extension_name, loaded, installed, extension_version, install_mode "
                         "FROM duckdb_extensions() WHERE extension_name='ducklake'",
                         'runtime-extension', result=True)
    require(versions == receipt['runtime_version'] and len(versions) == 1
            and versions[0]['source_id'] == DUCKDB[:10], ('wrong runtime DuckDB', versions))
    require(extensions == receipt['runtime_ducklake'] == [dict(extension_name='ducklake',
            loaded=True, installed=True, extension_version=DUCKLAKE[:10], install_mode='STATICALLY_LINKED')],
            ('wrong runtime DuckLake', extensions))
    proof = dict(source_lock_sha256=lock_sha, binary=str(binary), binary_sha256=elf_sha,
                 duckdb_commit=DUCKDB, ducklake_commit=DUCKLAKE, runtime_version=versions[0],
                 runtime_extension=extensions[0], metadata_format='1.1')
    return ref, proof


def export_catalog(native, ref, catalog, schema, root, directory):
    # Read only true metadata relations in this catalog; never stitch tables
    # from old reference fixtures, edit snapshot IDs, or skip history rows.
    relations = native.run("SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
                           "WHERE n.nspname=%s AND c.relkind='r' ORDER BY c.relname", (schema,))
    exported = read_native_catalog(native.conn, schema, {'tables': [{'table_name': r[0]} for r in relations]},
                                   directory / 'native-catalog.json')
    tables = {t['table_name']: t for t in exported['tables']}
    tags = tables['ducklake_metadata']['rows']
    require([r['value'] for r in tags if r['key'] == 'data_path' and r['scope'] is None] == [str(root) + '/'],
            f'{catalog}: native data_path is not the owned local root')
    require([r['value'] for r in tags if r['key'] == 'encrypted' and r['scope'] is None] == ['false'],
            f'{catalog}: oracle expects a genuinely unencrypted fixture')
    for table in ('ducklake_data_file', 'ducklake_delete_file'):
        require(all(r['encryption_key'] is None for r in tables[table]['rows']),
                f'{catalog}: unexpected encryption key in {table}')
    head = max(r['snapshot_id'] for r in tables['ducklake_snapshot']['rows'])
    require(head == native.snapshot(catalog), f'{catalog}: head advanced during export')
    transport = directory / 'transport.ducklake'
    transport_native(ref, transport, exported)  # also checks each transported relation's exact rows
    return transport, head


def create_ref_fixture(native, root):
    catalog, schema, relation = 'lake060_oracle', 'meta060_oracle', 'biz.ref060'
    native.run('SELECT og_lake.create_catalog(%s::name, %s::name, %s, NULL)', (catalog, schema, str(root)))
    for scope, cast, key, value in (
        (catalog, 'name', 'data_inlining_row_limit', '128'),
        (catalog, 'name', 'auto_compact', 'false'),
        (catalog, 'name', 'target_file_size', '20000000'),
    ):
        native.run(f'SELECT og_lake.set_option(%s::{cast}, %s, %s)', (scope, key, value))
    native.run(f"CREATE FOREIGN TABLE {relation} ({BIZ_TYPES}) SERVER {catalog} "
               "OPTIONS (binding_mode 'create', lake_schema 'main')")
    native.run('SELECT og_lake.set_option(%s::regclass, %s, %s)',
               (relation, 'target_file_size', str(2**24)))
    created = native.snapshot(catalog)
    original = [[10, 1, 'ten'], [11, 1, None], [20, 2, 'twenty'],
                [21, 2, 'twenty-one'], [30, 3, 'thirty'], [31, 3, 'thirty-one']]
    births = []
    for offset in (0, 2, 4):
        native.run(f'INSERT INTO {relation} VALUES (%s,%s,%s), (%s,%s,%s)',
                   original[offset] + original[offset+1])
        births.append(native.snapshot(catalog))
        native.run('SELECT og_lake.flush(%s::regclass)', (relation,))
    before = native.snapshot(catalog)
    inputs = native.run('SELECT file_size_bytes, record_count FROM meta060_oracle.ducklake_data_file '
                        'WHERE end_snapshot IS NULL ORDER BY COALESCE(file_order,data_file_id),data_file_id')
    require(len(inputs) == 3 and all(size > 0 and count == 2 for size, count in inputs), inputs)
    merged = native.run('SELECT * FROM og_lake.merge_adjacent_files(%s::regclass, 2, %s::bigint)',
                        (relation, inputs[0][0] + inputs[1][0]))
    require(merged == [['main', 'ref060', 2, 1]], ('ref060 manual merge', merged))
    after = native.snapshot(catalog)
    native.run(f"UPDATE {relation} SET val='eleven-updated' WHERE id=11")
    updated = native.snapshot(catalog)
    native.run(f'DELETE FROM {relation} WHERE id=30')
    deleted = native.snapshot(catalog)
    native.run('SELECT og_lake.flush(%s::regclass)', (relation,))
    head = native.snapshot(catalog)
    require(created < births[0] < births[1] < births[2] < before < after < updated < deleted < head,
            ('ref060 snapshot order', created, births, before, after, updated, deleted, head))
    current = [original[0], [11, 1, 'eleven-updated'], *original[2:4], original[5]]
    events = [[births[i//2], i, 'insert', *row] for i, row in enumerate(original)] + [
        [updated, 1, 'update_postimage', *current[1]], [updated, 1, 'update_preimage', *original[1]],
        [deleted, 4, 'delete', *original[4]]]
    return dict(catalog=catalog, schema=schema, table='ref060', created=created, before=before,
                after=after, updated=updated, deleted=deleted, head=head, original=original,
                current=current, events=events)


def ref_rows(raw, columns):
    require(isinstance(raw, list), ('unexpected DuckLake result', raw))
    for row in raw:
        require(isinstance(row, dict) and tuple(row) == columns, ('DuckLake output columns', row, columns))
    return [[row[col] for col in columns] for row in raw]


def unsigned_cli(value):
    # DuckDB's JSON CLI emits UBIGINT as decimal strings (incl. snapshot IDs).
    if value is None:
        return None
    require(type(value) is str and value.isascii() and value.isdecimal(), ('invalid CLI UBIGINT', value))
    number = int(value)
    require(str(number) == value and number <= 2**64 - 1, ('invalid CLI UBIGINT', value))
    return number


def local_file(value, root, *, reference):
    if value is None:
        return None
    prefix = str(root) + '/'
    if reference:
        require(isinstance(value, str) and value.startswith(prefix), ('DuckLake file outside native root', value, root))
        value = value[len(prefix):]
    require(isinstance(value, str) and all(p not in ('', '.', '..') for p in value.split('/')),
            ('noncanonical local file name', value))
    return value


def check_oracle(conn, output: Path, data_root: Path, windows: dict):
    """Run on the caller's idle autocommit 0.60 test DB; return JSON evidence.

    ``windows['merge_lane']`` is the original shared-catalog before/after pair.
    The extra catalog is isolated, uses its own never-before-used data root, and
    remains part of the caller's owned database on return or on failure.
    """
    require(conn.autocommit and conn.get_transaction_status() == TRANSACTION_STATUS_IDLE,
            'oracle requires the idle autocommit test connection')
    before, after = windows['merge_lane']
    require(type(before) is int and type(after) is int and 0 <= before < after, ('invalid merge window', windows))
    data_root = data_root.resolve(strict=True)
    output = output.resolve()
    require(not output.is_relative_to(data_root), 'do not put oracle evidence into the shared data root')
    output.mkdir(mode=0o700, parents=True, exist_ok=False)
    for name in ('native', 'reference-cli', 'shared', 'ref060'):
        (output / name).mkdir()
    native = Native(conn, output / 'native')
    result = dict(status='running', output=str(output), script_sha256=sha(Path(__file__)),
                  merge_lane_window=[before, after], checks=[], mismatches=[])
    summary = output / 'oracle-result.json'
    dump(summary, result)

    def compare(label, columns, observed, reference_rows, *, expected=None, sort_rows=False):
        def key(row):
            return json.dumps(row, ensure_ascii=False, separators=(',', ':'), allow_nan=False)

        if sort_rows:
            observed, reference_rows = sorted(observed, key=key), sorted(reference_rows, key=key)
        equal = key(observed) == key(reference_rows)
        expected_ok = expected is None or key(observed) == key(expected)
        dump(output / (label + '.json'), dict(columns=columns, native=observed, ducklake=reference_rows,
                                              expected=expected, parity=equal, fixture_expected=expected_ok))
        result['checks'].append(dict(name=label, native_rows=len(observed), ducklake_rows=len(reference_rows),
                                     parity=equal, fixture_expected=expected_ok))
        if not equal or not expected_ok:
            result['mismatches'].append(label)

    try:
        ref, result['reference'] = reference_runtime(output / 'reference-cli')
        status = json.loads(native.scalar('SELECT og_lake.host_status()'))
        version = native.scalar("SELECT extversion FROM pg_extension WHERE extname='og_lake'")
        result['native'] = dict(extension_version=version, host_status=status,
                                server_version=native.scalar('SELECT version()'))
        require(version in ('0.60', '0.73', '0.74') and status['module_version'] in ('0.60', '0.73', '0.74') and status['merge_adjacent_sql_version'] == '0.60'
                and status['merge_adjacent_files'] is True, ('wrong native SQL/module version', version, status))
        # Protect the already-selected merge_lane window without adding any shared-catalog writes.
        ref_root = output / 'ref060' / 'data'
        fixture = create_ref_fixture(native, ref_root)
        result['ref060_fixture'] = fixture
        catalogs = {}
        for label, catalog, schema, root in (('shared', 'lake060', 'meta060', data_root),
                                             ('ref060', fixture['catalog'], fixture['schema'], ref_root)):
            transport, head = export_catalog(native, ref, catalog, schema, root, output / label)
            file_inventory = [{'path': str(p.relative_to(root)), 'size_bytes': p.stat().st_size, 'sha256': sha(p)}
                              for p in sorted(root.rglob('*.parquet'))]
            require(file_inventory, ('no retained Parquet files', label, root))
            catalogs[label] = (transport, head, catalog, root, {r['path']: r['size_bytes'] for r in file_inventory})
            dump(output / label / 'file-inputs.json', file_inventory)
        require(after <= catalogs['shared'][1] and fixture['head'] == catalogs['ref060'][1],
                ('unstable oracle fixture heads', after, fixture['head'], catalogs))

        for label, table, points, old_rows, head_rows, counts in (
            ('shared', 'merge_lane', (before, after), [[i, 0, f'small-{i}'] for i in range(4)],
             [[i, 0, f'small-{i}'] for i in range(4)], (4, 3)),
            ('ref060', 'ref060', (fixture['before'], fixture['after']), fixture['original'], fixture['current'], (3, 2)),
        ):
            transport, head, catalog, root, sizes = catalogs[label]
            relation = f'biz.{table}'
            for point, sid in [('before', points[0]), ('after', points[1]), ('head', None)]:
                name = f'{label}-{point}'
                with (native.bound(catalog, sid) if sid is not None else nullcontext()):
                    observed_id = (native.snapshot(catalog) if sid is None else native.scalar(
                        'SELECT bound_snapshot FROM og_lake.catalog_status WHERE server_name=%s::name', (catalog,)))
                    observed = native.run(f'SELECT * FROM {relation} ORDER BY id', columns=BUSINESS)
                    info = native.run('SELECT * FROM og_lake.table_info(%s::name) ORDER BY table_id',
                                      (catalog,), INFO)
                sid_arg = f' AT (VERSION => {sid})' if sid is not None else ''
                dk_id = ref_rows(ref.read(transport, "SELECT * FROM ducklake_current_snapshot('lake')",
                                          name + '-snapshot', snapshot=sid), ('id',))
                compare(name + '-snapshot', ('id',), [[observed_id]], [[unsigned_cli(r[0])] for r in dk_id],
                        expected=[[head if sid is None else sid]])
                dk = ref.read(transport, f'SELECT * FROM lake.main.{qi(table)}{sid_arg} ORDER BY id', name + '-rows')
                compare(name + '-rows', BUSINESS, observed, ref_rows(dk, BUSINESS),
                        expected=head_rows if sid is None else old_rows)
                if sid is not None:
                    tt = native.run('SELECT * FROM og_lake.time_travel(%s::regclass, %s::bigint) '
                                    f'AS r({BIZ_TYPES}) ORDER BY id', (relation, sid), BUSINESS)
                    compare(name + '-time-travel', BUSINESS, tt, ref_rows(dk, BUSINESS), expected=old_rows)
                dk_info = ref_rows(ref.read(transport, "SELECT * FROM ducklake_table_info('lake') ORDER BY table_id",
                                            name + '-table-info', snapshot=sid), INFO)
                compare(name + '-table-info', INFO, info, dk_info)
                target = [r for r in info if r[0] == table]
                require(len(target) == 1, (name, table, info))
                if sid is not None:
                    expected_count = counts[0] if point == 'before' else counts[1]
                    require(target[0][4] == expected_count and target[0][6] == 0, (name, target))
                elif label == 'ref060':
                    require(target[0][6] == 2 and target[0][7] > 0, ('need real nonempty delete Parquets', target))
                # All visible catalog tables (including a zero-file table), all eight columns.
                # The catalogs are created by this suite and only contain the main schema.
                table_names = sorted({r[0] for r in info} | {r[0] for r in dk_info})
                for tbl in table_names:
                    list_name = name + '-list-files-' + tbl
                    pg_files = native.run('SELECT * FROM og_lake.list_files(%s::regclass, %s::bigint)',
                                          (f'biz.{tbl}', sid), FILES)
                    dk_arg = f', snapshot_version => {sid}' if sid is not None else ''
                    dk_files = ref_rows(ref.read(transport, "SELECT * FROM ducklake_list_files('lake', "
                                        f"{literal(tbl)}, schema => 'main'{dk_arg})", list_name), FILES)
                    for r in dk_files:
                        for idx in (1, 2, 5, 6):
                            r[idx] = unsigned_cli(r[idx])
                    for rows, is_ref in ((pg_files, False), (dk_files, True)):
                        for r in rows:
                            for idx in (0, 4):
                                r[idx] = local_file(r[idx], root, reference=is_ref)
                                require(r[idx] is None or sizes.get(r[idx]) == r[idx+1], (list_name, r))
                    compare(list_name, FILES, pg_files, dk_files, sort_rows=True)

            starts = native.run(sql.SQL('SELECT begin_snapshot FROM {}.ducklake_table WHERE table_name=%s '
                                        'ORDER BY begin_snapshot LIMIT 1').format(
                                        sql.Identifier('meta060' if label == 'shared' else fixture['schema'])), (table,))
            require(len(starts) == 1, (label, starts))
            start = starts[0][0]
            feed_windows = [('all', start, head), ('merge-only', points[1], points[1])]
            if label == 'ref060':
                feed_windows += [(event, fixture[event], fixture[event]) for event in ('updated', 'deleted')]
            for point, first, last in feed_windows:
                for fn in FEEDS:
                    is_changes = fn == 'table_changes'
                    columns = ('snapshot_id', 'rowid') + (('change_type',) if is_changes else ()) + BUSINESS
                    shape = 'snapshot_id bigint, rowid bigint, ' + ('change_type text, ' if is_changes else '') + BIZ_TYPES
                    order = ', '.join(columns)
                    observed = native.run(f'SELECT * FROM og_lake.{fn}(%s::regclass, %s::bigint, %s::bigint) '
                                          f'AS r({shape}) ORDER BY {order}', (relation, first, last), columns)
                    fn_name = f'{label}-{point}-{fn}'
                    dk = ref.read(transport, f'SELECT {order} FROM ducklake_{fn}'
                                  f"('lake', 'main', {literal(table)}, {first}::BIGINT, {last}::BIGINT) ORDER BY {order}", fn_name)
                    expected = [] if point == 'merge-only' or (label == 'shared' and fn == 'table_deletions') else None
                    if label == 'ref060':
                        events = [r for r in fixture['events'] if first <= r[0] <= last]
                        if is_changes:
                            expected = events
                        else:
                            kinds = ('insert', 'update_postimage') if fn == 'table_insertions' else ('delete', 'update_preimage')
                            expected = [r[:2] + r[3:] for r in events if r[2] in kinds]
                    compare(fn_name, columns, observed, ref_rows(dk, columns), expected=expected)
                    if label == 'shared' and point == 'all' and fn != 'table_deletions':
                        require(len(observed) == 4 and sorted(r[-3:] for r in observed) == old_rows
                                and (not is_changes or all(r[2] == 'insert' for r in observed)), (fn_name, observed))

            # Include description even if a product/reference wording difference fails parity.
            option_order = ' ORDER BY option_name, scope, scope_entry NULLS FIRST, value, description'
            observed = native.run('SELECT * FROM og_lake.options(%s::name)' + option_order, (catalog,), OPTIONS)
            dk = ref_rows(ref.read(transport, "SELECT * FROM ducklake_options('lake')" + option_order,
                                    label + '-options'), OPTIONS)
            compare(label + '-options', OPTIONS, observed, dk)
            # Separate fixture coverage from the complete five-column equality above.
            required_options = {('target_file_size', '20000000', 'GLOBAL', None),
                                ('auto_compact', 'false', 'GLOBAL', None)}
            if label == 'ref060':
                required_options.add(('target_file_size', str(2**24), 'TABLE', 'main.ref060'))
            require(required_options <= {(r[0], *r[2:]) for r in observed},
                    (label, 'required option scenarios absent', observed))
            require(native.snapshot(catalog) == head, (catalog, 'head moved during the oracle'))

        require(sha(ref.binary) == result['reference']['binary_sha256'], 'reference ELF changed during oracle run')
        require(not result['mismatches'], ('strict oracle mismatches (full rows retained)', result['mismatches'], str(output)))
        result['status'] = 'passed'
        return result
    except BaseException as exc:
        result.update(status='failed', error=repr(exc))
        raise
    finally:
        dump(summary, result)