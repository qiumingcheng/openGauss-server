#!/usr/bin/env bash
# Standalone reference only: never loads DuckDB into the openGauss product.
set -euo pipefail
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
exec python3 - "$SCRIPT_DIR/source-lock.json" "$@" <<'PY'
import argparse, datetime, fcntl, hashlib, json, os, pathlib, re, shlex, subprocess, sys

lock_path = pathlib.Path(sys.argv[1]).resolve()
root = lock_path.parents[2]
lock = json.loads(lock_path.read_text())
ref = root / '.reference/opengauss-native-lake'
ref.mkdir(parents=True, exist_ok=True)
parser = argparse.ArgumentParser(description='Build and run the exact standalone DuckLake reference')
parser.add_argument('command', choices=['fetch', 'verify', 'build', 'fixture', 'export', 'status'])
parser.add_argument('--case', choices=['F', 'I', 'N', 'HISTORY', 'TYPED', 'INLINE_HISTORY', 'SCHEMA', 'LONG_TEXT', 'ALL_NULL', 'UNKNOWN_BOUNDS', 'USTORE_STREAM', 'EMPTY_TABLE', 'EMPTY_NO_INLINE', 'ANALYZE_DISTRIBUTION', 'FILE_TYPED', 'FILE_BATCH', 'FILE_MIXED', 'FILE_INLINE_UPDATE', 'FILE_INLINE_DELETE', 'FILE_VERSIONED', 'FILE_VERSIONED_BATCH', 'FILE_SCHEMA_GAP', 'FILE_UNKNOWN_BOUNDS'])
parser.add_argument('--run-id')
parser.add_argument('--output', type=pathlib.Path)
parser.add_argument('--manifest', type=pathlib.Path)
parser.add_argument('--state')
parser.add_argument('--jobs', type=int, default=lock['reference_build']['jobs_default'])
args = parser.parse_args(sys.argv[2:])
if not 1 <= args.jobs <= lock['reference_build']['jobs_limit']:
    parser.error(f"--jobs must be between 1 and {lock['reference_build']['jobs_limit']}")
run_id = args.run_id or ('reference-' + datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ'))
if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]*', run_id):
    parser.error('run-id must be a simple identifier')
evidence = root / lock['evidence_root'] / run_id
evidence.mkdir(parents=True, exist_ok=True)
mutex = (ref / '.driver.lock').open('a+')
try:
    fcntl.flock(mutex, fcntl.LOCK_EX | fcntl.LOCK_NB)
except BlockingIOError:
    print('Another reference driver is running', file=sys.stderr)
    sys.exit(2)

def sha(path):
    h = hashlib.sha256()
    with pathlib.Path(path).open('rb') as f:
        for block in iter(lambda: f.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()

lock_hash = sha(lock_path)
if sha(lock_path.with_name("reference.sh")) != lock["reference_build"]["script_sha256"]:
    print("Reference driver differs from source-lock script SHA", file=sys.stderr)
    sys.exit(2)
lock_snapshot = evidence / 'source-lock.json'
if lock_snapshot.exists() and sha(lock_snapshot) != lock_hash:
    print('Run-id already belongs to another source-lock; choose a new run-id', file=sys.stderr)
    sys.exit(2)
lock_snapshot.write_bytes(lock_path.read_bytes())
script_snapshot = evidence / 'reference.sh'
script_snapshot.write_bytes(lock_path.with_name('reference.sh').read_bytes())
counter = 0

def command(argv, name, env=None, capture=False):
    global counter
    counter += 1
    path = evidence / f'{counter:03d}-{name}.log'
    with (evidence / 'commands.jsonl').open('a') as f:
        f.write(json.dumps({'argv': [str(a) for a in argv], 'cwd': str(root), 'log': str(path),
                            'started_at': datetime.datetime.now(datetime.timezone.utc).isoformat(),
                            'source_lock_hash': lock_hash}, ensure_ascii=False) + '\n')
    print(f'[{name}] {shlex.join([str(a) for a in argv])}', flush=True)
    with path.open('w') as log:
        result = subprocess.run([str(a) for a in argv], cwd=root, env=env,
                                stdout=subprocess.PIPE if capture else log, stderr=log, text=True)
        if capture:
            log.write(result.stdout)
    if result.returncode:
        raise RuntimeError(f'{name} failed (exit {result.returncode}); see {path}')
    return result.stdout if capture else None

def repo_path(name):
    return root / lock['repositories'][name]['path']

def verify_sources():
    verified = {}
    for name, entry in lock['repositories'].items():
        if entry['role'] == 'production_baseline':
            continue  # The product worktree can legitimately contain ongoing changes.
        path = repo_path(name)
        if not (path / '.git').exists():
            raise FileNotFoundError(f'Missing reference checkout: {path}; run reference.sh fetch')
        actual = subprocess.check_output(['git', '-C', str(path), 'rev-parse', 'HEAD'], text=True).strip()
        if actual != entry['commit']:
            raise RuntimeError(f'{name}: expected {entry["commit"]}, found {actual}; no automatic reset')
        if subprocess.run(['git', '-C', str(path), 'diff', '--quiet', '--ignore-submodules']).returncode:
            raise RuntimeError(f'{name}: tracked source changes are not recorded in source-lock')
        if subprocess.run(['git', '-C', str(path), 'diff', '--cached', '--quiet']).returncode:
            raise RuntimeError(f'{name}: staged source changes are not recorded in source-lock')
        lic = entry['license']
        if lic['file'] and lic['sha256'] and sha(path / lic['file']) != lic['sha256']:
            raise RuntimeError(f'{name}: license hash changed')
        verified[name] = actual
    (evidence / 'source-verification.json').write_text(json.dumps(verified, indent=2) + '\n')
    return verified

def fetch_sources():
    for name, entry in lock['repositories'].items():
        if entry['role'] == 'production_baseline' or name in ('duckdb', 'extension_ci_tools'):
            continue
        path = repo_path(name)
        if not path.exists():
            command(['git', 'init', path], 'init-' + name)
            command(['git', '-C', path, 'remote', 'add', 'origin', entry['url']], 'remote-' + name)
            command(['git', '-C', path, 'fetch', '--depth', '1', 'origin', entry['commit']], 'fetch-' + name)
            command(['git', '-C', path, 'checkout', '--detach', entry['commit']], 'checkout-' + name)
    command(['git', '-C', repo_path('ducklake'), 'submodule', 'update', '--init', '--depth', '1',
             '--jobs', '2', 'duckdb', 'extension-ci-tools'], 'fetch-submodules')
    verify_sources()

build_dir = ref / lock['reference_build']['build_subdir']
binary = build_dir / 'duckdb'
receipt_path = ref / 'build/receipt.json'

def verify_binary():
    if not binary.is_file() or not receipt_path.is_file():
        raise FileNotFoundError('Exact reference binary/receipt missing; run reference.sh build')
    receipt = json.loads(receipt_path.read_text())
    if receipt['source_lock_hash'] != lock_hash:
        raise RuntimeError('source-lock changed: rebuild reference before reusing fixtures')
    if receipt['binary_sha256'] != sha(binary):
        raise RuntimeError('Reference binary no longer matches build receipt')
    versions = receipt.get('runtime_version', [])
    extensions = receipt.get('runtime_ducklake', [])
    if (len(versions) != 1 or versions[0].get('source_id') != lock['repositories']['duckdb']['commit'][:10] or
            len(extensions) != 1 or not extensions[0].get('loaded') or
            extensions[0].get('extension_version') != lock['repositories']['ducklake']['commit'][:10] or
            extensions[0].get('install_mode') != 'STATICALLY_LINKED'):
        raise RuntimeError('Reference receipt lacks matching runtime identity attestations')
    return receipt

def q(value):
    return "'" + str(value).replace("'", "''") + "'"

def qi(value):
    return '"' + str(value).replace('"', '""') + '"'

def create_fixture(case, output):
    import shutil
    receipt = verify_binary()
    output = output.resolve()
    fixture = ref / 'fixtures' / run_id / case
    if fixture.exists() or output.exists():
        raise RuntimeError('Fixture directory or manifest already exists; use a new run-id/output')
    fixture.mkdir(parents=True)
    data = fixture / 'data'
    data.mkdir()
    catalog_file = fixture / 'metadata.ducklake'
    inlining = 0 if case in ('F', 'EMPTY_NO_INLINE', 'FILE_TYPED', 'FILE_BATCH', 'FILE_MIXED', 'FILE_INLINE_UPDATE', 'FILE_INLINE_DELETE', 'FILE_SCHEMA_GAP', 'FILE_UNKNOWN_BOUNDS') else (1000000 if case in ('USTORE_STREAM', 'ANALYZE_DISTRIBUTION', 'FILE_VERSIONED_BATCH') else 100)
    attach = (f"ATTACH {q('ducklake:' + str(catalog_file))} AS lake (DATA_PATH {q(data)}, "
              f"DUCKLAKE_VERSION '1.1', AUTOMATIC_MIGRATION FALSE, DATA_INLINING_ROW_LIMIT {inlining});\n")
    sql_count = 0
    def sql(statement, query=False):
        nonlocal sql_count
        sql_count += 1
        full = attach + statement
        (fixture / f'{sql_count:03d}.sql').write_text(full + '\n')
        result = command([binary, '-no-init', '-json', '-batch', '-c', full],
                         f'fixture-{case}-{sql_count:03d}', capture=True)
        return (json.loads(result) if result.strip() else []) if query else None
    schema = [{'name':'id','type':'INTEGER'}, {'name':'val','type':'INTEGER'}]
    expected_rows = [{'id':1,'val':10}, {'id':2,'val':100}, {'id':3,'val':200}]
    if case=='FILE_UNKNOWN_BOUNDS':
        expected_rows=[dict(id=1,val=0),dict(id=2,val=None),dict(id=3,val=None)]
    if case in ('EMPTY_TABLE', 'EMPTY_NO_INLINE'):
        expected_rows = []
    if case=='FILE_VERSIONED_BATCH':
        expected_rows=[dict(id=i,val=i*10)for i in range(1,12289)]
    if case == 'ANALYZE_DISTRIBUTION':
        schema += [{'name':'flag','type':'BOOLEAN'}, {'name':'note','type':'VARCHAR'}]
        expected_rows = [dict(id=i, val=1 if i <= 6000 else i,
            flag=None if i % 4 == 0 else i % 2 == 0,
            note=None if i % 5 == 0 else ('hot' if i <= 6000 else 'k'+str(i % 100)))
            for i in range(1, 12001)]
    if case == 'USTORE_STREAM':
        # Only legal business INSERT input. 1024 moderately wide rows cross
        # actual host pages while keeping multiple rows within each page.
        schema += [{'name':'note','type':'VARCHAR'}]
        expected_rows = [dict(id=i, val=i * 10, note=''.join(
            hashlib.sha256(f'{i}:{chunk}'.encode()).hexdigest() for chunk in range(6)))
            for i in range(1, 1025)]
    if case in ('TYPED', 'LONG_TEXT', 'FILE_TYPED', 'FILE_BATCH'):
        schema += [{'name':'flag','type':'BOOLEAN'}, {'name':'bigval','type':'BIGINT'},
                   {'name':'note','type':'VARCHAR'}]
        expected_rows = [
            {'id':1,'val':2147483647,'flag':True,'bigval':9223372036854775807,'note':"中文 ' 引号 \\ 与\n换行"},
            {'id':2,'val':-2147483648,'flag':False,'bigval':-9223372036854775808,'note':None},
            {'id':3,'val':None,'flag':None,'bigval':None,'note':''}]
        if case in ('LONG_TEXT', 'FILE_TYPED'):
            # Legal input rows only: deterministic high-entropy UTF8 payload avoids
            # compressing below the host TOAST threshold during the later OG test.
            expected_rows[0]['note'] = ''.join(
                hashlib.sha256(str(i).encode()).hexdigest() + '湖' for i in range(512))
            if len(expected_rows[0]['note'].encode('utf-8')) != 34304:
                raise RuntimeError('LONG_TEXT payload does not match its byte contract')
        if case == 'FILE_BATCH':
            expected_rows=[dict(id=i,val=i*10,flag=None if i%5==0 else i%2==0,
                bigval=-9223372036854775808 if i==1 else 9223372036854775807 if i==2 else None if i%7==0 else i*10000000000,
                note=None if i%13==0 else '' if i%11==0 else '湖/'+str(i)+'/'+'x'*128) for i in range(1,12289)]
    if case in ('ALL_NULL','UNKNOWN_BOUNDS'):
        schema += [{'name':'flag','type':'BOOLEAN'}, {'name':'bigval','type':'BIGINT'},
                   {'name':'note','type':'VARCHAR'}]
        # Legal business INSERT only. All four nullable fields begin as NULL
        # in a nonempty table; never edit the reference's statistics records.
        expected_rows = [dict(id=i,val=None,flag=None,bigval=None,note=None) for i in (1,2,3)]
        if case == 'UNKNOWN_BOUNDS':
            expected_rows[0] = dict(id=1,val=0,flag=False,bigval=0,note='')
    sql('CREATE SCHEMA lake.biz; CREATE TABLE lake.biz.orders(' +
        ','.join(qi(c['name'])+' '+c['type'] for c in schema) + ');')
    if case in ('UNKNOWN_BOUNDS','FILE_UNKNOWN_BOUNDS'):
        skipped="['val']"if case=='FILE_UNKNOWN_BOUNDS'else"['val','flag','bigval','note']"
        sql("CALL lake.set_option('skip_stats_columns', "+skipped+", schema => 'biz', table_name => 'orders');")
    def literal(value):
        if value is None: return 'NULL'
        if isinstance(value, bool): return 'TRUE' if value else 'FALSE'
        return str(value) if isinstance(value, int) else q(value)
    if case in ('EMPTY_TABLE', 'EMPTY_NO_INLINE'):
        pass  # Actual CREATE only: do not INSERT/DELETE to manufacture an empty layout.
    elif case == 'FILE_BATCH':
        sql("INSERT INTO lake.biz.orders SELECT i::INTEGER,(i*10)::INTEGER,CASE WHEN i%5=0 THEN NULL ELSE i%2=0 END,"
            "CASE WHEN i=1 THEN '-9223372036854775808'::BIGINT WHEN i=2 THEN '9223372036854775807'::BIGINT "
            "WHEN i%7=0 THEN NULL ELSE i*10000000000 END,CASE WHEN i%13=0 THEN NULL WHEN i%11=0 THEN '' "
            "ELSE '湖/'||i::VARCHAR||'/'||repeat('x',128) END FROM range(1,12289) t(i) ORDER BY i;")
    elif case == 'ANALYZE_DISTRIBUTION':
        # Short real SQL avoids the OS single-argv length limit for 12000 rows.
        # Independent Python expectations above are checked against actual reads.
        sql("INSERT INTO lake.biz.orders SELECT i::INTEGER, "
            "(CASE WHEN i<=6000 THEN 1 ELSE i END)::INTEGER, "
            "CASE WHEN i%4=0 THEN NULL ELSE i%2=0 END, "
            "CASE WHEN i%5=0 THEN NULL WHEN i<=6000 THEN 'hot' ELSE 'k'||CAST(i%100 AS VARCHAR) END "
            "FROM range(1,12001) t(i) ORDER BY i;")
    elif case == 'USTORE_STREAM':
        # Keep the command below the OS per-argv limit. The expected Python
        # values above are independently compared with the actual SQL result.
        note_expr = '||'.join("sha256(CAST(i AS VARCHAR)||':" + str(chunk) + "')" for chunk in range(6))
        sql('INSERT INTO lake.biz.orders SELECT i,i*10,' + note_expr + ' FROM range(1,1025) t(i);')
    elif case=='FILE_VERSIONED_BATCH':
        sql('INSERT INTO lake.biz.orders SELECT i,i*10 FROM range(1,12289) g(i);')
    elif case == 'N':
        sql('INSERT INTO lake.biz.orders VALUES (-1,-1);')
        sql('DELETE FROM lake.biz.orders;')
        expected_rows = []
    else:
        values = ','.join('('+','.join(literal(row[c['name']]) for c in schema)+')' for row in expected_rows)
        sql('INSERT INTO lake.biz.orders VALUES '+values+';')
    if case == 'FILE_MIXED':
        # A normal second writer switches its runtime inlining limit. Existing
        # file bytes and all protocol rows are left to DuckLake itself.
        attach=attach.replace('DATA_INLINING_ROW_LIMIT 0','DATA_INLINING_ROW_LIMIT 100')
        inlining=100
        sql('INSERT INTO lake.biz.orders VALUES (4,400);')
        expected_rows.append(dict(id=4,val=400))
    if case in ('FILE_INLINE_UPDATE','FILE_INLINE_DELETE'):
        # A normal writer records file-position deletes in the standard SQL
        # carrier. Never INSERT/UPDATE a protocol row to create this fixture.
        attach=attach.replace('DATA_INLINING_ROW_LIMIT 0','DATA_INLINING_ROW_LIMIT 100')
        inlining=100
        if case=='FILE_INLINE_UPDATE':
            sql('UPDATE lake.biz.orders SET val=11 WHERE id=1;')
            expected_rows[0]['val']=11
        else:
            sql('DELETE FROM lake.biz.orders WHERE id=1;')
            expected_rows=expected_rows[1:]
    if case in ('FILE_VERSIONED','FILE_VERSIONED_BATCH'):
        # Genuine inline versions and their standard flush, with no SDK-created
        # or manually manufactured protocol rows.
        if case=='FILE_VERSIONED_BATCH':
            sql('UPDATE lake.biz.orders SET val=id*20 WHERE id<=8192;')
            sql('UPDATE lake.biz.orders SET val=id*30 WHERE id<=8192;')
            for row in expected_rows:
                if row['id']<=8192:row['val']=row['id']*30
        else:
            sql('UPDATE lake.biz.orders SET val=20 WHERE id=1;')
            sql('UPDATE lake.biz.orders SET val=30 WHERE id=1;')
            expected_rows[0]['val']=30
        sql("CALL ducklake_flush_inlined_data('lake');")
    if case=='FILE_SCHEMA_GAP':
        sql('CREATE TABLE lake.biz.unrelated(other_id INTEGER);')
    if case in ('UNKNOWN_BOUNDS','FILE_UNKNOWN_BOUNDS'):
        # Normal configuration API clears the option after the initial INSERT.
        # Unknown old bounds are retained; never UPDATE statistics by hand.
        sql("CALL lake.set_option('skip_stats_columns', NULL, schema => 'biz', table_name => 'orders');")
    meta = qi('__ducklake_metadata_lake')
    version = sql(f"SELECT value FROM {meta}.ducklake_metadata WHERE key='version';", True)
    if version != [{'value':'1.1'}]:
        raise RuntimeError(f'Wrong actual format: {version}')
    table = sql(f"SELECT table_id,table_name FROM {meta}.ducklake_table WHERE table_name='orders' AND end_snapshot IS NULL;", True)
    if len(table) != 1: raise RuntimeError('orders binding is not unique')
    table_id = table[0]['table_id']
    def snapshot():
        return sql(f'SELECT MAX(snapshot_id) AS s FROM {meta}.ducklake_snapshot;', True)[0]['s']
    def read_rows(s=None, include_begin=False):
        at = f' AT (VERSION => {s})' if s is not None else ''
        begin = 'snapshot_id AS begin_snapshot,' if include_begin else ''
        return sql(f'SELECT rowid AS logical_rowid,{begin}* FROM lake.biz.orders{at} ORDER BY id;', True)
    def business(rows):
        return [{k:v for k,v in row.items() if k not in ('logical_rowid','begin_snapshot')} for row in rows]
    rows = read_rows(include_begin=True)
    if business(rows) != expected_rows: raise RuntimeError(f'Incorrect initial rows: {rows}')
    identities = {row['id']:row['logical_rowid'] for row in rows}
    aliases = {'S10':snapshot()}
    history = {}
    states = []
    def capture_state(label):
        found = read_rows()
        current_snapshot = snapshot()
        command([binary,'-no-init','-batch',catalog_file,'-c','CHECKPOINT;'],f'checkpoint-{case}-{label}')
        wal = pathlib.Path(str(catalog_file)+'.wal')
        if wal.exists() and wal.stat().st_size:
            raise RuntimeError('Checkpoint left WAL; refusing incomplete catalog copy')
        target = fixture / 'states' / label / 'metadata.ducklake'
        target.parent.mkdir(parents=True)
        shutil.copy2(catalog_file,target)
        if sha(target) != sha(catalog_file): raise RuntimeError('Catalog snapshot copy checksum differs')
        states.append({'label':label,'catalog':str(target),'path_sha256':sha(target),
                       'snapshot_id':current_snapshot,'expected':found,
                       'export':str(output.with_name(output.stem+'-'+label+'-catalog-export.json'))})
    initial_expected = [dict(row) for row in expected_rows]
    if case == 'HISTORY':
        sql("CALL ducklake_flush_inlined_data('lake');")
        aliases['S10'] = snapshot()
        sql('UPDATE lake.biz.orders SET val=20 WHERE id=1;')
        aliases['S11'] = snapshot()
        sql('UPDATE lake.biz.orders SET val=30 WHERE id=1;')
        aliases['S12'] = snapshot()
        sql("CALL ducklake_flush_inlined_data('lake');")
        aliases['S13'] = snapshot()
        expected_rows[0]['val'] = 30
        historical_expected = {label:[dict(r, val=value if r['id']==1 else r['val']) for r in initial_expected]
                               for label,value in [('S10',10),('S11',20),('S12',30),('S13',30)]}
    elif case == 'INLINE_HISTORY':
        capture_state('S10')
        sql('UPDATE lake.biz.orders SET val=20 WHERE id=1;')
        aliases['S11'] = snapshot()
        sql('DELETE FROM lake.biz.orders WHERE id=3;')
        aliases['S12'] = snapshot()
        expected_rows = [{'id':1,'val':20},{'id':2,'val':100}]
        capture_state('S12')
        historical_expected = {'S10':initial_expected,'S11':[{'id':1,'val':20},{'id':2,'val':100},{'id':3,'val':200}],
                               'S12':expected_rows}
    elif case == 'SCHEMA':
        sql('ALTER TABLE lake.biz.orders ADD COLUMN extra VARCHAR;')
        aliases['S11'] = snapshot()
        sql("INSERT INTO lake.biz.orders VALUES (4,400,'new schema');")
        aliases['S12'] = snapshot()
        schema += [{'name':'extra','type':'VARCHAR'}]
        expected_rows = [dict(row,extra=None) for row in expected_rows] + [{'id':4,'val':400,'extra':'new schema'}]
        historical_expected = {'S10':initial_expected,'S12':expected_rows}
    else:
        historical_expected = {}
    for label, expected in historical_expected.items():
        found = read_rows(aliases[label])
        if business(found) != expected: raise RuntimeError(f'Incorrect historical rows at {label}: {found}')
        if any(r['id'] in identities and r['logical_rowid'] != identities[r['id']] for r in found):
            raise RuntimeError('UPDATE/flush changed a committed logical identity')
        history[label] = found
    stats = sql(f'SELECT * FROM {meta}.ducklake_table_stats WHERE table_id={table_id};', True)
    column_stats = sql(f'SELECT * FROM {meta}.ducklake_table_column_stats WHERE table_id={table_id} ORDER BY column_id;', True)
    column_definitions = sql(f'SELECT column_id,column_name FROM {meta}.ducklake_column WHERE table_id={table_id} AND end_snapshot IS NULL ORDER BY column_id;', True)
    if case in ('ALL_NULL','UNKNOWN_BOUNDS','FILE_UNKNOWN_BOUNDS'):
        if len(stats) != 1 or stats[0]['record_count'] != 3 or stats[0]['next_row_id'] != 3:
            raise RuntimeError(case + ' must be a real nonempty three-row INSERT, not empty-table statistics')
        by_id = {row['column_id']:row for row in column_stats}
        by_name = {row['column_name']:by_id.get(row['column_id']) for row in column_definitions}
        for name in (('val',)if case=='FILE_UNKNOWN_BOUNDS'else('val','flag','bigval','note')):
            observed = by_name.get(name)
            if observed is None or observed['contains_null'] is not True or observed['min_value'] is not None or observed['max_value'] is not None:
                raise RuntimeError(case + ' actual initial column statistics differ: ' + name)
    cleared_skip_options = []
    if case in ('UNKNOWN_BOUNDS','FILE_UNKNOWN_BOUNDS'):
        cleared_skip_options = sql(f"SELECT key,value,scope,scope_id FROM {meta}.ducklake_metadata WHERE key='skip_stats_columns' AND scope='table' AND scope_id={table_id};", True)
        if any(row['value'] not in (None,'') for row in cleared_skip_options):
            raise RuntimeError('UNKNOWN_BOUNDS must clear the effective skip option before native attachment')
    files = sql(f'SELECT * FROM {meta}.ducklake_data_file WHERE table_id={table_id} ORDER BY data_file_id;', True)
    inline_tables = sql(f'SELECT * FROM {meta}.ducklake_inlined_data_tables WHERE table_id={table_id};', True)
    inline_rows = {t['table_name']:sql(f'SELECT * FROM {meta}.{qi(t["table_name"])};', True) for t in inline_tables}
    inline_file_deletes=[]
    if case in ('FILE_INLINE_UPDATE','FILE_INLINE_DELETE'):
        inline_file_deletes=sql(f'SELECT * FROM {meta}.{qi("ducklake_inlined_delete_"+str(table_id))};',True)
        delete_files=sql(f'SELECT * FROM {meta}.ducklake_delete_file WHERE table_id={table_id};',True)
        paths=list(data.rglob('*.parquet'))
        if (len(files)!=1 or len(paths)!=1 or delete_files or len(inline_file_deletes)!=1 or
                inline_file_deletes[0]['file_id']!=files[0]['data_file_id'] or inline_file_deletes[0]['row_id']!=0):
            raise RuntimeError(case+' requires one real data file and one standard inline position deletion')
        wanted_inline=1 if case=='FILE_INLINE_UPDATE' else 0
        if sum(map(len,inline_rows.values()))!=wanted_inline:
            raise RuntimeError(case+' must preserve the genuine updated/removed inline population')
    delete_files=sql(f'SELECT * FROM {meta}.ducklake_delete_file WHERE table_id={table_id};',True)
    versioned_delete_rows=[]
    physical, physical_schema, file_aliases = [], [], {}
    if case in ('FILE_TYPED','FILE_BATCH','FILE_MIXED'):
        paths=list(data.rglob('*.parquet'))
        file_population=3 if case=='FILE_MIXED' else len(expected_rows)
        if len(files)!=1 or len(paths)!=1 or files[0]['record_count']!=file_population:
            raise RuntimeError(case+' requires one genuine immutable data file')
        physical=sql(f'SELECT file_row_number,* FROM read_parquet({q(paths[0])},file_row_number=true) ORDER BY file_row_number;',True)
        physical_schema=sql(f'SELECT * FROM parquet_schema({q(paths[0])});',True)
        if business([{k:v for k,v in row.items() if k!='file_row_number'} for row in physical])!=expected_rows[:file_population]:
            raise RuntimeError(case+' physical business data differs')
        if [row['file_row_number'] for row in physical]!=list(range(file_population)):
            raise RuntimeError(case+' original file positions differ')
        if sum(map(len,inline_rows.values()))!=(1 if case=='FILE_MIXED' else 0):
            raise RuntimeError(case+' inline population differs')
        file_aliases['F1']={'data_file_id':files[0]['data_file_id'],'path':str(paths[0])}
    if case in ('FILE_VERSIONED','FILE_VERSIONED_BATCH'):
        if len(files)!=1 or len(delete_files)!=1 or any(inline_rows.values()):
            raise RuntimeError('FILE_VERSIONED needs exactly one flushed versioned data/delete pair')
        paths=list(data.rglob('*.parquet'))
        data_paths=[path for path in paths if path.name==files[0]['path']]
        delete_paths=[path for path in paths if path.name==delete_files[0]['path']]
        if len(data_paths)!=1 or len(delete_paths)!=1:
            raise RuntimeError('FILE_VERSIONED paths differ from the genuine catalog')
        physical=sql(f'SELECT file_row_number,* FROM read_parquet({q(data_paths[0])},file_row_number=true) ORDER BY file_row_number;',True)
        physical_schema=sql(f'SELECT * FROM parquet_schema({q(data_paths[0])});',True)
        versioned_delete_rows=sql(f'SELECT * FROM read_parquet({q(delete_paths[0])}) ORDER BY pos;',True)
        wanted_ids=[rid for rid in range(12288)for _ in range(3 if rid<8192 else 1)] if case=='FILE_VERSIONED_BATCH' else [0,0,0,1,2]
        wanted_positions=[pos for pos,row in enumerate(physical)if row['id']<=8192 and row['val']!=row['id']*30] if case=='FILE_VERSIONED_BATCH' else [0,1]
        if len(physical)!=len(wanted_ids) or files[0]['record_count']!=len(wanted_ids) or len(versioned_delete_rows)!=len(wanted_positions):
            raise RuntimeError(case+' physical/deletion population differs')
        if [r['_ducklake_internal_row_id']for r in physical]!=wanted_ids:
            raise RuntimeError(case+' logical identities were renumbered')
        if [r['pos']for r in versioned_delete_rows]!=wanted_positions:
            raise RuntimeError(case+' deletion positions differ')
        file_aliases['F1']={'data_file_id':files[0]['data_file_id'],'path':str(data_paths[0])}
    if case in ('F','FILE_SCHEMA_GAP','FILE_UNKNOWN_BOUNDS'):
        paths = list(data.rglob('*.parquet'))
        if len(files)!=1 or len(paths)!=1 or any(inline_rows.values()):
            raise RuntimeError('F requires exactly one physical data file and no inline rows')
        physical = sql(f'SELECT file_row_number,id,val FROM read_parquet({q(paths[0])},file_row_number=true) ORDER BY file_row_number;', True)
        physical_schema = sql(f'SELECT * FROM parquet_schema({q(paths[0])});', True)
        if [(r['file_row_number'],r['id'],r['val']) for r in physical] != [(i,r['id'],r['val'])for i,r in enumerate(expected_rows)]:
            raise RuntimeError(f'Unexpected physical positions: {physical}')
        if any(row['logical_rowid'] != files[0]['row_id_start']+i for i,row in enumerate(rows)):
            raise RuntimeError('F row_id_start/physical-position mapping differs')
        file_aliases['F1'] = {'data_file_id':files[0]['data_file_id'],'path':str(paths[0])}
    if case in ('I','TYPED','LONG_TEXT','ALL_NULL','UNKNOWN_BOUNDS') and (files or sum(map(len,inline_rows.values())) != 3):
        raise RuntimeError('Inline profile requires exactly three actual inline rows and no files')
    if case == 'USTORE_STREAM':
        if (files or len(inline_tables) != 1 or sum(map(len,inline_rows.values())) != 1024
                or len(stats) != 1 or stats[0]['record_count'] != 1024 or stats[0]['next_row_id'] != 1024):
            raise RuntimeError('USTORE_STREAM requires 1024 genuine inline INSERT rows and no files')
    if case in ('EMPTY_TABLE', 'EMPTY_NO_INLINE'):
        if stats or column_stats or files or rows or any(inline_rows.values()):
            raise RuntimeError(case + ' must be actual CREATE-only with no data or global statistics')
        if case == 'EMPTY_TABLE' and (len(inline_tables) != 1 or len(inline_rows) != 1):
            raise RuntimeError('EMPTY_TABLE with inline100 must retain its genuinely created empty carrier')
        if case == 'EMPTY_NO_INLINE' and (inline_tables or inline_rows):
            raise RuntimeError('EMPTY_NO_INLINE with limit0 must have no carrier')
    if case == 'ANALYZE_DISTRIBUTION':
        if (files or len(inline_tables) != 1 or sum(map(len,inline_rows.values())) != 12000
                or len(stats) != 1 or stats[0]['record_count'] != 12000 or stats[0]['next_row_id'] != 12000):
            raise RuntimeError('ANALYZE_DISTRIBUTION requires 12000 actual inline INSERT rows and no files')
    if case in ('INLINE_HISTORY','SCHEMA','N') and files:
        raise RuntimeError('Inline-only profile unexpectedly created a data file')
    if case == 'SCHEMA' and len({t['schema_version'] for t in inline_tables}) < 2:
        raise RuntimeError('SCHEMA requires multiple real inline schema versions')
    # This pinned reference does not decrement gross record_count for a committed-inline DELETE.
    # The logical scan is empty, while the expired physical row and allocation frontier remain.
    if case == 'N' and (len(stats)!=1 or stats[0]['record_count']!=1 or stats[0]['next_row_id']!=1):
        raise RuntimeError('N requires the actual post-delete statistics/allocation state (1,1)')
    reopened = read_rows()
    if business(reopened) != expected_rows: raise RuntimeError(f'Reopened values differ: {reopened}')
    artifact_files = [{'path':str(p),'sha256':sha(p),'size_bytes':p.stat().st_size}
                      for p in sorted(fixture.rglob('*')) if p.is_file()]
    manifest = {'schema_version':1,'run_id':run_id,'case':case,'status':'verified_reference_only',
                'source_lock_hash':lock_hash,'reference_commit':lock['repositories']['ducklake']['commit'],
                'duckdb_commit':lock['repositories']['duckdb']['commit'],'binary_sha256':receipt['binary_sha256'],
                'catalog':str(catalog_file),'catalog_engine':'DuckDB-local metadata (not openGauss shared catalog)',
                'format_version':'1.1','automatic_migration':False,'table_id':table_id,
                'snapshot_aliases':aliases,'row_identity_map':[dict(r,alias_rowid=1000+i) for i,r in enumerate(rows)],
                'file_aliases':file_aliases,'physical_positions':physical,'physical_schema':physical_schema,
                'schema':schema,'layout_settings':{'data_inlining_row_limit':inlining},'expected':reopened,
                'visible_count_expected':len(reopened),
                'history_expected':history,'catalog_states':states,'table_stats':stats,'column_stats':column_stats,
                'column_definitions':column_definitions,'cleared_skip_options':cleared_skip_options,'data_files':files,
                'inline_tables':inline_tables,'inline_rows':inline_rows,'inline_file_deletes':inline_file_deletes,'delete_files':delete_files,'versioned_delete_rows':versioned_delete_rows,'files':artifact_files,
                'cleanup_owner':str(fixture),
                'limitations':['No target Parquet SDK interoperation claim.','No PostgreSQL/openGauss shared-catalog compatibility claim.']}
    output.parent.mkdir(parents=True,exist_ok=True)
    output.write_text(json.dumps(manifest,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps({'manifest':str(output),'case':case,'format':'1.1','source_lock_hash':lock_hash}),flush=True)


def export_catalog(manifest_path, output, state_label=None):
    receipt = verify_binary()
    manifest_path = manifest_path.resolve()
    m = json.loads(manifest_path.read_text())
    if (m['format_version']!='1.1' or m['source_lock_hash']!=lock_hash or
            m['binary_sha256']!=receipt['binary_sha256']):
        raise RuntimeError('Export manifest does not match the current exact reference')
    selected = m
    if state_label:
        states = [s for s in m.get('catalog_states',[]) if s['label']==state_label]
        if len(states)!=1: raise RuntimeError('Requested catalog state is not unique/present')
        selected = states[0]
    catalog = pathlib.Path(selected['catalog'])
    artifacts = [f for f in m['files'] if pathlib.Path(f['path'])==catalog]
    if len(artifacts)!=1 or sha(catalog)!=artifacts[0]['sha256']:
        raise RuntimeError('Catalog file no longer matches fixture manifest')
    catalog_hash = sha(catalog)
    query_count = 0
    def query(statement):
        nonlocal query_count
        query_count += 1
        result = command([binary,'-readonly','-no-init','-json','-batch',catalog,'-c',statement],
                         f'export-{query_count:03d}',capture=True)
        return json.loads(result) if result.strip() else []
    if query("SELECT value FROM ducklake_metadata WHERE key='version'") != [{'value':'1.1'}]:
        raise RuntimeError('Actual export catalog is not format 1.1')
    tables = query('SELECT schema_name,table_name,sql FROM duckdb_tables() WHERE database_name=current_database() AND NOT internal ORDER BY schema_name,table_name')
    for table in tables:
        predicate = 'database_name=current_database() AND schema_name='+q(table['schema_name'])+' AND table_name='+q(table['table_name'])
        table['columns'] = query('SELECT column_index,column_name,data_type,is_nullable,column_default FROM duckdb_columns() WHERE '+predicate+' ORDER BY column_index')
        table['constraints'] = query('SELECT constraint_type,constraint_text,expression,constraint_column_names FROM duckdb_constraints() WHERE '+predicate+' ORDER BY constraint_index')
        table['rows'] = query('SELECT * FROM '+qi(table['schema_name'])+'.'+qi(table['table_name']))
    if sha(catalog)!=catalog_hash: raise RuntimeError('Read-only export changed the catalog artifact')
    output = output.resolve()
    if output.exists(): raise RuntimeError('Refusing to overwrite catalog export')
    output.parent.mkdir(parents=True,exist_ok=True)
    output.write_text(json.dumps({'scope':'actual_reference_catalog_export_not_host_compatibility',
        'source_manifest':str(manifest_path),'source_manifest_sha256':sha(manifest_path),
        'catalog':str(catalog),'catalog_sha256':catalog_hash,'catalog_state':state_label,
        'binary_sha256':receipt['binary_sha256'],'source_lock_hash':lock_hash,'format_version':'1.1',
        'expected':selected['expected'],'tables':tables},ensure_ascii=False,indent=2)+'\n')
    print(json.dumps({'output':str(output),'tables':len(tables),'rows':sum(len(t['rows']) for t in tables),'sha256':sha(output)}),flush=True)

def build_reference():
    verify_sources()
    env = dict(os.environ, DISABLE_EXTENSIONS_FOR_TEST='1', CMAKE_BUILD_PARALLEL_LEVEL=str(args.jobs))
    deps = ref / 'install'
    roaring_build = ref / 'build/croaring'
    command(['cmake','-S',repo_path('croaring'),'-B',roaring_build,'-G','Ninja',
             '-DCMAKE_BUILD_TYPE=Release',f'-DCMAKE_INSTALL_PREFIX={deps}',
             '-DCMAKE_POSITION_INDEPENDENT_CODE=ON','-DENABLE_ROARING_TESTS=OFF',
             '-DROARING_BUILD_STATIC=ON','-DROARING_USE_CPM=OFF'], 'configure-croaring', env)
    command(['cmake','--build',roaring_build,'--parallel',str(args.jobs)], 'build-croaring', env)
    command(['cmake','--install',roaring_build], 'install-croaring', env)
    command(['cmake','-S',repo_path('duckdb'),'-B',build_dir,'-G','Ninja',
             '-DCMAKE_BUILD_TYPE=Release',f'-DCMAKE_PREFIX_PATH={deps}',
             f'-DCMAKE_C_COMPILER={lock["reference_build"]["c_compiler"]}',
             f'-DCMAKE_CXX_COMPILER={lock["reference_build"]["compiler"]}',
             f'-DCMAKE_CXX_FLAGS_RELEASE={lock["reference_build"]["optimization"]}',
             f'-DCXX_EXTRA={lock["reference_build"]["optimization"]}',
             f'-DDUCKDB_OPTIMIZATION_PROFILE={lock["reference_build"]["architecture_profile"]}',
             '-DDISABLE_UNITY=ON','-DBUILD_UNITTESTS=OFF','-DBUILD_SHELL=ON',
             f'-DDUCKDB_EXTENSION_CONFIGS={repo_path("ducklake") / "extension_config.cmake"}'],
            'configure-duckdb', env)
    command(['cmake','--build',build_dir,'--target','shell','--parallel',str(args.jobs)], 'build-duckdb', env)
    verify_sources()
    if sha(lock_path) != lock_hash:
        raise RuntimeError('source-lock changed during build; rerun build against the new lock')
    if not binary.is_file():
        raise RuntimeError(f'Build did not create expected binary {binary}')
    receipt = {'source_lock_hash': lock_hash, 'binary': str(binary), 'binary_sha256': sha(binary),
               'build_origin': 'exact_pinned_source', 'jobs': args.jobs, 'evidence': str(evidence),
               'completed_at': datetime.datetime.now(datetime.timezone.utc).isoformat()}
    receipt_path.parent.mkdir(parents=True, exist_ok=True)
    version = json.loads(command([binary, '-no-init', '-json', '-c', 'PRAGMA version;'], 'binary-version', capture=True))
    if len(version) != 1 or version[0]['source_id'] != lock['repositories']['duckdb']['commit'][:10]:
        raise RuntimeError(f'Runtime DuckDB source id differs from pinned source: {version}')
    extensions = json.loads(command([binary, '-no-init', '-json', '-c',
                          "SELECT extension_name,loaded,installed,extension_version,install_mode FROM duckdb_extensions() WHERE extension_name='ducklake';"],
                          'binary-ducklake-extension', capture=True))
    if (len(extensions) != 1 or not extensions[0]['loaded'] or
            extensions[0]['extension_version'] != lock['repositories']['ducklake']['commit'][:10] or
            extensions[0]['install_mode'] != 'STATICALLY_LINKED'):
        raise RuntimeError(f'Runtime DuckLake is not the pinned statically linked extension: {extensions}')
    receipt['runtime_version'] = version
    receipt['runtime_ducklake'] = extensions
    receipt_path.write_text(json.dumps(receipt, indent=2) + '\n')
    create_fixture('I', evidence / 'build-smoke-manifest.json')

try:
    if args.command == 'fetch':
        fetch_sources()
    elif args.command == 'verify':
        print(json.dumps(verify_sources(), indent=2))
    elif args.command == 'build':
        build_reference()
    elif args.command == 'fixture':
        if not args.case or not args.output:
            parser.error('fixture requires --case and --output')
        verify_sources()
        create_fixture(args.case, args.output)
    elif args.command == 'export':
        if not args.manifest or not args.output:
            parser.error('export requires --manifest and --output')
        verify_sources()
        export_catalog(args.manifest, args.output, args.state)
    elif args.command == 'status':
        print(json.dumps({'source_lock_hash': lock_hash, 'binary_exists': binary.is_file(),
                          'receipt': json.loads(receipt_path.read_text()) if receipt_path.exists() else None}, indent=2))
except FileNotFoundError as exc:
    (evidence / 'result.json').write_text(json.dumps({'status':'blocked','error':str(exc),'source_lock_hash':lock_hash}, indent=2)+'\n')
    print(f'BLOCKED: {exc}', file=sys.stderr)
    sys.exit(2)
except (RuntimeError, subprocess.CalledProcessError, ValueError) as exc:
    (evidence / 'result.json').write_text(json.dumps({'status':'failed','error':str(exc),'source_lock_hash':lock_hash}, indent=2)+'\n')
    print(f'FAILED: {exc}', file=sys.stderr)
    sys.exit(1)
else:
    (evidence / 'result.json').write_text(json.dumps({'status':'passed','scope':args.command,'source_lock_hash':lock_hash}, indent=2)+'\n')
PY
