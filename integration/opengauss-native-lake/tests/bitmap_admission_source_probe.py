#!/usr/bin/env python3
"""Source/SQL-only check of bitmap definition admission and READY protocol.

Executes the exact status SELECT extracted from C string literals over temporary
DuckDB CTE inputs. Does not compile or start openGauss, qualify runtime paths,
or change any source/build/installed provider.
"""
import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess

ROOT = Path('/home/qiumc/data/code/cpp')


def sha(p):
    return hashlib.sha256(p.read_bytes()).hexdigest()


def strings(s):
    return ''.join(json.loads(p) for p in re.findall(r'"(?:[^"\\]|\\.)*"', s))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, default=ROOT / '.worktrees/opengauss-native-lake-bitmap-adapter')
    parser.add_argument('--run-id', required=True)
    args = parser.parse_args()
    if not re.fullmatch(r'[A-Za-z0-9_.-]+', args.run_id):
        parser.error('run-id must be a new simple identifier')
    out = ROOT / '公共文档/openGauss原生湖存储/验证' / args.run_id
    out.mkdir(exist_ok=False)
    files = [args.source / 'contrib/og_lake/lake_index.cpp', args.source / 'contrib/og_lake/lake_inline_index.cpp', Path(__file__)]
    source, inline = files[0].read_text(), files[1].read_text()
    result = dict(status='failed', scope=__doc__, inputs={str(p): sha(p) for p in files}, assertions=[])

    def check(name, actual, expected=True):
        valid = actual == expected
        result['assertions'].append(dict(name=name, observed=actual, expected=expected, status='pass' if valid else 'fail'))
        if not valid:
            raise AssertionError(name)

    try:
        guard = re.search(r'#ifndef USE_LAKE_BITMAP\s+if\(input.bitmap\).*?#endif', source, re.S)
        check('CREATE rejects bitmap with feature-not-supported before host/catalog changes', bool(guard and 'ERRCODE_FEATURE_NOT_SUPPORTED' in guard[0] and source.index(guard[0]) < source.index('Datum og_lake_create_lake_index')))
        check('scalar definition persists actual method in canonical identity', 'input.bitmap?"bitmap":"btree",' in source)
        check('normalized definition remains the definition hash input', 'LakeIndexHash(binding->table_uuid,normalized)' in source)
        baseline = subprocess.check_output(['git', 'show', 'HEAD:contrib/og_lake/lake_index.cpp'], cwd=args.source, text=True)
        old_template = json.loads(re.search(r'char\* normalized=psprintf\(("(?:[^"\\]|\\.)*")', baseline)[1])
        new_template = json.loads(re.search(r'char\* normalized=psprintf\(("(?:[^"\\]|\\.)*")', source)[1])
        def format_definition(template, *parameters):
            return template.replace('%lld', '%d').replace('%u', '%d') % parameters
        old_btree = format_definition(old_template, 7, 700, 0)
        new_btree = format_definition(new_template, 'btree', 7, 700, 0)
        bitmap = format_definition(new_template, 'bitmap', 7, 700, 0)
        check('existing B-tree canonical definition and hash stay byte identical', new_btree, old_btree)
        check('bitmap method has a distinct canonical hash', hashlib.sha256(bitmap.encode()).hexdigest() != hashlib.sha256(new_btree.encode()).hexdigest())
        check('scalar file builder receives bitmap method flag', bool(re.search(r'LakeBuildScalarFileIndexes\([^;]+snapshot,bitmap\)', inline, re.S)))
        check('native inline index stays B-tree/UB-tree', 'statement->accessMethod=pstrdup(ustore?"ubtree":"btree")' in inline)
        check('logical scalar method checked for existing inline OID and planning', inline.count('LakeInlineScalarMethod(') == 4)
        check('bitmap full-dictionary cost differs from preserved sorted lookup cost', 'if(bitmap){' in inline and 'double portableBytes=' in inline and 'double lookupPages=coveredFiles+ceil(ceil(coveredRows/4096.0)*128.0/BLCKSZ);' in inline)

        start = source.index('LakeMetaRows* rows=LakeMetadataQuery(psprintf("SELECT state::text')
        end = source.index('\n            INT4OID,', start)
        query = strings(source[start:end])
        values = iter([23, 20, 700, 701, 700, 701, 8305])
        query = re.sub(r'%u', lambda _: str(next(values)), query).replace('%s', 'NULL::INTEGER')
        query = query.replace('og_lake_internal.', '').replace('$1', "'idx'")
        result['extracted_status_sql'] = query
        duckdb = ROOT / '.reference/opengauss-native-lake/build/duckdb-gcc10/duckdb'
        result['reference_binary_sha256'] = sha(duckdb)
        cases = [('scalar', method, oid) for method in ['btree', 'bitmap'] for oid in [23, 20, 700, 701]]
        cases += [('vector', 'diskann', 8305), ('scalar', 'diskann', 23), ('vector', 'bitmap', 8305),
                  ('scalar', 'bitmap', 25), ('vector', 'diskann', 23), ('scalar', 'btree', 8305),
                  ('unknown', 'bitmap', 23), ('scalar', 'unknown', 23)]
        definitions, segments, matrix, expectations = [], [], [], []
        for kind, method, oid in cases:
            for generation in [1, 2]:
                for algorithm in [1, 2]:
                    for fmt in [1, 2]:
                        for state in ['READY', 'RETIRED']:
                            identity = 'case%03d' % len(matrix)
                            definition = json.dumps(dict(kind=kind, method=method), separators=(',', ':'))
                            definitions.append("('%s','ACTIVE',1,'%s'::JSON,%d)" % (identity, definition, oid))
                            segments.append("('%s',%d,'%s',%d,%d)" % (identity, generation, state, algorithm, fmt))
                            scalar = kind == 'scalar' and method in ['btree', 'bitmap'] and oid in [23, 20, 700, 701]
                            ann = kind == 'vector' and method == 'diskann' and oid == 8305
                            expected_algorithm = 2 if ann or oid in [700, 701] else 1
                            expected_format = 2 if method == 'bitmap' else 1
                            valid = (scalar or ann) and generation == 1 and state == 'READY' and algorithm == expected_algorithm and fmt == expected_format
                            matrix.append([kind, method, oid, generation, algorithm, fmt, state])
                            expectations.append(1 if valid else 0)
        cte = ("WITH index_definition(index_uuid,state,definition_generation,definition,type_oid) AS (VALUES %s), "
               "index_segment(index_uuid,definition_generation,state,algorithm_version,format_version) AS (VALUES %s) "
               % (','.join(definitions), ','.join(segments)))
        matrix_query = query.replace('SELECT state::text,', 'SELECT d.index_uuid AS case_id,state::text,', 1)
        matrix_query = matrix_query.replace('),NULL::INTEGER,', ') AS ready,NULL::INTEGER,', 1).replace("WHERE index_uuid='idx'", 'WHERE TRUE')
        command = [str(duckdb), '-no-init', '-batch', '-json', '-c', cte + matrix_query + ' ORDER BY case_id']
        process = subprocess.run(command, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True, timeout=30)
        rows = json.loads(process.stdout)
        check('all method/type/format/algorithm/generation mismatch combinations', [r['ready'] for r in rows], expectations)
        check('current source identity unchanged', all(sha(Path(p)) == digest for p, digest in result['inputs'].items()))
        result.update(status='passed', protocol_matrix=matrix)
    except BaseException as error:
        result.update(reason=str(error))
    (out / 'result.json').write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps(dict(status=result['status'], assertions=len(result['assertions']), reason=result.get('reason'), result=str(out / 'result.json')), ensure_ascii=False))
    return 0 if result['status'] == 'passed' else 1


if __name__ == '__main__':
    raise SystemExit(main())
