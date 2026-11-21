#!/usr/bin/env python3
"""Actual SQL19 installation and owner-transfer regression, in owned databases."""
import argparse
import json
from pathlib import Path
import shutil
import sys
import traceback

ROOT = Path(__file__).resolve().parents[3]
D = ROOT / 'integration/opengauss-native-lake'
sys.path[:0] = [str(D), str(D / 'tests')]
from file_dml_acceptance import check_build
from inline_fixture import query, scalar
from lake_harness import sha256, validate_run_id, write_json
from native_auto_flush import connect_control
from native_direct_insert import archive_inputs, source_contract
from native_test_context import OwnedDatabase
from permission_api import OwnedRoles, cleanup_roles


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--build-receipt', type=Path, required=True)
    parser.add_argument('--source-contract', type=Path, required=True)
    args = parser.parse_args()
    validate_run_id(args.run_id)
    out = ROOT / '公共文档/openGauss原生湖存储/验证' / args.run_id
    out.mkdir(exist_ok=False)
    result = dict(status='fail', scope=__doc__, cases=[])
    control = None
    try:
        build = check_build(args.build_receipt)
        source_contract(args.source_contract, build)
        result['inputs'] = archive_inputs(out, build, args.source_contract)
        script = out / 'executed-install-runner.py'
        shutil.copy2(__file__, script)
        result['runner_sha256'] = sha256(script)
        result['binary_sha256'] = build['binary_sha256']
        control = connect_control()
        query(control, 'SET session_timeout=0')
        for mode in ('fresh', 'upgrade-unconfigured', 'upgrade-configured'):
            child_id = args.run_id + '_' + mode.replace('-', '_')
            case_out = out / mode
            case_out.mkdir()
            case = dict(mode=mode, status='fail', assertions=[])
            result['cases'].append(case)
            db = roles = None
            def check(name, actual, expected):
                case['assertions'].append(dict(name=name, observed=actual, expected=expected,
                                               status='pass' if actual == expected else 'fail'))
                assert actual == expected, name
            try:
                db = OwnedDatabase(child_id, case_out)
                db.create()
                roles = OwnedRoles(control, child_id, case_out)
                conn = db.connect()
                query(conn, 'SET session_timeout=0')
                query(conn, 'SET statement_timeout=30000')
                initial = '0.19' if mode == 'fresh' else '0.18'
                query(conn, "CREATE EXTENSION og_lake VERSION '" + initial + "'")
                check('initial extension version', scalar(conn,
                    "SELECT extversion FROM pg_extension WHERE extname='og_lake'"), initial)
                check('fresh access policy is empty', scalar(conn,
                    'SELECT count(*) FROM og_lake_internal.access_policy'), 0)
                owner = roles.create('private', False)
                if mode == 'upgrade-configured':
                    query(conn, 'SELECT og_lake.configure_internal_owner(%s::name)', (owner,))
                if initial == '0.18':
                    query(conn, "ALTER EXTENSION og_lake UPDATE TO '0.19'")
                check('installed SQL19', scalar(conn,
                    "SELECT extversion FROM pg_extension WHERE extname='og_lake'"), '0.19')
                check('upgrade does not invent an owner', scalar(conn,
                    'SELECT count(*) FROM og_lake_internal.access_policy'),
                    1 if mode == 'upgrade-configured' else 0)
                if mode != 'upgrade-configured':
                    failure = None
                    try:
                        query(conn, 'SELECT og_lake.create_catalog(%s::name,%s::name,%s::text,NULL::text)',
                              ('install_lake', 'install_metadata', str(case_out / 'data')))
                    except Exception as exc:
                        failure = getattr(exc, 'pgcode', None)
                    check('unconfigured catalog creation remains refused', failure, '55000')
                    query(conn, 'SELECT og_lake.configure_internal_owner(%s::name)', (owner,))
                check('analysis state has exact configured private owner', scalar(conn,
                    "SELECT c.relowner=r.oid FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
                    "JOIN pg_roles r ON r.rolname=%s WHERE n.nspname='og_lake_internal' AND c.relname='analyze_state'",
                    (owner,)), True)
                query(conn, 'SELECT og_lake.create_catalog(%s::name,%s::name,%s::text,NULL::text)',
                      ('install_lake', 'install_metadata', str(case_out / 'data')))
                check('new catalog contains initial committed snapshot', scalar(conn,
                    'SELECT count(*) FROM install_metadata.ducklake_snapshot'), 1)
                case['status'] = 'pass'
            finally:
                if db is not None:
                    db.cleanup()
                    case['database_cleanup'] = db.cleanup_result
                if roles is not None:
                    case['role_cleanup'] = cleanup_roles(control, roles.path, child_id)
                write_json(case_out / 'result.json', case)
        check_build(args.build_receipt)
        assert all(sha256(Path(p)) == v['sha256'] == sha256(Path(v['archive']))
                   for p, v in result['inputs'].items())
        assert sha256(Path(__file__)) == result['runner_sha256']
        result['status'] = 'pass'
    except Exception as exc:
        result.update(reason=str(exc), sqlstate=getattr(exc, 'pgcode', None), traceback=traceback.format_exc())
    finally:
        if control is not None:
            control.close()
        write_json(out / 'result.json', result)
    print(json.dumps(dict(status=result['status'], reason=result.get('reason'), output=str(out / 'result.json'))))
    return 0 if result['status'] == 'pass' else 1


if __name__ == '__main__':
    raise SystemExit(main())
