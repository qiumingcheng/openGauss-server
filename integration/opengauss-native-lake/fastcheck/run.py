#!/usr/bin/env python3
"""Run a native lake FastCheck case with the published test drivers."""

import argparse
from datetime import datetime, timezone
import fcntl
import importlib
import json
import os
from pathlib import Path
import pwd
import re
import subprocess
import sys
import tempfile
import traceback

HERE = Path(__file__).resolve().parent
DRIVER_SUBDIR = Path('integration/opengauss-native-lake')
LANES = ('suites_lane', 'components_lane', 'registry_lane', 'standalone_lane')
PASS = 'openGauss native lake FastCheck: passed'


class Context:
    def __init__(self, config_path, out):
        self.config_path = config_path
        self.config = json.loads(config_path.read_text())
        if not isinstance(self.config, dict) or not self.config.get('workspace'):
            raise ValueError('lake FastCheck config needs a workspace path')
        self.workspace = self.path(self.config['workspace'])
        self.source = HERE.parents[2]
        self.driver = self.workspace / DRIVER_SUBDIR
        self.repo_driver = self.source / DRIVER_SUBDIR
        self.out = out
        self.python = sys.executable
        inherited_profiles = {'OG_LAKE_SOURCE', 'OG_LAKE_TEST_PROFILE', 'OG_LAKE_TEST_USER',
                              'OG_LAKE_060_ISOLATED_RUNTIME'}
        self.env = {
            k: v for k, v in os.environ.items()
            if not k.startswith(('PG', 'OG_LAKE_TEST_HOST', 'OG_LAKE_TEST_PORT'))
            and k not in inherited_profiles
        }
        self.env.update(CPP_WORKSPACE=str(self.workspace), PYTHONUNBUFFERED='1',
                        PGUSER=self.config.get('user') or pwd.getpwuid(os.getuid()).pw_name)
        self.commands = []
        self.runtimes = []

    def path(self, value):
        path = Path(value).expanduser()
        return (self.config_path.parent / path).resolve()

    def verify_execution_assets(self):
        # Most original drivers derive the workspace from their own location.
        # Require the real workspace copy and verify bytes before executing it.
        if self.driver.resolve() != self.driver or not self.driver.is_dir():
            raise ValueError('prepare a real integration/opengauss-native-lake copy under workspace')
        names = subprocess.check_output(
            ['git', '-C', str(self.source), 'ls-files', '-z', '--', str(DRIVER_SUBDIR)]
        ).decode().split('\0')
        for item in filter(None, names):
            path = Path(item)
            if path.is_relative_to(DRIVER_SUBDIR / 'fastcheck'):
                continue
            mirror = self.workspace / path
            if mirror.is_symlink() or not mirror.is_file() or mirror.read_bytes() != (self.source / path).read_bytes():
                raise ValueError(f'workspace test asset differs from source: {mirror}')

    def run(self, argv, label, *, cwd=None, env=None, timeout=None):
        if not re.fullmatch(r'[a-zA-Z0-9_.-]+', label):
            raise ValueError(f'invalid FastCheck log label: {label!r}')
        cmd = [str(part) for part in argv]
        path = self.out / (label + '.log')
        command_env = dict(self.env)
        if env:
            command_env.update(env)
        event = {'label': label, 'command': cmd, 'log': str(path), 'status': 'running'}
        self.commands.append(event)
        with path.open('x') as log:
            try:
                result = subprocess.run(cmd, cwd=cwd or self.workspace, env=command_env,
                                        timeout=timeout, stdout=log, stderr=subprocess.STDOUT)
            except BaseException:
                event['status'] = 'failed_to_complete'
                raise
        event.update(status='passed' if result.returncode == 0 else 'failed', returncode=result.returncode)
        result.check_returncode()
        return result


def lane_for(case):
    for name in LANES:
        lane = importlib.import_module(name)
        if case in lane.CASES:
            return lane
    raise ValueError(f'unknown lake FastCheck case {case!r}')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('case')
    args = parser.parse_args()
    if not __debug__:
        parser.error('assertions must be enabled')
    out = ctx = None
    status = 'failed'
    result = None
    failure = None
    started = datetime.now(timezone.utc).isoformat()
    try:
        config = os.environ.get('OG_LAKE_FASTCHECK_CONFIG')
        if not config:
            raise ValueError('set OG_LAKE_FASTCHECK_CONFIG; see fastcheck/README.md')
        lane = lane_for(args.case)
        root = Path(os.environ.get('OG_LAKE_FASTCHECK_ARTIFACTS', 'results')).resolve()
        root.mkdir(parents=True, exist_ok=True)
        out = Path(tempfile.mkdtemp(prefix=f'og_lake_{args.case}.', dir=root))
        ctx = Context(Path(config).expanduser().resolve(strict=True), out)
        ctx.verify_execution_assets()
        locks = ctx.workspace / '.runtime/opengauss-native-lake'
        locks.mkdir(parents=True, exist_ok=True)
        with (locks / 'fastcheck.lock').open('a') as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise RuntimeError('another lake FastCheck case owns this workspace') from None
            if args.case in ('registry', 'registry_gates'):
                from suites_lane import profile
                with profile(ctx, 'debug') as env:
                    ctx.env.update(env)
                    result = lane.run(args.case, ctx)
            else:
                result = lane.run(args.case, ctx)
            if not isinstance(result, dict) or not result:
                raise RuntimeError('test driver did not return a verification receipt')
            if result.get('status') in ('fail', 'failed', 'blocked', 'skip', 'skipped'):
                raise RuntimeError(f"test driver reported {result['status']}")
            status = 'passed'
    except Exception as exc:
        failure = traceback.format_exc()
        print(f'openGauss native lake FastCheck FAILED ({args.case}): {exc}', file=sys.stderr)
        if out is not None:
            print(f'Evidence: {out}', file=sys.stderr)
    finally:
        if out is not None:
            report = dict(case=args.case, status=status, started=started,
                          finished=datetime.now(timezone.utc).isoformat(),
                          commands=ctx.commands if ctx else [], runtimes=ctx.runtimes if ctx else [],
                          result=result, error=failure)
            (out / 'fastcheck.json').write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n')
    if status != 'passed':
        return 1
    print(PASS)
    return 0


if __name__ == '__main__':
    sys.exit(main())
