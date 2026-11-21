"""Explicitly bind native tests/builds to one owned Debug or Release runtime.

The default remains Debug. Release uses a separate source worktree so compiling
it cannot mix generated headers or objects with an ongoing Debug build.
"""
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
_PROFILES = {
    'debug': ('opengauss-native-lake', 55560),
    'release': ('opengauss-native-lake-release', 55570),
    'ann-debug': ('opengauss-native-lake', 55580),
}


def source_for_profile(profile):
    if profile not in _PROFILES:
        raise ValueError('Unknown native lake runtime profile: ' + str(profile))
    default = ROOT / '.worktrees' / _PROFILES[profile][0]
    return Path(os.getenv('OG_LAKE_SOURCE', str(default))).resolve()


PROFILE = os.getenv('OG_LAKE_TEST_PROFILE', 'debug')
if PROFILE not in _PROFILES:
    raise ValueError('OG_LAKE_TEST_PROFILE must be debug, release or ann-debug')
SOURCE = source_for_profile(PROFILE)
PORT = _PROFILES[PROFILE][1]
RUNTIME = ROOT / '.runtime/opengauss-native-lake' / PROFILE
INSTALL = ROOT / ('.install/opengauss-native-lake-' + ('debug' if PROFILE=='ann-debug' else PROFILE))
