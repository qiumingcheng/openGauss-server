# Native lake FastCheck

Run these schedules from the configured single-node openGauss source tree.
The scheduled executable cases invoke the original drivers. A failure, missing
dependency or incomplete receipt produces a FastCheck failure. Each invocation
gets a new results directory with the FastCheck summary and diffs, plus a
`fastcheck.json` report and original suite output per executable case.

| `PART` | Cases and prerequisites |
|---|---|
| `.og_lake` | Full executable inventory below. Provide the prerequisites for every group. |
| `.og_lake_smoke` | Registry harness, SDK6 SQL and probes, SDK54 bounded reader, native PQ stream, Release SQL073. |
| `.og_lake_functional` | Full SQL074 in Debug and Release, the three Debug compatibility profiles, and Release SQL073. |
| `.og_lake_history` | SQL056–070 across all Debug profiles, including automated SQL060 maintenance, plus three native datavec cases. |
| `.og_lake_components` | SDK6 SQL and independent probes, SDK54, SDK020, PQ and bitmap checks. Some checks require the original historical artifacts. |
| `.og_lake_long` | Release compatibility and performance, extra performance, and scale/soak. The latter also runs Release SQL056–070 history and datavec. |
| `.og_lake_registry` | Harness verification and every implemented ID in `state/cases.json`. Specification-only IDs are listed in the report. |
| `.og_lake_gates` | Published qualification gates in `state/gates.json`. Provide the original dependency and gate evidence. |
| `.og_lake_standalone` | Executable non-registry scripts listed in `standalone_cases.json`. |
| `.og_lake_standalone_smoke` | Pinned DuckLake flush history oracle without an openGauss instance. |
| `.og_lake_sdk6` | The existing isolated SDK6 SQL case only. |

The full schedule has 41 FastCheck entries. To append a specific executable
case to a group, pass its schedule name in `EXTRA_TESTS`.

Use Python 3.11 or newer with `numpy` and `psycopg2`. Prepare the installations,
profiles, reference builds and actual build receipts as described in the
[rebuild instructions](../README_MIGRATION.md). These jobs use the existing CPP
layout. Set `workspace` in the JSON config, put the attached product checkout
at `.worktrees/opengauss-native-lake`, and use a real, byte-identical driver
copy at `integration/opengauss-native-lake` beneath the workspace. A stale
copy fails before test execution. Put the JSON outside the product repository.
Relative paths in it are resolved against its containing directory.

```bash
export OG_LAKE_FASTCHECK_CONFIG=/path/to/lake-fastcheck.json
export OG_LAKE_FASTCHECK_PYTHON=/path/to/venv/bin/python
export OG_LAKE_SDK6_FASTCHECK_CONFIG=/path/to/sdk6-fastcheck.json
export OG_LAKE_SDK6_PYTHON="$OG_LAKE_FASTCHECK_PYTHON"
make -C src/test/regress fastcheck PART=.og_lake_smoke
```

Use `OG_LAKE_FASTCHECK_OUTPUT_ROOT` as a Make variable to specify the
artifact root. Default is `src/test/regress/results`. Use source and result
paths without whitespace or shell metacharacters because native `_bin`
dispatch expects them. Keep this validation workspace separate from business
instances and avoid sharing it between parallel jobs. The runtime lanes check
ownership, binary and SDK digests, and baseline configs before starting.
Managed profiles are stopped and restored on exit.
Each runtime report records the actual source tree, build receipt, executable
digest and installed SDK digests. The Release tests can use an independently
qualified Release tree and report that tree's build identity.

## Inputs by lane

Example configuration for current builds and component checks. Values shown as
placeholders must come from the actual build/fixture receipts.

```json
{
  "workspace": "/path/to/cpp",
  "suites": {
    "debug_build_receipt": "/path/to/debug/build.json",
    "release_build_receipt": "/path/to/release/build.json",
    "immutable_manifest": "/path/to/debug/immutable.json",
    "release_conf_sha256": "<actual baseline Release postgresql.conf SHA256>"
  },
  "components": {
    "sdk54_build": "/path/to/sdk54/build-result.json",
    "sdk6_build": "/path/to/sdk6/build-result.json",
    "duckdb_sha256": "<qualified DuckDB binary SHA256>",
    "blas_build": "/path/to/blas/build.json",
    "blas_build_sha256": "<actual BLAS build receipt SHA256>"
  }
}
```

- The `suites` immutable manifest has the four original keys `SDK54_SO`,
  `SDK6_SO`, `DEBUG_CONF`, and `ANN_DEBUG_CONF`. The receipt used for ann-debug
  must match the actual compiled source HEAD and source input identity. The
  optional `debug_index_fixture_timeout_ms` only controls index fixture
  preparation and retains the original default of 300000 ms.
- See the separate [SDK6 SQL config](../sdk_external_tests/README.md#fastcheck-sdk6-sql-lane)
  for the isolated case. The SDK6 standalone probes generate their own fixture
  using the qualified DuckDB path. `components.duckdb` can override that path
  for the standalone SDK6 probes. The SDK54 drivers expect the published
  reference-build layout.
- The `components.roaring_build` receipt is required for the current bitmap
  and allocator probes. Optional `previous_sdk54_build` reproduces the SDK54
  bounded-reader regression against the older provider before verifying the
  fixed one. SDK54 object-version checks require `object_version_legacy_workspace`
  and `object_version_legacy_sha256` for the real old provider, with a distinct
  digest. SDK54 candidate qualification uses
  `components.sdk54_candidate_inputs.{list_result,float_result,faults_result,lifecycle_result}`
  from the original passing qualification modes.

- The historical SDK020/SDK54 and PQ component entries continue to use their
  original published driver options and artifact identities. Supply
  `components.legacy_workspace` with the corresponding test mirror and pinned
  installation, and `components.legacy_inputs` keyed by scheduled case ID.
  Required keys are declared in `components_lane.py`. Their absence is reported
  as a failed case with the missing prerequisite. The current native PQ lanes
  cannot substitute for the historical PQ lanes. The original local SDK54
  qualification also needs an actual old ABI v1 installation and
  `legacy_inputs.sdk54_local_qualification.{provider_build,duckdb_sha256}`.
  SDK020 qualification uses `legacy_inputs.sdk020_qualification` with
  `provider_build`, `source`, `legacy_source`, and the existing provider owner's
  checked receipt. The complete SQL020 SDK/PQ pipeline uses
  `legacy_inputs.sql020_pipeline.{source,legacy_source}` and runs the original
  SDK build, SDK qualification, normal and sanitized PQ tests, and static
  archive validation in fresh prefixes.
- The full registry starts an owned Debug instance and runs the published IDs
  serially through `run_case.py`. The report separates specification-only IDs
  from actual runs. The native gate runner checks its own declared dependency
  receipts. The optional `registry` keys `boot_build_evidence`,
  `boot_full_build_evidence`, and `boot_bootstrap_evidence` point to the actual
  BOOT build and initialization receipts. Published contracts that require
  historical builds or external artifacts retain those requirements.


## Standalone historical drivers

The catalogue accounts for 152 directly executable Python entries in
`tests/` outside the 30 entry scripts used by the formal case registry.
It separates 130 tests and campaigns from 22 supporting fixtures, builders,
collectors, cleanup scripts and caller-only entries. C and C++ fixtures are
compiled and exercised by their owning test scripts.

Configure `standalone.cases` by script stem. Each entry is a list of explicit
invocations of that checked-in script. Use the same flags, build receipts and
version-qualified environments as the original driver. A driver that admits a
historic SQL or SDK version still needs that version. Many historical scripts
assume their original workspace layout and cannot use the current SQL074
installation as a substitute.

```json
{
  "workspace": "/path/to/cpp",
  "standalone": {
    "cases": {
      "reference_flush_history_oracle": [
        {
          "argv": ["--run-id", "{run_id}"],
          "result": "{workspace}/公共文档/openGauss原生湖存储/验证/{run_id}/result.json"
        }
      ]
    }
  }
}
```

Available substitutions are `{run_id}`, `{workspace}`, `{source}`,
`{driver}`, `{out}` and `{config}`. Specify the original driver's fresh
JSON result path beneath `{out}` or beneath a run-specific workspace evidence
directory. For tests using a managed local instance, an invocation can set
`profile` to `debug`, `ann-debug` or `release`. It can also set a positive
`timeout` in seconds. If a driver already manages its own instance, omit
`profile` and use the driver's published ownership and admission rules.

The full standalone case attempts all configured jobs and writes a receipt
for each. Jobs without an invocation or a true passing native result fail
the group, and are listed as blocked or failed in `standalone.json`. For
drivers that offer preparation-only modes, the catalogue requires
`--execute` when that flag is part of the original command. Use the complete
driver for file DML, index storage, and scalar file query checks. Where a
driver selects modes, profiles or case sets by argument, the configured
invocations determine the tested matrix. Include all required invocations
for the versions and environments being qualified.

The full schedule includes the 175 implemented registry IDs and the
standalone inventory. The 101 unimplemented IDs remain explicitly listed
as specification-only in registry receipts. Project qualification gates
are a separate schedule because they can require multi-host or production
fixtures beyond the executable local tests.

FastCheck does not advance the native task state. A successful smoke run only
covers the selected schedule. Archive and production gates still require their
original inputs.
