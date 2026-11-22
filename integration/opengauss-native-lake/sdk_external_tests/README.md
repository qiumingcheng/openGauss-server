# SDK6 external-file probes

This directory preserves the six PGL-2 probe sources/drivers and three private
host-admission prototype sources listed in the SQL074 migration inventory.
The prototype is test-only: it is not installed into `og_lake` and is not a
production SQL interface. The migration added path/input configuration and a
private-module build helper.

## Four standalone probes

Use Linux with `openat2` support, a C11 compiler, the pinned DuckDB executable,
and the separately built SDK6 provider from `contrib/og_lake/sdk_external/`.
SDK54 alone does not provide the v6 descriptor API. Supply the actual qualified
build digests rather than the historical original-machine hashes.

```bash
export CPP_WORKSPACE=/path/to/cpp
export CODE_BASE="$CPP_WORKSPACE/.worktrees/opengauss-native-lake"
export DUCKDB="$CPP_WORKSPACE/.reference/opengauss-native-lake/build/duckdb-gcc10/duckdb"
export SDK6_LIBRARY=/path/to/sdk6/lib/libog_lake_sdk_arrow.so
export DUCKDB_SHA256='<sha256 from the actual DuckDB build receipt>'
export SDK6_SHA256='<sha256 from the actual SDK6 build receipt>'
P="$CODE_BASE/integration/opengauss-native-lake/sdk_external_tests"
bash "$P/run_probe.sh" sdk6_repro_01
```

`CC`, `GCC_LIB`, `PROBE_LD_LIBRARY_PATH`, and `PROBE_OUTPUT_ROOT` are optional
environment overrides. With the bundled GCC10.3, include its compiler support
libraries if they are not on the system loader path:

```bash
toolchain="$CPP_WORKSPACE/binarylibs/buildtools/gcc10.3"
export PROBE_LD_LIBRARY_PATH="$toolchain/gcc/lib64:$toolchain/isl/lib:$toolchain/mpc/lib:$toolchain/mpfr/lib:$toolchain/gmp/lib"
```

The output run directory must not already exist.
The driver generates a 5,000-row Parquet fixture with row-group size 2,048;
the existing probes require three row groups and retain the exact NULL,
empty-string, UTF-8, ordinal ordering, descriptor identity, and rejection checks.
It runs `reader_probe`, `fd_probe`, `anchor_probe`, and `host_admission_probe`,
using independent copies for probes that replace filenames. Sources, input
binaries, generated fixture, probe binaries, and results are hashed into the new
run's `sha256sums.txt`. Nothing from an old output directory is reused or deleted.

## Private SQL prototype

`build_sql_prototype.py` only builds the private shared object; it never loads it
or connects to a database. It reuses the `lake_file_sdk.cpp` compiler entry from
the matching **Debug** build to preserve the header definitions and include
paths. For CMake, export it with `CMAKE_EXPORT_COMPILE_COMMANDS=ON`. For Make,
use `make -n -W lake_file_sdk.cpp lake_file_sdk.o` from the configured
`contrib/og_lake` directory with the build's toolchain and dependency inputs.
Record the expanded compile recipe as a single entry in a JSON array with
`directory`, `file`, and `arguments` fields. Use the actual compiler path and
expanded argument list. The installed Debug binary, configured source, and
compile entry must describe the same build.

Use Python 3.11 or later for both helpers. When compiling with the bundled
GCC10.3, pass `LD_LIBRARY_PATH="$PROBE_LD_LIBRARY_PATH"` to the build helper if
the compiler support libraries are needed.

```bash
python3 "$P/build_sql_prototype.py" \
  --code-base "$CODE_BASE" --compile-commands /path/to/debug/compile_commands.json \
  --prefix /path/to/debug/install --gaussdb-sha256 '<actual Debug gaussdb SHA256>' \
  --sdk-library "$SDK6_LIBRARY" --sdk-sha256 "$SDK6_SHA256" \
  --cc "$CPP_WORKSPACE/binarylibs/buildtools/gcc10.3/gcc/bin/gcc" \
  --out-dir /path/to/new/module-build
python3 "$P/run_sql_prototype.py" --run-id sdk6_sql_repro_01 \
  --workspace "$CPP_WORKSPACE" --prefix /path/to/debug/install \
  --module-build /path/to/new/module-build/module-build.json \
  --module-build-sha256 '<SHA256 of that new module-build.json>' \
  --fixture "$CPP_WORKSPACE/公共文档/openGauss原生湖存储/验证/sdk6_repro_01/data.parquet" \
  --fixture-sha256 '<fixture SHA256 recorded in that new probe run>'
```

The SQL driver requires `psycopg2`, the sibling migrated `build_driver.py`, and
matching installed openGauss executables. It verifies the module, SDK6, fixture,
and actual running `gaussdb` digests. It copies the matching Debug installation
into the new owned runtime, so allow space for one installation and one cluster.
The driver stages the verified module in the private install's `proc_srclib`
and uses that install's `gsql` for the password-authenticated nonadmin check.
It runs the original materialization and negative/privilege checks, stops the
owned cluster, and retains the runtime and output directories. Optional arguments are `--port`,
`--user`, `--out-dir`, and `--runtime-root`. It does not require an existing
`og_lake` extension or reuse user databases. On 2026-11-22 the four standalone probes and all 17 private SQL checks passed with the matching openEuler WSL2 Debug Make build.
