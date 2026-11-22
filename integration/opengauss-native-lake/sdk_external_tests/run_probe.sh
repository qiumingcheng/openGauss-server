#!/usr/bin/env bash
# Run four independent PGL-2 probes in a fresh owned output directory.
set -euo pipefail

if [[ $# != 1 || ! "$1" =~ ^[a-zA-Z0-9_][a-zA-Z0-9_.-]*$ ]]; then
    printf 'Usage: %s NEW_RUN_ID\nSee README.md for required SDK6/DuckDB inputs.\n' "$0" >&2
    exit 2
fi
HERE=$(dirname -- "$(realpath -- "$0")")
ROOT=$(realpath -- "${CPP_WORKSPACE:?Set CPP_WORKSPACE to your cpp workspace}")
if [[ -z ${CODE_BASE:-} ]]; then
    candidate=$(realpath -- "$HERE/../../..")
    if [[ -d "$candidate/contrib/og_lake" ]]; then CODE_BASE=$candidate
    else CODE_BASE=$ROOT/.worktrees/opengauss-native-lake; fi
fi
WORKTREE=$(realpath -- "$CODE_BASE/contrib/og_lake")
DUCKDB=${DUCKDB:-$ROOT/.reference/opengauss-native-lake/build/duckdb-gcc10/duckdb}
SDK=$(realpath -- "${SDK6_LIBRARY:?Set SDK6_LIBRARY to the built ABI6 provider}")
GCC_LIB=${GCC_LIB:-$ROOT/binarylibs/buildtools/gcc10.3/gcc/lib64}
if [[ -z ${CC:-} ]]; then
    if [[ -x "$ROOT/binarylibs/buildtools/gcc10.3/gcc/bin/gcc" ]]; then CC=$ROOT/binarylibs/buildtools/gcc10.3/gcc/bin/gcc
    else CC=cc; fi
fi
export LD_LIBRARY_PATH="${PROBE_LD_LIBRARY_PATH:-$GCC_LIB}${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
OUT_PARENT=${PROBE_OUTPUT_ROOT:-$ROOT/公共文档/openGauss原生湖存储/验证}
mkdir -p -- "$OUT_PARENT"
OUT=$(realpath -- "$OUT_PARENT")/$1

verify_hash() {
    local file=$1 expected=$2 digest
    [[ $expected =~ ^[[:xdigit:]]{64}$ ]] || { printf 'Invalid SHA256 for %s\n' "$file" >&2; return 1; }
    digest=$(sha256sum -- "$file")
    if [[ ${digest%% *} != "${expected,,}" ]]; then
        printf 'Unexpected input digest: %s\n' "$file" >&2; return 1
    fi
}
verify_hash "$DUCKDB" "${DUCKDB_SHA256:?Use the SHA256 from your actual DuckDB build receipt}"
verify_hash "$SDK" "${SDK6_SHA256:?Use the SHA256 from your actual SDK6 build receipt}"
mkdir -m 700 -- "$OUT"  # Existing results are never removed or reused.
includes=(-I "$WORKTREE/sdk_external" -I "$WORKTREE" -I "$HERE")
"$CC" -std=c11 -Wall -Wextra -Werror "${includes[@]}" "$HERE/reader_probe.c" -ldl -o "$OUT/reader_probe"
"$CC" -std=c11 -Wall -Wextra -Werror "${includes[@]}" "$HERE/fd_probe.c" -ldl -o "$OUT/fd_probe"
"$CC" -std=c11 -Wall -Wextra -Werror "${includes[@]}" "$HERE/host_admission_probe.c" "$HERE/lake_external_path.c" -ldl -o "$OUT/host_admission_probe"
"$CC" -std=c11 -Wall -Wextra -Werror "$HERE/anchor_probe.c" -o "$OUT/anchor_probe"

SQL_FILE=${OUT//\'/\'\'}/data.parquet
cat > "$OUT/fixture.sql" <<SQL
COPY (SELECT i::INTEGER AS id,
             CASE WHEN i % 5 = 0 THEN NULL WHEN i % 7 = 0 THEN '' ELSE '湖/csv' END AS text_col
      FROM range(5000) AS t(i) ORDER BY i)
TO '$SQL_FILE' (FORMAT PARQUET, ROW_GROUP_SIZE 2048);
SQL
timeout 6 "$DUCKDB" -no-init -batch < "$OUT/fixture.sql" > "$OUT/fixture.stdout" 2> "$OUT/fixture.stderr"
timeout 6 "$OUT/reader_probe" "$SDK" "$OUT/data.parquet" > "$OUT/ordinal-result.json" 2> "$OUT/ordinal.stderr"

# fd_probe replaces its input pathname; the base fixture is kept intact.
cp -- "$OUT/data.parquet" "$OUT/fd-input.parquet"
cp -- "$OUT/data.parquet" "$OUT/different-object.parquet"
timeout 6 "$OUT/fd_probe" "$SDK" "$OUT/fd-input.parquet" "$OUT/different-object.parquet" "$OUT/fd-retained.parquet" \
    > "$OUT/fd-result.json" 2> "$OUT/fd.stderr"

mkdir -p -- "$OUT/root/nested" "$OUT/outside"
printf safe > "$OUT/root/nested/data"
printf outside-only-test-data > "$OUT/outside/data"
ln -s ../outside/data "$OUT/root/symbolic-file"
ln -s ../outside "$OUT/root/binary-dir"
mkfifo -m 600 "$OUT/root/pipe"
timeout 5 "$OUT/anchor_probe" "$OUT/root" > "$OUT/anchor-result.json" 2> "$OUT/anchor.stderr"

mkdir -p -- "$OUT/host/root/nested" "$OUT/host/outside" "$OUT/host/root/directory.parquet"
cp -- "$OUT/data.parquet" "$OUT/host/root/nested/data.parquet"
cp -- "$OUT/data.parquet" "$OUT/host/outside/outside.parquet"
ln -s ../outside/outside.parquet "$OUT/host/root/symbol.parquet"
ln -s ../outside "$OUT/host/root/alias"
ln -s root "$OUT/host/shadow"
mkfifo -m 600 "$OUT/host/root/pipe.parquet"
timeout 6 "$OUT/host_admission_probe" "$SDK" "$OUT/host/root" "$OUT/host" \
    > "$OUT/host-result.json" 2> "$OUT/host.stderr"

sha256sum -- "$HERE/run_probe.sh" "$HERE/reader_probe.c" "$HERE/fd_probe.c" \
    "$HERE/anchor_probe.c" "$HERE/host_admission_probe.c" "$HERE/lake_external_path.c" \
    "$HERE/lake_external_path.h" "$DUCKDB" "$SDK" \
    "$WORKTREE/sdk_external/lake_parquet_sdk.h" "$WORKTREE/sdk_external/lake_parquet_fd_sdk.h" \
    "$WORKTREE/vendor/arrow20/abi.h" "$OUT/fixture.sql" "$OUT/data.parquet" \
    "$OUT/reader_probe" "$OUT/fd_probe" "$OUT/anchor_probe" "$OUT/host_admission_probe" \
    "$OUT/ordinal-result.json" "$OUT/fd-result.json" "$OUT/anchor-result.json" "$OUT/host-result.json" \
    > "$OUT/sha256sums.txt"
sha256sum --quiet -c "$OUT/sha256sums.txt"
cat "$OUT/ordinal-result.json" "$OUT/fd-result.json" "$OUT/anchor-result.json" "$OUT/host-result.json"
