#!/usr/bin/env bash
# Isolate all non-public C++ / COMDAT symbols in libog_lake_pq_cpu.a so only
# pq_cpu_* and pq_parquet_* remain externally visible across static links.
set -euo pipefail

if [[ $# -lt 2 || $# -gt 4 ]]; then
    echo "Usage: $0 <archive-path> <map-output> [objcopy] [ranlib]" >&2
    exit 2
fi

ARCHIVE="$1"
MAP_FILE="$2"
OBJCOPY="${3:-objcopy}"
RANLIB="${4:-ranlib}"
NM_BIN="${NM:-nm}"
READELF_BIN="${READELF:-readelf}"

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "${TMP_DIR}"' EXIT

# Collect defined external symbols (letter type column) plus COMDAT section group
# signature symbols so GCC C5/D5 groups cannot silently discard private helpers.
"${NM_BIN}" --defined-only --extern-only "${ARCHIVE}" 2>&1 \
    | awk 'NF >= 3 && $(NF-1) ~ /^[A-Za-z]$/ { sym=$NF; sub(/@.*/, "", sym); print sym }' \
    > "${TMP_DIR}/raw_syms.txt"

"${READELF_BIN}" --section-groups "${ARCHIVE}" 2>&1 \
    | sed -n 's/.*COMDAT group section .*\[[[:space:]]*\([^]\n]*\)[[:space:]]*\] contains.*/\1/p' \
    >> "${TMP_DIR}/raw_syms.txt" || true

LC_ALL=C sort -u "${TMP_DIR}/raw_syms.txt" \
    | awk '$0 != "" && $0 !~ /^(pq_cpu_|pq_parquet_)/ { print $0 " oglpqcpu_" $0 }' \
    > "${TMP_DIR}/symbol-map.txt"

if [[ ! -s "${TMP_DIR}/symbol-map.txt" ]]; then
    echo "ERROR: symbol isolation map is unexpectedly empty for ${ARCHIVE}" >&2
    exit 1
fi

cp -f "${ARCHIVE}" "${TMP_DIR}/libog_lake_pq_cpu.a.tmp"
"${OBJCOPY}" --redefine-syms="${TMP_DIR}/symbol-map.txt" "${TMP_DIR}/libog_lake_pq_cpu.a.tmp"
"${RANLIB}" "${TMP_DIR}/libog_lake_pq_cpu.a.tmp"

# Verify zero non-public defined globals remain un-isolated.
BAD_SYMS="$("${NM_BIN}" --defined-only --extern-only "${TMP_DIR}/libog_lake_pq_cpu.a.tmp" 2>&1 \
    | awk 'NF >= 3 && $(NF-1) ~ /^[A-Za-z]$/ { sym=$NF; sub(/@.*/, "", sym); if (sym !~ /^(pq_cpu_|pq_parquet_|oglpqcpu_)/) print sym }' \
    | head -n 20)"
if [[ -n "${BAD_SYMS}" ]]; then
    echo "ERROR: un-isolated symbols remained after objcopy: ${BAD_SYMS}" >&2
    exit 1
fi

mv -f "${TMP_DIR}/symbol-map.txt" "${MAP_FILE}"
mv -f "${TMP_DIR}/libog_lake_pq_cpu.a.tmp" "${ARCHIVE}"
