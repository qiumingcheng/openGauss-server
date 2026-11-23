#!/usr/bin/env bash
# Reconstructed-environment entry implementing the published migration runbook.
# build_driver.py owns admission, source/dependency identity and SQL verification.
set -euo pipefail
: "${CODE_BASE:?}" "${BINARYLIBS:?}" "${GAUSSHOME:?}" "${PGDATA:?}"
: "${PGPORT:?}" "${PGHOST:?}" "${OG_LAKE_BUILD_PROFILE:?}"
mode="${1:?full, incremental or restart required}"
case "$mode" in full|incremental|restart) ;; *) exit 2 ;; esac
case "$OG_LAKE_BUILD_PROFILE" in debug|release) ;; *) exit 2 ;; esac
gcc_root="$BINARYLIBS/buildtools/gcc10.3"
export PATH="$gcc_root/gcc/bin:$PATH"
export LD_LIBRARY_PATH="$GAUSSHOME/lib:$GAUSSHOME/lib/postgresql:$gcc_root/gcc/lib64:$gcc_root/isl/lib:$gcc_root/mpc/lib:$gcc_root/mpfr/lib:$gcc_root/gmp/lib:$BINARYLIBS/kernel/dependency/openssl/comm/lib"
if [[ -f "$PGDATA/postmaster.pid" ]]; then
    server_pid="$(head -n 1 "$PGDATA/postmaster.pid")"
    if kill -0 "$server_pid" 2>/dev/null; then
        [[ "$(readlink -f "/proc/$server_pid/exe")" == "$GAUSSHOME/bin/gaussdb" ]]
        "$GAUSSHOME/bin/gs_ctl" stop -D "$PGDATA" -m fast -w
    fi
fi
cd "$CODE_BASE"
if [[ "$mode" == full ]]; then
    # This entry requires a fresh worktree for each full build. It never cleans
    # an existing source tree or mixes generated Debug/Release configurations.
    [[ ! -e config.status ]] || { echo 'Full build requires an unconfigured worktree' >&2; exit 2; }
    flags=()
    optimization=-O2
    if [[ "$OG_LAKE_BUILD_PROFILE" == debug ]]; then
        flags+=(--enable-debug --enable-cassert)
        optimization=-O0
    fi
    env CC=g++ CFLAGS="$optimization" LDFLAGS="-L$gcc_root/gcc/lib64" \
        ./configure --gcc-version=10.3.0 --prefix="$GAUSSHOME" --3rd="$BINARYLIBS" \
        --enable-thread-safety --without-readline --without-zlib --without-libxml "${flags[@]}"
fi
if [[ "$mode" == full || "$mode" == incremental ]]; then
    make -s -j"${JOBS:-2}"
    if [[ -d "$GAUSSHOME/jre" ]]; then
        find "$GAUSSHOME/jre" -type f ! -writable -exec chmod u+w {} +
    fi
    make install -s -j1
    # Upstream install changes this tracked script's mode only.
    chmod 644 contrib/shark/smartmatch.pl
fi
if [[ ! -f "$PGDATA/PG_VERSION" ]]; then
    if [[ -d "$PGDATA" && -n "$(find "$PGDATA" -mindepth 1 -maxdepth 1 -print -quit)" ]]; then
        echo 'Refusing to initialize nonempty data directory' >&2
        exit 2
    fi
    mkdir -p "$PGHOST"
    password_file="$(mktemp "$(dirname "$PGDATA")/.init-password-XXXXXX")"
    trap 'rm -f "$password_file"' EXIT
    python3 -c 'import secrets; print("Og1!" + secrets.token_hex(12))' > "$password_file"
    "$GAUSSHOME/bin/gs_initdb" -D "$PGDATA" --nodename=og_native_lake \
        --dbcompatibility=PG --locale=C -E UTF8 -A trust --pwfile="$password_file"
    rm -f "$password_file"
    trap - EXIT
    cat >> "$PGDATA/postgresql.conf" <<EOF

# Isolated native lake reconstructed-environment instance.
listen_addresses = '127.0.0.1'
unix_socket_directory = '$PGHOST'
port = $PGPORT
shared_buffers = '128MB'
max_connections = 40
max_prepared_transactions = 10
work_mem = '16MB'
max_process_memory = '3GB'
enable_thread_pool = off
EOF
    chmod 600 "$PGDATA/postgresql.conf"
fi
"$GAUSSHOME/bin/gs_ctl" start -D "$PGDATA" -l "${SERVER_LOG:?}" -o "-p $PGPORT" -w -t 180
