/* Test-only lifecycle barrier. All Parquet operations use the real provider.
 * The adapter pauses one abort before delegating it, to observe protection
 * while a real candidate handle/file still exists. Never install for normal
 * qualification. The generated configuration names immutable owned paths. */
#define _POSIX_C_SOURCE 200809L
#include "sdk/lake_parquet_sdk.h"
#include "barrier_config.h"
#include <dlfcn.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

static pthread_once_t initialized = PTHREAD_ONCE_INIT;
static atomic_flag paused = ATOMIC_FLAG_INIT;
static void* real_library;
static const OgLakeParquetSdkApi* real_api;
static const OgLakeParquetLifecycleApi* real_lifecycle;
static OgLakeParquetSdkApi forward_api;
static OgLakeParquetLifecycleApi forward_lifecycle;

static int marker(const char* name, int create) {
    char path[4096];
    int length = snprintf(path, sizeof(path), "%s/%s", OG_LAKE_BARRIER_DIR, name);
    if (length < 0 || (size_t)length >= sizeof(path)) return 0;
    if (!create) return access(path, F_OK) == 0;
    int fd = open(path, O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW, 0600);
    if (fd < 0) return 0;
    static const char value[] = "real provider abort lifecycle barrier\n";
    int ok = write(fd, value, sizeof(value) - 1) == sizeof(value) - 1;
    if (close(fd) != 0) ok = 0;
    return ok;
}

static int abort_with_barrier(OgLakeSdkWriter** writer, OgLakeSdkError* error) {
    int selected = marker("arm", 0) && !atomic_flag_test_and_set(&paused);
    if (selected && marker("arrived", 1)) {
        struct timespec started, now, delay = {0, 10000000};
        clock_gettime(CLOCK_MONOTONIC, &started);
        do {
            if (marker("continue", 0)) break;
            nanosleep(&delay, NULL);
            clock_gettime(CLOCK_MONOTONIC, &now);
        } while (now.tv_sec - started.tv_sec < 10);
        if (!marker("continue", 0)) marker("timeout", 1);
    }
    int status = real_lifecycle->writer_abort(writer, error);
    if (selected) marker("completed", 1);
    return status;
}

static void initialize(void) {
    real_library = dlopen(OG_LAKE_REAL_PROVIDER, RTLD_NOW | RTLD_LOCAL);
    if (!real_library) return;
    OgLakeParquetSdkEntrypoint base = (OgLakeParquetSdkEntrypoint)dlsym(real_library, "og_lake_parquet_sdk_v1");
    OgLakeParquetLifecycleEntrypoint lifecycle =
        (OgLakeParquetLifecycleEntrypoint)dlsym(real_library, "og_lake_parquet_sdk_v2");
    real_api = base ? base() : NULL;
    real_lifecycle = lifecycle ? lifecycle() : NULL;
    if (!real_api || real_api->struct_size < sizeof(*real_api) || !real_lifecycle ||
        real_lifecycle->struct_size < sizeof(*real_lifecycle) || real_lifecycle->base != real_api ||
        !real_lifecycle->writer_abort) {
        real_api = NULL;
        real_lifecycle = NULL;
        return;
    }
    forward_api = *real_api;
    forward_lifecycle = *real_lifecycle;
    forward_lifecycle.base = &forward_api;
    forward_lifecycle.writer_abort = abort_with_barrier;
}

__attribute__((visibility("default"))) const OgLakeParquetSdkApi* og_lake_parquet_sdk_v1(void) {
    pthread_once(&initialized, initialize);
    return real_api ? &forward_api : NULL;
}

__attribute__((visibility("default"))) const OgLakeParquetLifecycleApi* og_lake_parquet_sdk_v2(void) {
    pthread_once(&initialized, initialize);
    return real_lifecycle ? &forward_lifecycle : NULL;
}
