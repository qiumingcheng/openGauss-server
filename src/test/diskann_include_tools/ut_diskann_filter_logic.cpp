#define DISKANN_FILTER_LOGIC_ONLY
/*
 * P2-2 L1 unit test: DiskANN in-filter three-valued logic (standalone).
 *
 * Build: g++ -std=c++11 -I<repo>/src/include -o ut_diskann_filter_logic ut_diskann_filter_logic.cpp
 * Exit 0 = all pass. The header under test only needs stdint/stdbool for
 * these helpers, so no backend link is required.
 */
#include <cstdio>
#include "access/datavec/diskannfilter.h"

/*
 * The header includes postgres.h in the real build; for the standalone
 * harness provide the minimal shims the pure helpers need.
 */
#ifndef DISKANN_FILTER_STANDALONE_SHIMS
#define DISKANN_FILTER_STANDALONE_SHIMS
#endif

static int g_failures = 0;
#define CHECK(cond, name)                                                     \
    do {                                                                       \
        if (!(cond)) {                                                         \
            std::fprintf(stderr, "FAIL: %s (line %d)\n", name, __LINE__);       \
            g_failures++;                                                      \
        }                                                                      \
    } while (0)

int main(void)
{
    using T = DiskAnnTriState;

    /* 1. strict op: NULL propagates either side */
    CHECK(diskann_tri_strict_op(true, false, true) == T::DISKANN_TRI_NULL, "strict value-null");
    CHECK(diskann_tri_strict_op(false, true, true) == T::DISKANN_TRI_NULL, "strict arg-null");
    CHECK(diskann_tri_strict_op(true, true, false) == T::DISKANN_TRI_NULL, "strict both-null");
    CHECK(diskann_tri_strict_op(false, false, true) == T::DISKANN_TRI_TRUE, "strict true");
    CHECK(diskann_tri_strict_op(false, false, false) == T::DISKANN_TRI_FALSE, "strict false");

    /* 2. NULL tests never return NULL */
    CHECK(diskann_tri_nulltest(true, false) == T::DISKANN_TRI_TRUE, "is-null on null");
    CHECK(diskann_tri_nulltest(false, false) == T::DISKANN_TRI_FALSE, "is-null on value");
    CHECK(diskann_tri_nulltest(true, true) == T::DISKANN_TRI_FALSE, "not-null on null");
    CHECK(diskann_tri_nulltest(false, true) == T::DISKANN_TRI_TRUE, "not-null on value");

    /* 3. ARRAY ANY: any TRUE wins; else any NULL -> NULL; else FALSE */
    {
        T allFalse[2] = {T::DISKANN_TRI_FALSE, T::DISKANN_TRI_FALSE};
        T falseNull[2] = {T::DISKANN_TRI_FALSE, T::DISKANN_TRI_NULL};
        T nullOnly[1] = {T::DISKANN_TRI_NULL};
        T trueSecond[3] = {T::DISKANN_TRI_FALSE, T::DISKANN_TRI_TRUE, T::DISKANN_TRI_NULL};
        CHECK(diskann_tri_array_any(2, allFalse, false) == T::DISKANN_TRI_FALSE, "any all-false");
        CHECK(diskann_tri_array_any(2, falseNull, false) == T::DISKANN_TRI_NULL, "any false+null");
        CHECK(diskann_tri_array_any(1, nullOnly, false) == T::DISKANN_TRI_NULL, "any null-only");
        CHECK(diskann_tri_array_any(3, trueSecond, false) == T::DISKANN_TRI_TRUE, "any true wins");
        CHECK(diskann_tri_array_any(0, NULL, false) == T::DISKANN_TRI_FALSE, "empty array false");
        CHECK(diskann_tri_array_any(2, allFalse, true) == T::DISKANN_TRI_NULL, "null array null");
    }

    /* 4. Kleene AND */
    CHECK(diskann_tri_and(T::DISKANN_TRI_TRUE, T::DISKANN_TRI_TRUE) == T::DISKANN_TRI_TRUE, "and TT");
    CHECK(diskann_tri_and(T::DISKANN_TRI_FALSE, T::DISKANN_TRI_NULL) == T::DISKANN_TRI_FALSE, "and FN");
    CHECK(diskann_tri_and(T::DISKANN_TRI_NULL, T::DISKANN_TRI_NULL) == T::DISKANN_TRI_NULL, "and NN");
    CHECK(diskann_tri_and(T::DISKANN_TRI_TRUE, T::DISKANN_TRI_NULL) == T::DISKANN_TRI_NULL, "and TN");

    /* 5. admission: only TRUE */
    CHECK(diskann_tri_admits(T::DISKANN_TRI_TRUE), "admit true");
    CHECK(!diskann_tri_admits(T::DISKANN_TRI_FALSE), "reject false");
    CHECK(!diskann_tri_admits(T::DISKANN_TRI_NULL), "reject null");

    if (g_failures == 0) {
        std::printf("ut_diskann_filter_logic: ALL PASS\n");
        return 0;
    }
    std::printf("ut_diskann_filter_logic: %d FAILURES\n", g_failures);
    return 1;
}
