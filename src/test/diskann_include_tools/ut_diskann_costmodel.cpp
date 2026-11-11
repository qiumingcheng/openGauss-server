/*
 * P2-5 L1 unit test: DiskANN cost-model pure helpers (standalone).
 * Build: g++ -std=c++11 -o ut_diskann_costmodel ut_diskann_costmodel.cpp
 */
#include <cstdio>
#include <cmath>
#include "access/datavec/diskanncostmodel.h"

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
    const int64_t probes = 128;
    const int64_t maxB = DISKANN_COST_MAX_SEARCH_BUDGET;

    /* 1. K monotonicity: p fixed, K up -> L not smaller */
    CHECK(diskann_cost_search_width(10, 0.1, probes, maxB) <=
          diskann_cost_search_width(100, 0.1, probes, maxB), "K monotone");

    /* 2. s monotonicity: K fixed, s down -> L not smaller */
    CHECK(diskann_cost_search_width(10, 0.5, probes, maxB) <=
          diskann_cost_search_width(10, 0.01, probes, maxB), "s monotone");

    /* 3. exact math: K=10, s=0.1 -> ceil(100*1.5)=150 */
    CHECK(diskann_cost_search_width(10, 0.1, probes, maxB) == 150, "exact 150");

    /* 4. clamp to minWidth when formula is smaller */
    CHECK(diskann_cost_search_width(1, 1.0, probes, maxB) == probes, "clamp min");

    /* 5. clamp to maxWidth under extreme filter */
    CHECK(diskann_cost_search_width(1000000, 1e-9, probes, maxB) == maxB, "clamp max");

    /* 6. degenerate inputs: s<=0 / NaN / K<=0 -> minWidth, no div0/inf */
    CHECK(diskann_cost_search_width(10, 0.0, probes, maxB) == probes, "s=0 fallback");
    CHECK(diskann_cost_search_width(10, NAN, probes, maxB) == probes, "NaN fallback");
    CHECK(diskann_cost_search_width(0, 0.1, probes, maxB) == probes, "K=0 fallback");
    CHECK(diskann_cost_search_width(-5, 0.1, probes, maxB) == probes, "K<0 fallback");

    /* 7. s>1 clamps to 1.0 */
    CHECK(diskann_cost_search_width(100, 5.0, probes, maxB) ==
          diskann_cost_search_width(100, 1.0, probes, maxB), "s>1 clamp");

    /* 8. heap candidates: C>=K always, <=N always */
    CHECK(diskann_cost_heap_candidates(10, 1.0, 1000) == 10, "C=K at s=1");
    CHECK(diskann_cost_heap_candidates(10, 0.001, 1000) == 1000, "C<=N");
    CHECK(diskann_cost_heap_candidates(10, 1e-9, 50) == 50, "C clamp N");
    CHECK(diskann_cost_heap_candidates(0, 0.1, 1000) == 0, "K=0 C=0");
    CHECK(diskann_cost_heap_candidates(10, 0.0, 1000) == 1000, "s=0 -> N");
    CHECK(diskann_cost_heap_candidates(10, NAN, 1000) == 1000, "NaN -> N");

    /* 9. C monotone in s (down) */
    CHECK(diskann_cost_heap_candidates(10, 0.5, 1000) <=
          diskann_cost_heap_candidates(10, 0.1, 1000), "C monotone");

    if (g_failures == 0) {
        std::printf("ut_diskann_costmodel: ALL PASS\n");
        return 0;
    }
    std::printf("ut_diskann_costmodel: %d FAILURES\n", g_failures);
    return 1;
}
