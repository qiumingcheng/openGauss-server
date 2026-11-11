/*
 * diskanncostmodel.h — DiskANN search-width estimation pure functions (P2-5)
 *
 * Design source: 多模_向标融合_openGauss_DiskANN包含列与索引内过滤详细设计.md §13.5.
 *
 * Pure helpers only (no backend deps beyond stdint/stdbool/math) so the
 * standalone unit test ut_diskann_costmodel.cpp compiles without the backend.
 */
#ifndef DISKANN_COSTMODEL_H
#define DISKANN_COSTMODEL_H

#define DISKANN_COSTMODEL_LOGIC_ONLY 1 /* default: standalone-friendly */

#include <stdint.h>
#include <stdbool.h>
#include <math.h>

/* [P2-5] Planning constants (design doc 13.5). Single definition point;
 * the AM cost estimator and EXPLAIN read these. Values are explainable
 * linear-approximation constants, calibrated later via G2 A/B/C data. */
#define DISKANN_COST_OVERSAMPLE 1.5      /* candidate oversampling factor */
#define DISKANN_COST_S_MIN 0.001         /* floor for filter selectivity */
#define DISKANN_COST_MAX_SEARCH_BUDGET 65536 /* hard L cap (pages factor base) */

/*
 * Estimate the initial graph search width for a filtered Top-K:
 *   L = ceil(K / max(s, s_min)) * oversample, clamped to [minWidth, maxWidth].
 * Monotonic: larger K or smaller s never decreases L; s<=0 or NaN falls back
 * to minWidth (no divide-by-zero, no inf).
 */
static inline int64_t diskann_cost_search_width(int64_t k, double s, int64_t minWidth, int64_t maxWidth)
{
    double eff;
    double raw;
    int64_t l;

    if (k <= 0) {
        return minWidth;
    }
    if (isnan(s) || s <= 0.0) {
        return minWidth; /* no estimate: conservative first-round width */
    }
    eff = s < DISKANN_COST_S_MIN ? DISKANN_COST_S_MIN : (s > 1.0 ? 1.0 : s);
    raw = ceil(((double)k / eff) * DISKANN_COST_OVERSAMPLE);
    if (raw < (double)minWidth) {
        return minWidth;
    }
    if (raw > (double)maxWidth) {
        return maxWidth;
    }
    l = (int64_t)raw;
    return l;
}

/*
 * Estimated heap candidates: C = clamp(K / max(s, s_min), K, N). Never
 * promises fewer heap visits than K, never more than the index holds.
 */
static inline int64_t diskann_cost_heap_candidates(int64_t k, double s, int64_t nTuples)
{
    double eff;
    double raw;

    if (k <= 0 || nTuples <= 0) {
        return 0;
    }
    if (isnan(s) || s <= 0.0) {
        return nTuples; /* unestimable: assume full scan of candidates */
    }
    eff = s < DISKANN_COST_S_MIN ? DISKANN_COST_S_MIN : (s > 1.0 ? 1.0 : s);
    raw = (double)k / eff;
    if (raw < (double)k) {
        return k;
    }
    if (raw > (double)nTuples) {
        return nTuples;
    }
    return (int64_t)raw;
}

#endif /* DISKANN_COSTMODEL_H */
