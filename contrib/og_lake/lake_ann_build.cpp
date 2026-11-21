/* Direct in-memory 2-pass Vamana construction with diverse multi-entry
 * landmarks and BFS reachability guarantee, exported directly to portable
 * Parquet graph storage without PG heap/WAL staging overhead. */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "lake_ann_build.h"
#include "lake_file_reader.h"
#include "lake_file_sdk.h"
#include "lake_inline_ann.h"
#include "lake_inline_reader.h"
#include "access/datavec/diskann.h"
#include "access/datavec/vector.h"
#include "catalog/pg_type.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include <algorithm>
#include <cfloat>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <vector>
#ifdef __x86_64__
#include <xmmintrin.h>
#endif

namespace {

struct LakeAnnHashEntry {
    uint64_t hash;
    uint32_t node_id;
    int32_t next;
};

struct LakeAnnInMemBuild {
    int metric;
    int dimension;
    int64 file_id;
    int64 physical_rows;
    int64 input_rows;
    int64 indexable_rows;
    int64 covered_rows;
    int64 posting_count;
    OgLakeAnnWriter* writer;
    OgLakeAnnReader* reader;
    std::vector<unsigned char> expected;
    std::vector<unsigned char> covered;
    std::vector<float> vectors; /* contiguous dim floats per node */
    std::vector<float> scratch_vec;
    std::vector<float> inv_norms; /* metric==2 cosine diversity helper */
    std::vector<std::vector<OgLakeAnnAddress>> postings;
    std::vector<std::vector<uint32_t>> edges;
    std::vector<uint8_t> is_entry;
    std::vector<uint32_t> entries;
    std::vector<int32_t> buckets;
    std::vector<LakeAnnHashEntry> hash_entries;
};

static inline uint64_t LakeSplitMix64(uint64_t x)
{
    x += 0x9e3779b97f4a7c15ULL;
    x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
    x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
    return x ^ (x >> 31);
}

static uint64_t LakeAnnHashVector(const float* v, int dim)
{
    uint64_t h = 0xcbf29ce484222325ULL ^ (uint64_t)dim;
    for (int d = 0; d < dim; ++d) {
        uint32_t bits = 0;
        std::memcpy(&bits, &v[d], 4);
        h = LakeSplitMix64(h ^ bits);
    }
    return h;
}

static inline float LakeAnnL2Sq(const float* a, const float* b, int dim)
{
#ifdef __x86_64__
    int i = 0;
    __m128 sum0 = _mm_setzero_ps();
    __m128 sum1 = _mm_setzero_ps();
    __m128 sum2 = _mm_setzero_ps();
    __m128 sum3 = _mm_setzero_ps();
    for (; i + 16 <= dim; i += 16) {
        __m128 d0 = _mm_sub_ps(_mm_loadu_ps(a + i), _mm_loadu_ps(b + i));
        __m128 d1 = _mm_sub_ps(_mm_loadu_ps(a + i + 4), _mm_loadu_ps(b + i + 4));
        __m128 d2 = _mm_sub_ps(_mm_loadu_ps(a + i + 8), _mm_loadu_ps(b + i + 8));
        __m128 d3 = _mm_sub_ps(_mm_loadu_ps(a + i + 12), _mm_loadu_ps(b + i + 12));
        sum0 = _mm_add_ps(sum0, _mm_mul_ps(d0, d0));
        sum1 = _mm_add_ps(sum1, _mm_mul_ps(d1, d1));
        sum2 = _mm_add_ps(sum2, _mm_mul_ps(d2, d2));
        sum3 = _mm_add_ps(sum3, _mm_mul_ps(d3, d3));
    }
    sum0 = _mm_add_ps(_mm_add_ps(sum0, sum2), _mm_add_ps(sum1, sum3));
    for (; i + 4 <= dim; i += 4) {
        __m128 d = _mm_sub_ps(_mm_loadu_ps(a + i), _mm_loadu_ps(b + i));
        sum0 = _mm_add_ps(sum0, _mm_mul_ps(d, d));
    }
    __m128 hi = _mm_movehl_ps(sum0, sum0);
    sum0 = _mm_add_ps(sum0, hi);
    hi = _mm_shuffle_ps(sum0, sum0, 0x55);
    sum0 = _mm_add_ss(sum0, hi);
    float acc = _mm_cvtss_f32(sum0);
    for (; i < dim; ++i) {
        float diff = a[i] - b[i];
        acc += diff * diff;
    }
    return acc;
#else
    float acc = 0.0f;
    for (int i = 0; i < dim; ++i) {
        float diff = a[i] - b[i];
        acc += diff * diff;
    }
    return acc;
#endif
}

static inline float LakeAnnDot(const float* a, const float* b, int dim)
{
#ifdef __x86_64__
    int i = 0;
    __m128 sum0 = _mm_setzero_ps();
    __m128 sum1 = _mm_setzero_ps();
    __m128 sum2 = _mm_setzero_ps();
    __m128 sum3 = _mm_setzero_ps();
    for (; i + 16 <= dim; i += 16) {
        sum0 = _mm_add_ps(sum0, _mm_mul_ps(_mm_loadu_ps(a + i), _mm_loadu_ps(b + i)));
        sum1 = _mm_add_ps(sum1, _mm_mul_ps(_mm_loadu_ps(a + i + 4), _mm_loadu_ps(b + i + 4)));
        sum2 = _mm_add_ps(sum2, _mm_mul_ps(_mm_loadu_ps(a + i + 8), _mm_loadu_ps(b + i + 8)));
        sum3 = _mm_add_ps(sum3, _mm_mul_ps(_mm_loadu_ps(a + i + 12), _mm_loadu_ps(b + i + 12)));
    }
    sum0 = _mm_add_ps(_mm_add_ps(sum0, sum2), _mm_add_ps(sum1, sum3));
    for (; i + 4 <= dim; i += 4) {
        sum0 = _mm_add_ps(sum0, _mm_mul_ps(_mm_loadu_ps(a + i), _mm_loadu_ps(b + i)));
    }
    __m128 hi = _mm_movehl_ps(sum0, sum0);
    sum0 = _mm_add_ps(sum0, hi);
    hi = _mm_shuffle_ps(sum0, sum0, 0x55);
    sum0 = _mm_add_ss(sum0, hi);
    float acc = _mm_cvtss_f32(sum0);
    for (; i < dim; ++i) {
        acc += a[i] * b[i];
    }
    return acc;
#else
    float acc = 0.0f;
    for (int i = 0; i < dim; ++i) {
        acc += a[i] * b[i];
    }
    return acc;
#endif
}

static inline float LakeAnnDistance(const LakeAnnInMemBuild* b, uint32_t u, uint32_t v)
{
    const float* pa = b->vectors.data() + (size_t)u * b->dimension;
    const float* pb = b->vectors.data() + (size_t)v * b->dimension;
    if (b->metric == 1) return LakeAnnL2Sq(pa, pb, b->dimension);
    return -LakeAnnDot(pa, pb, b->dimension);
}

static inline float LakeAnnDiversityDistance(const LakeAnnInMemBuild* b, uint32_t u, uint32_t v)
{
    const float* pa = b->vectors.data() + (size_t)u * b->dimension;
    const float* pb = b->vectors.data() + (size_t)v * b->dimension;
    if (b->metric == 1) return LakeAnnL2Sq(pa, pb, b->dimension);
    if (b->metric == 3) return std::max(0.0f, 1.0f - LakeAnnDot(pa, pb, b->dimension));
    /* Metric 2 (IP): angular diversity scaled robustly so negative dots never invert alpha. */
    float denom = b->inv_norms[u] * b->inv_norms[v];
    if (denom <= 0.0f) return LakeAnnL2Sq(pa, pb, b->dimension);
    return std::max(0.0f, 1.0f - LakeAnnDot(pa, pb, b->dimension) * denom);
}

static void LakeAnnSetBit(std::vector<unsigned char>& bits, int64 pos)
{
    bits[pos / 8] |= (unsigned char)(1U << (pos % 8));
}

static bool LakeAnnBit(const std::vector<unsigned char>& bits, int64 pos)
{
    return (bits[pos / 8] & (1U << (pos % 8))) != 0;
}

static void LakeAnnRehash(LakeAnnInMemBuild* b, size_t newMaskSize)
{
    b->buckets.assign(newMaskSize, -1);
    size_t mask = newMaskSize - 1;
    for (size_t i = 0; i < b->hash_entries.size(); ++i) {
        size_t slot = (size_t)(b->hash_entries[i].hash & mask);
        b->hash_entries[i].next = b->buckets[slot];
        b->buckets[slot] = (int32_t)i;
    }
}

static void LakeAnnCollectRow(const LakeInlineRowIdentity* row, Datum value, bool absent, void* argument)
{
    LakeAnnInMemBuild* b = (LakeAnnInMemBuild*)argument;
    if (row->file_id != (uint64)b->file_id || row->file_position != (uint64)b->input_rows ||
        b->input_rows >= b->physical_rows || row->row_id < 0 || row->begin_snapshot < 0)
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("ANN staging source identity is invalid")));
    ++b->input_rows;
    if ((b->input_rows & 4095) == 0) CHECK_FOR_INTERRUPTS();
    if (absent) return;

    Vector* vector = DatumGetVector(PG_DETOAST_DATUM(value));
    if (vector->dim != b->dimension) {
        if ((Pointer)vector != DatumGetPointer(value)) pfree(vector);
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("ANN staging vector dimension changed")));
    }
    double sq = 0.0;
    for (int d = 0; d < b->dimension; ++d) {
        float x = vector->x[d];
        if (!std::isfinite(x)) {
            if ((Pointer)vector != DatumGetPointer(value)) pfree(vector);
            ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("ANN staging vector must be finite")));
        }
        sq += (double)x * (double)x;
        b->scratch_vec[d] = x;
    }
    if ((Pointer)vector != DatumGetPointer(value)) pfree(vector);

    if (b->metric == 3) {
        if (sq <= 0.0) return;
        float inv = (float)(1.0 / std::sqrt(sq));
        for (int d = 0; d < b->dimension; ++d) {
            float val = b->scratch_vec[d] * inv;
            b->scratch_vec[d] = (val == 0.0f) ? 0.0f : val;
        }
    } else {
        for (int d = 0; d < b->dimension; ++d) {
            if (b->scratch_vec[d] == 0.0f) b->scratch_vec[d] = 0.0f;
        }
    }

    LakeAnnSetBit(b->expected, row->file_position);
    ++b->indexable_rows;

    OgLakeAnnAddress addr = {b->file_id, (int64_t)row->file_position, row->row_id, row->begin_snapshot};
    uint64_t h = LakeAnnHashVector(b->scratch_vec.data(), b->dimension);
    if (b->buckets.empty()) {
        size_t init = 1024;
        while (init < (size_t)std::max<int64>(b->physical_rows * 2, 1024)) init <<= 1;
        LakeAnnRehash(b, init);
    } else if (b->hash_entries.size() * 2 >= b->buckets.size()) {
        LakeAnnRehash(b, b->buckets.size() * 2);
    }

    size_t mask = b->buckets.size() - 1;
    size_t slot = (size_t)(h & mask);
    for (int32_t idx = b->buckets[slot]; idx >= 0; idx = b->hash_entries[idx].next) {
        if (b->hash_entries[idx].hash != h) continue;
        uint32_t nid = b->hash_entries[idx].node_id;
        if (b->postings[nid].size() >= OG_LAKE_ANN_MAX_POSTINGS) continue;
        const float* existing = b->vectors.data() + (size_t)nid * b->dimension;
        if (std::memcmp(existing, b->scratch_vec.data(), (size_t)b->dimension * sizeof(float)) == 0) {
            b->postings[nid].push_back(addr);
            ++b->posting_count;
            return;
        }
    }

    uint32_t nid = (uint32_t)b->postings.size();
    b->vectors.insert(b->vectors.end(), b->scratch_vec.begin(), b->scratch_vec.end());
    b->postings.emplace_back();
    b->postings.back().push_back(addr);
    if (b->metric == 2) b->inv_norms.push_back(sq > 0.0 ? (float)(1.0 / std::sqrt(sq)) : 0.0f);
    ++b->posting_count;

    LakeAnnHashEntry entry = {h, nid, b->buckets[slot]};
    b->buckets[slot] = (int32_t)b->hash_entries.size();
    b->hash_entries.push_back(entry);
}

struct LakeCandDist {
    float dist;
    uint32_t id;
    bool expanded;
};

static void LakeGreedySearch(const LakeAnnInMemBuild* b, uint32_t queryNode, uint32_t L,
    std::vector<uint32_t>& visitedGen, uint32_t gen,
    std::vector<LakeCandDist>& frontier, std::vector<std::pair<float, uint32_t>>& visitedOut)
{
    frontier.clear();
    visitedOut.clear();
    const uint32_t N = (uint32_t)b->postings.size();
    if (N == 0 || L == 0) return;

    auto insertCand = [&](uint32_t target, float d) {
        visitedOut.push_back({d, target});
        if (frontier.size() == L && d >= frontier.back().dist) return;
        size_t pos = frontier.size();
        for (size_t i = 0; i < frontier.size(); ++i) {
            if (frontier[i].id == target) return;
            if (d < frontier[i].dist && pos == frontier.size()) pos = i;
        }
        if (frontier.size() < L) {
            frontier.push_back({d, target, false});
            for (size_t i = frontier.size() - 1; i > pos; --i) frontier[i] = frontier[i - 1];
            frontier[pos] = {d, target, false};
        } else {
            for (size_t i = frontier.size() - 1; i > pos; --i) frontier[i] = frontier[i - 1];
            frontier[pos] = {d, target, false};
        }
    };

    for (uint32_t eid : b->entries) {
        if (eid == queryNode) continue;
        visitedGen[eid] = gen;
        insertCand(eid, LakeAnnDistance(b, queryNode, eid));
    }
    if (frontier.empty()) {
        for (uint32_t fallback = 0; fallback < N; ++fallback) {
            if (fallback != queryNode) {
                visitedGen[fallback] = gen;
                insertCand(fallback, LakeAnnDistance(b, queryNode, fallback));
                break;
            }
        }
    }

    while (true) {
        size_t bestIdx = frontier.size();
        for (size_t i = 0; i < frontier.size(); ++i) {
            if (!frontier[i].expanded) {
                bestIdx = i;
                break;
            }
        }
        if (bestIdx == frontier.size()) break;
        frontier[bestIdx].expanded = true;
        uint32_t cur = frontier[bestIdx].id;
        const auto& nbrs = b->edges[cur];
        for (size_t j = 0; j < nbrs.size(); ++j) {
            uint32_t w = nbrs[j];
            if (w == queryNode || visitedGen[w] == gen) continue;
            visitedGen[w] = gen;
#ifdef __x86_64__
            if (j + 1 < nbrs.size()) {
                _mm_prefetch((const char*)(b->vectors.data() + (size_t)nbrs[j + 1] * b->dimension), _MM_HINT_T0);
            }
#endif
            float d = LakeAnnDistance(b, queryNode, w);
            insertCand(w, d);
        }
    }
}

static void LakeRobustPrune(const LakeAnnInMemBuild* b, uint32_t u,
    std::vector<std::pair<float, uint32_t>>& pool, float alpha, uint32_t R,
    std::vector<uint32_t>& outEdges, std::vector<uint32_t>& seenGen, uint32_t gen)
{
    for (uint32_t ex : outEdges) {
        if (ex != u) pool.push_back({LakeAnnDistance(b, u, ex), ex});
    }
    std::sort(pool.begin(), pool.end(), [](const std::pair<float, uint32_t>& a, const std::pair<float, uint32_t>& c) {
        if (a.first != c.first) return a.first < c.first;
        return a.second < c.second;
    });

    size_t uniqueEnd = 0;
    for (size_t i = 0; i < pool.size(); ++i) {
        uint32_t cid = pool[i].second;
        if (cid == u || seenGen[cid] == gen) continue;
        seenGen[cid] = gen;
        pool[uniqueEnd++] = pool[i];
        if (uniqueEnd >= (size_t)R * 3 + 32) break;
    }
    pool.resize(uniqueEnd);

    outEdges.clear();
    if (pool.empty()) return;

    std::vector<float> candDivDist;
    if (b->metric != 1) {
        candDivDist.resize(pool.size());
        for (size_t i = 0; i < pool.size(); ++i) {
            candDivDist[i] = LakeAnnDiversityDistance(b, u, pool[i].second);
        }
    }

    float curAlpha = 1.0f;
    std::vector<uint8_t> occluded(pool.size(), 0);
    while (outEdges.size() < R && curAlpha <= alpha + 1e-4f) {
        for (size_t i = 0; i < pool.size() && outEdges.size() < R; ++i) {
            if (occluded[i]) continue;
            uint32_t p1 = pool[i].second;
            outEdges.push_back(p1);
            occluded[i] = 1;
            if (outEdges.size() >= R) break;
            for (size_t j = i + 1; j < pool.size(); ++j) {
                if (occluded[j]) continue;
                uint32_t p2 = pool[j].second;
                if (b->metric == 1) {
                    float d12 = LakeAnnDistance(b, p1, p2);
                    if ((double)curAlpha * curAlpha * d12 < (double)pool[j].first) occluded[j] = 1;
                } else {
                    float d12 = LakeAnnDiversityDistance(b, p1, p2);
                    float effAlpha = (b->metric == 2) ? 1.0f : curAlpha;
                    if ((double)effAlpha * d12 < (double)candDivDist[j]) occluded[j] = 1;
                }
            }
        }
        if (curAlpha < alpha) {
            curAlpha = alpha;
            std::fill(occluded.begin(), occluded.end(), 0);
            for (size_t i = 0; i < pool.size(); ++i) {
                for (uint32_t chosen : outEdges) {
                    if (pool[i].second == chosen) {
                        occluded[i] = 1;
                        break;
                    }
                }
            }
        } else {
            break;
        }
    }
}

static void LakeAnnBuildGraph(LakeAnnInMemBuild* b)
{
    const uint32_t N = (uint32_t)b->postings.size();
    b->edges.assign(N, {});
    b->is_entry.assign(N, 0);
    b->entries.clear();
    if (N == 0) return;

    if (N <= 65) {
        b->is_entry[0] = 1;
        b->entries.push_back(0);
        for (uint32_t u = 0; u < N; ++u) {
            b->edges[u].reserve(N - 1);
            for (uint32_t v = 0; v < N; ++v) {
                if (u != v) b->edges[u].push_back(v);
            }
        }
        return;
    }

    const uint32_t dim = (uint32_t)b->dimension;
    const uint32_t R = (dim >= 256) ? 48U : 64U;
    const uint32_t L1 = 64U;
    const uint32_t L2 = (dim >= 512) ? 80U : 100U;
    const uint32_t E = std::min<uint32_t>(N, std::min<uint32_t>(64U, std::max<uint32_t>(1U, N / 128U)));

    /* Diverse farthest-first landmark selection over stratified sample <= 2048 nodes. */
    std::vector<uint32_t> sampleIds;
    uint32_t sampleCount = std::min<uint32_t>(N, 2048U);
    sampleIds.reserve(sampleCount);
    for (uint32_t i = 0; i < sampleCount; ++i) {
        sampleIds.push_back((uint32_t)((uint64_t)i * N / sampleCount));
    }
    std::vector<double> centroid(dim, 0.0);
    uint32_t cSample = std::min<uint32_t>(sampleCount, 1024U);
    for (uint32_t i = 0; i < cSample; ++i) {
        const float* v = b->vectors.data() + (size_t)sampleIds[i] * dim;
        for (uint32_t d = 0; d < dim; ++d) centroid[d] += v[d];
    }
    std::vector<float> mean(dim, 0.0f);
    for (uint32_t d = 0; d < dim; ++d) mean[d] = (float)(centroid[d] / std::max<uint32_t>(1U, cSample));

    uint32_t e0 = sampleIds[0];
    double bestDist = DBL_MAX;
    for (uint32_t id : sampleIds) {
        const float* v = b->vectors.data() + (size_t)id * dim;
        double dist = (b->metric == 1) ? (double)LakeAnnL2Sq(v, mean.data(), (int)dim)
                                       : -(double)LakeAnnDot(v, mean.data(), (int)dim);
        if (dist < bestDist) {
            bestDist = dist;
            e0 = id;
        }
    }
    b->is_entry[e0] = 1;
    b->entries.push_back(e0);

    std::vector<float> minEntryDist(sampleCount, FLT_MAX);
    for (uint32_t i = 0; i < sampleCount; ++i) {
        if (sampleIds[i] == e0) {
            minEntryDist[i] = -FLT_MAX;
            continue;
        }
        minEntryDist[i] = LakeAnnDiversityDistance(b, sampleIds[i], e0);
    }
    while (b->entries.size() < E) {
        if (InterruptPending) return;
        uint32_t bestIdx = 0;
        float farthest = -FLT_MAX;
        for (uint32_t i = 0; i < sampleCount; ++i) {
            if (!b->is_entry[sampleIds[i]] && minEntryDist[i] > farthest) {
                farthest = minEntryDist[i];
                bestIdx = i;
            }
        }
        uint32_t nextEntry = sampleIds[bestIdx];
        if (b->is_entry[nextEntry]) break;
        b->is_entry[nextEntry] = 1;
        b->entries.push_back(nextEntry);
        for (uint32_t i = 0; i < sampleCount; ++i) {
            if (b->is_entry[sampleIds[i]]) continue;
            float d = LakeAnnDiversityDistance(b, sampleIds[i], nextEntry);
            if (d < minEntryDist[i]) minEntryDist[i] = d;
        }
    }

    /* Random seed graph of degree ~R/2 so greedy exploration starts well-mixed. */
    uint32_t seedDeg = std::min<uint32_t>(R / 2, N - 1);
    uint64_t rng = 0x517cc1b727220a95ULL ^ ((uint64_t)N << 32) ^ dim;
    for (uint32_t u = 0; u < N; ++u) {
        b->edges[u].reserve(R + 4);
        while (b->edges[u].size() < seedDeg) {
            rng = LakeSplitMix64(rng);
            uint32_t target = (uint32_t)(rng % N);
            if (target == u) continue;
            bool dup = false;
            for (uint32_t ex : b->edges[u]) if (ex == target) { dup = true; break; }
            if (!dup) b->edges[u].push_back(target);
        }
    }

    std::vector<uint32_t> perm(N);
    for (uint32_t i = 0; i < N; ++i) perm[i] = i;
    std::vector<uint32_t> visitedGen(N, 0), seenGen(N, 0);
    uint32_t vGen = 1, sGen = 1;
    std::vector<LakeCandDist> frontier;
    std::vector<std::pair<float, uint32_t>> pool;

    for (int pass = 0; pass < 2; ++pass) {
        const float alpha = (pass == 0) ? 1.0f : 1.2f;
        const uint32_t L = (pass == 0) ? L1 : L2;
        for (uint32_t i = N; i > 1; --i) {
            rng = LakeSplitMix64(rng);
            uint32_t j = (uint32_t)(rng % i);
            std::swap(perm[i - 1], perm[j]);
        }
        for (uint32_t idx = 0; idx < N; ++idx) {
            if ((idx & 63U) == 0 && InterruptPending) return;
            uint32_t u = perm[idx];
            if (++vGen == 0) { std::fill(visitedGen.begin(), visitedGen.end(), 0); vGen = 1; }
            LakeGreedySearch(b, u, L, visitedGen, vGen, frontier, pool);
            if (++sGen == 0) { std::fill(seenGen.begin(), seenGen.end(), 0); sGen = 1; }
            LakeRobustPrune(b, u, pool, alpha, R, b->edges[u], seenGen, sGen);

            for (uint32_t v : b->edges[u]) {
                auto& rev = b->edges[v];
                bool present = false;
                for (uint32_t x : rev) if (x == u) { present = true; break; }
                if (present) continue;
                if (rev.size() < R) {
                    rev.push_back(u);
                } else {
                    pool.clear();
                    pool.push_back({LakeAnnDistance(b, v, u), u});
                    if (++sGen == 0) { std::fill(seenGen.begin(), seenGen.end(), 0); sGen = 1; }
                    LakeRobustPrune(b, v, pool, alpha, R, rev, seenGen, sGen);
                }
            }
        }
    }

    /* Guarantee 100% reachability from entries via BFS connectivity repair. */
    std::vector<uint8_t> reached(N, 0);
    std::vector<uint32_t> bfs;
    bfs.reserve(N);
    for (uint32_t e : b->entries) {
        if (!reached[e]) { reached[e] = 1; bfs.push_back(e); }
    }
    size_t head = 0;
    while (head < bfs.size()) {
        uint32_t cur = bfs[head++];
        for (uint32_t w : b->edges[cur]) {
            if (!reached[w]) { reached[w] = 1; bfs.push_back(w); }
        }
    }
    if (bfs.size() < N) {
        for (uint32_t u = 0; u < N; ++u) {
            if (reached[u]) continue;
            uint32_t bridge = b->entries[u % b->entries.size()];
            if (b->edges[bridge].size() >= OG_LAKE_ANN_MAX_DEGREE) {
                for (uint32_t cand : bfs) {
                    if (b->edges[cand].size() < OG_LAKE_ANN_MAX_DEGREE) {
                        bridge = cand;
                        break;
                    }
                }
            }
            if (b->edges[bridge].size() >= OG_LAKE_ANN_MAX_DEGREE) b->edges[bridge].pop_back();
            b->edges[bridge].push_back(u);
            if (b->edges[u].size() < OG_LAKE_ANN_MAX_DEGREE) {
                bool dup = false;
                for (uint32_t ex : b->edges[u]) if (ex == bridge) { dup = true; break; }
                if (!dup) b->edges[u].push_back(bridge);
            }
            reached[u] = 1;
            bfs.push_back(u);
            while (head < bfs.size()) {
                uint32_t cur = bfs[head++];
                for (uint32_t w : b->edges[cur]) {
                    if (!reached[w]) { reached[w] = 1; bfs.push_back(w); }
                }
            }
        }
    }
}

static LakeFileCandidate* LakeAnnCandidate(const char* directory, bool lookup)
{
    int32 count = 0;
    const OgLakeSdkField* fields = og_lake_ann_fields(lookup ? 1 : 0, &count);
    OgLakeSdkWriteOptions options = {};
    options.struct_size = sizeof(options);
    options.path = psprintf("%s/%s", directory, lookup ? OG_LAKE_ANN_LOOKUP_FILE : OG_LAKE_ANN_NODES_FILE);
    options.limits = LakeFileLimits();
    options.num_fields = count;
    options.fields = fields;
    options.compression = OG_LAKE_SDK_ZSTD;
    options.row_group_rows = OG_LAKE_ANN_PAGE_ROWS;
    return LakeFileCandidateOpen(&options);
}

static void LakeAnnCloseBuild(LakeAnnInMemBuild* b)
{
    if (!b) return;
    HOLD_INTERRUPTS();
    og_lake_ann_reader_close(&b->reader);
    og_lake_ann_writer_close(&b->writer);
    RESUME_INTERRUPTS();
    delete b;
}

} /* namespace */

void LakeBuildAnnFileSegment(LakeInlineBinding* binding, int fileIndex, int column, int metric,
    const char* version, const char* context, const char* directory, LakeFileCandidate** nodes,
    LakeFileCandidate** lookup, OgLakeAnnWritten* written)
{
    if (!binding || fileIndex < 0 || fileIndex >= binding->file_count || column < 0 || column >= binding->column_count ||
        binding->columns[column].type != VECTOROID || metric < 1 || metric > 3 || !version || !context ||
        !directory || !nodes || !lookup || !written)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid native ANN file build")));
    int64 physicalRows = binding->files[fileIndex].record_count;
    int dimension = binding->columns[column].typmod;
    int64 fileId = binding->files[fileIndex].file_id;
    /* Keep the segment coverage cap consistent with portable format1 pages. */
    if (dimension < 1 || dimension > (int)OG_LAKE_ANN_MAX_DIM || fileId < 0 || physicalRows < 0 ||
        physicalRows > (int64)OG_LAKE_ANN_MAX_PAGES * OG_LAKE_ANN_PAGE_ROWS - FROZEN_POINT_SIZE)
        ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED), errmsg("ANN source exceeds one qualified graph segment")));
    LakeAnnCheckMethod();

    LakeAnnInMemBuild* volatile build = new LakeAnnInMemBuild();
    build->metric = metric;
    build->dimension = dimension;
    build->file_id = fileId;
    build->physical_rows = physicalRows;
    size_t bitBytes = (size_t)std::max<int64>((physicalRows + 7) / 8, 1);
    build->expected.assign(bitBytes, 0);
    build->covered.assign(bitBytes, 0);
    build->scratch_vec.assign((size_t)dimension, 0.0f);

    PG_TRY();
    {
        LakeFileVisitAnnRows(binding, fileIndex, column + 1, version, LakeAnnCollectRow, build);
        if (build->input_rows != build->physical_rows)
            ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("ANN staging input lacks complete physical coverage")));

        LakeAnnBuildGraph(build);
        CHECK_FOR_INTERRUPTS();

        uint32_t nodeCount = (uint32_t)build->postings.size();
        uint32_t entryCount = (uint32_t)build->entries.size();
        OgLakeAnnSpec spec = {sizeof(spec), OG_LAKE_ANN_FORMAT_VERSION, (uint32_t)dimension, (uint32_t)metric,
            nodeCount, entryCount, (int64_t)build->posting_count, context};

        *nodes = LakeAnnCandidate(directory, false);
        *lookup = LakeAnnCandidate(directory, true);
        OgLakeSdkLimits limits = LakeFileLimits();
        OgLakeSdkError error;
        HOLD_INTERRUPTS();
        int status = og_lake_ann_writer_open((*nodes)->api, &limits, &spec,
            (*nodes)->writer, (*lookup)->writer, &build->writer, &error);
        RESUME_INTERRUPTS();
        LakeFileCheckStatus(status, &error);
        CHECK_FOR_INTERRUPTS();

        OgLakeAnnNode batch[OG_LAKE_ANN_PAGE_ROWS];
        uint32_t batchCount = 0;
        for (uint32_t u = 0; u < nodeCount; ++u) {
            for (const auto& addr : build->postings[u]) {
                if (addr.file_id != fileId || addr.row_position >= physicalRows || !LakeAnnBit(build->expected, addr.row_position))
                    ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED), errmsg("native ANN posting leaves its physical source")));
                if (!LakeAnnBit(build->covered, addr.row_position)) {
                    LakeAnnSetBit(build->covered, addr.row_position);
                    ++build->covered_rows;
                }
            }
            OgLakeAnnNode& row = batch[batchCount++];
            row.node_id = u;
            row.entry = build->is_entry[u] ? 1U : 0U;
            row.deleted = 0U;
            row.neighbor_count = (uint32_t)build->edges[u].size();
            row.posting_count = (uint32_t)build->postings[u].size();
            row.vector = build->vectors.data() + (size_t)u * dimension;
            row.neighbors = row.neighbor_count ? build->edges[u].data() : NULL;
            row.postings = build->postings[u].data();

            if (batchCount == OG_LAKE_ANN_PAGE_ROWS || u + 1 == nodeCount) {
                HOLD_INTERRUPTS();
                status = og_lake_ann_writer_append(build->writer, batch, batchCount, &error);
                RESUME_INTERRUPTS();
                LakeFileCheckStatus(status, &error);
                CHECK_FOR_INTERRUPTS();
                batchCount = 0;
            }
        }
        /* Release large build memory before reader verification opens Parquet pages. */
        { std::vector<float>().swap(build->vectors); }
        { std::vector<float>().swap(build->inv_norms); }
        { std::vector<std::vector<OgLakeAnnAddress>>().swap(build->postings); }
        { std::vector<std::vector<uint32_t>>().swap(build->edges); }
        { std::vector<LakeAnnHashEntry>().swap(build->hash_entries); }
        { std::vector<int32_t>().swap(build->buckets); }

        if (build->covered_rows != build->indexable_rows || build->expected != build->covered)
            ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED), errmsg("native ANN graph lacks complete indexable physical coverage")));

        written->struct_size = sizeof(*written);
        HOLD_INTERRUPTS();
        status = og_lake_ann_writer_finish(build->writer, written, &error);
        RESUME_INTERRUPTS();
        LakeFileCheckStatus(status, &error);
        CHECK_FOR_INTERRUPTS();
        LakeFileCandidateFinish(*nodes);
        LakeFileCandidateFinish(*lookup);

        HOLD_INTERRUPTS();
        status = og_lake_ann_reader_open((*nodes)->api, &limits, &spec, written->manifest_hash,
            (*nodes)->path, (*lookup)->path, &build->reader, &error);
        RESUME_INTERRUPTS();
        LakeFileCheckStatus(status, &error);
        CHECK_FOR_INTERRUPTS();
        HOLD_INTERRUPTS();
        status = og_lake_ann_verify(build->reader, &error);
        RESUME_INTERRUPTS();
        LakeFileCheckStatus(status, &error);
        CHECK_FOR_INTERRUPTS();

        char* after = LakeFileObjectVersion(binding, fileIndex);
        if (!after || strcmp(after, version) != 0)
            ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("ANN source changed before artifact publication")));

        LakeAnnCloseBuild(build);
        build = NULL;
    }
    PG_CATCH();
    {
        LakeAnnCloseBuild(build);
        PG_RE_THROW();
    }
    PG_END_TRY();
}
