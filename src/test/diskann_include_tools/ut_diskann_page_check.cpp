/*
 * P0-1 L1 单元测试：DiskANN 页容量纯函数（独立编译，无后端依赖）
 *
 * 运行：g++ -std=c++11 -I<repo>/src/include -o ut_diskann_page_check ut_diskann_page_check.cpp && ./ut_diskann_page_check
 * 退出码 0 = 全部通过；非 0 = 失败（首个失败用例编号通过 stderr 报告）。
 *
 * 被测对象：src/include/access/datavec/diskannpagecheck.h
 * 对应设计文档 §9.3（页面空间检查）：完整页面占用 =
 *   MAXALIGN(itup) + MAXALIGN(node) + MAXALIGN(edge) + ItemId + MAXALIGN(opaque)
 *   且必须 <= BLCKSZ - MAXALIGN(PageHeader)
 */
#include <cstdio>
#include <cstddef>
#include <cstdint>
#include "access/datavec/diskannpagecheck.h"

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
    /* 与内核一致的典型页参数（8KB 页；对齐由调用方完成，此处传已对齐值） */
    const size_t BLCKSZ_8K = 8192;
    const size_t HDR = 24;            /* MAXALIGN(SizeOfPageHeaderData) 典型值 */
    const size_t ITEMID = 4;          /* sizeof(ItemIdData) */
    const size_t OPAQUE = 16;         /* MAXALIGN(sizeof(DiskAnnPageOpaqueData)) 典型值 */
    const size_t capacity = BLCKSZ_8K - HDR;

    /* 1. 恰好放满：required == capacity → fits */
    {
        size_t req = diskann_page_required_bytes(
            capacity - ITEMID - OPAQUE - 1024, /* tuple */
            700,                                /* node */
            324,                                /* edge */
            ITEMID, OPAQUE);
        CHECK(req == capacity, "exact-fit required == capacity");
        CHECK(diskann_page_fits(req, capacity), "exact-fit fits");
    }

    /* 2. 超 1 字节：required == capacity + 1 → 不放 */
    {
        size_t req = diskann_page_required_bytes(
            capacity - ITEMID - OPAQUE - 1024 + 1, 700, 324, ITEMID, OPAQUE);
        CHECK(req == capacity + 1, "one-over required == capacity+1");
        CHECK(!diskann_page_fits(req, capacity), "one-over does not fit");
    }

    /* 3. 1536 维 float32 向量的真实场景（设计文档 §9.1）：
     *    itup ≈ 6144+Header(~16) ≈ 6160，node(无PQ)≈？edge=772 */
    {
        size_t tuple = 6160;
        size_t node = 104;   /* offsetof(pqcode) 无 PQ 时较小，取代表值 */
        size_t edge = 772;   /* 96 边 × 8B */
        size_t req = diskann_page_required_bytes(tuple, node, edge, ITEMID, OPAQUE);
        /* 6160+104+772+4+16 = 7056 <= 8168 → 应放行 */
        CHECK(diskann_page_fits(req, capacity), "1536-dim no-PQ fits 8K");
    }

    /* 4. 同上再加 INCLUDE payload 到超限：tuple 增大 1200B */
    {
        size_t req = diskann_page_required_bytes(6160 + 1200, 104, 772, ITEMID, OPAQUE);
        CHECK(!diskann_page_fits(req, capacity), "1536-dim + 1200B payload rejected");
    }

    /* 5. 全零与极小值不崩溃、不误报 */
    {
        size_t req = diskann_page_required_bytes(0, 0, 0, 0, 0);
        CHECK(req == 0, "all-zero required == 0");
        CHECK(diskann_page_fits(req, capacity), "all-zero fits");
    }

    /* 6. size_t 溢出防御：各项均 < capacity 但总和溢出的构造不可行（size_t 上限），
     *    断言函数对合法最大单值（capacity）不溢出 */
    {
        size_t req = diskann_page_required_bytes(capacity, 0, 0, 0, 0);
        CHECK(req == capacity && diskann_page_fits(req, capacity),
              "single-component-at-capacity");
    }

    if (g_failures == 0) {
        std::printf("ut_diskann_page_check: ALL PASS\n");
        return 0;
    }
    std::printf("ut_diskann_page_check: %d FAILURES\n", g_failures);
    return 1;
}
