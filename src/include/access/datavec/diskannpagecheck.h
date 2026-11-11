/*
 * diskannpagecheck.h — DiskANN 节点页容量纯函数（P0-1）
 *
 * 设计来源：多模_向标融合_openGauss_DiskANN包含列与索引内过滤详细设计.md §9.3。
 *
 * 本文件必须保持无后端依赖（仅 <stddef.h>/<stdint.h>），使其可以脱离
 * openGauss 构建树独立编译单元测试（src/test/diskann_include_tools/
 * ut_diskann_page_check.cpp）。
 *
 * 语义约定（与内核调用方 diskannbuild.cpp::InsertVectorIntoPage 的使用一致）：
 *   - 传入的 tupleSize/nodeSize/edgeSize/opaqueSize 均为"已 MAXALIGN 的字节数"，
 *     对齐由内核侧完成，保证本函数只做纯加法与比较；
 *   - 一个 DiskANN 节点页的完整占用 =
 *       itup + node + edge（经 ReserveSpace 预留在页尾）+ ItemId + page opaque；
 *   - pageSize 传 BLCKSZ，headerSize 传 MAXALIGN(SizeOfPageHeaderData)，
 *     可用容量 = pageSize - headerSize；
 *   - 判定采用 "> 可用容量 即 不放行"（恰好放满允许）。
 *
 * P1-3 起 INCLUDE 列使 itup 变大后，同一公式继续成立，不得另建第二套口径。
 */
#ifndef DISKANN_PAGE_CHECK_H
#define DISKANN_PAGE_CHECK_H

#include <stddef.h>

/* 计算一个 DiskANN 节点页放下该元组所需的完整字节数（各分量需已对齐）。 */
static inline size_t diskann_page_required_bytes(size_t tupleSize, size_t nodeSize, size_t edgeSize,
                                                 size_t itemIdSize, size_t opaqueSize)
{
    return tupleSize + nodeSize + edgeSize + itemIdSize + opaqueSize;
}

/* 判定所需字节数是否不超过页面可用容量（availablePageSize = BLCKSZ - MAXALIGN(header)）。 */
static inline bool diskann_page_fits(size_t requiredBytes, size_t availablePageSize)
{
    return requiredBytes <= availablePageSize;
}

#endif /* DISKANN_PAGE_CHECK_H */
