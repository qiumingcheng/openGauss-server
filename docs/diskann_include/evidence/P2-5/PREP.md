# P2-5 准备笔记：成本模型感知 in-filter / index-only（2026-08-16 深夜）

## 现状（实测）

- `diskanncostestimate_internal`（diskann.cpp:35-72）：genericcostestimate 全索引口径
  ——不感知 LIMIT/选择率/L/probes（设计文档 §4.5 列的缺口全部仍在）
- `cost_index` 的 ANN 分支（costsize.cpp:1106-）只对 **HNSW** 做 annCount=limit/sel；
  DiskANN 走通用 generic 路径 + `run_cost += annIndexCost`（annIndexCost 对非 HNSW 恒 0）
- P2-1 已把 `annQuals/annQualCols` 填上（IndexPath 既有字段）——成本可感知过滤

## P2-5 最小面（按 §13.5 分层归属，不改 genericcostestimate 公共行为）

1. **diskanncostestimate_internal**（AM 私有）：
   - L0 = diskann_probes（首轮宽度）；有 filter 时按 §13.5：
     `L_filter = ceil(limitk / max(s, s_min)) * oversample`，clamp [probes, maxSearchBudget]
   - visitBudget = clamp(max(L0, L_filter), probes, N)
   - indexStartupCost = indexTotalCost = 图访问近似（V=min(N, factor*L) 的页 I/O + 距离 CPU +
     payload 比较 + rerank）——startup=total 保留（首轮批搜索语义，基线即如此）
   - 有 filter 时 indexSelectivity = clamp(C/N, 0, 1)（C=计划堆候选）→ **Heap fetch
     计费在 cost_index 通用部分自动下降**（§13.5 归属表）
2. **limitk 计划期估计**：cost_index 的 DiskANN 分支（非 HNSW）补 annCount 计算
   （与 HNSW 同式但乘 ANN_INDEX_COST 常数差异由 AM estimator 承担→此处只填 annCount
   供 executor 参考，不重复加 run_cost）
3. **常数**：集中 `#define DISKANN_GRAPH_PAGE_FACTOR` 等于 diskann.cpp 顶部；禁止散落

## 测试（判据映射 §12.6）

- L1：纯函数 `DiskAnnEstimateSearchWidth(k, s, probes, maxBudget)` 独立头文件单测
  （单调性：p↓→L↑、K↑→L↑；clamp 边界；s=0/NaN 不除零）
- L2：EXPLAIN (COSTS ON) 强过滤 vs 无过滤 DiskANN 计划 cost 差为正（过滤降低 heap 计费）；
  极强过滤 + B-tree 存在时优化器可选标量路径（hint 对照验证路径仍可选）
- 回归零退化

## 边界

- 不改 HNSW 分支；不动 genericcostestimate（其他 AM 共用）
- 成本单调性用例不做全参数扫描（G2 性能对照时以 A/B/C 组数据校准）
