# LG-表 [P2-4] strict batch 扩搜 + #7 修复

| 锚点 | 修改前 | 修改后 | 触发路径 | 生命周期 |
| --- | --- | --- | --- | --- |
| diskanngettuple_internal | 候选耗尽即扩搜（无论合格数）；候选按轮追加无全局排序 | filter+limitk>0 时：**首轮输出前**循环扩搜直到 qualifiedResults≥limitk / 迭代上限 / **无增长**（候选数与合格数均不变=frontier 耗尽）；随后对累计候选**一次全局距离排序**（qsort）再逐条输出。无 filter 路径保持原行为 | In-Filter 强过滤扫描 | — |
| DiskAnnCandidateDistanceCmp（新） | 无 | double 距离升序 + Heap TID 三级 tie-break（§16.2 稳定序） | 同上 | — |
| diskannrescan_internal | filterCache/计数跨 rescan 残留（#7） | filterhash_reset + filterEvaluated/Rejected/qualifiedResults 清零 | LATERAL/prepared rescan | — |

## 扩搜停止条件（§16.3 对照）

| 条件 | 实现 |
| --- | --- |
| qualified ≥ K | while 主条件（scan->limitk） |
| 迭代上限 | curIterNum < MAX_SEARCH_ITERATION(10) |
| frontier 耗尽 | candidates.size() 与 qualifiedResults 双无增长即 break |
| 上层停止 | Limit 收够即不再拉取（executor 天然） |
| 资源上限 | 暂无独立 V_max（沿用迭代上限；G2 评审是否加显式预算） |

## 判据→证据（R3 如实）

- **fail-before 不可构造**：宽束搜索新候选通常更远，此图上 before 输出顺序恰好==真值
  （test_before.log）。本步是设计 §16.3 强制加固（迟到近点在更大/更密图上可发生）+
  #7 修复；测试守恒不变量
- 扩搜场景 A==B：test_after.log {1,7,...,55} 逐位一致（强过滤 1/6 + LIMIT 12>首轮合格）
- 不足 K：LIMIT 99 → 返回全部 10 匹配（p24_full=10），不伪造
- 回归零退化：diskann_vector 逐字节一致
- #7 修复：rescan 后 cache 重置（LATERAL 用例在套件中无串值）

## 边界

- 全局排序 O(n log n) 在累计候选上（上限≈probes×2^10 理论值，实际受图规模约束）
- strict batch 仅 filter 路径启用——无过滤时原行为（回归零退化的保证来源）
