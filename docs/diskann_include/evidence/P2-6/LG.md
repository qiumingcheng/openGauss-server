# LG-表 [P2-6] EXPLAIN 统计收口

| 锚点 | 修改前 | 修改后 | 触发路径 | 生命周期 |
| --- | --- | --- | --- | --- |
| explain.cpp T_AnnIndexScan | Index Filter Evaluations 无；Heap Fetches 无 | filterMode=IN 且 ANALYZE：+`Index Filter Evaluations`（AM 评估计数，与 Rows Removed by Index Filter 分开——拒绝≠评估）；index_only_scan 且 ANALYZE：+`Heap Fetches`（iss_HeapFetches，与 IndexOnlyScan 同名同义） | EXPLAIN ANALYZE | — |

## 判据→证据（§21.2 对照 / §24.8 不变量）

| 项 | 证据 |
| --- | --- |
| Evaluations ≥ Rejected | stats_display.log：Evaluations=41, Removed=27（41≥27 ✅） |
| Heap Fetches（VM 命中=0） | heapfetch_display.log（新装库，canreturn=true）：**Ann Index Only Scan + Heap Fetches: 0**（VACUUM 后） |
| 计数不重复 | Rows Removed by Filter（recheck）与 Rows Removed by Index Filter（AM 拒绝）分开显示，语义正交 |
| 回归零退化 | diskann_vector 逐字节一致 |
| 旧实例（canreturn=0） | index-only 不被选（Ann Index Scan）→ Heap Fetches 不显示——门控正确 |

## §21.2 九项统计的 V1 覆盖声明（R3 如实）

已显示：Filter Mode / Index Filter / Rows Removed by Index Filter / Index Filter
Evaluations / Heap Fetches / Rows Removed by Filter（既有）。
未显示：Search Expansions / Initial-Final Search Budget / Candidates Visited——
这些需要 AM 内部轮次计数暴露（opaque 加字段+getter），且对调参的价值主要在大数据
基准（G2）；记为 V1 边界，G2 评审是否补（低风险增量）。
