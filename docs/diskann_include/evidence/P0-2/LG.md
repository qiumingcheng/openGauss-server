# LG-表 [P0-2] rescan 全量重置 + 逐轮 visited 重置

## 修改符号（@ feature/diskann-include 提交时行号）

| 锚点符号 | 修改前 | 修改后 | 调用点(rg 实测) | 触发路径 | 销毁/回退 |
| --- | --- | --- | --- | --- | --- |
| `diskannrescan_internal` (diskannscan.cpp:51) | 仅 `so->curpos=0` | 五项全量重置：curpos/candidates(resize(0))/ncandidates/curIterNum/nexpextedCandidates + `blockhash_reset(so->blocks)` | 标准 executor 每次 AnnIndexScan 启动与重复执行（nodeAnnIndexscan.cpp 调用，含首扫） | LATERAL 参数化内表：外层每行触发一次 rescan | 重置仅触及 scan-local 状态，无跨调用资源 |
| `diskannsearch` (diskannscan.cpp:180+) | `queue->clear()`（visited 跨轮保留） | 追加 `blockhash_reset(so->blocks)`：visited 变为逐轮状态 | gettuple 候选耗尽扩搜时 | 多轮扩搜（LIMIT > 首轮候选） | 同上 |

## 生命周期断言（begin → first gettuple → rescan → end → error）

- begin：blocks 创建于 tmpCtx；candidates VectorList 缓冲区可复用
- first gettuple：rescan 先行（executor 启动即 rescan 一次）→ 首扫状态=全新 ✅（冒烟首扫通过证明）
- rescan（换参数）：全部查询态清零 → 实证：qid=2 结果从 {1,3,2}（旧候选）修正为 {7,9,8}（真值集合）
- end：clear() 破坏性销毁 + tmpCtx 删除（未改，行为不变）
- error：无新增资源

## 实现过程中的失败与修正（重要）

首版补丁用 `so->candidates.clear()` → **服务器 SIGSEGV**：
`VectorList::clear()` 是破坏性销毁（utils.h:171-177，pfree+nullptr+capacity=0），
后续 push_back 对 nullptr repalloc。且 rescan 在首扫即被调用 → 首个查询即崩。
修正为 `resize(0)`（仅 pop，保缓冲）。已登记 KNOWN_ISSUES #3/#4。

这正是"测试先行 + 立即验证"机制的价值：缺陷在提交前一个测试周期内被抓获。

## 判据 → 测试映射

| 判据 | 证据 |
| --- | --- |
| 同一执行器换参数二次执行结果独立正确 | test_before_fix.log（qid=2={1,3,2}❌）vs test_after_fix.log（qid=2={7,9,8}✅==真值{7,8,9}集合） |
| 先失败后通过 | 同上两日志，同一测试文件 |
| 首扫不回归 | test_after_fix.log 冒烟段（Ann Index Scan + id 1,4） |
| 标准回归零退化 | regression_diskann_vector.log 与基线 diff 逐字节一致 |
| 计划形态 | EXPLAIN：Nested Loop → Ann Index Scan（非 Bypass） |

## rg 审计

- `Datum[1]/bool[1]`：未新增；`MAIN_FORKNUM`：未触碰；`IsMarkDeleted`：未触碰；
  `T_AnnIndexScan`：未触碰。
