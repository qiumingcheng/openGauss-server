# P0-2 准备笔记（fastcheck 运行期只读分析，2026-08-14）

## 1. 锚点代码事实（实测 @ 6df94c2，diskannscan.cpp）

| 符号 | 行号 | 当前行为 | 缺陷 |
| --- | --- | --- | --- |
| `diskannrescan_internal` | 51-66 | 仅复制 keys/orderByData + `so->curpos = 0` | candidates、blocks(visited)、curIterNum、nexpextedCandidates、query 相关状态全部残留 → 换参数二次执行先返回上一查询的旧候选 |
| `diskanngettuple_internal` | 128-165 | `curpos==0 \|\| curpos==ncandidates` 且 `curIterNum<MAX` 时翻倍重搜 | curIterNum 未随 rescan 重置 → 二次执行直接从翻倍档起步 |
| `diskannsearch` | 167-207 | `queue->clear()`；**不清 blocks**；结果 append 进 `so->candidates` | 跨轮 visited 残留（frozen 点已 visited → 无法真正扩搜，设计文档 §4.13 实锤）；候选跨轮累积 |
| 候选结构 | diskann.h `DiskAnnCandidatesData` | 只有 id + heapTid，无距离 | §6.4/9.4：跨轮无全局排序依据（距离字段属 P2-4 strict batch 的前置，P0-2 先不做） |
| `VectorList::contains()` | 197 行调用 | 线性去重 | O(n²)，P0-2 保持语义，hash 化留 P2-3 filter cache 一起做 |
| `diskannbeginscan_internal` | 68-113 | `so->params = *tmpDiskPQParams` 后未 pfree wrapper | §16.3 泄漏点实锤（wrapper 数组归 Relation cache，只能 pfree wrapper 本身）→ P0-3 顺手修复在其范围内 |
| `diskannendscan_internal` | 115-123 | clear 后 MemoryContextDelete(tmpCtx) | 顺序 OK；但 blocks hash 也在 tmpCtx（与 queue 混置）→ P0-3 拆分 |

## 2. P0-2 修复设计（最小面）

1. `diskannrescan_internal` 补全重置：candidates.clear()、ncandidates=0、curpos=0、
   curIterNum=0、nexpextedCandidates=0、blocks(visited) reset（blockhash 需重建或 reset 接口——
   以现有 simplehash 生成的 API 为准，P0-2 实现时核实 `blockhash_reset` 是否存在）。
   注：换查询参数后 GetScanValue 在 gettuple 重新求值，query 状态随之更新。
2. 扩搜轮次间：`diskannsearch` 开头对 visited 做与轮次一致的清理（MVP=每轮完整重搜，
   设计文档 §10.3 的取舍），dedup 语义保持"跨轮已输出不重复"（现 contains 承担）。
3. 不动：候选距离字段、严格排序、内存上下文拆分（分别属 P2-4/P0-3）。

## 3. 测试设计（先失败后通过）

L2 核心：prepared statement 换参数二次执行，断言第二次结果 == 独立计算的真值：
```sql
SET plan_cache_mode = force_generic_plan;
PREPARE p(vector) AS SELECT id FROM diskann_p02_t ORDER BY embedding <-> $1 LIMIT 3;
EXECUTE p('<远点A>');   -- 返回 A 的近邻集
EXECUTE p('<远点B>');   -- 修复前预期混入 A 的旧候选（错误），修复后 == B 真值
```
- 真值对照：同表同参数的无索引/顺序扫描结果（或显式 ORDER BY 全表 LIMIT）。
- 风险：ANN 路径 generic plan 是否可强制。若不可，回退触发方式 = 参数化嵌套循环内表 rescan
  （设计文档 F15 同源），或游标重执行。实现时先验证触发路径再固化用例。
- 计数独立断言（visited/轮次）属 AM 内部，无 EXPLAIN 出口，P0-2 以"结果正确性 +
  结果集不串"作为可观察判据；内部状态断言留 P2-6 EXPLAIN 统计字段落地后补强（记 DECISIONS）。

## 4. 依赖

- 无前置步骤依赖（P0-1 独立，可并行准备但串行实施——执行文档 R2 单线程）。
- 修复触及 diskannscan.cpp + diskann.h（DiskAnnScanOpaqueData 如需补 reset 接口声明）。

## 5. API 确认（2026-08-14 补）

- `blockhash_reset(hash*)` 由 lib/simplehash.h 生成（simplehash.h:196），可直接用于
  visited 清空——无需 blockhash_destroy+create 重建（P0-2 若采用 reset 方案，
  内存上下文归属在 P0-3 拆分时一并调整，避免双写）。
- `VAMANA_CAP=32766`（diskann.h:77）为 blockhash 初始容量，扩搜轮次远小于此值，
  reset 复用容量即可。
