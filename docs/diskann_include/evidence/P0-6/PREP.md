# P0-6 准备笔记：VACUUM tombstone 与 immediate-delete 门控（2026-08-14，只读）

## 1. 现状调用链（实测 @ 6df94c2）

### 1.1 VACUUM 空转（设计文档 §4.12/§12.3 实锤）

- `diskannbulkdelete_internal`（diskann.cpp:341-345）：**直接 return NULL**——
  死 TID 永远不被清理。
- `diskannvacuumcleanup_internal`（:346-355）：palloc0 stats + FreeSpaceMapVacuum，
  无 pageType 计数 → 单独调用时（无 ambulkdelete）统计全 0，pg_class 可能被错误清零
  （§12.3.1 场景）。

### 1.2 immediate-delete 断边链

- `execUtils.cpp:1413` `immediateDelete = RelationImmediateDelete(...)`
- `execUtils.cpp:1446-1448` `immediateDelete && !ri_hasDiskannIndex` → 置
  `ri_hasDiskannIndex = true`（**未按索引格式区分**——门控点，设计文档 §12.3.1 指定）
- `tableam.cpp:152-162`：UPDATE/DELETE 路径 `ri_hasDiskannIndex` →
  `DeleteDiskAnnIndexTuples(...)`
- `diskanndelete.cpp:280` → :247 → `DiskAnnMarkDead(rel, values, tid)`（:158）
  → `ItemIdMarkDead(itemid)`（:208，清反向边/本节点边）
- 搜索侧消费：`diskannscan.cpp:254` `IsMarkDeleted(rel, n->id)` → utils:856/867
  `ItemIdIsDead` → queue remove（§16.2 陷阱 3：remove 后仍用原 Neighbor*）

## 2. P0-6 实施设计（V1 最小面，对齐设计文档 §12.3）

### 2.1 deferred bulkdelete（diskann.cpp 或新 diskannvacuum.cpp）

按 §12.3.1 骨架：扫描 pageType=NODE 页（P0-4 前无 pageType 字段——**依赖 P0-4 的
pageType 写入**；P0-6 在 P0-4 之后实施，v1 旧格式索引 bulkdelete 保持现状 return NULL，
仅对 v2 索引走新逻辑。但 v2 索引 P1-3 才能创建 → P0-6 的新逻辑在 P0 阶段无真实数据
可跑，只能以单测/构造页验证。**处置**：P0-6 先落 2.2 门控 + 骨架 + 统计修正（可测），
v2 页扫描主体随 P1-3 数据可用后补强并点亮 fi_04（记 DECISIONS 判据分期））：

- `diskannvacuumcleanup_internal` 统计修正：无 ambulkdelete 先例时返回
  estimated_count=true + num_heap_tuples 推算，避免 pg_class 清零（SQL 可测：
  只跑 VACUUM ANALYZE 后 pg_class.reltuples 不归零）。

### 2.2 immediate-delete 格式门控（可完整落地）

- `execUtils.cpp:1446` 判定改为：`immediateDelete && !ri_hasDiskannIndex` 逻辑不变，
  但 `DiskAnnIndexUsesDeferredDelete(indexDesc)`（新增 helper：读 metapage version，
  v2 → true）参与：v2 索引不置 ri_hasDiskannIndex。
- `tableam.cpp` 调用点再校验一次（运行时防御，§8.1 要求）。
- 注：P0 阶段所有现存索引都是 v1 → 行为零变化（回归不退化的依据）；
  v2 门控路径单测 = 构造 metapage.version=2 的内存级测试或等 P1-3 真索引。
  先落代码 + cassert 防御 + 注释绑定。

### 2.3 IsMarkDeleted 消费侧（搜索仍用 dead 导航）

v2 的状态 helper（DiskAnnNodeGetState/CanReturn）依赖 v2 节点状态字段——P0 阶段
节点格式未变。**P0-6 不动搜索侧**（v1 行为保留）；状态机改造整体属 P1-3 伴随
（diskann.h 节点状态字段 + 格式 2），P0-6 只做门控+VACUUM 骨架。此分期与执行文档
P0-6 原文"deferred VACUUM tombstone 基础"对齐为：门控完整 + bulkdelete 骨架 +
cleanup 统计修正 + 测试脚本就绪。

## 3. 测试

- L2（可立即测）：VACUUM 后 pg_class.reltuples 不清零；UPDATE/DELETE 大量行后
  查询仍正确（v1 路径不回归）；immediate_delete 表 DML 后旧快照语义（现有行为快照
  对照，防 P0-6 改动引入回归）。
- L3：mvcc_01_dual_snapshot / mvcc_02_churn / fi_04 按分期点亮（fi_04 → P1-3 后）。
