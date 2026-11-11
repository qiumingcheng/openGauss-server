# LG-表 [P2-3] AM 内过滤

| 锚点 | 修改前 | 修改后 | 触发路径 | 生命周期 |
| --- | --- | --- | --- | --- |
| diskann.h | 无 | FilterCacheHashEntry（simplehash status 与 verdict filterState 分离——simplehash 内部机制独占 status 字段）+ filterhash 实例；opaque +filter/filterCache/filterEvaluated/filterRejected/qualifiedResults；DiskAnnFilterNode/diskann_set_filter/diskann_get_filter_stats 声明 | — | — |
| diskannutils.cpp | 无 | filterhash DEFINE；DiskAnnFilterNode（缓存单次 insert 探测→锁内 CopyIndexTuple 到 tmpCtx→解锁→三值评估[FunctionCall2Coll 全部在锁外]→缓存写回）；diskann_set/get_filter_stats | In-Filter 扫描的每个 frontier 候选 | cache 随 tmpCtx；set 时 reset |
| diskannscan.cpp | 候选无条件入表 | diskannsearch 候选循环：filter 非空先 DiskAnnFilterNode，未 admit 者不入 candidates（导航已在 SearchFixedPoint 完成——两类集合分离 §16.1）；beginscan 置空/endscan 清指针 | 同上 | — |
| nodeAnnIndexscan.cpp | FilterDesc 无消费者 | 编译成功后 diskann_set_filter 安装（**in_filter reloption=false 时不装**——运行时门控落地）；EXPLAIN ANALYZE 经 get_filter_stats 显示 | filterMode=IN 查询 | desc 跨 rescan 复用（参数原地刷新） |
| explain.cpp | 无 | "Rows Removed by Index Filter"（仅 AM 提前拒绝；与 Rows Removed by Filter[recheck] 分开计数 §21.2） | EXPLAIN ANALYZE | — |

## 判据→证据

| 判据 | 证据 |
| --- | --- |
| 实现前失败 | test_before.log（0 行 Rows Removed by Index Filter） |
| AM 侧拒绝发生 | test_after3.log：**Rows Removed by Index Filter: 31**（40 行表 tenant_id=1） |
| pushed==truth | A/B 对照：=1 与 IS NOT NULL 两查询 pushed 结果与无索引真值**逐行一致** |
| NULL 语义 | IS NOT NULL 含 NULL 行表正确（recheck 兜底一致） |
| in_filter off | reloption=false 索引查询结果不变（post-filter 路径） |
| 回归零退化 | regression.log 逐字节一致 |

## 过程事故（2 起，全部定位修复）

1. **simplehash NULL found 崩溃**：filterhash_insert 第二次传 NULL（simplehash :631
   `*found=true` 无条件解引用）→ SIGSEGV（gdb 单用户抓栈：filterhash_insert_hash_internal）。
   修复=所有 insert 传真实 bool 指针。**教训：本仓库 simplehash 的 found 不可为 NULL（与上游 PG 不同）**
2. **D-015 重演**：改 diskann.h 后只重编了直接编辑的 2 个 .o → diskanndelete.o 陈旧 →
   B3 段 SIGSEGV。touch 全目录后通过。教训已入 D-015 但执行遗漏——**touch 必须是
   改头文件后的第一个动作，不是想起来才做**。

## 边界

- by-ref payload 复制走整元组 CopyIndexTuple（比逐属性 datumCopy 简单且一次分配）
- filterCache 建在 tmpCtx（scan 生命周期）；rescan 由 diskann_set_filter 重置——但 P2-3 的
  rescan 路径未再调 set（desc 复用）→ **cache 跨 rescan 保留**：设计允许（节点 verdict 不随
  参数变化——verdict 依赖参数！IN 列表换参数时 cache 陈旧——记 KNOWN_ISSUES #7，
  P2-4 的 rescan 链中补 filterhash_reset）
