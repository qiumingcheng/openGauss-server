# LG-表 [P0-6a] vacuumcleanup 统计修复（范围按 D-009 定案）

| 锚点符号 | 修改前 | 修改后 | 调用点 | 触发路径 | 销毁/回退 |
| --- | --- | --- | --- | --- | --- |
| diskannvacuumcleanup_internal (diskann.cpp:352) | stats==NULL 时返回全零 | num_pages=真实块数；num_index_tuples=info->num_heap_tuples 估算；estimated_count=true | VACUUM/VACUUM ANALYZE 无前置 ambulkdelete 时 | 任何含 DiskANN 索引的表的 VACUUM | palloc0 stats 由 VACUUM 框架释放 |

## 实证（前后对比，同场景 100 行表+103 页索引）

- 修复前：VACUUM 后 idx relpages 103→0、reltuples 100→0（before_vacuum_stats.log）
  → 优化器成本模型被打穿
- 修复后：保持 103/100（after_vacuum_stats.log），索引查询正常

## 分期（D-009）

- immediate-delete 格式门控 + deferred bulkdelete 主体（pageType 扫描、tombstone
  状态机）→ P1-3（磁盘格式 v2 落地时）
- mvcc_01/mvcc_02/fi_04 保持 xfail，点亮权转 P1-3（STATUS.md 已注明归属）

## 判据 → 证据

| 判据 | 证据 |
| --- | --- |
| VACUUM 后 pg_class 统计不清零 | before/after_vacuum_stats.log 对比 |
| 普通 VACUUM 路径零回归 | regression_diskann_vector.log 与基线逐字节一致 |
