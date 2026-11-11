# DiskANN INCLUDE/In-Filter 故障注入与专项测试工具

本目录存放执行文档（`公共文档/多模_向标融合_DiskANN_INCLUDE_InFilter_执行文档.md`）S0-3
定义的专项测试骨架。设计来源：设计文档 §19.3（六个崩溃注入点）、§24.5/§24.6（并发/恢复场景）。

## 状态注册表（STATUS.md）

每个脚本在点亮前标记 `xfail`：允许失败、允许未实现，但**不允许静默消失**。
脚本与其"点亮步骤"的对应关系见 `STATUS.md`；步骤完成时必须把对应脚本改为 `pass`
并附证据路径，否则该步骤不得记 `done`（执行文档 R1）。

## 运行约定

- 环境变量：`PGPORT`（默认 55440）、`GSQL`（gsql 路径）、`PGDATA`。
- 崩溃注入通过 `kill -9` 实例进程实现（当前基线无注入 GUC；若后续步骤引入
  `diskann_debug_inject` 类 GUC，脚本升级为精确注入点，并在 STATUS.md 记录）。
- 每次运行把完整输出保存到 `docs/diskann_include/evidence/<STEP>/` 下。

## 脚本清单（S0-3 骨架版）

| 脚本 | 注入点（设计文档 §19.3） | 点亮步骤 |
| --- | --- | --- |
| fi_01_crash_after_itup_before_edge.sh | IndexTuple 加页后、边写入前 | P1-3 |
| fi_02_crash_before_live_publish.sh | 邻居已指向新节点、NODE_LIVE 发布前 | P1-3 |
| fi_03_crash_after_dup_detect.sh | duplicate 检测后、节点链接前 | P1-3 |
| fi_04_vacuum_tombstone_wal.sh | VACUUM payload-dead 状态 WAL 前后 | P0-6 |
| fi_05_meta_insertpage_update.sh | MetaPage 更新 insertPage 前后 | P0-5 |
| fi_06_initfork_stages.sh | init fork 创建 MetaPage/PQ/节点页各阶段 | P0-5 |
| mvcc_01_dual_snapshot.sh | 双会话双快照：旧快照不漏行（§24.5） | P0-6 |
| mvcc_02_churn_navigable.sh | 高 churn 后 dead 节点仍可导航（§24.5） | P0-6 |
| memcheck_01_scan_cycle.sh | memcheck 定向扫描生命周期（§16.3） | P0-3 |

骨架版脚本统一行为：打印 `XFAIL：<脚本名> 骨架未实现，等待步骤 <点亮步骤>` 并以退出码 2
结束——保证"先失败"原则在注册表中可见，且 CI/人工都不会把它误读为通过。
