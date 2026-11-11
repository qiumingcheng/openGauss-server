# 故障注入/专项脚本状态注册表

| 脚本 | 注入点 | 点亮步骤 | 状态 | 证据 |
| --- | --- | --- | --- | --- |
| fi_01_crash_after_itup_before_edge.sh | §19.3-1 IndexTuple 加页后、边写入前 | P1-3 | xfail | — |
| fi_02_crash_before_live_publish.sh | §19.3-2 LIVE 发布前 | P1-3 | xfail | — |
| fi_03_crash_after_dup_detect.sh | §19.3-3 duplicate 检测后 | P1-3 | xfail | — |
| fi_04_vacuum_tombstone_wal.sh | §19.3-4 VACUUM payload-dead WAL 前后 | P1-3 | xfail | 归属随 D-009 移至 P1-3 |
| fi_05_meta_insertpage_update.sh | §19.3-5 MetaPage insertPage 前后 | P0-5 | xfail | — |
| fi_06_initfork_stages.sh | §19.3-6 init fork 各阶段 | P0-5 | pass-partial | evidence/P0-5/after_fix_test.log（metapage+lock页已覆盖；PQ阶段本机不可测 D-008） |
| mvcc_01_dual_snapshot.sh | §24.5 双快照不漏行 | P1-3 | xfail | 归属随 D-009 移至 P1-3（v2 tombstone 落地） |
| mvcc_02_churn_navigable.sh | §24.5 churn 后可导航 | P1-3 | xfail | 归属随 D-009 移至 P1-3 |
| memcheck_01_scan_cycle.sh | §16.3 扫描生命周期 memcheck | P0-3 | xfail | — |

状态取值：`xfail / pass / blocked`。点亮时必须更新证据列为
`docs/diskann_include/evidence/<STEP>/` 下的真实日志路径（执行文档 R1/R3）。
