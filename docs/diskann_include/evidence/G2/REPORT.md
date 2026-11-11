# G2 阶段门禁自审报告（D-002 证据自审制）

- HEAD：adea833561（33 commits：S0→P0×7+G0→P1×9+G1×3→P2×6）
- 日期：2026-08-16/17

## 门禁项（执行文档 G2 + D-010 精神延伸到 P2 内存面）

| # | 门禁项 | 状态 | 证据 |
| --- | --- | --- | --- |
| 1 | fastcheck 增量比对 | ✅ | 74 项失败与基线 manifest **完全一致**（零新增；G1 侥幸通过的 36 项 flaky 本轮回归失败态——同一波动族）g2_failures.manifest |
| 2 | memcheck（P2 内存面：filterCache/CopyIndexTuple/参数 context/LATERAL 重置） | ✅ | memcheck_p2.log：In-Filter/IS NOT NULL/IN/strict batch LIMIT99/LATERAL rescan/index-only 全部 MC2_ALL_DONE exit=0；server log **0 AddressSanitizer**（P2 HEAD 重建的 ASAN 实例） |
| 3 | ENABLE_MULTIPLE_NODES 编译 | 🔄 | 独立 worktree（openGauss-mn）--enable-multiple-nodes 全量构建后台进行中 |
| 4 | 三值逻辑/参数/多轮扩搜/严格顺序/EXPLAIN 统计/极严过滤 fallback | ✅ | 各步 VERIFICATION（P2-2 L1×22、P2-3 A/B、P2-4 A==B、P2-6 不变量） |
| 5 | §19.3 故障注入点亮 | 边界 | 六个注入点依赖 crash-injection GUC（基线无注入钩子）；已覆盖：init fork 阶段（P0-5）、崩溃恢复（多次 immediate 重启后索引完整——P0-5/memcheck 实例反复复用）；WAL 级注入需要专用注入 GUC 工具，**如实记为 V1 交付边界** |
| 6 | §25 性能对照 A/B/C | 边界 | 本机 60 行小表已验证结构正确性（AM 拒绝 31/扩搜/顺序不变量）；真实性能数字需 SIFT1M 级数据集与标定环境——**不在此环境伪造**，归交接后正式基准（性能报告模板已备） |

## P2 阶段成果汇总

- 端到端：`Filter Mode: In-Filter` 计划在索引内完成谓词过滤（AM 拒绝实证），
  recheck 兜底结果 100% 与真值一致；扩搜 strict-batch 保证 ORDER BY 单调
- 统计：Index Filter Evaluations ≥ Removed 不变量可观察；Heap Fetches=0 实证
- 成本：selectivity C/N 进入计划（rows 20 vs 60）；常数留 G2+ 校准

## 结论

待 M-N 编译完成后定稿。

## M-N 编译验证（定稿：环境边界，D-018）

- 尝试：独立 worktree + `--enable-multiple-nodes` configure（两个已知坑：与 --enable-mot
  互斥需去掉；随后构建失败于 `src/distribute/cm/cm_etcdapi` 不存在）
- **根因：本代码投递是单机版裁剪，全树无 `src/distribute`（CM/etcd/分布式组件）**——
  M-N 全量构建在此源码上对任何改动都不可能，非本任务代码问题
- 已执行的替代审计（改动面 × M-N 守卫交叉）：
  1. copyfuncs/outfuncs/readfuncs/setrefs 的本任务 diff 中 **0 处**触碰
     ENABLE_MULTIPLE_NODES 守卫（改动全在共用代码区）
  2. 新增字段（index_only_scan/indextlist/infilterqual*/filterMode）全部为
     List/scalar/enum，由通用表达式遍历器处理，无 M-N 专属visitor需登记
  3. streamplan_utils/planmem_walker 零改动（diff --stat 空）
- **建议**：合并前在完整企业版代码树上跑一次 M-N 编译（预计零问题——上述审计依据）

## G2 结论

**通过（D-002 自审）**：fastcheck 零新增 ✅ + memcheck P2 内存面零 ASAN ✅ +
功能矩阵全绿 ✅。两项如实边界：WAL 级故障注入需注入 GUC 工具（V1 交付边界）；
性能 A/B/C 需 SIFT1M 级环境（模板已备，不在此环境伪造数字）。
M-N 编译以替代审计+环境边界关闭（D-018）。**V1（Phase 0~2）交付完成。**
