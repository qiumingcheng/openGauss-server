# VERIFICATION — 每步证据记录（执行文档 §6.1）

格式：实现前失败证据 / 实现后通过证据 / 回归证据 / LG-表 / 判据→测试映射 / rg 审计分类。

## [S0-1] 工作区清点与基线固化
- 日期：2026-08-14
- HEAD 核对：`git rev-parse HEAD` → `6df94c2ccb779611e55406e8fdb463068eea8b77` ✅
- dirty 分类：BASELINE.md §2（49 untracked + 1 deleted；已跟踪源码零修改）；
  保守处置按 D-001 自决通过（不动任何 untracked 文件）
- 活实例基线：`gsql -f src/test/regress/sql/diskann_vector.sql` exit=2
  （唯一失败为文件末尾 `\c regression`，pg_regress 环境专用，预期）；
  7 索引 + 4 Ann Index Scan 计划 + 3 预期报错 → 判定通过
  → evidence/S0-1/baseline_diskann_vector_live.log
- 权威基线：`make fastcheck PART=0` 64min，933 ok / 74 failed（exit 2）；
  失败清单固化 baseline_failures.manifest；判据按 D-006 增量制
  → evidence/S0-1/baseline_fastcheck_part0.log
- 判据映射：HEAD=基线 ✅→命令输出；分类确认 ✅→D-001；回归通过 ✅→两份日志

## [S0-2] 分支与构建链路
- 分支：`feature/diskann-include`（自 master@6df94c2ccb 创建，工作区保持）
- 增量编译计时：→ evidence/S0-2/incremental_build.log（本步含 P0-1 编辑后的编译；
  纯净树计时不可分离，记为"P0-1 编辑后增量构建"，判定标准=单次增量在可接受分钟数内）

## [S0-3] 测试骨架
- 产出：src/test/diskann_include_tools/{README.md,STATUS.md,9×xfail脚本} +
  src/test/regress/sql/diskann_include.sql
- 冒烟验证（含一次真实缺陷发现→修正循环）：首跑计划走 Seq Scan（小表无统计偏好），
  按 diskann_vector.sql 约定补 /*+ indexscan */ hint 后：Ann Index Scan + 结果正确
  （id 1,4）+ exit=0 → evidence/S0-3/smoke_skeleton.log
- 附带发现：基线 ANN 计划含 [Bypass]（OPFusion）标记 → P1-7 对照点（KNOWN_ISSUES 观察项）

## [P0-1] 页容量纯函数 + ERROR 路径 —— 完成 ✅
- 实现前失败：evidence/P0-1/l1_before_impl.log（头文件缺失，编译 exit=1）
- 实现后通过：evidence/P0-1/l1_after_impl.log（ALL PASS，run_exit=0）
- 语法验证：evidence/P0-1/syntax_check.log（fsyntax-only，项目原生命令行，0 警告）
- 全量编译/安装/重启：evidence/S0-2/incremental_build.log（8m19s，BUILD_EXIT=0；
  安装后 gaussdb 二进制含新错误串 "DiskANN index tuple is too large" 实证）
- 活实例回归：evidence/P0-1/regression_diskann_vector.log（exit=2 为 `\c regression`
  预期尾部；与基线输出 diff **逐字节一致** → 零行为变化 ✅）
- LG-表：evidence/P0-1/LG.md
- 判据映射：纯重构零行为变化 ✅（diff 为空）；release 语义 ERROR ✅（ereport 路径 +
  L1 纯函数边界）；SQL 级触发留 P1-3（判据拆分见 PREP §2）

## [P0-2] rescan 全量重置 + 逐轮 visited —— 完成 ✅
- 实现前失败（缺陷复现）：evidence/P0-2/test_before_fix.log
  —— LATERAL 参数化内表扫描，qid=2 返回 {1,3,2}（上一查询旧候选），
  真值 {7,8,9}；计划确认 Nested Loop → Ann Index Scan（非 Bypass）。
  触发路径演进：PREPARE custom plan 测不到（每次新建执行器）→ 换 LATERAL 触发成功
- 实现中失败被抓（SIGSEGV）：首版用 VectorList::clear()（破坏性销毁）→ 首扫即崩
  （signal 11，pg_log + WSL crash capture 佐证）；修正为 resize(0)。
  KNOWN_ISSUES #3（残留 .clear() 雷，owner:P0-3）#4（rescan 首扫即调用）
- 实现后通过：evidence/P0-2/test_after_fix.log —— qid=2 → {7,9,8} == 真值集合 ✅
- 标准回归：evidence/P0-2/regression_diskann_vector.log 与基线 diff 逐字节一致 ✅
- LG-表：evidence/P0-2/LG.md



## [P0-3a] 插入/扫描路径资源释放 —— 完成 ✅（范围按 D-007）
- 实施前失败证据：不适用（泄漏修复，SQL 级观测不可达，D-007 记录；功能验证先行：
  200 行批量插入 + 错误回滚 + 索引查询）
- 功能验证：evidence/P0-3/test_functional.log（exit=0，251 行，错误路径回滚正确）
- 标准回归：evidence/P0-3/regression_diskann_vector.log 与基线逐字节一致 ✅
- LG-表：evidence/P0-3/LG.md
- 遗留：三上下文拆分 → P1-3；.clear() 雷统一处理 → P0-3b/P1-3（KNOWN_ISSUES #3）

## [P0-4] MetaPage 版本感知读取 —— 完成 ✅
- v1 兼容：evidence/P0-4/regression_diskann_vector.log 与基线逐字节一致；
  test_functional.log（diskann_include 全量用例）exit=0（唯一 ERROR 为预期维度错误）
- 编译门禁抓获一次错误（StaticAssertStmt 文件作用域非法→static_assert），已归档
- 边界（D-008）：PQ 路径本机不可测（x86_64 stub）；magic/version 分支归 P1-4
- LG-表：evidence/P0-4/LG.md（含 PREP 的 V1 长度推导错误修正）

## [P0-5] forkNum 传递链（unlogged init fork）—— 完成 ✅
- 实现前失败：evidence/P0-5/before_fix_state.log（崩溃后索引 0 字节 + ERROR read block 0）
- 实现后通过：evidence/P0-5/after_fix_test.log（有效空索引 16KB、0 行、INSERT+查询正常）
- 标准回归：与基线逐字节一致 ✅
- STATUS 注册表：fi_06 → pass-partial（metapage/lock 页覆盖；PQ 阶段本机不可测 D-008）
- LG-表：evidence/P0-5/LG.md

## [P0-6a] vacuumcleanup 统计修复 —— 完成 ✅（范围 D-009）
- 实现前失败：evidence/P0-6/before_vacuum_stats.log（VACUUM 后索引统计 103/100→0/0）
- 实现后通过：evidence/P0-6/after_vacuum_stats.log（保持 103/100）
- 标准回归：与基线逐字节一致 ✅
- 分期：immediate-delete 门控与 deferred bulkdelete → P1-3（D-009）
- LG-表：evidence/P0-6/LG.md

## [P1-1] DDL 放开与校验 —— 完成 ✅
- 实现前失败：test_before_impl.log（"only supported in ubtree"）
- 实现后通过：test_after_impl.log（1 key+INCLUDE 成功；6 类拒绝各得正确错误；分区被既有 GPI 检查拒绝）
- 标准回归：与基线逐字节一致 ✅
- 编译门禁抓到 2 类错误（attrs 取址 / amapi.h）并修复归档
- LG-表：evidence/P1-1/LG.md；KNOWN_ISSUES #5（ALTER 后置拦截 → P1-3）

## [P1-2] reloptions 三处成对注册 —— 完成 ✅
- 实现前失败：test_before_impl.log（unrecognized parameter "in_filter"）
- 实现后通过：test_after_impl.log（接受/未知参数/越界/ALTER 双行为 7/7）
- 标准回归：与基线逐字节一致 ✅
- LG-表：evidence/P1-2/LG.md（含常量自决留痕）

## [P1-3a] 完整 IndexTuple + 一行一节点 + 冻结点 —— 完成 ✅
- 两阶段闭环：基线 4/4（合并掩盖）→ 一行一节点无修复 **5/4**（缺陷暴露）→ +冻结点修复 4/4
- T1 非 INCLUDE 12/12 不变；T3 DML 正确；diskann_vector 逐字节一致
- rg [1] 审计：仅剩合法 key-only 用法
- LG-表：evidence/P1-3/LG.md；遗留 v2 tail 等归 P1-3b

## [P1-3b] v2 格式 + 门控 + ALTER 拦截 —— 完成 ✅
- B1 前败后过：ALTER SET immediate_delete 静默成功 → ERROR（#5 关闭）
- B2/B3：v2 deferred DELETE 正确（15 行）；旧 immediate_delete 路径不变（7 行）
- 回归：与基线逐字节一致 ✅
- LG：evidence/P1-3/LG_3b.md；上下文拆分按 D-012 移 P1-7

## [P1-4] catalog：diskanncanreturn —— 完成 ✅（新装库路径；升级路径 D-013）
- 新装库：amcanreturn=8601、pg_proc 8601、行为全对（4/4、COUNT、0 ERROR）
- 实现前失败：内置符号缺失（before_impl）+ initdb 首败（.ini 依赖坑，D-013）
- 旧实例：功能套件 exit=0、diskann_vector 逐字节一致
- LG：evidence/P1-4/LG.md（含 4 个过程事故与 OID 8601 冲突检查记录）

## [P1-7] executor Index-Only —— 主功能闭环 ✅（1 阻塞：KNOWN_ISSUES #6）
- Ann Index Only Scan 端到端跑通：covered 查询计划+结果==真值；向量 key 投影正确 join-back；
  fusion 门控生效；旧实例回归逐字节一致（B3 隔离后 func exit=0）
- 过程抓获 3 个真实缺陷（xs_orderbyvals NULL 写/断言挂死/resjunk 判定），全部定位修复
- 阻塞：immediate_delete+DELETE 挂死（B3 隔离，G1 前必须解决）
- LG：evidence/P1-7/LG.md

## [P1-7 补遗] KNOWN_ISSUES #6 解决 —— 完整重编后全面重验 ✅
- 根因：make 头文件依赖失效（D-015），非代码缺陷；gdb 单用户抓 SIGSEGV 栈定位
- 重验（touch 全量重编后）：B3 通过 b3_rows=7；全量 func exit=0；
  diskann_vector 逐字节一致；新装库 Ann Index Only/Scan 双形态闭环 0 ERROR
- 证据：evidence/P1-7/{func_full_rebuild,regression_full_rebuild,closure_full_rebuild}.log

## [P2-1] 谓词白名单分类 —— 完成 ✅
- 实现前失败：0 Filter Mode 行；实现后：In-Filter/Index Filter/Post-Filter 三态全对
- 执行不变（原谓词保留 recheck）；diskann_vector 逐字节一致
- LG：evidence/P2-1/LG.md（含门控独立推导教训）

## [P2-2] FilterDesc 编译与三值逻辑 —— 完成 ✅
- L1：ut_diskann_filter_logic 22 断言 ALL PASS（NULL/数组/AND 三值全表）
- L2：In-Filter 查询 Init 编译+参数求值跑通；func exit=0
- 回归：diskann_vector 逐字节一致
- LG：evidence/P2-2/LG.md（双 context 归属表）

## [P2-3] AM 内过滤 —— 完成 ✅
- 实现前失败：0 行 Index Filter 统计；实现后：Rows Removed by Index Filter: 31（AM 真实拒绝）
- pushed==truth：=1 / IS NOT NULL 双查询 A/B 逐行一致；NULL 语义正确
- in_filter reloption 运行时门控生效；回归逐字节一致
- 2 起崩溃定位修复（simplehash NULL found[D-018 级教训]；D-015 重演）
- 遗留：filterCache rescan 陈旧（#7，recheck 兜底结果正确，P2-4 修）
- LG：evidence/P2-3/LG.md

## [P2-4] strict batch 扩搜 + #7 修复 —— 完成 ✅
- 扩搜场景 A==B 顺序逐位一致；不足 K 返回 10 不伪造；回归逐字节一致
- fail-before 如实声明不可构造（宽束特性使然）；测试守恒不变量（§16.3 加固）
- #7 关闭：rescan 重置 filterCache+计数
- LG：evidence/P2-4/LG.md

## [P2-5] 成本模型感知 in-filter —— 完成 ✅
- L1：ut_diskann_costmodel 17 断言 ALL PASS（单调/clamp/退化输入）
- selectivity 生效实证：过滤 rows=20 vs 无过滤 rows=60（C/N 进入计划）
- 小表成本差极小（如实）：大表效应归 G2 A/B/C 校准
- 回归：diskann_vector 逐字节一致；func exit=0
- LG：evidence/P2-5/LG.md

## [P2-6] EXPLAIN 统计收口 —— 完成 ✅
- Evaluations=41 ≥ Removed=27（不变量）；新装库 Ann Index Only + **Heap Fetches: 0**
- 计数正交（AM 拒绝 vs recheck）；回归逐字节一致
- 3 项轮次类统计（Expansions/Budget/Visited）记为 V1 边界→G2 评审
- LG：evidence/P2-6/LG.md
