# DiskANN INCLUDE + In-Filter 执行文档（小步快跑 · 双重保证版）

## 0. 文档定位

| 项目 | 内容 |
| --- | --- |
| 上游设计 | `公共文档/多模_向标融合_openGauss_DiskANN包含列与索引内过滤详细设计.md`（下称"设计文档"） |
| 代码基线 | `openGauss-server` @ `6df94c2ccb779611e55406e8fdb463068eea8b77` |
| 执行范围 | 仅设计文档 Phase 0～2（首版可交付）。Phase 3 平台适配不在本文档范围 |
| 执行方式 | 原子步骤串行；每步必须同时满足**代码执行逻辑保证**与**测试用例保证**才能关闭 |
| 状态字段 | 见第 9 节任务状态表；执行者每完成一步必须更新 |

本文档是执行合同。设计文档回答"做什么、为什么"；本文档回答"按什么顺序、每步怎么证明做完了"。两者冲突时，先停下来向用户报告，不得自行选择宽松说法。

---

## 1. 三条红线（凌驾于一切进度压力）

1. **R1 双重保证不可豁免**：任何一步缺少"逻辑追踪表"或"测试证据"之一，状态只能记 `blocked` 或 `in_progress`，不得记 `done`。
2. **R2 不跨步修改**：实施中发现非本步骤范围内的缺陷，记入 `docs/diskann_include/KNOWN_ISSUES.md`，不顺手修复。例外：该缺陷阻塞当前步骤且修复面 ≤ 20 行时，拆成前置子步骤单独立项。
3. **R3 不声称未验证的完成**：每条完成判据必须对应一条真实执行记录（命令 + 退出码 + 日志路径）。做不到就写明做不到。

---

## 2. 双重保证的定义

### 2.1 保证 A：代码执行逻辑（Logic Guarantee）

每个原子步骤交付前，必须产出一张**逻辑追踪表**，证明改动在真实调用链中生效，且生命周期闭环。表格式固定：

```text
[LG-表] 步骤编号：P1-3
| 锚点符号 | 修改前签名/行为 | 修改后签名/行为 | 全部调用点(数量+文件) | 触发路径(SQL或测试) | 销毁/回退路径 |
```

填写规则：

- **调用点数量**用 `rg -n '<symbol>'` 实测，不得照抄设计文档表格（设计文档 §23.1 自己声明"不代表全量调用点"）。
- **触发路径**必须是一条具体 SQL 或测试用例名，能从外部观察到该代码路径被执行（EXPLAIN 字段、计数器、日志或断点等价物）。
- 涉及扫描状态的步骤，追加生命周期行：`begin → first gettuple → rescan → end → error cleanup` 五个时点的状态断言。
- 涉及磁盘/WAL 的步骤，追加生命周期行：`build → insert → scan → vacuum → crash/replay → old-version open`。
- 每个新增字段必须回答"谁写、谁读、谁复制（copy/out/read）、谁重置（rescan/end）、序列化版本门控值"五行；答不全不得交付。

### 2.2 保证 B：测试用例（Test Guarantee）

每个原子步骤的测试必须满足：

1. **先失败后通过**：实现前先把测试写入仓库并执行一次，记录失败输出；实现后同一命令通过。只有"实现后通过"没有"实现前失败"的，视为验证不足。
2. **三层覆盖**（按步骤性质裁剪，但至少两层）：
   - L1 单元/纯函数测试（新增 `src/test/ut/diskann/` 目标，秒级）；
   - L2 SQL 回归（`src/test/regress/sql/diskann_include.sql` 新文件 + expected，端到端）；
   - L3 专项脚本（故障注入、双会话并发、memcheck 运行，`src/test/diskann_include_tools/` 脚本）。
3. **回归不退化**：现有 `diskann_vector` regression 在每步收口时必须通过；这是进入下一步的前置条件。
4. **判据映射**：步骤定义中的每条"完成判据"必须映射到至少一个具体测试名或脚本名，写在证据记录里。

---

## 3. 原子步骤内的固定循环（每步必走）

```text
1. claim    ：在状态表把该步标为 in_progress，写日期
2. 读       ：重读设计文档对应章节 + 基线源码锚点（rg 定位，不信行号）
3. 测试先行 ：写本步测试 → 执行 → 记录失败输出（保证 B 第 1 条）
4. 实现     ：最窄实现，只动本步 allowed 范围
5. 窄验证   ：本步 L1/L2 测试通过
6. 逻辑表   ：填写 LG-表，rg 实测调用点
7. 宽验证   ：diskann_vector 回归 + 受影响模块回归
8. 审计     ：git diff --check + §7 rg 审计清单 + 逐项分类记录
9. 证据     ：VERIFICATION.md 追加条目（格式见 §6）
10. 提交    ：单一目的 commit，消息格式见 §6.2
11. 更新状态表 → 下一步
```

任一环失败：回到本环修复；修不动则该步标 `blocked`，写明阻塞点和需要的决定，停止扩大修改。

---

## 4. 任务清单

> 每步给出：锚点（设计文档章节）、allowed 范围（文件）、LG 重点、测试重点、完成判据。
> 顺序不可颠倒；依赖关系如各阶段首行所述。

### 阶段 S0：环境固化（零业务代码）

**S0-1 工作区清点与基线固化**
- 锚点：设计文档 §1.1 规则 1/2
- 范围：只读 + 新建 `docs/diskann_include/` 目录
- LG：`git rev-parse HEAD` 输出 = 基线 commit；50 个 dirty 文件逐个标注"用户保留 / 属于本任务 / 废弃"并经用户确认
- 测试：`make fastcheck PART=0`（或当前构建树等价命令，按 `OPENGAUSS_BUILD_INSTALL_RUN_GUIDE.md` 确定）在未改动代码上通过一次，日志存档为基线
- 判据：基线日志存在且通过；dirty 清单获用户书面确认
- 产出：`docs/diskann_include/BASELINE.md`

**S0-2 分支与构建链路**
- 范围：git 分支 `feature/diskann-include`；确认 debug 增量编译链路（编译单目录级可用）
- 判据：一次单目录增量编译成功且耗时可接受（记录分钟数）

**S0-3 测试骨架先行**
- 范围：新增 `src/test/regress/sql/diskann_include.sql`（空壳+冒烟用例）、`src/test/diskann_include_tools/`（故障注入脚本骨架，对应设计文档 §19.3 六个注入点 + §24.6 场景，全部标记 xfail）
- 判据：骨架入库；每个 xfail 脚本有编号并与后续步骤挂钩（见各步"点亮"要求）

### 阶段 P0：地基修复（无新用户语法，依赖 S0 全部完成）

**P0-1 元组 helper 抽取与容量检查**
- 锚点：设计文档 §11.1、§9.3
- 范围：`diskannbuild.cpp`、`diskannutils.cpp`、`diskann.h`（仅抽取，不改行为）
- LG：`InsertTuple/InsertVectorIntoPage/AddTupleToSort` 调用点实测；容量检查从 Assert 改为 ERROR 的触发 SQL（超长变长值）
- 测试：L1 容量 helper 边界单测；L2 现有回归不变
- 判据：纯重构无行为变化（回归全过）+ 容量检查在 release 语义下可触发

**P0-2 rescan 与逐轮状态修复**
- 锚点：§4.13、§16.3
- 范围：`diskannscan.cpp`、`diskann.h`
- LG：生命周期五时点断言表；`roundVisited` 与跨轮 cache 的读写者清单
- 测试：L2 prepared statement 连续两次不同参数执行，结果与计数独立且正确（设计文档 §24.3 "candidates/visited/filter cache 没有串查询"）；预算翻倍后 visited 真实增长
- 判据：该用例实现前失败、实现后通过

**P0-3 内存上下文拆分**
- 锚点：§16.3（scanCtx/batchCtx/returnTupleCtx）
- 范围：`diskannscan.cpp`、`diskann.h`
- LG：opaque 内 C++ 容器与 context 的归属表；`New`/placement 与销毁路径成对
- 测试：L3 memcheck 构建跑 §24.3 定向用例，无 use-after-free/leak
- 判据：memcheck 报告干净（或仅存遗留问题且逐条归档）

**P0-4 MetaPage 双格式读取**
- 锚点：§9.2、§9.2.1
- 范围：`diskann.h`、`diskannbuild.cpp`（reader）、`DiskAnnInitPage` 收口
- LG：v1 前缀长度的编译期断言；`DiskAnnInitPage` 全部创建点清单（设计文档列举的 6 类页面）
- 测试：**格式单测先行**——v1 旧索引读回不变 / v2 读到追加字段 / 未知版本报可诊断错误 / 截断 metaBytes 报错 / `params` 强制 NULL。这组单测必须在实现前写好并失败
- 判据：五类格式用例全过；现有 v1 索引（基线数据）查询结果不变

**P0-5 forkNum 传递链修复**
- 锚点：§11.3、§19.1
- 范围：build 路径 helper（`CreateMetaPage/CreateRelationLockPage/DiskAnnPageExtension/DiskAnnUpdateMetaPage` 及 PQ/Graph helper）
- LG：`rg 'MAIN_FORKNUM'` 全量清单逐条分类（build 应传参 / 在线保留 MAIN）
- 测试：L3 unlogged 表重启后 init fork 恢复 v2 格式并完成一次查询和 INSERT（点亮 S0-3 对应注入脚本）
- 判据：unlogged restart 用例通过

**P0-6 VACUUM tombstone 与 immediate-delete 门控**
- 锚点：§12.1-12.3、§4.12
- 范围：`diskann.cpp`（或新 `diskannvacuum.cpp` + Makefile）、`tableam.cpp`/`execUtils.cpp` 门控、`diskanndelete.cpp` 保留旧路径
- LG：状态机（ALLOCATING→LIVE→VACUUM_DEAD→RECLAIMABLE）读写者矩阵；版本分流入口（不得残留 `ItemIdMarkDead` 旁路，rg 审计）
- 测试：L3 双会话双快照（§24.5：旧快照见旧版本、新快照见新版本）；churn 数据集召回不突降；`IndexBulkDeleteResult` 统计字段正确
- 判据：双快照不漏行；高 churn 后图仍可导航

**✂ 阶段门禁 G0（全部 P0 完成后、进入 P1 前执行，缺一不可）**
- debug 构建 + 现有 `diskann_vector` + `parallel_schedule0` 全过
- memcheck 定向用例干净
- 旧格式 v1 索引 open/insert/search 回归不变
- unlogged restart 通过
- 向用户提交 G0 证据包，**等待人工放行**

### 阶段 P1：INCLUDE + 覆盖返回（依赖 G0）

**P1-1 DDL 放开与校验**
- 锚点：§8.1、§8.1.1、§4.10
- 范围：`indexcmds.cpp`
- LG：`get_index_amroutine` 生命周期（syscache 释放前复制字段）；`numberOfKeyAttributes` 判定修改前/后全部调用点
- 测试：L2 四联用例——一个 key+INCLUDE 成功 / 两个 key 拒绝 / UBtree INCLUDE 行为不变 / 无 handler AM 错误不变（§8.1.1 完成判据）；类型白名单、表达式列拒绝、Ustore/分区/immediate_delete 组合拒绝（§24.1）
- 判据：四联全过且与基线行为 diff 为空（后两条）

**P1-2 reloptions 三处成对注册**
- 锚点：§6.1、§6.3
- 范围：`reloptions.cpp`、`DiskAnnOptions`、`diskannoptions_internal`
- LG：默认值常量唯一定义点；`offsetof` 表与结构体字段对照
- 测试：L2 未知参数报错 / 上下界 / `ALTER INDEX SET in_filter` 生效 + relcache invalidation / 原地改 `max_inline_payload` 拒绝并提示 REINDEX
- 判据：全部用例过；三处任缺一处的失败模式已在开发中实际复现并记录（证明三处确实成对）

**P1-3 完整 IndexTuple + 一行一节点 + 冻结点**
- 锚点：§9.1、§10、§11.2-11.5
- 范围：`diskannbuild.cpp`、`diskann.cpp`（insert）、`diskannutils.cpp`（`Link` 跳过 duplicate 合并）
- LG：`values/isnull` 从 BuildCallback/INSERT 到 `index_form_tuple` 的完整传递链；`heaptidsLength` 不变量三态表；两条 frozen 路径（build + 首条在线 INSERT）
- 测试：L2 §24.4 重复向量不同 payload 不串值；§24.2 cosine 显式投影向量走 join-back 且返回 Heap 原值；frozen 点不作为结果；§24.5 更新 INCLUDE 列产生新索引项；并行构建含 INCLUDE 一致性（§24.1）
- 判据：重复向量用例实现前失败、实现后通过；`rg 'Datum .+\[1\]'` 审计仅剩合法 key-only helper

**P1-4 catalog：diskanncanreturn + 升级 SQL**
- 锚点：§8.2、§8.3
- 范围：`diskann.cpp/h`、`pg_am.h`、`builtin_funcs.ini`、upgrade/rollback SQL
- LG：两套能力来源（DDL 用 amcaninclude / Index-Only 用 pg_am.amcanreturn）各自触发 SQL；OID 冲突 rg 全仓检查记录
- 测试：L2 新装库与升级库分别查 `pg_am.amcanreturn`/`pg_proc` 结果一致；无 INCLUDE 索引不产生 Index-Only Path（COUNT(*) 用例）；rollback SQL 在降级前置检查下行为正确
- 判据：**OID 编号经用户按 openGauss 分配流程确认后方可提交**（人工决策点）
- 点亮：`pg_am` 更新 SQL 的升级/回滚往返

**P1-5 planner：returnableHeapAttrs 与路径生成**
- 锚点：§13.1、§13.3、§13.4
- 范围：`relation.h`、`plancat.cpp`、`copyfuncs.cpp`（`_copyPartialIndexOptInfo`）、`hypopg_index.cpp`、`indxpath.cpp`、`pathnode.cpp`
- LG：bitmap 的 Heap attno 编码示例推演；三个构造/复制入口（plancat/copy/hypopg）逐个确认；`IndexPath.indexOnly` 在普通/并行/反向路径的初始化点
- 测试：L2 覆盖判定矩阵（只投影 INCLUDE / 投影向量 key / 投影未覆盖列 / 距离表达式 / rowmark）；`check_ann_index_only()` 与 `check_index_only()` 行为隔离（普通 AM 的 Index-Only 回归不变）
- 判据：覆盖矩阵全过；非 DiskANN AM 的 Index-Only 计划无任何 diff

**P1-6 Plan 五处成对修改与序列化**
- 锚点：§14（最小传播表）、§13.4
- 范围：`plannodes.h`、`createplan.cpp`、`copyfuncs.cpp`、`outfuncs.cpp`、`readfuncs.cpp`、`setrefs.cpp`、working version 常量
- LG：§14 最小传播表逐格填写实测结果；`index_only_scan=true` 时 setrefs 的 INDEX_VAR 重写前后 targetlist 对照
- 测试：L1 copy/out/read round-trip 逐字段相等单测；旧版本计划反序列化默认值（`false/NIL/NONE`）安全；L2 EXPLAIN 计划名 `Ann Index Only Scan`
- 判据：round-trip 单测实现前失败、实现后通过；ENABLE_MULTIPLE_NODES 配置编译通过（即使运行时门控 single-node）

**P1-7 executor Index-Only 分支 + OPFusion 门控**
- 锚点：§17、§20.5
- 范围：`nodeAnnIndexscan.cpp`、`execnodes.h`、`opfusion_util.cpp`、`explain.cpp`
- LG：slot 语义在 ExecInit 阶段确定的分支表；`xs_want_itup` 设置顺序（beginscan→set→rescan）；`iss_VMBuffer` 释放点清单（End/ReScan/错误清理）
- 测试：L2 VACUUM 后 `Heap Fetches: 0` 与 VM 清除对照非零且结果一致（§24.2）；predicate lock 分支（`heapVisited`）；L3 memcheck 下 `xs_itup` 生命周期（返回后 Buffer 已释放仍可用）；普通 ANN 查询 Fusion 行为不变
- 判据：VM 双路径用例过；memcheck 干净

**✂ 阶段门禁 G1（24.9 Phase 1 行全项）**
- debug + release 编译；catalog 生成流程完整；memcheck 优先项全过
- DDL / 完整 tuple / duplicate / frozen / Index-Only / VM/MVCC / copy-out-read round-trip 全过
- 单机 + ENABLE_MULTIPLE_NODES 编译
- 向用户提交 G1 证据包，**等待人工放行**

### 阶段 P2：In-Filter（依赖 G1）

**P2-1 谓词白名单分类（纯 planner）**
- 锚点：§13.2
- 范围：`indxpath.cpp`
- LG：`annQuals/annQualCols` 生产者/消费者闭环（设计文档 §4.15：字段已存在但无人使用）
- 测试：L1 分类表单测（§13.2 全部形态 + 拒绝形态：OR/NOT/LIKE/regex/volatile/UDF/SubPlan/跨表）；L2 EXPLAIN `Index Filter` 显示
- 判据：分类单测全过

**P2-2 FilterDesc 编译与三值逻辑**
- 锚点：§15.1、§15.2
- 范围：`nodeAnnIndexscan.cpp`（或新 `annIndexFilter.cpp`）、`diskannfilter.h`
- LG：FmgrInfo（filterFnCtx）与参数（filterArgCtx）双 context 归属表；rescan 重建链
- 测试：L1 三值逻辑全表——NULL 操作数/IS NULL/空数组/NULL 数组/NaN/定长 CHAR 尾空格/collation；by-reference 参数 datumCopy 后 rescan 不串值
- 判据：三值逻辑单测全过（实现前失败）

**P2-3 AM 内过滤 + filter cache**
- 锚点：§15.4、§16.1、§16.4
- 范围：`diskannfilter.cpp`（新）、`diskannscan.cpp`、Makefile
- LG：锁内复制/解锁比较的字段清单；`CopyLiveCandidateTupleToContext` 校验链（pageId/pageType/ItemId/边界/状态/TID 一致）
- 测试：L2 pushed filter 与 executor recheck 结果 100% 一致（§24.3）；dead payload 不执行 filter 仍导航；L3 memcheck（Datum 生命周期）；并发 VACUUM 与候选批读取（§24.5）
- 判据：一致性用例过；锁内函数调用审计为零

**P2-4 迭代扩搜与 strict batch**
- 锚点：§16.2、§16.3
- 范围：`diskannscan.cpp`
- LG：`GetDistance` float→double 收敛点标注；首次输出后不追加候选的守卫条件
- 测试：L2 输出 exact distance 单调不降；float32 舍入 tie 用例（§24.3）；过滤后少于 K/等于 K/无结果三态（§24.3）；`limitk<=0`/LIMIT 0 行为
- 判据：单调性用例实现前失败、实现后通过

**P2-5 成本模型与路径竞争**
- 锚点：§13.5
- 范围：DiskANN cost estimator、`costsize.cpp`
- LG：成本归属表（图访问/payload 比较/Heap fetch/qpqual 各计一次，无重复计数）；s 的 clamp 边界
- 测试：L1 成本单调性（固定 N/K 变 p；固定 N/p 变 K）；s=0/NaN/N=0 无除零；L2 极强过滤 + B-tree 时标量路径可胜出（§24.8 计划选择）
- 判据：单调性单测过；无重复计数的 EXPLAIN 计数一致性（§24.8）

**P2-6 EXPLAIN 统计与计数不重复**
- 锚点：§21
- 范围：`explain.cpp`、`execnodes.h`、instrumentation
- LG：`Index Filter` 与 `Rows Removed by Filter` 的计数点对照（AM 拒绝 vs ExecScan 复核）
- 测试：L2 §24.8 计数不变量：visited ≥ filter evaluated ≥ eligible，Heap Fetches 与 VM 一致；统计字段九项全展示
- 判据：不变量用例全过

**✂ 阶段门禁 G2（24.9 Phase 2 行全项 + 最终验收）**
- debug/release/memcheck 三构建全过；新增 filter 源文件 Makefile 完整
- 三值逻辑 / prepared 参数 / 多轮扩搜 / 严格顺序 / EXPLAIN 统计 / 极严过滤 fallback 全过
- §19.3 六个故障注入点全部点亮（对应 S0-3 脚本去 xfail）+ standby replay + unlogged restart
- §25 A/B/C/D 对照原始数据（等预算与等召回分开报告）
- 向用户提交 G2 证据包与验收报告，**等待人工放行**

---

## 5. 每步收口的固定审计命令

```bash
git diff --check
rg -n 'Datum .+\[1\]|bool .+\[1\]' src/gausskernel/storage/access/datavec/diskann* src/include/access/datavec/diskann.h
rg -n 'MAIN_FORKNUM' src/gausskernel/storage/access/datavec/diskann* src/include/access/datavec/diskann.h
rg -n 'IsMarkDeleted|ItemIdIsDead|ItemIdMarkDead|\.deleted' src/gausskernel/storage/access/datavec/diskann* src/include/access/datavec/diskann.h
rg -n 'T_AnnIndexScan|AnnIndexScan' src/common src/gausskernel src/include
```

结果不要求为零，但每次必须**逐条分类记录**到本步证据：长度 1 数组是否只用于合法 key-only helper、MAIN fork 是否只在线上路径、格式 2 是否脱离 LP_DEAD 旁路、Plan visitor 是否覆盖新表达式字段。

---

## 6. 证据与提交规范

### 6.1 VERIFICATION.md 条目格式（每步一条）

```text
## [P1-3] 完整 IndexTuple + 一行一节点
- 日期 / commit：
- 实现前失败证据：<测试命令、退出码、失败输出摘要、日志路径>
- 实现后通过证据：<同上>
- 回归证据：diskann_vector <退出码、日志路径>
- LG-表：<链接或内嵌>
- 完成判据 → 测试映射：<判据1 → test_xxx；判据2 → ...>
- rg 审计分类：<逐条结论>
- 已知问题（如有）：KNOW_ISSUES.md #n
```

### 6.2 Commit 规范

- 单一目的；消息首行 `[P1-3] full IndexTuple with one-row-per-node`。
- Commit body 附代码事实核对表：修改前/后签名、调用点数量、编译配置、对应测试。
- 每个阶段门禁通过后允许一次 squash 整理；门禁前禁止合并步骤。

---

## 7. 阻塞与人工决策点

### 触发立即 block 的条件

- 任一现有 DiskANN 回归失败且无法在 20 行内归因修复；
- 发现设计文档与基线代码事实不符（符号不存在、行为描述不一致）；
- 需要改动本步 allowed 范围之外才能继续；
- memcheck/ASAN 报告本步引入的错误；
- 故障注入产生无法解释的页面状态。

block 时：停止扩大修改，在状态表记 `blocked` + 最小复现 + 需要的决定，向用户交回控制。

### 预置人工决策点（2026-08-14 起按 D-002 调整）

> 用户已授权自主持续执行（见 `openGauss-server/docs/diskann_include/DECISIONS.md` D-002）：
> 下列决策点改为**执行者按最保守方案自决 + DECISIONS.md 留痕 + 证据齐备后继续**；
> 门禁真实失败或无法验证时仍必须停止。OID 分配若无法走正式流程，选用最低冲突
> 风险方案并全仓 rg 验证后留痕使用。

| 时机 | 决策 | 自决规则 |
| --- | --- | --- |
| S0-1 | dirty 文件归属确认 | 保守默认：一律不动（D-001 已决） |
| P1-4 | 内置函数 OID 分配 | 全仓冲突检查后选用候选 OID，留痕 |
| G0/G1/G2 | 阶段放行 | 证据全过→记录后继续；任一失败→停 |
| G2 | §25 性能门槛未达标时的偏差评审 | 未达标→保留原始数据、如实标注、不宣称达标 |
| 任何时候 | 升级/回滚 SQL 的 catalog 评审 | 留痕待用户事后复核 |

---

## 8. 回退策略

- 所有新行为挂默认关闭开关：reloption `in_filter`（默认开但路径需 DDL 支持）、格式版本判断、GUC `diskann_filter_mode`。
- 任一步引入回归且无法立即修复：`git revert` 该步单一 commit，回到上一步绿态；状态表记 `reverted` 并附原因。
- 阶段门禁失败：整阶段可 revert（每步独立 commit 保证可二分）。

---

## 9. 任务状态表（执行者维护，每步更新）

| 步骤 | 状态 | commit | 完成判据全过? | LG-表 | 证据链接 | 备注 |
| --- | --- | --- | --- | --- | --- | --- |
| S0-1 | done | a5c6c6ee82 | 是 | — | BASELINE.md + evidence/S0-1 | fastcheck 933ok/74既有失败(D-006增量判据)；dirty分类D-001 |
| S0-2 | done | a5c6c6ee82 | 是 | — | evidence/S0-2/incremental_build.log | 增量编译+安装+重启 8m19s |
| S0-3 | done | a5c6c6ee82 | 是 | 不适用 | evidence/S0-3/smoke_skeleton.log | 9 xfail 脚本+注册表 |
| P0-1 | done | a5c42cdfcd | 是 | evidence/P0-1/LG.md | evidence/P0-1 | L1前败后过；回归与基线逐字节一致 |
| P0-2 | done | cb27bb01b7 | 是 | evidence/P0-2/LG.md | evidence/P0-2 | 缺陷复现(LATERAL qid=2旧候选)→修复✅；实现中SIGSEGV被抓(clear()雷)并修正；回归零退化 |
| P0-3a | done | 8c835608ae | 是(D-007拆分) | evidence/P0-3/LG.md | evidence/P0-3 | PQ wrapper/graphStore 释放；三上下文拆分→P1-3 |
| P0-4 | done | 3afca2045d | 是 | evidence/P0-4/LG.md | evidence/P0-4 | 版本感知读取+消费者修复；PQ运行时不可测D-008 |
| P0-5 | done | f6c5dbe49e | 是 | evidence/P0-5/LG.md | evidence/P0-5 | unlogged崩溃后ERROR→有效空索引；fi_06 pass-partial |
| P0-6a | done | 2634b9b14b | 是(D-009分期) | evidence/P0-6/LG.md | evidence/P0-6 | VACUUM统计不再清零；门控+deferred bulkdelete→P1-3 |
| G0 | done | 4b4bef9071 | 通过 | — | evidence/G0/REPORT.md | fastcheck 失败集合与基线逐项一致(74/74零新增)；memcheck按D-010推至G1前 |
| P1-1 | done | e552185202 | 是 | evidence/P1-1/LG.md | evidence/P1-1 | 四联+8用例前败后过；KNOWN_ISSUES#5(ALTER拦截→P1-3) |
| P1-2 | done | b4d038b689 | 是 | evidence/P1-2/LG.md | evidence/P1-2 | 三处成对+ALTER拒绝；常量自决留痕 |
| P1-3a | done | a64af19964 | 是 | evidence/P1-3/LG.md | evidence/P1-3 | 两阶段证据链(5/4→4/4) |
| P1-3b | done | 9c8313b1ea | 是 | evidence/P1-3/LG_3b.md | evidence/P1-3 | v2 tail+门控+ALTER拦截(#5关闭)；上下文拆分→P1-7(D-012) |
| P1-4 | done | 6fb9926eb9 | 是(新装库路径) | evidence/P1-4/LG.md | evidence/P1-4 | OID 8601自决留痕；升级路径D-013分期 |
| P1-5 | done | ede895d330 | 是(D-014统一闭环) | evidence/P1-5/LG.md | evidence/P1-5 | bitmap三入口+indexOnly传递；闭环在P1-7 |
| P1-6 | done | 9ed0171e74 | 是(D-014统一闭环) | evidence/P1-6/LG.md | evidence/P1-6 | 五处成对+版本93099门控 |
| P1-7 | done | 09319cb910 | 是(主功能) | evidence/P1-7/LG.md | evidence/P1-7 | Ann Index Only Scan 闭环✅；阻塞#6(immediate_delete挂死)待解 |
| G1 | done | 55084e19f4 | 通过 | — | evidence/G1/REPORT.md | fastcheck 零新增 + memcheck 零 ASAN（worktree 构建）；M-N 编译验证归 G2 前 |
| P2-1 | done | 846d62fe23 | 是 | evidence/P2-1/LG.md | evidence/P2-1 | In-Filter/Post-Filter 三态；门控独立推导教训 |
| P2-2 | done | ad509232b1 | 是 | evidence/P2-2/LG.md | evidence/P2-2 | L1 22断言+编译链；AM消费归P2-3 |
| P2-3 | done | 10f022dc00 | 是 | evidence/P2-3/LG.md | evidence/P2-3 | AM拒绝31实证+pushed==truth；#7 rescan cache 陈旧→P2-4 |
| P2-4 | done | 86c6899d10 | 是 | evidence/P2-4/LG.md | evidence/P2-4 | strict batch+A==B+不足K不伪造；#7关闭 |
| P2-5 | done | 8e336a14b4 | 是 | evidence/P2-5/LG.md | evidence/P2-5 | L1 17断言+selectivity 生效(rows20vs60)；大表效应归G2校准 |
| P2-6 | done | adea833561 | 是 | evidence/P2-6/LG.md | evidence/P2-6 | Evaluations≥Removed+HeapFetches=0；轮次类3项→G2评审 |
| G2 | done | d98de98c80 | 通过(2项如实边界) | — | evidence/G2/REPORT.md | fastcheck零新增+memcheck零ASAN+M-N环境边界D-018；**V1交付完成** |

状态取值：`todo / in_progress / done / blocked / reverted`。`done` 的充要条件 = 第 2 节双重保证齐备 + 回归通过 + 证据落盘。
