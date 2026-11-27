# 原生湖完整 MERGE INTO 实现与回归报告

日期：2026-11-28

仓库：`qiumingcheng/openGauss-server`

分支：`codex/migrate-native-lake`

工作区：`/data/qiumc/code/cpp/opengauss-native-lake`

## 结论

原生湖表已经接入完整的 openGauss MERGE action 语义。当前实现覆盖以下能力：

- 任意数量的 `WHEN MATCHED` 和 `WHEN NOT MATCHED` 子句。
- 每个子句的 `AND` 条件和 action 后的 `WHERE` 条件。
- `UPDATE`、`DELETE`、`INSERT`、`DO NOTHING`。
- 按 SQL 文本顺序选择第一个满足条件的 action。
- 多个 INSERT action、默认值插入和不同 action 混合。
- 重复源行命中同一湖表逻辑行时的 cardinality violation。
- native lake row locator、行锁、EPQ、事务发布和回滚路径。

实现已在本机 Debug/O0＋断言构建上编译、安装并运行回归。代码和本报告会作为新的本地提交保存。当前未执行远端 push。

## 变更范围

| 层次 | 文件 | 处理内容 |
| --- | --- | --- |
| 后端语法 | `src/common/backend/parser/gram.y` | 增加 MATCHED/NOT MATCHED 条件、DELETE、DO NOTHING，并合并 AND 与 suffix WHERE |
| 前端逻辑解析 | `src/common/interfaces/libpq/frontend_parser/gram.y` | 同步客户端逻辑解析器语法，避免客户端侧提前拒绝完整 MERGE |
| 语义分析 | `src/common/backend/parser/parse_merge.cpp` | 为每个 action 转换条件，补充 DELETE 权限和 INSERT action 校验，移除原有两 action 限制 |
| Query rewrite | `src/gausskernel/optimizer/rewrite/rewriteHandler.cpp` | 允许 DO NOTHING action 通过 MERGE targetlist rewrite |
| 执行器 | `src/gausskernel/runtime/executor/execMerge.cpp` | 按 action 顺序执行 MATCHED/NOT MATCHED，加入 DELETE 和 DO NOTHING，保留 heap EPQ |
| 外表准入 | `src/gausskernel/runtime/executor/execMain.cpp` | MERGE 外表要求 INSERT、UPDATE、DELETE 回调及能力位 |
| native lake FDW | `contrib/og_lake/lake_modify.cpp`、`src/include/access/og_lake.h` | 增加 EPQ 失败、重选 action 和 prepared locator 的协作接口 |
| action 标志 | `src/include/executor/exec/execMerge.h` | 增加 `MERGE_DELETE` |

原生湖的 planner 仍生成普通 row `ModifyTable` 路径。native lake 外表不是 CU 表，因此不会进入 CU 专用 `VecMerge` 执行器。

## 关键语义

### Action 选择

执行器分别保存 MATCHED 和 NOT MATCHED action 列表，按原 SQL 顺序遍历。第一个满足 `WHEN` 与 suffix `WHERE` 的 action 被执行，后续 action 不再参与该源行。这保证了多个条件 action 的互斥选择语义。

### MATCHED action

- `UPDATE` 通过现有湖表 FDW 更新回调写入新版本。
- `DELETE` 通过湖表 FDW 删除回调写入删除版本。
- `DO NOTHING` 直接结束当前匹配行。
- action 条件可以读取目标和源两侧列。

### NOT MATCHED action

- `INSERT` 使用原有 MERGE projection 和默认值展开路径。
- `DO NOTHING` 直接跳过当前源行。
- action 条件和 INSERT 表达式只能读取源侧列，目标列引用仍由语义分析拒绝。

### 事务与重复命中

native lake 继续使用逻辑 row locator 和私有事务版本。一个 MERGE statement 内同一逻辑 row 被第二个源行再次修改时，执行器抛出 cardinality violation。事务回滚会撤销湖表私有版本和删除/插入意图，不发布部分结果。

## 复现并修复的并发缺陷

### EPQ 后目标行消失

复现方式是 waiter 先按旧快照匹配目标行，holder 删除该行并提交，waiter 随后在湖表 FDW 回调中进行 EPQ。

旧路径把 outer join 返回的非 NULL joined tuple 当成仍然 MATCHED 的行。由于刷新后的目标侧 locator 实际为 NULL，后续更新可能错误地作用到其他行。

修复后，MERGE EPQ 检查刷新 joined tuple 的 locator。locator 为 NULL 时标记 MATCHED 失败，执行器转入 NOT MATCHED action。回归结果如下：

```
10|reinsert|3
```

### EPQ 后目标键发生变化

holder 将 `id=20` 更新为 `id=21` 并提交，waiter 的源行仍为 `id=20`。修复前 waiter 可能沿用旧 action 结果，无法正确执行 NOT MATCHED INSERT。

修复后，刷新行的 locator 缺失会进入 NOT MATCHED 路径。回归结果如下：

```
20|reinsert-key|12
21|holder-key|11
```

### EPQ 后目标列改变了 action 条件

holder 将同一逻辑行的 `flag` 从 0 更新为 1。waiter 先根据旧值投影 action，再等待行锁。旧路径可能用旧 flag 选择 fallback action。

修复后，如果 EPQ 返回新的 MATCHED joined tuple，native lake FDW 保存最新 locator，执行器重新从第一条 MATCHED action 开始选择。这样 `WHEN` 条件和目标列引用都基于最新提交版本重新计算。

回归结果如下：

```
10|holder-first|2
```

同样的重选路径还验证了条件 DELETE。holder 把 flag 改为 1，waiter 的第一条 action 为 DELETE，最终行数为 0。

如果 EPQ 重选后没有 action 满足条件，prepared locator 会被显式释放。无 action 回归结果如下：

```
10|holder-no-action|1
```

## 动态回归结果

测试使用独立 Debug 实例：

- 构建源码：`/data/qiumc/code/cpp/.build/opengauss-native-lake/union-all-integration-20261123-212345.ZKGg/debug-source`
- 安装目录：`/data/qiumc/code/cpp/.build/opengauss-native-lake/union-all-integration-20261123-212345.ZKGg/debug-install`
- 数据目录：`/data/qiumc/code/cpp/.runtime/opengauss-native-lake/union-212345-debug/data`
- Unix socket：`/data/qiumc/code/cpp/.runtime/opengauss-native-lake/union-212345-debug/socket`
- 端口：55602

| 用例 | 结果 |
| --- | --- |
| MATCHED UPDATE＋NOT MATCHED INSERT | PASS，返回 `MERGE 2` |
| MATCHED DELETE＋UPDATE＋DO NOTHING | PASS，按 action 顺序得到删除、更新和跳过结果 |
| NOT MATCHED 条件 DO NOTHING 后的 INSERT fallback | PASS，满足条件的源行被跳过，其余源行插入 |
| 多个 NOT MATCHED INSERT action | PASS，第一条条件不满足时继续选择后续 action |
| MATCHED 与 NOT MATCHED suffix WHERE | PASS |
| MATCHED target-qualified 条件 | PASS |
| `INSERT DEFAULT VALUES` | PASS，得到 `202/default-value/7` |
| 仅 MATCHED DELETE | PASS |
| 仅 NOT MATCHED DO NOTHING | PASS，返回 `MERGE 0` |
| 重复源键命中同一目标行 | PASS，抛出 cardinality violation，回滚后原值保持 `2/suffix/2` |
| 普通 heap 表多 action MERGE | PASS，确认通用 row executor 没有回归 |
| EPQ 同键目标列变化 | PASS，得到 `holder-first` |
| EPQ 同键条件 DELETE | PASS，最终行不存在 |
| EPQ 目标删除后转 NOT MATCHED | PASS，得到 `reinsert` |
| EPQ 目标键变化后转 NOT MATCHED | PASS，得到 `reinsert-key` 和 holder 新键 |
| EPQ 重选后无 action | PASS，holder 版本保持不变 |
| NOT MATCHED 条件引用目标列 | PASS，语义分析明确拒绝 |

并发 waiter 均在 holder 提交前保持阻塞，holder 提交后 waiter 正常完成。没有出现脏写、错误目标更新或未释放事务结果。

## 构建与静态检查

执行过以下验证：

- backend parser、MERGE executor、native lake FDW 对象编译成功。
- `gaussdb` Debug/O0＋断言重新链接成功。
- 新二进制安装并启动成功，`gs_ctl query` 状态为 `Normal`。
- frontend parser grammar 由 Bison 重新生成并编译 `gram.o` 成功。
- `git diff --check` 通过。
- 源码工作树中的修改文件与实际测试构建源码逐个 `cmp` 一致。
- native lake MERGE `EXPLAIN` 显示普通 row `ModifyTable` 和 `Foreign Scan` 路径。

当前 `version()` 仍显示迁移基线 `16ea3ae0`。这是既有构建身份问题，不能用该字符串代替本地 WIP commit 身份。报告使用构建路径、时间和文件校验作为本次实例标识。

## 代码设计与性能审视

这次修改把 action 选择留在通用 MERGE 执行器，把 locator 解析、行锁、最新版本查找和 EPQ 注入留在 native lake FDW。两层通过三个窄接口协作：

- `LakeMergeRecheckFailed`
- `LakeMergeRecheckRetry`
- `LakeMergeRecheckDiscard`

这种分层避免让执行器直接访问湖表元数据，也避免 FDW 回调修改通用 MERGE action 列表。prepared locator 只在一次重选 action 中消费，DO NOTHING 或无匹配 action 会释放对应 read view。

每个冲突行仍然需要逻辑 row-head 锁、最新版本定位和必要的 EPQ。该路径优先保证正确性，尚未宣称批量锁、row locator 缓存或大批量 MERGE 的最终性能收益。高并发热点和百万级源数据需要单独的 Release 基线。

## 当前边界与后续处理建议

| 项目 | 当前处理 |
| --- | --- |
| 单目标 native lake MERGE | 已支持完整 action 语义 |
| row-lock 关闭 | 继续拒绝，因为湖表 EPQ 和 locator 正确性依赖行锁 |
| modifying CTE | 保持既有拒绝边界，未在本次 MERGE action 改造中扩大范围 |
| 文件来源 DML | 保持既有能力边界，未把只读外部文件路径误报为可写 MERGE |
| 分区父表、分布式多节点和 CU 向量执行器 | 未在本次 native lake 单节点 row 路径中扩大支持，需单独设计和回归 |
| 大批量性能 | 需要建立源行规模、热点比例、EPQ 冲突率和文件布局基线后再做缓存或批量化 |
| 长稳、崩溃恢复、物理 GC、远端对象存储、RAC | 仍按原生湖资格门单独验收 |

因此，本次可以把“湖表在现有单目标 row 执行路径上完整支持 MERGE action”作为已完成结果。不能把这组定向回归外推为所有分布式、分区、文件来源和长稳场景都已完成生产准入。

## 本地提交

本轮没有覆盖既有历史，没有 amend、rebase、reset 或 force push。实现和报告将使用新的普通本地提交保存。远端分支未修改。
