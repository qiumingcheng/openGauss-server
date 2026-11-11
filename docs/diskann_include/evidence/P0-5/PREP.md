# P0-5 准备笔记：forkNum 传递链完整审计（2026-08-14，只读）

## 1. 审计结论

`BuildIndex(..., forkNum)` 已有参数（diskannbuild.cpp:793），`diskannbuildempty_internal`
正确传入 `INIT_FORKNUM`（:947），`LogNewpageRange` 也正确使用 forkNum（:815-816）。
**但所有下层页面创建/更新函数要么硬编码 MAIN_FORKNUM，要么收了 forkNum 参数不用** →
unlogged 索引的 init fork 与 main fork 格式不一致（设计文档 §11.3/§19.1 判断实锤）。

## 2. 全量调用点清单（rg 实测 @ 6df94c2）

### 2.1 必改：build 路径硬编码 MAIN_FORKNUM

| # | 位置 | 函数 | 现状 | P0-5 改法 |
| --- | --- | --- | --- | --- |
| 1 | diskannbuild.cpp:147-160 | `CreateRelationLockPage(index)` | 无参，硬编码 MAIN（:152） | 加 forkNum 参数 |
| 2 | diskannbuild.cpp:162-168 | `CreateMetaPage(index, buildstate, forkNum)` | **收 forkNum 不用**，:168 硬编码 MAIN | 用参数（设计文档点名） |
| 3 | diskannbuild.cpp:231-241 | `DiskAnnPageExtension(index, pageLocker, buf)` | :233/:237 硬编码 MAIN | 加 forkNum 参数；build 路径传 buildstate 的 forkNum |
| 4 | diskannbuild.cpp:374 | `InsertTuple` 调 `DiskAnnUpdateMetaPage(..., MAIN_FORKNUM, ...)` | 硬编码 | InsertTuple 需知道当前 build 的 forkNum（随 building 场景） |
| 5 | diskannutils.cpp:109-114 | `DiskAnnUpdateMetaPage(..., forkNum, ...)` | **收 forkNum 不用**，:114 `ReadBuffer`(=main) | 用 ReadBufferExtended(forkNum) |
| 6 | diskannutils.cpp:1209-1231 | PQ table/centroids/offset 页创建 ×3 | 硬编码 MAIN（:1211/:1219/:1227） | forkNum 参数化（该函数由 build 调用，需确认调用链含 buildempty） |

### 2.2 需分类：GraphStore/iterator 的 `ReadBuffer(m_rel/index, ...)`

diskannutils.cpp :160（GetMetaPageInfo）、:199-398（GraphStore 读写）、:771、:798-1008
（iterator/搜索路径）。`ReadBuffer` 宏只访问 main fork。
- 在线 scan/insert：main fork 正确，不改。
- buildempty（heap=NULL、零元组）：GraphStore 遍历大概率不触达；但 metapage 读取
  （:160 GetMetaPageInfo 被在线和 build 共用）必须按 forkNum 读。
- 处置：P0-5 只改**确认在 build 路径上**的入口（上表 6 项）；GraphStore 深层逐函数
  fork 化留待 P0-5 实现时以"buildempty 全链路能否走通"为验收实测决定，改动面控制在
  必要最小（避免 R2 跨步扩大）。

### 2.3 明确不改：在线路径

在线 insert/scan/vacuum 的 main fork 使用全部正确保留。

## 3. 传递设计（最小面）

- `DiskAnnBuildState` 增加 `ForkNumber forkNum` 字段（设计文档 §19.1 建议），
  `BuildIndex` 入口统一赋值；:933 MAIN / :947 INIT 保持。
- 上表 6 处改为从 buildstate/参数取 forkNum。
- `DiskAnnNewBuffer`（utils:133）已正确接收 forkNum，无需改。

## 4. 测试设计

L3（点亮 fi_06_initfork_stages.sh 与 unlogged restart 场景）：
```sql
CREATE UNLOGGED TABLE u (id int, embedding vector(6));
CREATE INDEX ... USING diskann ...;
-- crash 模拟：重启实例
-- 断言：重启后 init fork 恢复出格式一致的空索引；INSERT+查询正常
```
修复前预期：init fork 内容错乱/缺失（重启后索引不可用或行为异常）——若基线本身
unlogged+diskann 就能工作（可能 init fork 空 = 空索引重建路径掩盖问题），则改用
"init fork 与 main fork 页数/内容比对"作为判据。实现时先跑基线行为确认可观察差异，
再固化（先失败后通过）。

## 5. 风险

- unlogged+diskann 组合若在基线就未被任何现有回归覆盖，修复的"实现前失败"证据
  需要构造页级比对（pg_relation_size 对 init fork）；若仍不可观察，降级为
  "代码审计清单 + 页数一致性断言"，记 DECISIONS 说明判据降级理由。
