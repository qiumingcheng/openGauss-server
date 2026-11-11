# LG-表 [P1-3a] 完整 IndexTuple + 一行一节点 + 冻结点修正

## 传递链（BuildCallback → AddTupleToSort → InsertTuple → InsertVectorIntoPage → index_form_tuple）

| 环节 | 修改前 | 修改后 |
| --- | --- | --- |
| BuildCallback | 收全列但压缩前丢列 | 原样传 values+isnull（tmpCtx 保护不变） |
| AddTupleToSort | `Datum insertValue[1]` 丢列；blkno 无条件入表 | 全列直传；InvalidBlockNumber 不入 blocksList/不计数（§11.2） |
| InsertTuple | 只用 values[0] | 全属性私有数组副本，[0] 换归一化向量；签名+isnull |
| InsertVectorIntoPage | `bool isnull[1]` | 全属性 index_form_tuple；payload 预算检查（max_inline_payload，§9.3 口径=fullItup−keyOnlyItup）|
| index_form_tuple | 单属性 | TupleDesc 全属性（INCLUDE 列在 pg_index/catalog 已就位，P1-1 放开）|

## 一行一节点 + 冻结点（§10）

- Link：INCLUDE 索引跳过 FindDuplicateNeighbor（一行一节点）；旧格式保留合并
  （判定=RelationGetNumberOfNumberOfAttributes>KeyAttributes，注意本仓库
  rd_index 无 indnkeyatts 字段，用 rel.h 的宏 rd_indnkeyatts——编译门禁抓过）
- GenerateFrozenPoint：INCLUDE 索引冻结点 Invalid TID + 全 NULL payload；
  非 INCLUDE 保持历史行为（合并路径依赖采样 TID）
- 在线首插 frozen 路径：全 NULL payload + Invalid TID（原本就 Invalid TID，补 payload）

## 两阶段证据链（冻结点修复是"有载"的）

| 阶段 | 二进制状态 | T2（重复向量+INCLUDE, LIMIT 10） |
| --- | --- | --- |
| 基线 | 合并开 | 4/4（合并掩盖） |
| 阶段一 | 一行一节点、**无**冻结点修复 | **5/4 ← 缺陷暴露**（phase1_frozen_bug.log） |
| 阶段二 | +冻结点修复 | **4/4 ✅**（phase2_frozen_fixed.log） |

## 判据 → 证据

| 判据 | 证据 |
| --- | --- |
| §24.4 重复向量不串值/不丢行 | T2 两阶段日志 |
| §24.5 UPDATE INCLUDE 列产生新索引项 | T3（UPDATE 后查询正确，21 行） |
| 非 INCLUDE 行为零变化 | T1 12/12 + diskann_vector 逐字节一致 |
| rg '[1]' 审计 | 仅剩 keyNull[1]（单属性 key-only tuple 的合法用法，:296）|

## 编译事故

- rd_index 无 indnkeyatts（在 CATALOG_VARLEN 段）→ 用 IndexRelationGetNumberOfKeyAttributes
- 一次 Edit 事故（helper 占位代码）当场识别重写；一次 sed 行号偏移未生效（阶段二首编
  仍是阶段一代码）→ 以 awk 复核行内容后修正

## 遗留（P1-3b）

- 磁盘格式 v2 tail（HAS_INCLUDE 等 flags）、节点状态机、immediate-delete 门控、
  ALTER 后置拦截（KNOWN_ISSUES #5）、三上下文拆分（D-007）
