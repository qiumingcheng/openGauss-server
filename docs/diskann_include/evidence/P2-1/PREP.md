# P2-1 准备笔记：谓词白名单分类（2026-08-15 深夜）

## 现状（实测）

- IndexPath.annQuals/annQualCols：字段已存在（relation.h），outfuncs 已输出
  （_outIndexPath :3556），**无生产者、无消费者**（除 copy 的 annQual 系列？查证）
- AnnIndexScan Plan 节点：P1-6 已加 index_only_scan/indextlist；**infilterqual/
  infilterqualorig/filterMode 未加**（设计文档 §14 要求）
- EXPLAIN：无 Index Filter / Filter Mode 显示

## P2-1 实施设计（可观察中间态：分类可见、执行不变）

1. **Plan 字段**（plannodes.h AnnIndexScan）：
   - `List* infilterqual;`（index-attno 形式，供 P2-3 编译 FilterDesc）
   - `List* infilterqualorig;`（原 Heap 表达式，EXPLAIN 用）
   - `AnnFilterMode filterMode;` 枚举（NONE/IN/POST）
   - 序列化五处成对（make/copy/out/read + DISKANN_INCLUDE_VERSION_NUM 门控；
     read 用 IF_EXIST 默认 NIL/NONE——与 P1-6 同模式）
2. **Planner 分类**（indxpath.cpp，新静态 match_clause_to_ann_include）：
   - 仅 DiskANN + INCLUDE 索引 + in_filter reloption 开
   - 白名单：`col op Const`、`Const op col`（翻转）、`col op PARAM_EXTERN`、
     `col IS [NOT] NULL`、`col IN (Const/Param...)`、BoolTest
   - 条件：列 ∈ INCLUDE 列、单基表、无 SubPlan/SRF/volatile/whole-row
   - OR/NOT/LIKE/regex/UDF → 不分类（留在 qpqual，POST 语义）
   - 结果填 IndexPath.annQuals（RestrictInfo）+ annQualCols（index attno）
3. **createplan**：ANN 分支复制 annQuals→plan.infilterqual（改写 index attno 形式
   留 P2-3——P2-1 存 orig 形式即可让 P2-3 再改写；最小面=存 orig+attno 两个形态
   直接从 annQuals/annQualCols 派生）
4. **EXPLAIN**（explain.cpp + optcommon）：filterMode=IN 时显示
   `Filter Mode: In-Filter` + `Index Filter: <orig quals>`
5. **执行不变**：原谓词保留 qpqual/plan.qual（recheck 不删——设计文档 §13.3），
   executor 不读 infilterqual（P2-3 才消费）→ 结果与基线完全一致

## 测试（先失败后通过）

实现前失败：EXPLAIN 无 Filter Mode/Index Filter 行
实现后：WHERE tenant_id=10 的 covered 查询 EXPLAIN 出现两行 + 结果不变 +
        OR/LIKE/UDF 查询无 In-Filter 行 + diskann_vector 逐字节一致

## 关键边界

- in_filter reloption off → 不分类（filterMode=NONE 全链）
- 无 INCLUDE 列的索引 → 不分类
- predicate 同时保留在 plan.qual（EXPLAIN 现有 Filter 行保留——设计文档 §6.2 明示）
