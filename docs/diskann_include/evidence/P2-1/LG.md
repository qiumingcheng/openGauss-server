# LG-表 [P2-1] 谓词白名单分类

| 锚点 | 修改前 | 修改后 | 触发路径 | 生命周期 |
| --- | --- | --- | --- | --- |
| plannodes.h | 无 | +AnnFilterMode 枚举（NONE/IN/POST，无 PRE——§13.1）；AnnIndexScan +infilterqual/infilterqualorig/filterMode（尾部追加） | — | — |
| indxpath.cpp | annQuals/annQualCols 无生产者 | collect_ann_infilter_clauses + ann_clause_matches_include（白名单：col op Const/ParamExtern/IS NULL/IN/BoolTest；var 必须 ∈ INCLUDE 段；操作符 immutable）挂在 build_index_paths 的 create_index_path 后（普通+并行两处） | DiskANN+INCLUDE+ST_INDEXSCAN 的路径生成 | 随 Path |
| createplan.cpp | 无消费 | ANN 分支：filterMode=IN/POST/NONE 三态；infilterqualorig=rinfo->clause 剥离；**原谓词保留 plan.qual**（recheck，§13.3） | — | — |
| copy/out/read | 无 | 三字段成对（DISKANN_INCLUDE_VERSION_NUM 门控；read IF_EXIST 默认 NIL/NONE） | — | — |
| setrefs.cpp | 无 | infilterqual/infilterqualorig fix_scan_list（heap Var 形式；index-only 分支同） | — | — |
| explain.cpp | 无 | filterMode=IN→"Filter Mode: In-Filter"+"Index Filter"行；POST→Post-Filter 行；Filter 行保留 | EXPLAIN | — |

## 判据→证据

| 判据 | 证据 |
| --- | --- |
| 实现前失败 | test_before_impl.log（0 个 Filter Mode 行） |
| 白名单→In-Filter | test_after2.log：tenant_id=10 → In-Filter+Index Filter；price>20 → In-Filter |
| 拒绝形态→Post-Filter | OR / id+tenant_id 表达式 → Post-Filter |
| 部分分类 | price>20 AND status IN：可分类子句入 Index Filter，其余留 Filter |
| 执行不变 | Filter 行保留（recheck）；全量 func exit=0 |
| 回归零退化 | regression.log 与基线逐字节一致 |

## 过程事故（门禁抓获）

首版门控用 returnableHeapAttrs≠NULL——它在 canreturn=false 的实例（旧实例未走升级路径）
为 NULL → 分类全不触发（Post-Filter 4/4）。修正为 relam==DISKANN+ncolumns>nkeycolumns
（in-filter 不依赖 index-only）。**教训：两个特性的门控要独立推导。**

## 边界

- in_filter reloption 的运行时读取归 P2-3（executor 打开索引处；planner 无 reloption 通道——
  IndexOptInfo 无 rd_options，为它加字段=hot header 大改，不值）
- infilterqual（index-attno 形式）暂 NIL——P2-3 编译 FilterDesc 时生成
- status IN ('OK') 因隐式 cast (status)::text 不再是裸 Var 而被拒——符合白名单形状规则
  （cast 判定归 P2-2 的 FilterDesc 校验层）
