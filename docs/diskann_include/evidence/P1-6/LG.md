# LG-表 [P1-6] Plan 五处成对序列化

| 处 | 修改前 | 修改后 |
| --- | --- | --- |
| make_annindexscan (createplan.cpp:6735) | 12 参数；index_only_scan/indextlist 不赋值 | +index_only_scan/+indextlist 参数并赋值（声明同步）|
| createplan ANN 分支 (:3124) | 只传 annCount | +best_path->indexOnly、+indexinfo->indextlist |
| _copyAnnIndexScan (copyfuncs.cpp:940) | 只复制 annCount | +COPY index_only_scan/indextlist |
| _outAnnIndexScanInfo (outfuncs.cpp:862) | 无这两个字段 | DISKANN_INCLUDE_VERSION_NUM(93099) 门控输出；**不受 out_plan_stat 控制**（执行语义字段，§13.4） |
| _readAnnIndexScan (readfuncs.cpp:4461) | 无 | IF_EXIST 门控读回；旧格式默认 false/NIL（join-back 语义） |
| miscadmin.h | — | DISKANN_INCLUDE_VERSION_NUM=93099（GRAND 93098 顺延） |
| plannodes.h | index_only_scan 已有但从未赋值；无 indextlist | +indextlist（完整索引列描述，setrefs/slot 用，P1-7 消费） |

## 五行检查（执行文档 §2.1 字段生命周期）

| 字段 | 谁写 | 谁读 | 谁复制 | 谁重置 | 版本门控 |
| --- | --- | --- | --- | --- | --- |
| index_only_scan | make_annindexscan | executor(P1-7)/EXPLAIN | copy/out/read | plan 生命周期 | 93099 |
| indextlist | make（来自 IndexOptInfo.indextlist） | setrefs(P1-7)/ExecInit slot | 同上 | 同上 | 93099 |

## 判据 → 证据（D-014：完整闭环在 P1-7）

- 旧实例（v1 索引不受影响）：func exit=0 + diskann_vector 逐字节一致
- 新装库：covered 查询（index_only_scan=true 已入 Plan）行为正确、计划仍 Ann Index Scan
  （executor 分支 P1-7 前为 join-back）——newinstall_verify.log 0 ERROR
- copy/out/read round-trip 单测：待 P1-7 一起（executor 分支落地后 round-trip 才有可观察差异）

## 边界

- 本环境单机；STREAMPLAN 分支的 indexname 解析路径未动（新增字段在所有 DN 可读，
  ENABLE_MULTIPLE_NODES 编译验证归 G1）
