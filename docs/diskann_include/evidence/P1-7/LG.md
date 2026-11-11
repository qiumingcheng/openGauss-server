# LG-表 [P1-7] executor Index-Only 分支 + AM xs_itup + OPFusion 门控

| 锚点 | 修改前 | 修改后 | 触发路径 | 生命周期 |
| --- | --- | --- | --- | --- |
| setrefs T_AnnIndexScan | 仅 fix_scan_list | index_only 时 INDEX_VAR 重写（return 风格对齐 IndexOnlyScan） | covered 查询规划 | — |
| check_index_only (indxpath) | resjunk ORDER BY 的向量 key 计入 attrs_used → 永不覆盖 | DiskANN 分支从**非 junk** targetlist+quals 收集（§13.4 判定2） | 同上 | — |
| ExecInitAnnIndexScan | 恒 heap slot | index_only：ExecTypeFromTL(indextlist) 完整索引 slot + **直接 ExecAssignProjectionInfo**（绕开 tlist_matches_tupdesc 对 INDEX_VAR 的断言——gdb 单用户模式抓到的 TRAP 挂死点）+ beginscan 后 xs_want_itup=true + VMBuffer/HeapFetches 初始化 | 查询执行 | End/ReScan 释放 VM pin |
| AnnIndexOnlyNext（新） | 无 | getnext_tid + VM 检查 + IndexFetchSlot + StoreIndexTuple + PredicateLockPage（Astore-only，仿 IndexOnlyNext） | index-only 执行 | — |
| diskannbeginscan | 无 xs_itupdesc | 发布 RelationGetDescr；**分配 xs_orderbyvals/nulls**（AM 自持——修复 NULL 写导致的执行挂死）；建 returnTupleCtx | 所有 diskann 扫描 | endscan 删 |
| diskanngettuple | 无距离/无 itup | 候选带 double 距离 → xs_orderbyvals[0]；xs_want_itup 时页校验（pageId/TID/ItemIdDead）+ CopyIndexTuple 到 returnTupleCtx，失候选跳过递归 | ORDER BY 消费者 | 下次调用前有效 |
| OPFusion getSelectAnnIndexType | 无门控 | index_only_scan 计划 → NOBYPASS_NO_INDEXSCAN | fusion 判定 | — |
| optcommon/instrument 计划名 | Ann Index Scan | index_only → "Ann Index Only Scan" | EXPLAIN | — |

## 统一闭环（D-014 承诺兑现，新装库 55442）

| 用例 | 结果 | 证据 |
| --- | --- | --- |
| T-cov1 covered 查询 | **Ann Index Only Scan** + tenant_id={20,10,40}==join-back 真值（id{2,1,4} 映射） | closure_final.log |
| WHERE+covered | Ann Index Only + {10} ✓ | 同上 |
| T-cov2 投影向量 key | **Ann Index Scan（join-back）** ✓ | 同上 |
| fusion 门控 | index-only 无 [Bypass]；普通 ANN 仍 [Bypass]（不回退） | vm_fusion_test.log |
| ANALYZE 执行 | actual rows=3 ✓（Heap Fetches 行未显示——openGauss AnnIndex instrumentation 无此项，记边界） | 同上 |
| 旧实例全量 | func exit=0（B3 隔离后）；diskann_vector 逐字节一致 | func_old_install3.log + regression_final.log |

## 过程中抓获的三个真实缺陷（全部 gdb/二分定位）

1. **xs_orderbyvals NULL 写**：该数组由 AM 自持（gist 在 beginscan 分配），diskann 从未分配 → 我的距离填充写 NULL → 执行层挂死。修复=beginscan 分配。
2. **tlist_matches_tupdesc 断言**：ExecAssignScanProjectionInfo 对 INDEX_VAR varno 断言失败 → TRAP 挂死（gdb 单用户模式抓栈）。修复=index-only 直接 ExecAssignProjectionInfo。
3. **resjunk ORDER BY 覆盖判定**：向量距离表达式作为 resjunk targetlist 把向量 key 计入"必须返回的列"→ index-only 永不触发。修复=非 junk 收集（设计文档 §13.4 判定 2 原文）。

## 未解阻塞（KNOWN_ISSUES #6）

immediate_delete+DELETE（DiskAnnMarkDead）静默挂死——复现脚本与排除项见上表隔离注释；B3 用例隔离；G1 前必须解决。
