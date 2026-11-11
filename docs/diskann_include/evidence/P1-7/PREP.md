# P1-7 准备笔记：executor Index-Only 分支（锚点勘察 2026-08-15）

## 1. 锚点（实测行号 @ 9ed0171e74）

### setrefs.cpp
- :400-413 `T_AnnIndexScan` 分支：fix_scan_list 各 qual；**无 targetlist INDEX_VAR 重写**
- :398 `set_indexonlyscan_references`（IndexOnlyScan 专用，可仿照）
- 改法：splan->index_only_scan==true 时，用 indextlist 建 indexed_tlist，
  targetlist+qual 走 fix_upper_expr(INDEX_VAR)（仿 :1224 set_indexonlyscan_references）

### nodeAnnIndexscan.cpp
- ExecInitAnnIndexScan / AnnIndexNext / ExecReScan / ExecEnd（:45-46 声明）
- 改法（§17）：
  - Init：index_only 时 ss_ScanTupleSlot = ExecTypeFromTL(indextlist)；beginscan 后
    GetIndexScanDesc(...)->xs_want_itup=true（顺序：beginscan→set→rescan）
  - AnnIndexNext：index_only 分支走 scan_handler_idx_getnext_tid + VM 检查 +
    StoreIndexTuple（抽 nodeIndexonlyscan 的 IndexOnlyCheckVisibility 为
    AnnIndexOnlyCheckVisibilityAstore 或直接复用——看该函数可见性）
  - VM buffer 生命周期（iss_VMBuffer）End/ReScan/错误清理释放

### diskannscan.cpp
- beginscan：无条件 xs_itupdesc=RelationGetDescr(index)（§16.2 要求）
- gettuple：xs_want_itup 时把完整 IndexTuple 复制到 returnTupleCtx（D-012 的
  三上下文拆分在此落地：scanCtx/batchCtx/returnTupleCtx；KNOWN_ISSUES #3 的
  .clear() 雷一并处理——candidates 改 resize(0) 已做，frozenBlks 等）
- Candidate 精确距离填 xs_orderbyvals（double）；xs_recheckorderby=false

### opfusion_util.cpp
- getSelectAnnIndexType :832；改法：annIndex 为 AnnIndexScan 且 index_only_scan
  → 返回 NOBYPASS（不走 Fusion）；普通 ANN 保持

### explain.cpp
- AnnIndexScan 分支：index_only_scan → 计划名 "Ann Index Only Scan"

## 2. 统一测试闭环（D-014 承诺）

新装库（55442，canreturn 生效）：
- T-cov1：只投影 INCLUDE 列 → EXPLAIN 出现 Ann Index Only Scan；结果==真值
- T-cov2：投影向量 key → Ann Index Scan（join-back）；cosine 返回 Heap 原值
- T-cov3：VACUUM 后 Heap Fetches=0（VM 命中）；VM 清除对照（后补，可用
  pg_visibility 无则跳过记 D）
- T-cov4：无 INCLUDE 普通索引 → 行为不变；OPFusion 普通 ANN 不回退（[Bypass] 标记）
- 旧实例（55440）：全量回归逐字节一致（executor 分支仅 index_only 计划可达，
  旧实例不产生）
- 实现前失败：T-cov1 在当前二进制 EXPLAIN 是 Ann Index Scan（非 Only）→ 即 before

## 3. 风险与边界

- setrefs INDEX_VAR 重写是最高风险点：重写后 targetlist 引用 INDEX_VAR，
  StoreIndexTuple 要求 slot natts==完整索引 TupleDesc——indextlist 必须一列不漏
- ExecTypeFromTL(indextlist) 的 indextlist 是 heap-Var 形式（INDEX_VAR 重写前），
  slot 构建用它，setrefs 后 targetlist/qual 的 INDEX_VAR 对应 slot 位置
- [Bypass] 若 OPFusion 门控漏掉 index_only → 错误执行路径 → T-cov1 会崩/错，
  测试兜底
