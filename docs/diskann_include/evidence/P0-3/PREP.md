# P0-3 准备笔记：内存上下文拆分与对象生命周期（2026-08-14，只读）

## 1. 扫描侧（diskannscan.cpp，结合 P0-2 PREP）

- `so->tmpCtx`（beginscan:79）同时容纳：blocks(visited hash, :105)、queue(:106)、
  gettuple 期间所有临时分配（:131 切换后 GetScanValue/搜索/候选）。endscan 先 clear
  两个 VectorList 再 MemoryContextDelete(tmpCtx)（:115-123）——顺序可用但混置。
- beginscan `so->params = *tmpDiskPQParams`（:90-93）后 **wrapper 未 pfree**（泄漏，
  每次扫描一个 wrapper；内部数组归 Relation cache 不能动）。
- 目标三上下文（设计文档 §16.3）：scanCtx（queue/PQ wrapper/长期 FmgrInfo，endscan 删）、
  batchCtx（query 副本/round visited/candidates，rescan 重置）、returnTupleCtx
  （xs_itup 副本，每次 gettuple 重置——P1-3 才有 xs_itup，P0-3 先建空壳并留接口）。

## 2. 插入侧（diskann.cpp:313-340）

- `metapage.params = InitDiskPQParamsOnDisk(...)`（:322）返回 palloc wrapper，**无人释放**；
  每次在线 INSERT 泄漏一份（设计文档 §11.4 实锤）。修法：拷贝/用毕 pfree(wrapper)。
- `graphStore = New(CurrentMemoryContext) DiskAnnGraphStore(index)`（:335）**无 delete**；
  Link 抛错/正常返回均靠上下文回收。设计文档 §11.4：正常路径显式 delete，
  错误路径 PG_TRY/PG_CATCH 或专用 context。最小面：包 PG_TRY/PG_FINALLY 语义
  （openGauss 用 PG_TRY/PG_CATCH + 事务 abort 回收兜底，显式 delete 正常路径）。

## 3. P0-3 实施顺序（在 P0-2 之上，避免冲突）

1. beginscan/endscan 拆 scanCtx+batchCtx（visited/queue 归属按设计文档 §16.3 表）；
2. pfree(tmpDiskPQParams) wrapper（beginscan + insert 两处）；
3. insert 路径 graphStore 显式 delete（正常路径）+ PG_TRY 包裹；
4. returnTupleCtx 建立空壳（P0-3 不使用，P1-3 接 xs_itup）。
   注：rescan 对 batchCtx 的 MemoryContextReset 必须与 P0-2 的重置语义一致
   （P0-2 若先改字段级 reset，P0-3 换成 context reset 时合并，避免双写）。

## 4. 测试

- L3 memcheck_01_scan_cycle.sh 点亮（memcheck 构建）——memcheck 构建是全量重编
  （build.sh -m memcheck 需独立 build 目录），成本高；P0-3 完成时若 memcheck 全量
  构建不可行（内存/时间），降级：debug+cassert 下反复扫描/插入的长时间运行 +
  代码路径审读，memcheck 推迟到 G0 门禁统一跑（记 DECISIONS）。
- L2：diskann_vector 回归（PQ on/off 两类索引均覆盖）+ 多轮扫描（LIMIT 远大于首轮
  候选触发翻倍）用例已在现有回归中。
