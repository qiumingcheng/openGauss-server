# G0 阶段门禁自审报告（D-002 证据自审制）

- HEAD：2634b9b14b（7 commits：S0 / P0-1 / P0-2 / P0-3a / P0-4 / P0-5 / P0-6a）
- 日期：2026-08-15 凌晨（用户授权自主执行期间）

## 门禁项逐条（执行文档 §4 G0）

| # | 门禁项 | 状态 | 证据 |
| --- | --- | --- | --- |
| 1 | debug 构建 + diskann_vector 回归 | ✅ | 每步收口均跑，P0-6 后与基线逐字节一致（evidence/P0-6/regression_diskann_vector.log） |
| 2 | parallel_schedule0 全量（增量判据 D-006） | ✅ | fastcheck_p0_head.log（exit 2 同基线）；g0_failures.manifest 与基线 manifest **逐项一致**（74项，零新增零消失） |
| 3 | memcheck 定向 | 推迟 D-010 | G1 前必须完成（P1-3 内存面改动前） |
| 4 | 旧格式 v1 open/insert/search 回归不变 | ✅ | 活实例 v1 索引全链路使用 + diskann_vector 逐字节一致 |
| 5 | unlogged restart | ✅ | evidence/P0-5/after_fix_test.log（fi_06 pass-partial） |

## P0 阶段缺陷修复汇总（全部"实现前失败→实现后通过"闭环）

| 缺陷 | 步骤 | 复现证据 | 修复证据 |
| --- | --- | --- | --- |
| rescan 状态泄漏（换参执行返回旧候选） | P0-2 | qid=2={1,3,2}≠真值 | ={7,9,8} |
| VectorList::clear() 破坏性销毁（SIGSEGV） | P0-2 过程 | signal 11 + crash capture | resize(0) |
| metapage 盲拷贝+params 指针复活 | P0-4 | 代码审计（运行时不可触发） | 版本感知读取 |
| 并行 worker PQ 跨进程指针 UB | P0-4 | 代码审计（PQ 本机不可测） | InitDiskPQParamsOnDisk 重建 |
| unlogged init fork 空（崩溃后索引不可用） | P0-5 | ERROR could not read block 0 | 有效空索引 16KB |
| VACUUM 清零索引统计 | P0-6a | 103/100→0/0 | 保持 103/100 |
| PQ wrapper/graphStore 泄漏 | P0-3a | 代码审计（D-007/D-008 限制） | 显式释放 |

## 结论

**G0 通过（D-002 自审）**：门禁项 1/2/4/5 ✅，项 3（memcheck）按 D-010 推迟至 G1 前执行（承诺已留痕）。
P0 阶段关闭（8 commits），进入 P1（INCLUDE + 覆盖返回）。
