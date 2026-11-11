# P0-1 准备笔记（fastcheck 运行期只读分析，2026-08-14）

## 1. 锚点代码事实（LG 表素材，实测行号 @ 6df94c2）

| 符号 | 位置 | 当前行为 | 问题（设计文档章节） |
| --- | --- | --- | --- |
| `InsertVectorIntoPage` | diskannbuild.cpp:262 | `bool isnull[1]`；`index_form_tuple` 只装向量 | §4.3 多列时越界 |
| 容量检查 | 同上 :276-278 | `Assert(itemsz <= BLCKSZ - ...)` | §9.3 Assert 在 release 被跳过；且只算 itup 自身，未加 `metaPage->itemSize`(node+edge) |
| `ReserveSpace` | :221 | 从 pd_upper 预留 node+edge | 断言 pd_upper-pd_lower>=size，同样 Assert-only |
| `InsertTuple` | :346 | detoast values[0]→normalize→InsertVectorIntoPage→更新 metapage | norm 失败返回 InvalidBlockNumber（调用者需检查） |
| `AddTupleToSort` | :381 | 压缩为 `Datum insertValue[1]`（丢列）；**无条件** push_back(blkno)+indtuples++ | §11.2 InvalidBlockNumber 也会入 blocksList/计数；并行 worker 在 spinlock 外未判断 |
| `BuildCallback` | :410 | 收到完整 values/isnull 但经 AddTupleToSort 丢列 | §11.2 传递链断点 |
| 在线 INSERT 调用者 | diskann.cpp:324,330（真实行 + frozen 路径） | 都只传 values | frozen 路径复用真实 values（§10.2 待改） |

调用点统计（rg 实测，待 LG 表正式化）：`InsertTuple` datavec 内 3 处（build×1 online×2）+ hnsw/ivf 各自的同名静态函数（互不相干，不受影响）。

## 2. 测试策略（判据适配，记 DECISIONS）

原执行文档 P0-1 判据"容量检查在 release 语义下可触发"依赖多属性 tuple，而多属性在 P1-3 才存在；
今日单属性向量受 1536 维上限约束无法超页。**判据拆分**：
- P0-1（现在）：容量计算抽为纯函数 `DiskAnnComputePageRequirement()`（无后端依赖），
  L1 独立编译单测覆盖边界（tuple/node/edge/special 各种组合、恰好放满、超 1 字节、0 尺寸）；
  替换 Assert 为调用纯函数的 ERROR **路径**接入完成（SQL 触发留待 P1-3）。
- P1-3：多属性真实超页 SQL 用例（预期 ERROR 消息断言），补 L2。

纯函数放 `src/include/access/datavec/diskannpagecheck.h`（新文件，仅依赖 stdint/stddef，
使单测可脱离后端独立编译）。实现随 P0-1 落入 diskannbuild.cpp 调用点。

## 3. P0-1 实施清单（fastcheck 结束后执行）

1. 新增 `src/include/access/datavec/diskannpagecheck.h`：纯函数声明 + 内联实现
   `DiskAnnComputePageRequirement(tupleSize, nodeSize, edgeSize)` 与
   `DiskAnnTupleFitsPage(required, pageSize)`。
2. 新增 L1 单测 `src/test/diskann_include_tools/ut_diskann_page_check.cpp`
   （plain assert + 独立 main，g++ 直编，不进 UT CMake 树；跑法记入脚本）。
3. `InsertVectorIntoPage`：Assert 替换为 ERROR（调用纯函数）；
   检查式扩为 itup+itemSize+ItemId+special 全口径（§9.3 公式）。
4. `ReserveSpace` 断言同步收紧（仍 Assert——内部不变量，非用户输入面）。
5. 回归：diskann_vector 活实例 + 增量编译计时（S0-2 遗留项一并完成）。
6. LG 表 + VERIFICATION 条目 + 单一 commit `[P0-1] page capacity pure helper and ERROR path`。

注意（§9.3 预留坑）：P0-1 只做"检查口径正确 + ERROR 化"；检查时机提前到
`DiskAnnPageExtension()` 之前（避免"先扩展页再报错"留下空页）属于同一处改动面，
一并完成，但 GenericXLogAbort 清理路径在 P1-3 在线多属性场景才真实可达，LG 表标注。
