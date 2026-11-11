# LG-表 [P2-2] FilterDesc 编译与三值逻辑

| 锚点 | 修改前 | 修改后 | 触发路径 | 生命周期 |
| --- | --- | --- | --- | --- |
| diskannfilter.h（新） | 无 | DiskAnnFilterKind/Key/Desc 运行时结构（绝不入 Plan/序列化——FmgrInfo 含进程地址）；纯三值逻辑 helper（strict_op/nulltest/array_any/and/admits，LOGIC_ONLY 门控可独立编译） | L1 单测直调 | scan 生命周期 |
| execnodes.h | 无 | iss_FilterDesc/FilterFnCtx/FilterArgCtx/FilterArgStates（尾部追加） | — | 见下 |
| nodeAnnIndexscan.cpp | 无 | AnnIndexBuildFilterDesc（形状再验证：OpExpr/NullTest/SAOP；op→get_opcode→immutable 校验；heap attno→index attno 经 indextlist 映射[D-016]；fmgr_info_cxt 入 fnCtx；参数 ExprState 存 argStates）+ AnnIndexRefreshFilterArgs（rescan 时 filterArgCtx reset+重求值） | filterMode=IN 的每次查询 Init/ReScan | End: MemoryContextDelete(fnCtx)（arg ctx 为其子，一次删俩） |

## 双 context 归属表（执行文档 LG 要求）

| 资源 | 上下文 | 创建 | 重置 | 释放 |
| --- | --- | --- | --- | --- |
| FmgrInfo（比较函数） | iss_FilterFnCtx | Build | 不重置（跨 rescan 复用） | ExecEnd |
| FilterDesc 本体 | iss_FilterFnCtx | Build | 不重置 | ExecEnd（随 ctx） |
| 参数 Datum | iss_FilterArgCtx | Refresh | 每次 rescan reset | ExecEnd（随父 ctx） |
| 参数 ExprState | fnCtx 分配 | Build | 状态保留仅重求值 | ExecEnd |

## 判据→证据

| 判据 | 证据 |
| --- | --- |
| 三值逻辑全表（实现前编译失败→通过） | ut_diskann_filter_logic.cpp ALL PASS（22 断言：NULL 传播/IS NULL 双向/数组 ANY 三态/空数组 FALSE/NULL 数组 NULL/Kleene AND/仅 TRUE admit） |
| 编译执行 | filterMode=IN 查询（2 个）跑通：Init 编译+Refresh 求值+正确结果（func exit=0，11 ERROR 全预期） |
| 回归零退化 | diskann_vector 逐字节一致 |

## 过程事故（编译门禁×2）

AttrNumberIsValid→AttributeNumberIsValid（本仓库命名）；ps_ResultContext 不存在→CurrentMemoryContext；
AnnIndexRefreshFilterArgs 前向声明缺失（定义在 ReScan 之后）。

## 边界

- AM 消费（payload 读取/锁纪律/过滤执行）归 P2-3——FilterDesc 已就绪待接管
- maxExpansions=0（P2-4 定扩搜界）
- in_filter reloption 的运行时读取归 P2-3（executor 打开索引处）
- param by-ref Datum 的 datumCopy 深拷贝在 Refresh 中按 get_typlenbyval 标注（argument 存
  于 argCtx 分配——ExprEvalExpr 在 argCtx 内求值，by-ref 结果天然存活于该 ctx）
