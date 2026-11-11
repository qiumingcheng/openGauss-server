# LG-表 [P0-1] 页容量纯函数 + ERROR 路径

## 修改符号清单（修改后行号 @ feature/diskann-include，P0-1 提交时固化）

| 锚点符号 | 修改前 | 修改后 | 调用点(rg 实测) | 触发路径 | 销毁/回退路径 |
| --- | --- | --- | --- | --- | --- |
| `InsertVectorIntoPage` (diskannbuild.cpp:263) | `bool isnull[1]`；Assert 弱容量检查（仅 itup，不含 node+edge；release 跳过） | 同函数内 Assert 替换为 `diskann_page_required_bytes` + `diskann_page_fits` 全口径检查 + `ereport(ERROR, ERRCODE_PROGRAM_LIMIT_EXCEEDED)`；检查点在 `DiskAnnPageExtension` **之前**（拒绝不留下已扩展空页） | 2 处：:387 `InsertTuple`、:675 `GenerateFrozenPoint`（两条路径都过新检查） | 任何 CREATE INDEX / INSERT 建节点（正常数据 → 检查通过不触发；超限 → ERROR，P1-3 多属性场景 SQL 可触发） | ereport ERROR → 事务回滚；此时尚未扩展页/未持有额外 buffer（检查前移的意义） |
| `diskannpagecheck.h`（新文件） | 不存在 | `diskann_page_required_bytes` / `diskann_page_fits` 纯函数（无后端依赖） | 2 处：diskannbuild.cpp + ut_diskann_page_check.cpp | L1 单测直调 | 静态 inline，无生命周期 |
| `diskannbuild.cpp` include 区 | 无 | +`#include "access/datavec/diskannpagecheck.h"` | — | — | — |

## 生命周期断言（本步骤触及面）

- 本步骤不改任何跨调用状态（无新增字段/上下文/磁盘格式），生命周期断言集为空集；
  页面写入路径仍为：form tuple → 容量检查(新) → extension → init page → reserve → add item → init node/edge。
- 全口径公式 = MAXALIGN(itup) + MAXALIGN(node) + MAXALIGN(edge) + ItemId + MAXALIGN(opaque)
  ≤ BLCKSZ − MAXALIGN(PageHeader)，与设计文档 §9.3 逐项一致。

## 判据 → 测试映射（执行文档 P0-1）

| 判据 | 证据 |
| --- | --- |
| 纯重构无行为变化（回归全过） | evidence/P0-1/regression_diskann_vector.log（待回填：与基线 diff 为空） |
| 容量检查 release 语义可触发 | L1 纯函数边界单测（前败后过）+ 内核 ERROR 路径接入；SQL 级触发留 P1-3 多属性场景（PREP §2 判据拆分，DECISIONS 留痕） |
| L1 单测先失败后通过 | l1_before_impl.log (exit=1) / l1_after_impl.log (ALL PASS) |
| 编译零新警告 | syntax_check.log（fsyntax-only 项目命令行 0 警告）+ incremental_build.log |

## rg 审计（执行文档 §5 命令，P0-1 收口）

- `Datum .+\[1\]|bool .+\[1\]`：本步骤未新增；既有 `isnull[1]`/`insertValue[1]` 属
  P1-3 改造范围（本步不动，R2）。
- MAIN_FORKNUM / IsMarkDeleted / T_AnnIndexScan：本步骤零触碰。
