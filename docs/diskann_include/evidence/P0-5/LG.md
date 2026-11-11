# LG-表 [P0-5] forkNum 传递链修复（unlogged init fork）

| 锚点符号 | 修改前 | 修改后 | 调用点 | 触发路径 | 销毁/回退 |
| --- | --- | --- | --- | --- | --- |
| DiskAnnBuildState (diskann.h:423) | 无 fork 字段 | + `ForkNumber forkNum` | BuildIndex 入口赋值 | 每次构建 | — |
| CreateRelationLockPage (diskannbuild.cpp:154) | 硬编码 MAIN | +forkNum 参数 | CreateMetaPage:189（传其 forkNum） | 每次构建（含 buildempty） | — |
| CreateMetaPage (diskannbuild.cpp:169) | **收 forkNum 不用**，硬编码 MAIN | 使用参数 | BuildIndex:838（MAIN / INIT） | 同上 | — |
| DiskAnnCreatePQPages (diskannutils.cpp:1245) ×3 处 | 硬编码 MAIN | buildstate->forkNum | DiskAnnFlushPQInfo:1297 | PQ 构建（本机不可达，D-008） | — |

## 生命周期（build → crash/replay → old-version open）

- build：normal → MAIN（行为不变，回归逐字节一致）；buildempty → INIT fork 现含
  metapage + lock page 有效空模板
- crash/replay：immediate 重启 → unlogged 索引自 init fork 重置为 16KB 有效空索引
- 实证（前后对比，同一场景 immediate 崩溃重启）：
  - 修复前：索引 0 字节，查询 ERROR "could not read block 0"（before_fix_state.log）
  - 修复后：索引 16384 字节，查询 (0 rows)，INSERT 后索引查询正常（after_fix_test.log）

## 明确不改（范围控制，与审计一致）

- InsertTuple/InsertVectorIntoPage/DiskAnnPageExtension 保持 MAIN：元组路径仅在
  有堆数据时可达（normal build=MAIN / 在线=MAIN）；buildempty 无元组。
  GraphStore ctor 的 MAIN 读：buildempty 时 MAIN metapage 已由先行 normal build
  写好（CREATE INDEX 先 ambuild 后 ambuildempty），实测无 magic 误报。

## 判据 → 证据

| 判据 | 证据 |
| --- | --- |
| unlogged 重启后格式一致可查 | after_fix_test.log（0 rows + INSERT + 索引查询 id=99）|
| 实现前失败 | before_fix_state.log（ERROR could not read block 0）|
| 普通（logged）构建零回归 | regression_diskann_vector.log 与基线逐字节一致 |
