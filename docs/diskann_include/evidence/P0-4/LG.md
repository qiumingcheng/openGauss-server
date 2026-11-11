# LG-表 [P0-4] MetaPage 版本感知读取

| 锚点符号 | 修改前 | 修改后 | 调用点(rg 实测) | 触发路径 | 销毁/回退 |
| --- | --- | --- | --- | --- | --- |
| DiskANNGetMetaPageInfo (diskannutils.cpp:154) | sizeof 盲拷贝；无 magic/version 校验；params 原样复活 | 校验 magic → 目的地清零 → version 分派（v1 拷贝钉死长度；未知版本 ereport ERR_CODE_INDEX_CORRUPTED + REINDEX 提示）→ 强制 params=NULL | 7 处（scan:90、insert:320/332、GraphStore ctor:175、GetInsertPage:217、buildstate 串行:829+worker:466） | 任何 CREATE INDEX/INSERT/SCAN/VACUUM 前 | 无新增资源 |
| 串行 build (diskannbuild.cpp:829) | 读回后直接用 params（同进程巧合指针） | 读回后 `metaPage.params = buildstate->params` 显式重挂本进程活对象 | CreateEntryPages/InsertVectorIntoPage PQ 编码 | enablePQ 构建 | params 由 FreeBuildState 原逻辑释放 |
| 并行 worker (diskannbuild.cpp:466) | 读回 leader 进程地址（跨进程 UB，基线潜在 bug） | enablePQ 时 InitDiskPQParamsOnDisk 重建本 worker params | 同上 | 并行 + PQ 构建 | FreeBuildState 新增 parallel 分支 pfree_ext(wrapper) |
| DISKANN_META_V1_DISK_LENGTH + static_assert | 无 | 钉死 v1 磁盘镜像长度；frozenBlkno 必须保持最后命名字段 | reader 自用 | 编译期 | — |

## 版本事实修正（相对 PREP）

PREP 曾按 "V1 长度 = offsetof(frozenBlkno)+4" 推导——**错误**：CreateMetaPage 以整个
sizeof（含 params 指针槽和尾部 padding，80B）推进 pd_lower，今日写盘的 v1 镜像即
sizeof。已按实际写盘行为钉死常量，P1 追加 v2 tail 必须在此长度之后。

## 验证边界（R3 如实）

- v1 兼容：diskann_vector 回归与基线逐字节一致 + diskann_include 功能全过（所有
  建/查/插路径都过新 reader）
- magic/version 错误分支：SQL 不可触发（需物理篡改页），归 P1-4 格式单测
- PQ 消费者修改：本机 x86_64 的 DiskAnnPQInit 被 #ifdef stub 为错误 → PQ 路径本环境
  运行时不可测；以编译 + 调用链审计保证，运行时验证需有 PQ 库环境（记 D-008）

## 编译事故记录

首次编译失败（StaticAssertStmt 在本 C++ 构建展开为运行期表达式，文件作用域非法）
→ 被 BUILD_EXIT=2 门禁抓获 → 改 static_assert 后通过。详见 compile_error_caught.md
