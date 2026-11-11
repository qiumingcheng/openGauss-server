# P0-4 准备笔记：MetaPage 双格式读取（2026-08-14，只读）

## 1. 结构事实（diskann.h @ 6df94c2）

- `DISKANN_VERSION = 1`（:40）
- `DiskAnnMetaPageData`（:386-412）字段顺序：
  magicNumber/version/nodeSize/itemSize/edgeSize/indexSize/insertPage/extendPageLocker/
  dimensions/nfrozen/pqM/pqcodeSize/pqTableSize/pqTableNblk/pqCentroidsSize/pqCentroidsblk/
  pqOffsetSize/pqOffsetblk/enablePQ/**DiskPQParams\* params**（运行时指针，却随页写盘）/
  **frozenBlkno[FROZEN_POINT_SIZE]（最后）**
- 读入口：`DiskANNGetMetaPageInfo(Relation, DiskAnnMetaPage)`（diskann.h:612，
  实现在 diskannutils.cpp ~:160，`ReadBuffer(index, DISKANN_METAPAGE_BLKNO)` main fork + 按
  sizeof 盲拷贝——设计文档 §8.3"不校验 version"实锤）
- 写入口：`CreateMetaPage`（diskannbuild.cpp:162+）设 magicNumber/version(:174-175)
- 全部读 metapage 的调用者（rg 实测待 LG 表化）：buildstate 初始化、beginscan、
  InsertTuple 路径、GetInsertPage、PQ init 等——P0-4 改的是 reader 单点，调用者无感。

## 2. P0-4 设计（最小面，只加 reader 与断言，不改写盘格式）

本步只做"读得对"，不提升版本号（v2 写入属 P1-3/P1-4 期，届时 CreateMetaPage 追加 tail）：

1. 固化 V1 前缀长度：
   `compile-time assert: offsetof(DiskAnnMetaPageData, frozenBlkno) + sizeof(frozenBlkno)`
   == 磁盘 V1 总长（padding 风险：结构含 uint32/uint16 混排 + 指针成员，须以
   static_assert(offsetof...) 形式锁死，不用 sizeof 直接当磁盘长度）。
2. `DiskANNGetMetaPageInfo` 改造：
   - 校验 magicNumber（不匹配 → 报索引损坏）；
   - version==1：只拷 V1 长度，输出结构先 palloc0 清零；
   - version==2（未来）：读 V1 前缀 + 追加 tail（本步预留分支，读到即填默认值——
     本步还无人写 v2，分支不可达，留 TODO 注释与 P1-3 的绑定）；
   - version 未知（>2）：ereport 要求升级二进制/REINDEX；
   - **拷贝后强制 params=NULL**，调用方按现有 InitDiskPQParamsOnDisk 重建（设计文档 §9.2）。
3. 新增 `DiskAnnReadMetaV1/V2` 静态 helper（同一文件，输入 buffer，输出清零的运行时结构）。

## 3. 测试设计（先失败后通过）

基线无 v2 索引，SQL 层无法直接区分读路径正确性；P0-4 的 L2 触达点：
- v1 旧索引（现有实例的 diskann 索引）读写不变 → diskann_vector 回归 + 活实例
  既有索引查询（回归本身就是判据）；
- magicNumber 损坏/未知 version 需要物理篡改页——SQL 不可达。处置（记 DECISIONS）：
  这两个分支的验证并入 P1-4 的格式单测（届时新增 src/test/ut 或脚本层用 pg_filedump
  式工具改页）；P0-4 先以"v1 兼容回归全过 + 静态断言在 debug 编译生效"为完成判据。
- params=NULL 强制：debug 断言 beginscan 后 InitDiskPQParamsOnDisk 必然重建
  （代码路径审读 + cassert 构建下的 PQ 查询回归）。

## 4. 依赖与边界

- 不动 CreateMetaPage 写路径、不动 DISKANN_VERSION 值（写 v2 在 P1-3）。
- 不动 ReadBuffer main fork 读取（P0-5 管 fork）。
- 编译期断言放 diskann.h 或独立头，须在 .cpp 中实例化（模板/static_assert 放头文件即可）。
