# LG-表 [P1-3b] 磁盘格式 v2 tail + immediate-delete 门控 + ALTER 拦截

| 锚点符号 | 修改前 | 修改后 | 调用点 | 触发路径 | 销毁/回退 |
| --- | --- | --- | --- | --- | --- |
| diskann.h v2 定义 | DISKANN_VERSION=1 单版本 | +DISKANN_VERSION_V2/DISKANN_META_V1_DISK_LENGTH/DiskAnnMetaV2Tail(4字段)/HAS_INCLUDE flag/DiskAnnIndexUsesDeferredDelete 声明 | writer+reader+门控 | — | — |
| CreateMetaPage | version=1 恒定 | INCLUDE→version=2 + 尾部追加（metaBytes/flags/nIndexAttrs/nKeyAttrs/maxInlinePayload），pd_lower 扩展 | 每次建索引 | CREATE INDEX ... INCLUDE | — |
| DiskANNGetMetaPageInfo | v2 分支不可达（P0-4 预留） | v2：v1 镜像 + tail 拷贝 + **目录一致性校验**（nIndexAttrs/nKeyAttrs vs rd_index、metaBytes、HAS_INCLUDE 位；不符→INDEX_CORRUPTED+REINDEX 提示） | 7 处读点 | 任何 v2 索引访问 | — |
| DiskAnnIndexUsesDeferredDelete（新，diskann.cpp） | 无 | 读 metapage version≥2 | execUtils:1454、diskanndelete.cpp 防御、tablecmds ALTER 检查 | 门控三处 | metapage 栈上副本 |
| ExecOpenIndices (execUtils.cpp:1446) | immediateDelete+diskann→置 ri_hasDiskannIndex | +!DeferredDelete（v2 不进立即删除分派） | 每次 IUD 打开索引 | UPDATE/DELETE on v2 表 | — |
| DeleteDiskAnnIndexTuples (diskanndelete.cpp:323) | 无格式区分 | 运行时防御：v2 跳过（并发 DDL/升级残留兜底） | tableam 两处调用 | ri_hasDiskannIndex 误置时 | — |
| ATExecSetRelOptions (tablecmds.cpp) | 无 | ALTER TABLE SET immediate_delete 时扫描索引列表，存在 v2 diskann → ERROR（KNOWN_ISSUES #5 关闭） | RELKIND_RELATION 分支 | B1 用例 | — |

## 生命周期（磁盘）

build：INCLUDE→v2（tail 完整）→ insert/scan（reader 校验通过）→ 旧二进制打开 v2 →
P0-4 reader 的"unsupported version"报错（设计文档 §8.3 门禁成立）

## 判据 → 证据

| 判据 | 证据 |
| --- | --- |
| B1 ALTER 拦截（#5 关闭） | 实现前：静默成功（test_before_3b.log）；实现后：ERROR cannot enable immediate_delete（test_after_3b.log:257） |
| B2 v2 deferred DELETE 正确 | b2_rows=15（20−5）+ 索引查询正确 |
| B3 旧行为不变 | b3_rows=7 + immediate_delete 可用 |
| 回归零退化 | regression_3b.log 与基线逐字节一致 |
| 目录一致性校验 | SQL 不可触发（需篡改页），归 P1-4 格式单测（同 D-008 模式） |

## 范围注记

- B2 的 DELETE 在"ALTER 已被拒"的前提下走普通路径（唯一可达状态）；立即删除分派对
  v2 的防御（diskanndelete.cpp）以代码审计保证，深度 MVCC/召回验证归 G1 的 mvcc 脚本
- 三上下文拆分按 D-012 移至 P1-7（xs_itup 消费者所在）
