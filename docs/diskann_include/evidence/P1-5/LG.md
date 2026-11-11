# LG-表 [P1-5] planner：returnableHeapAttrs 与 IndexPath.indexOnly

| 锚点符号 | 修改前 | 修改后 | 调用点 | 触发路径 | 销毁/回退 |
| --- | --- | --- | --- | --- | --- |
| IndexOptInfo (relation.h:962) | 仅粗粒度 canreturn | + returnableHeapAttrs（heap attno 编码；DiskANN=仅 INCLUDE 列） | plancat 填充/copy/hypopg | get_relation_info（每次规划打开索引） | planner MemoryContext |
| plancat.cpp:773 | 只填 canreturn | DiskANN+INCLUDE 时按 rd_index->indkey[i]（i≥nkeys）构建 bitmap（注意本仓库 rd_index 无 indnkeyatts，用 rel.h 宏——编译门禁又抓一次） | 同上 | 规划期 | — |
| copyfuncs _copyPartialIndexOptInfo | 无 | COPY_BITMAPSET_FIELD(returnableHeapAttrs) | plan management/选择率提取复制 | 复制路径 | bms 随节点释放 |
| hypopg_index.cpp:999 | 无 | 显式 NULL（假设索引不宣告 INCLUDE 返回能力） | hypo_inject | EXPLAIN 使用假设索引 | — |
| check_index_only (indxpath.cpp) | index_attrs=全部 indexkeys（向量 key 被当可回传） | returnableHeapAttrs 非空时改用它（就地分派代替独立 check_ann_index_only，语义等价、少改调用点） | build_index_paths | 覆盖判定 | bms_copy 用后释放 |
| IndexPath (relation.h) + pathnode.cpp:2522 | create_index_path 收 indexonly 参数但丢弃 | + indexOnly 字段并保存（ANN pathtype 仍恒 T_AnnIndexScan；executor 分支 P1-7 读） | create_index_path 全部 3 处调用（普通/并行/反向共用） | 路径生成 | — |

## 判据 → 证据（D-014：完整闭环在 P1-7）

| 判据 | 证据 |
| --- | --- |
| 非 DiskANN AM 的 Index-Only 计划零变化 | regression_old_install.log 与基线逐字节一致 |
| 旧实例（canreturn=0）零退化 | func_old_install.log exit=0 |
| 新装库覆盖矩阵行为正确 | newinstall_planner.log（4/4、tenant_id 投影、向量投影三态全对、计划仍 Ann Index Scan=join-back 正确形态、0 ERROR）|

## 过程事故

plancat 首版用了 rd_index->indnkeyatts（同 P1-3b 的坑，CATALOG_VARLEN 段不可直读）——编译门禁抓获，改 rel.h 宏。
