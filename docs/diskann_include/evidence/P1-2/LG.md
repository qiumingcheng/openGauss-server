# LG-表 [P1-2] reloptions 三处成对注册

| 锚点符号 | 修改前 | 修改后 | 调用点 | 触发路径 | 销毁/回退 |
| --- | --- | --- | --- | --- | --- |
| reloptions.cpp boolRelOpts/intRelOpts | 无 in_filter / max_inline_payload | 两项注册 RELOPT_KIND_DISKANN，默认/上下界常量来自 diskann.h（唯一定义点） | parseRelOptions | CREATE INDEX WITH / ALTER INDEX SET | rd_options 由 relcache 生命周期管理 |
| DiskAnnOptions (diskann.h) | 无字段 | + inFilter(bool) / maxInlinePayload(int)；+4 个常量宏 | offsetof 消费 | 同上 | — |
| diskannoptions_internal parse 表 (diskann.cpp:79) | 3 项 | +2 项（与注册表成对） | fillRelOptions | 同上 | — |
| tablecmds.cpp ALTER 分支 (RELKIND_INDEX) | 仅 bm25 dict_path 拒绝 | + diskann max_inline_payload 原地修改拒绝 + REINDEX errhint（沿用既有模式） | AT_SetRelOptions on index | ALTER INDEX SET | — |

## 常量选择（自决留痕）

max_inline_payload 上下界 [32, 4096]、默认 1024：设计文档只给默认 1024；
上界取半页（真实约束由 P0-1 页容量检查兜底），下界 32 = 最小有意义 payload。
若评审需要不同值，仅改 diskann.h 四个宏（三处自动一致）。

## 判据 → 证据

| 判据 | 证据 |
| --- | --- |
| 三处成对（缺一处失败模式） | test_before_impl.log：注册前报 unrecognized parameter；注册后接受 |
| 未知参数报错 | no_such_option → unrecognized（test_after_impl.log） |
| 上下界 | 999999 → out of bounds（test_after_impl.log） |
| ALTER in_filter 可切换 | 两次 ALTER INDEX 成功 |
| ALTER max_inline_payload 拒绝+REINDEX 提示 | ERROR ... not supported + errhint |
| 回归零退化 | regression_diskann_vector.log 与基线逐字节一致 |
