# LG-表 [P1-4] catalog：diskanncanreturn 注册

| 锚点符号 | 修改前 | 修改后 | 调用点 | 触发路径 | 销毁/回退 |
| --- | --- | --- | --- | --- | --- |
| diskanncanreturn（新，diskann.cpp） | 无 | Relation 参数 → bool（仅 INCLUDE 索引返回 true；§4.9 语义：规范化向量 key 不可回传） | index_can_return 的 GET_REL_PROCEDURE(amcanreturn)（diskann 在 builtin_indexes_without_amhandler，走 catalog 函数而非 handler 回调） | check_index_only 前置 | 无状态 |
| pg_am.h:171 | amcanreturn 槽 `-` | `diskanncanreturn`（新装库 bootstrap） | bootstrap 解析（regprocin 按 pg_proc 已插入行解析，builtin 函数在 pg_proc 创建时经 InsertBuiltinFuncInBootstrap 插入） | initdb | rollback SQL 置 0 |
| builtin_funcs.ini | 无 | AddFuncGroup diskanncanreturn，OID 8601（全仓冲突检查：catalog 无占用；diskann 函数段 8531-8543 已满，8544+ 被 halfvec 占用，8601 段空闲） | pg_builtin_proc.cpp #include（**注意：make 不跟踪 .ini 依赖，改动须 touch pg_builtin_proc.cpp**，D-013） | initdb | — |
| 升级/回滚 SQL（新交付物） | 无 | upgrade：清后直插 pg_proc + UPDATE pg_am（本环境执行受 D-013 限制）；rollback：UPDATE 0 + DELETE | — | 正式 inplace 流程 | — |

## 两套能力来源（§8.2 双链路验证）

- DDL 能力（amcaninclude）：P1-1 handler 链路 ✅（四联用例）
- Index-Only 能力（amcanreturn）：本步 catalog 链路 ✅（新装库 amcanreturn=8601，非 INCLUDE=false / INCLUDE=true）

## 判据 → 证据

| 判据 | 证据 |
| --- | --- |
| 新装库 catalog 正确 | newinstall_verify.log（amcanreturn=8601、pg_proc 8601 存在）|
| 新装库行为正确（canreturn=true 下） | newinstall_behavior.log（4/4 无重复、COUNT 正确、0 ERROR）|
| 升级库 | 本环境不可达（D-013：catcache 断言 + inplace GUC 拒绝）；升级 SQL 为交付物 |
| 实现前失败 | test_before_impl.log（"no built-in function named diskanncanreturn"）+ initdb 首次失败日志（.ini 依赖坑）|
| 旧实例零退化 | test_final_old_install.log（exit=0）+ regression.log 与基线逐字节一致 |

## 过程事故（全部复现并归档）

1. CREATE FUNCTION INTERNAL → "cache lookup failed for function 8601"（builtin 表 OID 期望 pg_proc 预存）
2. pg_proc oid 列不可 INSERT；isinplaceupgrade 拒绝
3. INSERT..SELECT 读 pg_proc → catcache 断言崩溃；改纯 INSERT 仍断言（PROALLARGS 缓存被本脚本前置 DELETE/UPDATE 实例化后必然触发）
4. initdb 失败 "function diskanncanreturn does not exist" → make 不跟踪 .ini 依赖（touch 重编后成功）
