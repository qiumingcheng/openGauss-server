# LG-表 [P1-1] DDL 放开与校验（INCLUDE 列）

| 锚点符号 | 修改前 | 修改后 | 调用点 | 触发路径 | 销毁/回退 |
| --- | --- | --- | --- | --- | --- |
| indexcmds.cpp:1012 白名单 | `strcmp("ubtree")!=0 && INCLUDE → ERROR` | 删除，改为 handler 能力检查（移至 pg_am 查询后） | DefineIndex | CREATE INDEX ... INCLUDE | — |
| indexcmds.cpp amcanmulticol 判定 | `numberOfAttributes > 1`（含 INCLUDE） | `numberOfKeyAttributes > 1`（仅 key） | 同一处 | 单 key+多 INCLUDE 通过 / 双 key 拒绝 | — |
| 新增能力检查块 | 无 | INCLUDE 存在时：有 handler → `get_index_amroutine(amhandler)->amcaninclude`（用后 pfree_ext；syscache 释放前复制 amhandler）；无 handler → 兼容 UBTREE_AM_OID | DefineIndex（pg_am 查询后） | 四联判据 1/2/4 | amroutine palloc→pfree 配对 |
| 新增 CheckDiskAnnIncludeRestrictions | 无 | DiskANN V1 约束：key 恰好 1 个且为 vector(n)（VECTOROID+typmod>0）；分区/GPI/CBI/Ustore/immediate_delete 拒绝；INCLUDE 仅普通列；类型白名单（bool/int2/4/8/float4/8/date/time/timestamp/uuid + 有 typmod 的 numeric/bpchar/varchar/nvarchar2） | 能力检查块内 | P1-1 测试矩阵 8 用例 | 纯校验无资源 |
| diskann.cpp:122 handler | `amcaninclude = false` | `= true` | DDL 期 get_index_amroutine 消费 | 四联判据 1 | — |

## 编译事故记录

- 首次语法检查脚本的 sed 把编译文件名误删（空编译假通过，已识别并作废）
- 全量编译抓到两类真实错误：`tupdesc->attrs[i]` 本仓库返回结构体非指针（`&` 取址）；
  IndexAmRoutine 完整定义需 `access/amapi.h`。均修复后 BUILD_EXIT=0
  → build_incremental.log / build_incremental2.log

## 四联判据 + 拒绝矩阵 → 证据映射

| 判据 | 结果 | 证据 |
| --- | --- | --- |
| 一个 key + INCLUDE 成功 | ✅ CREATE INDEX idx_p11_ok | test_after_impl.log |
| 两个 key 拒绝 | ✅ does not support multicolumn indexes | 同上 |
| UBtree 行为不变 | ✅ handler 分支 amcaninclude 来自各 AM handler，ubtree 走无 handler 兼容路径=原白名单语义；diskann_vector 回归逐字节一致 | regression log |
| 无 handler AM 错误行为不变 | ✅ else 分支保持 UBTREE-only | 待 G1 fastcheck 增量比对 |
| 表达式/类型/key/分区拒绝 | ✅ 8/8 用例各得正确错误 | test_after_impl.log |

## 实现前失败

test_before_impl.log：idx_p11_ok 被 "only supported in ubtree" 拒绝（旧行为）。

## 备注

- INCLUDE 索引此刻只验证 DDL 接受（存储仍单列，完整 IndexTuple 属 P1-3）
- ALTER TABLE 后开 immediate_delete 的后置拦截记 KNOWN_ISSUES（归 P1-3 范围）
