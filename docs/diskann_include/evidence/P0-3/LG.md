# LG-表 [P0-3a] 插入/扫描路径资源释放（范围按 D-007 拆分，上下文拆分推迟 P1-3）

| 锚点符号 | 修改前 | 修改后 | 调用点 | 触发路径 | 销毁/回退 |
| --- | --- | --- | --- | --- | --- |
| diskannbeginscan_internal (diskannscan.cpp:90) | PQ wrapper 复制后未释放 | 复制后 pfree_ext(wrapper) | 每次 AnnIndexScan 启动 | 任意扫描（PQ 索引） | wrapper 内四指针归 Relation rd_indexcxt，仅释放壳 |
| diskanninsert_internal (diskann.cpp:335) | graphStore new 后无 delete；PQ wrapper 无释放 | PG_TRY/PG_CATCH + 正常路径 delete graphStore + pfree_ext(params) | 每次在线 INSERT（含批量语句内 N 次） | 单语句多行 INSERT；错误路径 = Link 抛错 | 析构模式与 buildstate 路径一致（diskannbuild.cpp:142 同款 delete） |

## 生命周期断言
- 正常路径：Link 完成 → delete → pfree（test_functional.log 200 行批量插入 + 索引查询正确）
- 错误路径：维度错误 → PG_CATCH → 清理 → RE_THROW → 语句回滚（test_functional.log: ERROR expected 6 dimensions + 行数不变）
- 批量语句：单语句 200 次 INSERT 全走新清理路径，索引后续查询结果正确

## 判据 → 证据
- 功能零回归 → regression_diskann_vector.log 与基线 diff 逐字节一致
- 泄漏定量证明 → 本环境无内存 tracking/PQ 库，归 G0 memcheck（D-007）

## 过程记录
- 测试量校准：20000/2000 行在 debug -O0 下超时（每次图链接 ~0.2s，基线无此类批量用例），
  定格 200 行（140s 完成）；两次超时用 gs_ctl immediate 重启清场，实例崩溃恢复正常
  （顺带验证了 WAL 恢复路径）
