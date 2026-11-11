# KNOWN ISSUES（执行文档 R2 登记表）

执行中发现但不属于当前步骤范围的缺陷、遗留临时文件与异常现象，逐条登记。
处理规则：不顺手修复；等其归属步骤或用户决策。状态：`open / owner:<step> / wontfix`。

| # | 日期 | 发现步骤 | 描述 | 处置 |
| --- | --- | --- | --- | --- |
| 1 | 2026-08-14 | S0-1 | `src/gausskernel/optimizer/prep/preptopk.cpp.modified`：历史遗留临时文件（名称含 `.modified`），不属于任何已登记任务 | 保留原状；若后续步骤需触碰该目录，先请用户决策 |
| 2 | 2026-08-14 | S0-1 | `diskann_vector.sql` 末尾 `\c regression` 使其在活实例直跑必然以非零退出 | wontfix：pg_regress 环境约定，基线日志中已按预期排除 |
| 3 | 2026-08-14 | P0-2 | **VectorList::clear() 是破坏性销毁**（utils.h:171-177：pfree 数组 + p=nullptr + capacity=0），不是语义清空；clear 后 push_back 对 nullptr repalloc → SIGSEGV。P0-2 首版补丁用 clear() 导致服务器崩溃（signal 11，pg_log 22:58 + WSL crash capture 佐证），已改用 resize(0)（仅 pop 保缓冲）。残留风险：diskannutils.cpp:466-692 构建路径尚有多处 .clear()，基线可用（后续有重新赋值），但语义等雷；P0-3 上下文重构时统一处理 | owner:P0-3 |
| 4 | 2026-08-14 | P0-2 | rescan 在每次扫描启动时即被调用一次（非仅重复执行）——任何加进 rescan 的初始化都会影响首扫路径，测试必须覆盖首扫 | 已吸收进 P0-2 用例（冒烟即首扫） |
| 5 | 2026-08-15 | P1-1 | ALTER TABLE 后启用 immediate_delete 的依赖检查（§8.1.1） | **已关闭（P1-3b）**：ALTER SET immediate_delete 遇 v2 索引报错。转 Ustore/分区化的后置拦截仍未加——本环境 CREATE 时已拒（B3 之外的组合不可达），深度场景归 P2 平台适配 |
| 6 | 2026-08-15 | P1-7 | ~~immediate_delete + DELETE 静默挂死~~ **已解决（非代码缺陷）**：根因是 **make 头文件依赖失效**——P1-7 修改 diskann.h（opaque 加 returnTupleCtx 字段 + 候选加 distance）后 make 未重编 diskanndelete.o/diskannutils.o/diskannbuild.o（时间戳 09:11 vs 头文件 20:21 实证），旧布局 .o 读 `so->candidates` 偏移错 8 字节 → SIGSEGV at diskanndelete.cpp:83（gdb 单用户模式抓栈）；线程池模型下单线程崩溃杀死整实例 + WSL crash-capture 挂住 → 表现为"静默挂死"。sudo gdb（本机免密可用）是抓真实栈的关键。**处置：touch datavec/*.cpp 全量重编后 B3 通过（b3_rows=7）、全量 func exit=0、diskann_vector 逐字节一致、新装库闭环全对**。防范规则见 D-015 | resolved |
| 7 | 2026-08-16 | P2-3 | **filterCache 跨 rescan 陈旧**：LATERAL 换参数 rescan 时 diskann_set_filter 不被再调（desc 复用），filterhash 中旧参数下的 verdict 被复用——IN 列表/比较值随参数变化时可能错放行。当前 recheck 兜底使**结果仍正确**（AM 只多放/多拒候选，executor 复核纠偏），但过滤效率退化且计数不准 | **已修复（P2-4）**：diskannrescan_internal 补 filterhash_reset + 三计数清零 |
