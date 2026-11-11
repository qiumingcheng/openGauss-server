# DiskANN INCLUDE 覆盖索引独立发布说明

## 发布范围与基线

本分支只发布 DiskANN INCLUDE 覆盖返回、INCLUDE payload 索引内过滤及其必要的容量、重扫、内存、格式读取、fork 和 VACUUM 修复，并携带原有文档和测试。

- 目标仓库：`qiumingcheng/openGauss-server`
- 独立分支：`codex/migrate-diskann-include-infilter`
- 本次发布父基线：`7b4cd7e77b41e8254704e6d8c02bf08be9412b67`
- 历史实现基线：`6df94c2ccb779611e55406e8fdb463068eea8b77`
- 历史实现末端：`d98de98c8016ee99ac14079597ee7d76284904dd`
- 迁移输入：历史基线到末端的 26 个提交的净补丁，合成一个新迁移提交；不重写既有远端提交。

原历史基线比本次父基线多出 UNION ALL Top-K 的四个提交。本次不带入这四个提交及其代码、GUC、测试和文档，只把覆盖索引的净补丁应用到共同父基线。两组变化只有 `createplan.cpp` 文件重叠，覆盖索引补丁的独立应用检查通过；保留覆盖返回的规划改动，不恢复 UNION ALL 的规划改动。也不带入后续距离复用、列裁剪、HybridRecall、新向标融合或原生湖存储代码。

本分支与无覆盖索引的 `codex/migrate-vector-scalar-filter` 分开，不修改后者。该特性在 2026-10-11 被移出向标融合开发基线属于历史事实；本次是独立保存和发布，不表示已经重新整合到向标融合。

## 代码、文档与测试入口

原净补丁涉及 92 个文件：33 个代码文件、17 个测试／工具文件、42 个 `docs/diskann_include/` 文档和 manifest。原有实现与历史记录保留，仅因隔离 UNION ALL 基线使 `createplan.cpp` 的整体文件内容与旧末端不同。

- 设计文档：[DiskANN 包含列与索引内过滤详细设计](../../公共文档/多模_向标融合_openGauss_DiskANN包含列与索引内过滤详细设计.md)
- 执行文档：[DiskANN INCLUDE + In-Filter 执行文档](../../公共文档/多模_向标融合_DiskANN_INCLUDE_InFilter_执行文档.md)
- 历史说明：[BASELINE.md](BASELINE.md)、[DECISIONS.md](DECISIONS.md)、[KNOWN_ISSUES.md](KNOWN_ISSUES.md)、[VERIFICATION.md](VERIFICATION.md)
- 测试入口：`src/test/diskann_include_tools/README.md`、`STATUS.md`；`src/test/regress/sql/diskann_include*.sql`。

## 验证边界

本次仅做迁移范围、补丁应用、内容和权限的静态核对；不编译、不安装、不运行 SQL、单测、故障注入或包内脚本。历史通过结果属于当时源码、构建及环境，不能直接视为本次独立基线的运行验收。

17 个测试／工具文件包括 3 个 C++ 单测、3 个 SQL、9 个专项 shell 脚本和 2 个说明文件。9 个 shell 当前仍为输出 XFAIL 并退出 2 的骨架；`STATUS.md` 的 `fi_06` 历史 `pass-partial` 记录不表示脚本已经实现。该提交树没有配套 `diskann_include` expected 输出，不能宣称已完整接入自动化 pg_regress。

仓内保留阶段报告、逻辑追踪表和失败清单；它们引用的原始运行日志未随本次发布上传。WAL 注入、PQ、正式升级、多节点构建及正式规模性能验证等边界以原报告为准，不改写为全部通过。数据目录、安装与构建产物、缓存和其他特性资料不进入本次提交。

## 发布方式

只在云端创建新提交和普通 push；每次 push 前 fetch、整合远端更新、解决全部冲突并检查结果。禁止 force、force-with-lease、带加号的 refspec、重写已发布提交或创建 PR。原 WSL 工作树、历史备份分支和用户数据保持原状。
