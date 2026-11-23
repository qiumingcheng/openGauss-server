# 路线 C：外部文件接口与验证边界

本说明是 `PGL-2`／`N7-COPY` 的实施准备，不表示接口已合入或通过验收。现有写路径和业务快照契约沿用[主设计 §6.11](架构与实施设计.md#sec-6-11)、[§6.16](架构与实施设计.md#sec-6-16)。路线 B 最新运行状态以[继续开发交接](继续开发交接.md)为准；其构建冻结期不改产品输入。

## 实现可复用什么

| 路径 | 现有依据 | 不等同于已完成的工作 |
|---|---|---|
| 本地 CSV 只读 | 宿主 `contrib/file_fdw/file_fdw.cpp` 以 `BeginCopyFrom` 驱动读入，文件名仅管理员配置；原生湖 `COPY FROM STDIN` 已复用宿主 text/CSV 解析。 | 外部 CSV 配置还须单独验证授权、根路径、表级选项、坏数据和非本机来源；不能让任意 SQL 用户指定服务器文件。 |
| 本地 Parquet 只读 | SDK54 的 `footer_open`／`footer_info` 提供名称、类型、空性和字段 ID；`reader_open` 支持显式 ordinal 投影和分批。 | 外部文件通常没有 DuckLake `field_id`，不能送进受管理表的文件绑定器或自动补写 DuckLake 元数据。宿主必须先解析、冻结、校验外部名称与类型映射。 |
| COPY 到湖表 | `copy.cpp` 的 `LakeCheckCopyFromOptions` 目前对本项目只允许单机、无文件名、无自定义输入回调的 text/CSV；类型与发布均走已有事务写集。 | 服务器路径／URL、Parquet 格式、写出与模式推断并未开放；不能通过绕开入口检查获得功能。 |

对照固定 `pg_lake@873f51037f40af5cf8b3b3330e919a835bfd8c71` 的 `pg_lake_table/src/fdw/`、`pg_lake_copy/src/copy/` 及其 Parquet/COPY 测试。对照实现不作为 openGauss 二进制依赖；Oracle 运行单独固定 DuckDB/DuckLake 身份。

## 分批推进与约束

1. **独立只读入口与显式列定义**：先限定管理员配置的本地数据根；读取权限由宿主关系 ACL 控制，拒绝 `INSERT/UPDATE/DELETE/COPY FROM` 及外部 URL、不支持的压缩包装或文件类型。用户给出的逻辑文件名只能在授予的根内解析；绝对路径逃逸、`..`、符号链接、非普通文件、检查与打开间替换须明确拒绝。若 SDK 当前无法把路径检查与实际打开绑定为同一文件身份，先完善可信打开契约，不将 `realpath()` 单独视为充分防护。
2. **Parquet 名称映射与受限推断**：先以 footer 的精确字段名/类型和有界 ordinal 队列读取；拒绝重名、目标字段缺失和未证明的类型转换。推断函数返回可审阅的列信息，不写宿主表或湖目录。精度、时区、NULL、多文件联合列映射及 CSV 推断逐项定义；不以首行或采样类型替代实际入库校验。外部只读路径不参与 DuckLake row ID、删除可见性、索引和快照发布。
3. **本地文件 COPY 与事务**：文本复用宿主 COPY 格式、编码、列权限、默认值和错误行语义；Parquet 使用相同的文件映射和现有 ForeignInsert 路径。直写、文件分区、晚到错误、取消、ROLLBACK/保存点必须证明单一事务原子性；不在批次内部 COMMIT。导出只写独占候选文件，分别约定成功发布、失败与取消清理，不覆盖用户已有文件。
4. **URL 与远端 SDK**：在本地端验收之后另开适配，配置受信目的地、认证与安全边界，确保重定向、隐私、读取上限和故障回收。不得将本地 SDK54 合格结论外推为远端生产合格。

本地 SDK54 的 `RangeFile` 当前只对传入路径的最后一段使用 `O_NOFOLLOW`，已打开的 fd 有 `fstat` 身份检查，但它不能证明父目录始终处于被授权根。可复用现有本地清理适配的目录 fd 逐段定位方式，让 SDK 消费同一受控的只读 fd；该读接口应独立，不调用物理删除接口，不改变已合格的 managed-Parquet ABI 或默认路径。没有此合同不得让普通外表配置直接触达现有路径型读取函数。

## 第一批验收入口

- 从固定 DuckDB 生成**没有 field ID** 的本地 Parquet，包含多行组、NULL、空字符串和 UTF‑8；以打乱顺序的目标列读取，对拍数据及原始行序。还需覆盖空投影保留行数、字段重名/缺失、物理类型不符和文件身份不一致。
- CSV 用带换行和双引号的字段、空串与 NULL、列顺序及编码错误，逐行与宿主 COPY 对照。负例同时验证只读、权限及路径约束，失败不改变 DuckLake 目录。
- COPY 以事务结束为验收点：同一会话完成导入、重读、保存点回退及跨分区发布；独立会话核对提交前后可见性、完整行与文件快照。现有基础 STDIN、队列内写／溢写及分区组合证据先对照输入与版本，不能把旧结果签收为新文件入口。

**独立 SDK 实测（2026-11-18）**：[C11 只读探针](验证/pgl2_sdk_ordinal_probe/reader_probe.c)使用固定 DuckDB 生成 5,000 行、3 个行组、两个均无 `field_id` 的字段。现有 SDK54 以 `[文本列, 整数列]` 的 ordinal 顺序逐值读取，NULL、空串、UTF‑8、原始行序和 footer／reader 对象身份均正确；空投影保留全部 5,000 行；用 field-ID 模式代替 ordinal、重复或越界 ordinal 均被拒绝。[结果](验证/pgl2_sdk_ordinal_probe/run_01/result.json)及[输入指纹](验证/pgl2_sdk_ordinal_probe/run_01/sha256sums.txt)已保留。这证明现有 SDK 的一项能力，不表示 SQL 外表、路径约束、类型推断、COPY 或 URL 已实现。

**独立路径实测（同日）**：[Linux 根目录 fd 探针](验证/pgl2_sdk_ordinal_probe/anchor_probe.c)在本机通过了正常读取、11 类非法输入拒绝（含父级符号链接、FIFO 等），并验证路径被替换后不会改变已打开 fd 所指对象。[结果](验证/pgl2_sdk_ordinal_probe/anchor_run_01/result.json)不覆盖宿主 ACL、root 授权/初始化、非 Linux 平台、SDK fd 接入或跨进程并发，不能代替最终路径安全验收。

两项均可用[复跑脚本](验证/pgl2_sdk_ordinal_probe/run_probe.sh)加一个未使用的运行号复现；脚本校验固定 SDK 与参考 ELF、生成自己的夹具，不启动数据库。已复跑并核验[完整指纹](验证/pgl2_sdk_ordinal_probe/reproducible_run_02/sha256sums.txt)，两份结果与首轮逐字节一致。

**SDK6 只读 fd 候选（2026-11-19）**：[独立构建与验收汇总](验证/pgl2_sdk6_standalone_build_1119_01/index.json)。候选从已验收 SDK54 的真实源副本派生，不从略有差异的产品仓版本重建替换现用 DSO。新增可选 ABI6，原 v1–v5 头/导出保留。SDK 复制宿主授权的只读普通文件 fd，只用定位读，不接收路径；从 footer 到 reader 检查对象身份。C11 实测 5,000 行/3 行组、原路径替换后仍读取原授权对象、8 类无效 fd/选项拒绝、关闭/错误无描述符泄漏、调用者偏移未变。ABI1 按路径读取的同一测试结果逐字节一致。新 DSO 仅在隔离前缀，未替换 SQL0.60 已验 SDK54。下一步是宿主管理员路径准入、绑定校验及 SQL 行式只读接入；未有 SQL 外表、CSV、COPY 或远端验收。