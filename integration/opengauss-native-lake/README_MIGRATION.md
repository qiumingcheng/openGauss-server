# 原生湖 SQL0.74 迁移资产与新环境接续

本分支为 `qiumingcheng/openGauss-server` 的 `codex/migrate-native-lake`。最初发布时只迁移源码、文档和测试资产，**当时未在新环境编译或复测**；原机历史收据不代表当前机器已经通过测试。本文提供布局、依赖和重建入口，不承诺一键运行；后续新增的显式构建参数见文末。

| 仓内资产 | 工作区映射与用途 |
|---|---|
| 仓库根 | `$CPP_WORKSPACE/.worktrees/opengauss-native-lake`，保留原驱动的产品源码定位 |
| `integration/opengauss-native-lake/` | 原主控驱动与套件 313 文件；使用前恢复到 `$CPP_WORKSPACE/integration/opengauss-native-lake/` |
| `integration/opengauss-native-lake/suites_056_074/` | 从原机 scratch 收录的 24 份 Python，保留 `run_074_compat/replay_056_074/` 层级 |
| `contrib/og_lake/sdk/` | SDK54，`og_lake_parquet_sdk_v5`；原生内部读写 Provider |
| `contrib/og_lake/sdk_external/` | SDK6，`og_lake_parquet_sdk_v6`；PGL-2 外部 Parquet Provider，独立保存 7 份源码，不替换 SDK54 |
| `公共文档/openGauss原生湖存储/` | 设计、交接、迁移指南及筛选后的历史回执；驱动需要时恢复到工作区同名主题目录 |

两个 SDK 需分别构建、安装。外部表使用 `<GAUSSHOME>/lib/postgresql/og_lake_external_sdk/libog_lake_sdk_arrow.so`；SDK54 的库不能代替 SDK6。SDK6 可传给 `sdk/build_provider.py --provider-source`，其余参数及依赖构建说明见迁移指南。

下面命令只用于新机器获取发布分支及恢复最小目录布局。先进入你选择的 `cpp` 工作区根；已有路径时停止自动创建，检查所属仓库、分支和 dirty 状态，再人工决定接续方式。这里不更新已有目录，不执行构建或测试：

```bash
export CPP_WORKSPACE="$(pwd -P)"
LAKE_SOURCE="$CPP_WORKSPACE/.worktrees/opengauss-native-lake"

if [ -e "$LAKE_SOURCE" ] || [ -L "$LAKE_SOURCE" ]; then
    printf '%s\n' '源码路径已存在，请先核对；本段不覆盖。'
    git -C "$LAKE_SOURCE" status --short --branch
    git -C "$LAKE_SOURCE" remote -v
else
    mkdir -p "$CPP_WORKSPACE/.worktrees"
    git clone --branch codex/migrate-native-lake --single-branch \
        https://github.com/qiumingcheng/openGauss-server.git "$LAKE_SOURCE"
fi
```

确认上面的源码确为所需发布分支后，分别恢复辅助目录；已有目录仅提示，不覆盖：

```bash
for LAKE_RELATIVE in integration/opengauss-native-lake 公共文档/openGauss原生湖存储; do
    LAKE_TARGET="$CPP_WORKSPACE/$LAKE_RELATIVE"
    if [ -e "$LAKE_TARGET" ] || [ -L "$LAKE_TARGET" ]; then
        printf '已有目录，请先检查并整合：%s\n' "$LAKE_TARGET"
    else
        mkdir -p "$(dirname "$LAKE_TARGET")"
        cp -a "$LAKE_SOURCE/$LAKE_RELATIVE" "$LAKE_TARGET"
    fi
done
```

辅助目录采用副本是因为部分脚本用 `Path(__file__).resolve().parents[...]` 推导工作区；软链接会解析回产品仓路径，不能保证原推导成立。后续修改辅助副本时，应与仓内对应资产明确同步，避免两份代码分叉。

异机执行前还需要核对以下接续事项；这里保留最初迁移时的缺项说明，后续参数接口见文末：

- 检查 `ROOT`、`WORKSPACE` 及 `/home/qiumc/data/code/cpp` 硬编码，统一使用本机 `CPP_WORKSPACE`。仅导出环境变量不会自动改变未读取该变量的脚本；按上面布局复制后仍需检查硬编码。构建包装脚本、`binarylibs`、`.reference`、`.install` 和数据库目录不是随仓发布的可运行环境，按指南准备，或使用指南的直接构建入口。
- 24 份脚本位于 `suites_056_074/`：两份根脚本、`run_074_compat/` 下四份脚本及 `replay_056_074/` 下十八份脚本。`run_074_scale_soak_and_datavec.py` 的旧 scratch `REPLAY_DIR` 应改为其同目录下的 `replay_056_074`；其他旧 scratch 引用也要检查。Python 外部依赖包括 `numpy`、`psycopg2`；直接导入的本地助手随主驱动一并收录。
- scale/soak 的 `EXPECTED_SDK54`、`EXPECTED_SDK6`、`EXPECTED_RELEASE_CONF` 及旧 `build.json` 路径是历史身份约束。新机器应先形成真实构建回执，再明确更新引用和预期值；不能删除校验、伪造收据或把原机 hash 当作新构建验收。
- **完整运行 `verify_074_suite.py` 的 Part B 必须准备 DuckDB/DuckLake 参考构建**，不只是可选性能对拍依赖。参考版本按 `source-lock.json` 和迁移指南锁定；脚本当前指向 `$CPP_WORKSPACE/.reference/opengauss-native-lake/build/duckdb-gcc10/duckdb`。

2026-11-21 的文档补充曾保留脚本原始内容及历史指纹：`pq_native_build_inputs.py`、`pq_sql020_build_inputs.py` 涉及历史收据的 `invariant_guards`，`reference.sh` 涉及 `source-lock.json` 的 `script_sha256`。当时路径参数化尚未完成；后续适配见下文，确需修改时应保留旧回执，审查改动后为新环境重新生成并核验依赖及构建身份，不能删除校验或把旧 SHA 当成新环境已通过的证据。两仓登记与完整操作说明见[迁移与异地复测指南](../../公共文档/openGauss原生湖存储/迁移与异地复测指南.md)。

迁移指南及设计文档的历史绝对路径、`file://` 链接、旧 bundle/patch 运输包是原机证据定位。它们不表示引用附件均在 Git 仓库中，也不表示相应数据库、二进制、日志或完整 `验证/` 已随仓发布。后续运行验证必须记录实际远端提交、本机源码差异、构建与测试结果。

## 重建环境的显式构建入口

缺少历史 `deliverables/` 包装脚本时，可向 `build_driver.py` 传入仓内
`native_build_entry.sh`。该入口实现迁移指南的 Make 配置、安装和独立实例初始化；
Debug 使用 `-O0 --enable-debug --enable-cassert`，Release 使用 `-O2`。
每个画像必须使用独立、尚未 configure 的工作树；入口不会清理或覆盖已有工作树。
实例初始化使用本次安装的 `gs_initdb`，已有非空数据目录若缺少 `PG_VERSION` 会被拒绝。

先按指南完成 CRoaring OOM 变体及私有 OpenBLAS 构建。新的 OpenBLAS 回执必须同时
提供独立记录的 SHA256；旧回执默认指纹保留，线程、PIC、符号前缀、文件清单和所有权
检查继续生效。以下变量应指向本次实际成功构建的回执，不能使用历史成功记录代替：

```bash
python3 "$CPP_WORKSPACE/integration/opengauss-native-lake/build_driver.py" \
  debug full --run-id native_debug_repro --jobs 2 \
  --build-entry "$CPP_WORKSPACE/integration/opengauss-native-lake/native_build_entry.sh" \
  --roaring-build "$ROARING_BUILD_RECEIPT" \
  --pq-in-tree --blas-build "$BLAS_BUILD_RECEIPT" \
  --blas-build-sha256 "$BLAS_BUILD_RECEIPT_SHA256"
```

Release 使用 `release full` 和另一个唯一 `--run-id`；后续 restart/incremental
仍需传入相同依赖和显式入口。构建回执记录入口、源码、依赖、已安装二进制及实际运行
进程身份。初始实例限制 `max_process_memory=3GB`，性能测试前应记录实际机器资源与配置。
SDK54/SDK6 的构建和部署、CMake 构建以及完整数据库回归仍按迁移指南分别执行；
Make 驱动的基本 SQL 成功不等于 SQL0.74 全量回归通过。

规模与长稳入口支持 `--roaring-build`、`--blas-build`、`--blas-build-sha256`、
`--build-entry`、`--source`、`--sdk54-sha256`、`--sdk6-sha256` 和
`--release-conf-sha256`。SDK 预期指纹从独立构建回执取得；配置预期指纹应在实例
准备完成时记录，测试时校验，不能在测试失败后用实际输出更新预期。
历史回归目录使用脚本旁的 `replay_056_074/`，Release 默认源码为独立的
`.worktrees/opengauss-native-lake-release`。

`run_060_suite.py` 是需单独执行的 Debug 自动维护套，不在 Release 历史批量循环中。
完整运行需使用真实 Debug 构建/重启回执、`enable_thread_pool=on`、4 个 worker、
`enable_lake_maintenance=on`，并按 `worker_060.py` 设置自动 flush 阈值和时间预算；
调用时传 `--build <回执> --output <新目录> --pool on --maintenance on`。
不要用 `--manual-only` 代替完整自动维护验证。结束后恢复原配置并停止不再需要的实例。

## SDK54 有界读取迁移修复（2026-11-22）

从发布源码 `88d9d321` 重建的内部 SDK54 在 SQL0.74 完整用例的 1536 维
`ANALYZE` 阶段触发 `file provider reallocation budget exceeded`。
`LakeFileLimits()` 的 64 MiB 预算独立于 `work_mem`；调整宿主排序内存不能解决此问题。
迁移的 `sdk/arrow_parquet_provider.cpp` 缺少设计正文已记录、且同提交
`sdk_external/arrow_parquet_provider.cpp` 中已有的逐行组读取与内存不足缩批续读实现。

修复将同一公共 reader 实现恢复到 SDK54：只对内存不足缩小未交付批次，重开不可变
行组并精确重放已交付前缀，非整除边界使用 Slice；取消、I/O、模式错误保持终止语义，
单行仍无法容纳时继续报内存不足。64 MiB 预算、瞬时旧/新缓冲计账和 C ABI v1–v5
保持原合同，SDK6 的文件描述符接口不引入 SDK54。SDK 是独立共享库，需按指南重新
构建、记录新 SHA256 并在停止实例后部署；原库和旧构建回执保留原归属。

新增独立 C ABI 回归入口（需已构建锁定 DuckDB、Arrow 头文件和候选 SDK）：

```bash
python3 "$CPP_WORKSPACE/integration/opengauss-native-lake/sdk/test_reader_budget.py" \
  --library "$NEW_SDK54_SO" \
  --previous-library "$OLD_SDK54_SO" \
  --output "$CPP_WORKSPACE/公共文档/openGauss原生湖存储/验证/sdk54_reader_budget_repro"
```

`--previous-library` 可选，仅在保留旧库、需要重现修复前错误时提供。用例固定 320 行、
65 行初始批次与 8 MiB 预算，前 65 行为短文本、后续为 64 KiB 文本；验证已交付
65 行后缩批的非整除续读、全部业务字节与物理位置、未释放批次拒绝、取消及终止性 OOM。
原 SDK 读取合同测试、PQ Parquet 探针和 SQL0.74 完整入口仍需运行，不能用该专项
代替完整复测。原始主套件及兼容性断言无需修改。

## Debug 高维索引准备超时

本机 Debug/O0＋断言构建的 768 维索引准备超过主套默认的 300000 毫秒，原始运行
按规定返回 SQLSTATE 57014 并保留失败记录。主套新增 `--index-fixture-timeout-ms`，
默认仍为 300000，仅包围三处 `build_lake_index` 准备操作，结束或异常时立即恢复原值。
查询及 ANALYZE 超时、固定数据规模、召回率和一致性断言均不因此变化。
性能比较仍使用 Release；慢速 Debug 环境可显式运行：

```bash
python3 "$CPP_WORKSPACE/integration/opengauss-native-lake/suites_056_074/verify_074_suite.py" \
  --profile debug --mode full --dims 32,128,384,768,1536 \
  --index-fixture-timeout-ms 1800000
```

主套发生异常时会保留其独立数据库、角色和临时文件，并打印 `failed-run.json`
位置；成功运行仍清理自身夹具。不要把失败记录改成通过，也不要清理尚需诊断的现场。


## 本机完整复测范围（2026-11-22）

Make Debug/Release和CMake Release构建安装通过；SQL074 Release/Debug完整入口通过，
三画像兼容性361项通过（Release122、Debug117、ann-debug122），SQL060完整Debug自动维护10阶段通过，
Release历史15次调用、datavec3项、百万行规模、20k/50k向量和80表线程池压力入口通过。
ann-debug复用已验证Debug ELF及独立数据库；全部实例已停止，原配置已恢复。
本轮14个计划分组均有实际通过回执；PQ组件83＋270项、新SDK54预算专项1971项另列，不能混为一个用例总数。

这不是所有性能场景均获益：8000行PQ比exact慢，50k行PQ平均/最低Recall为0.96/0.8；
完整五档维度主套Recall为1.0属于不同固定输入。详细数值、6项迁移修复和未验证边界见
[迁移指南本机复测记录](../../公共文档/openGauss原生湖存储/迁移与异地复测指南.md#2026-11-22-openeuler-wsl2-异地重建复测结果)。
72小时生产长稳、真实S3/OGRAC等原后置边界不因本轮通过而关闭。

## P1 补充测试资产（2026-11-22，静态迁移）

本次新增 16 个文件：原盘点的 14 份脚本/探针/原型，加 SDK6 构建助手和 README。
测试目录共 357 文件。为调用链配套修改 16 份 replay 和 3 份兼容脚本的路径配置；
首次发布时没有编译或运行新增用例。后续本机 P1 增量复测见下文。
全部映射见[待提交与补充测试用例清单](../../公共文档/openGauss原生湖存储/原生湖SQL074待提交与补充测试用例清单.md)。

| 入口 | 用途 |
|---|---|
| `suites_056_074/run_074_compat/run_074_smp_closure_suite.py` | N7-SMP 闭环、故障注入、PREPARE/ReScan、扩展类型与宽列 |
| `suites_056_074/run_074_release_perf_bench.py` | Release 主性能、240k 行 SMP 与并发 flush 读延迟 |
| `suites_056_074/run_074_debug_history_campaign.py` | SQL056～070 Debug/线程池/ann-debug 历史回归与 datavec |
| `suites_056_074/run_074_compat_campaign.py` | 三画像 SQL071～073 兼容性＋SMP 闭环编排 |
| `suites_056_074/run_074_release_campaign.py` | Release 兼容性、SMP 和主性能编排 |
| `sdk_external_tests/run_probe.sh` | SDK6 ordinal、fd、openat2 锚点和宿主路径准入四探针 |
| `sdk_external_tests/build_sql_prototype.py`、`run_sql_prototype.py` | 私有 SQL 测试模块构建与隔离实例探针；不属于生产接口 |

沿用文首的真实 `cpp/integration/opengauss-native-lake` 目录副本，不能软链；
设置 `CPP_WORKSPACE`，数据库用户通过 `PGUSER` 或 `--user` 指定。
SMP 和性能入口可单独指定 host/port；总编排会管理已有固定画像端口与配置，
须在专用测试工作区运行。Debug 历史编排还保留结束后无 gaussdb 的原断言。
尚未修改的其他旧 helper 继续遵循上文的路径/冻结指纹限制，不能把此次小范围适配称为全部测试可移植。

基础入口示例（须先准备匹配安装、实例及依赖；以下命令本轮未执行）：

```bash
export S="$CPP_WORKSPACE/integration/opengauss-native-lake/suites_056_074"
python3 "$S/run_074_compat/run_074_smp_closure_suite.py" --profile debug --out "$OUT/smp.json"
python3 "$S/run_074_release_perf_bench.py" --profile release --out-dir "$OUT/performance"

python3 "$S/run_074_debug_history_campaign.py" \
  --workspace "$CPP_WORKSPACE" --source "$CODE_BASE" \
  --build-receipt "$DEBUG_BUILD_RECEIPT" \
  --build-entry "$CPP_WORKSPACE/integration/opengauss-native-lake/native_build_entry.sh" \
  --roaring-build "$ROARING_BUILD_RECEIPT" \
  --blas-build "$BLAS_BUILD_RECEIPT" --blas-build-sha256 "$BLAS_BUILD_RECEIPT_SHA256" \
  --expected-sdk54 "$SDK54_SHA256" --expected-sdk6 "$SDK6_SHA256" \
  --expected-debug-conf "$DEBUG_CONF_SHA256" --expected-ann-debug-conf "$ANN_DEBUG_CONF_SHA256"

python3 "$S/run_074_compat_campaign.py" \
  --workspace "$CPP_WORKSPACE" --build-receipt "$DEBUG_BUILD_RECEIPT" \
  --immutable-manifest "$IMMUTABLE_MANIFEST" --campaign-id native_074_compat_repro_01
python3 "$S/run_074_release_campaign.py" \
  --workspace "$CPP_WORKSPACE" --build-receipt "$RELEASE_BUILD_RECEIPT" \
  --expected-sdk54-sha256 "$SDK54_SHA256" --expected-sdk6-sha256 "$SDK6_SHA256" \
  --out-dir "$OUT/release_campaign"
```

`IMMUTABLE_MANIFEST` 是四键 JSON：`SDK54_SO`、`SDK6_SO`、`DEBUG_CONF`、
`ANN_DEBUG_CONF`，值为从真实构建/原始配置独立记录的 SHA256 字符串。
不得在失败后用实际值替换预期值。Release 编排沿用 SDK 部署步骤：若 Release
安装缺失或指纹不同，从 Debug 安装复制 SDK 目录，复制前后均核验指定 SHA；
执行完停止 Release。兼容编排结束停止 debug/ann-debug 并恢复原始配置。
Debug 历史编排使用仓内 PQ 及显式 BLAS/CRoaring 收据，保留原 SDK/配置校验。

SDK6 夹具与私有模块命令见 [sdk_external_tests/README.md](sdk_external_tests/README.md)。
探针需要锁定 DuckDB、已构建 SDK6 与实际输入 SHA；私有模块还需匹配 Debug
源码/安装的实际编译参数，可用 CMake 编译数据库或核对后的 Make 编译条目。脚本创建新的输出与独立实例，
保留失败回执；原机 Parquet、二进制、旧模块和历史日志没有作为发布文件上传。

本机 P1 增量复测已完成：Release 147 项与四阶段主性能，三画像兼容 399 项，
历史回归三画像各 16 次套件调用、独立 SQL060 自动维护 10 阶段及 datavec 3/3，
SDK6 独立探针 4/4、私有 SQL 17/17。目录修正、测试准备问题和两次历史非零退出
见[迁移指南的 P1 增量复测记录](../../公共文档/openGauss原生湖存储/迁移与异地复测指南.md#2026-11-22-p1-增量复测)。

SDK6 隔离 SQL 可以由 openGauss FastCheck 调度。在已配置源码树执行
`make -C src/test/regress fastcheck PART=.og_lake_sdk6`。此分组沿用原有
17 项断言并保存逐项结果。构建收据、SDK6 库及环境参数见
[SDK6 FastCheck 配置](sdk_external_tests/README.md#fastcheck-sdk6-sql-lane)。
