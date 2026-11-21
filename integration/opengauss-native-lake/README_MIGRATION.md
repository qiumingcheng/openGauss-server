# 原生湖 SQL0.74 迁移资产与新环境接续

本分支为 `qiumingcheng/openGauss-server` 的 `codex/migrate-native-lake`。本轮按用户要求只迁移源码、文档和测试资产，**未在新环境编译或复测**；原机历史收据不代表当前机器已经通过测试。本文提供布局和依赖入口，不承诺一键运行。

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

异机执行前还需要完成以下接续工作，本次迁移未修改这些历史脚本：

- 检查 `ROOT`、`WORKSPACE` 及 `/home/qiumc/data/code/cpp` 硬编码，统一使用本机 `CPP_WORKSPACE`。仅导出环境变量不会自动改变未读取该变量的脚本；按上面布局复制后仍需检查硬编码。构建包装脚本、`binarylibs`、`.reference`、`.install` 和数据库目录不是随仓发布的可运行环境，按指南准备，或使用指南的直接构建入口。
- 24 份脚本位于 `suites_056_074/`：两份根脚本、`run_074_compat/` 下四份脚本及 `replay_056_074/` 下十八份脚本。`run_074_scale_soak_and_datavec.py` 的旧 scratch `REPLAY_DIR` 应改为其同目录下的 `replay_056_074`；其他旧 scratch 引用也要检查。Python 外部依赖包括 `numpy`、`psycopg2`；直接导入的本地助手随主驱动一并收录。
- scale/soak 的 `EXPECTED_SDK54`、`EXPECTED_SDK6`、`EXPECTED_RELEASE_CONF` 及旧 `build.json` 路径是历史身份约束。新机器应先形成真实构建回执，再明确更新引用和预期值；不能删除校验、伪造收据或把原机 hash 当作新构建验收。
- **完整运行 `verify_074_suite.py` 的 Part B 必须准备 DuckDB/DuckLake 参考构建**，不只是可选性能对拍依赖。参考版本按 `source-lock.json` 和迁移指南锁定；脚本当前指向 `$CPP_WORKSPACE/.reference/opengauss-native-lake/build/duckdb-gcc10/duckdb`。

迁移指南及设计文档的历史绝对路径、`file://` 链接、旧 bundle/patch 运输包是原机证据定位。它们不表示引用附件均在 Git 仓库中，也不表示相应数据库、二进制、日志或完整 `验证/` 已随仓发布。后续运行验证必须记录实际远端提交、本机源码差异、构建与测试结果。
