# 向标融合 P1/P2 完整功能回归包

本包补齐历史 **1439 项核心检查**所需的验证脚本、数据生成器和 Oracle 固定输入。它与向标融合源码一起位于 `codex/migrate-vector-scalar-filter` 分支。源码版本采用外层 cpp 的 `AGENTS.md` §0.2；已有目录先检查 HEAD 和未提交修改，不覆盖其他特性工作树。

本次补包仅做静态核对，未编译或执行数据库测试。历史通过记录不等于当前新机器通过。这里运行的是 openGauss 对原始 Oracle 数据及结果的离线重放，不需要启动 Oracle，也不重新生成 Oracle 预期结果。

## 1. 内容及计数

| 组 | 检查项 |
|---|---:|
| 查询协议、行身份、PRE、IN、POST、读取边界 | 39 + 41 + 35 + 65 + 16 + 11 = 207 |
| Join 约束、路线、成本 | 25 + 148 + 41 = 214 |
| Oracle 关系语义、多匹配、Nested Loop | 75 + 108 + 158 = 341 |
| 外部参数、Hash 生命周期 | 38 + 13 = 51 |
| 并发、快照、异常与连接复用 | 626 |
| **核心合计** | **1439** |

另外运行已有的 8 项原生回归和取消后连接复用；fixture 准备的检查不计入 1439。并发组中 480 项来自 24 轮 × 5 种查询形态 × PRE/IN/POST/AUTO，其余 146 项检查计划、快照与资源异常。1439 是断言记录数，不是独立 SQL 或文件数。历史另列的 61 项成本验证不属于本表的 41 项。

- `scripts/`：15 个验证组、总入口及其依赖；`run_oracle_join_semantics.py` 仅保留原查询定义，独立 Oracle 采集不属于本包。
- `inputs/oracle/`：2 份 CSV 原始导出（50,000 条 FLOAT32/8 维向量、1,550 条业务行）和 4 份原始 Oracle `results.json` 内容。仅调整文件名，字节不变。
- `inputs/oracle-files.json`：上述输入的大小、SHA256 及原始来源。必须保留，不能用当前 openGauss 结果替换预期结果。
- `inputs/source-files.json`：40 个特性源码/原生测试输入的 SHA256；运行时核对实际文件并另记当前 Git HEAD。它不证明安装二进制一定来自这些源码。

普通合成表和索引由脚本重建。日志、数据库、旧性能记录不在本包中；运行输出放在源码树之外的新目录。脚本会创建或重建专用测试 schema，原生用例还会创建/删除 `test` 数据库，因此必须使用专门的空测试实例。

## 2. 源码、构建与依赖

首次在新机器接手时，按 cpp 的重建指南获取本特性分支，核对 §0.2 的提交。安装匹配平台的编译器和 openGauss `binarylibs`；读取本提交的构建说明，再从本工作树构建 Release/O2，例如：

```bash
# 当前目录应是 cpp 根；以下步骤仅在用户要求编译安装测试时执行。
export CPP_WORKSPACE="$(pwd -P)"
W="$CPP_WORKSPACE/openGauss-vector-filter-p12"
BINARYLIBS="$CPP_WORKSPACE/binarylibs"
cd "$W"
bash build/script/build_opengauss.sh -3rd "$BINARYLIBS" -m release -mc off
```

将本次构建产物安装到独立路径，并把下文 `I` 指向其中含 `bin/gaussdb`、`bin/gs_initdb`、`bin/gs_ctl`、`bin/gsql` 的安装目录。不能指向另一特性或旧版本安装；记录构建源 SHA、配置、构建日志与二进制 SHA256。若源码树的 `src/test/regress/pg_regress` 尚不存在，按该构建的环境单独构建该目标：`make -C "$W/src/test/regress" pg_regress`；不要借用另一源码树的比较器。

Python 3 环境需要 `numpy` 和 `psycopg2`。可在专用虚拟环境安装 `numpy psycopg2-binary`，或使用环境已有的兼容包。Python 使用系统 libpq；**不要给 Python 进程添加 openGauss 的 LD_LIBRARY_PATH**。只有原生客户端和 `pg_regress` 子进程使用本安装的库路径。额外 GCC 运行库可通过 runner 的 `--gcc-lib-dir` 显式传入。

## 3. 新建独立测试实例

下面是非 root 系统账号、Unix socket 隔离实例的示例。安装路径和端口以实际机器为准。先检查端口、服务及目录，端口需避开已有 openGauss 及其相邻内部端口；已有数据目录不要重复初始化。

```bash
set -e
I="$CPP_WORKSPACE/.install/diskann-vector-filter-p12-release"
R="$CPP_WORKSPACE/.runtime/diskann-vector-filter-p12-replay"
D="$R/data"
S="$R/socket"
PORT=55450
DB_USER="$(id -un)"
GCC_LIB="$CPP_WORKSPACE/binarylibs/buildtools/gcc10.3/gcc/lib64"
NATIVE_LIB="$I/lib"
if [ -d "$GCC_LIB" ]; then NATIVE_LIB="$NATIVE_LIB:$GCC_LIB"; fi

# 只在全新目录执行；失败即停止，切勿删除已有实例来重试。
test ! -e "$R" || { echo '目标实例目录已存在，请检查或选择新路径'; exit 1; }
mkdir -p "$S"
chmod 700 "$R" "$S"
env GAUSSHOME="$I" LD_LIBRARY_PATH="$NATIVE_LIB" \
  "$I/bin/gs_initdb" -D "$D" --nodename=vf_p12_replay -U "$DB_USER" -A trust
cat >> "$D/postgresql.conf" <<EOF
port = $PORT
listen_addresses = ''
unix_socket_directory = '$S'
EOF

# 将 socket/port/user 传入服务器环境，供自主事务连接使用。
env GAUSSHOME="$I" LD_LIBRARY_PATH="$NATIVE_LIB" PGHOST="$S" PGPORT="$PORT" PGUSER="$DB_USER" \
  "$I/bin/gs_ctl" start -D "$D" -l "$R/server.log" -w
for db in regression vf_p12_baseline; do
  env GAUSSHOME="$I" LD_LIBRARY_PATH="$NATIVE_LIB" \
    "$I/bin/gsql" -X -h "$S" -p "$PORT" -U "$DB_USER" -d postgres \
      -v ON_ERROR_STOP=1 -c "CREATE DATABASE $db;"
done
```

需要认证时通过本机 `.pgpass`/libpq 私有配置提供，不把密码写进命令、文档或仓库。示例 trust 只用于上述私有 socket、无 TCP 的独立测试实例。

## 4. 执行完整回归

```bash
P="$W/src/test/regress/diskann_filter_p12"
OUT="$CPP_WORKSPACE/.runtime/diskann-vector-filter-results/$(date +%Y%m%d-%H%M%S)"
# 如需额外 GCC 库，追加 --gcc-lib-dir "$GCC_LIB"。
env -u LD_LIBRARY_PATH python3 "$P/scripts/run_p12_release_checks.py" \
  --source "$W" --install "$I" --runtime "$R" \
  --host "$S" --port "$PORT" --user "$DB_USER" --database vf_p12_baseline \
  --output "$OUT"
```

入口先核对输入 SHA256、源码文件、数据库实际 `data_directory`、PID 对应可执行文件及 Release 模式，再串行准备 fixture，执行原生八项和十五组核心检查。默认检查本包的源码指纹；后续有意修改源码时，需审查变更并提供独立的 `--inputs` 文件，记录本轮文件哈希与实际提交，不覆盖历史记录或把未运行项写成通过。

`summary.json` 记录逐组执行结果、实际核心检查数量与失败情况；查看各组 `summary.json`、原始输出及 native-eight 的 `run/regression.diffs`。退出码为零才表示本轮入口完成。任何一项失败都保留原断言、原输入和失败记录，查明原因，不自动改 expected、删掉检查或降低比较标准。

若使用 `--start-at <组名>` 续跑，须先确认同一源码、实例和 fixture，输出到新目录；续跑只证明其实际执行的组，不能把续跑结果当作整套 1439 项通过。

完成后停止自己启动且不再需要的实例，保留数据和证据：

```bash
env GAUSSHOME="$I" LD_LIBRARY_PATH="$NATIVE_LIB" \
  "$I/bin/gs_ctl" stop -D "$D" -m fast -w
```

## 5. 验收边界

本包覆盖已列核心功能与原生前置回归，不包含完整历史任务控制状态、性能 ABBA、GDB 时序门、所有质量矩阵或第三阶段功能。输入校验及源码校验不是测试通过证据。完整通过必须以目标环境真实运行结果为准，不能把历史 1439/1439 直接抄成当前结论。
