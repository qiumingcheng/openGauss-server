# S0-1 基线固化报告（BASELINE.md）

- 日期：2026-08-14
- 执行者：Claude (GLM-5.3)
- 步骤：S0-1（执行文档 §4 阶段 S0）

## 1. 基线 commit 核对

```text
git rev-parse HEAD
→ 6df94c2ccb779611e55406e8fdb463068eea8b77
→ 与设计文档基线一致 ✅
```

当前分支：master（无本地分支；S0-2 将创建 `feature/diskann-include`）

## 2. 工作区 dirty 文件分类（49 untracked + 1 deleted）

### 2.1 用户文档与数据资产（保留，本任务不得触碰）

| 路径 | 说明 |
| --- | --- |
| `OPENGAUSS_BUILD_INSTALL_RUN_GUIDE.md` | 构建/安装/启停指南 |
| `opengauss_diskann_scalar_filter_topk_design.md` | Top-K 设计文档副本（用户已决定不做该项目） |
| `union_all_diskann_topk_test_cases.sql` | union all topk 测试用例（随基线 commit 交付） |
| `topk_test_data/` | **运行中数据库实例的数据目录（PGDATA），绝不允许修改/删除** |
| `D opengauss_master_union_all_vector_topk_pushdown_design.md` | 用户在工作区删除的旧版设计文档；保持删除状态，不恢复 |

### 2.2 构建产物/生成物（非修改源，不动）

按设计文档 §1.1 规则 4（parser 生成物、自动生成 header、`.modified` 临时文件不是修改源）归类：

- parser/bison/flex 生成物：`contrib/shark/**`（gram-backend、pl_gram、kwlist_d 等）、`src/common/backend/utils/adt/jsonpath_gram.cpp/hpp`、`src/common/interfaces/ecpg/preproc/*`、`src/common/pl/plpgsql/src/*_kwlist_d.h`、`src/include/parser/kwlist_d.h`
- 编译出的二进制：`src/bin/gs_loader/gs_loader`、`src/bin/gs_retrieve/*`、`src/bin/smb_mgr/*`、`src/common/interfaces/ecpg/preproc/ecpg`
- 构建过程复制/生成源：`src/lib/page_compression/**`、`src/lib/cm_common/cm_errcodes.h`、`src/include/cm/cm_errcodes.h`、`src/bin/gs_retrieve/xlogreader*.cpp`
- 构建临时文件：`src/common/interfaces/libpq/build.lock`、`ce_objfiles.txt`、`nce_objfiles.txt`、`src/gausskernel/storage/xlog_share_storage/errmsg_bak.h`

### 2.3 历史遗留临时文件（不动，已登记）

| 路径 | 处置 |
| --- | --- |
| `src/gausskernel/optimizer/prep/preptopk.cpp.modified` | 名称含 `.modified`，按设计文档规则视为临时文件，不属于任何任务；保留原状，登记 KNOWN_ISSUES 观察项 |

### 2.4 属于本任务的源码修改

**无。所有已跟踪源码文件（.cpp/.h）工作区均为干净状态。**
本任务从零开始实施，不存在与先前执行者的 overlap 风险。

## 3. 基线测试证据

### 3.1 活实例功能基线（已完成）

- 实例：`mppdb_temp_install`（debug+cassert 构建），PGDATA=`topk_test_data`，端口 55440，openGauss 7.0.0-RC3
- 命令：`gsql -X -d postgres -p 55440 -f src/test/regress/sql/diskann_vector.sql`
- 日志：`evidence/S0-1/baseline_diskann_vector_live.log`
- 结果：**通过**（唯一失败为文件末尾 `\c regression`，pg_regress 专用环境，与本实例无关）
  - 7 个 CREATE INDEX 成功；4 个 Ann Index Scan 计划生成
  - 3 个 ERROR 均为用例设计的预期报错（index_size=15/1001 越界、>1536 维拒绝）

### 3.2 权威全量基线（已完成）

- 命令：`make fastcheck PART=0`（parallel_schedule0 展开为 1027 个测试，含 diskann_vector @ 第 2 位）
- 日志：`evidence/S0-1/baseline_fastcheck_part0.log`（退出码 2；总时长 3855s ≈ 64 分钟）
- 结果：**933 ok / 74 failed**；失败清单固化于 `evidence/S0-1/baseline_failures.manifest`
- 失败性质：全部为既有环境/版本差异（collate/charset 本地化、postgres_fdw 网络、
  basebackup、pldebugger、行尾空格格式、NOTICE 消息与配置参数版本差如
  `enable_memoize` 不存在等）。**diskann_vector 通过**；向量域仅 ai_vector 失败，
  其 diff 为格式/NOTICE/配置差异，非向量功能错误。
- **基线判定标准（D-006）**：本任务所有后续回归以"**相对该 74 项清单无新增失败**"
  为通过判据；74 项清单本身不属于本任务修复范围（未触碰）。

### 3.3 构建命令约定（用户指定）

```bash
/home/qiumc/data/code/cpp/deliverables/multimodal_optimizer/opengauss_build_install_restart.sh incremental|full|restart
```

- incremental：增量 make → 停库 → make install → 启动 → SQL 连通性检查
- 配置：debug + cassert（-O0），GAUSSHOME=源码树 `mppdb_temp_install`，JOBS=4

## 4. 待用户确认项

1. §2 分类清单是否认可（特别是 `preptopk.cpp.modified` 保留原状的处置）；
2. `D opengauss_master_union_all_vector_topk_pushdown_design.md` 保持删除状态是否正确。
