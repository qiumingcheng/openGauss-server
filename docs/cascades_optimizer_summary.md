# Cascades 优化器开发总结

**日期**: 2026-04-24  
**分支**: cascades

---

## 一、核心问题与修复

### 1.1 单谓词与多谓词兼容 API

**问题**: 重构谓词下推支持多谓词后，移除了单谓词 API，导致现有规则编译失败。

**设计原则**: 多谓词 API 天然支持单谓词场景（单谓词是 predicate_ids 只有一个元素的特例）。

**修复方案**: 添加兼容方法作为便捷包装器

```cpp
// logical_filter.h 和 logical_scan.h

// 主 API（多谓词）
List* get_predicate_ids() const;
void set_predicate_ids(List* ids);
void* get_qual_list() const;
void set_qual_list(void* list);

// 兼容 API（单谓词便捷方法）
int get_predicate_id() const {
    if (predicate_ids_ == nullptr || list_length(predicate_ids_) == 0) {
        return -1;  // 无谓词
    }
    return lfirst_int(list_head(predicate_ids_));  // 返回第一个谓词 ID
}
void set_predicate_id(int id) {
    predicate_ids_ = list_make1_int(id);  // 创建单元素列表
}
```

**影响文件**:
- `operator/operators/logical_filter.h`
- `operator/operators/logical_scan.h`

---

### 1.2 USE_SPQ 编译选项缺失

**问题根因**:
- OpenGauss 的 `Makefile.global` 定义了 `-DUSE_SPQ`
- Cascades CMake 构建未继承此宏，导致 `nodes.h` 中节点类型枚举值对不上
- 执行时报错 `unrecognized node type: 101` (T_BaseResult)

**修复方案**:

1. **CMakeLists.txt 添加 USE_SPQ 编译定义**
```cmake
target_compile_definitions(cascadesoptimizer PRIVATE PGXC USE_SPQ)
target_compile_definitions(cascades_planner PRIVATE PGXC USE_SPQ)
```

2. **expression_to_plan.cpp 使用条件编译**
```cpp
#ifdef USE_SPQ
    Result* result_plan = makeNode(Result);
    result_plan->numHashFilterCols = 0;
    result_plan->hashFilterColIdx = nullptr;
    result_plan->hashFilterFuncs = nullptr;
#else
    BaseResult* result_plan = makeNode(BaseResult);
#endif
```

**影响文件**:
- `optimizer/cascades/CMakeLists.txt`
- `translator/expression_to_plan.cpp`

---

### 1.3 FilterImplRule 未正确创建 PhysicalFilter

**问题**: FilterImplRule 返回原输入 `LogicalFilter`，没有创建 `PhysicalFilter`，导致转换后的表达式树仍包含 Logical 节点。

**修复方案**:

```cpp
// filter_impl_rule.cpp
std::vector<OptExpression*> FilterImplRule::transform(...) {
    // 创建 PhysicalFilter 算子
    PhysicalFilter* physical_filter = NEW_EX(PhysicalFilter);
    
    // 转移谓词信息
    if (qual_list != nullptr) {
        physical_filter->set_qual_list(qual_list);
    }
    if (predicate_ids != nullptr && list_length(predicate_ids) > 0) {
        physical_filter->set_predicate_ids(predicate_ids);
    } else if (filter->has_predicate()) {
        physical_filter->set_predicate_id(filter->get_predicate_id());
    }
    
    return {OptExpression::create(physical_filter, child)};
}
```

**影响文件**:
- `rule/filter_impl_rule.cpp`
- `operator/operators/physical_filter.h`
- `translator/expression_to_plan.cpp`

---

### 1.4 Subquery 实现完善

**参考 commit**: f7aa6bab4

**添加的文件**:
- `operator/operators/logical_subquery_scan.h/.cpp`
- `operator/operators/physical_subquery_scan.h/.cpp`
- `rule/subquery_scan_impl_rule.h/.cpp`

**SubqueryScanImplRule 关键逻辑**:
1. 递归调用 `subquery_planner()` 生成子查询计划
2. 使用 `convert_subquery_pathkeys()` 转换排序键
3. 调用 `create_subqueryscan_path()` 创建路径
4. 创建 `PhysicalSubqueryScan` 包装子查询计划

---

## 二、编译配置

### 2.1 CMake 增量编译方法

**问题**: 修改 CMakeLists.txt 后，tmp_build 无法正确编译

**根因**: CMake 缓存中缺少 `THIRD_BIN_PATH` 环境变量，导致找不到 OpenSSL 等第三方库头文件

**快速增量编译流程**:

```bash
cd tmp_build

# 方法 1: 保持缓存，只清除 cascades 相关对象文件（推荐）
rm -rf CMakeFiles/cascadesoptimizer
rm -rf src/gausskernel/optimizer/cascades/CMakeFiles/cascadesoptimizer.dir
make cascadesoptimizer
make gaussdb

# 方法 2: 完全重新配置（当方法 1 失效时）
rm -rf *
export THIRD_BIN_PATH=/home/hcy/projects/binarylibs
cmake .. -DENABLE_OPENSSL3=ON -DUSE_SSL=ON
make cascadesoptimizer
make gaussdb
```

**完整编译** (使用 build.sh):
```bash
sh build.sh -m debug -3rd $BINARYLIBS --cmake
```

### 2.2 CMakeLists.txt 添加的源文件

| 类别 | 文件 |
|------|------|
| **Task** | rewrite_tree_task.cpp |
| **Operator** | logical_sort.cpp, logical_limit.cpp, physical_filter.cpp, physical_limit.cpp, logical_subquery_scan.cpp, physical_subquery_scan.cpp |
| **Rule** | filter_impl_rule.cpp, limit_impl_rule.cpp, subquery_scan_impl_rule.cpp |

---

## 三、LogicalFilter → PhysicalFilter 转换场景

**何时需要 LogicalFilter 转换为 PhysicalFilter？**

当 Filter 无法通过谓词下推优化合并到 Scan 时，需要保留为独立的 PhysicalFilter 节点。

### 场景 1: Filter 在 Join 之后
```sql
SELECT * FROM t1 JOIN t2 ON t1.id = t2.id WHERE t1.age > 25;
```
执行计划:
```
PhysicalFilter (t1.age > 25)
  PhysicalHashJoin
    PhysicalScan(t1)
    PhysicalScan(t2)
```

### 场景 2: 多表复杂条件
```sql
SELECT * FROM t1, t2 WHERE t1.id = t2.id AND t1.age + t2.score > 100;
```
执行计划:
```
PhysicalFilter (t1.age + t2.score > 100)  -- 涉及两个表的列，无法下推
  PhysicalNestLoopJoin
    PhysicalScan(t1)
    PhysicalScan(t2)
```

### 场景 3: 聚合后过滤 (HAVING)
```sql
SELECT dept, COUNT(*) FROM emp GROUP BY dept HAVING COUNT(*) > 5;
```
执行计划:
```
PhysicalFilter (COUNT(*) > 5)  -- 作用于聚合结果
  PhysicalHashAgg
    PhysicalScan(emp)
```

### 场景 4: 表达式计算后的过滤
```sql
SELECT * FROM t1 WHERE id + 1 > 10;
```
执行计划:
```
PhysicalFilter (id + 1 > 10)  -- 复杂表达式，可能无法下推
  PhysicalScan(t1)
```

---

## 四、提交记录

| Commit | 描述 |
|--------|------|
| 8807d4510 | 修复 USE_SPQ 编译选项：继承 OpenGauss 全局编译宏 |
| ede21bb26 | 修复 FilterImplRule: 正确创建 PhysicalFilter 算子 |
| e94ff8382 | 修复 CMakeLists.txt: 添加 subquery scan 源文件 |
| 711f0e46c | 支持单谓词和多谓词兼容 API，完善 Subquery 实现 |

---

## 五、待完成工作

1. **PhysicalFilter 执行器支持**: 确保 executor 能正确执行 PhysicalFilter 节点
2. **谓词转移完整实现**: FilterImplRule 中 qual_list 的完整转移逻辑
3. **更多物理算子实现**: Limit、Sort 等算子的 impl rule
4. **测试用例补充**: 针对各转换规则的单元测试

---

## 六、关键代码位置

| 功能 | 文件路径 |
|------|----------|
| 谓词下推规则 | `rule/predicate_push_down_rule.cpp` |
| 列裁剪规则 | `rule/column_prune_rule.cpp` |
| 常量折叠规则 | `rule/constant_fold_rule.cpp` |
| Filter 实现规则 | `rule/filter_impl_rule.cpp` |
| Scan 实现规则 | `rule/scan_impl_rule.cpp` |
| 表达式转计划 | `translator/expression_to_plan.cpp` |
| 查询转表达式 | `translator/query_to_expression.cpp` |
| 优化器入口 | `optimizer/cascades_optimizer.cpp` |
