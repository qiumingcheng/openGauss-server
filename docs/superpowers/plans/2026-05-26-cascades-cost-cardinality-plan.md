# Cascades 优化器代价和基数传播实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在 Cascades 优化器生成最佳执行计划时，确保每个 OptExpression 节点携带正确的代价(cost)和基数(cardinality/row_count)信息

**Architecture:** 代价和基数在 `EnforceAndCostTask::execute()` 中计算后，需要在 `Memo::build_best_plan()` 构建最终计划时传播回 OptExpression 节点

**Tech Stack:** C++, openGauss optimizer cascades

---

## 背景与现状分析

### 当前数据流

```
Query Parse Tree
     |
     v
QueryToExprConverter::convert()  --> OptExpression Tree (logical)
     |
     v
CascadesOptimizer::optimize()
     |
     +-- DeriveStatsTask --> Statistics (row_count) --> 存储在 Group
     |
     +-- EnforceAndCostTask --> CostModel::compute_cost() --> 存储在 GroupExpression
     |
     v
Memo::extract_best_plan() --> OptExpression Tree (physical)  <-- 问题：此处节点缺少 cost_ 和 statistics_
```

### 关键文件现状

| 组件 | 文件 | 现状 |
|------|------|------|
| OptExpression | `memo/opt_expression.h:50-51` | 有 `cost_` 和 `statistics_` 字段，有 getter/setter |
| CostModel | `cost/cost_model.cpp` | 代价计算逻辑完善 |
| EnforceAndCostTask | `task/enforce_and_cost_task.cpp:146` | 计算代价并存储在 `GroupExpression::lowest_cost_table_` |
| DeriveStatsTask | `task/derive_stats_task.cpp:57` | 计算基数并存储在 `Group::statistics_` |
| **Memo::build_best_plan** | `memo/memo.cpp:430-466` | **问题根源**：只重建 OptExpression 结构，不传播 cost_ 和 statistics_ |

### 问题根因

`Memo::build_best_plan()` 只调用 `OptExpression::create_with_childrens()` 创建新节点，不复制代价和基数信息：

```cpp
// memo.cpp:465 - 当前代码
return OptExpression::create_with_childrens(expr->get_op(), input_plans);
// 缺失：没有设置 cost_ 和 statistics_
```

---

## File Structure

```
src/gausskernel/optimizer/cascades/
├── memo/
│   ├── memo.cpp                    # 修改 build_best_plan 传播代价和基数
│   └── opt_expression.h/cpp         # 已有 cost_/statistics_ 字段
├── task/
│   └── enforce_and_cost_task.cpp   # 参考：代价如何计算
└── cost/
    └── cost_model.cpp              # 参考：代价计算公式

docs/superpowers/plans/
└── 2026-05-26-cascades-cost-cardinality-plan.md  # 本计划
```

---

## Task 1: 修改 Memo::build_best_plan 传播代价和基数

**Files:**
- Modify: `src/gausskernel/optimizer/cascades/memo/memo.cpp:430-466`

### 步骤

- [x] **Step 1: 添加辅助函数获取 GroupExpression 的代价**

在 `memo.cpp` 第 430 行前添加：

```cpp
static double get_group_expression_cost(GroupExpression* expr, const PhysicalPropertySet& required_property) {
    if (expr == nullptr) {
        return 0.0;
    }
    return expr->get_cost(required_property);
}
```

- [x] **Step 2: 修改 build_best_plan 传播代价和基数**

修改 `memo.cpp` 第 430-466 行的 `build_best_plan` 函数：

**原代码 (第 465 行)：**
```cpp
return OptExpression::create_with_childrens(expr->get_op(), input_plans);
```

**修改为：**
```cpp
OptExpression* result = OptExpression::create_with_childrens(expr->get_op(), input_plans);

// 传播代价：从 GroupExpression 获取已计算的代价
double cost = get_group_expression_cost(expr, required_property);
result->set_cost(cost);

// 传播基数：从 Group 获取统计信息
Group* group = expr->get_group();
if (group != nullptr && group->get_statistics() != nullptr) {
    Statistics* stats = NEW_EX(Statistics, *group->get_statistics());
    result->set_statistics(stats);
}

return result;
```

- [x] **Step 3: 添加调试日志**

在 `build_best_plan` 函数中添加日志确认代价和基数传播：

```cpp
ereport(WARNING, (errmsg("[CASCADES] build_best_plan: expr=%s, cost=%.2f, row_count=%.2f",
                     expr->get_op()->to_string().c_str(), cost,
                     group && group->get_statistics() ? group->get_statistics()->row_count() : 0.0)));
```

- [x] **Step 4: Review 代码变更**

按照用户要求，修改完代码后必须 review 分析，不能直接编译测试。

---

## Task 2: 验证代价传播完整性

**Files:**
- Modify: `src/gausskernel/optimizer/cascades/memo/memo.cpp:418-428` - extract_best_plan

### 步骤

- [x] **Step 1: 在 extract_best_plan 添加验证日志**

修改 `memo.cpp` 第 418-428 行的 `extract_best_plan` 函数：

```cpp
OptExpression* Memo::extract_best_plan(const PhysicalPropertySet& required_property) {
    GroupExpression* best_expr = root_group_->get_best_expression(required_property);
    ereport(WARNING, (errmsg("[CASCADES] extract_best_plan: required_property=%s", required_property.to_string().c_str())));
    ereport(WARNING, (errmsg("[CASCADES] extract_best_plan: best_expr=%s", best_expr->to_string().c_str())));
    if (best_expr == nullptr) {
        ereport(WARNING, (errmsg("[CASCADES] extract_best_plan: no best_expr for required_property, root_group id=%d", root_group_->id())));
        return nullptr;
    }
    OptExpression* plan = build_best_plan(best_expr, required_property);

    // 验证：检查根节点代价
    if (plan != nullptr) {
        ereport(WARNING, (errmsg("[CASCADES] extract_best_plan: root cost=%.2f, row_count=%.2f",
                         plan->get_cost(),
                         plan->get_statistics() ? plan->get_statistics()->row_count() : 0.0)));
    }
    return plan;
}
```

- [x] **Step 2: Review 代码变更**

---

## Task 3: 验证集成 (待用户触发)

**Files:**
- Test: 通过 SQL 查询验证执行计划包含代价和基数信息

### 步骤

- [ ] **Step 1: 集成测试验证**

按照用户提供的集成测试流程：
1. 增量编译 gaussdb
2. 重启数据库
3. 执行 SQL 查询 `EXPLAIN` 验证计划生成成功

- [ ] **Step 2: 确认调试日志输出**

在日志中确认看到：
```
[CASCADES] build_best_plan: expr=XXX, cost=XXX.XX, row_count=XXX
```

---

## Task 4: 让 translate_xxx 函数使用 OptExpression 的代价和基数

**Files:**
- Modify: `src/gausskernel/optimizer/cascades/translator/expression_to_plan.cpp`

### 背景

当前只有 `translate_hash_agg` 使用了 `expr->get_cost()` 和 `expr->get_statistics()`，其他函数都从 Path 对象或重新计算获取代价。

### translate 函数现状

| 函数 | 代价来源 | 基数来源 | 是否使用 OptExpr |
|------|----------|----------|------------------|
| `translate_hash_agg` | `expr->get_cost()` | `expr->get_statistics()->row_count()` | ✅ 已使用 |
| `translate_scan` | `path->total_cost` | `path->rows` | ✅ 已修改 |
| `translate_sort` | `path->total_cost` | `path->rows` | ✅ 已修改 |
| `translate_hash_join` | 重新计算 | 默认为1 | ✅ 已修改 |
| `translate_merge_join` | 重新计算 | 默认为1 | ✅ 已修改 |
| `translate_nestloop_join` | 重新计算 | 默认为1 | ✅ 已修改 |
| `translate_limit` | `child_plan->startup_cost` | `child_plan->plan_rows` | ❌ 继承子节点 |
| `translate_project` | 未设置 | 未设置 | ❌ 未设置 |
| `translate_filter` | 未设置 | 未设置 | ❌ 未设置 |

### 步骤

- [x] **Step 1: 修改 translate_scan 使用 OptExpression 代价和基数**

在 `translate_scan` 函数中（约第 296-298 行），在从 Path 获取代价后，优先使用 OptExpression 的值：

```cpp
// 原代码：从 Path 获取
plan->startup_cost = path->startup_cost;
plan->total_cost = path->total_cost;
plan->plan_rows = path->rows;

// 修改为：优先使用 OptExpression 的值
double expr_cost = expr->get_cost();
if (expr_cost > 0) {
    plan->startup_cost = expr_cost;
    plan->total_cost = expr_cost;
} else {
    plan->startup_cost = path->startup_cost;
    plan->total_cost = path->total_cost;
}
Statistics* stats = expr->get_statistics();
if (stats != nullptr) {
    plan->plan_rows = stats->row_count();
} else {
    plan->plan_rows = path->rows;
}
```

- [x] **Step 2: 修改 translate_sort 使用 OptExpression 代价和基数**

类似 translate_scan 修改方式。

- [x] **Step 3: 修改 translate_hash_join 使用 OptExpression 代价和基数**

```cpp
// 在设置 plan->startup_cost, plan->total_cost, plan->plan_rows 前
double expr_cost = expr->get_cost();
Statistics* stats = expr->get_statistics();

// 优先使用 OptExpression 的值
if (expr_cost > 0) {
    sort_plan->plan.startup_cost = expr_cost;
    sort_plan->plan.total_cost = expr_cost;
}
// ... 其余代码类似
if (stats != nullptr) {
    sort_plan->plan.plan_rows = stats->row_count();
}
```

- [x] **Step 4: 修改 translate_merge_join 使用 OptExpression 代价和基数**

- [x] **Step 5: 修改 translate_nestloop_join 使用 OptExpression 代价和基数**

- [x] **Step 6: Review 代码变更**

---

## Task 5: 验证 translate 函数代价传播

**Files:**
- Test: 通过 SQL 查询验证各操作符的代价和基数正确

### 步骤

- [ ] **Step 1: 验证 scan 操作符代价和基数**

- [ ] **Step 2: 验证 sort 操作符代价和基数**

- [ ] **Step 3: 验证 join 操作符代价和基数**

- [ ] **Step 4: 确认调试日志输出**

---

## 验证检查点

1. **编译通过** - 代码修改后能通过编译
2. **计划生成成功** - SQL 查询能生成执行计划，不报错
3. **代价传播** - 根节点 `plan->get_cost()` 返回非零值
4. **基数传播** - 根节点 `plan->get_statistics()->row_count()` 返回非零值

---

## Self-Review 检查清单

- [ ] Spec coverage: 每个需求都有对应的 task 实现
- [ ] Placeholder scan: 无 TBD/TODO 占位符
- [ ] Type consistency: `get_cost()` 返回 `double`，`get_statistics()` 返回 `Statistics*`，类型一致