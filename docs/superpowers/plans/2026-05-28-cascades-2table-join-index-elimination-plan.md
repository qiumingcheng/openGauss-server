# 2表JOIN索引消除ORDER BY实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 支持当 LogicalSort 的子节点是 LogicalJoin 时，通过 PassThrough 将 ORDER BY 要求传递给 Join 的外表，让外表使用索引扫描消除排序。

**Architecture:** 在 SortImplRule 中增加对 LogicalJoin 子节点的检测和处理。当发现子节点是 Join 时，递归到外表查找可用索引，如果找到则创建 PassThrough挂在 Join 上方。

**Tech Stack:** C++, Cascades optimizer framework

---

## 文件修改总览

| 文件 | 作用 |
|------|------|
| `src/gausskernel/optimizer/cascades/rule/sort_impl_rule.cpp` | 核心逻辑修改 |
| `src/gausskernel/optimizer/cascades/rule/sort_impl_rule.h` | 新增方法声明 |
| `src/gausskernel/optimizer/cascades/operator/operators/logical_join.h` | 已存在，无需修改（仅参考） |

---

## Task 1: 在 SortImplRule 新增 `find_index_for_sort_on_join_outer` 方法

**Files:**
- Modify: `src/gausskernel/optimizer/cascades/rule/sort_impl_rule.cpp:250`（在文件末尾 `}` 前插入）
- Modify: `src/gausskernel/optimizer/cascades/rule/sort_impl_rule.h`（新增方法声明）

**Step 1:** 在 `sort_impl_rule.h` 的 private 部分新增方法声明

在 `sort_impl_rule.h` 第28行后添加：
```cpp
bool find_index_for_sort_on_join_outer(OptExpression* join_expr, void* sort_clause,
                                       OptimizerContext* context, IndexOptInfo** out_index,
                                       List** out_pathkeys) const;
```

**Step 2:** 在 `sort_impl_rule.cpp` 第250行后（`}` 之前）添加实现

```cpp
bool SortImplRule::find_index_for_sort_on_join_outer(OptExpression* join_expr, void* sort_clause,
                                                     OptimizerContext* context, IndexOptInfo** out_index,
                                                     List** out_pathkeys) const
{
    *out_index = nullptr;
    *out_pathkeys = nullptr;

    if (join_expr == nullptr || sort_clause == nullptr) {
        return false;
    }

    OptExprOp* op = join_expr->get_op();
    if (op == nullptr || op->get_type() != OptExprOpType::LOGICAL_JOIN) {
        return false;
    }

    // Get outer (left) child of the join
    if (join_expr->arity() < 1) {
        return false;
    }

    OptExpression* outer_expr = join_expr->input_at(0);
    if (outer_expr == nullptr) {
        return false;
    }

    // Check join type - only non-FULL/RIGHT joins can propagate sort order
    LogicalJoin* join = static_cast<LogicalJoin*>(op);
    JoinType jtype = join->get_join_type();
    if (jtype == JOIN_FULL || jtype == JOIN_RIGHT || jtype == JOIN_RIGHT_ANTI_FULL) {
        return false;
    }

    // Delegate to the original find_index_for_sort on the outer child
    return find_index_for_sort(outer_expr, sort_clause, context, out_index, out_pathkeys);
}
```

---

## Task 2: 修改 `set_sort_requirement_on_scan` 支持 LogicalJoin

**Files:**
- Modify: `src/gausskernel/optimizer/cascades/rule/sort_impl_rule.cpp:252-279`

**Step 1:** 修改 `set_sort_requirement_on_scan` 方法

找到当前代码（第272-278行）：
```cpp
    // Recursively set on child for filter/project/aggregate
    if (op_type == OptExprOpType::LOGICAL_FILTER ||
        op_type == OptExprOpType::LOGICAL_PROJECT ||
        op_type == OptExprOpType::LOGICAL_AGGREGATE) {
        if (expr->arity() > 0) {
            set_sort_requirement_on_scan(expr->input_at(0), sort_clause);
        }
    }
}
```

替换为：
```cpp
    // Recursively set on child for filter/project/aggregate/join
    if (op_type == OptExprOpType::LOGICAL_FILTER ||
        op_type == OptExprOpType::LOGICAL_PROJECT ||
        op_type == OptExprOpType::LOGICAL_AGGREGATE) {
        if (expr->arity() > 0) {
            set_sort_requirement_on_scan(expr->input_at(0), sort_clause);
        }
        return;
    }

    if (op_type == OptExprOpType::LOGICAL_JOIN) {
        // Pass sort requirement to outer (left) child
        LogicalJoin* join = static_cast<LogicalJoin*>(op);
        JoinType jtype = join->get_join_type();
        if (jtype != JOIN_FULL && jtype != JOIN_RIGHT && jtype != JOIN_RIGHT_ANTI_FULL) {
            if (expr->arity() > 0) {
                set_sort_requirement_on_scan(expr->input_at(0), sort_clause);
            }
        }
        return;
    }
}
```

---

## Task 3: 修改 `transform` 方法支持 LogicalJoin 子节点

**Files:**
- Modify: `src/gausskernel/optimizer/cascades/rule/sort_impl_rule.cpp:307-399`

**Step 1:** 在 `transform` 方法中增加对 LogicalJoin 的处理

在第396行 `return results;` 之前添加：

```cpp
    // Path 3: PassThrough on Join - eliminate sort by using index on outer table
    // Check if child is a LogicalJoin
    if (child->get_op()->get_type() == OptExprOpType::LOGICAL_JOIN) {
        IndexOptInfo* join_index = nullptr;
        List* join_pathkeys = nullptr;
        if (find_index_for_sort_on_join_outer(child, sort_clause, context, &join_index, &join_pathkeys)) {
            ereport(WARNING, (errmsg("[CASCADES] SortImplRule: found index for sort on join outer, creating PassThrough")));

            // Create PassThrough with the Join as child
            PassThroughOperator* pass = NEW_EX(PassThroughOperator);
            OptExpression* pass_expr = OptExpression::create(pass, child);
            pass_expr->set_sort_requirement(sort_clause);
            results.push_back(pass_expr);

            ereport(WARNING, (errmsg("[CASCADES] SortImplRule: generated PassThrough on Join path")));
        }
    }
```

---

## Task 4: 验证编译（不出错）

**Step 1:** 验证修改后能编译

设置环境并编译：
```bash
cd /home/hcy/projects/openGauss-server/tmp_build
export THIRD_BIN_PATH=/home/hcy/projects/binarylibs
make -k 2>&1 | head -100
```

预期结果：编译无错误（允许警告）

---

## Task 5: 创建集成测试验证

**Files:**
- Create: `src/test/integ_tests/optimizer/cascades_join_sort/test_cascades_join_sort.py`（如果存在测试目录结构）

**Step 1:** 创建测试 SQL 文件

```sql
-- 场景1: ORDER BY 外表列，有可用索引
SELECT * FROM t1 JOIN t2 ON t1.id = t2.id ORDER BY t1.b;

-- 场景2: ORDER BY 外表主键列
SELECT * FROM t1 JOIN t2 ON t1.id = t2.id ORDER BY t1.id;

-- 场景3: ORDER BY 内表列，无法消除
SELECT * FROM t1 JOIN t2 ON t1.id = t2.id ORDER BY t2.b;

-- 场景4: RIGHT JOIN 外表列，无法消除
SELECT * FROM t1 RIGHT JOIN t2 ON t1.id = t2.id ORDER BY t1.b;
```

---

## 实现检查清单

- [ ] `sort_impl_rule.h` 新增方法声明
- [ ] `sort_impl_rule.cpp` 实现 `find_index_for_sort_on_join_outer`
- [ ] `sort_impl_rule.cpp` 修改 `set_sort_requirement_on_scan` 支持 LogicalJoin
- [ ] `sort_impl_rule.cpp` 修改 `transform` 增加 Join PassThrough 路径
- [ ] 编译通过
- [ ] 集成测试验证

---

## 约束与假设

1. 仅支持外表（left child）索引消除
2. 不支持 FULL/RIGHT/ANTI_FULL JOIN 的排序消除
3. 假设 `find_index_for_sort` 已能正确处理外表是 LogicalScan 的情况
4. 假设 `IndexScanImplRule` 已能正确响应 PassThrough 传递的排序要求