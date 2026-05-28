# 2表JOIN索引消除ORDER BY设计方案

## 1. 背景与目标

### 1.1 问题描述

当前 Cascades 优化器的 `SortImplRule` 只支持单表场景的排序消除（通过索引扫描）。当 SQL 包含 2 表 JOIN 且带有 ORDER BY 时，如果 ORDER BY 列恰好是外表（驱动表）的列且该列有可用索引，无法利用索引消除排序。

### 1.2 期望效果

对于如下 SQL：
```sql
SELECT * FROM t1 JOIN t2 ON t1.id = t2.id ORDER BY t1.b
```

期望执行计划：
```
PhysicalHashJoin
├── 外表: LogicalIndexScan(t1, idx_t1_b)  -- 通过PassThrough传递的排序要求选择索引
└── 内表: LogicalScan(t2)
```

即：通过在 Join 上方放置 PassThrough，将 ORDER BY 要求传递给外表，让外表使用索引扫描消除排序。

## 2. 技术分析

### 2.1 现有相关组件

| 组件 | 文件 | 作用 |
|------|------|------|
| SortImplRule | `rule/sort_impl_rule.cpp` | 将 LogicalSort 转换为 PhysicalSort 或 PassThrough |
| IndexScanImplRule | `rule/index_scan_impl_rule.cpp` | 将 LogicalScan 转换为带索引的 PhysicalIndexScan |
| PassThroughOperator | `operator/operators/physical_passthrough.cpp` | 传递排序要求给子节点 |
| RequiredPropertyDeriver | `property/required_property_deriver.cpp` | 推导子节点的 Required Property |

### 2.2 关键数据流

1. `SortImplRule::transform()` 生成两个路径：
   - `PhysicalSort`: 显式排序
   - `PassThrough`: 排序消除（传递排序要求给子节点）

2. `PassThrough` 设置 `sort_requirement`，传递给外表

3. 外表的 `IndexScanImplRule` 检测到排序要求，选择对应索引

### 2.3 当前限制

- `SortImplRule::find_index_for_sort()` 只处理 LogicalScan/LogicalFilter/LogicalProject/LogicalAggregate 作为子节点
- `SortImplRule::set_sort_requirement_on_scan()` 同样只处理上述类型
- 未处理 LogicalJoin 作为子节点的情况

## 3. 实现方案（路径1）

### 3.1 修改点总览

| 序号 | 文件 | 修改内容 |
|------|------|----------|
| 1 | `sort_impl_rule.cpp` | `transform()` 增加对 LogicalJoin 子节点的判断 |
| 2 | `sort_impl_rule.cpp` | `find_index_for_sort()` 增加递归到外表查索引的逻辑 |
| 3 | `sort_impl_rule.cpp` | `set_sort_requirement_on_scan()` 增加对 LogicalJoin 的处理 |
| 4 | `sort_impl_rule.h` | 新增辅助方法声明 |

### 3.2 详细设计

#### 3.2.1 `SortImplRule::transform()` 修改

**位置**: `sort_impl_rule.cpp` `SortImplRule::transform()`

**新增逻辑**:
```cpp
// 在创建 PassThrough 路径前，增加对 LogicalJoin 的检测
OptExpression* child = expr->get_child(0);
if (child->get_op()->get_type() == LOGICAL_JOIN) {
    // 检查外表是否有可用索引满足排序要求
    IndexOptInfo* index = nullptr;
    List* pathkeys = nullptr;
    if (find_index_for_sort_on_join_outer(child, sort_clause, &index, &pathkeys)) {
        // 创建 PassThrough，挂在 Join 上方
        PassThroughOperator* pass = NEW_EX(PassThroughOperator);
        OptExpression* pass_expr = OptExpression::create(pass, child);
        pass_expr->set_sort_requirement(sort_clause);
        results.push_back(pass_expr);
    }
    // 继续原有 PhysicalSort 路径
}
```

#### 3.2.2 `find_index_for_sort()` 修改 - 新增辅助方法

**新增方法**: `SortImplRule::find_index_for_sort_on_join_outer()`

**签名**:
```cpp
bool find_index_for_sort_on_join_outer(
    OptExpression* join_expr,
    void* sort_clause,
    IndexOptInfo** out_index,
    List** out_pathkeys) const
```

**逻辑**:
1. 获取 Join 的外表（outer child）: `join_expr->get_child(0)`
2. 递归调用原 `find_index_for_sort()` 在外表上查找索引
3. 如果找到，返回 true 并设置输出参数

#### 3.2.3 `set_sort_requirement_on_scan()` 修改

**位置**: `sort_impl_rule.cpp`

**新增 case**:
```cpp
case LOGICAL_JOIN: {
    // 将排序要求传递给外表（child 0）
    OptExpression* outer = expr->get_child(0);
    set_sort_requirement_on_scan(outer, sort_clause);
    break;
}
```

### 3.3 约束条件

1. **仅支持非 FULL/RIGHT JOIN**: 这类 JOIN 会破坏排序
2. **仅支持外表（outer child）消除**: 内表（inner child）作为被驱动表，不参与排序消除
3. **排序列必须是外表列**: ORDER BY 列需要属于外表对应的表
4. **外表需要有可用索引**: 索引列需要匹配排序键

### 3.4 测试场景

| 场景 | SQL | 期望行为 |
|------|-----|----------|
| 1 | `SELECT * FROM t1 JOIN t2 ON t1.id = t2.id ORDER BY t1.b` | 外表 t1 使用 idx_t1_b 索引消除排序 |
| 2 | `SELECT * FROM t1 JOIN t2 ON t1.id = t2.id ORDER BY t1.id` | 外表 t1 使用主键索引（id） |
| 3 | `SELECT * FROM t1 JOIN t2 ON t1.id = t2.id ORDER BY t2.b` | 内表列，无法消除，需要 PhysicalSort |
| 4 | `SELECT * FROM t1 RIGHT JOIN t2 ON t1.id = t2.id ORDER BY t1.b` | RIGHT JOIN 破坏排序，无法消除 |

## 4. 风险与注意事项

1. **路径键匹配**: 需要确保 PassThrough 传递的排序要求与 IndexScan 期望的路径键格式匹配
2. **等价比类（EquivalenceClass）**: 如果 ORDER BY 列在等价类中，需要正确处理
3. **多列排序**: 需要支持 `ORDER BY t1.b, t1.c` 等多列场景

## 5. 后续扩展

- 支持内表消除排序（NestLoop inner index scan 场景）
- 支持 3 表及更多表的 JOIN 场景
- 支持 MergeJoin 的排序消除