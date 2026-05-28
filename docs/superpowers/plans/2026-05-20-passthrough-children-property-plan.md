# PassThrough children_required_property 修复计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 修复 `Memo::build_best_plan` 函数，使其在处理 PassThrough 节点时正确检查 `children_required_property_`

**Architecture:** `build_best_plan` 需要像 `EnforceAndCostTask` 一样，先检查 `children_required_property_` 是否存在，如果存在则优先使用，否则回退到 `get_input_properties`

**Tech Stack:** C++, openGauss optimizer cascades

---

## 问题根因

在 `EnforceAndCostTask::execute()` 中处理 PassThrough 时：
```cpp
const PhysicalPropertySet* explicit_prop = expr->get_children_required_property();
if (explicit_prop != nullptr) {
    child_required = *explicit_prop;  // 正确使用
}
```

但在 `Memo::build_best_plan()` 中：
```cpp
std::vector<PhysicalPropertySet> input_properties = expr->get_input_properties(required_property);
// 只用 get_input_properties，从不检查 children_required_property_！
```

---

## File: src/gausskernel/optimizer/cascades/memo/memo.cpp

**Modify:** `memo.cpp:433-463` - `build_best_plan` 函数

- [ ] **Step 1: 修改 build_best_plan 检查 children_required_property_**

修改 `memo.cpp` 第 446-452 行：

```cpp
// 原代码：
PhysicalPropertySet child_required;
if (!input_properties.empty() && i < static_cast<int>(input_properties.size())) {
    child_required = input_properties[i];
}

// 修改为：
PhysicalPropertySet child_required;
// First check if GroupExpression has explicitly set children_required_property_
// This is set by SortImplRule for PassThrough operator
const PhysicalPropertySet* explicit_prop = expr->get_children_required_property();
if (explicit_prop != nullptr) {
    child_required = *explicit_prop;
} else if (!input_properties.empty() && i < static_cast<int>(input_properties.size())) {
    child_required = input_properties[i];
}
```

- [ ] **Step 2: 添加调试日志（可选）**

在 `build_best_plan` 中 PassThrough 分支添加日志，确认使用了正确的 child_required

- [ ] **Step 3: Review 代码变更**

检查修改是否与 `EnforceAndCostTask` 中的逻辑一致

---

## 验证

- [ ] **Step 4: 集成测试验证**

通过 SQL 查询 `ORDER BY` 带索引的表，验证执行计划不包含多余的 Sort 节点