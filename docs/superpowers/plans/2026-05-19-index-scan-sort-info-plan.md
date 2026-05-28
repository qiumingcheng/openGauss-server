# IndexScanImplRule 排序信息设置实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在 IndexScanImplRule::transform 中，当 IndexPath 匹配 ORDER BY 时，设置 PhysicalIndexScan 的排序信息

**Architecture:** 在 IndexPath 分支内，创建 PhysicalIndexScan 后从 sort_clause 提取排序信息并调用 add_sort_column

**Tech Stack:** C++, openGauss optimizer cascades

---

## File: src/gausskernel/optimizer/cascades/rule/index_scan_impl_rule.cpp

**Modify:** `index_scan_impl_rule.cpp:236` - 在 `physical_scan = NEW_EX(PhysicalIndexScan, (Path*)index_path);` 后添加排序信息设置

- [ ] **Step 1: 添加排序信息设置代码**

在第 236 行后添加：
```cpp
            // 设置排序信息：从 sort_clause 提取
            if (sort_clause != nullptr) {
                List* clauses = static_cast<List*>(sort_clause);
                List* targetlist = root->parse->targetList;
                ListCell* lc = nullptr;
                foreach (lc, clauses) {
                    SortGroupClause* sgcl = static_cast<SortGroupClause*>(lfirst(lc));
                    TargetEntry* tle = get_sortgroupref_tle(sgcl->tleSortGroupRef, targetlist, false);
                    if (tle != nullptr && tle->expr != nullptr && IsA(tle->expr, Var)) {
                        Var* var = (Var*)tle->expr;
                        bool ascending = (sgcl->sortop < 500);  // heuristic
                        physical_scan->add_sort_column(var->varattno, ascending);
                    }
                }
            }
```

- [ ] **Step 2: 检查是否需要头文件**

确认 `nodes/parsenodes.h` 已包含（用于 `TargetEntry`、`SortGroupClause`、`Var` 类型）

- [ ] **Step 3: Review 代码变更**

检查添加的代码与 `sort_impl_rule.cpp` 第 364-376 行逻辑一致性

---

## 验证

- [ ] **Step 4: 编译验证**

设置 THIRD_BIN_PATH 后在 tmp_build 目录执行 `make -k`

- [ ] **Step 5: 集成测试**

通过集成测试验证索引扫描计划包含正确的排序信息