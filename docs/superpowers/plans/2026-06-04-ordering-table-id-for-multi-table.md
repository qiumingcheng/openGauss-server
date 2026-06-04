# Ordering 新增 table_id 支持多表索引消除

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在 `Ordering` 结构体中新增 `table_id` 字段，使 `OrderSpec::is_satisfy` 在比较排序满足性时能区分不同表的排序列，修复多表 JOIN 场景下索引消除失败的问题。

**Architecture:** `Ordering` 当前只有 `column_id`(tleSortGroupRef) 和 `ascending`，在单表场景下 tleSortGroupRef 可唯一标识排序列。但在多表 JOIN 场景下，不同表的列可能产生相同 column_id 或列来源无法区分，导致 enforce 阶段排序满足性比较失败。新增 `table_id` 后，`is_satisfy` 比较时会同时检查 column_id、table_id 和 ascending 三个维度，确保多表场景下排序来源准确匹配。

**Tech Stack:** C++, openGauss Cascades Optimizer

---

## 分析与设计决策

### 问题根因

`Ordering` 结构体 (`sort_property.h:14-19`)：
```cpp
struct Ordering {
    int column_id;    // tleSortGroupRef
    bool ascending;
};
```

`OrderSpec::is_satisfy` 的比较 (`sort_property.cpp:8-24`)：
```cpp
for (size_t i = 0; i < required.orderings_.size(); ++i) {
    if (orderings_[i].column_id != required.orderings_[i].column_id ||
        orderings_[i].ascending != required.orderings_[i].ascending) {
        return false;
    }
}
```

在多表 JOIN 场景下，例如：
```sql
SELECT * FROM t1 JOIN t2 ON t1.a = t2.a ORDER BY t1.a;
```

- ORDER BY 的 tleSortGroupRef 引用的是 t1.a
- 但 enforce 比较时，如果某个表达式的 OrderSpec 来源于其他表的索引，column_id 相同但来源表不同，无法区分
- `PhysicalIndexScan` 上已有 `table_id_` 字段，但排序信息中的 `column_id` 不包含表标识，导致 enforce 阶段无法正确匹配

### 解决方案

在 `Ordering` 结构体中新增 `int table_id` 字段（默认值 -1 表示"未知表"）。`is_satisfy` 比较规则：只有当 output 和 required 的 `table_id` 均为有效值（>= 0）且不相等时，才判定为不满足；如果任一方为 -1，则跳过 table_id 比较（向后兼容）。

### 影响范围

| 文件 | 操作 |
|------|------|
| `property/sort_property.h` | 修改 Ordering、OrderSpec |
| `property/sort_property.cpp` | 修改 is_satisfy、hash、to_string |
| `task/enforce_and_cost_task.cpp` | 修改 5 处 OrderSpec 构建 |
| `task/apply_rule_task.cpp` | 修改 build_physical_property_set_from_sort_clause |
| `operator/operators/physical_scan.h` | 修改 add_sort_column 签名 |
| `operator/operators/physical_scan.cpp` | 修改 sort_columns_/sort_ascending_ 为 tuple |
| `rule/sort_impl_rule.cpp` | 修改 create_index_scan_to_group 中 add_sort_column |
| `rule/index_scan_impl_rule.cpp` | 修改 add_sort_column 调用 |
| `property/required_property_deriver.cpp` | 修改 MergeJoin 子属性推导 |

### 不影响的组件

- **PhysicalSort / PhysicalSort::append_enforcers** — 不依赖 OrderSpec 的 table_id
- **SortImplRule 的 index 查找逻辑** — 使用 SortGroupClause 直接比较
- **expression_to_plan** — 仅使用 PhysicalSort 的 sort_clause 指针

---

## Task 分解

### Task 1: 修改 Ordering 和 OrderSpec 数据结构

**Files:**
- Modify: `src/gausskernel/optimizer/cascades/property/sort_property.h:14-51`
- Modify: `src/gausskernel/optimizer/cascades/property/sort_property.cpp:8-33,65-78`

- [ ] **Step 1: 修改 sort_property.h 中的 Ordering 结构体**

将 `Ordering` 从：
```cpp
struct Ordering {
    int column_id;
    bool ascending;  // true = ASC, false = DESC

    Ordering(int col, bool asc) : column_id(col), ascending(asc) {}
};
```

改为：
```cpp
struct Ordering {
    int column_id;
    bool ascending;  // true = ASC, false = DESC
    int table_id;    // cascades table_id, -1 = unknown

    Ordering(int col, bool asc, int tid = -1) : column_id(col), ascending(asc), table_id(tid) {}
};
```

- [ ] **Step 2: 修改 sort_property.h 中 OrderSpec::add_ordering**

```cpp
void add_ordering(int column_id, bool ascending) {
    orderings_.emplace_back(column_id, ascending);
}
```

改为：
```cpp
void add_ordering(int column_id, bool ascending, int table_id = -1) {
    orderings_.emplace_back(column_id, ascending, table_id);
}
```

- [ ] **Step 3: 修改 sort_property.h 中 OrderSpec::operator==**

在 column_id 和 ascending 比较后增加 table_id 比较（仅当双方均有效时）：
```cpp
bool operator==(const OrderSpec& other) const {
    if (orderings_.size() != other.orderings_.size()) return false;
    for (size_t i = 0; i < orderings_.size(); ++i) {
        if (orderings_[i].column_id != other.orderings_[i].column_id ||
            orderings_[i].ascending != other.orderings_[i].ascending) {
            return false;
        }
        if (orderings_[i].table_id >= 0 && other.orderings_[i].table_id >= 0 &&
            orderings_[i].table_id != other.orderings_[i].table_id) {
            return false;
        }
    }
    return true;
}
```

- [ ] **Step 4: 修改 sort_property.cpp 中 OrderSpec::is_satisfy**

```cpp
// 原代码:
for (size_t i = 0; i < required.orderings_.size(); ++i) {
    if (orderings_[i].column_id != required.orderings_[i].column_id ||
        orderings_[i].ascending != required.orderings_[i].ascending) {
        return false;
    }
}

// 改为:
for (size_t i = 0; i < required.orderings_.size(); ++i) {
    if (orderings_[i].column_id != required.orderings_[i].column_id ||
        orderings_[i].ascending != required.orderings_[i].ascending) {
        return false;
    }
    // Both sides have valid table_id but they differ -> not satisfied
    if (orderings_[i].table_id >= 0 && required.orderings_[i].table_id >= 0 &&
        orderings_[i].table_id != required.orderings_[i].table_id) {
        return false;
    }
}
```

- [ ] **Step 5: 修改 sort_property.cpp 中 OrderSpec::hash**

```cpp
// 原代码:
for (const auto& ord : orderings_) {
    h = h * 31 + ord.column_id;
    h = h * 37 + (ord.ascending ? 1 : 0);
}

// 改为:
for (const auto& ord : orderings_) {
    h = h * 31 + ord.column_id;
    h = h * 37 + (ord.ascending ? 1 : 0);
    h = h * 41 + ord.table_id;
}
```

- [ ] **Step 6: 修改 sort_property.cpp 中 SortProperty::to_string**

```cpp
// 原代码:
result += "col" + std::to_string(orderings[i].column_id);
result += orderings[i].ascending ? " ASC" : " DESC";

// 改为:
result += "col" + std::to_string(orderings[i].column_id);
if (orderings[i].table_id >= 0) {
    result += "(table=" + std::to_string(orderings[i].table_id) + ")";
}
result += orderings[i].ascending ? " ASC" : " DESC";
```

- [ ] **Step 7: Commit**

```bash
git add src/gausskernel/optimizer/cascades/property/sort_property.h \
        src/gausskernel/optimizer/cascades/property/sort_property.cpp
git commit -m "feat(cascades): Ordering 结构体新增 table_id 字段支持多表排序区分"
```

### Task 2: 修改 PhysicalIndexScan 的 sort_columns 存储

`PhysicalIndexScan` 目前分别存储 `sort_columns_` (vector<int>) 和 `sort_ascending_` (vector<bool>)，需要改为同时存储 table_id。

**Files:**
- Modify: `src/gausskernel/optimizer/cascades/operator/operators/physical_scan.h:59-74`
- Modify: `src/gausskernel/optimizer/cascades/operator/operators/physical_scan.cpp:76-86`

- [ ] **Step 1: 修改 physical_scan.h 中 PhysicalIndexScan 的 sort 相关接口**

将：
```cpp
void add_sort_column(int column_id, bool ascending) {
    sort_columns_.push_back(column_id);
    sort_ascending_.push_back(ascending);
}
bool has_sort_info() const { return !sort_columns_.empty(); }
int get_sort_column_count() const { return sort_columns_.size(); }
int get_sort_column_id(int idx) const { return sort_columns_[idx]; }
bool get_sort_ascending(int idx) const { return sort_ascending_[idx]; }
```

改为：
```cpp
struct SortColumnInfo {
    int column_id = -1;
    bool ascending = true;
    int table_id = -1;
};

void add_sort_column(int column_id, bool ascending, int table_id = -1) {
    SortColumnInfo info;
    info.column_id = column_id;
    info.ascending = ascending;
    info.table_id = table_id;
    sort_columns_.push_back(info);
}
bool has_sort_info() const { return !sort_columns_.empty(); }
int get_sort_column_count() const { return sort_columns_.size(); }
int get_sort_column_id(int idx) const { return sort_columns_[idx].column_id; }
bool get_sort_ascending(int idx) const { return sort_columns_[idx].ascending; }
int get_sort_table_id(int idx) const { return sort_columns_[idx].table_id; }
```

并将：
```cpp
std::vector<int> sort_columns_;
std::vector<bool> sort_ascending_;
```

改为：
```cpp
std::vector<SortColumnInfo> sort_columns_;
```

- [ ] **Step 2: 修改 physical_scan.cpp 中 clone()**

```cpp
// 原代码:
for (size_t i = 0; i < sort_columns_.size(); i++) {
    cloned->add_sort_column(sort_columns_[i], sort_ascending_[i]);
}

// 改为:
for (size_t i = 0; i < sort_columns_.size(); i++) {
    cloned->add_sort_column(sort_columns_[i].column_id,
                            sort_columns_[i].ascending,
                            sort_columns_[i].table_id);
}
```

- [ ] **Step 3: Commit**

```bash
git add src/gausskernel/optimizer/cascades/operator/operators/physical_scan.h \
        src/gausskernel/optimizer/cascades/operator/operators/physical_scan.cpp
git commit -m "refactor(cascades): PhysicalIndexScan sort_columns 使用 SortColumnInfo 结构体存储 table_id"
```

### Task 3: 在 enforce_and_cost_task.cpp 中传递 table_id

这是核心修改点。`derive_output_property` 中从 PhysicalIndexScan 构建 OrderSpec 时需要传递 table_id；`build_sort_property_from_sort_clause` 中也需要设置 table_id（从 sort_clause 推导或留 -1）。

**Files:**
- Modify: `src/gausskernel/optimizer/cascades/task/enforce_and_cost_task.cpp:232-248,285-308,519-537`

- [ ] **Step 1: 修改 derive_output_property 中 IndexScan 分支**

```cpp
// 原代码 (line 232-242):
if (scan->has_sort_info()) {
    OrderSpec spec;
    for (int i = 0; i < scan->get_sort_column_count(); i++) {
        spec.add_ordering(scan->get_sort_column_id(i), scan->get_sort_ascending(i));
    }
    SortProperty sort_prop(spec);
    return PhysicalPropertySet(DistributionProperty(DistributionType::ANY), sort_prop);
}

// 改为:
if (scan->has_sort_info()) {
    OrderSpec spec;
    for (int i = 0; i < scan->get_sort_column_count(); i++) {
        spec.add_ordering(scan->get_sort_column_id(i),
                          scan->get_sort_ascending(i),
                          scan->get_sort_table_id(i));
    }
    SortProperty sort_prop(spec);
    return PhysicalPropertySet(DistributionProperty(DistributionType::ANY), sort_prop);
}
```

- [ ] **Step 2: 修改 build_sort_property_from_sort_clause**

```cpp
// 原代码 (line 526-534):
OrderSpec spec;
ListCell* lc = nullptr;
foreach (lc, clauses) {
    SortGroupClause* sgcl = static_cast<SortGroupClause*>(lfirst(lc));
    int column_id = sgcl->tleSortGroupRef;
    bool ascending = (sgcl->sortop < 500);
    spec.add_ordering(column_id, ascending);
}

// 改为 (table_id 暂留 -1，后续可从 SortGroupClause 的 Var 表达式推导):
OrderSpec spec;
ListCell* lc = nullptr;
foreach (lc, clauses) {
    SortGroupClause* sgcl = static_cast<SortGroupClause*>(lfirst(lc));
    int column_id = sgcl->tleSortGroupRef;
    bool ascending = (sgcl->sortop < 500);
    spec.add_ordering(column_id, ascending, -1);  // table_id unknown from sort_clause alone
}
```

- [ ] **Step 3: 修改 derive_output_property 中 PHYSICAL_SORT_AGG 分支**

```cpp
// 原代码 (line 290-293):
for (int key : group_keys) {
    spec.add_ordering(key, true);  // Assume ascending
}

// 改为:
for (int key : group_keys) {
    spec.add_ordering(key, true, -1);
}
```

- [ ] **Step 4: 修改 derive_output_property 中 PHYSICAL_MERGE_JOIN 分支**

```cpp
// 原代码 (line 302-305):
for (int key : probe_keys) {
    spec.add_ordering(key, true);  // Assume ascending
}

// 改为:
for (int key : probe_keys) {
    spec.add_ordering(key, true, -1);
}
```

- [ ] **Step 5: Commit**

```bash
git add src/gausskernel/optimizer/cascades/task/enforce_and_cost_task.cpp
git commit -m "refactor(cascades): enforce_and_cost_task 中传递 table_id 到 OrderSpec"
```

### Task 4: 修改 apply_rule_task.cpp 中的 OrderSpec 构建

**Files:**
- Modify: `src/gausskernel/optimizer/cascades/task/apply_rule_task.cpp:20-42`

- [ ] **Step 1: 修改 build_physical_property_set_from_sort_clause**

```cpp
// 原代码 (line 27-38):
OrderSpec spec;
ListCell* lc = nullptr;
foreach (lc, clauses) {
    SortGroupClause* sgcl = static_cast<SortGroupClause*>(lfirst(lc));
    int column_id = sgcl->tleSortGroupRef;
    bool ascending = (sgcl->sortop < 500);
    spec.add_ordering(column_id, ascending);
}

// 改为:
OrderSpec spec;
ListCell* lc = nullptr;
foreach (lc, clauses) {
    SortGroupClause* sgcl = static_cast<SortGroupClause*>(lfirst(lc));
    int column_id = sgcl->tleSortGroupRef;
    bool ascending = (sgcl->sortop < 500);
    spec.add_ordering(column_id, ascending, -1);  // table_id unknown from sort_clause alone
}
```

- [ ] **Step 2: Commit**

```bash
git add src/gausskernel/optimizer/cascades/task/apply_rule_task.cpp
git commit -m "refactor(cascades): apply_rule_task 中 add_ordering 传递 table_id"
```

### Task 5: 修改 sort_impl_rule.cpp 和 index_scan_impl_rule.cpp 设置 table_id

这是关键：在创建 PhysicalIndexScan 时，从 LogicalScan 获取 table_id 并设置到 sort_columns 中。

**Files:**
- Modify: `src/gausskernel/optimizer/cascades/rule/sort_impl_rule.cpp:385-393`
- Modify: `src/gausskernel/optimizer/cascades/rule/index_scan_impl_rule.cpp:238-247`

- [ ] **Step 1: 修改 sort_impl_rule.cpp 中 create_index_scan_to_group**

```cpp
// 原代码 (line 385-393):
List* clauses = static_cast<List*>(sort_clause);
List* targetlist = root->parse->targetList;
ListCell* lc = nullptr;
foreach (lc, clauses) {
    SortGroupClause* sgcl = static_cast<SortGroupClause*>(lfirst(lc));
    bool ascending = (sgcl->sortop < 500);  // heuristic
    index_scan->add_sort_column(sgcl->tleSortGroupRef, ascending);
}

// 改为 (从 scan 获取 table_id):
List* clauses = static_cast<List*>(sort_clause);
List* targetlist = root->parse->targetList;
ListCell* lc = nullptr;
int scan_table_id = scan->get_table_id();
foreach (lc, clauses) {
    SortGroupClause* sgcl = static_cast<SortGroupClause*>(lfirst(lc));
    bool ascending = (sgcl->sortop < 500);  // heuristic
    index_scan->add_sort_column(sgcl->tleSortGroupRef, ascending, scan_table_id);
}
```

- [ ] **Step 2: 修改 index_scan_impl_rule.cpp 中 transform**

```cpp
// 原代码 (line 238-247):
if (sort_clause != nullptr) {
    List* clauses = static_cast<List*>(sort_clause);
    List* targetlist = root->parse->targetList;
    ListCell* lc_clause = nullptr;
    foreach (lc_clause, clauses) {
        SortGroupClause* sgcl = static_cast<SortGroupClause*>(lfirst(lc_clause));
        bool ascending = (sgcl->sortop < 500);  // heuristic
        physical_scan->add_sort_column(sgcl->tleSortGroupRef, ascending);
    }
}

// 改为 (从 LogicalScan 获取 table_id):
if (sort_clause != nullptr) {
    List* clauses = static_cast<List*>(sort_clause);
    List* targetlist = root->parse->targetList;
    ListCell* lc_clause = nullptr;
    int scan_table_id = scan_op->get_table_id();
    foreach (lc_clause, clauses) {
        SortGroupClause* sgcl = static_cast<SortGroupClause*>(lfirst(lc_clause));
        bool ascending = (sgcl->sortop < 500);  // heuristic
        physical_scan->add_sort_column(sgcl->tleSortGroupRef, ascending, scan_table_id);
    }
}
```

- [ ] **Step 3: Commit**

```bash
git add src/gausskernel/optimizer/cascades/rule/sort_impl_rule.cpp \
        src/gausskernel/optimizer/cascades/rule/index_scan_impl_rule.cpp
git commit -m "feat(cascades): SortImplRule/IndexScanImplRule 创建 IndexScan 时设置 sort_column 的 table_id"
```

### Task 6: 修改 required_property_deriver.cpp

**Files:**
- Modify: `src/gausskernel/optimizer/cascades/property/required_property_deriver.cpp:117-122`

- [ ] **Step 1: 修改 derive_merge_join_child_property**

```cpp
// 原代码:
for (int key : *keys) {
    spec.add_ordering(key, true);  // true = ascending
}

// 改为:
for (int key : *keys) {
    spec.add_ordering(key, true, -1);  // table_id unknown for merge join keys
}
```

- [ ] **Step 2: Commit**

```bash
git add src/gausskernel/optimizer/cascades/property/required_property_deriver.cpp
git commit -m "refactor(cascades): required_property_deriver 中 add_ordering 传递 table_id"
```

### Task 7: 编译验证和集成测试

**Files:**
- 测试现有集成测试用例

- [ ] **Step 1: 增量编译**

```bash
cd /home/hcy/projects/openGauss-server/tmp_build
export THIRD_BIN_PATH=/home/hcy/projects/binarylibs
make cascadesoptimizer -j4
make install
```

- [ ] **Step 2: 重启 gaussdb**

```bash
export LD_LIBRARY_PATH=/home/hcy/gaussdb/lib:$LD_LIBRARY_PATH
/home/hcy/gaussdb/bin/gs_ctl restart -D /home/hcy/gaussdata2/ -l /tmp/gaussdb.log
sleep 3
```

- [ ] **Step 3: 单表 ORDER BY 回归测试**

验证单表场景不受影响：
```bash
export LD_LIBRARY_PATH=/home/hcy/gaussdb/lib:$LD_LIBRARY_PATH
/home/hcy/gaussdb/bin/gsql -d testdb -c "set enable_cascades_optimizer = on; EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM t1 ORDER BY a;"
/home/hcy/gaussdb/bin/gsql -d testdb -c "set enable_cascades_optimizer = off; EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM t1 ORDER BY a;"
```

- [ ] **Step 4: 多表 ORDER BY 索引消除测试**

验证多表 JOIN + ORDER BY 场景，索引消除应正确工作：
```bash
/home/hcy/gaussdb/bin/gsql -d testdb -c "set enable_cascades_optimizer = on; EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM t1 JOIN t2 ON t1.a = t2.a ORDER BY t1.a;"
/home/hcy/gaussdb/bin/gsql -d testdb -c "set enable_cascades_optimizer = off; EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM t1 JOIN t2 ON t1.a = t2.a ORDER BY t1.a;"
```

- [ ] **Step 5: 实际查询结果验证**

```bash
/home/hcy/gaussdb/bin/gsql -d testdb -c "SELECT * FROM t1 JOIN t2 ON t1.a = t2.a ORDER BY t1.a;"
```

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "test(cascades): 验证多表 ORDER BY 索引消除 table_id 支持的正确性"
```

## 自审

**Spec coverage:**
- Task 1: Ordering/OrderSpec 数据结构扩展 — 核心修改
- Task 2: PhysicalIndexScan sort_columns 存储升级 — 支持 table_id 持久化
- Task 3: enforce_and_cost_task 传递 table_id — enforce 阶段的比较核心
- Task 4: apply_rule_task 传递 table_id — sort requirement 构建
- Task 5: sort_impl_rule/index_scan_impl_rule 设置 table_id — 关键数据源
- Task 6: required_property_deriver 传递 table_id — MergeJoin 分支
- Task 7: 编译验证和集成测试 — 正确性验证

**Placeholder scan:** 无占位符，所有代码步骤包含具体代码。

**Type consistency:**
- `table_id` 类型统一为 `int`，与 LogicalScan::table_id_ 和 PhysicalIndexScan::table_id_ 一致
- 默认值统一为 -1，与现有 table_id 默认值一致
- `SortColumnInfo` 结构体定义在 physical_scan.h 的 PhysicalIndexScan 类内部，命名与 `get_sort_column_*` 前缀一致

**向后兼容设计:**
- `Ordering(int col, bool asc, int tid = -1)` — 构造函数向后兼容
- `add_ordering(int, bool, int = -1)` — 默认参数向后兼容
- `is_satisfy` 和 `operator==` 中 `table_id >= 0` 条件判断 — -1 不参与比较
- 所有现有 `add_ordering(a, b)` 调用无需修改即可编译

**缺陷风险:**
- `build_sort_property_from_sort_clause` 和 `build_physical_property_set_from_sort_clause` 中 table_id 暂为 -1，这意味着 enforce 时只比较 IndexScan 侧的 table_id。如果 IndexScan 的 table_id 为有效值，required 的 table_id 为 -1，比较会通过（-1 不参与比较）。这是期望行为：保证 IndexScan 声明的排序在单表场景下正常工作。后续可通过分析 SortGroupClause 中的 Var 表达式来精确设置 table_id。
