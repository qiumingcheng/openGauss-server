# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

openGauss is an open-source relational database management system optimized for multi-core high performance, NUMA architecture, and ARM platforms. This repository contains the **cascades** branch, which implements a Cascades-style query optimizer alongside the traditional PostgreSQL-based optimizer.

Key features:
- Multi-core high performance (150万+ tpmC on 128-core Kunpeng)
- Memory-Optimized Tables (MOT) engine (350万+ tpmC)
- Cascades query optimizer (experimental)
- LLVM JIT compilation
- Vectorized execution engine
- NUMA-aware data structures

## Build System

### Prerequisites

Install build dependencies (openEuler/CentOS):
```bash
sudo yum install -y libedit-devel libxml2-devel lz4-devel numactl-devel \
    unixODBC-devel java-1.8.0-openjdk-devel make binutils libaio-devel \
    flex bison ncurses-devel glibc-devel patch readline-devel \
    openblas-devel python3 gcc gcc-c++ cmake dkms
```

Download third-party binary libraries (binarylibs) from:
- https://opengauss.obs.cn-south-1.myhuaweicloud.com/latest/binarylibs/

Extract to `binarylibs/` directory.

### Full Build

```bash
# Standard release build with CMake (recommended)
./build.sh -3rd /path/to/binarylibs --cmake

# Debug build
./build.sh -m debug -3rd /path/to/binarylibs --cmake

# Build with packaging
./build.sh -3rd /path/to/binarylibs --cmake -wrap
```

Build artifacts are placed in:
- `tmp_build/` - CMake build directory
- `mppdb_temp_install/` - Installation directory

### Incremental Build (Cascades Optimizer)

When modifying Cascades optimizer code:

```bash
cd tmp_build

# Method 1: Clean only Cascades targets (fast)
rm -rf CMakeFiles/cascadesoptimizer
rm -rf src/gausskernel/optimizer/cascades/CMakeFiles/cascadesoptimizer.dir
make cascadesoptimizer -j8
make gaussdb

# Method 2: Full reconfigure (when Method 1 fails)
rm -rf *
export THIRD_BIN_PATH=/path/to/binarylibs
cmake .. -DENABLE_OPENSSL3=ON -DUSE_SSL=ON
make cascadesoptimizer -j8
make gaussdb
```

**Important**: The main `gaussdb` binary is ~650MB and requires 8GB+ RAM to link. Increase swap space if needed:
```bash
sudo dd if=/dev/zero of=/swapfile bs=1M count=8192
sudo chmod 600 /swapfile
sudo mkswap /swapfile
sudo swapon /swapfile
```

### Build Options

- `-m debug|release|memcheck` - Build mode (default: release)
- `-3rd <path>` - Path to binarylibs directory (required)
- `--cmake` - Use CMake build system (faster than autoconf)
- `-wrap` - Package binaries after build
- `-pm opengauss|lite|finance` - Product mode
- `-bs 4096|8192` - Block size (default: 8192)

## Code Architecture

### Directory Structure

```
src/
├── gausskernel/           # Core database kernel
│   ├── optimizer/         # Query optimizer
│   │   ├── cascades/      # Cascades optimizer (this branch's focus)
│   │   ├── plan/          # Traditional planner
│   │   ├── path/          # Path generation
│   │   └── rewrite/       # Query rewrite
│   ├── runtime/           # Execution engine
│   │   ├── executor/      # Executor nodes
│   │   └── vecexecutor/   # Vectorized executor
│   ├── storage/           # Storage layer
│   │   ├── access/        # Access methods (heap, btree, etc.)
│   │   ├── buffer/        # Buffer manager
│   │   └── mot/           # Memory-Optimized Tables engine
│   ├── process/           # Background processes
│   └── cbb/               # Common building blocks
├── bin/                   # Client tools (gsql, gs_dump, etc.)
├── common/                # Shared utilities
│   ├── backend/           # Backend common code
│   └── port/              # Platform portability
├── include/               # Header files
└── test/                  # Test suites
    └── regress/           # Regression tests
```

### Cascades Optimizer Architecture

Located in `src/gausskernel/optimizer/cascades/`:

```
cascades/
├── operator/              # Logical and physical operators
│   └── operators/         # Operator implementations
│       ├── logical_*.{h,cpp}   # Logical operators (Filter, Join, Scan, etc.)
│       └── physical_*.{h,cpp}  # Physical operators (HashJoin, SeqScan, etc.)
├── rule/                  # Transformation and implementation rules
│   ├── predicate_push_down_rule.cpp  # Push predicates down to scans
│   ├── column_prune_rule.cpp         # Eliminate unused columns
│   ├── filter_impl_rule.cpp          # Logical→Physical Filter
│   ├── scan_impl_rule.cpp            # Logical→Physical Scan
│   └── subquery_scan_impl_rule.cpp   # Subquery handling
├── translator/            # Query ↔ Expression conversion
│   ├── query_to_expression.cpp       # Parse tree → Cascades expression
│   └── expression_to_plan.cpp        # Cascades expression → Execution plan
├── task/                  # Optimization tasks
├── property/              # Physical properties (sort order, distribution)
├── cost/                  # Cost model
├── statistics/            # Statistics handling
└── base/                  # Core data structures (Memo, Group, etc.)
```

**Key Concepts**:
- **Memo**: Stores all equivalent logical expressions in compact form
- **Group**: Set of logically equivalent expressions
- **OptExpression**: Node in expression tree (operator + children)
- **Rule**: Transformation rule (logical→logical) or implementation rule (logical→physical)
- **Task**: Unit of optimization work (OptimizeGroup, ApplyRule, etc.)

### Cascades Optimizer Data Flow

```
SQL Query
    ↓
Parser (PostgreSQL-based)
    ↓
Query Tree (Query struct)
    ↓
query_to_expression.cpp
    ↓
Logical Expression Tree (OptExpression)
    ↓
Cascades Optimizer (cascades_optimizer.cpp)
    ├── Initialize Memo
    ├── Apply Transformation Rules (predicate pushdown, column pruning, etc.)
    ├── Generate Physical Alternatives (implementation rules)
    ├── Cost-based selection
    └── Choose best plan
    ↓
expression_to_plan.cpp
    ↓
Physical Plan Tree (Plan nodes)
    ↓
Executor
```

### Predicate Handling (Multi-Predicate Support)

The optimizer supports both single and multiple predicates per operator:

```cpp
// In LogicalFilter and LogicalScan:

// Primary API (multi-predicate)
List* get_predicate_ids() const;
void set_predicate_ids(List* ids);

// Convenience API (single-predicate)
int get_predicate_id() const {
    // Returns first predicate ID or -1 if none
}
void set_predicate_id(int id) {
    // Creates single-element list
}
```

Predicates are pushed down through `predicate_push_down_rule.cpp` and stored in `LogicalScan` nodes when applicable.

## Running the Database

### Environment Setup

```bash
# Set environment variables
source env.sh  # Located in repo root after build

# Or manually:
export GAUSSHOME=/path/to/mppdb_temp_install
export PATH=$GAUSSHOME/bin:$PATH
export LD_LIBRARY_PATH=$GAUSSHOME/lib:$LD_LIBRARY_PATH
```

### Initialize and Start Database

```bash
# Create data directory
mkdir -p /path/to/data

# Initialize database cluster
gs_initdb -D /path/to/data --nodename=single_node -w "YourPassword"

# Start database
gs_ctl start -D /path/to/data -Z single_node -l /path/to/data/logfile

# Check status
gs_ctl status -D /path/to/data

# Connect with client
gsql -d postgres -p 5432
```

### Stop Database

```bash
gs_ctl stop -D /path/to/data
```

## Testing

### Regression Tests

```bash
# Run regression test suite
cd src/test/regress
make installcheck

# Run specific test
make installcheck TESTS=test_name
```

### Cascades Optimizer Testing

Enable Cascades optimizer in PostgreSQL session:
```sql
-- Enable Cascades optimizer
SET enable_cascades = ON;

-- View optimizer debug output
SET client_min_messages = WARNING;  -- Shows debug messages
```

Debug output format:
```
WARNING: [PLANNER ENTRY] quals type=...
WARNING: [DEBUG] distribute_qual_to_rels: ...
WARNING: [EXEC_SIMPLE_QUERY] Entry: query_string=...
```

This is normal diagnostic output from the cascades branch.

## Important Build Configuration

### CMake Definitions

The Cascades optimizer requires these compile definitions (set in `src/gausskernel/optimizer/cascades/CMakeLists.txt`):

```cmake
target_compile_definitions(cascadesoptimizer PRIVATE PGXC USE_SPQ)
target_compile_definitions(cascades_planner PRIVATE PGXC USE_SPQ)
```

- `PGXC`: Multi-node configuration support
- `USE_SPQ`: Shared Plan Query (required for node type compatibility with base openGauss)

**Critical**: Without `USE_SPQ`, node type enums mismatch and cause runtime errors like "unrecognized node type: 101".

### GCC Version Detection

The build system auto-detects GCC version in `build/script/utils/common.sh`:

```bash
if [ -d "$BUILD_TOOLS_PATH/gcc10.3" ]; then
    gcc_version="10.3"
else
    gcc_version="7.3"
fi
```

Ensure binarylibs contains the matching GCC toolchain (gcc10.3 for this build).

## Special Libraries

### libonnx_wrapper

The cascades branch references `libonnx_wrapper` for AI features. If missing from binarylibs:

```bash
# Create stub library
cd tmp_build/lib
cat > onnx_wrapper.cpp << 'EOF'
extern "C" {
    void* ONNXEnvCreate() { return nullptr; }
    void* ONNXLoadModel(void*, const char*, const char*) { return nullptr; }
    void ONNXUnloadModel(void*, void*) {}
    int ONNXEmbeddingInferBatch(void*, char**, unsigned long, int*) { return -1; }
}
EOF
g++ -shared -fPIC -o libonnx_wrapper.so onnx_wrapper.cpp
```

## Key Files for Cascades Development

| Component | File Path |
|-----------|-----------|
| Optimizer entry | `optimizer/cascades/cascades_optimizer.cpp` |
| Query translation | `translator/query_to_expression.cpp` |
| Plan generation | `translator/expression_to_plan.cpp` |
| Predicate pushdown | `rule/predicate_push_down_rule.cpp` |
| Column pruning | `rule/column_prune_rule.cpp` |
| Filter impl | `rule/filter_impl_rule.cpp` |
| Scan impl | `rule/scan_impl_rule.cpp` |
| Join impl | `rule/join_impl_rule.cpp` |
| Logical operators | `operator/operators/logical_*.h` |
| Physical operators | `operator/operators/physical_*.h` |

## Known Issues (Cascades Branch)

1. **Memory Requirements**: Linking `gaussdb` binary requires 8GB+ RAM+Swap due to LLVM libraries
2. **AI Features**: libonnx_wrapper may need stub implementation if not in binarylibs
3. **Debug Output**: Cascades optimizer emits verbose WARNING messages - this is intentional diagnostic output
4. **PhysicalFilter**: Not all execution paths fully support PhysicalFilter nodes - some complex queries may fail
5. **Index Scan Cost Bug**: IndexScanImplRule triggers assertion failure in costsize.cpp line 1492 for small tables. Error: "The number of pages in table is larger than total_pages when estimating the number of pages actually fetched."
   - **Impact**: Multi-table joins fail when IndexScanImplRule is applied
   - **Workaround**: Currently no workaround as `enable_indexscan=OFF` is not respected by cascades optimizer
   - **Status**: Bug in cost estimation integration, needs fix in IndexScanImplRule or cost model
6. **Multi-Table JOIN Support**: 
   - **2-table JOINs**: ✅ Working correctly (tested with HashJoin and NestLoop)
   - **3+ table JOINs**: ❌ Currently blocked by IndexScanImplRule cost estimation bug
   - **Architecture Support**: Cascades optimizer architecture fully supports multi-way joins with join reordering (JoinCommutativity and JoinAssociativity rules exist)
   - **Join Enumeration**: Can generate multiple join orders, but currently fails during physical implementation phase

## Cascades Optimizer Deep Dive

### Core Architecture

The Cascades optimizer implements the Columbia query optimizer framework with a memo-based search strategy. It operates in four distinct phases:

#### Phase 1: Logical Rewrite
- Applies transformation rules to the input expression tree
- Rules: predicate pushdown, column pruning, join reordering, subquery unnesting
- Operates on `OptExpression` trees before memo construction
- Single-pass or iterative until fixed point

#### Phase 2: Memo Initialization
- Converts `OptExpression` tree into `Group`/`GroupExpression` structure
- Hash-based duplicate detection ensures each logical expression appears once
- Automatically merges equivalent groups during insertion

#### Phase 3: Exploration
- Task-driven optimization using a stack-based scheduler
- Generates physical alternatives via implementation rules
- Computes costs and tracks best plans per required property
- Prunes search space using cost lower bounds

#### Phase 4: Plan Extraction
- Recursively builds the best physical plan from memo
- Converts `OptExpression` back to PostgreSQL `Plan` nodes
- Preserves cost and cardinality estimates

---

### Data Structures

#### Memo (`memo/memo.h`)

The central data structure storing all equivalent expressions compactly.

**Key responsibilities:**
- **Duplicate Detection:** Uses `std::unordered_multimap<size_t, GroupExpression*>` for O(1) lookup
- **Group Management:** Maintains vector of all groups, assigns unique IDs
- **Group Merging:** Automatically merges groups when equivalent expressions are inserted into different groups
- **Best Plan Tracking:** Stores best expression per `(Group, PhysicalPropertySet)` pair

**Important methods:**
```cpp
// Initialize memo from expression tree
GroupExpression* init(OptExpression* expression);

// Insert expression, returns (is_new, group_expression)
std::pair<bool, GroupExpression*> insert_group_expression(
    GroupExpression* expr, Group* target_group = nullptr);

// Insert transformation result (reuses input groups)
std::pair<bool, GroupExpression*> insert_transform_result(
    GroupExpression* source_expr, OptExpression* transformed_expr);

// Extract best plan for required properties
OptExpression* extract_best_plan(const PhysicalPropertySet& required_property);
```

**Memory management:** All allocations use a dedicated `MemoryContext` for automatic cleanup.

---

#### Group (`memo/group.h`)

Represents a set of logically equivalent expressions.

**Contents:**
- `std::vector<GroupExpression*> logical_expressions_` - Different logical ways to compute result
- `std::vector<GroupExpression*> physical_expressions_` - Different physical implementations
- `LogicalProperty* logical_property_` - Shared logical property (output schema, etc.)
- `Statistics* statistics_` - Cardinality and column statistics
- `std::unordered_map<PhysicalPropertySet, std::pair<double, GroupExpression*>> lowest_cost_expressions_` - Best plan per property

**Key methods:**
```cpp
// Track best expression for a property
void set_best_expression(GroupExpression* expr, double cost,
                        const PhysicalPropertySet& property);

// Retrieve best expression
GroupExpression* get_best_expression(const PhysicalPropertySet& property) const;

// Cost lower bound for pruning
double get_cost_lower_bound(const PhysicalPropertySet& property) const;

// Merge another group into this one
void merge_group(Group* other);
```

**Exploration state:** Tracks whether transformation rules have been applied via `is_explored_` flag.

---

#### GroupExpression (`memo/group_expression.h`)

A node in the memo structure: an operator with references to child groups (not child expressions).

**Structure:**
```cpp
class GroupExpression {
    OptExprOp* op_;                    // The operator
    Group* group_;                     // Parent group (backpointer)
    std::vector<Group*> input_groups_; // Child groups (not expressions!)
    
    // Explored rules tracking
    std::vector<RuleType> explored_rules_;
    
    // Cost and property per required property
    std::unordered_map<PhysicalPropertySet, double> costs_;
    std::unordered_map<PhysicalPropertySet, PhysicalPropertySet> output_properties_;
};
```

**Key insight:** By referencing child *groups* instead of child *expressions*, the memo compactly represents all combinations without exponential explosion.

---

#### OptExpression (`memo/opt_expression.h`)

Tree-based expression representation used *before* memo construction and *after* best plan extraction.

**Structure:**
```cpp
class OptExpression {
    OptExprOp* op_;                          // The operator
    std::vector<OptExpression*> inputs_;     // Child expressions
};
```

**Usage:**
- Input to optimizer: Query translator produces `OptExpression` tree
- During optimization: Transformed into `Group`/`GroupExpression` in memo
- Output from optimizer: Extracted as `OptExpression` tree, then converted to `Plan` nodes

---

### Rule System

Rules are divided into two categories:

#### Transformation Rules (Logical → Logical)

Rewrite queries into equivalent but potentially cheaper forms.

| Rule Type | Description | Example |
|-----------|-------------|---------|
| `TF_PREDICATE_PUSH_DOWN` | Push filters closer to scans | `Filter(Join(A,B))` → `Join(Filter(A), B)` |
| `TF_COLUMN_PRUNE` | Eliminate unused columns | `Project(a,b from Scan(a,b,c,d))` → `Scan(a,b)` |
| `TF_JOIN_COMMUTATIVITY` | Reorder join operands | `Join(A,B)` → `Join(B,A)` |
| `TF_JOIN_ASSOCIATIVITY` | Regroup join trees | `Join(Join(A,B),C)` → `Join(A,Join(B,C))` |
| `TF_FILTER_MERGE` | Combine adjacent filters | `Filter(Filter(X))` → `Filter(X)` |
| `TF_OUTER_JOIN_SIMPLIFY` | Convert outer to inner join when safe | `LeftJoin(A,B) WHERE B.x IS NOT NULL` → `InnerJoin(A,B)` |

**Predicate Push Down Rule Deep Dive:**

From `rule/predicate_push_down_rule.cpp`:

```cpp
// Case 1: Push through INNER JOIN - can push to both sides
if (join_type == JOIN_INNER) {
    LogicalFilter* left_filter = NEW_EX(LogicalFilter);
    left_filter->set_qual_list(qual_list);
    
    LogicalFilter* right_filter = NEW_EX(LogicalFilter);
    right_filter->set_qual_list(qual_list);
    
    OptExpression* left_filtered = OptExpression::create(left_filter, left_child);
    OptExpression* right_filtered = OptExpression::create(right_filter, right_child);
    
    OptExpression* new_expr = OptExpression::create(new_join, left_filtered, right_filtered);
    result.push_back(new_expr);
}

// Case 2: Push to scan - merge with existing predicates
else if (child_op->get_type() == OptExprOpType::LOGICAL_SCAN) {
    LogicalScan* scan = static_cast<LogicalScan*>(child_op->clone());
    List* existing_quals = scan->get_qual_list();
    
    if (existing_quals != nullptr) {
        // Merge qual lists
        List* merged_quals = existing_quals;
        ListCell* cell;
        foreach(cell, qual_list) {
            merged_quals = lappend(merged_quals, lfirst(cell));
        }
        scan->set_qual_list(merged_quals);
    } else {
        scan->set_qual_list(qual_list);
    }
    
    // Return scan directly (filter absorbed into scan)
    OptExpression* scan_expr = OptExpression::create(scan);
    result.push_back(scan_expr);
}
```

**Important:** Predicates are stored as PostgreSQL `List*` of `RestrictInfo*`, allowing multiple predicates per operator.

---

#### Implementation Rules (Logical → Physical)

Convert logical operators into executable physical operators.

| Rule Type | Logical Operator | Physical Operator(s) |
|-----------|------------------|----------------------|
| `IMP_SCAN` | `LogicalScan` | `PhysicalSeqScan` |
| `IMP_INDEX_SCAN` | `LogicalScan` | `PhysicalIndexScan` (if applicable) |
| `IMP_FILTER` | `LogicalFilter` | `PhysicalFilter` |
| `IMP_PROJECT` | `LogicalProject` | `PhysicalProject` |
| `IMP_HASH_JOIN` | `LogicalJoin` | `PhysicalHashJoin` |
| `IMP_MERGE_JOIN` | `LogicalJoin` | `PhysicalMergeJoin` |
| `IMP_NESTLOOP_JOIN` | `LogicalJoin` | `PhysicalNestLoop` |
| `IMP_HASH_AGG` | `LogicalAggregate` | `PhysicalHashAgg` |
| `IMP_SORT` | `LogicalSort` | `PhysicalSort` |
| `IMP_LIMIT` | `LogicalLimit` | `PhysicalLimit` |

**Scan Implementation Rule Example:**

From `rule/scan_impl_rule.cpp`:

```cpp
std::vector<OptExpression*> ScanImplRule::transform(
    OptExpression* input, OptimizerContext* context) {
    
    LogicalScan* logical_scan = static_cast<LogicalScan*>(input->get_op());
    
    // Restore predicates to baserestrictinfo for path generation
    List* qual_list = logical_scan->get_qual_list();
    if (qual_list != nullptr) {
        // Merge into RelOptInfo's baserestrictinfo
        rte->baserestrictinfo = list_concat(rte->baserestrictinfo, qual_list);
    }
    
    // Generate scan path using PostgreSQL's path infrastructure
    Path* scan_path = create_seqscan_path(context->root(), rte, ...);
    
    // Create physical operator
    PhysicalSeqScan* physical_scan = NEW_EX(PhysicalSeqScan);
    physical_scan->set_table_id(logical_scan->get_table_id());
    physical_scan->set_path(scan_path);  // Attach cost info
    
    return {OptExpression::create(physical_scan)};
}
```

**Cost Integration:** Physical operators reference PostgreSQL `Path` structures that contain cost estimates computed by the existing cost model.

---

### Task Scheduling System

The optimizer uses a **stack-based task scheduler** to manage the search process. Tasks are pushed onto a stack and executed LIFO (last in, first out).

#### Task Types

**1. ExploreGroupTask** (`task/explore_group_task.h`)

Ensures all transformation rules are applied to a group.

```cpp
void ExploreGroupTask::execute() {
    if (group_->is_explored()) return;  // Already done
    
    // Push OptimizeExpressionTask for each logical expression
    for (auto* expr : group_->get_logical_expressions()) {
        push_task(new OptimizeExpressionTask(context_, expr, true));
    }
    
    group_->set_explored(true);
}
```

---

**2. OptimizeExpressionTask** (`task/optimize_expression_task.h`)

Applies rules to a single expression.

```cpp
void OptimizeExpressionTask::execute() {
    // Get applicable rules (filtered by pattern, not already explored)
    std::vector<Rule*> valid_rules = get_valid_rules();
    
    // Push tasks in reverse order (LIFO execution)
    
    // 1. Explore child groups first (pushed last, executed first)
    for (Group* child_group : expression_->input_groups()) {
        push_task(new ExploreGroupTask(context_, child_group));
    }
    
    // 2. Derive statistics
    push_task(new DeriveStatsTask(context_, expression_));
    
    // 3. Apply rules (pushed first, executed after children explored)
    for (Rule* rule : valid_rules) {
        push_task(new ApplyRuleTask(context_, expression_, rule));
    }
}
```

**Execution order:** Children explored → Stats derived → Rules applied

---

**3. ApplyRuleTask** (`task/apply_rule_task.h`)

Applies a single rule to an expression.

```cpp
void ApplyRuleTask::execute() {
    // Check rule applicability
    if (!rule_->check(expression_, context_)) return;
    
    // Transform: returns vector of alternative expressions
    std::vector<OptExpression*> results = rule_->transform(
        build_opt_expression(expression_), context_);
    
    // Insert each result into memo
    for (OptExpression* result : results) {
        auto [is_new, new_gexpr] = memo->insert_transform_result(expression_, result);
        
        if (is_new && new_gexpr->is_physical()) {
            // Trigger costing for new physical expression
            push_task(new EnforceAndCostTask(context_, new_gexpr->group(), 
                                            required_property_));
        }
    }
}
```

---

**4. EnforceAndCostTask** (`task/enforce_and_cost_task.h`)

Computes costs for physical expressions and tracks the best plan.

```cpp
void EnforceAndCostTask::execute() {
    for (GroupExpression* phys_expr : group_->get_physical_expressions()) {
        // Compute cost recursively
        double total_cost = 0.0;
        
        // 1. Cost of child plans
        for (size_t i = 0; i < phys_expr->arity(); ++i) {
            Group* child_group = phys_expr->input_group(i);
            PhysicalPropertySet child_required = derive_child_property(...);
            
            GroupExpression* best_child = child_group->get_best_expression(child_required);
            if (!best_child) continue;  // Child not optimized yet
            
            total_cost += best_child->get_cost(child_required);
        }
        
        // 2. Local operator cost (from Path structure)
        total_cost += compute_local_cost(phys_expr);
        
        // 3. Property enforcement cost (e.g., sort)
        total_cost += compute_enforcement_cost(...);
        
        // Update best expression if cheaper
        if (total_cost < group_->get_best_expression_cost(required_property_)) {
            group_->set_best_expression(phys_expr, total_cost, required_property_);
        }
    }
}
```

---

**5. DeriveStatsTask** (`task/derive_stats_task.h`)

Computes cardinality and statistics for a group.

```cpp
void DeriveStatsTask::execute() {
    GroupExpression* expr = group_->get_first_logical_expression();
    
    // Bottom-up: get child statistics first
    std::vector<Statistics*> child_stats;
    for (Group* child : expr->input_groups()) {
        child_stats.push_back(child->get_statistics());
    }
    
    // Derive this expression's statistics
    Statistics* stats = expr->op()->derive_statistics(child_stats, context_);
    
    // Example: Scan with filter
    if (auto* scan = dynamic_cast<LogicalScan*>(expr->op())) {
        double base_rows = get_table_row_count(scan->table_id());
        double selectivity = estimate_selectivity(scan->get_qual_list());
        stats->set_row_count(base_rows * selectivity);
    }
    
    group_->set_statistics(stats);
}
```

**Selectivity Estimation:** Uses PostgreSQL's selectivity functions (`clauselist_selectivity`) for predicate selectivity.

---

### Property System

Physical properties describe execution characteristics that must be satisfied.

#### PhysicalPropertySet (`property/physical_property_set.h`)

Contains:
- **DistributionProperty:** Data distribution (ANY, BROADCAST, HASH, SINGLE)
- **SortProperty:** Sort order (list of `(column_id, ascending)` pairs)

```cpp
class PhysicalPropertySet {
    DistributionProperty distribution_;
    SortProperty sort_;
    
    bool satisfies(const PhysicalPropertySet& required) const;
};
```

#### Property Enforcement

When a child doesn't satisfy required properties, **enforcer operators** are inserted:

```cpp
// Example: Sort enforcer
if (!child_output_prop.sort().satisfies(required_prop.sort())) {
    PhysicalSort* sort_enforcer = create_sort_enforcer(required_prop.sort());
    // Insert sort between parent and child
}
```

**Cost Impact:** Enforcers add cost (e.g., sort cost), affecting plan selection.

---

### Cost Model

Costs are computed using PostgreSQL's existing cost model.

**Key cost components:**

1. **Scan Cost** (`cost_seqscan`, `cost_index`):
   ```
   total_cost = startup_cost + run_cost
   run_cost = cpu_tuple_cost * rows + disk_page_cost * pages
   ```

2. **Join Cost** (`cost_hashjoin`, `cost_nestloop`, `cost_mergejoin`):
   ```
   hash_join_cost = build_cost(outer) + probe_cost(inner) + cpu_cost
   nest_loop_cost = outer_cost + inner_cost * outer_rows + cpu_cost
   ```

3. **Aggregation Cost** (`cost_agg`):
   ```
   agg_cost = input_cost + cpu_operator_cost * rows * agg_functions
   ```

**Selectivity-Adjusted Cardinality:**
```cpp
// From DeriveStatsTask
double base_rows = 4.0;  // test_table1
double selectivity = 0.333333;  // "value > 150" on uniform distribution
double output_rows = base_rows * selectivity;  // 1.33 → 1
```

**Observed costs in testing:**
- SeqScan (4 rows, no filter): 11.79
- SeqScan (4 rows, filter selectivity 0.33): 13.19
- Project (passthrough): 0.01
- HashJoin (4 rows × 4 rows): 25.02

---

### Query Translation

#### Query to Expression (`translator/query_to_expression.cpp`)

Converts PostgreSQL `Query` structure to Cascades `OptExpression`.

**Key steps:**

1. **Translate FROM clause:**
   ```cpp
   OptExpression* translate_from(Query* query) {
       if (single table) return translate_scan(rte);
       if (join) return translate_join(query->jointree);
       if (subquery) return translate_subquery_scan(rte);
   }
   ```

2. **Translate WHERE clause:**
   ```cpp
   // Predicates are already distributed to baserestrictinfo by standard planner
   // For scans: create LogicalScan with qual_list from baserestrictinfo
   LogicalScan* scan = NEW_EX(LogicalScan);
   scan->set_qual_list(rte->baserestrictinfo);
   ```

3. **Translate SELECT clause:**
   ```cpp
   // Wrap in LogicalProject
   LogicalProject* project = NEW_EX(LogicalProject);
   project->set_target_list(query->targetList);
   return OptExpression::create(project, from_expr);
   ```

4. **Translate JOIN:**
   ```cpp
   LogicalJoin* join = NEW_EX(LogicalInnerJoin);
   join->set_join_type(JOIN_INNER);
   join->set_join_quals(join_conditions);
   return OptExpression::create(join, left_expr, right_expr);
   ```

**Predicate handling:** The translator relies on PostgreSQL's `deconstruct_jointree` to distribute predicates to base relations. This is why you see `[DEBUG] distribute_qual_to_rels` messages during query processing.

---

#### Expression to Plan (`translator/expression_to_plan.cpp`)

Converts optimized `OptExpression` back to PostgreSQL `Plan` nodes.

**Dispatch by operator type:**

```cpp
Plan* ExprToPlanConverter::convert(OptExpression* expr) {
    switch (expr->get_op()->get_type()) {
        case OptExprOpType::PHYSICAL_SCAN:
            return translate_scan(static_cast<PhysicalSeqScan*>(expr->get_op()));
        
        case OptExprOpType::PHYSICAL_HASH_JOIN:
            return translate_hash_join(static_cast<PhysicalHashJoin*>(expr->get_op()),
                                      convert(expr->input_at(0)),
                                      convert(expr->input_at(1)));
        
        case OptExprOpType::PHYSICAL_PROJECT:
            return translate_project(static_cast<PhysicalProject*>(expr->get_op()),
                                    convert(expr->input_at(0)));
        // ... other operators
    }
}
```

**Plan node creation:**

```cpp
Plan* translate_scan(PhysicalSeqScan* phys_scan) {
    SeqScan* plan = makeNode(SeqScan);
    plan->scan.plan.targetlist = build_target_list(...);
    plan->scan.plan.qual = NIL;  // Predicates in scan path
    plan->scan.scanrelid = phys_scan->get_table_id();
    
    // Attach cost from path
    Path* path = phys_scan->get_path();
    plan->scan.plan.startup_cost = path->startup_cost;
    plan->scan.plan.total_cost = path->total_cost;
    plan->scan.plan.plan_rows = path->rows;
    
    return (Plan*)plan;
}
```

---

### Optimization Flow Example

**Query:** `SELECT * FROM test_table1 WHERE value > 150;`

**Step-by-step execution:**

1. **Translation** (`query_to_expression.cpp`):
   ```
   LogicalProject(cols=3)
     └─ LogicalScan(table=1, qual_list=[value > 150])
   ```

2. **Logical Rewrite**:
   - Apply ColumnPruneRule → no change (all columns needed)
   - Predicate already in scan → no further pushdown

3. **Memo Init**:
   ```
   Group 0: {LogicalScan(table=1, qual_list=1)}
   Group 1: {LogicalProject(cols=3) → Group 0}
   ```

4. **Exploration** (task sequence):
   ```
   ExploreGroupTask(Group 1)
     ├─ OptimizeExpressionTask(LogicalProject)
     │    ├─ ExploreGroupTask(Group 0)
     │    │    └─ OptimizeExpressionTask(LogicalScan)
     │    │         ├─ DeriveStatsTask → rows=1 (selectivity=0.33)
     │    │         ├─ ApplyRuleTask(IMP_INDEX_SCAN) → 0 results
     │    │         └─ ApplyRuleTask(IMP_SCAN) → PhysicalSeqScan
     │    │              └─ EnforceAndCostTask → cost=13.19
     │    ├─ DeriveStatsTask → rows=1
     │    └─ ApplyRuleTask(IMP_PROJECT) → PhysicalProject
     │         └─ EnforceAndCostTask → cost=13.20 (13.19 + 0.01)
   ```

5. **Best Plan**:
   ```
   Group 1: PhysicalProject [cost=13.20]
   Group 0: PhysicalSeqScan [cost=13.19]
   ```

6. **Extraction**:
   ```
   PhysicalProject [rows=1] [cost=13.20]
     └─ PhysicalSeqScan(table_id=1) [rows=1] [cost=13.19]
   ```

7. **Plan Conversion**:
   ```c
   ProjectionPath {
       subpath: SeqScan on test_table1
                Filter: (value > 150)
                Cost: 13.19, Rows: 1
       Cost: 13.20, Rows: 1
   }
   ```

8. **Execution**: Standard PostgreSQL executor runs the plan

---

### Key Design Patterns

#### 1. Visitor Pattern (Rule Application)

Rules use pattern matching to determine applicability:

```cpp
class Rule {
    Pattern* pattern_;  // e.g., Pattern::create(LOGICAL_FILTER, Pattern::any())
    
    bool matches(OptExprOp* op) {
        return pattern_->matches(op);
    }
};
```

#### 2. Memoization (Duplicate Detection)

Hash-based duplicate detection avoids redundant work:

```cpp
// Hash function for GroupExpression
size_t hash() const {
    size_t h = std::hash<int>()(op_->get_type());
    for (Group* child : input_groups_) {
        h ^= std::hash<int>()(child->id()) + 0x9e3779b9 + (h << 6) + (h >> 2);
    }
    return h;
}

// Lookup before insertion
GroupExpression* existing = memo->get_existing_expression(new_expr);
if (existing) return existing;  // Already have this
```

#### 3. Dynamic Programming (Best Plan Tracking)

Optimal substructure: best plan for a group is independent of parent:

```cpp
// Best plan for (Group, Property) is cached
GroupExpression* Group::get_best_expression(const PhysicalPropertySet& prop) {
    auto it = lowest_cost_expressions_.find(prop);
    return (it != lowest_cost_expressions_.end()) ? it->second.second : nullptr;
}
```

#### 4. Lazy Evaluation (On-Demand Costing)

Physical plans are costed only when needed:

```cpp
// EnforceAndCostTask pushed only when:
// 1. New physical expression inserted
// 2. Required property changes
if (is_new && new_gexpr->is_physical()) {
    push_task(new EnforceAndCostTask(...));
}
```

---

### Testing and Debugging

#### Enable Cascades Optimizer

```sql
SET enable_cascades_optimizer = ON;  -- Note: 'optimizer' suffix
SET client_min_messages = WARNING;    -- Show debug output
```

#### Interpret Debug Output

```
WARNING:  [CASCADES] ENTRY: quals type=314
```
- Entry point to optimizer, quals type is PostgreSQL node type

```
WARNING:  [CASCADES] translate_scan: table_id=1, output_columns=3
```
- Translating RangeTblEntry to LogicalScan

```
WARNING:  [CASCADES] Phase 1: logical_rewrite
```
- Starting transformation rule application

```
WARNING:  [CASCADES] OptimizeExpressionTask: found 2 valid rules
```
- Two applicable rules for this expression

```
WARNING:  [CASCADES] EnforceAndCostTask: expr=PhysicalSeqScan total_cost=13.19
```
- Computed cost for physical operator

```
WARNING:  [CASCADES] extract_best_plan: optimized plan tree:
PhysicalProject [rows=1] [cost=13.200000]
  child[0]:
    PhysicalSeqScan(table_id=1) [rows=1] [cost=13.190000]
```
- Final selected plan with costs

#### Common Issues

**1. Fallback to Standard Planner:**
```
WARNING:  Cascades optimizer: Query conversion failed, falling back to standard planner
```
**Causes:** Unsupported query features (e.g., no FROM clause, CTEs, window functions)

**2. Node Type Mismatch:**
```
ERROR:  unrecognized node type: 101
```
**Cause:** Missing `USE_SPQ` compile definition (see Build Configuration section)

**3. Memory Context Errors:**
```
ERROR:  invalid memory alloc request size
```
**Cause:** Accessing freed memory; check MemoryContext lifecycle

---

### Performance Characteristics

**Strengths:**
- **Extensibility:** Easy to add new rules and operators
- **Search Space Coverage:** Memo compactly represents all alternatives
- **Cost Accuracy:** Leverages PostgreSQL's mature cost model
- **Predicate Pushdown:** Effective at reducing intermediate result sizes

**Current Limitations:**
- **Optimization Overhead:** Higher than traditional planner for simple queries (task overhead)
- **Search Strategy:** Currently exhaustive; no pruning heuristics beyond cost bounds
- **Statistics:** Relies on PostgreSQL statistics; no learning/feedback
- **Complex Queries:** Some features not yet supported (window functions, recursive CTEs)

**Optimization Time Observations:**
- Single-table query: < 1ms overhead
- 2-table join: ~2-5ms overhead
- Complex multi-way joins: Linear in number of join orders explored

---

### Code Statistics

Module breakdown (file count):
- `operator/`: 47 files (logical and physical operators)
- `rule/`: 52 files (transformation and implementation rules)
- `task/`: 18 files (task scheduling system)
- `memo/`: 10 files (core data structures)
- `property/`: 10 files (physical properties)
- `statistics/`: 6 files (cardinality estimation)
- `translator/`: 6 files (query↔expression conversion)
- `cost/`: 4 files (cost model integration)
- `optimizer/`: 4 files (main optimizer entry point)

Total: ~150+ source files implementing a production-grade Cascades optimizer.

---

## Documentation

- Official docs: https://docs.opengauss.org/
- Build guide: https://opengauss.org/zh/docs/latest/docs/CompilationGuide/
- Cascades optimizer summary: `docs/cascades_optimizer_summary.md`
- Cascades test report: `CASCADES_TEST_REPORT.md`
- Installation summary: `INSTALL_SUMMARY.md`
- Startup guide: `STARTUP_GUIDE.md`
