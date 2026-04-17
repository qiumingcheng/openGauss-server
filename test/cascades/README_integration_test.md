# Cascades Optimizer 集成测试用例说明

## GUC 设置

Cascades 优化器默认是关闭的，需要先启用：

```sql
SET enable_cascades_optimizer = on;
SHOW enable_cascades_optimizer;  -- 验证设置
```

其他相关的 GUC 设置（可选）：

```sql
-- 启用/禁用特定连接方式
SET enable_seqscan = on;
SET enable_indexscan = on;
SET enable_hashjoin = on;
SET enable_mergejoin = on;
SET enable_nestloop = on;
```

## 测试数据表结构

### test_user
```sql
CREATE TABLE test_user (
    id SERIAL PRIMARY KEY,
    name VARCHAR(50),
    age INT,
    email VARCHAR(100),
    create_time TIMESTAMP DEFAULT NOW()
);
```

### user_order
```sql
CREATE TABLE user_order (
    order_id SERIAL PRIMARY KEY,
    user_id INT NOT NULL,
    order_no VARCHAR(32) NOT NULL,
    order_amount NUMERIC(10,2) NOT NULL,
    order_status VARCHAR(10) DEFAULT 'PENDING',
    create_time TIMESTAMP DEFAULT NOW(),
    FOREIGN KEY (user_id) REFERENCES test_user(id)
);
```

## 规则覆盖矩阵

| 测试用例 | 覆盖规则 | 说明 |
|---------|---------|------|
| 0.1-0.3 | EXPLAIN | 执行计划显示（验证 Cascades 与 EXPLAIN 集成，显示 Cost） |
| 1.1-1.2 | TF_JOIN_COMMUTATIVITY + EXPLAIN | 连接交换律：A ⋈ B → B ⋈ A |
| 2.1 | TF_JOIN_ASSOCIATIVITY + EXPLAIN | 连接结合律：(A ⋈ B) ⋈ C → A ⋈ (B ⋈ C) |
| 3.1-3.2 | TF_PREDICATE_PUSH_DOWN + EXPLAIN | 谓词下推到 Join 之前 |
| 4.1 | TF_FILTER_MERGE + EXPLAIN | 多个 Filter 合并为 A AND B |
| 5.1-5.2 | TF_COLUMN_PRUNE + EXPLAIN | 只选择需要的列 |
| 6.1-6.2 | TF_OUTER_JOIN_SIMPLIFY + EXPLAIN | 外连接简化为内连接 |
| 7.1-7.2 | TF_SORT_ELIMINATION + EXPLAIN | 消除不必要的排序 |
| 8.1-8.2 | TF_AGGREGATION_SIMPLIFY + EXPLAIN | 聚合简化 |
| 9.1-9.2 | TF_CONSTANT_FOLD + EXPLAIN | 常量表达式折叠 |
| 10.1 | TF_CROSS_JOIN_ELIMINATE + EXPLAIN | CROSS JOIN 转 INNER JOIN |
| 11.1 | TF_DUPLICATE_JOIN_ELIMINATION + EXPLAIN | 消除重复自连接 |
| 12.1-12.5 | IMP_* + EXPLAIN | 物理实现规则 |
| 13.1-13.2 | 多规则组合 + EXPLAIN | 综合测试 |

## 测试结构说明

每个测试用例包含两部分：
1. **EXPLAIN** - 显示执行计划和代价（Cost）
2. **SELECT** - 实际执行查询

```sql
-- 先查看执行计划和代价
EXPLAIN
SELECT ...;

-- 再实际执行
SELECT ...;
```

## 详细测试说明

### 0. EXPLAIN 执行计划测试

**原理**: EXPLAIN 命令通过 `pg_plan_query` 调用 planner，当 `enable_cascades_optimizer = on` 时，会自动使用 `cascades_planner` 生成执行计划。

**执行流程**:
```
ExplainQuery → ExplainOneQuery → pg_plan_query → cascades_planner
```

**测试验证**:
```sql
-- 基本 EXPLAIN（显示代价）
EXPLAIN
SELECT u.name, o.order_no FROM test_user u JOIN user_order o ON u.id = o.user_id WHERE u.age > 25;

-- VERBOSE 模式显示详细信息
EXPLAIN (VERBOSE)
SELECT u.name, o.order_no FROM test_user u JOIN user_order o ON u.id = o.user_id WHERE u.age > 25;
```

**预期输出**: 显示 Cascades 优化器生成的执行计划，包括：
- 计划节点类型（PhysicalScan、PhysicalHashJoin、PhysicalSort 等）
- 估计代价（start-up cost 和 total cost）
- 估计行数
- 其他详细信息

**代价说明**:
- `start-up cost` - 开始返回第一行前的代价
- `total cost` - 返回所有行的总代价
- 格式：`cost=启动代价..总代价`

### 1. TF_JOIN_COMMUTATIVITY (连接交换律)

**原理**: 对于内连接，A ⋈ B 等价于 B ⋈ A

**测试验证**:
```sql
-- 原始：user ⋈ order
SELECT u.name, o.order_no FROM test_user u JOIN user_order o ON u.id = o.user_id;

-- 优化后：order ⋈ user (小表驱动大表)
```

### 2. TF_JOIN_ASSOCIATIVITY (连接结合律)

**原理**: (A ⋈ B) ⋈ C 等价于 A ⋈ (B ⋈ C)

**测试验证**:
```sql
-- 原始：(u1 ⋈ o) ⋈ u2
-- 优化后：u1 ⋈ (o ⋈ u2) 或 (u1 ⋈ u2) ⋈ o
```

### 3. TF_PREDICATE_PUSH_DOWN (谓词下推)

**原理**: 将过滤条件下推到数据源，减少中间结果集

**测试验证**:
```sql
-- 原始：先 JOIN 后过滤
SELECT * FROM u JOIN o ON u.id = o.user_id WHERE u.age > 25;

-- 优化后：先过滤 age > 25，再 JOIN
```

### 4. TF_FILTER_MERGE (过滤器合并)

**原理**: 多个连续 Filter 可合并为一个 Filter(A AND B AND C)

**测试验证**:
```sql
-- 原始：多层 Filter
-- 优化后：单层 Filter(age > 20 AND name LIKE 'test%' AND ...)
```

### 5. TF_COLUMN_PRUNE (列剪枝)

**原理**: 只扫描查询需要的列

**测试验证**:
```sql
-- 原始：扫描所有列
SELECT u.id, o.order_no FROM u JOIN o ON u.id = o.user_id;

-- 优化后：只扫描 id 和 order_no 列
```

### 6. TF_OUTER_JOIN_SIMPLIFY (外连接简化)

**原理**: LEFT JOIN + IS NOT NULL 可简化为 INNER JOIN

**测试验证**:
```sql
-- 原始：LEFT JOIN + WHERE o.order_no IS NOT NULL
-- 优化后：INNER JOIN
```

### 7. TF_SORT_ELIMINATION (排序消除)

**原理**: 如果数据已经有序，消除多余的排序操作

**测试验证**:
```sql
-- GROUP BY u.id 已产生有序输出
SELECT u.id, COUNT(*) FROM u JOIN o ON u.id = o.user_id GROUP BY u.id ORDER BY u.id;
-- 优化后：消除 ORDER BY
```

### 8. TF_AGGREGATION_SIMPLIFY (聚合简化)

**原理**: 简化聚合表达式，优化聚合计算

### 9. TF_CONSTANT_FOLD (常量折叠)

**原理**: 在编译时计算常量表达式

**测试验证**:
```sql
-- 原始：WHERE order_amount > 100 * 2
-- 优化后：WHERE order_amount > 200
```

### 10. TF_CROSS_JOIN_ELIMINATE (交叉连接消除)

**原理**: CROSS JOIN + WHERE 条件 = INNER JOIN

**测试验证**:
```sql
-- 原始：CROSS JOIN WHERE u.id = o.user_id
-- 优化后：INNER JOIN ON u.id = o.user_id
```

### 11. TF_DUPLICATE_JOIN_ELIMINATION (重复连接消除)

**原理**: 相同表相同条件的自连接可消除

**测试验证**:
```sql
-- 原始：a JOIN b ON a.id = b.id (相同表相同 ID)
-- 优化后：直接使用 a
```

### 12. 物理实现规则 (IMP_*)

| 规则 | 说明 | 适用场景 |
|------|------|---------|
| IMP_HASH_JOIN | Hash Join | 大表等值连接 |
| IMP_MERGE_JOIN | Merge Join | 有序数据连接 |
| IMP_NESTLOOP_JOIN | NestLoop Join | 小表驱动 |
| IMP_HASH_AGG | Hash 聚合 | GROUP BY |
| IMP_SCAN | 扫描方式选择 | 全表/索引扫描 |

## 运行测试

```bash
# 1. 准备测试数据
psql -d testdb -f test/cascades/setup_test_data.sql

# 2. 运行集成测试（包含 EXPLAIN 和实际执行）
psql -d testdb -f test/cascades/integration_test.sql

# 3. 单独运行 EXPLAIN 测试查看执行计划和代价
psql -d testdb -c "SET enable_cascades_optimizer = on; EXPLAIN SELECT * FROM test_user u JOIN user_order o ON u.id = o.user_id WHERE u.age > 25;"

# 4. 使用 EXPLAIN ANALYZE 查看实际执行时间和计划
psql -d testdb -c "SET enable_cascades_optimizer = on; EXPLAIN ANALYZE SELECT * FROM test_user u JOIN user_order o ON u.id = o.user_id WHERE u.age > 25;"
```

## EXPLAIN 选项说明

| 选项 | 说明 |
|------|------|
| `EXPLAIN`（默认） | 显示执行计划和估计代价（cost） |
| `ANALYZE` | 实际执行查询并显示实际时间 |
| `VERBOSE` | 显示详细信息（输出列、表达式等） |
| `COSTS ON/OFF` | 是否显示估计代价（默认 ON） |
| `BUFFERS` | 显示缓冲区使用情况（需要 ANALYZE） |
| `FORMAT TEXT/JSON/XML/YAML` | 指定输出格式 |

**代价格式说明**:
```
Seq Scan on test_user  (cost=0.00..12.50 rows=100 width=50)
                              ↑
                        启动代价..总代价
```
- **启动代价**: 开始返回第一行前的估计代价
- **总代价**: 返回所有行的总估计代价
- **rows**: 估计返回的行数
- **width**: 每行的平均宽度（字节）

## 预期行为验证

对于每个测试用例，可以通过以下方式验证优化效果：

1. **执行计划检查**: 使用 `EXPLAIN` 查看实际执行计划
2. **规则应用追踪**: 添加日志输出规则应用过程
3. **性能对比**: 对比优化前后的执行时间

## 扩展测试

以下场景可作为后续扩展测试：

1. **多表连接**: 添加第三个表进行更复杂的结合律测试
2. **索引覆盖**: 创建索引后验证排序消除
3. **统计信息**: 基于统计信息的 Join 顺序选择
4. **并行执行**: 大数据量下的并行优化
