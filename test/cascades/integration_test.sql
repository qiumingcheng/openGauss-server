-- ============================================================================
-- Cascades Optimizer 集成测试用例
-- 基于 test_user 和 user_order 两个表
-- ============================================================================

-- ============================================================================
-- GUC 设置 - 启用 Cascades 优化器
-- ============================================================================
SET enable_cascades_optimizer = on;

-- 验证 GUC 设置
SHOW enable_cascades_optimizer;

-- ============================================================================
-- 0. EXPLAIN 执行计划测试
-- 验证 EXPLAIN 与 Cascades 优化器集成
-- ============================================================================

-- 测试 0.1: EXPLAIN 基本查询
EXPLAIN
SELECT u.name, o.order_no
FROM test_user u
JOIN user_order o ON u.id = o.user_id
WHERE u.age > 25;

-- 测试 0.2: EXPLAIN VERBOSE 显示详细信息
EXPLAIN (VERBOSE)
SELECT u.name, o.order_no
FROM test_user u
JOIN user_order o ON u.id = o.user_id
WHERE u.age > 25;

-- 测试 0.3: EXPLAIN 复杂查询
EXPLAIN
SELECT
    u.name,
    COUNT(o.order_id) as order_count,
    SUM(o.order_amount) as total_amount
FROM test_user u
JOIN user_order o ON u.id = o.user_id
WHERE u.age > 18
  AND o.order_status IN ('COMPLETED', 'PENDING')
GROUP BY u.id, u.name
HAVING COUNT(o.order_id) > 1
ORDER BY total_amount DESC
LIMIT 10;

-- ============================================================================
-- 1. TF_JOIN_COMMUTATIVITY - 连接交换律测试
-- A ⋈ B = B ⋈ A
-- ============================================================================

-- 测试 1.1: 基本的内连接交换
-- 验证优化器能否将 user ⋈ order 转换为 order ⋈ user
EXPLAIN
SELECT u.name, o.order_no, o.order_amount
FROM test_user u
JOIN user_order o ON u.id = o.user_id
WHERE o.order_status = 'PENDING';

SELECT u.name, o.order_no, o.order_amount
FROM test_user u
JOIN user_order o ON u.id = o.user_id
WHERE o.order_status = 'PENDING';

-- 测试 1.2: 带多个谓词的连接交换
EXPLAIN
SELECT u.name, u.email, o.order_no
FROM test_user u
JOIN user_order o ON u.id = o.user_id
WHERE u.age > 18 AND o.order_amount > 100;

SELECT u.name, u.email, o.order_no
FROM test_user u
JOIN user_order o ON u.id = o.user_id
WHERE u.age > 18 AND o.order_amount > 100;


-- ============================================================================
-- 2. TF_JOIN_ASSOCIATIVITY - 连接结合律测试
-- (A ⋈ B) ⋈ C = A ⋈ (B ⋈ C)
-- ============================================================================

-- 测试 2.1: 三表连接结合律 (需要自连接模拟三表)
-- user ⋈ order ⋈ (再次关联 user 的其他记录)
EXPLAIN
SELECT u1.name as user1, u2.name as user2, o.order_no
FROM test_user u1
JOIN user_order o ON u1.id = o.user_id
JOIN test_user u2 ON o.order_no LIKE '%' || u2.name || '%'
WHERE u1.age > 20;

SELECT u1.name as user1, u2.name as user2, o.order_no
FROM test_user u1
JOIN user_order o ON u1.id = o.user_id
JOIN test_user u2 ON o.order_no LIKE '%' || u2.name || '%'
WHERE u1.age > 20;


-- ============================================================================
-- 3. TF_PREDICATE_PUSH_DOWN - 谓词下推测试
-- 将过滤条件下推到 Join 之前
-- ============================================================================

-- 测试 3.1: 单表谓词下推到 Join 前
EXPLAIN
SELECT u.name, o.order_no
FROM test_user u
JOIN user_order o ON u.id = o.user_id
WHERE u.age > 25 AND o.order_amount > 500;

SELECT u.name, o.order_no
FROM test_user u
JOIN user_order o ON u.id = o.user_id
WHERE u.age > 25 AND o.order_amount > 500;
-- 期望：age > 25 下推到 test_user 扫描前，order_amount > 500 下推到 user_order 扫描前

-- 测试 3.2: 多层嵌套的谓词下推
EXPLAIN
SELECT u.name, o.order_no
FROM (SELECT * FROM test_user WHERE age > 18) u
JOIN (SELECT * FROM user_order WHERE order_amount > 100) o ON u.id = o.user_id
WHERE o.order_status = 'COMPLETED';

SELECT u.name, o.order_no
FROM (SELECT * FROM test_user WHERE age > 18) u
JOIN (SELECT * FROM user_order WHERE order_amount > 100) o ON u.id = o.user_id
WHERE o.order_status = 'COMPLETED';


-- ============================================================================
-- 4. TF_FILTER_MERGE - 过滤器合并测试
-- Filter(A) + Filter(B) = Filter(A AND B)
-- ============================================================================

-- 测试 4.1: 多个 WHERE 条件合并
EXPLAIN
SELECT u.name, o.order_no
FROM test_user u
JOIN user_order o ON u.id = o.user_id
WHERE u.age > 20
  AND u.name LIKE 'test%'
  AND o.order_amount > 100
  AND o.order_status != 'CANCELLED';

SELECT u.name, o.order_no
FROM test_user u
JOIN user_order o ON u.id = o.user_id
WHERE u.age > 20
  AND u.name LIKE 'test%'
  AND o.order_amount > 100
  AND o.order_status != 'CANCELLED';


-- ============================================================================
-- 5. TF_COLUMN_PRUNE - 列剪枝测试
-- 只选择需要的列
-- ============================================================================

-- 测试 5.1: 只选择必要列
EXPLAIN
SELECT u.id, o.order_no
FROM test_user u
JOIN user_order o ON u.id = o.user_id;

SELECT u.id, o.order_no
FROM test_user u
JOIN user_order o ON u.id = o.user_id;
-- 期望：不扫描 name, age, email, create_time 等不需要的列

-- 测试 5.2: COUNT(*) 列剪枝
EXPLAIN
SELECT COUNT(*)
FROM test_user u
JOIN user_order o ON u.id = o.user_id;

SELECT COUNT(*)
FROM test_user u
JOIN user_order o ON u.id = o.user_id;
-- 期望：不需要扫描任何实际数据列


-- ============================================================================
-- 6. TF_OUTER_JOIN_SIMPLIFY - 外连接简化测试
-- 在外连接可转换为内连接时进行简化
-- ============================================================================

-- 测试 6.1: LEFT JOIN 带非空谓词可简化为 INNER JOIN
EXPLAIN
SELECT u.name, o.order_no
FROM test_user u
LEFT JOIN user_order o ON u.id = o.user_id
WHERE o.order_no IS NOT NULL;

SELECT u.name, o.order_no
FROM test_user u
LEFT JOIN user_order o ON u.id = o.user_id
WHERE o.order_no IS NOT NULL;
-- 期望：LEFT JOIN 简化为 INNER JOIN

-- 测试 6.2: RIGHT JOIN 带非空谓词可简化为 INNER JOIN
EXPLAIN
SELECT u.name, o.order_no
FROM test_user u
RIGHT JOIN user_order o ON u.id = o.user_id
WHERE u.id IS NOT NULL;

SELECT u.name, o.order_no
FROM test_user u
RIGHT JOIN user_order o ON u.id = o.user_id
WHERE u.id IS NOT NULL;


-- ============================================================================
-- 7. TF_SORT_ELIMINATION - 排序消除测试
-- 消除不必要的排序操作
-- ============================================================================

-- 测试 7.1: 利用索引顺序消除排序 (需要 user_id 索引)
EXPLAIN
SELECT u.name, o.order_no
FROM test_user u
JOIN user_order o ON u.id = o.user_id
ORDER BY u.id;

SELECT u.name, o.order_no
FROM test_user u
JOIN user_order o ON u.id = o.user_id
ORDER BY u.id;
-- 期望：如果 user_id 是主键/索引，可以消除排序

-- 测试 7.2: 聚合后的排序消除
EXPLAIN
SELECT u.id, COUNT(o.order_id) as order_count
FROM test_user u
JOIN user_order o ON u.id = o.user_id
GROUP BY u.id
ORDER BY u.id;

SELECT u.id, COUNT(o.order_id) as order_count
FROM test_user u
JOIN user_order o ON u.id = o.user_id
GROUP BY u.id
ORDER BY u.id;
-- 期望：GROUP BY 已产生有序输出，可消除 ORDER BY


-- ============================================================================
-- 8. TF_AGGREGATION_SIMPLIFY - 聚合简化测试
-- ============================================================================

-- 测试 8.1: COUNT(*) 简化
EXPLAIN
SELECT u.id, COUNT(*) as order_count
FROM test_user u
JOIN user_order o ON u.id = o.user_id
GROUP BY u.id;

SELECT u.id, COUNT(*) as order_count
FROM test_user u
JOIN user_order o ON u.id = o.user_id
GROUP BY u.id;

-- 测试 8.2: SUM 聚合
EXPLAIN
SELECT u.id, SUM(o.order_amount) as total_amount
FROM test_user u
JOIN user_order o ON u.id = o.user_id
WHERE o.order_status = 'COMPLETED'
GROUP BY u.id;

SELECT u.id, SUM(o.order_amount) as total_amount
FROM test_user u
JOIN user_order o ON u.id = o.user_id
WHERE o.order_status = 'COMPLETED'
GROUP BY u.id;


-- ============================================================================
-- 9. TF_CONSTANT_FOLD - 常量折叠测试
-- 在编译时计算常量表达式
-- ============================================================================

-- 测试 9.1: 常量表达式折叠
EXPLAIN
SELECT u.name, o.order_amount
FROM test_user u
JOIN user_order o ON u.id = o.user_id
WHERE o.order_amount > 100 * 2;

SELECT u.name, o.order_amount
FROM test_user u
JOIN user_order o ON u.id = o.user_id
WHERE o.order_amount > 100 * 2;
-- 期望：100 * 2 折叠为 200

-- 测试 9.2: 布尔常量折叠
EXPLAIN
SELECT u.name
FROM test_user u
WHERE 1 = 1 AND u.age > 25;

SELECT u.name
FROM test_user u
WHERE 1 = 1 AND u.age > 25;
-- 期望：1 = 1 折叠为 TRUE


-- ============================================================================
-- 10. TF_CROSS_JOIN_ELIMINATE - 交叉连接消除测试
-- ============================================================================

-- 测试 10.1: 带 WHERE 条件的 CROSS JOIN 转换为 INNER JOIN
EXPLAIN
SELECT u.name, o.order_no
FROM test_user u
CROSS JOIN user_order o
WHERE u.id = o.user_id AND o.order_amount > 100;

SELECT u.name, o.order_no
FROM test_user u
CROSS JOIN user_order o
WHERE u.id = o.user_id AND o.order_amount > 100;
-- 期望：CROSS JOIN + WHERE 转换为 INNER JOIN


-- ============================================================================
-- 11. TF_DUPLICATE_JOIN_ELIMINATION - 重复连接消除测试
-- ============================================================================

-- 测试 11.1: 自连接消除 (相同表相同条件)
EXPLAIN
SELECT a.name, b.name
FROM test_user a
JOIN test_user b ON a.id = b.id
WHERE a.age > 20;

SELECT a.name, b.name
FROM test_user a
JOIN test_user b ON a.id = b.id
WHERE a.age > 20;
-- 期望：消除冗余的自连接


-- ============================================================================
-- 12. Implementation Rules - 物理实现规则测试
-- ============================================================================

-- 测试 12.1: IMP_HASH_JOIN - Hash Join 实现
-- 大表连接，期望使用 Hash Join
EXPLAIN
SELECT /*+ HASH_JOIN(u o) */ u.name, o.order_no
FROM test_user u
JOIN user_order o ON u.id = o.user_id;

SELECT /*+ HASH_JOIN(u o) */ u.name, o.order_no
FROM test_user u
JOIN user_order o ON u.id = o.user_id;

-- 测试 12.2: IMP_MERGE_JOIN - Merge Join 实现
-- 有序数据连接，期望使用 Merge Join
EXPLAIN
SELECT /*+ MERGE_JOIN(u o) */ u.name, o.order_no
FROM test_user u
JOIN user_order o ON u.id = o.user_id
ORDER BY u.id;

SELECT /*+ MERGE_JOIN(u o) */ u.name, o.order_no
FROM test_user u
JOIN user_order o ON u.id = o.user_id
ORDER BY u.id;

-- 测试 12.3: IMP_NESTLOOP_JOIN - NestLoop Join 实现
-- 小表驱动，期望使用 NestLoop
EXPLAIN
SELECT /*+ NESTLOOP(u o) */ u.name, o.order_no
FROM test_user u
JOIN user_order o ON u.id = o.user_id
WHERE u.id = 1;

SELECT /*+ NESTLOOP(u o) */ u.name, o.order_no
FROM test_user u
JOIN user_order o ON u.id = o.user_id
WHERE u.id = 1;

-- 测试 12.4: IMP_HASH_AGG - Hash 聚合实现
EXPLAIN
SELECT u.id, COUNT(*) as cnt
FROM test_user u
JOIN user_order o ON u.id = o.user_id
GROUP BY u.id;

SELECT u.id, COUNT(*) as cnt
FROM test_user u
JOIN user_order o ON u.id = o.user_id
GROUP BY u.id;

-- 测试 12.5: IMP_SCAN - 不同扫描方式
-- 全表扫描
EXPLAIN
SELECT * FROM test_user WHERE age > 25;

SELECT * FROM test_user WHERE age > 25;

-- 索引扫描 (如果有 age 索引)
EXPLAIN
SELECT /*+ INDEX_SCAN(u) */ * FROM test_user u WHERE u.id = 1;

SELECT /*+ INDEX_SCAN(u) */ * FROM test_user u WHERE u.id = 1;


-- ============================================================================
-- 13. 综合测试 - 多规则组合应用
-- ============================================================================

-- 测试 13.1: 复杂查询 - 多规则协同
EXPLAIN
SELECT
    u.name,
    COUNT(o.order_id) as order_count,
    SUM(o.order_amount) as total_amount
FROM test_user u
JOIN user_order o ON u.id = o.user_id
WHERE u.age > 18
  AND u.name LIKE 'test%'
  AND o.order_status IN ('COMPLETED', 'PENDING')
  AND o.order_amount > 50
GROUP BY u.id, u.name
HAVING COUNT(o.order_id) > 1
ORDER BY total_amount DESC
LIMIT 10;

SELECT
    u.name,
    COUNT(o.order_id) as order_count,
    SUM(o.order_amount) as total_amount
FROM test_user u
JOIN user_order o ON u.id = o.user_id
WHERE u.age > 18
  AND u.name LIKE 'test%'
  AND o.order_status IN ('COMPLETED', 'PENDING')
  AND o.order_amount > 50
GROUP BY u.id, u.name
HAVING COUNT(o.order_id) > 1
ORDER BY total_amount DESC
LIMIT 10;
-- 期望应用规则:
-- 1. TF_PREDICATE_PUSH_DOWN: 谓词下推到扫描前
-- 2. TF_FILTER_MERGE: 合并多个过滤条件
-- 3. TF_COLUMN_PRUNE: 只扫描需要的列
-- 4. TF_CONSTANT_FOLD: 折叠常量表达式
-- 5. IMP_HASH_JOIN / IMP_MERGE_JOIN: 选择合适的 Join 实现
-- 6. IMP_HASH_AGG: Hash 聚合
-- 7. IMP_SORT: 排序实现


-- 测试 13.2: 子查询展开测试 (TF_SUBQUERY_UNNEST)
EXPLAIN
SELECT u.name, stats.order_count
FROM test_user u
JOIN (
    SELECT user_id, COUNT(*) as order_count
    FROM user_order
    WHERE order_status = 'COMPLETED'
    GROUP BY user_id
) stats ON u.id = stats.user_id
WHERE u.age > 25;

SELECT u.name, stats.order_count
FROM test_user u
JOIN (
    SELECT user_id, COUNT(*) as order_count
    FROM user_order
    WHERE order_status = 'COMPLETED'
    GROUP BY user_id
) stats ON u.id = stats.user_id
WHERE u.age > 25;


-- ============================================================================
-- 14. 边界条件测试
-- ============================================================================

-- 测试 14.1: 空结果集
EXPLAIN
SELECT u.name, o.order_no
FROM test_user u
JOIN user_order o ON u.id = o.user_id
WHERE u.age > 1000;

SELECT u.name, o.order_no
FROM test_user u
JOIN user_order o ON u.id = o.user_id
WHERE u.age > 1000;

-- 测试 14.2: 单表全数据
EXPLAIN
SELECT * FROM test_user;

SELECT * FROM test_user;

-- 测试 14.3: LEFT JOIN 保留所有左表记录
EXPLAIN
SELECT u.name, o.order_no
FROM test_user u
LEFT JOIN user_order o ON u.id = o.user_id;

SELECT u.name, o.order_no
FROM test_user u
LEFT JOIN user_order o ON u.id = o.user_id;

-- 测试 14.4: 聚合函数处理 NULL
EXPLAIN
SELECT u.id, SUM(o.order_amount) as total
FROM test_user u
LEFT JOIN user_order o ON u.id = o.user_id
GROUP BY u.id;

SELECT u.id, SUM(o.order_amount) as total
FROM test_user u
LEFT JOIN user_order o ON u.id = o.user_id
GROUP BY u.id;
