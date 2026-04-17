-- ============================================================================
-- Cascades Optimizer test_user 单表集成测试用例
-- 测试目标：验证优化器规则在单表场景下的正确性
-- ============================================================================

-- ============================================================================
-- 前置准备：GUC 设置
-- ============================================================================
SET enable_cascades_optimizer = on;
SHOW enable_cascades_optimizer;

-- ============================================================================
-- 1. TF_PREDICATE_PUSH_DOWN - 谓词下推测试 (单表)
-- 验证谓词能否下推到 Scan 之前
-- ============================================================================

-- 测试 1.1: 基本谓词下推
-- 期望：age > 25 下推到 Scan 节点
EXPLAIN
SELECT * FROM test_user WHERE age > 25;

SELECT * FROM test_user WHERE age > 25;

-- 测试 1.2: 多条件谓词下推
-- 期望：多个条件下推到 Scan
EXPLAIN
SELECT * FROM test_user
WHERE age > 25 AND name LIKE 'user_%';

SELECT * FROM test_user
WHERE age > 25 AND name LIKE 'user_%';

-- 测试 1.3: OR 条件谓词
EXPLAIN
SELECT * FROM test_user
WHERE age > 30 OR age < 20;

SELECT * FROM test_user
WHERE age > 30 OR age < 20;

-- 测试 1.4: IN 条件谓词
EXPLAIN
SELECT * FROM test_user
WHERE age IN (25, 30, 35);

SELECT * FROM test_user
WHERE age IN (25, 30, 35);

-- 测试 1.5: BETWEEN 条件
EXPLAIN
SELECT * FROM test_user
WHERE age BETWEEN 25 AND 35;

SELECT * FROM test_user
WHERE age BETWEEN 25 AND 35;


-- ============================================================================
-- 2. TF_FILTER_MERGE - 过滤器合并测试
-- 验证多个 Filter 能否合并为一个
-- ============================================================================

-- 测试 2.1: 多个 AND 条件合并
EXPLAIN
SELECT * FROM test_user
WHERE age > 20
  AND name LIKE 'test%'
  AND email LIKE '%@example.com';

SELECT * FROM test_user
WHERE age > 20
  AND name LIKE 'test%'
  AND email LIKE '%@example.com';

-- 测试 2.2: 嵌套子查询中的过滤器合并
EXPLAIN
SELECT * FROM (
    SELECT * FROM test_user WHERE age > 18
) t1
WHERE t1.name LIKE 'user_%';

SELECT * FROM (
    SELECT * FROM test_user WHERE age > 18
) t1
WHERE t1.name LIKE 'user_%';


-- ============================================================================
-- 3. TF_COLUMN_PRUNE - 列剪枝测试
-- 验证只扫描需要的列
-- ============================================================================

-- 测试 3.1: 只选择单列
-- 期望：只扫描 id 列
EXPLAIN
SELECT id FROM test_user;

SELECT id FROM test_user;

-- 测试 3.2: 选择部分列
-- 期望：只扫描 name 和 email 列
EXPLAIN
SELECT name, email FROM test_user;

SELECT name, email FROM test_user;

-- 测试 3.3: COUNT(*) 列剪枝
-- 期望：不需要扫描任何实际列数据
EXPLAIN
SELECT COUNT(*) FROM test_user;

SELECT COUNT(*) FROM test_user;

-- 测试 3.4: COUNT(列) 列剪枝
-- 期望：只扫描 id 列
EXPLAIN
SELECT COUNT(id) FROM test_user;

SELECT COUNT(id) FROM test_user;

-- 测试 3.5: 聚合函数列剪枝
EXPLAIN
SELECT SUM(age), AVG(age) FROM test_user;

SELECT SUM(age), AVG(age) FROM test_user;

-- 测试 3.6: MIN/MAX 列剪枝
EXPLAIN
SELECT MIN(age), MAX(age) FROM test_user;

SELECT MIN(age), MAX(age) FROM test_user;


-- ============================================================================
-- 4. TF_CONSTANT_FOLD - 常量折叠测试
-- 验证常量表达式在编译时计算
-- ============================================================================

-- 测试 4.1: 算术常量折叠
EXPLAIN
SELECT * FROM test_user
WHERE age > 20 + 5;

SELECT * FROM test_user
WHERE age > 20 + 5;
-- 期望：20 + 5 折叠为 25

-- 测试 4.2: 比较常量折叠
EXPLAIN
SELECT * FROM test_user
WHERE 1 = 1 AND age > 25;

SELECT * FROM test_user
WHERE 1 = 1 AND age > 25;
-- 期望：1 = 1 折叠为 TRUE

-- 测试 4.3: 复杂常量表达式
EXPLAIN
SELECT * FROM test_user
WHERE age > (100 - 75) * 2;

SELECT * FROM test_user
WHERE age > (100 - 75) * 2;
-- 期望：(100 - 75) * 2 折叠为 50

-- 测试 4.4: 布尔常量折叠
EXPLAIN
SELECT * FROM test_user
WHERE (1 = 1 OR age > 1000) AND name LIKE 'user_%';

SELECT * FROM test_user
WHERE (1 = 1 OR age > 1000) AND name LIKE 'user_%';
-- 期望：1 = 1 OR ... 折叠为 TRUE


-- ============================================================================
-- 5. TF_SORT_ELIMINATION - 排序消除测试
-- 验证不必要的排序被消除
-- ============================================================================

-- 测试 5.1: 利用主键索引顺序
-- 期望：利用主键索引有序性，消除显式排序
EXPLAIN
SELECT * FROM test_user ORDER BY id;

SELECT * FROM test_user ORDER BY id;

-- 测试 5.2: 利用索引顺序
EXPLAIN
SELECT * FROM test_user WHERE age > 25 ORDER BY age;

SELECT * FROM test_user WHERE age > 25 ORDER BY age;
-- 期望：利用 idx_test_user_age 索引消除排序

-- 测试 5.3: GROUP BY 自带顺序
-- 期望：GROUP BY 已产生有序输出，ORDER BY 可被消除
EXPLAIN
SELECT age, COUNT(*) as cnt
FROM test_user
GROUP BY age
ORDER BY age;

SELECT age, COUNT(*) as cnt
FROM test_user
GROUP BY age
ORDER BY age;

-- 测试 5.4: MIN/MAX 利用索引
EXPLAIN
SELECT MIN(id), MAX(id) FROM test_user;
-- 期望：利用索引直接获取最值，无需全表扫描


-- ============================================================================
-- 6. TF_AGGREGATION_SIMPLIFY - 聚合简化测试
-- ============================================================================

-- 测试 6.1: COUNT(*) 简化
EXPLAIN
SELECT COUNT(*) FROM test_user;

SELECT COUNT(*) FROM test_user;

-- 测试 6.2: 单列 GROUP BY
EXPLAIN
SELECT age, COUNT(*) as cnt
FROM test_user
GROUP BY age;

SELECT age, COUNT(*) as cnt
FROM test_user
GROUP BY age;

-- 测试 6.3: 多列 GROUP BY
EXPLAIN
SELECT age, name, COUNT(*) as cnt
FROM test_user
GROUP BY age, name;

SELECT age, name, COUNT(*) as cnt
FROM test_user
GROUP BY age, name;

-- 测试 6.4: GROUP BY + HAVING
EXPLAIN
SELECT age, COUNT(*) as cnt
FROM test_user
GROUP BY age
HAVING COUNT(*) > 1;

SELECT age, COUNT(*) as cnt
FROM test_user
GROUP BY age
HAVING COUNT(*) > 1;

-- 测试 6.5: 聚合函数组合
EXPLAIN
SELECT
    COUNT(*) as total,
    COUNT(age) as with_age,
    SUM(age) as sum_age,
    AVG(age) as avg_age,
    MIN(age) as min_age,
    MAX(age) as max_age
FROM test_user;

SELECT
    COUNT(*) as total,
    COUNT(age) as with_age,
    SUM(age) as sum_age,
    AVG(age) as avg_age,
    MIN(age) as min_age,
    MAX(age) as max_age
FROM test_user;

-- 测试 6.6: DISTINCT 聚合
EXPLAIN
SELECT COUNT(DISTINCT age) FROM test_user;

SELECT COUNT(DISTINCT age) FROM test_user;


-- ============================================================================
-- 7. IMP_SCAN - 扫描方式选择测试
-- ============================================================================

-- 测试 7.1: 全表扫描 (无选择性条件)
EXPLAIN
SELECT * FROM test_user;

SELECT * FROM test_user;

-- 测试 7.2: 索引扫描 (主键等值)
EXPLAIN
SELECT * FROM test_user WHERE id = 1;

SELECT * FROM test_user WHERE id = 1;
-- 期望：使用主键索引扫描

-- 测试 7.3: 索引扫描 (有索引的列)
EXPLAIN
SELECT * FROM test_user WHERE age = 25;

SELECT * FROM test_user WHERE age = 25;
-- 期望：使用 idx_test_user_age 索引扫描

-- 测试 7.4: 索引范围扫描
EXPLAIN
SELECT * FROM test_user WHERE age > 30;

SELECT * FROM test_user WHERE age > 30;
-- 期望：使用 idx_test_user_age 范围扫描

-- 测试 7.5: LIKE 前缀匹配
EXPLAIN
SELECT * FROM test_user WHERE name LIKE 'user_%';

SELECT * FROM test_user WHERE name LIKE 'user_%';
-- 期望：使用 idx_test_user_name 索引扫描

-- 测试 7.6: LIKE 前缀通配 (无法使用索引)
EXPLAIN
SELECT * FROM test_user WHERE name LIKE '%user%';

SELECT * FROM test_user WHERE name LIKE '%user%';
-- 期望：全表扫描 (前缀通配无法使用索引)


-- ============================================================================
-- 8. IMP_PROJECT - 投影操作测试
-- ============================================================================

-- 测试 8.1: 简单投影
EXPLAIN
SELECT id, name FROM test_user;

SELECT id, name FROM test_user;

-- 测试 8.2: 表达式投影
EXPLAIN
SELECT id, age + 1 as next_age FROM test_user;

SELECT id, age + 1 as next_age FROM test_user;

-- 测试 8.3: CASE 表达式投影
EXPLAIN
SELECT
    name,
    CASE
        WHEN age < 20 THEN 'young'
        WHEN age < 40 THEN 'middle'
        ELSE 'senior'
    END as age_group
FROM test_user;

SELECT
    name,
    CASE
        WHEN age < 20 THEN 'young'
        WHEN age < 40 THEN 'middle'
        ELSE 'senior'
    END as age_group
FROM test_user;


-- ============================================================================
-- 9. LIMIT/OFFSET 测试
-- ============================================================================

-- 测试 9.1: 简单 LIMIT
EXPLAIN
SELECT * FROM test_user LIMIT 10;

SELECT * FROM test_user LIMIT 10;

-- 测试 9.2: LIMIT OFFSET
EXPLAIN
SELECT * FROM test_user LIMIT 10 OFFSET 5;

SELECT * FROM test_user LIMIT 10 OFFSET 5;

-- 测试 9.3: ORDER BY + LIMIT
EXPLAIN
SELECT * FROM test_user ORDER BY id LIMIT 5;

SELECT * FROM test_user ORDER BY id LIMIT 5;
-- 期望：利用索引有序性，提前终止扫描


-- ============================================================================
-- 10. 边界条件测试
-- ============================================================================

-- 测试 10.1: 空结果集
EXPLAIN
SELECT * FROM test_user WHERE age > 200;

SELECT * FROM test_user WHERE age > 200;
-- 期望：返回空结果

-- 测试 10.2: 单行结果
EXPLAIN
SELECT * FROM test_user WHERE id = 1;

SELECT * FROM test_user WHERE id = 1;

-- 测试 10.3: NULL 值处理
EXPLAIN
SELECT * FROM test_user WHERE email IS NULL;

SELECT * FROM test_user WHERE email IS NULL;

-- 测试 10.4: NOT NULL 条件
EXPLAIN
SELECT * FROM test_user WHERE email IS NOT NULL;

SELECT * FROM test_user WHERE email IS NOT NULL;

-- 测试 10.5: 空字符串
EXPLAIN
SELECT * FROM test_user WHERE name = '';

SELECT * FROM test_user WHERE name = '';


-- ============================================================================
-- 11. 综合测试 - 多规则组合
-- ============================================================================

-- 测试 11.1: 复杂单表查询
-- 期望应用规则:
-- 1. TF_PREDICATE_PUSH_DOWN: 谓词下推
-- 2. TF_FILTER_MERGE: 合并过滤条件
-- 3. TF_COLUMN_PRUNE: 列剪枝
-- 4. TF_CONSTANT_FOLD: 常量折叠
-- 5. TF_SORT_ELIMINATION: 排序消除 (如果可能)
EXPLAIN
SELECT
    age,
    COUNT(*) as user_count,
    AVG(LENGTH(name)) as avg_name_length
FROM test_user
WHERE age > 20
  AND age < 50
  AND name LIKE 'user_%'
  AND email LIKE '%@example.com'
GROUP BY age
HAVING COUNT(*) > 1
ORDER BY user_count DESC
LIMIT 10;

SELECT
    age,
    COUNT(*) as user_count,
    AVG(LENGTH(name)) as avg_name_length
FROM test_user
WHERE age > 20
  AND age < 50
  AND name LIKE 'user_%'
  AND email LIKE '%@example.com'
GROUP BY age
HAVING COUNT(*) > 1
ORDER BY user_count DESC
LIMIT 10;

-- 测试 11.2: 子查询 + 聚合
EXPLAIN
SELECT
    t1.age_group,
    COUNT(*) as cnt
FROM (
    SELECT
        CASE
            WHEN age < 25 THEN 'young'
            WHEN age < 40 THEN 'middle'
            ELSE 'senior'
        END as age_group
    FROM test_user
    WHERE age IS NOT NULL
) t1
GROUP BY t1.age_group;

SELECT
    t1.age_group,
    COUNT(*) as cnt
FROM (
    SELECT
        CASE
            WHEN age < 25 THEN 'young'
            WHEN age < 40 THEN 'middle'
            ELSE 'senior'
        END as age_group
    FROM test_user
    WHERE age IS NOT NULL
) t1
GROUP BY t1.age_group;


-- ============================================================================
-- 12. 性能回归测试点 (记录预期行为)
-- ============================================================================

-- 测试 12.1: 全表扫描成本
-- 用于后续对比优化效果
EXPLAIN (COSTS OFF)
SELECT COUNT(*) FROM test_user;

-- 测试 12.2: 索引扫描成本
EXPLAIN (COSTS OFF)
SELECT COUNT(*) FROM test_user WHERE id = 1;

-- 测试 12.3: 范围扫描成本
EXPLAIN (COSTS OFF)
SELECT * FROM test_user WHERE age > 30;

-- 测试 12.4: 聚合成本
EXPLAIN (COSTS OFF)
SELECT age, COUNT(*) FROM test_user GROUP BY age;


-- ============================================================================
-- 测试完成
-- ============================================================================
SELECT 'test_user single table test completed!' AS status;
