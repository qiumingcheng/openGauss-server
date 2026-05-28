-- ============================================================================
-- 2表JOIN索引消除ORDER BY 测试用例
-- 测试通过 PassThrough 机制让外表使用索引消除排序
-- ============================================================================

-- ============================================================================
-- GUC 设置 - 启用 Cascades 优化器
-- ============================================================================
SET enable_cascades_optimizer = on;

-- 验证 GUC 设置
SHOW enable_cascades_optimizer;

-- ============================================================================
-- 场景1: ORDER BY 外表列，有可用单列索引
-- 期望: 外表 test_user 使用 age 列上的索引消除排序
-- ============================================================================
EXPLAIN
SELECT u.name, o.order_no
FROM test_user u
JOIN user_order o ON u.id = o.user_id
ORDER BY u.age;

SELECT u.name, o.order_no
FROM test_user u
JOIN user_order o ON u.id = o.user_id
ORDER BY u.age;

-- ============================================================================
-- 场景2: ORDER BY 外表主键列
-- 期望: 外表 test_user 使用主键索引 id 消除排序
-- ============================================================================
EXPLAIN
SELECT u.name, o.order_no
FROM test_user u
JOIN user_order o ON u.id = o.user_id
ORDER BY u.id;

SELECT u.name, o.order_no
FROM test_user u
JOIN user_order o ON u.id = o.user_id
ORDER BY u.id;

-- ============================================================================
-- 场景3: ORDER BY 外表多列，使用多列索引
-- 期望: 外表 test_user 使用多列索引消除排序
-- ============================================================================
EXPLAIN
SELECT u.id, u.name, o.order_no
FROM test_user u
JOIN user_order o ON u.id = o.user_id
ORDER BY u.id, u.age;

SELECT u.id, u.name, o.order_no
FROM test_user u
JOIN user_order o ON u.id = o.user_id
ORDER BY u.id, u.age;

-- ============================================================================
-- 场景4: ORDER BY 外表列，但索引方向不匹配（降序）
-- 期望: 使用索引但需要反向扫描，或需要额外处理
-- ============================================================================
EXPLAIN
SELECT u.name, o.order_no
FROM test_user u
JOIN user_order o ON u.id = o.user_id
ORDER BY u.age DESC;

SELECT u.name, o.order_no
FROM test_user u
JOIN user_order o ON u.id = o.user_id
ORDER BY u.age DESC;

-- ============================================================================
-- 场景5: ORDER BY 内表列，无法消除
-- 期望: 需要 PhysicalSort 显式排序
-- ============================================================================
EXPLAIN
SELECT u.name, o.order_no
FROM test_user u
JOIN user_order o ON u.id = o.user_id
ORDER BY o.order_amount;

SELECT u.name, o.order_no
FROM test_user u
JOIN user_order o ON u.id = o.user_id
ORDER BY o.order_amount;

-- ============================================================================
-- 场景6: RIGHT JOIN 外表列，无法消除（RIGHT JOIN破坏排序）
-- 期望: 需要 PhysicalSort 显式排序
-- ============================================================================
EXPLAIN
SELECT u.name, o.order_no
FROM test_user u
RIGHT JOIN user_order o ON u.id = o.user_id
ORDER BY u.age;

SELECT u.name, o.order_no
FROM test_user u
RIGHT JOIN user_order o ON u.id = o.user_id
ORDER BY u.age;

-- ============================================================================
-- 场景7: FULL JOIN 外表列，无法消除（FULL JOIN破坏排序）
-- 期望: 需要 PhysicalSort 显式排序
-- ============================================================================
EXPLAIN
SELECT u.name, o.order_no
FROM test_user u
FULL JOIN user_order o ON u.id = o.user_id
ORDER BY u.age;

SELECT u.name, o.order_no
FROM test_user u
FULL JOIN user_order o ON u.id = o.user_id
ORDER BY u.age;

-- ============================================================================
-- 场景8: LEFT JOIN 外表列，非FULL/RIGHT类型
-- 期望: 外表 test_user 使用索引消除排序
-- ============================================================================
EXPLAIN
SELECT u.name, o.order_no
FROM test_user u
LEFT JOIN user_order o ON u.id = o.user_id
ORDER BY u.age;

SELECT u.name, o.order_no
FROM test_user u
LEFT JOIN user_order o ON u.id = o.user_id
ORDER BY u.age;

-- ============================================================================
-- 场景9: ORDER BY 外表列 + WHERE 条件
-- 期望: 外表索引扫描 + join
-- ============================================================================
EXPLAIN
SELECT u.name, o.order_no
FROM test_user u
JOIN user_order o ON u.id = o.user_id
WHERE u.age > 25
ORDER BY u.age;

SELECT u.name, o.order_no
FROM test_user u
JOIN user_order o ON u.id = o.user_id
WHERE u.age > 25
ORDER BY u.age;

-- ============================================================================
-- 场景10: JOIN后ORDER BY两表的混合列
-- 期望: 无法完全消除排序，需要 PhysicalSort
-- ============================================================================
EXPLAIN
SELECT u.name, o.order_no
FROM test_user u
JOIN user_order o ON u.id = o.user_id
ORDER BY u.age, o.order_amount;

SELECT u.name, o.order_no
FROM test_user u
JOIN user_order o ON u.id = o.user_id
ORDER BY u.age, o.order_amount;

-- ============================================================================
-- 场景11: NestLoop风格场景 + ORDER BY 外表列
-- 期望: 外表 test_user 使用索引消除排序，内表作为参数化扫描
-- ============================================================================
EXPLAIN
SELECT u.name, o.order_no
FROM test_user u
INNER JOIN user_order o ON u.id = o.user_id
WHERE u.id < 100
ORDER BY u.age;

SELECT u.name, o.order_no
FROM test_user u
INNER JOIN user_order o ON u.id = o.user_id
WHERE u.id < 100
ORDER BY u.age;

-- ============================================================================
-- 场景12: JOIN后ORDER BY只在外表列
-- 验证排序消除是否生效
-- ============================================================================
EXPLAIN
SELECT u.id, u.name, o.order_no, o.order_amount
FROM test_user u
JOIN user_order o ON u.id = o.user_id
WHERE u.age BETWEEN 20 AND 30
ORDER BY u.id;

SELECT u.id, u.name, o.order_no, o.order_amount
FROM test_user u
JOIN user_order o ON u.id = o.user_id
WHERE u.age BETWEEN 20 AND 30
ORDER BY u.id;

-- ============================================================================
-- 验证执行计划的方法
-- 在 EXPLAIN 输出中查找以下关键词判断是否正确:
-- - "Index Scan" 或 "Index Only Scan" -> 使用了索引
-- - "Sort" -> 需要显式排序
-- - "Passthrough" 或 "Pass" -> 通过 PassThrough 消除排序
-- ============================================================================

-- ============================================================================
-- 回归测试: 确保原有功能不受影响
-- ============================================================================

-- 原有测试: 单表 ORDER BY
EXPLAIN
SELECT * FROM test_user ORDER BY age;

SELECT * FROM test_user ORDER BY age;

-- 原有测试: JOIN 无 ORDER BY
EXPLAIN
SELECT u.name, o.order_no
FROM test_user u
JOIN user_order o ON u.id = o.user_id;

SELECT u.name, o.order_no
FROM test_user u
JOIN user_order o ON u.id = o.user_id;

-- 原有测试: JOIN + ORDER BY 内表列（无法消除）
EXPLAIN
SELECT u.name, o.order_no
FROM test_user u
JOIN user_order o ON u.id = o.user_id
ORDER BY o.order_no;

SELECT u.name, o.order_no
FROM test_user u
JOIN user_order o ON u.id = o.user_id
ORDER BY o.order_no;

-- ============================================================================
-- 性能对比测试: 有无排序消除的执行计划差异
-- ============================================================================

-- 期望看到索引扫描的计划
EXPLAIN
SELECT u.id, u.name
FROM test_user u
JOIN user_order o ON u.id = o.user_id
ORDER BY u.id
LIMIT 10;

-- 期望看到 Sort 的计划
EXPLAIN
SELECT u.id, u.name
FROM test_user u
JOIN user_order o ON u.id = o.user_id
ORDER BY o.order_amount
LIMIT 10;