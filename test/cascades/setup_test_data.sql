-- ============================================================================
-- Cascades Optimizer 集成测试 - 测试数据准备脚本
-- ============================================================================

-- 清理现有数据 (如果存在)
DROP TABLE IF EXISTS user_order CASCADE;
DROP TABLE IF EXISTS test_user CASCADE;

-- ============================================================================
-- 1. 创建测试表
-- ============================================================================

CREATE TABLE test_user (
    id SERIAL PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    age INT,
    email VARCHAR(100),
    create_time TIMESTAMP DEFAULT NOW()
);

CREATE TABLE user_order (
    order_id SERIAL PRIMARY KEY,
    user_id INT NOT NULL,
    order_no VARCHAR(32) NOT NULL,
    order_amount NUMERIC(10,2) NOT NULL DEFAULT 0,
    order_status VARCHAR(10) DEFAULT 'PENDING',
    create_time TIMESTAMP DEFAULT NOW(),
    CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES test_user(id)
);

-- ============================================================================
-- 2. 创建索引 (用于测试排序消除和索引扫描)
-- ============================================================================

-- user_order 表上的索引
CREATE INDEX idx_user_order_user_id ON user_order(user_id);
CREATE INDEX idx_user_order_status ON user_order(order_status);
CREATE INDEX idx_user_order_amount ON user_order(order_amount);
CREATE INDEX idx_user_order_create_time ON user_order(create_time);

-- test_user 表上的索引
CREATE INDEX idx_test_user_age ON test_user(age);
CREATE INDEX idx_test_user_name ON test_user(name);

-- ============================================================================
-- 3. 插入测试数据
-- ============================================================================

-- 插入 test_user 数据 (100 条记录)
INSERT INTO test_user (name, age, email) VALUES
('alice', 25, 'alice@example.com'),
('bob', 30, 'bob@example.com'),
('charlie', 35, 'charlie@example.com'),
('david', 20, 'david@example.com'),
('eve', 28, 'eve@example.com'),
('frank', 45, 'frank@example.com'),
('grace', 22, 'grace@example.com'),
('henry', 33, 'henry@example.com'),
('ivy', 29, 'ivy@example.com'),
('jack', 40, 'jack@example.com'),
('kate', 27, 'kate@example.com'),
('leo', 31, 'leo@example.com'),
('mary', 26, 'mary@example.com'),
('nick', 38, 'nick@example.com'),
('olivia', 24, 'olivia@example.com'),
('peter', 42, 'peter@example.com'),
('quinn', 23, 'quinn@example.com'),
('rose', 36, 'rose@example.com'),
('sam', 32, 'sam@example.com'),
('tina', 21, 'tina@example.com');

-- 插入更多用户 (使用 generate_series)
INSERT INTO test_user (name, age, email)
SELECT
    'user_' || i,
    18 + (i % 40),
    'user_' || i || '@example.com'
FROM generate_series(21, 100) AS i;

-- 插入 user_order 数据 (500 条记录)
INSERT INTO user_order (user_id, order_no, order_amount, order_status)
SELECT
    1 + (i % 100),  -- user_id: 1-100
    'ORD' || LPAD(i::text, 8, '0'),  -- order_no: ORD00000001, ORD00000002, ...
    (random() * 1000)::NUMERIC(10,2),  -- order_amount: 0-1000
    CASE (i % 5)
        WHEN 0 THEN 'PENDING'
        WHEN 1 THEN 'COMPLETED'
        WHEN 2 THEN 'SHIPPED'
        WHEN 3 THEN 'CANCELLED'
        ELSE 'PROCESSING'
    END
FROM generate_series(1, 500) AS i;

-- ============================================================================
-- 4. 数据验证查询
-- ============================================================================

-- 验证 test_user 数据量
SELECT COUNT(*) AS user_count FROM test_user;
-- 期望：100

-- 验证 user_order 数据量
SELECT COUNT(*) AS order_count FROM user_order;
-- 期望：500

-- 验证数据分布
SELECT age, COUNT(*) AS cnt FROM test_user GROUP BY age ORDER BY age LIMIT 10;

SELECT order_status, COUNT(*) AS cnt, SUM(order_amount) AS total
FROM user_order
GROUP BY order_status
ORDER BY cnt DESC;

-- 验证连接数据
SELECT
    COUNT(DISTINCT u.id) AS users_with_orders,
    COUNT(DISTINCT o.user_id) AS unique_buyers
FROM test_user u
JOIN user_order o ON u.id = o.user_id;

-- ============================================================================
-- 5. 分析统计信息 (用于优化器决策)
-- ============================================================================

ANALYZE test_user;
ANALYZE user_order;

-- 查看统计信息
SELECT relname, n_live_tup, n_dead_tup
FROM pg_stat_user_tables
WHERE relname IN ('test_user', 'user_order');

-- ============================================================================
-- 6. 测试用例执行准备完成
-- ============================================================================

-- 输出完成消息
SELECT 'Test data setup completed!' AS status;
SELECT 'test_user: ' || COUNT(*) AS user_count FROM test_user;
SELECT 'user_order: ' || COUNT(*) AS order_count FROM user_order;
