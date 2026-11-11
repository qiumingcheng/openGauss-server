-- diskann_include.sql — DiskANN INCLUDE + In-Feature 回归（骨架版，S0-3）
-- 执行文档：公共文档/多模_向标融合_DiskANN_INCLUDE_InFilter_执行文档.md
-- 说明：本文件随各原子步骤扩展；当前仅含 S0-3 冒烟用例，验证测试链路本身可用。
-- 运行方式（骨架阶段，未加入 schedule）：
--   gsql -X -d <scratch_db> -p 55440 -f src/test/regress/sql/diskann_include.sql

-- S0-3 冒烟：基线 DiskANN 能力可用（与 diskann_vector.sql 无重复断言，仅链路自检）
CREATE TABLE diskann_include_smoke (
    id          SERIAL PRIMARY KEY,
    tenant_id   INT,
    embedding   vector(6)
);
INSERT INTO diskann_include_smoke (tenant_id, embedding) VALUES
    (1, '[0.1, 0.2, 0.3, 0.4, 0.5, 0.6]'),
    (2, '[0.7, 0.8, 0.9, 1.0, 1.1, 1.2]'),
    (1, '[1.3, 1.4, 1.5, 1.6, 1.7, 1.8]'),
    (2, '[0.5, 0.4, 0.3, 0.2, 1.0, 1.2]');
ANALYZE diskann_include_smoke;
CREATE INDEX idx_include_smoke
    ON diskann_include_smoke USING diskann (embedding vector_l2_ops);

-- 计划应为 Ann Index Scan（小表需 hint 强制，与 diskann_vector.sql 约定一致）
EXPLAIN (COSTS OFF)
SELECT /*+ indexscan(diskann_include_smoke idx_include_smoke) */ id
FROM diskann_include_smoke
ORDER BY embedding <-> '[0.1, 0.2, 0.3, 0.4, 0.5, 0.6]' LIMIT 2;

SELECT /*+ indexscan(diskann_include_smoke idx_include_smoke) */ id
FROM diskann_include_smoke
ORDER BY embedding <-> '[0.1, 0.2, 0.3, 0.4, 0.5, 0.6]' LIMIT 2;

DROP TABLE diskann_include_smoke;

-- ============================================================================
-- [P0-2] rescan 状态隔离：prepared statement 换参数二次执行不得串上一查询候选
-- 缺陷背景（diskannscan.cpp:51-66 基线）：rescan 仅重置 curpos，candidates/
-- visited/curIterNum 跨执行残留 → 第二次 EXECUTE 先返回上一查询的旧候选。
-- ============================================================================

CREATE TABLE diskann_p02_t (
    id          INT PRIMARY KEY,
    embedding   vector(6)
);
-- 9 行确定数据：1-3 聚在 A 附近，7-9 聚在 B 附近，两集合结果不相交
INSERT INTO diskann_p02_t VALUES
    (1, '[0.10, 0.10, 0.10, 0.10, 0.10, 0.10]'),
    (2, '[0.12, 0.10, 0.10, 0.10, 0.10, 0.10]'),
    (3, '[0.10, 0.12, 0.10, 0.10, 0.10, 0.10]'),
    (4, '[2.00, 2.00, 2.00, 2.00, 2.00, 2.00]'),
    (5, '[2.50, 2.00, 2.00, 2.00, 2.00, 2.00]'),
    (6, '[2.00, 2.50, 2.00, 2.00, 2.00, 2.00]'),
    (7, '[5.00, 5.00, 5.00, 5.00, 5.00, 5.00]'),
    (8, '[5.12, 5.00, 5.00, 5.00, 5.00, 5.00]'),
    (9, '[5.00, 5.12, 5.00, 5.00, 5.00, 5.00]');
ANALYZE diskann_p02_t;
CREATE INDEX idx_p02 ON diskann_p02_t USING diskann (embedding vector_l2_ops);

-- 无索引镜像表 = 精确真值（设计文档 §19.4 模式）
CREATE TABLE diskann_p02_exact AS SELECT * FROM diskann_p02_t;

SET plan_cache_mode = force_generic_plan;
-- 关闭 OPFusion 旁路：Bypass 路径不走标准执行器 rescan，测不到目标缺陷
SET enable_opfusion = off;

-- 触发器：LATERAL 参数化内表扫描（外层每行驱动内层 AnnIndexScan rescan，
-- 换查询向量；PREPARE custom plan 每次新建执行器测不到，故用此形态）
DROP TABLE IF EXISTS diskann_p02_queries;
CREATE TABLE diskann_p02_queries (qid INT, qvec vector(6));
INSERT INTO diskann_p02_queries VALUES
    (1, '[0.10, 0.10, 0.10, 0.10, 0.10, 0.10]'),
    (2, '[5.00, 5.00, 5.00, 5.00, 5.00, 5.00]');

SELECT q.qid, t.id
FROM diskann_p02_queries q,
LATERAL (SELECT /*+ indexscan(diskann_p02_t idx_p02) */ id
         FROM diskann_p02_t
         ORDER BY embedding <-> q.qvec LIMIT 3) t;

-- 精确真值（无索引全表距离排序；qid=1 → {1,2,3}，qid=2 → {7,8,9}）
SELECT q.qid, e.id
FROM diskann_p02_queries q,
LATERAL (SELECT id FROM diskann_p02_exact
         ORDER BY embedding <-> q.qvec LIMIT 3) e;

-- 计划形态确认（内层应为 Ann Index Scan，非 Bypass）
EXPLAIN (COSTS OFF)
SELECT q.qid, t.id
FROM diskann_p02_queries q,
LATERAL (SELECT /*+ indexscan(diskann_p02_t idx_p02) */ id
         FROM diskann_p02_t
         ORDER BY embedding <-> q.qvec LIMIT 3) t;

RESET plan_cache_mode;
RESET enable_opfusion;
DROP TABLE diskann_p02_t;
DROP TABLE diskann_p02_exact;

-- ============================================================================
-- [P0-3a] 插入路径资源释放功能验证：graphStore delete（正常+错误路径）与
-- PQ wrapper pfree 后，批量插入、错误回滚、索引查询均正常
-- ============================================================================

DROP TABLE IF EXISTS diskann_p03_t;
CREATE TABLE diskann_p03_t (id INT PRIMARY KEY, embedding vector(6));
INSERT INTO diskann_p03_t SELECT g, ('[' || g || ',' || g || ',1,2,3,4]')::vector
FROM generate_series(1, 50) g;
CREATE INDEX idx_diskann_p03 ON diskann_p03_t USING diskann (embedding vector_l2_ops) WITH (index_size = 100);
ANALYZE diskann_p03_t;

-- 单语句批量插入（一个语句上下文内 N 次在线 INSERT → graphStore 正常路径 delete；
-- debug -O0 构建下每次图链接约 0.2s，200 行 ≈40s 内完成且充分覆盖路径）
INSERT INTO diskann_p03_t
SELECT g, ('[' || (g % 90 + 1) || ',' || (g % 80 + 1) || ',2,3,4,5]')::vector
FROM generate_series(100, 300) g;

-- 索引仍可正确查询（delete 未破坏图）
SELECT count(*) AS p03_rows FROM diskann_p03_t;
SELECT /*+ indexscan(diskann_p03_t idx_diskann_p03) */ id
FROM diskann_p03_t
ORDER BY embedding <-> '[1,1,1,1,1,1]' LIMIT 3;

-- 错误路径：插入中报错（维度不符）→ PG_CATCH 分支执行 → 语句回滚后索引完好
INSERT INTO diskann_p03_t VALUES (99999, '[1,2,3]'::vector);
SELECT count(*) AS p03_rows_after_err FROM diskann_p03_t;
SELECT /*+ indexscan(diskann_p03_t idx_diskann_p03) */ id
FROM diskann_p03_t
ORDER BY embedding <-> '[1,1,1,1,1,1]' LIMIT 3;

DROP TABLE diskann_p03_t;

-- ============================================================================
-- [P1-1] DDL 放开：diskann + INCLUDE 的接受与拒绝矩阵
-- ============================================================================

DROP TABLE IF EXISTS p11_t;
CREATE TABLE p11_t (
    id          INT,
    tenant_id   INT,
    status      VARCHAR(16),
    title       VARCHAR(64),
    price       NUMERIC(10,2),
    created     DATE,
    payload     TEXT,           -- 不支持类型
    embedding   VECTOR(6)
);
CREATE INDEX idx_p11_ok ON p11_t USING diskann (embedding vector_l2_ops)
    INCLUDE (tenant_id, status, price, created);
-- 两个 key 拒绝
CREATE INDEX idx_p11_twokeys ON p11_t USING diskann (embedding vector_l2_ops, tenant_id);
-- INCLUDE 与 key 相交拒绝（通用检查既有）
CREATE INDEX idx_p11_intersect ON p11_t USING diskann (embedding vector_l2_ops) INCLUDE (embedding);
-- 表达式列拒绝
CREATE INDEX idx_p11_expr ON p11_t USING diskann (embedding vector_l2_ops) INCLUDE ((tenant_id + 1));
-- 不支持类型拒绝（TEXT）
CREATE INDEX idx_p11_text ON p11_t USING diskann (embedding vector_l2_ops) INCLUDE (payload);
-- key 非向量列拒绝
CREATE INDEX idx_p11_intkey ON p11_t USING diskann (tenant_id) INCLUDE (status);
-- 分区表拒绝
DROP TABLE IF EXISTS p11_part;
CREATE TABLE p11_part (id INT, tenant_id INT, embedding VECTOR(6))
    PARTITION BY RANGE (id) (PARTITION p1 VALUES LESS THAN (100));
CREATE INDEX idx_p11_part ON p11_part USING diskann (embedding vector_l2_ops) INCLUDE (tenant_id);
DROP TABLE p11_part;
DROP INDEX idx_p11_ok;
DROP TABLE p11_t;

-- ============================================================================
-- [P1-2] reloptions：in_filter / max_inline_payload 三处成对注册
-- ============================================================================

DROP TABLE IF EXISTS p12_t;
CREATE TABLE p12_t (id INT, tenant_id INT, embedding VECTOR(6));
INSERT INTO p12_t SELECT g, g, ('['||g||','||g||',1,2,3,4]')::vector FROM generate_series(1,20) g;

-- 新 reloption 可接受（带值 + 默认省略）
CREATE INDEX idx_p12_opts ON p12_t USING diskann (embedding vector_l2_ops)
    WITH (index_size = 16, in_filter = true, max_inline_payload = 512);
CREATE INDEX idx_p12_def ON p12_t USING diskann (embedding vector_l2_ops)
    WITH (index_size = 16);

-- 未知参数报错（三处缺一处时的失败模式）
CREATE INDEX idx_p12_unknown ON p12_t USING diskann (embedding vector_l2_ops)
    WITH (index_size = 16, no_such_option = 1);

-- 越界值报错
CREATE INDEX idx_p12_over ON p12_t USING diskann (embedding vector_l2_ops)
    WITH (index_size = 16, max_inline_payload = 999999);

-- ALTER：in_filter 可动态切换（路径开关）
ALTER INDEX idx_p12_def SET (in_filter = false);
ALTER INDEX idx_p12_def SET (in_filter = true);

-- ALTER：max_inline_payload 原地修改拒绝并提示 REINDEX
ALTER INDEX idx_p12_def SET (max_inline_payload = 2048);

DROP INDEX idx_p12_opts;
DROP INDEX idx_p12_def;
DROP TABLE p12_t;

-- ============================================================================
-- [P1-3a] 冻结点不带真实 TID + 完整 IndexTuple + 一行一节点
-- ============================================================================

-- T1 冻结点重复：基线 frozen 节点携带真实 TID → 同一行从两个节点各返回一次
DROP TABLE IF EXISTS p13_t1;
CREATE TABLE p13_t1 (id INT, tenant_id INT, embedding VECTOR(6));
INSERT INTO p13_t1 SELECT g, g % 3, ('['||g||','||(g%7)||',2,3,4,5]')::vector FROM generate_series(1,12) g;
ANALYZE p13_t1;
CREATE INDEX idx_p13_t1 ON p13_t1 USING diskann (embedding vector_l2_ops) WITH (index_size = 16);
-- 取全部 12 行（远大于首轮候选数触发扩搜）→ 每个.id 恰一次
SELECT count(*) AS t1_total, count(DISTINCT id) AS t1_distinct
FROM (SELECT /*+ indexscan(p13_t1 idx_p13_t1) */ id FROM p13_t1
      ORDER BY embedding <-> '[1,2,2,3,4,5]' LIMIT 40) s;
DROP TABLE p13_t1;

-- T2 重复向量 + 一行一节点 + 完整 IndexTuple（INCLUDE）后行为正确
DROP TABLE IF EXISTS p13_t2;
CREATE TABLE p13_t2 (id INT, tenant_id INT, embedding VECTOR(6));
INSERT INTO p13_t2 VALUES
    (1, 10, '[1,1,1,1,1,1]'),
    (2, 20, '[1,1,1,1,1,1]'),   -- 与 id=1 向量相同、payload 不同
    (3, 30, '[5,5,5,5,5,5]'),
    (4, 40, '[5,5,5,5,5,5]');
ANALYZE p13_t2;
CREATE INDEX idx_p13_t2 ON p13_t2 USING diskann (embedding vector_l2_ops)
    INCLUDE (tenant_id) WITH (index_size = 16);
-- 两对重复向量：四行都必须各出现一次
SELECT count(*) AS t2_total, count(DISTINCT id) AS t2_distinct
FROM (SELECT /*+ indexscan(p13_t2 idx_p13_t2) */ id FROM p13_t2
      ORDER BY embedding <-> '[1,1,1,1,1,1]' LIMIT 10) s;
DROP TABLE p13_t2;

-- T3 INCLUDE 索引正常 DML：UPDATE payload 列后查询仍正确（新索引项路径）
DROP TABLE IF EXISTS p13_t3;
CREATE TABLE p13_t3 (id INT, tenant_id INT, embedding VECTOR(6));
INSERT INTO p13_t3 SELECT g, g, ('['||g||','||g||',1,2,3,4]')::vector FROM generate_series(1,20) g;
CREATE INDEX idx_p13_t3 ON p13_t3 USING diskann (embedding vector_l2_ops)
    INCLUDE (tenant_id) WITH (index_size = 16);
ANALYZE p13_t3;
INSERT INTO p13_t3 VALUES (99, 99, '[1,1,1,1,1,1]');
UPDATE p13_t3 SET tenant_id = 1000 WHERE id = 99;
SELECT count(*) AS t3_rows FROM p13_t3;
SELECT /*+ indexscan(p13_t3 idx_p13_t3) */ id FROM p13_t3
ORDER BY embedding <-> '[1,1,1,1,1,1]' LIMIT 2;
DROP TABLE p13_t3;

-- ============================================================================
-- [P1-3b] v2 格式 + immediate-delete 门控 + ALTER 拦截
-- ============================================================================

-- B1 建索引后启用 immediate_delete → 拒绝（KNOWN_ISSUES #5 关闭）
DROP TABLE IF EXISTS p13b_t;
CREATE TABLE p13b_t (id INT, tenant_id INT, embedding VECTOR(6));
INSERT INTO p13b_t SELECT g, g, ('['||g||','||g||',1,2,3,4]')::vector FROM generate_series(1,20) g;
CREATE INDEX idx_p13b ON p13b_t USING diskann (embedding vector_l2_ops)
    INCLUDE (tenant_id) WITH (index_size = 16);
ALTER TABLE p13b_t SET (immediate_delete = true);

-- B2 v2 索引上 DELETE 走 deferred：结果仍正确（MVCC 兜底）
DELETE FROM p13b_t WHERE id <= 5;
SELECT count(*) AS b2_rows FROM p13b_t;
SELECT /*+ indexscan(p13b_t idx_p13b) */ id FROM p13b_t
ORDER BY embedding <-> '[1,1,1,1,1,1]' LIMIT 3;

-- B3 无 INCLUDE 索引的表可启用 immediate_delete（旧行为不变）
-- [KNOWN_ISSUES #6 已解决：根因是 make 头文件依赖失效导致 diskanndelete.o
--  用旧结构布局编译→SIGSEGV；touch 重编后通过，非代码缺陷]
DROP TABLE IF EXISTS p13b_old;
CREATE TABLE p13b_old (id INT, embedding VECTOR(6));
INSERT INTO p13b_old SELECT g, ('['||g||','||g||',1,2,3,4]')::vector FROM generate_series(1,10) g;
CREATE INDEX idx_p13b_old ON p13b_old USING diskann (embedding vector_l2_ops) WITH (index_size = 16);
ALTER TABLE p13b_old SET (immediate_delete = true);
DELETE FROM p13b_old WHERE id <= 3;
SELECT count(*) AS b3_rows FROM p13b_old;
DROP TABLE p13b_old;
DROP TABLE p13b_t;

-- ============================================================================
-- [P1-4] catalog：diskanncanreturn 注册
-- 本实例（升级路径）验证：amcanreturn 基态为 0；SQL 直插 pg_proc 在 debug 构建
-- 会触发 catcache 断言（D-013），升级往返脚本保留为交付物但不在本环境执行。
-- 新装库路径验证：initdb 新实例 catalog 自带注册（newinstall_verify.log +
-- newinstall_behavior.log，见 evidence/P1-4）。
-- ============================================================================

SELECT amcanreturn = 0 AS c1_no_canreturn_old_install FROM pg_am WHERE oid = 8303;

-- C4：INCLUDE 索引在 canreturn=true（新装库）下结果正确——旧实例不生效，
-- 行为等价 P1-3 已覆盖；此处保留运行验证以防计划形态变化
DROP TABLE IF EXISTS p14_t;
CREATE TABLE p14_t (id INT, tenant_id INT, embedding VECTOR(6));
INSERT INTO p14_t SELECT g, g % 3, ('['||g||','||g||',1,2,3,4]')::vector FROM generate_series(1,20) g;
ANALYZE p14_t;
CREATE INDEX idx_p14 ON p14_t USING diskann (embedding vector_l2_ops)
    INCLUDE (tenant_id) WITH (index_size = 16);
SELECT count(*) AS c4_count FROM p14_t;
SELECT /*+ indexscan(p14_t idx_p14) */ id FROM p14_t
ORDER BY embedding <-> '[1,1,1,1,1,1]' LIMIT 3;
DROP TABLE p14_t;

-- ============================================================================
-- [P2-1] 谓词白名单分类：可下推谓词在 EXPLAIN 显示 Index Filter（执行不变）
-- ============================================================================

DROP TABLE IF EXISTS p21_t;
CREATE TABLE p21_t (
    id          INT,
    tenant_id   INT,
    status      VARCHAR(16),
    price       NUMERIC(10,2),
    embedding   VECTOR(6)
);
INSERT INTO p21_t SELECT g, g % 3, 'OK', g * 1.5,
    ('['||g||','||g||',1,2,3,4]')::vector FROM generate_series(1,30) g;
ANALYZE p21_t;
CREATE INDEX idx_p21 ON p21_t USING diskann (embedding vector_l2_ops)
    INCLUDE (tenant_id, status, price) WITH (index_size = 16);
VACUUM p21_t;

-- 白名单形态：等值（Const）→ 应出现 Filter Mode: In-Filter + Index Filter
EXPLAIN (COSTS OFF) SELECT /*+ indexscan(p21_t idx_p21) */ tenant_id FROM p21_t
WHERE tenant_id = 10 ORDER BY embedding <-> '[1,1,1,1,1,1]' LIMIT 3;
-- 结果不变（recheck 保留）
SELECT /*+ indexscan(p21_t idx_p21) */ tenant_id FROM p21_t
WHERE tenant_id = 10 ORDER BY embedding <-> '[1,1,1,1,1,1]' LIMIT 3;

-- 白名单形态：范围 + IN
EXPLAIN (COSTS OFF) SELECT /*+ indexscan(p21_t idx_p21) */ tenant_id FROM p21_t
WHERE price > 20 AND status IN ('OK') ORDER BY embedding <-> '[1,1,1,1,1,1]' LIMIT 3;

-- 拒绝形态：OR / 表达式函数 → 无 In-Filter 行（POST 语义）
EXPLAIN (COSTS OFF) SELECT /*+ indexscan(p21_t idx_p21) */ tenant_id FROM p21_t
WHERE tenant_id = 10 OR tenant_id = 20 ORDER BY embedding <-> '[1,1,1,1,1,1]' LIMIT 3;
EXPLAIN (COSTS OFF) SELECT /*+ indexscan(p21_t idx_p21) */ tenant_id FROM p21_t
WHERE id + tenant_id > 5 ORDER BY embedding <-> '[1,1,1,1,1,1]' LIMIT 3;

DROP TABLE p21_t;

-- ============================================================================
-- [P2-3] AM 内过滤：Rows Removed by Index Filter 证明 AM 侧拒绝发生（>0）
-- ============================================================================

DROP TABLE IF EXISTS p23_t;
CREATE TABLE p23_t (
    id          INT,
    tenant_id   INT,           -- 含 NULL 行
    embedding   VECTOR(6)
);
INSERT INTO p23_t SELECT g, CASE WHEN g % 4 = 0 THEN NULL ELSE g % 3 END,
    ('['||g||','||g||',1,2,3,4]')::vector FROM generate_series(1,40) g;
ANALYZE p23_t;
CREATE INDEX idx_p23 ON p23_t USING diskann (embedding vector_l2_ops)
    INCLUDE (tenant_id) WITH (index_size = 16);
VACUUM p23_t;

-- AM 侧拒绝计数 >0（实现前该行不存在/为 0）
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF)
SELECT /*+ indexscan(p23_t idx_p23) */ tenant_id FROM p23_t
WHERE tenant_id = 1 ORDER BY embedding <-> '[1,1,1,1,1,1]' LIMIT 5;

-- NULL 语义：IS NOT NULL 下推 + recheck 一致
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF)
SELECT /*+ indexscan(p23_t idx_p23) */ count(*) FROM p23_t
WHERE tenant_id IS NOT NULL ORDER BY embedding <-> '[1,1,1,1,1,1]' LIMIT 5;
SELECT /*+ indexscan(p23_t idx_p23) */ tenant_id FROM p23_t
WHERE tenant_id IS NOT NULL ORDER BY embedding <-> '[1,1,1,1,1,1]' LIMIT 5;

-- IN 列表
SELECT /*+ indexscan(p23_t idx_p23) */ tenant_id FROM p23_t
WHERE tenant_id IN (1, 2) ORDER BY embedding <-> '[1,1,1,1,1,1]' LIMIT 5;

-- in_filter reloption off → 不装过滤器（执行回退 post-filter，结果不变）
CREATE INDEX idx_p23_off ON p23_t USING diskann (embedding vector_l2_ops)
    INCLUDE (tenant_id) WITH (index_size = 16, in_filter = false);
SELECT /*+ indexscan(p23_t idx_p23_off) */ tenant_id FROM p23_t
WHERE tenant_id = 1 ORDER BY embedding <-> '[1,1,1,1,1,1]' LIMIT 5;

DROP TABLE p23_t;

-- ============================================================================
-- [P2-4] strict batch：扩搜后输出顺序 == 真值距离序（迟到近点不得乱序）
-- ============================================================================

DROP TABLE IF EXISTS p24_t;
CREATE TABLE p24_t (id INT, tenant_id INT, embedding VECTOR(6));
-- 60 行、tenant 1 仅 1/6（强过滤 → 首轮合格数 < LIMIT → 触发扩搜）
INSERT INTO p24_t SELECT g, CASE WHEN g % 6 = 1 THEN 1 ELSE g % 6 END,
    ('['||g||','||g||',1,2,3,4]')::vector FROM generate_series(1,60) g;
ANALYZE p24_t;
CREATE INDEX idx_p24 ON p24_t USING diskann (embedding vector_l2_ops)
    INCLUDE (tenant_id) WITH (index_size = 16);
VACUUM p24_t;

-- A: pushed（扩搜场景 LIMIT 12 > 首轮合格数）
SELECT /*+ indexscan(p24_t idx_p24) */ id FROM p24_t
WHERE tenant_id = 1 ORDER BY embedding <-> '[1,1,1,1,1,1]' LIMIT 12;
-- B: 真值顺序（同表无索引距离序）
SELECT id FROM p24_t
WHERE tenant_id = 1 ORDER BY embedding <-> '[1,1,1,1,1,1]' LIMIT 12;

-- 无足够匹配：返回全部 10 个匹配（不伪造满 K）
SELECT /*+ indexscan(p24_t idx_p24) */ count(*) AS p24_full FROM (
  SELECT /*+ indexscan(p24_t idx_p24) */ id FROM p24_t
  WHERE tenant_id = 1 ORDER BY embedding <-> '[1,1,1,1,1,1]' LIMIT 99) s;

DROP TABLE p24_t;
