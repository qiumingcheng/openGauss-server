# Oracle 26ai AI Vector Search 子报告：汽车智驾多模态场景实战

> **范围**：原理 + SQL 实战 + 最佳实践
> **覆盖资产**：摄像头图像、激光雷达点云帧、车机语音指令、车机/路侧视频片段、路网/地图文本、技术文档（PDF/Word/Markdown）
> **业务场景**：智能驾驶研发与运营——感知样本检索、相似场景挖掘、车机助手 RAG、法规与技术文档问答、车队语音检索

> ⚠️ **环境说明**：撰写时 WebFetch / WebSearch 不可用，文档地址无法直接验证；请以 Oracle 官方 docs 为准。

---

## 一、业务建模：智驾多模态资产如何映射到 Oracle 26ai

### 1.1 资产盘点与向量化方式

| 资产类型 | 数量级（典型车队） | 嵌入方式 | 向量维度 | 元数据维度 |
|---|---|---|---|---|
| **摄像头图像**（前视/环视/舱内） | 单车每天 ~10 万帧 → 千万级/年 | 多模态大模型（如 CLIP、SigLIP、DFN5B、COCA、车端 BEV 编码器） | 768 / 1024 / 4096 | 时间、车型、摄像头 ID、天气、地理位置、标签 |
| **激光雷达点云帧** | 单车每天 ~5 万帧 → 千万级/年 | PointNet++、VoxelNet、柱状特征网络、MinkowskiEngine 编码后投影到向量 | 256 / 512 / 1024 | 时间、车型、雷达型号、点云密度、雨雪雾标注 |
| **车机语音**（司机指令、舱内对话） | 千~万次/车/天 | Whisper 转文本 → text-embedding-3 或 BGE；或 Wav2Vec / ECAPA-TDNN 声纹向量 | 768 / 1536 / 1920 | 时长、说话人、方言、车速、舱内噪声分贝 |
| **车机/路侧视频片段** | 10~60 秒/段，TB/天 | 按帧抽关键帧 → 图像模型聚合；或 Video-MAE / TimeSformer | 768~4096 | 起止时间、地点、事件标签（行人横穿、追尾前 5s 等） |
| **路网/地图文本**（POI、限速、车道规则） | 百万条 | BGE、bce-embedding、text-embedding-3 | 512 / 1024 / 1536 | 区域、道路类型、更新时间、来源 |
| **技术文档**（法规、专利、整车/SOC 手册） | 10 万级 | Unstructured 解析 → 文本分段 → BGE / text-embedding-3 | 512 / 1024 / 1536 | 文档类型、版本、所属车型、机密等级 |

### 1.2 统一向量数据库（Converged Vector Store）

Oracle 26ai 的关键优势：**所有模态写入同一份 PDB**，通过 SQL 同时做：

- **向量检索**（相似场景挖掘、相似样本推荐）
- **结构化过滤**（车型、时间窗口、地理位置）
- **全文检索**（法规、技术文档关键字）
- **图查询**（PGQL：场景 → 事件 → 参与者之间的因果图）

---

## 二、原理速览：智驾需要懂的关键机制

### 2.1 距离度量（Distance Metrics）

| 度量 | 公式特点 | 智驾适用场景 |
|---|---|---|
| **COSINE** | 衡量方向相似性，对幅值不敏感 | 文本（法规、语音转写）、CLIP 图文嵌入 |
| **DOT / INNER_PRODUCT** | 同时考虑幅值与方向 | 经过 L2 归一化的嵌入、推荐召回 |
| **EUCLIDEAN (L2)** | 几何距离 | 图像原始特征、点云 BEV 特征 |
| **HAMMING** | 二值向量 | 经过二值量化的紧凑向量（嵌入式部署） |

26ai 通过 `VECTOR_DISTANCE(vec1, vec2, metric)` 函数显式指定。

### 2.2 索引：HNSW vs IVF（量化）

| 维度 | HNSW | IVF / IVF-PQ |
|---|---|---|
| 召回率 | 高（>0.95） | 中（0.85~0.95） |
| 构建速度 | 中 | 快 |
| 内存占用 | 高（无压缩） | 低（PQ 压缩后降到 1/8~1/32） |
| 增量更新 | 支持（在线插入） | 需要重建或局部刷新 |
| **智驾推荐** | 千万级以下图像/文本样本库；召回优先的研发环节 | 10 亿+ 点云帧；车载边缘部署；带宽/内存受限 |

26ai 在 23ai/26ai 支持的量化类型：

- `VECTOR` 字段上的 `*_QUANTIZE` 函数族（FETCH_APPROXIMATE、INT8、BINARY）
- 索引层 `NEIGHBOR GRAPH ... QUANTIZE` 选项（文档细节以 Oracle 26ai Vector Search User's Guide 为准）

### 2.3 混合检索（Hybrid Retrieval）—— 智驾召回的核心公式

```
final_score = α · vector_sim + β · bm25_score + γ · structured_match
```

- 智驾场景几乎都需要"标量过滤"：车型、地理围栏、时间窗、事件标签、车队 ID、机密等级。
- 通过 SQL 把向量距离 + 全文分 + 标量谓词放在同一查询中，配合 `FETCH APPROXIMATE FIRST n` 控制召回量。

---

## 三、SQL 实战：从建表到召回

> 以下 SQL 全部基于 Oracle Database 26ai 的 AI Vector Search 语法（与 23ai 兼容路径）。所有 `DBMS_VECTOR` / `UTL_TO_EMBEDDING` 调用可能因 RU 补丁而微调，请以实际版本文档为准。

### 3.1 启用 AI Vector Search 用户

```sql
-- 需要 DBA 在 PDB 内执行（每个 PDB 独立）
@$ORACLE_HOME/rdbms/admin/catvec.sql   -- 一次性安装 vector/ai 内核包
GRANT DB_DEVELOPER_ROLE TO drv_user CONTAINER = CURRENT;
GRANT EXECUTE ON DBMS_VECTOR TO drv_user;
GRANT EXECUTE ON DBMS_VECTOR_CHAIN TO drv_user;
GRANT EXECUTE ON UTL_TO_EMBEDDING TO drv_user;
```

### 3.2 配置凭据（用于调用外部嵌入模型）

```sql
-- 走 OCI Generative AI / Azure OpenAI / OpenAI / 自托管 vLLM 都可
BEGIN
  DBMS_VECTOR_CHAIN.DROP_CREDENTIAL('OPENAI_CRED');
EXCEPTION WHEN OTHERS THEN NULL; END;
/

BEGIN
  DBMS_VECTOR_CHAIN.CREATE_CREDENTIAL(
    credential_name => 'OPENAI_CRED',
    params => JSON_OBJECT(
      'provider'        VALUE 'openai',
      'access_token'    VALUE '<your-api-key>',
      'host'            VALUE 'api.openai.com',
      'url'             VALUE 'https://api.openai.com/v1/embeddings'
    )
  );
END;
/
```

### 3.3 多模态主表设计（一张统一表 + 类型字段）

```sql
CREATE TABLE driving_assets (
  asset_id        RAW(16) DEFAULT SYS_GUID() PRIMARY KEY,
  asset_type      VARCHAR2(16)  NOT NULL,    -- 'IMAGE','LIDAR','AUDIO','VIDEO','MAP_TEXT','DOC'
  source_uri      VARCHAR2(512) NOT NULL,    -- 对象存储路径 / 文件 URI
  fleet_id        VARCHAR2(32),
  vehicle_model   VARCHAR2(32),
  captured_at     TIMESTAMP,
  geo             SDO_GEOMETRY,               -- 可选：GPS 位置
  tags            JSON,                       -- {"weather":"rain","scene":"highway"}
  payload_blob    BLOB,                       -- 原始或压缩文件
  vector          VECTOR(1024, FLOAT32),      -- 统一维度；不同模型维度不一时可建多列或动态表
  external_meta   JSON,
  created_at      TIMESTAMP DEFAULT SYSTIMESTAMP
);

-- 时序 + 地理 + 元数据复合索引（标量侧）
CREATE INDEX ix_assets_capture  ON driving_assets (captured_at);
CREATE INDEX ix_assets_fleet    ON driving_assets (fleet_id, vehicle_model);
CREATE INDEX ix_assets_geo      ON driving_assets (geo) INDEXTYPE IS MDSYS.SPATIAL_INDEX;
```

> **技巧**：如果图像与点云使用的嵌入模型维度不同，可以拆为多张 `*_embeddings` 子表，统一以 `asset_id` 与主表关联；或在新版本里把 `VECTOR` 列改为 **多列**（`vec_image VECTOR(768)` / `vec_lidar VECTOR(512)`）。这也是 26ai 多模态融合的常见模式。

### 3.4 创建向量索引（按场景选型）

```sql
-- 图像/语音/文本（千万级以下） → HNSW，目标低延迟高召回
CREATE VECTOR INDEX idx_assets_hnsw
  ON driving_assets (vector)
  ORGANIZATION INMEMORY NEIGHBOR GRAPH
  DISTANCE COSINE
  WITH TARGET ACCURACY 95;

-- 点云帧（亿级）→ IVF + PQ 量化（语法以 RU 版本为准）
CREATE VECTOR INDEX idx_assets_ivfpq
  ON driving_assets (vector)
  ORGANIZATION INMEMORY NEIGHBOR PARTITIONS
  DISTANCE EUCLIDEAN
  WITH (NUM_PARTITIONS 4096, NUM_NEIGHBORS_PER_PARTITION 32, COMPRESSION PRODUCT QUANTIZE);
```

> **注意**：26ai 矢量索引的具体参数命名（`NEIGHBOR GRAPH` / `NEIGHBOR PARTITIONS`）与 `COMPRESSION` 子句请以 [Oracle AI Vector Search User's Guide](https://docs.oracle.com/en/database/oracle/oracle-database/26/vecse/) 实际章节为准；本文优先给到工程意图。

### 3.5 离线入库：多模态资产 → 嵌入 → 写库

#### 3.5.1 图像（调用 CLIP/SigLIP/DFN 服务）

```sql
INSERT INTO driving_assets (asset_type, source_uri, fleet_id, vehicle_model,
                            captured_at, tags, payload_blob, vector)
SELECT
  'IMAGE',
  's3://driving-fleets/'||file_key,
  fleet_id,
  model_code,
  ts_captured,
  JSON_OBJECT('weather' VALUE weather, 'scene' VALUE scene_label),
  to_blob(b),
  -- 通过 DBMS_VECTOR_CHAIN 把 base64 图像发到多模态嵌入服务
  UTL_TO_EMBEDDING(
    UTL_TO_GENERATE_TEXT(
      JSON_OBJECT('prompt'   VALUE 'embed this driving frame',
                  'content'  VALUE b),
      JSON_OBJECT('provider' VALUE 'oci',
                  'model'    VALUE 'cohere.embed-image-v1')
    ),
    JSON_OBJECT('provider' VALUE 'oci', 'model' VALUE 'cohere.embed-image-v1')
  )
FROM staging_image_files
WHERE NOT EXISTS (
  SELECT 1 FROM driving_assets a
  WHERE a.source_uri = 's3://driving-fleets/'||staging_image_files.file_key);
```

> 真实生产里通常 **不直接 SELECT-INSERT 大字段**。更常见的做法是用 Python/Go 端预生成向量二进制，使用绑定变量 + `INSERT ... VALUES (:vec)`；26ai 也支持 `VECTOR(...)` 字面量直接来自应用侧。

#### 3.5.2 文本（文档/法规/POI）

```sql
INSERT INTO driving_assets (asset_type, source_uri, tags, vector)
SELECT
  'DOC',
  doc_path,
  JSON_OBJECT('confidential' VALUE confidential_level,
              'doc_type'    VALUE doc_type),
  UTL_TO_EMBEDDING(
    chunk_text,
    JSON_OBJECT('provider' VALUE 'openai',
                'credential_name' VALUE 'OPENAI_CRED',
                'model'    VALUE 'text-embedding-3-large',
                'dimensions' VALUE 1024)
  )
FROM doc_chunks
WHERE chunk_text IS NOT NULL;
```

#### 3.5.3 语音

```sql
-- 步骤 1：调用 Whisper 转写
DECLARE
  l_transcript CLOB;
BEGIN
  l_transcript := UTL_TO_GENERATE_TEXT(
    JSON_OBJECT('provider' VALUE 'oci',
                'model'    VALUE 'whisper-large-v3',
                'file_uri' VALUE 's3://fleet-audio/'||:audio_key),
    JSON_OBJECT('language' VALUE 'zh')
  );

  -- 步骤 2：把转写文本嵌入并写库
  INSERT INTO driving_assets (asset_type, source_uri, tags, vector)
  VALUES (
    'AUDIO',
    's3://fleet-audio/'||:audio_key,
    JSON_OBJECT('duration_s' VALUE :duration,
                'speaker'    VALUE :speaker_id),
    UTL_TO_EMBEDDING(l_transcript, JSON_OBJECT('model' VALUE 'text-embedding-3-large'))
  );
END;
/
```

### 3.6 检索：典型智驾查询

#### 3.6.1 相似场景挖掘（图像 → 相似图像）

```sql
-- 1) 取出参考图像向量
DEFINE target_vec = SELECT vector FROM driving_assets WHERE asset_id = :qid;

-- 2) 找出上海区域、雨天、过去 7 天内的相似场景
SELECT asset_id, source_uri, captured_at,
       VECTOR_DISTANCE(vector, :target_vec, COSINE) AS dist
FROM   driving_assets
WHERE  asset_type = 'IMAGE'
  AND  geo       IS NOT NULL
  AND  SDO_WITHIN_DISTANCE(geo, :ref_geom, 'distance=50 unit=km') = 'TRUE'
  AND  captured_at > SYSTIMESTAMP - INTERVAL '7' DAY
  AND  JSON_VALUE(tags,'$.weather') = 'rain'
ORDER BY VECTOR_DISTANCE(vector, :target_vec, COSINE)
FETCH APPROXIMATE FIRST 50 ROWS ONLY WITH TARGET ACCURACY 90;
```

#### 3.6.2 跨模态召回：文本 → 图像（语义搜图）

```sql
-- 用户输入："雨天高速，前方车辆急刹场景"
WITH q AS (
  SELECT UTL_TO_EMBEDDING(
           :nl_query,
           JSON_OBJECT('model' VALUE 'text-embedding-3-large')
         ) AS vec
         FROM DUAL
)
SELECT a.asset_id, a.source_uri,
       VECTOR_DISTANCE(a.vector, q.vec, COSINE) AS dist
FROM   driving_assets a, q
WHERE  a.asset_type = 'IMAGE'
ORDER BY dist
FETCH APPROXIMATE FIRST 20 ROWS ONLY;
```

#### 3.6.3 文档 RAG：法规问答

```sql
WITH q AS (
  SELECT UTL_TO_EMBEDDING(:user_question,
           JSON_OBJECT('model' VALUE 'text-embedding-3-large')) AS vec
  FROM DUAL
)
SELECT a.asset_id, a.source_uri, a.tags,
       VECTOR_DISTANCE(a.vector, q.vec, COSINE) AS dist
FROM   driving_assets a, q
WHERE  a.asset_type = 'DOC'
  AND  JSON_VALUE(a.tags,'$.doc_type') IN ('GB','UN_R','EU_Reg')
  -- 权限过滤
  AND  JSON_VALUE(a.tags,'$.confidential') <= :user_clearance
ORDER BY dist
FETCH APPROXIMATE FIRST 8 ROWS ONLY
-- 可以再叠加 BM25 全文过滤（双路召回，再融合）
```

#### 3.6.4 语音检索：找出"和这条语音最相似的历史指令"

```sql
SELECT a.asset_id, a.source_uri,
       VECTOR_DISTANCE(a.vector, :query_vec, COSINE) AS dist
FROM   driving_assets a
WHERE  a.asset_type = 'AUDIO'
  AND  a.fleet_id  = :fleet_id
ORDER BY dist
FETCH APPROXIMATE FIRST 5 ROWS ONLY;
```

### 3.7 Re-rank 与 LLM 回答生成

```sql
-- 1) 用 SQL 把粗排 Top-50 拉出来
-- 2) Python/Node 端用 Cross-Encoder（如 BGE-reranker-large）做精排
-- 3) 把 Top-5 chunk 拼入 Prompt：

SELECT a.source_uri || ' | ' || a.tags AS ctx
FROM   driving_assets a
ORDER  BY VECTOR_DISTANCE(a.vector, :q_vec, COSINE)
FETCH  APPROXIMATE FIRST 5 ROWS ONLY;
```

LLM 调用建议走 `DBMS_CLOUD_AI.GENERATE_TEXT()` 或应用侧直连 OCI / vLLM；务必做敏感字段脱敏后再出库。

---

## 四、最佳实践（智驾专项）

### 4.1 数据建模

1. **统一资产表 + 类型字段**，跨模态关联用 `asset_id`；不要为每种模态建独立 PDB。
2. **元数据驱动过滤**：法规、技术机密、车型、地理围栏都进 `tags JSON` 或独立列，便于权限与召回过滤。
3. **元数据权限下沉**：机密等级（`public` / `internal` / `restricted`）写入文档，检索时强制比对。
4. **多向量字段**：图像与点云如果维度不同，用多列 `vec_image VECTOR(1024)` / `vec_lidar VECTOR(512)`，避免维度归一化丢失精度。
5. **版本化嵌入模型**：每次模型升级新建向量列 `vec_v2`，新旧并存，跑离线评估后再切流。

### 4.2 索引选型

| 场景 | 推荐 | 理由 |
|---|---|---|
| 图像样本库（< 5000 万） | HNSW + COSINE | 召回优先，毫秒级 |
| 点云帧（亿级） | IVF + PQ | 内存可控，构建时间短 |
| 法规/技术文档 | HNSW + COSINE + 全文索引 | 文本检索 + 关键字 hybrid |
| 实时车机端 | IVF-PQ + INT8 二值量化 | 边缘内存、带宽受限 |
| 多车队共享 + 单租户隔离 | 独立 PDB + HNSW | 多租户隔离 + 调参灵活 |

### 4.3 性能调优

1. **向量内存驻留**：监控 `V$VECTOR_INDEX` / `GV$VECTOR_MEMORY`，目标 ≥ 90% 命中 `buffer_cache`/`IM column store`。
2. **Exadata 智能扫描**：Smart Scan 在向量距离计算上收益有限，主要是减少向量块 IO；混合检索的标量侧收益明显。
3. **避免大结果集**：始终用 `FETCH APPROXIMATE FIRST n WITH TARGET ACCURACY 90`；`n` 控制在 50~200。
4. **并行度**：HNSW 构建很贵，用 `PARALLEL` + Resource Manager 把构建放在低峰窗口。
5. **量化**：INT8 量化能压缩内存 4 倍，召回下降通常 < 1%；PQ 可达 16~32 倍，但需重建索引。
6. **缓存层**：26ai 的 **True Cache** 可以把 Top-K 热门向量搜索结果直接缓存在应用侧，RAG 端到端 P99 显著下降。

### 4.4 安全与合规（智驾强约束）

1. **数据不出库**：嵌入生成与 LLM 调用尽量走 **OCI Generative AI / 本地 vLLM**；用 DBMS_CLOUD 控制出站白名单。
2. **机密分级**：法规文档中可能含 EU R155/R156、UN R157 等限制条款，机密等级写入 `tags.confidential`，SQL 层强制校验。
3. **审计**：对 `UTL_TO_EMBEDDING` / `UTL_TO_GENERATE_TEXT` 调用启用 Unified Auditing，记录调用方、目标向量 ID、SQL 文本。
4. **SQL Firewall**：先记录 7 天基线，再切到 `BLOCK`，防止异常脚本扫库。
5. **TDE + Key Vault**：法规、用户语音、舱内对话都是 GDPR/个保法敏感数据，TDE 全列加密。
6. **多租户**：主机厂 / Tier-1 / 算法供应商三方协作时，每个合作方一个 **PDB**，用 Lockdown Profile 限制跨 PDB 操作。

### 4.5 RAG 端到端流水线

```
┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐
│  数据采集层       │    │  嵌入与入库       │    │  检索与生成       │
│  摄像头/雷达/语音 │──▶│  26ai + VECTOR 列 │──▶│  Hybrid + Re-rank│
└──────────────────┘    └──────────────────┘    └──────────────────┘
        │                       │                       │
        ▼                       ▼                       ▼
   对象存储 OCI/S3       HNSW/IVF-PQ 索引         vLLM / OCI GenAI
   + Kafka 实时流         + JSON 元数据             + SQL Firewall
```

**关键工程要点：**

- **分块策略**：法规/PDF 用 512~1024 tokens，重叠 10%~20%；车机操作手册建议按章节切。
- **增量入库**：流式数据通过 Oracle Transactional Event Queues (TxEventQ) + Kafka Connect 入库，避免大批量 `INSERT`。
- **冷热分层**：近 30 天热数据在主 PDB；历史数据归档到 ADG 备库或 Object Storage，再按需 attach。
- **离线评估**：每周跑一次召回/准确率回归测试集，对比 HNSW vs IVF、量化前后差异。

### 4.6 自治化运维

- **Autonomous AI Database**：智驾研发团队小、变更频繁时，直接上 Autonomous AI Database，减少 DBA 负担；自带 Auto Scaling、Auto Indexing。
- **Database Management (OCI)**：统一监控多套 26ai 实例，提供 AWR 仓库、SQL 调优、容量预测。
- **Observability + Select AI**：26ai 增强了 AI 驱动的根因分析；DBA 可直接用自然语言问"过去 1 小时向量检索最慢的 SQL 是什么"。

---

## 五、智驾场景落地 Checklist

| # | 任务 | 关键点 |
|---|---|---|
| 1 | 资产清单与脱敏 | PII / 隐私 / 法规密级统一标签 |
| 2 | 嵌入模型选型 | 图像 CLIP/SigLIP、文本 BGE/text-embedding-3、点云 PointNet++ 离线编码 |
| 3 | 表结构与索引 | 统一资产表 + HNSW/IVF 双轨 |
| 4 | 离线入库 + 实时入库 | 批量走 Spark/Glue，流式走 Kafka/TxEventQ |
| 5 | 检索 API 化 | 单一 SQL endpoint 返回 Top-K，权限过滤前置 |
| 6 | Re-rank + LLM 编排 | 跨模态场景用 Cross-Encoder；最终答案用 LLM 拼接 |
| 7 | 评测集 | 每条业务场景做 100+ 真实问题的人工标注，回归 |
| 8 | 安全合规 | TDE + SQL Firewall + Unified Auditing + DB Vault |
| 9 | 高可用 | ADG 承担向量检索读负载；True Cache 做边缘缓存 |
| 10 | 自治化 | 监控 + 自动索引 + 容量预测 |

---

## 六、关键 SQL 速查卡

| 用途 | SQL 片段 |
|---|---|
| 余弦相似度 | `VECTOR_DISTANCE(a.vector, b.vector, COSINE)` |
| 欧氏距离 | `VECTOR_DISTANCE(a.vector, b.vector, EUCLIDEAN)` |
| 点积（归一化后） | `VECTOR_DISTANCE(a.vector, b.vector, DOT)` |
| 近似 Top-K | `ORDER BY VECTOR_DISTANCE(...) FETCH APPROXIMATE FIRST 50 ROWS ONLY WITH TARGET ACCURACY 95;` |
| 创建 HNSW | `CREATE VECTOR INDEX ... ORGANIZATION INMEMORY NEIGHBOR GRAPH DISTANCE COSINE ...` |
| 创建 IVF | `CREATE VECTOR INDEX ... ORGANIZATION INMEMORY NEIGHBOR PARTITIONS ...` |
| 嵌入文本 | `UTL_TO_EMBEDDING(:text, JSON_OBJECT('model' VALUE 'text-embedding-3-large'))` |
| 凭据 | `DBMS_VECTOR_CHAIN.CREATE_CREDENTIAL(...)` |

---

## 七、推荐下一步行动

1. **小范围 POC**：拿 50 万张摄像头图像 + 5 万段法规文本，跑完整入库 → 检索 → Re-rank → LLM 流水线。
2. **基准对比**：HNSW vs IVF、不同量化方案、26ai vs 独立向量数据库（如 Milvus / pgvector）。
3. **跨模态验证**：用同一段文本查询图像库和点云库，确认 CLIP/SigLIP 在智驾场景的语义召回率。
4. **合规对齐**：和法务 / 信息安全确认机密分级与跨境传输策略，再决定是否上 OCI / 私有化 vLLM。
5. **集成进 MLOps**：把 26ai 作为特征/检索库接入现有数据闭环（采集 → 标注 → 训练 → 仿真），实现"用真实场景数据驱动模型迭代"。

---

## 八、参考来源（建议手动验证）

- Oracle AI Database 26ai 主页：https://www.oracle.com/cn/database/ai-native-database-26ai/
- Oracle Database 26ai 文档总入口：https://docs.oracle.com/en/database/oracle/oracle-database/26/
- Oracle AI Vector Search User's Guide：https://docs.oracle.com/en/database/oracle/oracle-database/26/vecse/
- Oracle AI Vector Search SQL Guide（`VECTOR` / `VECTOR_DISTANCE`）：https://docs.oracle.com/en/database/oracle/oracle-database/26/sqlrf/
- Oracle LiveLabs（动手实验）：https://livelabs.oracle.com/
- Oracle Database Blog：https://blogs.oracle.com/database/

> 由于撰写时 WebFetch / WebSearch 在当前环境不可用，上文 SQL 语法细节（如 `UTL_TO_EMBEDDING` 完整签名、向量索引 `ORGANIZATION INMEMORY NEIGHBOR GRAPH` / `NEIGHBOR PARTITIONS` 子句、量化选项名）请以 Oracle 官方最新文档为准。