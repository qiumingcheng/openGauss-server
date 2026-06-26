# Oracle 26ai AI Vector Search 子报告详解:汽车智驾多模态场景

> 本文是对同目录下 `02-ai-vector-search-autonomous-driving.md` 的**逐节详解版**。原文给出可执行的工程方案与 SQL 模板,本文补充**为什么这样做 / 工程权衡 / 常见坑**。两份文档互为补充,建议配合阅读。
>
> **报告范围**:原理 + SQL 实战 + 最佳实践
> **覆盖资产**:摄像头图像 / 激光雷达点云 / 车机语音 / 视频片段 / 路网地图文本 / 技术文档
> **业务场景**:智驾研发与运营——感知样本检索、相似场景挖掘、车机助手 RAG、法规与技术文档问答、车队语音检索

> ⚠️ **环境说明**:撰写时 WebFetch / WebSearch 不可用,SQL 语法细节请以 Oracle 官方 docs 为准。

---

## 一、业务建模:智驾多模态资产如何映射到 Oracle 26ai

### 1.1 资产盘点与向量化方式(原文表)

| 资产类型 | 数量级 | 嵌入方式 | 向量维度 | 元数据维度 |
|---|---|---|---|---|
| **摄像头图像** | 单车每天 ~10 万帧 → 千万级/年 | 多模态大模型(CLIP / SigLIP / DFN5B / COCA / 车端 BEV 编码器) | 768 / 1024 / 4096 | 时间、车型、摄像头 ID、天气、地理位置、标签 |
| **激光雷达点云帧** | 单车每天 ~5 万帧 → 千万级/年 | PointNet++ / VoxelNet / 柱状特征 / MinkowskiEngine | 256 / 512 / 1024 | 时间、车型、雷达型号、点云密度、雨雪雾标注 |
| **车机语音** | 千~万次/车/天 | Whisper 转文本 → text-embedding-3 / BGE;或 Wav2Vec / ECAPA-TDNN 声纹向量 | 768 / 1536 / 1920 | 时长、说话人、方言、车速、舱内噪声分贝 |
| **车机/路侧视频片段** | 10~60 秒/段,TB/天 | 按帧抽关键帧 → 图像模型聚合;或 Video-MAE / TimeSformer | 768~4096 | 起止时间、地点、事件标签(行人横穿、追尾前 5s 等) |
| **路网/地图文本** | 百万条 | BGE、bce-embedding、text-embedding-3 | 512 / 1024 / 1536 | 区域、道路类型、更新时间、来源 |
| **技术文档** | 10 万级 | Unstructured 解析 → 文本分段 → BGE / text-embedding-3 | 512 / 1024 / 1536 | 文档类型、版本、所属车型、机密等级 |

#### 📌 详细解读

**为什么是这 6 类资产?**

智驾的"数据飞轮"里,真正能进入 RAG / 相似检索闭环的就是这 6 类:
- **图像 / 点云**:供感知算法找相似 corner case;
- **语音**:供车机助手理解用户意图;
- **视频片段**:多帧级的事件检索(追尾前 5s);
- **地图文本**:语义化的 POI / 限速 / 车道规则,辅助决策解释;
- **文档**:法规、专利、整车手册,辅助合规与研发。

**为什么维度不统一?**

| 模态 | 推荐维度 | 关键考虑 |
|---|---|---|
| 图像(CLIP/SigLIP) | 768~1024 | 跨模态搜图需要与文本嵌入对齐到同一空间(SigLIP-base=768, SigLIP-so400m=1152) |
| 点云 | 512~1024 | 不需要跨模态对齐,聚焦几何特征 |
| 语音(文本转写) | 1024~1536 | 文本嵌入主流维度 |
| 文档 | 1024~1536 | text-embedding-3-large 的 1024/3072 是质量甜点 |

> **工程权衡**:如果强求"一列统一维度",要么牺牲精度(短向量装长信息),要么浪费内存(长向量装短信息)。26ai 的多列 `VECTOR` 设计(`vec_image VECTOR(1024)` + `vec_lidar VECTOR(512)`)正是为这种现实服务的。

---

### 1.2 统一向量数据库(Converged Vector Store)

> 原文核心:Oracle 26ai 的关键优势是**所有模态写入同一份 PDB**,通过 SQL 同时做向量检索 / 结构化过滤 / 全文检索 / 图查询。

#### 📌 详细解读

**"Converged"为什么是杀手锏?**

| 传统做法 | 痛点 |
|---|---|
| Milvus 存向量 + MySQL 存元数据 + Elasticsearch 存全文 | 跨库 join、性能差、事务一致性弱、运维复杂、权限散落 |
| PostgreSQL + pgvector + tsvector | 单库 OK,但扩展性、并行度、生态不如 Oracle |
| **Oracle 26ai Converged** | **一库搞定**:SQL 一句话同时表达向量 + 标量 + JSON + 全文 + 空间 |

**智驾的真实收益**:
1. **事务一致性**:标注一条图像 + 写入一段元数据 + 触发一段告警,可以一次性 commit,不会"向量写了但元数据没写"。
2. **跨模态 join**:`image_vector` 和 `doc_vector` 直接 join,无需应用层串联。
3. **多租户隔离**:主机厂 / Tier-1 / 算法供应商三方共享一份 PDB 内各自的 PDB,数据隔离 + 资源配额。

---

## 二、原理速览:智驾需要懂的关键机制

### 2.1 距离度量(Distance Metrics)

| 度量 | 公式特点 | 智驾适用场景 |
|---|---|---|
| **COSINE** | 衡量方向相似性,对幅值不敏感 | 文本(法规、语音转写)、CLIP 图文嵌入 |
| **DOT / INNER_PRODUCT** | 同时考虑幅值与方向 | 经过 L2 归一化的嵌入、推荐召回 |
| **EUCLIDEAN (L2)** | 几何距离 | 图像原始特征、点云 BEV 特征 |
| **HAMMING** | 二值向量 | 经过二值量化的紧凑向量(嵌入式部署) |

#### 📌 详细解读

**为什么智驾场景 80% 用 COSINE?**

- **CLIP / SigLIP** 训练目标就是最大化解码文本-图像对的 cosine 相似度。
- **text-embedding-3** 默认 L2 归一化后,cosine == dot,选哪个都行,但 cosine 更直观。
- 法规文本与图像检索,通常关心"语义方向",不关心"幅度大小"。

**什么时候必须用 EUCLIDEAN?**

- 点云 BEV 特征 **没有 L2 归一化**,余弦会丢失幅值信息(密度、反射强度),应该用 EUCLIDEAN。
- 经过 `L2_NORMALIZE` 之后,COSINE 和 EUCLIDEAN 单调等价,可以互转。

**26ai 的调用方式**:
```sql
VECTOR_DISTANCE(vec_a, vec_b, COSINE)   -- 返回 0(同向) ~ 2(反向)
VECTOR_DISTANCE(vec_a, vec_b, DOT)      -- 越大越相似(注意排序方向)
VECTOR_DISTANCE(vec_a, vec_b, EUCLIDEAN) -- 越小越相似
```

---

### 2.2 索引:HNSW vs IVF(量化)

| 维度 | HNSW | IVF / IVF-PQ |
|---|---|---|
| 召回率 | 高(>0.95) | 中(0.85~0.95) |
| 构建速度 | 中 | 快 |
| 内存占用 | 高(无压缩) | 低(PQ 压缩后降到 1/8~1/32) |
| 增量更新 | 支持(在线插入) | 需要重建或局部刷新 |
| **智驾推荐** | 千万级以下图像/文本样本库;召回优先的研发环节 | 10 亿+ 点云帧;车载边缘部署;带宽/内存受限 |

#### 📌 详细解读

**HNSW 的本质**:

> **H(ierarchical)N(avigable)S(mall)W(orld)** = 多层"小世界"图。顶层是稀疏的"高速公路",底层是稠密的"小巷子"。查询时从顶层贪心下降,保证 log(n) 复杂度 + 高召回。

- **优点**:召回稳定,延迟可预测,在线插入友好;
- **缺点**:内存大(每向量 4~8 字节 × 维数 × 平均度数);构建慢;
- **智驾典型**:图像样本库、文档库、语音库(都在千万级以下)。

**IVF / IVF-PQ 的本质**:

> **I(nverted)F(ile)** = 把全量向量先聚类成 K 个簇,查询时只搜最近的 `nprobe` 个簇。**PQ(Product Quantization)** = 把高维向量拆成 m 段,每段单独聚类编码,内存降到 1/m。

- **优点**:内存极省,构建快,可水平分片;
- **缺点**:召回比 HNSW 低几个百分点;新增向量要局部重建或附加;
- **智驾典型**:亿级点云、车载边缘 INT8 / 二值量化部署。

**26ai 的量化机制**(以官方文档为准,以下为常见形式):
- 字段层:`VECTOR(... QUANTIZE INT8)` 或在写入时通过 `VECTOR_EMBEDDING(... QUANTIZE ...)`;
- 索引层:`WITH (COMPRESSION PRODUCT QUANTIZE)` / `BINARY`。

---

### 2.3 混合检索(Hybrid Retrieval)

```
final_score = α · vector_sim + β · bm25_score + γ · structured_match
```

#### 📌 详细解读

**为什么智驾几乎必须用混合检索?**

**真实案例**:用户问"上海雨天高速前车急刹场景"
- 纯向量:可能召回"上海雨天高速正常行驶"——向量相似但语义不符;
- 纯 BM25:可能因为没出现"前车急刹"字面词,什么都召回不了;
- 纯结构化过滤:只有 `tags.scene='highway'` 也召回太多。

**混合检索公式**:
1. 向量召回 Top-100(语义相关);
2. BM25 召回 Top-100(字面命中);
3. RRF(Reciprocal Rank Fusion)或加权打分;
4. 结构化谓词前置过滤(车型、时间、地理)。

**26ai 的做法**:
- 向量侧:`ORDER BY VECTOR_DISTANCE(...)`;
- 全文侧:`CONTAINS(text_col, '急刹')` + `SCORE()`;
- 结构化:`tags JSON_VALUE`、`geo SDO_WITHIN_DISTANCE`、常规 WHERE;
- 一次性 SQL 串起,优化器会做近似检索 + 标量过滤的剪枝。

---

## 三、SQL 实战:从建表到召回

### 3.1 启用 AI Vector Search 用户(原文)

```sql
@$ORACLE_HOME/rdbms/admin/catvec.sql
GRANT DB_DEVELOPER_ROLE TO drv_user CONTAINER = CURRENT;
GRANT EXECUTE ON DBMS_VECTOR TO drv_user;
GRANT EXECUTE ON DBMS_VECTOR_CHAIN TO drv_user;
GRANT EXECUTE ON UTL_TO_EMBEDDING TO drv_user;
```

#### 📌 详细解读

- `catvec.sql` 是 AI Vector Search 内核的安装脚本,只在 PDB 内首次启用时执行;
- `DBMS_VECTOR` 是低阶向量操作包,`DBMS_VECTOR_CHAIN` 是链式(嵌入→分块→生成),`UTL_TO_EMBEDDING` 是便捷函数;
- `CONTAINER = CURRENT` 意味着只在当前 PDB 授权,符合多租户最小权限原则。

**生产实践建议**:
- 为 AI/向量相关操作建独立角色 `VECTOR_DEVELOPER_ROLE`,把上述权限打包;
- DBA、APP_DBA、应用账号**三权分立**(对应 Database Vault 的 SoD)。

---

### 3.2 配置凭据(原文)

```sql
BEGIN
  DBMS_VECTOR_CHAIN.CREATE_CREDENTIAL(
    credential_name => 'OPENAI_CRED',
    params => JSON_OBJECT(
      'provider'     VALUE 'openai',
      'access_token' VALUE '<your-api-key>',
      'host'         VALUE 'api.openai.com',
      'url'          VALUE 'https://api.openai.com/v1/embeddings'
    )
  );
END;
/
```

#### 📌 详细解读

- 凭据以**名称**引用,SQL 里不出现密钥;
- 支持 OCI Generative AI / Azure OpenAI / OpenAI / Cohere / Hugging Face / 自托管 vLLM(走 HTTPS endpoint);
- **凭据存储在数据库内部**(可走 OCI Vault 加密),应用层拿不到明文;
- **生产建议**:凭据按环境隔离(`OPENAI_CRED_DEV` / `OPENAI_CRED_PROD`),并用 DBMS_SCHEDULER 定期轮换。

---

### 3.3 多模态主表设计(原文)

```sql
CREATE TABLE driving_assets (
  asset_id        RAW(16) DEFAULT SYS_GUID() PRIMARY KEY,
  asset_type      VARCHAR2(16)  NOT NULL,
  source_uri      VARCHAR2(512) NOT NULL,
  fleet_id        VARCHAR2(32),
  vehicle_model   VARCHAR2(32),
  captured_at     TIMESTAMP,
  geo             SDO_GEOMETRY,
  tags            JSON,
  payload_blob    BLOB,
  vector          VECTOR(1024, FLOAT32),
  external_meta   JSON,
  created_at      TIMESTAMP DEFAULT SYSTIMESTAMP
);
```

#### 📌 详细解读

**字段设计逐项剖析**:

| 字段 | 设计意图 |
|---|---|
| `asset_id RAW(16)` | 用 `SYS_GUID()` 代替 number 自增,分布式友好,跨 PDB 唯一 |
| `asset_type` | 多模态共用一表,类型字段区分;避免 6 张表 join |
| `source_uri` | 真实文件存 OCI/S3,数据库只存元数据 + 向量,避免 BLOB 把 PDB 撑爆 |
| `geo SDO_GEOMETRY` | Oracle Spatial 类型,支持地理围栏(50km 内、雨天、上海) |
| `tags JSON` | 半结构化扩展(天气、场景标签),省去无限 ALTER TABLE |
| `payload_blob` | 仅放小预览(thumbnail);完整文件在外 |
| `vector VECTOR(1024, FLOAT32)` | 26ai 原生向量类型,FLOAT32 是默认精度 |

**索引策略(原文)**:

```sql
CREATE INDEX ix_assets_capture  ON driving_assets (captured_at);
CREATE INDEX ix_assets_fleet    ON driving_assets (fleet_id, vehicle_model);
CREATE INDEX ix_assets_geo      ON driving_assets (geo) INDEXTYPE IS MDSYS.SPATIAL_INDEX;
```

- **时序索引**:支撑"最近 7 天"窗口;
- **车队索引**:支撑按 fleet/车型切片(算法供应商做 A/B 测试);
- **空间索引**:支撑地理围栏(上海、北京、六环内)。

**多向量字段的进阶设计**(原文注):

```sql
-- 当图像(1024)、点云(512)、文本(1536)维度不一致时
ALTER TABLE driving_assets ADD (
  vec_image VECTOR(1024, FLOAT32),
  vec_lidar VECTOR(512, FLOAT32),
  vec_text  VECTOR(1536, FLOAT32)
);
```

- 各模态分别建索引,各取所需;
- 跨模态检索时通过 `UNION ALL` 或子查询拼接。

---

### 3.4 创建向量索引(原文)

```sql
-- HNSW
CREATE VECTOR INDEX idx_assets_hnsw
  ON driving_assets (vector)
  ORGANIZATION INMEMORY NEIGHBOR GRAPH
  DISTANCE COSINE
  WITH TARGET ACCURACY 95;

-- IVF-PQ
CREATE VECTOR INDEX idx_assets_ivfpq
  ON driving_assets (vector)
  ORGANIZATION INMEMORY NEIGHBOR PARTITIONS
  DISTANCE EUCLIDEAN
  WITH (NUM_PARTITIONS 4096, NUM_NEIGHBORS_PER_PARTITION 32, COMPRESSION PRODUCT QUANTIZE);
```

#### 📌 详细解读

**`ORGANIZATION INMEMORY NEIGHBOR GRAPH` 关键参数**:

| 子句 | 含义 |
|---|---|
| `ORGANIZATION INMEMORY` | 索引用 In-Memory 存储,要求足够的 IM column store 配额 |
| `NEIGHBOR GRAPH` | HNSW 图索引 |
| `NEIGHBOR PARTITIONS` | IVF 倒排分区索引 |
| `DISTANCE COSINE` | 索引内距离度量(必须与查询一致) |
| `TARGET ACCURACY 95` | 构建时保证 95% 召回;数值越高图越稠密,内存越大 |
| `WITH (NUM_PARTITIONS 4096, ...)` | IVF 簇数(经验值:√N ~ 4√N,N 为向量总数) |

**⚠️ 注意事项(原文也有强调)**:
- 26ai 的索引参数名以 **Oracle AI Vector Search User's Guide** 为准;
- RU 补丁可能调整子句名,生产部署前必须查版本文档;
- `TARGET ACCURACY` 太低会丢召回,太高会爆内存;智驾研发建议从 95 起步。

---

### 3.5 离线入库:多模态资产 → 嵌入 → 写库

#### 3.5.1 图像入库(原文)

```sql
INSERT INTO driving_assets (asset_type, source_uri, ..., vector)
SELECT
  'IMAGE',
  's3://driving-fleets/'||file_key,
  ...,
  UTL_TO_EMBEDDING(
    UTL_TO_GENERATE_TEXT(
      JSON_OBJECT('prompt'   VALUE 'embed this driving frame',
                  'content'  VALUE b),
      JSON_OBJECT('provider' VALUE 'oci',
                  'model'    VALUE 'cohere.embed-image-v1')
    ),
    JSON_OBJECT('provider' VALUE 'oci', 'model' VALUE 'cohere.embed-image-v1')
  )
FROM staging_image_files;
```

#### 📌 详细解读

**三层调用链**:
1. `UTL_TO_GENERATE_TEXT` —— 把图像 base64 发到多模态服务,得到描述向量或临时 token;
2. 再用 `UTL_TO_EMBEDDING` 生成嵌入向量(实际多模态服务一般一步到位);
3. 写入 `vector` 列。

**原文的工程警告**:
> 真实生产里通常**不直接 SELECT-INSERT 大字段**。

正确做法:
- **离线批处理**:Python/Go 用 `oracledb` 驱动,绑定变量 + `VECTOR(...)` 字面量;
- **流式入库**:Kafka → TxEventQ → DBMS_VECTOR_CHAIN;
- **异步嵌入**:先存元数据(无向量),后台 worker 计算嵌入后回填。

**生产 SQL 形态(更推荐)**:

```sql
INSERT INTO driving_assets (asset_id, asset_type, source_uri, vector)
VALUES (:asset_id, 'IMAGE', :uri, :vec);  -- :vec 是 Python 端预生成的 VECTOR 字面量
```

---

#### 3.5.2 文本入库(原文)

```sql
INSERT INTO driving_assets (asset_type, source_uri, tags, vector)
SELECT
  'DOC',
  doc_path,
  JSON_OBJECT('confidential' VALUE confidential_level,
              'doc_type'    VALUE doc_type),
  UTL_TO_EMBEDDING(
    chunk_text,
    JSON_OBJECT('provider'         VALUE 'openai',
                'credential_name'  VALUE 'OPENAI_CRED',
                'model'            VALUE 'text-embedding-3-large',
                'dimensions'       VALUE 1024)
  )
FROM doc_chunks
WHERE chunk_text IS NOT NULL;
```

#### 📌 详细解读

**分块策略建议**(原文 4.5 节):

| 文档类型 | 分块粒度 | 重叠 | 备注 |
|---|---|---|---|
| 法规(GB / UN_R) | 512~1024 tokens | 10%~20% | 按章节切,保留条款编号 |
| 专利 | 256~512 tokens | 15% | 权利要求项独立切 |
| 整车手册 | 1024 tokens | 20% | 按章节切,保留图注 |
| POI / 限速 | 单条 = 单段 | 0% | 短文本直接嵌入 |

**`dimensions` 1024 的选择**:
- OpenAI text-embedding-3-large 支持 256 / 512 / 1024 / 3072;
- 1024 是质量 vs 成本的甜点;
- 256 适合大规模、低成本场景,质量略降。

---

#### 3.5.3 语音入库(原文)

```sql
DECLARE
  l_transcript CLOB;
BEGIN
  l_transcript := UTL_TO_GENERATE_TEXT(
    JSON_OBJECT('provider' VALUE 'oci',
                'model'    VALUE 'whisper-large-v3',
                'file_uri' VALUE 's3://fleet-audio/'||:audio_key),
    JSON_OBJECT('language' VALUE 'zh')
  );

  INSERT INTO driving_assets (asset_type, source_uri, tags, vector)
  VALUES (
    'AUDIO', 's3://fleet-audio/'||:audio_key,
    JSON_OBJECT('duration_s' VALUE :duration, 'speaker' VALUE :speaker_id),
    UTL_TO_EMBEDDING(l_transcript, JSON_OBJECT('model' VALUE 'text-embedding-3-large'))
  );
END;
/
```

#### 📌 详细解读

**两阶段管道**:
1. ASR(Whisper-large-v3)→ 文本;
2. 文本 → 嵌入(text-embedding-3-large);

**为什么不用声纹向量直接索引?**
- 司机意图召回,关心**语义内容**("开窗""导航回家")而不是声纹身份;
- 声纹向量(Wav2Vec/ECAPA)用于**身份识别**,和意图召回是两个索引。

**进阶做法**:同一行存两个向量(`vec_asr_text VECTOR(1536)` 存语义、`vec_speaker VECTOR(1920)` 存声纹),各自建索引。

---

### 3.6 检索:典型智驾查询

#### 3.6.1 相似场景挖掘(图像 → 相似图像)(原文)

```sql
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

#### 📌 详细解读

**逐行拆解**:

| 片段 | 含义 |
|---|---|
| `asset_type = 'IMAGE'` | 类型过滤(标量剪枝) |
| `SDO_WITHIN_DISTANCE(...)` | 地理围栏,50 km 内(空间剪枝) |
| `captured_at > SYSTIMESTAMP - INTERVAL '7' DAY` | 时间窗口(标量剪枝) |
| `JSON_VALUE(tags,'$.weather') = 'rain'` | JSON 标签过滤(标量剪枝) |
| `ORDER BY VECTOR_DISTANCE(...)` | 在剩余集合上做向量距离排序 |
| `FETCH APPROXIMATE FIRST 50 ROWS ONLY WITH TARGET ACCURACY 90` | 近似 Top-50,目标 90% 召回 |

**智驾价值**:
- 算法工程师上传一张"上海雨天高速急刹"参考图,系统召回 50 张相似 corner case;
- 直接喂给模型做难例挖掘(data mining);
- 比"按标签筛选"强大得多——能发现**没被人工标注**的相似场景。

**关键优化点**:
1. **过滤前置**:把标量谓词写在 WHERE,优化器会先做剪枝,再算向量;
2. **避免 SELECT \***:只取需要的列,减少 IO;
3. **向量在 IM column store 中**:SGA 大小要能装下索引 + 热向量。

---

#### 3.6.2 跨模态召回:文本 → 图像(原文)

```sql
WITH q AS (
  SELECT UTL_TO_EMBEDDING(:nl_query,
           JSON_OBJECT('model' VALUE 'text-embedding-3-large')) AS vec
  FROM DUAL
)
SELECT a.asset_id, a.source_uri,
       VECTOR_DISTANCE(a.vector, q.vec, COSINE) AS dist
FROM   driving_assets a, q
WHERE  a.asset_type = 'IMAGE'
ORDER BY dist
FETCH APPROXIMATE FIRST 20 ROWS ONLY;
```

#### 📌 详细解读

**核心思想**:
- 用户的文本"雨天高速前车急刹"先被 text-embedding-3-large 嵌入;
- 在图像库里用同样的 embedding 模型空间做距离计算;
- **前提**:图像也用同一个 CLIP/SigLIP 模型(它把文本、图像映射到同一向量空间)。

**注意**:
- 如果图像用 SigLIP,文本也必须用 SigLIP 的文本塔;
- 如果图像用 CLIP-OpenAI,文本只能用 CLIP 的文本分支,**不能混** OpenAI text-embedding-3;
- **工程经验**:跨模态检索的召回率,通常 60%~80% 取决于**模型选对没有**。

**典型应用**:
- 运营标注"找出和这段描述相似的所有摄像头画面";
- 测试团队上传文字描述,系统自动找对应数据;
- 数据集审查:用一句话"找出所有儿童横穿马路的场景"。

---

#### 3.6.3 文档 RAG:法规问答(原文)

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
  AND  JSON_VALUE(a.tags,'$.confidential') <= :user_clearance
ORDER BY dist
FETCH APPROXIMATE FIRST 8 ROWS ONLY;
```

#### 📌 详细解读

**这条 SQL 体现了 26ai 的"Converged"精髓**:

| 谓词 | 解决的工程问题 |
|---|---|
| `asset_type = 'DOC'` | 只在文档类型中检索 |
| `doc_type IN ('GB','UN_R','EU_Reg')` | 只召回法规类(去掉专利、手册) |
| `confidential <= :user_clearance` | **权限过滤**——智驾合规强约束 |

**机密分级模型**(原文 4.4):
- `public`(0) → 所有人都能查;
- `internal`(1) → 主机厂员工;
- `restricted`(2) → 仅法务、合规;
- `top_secret`(3) → 仅核心团队。

SQL 层的强制校验,比"应用层做权限判断"更安全——直接绕不开。

**RAG 流水线衔接**:
1. 上面 SQL 召回 Top-8 chunk;
2. 应用层用 `BGE-reranker-large` 做精排;
3. 把 Top-3 chunk 拼入 LLM Prompt;
4. LLM 生成答案 + 引用(`source_uri` 直接给到法规 URL)。

---

#### 3.6.4 语音检索(原文)

```sql
SELECT a.asset_id, a.source_uri,
       VECTOR_DISTANCE(a.vector, :query_vec, COSINE) AS dist
FROM   driving_assets a
WHERE  a.asset_type = 'AUDIO'
  AND  a.fleet_id  = :fleet_id
ORDER BY dist
FETCH APPROXIMATE FIRST 5 ROWS ONLY;
```

#### 📌 详细解读

**典型场景**:
- 车主说"打开座椅加热",系统找出历史相似指令(可能是"上次车主说同样话时的反应");
- 车队运营找出"这周最频繁的 5 条司机指令",作为产品改进输入;
- 安全审计:找出"包含敏感词的语音"。

**注意**:`:query_vec` 是 query 语音**经过 ASR + 嵌入**后的向量,不是声纹向量。

---

### 3.7 Re-rank 与 LLM 回答生成(原文)

```sql
SELECT a.source_uri || ' | ' || a.tags AS ctx
FROM   driving_assets a
ORDER  BY VECTOR_DISTANCE(a.vector, :q_vec, COSINE)
FETCH  APPROXIMATE FIRST 5 ROWS ONLY;
```

#### 📌 详细解读

**Re-rank 的必要性**:

| 阶段 | 召回数 | 方法 | 延迟 |
|---|---|---|---|
| 粗排 | 50~200 | 向量近似 + BM25 | 10~50 ms |
| 精排 | 5~20 | Cross-Encoder (BGE-reranker-large) | 100~500 ms |
| 生成 | 1 | LLM (vLLM / OCI GenAI) | 500~3000 ms |

**Cross-Encoder** 把"query+chunk"作为一个整体输入 BERT-style 编码器,精度远高于双塔向量,但慢很多。
所以**只对粗排 Top-50 做精排**,而不是全量。

**Prompt 模板示例**:
```
你是一个汽车智驾法规助手,基于以下上下文回答用户问题。
若上下文不相关,请回答"未找到对应信息",**禁止编造**。

上下文:
[1] GB/T 40429-2021 § 4.2 自动驾驶系统应满足 ...
[2] UN R157 § 5.1 关于 ALKS 的速度限制 ...
...

问题:用户问题
```

---

## 四、最佳实践(智驾专项)

### 4.1 数据建模(原文 5 条)

| # | 要点 | 工程意义 |
|---|---|---|
| 1 | 统一资产表 + 类型字段 | 跨模态 join、一次查询搞定 |
| 2 | 元数据驱动过滤 | 标签进 JSON,免 ALTER TABLE |
| 3 | 机密分级下沉到元数据 | 权限校验前置到 SQL 层 |
| 4 | 多向量字段 | 不同模态不同维度,避免精度损失 |
| 5 | 版本化嵌入模型 | 嵌入模型升级可灰度 |

#### 📌 详细解读

**第 5 条最容易被忽略**:
> 嵌入模型每次升级(如 SigLIP → SigLIP2),向量空间分布会变,**新向量和旧向量不能直接做距离**。

正确做法:
- 新加列 `vec_v2 VECTOR(1152)`;
- 双写一段时间;
- 离线对比新旧向量召回差异;
- 切流,删除旧列(物理保留一段时间)。

---

### 4.2 索引选型(原文表)

| 场景 | 推荐 | 理由 |
|---|---|---|
| 图像样本库(< 5000 万) | HNSW + COSINE | 召回优先,毫秒级 |
| 点云帧(亿级) | IVF + PQ | 内存可控,构建时间短 |
| 法规/技术文档 | HNSW + COSINE + 全文索引 | 文本检索 + 关键字 hybrid |
| 实时车机端 | IVF-PQ + INT8 二值量化 | 边缘内存、带宽受限 |
| 多车队共享 + 单租户隔离 | 独立 PDB + HNSW | 多租户隔离 + 调参灵活 |

#### 📌 详细解读

**车机端的特殊挑战**:
- 车机 CPU 通常 ARM、低功耗,内存 4~8 GB;
- 不能用全精度 FLOAT32 向量(1024 维 × 4 字节 = 4 KB/条,百万条就 4 GB);
- IVF-PQ + INT8 二值化:每条 ~64 字节,百万条仅 64 MB;
- 离线构建索引 + 序列化,OTA 推送到车机。

**多车队共享的取舍**:
- 单 PDB 共享表 + `fleet_id` 过滤:运维简单,但权限粒度粗;
- **每车队一个 PDB**:严格隔离,故障不传染,但运维复杂;
- **推荐折中**:核心车队独立 PDB,长尾车队共享 PDB + 严格 RLS(Row-Level Security)。

---

### 4.3 性能调优(原文 6 条)

| # | 要点 |
|---|---|
| 1 | 监控 `V$VECTOR_INDEX` / `V$VECTOR_MEMORY`,目标 ≥ 90% 命中 |
| 2 | Exadata Smart Scan 在向量侧收益有限,主要是标量剪枝 |
| 3 | `FETCH APPROXIMATE FIRST n WITH TARGET ACCURACY 90`,`n` 控制在 50~200 |
| 4 | HNSW 构建用 `PARALLEL` + Resource Manager 放低峰窗口 |
| 5 | INT8 量化内存 4 倍压缩,召回下降 < 1%;PQ 16~32 倍压缩 |
| 6 | True Cache 缓存 Top-K,RAG P99 显著下降 |

#### 📌 详细解读

**`V$VECTOR_MEMORY` 命中率怎么看?**

```sql
SELECT name, value FROM V$VECTOR_MEMORY_ADVICE;
-- 输出建议:扩 SGA、加 IM column store、或考虑量化
```

**Exadata 的真实作用**:
- Smart Scan 让 IO 在存储节点完成,适合**大表扫描 + 标量过滤**;
- 向量距离计算是 CPU bound,在存储节点算也帮不上忙;
- **真正收益**:50 km 地理围栏、时间窗过滤先在存储节点完成,只把候选向量返回给计算节点。

**HNSW 构建的真实成本**:
- 千万级图像(1024 维)+ TARGET ACCURACY 95 → 单实例构建可能 **数小时**;
- **必须**用并行 + Resource Manager 把构建放到周末;
- 期间不要对该表做 DML,否则可能需要重建。

---

### 4.4 安全与合规(原文 6 条)

| # | 要点 |
|---|---|
| 1 | 数据不出库:嵌入与 LLM 调用走 OCI GenAI 或本地 vLLM |
| 2 | 机密分级:EU R155/R156、UN R157 写入 `tags.confidential` |
| 3 | Unified Auditing:记录 `UTL_TO_EMBEDDING` / `UTL_TO_GENERATE_TEXT` |
| 4 | SQL Firewall:先 7 天基线再切 BLOCK |
| 5 | TDE + Key Vault:法规、语音、舱内对话列级加密 |
| 6 | 多租户:主机厂 / Tier-1 / 算法供应商每方一个 PDB |

#### 📌 详细解读

**第 1 条"数据不出库"是合规生死线**:
- 智驾数据多含地理坐标、车牌、人脸;
- 一旦发到外部 API(OpenAI、Azure),触发 GDPR / 个保法 / 数据出境;
- **方案 A**:用 OCI Generative AI(SOC2/ISO27001/数据驻留可控);
- **方案 B**:本地 vLLM + Qwen2.5 / Llama3 / DeepSeek;
- **方案 C**:Oracle Database 内的 ONNX 模型推理(完全不出库,适合敏感场景)。

**第 2 条智驾法规举例**:
- UN R157:ALKS(自动车道保持系统)速度上限 60 km/h;
- EU R155:网络安全管理体系;
- EU R156:软件更新管理体系;
- 这些条款机密等级通常 `restricted`,仅合规组可访问。

**第 6 条多租户的工程实现**:
```sql
-- Lockdown Profile 限制跨 PDB 操作
CREATE LOCKDOWN PROFILE t1_restrict FROM OLS_DEFAULT;
ALTER LOCKDOWN PROFILE t1_restrict DISABLE FEATURE = ("CONNECT","ADMIN","ALL");
-- 各合作方只能在自己 PDB 内做事
ALTER PDB t1_pdb ENABLE LOCKDOWN PROFILE t1_restrict;
```

---

### 4.5 RAG 端到端流水线(原文)

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

#### 📌 详细解读

**流式入库的关键路径**:

```
车辆 → 4G/5G → Kafka Topic
   ↓
Kafka Connect for Oracle (TxEventQ 桥接)
   ↓
DBMS_VECTOR_CHAIN 入库
   ↓
HNSW 增量更新(在线插入)
   ↓
供下游检索 / RAG
```

**冷热分层策略**:

| 数据年龄 | 存储位置 | 索引 |
|---|---|---|
| 0~30 天(热) | 主 PDB 生产表 | HNSW 全精度 |
| 30~180 天(温) | ADG 备库,只读 | IVF-PQ 量化 |
| 180 天+(冷) | Object Storage + 元数据索引 | 无,按需 attach |

---

### 4.6 自治化运维(原文)

- **Autonomous AI Database**:团队小、变更频繁时,直接上 Autonomous,自带 Auto Scaling、Auto Indexing;
- **Database Management (OCI)**:统一监控多套 26ai 实例;
- **Observability + Select AI**:DBA 用自然语言问"过去 1 小时向量检索最慢的 SQL 是什么"。

#### 📌 详细解读

**Select AI 在 DBA 场景的价值**:
```sql
SELECT DBMS_CLOUD_AI.GENERATE(
  prompt   => '找出过去 1 小时平均执行时间最长的 5 条向量检索 SQL,给出优化建议',
  profile  => 'DBA_PROFILE',
  action   => 'runsql'
);
-- 系统会自动生成 SQL → 执行 → 总结 → 给优化建议
```

**Auto Indexing 在向量场景的局限**:
- 23ai/26ai 的 Auto Indexing 主要针对 B-tree / Bitmap;
- **向量索引需要人工选型**(HNSW vs IVF);
- DBA 仍然要监控向量索引的命中率,手动重建或量化。

---

## 五、智驾场景落地 Checklist(原文)

| # | 任务 | 关键点 |
|---|---|---|
| 1 | 资产清单与脱敏 | PII / 隐私 / 法规密级统一标签 |
| 2 | 嵌入模型选型 | 图像 CLIP/SigLIP、文本 BGE/text-embedding-3、点云 PointNet++ 离线编码 |
| 3 | 表结构与索引 | 统一资产表 + HNSW/IVF 双轨 |
| 4 | 离线入库 + 实时入库 | 批量走 Spark/Glue,流式走 Kafka/TxEventQ |
| 5 | 检索 API 化 | 单一 SQL endpoint 返回 Top-K,权限过滤前置 |
| 6 | Re-rank + LLM 编排 | 跨模态场景用 Cross-Encoder;最终答案用 LLM 拼接 |
| 7 | 评测集 | 每条业务场景做 100+ 真实问题的人工标注,回归 |
| 8 | 安全合规 | TDE + SQL Firewall + Unified Auditing + DB Vault |
| 9 | 高可用 | ADG 承担向量检索读负载;True Cache 做边缘缓存 |
| 10 | 自治化 | 监控 + 自动索引 + 容量预测 |

---

## 六、关键 SQL 速查卡(原文)

| 用途 | SQL 片段 |
|---|---|
| 余弦相似度 | `VECTOR_DISTANCE(a.vector, b.vector, COSINE)` |
| 欧氏距离 | `VECTOR_DISTANCE(a.vector, b.vector, EUCLIDEAN)` |
| 点积(归一化后) | `VECTOR_DISTANCE(a.vector, b.vector, DOT)` |
| 近似 Top-K | `ORDER BY VECTOR_DISTANCE(...) FETCH APPROXIMATE FIRST 50 ROWS ONLY WITH TARGET ACCURACY 95;` |
| 创建 HNSW | `CREATE VECTOR INDEX ... ORGANIZATION INMEMORY NEIGHBOR GRAPH DISTANCE COSINE ...` |
| 创建 IVF | `CREATE VECTOR INDEX ... ORGANIZATION INMEMORY NEIGHBOR PARTITIONS ...` |
| 嵌入文本 | `UTL_TO_EMBEDDING(:text, JSON_OBJECT('model' VALUE 'text-embedding-3-large'))` |
| 凭据 | `DBMS_VECTOR_CHAIN.CREATE_CREDENTIAL(...)` |

---

## 七、推荐下一步行动(原文)

1. **小范围 POC**:50 万张摄像头图像 + 5 万段法规文本,跑完整流水线;
2. **基准对比**:HNSW vs IVF、不同量化、26ai vs Milvus / pgvector;
3. **跨模态验证**:同一段文本查询图像库和点云库,确认 CLIP/SigLIP 语义召回;
4. **合规对齐**:和法务 / 信安确认机密分级与跨境传输;
5. **集成进 MLOps**:26ai 作为特征/检索库接入数据闭环(采集 → 标注 → 训练 → 仿真)。

---

## 八、参考来源(原文)

- Oracle AI Database 26ai 主页:https://www.oracle.com/cn/database/ai-native-database-26ai/
- Oracle Database 26ai 文档总入口:https://docs.oracle.com/en/database/oracle/oracle-database/26/
- Oracle AI Vector Search User's Guide:https://docs.oracle.com/en/database/oracle/oracle-database/26/vecse/
- Oracle AI Vector Search SQL Guide(`VECTOR` / `VECTOR_DISTANCE`):https://docs.oracle.com/en/database/oracle/oracle-database/26/sqlrf/
- Oracle LiveLabs(动手实验):https://livelabs.oracle.com/
- Oracle Database Blog:https://blogs.oracle.com/database/

---

## 🎯 总结:整篇报告的"三句话"

1. **架构上**,Oracle 26ai 的"Converged"哲学让智驾的 6 类多模态资产(图像/点云/语音/视频/地图/文档)可以在同一个 PDB 内统一存储、索引、检索,SQL 同时表达向量 + 标量 + JSON + 空间 + 全文 + 图,事务一致性和合规审计一体达成。

2. **实战上**,通过 `VECTOR` 列 + HNSW/IVF-PQ 索引 + `VECTOR_DISTANCE` 函数 + `FETCH APPROXIMATE` 剪枝,可以在毫秒~百毫秒级完成跨模态相似场景挖掘、法规 RAG 问答、车队语音检索等核心智驾场景;关键工程要点是**标量过滤前置、嵌入模型与索引距离一致、跨模态使用统一模型族**。

3. **工程上**,智驾对 26ai 的真正考验不是"能不能存向量",而是**向量与机密分级、地理围栏、车型标签、跨租户隔离的 SQL 层一体化**——这恰恰是 26ai 相对独立向量数据库(Milvus / pgvector)的最强差异点。

---

## 文档对照表

| 文档 | 角色 | 适合谁读 |
|---|---|---|
| `01-oracle-26ai-overview.md` | 总报告(架构 / 核心特性 / 最佳实践) | 决策者、架构师、所有人 |
| `02-ai-vector-search-autonomous-driving.md` | 智驾子报告(原理 + SQL + 实践) | 智驾研发、DBA、应用工程师 |
| `03-ai-vector-search-autonomous-driving-explained.md`(本文) | 02 的逐节详解(为什么 + 权衡 + 坑) | 想深入理解工程权衡的开发者 |