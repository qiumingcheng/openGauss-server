# Oracle Database 26ai（AI 原生数据库）综合总结

> ⚠️ **说明**：本次研究中的 WebFetch 和 WebSearch 工具均不可用（Oracle 官方域名被网络安全策略拦截，WebSearch API 返回 400 错误）。以下内容基于模型训练知识（截止 2026 年 1 月）撰写，并参考了 [Oracle 26ai 官方页面](https://www.oracle.com/cn/database/ai-native-database-26ai/) 的主题框架。涉及具体版本号、GA 日期、定价等可能存在时效偏差，建议以 Oracle 官方最新文档为准。

---

## 一、架构（Architecture）

### 1.1 定位与演进
Oracle Database 26ai 是 Oracle 在 2024 年推出 **23ai** 后的下一代长期支持版本，从 23ai 的 "AI for Data" 进一步演进为 **"AI Native Database"**，将 AI 能力从"附加功能"提升为数据库内核的一等公民。它延续了 Oracle 多版本架构的演进：19c → 21c → 23ai → 26ai。

### 1.2 整体技术栈分层
26ai 采用 **"一体化（Converged）数据库"** 的核心设计思想，将多种数据模型与 AI 能力集成在单一引擎中：

| 层级 | 关键组件 |
|---|---|
| **存储层** | 传统行/列存储、内存列存（IM column store）、JSON、Graph（Spatial & Graph）、时序（Time Series）、区块链表 |
| **AI 内核层** | AI Vector Search 向量索引（HNSW / IVF）、ONNX Runtime 嵌入模型服务、Select AI 自然语言查询 |
| **计算层** | 多租户容器数据库（CDB/PDB）、RAC 并行集群、Sharding 分片 |
| **自治与运维层** | AutoML、自动索引、SQL 计划管理、Exadata 智能扫描 |
| **安全层** | Database Vault、Transparent Data Encryption (TDE)、Oracle Key Vault、SQL Firewall |

### 1.3 多租户架构（Multitenant）
- **CDB（容器数据库）/ PDB（可插拔数据库）**：单实例多租户仍是基础单元。26ai 在 23ai 基础上增强了 **READ-ONLY PDB 用户** 和 **动态 PDB 资源管理**，支持更细粒度的多租户隔离。
- **克隆与迁移**：支持热克隆（Hot Clone）、刷新克隆（Refreshable Clone）以及跨 CDB 的 PDB 搬迁。

### 1.4 AI Vector Search 的存储与索引
- **向量列类型 `VECTOR`**：直接作为表列存储，原生支持 `VECTOR( dims, format )`。
- **向量索引**：
  - **HNSW（Hierarchical Navigable Small World）**：基于图的近似最近邻（ANN）算法，适合高召回、低延迟场景。
  - **IVF（Inverted File Flat）**：基于聚类的分区索引，适合超大规模数据集。
- **向量与关系数据的融合检索**：通过 SQL 直接做 `WHERE ... ORDER BY VECTOR_DISTANCE(...)`，可在同一查询中结合标量过滤、JSON 路径、全文检索。

### 1.5 部署形态
- **On-Premises**：Exadata、ODA（Oracle Database Appliance）、通用服务器。
- **Oracle Database Cloud Service**。
- **Autonomous Database**：Autonomous AI Database（基于 26ai 内核的全自治服务）。
- **Oracle Database@Azure / @Google / @AWS**：跨云部署选项。

---

## 二、核心特性功能（Core Features & Functions）

### 2.1 AI Vector Search（AI 向量检索）
- **核心能力**：存储、索引、检索高维嵌入向量。
- **典型应用**：RAG（Retrieval-Augmented Generation，检索增强生成）、语义搜索、推荐系统、图像/文本相似度匹配、欺诈检测。
- **新功能（vs 23ai）**：
  - 更成熟的 **HNSW 索引** 与并行构建；
  - 支持 **量化（Quantization）**，压缩向量内存占用；
  - 与 **Graph RAG**（结合知识图谱的检索增强）原生集成；
  - 改进的 **向量 + 结构化数据的混合检索** 优化器。

### 2.2 Select AI（自然语言查询）
- 通过自然语言生成 SQL 或调用 LLM 进行数据分析。
- 支持用户自带 LLM API（OpenAI、Azure OpenAI、Cohere 等）或使用 OCI Generative AI。
- 集成 RAG：可直接对 PDB 内的业务数据做 NL2SQL + 检索增强问答。

### 2.3 AI for Data（数据智能化能力）
- **AutoML / AutoML UI**：在数据库内做特征工程、模型训练、评估、部署，无需将数据导出。
- **ONNX 模型原生推理**：通过 `DBMS_DATA_MINING` 或 `OML4SQL` 直接对 ONNX 格式模型做推理。
- **模型管理**：模型版本化、A/B 测试、漂移检测。

### 2.4 JSON Relational Duality Views（JSON 关系二元性视图）
- **核心思想**：将同一份底层关系数据同时以 **关系表** 和 **JSON 文档** 形式暴露，应用可以任选范式（Document 或 Relational）。
- **可写 Duality View**：可直接对视图做 `INSERT/UPDATE/DELETE`，由数据库自动同步到基础关系表。
- **收益**：
  - 微服务/移动端可直接消费 JSON，无需 ORM 映射；
  - 关系模型的一致性与约束仍被保留；
  - 减少数据复制与同步链路。

### 2.5 Graph 数据库能力（Spatial & Graph）
- 图数据模型（PGQL、Property Graph）；
- 与关系表、向量数据融合查询；
- 26ai 增强了 **图 + 向量** 混合推理能力（GraphRAG）。

### 2.6 安全性增强
- **SQL Firewall**：基于白名单/黑名单的 SQL 语句级防护。
- **Database Vault**：强制执行职责分离（SoD）。
- **TDE 与 Always Encrypted**：列级加密。
- **新功能**：26ai 在 23ai 基础上进一步强化了 **身份与访问管理（IAM）** 与 **零信任** 模型集成。

### 2.7 性能与可扩展性
- **True Cache**：内存一致性缓存层（23ai 引入，26ai 增强）。
- **Automatic Indexing**：自动创建、验证、删除索引。
- **SQL Plan Management + SPM Evolve Advisor**：自动演进执行计划。
- **Exadata 智能特性**：Smart Scan、Hybrid Columnar Compression、Persistent Memory（PMEM）支持。

### 2.8 可观测性
- **Database Observability** 仪表盘（基于 OMC / OCI Database Management）；
- **Real-Time SQL Monitoring**；
- 增强了 **AI 驱动的根因分析**。

---

## 三、最佳实践（Best Practices）

### 3.1 升级与迁移
1. **评估路径**：使用 **AutoUpgrade** 工具链进行升级评估（19c → 26ai / 23ai → 26ai）。
2. **兼容性矩阵**：升级前通过 `DBUA` 或命令行预检，重点关注已弃用特性。
3. **测试顺序**：开发 → 测试 → 预生产 → 生产，每阶段执行完整回归与性能基线对比。
4. **回滚预案**：保留升级前的 RMAN 备份与可回滚 PDB 克隆。

### 3.2 AI Vector Search 部署最佳实践
1. **维度选择**：选择 LLM 模型对应的 embedding 维度（如 OpenAI text-embedding-3 = 1536/3072）。
2. **索引选择**：
   - 数据量 < 100 万：HNSW 优先（低延迟、高召回）；
   - 数据量 > 1000 万：IVF 或 IVF+HNSW 混合，配合量化压缩。
3. **混合检索**：始终使用 **"向量 + 标量过滤 + BM25 全文"** 的混合召回。
4. **RAG 架构**：
   - **分块策略**：按语义切块（512~1024 tokens，重叠 10%~20%）；
   - **元数据注入**：把业务字段（时间、地区、权限标签）写入 chunk 元数据；
   - **重排序（Re-rank）**：用 Cross-Encoder 或 LLM 对 Top-K 结果重排。
5. **性能调优**：调整 PGA / SGA，Exadata 上启用 Smart Scan offload，监控 `V$SQL` 中的 `VECTOR_DISTANCE` 调用耗时。

### 3.3 JSON Relational Duality Views 最佳实践
1. **适用场景**：微服务架构、移动端 API、内容管理、电商目录等需要 JSON 灵活性的场景。
2. **设计原则**：关系约束（PK/FK/CHECK）放在基础表上，Duality View 只做投影；嵌套深度建议 ≤ 3 层。
3. **权限隔离**：为 API 服务单独授予视图级权限。

### 3.4 多租户与资源管理
1. **PDB 隔离策略**：关键业务用独立 PDB；同一业务的不同环境用 **READ-ONLY Clone PDB**；启用 **Lockdown Profile**。
2. **资源管理**：使用 **Resource Manager** 设定 PDB 级别的 CPU、IO、并行度配额。

### 3.5 安全合规
1. **最小权限**：每个应用账号授予所需的最小 DML 权限；
2. **TDE 必开**：所有 PDB 启用 Transparent Data Encryption；
3. **SQL Firewall 部署**：先观察模式（`RECORD ONLY`），基线建立后切到 `BLOCK`；
4. **审计**：启用 **Unified Auditing**；
5. **Database Vault**：对 DBA、Application Owner 启用职责分离。

### 3.6 高可用与灾备
1. **Data Guard**：根据 RPO 选 SYNC / ASYNC；
2. **Active Data Guard**：备库可承担只读查询与 AI 向量检索负载；
3. **跨区域灾备**：使用 **Far Sync**；
4. **RMAN + ZDLRA**：关键业务保护 RPO≈0；
5. **True Cache**：用于分布式读扩展。

### 3.7 应用开发最佳实践
1. **数据访问**：使用 Connection Pool、绑定变量、长事务拆分。
2. **AI 集成**：用 **Select AI** 或 **DBMS_VECTOR** 包实现 RAG；敏感数据先**脱敏**再发送给 LLM。
3. **监控**：启用 AWR + SQL Tuning Advisor；AI 特性单独监控。

### 3.8 自治与运维
- **Autonomous Database**：推荐迁移到 **Autonomous AI Database**（基于 26ai 的全自治服务）。
- **Database Management（OCI）**：统一监控多套 26ai 实例。

---

## 四、风险与注意事项

| 风险 | 说明 |
|---|---|
| **厂商锁定** | Oracle 特有特性迁移到其他数据库成本高 |
| **许可成本** | AI 特性的额外许可需要确认 |
| **AI 自托管复杂度** | 自带 LLM（BYO-LLM）涉及密钥管理、网络出站合规、向量嵌入成本 |
| **新版本成熟度** | 26ai 是新代次版本，建议大型核心系统等待 1-2 个 RU 补丁集后再生产部署 |

---

## 五、参考来源（Sources）

由于当前环境 WebFetch/WebSearch 受限，以下页面建议您手动查阅：

- Oracle 官方：[Oracle AI Database 26ai](https://www.oracle.com/cn/database/ai-native-database-26ai/)
- Oracle Database 26ai 文档：[docs.oracle.com/en/database/oracle/oracle-database/26/](https://docs.oracle.com/en/database/oracle/oracle-database/26/)
- Oracle Database 26ai 升级指南：[Upgrade Guide](https://docs.oracle.com/en/database/oracle/oracle-database/26/upgrd/)
- Oracle AI Vector Search 用户指南：[Vector Search User's Guide](https://docs.oracle.com/en/database/oracle/oracle-database/26/vecse/)
- Oracle LiveLabs：[livelabs.oracle.com](https://livelabs.oracle.com/)
- Oracle Database Blog：[blogs.oracle.com/database](https://blogs.oracle.com/database/)

---

## 六、推荐下一步行动

1. **动手实验**：在 Oracle LiveLabs 找一个 26ai 的 free workshop 跑一遍。
2. **免费试用**：OCI 上开通 Always Free Autonomous AI Database。
3. **概念验证（POC）**：选定一个 RAG 场景做 POC。
4. **升级评估**：对现有 19c/23c 环境运行 AutoUpgrade 评估清单。