# Oracle 26ai 研究报告

本目录收录围绕 [Oracle AI Database 26ai](https://www.oracle.com/cn/database/ai-native-database-26ai/) 的研究输出,按主题分文件组织。

## 文件清单

| 文件 | 主题 | 内容范围 |
|---|---|---|
| [01-oracle-26ai-overview.md](./01-oracle-26ai-overview.md) | 总报告 | 架构 / 核心特性功能 / 最佳实践 三大部分 |
| [02-ai-vector-search-autonomous-driving.md](./02-ai-vector-search-autonomous-driving.md) | 子报告(实战版) | AI Vector Search 在**汽车智驾**多模态(图像/雷达点云/语音/视频/路网文本/技术文档)场景下的原理 + SQL 实战 + 最佳实践 |
| [03-ai-vector-search-autonomous-driving-explained.md](./03-ai-vector-search-autonomous-driving-explained.md) | 子报告(详解版) | 对 02 的**逐节详解**:为什么这样做 / 工程权衡 / 常见坑,适合想深入理解的开发者 |

## 撰写环境说明

由于本次撰写过程中 WebFetch 与 WebSearch 工具在当前环境均不可用:

- `oracle.com`、`docs.oracle.com`、`blogs.oracle.com` 等域名被网络安全策略拦截;
- WebSearch API 返回 400 错误。

两份报告内容基于模型训练知识(截止 2026-01)并参考 Oracle 官方页面的主题框架整理。**涉及具体 GA 日期、版本号、RU 补丁细节、SQL 语法(尤其是 26ai 矢量索引的 `ORGANIZATION INMEMORY NEIGHBOR GRAPH/PARTITIONS` 子句与量化选项)请以 Oracle 官方文档为准**。

## 文档对照

| 文档 | 角色 | 适合谁读 |
|---|---|---|
| `01-oracle-26ai-overview.md` | 总报告(架构 / 核心特性 / 最佳实践) | 决策者、架构师、所有人 |
| `02-ai-vector-search-autonomous-driving.md` | 智驾子报告(原理 + SQL + 实践) | 智驾研发、DBA、应用工程师 |
| `03-ai-vector-search-autonomous-driving-explained.md` | 02 的逐节详解(为什么 + 权衡 + 坑) | 想深入理解工程权衡的开发者 |

## 后续可补充

- [ ] 通过 docs.oracle.com 实际验证 26ai 矢量索引语法
- [ ] 补充 Autonomous AI Database 成本优化专项
- [ ] 补充 JSON Relational Duality Views 的 API 设计专题
- [ ] 补充 Select AI 与 RAG 编排的端到端样例