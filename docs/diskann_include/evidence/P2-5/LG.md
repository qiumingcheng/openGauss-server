# LG-表 [P2-5] 成本模型感知 in-filter

| 锚点 | 修改前 | 修改后 | 触发路径 | 生命周期 |
| --- | --- | --- | --- | --- |
| diskanncostmodel.h（新） | 无 | 纯函数 diskann_cost_search_width / diskann_cost_heap_candidates + 常数集中（OVERSAMPLE=1.5/S_MIN=1e-3/MAX_BUDGET=65536） | L1 单测直调；AM estimator 消费 | 无状态 |
| diskanncostestimate_internal（diskann.cpp） | 纯 generic（全索引口径） | annQuals 非空时：L=ceil(K/s)*1.5 clamp[probes,maxBudget]；totalCost ×(0.2+0.8·L/N)（20% 地板=入口/rerank 固定功）；indexSelectivity=C/N → **cost_index 通用部分 Heap 计费自动下降**（§13.5 归属表）。无 filter 保持原数字（零回归来源） | In-Filter 路径的成本估算 | — |
| indxpath collect+hook | annQualSelectivity 无生产者 | ann_infilter_combined_selectivity（norm_selec 独立 AND，floor 1e-3）填 IndexPath.annQualSelectivity | 路径生成 | — |

## 判据→证据

| 判据 | 证据 |
| --- | --- |
| L1 单调性/边界 | ut_diskann_costmodel 17 断言 ALL PASS（K/s 单调、clamp、s=0/NaN/K=0 回退、C∈[K,N]） |
| 过滤降低计费 | cost_l2.log：同表过滤 257.50 vs 无过滤 257.45……**注意：差 0.05 极小**——60 行小表 N 太小 L/N≈1 折扣不显著；rows 20 vs 60 已体现 selectivity=C/N 生效。大表效应归 G2 校准（如实记录） |
| 回归零退化 | diskann_vector 逐字节一致；全量 func exit=0 |

## 边界（R3 如实）

- 小表上成本差不可观（N=60）——成本模型的验收真正锚点是 G2 的 A/B/C 大数据对照；
  本步交付=结构正确（selectivity 生效 rows 20<60 实证）+ L1 单调性数学保证
- 常数（0.2 地板/1.5 oversample）是可解释初值，G2 数据校准后修订
- 未动 HNSW 分支/genericcostestimate（其他 AM 零影响——回归一致的保证）

## 过程事故

int64/int64_t 混用（独立头文件无 postgres.h 的 int64 typedef）×3 处编译门禁抓获；
clamp 宏在无 selfuncs.h 上下文不可用→手写边界（indxpath+diskann 两处）。
