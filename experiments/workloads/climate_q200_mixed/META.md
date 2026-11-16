# workload: climate_q200_mixed

| 字段 | 值 |
|------|----|
| 数据集 | climate |
| 查询条数 | 200 |
| 来源 | `experiments/workloads/gen/gen_climate_queries.py` |
| 用途 | **主实验负载**：降低单条 FPR 波动；均匀 + 相关各半 |
| 生成模式 | `mixed` |
| 混合比例 | uniform 50% + correlated 50% |
| 各维选择性目标区间 | dim0=[0.25,0.75], dim1=[0.05,0.55], dim2=[0.10,0.50], dim3=[0.15,1.00] |
| 实际平均选择性 | 0.505, 0.313, 0.301, 0.575 |
| 随机种子 | 20260916 |
| 域基数 d | {4, 1121, 68, 7} |
| 锚点数据 | `data/test/climate/data.txt`（只读，仅 correlated 用） |
| 生成日期 | 2026-09-16 |
| 非空命中查询数 | 133 / 200 |
| 命中元组数 median / mean | 14.0 / 65.63 |

`query.txt` 每行格式：`L0 H0 L1 H1 L2 H2 L3 H3`（闭区间，与 `loadQuery` 一致）。

重新生成（在仓库根目录）：

```bash
python3 experiments/workloads/gen/gen_climate_queries.py --write-defaults
```
