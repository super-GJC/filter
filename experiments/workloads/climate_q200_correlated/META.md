# workload: climate_q200_correlated

| 字段 | 值 |
|------|----|
| 数据集 | climate |
| 查询条数 | 200 |
| 来源 | `experiments/workloads/gen/gen_climate_queries.py` |
| 用途 | 可选压力测：查询围绕真实元组扩展，命中率更高 |
| 生成模式 | `correlated` |
| 混合比例 | N/A（纯模式） |
| 各维选择性目标区间 | dim0=[0.25,0.75], dim1=[0.05,0.55], dim2=[0.10,0.50], dim3=[0.15,1.00] |
| 实际平均选择性 | 0.495, 0.308, 0.308, 0.585 |
| 随机种子 | 20260917 |
| 域基数 d | {4, 1121, 68, 7} |
| 锚点数据 | `data/test/climate/data.txt`（只读，仅 correlated 用） |
| 生成日期 | 2026-09-16 |
| 非空命中查询数 | 200 / 200 |
| 命中元组数 median / mean | 99.5 / 133.72 |

`query.txt` 每行格式：`L0 H0 L1 H1 L2 H2 L3 H3`（闭区间，与 `loadQuery` 一致）。

重新生成（在仓库根目录）：

```bash
python3 experiments/workloads/gen/gen_climate_queries.py --write-defaults
```
