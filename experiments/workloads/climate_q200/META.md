# workload: climate_q200

| 字段 | 值 |
|------|----|
| 数据集 | climate |
| 查询条数 | 200 |
| 用途 | **主实验负载** |
| 来源 | 在随机混合负载基础上，按 MD 基线实测 FPR≤0.1 筛选并补全 |
| 筛选绑定 run | `experiments/runs/20260916_md_climate_q200/`（同次结果抽取归档） |
| 对照负载 | `climate_q200_mixed`（未筛选的随机混合）、`climate_q8` |
| 生成日期 | 2026-09-16 |

`query.txt` 每行格式：`L0 H0 L1 H1 L2 H2 L3 H3`（闭区间，与 `loadQuery` 一致）。

写论文时须说明：本主负载经 MD 实测 FPR 筛选；随机未筛选版本见 `climate_q200_mixed`。
