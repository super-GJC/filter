# run: 20260917_surf_1d_and_climate_q200_mixed

| 字段 | 值 |
|------|----|
| 日期 | 2026-09-17 |
| 方法 | surf_1d_and（and（各维 SuRF 全通过才保留）） |
| 数据集 | climate |
| 工作负载 | climate_q200_mixed（未按 MD FPR 筛选的随机混合） |
| 工作负载路径 | `experiments/workloads/climate_q200_mixed/query.txt` |
| 本记录目录 | `experiments/runs/20260917_surf_1d_and_climate_q200_mixed/` |
| 原始输出 | `raw/result.txt` |
| 入口 | `bench/surf_1d/adapter/climate_surf_bench --mode and` |
| d / logical_size | {4,1121,68,7} / {2,16,16,4} |
| SuRF | real_suffix=8 |
| filter_bytes | 1004853 |
| n_fpr_eq_0 / finite | 7 / 194（frac=0.0360825） |
| 备注 | 与 MD `20260916_md_climate_q200_mixed` 同负载对照 |

## 本目录应包含

- [x] `config.md` / `query.txt` / `summary.csv` / `raw/result.txt`
