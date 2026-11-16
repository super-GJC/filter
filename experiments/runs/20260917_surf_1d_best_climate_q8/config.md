# run: 20260917_surf_1d_best_climate_q8

| 字段 | 值 |
|------|----|
| 日期 | 2026-09-17 |
| 方法 | surf_1d_best（best（每 chunk 仅一维）） |
| 数据集 | climate |
| 工作负载 | climate_q8 |
| 工作负载路径 | `experiments/workloads/climate_q8/query.txt` |
| 本记录目录 | `experiments/runs/20260917_surf_1d_best_climate_q8/` |
| 原始输出 | `raw/result.txt` |
| 入口 | `bench/surf_1d/adapter/climate_surf_bench --mode best` |
| d / logical_size | {4,1121,68,7} / {2,16,16,4} |
| SuRF | real_suffix=8; best_dim=auto |
| filter_bytes | 255666 |
| n_fpr_eq_0 / finite | 3 / 8（frac=0.375） |
| 备注 | 与 MD `20260914_md_climate_q8` 同负载对照 |

## 本目录应包含

- [x] `config.md` / `query.txt` / `summary.csv` / `raw/result.txt`
