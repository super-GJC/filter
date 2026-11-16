# run: 20260917_surf_1d_best_climate_q200

| 字段 | 值 |
|------|----|
| 日期 | 2026-09-17 |
| 方法 | surf_1d_best（每 chunk 仅一维 SuRF） |
| 数据集 | climate（`data/test/climate/data.txt` 只读） |
| 工作负载 | climate_q200 |
| 工作负载路径 | `experiments/workloads/climate_q200/query.txt` |
| 本记录目录 | `experiments/runs/20260917_surf_1d_best_climate_q200/` |
| 原始输出 | `raw/result.txt`（自 `bench/surf_1d/output/result_best.txt`） |
| 入口 | `bench/surf_1d/adapter/climate_surf_bench --mode best` |
| d | {4,1121,68,7} |
| logical_size | {2,16,16,4} |
| SuRF | real_suffix=8；best_dim=auto（每 chunk distinct 最多维） |
| filter_bytes | 255666（仅累计各 chunk 的 best 维） |
| 备注 | chunk 级 FPR，与 MD `process_Queries` 对齐；计时为适配层内存扫描，不可与 MD 页 I/O 计时直接比 |

## 本目录应包含

- [x] `config.md`
- [x] `query.txt`（200 条快照）
- [x] `summary.csv`
- [x] `raw/result.txt`
