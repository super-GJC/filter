# run: 20260917_surf_1d_and_climate_q200

| 字段 | 值 |
|------|----|
| 日期 | 2026-09-17 |
| 方法 | surf_1d_and（每 chunk 各维 SuRF，全部通过才保留） |
| 数据集 | climate（`data/test/climate/data.txt` 只读） |
| 工作负载 | climate_q200 |
| 工作负载路径 | `experiments/workloads/climate_q200/query.txt` |
| 本记录目录 | `experiments/runs/20260917_surf_1d_and_climate_q200/` |
| 原始输出 | `raw/result.txt`（自 `bench/surf_1d/output/result_and.txt`） |
| 入口 | `bench/surf_1d/adapter/climate_surf_bench --mode and` |
| d | {4,1121,68,7} |
| logical_size | {2,16,16,4} |
| SuRF | real_suffix=8 |
| filter_bytes | 1004853（累计所有维，约维数倍于 best） |
| 备注 | chunk 级 FPR，与 MD `process_Queries` 对齐；计时为适配层内存扫描，不可与 MD 页 I/O 计时直接比 |

## 本目录应包含

- [x] `config.md`
- [x] `query.txt`（200 条快照）
- [x] `summary.csv`
- [x] `raw/result.txt`
