# run: 20260916_md_climate_q200_mixed

| 字段 | 值 |
|------|----|
| 日期 | 2026-09-16 |
| 方法 | md（基线多维区域过滤器） |
| 数据集 | climate（`data_dir=./data/test/climate/`） |
| 工作负载 | climate_q200_mixed |
| 工作负载路径 | `experiments/workloads/climate_q200_mixed/query.txt` |
| 本记录目录 | `experiments/runs/20260916_md_climate_q200_mixed/` |
| 程序 run_dir | `.../20260916_md_climate_q200_mixed/raw/` |
| bitsperkey | 20 |
| d | {4,1121,68,7} |
| logical_size | {2,16,16,4} |
| 入口 | `run_construct_rangefilter_compare_test` |
| filter.txt 字节 | 2613248（`raw/filter.txt`） |
| filter_workload.txt 字节 | 2568192 |
| 备注 | summary 已按 `raw/result.txt` 聚合；21 条 FPR=nan（chunklist 空）未计入均值/中位数。 |

## 本目录应包含

- [x] `config.md`（本文件）
- [x] `query.txt`（本次查询完整副本，200 条）
- [x] `summary.csv`（已填）
- [x] `raw/`（程序输出：result / filter / offset / binary 等）
