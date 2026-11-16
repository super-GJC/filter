# run: 20260916_md_climate_q200

| 字段 | 值 |
|------|----|
| 日期 | 2026-09-16 |
| 方法 | md |
| 数据集 | climate（`data_dir=./data/test/climate/`） |
| 工作负载 | climate_q200（主实验，FPR≤0.1 导向） |
| 工作负载路径 | `experiments/workloads/climate_q200/query.txt` |
| 本记录目录 | `experiments/runs/20260916_md_climate_q200/` |
| 程序 run_dir | `.../20260916_md_climate_q200/raw/` |
| bitsperkey | 20 |
| d | {4,1121,68,7} |
| logical_size | {2,16,16,4} |
| 备注 | summary / query / raw/result.txt 绑定同一次筛选跑批抽取 |

## 本目录应包含

- [x] `config.md`
- [x] `query.txt`（200 条）
- [x] `summary.csv`
- [x] `raw/`
