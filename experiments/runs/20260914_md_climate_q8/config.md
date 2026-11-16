# run: 20260914_md_climate_q8

| 字段 | 值 |
|------|----|
| 日期 | 2026-09-14 |
| 方法 | md（基线多维区域过滤器） |
| 数据集 | climate（`data_dir=./data/test/climate/`） |
| 工作负载 | climate_q8 |
| 工作负载路径 | `experiments/workloads/climate_q8/query.txt` |
| 本记录目录 | `experiments/runs/20260914_md_climate_q8/` |
| 原始输出子目录 | `raw/`（程序 `run_dir` 应指向此处） |
| bitsperkey | 20 |
| d | {4,1121,68,7} |
| logical_size | {2,16,16,4} |
| 入口 | `run_construct_rangefilter_compare_test` |
| 归档来源 | **本机运行**；原始结果自原 `md_climate_q8/` 迁入 `raw/` |
| filter.txt 字节 | 2613248（`raw/filter.txt`） |
| filter_workload.txt 字节 | 1605632 |
| binary1.txt 字节 | 1306624 |
| 备注 | 记录根目录只放元数据与统计；程序产物一律在 `raw/`。 |

## 目录布局

```
20260914_md_climate_q8/
├── config.md          ← 本文件
├── query.txt          ← 负载快照
├── summary.csv        ← 聚合统计
└── raw/               ← 程序直接写出的原始结果
    ├── result.txt
    ├── result_workload.txt
    ├── filter.txt / filter_workload.txt
    ├── offset.txt / offset_workload.txt
    ├── binary1.txt
    └── append_cursor.txt
```
