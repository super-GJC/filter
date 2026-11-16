# run config（复制为 `../YYYYMMDD_<method>_<workload>/` 后填写）

| 字段 | 值 |
|------|----|
| 日期 | YYYY-MM-DD |
| 方法 | md / surf_1d_best / surf_1d_and / snarf_1d_best / … |
| 数据集 | climate |
| 工作负载 | climate_q8 / climate_q200 / … |
| 工作负载路径 | `experiments/workloads/<name>/query.txt` |
| 本记录目录 | `experiments/runs/YYYYMMDD_<method>_<workload>/` |
| 程序 run_dir | `.../YYYYMMDD_<method>_<workload>/raw/` |
| bitsperkey / 空间预算 | |
| 其它参数 | logical_size、是否 workload 维选择等 |
| 可执行文件 / 命令 | |
| 备注 | |

## 本目录应包含

- [ ] `config.md`（本文件，记录根目录）
- [ ] `query.txt`（本次查询完整副本，记录根目录）
- [ ] `summary.csv`（聚合一行；字段见下）
- [ ] `raw/`（程序输出：result / filter / offset / binary 等）

## summary.csv 字段说明（延用）

| 列名 | 含义 |
|------|------|
| avg_fpr / median_fpr | 各查询 FPR 的均值 / 中位数（仅对有限 FPR；nan 不计） |
| avg_filter_time_s | 过滤时间均值（秒），对应 `raw/result.txt` 中 filtertime |
| avg_process_time_s | 扫描/处理时间均值（秒），对应 processtime |
| filter_bytes | 过滤器文件字节数（通常 `raw/filter.txt`） |
| n_fpr_finite | 有限 FPR 的查询条数（排除 chunklist 空导致的 nan） |
| n_fpr_eq_0 | 其中 FPR 恰好为 0 的条数（辅助指标） |
| frac_fpr_eq_0 | `n_fpr_eq_0 / n_fpr_finite` |

不对 overlap / border / nempty 求平均（逐查询细节只留在 `raw/result.txt`）。
主结论仍看 avg/median FPR 与空间；`n_fpr_eq_0` / `frac_fpr_eq_0` 为辅助证据。
