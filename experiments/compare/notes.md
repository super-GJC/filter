# compare/

从 `experiments/runs/*/summary.csv` 汇总后填写下方 CSV。  
只放跨方法对比，不放单次原始逐查询结果。

## 当前计划对比

1. MD（多维区域过滤器） vs SuRF 1D-best / 1D-AND（同 workload）
2. 先用 `climate_q8` 打通流程，主结论用 `climate_q200`

## 指标说明

- 主指标：`avg_fpr` / `median_fpr`，以及 `space_by_method.csv` 中的 `filter_bytes`
- 辅助：`n_fpr_eq_0`、`frac_fpr_eq_0`（有限 FPR 中恰好为 0 的条数/比例）；nan 不计入分母

## 参数敏感性汇总（中文表头）

- `summary_chunksize_fpr_space.csv`：不同块大小（`logical_size`）下 MD / SuRF_1d_BEST / SuRF_1d_AND 的 平均FPR / 中位数FPR / 占用空间。
- `summary_bitsperkey_fpr_space.csv`：不同 `bits per key` 下三者同上指标（SuRF 不随 bpk 变，故其在各行恒定，作参照）。
- 原始数据：`experiments/runs/20261110_sensitivity_climate_q200/`（见其 `config.md`）。
- **口径**：SuRF 为确定性结构，平均/中位数直接来自归档逐查询结果；**MD 的 FPR 有运行间非确定性**（读页 `Can not seek!` 未初始化内存 bug），故「平均FPR」取多次重复均值（见 `sensitivity_*.csv` 的 mean/min/max），「中位数FPR」取代表性单次（稳定，多为 0）。
