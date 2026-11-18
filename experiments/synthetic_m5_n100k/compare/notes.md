# synthetic_m5_n100k compare notes

- 数据：独立均匀，`M=5`，`n=100000`，`d={16,64,32,16,16}`，`logical_size={4,4,4,4,4}`（`allmranges=2^20`）。
- 负载：100 条未筛选满 5 维均匀矩形查询。
- 口径：chunk 级 FPR；MD 用基线 `result.txt`；SuRF 与 MD 的 overlap/border 已校验一致。
- 计时不可跨 MD vs SuRF 直接比。
- 空间未对齐：MD 过滤器约 237MB，显著大于 SuRF；写结论时需注明。

## q1000 (2026-11-17)
- 新负载 `synth_uniform_q1000`（1000 条），独立 run `20261117_*_q1000`，未覆盖 q100。
- 见 `compare/fpr_q1000_by_method.csv`。
