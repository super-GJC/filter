# synthetic compare notes

- 数据：独立均匀合成，`n=10000`，`d={16,256,64,16}`，`logical_size={2,16,16,4}`（`allmranges=2^20`，与 climate 同级）。
- 负载：100 条未筛选满 4 维均匀矩形查询。
- 口径：chunk 级 FPR；MD 用基线 `result.txt`；SuRF 与 MD 的 overlap/border 已校验一致。
- 计时不可跨 MD vs SuRF 直接比（I/O vs 内存）。

## q1000 (2026-11-17)
- 新负载 `synth_uniform_q1000`（1000 条），独立 run `20261117_*_q1000`，未覆盖 q100。
- 见 `compare/fpr_q1000_by_method.csv`。
