# m5 文档式实验：选择度 + 维度消融

- 基负载：`workloads/synth_uniform_q1000/`（1000 条）
- 数据：`datasets/synth_uniform_n100k/`，`d={16,64,32,16,16}`，`logical_size={4,4,4,4,4}`，`bitsperkey=20`
- SuRF：`real-suffix=8`
- MD 重复：每点 1 次
- `k5` / `m1p0` 与已归档 q1000 相同，直接复用，不重跑

汇总见 `../compare_ext/`。