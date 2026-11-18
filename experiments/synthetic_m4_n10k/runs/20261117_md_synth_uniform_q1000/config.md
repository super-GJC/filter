# run: 20261117_md_synth_uniform_q1000

| 字段 | 值 |
|------|----|
| 日期 | 2026-11-17 |
| 方法 | md |
| 数据集 | `experiments/synthetic_m4_n10k/datasets/synth_uniform_n10k/`（与 q100 相同） |
| 负载 | `experiments/synthetic_m4_n10k/workloads/synth_uniform_q1000/`（1000 条，未覆盖 q100） |
| d / logical_size / bitsperkey | `{16,256,64,16}` / `{2,16,16,4}` / 20 |
| 备注 | 独立新 run；不覆盖 `20261116_*_q100`。 |
