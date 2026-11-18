# run: 20261117_md_synth_m5_n100k_q1000

| 字段 | 值 |
|------|----|
| 日期 | 2026-11-17 |
| 方法 | md |
| 数据集 | 同 q100：`datasets/synth_uniform_n100k/` |
| 负载 | `workloads/synth_uniform_q1000/`（1000 条；不覆盖 q100） |
| d / logical_size / bitsperkey | `{16,64,32,16,16}` / `{4,4,4,4,4}` / 20 |
| 备注 | 新 run 目录；不覆盖 `20261116_*_q100`。 |
