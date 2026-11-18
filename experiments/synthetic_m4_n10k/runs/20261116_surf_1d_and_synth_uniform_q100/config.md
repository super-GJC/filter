# run: 20261116_surf_1d_and_synth_uniform_q100

| 字段 | 值 |
|------|----|
| 日期 | 2026-11-16 |
| 方法 | surf_1d_and |
| 数据集 | `experiments/synthetic_m4_n10k/datasets/synth_uniform_n10k/` |
| 负载 | `experiments/synthetic_m4_n10k/workloads/synth_uniform_q100/` |
| d | `{16,256,64,16}` |
| logical_size | `{2,16,16,4}` |
| real_suffix | 8 |
| 命令 | 同 best，一次 `--mode both` 产出 |
| 备注 | overlap/border 与 MD 逐行一致（mismatch=0）。 |
