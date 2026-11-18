# run: 20261116_md_synth_m5_n100k_q100

| 字段 | 值 |
|------|----|
| 日期 | 2026-11-16 |
| 方法 | md（基线 filter.txt / result.txt） |
| 数据集 | `experiments/synthetic_m5_n100k/datasets/synth_uniform_n100k/` |
| 负载 | `experiments/synthetic_m5_n100k/workloads/synth_uniform_q100/` |
| d | `{16,64,32,16,16}` |
| logical_size | `{4,4,4,4,4}` |
| bitsperkey | 20 |
| 命令 | `./main --d 16,64,32,16,16 --logical-size 4,4,4,4,4 --bitsperkey 20 --data-dir ./experiments/synthetic_m5_n100k/datasets/synth_uniform_n100k/ --workload-dir ./experiments/synthetic_m5_n100k/workloads/synth_uniform_q100/ --run-dir ./experiments/synthetic_m5_n100k/runs/20261116_md_synth_m5_n100k_q100/raw/` |
| 备注 | 非空 chunk=8192（满网格）；对比用基线 result.txt；baseline vs workload compare OK。 |
