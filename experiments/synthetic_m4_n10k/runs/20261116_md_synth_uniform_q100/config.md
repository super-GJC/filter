# run: 20261116_md_synth_uniform_q100

| 字段 | 值 |
|------|----|
| 日期 | 2026-11-16 |
| 方法 | md（多维范围过滤器，基线 filter.txt / result.txt） |
| 数据集 | `experiments/synthetic_m4_n10k/datasets/synth_uniform_n10k/` |
| 负载 | `experiments/synthetic_m4_n10k/workloads/synth_uniform_q100/` |
| d | `{16,256,64,16}` |
| logical_size | `{2,16,16,4}` |
| bitsperkey | 20 |
| 命令 | `./main --d 16,256,64,16 --logical-size 2,16,16,4 --bitsperkey 20 --data-dir ./experiments/synthetic_m4_n10k/datasets/synth_uniform_n10k/ --workload-dir ./experiments/synthetic_m4_n10k/workloads/synth_uniform_q100/ --run-dir ./experiments/synthetic_m4_n10k/runs/20261116_md_synth_uniform_q100/raw/` |
| 备注 | 非空 chunk=2029；对比用基线 result.txt。存在既有 `Can not seek!` 日志；workload 版 filter 更小属预期。 |
