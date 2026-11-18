# 合成数据 MD vs SuRF（M=5, n=100k）

目录名：`experiments/synthetic_m5_n100k/`（与 `synthetic_m4_n10k/`、climate 主实验隔离）。

## 第一版规格（已落地）

| 项 | 取值 |
|----|------|
| 数据集 | `datasets/synth_uniform_n100k/`（100000 行） |
| d | `{16,64,32,16,16}` |
| 分布 | 各维独立均匀；禁止全 0；去重 |
| logical_size | `{4,4,4,4,4}` → `allmranges=2^20`，chunknum=8192 |
| 负载 | `workloads/synth_uniform_q100/`（100 条，未筛选） |
| 方法 | MD、SuRF-1D-best、SuRF-1D-AND |

## 安全门槛（已通过）

| 项 | 值 |
|----|----|
| Σ oneDrange_bit | 20 |
| allmranges | 1048576 |
| bitmapbytes | 131072 |
| chunknum | 8192 |
| 非空 chunk | 8192（满网格） |

## 结果摘要（`compare/fpr_by_method.csv`）

| 方法 | avg_fpr | median_fpr | n_fpr_eq_0 | filter_bytes |
|------|---------|------------|------------|--------------|
| md | **0** | **0** | **100/100** | 248197120（~237MB） |
| surf_1d_and | 0.266 | 0.265 | 5/100 | 33218266（~32MB） |
| surf_1d_best | 0.289 | 0.299 | 5/100 | 6645571（~6.3MB） |

## 复现

```bash
python3 experiments/synthetic_m5_n100k/gen_synth.py

./main --d 16,64,32,16,16 --logical-size 4,4,4,4,4 --bitsperkey 20 \
  --data-dir ./experiments/synthetic_m5_n100k/datasets/synth_uniform_n100k/ \
  --workload-dir ./experiments/synthetic_m5_n100k/workloads/synth_uniform_q100/ \
  --run-dir ./experiments/synthetic_m5_n100k/runs/20261116_md_synth_m5_n100k_q100/raw/

./bench/surf_1d/adapter/climate_surf_bench \
  --data experiments/synthetic_m5_n100k/datasets/synth_uniform_n100k/data.txt \
  --query experiments/synthetic_m5_n100k/workloads/synth_uniform_q100/query.txt \
  --out /tmp/synth_m5_surf_out --mode both \
  --d 16,64,32,16,16 --logical-size 4,4,4,4,4 --real-suffix 8
```

## 状态

- [x] 规格确认、隔离目录创建
- [x] 生成数据集与 100 条查询
- [x] 跑通 MD / SuRF-best / SuRF-AND 并归档
- [x] 填写 compare 汇总

## q1000 补充实验（不覆盖 q100）

- 负载：`workloads/synth_uniform_q1000/`（1000 条）
- runs：`20261117_{md,surf_1d_and,surf_1d_best}_synth_m5_n100k_q1000/`
- 汇总：`compare/fpr_q1000_by_method.csv`

| 方法 | avg_fpr | median_fpr | n_fpr_eq_0 | max_fpr |
|------|---------|------------|------------|--------|
| md | 4.46e-5 | 0 | 998/1000 | 0.032 |
| surf_1d_and | 0.289 | 0.286 | 17/1000 | 0.778 |
| surf_1d_best | 0.315 | 0.312 | 15/1000 | 0.778 |
