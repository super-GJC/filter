# 合成数据 MD vs SuRF（M=4, n=10k）

目录名：`experiments/synthetic_m4_n10k/`（与 `synthetic_m5_n100k/` 并列）。

本目录与 climate 主实验隔离。

## 第一版规格（已落地）

| 项 | 取值 |
|----|------|
| 数据集 | `datasets/synth_uniform_n10k/`（10000 行） |
| d | `{16,256,64,16}` |
| 分布 | 各维独立均匀；禁止全 0；去重 |
| logical_size | `{2,16,16,4}` → `allmranges=2^20`，chunknum=2048 |
| 负载 | `workloads/synth_uniform_q100/`（100 条，未筛选） |
| 方法 | MD、SuRF-1D-best、SuRF-1D-AND |

## 安全门槛（已通过）

| 项 | 值 |
|----|----|
| Σ oneDrange_bit | 20 |
| allmranges | 1048576 |
| bitmapbytes | 131072 |
| chunknum | 2048 |
| 非空 chunk | 2029 |

## 结果摘要（`compare/fpr_by_method.csv`）

| 方法 | avg_fpr | median_fpr | n_fpr_eq_0 | filter_bytes |
|------|---------|------------|------------|--------------|
| md | **0** | **0** | **100/100** | 39206912 |
| surf_1d_and | 0.268 | 0.267 | 4/100 | 6555527 |
| surf_1d_best | 0.383 | 0.375 | 1/100 | 1645400 |

## 复现

```bash
# 重新生成（可选）
python3 experiments/synthetic_m4_n10k/gen_synth.py

# MD
./main --d 16,256,64,16 --logical-size 2,16,16,4 --bitsperkey 20 \
  --data-dir ./experiments/synthetic_m4_n10k/datasets/synth_uniform_n10k/ \
  --workload-dir ./experiments/synthetic_m4_n10k/workloads/synth_uniform_q100/ \
  --run-dir ./experiments/synthetic_m4_n10k/runs/20261116_md_synth_uniform_q100/raw/

# SuRF
./bench/surf_1d/adapter/climate_surf_bench \
  --data experiments/synthetic_m4_n10k/datasets/synth_uniform_n10k/data.txt \
  --query experiments/synthetic_m4_n10k/workloads/synth_uniform_q100/query.txt \
  --out /tmp/synth_surf_out --mode both \
  --d 16,256,64,16 --logical-size 2,16,16,4 --real-suffix 8
```

## 状态

- [x] 规格确认、目录创建
- [x] 生成数据集与查询负载
- [x] 跑通 MD / SuRF-best / SuRF-AND 并归档
- [x] 填写 compare 汇总

## q1000 补充实验（不覆盖 q100）

- 负载：`workloads/synth_uniform_q1000/`（1000 条）
- runs：`20261117_{md,surf_1d_and,surf_1d_best}_synth_uniform_q1000/`
- 汇总：`compare/fpr_q1000_by_method.csv`

| 方法 | avg_fpr | median_fpr | n_fpr_eq_0 | max_fpr |
|------|---------|------------|------------|--------|
| md | 0.000832 | 0 | 979/1000 | 0.1 |
| surf_1d_and | 0.296 | 0.295 | 24/1000 | 0.762 |
| surf_1d_best | 0.412 | 0.416 | 5/1000 | 0.833 |
