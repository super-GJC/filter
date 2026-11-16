# experiments/ablations/

「维度消融 + 选择度扫描」独立研究（**不修改任何既有代码/结果**，仅调用已编译的 `./main` 与 `bench/surf_1d/adapter/climate_surf_bench`）。

## 目录结构（自顶向下）

```
experiments/ablations/
├── gen_variants.py            # 只读现有 workload，生成变体 query.txt
├── run_study.py               # 批量运行 + 聚合（调用现有二进制）
├── queries/                   # 生成的变体查询 + 选择度元信息
│   └── <base>/                # base ∈ {q200, q200_mixed}
│       ├── variants_meta.csv  # 每个变体的整体选择度/各维覆盖
│       ├── dimablation/{k1,k2,k3,k4}/query.txt
│       └── selectivity/{m0p5,m1p0,m2p0,m4p0}/query.txt
├── runs/                      # 每次运行的原始逐查询结果
│   └── <base>/<study>/<variant>/
│       ├── md_rep1/result.txt # MD 第 1..3 次重复（7 列，最后列为 FPR）
│       ├── md_rep2/result.txt
│       ├── md_rep3/result.txt
│       └── surf/              # SuRF 两种模式
│           ├── result_best.txt
│           ├── result_and.txt
│           └── summary.csv    # 含 filter_bytes
├── results/                   # 汇总（论文用，中文表头）
│   ├── dimablation_summary.csv
│   ├── selectivity_summary.csv
│   └── detail_with_minmax.tsv # 含 MD 重复的 min/max
└── README.md / config.md
```

## 怎么读

- **主表**：`results/dimablation_summary.csv`（FPR 随约束维数变化）、`results/selectivity_summary.csv`（FPR 随区间宽度变化）。
- 想看抖动：`results/detail_with_minmax.tsv` 里 `md_min/md_max`（MD 有运行间非确定性，故给重复区间）。
- 想复算/画图：`runs/**/result.txt` 是逐查询 7 列（`overlap border nempty ratio filtertime processtime FPR`）。

## 如何重跑

```bash
# 1) 生成变体（只读现有 workload）
python3 experiments/ablations/gen_variants.py --base experiments/workloads/climate_q200/query.txt \
    --tag q200 --out-root experiments/ablations/queries
# 2) 运行 + 聚合
python3 experiments/ablations/run_study.py
```

详见 `config.md` 的实验条件、口径与结论要点。
