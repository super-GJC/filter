# SuRF climate 适配层（1D-best / 1D-AND）

独立于 `main.cpp` / `rfilter.*`：读 climate 数据与查询，按与 MD **相同的 chunk 网格**建 SuRF，用 **相同的 chunk 级 FPR** 对比。

## 目录

```
bench/surf_1d/
├── SuRF/                 # 官方 header-only
├── adapter/              # 本目录
│   ├── climate_surf_bench.cpp
│   ├── build.sh
│   └── README.md
└── output/               # 默认结果输出
    ├── result_best.txt
    ├── result_and.txt
    └── summary.csv
```

## 语义对齐

与 `Rfilter::process_Queries` 一致：

1. 用 `d` / `logical_size` 算 `piecesbit`、`chunknum`、chunk id  
2. 查询 → overlap / border chunk（解码顺序与源码相同）  
3. **完全覆盖的非空 chunk**：不过滤，直接保留  
4. **border chunk**：走 1D 过滤器  
5. FPR = `(|kept| - |true-hit kept|) / |kept|`（`kept` 为空则为 nan）

### 1D-best

每个非空 chunk 只使用一维 SuRF。默认 `auto`：取该 chunk 上 distinct 值最多的维；也可用 `--best-dim N` 固定维。

### 1D-AND

每个非空 chunk 各维各建一个 SuRF；border 上 **所有维** `lookupRange` 都为真才保留。

### Key 编码

整型域值 → 20 位零填充十进制字符串（保序），范围查询闭区间调用 `SuRF::lookupRange(lo,true,hi,true)`。

默认 `--real-suffix 8`（`kReal`）；`0` 为无 suffix 的 basic SuRF。

## 构建与运行

在仓库内：

```bash
cd bench/surf_1d/adapter
bash build.sh

# 默认：climate data + experiments/workloads/climate_q200 + mode=both
./climate_surf_bench

# 显式指定
./climate_surf_bench \
  --data ../../../data/test/climate/data.txt \
  --query ../../../experiments/workloads/climate_q200/query.txt \
  --out ../output \
  --mode both
```

输出列与 MD `result.txt` 相同：  
`overlap border nempty ratio filtertime processtime FPR`

归档到 `experiments/runs/` 时，把 `output/result_*.txt` 与 `summary.csv` 拷入对应 run 的 `raw/` / 根目录即可（本适配层默认只写 `bench/surf_1d/output/`）。

## 注意

- 不拉 SuRF 的 `ARF` 子模块。  
- `filter_bytes`：best 只累计各 chunk 的 best 维；AND 累计所有维（通常约 M 倍）。对比空间时需同时看该列。  
- 与 MD 对比请用**同一** `query.txt`；`climate_q200` 曾按 MD FPR 筛选，论文中建议同时报 `climate_q200_mixed`。
