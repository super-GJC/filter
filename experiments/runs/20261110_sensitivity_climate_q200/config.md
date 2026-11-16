# run: 20261110_sensitivity_climate_q200

参数敏感性实验：改变**运行条件**（数据块大小 `logical_size`、MD 的 `bits per key`），验证 MD 是否仍然显著优于 SuRF 一维基线（1D-best / 1D-AND）。

| 字段 | 值 |
|------|----|
| 日期 | 2026-11-10 |
| 方法 | MD + SuRF 1D-best / 1D-AND |
| 数据集 | climate（`data/test/climate/data.txt`，1459 元组） |
| 工作负载 | climate_q200（200 条，`experiments/workloads/climate_q200/query.txt`） |
| d | {4, 1121, 68, 7}（固定） |
| 目的 | chunk 大小 & bits per key 敏感性，检验结论稳健性 |

## 子实验 1：chunk 大小（`sensitivity_logicalsize.csv`）

- `logical_size ∈ {4,4,4,4} / {2,16,16,4} / {4,16,16,4} / {4,64,64,4}`，`bitsperkey=20`。
- MD 重复 R 次报 `mean/min/max`；SuRF（trie，确定性）单次。
- 每个配置都校验了 MD 与 SuRF 的 `overlap/border` **逐行一致（mismatch=0）**，确保是同一套 chunk 网格。

## 子实验 2：bits per key（`sensitivity_bitsperkey.csv`）

- `logical_size={2,16,16,4}` 固定，`bitsperkey ∈ {4,8,16,18,20,22,24,40,80}`，MD 重复 3 次（含细粒度 16/18/22/24 档）。
- 同网格 SuRF 参照：1D-best FPR=0.239521 / 255666B，1D-AND FPR=0.211607 / 1004853B。

## 口径与关键约束

- `logical_size` 必须是**完全平方数**（使 `logical_size0=intervallen²` 无偏差），且每维 1D range 数 < 256（源码用 `uint8_t` 存 range id），因此单维 `logical_size ≲ 64`。
- `bitsperkey` 不是经典「每 key 位数」，而是 rfilter 里**位图 vs 布隆的阈值**：`allmranges/rangeids.size() >= bitsperkey` 时用布隆，否则位图。调大 → 更多块用位图 → **空间↑、FPR↓**。
- **MD 的 FPR 运行间非确定**：根因是读页时出现 `Can not seek!`（无效 block id）读入未初始化内存，导致每次元组集合略有不同；`overlap/border/nempty` 与 `filter_bytes` 恒定。故 MD 一律报重复均值与 min/max。此为**既有 I/O bug，待单独修复**，不影响「MD 是否优于 SuRF」的判断（抖动幅度远小于两者差距）。
- SuRF 为确定性结构。

## 结论

- **所有 chunk 大小下，MD FPR 都比 SuRF 1D-best/1D-AND 低约 1.5–2 个数量级**（如最小块 0.0056 vs 0.44；最大块 0.0009 vs 0.13），结论对 chunk 大小稳健。
- **bits per key 增大**：MD 空间线性上升（0.5MB→10.4MB）、FPR 下降；在 **~1MB（bpk=8，与 SuRF-AND 同空间）时 MD≈0.023 vs SuRF-AND≈0.21**，即使同等空间 MD 仍约 9× 更优。

## raw/ 布局

```
raw/
├── ls_<tag>/   md_result.txt, surf_best.txt, surf_and.txt, surf_summary.csv
└── bpk_<k>/    md_result.txt（代表性单次）
```

`<tag>` ∈ {sm=4,4,4,4 / cur=2,16,16,4 / sq=4,16,16,4 / big=4,64,64,4}。
