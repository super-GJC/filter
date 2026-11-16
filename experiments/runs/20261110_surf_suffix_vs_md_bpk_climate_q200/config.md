# config: SuRF real-suffix vs MD bitsperkey（空间-FPR 对齐对比）

目的：MD 的 bpk 扫描只画出了 MD 自己的空间-FPR 曲线，SuRF 仅一个点（suffix=8）。本实验把 SuRF 的
`--real-suffix` 旋钮按 MD 已测的 bpk 值 `{4,8,16,18,20,22,24,40,80}` 逐一扫一遍，做"同值 + 同空间"的公平对比。

## 固定条件

| 项 | 值 |
|----|----|
| 数据 / 负载 | climate `data/test/climate/data.txt`，`climate_q200` |
| logical_size | {2,16,16,4}（与 bpk 实验一致） |
| MD | bitsperkey ∈ {4,8,16,18,20,22,24,40,80}，3 次重复（已有结果，直接复用） |
| SuRF | `--mode both`，`--real-suffix` ∈ {0,4,8,16,18,20,22,24,40,64}（确定性，单次） |

## 关于 real-suffix 的硬上限

- SuRF 断言 `hash_suffix_len + real_suffix_len <= kWordSize(=64)`，故 **real-suffix 最大 64**；
  取 80 会 abort（`suffix.hpp:30 ... <= kWordSize`）。因此 MD 的 bpk=80 **无 SuRF 对应点**（表中留空并备注）。
- `real-suffix=0` = `kNone`（最省空间、FPR 最高），补作曲线低端。

## 关键结果

### 同值对比（平均 FPR，单位不同，见注意事项）

| 数值 | MD(bpk) FPR | SuRF-Best(suffix) | SuRF-AND(suffix) |
|---|---|---|---|
| 4 | 0.0982 | 0.2403 | 0.2164 |
| 8 | 0.0228 | 0.2395 | 0.2116 |
| 16 | 0.00545 | 0.2403 | 0.2164 |
| 20 | 0.00507 | 0.2403 | 0.2164 |
| 40 | 0.00494 | 0.2403 | 0.2164 |

### 核心观察

1. **SuRF 的 FPR 对 real-suffix 几乎不敏感**：suffix 从 0 到 64，Best 恒 ~0.240、AND 恒 ~0.216（suffix=8 仅低约 2%，属噪声级）。空间也只从 ~247KB/971KB 缓增到 ~258KB/993KB。→ SuRF 的"空间旋钮"几乎买不到更低 FPR；其假阳性来自"多维不同元组各自满足不同维"的联合效应，而非 trie 边界精度。
2. **MD 的 FPR 随空间显著下降**：bpk 4→18，空间 512KB→2296KB，FPR 0.098→0.0035；之后进入平台。
3. **同空间对比 MD 全面占优**：~1MB 处 MD(bpk8,1020KB)=0.0228 vs SuRF-AND(981KB)=0.2116（约 9×）；~2.3MB 处 MD(bpk18)=0.0035 vs SuRF-AND=0.216（约 61×）。在空间-FPR 平面上 MD 曲线整体压在 SuRF 下方。

## 注意事项（必须如实表述）

- **bpk 与 real-suffix 不是同一物理单位**：bpk 是"位图/布隆阈值比"，real-suffix 是"真实前缀位数"。"同值"只是**数值对齐**，不是物理等价；真正公平的横轴是**占用空间**（见 `pareto_space_fpr.csv` 与图右panel）。
- suffix=8 的 FPR 略低于其它值是 2% 级别的偶然，不构成"最优 suffix"结论；整体应表述为"flat"。
- MD 有运行间非确定性，FPR 为 3 次重复均值；SuRF 确定性。

## 产物

- 同值表：`results/same_value_comparison.csv`（中文表头）
- Pareto 长表：`results/pareto_space_fpr.csv`
- 图：`results/suffix_vs_bpk.png`（左：同值 FPR；右：空间-FPR Pareto）
- 原始 SuRF 逐查询：`raw/suffix_<N>/{result_best.txt,result_and.txt,summary.csv,console.txt}`
- 聚合脚本：`aggregate.py`
