# m5 成图说明

脚本：`plot_m5_figures.py`（全部轴/图例/注释用 **英文**，避免缺中文字体出现空白方框）。

风格对齐：
- 块大小 / bpk / 空间-FPR：`paper_figures/`（DejaVu Sans，log FPR，分组柱状空间图，Pareto）
- 维度消融 / 选择度：`experiments/ablations/neutral/plot_individual.py`

## 图清单

| 文件 | 内容 |
|------|------|
| `fig1a_fpr_vs_chunk.png` | FPR vs cells/chunk (B) |
| `fig1b_space_vs_chunk.png` | Space (MB) vs B（分组柱） |
| `fig2a_fpr_vs_bpk.png` | FPR vs bits per key |
| `fig2b_space_vs_bpk.png` | Space (MB) vs bpk（分组柱） |
| `fig3a_pareto_chunk.png` / `space_fpr_vs_chunk.png` | 块大小空间-FPR |
| `fig3b_pareto_bpk.png` / `space_fpr_vs_bpk.png` | bpk 空间-FPR（SuRF 固定 suffix=8） |
| `fpr_vs_dimcount.png` / `维度消融_m5_q1000.png` | 维度消融 |
| `fpr_vs_selectivity.png` / `选择度_m5_q1000.png` | 选择度 |

副本同步在 `figures/`。重跑：`python3 plot_m5_figures.py`
