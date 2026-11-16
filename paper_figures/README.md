# paper_figures/ — 论文插图（每个子图单独成图）

数据来源（仅按文件内容绘图）：`../FPR随块大小变化实验结果.xlsx`、`../FPR随bpk变化实验结果.xlsx`
脚本：`plot_paper_figures_split.py`（300 dpi，英文标注）。
约定：FPR 轴标注为 `False Positive Rate`；空间占用为"每个取值下三种方法的分组柱状图"。

## 图清单

| 文件 | 内容 |
|---|---|
| `fig1a_fpr_vs_chunk.png` | 块长度 4/8/16 下三种方法的 FPR（折线，纵轴 FPR，对数） |
| `fig1b_space_vs_chunk.png` | 块长度 4/8/16 下三种方法的空间占用（分组柱状图） |
| `fig2a_fpr_vs_bpk.png` | bits per key 下三种方法的 FPR（折线，纵轴 FPR，对数） |
| `fig2b_space_vs_bpk.png` | bits per key 下三种方法的空间占用（分组柱状图） |
| `fig3a_pareto_chunk.png` | 块长度实验的空间-FPR Pareto |
| `fig3b_pareto_bpk.png` | bits per key 实验的空间-FPR Pareto |

## 实验结论分析

### 块长度实验（图1a/1b、图3a）
- FPR：MD 0.027/0.015/0.008（块长度 4/8/16），SuRF-1D-AND 0.44/0.21/0.13，SuRF-1D-best 0.46/0.24/0.14。**每个块长度下 MD 都比 SuRF 低约 15–17×**，三者均随块长度增大而下降。
- 空间：MD 596→2552→6896 KB（随块增大而增）；SuRF-AND 2407→981→149 KB、SuRF-best 608→250→39 KB（随块增大而减）。
- 结论：**FPR 优势对块长度稳健**；块大小存在"空间-精度"权衡，应按空间预算选取。

### bits per key 实验（图2a/2b、图3b）
- FPR：MD 0.1→0.02→0.007→0.0055→0.005→0.0049→0.0046→0.0035（bpk 4→40），**拐点在 bpk≈16，之后进入平台**；SuRF 两种模式基本恒定（best≈0.240、AND≈0.216）。
- 空间：MD 512→5100 KB 近似线性；SuRF 基本恒定（≈250 / ≈980 KB）。
- 同空间对比：≈1 MB 处 MD(bpk8,1020KB)=0.02 vs SuRF-AND(981KB)=0.2116（约 10.6×）；bpk16(2040KB)=0.007（约 31×）；bpk40(5100KB)=0.0035（约 62×）。
- 结论：**MD 的空间投入能实质降 FPR**；SuRF 靠加空间/调参无法降低 FPR。

### 总述
两组实验一致：MD 相对一维基线 SuRF 的 FPR 优势在块长度与空间参数变化下均稳健，且在相同空间下仍低约 10× 以上，说明优势来自**多维联合信息**而非额外空间。
