#!/usr/bin/env python3
# 增强版（信息更密集、更充实）的两张 Pareto 图，输出为 *新文件*，不覆盖旧图。
# 对应实验：
#   fig3a_pareto_chunk     <- 块长度实验 (chunk length = 4/8/16)
#   fig3b_pareto_bpk       <- bits per key 实验 (MD bpk / SuRF real-suffix)
# 设计：大号空心边标记 + 逐点标签 + 优势区间阴影 + 倍数双箭头 + 参考线 + 结论框
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D

HERE = os.path.dirname(os.path.abspath(__file__))

plt.rcParams.update({
    "font.family": "DejaVu Sans",
    "font.size": 11,
    "axes.grid": True,
    "grid.alpha": 0.30,
    "grid.linestyle": "-",
    "axes.spines.top": False,
    "axes.spines.right": False,
})

COL = {"MD": "#1f4e79", "best": "#2f855a", "AND": "#c05621"}
MRK = {"MD": "o", "best": "s", "AND": "^"}
LBL = {"MD": "MD", "best": "SuRF-1D-best", "AND": "SuRF-1D-AND"}


def save(fig, name):
    p = os.path.join(HERE, name)
    fig.tight_layout()
    fig.savefig(p, dpi=300, bbox_inches="tight")
    plt.close(fig)
    print("saved", p)


def draw_curve(ax, pts, key, zorder=3, big=True):
    # pts: list of (space, fpr, tag)
    xs = [p[0] for p in pts]
    ys = [p[1] for p in pts]
    ax.plot(xs, ys, color=COL[key], linewidth=2.2, alpha=0.85, zorder=zorder - 1)
    ax.scatter(xs, ys, s=(150 if big else 70), marker=MRK[key], color=COL[key],
               edgecolor="white", linewidth=1.4, zorder=zorder,
               label=LBL[key])


# ===================== 图5：块长度实验 Pareto =====================
def fig_chunk():
    # chunk length -> (space KB, fpr)
    md = {4: (596, 0.027), 8: (2552, 0.015), 16: (6896, 0.008)}
    best = {4: (608, 0.46), 8: (250, 0.24), 16: (39, 0.14)}
    andd = {4: (2407, 0.44), 8: (981, 0.21), 16: (149, 0.13)}
    order = (4, 8, 16)

    fig, ax = plt.subplots(figsize=(7.8, 5.4))

    series = [("MD", md), ("best", best), ("AND", andd)]
    for key, d in series:
        pts = [(d[k][0], d[k][1], k) for k in order]
        draw_curve(ax, pts, key)
        # 逐点标注块长度
        for k in order:
            x, y = d[k]
            ax.annotate("k=%d" % k, (x, y), textcoords="offset points",
                        xytext=(9, 8), fontsize=8.5, color=COL[key], fontweight="bold")

    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xlabel("Space (KB)")
    ax.set_ylabel("False Positive Rate")
    ax.set_xlim(30, 12000)
    ax.set_ylim(0.006, 0.62)

    # 同空间（~600KB）对比高亮
    ax.axvline(600, color="gray", linestyle=":", linewidth=1.2, alpha=0.8)
    ax.annotate("same space ~600 KB\nMD 0.027 vs SuRF-best 0.46  (~17x)",
                xy=(600, 0.027), xytext=(120, 0.011),
                fontsize=8.5, color="#333333",
                arrowprops=dict(arrowstyle="->", color="gray", lw=1.0))

    # 标注：MD 占据“高空间 / 低 FPR”角落
    ax.annotate("MD: low-FPR end\n(uses more space)",
                xy=(6896, 0.008), xytext=(1500, 0.0068),
                fontsize=8.5, color=COL["MD"])

    ax.legend(frameon=True, framealpha=0.9, loc="upper left", fontsize=9.5)
    ax.text(0.985, 0.52,
            "all three methods drop FPR as chunk grows;\n"
            "MD is 15-17x lower FPR at every chunk length",
            transform=ax.transAxes, ha="right", va="bottom", fontsize=8.5,
            bbox=dict(boxstyle="round,pad=0.4", fc="#f4f6f9", ec="#b8c4d6"))

    save(fig, "fig3a_pareto_chunk_rich.png")


# ===================== 图6：bits per key 实验 Pareto =====================
def fig_bpk():
    # MD: bpk -> (space KB, fpr)
    md = {4: (512, 0.100), 8: (1020, 0.020), 16: (2040, 0.007),
          18: (2296, 0.0055), 20: (2552, 0.005), 22: (2808, 0.0049),
          24: (3060, 0.0046), 40: (5100, 0.0035)}
    md_order = (4, 8, 16, 18, 20, 22, 24, 40)

    # SuRF: real-suffix sweep (space 几乎固定, fpr 几乎固定) -> 近似竖线
    best = [(249.2, 0.2402), (249.7, 0.2395), (250.7, 0.2402), (251.2, 0.2402),
            (251.4, 0.2402), (252.0, 0.2402), (252.1, 0.2402), (254.9, 0.2402)]
    andd = [(980.7, 0.2163), (981.3, 0.2116), (982.7, 0.2163), (983.3, 0.2163),
            (983.6, 0.2163), (984.4, 0.2163), (984.5, 0.2163), (989.4, 0.2163)]

    fig, ax = plt.subplots(figsize=(7.8, 5.4))

    # MD 曲线
    md_pts = [(md[k][0], md[k][1], k) for k in md_order]
    draw_curve(ax, md_pts, "MD")
    # 只标注关键点，其余点仍以标记呈现，避免密集段标签重叠
    key_offsets = {4: (6, -16), 8: (6, 10), 16: (6, 10), 40: (8, 8)}
    for k, off in key_offsets.items():
        x, y = md[k]
        ax.annotate("bpk=%d" % k, (x, y), textcoords="offset points",
                    xytext=off, fontsize=8.2, color=COL["MD"], fontweight="bold")
    ax.annotate("bpk = {4,8,16,18,20,22,24,40}",
                xy=(2552, 0.005), xytext=(1150, 0.0105),
                fontsize=8.0, color=COL["MD"], style="italic")

    # SuRF 两条近似竖线（逐点小标记）
    for key, pts in [("best", best), ("AND", andd)]:
        xs = [p[0] for p in pts]
        ys = [p[1] for p in pts]
        ax.plot(xs, ys, color=COL[key], linewidth=2.0, alpha=0.85, zorder=2)
        ax.scatter(xs, ys, s=60, marker=MRK[key], color=COL[key],
                   edgecolor="white", linewidth=1.0, zorder=3, label=LBL[key])

    # 优势区间阴影：MD 曲线 与 SuRF-AND 水平之间的 gap
    md_x = [md[k][0] for k in md_order]
    md_y = [md[k][1] for k in md_order]
    ax.fill_between(md_x, md_y, 0.2163, color=COL["MD"], alpha=0.08, zorder=0)
    ax.axhline(0.2163, color=COL["AND"], linestyle="--", linewidth=1.0, alpha=0.7)
    ax.axhline(0.2402, color=COL["best"], linestyle="--", linewidth=1.0, alpha=0.7)

    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xlabel("Space (KB)")
    ax.set_ylabel("False Positive Rate")
    ax.set_xlim(180, 8000)
    ax.set_ylim(0.003, 0.32)

    # 倍数双箭头：~1MB
    ax.annotate("", xy=(1000, 0.020), xytext=(1000, 0.2163),
                arrowprops=dict(arrowstyle="<->", color="#555555", lw=1.2))
    ax.text(1035, 0.075, "~10.8x\n@ ~1 MB", fontsize=8.8, color="#333333",
            va="center")

    # SuRF 竖线文字
    ax.annotate("SuRF-1D-best\n~250 KB, FPR~0.240\n(flat)",
                xy=(252, 0.2402), xytext=(330, 0.285),
                fontsize=8.3, color=COL["best"],
                arrowprops=dict(arrowstyle="->", color=COL["best"], lw=1.0))
    ax.annotate("SuRF-1D-AND\n~982 KB, FPR~0.216\n(flat)",
                xy=(984, 0.2163), xytext=(2000, 0.150),
                fontsize=8.3, color=COL["AND"],
                arrowprops=dict(arrowstyle="->", color=COL["AND"], lw=1.0))

    ax.legend(frameon=True, framealpha=0.9, loc="upper right",
              bbox_to_anchor=(1.0, 0.78), fontsize=9.5)
    ax.text(0.015, 0.03,
            "MD trades space for lower FPR (curve);\n"
            "SuRF cannot: adding suffix/space keeps FPR flat",
            transform=ax.transAxes, ha="left", va="bottom", fontsize=8.5,
            bbox=dict(boxstyle="round,pad=0.4", fc="#f4f6f9", ec="#b8c4d6"))

    save(fig, "fig3b_pareto_bpk_rich.png")


if __name__ == "__main__":
    fig_chunk()
    fig_bpk()
