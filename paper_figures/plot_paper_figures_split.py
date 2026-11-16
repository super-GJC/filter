#!/usr/bin/env python3
# 依据根目录两个 xlsx 绘制论文插图（v4）：
#   - FPR 轴标注为 "False Positive Rate"
#   - 空间占用子图：每个取值下三种方法的分组柱状图
#   - 每个子图单独输出为独立图片
#   - 所有图均不显示标题；不使用中文字体
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import openpyxl

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, ".."))
CHUNK_XLSX = os.path.join(ROOT, "FPR随块大小变化实验结果.xlsx")
BPK_XLSX = os.path.join(ROOT, "FPR随bpk变化实验结果.xlsx")

plt.rcParams.update({
    "font.family": "DejaVu Sans",
    "font.size": 11,
    "axes.grid": True,
    "grid.alpha": 0.3,
    "axes.spines.top": False,
    "axes.spines.right": False,
})

METHOD_ORDER = ["MD", "SuRF-1D-best", "SuRF-1D-AND"]
STYLE = {
    "MD":           dict(color="#1f4e79", marker="o", label="MD"),
    "SuRF-1D-best": dict(color="#2f855a", marker="s", label="SuRF-1D-best"),
    "SuRF-1D-AND":  dict(color="#c05621", marker="^", label="SuRF-1D-AND"),
}


def norm(m):
    m = m.strip()
    return "MD" if m in ("MD", "MDRF") else m


def load(path):
    wb = openpyxl.load_workbook(path, data_only=True)
    ws = wb["Sheet1"]
    rows = list(ws.iter_rows(values_only=True))
    data = {m: {} for m in METHOD_ORDER}
    xs = []
    for r in rows[1:]:
        if r[0] is None:
            continue
        method = norm(str(r[0]))
        x = r[1]
        fpr = float(r[2])
        kb = float(r[3])
        data.setdefault(method, {})[x] = (fpr, kb)
        if x not in xs:
            xs.append(x)
    xs.sort()
    return xs, data


def new_fig():
    return plt.subplots(figsize=(6.6, 4.6))


def fpr_line(ax, xs, data, xlabel):
    pos = list(range(len(xs)))
    for m in METHOD_ORDER:
        ys = [data[m][x][0] for x in xs]
        ax.plot(pos, ys, marker=STYLE[m]["marker"], color=STYLE[m]["color"],
                label=STYLE[m]["label"], linewidth=1.8, markersize=7)
    ax.set_xticks(pos)
    ax.set_xticklabels([str(x) for x in xs])
    ax.set_xlabel(xlabel)
    ax.set_ylabel("False Positive Rate")
    ax.set_yscale("log")
    ax.legend(frameon=False)


def space_grouped_bar(ax, xs, data, xlabel):
    n = len(xs)
    m = len(METHOD_ORDER)
    width = 0.8 / m
    idx = np.arange(n)
    ymax = 0.0
    for i, meth in enumerate(METHOD_ORDER):
        ys = [data[meth][x][1] for x in xs]
        ymax = max(ymax, max(ys))
        off = (i - (m - 1) / 2.0) * width
        bars = ax.bar(idx + off, ys, width=width, color=STYLE[meth]["color"],
                      label=STYLE[meth]["label"], edgecolor="black", linewidth=0.4)
        for b, v in zip(bars, ys):
            ax.annotate("%.0f" % v, (b.get_x() + b.get_width() / 2.0, b.get_height()),
                        ha="center", va="bottom", fontsize=7)
    ax.set_xticks(idx)
    ax.set_xticklabels([str(x) for x in xs])
    ax.set_xlabel(xlabel)
    ax.set_ylabel("Space (KB)")
    ax.set_ylim(0, ymax * 1.18)
    ax.legend(frameon=False)


def pareto(ax, xs, data):
    for m in METHOD_ORDER:
        pts = sorted((data[m][x][1], data[m][x][0]) for x in xs)  # (KB, FPR) 按空间升序
        ax.plot([p[0] for p in pts], [p[1] for p in pts], marker=STYLE[m]["marker"],
                color=STYLE[m]["color"], label=STYLE[m]["label"], linewidth=1.6, markersize=7)
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xlabel("Space (KB)")
    ax.set_ylabel("False Positive Rate")
    ax.legend(frameon=False)


def save(fig, name):
    p = os.path.join(HERE, name)
    fig.tight_layout()
    fig.savefig(p, dpi=300, bbox_inches="tight")
    plt.close(fig)
    print("saved", p)


def main():
    xs1, d1 = load(CHUNK_XLSX)
    xs2, d2 = load(BPK_XLSX)

    fig, ax = new_fig()
    fpr_line(ax, xs1, d1, "Chunk length")
    save(fig, "fig1a_fpr_vs_chunk.png")

    fig, ax = new_fig()
    space_grouped_bar(ax, xs1, d1, "Chunk length")
    save(fig, "fig1b_space_vs_chunk.png")

    fig, ax = new_fig()
    fpr_line(ax, xs2, d2, "Bits per key")
    save(fig, "fig2a_fpr_vs_bpk.png")

    fig, ax = new_fig()
    space_grouped_bar(ax, xs2, d2, "Bits per key")
    save(fig, "fig2b_space_vs_bpk.png")

    fig, ax = new_fig()
    pareto(ax, xs1, d1)
    save(fig, "fig3a_pareto_chunk.png")

    fig, ax = new_fig()
    pareto(ax, xs2, d2)
    save(fig, "fig3b_pareto_bpk.png")


if __name__ == "__main__":
    main()
