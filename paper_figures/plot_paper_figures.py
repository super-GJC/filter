#!/usr/bin/env python3
# 依据项目根目录两个 xlsx 绘制论文插图（不校验数据，仅按文件内容作图）。
# 输出到 paper_figures/。
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import openpyxl

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, ".."))
CHUNK_XLSX = os.path.join(ROOT, "FPR随块大小变化实验结果.xlsx")
BPK_XLSX = os.path.join(ROOT, "FPR随bpk变化实验结果.xlsx")

# 统一风格
plt.rcParams.update({
    "font.family": "DejaVu Sans",
    "font.size": 11,
    "axes.grid": True,
    "grid.alpha": 0.3,
    "axes.spines.top": False,
    "axes.spines.right": False,
    "figure.dpi": 120,
})

METHOD_ORDER = ["MD", "SuRF-1D-best", "SuRF-1D-AND"]
STYLE = {
    "MD":            dict(color="#1f4e79", marker="o", label="MD"),
    "SuRF-1D-best":  dict(color="#2f855a", marker="s", label="SuRF-1D-best"),
    "SuRF-1D-AND":   dict(color="#c05621", marker="^", label="SuRF-1D-AND"),
}


def norm(m):
    m = m.strip()
    return "MD" if m in ("MD", "MDRF") else m


def load(path):
    wb = openpyxl.load_workbook(path, data_only=True)
    ws = wb["Sheet1"]
    rows = list(ws.iter_rows(values_only=True))
    header = rows[0]
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
    return header, xs, data


def line_panel(ax, xs, data, field, ylabel, title, logy=True):
    pos = list(range(len(xs)))
    for m in METHOD_ORDER:
        if m not in data:
            continue
        ys = [data[m][x][field] for x in xs]
        ax.plot(pos, ys, marker=STYLE[m]["marker"], color=STYLE[m]["color"],
                label=STYLE[m]["label"], linewidth=1.8, markersize=7)
    ax.set_xticks(pos)
    ax.set_xticklabels([str(x) for x in xs])
    ax.set_xlabel(title["xlabel"])
    ax.set_ylabel(ylabel)
    ax.set_title(title["title"])
    if logy:
        ax.set_yscale("log")
    ax.legend(frameon=False)


def pareto_panel(ax, xs, data, title):
    for m in METHOD_ORDER:
        if m not in data:
            continue
        pts = sorted((data[m][x][1], data[m][x][0]) for x in xs)  # (kb, fpr) 按空间升序
        kbs = [p[0] for p in pts]
        fprs = [p[1] for p in pts]
        ax.plot(kbs, fprs, marker=STYLE[m]["marker"], color=STYLE[m]["color"],
                label=STYLE[m]["label"], linewidth=1.6, markersize=7)
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xlabel("Space (KB, log)")
    ax.set_ylabel("False Positive Rate")
    ax.set_title(title)


def main():
    os.makedirs(HERE, exist_ok=True)

    # ---------- 图1：块长度 ----------
    _, xs1, d1 = load(CHUNK_XLSX)
    fig, ax = plt.subplots(1, 2, figsize=(11, 4.2))
    line_panel(ax[0], xs1, d1, 0, "False Positive Rate",
               {"xlabel": "Chunk length", "title": "(a) FPR vs chunk length"})
    line_panel(ax[1], xs1, d1, 1, "Space (KB, log)",
               {"xlabel": "Chunk length", "title": "(b) Space vs chunk length"})
    fig.tight_layout()
    p1 = os.path.join(HERE, "fig1_fpr_space_vs_chunk.png")
    fig.savefig(p1, dpi=300, bbox_inches="tight")
    plt.close(fig)

    # ---------- 图2：bits per key ----------
    _, xs2, d2 = load(BPK_XLSX)
    fig, ax = plt.subplots(1, 2, figsize=(11.5, 4.2))
    line_panel(ax[0], xs2, d2, 0, "False Positive Rate",
               {"xlabel": "Bits per key", "title": "(a) FPR vs bits per key"})
    line_panel(ax[1], xs2, d2, 1, "Space (KB, log)",
               {"xlabel": "Bits per key", "title": "(b) Space vs bits per key"})
    fig.tight_layout()
    p2 = os.path.join(HERE, "fig2_fpr_space_vs_bpk.png")
    fig.savefig(p2, dpi=300, bbox_inches="tight")
    plt.close(fig)

    # ---------- 图3：空间-FPR Pareto（两实验各一子图） ----------
    fig, ax = plt.subplots(1, 2, figsize=(11.5, 4.4))
    pareto_panel(ax[0], xs1, d1, "(a) Chunk-length experiment")
    pareto_panel(ax[1], xs2, d2, "(b) Bits-per-key experiment")
    fig.tight_layout()
    p3 = os.path.join(HERE, "fig3_pareto_space_fpr.png")
    fig.savefig(p3, dpi=300, bbox_inches="tight")
    plt.close(fig)

    print("saved:")
    for p in (p1, p2, p3):
        print("  ", p)


if __name__ == "__main__":
    main()
