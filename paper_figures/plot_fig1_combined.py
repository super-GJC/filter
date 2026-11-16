#!/usr/bin/env python3
# 新图：把块长度实验的 FPR(fig1a) 与 空间(fig1b) 合并为一张图（左右两子图）。
#   (a) 标题「假阳性率FPR」  (b) 标题「空间占用（KB）」
# 不覆盖已有单图。中文字体放在项目外 /tmp（不进仓库）。
import os
import urllib.request

import matplotlib
matplotlib.use("Agg")
import matplotlib.font_manager as fm
import matplotlib.pyplot as plt
import numpy as np
import openpyxl

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, ".."))
CHUNK_XLSX = os.path.join(ROOT, "FPR随块大小变化实验结果.xlsx")

FONT_PATH = "/tmp/wqy-microhei.ttc"  # 项目外，避免污染仓库
FONT_URL = "https://cdn.jsdelivr.net/gh/anthonyfok/fonts-wqy-microhei@master/wqy-microhei.ttc"

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


def ensure_font():
    if not (os.path.exists(FONT_PATH) and os.path.getsize(FONT_PATH) > 100000):
        urllib.request.urlretrieve(FONT_URL, FONT_PATH)
    fm.fontManager.addfont(FONT_PATH)
    return fm.FontProperties(fname=FONT_PATH)


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
        data.setdefault(method, {})[x] = (float(r[2]), float(r[3]))
        if x not in xs:
            xs.append(x)
    xs.sort()
    return xs, data


def main():
    CJK = ensure_font()
    xs, data = load(CHUNK_XLSX)
    pos = list(range(len(xs)))

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(11.5, 4.5))

    # (a) 假阳性率 FPR —— 折线
    for m in METHOD_ORDER:
        ys = [data[m][x][0] for x in xs]
        ax1.plot(pos, ys, marker=STYLE[m]["marker"], color=STYLE[m]["color"],
                 label=STYLE[m]["label"], linewidth=1.8, markersize=7)
    ax1.set_xticks(pos)
    ax1.set_xticklabels([str(x) for x in xs])
    ax1.set_xlabel("Chunk length")
    ax1.set_ylabel("False Positive Rate")
    ax1.set_yscale("log")
    ax1.set_title("(a) 假阳性率FPR", fontproperties=CJK)
    ax1.legend(frameon=False)

    # (b) 空间占用 —— 分组柱状图
    n = len(xs)
    m = len(METHOD_ORDER)
    width = 0.8 / m
    idx = np.arange(n)
    ymax = 0.0
    for i, meth in enumerate(METHOD_ORDER):
        ys = [data[meth][x][1] for x in xs]
        ymax = max(ymax, max(ys))
        off = (i - (m - 1) / 2.0) * width
        bars = ax2.bar(idx + off, ys, width=width, color=STYLE[meth]["color"],
                       label=STYLE[meth]["label"], edgecolor="black", linewidth=0.4)
        for b, v in zip(bars, ys):
            ax2.annotate("%.0f" % v, (b.get_x() + b.get_width() / 2.0, b.get_height()),
                         ha="center", va="bottom", fontsize=7)
    ax2.set_xticks(idx)
    ax2.set_xticklabels([str(x) for x in xs])
    ax2.set_xlabel("Chunk length")
    ax2.set_ylabel("Space (KB)")
    ax2.set_ylim(0, ymax * 1.18)
    ax2.set_title("(b) 空间占用（KB）", fontproperties=CJK)
    ax2.legend(frameon=False)

    fig.tight_layout()
    out = os.path.join(HERE, "fig1_combined_chunk.png")
    fig.savefig(out, dpi=300, bbox_inches="tight")
    plt.close(fig)
    print("saved", out)


if __name__ == "__main__":
    main()
