#!/usr/bin/env python3
# 读取 results/detail_with_minmax.tsv，画两张对比图（英文标注以避开中文字体问题）。
import csv
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

HERE = os.path.dirname(os.path.abspath(__file__))
TSV = os.path.join(HERE, "results", "detail_with_minmax.tsv")
OUT = os.path.join(HERE, "results")

rows = []
with open(TSV) as f:
    for r in csv.DictReader(f, delimiter="\t"):
        rows.append(r)

METHODS = [
    ("md", "MD", "-o"),
    ("best", "SuRF-1D-Best", "-s"),
    ("and", "SuRF-1D-AND", "-^"),
]

# 选择度变体的横轴显示名：m0p5 -> m=0.5（p 是文件名中的小数点替身）
SEL_DISPLAY = {"m0p5": "m=0.5", "m1p0": "m=1.0", "m2p0": "m=2.0", "m4p0": "m=4.0"}


def get(study, base, xfield):
    xs = []
    for r in rows:
        if r["study"] == study and r["base"] == base:
            xs.append(r)
    return xs


def plot_study(study, xfield, xlabels, xlabel, title, fname):
    bases = [("q200", "climate_q200 (main)"), ("q200_mixed", "climate_q200_mixed (unbiased)")]
    fig, axes = plt.subplots(1, 2, figsize=(11, 4.2), sharey=True)
    for ax, (base, btitle) in zip(axes, bases):
        rs = get(study, base, xfield)
        # 变体按 x 排序
        order = {v: i for i, v in enumerate(xlabels)}
        rs.sort(key=lambda r: order.get(r["variant"], 99))
        col = {"md": "md_mean", "best": "best_avg", "and": "and_avg"}
        for prefix, label, style in METHODS:
            ys = [float(r[col[prefix]]) for r in rs]
            ax.plot(range(len(rs)), ys, style, label=label, markersize=6)
            if prefix == "md":
                lo = [float(r["md_min"]) for r in rs]
                hi = [float(r["md_max"]) for r in rs]
                ax.fill_between(range(len(rs)), lo, hi, alpha=0.15)
        ax.set_xticks(range(len(rs)))
        ax.set_xticklabels([SEL_DISPLAY.get(r["variant"], r["variant"]) if study == "selectivity"
                            else r["variant"] for r in rs])
        ax.set_xlabel(xlabel)
        ax.set_title(btitle)
        ax.grid(True, alpha=0.3)
    axes[0].set_ylabel("Average FPR (border chunks)")
    axes[0].legend()
    fig.suptitle(title)
    fig.tight_layout()
    fig.savefig(os.path.join(OUT, fname), dpi=130)
    print("saved", fname)


plot_study("dimablation", "variant", ["k1", "k2", "k3", "k4"],
           "number of constrained dimensions (k)",
           "FPR vs #constrained dims (higher k = more multi-dimensional)",
           "fpr_vs_dimcount.png")

plot_study("selectivity", "variant", ["m0p5", "m1p0", "m2p0", "m4p0"],
           "range-width multiplier m (larger = broader / less selective)",
           "FPR vs query selectivity (interval width x m)",
           "fpr_vs_selectivity.png")
