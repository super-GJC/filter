#!/usr/bin/env python3
"""三基负载（q200 / q200_mixed / q200_correlated）维度消融 + 选择度对比图。"""
import csv
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

HERE = os.path.dirname(os.path.abspath(__file__))
TSV = os.path.join(HERE, "results", "detail_all.tsv")
OUT = os.path.join(HERE, "results")

rows = list(csv.DictReader(open(TSV), delimiter="\t"))

BASES = [
    ("q200", "climate_q200 (MD-filtered, main)"),
    ("q200_mixed", "climate_q200_mixed (unfiltered, mixed)"),
    ("q200_correlated", "climate_q200_correlated (unfiltered, correlated)"),
]
METHODS = [("md", "MD", "-o"), ("best", "SuRF-1D-Best", "-s"), ("and", "SuRF-1D-AND", "-^")]
COL = {"md": "md_mean", "best": "best_avg", "and": "and_avg"}

# 选择度变体的横轴显示名：m0p5 -> m=0.5（p 是文件名中的小数点替身）
SEL_DISPLAY = {"m0p5": "m=0.5", "m1p0": "m=1.0", "m2p0": "m=2.0", "m4p0": "m=4.0"}


def plot(study, xlabels, xlabel, title, fname):
    fig, axes = plt.subplots(1, 3, figsize=(15, 4.4), sharey=True)
    order = {v: i for i, v in enumerate(xlabels)}
    for ax, (base, btitle) in zip(axes, BASES):
        rs = [r for r in rows if r["study"] == study and r["base"] == base]
        rs.sort(key=lambda r: order.get(r["variant"], 99))
        for pre, label, style in METHODS:
            ys = [float(r[COL[pre]]) for r in rs]
            ax.plot(range(len(rs)), ys, style, label=label, markersize=6)
            if pre == "md":
                lo = [float(r["md_min"]) for r in rs]
                hi = [float(r["md_max"]) for r in rs]
                ax.fill_between(range(len(rs)), lo, hi, alpha=0.15)
        ax.set_xticks(range(len(rs)))
        ax.set_xticklabels([SEL_DISPLAY.get(r["variant"], r["variant"]) if study == "selectivity"
                            else r["variant"] for r in rs])
        ax.set_xlabel(xlabel)
        ax.set_title(btitle, fontsize=10)
        ax.grid(True, alpha=0.3)
    axes[0].set_ylabel("Average FPR (border chunks)")
    axes[0].legend()
    fig.suptitle(title)
    fig.tight_layout()
    fig.savefig(os.path.join(OUT, fname), dpi=130)
    print("saved", fname)


plot("dimablation", ["k1", "k2", "k3", "k4"],
     "number of constrained dimensions (k)",
     "FPR vs #constrained dims, across 3 base workloads",
     "fpr_vs_dimcount_3base.png")

plot("selectivity", ["m0p5", "m1p0", "m2p0", "m4p0"],
     "range-width multiplier m (larger = broader / less selective)",
     "FPR vs query selectivity, across 3 base workloads",
     "fpr_vs_selectivity_3base.png")
