#!/usr/bin/env python3
"""论文风格对比图：维度消融 + 选择度，三种基负载并排。

读取 results/detail_all.tsv（三基负载明细），输出：
  - comparison_2x3.png           上排=维度消融、下排=选择度、三列=三基负载（主图）
  - fpr_vs_dimcount_pretty.png   维度消融大图（1x3）
  - fpr_vs_selectivity_pretty.png选择度大图（1x3）
"""
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
    ("q200", "climate_q200\n(MD-filtered, main)"),
    ("q200_mixed", "climate_q200_mixed\n(unfiltered, mixed)"),
    ("q200_correlated", "climate_q200_correlated\n(unfiltered, correlated)"),
]
STUDIES = {
    "dimablation": dict(
        xlabels=["k1", "k2", "k3", "k4"],
        xlabel="constrained dims (k)  —  k=1 is effectively 1D",
        ylabel="Average FPR (border chunks)",
        divider=0.5,
        divider_text="1D | multi-D",
    ),
    "selectivity": dict(
        xlabels=["m0p5", "m1p0", "m2p0", "m4p0"],
        xlabel="range-width multiplier m (larger = broader)",
        ylabel="Average FPR (border chunks)",
        divider=None,
        divider_text=None,
    ),
}
METHODS = [
    ("md", "MD (multi-dim)", "#1f77b4", "-o"),
    ("best", "SuRF 1D-Best", "#ff7f0e", "-s"),
    ("and", "SuRF 1D-AND", "#2ca02c", "-^"),
]
COL = {"md": "md_mean", "best": "best_avg", "and": "and_avg"}
FLOOR = 1e-4

# 选择度变体的横轴显示名：m0p5 -> m=0.5（p 是文件名中的小数点替身）
SEL_DISPLAY = {"m0p5": "m=0.5", "m1p0": "m=1.0", "m2p0": "m=2.0", "m4p0": "m=4.0"}


def tick_label(study, variant):
    return SEL_DISPLAY.get(variant, variant) if study == "selectivity" else variant

plt.rcParams.update({
    "font.size": 11,
    "axes.titlesize": 12,
    "axes.labelsize": 11,
    "legend.fontsize": 10,
})


def series(study, base):
    rs = [r for r in rows if r["study"] == study and r["base"] == base]
    order = {v: i for i, v in enumerate(STUDIES[study]["xlabels"])}
    rs.sort(key=lambda r: order.get(r["variant"], 99))
    return rs


def draw_panel(ax, study, base, legend=False, annotate=False):
    cfg = STUDIES[study]
    rs = series(study, base)
    xs = list(range(len(rs)))

    for pre, label, color, style in METHODS:
        ys = [max(float(r[COL[pre]]), FLOOR) for r in rs]
        ax.plot(xs, ys, style, color=color, label=label, markersize=7,
                linewidth=2.2, markeredgecolor="white", markeredgewidth=0.6)
        if pre == "md":
            lo = [max(float(r["md_min"]), FLOOR) for r in rs]
            hi = [max(float(r["md_max"]), FLOOR) for r in rs]
            ax.fill_between(xs, lo, hi, color=color, alpha=0.15, linewidth=0)

    if study == "selectivity":
        # 选择度图用线性纵轴，0~1 显式刻度，更直观
        ax.set_yscale("linear")
        ax.set_ylim(0.0, 1.0)
        ax.set_yticks([0.0, 0.2, 0.4, 0.6, 0.8, 1.0])
    else:
        ax.set_yscale("log")
    ax.set_xticks(xs)
    ax.set_xticklabels([tick_label(study, r["variant"]) for r in rs])
    ax.set_xlim(-0.4, len(rs) - 0.6)
    ax.set_xlabel(cfg["xlabel"])
    ax.grid(True, which="both", alpha=0.25, linewidth=0.6)

    if cfg["divider"] is not None:
        ax.axvspan(-0.4, cfg["divider"], color="grey", alpha=0.10, linewidth=0)
        ax.axvline(cfg["divider"], color="grey", linestyle="--", linewidth=1)
        ymin = min(float(r[COL["best"]]) for r in rs) * 0.6
        ax.text(cfg["divider"] - 0.06, ax.get_ylim()[1] * 0.4,
                "1D query\n(SuRF wins)", ha="right", va="center", fontsize=8.5, color="dimgray")

    if study == "selectivity":
        # 高亮低选择度区（m≤1，区间窄）：MD 显著更优，这是主图要强调的部分
        ax.axvspan(-0.4, 1.4, color="#1f77b4", alpha=0.06, linewidth=0)
        md0, best0 = float(rs[0]["md_mean"]), float(rs[0]["best_avg"])
        md1, best1 = float(rs[1]["md_mean"]), float(rs[1]["best_avg"])
        ratios = sorted([best0 / md0, best1 / md1])
        rtxt = ("%.0f" % ratios[0]) if round(ratios[0]) == round(ratios[1]) else ("%.0f–%.0f" % (ratios[0], ratios[1]))
        ax.text(0.02, 0.04,
                "shaded: low selectivity (m≤1)\nMD ~%s× lower FPR than SuRF" % rtxt,
                transform=ax.transAxes, ha="left", va="bottom", fontsize=8, color="#1f77b4")

    if annotate:
        last = rs[-1]
        md = float(last["md_mean"])
        best = float(last["best_avg"])
        ax.annotate("MD %.3g vs SuRF-Best %.3g\n≈ %.0f× lower" % (md, best, best / md),
                    xy=(len(rs) - 1, md), xytext=(0.02, 0.05),
                    textcoords="axes fraction", fontsize=8.5, color="#1f77b4",
                    arrowprops=dict(arrowstyle="->", color="#1f77b4", lw=1))

    if legend:
        ax.legend(loc="upper right" if study == "selectivity" else "upper left", framealpha=0.9)


def main():
    # 主图 2x3
    fig, axes = plt.subplots(2, 3, figsize=(16.5, 8.6))
    for ci, (base, _b) in enumerate(BASES):
        draw_panel(axes[0][ci], "dimablation", base,
                   legend=(ci == 0), annotate=(base == "q200_correlated"))
        axes[0][ci].set_title(BASES[ci][1])
        draw_panel(axes[1][ci], "selectivity", base, legend=(ci == 0))
        axes[1][ci].set_title("")
    axes[0][0].set_ylabel("Average FPR (border chunks)  [log]")
    axes[1][0].set_ylabel("Average FPR (border chunks)")
    # 行标题
    fig.text(0.5, 0.955, "Dimensionality ablation (k = #constrained dims)", ha="center",
             fontsize=14, fontweight="bold")
    fig.text(0.5, 0.485, "Selectivity sweep (m = interval-width multiplier)", ha="center",
             fontsize=14, fontweight="bold")
    fig.tight_layout(rect=[0, 0, 1, 0.93])
    p = os.path.join(OUT, "comparison_2x3.png")
    fig.savefig(p, dpi=140)
    print("saved", p)

    # 单独大图
    for study, fname in [("dimablation", "fpr_vs_dimcount_pretty.png"),
                         ("selectivity", "fpr_vs_selectivity_pretty.png")]:
        fig, axes = plt.subplots(1, 3, figsize=(16.5, 4.6), sharey=True)
        for ci, (base, _b) in enumerate(BASES):
            draw_panel(axes[ci], study, base, legend=(ci == 0),
                       annotate=(study == "dimablation" and base == "q200_correlated"))
            axes[ci].set_title(BASES[ci][1])
        axes[0].set_ylabel("Average FPR (border chunks)  [log]" if study == "dimablation"
                           else "Average FPR (border chunks)")
        fig.suptitle(("FPR vs #constrained dimensions" if study == "dimablation"
                      else "FPR vs query selectivity") + ", across 3 base workloads",
                     fontsize=13, fontweight="bold")
        fig.tight_layout(rect=[0, 0, 1, 0.94])
        p = os.path.join(OUT, fname)
        fig.savefig(p, dpi=140)
        print("saved", p)


if __name__ == "__main__":
    main()
