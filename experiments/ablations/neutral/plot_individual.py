#!/usr/bin/env python3
"""把维度消融 / 选择度两种实验的主图基负载（q200_correlated）各出一张单独图。

图内文字用英文（系统无中文字体，中文会变方框），文件名用中文便于辨认。
维度消融图作为独立主图，不显示顶部标题说明。
输出目录：results/individual/
"""
import csv
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

HERE = os.path.dirname(os.path.abspath(__file__))
TSV = os.path.join(HERE, "results", "detail_all.tsv")
OUT = os.path.join(HERE, "results", "individual")
os.makedirs(OUT, exist_ok=True)

rows = list(csv.DictReader(open(TSV), delimiter="\t"))

BASES = {
    "q200": dict(zh="Q200主实验_已筛选", en="climate_q200 (MD-filtered, main)"),
    "q200_mixed": dict(zh="Q200混合_未筛选", en="climate_q200_mixed (unfiltered, mixed)"),
    "q200_correlated": dict(zh="Q200相关_未筛选", en="climate_q200_correlated (unfiltered, correlated)"),
}
STUDIES = {
    "dimablation": dict(
        zh="维度消融", en="FPR vs #constrained dims",
        xlabels=["k1", "k2", "k3", "k4"],
        xlabel="constrained dims (k)  —  k=1 is effectively 1D",
        divider=0.5,
    ),
    "selectivity": dict(
        zh="选择度", en="FPR vs query selectivity",
        xlabels=["m0p5", "m1p0", "m2p0", "m4p0"],
        xlabel="range-width multiplier m (larger = broader)",
        divider=None,
    ),
}
METHODS = [
    ("md", "MD", "#1f77b4", "-o"),
    ("best", "SuRF 1D-Best", "#ff7f0e", "-s"),
    ("and", "SuRF 1D-AND", "#2ca02c", "-^"),
]
COL = {"md": "md_mean", "best": "best_avg", "and": "and_avg"}
FLOOR = 1e-4

# 选择度变体的横轴显示名：m0p5 -> m=0.5（p 是文件名中的小数点替身）
SEL_DISPLAY = {"m0p5": "m=0.5", "m1p0": "m=1.0", "m2p0": "m=2.0", "m4p0": "m=4.0"}


def tick_label(study, variant):
    return SEL_DISPLAY.get(variant, variant) if study == "selectivity" else variant

plt.rcParams.update({"font.size": 12, "axes.titlesize": 13,
                     "axes.labelsize": 12, "legend.fontsize": 11})


def series(study, base):
    rs = [r for r in rows if r["study"] == study and r["base"] == base]
    order = {v: i for i, v in enumerate(STUDIES[study]["xlabels"])}
    rs.sort(key=lambda r: order.get(r["variant"], 99))
    return rs


def make_figure(study, base, show_title=True, legend_kw=None, anno_float=None):
    cfg = STUDIES[study]
    rs = series(study, base)
    xs = list(range(len(rs)))

    fig, ax = plt.subplots(figsize=(7.6, 5.0))
    for pre, label, color, style in METHODS:
        ys = [max(float(r[COL[pre]]), FLOOR) for r in rs]
        ax.plot(xs, ys, style, color=color, label=label, markersize=8,
                linewidth=2.4, markeredgecolor="white", markeredgewidth=0.6)

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
    ax.set_ylabel("False Positive Rate")
    if show_title:
        ax.set_title("%s\n%s" % (cfg["en"], BASES[base]["en"]), fontsize=11.5)
    ax.grid(True, which="both", alpha=0.25, linewidth=0.6)

    if cfg["divider"] is not None:
        ax.axvspan(-0.4, cfg["divider"], color="grey", alpha=0.10, linewidth=0)
        ax.axvline(cfg["divider"], color="grey", linestyle="--", linewidth=1)
        ax.text(cfg["divider"] - 0.06, ax.get_ylim()[1] * 0.35,
                "1D query\n(SuRF wins)", ha="right", va="center", fontsize=9.5, color="dimgray")

    # 右侧末尾标注 MD 与 SuRF-Best 的倍数
    if study == "dimablation":
        last = rs[-1]
        md = float(last["md_mean"])
        best = float(last["best_avg"])
        ratio = best / md
        if ratio >= 1.2:
            txt = "k=%s: MD %.3g vs SuRF-Best %.3g\n(~%.0f× lower)" % (last["variant"], md, best, ratio)
        else:
            txt = "k=%s: SuRF-Best %.3g vs MD %.3g\n(SuRF ~%.1f× lower)" % (last["variant"], best, md, md / best)
        xytext = anno_float if anno_float is not None else (0.03, 0.06)
        ha = "center" if anno_float is not None else "left"
        va = "center" if anno_float is not None else "baseline"
        ax.annotate(txt, xy=(len(rs) - 1, md), xytext=xytext,
                    textcoords="axes fraction", fontsize=9.5, color="#1f77b4",
                    ha=ha, va=va,
                    arrowprops=dict(arrowstyle="->", color="#1f77b4", lw=1))
    else:
        # 选择度图：重点强调低选择度区（m≤1，区间窄）MD 显著更优；m 大时过滤失效仅作背景
        ax.axvspan(-0.4, 1.4, color="#1f77b4", alpha=0.07, linewidth=0)
        md0 = float(rs[0]["md_mean"])
        best0 = float(rs[0]["best_avg"])
        md1 = float(rs[1]["md_mean"])
        best1 = float(rs[1]["best_avg"])
        ratios = sorted([best0 / md0, best1 / md1])
        rtxt = ("%.0f" % ratios[0]) if round(ratios[0]) == round(ratios[1]) else ("%.0f–%.0f" % (ratios[0], ratios[1]))
        ax.text(0.66, 0.42,
                "low selectivity (m≤1, narrow ranges)\n"
                "MD %.2f/%.2f vs SuRF-Best %.2f/%.2f\n(~%s× lower FPR)"
                % (md0, md1, best0, best1, rtxt),
                transform=ax.transAxes, ha="center", va="center",
                fontsize=12, color="#1f77b4")

    if legend_kw is None:
        legend_kw = dict(loc="upper left")
    ax.legend(framealpha=0.9, **legend_kw)
    fig.tight_layout()
    fname = "%s_%s.png" % (cfg["zh"], BASES[base]["zh"])
    path = os.path.join(OUT, fname)
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print("saved", path)


def main():
    # 仅保留主图基负载 q200_correlated（其他两个负载的单独图已删除，不再生成）
    # 维度消融图作为独立主图：去掉顶部标题；图例移到 k4 处绿色三角与蓝点之间的空白（右侧中部）
    # 注释文字下移到 MD 曲线与横轴之间的空白区，并保留指向 k4 的箭头（不穿图例）
    make_figure("dimablation", "q200_correlated", show_title=False,
                legend_kw=dict(loc="center right", bbox_to_anchor=(0.98, 0.63)),
                anno_float=(0.48, 0.22))
    # 选择度图：去掉顶部标题；图例放右上，说明文字置于图例与 m=2.0/m=4.0 曲线间的空白
    make_figure("selectivity", "q200_correlated", show_title=False, legend_kw=dict(loc="upper right"))


if __name__ == "__main__":
    main()
