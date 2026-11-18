#!/usr/bin/env python3
"""Redraw m5 synthetic experiment figures in paper style.

- Block / bpk / Pareto: style aligned with paper_figures/ (English labels,
  DejaVu Sans, no CJK — avoids tofu boxes).
- Dim-ablation / selectivity: style aligned with
  experiments/ablations/neutral/plot_individual.py.

Reads CSVs in this directory; writes PNGs here (and figures/ subfolder).
"""
from __future__ import annotations

import csv
import os
import shutil

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
FIG = os.path.join(HERE, "figures")
os.makedirs(FIG, exist_ok=True)

plt.rcParams.update(
    {
        "font.family": "DejaVu Sans",
        "font.size": 11,
        "axes.grid": True,
        "grid.alpha": 0.3,
        "axes.spines.top": False,
        "axes.spines.right": False,
    }
)

# paper_figures palette
STYLE = {
    "MD": dict(color="#1f4e79", marker="o", label="MD"),
    "SuRF-1D-best": dict(color="#2f855a", marker="s", label="SuRF-1D-best"),
    "SuRF-1D-AND": dict(color="#c05621", marker="^", label="SuRF-1D-AND"),
}
# ablation/individual palette (slightly different naming)
ABL_METHODS = [
    ("MD", "MD", "#1f77b4", "o"),
    ("SuRF-1D-Best", "SuRF 1D-Best", "#ff7f0e", "s"),
    ("SuRF-1D-AND", "SuRF 1D-AND", "#2ca02c", "^"),
]

METHOD_MAP = {
    "MD": "MD",
    "SuRF_1d_Best": "SuRF-1D-best",
    "SuRF_1d_AND": "SuRF-1D-AND",
    "SuRF-1D-Best": "SuRF-1D-best",
    "SuRF-1D-AND": "SuRF-1D-AND",
}
ORDER = ["MD", "SuRF-1D-best", "SuRF-1D-AND"]
FLOOR = 1e-6  # log-scale floor for observed FPR==0


def save(fig, name: str):
    for d in (HERE, FIG):
        p = os.path.join(d, name)
        fig.tight_layout()
        fig.savefig(p, dpi=300, bbox_inches="tight")
        print("saved", p)
    plt.close(fig)


def fpr_floor(x: float) -> float:
    return max(float(x), FLOOR)


def load_blocksize():
    rows = list(csv.DictReader(open(os.path.join(HERE, "blocksize_summary.csv"), encoding="utf-8")))
    # order: sm, mid, C, A by B
    tiers = []
    seen = set()
    data = {m: {} for m in ORDER}
    for r in rows:
        tier = r["档位"]
        B = int(r["每块逻辑单元数B"])
        meth = METHOD_MAP[r["方法"]]
        fpr = float(r["平均FPR"])
        kb = float(r["占用字节"]) / 1024.0
        key = (tier, B)
        if key not in seen:
            tiers.append(key)
            seen.add(key)
        data[meth][B] = (fpr, kb)
    tiers.sort(key=lambda t: t[1])
    xs = [B for _, B in tiers]
    labels = ["%d" % B for B in xs]
    return xs, labels, data


def load_bpk():
    rows = list(csv.DictReader(open(os.path.join(HERE, "bitsperkey_summary.csv"), encoding="utf-8")))
    xs = []
    data = {m: {} for m in ORDER}
    for r in rows:
        bpk = int(r["bits_per_key"])
        meth = METHOD_MAP[r["方法"]]
        fpr = float(r["平均FPR"])
        kb = float(r["占用字节"]) / 1024.0
        data[meth][bpk] = (fpr, kb)
        if bpk not in xs:
            xs.append(bpk)
    xs.sort()
    return xs, data


def load_dimablation():
    rows = list(csv.DictReader(open(os.path.join(HERE, "dimablation_summary.csv"), encoding="utf-8")))
    rows.sort(key=lambda r: int(r["约束维数k"]))
    return rows


def load_selectivity():
    rows = list(csv.DictReader(open(os.path.join(HERE, "selectivity_summary.csv"), encoding="utf-8")))
    rows.sort(key=lambda r: float(r["区间倍数m"]))
    return rows


def new_fig(w=6.6, h=4.6):
    return plt.subplots(figsize=(w, h))


def fpr_line(ax, xs, data, xlabel, xticklabels=None):
    pos = list(range(len(xs)))
    for m in ORDER:
        ys = [fpr_floor(data[m][x][0]) for x in xs]
        ax.plot(
            pos,
            ys,
            marker=STYLE[m]["marker"],
            color=STYLE[m]["color"],
            label=STYLE[m]["label"],
            linewidth=1.8,
            markersize=7,
            markeredgecolor="white",
            markeredgewidth=0.6,
        )
    ax.set_xticks(pos)
    ax.set_xticklabels(xticklabels if xticklabels is not None else [str(x) for x in xs])
    ax.set_xlabel(xlabel)
    ax.set_ylabel("False Positive Rate")
    ax.set_yscale("log")
    ax.legend(frameon=False)


def space_grouped_bar(ax, xs, data, xlabel, xticklabels=None):
    """Bar heights in MB (m5 filters are tens–hundreds of MB)."""
    n = len(xs)
    m = len(ORDER)
    width = 0.8 / m
    idx = np.arange(n)
    ymax = 0.0
    for i, meth in enumerate(ORDER):
        ys = [data[meth][x][1] / 1024.0 for x in xs]  # KB -> MB
        ymax = max(ymax, max(ys))
        off = (i - (m - 1) / 2.0) * width
        bars = ax.bar(
            idx + off,
            ys,
            width=width,
            color=STYLE[meth]["color"],
            label=STYLE[meth]["label"],
            edgecolor="black",
            linewidth=0.4,
        )
        for b, v in zip(bars, ys):
            txt = "%.0f" % v if v >= 10 else "%.1f" % v
            ax.annotate(
                txt,
                (b.get_x() + b.get_width() / 2.0, b.get_height()),
                ha="center",
                va="bottom",
                fontsize=7,
            )
    ax.set_xticks(idx)
    ax.set_xticklabels(xticklabels if xticklabels is not None else [str(x) for x in xs])
    ax.set_xlabel(xlabel)
    ax.set_ylabel("Space (MB)")
    ax.set_ylim(0, ymax * 1.22)
    ax.legend(frameon=False)


def plot_blocksize():
    xs, labels, data = load_blocksize()
    xlabel = "Cells per chunk (B)"

    # fig1a: linear FPR axis (MD ~1e-5 looks exaggerated on log).
    # SuRF peaks ~0.40, so ticks 0.00..0.40 step 0.08 (same style as 0.00/0.04/...).
    fig, ax = new_fig()
    pos = list(range(len(xs)))
    # True FPR values (no visual offset). Pad ylim below 0 so the "0.00" tick
    # floats above the bottom spine — same style as the reference plot.
    for m in ["SuRF-1D-best", "SuRF-1D-AND", "MD"]:
        ys = [data[m][x][0] for x in xs]
        ms = 10 if m == "MD" else 7
        lw = 2.4 if m == "MD" else 1.8
        ax.plot(
            pos,
            ys,
            marker=STYLE[m]["marker"],
            color=STYLE[m]["color"],
            label=STYLE[m]["label"],
            linewidth=lw,
            markersize=ms,
            markeredgecolor="white",
            markeredgewidth=1.0,
            zorder=5 if m == "MD" else 3,
        )
    ax.set_xticks(pos)
    ax.set_xticklabels(labels)
    ax.set_xlabel(xlabel)
    ax.set_ylabel("False Positive Rate")
    ax.set_yscale("linear")
    # Empty band below 0.00 (~0.02 data units), then ticks from 0.00 upward.
    ax.set_ylim(-0.02, 0.44)
    ax.set_yticks([0.00, 0.08, 0.16, 0.24, 0.32, 0.40])
    ax.set_yticklabels(["0.00", "0.08", "0.16", "0.24", "0.32", "0.40"])
    ax.legend(frameon=False, loc="upper right")
    save(fig, "fig1a_fpr_vs_chunk.png")

    fig, ax = new_fig()
    space_grouped_bar(ax, xs, data, xlabel, labels)
    save(fig, "fig1b_space_vs_chunk.png")

    # Pareto (chunk): space log-x, linear FPR y with pad below 0 (like fig1a).
    fig, ax = new_fig(7.8, 5.4)
    for m in ORDER:
        pts = sorted((data[m][x][1], data[m][x][0], x) for x in xs)  # raw FPR
        # MD: sm/mid spaces nearly equal → markers overlap. For display, put
        # B=512 left and B=1024 right (by B order), then larger-B points.
        # Axis limits / CSV values unchanged.
        disp = list(pts)
        if m == "MD" and len(disp) >= 2:
            by_B = {B: (kb, fpr, B) for kb, fpr, B in disp}
            if 512 in by_B and 1024 in by_B:
                kb_a, y_a, _ = by_B[512]
                kb_b, y_b, _ = by_B[1024]
                mid_x = 0.5 * (kb_a + kb_b)
                # Separated around their midpoint: 512 left, 1024 right.
                p512 = (mid_x * 0.82, y_a, 512)
                p1024 = (mid_x * 1.18, y_b, 1024)
                rest = sorted(
                    [(kb, fpr, B) for kb, fpr, B in disp if B not in (512, 1024)],
                    key=lambda t: t[0],
                )
                disp = [p512, p1024] + rest
        ax.plot(
            [p[0] for p in disp],
            [p[1] for p in disp],
            color=STYLE[m]["color"],
            linewidth=2.2,
            alpha=0.85,
            zorder=2,
        )
        ax.scatter(
            [p[0] for p in disp],
            [p[1] for p in disp],
            s=130,
            marker=STYLE[m]["marker"],
            color=STYLE[m]["color"],
            edgecolor="white",
            linewidth=1.2,
            zorder=3,
            label=STYLE[m]["label"],
        )
        if m == "MD":
            # Spread labels so they do not overlap; arrows point to markers.
            # Fan out: low-left, high-center, mid-right, high-far-right.
            label_style = {
                512: dict(xytext=(-55, 18), ha="right"),
                1024: dict(xytext=(-15, 72), ha="center"),
                2048: dict(xytext=(12, 28), ha="left"),   # shifted left
                4096: dict(xytext=(36, 72), ha="left"),   # shifted left
            }
            for kb, fpr, B in disp:
                st = label_style.get(B, dict(xytext=(0, 28), ha="center"))
                ax.annotate(
                    "B=%d" % B,
                    (kb, fpr),
                    textcoords="offset points",
                    xytext=st["xytext"],
                    ha=st["ha"],
                    va="bottom",
                    fontsize=8.2,
                    color=STYLE[m]["color"],
                    fontweight="bold",
                    arrowprops=dict(
                        arrowstyle="->",
                        color=STYLE[m]["color"],
                        lw=1.0,
                        shrinkA=0,
                        shrinkB=3,
                    ),
                )
    ax.set_xscale("log")
    ax.set_yscale("linear")
    ax.set_ylim(-0.02, 0.44)
    ax.set_yticks([0.00, 0.08, 0.16, 0.24, 0.32, 0.40])
    ax.set_yticklabels(["0.00", "0.08", "0.16", "0.24", "0.32", "0.40"])
    ax.set_xlabel("Space (KB)")
    ax.set_ylabel("False Positive Rate")
    ax.legend(frameon=True, framealpha=0.9, loc="upper right", fontsize=9.5)
    ax.text(
        0.98,
        0.42,
        "MD stays ~1e-5 FPR across block sizes;\n"
        "SuRF FPR drops with larger blocks but remains >> MD",
        transform=ax.transAxes,
        ha="right",
        va="bottom",
        fontsize=8.5,
        bbox=dict(boxstyle="round,pad=0.4", fc="#f4f6f9", ec="#b8c4d6"),
    )
    save(fig, "fig3a_pareto_chunk.png")


def plot_bpk():
    xs, data = load_bpk()
    xlabel = "Bits per key"

    # fig2a: linear FPR + pad below 0 (same as fig1a), so MD≈0 floats above x-spine.
    fig, ax = new_fig()
    pos = list(range(len(xs)))
    for m in ["SuRF-1D-best", "SuRF-1D-AND", "MD"]:
        ys = [data[m][x][0] for x in xs]
        ms = 10 if m == "MD" else 7
        lw = 2.4 if m == "MD" else 1.8
        ax.plot(
            pos,
            ys,
            marker=STYLE[m]["marker"],
            color=STYLE[m]["color"],
            label=STYLE[m]["label"],
            linewidth=lw,
            markersize=ms,
            markeredgecolor="white",
            markeredgewidth=1.0,
            zorder=5 if m == "MD" else 3,
        )
    ax.set_xticks(pos)
    ax.set_xticklabels([str(x) for x in xs])
    ax.set_xlabel(xlabel)
    ax.set_ylabel("False Positive Rate")
    ax.set_yscale("linear")
    ax.set_ylim(-0.02, 0.36)
    ax.set_yticks([0.00, 0.08, 0.16, 0.24, 0.32])
    ax.set_yticklabels(["0.00", "0.08", "0.16", "0.24", "0.32"])
    # Right-aligned; vertical center near FPR≈0.20 (between 0.16 and 0.24).
    # axes fraction: y ≈ (0.20 - (-0.02)) / (0.36 - (-0.02)) ≈ 0.58
    ax.legend(frameon=False, loc="center right", bbox_to_anchor=(1.0, 0.58))
    save(fig, "fig2a_fpr_vs_bpk.png")

    fig, ax = new_fig()
    space_grouped_bar(ax, xs, data, xlabel)
    save(fig, "fig2b_space_vs_bpk.png")

    # Rich Pareto (bpk): linear FPR + pad below 0; SuRF fixed suffix refs.
    fig, ax = new_fig(7.8, 5.4)
    md_pts = [(data["MD"][k][1], data["MD"][k][0], k) for k in xs]  # raw FPR
    ax.plot([p[0] for p in md_pts], [p[1] for p in md_pts],
            color=STYLE["MD"]["color"], linewidth=2.2, alpha=0.85, zorder=2)
    ax.scatter([p[0] for p in md_pts], [p[1] for p in md_pts],
               s=140, marker="o", color=STYLE["MD"]["color"],
               edgecolor="white", linewidth=1.3, zorder=3, label="MD")
    for k, off, lbl in [
        (4, (6, -14), "bpk=4"),
        (8, (6, 10), "bpk=8"),
        (16, (6, 10), "bpk=16...24"),
        (40, (8, 8), "bpk=40"),
    ]:
        kb, fpr = data["MD"][k][1], data["MD"][k][0]
        ax.annotate(lbl, (kb, fpr), textcoords="offset points",
                    xytext=off, fontsize=8.2, color=STYLE["MD"]["color"], fontweight="bold")

    for key, name in [("SuRF-1D-best", "best"), ("SuRF-1D-AND", "AND")]:
        kb, fpr = data[key][xs[0]][1], data[key][xs[0]][0]
        ax.axhline(fpr, color=STYLE[key]["color"], linestyle="--", linewidth=1.0, alpha=0.7)
        ax.scatter([kb], [fpr], s=120, marker=STYLE[key]["marker"],
                   color=STYLE[key]["color"], edgecolor="white", linewidth=1.2,
                   zorder=3, label=STYLE[key]["label"])

    and_kb, and_fpr = data["SuRF-1D-AND"][xs[0]][1], data["SuRF-1D-AND"][xs[0]][0]
    md4_kb, md4_fpr = data["MD"][4][1], data["MD"][4][0]
    ratio = and_fpr / max(md4_fpr, 1e-12)
    ax.annotate(
        "",
        xy=(md4_kb, md4_fpr),
        xytext=(md4_kb, and_fpr),
        arrowprops=dict(arrowstyle="<->", color="#555555", lw=1.2),
    )
    ax.text(md4_kb * 1.08, 0.5 * (md4_fpr + and_fpr),
            "~%.0fx\n@ MD bpk=4\n(~%.0f MB vs SuRF-AND ~%.0f MB)"
            % (ratio, md4_kb / 1024.0, and_kb / 1024.0),
            fontsize=8.2, color="#333333", va="center")

    ax.annotate(
        "SuRF-1D-best\n~%.1f MB, FPR~%.3f\n(fixed suffix=8)"
        % (data["SuRF-1D-best"][xs[0]][1] / 1024.0, data["SuRF-1D-best"][xs[0]][0]),
        xy=(data["SuRF-1D-best"][xs[0]][1], data["SuRF-1D-best"][xs[0]][0]),
        xytext=(data["SuRF-1D-best"][xs[0]][1] * 1.5, 0.22),
        fontsize=8.2, color=STYLE["SuRF-1D-best"]["color"],
        arrowprops=dict(arrowstyle="->", color=STYLE["SuRF-1D-best"]["color"], lw=1.0),
    )
    ax.annotate(
        "SuRF-1D-AND\n~%.1f MB, FPR~%.3f\n(fixed suffix=8)"
        % (and_kb / 1024.0, and_fpr),
        xy=(and_kb, and_fpr),
        xytext=(and_kb * 0.5, 0.12),
        fontsize=8.2, color=STYLE["SuRF-1D-AND"]["color"],
        arrowprops=dict(arrowstyle="->", color=STYLE["SuRF-1D-AND"]["color"], lw=1.0),
    )

    ax.set_xscale("log")
    ax.set_yscale("linear")
    ax.set_xlabel("Space (KB)")
    ax.set_ylabel("False Positive Rate")
    ax.set_ylim(-0.02, 0.36)
    ax.set_yticks([0.00, 0.08, 0.16, 0.24, 0.32])
    ax.set_yticklabels(["0.00", "0.08", "0.16", "0.24", "0.32"])
    ax.legend(frameon=True, framealpha=0.9, loc="center right",
              bbox_to_anchor=(1.0, 0.5), fontsize=9.5)
    ax.text(
        0.015,
        0.03,
        "MD trades space for lower FPR (curve);\n"
        "SuRF fixed at real-suffix=8: FPR stays flat",
        transform=ax.transAxes,
        ha="left",
        va="bottom",
        fontsize=8.5,
        bbox=dict(boxstyle="round,pad=0.4", fc="#f4f6f9", ec="#b8c4d6"),
    )
    save(fig, "fig3b_pareto_bpk.png")
    for src, dst in [
        ("fig3b_pareto_bpk.png", "space_fpr_vs_bpk.png"),
        ("fig3a_pareto_chunk.png", "space_fpr_vs_chunk.png"),
    ]:
        shutil.copy2(os.path.join(HERE, src), os.path.join(HERE, dst))
        shutil.copy2(os.path.join(FIG, src), os.path.join(FIG, dst))
        print("copied", dst)


def plot_dimablation():
    rows = load_dimablation()
    xs = list(range(len(rows)))
    fig, ax = plt.subplots(figsize=(7.6, 5.0))
    plt.rcParams.update({"font.size": 12})

    series = [
        ("MD平均FPR", "MD", "#1f77b4", "o"),
        ("SuRF_1d_Best平均FPR", "SuRF 1D-Best", "#ff7f0e", "s"),
        ("SuRF_1d_AND平均FPR", "SuRF 1D-AND", "#2ca02c", "^"),
    ]
    for col, label, color, mk in series:
        ys = [fpr_floor(float(r[col])) for r in rows]
        ax.plot(xs, ys, "-" + mk, color=color, label=label, markersize=8,
                linewidth=2.4, markeredgecolor="white", markeredgewidth=0.6)

    ax.set_yscale("log")
    ax.set_xticks(xs)
    ax.set_xticklabels(["k%d" % int(r["约束维数k"]) for r in rows])
    ax.set_xlim(-0.4, len(rows) - 0.6)
    ax.set_xlabel("constrained dims (k)  —  k=1 is effectively 1D")
    ax.set_ylabel("False Positive Rate")
    ax.grid(True, which="both", alpha=0.25, linewidth=0.6)

    # shade 1D region (no claim that SuRF wins on this synthetic set)
    ax.axvspan(-0.4, 0.5, color="grey", alpha=0.10, linewidth=0)
    ax.axvline(0.5, color="grey", linestyle="--", linewidth=1)
    ax.text(0.5 - 0.06, ax.get_ylim()[1] * 0.25,
            "1D query\nregion", ha="right", va="center", fontsize=9.5, color="dimgray")

    last = rows[-1]
    md = float(last["MD平均FPR"])
    best = float(last["SuRF_1d_Best平均FPR"])
    md_s = fpr_floor(md)
    ratio = best / md_s if md_s > 0 else float("inf")
    ax.annotate(
        "k=5: MD %.2g vs SuRF-Best %.3g\n(~%.0f× lower)" % (md if md > 0 else FLOOR, best, ratio),
        xy=(len(rows) - 1, md_s),
        xytext=(0.42, 0.40),
        textcoords="axes fraction",
        fontsize=9.5,
        color="#1f77b4",
        arrowprops=dict(arrowstyle="->", color="#1f77b4", lw=1),
    )
    ax.legend(framealpha=0.9, loc="center right", bbox_to_anchor=(0.98, 0.63))
    fig.tight_layout()
    for name in ("fpr_vs_dimcount.png", "维度消融_m5_q1000.png"):
        p = os.path.join(HERE, name)
        fig.savefig(p, dpi=300, bbox_inches="tight")
        print("saved", p)
        fig.savefig(os.path.join(FIG, name), dpi=300, bbox_inches="tight")
    plt.close(fig)


def plot_selectivity():
    rows = load_selectivity()
    xs = list(range(len(rows)))
    fig, ax = plt.subplots(figsize=(7.6, 5.0))

    series = [
        ("MD平均FPR", "MD", "#1f77b4", "o"),
        ("SuRF_1d_Best平均FPR", "SuRF 1D-Best", "#ff7f0e", "s"),
        ("SuRF_1d_AND平均FPR", "SuRF 1D-AND", "#2ca02c", "^"),
    ]
    for col, label, color, mk in series:
        ys = [float(r[col]) for r in rows]
        ax.plot(xs, ys, "-" + mk, color=color, label=label, markersize=8,
                linewidth=2.4, markeredgecolor="white", markeredgewidth=0.6)

    ax.set_yscale("linear")
    # keep 0.00 just above the bottom spine so MD's near-zero curve stays visible
    ax.set_ylim(-0.03, 0.62)
    ax.set_yticks([0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6])
    ax.set_xticks(xs)
    ax.set_xticklabels(["m=%.1f" % float(r["区间倍数m"]) for r in rows])
    ax.set_xlim(-0.4, len(rows) - 0.6)
    ax.set_xlabel("range-width multiplier m (larger = broader)")
    ax.set_ylabel("False Positive Rate")
    ax.grid(True, which="both", alpha=0.25, linewidth=0.6)

    ax.axvspan(-0.4, 1.4, color="#1f77b4", alpha=0.07, linewidth=0)
    md0, best0 = float(rows[0]["MD平均FPR"]), float(rows[0]["SuRF_1d_Best平均FPR"])
    md1, best1 = float(rows[1]["MD平均FPR"]), float(rows[1]["SuRF_1d_Best平均FPR"])
    r0 = best0 / max(md0, FLOOR)
    r1 = best1 / max(md1, FLOOR)
    ax.text(
        0.62,
        0.55,
        "low selectivity (m≤1, narrow ranges)\n"
        "MD %.2g/%.2g vs SuRF-Best %.2f/%.2f\n(~%.0f–%.0f× lower FPR)"
        % (md0, md1, best0, best1, min(r0, r1), max(r0, r1)),
        transform=ax.transAxes,
        ha="center",
        va="center",
        fontsize=11,
        color="#1f77b4",
    )
    ax.legend(framealpha=0.9, loc="upper right")
    fig.tight_layout()
    for name in ("fpr_vs_selectivity.png", "选择度_m5_q1000.png"):
        p = os.path.join(HERE, name)
        fig.savefig(p, dpi=300, bbox_inches="tight")
        print("saved", p)
        fig.savefig(os.path.join(FIG, name), dpi=300, bbox_inches="tight")
    plt.close(fig)


def main():
    plot_blocksize()
    plot_bpk()
    plot_dimablation()
    plot_selectivity()
    # remove old broken scatter name confusion
    old = os.path.join(HERE, "space_fpr_scatter.png")
    if os.path.isfile(old):
        os.remove(old)
        print("removed", old)


if __name__ == "__main__":
    main()
