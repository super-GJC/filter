#!/usr/bin/env python3
# 合并 SuRF(real-suffix) 新结果 与 已有 MD(bitsperkey) 结果，输出中文表头 CSV + Pareto 图。
# 只读既有 MD 结果与本次新跑 SuRF 结果，不修改任何既有文件。
import csv
import glob
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", "..", ".."))
MD_CSV = os.path.join(ROOT, "experiments", "compare", "summary_bitsperkey_fpr_space.csv")
RES = os.path.join(HERE, "results")
os.makedirs(RES, exist_ok=True)


def load_md():
    md = {}
    with open(MD_CSV) as f:
        for r in csv.DictReader(f):
            if r["方法"] != "MD":
                continue
            md[int(r["bits_per_key"])] = {
                "avg": float(r["平均FPR"]),
                "med": float(r["中位数FPR"]),
                "kb": float(r["占用空间_KB"]),
            }
    return md


def load_surf():
    surf = {}
    for p in glob.glob(os.path.join(HERE, "raw", "suffix_*", "summary.csv")):
        sfx = int(os.path.basename(os.path.dirname(p)).split("_")[1])
        d = {}
        with open(p) as f:
            for r in csv.DictReader(f):
                d[r["method"]] = r
        best = d["surf_1d_best"]
        andd = d["surf_1d_and"]
        surf[sfx] = {
            "best_avg": float(best["avg_fpr"]), "best_med": float(best["median_fpr"]),
            "best_kb": round(int(best["filter_bytes"]) / 1024.0, 1),
            "and_avg": float(andd["avg_fpr"]), "and_med": float(andd["median_fpr"]),
            "and_kb": round(int(andd["filter_bytes"]) / 1024.0, 1),
        }
    return surf


def main():
    md = load_md()
    surf = load_surf()

    # ---- 1) 同值对比（MD bpk 值集合）----
    same_path = os.path.join(RES, "same_value_comparison.csv")
    with open(same_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["数值", "MD_平均FPR", "MD_中位数FPR", "MD_空间KB",
                    "SuRF_Best_平均FPR", "SuRF_Best_中位数FPR", "SuRF_Best_空间KB",
                    "SuRF_AND_平均FPR", "SuRF_AND_中位数FPR", "SuRF_AND_空间KB", "备注"])
        for v in sorted(md):
            m = md[v]
            s = surf.get(v)
            if s:
                w.writerow([v, "%.5g" % m["avg"], "%.5g" % m["med"], m["kb"],
                            "%.6g" % s["best_avg"], "%.6g" % s["best_med"], s["best_kb"],
                            "%.6g" % s["and_avg"], "%.6g" % s["and_med"], s["and_kb"], ""])
            else:
                note = "real-suffix 上限64，SuRF 无法取 %d" % v
                w.writerow([v, "%.5g" % m["avg"], "%.5g" % m["med"], m["kb"],
                            "", "", "", "", "", "", note])

    # ---- 2) Pareto（空间 vs FPR，长表）----
    pareto = []
    for v, m in md.items():
        pareto.append(("MD", "bitsperkey", v, m["avg"], m["kb"]))
    for v, s in surf.items():
        pareto.append(("SuRF_1d_Best", "real-suffix", v, s["best_avg"], s["best_kb"]))
        pareto.append(("SuRF_1d_AND", "real-suffix", v, s["and_avg"], s["and_kb"]))
    pareto.sort(key=lambda x: (x[0], x[4]))
    pareto_path = os.path.join(RES, "pareto_space_fpr.csv")
    with open(pareto_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["方法", "旋钮", "参数值", "平均FPR", "占用空间KB"])
        for meth, knob, v, fpr, kb in pareto:
            w.writerow([meth, knob, v, "%.6g" % fpr, kb])

    # ---- 3) 图 ----
    fig, axes = plt.subplots(1, 2, figsize=(12, 4.6))

    # (a) 同值对比：x=数值, y=FPR
    ax = axes[0]
    xs = sorted(md)
    ax.plot(xs, [md[v]["avg"] for v in xs], "-o", label="MD (bitsperkey)")
    sxs = sorted(surf)
    ax.plot(sxs, [surf[v]["best_avg"] for v in sxs], "-s", label="SuRF-1D-Best (real-suffix)")
    ax.plot(sxs, [surf[v]["and_avg"] for v in sxs], "-^", label="SuRF-1D-AND (real-suffix)")
    ax.set_xscale("log")
    ax.set_xlabel("parameter value (MD bitsperkey  /  SuRF real-suffix)")
    ax.set_ylabel("average FPR")
    ax.set_title("Same-value comparison")
    ax.grid(True, alpha=0.3)
    ax.legend()

    # (b) Pareto：x=space, y=FPR
    ax = axes[1]
    ax.plot([md[v]["kb"] for v in xs], [md[v]["avg"] for v in xs], "-o", label="MD")
    ax.plot([surf[v]["best_kb"] for v in sxs], [surf[v]["best_avg"] for v in sxs], "-s", label="SuRF-1D-Best")
    ax.plot([surf[v]["and_kb"] for v in sxs], [surf[v]["and_avg"] for v in sxs], "-^", label="SuRF-1D-AND")
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xlabel("space (KB, log)")
    ax.set_ylabel("average FPR (log)")
    ax.set_title("Space-FPR Pareto (lower-left is better)")
    ax.grid(True, alpha=0.3)
    ax.legend()

    fig.tight_layout()
    png = os.path.join(RES, "suffix_vs_bpk.png")
    fig.savefig(png, dpi=130)

    # ---- 控制台摘要 ----
    print("value |  MD_avg/med/KB  |  SuRF-Best avg/KB | SuRF-AND avg/KB")
    for v in sorted(set(list(md) + list(surf))):
        m = md.get(v)
        s = surf.get(v)
        ma = ("%.5g/%.5g/%.0f" % (m["avg"], m["med"], m["kb"])) if m else "-"
        ba = ("%.5g/%.0f" % (s["best_avg"], s["best_kb"])) if s else "-"
        aa = ("%.5g/%.0f" % (s["and_avg"], s["and_kb"])) if s else "-"
        print("%5d | %-16s | %-18s | %s" % (v, ma, ba, aa))
    print("saved:", same_path, pareto_path, png, sep="\n  ")


if __name__ == "__main__":
    main()
