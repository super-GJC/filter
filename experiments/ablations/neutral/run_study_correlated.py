#!/usr/bin/env python3
"""增量研究：在「未筛选」的 climate_q200_correlated 上重跑维度消融 + 选择度扫描。

目的：消除主图基负载 climate_q200 的「被 MD FPR<=0.1 筛选过」偏差，
用一个未经结果筛选、命中充足、可复现（固定种子 20260917）的负载复核结论。

约束：不修改任何既有代码/结果；只新增到 experiments/ablations/neutral/ 下，
运行的是同一套已编译二进制 ./main 与 climate_surf_bench，口径与 ../run_study.py 完全一致。
"""
import csv
import os
import statistics
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ABL = os.path.dirname(HERE)                 # experiments/ablations
ROOT = os.path.abspath(os.path.join(HERE, "..", "..", ".."))
sys.path.insert(0, ABL)
import gen_variants as gv                    # 复用生成逻辑（只读）

DATA = os.path.join(ROOT, "data", "test", "climate")
MD = os.path.join(ROOT, "main")
SURF = os.path.join(ROOT, "bench", "surf_1d", "adapter", "climate_surf_bench")

D = [4, 1121, 68, 7]
M = 4
ORDER = sorted(range(M), key=lambda j: D[j], reverse=True)  # [1,2,3,0] 高基数优先
LOGICAL = "2,16,16,4"
BPK = "20"
SUFFIX = "8"
MD_REPS = 3

BASE = "q200_correlated"
BASE_QUERY = os.path.join(ROOT, "experiments", "workloads", "climate_" + BASE, "query.txt")
QROOT = os.path.join(HERE, "queries", BASE)
RROOT = os.path.join(HERE, "runs", BASE)
RES = os.path.join(HERE, "results")

STUDIES = {
    "dimablation": ["k1", "k2", "k3", "k4"],
    "selectivity": ["m0p5", "m1p0", "m2p0", "m4p0"],
}
MULTIPLIERS = {"m0p5": 0.5, "m1p0": 1.0, "m2p0": 2.0, "m4p0": 4.0}


def read_col7(path):
    vals = []
    with open(path) as f:
        for line in f:
            p = line.split()
            if len(p) < 7:
                continue
            try:
                v = float(p[6])
            except ValueError:
                continue
            if v == v:  # 非 nan
                vals.append(v)
    return vals


def avg(xs):
    return sum(xs) / len(xs) if xs else float("nan")


def med(xs):
    return statistics.median(xs) if xs else float("nan")


def run(cmd):
    subprocess.run(cmd, cwd=ROOT, stdout=subprocess.DEVNULL,
                   stderr=subprocess.DEVNULL, check=False)


def parse_surf_summary(path):
    out = {}
    with open(path) as f:
        for row in csv.DictReader(f):
            out[row["method"]] = row
    return out


def desc_for_dim(var):
    k = int(var[1:])
    return "constrained=" + " ".join("dim%d" % j for j in ORDER[:k])


def main():
    os.makedirs(RES, exist_ok=True)
    qs = gv.load_queries(BASE_QUERY, M)
    print("[base] %s  n=%d" % (BASE, len(qs)), flush=True)

    # 1) 生成变体（只读现有负载）
    variants = {}
    for k in range(1, M + 1):
        variants[("dimablation", "k%d" % k)] = gv.make_ablation(qs, M, D, ORDER, k)
    for var, m in MULTIPLIERS.items():
        variants[("selectivity", var)] = gv.make_selectivity(qs, M, D, m)
    for (study, var), v in variants.items():
        qpath = os.path.join(QROOT, study, var, "query.txt")
        gv.write_queries(qpath, v)
        ov, _ = gv.stats(v, M, D)
        variants[(study, var)] = (v, ov)

    # 2) 运行
    records = []
    for (study, var), (v, ov) in variants.items():
        qdir = os.path.join(QROOT, study, var)
        qpath = os.path.join(qdir, "query.txt")
        runbase = os.path.join(RROOT, study, var)
        os.makedirs(runbase, exist_ok=True)

        rep_avgs = []
        md_med = float("nan")
        md_bytes = 0
        for r in range(1, MD_REPS + 1):
            rdir = os.path.join(runbase, "md_rep%d" % r)
            os.makedirs(rdir, exist_ok=True)
            res = os.path.join(rdir, "result.txt")
            if not (os.path.exists(res) and read_col7(res)):
                run([MD, "--logical-size", LOGICAL, "--bitsperkey", BPK,
                     "--data-dir", DATA + os.sep, "--workload-dir", qdir + os.sep,
                     "--run-dir", rdir + os.sep])
            vals = read_col7(res)
            rep_avgs.append(avg(vals))
            if r == 1:
                md_med = med(vals)
            fp = os.path.join(rdir, "filter.txt")
            if os.path.exists(fp):
                md_bytes = os.path.getsize(fp)
            for junk in ["binary1.txt", "filter.txt", "filter_workload.txt",
                         "offset.txt", "offset_workload.txt", "append_cursor.txt",
                         "result_workload.txt"]:
                jp = os.path.join(rdir, junk)
                if os.path.exists(jp):
                    os.remove(jp)
        if md_bytes == 0:  # 复用了已有结果且 filter.txt 已清理：本配置字节数与数据/负载无关，为常量
            md_bytes = 2613248

        sdir = os.path.join(runbase, "surf")
        os.makedirs(sdir, exist_ok=True)
        if not (os.path.exists(os.path.join(sdir, "result_best.txt"))
                and os.path.exists(os.path.join(sdir, "result_and.txt"))):
            run([SURF, "--data", os.path.join(DATA, "data.txt"), "--query", qpath,
                 "--out", sdir, "--mode", "both", "--logical-size", LOGICAL,
                 "--real-suffix", SUFFIX])
        sb = read_col7(os.path.join(sdir, "result_best.txt"))
        sa = read_col7(os.path.join(sdir, "result_and.txt"))
        ssum = parse_surf_summary(os.path.join(sdir, "summary.csv"))
        best_bytes = int(ssum.get("surf_1d_best", {}).get("filter_bytes", 0) or 0)
        and_bytes = int(ssum.get("surf_1d_and", {}).get("filter_bytes", 0) or 0)

        rec = dict(base=BASE, study=study, variant=var, overall_sel=ov,
                   md_mean=avg(rep_avgs), md_min=min(rep_avgs), md_max=max(rep_avgs),
                   md_med=md_med, md_bytes=md_bytes,
                   best_avg=avg(sb), best_med=med(sb), best_bytes=best_bytes,
                   and_avg=avg(sa), and_med=med(sa), and_bytes=and_bytes)
        records.append(rec)
        print("[done] %-12s %-6s sel=%.5g md=%.5g best=%.5g and=%.5g"
              % (study, var, ov, rec["md_mean"], rec["best_avg"], rec["and_avg"]), flush=True)

    # 3) 明细（与既有 detail_with_minmax.tsv 同列）
    cols = ["base", "study", "variant", "overall_sel", "md_mean", "md_min", "md_max",
            "md_med", "md_bytes", "best_avg", "best_med", "best_bytes",
            "and_avg", "and_med", "and_bytes"]
    corr_tsv = os.path.join(RES, "detail_correlated.tsv")
    with open(corr_tsv, "w") as f:
        f.write("\t".join(cols) + "\n")
        for rec in records:
            f.write("\t".join(str(rec[k]) for k in cols) + "\n")

    # 4) 与既有两基负载合并 -> detail_all.tsv
    existing = os.path.join(ABL, "results", "detail_with_minmax.tsv")
    all_rows = []
    if os.path.exists(existing):
        with open(existing) as f:
            for r in csv.DictReader(f, delimiter="\t"):
                all_rows.append(r)
    for rec in records:
        all_rows.append({k: str(rec[k]) for k in cols})
    all_tsv = os.path.join(RES, "detail_all.tsv")
    with open(all_tsv, "w") as f:
        f.write("\t".join(cols) + "\n")
        for r in all_rows:
            f.write("\t".join(str(r[k]) for k in cols) + "\n")

    # 5) 中文表头汇总（三基负载）
    def kb(b):
        return round(b / 1024.0, 1)

    dim_csv = os.path.join(RES, "dimablation_3base.csv")
    with open(dim_csv, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["基负载", "约束维数k", "约束的维度", "平均整体选择度",
                    "方法", "平均FPR", "中位数FPR", "占用空间_KB"])
        for r in all_rows:
            if r["study"] != "dimablation":
                continue
            desc = desc_for_dim(r["variant"])
            for meth, a, m, b in [
                ("MD", r["md_mean"], r["md_med"], r["md_bytes"]),
                ("SuRF_1d_BEST", r["best_avg"], r["best_med"], r["best_bytes"]),
                ("SuRF_1d_AND", r["and_avg"], r["and_med"], r["and_bytes"]),
            ]:
                w.writerow([r["base"], r["variant"], desc, "%.5g" % float(r["overall_sel"]),
                            meth, "%.6g" % float(a), "%.6g" % float(m), kb(int(b))])

    sel_csv = os.path.join(RES, "selectivity_3base.csv")
    with open(sel_csv, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["基负载", "区间缩放倍数", "平均整体选择度",
                    "方法", "平均FPR", "中位数FPR", "占用空间_KB"])
        for r in all_rows:
            if r["study"] != "selectivity":
                continue
            for meth, a, m, b in [
                ("MD", r["md_mean"], r["md_med"], r["md_bytes"]),
                ("SuRF_1d_BEST", r["best_avg"], r["best_med"], r["best_bytes"]),
                ("SuRF_1d_AND", r["and_avg"], r["and_med"], r["and_bytes"]),
            ]:
                w.writerow([r["base"], r["variant"], "%.5g" % float(r["overall_sel"]),
                            meth, "%.6g" % float(a), "%.6g" % float(m), kb(int(b))])

    print("ALL DONE ->", RES, flush=True)


if __name__ == "__main__":
    main()
