#!/usr/bin/env python3
# 运行"维度消融 + 选择度扫描"，只调用现有二进制，不修改任何既有代码/结果。
# 输出全在新目录 experiments/ablations/ 下：
#   runs/<base>/<study>/<variant>/md_rep{1..3}/result.txt + /surf/{result_best.txt,result_and.txt,summary.csv}
#   results/dimablation_summary.csv, results/selectivity_summary.csv（中文表头）
import csv
import os
import statistics
import subprocess

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
ABL = os.path.join(ROOT, "experiments", "ablations")
QROOT = os.path.join(ABL, "queries")
RROOT = os.path.join(ABL, "runs")
RES = os.path.join(ABL, "results")
DATA = os.path.join(ROOT, "data", "test", "climate")
MD = os.path.join(ROOT, "main")
SURF = os.path.join(ROOT, "bench", "surf_1d", "adapter", "climate_surf_bench")
LOGICAL = "2,16,16,4"
BPK = "20"
SUFFIX = "8"
MD_REPS = 3

BASES = ["q200", "q200_mixed"]
STUDIES = {
    "dimablation": ["k1", "k2", "k3", "k4"],
    "selectivity": ["m0p5", "m1p0", "m2p0", "m4p0"],
}


def read_col7(path):
    vals = []
    with open(path) as f:
        for line in f:
            parts = line.split()
            if len(parts) < 7:
                continue
            try:
                v = float(parts[6])
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
    subprocess.run(cmd, cwd=ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)


def md_filter_bytes(summary_rows):
    return summary_rows


def parse_surf_summary(path):
    out = {}
    with open(path) as f:
        r = csv.DictReader(f)
        for row in r:
            out[row["method"]] = row
    return out


def load_meta(base):
    meta = {}
    p = os.path.join(QROOT, base, "variants_meta.csv")
    with open(p) as f:
        for row in csv.DictReader(f):
            meta[(row["study"], row["variant"])] = row
    return meta


def main():
    os.makedirs(RES, exist_ok=True)
    dim_rows = []
    sel_rows = []

    for base in BASES:
        meta = load_meta(base)
        qbase = os.path.join(QROOT, base)
        for study, variants in STUDIES.items():
            for var in variants:
                qdir = os.path.join(qbase, study, var)
                runbase = os.path.join(RROOT, base, study, var)
                os.makedirs(runbase, exist_ok=True)

                # --- MD: 多重复 ---
                rep_avgs = []
                md_median = float("nan")
                md_bytes = 0
                for r in range(1, MD_REPS + 1):
                    rdir = os.path.join(runbase, "md_rep%d" % r)
                    os.makedirs(rdir, exist_ok=True)
                    run([MD, "--logical-size", LOGICAL, "--bitsperkey", BPK,
                         "--data-dir", DATA + os.sep, "--workload-dir", qdir + os.sep,
                         "--run-dir", rdir + os.sep])
                    res = os.path.join(rdir, "result.txt")
                    vals = read_col7(res)
                    rep_avgs.append(avg(vals))
                    if r == 1:
                        md_median = med(vals)
                    fp = os.path.join(rdir, "filter.txt")
                    if os.path.exists(fp):
                        md_bytes = os.path.getsize(fp)
                    # 清理大的中间产物，保留 result.txt
                    for junk in ["binary1.txt", "filter.txt", "filter_workload.txt",
                                 "offset.txt", "offset_workload.txt", "append_cursor.txt",
                                 "result_workload.txt"]:
                        jp = os.path.join(rdir, junk)
                        if os.path.exists(jp):
                            os.remove(jp)

                # --- SuRF ---
                sdir = os.path.join(runbase, "surf")
                os.makedirs(sdir, exist_ok=True)
                run([SURF, "--data", os.path.join(DATA, "data.txt"),
                     "--query", os.path.join(qdir, "query.txt"), "--out", sdir,
                     "--mode", "both", "--logical-size", LOGICAL, "--real-suffix", SUFFIX])
                sb = read_col7(os.path.join(sdir, "result_best.txt"))
                sa = read_col7(os.path.join(sdir, "result_and.txt"))
                ssum = parse_surf_summary(os.path.join(sdir, "summary.csv"))
                best_bytes = int(ssum.get("surf_1d_best", {}).get("filter_bytes", 0) or 0)
                and_bytes = int(ssum.get("surf_1d_and", {}).get("filter_bytes", 0) or 0)

                ov = float(meta[(study, var)]["avg_overall_selectivity"])
                desc = meta[(study, var)]["description"]

                rec = {
                    "base": base, "study": study, "var": var, "desc": desc, "ov": ov,
                    "md_mean": avg(rep_avgs), "md_min": min(rep_avgs), "md_max": max(rep_avgs),
                    "md_med": md_median, "md_bytes": md_bytes,
                    "best_avg": avg(sb), "best_med": med(sb), "best_bytes": best_bytes,
                    "and_avg": avg(sa), "and_med": med(sa), "and_bytes": and_bytes,
                }
                (dim_rows if study == "dimablation" else sel_rows).append(rec)
                print("[done] %-10s %-12s %-6s md=%.5g best=%.5g and=%.5g"
                      % (base, study, var, rec["md_mean"], rec["best_avg"], rec["and_avg"]), flush=True)

    def kb(b):
        return round(b / 1024.0, 1)

    with open(os.path.join(RES, "dimablation_summary.csv"), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["基负载", "约束维数k", "约束的维度", "平均整体选择度",
                    "方法", "平均FPR", "中位数FPR", "占用空间_KB"])
        for rec in dim_rows:
            k = rec["var"]
            for meth, a, m, b in [
                ("MD", rec["md_mean"], rec["md_med"], rec["md_bytes"]),
                ("SuRF_1d_BEST", rec["best_avg"], rec["best_med"], rec["best_bytes"]),
                ("SuRF_1d_AND", rec["and_avg"], rec["and_med"], rec["and_bytes"]),
            ]:
                w.writerow([rec["base"], k, rec["desc"], "%.5g" % rec["ov"],
                            meth, "%.6g" % a, "%.6g" % m, kb(b)])

    with open(os.path.join(RES, "selectivity_summary.csv"), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["基负载", "区间缩放倍数", "平均整体选择度",
                    "方法", "平均FPR", "中位数FPR", "占用空间_KB"])
        for rec in sel_rows:
            for meth, a, m, b in [
                ("MD", rec["md_mean"], rec["md_med"], rec["md_bytes"]),
                ("SuRF_1d_BEST", rec["best_avg"], rec["best_med"], rec["best_bytes"]),
                ("SuRF_1d_AND", rec["and_avg"], rec["and_med"], rec["and_bytes"]),
            ]:
                w.writerow([rec["base"], rec["var"], "%.5g" % rec["ov"],
                            meth, "%.6g" % a, "%.6g" % m, kb(b)])

    # 机器可读的明细（含 min/max）
    with open(os.path.join(RES, "detail_with_minmax.tsv"), "w") as f:
        f.write("base\tstudy\tvariant\toverall_sel\tmd_mean\tmd_min\tmd_max\tmd_med\tmd_bytes"
                "\tbest_avg\tbest_med\tbest_bytes\tand_avg\tand_med\tand_bytes\n")
        for rec in dim_rows + sel_rows:
            f.write("\t".join(str(rec[k]) for k in
                              ["base", "study", "var", "ov", "md_mean", "md_min", "md_max", "md_med", "md_bytes",
                               "best_avg", "best_med", "best_bytes", "and_avg", "and_med", "and_bytes"]) + "\n")
    print("ALL DONE", flush=True)


if __name__ == "__main__":
    main()
