#!/usr/bin/env python3
"""Run locked doc-style study on synthetic_m5_n100k.

Order: selectivity+dimablation -> bpk -> block-size -> aggregate.
Does not overwrite existing q100/q1000 archives.
"""
from __future__ import annotations

import argparse
import csv
import math
import os
import shutil
import statistics
import subprocess
import sys
import time

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
M5 = os.path.join(ROOT, "experiments", "synthetic_m5_n100k")
DATA_DIR = os.path.join(M5, "datasets", "synth_uniform_n100k")
DATA_FILE = os.path.join(DATA_DIR, "data.txt")
Q1000 = os.path.join(M5, "workloads", "synth_uniform_q1000", "query.txt")
ABL_Q = os.path.join(M5, "ablations", "queries", "q1000")
ABL_R = os.path.join(M5, "ablations", "runs", "q1000")
SEN_R = os.path.join(M5, "sensitivity", "runs")
CMP = os.path.join(M5, "compare_ext")

MD_BIN = os.path.join(ROOT, "main")
SURF_BIN = os.path.join(ROOT, "bench", "surf_1d", "adapter", "climate_surf_bench")

D = "16,64,32,16,16"
BASE_LS = "4,4,4,4,4"
BASE_BPK = 20
SUFFIX = 8

# sm 原定 {2,2,2,2,2}（chunknum=262144）在 8GB 机器上构建期 OOM（~7GB RSS）。
# 改为 {2,4,4,4,4}：B=512（仍小于 mid 的 1024），chunknum=16384，Σbit=20。
BLOCK_CFGS = [
    ("sm", "2,4,4,4,4"),
    ("mid", "4,4,4,4,4"),
    ("C", "4,8,4,4,4"),
    ("A", "4,8,8,4,4"),
]
BPKS = [4, 8, 16, 18, 20, 22, 24, 40]
ABL_STUDIES = {
    "dimablation": ["k1", "k2", "k3", "k4", "k5"],
    "selectivity": ["m0p5", "m1p0", "m2p0", "m4p0"],
}
# Identical to archived q1000 — reuse instead of re-running MD/SuRF.
REUSE = {
    ("dimablation", "k5"),
    ("selectivity", "m1p0"),
}
ARCH_MD = os.path.join(M5, "runs", "20261117_md_synth_m5_n100k_q1000")
ARCH_BEST = os.path.join(M5, "runs", "20261117_surf_1d_best_synth_m5_n100k_q1000")
ARCH_AND = os.path.join(M5, "runs", "20261117_surf_1d_and_synth_m5_n100k_q1000")


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)


def read_fprs(path: str):
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
            if v == v:
                vals.append(v)
    return vals


def agg(vals):
    if not vals:
        return float("nan"), float("nan"), 0, 0
    return (
        sum(vals) / len(vals),
        statistics.median(vals),
        sum(1 for x in vals if x == 0.0),
        len(vals),
    )


def run(cmd, label: str) -> None:
    log("RUN " + label)
    t0 = time.time()
    r = subprocess.run(cmd, cwd=ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    dt = time.time() - t0
    if r.returncode != 0:
        raise SystemExit(f"command failed ({r.returncode}): {label}")
    log(f"OK  {label}  ({dt:.1f}s)")


def filter_bytes_of(path: str) -> int:
    if os.path.isfile(path):
        return os.path.getsize(path)
    return 0


def one_drange_bit(L: int) -> int:
    iv = math.ceil(math.sqrt(L))
    ls0 = iv * iv
    return math.ceil(math.log2(3.5 * ls0 - 1.5 * iv - 2))


def safety_ok(ls_csv: str, limit: int = 22) -> int:
    ls = [int(x) for x in ls_csv.split(",")]
    s = sum(one_drange_bit(x) for x in ls)
    if s > limit:
        raise SystemExit(f"safety fail: logical_size={ls_csv} Σbit={s} > {limit}")
    return s


def run_md(workload_dir: str, run_raw: str, logical: str, bpk: int) -> None:
    ensure_dir(run_raw)
    # Clear previous partial outputs in this raw dir only.
    for name in ("result.txt", "filter.txt", "offset.txt", "binary1.txt"):
        p = os.path.join(run_raw, name)
        if os.path.exists(p):
            os.remove(p)
    cmd = [
        MD_BIN,
        "--d", D,
        "--logical-size", logical,
        "--bitsperkey", str(bpk),
        "--data-dir", DATA_DIR + "/",
        "--workload-dir", workload_dir if workload_dir.endswith("/") else workload_dir + "/",
        "--run-dir", run_raw if run_raw.endswith("/") else run_raw + "/",
        "--skip-workload",
    ]
    tag = os.path.basename(workload_dir.rstrip("/"))
    run(cmd, f"md ls={logical} bpk={bpk} wl={tag}")


def run_surf(query: str, out_dir: str, logical: str) -> None:
    ensure_dir(out_dir)
    cmd = [
        SURF_BIN,
        "--data", DATA_FILE,
        "--query", query,
        "--out", out_dir,
        "--mode", "both",
        "--d", D,
        "--logical-size", logical,
        "--real-suffix", str(SUFFIX),
    ]
    run(cmd, f"surf ls={logical} q={os.path.basename(os.path.dirname(query))}")


def parse_surf_summary(path: str):
    out = {}
    if not os.path.isfile(path):
        return out
    with open(path) as f:
        for row in csv.DictReader(f):
            out[row["method"]] = row
    return out


def copy_reuse(study: str, var: str, dest: str) -> None:
    ensure_dir(dest)
    md_dst = os.path.join(dest, "md")
    surf_dst = os.path.join(dest, "surf")
    ensure_dir(md_dst)
    ensure_dir(surf_dst)
    shutil.copy2(os.path.join(ARCH_MD, "raw", "result.txt"), os.path.join(md_dst, "result.txt"))
    # Keep bytes from archived summary if present.
    md_sum = os.path.join(ARCH_MD, "summary.csv")
    if os.path.isfile(md_sum):
        shutil.copy2(md_sum, os.path.join(md_dst, "summary.csv"))
    shutil.copy2(os.path.join(ARCH_BEST, "raw", "result.txt"), os.path.join(surf_dst, "result_best.txt"))
    shutil.copy2(os.path.join(ARCH_AND, "raw", "result.txt"), os.path.join(surf_dst, "result_and.txt"))
    # Rebuild a tiny summary.csv for surf bytes.
    best_b = 0
    and_b = 0
    with open(os.path.join(ARCH_BEST, "summary.csv")) as f:
        row = next(csv.DictReader(f))
        best_b = int(float(row["filter_bytes"]))
    with open(os.path.join(ARCH_AND, "summary.csv")) as f:
        row = next(csv.DictReader(f))
        and_b = int(float(row["filter_bytes"]))
    with open(os.path.join(surf_dst, "summary.csv"), "w", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "method", "workload", "n_queries", "avg_fpr", "median_fpr",
                "avg_filter_time_s", "avg_process_time_s", "filter_bytes", "notes",
            ],
        )
        w.writeheader()
        w.writerow({
            "method": "surf_1d_best", "workload": "q1000_reuse", "n_queries": 1000,
            "avg_fpr": "", "median_fpr": "", "avg_filter_time_s": "", "avg_process_time_s": "",
            "filter_bytes": best_b, "notes": "reused from 20261117 archive; real_suffix=8",
        })
        w.writerow({
            "method": "surf_1d_and", "workload": "q1000_reuse", "n_queries": 1000,
            "avg_fpr": "", "median_fpr": "", "avg_filter_time_s": "", "avg_process_time_s": "",
            "filter_bytes": and_b, "notes": "reused from 20261117 archive; real_suffix=8",
        })
    log(f"REUSE archive -> {study}/{var}")


def run_ablations(only: str | None = None) -> None:
    ensure_dir(ABL_R)
    for study, variants in ABL_STUDIES.items():
        if only and only != study and only != "ablations":
            continue
        for var in variants:
            if only and only not in (study, "ablations", var):
                # allow --only dimablation / selectivity / k1 / m0p5
                if only not in variants and only != study:
                    continue
            dest = os.path.join(ABL_R, study, var)
            md_raw = os.path.join(dest, "md")
            surf_out = os.path.join(dest, "surf")
            qpath = os.path.join(ABL_Q, study, var, "query.txt")
            if not os.path.isfile(qpath):
                raise SystemExit("missing query: " + qpath)
            if (study, var) in REUSE:
                copy_reuse(study, var, dest)
                continue
            md_done = os.path.isfile(os.path.join(md_raw, "result.txt"))
            surf_done = os.path.isfile(os.path.join(surf_out, "result_best.txt")) and os.path.isfile(
                os.path.join(surf_out, "result_and.txt")
            )
            if md_done and surf_done:
                log(f"SKIP existing {study}/{var}")
                continue
            if not md_done:
                run_md(os.path.join(ABL_Q, study, var), md_raw, BASE_LS, BASE_BPK)
            else:
                log(f"KEEP existing md {study}/{var}")
            if not surf_done:
                run_surf(qpath, surf_out, BASE_LS)


def run_bpk() -> None:
    ensure_dir(os.path.join(SEN_R, "bpk"))
    # SuRF reference once (suffix=8, base grid, q1000)
    surf_ref = os.path.join(SEN_R, "bpk", "surf_ref_suffix8")
    if not os.path.isfile(os.path.join(surf_ref, "result_best.txt")):
        run_surf(Q1000, surf_ref, BASE_LS)
    else:
        log("SKIP existing surf_ref_suffix8")
    for bpk in BPKS:
        tag = f"bpk{bpk}"
        dest = os.path.join(SEN_R, "bpk", tag)
        md_raw = os.path.join(dest, "md")
        if os.path.isfile(os.path.join(md_raw, "result.txt")):
            log(f"SKIP existing {tag}")
            continue
        # bpk=20 base can reuse archived MD q1000
        if bpk == BASE_BPK and os.path.isfile(os.path.join(ARCH_MD, "raw", "result.txt")):
            ensure_dir(md_raw)
            shutil.copy2(os.path.join(ARCH_MD, "raw", "result.txt"), os.path.join(md_raw, "result.txt"))
            ft = os.path.join(ARCH_MD, "raw", "filter.txt")
            if os.path.isfile(ft):
                # do not copy 237MB filter; record bytes from summary
                pass
            shutil.copy2(os.path.join(ARCH_MD, "summary.csv"), os.path.join(md_raw, "summary.csv"))
            log("REUSE archive MD for bpk20")
            continue
        run_md(os.path.join(M5, "workloads", "synth_uniform_q1000"), md_raw, BASE_LS, bpk)


def run_blocks() -> None:
    ensure_dir(os.path.join(SEN_R, "blocksize"))
    for name, ls in BLOCK_CFGS:
        safety_ok(ls, 22)
        dest = os.path.join(SEN_R, "blocksize", name)
        md_raw = os.path.join(dest, "md")
        surf_out = os.path.join(dest, "surf")
        if name == "mid" and os.path.isfile(os.path.join(ARCH_MD, "raw", "result.txt")):
            if not os.path.isfile(os.path.join(md_raw, "result.txt")):
                ensure_dir(md_raw)
                ensure_dir(surf_out)
                shutil.copy2(os.path.join(ARCH_MD, "raw", "result.txt"), os.path.join(md_raw, "result.txt"))
                shutil.copy2(os.path.join(ARCH_MD, "summary.csv"), os.path.join(md_raw, "summary.csv"))
                shutil.copy2(os.path.join(ARCH_BEST, "raw", "result.txt"), os.path.join(surf_out, "result_best.txt"))
                shutil.copy2(os.path.join(ARCH_AND, "raw", "result.txt"), os.path.join(surf_out, "result_and.txt"))
                # surf summary with bytes
                best_b = and_b = 0
                with open(os.path.join(ARCH_BEST, "summary.csv")) as f:
                    best_b = int(float(next(csv.DictReader(f))["filter_bytes"]))
                with open(os.path.join(ARCH_AND, "summary.csv")) as f:
                    and_b = int(float(next(csv.DictReader(f))["filter_bytes"]))
                with open(os.path.join(surf_out, "summary.csv"), "w", newline="") as f:
                    w = csv.writer(f)
                    w.writerow(["method", "filter_bytes", "notes"])
                    w.writerow(["surf_1d_best", best_b, "reuse mid archive"])
                    w.writerow(["surf_1d_and", and_b, "reuse mid archive"])
                log("REUSE archive for block mid")
            else:
                log("SKIP existing block mid")
            continue
        if os.path.isfile(os.path.join(md_raw, "result.txt")) and os.path.isfile(
            os.path.join(surf_out, "result_best.txt")
        ):
            log(f"SKIP existing block {name}")
            continue
        run_md(os.path.join(M5, "workloads", "synth_uniform_q1000"), md_raw, ls, BASE_BPK)
        run_surf(Q1000, surf_out, ls)


def md_bytes_from(md_dir: str) -> int:
    # Prefer filter.txt size; else summary.csv
    ft = os.path.join(md_dir, "filter.txt")
    if os.path.isfile(ft):
        return os.path.getsize(ft)
    sm = os.path.join(md_dir, "summary.csv")
    if os.path.isfile(sm):
        with open(sm) as f:
            row = next(csv.DictReader(f))
            if "filter_bytes" in row and row["filter_bytes"]:
                return int(float(row["filter_bytes"]))
    # archived raw may live elsewhere for reuse cases
    return 0


def surf_bytes_from(surf_dir: str):
    sm = parse_surf_summary(os.path.join(surf_dir, "summary.csv"))
    best = and_ = 0
    if "surf_1d_best" in sm and sm["surf_1d_best"].get("filter_bytes"):
        best = int(float(sm["surf_1d_best"]["filter_bytes"]))
    if "surf_1d_and" in sm and sm["surf_1d_and"].get("filter_bytes"):
        and_ = int(float(sm["surf_1d_and"]["filter_bytes"]))
    # alternate minimal summary
    if best == 0 and and_ == 0 and os.path.isfile(os.path.join(surf_dir, "summary.csv")):
        with open(os.path.join(surf_dir, "summary.csv")) as f:
            for row in csv.DictReader(f):
                if row.get("method") == "surf_1d_best":
                    best = int(float(row["filter_bytes"]))
                if row.get("method") == "surf_1d_and":
                    and_ = int(float(row["filter_bytes"]))
    return best, and_


def load_meta():
    meta = {}
    p = os.path.join(ABL_Q, "variants_meta.csv")
    with open(p) as f:
        for row in csv.DictReader(f):
            meta[(row["study"], row["variant"])] = row
    return meta


def aggregate() -> None:
    ensure_dir(CMP)
    meta = load_meta()
    arch_md_bytes = 0
    with open(os.path.join(ARCH_MD, "summary.csv")) as f:
        arch_md_bytes = int(float(next(csv.DictReader(f))["filter_bytes"]))
    arch_best = arch_and = 0
    with open(os.path.join(ARCH_BEST, "summary.csv")) as f:
        arch_best = int(float(next(csv.DictReader(f))["filter_bytes"]))
    with open(os.path.join(ARCH_AND, "summary.csv")) as f:
        arch_and = int(float(next(csv.DictReader(f))["filter_bytes"]))

    # --- ablations ---
    dim_path = os.path.join(CMP, "dimablation_summary.csv")
    sel_path = os.path.join(CMP, "selectivity_summary.csv")
    with open(dim_path, "w", newline="") as fd, open(sel_path, "w", newline="") as fs:
        hd = [
            "约束维数k", "约束维", "整体选择度",
            "MD平均FPR", "MD中位数FPR", "MD_FPR为0条数",
            "SuRF_1d_AND平均FPR", "SuRF_1d_AND中位数FPR",
            "SuRF_1d_Best平均FPR", "SuRF_1d_Best中位数FPR",
            "MD占用字节", "SuRF_AND占用字节", "SuRF_Best占用字节",
        ]
        hs = [
            "区间倍数m", "整体选择度",
            "MD平均FPR", "MD中位数FPR", "MD_FPR为0条数",
            "SuRF_1d_AND平均FPR", "SuRF_1d_AND中位数FPR",
            "SuRF_1d_Best平均FPR", "SuRF_1d_Best中位数FPR",
            "MD占用字节", "SuRF_AND占用字节", "SuRF_Best占用字节",
        ]
        wd = csv.writer(fd); wd.writerow(hd)
        ws = csv.writer(fs); ws.writerow(hs)
        for study, variants in ABL_STUDIES.items():
            for var in variants:
                dest = os.path.join(ABL_R, study, var)
                md_p = os.path.join(dest, "md", "result.txt")
                best_p = os.path.join(dest, "surf", "result_best.txt")
                and_p = os.path.join(dest, "surf", "result_and.txt")
                if not (os.path.isfile(md_p) and os.path.isfile(best_p) and os.path.isfile(and_p)):
                    log(f"AGG skip missing {study}/{var}")
                    continue
                md_a, md_m, md_z, md_n = agg(read_fprs(md_p))
                ba, bm, _, _ = agg(read_fprs(best_p))
                aa, am, _, _ = agg(read_fprs(and_p))
                mb = md_bytes_from(os.path.join(dest, "md")) or arch_md_bytes
                sb, sa = surf_bytes_from(os.path.join(dest, "surf"))
                if not sb:
                    sb = arch_best
                if not sa:
                    sa = arch_and
                mrow = meta.get((study, var), {})
                sel = mrow.get("avg_overall_selectivity", "")
                if study == "dimablation":
                    k = int(var[1:])
                    cons = mrow.get("description", "").replace("constrained=", "")
                    wd.writerow([k, cons, sel, f"{md_a:.6g}", f"{md_m:.6g}", f"{md_z}/{md_n}",
                                 f"{aa:.6g}", f"{am:.6g}", f"{ba:.6g}", f"{bm:.6g}", mb, sa, sb])
                else:
                    m = var.replace("m", "").replace("p", ".")
                    ws.writerow([m, sel, f"{md_a:.6g}", f"{md_m:.6g}", f"{md_z}/{md_n}",
                                 f"{aa:.6g}", f"{am:.6g}", f"{ba:.6g}", f"{bm:.6g}", mb, sa, sb])

    # --- bpk ---
    bpk_path = os.path.join(CMP, "bitsperkey_summary.csv")
    surf_ref = os.path.join(SEN_R, "bpk", "surf_ref_suffix8")
    # Prefer fresh surf ref; else archive
    if os.path.isfile(os.path.join(surf_ref, "result_best.txt")):
        sba, sbm, _, _ = agg(read_fprs(os.path.join(surf_ref, "result_best.txt")))
        saa, sam, _, _ = agg(read_fprs(os.path.join(surf_ref, "result_and.txt")))
        sb_b, sa_b = surf_bytes_from(surf_ref)
        if not sb_b:
            sb_b, sa_b = arch_best, arch_and
    else:
        sba, sbm, _, _ = agg(read_fprs(os.path.join(ARCH_BEST, "raw", "result.txt")))
        saa, sam, _, _ = agg(read_fprs(os.path.join(ARCH_AND, "raw", "result.txt")))
        sb_b, sa_b = arch_best, arch_and
    with open(bpk_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow([
            "bits_per_key", "方法", "平均FPR", "中位数FPR", "FPR为0条数", "占用字节", "备注",
        ])
        for bpk in BPKS:
            md_p = os.path.join(SEN_R, "bpk", f"bpk{bpk}", "md", "result.txt")
            if not os.path.isfile(md_p):
                continue
            a, m, z, n = agg(read_fprs(md_p))
            mb = md_bytes_from(os.path.join(SEN_R, "bpk", f"bpk{bpk}", "md"))
            if bpk == 20 and mb == 0:
                mb = arch_md_bytes
            w.writerow([bpk, "MD", f"{a:.6g}", f"{m:.6g}", f"{z}/{n}", mb, "logical_size=4,4,4,4,4"])
            w.writerow([bpk, "SuRF_1d_AND", f"{saa:.6g}", f"{sam:.6g}", "", sa_b, "real_suffix=8 固定参照"])
            w.writerow([bpk, "SuRF_1d_Best", f"{sba:.6g}", f"{sbm:.6g}", "", sb_b, "real_suffix=8 固定参照"])

    # --- block size ---
    blk_path = os.path.join(CMP, "blocksize_summary.csv")
    with open(blk_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow([
            "档位", "logical_size", "Σbit", "每块逻辑单元数B",
            "方法", "平均FPR", "中位数FPR", "FPR为0条数", "占用字节",
        ])
        for name, ls in BLOCK_CFGS:
            dest = os.path.join(SEN_R, "blocksize", name)
            md_p = os.path.join(dest, "md", "result.txt")
            best_p = os.path.join(dest, "surf", "result_best.txt")
            and_p = os.path.join(dest, "surf", "result_and.txt")
            if not (os.path.isfile(md_p) and os.path.isfile(best_p)):
                continue
            sbit = safety_ok(ls, 22)
            B = 1
            for x in ls.split(","):
                B *= int(x)
            rows = [
                ("MD", md_p, md_bytes_from(os.path.join(dest, "md")) or (arch_md_bytes if name == "mid" else 0)),
                ("SuRF_1d_AND", and_p, surf_bytes_from(os.path.join(dest, "surf"))[1] or (arch_and if name == "mid" else 0)),
                ("SuRF_1d_Best", best_p, surf_bytes_from(os.path.join(dest, "surf"))[0] or (arch_best if name == "mid" else 0)),
            ]
            for method, path, nbytes in rows:
                a, m, z, n = agg(read_fprs(path))
                w.writerow([name, ls, sbit, B, method, f"{a:.6g}", f"{m:.6g}", f"{z}/{n}", nbytes])

    # --- space-FPR long table ---
    long_path = os.path.join(CMP, "space_fpr_points.csv")
    with open(long_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["来源", "配置", "方法", "平均FPR", "占用字节"])
        # from bpk
        if os.path.isfile(bpk_path):
            with open(bpk_path) as bf:
                for row in csv.DictReader(bf):
                    w.writerow(["bitsperkey", f"bpk={row['bits_per_key']}", row["方法"], row["平均FPR"], row["占用字节"]])
        if os.path.isfile(blk_path):
            with open(blk_path) as bf:
                for row in csv.DictReader(bf):
                    w.writerow(["blocksize", f"{row['档位']}:{row['logical_size']}", row["方法"], row["平均FPR"], row["占用字节"]])

    log("AGG wrote " + CMP)


def try_plot() -> None:
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except Exception as e:
        log(f"plot skip: {e}")
        return

    def load_cn(path):
        rows = list(csv.DictReader(open(path)))
        return rows

    # dimablation
    p = os.path.join(CMP, "dimablation_summary.csv")
    if os.path.isfile(p):
        rows = load_cn(p)
        ks = [int(r["约束维数k"]) for r in rows]
        plt.figure(figsize=(7, 4.5))
        plt.plot(ks, [float(r["MD平均FPR"]) for r in rows], "o-", label="MD")
        plt.plot(ks, [float(r["SuRF_1d_AND平均FPR"]) for r in rows], "s-", label="SuRF-1D-AND")
        plt.plot(ks, [float(r["SuRF_1d_Best平均FPR"]) for r in rows], "^-", label="SuRF-1D-Best")
        plt.xlabel("约束维数 k"); plt.ylabel("平均 FPR"); plt.grid(True, alpha=0.3)
        plt.legend(); plt.tight_layout()
        plt.savefig(os.path.join(CMP, "fpr_vs_dimcount.png"), dpi=140); plt.close()

    p = os.path.join(CMP, "selectivity_summary.csv")
    if os.path.isfile(p):
        rows = load_cn(p)
        ms = [float(r["区间倍数m"]) for r in rows]
        plt.figure(figsize=(7, 4.5))
        plt.plot(ms, [float(r["MD平均FPR"]) for r in rows], "o-", label="MD")
        plt.plot(ms, [float(r["SuRF_1d_AND平均FPR"]) for r in rows], "s-", label="SuRF-1D-AND")
        plt.plot(ms, [float(r["SuRF_1d_Best平均FPR"]) for r in rows], "^-", label="SuRF-1D-Best")
        plt.xlabel("区间倍数 m"); plt.ylabel("平均 FPR"); plt.grid(True, alpha=0.3)
        plt.legend(); plt.tight_layout()
        plt.savefig(os.path.join(CMP, "fpr_vs_selectivity.png"), dpi=140); plt.close()

    p = os.path.join(CMP, "space_fpr_points.csv")
    if os.path.isfile(p):
        rows = load_cn(p)
        plt.figure(figsize=(7.5, 5))
        markers = {"MD": "o", "SuRF_1d_AND": "s", "SuRF_1d_Best": "^"}
        for method, mk in markers.items():
            xs, ys = [], []
            for r in rows:
                if r["方法"] != method:
                    continue
                try:
                    x = float(r["占用字节"]); y = float(r["平均FPR"])
                except ValueError:
                    continue
                if x > 0:
                    xs.append(x / 1024.0); ys.append(y)
            if xs:
                plt.scatter(xs, ys, marker=mk, label=method, s=40)
        plt.xscale("log"); plt.yscale("log")
        plt.xlabel("占用空间 (KB, log)"); plt.ylabel("平均 FPR (log)")
        plt.grid(True, which="both", alpha=0.3); plt.legend(); plt.tight_layout()
        plt.savefig(os.path.join(CMP, "space_fpr_scatter.png"), dpi=140); plt.close()
    log("plots done")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--stage", default="all",
                    choices=["all", "ablations", "bpk", "blocks", "aggregate", "plot"])
    args = ap.parse_args()
    if not os.path.isfile(MD_BIN) or not os.path.isfile(SURF_BIN):
        raise SystemExit("missing binaries")
    if args.stage in ("all", "ablations"):
        run_ablations()
    if args.stage in ("all", "bpk"):
        run_bpk()
    if args.stage in ("all", "blocks"):
        run_blocks()
    if args.stage in ("all", "aggregate", "plot"):
        aggregate()
        try_plot()
    log("DONE stage=" + args.stage)


if __name__ == "__main__":
    main()
