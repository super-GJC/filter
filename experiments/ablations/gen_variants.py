#!/usr/bin/env python3
# 只读地基于现有 workload 生成两类变体查询（不修改任何既有文件/结果）：
#   1) 维度消融 dimablation：只保留前 k 个"高基数"维的约束，其余维放宽为全域 [0,d-1]
#   2) 选择度扫描 selectivity：把每条查询各维区间宽度按倍数 m 围绕中点缩放（裁剪到值域内）
# 用法（在仓库根目录）：
#   python3 experiments/ablations/gen_variants.py --base experiments/workloads/<wl>/query.txt \
#       --tag <wl> --out-root experiments/ablations/queries
import argparse
import csv
import os


def parse_ints(s):
    return [int(x) for x in s.replace(",", " ").split()]


def load_queries(path, M):
    qs = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = [int(x) for x in line.split()]
            if len(parts) < 2 * M:
                raise SystemExit("query line has < %d ints: %s" % (2 * M, line))
            qs.append(parts[: 2 * M])
    return qs


def write_queries(path, qs):
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)
    with open(path, "w") as f:
        for q in qs:
            f.write(" ".join(str(x) for x in q) + "\n")


def make_ablation(qs, M, d, order, k):
    relaxed = set(order[k:])
    out = []
    for q in qs:
        nq = list(q)
        for j in relaxed:
            nq[2 * j] = 0
            nq[2 * j + 1] = d[j] - 1
        out.append(nq)
    return out


def make_selectivity(qs, M, d, m):
    out = []
    for q in qs:
        nq = list(q)
        for j in range(M):
            L, H = q[2 * j], q[2 * j + 1]
            w = H - L + 1
            nw = max(1, min(d[j], int(round(w * m))))
            center = (L + H) / 2.0
            nl = int(round(center - (nw - 1) / 2.0))
            if nl < 0:
                nl = 0
            if nl + nw - 1 > d[j] - 1:
                nl = d[j] - nw
            nq[2 * j] = nl
            nq[2 * j + 1] = nl + nw - 1
        out.append(nq)
    return out


def stats(qs, M, d):
    n = len(qs)
    cov = [0.0] * M
    overall = 0.0
    for q in qs:
        p = 1.0
        for j in range(M):
            c = (q[2 * j + 1] - q[2 * j] + 1) / d[j]
            cov[j] += c
            p *= c
        overall += p
    return overall / n, [c / n for c in cov]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", required=True)
    ap.add_argument("--tag", required=True)
    ap.add_argument("--out-root", required=True)
    ap.add_argument("--d", default="4,1121,68,7")
    ap.add_argument("--multipliers", default="0.5,1.0,2.0,4.0")
    args = ap.parse_args()

    d = parse_ints(args.d)
    M = len(d)
    qs = load_queries(args.base, M)
    order = sorted(range(M), key=lambda j: d[j], reverse=True)  # 高基数优先
    base_dir = os.path.join(args.out_root, args.tag)

    rows = []

    for k in range(1, M + 1):
        v = make_ablation(qs, M, d, order, k)
        rel = os.path.join("dimablation", "k%d" % k, "query.txt")
        write_queries(os.path.join(base_dir, rel), v)
        ov, cov = stats(v, M, d)
        cons = " ".join("dim%d" % j for j in order[:k])
        rows.append(["dimablation", "k%d" % k, "constrained=" + cons, ov] + cov)

    for mm in args.multipliers.split(","):
        m = float(mm)
        v = make_selectivity(qs, M, d, m)
        rel = os.path.join("selectivity", "m%s" % mm.replace(".", "p"), "query.txt")
        write_queries(os.path.join(base_dir, rel), v)
        ov, cov = stats(v, M, d)
        rows.append(["selectivity", "m%s" % mm.replace(".", "p"), "multiplier=%.2f" % m, ov] + cov)

    meta = os.path.join(base_dir, "variants_meta.csv")
    with open(meta, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["study", "variant", "description", "avg_overall_selectivity",
                    "avg_cov_dim0", "avg_cov_dim1", "avg_cov_dim2", "avg_cov_dim3"])
        for r in rows:
            w.writerow(r[:3] + ["%.6g" % r[3]] + ["%.4f" % x for x in r[4:]])

    print("base=%s  M=%d  order(high-card first)=%s  queries=%d" % (args.tag, M, order, len(qs)))
    for r in rows:
        print("  %-12s %-6s %-22s overall_sel=%.5g cov=%s"
              % (r[0], r[1], r[2], r[3], " ".join("%.3f" % x for x in r[4:])))


if __name__ == "__main__":
    main()
