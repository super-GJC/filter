#!/usr/bin/env python3
"""Generate synthetic multi-D integer tuples + range queries for MD/SuRF experiments.

Writes program-ready forms only (no raw→encode pipeline):
  datasets/<name>/data.txt   : whitespace-separated ints, M cols
  workloads/<name>/query.txt : L0 H0 ... L{M-1} H{M-1}
"""

from __future__ import annotations

import argparse
import math
import random
import statistics
from pathlib import Path

ROOT = Path(__file__).resolve().parent

# Agreed first-pass config
DEFAULT_D = (16, 256, 64, 16)
DEFAULT_LOGICAL_SIZE = (2, 16, 16, 4)
DEFAULT_N = 10000
DEFAULT_NQ = 100
DEFAULT_SEED_DATA = 20261116
DEFAULT_SEED_QUERY = 20261117
# Mid-low selectivity (fraction of domain per dim)
DEFAULT_SEL = (
    (0.15, 0.40),
    (0.10, 0.35),
    (0.15, 0.40),
    (0.15, 0.40),
)


def safety_report(d: tuple[int, ...], logical_size: tuple[int, ...]) -> dict:
    m = len(d)
    bits = []
    pieces = []
    for i in range(m):
        iv = math.ceil(math.sqrt(logical_size[i]))
        ls0 = iv * iv
        cnt = 3.5 * ls0 - 1.5 * iv - 2
        bit = math.ceil(math.log(cnt) / math.log(2)) if cnt > 1 else 0
        bits.append(bit)
        dimbit = math.ceil(math.log(d[i]) / math.log(2))
        chunkbit = math.ceil(math.log(logical_size[i]) / math.log(2))
        if dimbit < chunkbit:
            raise ValueError(f"dim{i}: logical_size={logical_size[i]} > domain bits for d={d[i]}")
        pieces.append(dimbit - chunkbit)
    oneDrangebits = sum(bits)
    allmranges = 2**oneDrangebits
    bitmapbytes = math.ceil(allmranges / 8)
    chunknum = 2 ** sum(pieces)
    ok = oneDrangebits <= 20 and bitmapbytes <= 256 * 1024 and chunknum <= 16384
    return {
        "oneDrange_bit": bits,
        "oneDrangebits": oneDrangebits,
        "allmranges": allmranges,
        "bitmapbytes": bitmapbytes,
        "piecesbit": pieces,
        "chunknum": chunknum,
        "ok": ok,
    }


def gen_tuples(n: int, d: tuple[int, ...], seed: int) -> list[tuple[int, ...]]:
    rng = random.Random(seed)
    m = len(d)
    out: list[tuple[int, ...]] = []
    seen: set[tuple[int, ...]] = set()
    # Reject all-zero (empty_tuple sentinel) and try to avoid exact dups for density
    attempts = 0
    max_attempts = n * 50
    while len(out) < n and attempts < max_attempts:
        attempts += 1
        t = tuple(rng.randrange(d[j]) for j in range(m))
        if all(v == 0 for v in t):
            continue
        if t in seen:
            continue
        seen.add(t)
        out.append(t)
    if len(out) < n:
        raise RuntimeError(f"only generated {len(out)}/{n} unique non-zero tuples")
    return out


def _span(rng: random.Random, d_j: int, sel_lo: float, sel_hi: float) -> int:
    s = rng.uniform(sel_lo, sel_hi)
    return max(1, min(d_j, int(round(s * d_j))))


def gen_queries(
    nq: int,
    d: tuple[int, ...],
    sel: tuple[tuple[float, float], ...],
    seed: int,
) -> list[tuple[int, ...]]:
    rng = random.Random(seed)
    qs: list[tuple[int, ...]] = []
    for _ in range(nq):
        bounds: list[int] = []
        for j, d_j in enumerate(d):
            span = _span(rng, d_j, sel[j][0], sel[j][1])
            lo = rng.randint(0, d_j - span)
            hi = lo + span - 1
            bounds.extend((lo, hi))
        qs.append(tuple(bounds))
    return qs


def hit_count(q: tuple[int, ...], tuples: list[tuple[int, ...]]) -> int:
    m = len(q) // 2
    h = 0
    for t in tuples:
        if all(q[2 * j] <= t[j] <= q[2 * j + 1] for j in range(m)):
            h += 1
    return h


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=DEFAULT_N)
    ap.add_argument("--nq", type=int, default=DEFAULT_NQ)
    ap.add_argument("--d", default=",".join(map(str, DEFAULT_D)))
    ap.add_argument("--logical-size", default=",".join(map(str, DEFAULT_LOGICAL_SIZE)))
    ap.add_argument("--seed-data", type=int, default=DEFAULT_SEED_DATA)
    ap.add_argument("--seed-query", type=int, default=DEFAULT_SEED_QUERY)
    ap.add_argument("--dataset-name", default="synth_uniform_n10k")
    ap.add_argument("--workload-name", default="synth_uniform_q100")
    args = ap.parse_args()

    d = tuple(int(x) for x in args.d.replace(" ", ",").split(",") if x)
    logical_size = tuple(int(x) for x in args.logical_size.replace(" ", ",").split(",") if x)
    if len(d) != len(logical_size):
        raise SystemExit("d and logical_size length mismatch")

    safe = safety_report(d, logical_size)
    print("[safety]", safe)
    if not safe["ok"]:
        raise SystemExit("safety gate FAILED; refuse to write dataset")

    tuples = gen_tuples(args.n, d, args.seed_data)
    queries = gen_queries(args.nq, d, DEFAULT_SEL, args.seed_query)

    ds_dir = ROOT / "datasets" / args.dataset_name
    wl_dir = ROOT / "workloads" / args.workload_name
    ds_dir.mkdir(parents=True, exist_ok=True)
    wl_dir.mkdir(parents=True, exist_ok=True)

    data_path = ds_dir / "data.txt"
    with data_path.open("w", encoding="utf-8") as f:
        for t in tuples:
            f.write(" ".join(map(str, t)) + "\n")

    query_path = wl_dir / "query.txt"
    with query_path.open("w", encoding="utf-8") as f:
        for q in queries:
            f.write(" ".join(map(str, q)) + "\n")

    # per-dim occupied
    occupied = []
    for j in range(len(d)):
        occupied.append(len({t[j] for t in tuples}))

    hits = [hit_count(q, tuples) for q in queries]
    sels = []
    for j in range(len(d)):
        sels.append([(q[2 * j + 1] - q[2 * j] + 1) / d[j] for q in queries])

    ds_meta = f"""# dataset: {args.dataset_name}

| 字段 | 值 |
|------|----|
| 路径 | `experiments/synthetic_m4_n10k/datasets/{args.dataset_name}/data.txt` |
| 元组数 n | {args.n} |
| 维数 M | {len(d)} |
| 各维基数 d | `{{{", ".join(map(str, d))}}}` |
| logical_size（实验约定） | `{{{", ".join(map(str, logical_size))}}}` |
| 分布 | 各维独立均匀；禁止全 0 元组；去重 |
| seed | {args.seed_data} |
| 形态 | 程序输入整数元组（无需再编码） |

## 安全门槛

| 项 | 值 |
|----|----|
| oneDrange_bit | {safe["oneDrange_bit"]} |
| Σ oneDrange_bit | {safe["oneDrangebits"]} |
| allmranges | {safe["allmranges"]} (=2^{safe["oneDrangebits"]}) |
| bitmapbytes（单块位图上限） | {safe["bitmapbytes"]} |
| piecesbit | {safe["piecesbit"]} |
| chunknum | {safe["chunknum"]} |
| 门槛通过 | {safe["ok"]} |

## 数据概况

| 项 | 值 |
|----|----|
| 各维实际出现值数 | {occupied} |
| 全 0 元组 | 0 |
"""
    (ds_dir / "META.md").write_text(ds_meta, encoding="utf-8")

    wl_meta = f"""# workload: {args.workload_name}

| 字段 | 值 |
|------|----|
| 路径 | `experiments/synthetic_m4_n10k/workloads/{args.workload_name}/query.txt` |
| 查询条数 | {args.nq} |
| 配套数据 | `datasets/{args.dataset_name}/` |
| d | `{{{", ".join(map(str, d))}}}` |
| 模式 | 独立均匀矩形（满 4 维） |
| 选择度目标/维 | {list(DEFAULT_SEL)} |
| seed | {args.seed_query} |
| MD-FPR 筛选 | 否 |

## 统计

| 项 | 值 |
|----|----|
| 有命中查询 | {sum(1 for h in hits if h > 0)} / {len(hits)} |
| 命中均值 / 中位数 | {statistics.mean(hits):.2f} / {statistics.median(hits):.1f} |
| 各维平均选择度 | {[round(statistics.mean(s), 4) for s in sels]} |
"""
    (wl_dir / "META.md").write_text(wl_meta, encoding="utf-8")

    print(f"[ok] wrote {data_path} ({args.n} rows)")
    print(f"[ok] wrote {query_path} ({args.nq} queries)")
    print(f"[stats] hits nonempty={sum(1 for h in hits if h > 0)}/{len(hits)} "
          f"mean={statistics.mean(hits):.2f} median={statistics.median(hits)}")


if __name__ == "__main__":
    main()
