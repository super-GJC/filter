#!/usr/bin/env python3
"""Generate climate multi-dimensional range queries for filter experiments.

Output format matches loadQuery() / existing query.txt:
  one query per line, 2*M integers: L0 H0 L1 H1 ... L{M-1} H{M-1}
  inclusive bounds; each Li,Hi in [0, d[i]-1], Li <= Hi.

Modes:
  uniform     — random axis-aligned boxes (independent of data points)
  correlated  — expand intervals around a sampled data tuple
  mixed       — mix of the two (default main workload)

Default domain / selectivity follow main.cpp climate config and climate_q8.
"""

from __future__ import annotations

import argparse
import statistics
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Sequence

# Matches main.cpp / datasets/climate/META.md
DEFAULT_D = (4, 1121, 68, 7)

# Per-dim selectivity = (hi-lo+1)/d[i], ranges calibrated from climate_q8
# (empirical q8 spans: dim0≈0.25–0.75, dim1≈0.05–0.58, dim2≈0.16–0.46, dim3≈0.14–1.0)
DEFAULT_SEL = (
    (0.25, 0.75),
    (0.05, 0.55),
    (0.10, 0.50),
    (0.15, 1.00),
)

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_DATA = REPO_ROOT / "data" / "test" / "climate" / "data.txt"
WORKLOADS = REPO_ROOT / "experiments" / "workloads"


@dataclass(frozen=True)
class Query:
    bounds: tuple[int, ...]  # length 2*M

    def line(self) -> str:
        return " ".join(str(x) for x in self.bounds)


def load_tuples(path: Path, m: int) -> list[tuple[int, ...]]:
    """Read data.txt: first m ints are dimensions; trailing columns ignored."""
    rows: list[tuple[int, ...]] = []
    with path.open(encoding="utf-8") as f:
        for lineno, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            parts = line.split()
            if len(parts) < m:
                raise ValueError(f"{path}:{lineno}: need >= {m} columns, got {len(parts)}")
            try:
                row = tuple(int(parts[i]) for i in range(m))
            except ValueError as e:
                raise ValueError(f"{path}:{lineno}: non-integer field") from e
            rows.append(row)
    if not rows:
        raise ValueError(f"{path}: empty data file")
    return rows


def validate_domain(tuples: Sequence[tuple[int, ...]], d: Sequence[int]) -> None:
    m = len(d)
    for t in tuples:
        if len(t) != m:
            raise ValueError(f"tuple arity {len(t)} != M={m}")
        for j, v in enumerate(t):
            if v < 0 or v >= d[j]:
                raise ValueError(
                    f"tuple value {v} out of domain dim{j}=[0,{d[j]-1}] "
                    f"(check d vs data.txt)"
                )


def _sample_span(rng, d_j: int, sel_lo: float, sel_hi: float) -> int:
    if not (0.0 < sel_lo <= sel_hi <= 1.0):
        raise ValueError(f"invalid selectivity [{sel_lo},{sel_hi}]")
    s = rng.uniform(sel_lo, sel_hi)
    span = int(round(s * d_j))
    return max(1, min(d_j, span))


def _uniform_interval(rng, d_j: int, sel_lo: float, sel_hi: float) -> tuple[int, int]:
    span = _sample_span(rng, d_j, sel_lo, sel_hi)
    lo = rng.randint(0, d_j - span)
    return lo, lo + span - 1


def _correlated_interval(
    rng, d_j: int, anchor: int, sel_lo: float, sel_hi: float
) -> tuple[int, int]:
    """Inclusive interval covering anchor, length ~ selectivity * d_j."""
    if not (0 <= anchor < d_j):
        raise ValueError(f"anchor {anchor} out of [0,{d_j - 1}]")
    span = _sample_span(rng, d_j, sel_lo, sel_hi)
    # place [lo, lo+span-1] so that lo <= anchor <= hi
    max_lo = min(anchor, d_j - span)
    lo = rng.randint(0, max_lo) if max_lo > 0 else 0
    # ensure coverage if randint range was empty edge
    hi = lo + span - 1
    if anchor < lo:
        lo = anchor
        hi = lo + span - 1
    if hi >= d_j:
        hi = d_j - 1
        lo = hi - span + 1
    if lo < 0:
        lo = 0
        hi = span - 1
    if not (lo <= anchor <= hi):
        # fallback: clamp around anchor
        left = min(anchor, span - 1)
        lo = anchor - left
        hi = lo + span - 1
        if hi >= d_j:
            hi = d_j - 1
            lo = max(0, hi - span + 1)
    return lo, hi


def gen_uniform(
    rng, d: Sequence[int], sel: Sequence[tuple[float, float]]
) -> Query:
    bounds: list[int] = []
    for j, d_j in enumerate(d):
        lo, hi = _uniform_interval(rng, d_j, sel[j][0], sel[j][1])
        bounds.extend((lo, hi))
    return Query(tuple(bounds))


def gen_correlated(
    rng,
    d: Sequence[int],
    sel: Sequence[tuple[float, float]],
    tuples: Sequence[tuple[int, ...]],
) -> Query:
    anchor = tuples[rng.randrange(len(tuples))]
    bounds: list[int] = []
    for j, d_j in enumerate(d):
        lo, hi = _correlated_interval(rng, d_j, anchor[j], sel[j][0], sel[j][1])
        bounds.extend((lo, hi))
    return Query(tuple(bounds))


def validate_query(q: Query, d: Sequence[int]) -> None:
    m = len(d)
    if len(q.bounds) != 2 * m:
        raise ValueError(f"query width {len(q.bounds)} != 2*M={2 * m}")
    for j in range(m):
        lo, hi = q.bounds[2 * j], q.bounds[2 * j + 1]
        if lo > hi:
            raise ValueError(f"dim{j}: lo={lo} > hi={hi}")
        if lo < 0 or hi >= d[j]:
            raise ValueError(f"dim{j}: [{lo},{hi}] outside [0,{d[j] - 1}]")


def generate_workload(
    mode: str,
    n: int,
    seed: int,
    d: Sequence[int],
    sel: Sequence[tuple[float, float]],
    tuples: Sequence[tuple[int, ...]],
    mixed_uniform_frac: float,
) -> list[Query]:
    import random

    if n <= 0:
        raise ValueError("n must be positive")
    if len(sel) != len(d):
        raise ValueError("sel length must equal M")
    if not (0.0 <= mixed_uniform_frac <= 1.0):
        raise ValueError("mixed_uniform_frac must be in [0,1]")

    rng = random.Random(seed)
    out: list[Query] = []

    if mode == "uniform":
        for _ in range(n):
            out.append(gen_uniform(rng, d, sel))
    elif mode == "correlated":
        for _ in range(n):
            out.append(gen_correlated(rng, d, sel, tuples))
    elif mode == "mixed":
        n_u = int(round(n * mixed_uniform_frac))
        n_c = n - n_u
        for _ in range(n_u):
            out.append(gen_uniform(rng, d, sel))
        for _ in range(n_c):
            out.append(gen_correlated(rng, d, sel, tuples))
        rng.shuffle(out)
    else:
        raise ValueError(f"unknown mode: {mode}")

    for q in out:
        validate_query(q, d)
    return out


def hit_count(q: Query, tuples: Sequence[tuple[int, ...]]) -> int:
    m = len(q.bounds) // 2
    hits = 0
    for t in tuples:
        ok = True
        for j in range(m):
            if t[j] < q.bounds[2 * j] or t[j] > q.bounds[2 * j + 1]:
                ok = False
                break
        if ok:
            hits += 1
    return hits


def summarize(
    queries: Sequence[Query], d: Sequence[int], tuples: Sequence[tuple[int, ...]]
) -> dict:
    m = len(d)
    sels: list[list[float]] = [[] for _ in range(m)]
    hits = []
    nonempty = 0
    for q in queries:
        for j in range(m):
            lo, hi = q.bounds[2 * j], q.bounds[2 * j + 1]
            sels[j].append((hi - lo + 1) / d[j])
        h = hit_count(q, tuples)
        hits.append(h)
        if h > 0:
            nonempty += 1
    return {
        "n": len(queries),
        "nonempty_queries": nonempty,
        "empty_queries": len(queries) - nonempty,
        "hit_median": statistics.median(hits) if hits else 0,
        "hit_mean": statistics.mean(hits) if hits else 0,
        "sel_mean": [statistics.mean(s) for s in sels],
        "sel_min": [min(s) for s in sels],
        "sel_max": [max(s) for s in sels],
    }


def write_query_txt(path: Path, queries: Sequence[Query]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    text = "\n".join(q.line() for q in queries) + "\n"
    path.write_text(text, encoding="utf-8")


def write_meta(
    path: Path,
    *,
    name: str,
    purpose: str,
    mode: str,
    n: int,
    seed: int,
    d: Sequence[int],
    sel: Sequence[tuple[float, float]],
    mixed_uniform_frac: float | None,
    data_path: Path,
    stats: dict,
) -> None:
    today = date.today().isoformat()
    mix_row = (
        f"| 混合比例 | uniform {mixed_uniform_frac:.0%} + correlated "
        f"{1.0 - mixed_uniform_frac:.0%} |\n"
        if mixed_uniform_frac is not None
        else "| 混合比例 | N/A（纯模式） |\n"
    )
    sel_str = ", ".join(f"dim{j}=[{a:.2f},{b:.2f}]" for j, (a, b) in enumerate(sel))
    mean_sel = ", ".join(f"{x:.3f}" for x in stats["sel_mean"])
    body = f"""# workload: {name}

| 字段 | 值 |
|------|----|
| 数据集 | climate |
| 查询条数 | {n} |
| 来源 | `experiments/workloads/gen/gen_climate_queries.py` |
| 用途 | {purpose} |
| 生成模式 | `{mode}` |
{mix_row}| 各维选择性目标区间 | {sel_str} |
| 实际平均选择性 | {mean_sel} |
| 随机种子 | {seed} |
| 域基数 d | {{{", ".join(str(x) for x in d)}}} |
| 锚点数据 | `{data_path.as_posix()}`（只读，仅 correlated 用） |
| 生成日期 | {today} |
| 非空命中查询数 | {stats["nonempty_queries"]} / {n} |
| 命中元组数 median / mean | {stats["hit_median"]} / {stats["hit_mean"]:.2f} |

`query.txt` 每行格式：`L0 H0 L1 H1 L2 H2 L3 H3`（闭区间，与 `loadQuery` 一致）。

重新生成（在仓库根目录）：

```bash
python3 experiments/workloads/gen/gen_climate_queries.py --write-defaults
```
"""
    path.write_text(body, encoding="utf-8")


def parse_sel(s: str) -> tuple[tuple[float, float], ...]:
    """Parse 'a-b,c-d,...' into ((a,b),(c,d),...)."""
    parts = []
    for chunk in s.split(","):
        chunk = chunk.strip()
        lo_s, hi_s = chunk.split("-")
        parts.append((float(lo_s), float(hi_s)))
    return tuple(parts)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--write-defaults",
        action="store_true",
        help="Write climate_q200_mixed + climate_q200_correlated with fixed seeds",
    )
    p.add_argument("--mode", choices=("uniform", "correlated", "mixed"), default="mixed")
    p.add_argument("--n", type=int, default=200)
    p.add_argument("--seed", type=int, default=20260916)
    p.add_argument("--data", type=Path, default=DEFAULT_DATA)
    p.add_argument("--out", type=Path, help="Output query.txt path (single workload)")
    p.add_argument("--meta", type=Path, help="Optional META.md path")
    p.add_argument(
        "--mixed-uniform-frac",
        type=float,
        default=0.5,
        help="For mixed: fraction of uniform queries (default 0.5)",
    )
    p.add_argument(
        "--sel",
        type=str,
        default=None,
        help="Override selectivity, e.g. 0.25-0.75,0.05-0.55,0.10-0.50,0.15-1.00",
    )
    args = p.parse_args()

    d = DEFAULT_D
    sel = parse_sel(args.sel) if args.sel else DEFAULT_SEL
    if len(sel) != len(d):
        raise SystemExit(f"--sel must have {len(d)} ranges, got {len(sel)}")

    tuples = load_tuples(args.data, len(d))
    validate_domain(tuples, d)

    if args.write_defaults:
        jobs = [
            {
                "name": "climate_q200_mixed",
                "mode": "mixed",
                "seed": 20260916,
                "purpose": "**主实验负载**：降低单条 FPR 波动；均匀 + 相关各半",
                "mixed_frac": 0.5,
            },
            {
                "name": "climate_q200_correlated",
                "mode": "correlated",
                "seed": 20260917,
                "purpose": "可选压力测：查询围绕真实元组扩展，命中率更高",
                "mixed_frac": None,
            },
        ]
        for job in jobs:
            queries = generate_workload(
                mode=job["mode"],
                n=200,
                seed=job["seed"],
                d=d,
                sel=sel,
                tuples=tuples,
                mixed_uniform_frac=job["mixed_frac"] if job["mixed_frac"] is not None else 0.5,
            )
            stats = summarize(queries, d, tuples)
            out_dir = WORKLOADS / job["name"]
            write_query_txt(out_dir / "query.txt", queries)
            write_meta(
                out_dir / "META.md",
                name=job["name"],
                purpose=job["purpose"],
                mode=job["mode"],
                n=200,
                seed=job["seed"],
                d=d,
                sel=sel,
                mixed_uniform_frac=job["mixed_frac"],
                data_path=args.data.relative_to(REPO_ROOT)
                if args.data.is_relative_to(REPO_ROOT)
                else args.data,
                stats=stats,
            )
            print(
                f"[ok] {job['name']}: n={stats['n']} "
                f"nonempty={stats['nonempty_queries']} "
                f"hit_mean={stats['hit_mean']:.2f} -> {out_dir / 'query.txt'}"
            )
        return

    if args.out is None:
        raise SystemExit("provide --out PATH or use --write-defaults")

    queries = generate_workload(
        mode=args.mode,
        n=args.n,
        seed=args.seed,
        d=d,
        sel=sel,
        tuples=tuples,
        mixed_uniform_frac=args.mixed_uniform_frac,
    )
    stats = summarize(queries, d, tuples)
    write_query_txt(args.out, queries)
    if args.meta:
        write_meta(
            args.meta,
            name=args.out.parent.name,
            purpose="custom",
            mode=args.mode,
            n=args.n,
            seed=args.seed,
            d=d,
            sel=sel,
            mixed_uniform_frac=args.mixed_uniform_frac if args.mode == "mixed" else None,
            data_path=args.data,
            stats=stats,
        )
    print(
        f"[ok] wrote {args.out} n={stats['n']} "
        f"nonempty={stats['nonempty_queries']} hit_mean={stats['hit_mean']:.2f}"
    )


if __name__ == "__main__":
    main()
