# Proteus 1D adapter（SuRF 形态）

仿 `bench/surf_1d/adapter/climate_surf_bench.cpp`：同一分块网格、1D-best / 1D-AND、块级 FPR。

- 独立目录；不改 MD / SuRF / Grafite / SNARF / `bench/Proteus` 源码
- 默认负载：synthetic **m4 q100**
- 默认构造：`trie_depth=64`，`sparse_dense_cutoff=0`，`prefix_length=0`，`bpk=10`（整型全深度 trie，无 PBF）
- **区间语义**：Proteus `Query(L,R)` 为 `[L,R)`；适配层把闭区间 `[lo,hi]` 转成 `Query(lo, hi+1)`

```bash
bash build.sh
./m4_proteus_bench --mode both --out ../output/m4_q100
# 或显式：
./m4_proteus_bench \
  --data ../../../experiments/synthetic_m4_n10k/datasets/synth_uniform_n10k/data.txt \
  --query ../../../experiments/synthetic_m4_n10k/workloads/synth_uniform_q100/query.txt \
  --out ../output/m4_q100 \
  --mode both --bits-per-key 10 --trie-depth 64
```
