# SNARF 1D adapter（SuRF 形态）

仿 `bench/surf_1d/adapter/climate_surf_bench.cpp`：同一分块网格、1D-best / 1D-AND、块级 FPR。

- 独立目录；不改 MD / SuRF / Grafite / `bench/SNARF` 源码
- 默认负载：synthetic **m4 q100**
- 编译带 `-DNDEBUG`：上游 `snarf_model_builder` 断言 `N>10000`，块内键数远小于此

```bash
bash build.sh
./m4_snarf_bench --mode both
# 或显式：
./m4_snarf_bench \
  --data ../../../experiments/synthetic_m4_n10k/datasets/synth_uniform_n10k/data.txt \
  --query ../../../experiments/synthetic_m4_n10k/workloads/synth_uniform_q100/query.txt \
  --out ../output/m4_q100 \
  --mode both --bits-per-key 10 --batch-size 100
```
