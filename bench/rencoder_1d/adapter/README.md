# REncoder 1D adapter（SuRF 形态）

仿 `bench/surf_1d/adapter/climate_surf_bench.cpp`：同一分块网格、1D-best / 1D-AND、块级 FPR。

- 独立目录；不改 MD / SuRF / Grafite / SNARF / Proteus / `bench/REncoder` 源码
- 默认负载：synthetic **m4 q100**
- **非 SIMD**：本机无 AVX-512；编译不加 `-DUSE_SIMD` / `-mavx512f`
- 默认构造：固定 `Insert`，`stored_levels=8`，`bpk=10`，`hash_num=3`（每维构建前重置 `first_insert`/`last_p`）
- `RangeQuery(lo,hi)` 按上游为闭区间 `[lo,hi]`

```bash
bash build.sh
./m4_rencoder_bench --mode both --out ../output/m4_q100
```
