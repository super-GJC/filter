# Grafite 1D-best 探针（m5）

最小接入：仿 `bench/surf_1d/adapter/climate_surf_bench.cpp`，**只跑 1D-best**。

- 不改 MD / SuRF 源码
- 无统一 harness；单文件 + `build.sh`
- 构建失败的块：查询时**不过滤（保留）**，并统计 `build_fail`

```bash
bash build.sh
./m5_grafite_best --bpk 2
# 结果默认：../output/{result_best.txt,summary.csv,build_stats.txt}
```
