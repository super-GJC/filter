# Border chunk 各维 ρ 统计（层 B）

本文档说明在已有 $Q_c$（border chunk → 查询下标列表）之上，如何计算每个 border chunk 在各维度上的 **平均重叠比例** $\bar\rho(c,d)$ 与 **最小重叠比例** $\rho_{\min}(c,d)$，供后续 `rfilter` 按负载选择过滤器编码维度时使用。

相关文档：[workloadAnalyzer.md](./workloadAnalyzer.md)（层 A：$Q_c$）、[filter_contribution.md](./filter_contribution.md)（启发式动机与 $d_1,d_2$ 例子）。

---

## 1. 理论定义

### 1.1 块与查询在值域上的区间

对 border chunk $c$，先解码各维块下标 $\mathrm{coor}(c,j)$（与 `process_Queries` 一致），块在维 $d$ 上的 **值域**为：

$$
L_c(d) = \mathrm{coor}(c,d)\cdot \ell_d,\quad
H_c(d) = (\mathrm{coor}(c,d)+1)\cdot \ell_d - 1
$$

其中 $\ell_d = \texttt{logical\_size}[d]$ 为块长（点数），$\ell_c(d) = H_c(d)-L_c(d)+1 = \ell_d$。

对 $q \in Q_c$，查询在维 $d$ 上的全局区间为 $[q_{\min,d}, q_{\max,d}]$。**块内裁剪交集**为：

$$
I(q,c,d) = \bigl[\max(q_{\min,d}, L_c(d)),\; \min(q_{\max,d}, H_c(d))\bigr]
$$

若 $\max > \min$，则交集为空，定义 $\rho(q,c,d)=0$（正常 border 命中时不应出现）。

### 1.2 单查询重叠比例

$$
\rho(q,c,d) = \frac{|I(q,c,d)|}{\ell_c(d)}
$$

其中 $|I| = \max(0,\; \min - \max + 1)$ 为离散整数区间长度。$\rho \in [0,1]$：

| $\rho$ | 含义 |
|--------|------|
| $1$ | 查询在块内铺满该维（对过滤无判别力，可优先删维） |
| 接近 $1$ | 多数查询很宽 |
| 小 | 存在窄查询，该维对降低 FPR 更重要 |

### 1.3 聚合：$\bar\rho$ 与 $\rho_{\min}$

对固定 $(c,d)$，仅在 $Q_c$ 上聚合：

$$
\bar\rho(c,d) = \frac{1}{|Q_c|}\sum_{q\in Q_c} \rho(q,c,d)
$$

$$
\rho_{\min}(c,d) = \min_{q\in Q_c} \rho(q,c,d)
$$

**启发式用途（排序，非充要删维条件）：**

- $\bar\rho$ **大** → 该维对负载 **选择性弱** → **优先从编码维度中剔除**；
- $\rho_{\min}$ **小** → 至少有一条查询在该维上很窄 → **删维风险大**（可能显著抬高 FPR），宜作 **保留维** 的保护信号。

**安全删维（层 B 不自动执行）：** 仅当 $\rho_{\min}(c,d)=1$ 时，对应「每条 $q\in Q_c$ 均满覆盖」的充分条件（见 filter_contribution 第 2 节）。

### 1.4 正确性要点（与 $Q_c$ 一致）

$\rho$ 只对 **会使用块 $c$ 过滤器的查询** $Q_c$ 统计，不对全体查询平均。故必须先有层 A 的 $Q_c$，再算 $\bar\rho,\rho_{\min}$；块坐标 $L_c,H_c$ 与 `process_Queries` 中对 `q[2*d]` 的裁剪使用同一套 $\mathrm{coor}$ 与 $\texttt{logical\_size}$。

---

## 2. 实现思路

### 2.1 数据结构（`workloadAnalyzer.h`）

```cpp
struct BorderChunkRhoProfile {
    std::vector<double> rho_bar;  // 长度 m，各维平均 ρ
    std::vector<double> rho_min;  // 长度 m，各维最小 ρ
};

using BorderChunkRhoMap = std::unordered_map<int, BorderChunkRhoProfile>;
// chunk_id -> 该块 profile
```

`BorderChunkRhoMap` 的 key 集合与 `BorderChunkQueryMap` 一致（仅含 $Q_c$ 非空的 border chunk）。

### 2.2 接口（`workloadAnalyzer.cpp` 新增，未改动已有函数）

| 函数 | 作用 |
|------|------|
| `computeBorderChunkRhoStats(rf, qc_map, queries)` | 在已有 $Q_c$ 上计算全部 $\bar\rho,\rho_{\min}$ |
| `collectBorderChunkRhoStats(rf, queries)` | `collectBorderChunkQueryMap` + 上一函数 |
| `collectBorderChunkRhoStatsFromQueries(rf, path)` | 从 `query.txt` 加载后一站式计算 |

内部步骤（对每个 `chunk_id` ∈ `qc_map`）：

1. `chunk_id_to_coor` 解码 $\mathrm{coor}(c,\cdot)$；
2. 对每个维 $j$：算 $L_c,H_c,\ell_c$；
3. 对每个 `qi ∈ Q_c`：用 `rho_query_chunk_dim` 算 $\rho(q,c,j)$，累加求平均、取最小；
4. 写入 `BorderChunkRhoProfile`。

### 2.3 与未来 `rfilter` 的衔接（尚未实现）

构建 border chunk $c$ 的过滤器时，可读取 `BorderChunkRhoMap[c]`：

- 按 `rho_bar` **降序**丢弃维度，或按 `rho_bar` **升序**保留 top-$k$；
- 若 `rho_min[d] < 1`（或低于阈值），强制保留维 $d$；
- 在 `offset` 或侧车元数据中持久化选中的维度掩码 $S_c$。

---

## 3. 测试与正确性说明

### 3.1 测试程序

```bash
# 仓库根目录
g++ -std=c++17 -O0 -g -I. BlockManager.cpp Timer.cpp bfilter.cpp rfilter.cpp \
  query.cpp workloadAnalyzer.cpp test/workload_rho_test.cpp \
  -o test/workload_rho_test
./test/workload_rho_test
```

### 3.2 验证策略

1. **全量暴力对照**：对 `qc_map` 中每个 chunk $c$，用测试内独立实现的 `brute_rho_profile`（逐 $(q,d)$ 手算 $\rho$ 再聚合）与 `computeBorderChunkRhoStats` 结果逐维比较，容差 $10^{-12}$。
2. **路径一致**：`collectBorderChunkRhoStats` 与 `collectBorderChunkRhoStatsFromQueries` 结果相同。
3. **闭式手算例**（对应 filter_contribution §4）：两条查询在 dim1 上均为 $[0,14]$、dim2 上为 $[0,15]$ 与 $[0,1]$，块在 dim1/dim2 上值域为 $[0,15]$ 时：
   - $\bar\rho(c,d_1)=15/16$，$\bar\rho(c,d_2)=9/16$；
   - $\rho_{\min}(c,d_1)=15/16$，$\rho_{\min}(c,d_2)=1/8$；
   - 故 $\bar\rho(c,d_1) > \bar\rho(c,d_2)$（$d_1$ 更优先舍弃）。

### 3.3 实测结果摘要

在 `logical_size = {2,16,16,4}`、climate `query.txt`（8 条查询）上：

| 项目 | 结果 |
|------|------|
| border chunk 数（有 $Q_c$） | 352 |
| 快路径 vs 暴力 | **352 块 × 4 维全部一致** |
| 手算例 chunk | `c2048`，$\bar\rho[d_1]=0.9375$，$\bar\rho[d_2]=0.5625$ |
| 排序结论 | $\bar\rho(d_1) > \bar\rho(d_2)$，与理论一致 |

可视化（climate **全部 352** 个 border chunk）：

- `test/output/border_chunk_rho_report.html`：`rho_bar` / `rho_min` 分列，含 $|Q_c|$，按 $|Q_c|$ 降序
- `test/output/rho_report.html`：每维一格「bar / min」，高/低 bar 着色（hi/lo），按 chunk id 升序

### 3.4 与层 A 测试的关系

`workload_analyzer_test` 仍只验证 $Q_c$；层 B 由 `workload_rho_test` 单独覆盖。二者共用 `workloadAnalyzer.cpp`，编译时需链接同一实现文件。

---

## 4. 使用示例（伪代码）

```cpp
Rfilter rf;
vector<vector<int>> queries;
loadQuery("data/test/climate/query.txt", queries);

BorderChunkQueryMap qc = collectBorderChunkQueryMap(rf, queries);
BorderChunkRhoMap rho = computeBorderChunkRhoStats(rf, qc, queries);

int c = 2048;
double bar_d1 = rho.at(c).rho_bar[1];
double min_d2 = rho.at(c).rho_min[2];
// 后续：按 bar 排序选 S_c，结合 min 保护窄查询维
```

或一行：

```cpp
BorderChunkRhoMap rho = collectBorderChunkRhoStatsFromQueries(rf, querypath);
```

---

## 5. 小结

| 量 | 定义域 | 用途 |
|----|--------|------|
| $\rho(q,c,d)$ | 单查询 | 块内查询覆盖占块长比例 |
| $\bar\rho(c,d)$ | $Q_c$ 上平均 | **排序**：大 → 优先舍弃 |
| $\rho_{\min}(c,d)$ | $Q_c$ 上最小 | **保护**：小 → 慎删 |

层 B 在层 A 的 $Q_c$ 之上完成，不读取块内元组、不修改 `rfilter`；输出 `BorderChunkRhoMap` 供后续按块选维构建过滤器。
