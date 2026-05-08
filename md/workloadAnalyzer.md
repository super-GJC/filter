# 工作负载分析器（Workload Analyzer）技术说明

## 背景与目标

在 range filter 流水线中，`Rfilter::process_Queries()` 将「与范围查询相交的块」划分为：

- **完全落在查询 chunk 网格内部的块**（`isborderchunk == 0`）：不经过块上过滤器，直接按需读数据；
- **边界块 border chunk**（`isborderchunk == 1`）：查询在分块边界上「擦边」，需用该块的多维过滤器做初筛。

工作负载分析器在**不读二进制数据、不加载过滤器文件**的前提下，仅根据 `query.txt` 与当前分块配置，计算**所有查询共同作用下的 border chunk id 集合**（去重、升序）。典型用途包括：

- 工作负载感知的**选择性构建过滤器**（仅对 border chunk 物化过滤器，需与查询路径约定一致）；
- 离线估算需要过滤器参与的块数量。

## 与 `process_Queries()` 的一致性

实现严格复刻 `rfilter.cpp` 中下列逻辑（见 `process_Queries` 内 `for(k = lowchunk; k <= highchunk; k++)` 循环）：

1. 对每条查询，`p1[j] = q[2*j] / logical_size[j]`，`p2[j] = q[2*j+1] / logical_size[j]`；
2. `lowchunk` / `highchunk` 由各维 `piecesbit[j]` 与 `p1`、`p2` 按与源码相同的移位加法合成；
3. 对每个候选 `k`，自高维向低维解码 `coor[j]`（与源码相同顺序与位宽）；
4. 若存在 `coor[j] < p1[j] || coor[j] > p2[j]`，则该 `k` 对该查询不是相交块；
5. 若相交，且存在某维 `coor[j] == p1[j] || coor[j] == p2[j]`，则该块为该查询的 **border chunk**，将该 `k` 记入全局集合。

**语义说明（重要）**：此处的「边界」是 **chunk 下标网格相对查询的 `[p1,p2]` 包络是否为贴边**，与纯几何上「块与查询矩形是否部分重叠」并不总等价。例如查询范围完全落在一个 chunk 的值域内，但该 chunk 的下标仍可能等于 `p1` 或 `p2`，都会被标为 border。若要与 `process_Queries` 行为一致，必须以源码为准。

## 接口

| 符号 | 说明 |
|------|------|
| `collectBorderChunkIds(rf, queries)` | 输入已解析的查询行（每行 `2*m` 个整数，与 `loadQuery` 一致），返回 `vector<int>`（升序、无重复）。 |
| `collectBorderChunkIdsFromQueries(rf, querypath)` | 从文件路径加载查询后调用上一函数。 |

依赖：

- `Rfilter rf`：提供 `m`、`piecesbit`、`powpieces`、`chunknum` 等与分块一致的元数据（通常与主程序共用的全局 `d`、`logical_size`、`logical_size0` 初始化后构造）。
- 全局 `logical_size`：与构造 `rf` 时相同，用于计算 `p1`/`p2`。

## 文件与构建

| 文件 | 作用 |
|------|------|
| `workloadAnalyzer.h` / `workloadAnalyzer.cpp` | 分析器实现。 |
| `query.cpp` | `loadQuery`、`compare_Twotuples`、`strmncpy`（由原 `main.cpp` 迁出，供 `main`、`rfilter`、`bfilter` 及测试链接）。 |
| `test/workload_analyzer_test.cpp` | 自测：与「按 `k=0..chunknum-1` 暴力枚举」的结果比对；并对 `data/test/climate/query.txt` 冒烟测试。 |

自测构建与运行示例（在**仓库根目录**执行；`-I.` 保证 `test/` 下源文件能找到根目录头文件）：

```bash
g++ -std=c++17 -g -I. BlockManager.cpp Timer.cpp bfilter.cpp rfilter.cpp \
  query.cpp workloadAnalyzer.cpp test/workload_analyzer_test.cpp \
  -o test/workload_analyzer_test
./test/workload_analyzer_test
```

也可在 `test/` 目录下直接运行 `./workload_analyzer_test`（程序会自动尝试 `./data/...` 与 `../data/...` 以定位 `query.txt`）。border chunk 数量随 `query.txt` 与分块参数变化；请以运行时打印为准。

## 实现思路合理性简评

「逐条查询计算 border chunk id，再求并集去重」与 `process_Queries` 按查询独立处理再统计 border 的方式一致，等价于对全负载做并集，**合理且完整**。复杂度约为 `O(查询条数 × 每条查询覆盖的 chunk 数)`；暴力验证为 `O(chunknum × 查询条数)`，仅用于测试。

## 后续可扩展方向（非本模块范围）

- 输出**完全覆盖块**集合（`isborderchunk == 0` 且相交），用于与 border 集合互补；
- 对每个 border chunk 记录「命中它的查询子集」，用于按负载选择编码维度；
- 与 `construct_Rangefilter` 衔接：仅对返回的 id 列表构建/writing `filter.txt`。
