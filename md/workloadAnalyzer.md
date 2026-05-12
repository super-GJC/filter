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

---

## 工作负载感知构建与读取（已实现）

### `construct_Rangefilter` 六参数重载

- 声明：`void construct_Rangefilter(..., bool useWorkload, const std::unordered_set<int>& border_chunk_ids);`
- **`useWorkload == false`**：直接调用四参数版本，行为与改造前完全一致。
- **`useWorkload == true`**：
  - 在遍历非空块时，若 `i ∉ border_chunk_ids`：不向 `filter.txt` 追加数据，仅向 `offset.txt` 写入一行  
    `(i, page_startid[i], page_endid[i], 2, 0, 0, 0, 0)`，其中 **`filter_offset[i][0] == 2`** 表示未物化过滤器。
  - 若 `i ∈ border_chunk_ids`：与四参数版本相同，读页、`compute_Rangeset`、位图或布隆，并写入 `filter.txt` 与 offset。
  - 构建开始前 **`bloomFilterMap.clear()`**，避免沿用上一次的布隆 `num` 与本次 `filter.txt` 不一致。
  - **尾页刷盘**：`write_RFbitmap` 内不再按 `last_validchunk` 单独刷尾页；基线与工作负载路径均在 `construct_Rangefilter` 的循环结束后，若 **`beginbyte1 > 0`** 则对 `sdata1` 做一次整页 `WriteBlock`（位图/布隆统一），避免 `filter.txt` / `filter_workload.txt` 末尾截断，且工作负载下不会出现「`last_validchunk` 内刷一次 + 循环外再刷一次」的重复写盘。

### `read_Filters` 与类型 2

- 在 `page_startid[i] == -1` 之后：若 **`filter_offset[i][0] == 2`**，则 `continue`，不向 `filter.txt` 读取字节。
- **跨块 `sign` 优化**：原逻辑用 `sign` 避免相邻块在同一页内重复 `ReadBlock`；当中间夹有「文件中不占字节的 type 2 块」时，沿用 `sign` 会错位。处理方式为：**每个需要读取过滤器的块在读取前执行 `sign = 0`**，牺牲少量重复读页，保证与「稀疏写入」的 `filter.txt` 布局一致。

### `main.cpp` 中的验证流程（climate / `query.txt`）

1. `collectBorderChunkIdsFromQueries` 得到负载 border id 列表；转为 `unordered_set`。
2. **基线**：四参数 `construct_Rangefilter` → `process_Queries`，结果写入 `result1.txt`，过滤器 `filter.txt`。
3. **基线构建之后**从集合中 **剔除 `chunksize[i]==0` 的 id**（负载分析在 `transfer_Txt_ToBinaryfile` 之前执行，集合中可能含无元组块；若不剔除，重载在 `chunksize==0` 处 `continue` 不写 offset，会导致 `filter_offset` 仍为默认 0，查询时误判为位图）。
4. **工作负载**：六参数 `construct_Rangefilter(..., true, border_set)` 写入 `filter_workload.txt` / `offset_workload.txt` → `process_Queries` 写入 `result_workload.txt`。
5. **对比**：逐行比较 **overlap、borders、nempty、ratio、FPR**（列下标 0–3 与 6）；**`filtertime` / `processtime`** 因计时器浮动允许不同。同时打印 **`filter.txt` 与 `filter_workload.txt` 的字节数**，工作负载版应明显更小。

### 使用前提

- 运行时的查询集合应 **与用于计算 `border_chunk_ids` 的负载一致**（或为其子集），否则可能出现 **border 块未建过滤器** 而仍走布隆/位图分支的错误。扩展方案可为 `filter_offset[k][0]==2` 在 `process_Queries` 中增加保守分支（未在本轮最小改造中实现）。
