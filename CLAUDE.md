# filter 项目说明 (CLAUDE.md)

本文档帮助快速理解项目用途、结构和约定，供 AI 助手或新开发者使用。

## 项目在做什么

本项目是一个 **多维范围过滤器 (Range Filter)** 的 C++ 实现，用于在 **多维数据** 上加速 **范围查询**，通过“先过滤再读数据”减少 I/O。

### 核心流程

1. **数据分区与二进制存储**
   - 读入多维元组（如 `data.txt`，制表符分隔），按各维的 `logical_size` 划分成 chunk。
   - 将每个 chunk 的元组按页（默认 4096 字节）写入二进制文件（如 `binary1.txt`），由 `BlockManager` 管理读写。

2. **为每个 chunk 构建过滤器**
   - 对每个非空 chunk，根据其内元组计算 **一维范围** 与 **多维范围集合**（range set）。
   - 根据范围数量选择过滤器类型：
     - **位图 (bitmap)**：当 `allmranges/rangeids.size() >= bitsperkey` 时使用，无假阳性。
     - **布隆过滤器 (Bloom filter)**：否则使用，节省空间但可能有假阳性。
   - 过滤器写入 `filter.txt`，chunk 的页范围与过滤器在文件中的偏移写入 `offset.txt`。

3. **范围查询处理**
   - 从 `query.txt` 读入多维范围查询。
   - 找出与查询相交的 chunk（含完全被覆盖与边界 chunk）。
   - 对边界 chunk 用对应过滤器判断是否可能包含命中元组；仅对通过过滤的 chunk 读页并做精确扫描。
   - 统计并输出：重叠 chunk 数、边界 chunk 数、实际非空 chunk 数、FPR、过滤时间、扫描时间等到 `result1.txt`。

### 主要概念

- **维度**：`d` 为各维基数，`dbit` 为对应位数，`M` 为维度数（与 `Rfilter::m` 一致）。
- **Chunk**：按 `logical_size` 划分的数据块，总数为 `chunknum`，每个 chunk 有 `page_startid` / `page_endid`。
- **一维/多维范围**：基于论文中的划分（partition value、interval、绿色圆/红色菱形等），用于将点/区间映射到范围 ID，再组合成多维范围 ID 用于过滤。
- **empty_tuple**：全 0 元组，用于填充页内空位；与“各维取最小值”的元组可能冲突（见代码注释 FIXME）。

## 目录与文件结构

```
filter/
├── main.cpp           # 入口：初始化 d/dbit/logical_size，调用 Rfilter 构建与查询
├── rfilter.cpp/h      # 范围过滤器：分块、二进制转换、范围集计算、位图/布隆、查询流程
├── bfilter.cpp/h      # 布隆过滤器：构造、MurmurHash、写入/查询
├── common.h           # 全局常量、宏、extern 变量（M, d, dbit, logical_size, filter_offset 等）
├── BlockManager.cpp/h # 按块（页）读写文件，O_DIRECT 等
├── Timer.cpp/h        # 计时
├── build.sh           # 构建脚本（当前仅 g++）
├── data/test/climate/ # 测试数据目录
│   ├── data.txt       # 输入元组
│   ├── query.txt     # 范围查询
│   ├── binary1.txt    # 输出二进制数据
│   ├── filter.txt    # 输出过滤器
│   ├── offset.txt    # 输出 chunk 页范围与过滤器偏移
│   └── result1.txt   # 输出查询统计
└── CLAUDE.md          # 本文件
```

## 关键全局变量（common.h / main.cpp）

- `d`：各维基数；`dbit`：各维位数；`dbit_sum`：总位数；`page_capacity`：每页元组数。
- `logical_size`：各维 chunk 逻辑大小；`logical_size0`：与之相关的平方上取整（用于一维范围计算）。
- `empty_tuple`：全 0 元组。
- `filter_offset`：每 chunk 的 [filter_type, start_page, start_byte, end_page, end_byte]。
- `bitsperkey`：布隆过滤器每 key 位数；`allmranges` 等与范围空间大小相关。

## 配置与数据路径

- 数据目录在 `main.cpp` 中写死为 `./data/test/climate/`。
- 维度与逻辑大小在 `main.cpp` 中写死：`d = {4,1121,68,7}`，`logical_size = {2,16,16,4}`。
- 页大小：`PAGESIZE = 4096`，`BYTE = 8`（common.h）。

## 构建与运行

- 使用 `build.sh` 或直接 `g++` 编译（需链接所有 .cpp，包含 BlockManager、Timer、main、rfilter、bfilter）。
- 运行前需准备 `data/test/climate/data.txt` 和 `query.txt`；运行后生成 binary1.txt、filter.txt、offset.txt、result1.txt。

## 实现注意点（与代码注释一致）

- **栈/内存**：`rfilter.cpp` 中 `vector<vector<int>> buffer[chunknum]` 在 chunknum 很大时可能栈溢出，建议改为堆上分配（如 `vector<vector<vector<int>>>` 或动态数组）。
- **empty_tuple 与最小值**：用全 0 表示空位可能与“各维最小值”元组混淆；不包含最小值的数集可能引入“虚幻元组”（见 read_Page 附近 FIXME）。
- **大 allmranges**：`bitmapbytes` 用 int，极大时需考虑溢出。
- **FPR**：当前在 process_Queries 中计算并输出 FPR（假阳性率）。

## 总结

本项目实现了一个基于 chunk 的多维范围过滤器：将多维数据分块、建索引（位图或布隆），在范围查询时先过滤 chunk 再读数据，用于减少 I/O、加速多维范围查询；测试数据为气候相关多维数据。
