# climate 查询负载生成说明

本文说明：`query.txt` 格式、climate 数据维度、生成脚本怎么工作，以及你怎么复现/改参数。

## 1. 现有格式（必须对齐）

程序用 `loadQuery()`（`query.cpp`）按**空格分隔**读入整数，每行一条查询：

```text
L0 H0 L1 H1 L2 H2 L3 H3
```

含义（与 `Rfilter::process_Queries` 一致）：

| 字段 | 含义 |
|------|------|
| `L_j` / `H_j` | 第 `j` 维闭区间下界 / 上界 |
| 合法范围 | `0 ≤ L_j ≤ H_j ≤ d[j]-1` |
| 维度数 M | 4（climate） |

示例（现有 `climate_q8` 第一条）：

```text
2 3 181 458 20 30 0 6
```

即维 0∈[2,3]，维 1∈[181,458]，维 2∈[20,30]，维 3∈[0,6]。

**不要**改写到 `data/test/climate/query.txt`；生成结果只进 `experiments/workloads/<name>/`。

## 2. 数据与域

| 项 | 值 | 依据 |
|----|-----|------|
| 数据文件 | `data/test/climate/data.txt`（只读） | 实验约定 |
| 行数 | 1459 | `wc -l` |
| 文件列数 | 5（前 4 列为维度，末列忽略） | 实测 |
| 域基数 `d` | `{4, 1121, 68, 7}` | `main.cpp` |
| 实际取值 | 各维 `[0, d[j]-1]`，与数据一致 | 实测 min/max |
| `logical_size` | `{2, 16, 16, 4}` | 分块用；生成查询时不改区间语义 |

## 3. 为什么扩到约 200 条

`climate_q8` 只有 8 条，单条 FPR 波动大，论文说服力弱。主实验用约 200 条，便于报均值/中位数。

| 负载目录 | 条数 | 角色 |
|----------|------|------|
| `climate_q8/` | 8 | 小负载复现 / 对照（手工已有） |
| `climate_q200/` | 200 | **主实验**（FPR≤0.1 筛选补全） |
| `climate_q200_mixed/` | 200 | 原始随机混合（对照） |
| `climate_q200_correlated/` | 200 | 可选压力测 |

## 4. 生成策略（脚本在做什么）

脚本：`gen_climate_queries.py`

### 4.1 各维选择性

选择性 = `(H_j - L_j + 1) / d[j]`。目标区间按 **climate_q8 实测** 标定，避免生成过宽/过窄、不像真实负载的框：

| 维 | d[j] | 目标选择性区间 |
|----|------|----------------|
| 0 | 4 | 0.25–0.75 |
| 1 | 1121 | 0.05–0.55 |
| 2 | 68 | 0.10–0.50 |
| 3 | 7 | 0.15–1.00 |

### 4.2 两种查询

1. **uniform（均匀）**  
   在域上独立随机抽闭区间，**不依赖**数据点。部分查询可能 0 命中（对测空区假阳性有用）。

2. **correlated（相关）**  
   先从 `data.txt` 均匀抽一条真实元组作锚点，再在各维扩展区间并**保证覆盖该点**。命中率更高，模拟「围着数据问」。

3. **mixed（混合，主负载）**  
   默认 **50% uniform + 50% correlated**，再按种子打乱。兼顾空区压力与真实命中。

### 4.3 固定种子（可复现）

| 产物 | mode | seed |
|------|------|------|
| `climate_q200_mixed` | mixed | `20260916` |
| `climate_q200_correlated` | correlated | `20260917` |

同版本脚本 + 同种子 + 同 `data.txt` → 相同 `query.txt`。

## 5. 怎么用

在仓库根目录：

```bash
# 一键写出两个默认负载，并更新各自 META.md
python3 experiments/workloads/gen/gen_climate_queries.py --write-defaults
```

自定义单次输出：

```bash
python3 experiments/workloads/gen/gen_climate_queries.py \
  --mode mixed --n 200 --seed 20260916 \
  --out experiments/workloads/climate_q200_mixed/query.txt \
  --meta experiments/workloads/climate_q200_mixed/META.md
```

跑 MD 实验时，在 `main.cpp` 只改路径，例如：

```text
workload_dir = ./experiments/workloads/climate_q200/
run_dir      = ./experiments/runs/YYYYMMDD_md_climate_q200/raw/
```

数据仍读 `./data/test/climate/`，**不要**覆盖其中的 `query.txt`。

## 6. 脚本会写什么

每个负载目录：

```text
experiments/workloads/<name>/
├── query.txt   # 生成的查询
└── META.md     # 种子、模式、选择性、命中统计等
```

`META.md` 里会记录：非空命中查询数、命中元组 median/mean、各维实际平均选择性，便于写论文方法小节。

## 7. 约定与边界

- 输入只读 `data/test/climate/data.txt`；输出只写 `experiments/workloads/...`
- 不覆盖 `data/test/climate/query.txt`、不写入 `runs/`
- 若改 `d` 或 data，必须同步改脚本默认 `DEFAULT_D` 并重新 `--write-defaults`
- 选择性可用 `--sel 0.25-0.75,0.05-0.55,0.10-0.50,0.15-1.00` 覆盖
