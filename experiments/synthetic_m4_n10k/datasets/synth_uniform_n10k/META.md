# dataset: synth_uniform_n10k

| 字段 | 值 |
|------|----|
| 路径 | `experiments/synthetic_m4_n10k/datasets/synth_uniform_n10k/data.txt` |
| 元组数 n | 10000 |
| 维数 M | 4 |
| 各维基数 d | `{16, 256, 64, 16}` |
| logical_size（实验约定） | `{2, 16, 16, 4}` |
| 分布 | 各维独立均匀；禁止全 0 元组；去重 |
| seed | 20261116 |
| 形态 | 程序输入整数元组（无需再编码） |

## 安全门槛

| 项 | 值 |
|----|----|
| oneDrange_bit | [4, 6, 6, 4] |
| Σ oneDrange_bit | 20 |
| allmranges | 1048576 (=2^20) |
| bitmapbytes（单块位图上限） | 131072 |
| piecesbit | [3, 4, 2, 2] |
| chunknum | 2048 |
| 门槛通过 | True |

## 数据概况

| 项 | 值 |
|----|----|
| 各维实际出现值数 | [16, 256, 64, 16] |
| 全 0 元组 | 0 |
