# dataset: climate

| 字段 | 值 |
|------|----|
| 原始路径 | `data/test/climate/`（只读，勿改） |
| 数据文件 | `data/test/climate/data.txt` |
| 维度数 M | 4 |
| 各维基数 d | `{4, 1121, 68, 7}` |
| logical_size | `{2, 16, 16, 4}` |
| 元组约数 | ~1459 行（以 `wc -l data.txt` 为准） |
| 查询格式 | 每行 8 个整数：`L0 H0 L1 H1 L2 H2 L3 H3` |

本目录不存放 `data.txt` 副本，避免与原始数据双份漂移。实验一律从上述原始路径只读引用。
