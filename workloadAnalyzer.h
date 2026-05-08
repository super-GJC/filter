#pragma once
#include <vector>

class Rfilter;

/**
 * 根据查询负载计算「边界块」chunk id 集合（去重、升序）。
 * 判定规则与 Rfilter::process_Queries() 中 isborderchunk==1 的块一致：
 * 与查询在分块网格上相交，且至少有一维的 chunk 下标落在该查询的 p1[j] 或 p2[j] 上
 * （即下标意义下的块网格边界，而非几何上的「部分重叠」）。
 *
 * 依赖全局配置：logical_size（与 Rfilter 构造时一致），由 main 或测试在构造 Rfilter 前初始化。
 */
std::vector<int> collectBorderChunkIds(const Rfilter& rf,
                                       const std::vector<std::vector<int>>& queries);

std::vector<int> collectBorderChunkIdsFromQueries(const Rfilter& rf,
                                                  const char* querypath);
