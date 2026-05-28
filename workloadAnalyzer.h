#pragma once
#include <unordered_map>
#include <vector>

class Rfilter;

/** chunk_id -> 命中该块的查询下标列表 Q_c（升序、无重复；下标与 queries 向量顺序一致） */
using BorderChunkQueryMap = std::unordered_map<int, std::vector<int>>;

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

/**
 * 对每个 border chunk c，记录 Q_c：所有使 (q,c) 满足 inrange && isborderchunk==1 的查询下标。
 * 判定与 collectBorderChunkIds / process_Queries 一致；仅包含有至少一条此类查询的 chunk。
 */
BorderChunkQueryMap collectBorderChunkQueryMap(const Rfilter& rf,
                                               const std::vector<std::vector<int>>& queries);

BorderChunkQueryMap collectBorderChunkQueryMapFromQueries(const Rfilter& rf,
                                                          const char* querypath);

/**
 * 单个 border chunk 在 Q_c 负载下的各维 ρ 统计（与 queries 维度数 m 等长）。
 * rho_bar[d] = (1/|Q_c|) * sum_{q in Q_c} rho(q,c,d)
 * rho_min[d] = min_{q in Q_c} rho(q,c,d)
 * 其中 rho(q,c,d) = |I(q,c,d)| / ell_c(d)，I 为查询值域与块值域 [L_c,H_c] 的交集（与 process_Queries 裁剪一致）。
 */
struct BorderChunkRhoProfile {
    std::vector<double> rho_bar;
    std::vector<double> rho_min;
};

/** chunk_id -> 该块各维 rho_bar / rho_min（仅含 collectBorderChunkQueryMap 中的 border chunk） */
using BorderChunkRhoMap = std::unordered_map<int, BorderChunkRhoProfile>;

/**
 * 在已有 Q_c 上计算每个 border chunk 的 rho_bar、rho_min。
 * qc_map 须与 queries 对应（通常为 collectBorderChunkQueryMap 的返回值）。
 */
BorderChunkRhoMap computeBorderChunkRhoStats(const Rfilter& rf,
                                             const BorderChunkQueryMap& qc_map,
                                             const std::vector<std::vector<int>>& queries);

/** collectBorderChunkQueryMap + computeBorderChunkRhoStats */
BorderChunkRhoMap collectBorderChunkRhoStats(const Rfilter& rf,
                                             const std::vector<std::vector<int>>& queries);

BorderChunkRhoMap collectBorderChunkRhoStatsFromQueries(const Rfilter& rf,
                                                        const char* querypath);
