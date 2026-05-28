#include "workloadAnalyzer.h"
#include "rfilter.h"
#include "common.h"

#include <unordered_set>
#include <algorithm>
#include <utility>

using namespace std;

/**
 * 收集所有 border chunk 的 id（去重），不记录「是哪条查询命中」。
 * 判定逻辑与 process_Queries 中 isborderchunk==1 完全一致。
 */
vector<int> collectBorderChunkIds(const Rfilter& rf,
                                  const vector<vector<int>>& queries) {
    const int m = rf.m;
    vector<int> p1(m), p2(m), coor(m); // p1/p2：查询在各维上的 chunk 下标包络；coor：候选块 k 的各维坐标
    unordered_set<int> border;         // 全局 border chunk 集合（去重）

    // 外层：逐条查询遍历负载
    for (const auto& query : queries) {
        int lowchunk = 0, highchunk = 0;

        // 将查询值域 [q[2j], q[2j+1]] 映射为各维 chunk 下标区间 [p1[j], p2[j]]，
        // 并合成为线性 chunk id 的粗筛范围 [lowchunk, highchunk]
        for (int j = 0; j < m; j++) {
            p1[j] = query[static_cast<size_t>(2 * j)] / logical_size[j];
            lowchunk = (lowchunk << rf.piecesbit[j]) + p1[j];
            p2[j] = query[static_cast<size_t>(2 * j + 1)] / logical_size[j];
            highchunk = (highchunk << rf.piecesbit[j]) + p2[j];
        }

        // 枚举粗筛范围内的每个候选块 k
        for (int k = lowchunk; k <= highchunk; k++) {
            int cid = k;
            int inrange = 1;       // k 是否真正落在查询的 chunk 包络内（各维 coor[j]∈[p1[j],p2[j]]）
            int isborderchunk = 0; // 是否至少一维「贴边」（coor[j] 等于 p1[j] 或 p2[j]）

            // 从高维到低维解码 k → coor[]，并判定 inrange / border
            for (int j = m - 1; j >= 0; j--) {
                coor[j] = cid & (rf.powpieces[j] - 1);
                cid = cid >> rf.piecesbit[j];
                if (coor[j] < p1[j] || coor[j] > p2[j]) {
                    inrange = 0;
                    break;
                }
                if (coor[j] == p1[j] || coor[j] == p2[j])
                    isborderchunk = 1;
            }
            if (inrange == 0)
                continue;
            // 仅 border 块在 process_Queries 中会使用块上过滤器
            if (isborderchunk)
                border.insert(k);
        }
    }

    // 转为升序 vector 返回（set 本身无序，便于与暴力结果逐元素比对）
    vector<int> out(border.begin(), border.end());
    sort(out.begin(), out.end());
    return out;
}

/** 从 query 文件加载后调用 collectBorderChunkIds */
vector<int> collectBorderChunkIdsFromQueries(const Rfilter& rf,
                                             const char* querypath) {
    vector<vector<int>> queries;
    loadQuery(querypath, queries);
    return collectBorderChunkIds(rf, queries);
}

/**
 * 为每个 border chunk 建立 Q_c：会用到该块上过滤器的查询下标列表。
 *
 * 与 collectBorderChunkIds 使用同一套判定（复刻 process_Queries），区别是：
 * - collectBorderChunkIds 只记「有哪些 chunk 曾是 border」；
 * - 本函数还记「每个这样的 chunk 被哪些查询命中」→ out[chunk_id] = Q_c。
 *
 * 返回的 map 仅包含 Q_c 非空的 chunk（即至少有一条查询使其为 border）。
 */
BorderChunkQueryMap collectBorderChunkQueryMap(const Rfilter& rf,
                                               const vector<vector<int>>& queries) {
    const int m = rf.m;
    vector<int> p1(m), p2(m), coor(m);
    BorderChunkQueryMap out; // key=chunk_id, value=命中该块的查询下标列表 Q_c

    // 外层：按查询下标 qi 遍历负载（qi 与 queries[qi]、loadQuery 读入顺序一致）
    for (size_t qi = 0; qi < queries.size(); qi++) {
        const auto& query = queries[qi];

        // 将本条查询的值域范围映射到各维的 chunk 下标区间 [p1[j], p2[j]]，
        // 再合成线性 chunk id 范围 [lowchunk, highchunk]（与 process_Queries 相同）
        int lowchunk = 0, highchunk = 0;
        for (int j = 0; j < m; j++) {
            p1[j] = query[static_cast<size_t>(2 * j)] / logical_size[j];
            lowchunk = (lowchunk << rf.piecesbit[j]) + p1[j];
            p2[j] = query[static_cast<size_t>(2 * j + 1)] / logical_size[j];
            highchunk = (highchunk << rf.piecesbit[j]) + p2[j];
        }

        // 中层：枚举查询包络内的每个候选块 k（粗筛，可能仍含实际不相交的 k）
        for (int k = lowchunk; k <= highchunk; k++) {
            int cid = k;
            int inrange = 1;       // k 是否与查询在各维 chunk 网格上真正相交
            int isborderchunk = 0; // k 是否为 border（至少一维贴查询包络边界）

            // 内层：把线性 id k 解码为各维块坐标 coor[j]，并判定 inrange / border
            for (int j = m - 1; j >= 0; j--) {
                coor[j] = cid & (rf.powpieces[j] - 1);
                cid = cid >> rf.piecesbit[j];
                if (coor[j] < p1[j] || coor[j] > p2[j]) {
                    inrange = 0;
                    break;
                }
                // 该维块下标等于查询在该维上的最小/最大 chunk 下标 → 贴边 → border
                if (coor[j] == p1[j] || coor[j] == p2[j])
                    isborderchunk = 1;
            }
            if (inrange == 0)
                continue;
            // 仅 border 块在 process_Queries 中会走块上过滤器，故只把这些 (qi,k) 记入 Q_c
            if (isborderchunk)
                out[k].push_back(static_cast<int>(qi));
        }
    }

    // 后处理：每个 chunk 的 Q_c 排序并去重，得到稳定、可比的查询下标列表
    for (auto& kv : out) {
        vector<int>& v = kv.second;
        sort(v.begin(), v.end());
        v.erase(unique(v.begin(), v.end()), v.end());
    }
    return out;
}

/** 从 query 文件加载后调用 collectBorderChunkQueryMap */
BorderChunkQueryMap collectBorderChunkQueryMapFromQueries(const Rfilter& rf,
                                                          const char* querypath) {
    vector<vector<int>> queries;
    loadQuery(querypath, queries);
    return collectBorderChunkQueryMap(rf, queries);
}

/**
 * 将线性 chunk id 解码为各维块下标 coor[j]。
 * 解码顺序：j 从 m-1 到 0（高维到低维），与 process_Queries / collectBorderChunkQueryMap 一致。
 */
static void chunk_id_to_coor(const Rfilter& rf, int chunk_id, vector<int>& coor) {
    coor.resize(static_cast<size_t>(rf.m));
    int cid = chunk_id;
    for (int j = rf.m - 1; j >= 0; j--) {
        // 取当前维的低 piecesbit[j] 位作为该维块下标
        coor[static_cast<size_t>(j)] = cid & (rf.powpieces[j] - 1);
        cid = cid >> rf.piecesbit[j];
    }
}

/**
 * 计算单条查询 q 在块 c 的某一维 dim 上的重叠比例 rho(q,c,dim)。
 *
 * Lc,Hc：块在该维上的值域端点（由 coor[dim] 与 logical_size[dim] 决定）；
 * chunk_len：块在该维上的点数（= Hc-Lc+1，此处等于 logical_size[dim]）。
 *
 * 返回 |I(q,c,dim)| / chunk_len，I 为 [qlo,qhi] 与 [Lc,Hc] 的交集；无交集则返回 0。
 */
static double rho_query_chunk_dim(const vector<int>& query, int dim,
                                  int Lc, int Hc, int chunk_len) {
    const int qlo = query[static_cast<size_t>(2 * dim)];     // 查询在该维的下界
    const int qhi = query[static_cast<size_t>(2 * dim + 1)]; // 查询在该维的上界
    const int ilo = max(qlo, Lc);                            // 交集下界
    const int ihi = min(qhi, Hc);                            // 交集上界
    if (ilo > ihi)
        return 0.0; // 不相交（border 正常命中时不应出现，防御性处理）
    const int intersect_len = ihi - ilo + 1;
    return static_cast<double>(intersect_len) / static_cast<double>(chunk_len);
}

/**
 * 在已有 Q_c（qc_map）上，为每个 border chunk 计算各维 rho_bar（平均 ρ）与 rho_min（最小 ρ）。
 *
 * 仅遍历 qc_map 中的 chunk；每个 chunk 只对 Q_c 中的查询聚合，不对全体查询平均。
 */
BorderChunkRhoMap computeBorderChunkRhoStats(const Rfilter& rf,
                                             const BorderChunkQueryMap& qc_map,
                                             const vector<vector<int>>& queries) {
    const int m = rf.m;
    BorderChunkRhoMap out; // chunk_id -> 该块各维 rho 统计
    vector<int> coor(m);   // 当前块的各维块下标，由 chunk_id 解码得到

    // 外层：遍历每个已有 Q_c 的 border chunk
    for (const auto& kv : qc_map) {
        const int chunk_id = kv.first;
        const vector<int>& Qc = kv.second; // 会用到该块过滤器的查询下标列表
        if (Qc.empty())
            continue; // 理论上 qc_map 不应含空列表，跳过以防万一

        // 步骤 1：chunk_id -> coor[]，用于确定块在各维值域上的 [Lc, Hc]
        chunk_id_to_coor(rf, chunk_id, coor);

        // 步骤 2：为该块初始化 profile；rho_min 初值 1.0 表示「尚未见到更小的 ρ」
        BorderChunkRhoProfile profile;
        profile.rho_bar.assign(static_cast<size_t>(m), 0.0);
        profile.rho_min.assign(static_cast<size_t>(m), 1.0);

        // 中层：对块的每一维 j 分别统计
        for (int j = 0; j < m; j++) {
            // 步骤 3：由块坐标 coor[j] 得到该维值域 [Lc, Hc] 与块长 chunk_len
            const int Lc = coor[static_cast<size_t>(j)] * logical_size[j];
            const int Hc = (coor[static_cast<size_t>(j)] + 1) * logical_size[j] - 1;
            const int chunk_len = logical_size[j];

            double sum_rho = 0.0; // 累加 Q_c 中各查询的 ρ，用于求平均
            double min_rho = 1.0; // Q_c 中该维 ρ 的最小值（保护「存在窄查询」的维）

            // 内层：仅对 Q_c 中的查询计算 ρ(q,c,j)，不做全负载平均
            for (int qi : Qc) {
                const double r = rho_query_chunk_dim(queries[static_cast<size_t>(qi)], j,
                                                       Lc, Hc, chunk_len);
                sum_rho += r;
                if (r < min_rho)
                    min_rho = r;
            }

            // 步骤 4：写入该维聚合结果
            profile.rho_bar[static_cast<size_t>(j)] =
                sum_rho / static_cast<double>(Qc.size()); // barρ(c,d) = (1/|Q_c|) Σ ρ
            profile.rho_min[static_cast<size_t>(j)] = min_rho; // minρ(c,d) = min_{q∈Q_c} ρ
        }

        // 步骤 5：将该块的 profile 存入输出 map（移动语义避免拷贝 vector）
        out[chunk_id] = std::move(profile);
    }
    return out;
}

/**
 * 一站式：先 collectBorderChunkQueryMap 得到 Q_c，再 computeBorderChunkRhoStats 得到 ρ 统计。
 */
BorderChunkRhoMap collectBorderChunkRhoStats(const Rfilter& rf,
                                             const vector<vector<int>>& queries) {
    BorderChunkQueryMap qc_map = collectBorderChunkQueryMap(rf, queries);
    return computeBorderChunkRhoStats(rf, qc_map, queries);
}

/** 从 query 文件加载后调用 collectBorderChunkRhoStats */
BorderChunkRhoMap collectBorderChunkRhoStatsFromQueries(const Rfilter& rf,
                                                        const char* querypath) {
    vector<vector<int>> queries;
    loadQuery(querypath, queries);
    return collectBorderChunkRhoStats(rf, queries);
}
