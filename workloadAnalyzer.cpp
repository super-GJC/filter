#include "workloadAnalyzer.h"
#include "rfilter.h"
#include "common.h"

#include <unordered_set>
#include <algorithm>

using namespace std;

vector<int> collectBorderChunkIds(const Rfilter& rf,
                                  const vector<vector<int>>& queries) {
    const int m = rf.m;
    vector<int> p1(m), p2(m), coor(m);
    unordered_set<int> border;

    for (const auto& query : queries) {
        int lowchunk = 0, highchunk = 0;
        for (int j = 0; j < m; j++) {
            p1[j] = query[static_cast<size_t>(2 * j)] / logical_size[j];
            lowchunk = (lowchunk << rf.piecesbit[j]) + p1[j];
            p2[j] = query[static_cast<size_t>(2 * j + 1)] / logical_size[j];
            highchunk = (highchunk << rf.piecesbit[j]) + p2[j];
        }
        // 与 process_Queries 一致：遍历 [lowchunk, highchunk]，筛 inrange，
        // isborderchunk = 任一维 coor[j]==p1[j] 或 coor[j]==p2[j]
        for (int k = lowchunk; k <= highchunk; k++) {
            int cid = k;
            int inrange = 1;
            int isborderchunk = 0;
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
            if (isborderchunk)
                border.insert(k);
        }
    }

    vector<int> out(border.begin(), border.end());
    sort(out.begin(), out.end());
    return out;
}

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
    BorderChunkQueryMap out;

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

BorderChunkQueryMap collectBorderChunkQueryMapFromQueries(const Rfilter& rf,
                                                          const char* querypath) {
    vector<vector<int>> queries;
    loadQuery(querypath, queries);
    return collectBorderChunkQueryMap(rf, queries);
}
