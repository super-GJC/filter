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
