/**
 * 自测：collectBorderChunkIds 与按 chunk 暴力枚举的结果一致；并用 climate/query.txt 冒烟测试。
 * 从仓库根目录编译（需 -I 指向根目录以便找到 common.h 等头文件）：
 *   g++ -std=c++17 -O0 -g -I. BlockManager.cpp Timer.cpp bfilter.cpp rfilter.cpp \
 *       query.cpp workloadAnalyzer.cpp test/workload_analyzer_test.cpp \
 *       -o test/workload_analyzer_test
 * 运行：仓库根目录下 ./test/workload_analyzer_test；或在 test/ 下 ./workload_analyzer_test（会自动解析 query 路径）。
 */
#include "common.h"
#include "rfilter.h"
#include "workloadAnalyzer.h"

#include <cassert>
#include <fstream>
#include <iostream>
#include <unordered_set>

using namespace std;

/* 与 main.cpp 一致的全局定义（独立可执行文件，避免与 main 链接冲突） */
int subset_num;
int n;
int N;
int M = 4;
int B;
int bucket_num;
int independent = 1;
int dtype = 1;
vector<int> d = {4, 1121, 68, 7};
vector<int> dbit;
int dbit_sum;
int page_capacity;
vector<int> logical_size = {2, 16, 16, 4};
vector<int> logical_size0;
int lnum_max;
int batch;
vector<int> empty_tuple;
vector<int> shape;

int bitsperkey = 20;
BlockManager* block1;
int fcurpageid;
char* sdata1;
int beginbyte1;
vector<vector<int>> filter_offset;
int last_validchunk;

/** 支持从仓库根目录或 test/ 子目录运行，避免仅依赖 CWD 的相对路径失效 */
static string climate_query_path_string() {
    static const char* candidates[] = {"./data/test/climate/query.txt",
                                       "../data/test/climate/query.txt"};
    for (const char* p : candidates) {
        ifstream f(p);
        if (f.is_open())
            return string(p);
    }
    return string(candidates[0]);
}

static void init_globals_like_main() {
    sdata1 = new char[PAGESIZE];
    logical_size0.clear();
    for (size_t i = 0; i < d.size(); i++) {
        logical_size0.push_back((int)pow(ceil(sqrt(static_cast<double>(logical_size[i]))), 2));
    }
    dbit.clear();
    dbit_sum = 0;
    for (size_t i = 0; i < d.size(); i++) {
        double a = log(d[i]) / log(2);
        dbit.push_back((int)ceil(a));
        dbit_sum += dbit[dbit.size() - 1];
    }
    page_capacity = PAGESIZE * BYTE / dbit_sum;
    empty_tuple.clear();
    for (size_t i = 0; i < d.size(); i++)
        empty_tuple.push_back(0);
}

/**
 * 独立验证：对每个 chunk id 遍历所有查询，若存在某查询使其为 border 则记入集合。
 * 循环顺序与 collectBorderChunkIds 不同，用于交叉检验。
 */
static unordered_set<int> brute_border_chunks(const Rfilter& rf,
                                              const vector<vector<int>>& queries) {
    unordered_set<int> border;
    const int m = rf.m;
    vector<int> p1(m), p2(m), coor(m);

    for (int k = 0; k < rf.chunknum; k++) {
        for (const auto& query : queries) {
            int lowchunk = 0, highchunk = 0;
            for (int j = 0; j < m; j++) {
                p1[j] = query[static_cast<size_t>(2 * j)] / logical_size[j];
                lowchunk = (lowchunk << rf.piecesbit[j]) + p1[j];
                p2[j] = query[static_cast<size_t>(2 * j + 1)] / logical_size[j];
                highchunk = (highchunk << rf.piecesbit[j]) + p2[j];
            }
            if (k < lowchunk || k > highchunk)
                continue;
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
            if (inrange && isborderchunk) {
                border.insert(k);
                break;
            }
        }
    }
    return border;
}

static void assert_same_set(const vector<int>& sorted_vec,
                            const unordered_set<int>& expected) {
    assert(sorted_vec.size() == expected.size());
    for (int x : sorted_vec)
        assert(expected.count(x) == 1);
}

int main() {
    init_globals_like_main();
    Rfilter rf;

    /* 小规模合成查询：2 维便于心算，d 与 logical 在本测试中若不匹配 Rfilter 构造会错——
     * 此处仍用与 main 相同的 d/logical_size，仅构造两条窄查询做逻辑回归。 */
    vector<vector<int>> synth = {
        {0, 1, 0, 15, 0, 15, 0, 3}, // 全空间一条
        {0, 0, 100, 200, 10, 20, 0, 3},
    };
    vector<int> fast = collectBorderChunkIds(rf, synth);
    unordered_set<int> slow = brute_border_chunks(rf, synth);
    assert_same_set(fast, slow);

    string qpath_str = climate_query_path_string();
    const char* qpath = qpath_str.c_str();
    vector<vector<int>> climate_queries;
    loadQuery(qpath, climate_queries);
    vector<int> climate_ids = collectBorderChunkIds(rf, climate_queries);
    unordered_set<int> climate_brute = brute_border_chunks(rf, climate_queries);
    assert_same_set(climate_ids, climate_brute);

    vector<int> from_file = collectBorderChunkIdsFromQueries(rf, qpath);
    assert(climate_ids == from_file);

    cout << "workload_analyzer_test: OK. climate border chunk count = "
         << climate_ids.size() << endl;
    return 0;
}
