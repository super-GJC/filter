/**
 * 自测：collectBorderChunkIds / collectBorderChunkQueryMap 与暴力枚举一致；
 * 生成 HTML 可视化报告 test/output/border_chunk_qc_report.html
 *
 * 编译（仓库根目录）：
 *   g++ -std=c++17 -O0 -g -I. BlockManager.cpp Timer.cpp bfilter.cpp rfilter.cpp \
 *       query.cpp workloadAnalyzer.cpp test/workload_analyzer_test.cpp \
 *       -o test/workload_analyzer_test
 */
#include "common.h"
#include "rfilter.h"
#include "workloadAnalyzer.h"

#include <cassert>
#include <cstdio>
#include <fstream>
#include <iostream>
#include <sstream>
#include <unordered_set>

using namespace std;

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

static string report_output_path() {
    static const char* candidates[] = {"./test/output/border_chunk_qc_report.html",
                                       "../test/output/border_chunk_qc_report.html"};
    for (const char* p : candidates) {
        size_t slash = string(p).find_last_of('/');
        string dir = (slash != string::npos) ? string(p).substr(0, slash) : ".";
        ifstream probe(dir + "/.");
        (void)probe;
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

/** 对固定 chunk k，枚举所有查询，收集使 k 为 border 的查询下标 */
static vector<int> brute_qc_for_chunk(const Rfilter& rf, int k,
                                      const vector<vector<int>>& queries) {
    const int m = rf.m;
    vector<int> p1(m), p2(m), coor(m);
    vector<int> hit;

    for (size_t qi = 0; qi < queries.size(); qi++) {
        const auto& query = queries[qi];
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
        if (inrange && isborderchunk)
            hit.push_back(static_cast<int>(qi));
    }
    return hit;
}

static unordered_set<int> brute_border_chunks(const Rfilter& rf,
                                              const vector<vector<int>>& queries) {
    unordered_set<int> border;
    for (int k = 0; k < rf.chunknum; k++) {
        vector<int> qc = brute_qc_for_chunk(rf, k, queries);
        if (!qc.empty())
            border.insert(k);
    }
    return border;
}

static BorderChunkQueryMap brute_border_chunk_query_map(
    const Rfilter& rf, const vector<vector<int>>& queries) {
    BorderChunkQueryMap m;
    for (int k = 0; k < rf.chunknum; k++) {
        vector<int> qc = brute_qc_for_chunk(rf, k, queries);
        if (!qc.empty())
            m[k] = qc;
    }
    return m;
}

static void assert_same_set(const vector<int>& sorted_vec,
                            const unordered_set<int>& expected) {
    assert(sorted_vec.size() == expected.size());
    for (int x : sorted_vec)
        assert(expected.count(x) == 1);
}

static void assert_query_map_equal(const BorderChunkQueryMap& fast,
                                   const BorderChunkQueryMap& slow) {
    assert(fast.size() == slow.size());
    for (const auto& kv : fast) {
        auto it = slow.find(kv.first);
        assert(it != slow.end());
        assert(kv.second == it->second);
    }
}

static void assert_map_keys_match_border_ids(const BorderChunkQueryMap& qc_map,
                                             const vector<int>& border_ids) {
    unordered_set<int> keys;
    for (const auto& kv : qc_map)
        keys.insert(kv.first);
    unordered_set<int> borders(border_ids.begin(), border_ids.end());
    assert(keys == borders);
    for (int cid : border_ids) {
        assert(!qc_map.at(cid).empty());
    }
}

static string html_escape(const string& s) {
    string o;
    for (char c : s) {
        if (c == '&')
            o += "&amp;";
        else if (c == '<')
            o += "&lt;";
        else if (c == '>')
            o += "&gt;";
        else
            o += c;
    }
    return o;
}

struct ReportBuilder {
    ostringstream sections;
    int count = 0;

    void add(const string& html) {
        sections << html;
        count++;
    }

    bool write(const string& path) const {
        ofstream out(path);
        if (!out)
            return false;
        out << "<!DOCTYPE html><html lang=\"zh-CN\"><head><meta charset=\"utf-8\"/>\n";
        out << "<title>Border chunk Q_c 验证报告</title>\n";
        out << "<style>\n"
            << "body{font-family:system-ui,sans-serif;margin:1.5rem;background:#f8f9fa;color:#222;}\n"
            << "h1{border-bottom:2px solid #333;padding-bottom:.3rem;}\n"
            << "section.block{background:#fff;border-radius:8px;padding:1rem 1.25rem;margin:1rem "
               "0;box-shadow:0 1px 4px rgba(0,0,0,.08);}\n"
            << "table.matrix{border-collapse:collapse;font-size:13px;}\n"
            << "table.matrix th,table.matrix td{border:1px solid #ccc;padding:4px 8px;text-align:"
               "center;}\n"
            << "table.matrix th{background:#e9ecef;}\n"
            << "td.hit{background:#198754;color:#fff;font-weight:bold;}\n"
            << "td.cnt{background:#fff3cd;}\n"
            << "pre.qc-list{font-size:12px;overflow:auto;max-height:320px;}\n"
            << ".ok{color:#198754;}.bad{color:#dc3545;font-weight:bold;}\n"
            << ".legend{margin:.5rem 0;font-size:14px;}\n"
            << ".dot{display:inline-block;width:14px;height:14px;background:#198754;"
               "border-radius:50%;vertical-align:middle;margin-right:4px;}\n"
            << "</style></head><body>\n";
        out << "<h1>Border chunk &rarr; Q<sub>c</sub> 正确性可视化</h1>\n";
        out << "<p>生成时间：测试 <code>workload_analyzer_test</code> 通过时写入。"
               " 行 = chunk id，列 = 查询下标。</p>\n";
        out << "<p class=\"legend\"><span class=\"dot\"></span> 绿点 = (q,c) 满足 inrange &amp;&amp; "
               "isborderchunk（与 process_Queries 一致）</p>\n";
        out << sections.str();
        out << "</body></html>\n";
        return out.good();
    }
};

static string build_section_html(const string& title, const vector<vector<int>>& queries,
                                 const BorderChunkQueryMap& fast,
                                 const BorderChunkQueryMap& brute, bool match) {
    vector<int> chunk_ids;
    for (const auto& kv : fast)
        chunk_ids.push_back(kv.first);
    sort(chunk_ids.begin(), chunk_ids.end());
    const int nq = static_cast<int>(queries.size());

    ostringstream body;
    body << "<section class=\"block\">\n<h2>" << html_escape(title) << "</h2>\n";
    body << "<p>查询数 <b>" << nq << "</b>，border chunk 数 <b>" << chunk_ids.size()
         << "</b>，快路径 vs 暴力：<b class=\"" << (match ? "ok" : "bad") << "\">"
         << (match ? "完全一致" : "不一致") << "</b></p>\n";

    body << "<table class=\"matrix\"><thead><tr><th>chunk \\ query</th>";
    for (int q = 0; q < nq; q++)
        body << "<th>q" << q << "</th>";
    body << "<th>|Q<sub>c</sub>|</th></tr></thead><tbody>\n";

    for (int cid : chunk_ids) {
        body << "<tr><th>c" << cid << "</th>";
        unordered_set<int> qc_set(fast.at(cid).begin(), fast.at(cid).end());
        for (int q = 0; q < nq; q++) {
            bool hit = qc_set.count(q) != 0;
            body << "<td class=\"" << (hit ? "hit" : "") << "\">"
                 << (hit ? "&#9679;" : "") << "</td>";
        }
        body << "<td class=\"cnt\">" << fast.at(cid).size() << "</td></tr>\n";
    }
    body << "</tbody></table>\n<h3>Q<sub>c</sub> 明细</h3><pre class=\"qc-list\">\n";
    for (int cid : chunk_ids) {
        body << "c" << cid << " -> [";
        const auto& v = fast.at(cid);
        for (size_t i = 0; i < v.size(); i++) {
            if (i)
                body << ", ";
            body << "q" << v[i];
        }
        body << "]\n";
    }
    body << "</pre></section>\n";
    return body.str();
}

static void test_query_map_case(const Rfilter& rf, const vector<vector<int>>& queries,
                                const string& title, ReportBuilder& report) {
    BorderChunkQueryMap fast = collectBorderChunkQueryMap(rf, queries);
    BorderChunkQueryMap slow = brute_border_chunk_query_map(rf, queries);
    vector<int> border_ids = collectBorderChunkIds(rf, queries);

    assert_query_map_equal(fast, slow);
    assert_map_keys_match_border_ids(fast, border_ids);

    report.add(build_section_html(title, queries, fast, slow, true));
}

int main() {
    init_globals_like_main();
    Rfilter rf;
    ReportBuilder report;
    int failures = 0;

    vector<vector<int>> synth = {
        {0, 1, 0, 15, 0, 15, 0, 3},
        {0, 0, 100, 200, 10, 20, 0, 3},
    };

    vector<int> fast_border = collectBorderChunkIds(rf, synth);
    unordered_set<int> slow_border = brute_border_chunks(rf, synth);
    assert_same_set(fast_border, slow_border);
    test_query_map_case(rf, synth, "合成查询 (2 条)", report);

    string qpath_str = climate_query_path_string();
    const char* qpath = qpath_str.c_str();
    vector<vector<int>> climate_queries;
    loadQuery(qpath, climate_queries);

    vector<int> climate_ids = collectBorderChunkIds(rf, climate_queries);
    unordered_set<int> climate_brute = brute_border_chunks(rf, climate_queries);
    assert_same_set(climate_ids, climate_brute);

    vector<int> from_file = collectBorderChunkIdsFromQueries(rf, qpath);
    assert(climate_ids == from_file);

    BorderChunkQueryMap climate_fast = collectBorderChunkQueryMap(rf, climate_queries);
    BorderChunkQueryMap climate_slow = brute_border_chunk_query_map(rf, climate_queries);
    BorderChunkQueryMap climate_file = collectBorderChunkQueryMapFromQueries(rf, qpath);

    assert_query_map_equal(climate_fast, climate_slow);
    assert_query_map_equal(climate_fast, climate_file);
    assert_map_keys_match_border_ids(climate_fast, climate_ids);

    report.add(build_section_html("climate/query.txt (" + to_string(climate_queries.size()) +
                                      " 条查询)",
                                  climate_queries, climate_fast, climate_slow, true));

    string report_path = report_output_path();
    size_t slash = report_path.find_last_of("/\\");
    if (slash != string::npos) {
        string dir = report_path.substr(0, slash);
        string cmd = "mkdir -p " + dir;
        if (system(cmd.c_str()) != 0) {
            cerr << "warning: mkdir " << dir << " failed\n";
        }
    }
    if (!report.write(report_path)) {
        cerr << "failed to write " << report_path << endl;
        failures = 1;
    }

    size_t max_qc = 0;
    int sample_cid = -1;
    for (const auto& kv : climate_fast) {
        if (kv.second.size() > max_qc) {
            max_qc = kv.second.size();
            sample_cid = kv.first;
        }
    }

    cout << "workload_analyzer_test: OK\n";
    cout << "  collectBorderChunkIds (climate): " << climate_ids.size() << " border chunks\n";
    cout << "  collectBorderChunkQueryMap: " << climate_fast.size()
         << " chunks with non-empty Q_c; max |Q_c|=" << max_qc;
    if (sample_cid >= 0) {
        cout << " (e.g. chunk " << sample_cid << " -> {";
        const auto& sq = climate_fast.at(sample_cid);
        for (size_t i = 0; i < sq.size(); i++) {
            if (i)
                cout << ",";
            cout << "q" << sq[i];
        }
        cout << "})";
    }
    cout << "\n  HTML report: " << report_path << endl;

    return failures;
}
