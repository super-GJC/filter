/**
 * 层 B 自测：BorderChunkRhoMap 与暴力逐 (c,d,q) 计算一致；含 filter_contribution 闭式例。
 *
 * 编译（仓库根目录）：
 *   g++ -std=c++17 -O0 -g -I. BlockManager.cpp Timer.cpp bfilter.cpp rfilter.cpp \
 *       query.cpp workloadAnalyzer.cpp test/workload_rho_test.cpp \
 *       -o test/workload_rho_test
 */
#include "common.h"
#include "rfilter.h"
#include "workloadAnalyzer.h"

#include <cassert>
#include <cmath>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>

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

static const double kRhoEps = 1e-12;

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

static string border_chunk_rho_report_path() {
    return "./test/output/border_chunk_rho_report.html";
}

static string rho_report_compact_path() {
    return "./test/output/rho_report.html";
}

static string rho_cell_class(double rho_bar) {
    if (rho_bar >= 0.9375)
        return "hi";
    if (rho_bar <= 0.125)
        return "lo";
    return "";
}

static string format_rho(double v) {
    ostringstream os;
    if (fabs(v - 1.0) < 1e-9)
        os << "1";
    else if (fabs(v - round(v)) < 1e-9)
        os << static_cast<long long>(round(v));
    else
        os << setprecision(6) << v;
    return os.str();
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

static void chunk_id_to_coor(const Rfilter& rf, int chunk_id, vector<int>& coor) {
    coor.resize(static_cast<size_t>(rf.m));
    int cid = chunk_id;
    for (int j = rf.m - 1; j >= 0; j--) {
        coor[static_cast<size_t>(j)] = cid & (rf.powpieces[j] - 1);
        cid = cid >> rf.piecesbit[j];
    }
}

static double rho_query_chunk_dim(const vector<int>& query, int dim, int Lc, int Hc, int chunk_len) {
    const int qlo = query[static_cast<size_t>(2 * dim)];
    const int qhi = query[static_cast<size_t>(2 * dim + 1)];
    const int ilo = max(qlo, Lc);
    const int ihi = min(qhi, Hc);
    if (ilo > ihi)
        return 0.0;
    return static_cast<double>(ihi - ilo + 1) / static_cast<double>(chunk_len);
}

static BorderChunkRhoProfile brute_rho_profile(const Rfilter& rf, int chunk_id,
                                               const vector<int>& Qc,
                                               const vector<vector<int>>& queries) {
    const int m = rf.m;
    vector<int> coor(m);
    chunk_id_to_coor(rf, chunk_id, coor);

    BorderChunkRhoProfile profile;
    profile.rho_bar.assign(static_cast<size_t>(m), 0.0);
    profile.rho_min.assign(static_cast<size_t>(m), 1.0);

    for (int j = 0; j < m; j++) {
        const int Lc = coor[static_cast<size_t>(j)] * logical_size[j];
        const int Hc = (coor[static_cast<size_t>(j)] + 1) * logical_size[j] - 1;
        const int chunk_len = logical_size[j];
        double sum = 0.0;
        double rmin = 1.0;
        for (int qi : Qc) {
            const double r =
                rho_query_chunk_dim(queries[static_cast<size_t>(qi)], j, Lc, Hc, chunk_len);
            sum += r;
            if (r < rmin)
                rmin = r;
        }
        profile.rho_bar[static_cast<size_t>(j)] = sum / static_cast<double>(Qc.size());
        profile.rho_min[static_cast<size_t>(j)] = rmin;
    }
    return profile;
}

static BorderChunkRhoMap brute_border_chunk_rho_map(const Rfilter& rf,
                                                  const BorderChunkQueryMap& qc_map,
                                                  const vector<vector<int>>& queries) {
    BorderChunkRhoMap m;
    for (const auto& kv : qc_map) {
        m[kv.first] = brute_rho_profile(rf, kv.first, kv.second, queries);
    }
    return m;
}

static void assert_rho_equal(double a, double b, const char* msg) {
    if (fabs(a - b) > kRhoEps) {
        cerr << "rho mismatch: " << msg << " got " << a << " expected " << b << endl;
        assert(false);
    }
}

static void assert_rho_profiles_equal(const BorderChunkRhoProfile& fast,
                                      const BorderChunkRhoProfile& slow, int chunk_id) {
    assert(fast.rho_bar.size() == slow.rho_bar.size());
    assert(fast.rho_min.size() == slow.rho_min.size());
    for (size_t j = 0; j < fast.rho_bar.size(); j++) {
        ostringstream msg;
        msg << "chunk " << chunk_id << " dim " << j << " rho_bar";
        assert_rho_equal(fast.rho_bar[j], slow.rho_bar[j], msg.str().c_str());
        msg.str("");
        msg << "chunk " << chunk_id << " dim " << j << " rho_min";
        assert_rho_equal(fast.rho_min[j], slow.rho_min[j], msg.str().c_str());
    }
}

static void assert_rho_maps_equal(const BorderChunkRhoMap& fast, const BorderChunkRhoMap& slow) {
    assert(fast.size() == slow.size());
    for (const auto& kv : fast) {
        assert_rho_profiles_equal(kv.second, slow.at(kv.first), kv.first);
    }
}

/** filter_contribution 文档例：d1=[0,14]×2，d2=[0,15] 与 [0,1]；在 dim1/dim2 块坐标为 0 的 chunk 上验证 */
static void test_hand_example_rho_ordering(const Rfilter& rf) {
    vector<vector<int>> hand = {
        {0, 3, 0, 14, 0, 15, 0, 3},
        {0, 3, 0, 14, 0, 1, 0, 3},
    };
    BorderChunkQueryMap qc = collectBorderChunkQueryMap(rf, hand);
    BorderChunkRhoMap rho = computeBorderChunkRhoStats(rf, qc, hand);

    int found = -1;
    for (const auto& kv : qc) {
        if (kv.second.size() == 2 && kv.second[0] == 0 && kv.second[1] == 1) {
            vector<int> coor(rf.m);
            chunk_id_to_coor(rf, kv.first, coor);
            if (coor[1] == 0 && coor[2] == 0) {
                found = kv.first;
                break;
            }
        }
    }
    assert(found >= 0);

    const BorderChunkRhoProfile& p = rho.at(found);
    const double expect_bar_d1 = 15.0 / 16.0;
    const double expect_bar_d2 = 9.0 / 16.0;
    const double expect_min_d1 = 15.0 / 16.0;
    const double expect_min_d2 = 1.0 / 8.0;

    assert_rho_equal(p.rho_bar[1], expect_bar_d1, "hand d1 rho_bar");
    assert_rho_equal(p.rho_bar[2], expect_bar_d2, "hand d2 rho_bar");
    assert_rho_equal(p.rho_min[1], expect_min_d1, "hand d1 rho_min");
    assert_rho_equal(p.rho_min[2], expect_min_d2, "hand d2 rho_min");
    assert(p.rho_bar[1] > p.rho_bar[2]);

    cout << "  hand example chunk c" << found << ": rho_bar[dim1]=" << p.rho_bar[1]
         << " rho_bar[dim2]=" << p.rho_bar[2] << " (d1 > d2 => d1 更优先舍弃)\n";
}

/** border_chunk_rho_report.html：分列展示 rho_bar / rho_min，含 |Q_c|，按 |Q_c| 降序 */
static bool write_border_chunk_rho_report_html(const string& path, const Rfilter& rf,
                                               const BorderChunkQueryMap& qc,
                                               const BorderChunkRhoMap& rho) {
    vector<int> ids;
    ids.reserve(rho.size());
    for (const auto& kv : rho)
        ids.push_back(kv.first);

    sort(ids.begin(), ids.end(), [&](int a, int b) {
        const size_t sa = qc.at(a).size();
        const size_t sb = qc.at(b).size();
        if (sa != sb)
            return sa > sb;
        return a < b;
    });

    ofstream out(path);
    if (!out)
        return false;
    out << "<!DOCTYPE html><html lang=\"zh-CN\"><head><meta charset=\"utf-8\"/>\n";
    out << "<title>Border chunk rho 报告</title>\n";
    out << "<style>body{font-family:system-ui,sans-serif;margin:1.5rem;background:#f8f9fa;}"
        << "table{border-collapse:collapse;font-size:12px;background:#fff;}"
        << "th,td{border:1px solid #ccc;padding:4px 6px;text-align:center;}"
        << "th{background:#e9ecef;}</style></head><body>\n";
    out << "<h1>Border chunk &rho; 统计（共 " << ids.size() << " 块）</h1>\n";
    out << "<p>climate 全部 border chunk；列：rho_bar / rho_min 分列；行按 |Q<sub>c</sub>| 降序，|Q_c| 相同按 chunk id 升序。</p>\n";

    out << "<table><thead><tr><th>chunk</th><th>|Q<sub>c</sub>|</th>";
    for (int j = 0; j < rf.m; j++)
        out << "<th>d" << j << " bar</th><th>d" << j << " min</th>";
    out << "</tr></thead><tbody>\n";

    for (int cid : ids) {
        const auto& p = rho.at(cid);
        out << "<tr><td>c" << cid << "</td><td>" << qc.at(cid).size() << "</td>";
        for (int j = 0; j < rf.m; j++) {
            out << "<td>" << p.rho_bar[static_cast<size_t>(j)] << "</td>";
            out << "<td>" << p.rho_min[static_cast<size_t>(j)] << "</td>";
        }
        out << "</tr>\n";
    }
    out << "</tbody></table></body></html>\n";
    return true;
}

/** rho_report.html：每维一格「bar / min」，高 bar 标 hi、低 bar 标 lo，按 chunk id 升序 */
static bool write_rho_report_compact_html(const string& path, const Rfilter& rf,
                                         const BorderChunkRhoMap& rho) {
    vector<int> ids;
    ids.reserve(rho.size());
    for (const auto& kv : rho)
        ids.push_back(kv.first);
    sort(ids.begin(), ids.end());

    ofstream out(path);
    if (!out)
        return false;
    out << "<!DOCTYPE html><html lang=\"zh-CN\"><head><meta charset=\"utf-8\"/>\n";
    out << "<title>Border chunk rho report</title>\n";
    out << "<style>\n"
        << "body{font-family:system-ui,sans-serif;margin:1.5rem;background:#f8f9fa;}\n"
        << "table{border-collapse:collapse;font-size:12px;background:#fff;}\n"
        << "th,td{border:1px solid #ccc;padding:4px 8px;text-align:center;}\n"
        << "th{background:#e9ecef;} .hi{background:#d1e7dd;} .lo{background:#f8d7da;}\n"
        << "</style></head><body>\n";
    out << "<h1>&rho; 统计（&rho;<sub>bar</sub> / &rho;<sub>min</sub>）</h1>\n";
    out << "<p>块数 " << ids.size() << "，维数 " << rf.m
        << "。单元格：均值 / 最小；bar&ge;0.9375 为 hi，bar&le;0.125 为 lo。</p>\n";
    out << "<table><thead><tr><th>chunk</th>";
    for (int j = 0; j < rf.m; j++)
        out << "<th>d" << j << "</th>";
    out << "</tr></thead><tbody>\n";

    for (int cid : ids) {
        const auto& p = rho.at(cid);
        out << "<tr><th>c" << cid << "</th>";
        for (int j = 0; j < rf.m; j++) {
            const double bar = p.rho_bar[static_cast<size_t>(j)];
            const double rmin = p.rho_min[static_cast<size_t>(j)];
            const string cls = rho_cell_class(bar);
            out << "<td class=\"" << cls << "\">" << format_rho(bar) << " / " << format_rho(rmin)
                << "</td>";
        }
        out << "</tr>\n";
    }
    out << "</tbody></table></body></html>\n";
    return true;
}

int main() {
    init_globals_like_main();
    Rfilter rf;
    int failures = 0;

    vector<vector<int>> synth = {
        {0, 1, 0, 15, 0, 15, 0, 3},
        {0, 0, 100, 200, 10, 20, 0, 3},
    };
    BorderChunkQueryMap synth_qc = collectBorderChunkQueryMap(rf, synth);
    BorderChunkRhoMap synth_rho = computeBorderChunkRhoStats(rf, synth_qc, synth);
    BorderChunkRhoMap synth_brute = brute_border_chunk_rho_map(rf, synth_qc, synth);
    assert_rho_maps_equal(synth_rho, synth_brute);

    BorderChunkRhoMap synth_all = collectBorderChunkRhoStats(rf, synth);
    assert_rho_maps_equal(synth_rho, synth_all);

    test_hand_example_rho_ordering(rf);

    string qpath = climate_query_path_string();
    vector<vector<int>> climate_queries;
    loadQuery(qpath.c_str(), climate_queries);
    BorderChunkQueryMap climate_qc = collectBorderChunkQueryMap(rf, climate_queries);
    BorderChunkRhoMap climate_rho = computeBorderChunkRhoStats(rf, climate_qc, climate_queries);
    BorderChunkRhoMap climate_brute = brute_border_chunk_rho_map(rf, climate_qc, climate_queries);
    BorderChunkRhoMap climate_file = collectBorderChunkRhoStatsFromQueries(rf, qpath.c_str());

    assert_rho_maps_equal(climate_rho, climate_brute);
    assert_rho_maps_equal(climate_rho, climate_file);

    system("mkdir -p ./test/output");
    assert(climate_rho.size() == 352);

    if (!write_border_chunk_rho_report_html(border_chunk_rho_report_path(), rf, climate_qc,
                                            climate_rho)) {
        cerr << "failed to write " << border_chunk_rho_report_path() << endl;
        failures = 1;
    }
    if (!write_rho_report_compact_html(rho_report_compact_path(), rf, climate_rho)) {
        cerr << "failed to write " << rho_report_compact_path() << endl;
        failures = 1;
    }

    cout << "workload_rho_test: OK\n";
    cout << "  climate border chunks with rho: " << climate_rho.size() << "\n";
    cout << "  hand-example ordering verified (rho_bar[d1] > rho_bar[d2])\n";
    cout << "  HTML (352 chunks): " << border_chunk_rho_report_path() << "\n";
    cout << "  HTML (352 chunks): " << rho_report_compact_path() << endl;
    return failures;
}
