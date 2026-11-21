// m4 × SNARF adapter: chunk-level FPR for 1D-best / 1D-AND (SuRF-style).
// Mirrors climate_surf_bench grid / border / FPR. Does NOT modify MD / SuRF / Grafite.
//
// Note: upstream SNARF asserts N>10000 in snarf_model_builder. Per-chunk key counts
// are far smaller, so this adapter is built with -DNDEBUG (see build.sh) to allow the
// same model path with num_models=ceil(N/10000). Documented, SNARF sources untouched.

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <numeric>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_set>
#include <vector>

#include "include/snarf.cpp"

namespace {

using Clock = std::chrono::steady_clock;

struct Config {
    std::string data_path =
        "../../../experiments/synthetic_m4_n10k/datasets/synth_uniform_n10k/data.txt";
    std::string query_path =
        "../../../experiments/synthetic_m4_n10k/workloads/synth_uniform_q100/query.txt";
    std::string out_dir = "../output";
    std::string mode = "both";  // best | and | both
    std::vector<int> d = {16, 256, 64, 16};
    std::vector<int> logical_size = {2, 16, 16, 4};
    double bits_per_key = 10.0;
    int batch_size = 100;
    int best_dim = -1;  // -1 = auto
};

struct ChunkMeta {
    int piecesbit = 0;
    int powpieces = 0;
};

struct ChunkGrid {
    int m = 0;
    int chunknum = 0;
    std::vector<ChunkMeta> dim;
    std::vector<int> logical_size;
    std::vector<int> d;
};

ChunkGrid make_grid(const std::vector<int>& d, const std::vector<int>& logical_size) {
    if (d.size() != logical_size.size() || d.empty())
        throw std::runtime_error("d and logical_size size mismatch / empty");
    ChunkGrid g;
    g.m = static_cast<int>(d.size());
    g.d = d;
    g.logical_size = logical_size;
    g.dim.resize(static_cast<size_t>(g.m));
    int pieces_sum = 0;
    for (int i = 0; i < g.m; ++i) {
        if (d[static_cast<size_t>(i)] <= 0 || logical_size[static_cast<size_t>(i)] <= 0)
            throw std::runtime_error("non-positive d / logical_size");
        const int dimbit =
            static_cast<int>(std::ceil(std::log(d[static_cast<size_t>(i)]) / std::log(2.0)));
        const int chunkbit = static_cast<int>(
            std::ceil(std::log(logical_size[static_cast<size_t>(i)]) / std::log(2.0)));
        if (dimbit < chunkbit)
            throw std::runtime_error("logical_size larger than domain on a dimension");
        g.dim[static_cast<size_t>(i)].piecesbit = dimbit - chunkbit;
        g.dim[static_cast<size_t>(i)].powpieces = 1 << g.dim[static_cast<size_t>(i)].piecesbit;
        pieces_sum += g.dim[static_cast<size_t>(i)].piecesbit;
    }
    g.chunknum = 1 << pieces_sum;
    return g;
}

int tuple_chunk_id(const std::vector<int>& tup, const ChunkGrid& g) {
    int chunkid = tup[0] / g.logical_size[0];
    for (int i = 1; i < g.m; ++i) {
        chunkid = (chunkid << g.dim[static_cast<size_t>(i)].piecesbit) +
                  (tup[static_cast<size_t>(i)] / g.logical_size[static_cast<size_t>(i)]);
    }
    return chunkid;
}

std::vector<std::vector<int>> load_tuples(const std::string& path, int m) {
    std::ifstream fin(path);
    if (!fin)
        throw std::runtime_error("cannot open data: " + path);
    std::vector<std::vector<int>> rows;
    std::string line;
    while (std::getline(fin, line)) {
        if (line.empty())
            continue;
        std::istringstream iss(line);
        std::vector<int> t(static_cast<size_t>(m));
        for (int j = 0; j < m; ++j) {
            if (!(iss >> t[static_cast<size_t>(j)]))
                throw std::runtime_error("bad data line: " + line);
        }
        rows.push_back(std::move(t));
    }
    if (rows.empty())
        throw std::runtime_error("empty data file");
    return rows;
}

std::vector<std::vector<int>> load_queries(const std::string& path, int m) {
    std::ifstream fin(path);
    if (!fin)
        throw std::runtime_error("cannot open query: " + path);
    std::vector<std::vector<int>> qs;
    std::string line;
    while (std::getline(fin, line)) {
        if (line.empty())
            continue;
        std::istringstream iss(line);
        std::vector<int> q;
        int x;
        while (iss >> x)
            q.push_back(x);
        if (static_cast<int>(q.size()) != 2 * m)
            throw std::runtime_error("query width != 2*M: " + line);
        for (int j = 0; j < m; ++j) {
            if (q[static_cast<size_t>(2 * j)] > q[static_cast<size_t>(2 * j + 1)])
                throw std::runtime_error("query lo > hi: " + line);
        }
        qs.push_back(std::move(q));
    }
    if (qs.empty())
        throw std::runtime_error("empty query file");
    return qs;
}

class DimSnarf {
public:
    DimSnarf() = default;
    DimSnarf(const DimSnarf&) = delete;
    DimSnarf& operator=(const DimSnarf&) = delete;
    DimSnarf(DimSnarf&& o) noexcept
        : ok_(o.ok_), snarf_(std::move(o.snarf_)) {
        o.ok_ = false;
    }
    DimSnarf& operator=(DimSnarf&& o) noexcept {
        if (this != &o) {
            ok_ = o.ok_;
            snarf_ = std::move(o.snarf_);
            o.ok_ = false;
        }
        return *this;
    }

    void build(std::vector<uint64_t> keys, double bits_per_key, int batch_size) {
        ok_ = false;
        if (keys.empty())
            return;
        std::sort(keys.begin(), keys.end());
        keys.erase(std::unique(keys.begin(), keys.end()), keys.end());
        if (keys.empty())
            return;
        if (bits_per_key <= 3.0)
            throw std::runtime_error("SNARF requires bits_per_key > 3");
        // snarf_init takes non-const vector&
        snarf_ = snarf_updatable_gcs<uint64_t>();
        snarf_.snarf_init(keys, bits_per_key, batch_size);
        ok_ = true;
    }

    bool lookup_range_inclusive(uint64_t lo, uint64_t hi) const {
        if (!ok_)
            return false;
        return snarf_.range_query(lo, hi);
    }

    uint64_t memory_bytes() const {
        return ok_ ? static_cast<uint64_t>(snarf_.return_size()) : 0;
    }

    bool ok() const { return ok_; }

private:
    bool ok_ = false;
    snarf_updatable_gcs<uint64_t> snarf_;
};

struct ChunkStore {
    std::vector<std::vector<int>> tuples;
    std::vector<DimSnarf> snarf_per_dim;
    int best_dim = 0;
    bool nonempty = false;
};

int pick_best_dim(const std::vector<std::vector<int>>& tuples, int m, int fixed_dim) {
    if (fixed_dim >= 0) {
        if (fixed_dim >= m)
            throw std::runtime_error("best_dim out of range");
        return fixed_dim;
    }
    std::vector<std::unordered_set<int>> uniq(static_cast<size_t>(m));
    for (const auto& t : tuples) {
        for (int j = 0; j < m; ++j)
            uniq[static_cast<size_t>(j)].insert(t[static_cast<size_t>(j)]);
    }
    int best = 0;
    size_t best_n = 0;
    for (int j = 0; j < m; ++j) {
        const size_t n = uniq[static_cast<size_t>(j)].size();
        if (n > best_n) {
            best_n = n;
            best = j;
        }
    }
    return best;
}

void build_chunk_snarfs(ChunkStore& c, int m, double bits_per_key, int batch_size,
                        bool build_all_dims, int fixed_best_dim) {
    c.nonempty = !c.tuples.empty();
    c.snarf_per_dim.clear();
    c.snarf_per_dim.resize(static_cast<size_t>(m));
    if (!c.nonempty)
        return;
    c.best_dim = pick_best_dim(c.tuples, m, fixed_best_dim);
    auto build_dim = [&](int j) {
        std::vector<uint64_t> keys;
        keys.reserve(c.tuples.size());
        for (const auto& t : c.tuples)
            keys.push_back(static_cast<uint64_t>(t[static_cast<size_t>(j)]));
        c.snarf_per_dim[static_cast<size_t>(j)].build(std::move(keys), bits_per_key, batch_size);
    };
    if (build_all_dims) {
        for (int j = 0; j < m; ++j)
            build_dim(j);
    } else {
        build_dim(c.best_dim);
    }
}

bool chunk_has_hit(const ChunkStore& c, const std::vector<int>& query, int m) {
    for (const auto& t : c.tuples) {
        bool ok = true;
        for (int j = 0; j < m; ++j) {
            if (t[static_cast<size_t>(j)] < query[static_cast<size_t>(2 * j)] ||
                t[static_cast<size_t>(j)] > query[static_cast<size_t>(2 * j + 1)]) {
                ok = false;
                break;
            }
        }
        if (ok)
            return true;
    }
    return false;
}

enum class Mode { Best, And };

bool filter_border_keep(const ChunkStore& c, const std::vector<int>& query,
                        const std::vector<int>& coor, const ChunkGrid& g, Mode mode) {
    if (!c.nonempty)
        return false;
    auto clipped_abs = [&](int j) -> std::pair<int, int> {
        const int L = g.logical_size[static_cast<size_t>(j)];
        const int base = coor[static_cast<size_t>(j)] * L;
        const int lo = std::max(query[static_cast<size_t>(2 * j)], base);
        const int hi =
            std::min(query[static_cast<size_t>(2 * j + 1)], (coor[static_cast<size_t>(j)] + 1) * L - 1);
        return {lo, hi};
    };

    if (mode == Mode::Best) {
        const int j = c.best_dim;
        const auto [lo, hi] = clipped_abs(j);
        if (lo > hi)
            return false;
        if (!c.snarf_per_dim[static_cast<size_t>(j)].ok())
            return true;  // should not happen with NDEBUG; keep if missing
        return c.snarf_per_dim[static_cast<size_t>(j)].lookup_range_inclusive(
            static_cast<uint64_t>(lo), static_cast<uint64_t>(hi));
    }

    for (int j = 0; j < g.m; ++j) {
        const auto [lo, hi] = clipped_abs(j);
        if (lo > hi)
            return false;
        if (!c.snarf_per_dim[static_cast<size_t>(j)].ok())
            return true;
        if (!c.snarf_per_dim[static_cast<size_t>(j)].lookup_range_inclusive(
                static_cast<uint64_t>(lo), static_cast<uint64_t>(hi)))
            return false;
    }
    return true;
}

struct QueryStats {
    int overlap = 0;
    int border = 0;
    int nempty = 0;
    double ratio = 0;
    double filter_s = 0;
    double process_s = 0;
    double fpr = std::nan("");
};

QueryStats run_one_query(const std::vector<ChunkStore>& chunks, const ChunkGrid& g,
                         const std::vector<int>& query, Mode mode) {
    QueryStats st;
    std::vector<int> p1(static_cast<size_t>(g.m)), p2(static_cast<size_t>(g.m)),
        coor(static_cast<size_t>(g.m));
    int lowchunk = 0, highchunk = 0;
    for (int j = 0; j < g.m; ++j) {
        p1[static_cast<size_t>(j)] =
            query[static_cast<size_t>(2 * j)] / g.logical_size[static_cast<size_t>(j)];
        lowchunk = (lowchunk << g.dim[static_cast<size_t>(j)].piecesbit) + p1[static_cast<size_t>(j)];
        p2[static_cast<size_t>(j)] =
            query[static_cast<size_t>(2 * j + 1)] / g.logical_size[static_cast<size_t>(j)];
        highchunk =
            (highchunk << g.dim[static_cast<size_t>(j)].piecesbit) + p2[static_cast<size_t>(j)];
    }

    std::vector<int> chunklist;
    const auto t0 = Clock::now();
    for (int k = lowchunk; k <= highchunk; ++k) {
        int cid = k;
        int inrange = 1;
        int isborder = 0;
        for (int j = g.m - 1; j >= 0; --j) {
            coor[static_cast<size_t>(j)] = cid & (g.dim[static_cast<size_t>(j)].powpieces - 1);
            cid >>= g.dim[static_cast<size_t>(j)].piecesbit;
            if (coor[static_cast<size_t>(j)] < p1[static_cast<size_t>(j)] ||
                coor[static_cast<size_t>(j)] > p2[static_cast<size_t>(j)]) {
                inrange = 0;
                break;
            }
            if (coor[static_cast<size_t>(j)] == p1[static_cast<size_t>(j)] ||
                coor[static_cast<size_t>(j)] == p2[static_cast<size_t>(j)])
                isborder = 1;
        }
        if (!inrange)
            continue;
        st.overlap++;
        if (!isborder) {
            if (chunks[static_cast<size_t>(k)].nonempty)
                chunklist.push_back(k);
            continue;
        }
        st.border++;
        if (filter_border_keep(chunks[static_cast<size_t>(k)], query, coor, g, mode))
            chunklist.push_back(k);
    }
    st.filter_s = std::chrono::duration<double>(Clock::now() - t0).count();

    const auto t1 = Clock::now();
    for (int k : chunklist) {
        if (chunk_has_hit(chunks[static_cast<size_t>(k)], query, g.m))
            st.nempty++;
    }
    st.process_s = std::chrono::duration<double>(Clock::now() - t1).count();

    if (st.overlap > 0)
        st.ratio = static_cast<double>(st.nempty) / static_cast<double>(st.overlap);
    if (!chunklist.empty())
        st.fpr = static_cast<double>(static_cast<int>(chunklist.size()) - st.nempty) /
                 static_cast<double>(chunklist.size());
    return st;
}

uint64_t total_filter_bytes(const std::vector<ChunkStore>& chunks, Mode mode) {
    uint64_t sum = 0;
    for (const auto& c : chunks) {
        if (!c.nonempty)
            continue;
        if (mode == Mode::Best) {
            sum += c.snarf_per_dim[static_cast<size_t>(c.best_dim)].memory_bytes();
        } else {
            for (const auto& s : c.snarf_per_dim)
                sum += s.memory_bytes();
        }
    }
    return sum;
}

struct Agg {
    int n = 0;
    int nan_fpr = 0;
    double avg_fpr = 0;
    double median_fpr = 0;
    double avg_filter = 0;
    double avg_process = 0;
    uint64_t filter_bytes = 0;
};

Agg aggregate(const std::vector<QueryStats>& rows, uint64_t filter_bytes) {
    Agg a;
    a.n = static_cast<int>(rows.size());
    a.filter_bytes = filter_bytes;
    std::vector<double> fprs;
    fprs.reserve(rows.size());
    double sf = 0, sp = 0;
    for (const auto& r : rows) {
        sf += r.filter_s;
        sp += r.process_s;
        if (std::isfinite(r.fpr))
            fprs.push_back(r.fpr);
        else
            a.nan_fpr++;
    }
    a.avg_filter = sf / a.n;
    a.avg_process = sp / a.n;
    if (!fprs.empty()) {
        a.avg_fpr =
            std::accumulate(fprs.begin(), fprs.end(), 0.0) / static_cast<double>(fprs.size());
        std::sort(fprs.begin(), fprs.end());
        const size_t mid = fprs.size() / 2;
        a.median_fpr = (fprs.size() % 2) ? fprs[mid] : 0.5 * (fprs[mid - 1] + fprs[mid]);
    }
    return a;
}

void write_result(const std::string& path, const std::vector<QueryStats>& rows) {
    std::ofstream fout(path);
    if (!fout)
        throw std::runtime_error("cannot write " + path);
    for (const auto& r : rows) {
        fout << r.overlap << ' ' << r.border << ' ' << r.nempty << ' ' << r.ratio << ' '
             << r.filter_s << ' ' << r.process_s << ' ' << r.fpr << '\n';
    }
}

void append_summary_row(const std::string& path, const std::string& method,
                        const std::string& workload, const Agg& a, const std::string& notes) {
    const bool need_header = [&]() {
        std::ifstream in(path);
        return !in.good() || in.peek() == std::ifstream::traits_type::eof();
    }();
    std::ofstream fout(path, std::ios::app);
    if (!fout)
        throw std::runtime_error("cannot write " + path);
    if (need_header) {
        fout << "method,workload,n_queries,avg_fpr,median_fpr,avg_filter_time_s,avg_process_time_s,"
                "filter_bytes,notes\n";
    }
    fout << method << ',' << workload << ',' << a.n << ',' << a.avg_fpr << ',' << a.median_fpr << ','
         << a.avg_filter << ',' << a.avg_process << ',' << a.filter_bytes << ',' << notes << '\n';
}

std::string workload_name_from_path(const std::string& p) {
    std::string s = p;
    while (!s.empty() && (s.back() == '/' || s.back() == '\\'))
        s.pop_back();
    const auto slash = s.find_last_of("/\\");
    std::string leaf = (slash == std::string::npos) ? s : s.substr(slash + 1);
    if (leaf == "query.txt" && slash != std::string::npos) {
        const std::string parent = s.substr(0, slash);
        const auto slash2 = parent.find_last_of("/\\");
        return (slash2 == std::string::npos) ? parent : parent.substr(slash2 + 1);
    }
    return leaf;
}

std::vector<int> parse_int_list(const std::string& v) {
    std::vector<int> out;
    std::string cur;
    for (char ch : v) {
        if (ch == ',' || ch == ' ') {
            if (!cur.empty()) {
                out.push_back(std::stoi(cur));
                cur.clear();
            }
        } else {
            cur.push_back(ch);
        }
    }
    if (!cur.empty())
        out.push_back(std::stoi(cur));
    return out;
}

void usage() {
    std::cerr
        << "Usage: m4_snarf_bench [options]\n"
        << "  --data PATH\n"
        << "  --query PATH\n"
        << "  --out DIR\n"
        << "  --mode best|and|both   (default both)\n"
        << "  --bits-per-key F       (default 10; must be > 3)\n"
        << "  --batch-size N         (default 100)\n"
        << "  --d a,b,...\n"
        << "  --logical-size a,b,...\n"
        << "  --best-dim auto|N\n";
}

Config parse_args(int argc, char** argv) {
    Config c;
    for (int i = 1; i < argc; ++i) {
        const std::string a = argv[i];
        auto need = [&](const char* name) -> std::string {
            if (i + 1 >= argc)
                throw std::runtime_error(std::string("missing value for ") + name);
            return argv[++i];
        };
        if (a == "--data")
            c.data_path = need("--data");
        else if (a == "--query")
            c.query_path = need("--query");
        else if (a == "--out")
            c.out_dir = need("--out");
        else if (a == "--mode")
            c.mode = need("--mode");
        else if (a == "--bits-per-key")
            c.bits_per_key = std::stod(need("--bits-per-key"));
        else if (a == "--batch-size")
            c.batch_size = std::stoi(need("--batch-size"));
        else if (a == "--d") {
            c.d = parse_int_list(need("--d"));
            if (c.d.empty())
                throw std::runtime_error("--d must be non-empty");
        } else if (a == "--logical-size") {
            c.logical_size = parse_int_list(need("--logical-size"));
        } else if (a == "--best-dim") {
            const std::string v = need("--best-dim");
            if (v == "auto")
                c.best_dim = -1;
            else
                c.best_dim = std::stoi(v);
        } else if (a == "-h" || a == "--help") {
            usage();
            std::exit(0);
        } else {
            throw std::runtime_error("unknown arg: " + a);
        }
    }
    if (c.mode != "best" && c.mode != "and" && c.mode != "both")
        throw std::runtime_error("--mode must be best|and|both");
    if (c.logical_size.size() != c.d.size())
        throw std::runtime_error("--logical-size needs same length as --d");
    if (c.bits_per_key <= 3.0)
        throw std::runtime_error("--bits-per-key must be > 3");
    if (c.batch_size <= 0)
        throw std::runtime_error("--batch-size must be > 0");
    return c;
}

}  // namespace

int main(int argc, char** argv) {
    try {
        const Config cfg = parse_args(argc, argv);
        const ChunkGrid grid = make_grid(cfg.d, cfg.logical_size);
        std::cout << "[snarf_adapter] m=" << grid.m << " chunknum=" << grid.chunknum
                  << " bpk=" << cfg.bits_per_key << " batch=" << cfg.batch_size << '\n';
        std::cout << "[snarf_adapter] data=" << cfg.data_path << '\n';
        std::cout << "[snarf_adapter] query=" << cfg.query_path << '\n';
        std::cout << "[snarf_adapter] out=" << cfg.out_dir << '\n';

        if (std::system(("mkdir -p \"" + cfg.out_dir + "\"").c_str()) != 0)
            throw std::runtime_error("mkdir failed: " + cfg.out_dir);

        const auto tuples = load_tuples(cfg.data_path, grid.m);
        const auto queries = load_queries(cfg.query_path, grid.m);
        const std::string wl_name = workload_name_from_path(cfg.query_path);
        std::cout << "[snarf_adapter] tuples=" << tuples.size() << " queries=" << queries.size()
                  << " workload=" << wl_name << '\n';

        for (const auto& t : tuples) {
            for (int j = 0; j < grid.m; ++j) {
                if (t[static_cast<size_t>(j)] < 0 ||
                    t[static_cast<size_t>(j)] >= cfg.d[static_cast<size_t>(j)])
                    throw std::runtime_error("tuple out of domain");
            }
        }

        std::vector<ChunkStore> chunks(static_cast<size_t>(grid.chunknum));
        for (const auto& t : tuples) {
            const int id = tuple_chunk_id(t, grid);
            if (id < 0 || id >= grid.chunknum)
                throw std::runtime_error("chunk id out of range");
            chunks[static_cast<size_t>(id)].tuples.push_back(t);
        }

        int nonempty = 0;
        for (const auto& c : chunks)
            if (!c.tuples.empty())
                ++nonempty;
        std::cout << "[snarf_adapter] nonempty_chunks=" << nonempty << '\n';

        const bool need_best = (cfg.mode == "best" || cfg.mode == "both");
        const bool need_and = (cfg.mode == "and" || cfg.mode == "both");
        const bool build_all_dims = need_and;

        int built = 0;
        for (auto& c : chunks) {
            if (c.tuples.empty())
                continue;
            build_chunk_snarfs(c, grid.m, cfg.bits_per_key, cfg.batch_size, build_all_dims,
                               cfg.best_dim);
            ++built;
        }
        std::cout << "[snarf_adapter] built_filters_on_chunks=" << built
                  << " all_dims=" << (build_all_dims ? 1 : 0) << '\n';

        auto run_mode = [&](Mode mode, const std::string& method, const std::string& result_name) {
            std::vector<QueryStats> rows;
            rows.reserve(queries.size());
            for (size_t qi = 0; qi < queries.size(); ++qi) {
                rows.push_back(run_one_query(chunks, grid, queries[qi], mode));
                if ((qi + 1) % 20 == 0 || qi + 1 == queries.size()) {
                    std::cout << "[snarf_adapter] " << method << " queries " << (qi + 1) << "/"
                              << queries.size() << '\n';
                }
            }
            const uint64_t bytes = total_filter_bytes(chunks, mode);
            const Agg agg = aggregate(rows, bytes);
            const std::string result_path = cfg.out_dir + "/" + result_name;
            write_result(result_path, rows);
            std::ostringstream notes;
            notes << "bpk=" << cfg.bits_per_key << ";batch=" << cfg.batch_size
                  << ";nan_fpr=" << agg.nan_fpr << ";NDEBUG_for_Ngt10000_assert";
            if (mode == Mode::Best)
                notes << ";best_dim=" << (cfg.best_dim < 0 ? "auto" : std::to_string(cfg.best_dim));
            append_summary_row(cfg.out_dir + "/summary.csv", method, wl_name, agg, notes.str());
            std::cout << "[snarf_adapter] wrote " << result_path << " avg_fpr=" << agg.avg_fpr
                      << " median_fpr=" << agg.median_fpr << " filter_bytes=" << agg.filter_bytes
                      << '\n';
        };

        {
            std::ofstream wipe(cfg.out_dir + "/summary.csv", std::ios::trunc);
        }

        if (need_and)
            run_mode(Mode::And, "snarf_1d_and", "result_and.txt");
        if (need_best)
            run_mode(Mode::Best, "snarf_1d_best", "result_best.txt");

        std::cout << "[snarf_adapter] done\n";
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "[snarf_adapter] ERROR: " << e.what() << '\n';
        return 1;
    }
}
