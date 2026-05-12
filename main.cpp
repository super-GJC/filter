#include "common.h"
#include "bfilter.h"
#include "rfilter.h"
#include "workloadAnalyzer.h"

#include <fstream>
#include <sstream>
#include <unordered_set>


int subset_num;
int n ;
int N ;//data set size
int M = 4 ;//dimension number  M 与 RFilter类的属性 m 有区别吗？ 
int B ;
int bucket_num ;
int independent = 1 ;//1:independent, 0:dependent 这是维度排序算法中是否独立的标记变量吗？
int dtype = 1 ;
vector<int> d = {4,1121,68,7};
vector<int> dbit ; // dbit[i] 计算公式与 dimbit[i] 相同。即多维数组第i维的大小为pow(2,dbit[i])
int dbit_sum ; //整个多维数组的逻辑大小(单元格数)为 pow(2,dbit_sum)
int page_capacity ;//the tuple number in a page
vector<int> logical_size = {2,16,16,4};///2^a
vector<int> logical_size0;
int lnum_max ;
int batch ;
vector<int> empty_tuple;
vector<int> shape;

int bitsperkey=20;
BlockManager* block1;
int fcurpageid;
char* sdata1;
int beginbyte1;///write the filter file
vector<vector<int>> filter_offset;
int last_validchunk;///the last non-empty chunk

/// 逐行比较两次 process_Queries 输出：列 0–3 与 6（FPR）须一致；列 4–5 为计时允许不同
static bool compare_query_results_semantic(const string& path_a, const string& path_b) {
    ifstream fa(path_a), fb(path_b);
    string la, lb;
    while (getline(fa, la) && getline(fb, lb)) {
        istringstream ia(la), ib(lb);
        vector<string> ta, tb;
        string w;
        while (ia >> w)
            ta.push_back(w);
        while (ib >> w)
            tb.push_back(w);
        if (ta.size() < 7 || tb.size() < 7)
            return false;
        for (int c = 0; c < 4; c++)
            if (ta[c] != tb[c])
                return false;
        if (ta[6] != tb[6])
            return false;
    }
    if (getline(fa, la) || getline(fb, lb))
        return false;
    return true;
}

static streamoff file_size_bytes(const string& path) {
    ifstream f(path, ios::binary | ios::ate);
    if (!f)
        return -1;
    return f.tellg();
}

/// 非空块中已在 filter.txt 物化过滤器的数量（filter_offset[i][0] 为 0 位图或 1 布隆；2 表示未建）
static int count_materialized_filter_chunks(const Rfilter* rf) {
    int n = 0;
    for (int i = 0; i < rf->chunknum; i++) {
        if (rf->chunksize[i] == 0)
            continue;
        int t = filter_offset[i][0];
        if (t == 0 || t == 1)
            n++;
    }
    return n;
}

/**
 * 基线四参数 construct_Rangefilter 与六参数工作负载版对比测试：
 * 负载分析 → 基线构建与查询 → 剔除空 border id → 工作负载构建与查询 → 结果语义比对与 filter 体积比对。
 */
void run_construct_rangefilter_compare_test(Rfilter* rf, const char* dataPath, const char* binaryPath,
                                            const char* queryPath, const string& filterpath,
                                            const string& offsetpath, const string& resultpath1,
                                            const string& filter_workload, const string& offset_workload,
                                            const string& result_workload) {
    int i;
    vector<int> border_chunk_ids = collectBorderChunkIdsFromQueries(*rf, queryPath);
    unordered_set<int> border_chunk_set(border_chunk_ids.begin(), border_chunk_ids.end());

    cout << "[workload analyzer] border chunk count: " << border_chunk_ids.size() << endl;
    cout << "[workload analyzer] border chunk ids:";
    for (i = 0; i < (int)border_chunk_ids.size(); i++) {
        cout << " " << border_chunk_ids[i];
    }
    cout << endl;

    rf->construct_Rangefilter(dataPath, binaryPath, filterpath.c_str(), offsetpath.c_str());
    const int baseline_materialized_filters = count_materialized_filter_chunks(rf);
    rf->process_Queries(binaryPath, queryPath, offsetpath.c_str(), filterpath.c_str(), resultpath1.c_str());

    for (auto it = border_chunk_set.begin(); it != border_chunk_set.end();) {
        if (rf->chunksize[*it] == 0)
            it = border_chunk_set.erase(it);
        else
            ++it;
    }

    rf->construct_Rangefilter(dataPath, binaryPath, filter_workload.c_str(), offset_workload.c_str(), true,
                              border_chunk_set);
    const int workload_materialized_filters = count_materialized_filter_chunks(rf);

    // 校验 append_cursor.txt：filter_workload 已写字节数应等于 fcurpageid*PAGESIZE+beginbyte1（下一字节逻辑偏移）
    {
        size_t slash = offset_workload.find_last_of("/\\");
        string append_cursor_path = (slash == string::npos)
                                          ? string("append_cursor.txt")
                                          : offset_workload.substr(0, slash + 1) + "append_cursor.txt";
        ifstream ac(append_cursor_path);
        long long cur_page = -1, cur_byte = -1;
        if (ac >> cur_page >> cur_byte) {
            long long sz = (long long)file_size_bytes(filter_workload);
            long long expected_next = cur_page * (long long)PAGESIZE + cur_byte;
            if (sz == expected_next) {
                cout << "[append_cursor] OK: filter file size == next write offset (" << sz << " bytes)" << endl;
            } else {
                cerr << "[append_cursor] FAIL: file_size=" << sz << " expected fcur*PAGESIZE+begin=" << expected_next
                     << " (page=" << cur_page << " beginbyte1=" << cur_byte << ")" << endl;
            }
        } else {
            cerr << "[append_cursor] FAIL: could not read " << append_cursor_path << endl;
        }
    }

    rf->process_Queries(binaryPath, queryPath, offset_workload.c_str(), filter_workload.c_str(),
                        result_workload.c_str());

    if (!compare_query_results_semantic(resultpath1, result_workload)) {
        cerr << "[compare] result1 vs result_workload: MISMATCH (overlap/border/nempty/ratio/FPR)" << endl;
    } else {
        cout << "[compare] result1 vs result_workload: OK (same stats & FPR; times may differ)" << endl;
    }

    streamoff sz_filter = file_size_bytes(filterpath);
    streamoff sz_filter_wl = file_size_bytes(filter_workload);
    cout << "[compare] filter.txt size (bytes): " << sz_filter << endl;
    cout << "[compare] filter_workload.txt size (bytes): " << sz_filter_wl << endl;
    if (sz_filter > 0 && sz_filter_wl > 0 && sz_filter_wl < sz_filter) {
        cout << "[compare] workload filter file is smaller than baseline (expected)" << endl;
    }

    cout << "[compare] materialized filter chunks (baseline, no workload): " << baseline_materialized_filters
         << endl;
    cout << "[compare] materialized filter chunks (workload-aware build): " << workload_materialized_filters
         << endl;
    cout << "[compare] non-empty chunks total (cknum): " << rf->cknum << endl;
}


int main()
{
    int i;
    sdata1 = new char[PAGESIZE];
    for(i = 0; i < d.size(); i++){
        logical_size0.push_back((int)pow(ceil(sqrt(logical_size[i])), 2));
    }
        dbit_sum = 0;
    for(i = 0; i < d.size(); i++){
        double a = log(d[i]) / log(2);
        dbit.push_back((int)ceil(a));
        dbit_sum += dbit[dbit.size()-1];
    }
    page_capacity = PAGESIZE * BYTE / dbit_sum;
    for(i = 0; i < d.size(); i++){
        empty_tuple.push_back(0); // 问题：使用全部维度都为0的元组作为empty_tuple，是否会与各个维度都为各自值域最小值的元组冲突？
    }

    string dataf = "./data/test/climate/";
    const char* dataFolder = dataf.c_str();
    string datapath = dataf + "data.txt";
    const char* dataPath = datapath.c_str();

    string querypath = dataf + "query.txt";
    const char* queryPath = querypath.c_str();

    string pointquerypath = dataf + "pointquery.txt";
    const char* pointqueryPath = pointquerypath.c_str();

    string binarypath1 = dataf + "binary1.txt";
    const char* binaryPath1 = binarypath1.c_str();

    string offsetpath = dataf + "offset.txt";
    const char* offsetPath = offsetpath.c_str();

    string filterpath = dataf + "filter.txt";
    const char* filterPath = filterpath.c_str();

    string resultpath1 = dataf + "result1.txt";
    const char* resultPath1 = resultpath1.c_str();

    // 工作负载构建与基线对比输出（与 query.txt 对应）；基线仍写入 result1.txt
    string result_workload = dataf + "result_workload.txt";
    string offset_workload = dataf + "offset_workload.txt";
    string filter_workload = dataf + "filter_workload.txt";

    string resultpath2 = dataf + "result2.txt";
    const char* resultPath2 = resultpath2.c_str();

    string outputpath = dataf + "output.txt";
    const char* outputPath = outputpath.c_str();



    Rfilter* rf = new Rfilter();
    // rf->construct_Rangefilter(dataPath, binaryPath1, filterpath, offsetpath);
    // rf->process_Queries(binaryPath1, queryPath, offsetpath, filterpath, resultpath1);

    run_construct_rangefilter_compare_test(rf, dataPath, binaryPath1, queryPath, filterpath, offsetpath,
                                         resultpath1, filter_workload, offset_workload, result_workload);

    cout << "Ends!" << endl;
    return 0;
}


///------------------------------------------------------------------------------------------------------------------------------------------
void read_Cardinality(const char* datafolder){
    string data_folder(datafolder);
    string scardpath = data_folder + "cardinality.txt";
    const char* cardpath = scardpath.c_str();
    ifstream fin(cardpath);
	string line;
	d.clear();
	dbit.clear();
	dbit_sum=0;
	if (fin)
	{
		while (getline(fin, line))
		{
			istringstream iss(line);
			string temp;
			while (getline(iss, temp, SPACE_CHAR)) {
                d.push_back(stoi(temp));
                double a = log(stof(temp)) / log(2);
                dbit.push_back((int)ceil(a));
                dbit_sum += dbit[dbit.size()-1];
			}
        }
    }
    fin.clear();
    fin.close();
    page_capacity = PAGESIZE * BYTE / dbit_sum;
    cout<<dbit_sum<<endl;
    cout<<page_capacity<<endl;
    return;
}
///------------------------------------------------------------------------------------------------------------------------------------------
void splitString(const string& s, vector<string>& tokens, char delim) {
	tokens.clear();
	auto string_find_first_not = [s, delim](size_t pos = 0) -> size_t {
		for (size_t i = pos; i < s.size(); i++) {
			if (s[i] != delim) return i;
		}
		return string::npos;
	};
	size_t lastPos = string_find_first_not(0);
	size_t pos = s.find(delim, lastPos);
	while (lastPos != string::npos) {
		tokens.emplace_back(s.substr(lastPos, pos - lastPos));
		lastPos = string_find_first_not(pos);
		pos = s.find(delim, lastPos);
	}
	return;
}
///------------------------------------------------------------------------------------------------//
