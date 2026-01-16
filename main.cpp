#include "common.h"
#include "bfilter.h"
#include "rfilter.h"


int subset_num;
int n ;
int N ;//data set size
int M = 3 ;//dimension number  M 与 RFilter类的属性 m 有区别吗？ 
int B ;
int bucket_num ;
int independent = 1 ;//1:independent, 0:dependent 这是维度排序算法中是否独立的标记变量吗？
int dtype = 1 ;
vector<int> d = {16,16};
vector<int> dbit ; // dbit[i] 计算公式与 dimbit[i] 相同。即多维数组第i维的大小为pow(2,dbit[i])
int dbit_sum ; //整个多维数组的逻辑大小(单元格数)为 pow(2,dbit_sum)
int page_capacity ;//the tuple number in a page
vector<int> logical_size = {8,8};///2^a
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

    string dataf = "./data/test/";
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

    string resultpath2 = dataf + "result2.txt";
    const char* resultPath2 = resultpath2.c_str();

    string outputpath = dataf + "output.txt";
    const char* outputPath = outputpath.c_str();



    Rfilter* rf = new Rfilter();
    rf->construct_Rangefilter(dataPath, binaryPath1, filterPath, offsetPath);
    rf->process_Queries(binaryPath1,queryPath,offsetPath,filterPath,resultPath1);


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
int compare_Twotuples(vector<int> a, vector<int> b){
    int i;
    int same = 0;
    for(i = 0; i < a.size(); i++){
        if(a[i]+1==b[i]) {same++;continue;}///the two tuples are continous
        if(a[i]!=b[i]) return 0;
    }
    if(same == 1) return 2;
    else if(same > 1) return 0;
    return 1;///the two tuples are the same
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
void loadQuery(const char* querypath, vector<vector<int>> &qarray) {
	ifstream fin(querypath);
	string line;
	if (fin)
	{
		while (getline(fin, line))
		{
			istringstream iss(line);
			string temp;
			vector<int> sv;
			while (getline(iss, temp, ' ')) {
				sv.push_back(stoi(temp));
			}
			qarray.push_back(sv);
		}
	}
	fin.clear();
	fin.close();
}
///------------------------------------------------------------------------------------------------//
void strmncpy(char* s, int start1, int len, char* t, int start2){
    for(int i = 0; i < len; i++)
        t[start2+i] = s[start1+i];
    return;
}
