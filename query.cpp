#include "common.h"

int compare_Twotuples(vector<int> a, vector<int> b) {
    int i;
    int same = 0;
    for (i = 0; i < (int)a.size(); i++) {
        if (a[i] + 1 == b[i]) {
            same++;
            continue;
        }
        if (a[i] != b[i])
            return 0;
    }
    if (same == 1)
        return 2;
    else if (same > 1)
        return 0;
    return 1;
}

void strmncpy(char* s, int start1, int len, char* t, int start2) {
    for (int i = 0; i < len; i++)
        t[start2 + i] = s[start1 + i];
}

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
