#pragma once
#include <bits/stdc++.h>
#include <random>
#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <inttypes.h>
#include <assert.h>
#include <errno.h>
#include <semaphore.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>
#include <queue>
#include <cstdio>//
#include <limits.h>
#include <iomanip>
#include <stdexcept>//
#include "BlockManager.h"


using namespace std;
#define BYTE 8
#define PAGESIZE 4096
#define SPACE_CHAR ' '
#define TAB_CHAR ' '
#define SPLIT_CHAR ' '
#define BRANCH 5
#define PI 3.14159

#define GETMASK(index, size) (((1 << (size)) - 1) << (index))
#define READFROM(data, index, size) (((data) & GETMASK((index), (size))) >> (index))
#define WRITETO(data, index, size, value) ((data) = ((data) & (~GETMASK((index), (size)))) | ((value) << (index)))
#define FIELD(data, name, index, size) \
  inline decltype(data) name() { return READFROM(data, index, size); } \
  inline void set_##name(decltype(data) value) { WRITETO(data, index, size, value); }

extern int M;//dimension number
extern int N;//data set size
extern int n;//sample size
extern int dtype;
extern int independent;
extern vector<int> d;//cardinality
extern vector<int> dbit;//the bit number of cardinality
extern int dbit_sum;
extern int page_capacity;//the tuple number in a page
extern int subset_num;
extern string depend_pattern;
extern int bucket_num;
extern int B;
extern vector<int> logical_size0;
extern vector<int> logical_size;
extern int lnum_max;
extern int batch;
extern vector<int> shape;
extern vector<int> empty_tuple;
typedef pair<double, double> point;
typedef vector<point> points;
static const int CHILDS = 3;
static const double INF = 999999999;

extern int bitsperkey;
extern BlockManager* block1;
extern int fcurpageid;
extern char* sdata1;
extern int beginbyte1;///write the filter file
extern vector<vector<int>> filter_offset;
extern vector<string> filters;
extern int last_validchunk;



void read_Cardinality(const char* datafolder);
int compare_Twotuples(vector<int> a, vector<int> b);
void splitString(const string& s, vector<string>& tokens, char delim);
void loadQuery(const char* querypath, vector<vector<int>> &qarray);
void strmncpy(char* s, int start1, int len, char* t, int start2);

