#pragma once
#include "common.h"
#include "BlockManager.h"
#include "Timer.h"
#include "bfilter.h"
#include <unordered_map>



class Rfilter
{
public:

    int m; //猜测 m 是维数，之后要在全部维度中定义指标来选择
    vector<int> dimbit;///the bits of cardinality on each dimension
    vector<int> chunkbit;///the bits of chunk length (logical_size) on each dimension
    vector<int> piecesbit;///the bits of chunk number on each dimension.对于第i维，该维度分成块的数量为pow(2, piecesbit[i])
    vector<int> powpieces;//powpieces[i]:对于维度i，该维度分成块的数量。 powpieces[i] = pow(2, piecesbit[i])
    vector<int> intervallen;///the length of partition interval on each dim
    vector<int> intervalbit;
    int chunknum;///total chunk number
    int cknum = 0;///non-empty chunk number
    int chunk_logsize;///fixed logical size
    vector<int> chunksize;///tuple number (pysical size) of each chunk
    vector<int> page_startid;
    vector<int> page_endid;
    char* sdata;///write the data file

    int bitmapbytes;///the byte number of bitmap on each chunk
    char* rfbitmap;///the bitmap filter of each chunk
    vector<vector<vector<int>>> oneDrange;///all the possible one-dimensional ranges for each value on each dim
    vector<int> oneDrange_bit;///the bit of one-dimensional range number on each dim
    int oneDrangebits;///the sum of oneDrange_bit
    uint64_t allmranges;///the number of all possible multi-dimensional ranges
    vector<vector<vector<uint16_t>>> mulDrange;
    vector<string> filters;

    //key:使用bloom filter保存过滤器结构的chunkid;
    //value:该数据块创建bloom filter时传入num参数的大小(即该数据块的rangeids.size())
    unordered_map<int, uint64_t> bloomFilterMap;





    Rfilter(){
        int i;
        int j = 0, k = 0;
        m = d.size();
        dimbit.resize(m);
        chunkbit.resize(m);
        piecesbit.resize(m);
        powpieces.resize(m);
        intervallen.resize(m);
        intervalbit.resize(m);
        oneDrange.resize(m);
        oneDrange_bit.resize(m);
        oneDrangebits = 0;
        sdata = new char[PAGESIZE];
        for(i = 0; i < m; i++){
            dimbit[i] = (int)ceil(log(d[i]) / log(2)); //dimbit[i]:多维数组第i维的大小为pow(2,dimbit[i])。 假设数组大小为8*16，则d[0]=8,d[1]=16, dimbit[0]=3,dimbit[1]=4
            chunkbit[i] = (int)ceil(log(logical_size[i]) / log(2)); //chunkbit[i]:多维数组第i维的逻辑大小(单元格数)为pow(2,chunkbit[i])。 假设逻辑分块大小为2*4，则logical_size[0]=2,logical_size[1]=4, chunkbit[0]=1,chunkbit[1]=2
            k += chunkbit[i];
            piecesbit[i] = dimbit[i] - chunkbit[i];
            j += piecesbit[i];
            powpieces[i] = pow(2,piecesbit[i]); //powpieces[i]:对于维度i，该维度分成块的数量。 powpieces[i] = pow(2, piecesbit[i])
            intervallen[i] = (int)ceil(sqrt(logical_size[i]));
            intervalbit[i] = (int)ceil(log(intervallen[i]) / log(2));
        }
        chunknum = pow(2, j); //chunknum:数据块总数。chunknum = pow(2, sum(piecesbit[i]))
        chunk_logsize = pow(2, k); //chunk_logsize:一个数据块的逻辑大小，即单元格数。chunk_logsize = pow(2, sum(chunkbit[i]))
        mulDrange.resize(chunk_logsize);
        chunksize.resize(chunknum, 0);
        page_startid.resize(chunknum, -1);
        page_endid.resize(chunknum, -1);
        filter_offset.resize(chunknum);
        filters.resize(chunknum);
        for(i = 0; i < chunknum; i++) filter_offset[i].resize(5);
        for(i = 0; i < m; i++){
            /*
            总共的一维范围数量：
            紫色三角形：logical_size0[i] - intervallen[i] -1
            蓝色正方形：logical_size0[i] - intervallen[i] -1
            绿色圆形：logical_size0[i]
            红色菱形：1 + 2 + ... + (logical_size0[i] / intervallen[i]) = 等差数列求和 
                                                                       = (intervallen[i] + logical_size0[i]) * logical_size0[i] / (intervallen[i] ** 2) * 2
                                                                       = (intervallen[i] + logical_size0[i]) / 2
                                                                       = 0.5 * intervallen[i] + 0.5 * logical_size0[i]
            */
           //对于维度i候选的CRScⁱ集合的大小为上述四个数的累加，即3.5 * logical_size0[i] - 1.5 * intervallen[i] - 2
            oneDrange_bit[i] = ceil(log(3.5 * logical_size0[i] - 1.5 * intervallen[i] - 2) / log(2));
            oneDrangebits += oneDrange_bit[i];
        }
        allmranges = pow(2, oneDrangebits); //所有多维范围的排序数范围为[0,allmranges-1]
    }



    void transfer_Txt_ToBinaryfile(const char* datapath, const char* binarypath);
    void transfer_Tuple_ToBinary(vector<int> tup, unsigned char &c, int &beginbyte, int &beginbit);
    void read_Page(BlockManager* block, int pid, vector<vector<int>> &tuples);
    void read_Offsets(const char* offsetpath);

    void construct_Rangefilter(const char* datapath, const char* binarypath, const char* filterpath, const char* offsetpath);
    void compute_Total1Drange();///one-dimensional ranges
    void compute_Rangeset(int chunkid, vector<vector<int>> alltuples, vector<uint64_t> &rangeIDs);///multi-dimensional ranges, build bitmap as filter
    void write_RFbitmap(int chunkid);

    void read_Filters(const char* offsetpath, const char* filterpath);///read the filters from file
    int search_RFbitmap(int chunkid, uint64_t arange);///Identify whether a range is empty on rfbitmap
    void get_MulRanges4query(vector<int> aquery, vector<uint64_t> &mranges);
    void process_Queries(const char* binarypath, const char* querypath, const char* offsetpath, const char* filterpath, const char* resultpath);



};

