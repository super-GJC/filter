#pragma once
#include "common.h"
#include "BlockManager.h"
#include "Timer.h"
#include <cmath>

class Bfilter
{
public:
    int m;
    int beginbyte, beginbit;
    unsigned char c;
    int k;///the hash function number
    int bytenum;
    int bitnum;
    int bpk;
    char* bitmap;

    Bfilter(int num){///b is the bits per key, num is key number (should be written to file).
        int i, j;
        m = d.size();
        bitnum = num * bitsperkey;
//        bpk = b;
        beginbyte = 0;
        beginbit = 0;
        c = 0;
        k = (int)ceil(log(2) * bitsperkey); //cout<<"k="<<k<<endl;
        bytenum = (int)ceil((double)bitnum / BYTE); //cout<<"bytenum="<<bytenum<<endl;

    }


    int construct_Bloomfilter(int chunkid, vector<uint64_t> ranges);
    int search_Bloomfilter(string sbitmap, uint64_t srange);
    uint32_t MurmurHash3_x86_32(const uint8_t* key, int len, uint32_t seed);
    inline uint32_t ROTL32 ( uint32_t x, int8_t r );
    uint32_t fmix32 ( uint32_t h );
    void write_Bloomfilter(int chunkid);

};


