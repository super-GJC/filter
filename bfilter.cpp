#include "bfilter.h"



int Bfilter::construct_Bloomfilter(int chunkid, vector<uint64_t> ranges){
    int i, j;
    uint32_t p;
    bitmap = new char[bytenum];
    bitmap[0] = '\0';
    for(i = 0; i < bytenum; i++) bitmap[i] = 0;
    for(i = 0; i < ranges.size(); i++){
        for(j = 0; j < k; j++){
            const char* c = to_string(ranges[i]).c_str();
            const uint8_t* data = (const uint8_t*)c;
            p = MurmurHash3_x86_32(data, strlen(c), j*10);
            beginbyte = p % bitnum / BYTE;
            beginbit = p % bitnum % BYTE;
            bitmap[beginbyte] = bitmap[beginbyte] | (1 << (7-beginbit));
        }
    }
    write_Bloomfilter(chunkid);

    return bytenum;
}
///------------------------------------------------------------------------------------------------------------------------------------------
void Bfilter::write_Bloomfilter(int chunkid){
    int offset = 0, offset1;
    /*
    变量含义和使用bitmap过滤时相同
    offset1:在第一个可以写入的页中写入的字节数
    offset：对于当前处理的数据块chunkid，对应的bitmap已经写入的字节数
    */
    filter_offset[chunkid][0] = 1;///1 represents using bloom filter
    filter_offset[chunkid][1] = fcurpageid;
    filter_offset[chunkid][2] = beginbyte1;
    if(PAGESIZE - beginbyte1 > bytenum){
        strmncpy(bitmap, 0, bytenum, sdata1, beginbyte1);
        beginbyte1 += bytenum;
    }
    else{
        strmncpy(bitmap, 0, PAGESIZE - beginbyte1, sdata1, beginbyte1);
        block1->WriteBlock(sdata1, fcurpageid++, PAGESIZE);
        beginbyte1 = 0;
        sdata1[0] = '\0';
        offset += (PAGESIZE - beginbyte1);
        offset1 = offset;
        for(int k = 0; k < (bytenum - offset1) / PAGESIZE; k++){
            strmncpy(bitmap, offset, PAGESIZE, sdata1, beginbyte1);
            block1->WriteBlock(sdata1, fcurpageid++, PAGESIZE);
            beginbyte1 = 0;
            sdata1[0] = '\0';
            offset += PAGESIZE;
        }
        if(offset!=bytenum){
            strmncpy(bitmap, offset, bytenum-offset, sdata1, beginbyte1);
            beginbyte1 = bytenum-offset;
        }
    }
    filter_offset[chunkid][3] = fcurpageid;
    filter_offset[chunkid][4] = beginbyte1;
    return;
}
///------------------------------------------------------------------------------------------------------------------------------------------
int Bfilter::search_Bloomfilter(string sbitmap, uint64_t srange){
    cout << "sbitmap的长度:" << sbitmap.length() << " 字节" << endl;
    int i, j;
    uint32_t p, u;
    const char* c = to_string(srange).c_str();
    const uint8_t* data = (const uint8_t*)c;
    for(j = 0; j < k; j++){
        p = MurmurHash3_x86_32(data, strlen(c), j*10);
        beginbyte = p % bitnum / BYTE;
        beginbit = p % bitnum % BYTE;
        ///-----------------------------///
        u = sbitmap[beginbyte];
        if(((u >> (7-beginbit)) & 1) != 1) return 0;
    }
    return 1;
}

///------------------------------------------------------------------------------------------------------------------------------------------
uint32_t Bfilter::MurmurHash3_x86_32(const uint8_t* key, int len, uint32_t seed) {
    const uint8_t* data = (const uint8_t*)key;
    const int nblocks = len / 4;

    uint32_t h1 = seed;

    const uint32_t c1 = 0xcc9e2d51;
    const uint32_t c2 = 0x1b873593;

    const uint32_t* blocks = (const uint32_t*)(data + nblocks * 4);
    for (int i = -nblocks; i; i++) {
        uint32_t k1 = blocks[i];
        k1 *= c1;
        k1 = ROTL32(k1, 15);
        k1 *= c2;

        h1 ^= k1;
        h1 = ROTL32(h1, 13);
        h1 = h1 * 5 + 0xe6546b64;
    }

    const uint8_t* tail = (const uint8_t*)(data + nblocks * 4);

    uint32_t k1 = 0;

    switch (len & 3) {
    case 3:
        k1 ^= tail[2] << 16;
    case 2:
        k1 ^= tail[1] << 8;
    case 1:
        k1 ^= tail[0];
        k1 *= c1;
        k1 = ROTL32(k1, 15);
        k1 *= c2;
        h1 ^= k1;
    }
    h1 ^= len;
    h1 = fmix32(h1);

    return h1;
}
///------------------------------------------------------------------------------------------------------------------------------------------
inline uint32_t Bfilter::ROTL32 ( uint32_t x, int8_t r )
{
  return (x << r) | (x >> (32 - r));
}
///------------------------------------------------------------------------------------------------------------------------------------------
uint32_t Bfilter::fmix32 ( uint32_t h )
{
    h ^= h >> 16;
    h *= 0x85ebca6b;
    h ^= h >> 13;
    h *= 0xc2b2ae35;
    h ^= h >> 16;
    return h;
}
