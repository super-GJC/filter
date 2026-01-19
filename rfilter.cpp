#include "rfilter.h"



///--------------------------------------------------------------------------------------------------------------------
// TODO:体会 piecesbit[i] 和 logical_size[i] 的含义
// GUESS: piecesbit[i]的含义：对于第i维，该维度分成块的数量为pow(2, piecesbit[i])
//        logical_size[i]的含义：分块时第i维的逻辑大小为logical_size[i]。注意到logical_size[i]没有初始化代码，只能由人为手动给出？
void Rfilter::transfer_Txt_ToBinaryfile(const char* datapath, const char* binarypath){
    int i, j, g;
    ///read data, the first time
    //第一轮读取数据文件，是为了进行统计分块。计算出每个块有多少元组，每个块从哪一页开始到哪一页结束
    string line;
    ifstream fin(datapath);
    if (fin)
    {
        while (getline(fin, line))
        {
            istringstream iss(line);
            string temp; // 从文件读取的字符串形式的属性值
            vector<int> tup; //一行文件输入代表的元组
            while (getline(iss, temp, TAB_CHAR)) {
                tup.push_back(stoi(temp)); //每次读取一个属性值，转换为整数，作为元组的一个维度值
            }
            //计算出新读取的这个元组属于哪个数据块chunk
            int chunkid = 0;
            chunkid = tup[0] / logical_size[0];
            for(i = 1; i < m; i++){
                chunkid = chunkid << piecesbit[i];
                chunkid += tup[i] / logical_size[i];
            }
            chunksize[chunkid]++; //数据块中元组数计数++
        }
    }
    fin.clear();
    fin.close();
    int former = 0; //former:下一个页保存的第一个page的id
    cknum = 0; // cknum:非空数据块数量
    //计算数据块i包含的元组从哪个页开始保存page_startid[i]，到哪个页结束保存page_endid[i]。相当于可以以页为单位访问到数据块中的元组
    for(i = 0; i < chunknum; i++){
        if(chunksize[i] == 0) continue;
        page_startid[i] = former;
        former += ceil((double)chunksize[i] / page_capacity);
        page_endid[i] = former - 1;
        cknum++;
        last_validchunk = i;
    }

    ///read data the second time, and write the chunks
    //第二轮读取数据文件，是为了将具体的元组写入到对应的数据块chunk中去
    vector<vector<int>> buffer[chunknum];
    int beginbyte = 0, beginbit = 0;
    unsigned char c = 0;
    sdata[0] = '\0';
    vector<int> page_currentid; // page_currentid[i]表示数据块i当前写的页的page_id
    page_currentid = page_startid; //初始化为每个数据块的起始页id
    BlockManager* block = new BlockManager(binarypath, O_CREAT, PAGESIZE);
    ifstream fin1(datapath);
    if (fin1)
    {
        while (getline(fin1, line))
        {
            istringstream iss(line);
            string temp;
            vector<int> tup;
            while (getline(iss, temp, TAB_CHAR)) {
                tup.push_back(stoi(temp));
            }
            int chunkid = 0;
            chunkid = tup[0] / logical_size[0];
            for(i = 1; i < m; i++){
                chunkid = chunkid << piecesbit[i];
                chunkid += tup[i] / logical_size[i];
            }
            buffer[chunkid].push_back(tup); //相当于当前行元组被放入到chunkid对应的数据块缓冲区中
            if(buffer[chunkid].size() == page_capacity){ // 缓冲区满，写出，相当于像chunkid对应的数据块中写入一页元组
                for(j = 0; j < page_capacity; j++){
                    transfer_Tuple_ToBinary(buffer[chunkid][j], c, beginbyte, beginbit);
                }
                block->WriteBlock(sdata, page_currentid[chunkid]++, PAGESIZE); // 写出一页元组数据，对应的块当前写入的pageid++
                sdata[0] = '\0'; beginbyte = 0; beginbit = 0; c = 0;
                buffer[chunkid].clear();
            }
        }
    }
    fin1.clear();
    fin1.close();

    ///write the left
    //将缓冲区中剩余的元组写入到对应的数据块chunk中去
    for(i = 0; i < chunknum; i++){
        if(buffer[i].size() == 0) continue;
        for(g = 0; g < buffer[i].size(); g++){
            transfer_Tuple_ToBinary(buffer[i][g], c, beginbyte, beginbit);
        }
        for(g < buffer[i].size(); g < page_capacity; g++){ //这是在干什么？
            transfer_Tuple_ToBinary(empty_tuple, c, beginbyte, beginbit); //GUESS:empty_tuple标记着输入元组的结束
        }
        block->WriteBlock(sdata, page_currentid[i]++, PAGESIZE);
        sdata[0] = '\0'; beginbyte = 0; beginbit = 0; c = 0;
        buffer[i].clear();
    }
    delete block;
    return;
}

///------------------------------------------------------------------------------------------------------------------------------------------
void Rfilter::transfer_Tuple_ToBinary(vector<int> tup, unsigned char &c, int &beginbyte, int &beginbit){
    int i, j;
    int length;
    for (j = 0; j < M; j++) {//对tuple的每一维取后len[j]位
        length = dbit[j];
        while (beginbit + length >= BYTE) {
            c = (c << (BYTE - beginbit)) + READFROM(tup[j], length - BYTE + beginbit, BYTE - beginbit);
            length -= (BYTE - beginbit);
            sdata[beginbyte] = c;
            beginbyte++;
            c = 0;
            beginbit = 0;
        }
        if (length != 0) {
            c = (c << length) + READFROM(tup[j], 0, length);
            beginbit += length;
        }
    }
    return;
}
///------------------------------------------------------------------------------------------------------------------------------------------
void Rfilter::read_Page(BlockManager* block, int pid, vector<vector<int>> &tuples){
    int k, g;
    unsigned char c = 0;
    sdata[0] = '\0';
	int beginbyte = 0, beginbit = 0;
	int length = 0;
    block->ReadBlock(sdata, pid, PAGESIZE);
    //flag:标识是否遇到0 0，遇到0 0可能有两种情况：
    //1.遇到了所有维度的值都是最小值的元组
    //2.遇到了空元组empty_tuple
    //当第一次遇到0 0时，将flag修改为true；判断下一个元组若不是0 0，则将flag改为false，若下一个元组也为0 0，认为该page的元组读取完毕，退出循环
    bool flag = false;
    for (k = 0; k < page_capacity; k++) { //每个线性化值写到arr中，再存入struct
		vector<int> arr(M);
		beginbyte = k * dbit_sum / BYTE;
		beginbit = k * dbit_sum % BYTE;
		for (g = 0; g < M; g++) {
			length = dbit[g];
			while (BYTE - beginbit <= length) {
				c = *(sdata + beginbyte);
				arr[g] = arr[g] << (BYTE - beginbit);
				arr[g] += READFROM(c, 0, BYTE - beginbit);//从右侧第0位向左读BYTE - beginbit位;
				length -= BYTE - beginbit;
				beginbit = 0;
				beginbyte++;
			}
			if (length != 0) {
				c = *(sdata + beginbyte);
				arr[g] = arr[g] << length;
				arr[g] += READFROM(c, BYTE - beginbit - length, length);
				beginbit += length;
			}
		}
        //FIXME：对于不包含所有维度属性都为最小值的数据集，会为0 0 ... 0单元格添加一条虚幻的元组
		if(compare_Twotuples(arr, empty_tuple)==1){
            if(pid == 0){
                if(flag == true) break;
                else flag = true;
            } else {
                break;
            }
        }
		tuples.push_back(arr);
	}
    return;
}
///------------------------------------------------------------------------------------------------------------------------------------------
void Rfilter::read_Offsets(const char* offsetpath){
    int i, j;
    ifstream fin(offsetpath);
    string line;
    if (fin)
    {
        while (getline(fin, line))
        {
            istringstream iss(line);
            string temp;
            vector<int> a;
            while (getline(iss, temp, ' ')) {
                a.push_back(stoi(temp));
            }
            page_startid[a[0]] = a[1];
            page_endid[a[0]] = a[2];
            filter_offset[a[0]][0] = a[3];///filter type
            filter_offset[a[0]][1] = a[4];///filter beginning page
            filter_offset[a[0]][2] = a[5];///......byte
            filter_offset[a[0]][3] = a[6];///filter ending page
            filter_offset[a[0]][4] = a[7];///......byte
            last_validchunk = a[0];
        }
    }
    fin.clear();
    fin.close();
    return;
}
///------------------------------------------------------------------------------------------------------------------------------------------
void Rfilter::construct_Rangefilter(const char* datapath, const char* binarypath, const char* filterpath, const char* offsetpath){
    int i, j, k, g;
    int currentid;
    ///partition array to chunks and generate binary file
    transfer_Txt_ToBinaryfile(datapath, binarypath); // 将输入的数组数据datapath划分成数据块chunks，写入到二进制文件binarypath中
    ofstream fout(offsetpath, ios::out);
    vector<vector<int>> tuples;//the read tuples of a page，一页中读到的元组。元组tuple的形式：vector<int>
    vector<vector<int>> tuplesintotal;//the total tuples of a chunk，一个数据块chunk包含多个页page。
    int beginbyte = 0, beginbit = 0;
    unsigned char c = 0;
    sdata[0] = '\0';  //data
    sdata1[0] = '\0'; //filter
    fcurpageid = 0; beginbyte1 = 0;
    BlockManager* block = new BlockManager(binarypath, O_CREAT, PAGESIZE);
    block1 = new BlockManager(filterpath, O_CREAT, PAGESIZE);
    ///compute one-dimensional ranges
    compute_Total1Drange();
    for(i = 0; i < chunknum; i++){ //循环变量i遍历所有的数据块chunk
        if(chunksize[i] == 0) continue;
        currentid = page_startid[i]; //数据块i的起始页id
        for(g = page_startid[i]; g <= page_endid[i]; g++){
            read_Page(block, g, tuples);
            tuplesintotal.insert(tuplesintotal.end(), tuples.begin(), tuples.end());
            tuples.clear();
        }
        ///compute range-set (multidimensional ranges) and construct range filter
        vector<uint64_t> rangeids;
        compute_Rangeset(i, tuplesintotal, rangeids); // 对于数据块i，根据其块中所有元组tuplesintotal计算其多维范围集rangeids
        if(allmranges / rangeids.size() >= bitsperkey) { ///build bloom filter
            Bfilter *bf = new Bfilter(rangeids.size());
            bf->construct_Bloomfilter(i, rangeids);
            bloomFilterMap[i] = rangeids.size();
        }
        else{
            write_RFbitmap(i);////////////////////////////final page is not writen
        }
        tuplesintotal.clear();
        fout<<i<<" "<<page_startid[i]<<" "<<page_endid[i]<<" "<<filter_offset[i][0]<<" "<<filter_offset[i][1]<<" "<<filter_offset[i][2]<<" "<<filter_offset[i][3]<<" "<<filter_offset[i][4]<<endl;
    }
    fout.clear();
    fout.close();
    return;
}

///------------------------------------------------------------------------------------------------------------------------------------------
///compute all the one-dimensional ranges for each value on each dim
void Rfilter::compute_Total1Drange(){
    int i, j, k, g;
    vector<int> intervalnum(m);
    for(i = 0; i < m; i++){ // 猜测：i——for each dim
        oneDrange[i].resize(logical_size[i]);
        int offset = 3 * logical_size0[i] - 2 * intervallen[i] - 2;///the number of the first two kinds ranges，对应论文中前三种集合的数量，绿色圆形+紫色三角+蓝色正方形
        for (j = 0; j < logical_size[i]; j++){ // 猜测：j——for each value 
            ///1. [a,b], a and b are not partition values ，对应PPT中紫色三角和蓝色正方形
            int low, high;
            int interval = j / intervallen[i];
            if(((j + 1) % intervallen[i]) == 0) {///j is partition value
                if(j == intervallen[i] - 1) {///the first partition value
                    low = intervallen[i] - 2;
                    high = low + 2 * (intervallen[i] - 1) - 2;
                }
                else{
                    if(j == logical_size0[i] - 1){///the last partition value
                        high = 2 * (intervallen[i] - 1) * ((j + 1) / intervallen[i]) - 3;
                        low = high - intervallen[i] + 2;
                    }
                    else{///other partition values
                        low = 2 * (intervallen[i] - 1) * interval - 2 + intervallen[i] - 1;
                        high = low + 2 * intervallen[i] - 3;
                    }
                }
            }
            else{///j is not partition value
                if(j < intervallen[i]){///fall into the first interval
                    if(j==0){
                        low = 0; high = intervallen[i] - 3;
                    }
                    else{
                        low = j - 1; high = low + intervallen[i] - 1 - 1;
                    }
                }
                else{///other intervals
                    low = 2 * (intervallen[i] - 1) * interval - 2 + j % intervallen[i];
                    high = low + intervallen[i] - 1;
                }
            }
            for(k = low; k <= high; k++) oneDrange[i][j].push_back(k);
            ///2. [a,a]，对应PPT中绿色圆形
            //logical_size0[i] - intervallen[i] -1 对应的是紫色三角和蓝色正方形的数量
            oneDrange[i][j].push_back((uint8_t)(2*(logical_size0[i] - intervallen[i] -1) + j));
            ///3. [a,b], a and b are partition values ，对应PPT中红色菱形
            int sum = offset, beginp;
            for(k = 0; k < interval + 1; k++){//row
                sum = sum + intervallen[i] - k;
                beginp = sum - (intervallen[i] - interval);
                for(g = interval; g < intervallen[i]; g++){
                    oneDrange[i][j].push_back( (uint8_t)beginp++);
                }
            }
        }//for j
    }//for i
    return;
}
///------------------------------------------------------------------------------------------------------------------------------------------
///compute multidimensional ranges
/**
 * @param chunkid 当前处理的数据块id
 * @param alltuples 数据块chunkid中包含的所有元组
 * @param rangeIDs 输出参数，数据块chunkid的多维范围集
 */
void Rfilter::compute_Rangeset(int chunkid, vector<vector<int>> alltuples, vector<uint64_t> &rangeIDs){
    int i, j, g, k;
    int mbyte, mbit;
    bitmapbytes = (int)ceil((double)allmranges / BYTE); //chunkid对应的数据块的位图需要的字节数为bitmapbytes
    rfbitmap = new char[bitmapbytes]; // rfbitmap为数据块chunkid对应的位图
    for(i = 0; i < bitmapbytes; i++) rfbitmap[i] = 0;
    ///calculate chunk coordinates
    vector<int> coords(m); //coords[i]:当前块在维度i上的是第(coords[i]+1)个块
    int cid = chunkid;
    for(j = m-1; j >= 0; j--){
        coords[j] = cid & (powpieces[j] - 1);
        cid  = cid >> piecesbit[j];
    }
    for(j = 0; j < alltuples.size(); j++){
        ///compute the offsets of the tuple on each dim
        vector<int> tupoffset(m);
        int tupleid;
        int rangenum = 1; //RSc(t)集合的大小，RSc(t) = RSc¹(t) * RSc²(t) * ... * RScᵐ(t)
        for(k = 0; k < m; k++) {
            //alltuples[j][k] - coords[k] * logical_size[k]：第j条元组在维度k上的偏移量，即把第j条元组维度k的取值放到一维范围的PPT图(绿色圆形、红色菱形...)中
            tupoffset[k] = alltuples[j][k] - coords[k] * logical_size[k]; 
            rangenum *= oneDrange[k][tupoffset[k]].size(); //oneDrange[k][tupoffset[k]]:对应RScᵏ(t)集合，其中元组t的第k维取值平移偏移量后值为tupoffset[k]
        }
        vector<uint64_t> mranges(rangenum); //对于一条元组alltuples[j]，能计算得到的RSc(t)
        //length[i]:在下标为i的维度第一次选中一个区间(排序数)后，需要连续选中该排序数的次数为length[i]，对应分析11-1、11-2、11-3
        vector<uint64_t> lengths(m, 1);
        lengths[0] = 1;
        for(k = 1; k < m; k++) {
            lengths[k] = lengths[k-1] * oneDrange[k-1][tupoffset[k-1]].size();
        }
        ///compute the multi-dimensional ranges
        // mranges[g]：对应论文中no(r)的计算
        for(g = 0; g < rangenum; g++) {
            // GUESS:mranges[g]表达的含义：第g+1个多维范围的排序数为mranges[g]
            //g % oneDrange[0][tupoffset[0]].size() 这个取余表示的是 mranges[g] 在第0维选择是列表中的第几个
            mranges[g] = oneDrange[0][tupoffset[0]][g % oneDrange[0][tupoffset[0]].size()];///////////// 第一维初始化
        }
        for(k = 1; k < m; k++) {
            for(g = 0; g < rangenum; g++){
                // GUESS:之后的每一个维度乘完了累加？
                mranges[g] = mranges[g] << oneDrange_bit[k];//////////////
                // GUESS：与347行同理，g / lengths[k] % oneDrange[k][tupoffset[k]].size() 这个取余表示的是 mranges[g] 在第k维选择是列表中的第几个
                mranges[g] += oneDrange[k][tupoffset[k]][g / lengths[k] % oneDrange[k][tupoffset[k]].size()];//////////////
            }
        }
        for(g = 0; g < rangenum; g++){
            mbyte = mranges[g] / BYTE;
            mbit = mranges[g] % BYTE;
            char a = rfbitmap[mbyte];
            a = a >> (7-mbit);
            int f = a & 1;
            if(f != 1){ //相当于是由CRSc i(上标)中有效的一维范围组合得到的多维范围，即这个多维范围对应的范围查询在数据块c中是有元组的
                //修改位图
                rfbitmap[mbyte] = (rfbitmap[mbyte] | (1 << (7-mbit)));
                rangeIDs.push_back(mranges[g]); //将对应的多维范围排序数加入结果集合
            }
        }
    }///tuple

    //处理完成后，rfbitmap为数据块chunkid对应的多维范围的结果位图
    return;
}
///------------------------------------------------------------------------------------------------------------------------------------------
void Rfilter::write_RFbitmap(int chunkid){
    int i;char v;
    int offset = 0, offset1;///the offset on rfbitmap
    /*
    offset1:在第一个可以写入的页中写入的字节数
    offset：对于当前处理的数据块chunkid，对应的位图rfbitmap已经写入的字节数
    例子：假设PAGESIZE=4096B，要写入的第一页已经写入了1000B，即beginbyte1=1000；假设rfbitmap的大小bitmapbytes=10000B
    那么offset初始为0，offset1初始为0；
    1.先向当前页fcurpageid写入rfbitmap的前3096B(4096-1000)，写完后，offset=3096，offset1=3096，beginbyte1=0；
    2.fcurpageid++表示新开一页，向该页写入rfbitmap的接下来的4096B，写完后，offset=7192，beginbyte1=0；
    3.fcurpageid++表示再新开一页，向该页写入rfbitmap的最后的2808B(bitmapbytes-offset = 10000-7192 = 2808)，写完后，beginbyte1=2808，表示下一个数据块的rfbitmap开始写入时从该页的第2808B处开始写入
    */
    
    filter_offset[chunkid][0] = 0;///0 represents bitmap as filter
    filter_offset[chunkid][1] = fcurpageid;
    filter_offset[chunkid][2] = beginbyte1;
    if(PAGESIZE - beginbyte1 > bitmapbytes){ //当前页剩余的空间还可以放下chunkid数据块的整个位图
        strmncpy(rfbitmap, 0, bitmapbytes, sdata1, beginbyte1);
        beginbyte1 += bitmapbytes;
    }
    else{
        strmncpy(rfbitmap, 0, PAGESIZE - beginbyte1, sdata1, beginbyte1);
        block1->WriteBlock(sdata1, fcurpageid++, PAGESIZE);
        offset += (PAGESIZE - beginbyte1);
        offset1 = offset;
        beginbyte1 = 0;
        sdata1[0] = '\0';
        for(int k = 0; k < (bitmapbytes - offset1) / PAGESIZE; k++){ //用来处理rfbitmap很大(bitmapbytes很大，> PAGESIZE)，需要多个页来存储
            strmncpy(rfbitmap, offset, PAGESIZE, sdata1, beginbyte1);
            block1->WriteBlock(sdata1, fcurpageid++, PAGESIZE);
            beginbyte1 = 0;
            sdata1[0] = '\0';
            offset += PAGESIZE;
        }
        if(offset != bitmapbytes){
            strmncpy(rfbitmap, offset, bitmapbytes-offset, sdata1, beginbyte1);
            beginbyte1 = bitmapbytes-offset;
        }
    }
    filter_offset[chunkid][3] = fcurpageid;
    filter_offset[chunkid][4] = beginbyte1;///the next beginning byte
    if(chunkid==last_validchunk) block1->WriteBlock(sdata1, fcurpageid++, PAGESIZE);
    return;
}
///------------------------------------------------------------------------------------------------------------------------------------------
int Rfilter::search_RFbitmap(int chunkid, uint64_t arange){///Identify whether a range is empty on bitmap
    int i, j, k;
    int byte = arange / BYTE;
    int bit = arange % BYTE;
    char a = filters[chunkid][byte];
    if(((a >> (7-bit)) & 1) == 1) return 1;
    return 0;
}
///------------------------------------------------------------------------------------------------------------------------------------------
//int Rfilter::search_Bloomfilter(char* sbitmap, uint64_t srange){
//    int i, j, k;
//    k = (int)ceil(log(2) * bitsperkey);
//    uint32_t p, u;
//    const char* c = to_string(srange).c_str();
//    const uint8_t* data = (const uint8_t*)c;
//    for(j = 0; j < k; j++){
//        p = MurmurHash3_x86_32(data, strlen(c), j*10);
//        beginbyte = p % bitnum / BYTE;
//        beginbit = p % bitnum % BYTE;
//        u = sbitmap[beginbyte];
//        if(((u >> (7-beginbit)) & 1) != 1) return 0;
//    }
//    return 1;
//}
///------------------------------------------------------------------------------------------------------------------------------------------
void Rfilter::read_Filters(const char* offsetpath, const char* filterpath){///read the filters from file
    int i, j;
    sdata[0] = '\0';
    int sign = 0;
    read_Offsets(offsetpath);
    BlockManager* block = new BlockManager(filterpath, O_CREAT, PAGESIZE);
    for(i = 0; i < chunknum; i++){
        if(page_startid[i]==-1) continue;
        //length:chunkid为i的数据块对应的过滤器结构占用的字节数
        //offset:当前chunkid为i的数据块的过滤器内容已经读取了多少字节
        int length = 0, offset = 0; 
        if(filter_offset[i][1] == filter_offset[i][3]) //chunkid为i的数据块对应的过滤器内容保存在同一页(同一个page)中
            length = filter_offset[i][4] - filter_offset[i][2];
        else{ //chunkid为i的数据块对应的过滤器内容需要跨页保存
            length = PAGESIZE - filter_offset[i][2];
            for(j = filter_offset[i][1]+1; j <= filter_offset[i][3]; j++){
                length += PAGESIZE;
            }
            length += filter_offset[i][4];
        }
        char* meta = new char[length];
        meta[0] = '\0';
        if(sign == 0) block->ReadBlock(sdata, filter_offset[i][1], PAGESIZE);///need to read the first page
        if(filter_offset[i][1] == filter_offset[i][3]){///begin page is also end page
            strmncpy(sdata, filter_offset[i][2], filter_offset[i][4] - filter_offset[i][2], meta, offset);
            offset = offset + filter_offset[i][4] - filter_offset[i][2];
        }
        else{
            strmncpy(sdata, filter_offset[i][2], PAGESIZE - filter_offset[i][2], meta, offset);
            offset = offset + PAGESIZE - filter_offset[i][2];
            sdata[0] = '\0';
            for(j = filter_offset[i][1]+1; j < filter_offset[i][3]; j++){
                block->ReadBlock(sdata, i, PAGESIZE);
                strmncpy(sdata, 0, PAGESIZE, meta, offset);
                offset += PAGESIZE;
                sdata[0] = '\0';
            }
            block->ReadBlock(sdata, i, PAGESIZE);
            strmncpy(sdata, 0, filter_offset[i][4], meta, offset);
            sign = 1; //已经在484行把这一个block的内容读进来了，对于下一个数据块chunkid，可以直接从当前sdata中获得数据，无需再ReadBlock
        }
        string afilter(meta, meta+length);
    
        cout << "十六进制转储 (" << afilter.length() << " 字节):" << endl;
        for (int i = 0; i < length; ++i) {
            cout << hex << setw(2) << setfill('0') 
                      << (static_cast<unsigned int>(afilter[i]) & 0xFF) << " ";
        
            if ((i + 1) % 8 == 0) {
                cout << " ";
            }
        
            if ((i + 1) % 16 == 0) {
                cout << endl;
            }
        }
    
        // 处理可能不足16字节的最后一行
        if (length % 16 != 0) {
            cout << endl;
        }
        cout << dec;

        filters[i] = afilter;
        
        cout << "filters[i]的长度：" << filters[i].length() << " 字节" << endl;
        cout << dec << endl;
    }
    return;
}
///------------------------------------------------------------------------------------------------------------------------------------------
void Rfilter::process_Queries(const char* binarypath, const char* querypath, const char* offsetpath, const char* filterpath, const char* resultpath){
    int i, j, k, g;
    int lowchunk, highchunk; // lowchunk——与范围查询有重合、需要计算的最低数据块id；highchunk——与范围查询有重合、需要计算的最高数据块id
    vector<int> p1(m);
    vector<int> p2(m);
    vector<int> coor(m); // coor[j]:当前数据块在下标为j的维度上是第(coor[j]+1)个块
    vector<int> q(2*m); //q:区间查询与当前块重叠的范围相对于当前块的偏移量，相当于论文中符号q_c'
    int nemptychunks, borderchunks, overlapchunks;
    int cid, inrange, isborderchunk;
    double filtertime, processtime;
    Timer* timer = new Timer();

    //TODO:在搜索某一块的BloomFilter时，需要确定传入的参数num(该块rangeids.size())后重新初始化
    // Bfilter* bf = new Bfilter(12); //为什么这里bf传参传了一个100？不是应该从文件读吗？
    ofstream fout(resultpath, ios::out);
    BlockManager* block = new BlockManager(binarypath, O_CREAT, PAGESIZE);
    read_Filters(offsetpath, filterpath);
    vector<vector<int>> query;
    loadQuery(querypath, query);
    for(i = 0; i < query.size(); i++){
        lowchunk = 0, highchunk = 0;
        //TODO：chunklist中还应该加入非空的完全覆盖的块Ccover,在if(borderchunk == 0)语句块中进行判断操作
        vector<int> chunklist;
        for(j = 0; j < m; j++){
            p1[j] = query[i][2*j] / logical_size[i];
            lowchunk = (lowchunk << piecesbit[j]) + p1[j];
            p2[j] = query[i][2*j+1] / logical_size[i];
            highchunk = (highchunk << piecesbit[j]) + p2[j];
            /*
            p1[j] = query[i][2*j] / logical_size[i];
            lowchunk = lowchunk << piecesbit[j] + p1[j];
            p2[j] = query[i][2*j+1] / logical_size[i];
            highchunk = highchunk << piecesbit[j] + p2[j];
            */
        }
        ///compute the overlapping chunks, find the border chunks and filter them
        //overlapchunks：包括被区间查询完全覆盖的块 和 部分覆盖的块(borderchunk)
        borderchunks = 0, overlapchunks = 0;
        timer->Start();
        for(k = lowchunk; k <= highchunk; k++){
            cid = k;
            inrange = 1;
            isborderchunk = 0;
            for(j = m-1; j >= 0; j--){
                coor[j] = cid & (powpieces[j] - 1);
                cid = cid >> piecesbit[j];
                // cid >> piecesbit[j];
                if(coor[j]<p1[j] || coor[j]>p2[j]) {
                    inrange = 0;
                    break;
                }
                if(coor[j]==p1[j] || coor[j]==p2[j]) isborderchunk = 1;
            }
            if(inrange==0) continue;
            overlapchunks++;
            if(isborderchunk==0){
                if(chunksize[k] > 0){
                    chunklist.push_back(k);
                    nemptychunks++;
                }
                continue;
            }
            borderchunks++;///overlapping border chunks, not considering empty or non-empty-----///
            ///search range on the chunk's filter
            for(j = 0; j < m; j++){
                q[2*j] = max(query[i][2*j], coor[j]*logical_size[j]) - coor[j]*logical_size[j];
                q[2*j+1] = min(query[i][2*j+1], (coor[j] + 1) * logical_size[j] - 1) - coor[j] * logical_size[j]; //(coor[j] + 1) * logical_size[j] - 1)为当前块在维度j上最大的下标号
            }
            vector<uint64_t> mranges;
            get_MulRanges4query(q, mranges);
            ///filtering
            if(filter_offset[k][0]==0){///bitmap
                for(j = 0; j < mranges.size(); j++){
                    if(search_RFbitmap(k, mranges[j])==1){
                        chunklist.push_back(k);
                        break;
                    }
                }
            }
            else{///bloom filter
                Bfilter* bf = new Bfilter(bloomFilterMap[k]);
                for(j = 0; j < mranges.size(); j++){
                    if(bf->search_Bloomfilter(filters[k], mranges[j])==1){
                        chunklist.push_back(k);
                        break;
                    }
                }
            }
        }//chunk
        filtertime = timer->GetRunningTime();
        timer->Stop();
        ///read and output non-empty chunks
        vector<vector<int>> tuples;
        int sign1, sign2; //sign1标志当前页中是否有在范围查询内的元组，sign2标志当前元组是否在范围查询内
        nemptychunks = 0;///real non-empty chunks
        timer->Start();
        for(k = 0; k < chunklist.size(); k++){
            tuples.clear();
            for(j = page_startid[chunklist[k]] /*filter_offset[chunklist[k]][1]*/; j <= page_endid[chunklist[k]] /*filter_offset[chunklist[k]][3]*/; j++){//page
                read_Page(block, j, tuples);
                sign1 = 0;
                for(g = 0; g < tuples.size(); g++){
                    sign2 = 1;
                    for(int v = 0; v < m; v++){
                        if(tuples[g][v] < query[i][2*v] || tuples[g][v] > query[i][2*v+1]) {
                            sign2 = 0; break;
                        }
                    }
                    if(sign2==1) {sign1=1; break;}
                }
                if(sign1==1) {nemptychunks++; break;}
            }
        }
        processtime = timer->GetRunningTime();
        timer->Stop();
        fout<<overlapchunks<<" "<<borderchunks<<" "<<nemptychunks<<" "<<(double)nemptychunks/overlapchunks<<" "<<filtertime<<" "<<processtime<<endl;
    }//query
    fout.clear();
    fout.close();
    return;
}
///------------------------------------------------------------------------------------------------------------------------------------------
void Rfilter::get_MulRanges4query(vector<int> aquery, vector<uint64_t> &mranges){
    int i, j, k, g;
    int interval1, interval2, offset1, offset2, sum, p1, p2, num1, num2;
    vector<vector<int>> oneranges(m);
    ///get one-dimensional ranges
    /*
    GUESS:aquery[0]——第一个维度查询范围的最小值； aquery[1]——第一个维度查询范围的最大值；
          aquery[2]——第二个维度查询范围的最小值； aquery[3]——第二个维度查询范围的最大值；依此类推
    */
    for(i = 0; i < aquery.size()/2; i++){ 
        interval1 = aquery[2*i] / intervallen[i];
        interval2 = aquery[2*i+1] / intervallen[i];
        offset1 = aquery[2*i] % intervallen[i];
        offset2 = aquery[2*i+1] % intervallen[i];
        p1 = (aquery[2*i]+1) / intervallen[i];
        p2 = (aquery[2*i+1]+1) / intervallen[i];
        num1 = 2*(logical_size0[i] - intervallen[i] - 1);
        num2 = 3*logical_size0[i] - 2*intervallen[i] - 2;
        if(aquery[2*i] == aquery[2*i+1]){///[a,a]
            oneranges[i].push_back(num1 + aquery[2*i]);
            continue;
        }
        if(aquery[2*i]==0 || offset1 == intervallen[i]-1){///[a,b], a is partition value
            if(offset2 == intervallen[i]-1){///b is a partition value
                sum = 0;
                for(j = 0; j < p1; j++)///row
                    sum = sum + intervallen[i] - j;
                oneranges[i].push_back(num2 + sum + p2 - p1 - 1);
            }
            else{///b is not a partition value
                if(aquery[2*i+1] - aquery[2*i] < intervallen[i]){ ///b is near, distance is smaller than interval length
                    if(aquery[2*i]==0) {
                        oneranges[i].push_back(offset2 - 1);
                    }
                    else {
                        oneranges[i].push_back(2*(intervallen[i]-1) * p1 - 2 + offset2);
                    }
                }
                else{///b is far
                    sum = 0;
                    for(j = 0; j < p1; j++)///row
                        sum = sum + intervallen[i] - j;
                    oneranges[i].push_back(num2 + sum + p2 - p1 - 1);
                    oneranges[i].push_back(2*(intervallen[i]-1) * interval2 - 2 + offset2);
                }
            }
            continue;
        }
        if(offset2 == intervallen[i]-1){///[a,b], b is a partition value
            if(aquery[2*i+1] - aquery[2*i] < intervallen[i]){///a is near
                oneranges[i].push_back(2*(intervallen[i]-1) * interval1 - 2 + intervallen[i]-1 + offset1);
            }
            else{///a is far
                sum = 0;
                for(j = 0; j <= p1; j++)///row
                    sum = sum + intervallen[i] - j;
                oneranges[i].push_back(num2 + sum + interval2 - interval1 - 1);
                oneranges[i].push_back(2*(intervallen[i]-1) * interval1 - 2 + intervallen[i]-1 + offset1);
            }
            continue;
        }
        ///[a,b], a and b are not partition values
        if(interval1==interval2){
            for(j = offset1; j <= offset2; j++){
                oneranges[i].push_back(num1 + aquery[2*i]+j);
            }
        }
        else{
            if(interval1 + 1 == interval2){///adjcent intervals
                oneranges[i].push_back(2*(intervallen[i]-1) * interval1 - 2 + intervallen[i]-1 + offset1);
                oneranges[i].push_back(2*(intervallen[i]-1) * interval2 - 2 + offset2);
            }
            else{
                oneranges[i].push_back(2*(intervallen[i]-1) * interval1 - 2 + intervallen[i]-1 + offset1);
                sum = 0;
                for(j = 0; j <= p1; j++)///row
                    sum = sum + intervallen[i] - j;
                oneranges[i].push_back(num2 + sum + interval2 - interval1 - 2);
                oneranges[i].push_back(2*(intervallen[i]-1) * interval2 - 2 + offset2);
            }
        }
    }
    ///combine multidimensional ranges
    vector<uint64_t> lengths(m, 1);
    lengths[0] = 1;
    for(k = 1; k < m; k++) {
        lengths[k] = lengths[k-1] * oneranges[k-1].size();
    }
    int rnum = lengths[m-1] * oneranges[m-1].size();
    mranges.resize(rnum);
    ///compute the multi-dimensional ranges
    for(g = 0; g < rnum; g++) {
        mranges[g] = oneranges[0][g % oneranges[0].size()];
    }
    for(k = 1; k < m; k++) {
        for(g = 0; g < rnum; g++){
            mranges[g] = mranges[g] << oneDrange_bit[k];
            mranges[g] += oneranges[k][g / lengths[k] % oneranges[k].size()];
        }
    }
    return;
}
