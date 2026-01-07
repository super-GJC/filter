#include "BlockManager.h"
///----------------------------------------------------------------------------------------------------------------------------
BlockManager::BlockManager(const char* file_name, int file_mode, int block_size)
{
	this->block_size = block_size;
	strcpy(this->file_name, file_name);

	this->Open(this->file_name, file_mode);
}
///----------------------------------------------------------------------------------------------------------------------------
int BlockManager::Open(const char* file_name, int file_mode)
{
	if (this == NULL) {
		printf("NULL Pointer!\n");
		return FAIL;
	}

	if (file_name == NULL)
	{
		printf("File Name is NULL!");
		return FAIL;
	}

	if (this->block_size < 0)
	{
		printf("block_size is not set yet!\n");
		return FAIL;
		return 0;
	}

	open_fd = -1;
	//check whether the file is existing.
	int exist = access(file_name, F_OK);

	//if the file doesn't exist and the file_mode is O_CREAT, then create a new file
	if (exist < 0)
	{
		if (file_mode == O_CREAT) {
#if _USE_GNU
			open_fd = open(file_name, O_RDWR | O_CREAT | O_EXCL | O_DIRECT, 0666);
#else
			open_fd = open(file_name, O_RDWR | O_CREAT | O_EXCL, 0666);
#endif
		}
		else
		{
			printf("No File is opened!\n");
			return FAIL;
		}
	}
	else
	{
#if _USE_GNU
		open_fd = open(file_name, O_RDWR | O_EXCL | O_DIRECT, 0666);
#else
		open_fd = open(file_name, O_RDWR | O_EXCL, 0666);
#endif
	}

	if (open_fd < 0)
	{
		printf("Open File Failed!\n");
		return FAIL;
	}

	if (lseek(open_fd, 0, SEEK_SET) == -1)
	{
		printf("Can not seek!\n");
		return FAIL;
	}

	//try to lock the file, editing is not allowed when we r/w this file.
	if (lockf(open_fd, F_LOCK, 0) == -1)
	{
		printf("Failed to lock file \"%s\"\n", file_name);//It means that other application is working on this file
		close(open_fd);
		return FAIL;
	}

	return SUCCESS;
}
///----------------------------------------------------------------------------------------------------------------------------
int BlockManager::ReadBlock(char* des, ulong blockId, int block_length)
{
	if (this == NULL) {
		printf("NULL Pointer!\n");
		return FAIL;
	}

	ulong block_offset = this->getoffset(blockId);

	if (lseek(this->open_fd, block_offset, SEEK_SET) == -1)
	{
	std::cout<<"block id="<<blockId<<std::endl;
		printf("Can not seek!\n");
		return FAIL;
	}

#if _USE_GNU
	char* alignmentbuf;
	posix_memalign((void**)&(alignmentbuf), 4096, (block_length));
	if (read(this->open_fd, alignmentbuf, block_length) != block_length) {
        std::cout<<"read length="<<read(this->open_fd, des, block_length)<<"block id="<<blockId<<std::endl;
		printf("Can not read data\n");
		return FAIL;
	}
	memmove(des, alignmentbuf, block_length);
	delete alignmentbuf;
#else
	if ( read(this->open_fd, des, block_length)!= block_length) {
        std::cout<<"read length="<<read(this->open_fd, des, block_length)<<"block id="<<blockId<<std::endl;
		printf("Can not read data\n");
		return FAIL;
	}
#endif
	return SUCCESS;
}
///----------------------------------------------------------------------------------------------------------------------------
int BlockManager::WriteBlock(char* src, ulong blockId, int block_length)
{
//std::cout<<"a"<<std::endl;
	if (this == NULL) {
		printf("NULL Pointer!\n");
		return FAIL;
	}
//std::cout<<"b"<<std::endl;
	ulong block_offset = this->getoffset(blockId);
	if (lseek(this->open_fd, block_offset, SEEK_SET) == -1)
	{
		printf("Can not seek!\n");
		return FAIL;
	}
	//std::cout<<"c"<<std::endl;
#if _USE_GNU
	char* alignmentbuf;
	posix_memalign((void**)&(alignmentbuf), 4096, block_length);
	memmove(alignmentbuf, src, block_length);
	if (write(this->open_fd, alignmentbuf, block_length) != block_length)
	{
		printf("Can't write data\n");
		return FAIL;
	}
	delete[] alignmentbuf;
#else
	if (write(this->file_fd, src, block_length) == -1)
	{
		printf("Can't write data\n");
		return FAIL;
	}
#endif
	return SUCCESS;
}
///----------------------------------------------------------------------------------------------------------------------------
ulong BlockManager::getoffset(ulong blockId)
{
	return blockId * this->block_size;
}
///----------------------------------------------------------------------------------------------------------------------------
int BlockManager::Close(int fd)
{
	if (this == NULL) {
		printf("NULL Pointer!\n");
		return FAIL;
	}

	if (close(fd) == -1)
	{
		printf("File Close Failed!\n");
		printf("Message : %s\n", strerror(errno));
		return FAIL;
	}
	return SUCCESS;
}
///----------------------------------------------------------------------------------------------------------------------------
BlockManager::~BlockManager()
{
	this->Close(this->open_fd);
}
///----------------------------------------------------------------------------------------------------------------------------
