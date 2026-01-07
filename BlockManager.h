#pragma once
#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <inttypes.h>
#include <stdlib.h>
#include <iostream>
#include <list>
#include <map>
#include <string>
#include <assert.h>
#include <errno.h>
#include <semaphore.h>
#define _USE_GNU 1

enum ret {
	SUCCESS = 0,
	FAIL = -1,
};

class BlockManager
{
	int open_fd = -1;
	int block_size = -1;
	char file_name[256];
public:
	/**
	* Allow to r/w file in parallel or one by one in O_DIRECT mode
	* parameter file_name: the file to be r/w
	* parameter file_mode: O_CREAT means create a new file or others
	* parameter block_size: the r/w unit operation size
	*/
	BlockManager(const char* file_name, int file_mode, int block_size);

	/**
	* open a file, and return the fd of the file
	* parameter file_name: the file to be opened;
	* parameter file_mode: O_CREAT means create a new file or others
	* return: FAIL or SUCCESS
	*/
	int Open(const char* file_name, int file_mode);

	/**
	* read a block from file, and return the status
	* parameter des: the buffer to fetch data
	* parameter blockId: the page id to be read
	* parameter block_length: the size to be read from blockId
	* return: FAIL or SUCCESS
	*/
	int ReadBlock(char* des, uint64_t blockId, int block_length);

	/**
	 * write a block into file, and return the status
	 * parameter src: the buffer of data to be written
	 * parameter blockId: the page id to be written
	 * parameter block_length: the size to be written
	 * return: FAIL or SUCCESS
	 */
	int WriteBlock(char* src, uint64_t blockId, int block_length);

	/**
	* return the offset of the blockId
	* parameter blockId: the page id to be read
	* return: the logic address of the blockId
	*/
	uint64_t getoffset(uint64_t blockId);

	/**
	* close a file, and return the status of the close opertaion
	* return: FAIL or SUCCESS
	*/
	int Close(int fd);

	~BlockManager();
};


