#pragma once
#include "time.h"
#include <sys/time.h>
#include <iostream>
static double get_wall_time()
{
	struct timeval time;
	if (gettimeofday(&time, NULL)) {
		return 0;
	}
	return (double)time.tv_sec + (double)time.tv_usec * .000001;
}

class Timer
{
//private:


public:
	double start_time;
	double pause_time;
	bool is_pause;
	bool is_stop;
	double time_interval;

	Timer();
	bool isPause();
	bool isStop();
	void Start();
	void Pause();
	void Stop();
	inline long long getStartTime() { return start_time; }
	void ShowRunningTime();
	double GetRunningTime();
	void ShowRunnedTime();
	void Test();
	double GetTimeInterval();
	~Timer();
};



