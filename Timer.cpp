#include "Timer.h"


Timer::Timer()
{
	is_pause = false;
	is_stop = true;
	time_interval = 0;
}

bool Timer::isPause()
{
	if (is_pause)
		return true;
	else
		return false;
}

bool Timer::isStop()
{
	if (is_stop)
		return true;
	return false;
}

void Timer::Start()
{
	if (is_stop)
	{
		start_time = get_wall_time();
		is_stop = false;
	}
	else if (is_pause)
	{
		is_pause = false;
		start_time += get_wall_time() - pause_time;
	}
}

void Timer::Pause()
{
	if (is_stop || is_pause)
		return;
	else
	{
		is_pause = true;
		pause_time = get_wall_time();
	}
}

void Timer::Stop()
{
	if (is_stop)
		return;
	else if (is_pause)
	{
		is_pause = false;
		is_stop = true;
		time_interval = pause_time - start_time;
	}
	else if (!is_stop)
	{
		is_stop = true;
		time_interval = get_wall_time() - start_time;
	}
}


void Timer::ShowRunningTime()
{
	double t = get_wall_time() - start_time;
	std::cout << t << " s" << std::endl;
}

double Timer::GetRunningTime()
{
	double t = get_wall_time() - start_time;
	return t;
}

void Timer::ShowRunnedTime()
{
	std::cout << time_interval << " s" << std::endl;
}

void Timer::Test()
{
	Timer t;
	t.Start();

	for (int i = 0; i < 100000000; i++)
	{
		t.ShowRunningTime();
	}
	t.Stop();
	t.ShowRunnedTime();
}

double Timer::GetTimeInterval()
{
	return time_interval;
}

Timer::~Timer()
{
}


