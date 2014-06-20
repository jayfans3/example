
#include "UsageEnvironment.hh"

void UsageEnvironment::reclaim() {
	// We delete ourselves only if we have no remainining state:
	if (liveMediaPriv == NULL && groupsockPriv == NULL) delete this;
}

UsageEnvironment::UsageEnvironment(TaskScheduler& scheduler)
: liveMediaPriv(NULL), groupsockPriv(NULL), fScheduler(scheduler) {
}

UsageEnvironment::~UsageEnvironment() {
}

// By default, we handle 'should not occur'-type library errors by calling abort().  Subclasses can redefine this, if desired.
void UsageEnvironment::internalError() {
	abort();
}


TaskScheduler::TaskScheduler() {
}

TaskScheduler::~TaskScheduler() {
}

void TaskScheduler::rescheduleDelayedTask(TaskToken& task,int64_t microseconds, TaskFunc* proc,void* clientData) 
{
	unscheduleDelayedTask(task);
	task = scheduleDelayedTask(microseconds, proc, clientData);
}

// By default, we handle 'should not occur'-type library errors by calling abort().  Subclasses can redefine this, if desired.
void TaskScheduler::internalError() 
{
	abort();
}
