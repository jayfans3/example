package com.test.jmxserver;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 
 * 在创建管理接口时，某些参数和操作的特点很自然地就表明这些参数和数据应当被包含在内，例如配置参数、操作统计值、调试操作（例如修改日志级别或把应用程序状态导出到文件）、生命周期操作（启动、停止）。检测一个应用程序，让它支持对这些属性和操作的访问，通常相当容易。但是，要从 JMX 获得最大价值，就要在设计时考虑什么数据在运行时对用户和操作员有用。
如果用 JMX 了解服务器应用程序的工作情况，需要一种标识和跟踪工作单元的机制。如果使用标准的 Runnable 和 Callable 接口描述任务，通过让任务类自描述（例如实现toString() 方法），可以在任务生命周期内跟踪它们，并提供 MBean 方法来返回等候中、处理中和完成的任务列表。
清单 3 中的 TrackingThreadPool 演示的是 ThreadPoolExecutor 的一个子类，它及时给出正在处理中的是哪些任务，以及已经完成的任务的时间统计值。它通过覆盖 beforeExecute() 和 afterExecute() 挂钩，并提供能检索所搜集数据的 getter，实现这些任务。
 * @author liujs3
 *
 */
public class TrackingThreadPool extends ThreadPoolExecutor {
    private final Map<Runnable, Boolean> inProgress 
        = new ConcurrentHashMap<Runnable,Boolean>();
    private final ThreadLocal<Long> startTime = new ThreadLocal<Long>();
    private long totalTime;
    private int totalTasks;

    public TrackingThreadPool(int corePoolSize, int maximumPoolSize, long keepAliveTime,
       TimeUnit unit, BlockingQueue<Runnable> workQueue) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);
    }

    protected void beforeExecute(Thread t, Runnable r) {
        super.beforeExecute(t, r);
        inProgress.put(r, Boolean.TRUE);
        startTime.set(new Long(System.currentTimeMillis()));
    }

    protected void afterExecute(Runnable r, Throwable t) {
        long time = System.currentTimeMillis() - startTime.get().longValue();
        synchronized (this) {
            totalTime += time;
            ++totalTasks;
        }
        inProgress.remove(r);
        super.afterExecute(r, t);
    }

    public Set<Runnable> getInProgressTasks() {
        return Collections.unmodifiableSet(inProgress.keySet());
    }

    public synchronized int getTotalTasks() {
        return totalTasks;
    }

    public synchronized double getAverageTaskTime() {
        return (totalTasks == 0) ? 0 : totalTime / totalTasks;
    }
}