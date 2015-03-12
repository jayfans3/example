package com.test.jmxserver;

public interface ThreadPoolStatusMBean {
    public int getActiveThreads();
    public int getActiveTasks();
    public int getTotalTasks();
    public int getQueuedTasks();
    public double getAverageTaskTime();
    public String[] getActiveTaskNames();
    public String[] getQueuedTaskNames();
}

/**
*如果任务的重量级足够，那么甚至可以再进一步，在每个任务提交时都为它注册一个 MBean （然后在任务完成时再取消注册）。然后可以用管理接口查询每个任务的当前状态、运行了多长时间，或者请求取消任务。
清单 5 中的 ThreadPoolStatus 实现了 ThreadPoolStatusMBean 接口，它提供了每个访问器的明显实现。与 MBean 实现类中的典型情况一样，每个操作实现起来都很细碎，所以把实现委托给了底层受管对象。在这个示例中，JMX 代码完全独立于受管实体的代码。TrackingThreadPool 对于 JMX 一无所知；通过为相关的属性提供管理方法和访问器，它提供了自己的编程管理接口。 还可以选择在实现类中直接实现管理功能（让 TrackingThreadPool 实现 TrackingThreadPoolMBean 接口），或者单独实现（如清单 4 和 5 所示）。
*/