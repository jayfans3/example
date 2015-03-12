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
 * �ڴ�������ӿ�ʱ��ĳЩ�����Ͳ������ص����Ȼ�ؾͱ�����Щ����������Ӧ�����������ڣ��������ò���������ͳ��ֵ�����Բ����������޸���־������Ӧ�ó���״̬�������ļ������������ڲ�����������ֹͣ�������һ��Ӧ�ó�������֧�ֶ���Щ���ԺͲ����ķ��ʣ�ͨ���൱���ס����ǣ�Ҫ�� JMX �������ֵ����Ҫ�����ʱ����ʲô����������ʱ���û��Ͳ���Ա���á�
����� JMX �˽������Ӧ�ó���Ĺ����������Ҫһ�ֱ�ʶ�͸��ٹ�����Ԫ�Ļ��ơ����ʹ�ñ�׼�� Runnable �� Callable �ӿ���������ͨ����������������������ʵ��toString() ���������������������������ڸ������ǣ����ṩ MBean ���������صȺ��С������к���ɵ������б�
�嵥 3 �е� TrackingThreadPool ��ʾ���� ThreadPoolExecutor ��һ�����࣬����ʱ�������ڴ����е�����Щ�����Լ��Ѿ���ɵ������ʱ��ͳ��ֵ����ͨ������ beforeExecute() �� afterExecute() �ҹ������ṩ�ܼ������Ѽ����ݵ� getter��ʵ����Щ����
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