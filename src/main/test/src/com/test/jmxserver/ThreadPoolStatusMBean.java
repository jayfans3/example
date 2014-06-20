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
*���������������㹻����ô���������ٽ�һ������ÿ�������ύʱ��Ϊ��ע��һ�� MBean ��Ȼ�����������ʱ��ȡ��ע�ᣩ��Ȼ������ù���ӿڲ�ѯÿ������ĵ�ǰ״̬�������˶೤ʱ�䣬��������ȡ������
�嵥 5 �е� ThreadPoolStatus ʵ���� ThreadPoolStatusMBean �ӿڣ����ṩ��ÿ��������������ʵ�֡��� MBean ʵ�����еĵ������һ����ÿ������ʵ����������ϸ�飬���԰�ʵ��ί�и��˵ײ��ܹܶ��������ʾ���У�JMX ������ȫ�������ܹ�ʵ��Ĵ��롣TrackingThreadPool ���� JMX һ����֪��ͨ��Ϊ��ص������ṩ�������ͷ����������ṩ���Լ��ı�̹���ӿڡ� ������ѡ����ʵ������ֱ��ʵ�ֹ����ܣ��� TrackingThreadPool ʵ�� TrackingThreadPoolMBean �ӿڣ������ߵ���ʵ�֣����嵥 4 �� 5 ��ʾ����
*/