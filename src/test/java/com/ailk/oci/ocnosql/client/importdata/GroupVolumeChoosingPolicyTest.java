package com.ailk.oci.ocnosql.client.importdata;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.util.DiskChecker;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class GroupVolumeChoosingPolicyTest {
    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Before
    public void before() throws Exception {
    }

    @After
    public void after() throws Exception {
    }

    /**
     * Method: setConf(Configuration conf)
     */
    @Test
    public void testSetConf() throws Exception {
        GroupVolumeChoosingPolicy policy = new GroupVolumeChoosingPolicy();
        Configuration conf = new Configuration();
        conf.set("hbase.disk", "1,1,1,1,0,0,0,0,0,0");
        policy.setConf(conf);
        assertEquals(2, policy.getGroups().length);
        assertArrayEquals(new int[]{0, 1, 2, 3}, policy.getGroups()[0]);
        assertArrayEquals(new int[]{4, 5, 6, 7, 8, 9}, policy.getGroups()[1]);
        conf.set("hbase.disk", "1,1,2,1,3,0,1,3,2,5");
        policy.setConf(conf);
        assertEquals(5, policy.getGroups().length);
        assertArrayEquals(new int[]{9}, policy.getGroups()[0]);
        assertArrayEquals(new int[]{4, 7}, policy.getGroups()[1]);
        assertArrayEquals(new int[]{2, 8}, policy.getGroups()[2]);
        assertArrayEquals(new int[]{0, 1, 3, 6}, policy.getGroups()[3]);
        assertArrayEquals(new int[]{5}, policy.getGroups()[4]);
    }

    /**
     * Method: getConf()
     */
    @Test
    public void testGetConf() throws Exception {
        assertNull(new GroupVolumeChoosingPolicy().getConf());
    }

    /**
     * 按照组优先级，先写高优先级组，满了后写低优先级组
     */
    @Test
    public void testChooseVolume() throws Exception {
        GroupVolumeChoosingPolicy policy = new GroupVolumeChoosingPolicy();
        Configuration conf = new Configuration();
        conf.set("hbase.disk", "1,1,0,0,0");
        policy.setConf(conf);
        List<FsVolumeSpi> list = new ArrayList<FsVolumeSpi>();
        FsVolumeSpi ssd1 = mock(FsVolumeSpi.class);
        when(ssd1.getAvailable()).thenAnswer(new GetAvailable(310));
        FsVolumeSpi ssd2 = mock(FsVolumeSpi.class);
        when(ssd2.getAvailable()).thenAnswer(new GetAvailable(200));
        FsVolumeSpi sac1 = mock(FsVolumeSpi.class);
        when(sac1.getAvailable()).thenAnswer(new GetAvailable(205));
        FsVolumeSpi sac2 = mock(FsVolumeSpi.class);
        when(sac2.getAvailable()).thenAnswer(new GetAvailable(205));
        FsVolumeSpi sac3 = mock(FsVolumeSpi.class);
        when(sac3.getAvailable()).thenAnswer(new GetAvailable(205));
        list.add(ssd1);
        list.add(ssd2);
        list.add(sac1);
        list.add(sac2);
        list.add(sac3);
        assertSame(ssd1, policy.chooseVolume(list, 100));
        assertSame(ssd2, policy.chooseVolume(list, 100));
        assertSame(ssd1, policy.chooseVolume(list, 100));
        assertSame(ssd1, policy.chooseVolume(list, 100));
        assertSame(sac1, policy.chooseVolume(list, 100));
        assertSame(sac2, policy.chooseVolume(list, 100));
        assertSame(sac3, policy.chooseVolume(list, 100));
        assertSame(sac1, policy.chooseVolume(list, 100));
        assertSame(sac2, policy.chooseVolume(list, 100));
        assertSame(sac3, policy.chooseVolume(list, 100));
        exception.expect(DiskChecker.DiskOutOfSpaceException.class);
        policy.chooseVolume(list, 100);

    }

    /**
     * 高优先组的磁盘由于删除文件操作等又有空间了，则重新写高优先组
     *
     * @throws Exception
     */
    @Test
    public void testChooseVolumeWithDeleteFile() throws Exception {
        GroupVolumeChoosingPolicy policy = new GroupVolumeChoosingPolicy();
        Configuration conf = new Configuration();
        conf.set("hbase.disk", "1,1,0,0,0");
        policy.setConf(conf);
        List<FsVolumeSpi> list = new ArrayList<FsVolumeSpi>();
        FsVolumeSpi ssd1 = mock(FsVolumeSpi.class);
        GetAvailable getAvailable1 = new GetAvailable(310);
        when(ssd1.getAvailable()).thenAnswer(getAvailable1);
        FsVolumeSpi ssd2 = mock(FsVolumeSpi.class);
        when(ssd2.getAvailable()).thenAnswer(new GetAvailable(200));
        FsVolumeSpi sac1 = mock(FsVolumeSpi.class);
        when(sac1.getAvailable()).thenAnswer(new GetAvailable(205));
        FsVolumeSpi sac2 = mock(FsVolumeSpi.class);
        when(sac2.getAvailable()).thenAnswer(new GetAvailable(205));
        FsVolumeSpi sac3 = mock(FsVolumeSpi.class);
        when(sac3.getAvailable()).thenAnswer(new GetAvailable(205));
        list.add(ssd1);
        list.add(ssd2);
        list.add(sac1);
        list.add(sac2);
        list.add(sac3);
        assertSame(ssd1, policy.chooseVolume(list, 100));
        assertSame(ssd2, policy.chooseVolume(list, 100));
        assertSame(ssd1, policy.chooseVolume(list, 100));
        assertSame(ssd1, policy.chooseVolume(list, 100));
        assertSame(sac1, policy.chooseVolume(list, 100));
        assertSame(sac2, policy.chooseVolume(list, 100));
        assertSame(sac3, policy.chooseVolume(list, 100));

        getAvailable1.setAvailable(401);
        assertSame(ssd1, policy.chooseVolume(list, 100));
        assertSame(ssd1, policy.chooseVolume(list, 100));
        assertSame(ssd1, policy.chooseVolume(list, 100));
        assertSame(ssd1, policy.chooseVolume(list, 100));
        assertSame(sac1, policy.chooseVolume(list, 100));

    }

    private static class GetAvailable implements Answer {
        private long available;

        private GetAvailable(long available) {
            this.available = available;
        }

        @Override
        public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
            long a = available;
            available -= 100;
            return a;
        }

        private void setAvailable(long available) {
            this.available = available;
        }
    }

} 
