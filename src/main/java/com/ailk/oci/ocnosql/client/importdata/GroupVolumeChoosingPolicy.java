/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ailk.oci.ocnosql.client.importdata;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.VolumeChoosingPolicy;
import org.apache.hadoop.util.DiskChecker.DiskOutOfSpaceException;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class GroupVolumeChoosingPolicy<V extends FsVolumeSpi>
        implements VolumeChoosingPolicy<V>, Configurable {
    private static final Log LOG = LogFactory.getLog(GroupVolumeChoosingPolicy.class);
    private int curVolume = 0;
    private int[][] groups; //分组，按优先级从大到小排列
    private int curGroup = 0;
    private File groupsFile = null;
    {
        new  CheckThread().start();
    }

    @Override
    public synchronized void setConf(Configuration conf) {
        String dirGroups = conf.get("dfs.datanode.data.dir.groups");
        config(dirGroups);
        groupsFile = new File(conf.get("dfs.datanode.data.dir.groups.file"));
        LOG.debug("dfs.datanode.data.dir.groups.file:" + groupsFile);
    }

    private synchronized void config(String dirGroups) {
        LOG.debug("dfs.datanode.data.dir.groups:" + dirGroups);
        String[] disk = dirGroups.split(",");
        //分优先级，优先级高的未满之前，不往优先级低的磁盘写。
        TreeMap<Integer, List<Integer>> groupsTree = new TreeMap<Integer, List<Integer>>();
        for (int i = 0; i < disk.length; i++) {
            int groupId = Integer.parseInt(disk[i]);
            List<Integer> group = groupsTree.get(groupId);
            if (group == null) {
                group = new ArrayList<Integer>();
                groupsTree.put(groupId, group);
            }
            group.add(i);
        }
        groups = new int[groupsTree.size()][];
        int i = groups.length - 1;
        for (Map.Entry<Integer, List<Integer>> entry : groupsTree.entrySet()) {
            List<Integer> volumes = entry.getValue();
            int size = volumes.size();
            int[] group = new int[size];
            int j = 0;
            for (Integer volume : volumes) {
                group[j++] = volume;
            }
            groups[i--] = group;
        }
    }

    private class CheckThread extends Thread {
        @Override
        public void run() {
            for (; ; ) {
                if (groupsFile != null && groupsFile.exists()) {
                    BufferedReader reader = null;
                    try {
                        reader = new BufferedReader(new InputStreamReader(new FileInputStream(groupsFile)));
                        config(reader.readLine());
                    } catch (Throwable e) {
                        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                    } finally {
                        if (reader != null) {
                            try {
                                reader.close();
                            } catch (Throwable e) {
                            }
                        }
                        groupsFile.delete();
                    }
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                }
            }
        }
    }

    @Override
    public synchronized Configuration getConf() {
        // Nothing to do. Only added to fulfill the Configurable contract.
        return null;
    }

    @Override
    public synchronized V chooseVolume(final List<V> volumes, final long blockSize) throws IOException {
        if (volumes.size() < 1) {
            throw new DiskOutOfSpaceException("No more available volumes");
        }
        for (int i = 0; i < curGroup; i++) { //遍历所有大于当前分组优先级的磁盘，如果磁盘有空间了，则修改当前group
            for (int volumn : groups[i]) {
                final V volume = volumes.get(volumn);
                long availableVolumeSize = volume.getAvailable();
                if (availableVolumeSize > blockSize) {
                    curGroup = i;
                    curVolume = 0;
                    return volume;
                }
            }
        }
        //如果优先级高的都满了，则遍历当前的group
        int startVolume = curVolume;
        long maxAvailable = 0;
        while (true) {
            final V volume = volumes.get(groups[curGroup][curVolume]);
            curVolume = (curVolume + 1) % groups[curGroup].length;
            long availableVolumeSize = volume.getAvailable();

            if (availableVolumeSize > blockSize) {
                return volume;
            }
            if (availableVolumeSize > maxAvailable) {
                maxAvailable = availableVolumeSize;
            }
            if (curVolume == startVolume) {//当前group都满了，则遍历下一个group
                curVolume = 0;
                curGroup++;
                if (curGroup < groups.length) {
                } else {
                    throw new DiskOutOfSpaceException("Out of space: "
                            + "The volume with the most available space (=" + maxAvailable
                            + " B) is less than the block size (=" + blockSize + " B).");
                }
            }
        }
    }

    public int[][] getGroups() {
        return groups;
    }
}
