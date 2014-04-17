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

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.VolumeChoosingPolicy;
import org.apache.hadoop.util.DiskChecker.DiskOutOfSpaceException;

import java.io.IOException;
import java.util.*;

public class OCiVolumeChoosingPolicy<V extends FsVolumeSpi>
        implements VolumeChoosingPolicy<V>, Configurable {

    private int curVolume = 0;
    private String[] disk;
    private Map<Integer, Integer> ratioMap = new HashMap<Integer, Integer>();

    @Override
    public synchronized void setConf(Configuration conf) {
        disk = conf.get("hbase.disk").split(",");
        List<Integer> list = new ArrayList<Integer>();
        for (int i = 0; i < 100; i++) {
            list.add(i);
        }
        Collections.shuffle(list);
        int i = 0;
        for (int j = 0; j < disk.length; j++) {
            String metaDisk = disk[j];
            int temp = i + Integer.parseInt(metaDisk);
            for (; i < temp; i++) {
                ratioMap.put(list.get(i), j);
            }
        }
    }

    @Override
    public synchronized Configuration getConf() {
        // Nothing to do. Only added to fulfill the Configurable contract.
        return null;
    }

    @Override
    public synchronized V chooseVolume(final List<V> volumes, final long blockSize
    ) throws IOException {
        if (volumes.size() < 1) {
            throw new DiskOutOfSpaceException("No more available volumes");
        }

        int startVolume = curVolume;
        long maxAvailable = 0;

        while (true) {
            Integer index = ratioMap.get(curVolume);
            final V volume = volumes.get(index);

            curVolume = (curVolume + 1) % 100;
            long availableVolumeSize = volume.getAvailable();

            if (availableVolumeSize > blockSize) {
                return volume;
            }
            if (availableVolumeSize > maxAvailable) {
                maxAvailable = availableVolumeSize;
            }

            if (curVolume == startVolume) {
                throw new DiskOutOfSpaceException("Out of space: "
                        + "The volume with the most available space (=" + maxAvailable
                        + " B) is less than the block size (=" + blockSize + " B).");
            }
        }
    }
}
