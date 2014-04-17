package com.ailk.oci.ocnosql.client.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.PoolMap;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * wangyp5
 */
public class HTableUtilsV2 {
    private static Map<Configuration, HTablePool> poolMap = new HashMap<Configuration, HTablePool>();
    public static HTableInterface getTable(Configuration conf, final String tableName) {
        return getTable(conf, Bytes.toBytes(tableName));
    }
    public static HTableInterface getTable(Configuration conf, final byte[] tableName) {
        HTablePool pool = poolMap.get(conf);
        if (pool == null) {
            synchronized (poolMap) {
                pool = poolMap.get(conf);
                if (pool == null) {
                    pool = new HTablePool(conf, 100, PoolMap.PoolType.ThreadLocal);
                    poolMap.put(conf, pool);
                }
            }
        }
        //System.out.println("=================25" + pool.getTable(tableName).getTableName());
        return pool.getTable(tableName);
    }

}
