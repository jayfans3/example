package com.ailk.oci.ocnosql.client;

import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: shaoaq
 * Date: 13-5-18
 * Time: 下午6:46
 * To change this template use File | Settings | File Templates.
 */
public class TestCoprocessor extends BaseRegionObserver {
    private ThreadLocal<HTableInterface> indexTableLocal = new ThreadLocal<HTableInterface>();
    Set<HTable> indexTables = new HashSet<HTable>();
    private CoprocessorEnvironment env;

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
        this.env = env;
    }

    @Override
    public void postPut(
            final ObserverContext<RegionCoprocessorEnvironment> observerContext,
            final Put put,
            final WALEdit edit,
            final boolean writeToWAL)
            throws IOException {
        try {
            HTable indexTable = (HTable) indexTableLocal.get();
            if (indexTable == null) {
                indexTable = new HTable(env.getConfiguration(), Constants.INDEX_TABLE);
                indexTableLocal.set(indexTable);
                synchronized (indexTables) {
                    indexTables.add(indexTable);
                }
            }
            final List<KeyValue> filteredList = put.get(Constants.FAMILY_NAME, Constants.INDEXED_QUALIFIER_NAME);
            byte[] index = filteredList.get(0).getValue();
            Put indexPut = new Put(index);
            indexPut.add(Constants.FAMILY_NAME, Constants.QUALIFIER_NAME, put.getRow());
            indexTable.put(indexPut);
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void preGet(ObserverContext<RegionCoprocessorEnvironment> e, Get get, List<KeyValue> results) throws IOException {
        super.preGet(e, get, results);    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public void stop(CoprocessorEnvironment env) throws IOException {
        synchronized (indexTables) {
            for (HTable indexTable : indexTables) {
                indexTable.close();
            }
        }
    }
}
