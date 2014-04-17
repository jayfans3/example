package com.ailk.oci.ocnosql.client.importdata;

import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class OciHTableUtil {

    private static final int INITIAL_LIST_SIZE = 500;
    private static final int FLUSH_THRESHOLD = 500;
    private static final Map<byte[],List<Put>> unFlushedPuts = new HashMap<byte[], List<Put>>();

    public static void bucketRsPut(HTable htable, List<Put> puts, boolean forceFlush) throws IOException {
        List<Put> cachePut = unFlushedPuts.get(htable.getTableName());
        if(cachePut!=null&&cachePut.size()!=0){
            puts.addAll(cachePut);
        }
        Map<String, List<Put>> putMap = createRsPutMap(htable, puts);
        for (List<Put> rsPuts: putMap.values()) {
            if(rsPuts.size()<FLUSH_THRESHOLD&&!forceFlush){
                unFlushedPuts.put(htable.getTableName(),rsPuts);
            }
            else{
                htable.put(rsPuts);
            }
        }
        htable.flushCommits();
    }
    private static Map<String,List<Put>> createRsPutMap(HTable htable, List<Put> puts) throws IOException {

        Map<String, List<Put>> putMap = new HashMap<String, List<Put>>();
        for (Put put: puts) {
            HRegionLocation rl = htable.getRegionLocation( put.getRow() );
            String hostname = rl.getHostname();
            List<Put> recs = putMap.get(hostname);
            if (recs == null) {
                recs = new ArrayList<Put>(INITIAL_LIST_SIZE);
                putMap.put( hostname, recs);
            }
            recs.add(put);
        }
        return putMap;
    }
}
