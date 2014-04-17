package com.ailk.oci.ocnosql.client.importdata;


import com.ailk.oci.ocnosql.client.config.spi.Connection;
import com.ailk.oci.ocnosql.client.rowkeygenerator.BusiRowKeyGenerator;
import com.ailk.oci.ocnosql.client.rowkeygenerator.RowKeyGenerator;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.ByteArrayOutputStream;
import java.util.*;

public class HbasePutUtil {
    private final static Log log = LogFactory.getLog(HbasePutUtil.class);
    private RowKeyGenerator rowkeyGenerator = new BusiRowKeyGenerator();
    private final static String SEPARATOR =Connection.getInstance().get("hbase.put.separator", ";");
    private final static byte[] tableName = Bytes.toBytes(Connection.getInstance().get("hbase.tablename", "drquery"));
    private final static byte[] timeIndexTablname = Bytes.toBytes(Connection.getInstance().get("hbase.timeIndexTablname", "drquery_time_index"));
    private final static byte[] destNumTable = Bytes.toBytes(Connection.getInstance().get("hbase.destNumTable", "drquery_destNum_index"));
    private static boolean isCreateIndexTable = Boolean.parseBoolean(Connection.getInstance().get("hbase.isCreateIndexTable", "true"));
    private final static byte[] cf = Bytes.toBytes("f");
    private final static byte[] col = Bytes.toBytes("c");
    private final static Map<String,int[]> cacheBuisIndex = new HashMap<String, int[]>();//busiName,index

    public List<TablePutPair> createPut(String fileName,String rowConent){
        int[] index=null;
        String busiName=null;
        List<TablePutPair> list = new ArrayList<TablePutPair>();
        try {
            busiName = StringUtils.split(fileName,"_")[0];
            index = cacheBuisIndex.get(busiName);
            if(index==null){
                String[] strIndex = StringUtils.split(Connection.getInstance().get("param.buis.".concat(busiName),""),",");
                index = new int[strIndex.length];
                for(int i=0;i<strIndex.length;i++){
                    index[i] =  Integer.parseInt(strIndex[i]);
                }
                cacheBuisIndex.put(busiName,index);
            }
            String[] currentRow = StringUtils.splitByWholeSeparatorPreserveAllTokens(rowConent, SEPARATOR);
            String rowkey = this.rowkeyGenerator.generate(currentRow[index[0]], currentRow[index[0]], currentRow, new int[]{index[2]}, busiName);
            TablePutPair tableNamedata = new TablePutPair(tableName,createNativePut(Bytes.toBytes(rowkey),rowConent));
            list.add(tableNamedata);
            if(isCreateIndexTable){
                String destNumTableRowkey = rowkeyGenerator.generate(currentRow[index[1]], currentRow[index[1]], currentRow, new int[]{index[2]}, busiName);
                TablePutPair destNumTabledata = new TablePutPair(destNumTable,createNativePut(Bytes.toBytes(destNumTableRowkey),rowkey));
                list.add(destNumTabledata);
                String timeIndexTablnameRowkey = rowkeyGenerator.generate(currentRow[index[2]], null, currentRow, new int[]{index[0]}, busiName);
                TablePutPair timeIndexTabledata = new TablePutPair(timeIndexTablname,createNativePut(Bytes.toBytes(timeIndexTablnameRowkey), currentRow[index[0]]));
                list.add(timeIndexTabledata);
            }
            return list;
        } catch (Throwable e) {
            throw new RuntimeException("fileName:["+fileName+"],rowConent:["+rowConent+"],busiName:["+busiName+"],index.length["+index.length+"]",e);
        }
    }
    private Put createNativePut(byte[] rowKey,String currentData){
        Put put = new Put(rowKey);
        put.add(cf, col, Bytes.toBytes(currentData));
        return put;
    }


    public List<byte[]> retrieveTablesName(){
        List<byte[]> tablesName = new ArrayList<byte[]>();
        tablesName.add(tableName);
        if(isCreateIndexTable){
            tablesName.add(timeIndexTablname);
            tablesName.add(destNumTable);
        }
        return tablesName;
    }

}
