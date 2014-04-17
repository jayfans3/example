package com.ailk.oci.ocnosql.client.importdata;

import com.ailk.oci.ocnosql.client.config.spi.Connection;
import com.ailk.oci.ocnosql.client.rowkeygenerator.BusiRowKeyGenerator;
import com.ailk.oci.ocnosql.client.rowkeygenerator.RowKeyGenerator;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;

public class PutThread implements Runnable {
    private static final Log log = LogFactory.getLog(PutThread.class.getSimpleName());
    private int telNumIndex;
    private int oppNumIndex;
    private String timeZoneindex;
    private String posIndex;
    private ArrayBlockingQueue<String[]> fileContentInMem;
    private RowKeyGenerator rowkeyGenerator;
    private static boolean isWriteToWAL = Boolean.parseBoolean(Connection.getInstance().get("hbase.isWriteToWAL", "true"));
    public PutThread(int telNumIndex, int oppNumIndex, String timeZoneindex, String posIndex, ArrayBlockingQueue<String[]> fileContentInMem) {
        this.telNumIndex = telNumIndex;
        this.oppNumIndex = oppNumIndex;
        this.timeZoneindex = timeZoneindex;
        this.posIndex = posIndex;
        this.fileContentInMem=fileContentInMem;
    }

    @Override
    public void run() {
        try {
//            putDataFromFile();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

//    public void putDataFromFile() throws Exception {
//
//        posIndex = timeZoneindex + ";" + posIndex;
//        int[] posIndexArr = new int[StringUtils.countMatches(posIndex, ";") + 1];
//        int arrIndex = 0;
//        for (String pos : StringUtils.split(posIndex, ";")) {
//            posIndexArr[arrIndex] = Integer.parseInt(pos);
//            arrIndex++;
//        }
//        rowkeyGenerator = new BusiRowKeyGenerator();//RowKeyGeneratorHolder.resolveGenerator("busi");
//        Configuration conf = Connection.getInstance().getConf();
//
//        HTable table = new HTable(conf, Connection.getInstance().get("hbase.tablename", "drquery"));
//        table.setWriteBufferSize(Integer.parseInt(Connection.getInstance().get("hbase.writeBufferSize", "2048")) * 1024);
//        table.setAutoFlush(false);
//        HTable timeIndexTable = new HTable(conf, Connection.getInstance().get("hbase.timeIndexTablname", "drquery_time_index"));
//        timeIndexTable.setWriteBufferSize(Integer.parseInt(Connection.getInstance().get("hbase.writeBufferSize", "2048")) * 1024);
//        timeIndexTable.setAutoFlush(false);
//        HTable destNumTable = new HTable(conf, Connection.getInstance().get("hbase.destNumTable", "drquery_destNum_index"));
//        destNumTable.setWriteBufferSize(Integer.parseInt(Connection.getInstance().get("hbase.writeBufferSize", "2048")) * 1024);
//        destNumTable.setAutoFlush(false);
//        byte[] cf = Bytes.toBytes("f");
//        byte[] col = Bytes.toBytes("c");
//        List<Put> put4Table = new ArrayList<Put>();
//        List<Put> put4TimeIndexTable = new ArrayList<Put>();
//        List<Put> put4DestNumTable = new ArrayList<Put>();
//        int line = 0;
//        String[] currentRow;
//        long currentTime = System.currentTimeMillis();
//        int timeZoneIndexInt = Integer.parseInt(timeZoneindex);
//        long startTime = System.currentTimeMillis();
//        for (; ; ) {
//            String[] batchData = fileContentInMem.take();
//            if (batchData[0].equals("end")) {
//                fileContentInMem.put(new String[]{"end"});
//                break;
//            }
//            for(int index=0;index<batchData.length;index++){
//                String currentRowStr = batchData[index];
//                if(StringUtils.isEmpty(currentRowStr)){
//                    continue;
//                }
//                currentRow = StringUtils.splitByWholeSeparatorPreserveAllTokens(currentRowStr, ";");
//
//                String fileName = currentRow[currentRow.length-1];
//                String busiName = StringUtils.split(fileName, "_")[0];
//
//                String rowkey = rowkeyGenerator.generate(currentRow[telNumIndex], currentRow[telNumIndex], currentRow, posIndexArr, busiName);
//                HTableBean hTableBean = buildHtableBean(null, Bytes.toBytes(rowkey), cf, col, currentRowStr);
//                put4Table.add(this.createPut(hTableBean));
//
//                String destNumTableRowkey = rowkeyGenerator.generate(currentRow[oppNumIndex], currentRow[oppNumIndex], currentRow, posIndexArr, busiName);
//                HTableBean destNumTablebean = buildHtableBean(null, Bytes.toBytes(destNumTableRowkey), cf, col, rowkey);
//                put4DestNumTable.add(this.createPut(destNumTablebean));
//
//                String time = currentRow[timeZoneIndexInt];
//                String hashValue = null;
//                if (time.length() > 10) {
//                    time = StringUtils.substring(time, 0, 10);
//                    hashValue = StringUtils.substring(time, 0, 6);
//                }
//                else {
//                    log.error("Bad time format[" + time + "] in line[" + line + "]:" + currentRowStr);
//                }
//                String timeIndexTablnameRowkey = rowkeyGenerator.generate(time, hashValue, currentRow, new int[]{telNumIndex}, busiName);
//                HTableBean timeIndexTablnamebean = buildHtableBean(null, Bytes.toBytes(timeIndexTablnameRowkey), cf, col, currentRow[telNumIndex]);
//                put4TimeIndexTable.add(this.createPut(timeIndexTablnamebean));
//                line++;
//            }
//            long elapsedTime = System.currentTimeMillis()-startTime;
//            log.info("elapsed time:"+elapsedTime+"ms,put size["+batchData.length+"]");
//            batchData=null;//help gc
//            startTime = System.currentTimeMillis();
//            table.put(put4Table);
//            table.flushCommits();
//            put4Table.clear();
//
//            timeIndexTable.put(put4TimeIndexTable);
//            timeIndexTable.flushCommits();
//            put4TimeIndexTable.clear();
//
//            destNumTable.put(put4DestNumTable);
//            destNumTable.flushCommits();
//            put4DestNumTable.clear();
//
//        }
//
//        long elapsedTime = System.currentTimeMillis()-currentTime;
//        log.info("This thread elapsed time:"+elapsedTime+"ms,put size["+line+"]");
//    }

//    /**
//     * 插入一行记录
//     */
//    private Put createPut(HTableBean hTableBean) {
//        Put put = new Put(hTableBean.getRowkeyValue());
//        String currentData = hTableBean.getCurrentRowData();
//        put.add(hTableBean.getCfNames(), hTableBean.getColNames(), Bytes.toBytes(currentData == null ? "" : currentData));
//        put.setWriteToWAL(isWriteToWAL);
//        return put;
//    }
//
//    private HTableBean buildHtableBean(String tableName, byte[] rowKey, byte[] cf, byte[] col, String tempString) {
//        HTableBean hTableBean = new HTableBean();
//        hTableBean.setTableName(tableName);
//        hTableBean.setCfNames(cf);
//        hTableBean.setColNames(col);
//
//        hTableBean.setCurrentRowData(tempString);
//        hTableBean.setRowkeyValue(rowKey);
//        return hTableBean;
//    }
}