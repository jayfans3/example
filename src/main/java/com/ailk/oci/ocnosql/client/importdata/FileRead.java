package com.ailk.oci.ocnosql.client.importdata;

import com.ailk.oci.ocnosql.client.config.spi.Connection;
import com.ailk.oci.ocnosql.client.rowkeygenerator.BusiRowKeyGenerator;
import com.ailk.oci.ocnosql.client.rowkeygenerator.RowKeyGenerator;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import com.ailk.oci.ocnosql.client.util.HTableUtilsV2;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class FileRead implements Runnable {
    private static final Log log = LogFactory.getLog(FileRead.class.getSimpleName());
    private static final Log errorlog = LogFactory.getLog("valiationLog");
    private static int maxPutNum = Integer.parseInt(Connection.getInstance().get("hbase.maxPutNum", "1000"));
    private static boolean isWriteToWAL = Boolean.parseBoolean(Connection.getInstance().get("hbase.isWriteToWAL", "true"));
    private static boolean isCreateTableByMonth = Boolean.parseBoolean(Connection.getInstance().get("hbase.isCreateTableByMonth", "false"));
    private static boolean isCreateIndexTable = Boolean.parseBoolean(Connection.getInstance().get("hbase.isCreateIndexTable", "true"));
    private final static String SEPARATOR =Connection.getInstance().get("hbase.put.separator", ";");
    private static HashMap<String, Integer> columNum = new HashMap();
    private RowKeyGenerator rowkeyGenerator = new BusiRowKeyGenerator();
    private static final Random randomSeed = new Random(System.currentTimeMillis());

    protected final Random rand = new Random(nextRandomSeed());
    private int telNumIndex;
    private int oppNumIndex;
    private int timeZoneIndexInt;
    private String posIndex;
    private File f;
    protected CountDownLatch runningThreadNum;

    static {
        Map<String, String> busiMapConf = Connection.getInstance().retrieveValueByPrefix("put.columSize");
        for (Map.Entry<String, String> entry : busiMapConf.entrySet()) {
            columNum.put(entry.getKey().replace("put.columSize.", ""), Integer.parseInt(entry.getValue()));
        }
    }

    private static long nextRandomSeed() {
        return randomSeed.nextLong();
    }

    public FileRead(CountDownLatch runningThreadNum,File f, int telNumIndex, int oppNumIndex, int timeZoneindex, String posIndex) {
        this.runningThreadNum = runningThreadNum;
        this.f = f;
        this.telNumIndex = telNumIndex;
        this.oppNumIndex = oppNumIndex;
        this.timeZoneIndexInt = timeZoneindex;
        this.posIndex = posIndex;
    }

    @Override
    public void run() {
        try {
            readFile(this.f);
        }
        catch (Exception e) {
            String path = getPathName(f.getPath());
            String newPath = path + "error/" + f.getName().replaceAll(".tmp","");;
            f.renameTo(new File(newPath));
            errorlog.error("failed read file[" + f.getName() + "]", e);
        }
        finally {
            runningThreadNum.countDown();
        }
    }

    private void readFile(File f) throws Exception {
        long startTime = System.currentTimeMillis();
        String fileName = f.getName();
        Configuration conf = Connection.getInstance().getConf();
        String tableName = Connection.getInstance().get("hbase.tablename", "drquery");
        String timeIndexTablname = Connection.getInstance().get("hbase.timeIndexTablname", "drquery_time_index");
        String destNumTableStr = Connection.getInstance().get("hbase.destNumTable", "drquery_destNum_index");
        int writeBufferSize = Integer.parseInt(Connection.getInstance().get("hbase.writeBufferSize", "2048")) * 1024;
        if (isCreateTableByMonth) {
            String time = StringUtils.split(fileName, "_")[1];
            if (time.length() > 6) {
                time = StringUtils.substring(time, 0, 6);
            }
            tableName = tableName.concat("_").concat(time);
            timeIndexTablname = timeIndexTablname.concat("_").concat(time);
            destNumTableStr = destNumTableStr.concat("_").concat(time);
        }
        HTableInterface table = HTableUtilsV2.getTable(conf, tableName);
        table.setWriteBufferSize(writeBufferSize);
        table.setAutoFlush(false);
        HTableInterface timeIndexTable = null;
        HTableInterface destNumTable = null;
        if(isCreateIndexTable){
            timeIndexTable = HTableUtilsV2.getTable(conf, timeIndexTablname);
            timeIndexTable.setWriteBufferSize(writeBufferSize);
            timeIndexTable.setAutoFlush(false);
            destNumTable = HTableUtilsV2.getTable(conf, destNumTableStr);
            destNumTable.setWriteBufferSize(writeBufferSize);
            destNumTable.setAutoFlush(false);
        }
        byte[] cf = Bytes.toBytes("f");
        byte[] col = Bytes.toBytes("c");

        List put4Table = new ArrayList(maxPutNum);
        List put4TimeIndexTable = new ArrayList(maxPutNum);
        List put4DestNumTable = new ArrayList(maxPutNum);

        BufferedInputStream bis = new BufferedInputStream(new FileInputStream(f));
        BufferedReader in = new BufferedReader(new InputStreamReader(bis, "utf-8"), 20971520);
        String busiName = StringUtils.split(fileName, "_")[0];

        int[] posIndexArr = convertPosIndex();
        boolean isFoundError = false;
        Map<String, HashSet<String>> timeZone = new HashMap<String, HashSet<String>>();
        try {
            int rowCount=0;
            int index = 0;
            String currentRowStr;
            while ((currentRowStr = in.readLine()) != null) {
                if (index < maxPutNum) {
                    String[] currentRow = StringUtils.splitByWholeSeparatorPreserveAllTokens(currentRowStr, SEPARATOR);
                    if (currentRow.length != columNum.get(busiName)) {
                        errorlog.error("This row[" + index + "]'s length["+currentRow.length+"] isn't equal with valid length[" + columNum.get(busiName) + "]");
                        isFoundError = true;
                        index++;
                        continue;
                    }
                    String rowkey = this.rowkeyGenerator.generate(currentRow[this.telNumIndex], currentRow[this.telNumIndex], currentRow, posIndexArr, busiName);
                    HTableBean hTableBean = buildHtableBean(null, rowkey, cf, col, currentRowStr);
                    Put tablePut = this.createPut(hTableBean);
                    put4Table.add(tablePut);

                    if(isCreateIndexTable){
                        String destNumTableRowkey = this.rowkeyGenerator.generate(currentRow[this.oppNumIndex], currentRow[this.oppNumIndex], currentRow, posIndexArr, busiName);
                        HTableBean destNumTablebean = buildHtableBean(null, destNumTableRowkey, cf, col, rowkey);
                        Put destNumTablePut = this.createPut(destNumTablebean);
                        put4DestNumTable.add(destNumTablePut);
                        generateTimeZoneData(timeZone, currentRow, busiName, currentRow[this.timeZoneIndexInt]);
                    }

                    index++;
                    rowCount++;
                }
                while (true){
                    try{
                        table.put(put4Table);
                        break;
                    }
                    catch (IOException e){
                        Thread.sleep(100);
                    }
                }
                put4Table = new ArrayList(maxPutNum);
                if(isCreateIndexTable){
                    while (true){
                        try{
                            destNumTable.put(put4DestNumTable);

                            break;
                        }
                        catch (IOException e){
                            Thread.sleep(100);
                        }
                    }
                    put4DestNumTable = new ArrayList(maxPutNum);
                }
                index = 0;
            }

            while (true){
                try{
                    table.put(put4Table);
                    table.flushCommits();
                    break;
                }
                catch (IOException e){
                    Thread.sleep(100);
                }
            }
            put4Table.clear();
            if(isCreateIndexTable){
                byte[] timecol = generateCellName(this.rand);
                ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                for (Map.Entry<String, HashSet<String>> entry : timeZone.entrySet()) {
                    HashSet<String> values = entry.getValue();
                    byteArrayOutputStream.reset();
                    for (String metaValue : values) {
                        byteArrayOutputStream.write(",".getBytes());
                        byteArrayOutputStream.write(metaValue.getBytes());
                    }
                    HTableBean timeIndexTablnamebean = buildHtableBean(null, entry.getKey(), cf, timecol, byteArrayOutputStream.toString());
                    Put timetablePut = this.createPut(timeIndexTablnamebean);
                    put4TimeIndexTable.add(timetablePut);
                }
                while (true){
                    try{
                        timeIndexTable.put(put4TimeIndexTable);
                        timeIndexTable.flushCommits();
                        break;
                    }
                    catch (IOException e){
                        Thread.sleep(100);
                    }
                }

                while (true){
                    try{
                        destNumTable.put(put4DestNumTable);
                        destNumTable.flushCommits();
                        break;
                    }
                    catch (IOException e){
                        Thread.sleep(100);
                    }
                }
            }
            in.close();
            long elapsedTime = System.currentTimeMillis()-startTime;
            if(log.isInfoEnabled()){
                log.info("Put file["+fileName+"] elapsed time:"+elapsedTime+"ms,put row size["+rowCount+"]");
            }
        }
        finally {
            table.close();
            if(isCreateIndexTable){
                timeIndexTable.close();
                destNumTable.close();
            }
        }
        if (isFoundError) {
            String path = getPathName(f.getPath());
            String newPath = path + "error/" + fileName.replaceAll(".tmp","");
            f.renameTo(new File(newPath));
        } else {
            String path = getPathName(f.getPath());
            String newPath = path + "bak/" + fileName.replaceAll(".tmp","");
            f.renameTo(new File(newPath));
        }

    }

    private String getPathName(String path) {
        return path.substring(0, path.lastIndexOf("/") + 1);
    }

    private void generateTimeZoneData(Map<String, HashSet<String>> timeZone, String[] currentRow, String busiName, String time) {
        String timeIndexTablnameRowkey = this.rowkeyGenerator.generate(time, null, currentRow, null, busiName);
        String callNumber = currentRow[this.telNumIndex];
        HashSet rowValues = (HashSet) timeZone.get(timeIndexTablnameRowkey);
        if (rowValues != null) {
            rowValues.add(callNumber);
        } else {
            rowValues = new HashSet();
            rowValues.add(callNumber);
            timeZone.put(timeIndexTablnameRowkey, rowValues);
        }
    }

    private int[] convertPosIndex() {
        this.posIndex = (this.timeZoneIndexInt + ";" + this.posIndex);
        int[] posIndexArr = new int[org.apache.commons.lang.StringUtils.countMatches(this.posIndex, ";") + 1];
        int arrIndex = 0;
        for (String pos : org.apache.commons.lang3.StringUtils.split(this.posIndex, ";")) {
            posIndexArr[arrIndex] = Integer.parseInt(pos);
            arrIndex++;
        }
        return posIndexArr;
    }

    private HTableBean buildHtableBean(String tableName, String rowKey, byte[] cf, byte[] col, String tempString) {
        HTableBean hTableBean = new HTableBean();
        hTableBean.setTableName(tableName);
        ArrayList cfs = new ArrayList();
        cfs.add(cf);
        hTableBean.setCfNames(cfs);

        ArrayList cols = new ArrayList();
        cols.add(col);
        hTableBean.setColNames(cols);

        hTableBean.setCurrentRowData(tempString);
        hTableBean.setRowkeyValue(rowKey);
        return hTableBean;
    }

    private Put createPut(HTableBean hTableBean) {
        Put put = new Put(Bytes.toBytes(hTableBean.getRowkeyValue()));
        ArrayList<byte[]> cfs = hTableBean.getCfNames();
        String currentData = hTableBean.getCurrentRowData();
        for (byte[] cf : cfs) {
            put.add(cf, hTableBean.getColNames().get(0), Bytes.toBytes(currentData == null ? "" : currentData));
        }
        put.setWriteToWAL(isWriteToWAL);
        return put;
    }

    private static byte[] generateCellName(Random r) {
        byte[] b = new byte[10];
        int i = 0;

        for (i = 0; i < 2; i += 8) {
            b[i] = (byte) (65 + r.nextInt(26));
            b[(i + 1)] = b[i];
            b[(i + 2)] = b[i];
            b[(i + 3)] = b[i];
            b[(i + 4)] = b[i];
            b[(i + 5)] = b[i];
            b[(i + 6)] = b[i];
            b[(i + 7)] = b[i];
        }

        byte a = (byte) (65 + r.nextInt(26));
        for (; i < 10; i++) {
            b[i] = a;
        }
        return b;
    }
}
