/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ailk.oci.ocnosql.client.importdata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.Compression;

/**
 *
 * @author tianyi
 */
public class TestPut extends Thread {

    private static int ONE_PUTS_SIZE = 2000;
    private static int ONE_THREAD_SIZE = 50;
    private static int THREAD_NUM = 20;
    private static int ROWKEY_NUMBER = 20;
    private static int RREGION_NUMBER = 20;
    private static int VALUE_LENGTH = 40;
    private static byte[] FAMILY = "f".getBytes();
    private static byte[] CELL = "c".getBytes();
    private static byte[] TABLE = "tianyi".getBytes();
    private static final Log log = LogFactory.getLog(TestPut.class.getSimpleName());

    private static void parseCommand(String[] args) {
        for (String arg : args) {
            if (arg.startsWith("--table")) {
                TABLE = arg.split("=")[1].getBytes();
            }
            if (arg.startsWith("--cell")) {
                CELL = arg.split("=")[1].getBytes();
            }
            if (arg.startsWith("--family")) {
                FAMILY = arg.split("=")[1].getBytes();
            }
            if (arg.startsWith("--rowkey-num")) {
                ROWKEY_NUMBER = Integer.parseInt(arg.split("=")[1]);
            }
            if (arg.startsWith("--value-length")) {
                VALUE_LENGTH = Integer.parseInt(arg.split("=")[1]);
            }
            if (arg.startsWith("--region-num")) {
                RREGION_NUMBER = Integer.parseInt(arg.split("=")[1]);
            }
            if (arg.startsWith("--thread-num")) {
                THREAD_NUM = Integer.parseInt(arg.split("=")[1]);
            }
            if (arg.startsWith("--thread-onceput")) {
                ONE_PUTS_SIZE = Integer.parseInt(arg.split("=")[1]);
            }
            if (arg.startsWith("--thread-totalput")) {
                ONE_THREAD_SIZE = Integer.parseInt(arg.split("=")[1]);
            }
            if (arg.startsWith("--help")) {
                System.out.println("usage: --table=tianyi --cell=c --family=f --rowkey-num=20 --value-length=40 --region-num=20 --thread-num=20 --thread-onceput=2000 --thread-totalput=50");
                System.exit(0);
            }
        }
    }

    private static String randomString(int length) {
        char[] wordChar = new char[length];
        for (int j = 0; j < wordChar.length; j++) {
            wordChar[j] = (char) (Math.random() * 26 + 97);
        }
        return new String(wordChar);
    }

    private static String randomIntegerHexString(int stop) {
        return StringUtils.reverse(Integer.toHexString((int) (Math.random() * stop)));
    }
    
    public static byte[][] splits(int numRegions) {
        int total = 0;
        int length = 0;
        if (numRegions < 3) {
            throw new UnsupportedOperationException("the number of ranges must more than 3");
        } 
        else if (numRegions < 16) {
            total = 16 * 16;
            length = 2;
        }
        else if (numRegions < 16 * 16) {
            total = 16 * 16 * 16;
            length = 3;
        } 
        else if (numRegions < 16 * 16 * 16) {
            total = 16 * 16 * 16 * 16;
            length = 4;
        } 
        else {
            throw new UnsupportedOperationException("not support more than 16 * 16 * 16 of ranges");
        }
        byte[][] result = new byte[numRegions][length];
        float div = total / (numRegions-1);
        for (int i = 0; i < numRegions-1; i++) {
            int curr = (int) (i * div);
            byte[] temp = Integer.toHexString(curr).getBytes();
            if (temp.length < length) {
                int j = 0;
                for (; j < length - temp.length; j++) {
                    result[i][j] = '0';
                }
                for (int k = 0; k < temp.length; k++) {
                    result[i][j] = temp[k];
                    j++;
                }
            } else {
                result[i] = temp;
            }
        }
        for(int i = 0;i<length;i++) {
            result[numRegions-1][i] = 'f';
        }
        
        return result;
    }

    private static void createTableIfNotExist() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        HBaseAdmin admin = new HBaseAdmin(conf);
        if (admin.tableExists(TABLE)) {
            System.out.println("table : " + TABLE + " already exist.");
            return;
        }

        HTableDescriptor tableDesc = new HTableDescriptor(TABLE);
        HColumnDescriptor columnDesc = new HColumnDescriptor(FAMILY);
        columnDesc.setCompactionCompressionType(Compression.Algorithm.SNAPPY);
        columnDesc.setMaxVersions(1);
        columnDesc.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF);
        columnDesc.setCompactionCompressionType(Compression.Algorithm.SNAPPY);
        tableDesc.addFamily(columnDesc);
        admin.createTable(tableDesc, splits(RREGION_NUMBER));
        System.out.println("table : " + TABLE + " created");
    }

    @Override
    public void run() {
        try {
            Configuration conf = HBaseConfiguration.create();
            HTable table = new HTable(conf, TABLE);
            table.setAutoFlush(false);
            for (int j = 0; j < ONE_THREAD_SIZE; j++) {
                log.info("start put [" + ONE_PUTS_SIZE + "] records");
                long start = System.currentTimeMillis();
                List<Put> list = new ArrayList<Put>();
                for (int i = 0; i < ONE_PUTS_SIZE; i++) {
                    String key = randomIntegerHexString(ROWKEY_NUMBER);
                    String value = randomString(VALUE_LENGTH);
                    Put put = new Put(key.getBytes());
                    put.add(FAMILY, CELL, value.getBytes());
                    list.add(put);
                }
                table.put(list);
                table.flushCommits();

                long stop = System.currentTimeMillis();
                log.info("finish put [" + ONE_PUTS_SIZE + "] records, use [" + (stop - start) + "] ms");
            }
        } catch (Exception e) {
            System.err.println("failed put record" + e);
        }
    }

    public static void main(String[] args) throws InterruptedException, IOException {

        parseCommand(args);
        createTableIfNotExist();
        List<TestPut> threads = new ArrayList<TestPut>();
        long start = System.currentTimeMillis();
        for (int i = 0; i < THREAD_NUM; i++) {
            TestPut thread = new TestPut();
            thread.start();
            threads.add(thread);
        }
        for (int i = 0; i < THREAD_NUM; i++) {
            threads.get(i).join();
        }
        long stop = System.currentTimeMillis();
        System.out.println("total put [" + THREAD_NUM * ONE_THREAD_SIZE * ONE_PUTS_SIZE + "] records");
        System.out.println("total use [" + (stop - start) / 1000 + "] seconds");
        System.out.println("avg put [" + THREAD_NUM * ONE_THREAD_SIZE * ONE_PUTS_SIZE / ((stop - start) / 1000) + "] per second");
    }
}
