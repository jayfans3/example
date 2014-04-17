package com.ailk.oci.ocnosql.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created with IntelliJ IDEA.
 * User: shaoaq
 * Date: 13-5-18
 * Time: 下午6:14
 * To change this template use File | Settings | File Templates.
 */
public class CoprocessorTest extends Configured implements Tool {

    private static int ThreadCount = 10;
    private static int RowCount = 100 * 10000;
    private static int ROW_LENGTH = 1000;
    private static boolean USE_COPROCESSOR = true;


    @Override
    public int run(String[] strings) throws Exception {
        main(strings);
        return 0;
    }

    public static void main(String[] args) throws IOException, InterruptedException, URISyntaxException {
        for (String arg : args) {
            if (arg.startsWith("--thread=")) {
                ThreadCount = Integer.parseInt(arg.substring("--thread=".length()));
            }
            if (arg.startsWith("--rows=")) {
                RowCount = Integer.parseInt(arg.substring("--rows=".length()));
            }
            if (arg.startsWith("--rowLen=")) {
                ROW_LENGTH = Integer.parseInt(arg.substring("--rowLen=".length()));
            }
            if (arg.startsWith("--useCoprocessor=")) {
                USE_COPROCESSOR = Boolean.parseBoolean(arg.substring("--useCoprocessor=".length()));
            }
        }
        System.out.println("./hbase com.ailk.oci.ocnosql.CoprocessorTest --thread=" + ThreadCount + " --rows=" + RowCount + " --rowLen=" + ROW_LENGTH + " --useCoprocessor=" + USE_COPROCESSOR);
        Configuration conf = HBaseConfiguration.create();
        HBaseAdmin admin = new HBaseAdmin(conf);
        createTable(admin);
        CountDownLatch latch = new CountDownLatch(RowCount);
        Thread[] threads = new Thread[ThreadCount];
        AtomicInteger countDown = new AtomicInteger(RowCount);
        for (int i = 0; i < ThreadCount; i++) {
            threads[i] = new PutThread(conf, countDown, RowCount);
        }
        long begin = System.currentTimeMillis();
        for (int i = 0; i < ThreadCount; i++) {
            threads[i].start();
        }
        for (int i = 0; i < ThreadCount; i++) {
            threads[i].join();
        }
        System.out.println("rows[" + RowCount + "] cost " + (System.currentTimeMillis() - begin) / 1000 + "s");
    }

    private static void createTable(HBaseAdmin admin) throws IOException, URISyntaxException {
        if (admin.tableExists("TestTable2")) {
            System.out.println("表已经存在！先删除");
            admin.disableTable(Constants.TableName);
            admin.deleteTable(Constants.TableName);
            admin.disableTable(Constants.INDEX_TABLE);
            admin.deleteTable(Constants.INDEX_TABLE);
            System.out.println("表删除成功！");
        }
        HTableDescriptor tableDesc = new HTableDescriptor(Constants.TableName);
        HColumnDescriptor info = new HColumnDescriptor("info");
        tableDesc.addFamily(info);
        if (USE_COPROCESSOR) {
            URI pathURI = CoprocessorTest.class.getProtectionDomain().getCodeSource().getLocation().toURI();
            Path path = new Path(pathURI);
            System.out.println(pathURI);
            tableDesc.addCoprocessor(TestCoprocessor.class.getName(), path, Coprocessor.PRIORITY_USER, null);
        }
        admin.createTable(tableDesc);
        HTableDescriptor indexTableDesc = new HTableDescriptor(Constants.INDEX_TABLE);
        info = new HColumnDescriptor("info");
        indexTableDesc.addFamily(info);
        admin.createTable(indexTableDesc);
        System.out.println("表创建成功！");
    }

    private static class PutThread extends Thread {
        static final Random rand = new Random(new Random(System.currentTimeMillis()).nextLong());
        private Configuration conf;
        private AtomicInteger countDown;
        private int rowCount;
        private HTable table;
        private HTable indexTable;

        private PutThread(Configuration conf, AtomicInteger countDown, int rowCount) {
            this.conf = conf;
            this.countDown = countDown;
            this.rowCount = rowCount;
            try {
                table = new HTable(conf, Constants.TableName);
                table.setAutoFlush(false);
                if (!CoprocessorTest.USE_COPROCESSOR) {
                    indexTable = new HTable(conf, Constants.INDEX_TABLE);
                    indexTable.setAutoFlush(false);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void run() {
            for (int i = countDown.decrementAndGet(); i > 0; i = countDown.decrementAndGet()) {
                if (i % 10000 == 0) {
                    System.out.println(i);
                }
                try {
                    byte[] row = format((rand.nextInt(Integer.MAX_VALUE) - i) % rowCount);
                    Put put = new Put(row);
                    byte[] value = generateValue(this.rand);
                    put.add(Constants.FAMILY_NAME, Constants.QUALIFIER_NAME, value);
                    byte[] indexCol = format((rand.nextInt(Integer.MAX_VALUE) - i * 2) % rowCount);
                    put.add(Constants.FAMILY_NAME, Constants.INDEXED_QUALIFIER_NAME, indexCol);
                    table.put(put);
                    if (indexTable != null) {
                        Put indexPut = new Put(indexCol);
                        indexPut.add(Constants.FAMILY_NAME, Constants.QUALIFIER_NAME, row);
                        indexTable.put(indexPut);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            try {
                table.close();
            } catch (IOException e) {
            }
            if (indexTable != null) {
                try {
                    indexTable.close();
                } catch (IOException e) {
                }
            }
        }

        public static byte[] format(final int number) {
            byte[] b = new byte[10];
            int d = Math.abs(number);
            for (int i = b.length - 1; i >= 0; i--) {
                b[i] = (byte) ((d % 10) + '0');
                d /= 10;
            }
            return b;
        }

        public static byte[] generateValue(final Random r) {
            byte[] b = new byte[ROW_LENGTH];
            int i = 0;

            for (i = 0; i < (ROW_LENGTH - 8); i += 8) {
                b[i] = (byte) (65 + r.nextInt(26));
                b[i + 1] = b[i];
                b[i + 2] = b[i];
                b[i + 3] = b[i];
                b[i + 4] = b[i];
                b[i + 5] = b[i];
                b[i + 6] = b[i];
                b[i + 7] = b[i];
            }

            byte a = (byte) (65 + r.nextInt(26));
            for (; i < ROW_LENGTH; i++) {
                b[i] = a;
            }
            return b;
        }
    }
}
