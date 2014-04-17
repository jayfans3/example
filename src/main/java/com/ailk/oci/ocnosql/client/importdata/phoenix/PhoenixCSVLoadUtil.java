package com.ailk.oci.ocnosql.client.importdata.phoenix;

import java.io.File;
import java.io.FilenameFilter;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.ailk.oci.ocnosql.client.config.spi.Connection;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 该工具类封装了com.salesforce.phoenix.util.PhoenixRuntime的mian方法以实现数据导入<br>
 * 跟直接使用psql.sh效果一样,底层调用的CSVLoader，实际上是将csv文件的数据以sql语句的方式导入hbase，索引数据会自动生成的。
 * 注意：本工具类不支持hdfs的csv文件数据的导入
 *
 * @author lifei5
 */
public class PhoenixCSVLoadUtil {
    private static Log log = LogFactory.getLog(PhoenixCSVLoadUtil.class);
    private static final String SQL_FILE_EXT = ".sql";
    private static final String CSV_FILE_EXT = ".csv";

    /**
     * 单表导入，直接调用PhoenixRuntime的main方法<br>
     * 注意：<br>
     * <ul>
     * <li>csv文件必须跟表名一致，sql文件不需要跟表名一致</li>
     * <li>sql文件必须在csv文件的前面</li>
     * <li>会自动生成索引数据</li>
     * <li>多表导入有问题</li>
     * </ul>
     *
     * @param zk          zookeeper信息
     * @param sqlPath     建表脚本
     * @param csvPath     csv文件(文件必须跟建表脚本中的文件名一致)
     * @throws Exception
     */
    public static void load(String zk, String sqlPath, String csvPath)
            throws Exception {
        PhoenixCSVLoadRunner loadRunner = new PhoenixCSVLoadRunner(zk, sqlPath,
                csvPath);
        loadRunner.load(zk, sqlPath, csvPath);
    }

    /**
     * 多表导入(多线程)，根据pathMap的大小开启一个线程池
     *
     * @param zk      zookeeper信息
     * @param pathMap <key--建表脚本文件,value--对应的csv文件> 特别注意：<br>
     *                如果pathMap中脚本文件和csv文件不是对应的会使导入中断
     * @throws InterruptedException
     */
    public static void batchLoadFromFile(String zk, Map<String, String> pathMap)
            throws InterruptedException {
        batchLoadFromFile(zk, pathMap, pathMap.size(), pathMap.size() + 5);
    }

    /**
     * @param zk
     * @param pathMap
     * @param corePoolSize
     * @param maximumPoolSize
     * @throws InterruptedException
     */
    public static void batchLoadFromFile(String zk,
                                         Map<String, String> pathMap, int corePoolSize, int maximumPoolSize)
            throws InterruptedException {
        // 创建线程池
        ThreadPoolExecutor threadPool = new ThreadPoolExecutor(
                corePoolSize, // 并发运行的线程数
                maximumPoolSize, // 池中最大线程数
                60, // 池中线程最大等待时间
                TimeUnit.SECONDS, // 池中线程最大等待时间单位
                new LinkedBlockingQueue<Runnable>(), // 池中队列维持对象
                Executors.defaultThreadFactory(),
                new ThreadPoolExecutor.CallerRunsPolicy() // 超过最大线程数方法，自动重新调用execute方法
        );
        CountDownLatch runningThreadNum = new CountDownLatch(pathMap.size());
        List<Future<String>> futures = new ArrayList<Future<String>>();
        for (Map.Entry<String, String> entry : pathMap.entrySet()) {
            PhoenixCSVLoadRunner loadRunner = new PhoenixCSVLoadRunner(zk,
                    entry.getKey(), entry.getValue(), runningThreadNum);
            futures.add(threadPool.submit(loadRunner));
        }

        // 等待导入线程结束
        runningThreadNum.await();

        // 打印导入结果
        for (Future<String> future : futures) {
            try {
                String result = future.get();
                System.out.println(result);
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }

    }

    public static void batchLoadFromDir(String zk, String tableName,
                                        String columus, String sqlPath, String[] csvDirs) throws Exception {
        batchLoadFromDir(zk, tableName, columus, sqlPath, csvDirs,0, 0);
    }

    /**
     * @param zk
     * @param tableName
     * @param columus         列信息,必须的结构是col1,col2,col3
     * @param sqlPath         建表的sql脚本
     * @param csvDirs         csv目录数组
     * @param corePoolSize    并发运行的线程数
     * @param maximumPoolSize 池中最大线程数
     * @throws Exception
     */
    public static void batchLoadFromDir(String zk, String tableName,
                                        String columus, String sqlPath, String[] csvDirs,int corePoolSize, int maximumPoolSize) throws Exception {
        // 验证参数是否正确
        if (null == zk || zk.length() <= 0) {
            throw new RuntimeException("zk Can't be null or empty");
        }
        if (null == sqlPath || sqlPath.length() <= 0) {
            throw new RuntimeException("sqlPath Can't be null or empty");
        }
        if (!sqlPath.endsWith(SQL_FILE_EXT)) {
            throw new RuntimeException("sqlPath must be endsWith "
                    + SQL_FILE_EXT);
        }
        if (null == csvDirs || csvDirs.length <= 0) {
            throw new RuntimeException("csvDirs Can't be null or empty");
        }
        if (corePoolSize < 0) {
            throw new RuntimeException("corePoolSize must be greater than or equal to zero");
        }
        if (maximumPoolSize < 0) {
            throw new RuntimeException("maximumPoolSize must be greater than or equal to zero");
        }
        if (corePoolSize > maximumPoolSize) {
            throw new RuntimeException("corePoolSize must be Less than or equal to maximumPoolSize");
        }
        // 验证sqlPath文件是否存在
        File sqlFile = new File(sqlPath);
        if (!sqlFile.exists()) {
            throw new RuntimeException("File Not Found:[" + sqlPath + "]");
        }
        // 验证csvPath文件是否存在

        List<File> csvList=new LinkedList<File>();
        for (String dirName : csvDirs) {
            File dir = new File(dirName);
            if (!dir.exists()) {
                throw new RuntimeException("dir Not Found:[" + dirName + "]");
            } else if (dir.isFile()) {
                throw new RuntimeException(" is not a dir:[" + dirName + "]");
            } else {
                File[] fileArr=dir.listFiles(new FilenameFilter(){
                    @Override
                    public boolean accept(File dir, String name) {
                        if(name.toLowerCase().endsWith(".csv")){
                           return true;
                        } else{
                           return false;
                        }
                    }
                });
                csvList.addAll(Arrays.asList(fileArr));
            }
        }
        int size=csvList.size();

        // 先建表
        List<String> list = new ArrayList<String>();
        list.add(zk);
        list.add(sqlPath);
        String[] arrs = list.toArray(new String[list.size()]);
        PhoenixCSVLoadRunner.main(arrs);

        //导入数据
        // 创建线程池
        ThreadPoolExecutor threadPool = new ThreadPoolExecutor(
                corePoolSize == 0 ? size : corePoolSize, // 并发运行的线程数
                maximumPoolSize == 0 ? size + 3 : maximumPoolSize, // 池中最大线程数
                60, // 池中线程最大等待时间
                TimeUnit.SECONDS, // 池中线程最大等待时间单位
                new LinkedBlockingQueue<Runnable>(), // 池中队列维持对象
                Executors.defaultThreadFactory(),
                new ThreadPoolExecutor.CallerRunsPolicy() // 超过最大线程数方法，自动重新调用execute方法
        );
        CountDownLatch runningThreadNum = new CountDownLatch(size);
        List<Future<String>> futures = new ArrayList<Future<String>>();

        for(File csvFile:csvList){
            PhoenixCSVLoadDirRunner loadRunner = new PhoenixCSVLoadDirRunner(zk, tableName,
                        columus,csvFile.getPath(), runningThreadNum);
            futures.add(threadPool.submit(loadRunner));
        }
        // 等待导入线程结束
        runningThreadNum.await();

        // 打印导入结果
        for (Future<String> future : futures) {
            try {
                String result = future.get();
                System.out.println(result);
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
    }

    private static void usageError() {
        System.err.println("Usage: phoenixCSVLoad.sh <-t table-name> <-h comma-separated-column-names> <-s path-to-sql> <-c comma-separated-path-to-csv>...\n" +
                "Examples:\n" +
                "  phoenixCSVLoad.sh -t test01 -h id,name,tel -s D:\\test01.sql -c D:\\test1,D:\\test2\n"
        );
        System.exit(-1);
    }

    public static void main(String[] args) {
        String zk= Connection.get("hbase.zookeeper.quorum",null);
        String tableName=null;
        String columus=null;
        String sqlPath=null;
        String[] csvDirs=null;
        try {
            // 参数验证
            Options options = new Options();
            //options.addOption("z", true, "zookeeper address");//直接从配置文件取了
            options.addOption("t", true, "tablename");
            options.addOption("h", true, "comma-separated-column-names");//不需要列族
            options.addOption("s", true, "path-to-sql");
            options.addOption("c", true, "comma-separated-path-to-csv,Must be a directory");

            CommandLineParser parser = new PosixParser();
            CommandLine cmd = parser.parse(options, args);
            String parser_error = "ERROR while parsing arguments. ";

            if(null==zk){
                System.err.println("hbase.zookeeper.quorum is not provide in the configfile[client-runtime.properties]");
                System.exit(-1);
            }

            if (cmd.hasOption("t")) {
                tableName = cmd.getOptionValue("t");
            } else {
                System.err.println(parser_error + "Please provide tablename");
                usageError();
            }

            if (cmd.hasOption("h")) {
                columus = cmd.getOptionValue("h");
            } else {
                System.err.println(parser_error + "Please provide comma-separated-column-names");
               usageError();
            }

            if (cmd.hasOption("s")) {
                sqlPath = cmd.getOptionValue("s");
            } else {
                System.err.println(parser_error + "Please provide path-to-sql");
                usageError();
            }

            if (cmd.hasOption("c")) {
                csvDirs = cmd.getOptionValue("c").split(",");
            } else {
                System.err.println(parser_error + "Please provide comma-separated-path-to-csv,Must be a directory");
                usageError();
            }

            batchLoadFromDir(zk,tableName,columus,sqlPath,csvDirs);
        } catch (Throwable t) {
            t.printStackTrace();
        } finally {

        }
    }
}