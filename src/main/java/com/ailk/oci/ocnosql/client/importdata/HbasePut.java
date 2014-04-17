package com.ailk.oci.ocnosql.client.importdata;

import com.ailk.oci.ocnosql.client.config.spi.Connection;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.*;

@SuppressWarnings("static-access")
public class HbasePut {
    private static final Log log = LogFactory.getLog(HbasePut.class.getSimpleName());
    
	private static int exePutThreadNum = Integer.parseInt(Connection.getInstance().get("hbase.exePutThreadNum", "5"));
    private static int maxPutThreadNum = Integer.parseInt(Connection.getInstance().get("hbase.maxPutThreadNum", "10"));
    private static boolean isDamonThread = Boolean.parseBoolean(Connection.getInstance().get("hbase.readFile.isDamonThread","false"));
    
    public void run(String[] args){
        if (args.length != 5) {
            System.out.println("start put data to Hbase server.....args.length=" + args.length);
            usage();
            return;
        }
        String dirpath = args[0];
        String rowkeyIndex = args[1];
        String oppNumIndex = args[2];
        String timeZone = args[3];
        String posindex = args[4];
        BufferedReader reader = null;
        try {
            File filedir = new File(dirpath);
            ExecutorService pool = new ThreadPoolExecutor(exePutThreadNum, maxPutThreadNum,
                    1000, TimeUnit.SECONDS,
                    new LinkedBlockingQueue<Runnable>(),
                    Executors.defaultThreadFactory());
            long startTime = System.currentTimeMillis();
            if (filedir.isDirectory()) {
                do {
                    File[] files = filedir.listFiles();
                    if(files.length==2){
                        try {
                            log.info("The file dir is Empty!");
                            Thread.sleep(2*60*1000);
                            continue;
                        } catch (InterruptedException ex) {
                            log.error("Thread InterruptedException!");
                            return;
                        }
                    }
                    CountDownLatch runningThreadNum = new CountDownLatch(files.length);
                    for (File f : files) {
                        String fileName = f.getName();
                        if(fileName.equals("error")||fileName.equals("bak")){
                            runningThreadNum.countDown();
                            continue;
                        }
                        if(fileName.endsWith(".tmp")){
                            runningThreadNum.countDown();
                            continue;
                        }
                        String newPath = f.getPath().concat(".tmp");
                        File newFile = new File(newPath);
                        f.renameTo(newFile);
                        FileRead fileUtils = new FileRead(runningThreadNum,newFile,Integer.parseInt(rowkeyIndex), Integer.parseInt(oppNumIndex), Integer.parseInt(timeZone), posindex);
                        pool.submit(fileUtils);
                    }
                    runningThreadNum.await();
                    long elapsedTime = System.currentTimeMillis()-startTime;
                    if(log.isInfoEnabled()){
                        log.info("Put all files elapsed time:"+elapsedTime+"ms");
                    }
                }
                while(isDamonThread);
            }
            pool.shutdown();

        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            if (reader != null) {
                try {
                    reader.close();
                }
                catch (IOException e1) {
                }
            }
        }
    }
    public static void main(String[] args) {
        HbasePut put = new HbasePut();
        put.run(args);

    }

    private static void usage() {
        System.err.println("usage: " +
                " talename path rowkeyindex rowposindex");
    }
}
