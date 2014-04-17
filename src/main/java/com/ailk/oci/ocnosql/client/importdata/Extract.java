/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ailk.oci.ocnosql.client.importdata;

import com.ailk.oci.ocnosql.client.config.spi.Connection;
import java.io.*;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.SnappyCodec;

/**
 *
 * @author tianyi
 */
public class Extract extends Thread {

    private static final Log log = LogFactory.getLog(Extract.class.getSimpleName());
    private static String source_;
    private static String dest_;
    private static String bak_;
    private static String compress_;
    private static int bufferSize_ = 102400;
    private static Configuration conf_;
    private static int threadNumber_ = 20;
    private static Queue<String> queue_ = null;
    private DFSClient client_;

    public void setQueue(Queue<String> jobQueue) {
        queue_ = jobQueue;
    }

    public void run() {
        while (true) {
            String job = null;
            synchronized (queue_) {
                job = queue_.poll();
            }
            if (job == null) {
                try {
                    Thread.sleep(1000);
                    continue;
                } catch (InterruptedException ex) {
                    log.error("ExtractThread Exit!");
                    return;
                }
            }
            log.info("start handle upload [" + job + "] ");
            if (!uploadFile(job)) {
                log.error("ExtractThread Exit!");
                return;
            }
        }
    }

    private String getFileName(String path) {
        return path.substring(path.lastIndexOf("/") + 1);
    }

    private boolean uploadFile(String path) {
        BufferedInputStream is = null;
        BufferedOutputStream os = null;
        File file = new File(path);
        String oriName = getFileName(path);
        oriName = oriName.substring(0, oriName.length() - 11);
        try {
            if (client_ == null) {
                client_ = new DFSClient(NameNode.getAddress(conf_), conf_);
            }

            SnappyCodec codec = null;

            if ("snappy".equals(compress_)) {
                codec = new SnappyCodec();

            }
            long start = System.currentTimeMillis();
            codec.setConf(conf_);
            is = new BufferedInputStream(new FileInputStream(file));
            os = new BufferedOutputStream(codec.createOutputStream(client_.create(dest_ + oriName, false)));
            byte[] buffer = new byte[bufferSize_];
            int size = is.read(buffer);
            while (size > 0) {
                os.write(buffer, 0, size);
                size = is.read(buffer);
            }

            long stop = System.currentTimeMillis();
            log.info("upload file [" + path + "] done, use [" + (stop - start) + "] ms");

            file.renameTo(new File(bak_ + oriName));
            return true;
        } catch (Exception ex) {
            file.renameTo(new File(bak_ + oriName));
            log.error("upload file failed", ex);
            return false;
        } finally {
            if (is != null) {
                try {
                    is.close();
                } catch (IOException ex) {
                    log.error("close file reader failed", ex);
                }
                try {
                    os.close();
                } catch (IOException ex) {
                    log.error("close hdfsfile writer failed", ex);
                }
            }
        }
    }

    public static void main(String[] args) {

        //args = new String[4];
        //args[0] = "/Users/tianyi/test/";
        //args[1] = "/Users/tianyi/bak/";
        //args[2] = "/tianyi/";
        //args[3] = "snappy";


        if (argumentIsInvalid(args)) {
            showUsage();
            System.exit(-1);
        }

        if (sourcePathIsInvalid()) {
            log.error("source path is invalid");
            System.exit(-1);
        }

        if (bakPathIsInvalid()) {
            log.error("source path is invalid");
            System.exit(-1);
        }

        conf_ = Connection.getInstance().getConf();
        //if (destPathIsInvalid()) {
        //    log.error("dest path is invalid");
        //    System.exit(-1);
        //}

        if (compressIsInvalid()) {
            System.exit(-1);
        }

        Queue<String> queue = new LinkedBlockingQueue();

        for (int i = 0; i < threadNumber_; i++) {
            Extract thread = new Extract();
            thread.setQueue(queue);
            thread.start();
        }

        while (true) {
            File file = new File(source_);
            synchronized (queue_) {
                if (queue_.size() == 0) {
                    File[] fileArray = file.listFiles(new FilenameFilter() {

                        @Override
                        public boolean accept(File file, String string) {
                            if (string.endsWith(".processing")) {
                                return false;
                            } else {
                                return true;
                            }
                        }
                    });
                    for (File job : fileArray) {
                        File newJob = new File(job.getAbsoluteFile() + ".processing");
                        job.renameTo(newJob);
                        queue.add(newJob.getAbsolutePath());
                    }
                }
            }
            try {
                Thread.sleep(10000);
            } catch (InterruptedException ex) {
                Logger.getLogger(Extract.class.getName()).log(Level.SEVERE, null, ex);
            }
        }

    }

    private static void showUsage() {
        System.err.println("usage: source-path bak-path dest-path(on hdfs) compress-method[gzip|snappy]");
    }

    private static boolean argumentIsInvalid(String[] args) {
        if (args.length < 3 || args.length > 5) {
            return true;
        }
        source_ = args[0].endsWith("/") ? args[0] : args[0] + "/";
        bak_ = args[1].endsWith("/") ? args[1] : args[1] + "/";
        dest_ = args[2].endsWith("/") ? args[2] : args[2] + "/";
        if (args.length < 4) {
            compress_ = "snappy";
        } else {
            compress_ = args[3];
        }
        if (args.length < 5) {
            //use default;
        } else {
            bufferSize_ = Integer.valueOf(args[4]);
        }
        return false;
    }

    private static boolean sourcePathIsInvalid() {
        File sourcePath = new File(source_);
        if (!sourcePath.canRead()) {
            log.error("source path should be able to read");
            return true;
        }
        if (!sourcePath.canWrite()) {
            log.error("source path should be able to modify");
            return true;
        }
        if (!sourcePath.isDirectory()) {
            log.error("source path should be a directory");
            return true;
        }
        return false;
    }

    private static boolean bakPathIsInvalid() {
        File sourcePath = new File(bak_);
        if (!sourcePath.canRead()) {
            log.error("bak path should be able to read");
            return true;
        }
        if (!sourcePath.canWrite()) {
            log.error("bak path should be able to modify");
            return true;
        }
        if (!sourcePath.isDirectory()) {
            log.error("bak path should be a directory");
            return true;
        }
        return false;
    }

    private static boolean destPathIsInvalid() {
        conf_ = Connection.getInstance().getConf();
        try {
            DFSClient client = new DFSClient(NameNode.getAddress(conf_), conf_);
            return client.getFileInfo(dest_).isDir();
        } catch (IOException ex) {
            log.error("connect to hdfs failed", ex);
            return true;
        }
    }

    private static boolean compressIsInvalid() {
        if ("snappy".equals(compress_)) {
            return false;
        } else if ("gzip".equals(compress_)) {
            log.error("not support compress type [" + compress_ + "] yet");
            return true;
        } else {
            log.error("not support compress type [" + compress_ + "] yet");
            return true;
        }
    }
}
