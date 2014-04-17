/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ailk.oci.ocnosql.client.importdata;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 这个类用于在各个测试场景中创建随机话单数据,以供测试 使用方法: Generate.sh seprate pattern outputPath
 * [--maxsize=1024] [--prefix=random-output] [--thread=20] 参数说明: pattern
 * 话单格式,支持使用RANDOM_NUMBER RANDOM_STRING等参数 outputPath 输出路径 maxsize 单个文件最大值
 * prefix 文件名前缀
 *
 * @author tianyi
 */
public class GenerateRandomRecord extends Thread {

    private static final Log log = LogFactory.getLog(GenerateRandomRecord.class.getSimpleName());
    private static String seprate = "";
    private static List<String> pattern = new ArrayList<String>();
    private static String output = "";
    private static int maxSize = 1024;
    private static String prefix = "random-output";
    private static int thread = 20;
    private static int threadRecord = 1024;
    private static long now = System.currentTimeMillis();
    private static SimpleDateFormat sdf = new SimpleDateFormat("HHmmss");
    private static long fileSeq = 0L;
    private static final Byte fileSeqLock = '0';

    @Override
    public void run() {
        log.debug("start running!");
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        BufferedOutputStream bos = null;
        try {
            bos = new BufferedOutputStream(new FileOutputStream(generateFile()));
        } catch (FileNotFoundException ex) {
            Logger.getLogger(GenerateRandomRecord.class.getName()).log(Level.SEVERE, null, ex);
        }
        int fileSize = 0;
        int recordSize = 0;
        try {
            while (true) {
                baos.reset();
                for (String p : pattern) {
                    if (p.startsWith("RANDOM_NUM_ENUM")) {
                        int start = Integer.valueOf(p.split(",")[1]);
                        int stop = Integer.valueOf(p.split(",")[2]);
                        baos.write(randomIntegerString(start, stop).getBytes());
                    } else if (p.startsWith("RANDOM_NUMBER")) {
                        int length = Integer.valueOf(p.split(",")[1]);
                        baos.write(randomIntegerString(length).getBytes());
                    } else if (p.startsWith("RANDOM_STR_ENUM")) {
                        String[] strs = p.split(",");
                        baos.write(randomEnumString(strs).getBytes());
                    } else if (p.startsWith("RANDOM_STRING")) {
                        int length = Integer.valueOf(p.split(",")[1]);
                        baos.write(randomString(length).getBytes());
                    } else if (p.startsWith("RANDOM_TIME")) {
                        baos.write(randomTime().getBytes());
                    } else {
                        baos.write(p.getBytes());
                    }
                    //baos.write(seprate.getBytes());
                }
                baos.write("\n".getBytes());
                fileSize += baos.toByteArray().length;
                recordSize ++;
                if(recordSize >= threadRecord) {
                    break;
                }
                bos.write(baos.toByteArray());
                if (fileSize >= maxSize) {
                    try {
                        bos.close();
                        bos = new BufferedOutputStream(new FileOutputStream(generateFile()));
                        fileSize = 0;
                    } catch (FileNotFoundException ex) {
                        Logger.getLogger(GenerateRandomRecord.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
            }
            log.debug("finish running!");
        } catch (Exception e) {
            log.error("write file error!", e);
        } finally {
            if (bos != null) {
                try {
                    bos.close();
                } catch (IOException ex) {
                    Logger.getLogger(GenerateRandomRecord.class.getName()).log(Level.SEVERE, null, ex);
                }
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
    
    private static String randomIntegerString(int length) {
        char[] wordChar = new char[length];
        for (int j = 0; j < wordChar.length; j++) {
            wordChar[j] = (char) (Math.random() * 10 + 48);
        }
        return new String(wordChar);
    }
    
    private static String randomEnumString(String[] strs) {
        int length = strs.length -1;
        return strs[(int) (Math.random() * length + 1)];
    }
    
    private static String randomIntegerString(int start, int stop) {
        if(stop - start == 1) {
            if(Math.random() * 100 > 50) { 
                return String.valueOf(stop);
            } else {
                return String.valueOf(start);
            }
        }
        
        return String.valueOf((int) (start  + Math.random() * (stop -start)));
    }

    private static String randomTime() {
        return sdf.format(new Date((long) (now + Math.random() * 86400000)));
    }

    private String generateFile() {
        String filename = null;
        synchronized(fileSeqLock) {
            fileSeq ++;
            filename = output + prefix + "-" + fileSeq + ".dat";
            while(new File(filename).exists()) {
                fileSeq ++;
                filename = output + prefix + "-" + fileSeq + ".dat";
            }
            return filename;
        }
    }

    public static void main(String[] args) {
        //for test only, need remove
        args = new String[6];
        args[0] = "a;b;c;d;e;f;g;###RANDOM_NUMBER,10###;###RANDOM_STRING,10###;20130523###RANDOM_TIME###;h;i;j;k;l";
        args[1] = "e:/test_data/";
        args[2] = "--maxsize=1024000";
        args[3] = "--prefix=tianyi-test";
        args[4] = "--thread=1";
        args[5] = "--threadRecord=3000";
        
        if (args.length < 2) {
            System.err.println("Generate.sh pattern outputPath [--maxsize=1024] [--prefix=random-output] [--thread=20] [--threadRecord=1024]");
            System.exit(-1);
        }
        if (readParam(args) < 0) {
            System.err.println("read param failed");
            System.err.println("Generate.sh seprate pattern outputPath [--maxsize=1024] [--prefix=random-output] [--thread=20] [--threadRecord=1024]");
            System.exit(-1);
        }
        List<GenerateRandomRecord> running = new ArrayList<GenerateRandomRecord>();
        for (int i = 0; i < thread; i++) {
            GenerateRandomRecord grr = new GenerateRandomRecord();
            grr.start();
            running.add(grr);
        }
        for (GenerateRandomRecord target : running) {
            try {
                target.join();
            } catch (InterruptedException ex) {
                Logger.getLogger(GenerateRandomRecord.class.getName()).log(Level.SEVERE, null, ex);
            }
        }

    }

    private static int readParam(String[] args) {
        for (String p : args[0].split("###")) {
            pattern.add(p);
        }
        output = args[1];
        if (args.length > 2) {
            for (int i = 2; i < args.length; i++) {
                if (args[i].indexOf("=") < 0) {
                    return -1;
                } else {
                    String[] kv = args[i].split("=");
                    if ("--maxsize".equals(kv[0])) {
                        maxSize = Integer.valueOf(kv[1]);
                    } else if ("--prefix".equals(kv[0])) {
                        prefix = kv[1];
                    } else if ("--threadRecord".equals(kv[0])) {
                        threadRecord = Integer.valueOf(kv[1]);
                    } else if ("--thread".equals(kv[0])) {//
                        thread = Integer.valueOf(kv[1]);
                    } else {
                        return -1;
                    }
                }
            }
        }
        return 0;
    }
}
