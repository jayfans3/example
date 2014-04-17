package com.ailk.oci.ocnosql.client.export.readHFile;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.ailk.oci.ocnosql.client.compress.Compress;
import com.ailk.oci.ocnosql.client.compress.HbaseCompressImpl;

public class HFileReaderSample {
	public static String TABLE_NAME;
	public static int RECORDE_NUM_PER_FILE = 100000;
	public static int ROWKEY_INDEX = -1;
	public static String SEPERATOR;
	public static Properties prop = null;
	public static ThreadPoolExecutor threadPool;
	
	public static void main(String[] args) {
		String tableName = args[0];
		String rowkeyIndex = args[1];
		String seperatorStr = args[2];
		String destPath = args[3];
		String record_num_per_file = args[4];
		if(StringUtils.isEmpty(tableName)){
			System.err.println("tableName is null");
			usage();
			System.exit(0);
		}
		TABLE_NAME = tableName;
		if(StringUtils.isEmpty(seperatorStr)||StringUtils.isEmpty(destPath)||StringUtils.isEmpty(destPath)){
			System.err.println("rowkeyIndex or seperator or destPath or destPath or hdfs_url is null");
			usage();
			System.exit(0);
		}
		try {
			readConfig();
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		System.out.println("start export table : " + TABLE_NAME);
		RECORDE_NUM_PER_FILE = Integer.parseInt(record_num_per_file);
		ROWKEY_INDEX = Integer.parseInt(rowkeyIndex);
		SEPERATOR = seperatorStr;
		
		Compress decompress = new HbaseCompressImpl();
		FileSystem fs = null;
		Path tempPath = null;
		try {
			fs = FileSystem.getLocal(new Configuration());
			Path dest = new Path(destPath);
			tempPath = new Path(dest.getParent() + "/exportTmp/");
			if(fs.exists(tempPath)){
				fs.delete(tempPath, true);
			}
			if(!fs.exists(tempPath)){
				fs.mkdirs(tempPath);
			}
			System.out.println("copy to " + tempPath.toUri().getPath() + " from /hbasedata/" + TABLE_NAME );
			
			Configuration conf = new Configuration();
			conf.set("fs.default.name", prop.getProperty("hdfs.url")==null?"hdfs://localhost:9000/" : prop.getProperty("hdfs.url"));
			FileSystem remoteFs = FileSystem.get(conf);
			long time = System.currentTimeMillis();
			remoteFs.copyToLocalFile(new Path("/hbasedata/" + TABLE_NAME), tempPath);
			System.out.println("copy cost : " + (System.currentTimeMillis() - time) + "ms");
			// 遍历路径下的HFile
			List<Path> hfiles = getHFiles(fs, tempPath);
			System.out.println("find hfile size : " + hfiles==null?0:hfiles.size());
			writeData(fs, hfiles, decompress, destPath);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} finally{
			try {
				if(fs != null){
					if(tempPath != null){
						fs.delete(tempPath.getParent(), true);
					}
				}
				threadPool.shutdown();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	private static void readConfig() throws FileNotFoundException, IOException{
//		URL url = HFileReaderSample.class.getClassLoader().getResource("export.properties");
		URL url = HFileReaderSample.class.getClassLoader().getResource("client-runtime.properties");
		if(url == null){
			System.err.println("could not found export.properties");
			usage();
			System.exit(0);
		}
		String path = url.getPath();
		File file = new File(path);
		prop = new Properties();
		prop.load(new FileInputStream(file));
		
		// 初始化线程池
		String coreSize = prop.getProperty("threadpool.core.size");
		int corePoolSize = Integer.parseInt(coreSize==null?"50" : coreSize);// 默认并发运行的线程数
		String maxSize = prop.getProperty("threadpool.max.size");
		int maximumPoolSize = Integer.parseInt(maxSize==null? "100" : maxSize);// 默认池中最大线程数
		String keepTime = prop.getProperty("threadpool.keepalivetime");
		long keepAliveTime = Integer.parseInt(keepTime==null? "60" : keepTime);// 默认池中线程最大等待时间
		threadPool = new ThreadPoolExecutor(
				corePoolSize, // 并发运行的线程数
				maximumPoolSize, // 池中最大线程数
				keepAliveTime, // 池中线程最大等待时间
				TimeUnit.SECONDS, // 池中线程最大等待时间单位
				new LinkedBlockingQueue<Runnable>(), // 池中队列维持对象
				Executors.defaultThreadFactory(),
				new ThreadPoolExecutor.CallerRunsPolicy() // 超过最大线程数方法，此处为自动重新调用execute方法
		);
	}
	
	private static List<Path> getHFiles(FileSystem fs, Path path){
		List<Path> pathList = new ArrayList<Path>();
		try {
			if(path.getName().startsWith(".")){
				return pathList;
			}
			if(fs.isFile(path)){
				pathList.add(path);
			}else{
				FileStatus[] fstatuses = fs.listStatus(path);
				for(FileStatus fstatus : fstatuses){
					pathList.addAll(getHFiles(fs, fstatus.getPath()));
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return pathList;
	}
	
	private static void usage(){
		System.out.println("Usage : export.sh <tableName> <destDir>");
	}
	
	private static void writeData(FileSystem fs, List<Path> paths, Compress decompress, String destPath) throws InterruptedException{
		File destDir = new File(destPath);
		if(!destDir.exists()){
			destDir.mkdir();
		}
		CountDownLatch runningThreadNum = new CountDownLatch(paths.size());
		for(Path path : paths){
			WriterThread writer = new WriterThread(fs, path, decompress, destPath, runningThreadNum);
			threadPool.execute(writer);
		}
		runningThreadNum.await();
	}

}
