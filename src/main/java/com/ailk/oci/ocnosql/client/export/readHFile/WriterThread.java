package com.ailk.oci.ocnosql.client.export.readHFile;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;

import com.ailk.oci.ocnosql.client.compress.Compress;
import com.ailk.oci.ocnosql.client.config.spi.CommonConstants;

public class WriterThread implements Runnable {
	private FileSystem fs;
	private Path hfilePath;
	private Compress decompress;
	private String destPath;
	private CountDownLatch runningThreadNum;
	
	public WriterThread(FileSystem fs, Path hfilePath, Compress decompress, String destPath, CountDownLatch runningThreadNum){
		this.fs = fs;
		this.hfilePath = hfilePath;
		this.decompress = decompress;
		this.destPath = destPath;
		this.runningThreadNum = runningThreadNum;
	}

	public void run() {
//		BufferedWriter bw = null;
//		long time = 0l;
//		try {
//			HFile.Reader reader = new HFile.Reader(fs, hfilePath, null, false);
//			HFileScanner scan  = reader.getScanner(false, false);
//			reader.loadFileInfo();
//			scan.seekTo();
//			StringBuffer sbuffer = new StringBuffer();
//			int counter = 0;
//			int index = 0;
//			do{
//				KeyValue kv = scan.getKeyValue();
//				Map<String, String> param = new HashMap<String, String>();
//				param.put(CommonConstants.SEPARATOR, HFileReaderSample.SEPERATOR);
//				List<String[]> result = decompress.deCompress(kv, param);
//				String rowKey = new String(kv.getRow());
//				String rowkeyGennerator = HFileReaderSample.prop.getProperty(
//						"rowkey.generator")==null? "md5" : HFileReaderSample.prop.getProperty("rowkey.generator");
//				if(rowkeyGennerator.equalsIgnoreCase("md5")){
//					rowKey = rowKey.substring(3);
//				}
//				for(String[] strs : result){
//					if(counter == HFileReaderSample.RECORDE_NUM_PER_FILE){
//						counter = 0;
//						System.out.println("write " + HFileReaderSample.RECORDE_NUM_PER_FILE + " record cost : " + (System.currentTimeMillis() - time) + "ms");
//					}
//					if(counter == 0){
//						if(bw != null){
//							bw.flush();
//							bw.close();
//						}
//						String fileName = destPath + File.separator + HFileReaderSample.TABLE_NAME + "_" + hfilePath.getName() + "_" + index;
//						System.out.println("start write " + fileName);
//						File file = new File(fileName);
//						bw = new BufferedWriter(new FileWriter(file, true));
//						time = System.currentTimeMillis();
//						index ++;
//					}
//					int k = 0;
//					for(String str : strs){
//						if(k == HFileReaderSample.ROWKEY_INDEX ){
//							sbuffer.append(rowKey + HFileReaderSample.SEPERATOR);
//						}else {
//							sbuffer.append(str + HFileReaderSample.SEPERATOR);
//						}
//                      k++;
//					}
//					sbuffer.deleteCharAt(sbuffer.length() - 1);
//					bw.write(sbuffer.toString() + "\n");
//					sbuffer.setLength(0);
//					counter ++;
//				}
//			}while(scan.next());
//			reader.close();
//		} catch (IOException e) {
//			e.printStackTrace();
//		} finally {
//			runningThreadNum.countDown();
//			try {
//				if(bw != null){
//					bw.close();
//				}
//			} catch (IOException e) {
//				e.printStackTrace();
//			}
//		}
	}

}
