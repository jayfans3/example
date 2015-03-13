package com.asiainfo.billing.bill.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class WriteFile {

	public static void main(String[] args) {
		writeFile(args[0],args[1],args[2]);
	}

	public static void writeFile(String rootFile,String rowkey,String content) {

//		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//		String content = sdf.format(new Date());
//		System.out.println("现在时间：" + content);
		FileOutputStream out = null;
		File file;

		try {
//			String rootFile = "D:\\tests\\license";
			file = new File(rootFile);

			if (!file.exists()) {

				/*
				 * 
				 * file.mkdirs()：创建没有存在的所有文件夹
				 * 
				 * file.mkdir()：创建没有存在的最后一层文件夹
				 * 
				 * 例如：在硬盘上有D://test
				 * 文件夹，但是现在需要创建D://test//license//save,这个时候就需要使用file
				 * .mkdirs()而不能使用file
				 * .mkdir()，另外这两个方法都是仅仅能创建文件夹，不能创建文件，即使创建D://test
				 * //license//save/
				 * /systemTime.dat如果使用该方法创建的SystemTime.dat也是一个文件夹 ，而不是文件
				 */
				file.mkdirs();
			}
			File fileDat = new File(rootFile + java.io.File.separator+rowkey.trim()+".dat");
			/*
			 * 
			 * if(!fileDat.exists()){
			 * 
			 * //创建文件 不是文件夹，在程序中这这一步没有必要，因为 new
			 * FileOutputStream(fileDat);该语句有创建文件的功能 fileDat.createNewFile();//
			 * }
			 */
			out = new FileOutputStream(fileDat);
			byte[] contentInBytes = content.getBytes("UTF-8");

			out.write(contentInBytes);
			out.flush();
			out.close();

			System.out.println("Done");

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (out != null) {
					out.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	public static Map<String,List<String>> readFile(String rootFile) throws Exception{

//		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//		String content = sdf.format(new Date());
//		System.out.println("现在时间：" + content);
		FileInputStream in = null;
		File file;

		try {
//			String rootFile = "D:\\tests\\license";
			file = new File(rootFile);

			if (!file.exists()) {

				/*
				 * 
				 * file.mkdirs()：创建没有存在的所有文件夹
				 * 
				 * file.mkdir()：创建没有存在的最后一层文件夹
				 * 
				 * 例如：在硬盘上有D://test
				 * 文件夹，但是现在需要创建D://test//license//save,这个时候就需要使用file
				 * .mkdirs()而不能使用file
				 * .mkdir()，另外这两个方法都是仅仅能创建文件夹，不能创建文件，即使创建D://test
				 * //license//save/
				 * /systemTime.dat如果使用该方法创建的SystemTime.dat也是一个文件夹 ，而不是文件
				 */
//				file.mkdirs();
				new Exception("没目录！！");
			}
			File[] fs=file.listFiles();
			Map<String,List<String>> tabq_sb=new HashMap();
			for(int i =0 ;i<fs.length;i++){
				File[] fs1=fs[i].listFiles();
				for(int j=0;j<fs1.length;j++){
					File fileDat =fs1[j];
					in = new FileInputStream(fileDat);
					byte[] b =new byte[102400];
					in.read(b);
					String[] s_=new String(b,"UTF-8").split("\n");
					tabq_sb.put(fs[i].getName()+"%"+fs1[j].getName().substring(0,fs1[j].getName().indexOf(".dat")),Arrays.asList(s_));
				}
			}
			in.close();
			return tabq_sb;

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (in != null) {
					in.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return null;
	}
	
	
	
	public static Map<String,List<String>> readTableRowkeys(String rootFile) throws Exception{

//		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//		String content = sdf.format(new Date());
//		System.out.println("现在时间：" + content);
		FileInputStream in = null;
		File file;

		try {
//			String rootFile = "D:\\tests\\license";
			file = new File(rootFile+java.io.File.separator+"input");

			if (!file.exists()) {

				/*
				 * 
				 * file.mkdirs()：创建没有存在的所有文件夹
				 * 
				 * file.mkdir()：创建没有存在的最后一层文件夹
				 * 
				 * 例如：在硬盘上有D://test
				 * 文件夹，但是现在需要创建D://test//license//save,这个时候就需要使用file
				 * .mkdirs()而不能使用file
				 * .mkdir()，另外这两个方法都是仅仅能创建文件夹，不能创建文件，即使创建D://test
				 * //license//save/
				 * /systemTime.dat如果使用该方法创建的SystemTime.dat也是一个文件夹 ，而不是文件
				 */
//				file.mkdirs();
				new Exception("没目录！！");
			}
			File[] fs=file.listFiles();
			Map<String,List<String>> tablename_rowkeys=new HashMap();
				for(int j=0;j<fs.length;j++){
					File fileDat =fs[j];
					in = new FileInputStream(fileDat);
					byte[] b =new byte[102400];
					in.read(b);
					String[] s_=new String(b).trim().split("\n");
					tablename_rowkeys.put(fs[j].getName().substring(0,fs[j].getName().indexOf(".dat")),Arrays.asList(s_));
				}
			in.close();
			return tablename_rowkeys;

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (in != null) {
					in.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return null;
	}
}