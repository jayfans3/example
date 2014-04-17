package com.ailk.oci.ocnosql.client.importdata.phoenix;

import java.util.ArrayList;
import java.util.List;

import com.salesforce.phoenix.map.reduce.CSVBulkLoader;

/**
 * 此类直接封装了com.salesforce.phoenix.map.reduce.CSVBulkLoader<br>
 * CSVBulkLoader使用map-reduce的方式生成hfile，然后导入到hbase的对应目录下从而实现快速导入数据<br>
 * 用户直接使用静态方法load即可，跟在命令行执行脚本效果一样<br>
 * 注意：csv文件为hdfs上的文件
 * 
 * @author lifei5
 * 
 */
public class PhoenixBulkLoader {
	/**
	 * 此方法直接调用CSVBulkLoader的main方法已启动bulkload的map-reduce任务<br>
	 * 命令行调用CSVBulkLoader的示例:<br>
	 * ./csv-bulk-loader.sh -i hdfs://server:9000/mydir/data.csv -s ns -t
	 * example -sql ~/Documents/createTable.sql -zk server:2181 -hd
	 * hdfs://server:9000 -mr server:9001 封装后方法参数与命令行option的对应关系:<br>
	 * 
	 * @param csvPath
	 *            -i CSV data file path in hdfs (mandatory)<br>
	 * @param schemaName
	 *            -s Phoenix schema name (mandatory if not default)<br>
	 * @param tableName
	 *            -t Phoenix table name (mandatory)<br>
	 * @param createSqlPath
	 *            -sql Phoenix create table sql file path (mandatory)<br>
	 * @param zk
	 *            -zk Zookeeper IP:<port> (mandatory)<br>
	 * @param mr
	 *            -mr MapReduce Job Tracker IP:<port> (mandatory)<br>
	 * @param hd
	 *            -hd HDFS NameNode IP:<port> (mandatory)<br>
	 * @param outputPath
	 *            -o Output directory path in hdfs (optional)<br>
	 * @param ignoreErr
	 *            -error Ignore error while reading rows from CSV ? (1-YES |
	 *            0-NO, default-1) (optional)<br>
	 * @param help
	 *            -help Print all options (optional)<br>
	 * 
	 *            以下参数CSVBulkLoader暂时不支持，因此封装的时候屏蔽掉了:<br>
	 *            -idx Phoenix index table name (optional, not yet supported)<br>
	 * 
	 * 有两个参数需要注意：<br>
	 *            ignoreErr:为true则忽略从csv读取时的错误，反之不忽略，默认为true<br>
	 *            help：一旦help为true就不会启动bulkload，而是打印帮助信息到控制台。
	 * 特别注意：<br>
	 * 本方法目前还不支持自建索引数据，如果需要索引数据请使用com.ailk.oci.ocnosql.client.importdata.phoenix.PhoenixCSVLoader
	 */
	public static void load(String csvPath, String schemaName,
			String tableName, String createSqlPath, String zk, String mr,
			String hd, String outputPath, boolean ignoreErr, boolean help) throws Exception {
		// 把方法参数转换为CSVBulkLoader需要的main函数参数

		// csvPath
		List<String> list = new ArrayList<String>();
		if (null != csvPath && csvPath.length() > 0) {
			list.add("-i");
			list.add(csvPath);
		}

		// schemaName
		if (null != schemaName && schemaName.length() > 0) {
			list.add("-s");
			list.add(schemaName);
		}

		// tableName
		if (null != tableName && tableName.length() > 0) {
			list.add("-t");
			list.add(tableName);
		}

		// createSqlPath
		if (null != createSqlPath && createSqlPath.length() > 0) {
			list.add("-sql");
			list.add(createSqlPath);
		}

		// zk
		if (null != zk && zk.length() > 0) {
			list.add("-zk");
			list.add(zk);
		}

		// mr
		if (null != mr && mr.length() > 0) {
			list.add("-mr");
			list.add(mr);
		}

		// hd
		if (null != hd && hd.length() > 0) {
			list.add("-hd");
			list.add(hd);
		}

		// outputPath
		if (null != outputPath && outputPath.length() > 0) {
			list.add("-o");
			list.add(outputPath);
		}

		// ignoreErr
		if (ignoreErr) {
			list.add("-error");
			list.add("1");
		} else {
			list.add("-error");
			list.add("0");
		}
		// help
		if (help) {
			list.add("-help");
		}

		String[] arrs = list.toArray(new String[list.size()]);

		for (String arr : arrs) {
			System.out.println(arr);
		}

		CSVBulkLoader.main(arrs);
	}
	/**
	 * 该方法省略了几个参数，这几个参数使用默认值，推荐大家使用本方法<br>
	 * @param csvPath
	 * @param tableName
	 * @param createSqlPath
	 * @param zk
	 * @param mr
	 * @param hd
	 * @throws Exception
	 */
	public static void load(String csvPath,String tableName,String createSqlPath, String zk, String mr, String hd) throws Exception {
		load(csvPath, null, tableName, createSqlPath, zk, mr, hd, null, true, false);
	}

	
	public static void main(String[] args) {
		try {
			/*
			load("hdfs://ocdata05:8020/phoenix_mms_20130115.csv", "user", "phoenix_mms_20130115", "D:\\phoenix_mms_20130115.sql", "ocdata05:2485", "ocdata05:8021", "hdfs://ocdata05:8020",
					null, true, false);
					*/
			load("hdfs://ocdata05:8020/phoenix_mms_20130115.csv", "phoenix_mms_20130115", "D:\\phoenix_mms_20130115.sql", "ocdata05:2485", "ocdata05:8021", "hdfs://ocdata06:8020");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
