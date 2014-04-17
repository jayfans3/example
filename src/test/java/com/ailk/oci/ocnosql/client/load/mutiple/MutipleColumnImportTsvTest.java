package com.ailk.oci.ocnosql.client.load.mutiple;

import org.apache.commons.lang.StringUtils;
import org.junit.Test;

import com.ailk.oci.ocnosql.client.cache.OciTableRef;
import com.ailk.oci.ocnosql.client.config.spi.Connection;
import com.ailk.oci.ocnosql.client.load.single.SingleColumnImportTsv;

public class MutipleColumnImportTsvTest {

	@Test
	public static void bulkload(String[] args) throws Exception{
		
//		String tableName = "test";
//		String input = "hdfs://mycluster/zhuangyang/data";
//		String output = "hdfs://mycluster/zhuangyang/output";
//		String seperator = ";";
//		String columns = "HBASE_ROW_KEY,f:name,f:age";
//		String compressType = "com.ailk.oci.ocnosql.client.compress.HbaseCompressImpl";
//		String loadType = "mutiple";
		
		if(args.length <= 5){
			System.err.println("args is invalidate.");
			System.out.println("usage: <tableName> <input> <output> <seperator> <columns> [<rowkeyGenerator> <compressorType> <loadType>]");
			System.exit(1);
		}
		
		String tableName = args[0];
		String input = args[1];
		String output = args[2];
		String seperator = args[3];
		String columns = args[4];
		String rowkeyGenerator = null;
		if(args.length >= 6){
			rowkeyGenerator = args[5];
		}
		String compressorType = null;
		if(args.length >= 7 && StringUtils.isNotEmpty(args[6])){
			compressorType = args[6];
		}
		String loadType = "";
		if(args.length >= 8){
			loadType = args[7];
		}
		
		MutipleColumnImportTsv mutipleImporter = new MutipleColumnImportTsv();
		SingleColumnImportTsv singleImport = new SingleColumnImportTsv();
		
		OciTableRef table = new OciTableRef(); 
		table.setName(tableName);
		table.setColumns(columns);
		table.setImportTmpOutputPath(output);
		table.setInputPath(input);  
		table.setSeperator(seperator);
		table.setCompressor(compressorType);
		table.setRowkeyGenerator(rowkeyGenerator);
		
		Connection conn = Connection.getInstance();
		if(loadType.equals("mutiple")){
			mutipleImporter.execute(conn, table);
		}else{
			singleImport.execute(conn, table);
		}
	} 

}
