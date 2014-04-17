package com.ailk.oci.ocnosql.client.query;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.ailk.oci.ocnosql.client.config.spi.Connection;
import com.ailk.oci.ocnosql.client.spi.ClientAdaptor;

/**
 * 
 * @author lifei5
 * 
 */
public class TestQueryByRowkeyPrefix {
	private static Connection conn = null;

	@BeforeClass
	// 开始测试之前准备资源
	public static void beforeTest() {
		System.out.println("-------------测试前准备资源--------------");
		conn = Connection.getInstance();
		System.out.println("-------------资源已就绪--------------");
	}

	@AfterClass
	// 测试完成之后释放资源
	public static void afterTest() {
		System.out.println("-------------测试结束释放资源--------------");
		conn.getThreadPool().shutdown();
		System.out.println("-------------资源已释放--------------");
	}

	//@Test
	//多列查询(需要配置为多列查询)，测试需求3067
	/*
	public void testOnMultiCol() {
		System.out.println("--------------测试开始(多列)-------------");
		ClientAdaptor client = new ClientAdaptor();
		Criterion criterion = new Criterion();
		criterion.setEqualsTo("age", "23");
		
		List<ColumnFamily> columnFamilies = new ArrayList<ColumnFamily>();
		ColumnFamily cf = new ColumnFamily();
		cf.setFamily("info");
		cf.setColumns(new String[] {CommonConstants.ROW_KEY,"name","email"});
		columnFamilies.add(cf);
		
		List<String[]> list = client.queryByRowkeyPrefix(conn, "li", Arrays.asList("student"), null, null,columnFamilies);
		
		for (String[] record : list) {
			System.out.println(StringUtils.join(record, ";"));
		}

		System.out.println("--------------测试结束(多列)-------------");
	}
	*/
	@Test
	//单列查询(需要配置为单列查询),测试需求3071
	public void testOnSingelCol() {
		System.out.println("--------------测试开始(单列)-------------");
		ClientAdaptor client = new ClientAdaptor();
		/*
		Criterion criterion = new Criterion();
		criterion.setEqualsTo("age", "23");
		
		//单列设置Column没有意义
		*/
		List<String[]> list = client.queryByRowkeyPrefix(conn, "cc", Arrays.asList("dr_query20130301"), null, null);
		
		for (String[] record : list) {
			System.out.println(StringUtils.join(record, ";"));
		}

		System.out.println("--------------测试结束(单列)-------------");
	}
	/*
	 * hbase的行键范围查询返回结果是不包括stopKey
	public static void main(String[] agrs) throws IOException {
		conn = Connection.getInstance();
		try {
			Scan s = new Scan(Bytes.toBytes("alibab"),Bytes.toBytes("cu"));
			s.setMaxVersions();// 设置scan的版本数，若不设置，则默认返回最新的一个版本
			ResultScanner rs = new HTable(conn.getConf(),"student").getScanner(s);
			for (Result r : rs) {
				print(r);
			}
		} finally {
			conn.getThreadPool().shutdown();
		}
	}

	public static void print(Result r){ 
		for(KeyValue kv: r.raw()){
			System.out.print("rowkey : " + new String(kv.getRow()) + " "); 
			System.out.println(new String(kv.getFamily()) + ":"+ new String(kv.getQualifier()) + " = " + new String(kv.getValue()));
		} 
	}
	*/

}
