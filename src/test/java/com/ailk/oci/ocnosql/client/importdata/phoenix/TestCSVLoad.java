package com.ailk.oci.ocnosql.client.importdata.phoenix;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.ailk.oci.ocnosql.client.jdbc.phoenix.PhoenixJdbcHelper;

public class TestCSVLoad {
	
	@Test
	//单线程导入10张表的数据，每张表10000条记录
	public void testLoad() {
		long startTime = System.currentTimeMillis(); // 获取开始时间
		try {
			PhoenixCSVLoadUtil.load("ocdata05,ocdata06,ocdata07:2485", "D:\\test\\test00.sql",
                    "D:\\test\\test00.csv");
			PhoenixCSVLoadUtil.load("ocdata05,ocdata06,ocdata07:2485", "D:\\test\\test01.sql",
                    "D:\\test\\test01.csv");
			PhoenixCSVLoadUtil.load("ocdata05,ocdata06,ocdata07:2485", "D:\\test\\test02.sql",
                    "D:\\test\\test02.csv");
			PhoenixCSVLoadUtil.load("ocdata05,ocdata06,ocdata07:2485", "D:\\test\\test03.sql",
                    "D:\\test\\test03.csv");
			PhoenixCSVLoadUtil.load("ocdata05,ocdata06,ocdata07:2485", "D:\\test\\test04.sql",
                    "D:\\test\\test04.csv");
			PhoenixCSVLoadUtil.load("ocdata05,ocdata06,ocdata07:2485", "D:\\test\\test05.sql",
                    "D:\\test\\test05.csv");
			PhoenixCSVLoadUtil.load("ocdata05,ocdata06,ocdata07:2485", "D:\\test\\test06.sql",
                    "D:\\test\\test06.csv");
			PhoenixCSVLoadUtil.load("ocdata05,ocdata06,ocdata07:2485", "D:\\test\\test07.sql",
                    "D:\\test\\test07.csv");
			PhoenixCSVLoadUtil.load("ocdata05,ocdata06,ocdata07:2485", "D:\\test\\test08.sql",
                    "D:\\test\\test08.csv");
			PhoenixCSVLoadUtil.load("ocdata05,ocdata06,ocdata07:2485", "D:\\test\\test09.sql",
                    "D:\\test\\test09.csv");
			long endTime = System.currentTimeMillis(); // 获取结束时间
			System.out.println("Single thread run time:" + (endTime - startTime) + "ms");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Test
	//删除测试表的数据(一个目的是清除数据，另一个目的是初始化一下连接池)
	//再次删除测试表的数据
	public void testDel() {
		String[] sqls = new String[] { "drop table test00 ",
				"drop table test01", "drop table test02", "drop table test03 ",
				"drop table test04", "drop table test05", "drop table test06 ",
				"drop table test07", "drop table test08", "drop table test09" };

		PhoenixJdbcHelper helper = new PhoenixJdbcHelper();
		try {
			helper.excuteNonQuery(sqls, 3);
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				helper.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	
	@Test
	//多线程导入数据
	public void testBatchLoad() {
		long startTime = System.currentTimeMillis(); // 获取开始时间
		Map<String, String> pathMap = new HashMap<String, String>();
		pathMap.put("D:\\test\\test00.sql", "D:\\test\\test00.csv");
		pathMap.put("D:\\test\\test01.sql", "D:\\test\\test01.csv");
		pathMap.put("D:\\test\\test02.sql", "D:\\test\\test02.csv");
		pathMap.put("D:\\test\\test03.sql", "D:\\test\\test03.csv");
		pathMap.put("D:\\test\\test04.sql", "D:\\test\\test04.csv");
		pathMap.put("D:\\test\\test05.sql", "D:\\test\\test05.csv");
		pathMap.put("D:\\test\\test06.sql", "D:\\test\\test06.csv");
		pathMap.put("D:\\test\\test07.sql", "D:\\test\\test07.csv");
		pathMap.put("D:\\test\\test08.sql", "D:\\test\\test08.csv");
		pathMap.put("D:\\test\\test09.sql", "D:\\test\\test09.csv");
		try {
			PhoenixCSVLoadUtil.batchLoadFromFile("ocdata05,ocdata06,ocdata07:2485",
                    pathMap);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		long endTime = System.currentTimeMillis(); // 获取结束时间
		System.out.println("multithread run time:" + (endTime - startTime) + "ms");
	}
}
