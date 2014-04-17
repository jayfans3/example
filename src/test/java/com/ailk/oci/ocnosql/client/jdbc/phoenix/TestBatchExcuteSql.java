package com.ailk.oci.ocnosql.client.jdbc.phoenix;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

/**
 * 本用例主要测试批量操作接口
 * @author lifei5
 *
 */
public class TestBatchExcuteSql {
	
	/**
	 * 以statement方式批量执行sql
	 * excuteNonQuery(String[] sqls, int batchSize)
	 */
	@Test
	public void test1(){
		String[] sqls=new String[]{
				"upsert into test_lifei values('101','lifei1',21,'大家好1') ",
				"upsert into test_lifei values('102','lifei2',22,'大家好2') ",
				"upsert into test_lifei values('103','lifei3',23,'大家好3') ",
				"upsert into test_lifei values('104','lifei4',24,'大家好4') ",
				"upsert into test_lifei values('105','lifei5',25,'大家好5') ",
				"upsert into test_lifei values('106','lifei6',26,'大家好6') ",
				"upsert into test_lifei values('107','lifei7',27,'大家好7') ",
				"upsert into test_lifei values('108','lifei8',28,'大家好8') "
		};
		
		PhoenixJdbcHelper helper = new PhoenixJdbcHelper();
		try {
			helper.excuteNonQuery(sqls, 3);
		} catch (SQLException e) {
			e.printStackTrace();
			Assert.fail();
		}finally{
			try {
				helper.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	
	@Test
	public void test2(){
		String[] sqls=new String[]{
				"delete from test_lifei where id=?",
				"delete from test_lifei where id=?",
				"delete from test_lifei where id=?",
				"delete from test_lifei where id=?",
				"delete from test_lifei where id=?",
				"delete from test_lifei where id=?",
				"delete from test_lifei where id=?",
				"delete from test_lifei where id=?",
		};
		
		List<Object[]> args=new ArrayList<Object[]>();
		args.add(new Object[]{"101"});
		args.add(new Object[]{"102"});
		args.add(new Object[]{"103"});
		args.add(new Object[]{"104"});
		args.add(new Object[]{"105"});
		args.add(new Object[]{"106"});
		args.add(new Object[]{"107"});
		args.add(new Object[]{"108"});
		
		PhoenixJdbcHelper helper = new PhoenixJdbcHelper();
		try {
			helper.excuteNonQuery(sqls, args, 3);
		} catch (SQLException e) {
			e.printStackTrace();
			Assert.fail();
		}finally{
			try {
				helper.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
}
