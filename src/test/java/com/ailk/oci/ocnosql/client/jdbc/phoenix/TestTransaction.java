package com.ailk.oci.ocnosql.client.jdbc.phoenix;

import java.sql.SQLException;

import org.junit.Assert;
import org.junit.Test;

/**
 * 本用例用来测试事务Transaction
 * @author lifei5
 *
 */
public class TestTransaction {
	@Test
	public void test(){
		String[] sqls=new String[]{
				"upsert into test_lifei values('101','lifei1',21,'大家好1') ",
				"upsert into test_lifei values('102','lifei2',22,'大家好2') ",
				"upsert into test_lifei values('103','lifei3',23,'大家好3') ",
				"upsert into test_lifei111 values('104','lifei4',24,'大家好4') ",//这个表不存在，故意报错
				"upsert into test_lifei values('105','lifei5',25,'大家好5') ",
				"upsert into test_lifei values('106','lifei6',26,'大家好6') ",
				"upsert into test_lifei values('107','lifei7',27,'大家好7') ",
				"upsert into test_lifei values('108','lifei8',28,'大家好8') "
		};
		//1.初始化helper
		PhoenixJdbcHelper helper = new PhoenixJdbcHelper();
		try {
			//2.开启事物
			helper.beginTransaction();
			
			//3.执行jdbc操作
			for(String sql:sqls){
				helper.excuteNonQuery(sql);
			}
			
			//4.提交事务
			helper.commitTransaction();
		} catch (SQLException e) {
			e.printStackTrace();
			//5.回滚事务
			try {
				helper.rollbackTransaction();
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
			Assert.fail();
		}finally{
			try {
				//6.关闭helper(关闭连接)
				helper.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
}
