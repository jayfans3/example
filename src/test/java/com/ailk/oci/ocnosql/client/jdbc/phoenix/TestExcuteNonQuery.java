package com.ailk.oci.ocnosql.client.jdbc.phoenix;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.Assert;
import org.junit.Test;
/**
 * 单条sql无需开启事物,连接会自动提交
 * @author lifei5
 *
 */
public class TestExcuteNonQuery {
	@Test
	public void testCreateTable() {
		String sql = "create table if not exists test_lifei(id varchar primary key,name varchar,age integer,note varchar) ";
		PhoenixJdbcHelper helper = new PhoenixJdbcHelper();
		try {
			helper.excuteNonQuery(sql);
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
	public void testUpsert() {
		String sql = "upsert into test_lifei values('100','lifei',28,'大家好') ";
		PhoenixJdbcHelper helper = new PhoenixJdbcHelper();
		try {
			helper.excuteNonQuery(sql);
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
	public void testDelete() {
		String sql = "delete from test_lifei";
		PhoenixJdbcHelper helper = new PhoenixJdbcHelper();
		try {
			helper.excuteNonQuery(sql);
			ResultSet res=helper.executeQueryRaw("select count(*) from test_lifei");
			while(res.next()){
				int num=res.getInt(1);
				Assert.assertTrue(num==0);
			}
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
	
	//以下方法测试PreparedStatement(Phoenix2.1目前支持的还不是很好)
	@Test
	public void testCreateTable1() {
		String sql = "create table if not exists test_lifei1(id varchar primary key,name varchar,age integer,note varchar) ";
		PhoenixJdbcHelper helper = new PhoenixJdbcHelper();
		try {
			helper.excuteNonQuery(sql, null);
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
	public void testUpsert1() {
		String sql = "upsert into test_lifei1 values(?,?,?,?) ";
		PhoenixJdbcHelper helper = new PhoenixJdbcHelper();
		try {
			Object[] args=new Object[]{"100","lifei",28,"大家好"};
			helper.excuteNonQuery(sql, args);
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
	public void testDelete1() {
		String sql = "delete from test_lifei1";
		PhoenixJdbcHelper helper = new PhoenixJdbcHelper();
		try {
			helper.excuteNonQuery(sql,null);
			ResultSet res=helper.executeQueryRaw("select count(*) from test_lifei1",null);
			while(res.next()){
				int num=res.getInt(1);
				Assert.assertTrue(num==0);
			}
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
