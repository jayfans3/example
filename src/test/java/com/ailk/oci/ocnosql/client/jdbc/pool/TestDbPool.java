package com.ailk.oci.ocnosql.client.jdbc.pool;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.Test;
/**
 * 使用连接池
 */
public class TestDbPool {
	
	@Test
	public void dbPool(String[] args) throws Exception {
		System.out.println("使用连接池................................");
		
		//先调用一下getConnection排除锁的干扰(第一次获取连接的时候会初始化线程池，所有线程都会等待的)
        long beginTime = System.currentTimeMillis();
		Connection conn = DbPool.getConnection();
		DbPool.closeConnection();
        long endTime = System.currentTimeMillis();
					System.out.println("初始化连接池花费时间为:"
							+ (endTime - beginTime));
		for (int i = 0; i < 20; i++) {
			final int k=i;
			new Thread(new Runnable() {
				
				@Override
				public void run() {
					long beginTime = System.currentTimeMillis();
					Connection conn = DbPool.getConnection();
					try {
						Statement statement = conn.createStatement();
						ResultSet rs = statement.executeQuery("select * from student");
						while (rs.next()) {
							// do nothing...
						}
					} catch (SQLException e) {
						e.printStackTrace();
					} finally {
						DbPool.closeConnection();
					}
					long endTime = System.currentTimeMillis();
					System.out.println("第" + (k + 1) + "次执行花费时间为:"
							+ (endTime - beginTime));
				}
			}).start();
		}
	}
}
