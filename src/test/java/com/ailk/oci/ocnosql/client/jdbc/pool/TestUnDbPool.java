package com.ailk.oci.ocnosql.client.jdbc.pool;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.Test;
/**
 * 不使用连接池
 * @author lifei5
 *
 */
public class TestUnDbPool {
	
	@Test
	public void unDbPool(String[] args) {
		System.out.println("不使用连接池................................");
		for (int i = 0; i < 20; i++) {
			final int k=i;
			new Thread(new Runnable() {
				@Override
				public void run() {
					long beginTime = System.currentTimeMillis();
					
					Connection conn =null;
					try {
						Class.forName("com.salesforce.phoenix.jdbc.PhoenixDriver");
						conn =DriverManager.getConnection("jdbc:phoenix:ocdata05,ocdata06,ocdata07:2485");
						Statement statement = conn.createStatement();
						ResultSet rs = statement
								.executeQuery("select * from student");
						while (rs.next()) {
							// do nothing...
						}
					} catch (SQLException e) {
						e.printStackTrace();
					} catch (ClassNotFoundException e) {
						e.printStackTrace();
					} finally {
						try {
							if(null!=conn){
								conn.close();
							}
						} catch (SQLException e) {
							e.printStackTrace();
						}
					}
					long endTime = System.currentTimeMillis();
					System.out.println("第" + (k + 1) + "次执行花费时间为:"
							+ (endTime - beginTime));
				}
			}).start();
		}
	}
}
