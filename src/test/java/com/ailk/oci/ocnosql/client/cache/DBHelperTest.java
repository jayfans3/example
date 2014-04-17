package com.ailk.oci.ocnosql.client.cache;

import java.sql.SQLException;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

public class DBHelperTest {
	private DBHelper dbhelper;
	String driver = "oracle.jdbc.OracleDriver";
	String url = "jdbc:oracle:thin:@10.10.141.213:1521:cszg3";
	String dbname = "drquery";
	String dbpass = "drquery";
	
	@Before
	public void setUp(){
		dbhelper = new DBHelper(driver, url, dbname, dbpass);
	}

	@Test
	public void testExecuteQuery(){
		String sql = "select * from OC_TABLE where TABLE_STATUS='Active'";
		try {
			List<OciTableRef> tableList = dbhelper.executeQuery(sql);
			for(OciTableRef table : tableList){
				System.out.println("******************" + table.getName() + "****************");
				System.out.println("Table Type : " + table.getColumns());
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
}
