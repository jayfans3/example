package com.ailk.oci.ocnosql.client.cache;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

public class TableTupleCacheTest {
	@Before
	public void setUp(){
		
	}

	@Test
	public void testRefreshAll(){
		try {
			TableTupleCache.getInstance().refreshAll();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void testRefresh(){
		String tableName = "dr_query20130114";
		try {
			TableTupleCache.getInstance().refresh(tableName);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void testGet(){
		String tableName = "";
        OciTableRef table;
		try {
			table = TableTupleCache.getInstance().get(tableName);
			System.out.println("Table Name : " + table.getName());
			System.out.println("Table Columns : " + table.getColumns());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
