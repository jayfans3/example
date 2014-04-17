package com.ailk.oci.ocnosql.client.importdata.phoenix;

import org.junit.Test;

public class TestPhoenixBulkLoader {
	@Test
	public void test(){
		try {
			PhoenixBulkLoader.load("hdfs://ocdata06:8020/phoenix_mms_20130115.csv", "phoenix_mms_20130115", "D:\\phoenix_mms_20130115.sql", "ocdata05:2485", "ocdata05:8021", "hdfs://ocdata06:8020");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
