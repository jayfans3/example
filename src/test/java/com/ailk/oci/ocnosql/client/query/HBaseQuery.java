package com.ailk.oci.ocnosql.client.query;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

public class HBaseQuery {

	@Test
	public void getHDFSFile() throws IOException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://mycluster");
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] files = fs.listStatus(new Path("/"));
		for (FileStatus file : files) {
			System.out.println(file.getPath().toUri().getPath());
		}
	}

	@Test
	public void query() throws IOException {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "ocdc03,ocdc04,ocdc02");
		conf.set("hbase.zookeeper.property.clientPort", "2485");
		String rowkey = "13429100019";
		String tableName = "dr_query20130302";
		HTable table = new HTable(conf, tableName);
		Get get = new Get(rowkey.getBytes());
		Result result = table.get(get);
		System.out.println(Bytes.toString(result.getRow()));
	}
}
