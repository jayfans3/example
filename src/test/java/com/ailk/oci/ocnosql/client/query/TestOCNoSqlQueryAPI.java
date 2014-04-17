/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.ailk.oci.ocnosql.client.query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.junit.Test;

import com.ailk.oci.ocnosql.client.config.spi.Connection;
import com.ailk.oci.ocnosql.client.spi.ClientAdaptor;

/**
 * 
 * @author liuxiang2
 */
public class TestOCNoSqlQueryAPI {
	private static Connection conn = null;

	public void queryGetSigleKey(String rowkey, String tablename,
			List columnfamilies) {
		System.out.println("begin hbase get ");
		ClientAdaptor client = new ClientAdaptor();
		List<String[]> list = client.queryByRowkey(conn, rowkey,
				Arrays.asList(tablename), null, null, columnfamilies);
		System.out.println("lenth is " + list.size());
		for (String[] result : list) {
			System.out.println("result is " + StringUtils.join(result, ";"));
		}
	}

	public void queryGetMultiKey(String[] rowkey, String tablename,
			List columnfamilies) {
		System.out.println("begin hbase get ");
		ClientAdaptor client = new ClientAdaptor();
		List<String[]> list = client.queryByRowkey(conn, rowkey,
				Arrays.asList(tablename), null, null, columnfamilies);
		System.out.println("lenth is " + list.size());
		for (String[] result : list) {
			System.out.println("result is " + StringUtils.join(result, ";"));
		}
	}

	public void queryRowKeyPreFix(String rowkey, String tablename,
			List columnfamilies) {
		System.out.println("begin hbase get ");
		ClientAdaptor client = new ClientAdaptor();
		List<String[]> list = client.queryByRowkeyPrefix(conn, rowkey,
				Arrays.asList(tablename), null, null, columnfamilies);
		System.out.println("lenth is " + list.size());
		for (String[] result : list) {
			System.out.println("result is " + StringUtils.join(result, ";"));
		}
	}

	public void queryRowKeyRange(String startKey, String EndKey,
			String tablename, List columnfamilies) {
		System.out.println("begin hbase get ");
		ClientAdaptor client = new ClientAdaptor();
		List<String[]> list = client.queryByRowkey(conn, startKey, EndKey,
				Arrays.asList(tablename), null, null, columnfamilies);
		System.out.println("lenth is " + list.size());
		for (String[] result : list) {
			System.out.println("result is " + StringUtils.join(result, ";"));
		}
	}

	@Test
	public void queryRowKeyByCondition(String[] args) {
		if (args.length < 4) {
			System.out
					.println("Usage: Need at least four parameter: CaseId, Column/NoColumn, tableName and rowKey");
			System.exit(0);
		}

		System.out.println("connect HBase");
		String caseId = args[0];
		String columFam = args[1];
		String tableName = args[2];
		// String caseId = "Range"; // Sigle Multi PreFix Range
		// String columFam = "columnFamilies"; // null columnFamilies
		// String tableName = "AUTO_TEST_BY_LX4";
		int lenth = args.length;
		String rowKey; // ="9c0138666666666";
		String mutiRowKey[] = new String[lenth - 3]; // =
														// {"9c0138666666666","ce3222"};
		String preFixRowKey; // = "4";
		String startKey; // = "0c7138555555555";
		String endKey; // = "8e22";
		List<ColumnFamily> columnFamilies = new ArrayList<ColumnFamily>();
		conn = Connection.getInstance();

		if (columFam.equals("NoColumn")) {
			columnFamilies = null;
		} else {
			ColumnFamily cf = new ColumnFamily();
			cf.setFamily("F");
			cf.setColumns(new String[] { "A1", "A2", "A22" });
			columnFamilies.add(cf);
		}

		System.out.println("conn =" + conn.getThreadPool().isTerminated());

		System.out.println(caseId + " Query with  " + columFam);

		if ("Sigle".equals(caseId)) {
			rowKey = args[3];
			queryGetSigleKey(rowKey, tableName, columnFamilies);
		} else if ("Multi".equals(caseId)) {
			for (int i = 3; i < args.length; i++) {
				mutiRowKey[i - 3] = args[i];
				System.out.println("multiRowKey is " + mutiRowKey[i - 3]);
			}
			queryGetMultiKey(mutiRowKey, tableName, columnFamilies);
		} else if ("PreFix".equals(caseId)) {
			preFixRowKey = args[3];
			queryRowKeyPreFix(preFixRowKey, tableName, columnFamilies);
		} else if ("Range".equals(caseId)) {
			startKey = args[3];
			endKey = args[4];
			queryRowKeyRange(startKey, endKey, tableName, columnFamilies);
		} else {
			System.out.println("No Case match");
			System.out
					.println("Usage: firs Param must be one os 'Sigle  Multi  PreFix  Range'");
		}

		conn.getThreadPool().shutdown();
		System.out.println("release connection and conn is "
				+ conn.getThreadPool().isTerminated());
	}

}
