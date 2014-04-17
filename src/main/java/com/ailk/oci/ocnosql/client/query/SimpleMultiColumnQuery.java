package com.ailk.oci.ocnosql.client.query;

import static java.util.Collections.EMPTY_LIST;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.NoServerForRegionException;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.ailk.oci.ocnosql.client.config.spi.Connection;
import com.ailk.oci.ocnosql.client.query.schema.OCTable;
import com.ailk.oci.ocnosql.client.rowkeygenerator.RowKeyGenerator;
import com.ailk.oci.ocnosql.client.spi.ClientConnectionException;
import com.ailk.oci.ocnosql.client.util.HTableUtilsV2;

public class SimpleMultiColumnQuery {
	
	public OCResultSet getDBResult(List<Result> result) {
        OCResultSet resultSet = new OCResultSet();
        if(result!=null&&!result.isEmpty()){
            OCTable currentOCTable = new OCTable();
            for (Result res : result) {
                int index = 0;
                //<列族,<列名<version，value>>>
                NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> allFamiliesData = res.getMap();
                
                if(allFamiliesData==null){
                	return resultSet;
                }
                for (byte[] family : allFamiliesData.keySet()) {
                    NavigableMap<byte[], NavigableMap<Long, byte[]>> currentRow = allFamiliesData.get(family);
                    int columnSize = currentRow.size();
                    String currentRowdata[] = new String[columnSize];
                    int columnIndex = 0;
                    for (byte[] column : currentRow.keySet()) {
                        currentOCTable.addColumn(Bytes.toString(column), columnIndex);
                        columnIndex++;
                        NavigableMap<Long, byte[]> allVersionsCell = currentRow.get(column);
                        for (long version : allVersionsCell.keySet()) {
                            currentRowdata[index] = new String(allVersionsCell.get(version));
                        }
                        index++;
                    }
                    resultSet.insertRow(currentRowdata);
                }
            }
            resultSet.setCurrentOCTable(currentOCTable);
        }

        return resultSet;
    }
	private List<Result>  getRawResult(String tablename, String rowkey,String rowkeyGenerator) throws Exception{
        HTableInterface table = null;
        Configuration hbaseConf = Connection.getInstance().getConf();
		List<Result>  list = new ArrayList<Result>();
		try {
			table = HTableUtilsV2.getTable(hbaseConf, tablename);
			RowKeyGenerator generator = com.ailk.oci.ocnosql.client.rowkeygenerator.RowKeyGeneratorHolder.resolveGenerator(rowkeyGenerator);
			if(generator!=null){
				rowkey = (String) generator.generate(rowkey);
			}

			Get get = new Get(rowkey.getBytes());
			get.setCacheBlocks(false);
			get.setMaxVersions();
			Result result = null;
			result = table.get(get);
			if(result == null || result.size() == 0){
				return null;
			}
			list.add(result);
			return list;
		} 
		catch (Exception e1) {
			handlerException(tablename, e1);
		}
		finally{
			table.close();
		}
		return EMPTY_LIST;
	}

	private void handlerException(String tablename, Exception e1)
			throws Exception {
		if(e1 instanceof org.apache.hadoop.hbase.TableNotFoundException||
				(e1.getCause()!=null&& e1.getCause() instanceof org.apache.hadoop.hbase.TableNotFoundException)){
			throw new TableNotFoundException("failed get table from hbase ds, caused by " + e1.getLocalizedMessage());
		} 
		else if (e1 instanceof ZooKeeperConnectionException||
				(e1.getCause()!=null&& e1.getCause() instanceof ZooKeeperConnectionException)){
			throw new ClientConnectionException("failed connect zookeeper, caused by " + e1.getLocalizedMessage());
		} 
		else if (e1 instanceof NoServerForRegionException||
				(e1.getCause()!=null&& e1.getCause() instanceof NoServerForRegionException)){
			throw new ClientConnectionException("faild init HTable object,caused by searching region occur error, caused by " + e1.getLocalizedMessage());
		}
		else {
			throw new Exception("connect table " + tablename + " occur error, caused by " + e1.getLocalizedMessage());
		}
	} 
}
