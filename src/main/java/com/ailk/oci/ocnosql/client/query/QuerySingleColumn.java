package com.ailk.oci.ocnosql.client.query;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

import com.ailk.oci.ocnosql.client.cache.OciTableRef;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.ailk.oci.ocnosql.client.ClientRuntimeException;
import com.ailk.oci.ocnosql.client.config.spi.CommonConstants;
import com.ailk.oci.ocnosql.client.config.spi.Connection;
import com.ailk.oci.ocnosql.client.query.criterion.Criterion;
import com.ailk.oci.ocnosql.client.spi.ClientConnectionException;

public class QuerySingleColumn implements Query {
	private final static Log log = LogFactory.getLog(QuerySingleColumn.class.getSimpleName());
	private String msg;
	
	
	@Override
	public List<String[]> query(Connection conn, String[] rowkey, List<String> tableNames, Criterion criterion, List<ColumnFamily> columnFamilies) throws ClientRuntimeException {
		checkParamNotEmpty(conn, rowkey,tableNames);
		try {
			List<String[]> result = search(conn, rowkey, null, null, tableNames, criterion, columnFamilies);
			return result;
		} catch (Exception e) {
			msg = "search occur error, cause by " + e;
			log.error(msg);
			throw new ClientRuntimeException(msg, e);
		}
	}

	
	@Override
	public List<String[]> query(Connection conn, String startKey, String stopKey, List<String> tableNames, Criterion criterion, List<ColumnFamily> columnFamilies) throws ClientRuntimeException {
		
		if(StringUtils.isEmpty(startKey) || StringUtils.isEmpty(stopKey) ){
			String errorMsg = "startKey or stopKey must not be null";
            log.error(errorMsg);
            throw new ClientRuntimeException(errorMsg);
		}
		
		if(conn == null){
		    String errorMsg = "Connection object must not be null";
		    log.error(errorMsg);
		    throw new ClientRuntimeException(errorMsg);
		}
		
		if(tableNames == null || tableNames.size()==0){
		    String errorMsg = "tableNames must not be null";
		    log.error(errorMsg);
		    throw new ClientRuntimeException(errorMsg);
		}
		
		try {
			List<String[]> result = search(conn, null, startKey, stopKey, tableNames, criterion, columnFamilies);
			return result;
		} catch (Exception e) {
			msg = "search occur error, cause by " + e;
			log.error("search occur error, cause by ",e);
			throw new ClientRuntimeException(msg);
		}
	}
	

	private void checkParamNotEmpty(Connection conn, String[] rowkey, List<String> tableNames) throws ClientRuntimeException{
		if(conn == null){
		    String errorMsg = "Connection object must not be null";
		    log.error(errorMsg);
		    throw new ClientRuntimeException(errorMsg);
		}
		
		if(tableNames == null || tableNames.size()==0){
		    String errorMsg = "tableNames must not be null";
		    log.error(errorMsg);
		    throw new ClientRuntimeException(errorMsg);
		}
		
		if(rowkey.length == 0) {
			 String errorMsg = "rowkey must not be null";
		     log.error(errorMsg);
		     throw new ClientRuntimeException(errorMsg);
		}
	}
	
	 
	@Override
	public List<String[]> executeSql(Connection conn, String sql)
			throws ClientRuntimeException {
		throw new ClientConnectionException("yet implement method execute");
	}
	
	
	@SuppressWarnings("rawtypes")
	private List<String[]> search(Connection conn, String rowkey[], String startKey, String stopKey, List<String> tableNames, Criterion criterion, List<ColumnFamily> columnFamilies) throws Exception{
		List<String[]> ret = new ArrayList<String[]>();
		CountDownLatch runningThreadNum = new CountDownLatch(tableNames.size());
		List<QuerySingleColumnActor> threadList = new ArrayList<QuerySingleColumnActor>();
		List<Future> futures = new ArrayList<Future>();
		LinkedBlockingQueue<Exception> exceptionQueue = new LinkedBlockingQueue<Exception>();
		
		for(String tableName : tableNames){
			OciTableRef tableInternal =  new OciTableRef();
			tableInternal.setName(tableName);
			tableInternal.setRowkey(rowkey);
			tableInternal.setStartKey(startKey);
			tableInternal.setStopKey(stopKey);
			tableInternal.setRowkeyGenerator(conn.getConf().get(CommonConstants.ROWKEY_GENERATOR));
			tableInternal.setCompressor(conn.getConf().get(CommonConstants.COMPRESSOR));
			tableInternal.setSeperator(conn.getConf().get(CommonConstants.SEPARATOR));
			tableInternal.setColumnFamilies(columnFamilies);
			QuerySingleColumnActor queryThread = new QuerySingleColumnActor(conn.getConf(), tableInternal, criterion, runningThreadNum);
			queryThread.setQueue(exceptionQueue);
			futures.add(conn.getThreadPool().submit(queryThread));
			threadList.add(queryThread);
		}
		
		runningThreadNum.await();
        if (exceptionQueue.size() != 0) {
            Exception e = exceptionQueue.poll();
            throw e;
        }
        
		for (QuerySingleColumnActor queryThread : threadList) {
			List<String[]> tempList = queryThread.getResult().getData();
			if (!CollectionUtils.isEmpty(tempList)) {
				ret.addAll(tempList);
			}
		}
		log.info("[ocnosql]search completed,return " + ret.size() + " records");
		
		return ret;
	}

	
}
