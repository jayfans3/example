package com.ailk.oci.ocnosql.client.query;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;

import com.ailk.oci.ocnosql.client.rowkeygenerator.*;
import com.ailk.oci.ocnosql.common.util.PropertiesUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.NoServerForRegionException;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RetriesExhaustedException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.ailk.oci.ocnosql.client.ClientRuntimeException;
import com.ailk.oci.ocnosql.client.cache.OciTableRef;
import com.ailk.oci.ocnosql.client.cache.TableMetaCache;
import com.ailk.oci.ocnosql.client.config.spi.CommonConstants;
import com.ailk.oci.ocnosql.client.query.criterion.Criterion;
import com.ailk.oci.ocnosql.client.query.schema.OCTable;
import com.ailk.oci.ocnosql.client.spi.ClientConnectionException;
import com.ailk.oci.ocnosql.client.spi.ConfigException;
import com.ailk.oci.ocnosql.client.util.HTableUtils;

/**
 * 功能：多列方式查询线程
 */
public class QueryMultiColumnActor extends QueryActor {
	
    private static Log log = LogFactory.getLog(QueryMultiColumnActor.class);

    public QueryMultiColumnActor(Configuration conf, OciTableRef tableInternal, Criterion criterion, CountDownLatch runningThreadNum) {
        super(conf, tableInternal, criterion, runningThreadNum);
    }

    
    public void run() {
        String tableName = tableInternal.getName();
        String[] rowkey = tableInternal.getRowkey();
//        if(tableInternal.getColumnFamilies() != null && tableInternal.getColumnFamilies().size() > 0) {
//        	String[] columns = tableInternal.getColumnFamilies().get(0).getColumns();
//        	for(int i = 0; i < columns.length; i++){
//        		if(columns[i].equals(CommonConstants.ROW_KEY)){
//        			this.rowkeyIndex = i;
//        			break;
//        		}
//        	}
//        }
        try {
            if (log.isDebugEnabled()) {
                log.debug("[ocnosql]start to search, table=" + tableName + ", rowkey=" + StringUtils.join(rowkey, ", "));
            }
            if (StringUtils.isEmpty(tableName)) {
                log.error("[ocnosql]Not set tablename in query object");
                return;
            }
            if((rowkey == null || rowkey.length == 0)){
            	if(StringUtils.isEmpty(tableInternal.getStartKey())){
            		log.error("[ocnosql]Not set startKey in query object");
                    return;
            	}
            	if(StringUtils.isEmpty(tableInternal.getStopKey())){
            		log.error("[ocnosql]Not set stopKey in query object");
                    return;
            	}
            }
            //是否有多线程问题？
            result = search(tableName, rowkey, tableInternal.getStartKey(), tableInternal.getStopKey());
            if (log.isDebugEnabled()) {
            	StringBuilder sb = new StringBuilder();
            	sb.append("[ocnosql]end to search, table=" + tableName + ",");
            	if(rowkey == null){
            		
            	}
                log.debug("[ocnosql]end to search, table=" + tableName + ", rowkey=" + StringUtils.join(rowkey, ", "));
            }
        } catch (Exception e) {
            queue.add(e);
        }finally{
        	runningThreadNum.countDown();
        }
    }
    
    
    protected OCResultSet search(String tablename, String rowkey[], String startKey, String stopKey) throws Exception {

        OCResultSet resultSet = getDBResult(getRawResult(tablename, rowkey, startKey, stopKey));
        //将表的原型信息放入缓存，代码待优化，进行方法抽离
        TableMetaCache tableMetaCache = TableMetaCache.getInstance();
        if(!tableMetaCache.containCache(tablename)){
            OCTable ocTable = resultSet.getCurrentOCTable();
            if(ocTable!=null){
                ocTable.setName(tablename);
                tableMetaCache.addOneMeta2Cache(ocTable);
            }
        }
        return resultSet;
    }
    
    
    protected List<Result> getRawResult(String tablename, String[] rowkey, String startKey, String stopKey) throws Exception {
        HTableInterface table = createHTable(tablename);
        if (table == null) return null;
        List<Result> results = null;
        try{
	        RowKeyGenerator generator = RowKeyGeneratorHolder.resolveGenerator(tableInternal.getRowkeyGenerator());
	        if(generator != null){
	        	for(int i = 0; i < rowkey.length; i++){
	                rowkey[i] = (String) generator.generate(rowkey[i]);
	        	}
	        }
	        if (rowkey == null || rowkey.length == 0){	//区间查询
	            results = this.queryByPrefix(tablename, table, startKey, stopKey);
	        } else {
	            results = this.queryByGet(tablename, table, rowkey);
	        }
        }finally{
        	table.close();
        }
        return results;
    }


    private HTableInterface createHTable(String tablename) throws Exception {
        HTableInterface table = null;
        try {
            table = HTableUtils.getTable(conf, tablename);
        } catch (Exception e1) {
            exceptionHbase(tablename, e1);
        }
        if (table == null) {
            if (log.isErrorEnabled()) {
                log.error("[ocnosql]can't connect table " + tablename + " or table " + tablename + " is not exist.");
            }
            return null;
        }
        return table;
    }

    
    
    
    
    /**
     * 根据startKey和stopKey查询
     * @param tablename
     * @param table
     * @param startKey
     * @param stopKey
     * @return
     * @throws IOException
     */
    private List<Result> queryByPrefix(String tablename, HTableInterface table, String startKey, String stopKey) throws IOException {
        List<Result>  list = new ArrayList<Result>();
        ResultScanner resultScanner = null;
        try {
        	log.info("tablename=" + tablename + ", startKey=" + startKey + ", stopKey=" + stopKey);
        	byte[] stopKeyBytes = stopKey.getBytes();
        	stopKeyBytes[stopKeyBytes.length - 1] ++;
            Scan scan = new Scan(startKey.getBytes(), stopKeyBytes);
            scan.setCaching(conf.getInt(CommonConstants.QUERY_CACHE_SIZE, 1));
            
            List<ColumnFamily> columnFamilies = tableInternal.getColumnFamilies();
            if(columnFamilies != null){
           	 	for(int i = 0; i < columnFamilies.size(); i++){
    	       		 for(int j = 0; j < columnFamilies.get(i).getColumns().length; j++){
    	       			 scan.addColumn(columnFamilies.get(i).getFamily().getBytes(), columnFamilies.get(i).getColumns()[j].getBytes());
    	       		 }
           	 	}
            }

            if(criterion!=null){
                scan.setFilter(criterion.genFilter());
            }
            resultScanner = table.getScanner(scan);
            Iterator<Result> iteratorResult = resultScanner.iterator();
            long maxSize = Long.valueOf(PropertiesUtil.getProperty( CommonConstants.FILE_NAME,
                    CommonConstants.QUERY_RESULT_MAX_SIZE, CommonConstants.DEFAULT_QUERY_RESULT_MAX_SIZE.toString()) );
            long resultSize = 0;
            while (iteratorResult.hasNext()){
                list.add(iteratorResult.next());
                resultSize++;
                if(resultSize>=maxSize) {
                    log.error("Too many records returned:>=" + maxSize + ". Auto cutted! Please check the condition");
                    log.error("Please check the condition:"+scan.toJSON());
                    break;
                }
            }
        } catch (RetriesExhaustedException e) {
            log.error("[ocnosql]connect region server occur error,nested exception:" + e.getMessage() + ".");
//            if (table != null) table.clearRegionCache();
            throw new ClientConnectionException("connect region server occur error, caused by " + e.getMessage());
        } catch (NoServerForRegionException e) {
            log.error("searching region occur error, cased by no server for region.");
//            if (table != null) table.clearRegionCache();
            throw new ClientConnectionException("searching region occur error, caused by " + e.getMessage());
        } catch (IOException e) {
            throw new ClientRuntimeException("search table " + tablename + " occur error, caused by " + e.getMessage());
        } catch (Exception e) {
            throw new ClientRuntimeException("search table " + tablename + " occur error, caused by " + e.getMessage());
        } finally {
        	if(resultScanner != null){
        		resultScanner.close();
        	}
        }
        return list;
    }
    
    
    
    /**
     * 根据Get查询
     * @param tablename
     * @param table
     * @param rowkey
     * @return
     */
    private List<Result> queryByGet(String tablename, HTableInterface table, String[] rowkey){
    	List<Get>  getList = new ArrayList<Get>();
        for(int i = 0; i<rowkey.length;i++){
             Get get = new Get(rowkey[i].getBytes());
             get.setCacheBlocks(true);
             if(criterion!=null) {
                 get.setFilter(criterion.genFilter());
             }
             getList.add(get);
        }
    	
        TableMetaCache tableMetaCache = TableMetaCache.getInstance();
        OCTable ocTable = tableMetaCache.getTableCache(tableInternal.getName());
        if(ocTable == null){
            if(log.isInfoEnabled()){
                log.info("We can't retrieve OCTable from cache via table["+tableInternal.getName()+"]");
            }
        }

        Result[] result = null;
        try {
            result = table.get(getList);
        }
        catch (RetriesExhaustedException e) {
            log.error("[ocnosql]connect region server occur error,nested exception:" + e.getMessage() + ".");
            //TODO 这个还需要吗？
//            if(table!=null) table.clearRegionCache();
            throw new ClientConnectionException("connect region server occur error, caused by " + e.getMessage());
        }
        catch (NoServerForRegionException e) {
            log.error("searching region occur error, cased by no server for region.");
//            if(table!=null) table.clearRegionCache();
            throw new ClientConnectionException("searching region occur error, caused by " + e.getMessage());
        }
        catch (IOException e) {
            throw new ClientRuntimeException("search table " + tablename + " occur error, caused by " + e.getMessage());
        }
        if(result == null || result.length == 0){
            return null;
        }
        List<Result>  results = new ArrayList<Result>();
        results.addAll(Arrays.asList(result));
        return results;

    }

    private  OCResultSet getDBResult(List<Result> resultList) {
        OCResultSet resultSet = new OCResultSet();
        if(resultList != null && !resultList.isEmpty()){
            OCTable currentOCTable = new OCTable();
            List<ColumnFamily> columnFamilies = tableInternal.getColumnFamilies();
            if(tableInternal.getColumnFamilies() == null || tableInternal.getColumnFamilies().size() == 0){
	            for (Result result : resultList) {
	                int index = 0;
	                //<列族,<列名<version，value>>>
	                NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> allFamiliesData = result.getMap();
	                
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
	                        KeyValue kv = result.getColumnLatest(family, column);
	                        currentRowdata[index] = Bytes.toString(kv.getValue());
	                        index++;
	                    }
	                    resultSet.insertRow(currentRowdata);
	                }
	            }
            }else {
            	for (Result result : resultList) {
                    //当按照rowkey和filter查询不出相应的结果时，会产生一条size为0的记录，需要忽略掉。
                    if(result.size()<=0) continue;
            		for(int i = 0; i < columnFamilies.size(); i++){
            			String[] rowData = new String[columnFamilies.get(i).getColumns().length];
            			byte[] f = columnFamilies.get(i).getFamily().getBytes();
	            		for(int j = 0; j < columnFamilies.get(i).getColumns().length; j++){
	            			String columName = columnFamilies.get(i).getColumns()[j];
	            			currentOCTable.addColumn(columName, j);
	            			if(!columName.equals(CommonConstants.ROW_KEY)) {
	            				if(!result.containsColumn(f, columName.getBytes())){
	            					throw new ClientRuntimeException("column field: '" + columName + "' is not exsit.");
	            				}
		            			KeyValue kv = result.getColumnLatest(f, columName.getBytes());
		            			rowData[j] = Bytes.toString(kv.getValue());
	            			}else{
	            				rowData[j] = Bytes.toString(result.getRow());
	            			}
	            		}
	            		resultSet.insertRow(rowData);
            		}
            	}
            }
            resultSet.setCurrentOCTable(currentOCTable);
        }
        return resultSet;
    }
    
    
    /**
     * 将返回的结果根据条件过滤
     * @param resultSet
     * @param criterion
     * @return
     * @throws Exception
     */
    private List<String[]> doMutiFilter(OCResultSet resultSet, Criterion criterion) throws Exception {
        if(null == criterion){
            return resultSet.getData();
        }
        List<String[]> dest = new ArrayList<String[]>();
        String[] detail = null;
        try {
            OCTable currentOCTable = resultSet.getCurrentOCTable();
            for (int j = 0; j < resultSet.getSize(); j++) {
                // 获取一条数据
                detail = resultSet.getData().get(j);
                if(detail.length > currentOCTable.getColumnSize()){
                    throw new ConfigException("The data column is greater than the number of current OCTable columns");
                }
                boolean match = visitQueryParam(detail,criterion, currentOCTable);
                if (match) {
                    // 将rowkey添加到返回结果中
                    if (rowkeyIndex != -1) {
                        String[] newDetail = new String[detail.length + 1];
                        System.arraycopy(detail, 0, newDetail, 0, rowkeyIndex);
                        //newDetail[rowkeyIndex] = tableInternal.getRowkey();
                        System.arraycopy(detail, rowkeyIndex, newDetail,rowkeyIndex + 1, detail.length - rowkeyIndex);
                        dest.add(newDetail);
                    } else {
                        dest.add(detail);
                    }
                }
            }
        }
        catch (Exception e) {
            exceptionFilter(dest, detail, e);
            return dest;
        }
        return dest;
    }

    
    private void exceptionFilter(List<String[]> dest, String[] detail, Exception e) throws Exception {
        dest.clear();
        if(e instanceof ArrayIndexOutOfBoundsException){
            StringBuilder err = new StringBuilder();
            err.append("[ocnosql]do filter occur error ,detail record [");
            if(detail == null){
                err.append(" null ]");
                log.error(err);
            }else {
                for(String str : detail){
                    err.append(str + ",");
                }
                if(err.charAt(err.length() - 1) == ',')err.deleteCharAt(err.length() - 1);
                log.error(err + "]");
            }
        } else {
            throw e;
        }
    }
    
    
    private void exceptionHbase(String tablename, Exception e1) throws Exception {
    	log.error("", e1);
        if (e1 instanceof org.apache.hadoop.hbase.TableNotFoundException ||
                (e1.getCause() != null && e1.getCause() instanceof org.apache.hadoop.hbase.TableNotFoundException)) {
            throw new TableNotFoundException("failed get table from hbase ds, caused by " + e1.getLocalizedMessage());
        } else if (e1 instanceof ZooKeeperConnectionException ||
                (e1.getCause() != null && e1.getCause() instanceof ZooKeeperConnectionException)) {
            throw new ClientConnectionException("failed connect zookeeper, caused by " + e1.getLocalizedMessage());
        } else if (e1 instanceof NoServerForRegionException ||
                (e1.getCause() != null && e1.getCause() instanceof NoServerForRegionException)) {
            throw new ClientConnectionException("failed init HTable object,caused by searching region occur error, caused by " + e1.getLocalizedMessage());
        } else {
            throw new Exception("connect table " + tablename + " occur error, caused by " + e1.getLocalizedMessage());
        }
    }


}
