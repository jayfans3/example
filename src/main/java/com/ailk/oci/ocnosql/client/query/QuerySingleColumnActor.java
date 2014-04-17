package com.ailk.oci.ocnosql.client.query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import com.ailk.oci.ocnosql.client.cache.OciTableRef;
import com.ailk.oci.ocnosql.client.config.spi.*;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.ailk.oci.ocnosql.client.query.criterion.Criterion;

/**
 * 功能：单列方式查询线程
 * 
 */
public class QuerySingleColumnActor  extends QueryActor {
	private static Log log = LogFactory.getLog(QuerySingleColumnActor.class);
	
	public QuerySingleColumnActor(Configuration conf, OciTableRef tableInternal, Criterion criterion, CountDownLatch runningThreadNum) {
		super(conf, tableInternal, criterion, runningThreadNum);
		//初始化字段信息
		modelMap = getModelMap(TableConfiguration.getInstance().getTableInfo(tableInternal.getName(), CommonConstants.COLUMNS, conf));
	}

	public void run() {
		String tableName = tableInternal.getName();
		String[] rowkey = tableInternal.getRowkey();
		try {
			if(log.isDebugEnabled()){
				log.debug("[ocnosql]start to search,table=" + tableName + ",rowkey=" + StringUtils.join(rowkey, ","));
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
			result = search(tableName, rowkey, tableInternal.getStartKey(), tableInternal.getStopKey());
			if(log.isDebugEnabled()){
				log.debug("[ocnosql]end to search,table=" + tableName + ",rowkey="+ StringUtils.join(rowkey, ","));
			}
		} catch (Exception e) {
			queue.add(e);
		}finally{
			runningThreadNum.countDown();
		}
		
	}
	
	
	public OCResultSet getResult() {
		return result;
	}

	
	protected OCResultSet search(String tablename, String[] rowkey, String startKey, String stopKey) throws Exception {
		List<String[]> res = new ArrayList<String[]>();

		Map<String, String> param = new HashMap<String, String>();
		param.put(CommonConstants.SEPARATOR, tableInternal.getSeperator());
        OCResultSet resultSet = new OCResultSet();
        
        List<Result> resultList = getRawResult(tablename, rowkey,  startKey, stopKey);
        
        if(resultList == null)
        	return resultSet;
        
        for(Result result : resultList) {
        	String rk = Bytes.toString(result.getRow());
        	for(KeyValue kv : result.raw()) {
    			List<String[]> tmpList = this.deCompressor.deCompress(kv, param);
    			tmpList = doFilter(tmpList, criterion, rk);
    			if(tmpList != null){
    				res.addAll(tmpList);
    			}
                resultSet.appendRows(tmpList);
        	}
        }
		return resultSet;
	}
}
