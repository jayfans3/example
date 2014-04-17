package com.ailk.oci.ocnosql.client.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

import com.ailk.oci.ocnosql.client.ClientRuntimeException;
import com.ailk.oci.ocnosql.client.cache.OciTableRef;
import com.ailk.oci.ocnosql.client.cache.TableMetaCache;
import com.ailk.oci.ocnosql.client.query.schema.OCTable;
import com.ailk.oci.ocnosql.client.util.HTableUtils;
import com.ailk.oci.ocnosql.common.util.PropertiesUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.*;

import com.ailk.oci.ocnosql.client.compress.Compress;
import com.ailk.oci.ocnosql.client.config.spi.CommonConstants;
import com.ailk.oci.ocnosql.client.query.criterion.Criterion;
import com.ailk.oci.ocnosql.client.query.criterion.Expression;
import com.ailk.oci.ocnosql.client.rowkeygenerator.RowKeyGenerator;
import com.ailk.oci.ocnosql.client.spi.ClientConnectionException;

/**
 * 功能：多线程查询
 * <br>
 * 非线程安全
 * 
 * @author zhuangyang
 */
public class QueryActor implements Runnable {
	private static Log log = LogFactory.getLog(QueryActor.class);
	
	protected Configuration conf;
	protected OciTableRef tableInternal;
	protected Criterion criterion;
	protected Compress deCompressor;
	protected Map<String, Integer> modelMap;
	protected int rowkeyIndex = -1; // rowkey索引
	protected CountDownLatch runningThreadNum;
	protected OCResultSet result = new OCResultSet();
	protected LinkedBlockingQueue<Exception> queue;
    public QueryActor(Configuration conf, OciTableRef tableInternal, Criterion criterion, CountDownLatch runningThreadNum) {
		this.conf = conf;
		this.tableInternal = tableInternal;
		this.criterion = criterion;
		this.runningThreadNum = runningThreadNum;
		
		try {
			String deCompressClassName = tableInternal.getCompressor();
			Class<?> deCompressorClass = Class.forName(deCompressClassName);
			deCompressor = (Compress)deCompressorClass.newInstance();
		}
        catch (Exception e) {
			log.error("create decompressor occur error,cause by " + e.getMessage());
		}
	}

	public void setQueue(LinkedBlockingQueue<Exception> queue) {
		this.queue = queue;
	}

	public void run() {
		
	}

	// 构造字段和索引位置信息
	protected Map<String, Integer> getModelMap(String colString) {
		Map<String, Integer> map = new HashMap<String, Integer>();
		if (StringUtils.isEmpty(colString)) {
			log.error("column string is null");
			return null;
		}
		String[] cols = StringUtils.splitByWholeSeparatorPreserveAllTokens(colString, ",");
		for (int m = 0; m < cols.length; m++) {
			if (StringUtils.equals(cols[m], CommonConstants.ROW_KEY)) {
				rowkeyIndex = m;
				break;
			}
		}
		if (colString.contains(CommonConstants.ROW_KEY + ",")) {
			colString = StringUtils.remove(colString, CommonConstants.ROW_KEY + ",");
		} 
		else if (colString.contains("," + CommonConstants.ROW_KEY)) {
			colString = StringUtils.remove(colString, "," + CommonConstants.ROW_KEY);
		}
		String[] colunmName = StringUtils.splitPreserveAllTokens(colString, ",");
		for (int k = 0; k < colunmName.length; k++) {
			String[] col = StringUtils.splitPreserveAllTokens(colunmName[k], ":");
			map.put(col.length==1?col[0]:col[1], k);
		}
		return map;
	}

	public OCResultSet getResult() {
		return result;
	}
	


	protected List<Result>  getRawResult(String tablename, String rowkey[], String startKey, String stopKey) throws Exception{
        HTableInterface table = null;
        List<Result> results = new ArrayList<Result>();
		try {
			table = HTableUtils.getTable(conf, tablename);
		} 
		catch (Exception e1) {
                      
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
		if (table == null) {
			if(log.isErrorEnabled()){
				log.error("[ocnosql]can't connect table " + tablename + " or table " + tablename + " is not exist.");
			}
			return null;
		}
		
		RowKeyGenerator generator = com.ailk.oci.ocnosql.client.rowkeygenerator.RowKeyGeneratorHolder.resolveGenerator(tableInternal.getRowkeyGenerator());
		if(generator != null){
			for(int i = 0; i < rowkey.length; i++){
                rowkey[i] = (String) generator.generate(rowkey[i]);
        	}
		}
		
		if (rowkey == null || rowkey.length == 0){	//区间查询
            results = queryByPrefix(tablename, table, startKey, stopKey);
        } else {
            results = queryByGet(tablename, table, rowkey);
        }
		

//		Get get = new Get(rowkey.getBytes());
//		get.setCacheBlocks(false);
//		get.setMaxVersions();
//		Result result = null;
//		try {
//			
//			result = table.get(get);
//		} catch (RetriesExhaustedException e) {
//			log.error("[ocnosql]connect region server occur error,nested exception:" + e.getMessage() + ".");
////			if(table!=null) table.clearRegionCache();
//			throw new ClientConnectionException("connect region server occur error, caused by " + e.getMessage());
//		} catch (NoServerForRegionException e) {
//			log.error("searching region occur error, cased by no server for region.");
////			if(table!=null) table.clearRegionCache();
//			throw new ClientConnectionException("searching region occur error, caused by " + e.getMessage());
//		} catch (IOException e) {
//			throw new IOException("search table " + tablename + " occur error, caused by " + e.getMessage());
//		} 
//		if(result == null || result.size() == 0){
//			return null;
//		}
//		Map<String, String> param = new HashMap<String, String>();
//		param.put(CommonConstants.SEPARATOR, tableInternal.getSeperator());
//		list.add(result);
		return results;
		
	}
	
	
	private List<Result> queryByPrefix(String tablename, HTableInterface table, String startKey, String stopKey) throws IOException {
		List<Result>  list = new ArrayList<Result>();
        ResultScanner resultScanner = null;
        try {
        	log.info("tablename=" + tablename + ", startKey=" + startKey + ", stopKey=" + stopKey);
        	byte[] stopKeyBytes = stopKey.getBytes();
        	stopKeyBytes[stopKeyBytes.length - 1] ++;
            Scan scan = new Scan(startKey.getBytes(), stopKeyBytes);
            scan.setCaching(conf.getInt(CommonConstants.QUERY_CACHE_SIZE, 1));
        	
            resultScanner = table.getScanner(scan);
            Iterator<Result> iteratorResult = resultScanner.iterator();
            long maxSize = Long.valueOf(PropertiesUtil.getProperty(CommonConstants.FILE_NAME,
                CommonConstants.QUERY_RESULT_MAX_SIZE, CommonConstants.DEFAULT_QUERY_RESULT_MAX_SIZE.toString()) );
            long resultSize = 0;
            while (iteratorResult.hasNext()){
                list.add(iteratorResult.next());
                resultSize++;
                if(resultSize>=maxSize) {
                    log.error("Too many records returned: >= " + maxSize + ". Auto cutted! Please check the condition");
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
	
	
	private List<Result> queryByGet(String tablename, HTableInterface table, String[] rowkey){
		List<Get>  getList = new ArrayList<Get>();
    	for(int i = 0; i<rowkey.length;i++){
    		 Get get = new Get(rowkey[i].getBytes());
    	     get.setCacheBlocks(true);
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
	
	
	

	protected List<String[]> doFilter(List<String[]> tmpList, Criterion criterion, String rowkey) throws Exception {
		
//		if(null==criterion){
//			return tmpList;
//		}

		List<String[]> dest = new ArrayList<String[]>();
		boolean match = true;
		
		String[] detail = null;
		try {
			for (int j = 0; j < tmpList.size(); j++) {
				// 获取一条数据
				detail = tmpList.get(j);
				if(criterion != null)
					match = visitQueryParam(detail,criterion, null);
				
				if (match) {
					// 将rowkey添加到返回结果中
					if (rowkeyIndex != -1) {
						String[] newDetail = new String[detail.length + 1];
						System.arraycopy(detail, 0, newDetail, 0, rowkeyIndex);
						newDetail[rowkeyIndex] = rowkey;
						System.arraycopy(detail, rowkeyIndex, newDetail,rowkeyIndex + 1, detail.length - rowkeyIndex);
						dest.add(newDetail);
					} else {
						dest.add(detail);
					}
				}
			}
		} catch (Exception e) {
			dest.clear();
			if(e instanceof ArrayIndexOutOfBoundsException){
				StringBuilder err = new StringBuilder();
				err.append("[ocnosql]do filter occur error ,detail record [");
				if(detail == null){
					err.append(" null ]");
					log.error(err, e);
				}else {
					for(String str : detail){
						err.append(str + ",");
					}
					if(err.charAt(err.length() - 1) == ',')err.deleteCharAt(err.length() - 1);
					log.error(err + "]", e);
				}
			}else {
				throw e;
			}
			
			return dest;
		}
		return dest;
	}
	

	@SuppressWarnings("rawtypes")
	protected boolean visitQueryParam(String[] detail, Criterion query, OCTable octable)  throws Exception {
		Boolean match = null;
		String oprType = query.getOprType();
		if(query.hasNestedQueryParams()){
			for(Criterion nestedQuery : query.getQueryParams()){
				boolean nestedMatch = visitQueryParam(detail, nestedQuery, octable);
				if(match == null){
					match = nestedMatch;
				}
				if(oprType.equals("and")){
					match = match && nestedMatch;
					if(!nestedMatch){
						break;
					}
				}else{
					match = match || nestedMatch;
					if(nestedMatch){
						break;
					}
				}
			}
			if(oprType.equals("or") && match){
				return true;
			}else if(oprType.equals("and") && !match){
				return false;
			}
		}
		Boolean matchSimple = null;
		Set<String> key = query.getOpr().keySet();
		for (Iterator it = key.iterator(); it.hasNext();) {//1：遍历所有涉及到的字段
			String s = (String) it.next();
			
			List opr = query.getOpr().get(s);
			for (Object o : opr) {//2：遍历涉及到的该字段的所有比对，比如a=1 or a=2
				Expression ep = (Expression) o;
				//System.out.println(s + " = " + OCTable.getColumnByIndex(Integer.parseInt(s)).getPosition());
				//matchSimple = ep.accept((detail[OCTable.getColumnByIndex(Integer.parseInt(s)).getPosition()]));
				//matchSimple = ep.accept((detail[Integer.parseInt(s)]));
				if (octable != null) {
					if(octable.getColumnByName(s) == null)
						throw new ClientRuntimeException("column field: '" + s + "' is not exsit.");
					matchSimple = ep.accept((detail[octable.getColumnByName(s).getPosition()]));
				} else
					matchSimple = ep.accept((detail[modelMap.get(s)]));
				if (query.getOprType().equals("and")) {
					if (!matchSimple)
						break;
				} else {
					if (matchSimple)
						break;
				}
			}
			if (query.getOprType().equals("and")) {
				if (!matchSimple)
					break;
			} else {
				if (matchSimple)
					break;
			}
		}
		if(key.size() == 0 && !query.hasNestedQueryParams()){
			return true;
		}
		if(!query.hasNestedQueryParams()){
			match = matchSimple;
		}else{
			if(matchSimple != null){
				if(oprType.equals("and")){
					match =  match && matchSimple;
				}else{
					match =  match || matchSimple;
				}
			}
		}
		return match;
	}
	
}
