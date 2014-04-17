package com.ailk.oci.ocnosql.client.put;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.ailk.oci.ocnosql.client.ClientRuntimeException;
import com.ailk.oci.ocnosql.client.config.spi.CommonConstants;
import com.ailk.oci.ocnosql.client.config.spi.Connection;
import com.ailk.oci.ocnosql.client.importdata.PutLoad;
import com.ailk.oci.ocnosql.client.rowkeygenerator.RowKeyGenerator;
import com.ailk.oci.ocnosql.client.util.HTableUtilsV2;
import com.ailk.oci.ocnosql.common.util.ParseUtil;
import com.ailk.oci.ocnosql.client.rowkeygenerator.*;
import com.ailk.oci.ocnosql.client.config.spi.*;

public class HBasePut {

public static final Log log = LogFactory.getLog(HBasePut.class);
	
	private List<Put> putList = new ArrayList<Put>();
	private Map<String, List<String>> expTokensCache = new HashMap<String, List<String>>();
	private Map<String, Integer> mapping = new HashMap<String, Integer>();
	private String columns;
	private Connection conn;
	private HTableInterface table;
	private StringBuilder sb = new StringBuilder();
	private String curTime;
	String seperator;
	boolean skipBadLine; 
	boolean batchPut;
	int maxPutNum;
	String storageStrategy;
	byte[] family;
    String rowKeyColumn; //构成rowkey前缀列
    String rowKeyGenerator; // rowkey算法
    String algoColumn; //算法列
    String callback; //rowkey后缀

    private TableRowKeyGenerator tableRowKeyGenerator;
	
	public HBasePut(String tableName, Map<String, String> param){
		setup(tableName, param);
	}
	
	
	public void setup(String tableName, Map<String, String> param){
		seperator = MapUtils.getString(param, CommonConstants.SEPARATOR);
		skipBadLine = MapUtils.getBoolean(param, CommonConstants.SKIPBADLINE, false);
		batchPut = MapUtils.getBoolean(param, CommonConstants.BATCH_PUT, false);
		maxPutNum = MapUtils.getInteger(param, CommonConstants.HBASE_MAXPUTNUM, 1000);
		storageStrategy = MapUtils.getString(param, CommonConstants.STORAGE_STRATEGY, "mutipleimporttsv");
		columns = MapUtils.getString(param, CommonConstants.COLUMNS);
        rowKeyColumn = MapUtils.getString(param, CommonConstants.ROWKEYCOLUMN);
        rowKeyGenerator = MapUtils.getString(param, CommonConstants.ROWKEYGENERATOR);
        algoColumn = MapUtils.getString(param, CommonConstants.ALGOCOLUMN);
        callback = MapUtils.getString(param, CommonConstants.ROWKEYCALLBACK);

        String familyStr = MapUtils.getString(param, CommonConstants.SINGLE_FAMILY);
        if(StringUtils.isEmpty(familyStr)){
            int firstIndex = columns.indexOf(":");
            family = columns.substring(0,firstIndex).getBytes();
        }else{
            family = familyStr.getBytes();
        }

		String[] columnArr = columns.split(",");
		for(int i = 0; i < columnArr.length; i++){
			mapping.put(columnArr[i], i);
		}
		SimpleDateFormat formatter = new SimpleDateFormat("ddHHmm");
		curTime = formatter.format(new Date());

        conn = Connection.getInstance();
        Configuration conf = conn.getConf();
        System.out.println("tableName=" + tableName);
        table = HTableUtilsV2.getTable(conf, tableName);
		table.setAutoFlush(false);

        conf.set(CommonConstants.ROWKEYCOLUMN,rowKeyColumn);
        conf.set(CommonConstants.SEPARATOR,seperator);
        conf.get(CommonConstants.ROWKEYGENERATOR,rowKeyGenerator);
        conf.get(CommonConstants.ALGOCOLUMN,algoColumn);
        conf.get(CommonConstants.ROWKEYCALLBACK,callback);

		try{
			log.info("writting table configuratio...");
            TableConfiguration.getInstance().writeTableConfiguration(tableName, columns, seperator, conn.getConf());
		} catch(Exception e){
			log.error("write table configuration to HDFS exception.", e);
		}
        List<GenRKStep> genRKStepList = TableConfiguration.getInstance().getTableGenRKSteps(tableName,conf);
        tableRowKeyGenerator = new TableRowKeyGenerator(conf,genRKStepList);

	}
	
	
	/**
	 * 将数据put到HBase中
	 * @param line
	 * @return
	 * @throws ClientRuntimeException
	 */
	public boolean put(String line) throws ClientRuntimeException{
		String[] record = StringUtils.splitByWholeSeparatorPreserveAllTokens(line, seperator);
		Map<String, Integer> columnIndexMap = new HashMap<String, Integer>();
		try{
			String[] columnArr = StringUtils.splitByWholeSeparatorPreserveAllTokens(columns, ",");
			for(int i = 0;  i < columnArr.length; i++){
				columnIndexMap.put("${" + columnArr[i] + "}", i);
			}
			if(record.length != columnArr.length){
				throw new BadLineException("badLine exception. require columns size=" + columnArr.length + ", but found record size=" + record.length);
			}
            String rowkey = tableRowKeyGenerator.generateByGenRKStep(line,true);
			Put put = new Put(rowkey.getBytes());
			if("mutipleimporttsv".equals(storageStrategy)) {
				for(int i = 0;  i < columnArr.length; i++){
					String familyName = columnArr[i].split(":")[0];
					String columnName = columnArr[i].split(":")[1];
					put.add(familyName.getBytes(), columnName.getBytes(), Bytes.toBytes(record[i]));
				}
			}else if("singleimporttsv".equals(storageStrategy)) {
				for(int i = 0;  i < columnArr.length; i++){
					sb.append(record[i]);
					if(i != columnArr.length -1)
						sb.append(seperator);
				}
				put.add(family, curTime.getBytes(), sb.toString().getBytes());
			}
			if(batchPut){
				putList.add(put);
				if(putList.size() >= maxPutNum){
					table.put(putList);
					table.flushCommits();
					putList.clear();
				}
			}else{
				table.put(put);
			}
		}catch(BadLineException e) {
			if(skipBadLine){
				if(!batchPut){
					log.error("skip badLine: " + StringUtils.join(record, seperator));
				}else{
					log.error("skip batch badLines, batch size=" + putList.size());
				}
			}else{
				throw new ClientRuntimeException("BadLine exception. Nested message is: " + e.getMessage(), e);
			}
			
		}catch (IOException e) {
			throw new ClientRuntimeException("Put data exception. Nested message is: " + e.getMessage(), e);
		}finally{
			if(sb.length() > 0)
				sb.delete(0, sb.length()-1);
		}
		return true;
	}
	
	
	/**
	 * 提交数据
	 */
	public void commit(){
		try {
			if(putList != null && putList.size() > 0) {
				table.put(putList);
			}
			table.flushCommits();
			if(putList != null && putList.size() > 0) {
				putList.clear();
			}
		} catch (IOException e) {
			throw new ClientRuntimeException("commit put data exception. Nested message is: " + e.getMessage(), e);
		}
	}
	
	
	/**
	 * 关闭连接，同时提交数据
	 */
	public void close() {
		try{
			commit();
		}finally {
			if(table != null){
				try {
					table.close();
				} catch (IOException e) {
					log.error("close hbase table exception.", e);
				}
			}
		}
	}
	
	
	/**
	 * 生成rowkey
	 * @param rowkey
	 * @param rowKeyGenerator
	 * @param rowkeyExp
	 * @param record
	 * @param columnIndexMap
	 * @param rowkeyUnique
	 * @return
	 */
	public String generateRowkey(String rowkey, RowKeyGenerator rowKeyGenerator, String rowkeyExp, String[] record, Map<String, Integer> columnIndexMap, boolean rowkeyUnique){
		String newRowkey = null;
		
		if(StringUtils.isNotEmpty(rowkeyExp)) {
			List<String> tokens = expTokensCache.get(rowkeyExp);
			if(tokens == null){
				tokens = ParseUtil.parse(rowkeyExp);
				expTokensCache.put(rowkeyExp, tokens);
			}
			newRowkey = ParseUtil.executeExp(rowkeyExp, tokens, record, columnIndexMap);
		}else{
			newRowkey = rowkey;
		}
		if(rowKeyGenerator != null) {
			newRowkey = (String) rowKeyGenerator.generatePrefix(rowkey) + newRowkey;
		}
		if(rowkeyUnique){
			newRowkey = newRowkey + System.nanoTime();
		}
		return newRowkey;
	}
	
	
	public String getValue(String[] record, String name){
		return record[mapping.get(name)];
	}
	
	
	public static class BadLineException extends Exception {
	      public BadLineException(String err) {
	        super(err);
	      }
	      private static final long serialVersionUID = 1L;
	}
	
}
