package com.ailk.oci.ocnosql.client.spi;

import java.util.List;
import java.util.Map;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import com.ailk.oci.ocnosql.client.ClientRuntimeException;
import com.ailk.oci.ocnosql.client.cache.OciTableRef;
import com.ailk.oci.ocnosql.client.config.spi.CommonConstants;
import com.ailk.oci.ocnosql.client.config.spi.Connection;
import com.ailk.oci.ocnosql.client.export.OcnosqlExport;
import com.ailk.oci.ocnosql.client.importdata.PutLoad;
import com.ailk.oci.ocnosql.client.load.mutiple.MutipleColumnImportTsv;
import com.ailk.oci.ocnosql.client.load.single.SingleColumnImportTsv;
import com.ailk.oci.ocnosql.client.query.ColumnFamily;
import com.ailk.oci.ocnosql.client.query.Query;
import com.ailk.oci.ocnosql.client.query.QueryMultiColumn;
import com.ailk.oci.ocnosql.client.query.criterion.Criterion;

public class ClientMultiColumn implements Client {
	private final static Log log = LogFactory.getLog(ClientMultiColumn.class.getSimpleName());
	private Connection connection;
	private String msg;
	private Map<String, Object> resultMap;
	private PutLoad putLoad;
	
	public ClientMultiColumn(){}
	
	public ClientMultiColumn(Connection connection){
		this.connection = connection;
	}
	
	@Override
	/**
	 * 参数说明：分隔符、表名称、导出路径
	 */
	public boolean exportData(String tableName, Map<String, String> param) throws ClientRuntimeException {
		log.info("invok exportData,param=[" + param + "]");
		checkConnection();
		if(StringUtils.isEmpty(tableName)){
			msg = "tableName must be not null";
			log.error(msg);
			throw new ClientRuntimeException(msg);
		}
		if(MapUtils.isEmpty(param)){
			msg = "param must be not null";
			log.error(msg);
			throw new ClientRuntimeException(msg);
		}
		
		OciTableRef ociTableRef = new OciTableRef();
		
        ociTableRef.setSeperator(MapUtils.getString(param, CommonConstants.SEPARATOR));
        ociTableRef.setExportPath(MapUtils.getString(param, CommonConstants.EXPOTR_OUTPUT));
        ociTableRef.setCompressor(MapUtils.getString(param, CommonConstants.COMPRESSOR, CommonConstants.DEFAULT_COMPRESSOR));
        ociTableRef.setRowkeyGenerator(MapUtils.getString(param, CommonConstants.ROWKEY_GENERATOR,
                com.ailk.oci.ocnosql.client.rowkeygenerator.RowKeyGeneratorHolder.TYPE.md5.name()));
		
		OcnosqlExport ocnosqlExport = new OcnosqlExport();
		return ocnosqlExport.execute(connection, ociTableRef);
	}

	@Override
	/**
	 * 	 * @param tableName 表名
	 * @param param的key的常量定义:
	 *        CommonConstants.COLUMNS            --列名
	 *        CommonConstants.SEPARATOR          --分隔符
	 *        CommonConstants.SKIPBADLINE        --是否跳过错行  默认true
	 *        CommonConstants.INPUT              --输入路径
	 *        CommonConstants.IMPORT_TMP_OUTPUT  --临时输出路径
	 *        CommonConstants.COMPRESSOR         --压缩方式   默认CommonConstants.DEFAULT_COMPRESSOR。
	 *        CommonConstants.ALGOCOLUMN         --算法列 (key前缀)
	 *        CommonConstants.ROWKEY_GENERATOR   --对算法列要做的算法
	 *        CommonConstants.ROWKEYCOLUMN       --构成key的列 (跟在key前缀后)
	 *        CommonConstants.ROWKEY_UNIQUE      --是否要求rowkey唯一, 默认true
	 *        CommonConstants.ROWKEYCALLBACK     --key后缀处理，如果ROWKEY_UNIQUE为true，需要加此参数。
	 *                                             如果没加默认为GenRKCallBackDefaultImpl.
	 */
	public boolean importData(String tableName, Map<String, String> param) throws ClientRuntimeException {
		log.info("invok importData,param=[" + param + "]");
		checkConnection();
		if(StringUtils.isEmpty(tableName)){
			msg = "tableName must be not null";
			log.error(msg);
			throw new ClientRuntimeException(msg);
		}
		if(MapUtils.isEmpty(param)){
			msg = "param must be not null";
			log.error(msg);
			throw new ClientRuntimeException(msg);
		}
		
		OciTableRef ociTableRef = new OciTableRef();
        ociTableRef.setName(tableName);
        ociTableRef.setColumns(MapUtils.getString(param, CommonConstants.COLUMNS));//字段串
        ociTableRef.setInputPath(MapUtils.getString(param, CommonConstants.INPUT));//输入路径
        ociTableRef.setImportTmpOutputPath(MapUtils.getString(param, CommonConstants.IMPORT_TMP_OUTPUT));//临时输出路径
        ociTableRef.setSkipBadLine(MapUtils.getString(param, CommonConstants.SKIPBADLINE, "true"));//是否滤掉过错误行
        ociTableRef.setSeperator(MapUtils.getString(param, CommonConstants.SEPARATOR));//字段分隔符
        ociTableRef.setCompressor(MapUtils.getString(param, CommonConstants.COMPRESSOR,
                                                         CommonConstants.DEFAULT_COMPRESSOR));//压缩解压类名称
        ociTableRef.setRowkeyGenerator(MapUtils.getString(param, CommonConstants.ROWKEYGENERATOR));
        ociTableRef.setRowkeyColumn(MapUtils.getString(param, CommonConstants.ROWKEYCOLUMN));
        String rowkeyUnique = MapUtils.getString(param,CommonConstants.ROWKEY_UNIQUE,"true");
        ociTableRef.setAlgoColumn(MapUtils.getString(param, CommonConstants.ALGOCOLUMN));
        ociTableRef.setRowKeyUnique(rowkeyUnique);
        if(rowkeyUnique.equalsIgnoreCase("true")){
           ociTableRef.setCallback(MapUtils.getString(param,
                   CommonConstants.ROWKEYCALLBACK,CommonConstants.DEFAULT_ROWKEYCALLBACK));
        }
        MutipleColumnImportTsv mutipleImportor = new MutipleColumnImportTsv();
        boolean loadResult = mutipleImportor.execute(connection, ociTableRef);
        resultMap = mutipleImportor.getReturnMap();
		return loadResult;
	}
	
	
	/**
	 * put数据
	 * @param tableName
	 * @param record
	 * @param param
	 * @return
	 * @throws ClientRuntimeException
	 */
	public boolean putData(String tableName, String[] record, Map<String, String> param) throws ClientRuntimeException{
		if(putLoad == null){
			putLoad = new PutLoad();
		}
        param.put(CommonConstants.STORAGE_STRATEGY, "mutipleimporttsv");
		return putLoad.putData(tableName, record, param);
	}
	

	
	@Override
	public List<String[]> queryByRowkey(Connection conn, String rowkey,
			List<String> tableNames, Criterion criterion,
			Map<String, String> param) throws ClientRuntimeException {
		return queryByRowkey(conn, new String[]{rowkey}, tableNames, criterion, param, null);
	}

	@Override
	public List<String[]> queryByRowkey(Connection conn, String rowkey,
			List<String> tableNames, Criterion criterion,
			Map<String, String> param, List<ColumnFamily> columnFamilies)
			throws ClientRuntimeException {
		return queryByRowkey(conn, new String[]{rowkey}, tableNames, criterion, param, columnFamilies);
	}
	
	
	@Override
	public List<String[]> queryByRowkey(Connection conn,String[] rowkey, List<String> tableNames, Criterion criterion, Map<String, String> param)
			throws ClientRuntimeException {
		return queryByRowkey(conn, rowkey, tableNames, criterion, param, null);
	}
	

	@Override
	public List<String[]> queryByRowkey(Connection conn, String[] rowkey,
			List<String> tableNames, Criterion criterion,
			Map<String, String> param, List<ColumnFamily> columnFamilies)
			throws ClientRuntimeException {
		checkConnection(conn);
		Configuration conf = conn.getConf();
		Query query = new QueryMultiColumn(); 
		if(param != null && param.size() > 0){
			for(String key : param.keySet()){
				conf.set(key, param.get(key));
			}
		}
		List<String[]> result = query.query(conn, rowkey, tableNames, criterion, columnFamilies);
		if(null == result){
			msg = "query failed";
			throw new ClientRuntimeException(msg);
		}
		return result;
	}
	

	@Override
	public List<String[]> queryByRowkey(Connection conn, String startKey,
			String stopKey, List<String> tableNames, Criterion criterion,
			Map<String, String> param) throws ClientRuntimeException {
		return queryByRowkey(conn, startKey, stopKey, tableNames, criterion, param, null);
	}
	

	@Override
	public List<String[]> queryByRowkey(Connection conn, String startKey,
			String stopKey, List<String> tableNames, Criterion criterion,
			Map<String, String> param, List<ColumnFamily> columnFamilies)
			throws ClientRuntimeException {
		checkConnection(conn);
		Configuration conf = conn.getConf();
		Query query = new QueryMultiColumn(); 
		if(param != null && param.size() > 0){
			for(String key : param.keySet()){
				conf.set(key, param.get(key));
			}
		}
		List<String[]> result = query.query(conn, startKey, stopKey, tableNames, criterion, columnFamilies);
		
		if(null == result){
			msg = "query failed";
			throw new ClientRuntimeException(msg);
		}
		return result;
	}
	
	

	@Override
	public List<String[]> queryBySql(String sql, Map<String, String> param)
			throws ClientRuntimeException {
		checkConnection();
		Configuration conf = connection.getConf();
		Query query = new QueryMultiColumn(); 
		if(param != null && param.size() > 0){
			for(String key : param.keySet()){
				conf.set(key, param.get(key));
			}
		}
		return query.executeSql(connection, sql);
	}
	
	private void checkConnection() throws ClientConnectionException {
		if(connection == null){
			msg = "Not connect ocnosql, cause by not set conection.Please invok setConnection()" ;
			log.error(msg);
			throw new ClientConnectionException(msg);
		}
	}
	
	private void checkConnection(Connection connection) throws ClientConnectionException {
		if(connection == null){
			msg = "Not connect ocnosql, cause by not set conection.Please invok setConnection()" ;
			log.error(msg);
			throw new ClientConnectionException(msg);
		}
	}

	public Connection getConnection() {
		return connection;
	}

	public void setConnection(Connection connection) {
		this.connection = connection;
	}
	public Map<String, Object> getResultMap() {
		return resultMap;
	}

	

}
