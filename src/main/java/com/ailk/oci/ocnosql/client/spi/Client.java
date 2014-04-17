package com.ailk.oci.ocnosql.client.spi;

import java.util.List;
import java.util.Map;

import com.ailk.oci.ocnosql.client.ClientRuntimeException;
import com.ailk.oci.ocnosql.client.config.spi.Connection;
import com.ailk.oci.ocnosql.client.query.ColumnFamily;
import com.ailk.oci.ocnosql.client.query.criterion.Criterion;

/**
 * OCNOSQL客户端接口，目前有两个实现类：
 * ClientSingleColumn 单列查询
 * ClientMultiColumn 多列查询
 */
public interface Client {
	
	public List<String[]> queryByRowkey(
			Connection conn, 
			String rowkey, 
			List<String> tableNames, 
			Criterion criterion, 
			Map<String, String> param
			) throws ClientRuntimeException;
	
	
	public List<String[]> queryByRowkey(
			Connection conn, 
			String rowkey, 
			List<String> tableNames, 
			Criterion criterion, 
			Map<String, String> param,
			List<ColumnFamily> columnFamilies) throws ClientRuntimeException;
	
	/**
	 * 通过Rowkey查询
	 * @param rowkey rowkey值
	 * @param tableNames 需要查询的表的名称列表
	 * @param criterion 过滤条件列表
	 * @param param 输入参数
	 * @return
	 * @throws ClientRuntimeException
	 */
	public List<String[]> queryByRowkey(
			Connection conn, 
			String rowkey[], 
			List<String> tableNames, 
			Criterion criterion, 
			Map<String, String> param
			) throws ClientRuntimeException;
	
	
	public List<String[]> queryByRowkey(
			Connection conn, 
			String rowkey[], 
			List<String> tableNames, 
			Criterion criterion, 
			Map<String, String> param,
			List<ColumnFamily> columnFamilies) throws ClientRuntimeException;
	
	
	public List<String[]> queryByRowkey(
			Connection conn, 
			String startKey, 
			String stopKey,
			List<String> tableNames, 
			Criterion criterion, 
			Map<String, String> param
			) throws ClientRuntimeException;
	
	
	public List<String[]> queryByRowkey(
			Connection conn, 
			String startKey, 
			String stopKey,
			List<String> tableNames, 
			Criterion criterion, 
			Map<String, String> param,
			List<ColumnFamily> columnFamilies) throws ClientRuntimeException;
	
	
	/**
	 * 通过SQL查询
	 * @param sql sql 需要执行的sql
	 * @param param 输入参数
	 * @return
	 * @throws ClientRuntimeException
	 */
	public List<String[]> queryBySql(
			String sql, 
			Map<String, String> param
			) throws ClientRuntimeException;
	
	/**
	 * 	 * 数据导入
	 * @param tableId 表ID
	 * @param param 输入参数
	 * @return
	 * @throws ClientRuntimeException
	 */
	public boolean importData(String tableId, Map<String, String> param) throws ClientRuntimeException;
	
	/**
	 * * 数据整表导出
	 * @param tableId 表ID
	 * @param param 输入参数
	 * @return
	 * @throws ClientRuntimeException
	 */
	public boolean exportData(String tableId, Map<String, String> param) throws ClientRuntimeException;


	/**
	 * * 结果
	 * @return
	 */
	public Map<String, Object> getResultMap();
	
	
	/**
	 * put导入数据，param包括以下参数：
	 * 	CommonConstants.COLUMNS：列明，如："f:a,f:b,HBASE_ROW_KEY,f:r"
	 * 	CommonConstants.SEPARATOR：字段分隔符
	 * 	CommonConstants.BATCH_PUT：是否批量提交
	 * 	CommonConstants.HBASE_MAXPUTNUM：批量提交数量
	 * 	CommonConstants.ROWKEY_GENERATOR：hash算法
	 * 	CommonConstants.ROW_KEY_EXPRESSION：rowkey表达式，如${f:b}_${f:r}_${HBASE_ROW_KEY}__${f:a}_
	 * 	CommonConstants.ROWKEY_UNIQUE：rowkey末尾是否加入唯一标示
	 * 
	 * @param tableName
	 * @param record	处理之后的单条记录
	 * @param param
	 * @return
	 * @throws ClientRuntimeException
	 */
	public boolean putData(String tableName, String[] record, Map<String, String> param) throws ClientRuntimeException;
}
