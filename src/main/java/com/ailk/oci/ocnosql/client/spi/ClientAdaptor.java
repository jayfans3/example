package com.ailk.oci.ocnosql.client.spi;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.util.ReflectionUtils;

import com.ailk.oci.ocnosql.client.ClientRuntimeException;
import com.ailk.oci.ocnosql.client.config.spi.CommonConstants;
import com.ailk.oci.ocnosql.client.config.spi.Connection;
import com.ailk.oci.ocnosql.client.query.ColumnFamily;
import com.ailk.oci.ocnosql.client.query.criterion.Criterion;
import com.ailk.oci.ocnosql.common.util.PropertiesUtil;

/**
 * ocnosql clientAdapter
 * OCNOSQL客户端接口封装
 *
 */
public class ClientAdaptor {
	
	private Client client;//客户端接口
	String[] s=new String[1];
	
	public ClientAdaptor(){
		try {
			getClient();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 通过反射获得客户端接口类
	 * @return void
	 */
	private void getClient() throws ClassNotFoundException{
		Class<?> clientClass = Class.forName(
				PropertiesUtil.getProperty(
						CommonConstants.FILE_NAME,  //配置文件名称
						CommonConstants.CLIENT_TYPE, //查询客户端类：单列或者多列
						"com.ailk.oci.ocnosql.client.spi.ClientSingleColumn" //默认为单列类
						));
		client = (Client)ReflectionUtils.newInstance(clientClass, null);
	}
	
	
	public List<String[]> queryByRowkey(Connection conn,String rowkey, List<String> tableNames, Criterion criterion, Map<String, String> param) throws ClientRuntimeException{
		return queryByRowkey(conn, new String[]{rowkey}, tableNames, criterion, param, null);
	}
	
	
	public List<String[]> queryByRowkey(Connection conn,String rowkey, List<String> tableNames, Criterion criterion, Map<String, String> param, List<ColumnFamily> columnFamilies) throws ClientRuntimeException{
		return queryByRowkey(conn, new String[]{rowkey}, tableNames, criterion, param, columnFamilies);
	}
	
	
	public List<String[]> queryByRowkey(
			Connection conn, 
			String[] rowkey, 
			List<String> tableNames, 
			Criterion criterion, 
			Map<String, String> param
			) throws ClientRuntimeException{
		
		//直接调用Client接口
		return queryByRowkey(conn,rowkey, tableNames, criterion, param, null);
		    
	}
	
	
	/**
	 * 通过Rowkey查询多个表，可以设置过滤条件
	 * @param conn 连接
	 * @param rowkey rowkey值
	 * @param tableNames 需要查询的表的名称列表
	 * @param criterion 过滤条件列表
	 * @param param 输入参数
	 * @return 查询结果值
	 * @throws ClientRuntimeException
	 */
	public List<String[]> queryByRowkey(
			Connection conn, 
			String[] rowkey, 
			List<String> tableNames, 
			Criterion criterion, 
			Map<String, String> param,
			List<ColumnFamily> columnFamilies) throws ClientRuntimeException{
		
		//直接调用Client接口
		return client.queryByRowkey(conn, rowkey, tableNames, criterion, param, columnFamilies);
		    
	}
	
	
	
	
	/**
	 * 通过rowkey的前缀查询多个表，可以设置过滤条件
	 * @param conn 连接
	 * @param rowkeyPrefix rowkeyPrefix值
	 * @param tableNames 需要查询的表的名称列表
	 * @param criterion 过滤条件列表
	 * @param param 输入参数
	 * @return 查询结果值
	 * @throws ClientRuntimeException
	 */
	public List<String[]> queryByRowkeyPrefix(
			Connection conn,
			String rowkeyPrefix, 
			List<String> tableNames, 
			Criterion criterion, 
			Map<String, String> param
			) throws ClientRuntimeException{
		
		return queryByRowkey(conn, rowkeyPrefix, rowkeyPrefix, tableNames, criterion, param, null);
	}
	
	
	public List<String[]> queryByRowkeyPrefix(
			Connection conn,
			String rowkeyPrefix, 
			List<String> tableNames, 
			Criterion criterion, 
			Map<String, String> param,
			List<ColumnFamily> columnFamilies) throws ClientRuntimeException{
		
		return queryByRowkey(conn, rowkeyPrefix, rowkeyPrefix, tableNames, criterion, param, columnFamilies);
	}
	
	
	public List<String[]> queryByRowkey(
			Connection conn,
			String startKey,
			String stopKey,
			List<String> tableNames, 
			Criterion criterion, 
			Map<String, String> param
			) throws ClientRuntimeException{
		
		return queryByRowkey(conn, startKey, stopKey, tableNames, criterion, param, null);
	}	
	
	
	public List<String[]> queryByRowkey(
			Connection conn,
			String startKey,
			String stopKey,
			List<String> tableNames, 
			Criterion criterion, 
			Map<String, String> param,
			List<ColumnFamily> columnFamilies) throws ClientRuntimeException{
		
		return client.queryByRowkey(conn, startKey, stopKey, tableNames, criterion, param, columnFamilies);
	}	
		
		
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
			) throws ClientRuntimeException{
		
		//直接调用Client接口
		return client.queryBySql(sql, param);
		
	}
	
	/**
	 * 数据导入
	 * @param tableId 表ID
	 * @param param 输入参数
	 * @return
	 * @throws ClientRuntimeException
	 */
	public boolean importData(String tableId, Map<String, String> param) throws ClientRuntimeException{
		return client.importData(tableId, param);
	}
	
	/**
	 * 数据整表导出
	 * @param tableId 表ID
	 * @param param 输入参数
	 * @return
	 * @throws ClientRuntimeException
	 */
	public boolean exportData(String tableId, Map<String, String> param) throws ClientRuntimeException{
		return client.exportData(tableId, param);
		
	}
	
	 
	public boolean putData(String tableName, String[] record, Map<String, String> param) throws ClientRuntimeException{
		return client.putData(tableName, record, param);
	}
}
