package com.ailk.oci.ocnosql.client.thrift.serviceImpl;

import com.ailk.oci.ocnosql.client.jdbc.*;
import com.ailk.oci.ocnosql.client.thrift.exception.*;
import com.ailk.oci.ocnosql.client.thrift.exception.SQLException;
import com.ailk.oci.ocnosql.client.thrift.service.*;
import com.google.common.collect.*;
import org.apache.thrift.*;

import java.sql.*;
import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: lile3
 * Date: 13-11-19
 * Time: 下午2:51
 * To change this template use File | Settings | File Templates.
 */
public class SQLServiceImpl implements SQLService.Iface{

    HbaseJdbcHelper jdbcHelper;

    public SQLServiceImpl(){}

    public SQLServiceImpl(HbaseJdbcHelper jdbcHelper){
        this.jdbcHelper = jdbcHelper;
    }


    @Override
    public int excuteNonQueryFir(String sql) throws SQLException, TException {
        try{
            System.out.println("received sql = " + sql);
            return jdbcHelper.excuteNonQuery(sql);
        } catch (java.sql.SQLException e){
            System.out.println("sql Exception = " + e.getMessage());
            throw new SQLException().setErrormessage(e.getMessage());
        }
    }

    @Override
    public int excuteNonQuerySec(String sql, List<String> param) throws SQLException, TException {
        try{
            return jdbcHelper.excuteNonQuery(sql,param.toArray());
        } catch (java.sql.SQLException e){
            throw new SQLException().setErrormessage(e.getMessage());
        }
    }

    @Override
    public void excuteNonQueryThr(List<String> sql, int batchSize) throws SQLException, TException {
        try{
             String[] sqlArr = null;
             if(sql != null){
                sqlArr = transferObjectArrToStringArr(sql.toArray());
             }
            jdbcHelper.excuteNonQuery(sqlArr,batchSize);
        } catch (java.sql.SQLException e){
            throw new SQLException().setErrormessage(e.getMessage());
        }
    }

    @Override
    public void excuteNonQueryFou(List<String> sql, List<List<String>> params, int batchSize) throws SQLException, TException {
        try{
             List<Object[]> paramList = new ArrayList<Object[]>();
             if(params != null){
                for(List<String> param : params){
                    paramList.add(param.toArray());
                }
             }
             String[] sqlArr = null;
             if(sql != null){
                sqlArr = transferObjectArrToStringArr(sql.toArray());
             }
            jdbcHelper.excuteNonQuery(sqlArr,paramList,batchSize);
        } catch (java.sql.SQLException e){
            throw new SQLException().setErrormessage(e.getMessage());
        }
    }

    @Override
    public List<Map<String, String>> executeQueryRawFir(String sql) throws SQLException, TException {
         try{
            ResultSet rsResultSet = jdbcHelper.executeQueryRaw(sql);
            List<Map<String, String>> list = new ArrayList<Map<String, String>>();
            int columnCount = rsResultSet.getMetaData().getColumnCount();
            while (rsResultSet.next()) {
                Map<String, String> map = new HashMap<String, String>();
                for (int i = 1; i <= columnCount; i++) {
                    map.put(rsResultSet.getMetaData().getColumnName(i),
                            rsResultSet.getString(i));
                }
                list.add(map);
            }
            return list;
        } catch (java.sql.SQLException e){
            throw new SQLException().setErrormessage(e.getMessage());
        }
    }

    @Override
    public List<Map<String, String>> executeQueryRawSec(String sql, List<String> param) throws SQLException, TException {
         try{
            ResultSet rsResultSet = jdbcHelper.executeQueryRaw(sql,param.toArray());
            List<Map<String, String>> list = new ArrayList<Map<String, String>>();
            int columnCount = rsResultSet.getMetaData().getColumnCount();
            while (rsResultSet.next()) {
                Map<String, String> map = new HashMap<String, String>();
                for (int i = 1; i <= columnCount; i++) {
                    map.put(rsResultSet.getMetaData().getColumnName(i),
                            rsResultSet.getString(i));
                }
                list.add(map);
            }
            return list;
        } catch (java.sql.SQLException e){
            throw new SQLException().setErrormessage(e.getMessage());
        }
    }

    @Override
    public List<Map<String, String>> executeQueryFir(String sql) throws SQLException, TException {
         try{
            List<Map<String, Object>> list = jdbcHelper.executeQuery(sql);

            List<Map<String,String>> resList = new ArrayList<Map<String, String>>();
            if(list != null){
                for (int i = 0; i < list.size(); i++) {
                    resList.add(transferObjectMapToStringMap(list.get(i)));
                }
            }
            list.clear();
            list = null;
            return resList;
        } catch (java.sql.SQLException e){
            throw new SQLException().setErrormessage(e.getMessage());
        }
    }

    @Override
    public List<Map<String, String>> executeQuerySec(String sql, List<String> param) throws SQLException, TException {
         try{
            List<Map<String, Object>> list = jdbcHelper.executeQuery(sql,param.toArray());
            List<Map<String,String>> resList = new ArrayList<Map<String, String>>();
            if(list != null){
                for (int i = 0; i < list.size(); i++) {
                    resList.add(transferObjectMapToStringMap(list.get(i)));
                }
            }
            list.clear();
            list = null;
            return resList;
        } catch (java.sql.SQLException e){
            throw new SQLException().setErrormessage(e.getMessage());
        }
    }

    @Override
    public void beginTransaction() throws SQLException, TException {
        try{
          jdbcHelper.beginTransaction();
        } catch (java.sql.SQLException e){
            throw new SQLException().setErrormessage(e.getMessage());
        }
    }

    @Override
    public void commitTransaction() throws SQLException, TException {
        try{
          jdbcHelper.commitTransaction();
        } catch (java.sql.SQLException e){
            throw new SQLException().setErrormessage(e.getMessage());
        }
    }

    @Override
    public void rollbackTransaction() throws SQLException, TException {
        try{
          jdbcHelper.rollbackTransaction();
        } catch (java.sql.SQLException e){
            throw new SQLException().setErrormessage(e.getMessage());
        }
    }

    @Override
    public void close() throws SQLException, TException {
        try{
          jdbcHelper.close();
        } catch (java.sql.SQLException e){
            throw new SQLException().setErrormessage(e.getMessage());
        }
    }

    private String[] transferObjectArrToStringArr(Object[] arrObj){
        if(arrObj == null) return null;
        String[] arrStr = new String[arrObj.length];
        int i=0;
        for(Object obj : arrObj){
          arrStr[i++] = (String)obj;
        }
        arrObj = null;//置为null，加快回收
        return arrStr;
    }

    private Map<String,String> transferObjectMapToStringMap(Map<String,Object> objMap){
        if(objMap == null) return null;
        Map<String,String> strMap = new HashMap<String, String>();
        Set<Map.Entry<String,Object>> set = objMap.entrySet();
        for(Iterator<Map.Entry<String,Object>> it = set.iterator(); it.hasNext();){
           Map.Entry<String,Object> entry = it.next();
           strMap.put(entry.getKey(), String.valueOf(entry.getValue()));
        }
        objMap.clear();
        objMap = null; //置为null，加快回收
        return strMap;
    }
}
