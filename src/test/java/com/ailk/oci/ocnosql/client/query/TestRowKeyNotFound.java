package com.ailk.oci.ocnosql.client.query;

import com.ailk.oci.ocnosql.client.config.spi.*;
import com.ailk.oci.ocnosql.client.config.spi.Connection;
import com.ailk.oci.ocnosql.client.jdbc.*;
import com.ailk.oci.ocnosql.client.jdbc.phoenix.*;
import com.ailk.oci.ocnosql.client.rowkeygenerator.*;
import com.ailk.oci.ocnosql.client.spi.*;
import org.apache.commons.lang.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;
import java.util.*;
/**
 * Created by IntelliJ IDEA.
 * User: lile3
 * Date: 13-11-7
 * Time: 下午5:05
 * To change this template use File | Settings | File Templates.
 */
public class TestRowKeyNotFound {

        private static Connection conn = null;
        private String tableName="LILETEST6";

        // 开始测试之前准备资源
        @Before
        public  void before() {
           System.out.println("connect HBase");
           conn = Connection.getInstance();
        }

        // 测试完成之后释放资源
        @After
        public static void after() {
           conn.getThreadPool().shutdown();
           System.out.println("release connection");
        }



        //测试给定row key，获取数据，使用ocnosql api
        @Test
        public void get() throws java.io.IOException {
           System.out.println("--------------testGet OCnosql-------------");
           ClientAdaptor client = new ClientAdaptor();
//           List<String[]> list = client.queryByRowkey(conn, rowKey,
//                  Arrays.asList(tableName), null, null);//初始化zookeeper
           long startTime=System.currentTimeMillis();   //获取开始时间
           
           Map<String,String> columnMap = new HashMap<String, String>();
           columnMap.put("tel","13290909336");
           columnMap.put("name","uv");
           //columnMap.put("sex","man");
           
           String rowKey = GetRowKeyUtil.getRowKeyByTableName(tableName,columnMap);
           List<String[]> list  = client.queryByRowkeyPrefix(conn,rowKey,Arrays.asList(tableName),null,null);
           
//           List<String[]> list = client.queryByRowkey(conn, rowKey,
//                  Arrays.asList(tableName), null, null);
           for (String[] record : list) {
               System.out.println(StringUtils.join(record, ";"));
           }
           long endTime=System.currentTimeMillis(); //获取结束时间
           System.out.println("hbase run time:"+(endTime-startTime)+"ms");
           System.out.println("--------------end-------------");
           
           HbaseJdbcHelper helper = new PhoenixJdbcHelper();
           try{
             List<Map<String,Object>> resList = helper.executeQuery("select * from LILETEST6");
             System.out.println(resList.get(0).get("SEX"));
             System.out.println(resList.get(1).get("SEX"));
             System.out.println(resList.get(2).get("SEX"));
           }catch (SQLException e){
              e.printStackTrace();
           }
        }
}