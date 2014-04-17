package com.ailk.oci.ocnosql.client.importdata.phoenix;

import com.ailk.oci.ocnosql.client.jdbc.pool.*;
import org.apache.commons.logging.*;

import java.sql.*;

/**
 * Created by IntelliJ IDEA.
 * User: yangjing5
 * Date: 13-12-12
 * Time: 下午1:28
 * To change this template use File | Settings | File Templates.
 */
public class PhoenixCreate {
    private final static Log log = LogFactory.getLog(PhoenixCreate.class);

    public static Connection conn;
	static{
        conn = DbPool.getConnection();
//		try {
//			Class.forName("com.salesforce.phoenix.jdbc.PhoenixDriver");
//			conn = DriverManager.getConnection("jdbc:phoenix:ocdata05:2485");
//			System.out.println("init done");
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
	}

    public static void main(String[] args){
        String tableName=args[0];
        String tablePrimaryKey=args[1];
        String tableColumns=args[2];
        String[] columns=tableColumns.split(",");
        System.out.println(columns.length);
        StringBuffer sqlBuf = new StringBuffer();
        sqlBuf.append("create table if not exists ").append(tableName)
                .append("(").append(tablePrimaryKey).append(" varchar primary key,");
        for(int i=0;i<columns.length;i++){
            System.out.println(columns[i]);
            sqlBuf.append(columns[i]).append(" varchar ,");
        }
        String sql = sqlBuf.replace(sqlBuf.length()-1,sqlBuf.length(),")").toString();
        System.out.println("sql--------"+sql);
        try{
            Statement st = conn.createStatement();
            boolean re=st.execute(sql); //false 不是失败，而是代表返回的不是ResultSet结果集。
            conn.commit();
            System.out.println("create result = " + (re==false));
        }catch(SQLException e){
           log.error("create table error :",e);
        }finally {
            if(conn != null){
                try {
                    conn.close();
                } catch (SQLException e) {
                   log.error("close connection error:",e);
                }
            }
        }
    }

}

