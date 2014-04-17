package com.ailk.oci.ocnosql.client.cache;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class DBHelper {
	private final static Log log = LogFactory.getLog(DBHelper.class.getSimpleName());

    private String driver;
    private String url;
    private String dbname;
    private String dbpass;

    public DBHelper(String driver, String url, String dbname, String dbpass) {
        super();
        this.driver = driver;
        this.url = url;
        this.dbname = dbname;
        this.dbpass = dbpass;
    }

    private Connection getConn() throws ClassNotFoundException, SQLException {

        Class.forName(driver);
        Connection conn = (Connection) DriverManager.getConnection(url, dbname, dbpass);
        return conn;
    }

    /**
     * 释放资源
     *
     * @param conn-连接对象
     * @param rs-结果集
     */
    private void closeAll(Connection conn, Statement ps, ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (ps != null) {
            try {
                ps.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 执行查询操作
     *
     * @param sql sql语句
     * @return RowSet 结果集，可直接使用
     */
    public List<OciTableRef> executeQuery(String sql) throws SQLException, ClassNotFoundException, Exception {
        Connection connection = null;
        Statement state = null;
        ResultSet rs = null;
        List<OciTableRef> tableList = null;
        try {
            connection = this.getConn();
            state = connection.createStatement();

            log.info("sql: " + sql);
            long b = System.currentTimeMillis();
            log.info("开始查询");

            rs = state.executeQuery(sql);
            
            if(rs == null){
            	log.warn("result set is null");
            	return tableList;
            }
            tableList = new ArrayList<OciTableRef>();
            while(rs.next()){
            	String id = rs.getString("ID");
            	String name = rs.getString("TABLE_NAME");
            	String columns = rs.getString("TABLE_COLUMN");
            	
            	if(StringUtils.isEmpty(name)||StringUtils.isEmpty(columns)){
            		log.error("table_name or table_column is null,tableID=" + id);
            		continue;
            	}
            	
            	OciTableRef tableInternal = new OciTableRef();
            	tableInternal.setName(name);
            	tableInternal.setColumns(columns);
            	tableList.add(tableInternal);
            }

            log.info("共消耗: " + (System.currentTimeMillis() - b) + "ms");

        } catch (SQLException e) {
            throw e;
        } catch (ClassNotFoundException e) {
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            closeAll(connection, state, rs);
        }
        return tableList;
    }

}
