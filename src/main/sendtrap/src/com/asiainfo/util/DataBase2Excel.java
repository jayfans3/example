package com.asiainfo.util;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


public class DataBase2Excel {
	
	public void excel() throws SQLException{
		int retryTime = 0;
		Connection conn = null;
		try {
			Class.forName("com.mysql.jdbc.Driver");
			String username ="etl";
			String pwd = "etl";
			String url ="10.4.52.108";
			do {
				conn = DriverManager.getConnection(url, username, pwd);
				if(conn == null) {
					Thread.sleep(3000);
				} 
			} while(conn == null && retryTime <= 20);
		} catch (Exception e) {
		}
		
		Statement s=conn.createStatement();
		ResultSet rs=s.executeQuery("select  t5.value_ as 'etl time' from(select t2.dbid_,t2.start_ ,t6.value_ from sch_job t4,sch_job_log t3,df_his_act_inst t2,sch_var_log t6 where ( t4.sn_ ='gprs_bh_bj') and  t3.sch_job_id_=t4.dbid_ and t2.flow_inst_=t3.dbid_ and t3.dbid_ =t6.task_id_ and t6.name_='date' and t6.value_ like '$time%') t5 left join df_act_varible t1 on  t1.act_inst_ =t5.dbid_ and t1.key_ in('MAP_INPUT_RECORDS') group by t5.value_  having max(t5.value_) order by t5.value_ asc;");
		String r="";
		while(rs.next()){
			r+=rs.getString(0)+",";
		}
		System.out.println(r); 
	}
	
	public static void main(String args[]){
			DataBase2Excel dx=new DataBase2Excel();
			try {
				dx.excel();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
		}
	}
}
