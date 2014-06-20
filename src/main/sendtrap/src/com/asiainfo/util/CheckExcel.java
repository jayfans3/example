package com.asiainfo.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;

import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
 

public class CheckExcel {

	public String[][] excel(String args[]) throws SQLException{
		Connection conn = null;
		try {
			Class.forName("com.mysql.jdbc.Driver").newInstance();
			String url ="jdbc:mysql://10.4.52.108/etl?user=etl&password=etl&useUnicode=true&characterEncoding=utf-8";
			conn = DriverManager.getConnection(url);
		Statement s1=conn.createStatement();
		Statement s2=conn.createStatement();
		Statement s3=conn.createStatement();
		Statement s4=conn.createStatement();
		Statement s5=conn.createStatement();
		Statement s6=conn.createStatement();
		Statement s7=conn.createStatement();
		String time=args[0];//"20140122";
		ResultSet rs1=s1.executeQuery("select  t5.value_ as 'etl time' from(select t2.dbid_,t2.start_ ,t6.value_ from sch_job t4,sch_job_log t3,df_his_act_inst t2,sch_var_log t6 where ( t4.sn_ ='gprs_bh_bj') and  t3.sch_job_id_=t4.dbid_ and t2.flow_inst_=t3.dbid_ and t3.dbid_ =t6.task_id_ and t6.name_='date' and t6.value_ like '"+time+"%') t5 left join df_act_varible t1 on  t1.act_inst_ =t5.dbid_ and t1.key_ in('MAP_INPUT_RECORDS') group by t5.value_  having max(t5.value_) order by t5.value_ asc;");
		ResultSet rs2=s2.executeQuery("select  sum(t1.long_value_) as 'sum' from(select t2.dbid_,t2.start_ ,t6.value_ from sch_job t4,sch_job_log t3,df_his_act_inst t2,sch_var_log t6 where ( t4.sn_ ='gprs_bh_bj') and  t3.sch_job_id_=t4.dbid_ and t2.flow_inst_=t3.dbid_ and t3.dbid_ =t6.task_id_ and t6.name_='date' and t6.value_ like '"+time+"%') t5 left join df_act_varible t1 on  t1.act_inst_ =t5.dbid_ and t1.key_ in('MAP_INPUT_RECORDS') group by t5.value_  having max(t5.value_) order by t5.value_ asc;");
		ResultSet rs3=s3.executeQuery("select  sum(t1.long_value_) as 'right' from(select t2.dbid_,t2.start_,t6.value_ from sch_job t4,sch_job_log t3,df_his_act_inst t2,sch_var_log t6  where ( t4.sn_ ='gprs_bh_bj') and  t3.sch_job_id_=t4.dbid_ and t2.flow_inst_=t3.dbid_ and t3.dbid_ =t6.task_id_ and t6.name_='date' and t6.value_ like  '"+time+"%') t5 left join df_act_varible t1 on  t1.act_inst_ =t5.dbid_ and t1.key_ in('RIGHT_RECORD_NUM') group by t5.value_ having max(t5.value_) order by t5.value_  asc;");
		ResultSet rs4=s4.executeQuery("select  sum(t1.long_value_) as 'wrong' from(select t2.dbid_,t2.start_,t6.value_ from sch_job t4,sch_job_log t3,df_his_act_inst t2,sch_var_log t6  where ( t4.sn_ ='gprs_bh_bj') and  t3.sch_job_id_=t4.dbid_ and t2.flow_inst_=t3.dbid_  and t3.dbid_ =t6.task_id_ and t6.name_='date' and t6.value_ like  '"+time+"%') t5 left join df_act_varible t1 on  t1.act_inst_ =t5.dbid_ and t1.key_ in('ERROR_RECORD_NUM') group by t5.value_  having max(t5.value_) order by t5.value_ asc;");
		ResultSet rs5=s5.executeQuery("select  sum(t1.long_value_) as 'value' from(select t2.dbid_,t2.start_ ,t6.value_ from sch_job t4,sch_job_log t3,df_his_act_inst t2,sch_var_log t6 where ( t4.sn_ ='gprs_bh_bj') and  t3.sch_job_id_=t4.dbid_ and t2.flow_inst_=t3.dbid_ and t3.dbid_ =t6.task_id_ and t6.name_='date' and t6.value_ like '"+time+"%') t5 left join df_act_varible t1 on  t1.act_inst_ =t5.dbid_ and t1.key_ like 'outputLine' group by t5.value_  order by t5.value_ asc;");
		ResultSet rs6=s6.executeQuery("select  sum(t1.long_value_) as 'value' from(select t2.dbid_,t2.start_ ,t6.value_ from sch_job t4,sch_job_log t3,df_his_act_inst t2,sch_var_log t6 where ( t4.sn_ ='gprs_bh_bj') and  t3.sch_job_id_=t4.dbid_ and t2.flow_inst_=t3.dbid_ and t3.dbid_ =t6.task_id_ and t6.name_='date' and t6.value_ like '"+time+"%') t5 left join df_act_varible t1 on  t1.act_inst_ =t5.dbid_ and t1.key_ like 'inputLine' group by t5.value_  order by t5.value_ asc;");
		ResultSet rs7=s7.executeQuery("select  sum(t1.long_value_) as 'value' from(select t2.dbid_,t2.start_ ,t6.value_ from sch_job t4,sch_job_log t3,df_his_act_inst t2,sch_var_log t6 where ( t4.sn_ ='gprs_bh_bj') and  t3.sch_job_id_=t4.dbid_ and t2.flow_inst_=t3.dbid_ and t3.dbid_ =t6.task_id_ and t6.name_='date' and t6.value_ like '"+time+"%') t5 left join df_act_varible t1 on  t1.act_inst_ =t5.dbid_ and t1.key_ like 'badLine' group by t5.value_  order by t5.value_ asc;");
		String[][] r=new String[7][24];
		String[] r1=new String[24];
		String[] r2=new String[24];
		String[] r3=new String[24];
		String[] r4=new String[24];
		String[] r5=new String[24];
		String[] r6=new String[24];
		String[] r7=new String[24];
		for(int i=0;i<24;i++){
			rs1.next();rs2.next();rs3.next();rs4.next();rs5.next();rs6.next();rs7.next();
			try {
				r1[i]=rs1.getString(1);
			} catch (Exception e) {
				r1[i]="0";r2[i]="0";r3[i]="0";r4[i]="0";r5[i]="0";r6[i]="0";r7[i]="0";
				e.printStackTrace();
				continue;
			}
			r2[i]=rs2.getString(1);
			r3[i]=rs3.getString(1);
			r4[i]=rs4.getString(1);
			r5[i]=rs5.getString(1);
			r6[i]=rs6.getString(1);
			r7[i]=rs7.getString(1);
		}
		r[0]=r1;
		r[1]=r2;
		r[2]=r3;
		r[3]=r4;
		r[4]=r5;
		r[5]=r6;
		r[6]=r7;
		return r;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}finally{
			if(conn!=null)conn.close();
		}
	}
	
	public static void main(String args[]){
		CheckExcel dx=new CheckExcel();
			try {
			dx.loadContactsFromExcel(dx.excel(args),args);
			} catch (SQLException e) {
				e.printStackTrace();
		}
	}
	
	  /**
     * 读取人员的联系电话信息
     */
    private void loadContactsFromExcel(String[][] step1,String args[]) {
        try { 
        	boolean isexits=false;
        	String[] s =new String[24];
        	FileInputStream fi=null;
			try {
				fi = new FileInputStream(new File(args[1]));isexits=true;
			} catch (Exception e1) {
				e1.printStackTrace();
				XSSFWorkbook book = new XSSFWorkbook();
				FileOutputStream fileOut = new FileOutputStream(new File(args[1]));   
		        book.write(fileOut);  
		        fi = new FileInputStream(new File(args[1]));isexits=false;
			}  
            Workbook book = null;
            try {
                book = new XSSFWorkbook(fi);
            } catch (Exception ex) {
                book = new HSSFWorkbook(fi);
            }Sheet sheet =null;
            if(isexits){sheet = book.getSheetAt(1);
            for (int i = 2; i < 26; i++) {
            	Row row = sheet.getRow(i);
                Cell scaout = row.getCell(7);
                DecimalFormat df =  new DecimalFormat("###############");// 16位整数位，两小数位
                String temp  = df.format(scaout.getNumericCellValue());
                s[i-2]=temp;
            }}
            Sheet rs =book.createSheet("check");
            Row tti = rs.createRow(0);
            Cell tc=tti.createCell(0);//date
        	Cell tc2=tti.createCell(1);//sca
        	Cell tc3=tti.createCell(2);//sum
        	Cell tc4=tti.createCell(3);//right
        	Cell tc5=tti.createCell(4);//wrong
        	Cell tc6=tti.createCell(5);//差
        	Cell tc7=tti.createCell(6);//inputLine 
        	Cell tc8=tti.createCell(7);//outputLine
        	Cell tc9=tti.createCell(8);//badLine 
        	tc.setCellValue("date");
        	tc2.setCellValue("sca");
        	tc3.setCellValue("sum");
        	tc4.setCellValue("right");
        	tc5.setCellValue("wrong");
        	tc6.setCellValue("sum-sca");
        	tc7.setCellValue("inputLine");
        	tc8.setCellValue("outputLine");
        	tc9.setCellValue("badLine");
            for (int i = 0; i < 24; i++) {
            	Row row = rs.createRow(i+1);
            	Cell c=row.createCell(0);//date
            	Cell c2=row.createCell(1);//sca
            	Cell c3=row.createCell(2);//sum
            	Cell c4=row.createCell(3);//right
            	Cell c5=row.createCell(4);//wrong
            	Cell c6=row.createCell(5);//差
            	Cell c7=row.createCell(6);//inputLine 
            	Cell c8=row.createCell(7);//outputLine
            	Cell c9=row.createCell(8);//badLine 
            	c.setCellValue(step1[0][i]);
            	if(isexits)c2.setCellValue(s[i]);
            	c3.setCellValue(step1[1][i]);
            	c4.setCellValue(step1[2][i]);
            	c5.setCellValue(step1[3][i]);
            	c7.setCellValue(step1[4][i]);
            	c8.setCellValue(step1[5][i]);
            	c9.setCellValue(step1[6][i]);
            	try {
            		if(isexits)c6.setCellValue(Integer.parseInt(step1[1][i])-Integer.parseInt(s[i]));
				} catch (Exception e) {
					c6.setCellValue("null");
//					e.printStackTrace();
				}
            }
            FileOutputStream fileOut = new FileOutputStream(new File(args[1]));   
            book.write(fileOut);   
        } catch (Exception e) {
        	System.out.print(e);
        }
    }

}
