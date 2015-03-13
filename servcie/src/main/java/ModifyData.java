package com.asiainfo.billing.bill.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.ailk.ocnosql.queryzd.impl.QueryParam;
import com.asiainfo.billing.bill.service.ModifyQuery;

public class ModifyData {

	public static void main(String args[]) throws Exception{
		/**
		 * 1、把需要需要测试类（如：OtherCRMDao.java）添加到xml文件，让spring启动加载
		 * 2、测试类加载时，需要加载所有的xml文件
		 */ 
		ApplicationContext context = new ClassPathXmlApplicationContext("drquery.service/spring/*.xml");
		
//		String home="D:\\modiryib";
		String home=args[0];
		if(StringUtils.isBlank(home)){System.out.println("home 没配置！");return;}
		Map<String,List<String>> tablename_rows =WriteFile.readTableRowkeys(home);
		
		for(Map.Entry<String,List<String>> t_r:tablename_rows.entrySet()){
			for(String s:t_r.getValue()){
			List<QueryParam> a=new ArrayList<QueryParam>();
			QueryParam param = new QueryParam();
	        param.setAND();
	        param.setRowkey(s.trim());
//	        param.setRowkey("5011827199");
//	        param.setTableName("ACC_EMEND_ITEM201406");
	        param.setTableName(t_r.getKey());
	        a.add(param);
	        List<Map<String,String>> aa=ModifyQuery.query(a);
	        StringBuffer sb=new StringBuffer();
	        for(Map<String,String> map:aa){
	         for(Entry<String, String> evalue:map.entrySet()){
	        	 sb.append(evalue.getKey()+"	"+evalue.getValue()+"	");
	         }
	         sb.deleteCharAt(sb.length()-1);
	         sb.append("\n");
	        }
	        sb.deleteCharAt(sb.length()-1);
	        WriteFile.writeFile(home+java.io.File.separator+"output"+java.io.File.separator+param.getTableName(),param.getRowkey(),sb.toString());
	        WriteFile.writeFile(home+java.io.File.separator+"backup"+System.currentTimeMillis()+java.io.File.separator+param.getTableName(),param.getRowkey(),sb.toString());
//			System.out.println(retValues);
	        		}}
		System.exit(0);
	}
	
}
 