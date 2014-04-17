package com.ailk.oci.ocnosql.client.query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.junit.Test;

import com.ailk.oci.ocnosql.client.config.spi.Connection;
import com.ailk.oci.ocnosql.client.spi.ClientAdaptor;


/**
 * 单列查询测试.
 * 必要条件：hadoop上必须存在/ocnosqlConf/ocnosqlTable.xml文件
 * 如:
 * <?xml version="1.0" encoding="UTF-8"?>
 * <tables><table tableName="test" importtsv.columns="f:a,f:b,HBASE_ROW_KEY,f:d" importtsv.separator=";"/></tables>
 * 
 * hbase数据：put 'test','18601134210', 'f:a', 'a;b;xiongzhang'
 * @author wangkai8
 *
 */
public class SingleColumnQueryTest {

	@Test
	public void query(){
		Connection conn = Connection.getInstance();
		try{
			ClientAdaptor client = new ClientAdaptor();
			/*
			Criterion criterion = new Criterion();
			criterion.setEqualsTo("field_1", "2");
			
			Criterion queryParam = new Criterion();
			queryParam.setAND();
			queryParam.setEqualsTo("field_2", "50003");
			queryParam.setEqualsTo("field_3", "0");
			queryParam.setEqualsTo("field_4", "Y|v?uj9MPtvC1(g]{.IvT'Nm5yp$fmZJ$tvoB^=G");
			
			criterion.setQueryParam(queryParam);
			*/
			String rowkey = "lifei";
			
			List<ColumnFamily> columnFamilies = new ArrayList<ColumnFamily>();
			ColumnFamily cf = new ColumnFamily();
			cf.setFamily("info");
			//cf.setColumns(new String[]{CommonConstants.ROW_KEY, "email"});
			cf.setColumns(new String[]{"name","age","email"});
			columnFamilies.add(cf);

			List<String[]>  list = client.queryByRowkey(conn, rowkey, Arrays.asList("student"), null, null, columnFamilies);
			for(String[] record : list) {
				System.out.println(StringUtils.join(record, ";"));
			}
			System.out.println("---------------------------");
			//client.queryByRowkey(conn, rowkey, Arrays.asList("dr_query20130302"), criterion, null);
		}finally{
			conn.getThreadPool().shutdown();
		}
		
	}

}
