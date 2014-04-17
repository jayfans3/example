package com.ailk.oci.ocnosql.client.query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.junit.Test;

import com.ailk.oci.ocnosql.client.config.spi.CommonConstants;
import com.ailk.oci.ocnosql.client.config.spi.Connection;
import com.ailk.oci.ocnosql.client.query.criterion.Criterion;
import com.ailk.oci.ocnosql.client.spi.ClientAdaptor;


public class ScanQueryTest {

	@Test
	public void queryForSingle(){
		Connection conn = Connection.getInstance();
		try{
			ClientAdaptor client = new ClientAdaptor();
			Criterion criterion = new Criterion();
			criterion.setEqualsTo("f1","added1", "a");
			criterion.setEqualsTo("f1","added1", "value1");
			criterion.setEqualsTo("f1","added1", "11111");
			Criterion queryParam = new Criterion();
			queryParam.setAND();
			queryParam.setEqualsTo("f1","a", "a");
			queryParam.setEqualsTo("f1","b", "b");
			queryParam.setEqualsTo("f1","d", "xiongzhang");
			criterion.setQueryParam(queryParam);
			List<String[]>  list = client.queryByRowkey(conn, "18601134210", "18601134210", Arrays.asList("test1"), criterion, null, null);
			for(String[] record : list) {
				System.out.println(StringUtils.join(record, ","));
			}
		}finally{
			conn.getThreadPool().shutdown();
		}
	}
	
	
	@Test
	public void queryForMulti(){
		Connection conn = Connection.getInstance();
		try{
			ClientAdaptor client = new ClientAdaptor();
			Criterion criterion = new Criterion();
            criterion.setAND();
            criterion.setMinorThan("f1","c1","b");
            List a = new ArrayList<String>() ;
            criterion.setInExpression("f1","c1",a) ;
//			Criterion queryParam = new Criterion();
//			queryParam.setAND();
//			queryParam.setEqualsTo("f1","c1", "2222");
//			queryParam.setEqualsTo("f1","added1","value1");
//          criterion.setQueryParam(queryParam);
//			Criterion queryParam2 = new Criterion();
//			queryParam2.setOR();
//			queryParam2.setEqualsTo("r", "qieslwtaiy");
//			queryParam2.setEqualsTo("r", "phdtagkyom");
//			queryParam.setQueryParam(queryParam2);
//			criterion.setQueryParam(queryParam);
			List<ColumnFamily> columnFamilies = new ArrayList<ColumnFamily>();
			ColumnFamily cf = new ColumnFamily();
			cf.setFamily("f1");
			cf.setColumns(new String[]{CommonConstants.ROW_KEY, "c1","added1"});
			columnFamilies.add(cf);
			List<String[]>  list = client.queryByRowkey(conn, new String[]{"1111","1113"}, Arrays.asList("wucs1"), criterion, null, columnFamilies);
			for(String[] record : list) {
				System.out.println(StringUtils.join(record, ","));
			}
		}finally{
			conn.getThreadPool().shutdown();
		}
	}

}
