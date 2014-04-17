package com.ailk.oci.ocnosql.client.put;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.ailk.oci.ocnosql.client.config.spi.CommonConstants;

public class MutilPutTest {
	
	@Test
	public void testPut() {
		String tableName = "people_put_test";
		Map<String, String> params = new HashMap<String, String>();
		params.put(CommonConstants.SEPARATOR, ";");
		params.put(CommonConstants.COLUMNS, "f:name,f:age,f:tel,f:sex,f:addr");
		params.put(CommonConstants.SKIPBADLINE, "true");
		params.put(CommonConstants.BATCH_PUT, "true");
		params.put(CommonConstants.HBASE_MAXPUTNUM, "100");
		params.put(CommonConstants.ROWKEY_GENERATOR, "md5");
		params.put(CommonConstants.STORAGE_STRATEGY, "mutipleimporttsv");
		params.put(CommonConstants.ALGOCOLUMN, "tel");
        params.put(CommonConstants.ROWKEYGENERATOR, "md5");
        params.put(CommonConstants.ROWKEYCOLUMN, "name,tel");
        params.put(CommonConstants.ROWKEYCALLBACK, "com.ailk.oci.ocnosql.client.rowkeygenerator.GenRKCallBackDefaultImpl");
        HBasePut client = new HBasePut(tableName, params);
		//params.put(CommonConstants.STORAGE_STRATEGY, "singleimporttsv");
		String line = null;
		try{
			for(int i = 1; i <= 10; i++) {
				line = "row_" + i + ";n" + ((int)(Math.random()*100)) + ";t158" + ((int)(Math.random()*1000))
                        + ";s" + ((int)(Math.random()*10))+ ";a" + ((int)(Math.random()*10000));
				client.put(line);
			}
		}finally{
			client.close();
		}
		System.out.println("put done.");
	}
}
