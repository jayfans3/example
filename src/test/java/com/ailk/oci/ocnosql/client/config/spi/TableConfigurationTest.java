package com.ailk.oci.ocnosql.client.config.spi;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class TableConfigurationTest {

	/**
	 * @param args
	 * @throws Exception 
	 */
	@Test
	public  void writeTableConfiguration() throws Exception {
		TableConfiguration tabConf = TableConfiguration.getInstance();
		Configuration conf = Connection.getInstance().getConf();
		Map<String, String> tableMap = new HashMap<String, String>();
		tableMap.put(CommonConstants.TABLE_NAME, "people");
		tableMap.put(CommonConstants.COLUMNS, "f:name,f:age,f:tel,f:sex,f:addr");
		tableMap.put(CommonConstants.SEPARATOR, ";");
        conf.set(CommonConstants.ROWKEY_GENERATOR,"md5");
        conf.set(CommonConstants.ROWKEYCOLUMN,"tel");
		tabConf.writeTableConfiguration(tableMap, conf);
	}

}
