package com.ailk.oci.ocnosql.client.put;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.ailk.oci.ocnosql.client.config.spi.CommonConstants;

public class HBasePutTest {

	@Test
	public static void hbasePut(String[] args){
		String tableConfStr = 
				  "	<table name=\"drquery_test\" separator=\";\">"
				+ "		<family>"
				+ "			<name>fm1</name>"
				+ "			<column>"
				+ "				<columnName>c1</columnName>"
				+ "				<composeColumn>true</composeColumn>"
				+ "				<composeColumnRef>a,b,c,d,h</composeColumnRef>"
				+ "			</column>"
				+ "		</family>"
				+ "		<family>"
				+ "			<name>fm2</name>"
				+ "			<column>"
				+ "				<columnRef>f</columnRef>"
				+ "			</column>"
				+ "			<column>"
				+ "				<columnRef>g</columnRef>"
				+ "			</column>"
				+ "			<column>"
				+ "				<columnRef>h</columnRef>"
				+ "			</column>"
				+ "		</family>"
				+ "		<column>a,b,c,d,e,f,g,h</column>"
				+ "		<rowkeyRef>b</rowkeyRef>"
				+ "		<rowkeyExp>${h}_rowkey_${b}_${a}</rowkeyExp>"
				+ "	</table>";
		Map<String, String> param = new HashMap<String, String>();
		param.put(CommonConstants.TABLE_CONF, tableConfStr.replace("\t", "").replace("\n", "").replace("\r", ""));
		HBasePut hbasePut = new HBasePut("drquery_test", param);
		hbasePut.put("11,haha,33,44__,55,66,77,88");
	}

}
