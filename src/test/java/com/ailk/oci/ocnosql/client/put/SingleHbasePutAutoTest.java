package com.ailk.oci.ocnosql.client.put;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.ailk.oci.ocnosql.client.ClientRuntimeException;
import com.ailk.oci.ocnosql.client.config.spi.CommonConstants;

public class SingleHbasePutAutoTest {

	@Test
	public static void put(String[] args) {
		// String tableName = "auto_func_test";
		String tableName = args[0];
		String filePath = args[1];
		String fulshNum = args[2];
		Map<String, String> params = new HashMap<String, String>();
		params.put(CommonConstants.SEPARATOR, ";");
		params.put(CommonConstants.SINGLE_FAMILY, "F");
		params.put(CommonConstants.COLUMNS,"F:A1,F:A2,F:A3,F:A4,F:A5,F:A6,F:A7,F:A8,HBASE_ROW_KEY,F:A10,F:A11,F:A12,F:A13,"
			+ "F:A14,F:A15,F:A16,F:A17,F:A18,F:A19,F:A20,F:A21,F:A22,F:A23,F:A24,F:A25,F:A26,F:A27,F:A28,F:A29,F:A30,F:A31,"
			+ "F:A32,F:A33,F:A34,F:A35,F:A36,F:A37,F:A38,F:A39,F:A40,F:A41,F:A42,F:A43,F:A44,F:A45,F:A46,F:A47,F:A48,F:A49,F:A50,F:A51,F:A52,F:A53,F:A54,"
			+ "F:A55,F:A56,F:A57,F:A58,F:A59,F:A60,F:A61,F:A62,F:A63,F:A64,F:A65,F:A66,F:A67,F:A68,F:A69,F:A70,F:A71,F:A72,"
			+ "F:A73,F:A74,F:A75,F:A76,F:A77,F:A78,F:A79,F:A80,F:A81,F:A82,F:A83,F:A84,F:A85,F:A86,F:A87,F:A88,F:A89,F:A90,"
			+ "F:A91,F:A92,F:A93,F:A94,F:A95,F:A96,F:A97,F:A98,F:A99,F:A100,F:A101,F:A102,F:A103,F:A104,F:A105,F:A106,F:A107,"
			+ "F:A108,F:A109,F:A110,F:A111,F:A112,F:A113,F:A114,F:A115,F:A116,F:A117,F:A118,F:A119,F:A120,F:A121,F:A122,"
			+ "F:A123,F:A124,F:A125,F:A126,F:A127,F:A128,F:A129,F:A130,F:A131,F:A132,F:A133,F:A134,F:A135,F:A136,F:A137,"
			+ "F:A138,F:A139,F:A140,F:A141,F:A142,F:A143,F:A144,F:A145,F:A146,F:A147,F:A148,F:A149,F:A150,F:A151,F:A152,"
			+ "F:A153,F:A154,F:A155,F:A156,F:A157,F:A158,F:A159,F:A160,F:A161,F:A162,F:A163,F:A164,F:A165,F:A166,F:A167,F:A168,F:A169,F:A170");
		params.put(CommonConstants.SKIPBADLINE, "true");
		params.put(CommonConstants.BATCH_PUT, "false");
		params.put(CommonConstants.ROWKEY_UNIQUE, "false");
		params.put(CommonConstants.HBASE_MAXPUTNUM, "100");
		params.put(CommonConstants.ROWKEY_GENERATOR, "md5");
		params.put(CommonConstants.STORAGE_STRATEGY, "singleimporttsv");
		HBasePut client = new HBasePut(tableName, params);
		try {
			File file = new File(filePath);
			if (!file.exists() || file.isDirectory())
				throw new FileNotFoundException();
			BufferedReader br = new BufferedReader(new FileReader(file));
			String temp = null;
			System.out.println("begin put data into " + tableName);
			long startTime = System.currentTimeMillis(); // 获取开始时间
			temp = br.readLine();
			int count = 0;
			while (temp != null) {
				count++;
				client.put(temp);
				temp = br.readLine();
				if (count % Integer.parseInt(fulshNum) == 0) {
					System.out.println("current count " + count);
				}
			}
			long endTime = System.currentTimeMillis(); // 获取结束时间
			System.out
					.println("hbase run time:" + (endTime - startTime) + "ms");

		} catch (IOException e) {
		} catch (ClientRuntimeException e) {
		} catch (NumberFormatException e) {
		} finally {
			client.close();
		}
	}
}
