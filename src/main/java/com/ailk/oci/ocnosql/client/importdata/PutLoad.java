package com.ailk.oci.ocnosql.client.importdata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.ailk.oci.ocnosql.client.put.*;
import com.ailk.oci.ocnosql.client.rowkeygenerator.*;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.ailk.oci.ocnosql.client.ClientRuntimeException;
import com.ailk.oci.ocnosql.client.config.spi.CommonConstants;
import com.ailk.oci.ocnosql.client.config.spi.Connection;
import com.ailk.oci.ocnosql.client.util.HTableUtilsV2;
import com.ailk.oci.ocnosql.common.util.ParseUtil;

public class PutLoad {
	
	public static final Log log = LogFactory.getLog(PutLoad.class);

	public boolean putData(String tableName, String[] recordArr, Map<String, String> param) throws ClientRuntimeException{
        HBasePut put = new HBasePut(tableName,param);
        for(String record : recordArr){
           put.put(record);
        }
        put.close();
        return true;
	}
}
