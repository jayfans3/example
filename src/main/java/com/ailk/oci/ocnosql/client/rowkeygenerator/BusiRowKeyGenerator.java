package com.ailk.oci.ocnosql.client.rowkeygenerator;

import com.ailk.oci.ocnosql.client.config.spi.*;
import org.apache.commons.collections.*;
import org.apache.commons.lang3.*;

import java.security.*;
import java.util.*;

/**
 * @author Administrator
 *
 */
public class BusiRowKeyGenerator implements RowKeyGenerator{
    private MessageDigest md = null;
    private static HashMap<String,String> busiMap = new HashMap<String,String>();
    static {
        Map<String,String> busiMapConf = Connection.getInstance().retrieveValueByPrefix("rowkey.busi.");
        
        for(Map.Entry<String,String> entry:busiMapConf.entrySet()){
            busiMap.put(entry.getKey().replace("rowkey.busi.",""), entry.getValue());
        }
    }

    /* (non-Javadoc)
     * @see com.ailk.oci.ocnosql.client.rowkeygenerator.RowKeyGenerator#generate(java.lang.String, java.lang.String, java.lang.String[], int[], java.lang.String)
     * MD5(needHashValue)+MapUtils.getString(busiMap, appendValue, "")+oriRowKey+currenRowdata[posIndex[0]
     */
    @Override
    public String generate(String oriRowKey, String needHashValue, String[] currenRowdata, int[] posIndex, String appendValue)
    {
        String hashValue = "";
        if (StringUtils.isNotEmpty(needHashValue)) {
            hashValue = getMD5(needHashValue);
        }
        String rowkey = hashValue.concat(MapUtils.getString(busiMap, appendValue, "")).concat(oriRowKey);
        if ((posIndex != null) && (posIndex.length != 0)) {
            rowkey = rowkey.concat(currenRowdata[posIndex[0]]);
        }
        return rowkey;
    }
    
    @Override
    public String generate(String oriRowKey) {
        return getMD5(oriRowKey)+oriRowKey;
    }
    
    /**
     * @param oriRowKey 原始rowkey
     * @return MD5HASH后的rowkey
     */
    private String getMD5(String oriRowKey) {
        if (oriRowKey == null) {
            throw new RowKeyGeneratorException("param of oriRowKey is null");
        }
        if (this.md == null) {
            try {
            	//MD5算法
                this.md = MessageDigest.getInstance("MD5");
            }
            catch (NoSuchAlgorithmException ex) {
                throw new RowKeyGeneratorException("failed init MD5 instance.", ex);
            }
        }
        this.md.reset();
        this.md.update(oriRowKey.getBytes());
        byte[] digest = this.md.digest();
        StringBuffer sb = new StringBuffer();
        for (byte b : digest) {
        	//转为十六进制字符串
            sb.append(Integer.toHexString(b & 0xFF));
        }
        String result = sb.toString();
        //取字符串的1、3、5位
        return result.substring(1, 2).concat(result.substring(3, 4)).concat(sb.toString().substring(5, 6));
    }
	@Override
	public Object generatePrefix(String oriRowKey) {
		return getMD5(oriRowKey);
	}

}
