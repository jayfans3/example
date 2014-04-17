package com.ailk.oci.ocnosql.client.rowkeygenerator;


/**
 * rowkey生成器
 * 
 * @author Rex Wang
 *
 * @version
 */
public interface RowKeyGenerator {
	
	/**
	 * @param oriRowKey
	 * @return
	 */
	Object generate(String oriRowKey);
	
	Object generatePrefix(String oriRowKey);

    /**
     * @param oriRowKey 原始rowkey
     * @param needHashValue 需要md5hash的值
     * @param currenRowdata rowkey对应的value值
     * @param posIndex 
     * @param appendValue
     * @return
     */
    String generate(String oriRowKey, String needHashValue, String[] currenRowdata, int[] posIndex, String appendValue);
}
