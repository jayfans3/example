package com.ailk.oci.ocnosql.client.compress;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import com.ailk.oci.ocnosql.client.config.spi.CommonConstants;


/**
 * 功能：压缩和解压缩 HbaseNullCompress区别于 {@link HbaseCompressImpl}
 * ，相同rowkey的数据存储在一个cell中， 但是没有重复数据的压缩处理 。 如，以下有三行相同rowkey的数据: a,b,c,d a,b,e,f
 * m,d,c,d 入库后的存储结构为:a,b,c,d`a,b,e,f`m,d,c,d. 解压是压缩的逆过程.
 * 
 * @author zhuangyang
 * 
 */
public class HbaseNullCompress implements Compress {

	private String seperator = ","; // 字段间的分隔符

	/**
	 * 功能：压缩
	 */
	public String compress(ArrayWritable arr, boolean isFirst,
			Configuration conf) {
		seperator = new String(Base64.decode(conf.get("importtsv.separator")));
		StringBuffer sb = new StringBuffer();
		for (Writable wt : arr.get()) {
			Text t = (Text) wt;
			sb.append(t.toString());
			sb.append(seperator);
		}
		sb.deleteCharAt(sb.lastIndexOf(seperator));
		return sb.toString();
	}

	/**
	 * 功能：解压缩
	 */
	public List<String[]> deCompress(KeyValue keyValue,
			Map<String, String> param) {
		ArrayList<String[]> list = new ArrayList<String[]>();
		seperator = param.get(CommonConstants.SEPARATOR);

		String lineValue;
		String[] recordeInclounm;
		String[] clounm;
		lineValue = new String(keyValue.getValue());
		recordeInclounm = StringUtils.splitByWholeSeparatorPreserveAllTokens(lineValue, "`");
		if(recordeInclounm == null || recordeInclounm.length == 0){
			return list;
		}
		for(String recorde : recordeInclounm){
			clounm = StringUtils.splitByWholeSeparatorPreserveAllTokens(recorde, seperator);
			list.add(clounm);
		}
		return list;
	}
}
