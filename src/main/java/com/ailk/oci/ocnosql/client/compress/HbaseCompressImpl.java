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
 * 功能：压缩和解压缩。
 * 压缩算法是：将相同rowkey的多行数据存储在一个cell中，且每行数据间用分隔符分开
 * 如，以下有三行相同rowkey的数据:
 * a;b;c;d 
 * a;b;e;f 
 * m;d;c;d
 * 压缩后:a;b;c;d`2-e;3-f`0-m;1-d;2-c;3-d
 * 解压是压缩的逆过程.
 * 说明: 在同一次load中会有压缩
 * @author zhuangyang
 * 
 */
public class HbaseCompressImpl implements Compress {

	private ArrayWritable former = null; // 上一条记录对象

	/**
	 * 功能：压缩
	 */
	/* (non-Javadoc)
	 * @see com.ailk.oci.ocnosql.client.compress.Compress#compress(org.apache.hadoop.io.ArrayWritable, boolean, org.apache.hadoop.conf.Configuration)
	 */
	public String compress(ArrayWritable arr, boolean isFirst, Configuration conf) {
		String seperator = new String(Base64.decode(conf.get("importtsv.separator")));
		StringBuffer sb = new StringBuffer();
		if (isFirst) {//第一条记录直接级加分隔符后返回
			former = arr; 
			for (Writable wt : former.get()) {
				Text t = (Text) wt;
				sb.append(t.toString());
				sb.append(seperator);
			}
			sb.deleteCharAt(sb.lastIndexOf(seperator));
			return sb.toString();
		}

		//第一条记录
		Writable[] a = former.get();
		//当前记录
		Writable[] b = arr.get();

		//如果不是第一条记录，返回比对第一条记录的增量压缩
		for (int i = 0; i < a.length; i++) {
			Text t1 = (Text) a[i];
			Text t2 = (Text) b[i];
			if (!t1.toString().equals(t2.toString())) {
				sb = sb.append(i);
				sb = sb.append("-");
				sb = sb.append(t2.toString());
				sb = sb.append(seperator);
			}
		}
		if (!StringUtils.isEmpty(sb.toString())) {
			sb.deleteCharAt(sb.lastIndexOf(seperator)).toString();
		}
		former = arr; //按照上一条记录对比压缩
		return sb.toString();

	}

	/**
	 * 功能：解压缩
	 */
	public List<String[]> deCompress(KeyValue keyValue, Map<String, String> param) {
		ArrayList<String[]> list = new ArrayList<String[]>();
		String seperator = param.get(CommonConstants.SEPARATOR);
		String lineValue = new String(keyValue.getValue());
		String[] recordeInclounm = StringUtils.splitByWholeSeparatorPreserveAllTokens(lineValue, "`");
		String[] parseFormer = StringUtils.splitByWholeSeparatorPreserveAllTokens(recordeInclounm[0], seperator);
		list.add(parseFormer.clone());
		
		int i = 1;
		String[] next = null;
		try {
			for (; i < recordeInclounm.length; i++) {
				next = StringUtils.splitByWholeSeparatorPreserveAllTokens(recordeInclounm[i], seperator); 
				parseFormer = getWhole(parseFormer, next);
				list.add(parseFormer.clone());
			}
		} catch (Exception e) {
			if(e instanceof ArrayIndexOutOfBoundsException){
				StringBuilder msg = new StringBuilder();
				msg.append("Row index["+i+"] occur error when decompress , preRecord:");
				msg.append("[length:").append(parseFormer.length).append("] content:[");
				for(String col : parseFormer){
					msg.append(col + seperator);
				}
				if(msg.toString().endsWith(seperator)){
					msg.deleteCharAt(msg.length() - 1);
				}
				msg.append("]");
				msg.append(",nextRecord:");
				msg.append("[length:").append(next.length).append("] content:[");
				for(String col : next){
					msg.append(col + seperator);
				}
				if(msg.toString().endsWith(seperator)){
					msg.deleteCharAt(msg.length() - 1);
				}
				msg.append("]");
				throw new CompressException(msg.toString(), e);
			}
		}
		return list;
	}

	private String[] getWhole(String[] formerRecord, String[] latterRecord) {
		String[] kv;
		if(latterRecord == null){ // 如果下一条记录是’，则直接返回上一条结果
			return formerRecord;
		}
		for (int i = 0; i < latterRecord.length; i++) {
			kv = latterRecord[i].split("-");
			if (kv.length >= 2) {
				formerRecord[Integer.parseInt(kv[0])] = StringUtils.substringAfter(latterRecord[i], "-");
			} 
			else {
				formerRecord[Integer.parseInt(kv[0])] = "";
			}
		}
		return formerRecord;
	}
}
