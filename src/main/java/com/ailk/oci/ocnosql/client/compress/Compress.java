package com.ailk.oci.ocnosql.client.compress;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.io.ArrayWritable;

/**
 * 压缩和解压缩
 * @author zhuangyang
 *
 */
public interface Compress {
	
	/**
	 * 压缩
	 * @param arr 需要压缩的数据
	 * @param isFirst 是否是第一条数据记录
	 * @param conf
	 * @return
	 */
	public String compress(ArrayWritable arr, boolean isFirst, Configuration conf) throws CompressException;
	
	/**
	 * 解压缩
	 * @param keyValue 需要解压的keyvalue对象
	 * @param param 解压参数
	 * @return
	 */
	public List<String[]> deCompress(KeyValue keyValue, Map<String, String> param) throws CompressException;
	
}
