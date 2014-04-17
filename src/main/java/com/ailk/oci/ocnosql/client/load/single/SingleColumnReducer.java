/**
 * Copyright 2010 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ailk.oci.ocnosql.client.load.single;

import com.ailk.oci.ocnosql.client.compress.*;
import com.ailk.oci.ocnosql.client.config.spi.*;
import com.ailk.oci.ocnosql.client.load.HFileOutputFormat;
import com.ailk.oci.ocnosql.client.load.single.SingleColumnImportTsv.*;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.*;
import org.apache.hadoop.hbase.mapreduce.*;
import org.apache.hadoop.hbase.util.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;

import java.io.*;
import java.text.*;
import java.util.*;

/**
 * Emits sorted Puts.
 * Reads in all Puts from passed Iterator, sorts them, then emits
 * Puts in sorted order.  If lots of columns per row, it will use lots of
 * memory sorting.
 * @see HFileOutputFormat
 * @see KeyValueSortReducer
 */
public class SingleColumnReducer extends
    Reducer<ImmutableBytesWritable, TextArrayWritable, ImmutableBytesWritable, KeyValue> {
	String separator = ",";

	String record_separator = "`";

	private StringBuilder sb;

	private String curTime;

	TreeSet<KeyValue> map = null;
	byte[] family = null;

	private Compress compressor;

	protected void setup(Context context)
			throws IOException, InterruptedException {
		super.setup(context);

		sb = new StringBuilder();
		Date date = new Date();
		SimpleDateFormat formatter = new SimpleDateFormat("ddHHmm");
		curTime = formatter.format(date);
		
		//keyvalue的集合
		map = new TreeSet<KeyValue>(KeyValue.COMPARATOR);

		//记录间隔符
		record_separator = CommonConstants.RECORD_SEPARATOR;
		
		//列族名称
        String familyStr = context.getConfiguration().get(CommonConstants.SINGLE_FAMILY);
        if(StringUtils.isEmpty(familyStr)){
            String columns = context.getConfiguration().get(CommonConstants.COLUMNS);
            familyStr = columns.substring(0,columns.indexOf(":"));
        }
        family = Bytes.toBytes(familyStr);

		try {
			//实例化压缩解压类
			initCompressInstance(context);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
	
  @Override
  protected void reduce(ImmutableBytesWritable row,
			Iterable<TextArrayWritable> texts,Context context)
      throws java.io.IOException, InterruptedException
  {
	//列名为当前时间
	String columName = curTime;

	map.clear();

	Put put = new Put(row.get());
	Iterator<TextArrayWritable> iter = texts.iterator();
	TextArrayWritable latterText = null;
	try {
		int i = 0;
		boolean isFirst = false; 		//是否是首行；
		while (iter.hasNext()) {
			latterText = ((TextArrayWritable) iter.next()).clone();
			if(i == 0){
				isFirst = true;
			}else{
				isFirst = false;
			}

			String dealStr = compressor.compress(latterText, isFirst, context
					.getConfiguration());
			if (!StringUtils.isEmpty(dealStr)) {
				sb.append(dealStr);
			}
			sb = sb.append(record_separator);
			i ++;
		}
		//删除最后的记录间隔符
		sb.deleteCharAt(sb.lastIndexOf(record_separator));
		put.add(family, columName.getBytes(), Bytes.toBytes(sb.toString()));
		sb.setLength(0);

		for (List<KeyValue> kvList : put.getFamilyMap().values()) {
			for (KeyValue kv : kvList) {
				map.add(kv);
			}
		}

		for (KeyValue kv : map) {
			context.write(row, kv);
		}
	} catch (Exception e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
  }
  
	@SuppressWarnings("unchecked")
	private void initCompressInstance(Context context) throws ClassNotFoundException{
		//压缩解压类的名称
		String compressorName = context.getConfiguration().get(CommonConstants.COMPRESSOR);
		//通过反射方式实例化压缩解压类
		Class compressorClass = Class.forName(compressorName);
		compressor = (Compress)ReflectionUtils.newInstance(compressorClass, context.getConfiguration());
	}
}
