/**
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

import com.ailk.oci.ocnosql.client.config.spi.*;
import com.ailk.oci.ocnosql.client.load.single.SingleColumnImportTsv.*;
import com.ailk.oci.ocnosql.client.load.single.SingleColumnImportTsv.TsvParser.*;
import com.ailk.oci.ocnosql.client.rowkeygenerator.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.hbase.io.*;
import org.apache.hadoop.hbase.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import java.io.*;
import java.util.*;

/**
 * Write table content out to files in hdfs.
 */
public class SingleColumnImporterMapper
extends Mapper<LongWritable, Text, ImmutableBytesWritable, TextArrayWritable>
{

  /** Timestamp for all inserted rows */
  private long ts;

  /** Column seperator */
  private String separator;

  /** Should skip bad lines */
  private boolean skipBadLines;
  private Counter badLineCount;
  private TableRowKeyGenerator rowkeyGenerator;
  private Text text;
  private TextArrayWritable writer;
  private String tableName;
  private SingleColumnImportTsv.TsvParser parser;

  public long getTs() {
    return ts;
  }

  public boolean getSkipBadLines() {
    return skipBadLines;
  }

  public Counter getBadLineCount() {
    return badLineCount;
  }

  public void incrementBadLineCount(int count) {
    this.badLineCount.increment(count);
  }

  /**
   * Handles initializing this class with objects specific to it (i.e., the parser).
   * Common initialization that might be leveraged by a subsclass is done in
   * <code>doSetup</code>. Hence a subclass may choose to override this method
   * and call <code>doSetup</code> as well before handling it's own custom params.
   *
   * @param context
   */
  @Override
  protected void setup(Context context) {
    System.out.print("single set up");
    doSetup(context);

    Configuration conf = context.getConfiguration();

    parser = new SingleColumnImportTsv.TsvParser(conf.get(CommonConstants.COLUMNS),
                           separator);
//    if (parser.getRowKeyColumnIndex() == -1) {
//      throw new RuntimeException("No row key column specified");
//    }
  }

  /**
   * Handles common parameter initialization that a subclass might want to leverage.
   * @param context
   */
  protected void doSetup(Context context) {
    Configuration conf = context.getConfiguration();

    // If a custom separator has been used,
    // decode it back from Base64 encoding.
    // 文件分隔符，需要变成BASE64编码
    separator = conf.get(CommonConstants.SEPARATOR);
    if (separator == null) {
      separator = CommonConstants.DEFAULT_SEPARATOR;
    } else {
      separator = new String(Base64.decode(separator));
    }

    skipBadLines = context.getConfiguration().getBoolean(
        CommonConstants.SKIPBADLINE, true);
    
    //获取badline的计数器
    badLineCount = context.getCounter("ImportTsv", "Bad Lines");

//    String rowkeyGennerator = context.getConfiguration().get(CommonConstants.ROWKEY_GENERATOR);
//    //目前只支持MD5算法
//    if(RowKeyGeneratorHolder.TYPE.md5.name().equalsIgnoreCase(rowkeyGennerator)){
//    	rowkeyGenerator = new MD5RowKeyGenerator();
//    }
    tableName = conf.get(CommonConstants.TABLE_NAME);
    List<GenRKStep> genRKStepList = TableConfiguration.getInstance().getTableGenRKSteps(tableName,conf);
    rowkeyGenerator = new TableRowKeyGenerator(conf,genRKStepList);
    writer = new TextArrayWritable();
  }

  /**
   * Convert a line of TSV text into an HBase table row.
   * 
   */
@Override
  public void map(LongWritable offset, Text value,
    Context context)
  throws IOException {
	  byte[] lineBytes = value.getBytes();

		try {
			TsvParser.ParsedLine parsed = parser.parse(lineBytes, value.getLength());
			//列数组
			Text[] texts = new Text[parsed.getColumnCount()];
			int index = 0;
			for (int i = 0; i < parsed.getColumnCount(); i++) {
//				if (i == parser.getRowKeyColumnIndex()){
//					continue;
//				}
				text = new Text();
				//每列的值
				text.append(lineBytes, parsed.getColumnOffset(i), parsed.getColumnLength(i));
				texts[index] = text;
				index++;
			}
			writer.set(texts);
            /*
			//rowkey
			String oriRowKey = new String(lineBytes, parsed.getRowKeyOffset(), parsed.getRowKeyLength());
			
			// hash rowkey
		      String newRowKey = oriRowKey;
		      if(rowkeyGenerator != null){
		    	  newRowKey = (String)rowkeyGenerator.generate(oriRowKey);
		      }
		    */
            String newRowKey = rowkeyGenerator.generateByGenRKStep(value.toString(),false);//由配置文件的规则来直接生成rowkey
            //System.out.println("single column newRowKey = " + newRowKey);
			context.write(new ImmutableBytesWritable(newRowKey.getBytes()), writer);
		}
		catch (BadTsvLineException badLine) {
			if (skipBadLines) {
				System.err.println("Bad line at offset: " + offset.get() + ":\n" + badLine.getMessage());
				badLineCount.increment(1);
				return;
			} 
			else {
				throw new IOException(badLine);
			}
		} 
		catch (InterruptedException e) {
			e.printStackTrace();
		}
  }
}
