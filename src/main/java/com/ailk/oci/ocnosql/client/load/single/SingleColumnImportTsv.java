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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

import com.ailk.oci.ocnosql.client.config.spi.*;
import com.ailk.oci.ocnosql.client.rowkeygenerator.*;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.KeyValueSortReducer;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ToolRunner;

import com.ailk.oci.ocnosql.client.ClientRuntimeException;
import com.ailk.oci.ocnosql.client.cache.OciTableRef;
import com.ailk.oci.ocnosql.client.compress.HbaseNullCompress;
import com.ailk.oci.ocnosql.client.load.HFileOutputFormat;
import com.ailk.oci.ocnosql.client.load.LoadIncrementalHFiles;
import com.ailk.oci.ocnosql.client.spi.ConfigException;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import org.apache.log4j.spi.*;

/**
 * Tool to import data from a TSV file.
 *
 * This tool is rather simplistic - it doesn't do any quoting or
 * escaping, but is useful for many data loads.
 *
 * @see SingleColumnImportTsv#usage(String)
 */
@SuppressWarnings({ "unchecked", "rawtypes" })
public class SingleColumnImportTsv {
  static Log LOG = LogFactory.getLog(SingleColumnImportTsv.class);
  public final static String NAME = "singleimporttsv";
  public final static String CONF_FILE = "client-runtime.properties";
  
  public static String FAILED_REASON = "failed_reason";
  private String msg;
  private Map<String, Object> retMap = new HashMap<String, Object>();
  
  static final String COMPRESSION_CONF_KEY = "hbase.hfileoutputformat.families.compression";
  final static String MAPPER_CONF_KEY = "importtsv.mapper.class";
  final static String TIMESTAMP_CONF_KEY = "importtsv.timestamp";
  final static String DEFAULT_COMPRESSOR = HbaseNullCompress.class.getName();
  final static Class DEFAULT_MAPPER = SingleColumnImporterMapper.class;
  private static HBaseAdmin hbaseAdmin;

  static class TsvParser {
    /**
     * Column families and qualifiers mapped to the TSV columns
     */
    private final byte[][] families;
    private final byte[][] qualifiers;

    private final byte separatorByte;

    //rowKey在columns中的位置
    //private int rowKeyColumnIndex;

    //public static String ROWKEY_COLUMN_SPEC="HBASE_ROW_KEY";

    /**
     * @param columnsSpecification the list of columns to parser out, comma separated.
     * The row key should be the special token TsvParser.ROWKEY_COLUMN_SPEC
     */
    public TsvParser(String columnsSpecification, String separatorStr) {
      // Configure separator
      byte[] separator = Bytes.toBytes(separatorStr);
      Preconditions.checkArgument(separator.length == 1,
        "TsvParser only supports single-byte separators");
      //分隔符
      separatorByte = separator[0];

      // Configure columns
      ArrayList<String> columnStrings = Lists.newArrayList(
        Splitter.on(',').trimResults().split(columnsSpecification));

      families = new byte[columnStrings.size()][];
      qualifiers = new byte[columnStrings.size()][];

      for (int i = 0; i < columnStrings.size(); i++) {
        String str = columnStrings.get(i);
//        if (ROWKEY_COLUMN_SPEC.equals(str)) {
//          rowKeyColumnIndex = i;
//          continue;
//        }
        String[] parts = str.split(":", 2);
        if (parts.length == 1) {//只有列族名
          families[i] = str.getBytes();
          qualifiers[i] = HConstants.EMPTY_BYTE_ARRAY;
        } else {//有列族名和列名
          families[i] = parts[0].getBytes();
          qualifiers[i] = parts[1].getBytes();
        }
      }
    }

//    public int getRowKeyColumnIndex() {
//      return rowKeyColumnIndex;
//    }
    public byte[] getFamily(int idx) {
      return families[idx];
    }
    public byte[] getQualifier(int idx) {
      return qualifiers[idx];
    }

    /**
     * @param lineBytes 每行的值
     * @param length 行的长度
     * @return
     * @throws BadTsvLineException
     */
    public ParsedLine parse(byte[] lineBytes, int length)
    throws BadTsvLineException {
      // Enumerate separator offsets
      ArrayList<Integer> tabOffsets = new ArrayList<Integer>(families.length);
      for (int i = 0; i < length; i++) {
        if (lineBytes[i] == separatorByte) {
          tabOffsets.add(i);
        }
      }
      if (tabOffsets.isEmpty()) {
        throw new BadTsvLineException("No delimiter");
      }

      tabOffsets.add(length);

      if (tabOffsets.size() > families.length) {
        throw new BadTsvLineException("Excessive columns");
      }
      return new ParsedLine(tabOffsets, lineBytes);
    }

    class ParsedLine {
      private final ArrayList<Integer> tabOffsets;
      private byte[] lineBytes;

      ParsedLine(ArrayList<Integer> tabOffsets, byte[] lineBytes) {
        this.tabOffsets = tabOffsets;
        this.lineBytes = lineBytes;
      }

//      public int getRowKeyOffset() {
//        return getColumnOffset(rowKeyColumnIndex);
//      }
//      public int getRowKeyLength() {
//        return getColumnLength(rowKeyColumnIndex);
//      }
      public int getColumnOffset(int idx) {
        if (idx > 0)
          return tabOffsets.get(idx - 1) + 1;
        else
          return 0;
      }
      public int getColumnLength(int idx) {
        return tabOffsets.get(idx) - getColumnOffset(idx);
      }
      public int getColumnCount() {
        return tabOffsets.size();
      }
      public byte[] getLineBytes() {
        return lineBytes;
      }
    }

    public static class BadTsvLineException extends Exception {
      public BadTsvLineException(String err) {
        super(err);
      }
      private static final long serialVersionUID = 1L;
    }
  }
  
	static class TextArrayWritable extends ArrayWritable {

		public TextArrayWritable() {
			super(Text.class);
		}

		@Override
		protected TextArrayWritable clone() throws CloneNotSupportedException {
			// TODO Auto-generated method stub
			TextArrayWritable newObj = new TextArrayWritable();
			newObj.set(this.get());
			return newObj;
		}
	}

  /**
   * Sets up the actual job. 执行importtsv的mapreduce job
   *
   * @param conf  The current configuration.
   * @return The newly created job.
   * @throws IOException When setting up the job fails.
   */
  public static Job createSubmittableJob(Configuration conf, String tableName, String inputPath, String tmpOutputPath)
  throws IOException, ClassNotFoundException {

    // Support non-XML supported characters
    // by re-encoding the passed separator as a Base64 string.
	//分隔符重新编码成BASE64格式
    String actualSeparator = conf.get(CommonConstants.SEPARATOR);
    if (actualSeparator != null) {
      conf.set(CommonConstants.SEPARATOR,
               Base64.encodeBytes(actualSeparator.getBytes()));
    }

    // See if a non-default Mapper was set，看是否有自定义mapper，否则用默认SingleColumnImporterMapper
    String mapperClassName = conf.get(MAPPER_CONF_KEY);
    Class mapperClass = mapperClassName != null ?
        Class.forName(mapperClassName) : DEFAULT_MAPPER;

    Path inputDir = new Path(inputPath);
    //开始初始化job
    Job job = new Job(conf, NAME + "_" + tableName);
    //Set the Jar by finding where a given class came from.
    job.setJarByClass(SingleColumnImportTsv.class);
    //设置输入路径
    FileInputFormat.setInputPaths(job, inputDir);
    //设置job的inputformat
    job.setInputFormatClass(TextInputFormat.class);
    //设置mapper
    job.setMapperClass(mapperClass);

    String hfileOutPath = tmpOutputPath;
    if (hfileOutPath != null) {
      //如果表不存在，则先创建表
      if (!doesTableExist(tableName)) {
        createTable(conf, tableName);
      }
      HTable table = new HTable(conf, tableName);
      //设置reducer
      job.setReducerClass(SingleColumnReducer.class);
      
      Path outputDir = new Path(hfileOutPath);
      //设置输出路径
      FileOutputFormat.setOutputPath(job, outputDir);
      job.setMapOutputKeyClass(ImmutableBytesWritable.class);
      job.setMapOutputValueClass(TextArrayWritable.class);
      //设置job的参数：partition、outputformat、reduce个数等
      configureIncrementalLoad(job, table);
      
    } else {//如果没有输出路径，则直接往表中put
      // No reducers.  Just write straight to table.  Call initTableReducerJob
      // to set up the TableOutputFormat.
      TableMapReduceUtil.initTableReducerJob(tableName, null, job);
      job.setNumReduceTasks(0);
    }

    TableMapReduceUtil.addDependencyJars(job);
    TableMapReduceUtil.addDependencyJars(job.getConfiguration(),
        com.google.common.base.Function.class /* Guava used by TsvParser */);
    return job;
  }
  
  /**
   * Return the start keys of all of the regions in this table,
   * as a list of ImmutableBytesWritable.
   */
  private static List<ImmutableBytesWritable> getRegionStartKeys(HTable table)
  throws IOException {
    byte[][] byteKeys = table.getStartKeys();
    ArrayList<ImmutableBytesWritable> ret =
      new ArrayList<ImmutableBytesWritable>(byteKeys.length);
    for (byte[] byteKey : byteKeys) {
      ret.add(new ImmutableBytesWritable(byteKey));
    }
    return ret;
  }

  /**
   * Write out a SequenceFile that can be read by TotalOrderPartitioner
   * that contains the split points in startKeys.
   * @param partitionsPath output path for SequenceFile
   * @param startKeys the region start keys
   */
  private static void writePartitions(Configuration conf, Path partitionsPath,
      List<ImmutableBytesWritable> startKeys) throws IOException {
    if (startKeys.isEmpty()) {
      throw new IllegalArgumentException("No regions passed");
    }

    // We're generating a list of split points, and we don't ever
    // have keys < the first region (which has an empty start key)
    // so we need to remove it. Otherwise we would end up with an
    // empty reducer with index 0
    TreeSet<ImmutableBytesWritable> sorted =
      new TreeSet<ImmutableBytesWritable>(startKeys);

    ImmutableBytesWritable first = sorted.first();
    if (!first.equals(HConstants.EMPTY_BYTE_ARRAY)) {
      throw new IllegalArgumentException(
          "First region of table should have empty start key. Instead has: "
          + Bytes.toStringBinary(first.get()));
    }
    sorted.remove(first);

    // Write the actual file
    FileSystem fs = partitionsPath.getFileSystem(conf);
    SequenceFile.Writer writer = SequenceFile.createWriter(fs,
        conf, partitionsPath, ImmutableBytesWritable.class, NullWritable.class);

    try {
      for (ImmutableBytesWritable startKey : sorted) {
        writer.append(startKey, NullWritable.get());
      }
    } finally {
      writer.close();
    }
  }

  /**
   * Configure a MapReduce Job to perform an incremental load into the given
   * table. This
   * <ul>
   *   <li>Inspects the table to configure a total order partitioner</li>
   *   <li>Uploads the partitions file to the cluster and adds it to the DistributedCache</li>
   *   <li>Sets the number of reduce tasks to match the current number of regions</li>
   *   <li>Sets the output key/value class to match HFileOutputFormat's requirements</li>
   *   <li>Sets the reducer up to perform the appropriate sorting (either KeyValueSortReducer or
   *     PutSortReducer)</li>
   * </ul>
   * The user should be sure to set the map output value class to either KeyValue or Put before
   * running this function.
   */
  public static void configureIncrementalLoad(Job job, HTable table)
  throws IOException {
    Configuration conf = job.getConfiguration();
    Class<? extends Partitioner> topClass;
    try {
      topClass = getTotalOrderPartitionerClass();
    } catch (ClassNotFoundException e) {
      throw new IOException("Failed getting TotalOrderPartitioner", e);
    }
    //设置partition类
    job.setPartitionerClass(topClass);
    //Set the key class for the job output data
    job.setOutputKeyClass(ImmutableBytesWritable.class);
    //Set the value class for job outputs
    job.setOutputValueClass(KeyValue.class);
    //设置outputformat为Hfile
    job.setOutputFormatClass(HFileOutputFormat.class);

    // Based on the configured map output class, set the correct reducer to properly
    // sort the incoming values.
    // TODO it would be nice to pick one or the other of these formats.
    if (KeyValue.class.equals(job.getMapOutputValueClass())) {
      job.setReducerClass(KeyValueSortReducer.class);
    } else if (Put.class.equals(job.getMapOutputValueClass())) {
      job.setReducerClass(SingleColumnReducer.class);
    } else {
      LOG.warn("Unknown map output value type:" + job.getMapOutputValueClass());
    }

    LOG.info("Looking up current regions for table " + table);
    //获取表的所有的region的starkey
    List<ImmutableBytesWritable> startKeys = getRegionStartKeys(table);
    LOG.info("Configuring " + startKeys.size() + " reduce partitions " +
        "to match current region count");
    
    //根据region的数量设置reduce数量
    job.setNumReduceTasks(startKeys.size());

    Path partitionsPath = new Path(job.getWorkingDirectory(),
                                   "partitions_" + UUID.randomUUID());
    LOG.info("Writing partition information to " + partitionsPath);

    FileSystem fs = partitionsPath.getFileSystem(conf);
    writePartitions(conf, partitionsPath, startKeys);
    partitionsPath.makeQualified(fs);

    URI cacheUri;
    try {
      // Below we make explicit reference to the bundled TOP.  Its cheating.
      // We are assume the define in the hbase bundled TOP is as it is in
      // hadoop (whether 0.20 or 0.22, etc.)
      cacheUri = new URI(partitionsPath.toString() + "#" +
        org.apache.hadoop.hbase.mapreduce.hadoopbackport.TotalOrderPartitioner.DEFAULT_PATH);
    } catch (URISyntaxException e) {
      throw new IOException(e);
    }
    DistributedCache.addCacheFile(cacheUri, conf);
    DistributedCache.createSymlink(conf);

    // Set compression algorithms based on column families
    configureCompression(table, conf);

    TableMapReduceUtil.addDependencyJars(job);
    LOG.info("Incremental table output configured.");
  }
  
  /**
   * If > hadoop 0.20, then we want to use the hadoop TotalOrderPartitioner.
   * If 0.20, then we want to use the TOP that we have under hadoopbackport.
   * This method is about hbase being able to run on different versions of
   * hadoop.  In 0.20.x hadoops, we have to use the TOP that is bundled with
   * hbase.  Otherwise, we use the one in Hadoop.
   * @return Instance of the TotalOrderPartitioner class
   * @throws ClassNotFoundException If can't find a TotalOrderPartitioner.
   */
private static Class<? extends Partitioner> getTotalOrderPartitionerClass()
  throws ClassNotFoundException {
    Class<? extends Partitioner> clazz = null;
    try {
      clazz = (Class<? extends Partitioner>) Class.forName("org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner");
    } catch (ClassNotFoundException e) {
      clazz =
        (Class<? extends Partitioner>) Class.forName("org.apache.hadoop.hbase.mapreduce.hadoopbackport.TotalOrderPartitioner");
    }
    return clazz;
  }


  /**
   * Serialize column family to compression algorithm map to configuration.
   * Invoked while configuring the MR job for incremental load.
   *
   * Package-private for unit tests only.
   *
   * @throws IOException
   *           on failure to read column family descriptors
   */
  static void configureCompression(HTable table, Configuration conf) throws IOException {
    StringBuilder compressionConfigValue = new StringBuilder();
    HTableDescriptor tableDescriptor = table.getTableDescriptor();
    if(tableDescriptor == null){
      // could happen with mock table instance
      return;
    }
    Collection<HColumnDescriptor> families = tableDescriptor.getFamilies();
    int i = 0;
    for (HColumnDescriptor familyDescriptor : families) {
      if (i++ > 0) {
        compressionConfigValue.append('&');
      }
      compressionConfigValue.append(URLEncoder.encode(familyDescriptor.getNameAsString(), "UTF-8"));
      compressionConfigValue.append('=');
      compressionConfigValue.append(URLEncoder.encode(familyDescriptor.getCompression().getName(), "UTF-8"));
    }
    // Get rid of the last ampersand
    conf.set(COMPRESSION_CONF_KEY, compressionConfigValue.toString());
  }

  private static boolean doesTableExist(String tableName) throws IOException {
    return hbaseAdmin.tableExists(tableName.getBytes());
  }

  private static void createTable(Configuration conf, String tableName)
      throws IOException {
    HTableDescriptor htd = new HTableDescriptor(tableName.getBytes());
    String columns[] = conf.getStrings(CommonConstants.COLUMNS);
    Set<String> cfSet = new HashSet<String>();
    //查找columns中的列族
    for (String aColumn : columns) {
      //if (TsvParser.ROWKEY_COLUMN_SPEC.equals(aColumn)) continue;
      // we are only concerned with the first one (in case this is a cf:cq)
      cfSet.add(aColumn.split(":", 2)[0]);
    }
    for (String cf : cfSet) {
      HColumnDescriptor hcd = new HColumnDescriptor(Bytes.toBytes(cf));
      htd.addFamily(hcd);
    }
    hbaseAdmin.createTable(htd);
  }

  /*
   * @param errorMsg Error message.  Can be null.
   */
  private static void usage(final String errorMsg) {
    if (errorMsg != null && errorMsg.length() > 0) {
      System.err.println("ERROR:"+errorMsg);
    }
    String usage = 
      "Usage: " + NAME + " -Dimporttsv.columns=a,b,c <tablename> <inputdir>\n" +
      "\n" +
      "Imports the given input directory of TSV data into the specified table.\n" +
      "\n" +
      "The column names of the TSV data must be specified using the -Dimporttsv.columns\n" +
      "option. This option takes the form of comma-separated column names, where each\n" +
      "column name is either a simple column family, or a columnfamily:qualifier. The special\n" +
      "column name HBASE_ROW_KEY is used to designate that this column should be used\n" +
      "as the row key for each imported record. You must specify exactly one column\n" +
      "to be the row key, and you must specify a column name for every column that exists in the\n" +
      "input data.\n" +
      "\n" +
      "By default importtsv will load data directly into HBase. To instead generate\n" +
      "HFiles of data to prepare for a bulk data load, pass the option:\n" +
      "  -D" + CommonConstants.IMPORT_TMP_OUTPUT + "=/path/for/output\n" +
      "  Note: if you do not use this option, then the target table must already exist in HBase\n" +
      "\n" +
      "Other options that may be specified with -D include:\n" +
      "  -D" + CommonConstants.SKIPBADLINE + "=false - fail if encountering an invalid line\n" +
      "  '-D" + CommonConstants.SEPARATOR + "=|' - eg separate on pipes instead of tabs\n" +
      "  -D" + TIMESTAMP_CONF_KEY + "=currentTimeAsLong - use the specified timestamp for the import\n" +
      "  -D" + CommonConstants.ROWKEYCOLUMN + "=you can specify which column as row key\n" +
      "  -D" + MAPPER_CONF_KEY + "=my.Mapper - A user-defined Mapper to use instead of " + DEFAULT_MAPPER.getName() + "\n" +
      "For performance consider the following options:\n" +
      "  -Dmapred.map.tasks.speculative.execution=false\n" +
      "  -Dmapred.reduce.tasks.speculative.execution=false";
    System.err.println("ERROR:"+usage);
  }

  /**
   * Used only by test method
   * @param conf
   */
  static void createHbaseAdmin(Configuration conf) throws IOException {
    hbaseAdmin = new HBaseAdmin(conf);
  }
  
  public boolean execute(Connection conn, OciTableRef table) {
		if(conn == null){
	  		msg = "Connection object must not be null";
	  		retMap.put(FAILED_REASON, msg);
			LOG.error(msg);
			throw new ClientRuntimeException(msg);
	  	}
		Configuration conf = conn.getConf();
		if (table == null) {
			msg = "table must not be null";
			retMap.put(FAILED_REASON, msg);
			LOG.error(msg);
			throw new ClientRuntimeException(msg);
		}
		
		String tableName = table.getName();
		String column = table.getColumns();
		String seperator = table.getSeperator();
		String inputPath = table.getInputPath();
		String tmpOutPut = table.getImportTmpOutputPath();
		String skipBadLine = table.getSkipBadLine();
		String compressor = table.getCompressor();
        String rowkeyUnique = table.getRowKeyUnique();
        String algoColumn = table.getAlgoColumn();
        String rowkeyGenerator = table.getRowkeyGenerator();
        String rowkeyColumn = table.getRowkeyColumn();
        String callback = table.getCallback();
		
		if(StringUtils.isEmpty(tableName)){
			msg = "No " + CommonConstants.TABLE_NAME + " specified. Please check config,then try again after refreshing cache";
			retMap.put(FAILED_REASON, msg);
			LOG.error(msg);
			throw new ConfigException(msg);
		}
		conf.set(CommonConstants.TABLE_NAME, tableName);
		
//		if(StringUtils.isEmpty(seperator)){
//			msg = "No " + CommonConstants.SEPARATOR + " specified. Please check config,then try again after refreshing cache";
//			retMap.put(FAILED_REASON, msg);
//			LOG.error(msg);
//			throw new ConfigException(msg);
//		}
//		conf.set(CommonConstants.SEPARATOR, seperator);
		
	    if(StringUtils.isEmpty(seperator)){
	    	conf.set(CommonConstants.SEPARATOR, CommonConstants.DEFAULT_SEPARATOR);
	    }
		
		// Make sure columns are specified, splited by ","
		String columns[] = StringUtils.splitByWholeSeparatorPreserveAllTokens(column, ",");
		if (columns == null) {
			msg = "No " + CommonConstants.COLUMNS + " specified. Please check config,then try again after refreshing cache";
			retMap.put(FAILED_REASON, msg);
			LOG.error(msg);
			throw new ConfigException(msg);
		}
		conf.set(CommonConstants.COLUMNS, column);

        if(StringUtils.isEmpty(rowkeyColumn) && StringUtils.isEmpty(algoColumn)){
			msg = "No " + CommonConstants.ROW_KEY + " rule specified. Please check config,then try again after refreshing cache";
			retMap.put(FAILED_REASON, msg);
			LOG.error(msg);
			throw new ConfigException(msg);
		}
		conf.set(CommonConstants.SEPARATOR, seperator);

//		int rowkeysFound = 0;
//		for (String col : columns) {
//			if (col.equals(CommonConstants.ROW_KEY))
//				rowkeysFound++;
//		}
//		//HBASE_ROW_KEY只能有一个
//		if (rowkeysFound != 1) {
//			msg = "Must specify exactly one column as " + CommonConstants.ROW_KEY + ". Please check config,then again after refreshing cache";
//			retMap.put(FAILED_REASON, msg);
//			LOG.error(msg);
//			throw new ConfigException(msg);
//		}

		//除了HBASE_ROW_KEY外，至少还要有一个column
		if (columns.length < 2) {
			msg = "One or more columns in addition to the row key are required. Please check config,then try again after refreshing cache";
			retMap.put(FAILED_REASON, msg);
			LOG.error(msg);
			throw new ConfigException(msg);
		}

		//查找列，有":"的是列
		String[] columnTmp = null;
		for (int i = 0; i < columns.length; i++) {
			columnTmp = columns[i].split(":");
			if (columnTmp != null && columnTmp.length == 2) {
				break;
			}
		}
		
		//列族名称
		conf.set(CommonConstants.SINGLE_FAMILY, columnTmp[0]);
		
		//是否过滤错行
		if(!StringUtils.isEmpty(skipBadLine)){
			conf.set(CommonConstants.SKIPBADLINE, skipBadLine);
		}
		//压缩方式
	  conf.set(CommonConstants.COMPRESSOR, (compressor == null)?DEFAULT_COMPRESSOR:compressor);
      conf.set(CommonConstants.ALGOCOLUMN, algoColumn);
      conf.set(CommonConstants.ROWKEY_GENERATOR, rowkeyGenerator);
	  conf.set(CommonConstants.ROWKEYCOLUMN, rowkeyColumn);
	  conf.set(CommonConstants.ROWKEYCALLBACK, callback);


		boolean ret = false;
//		Counter failCounter = null;
		try {
			hbaseAdmin = new HBaseAdmin(conf);
            TableConfiguration.getInstance().writeTableConfiguration(tableName, column, seperator, conf);
//			Job job = createSubmittableJob(conf, tableName, inputPath, tmpOutPut);
//			//执行job
//			ret = job.waitForCompletion(true);
//			Counters counters = job.getCounters();
//			for (String groupName : counters.getGroupNames()) {
//				failCounter = counters.findCounter(groupName, "NUM_FAILED_MAPS");
//				if(failCounter != null){
//					break;
//				}
//			}
            conf.set(CommonConstants.TABLE_NAME,tableName);
            String hdfs_url = conf.get(CommonConstants.HDFS_URL);
            FileSystem fs =  FileSystem.get(URI.create(hdfs_url),conf);
            FileStatus[] fileStatusArr = fs.listStatus(new Path(hdfs_url + inputPath));
            if(fileStatusArr != null && fileStatusArr.length > 0){
                if(fileStatusArr[0].isFile()){
                  ret= runJob(conf, tableName, inputPath, tmpOutPut);
                }
                int inputPathNum = 0;
                for(FileStatus everyInputPath : fileStatusArr){
                    Path inputPathStr = everyInputPath.getPath();
                    String absoluteInputPathStr =  inputPath + "/"+ inputPathStr.getName();
                    boolean retCode = runJob(conf, tableName, absoluteInputPathStr, tmpOutPut+"/"+ inputPathStr.getName());
                    if(retCode){
                      String base64Seperator = conf.get(CommonConstants.SEPARATOR);
                      conf.set(CommonConstants.SEPARATOR,new String(Base64.decode(base64Seperator))); //重置separator
                      if(inputPathNum == fileStatusArr.length-1){
                          ret = true;
                      }
                      inputPathNum++;
                      continue;
                    }else{ //出现错误
                        ret = false;
                        inputPathNum++;
                        break;
                    }
                }
            }


		} catch (Exception e) {
			msg = "job execute failed,nested exception is " + e;
			retMap.put(FAILED_REASON, msg);
			LOG.error(msg);
			throw new ClientRuntimeException(msg);
		}
		
		boolean result = true;
		if (!ret) {
			msg = "execute job failed,please check map/reduce log in jobtracker page";
			retMap.put(FAILED_REASON, msg);
			result = false;
		}
        /*
        else {
			String[] params = new String[2];
			params[0] = tmpOutPut;
			params[1] = tableName;
			int retrunCode = -1;
			try {
				//bulkload complete
				retrunCode = ToolRunner.run(new LoadIncrementalHFiles(conf),
						params);
			} catch (Exception e) {
				msg = "job execute failed,nested exception is " + e;
				retMap.put(FAILED_REASON, msg);
				LOG.error(msg);
				throw new ClientRuntimeException(msg);
			}
			if(retrunCode != 0) result = false;
		}
        */
		return result;
	}
  
  private static Map<String, String> getProperty(){
	  Map<String, String> map = null;
	  try {
		  String conf_Path = SingleColumnImportTsv.class.getClassLoader().getResource(CONF_FILE).getPath();
		  
		  File file = new File(conf_Path);
		  Properties prop = new Properties();
		  prop.load(new FileReader(file));
		  
		  map = new HashMap<String, String>();
		  for(Object key : prop.keySet()){
			  map.put((String)key, (String)prop.get(key)); 
		  }
	} catch (FileNotFoundException e) {
		e.printStackTrace();
	} catch (IOException e) {
		e.printStackTrace();
	}
	  return map;
  }

  /**
   * Main entry point.
   *
   * @param args  The command line parameters.
   * @throws Exception When running the job fails.
   */
  public static void main(String[] args) throws Exception {
	  Map<String, String> map = getProperty();
	  if(map == null || map.size() == 0){
		  System.err.println("Error: read conf file " + CONF_FILE + " occur error.");
          System.exit(0);
	  }
	  Configuration conf = Connection.getInstance().getConf();
    
      String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
      if (otherArgs.length < 2) {
          usage("Wrong number of arguments: " + otherArgs.length);
          System.exit(-1);
      }

    // Make sure columns are specified
    String columns = conf.get(CommonConstants.COLUMNS);
    if (columns == null) {
      usage("No columns specified. Please specify with -D" + CommonConstants.COLUMNS+"=...");
      System.exit(-1);
    }
    String seperator = conf.get(CommonConstants.SEPARATOR);
    if(seperator == null){
    	conf.set(CommonConstants.SEPARATOR, CommonConstants.DEFAULT_SEPARATOR);
        seperator = CommonConstants.DEFAULT_SEPARATOR;
    }
    // Make sure one or more columns are specified
    if (columns.split(",").length < 2) {
      usage("One or more columns in addition to the row key are required");
      System.exit(-1);
    }
    //make sure tableName and columns are upper to used by phoenix.
    columns = columns.toUpperCase();
    String tableName = otherArgs[0].toUpperCase();
    String inputPath = otherArgs[1];

    hbaseAdmin = new HBaseAdmin(conf);
    String tmpOutputPath = conf.get(CommonConstants.IMPORT_TMP_OUTPUT);
    conf.set(CommonConstants.TABLE_NAME,tableName);
    conf.set(CommonConstants.COLUMNS,columns);
    String hdfs_url = conf.get(CommonConstants.HDFS_URL);
    FileSystem fs =  FileSystem.get(URI.create(hdfs_url),conf);
    FileStatus[] fileStatusArr = fs.listStatus(new Path(hdfs_url + inputPath));
    if(fileStatusArr != null && fileStatusArr.length > 0){
        TableConfiguration.getInstance().writeTableConfiguration(tableName,columns,seperator,conf);
        if(fileStatusArr[0].isFile()){ //目录只包含文件
           boolean result = runJob(conf, tableName, inputPath, tmpOutputPath);
           if(result){
            System.exit(0);
           }
           System.exit(-1);
        }
        for(FileStatus everyInputPath : fileStatusArr){ //目录包含子目录
            Path inputPathStr = everyInputPath.getPath();
            String absoluteInputPathStr =  inputPath + "/"+ inputPathStr.getName();
            FileStatus[] subFileStatusArr = fs.listStatus(new Path(hdfs_url + absoluteInputPathStr));
            if(subFileStatusArr == null || subFileStatusArr.length==0)//如果目录为空则不用起job
                continue;
            boolean ret = runJob(conf, tableName, absoluteInputPathStr, tmpOutputPath+"/"+ inputPathStr.getName());
            if(ret){
              String base64Seperator = conf.get(CommonConstants.SEPARATOR);
              conf.set(CommonConstants.SEPARATOR,new String(Base64.decode(base64Seperator))); //重置separator
              continue;
            }else //出现错误
              System.exit(-1);

        }
    }
    System.exit(0); //空目录
  }

    private static boolean runJob(Configuration conf, String tableName, String inputPath, String tmpOutputPath){
        Job job = null;
        try{
            job = createSubmittableJob(conf, tableName, inputPath, tmpOutputPath);
        }catch (Exception e){
            System.err.println("ERROR:singlecolumn bulkload when create submittableJob error is :"
                    + e.fillInStackTrace());
            return false;
        }
        boolean completion = false;
        try{
            if(job == null)
                return false;
            completion = job.waitForCompletion(true);
        }catch (Exception e){
            System.err.println("ERROR:singlecolumn bulkload when execute Job error is :"+
                    e.fillInStackTrace());
            return false;
        }
        try{
            if(completion && !StringUtils.isEmpty(tmpOutputPath)){
                 String[] toolRunnerArgs = new String[]{tmpOutputPath,tableName};
                 int ret = ToolRunner.run(new LoadIncrementalHFiles(conf), toolRunnerArgs);
                 return ret==0;
            }else{
                return false;
            }
        }catch (Exception e){
            System.err.println("ERROR:singlecolumn bulkload when LoadIncrementalHFiles error is :"
                    + e.fillInStackTrace());
            return false;
        }
    }

    public Map<String, Object> getReturnMap() {
        return retMap;
    }

}
