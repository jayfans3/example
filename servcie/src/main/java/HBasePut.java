package com.asiainfo.billing.bill.util;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.NoServerForRegionException;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.util.ReflectionUtils;
import com.ailk.ocnosql.core.CommonConstants;
import com.ailk.ocnosql.core.OCNoSqlConnectException;
import com.ailk.ocnosql.core.TableNotFoundException;
import com.ailk.ocnosql.core.compress.IHbaseCompress;
import com.ailk.ocnosql.core.config.OCNoSqlConfiguration;
import com.ailk.ocnosql.core.config.TableConfiguration;
import com.ailk.ocnosql.core.utils.rowkeygenerator.RowKeyGenerator;
import com.ailk.ocnosql.core.utils.rowkeygenerator.RowKeyGeneratorHolder;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;


public class HBasePut {
	private static Log log = LogFactory.getLog (HBasePut.class);

	private  String tablename;

	private HTable table;

	private String oriRowKey;
	
	private Map<String,List<TextArrayWritable>> qualifiers_texts = new HashMap<String,List<TextArrayWritable>>();
	
	List<Put> redueMapResult = new ArrayList<Put>();

	private List<String> inputData;
	
	public void hbasePut() throws Exception{
		insertHbase();
	}

	//读磁盘的修改数据文件
	private Map<String,List<String>> readFile(String args) throws Exception {
		Map<String,List<String>> s=WriteFile.readFile(args+java.io.File.separator+"output");
		return s;
	}

	private void insertHbase() {
		
		try {
			//组装数据
			initParser();
			initCompress();
			createPut();
			getTableAndPut();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}


	private void getTableAndPut() {
        HTablePool tablePool = OCNoSqlConfiguration. getTablePool();
        if (tablePool == null) {
             throw new NullPointerException("HTablePool object is null,please check out config");
        }
        try {
             table = (HTable) tablePool.getTable(tablename);
        }
        catch (Exception e1) {
                  
             if(e1 instanceof org.apache.hadoop.hbase.TableNotFoundException||
                       (e1.getCause()!= null&& e1.getCause() instanceof org.apache.hadoop.hbase.TableNotFoundException)){
                  throw new TableNotFoundException("failed get table from hbase ds, caused by " + e1.getLocalizedMessage());
             }
             else if (e1 instanceof ZooKeeperConnectionException||
                       (e1.getCause()!= null&& e1.getCause() instanceof ZooKeeperConnectionException)){
                  throw new OCNoSqlConnectException("failed connect zookeeper, caused by " + e1.getLocalizedMessage());
             }
             else if (e1 instanceof NoServerForRegionException||
                       (e1.getCause()!= null&& e1.getCause() instanceof NoServerForRegionException)){
                  throw new OCNoSqlConnectException("faild init HTable object,caused by searching region occur error, caused by " + e1.getLocalizedMessage());
             }
             else {
                  try {
					throw new Exception("connect table " + tablename + " occur error, caused by " + e1.getLocalizedMessage());
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
             }
        }
        if (table == null) {
             if(log .isErrorEnabled()){
                  log.error("[ocnosql]can't connect table " + tablename + " or table " + tablename + " is not exist. id=");
             }
        }
        try {
			table.put(redueMapResult);
			table.flushCommits();
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	// 包括 parase |压缩 
	
	private void initParser() throws Exception {
		setup();
		for(String inputData_row:inputData){
		Text value=new Text();
		value.set(inputData_row);
		value=parser.transValue(value);
		byte[] lineBytes = value.getBytes();
		try {
			TsvParser.ParsedLine parsed = parser.parse(lineBytes, value.getLength());
			Text[] texts = new Text[parsed.getColumnCount() -1];
			int index = 0;
			for (int i = 0; i < parsed.getColumnCount(); i++) {
				if (i == parser.getRowKeyColumnIndex()){
					continue;
				}
				text = new Text();
				text.append(lineBytes, parsed.getColumnOffset(i), parsed.getColumnLength(i));
				texts[index] = text;
				index++;
			}
			TextArrayWritable writer= new TextArrayWritable();
			writer.set(texts);
			String currentQ=""+parser.quatiter+"_"+parser.prossestime;
			if(qualifiers_texts.containsKey(currentQ)){
				List<TextArrayWritable> qualifier=qualifiers_texts.get(currentQ);
				qualifier.add(writer);
				qualifiers_texts.put(currentQ, qualifier);
			}else{
				List<TextArrayWritable> qualifier=new ArrayList<TextArrayWritable>();
				qualifier.add(writer);
				qualifiers_texts.put(currentQ, qualifier);
			}
		} 
		catch (Exception badLine) {
			badLine.printStackTrace();
			if (skipBadLines) {
				System.err.println("Bad line at offset:\n" + badLine.getMessage());
				badLineCount.increment(1);
				return;
			}
		} }
	}

	private TsvParser parser;
	private String generateRowKeyRule=null;
	String SEPARATOR_CONF_KEY = "importtsv.separator";
	String DEFAULT_SEPARATOR = "\t";
	String COLUMNS_CONF_KEY = "importtsv.columns";
	/** Should skip bad lines */
	private boolean skipBadLines;
	private Counter badLineCount;
	private Text text;
	final static String SKIP_LINES_CONF_KEY = "importtsv.skip.bad.lines";
	protected void setup() throws Exception {
		Configuration conf = OCNoSqlConfiguration.getConfiguration();
		generateRowKeyRule = conf.get("rowkey.generator");
		// If a custom separator has been used,
		// decode it back from Base64 encoding.
		String separator = conf.get(SEPARATOR_CONF_KEY);
		if (separator == null || "\\t".equals(separator)) {
			separator = DEFAULT_SEPARATOR;
		} else {
			separator = new String(Base64.decode(separator));
		}
		parser = new TsvParser();
		if (parser.getRowKeyColumnIndex() == -1) {
			throw new RuntimeException("No row key column specified");
		}
		
		skipBadLines = OCNoSqlConfiguration.getConfiguration().getBoolean(
				SKIP_LINES_CONF_KEY, true);
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
	
	 class TsvParser {
		/**
		 * Column families and qualifiers mapped to the TSV columns
		 */
		private  byte[][] families;
		private  byte[][] qualifiers;
		private List<String> qualifiers_list;

		private  byte separatorByte;

		private int rowKeyColumnIndex; 
		
		private String quatiter;
		
		private String prossestime;
		
		private CharSequence columnsSpecification;

		public  String ROWKEY_COLUMN_SPEC = "HBASE_ROW_KEY";
		
		public  String Qualifier_SPEC = "Qualifier";
		
		public  String prossestime_SPEC = "prossestime";
		
		public  String tablename_SPEC = "tablename";

		/**
		 * @param columnsSpecification
		 *            the list of columns to parser out, comma separated. The
		 *            row key should be the special token
		 *            TsvParser.ROWKEY_COLUMN_SPEC
		 * @throws Exception 
		 */
		public  TsvParser() throws Exception {
			// Configure separator
			byte[] separator = Bytes.toBytes("	");
			Preconditions.checkArgument(separator.length == 1,
					"TsvParser only supports single-byte separators");
			separatorByte = separator[0];
			columnsSpecification= TableConfiguration.getInstance().getTableInfo(tablename, CommonConstants.COLUMNS);
			ArrayList<String> columnStrings = Lists.newArrayList(Splitter.on(
					',').trimResults().split(columnsSpecification));
			families = new byte[columnStrings.size()][];
			qualifiers = new byte[columnStrings.size()][];
			for (int i = 0; i < columnStrings.size(); i++) {
				String str = columnStrings.get(i);
				if (ROWKEY_COLUMN_SPEC.equals(str)) {
					rowKeyColumnIndex = i;
					continue;
				}
				String[] parts = str.split(":", 2);
				if (parts.length == 1) {
					families[i] = str.getBytes();
					qualifiers[i] = HConstants.EMPTY_BYTE_ARRAY;
				} else {
					families[i] = parts[0].getBytes();
					qualifiers[i] = parts[1].getBytes();
				}
			}
		}

		public Text transValue(Text value) {
			String[] row=value.toString().split("	");
//			row ad protime 12134 cloumnsname 123 cl1 2343
			StringBuffer sb=new StringBuffer();
			for(int i=0;i<qualifiers.length;i++){
				
				//rowkey
				if(qualifiers[i]==null){
					for(int j=0;j<row.length;j++){
					if(rowKeyColumnIndex==i&&row[j].equals("ROW_KEY")){
					sb.append(row[j+1]+"	");continue;
				    }
					}}else{
				for(int j=0;j<row.length;j++){
					
					if(row[j].equals("Qualifier")){
						quatiter=row[j+1];continue;
						}
					else if(row[j].equals("prossestime")){
						prossestime=row[j+1];continue;
						}
					else if(row[j].equals("tablename")){
						continue;
						}
					else if(new String(qualifiers[i]).equals(row[j])||new String(qualifiers[i]).equals(row[j]+"_[INDEX]")||new String(qualifiers[i]).equals(row[j]+"_[SUM]")){
						sb.append(row[j+1]+"	");continue;
					}
				}}
			}
			sb.deleteCharAt(sb.length()-1);
			return new Text(sb.toString());
		}

		public int getRowKeyColumnIndex() {
			return rowKeyColumnIndex;
		}

		public byte[] getFamily(int idx) {
			return families[idx];
		}

		public byte[] getQualifier(int idx) {
			return qualifiers[idx];
		}

		public ParsedLine parse(byte[] lineBytes, int length)
				throws BadTsvLineException {
			// Enumerate separator offsets
			ArrayList<Integer> tabOffsets = new ArrayList<Integer>(
					families.length);
			String escapeStr = "\\";
			byte b = escapeStr.getBytes()[0];
			for (int i = 0; i < length; i++) {
				if (i != 0) {
					if (lineBytes[i - 1] != b && lineBytes[i] == separatorByte) { // 新增转移字符的处理  zhuangyang 2012-6-25 0:18:08
						tabOffsets.add(i);
					}
				} else {
					if (lineBytes[i] == separatorByte) {
						tabOffsets.add(i);
					}
				}
			}
			if (tabOffsets.isEmpty()) {
				throw new BadTsvLineException("No delimiter：{" + Bytes.toString(lineBytes) + "}");
			}

			tabOffsets.add(length);

			if (tabOffsets.size() > families.length) {
				throw new BadTsvLineException("Excessive columns：{" + Bytes.toString(lineBytes) + "}");
			} else if (tabOffsets.size() < families.length) {
				throw new BadTsvLineException("Missing columns：{" + Bytes.toString(lineBytes) + "}");
			}else if (tabOffsets.size() <= getRowKeyColumnIndex()) {
				throw new BadTsvLineException("No row key：{" + Bytes.toString(lineBytes) + "}");
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

			public int getRowKeyOffset() {
				return getColumnOffset(rowKeyColumnIndex);
			}
			
			public int getRowKeyLength() {
				return getColumnLength(rowKeyColumnIndex);
			}

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

		public  class BadTsvLineException extends Exception {
			public BadTsvLineException(String err) {
				super(err);
			}

			private static final long serialVersionUID = 1L;
		}

	}
	
	
	
	
	
	
	
	final static String RECORD_SEPARATOR_CONF_KEY = "importtsv.record.separator";
	final static String SINGLE_FAMILY = "importtsv.family";
	final static String DEFAULT_SINGLE_FAMILY = "f"; // zhuangyang
	private StringBuilder sb = new StringBuilder();
	private IHbaseCompress compressor;
	byte[] family = null;
	String record_separator = "`";
	public void initCompress(){
	//===============reduece
	TreeSet<KeyValue> map = null;
	SimpleDateFormat formatter = new SimpleDateFormat("ddHHmm");
	map = new TreeSet<KeyValue>(KeyValue.COMPARATOR);
	// separator = new String(Base64.decode(context.getConfiguration().get(
	// CloudETLImportTsv.SEPARATOR_CONF_KEY)));

	if (OCNoSqlConfiguration.getConfiguration().get(
			RECORD_SEPARATOR_CONF_KEY) != null) {
		record_separator = OCNoSqlConfiguration.getConfiguration().get(RECORD_SEPARATOR_CONF_KEY);
	}
	family = Bytes.toBytes(OCNoSqlConfiguration.getConfiguration().get(
			SINGLE_FAMILY,DEFAULT_SINGLE_FAMILY));
	
	try {
		initCompressInstance();
	} catch (ClassNotFoundException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	}
	
	@SuppressWarnings("unchecked")
	private void initCompressInstance() throws ClassNotFoundException{
		String compressorName = OCNoSqlConfiguration.getConfiguration().get(CommonConstants.COMPRESSOR);
		Class compressorClass = Class.forName(compressorName);
		compressor = (IHbaseCompress)ReflectionUtils.newInstance(compressorClass, OCNoSqlConfiguration.getConfiguration());
	}
	
	
	
	
	private void createPut() {

		redueMapResult.clear();
		Set<String> keys=qualifiers_texts.keySet();
		RowKeyGenerator generator = RowKeyGeneratorHolder.resolveGenerator(generateRowKeyRule);
		if(generator!=null){
			oriRowKey = (String) generator.generate(oriRowKey);
		}
		 ArrayList<String> keyes= new ArrayList<String>(keys);
         Collections. sort(keyes, new Comparator<String>() {
                 public int compare(String o1, String o2) {
                      String[] qua_time=o1.split( "_");
                      String[] qua_time2=o2.split( "_");
                       long l1=Long.parseLong(qua_time[1]);
                       long l2=Long.parseLong(qua_time2[1]);
                       if(l1>l2){return 1;}else if(l2<l1){return -1;} return 1;
                }});
		for(String key:keyes){
		List<TextArrayWritable> texts=qualifiers_texts.get(key);
		Put put = new Put(oriRowKey.getBytes());
		TextArrayWritable latterText = null;
		long ts=0;
		try {
			int i = 0;
			boolean isFirst = false;
			for(int j =0 ;i<texts.size();j++) {
				latterText = ((TextArrayWritable) texts.get(j)).clone();
				if(i == 0){
					isFirst = true;
				}else{
					isFirst = false;
				}
				String dealStr = compress(latterText, isFirst,  OCNoSqlConfiguration.getConfiguration());
				if (!StringUtils.isEmpty(dealStr)) {
					sb.append(dealStr);
				}
				sb = sb.append(record_separator);
				i ++;
			}
			sb.deleteCharAt(sb.lastIndexOf(record_separator));
			String[] qua_time=key.split("_");
//			put.add(family, qua_time[0].getBytes(),Bytes.toBytes(sb.toString()));
			put.add(family, qua_time[0].getBytes(),Long.parseLong(qua_time[1])+1, Bytes.toBytes(sb.toString()));
			sb.setLength(0);
			redueMapResult.add(put);
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
		Collections.sort(redueMapResult, new Comparator(){
			@Override
			public int compare(Object o1, Object o2) {
				long l1=((org.apache.hadoop.hbase.client.Put)o1).getTimeStamp();
				long l2=((org.apache.hadoop.hbase.client.Put)o2).getTimeStamp();
				if(l1>l2){return 1;}else if(l2<l1){return -1;} return 1;
			}
		});	
	}
	
	 /**
     * 功能：压缩
     */
	 private ArrayWritable former = null; // 上一条记录对象
    public String compress(ArrayWritable arr, boolean isFirst, Configuration conf) {
        String seperator = new String(";");
        StringBuffer sb = new StringBuffer();
        if (isFirst) {
            former = arr; //按照上一条记录对比压缩
            for (Writable wt : former.get()) {
                Text t = (Text) wt;
                sb.append(t.toString());
                sb.append(seperator);
            }
            sb.deleteCharAt(sb.lastIndexOf(seperator));
            return sb.toString();
        }

        Writable[] a = former.get();
        Writable[] b = arr.get();

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
	
	public static void main(String args[]) throws Exception{
//		Map<String,List<String>> rs=hbp.readFile(args[0]);
//		Map<String,List<String>> rs=WriteFile.readFile("D:\\modiryib"+"\\output");
		Map<String,List<String>> rs=WriteFile.readFile(args[0]+java.io.File.separator+"output");
		Set<Entry<String, List<String>>> entset=rs.entrySet();
		for(Entry<String, List<String>> e_:entset){
			HBasePut hbp=new HBasePut();
			String[] sb=e_.getKey().split("%");
			hbp.tablename=sb[0];
			hbp.oriRowKey=sb[1];
			hbp.inputData=e_.getValue();
			hbp.hbasePut();
		}
		System.out.println("完成！");
	}
}
