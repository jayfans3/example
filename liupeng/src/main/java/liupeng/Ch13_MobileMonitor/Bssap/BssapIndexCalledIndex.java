package HBaseIndexAndQuery.QueryHBase.Bssap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import HBaseIndexAndQuery.QueryHBase.HBaseIndex;
import HBaseIndexAndQuery.QueryHBase.QueryBase;

public class BssapIndexCalledIndex  implements HBaseIndex{
	
	private String timeName = null;
	private List<Pair<Integer, Integer>> timeRangeList = null;
	private String calledNum = null;
	private String callingNum = null;
	private List<Pair<Long, Long>> numRangeList = null;
	private String cdr_type = null;
	private List<Integer> cdrValList = null;
	private String delimet =",";
	HTable table =null;
	Scan scan = null;
	QueryBase qb = null;
	boolean checkType = false;
	
	
	public BssapIndexCalledIndex(  QueryBase  lqb)
	{
		try
		{
		qb = lqb;
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.master", "192.168.1.8:60000");
		conf.set("hbase.zookeeper.quorum", "192.168.1.14");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		table = new HTable(conf, "hbase_bssap_cdr_CalledIndex");
		}catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	

	
  public byte[] CombineRowKey(byte[][] byteArrays)
	{
		// 字节总数为：callingNumber(8个字节),time(4个字节),cdr_index(4个字节) 后面的4个字节，补0；
		byte[] rowKey = new byte[17];
		int i = 0;
		for (i = 0; i < 8; i++) {
			rowKey[i] = byteArrays[0][i];
		}
		for (i = 0; i < 4; i++) {
			rowKey[8 +  i] =byteArrays[1][i];
		}
		rowKey[12] = 0;
		rowKey[13] = 0;
		rowKey[14] = 0;
		rowKey[15] = 0;
		rowKey[16] = 0;
		return rowKey;

	}
	
	public byte[] GetTheScanFromTheFirstRow( byte[] beginRowKey, HTable table)
	{
		// 函数getRowOrBefore
		byte[] reslut = null;
		Result a;
		try {
			a = table.getRowOrBefore(beginRowKey, "Offset".getBytes());
			if (a == null) {
				System.out.println("从0开始");
			} else {
				reslut = a.getRow();
				System.out.println("Resul.getRow: " + Bytes.toLong(reslut, 0, 8));
			}
			return reslut;
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public boolean  CheckAttribute(byte[] rowKey)
	{
		
		return true;
//		if( checkType)
//		{
//			return true;
//		}
//		else
//		{
//			return false;
//		}

	}
	
	public void FilterReocordsByIndexTable(ResultScanner scanner, int getOnceNumber,long startCallNumber, 
			int starttime, int endtime, List<Integer> typelist)
			 {
		byte[] cid 	        = new byte[4];
		byte[] start_time = new byte[4];
		byte[] called_number = new byte[8];
		byte[] rowkey = null;
		int time = 0;
		Result[] indexResults = null;
		int i = 0;
		boolean getIndexDataOver = false;
		long findNumber = 0;
		int count = 0;
		int mscID = 0;
		
		List<byte[]> reslutList = new ArrayList<byte[]>();
		
		while (false == getIndexDataOver) {
			try{
			indexResults = scanner.next(getOnceNumber / 4);
			}catch (Exception e) {
			e.printStackTrace();
			}
			for (i = 0; i < indexResults.length && indexResults[i] != null; i++) {
				count++;
				rowkey = indexResults[i].getRow();

				System.arraycopy(rowkey, 0, 		called_number, 0, 8);
				System.arraycopy(rowkey, 8, 	start_time, 0, 4);
								
				findNumber = Bytes.toLong(called_number, 0, 8);
				time = Bytes.toInt(start_time, 0, 4);
				
				
				if(findNumber > startCallNumber)
				{
					qb.ParseCDRPos(reslutList);
					reslutList.clear();
					return ;
				}
	
				if (findNumber == startCallNumber 
						&& time >= starttime && time <= endtime ) {
					if ( CheckAttribute(rowkey)) {
						reslutList.add(indexResults[i].getValue(
								"Offset".getBytes(), null));
//						System.out.println("############################get one data");
					}
				} else if (time > endtime) {
					getIndexDataOver = true;
				}	
				if(reslutList.size() > 0)
				{
					qb.ParseCDRPos(reslutList);
					reslutList.clear();
				}
			}  // end for
			
		}
		if(reslutList.size() > 0)
		{
			qb.ParseCDRPos(reslutList);
			reslutList.clear();
		}
		return ;
	}	

	
	public void SetCondtion(String name ,  List  list)
		{
			if(name.equals(BssapConditionColumn.start_time))
			{
				timeRangeList = list;
			}
			else if(name.equals(BssapConditionColumn.called_number))
			{
				numRangeList = list;
			}
			else if(name.equals(BssapConditionColumn.cdr_type))
			{
				cdrValList  = list;
			}
		}
	
	public void RunQuery()
 {
		int phoneNumberIndex = 0;
		int timeRangeIndex = 0;
		int numGetFromIndex = 1000;

		System.out.println("gyy:***************************BssapIndexCalledIndex  ************************************");
		for (phoneNumberIndex = 0; phoneNumberIndex < numRangeList.size(); phoneNumberIndex++) {
			Pair<Long, Long> PhoneNumber = numRangeList.get(phoneNumberIndex);

			for (timeRangeIndex = 0; timeRangeIndex < timeRangeList.size(); timeRangeIndex++) {
				Pair<Integer, Integer> timePair = timeRangeList
						.get(timeRangeIndex);
				
				for(long number = PhoneNumber.getFirst() ; number <= PhoneNumber.getSecond() ; number++)	
				{
					byte[][] byteArrays = new byte[2][];
					byteArrays[0] = Bytes.toBytes(number);
					byteArrays[1] = Bytes.toBytes(timePair.getFirst());
					
					byte[] beginrowKey = CombineRowKey(byteArrays);
					byte[] startRowKey = GetTheScanFromTheFirstRow(beginrowKey,
							table);
					if (scan == null) {
						scan = new Scan(startRowKey);
					}
					{
						scan.setStartRow(startRowKey);
					}
					
					ResultScanner scanner;
					try {
						scanner = table.getScanner(scan);
						FilterReocordsByIndexTable(scanner,
									numGetFromIndex,
									PhoneNumber.getFirst(),
									timePair.getFirst(), timePair.getSecond(),
									cdrValList );
							
					} catch (IOException e) {
						e.printStackTrace();
					}
					
				}
			}
		}
	}

}
	

