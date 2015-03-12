package HBaseIndexAndQuery.QueryHBase.Bssap;

import java.io.IOException;
import java.text.Format;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import HBaseIndexAndQuery.QueryHBase.HBaseIndex;
import HBaseIndexAndQuery.QueryHBase.QueryBase;
import Format.BytesToString;

public class BssapIndexCIDCalledIndex   implements HBaseIndex{
	

	private List<Pair<Integer, Integer>> timeRangeList = null;
	private List<Pair<Long, Long>> numRangeList = null;
	private List<Integer> cdrValList = null;
	private List<Long> cidList=null;
	
	private String delimet =",";
	HTable table =null;
	Scan scan = null;
	QueryBase qb = null;
	boolean checkType = false;


	public BssapIndexCIDCalledIndex(  QueryBase  lqb)
	{
		try
		{
		qb = lqb;
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.master", "192.168.1.8:60000");
		conf.set("hbase.zookeeper.quorum", "192.168.1.14");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		table = new HTable(conf, "hbase_bssap_cdr_cid_CalledIndex");
		}catch(Exception e)
		{
			e.printStackTrace();
		}
		
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
		else if(name.equals(BssapConditionColumn.cid))
		{
			cidList = list;
		}
	}
		
	public byte[] CombineRowKey(byte[][] byteArrays)
	{
		// 字节总数为： mscid(4个字节) + callingNumber(8个字节) + time(4个字节) + 补0；
		
/*		System.arraycopy(mscid, 0, mscCalledNumTime, 0, 4);
		System.arraycopy(called_number, 0, mscCalledNumTime, 4, 8);
		System.arraycopy(start_time_s, 0, mscCalledNumTime, 12, 4);
		System.arraycopy(cdr_type, 0, mscCalledNumTime, 16, 1);
		System.arraycopy(cdr_index, 0, mscCalledNumTime, 17, 4);*/
		
		byte[] rowKey = new byte[21];
		int i = 0;
		for( i = 0 ; i < 4 ; i ++)
		{
			rowKey[i] = byteArrays[0][i];
		}
		for (i = 0; i < 8; i++) {
			rowKey[4+i] = byteArrays[1][i];
		}
		for (i = 0; i < 4; i++) {
			rowKey[4 + 8 +  i] =byteArrays[2][i];
		}
		rowKey[16] = 0;
		rowKey[17] = 0;
		rowKey[18] = 0;
		rowKey[19] = 0;
		rowKey[20] = 0;
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

	}
	
	public void FilterReocordsByIndexTable(ResultScanner scanner, 
			int getOnceNumber,
			long startCallNumber, 
			int starttime, int endtime, List<Integer> typelist , int  pcid)
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
//				System.out.println("***************find  rowkey bytes is " + Arrays.toString(rowkey));
				
				System.arraycopy(rowkey, 0, 		cid, 0, 4);
				System.arraycopy(rowkey, 4, 		called_number, 0, 8);
				System.arraycopy(rowkey, 12, 	start_time, 0, 4);
				
//				System.out.println("***************find  cid bytes is " + BytesToString.Bytes4ToString(cid));
//				System.out.println("***************find  called_number bytes is " + BytesToString.Bytes8tolong(called_number));
//				System.out.println("***************find  start_time bytes is " + BytesToString.Bytes4ToString(start_time));
				
				mscID = Bytes.toInt(cid, 0, 4);
				findNumber = Bytes.toLong(called_number, 0, 8);
				time = Bytes.toInt(start_time, 0, 4);
//				System.out.println("**********mscid is " + mscID);
				if(mscID > pcid)
				{
					qb.ParseCDRPos(reslutList);
					reslutList.clear();
					return ;
				}
//				System.out.println("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&");
//				System.out.println("WWWW***************find  cid bytes is " + mscID);
//				System.out.println("WWWW***************find  called_number bytes is " + findNumber );
//				System.out.println("WWWW***************find  start_time bytes is " + time );
//				
//				System.out.println("WWWW***************  cid bytes is " +pcid );
//				System.out.println("WWWW***************  called_number bytes is " + startCallNumber);
//				System.out.println("WWWW***************  start_time bytes is " + starttime);
//				System.out.println("WWWW***************  end_time bytes is " + endtime);
				
				
				if (mscID == pcid &&  findNumber == startCallNumber 
						&& time >= starttime && time <= endtime ) {
					if ( CheckAttribute(rowkey)) {
						reslutList.add(indexResults[i].getValue(
								"Offset".getBytes(), null));
//						System.out.println("############################get one data");
					}
				} else if (mscID == pcid
						&& ((findNumber > startCallNumber) || ((findNumber == startCallNumber) && (time > endtime)))) {
					getIndexDataOver = true;
				}
			}
		}
		if(reslutList.size() > 0)
		{
			qb.ParseCDRPos(reslutList);
			reslutList.clear();
		}
		return ;
	}	

	public void RunQuery()
 {
		int phoneNumberIndex = 0;
		int timeRangeIndex = 0;
		int numGetFromIndex = 1000;
		int cidNumber = 0;
		
		for( cidNumber = 0 ;  cidNumber < cidList.size() ; cidNumber++ )
		{			long orgcid = cidList.get(cidNumber);
					int   cid = Bytes.toInt(Bytes.toBytes(orgcid),4,4);
					
					for (phoneNumberIndex = 0; phoneNumberIndex < numRangeList.size(); phoneNumberIndex++) {
						Pair<Long, Long> PhoneNumber = numRangeList.get(phoneNumberIndex);
			
						for (timeRangeIndex = 0; timeRangeIndex < timeRangeList.size(); timeRangeIndex++) {
							Pair<Integer, Integer> timePair = timeRangeList
									.get(timeRangeIndex);
							
								for(long number = PhoneNumber.getFirst() ; number <= PhoneNumber.getSecond() ; number++)	
								{
									byte[][] byteArrays = new byte[3][];
									byteArrays[0] = Bytes.toBytes(cid);
									byteArrays[1] = Bytes.toBytes(number);
									byteArrays[2] = Bytes.toBytes(timePair.getFirst());
									
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
													cdrValList , cid);
											
									} catch (IOException e) {
										e.printStackTrace();
									}
									
								}
						}
					}
		}
	}
}
	

