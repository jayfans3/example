package HBaseIndexAndQuery.QueryHBase;

import java.util.List;

import org.apache.hadoop.hbase.client.HTable;

public interface HBaseIndex {

/*	public void SetConditionRange(String valName, String start, String end) ;
	
	public void SetConditionValue(String valName,String values);*/
	
	public byte[] CombineRowKey(byte[][] byteArrays);
	
	public byte[] GetTheScanFromTheFirstRow( byte[] beginRowKey, HTable table);
	
	public void RunQuery();
	
	public void SetCondtion(String name ,  List  list);
	
	public boolean  CheckAttribute(byte[] rowKey);
	
	
	
}
