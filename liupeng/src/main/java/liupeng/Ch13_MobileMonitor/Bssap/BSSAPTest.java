package HBaseIndexAndQuery.QueryHBase.Bssap;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import HBaseIndexAndQuery.HBaseDao.HBaseDaoImp;
import Format.BytesToString;



public class BSSAPTest {
	
	public static  void main(String args[])
	{
		HBaseDaoImp dao =  HBaseDaoImp.GetDefaultDao();
		try {
			HTable table = dao.getHBaseTable("hbase_bssap_cdr_CalledIndex");
			Scan scan = new Scan();
			ResultScanner rs = table.getScanner(scan);
			Iterator<Result>  results = rs.iterator();
			while(results.hasNext())
			{
				Result res =results.next();
				byte[]  rowkey = res.getRow();
				
				byte[]  bcalledNumber   =new byte[8];
				System.arraycopy(rowkey, 0, bcalledNumber, 0, 8);

				System.out.println("***************find  calledNumber  is " +   BytesToString.Bytes8toString(bcalledNumber) );
				System.out.println("***************find  calledNumber  is " +   Arrays.toString(bcalledNumber));
				
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}

}
