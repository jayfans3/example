package HBaseIndexAndQuery.QueryHBase;

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.hbase.util.Pair;

import HBaseIndexAndQuery.HBaseDao.HBaseDaoImp;
import HBaseIndexAndQuery.HBaseDao.HBaseFileID;


public class CDRPosInFile {
	
	static HashMap<Long ,String> map ;

	static{
		map = new HashMap<Long ,String>();
	}
	

	public long   fileID  = -1;
	public long   offset   = -1;
	//public String fileName = null;
	
	private static int    range = 100;	
	public CDRPosInFile(long lfileID ,long loffset)
	{
		fileID 		= 	lfileID;
		offset 		=  	loffset;
		//fileName 	= GetFileName();
	}
	
	
	public static String  GetFileName(long id)
	{
		String path = map.get(id);
		
		if(path == null)
		{
			GetFileFromID(id , range);
		}
		path = map.get(id);
		return path;
	}
	
	public long GetOffset()
	{
		return offset;
	}
	
	public String toString()
	{
		return "  " + CDRPosInFile.GetFileName(fileID)+"  "+offset;
	}
	
	public static void GetFileFromID(long id , int range)
	{
		System.out.println("find the file ID");
		HBaseDaoImp imp =  HBaseDaoImp.GetDefaultDao();
		ArrayList<Pair<Long,String>>  list = HBaseFileID.ConvertIDToPath(id,range,imp);
		for( int i = 0 ; i < list.size() ; i++)
		{
			CDRPosInFile.map.put(list.get(i).getFirst(), list.get(i).getSecond());
		}
	}
	
}
