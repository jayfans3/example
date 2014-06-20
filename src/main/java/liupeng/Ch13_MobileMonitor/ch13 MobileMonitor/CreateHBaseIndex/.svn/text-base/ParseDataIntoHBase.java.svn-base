package HBaseIndexAndQuery.CreateHBaseIndex;
import java.io.IOException;
import java.util.Calendar;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import HBaseIndexAndQuery.HBaseDao.HBaseDaoImp;
import HBaseIndexAndQuery.HBaseDao.HBaseFileID;
import HBaseIndexAndQuery.HBaseDao.HBaseFileLog;
import ParseCDR.ParseData;
import cn.cstor.cloud.hbase.cdr.CDRBase.CDRTable;

public class ParseDataIntoHBase implements ParseData{

	private String       fileName  = null;
	private CDRTable.CDR  type = null;
	private RequestParse parse = null;
	private HBaseDaoImp  dao = null;
	private FileSystem   fs = null;
	private int			cdrSize = 0;
	private long fileNameID = 0;
	private String conFile ="";
	private String fsURL = "hdfs://192.168.1.8:9000";
	
	public void setGenerateConfFile(String filename)
	{
		conFile  =   filename;
	}
	
	public ParseDataIntoHBase( )
	{
		dao = HBaseDaoImp.GetDefaultDao();
		Configuration fsConf = new Configuration();
		fsConf.set("fs.default.name", fsURL);
		try {
			fs = FileSystem.get(fsConf);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}
	
	public void  LogRecordFileNameAndID( String fileName, long fileID  )
	{
		HBaseFileLog log = new HBaseFileLog(dao);
		log.InsertFileAndID(fileName, fileID);
	}
	
	public void addCDRInputFileName(String lName,long lfileNameID)
	{
		fileName = lName;
		fileNameID = lfileNameID;
	}
	
	public void setCDRTableType( CDRTable.CDR ltype)
	{
		type = ltype;
		if( type == CDRTable.CDR.BSSAP)
		{
			try {
				System.out.println("my name is bssap");
				parse  = new BSSAPRequestParse(dao);
				cdrSize = CDRTable.BSSAPSIZE;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}else if( type == CDRTable.CDR.IUCS)
		{
			try {
				parse  = new IUCSRequestParse(dao);
				cdrSize = CDRTable.IUCSSIZE;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}else  if( type == CDRTable.CDR.BICC)
		{
			try {
				parse  = new BICCRequestParse(dao);
				cdrSize = CDRTable.BICCSIZE;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public boolean doWithCDR()
	{
		return InserCDRIntoHBase();
	}
	
	private boolean InserCDRIntoHBase()
	{
		FSDataInputStream hdfsInStream = null;
		try
		{

			if(!fs.exists(new Path(fileName)))
			{
				System.out.println( fileName + " is not exists in the HDFS,please check ");
				fs.close();		
				return false;
			}
			System.out.println(fileName);
			hdfsInStream = fs.open(new Path(fileName));

			int i = -1;
			while(true)
			{
				if(hdfsInStream.available() > 0)
				{
					 i++;
					 parse.ParseAndInsert(fileNameID,i*cdrSize,hdfsInStream,cdrSize);
					 if( i% 10000 == 0)
					 {
						 	if(parse  == null)
						 	{
						 		System.out.println("parse is null: before the parse");
						 	}
						 	parse.Commit();
					 }					 
				}
				else
				{
					break;
				}
				
			}
			parse.Commit();
			hdfsInStream.close();
			return true;
		}
		catch(Exception e )
		{
			e.printStackTrace();
			return false;
		}
		finally
		{
			try
			{
				if(hdfsInStream != null)
				{
					hdfsInStream.close();
				}
			}
			catch(Exception e )
			{
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) throws IOException {

		System.out.println("Start Time: "
				+ Calendar.getInstance().getTime().toString());
		ParseDataIntoHBase a = new ParseDataIntoHBase();
		a.setCDRTableType(CDRTable.CDR.BSSAP);
		a.addCDRInputFileName("/ftest/bassap3/20110319/19/1300535993_933115_1300536000_283786_SMPBAK.dat",0);
		a.doWithCDR();
		
	}
	
	
}
