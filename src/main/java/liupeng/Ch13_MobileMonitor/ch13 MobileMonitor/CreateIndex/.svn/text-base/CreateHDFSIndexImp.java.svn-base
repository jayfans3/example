package CreateIndex;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Pair;

import ParseCDR.ParseData;
import cdr_drill.CDRDrillIndexer;
import cn.cstor.cloud.hbase.cdr.CDRBase.CDRTable;

public class CreateHDFSIndexImp {
	
	 String bssapType ="BSSAP";
	 String iucsType	 ="IUCS";
	 String biccType	 ="BICC";
	 ParseData pdbssap =null;
	 ParseData drillbasap =null;
	 String logFile="";
	 PrintWriter  myFile =  null;
	 String queryPath="";
	 FileSystem fs=null;
	 
	 ArrayList<Pair<String,Long>>list= null;
	 
	 public  CreateHDFSIndexImp()
	 {
			 pdbssap = new generateIndex();
			 drillbasap = new  CDRDrillIndexer();
	 }
	 
	 public void setLogFile(String llogFile)
	 {
		 	logFile =llogFile;
			File myFilePath = new File(logFile);
			try {
					if( !myFilePath.exists())
					{
							myFilePath.createNewFile();
							FileWriter resultFile = new FileWriter(myFilePath,true); 
							myFile = new PrintWriter(resultFile); 
					}
					FileWriter resultFile = new FileWriter(myFilePath,true); 
					myFile = new PrintWriter(resultFile); 
			} catch (IOException e) {
				e.printStackTrace();
			}
	 }
	 
	 public void setLunXunPath(String path)
	 {
		 queryPath=path;
	 }
	 
	 public void setFileSystem( FileSystem lfs)
	 {
		 fs = lfs;
	 }
	 
	public void setGenerateConfFile(String confFileName)
	{
		System.out.println("pdbssap   is " + pdbssap);
		System.out.println("confFileName  is " + confFileName);

		pdbssap.setGenerateConfFile(confFileName);
		drillbasap.setGenerateConfFile(confFileName);
	}
	public void setCDRTableType( CDRTable.CDR type)
	{
		pdbssap.setCDRTableType(type);
		drillbasap.setCDRTableType(type);
	}
	
	public void setFileList( 	ArrayList<Pair<String,Long>> lists )
	{
		list = lists;
		for( int i = 0 ; i < lists.size() ; i ++)
		{
			drillbasap.addCDRInputFileName(lists.get(i).getFirst(), lists.get(i).getSecond());
			pdbssap.addCDRInputFileName( lists.get(i).getFirst(), lists.get(i).getSecond());
		}
	}
	
	synchronized public void AddLog(String name)
	{
		myFile.println("file is" + name);
		myFile.flush();
	}
	
	public void createIndex()
	{
		if(list.size() > 0)
		{
			drillbasap.doWithCDR();
			pdbssap.doWithCDR();
			for( int i=0;i<list.size();i++)
			{
				String lunPath = list.get(i).getFirst().replace('/', '#');
				lunPath = queryPath+"/" + lunPath.substring(1,lunPath.length());
				try {
					AddLog(lunPath);
					fs.delete(new Path(lunPath));
				} catch (IOException e) {
					e.printStackTrace();
				}
				System.out.println(lunPath+" deleted");
			}				
		}
	}
}
