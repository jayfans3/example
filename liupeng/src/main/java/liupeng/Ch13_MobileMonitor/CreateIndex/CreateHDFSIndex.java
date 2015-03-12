package CreateIndex;


import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Pair;

import HBaseIndexAndQuery.CreateHBaseIndex.ParseDataIntoHBase;
import MyXMLReader.MyXmlReader;
import ParseCDR.ParseData;
import cdr_drill.CDRDrillIndexer;
import cn.cstor.cloud.hbase.cdr.CDRBase.CDRTable;
public class CreateHDFSIndex implements Runnable {

		 String  fileName = null;
		 CDRTable.CDR type = null;

		 String cut ="@";
		
		 String bssapType ="BSSAP";
		 String iucsType	 ="IUCS";
		 String biccType	 ="BICC";
		    
		 ReadFromHDFSClient parent = null;
		 String content = "";
		 String nodeName = "";
		 String confFileName ="";
		 String hdfsURL ="";
		 String queryPath="";
		 FileSystem fs =null;
		 String fileLog ="";
		 PrintWriter  myFile =  null;
		 
		ArrayList<Pair<String,Long>> bssaps = new ArrayList<Pair<String,Long>>();
		ArrayList<Pair<String,Long>>  iucss = new ArrayList<Pair<String,Long>>();
		ArrayList<Pair<String,Long>>  biccs  = new ArrayList<Pair<String,Long>>();
		ArrayList<Pair<String,Long>>  events  = new ArrayList<Pair<String,Long>>();
		
		 
		public CreateHDFSIndex( String lcontent  ,ReadFromHDFSClient lparent ,String lnodeName  ,String lConfFileName ,FileSystem lfs)
		{
			content	 = lcontent;
			parent   = lparent;
			nodeName = lnodeName;
			confFileName = lConfFileName;
			fs 	= lfs;
		}
		
		public void InitConf()
		{
	    	MyXmlReader reader = new MyXmlReader(confFileName);
			queryPath =reader.getName("sourceRoot");
			fileLog =reader.getName("fileLog");
			
		}
		
		//  for hdfs index
		public  void run()
		{
				InitConf();
		//		RunHBase();
				RunHDFS();
				myFile.close();
		}
		
		public void RunHDFS()
		{			
			System.out.println("in  CreateHDFSIndex ");
			StringTokenizer as = new StringTokenizer(content,cut);
			while(as.hasMoreTokens())
			{
				String fileName = as.nextToken();
				fileName = "/" + fileName.replace("#", "/");
				System.out.println("fileName is " + fileName);
				long fileNumber = Long.parseLong(as.nextToken());
				
				//Èç¹ûÊÇbssap
				if (fileName.matches(".+/bssap.*")) {
						if(  ! fileName.matches(".+/bssapEvent.*") )
						{
							bssaps.add(new Pair(fileName,fileNumber));
						}
						else
						{
							events.add(new Pair(fileName,fileNumber));
						}
				} else if (fileName.matches(".+/bicc.*")) {

					if(  ! fileName.matches(".+/biccEvent.*") )
					{
						biccs.add(new Pair(fileName,fileNumber));
					}
					else
					{
						events.add(new Pair(fileName,fileNumber));
					}
				} else if (fileName.matches(".+/iucs.*")) {
					
					if(  ! fileName.matches(".+/iucsEvent.*") )
					{
						iucss.add(new Pair(fileName,fileNumber));
					}
					else
					{
						events.add(new Pair(fileName,fileNumber));
					}
				}
			}
			
			System.out.println("I am herer");
			int i = 0 ;
			CreateHDFSIndexImp bssap = new CreateHDFSIndexImp();
			bssap.setGenerateConfFile(confFileName);
			bssap.setCDRTableType(CDRTable.CDR.BSSAP);
			bssap.setFileList(bssaps);
			bssap.setLogFile(fileLog);
			bssap.setFileSystem(fs);
			bssap.createIndex();
	
				
			
			CreateHDFSIndexImp iucs = new CreateHDFSIndexImp();
			iucs.setGenerateConfFile(confFileName);
			iucs.setCDRTableType(CDRTable.CDR.IUCS);
			iucs.setFileList(iucss);
			iucs.setLogFile(fileLog);
			iucs.setFileSystem(fs);
			iucs.createIndex();
			
			
			CreateHDFSIndexImp bicc = new CreateHDFSIndexImp();
			bicc.setGenerateConfFile(confFileName);
			bicc.setCDRTableType(CDRTable.CDR.BICC);
			bicc.setFileList(biccs);
			bicc.setLogFile(fileLog);
			bicc.setFileSystem(fs);
			bicc.createIndex();
			
			
			for(i = 0 ; i < events.size() ; i++)
			{
				String lunPath = events.get(i).getFirst().replace('/', '#');
				lunPath = queryPath+"/" + lunPath.substring(1,lunPath.length());
				try {
					AddLog(lunPath);
					fs.delete(new Path(lunPath));
				} catch (IOException e) {
					e.printStackTrace();
				}
				System.out.println(lunPath+" deleted");
			}
			parent.RequestIsOver(nodeName);
		}
		

		public void RunHBase()
		{
			System.out.println("*******************************start hbase insert****************");
			System.out.println("content  is " + content +"   cut  is " + cut);
			StringTokenizer as = new StringTokenizer(content,cut);
			while(as.hasMoreTokens())
			{
				String fileName = as.nextToken();
				fileName = "/" + fileName.replace("#", "/");
				System.out.println("fileName is " + fileName);
				long fileNumber = Long.parseLong(as.nextToken());
				Pair<String,CDRTable.CDR> pair = null;
				
				if (fileName.matches(".+/bssap.*")) {
					if( !fileName.matches(".+/bssapEvent.*"))
					pair = new Pair<String, CDRTable.CDR>(fileName,CDRTable.CDR.BSSAP);
				}
				System.out.println("I am here");
				if(pair != null)
				{		System.out.println("********start hbase insert  file is  :" + fileName );
						ParseDataIntoHBase parse = new ParseDataIntoHBase();
						parse.addCDRInputFileName(pair.getFirst(),fileNumber);
						parse.setCDRTableType(pair.getSecond());
						
						if( !parse.doWithCDR())
						{
							System.out.println(" file dowith is failed : " + pair.getFirst());
						}
						parse.LogRecordFileNameAndID(pair.getFirst(), fileNumber);
						System.out.println("file dowith is over"+pair.getFirst()+" number is " + fileNumber + "  cdrtype is " +pair.getSecond() +"  thread num " + Thread.currentThread() );
				}
			}
		}
		

	
		synchronized public void AddLog(String name)
		{
			myFile.println("file is" + name);
			myFile.flush();
		}
}


