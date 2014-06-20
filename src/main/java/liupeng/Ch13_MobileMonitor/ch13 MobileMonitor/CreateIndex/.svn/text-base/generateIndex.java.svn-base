package CreateIndex;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import cn.cstor.cloud.hbase.cdr.CDRBase.BICCCDR;
import cn.cstor.cloud.hbase.cdr.CDRBase.BSSAPCDR;
import cn.cstor.cloud.hbase.cdr.CDRBase.CDRTable;
import cn.cstor.cloud.hbase.cdr.CDRBase.IUCSCDR;

import MyXMLReader.MyXmlReader;
import ParseCDR.ParseData;

public class generateIndex  implements ParseData {
	public  TreeMap<String,StringBuffer> treeMap=new TreeMap<String,StringBuffer>();
	public  StringBuffer strBuf=new StringBuffer();
	
	public String indexPath="";//"/indexingCDR/iucs/tmp_index/";
	public String toBeMergedPath="";//"/toBeMerged/miniute/iucs/";
	public String recordCounterPath="";
	public String tmpName="";//"tmp_x_index";
	public String mergedInfo=""; 
	public String  sourceRootPath="";   //  /CDRDIR/

	public  long miniStartTime=Long.MAX_VALUE;
	public  long maxStartTime=Long.MIN_VALUE;
	public  Configuration conf;
	public  DistributedFileSystem fs;
	public  long totalCount=0;
	public  String conFileName ="";
	private String hdfsURL ="";
	public  long cc1=0;
	public  long cc2=0;
	public 	long recordNum=0;
	private CDRTable.CDR tableName = null;

	private ArrayList<String> sourceFilePaths = new ArrayList<String>();

	public void addCDRInputFileName(String lName,long lfileNameID)
	{
		sourceFilePaths.add(lName);
	}
	public void setCDRTableType( CDRTable.CDR ltype)
	{
		tableName=ltype;
	}
	
	public void setGenerateConfFile(String filename)
	{
		conFileName = filename;
	}
	
	public static void main(String[] args)
	{
		System.out.println("testing");
	}
	
	public  int decodeTableTypeByName(CDRTable.CDR ltype)
	{
		if(ltype == CDRTable.CDR.BSSAP)
			return 0;
		else if(ltype==CDRTable.CDR.IUCS)
			return 1;
		else
			return 2;
	}
	
	 public  String getHostName() {
		 InetAddress ia = null;
		 try {
		 ia = InetAddress.getLocalHost();
		 } catch (UnknownHostException e) {
		 // TODO Auto-generated catch block
		 e.printStackTrace();
		 } 
		 if (ia == null ) {
		 return "some error..";
		 }
		 else 
		 return ia.getHostName();
		 }
	 
	public  void generateIndexFile(int tableType,ArrayList<String> pathes ) throws IOException
	{
		String time=String.valueOf(new Date().getTime());
    	MyXmlReader reader = new MyXmlReader(conFileName);
		hdfsURL= reader.getName("hdfsURL");
		sourceRootPath = reader.getName("sourceRoot");
		//TODO:
		recordCounterPath=reader.getName("recordCounterPath");//"/indexingCDR/CDRTotalNum/";
		System.out.println("sourceRootPath    is  "  +   reader.getName("sourceRoot"));
		switch(tableType)
		{
			case 0:
				tmpName="tmp_"+time+"_index";
				indexPath= reader.getName("bssapIndexPath")+tmpName+"/";
				toBeMergedPath=  reader.getName("bssapMergePath");
				mergedInfo=  reader.getName("bssapMergeInfo");
				for(int i=0;i<pathes.size();i++)
					scanBassapRawFile(pathes.get(i));
				break;
			case 1:
				tmpName="tmp_"+time+"_index";
				indexPath=reader.getName("iucsIndexPath")+tmpName+"/";
				toBeMergedPath= reader.getName("iucsMergePath") ;//"/toBeMerged/miniute/iucs/";
				mergedInfo=   reader.getName("iucsMergeInfo");  //"#indexingCDR#iucs#";
				for(int i=0;i<pathes.size();i++)
					scanIucsRawFile(pathes.get(i));
				break;
			case 2:
				tmpName="tmp_"+time+"_index";
				indexPath=reader.getName("biccIndexPath")+tmpName+"/";
				toBeMergedPath=reader.getName("biccMergePath");
				mergedInfo=reader.getName("biccMergeInfo"); 
				for(int i=0;i<pathes.size();i++)
					scanBiccRawFile(pathes.get(i));
				break;
			default:
				break;
		}
	}

	public static boolean isLegalNum(String str)
	{
	boolean flag=true;
	try
	{
	Long.valueOf(str); 
	}
	catch(Exception e)
	{
	flag=false;
	System.out.println(str);
	}
	return flag;
	} 

	public  void scanIucsRawFile(String filePath) throws IOException
	{
		byte[] content = new byte[IUCSCDR.SIZE];
		
		FSDataInputStream in=fs.open(new Path(filePath));
		BufferedInputStream bufferIn=new BufferedInputStream(in,65*1024*1024);
		
		IUCSCDR iucsCDR=new IUCSCDR();
		
		int offset=0;
		long tmpTime=0;
		StringBuffer strBuffer=new StringBuffer(300);
		int count=0;
		while(bufferIn.read(content,0,content.length)!=-1)
		{
			if(!iucsCDR.decode(content))
			{
				System.out.println("there is nothing to read now,end ofset is"+offset);
				break;
			}	
			String callNum=iucsCDR.getCalling_number();
			String calledNum=iucsCDR.getCalled_number();
			String recordPath=filePath+"%"+String.valueOf(offset);
			String startTime=iucsCDR.getStart_time_s();
			String endTime=iucsCDR.getEnd_time_s();
			String cdrType=iucsCDR.getCdr_type();
			String wangYuan=iucsCDR.getMscid()+"%"+iucsCDR.getBscid()+"%"+iucsCDR.getCid();
			String tmpNum="0";
			
			if(!callNum.equals("0")&&isLegalNum(callNum))
			{
				//info=cdrType+","+"b,"+startTime+","+endTime+","+wangYuan+","+recordPath;
				strBuffer.append(cdrType);
				strBuffer.append(",");
				strBuffer.append("z");
				strBuffer.append(",");
				strBuffer.append(startTime);
				strBuffer.append(",");
				strBuffer.append(endTime);
				strBuffer.append(",");
				strBuffer.append(wangYuan);
				strBuffer.append(",");
				strBuffer.append(recordPath);
			
				tmpNum=FormatePhoneNum(callNum);
				if(Long.valueOf(tmpNum).longValue()!=0)
					putInTreeTable(tmpNum,strBuffer.toString());
				strBuffer.delete(0, strBuffer.length());
				
				tmpTime=Long.valueOf(startTime);
				if(tmpTime<miniStartTime&&tmpTime>=1200000000)
					miniStartTime=tmpTime;
				if(tmpTime>maxStartTime&&tmpTime<=1500000000)
					maxStartTime=tmpTime;
//				System.out.println(tmpTime);
			}
			
			if(!calledNum.equals("0")&&isLegalNum(calledNum))
			{
				//info=cdrType+","+"b,"+startTime+","+endTime+","+wangYuan+","+recordPath;
				strBuffer.append(cdrType);
				strBuffer.append(",");
				strBuffer.append("b");
				strBuffer.append(",");
				strBuffer.append(startTime);
				strBuffer.append(",");
				strBuffer.append(endTime);
				strBuffer.append(",");
				strBuffer.append(wangYuan);
				strBuffer.append(",");
				strBuffer.append(recordPath);
				
				tmpNum=FormatePhoneNum(calledNum);
				if(Long.valueOf(tmpNum).longValue()!=0)
					putInTreeTable(tmpNum,strBuffer.toString());
				strBuffer.delete(0, strBuffer.length());
				
				tmpTime=Long.valueOf(startTime);
				if(tmpTime<miniStartTime&&tmpTime>=1200000000)
					miniStartTime=tmpTime;
				if(tmpTime>maxStartTime&&tmpTime<=1500000000)
					maxStartTime=tmpTime;
				
//				System.out.println(tmpTime);
			}
			count++;
			offset=content.length*count;
			recordNum++;
		}
		bufferIn.close();
		in.close();
		
		System.out.println("file:"+filePath+" is done!");
		totalCount+=count;
	}

	public  void scanBiccRawFile(String filePath) throws IOException
	{
		byte[] content = new byte[BICCCDR.SIZE];
		
		FSDataInputStream in=fs.open(new Path(filePath));
		BufferedInputStream bufferIn=new BufferedInputStream(in,65*1024*1024);
		
		BICCCDR biccCDR=new BICCCDR();
		
		int offset=0;
		long tmpTime=0;
		StringBuffer strBuffer=new StringBuffer(300);
		int count=0;
		while(bufferIn.read(content,0,content.length)!=-1)
		{
			if(!biccCDR.decode(content))
			{
				System.out.println("there is nothing to read now,end ofset is"+offset);
				break;
			}	
			String callNum=biccCDR.getCalling_party_number();
			String calledNum=biccCDR.getCalled_party_number();
			String recordPath=filePath+"%"+String.valueOf(offset);
			String startTime=biccCDR.getStart_time_s();
			String endTime=biccCDR.getEnd_time_s();
			String opc=biccCDR.getOpc();
			String dpc=biccCDR.getDpc();
			String tmpNum="0";
//			if(count<2)
//			{
//				System.out.println(callNum);
//				System.out.println(calledNum);
//				System.out.println(startTime);
//				System.out.println(endTime);
//				System.out.println(opc);
//				System.out.println(dpc);
//				System.out.println(recordPath);
//			}
			
			if(callNum.equals("0")&&calledNum.equals("0"))
			{
				cc1++;
			}
			else
				cc2++;
			
			if(!callNum.equals("0")&&isLegalNum(callNum))
			{
				//info="z,"+startTime+","+endTime+","+opc+","+dpc+","+recordPath;
				strBuffer.append("z");
				strBuffer.append(",");
				strBuffer.append(startTime);
				strBuffer.append(",");
				strBuffer.append(endTime);
				strBuffer.append(",");
				strBuffer.append(opc);
				strBuffer.append(",");
				strBuffer.append(dpc);
				strBuffer.append(",");
				strBuffer.append(recordPath);
			
				tmpNum=FormatePhoneNum(callNum);
				if(Long.valueOf(tmpNum).longValue()!=0)
					putInTreeTable(tmpNum,strBuffer.toString());
				strBuffer.delete(0, strBuffer.length());
				
				tmpTime=Long.valueOf(startTime);
				if(tmpTime<miniStartTime&&tmpTime>=1200000000)
					miniStartTime=tmpTime;
				if(tmpTime>maxStartTime&&tmpTime<=1500000000)
					maxStartTime=tmpTime;
				
//				System.out.println(tmpTime);
			}
			if(!calledNum.equals("0")&&isLegalNum(calledNum))
			{
				//info="b,"+startTime+","+endTime+","+opc+","+dpc+","+recordPath;
				strBuffer.append("b");
				strBuffer.append(",");
				strBuffer.append(startTime);
				strBuffer.append(",");
				strBuffer.append(endTime);
				strBuffer.append(",");
				strBuffer.append(opc);
				strBuffer.append(",");
				strBuffer.append(dpc);
				strBuffer.append(",");
				strBuffer.append(recordPath);
				
				tmpNum=FormatePhoneNum(calledNum);
				if(Long.valueOf(tmpNum).longValue()!=0)
					putInTreeTable(tmpNum,strBuffer.toString());
				strBuffer.delete(0, strBuffer.length());
				
				tmpTime=Long.valueOf(startTime);
				if(tmpTime<miniStartTime&&tmpTime>=1200000000)
					miniStartTime=tmpTime;
				if(tmpTime>maxStartTime&&tmpTime<=1500000000)
					maxStartTime=tmpTime;
//				System.out.println(tmpTime);
			}
			count++;
			offset=content.length*count;
			recordNum++;
		}
		bufferIn.close();
		in.close();
		
		System.out.println("file:"+filePath+" is done!");
		totalCount+=count;
	}

	public  void scanBassapRawFile(String filePath) throws IOException
	{
		byte[] content = new byte[BSSAPCDR.SIZE];
		
		FSDataInputStream in=fs.open(new Path(filePath));
		BufferedInputStream bufferIn=new BufferedInputStream(in,65*1024*1024);
		
		BSSAPCDR bssapReader=new BSSAPCDR();
		
		int offset=0;
		long tmpTime=0;
		StringBuffer strBuffer=new StringBuffer(300);
		int count=0;
		while(bufferIn.read(content,0,content.length)!=-1)
		{
			if(!bssapReader.decode(content))
			{
				System.out.println("there is nothing to read now,end ofset is"+offset);
				break;
			}	
			String callNum=bssapReader.getCalling_number();
			String calledNum=bssapReader.getCalled_number();
			String recordPath=filePath+"%"+String.valueOf(offset);
			String startTime=bssapReader.getStart_time_s();
			String endTime=bssapReader.getEnd_time_s();
			String cdrType=bssapReader.getCdr_type();
			String wangYuan=bssapReader.getMscid()+"%"+bssapReader.getBscid()+"%"+bssapReader.getCid();
			String tmpNum="0";
//			if(count<5)
//			{
//				System.out.println(callNum);
//				System.out.println(calledNum);
//				System.out.println(startTime);
//				System.out.println(endTime);
//				System.out.println(wangYuan);
//				System.out.println(recordPath);
//			}
			
			if(callNum.equals("0")&&calledNum.equals("0"))
			{
				cc1++;
			}
			else
				cc2++;
			
			if(!callNum.equals("0")&&isLegalNum(callNum))
			{
				//info=cdrType+","+"b,"+startTime+","+endTime+","+wangYuan+","+recordPath;
				strBuffer.append(cdrType);
				strBuffer.append(",");
				strBuffer.append("z");
				strBuffer.append(",");
				strBuffer.append(startTime);
				strBuffer.append(",");
				strBuffer.append(endTime);
				strBuffer.append(",");
				strBuffer.append(wangYuan);
				strBuffer.append(",");
				strBuffer.append(recordPath);
				
				tmpNum=FormatePhoneNum(callNum);
				if(Long.valueOf(tmpNum).longValue()!=0)
					putInTreeTable(tmpNum,strBuffer.toString());
				strBuffer.delete(0, strBuffer.length());
				
				tmpTime=Long.valueOf(startTime);
				if(tmpTime<miniStartTime&&tmpTime>=1200000000)
					miniStartTime=tmpTime;
				if(tmpTime>maxStartTime&&tmpTime<=1500000000)
					maxStartTime=tmpTime;
				
//				System.out.println(tmpTime);
			}
			if(!calledNum.equals("0")&&isLegalNum(calledNum))
			{
				//info=cdrType+","+"b,"+startTime+","+endTime+","+wangYuan+","+recordPath;
				strBuffer.append(cdrType);
				strBuffer.append(",");
				strBuffer.append("b");
				strBuffer.append(",");
				strBuffer.append(startTime);
				strBuffer.append(",");
				strBuffer.append(endTime);
				strBuffer.append(",");
				strBuffer.append(wangYuan);
				strBuffer.append(",");
				strBuffer.append(recordPath);
				
				tmpNum=FormatePhoneNum(calledNum);
				if(Long.valueOf(tmpNum).longValue()!=0)
					putInTreeTable(tmpNum,strBuffer.toString());
				strBuffer.delete(0, strBuffer.length());
				
				tmpTime=Long.valueOf(startTime);
				if(tmpTime<miniStartTime&&tmpTime>=1200000000)
					miniStartTime=tmpTime;
				if(tmpTime>maxStartTime&&tmpTime<=1500000000)
					maxStartTime=tmpTime;
				
//				System.out.println(tmpTime);
			}
			count++;
			offset=content.length*count;
			recordNum++;
		}
		bufferIn.close();
		in.close();

		System.out.println("file:"+filePath+" is done!");
		totalCount+=count;
	}

	
	public  boolean doWithCDR() 
	{
		try
		{
					System.out.println("where there is a will ,there is a way");
				  	MyXmlReader reader = new MyXmlReader(conFileName);
					hdfsURL= reader.getName("hdfsURL");
					
					Date time1=new Date();
					conf = new Configuration();
					fs = new DistributedFileSystem();
					fs.initialize(new URI(hdfsURL), conf);
										
					int tableType=decodeTableTypeByName(tableName);

					generateIndexFile(tableType,sourceFilePaths);

					System.out.println("*****");
				
					//ShowHashTable();
					
					Date time2=new Date();
					System.out.println("scan costs "+(time2.getTime()-time1.getTime())/1000+" seconds");
					System.out.println("scan costs "+(time2.getTime()-time1.getTime())+" ms");
//					System.out.println(cc1);
//					System.out.println(cc2);
					
					writeToFile();		
//					for(int i=0;i<sourceFilePaths.size();i++)
//					{
//						String lunPath = sourceFilePaths.get(i).replace('/', '#');
//						lunPath = sourceRootPath+"/" + lunPath.substring(1,lunPath.length());
//						fs.delete(new Path(lunPath));
//						System.out.println(lunPath+" deleted");
//					}
					Date time3=new Date();
					System.out.println("write costs "+(time3.getTime()-time2.getTime())/1000+" seconds");
					System.out.println("write costs "+(time3.getTime()-time2.getTime())+" ms");
					
					System.out.println("Total costs "+(time3.getTime()-time1.getTime())/1000+" seconds");
					System.out.println("ToTAL costs "+(time3.getTime()-time1.getTime())+" ms");
			
					System.out.println("Scaned "+totalCount+" in total");
					System.out.println("MiniStartTime:"+miniStartTime);
					System.out.println("MaxStartTime:"+maxStartTime);
					
			//		System.out.println(treeMap.size());
			//		File f = new File("E:/testdata/CDR/index/");
			//		String strInfo="E:/testdata/CDR/index/".replaceFirst("index",String.valueOf(miniStartTime)+"_"+String.valueOf(maxEndTime));
			//		System.out.println(strInfo);
			//		File ft = new File(strInfo);
			//		f.renameTo(ft);
					
			//		String ss=readLineByOffset("C:/testdata/CDR/rawCDR1G.txt", 10931429);
			//		System.out.println(ss);
					return true;
		}catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}
	
	public  void updateStartTime(String startTime)
	{
		long tmp=Long.valueOf(startTime);
		if(tmp<miniStartTime)
			miniStartTime=tmp;
		if(tmp>maxStartTime)
			maxStartTime=tmp;
	}
	
	public  void writeToFile() throws IOException
	{
		if(treeMap.size()==0)
		{
			System.out.println("empty file");
			return;
		}
		TreeMap<String,String> secondLevelInfo=new TreeMap<String,String>();
		String firstLevelPath=indexPath+"first_level.data";
		String secondLevelPath=indexPath+"second_level.data";

		fs.mkdirs(new Path(indexPath));
		FSDataOutputStream firstOut;
		FSDataOutputStream secondOut;
		
		//path=firstLevelPath+"index.data";
		//System.out.println(path);
		
		boolean flag=false;
//		System.out.println(treeMap.size());
		long offset=0;
		if(!fs.exists(new Path(firstLevelPath))&&!fs.exists(new Path(secondLevelPath)))
		{
			System.out.println("((((");
			int i=treeMap.entrySet().size();
			firstOut=fs.create(new Path(firstLevelPath));
			String str;
			for(Entry<String,StringBuffer> innerEntry:treeMap.entrySet())
				{
					str=innerEntry.getKey()+" "+innerEntry.getValue()+"\r\n";
					firstOut.writeBytes(str);
					secondLevelInfo.put(innerEntry.getKey(),String.format("%019d",offset));
					offset+=str.length();
//					System.out.println(i);
					i--;
				}
			System.out.println("))))");
			firstOut.close();
			
			StringBuffer strBuffer=new StringBuffer();
			for(Entry<String,String> innerEntry:secondLevelInfo.entrySet())
			{				
				strBuffer.append(innerEntry.getKey()+" "+innerEntry.getValue()+"\r\n");
			}
			secondOut=fs.create(new Path(secondLevelPath));
			secondOut.writeBytes(strBuffer.toString());
			secondOut.close();
			strBuffer.delete(0, strBuffer.length());
			secondLevelInfo.clear();
			treeMap.clear();
		}
		else
			System.out.println("File Already exsits");
		
		String time=String.valueOf(new Date().getTime());
		fs.mkdirs(new Path(indexPath.replaceFirst(tmpName, miniStartTime+"_"+maxStartTime+"_"+time)));
		fs.rename(new Path(firstLevelPath), new Path(indexPath.replaceFirst(tmpName, miniStartTime+"_"+maxStartTime+"_"+time)));
		fs.rename(new Path(secondLevelPath), new Path(indexPath.replaceFirst(tmpName, miniStartTime+"_"+maxStartTime+"_"+time)));
	
		fs.create(fs,new Path(toBeMergedPath+mergedInfo+miniStartTime+"_"+maxStartTime+"_"+time),new FsPermission((short) 777));
		fs.create(fs,new Path(recordCounterPath+recordNum+"_"+time),new FsPermission((short) 777));
		fs.delete(new Path(indexPath), true);
		treeMap.clear();
	}
	
	
	public static String FormatePhoneNum(String phoneNum)
	{
		if(phoneNum.length()==11)
			return phoneNum;
		else if(phoneNum.length()>11)
			return phoneNum.substring(phoneNum.length()-11);
		else
			return String.format("%011d", Long.valueOf(phoneNum));
	}
	
	public  void ShowHashTable()
	{
		for(Entry<String,StringBuffer> entry:treeMap.entrySet())
		{
			System.out.println(entry.getKey());
			System.out.println(entry.getValue());
			System.out.println("**********");
		}
	}
	
	public  void putInTreeTable(String phoneNum,String info)
	{
		if(treeMap.containsKey(phoneNum))
		{
			treeMap.put(phoneNum, treeMap.get(phoneNum).append(";").append(info));
//			//String tmp=treeMap.get(phoneNum);
//			strBuf.append(treeMap.get(phoneNum));
//			strBuf.append(";");
//			strBuf.append(info);
//			//treeMap.put(phoneNum, tmp+";"+info);
//			treeMap.put(phoneNum, strBuf.toString());
//			strBuf.delete(0, strBuf.length());
		}
		else
			treeMap.put(phoneNum, new StringBuffer(info));
	}
		
	public  void newFolder(String folderPath) {
	    try {
	    
	      String filePath = folderPath;
	      filePath = filePath.toString();
	      File myFilePath = new File(filePath);
	      if (!myFilePath.exists()) 
	      {
	        myFilePath.mkdir();
	      }
	    }
	    catch (Exception e) {
	      System.out.println("");
	      e.printStackTrace();
	    }
	}
}
