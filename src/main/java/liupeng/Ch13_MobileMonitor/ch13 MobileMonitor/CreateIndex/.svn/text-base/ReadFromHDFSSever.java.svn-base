package CreateIndex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.zookeeper.KeeperException;

import CloudComputingServer.CloudComputingServer;
import CloudComputingServer.SeverTest;
import HBaseIndexAndQuery.HBaseDao.HBaseFileID;
import MyXMLReader.MyXmlReader;

public class ReadFromHDFSSever  extends  CloudComputingServer {

	
    FileSystem fs;
    long endTime = 0;
    String parentName ="";

    long  hdfsFileNumber = 0;
    private String subCut="#";
    private String cut = "@";
    private String hdfsURL ="";
    
    HBaseFileID  hbaseFileID;
    
    private String orgRequestContent="";
	
	public ReadFromHDFSSever(String fileName ,String repairFileName)
	{
		super(fileName);
		initConf(fileName);
		Configuration conf = new Configuration(); 
    	conf.set("fs.default.name",hdfsURL);
    //	HBaseInit();
   	try {
			fs = FileSystem.get(conf);
		} catch (IOException e) { 
			e.printStackTrace();
		}
		RepairThread repair = new RepairThread(fileName);
		repair.setCreateIndexConf(repairFileName);
		repair.setFileSystem(fs);
		Thread thread = new Thread(repair);
		thread.start();
	}
	
	
	public  void HBaseInit()
	{
    	hdfsFileNumber = HBaseFileID.GetMaxFileID();
     	hbaseFileID  = new HBaseFileID();
	}
	
	public void HBaseDoWith()
	{
    	String content = orgRequestContent;
    	StringTokenizer as = new StringTokenizer(content , cut);
    	System.out.println("orgRequestContent  is " + orgRequestContent);
    	while(as.hasMoreTokens())
    	{
    		String path = as.nextToken();
    		System.out.println("path is " + path);
    		long   id = Long.parseLong(as.nextToken());
    		if(hbaseFileID != null)
    		{
    			hbaseFileID.InsertFileAndID("/"+path.replace("#", "/"), id);
    		}
    	}
		if(hbaseFileID != null)
		{
			hbaseFileID.setMaxID(hdfsFileNumber-1);
		}
	}
	
	
    private void initConf(String confFileName)
    {
    	MyXmlReader reader = new MyXmlReader(confFileName);
    		hdfsURL= reader.getName("hdfsURL");
    		parentName = reader.getName("sourceRoot");
    }
	
    
    public void parseAttribute()
    {
    	endTime 				= Long.parseLong(getNextAttibute());
    	hdfsFileNumber = Long.parseLong(getNextAttibute());
    	super.parseAttribute();
    	PrintAttribute();
    }
    
    public void PrintAttribute()
    {
    	System.out.println("endTime  is " + endTime);
    	System.out.println("hdfsFileNumber  is " + hdfsFileNumber);
    	super.PrintAttribute();
    }
    public  void saveAttribute()
    {
    	clearAttribute();
		this.addSaveAttribute(endTime+"");
		this.addSaveAttribute(hdfsFileNumber+"");
    	super.saveAttribute();
    }
    
    public  String  EncapsulationRequest()
	{
		 //format:path@ID@path@ID@path@ID     write for the client , save for sever change recover the file and ID;
		return  orgRequestContent;
	}
	
    
    public void beforeSendData()
    {
    	 //format:path@ID@path@ID@path@ID     write for the client , save for sever change recover the file and ID;
    	//HBaseDoWith();
    }
    
	
	//主master主要做的事
	public void doAction() 
	{
    	while(true)
    	{	
    		System.out.println("开始轮询");
    		CheckNewFile();
    		try {
				Thread.sleep(10000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
    	}
	}
	
	
	 public void CheckNewFile( )
	    {
	    	 List<String>  list = GetFileNames();
	    		try {
	    			
	    		    //format:path@ID@path@ID@path@ID     write for the client , save for sever change recover the file and ID;
	    			orgRequestContent ="";
	    			for( int m = 0 ; m < list.size()  ; m++)
	    			{
	    				String nodePath = list.get(m);
	    				hdfsFileNumber++;
	    				orgRequestContent = orgRequestContent+nodePath+cut+hdfsFileNumber+cut;
	    			}
	    			if(orgRequestContent.length()  > 0)
	    			{
		    			orgRequestContent = orgRequestContent.substring(0,orgRequestContent.length()-1);
		    			sendData(EncapsulationRequest());
	    			}
				} catch (Exception e) {
					e.printStackTrace();
				}
	    }
	 
	 
	    public String GetParentName()
	    {	
	    	return parentName;
	    }
	    private  List<String> GetFileNames( )
	    {
	    	String parentName = GetParentName();
	    	ArrayList<String> fileNameList = new ArrayList<String>();
	    	 try {
	    		 System.out.println(parentName+"******************************");
	    		 if( fs  != null)
	    		 {
					FileStatus fileList[] = fs.listStatus(new Path(parentName));
					if(fileList  != null)
					{
									int size = fileList.length;
									long onceMaxTime = 0;
									long cdrEndTime =endTime;
								
									for (int i = 0; i < size; i++) {
										if (onceMaxTime < fileList[i].getModificationTime()) {
											onceMaxTime = fileList[i].getModificationTime();
										}
				
										if (fileList[i].getModificationTime() > cdrEndTime) {
											fileNameList.add(fileList[i].getPath().getName());
										}
									}
									endTime = onceMaxTime;
						}
				}
	    	 } catch (IOException e) {
	 			e.printStackTrace();
	 		}
	    	return fileNameList;
	    }
	     
	public static void main(String[] args)
	{
		String confile =args[0];
		String repairFile=args[1];
		ReadFromHDFSSever  test = new ReadFromHDFSSever(confile,repairFile);
		try {
			test.check();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (KeeperException e) {
			e.printStackTrace();
		}
	}
}
