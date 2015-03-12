package CloudComputingServer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.zookeeper.KeeperException;

import MyXMLReader.MyXmlReader;

public class SeverTest  extends  CloudComputingServer{
	
    FileSystem fs;
    long endTime = 0;
    String parentName = "/CDRDIR/";

    long  hdfsFileNumber = 0;
    private String subCut="#";
    private String cut = "@";
    private String hdfsURL ="";
    
    
    
    private String orgRequestContent="";
	
	public SeverTest(String fileName )
	{
		super(fileName);
		initConf(fileName);
		Configuration conf = new Configuration(); 
    	conf.set("fs.default.name",hdfsURL);
    	try {
			fs = FileSystem.get(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
    	
	}
	
    private void initConf(String confFileName)
    {
    	MyXmlReader reader = new MyXmlReader(confFileName);
    		hdfsURL= reader.getName("hdfsURL");
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
	    	 } catch (IOException e) {
	 			e.printStackTrace();
	 		}
	    	return fileNameList;
	    }
	     
	public static void main(String[] args)
	{
		String confile ="D:/conf/hdfsReader.xml";
		SeverTest  test = new SeverTest(confile);
		try {
			test.check();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (KeeperException e) {
			e.printStackTrace();
		}
	}

}
