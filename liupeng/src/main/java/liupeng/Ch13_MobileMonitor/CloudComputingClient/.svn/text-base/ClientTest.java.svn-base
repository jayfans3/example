package CloudComputingClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.zookeeper.KeeperException;

import CloudComputingServer.SeverTest;
import MyXMLReader.MyXmlReader;

public class ClientTest  extends CloudComputingClient {

    FileSystem fs;
    long endTime = 0;

    long  hdfsFileNumber = 0;
    
    private String subCut="#";
    private String cut = "@";
    private String hdfsURL ="";
    
    
    
    private String orgRequestContent="";
	
	public ClientTest(String fileName )
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
	
	protected  void  parseRequest()
	{
		
		 //format:path@ID@path@ID@path@ID     write for the client , save for sever change recover the file and ID;
		
	}
	
	
	//Client主要做的事
	  public void doAction(String RequestContent,String nodeName)
	{
	//	  System.out.println("RequestContent  is " + RequestContent +"   nodeName  is " + nodeName );
		  Thread thread = new Thread( new TestDoWithQuestThread(this,nodeName,RequestContent));
		  thread.start();
		  //RequestIsOver(nodeName);
	}
		     
	public static void main(String[] args)
	{
		String confile ="D:/conf/hdfsReader.xml";;
		ClientTest  test = new ClientTest(confile);
		try {
			test.check();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (KeeperException e) {
			e.printStackTrace();
		}
	}

	
}
