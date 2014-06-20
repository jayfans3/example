package CreateIndex;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.zookeeper.KeeperException;

import CloudComputingClient.ClientTest;
import CloudComputingClient.CloudComputingClient;
import CloudComputingClient.TestDoWithQuestThread;
import MyXMLReader.MyXmlReader;

public class ReadFromHDFSClient  extends CloudComputingClient{
	
    FileSystem fs;
    long endTime = 0;

    long  hdfsFileNumber = 0;
    
    private String subCut="#";
    private String cut = "@";
    private String hdfsURL ="";
    private String  indexConfile  ="";

	
	public ReadFromHDFSClient(String conFileName , String  lindexConfile)
	{
		super(conFileName);
		indexConfile = lindexConfile;
		initConf(conFileName);
		Configuration conf = new Configuration();
		conf.set("fs.default.name", hdfsURL );
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
    
	private  void  parseRequest()
	{
		
		 //format:path@ID@path@ID@path@ID     write for the client , save for sever change recover the file and ID;
		
	}
	
	//Client主要做的事
	  public void doAction(String RequestContent,String nodeName)
	{
		  System.out.println("RequestContent  is " + RequestContent +"   nodeName  is " + nodeName );
		  Thread thread = new Thread( new CreateHDFSIndex( RequestContent, this, nodeName , indexConfile ,fs));
		  thread.start();
	}
	  
	  
		public static void main(String[] args)
		{
			String confile =args[0];
			String indexConfile =args[1];
			ReadFromHDFSClient  test = new ReadFromHDFSClient(confile ,indexConfile );
			try {
				test.check();
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (KeeperException e) {
				e.printStackTrace();
			}
		}

}
