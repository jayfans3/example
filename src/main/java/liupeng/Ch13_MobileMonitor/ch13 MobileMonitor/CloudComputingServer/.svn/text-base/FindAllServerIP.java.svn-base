package CloudComputingServer;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import MyXMLReader.MyXmlReader;

public class FindAllServerIP  implements Watcher{

   	private    String  connectFileName ="";
   	private     int sessionTimeout =0;
   	private     String  app ="";
   	private     String serverTmpPath="";
   	private   ZooKeeper zk = null;
   	
    public void process(WatchedEvent event) {
    	
    }
   	
   	public   void getConfig( String fileName)
   	{
    	MyXmlReader reader = new MyXmlReader(fileName);
    	connectFileName = reader.getName("connectString");
    	sessionTimeout = Integer.parseInt( reader.getName("sessionTimeout") );
    	app = reader.getName("app");
    	serverTmpPath= 	app+"/"+reader.getName("serverTmp");
   	}
   	
   	public FindAllServerIP( String fileName)
   	{
   		getConfig(fileName);
   		try {
			zk = new ZooKeeper(connectFileName, sessionTimeout, this);
		} catch (IOException e) {
			e.printStackTrace();
		}
   	}
   	
   	public void printAllServerIP()
   	{
   		try {
		List<String> list	= zk.getChildren(serverTmpPath, false);
		for(int i = 0 ; i < list.size() ; i ++)
		{

				String server = list.get(i);
				String ip = Bytes.toString(zk.getData(serverTmpPath+"/"+server, null, null));
				System.out.println( "app is : "+ app +"  server  ip is  " + ip);
		}
			
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
   	}
   	
	public  static void main(String[] args)
	{
		for( int i = 0 ; i <  args.length ; i ++)
		{
			FindAllServerIP ip = new FindAllServerIP(args[i]);
			ip.printAllServerIP();
		}
	}
	
}

