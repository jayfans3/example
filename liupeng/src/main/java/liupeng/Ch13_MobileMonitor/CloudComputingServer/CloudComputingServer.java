package CloudComputingServer;

import java.io.IOException;
import java.io.ObjectInputStream.GetField;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;
import java.util.concurrent.LinkedBlockingDeque;


import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event;
import org.apache.zookeeper.data.Stat;

import MyXMLReader.MyXmlReader;


public class CloudComputingServer extends CloudComputingServerBase{
	
	
	private String cutRequest="$";
	String cut="@";
	String attributes=""; 
	
	
    String myZnode   ="";
    
    private boolean active = false;    	
    private  String connectString = "";
    private int sessionTimeout = 0;
    private String root="";
    private String serverPath ="";
    private String serverTmp ="";
    private String serverCon ="";
    private String serverBeforCon="";
    
    private String clientPath ="";
    private String clientTmp ="";
    private String clientPei ="";
    
    long  requestCount = 0;

    int   chooseNumber = 0;
    
    private boolean isBefore = true;
    private String request="";
    
    private boolean start = true;
    
 
    //read the conf file
    private void initConf(String confFileName)
    {
    	
    	MyXmlReader reader = new MyXmlReader(confFileName);
    	connectString= reader.getName("connectString");
        sessionTimeout =Integer.parseInt(reader.getName("sessionTimeout"));
        root=reader.getName("app");
        serverPath =root+"/"+ reader.getName("serverPath");
        serverTmp =root+"/"+ reader.getName("serverTmp");
       serverCon = root+"/"+  reader.getName("serverCon");
        
        clientPath =root+"/"+  reader.getName("clientPath");
        clientTmp =root+"/"+  reader.getName("clientTmp");
        clientPei = root+"/"+  reader.getName("clientPei");
        
        serverBeforCon=serverCon+"before";
    }
    
    
    public CloudComputingServer(String filename) {
    	initConf(filename);
        if(zk == null){
            try {
                zk = new ZooKeeper(connectString, sessionTimeout, this);
                mutex = new Integer(-1);
            } catch (Exception e) {
                zk = null;    
                e.printStackTrace();
            }
        }
        //init the zookeeper
        if (zk != null) {
            try {
                Stat s = zk.exists(root, false);
                if (s == null) {
                    zk.create(root, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                }
               s = zk.exists(serverPath, false);
               if( s == null){
            	   zk.create(serverPath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
               }
               s = zk.exists(serverTmp, false);
               if( s == null){
            	   zk.create(serverTmp, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
               }
               s = zk.exists(serverCon, false);
               if( s == null){
            	   zk.create(serverCon, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
               }
               
               s = zk.exists(serverBeforCon, false);
               if( s == null){
            	   zk.create(serverBeforCon, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
               }
               
               s = zk.exists(clientPath, false);
               if( s == null){
            	   zk.create(clientPath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
               }
               
               s = zk.exists(clientTmp, false);
               if( s == null){
            	   zk.create(clientTmp, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
               }
               
               s = zk.exists(clientPei, false);
               if( s == null){
            	   zk.create(clientPei, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            	   
               }
            } catch (KeeperException e) {
            	e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            Thread thead = new Thread( new MonitorTheClientThread(this, connectString  ,sessionTimeout ,clientTmp));
            thead.start();
        }
    }

    
    void getLock() throws KeeperException, InterruptedException{
        List<String> list = zk.getChildren(serverTmp, false);
        String[] nodes = list.toArray(new String[list.size()]);
        Arrays.sort(nodes);
        if(myZnode.equals(serverTmp+"/"+nodes[0])){
        	active = true;
        	if(!start)
        	{
        		getAttribute();
        	}
        	System.out.println("here");
            doAction();
        }
        else{
            waitForLock(nodes[0]);
        }
    }
    
    
    public  void check() throws InterruptedException, KeeperException {
        myZnode = zk.create(serverTmp + "/" , new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL_SEQUENTIAL);
        zk.setData(myZnode, Bytes.toBytes(IP.getIP()), -1);
        getLock();
    }
    
    void waitForLock(String lower) throws InterruptedException, KeeperException {
        Stat stat = zk.exists(serverTmp + "/" + lower,true);
        if(stat != null){
        	   System.out.println("I am a server ,and I am waiting");
        		waitEvent();
        	  	System.out.println("I am a wake");
        }
        getLock();
        
    }
    
    public boolean isActive()
    {
    	return active;
    }
    
    public void recover( String beforattirbute)
    {
    		//获取 派发的节点 ，,以及 派发的内容；
    		StringTokenizer as = new StringTokenizer(beforattirbute, cutRequest );
    		as.nextToken();
    		String node					  = as.nextToken();
    		String lastRequest        = as.nextToken();
    		long number = 0 - requestCount;
    		System.out.println("start recover");
    		try {
				Thread.sleep(10000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
    		sendData(lastRequest,node ,  number);
    }
    
    public void beforeSendData()
    {
    	
    }
    
    @Override
    public void process(WatchedEvent event) {
        if(event.getType() == Event.EventType.NodeDeleted){
            System.out.println("I am a server ,and I am wake");
            long tmp = 0;
            try
            {
            	start = false;
            }catch( Exception e)
            {
            	e.printStackTrace();
            }
            super.process(event);
        }
    }
    /**
     * 执行其他任务
     */
    public void doAction(){

    }
    
    public int getClientNumber()
    {
    	   List<String> orglist = null;
    	   try {
			orglist = zk.getChildren(clientTmp,false);
    	   } catch (KeeperException e) {
   			e.printStackTrace();
   		} catch (InterruptedException e) {
   			e.printStackTrace();
   		}
   		return orglist.size();
    }

    //派发请求时，选择节点
    public String GetPersistentNode()
    {
    	   List<String> orglist = null;
    	   try {
			orglist = zk.getChildren(clientTmp,false);
			while(orglist.size() == 0)
			{
	        	  try {
	  				Thread.sleep(1000);
	  			} catch (InterruptedException e) {
	  				e.printStackTrace();
	  			}
				orglist = zk.getChildren(clientTmp,false);
			}
			
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
        String[] nodes = orglist.toArray(new String[orglist.size()]);
        Arrays.sort(nodes);
        
        byte[] reslut  = null;
        while(reslut  == null)
        {
        	System.out.println("chooseNumber  is " + chooseNumber );
        	  try {
				reslut = zk.getData( clientTmp+"/"+nodes[chooseNumber%nodes.length], false,null);
			} catch (KeeperException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
        	  chooseNumber++;
        	  if( reslut != null)
        	  {
        		  return  Bytes.toString(reslut);
        	  }
        	  try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
        }
        
        return  Bytes.toString(reslut);
    }
    
    public long GetRequestNumber( )
    {
    	if(requestCount < 0)
    	{
    		requestCount = 0;
    	}
		return requestCount;
    }

    public void IncRequestNumber()
    {
    	requestCount++;
    }
    
    
    public void addSaveAttribute(String attribute)
    {
    	attributes=attributes+attribute+cut;
    }
    
    
    public void clearAttribute()
    {
    	attributes ="";
    }
    
    public void PrintAttribute()
    {
    	System.out.println("requestCount  is " + requestCount);
    }
    public void parseAttribute()
    {
    	String content = getNextAttibute();
    	StringTokenizer as = new StringTokenizer(content,cutRequest);
    	requestCount = Long.parseLong(as.nextToken());
    }
    
    public String getNextAttibute()
    {
    	StringTokenizer as = new StringTokenizer(attributes,cut);
    	if( !as.hasMoreTokens())
    	{
    		return attributes;
    	}
    	String reslut =  as.nextToken();
    	attributes = attributes.substring(reslut.length(), attributes.length());
    	return reslut;
    }
    
    public void getAttribute()
    {
    	byte[]  byteAttribute = null;
    	byte[] beforebyteAttribute = null;
		  try {
		byteAttribute =zk.getData(serverCon, false,null);
		beforebyteAttribute = zk.getData(serverBeforCon, false, null);
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		attributes = Bytes.toString(byteAttribute);
		String beforeAttributes = Bytes.toString(beforebyteAttribute);

		if(attributes.length() > 0   &&  beforeAttributes.length() > 0 && beforeAttributes  != attributes  )
		{
			attributes =  beforeAttributes;
			parseAttribute();
			recover(beforeAttributes);
		}
		else
		{
			if(attributes.length() > 0)
			{
				parseAttribute();
			}
		}
    }
    

    public  void saveAttribute ()
    {
    	attributes = attributes+GetRequestNumber()+cutRequest + request;
		try {
			System.out.println("save   attributes  is " + attributes);
			if(isBefore)
			{
				zk.setData(serverBeforCon,  attributes.getBytes(),-1);
			}else
			{
			zk.setData(serverCon,  attributes.getBytes(),-1);
			}
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
    }
    
    public void sendData(String requestContent  )
	{
		if(requestContent.length() > 0)
		{
	    	String parentNode = GetPersistentNode();
	    	 if(parentNode == null)
	    	 {
	    		 System.out.println("parentNode is null");
	    		 return;
	    	 }
	    	 
			IncRequestNumber();
			sendData(requestContent , parentNode,GetRequestNumber() );
		};
	}
    
	//派发请求  fileContent为请求，parentNode为选中的的节点
    public  void sendData(String requestContent , String parentNode , long requestNumber  )
    {
    	
    	beforeSendData();
    	try
    	{
		if(requestContent.length() > 0)
		{

	    	 System.out.println("parentNode " +  parentNode);
			
			System.out.println("requestContent is "+requestContent );
			System.out.println("pesi_path is " +parentNode+"/"+requestNumber);
			request = parentNode + cutRequest + requestContent;
			isBefore = true;
			saveAttribute();
			clearAttribute();
			if(zk.exists(parentNode+"/"+requestNumber, false) == null)
			{
				zk.create(parentNode+"/"+requestNumber , requestContent.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
			}
			isBefore = false;
			saveAttribute();
			clearAttribute();
		}
    	}catch(Exception e)
    	{
    		e.printStackTrace();
    	}
    }
    
    
    public String EncapsulationRequest()
    {
    	return "";
    }

    
   

    
    public static void main(String[] args) {
        String connectString = "192.168.1.8:"+2181;

        CloudComputingServer lk = new CloudComputingServer(connectString);
        try {
            lk.check();
        } catch (InterruptedException e) {
           e.printStackTrace();
        } catch (KeeperException e) {
        	e.printStackTrace();
        }
    }
	

	
}
