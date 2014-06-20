package CloudComputingClient;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;

import CloudComputingServer.IP;
import MyXMLReader.MyXmlReader;

public class CloudComputingClient extends CloudComputingClientBase {

	    String myZnode;
	    String myContentNode;
	   int      sessionTimeout = 0;
	    String connectFileName ="";
	    String root="";
	    String clientPath="";
	    String clientTmpPath="";
	    String clientPeiPath ="";
	    int    lastEnd  =-1;
	    String cut="@";
	   
	    private ArrayList<Integer>  failFileName = new ArrayList<Integer>();
	    
	    //read the conf file
	    private void initConf(String confFileName)
	    {
	    	MyXmlReader reader = new MyXmlReader(confFileName);
	    	connectFileName = reader.getName("connectString");
	    	root = reader.getName("app");
	    	clientPath= 		root +"/" + reader.getName("clientPath");
	    	clientTmpPath =  root +"/" + reader.getName("clientTmp");
	    	clientPeiPath =  root +"/" + reader.getName("clientPei");
	    	sessionTimeout = Integer.parseInt(reader.getName("sessionTimeout"));
	    	System.out.println("clientPeiPath  is " + clientPeiPath);

	    }
	    
	    public CloudComputingClient(String filePath) {
	    	initConf(filePath);
	        if(zk == null){
	            try {
	                System.out.println("创建一个新的连接:");
	                zk = new ZooKeeper(connectFileName, sessionTimeout, this);
	                mutex = new Integer(-1);
	            } catch (Exception e) {
	                zk = null;
	                e.printStackTrace();
	            }
	        }
	        if (zk != null) {
	            try {
	                Stat s = zk.exists(root, false);
	                if (s == null) {
	                    zk.create(root, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
	                }
	                 s = zk.exists(clientPath, false);
	                if (s == null) {
	                    zk.create(clientPath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
	                }
	                 s = zk.exists(clientTmpPath, false);
	                if (s == null) {
	                    zk.create(clientTmpPath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
	                }
	                 s = zk.exists(clientPeiPath, false);
		                if (s == null) {
		                    zk.create(clientPeiPath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		                }
	                
	            } catch (KeeperException e) {
	            	e.printStackTrace();
	            } catch (InterruptedException e) {
	                e.printStackTrace();
	            }
	        }
	    }
	    
	    public synchronized void RequestIsOver(String fileName)
	    {
	    	  try {
	    	  zk.delete(fileName, -1);
	    	  zk.getChildren(myContentNode, true); //register new watch
	    	  System.out.println("zk delete : " + fileName );
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (KeeperException e) {
				e.printStackTrace();
			}
	    }
	    
	    public void check() throws InterruptedException, KeeperException {

	    	 System.out.println("create the node");    	 
	    	myContentNode = zk.create(clientPeiPath+"/" ,IP.getIP().getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT_SEQUENTIAL);
	        myZnode = zk.create(clientTmpPath + "/" , myContentNode.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL_SEQUENTIAL);
	        
	        System.out.println("myContentNode is " + myContentNode);
	        System.out.println("myZnode is "+ myZnode);
	        
	        //监控请求是否到来
	        checkRequestComing(myContentNode);

	    }
	    
	    
	    boolean checkIsContain( ArrayList<Integer> list ,int number)
	    {
	    	for(int i = 0 ; i < list.size() ; i ++)
	    	{
	    		if(list.get(i).intValue() == number)
	    		{
	    			return true;
	    		}
	    	}
	    	return false;
	    }
	    


	    void checkRequestComing(String myContentNode)
	    {
	    	  int maxNumber = -1;
	        while(true)
	        {	  
	              List<String> orglist = null;;
				try {
					orglist = zk.getChildren(myContentNode, true);
				} catch (KeeperException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
	              List<String> list = new ArrayList<String>();
	              
	               
	               boolean clearFailFileTag = true;
	               for(int i = 0 ; i < orglist.size() ; i++)
	               {
	            	   int number = Integer.parseInt(orglist.get(i));
	            	   // 负数代表 1. 其他节点失败后，转移到本节点的内容  
	            	   //               2.相同的一个请求，因为server切换，导致连续发送了2次，第二次以负数的形式发送，直接无视；
	            	   if(number < 0 )
	            	   {
	            		   clearFailFileTag = false;
	            	   }
	            	   if(number < 0 && ! checkIsContain(failFileName,number))
	            	   {
		            		   if(Math.abs(number)  == lastEnd)
		            		   {
		            			   //需要清除node ,还需要清除轮询目录，这里**********
		            			   System.out.println("第二种情况出现，忽略该节点" + number);
		            			   RequestIsOver(myContentNode+"/"+number);
		            		   }
		            		   else
		            		   {
		            		   System.out.println(" number is " + number);
		            		   failFileName.add(number);
		            		   list.add(orglist.get(i));
		            		   }
	            	   }
	            	   
	            	   if( number > lastEnd && number > 0)
	            	   {
	            		   list.add(orglist.get(i));
	            	   }
	            	   
	            	   if(maxNumber < number )
	            	   {
	            		   maxNumber = number;
	            	   }
	               }
	             
	               lastEnd	= maxNumber;
	               
	               if(clearFailFileTag)
	               {
	            	   failFileName.clear();
	               }
	               
	               String[] nodes = list.toArray(new String[list.size()]);
	               for(int i = 0 ; i < nodes.length ; i++)
	               {
	            	   byte[] data = null;
						try {
							data = zk.getData(myContentNode+"/"+nodes[i], null, null);
						} catch (KeeperException e) {
							e.printStackTrace();
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
	            	   doAction(Bytes.toString(data),myContentNode+"/"+nodes[i]);
	               }     

		       	   this.waitEvent(); 	 

	        }
	    }
	    	    
	 
	    
	    @Override
	    public void process(WatchedEvent event) {
	    	if(event.getType() == Event.EventType.NodeChildrenChanged)
	    	{	
	    		super.process(event);
	    	}	        
	    }
	    /**
	     * 执行其他任务
	     */
	       public void doAction(String fileName,String nodeName){
	    	   System.out.println("************************************************************************");
//	    	executorService.execute( new ParseAndDoWithThread(hDao,fsURL,fileName,this,nodeName));    	
	    }
	    
	

}

	
	

