package CloudComputingServer;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event;
import org.apache.zookeeper.Watcher.Event.KeeperState;

import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.HashMap;


//作用：监控临时文件的
public class MonitorTheClientThread implements Watcher,Runnable{
	Integer mutex = -1;
	CloudComputingServer server = null;
	ZooKeeper zk = null;
	String  clientPath ="";
	private CountDownLatch connectSignal = new CountDownLatch(1);
	List<String> lastDirNode = new ArrayList<String>(); //tmpPath
	HashMap<String,String> map = new HashMap<String,String>();
	 String cut="@";

	
	
	public MonitorTheClientThread(CloudComputingServer lserver,String connectString , int sessionTimeout ,String lclientPath)
	{
		server = lserver;
		clientPath = lclientPath;
		  if(zk == null){
	            try {
	                System.out.println("创建一个新的连接:监控client的状态");
	                zk = new ZooKeeper(connectString, sessionTimeout, this);
	                connectSignal.await();
		               if( zk.exists(clientPath, false) == null)
		               {
		            	   zk.create(clientPath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		               }
	                mutex = new Integer(-1);
	            } catch (Exception e) {
	                zk = null;
	                e.printStackTrace();
	            }
	        }
	}
	
	public void run()
	{     
		//监控readClient的进程
		while(true)
		{
			 try {
				  List<String> orglist = zk.getChildren(clientPath, true);
				  List<String> orglistOp = new ArrayList<String>();
				  List<String> oldPathOp = new ArrayList<String>();
				  int i = 0;
				  for( i = 0 ; i < orglist.size() ; i++)
				  {
					  orglistOp.add(orglist.get(i));
				  }
				  for( i = 0 ; i < lastDirNode.size(); i++)
				  {
					  oldPathOp.add(lastDirNode.get(i));
				  }
				  //更新现在的目录
				  orglistOp.removeAll(lastDirNode);   //新增的节点
				  oldPathOp.removeAll(orglist);		 //删除的节点
				  lastDirNode.addAll(orglistOp);
				  lastDirNode.removeAll(oldPathOp);
				 
					  for(int m = 0 ; m < orglistOp.size() ; m++)
					  {
						  map.put(orglistOp.get(m),
								  Bytes.toString(zk.getData(clientPath+"/"+orglistOp.get(m),null,null))
								  );
						  System.out.println("新增  节点     "+orglistOp.get(m) +"    ");
						  String pesiPath = Bytes.toString(zk.getData(clientPath+"/"+orglistOp.get(m), null, null));
						  System.out.println("content is " + pesiPath);

					  }
				  
					  for(int m = 0 ; m < oldPathOp.size() ; m++)
					  {
						  String filePath = map.get(oldPathOp.get(m));
						  System.out.println("删除 节点     "+oldPathOp.get(m) +"    ");
						  if(server.isActive())
						  {
							  Thread thread = new Thread(new BalanceWhenFailThread(zk,filePath,clientPath));
							  thread.start();
						  }
					  }
				 
				  synchronized (mutex) {
				            mutex.wait();
				   }
			} catch (KeeperException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
		public void process(WatchedEvent event) {
			System.out.println("event type is " +event.getType());
			if(event.getType() == Event.EventType.NodeChildrenChanged)
	    	{	
	    		  synchronized (mutex) {
	    	            mutex.notify();
	    	        }
	    	}
	    	if(event.getState() == KeeperState.SyncConnected)
	    	{	
	    		connectSignal.countDown();
	    		return ;
	    	}
	    	
    
      
    }

}
