package namenodezk;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;


public class addMemmber implements Watcher{
	
	
	private ZooKeeper zk;
	private CountDownLatch connectedSignal=new CountDownLatch(1);
	private static final Charset CHARSET=Charset.forName("UTF-8");
	
	public void connect(String hosts) throws IOException, InterruptedException{
		int SESSION_TIMEOUT=5000;
		//System.out.println("i begin");
		zk=new ZooKeeper(hosts,SESSION_TIMEOUT,this);
		
		//System.out.println("i am here");
		connectedSignal.await();
	}
		
	public boolean isNodeExit(String path) throws KeeperException, InterruptedException{
		Stat st=zk.exists(path, this);
		if(st==null)
			return false;
		else
			return true;
	}
	
	public String getetdate(String path) throws KeeperException, InterruptedException{
		byte[] data=zk.getData(path, this, null);
		
		return new String(data);
	}
	
	public void setdate(String path,String d) throws KeeperException, InterruptedException{
		
		zk.setData(path, d.getBytes(CHARSET), -1);
	}
    
	public void appeddate(String path,String d) throws KeeperException, InterruptedException{
		
		String m=this.getetdate(path);
		
		StringBuilder sb=new StringBuilder(m);
		sb=sb.append(d);
		String c=sb.toString();
		
    	 zk.setData(path, c.getBytes(CHARSET), -1);
	}
	@Override
	public void process(WatchedEvent event) {
		// TODO Auto-generated method stub
	
		if(event.getState()==KeeperState.SyncConnected){
			connectedSignal.countDown();
			//System.out.println("wo shi watcher");
			System.out.println("the envent is"+event.toString());
			
		}
	}
	public void create(String groupName) throws KeeperException, InterruptedException{
		String path="/"+groupName;
		//System.out.println("i use watcher");
		String createdPath=zk.create(path, null/*data*/, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		//System.out.print("create"+createdPath);	
	}
	
	public void join(String groupName,String m) throws KeeperException, InterruptedException{
		String path="/"+groupName+"/"+m;
		System.out.println("i use watcher");
		String createdPath=zk.create(path, "1111".getBytes()/*data*/, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
		System.out.print("create"+createdPath);	
	}
	public void joinOne(String m) throws KeeperException, InterruptedException{
		String path=m;
		//System.out.println("i use watcher");
		String createdPath=zk.create(path, "1111".getBytes()/*data*/, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
		//System.out.print("create"+createdPath);	
	}
	public void list(String group) throws KeeperException, InterruptedException{
		
		List<String> children=zk.getChildren("/"+group, this);
		if(children.isEmpty())
			System.out.print(group+"* ");
		else 
			{
			System.out.println(group+"^ ");   
			for(String a:children){
				//group=
			    list(group+"/"+a);
				}
			    }
			
		
	}
	public void deleteincludechildren(String group) throws KeeperException, InterruptedException{
		List<String> children=zk.getChildren("/"+group, this);
		if(children.isEmpty())
			zk.delete("/"+group, -1);
		else 
			{
			   
			for(String a:children){
				
			    list(group+"/"+a);
			  
				}
			
			}
	}
			    
	
	public void close() throws InterruptedException{
		zk.close();
	}
	
	}


