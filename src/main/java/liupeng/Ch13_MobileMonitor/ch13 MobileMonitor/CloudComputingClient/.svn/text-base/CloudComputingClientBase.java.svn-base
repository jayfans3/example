package CloudComputingClient;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class CloudComputingClientBase implements Watcher {

	
	   	protected  ZooKeeper zk = null;
	    protected  Integer mutex;
	    
	 	    
	    public CloudComputingClientBase() {

	    }
	    synchronized public void process(WatchedEvent event) {
	        synchronized (mutex) {
	            mutex.notify();
	        }
	    }
	    
	     public void waitEvent() {
	        synchronized (mutex) {
	            try {
					mutex.wait();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
	        }
	    }
}
